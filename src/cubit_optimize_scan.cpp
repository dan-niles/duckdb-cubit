#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

#include "cubit.hpp"
#include "cubit_index.hpp"
#include "cubit_index_scan.hpp"

namespace duckdb {

//-----------------------------------------------------------------------------
// Plan rewriter
//-----------------------------------------------------------------------------
class CUBITIndexScanOptimizer : public OptimizerExtension {
public:
	CUBITIndexScanOptimizer() {
		optimize_function = Optimize;
	}

    static unique_ptr<CUBITIndexScanBindData> TryCreateIndexBindData(
        ClientContext &context,
        DuckTableEntry &duck_table,
        DataTableInfo &table_info,
        idx_t column_index,
        const Value &search_value
    ) {
        unique_ptr<CUBITIndexScanBindData> bind_data = nullptr;

        table_info.GetIndexes().BindAndScan<CUBITIndex>(context, table_info, [&](CUBITIndex &cubit_index) {
            if (!cubit_index.MatchesColumn(column_index)) {
                return false;
            }

            uint32_t encoded_val;

            if (search_value.type().id() == LogicalTypeId::VARCHAR) {
                auto str = search_value.ToString();
                auto it = cubit_index.string_to_code.find(str);
                if (it == cubit_index.string_to_code.end()) {
                    return false; // no match in dictionary
                }
                encoded_val = it->second;
            } else {
                try {
                    encoded_val = search_value.GetValue<uint32_t>();
                } catch (...) {
                    return false; // unsupported or invalid cast
                }
            }

            bind_data = make_uniq<CUBITIndexScanBindData>(duck_table, cubit_index, /*limit*/ 0, encoded_val);
            return true;
        });

        return bind_data;
    }

    static bool TryOptimizeGetWithFilter(ClientContext &context, unique_ptr<LogicalOperator> &plan, LogicalFilter &filter, LogicalGet &get) {
        if (get.function.name != "seq_scan") {
            return false;
        }

        auto &table = *get.GetTable();
        if (!table.IsDuckTable()) {
            return false;
        }
        auto &duck_table = table.Cast<DuckTableEntry>();
        auto &table_info = *table.GetStorage().GetDataTableInfo();

        for (auto &expr : filter.expressions) {
            if (expr->type != ExpressionType::COMPARE_EQUAL) continue;
            auto &cmp = expr->Cast<BoundComparisonExpression>();

            // Only support column = constant
            if (cmp.left->type != ExpressionType::BOUND_COLUMN_REF ||
                cmp.right->type != ExpressionType::VALUE_CONSTANT) continue;

            auto &column_ref = cmp.left->Cast<BoundColumnRefExpression>();
            const Value &const_val = cmp.right->Cast<BoundConstantExpression>().value;

            idx_t column_index = column_ref.binding.column_index;
            unique_ptr<CUBITIndexScanBindData> bind_data = nullptr;

            bind_data = TryCreateIndexBindData(context, duck_table, table_info, column_index, const_val);

            if (!bind_data) continue;

            // Replace with index scan
            get.function = CUBITIndexScanFunction::GetFunction();
            get.bind_data = std::move(bind_data);

            // Remove the filter — we pushed it into the scan
            plan = std::move(filter.children[0]);
            return true;
        }

        return false;
    }

    static bool TryOptimizePushedDownFilter(ClientContext &context, unique_ptr<LogicalOperator> &plan, LogicalGet &get) {
        if (get.function.name != "seq_scan") {
            return false;
        }

        auto &table = *get.GetTable();
        if (!table.IsDuckTable()) {
            return false;
        }
        auto &duck_table = table.Cast<DuckTableEntry>();
        auto &table_info = *table.GetStorage().GetDataTableInfo();

        // Look into filters pushed into the GET node
        for (auto &filter_entry : get.table_filters.filters) {
            idx_t column_index = filter_entry.first;
            TableFilter *filter = filter_entry.second.get();

            // Only handle equality filter
            if (filter->filter_type != TableFilterType::CONSTANT_COMPARISON) continue;

            auto &cmp_filter = filter->Cast<ConstantFilter>();
            if (cmp_filter.comparison_type != ExpressionType::COMPARE_EQUAL) continue;

            const Value &search_value = cmp_filter.constant;

            unique_ptr<CUBITIndexScanBindData> bind_data = nullptr;
            bind_data = TryCreateIndexBindData(context, duck_table, table_info, column_index, search_value);

            if (!bind_data) continue;

            // Update scan to use index
            get.function = CUBITIndexScanFunction::GetFunction();
            get.bind_data = std::move(bind_data);
            return true;
        }

        return false;
    }

    static bool TryOptimize(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
        if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
            auto &filter = plan->Cast<LogicalFilter>();
            if (filter.children.size() != 1 || filter.children[0]->type != LogicalOperatorType::LOGICAL_GET) {
                return false;
            }
            auto &get = filter.children[0]->Cast<LogicalGet>();
            return TryOptimizeGetWithFilter(context, plan, filter, get);

        } else if (plan->type == LogicalOperatorType::LOGICAL_GET) {
            auto &get = plan->Cast<LogicalGet>();
            return TryOptimizePushedDownFilter(context, plan, get);
        }

        return false;
    }

	static bool OptimizeChildren(ClientContext &context, unique_ptr<LogicalOperator> &plan) {

		auto ok = TryOptimize(context, plan);
		// Recursively optimize the children
		for (auto &child : plan->children) {
			ok |= OptimizeChildren(context, child);
		}
		return ok;
	}

	static void MergeProjections(unique_ptr<LogicalOperator> &plan) {
		if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			if (plan->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto &child = plan->children[0];

				if (child->children[0]->type == LogicalOperatorType::LOGICAL_GET &&
				    child->children[0]->Cast<LogicalGet>().function.name == "cubit_index_scan") {
					auto &parent_projection = plan->Cast<LogicalProjection>();
					auto &child_projection = child->Cast<LogicalProjection>();

					column_binding_set_t referenced_bindings;
					for (auto &expr : parent_projection.expressions) {
						ExpressionIterator::EnumerateExpression(expr, [&](Expression &expr_ref) {
							if (expr_ref.type == ExpressionType::BOUND_COLUMN_REF) {
								auto &bound_column_ref = expr_ref.Cast<BoundColumnRefExpression>();
								referenced_bindings.insert(bound_column_ref.binding);
							}
						});
					}

					auto child_bindings = child_projection.GetColumnBindings();
					for (idx_t i = 0; i < child_projection.expressions.size(); i++) {
						auto &expr = child_projection.expressions[i];
						auto &outgoing_binding = child_bindings[i];

						if (referenced_bindings.find(outgoing_binding) == referenced_bindings.end()) {
							// The binding is not referenced
							// We can remove this expression. But positionality matters so just replace with int.
							expr = make_uniq_base<Expression, BoundConstantExpression>(Value(LogicalType::TINYINT));
						}
					}
					return;
				}
			}
		}
		for (auto &child : plan->children) {
			MergeProjections(child);
		}
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		auto did_use_cubit_scan = OptimizeChildren(input.context, plan);
		if (did_use_cubit_scan) {
			MergeProjections(plan);
		}
	}
};

//-----------------------------------------------------------------------------
// Register
//-----------------------------------------------------------------------------
void CUBITModule::RegisterScanOptimizer(DatabaseInstance &db) {
	// Register the optimizer extension
	db.config.optimizer_extensions.push_back(CUBITIndexScanOptimizer());
}

} // namespace duckdb