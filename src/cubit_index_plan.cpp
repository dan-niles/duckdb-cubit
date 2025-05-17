#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "cubit.hpp"
#include "cubit_index.hpp"
#include "cubit_index_physical_create.hpp"

namespace duckdb {

PhysicalOperator &CUBITIndex::CreatePlan(PlanIndexInput &input) {
	auto &create_index = input.op;
	auto &context = input.context;
	auto &planner = input.planner;

	// Verify the expression type
	if (create_index.expressions.size() != 1) {
		throw BinderException("CUBIT indexes can only be created over a single column of keys.");
	}
	auto &arr_type = create_index.expressions[0]->return_type;
	if (arr_type.id() != LogicalTypeId::INTEGER) {
		throw BinderException("CUBIT index keys must be of type INTEGER.");
	}

	// projection to execute expressions on the key columns

	vector<LogicalType> new_column_types;
	vector<unique_ptr<Expression>> select_list;
	for (auto &expression : create_index.expressions) {
		new_column_types.push_back(expression->return_type);
		select_list.push_back(std::move(expression));
	}
	new_column_types.emplace_back(LogicalType(LogicalType::ROW_TYPE));
	select_list.push_back(
	    make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, create_index.info->scan_types.size() - 1));

	auto &projection =
	    planner.Make<PhysicalProjection>(new_column_types, std::move(select_list), create_index.estimated_cardinality);
	projection.children.push_back(input.table_scan);

	// filter operator for IS_NOT_NULL on each key column
	vector<LogicalType> filter_types;
	vector<unique_ptr<Expression>> filter_select_list;

	for (idx_t i = 0; i < new_column_types.size() - 1; i++) {
		filter_types.push_back(new_column_types[i]);
		auto is_not_null_expr =
		    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		auto bound_ref = make_uniq<BoundReferenceExpression>(new_column_types[i], i);
		is_not_null_expr->children.push_back(std::move(bound_ref));
		filter_select_list.push_back(std::move(is_not_null_expr));
	}

	auto &null_filter = planner.Make<PhysicalFilter>(std::move(filter_types), std::move(filter_select_list),
	                                                 create_index.estimated_cardinality);
	null_filter.types.emplace_back(LogicalType(LogicalType::ROW_TYPE));
	null_filter.children.push_back(projection);

	auto &physical_create_index = planner.Make<PhysicalCreateCUBITIndex>(
	    create_index.types, create_index.table, create_index.info->column_ids, std::move(create_index.info),
	    std::move(create_index.unbound_expressions), create_index.estimated_cardinality);
	physical_create_index.children.push_back(null_filter);
	return physical_create_index;
}

} // namespace duckdb