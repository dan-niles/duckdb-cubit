// src/cubit_scalar_functions.cpp
#include "cubit.hpp"          // For CUBITModule (if RegisterScalarFunctions is a member)
#include "cubit_index.hpp"    // For CUBITIndex
#include <chrono> 
#include <iomanip>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

// Bind data for the cubit_count_explicit function
struct CubitCountExplicitBindData : public FunctionData {
    CUBITIndex &index;
    Value const_value_to_count;

    CubitCountExplicitBindData(CUBITIndex &idx, Value val) : index(idx), const_value_to_count(std::move(val)) {}

    unique_ptr<FunctionData> Copy() const override {
        return make_uniq<CubitCountExplicitBindData>(index, const_value_to_count);
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<CubitCountExplicitBindData>();
        return &index == &other.index && const_value_to_count == other.const_value_to_count;
    }
};

// Function implementation for cubit_count_explicit
static void CubitCountExplicitExec(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &bind_data = (*state.expr.Cast<BoundFunctionExpression>().bind_info).Cast<CubitCountExplicitBindData>();

    uint32_t encoded_value;
    LogicalTypeId value_type_id = bind_data.const_value_to_count.type().id();

    if (value_type_id == LogicalTypeId::VARCHAR) {
        string s_val = bind_data.const_value_to_count.GetValue<string>();
        auto it = bind_data.index.string_to_code.find(s_val);
        if (it != bind_data.index.string_to_code.end()) {
            encoded_value = it->second;
        } else { // Value not in dictionary, count is 0
            result.Reference(Value::BIGINT(0));
            return;
        }
    } else if (value_type_id == LogicalTypeId::INTEGER) {
        encoded_value = static_cast<uint32_t>(bind_data.const_value_to_count.GetValue<int32_t>());
    } else {
        throw InternalException("cubit_count_explicit: Unexpected type in execution. This should have been caught during binding.");
    }

    auto start = std::chrono::high_resolution_clock::now();

    int count = bind_data.index.Count(encoded_value);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start).count();
    std::cerr << std::fixed << std::setprecision(3)
            << "Count query on CUBIT took "
            << duration << " ms. Result: " << count << std::endl;

    result.Reference(Value::BIGINT(count));
}

// Generic bind logic for cubit_count_explicit
static unique_ptr<FunctionData> CubitCountExplicitBindLogic(ClientContext &context, ScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments, const LogicalType& expected_value_arg_type) {
    if (arguments.size() != 3) {
        throw BinderException("cubit_count_explicit requires 3 arguments: table_name (VARCHAR), column_name (VARCHAR), value_to_count (INTEGER or VARCHAR)");
    }

    for (size_t i = 0; i < 3; ++i) {
        if (!arguments[i]->IsFoldable()) {
            throw BinderException(StringUtil::Format("Argument %zu to cubit_count_explicit must be a constant literal.", i + 1));
        }
        // Evaluate the constant expression using EvaluateScalar
        Value constant_arg_value = ExpressionExecutor::EvaluateScalar(context, *arguments[i]);
        // Replace the expression in the arguments vector with a BoundConstantExpression
        arguments[i] = make_uniq<BoundConstantExpression>(constant_arg_value);

        // This check is now somewhat redundant if EvaluateScalar succeeded without allow_unfoldable=true,
        // but it's a good assertion that the replacement worked as expected.
        if (arguments[i]->type != ExpressionType::VALUE_CONSTANT) {
             throw BinderException(StringUtil::Format("Argument %zu to cubit_count_explicit must evaluate to a constant value. Replacement failed.", i + 1));
        }
    }

    Value table_name_val = arguments[0]->Cast<BoundConstantExpression>().value;
    Value column_name_val = arguments[1]->Cast<BoundConstantExpression>().value;
    Value value_to_count_val = arguments[2]->Cast<BoundConstantExpression>().value;

    if (table_name_val.IsNull() || table_name_val.type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("First argument (table_name) to cubit_count_explicit must be a non-null VARCHAR literal.");
    }
    if (column_name_val.IsNull() || column_name_val.type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("Second argument (column_name) to cubit_count_explicit must be a non-null VARCHAR literal.");
    }
    if (value_to_count_val.IsNull() || value_to_count_val.type() != expected_value_arg_type) {
         throw BinderException(StringUtil::Format("Third argument (value_to_count) to cubit_count_explicit must be a non-null %s literal.", LogicalTypeIdToString(expected_value_arg_type.id())));
    }

    string table_name_str = table_name_val.GetValue<string>();
    string column_name_str = column_name_val.GetValue<string>();

    auto &catalog = Catalog::GetCatalog(context, "memory");
    auto table_entry_ptr = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, DEFAULT_SCHEMA, table_name_str, OnEntryNotFound::THROW_EXCEPTION);
    auto &table_entry = table_entry_ptr->Cast<TableCatalogEntry>();

    if (!table_entry.IsDuckTable()) {
        throw BinderException("cubit_count_explicit currently only supports DuckTables.");
    }
    auto &duck_table = table_entry.Cast<DuckTableEntry>();
    auto &table_info = *duck_table.GetStorage().GetDataTableInfo();

    LogicalIndex logical_col_idx = duck_table.GetColumnIndex(column_name_str);
    if (logical_col_idx.index == DConstants::INVALID_INDEX) {
        throw BinderException(StringUtil::Format("Column '%s' not found in table '%s'.", column_name_str, table_name_str));
    }
    column_t storage_col_id = duck_table.GetColumns().LogicalToPhysical(logical_col_idx).index;

    CUBITIndex *found_cubit_index = nullptr;
    table_info.GetIndexes().Scan([&](Index &index_instance) {
        if (index_instance.GetIndexType() == CUBITIndex::TYPE_NAME) {
            auto &cubit_idx_ref = static_cast<CUBITIndex &>(index_instance);
            if (cubit_idx_ref.MatchesColumn(storage_col_id)) {
                found_cubit_index = &cubit_idx_ref;
                return true;
            }
        }
        return false;
    });

    if (!found_cubit_index) {
        throw BinderException(StringUtil::Format("No CUBIT index found on column '%s' of table '%s'.", column_name_str, table_name_str));
    }

    LogicalType indexed_expression_type = found_cubit_index->logical_types[0];
    if (value_to_count_val.type().id() != indexed_expression_type.id()) {
        throw BinderException(StringUtil::Format(
            "Type mismatch for cubit_count_explicit: Index on column '%s' (expression type: %s) is incompatible with provided 'value_to_count' type %s.",
            column_name_str, LogicalTypeIdToString(indexed_expression_type.id()), LogicalTypeIdToString(value_to_count_val.type().id())));
    }

    return make_uniq<CubitCountExplicitBindData>(*found_cubit_index, value_to_count_val);
}

// Specific bind functions for overloaded types
static unique_ptr<FunctionData> CubitCountExplicitBindInt(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {
    return CubitCountExplicitBindLogic(context, bound_function, arguments, LogicalType::INTEGER);
}

static unique_ptr<FunctionData> CubitCountExplicitBindVarchar(ClientContext &context, ScalarFunction &bound_function,
                                                          vector<unique_ptr<Expression>> &arguments) {
    return CubitCountExplicitBindLogic(context, bound_function, arguments, LogicalType::VARCHAR);
}

// Definition of the registration function declared in cubit.hpp
void CUBITModule::RegisterScalarFunctions(DatabaseInstance &db) {
    ScalarFunctionSet cubit_count_explicit_set("cubit_count");

    cubit_count_explicit_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
        LogicalType::BIGINT,
        CubitCountExplicitExec,
        CubitCountExplicitBindInt
    ));

    cubit_count_explicit_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::BIGINT,
        CubitCountExplicitExec,
        CubitCountExplicitBindVarchar
    ));

    ExtensionUtil::RegisterFunction(db, cubit_count_explicit_set);
}

} // namespace duckdb