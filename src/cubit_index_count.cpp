#include "cubit_index_count.hpp"
#include "cubit_bridge.hpp" // For CUBIT_TABLE_EVALUATE
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/data_table/data_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/table_binding.hpp" // Required for BindInfo related to table
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"


namespace duckdb {

// CUBITIndexCountBindData is defined in the header.

// This is the bind function that will be called by the optimizer
// or when the function is used directly in SQL.
// The optimizer (from step 1) should create and set the CUBITIndexCountBindData.
// This function primarily defines the return type and column names.
unique_ptr<FunctionData> CUBITIndexCountFunction::CUBITIndexCountBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {

    return_types.push_back(LogicalType::UBIGINT);
    names.push_back("count"); // Or a more descriptive name like "cubit_count_val"

    // When the optimizer uses this function, it will prepare and set the bind_data.
    // If this function were to be used directly from SQL (e.g., SELECT * FROM cubit_index_count(...)),
    // we would need to parse input.inputs here to get table name, column name, and search value,
    // then find the CUBITIndex and construct the CUBITIndexCountBindData.
    // For now, we assume the optimizer path is primary, so we return nullptr here,
    // expecting the optimizer to provide the fully formed bind_data.
    // If the optimizer doesn't set it, get_bind_info might fail.
    // However, the optimizer in cubit_optimize_scan.cpp *will* create it.
    return nullptr;
}

unique_ptr<GlobalTableFunctionState> CUBITIndexCountFunction::CUBITIndexCountInitGlobal(
    ClientContext &context, TableFunctionInitInput &input) {
    // No global state needed for a simple count from the index.
    // The CUBITIndexCountExecute function will perform the count in one go.
    return nullptr;
}

void CUBITIndexCountFunction::CUBITIndexCountExecute(
    ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {

    auto &bind_data = data_p.bind_data->Cast<CUBITIndexCountBindData>();

    // Call the CUBIT index's evaluate method.
    // Using tid = 0 as a placeholder or default. This might need refinement
    // if CUBIT requires specific thread IDs from DuckDB's execution context.
    uint64_t count_val = CUBIT_TABLE_EVALUATE(bind_data.index.cubit_table.get(), 0 /* tid */, bind_data.search_value);

    output.SetCardinality(1);
    output.data[0].SetValue(0, Value::UBIGINT(count_val));
}

// Provides BindInfo for the table function, primarily used by the planner
// to understand what table this function is operating on.
BindInfo CUBITIndexCountFunction::CUBITIndexCountGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    if (!bind_data_p) {
        // This should ideally not happen if the optimizer path correctly sets up bind_data.
        // If called directly from SQL without proper bind_data setup, this would be an issue.
        throw InternalException("CUBITIndexCountGetBindInfo called without bind_data. Optimizer should set this.");
    }
    auto &bind_data = bind_data_p->Cast<CUBITIndexCountBindData>();
    
    // Return BindInfo associated with the table being scanned.
    // This helps DuckDB understand which table's data this function provides.
    // We need to get the table_index for the BindInfo.
    // If the DuckTableEntry doesn't directly provide it, we might need to rethink
    // how BindInfo is constructed or what it signifies here.
    // For now, constructing it with the table alias or name.
    // A proper TableBinding might be needed if this interacts deeply with other parts of the planner.
    // However, for a custom table function replacing a scan, providing the table itself is key.
    return BindInfo(bind_data.table); // This constructor expects a TableCatalogEntry&
}


TableFunction CUBITIndexCountFunction::GetFunction() {
    TableFunction func("cubit_index_count");
    func.arguments = {}; // No SQL arguments expected for optimizer-driven use.
    
    func.bind = CUBITIndexCountBind;
    func.init_global = CUBITIndexCountInitGlobal;
    func.function = CUBITIndexCountExecute;
    func.get_bind_info = CUBITIndexCountGetBindInfo;

    func.projection_pushdown = false;
    func.filter_pushdown = false;
    // func.filter_prune = true; // If applicable, allows filter pruning

    // Indicates that this function produces a result that can be cached for parallel execution.
    // For a simple count that's read-only and deterministic, this should be safe.
    func.is_parallelisable = true;


    return func;
}

} // namespace duckdb
