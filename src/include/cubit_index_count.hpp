#pragma once

#include "duckdb/function/table_function.hpp"
#include "cubit_index.hpp" // For CUBITIndex
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp" // For DuckTableEntry
#include "duckdb/planner/table_binding.hpp" // For BindInfo

namespace duckdb {

struct CUBITIndexCountBindData : public TableFunctionData {
    CUBITIndexCountBindData(DuckTableEntry &table, CUBITIndex &index, uint32_t search_value)
        : table(table), index(index), search_value(search_value) {}

    DuckTableEntry &table;
    CUBITIndex &index;
    uint32_t search_value;

    unique_ptr<TableFunctionData> Copy() const override {
        // Ensure a const_cast is not accidentally hiding a deeper issue if table/index are not truly const-safe for copying.
        // However, for typical references in bind data, this pattern is common.
        return make_uniq<CUBITIndexCountBindData>(const_cast<DuckTableEntry&>(table), const_cast<CUBITIndex&>(index), search_value);
    }

    bool Equals(const TableFunctionData &other_p) const override {
        auto &other = other_p.Cast<CUBITIndexCountBindData>();
        return &table == &other.table && &index == &other.index && search_value == other.search_value;
    }
};

struct CUBITIndexCountFunction {
    static TableFunction GetFunction();

private:
    static unique_ptr<FunctionData> CUBITIndexCountBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> CUBITIndexCountInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input);
    static void CUBITIndexCountExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static BindInfo CUBITIndexCountGetBindInfo(const optional_ptr<FunctionData> bind_data_p);
};

} // namespace duckdb
