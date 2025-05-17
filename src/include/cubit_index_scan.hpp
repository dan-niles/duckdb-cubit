#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/table_scan.hpp"

namespace duckdb {

class Index;

// This is created by the optimizer rule
struct CUBITIndexScanBindData final : public TableScanBindData {
	explicit CUBITIndexScanBindData(TableCatalogEntry &table, Index &index, idx_t limit, uint32_t search_value)
	    : TableScanBindData(table), index(index), limit(limit), search_value(search_value) {
	}

	//! The index to use
	Index &index;

	//! The limit of the scan
	idx_t limit;

	//! The value to search for (i.e., bitmap lookup key)
	uint32_t search_value;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CUBITIndexScanBindData>();
		return &other.table == &table && search_value == other.search_value;
	}
};

struct CUBITIndexScanFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb
