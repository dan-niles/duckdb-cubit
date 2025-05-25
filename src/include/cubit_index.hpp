#pragma once

#include "cubit/table_lf.h"

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

// class FunctionExpressionMatcher;
// class StorageLock;

struct CUBITIndexStats {
	idx_t max_level;
	idx_t count;
	idx_t capacity;
	idx_t approx_size;
};

class CUBITIndex : public BoundIndex {
public:
    static constexpr const char *TYPE_NAME = "CUBIT";
    using CUBITIndexType = std::unique_ptr<cubit_lf::CubitLF>;

public:
    CUBITIndex(const string &name, IndexConstraintType constraint_type, const vector<column_t> &column_ids,
               TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
               AttachedDatabase &db, const case_insensitive_map_t<Value> &options,
               const IndexStorageInfo &info = IndexStorageInfo(), idx_t estimated_cardinality = 0);

    static PhysicalOperator &CreatePlan(PlanIndexInput &input);

	virtual ~CUBITIndex();

    CUBITIndexType index;

	unordered_map<string, uint32_t> string_to_code;
	vector<string> code_to_string;
	uint32_t next_string_code = 1;

	unique_ptr<IndexScanState> InitializeScan(Expression *expr, idx_t limit, ClientContext &context);
	idx_t Scan(IndexScanState &state, Vector &row_ids);

public:
    //! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	ErrorData Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;

	//! Deletes all data from the index. The lock obtained from InitializeLock must be held
	void CommitDrop(IndexLock &index_lock) override;

	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;

	//! Insert a chunk of entries into the index
	ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	int64_t Count(uint32_t val);

    IndexStorageInfo GetStorageInfo(const case_insensitive_map_t<Value> &options, bool to_wal) override;
    idx_t GetInMemorySize(IndexLock &lock) override;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;

	//! Traverses an HNSWIndex and vacuums the qualifying nodes. The lock obtained from InitializeLock must be held
	void Vacuum(IndexLock &state) override;

    //! Returns the string representation of the CUBITIndex, or only traverses and verifies the index.
	string VerifyAndToString(IndexLock &state, const bool only_verify) override;
	//! Ensures that the node allocation counts match the node counts.
	void VerifyAllocations(IndexLock &state) override;

	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) override {
		return "Constraint violation in CUBIT index";
	}

	bool MatchesColumn(idx_t column_index) const;

	void SetDirty() {
		is_dirty = true;
	}
	void SyncSize() {
		index_size = index ? index->size() : 0;
	}

private:
    bool is_dirty = false;
	// StorageLock rwlock;
	atomic<idx_t> index_size = {0};
    column_t indexed_column;
	unique_ptr<ExpressionMatcher> function_matcher;
	unique_ptr<ExpressionMatcher> MakeFunctionMatcher() const;
};

} // namespace duckdb
