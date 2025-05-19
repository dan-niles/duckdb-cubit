#include "cubit.hpp"
#include "cubit_index.hpp"
#include "cubit_bridge.hpp"
#include "cubit_thread_utils.hpp"

#include "cubit/table_lf.h"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <json/json.hpp>
#include <fstream>

namespace duckdb {

Table_config* GenerateTableConfig() {
	std::ifstream config_file("/home/danniles/Projects/duckdb-cubit/cubit/config.json");
	nlohmann::json j;
    config_file >> j;

    auto config = new Table_config{};

    config->n_workers = j["n_workers"];
    config->g_cardinality = j["g_cardinality"];
    config->DATA_PATH = j["DATA_PATH"];
    config->INDEX_PATH = j["INDEX_PATH"];
    config->n_rows = j["n_rows"];
    config->n_udis = j["n_udis"];
    config->n_queries = j["n_queries"];
    config->verbose = j["verbose"];
    config->enable_fence_pointer = j["enable_fence_pointer"];
    config->enable_parallel_cnt = j["enable_parallel_cnt"];
    config->show_memory = j["show_memory"];
    config->on_disk = j["on_disk"];
    config->approach = j["approach"];
    config->nThreads_for_getval = j["nThreads_for_getval"];
    config->time_out = j["time_out"];
    config->n_range = j["n_range"];
    config->range_algo = j["range_algo"];
    config->showEB = j["showEB"];
    config->decode = j["decode"];
    config->autoCommit = j["autoCommit"];
    config->n_merge_threshold = j["n_merge_threshold"];
    config->db_control = j["db_control"];
    config->n_workers_per_merge_th = j["n_workers_per_merge_th"];

    std::string encoding = j["encoding"];
    if (encoding == "EE") config->encoding = EE;
    else if (encoding == "AE") config->encoding = AE;
    else if (encoding == "RE") config->encoding = RE;

    config->AE_left_margin = j["AE_left_margin"];
    config->AE_right_margin = j["AE_right_margin"];
    config->AE_interval = j["AE_interval"];
    config->AE_anchors = {};

    config->segmented_btv = j["segmented_btv"];
    config->encoded_word_len = j["encoded_word_len"];
    config->rows_per_seg = j["rows_per_seg"];

    return config;
}

CUBITIndex::CUBITIndex(const string &name, IndexConstraintType constraint_type, const vector<column_t> &column_ids,
                       TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
                       AttachedDatabase &db, const case_insensitive_map_t<Value> &options,
                       const IndexStorageInfo &info, idx_t estimated_cardinality)
    : BoundIndex(name, TYPE_NAME, constraint_type, column_ids, table_io_manager, unbound_expressions, db) {

	if (index_constraint_type != IndexConstraintType::NONE) {
		throw NotImplementedException("Cubit indexes do not support unique or primary key constraints");
	}

    if (column_ids.size() != 1) {
        throw NotImplementedException("Cubit index only supports single-column indexing");
    }
    indexed_column = column_ids[0];

    index = std::make_unique<cubit_lf::CubitLF>(GenerateTableConfig());

	if (info.IsValid()) {
        throw NotImplementedException("CUBIT index persistence is not implemented yet");
    }

	index_size = index->get_g_number_of_rows();
    function_matcher = MakeFunctionMatcher();
}

CUBITIndex::~CUBITIndex() = default;

// Scan State
struct CUBITIndexScanState : public IndexScanState {
	idx_t current_row = 0;
	idx_t total_rows = 0;
	std::vector<uint32_t> matched_rows;
	uint32_t search_val = 0;
};

unique_ptr<IndexScanState> CUBITIndex::InitializeScan(Expression *expr, idx_t limit, ClientContext &context) {
	auto state = make_uniq<CUBITIndexScanState>();

	if (!expr) {
		return state;
	}

	if (expr->type == ExpressionType::VALUE_CONSTANT) {
		auto &const_expr = expr->Cast<BoundConstantExpression>();
		auto val = const_expr.value;

		if (!val.IsNull()) {
			state->search_val = val.GetValue<uint32_t>();
			state->matched_rows = index->query(GetThreadID(), state->search_val);
			state->total_rows = state->matched_rows.size();
		}
	} else if (expr->type == ExpressionType::COMPARE_EQUAL) {
		// Support fallback (optional)
		auto &comp = expr->Cast<BoundComparisonExpression>();
		if (comp.right && comp.right->IsFoldable()) {
			Value val = ExpressionExecutor::EvaluateScalar(context, *comp.right);
			if (!val.IsNull()) {
				state->search_val = val.GetValue<uint32_t>();
				state->matched_rows = index->query(GetThreadID(), state->search_val);
				state->total_rows = state->matched_rows.size();
			}
		}
	}

	return state;
}

idx_t CUBITIndex::Scan(IndexScanState &state, Vector &row_ids) {
	auto &scan_state = state.Cast<CUBITIndexScanState>();

	idx_t count = 0;
	auto output = FlatVector::GetData<row_t>(row_ids);

	// Copy matched row_ids up to STANDARD_VECTOR_SIZE
	while (scan_state.current_row < scan_state.total_rows && count < STANDARD_VECTOR_SIZE) {
		output[count++] = scan_state.matched_rows[scan_state.current_row++];
	}

	row_ids.SetVectorType(VectorType::FLAT_VECTOR); // Ensure type is set
	return count;
}

void CUBITIndex::CommitDrop(IndexLock &index_lock) {
    // Drop the in-memory CUBIT index
    index->reset();
    index_size = 0;
}

ErrorData CUBITIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	// We assume a single column of INT32 values
	D_ASSERT(input.ColumnCount() == 1);
	auto &col_vec = input.data[0];
	auto &col_type = col_vec.GetType();
	D_ASSERT(col_type.id() == LogicalTypeId::INTEGER || col_type.id() == LogicalTypeId::VARCHAR);

	UnifiedVectorFormat format;
	col_vec.ToUnifiedFormat(input.size(), format);

	for (idx_t i = 0; i < input.size(); ++i) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			continue; // skip nulls
		}

		uint32_t encoded_value;

		if (col_type.id() == LogicalTypeId::INTEGER) {
			auto data = (int32_t *)format.data;
			encoded_value = static_cast<uint32_t>(data[idx]);

		} else if (col_type.id() == LogicalTypeId::VARCHAR) {
			auto data = (string_t *)format.data;
			string s = data[idx].GetString();

			auto it = string_to_code.find(s);
			if (it != string_to_code.end()) {
				encoded_value = it->second;
			} else {
				encoded_value = next_string_code++;
				string_to_code[s] = encoded_value;
				code_to_string.push_back(s);
			}
		} else {
			throw InternalException("Unsupported column type in CUBITIndex::Insert");
		}

		auto result = index->append(GetThreadID(), encoded_value);
		if (result != 0) {
			return ErrorData{ExceptionType::INDEX, "CUBIT Insert failed"};
		}
	}

	return ErrorData{};
}

void CUBITIndex::Delete(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	// Mark this index as dirty so we checkpoint it properly
	is_dirty = true;

	auto count = input.size();
	rowid_vec.Flatten(count);
	auto row_id_data = FlatVector::GetData<row_t>(rowid_vec);

	// For deleting from the index, we need an exclusive lock
	// auto _lock = rwlock.GetExclusiveLock();

	for (idx_t i = 0; i < count; i++) {
		auto row_id = static_cast<uint64_t>(row_id_data[i]);
		auto result = index->remove(GetThreadID(), row_id);
		if (result != 0) {
			// Optional: handle failed removal
			throw InternalException("CUBITIndex::Delete failed to remove row ID: " + std::to_string(row_id));
		}
	}

	// Optional: update index size if supported
	index_size = index->get_g_number_of_rows(); // If supported
}

IndexStorageInfo CUBITIndex::GetStorageInfo(const case_insensitive_map_t<Value> &options, const bool to_wal) {
    IndexStorageInfo info;
    info.name = name;

    // No root block or buffers since we don’t persist
    info.root = block_id_t(); // Set to INVALID_BLOCK if unused
    info.buffers = {};
    info.allocator_infos = {};

    return info;
}

idx_t CUBITIndex::GetInMemorySize(IndexLock &state) {
	return index->g_number_of_rows * sizeof(row_t) + sizeof(CUBITIndex);
}

bool CUBITIndex::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	throw NotImplementedException("CUBITIndex::MergeIndexes() not implemented");
}

void CUBITIndex::Vacuum(IndexLock &state) {
}

string CUBITIndex::VerifyAndToString(IndexLock &state, const bool only_verify) {
	throw NotImplementedException("CUBITIndex::VerifyAndToString() not implemented");
}

void CUBITIndex::VerifyAllocations(IndexLock &state) {
	throw NotImplementedException("CUBITIndex::VerifyAllocations() not implemented");
}

ErrorData CUBITIndex::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// Resolve expressions on the input chunk (e.g., evaluate column expressions)
	ExecuteExpressions(appended_data, expression_result);

	// Insert into the index
	Insert(lock, expression_result, row_identifiers);

	return ErrorData {};
}

unique_ptr<ExpressionMatcher> CUBITIndex::MakeFunctionMatcher() const {
    unordered_set<string> supported_functions = {"=", "between"};

    auto matcher = make_uniq<FunctionExpressionMatcher>();
    matcher->function = make_uniq<ManyFunctionMatcher>(supported_functions);
    matcher->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::BOUND_FUNCTION);
    matcher->policy = SetMatcher::Policy::UNORDERED;

    // LHS: match any type (loose match)
    auto lhs_matcher = make_uniq<ExpressionMatcher>();
    matcher->matchers.push_back(std::move(lhs_matcher));

    // RHS: also match any type (loose match)
    auto rhs_matcher = make_uniq<ExpressionMatcher>();
    matcher->matchers.push_back(std::move(rhs_matcher));

    return matcher;
}

bool CUBITIndex::MatchesColumn(idx_t column_index) const {
    return indexed_column == column_index;
}

//------------------------------------------------------------------------------
// Register Index Type
//------------------------------------------------------------------------------
void CUBITModule::RegisterIndex(DatabaseInstance &db) {

	IndexType index_type;

	index_type.name = CUBITIndex::TYPE_NAME;
	index_type.create_instance = [](CreateIndexInput &input) -> unique_ptr<BoundIndex> {
		auto res = make_uniq<CUBITIndex>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                                input.unbound_expressions, input.db, input.options, input.storage_info);
		return std::move(res);
	};
	index_type.create_plan = CUBITIndex::CreatePlan;

	// Register the index type
	db.config.GetIndexTypes().RegisterIndexType(index_type);
}

} // namespace duckdb