#include "cubit_index.hpp"
#include "cubit_index_physical_create.hpp"

#include "cubit/table_lf.h"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

PhysicalCreateCUBITIndex::PhysicalCreateCUBITIndex(const vector<LogicalType> &types_p, TableCatalogEntry &table_p,
                                                 const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                                 vector<unique_ptr<Expression>> unbound_expressions,
                                                 idx_t estimated_cardinality)
    // Declare this operators as a EXTENSION operator
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, types_p, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)), unbound_expressions(std::move(unbound_expressions)),
      sorted(false) {

	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//-------------------------------------------------------------
// Global State
//-------------------------------------------------------------
class CreateCUBITIndexGlobalState final : public GlobalSinkState {
public:
	CreateCUBITIndexGlobalState(const PhysicalOperator &op_p) : op(op_p) {
	}

	const PhysicalOperator &op;
	//! Global index to be added to the table
	unique_ptr<CUBITIndex> global_index;

	mutex glock;
	unique_ptr<ColumnDataCollection> collection;
	shared_ptr<ClientContext> context;

	// Parallel scan state
	ColumnDataParallelScanState scan_state;

	// Track which phase we're in
	atomic<bool> is_building = {false};
	atomic<idx_t> loaded_count = {0};
	atomic<idx_t> built_count = {0};
};

unique_ptr<GlobalSinkState> PhysicalCreateCUBITIndex::GetGlobalSinkState(ClientContext &context) const {
	auto gstate = make_uniq<CreateCUBITIndexGlobalState>(*this);

	vector<LogicalType> data_types = {unbound_expressions[0]->return_type, LogicalType::ROW_TYPE};
	gstate->collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), data_types);
	gstate->context = context.shared_from_this();

	// Create the index
	auto &storage = table.GetStorage();
	auto &table_manager = TableIOManager::Get(storage);
	auto &constraint_type = info->constraint_type;
	auto &db = storage.db;
	gstate->global_index =
	    make_uniq<CUBITIndex>(info->index_name, constraint_type, storage_ids, table_manager, unbound_expressions, db,
	                         info->options, IndexStorageInfo(), estimated_cardinality);

	return std::move(gstate);
}

//-------------------------------------------------------------
// Local State
//-------------------------------------------------------------
class CreateCUBITIndexLocalState final : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

unique_ptr<LocalSinkState> PhysicalCreateCUBITIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CreateCUBITIndexLocalState>();

	vector<LogicalType> data_types = {unbound_expressions[0]->return_type, LogicalType::ROW_TYPE};
	state->collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context.client), data_types);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

//-------------------------------------------------------------
// Sink
//-------------------------------------------------------------

SinkResultType PhysicalCreateCUBITIndex::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {

	auto &lstate = input.local_state.Cast<CreateCUBITIndexLocalState>();
	auto &gstate = input.global_state.Cast<CreateCUBITIndexGlobalState>();
	lstate.collection->Append(lstate.append_state, chunk);
	gstate.loaded_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//-------------------------------------------------------------
// Combine
//-------------------------------------------------------------
SinkCombineResultType PhysicalCreateCUBITIndex::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateCUBITIndexGlobalState>();
	auto &lstate = input.local_state.Cast<CreateCUBITIndexLocalState>();

	if (lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = std::move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}

	return SinkCombineResultType::FINISHED;
}

//-------------------------------------------------------------
// Finalize
//-------------------------------------------------------------

class CUBITIndexConstructTask final : public ExecutorTask {
public:
	CUBITIndexConstructTask(shared_ptr<Event> event_p, ClientContext &context, CreateCUBITIndexGlobalState &gstate_p,
	                       size_t thread_id_p, const PhysicalCreateCUBITIndex &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), gstate(gstate_p), thread_id(thread_id_p),
	      local_scan_state() {
		// Initialize the scan chunk
		gstate.collection->InitializeScanChunk(scan_chunk);
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {

		auto &index = gstate.global_index->index;
		auto &scan_state = gstate.scan_state;
		auto &collection = gstate.collection;

		while (collection->Scan(scan_state, local_scan_state, scan_chunk)) {

			const auto count = scan_chunk.size();
			auto &col_vector = scan_chunk.data[0];
			auto &rowid_vec = scan_chunk.data[1];

			UnifiedVectorFormat col_format;
			UnifiedVectorFormat rowid_format;

			col_vector.ToUnifiedFormat(count, col_format);
			rowid_vec.ToUnifiedFormat(count, rowid_format);

			auto data_ptr = UnifiedVectorFormat::GetData<int32_t>(col_format);
			auto row_ptr = UnifiedVectorFormat::GetData<row_t>(rowid_format);

			for (idx_t i = 0; i < count; i++) {
				const auto col_idx = col_format.sel->get_index(i);
				const auto row_idx = rowid_format.sel->get_index(i);

				// Check for NULL values
				if (!col_format.validity.RowIsValid(col_idx) || !rowid_format.validity.RowIsValid(row_idx)) {
					executor.PushError(ErrorData("Invalid data in CUBIT index construction: NULL values not supported"));
					return TaskExecutionResult::TASK_ERROR;
				}

				// Add the row to the index
				const auto value = data_ptr[col_idx];
				const int result = index->append(thread_id, value);

				// Check for errors
				if (result != 0) {
					executor.PushError(ErrorData("Error in CUBIT index construction"));
					return TaskExecutionResult::TASK_ERROR;
				}
			}

			// Update the built count
			gstate.built_count += count;

			if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
				// yield!
				return TaskExecutionResult::TASK_NOT_FINISHED;
			}
		}

		// Finish task!
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	CreateCUBITIndexGlobalState &gstate;
	size_t thread_id;

	DataChunk scan_chunk;
	ColumnDataLocalScanState local_scan_state;
};

class CUBITIndexConstructionEvent final : public BasePipelineEvent {
public:
	CUBITIndexConstructionEvent(const PhysicalCreateCUBITIndex &op_p, CreateCUBITIndexGlobalState &gstate_p,
	                           Pipeline &pipeline_p, CreateIndexInfo &info_p, const vector<column_t> &storage_ids_p,
	                           DuckTableEntry &table_p)
	    : BasePipelineEvent(pipeline_p), op(op_p), gstate(gstate_p), info(info_p), storage_ids(storage_ids_p),
	      table(table_p) {
	}

	const PhysicalCreateCUBITIndex &op;
	CreateCUBITIndexGlobalState &gstate;
	CreateIndexInfo &info;
	const vector<column_t> &storage_ids;
	DuckTableEntry &table;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		// Schedule tasks equal to the number of threads, which will construct the index
		auto &ts = TaskScheduler::GetScheduler(context);
		const auto num_threads = NumericCast<size_t>(ts.NumberOfThreads());

		vector<shared_ptr<Task>> construct_tasks;
		for (size_t tnum = 0; tnum < num_threads; tnum++) {
			construct_tasks.push_back(make_uniq<CUBITIndexConstructTask>(shared_from_this(), context, gstate, tnum, op));
		}
		SetTasks(std::move(construct_tasks));
	}

	void FinishEvent() override {

		// Mark the index as dirty, update its count
		gstate.global_index->SetDirty();
		gstate.global_index->SyncSize();

		auto &storage = table.GetStorage();

		if (!storage.IsRoot()) {
			throw TransactionException("Cannot create index on non-root transaction");
		}

		// Create the index entry in the catalog
		auto &schema = table.schema;
		info.column_ids = storage_ids;

		if (schema.GetEntry(schema.GetCatalogTransaction(*gstate.context), CatalogType::INDEX_ENTRY, info.index_name)) {
			if (info.on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
				throw CatalogException("Index with name \"%s\" already exists", info.index_name);
			}
		}

		const auto index_entry = schema.CreateIndex(schema.GetCatalogTransaction(*gstate.context), info, table).get();
		D_ASSERT(index_entry);
		auto &duck_index = index_entry->Cast<DuckIndexEntry>();
		duck_index.initial_index_size = gstate.global_index->Cast<BoundIndex>().GetInMemorySize();

		// Finally add it to storage
		storage.AddIndex(std::move(gstate.global_index));
	}
};

SinkFinalizeType PhysicalCreateCUBITIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {

	// Get the global collection we've been appending to
	auto &gstate = input.global_state.Cast<CreateCUBITIndexGlobalState>();
	auto &collection = gstate.collection;

	// Move on to the next phase
	gstate.is_building = true;

	// Reserve the index size
	auto &ts = TaskScheduler::GetScheduler(context);
	auto &index = gstate.global_index->index;
	// index.reserve({static_cast<size_t>(collection->Count()), static_cast<size_t>(ts.NumberOfThreads())});

	// Initialize a parallel scan for the index construction
	collection->InitializeScan(gstate.scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);

	// Create a new event that will construct the index
	auto new_event = make_shared_ptr<CUBITIndexConstructionEvent>(*this, gstate, pipeline, *info, storage_ids, table);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

ProgressData PhysicalCreateCUBITIndex::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
                                                      ProgressData source_progress) const {
	// The "source_progress" is not relevant for CREATE INDEX statements
	ProgressData res;

	const auto &state = gstate.Cast<CreateCUBITIndexGlobalState>();
	// First half of the progress is appending to the collection
	if (!state.is_building) {
		res.done = state.loaded_count + 0.0;
		res.total = estimated_cardinality + estimated_cardinality;
	} else {
		res.done = state.loaded_count + state.built_count;
		res.total = state.loaded_count + state.loaded_count;
	}
	return res;
}

} // namespace duckdb