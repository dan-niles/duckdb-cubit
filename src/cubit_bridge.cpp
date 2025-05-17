#include "cubit_bridge.hpp"

#include <sstream>
#include <memory>
#include <mutex>

#include "cubit/table_lf.h"
using namespace cubit_lf;

static std::unique_ptr<cubit_lf::CubitLF> cubit_table;
static std::once_flag init_flag;

Table_config GenerateTableConfig() {
    auto config = new Table_config{};

    // Simulate command line options:
    config->n_workers = 3;
    config->g_cardinality = 5;
    config->DATA_PATH = "/home/danniles/cubit/data";
    config->INDEX_PATH = "/home/danniles/cubit/index";
    config->n_rows = 1;
    config->n_udis = 100;
    config->n_queries = 100;
    config->verbose = false;
    config->enable_fence_pointer = false;
    config->enable_parallel_cnt = false;
    config->show_memory = false;
    config->on_disk = false;
    config->approach = "cubit-lf";
    config->nThreads_for_getval = 4;          // Must be > 0
    config->time_out = 1000;
    config->n_range = 1;                       // Must be >= 1
    config->range_algo = "naive";
    config->showEB = false;
    config->decode = false;
    config->autoCommit = true;
    config->n_merge_threshold = 10;
    config->db_control = false;
    config->n_workers_per_merge_th = 1;

    // Encoding: default EE, or set to AE/RE as you want
    config->encoding = EE;

    // AE margins and intervals will need to be set explicitly after this
    config->AE_left_margin = 0;
    config->AE_right_margin = 0;
    config->AE_interval = 0;
    config->AE_anchors = {};

    config->segmented_btv = true;
    config->encoded_word_len = 31;
    config->rows_per_seg = 1000;

    return *config;
}

void InitCubitTable() {
    auto config = GenerateTableConfig();
    cubit_table = std::make_unique<cubit_lf::CubitLF>(config);
}

std::string RunCubitQuery(const std::string &query) {
    std::call_once(init_flag, InitCubitTable);

    std::istringstream ss(query);
    std::string cmd;
    ss >> cmd;

    std::ostringstream result;

    int tid = 0; // Assuming single-threaded for simplicity

	if (cmd == "append") {
		int value;
		ss >> value;
		if (cubit_table->append(tid, value) == 0) {
			result << "Append successful";
		} else {
			result << "Append failed";
		}

    } else if (cmd == "update") {
        uint64_t rowID;
        int value;
        ss >> rowID >> value;
        if (cubit_table->update(tid, rowID, value) == 0) {
            result << "Update successful";
        } else {
            result << "Update failed";
        }

    } else if (cmd == "remove") {
        uint64_t rowID;
        ss >> rowID;
        if (cubit_table->remove(tid, rowID) == 0) {
            result << "Remove successful";
        } else {
            result << "Remove failed";
        }

	} else if (cmd == "evaluate") {
		uint32_t value;
		ss >> value;
		int matches = cubit_table->evaluate(tid, value);
		result << "Matches: " << matches;

	} else if (cmd == "range") {
		uint32_t low, high;
		ss >> low >> high;
		int matches = cubit_table->range(tid, low, high);
		result << "Range matches: " << matches;

    } else if (cmd == "get_g_timestamp") {
        uint64_t timestamp = cubit_table->get_g_timestamp();
        result << "Global timestamp: " << timestamp;

    } else if (cmd == "get_g_number_of_rows") {
        uint64_t rows = cubit_table->get_g_number_of_rows();
        result << "Global number of rows: " << rows;
    
	} else {
		result << "Unknown command: " << cmd;
	}

	return result.str();
}
