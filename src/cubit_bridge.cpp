#include "cubit_bridge.hpp"

#include <sstream>
#include <memory>
#include <mutex>

#include "cubit/table_lf.h"
using namespace cubit_lf;

// Optionally make this static/global if you want to reuse it across calls
static std::unique_ptr<cubit_lf::CubitLF> cubit_table;
static std::once_flag init_flag;

// Initialize only once for all DuckDB queries
void InitCubitTable() {
    auto config = new Table_config{};

    // Simulate command line options:
    config->n_workers = 1;
    config->DATA_PATH = "/home/danniles/cubit/data";
    config->g_cardinality = 5;
    config->INDEX_PATH = "/home/danniles/cubit/index";
    config->n_rows = 10000;
    config->n_udis = 1;
    config->n_queries = 1;
    config->verbose = false;
    config->enable_fence_pointer = false;
    config->enable_parallel_cnt = false;
    config->show_memory = false;
    config->on_disk = false;
    config->approach = "cubit-lf";
    config->nThreads_for_getval = 4;          // Must be > 0
    config->time_out = 1000;
    config->n_range = 1;                       // Must be >= 1
    config->range_algo = "default";
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

    // Finally create Cubit with fully initialized config
    cubit_table = std::make_unique<cubit_lf::CubitLF>(config);
    // cubit_table->init_sequential_test();
}

// Simple parser for demo: "evaluate 0 123" → evaluate(bitmap_id=0, value=123)
std::string RunCubitQuery(const std::string &query) {
    std::call_once(init_flag, InitCubitTable);

    std::istringstream ss(query);
    std::string cmd;
    ss >> cmd;

    std::ostringstream result;

    if (cmd == "append") {
        int bitmap_id, value;
        ss >> bitmap_id >> value;
        if (cubit_table->append(bitmap_id, value) == 0) {
            result << "Append successful";
        } else {
            result << "Append failed";
        }
    } else if (cmd == "evaluate") {
        int bitmap_id;
        uint32_t value;
        ss >> bitmap_id >> value;
        int matches = cubit_table->evaluate(bitmap_id, value);
        result << "Matches: " << matches;
    } else if (cmd == "range") {
        int bitmap_id;
        uint32_t low, high;
        ss >> bitmap_id >> low >> high;
        int matches = cubit_table->range(bitmap_id, low, high);
        result << "Range matches: " << matches;
    } else {
        result << "Unknown command: " << cmd;
    }

    return result.str();
}
