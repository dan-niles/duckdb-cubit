#include "cubit_bridge.hpp"

#include <json/json.hpp>
#include <sstream>
#include <memory>
#include <mutex>
#include <fstream>

#include "cubit/table_lf.h"
using namespace cubit_lf;

static std::unique_ptr<cubit_lf::CubitLF> cubit_table;
static std::once_flag init_flag;

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

	} else if (cmd == "query") {
		uint32_t value;
		ss >> value;
		std::vector<uint32_t> matches = cubit_table->query(tid, value);
		result << "Matches: " << matches.size() << " [";
        for (const auto &match : matches) {
            result << match << " ";
        }
        result << "]";
	}
    else if (cmd == "range") {
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
