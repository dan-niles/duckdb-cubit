#include "cubit_bridge.hpp"
#include "cubit/table.h"
#include <sstream>
#include <memory>
#include <mutex>

// Optionally make this static/global if you want to reuse it across calls
static std::unique_ptr<cubit::Cubit> cubit_table;
static std::once_flag init_flag;

// Initialize only once for all DuckDB queries
void InitCubitTable() {
    auto config = new Table_config(); // Adjust or load your config as needed
    cubit_table = std::make_unique<cubit::Cubit>(config);
    cubit_table->init_sequential_test(); // or your own init method
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
