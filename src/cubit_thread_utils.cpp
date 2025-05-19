#include "cubit_thread_utils.hpp"

int GetThreadID() {
    thread_local int tid = [] {
        static std::mutex mutex;
        static std::unordered_map<std::thread::id, int> thread_id_map;
        static int next_thread_id = 0;

        std::thread::id this_id = std::this_thread::get_id();

        std::lock_guard<std::mutex> lock(mutex);
        int assigned_id = next_thread_id++;
        thread_id_map[this_id] = assigned_id;
        return assigned_id;
    }();

    return tid;
}
