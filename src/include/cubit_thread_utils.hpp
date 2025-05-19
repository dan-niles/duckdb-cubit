#pragma once

#include <thread>
#include <unordered_map>
#include <mutex>

// Returns a unique thread-local integer ID for the current thread
int GetThreadID();