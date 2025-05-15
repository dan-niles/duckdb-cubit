#ifndef NIC_UTIL_H
#define NIC_UTIL_H

#include <string>
#include <iostream>
#include <numeric> // std::accumulate
#include <cmath>   // std::minus
#include <boost/program_options.hpp>
#include <vector>
#include <atomic>
#include <type_traits>

// using namespace std;

// #define VERIFY_RESULTS 1

#define T_WORD uint64_t

namespace po = boost::program_options;

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;


/***********************************/

#define PAGE_SIZE 4096
#define CACHE_LINE_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHE_LINE_SIZE)))
#define DOUBLE_CACHE_ALIGNED __attribute__((aligned(2 * CACHE_LINE_SIZE)))

// Generate 64-bits long timestamp
// by fetching the rdtsc hardware register.
static __inline__ long read_timestamp(void)
{
    unsigned int __a, __d;
    __asm__ __volatile__("rdtsc" : "=a" (__a), "=d"(__d));
    return ((long)__a) | (((long)__d)<<32);
}

/************************************/

#define MM_CST          __ATOMIC_SEQ_CST
#define MM_RELAXED      __ATOMIC_RELAXED
#define MM_ACQUIRE      __ATOMIC_ACQUIRE
#define MM_RELEASE      __ATOMIC_RELEASE

int _CAS2(volatile long * ptr, long * cmp1, long * cmp2, long val1, long val2);

#define CAS2(p, o1, o2, n1, n2) \
  _CAS2((volatile long *) p, (long *) o1, (long *) o2, (long) n1, (long) n2)

/* Borrowed from perfbook */
template <typename T>
inline T ACCESS_ONCE(const T &x) {
    return *(volatile const T *)&x;
}

#define READ_ONCE(x) ([&]() -> std::remove_reference_t<decltype(x)> { return ACCESS_ONCE(x); })()

template <typename T>
inline void WRITE_ONCE(T &x, T val) {
    *(volatile T *)&x = val;
}

#define barrier() std::atomic_thread_fence(std::memory_order_seq_cst)

template <typename T>
inline T* cmpxchg(T** ptr, T* old_val, T* new_val) {
    T* expected = old_val;
    __atomic_compare_exchange_n(ptr, &expected, new_val, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    return expected;
}
/***********************************/

enum Index_encoding {EE, RE, AE, IE};

class Table_config {
    public:
        int32_t g_cardinality;
        Index_encoding encoding;
        std::string DATA_PATH;
        std::string INDEX_PATH;
        uint32_t n_workers;
        uint32_t n_udis;
        uint32_t n_queries;
        uint64_t n_rows;
        bool verbose;
        bool enable_fence_pointer;
        bool enable_parallel_cnt;
        bool show_memory;
        bool on_disk;
        std::string approach;
        unsigned int nThreads_for_getval;
        unsigned int time_out;
        uint32_t n_range;
        std::string range_algo;
        bool showEB;
        bool decode;
        bool autoCommit;
        int n_merge_threshold;
        bool db_control;
        uint32_t n_workers_per_merge_th;

        // For Anchor Encoding only.
        int AE_left_margin;
        int AE_right_margin;
        int AE_interval;
        std::map<int, int> AE_margins;
        std::map<int, int> AE_anchors;
        std::map<int, int> AE_curves;

        // For SegBtv only.
        bool segmented_btv;
        uint32_t encoded_word_len;
        uint64_t rows_per_seg;
};

/***********************************/

double time_diff(struct timeval x , struct timeval y);
po::variables_map get_options(const int argc, const char *argv[]);
void init_config(Table_config *config, po::variables_map options);
long rdtsc_diff(long before, long after);

extern struct ThreadInfo g_ths_info[];
extern bool enable_fence_pointer;

#endif
