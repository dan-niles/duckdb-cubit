cmake_minimum_required(VERSION 3.5)
project(cubit)

option(test "Build all tests." OFF)

# --- Common include path to access all CUBIT headers
# PUBLIC so downstream consumers (DuckDB extension) can find them
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# --- Platform-specific flags and dependencies
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(CMAKE_CXX_FLAGS "-g -O3 -std=c++11 -fPIC")
    include_directories("/usr/include/x86_64-linux-gnu" "/usr/local/include")
    find_package(Boost REQUIRED COMPONENTS filesystem program_options system)
    add_definitions(-DLINUX)
endif()
find_library(URCU_LIBRARIES NAMES urcu HINTS "/usr/lib/x86_64-linux-gnu" "/usr/local/lib")

# --- Source file lists
set(FASTBIT_FILES
    src/fastbit/bitvector.cpp
    src/fastbit/fileManager.cpp
    src/fastbit/resource.cpp
    src/fastbit/util.cpp
    src/fastbit/array_t.cpp
)
set(NAIVE_SOURCE_FILES src/naive/table.cpp)
set(UCB_SOURCE_FILES src/ucb/table.cpp)
set(UB_SOURCE_FILES src/ub/table.cpp)
set(NBUB_SOURCE_FILES
    src/cubit/table.cpp
    src/cubit/table_lk.cpp
    src/cubit/table_lf.cpp
    src/cubit/test.cpp
)
set(BASE_FILES
    src/nicolas/base_table.cpp
    src/nicolas/tablex.cpp
    src/nicolas/util.cpp
    src/nicolas/segBtv.cpp
    src/nicolas/perf.cpp
)

# --- Build the bitmap static library
add_library(bitmap STATIC
    ${BASE_FILES}
    ${FASTBIT_FILES}
    ${NAIVE_SOURCE_FILES}
    ${UCB_SOURCE_FILES}
    ${UB_SOURCE_FILES}
    ${NBUB_SOURCE_FILES}
)

install(TARGETS bitmap
    EXPORT DuckDBExports
    ARCHIVE DESTINATION lib
)

# Make headers available to link consumers
target_include_directories(bitmap PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${Boost_INCLUDE_DIRS}
)

# Link dependencies
target_link_libraries(bitmap
    pthread atomic ${Boost_LIBRARIES} ${URCU_LIBRARIES}
)

# (Optional) disable building the standalone 'nicolas' executable
# or wrap in 'if (BUILD_EXECUTABLE)' guard
# add_executable(nicolas src/nicolas/nicolas.cpp)
# target_link_libraries(nicolas bitmap pthread atomic ${Boost_LIBRARIES} ${URCU_LIBRARIES})
