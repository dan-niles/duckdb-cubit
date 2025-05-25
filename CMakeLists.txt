cmake_minimum_required(VERSION 3.5)
project(cubit_extension)

# Use C++17 and position-independent code for shared libs
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# --- Platform-specific dependencies
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    add_definitions(-DLINUX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -fPIC")
    find_package(Boost REQUIRED COMPONENTS filesystem program_options system)
endif()

# RCU library (for CUBIT internals)
find_library(URCU_LIBRARIES NAMES urcu)

# --- Build CUBIT (bitmap) as a subdirectory
add_subdirectory(cubit)

# --- Include paths: expose CUBIT headers to extension
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/cubit/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/include
)

# --- DuckDB extension sources
set(EXTENSION_SOURCES
    src/cubit_extension.cpp
    src/cubit_bridge.cpp
    src/cubit_index.cpp
    src/cubit_index_plan.cpp
    src/cubit_index_physical_create.cpp
    src/cubit_index_scan.cpp
    src/cubit_optimize_scan.cpp
    src/cubit_thread_utils.cpp
    src/cubit_scalar_functions.cpp
)

# --- Use DuckDB-provided macros to build the extension
# These define targets: cubit_extension and cubit_loadable_extension
build_static_extension(cubit ${EXTENSION_SOURCES})
build_loadable_extension(cubit "" ${EXTENSION_SOURCES})

# --- Link against the CUBIT "bitmap" library and its deps
# Use the plain signature (no PRIVATE/PUBLIC keywords) to remain consistent
target_link_libraries(cubit_extension bitmap pthread atomic ${Boost_LIBRARIES} ${URCU_LIBRARIES})
target_link_libraries(cubit_loadable_extension bitmap pthread atomic ${Boost_LIBRARIES} ${URCU_LIBRARIES})

# --- Install only the DuckDB extension targets
install(TARGETS cubit_extension
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
install(TARGETS cubit_loadable_extension
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
)