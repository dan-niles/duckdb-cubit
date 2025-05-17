#define DUCKDB_EXTENSION_MAIN
#include "cubit.hpp"

#include "cubit_extension.hpp"
#include "cubit_bridge.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>


namespace duckdb {

inline void CubitQueryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
	    input, result, args.size(),
	    [&](string_t query) {
	        // Call into CUBIT
	        std::string q = query.GetString();
	        std::string response = RunCubitQuery(q);
	        return StringVector::AddString(result, response);
	    });
}

inline void CubitScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Cubit "+name.GetString()+" 🐥");
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto cubit_scalar_function = ScalarFunction("cubit", {LogicalType::VARCHAR}, LogicalType::VARCHAR, CubitScalarFun);
    ExtensionUtil::RegisterFunction(instance, cubit_scalar_function);

	// Register a query function
	auto cubit_query_function = ScalarFunction("cubit_query", {LogicalType::VARCHAR}, LogicalType::VARCHAR, CubitQueryFunction);
	ExtensionUtil::RegisterFunction(instance, cubit_query_function);

	CUBITModule::Register(instance);
}

void CubitExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string CubitExtension::Name() {
	return "cubit";
}

std::string CubitExtension::Version() const {
#ifdef EXT_VERSION_CUBIT
	return EXT_VERSION_CUBIT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void cubit_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::CubitExtension>();
}

DUCKDB_EXTENSION_API const char *cubit_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
