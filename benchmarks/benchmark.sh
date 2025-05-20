#!/bin/bash

# Path to custom DuckDB build
DUCKDB_PATH="../build/release/duckdb"

# Output result file
RESULT_FILE="benchmark_results.csv"

# Parameters: total rows and cardinality
TOTAL_ROWS=${1:-1000}      # default 100,000 rows
CARDINALITY=${2:-10}        # default 100 unique values

# Prepare result file
echo "Test,IndexType,Cardinality,TotalRows,QueryValue,AccessTime_ms" > "$RESULT_FILE"

# Logging function
log() {
    echo "[$(date +'%F %T')] $1"
}

# Create and populate test data
setup_database() {
    local db_file="$1"

    "$DUCKDB_PATH" "$db_file" <<EOF
DROP TABLE IF EXISTS test;
CREATE TABLE test(i INTEGER);
WITH data AS (
    SELECT (RANDOM() % $CARDINALITY + 1)::INTEGER AS i
    FROM range($TOTAL_ROWS)
)
INSERT INTO test SELECT * FROM data;
EOF
}

benchmark_query() {
    local db_file="$1"
    local index_type="$2"
    local index_stmt="$3"
    local query_val=$((CARDINALITY / 2))

    log "Running benchmark: $index_type"

    # Apply index if needed
    if [ "$index_stmt" != "-- No index" ]; then
        echo "$index_stmt" | "$DUCKDB_PATH" "$db_file"
    fi

    # Run query and time it
    local start_time=$(date +%s%N)
    echo "SELECT COUNT(*) FROM test WHERE i = $query_val;" | "$DUCKDB_PATH" "$db_file" > /dev/null
    local end_time=$(date +%s%N)

    # Calculate duration in ms
    local duration_ns=$((end_time - start_time))
    local access_time_ms=$(echo "scale=3; $duration_ns / 1000000" | bc)

    echo "$index_type,$CARDINALITY,$TOTAL_ROWS,$query_val,$access_time_ms" >> "$RESULT_FILE"
    log "$index_type access time: ${access_time_ms} ms"
}

# Step 1: No Index
log "=== Testing Without Index ==="
DB_FILE="test_noindex.db"
rm -f "$DB_FILE"
setup_database "$DB_FILE"
benchmark_query "$DB_FILE" "NoIndex" "-- No index"
rm -f "$DB_FILE"

# Step 2: CUBIT Index
log "=== Testing CUBIT Index ==="
DB_FILE="test_cubit.db"
rm -f "$DB_FILE"
setup_database "$DB_FILE"
benchmark_query "$DB_FILE" "CUBIT" "CREATE INDEX idx ON test USING CUBIT(i);"
rm -f "$DB_FILE"

# Step 3: Default Index
log "=== Testing Default Index ==="
DB_FILE="test_default.db"
rm -f "$DB_FILE"
setup_database "$DB_FILE"
benchmark_query "$DB_FILE" "Default" "CREATE INDEX idx ON test(i);"
rm -f "$DB_FILE"

log "Benchmark completed. Results saved to $RESULT_FILE"
