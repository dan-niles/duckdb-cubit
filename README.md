# CUBIT Extension for DucKDB

This extension, allows you to create Concurrently Updatable Bitmap (CUBIT) indices in duckdb.

## Building

### Build steps

Prerequisites:
- [ccache](https://ccache.dev/)
- [ninja](https://ninja-build.org/)
- [Boost](https://www.boost.org/) (filesystem, system, program_options)
- [Userspace RCU](https://liburcu.org/) (liburcu)

```sh
sudo apt update
sudo apt install -y \
    libboost-filesystem-dev \
    libboost-program-options-dev \
    libboost-system-dev \
    liburcu-dev \
    cmake \
    build-essential
```

Clone the repo:
```sh
git clone --recurse-submodules https://github.com/dan-niles/duckdb-cubit.git
```

Now to build the extension, run:
```sh
GEN=ninja make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/cubit/cubit.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `cubit.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a scalar function `cubit_query()` that takes a string arguments and returns a string:
```sql
D select cubit_query('append 1') as result;
┌───────────────────┐
│      result       │
│      varchar      │
├───────────────────┤
│ Append successful │
└───────────────────┘

D select cubit_query('evaluate 1') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Matches: 1    │
└───────────────┘
```

This is used just for testing & debugging the functionality of the index. Refer to the section below for using CUBIT with an actual table. Check `src/cubit_bridge.cpp` for the available options.

### Using CUBIT in a table

Initializing CUBIT on an `INTEGER` column:
```sql
CREATE TABLE t(i INTEGER);
INSERT INTO t VALUES (1), (1), (2), (2), (3);
CREATE INDEX cubit_idx_int ON t USING CUBIT(i);
SELECT rowid, i FROM t WHERE i=2;
DELETE FROM t WHERE i=2;
```

Initializing CUBIT on a `VARCHAR` column:
```sql
CREATE TABLE v (id INTEGER, j VARCHAR);
INSERT INTO v VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'carol'), (5, 'bob');
CREATE INDEX cubit_idx_varchar ON v USING CUBIT(j);
SELECT * FROM v WHERE j = 'bob';
DELETE FROM v WHERE j = 'bob';
```

Explain a SQL statement:
```sql
EXPLAIN SELECT rowid, i FROM t WHERE i = 3;
```

Show all indexes initialized for a particular table:
```sql
SELECT * FROM duckdb_indexes WHERE table_name = 't';
```

Drop the indexes:
```sql
DROP INDEX cubit_idx_int;
DROP INDEX cubit_idx_varchar;
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

## Acknowledgements

CUBIT Source Code: https://github.com/junchangwang/CUBIT

Wang, Junchang, and Manos Athanassoulis. "CUBIT: Concurrent Updatable Bitmap Indexing (Extended Version)." arXiv preprint arXiv:2410.16929 (2024).
