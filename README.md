# CUBIT Extension for DucKDB

This extension, CUBIT, allows you to create concurrently updatable bitmap indices in duckdb.

## Building

### Build steps

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

This is used just for testing & debugging the functionality of the index. Refer to the section below for using CUBIT with an actual table.

### Using CUBIT in a table

```sql
CREATE TABLE t(i INTEGER);

INSERT INTO t VALUES (1), (2), (3), (4), (5);

CREATE INDEX cubit_idx ON t USING CUBIT(i);
```

```sql
EXPLAIN SELECT * FROM t WHERE i = 3;
```

```sql
SELECT * FROM duckdb_indexes WHERE table_name = 't';
```

```sql
DELETE FROM t WHERE i = 2;
INSERT INTO t VALUES (6);

-- See if index still works
SELECT * FROM t WHERE i = 6;

-- Drop the index
DROP INDEX cubit_idx;
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

## Acknowledgements

CUBIT Source Code: https://github.com/junchangwang/CUBIT

Wang, Junchang, and Manos Athanassoulis. "CUBIT: Concurrent Updatable Bitmap Indexing (Extended Version)." arXiv preprint arXiv:2410.16929 (2024).