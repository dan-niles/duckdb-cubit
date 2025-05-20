# Things TODO

1. Update is not working 
2. ~~Test delete functionality~~
3. ~~Change tids (Currently using hardcoded 0)~~
4. Support BETWEEN and <,>,<=,>= operators?
5. Do benchmarks
6. Find a way to initialize with dynamic no. of rows, cardinality
7. ~~Add support for indexing string columns~~

# Already Implemented

1. Query method for CUBIT to get row_ids (evaluate() only returns count of matches)
2. Initialize CUBIT with CREATE INDEX
3. INSERT, DELETE support for INTEGER columns
4. Search index when SELECT expressions are called with WHERE clause
5. Added support for indexing VARCHAR columns in CUBIT (encode values to int)
6. Reset method to deallocate the index when CommitDrop is invoked