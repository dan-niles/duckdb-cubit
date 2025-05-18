# Things TODO

1. Update is not working 
2. ~~Test delete functionality~~
3. Change tids (Currently using hardcoded 0)
4. Support BETWEEN and <,>,<=,>= operators?
5. Do benchmarks
6. Find a way to initialize with dynamic no. of rows, cardinality
7. Add support for indexing string columns
    - Internally hash or dictionary-encode string values to integers in `CUBITIndex::Append` and `CUBITIndex::Scan`.
    - Store a map like unordered_map<string, uint32_t> for category-to-index.
    - During scans, map the filter string to the integer key before querying.