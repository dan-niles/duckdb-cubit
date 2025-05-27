```sql
CREATE TABLE t(i INTEGER);

INSERT INTO t
SELECT (i % 10) AS i
FROM range(1000) tbl(i);

CREATE INDEX cubit_idx_int ON t USING CUBIT(i);

EXPLAIN ANALYZE SELECT cubit_count('t', 'i', 5);

EXPLAIN ANALYZE SELECT count(i) FROM t WHERE i = 5;

INSERT INTO t VALUES (10);

EXPLAIN ANALYZE SELECT cubit_count('t', 'i', 10);

EXPLAIN ANALYZE SELECT count(i) FROM t WHERE i = 10;

```
