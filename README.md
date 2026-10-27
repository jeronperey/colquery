# colquery — a mini columnar query engine in C++17

A from-scratch columnar storage format and vectorized SQL engine (~1,600 lines, no dependencies).

```
SELECT country, MAX(amount) FROM t WHERE ts >= 1700000000000 AND country = 'US' GROUP BY country
        │ parse (lexer + recursive descent)
        ▼
      Query AST ──► plan (name resolution, dictionary rewrite, column pruning, predicate ordering)
        │
        ▼
   Scan ─► Filter ─► Aggregate        1024-row batches, selection vectors, zone-map pushdown
```

## Features

| Area | What's implemented |
|---|---|
| **Format** (`format.h`, `writer.h`, `reader.h`) | Blocked columnar file (64K rows/block), mmap'd reader, footer with schema + per-block metadata |
| **Encodings** | Sorted dictionary for strings (code order == lexical order); per-block choice of RLE vs. frame-of-reference + 1/2/4/8-byte narrowing |
| **Indexes** | Per-block min/max zone maps; 3-way test (skip block / scan / predicate trivially true → skip the filter) |
| **Execution** (`exec.h`) | Vectorized Scan/Filter/Aggregate on 1024-row batches; branchless selection-vector kernels templated per comparison op; direct-array grouping for dictionary columns, hash grouping otherwise |
| **SQL** (`sql.h`) | `SELECT col, COUNT/SUM/MIN/MAX/AVG(...) FROM t [WHERE a op v AND ...] [GROUP BY col]`, `EXPLAIN` |
| **Optimizer** (`plan.h`) | String literals → dictionary codes (folds always-true / always-false predicates using the dictionary), column pruning, predicate reordering, predicate pushdown into scan |
| **Baseline** (`baseline.h`) | Volcano row-at-a-time engine (virtual `next()` per row, virtual expression eval, per-row hash lookup) over an in-memory row store, same plan |

## Build & run

```sh
make && make test
./colquery gen 100000000 data.cq            # synthetic 100M-row event table
./colquery info data.cq
./colquery query data.cq "SELECT country, SUM(amount) FROM t WHERE ts > 1700000500000000 GROUP BY country"
./colquery query data.cq "EXPLAIN SELECT COUNT(*) FROM t WHERE country = 'US'"
./colquery bench data.cq
./colquery import some.csv some.cq          # arbitrary CSV (int / string columns)
```

## Results (Apple M4, 16 GB, single thread, warm page cache, 100M rows)

Data: `ts` (monotonic int64), `country` (50-value skewed string), `status` (long runs), `amount` (uniform noise).

**Compression:** 2,389 MB raw CSV → 715 MB (**3.34× smaller**). Per column: `status` 15 MB (RLE, every block),
`country` 100 MB (dictionary, 1 byte/row), `amount` 200 MB and `ts` 400 MB (frame-of-reference narrowing only).
`ts` and `amount` are incompressible-ish by construction (random deltas / uniform noise), so the overall ratio is
conservative for real-world data with more repetition.

| Query | vectorized + zone maps | vectorized, no zone maps | row-at-a-time | speedup vs row | blocks skipped |
|---|---:|---:|---:|---:|---:|
| full-scan agg | 43 ms | 43 ms | 1163 ms | **27.0×** | 0% |
| group by string | 186 ms | 185 ms | 1192 ms | **6.4×** | 0% |
| filter + group by | 104 ms | 105 ms | 1090 ms | **10.5×** | 0% |
| 1% ts range | 0.4 ms | 65 ms | 1241 ms | 2763× | **99.0%** |
| 10% ts range + `country='US'` | 8.9 ms | 66 ms | 1222 ms | 138× | **90.0%** |

- Vectorization alone (no pruning possible): **6–27×**.
- Zone maps on clustered/selective predicates: **skip 90–99% of blocks**, a further ~7–160× over the vectorized scan.
- All three engines return identical results on every benchmark query (checked on every run).

Raw output: [`bench_results.txt`](bench_results.txt).

## Correctness

`make test` generates a 200,003-row table (partial tail block), round-trips it through CSV import, and checks 27 queries
(empty results, absent dictionary literals, boundary comparisons, every operator, multi-predicate group-bys) across the
vectorized engine (with and without zone maps) and the row engine against an independent brute-force evaluator that
compares raw strings and never touches the planner's dictionary rewrite or zone maps.

## Honest limits / what a "v2" would do

- Single-threaded; no parallel scan across blocks.
- The baseline is an in-memory row store with a Volcano iterator — a fair model of "row-at-a-time", but not a tuned
  row engine. The vectorized numbers read from an mmap'd file in page cache (no cold-disk I/O).
- SQL subset only: conjunctive predicates, one GROUP BY column, no joins/ORDER BY/LIMIT/NULLs.
- Possible extensions: SIMD filter kernels, filter on RLE runs without expanding, multi-threaded block scan,
  late materialization, bloom filters for string equality, cardinality-based predicate ordering.
