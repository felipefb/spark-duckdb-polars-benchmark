# Spark vs DuckDB vs Polars — reproducible local benchmark

Inspired by Jacob Verhoeks' Nov 3–4, 2026 benchmark and correctness audit:
- https://jacob.verhoeks.org/blog/150m-rows-under-a-45-gib-cap-sqe-next-to-duckdb-polars-and-spark-2l1b/
- https://jacob.verhoeks.org/blog/2026-11-04-half-the-bugs-were-in-the-benchmark/

## Setup
Python 3.11+, Java 17+ (Spark), 8+ GB RAM recommended. Use a fresh virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python benchmark.py generate --rows 1000000
python benchmark.py run --repeats 3 --threads 4
```

For a smoke test: `python benchmark.py generate --rows 10000` then `python benchmark.py run --repeats 1 --threads 2`. For larger data: `--rows 10000000` (requires more disk/RAM).

## Methodology
- Identical deterministic Parquet input, integer-cent currency, same four business operations.
- One fresh subprocess per query/engine/repetition; rotate engine order; output materialized to CSV.
- Time `wall_seconds` includes startup, read, execution and CSV write. `seconds` inside worker includes initialization, read and compute, not CSV write. Compare **wall_seconds** for end-to-end and **seconds** for engine execution; Spark startup is included.
- Process-tree peak RSS sampled every 50 ms is an approximation, not a hard memory limit or full accounting of off-heap allocations. All engines are configured for four worker threads where supported; Spark is local mode, not a distributed cluster.
- CSV outputs compared as sorted exact strings; exact integer money avoids floating-point tolerances. Independent invariants include source count, unique customers, group count sum and join row count.
- The `validation.json` `all_pass` must be true before interpreting timings. Any failed run must be excluded and investigated, not treated as a slow measurement.
- `results/runs.csv` contains raw runs. Compute medians per engine/query using only successful runs; do not average across heterogeneous queries.

## Caveats
- This is a *new simplified reproduction*, not the original 150M-row benchmark; the queries and generated data differ. It is not a certified TPC-DS/TPCx-BB benchmark.
- No hard memory cap, warm/cold cache control, CPU isolation, confidence intervals, cost accounting or distributed Spark cluster is provided.
- DuckDB is the cross-engine reference, but mathematical invariants reduce (do not eliminate) correlated-error risk. Production-quality benchmarking should include independent reference implementations, NULL/NaN/duplicate-key edge-case fixtures, repeated independent runs, and output hashes.
- A high-cardinality Q3 output can be large and `collect()` uses memory; start small.
- Dependencies are pinned to a known set for reproducibility; update only after compatibility testing.
