"""One engine x one query in a fresh process. Phase timings are written to a JSON file
(never parsed from stdout, which engines/launchers may pollute)."""
from __future__ import annotations

import argparse
import importlib
import json
import time
import traceback
from pathlib import Path

T_PROCESS_PY = time.perf_counter()  # interpreter is up; harness measures pre-interpreter time separately

import pyarrow.parquet as pq  # noqa: E402  (common to every engine, excluded from engine import time)

ENGINE_MODULE = {"duckdb": "duckdb", "polars": "polars", "spark": "pyspark.sql"}


def main():
    ap = argparse.ArgumentParser()
    for a in ("engine", "query", "sales", "products", "out", "result_json", "tmp_dir", "engine_config"):
        ap.add_argument("--" + a.replace("_", "-"), dest=a, required=True)
    ap.add_argument("--threads", type=int, required=True)
    a = ap.parse_args()
    res = {"engine": a.engine, "query": a.query, "ok": False}
    try:
        cfg = json.loads(a.engine_config)
        t = time.perf_counter(); importlib.import_module(ENGINE_MODULE[a.engine])
        from bench.queries import ENGINES
        res["t_import"] = time.perf_counter() - t
        t = time.perf_counter(); eng = ENGINES[a.engine](a.threads, Path(a.tmp_dir), **cfg)
        res["t_init"] = time.perf_counter() - t
        t = time.perf_counter(); eng.register(Path(a.sales), Path(a.products))
        res["t_register"] = time.perf_counter() - t
        t = time.perf_counter(); table = eng.run(a.query)
        res["t_query"] = time.perf_counter() - t          # read + process + materialize to Arrow (not separable for lazy engines)
        t = time.perf_counter(); pq.write_table(table, a.out)
        res["t_write"] = time.perf_counter() - t          # identical pyarrow writer for all engines
        t = time.perf_counter(); eng.close()
        res["t_close"] = time.perf_counter() - t
        res["t_worker_total"] = time.perf_counter() - T_PROCESS_PY
        res["output_rows"] = table.num_rows
        res["output_bytes"] = Path(a.out).stat().st_size
        res["ok"] = True
    except Exception as e:  # recorded as ERROR, never as a slow measurement
        res["error"] = f"{type(e).__name__}: {e}"[:4000]
        res["traceback"] = traceback.format_exc()[-8000:]
    Path(a.result_json).write_text(json.dumps(res))
    raise SystemExit(0 if res["ok"] else 1)


if __name__ == "__main__":
    main()
