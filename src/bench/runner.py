"""Benchmark orchestration: data -> independent reference -> warm-up + measured rounds -> per-run validation."""
from __future__ import annotations

import csv
import itertools
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from . import envinfo
from .queries import QUERIES, SCHEMA
from .reference import python_reference, sqlite_reference
from .validate import canonical_hash, compare

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MAX_ROWS = 1_000_000
PA_TYPES = {"int32": pa.int32(), "int64": pa.int64(), "string": pa.string()}


def load_config(path: Path | None = None) -> dict:
    return json.loads((path or ROOT / "configs" / "benchmark.json").read_text())


def data_dir(scale: str) -> Path:
    return ROOT / "data" / scale


def ensure_data(scale: str, rows: int, seed: int) -> dict:
    d = data_dir(scale)
    m = d / "manifest.json"
    if m.exists():
        man = json.loads(m.read_text())
        if man["rows"] == rows and man["seed"] == seed:
            return man
    sys.path.insert(0, str(ROOT))
    from data_generator.generate import generate
    return generate(rows, d, seed)


def ref_table(query, rows):
    cols = list(zip(*rows)) if rows else [[] for _ in SCHEMA[query]]
    return pa.table({c: pa.array(list(v), type=PA_TYPES[t]) for (c, t), v in zip(SCHEMA[query], cols)})


def ensure_reference(scale: str, out: Path) -> dict:
    """Pure-Python oracle (always) + SQLite cross-check (<= 1M rows). Cached on disk."""
    rdir = out / "reference"
    meta_p = rdir / "reference.json"
    d = data_dir(scale)
    man = json.loads((d / "manifest.json").read_text())
    if meta_p.exists():
        meta = json.loads(meta_p.read_text())
        if meta.get("sales_content_sha256") == man["files"]["sales.parquet"]["content_sha256"]:
            meta["rows"] = {q: [tuple(r) for r in zip(*[c.to_pylist() for c in pq.read_table(rdir / f"{q}.parquet").columns])]
                            for q in QUERIES}
            return meta
    rdir.mkdir(parents=True, exist_ok=True)
    t = time.perf_counter()
    rows, inv = python_reference(d / "sales.parquet", d / "products.parquet")
    meta = {"sales_content_sha256": man["files"]["sales.parquet"]["content_sha256"], "invariants": inv,
            "python_reference_seconds": round(time.perf_counter() - t, 3),
            "hashes": {q: canonical_hash(rows[q]) for q in QUERIES}, "row_counts": {q: len(rows[q]) for q in QUERIES}}
    if man["rows"] <= SQLITE_MAX_ROWS:
        t = time.perf_counter()
        srows = sqlite_reference(d / "sales.parquet", d / "products.parquet")
        meta["sqlite_seconds"] = round(time.perf_counter() - t, 3)
        meta["sqlite_agrees_with_python"] = {q: canonical_hash(srows[q]) == meta["hashes"][q] for q in QUERIES}
        if not all(meta["sqlite_agrees_with_python"].values()):
            meta_p.write_text(json.dumps(meta, indent=2))
            raise SystemExit(f"Reference implementations disagree: {meta['sqlite_agrees_with_python']}")
    else:
        meta["sqlite_agrees_with_python"] = "not run (scale > %d rows)" % SQLITE_MAX_ROWS
    for q in QUERIES:
        pq.write_table(ref_table(q, rows[q]), rdir / f"{q}.parquet")
    meta_p.write_text(json.dumps(meta, indent=2))
    meta["rows"] = rows
    return meta


# ---- sleep/standby protection (Windows). A run that spans a system suspend is INVALID, never a slow run.
def _unbiased_seconds():
    """Monotonic time EXCLUDING suspend (Windows QueryUnbiasedInterruptTime). None elsewhere."""
    if sys.platform != "win32":
        return None
    import ctypes
    v = ctypes.c_ulonglong()
    ctypes.windll.kernel32.QueryUnbiasedInterruptTime(ctypes.byref(v))
    return v.value / 1e7


def keep_awake(on: bool):
    if sys.platform != "win32":
        return  # Linux/macOS: use `systemd-inhibit` / `caffeinate` (see README)
    import ctypes
    ES_CONTINUOUS, ES_SYSTEM_REQUIRED, ES_DISPLAY_REQUIRED = 0x80000000, 0x00000001, 0x00000002
    flags = ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED if on else ES_CONTINUOUS
    ctypes.windll.kernel32.SetThreadExecutionState(flags)


SUSPEND_TOLERANCE_S = 1.0


def _dir_size(p: Path) -> int:
    total = 0
    for dp, _, files in os.walk(p):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(dp, f))
            except OSError:
                pass
    return total


def _kill_tree(proc: psutil.Process):
    try:
        kids = proc.children(recursive=True)
    except psutil.Error:
        kids = []
    for k in kids + [proc]:
        try:
            k.kill()
        except psutil.Error:
            pass


def measure(cmd, env, log_path: Path, tmp_dir: Path, interval: float, timeout: float) -> dict:
    """Run cmd; sample process-tree RSS / CPU / IO; stdout+stderr go to a log file (no pipe deadlock)."""
    seen = {}  # pid -> (cpu_seconds, read_bytes, write_bytes)
    peak_rss = 0; peak_spill = 0; tick = 0; n_samples = 0; sample_errors = 0
    vm = psutil.virtual_memory(); sw = psutil.swap_memory()
    with open(log_path, "w", encoding="utf-8", errors="replace") as log:
        started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        u0 = _unbiased_seconds()
        t0 = time.perf_counter()
        p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env, cwd=str(ROOT))
        root = psutil.Process(p.pid)
        timed_out = False
        while p.poll() is None:
            try:
                procs = [root] + root.children(recursive=True)
            except (psutil.Error, OSError):  # e.g. WinError 1455 under commit pressure: skip this sample
                procs = []; sample_errors += 1
            rss = 0
            for pr in procs:
                try:
                    with pr.oneshot():
                        rss += pr.memory_info().rss
                        c = pr.cpu_times()
                        try:
                            io = pr.io_counters(); rb, wb = io.read_bytes, io.write_bytes
                        except (psutil.Error, AttributeError):
                            rb = wb = None
                        seen[pr.pid] = (c.user + c.system, rb, wb)
                except (psutil.Error, OSError):
                    pass
            peak_rss = max(peak_rss, rss); n_samples += 1
            if tick % 10 == 0:
                peak_spill = max(peak_spill, _dir_size(tmp_dir))
            tick += 1
            if time.perf_counter() - t0 > timeout:
                timed_out = True; _kill_tree(root); break
            time.sleep(interval)
        rc = p.wait()
        wall = time.perf_counter() - t0
        u1 = _unbiased_seconds()
    suspended = None if u0 is None else max(0.0, wall - (u1 - u0))
    vals = list(seen.values())
    io_ok = vals and all(v[1] is not None for v in vals)
    return {"started_utc": started_utc, "suspended_seconds": suspended, "wall_seconds": wall, "returncode": rc, "timed_out": timed_out,
            "peak_tree_rss_mib": peak_rss / 2**20, "rss_samples": n_samples,
            "cpu_seconds_tree": sum(v[0] for v in vals),
            "io_read_mib_tree": sum(v[1] for v in vals) / 2**20 if io_ok else None,
            "io_write_mib_tree": sum(v[2] for v in vals) / 2**20 if io_ok else None,
            "peak_tmp_spill_mib": peak_spill / 2**20, "n_processes_seen": len(seen),
            "sampler_errors": sample_errors, "ram_available_gib_before": vm.available / 2**30,
            "swap_used_gib_before": sw.used / 2**30}


def engine_cfg(cfg, engine):
    if engine == "duckdb":
        return {"settings": cfg["duckdb"]["settings"]}
    if engine == "spark":
        return {"conf": cfg["spark"]["conf"]}
    return {}


SCRATCH = ROOT / "tmp" / "scratch"


def scratch_env() -> dict:
    """Every temp/spill location (Python, JVM, Spark, DuckDB, Polars) under the project disk, never the OS temp."""
    SCRATCH.mkdir(parents=True, exist_ok=True)
    s = str(SCRATCH)
    return {"TEMP": s, "TMP": s, "TMPDIR": s, "POLARS_TEMP_DIR": s, "SPARK_LOCAL_DIRS": s,
            "JAVA_TOOL_OPTIONS": f"-Djava.io.tmpdir={s}"}


def child_env(cfg, threads):
    env = os.environ.copy()
    env.update(cfg["common_env"])
    env.update(scratch_env())
    env["POLARS_MAX_THREADS"] = str(threads)
    env["PYSPARK_PYTHON"] = env.get("PYSPARK_PYTHON") or sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = env.get("PYSPARK_DRIVER_PYTHON") or sys.executable
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    return env


def schedule(engines, queries, warmup, measured):
    """Round r: queries rotated by r, engine order = r-th permutation (6 perms for 3 engines)."""
    perms = list(itertools.permutations(engines))
    plan = []
    for r in range(warmup + measured):
        qs = list(queries[r % len(queries):]) + list(queries[:r % len(queries)])
        for q in qs:
            for e in perms[r % len(perms)]:
                plan.append({"round": r, "warmup": r < warmup, "query": q, "engine": e})
    return plan


def preflight_memory(min_available_gib: float):
    vm = psutil.virtual_memory()
    if vm.available / 2**30 < min_available_gib:
        raise SystemExit(f"PRE-FLIGHT: only {vm.available/2**30:.1f} GiB RAM available (< {min_available_gib} GiB). "
                         "Close other applications before measuring; results under memory pressure are not comparable.")
    return vm.available / 2**30


def run_scale(scale: str, cfg: dict, engines=None, queries=None, measured=None, warmup=None) -> Path:
    rows = cfg["scales"][scale]
    engines = engines or cfg["engines"]; queries = queries or cfg["queries"]
    measured = cfg["measured_rounds"] if measured is None else measured
    warmup = cfg["warmup_rounds"] if warmup is None else warmup
    threads = cfg["threads"]
    out = ROOT / "results" / scale
    (out / "outputs").mkdir(parents=True, exist_ok=True)
    logs = ROOT / "logs" / "runs" / scale; logs.mkdir(parents=True, exist_ok=True)
    tmp_root = ROOT / "tmp" / scale

    print(f"[{scale}] RAM available: {preflight_memory(cfg.get('min_available_ram_gib', {}).get(scale, 0)):.1f} GiB", flush=True)
    print(f"[{scale}] data ({rows:,} rows)...", flush=True)
    man = ensure_data(scale, rows, cfg["seed"])
    d = data_dir(scale)
    print(f"[{scale}] independent reference...", flush=True)
    ref = ensure_reference(scale, out)
    env_m = envinfo.manifest(d, cfg)
    (out / "environment.json").write_text(json.dumps(env_m, indent=2))
    (out / "dataset_manifest.json").write_text(json.dumps(man, indent=2))

    env = child_env(cfg, threads)
    records, validations = [], []
    kept = set()
    plan = schedule(engines, queries, warmup, measured)
    keep_awake(True)
    try:
        _run_plan(plan, scale, rows, threads, d, out, logs, tmp_root, cfg, env, ref, records, validations, kept)
    finally:
        keep_awake(False)
    _write_outputs(out, scale, rows, engines, queries, ref, records, validations)
    return out


def _run_plan(plan, scale, rows, threads, d, out, logs, tmp_root, cfg, env, ref, records, validations, kept):
    for i, item in enumerate(plan):
        e, q, r = item["engine"], item["query"], item["round"]
        tag = f"{e}_{q}_r{r}"
        tmp = tmp_root / tag
        shutil.rmtree(tmp, ignore_errors=True); tmp.mkdir(parents=True)
        out_file = out / "outputs" / f"{tag}.parquet"
        res_file = tmp_root / f"{tag}.result.json"
        cmd = [sys.executable, "-m", "bench.worker", "--engine", e, "--query", q, "--threads", str(threads),
               "--sales", str(d / "sales.parquet"), "--products", str(d / "products.parquet"),
               "--out", str(out_file), "--result-json", str(res_file), "--tmp-dir", str(tmp),
               "--engine-config", json.dumps(engine_cfg(cfg, e))]
        m = measure(cmd, env, logs / f"{tag}.log", tmp, cfg["sampler_interval_seconds"], cfg["run_timeout_seconds"])
        rec = {"scale": scale, "input_rows": rows, "engine": e, "query": q, "round": r, "warmup": item["warmup"],
               "order_index": i, "threads": threads, **m}
        w = json.loads(res_file.read_text()) if res_file.exists() else {"ok": False, "error": "no result json (crash/timeout)"}
        for k in ("t_import", "t_init", "t_register", "t_query", "t_write", "t_close", "t_worker_total",
                  "output_rows", "output_bytes", "error"):
            rec[k] = w.get(k)
        if m["suspended_seconds"] and m["suspended_seconds"] > SUSPEND_TOLERANCE_S:
            v = {"status": "INVALID_SUSPEND", "checks": {}, "detail": {"suspended_seconds": m["suspended_seconds"]}}
            rec["status"] = "INVALID_SUSPEND"
        elif w.get("ok") and m["returncode"] == 0:
            rec["t_engine_work"] = rec["t_register"] + rec["t_query"]
            tbl = pq.read_table(out_file)
            v = compare(q, tbl, ref["rows"][q], ref["invariants"], ref["hashes"][q])
            rec["status"] = v["status"]
        else:
            v = {"status": "ERROR", "checks": {}, "detail": {"error": w.get("error"), "traceback": w.get("traceback")}}
            rec["status"] = "ERROR"
        validations.append({"engine": e, "query": q, "round": r, "warmup": item["warmup"], **v})
        # keep the first measured output per engine/query + every failing one; delete the rest (disk)
        key = (e, q)
        if out_file.exists():
            if rec["status"] == "PASS" and (key in kept or item["warmup"]):
                out_file.unlink()
            elif rec["status"] == "PASS":
                kept.add(key)
        shutil.rmtree(tmp, ignore_errors=True)
        records.append(rec)
        print(f"[{scale}] {i+1}/{len(plan)} {tag:<18} {rec['status']:<5} wall={m['wall_seconds']:.3f}s "
              f"work={rec.get('t_engine_work') or float('nan'):.4f}s rss={m['peak_tree_rss_mib']:.0f}MiB", flush=True)
        # incremental raw log: survives an interrupted scale
        with open(out / "runs.partial.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, default=str) + "\n")


def _write_outputs(out, scale, rows, engines, queries, ref, records, validations):
    fields = list(dict.fromkeys(k for r in records for k in r))
    with open(out / "runs.csv", "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=fields); wr.writeheader(); wr.writerows(records)
    summary = {}
    for e in engines:
        for q in queries:
            vs = [v for v in validations if v["engine"] == e and v["query"] == q]
            bad = [v for v in vs if v["status"] in ("FAIL", "ERROR")]
            invalid = sum(v["status"] == "INVALID_SUSPEND" for v in vs)
            measured_pass = sum(v["status"] == "PASS" and not v["warmup"] for v in vs)
            status = "FAIL" if bad or not vs else "PASS"
            summary[f"{e}/{q}"] = {"status": status, "runs": len(vs),
                                   "pass_runs": sum(v["status"] == "PASS" for v in vs),
                                   "measured_pass_runs": measured_pass, "invalid_suspend_runs": invalid}
    report = {"scale": scale, "input_rows": rows,
              "reference": {k: v for k, v in ref.items() if k != "rows"},
              "summary": summary, "all_pass": all(s["status"] == "PASS" for s in summary.values()),
              "runs": validations}
    (out / "validation.json").write_text(json.dumps(report, indent=2, default=str))
    print(f"[{scale}] all_pass={report['all_pass']}", flush=True)
    return out
