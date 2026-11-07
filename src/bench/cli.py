"""python -m bench.cli {run,report,env} ...   (PYTHONPATH=src, or use scripts/)"""
from __future__ import annotations

import argparse
import json

from .runner import load_config, run_scale


def main():
    ap = argparse.ArgumentParser(prog="bench")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="generate data (if needed), build reference, run warm-up + measured rounds")
    r.add_argument("--scale", required=True, nargs="+")
    r.add_argument("--engines", nargs="+")
    r.add_argument("--queries", nargs="+")
    r.add_argument("--measured", type=int)
    r.add_argument("--warmup", type=int)
    sub.add_parser("report", help="statistics + charts from results/*/runs.csv")
    sub.add_parser("env", help="print environment manifest")
    a = ap.parse_args()
    import os
    from .runner import scratch_env
    os.environ.update(scratch_env())  # the orchestrator's own temp files also stay on the project disk
    cfg = load_config()
    if a.cmd == "run":
        for s in a.scale:
            run_scale(s, cfg, a.engines, a.queries, a.measured, a.warmup)
    elif a.cmd == "report":
        from .report import main as rep
        rep()
    else:
        from .envinfo import manifest
        print(json.dumps(manifest(config=cfg), indent=2))


if __name__ == "__main__":
    main()
