import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT))
_scratch = str(ROOT / "tmp" / "scratch"); os.makedirs(_scratch, exist_ok=True)
for _k in ("TEMP", "TMP", "TMPDIR", "POLARS_TEMP_DIR", "SPARK_LOCAL_DIRS"):
    os.environ[_k] = _scratch
os.environ["JAVA_TOOL_OPTIONS"] = f"-Djava.io.tmpdir={_scratch}"
import tempfile  # noqa: E402
tempfile.tempdir = _scratch
os.environ.setdefault("POLARS_MAX_THREADS", "2")
os.environ.setdefault("TZ", "UTC")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import json  # noqa: E402

import pytest  # noqa: E402

import fixtures as fx  # noqa: E402

CFG = json.loads((ROOT / "configs" / "benchmark.json").read_text())


def java_available():
    return bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="session")
def datasets(tmp_path_factory):
    base = tmp_path_factory.mktemp("fixtures")
    return {
        "edge": fx.write(base / "edge", fx.EDGE_SALES),
        "empty": fx.write(base / "empty", fx.EMPTY_SALES),
        "cardinality": fx.write(base / "card", fx.cardinality_sales()),
    }


@pytest.fixture(scope="session")
def engines(tmp_path_factory):
    from bench.queries import DuckDBEngine, PolarsEngine, SparkEngine
    tmp = tmp_path_factory.mktemp("enginetmp")
    out = {"duckdb": DuckDBEngine(2, tmp, CFG["duckdb"]["settings"]), "polars": PolarsEngine(2, tmp)}
    if java_available():
        conf = dict(CFG["spark"]["conf"]); conf["spark.driver.memory"] = "1g"
        out["spark"] = SparkEngine(2, tmp, conf)
    yield out
    for e in out.values():
        e.close()
