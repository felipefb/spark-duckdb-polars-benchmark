"""Blocking correctness tests. Run: python -m pytest   (see README)."""
import pyarrow as pa
import pytest

import fixtures as fx
from bench.queries import QUERIES, SCHEMA
from bench.reference import python_reference, sqlite_reference
from bench.validate import canonical_hash, canonical_rows, compare

ENGINE_NAMES = ["duckdb", "polars", "spark"]


# --- 1. the independent references agree with hand-written expectations ------------------------
@pytest.mark.parametrize("query", QUERIES)
def test_python_reference_matches_handwritten(datasets, query):
    rows, _ = python_reference(*datasets["edge"])
    assert canonical_rows(rows[query]) == canonical_rows(fx.EXPECTED_EDGE[query])


@pytest.mark.parametrize("ds", ["edge", "empty", "cardinality"])
def test_sqlite_matches_python_reference(datasets, ds):
    py, _ = python_reference(*datasets[ds])
    sq = sqlite_reference(*datasets[ds])
    for q in QUERIES:
        assert canonical_hash(sq[q]) == canonical_hash(py[q]), q


def test_empty_expectations(datasets):
    rows, _ = python_reference(*datasets["empty"])
    for q, exp in fx.EXPECTED_EMPTY.items():
        assert rows[q] == exp


# --- 2. every engine matches the reference on every fixture --------------------------------------
@pytest.mark.parametrize("engine", ENGINE_NAMES)
@pytest.mark.parametrize("ds", ["edge", "empty", "cardinality"])
@pytest.mark.parametrize("query", QUERIES)
def test_engine_matches_reference(engines, datasets, engine, ds, query):
    if engine not in engines:
        pytest.skip("BLOCKED: Java not available for Spark")
    sales, products = datasets[ds]
    ref, inv = python_reference(sales, products)
    eng = engines[engine]
    eng.register(sales, products)
    v = compare(query, eng.run(query), ref[query], inv)
    assert v["status"] == "PASS", v


# --- 3. the validator really detects errors (mutation tests) -------------------------------------
def _ref_table(query, rows):
    from bench.runner import ref_table
    return ref_table(query, rows)


def test_validator_detects_value_change():
    rows = list(fx.EXPECTED_EDGE["q4"]); bad = list(rows); bad[0] = (10, 3, 1201)
    assert compare("q4", _ref_table("q4", bad), rows)["status"] == "FAIL"


def test_validator_detects_missing_row():
    rows = fx.EXPECTED_EDGE["q3"]
    assert compare("q3", _ref_table("q3", rows[1:]), rows)["status"] == "FAIL"


def test_validator_detects_null_vs_zero():
    rows = fx.EXPECTED_EDGE["q4"]
    bad = [(c, n, 0 if t is None else t) for c, n, t in rows]
    assert compare("q4", _ref_table("q4", bad), rows)["status"] == "FAIL"


def test_validator_detects_type_change():
    rows = fx.EXPECTED_EDGE["q1"]
    t = _ref_table("q1", rows)
    t = t.set_column(0, "store_id", t.column("store_id").cast(pa.int64()))
    v = compare("q1", t, rows)
    assert v["status"] == "FAIL" and v["checks"]["schema"] is False


def test_validator_detects_duplicate_keys():
    rows = fx.EXPECTED_EDGE["q2"]
    v = compare("q2", _ref_table("q2", rows + [rows[0]]), rows)
    assert v["status"] == "FAIL"


def test_validator_accepts_large_string_physical_type():
    rows = fx.EXPECTED_EDGE["q2"]
    t = _ref_table("q2", rows)
    t = t.set_column(0, "category", t.column("category").cast(pa.large_string()))
    assert compare("q2", t, rows)["status"] == "PASS"


# --- 4. documented defects of the ORIGINAL ZIP logic (these must keep failing) ---------------------
def test_original_polars_sum_of_all_null_returns_zero(datasets):
    """Original Polars Q4 used .sum(): returns 0 where SQL returns NULL (customer 13)."""
    import polars as pl
    s = pl.scan_parquet(str(datasets["edge"][0]))
    x = s.group_by("customer_id").agg(pl.len().alias("n"), pl.col("amount_cents").sum().alias("total_cents"))
    got = {r[0]: r[2] for r in x.collect().iter_rows()}
    assert got[13] == 0  # SQL semantics: NULL


def test_original_polars_q3_null_ordering_differs(datasets):
    """Original Polars Q3 sort (no nulls_last) puts NULL day first under descending -> picks order 15."""
    import polars as pl
    s = pl.scan_parquet(str(datasets["edge"][0]))
    x = (s.sort(["customer_id", "day_id", "order_id"], descending=[False, True, True])
          .unique(subset=["customer_id"], keep="first", maintain_order=True))
    got = {r[0]: r[1] for r in x.select(["customer_id", "order_id"]).collect().iter_rows()}
    assert got[15] == 15  # expected under SQL DESC NULLS LAST: 16


def test_original_q2_invariant_breaks_with_orphans(datasets):
    """Original invariant sum(q2.n) == count(status<>'cancelled') ignores orphan/NULL join keys."""
    rows, inv = python_reference(*datasets["edge"])
    assert sum(r[1] for r in rows["q2"]) != inv["noncancelled_rows"]
    assert sum(r[1] for r in rows["q2"]) + inv["q2_unmatched_rows"] == inv["noncancelled_rows"]


def test_original_polars_len_is_uint32():
    """Original Polars pl.len() yields UInt32 while SQL engines yield INT64 (schema mismatch hidden by CSV)."""
    import polars as pl
    df = pl.DataFrame({"a": [1, 1]}).group_by("a").agg(pl.len().alias("n"))
    assert df.schema["n"] == pl.UInt32


def test_schemas_declared_for_all_queries():
    assert set(SCHEMA) == set(QUERIES)
