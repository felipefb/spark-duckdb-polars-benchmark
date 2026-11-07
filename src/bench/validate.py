"""Correctness checks: schema, row count, key set, key uniqueness, canonical hash, exact values, invariants."""
from __future__ import annotations

import hashlib
from collections import Counter

import pyarrow as pa

from .queries import KEYS, SCHEMA

LOGICAL = {
    pa.int32(): "int32", pa.int64(): "int64", pa.string(): "string", pa.large_string(): "string",
    pa.string_view(): "string",
}


def logical_schema(table: pa.Table):
    return [(f.name, LOGICAL.get(f.type, str(f.type))) for f in table.schema]


def physical_schema(table: pa.Table):
    return [(f.name, str(f.type)) for f in table.schema]


def _sort_key(row):
    # NULL-safe total order; type tag avoids comparing int with str.
    return tuple((0, "") if v is None else (1, v) for v in row)


def canonical_rows(rows):
    return sorted((tuple(r) for r in rows), key=_sort_key)


def canonical_hash(rows) -> str:
    h = hashlib.sha256()
    for r in canonical_rows(rows):
        h.update(repr(r).encode()); h.update(b"\n")
    return h.hexdigest()


def table_rows(table: pa.Table):
    return list(zip(*[table.column(i).to_pylist() for i in range(table.num_columns)])) if table.num_rows else []


def check_invariants(query: str, rows, inv: dict) -> dict:
    cols = [c for c, _ in SCHEMA[query]]
    ix = {c: i for i, c in enumerate(cols)}
    res = {}
    if query == "q0":
        res["n_rows_equals_input"] = bool(rows) and rows[0][ix["n_rows"]] == inv["total_rows"]
        res["sum_amount_equals_input"] = bool(rows) and (rows[0][ix["sum_amount"]] or 0) == inv["sum_amount_all"]
    elif query == "q1":
        res["sum_n_equals_filtered_rows"] = sum(r[ix["n"]] for r in rows) == inv["q1_filtered_rows"]
    elif query == "q2":
        res["join_reconciliation(matched+unmatched=noncancelled)"] = (
            sum(r[ix["n"]] for r in rows) + inv["q2_unmatched_rows"] == inv["noncancelled_rows"])
    elif query == "q3":
        res["one_row_per_customer"] = len(rows) == inv["distinct_customers_incl_null"]
    elif query == "q4":
        res["sum_n_equals_input_rows"] = sum(r[ix["n"]] for r in rows) == inv["total_rows"]
        res["sum_total_equals_input_amount"] = (
            sum(r[ix["total_cents"]] for r in rows if r[ix["total_cents"]] is not None) == inv["sum_amount_all"])
        res["groups_equal_distinct_customers"] = len(rows) == inv["distinct_customers_incl_null"]
    return res


def compare(query: str, table: pa.Table, ref_rows, inv: dict | None = None, ref_hash: str | None = None,
            max_examples: int = 5) -> dict:
    expected_schema = [list(x) for x in SCHEMA[query]]
    got_schema = [list(x) for x in logical_schema(table)]
    rows = table_rows(table)
    checks = {"schema": got_schema == expected_schema, "row_count": len(rows) == len(ref_rows)}
    cols = [c for c, _ in SCHEMA[query]]
    if checks["schema"] and KEYS[query]:
        kix = [cols.index(k) for k in KEYS[query]]
        keys = Counter(tuple(r[i] for i in kix) for r in rows)
        ref_keys = {tuple(r[i] for i in kix) for r in ref_rows}
        checks["keys_unique"] = all(v == 1 for v in keys.values())
        checks["key_set"] = set(keys) == ref_keys
    got_hash = canonical_hash(rows)
    ref_hash = ref_hash or canonical_hash(ref_rows)
    checks["canonical_hash"] = got_hash == ref_hash
    checks["exact_values"] = checks["canonical_hash"]  # hash is over exact canonical values
    detail = {"rows": len(rows), "ref_rows": len(ref_rows), "hash": got_hash, "ref_hash": ref_hash,
              "logical_schema": got_schema, "physical_schema": physical_schema(table)}
    if not checks["canonical_hash"]:
        got_c, ref_c = Counter(map(tuple, rows)), Counter(map(tuple, ref_rows))
        missing = list((ref_c - got_c).elements())[:max_examples]
        extra = list((got_c - ref_c).elements())[:max_examples]
        detail["missing_examples(expected_not_returned)"] = [list(r) for r in missing]
        detail["extra_examples(returned_not_expected)"] = [list(r) for r in extra]
    if inv is not None and checks["schema"]:
        for k, v in check_invariants(query, rows, inv).items():
            checks["inv:" + k] = bool(v)
    status = "PASS" if all(checks.values()) else "FAIL"
    return {"status": status, "checks": checks, "detail": detail}
