"""Independent reference implementations.

1. `python_reference`: plain-Python streaming implementation (dicts and loops). It shares
   NO code with the engines and does not use SQL. This is the primary oracle.
2. `sqlite_reference`: SQLite (stdlib) executing the same SQL text. It is an independent
   engine implementation, used as a second oracle for small/medium scales.

Both also compute independent invariants from the raw input.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pyarrow.parquet as pq

from .queries import SCHEMA, SQL

BATCH = 500_000


def _iter_rows(path: Path, columns):
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=BATCH, columns=columns):
        cols = [batch.column(c).to_pylist() for c in columns]
        yield from zip(*cols)


def _desc_nulls_last(v):
    # Bigger is better; NULL is worst.
    return (0, 0) if v is None else (1, v)


class _Agg:
    __slots__ = ("n", "s", "nn")

    def __init__(self):
        self.n = 0; self.s = 0; self.nn = 0

    def add(self, amount):
        self.n += 1
        if amount is not None:
            self.s += amount; self.nn += 1

    def total(self):
        return self.s if self.nn else None


def python_reference(sales: Path, products: Path) -> tuple[dict, dict]:
    """Returns ({query: list_of_tuples}, invariants)."""
    dim = {}
    for pid, cat in _iter_rows(products, ["product_id", "category"]):
        if pid in dim:
            raise ValueError(f"products.product_id not unique: {pid}")
        dim[pid] = cat

    q0 = {"n": 0, "n_customer": 0, **{k: [0, 0] for k in ("store", "product", "day", "status_len", "amount", "order")}}
    q1, q2, q3, q4 = {}, {}, {}, {}
    inv = {"total_rows": 0, "q1_filtered_rows": 0, "noncancelled_rows": 0, "q2_unmatched_rows": 0,
           "sum_amount_all": 0}

    def acc(key, v):
        if v is not None:
            q0[key][0] += v; q0[key][1] += 1

    cols = ["order_id", "customer_id", "store_id", "product_id", "day_id", "status", "amount_cents"]
    for order_id, customer_id, store_id, product_id, day_id, status, amount in _iter_rows(sales, cols):
        # q0
        q0["n"] += 1
        if customer_id is not None:
            q0["n_customer"] += 1
        acc("store", store_id); acc("product", product_id); acc("day", day_id)
        acc("status_len", None if status is None else len(status)); acc("amount", amount); acc("order", order_id)
        inv["total_rows"] += 1
        if amount is not None:
            inv["sum_amount_all"] += amount
        # q1: NULL comparisons are not true -> row excluded
        if day_id is not None and day_id >= 180 and status is not None and status == "paid":
            q1.setdefault((store_id, status), _Agg()).add(amount)
            inv["q1_filtered_rows"] += 1
        # q2: status <> 'cancelled' is NULL (excluded) when status is NULL; inner join drops NULL/orphan keys
        if status is not None and status != "cancelled":
            inv["noncancelled_rows"] += 1
            if product_id is not None and product_id in dim:
                q2.setdefault(dim[product_id], _Agg()).add(amount)
            else:
                inv["q2_unmatched_rows"] += 1
        # q3: NULL customer_id forms its own partition
        rank = (_desc_nulls_last(day_id), _desc_nulls_last(order_id), _desc_nulls_last(amount))
        best = q3.get(customer_id)
        if best is None or rank > best[0]:
            q3[customer_id] = (rank, (customer_id, order_id, day_id, amount))
        # q4: NULL customer_id is one group
        q4.setdefault(customer_id, _Agg()).add(amount)

    def tot(k):
        return q0[k][0] if q0[k][1] else None

    out = {
        "q0": [(q0["n"], q0["n_customer"], tot("store"), tot("product"), tot("day"), tot("status_len"),
                tot("amount"), tot("order"))],
        "q1": [(k[0], k[1], a.n, a.total()) for k, a in q1.items()],
        "q2": [(k, a.n, a.total()) for k, a in q2.items()],
        "q3": [v[1] for v in q3.values()],
        "q4": [(k, a.n, a.total()) for k, a in q4.items()],
    }
    inv["distinct_customers_incl_null"] = len(q4)
    return out, inv


def sqlite_reference(sales: Path, products: Path) -> dict:
    if sqlite3.sqlite_version_info < (3, 30, 0):
        raise RuntimeError("SQLite >= 3.30 required for NULLS LAST")
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE sales(order_id INTEGER, customer_id INTEGER, store_id INTEGER, product_id INTEGER,"
                " day_id INTEGER, status TEXT, amount_cents INTEGER)")
    con.execute("CREATE TABLE products(product_id INTEGER, category TEXT)")
    cols = ["order_id", "customer_id", "store_id", "product_id", "day_id", "status", "amount_cents"]
    con.executemany("INSERT INTO sales VALUES (?,?,?,?,?,?,?)", _iter_rows(sales, cols))
    con.executemany("INSERT INTO products VALUES (?,?)", _iter_rows(products, ["product_id", "category"]))
    out = {q: [tuple(r) for r in con.execute(sql)] for q, sql in SQL.items()}
    con.close()
    return out


def columns(query):
    return [c for c, _ in SCHEMA[query]]
