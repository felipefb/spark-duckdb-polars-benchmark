"""Edge-case fixtures with HAND-WRITTEN expected results (defined before running any engine).

Semantics (see docs/SEMANTICS.md):
  * SQL three-valued logic: NULL in a filter predicate => row excluded.
  * COUNT(*) counts rows (incl. NULL amounts); SUM ignores NULL; SUM over zero non-NULL values = NULL.
  * GROUP BY / PARTITION BY treat NULL as one group.
  * INNER JOIN drops NULL and orphan keys.
  * Q3 order: day_id DESC NULLS LAST, order_id DESC NULLS LAST, amount_cents DESC NULLS LAST.
  * String comparison is case-sensitive ('PAID' != 'paid'). No dedup of duplicate rows.
"""
import pyarrow as pa
import pyarrow.parquet as pq

SALES_SCHEMA = pa.schema([
    ("order_id", pa.int64()), ("customer_id", pa.int64()), ("store_id", pa.int32()),
    ("product_id", pa.int64()), ("day_id", pa.int32()), ("status", pa.string()), ("amount_cents", pa.int64()),
])
PRODUCTS_SCHEMA = pa.schema([("product_id", pa.int64()), ("category", pa.string())])

# (order_id, customer_id, store_id, product_id, day_id, status, amount_cents)
EDGE_SALES = [
    (1, 10, 1, 1, 200, "paid", 1000),
    (2, 10, 1, 1, 200, "paid", 500),        # same customer/day as #1 -> tie broken by order_id DESC
    (3, 10, 1, 2, 199, "paid", -300),       # negative (refund)
    (4, 11, 2, 99, 180, "paid", 0),         # orphan product; zero amount; day boundary 180 (included)
    (5, 11, 2, None, 179, "pending", 700),  # NULL product_id; day 179 (excluded by q1)
    (6, None, 3, 1, 364, "paid", 100),      # NULL customer -> its own group; max day
    (7, None, 3, 2, 0, "cancelled", 200),   # min day; cancelled
    (8, 12, 4, 3, 250, None, 900),          # NULL status -> excluded from q1 and q2
    (9, 12, 4, 3, 250, "pending", None),    # NULL amount; product 3 has NULL category
    (10, 13, 5, 2, 300, "paid", None),      # customer 13: all amounts NULL -> SUM NULL (not 0)
    (11, 13, 5, 2, 300, "PAID", None),      # case-sensitive: not 'paid'
    (12, 14, 6, 1, 100, "paid", 50),        # duplicate order_id 12 (x3) -> tie broken by amount DESC
    (12, 14, 6, 1, 100, "paid", 70),
    (12, 14, 6, 1, 100, "paid", 70),        # exact duplicate row: counted, never deduplicated
    (15, 15, 7, 1, None, "paid", 10),       # NULL day_id: loses to any non-NULL day in q3
    (16, 15, 7, 1, 5, "paid", 20),
    (17, 16, 8, 1, None, "paid", 5),        # only row of customer 16 has NULL day
]
EDGE_PRODUCTS = [(1, "cat_a"), (2, "cat_b"), (3, None), (4, "cat_unused")]

EXPECTED_EDGE = {
    "q0": [(17, 15, 71, 122, 2727, 75, 3325, 150)],
    "q1": [(1, "paid", 3, 1200), (2, "paid", 1, 0), (3, "paid", 1, 100), (5, "paid", 1, None)],
    "q2": [("cat_a", 9, 1825), ("cat_b", 3, -300), (None, 1, None)],
    "q3": [(10, 2, 200, 500), (11, 4, 180, 0), (None, 6, 364, 100), (12, 9, 250, None), (13, 11, 300, None),
           (14, 12, 100, 70), (15, 16, 5, 20), (16, 17, None, 5)],
    "q4": [(10, 3, 1200), (11, 2, 700), (None, 2, 300), (12, 2, 900), (13, 2, None), (14, 3, 190), (15, 2, 30),
           (16, 1, 5)],
}

# All rows cancelled -> q1 and q2 return EMPTY results (schema must still be correct).
EMPTY_SALES = [(1, 1, 1, 1, 200, "cancelled", 10), (2, 2, 1, 1, 200, "cancelled", 20)]
EXPECTED_EMPTY = {"q1": [], "q2": []}


def cardinality_sales():
    """Extreme cardinality: 5000 single-row customers + one customer with 2000 rows (skew)."""
    rows, oid = [], 1
    for c in range(1, 5001):
        rows.append((oid, c, (c % 100) + 1, (c % 4) + 1, c % 365, "paid", c)); oid += 1
    for i in range(2000):
        rows.append((oid, 999_999, 1, 1, i % 365, "pending" if i % 3 else "paid", i - 1000)); oid += 1
    return rows


def write(dir_path, sales_rows, products_rows=EDGE_PRODUCTS):
    dir_path.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*sales_rows))
    pq.write_table(pa.table({f.name: pa.array(list(c), type=f.type) for f, c in zip(SALES_SCHEMA, cols)},
                            schema=SALES_SCHEMA), dir_path / "sales.parquet")
    pcols = list(zip(*products_rows))
    pq.write_table(pa.table({f.name: pa.array(list(c), type=f.type) for f, c in zip(PRODUCTS_SCHEMA, pcols)},
                            schema=PRODUCTS_SCHEMA), dir_path / "products.parquet")
    return dir_path / "sales.parquet", dir_path / "products.parquet"
