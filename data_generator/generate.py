"""Deterministic retail dataset generator (sales fact + products dimension).

Determinism: numpy.random.Generator(PCG64(seed)), generated in fixed-size chunks so
the stream does not depend on available memory. The manifest stores a *content*
hash (independent of Parquet encoder bytes) so two machines can prove identical data.

Data characteristics (intentional, so the main dataset also exercises edge semantics):
  - order_id: 1..N, unique, NOT NULL
  - customer_id: uniform 1..max(1, N//10)  (high cardinality, ~N/10 groups); 0.05% NULL
  - store_id: int32 1..100
  - product_id: 1..1050; ids 1001..1050 do NOT exist in products (orphans, ~4.8%); 0.05% NULL
  - day_id: int32 0..364 (many ties per customer)
  - status: paid 80% / pending 12% / cancelled 7.9% / NULL 0.1%
  - amount_cents: int64 100..99999; 1% negative (refunds), 0.5% zero, 0.1% NULL
  - products: 1000 rows, product_id unique, category 'category_00'..'category_19', product 1000 has NULL category
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK = 1_000_000
N_PRODUCTS = 1000
N_ORPHAN_PRODUCTS = 50
SALES_SCHEMA = pa.schema([
    ("order_id", pa.int64()), ("customer_id", pa.int64()), ("store_id", pa.int32()),
    ("product_id", pa.int64()), ("day_id", pa.int32()), ("status", pa.string()),
    ("amount_cents", pa.int64()),
])
PRODUCTS_SCHEMA = pa.schema([("product_id", pa.int64()), ("category", pa.string())])
STATUSES = np.array(["paid", "pending", "cancelled"], dtype=object)


def _null_mask(rng, n, p):
    return rng.random(n) < p


def sales_chunk(rng: np.random.Generator, start: int, n: int, total_rows: int) -> pa.Table:
    n_customers = max(1, total_rows // 10)
    order_id = np.arange(start, start + n, dtype=np.int64)
    customer_id = rng.integers(1, n_customers + 1, size=n, dtype=np.int64)
    store_id = rng.integers(1, 101, size=n, dtype=np.int32)
    product_id = rng.integers(1, N_PRODUCTS + N_ORPHAN_PRODUCTS + 1, size=n, dtype=np.int64)
    day_id = rng.integers(0, 365, size=n, dtype=np.int32)
    u = rng.random(n)
    status_idx = np.where(u < 0.80, 0, np.where(u < 0.92, 1, 2))
    status = STATUSES[status_idx]
    amount = rng.integers(100, 100_000, size=n, dtype=np.int64)
    v = rng.random(n)
    amount = np.where(v < 0.01, -amount, amount)              # refunds
    amount = np.where((v >= 0.01) & (v < 0.015), 0, amount)   # zero-value
    cust_null = _null_mask(rng, n, 0.0005)
    prod_null = _null_mask(rng, n, 0.0005)
    status_null = _null_mask(rng, n, 0.001)
    amount_null = _null_mask(rng, n, 0.001)
    return pa.table({
        "order_id": pa.array(order_id),
        "customer_id": pa.array(customer_id, mask=cust_null),
        "store_id": pa.array(store_id),
        "product_id": pa.array(product_id, mask=prod_null),
        "day_id": pa.array(day_id),
        "status": pa.array(status, type=pa.string(), mask=status_null),
        "amount_cents": pa.array(amount, mask=amount_null),
    }, schema=SALES_SCHEMA)


def products_table(rng: np.random.Generator) -> pa.Table:
    pid = np.arange(1, N_PRODUCTS + 1, dtype=np.int64)
    cat = np.array([f"category_{i:02d}" for i in rng.integers(0, 20, size=N_PRODUCTS)], dtype=object)
    mask = pid == N_PRODUCTS
    return pa.table({"product_id": pa.array(pid), "category": pa.array(cat, type=pa.string(), mask=mask)},
                    schema=PRODUCTS_SCHEMA)


def content_hash(path: Path) -> str:
    """SHA-256 over column values + validity, row order preserved. Encoder-independent."""
    h = hashlib.sha256()
    pf = pq.ParquetFile(path)
    h.update(str(pf.schema_arrow).encode())
    for batch in pf.iter_batches(batch_size=CHUNK):
        for col in batch.columns:
            valid = np.asarray(col.is_valid())
            h.update(valid.tobytes())
            if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
                h.update("\x1f".join("" if x is None else x for x in col.to_pylist()).encode())
            else:
                h.update(np.asarray(col.fill_null(0)).tobytes())
    return h.hexdigest()


def generate(rows: int, out_dir: Path, seed: int, row_group_size: int = 122_880) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    rng = np.random.Generator(np.random.PCG64(seed))
    products = products_table(rng)
    pq.write_table(products, out_dir / "products.parquet")
    sales_path = out_dir / "sales.parquet"
    with pq.ParquetWriter(sales_path, SALES_SCHEMA) as w:
        start = 1
        while start <= rows:
            n = min(CHUNK, rows - start + 1)
            w.write_table(sales_chunk(rng, start, n, rows), row_group_size=row_group_size)
            start += n
    gen_seconds = time.perf_counter() - t0
    t1 = time.perf_counter()
    manifest = {
        "rows": rows, "seed": seed, "rng": "numpy PCG64", "numpy": np.__version__, "pyarrow": pa.__version__,
        "chunk_rows": CHUNK, "row_group_size": row_group_size, "currency": "int64 cents",
        "products": N_PRODUCTS, "orphan_product_ids": [N_PRODUCTS + 1, N_PRODUCTS + N_ORPHAN_PRODUCTS],
        "generation_seconds": round(gen_seconds, 4),
        "files": {
            "sales.parquet": {"bytes": sales_path.stat().st_size, "content_sha256": content_hash(sales_path)},
            "products.parquet": {"bytes": (out_dir / "products.parquet").stat().st_size,
                                 "content_sha256": content_hash(out_dir / "products.parquet")},
        },
    }
    manifest["hash_seconds"] = round(time.perf_counter() - t1, 4)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=20261106)
    a = ap.parse_args()
    print(json.dumps(generate(a.rows, a.out, a.seed), indent=2))
