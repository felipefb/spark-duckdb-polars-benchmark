"""Single source of truth for query semantics, output schemas and engine implementations.

Semantics are defined in docs/SEMANTICS.md *before* execution; every engine must match the
independent pure-Python reference (bench.reference) exactly.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa

QUERIES = ("q0", "q1", "q2", "q3", "q4")

# Logical output schema (name, logical type). Validation compares these strictly.
SCHEMA = {
    "q0": [("n_rows", "int64"), ("n_customer", "int64"), ("sum_store", "int64"), ("sum_product", "int64"),
           ("sum_day", "int64"), ("sum_status_len", "int64"), ("sum_amount", "int64"), ("sum_order", "int64")],
    "q1": [("store_id", "int32"), ("status", "string"), ("n", "int64"), ("total_cents", "int64")],
    "q2": [("category", "string"), ("n", "int64"), ("total_cents", "int64")],
    "q3": [("customer_id", "int64"), ("order_id", "int64"), ("day_id", "int32"), ("amount_cents", "int64")],
    "q4": [("customer_id", "int64"), ("n", "int64"), ("total_cents", "int64")],
}
KEYS = {"q0": [], "q1": ["store_id", "status"], "q2": ["category"], "q3": ["customer_id"], "q4": ["customer_id"]}
DESCRIPTION = {
    "q0": "scan proxy: full-column aggregates (read-cost proxy, not a business query)",
    "q1": "filter + aggregation: paid sales with day_id >= 180 by store",
    "q2": "inner join with products + aggregation by category (status <> 'cancelled')",
    "q3": "window: latest transaction per customer (ROW_NUMBER, deterministic tie-break)",
    "q4": "high-cardinality aggregation by customer_id",
}

SQL = {
    "q0": """SELECT CAST(COUNT(*) AS BIGINT) AS n_rows,
       CAST(COUNT(customer_id) AS BIGINT) AS n_customer,
       CAST(SUM(store_id) AS BIGINT) AS sum_store,
       CAST(SUM(product_id) AS BIGINT) AS sum_product,
       CAST(SUM(day_id) AS BIGINT) AS sum_day,
       CAST(SUM(LENGTH(status)) AS BIGINT) AS sum_status_len,
       CAST(SUM(amount_cents) AS BIGINT) AS sum_amount,
       CAST(SUM(order_id) AS BIGINT) AS sum_order
FROM sales""",
    "q1": """SELECT store_id, status, CAST(COUNT(*) AS BIGINT) AS n, CAST(SUM(amount_cents) AS BIGINT) AS total_cents
FROM sales WHERE day_id >= 180 AND status = 'paid'
GROUP BY store_id, status""",
    "q2": """SELECT p.category, CAST(COUNT(*) AS BIGINT) AS n, CAST(SUM(s.amount_cents) AS BIGINT) AS total_cents
FROM sales s INNER JOIN products p ON s.product_id = p.product_id
WHERE s.status <> 'cancelled'
GROUP BY p.category""",
    "q3": """SELECT customer_id, order_id, day_id, amount_cents FROM (
  SELECT customer_id, order_id, day_id, amount_cents,
         ROW_NUMBER() OVER (PARTITION BY customer_id
                            ORDER BY day_id DESC NULLS LAST, order_id DESC NULLS LAST, amount_cents DESC NULLS LAST) AS rn
  FROM sales) t
WHERE rn = 1""",
    "q4": """SELECT customer_id, CAST(COUNT(*) AS BIGINT) AS n, CAST(SUM(amount_cents) AS BIGINT) AS total_cents
FROM sales GROUP BY customer_id""",
}


# ----------------------------------------------------------------------------- DuckDB
class DuckDBEngine:
    name = "duckdb"

    def __init__(self, threads: int, tmp_dir: Path | None = None, settings: dict | None = None):
        import duckdb
        self.con = duckdb.connect()
        self.con.execute(f"SET threads={int(threads)}")
        if tmp_dir is not None:
            self.con.execute(f"SET temp_directory='{Path(tmp_dir).as_posix()}'")
        for k, v in (settings or {}).items():
            self.con.execute(f"SET {k}={v}")

    def register(self, sales: Path, products: Path):
        s, p = Path(sales).as_posix().replace("'", "''"), Path(products).as_posix().replace("'", "''")
        self.con.execute(f"CREATE OR REPLACE VIEW sales AS SELECT * FROM read_parquet('{s}')")
        self.con.execute(f"CREATE OR REPLACE VIEW products AS SELECT * FROM read_parquet('{p}')")

    def run(self, query: str) -> pa.Table:
        return self.con.execute(SQL[query]).fetch_arrow_table()

    def close(self):
        self.con.close()


# ----------------------------------------------------------------------------- Polars
class PolarsEngine:
    name = "polars"

    def __init__(self, threads: int, tmp_dir: Path | None = None, **_):
        # Thread count must be set through POLARS_MAX_THREADS before import (done by the runner/tests).
        import polars as pl
        self.pl = pl

    def register(self, sales: Path, products: Path):
        self.s = self.pl.scan_parquet(str(sales))
        self.p = self.pl.scan_parquet(str(products))

    def _sum(self, col):
        pl = self.pl
        # SQL semantics: SUM over zero non-NULL values is NULL (Polars .sum() returns 0).
        c = pl.col(col).cast(pl.Int64)
        return pl.when(c.count() > 0).then(c.sum()).otherwise(None).cast(pl.Int64)

    def plan(self, query: str):
        pl, s, p = self.pl, self.s, self.p
        i64 = pl.Int64
        if query == "q0":
            x = s.select(
                pl.len().cast(i64).alias("n_rows"),
                pl.col("customer_id").count().cast(i64).alias("n_customer"),
                self._sum("store_id").alias("sum_store"),
                self._sum("product_id").alias("sum_product"),
                self._sum("day_id").alias("sum_day"),
                (pl.when(pl.col("status").count() > 0)
                   .then(pl.col("status").str.len_chars().cast(i64).sum()).otherwise(None).cast(i64)
                   .alias("sum_status_len")),
                self._sum("amount_cents").alias("sum_amount"),
                self._sum("order_id").alias("sum_order"),
            )
        elif query == "q1":
            x = (s.filter((pl.col("day_id") >= 180) & (pl.col("status") == "paid"))
                  .group_by(["store_id", "status"])
                  .agg(pl.len().cast(i64).alias("n"), self._sum("amount_cents").alias("total_cents")))
        elif query == "q2":
            x = (s.filter(pl.col("status") != "cancelled")
                  .join(p, on="product_id", how="inner")
                  .group_by("category")
                  .agg(pl.len().cast(i64).alias("n"), self._sum("amount_cents").alias("total_cents")))
        elif query == "q3":
            x = (s.sort(["customer_id", "day_id", "order_id", "amount_cents"],
                        descending=[False, True, True, True], nulls_last=True)
                  .unique(subset=["customer_id"], keep="first", maintain_order=True))
        elif query == "q4":
            x = s.group_by("customer_id").agg(pl.len().cast(i64).alias("n"),
                                              self._sum("amount_cents").alias("total_cents"))
        else:
            raise ValueError(query)
        return x.select([c for c, _ in SCHEMA[query]])

    def run(self, query: str) -> pa.Table:
        return self.plan(query).collect().to_arrow()

    def close(self):
        pass


# ----------------------------------------------------------------------------- Spark
class SparkEngine:
    name = "spark"

    def __init__(self, threads: int, tmp_dir: Path | None = None, conf: dict | None = None, **_):
        from pyspark.sql import SparkSession
        b = SparkSession.builder.master(f"local[{int(threads)}]").appName("engine-bench")
        for k, v in (conf or {}).items():
            b = b.config(k, v)
        if tmp_dir is not None:
            b = b.config("spark.local.dir", str(tmp_dir))
        self.spark = b.getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def register(self, sales: Path, products: Path):
        self.spark.read.parquet(str(sales)).createOrReplaceTempView("sales")
        self.spark.read.parquet(str(products)).createOrReplaceTempView("products")

    def run(self, query: str) -> pa.Table:
        return self.spark.sql(SQL[query]).toArrow()

    def close(self):
        self.spark.stop()


ENGINES = {"duckdb": DuckDBEngine, "polars": PolarsEngine, "spark": SparkEngine}
