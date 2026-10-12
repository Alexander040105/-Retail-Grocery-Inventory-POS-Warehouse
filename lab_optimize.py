#!/usr/bin/env python3
"""
lab_optimize.py
Group 8: Retail Grocery Inventory & Point-of-Sale (POS) Warehouse
Lab Activity 4 - Performance Tuning & Departmental Data Mart Creation
(Deliverable: Optimization & Indexing Script)

Tunes warehouse.db read performance:

  1. Online backup via the sqlite3 backup API -> backups/
  2. PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON
  3. EXPLAIN QUERY PLAN + timing baseline on the join/filter queries used
     most heavily by olap_analysis.py and monthly_variance.sql
  4. CREATE INDEX on those columns (FK joins, order dates, status,
     product category)
  5. ANALYZE, then the same EXPLAIN + timings to prove
     full SCAN -> index SEARCH and record the speedups
  6. integrity_check + foreign_key_check

Re-runnable: the lab indexes are dropped at the start so the before/after
comparison is always honest.

Usage:
    .venv\\Scripts\\python.exe lab_optimize.py
"""

import logging
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "warehouse.db"
BACKUP_DIR = SCRIPT_DIR / "backups"

BENCH_ITERS = 500

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger("lab_optimize")


INDEXES: List[Tuple[str, str, str]] = [
    (
        "idx_fact_product_key",
        "CREATE INDEX IF NOT EXISTS idx_fact_product_key "
        "ON fact_inventory(product_key)",
        "FK join to dim_product in nearly every OLAP query",
    ),
    (
        "idx_fact_supplier_key",
        "CREATE INDEX IF NOT EXISTS idx_fact_supplier_key "
        "ON fact_inventory(supplier_key)",
        "FK join to dim_supplier (DICE, supplier RANK queries)",
    ),
    (
        "idx_fact_store_key",
        "CREATE INDEX IF NOT EXISTS idx_fact_store_key "
        "ON fact_inventory(store_key)",
        "FK join to dim_store used by both warehouse views",
    ),
    (
        "idx_fact_last_order_date",
        "CREATE INDEX IF NOT EXISTS idx_fact_last_order_date "
        "ON fact_inventory(last_order_date)",
        "date filtering / monthly grouping field in all variance queries",
    ),
    (
        "idx_fact_status_orderdate",
        "CREATE INDEX IF NOT EXISTS idx_fact_status_orderdate "
        "ON fact_inventory(status, last_order_date)",
        "compound index covering the status + date-range DICE pattern",
    ),
    (
        "idx_dim_product_category",
        "CREATE INDEX IF NOT EXISTS idx_dim_product_category "
        "ON dim_product(category)",
        "category filter / GROUP BY column in ABC, drill-down, slice",
    ),
]

BENCHMARK_QUERIES: Dict[str, Tuple[str, tuple]] = {
    "Q1 category revenue join+group": (
        """
        SELECT p.category,
               SUM(f.sales_volume)                        AS units_sold,
               ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
        FROM fact_inventory f
        JOIN dim_product p ON p.product_key = f.product_key
        GROUP BY p.category;
        """,
        (),
    ),
    "Q2 dice: category IN + status + date range": (
        """
        SELECT s.supplier_name, p.category,
               SUM(f.sales_volume)                        AS units_sold,
               ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
        FROM fact_inventory f
        JOIN dim_product  p ON p.product_key  = f.product_key
        JOIN dim_supplier s ON s.supplier_key = f.supplier_key
        WHERE p.category IN ('Dairy', 'Beverages')
          AND f.status = 'Active'
          AND f.last_order_date BETWEEN '2024-07-01' AND '2024-12-31'
        GROUP BY s.supplier_name, p.category
        ORDER BY revenue DESC
        LIMIT 10;
        """,
        (),
    ),
    "Q3 supplier revenue join+group": (
        """
        SELECT s.supplier_name,
               SUM(f.sales_volume)                        AS units_sold,
               ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
        FROM fact_inventory f
        JOIN dim_supplier s ON s.supplier_key = f.supplier_key
        GROUP BY s.supplier_name
        ORDER BY revenue DESC
        LIMIT 15;
        """,
        (),
    ),
    "Q4 single-month sales report": (
        """
        SELECT COUNT(*), SUM(sales_volume)
        FROM fact_inventory
        WHERE last_order_date BETWEEN '2024-09-01' AND '2024-09-30';
        """,
        (),
    ),
}


def backup_database(src_path: Path, dst_dir: Path) -> Path:
    """Online backup via the sqlite3 backup API; returns the backup path."""
    dst_dir.mkdir(exist_ok=True)
    dst = dst_dir / f"{src_path.stem}_backup_{datetime.now():%Y%m%d_%H%M%S}.db"
    src = sqlite3.connect(src_path)
    dest = sqlite3.connect(dst)
    src.backup(dest)
    src_pages = src.execute("PRAGMA page_count").fetchone()[0]
    dst_pages = dest.execute("PRAGMA page_count").fetchone()[0]
    src.close()
    dest.close()
    logger.info(
        f"[BACKUP] {src_path.name} -> {dst.relative_to(SCRIPT_DIR)} "
        f"({dst.stat().st_size:,} bytes, {dst_pages}/{src_pages} pages verified)"
    )
    if src_pages != dst_pages:
        raise RuntimeError("backup verification failed: page count mismatch")
    return dst


def apply_pragmas(conn: sqlite3.Connection) -> None:
    mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    conn.execute("PRAGMA foreign_keys=ON")
    fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    logger.info(f"[PRAGMA] journal_mode={mode}, foreign_keys={fk}")


def explain_plan(conn: sqlite3.Connection, sql: str) -> List[str]:
    rows = conn.execute("EXPLAIN QUERY PLAN " + sql, ()).fetchall()
    return [r[3] for r in rows]


def time_query(conn: sqlite3.Connection, sql: str, params: tuple, iters: int) -> float:
    """Total milliseconds for `iters` executions (fetchall included)."""
    start = time.perf_counter()
    for _ in range(iters):
        conn.execute(sql, params).fetchall()
    return (time.perf_counter() - start) * 1000.0


def main() -> None:
    start = time.time()
    if not DB_PATH.exists():
        logger.error(f"{DB_PATH.name} not found - run etl_pipeline.py first.")
        sys.exit(1)

    report: List[str] = [
        "Group 8: Grocery Inventory & POS Warehouse - Lab 4 Optimization",
        "=" * 66,
        f"Database : {DB_PATH.name}",
        f"Benchmark: {BENCH_ITERS} iterations per query (db is small; "
        "loops make microsecond-scale differences measurable)",
        "",
    ]

    def section(title: str) -> None:
        report.append("")
        report.append(f"[ {title} ]")
        report.append("-" * 66)

    backup_database(DB_PATH, BACKUP_DIR)

    conn = sqlite3.connect(DB_PATH)
    apply_pragmas(conn)

    section("BASELINE ROW COUNTS")
    for table in ("dim_product", "dim_supplier", "dim_store", "fact_inventory"):
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        report.append(f"    {table:<18} {n:>6} rows")

    section("RESET (drop previous lab indexes for an honest baseline)")
    for name, _, _ in INDEXES:
        conn.execute(f"DROP INDEX IF EXISTS {name}")
    conn.commit()
    report.append("    dropped " + ", ".join(n for n, _, _ in INDEXES))

    before: Dict[str, Tuple[List[str], float]] = {}
    section("BEFORE: EXPLAIN QUERY PLAN + timing")
    for label, (sql, params) in BENCHMARK_QUERIES.items():
        plan = explain_plan(conn, sql)
        ms = time_query(conn, sql, params, BENCH_ITERS)
        before[label] = (plan, ms)
        report.append(f"  {label}  ({ms:.1f} ms / {BENCH_ITERS} runs)")
        for line in plan:
            report.append(f"      {line}")

    section("CREATE INDEX")
    for name, ddl, why in INDEXES:
        conn.execute(ddl)
        report.append(f"    {ddl}")
        report.append(f"      -- {name}: {why}")
    conn.execute("ANALYZE")
    conn.commit()
    report.append("    ANALYZE executed (planner statistics refreshed)")

    after: Dict[str, Tuple[List[str], float]] = {}
    section("AFTER: EXPLAIN QUERY PLAN + timing")
    for label, (sql, params) in BENCHMARK_QUERIES.items():
        plan = explain_plan(conn, sql)
        ms = time_query(conn, sql, params, BENCH_ITERS)
        after[label] = (plan, ms)
        report.append(f"  {label}  ({ms:.1f} ms / {BENCH_ITERS} runs)")
        for line in plan:
            report.append(f"      {line}")

    section("SPEEDUP SUMMARY")
    report.append(f"    {'query':<46} {'before ms':>10} {'after ms':>10} {'speedup':>8}")
    for label in BENCHMARK_QUERIES:
        b_ms, a_ms = before[label][1], after[label][1]
        speedup = b_ms / a_ms if a_ms > 0 else float("inf")
        report.append(f"    {label:<46} {b_ms:>10.1f} {a_ms:>10.1f} {speedup:>7.2f}x")

    section("VERIFICATION")
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    report.append(f"    integrity_check    : {integrity}")
    report.append(f"    foreign_key_check  : {len(fk_violations)} violations")
    report.append("    indexes now on warehouse.db:")
    for name, tbl in conn.execute(
        "SELECT name, tbl_name FROM sqlite_master "
        "WHERE type='index' AND name LIKE 'idx_%' ORDER BY name"
    ):
        report.append(f"      {name:<28} on {tbl}")

    conn.close()
    report.append("")
    report.append("=" * 66)
    report.append(f"Execution time : {time.time() - start:.2f} seconds")

    logger.info("\n".join(report))
    logger.info("[COMPLETE] Optimization finished - backup in backups/, indexes applied.")


if __name__ == "__main__":
    main()
