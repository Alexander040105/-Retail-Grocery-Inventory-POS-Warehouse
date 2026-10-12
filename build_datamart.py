#!/usr/bin/env python3
"""
build_datamart.py
Group 8: Retail Grocery Inventory & Point-of-Sale (POS) Warehouse
Lab Activity 4 - Performance Tuning & Departmental Data Mart Creation
(Deliverable: Materialized Departmental Data Mart)

Builds marketing_datamart.db - a restricted, high-speed data mart for the
Marketing department, extracted from the tuned core warehouse
(warehouse.db). The mart deliberately carries unit / volume /
availability metrics ONLY: unit_price and any derived revenue are
excluded so no financial margin data leaves the core warehouse.

Mart tables (all constrained with NOT NULL / CHECK):

  mart_monthly_category_sales    month x category demand
  mart_product_demand            per-product sales velocity + demand share
  mart_location_category_demand  which categories move at which sites
  mart_category_availability     stock posture per category x status
                                 (do not promote backordered items)

Re-runnable: tables are dropped and rebuilt; totals are reconciled
against fact_inventory after the load.

Usage:
    .venv\\Scripts\\python.exe build_datamart.py
"""

import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import List, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "warehouse.db"
MART_PATH = SCRIPT_DIR / "marketing_datamart.db"

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger("build_datamart")


MART_DDL = """
DROP TABLE IF EXISTS mart_monthly_category_sales;
CREATE TABLE mart_monthly_category_sales (
    sales_month   TEXT    NOT NULL CHECK (sales_month GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'),
    category      TEXT    NOT NULL,
    units_sold    INTEGER NOT NULL CHECK (units_sold >= 0),
    active_items  INTEGER NOT NULL CHECK (active_items >= 0),
    PRIMARY KEY (sales_month, category)
);

DROP TABLE IF EXISTS mart_product_demand;
CREATE TABLE mart_product_demand (
    product_name       TEXT    NOT NULL,
    category           TEXT    NOT NULL,
    total_units_sold   INTEGER NOT NULL CHECK (total_units_sold >= 0),
    months_active      INTEGER NOT NULL CHECK (months_active >= 0),
    avg_monthly_units  REAL    NOT NULL CHECK (avg_monthly_units >= 0),
    last_order_month   TEXT    NOT NULL
        CHECK (last_order_month GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'),
    demand_share_pct   REAL    NOT NULL CHECK (demand_share_pct BETWEEN 0 AND 100),
    PRIMARY KEY (product_name, category)
);

DROP TABLE IF EXISTS mart_location_category_demand;
CREATE TABLE mart_location_category_demand (
    warehouse_location TEXT    NOT NULL,
    category           TEXT    NOT NULL,
    units_sold         INTEGER NOT NULL CHECK (units_sold >= 0),
    items_stocked      INTEGER NOT NULL CHECK (items_stocked >= 0),
    PRIMARY KEY (warehouse_location, category)
);

DROP TABLE IF EXISTS mart_category_availability;
CREATE TABLE mart_category_availability (
    category      TEXT    NOT NULL,
    status        TEXT    NOT NULL
        CHECK (status IN ('Active', 'Backordered', 'Discontinued')),
    item_count    INTEGER NOT NULL CHECK (item_count >= 0),
    units_on_hand INTEGER NOT NULL CHECK (units_on_hand >= 0),
    PRIMARY KEY (category, status)
);
"""

MART_LOADS: List[Tuple[str, str]] = [
    (
        "mart_monthly_category_sales",
        """
        INSERT INTO mart_monthly_category_sales
            (sales_month, category, units_sold, active_items)
        SELECT strftime('%Y-%m', f.last_order_date) AS sales_month,
               p.category,
               SUM(f.sales_volume)                 AS units_sold,
               COUNT(DISTINCT f.product_key)       AS active_items
        FROM wh.fact_inventory f
        JOIN wh.dim_product p ON p.product_key = f.product_key
        GROUP BY sales_month, p.category;
        """,
    ),
    (
        "mart_product_demand",
        """
        INSERT INTO mart_product_demand
            (product_name, category, total_units_sold, months_active,
             avg_monthly_units, last_order_month, demand_share_pct)
        SELECT p.product_name,
               p.category,
               SUM(f.sales_volume)                            AS total_units_sold,
               COUNT(DISTINCT strftime('%Y-%m', f.last_order_date)) AS months_active,
               ROUND(1.0 * SUM(f.sales_volume)
                     / COUNT(DISTINCT strftime('%Y-%m', f.last_order_date)), 2),
               MAX(strftime('%Y-%m', f.last_order_date)),
               ROUND(100.0 * SUM(f.sales_volume)
                     / (SELECT SUM(sales_volume) FROM wh.fact_inventory), 3)
        FROM wh.fact_inventory f
        JOIN wh.dim_product p ON p.product_key = f.product_key
        GROUP BY p.product_name, p.category;
        """,
    ),
    (
        "mart_location_category_demand",
        """
        INSERT INTO mart_location_category_demand
            (warehouse_location, category, units_sold, items_stocked)
        SELECT st.warehouse_location,
               p.category,
               SUM(f.sales_volume) AS units_sold,
               COUNT(*)            AS items_stocked
        FROM wh.fact_inventory f
        JOIN wh.dim_product p  ON p.product_key = f.product_key
        JOIN wh.dim_store   st ON st.store_key   = f.store_key
        GROUP BY st.warehouse_location, p.category;
        """,
    ),
    (
        "mart_category_availability",
        """
        INSERT INTO mart_category_availability
            (category, status, item_count, units_on_hand)
        SELECT p.category,
               f.status,
               COUNT(*)              AS item_count,
               SUM(f.stock_quantity) AS units_on_hand
        FROM wh.fact_inventory f
        JOIN wh.dim_product p ON p.product_key = f.product_key
        GROUP BY p.category, f.status;
        """,
    ),
]


def main() -> None:
    start = time.time()
    if not DB_PATH.exists():
        logger.error(f"{DB_PATH.name} not found - run etl_pipeline.py first.")
        sys.exit(1)

    logger.info("Group 8: Grocery Inventory & POS Warehouse - Marketing Data Mart")
    logger.info("=" * 66)
    logger.info("Scope: Marketing dept - unit/volume/availability metrics only.")
    logger.info("unit_price and derived revenue are NOT exported (no margin data).")
    logger.info("")

    logger.info("[ 1. CREATE constrained mart schema ]")
    logger.info("-" * 66)
    if MART_PATH.exists():
        MART_PATH.unlink()
    mart = sqlite3.connect(MART_PATH)
    mart.execute("PRAGMA foreign_keys=ON")
    mart.executescript(MART_DDL)
    mart.execute(f"ATTACH DATABASE '{DB_PATH}' AS wh")
    for table, _ in MART_LOADS:
        logger.info(f"    created {table}")

    logger.info("")
    logger.info("[ 2. EXTRACT + AGGREGATE from tuned warehouse ]")
    logger.info("-" * 66)
    for table, sql in MART_LOADS:
        mart.execute(sql)
        n = mart.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"    {table:<34} {n:>5} rows")
    mart.commit()

    logger.info("")
    logger.info("[ 3. RECONCILIATION - mart totals must match fact_inventory ]")
    logger.info("-" * 66)
    source_units = mart.execute(
        "SELECT SUM(sales_volume) FROM wh.fact_inventory"
    ).fetchone()[0]
    checks = [
        ("mart_monthly_category_sales.units_sold",
         "SELECT SUM(units_sold) FROM mart_monthly_category_sales"),
        ("mart_product_demand.total_units_sold",
         "SELECT SUM(total_units_sold) FROM mart_product_demand"),
        ("mart_location_category_demand.units_sold",
         "SELECT SUM(units_sold) FROM mart_location_category_demand"),
    ]
    all_ok = True
    for label, sql in checks:
        value = mart.execute(sql).fetchone()[0]
        ok = value == source_units
        all_ok = all_ok and ok
        logger.info(f"    {label:<42} {value:>6} vs {source_units}  {'OK' if ok else 'MISMATCH'}")
    n_items = mart.execute("SELECT SUM(item_count) FROM mart_category_availability").fetchone()[0]
    fact_rows = mart.execute("SELECT COUNT(*) FROM wh.fact_inventory").fetchone()[0]
    ok = n_items == fact_rows
    all_ok = all_ok and ok
    logger.info(f"    {'mart_category_availability.item_count':<42} {n_items:>6} vs {fact_rows}  {'OK' if ok else 'MISMATCH'}")

    logger.info("")
    logger.info("[ 4. CONFIDENTIALITY CHECK - no financial columns in mart ]")
    logger.info("-" * 66)
    financial_hits = []
    for (tbl,) in mart.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'mart_%'"
    ):
        for col in mart.execute(f"PRAGMA table_info({tbl})").fetchall():
            if any(k in col[1].lower() for k in ("price", "revenue", "cost", "margin")):
                financial_hits.append(f"{tbl}.{col[1]}")
    logger.info(
        "    financial columns present: " + (", ".join(financial_hits) if financial_hits else "none")
    )

    mart.execute("DETACH DATABASE wh")
    mart.execute("PRAGMA journal_mode=WAL")
    mart.close()

    logger.info("")
    logger.info("=" * 66)
    logger.info(f"Mart file    : {MART_PATH.name} ({MART_PATH.stat().st_size:,} bytes)")
    logger.info(f"Execution time : {time.time() - start:.2f} seconds")
    if not all_ok:
        logger.error("[FAILED] reconciliation mismatch - mart does not match source.")
        sys.exit(1)
    logger.info("[COMPLETE] Marketing data mart materialized and reconciled.")


if __name__ == "__main__":
    main()
