#!/usr/bin/env python3
"""
olap_analysis.py
Group 8: Retail Grocery Inventory & Point-of-Sale (POS) Warehouse
Mini Project Phase 2 - Data Warehousing (Milestone 2)

Runs the OLAP analysis set against the star schema in warehouse.db
(dim_product, dim_supplier, dim_store, fact_inventory + views) and
writes the full console log to olap_output.txt.

Analytical operations demonstrated:

  1. ROLL-UP      - monthly totals consolidated to yearly totals
                    (GROUPING SETS emulated with UNION ALL - SQLite has
                    no ROLLUP/CUBE syntax)
  2. DRILL-DOWN   - the top revenue category expanded month by month
  3. SLICE        - one dimension member fixed (top category), trended
  4. DICE         - a sub-cube: two categories x status='Active' x H2-2024,
                    ranked by supplier revenue
  5. PIVOT (cube) - category x month revenue matrix via pandas pivot_table
  6. RANK         - suppliers by total revenue with running totals
                    (RANK() + SUM() OVER window functions)
  7. ABC / Pareto - categories tiered by cumulative revenue share
                    (A = top ~80%, B = next ~15%, C = tail)
  8. STOCKOUT     - the warehouse's prevention posture: risk tier counts
                    and the 10 most urgent items (v_stockout_risk)

Usage:
    python olap_analysis.py            # writes olap_output.txt
"""

import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import List, Tuple

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "warehouse.db"
OUTPUT_PATH = SCRIPT_DIR / "olap_output.txt"

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger("olap_analysis")


def run_query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Tuple[List[str], list]:
    """Execute one query; return (column names, rows)."""
    cur = conn.execute(sql, params)
    return [d[0] for d in cur.description], cur.fetchall()


def format_table(cols: List[str], rows: list, limit: int = 12) -> List[str]:
    """Render a result set as 'a | b | c' lines, truncated at `limit` rows."""
    lines = ["    " + " | ".join(cols)]
    for row in rows[:limit]:
        lines.append("    " + " | ".join(str(v) for v in row))
    if len(rows) > limit:
        lines.append(f"    ... ({len(rows) - limit} more rows)")
    return lines


# ---------------------------------------------------------------------------
# 1. ROLL-UP - monthly -> yearly revenue / units (GROUPING SETS emulation)
# ---------------------------------------------------------------------------
SQL_ROLLUP = """
WITH monthly AS (
    SELECT
        strftime('%Y-%m', last_order_date) AS sales_month,
        SUM(sales_volume)                  AS units_sold,
        ROUND(SUM(sales_volume * unit_price), 2) AS revenue
    FROM fact_inventory
    GROUP BY sales_month
)
SELECT 'MONTH' AS level, sales_month AS period, units_sold, revenue
FROM monthly
UNION ALL
SELECT 'YEAR', strftime('%Y', sales_month || '-01'), SUM(units_sold), ROUND(SUM(revenue), 2)
FROM monthly
GROUP BY 2
ORDER BY period;
"""

# ---------------------------------------------------------------------------
# 4. DICE - sub-cube: 2 categories x Active x Jul-Dec 2024 -> top suppliers
# ---------------------------------------------------------------------------
SQL_DICE = """
SELECT
    s.supplier_name,
    p.category,
    SUM(f.sales_volume)                       AS units_sold,
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
"""

# ---------------------------------------------------------------------------
# 6. RANK - suppliers by revenue + running total (window functions)
# ---------------------------------------------------------------------------
SQL_RANK_SUPPLIERS = """
WITH supplier_revenue AS (
    SELECT
        s.supplier_name,
        SUM(f.sales_volume)                          AS units_sold,
        ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
    FROM fact_inventory f
    JOIN dim_supplier s ON s.supplier_key = f.supplier_key
    GROUP BY s.supplier_name
)
SELECT
    RANK() OVER (ORDER BY revenue DESC)              AS rnk,
    supplier_name,
    units_sold,
    revenue,
    ROUND(SUM(revenue) OVER (ORDER BY revenue DESC), 2) AS running_revenue
FROM supplier_revenue
ORDER BY rnk
LIMIT 15;
"""

# ---------------------------------------------------------------------------
# 7. ABC / Pareto - categories by cumulative revenue share
# ---------------------------------------------------------------------------
SQL_ABC = """
WITH category_revenue AS (
    SELECT
        p.category,
        ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
    FROM fact_inventory f
    JOIN dim_product p ON p.product_key = f.product_key
    GROUP BY p.category
),
ranked AS (
    SELECT
        category,
        revenue,
        RANK() OVER (ORDER BY revenue DESC)                 AS rnk,
        ROUND(100.0 * revenue / SUM(revenue) OVER (), 1)    AS revenue_pct,
        ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC)
                    / SUM(revenue) OVER (), 1)              AS cumulative_pct
    FROM category_revenue
)
SELECT
    rnk,
    category,
    revenue,
    revenue_pct,
    cumulative_pct,
    CASE
        WHEN cumulative_pct <= 80  THEN 'A'
        WHEN cumulative_pct <= 95  THEN 'B'
        ELSE 'C'
    END AS abc_tier
FROM ranked
ORDER BY rnk;
"""

# ---------------------------------------------------------------------------
# 8. STOCKOUT posture - tier counts + most urgent items
# ---------------------------------------------------------------------------
SQL_STOCKOUT_TIERS = """
SELECT
    stockout_risk,
    COUNT(*) AS items
FROM v_stockout_risk
GROUP BY stockout_risk
ORDER BY CASE stockout_risk
    WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
    WHEN 'WATCH' THEN 3 ELSE 4 END;
"""

SQL_STOCKOUT_TOP10 = """
SELECT
    product_name,
    category,
    warehouse_location,
    stock_quantity,
    sales_volume,
    days_of_cover,
    stockout_risk
FROM v_stockout_risk
WHERE stockout_risk IN ('CRITICAL', 'HIGH')
ORDER BY days_of_cover ASC
LIMIT 10;
"""


def main() -> None:
    start = time.time()
    if not DB_PATH.exists():
        logger.error(f"{DB_PATH.name} not found - run etl_pipeline.py first.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    report: List[str] = [
        "Group 8: Grocery Inventory & POS Warehouse - OLAP Analysis",
        "=" * 62,
        f"Database : {DB_PATH.name}",
        f"Fact rows: {conn.execute('SELECT COUNT(*) FROM fact_inventory').fetchone()[0]}",
        "Revenue is computed as sales_volume x unit_price grouped by "
        "last_order_date month (see Assumptions in the report).",
        "",
    ]

    def section(title: str) -> None:
        report.append("")
        report.append(f"[ {title} ]")
        report.append("-" * 62)

    # -- 1. ROLL-UP ----------------------------------------------------------
    section("1. ROLL-UP  monthly -> yearly totals")
    report.append("   (GROUPING SETS emulated via UNION ALL)")
    cols, rows = run_query(conn, SQL_ROLLUP)
    report += format_table(cols, rows, limit=8)
    report.append(f"    ({len(rows)} rows: month + year levels)")

    # -- 2. DRILL-DOWN -------------------------------------------------------
    section("2. DRILL-DOWN  top category -> monthly detail")
    _, top = run_query(
        conn,
        """
        SELECT p.category,
               ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
        FROM fact_inventory f
        JOIN dim_product p ON p.product_key = f.product_key
        GROUP BY p.category
        ORDER BY revenue DESC
        LIMIT 1;
        """,
    )
    top_category = top[0][0]
    report.append(f"   Top category by revenue: {top_category} ({top[0][1]})")
    cols, rows = run_query(
        conn,
        """
        SELECT strftime('%Y-%m', f.last_order_date) AS sales_month,
               SUM(f.sales_volume)                  AS units_sold,
               ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
        FROM fact_inventory f
        JOIN dim_product p ON p.product_key = f.product_key
        WHERE p.category = ?
        GROUP BY sales_month
        ORDER BY sales_month;
        """,
        (top_category,),
    )
    report += format_table(cols, rows)

    # -- 3. SLICE ------------------------------------------------------------
    section(f"3. SLICE  category = '{top_category}' across status")
    cols, rows = run_query(
        conn,
        """
        SELECT f.status,
               COUNT(*) AS items,
               SUM(f.sales_volume) AS units_sold,
               ROUND(SUM(f.sales_volume * f.unit_price), 2) AS revenue
        FROM fact_inventory f
        JOIN dim_product p ON p.product_key = f.product_key
        WHERE p.category = ?
        GROUP BY f.status
        ORDER BY revenue DESC;
        """,
        (top_category,),
    )
    report += format_table(cols, rows)

    # -- 4. DICE -------------------------------------------------------------
    section("4. DICE  Dairy + Beverages x Active x Jul-Dec 2024")
    cols, rows = run_query(conn, SQL_DICE)
    report += format_table(cols, rows)

    # -- 5. PIVOT (cube) ------------------------------------------------------
    section("5. PIVOT  category x month revenue matrix (pandas)")
    df = pd.read_sql_query(
        """
        SELECT p.category,
               strftime('%Y-%m', f.last_order_date) AS sales_month,
               f.sales_volume * f.unit_price        AS revenue
        FROM fact_inventory f
        JOIN dim_product p ON p.product_key = f.product_key;
        """,
        conn,
    )
    pivot = df.pivot_table(
        index="category", columns="sales_month",
        values="revenue", aggfunc="sum", margins=True, margins_name="TOTAL",
    ).round(0)
    for line in pivot.to_string().splitlines():
        report.append("    " + line)

    # -- 6. RANK -------------------------------------------------------------
    section("6. RANK  suppliers by revenue (+ running total)")
    cols, rows = run_query(conn, SQL_RANK_SUPPLIERS)
    report += format_table(cols, rows)

    # -- 7. ABC / Pareto ------------------------------------------------------
    section("7. ABC / PARETO  categories by cumulative revenue share")
    cols, rows = run_query(conn, SQL_ABC)
    report += format_table(cols, rows, limit=20)

    # -- 8. STOCKOUT posture --------------------------------------------------
    section("8. STOCKOUT  prevention posture (v_stockout_risk)")
    cols, rows = run_query(conn, SQL_STOCKOUT_TIERS)
    report += format_table(cols, rows)
    report.append("")
    report.append("   Most urgent items:")
    cols, rows = run_query(conn, SQL_STOCKOUT_TOP10)
    report += format_table(cols, rows)

    conn.close()

    report.append("")
    report.append("=" * 62)
    report.append(f"Execution time : {time.time() - start:.2f} seconds")

    text = "\n".join(report)
    OUTPUT_PATH.write_text(text, encoding="utf-8")
    logger.info(text)
    logger.info(f"[COMPLETE] OLAP output written to {OUTPUT_PATH.name}")


if __name__ == "__main__":
    main()
