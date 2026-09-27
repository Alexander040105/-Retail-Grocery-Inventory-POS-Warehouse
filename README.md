# Retail Grocery Inventory & Point-of-Sale (POS) Warehouse

## Project Background

A mid-sized supermarket chain operates multiple branches and needs to monitor
daily store sales, stock turnover rates, and inventory reorder thresholds.
From the perspective of a data analyst embedded with the operations team, the
business problem is that POS and inventory records arrive as messy per-branch
extracts — prices carry currency symbols, dates are inconsistent, and there is
no single place to answer "what should we reorder today, and which shelves are
about to go empty?"

This project builds an end-to-end warehouse over the Kaggle *Grocery Store
Sales and Inventory* dataset (990 rows × 16 columns, covering 13 months from
2024-02 to 2025-02). Messy POS/inventory records are cleansed, loaded into a
star-schema SQLite warehouse, mirrored by supplier audit logs in MongoDB
Atlas, and analyzed with SQL CTEs and OLAP operations.

Insights and recommendations are provided on the following key areas:

- **Sales trends** — monthly revenue variance, seasonality, and category Pareto
- **Inventory health** — reorder pressure and stockout risk
- **Supplier audit** — operational events tracked per supplier in MongoDB
- **Data quality** — issues found in the raw extract and how they were fixed

The ETL pipeline used to inspect and clean the data can be found in
[`etl_pipeline.py`](etl_pipeline.py).

Targeted SQL queries for the business questions (monthly sales variance,
reorder pressure, top stockout risks) can be found in
[`monthly_variance.sql`](monthly_variance.sql).

An interactive notebook used to explore the pipeline and results can be found
in [`miniProjectPhase1.ipynb`](miniProjectPhase1.ipynb), and the OLAP analysis
script in [`olap_analysis.py`](olap_analysis.py).

## Data Structure & Initial Checks

The warehouse is a star schema in `warehouse.db` with four tables and two
analytical views, holding 990 fact records in total. A description of each
table:

- **`dim_product`** (124 rows) — grain: product name + category. Category
  distribution: Fruits & Vegetables 41, Dairy 24, Grains & Pulses 20,
  Seafood 10, Oils & Fats 10, Bakery 10, Beverages 8, Uncategorized 1.
- **`dim_supplier`** (350 rows) — grain: supplier name.
- **`dim_store`** (990 rows) — grain: warehouse location (unique free-text
  addresses in the source, so it is 1:1 with the fact rows).
- **`fact_inventory`** (990 rows) — grain: one inventory/sales record.
  Measures: `stock_quantity`, `reorder_level`, `reorder_quantity`,
  `unit_price`, `sales_volume`, `inventory_turnover_rate`,
  `date_received`, `last_order_date`, `expiration_date`, `status`.
  `row_hash` (UNIQUE) makes pipeline re-runs idempotent.

```
 dim_product              dim_supplier             dim_store
 (name + category, 124)   (name, 350)              (location, 990)
        \                       |                       /
         \                      |                      /
                    fact_inventory (990 rows)
```

The raw `product_id` / `supplier_id` are record-level (unique per row), so
they are kept on the fact as **degenerate dimensions** while the true
business entities become conformed dimensions with surrogate keys.

Two views carry the business outcomes:

- **`v_reorder_recommendations`** — every item at/below reorder level with
  `suggested_order_qty` and CRITICAL/HIGH priority → automated replenishment
  queue (298 items flagged).
- **`v_stockout_risk`** — `days_of_cover` (on-hand ÷ monthly sales velocity)
  tiered CRITICAL <7d / HIGH <15d / WATCH <30d / LOW → stockout prevention
  watchlist.

## Executive Summary

### Overview of Findings

Across 13 months the chain recorded ~$344K in revenue, but demand is volatile
— revenue contracted −41.3% in September 2024 after peaking at +63.9% growth
in March. The category mix is concentrated: Fruits & Vegetables alone drives
26% of revenue ($89,479), yet 38% of its items are discontinued or
backordered, putting a third of the category's revenue at risk. Operationally,
298 of 990 items are already at or below their reorder level and 27 items sit
at CRITICAL stockout risk with under ~5 days of cover — replenishment is not a
planning exercise, it is overdue.

## Insights Deep Dive

### Sales Trends

- **Total revenue ≈ $344,269** over the 13-month window (2024-02 → 2025-02);
  the 2024 calendar-year roll-up is $294,055 across 48,973 units.
- **Strong early growth, sharp autumn dip.** Month-over-month variance peaked
  at +63.9% in 2024-03 and +56.5% in 2024-04, then swung to **−41.3% in
  2024-09** — a clear seasonal contraction worth a demand-planning note.
- **Concentrated revenue base.** ABC/Pareto analysis puts Fruits &
  Vegetables, Beverages, Seafood, and Dairy in tier A — four categories
  carrying **78.7% of revenue**; Grains & Pulses and Oils & Fats sit in tier
  B; Bakery and Uncategorized in tier C.
- **Supplier concentration.** Top supplier Youfeed accounts for $6,613; the
  top 10 suppliers together represent $45,479 of supplier-attributed revenue.

### Inventory Health

- **298 items are at or below reorder level** — the
  `v_reorder_recommendations` view is effectively a standing purchasing queue
  with a computed `suggested_order_qty`.
- **Stockout posture:** 27 CRITICAL / 108 HIGH / 221 WATCH / 301 LOW. The 10
  most urgent items have under 5 days of cover — e.g. Egg (Chicken) at 3.0
  days, Plum at 3.3 days, Black Coffee at 3.7 days.
- **Status split is a third unhealthy:** 332 Active / 325 Backordered /
  333 Discontinued across the 990 records — roughly two-thirds of the
  catalogue is not cleanly sellable.
- **Reorder pressure is persistent, not episodic:** the monthly reorder
  pressure CTE shows 9–38 products at risk every month, accumulating to 298
  distinct flagged items by the end of the window.

### Supplier Audit

- **350 supplier audit documents** in MongoDB Atlas (`supplier_audit_logs`),
  one per supplier entity, capturing **1,839 deterministic audit events**.
- **Event mix:** EXPIRED_BEFORE_LAST_ORDER 503, REORDER_TRIGGERED 465,
  DISCONTINUED_WITH_STOCK 333, BACKORDERED 325, STOCKOUT_RISK 212,
  MISSING_CATEGORY 1.
- **Severity profile:** 790 HIGH, 503 MEDIUM, 334 LOW, 212 CRITICAL — the
  CRITICAL tier maps directly to the stockout watchlist.
- **503 "expired before last order" anomalies** suggest ordering lag on
  perishables — a supplier-side or warehouse-side process issue rather than a
  data bug, since each event is traceable to a product and location.

### Data Quality

- **Currency symbols everywhere:** `Unit_Price` arrived as strings like
  `"$4.50 "` on all 990 rows — stripped `$`, commas, and whitespace, cast to
  `REAL`.
- **Misspelled header:** `Catagory` → renamed to `category`.
- **Missing category:** 1 null imputed to `'Uncategorized'` and logged as a
  `MISSING_CATEGORY` audit event rather than silently dropped.
- **Mixed-width dates:** `M/D/YYYY` and `MM/DD/YYYY` variants normalized to
  ISO `YYYY-MM-DD`.
- **Unvalidated IDs:** the `XX-XXX-XXXX` pattern was regex-validated for
  `product_id`/`supplier_id`; negative-quantity quarantine rules are armed.
  **Result: 990/990 rows cleansed, 0 quarantined.**

## Recommendations

Based on the insights above, we would recommend the supply-chain and
inventory management team to consider the following:

- **298 items are already at/below reorder level** — feed
  `v_reorder_recommendations` into purchasing daily; `suggested_order_qty`
  is directly actionable as an order list.
- **27 items have under ~5 days of cover** — put CRITICAL stockout items on
  immediate expedite; any supplier delay becomes a shelf stockout.
- **Fruits & Vegetables is both the revenue leader and heavily flagged** —
  prioritize the perishables reorder cycle before lower-impact categories.
- **Four tier-A categories carry ~79% of revenue** — give their suppliers the
  strictest SLA terms and monitor their audit events first.
- **503 expiry-before-order anomalies exist in the audit log** — review
  receiving/ordering lag with the affected suppliers; the MongoDB audit
  trail already isolates which suppliers and products are involved.

## Assumptions and Caveats

Throughout the analysis, multiple assumptions were made to manage challenges
with the data:

- The dataset has no true POS transaction timestamp, so
  **`last_order_date` is used as the sales month** for all revenue and
  variance analysis.
- `sales_volume` is treated as **monthly sales velocity** when computing
  `days_of_cover`.
- `dim_store` is 1:1 with fact rows because locations are unique free-text
  addresses in the source — no store hierarchy exists to conform to.
- 0 quarantined rows reflects this dataset; the quarantine rules are armed
  and will produce `quarantined_rows.csv` when violations occur.
- `product_id`/`supplier_id` are degenerate dimensions (unique per record);
  product/supplier identity is resolved by name.

## Appendix — How to Run

From this folder, using the repository's virtual environment:

```powershell
cd data_warehousing\midterms\miniProjectPhase1
..\..\..\.venv\Scripts\python.exe etl_pipeline.py                # full run (needs MONGODB_URI)
..\..\..\.venv\Scripts\python.exe etl_pipeline.py --dry-run      # offline run via mongomock
..\..\..\.venv\Scripts\python.exe etl_pipeline.py --skip-mongo   # SQLite only
..\..\..\.venv\Scripts\python.exe etl_pipeline.py --full-refresh # rebuild targets
..\..\..\.venv\Scripts\python.exe olap_analysis.py               # OLAP operations -> olap_output.txt
```

`run_etl.bat` and `run_olap.bat` wrap the same commands. The MongoDB
connection is read from `MONGODB_URI` / `MONGODB_DATABASE` in the first
`.env` found in this folder, then `data_warehousing/.env`. Re-runs are
idempotent (`row_hash` UNIQUE in SQLite, `_id = supplier_name` upserts in
MongoDB).

| File | Purpose |
|---|---|
| `Grocery_Inventory_and_Sales_Dataset.csv` | Raw extract (990 rows × 16 cols) |
| `etl_pipeline.py` | Full ETL pipeline: extract → cleanse → SQLite + MongoDB load → analytics |
| `monthly_variance.sql` | CTE-based analytics (monthly variance, reorder pressure, stockout list) |
| `olap_analysis.py` | OLAP operations (roll-up, drill-down, slice, dice, rank, ABC) |
| `olap_output.txt` | OLAP run output |
| `miniProjectPhase1.ipynb` | Narrated walkthrough notebook |
| `Group_8_Report.md` | Analytical summary report |
| `cleaned_grocery_inventory.csv` | Cleaned output dataset |
| `quarantined_rows.csv` | Rows that failed validation — currently empty |
| `warehouse.db` | SQLite warehouse (star schema + views) |
| `supplier_audit_logs.json` | Local export of the MongoDB audit documents |
| `mongo_verification.json` | Sample document + collection count proof |
| `milestone1_log.txt` | ETL run log |
