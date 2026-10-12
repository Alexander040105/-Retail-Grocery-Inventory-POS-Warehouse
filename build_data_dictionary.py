#!/usr/bin/env python3
"""
build_data_dictionary.py
Group 8: Retail Grocery Inventory & Point-of-Sale (POS) Warehouse
Lab Activity 4 - Performance Tuning & Departmental Data Mart Creation
(Deliverable: Technical Data Dictionary)

Generates Data_Dictionary.md and Data_Dictionary.html by introspecting
the real schema of warehouse.db and marketing_datamart.db (PRAGMA
table_info / foreign_key_list / index_list + sqlite_master DDL) and
merging curated business definitions and source-field mappings.

Every table, view, and mart table is documented with column name, data
type, constraints, business definition, and source mapping. The MongoDB
supplier_audit_logs document schema is listed from the $jsonSchema
validator applied by lab_governance.py.

Usage:
    .venv\\Scripts\\python.exe build_data_dictionary.py
"""

import html
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from lab_governance import AUDIT_VALIDATOR

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "warehouse.db"
MART_PATH = SCRIPT_DIR / "marketing_datamart.db"
MD_PATH = SCRIPT_DIR / "Data_Dictionary.md"
HTML_PATH = SCRIPT_DIR / "Data_Dictionary.html"

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger("build_data_dictionary")

CSV = "Grocery_Inventory_and_Sales_Dataset.csv"

DEFS: Dict[str, Dict[str, str]] = {
    "dim_product": {
        "product_key": "Surrogate primary key for the product dimension.",
        "product_name": "Business name of the stocked product.",
        "category": "Merchandising category (misspelled 'Catagory' in the source; "
                    "one null imputed to 'Uncategorized').",
    },
    "dim_supplier": {
        "supplier_key": "Surrogate primary key for the supplier dimension.",
        "supplier_name": "Business name of the supplier entity.",
    },
    "dim_store": {
        "store_key": "Surrogate primary key for the store/warehouse dimension.",
        "warehouse_location": "Free-text branch/warehouse address; unique per "
                              "record so this dimension is 1:1 with the fact.",
    },
    "fact_inventory": {
        "record_key": "Surrogate primary key per inventory/sales record.",
        "product_id": "Degenerate dimension: source SKU reference "
                      "(XX-XXX-XXXX pattern enforced by CHECK).",
        "supplier_id": "Degenerate dimension: source supplier contract reference "
                       "(XX-XXX-XXXX pattern enforced by CHECK).",
        "product_key": "Foreign key -> dim_product.product_key.",
        "supplier_key": "Foreign key -> dim_supplier.supplier_key.",
        "store_key": "Foreign key -> dim_store.store_key.",
        "stock_quantity": "Units on hand; CHECK >= 0.",
        "reorder_level": "Reorder threshold; replenishment flags when "
                         "stock_quantity <= reorder_level; CHECK >= 0.",
        "reorder_quantity": "Standard replenishment order size; CHECK >= 0.",
        "unit_price": "Unit selling price in dollars; currency symbols stripped "
                      "during cleansing; CHECK > 0.",
        "date_received": "Date stock was received (ISO YYYY-MM-DD; CHECK).",
        "last_order_date": "Most recent order date; used as the sales month for "
                           "all revenue/variance analysis (ISO; CHECK).",
        "expiration_date": "Perishable expiry date (ISO; CHECK).",
        "sales_volume": "Units sold in the period; treated as monthly sales "
                        "velocity; CHECK >= 0.",
        "inventory_turnover_rate": "Source-provided turnover ratio; CHECK >= 0.",
        "status": "Lifecycle status: Active / Backordered / Discontinued "
                  "(domain enforced by CHECK).",
        "row_hash": "SHA-256 of the cleansed record; UNIQUE makes ETL "
                    "re-runs idempotent.",
        "loaded_at": "Warehouse load timestamp (DEFAULT CURRENT_TIMESTAMP).",
    },
    "v_reorder_recommendations": {
        "record_key": "Fact record reference.",
        "product_id": "Source SKU reference.",
        "product_name": "Product needing replenishment.",
        "category": "Merchandising category.",
        "supplier_name": "Supplier to reorder from.",
        "warehouse_location": "Site holding the low stock.",
        "stock_quantity": "Units on hand.",
        "reorder_level": "Reorder threshold.",
        "suggested_order_qty": "reorder_quantity carried as the order "
                               "suggestion.",
        "unit_price": "Unit price (for spend estimation).",
        "sales_volume": "Monthly sales velocity.",
        "status": "Lifecycle status (Discontinued rows excluded).",
        "replenishment_priority": "CRITICAL when stock <= 50% of reorder "
                                  "level, HIGH at/below it, else NORMAL.",
    },
    "v_stockout_risk": {
        "record_key": "Fact record reference.",
        "product_id": "Source SKU reference.",
        "product_name": "At-risk product.",
        "category": "Merchandising category.",
        "warehouse_location": "Site holding the stock.",
        "stock_quantity": "Units on hand.",
        "sales_volume": "Monthly sales velocity.",
        "days_of_cover": "stock_quantity / (sales_volume / 30) - days until "
                         "stockout at current velocity.",
        "stockout_risk": "CRITICAL <7d, HIGH <15d, WATCH <30d, else LOW "
                         "(Discontinued rows excluded).",
    },
    "mart_monthly_category_sales": {
        "sales_month": "Calendar month of demand (YYYY-MM, from "
                       "last_order_date).",
        "category": "Merchandising category.",
        "units_sold": "Total units sold that month in the category.",
        "active_items": "Distinct products with sales that month.",
    },
    "mart_product_demand": {
        "product_name": "Product business name.",
        "category": "Merchandising category.",
        "total_units_sold": "Lifetime units sold (13-month window).",
        "months_active": "Distinct months with recorded sales.",
        "avg_monthly_units": "total_units_sold / months_active - demand "
                             "velocity.",
        "last_order_month": "Most recent month with sales.",
        "demand_share_pct": "Share of total units sold across the catalogue "
                            "(0-100).",
    },
    "mart_location_category_demand": {
        "warehouse_location": "Branch/warehouse address.",
        "category": "Merchandising category.",
        "units_sold": "Units of the category sold at the site.",
        "items_stocked": "Fact records (stocked items) for the pair.",
    },
    "mart_category_availability": {
        "category": "Merchandising category.",
        "status": "Stock posture bucket (Active/Backordered/Discontinued).",
        "item_count": "Fact records in the bucket.",
        "units_on_hand": "Units physically on hand in the bucket.",
    },
}

SRC: Dict[str, Dict[str, str]] = {
    "dim_product": {
        "product_key": "generated (AUTOINCREMENT)",
        "product_name": f"{CSV} :: Product_Name",
        "category": f"{CSV} :: Catagory (renamed; null -> 'Uncategorized')",
    },
    "dim_supplier": {
        "supplier_key": "generated (AUTOINCREMENT)",
        "supplier_name": f"{CSV} :: Supplier_Name",
    },
    "dim_store": {
        "store_key": "generated (AUTOINCREMENT)",
        "warehouse_location": f"{CSV} :: Warehouse_Location",
    },
    "fact_inventory": {
        "record_key": "generated (AUTOINCREMENT)",
        "product_id": f"{CSV} :: Product_ID",
        "supplier_id": f"{CSV} :: Supplier_ID",
        "product_key": "derived via dim_product lookup",
        "supplier_key": "derived via dim_supplier lookup",
        "store_key": "derived via dim_store lookup",
        "stock_quantity": f"{CSV} :: Stock_Quantity",
        "reorder_level": f"{CSV} :: Reorder_Level",
        "reorder_quantity": f"{CSV} :: Reorder_Quantity",
        "unit_price": f"{CSV} :: Unit_Price (stripped '$', commas, spaces -> REAL)",
        "date_received": f"{CSV} :: Date_Received (M/D/YYYY -> ISO)",
        "last_order_date": f"{CSV} :: Last_Order_Date (M/D/YYYY -> ISO)",
        "expiration_date": f"{CSV} :: Expiration_Date (M/D/YYYY -> ISO)",
        "sales_volume": f"{CSV} :: Sales_Volume",
        "inventory_turnover_rate": f"{CSV} :: Inventory_Turnover_Rate",
        "status": f"{CSV} :: Status",
        "row_hash": "derived (SHA-256 of cleansed row)",
        "loaded_at": "generated at load",
    },
    "v_reorder_recommendations": {},
    "v_stockout_risk": {},
    "mart_monthly_category_sales": {
        "sales_month": "fact_inventory.last_order_date (strftime %Y-%m)",
        "category": "dim_product.category",
        "units_sold": "SUM(fact_inventory.sales_volume)",
        "active_items": "COUNT(DISTINCT fact_inventory.product_key)",
    },
    "mart_product_demand": {
        "product_name": "dim_product.product_name",
        "category": "dim_product.category",
        "total_units_sold": "SUM(fact_inventory.sales_volume)",
        "months_active": "COUNT(DISTINCT sales_month)",
        "avg_monthly_units": "total_units_sold / months_active",
        "last_order_month": "MAX(sales_month)",
        "demand_share_pct": "total_units_sold / total units x 100",
    },
    "mart_location_category_demand": {
        "warehouse_location": "dim_store.warehouse_location",
        "category": "dim_product.category",
        "units_sold": "SUM(fact_inventory.sales_volume)",
        "items_stocked": "COUNT(*) fact rows",
    },
    "mart_category_availability": {
        "category": "dim_product.category",
        "status": "fact_inventory.status",
        "item_count": "COUNT(*) fact rows",
        "units_on_hand": "SUM(fact_inventory.stock_quantity)",
    },
}


def columns_of(conn: sqlite3.Connection, obj: str) -> List[dict]:
    """PRAGMA table_info + fk_list for one table/view."""
    fks = {r[3]: f"{r[2]}.{r[4]}" for r in conn.execute(f"PRAGMA foreign_key_list({obj})")}
    cols = []
    for cid, name, ctype, notnull, dflt, pk in conn.execute(f"PRAGMA table_info({obj})"):
        cons = []
        if pk:
            cons.append("PK")
        if notnull:
            cons.append("NOT NULL")
        if name in fks:
            cons.append(f"FK -> {fks[name]}")
        if dflt is not None:
            cons.append(f"DEFAULT {dflt}")
        cols.append({
            "name": name,
            "type": ctype or "-",
            "constraints": ", ".join(cons) or "-",
        })
    return cols


def collect_objects(conn: sqlite3.Connection, db_label: str) -> List[dict]:
    objects = []
    for typ in ("table", "view"):
        for name, sql in conn.execute(
            f"SELECT name, sql FROM sqlite_master WHERE type='{typ}' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ):
            objects.append({
                "db": db_label, "kind": typ, "name": name, "ddl": sql,
                "columns": columns_of(conn, name),
            })
    return objects


def collect_indexes(conn: sqlite3.Connection) -> List[tuple]:
    return conn.execute(
        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' "
        "AND sql IS NOT NULL ORDER BY name"
    ).fetchall()


def to_markdown(objects: List[dict], indexes: Dict[str, List[tuple]]) -> str:
    lines = [
        "# Data Dictionary — Group 8 Grocery Inventory & POS Warehouse",
        "",
        f"Generated {datetime.now():%Y-%m-%d %H:%M} by `build_data_dictionary.py` "
        "(introspected from the live schemas).",
        "",
        "Databases: `warehouse.db` (core star schema, governed per Lab 4) and "
        "`marketing_datamart.db` (restricted marketing mart - no financial "
        "margin data).",
        "",
    ]
    for obj in objects:
        lines.append(f"## `{obj['db']}` — {obj['kind']} `{obj['name']}`")
        lines.append("")
        lines.append("| Column | Type | Constraints | Business definition | Source |")
        lines.append("|---|---|---|---|---|")
        defs, srcs = DEFS.get(obj["name"], {}), SRC.get(obj["name"], {})
        for col in obj["columns"]:
            lines.append(
                f"| `{col['name']}` | {col['type']} | {col['constraints']} "
                f"| {defs.get(col['name'], '-')} | {srcs.get(col['name'], '-')} |"
            )
        lines.append("")
        lines.append("<details><summary>DDL</summary>")
        lines.append("")
        lines.append("```sql")
        lines.append(obj["ddl"])
        lines.append("```")
        lines.append("</details>")
        lines.append("")
    for db_label, idx in indexes.items():
        lines.append(f"## `{db_label}` — indexes")
        lines.append("")
        lines.append("| Index | Table | DDL |")
        lines.append("|---|---|---|")
        for name, tbl, sql in idx:
            lines.append(f"| `{name}` | `{tbl}` | `{sql}` |")
        lines.append("")
    lines.append("## MongoDB — collection `supplier_audit_logs`")
    lines.append("")
    lines.append("Document schema enforced by a strict `$jsonSchema` validator "
                 "(applied by `lab_governance.py`, validationLevel=strict, "
                 "validationAction=error):")
    lines.append("")
    lines.append("| Field | Type | Rule |")
    lines.append("|---|---|---|")
    schema = AUDIT_VALIDATOR["$jsonSchema"]
    props = schema["properties"]
    for field in schema["required"]:
        rule = props.get(field, {})
        lines.append(f"| `{field}` | {rule.get('bsonType', '-')} | required |")
    for field, rule in props.items():
        if field not in schema["required"]:
            lines.append(f"| `{field}` | {rule.get('bsonType', '-')} | optional |")
    lines.append("")
    lines.append("`summary` sub-fields (all required): supplier_ids (array of "
                 "source supplier ids), products_supplied, total_stock, "
                 "total_sales_volume, avg_unit_price, status_counts, "
                 "event_count, severity_counts.")
    lines.append("")
    lines.append("`audit_events` items (all required): product_id, product_name, "
                 "warehouse_location, event_type (enum: REORDER_TRIGGERED, "
                 "STOCKOUT_RISK, EXPIRED_BEFORE_LAST_ORDER, BACKORDERED, "
                 "DISCONTINUED_WITH_STOCK, MISSING_CATEGORY), severity (enum: "
                 "LOW, MEDIUM, HIGH, CRITICAL), detail.")
    lines.append("")
    return "\n".join(lines)


def to_html(md_objects: List[dict], indexes: Dict[str, List[tuple]]) -> str:
    parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Data Dictionary - Group 8 Grocery Warehouse</title>",
        "<style>",
        "body{font-family:Segoe UI,Arial,sans-serif;margin:2em;color:#222}",
        "h1{border-bottom:3px solid #2c5f8a;padding-bottom:.3em}",
        "h2{margin-top:1.8em;color:#2c5f8a}",
        "table{border-collapse:collapse;width:100%;font-size:.9em}",
        "th{background:#2c5f8a;color:#fff;text-align:left}",
        "th,td{border:1px solid #bbb;padding:5px 8px;vertical-align:top}",
        "tr:nth-child(even){background:#f4f7fa}",
        "code,pre{background:#f0f0f0;font-family:Consolas,monospace}",
        "pre{padding:.8em;overflow-x:auto;font-size:.8em}",
        "@media print{h2{page-break-before:always} pre{white-space:pre-wrap}}",
        "</style></head><body>",
        f"<h1>Data Dictionary — Group 8 Grocery Inventory & POS Warehouse</h1>",
        f"<p>Generated {datetime.now():%Y-%m-%d %H:%M} by "
        "<code>build_data_dictionary.py</code>. Print to PDF via the browser "
        "(Ctrl+P) for submission.</p>",
    ]
    for obj in md_objects:
        parts.append(f"<h2><code>{html.escape(obj['db'])}</code> — "
                     f"{obj['kind']} <code>{html.escape(obj['name'])}</code></h2>")
        parts.append("<table><tr><th>Column</th><th>Type</th><th>Constraints</th>"
                     "<th>Business definition</th><th>Source</th></tr>")
        defs, srcs = DEFS.get(obj["name"], {}), SRC.get(obj["name"], {})
        for col in obj["columns"]:
            parts.append(
                f"<tr><td><code>{html.escape(col['name'])}</code></td>"
                f"<td>{html.escape(col['type'])}</td>"
                f"<td>{html.escape(col['constraints'])}</td>"
                f"<td>{html.escape(defs.get(col['name'], '-'))}</td>"
                f"<td>{html.escape(srcs.get(col['name'], '-'))}</td></tr>"
            )
        parts.append("</table>")
        parts.append(f"<details><summary>DDL</summary><pre>{html.escape(obj['ddl'])}</pre></details>")
    for db_label, idx in indexes.items():
        parts.append(f"<h2><code>{html.escape(db_label)}</code> — indexes</h2>")
        parts.append("<table><tr><th>Index</th><th>Table</th><th>DDL</th></tr>")
        for name, tbl, sql in idx:
            parts.append(
                f"<tr><td><code>{html.escape(name)}</code></td>"
                f"<td><code>{html.escape(tbl)}</code></td>"
                f"<td><code>{html.escape(sql)}</code></td></tr>"
            )
        parts.append("</table>")
    schema = AUDIT_VALIDATOR["$jsonSchema"]
    parts.append("<h2>MongoDB — collection <code>supplier_audit_logs</code></h2>")
    parts.append("<p>Document schema enforced by a strict <code>$jsonSchema</code> "
                 "validator (validationLevel=strict, validationAction=error):</p>")
    parts.append("<table><tr><th>Field</th><th>Type</th><th>Rule</th></tr>")
    for field in schema["required"]:
        rule = schema["properties"].get(field, {})
        bt = rule.get("bsonType", "-")
        parts.append(f"<tr><td><code>{field}</code></td><td>{bt}</td><td>required</td></tr>")
    parts.append("</table>")
    parts.append("<p><code>summary</code>: supplier_ids (array), products_supplied, "
                 "total_stock, total_sales_volume, avg_unit_price, status_counts, "
                 "event_count, severity_counts — all required.<br>"
                 "<code>audit_events</code> items: product_id, product_name, "
                 "warehouse_location, event_type (enum), severity (enum), "
                 "detail — all required.</p>")
    parts.append("</body></html>")
    return "\n".join(parts)


def main() -> None:
    for path in (DB_PATH, MART_PATH):
        if not path.exists():
            logger.error(f"{path.name} not found - run the pipeline/mart scripts first.")
            sys.exit(1)

    wh = sqlite3.connect(DB_PATH)
    mart = sqlite3.connect(MART_PATH)
    objects = collect_objects(wh, "warehouse.db") + collect_objects(mart, "marketing_datamart.db")
    indexes = {
        "warehouse.db": collect_indexes(wh),
        "marketing_datamart.db": collect_indexes(mart),
    }
    wh.close()
    mart.close()

    MD_PATH.write_text(to_markdown(objects, indexes), encoding="utf-8")
    HTML_PATH.write_text(to_html(objects, indexes), encoding="utf-8")
    logger.info(f"[DICTIONARY] wrote {MD_PATH.name} ({MD_PATH.stat().st_size:,} bytes)")
    logger.info(f"[DICTIONARY] wrote {HTML_PATH.name} ({HTML_PATH.stat().st_size:,} bytes)")
    logger.info(f"[DICTIONARY] {len(objects)} objects documented, "
                f"{sum(len(v) for v in indexes.values())} indexes listed")


if __name__ == "__main__":
    main()
