#!/usr/bin/env python3
"""
etl_pipeline.py
Group 8: Retail Grocery Inventory & Point-of-Sale (POS) Warehouse
Mini Project Phase 1 - Data Warehousing

Extracts the messy grocery inventory/POS extract
(Grocery_Inventory_and_Sales_Dataset.csv), cleanses it (currency symbols,
mixed-width M/D/YYYY dates, misspelled 'Catagory' header), quarantines
unrecoverable rows, and loads:

  - SQLite warehouse  : warehouse.db
        dim_product, dim_supplier, dim_store, fact_inventory
        + v_reorder_recommendations / v_stockout_risk views
  - MongoDB Atlas     : supplier_audit_logs collection
        one audit document per supplier, with embedded audit events
        derived deterministically from the cleaned data.

Then runs the monthly sales variance CTE queries in monthly_variance.sql
and writes a milestone1_log.txt report.

Usage:
    python etl_pipeline.py                 # incremental run (MongoDB Atlas)
    python etl_pipeline.py --dry-run       # use mongomock instead of Atlas
    python etl_pipeline.py --skip-mongo    # SQLite only
    python etl_pipeline.py --full-refresh  # rebuild targets from scratch

MongoDB config: MONGODB_URI / MONGODB_DATABASE are read from the first
.env found in this folder, then in data_warehousing/.env.
"""

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    PYMONGO_AVAILABLE = True
except ImportError:  # pragma: no cover
    MongoClient = PyMongoError = None
    PYMONGO_AVAILABLE = False

try:
    import mongomock

    MONGOMOCK_AVAILABLE = True
except ImportError:  # pragma: no cover
    mongomock = None
    MONGOMOCK_AVAILABLE = False

SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = SCRIPT_DIR / "Grocery_Inventory_and_Sales_Dataset.csv"
DB_PATH = SCRIPT_DIR / "warehouse.db"
SQL_PATH = SCRIPT_DIR / "monthly_variance.sql"
CLEANED_CSV = SCRIPT_DIR / "cleaned_grocery_inventory.csv"
QUARANTINE_CSV = SCRIPT_DIR / "quarantined_rows.csv"
AUDIT_EXPORT_JSON = SCRIPT_DIR / "supplier_audit_logs.json"
MONGO_VERIFY_JSON = SCRIPT_DIR / "mongo_verification.json"
SUMMARY_PATH = SCRIPT_DIR / "milestone1_log.txt"
ENV_CANDIDATES = [SCRIPT_DIR / ".env", SCRIPT_DIR.parent.parent / ".env"]

DEFAULT_MONGO_DB = "grocery_warehouse"
AUDIT_COLLECTION = "supplier_audit_logs"


logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger("etl_pipeline")

def load_env() -> Optional[Path]:
    """Load KEY=VALUE pairs from the first existing .env candidate.

    Returns the path that was loaded (or None). Does not override variables
    that are already present in os.environ.
    """
    for path in ENV_CANDIDATES:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key, value = key.strip(), value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                if key and os.environ.get(key) is None:
                    os.environ[key] = value
        return path
    return None

def extract_csv(path: Path = CSV_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    logger.info(f"[EXTRACT] Read {len(df)} rows x {len(df.columns)} cols from {path.name}")
    return df


# ---------------------------------------------------------------------------
# Transform / Cleanse
# ---------------------------------------------------------------------------
COLUMN_RENAME = {
    "Product_ID": "product_id",
    "Product_Name": "product_name",
    "Catagory": "category",  # source header is misspelled
    "Supplier_ID": "supplier_id",
    "Supplier_Name": "supplier_name",
    "Stock_Quantity": "stock_quantity",
    "Reorder_Level": "reorder_level",
    "Reorder_Quantity": "reorder_quantity",
    "Unit_Price": "unit_price",
    "Date_Received": "date_received",
    "Last_Order_Date": "last_order_date",
    "Expiration_Date": "expiration_date",
    "Warehouse_Location": "warehouse_location",
    "Sales_Volume": "sales_volume",
    "Inventory_Turnover_Rate": "inventory_turnover_rate",
    "Status": "status",
}

DATE_COLS = ["date_received", "last_order_date", "expiration_date"]
ID_PATTERN = r"^\d{2}-\d{3}-\d{4}$"


def clean_unit_price(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Strip currency symbols / commas / whitespace from unit_price -> float."""
    raw = df["unit_price"].astype(str)
    cleaned = raw.str.replace(r"[$,\s]", "", regex=True)
    n_fixed = int((raw != cleaned).sum())
    df["unit_price"] = pd.to_numeric(cleaned, errors="coerce")
    return df, n_fixed


def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse the mixed-width M/D/YYYY date columns to ISO date strings."""
    for col in DATE_COLS:
        df[col] = pd.to_datetime(df[col], format="mixed", dayfirst=False, errors="coerce")
    return df


def transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    metrics = {
        "raw_rows": len(df),
        "price_symbols_cleaned": 0,
        "missing_category_filled": 0,
        "duplicates_dropped": 0,
        "quarantined": 0,
        "cleaned_rows": 0,
    }

    df = df.rename(columns=COLUMN_RENAME)
    for col in df.columns:
        df[col] = df[col].astype("string").str.strip()

    df, metrics["price_symbols_cleaned"] = clean_unit_price(df)

    df["category_was_missing"] = df["category"].isna()
    metrics["missing_category_filled"] = int(df["category_was_missing"].sum())
    df["category"] = df["category"].fillna("Uncategorized")

    df = clean_dates(df)


    for col in [
        "stock_quantity",
        "reorder_level",
        "reorder_quantity",
        "sales_volume",
        "inventory_turnover_rate",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["product_id_ok"] = df["product_id"].str.match(ID_PATTERN, na=False)
    df["supplier_id_ok"] = df["supplier_id"].str.match(ID_PATTERN, na=False)

    reasons = pd.Series("", index=df.index)
    rules = {
        "invalid_product_id": ~df["product_id_ok"],
        "invalid_supplier_id": ~df["supplier_id_ok"],
        "unparseable_unit_price": df["unit_price"].isna(),
        "unparseable_last_order_date": df["last_order_date"].isna(),
        "negative_stock": df["stock_quantity"].fillna(0) < 0,
        "negative_sales_volume": df["sales_volume"].fillna(0) < 0,
    }
    for reason, mask in rules.items():
        reasons = reasons.where(~mask, reasons + reason + ";")

    quarantine = df[reasons != ""].copy()
    quarantine["quarantine_reason"] = reasons[reasons != ""].str.rstrip(";")
    df = df[reasons == ""].copy()
    metrics["quarantined"] = len(quarantine)

    # Drop exact duplicates after normalization
    before = len(df)
    df = df.drop_duplicates(
        subset=[c for c in df.columns if c not in ("product_id_ok", "supplier_id_ok")]
    )
    metrics["duplicates_dropped"] = before - len(df)

    df["row_hash"] = df.apply(_compute_row_hash, axis=1)

    for col in [
        "stock_quantity",
        "reorder_level",
        "reorder_quantity",
        "sales_volume",
        "inventory_turnover_rate",
    ]:
        df[col] = df[col].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)

    metrics["cleaned_rows"] = len(df)
    logger.info(
        f"[TRANSFORM] {metrics['cleaned_rows']}/{metrics['raw_rows']} rows clean; "
        f"{metrics['quarantined']} quarantined; {metrics['duplicates_dropped']} dupes; "
        f"{metrics['price_symbols_cleaned']} currency symbols stripped; "
        f"{metrics['missing_category_filled']} missing categories filled."
    )
    return df, quarantine, metrics


def _compute_row_hash(row: pd.Series) -> str:
    """Deterministic SHA-256 hash of a cleaned row (bookkeeping cols skipped)."""
    skip = {"row_hash", "product_id_ok", "supplier_id_ok", "category_was_missing"}
    parts = ["grocery"]
    for col in sorted(c for c in row.index if c not in skip):
        value = row[col]
        if pd.isna(value):
            parts.append("")
        elif isinstance(value, pd.Timestamp):
            parts.append(value.strftime("%Y-%m-%d"))
        elif isinstance(value, float):
            parts.append(f"{value:.4f}")
        else:
            parts.append(str(value).strip().lower())
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

def build_star_schema(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split the cleaned frame into dimension frames + a fact frame.

    The raw product_id / supplier_id are unique per row (record-level
    identifiers), so the business entities - product_name and
    supplier_name - become the dimension natural keys. The raw IDs are
    kept on the fact as degenerate references.
    """
    dim_product = (
        df[["product_name", "category"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_supplier = (
        df[["supplier_name"]].drop_duplicates().reset_index(drop=True)
    )
    dim_store = (
        df[["warehouse_location"]].drop_duplicates().reset_index(drop=True)
    )

    fact = df.copy()
    for col in DATE_COLS:
        fact[col] = fact[col].dt.strftime("%Y-%m-%d")

    return dim_product, dim_supplier, dim_store, fact


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_product (
    product_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    UNIQUE (product_name, category)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    warehouse_location  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_inventory (
    record_key               INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id               TEXT NOT NULL,   -- degenerate: source SKU ref
    supplier_id              TEXT NOT NULL,   -- degenerate: source contract ref
    product_key              INTEGER NOT NULL,
    supplier_key             INTEGER NOT NULL,
    store_key                INTEGER NOT NULL,
    stock_quantity           INTEGER,
    reorder_level            INTEGER,
    reorder_quantity         INTEGER,
    unit_price               REAL,
    date_received            TEXT,
    last_order_date          TEXT,
    expiration_date          TEXT,
    sales_volume             INTEGER,
    inventory_turnover_rate  INTEGER,
    status                   TEXT,
    row_hash                 TEXT UNIQUE,
    loaded_at                TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_key)  REFERENCES dim_product(product_key),
    FOREIGN KEY (supplier_key) REFERENCES dim_supplier(supplier_key),
    FOREIGN KEY (store_key)    REFERENCES dim_store(store_key)
);

-- Automated replenishment: items at or below reorder level, prioritized.
CREATE VIEW IF NOT EXISTS v_reorder_recommendations AS
SELECT
    f.record_key,
    f.product_id,
    p.product_name,
    p.category,
    s.supplier_name,
    st.warehouse_location,
    f.stock_quantity,
    f.reorder_level,
    f.reorder_quantity          AS suggested_order_qty,
    f.unit_price,
    f.sales_volume,
    f.status,
    CASE
        WHEN f.stock_quantity <= f.reorder_level * 0.5 THEN 'CRITICAL'
        WHEN f.stock_quantity <= f.reorder_level       THEN 'HIGH'
        ELSE 'NORMAL'
    END                         AS replenishment_priority
FROM fact_inventory f
JOIN dim_product  p  ON p.product_key  = f.product_key
JOIN dim_supplier s  ON s.supplier_key = f.supplier_key
JOIN dim_store    st ON st.store_key   = f.store_key
WHERE f.stock_quantity <= f.reorder_level
  AND f.status != 'Discontinued';

-- Stockout prevention: days of cover = on-hand / daily sales velocity.
CREATE VIEW IF NOT EXISTS v_stockout_risk AS
SELECT
    f.record_key,
    f.product_id,
    p.product_name,
    p.category,
    st.warehouse_location,
    f.stock_quantity,
    f.sales_volume,
    ROUND(f.stock_quantity * 30.0 / NULLIF(f.sales_volume, 0), 1)
        AS days_of_cover,
    CASE
        WHEN f.stock_quantity * 30.0 / NULLIF(f.sales_volume, 0) < 7  THEN 'CRITICAL'
        WHEN f.stock_quantity * 30.0 / NULLIF(f.sales_volume, 0) < 15 THEN 'HIGH'
        WHEN f.stock_quantity * 30.0 / NULLIF(f.sales_volume, 0) < 30 THEN 'WATCH'
        ELSE 'LOW'
    END AS stockout_risk
FROM fact_inventory f
JOIN dim_product p  ON p.product_key = f.product_key
JOIN dim_store   st ON st.store_key  = f.store_key
WHERE f.status != 'Discontinued';
"""


def load_sqlite(
    db_path: Path,
    dim_product: pd.DataFrame,
    dim_supplier: pd.DataFrame,
    dim_store: pd.DataFrame,
    fact: pd.DataFrame,
    full_refresh: bool = False,
) -> Dict[str, int]:
    """Create schema + views and insert dims/facts idempotently."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if full_refresh:
        cur.executescript(
            """
            DROP VIEW  IF EXISTS v_reorder_recommendations;
            DROP VIEW  IF EXISTS v_stockout_risk;
            DROP TABLE IF EXISTS fact_inventory;
            DROP TABLE IF EXISTS dim_product;
            DROP TABLE IF EXISTS dim_supplier;
            DROP TABLE IF EXISTS dim_store;
            """
        )

    cur.executescript(SQLITE_SCHEMA)

    cur.executemany(
        "INSERT OR IGNORE INTO dim_product (product_name, category) VALUES (?, ?)",
        dim_product[["product_name", "category"]].itertuples(index=False),
    )
    cur.executemany(
        "INSERT OR IGNORE INTO dim_supplier (supplier_name) VALUES (?)",
        [(r,) for r in dim_supplier["supplier_name"]],
    )
    cur.executemany(
        "INSERT OR IGNORE INTO dim_store (warehouse_location) VALUES (?)",
        [(r,) for r in dim_store["warehouse_location"]],
    )

    product_map = {
        (name, cat): key
        for name, cat, key in cur.execute(
            "SELECT product_name, category, product_key FROM dim_product"
        )
    }
    supplier_map = dict(cur.execute("SELECT supplier_name, supplier_key FROM dim_supplier"))
    store_map = dict(cur.execute("SELECT warehouse_location, store_key FROM dim_store"))

    fact_rows = [
        (
            r["product_id"],
            r["supplier_id"],
            product_map[(r["product_name"], r["category"])],
            supplier_map[r["supplier_name"]],
            store_map[r["warehouse_location"]],
            int(r["stock_quantity"]),
            int(r["reorder_level"]),
            int(r["reorder_quantity"]),
            float(r["unit_price"]),
            r["date_received"],
            r["last_order_date"],
            r["expiration_date"],
            int(r["sales_volume"]),
            int(r["inventory_turnover_rate"]),
            r["status"],
            r["row_hash"],
        )
        for _, r in fact.iterrows()
    ]
    cur.executemany(
        """
        INSERT OR IGNORE INTO fact_inventory
        (product_id, supplier_id, product_key, supplier_key, store_key,
         stock_quantity, reorder_level, reorder_quantity, unit_price,
         date_received, last_order_date, expiration_date, sales_volume,
         inventory_turnover_rate, status, row_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        fact_rows,
    )
    conn.commit()

    counts = {
        "fact_inventory": cur.execute("SELECT COUNT(*) FROM fact_inventory").fetchone()[0],
        "dim_product": cur.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0],
        "dim_supplier": cur.execute("SELECT COUNT(*) FROM dim_supplier").fetchone()[0],
        "dim_store": cur.execute("SELECT COUNT(*) FROM dim_store").fetchone()[0],
        "reorder_flags": cur.execute(
            "SELECT COUNT(*) FROM v_reorder_recommendations"
        ).fetchone()[0],
        "stockout_critical": cur.execute(
            "SELECT COUNT(*) FROM v_stockout_risk WHERE stockout_risk = 'CRITICAL'"
        ).fetchone()[0],
    }
    conn.close()
    logger.info(
        f"[LOAD - SQLITE] fact_inventory={counts['fact_inventory']}, "
        f"dim_product={counts['dim_product']}, dim_supplier={counts['dim_supplier']}, "
        f"dim_store={counts['dim_store']}"
    )
    return counts

def build_supplier_audit_docs(df: pd.DataFrame) -> List[dict]:
    """Derive one audit document per supplier entity from the cleaned data.

    Suppliers are keyed by supplier_name - the source supplier_id values are
    record-level (unique per row) and are listed under summary.supplier_ids.
    Each document embeds deterministic audit events flagged during cleansing
    and business-rule evaluation (reorder triggers, expiry anomalies, etc.).
    """
    docs: List[dict] = []
    generated_at = datetime.now(timezone.utc).isoformat()

    for supplier_name, grp in df.groupby("supplier_name"):
        events: List[dict] = []
        for _, r in grp.iterrows():
            base = {
                "product_id": r["product_id"],
                "product_name": r["product_name"],
                "warehouse_location": r["warehouse_location"],
            }

            if r["stock_quantity"] <= r["reorder_level"]:
                events.append(
                    {
                        **base,
                        "event_type": "REORDER_TRIGGERED",
                        "severity": "HIGH",
                        "detail": (
                            f"stock {int(r['stock_quantity'])} <= reorder level "
                            f"{int(r['reorder_level'])}; suggested order qty "
                            f"{int(r['reorder_quantity'])}"
                        ),
                    }
                )
            if r["sales_volume"] > 0 and r["stock_quantity"] < 0.5 * r["sales_volume"]:
                events.append(
                    {
                        **base,
                        "event_type": "STOCKOUT_RISK",
                        "severity": "CRITICAL",
                        "detail": (
                            f"on-hand {int(r['stock_quantity'])} covers less than "
                            f"half of sales volume {int(r['sales_volume'])}"
                        ),
                    }
                )
            if r["expiration_date"] < r["last_order_date"]:
                events.append(
                    {
                        **base,
                        "event_type": "EXPIRED_BEFORE_LAST_ORDER",
                        "severity": "MEDIUM",
                        "detail": (
                            f"expiration {r['expiration_date']:%Y-%m-%d} precedes "
                            f"last order {r['last_order_date']:%Y-%m-%d}"
                        ),
                    }
                )
            if r["status"] == "Backordered":
                events.append(
                    {
                        **base,
                        "event_type": "BACKORDERED",
                        "severity": "HIGH",
                        "detail": "product is backordered",
                    }
                )
            if r["status"] == "Discontinued" and r["stock_quantity"] > 0:
                events.append(
                    {
                        **base,
                        "event_type": "DISCONTINUED_WITH_STOCK",
                        "severity": "LOW",
                        "detail": (
                            f"{int(r['stock_quantity'])} units on hand for a "
                            "discontinued product"
                        ),
                    }
                )
            if r["category_was_missing"]:
                events.append(
                    {
                        **base,
                        "event_type": "MISSING_CATEGORY",
                        "severity": "LOW",
                        "detail": "category missing in source; imputed 'Uncategorized'",
                    }
                )

        severity_counts = {}
        for e in events:
            severity_counts[e["severity"]] = severity_counts.get(e["severity"], 0) + 1

        docs.append(
            {
                "_id": supplier_name,
                "supplier_name": supplier_name,
                "source": "etl_pipeline.py",
                "generated_at": generated_at,
                "summary": {
                    "supplier_ids": sorted(grp["supplier_id"].tolist()),
                    "products_supplied": int(len(grp)),
                    "total_stock": int(grp["stock_quantity"].sum()),
                    "total_sales_volume": int(grp["sales_volume"].sum()),
                    "avg_unit_price": round(float(grp["unit_price"].mean()), 2),
                    "status_counts": {
                        k: int(v) for k, v in grp["status"].value_counts().items()
                    },
                    "event_count": len(events),
                    "severity_counts": severity_counts,
                },
                "audit_events": events,
            }
        )

    logger.info(
        f"[TRANSFORM] Built {len(docs)} supplier audit documents "
        f"({sum(d['summary']['event_count'] for d in docs)} events)."
    )
    return docs


_DRY_RUN_CLIENT = None


def _get_mongo_collection(dry_run: bool):
    """Return (collection, client) for Atlas or mongomock.

    In dry-run mode a module-level mongomock client is reused so the
    in-memory 'server' persists across calls within one run.
    """
    global _DRY_RUN_CLIENT
    db_name = os.environ.get("MONGODB_DATABASE", DEFAULT_MONGO_DB)

    if dry_run:
        if not MONGOMOCK_AVAILABLE:
            logger.error("[LOAD - MONGODB] mongomock not installed.")
            return None, None
        if _DRY_RUN_CLIENT is None:
            _DRY_RUN_CLIENT = mongomock.MongoClient()
        return _DRY_RUN_CLIENT[db_name][AUDIT_COLLECTION], _DRY_RUN_CLIENT

    if not PYMONGO_AVAILABLE:
        logger.error("[LOAD - MONGODB] pymongo not installed: pip install 'pymongo[srv]'")
        return None, None

    uri_vars = sorted(
        k for k in os.environ if k == "MONGODB_URI" or k.startswith("MONGODB_URI")
    )
    if not uri_vars:
        logger.error(
            "[LOAD - MONGODB] MONGODB_URI not set. Add it to miniProjectPhase1/.env "
            "or data_warehousing/.env, or run with --dry-run / --skip-mongo."
        )
        return None, None

    for var in uri_vars:
        uri = os.environ[var]
        try:
            client = MongoClient(
                uri, serverSelectionTimeoutMS=8000, connectTimeoutMS=10000
            )
            client.admin.command("ping")
            logger.info(f"[LOAD - MONGODB] Connected via {var}")
            return client[db_name][AUDIT_COLLECTION], client
        except PyMongoError as exc:
            logger.warning(f"[LOAD - MONGODB] {var} failed: {exc}")

    logger.error("[LOAD - MONGODB] No reachable MongoDB URI found.")
    return None, None


def load_mongodb(
    docs: List[dict], dry_run: bool = False, full_refresh: bool = False
) -> int:
    """Upsert supplier audit docs (_id = supplier_id) -> idempotent re-runs."""
    col, client = _get_mongo_collection(dry_run)
    if col is None:
        return 0

    target = "mongomock" if dry_run else "Atlas"
    if full_refresh:
        col.delete_many({})

    upserted = 0
    for doc in docs:
        result = col.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        if result.upserted_id is not None or result.modified_count:
            upserted += 1

    total = col.count_documents({})
    client.close()
    logger.info(
        f"[LOAD - MONGODB] Upserted {upserted} docs into '{AUDIT_COLLECTION}' "
        f"({target}...); collection total = {total}."
    )
    return total


def export_audit_json(docs: List[dict], path: Path = AUDIT_EXPORT_JSON) -> None:
    """Local copy of the audit docs (offline proof of the MongoDB payload)."""
    with path.open("w", encoding="utf-8") as handle:
        json.dump(docs, handle, indent=2, default=str)
    logger.info(f"[EXPORT] Wrote {len(docs)} audit docs to {path.name}")


def export_mongo_verification(dry_run: bool) -> None:
    """Write a sample audit document + collection stats as verification proof."""
    col, client = _get_mongo_collection(dry_run)
    if col is None:
        return
    sample = col.find_one({})
    if sample and isinstance(sample.get("audit_events"), list):
        sample["audit_events"] = sample["audit_events"][:3]
        sample["audit_events_truncated"] = True
    payload = {
        "collection": AUDIT_COLLECTION,
        "backend": "mongomock" if dry_run else "mongodb_atlas",
        "document_count": col.count_documents({}),
        "sample_document": sample,
    }
    client.close()
    with MONGO_VERIFY_JSON.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
    logger.info(f"[VERIFY - MONGODB] Exported sample doc to {MONGO_VERIFY_JSON.name}")


def run_analytics(db_path: Path, sql_path: Path = SQL_PATH) -> List[str]:
    """Execute each named query in monthly_variance.sql; return report lines."""
    if not sql_path.exists():
        logger.warning(f"[ANALYTICS] {sql_path.name} not found; skipping.")
        return []

    # Queries are separated by sentinel comments:  -- >>> query_name
    sql_text = sql_path.read_text(encoding="utf-8")
    queries: List[Tuple[str, str]] = []
    name, buf = None, []
    for line in sql_text.splitlines():
        if line.strip().startswith("-- >>>"):
            if name and buf:
                queries.append((name, "\n".join(buf)))
            name = line.split(">>>", 1)[1].strip()
            buf = []
        elif name:
            buf.append(line)
    if name and buf:
        queries.append((name, "\n".join(buf)))

    conn = sqlite3.connect(db_path)
    report: List[str] = []
    for qname, qsql in queries:
        qsql = qsql.strip().rstrip(";")
        if not qsql:
            continue
        try:
            cur = conn.execute(qsql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            report.append(f"  {qname}: ERROR - {exc}")
            continue

        report.append(f"  {qname} ({len(rows)} rows)")
        report.append("    " + " | ".join(cols))
        for row in rows[:8]:
            report.append("    " + " | ".join(str(v) for v in row))
        if len(rows) > 8:
            report.append(f"    ... ({len(rows) - 8} more rows)")
        report.append("")
    conn.close()
    return report


def verify_sqlite(db_path: Path) -> Dict[str, object]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    out = {
        "fact_rows": cur.execute("SELECT COUNT(*) FROM fact_inventory").fetchone()[0],
        "date_range": cur.execute(
            "SELECT MIN(last_order_date), MAX(last_order_date) FROM fact_inventory"
        ).fetchone(),
        "reorder_flags": cur.execute(
            "SELECT COUNT(*) FROM v_reorder_recommendations"
        ).fetchone()[0],
    }
    conn.close()
    return out

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Group 8 grocery warehouse ETL pipeline")
    p.add_argument("--dry-run", action="store_true", help="use mongomock instead of Atlas")
    p.add_argument("--skip-mongo", action="store_true", help="skip the MongoDB load")
    p.add_argument("--full-refresh", action="store_true", help="rebuild all targets")
    return p.parse_args()


def run() -> None:
    args = parse_args()
    start = time.time()
    env_used = load_env()

    # 1. Extract + transform
    raw = extract_csv()
    clean, quarantine, metrics = transform(raw)

    clean_out = clean.copy()
    for col in DATE_COLS:
        clean_out[col] = clean_out[col].dt.strftime("%Y-%m-%d")
    clean_out.to_csv(CLEANED_CSV, index=False)
    quarantine.to_csv(QUARANTINE_CSV, index=False)

    # 2. SQLite warehouse
    dim_product, dim_supplier, dim_store, fact = build_star_schema(clean)
    sqlite_counts = load_sqlite(
        DB_PATH, dim_product, dim_supplier, dim_store, fact,
        full_refresh=args.full_refresh,
    )

    # 3. MongoDB supplier audit logs
    audit_docs = build_supplier_audit_docs(clean)
    export_audit_json(audit_docs)
    mongo_total = 0
    if not args.skip_mongo:
        mongo_total = load_mongodb(
            audit_docs, dry_run=args.dry_run, full_refresh=args.full_refresh
        )
        export_mongo_verification(dry_run=args.dry_run)

    # 4. Analytics (monthly sales variance CTEs)
    analytics_report = run_analytics(DB_PATH)
    sqlite_v = verify_sqlite(DB_PATH)

    elapsed = time.time() - start
    summary = [
        "Group 8: Grocery Inventory & POS Warehouse - Pipeline Summary",
        "=" * 62,
        "Extraction / Cleansing:",
        f"  - Raw rows                 : {metrics['raw_rows']}",
        f"  - Currency symbols cleaned : {metrics['price_symbols_cleaned']}",
        f"  - Missing category filled  : {metrics['missing_category_filled']}",
        f"  - Duplicates dropped       : {metrics['duplicates_dropped']}",
        f"  - Quarantined rows         : {metrics['quarantined']} -> {QUARANTINE_CSV.name}",
        f"  - Cleaned rows             : {metrics['cleaned_rows']} -> {CLEANED_CSV.name}",
        "",
        "SQLite Load (warehouse.db):",
        f"  - fact_inventory rows      : {sqlite_counts['fact_inventory']}",
        f"  - dim_product / supplier / store : "
        f"{sqlite_counts['dim_product']} / {sqlite_counts['dim_supplier']} / {sqlite_counts['dim_store']}",
        f"  - last_order_date range    : {sqlite_v['date_range'][0]} to {sqlite_v['date_range'][1]}",
        f"  - reorder recommendations  : {sqlite_counts['reorder_flags']} flagged",
        f"  - CRITICAL stockout risk   : {sqlite_counts['stockout_critical']} items",
        "",
        "MongoDB Load (supplier_audit_logs):",
        f"  - backend                  : {'mongomock (dry-run)' if args.dry_run else 'MongoDB Atlas'}",
        f"  - documents in collection  : {mongo_total}",
        f"  - local export             : {AUDIT_EXPORT_JSON.name}",
        f"  - env file used            : {env_used or 'none (MONGODB_URI from environment)'}",
        "",
        "Analytics - monthly_variance.sql:",
        *analytics_report,
        "Verification artifacts:",
        f"  - SQLite DB                : {DB_PATH}",
        f"  - Audit JSON export        : {AUDIT_EXPORT_JSON}",
        f"  - Mongo verification       : {MONGO_VERIFY_JSON}",
        f"  - Summary report           : {SUMMARY_PATH}",
        "",
        f"Execution time              : {elapsed:.2f} seconds",
    ]
    SUMMARY_PATH.write_text("\n".join(summary), encoding="utf-8")
    logger.info("\n".join(summary))
    logger.info(f"[COMPLETE] Pipeline finished in {elapsed:.2f}s.")


if __name__ == "__main__":
    run()
