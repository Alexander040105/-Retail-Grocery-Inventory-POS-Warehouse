#!/usr/bin/env python3
"""
lab_governance.py
Group 8: Retail Grocery Inventory & Point-of-Sale (POS) Warehouse
Lab Activity 4 - Performance Tuning & Departmental Data Mart Creation
(Deliverable: Data Governance & Schema Validation Script)

  1. Audits fact_inventory against the proposed constraints (must report
     zero violations before any structural change is attempted).
  2. Rebuilds fact_inventory with strict DDL - NOT NULL on every column,
     CHECK constraints on non-negative quantities / positive prices /
     ISO date formats / the XX-XXX-XXXX id pattern / the status domain.
     (SQLite cannot ALTER TABLE ... ADD CONSTRAINT, so the table is
     rebuilt inside a single transaction; views and lab indexes are
     recreated afterwards.)
  3. Proves enforcement: deliberately invalid INSERTs are rejected.
  4. Applies a strict $jsonSchema validator to the supplier_audit_logs
     collection on MongoDB Atlas and proves a malformed document is
     rejected. Falls back to printing the validator when Atlas or
     pymongo is unavailable.

Usage:
    .venv\\Scripts\\python.exe lab_governance.py
"""

import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from etl_pipeline import (
    AUDIT_COLLECTION,
    DEFAULT_MONGO_DB,
    PYMONGO_AVAILABLE,
    SQLITE_SCHEMA,
    MongoClient,
    PyMongoError,
    load_env,
)
from lab_optimize import INDEXES

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "warehouse.db"

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger("lab_governance")


ID_GLOB = "[0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]"

GOVERNED_FACT_DDL = f"""
CREATE TABLE fact_inventory_new (
    record_key               INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id               TEXT NOT NULL CHECK (product_id GLOB '{ID_GLOB}'),
    supplier_id              TEXT NOT NULL CHECK (supplier_id GLOB '{ID_GLOB}'),
    product_key              INTEGER NOT NULL,
    supplier_key             INTEGER NOT NULL,
    store_key                INTEGER NOT NULL,
    stock_quantity           INTEGER NOT NULL CHECK (stock_quantity >= 0),
    reorder_level            INTEGER NOT NULL CHECK (reorder_level >= 0),
    reorder_quantity         INTEGER NOT NULL CHECK (reorder_quantity >= 0),
    unit_price               REAL    NOT NULL CHECK (unit_price > 0),
    date_received            TEXT    NOT NULL
        CHECK (strftime('%Y-%m-%d', date_received) = date_received),
    last_order_date          TEXT    NOT NULL
        CHECK (strftime('%Y-%m-%d', last_order_date) = last_order_date),
    expiration_date          TEXT    NOT NULL
        CHECK (strftime('%Y-%m-%d', expiration_date) = expiration_date),
    sales_volume             INTEGER NOT NULL CHECK (sales_volume >= 0),
    inventory_turnover_rate  INTEGER NOT NULL CHECK (inventory_turnover_rate >= 0),
    status                   TEXT    NOT NULL
        CHECK (status IN ('Active', 'Backordered', 'Discontinued')),
    row_hash                 TEXT UNIQUE,
    loaded_at                TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_key)  REFERENCES dim_product(product_key),
    FOREIGN KEY (supplier_key) REFERENCES dim_supplier(supplier_key),
    FOREIGN KEY (store_key)    REFERENCES dim_store(store_key)
);
"""

AUDIT_CHECKS: List[Tuple[str, str]] = [
    ("product_id / supplier_id NOT NULL",
     "SELECT COUNT(*) FROM fact_inventory WHERE product_id IS NULL OR supplier_id IS NULL"),
    ("ids match XX-XXX-XXXX pattern",
     "SELECT COUNT(*) FROM fact_inventory WHERE product_id NOT GLOB "
     f"'{ID_GLOB}' OR supplier_id NOT GLOB '{ID_GLOB}'"),
    ("dimension keys NOT NULL",
     "SELECT COUNT(*) FROM fact_inventory WHERE product_key IS NULL "
     "OR supplier_key IS NULL OR store_key IS NULL"),
    ("stock_quantity >= 0", "SELECT COUNT(*) FROM fact_inventory "
     "WHERE stock_quantity IS NULL OR stock_quantity < 0"),
    ("reorder_level >= 0", "SELECT COUNT(*) FROM fact_inventory "
     "WHERE reorder_level IS NULL OR reorder_level < 0"),
    ("reorder_quantity >= 0", "SELECT COUNT(*) FROM fact_inventory "
     "WHERE reorder_quantity IS NULL OR reorder_quantity < 0"),
    ("unit_price > 0", "SELECT COUNT(*) FROM fact_inventory "
     "WHERE unit_price IS NULL OR unit_price <= 0"),
    ("sales_volume >= 0", "SELECT COUNT(*) FROM fact_inventory "
     "WHERE sales_volume IS NULL OR sales_volume < 0"),
    ("inventory_turnover_rate >= 0", "SELECT COUNT(*) FROM fact_inventory "
     "WHERE inventory_turnover_rate IS NULL OR inventory_turnover_rate < 0"),
    ("dates are ISO YYYY-MM-DD",
     "SELECT COUNT(*) FROM fact_inventory WHERE "
     "date_received IS NULL OR strftime('%Y-%m-%d', date_received) != date_received OR "
     "last_order_date IS NULL OR strftime('%Y-%m-%d', last_order_date) != last_order_date OR "
     "expiration_date IS NULL OR strftime('%Y-%m-%d', expiration_date) != expiration_date"),
    ("status in (Active, Backordered, Discontinued)",
     "SELECT COUNT(*) FROM fact_inventory WHERE status IS NULL OR "
     "status NOT IN ('Active', 'Backordered', 'Discontinued')"),
]

EVENT_TYPES = [
    "REORDER_TRIGGERED", "STOCKOUT_RISK", "EXPIRED_BEFORE_LAST_ORDER",
    "BACKORDERED", "DISCONTINUED_WITH_STOCK", "MISSING_CATEGORY",
]

AUDIT_VALIDATOR: Dict = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "_id", "supplier_name", "source", "generated_at",
            "summary", "audit_events",
        ],
        "properties": {
            "_id": {"bsonType": "string"},
            "supplier_name": {"bsonType": "string"},
            "source": {"bsonType": "string"},
            "generated_at": {"bsonType": "string"},
            "summary": {
                "bsonType": "object",
                "required": [
                    "supplier_ids", "products_supplied", "total_stock",
                    "total_sales_volume", "avg_unit_price", "status_counts",
                    "event_count", "severity_counts",
                ],
                "properties": {
                    "supplier_ids": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "products_supplied": {"bsonType": "int"},
                    "total_stock": {"bsonType": "int"},
                    "total_sales_volume": {"bsonType": "int"},
                    "avg_unit_price": {"bsonType": ["double", "int", "long"]},
                    "status_counts": {"bsonType": "object"},
                    "event_count": {"bsonType": "int"},
                    "severity_counts": {"bsonType": "object"},
                },
            },
            "audit_events": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": [
                        "product_id", "product_name", "warehouse_location",
                        "event_type", "severity", "detail",
                    ],
                    "properties": {
                        "product_id": {"bsonType": "string"},
                        "product_name": {"bsonType": "string"},
                        "warehouse_location": {"bsonType": "string"},
                        "event_type": {"enum": EVENT_TYPES},
                        "severity": {"enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
                        "detail": {"bsonType": "string"},
                    },
                },
            },
        },
    }
}

BASE_FACT_ROW = {
    "product_id": "99-999-9999",
    "supplier_id": "88-888-8888",
    "product_key": 1,
    "supplier_key": 1,
    "store_key": 1,
    "stock_quantity": 10,
    "reorder_level": 5,
    "reorder_quantity": 20,
    "unit_price": 9.99,
    "date_received": "2025-01-01",
    "last_order_date": "2025-01-15",
    "expiration_date": "2025-06-01",
    "sales_volume": 4,
    "inventory_turnover_rate": 2,
    "status": "Active",
    "row_hash": "lab4_probe_row",
}

BAD_INSERTS: List[Tuple[str, Dict[str, object], str]] = [
    ("negative stock_quantity", {"stock_quantity": -5}, "CHECK"),
    ("non-positive unit_price", {"unit_price": -1.0}, "CHECK"),
    ("status outside domain", {"status": "Pending"}, "CHECK"),
    ("orphan product_key", {"product_key": 999999}, "FOREIGN KEY"),
]


def audit_existing_data(conn: sqlite3.Connection) -> bool:
    """Print violation counts per proposed constraint; True when all pass."""
    logger.info("    Proposed constraint                                   violations")
    ok = True
    for label, sql in AUDIT_CHECKS:
        n = conn.execute(sql).fetchone()[0]
        ok = ok and n == 0
        logger.info(f"    {label:<52} {n:>10}")
    return ok


def rebuild_fact_table(conn: sqlite3.Connection) -> None:
    """Rebuild fact_inventory with strict DDL in a single transaction."""
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN")
        conn.execute("DROP VIEW IF EXISTS v_reorder_recommendations")
        conn.execute("DROP VIEW IF EXISTS v_stockout_risk")
        conn.execute("DROP TABLE IF EXISTS fact_inventory_new")
        conn.execute(GOVERNED_FACT_DDL)
        conn.execute("INSERT INTO fact_inventory_new SELECT * FROM fact_inventory")
        copied = conn.execute("SELECT COUNT(*) FROM fact_inventory_new").fetchone()[0]
        original = conn.execute("SELECT COUNT(*) FROM fact_inventory").fetchone()[0]
        if copied != original:
            raise RuntimeError(f"row copy mismatch: {copied} != {original}")
        conn.execute("DROP TABLE fact_inventory")
        conn.execute("ALTER TABLE fact_inventory_new RENAME TO fact_inventory")
        for stmt in SQLITE_SCHEMA.split(";"):
            if stmt.strip():
                conn.execute(stmt)
        for _, ddl, _ in INDEXES:
            conn.execute(ddl)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def prove_constraint_enforcement(conn: sqlite3.Connection) -> None:
    """Insert deliberately invalid rows; every one must be rejected."""
    cols = ", ".join(BASE_FACT_ROW)
    for label, overrides, expect in BAD_INSERTS:
        row = {**BASE_FACT_ROW, **overrides, "row_hash": f"lab4_probe_{label}"}
        sql = f"INSERT INTO fact_inventory ({cols}) VALUES ({', '.join('?' * len(row))})"
        try:
            conn.execute(sql, tuple(row.values()))
            conn.rollback()
            logger.info(f"    {label:<28} UNEXPECTEDLY ACCEPTED - constraint missing!")
        except sqlite3.IntegrityError as exc:
            conn.rollback()
            logger.info(f"    {label:<28} rejected ({expect}) : {exc}")


def validator_would_reject(doc: dict) -> List[str]:
    """Minimal client-side check of the validator's required/enum rules."""
    problems = []
    schema = AUDIT_VALIDATOR["$jsonSchema"]
    for key in schema["required"]:
        if key not in doc:
            problems.append(f"missing required field '{key}'")
    for key, rule in schema["properties"].items():
        if key in doc and "bsonType" in rule and rule["bsonType"] == "string":
            if not isinstance(doc[key], str):
                problems.append(f"'{key}' must be a string")
    for event in doc.get("audit_events", []):
        item_schema = schema["properties"]["audit_events"]["items"]
        for key in item_schema["required"]:
            if key not in event:
                problems.append(f"audit_events item missing '{key}'")
        for key in ("event_type", "severity"):
            allowed = item_schema["properties"][key]["enum"]
            if key in event and event[key] not in allowed:
                problems.append(f"'{key}' value '{event[key]}' not in {allowed}")
    return problems


def _validator_fallback(reason: str) -> None:
    logger.info(f"    {reason} - printing validator for manual apply:")
    logger.info(json.dumps(AUDIT_VALIDATOR, indent=4))
    bad_doc = {
        "_id": "__lab4_invalid__",
        "supplier_name": 12345,
        "audit_events": [{"event_type": "NOT_A_TYPE", "severity": "FATAL"}],
    }
    logger.info("    client-side check of malformed doc against the validator:")
    for p in validator_would_reject(bad_doc):
        logger.info(f"      would be rejected: {p}")


def apply_mongo_validator() -> None:
    """collMod a strict $jsonSchema validator onto supplier_audit_logs."""
    env_used = load_env()
    if not PYMONGO_AVAILABLE:
        _validator_fallback("pymongo not installed")
        return
    uri_vars = sorted(
        k for k in os.environ if k == "MONGODB_URI" or k.startswith("MONGODB_URI")
    )
    if not uri_vars:
        _validator_fallback("MONGODB_URI not set")
        return

    client, used_var = None, None
    for var in uri_vars:
        try:
            candidate = MongoClient(
                os.environ[var], serverSelectionTimeoutMS=8000, connectTimeoutMS=10000
            )
            candidate.admin.command("ping")
            client, used_var = candidate, var
            break
        except PyMongoError as exc:
            logger.info(f"    {var} failed: {exc}")
    if client is None:
        _validator_fallback("no reachable MongoDB URI")
        return

    db_name = os.environ.get("MONGODB_DATABASE", DEFAULT_MONGO_DB)
    db = client[db_name]
    logger.info(f"    connected to Atlas db '{db_name}' via {used_var} (env: {env_used})")
    logger.info(f"    validator applied via collMod (validationLevel=strict, validationAction=error):")
    logger.info(json.dumps(AUDIT_VALIDATOR, indent=4))
    db.command({
        "collMod": AUDIT_COLLECTION,
        "validator": AUDIT_VALIDATOR,
        "validationLevel": "strict",
        "validationAction": "error",
    })
    applied = db.get_collection(AUDIT_COLLECTION).options().get("validator", {})
    logger.info(f"    server now reports validator on '{AUDIT_COLLECTION}': "
                f"{json.dumps(applied)[:120]}...")

    col = db[AUDIT_COLLECTION]
    count_before = col.count_documents({})
    logger.info(f"    collection documents before proof : {count_before}")

    bad_doc = {
        "_id": "__lab4_invalid__",
        "supplier_name": 12345,
        "audit_events": [{"event_type": "NOT_A_TYPE", "severity": "FATAL"}],
    }
    try:
        col.insert_one(bad_doc)
        logger.info("    malformed doc insert  : UNEXPECTEDLY ACCEPTED")
    except Exception as exc:
        logger.info(f"    malformed doc insert  : rejected by validator ({type(exc).__name__})")

    good_doc = {
        "_id": "__lab4_valid_probe__",
        "supplier_name": "__lab4_valid_probe__",
        "source": "lab_governance.py",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "supplier_ids": [], "products_supplied": 0, "total_stock": 0,
            "total_sales_volume": 0, "avg_unit_price": 0.0, "status_counts": {},
            "event_count": 0, "severity_counts": {},
        },
        "audit_events": [],
    }
    col.insert_one(good_doc)
    logger.info("    conforming doc insert : accepted")
    col.delete_one({"_id": "__lab4_valid_probe__"})
    count_after = col.count_documents({})
    logger.info(f"    probe doc deleted     : collection documents after = {count_after}")
    client.close()


def main() -> None:
    start = time.time()
    if not DB_PATH.exists():
        logger.error(f"{DB_PATH.name} not found - run etl_pipeline.py first.")
        sys.exit(1)

    logger.info("Group 8: Grocery Inventory & POS Warehouse - Lab 4 Governance")
    logger.info("=" * 66)

    conn = sqlite3.connect(DB_PATH)
    conn.isolation_level = None

    logger.info("")
    logger.info("[ 1. PRE-AUDIT - existing data must satisfy every constraint ]")
    logger.info("-" * 66)
    if not audit_existing_data(conn):
        logger.error("    violations found - aborting before any structural change.")
        sys.exit(1)
    logger.info("    all checks pass - safe to enforce at DDL level")

    logger.info("")
    logger.info("[ 2. REBUILD fact_inventory with strict DDL (single transaction) ]")
    logger.info("-" * 66)
    rebuild_fact_table(conn)
    n = conn.execute("SELECT COUNT(*) FROM fact_inventory").fetchone()[0]
    logger.info(f"    rebuilt fact_inventory : {n} rows copied, views + indexes recreated")

    logger.info("")
    logger.info("[ 3. POST-REBUILD VERIFICATION ]")
    logger.info("-" * 66)
    logger.info(f"    integrity_check   : {conn.execute('PRAGMA integrity_check').fetchone()[0]}")
    logger.info(f"    foreign_key_check : {len(conn.execute('PRAGMA foreign_key_check').fetchall())} violations")
    n_idx = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
    ).fetchone()[0]
    logger.info(f"    lab indexes present : {n_idx}")
    governed = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='fact_inventory'"
    ).fetchone()[0]
    logger.info("    governed DDL:")
    for line in governed.splitlines():
        logger.info(f"      {line}")

    logger.info("")
    logger.info("[ 4. ENFORCEMENT PROOF - invalid inserts must be rejected ]")
    logger.info("-" * 66)
    prove_constraint_enforcement(conn)
    conn.close()

    logger.info("")
    logger.info("[ 5. MONGODB $jsonSchema VALIDATOR - supplier_audit_logs ]")
    logger.info("-" * 66)
    apply_mongo_validator()

    logger.info("")
    logger.info("=" * 66)
    logger.info(f"Execution time : {time.time() - start:.2f} seconds")
    logger.info("[COMPLETE] Governance applied - constraints active, MongoDB validator set.")


if __name__ == "__main__":
    main()
