# Data Dictionary — Group 8 Grocery Inventory & POS Warehouse

Generated 2026-10-10 18:07 by `build_data_dictionary.py` (introspected from the live schemas).

Databases: `warehouse.db` (core star schema, governed per Lab 4) and `marketing_datamart.db` (restricted marketing mart - no financial margin data).

## `warehouse.db` — table `dim_product`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `product_key` | INTEGER | PK | Surrogate primary key for the product dimension. | generated (AUTOINCREMENT) |
| `product_name` | TEXT | NOT NULL | Business name of the stocked product. | Grocery_Inventory_and_Sales_Dataset.csv :: Product_Name |
| `category` | TEXT | NOT NULL | Merchandising category (misspelled 'Catagory' in the source; one null imputed to 'Uncategorized'). | Grocery_Inventory_and_Sales_Dataset.csv :: Catagory (renamed; null -> 'Uncategorized') |

<details><summary>DDL</summary>

```sql
CREATE TABLE dim_product (
    product_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    UNIQUE (product_name, category)
)
```
</details>

## `warehouse.db` — table `dim_store`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `store_key` | INTEGER | PK | Surrogate primary key for the store/warehouse dimension. | generated (AUTOINCREMENT) |
| `warehouse_location` | TEXT | NOT NULL | Free-text branch/warehouse address; unique per record so this dimension is 1:1 with the fact. | Grocery_Inventory_and_Sales_Dataset.csv :: Warehouse_Location |

<details><summary>DDL</summary>

```sql
CREATE TABLE dim_store (
    store_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    warehouse_location  TEXT NOT NULL UNIQUE
)
```
</details>

## `warehouse.db` — table `dim_supplier`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `supplier_key` | INTEGER | PK | Surrogate primary key for the supplier dimension. | generated (AUTOINCREMENT) |
| `supplier_name` | TEXT | NOT NULL | Business name of the supplier entity. | Grocery_Inventory_and_Sales_Dataset.csv :: Supplier_Name |

<details><summary>DDL</summary>

```sql
CREATE TABLE dim_supplier (
    supplier_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name  TEXT NOT NULL UNIQUE
)
```
</details>

## `warehouse.db` — table `fact_inventory`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `record_key` | INTEGER | PK | Surrogate primary key per inventory/sales record. | generated (AUTOINCREMENT) |
| `product_id` | TEXT | NOT NULL | Degenerate dimension: source SKU reference (XX-XXX-XXXX pattern enforced by CHECK). | Grocery_Inventory_and_Sales_Dataset.csv :: Product_ID |
| `supplier_id` | TEXT | NOT NULL | Degenerate dimension: source supplier contract reference (XX-XXX-XXXX pattern enforced by CHECK). | Grocery_Inventory_and_Sales_Dataset.csv :: Supplier_ID |
| `product_key` | INTEGER | NOT NULL, FK -> dim_product.product_key | Foreign key -> dim_product.product_key. | derived via dim_product lookup |
| `supplier_key` | INTEGER | NOT NULL, FK -> dim_supplier.supplier_key | Foreign key -> dim_supplier.supplier_key. | derived via dim_supplier lookup |
| `store_key` | INTEGER | NOT NULL, FK -> dim_store.store_key | Foreign key -> dim_store.store_key. | derived via dim_store lookup |
| `stock_quantity` | INTEGER | NOT NULL | Units on hand; CHECK >= 0. | Grocery_Inventory_and_Sales_Dataset.csv :: Stock_Quantity |
| `reorder_level` | INTEGER | NOT NULL | Reorder threshold; replenishment flags when stock_quantity <= reorder_level; CHECK >= 0. | Grocery_Inventory_and_Sales_Dataset.csv :: Reorder_Level |
| `reorder_quantity` | INTEGER | NOT NULL | Standard replenishment order size; CHECK >= 0. | Grocery_Inventory_and_Sales_Dataset.csv :: Reorder_Quantity |
| `unit_price` | REAL | NOT NULL | Unit selling price in dollars; currency symbols stripped during cleansing; CHECK > 0. | Grocery_Inventory_and_Sales_Dataset.csv :: Unit_Price (stripped '$', commas, spaces -> REAL) |
| `date_received` | TEXT | NOT NULL | Date stock was received (ISO YYYY-MM-DD; CHECK). | Grocery_Inventory_and_Sales_Dataset.csv :: Date_Received (M/D/YYYY -> ISO) |
| `last_order_date` | TEXT | NOT NULL | Most recent order date; used as the sales month for all revenue/variance analysis (ISO; CHECK). | Grocery_Inventory_and_Sales_Dataset.csv :: Last_Order_Date (M/D/YYYY -> ISO) |
| `expiration_date` | TEXT | NOT NULL | Perishable expiry date (ISO; CHECK). | Grocery_Inventory_and_Sales_Dataset.csv :: Expiration_Date (M/D/YYYY -> ISO) |
| `sales_volume` | INTEGER | NOT NULL | Units sold in the period; treated as monthly sales velocity; CHECK >= 0. | Grocery_Inventory_and_Sales_Dataset.csv :: Sales_Volume |
| `inventory_turnover_rate` | INTEGER | NOT NULL | Source-provided turnover ratio; CHECK >= 0. | Grocery_Inventory_and_Sales_Dataset.csv :: Inventory_Turnover_Rate |
| `status` | TEXT | NOT NULL | Lifecycle status: Active / Backordered / Discontinued (domain enforced by CHECK). | Grocery_Inventory_and_Sales_Dataset.csv :: Status |
| `row_hash` | TEXT | - | SHA-256 of the cleansed record; UNIQUE makes ETL re-runs idempotent. | derived (SHA-256 of cleansed row) |
| `loaded_at` | TEXT | DEFAULT CURRENT_TIMESTAMP | Warehouse load timestamp (DEFAULT CURRENT_TIMESTAMP). | generated at load |

<details><summary>DDL</summary>

```sql
CREATE TABLE "fact_inventory" (
    record_key               INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id               TEXT NOT NULL CHECK (product_id GLOB '[0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'),
    supplier_id              TEXT NOT NULL CHECK (supplier_id GLOB '[0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'),
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
)
```
</details>

## `warehouse.db` — view `v_reorder_recommendations`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `record_key` | INTEGER | - | Fact record reference. | - |
| `product_id` | TEXT | - | Source SKU reference. | - |
| `product_name` | TEXT | - | Product needing replenishment. | - |
| `category` | TEXT | - | Merchandising category. | - |
| `supplier_name` | TEXT | - | Supplier to reorder from. | - |
| `warehouse_location` | TEXT | - | Site holding the low stock. | - |
| `stock_quantity` | INTEGER | - | Units on hand. | - |
| `reorder_level` | INTEGER | - | Reorder threshold. | - |
| `suggested_order_qty` | INTEGER | - | reorder_quantity carried as the order suggestion. | - |
| `unit_price` | REAL | - | Unit price (for spend estimation). | - |
| `sales_volume` | INTEGER | - | Monthly sales velocity. | - |
| `status` | TEXT | - | Lifecycle status (Discontinued rows excluded). | - |
| `replenishment_priority` | - | - | CRITICAL when stock <= 50% of reorder level, HIGH at/below it, else NORMAL. | - |

<details><summary>DDL</summary>

```sql
CREATE VIEW v_reorder_recommendations AS
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
  AND f.status != 'Discontinued'
```
</details>

## `warehouse.db` — view `v_stockout_risk`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `record_key` | INTEGER | - | Fact record reference. | - |
| `product_id` | TEXT | - | Source SKU reference. | - |
| `product_name` | TEXT | - | At-risk product. | - |
| `category` | TEXT | - | Merchandising category. | - |
| `warehouse_location` | TEXT | - | Site holding the stock. | - |
| `stock_quantity` | INTEGER | - | Units on hand. | - |
| `sales_volume` | INTEGER | - | Monthly sales velocity. | - |
| `days_of_cover` | - | - | stock_quantity / (sales_volume / 30) - days until stockout at current velocity. | - |
| `stockout_risk` | - | - | CRITICAL <7d, HIGH <15d, WATCH <30d, else LOW (Discontinued rows excluded). | - |

<details><summary>DDL</summary>

```sql
CREATE VIEW v_stockout_risk AS
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
WHERE f.status != 'Discontinued'
```
</details>

## `marketing_datamart.db` — table `mart_category_availability`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `category` | TEXT | PK, NOT NULL | Merchandising category. | dim_product.category |
| `status` | TEXT | PK, NOT NULL | Stock posture bucket (Active/Backordered/Discontinued). | fact_inventory.status |
| `item_count` | INTEGER | NOT NULL | Fact records in the bucket. | COUNT(*) fact rows |
| `units_on_hand` | INTEGER | NOT NULL | Units physically on hand in the bucket. | SUM(fact_inventory.stock_quantity) |

<details><summary>DDL</summary>

```sql
CREATE TABLE mart_category_availability (
    category      TEXT    NOT NULL,
    status        TEXT    NOT NULL
        CHECK (status IN ('Active', 'Backordered', 'Discontinued')),
    item_count    INTEGER NOT NULL CHECK (item_count >= 0),
    units_on_hand INTEGER NOT NULL CHECK (units_on_hand >= 0),
    PRIMARY KEY (category, status)
)
```
</details>

## `marketing_datamart.db` — table `mart_location_category_demand`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `warehouse_location` | TEXT | PK, NOT NULL | Branch/warehouse address. | dim_store.warehouse_location |
| `category` | TEXT | PK, NOT NULL | Merchandising category. | dim_product.category |
| `units_sold` | INTEGER | NOT NULL | Units of the category sold at the site. | SUM(fact_inventory.sales_volume) |
| `items_stocked` | INTEGER | NOT NULL | Fact records (stocked items) for the pair. | COUNT(*) fact rows |

<details><summary>DDL</summary>

```sql
CREATE TABLE mart_location_category_demand (
    warehouse_location TEXT    NOT NULL,
    category           TEXT    NOT NULL,
    units_sold         INTEGER NOT NULL CHECK (units_sold >= 0),
    items_stocked      INTEGER NOT NULL CHECK (items_stocked >= 0),
    PRIMARY KEY (warehouse_location, category)
)
```
</details>

## `marketing_datamart.db` — table `mart_monthly_category_sales`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `sales_month` | TEXT | PK, NOT NULL | Calendar month of demand (YYYY-MM, from last_order_date). | fact_inventory.last_order_date (strftime %Y-%m) |
| `category` | TEXT | PK, NOT NULL | Merchandising category. | dim_product.category |
| `units_sold` | INTEGER | NOT NULL | Total units sold that month in the category. | SUM(fact_inventory.sales_volume) |
| `active_items` | INTEGER | NOT NULL | Distinct products with sales that month. | COUNT(DISTINCT fact_inventory.product_key) |

<details><summary>DDL</summary>

```sql
CREATE TABLE mart_monthly_category_sales (
    sales_month   TEXT    NOT NULL CHECK (sales_month GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'),
    category      TEXT    NOT NULL,
    units_sold    INTEGER NOT NULL CHECK (units_sold >= 0),
    active_items  INTEGER NOT NULL CHECK (active_items >= 0),
    PRIMARY KEY (sales_month, category)
)
```
</details>

## `marketing_datamart.db` — table `mart_product_demand`

| Column | Type | Constraints | Business definition | Source |
|---|---|---|---|---|
| `product_name` | TEXT | PK, NOT NULL | Product business name. | dim_product.product_name |
| `category` | TEXT | PK, NOT NULL | Merchandising category. | dim_product.category |
| `total_units_sold` | INTEGER | NOT NULL | Lifetime units sold (13-month window). | SUM(fact_inventory.sales_volume) |
| `months_active` | INTEGER | NOT NULL | Distinct months with recorded sales. | COUNT(DISTINCT sales_month) |
| `avg_monthly_units` | REAL | NOT NULL | total_units_sold / months_active - demand velocity. | total_units_sold / months_active |
| `last_order_month` | TEXT | NOT NULL | Most recent month with sales. | MAX(sales_month) |
| `demand_share_pct` | REAL | NOT NULL | Share of total units sold across the catalogue (0-100). | total_units_sold / total units x 100 |

<details><summary>DDL</summary>

```sql
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
)
```
</details>

## `warehouse.db` — indexes

| Index | Table | DDL |
|---|---|---|
| `idx_dim_product_category` | `dim_product` | `CREATE INDEX idx_dim_product_category ON dim_product(category)` |
| `idx_fact_last_order_date` | `fact_inventory` | `CREATE INDEX idx_fact_last_order_date
    ON fact_inventory(last_order_date)` |
| `idx_fact_product_key` | `fact_inventory` | `CREATE INDEX idx_fact_product_key
    ON fact_inventory(product_key)` |
| `idx_fact_status_orderdate` | `fact_inventory` | `CREATE INDEX idx_fact_status_orderdate
    ON fact_inventory(status, last_order_date)` |
| `idx_fact_store_key` | `fact_inventory` | `CREATE INDEX idx_fact_store_key
    ON fact_inventory(store_key)` |
| `idx_fact_supplier_key` | `fact_inventory` | `CREATE INDEX idx_fact_supplier_key
    ON fact_inventory(supplier_key)` |

## `marketing_datamart.db` — indexes

| Index | Table | DDL |
|---|---|---|

## MongoDB — collection `supplier_audit_logs`

Document schema enforced by a strict `$jsonSchema` validator (applied by `lab_governance.py`, validationLevel=strict, validationAction=error):

| Field | Type | Rule |
|---|---|---|
| `_id` | string | required |
| `supplier_name` | string | required |
| `source` | string | required |
| `generated_at` | string | required |
| `summary` | object | required |
| `audit_events` | array | required |

`summary` sub-fields (all required): supplier_ids (array of source supplier ids), products_supplied, total_stock, total_sales_volume, avg_unit_price, status_counts, event_count, severity_counts.

`audit_events` items (all required): product_id, product_name, warehouse_location, event_type (enum: REORDER_TRIGGERED, STOCKOUT_RISK, EXPIRED_BEFORE_LAST_ORDER, BACKORDERED, DISCONTINUED_WITH_STOCK, MISSING_CATEGORY), severity (enum: LOW, MEDIUM, HIGH, CRITICAL), detail.
