-- >>> monthly_sales_variance
WITH monthly_sales AS (
    SELECT
        strftime('%Y-%m', last_order_date)      AS sales_month,
        COUNT(*)                                AS products_sold,
        SUM(sales_volume)                       AS units_sold,
        ROUND(SUM(sales_volume * unit_price),2) AS revenue
    FROM fact_inventory
    GROUP BY sales_month
),
monthly_variance AS (
    SELECT
        sales_month,
        products_sold,
        units_sold,
        revenue,
        LAG(revenue) OVER (ORDER BY sales_month)          AS prev_month_revenue,
        ROUND(revenue - LAG(revenue) OVER (ORDER BY sales_month), 2)
                                                          AS mom_variance,
        ROUND(
            100.0 * (revenue - LAG(revenue) OVER (ORDER BY sales_month))
                  / LAG(revenue) OVER (ORDER BY sales_month), 1)
                                                          AS mom_variance_pct
    FROM monthly_sales
)
SELECT * FROM monthly_variance ORDER BY sales_month;

-- >>> category_monthly_variance
WITH category_monthly AS (
    SELECT
        p.category,
        strftime('%Y-%m', f.last_order_date)      AS sales_month,
        ROUND(SUM(f.sales_volume * f.unit_price),2) AS revenue,
        SUM(f.sales_volume)                       AS units_sold
    FROM fact_inventory f
    JOIN dim_product p ON p.product_key = f.product_key
    GROUP BY p.category, sales_month
),
category_variance AS (
    SELECT
        category,
        sales_month,
        units_sold,
        revenue,
        ROUND(revenue - LAG(revenue) OVER (
                  PARTITION BY category ORDER BY sales_month), 2) AS mom_variance,
        ROUND(
            100.0 * (revenue - LAG(revenue) OVER (
                         PARTITION BY category ORDER BY sales_month))
                  / NULLIF(LAG(revenue) OVER (
                         PARTITION BY category ORDER BY sales_month), 0), 1)
                                                              AS mom_variance_pct
    FROM category_monthly
)
SELECT * FROM category_variance ORDER BY category, sales_month;

-- >>> monthly_reorder_pressure
WITH monthly_flagged AS (
    SELECT
        strftime('%Y-%m', last_order_date)  AS sales_month,
        COUNT(*)                            AS products_at_risk,
        SUM(reorder_quantity)               AS units_to_reorder,
        ROUND(SUM(reorder_quantity * unit_price), 2)
                                            AS estimated_reorder_spend
    FROM fact_inventory
    WHERE stock_quantity <= reorder_level
      AND status != 'Discontinued'
    GROUP BY sales_month
)
SELECT
    sales_month,
    products_at_risk,
    units_to_reorder,
    estimated_reorder_spend,
    SUM(products_at_risk) OVER (ORDER BY sales_month) AS cumulative_at_risk
FROM monthly_flagged
ORDER BY sales_month;

-- >>> top_stockout_risks
SELECT
    product_id,
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
LIMIT 15;
