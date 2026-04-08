WITH cat_monthly AS (
    select 
        p.product_category_name,
        YEAR(f.order_date) AS year,
        MONTH(f.order_date) AS month,
        SUM(f.price + f.freight_value) AS monthly_revenue,
        COUNT(*) AS txn_count
    from fact_order_items f
    inner join dim_products p ON f.product_key = p.product_key
    GROUP BY 
        p.product_category_name,
        YEAR(f.order_date),
        MONTH(f.order_date)
    HAVING COUNT(*) >= 10
),

top5 AS (
    SELECT 
        product_category_name,
        SUM(monthly_revenue) AS total_rev
    from cat_monthly
    GROUP BY product_category_name
    ORDER BY total_rev DESC
    LIMIT 5
),

rnk AS(
    SELECT 
        cm.product_category_name,
        cm.year,
        cm.month,
        cm.monthly_revenue,
        RANK() OVER(
            PARTITION BY cm.year, cm.month 
            ORDER BY cm.monthly_revenue DESC
        ) AS monthly_rank
    from cat_monthly cm
    INNER join top5 t 
        ON cm.product_category_name = t.product_category_name
)

SELECT 
    product_category_name,
    year,
    month,
    monthly_revenue,
    monthly_rank,
    ROUND(
        ((monthly_revenue - LAG(monthly_revenue) OVER(
            partition BY product_category_name 
            ORDER BY year, month
        )) / LAG(monthly_revenue) OVER (
            partition BY product_category_name 
            ORDER BY year,month
        )) * 100, 
        2
    ) AS mom_growth_pct,
    ROUND(
        AVG(monthly_revenue) OVER(
            PARTITION BY product_category_name 
            ORDER BY year,month
            rows between 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_3m_avg_revenue
from rnk
ORDER BY 
    product_category_name, 
    year, 
    month
