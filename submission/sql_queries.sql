-- ============================================================================
-- Query 1 — Revenue Trend Analysis with Ranking (Required)
-- ============================================================================
-- 
-- Approach:
-- 1. Join fact_order_items with dim_products to get category information
-- 2. Calculate monthly revenue (price + freight_value) per category
-- 3. Filter to months with at least 10 transactions per category
-- 4. Rank categories within each month by revenue
-- 5. Identify top 5 categories by overall revenue
-- 6. Calculate month-over-month growth percentage using LAG window function
-- 7. Calculate 3-month rolling average revenue using windowing
-- ============================================================================

WITH monthly_category_revenue AS (
    -- Step 1: Calculate monthly revenue per category with transaction count
    SELECT 
        p.product_category_name,
        YEAR(f.order_date) AS year,
        MONTH(f.order_date) AS month,
        SUM(f.price + f.freight_value) AS monthly_revenue,
        COUNT(*) AS transaction_count
    FROM fact_order_items f
    INNER JOIN dim_products p ON f.product_key = p.product_key
    GROUP BY 
        p.product_category_name,
        YEAR(f.order_date),
        MONTH(f.order_date)
    HAVING COUNT(*) >= 10  -- Only include months with at least 10 transactions
),

top_categories AS (
    -- Step 2: Identify top 5 categories by overall revenue
    SELECT 
        product_category_name,
        SUM(monthly_revenue) AS total_revenue
    FROM monthly_category_revenue
    GROUP BY product_category_name
    ORDER BY total_revenue DESC
    LIMIT 5
),

ranked_revenue AS (
    -- Step 3: Calculate monthly rank for top categories
    SELECT 
        mcr.product_category_name,
        mcr.year,
        mcr.month,
        mcr.monthly_revenue,
        RANK() OVER (
            PARTITION BY mcr.year, mcr.month 
            ORDER BY mcr.monthly_revenue DESC
        ) AS monthly_rank
    FROM monthly_category_revenue mcr
    INNER JOIN top_categories tc 
        ON mcr.product_category_name = tc.product_category_name
)

-- Step 4: Calculate MoM growth and rolling average
SELECT 
    product_category_name,
    year,
    month,
    monthly_revenue,
    monthly_rank,
    -- Month-over-month growth percentage
    ROUND(
        ((monthly_revenue - LAG(monthly_revenue) OVER (
            PARTITION BY product_category_name 
            ORDER BY year, month
        )) / LAG(monthly_revenue) OVER (
            PARTITION BY product_category_name 
            ORDER BY year, month
        )) * 100, 
        2
    ) AS mom_growth_pct,
    -- 3-month rolling average
    ROUND(
        AVG(monthly_revenue) OVER (
            PARTITION BY product_category_name 
            ORDER BY year, month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_3m_avg_revenue
FROM ranked_revenue
ORDER BY 
    product_category_name, 
    year, 
    month;


-- ============================================================================
-- Query 2 — Seller Performance Scorecard (Stretch)
-- ============================================================================
--
-- Approach:
-- 1. Calculate base metrics per seller (late delivery rate, avg delivery vs estimate, 
--    total revenue, order count)
-- 2. Filter to sellers with at least 20 orders
-- 3. Calculate percentile ranks for each metric using PERCENT_RANK()
-- 4. Invert percentiles for "bad" metrics (late rate, delivery delay) so lower = better
-- 5. Calculate composite score as weighted average: 
--    - On-time delivery: 40%
--    - Delivery speed: 30%
--    - Revenue: 30%
-- 6. Rank sellers by composite score
-- ============================================================================

WITH seller_metrics AS (
    -- Step 1: Calculate base metrics per seller
    SELECT 
        s.seller_id,
        s.seller_state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.price + f.freight_value) AS total_revenue,
        -- Late delivery rate as percentage
        ROUND(
            (SUM(CASE WHEN f.is_late_delivery = TRUE THEN 1 ELSE 0 END) * 100.0) / 
            COUNT(CASE WHEN f.is_late_delivery IS NOT NULL THEN 1 END),
            2
        ) AS late_delivery_rate,
        -- Average days delivery vs estimate
        ROUND(
            AVG(f.days_delivery_vs_estimate),
            2
        ) AS avg_days_vs_estimate
    FROM fact_order_items f
    INNER JOIN dim_sellers s ON f.seller_key = s.seller_key
    WHERE f.days_delivery_vs_estimate IS NOT NULL  -- Only delivered orders
    GROUP BY s.seller_id, s.seller_state
    HAVING COUNT(DISTINCT f.order_id) >= 20  -- At least 20 orders
),

seller_percentiles AS (
    -- Step 2: Calculate percentile ranks for each metric
    SELECT 
        seller_id,
        seller_state,
        total_orders,
        total_revenue,
        late_delivery_rate,
        avg_days_vs_estimate,
        -- On-time percentile: invert late_delivery_rate so lower rate = higher percentile
        ROUND(
            (1 - PERCENT_RANK() OVER (ORDER BY late_delivery_rate)) * 100,
            2
        ) AS on_time_pctl,
        -- Speed percentile: invert avg_days_vs_estimate so lower days = higher percentile
        ROUND(
            (1 - PERCENT_RANK() OVER (ORDER BY avg_days_vs_estimate)) * 100,
            2
        ) AS speed_pctl,
        -- Revenue percentile: higher revenue = higher percentile
        ROUND(
            PERCENT_RANK() OVER (ORDER BY total_revenue) * 100,
            2
        ) AS revenue_pctl
    FROM seller_metrics
)

-- Step 3: Calculate composite score and final ranking
SELECT 
    seller_id,
    seller_state,
    total_orders,
    ROUND(total_revenue, 2) AS total_revenue,
    late_delivery_rate,
    avg_days_vs_estimate,
    on_time_pctl,
    speed_pctl,
    revenue_pctl,
    -- Composite score: weighted average of percentiles
    ROUND(
        (on_time_pctl * 0.40) + 
        (speed_pctl * 0.30) + 
        (revenue_pctl * 0.30),
        2
    ) AS composite_score,
    -- Overall rank based on composite score
    RANK() OVER (ORDER BY 
        (on_time_pctl * 0.40) + 
        (speed_pctl * 0.30) + 
        (revenue_pctl * 0.30) DESC
    ) AS overall_rank
FROM seller_percentiles
ORDER BY overall_rank;
