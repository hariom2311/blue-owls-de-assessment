WITH base_stats AS (
    select 
        s.seller_id,
        s.seller_state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.price + f.freight_value) AS total_revenue,
        ROUND(
            (SUM(CASE WHEN f.is_late_delivery = TRUE THEN 1 ELSE 0 END) * 100.0) / 
            COUNT(CASE WHEN f.is_late_delivery IS NOT NULL THEN 1 END),
            2
        ) AS late_delivery_rate,
        ROUND(
            AVG(f.days_delivery_vs_estimate),
            2
        ) AS avg_days_vs_estimate
    from fact_order_items f
    inner join dim_sellers s ON f.seller_key=s.seller_key
    WHERE f.days_delivery_vs_estimate IS NOT NULL
    group by s.seller_id, s.seller_state
    HAVING COUNT(DISTINCT f.order_id) >= 20
),

pct_ranks AS(
    select 
        seller_id,
        seller_state,
        total_orders,
        total_revenue,
        late_delivery_rate,
        avg_days_vs_estimate,
        ROUND(
            (1 - PERCENT_RANK() OVER(ORDER BY late_delivery_rate)) * 100,
            2
        ) AS on_time_pctl,
        ROUND(
            (1 - PERCENT_RANK() OVER (ORDER BY avg_days_vs_estimate)) * 100,
            2
        ) AS speed_pctl,
        ROUND(
            PERCENT_RANK() OVER (ORDER BY total_revenue) * 100,
            2
        ) AS revenue_pctl
    from base_stats
)

select 
    seller_id,
    seller_state,
    total_orders,
    ROUND(total_revenue,2) AS total_revenue,
    late_delivery_rate,
    avg_days_vs_estimate,
    on_time_pctl,
    speed_pctl,
    revenue_pctl,
    ROUND(
        (on_time_pctl * 0.40) + 
        (speed_pctl * 0.30) + 
        (revenue_pctl * 0.30),
        2
    ) AS composite_score,
    RANK() OVER(ORDER BY 
        (on_time_pctl * 0.40) + 
        (speed_pctl * 0.30) + 
        (revenue_pctl * 0.30) DESC
    ) AS overall_rank
from pct_ranks
ORDER BY overall_rank
