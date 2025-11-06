{{ config(
    materialized = 'table'
) }}

WITH orders AS (
  SELECT
    customer_id,
    order_id,
    order_date
  FROM {{ ref('orders') }}  o
  WHERE EXTRACT(YEAR FROM order_date) = 2023
),
orders_with_history AS (
  SELECT
    o1.customer_id,
    o1.order_id,
    o1.order_date,
    COUNT(o2.order_id) AS past_12m_orders
  FROM orders o1
  LEFT JOIN  {{ ref('orders') }} o2
    ON o1.customer_id = o2.customer_id
    AND o2.order_date < o1.order_date
    AND o2.order_date >= DATE_SUB(o1.order_date, INTERVAL 12 MONTH)
  GROUP BY 1, 2, 3
)
SELECT
  customer_id,
  order_id,
  order_date,
  past_12m_orders,
  CASE
    WHEN past_12m_orders = 0 THEN 'New'
    WHEN past_12m_orders BETWEEN 1 AND 3 THEN 'Returning'
    ELSE 'VIP'
  END AS customer_segment
FROM orders_with_history