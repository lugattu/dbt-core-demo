{{ config(
    materialized = 'table'
) }}

SELECT 
  o.order_id,
  COUNT(s.product_id) AS qty_product
FROM {{ ref('sales') }} AS s
LEFT JOIN {{ ref('orders') }} AS o
  ON o.order_id = s.order_id
WHERE EXTRACT(YEAR FROM o.order_date) IN (2022, 2023)
GROUP BY o.order_id