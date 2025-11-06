{{ config(
    materialized = 'table'
) }}

SELECT 
  o.order_id,
  o.customer_id,
  order_date,
  SUM(s.net_sales) total_sales,
  MAX(o.net_sales) net_orders,
  COUNT(DISTINCT s.product_id) total_products, 
  SUM(s.quantity) quantity,
FROM {{ ref('sales') }}  s
LEFT JOIN {{ ref('orders') }}  o
ON s.order_id = o.order_id
GROUP BY 1, 2, 3