SELECT
  date_date AS order_date,
  customers_id AS customer_id,
  orders_id AS order_id,
  net_sales
FROM {{ source('sales_orders', 'raw_orders') }}