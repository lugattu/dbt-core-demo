SELECT
  date_date AS sale_date,
  customer_id,
  order_id,
  products_id AS product_id,
  net_sales,
  qty AS quantity
FROM {{ source('sales_orders', 'raw_sales') }}