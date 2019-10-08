-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Databrick SQL Guide
-- MAGIC https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/tables/retail_db/orders

-- COMMAND ----------

CREATE EXTERNAL Table orders (
order_id int,
order_date timestamp,
order_customer_id int,
order_status string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/FileStore/tables/retail_db/orders'

-- COMMAND ----------

Select * from Orders;

-- COMMAND ----------

CREATE EXTERNAL TABLE order_items (
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/FileStore/tables/retail_db/order_items';

SELECT * FROM order_items;

-- COMMAND ----------

CREATE EXTERNAL TABLE products (
  product_id INT,
  product_category_id INT,
  product_name STRING,
  product_description STRING,
  product_price FLOAT,
  product_image STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/FileStore/tables/retail_db/products';

SELECT * FROM products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualize SQL Results

-- COMMAND ----------

Select order_status,count(1) as status_count 
from Orders 
group by order_status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Join and Select data using Spark SQL

-- COMMAND ----------

SELECT order_date, product_name, order_item_subtotal
FROM orders 
JOIN order_items
  ON order_id = order_item_order_id
JOIN products
  ON product_id = order_item_product_id
WHERE order_status IN ('COMPLETE', 'CLOSED');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Get Daily Product Revenue Sorted using Spark SQL

-- COMMAND ----------

SELECT order_date, product_name, round(sum(order_item_subtotal), 2) AS revenue
FROM orders 
JOIN order_items
ON order_id = order_item_order_id
JOIN products
ON product_id = order_item_product_id
WHERE order_status IN ('COMPLETE', 'CLOSED')
GROUP BY order_date, product_name
ORDER BY order_date, revenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Save output to a table using Spark SQL

-- COMMAND ----------

CREATE TABLE daily_product_revenue (
  order_date TIMESTAMP,
  product_name STRING,
  revenue FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/FileStore/tables/retail_db/daily_product_revenue'



-- COMMAND ----------

--it is taking 200 thread and creating 200 files
INSERT OVERWRITE TABLE daily_product_revenue
SELECT order_date, product_name, round(sum(order_item_subtotal), 2) AS revenue
FROM orders 
JOIN order_items
ON order_id = order_item_order_id
JOIN products
ON product_id = order_item_product_id
WHERE order_status IN ('COMPLETE', 'CLOSED')
GROUP BY order_date, product_name
ORDER BY order_date, revenue DESC;

-- COMMAND ----------

SET spark.sql.shuffle.partitions=2;
INSERT OVERWRITE TABLE daily_product_revenue
SELECT order_date, product_name, round(sum(order_item_subtotal), 2) AS revenue
FROM orders 
JOIN order_items
ON order_id = order_item_order_id
JOIN products
ON product_id = order_item_product_id
WHERE order_status IN ('COMPLETE', 'CLOSED')
GROUP BY order_date, product_name
ORDER BY order_date, revenue DESC;

-- COMMAND ----------

Select * from daily_product_revenue;

-- COMMAND ----------

