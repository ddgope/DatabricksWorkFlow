# Databricks notebook source
# MAGIC %md
# MAGIC ## Check the Reatil DB Original files

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/retail_db/orders/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read order,order_items and products into DataFrames using Python

# COMMAND ----------

orders = spark.read. \
  csv("/FileStore/tables/retail_db/orders",
     schema="order_id INT, order_date TIMESTAMP, order_customer_id INT, order_status STRING")
#help(orders.schema)
orders.printSchema()
orders.show()
orders.count()

# COMMAND ----------

orderItems = spark.read. \
  csv("/FileStore/tables/retail_db/order_items",
     schema='''order_item_id INT, 
      order_item_order_id INT, 
      order_item_product_id INT, 
      order_item_quantity INT,
      order_item_subtotal FLOAT,
      order_item_product_price FLOAT''')

orderItems.printSchema()
orderItems.show()
orderItems.count()

# COMMAND ----------

products = spark.read. \
  csv("/FileStore/tables/retail_db/products",
     schema='''product_id INT, 
      product_category_id INT, 
      product_name STRING, 
      product_description STRING,
      product_price FLOAT,
      product_image STRING''')

products.printSchema()
products.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Data using Display in Python

# COMMAND ----------

from pyspark.sql.functions import count, lit

orderStatusCount = orders.groupBy('order_status'). \
  agg(count(lit(1)).alias('status_count'))
# display function to get the visualization as part of Databricks Notebook Method for dispalying tabular format data.
display(orderStatusCount)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Complete or closed orders using Python

# COMMAND ----------

ordersCompleted = orders.filter("order_status IN ('COMPLETE', 'CLOSED')")

ordersCompleted.show()
ordersCompleted.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join and Select data using Python

# COMMAND ----------

from pyspark.sql.functions import col

joinResults = ordersCompleted.join(orderItems, col("order_id") == orderItems["order_item_order_id"]). \
  join(products, col("product_id") == col("order_item_product_id")). \
  select("order_date", "product_name", "order_item_subtotal")

joinResults.show(truncate=False)
joinResults.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Daily Product Revenue using Python

# COMMAND ----------

from pyspark.sql.functions import sum, round

dailyProductRevenue = joinResults. \
  groupBy("order_date", "product_name"). \
  agg(round(sum("order_item_subtotal"), 2).alias("revenue"))

dailyProductRevenue.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Daily Product Revenue Sorted using Python

# COMMAND ----------

#need to perform composite sorting.
# From our data set it makes sense to sort the data in ascending order by order_date and then in descending order by revenue with in each date.
dailyProductRevenueSorted = dailyProductRevenue. \
  orderBy("order_date", col("revenue").desc())

dailyProductRevenueSorted.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save output to CSV Files using Python

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "2")
dailyProductRevenueSorted.write. \
  mode("overwrite"). \
  csv("/FileStore/tables/retail_db/daily_product_revenue")

dailyProductRevenue = spark.read. \
  schema("order_date TIMESTAMP, product_name STRING, revenue FLOAT"). \
  csv("/FileStore/tables/retail_db/daily_product_revenue")

display(dailyProductRevenue)

# COMMAND ----------

help(display)

# COMMAND ----------

