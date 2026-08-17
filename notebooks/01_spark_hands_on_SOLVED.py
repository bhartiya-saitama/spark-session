# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Hands-On — Retail E-Commerce (SOLVED)
# MAGIC
# MAGIC Facilitator copy. Every `YOU DO` cell is filled in.
# MAGIC Attendees use `01_spark_hands_on.py`.
# MAGIC
# MAGIC Runs on **Databricks Free Edition / serverless**. No RDDs, no `sparkContext`,
# MAGIC no cluster configs — serverless blocks all of those.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup (run this BEFORE the session)
# MAGIC
# MAGIC Creates a Unity Catalog Volume and puts the five data files in it.
# MAGIC Source order: the `data/` folder next to this notebook (if you cloned the
# MAGIC repo as a Git folder), otherwise download from GitHub.

# COMMAND ----------

import os
import shutil
import urllib.request
from pathlib import Path

CATALOG, SCHEMA, VOLUME = "workspace", "default", "spark_session"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# TODO(facilitator): point this at your public repo before sharing.
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/CHANGE-ME/spark-session/main/data"

FILES = [
    "customers.csv.gz",
    "products.csv.gz",
    "orders.csv.gz",
    "order_items.csv.gz",
    "events.jsonl.gz",
]

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

local_data = Path(os.getcwd()).parent / "data"
for name in FILES:
    target = f"{BASE}/{name}"
    if os.path.exists(target):
        continue
    source = local_data / name
    if source.exists():
        shutil.copyfile(source, target)
        print(f"copied  {name}")
    else:
        urllib.request.urlretrieve(f"{GITHUB_RAW_BASE}/{name}", target)
        print(f"download {name}")

display(dbutils.fs.ls(BASE))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Read & inspect (7 min)
# MAGIC
# MAGIC Three ways to read the same CSV. They are not equivalent.

# COMMAND ----------

# DEMO 1a — no options: every column is a string.
orders_str = spark.read.csv(f"{BASE}/orders.csv.gz", header=True)
orders_str.printSchema()
display(orders_str.limit(5))

# COMMAND ----------

# DEMO 1b — inferSchema: Spark reads the file an extra time to guess types.
orders_inf = spark.read.csv(f"{BASE}/orders.csv.gz", header=True, inferSchema=True)
orders_inf.printSchema()
print("rows:", orders_inf.count())
# order_ts is STILL a string: the file uses day-first (14-06-2024), which
# Spark's inference does not recognise as a timestamp.

# COMMAND ----------

# DEMO 1c — explicit schema: correct types, one pass over the file, no surprises.
orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_ts", StringType()),  # parsed properly in Section 2
    StructField("status", StringType()),
    StructField("channel", StringType()),
    StructField("currency", StringType()),
])
orders = spark.read.csv(f"{BASE}/orders.csv.gz", header=True, schema=orders_schema)
orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOU DO 1 — read `order_items.csv.gz` with an explicit schema
# MAGIC
# MAGIC Columns: `order_item_id, order_id, product_id, quantity, unit_price, discount_pct`
# MAGIC
# MAGIC 1. Type `quantity` as integer, `unit_price` and `discount_pct` as double.
# MAGIC 2. Count how many `unit_price` values came out **null**.
# MAGIC 3. Read the same file with `inferSchema=True` and compare the `unit_price` type.

# COMMAND ----------

items_schema = StructType([
    StructField("order_item_id", StringType()),
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_pct", DoubleType()),
])
items_raw = spark.read.csv(f"{BASE}/order_items.csv.gz", header=True, schema=items_schema)

print("total rows      :", items_raw.count())                                    # 150,000
print("null unit_price :", items_raw.filter(F.col("unit_price").isNull()).count())  # 2,994

spark.read.csv(f"{BASE}/order_items.csv.gz", header=True, inferSchema=True).printSchema()
# inferSchema types unit_price as STRING, because ~2% of the values are the
# literal text "N/A". One bad token poisons the whole column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — Transform (5 min)
# MAGIC
# MAGIC `select` / `filter` / `withColumn` / `dropDuplicates` — the daily bread.

# COMMAND ----------

# DEMO 2a — the raw file contains exact duplicate rows.
print("raw rows       :", orders.count())              # 60,500
print("distinct rows  :", orders.dropDuplicates().count())  # 60,000

# COMMAND ----------

# DEMO 2b — parsing a day-first timestamp without saying so gives you nulls,
# not an error. try_to_timestamp returns null instead of failing the job.
display(
    orders.select(
        "order_ts",
        F.try_to_timestamp("order_ts").alias("naive_parse"),
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm:ss").alias("correct_parse"),
    ).limit(5)
)

# COMMAND ----------

orders_clean = (
    orders
    .dropDuplicates()
    .withColumn("order_ts", F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm:ss"))
    .withColumn("order_month", F.date_format("order_ts", "yyyy-MM"))
    .filter(F.col("status") != "CANCELLED")
)
print("orders_clean:", orders_clean.count())  # 55,798
display(orders_clean.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOU DO 2 — clean up `items_raw`
# MAGIC
# MAGIC 1. Drop rows where `quantity` is negative (those are returns, not sales).
# MAGIC 2. Drop rows where `unit_price` is null.
# MAGIC 3. Add `line_total = quantity * unit_price * (1 - discount_pct)`, rounded to 2 dp.
# MAGIC 4. Print the row count before and after.

# COMMAND ----------

items_clean = (
    items_raw
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price").isNotNull())
    .withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2),
    )
)
print("before:", items_raw.count())    # 150,000
print("after :", items_clean.count())  # 145,553
display(items_clean.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Aggregate (6 min)

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("signup_date", StringType()),
    StructField("segment", StringType()),
])
customers_raw = spark.read.csv(f"{BASE}/customers.csv.gz", header=True, schema=customers_schema)

# DEMO 3a — 10 countries in the business, 31 groups in the result. Spark did
# exactly what you asked. It just wasn't what you meant.
display(customers_raw.groupBy("country").count().orderBy(F.desc("count")))

# COMMAND ----------

customers = (
    customers_raw
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .na.fill({"city": "UNKNOWN"})
)
display(customers.groupBy("country").count().orderBy(F.desc("count")))  # 10 + null

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOU DO 3 — top 10 products by revenue
# MAGIC
# MAGIC From `items_clean`, group by `product_id` and compute:
# MAGIC `revenue` (sum of line_total), `units` (sum of quantity),
# MAGIC `orders` (distinct order_id), `avg_discount`. Show the top 10 by revenue.

# COMMAND ----------

top_products = (
    items_clean
    .groupBy("product_id")
    .agg(
        F.round(F.sum("line_total"), 2).alias("revenue"),
        F.sum("quantity").alias("units"),
        F.countDistinct("order_id").alias("orders"),
        F.round(F.avg("discount_pct"), 3).alias("avg_discount"),
    )
    .orderBy(F.desc("revenue"))
)
display(top_products.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — Join (6 min)

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("supplier_country", StringType()),
])
products = spark.read.csv(f"{BASE}/products.csv.gz", header=True, schema=products_schema)

# DEMO 4a — an inner join silently deletes rows that do not match.
inner_n = items_clean.join(products, "product_id", "inner").count()
left_n = items_clean.join(products, "product_id", "left").count()
print("inner:", inner_n)
print("left :", left_n)
print("lost :", left_n - inner_n)  # 185 rows whose product_id is not in products

# COMMAND ----------

# DEMO 4b — left_anti shows you exactly what you were about to lose.
display(
    items_clean.join(products, "product_id", "left_anti")
    .groupBy("product_id").count().orderBy(F.desc("count")).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOU DO 4 — build the enriched fact table
# MAGIC
# MAGIC Join `items_clean` → `products` → `orders_clean` → `customers`.
# MAGIC Keep: order_id, order_month, channel, category, product_name, country,
# MAGIC segment, quantity, line_total. Then show revenue by country and category.

# COMMAND ----------

sales = (
    items_clean
    .join(F.broadcast(products), "product_id", "inner")
    .join(orders_clean.select("order_id", "customer_id", "order_month", "channel"), "order_id", "inner")
    .join(customers.select("customer_id", "country", "segment"), "customer_id", "inner")
    .select(
        "order_id", "order_month", "channel", "category", "product_name",
        "country", "segment", "quantity", "line_total",
    )
)

revenue_by_country_category = (
    sales.groupBy("country", "category")
    .agg(F.round(F.sum("line_total"), 2).alias("revenue"))
    .orderBy(F.desc("revenue"))
)
display(revenue_by_country_category.limit(15))

# COMMAND ----------

# DEMO 4c — why broadcast? products is tiny (500 rows). Broadcasting it ships a
# copy to every executor and removes a shuffle. Look for BroadcastHashJoin.
items_clean.join(F.broadcast(products), "product_id").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 — Spark SQL + windows (6 min)
# MAGIC
# MAGIC DataFrame API and SQL compile to the *same* plan. Use whichever reads better.

# COMMAND ----------

sales.createOrReplaceTempView("sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, category, ROUND(SUM(line_total), 2) AS revenue
# MAGIC FROM sales
# MAGIC GROUP BY country, category
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOU DO 5 — top 3 products per category
# MAGIC
# MAGIC Use `ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC)`.
# MAGIC A window function ranks rows *within* a group without collapsing them.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH product_revenue AS (
# MAGIC   SELECT category, product_name, ROUND(SUM(line_total), 2) AS revenue
# MAGIC   FROM sales
# MAGIC   GROUP BY category, product_name
# MAGIC )
# MAGIC SELECT * FROM (
# MAGIC   SELECT
# MAGIC     category,
# MAGIC     product_name,
# MAGIC     revenue,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank_in_category
# MAGIC   FROM product_revenue
# MAGIC )
# MAGIC WHERE rank_in_category <= 3
# MAGIC ORDER BY category, rank_in_category

# COMMAND ----------

# DEMO 5b — JSON with a nested field. No schema written by hand, and you reach
# into the struct with dot notation.
events = spark.read.json(f"{BASE}/events.jsonl.gz")
events.printSchema()

display(
    events.groupBy("event_type", F.col("device.os").alias("os"))
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6 — Lazy evaluation & query plans (DEMO, 4 min)

# COMMAND ----------

import time

# Transformations build a recipe. Nothing runs yet.
start = time.time()
chain = (
    sales
    .filter(F.col("country") == "IN")
    .filter(F.col("category") == "Electronics")
    .withColumn("net", F.col("line_total") * 0.82)
    .groupBy("order_month")
    .agg(F.sum("net").alias("net_revenue"))
)
print(f"building the chain: {time.time() - start:.3f}s")

# count() is an ACTION. Now Spark actually reads the files.
start = time.time()
print("rows:", chain.count())
print(f"first action: {time.time() - start:.3f}s")

# COMMAND ----------

# The filters were pushed down to the file scan — Spark reads less than you asked
# it to. That optimisation is only possible because execution was deferred.
chain.explain()

# COMMAND ----------

# Recomputation is the default. cache() makes the second pass cheap.
start = time.time()
chain.count()
print(f"second run, no cache: {time.time() - start:.3f}s")

chain.cache()
chain.count()  # materialises the cache

start = time.time()
chain.count()
print(f"third run, cached   : {time.time() - start:.3f}s")

chain.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC **`display()` vs `show()` vs `collect()`**
# MAGIC
# MAGIC - `display(df)` — Databricks UI, samples the data. Safe.
# MAGIC - `df.show(20)` — prints 20 rows to the log. Safe.
# MAGIC - `df.collect()` — pulls **every row** into the driver's memory. On a real
# MAGIC   table this is how you kill the cluster. Use `.limit(n).collect()` or don't.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7 — Write it out (DEMO, 3 min)

# COMMAND ----------

monthly_summary = (
    sales.groupBy("order_month", "country", "category")
    .agg(
        F.round(F.sum("line_total"), 2).alias("revenue"),
        F.sum("quantity").alias("units"),
        F.countDistinct("order_id").alias("orders"),
    )
)

(monthly_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.sales_monthly_summary"))

print("written")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.sales_monthly_summary
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.default.sales_monthly_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix — where to go next (not run in the session)
# MAGIC
# MAGIC Read this on your own. Each block is a whole topic.
# MAGIC
# MAGIC **Partitions and shuffle.** A DataFrame is split into partitions; one task
# MAGIC runs per partition. `groupBy` and `join` cause a *shuffle* — data moves
# MAGIC across the network and regroups. Shuffles are the expensive thing in Spark.
# MAGIC `repartition(n)` reshuffles into n partitions; `coalesce(n)` merges without
# MAGIC a shuffle but only reduces.
# MAGIC
# MAGIC **Skew.** If one key holds 40% of the rows, one task does 40% of the work
# MAGIC while the rest idle. Symptom: 199 tasks finish in seconds, one runs for an
# MAGIC hour. Fixes: broadcast the small side, salt the key, or enable AQE skew join.
# MAGIC
# MAGIC **Delta Lake beyond `write`.** `MERGE INTO` for upserts, `VERSION AS OF` for
# MAGIC time travel, `OPTIMIZE` to compact small files, `VACUUM` to clean up.
# MAGIC
# MAGIC **Structured Streaming.** Same DataFrame API, unbounded input:
# MAGIC `spark.readStream.format("cloudFiles")...writeStream.trigger(availableNow=True)`.
# MAGIC
# MAGIC **Notebooks vs jobs.** Production Spark is not a notebook — it is a Python
# MAGIC file submitted with `spark-submit` or a Databricks Job, version-controlled
# MAGIC and tested.
# MAGIC
# MAGIC **Resources**
# MAGIC - Spark SQL built-in functions: https://spark.apache.org/docs/latest/api/sql/
# MAGIC - PySpark DataFrame API: https://spark.apache.org/docs/latest/api/python/reference/
# MAGIC - *Learning Spark, 2nd Edition* — free PDF from Databricks
# MAGIC - Delta Lake docs: https://docs.delta.io/
