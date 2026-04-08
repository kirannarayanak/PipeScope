"""Sample PySpark script for parser tests (no Spark runtime required)."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fixture").getOrCreate()

orders = spark.table("db.orders")
customers = spark.read.parquet("s3://warehouse/customers")

joined = orders.join(customers, "customer_id")
joined.write.saveAsTable("db.orders_enriched")
