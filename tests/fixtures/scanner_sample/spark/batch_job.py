"""Spark-style script for scanner classification."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fixture").getOrCreate()
df = spark.read.parquet("/tmp/data")
