# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
# spark.read.load()
read_df = (spark.read.format('avro').option("inferSchema", "true").load(path='/mnt/rawadls/Test/lz/3'))
display(read_df)
read_df.coalesce(1).write.mode('append').format('delta').save('/mnt/rawadls/Test/raw')

# COMMAND ----------

raw_df = (spark.read.format('delta').load(path='/mnt/rawadls/Test/raw'))
print(raw_df.rdd.getNumPartitions())
# display(raw_df)

# COMMAND ----------

raw_df.coalesce(4).write.mode('overwrite').format('delta').save('/mnt/rawadls/Test/raw')
print(spark.read.format('delta').load('/mnt/rawadls/Test/raw').rdd.getNumPartitions())

# COMMAND ----------

read_df.rdd.getNumPartitions()

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

dt = DeltaTable.forPath(spark, '/mnt/rawadls/Test/raw')

# COMMAND ----------

dt.vacuum(0)

display(dt.history())

# COMMAND ----------

# spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
print(spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled"))
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", 'false')

# COMMAND ----------

files = dbutils.fs.ls('mnt/rawadls/Test/delta')
delta_size = 0
for file in files:
  delta_size += file.size
  print(file)
print(f"Total Bytes: {delta_size}")
