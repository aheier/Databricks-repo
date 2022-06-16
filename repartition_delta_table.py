# Databricks notebook source
"""
This Script extracts data from the souce and appends the data to the target path
without loosing columns
"""
from pyspark.sql import SparkSession, DataFrame
from delta.tables import *

# COMMAND ----------

RETENTION_HOURS = 0 #168 is minimum without setting

# COMMAND ----------

def extract_data(spark_session: SparkSession, source_path: str, source_format: str):
    """
    extract_data

    This fuction extracts the data from the source and returns the data frame
    """
    source_df = (spark_session.read.format(source_format.lower())
                 .option("inferSchema", "true")
                 .load(source_path))
    return source_df

# COMMAND ----------

def load_data(append_df: DataFrame, target_path: str):
    """
    load_data

    This fuction appends the CDC data to the target path
    """
    target_dt = (append_df.sort(['pos', 'op_ts'], ascending=False)
                 .coalesce(1)
                 .write
                 .mode('append')
                 .format('delta')
                 .save(path=target_path))
    return target_dt

# COMMAND ----------

def repartition_delta_table(spark_session: SparkSession, 
                            format_df: DataFrame, 
                            target_path: str):
    max_partitions = 8
    if(format_df.rdd.getNumPartitions() >= max_partitions):
      format_df.coalesce(4).write.mode('overwrite').format('delta').save(target_path)
      dt = DeltaTable.forPath(spark_sesssion, target_path)
      dt.vaccum(RETENTION_HOURS)

# COMMAND ----------

def get_total_file_size(dt):
  
    files = dbutils.fs.ls('mnt/rawadls/Test/raw')
    delta_size = 0
    for file in files:
        delta_size += file.size
        print(file)
    print(f"Total Bytes: {delta_size}")
    return delta_size

# COMMAND ----------

def run_job(spark_session: SparkSession):
    """
    run_job

    This function controls the operations of the job
    """
    source_df = extract_data(spark_session, source_file_path, source_file_format)
    load_data(source_df, target_file_path)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
    spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    run_job(spark)

# COMMAND ----------

tmp_df = (spark.read.format('delta').load(target_file_path))
DeltaTable.DeltaTable.isDeltaTable

# COMMAND ----------

print(get_total_file_size('x'))
