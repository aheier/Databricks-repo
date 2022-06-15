# Databricks notebook source
"""
This Script extracts data from the souce and appends the data to the target path
without loosing columns
"""
from pyspark.sql import SparkSession, DataFrame

# COMMAND ----------

dbutils.widgets.text('source_file_path', '', 'Source File Path')
dbutils.widgets.text('source_file_format', '', 'Source File Format')
dbutils.widgets.text('target_file_path', '', 'Target File Path')
dbutils.widgets.text('primary_keys', '', 'Primary Keys')

# COMMAND ----------

source_file_path = dbutils.widgets.get('source_file_path')
source_file_format = dbutils.widgets.get('source_file_format')
target_file_path = dbutils.widgets.get('target_file_path')
primary_keys = dbutils.widgets.get('primary_keys')

# COMMAND ----------

def extract_data(spark_session: SparkSession, source_path: str, source_format: str):
    """
    extract_data

    This fuction extracts the data from the source and returns the data frame
    """
    source_df = spark_session.read.format(source_format.lower()).load(source_path)
    return source_df

# COMMAND ----------

def load_data(append_df: DataFrame, target_path: str):
    """
    load_data

    This fuction appends the CDC data to the target path
    """
    append_df.sort(['pos', 'op_ts'], ascending=False).write.mode('append').parquet(path=target_path)

# COMMAND ----------

def run_job(spark_session: SparkSession):
    """
    run_job

    This function controls the operations of the job
    """
    source_df = extract_data(spark_session, source_file_path, source_file_format)
    load_data(source_df, target_file_path)
    tmp_df = (spark_session.read.format('parquet').load(target_file_path))
    display(tmp_df)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
    spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    run_job(spark)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.read.load()
tmp_source_df = (spark.read.format('avro').option("inferSchema", "true")
    .load(path='/mnt/landingzoneadls/topics/OMS_GG_BIN_PLANOGRAM_AVRO/year=2022/month=06/day=13'))
display(tmp_source_df)

# COMMAND ----------
