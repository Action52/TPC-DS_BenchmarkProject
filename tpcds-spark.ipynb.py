# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL TPC-DS
# MAGIC ## Testing decision support queries on a Spark Cloud deployment.
# MAGIC 
# MAGIC The purpose of this notebook is to execute the tpc-ds benchmark on a spark environment in the cloud. Modern implementations of data warehouses are almost certainly on the cloud. Let's evaluate how they behave assuming a small system (for testing and cost purposes). Current cluster assumes a spark environment of 1 master node and 2-8 workers. Each worker has 8GB of memory with 

# COMMAND ----------

# MAGIC %pip install tqdm

# COMMAND ----------

# Import statements
import pyspark
import os
import logging
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from tqdm.notebook import tqdm_notebook

# Variable definition
tables = ["call_center", "catalog_page", "catalog_returns", "catalog_sales",
             "customer_address", "customer_demographics", "customer", "date_dim",
             "dbgen_version", "household_demographics", "income_band", "inventory", "item",
             "promotion", "reason", "ship_mode", "store_returns", "store_sales", "store",
             "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"
            ]

data_size = "1GB"  # 2GB 4GB
s3_bucket = "s3n://tpcds-spark/"
db_name = "tpcds"
schemas_location = "scripts/queries/table/"

# COMMAND ----------

# Create database and tables
def create_database(name=db_name):
    spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")
    spark.sql(f"CREATE DATABASE {name}")
    spark.sql(f"USE {name}")
def create_table(relation, s3_bucket=s3_bucket, db_name=db_name, schemas_location=schemas_location, data_size=data_size, spark=spark):
    spark.sql(f"USE {db_name}")
    schema_path = f"{schemas_location}{relation}.sql"
    data_path = f"{s3_bucket}{data_size}/{relation}/{relation}/parquet/"
    with open(schema_path) as schema_file:
        print(data_path)
        query = schema_file.read().strip("\n").replace("${data_path}", data_path)
        spark.sql(f"DROP TABLE IF EXISTS {relation}")
        spark.sql(query)
    
create_database()
create_table(relation="call_center")

spark.sql("SHOW DATABASES").show()
spark.sql("SELECT * FROM call_center LIMIT 5").show()

# COMMAND ----------

def create_database(name="tpcds"):
    spark = SparkSession.builder.appName("tpcds-loaddata-testing")\
    .master("spark://spark-master:7077")\
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", True)\
    .enableHiveSupport()\
    .getOrCreate()
