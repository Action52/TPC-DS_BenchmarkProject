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
s3_bucket = "s3://tpcds-spark/"
db_name = "tpcds"
schemas_location = "scripts/queries/table/"

# COMMAND ----------

# Create database and tables
def create_database(name=db_name):
    spark.sql(f"DROP DATABASE IF EXISTS {name}")
    spark.sql(f"CREATE DATABASE {name}")
    spark.sql(f"USE {name}")
    
def create_table(db, schema, ):

# COMMAND ----------

def create_database(name="tpcds"):
    spark = SparkSession.builder.appName("tpcds-loaddata-testing")\
    .master("spark://spark-master:7077")\
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", True)\
    .enableHiveSupport()\
    .getOrCreate()
