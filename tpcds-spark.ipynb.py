# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL TPC-DS
# MAGIC ## Testing decision support queries on a Spark Databricks deployment.
# MAGIC 
# MAGIC The purpose of this notebook is to execute the tpc-ds benchmark on a spark environment in the cloud. Modern implementations of data warehouses are almost certainly on the cloud. Let's evaluate how they behave assuming a small system (for testing and cost purposes). Current cluster assumes a spark environment of 1 master node and 2-8 workers. Each worker has 8GB of memory with 

# COMMAND ----------

# MAGIC %pip install tqdm
# MAGIC %pip install joblib

# COMMAND ----------

# Import statements
import pyspark
import os
import logging
from pyspark import SparkContext
from pyspark.sql import Row, SQLContext, SparkSession, types
from tqdm.notebook import tqdm_notebook
import time

# Variable definition
tables = ["call_center", "catalog_page", "catalog_returns", "catalog_sales",
             "customer_address", "customer_demographics", "customer", "date_dim",
             "dbgen_version", "household_demographics", "income_band", "inventory", "item",
             "promotion", "reason", "ship_mode", "store_returns", "store_sales", "store",
             "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"
            ]

data_size = "1G"  # 2GB 4GB
s3_bucket = "s3a://tpcds-spark/"
db_name = "tpcds"
schemas_location = "scripts/queries/table/"
schema = types.StructType([types.StructField("name", types.IntegerType(), True), 
                           types.StructField("query_id", types.IntegerType(), True), 
                           types.StructField("start_time", types.DoubleType(), True),
                           types.StructField("end_time", types.DoubleType(), True),
                           types.StructField("elapsed_time", types.DoubleType(), True),
                           types.StructField("result", types.StringType(), True),
                           types.StructField("row_count", types.IntegerType(), True),
                           types.StructField("error", types.BooleanType(), True)
                           ])

# AWS_ACCESS_KEY_ID = ""
# AWS_SECRET_ACCESS_KEY = ""



# spark = SparkSession.builder.appName("tpcds")\
#     .config("spark.pyspark.python", "python") \
#     .enableHiveSupport()\
#     .getOrCreate()
    
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the schema and loading the tables
# MAGIC 
# MAGIC The TPCDS schema has been defined under the /scripts/queries/table/ of the repo. These sql templates will create a Hive Metastore table inside of the Databricks cluster. Once created, we are telling Spark to pull the data (that is stored in parquet format) from the corresponding s3 bucket. The data was generated using the dbsdgen tooling provided by TPCDS. For this experiment, we created samples for 1GB, 2GB and 4GB scale factors.  
# MAGIC Once we have created the metastore, we can test Spark SQL decision support capabilities with the tpcds queries.

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
        queries = schema_file.read().strip("\n").replace("${data_path}", data_path).split(";")
    for query in queries:
        spark.sql(query)

def create_tables(relations, s3_bucket, db_name, schemas_location, data_size, spark):
    for relation in relations:
        create_table(relation, 
                     s3_bucket=s3_bucket, 
                     db_name=db_name, 
                     schemas_location=schemas_location, 
                     data_size=data_size, 
                     spark=spark)

create_database(name=db_name)
create_tables(tables, s3_bucket, db_name, schemas_location, data_size, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Executing the queries and recording performance metrics

# COMMAND ----------

import csv

def save_list_results(url, data):
    data_frame = spark.createDataFrame(Row(**x) for x in data)
    data_frame.write.format("csv").mode("overwrite").option("header", "true").save(url)

# COMMAND ----------

from joblib import Parallel, delayed

NUM_THREADS = 5

def load_queries(path_to_queries) -> list:
    with open(path_to_queries) as file_obj:
        comment_count = 0
        queries = []
        query_lines = []
        for line in file_obj:
            if comment_count == 0 and "--" in line:  # it is a comment and therefore the beginning or end of a query
                comment_count += 1
                query_lines = []
                continue
            elif comment_count == 1 and "--" not in line:  # we are reading part of the query
                query_lines.append(line)
            elif comment_count == 1 and "--" in line:  # it is the second comment indicating this is the end of the query
                query = "".join(query_lines)
                queries.append(query)
                comment_count = 0
    return queries

def run_query(run_id, query_number, queries, path_to_save_results, data_size, print_result=False):
    try:
        print(run_id, query_number, queries)
        start = time.time()
        result = spark.sql(queries[query_number-1])
        count = result.count()
        end = time.time()
        result.write.format("csv").mode("overwrite").option("header", "true").save(path_to_save_results.format(size=data_size, query_number=query_number))
        stats = {
            "run_id": run_id,
            "query_id": query_number,
            "start_time": start,
            "end_time": end,
            "elapsed_time": end-start,
            "row_count": count,
            'error': False
        }
        if (print_result is True):
            print(stats)
            print(result.show())
        return stats
    except Exception as e:
        return {
            "run_id": run_id,
            "query_id": query_number,
            "start_time": time.time(),
            "end_time": time.time(),
            "elapsed_time": 0,
            "row_count": 0,
            "error": True
        }

def run_queries(run_id, queries, path_to_save_results, path_to_save_stats, data_size):
    print("running queries", path_to_save_results, path_to_save_stats)
    # stats = []
    # for i in range(len(queries)):
    #     stats.append(run_query(run_id, i+1, queries, path_to_save_results, data_size, True))
    stats = Parallel(n_jobs=NUM_THREADS, prefer="threads")(delayed(run_query)(run_id, i+1, queries, path_to_save_results, data_size, True) for i in range(len(queries)))
    print(stats)
    save_list_results(path_to_save_stats, stats)

# COMMAND ----------

# queries = load_queries("scripts/queries_1G.sql")
# # # run_query(1, 5, queries, "s3://tpcds-spark/results/1G/test_run_csv", print_result=True)
# run_queries(1, queries, "s3://tpcds-spark/results/1G/test_run_csv", "s3://tpcds-spark/stats/1G/test_run_stats_csv")

def run_test():
    # Run all the dataset from 1G, 2G, and 4G
    data_sizes = ["1G"] #  ["1G", 2G", "4G"]
    
    for i, data_size in enumerate(data_sizes):
        queries_path = "scripts/queries_generated/queries_{size}_Fixed.sql".format(size=data_size)
        result_path = "s3a://tpcds-spark/results/{size}/{query_number}/test_run_csv"
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        
        queries = load_queries(queries_path)[0:1]
        print("Processing queries")
        run_queries(i+1, queries, result_path, stats_path, data_size)

run_test()
