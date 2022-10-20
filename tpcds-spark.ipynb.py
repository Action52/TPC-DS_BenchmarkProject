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
from pyspark.sql import SQLContext, SparkSession
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
s3_bucket = "s3n://tpcds-spark/"
db_name = "tpcds"
schemas_location = "scripts/queries/table/"

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
    data_frame = spark.createDataFrame(data)
    data_frame.write.format("csv").mode("overwrite").option

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

def run_query(run_id, query_number, queries, path_to_save_results, print_result=False):
    try:
        start = time.time()
        result = spark.sql(queries[query_number-1])
        count = result.count()
        end = time.time()
        result.write.format("csv").mode("overwrite").option("header", "true").save(path_to_save_results.format(query_number=query_number))
        stats = {
            "run_id": run_id,
            "query_id": query_number,
            "start_time": start,
            "end_time": end,
            "elapsed_time": end-start,
            "result": result.show(),
            "row_count": count
        }
        if (print_result is True):
            print(stats)
            print(result.show())
        return stats
    except:
        return {}

def run_queries(run_id, queries, path_to_save_results, path_to_save_stats):
    stats = Parallel(n_jobs=NUM_THREADS, prefer="threads")(delayed(run_query)(run_id, i+1, queries, path_to_save_results, True) for i in range(len(queries)))
    save_list_results(path_to_save_stats, stats)

# COMMAND ----------

# queries = load_queries("scripts/queries_1G.sql")
# # # run_query(1, 5, queries, "s3://tpcds-spark/results/1G/test_run_csv", print_result=True)
# run_queries(1, queries, "s3://tpcds-spark/results/1G/test_run_csv", "s3://tpcds-spark/stats/1G/test_run_stats_csv")

def run_test():
    # Run all the dataset from 1G, 2G, and 3G
    data_sizes = ["1G"] #  ["1G", 2G", "4G"]
    
    for i, data_size in enumerate(data_sizes):
        queries_path = "scripts/queries_{size}.sql".format(size=data_size)
        result_path = "s3://tpcds-spark/results/{size}/{query_number}/test_run_csv".format(size=data_size)
        stats_path = "s3://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        
        queries = load_queries(queries_path)
        run_queries(i+1, queries, result_path, stats_path)


# COMMAND ----------

len([5, 12, 14, 16, 20, 21, 23, 24, 32, 37, 39, 40, 50, 62, 64, 77, 80, 82, 92, 94, 95, 98, 99])
