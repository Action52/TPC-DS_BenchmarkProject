# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL TPC-DS
# MAGIC ## Testing decision support queries on a Spark Databricks deployment.
# MAGIC 
# MAGIC The purpose of this notebook is to execute the tpc-ds benchmark on a spark environment in the cloud. Modern implementations of data warehouses are almost certainly on the cloud. Let's evaluate how they behave assuming a small system (for testing and cost purposes). This testing framework works with scale factors of 1, 2, 3 and 4GB sizes.

# COMMAND ----------

# MAGIC %pip install tqdm
# MAGIC %pip install joblib

# COMMAND ----------

# MAGIC %md
# MAGIC #### Variables declaration
# MAGIC Please run all the cells in order to perform the experiments. Change the data size when needed.

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

data_size = "3G"  # 2GB 4GB
s3_bucket = "s3a://tpcds-spark/"
db_name = "tpcds"
schemas_location = "scripts/queries/table/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the schema and loading the tables
# MAGIC 
# MAGIC The TPCDS schema has been defined under the /scripts/queries/table/ of the repo. These sql templates will create a Hive Metastore table inside of the Databricks cluster. Once created, we are telling Spark to pull the data (stored in parquet format) from the corresponding s3 bucket. The data was generated using the dbsdgen tooling provided by TPCDS. For this experiment, we created samples for 1GB, 2GB and 4GB scale factors.  
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


# COMMAND ----------

# MAGIC %md
# MAGIC ### Executing the queries and recording performance metrics
# MAGIC In this section we will execute the TPC-DS queries provided to us. First, we parse the queries from generated queries file from the templates. For each data size we will run each query and save the result to csv file. We will also collect statistical data regarding the execution time of each query.

# COMMAND ----------

import csv

def save_list_results(url, data):
    data_frame = spark.createDataFrame(Row(**x) for x in data)
    data_frame.write.partitionBy('run_id').format("csv").mode("overwrite").option("header", "true").save(url)

# COMMAND ----------

from joblib import Parallel, delayed
from multiprocessing.pool import Pool
import traceback

NUM_THREADS = 5
NUM_POOLS = 10

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
    print(f"Running query {query_number} for scale factor {data_size}, saving results at {path_to_save_results}")
    try:
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
            "elapsed_time": 0.0,
            "row_count": 0,
            "error": True
        }

def run_queries(run_id, queries, path_to_save_results, path_to_save_stats, data_size, print_result=False):
#     with Pool(processes=NUM_POOLS) as pool:
#         stats = pool.starmap(run_query, [(run_id, i+1, queries, path_to_save_results, data_size, print_result) for i in range(len(queries))])
    stats = Parallel(n_jobs=NUM_THREADS, prefer="threads")(delayed(run_query)(run_id, i+1, queries, path_to_save_results, data_size, print_result) for i in range(len(queries)))
    save_list_results(path_to_save_stats, stats)

# COMMAND ----------

def run(data_sizes=['1G']):    
    for i, data_size in enumerate(data_sizes):
        queries_path = "scripts/queries_generated/queries_{size}_Fixed.sql".format(size=data_size)
        result_path = "s3a://tpcds-spark/results/{size}/{query_number}/test_run_csv"
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        # Create metastore for the given size
        create_database(name=db_name)
        create_tables(tables, s3_bucket, db_name, schemas_location, data_size, spark)
        
        # Load queries for the given size
        queries = load_queries(queries_path)
#         queries_need_to_be_fixed = [queries[13], queries[22], queries[23], queries[34], queries[38]]
        
        run_queries(i+1, queries, result_path, stats_path, data_size)

# COMMAND ----------

# Please don't run full pipeline unless ready, try with run(data_sizes=['1G'])
run(data_sizes=['1G', '2G', '3G', '4G'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization

# COMMAND ----------

import pandas as pd
from matplotlib import pyplot as plt
import math

# COMMAND ----------


def get_visualization_tables_per_scale():
    data_sizes = ["1G", "2G", "3G","4G" ]# "3G"] #  ["1G", 2G", "4G"]
    
    for i, data_size in enumerate(data_sizes):
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        schema = types.StructType([types.StructField("run_id", types.IntegerType(), True), 
                           types.StructField("query_id", types.IntegerType(), True), 
                           types.StructField("start_time", types.DoubleType(), True),
                           types.StructField("end_time", types.DoubleType(), True),
                           types.StructField("elapsed_time", types.DoubleType(), True),
                           types.StructField("row_count", types.IntegerType(), True),
                           types.StructField("error", types.BooleanType(), True)
                           ])
        df_s = spark.read.option("header", "true").csv(stats_path, schema)
        df= df_s.toPandas()
        #Time plot
        df.plot(x="query_id", y="elapsed_time", kind="bar", figsize=(20,10), title=f"Query runtime for scale factor {data_size}")

def get_visualization_tables_per_scale_for_all():
    data_sizes = ["1G", "2G", "3G","4G"]#, "2G", "3G"] #  ["1G", 2G", "4G"]
    dfs=[]
    for i, data_size in enumerate(data_sizes):
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        schema = types.StructType([types.StructField("run_id", types.IntegerType(), True), 
                           types.StructField("query_id", types.IntegerType(), True), 
                           types.StructField("start_time", types.DoubleType(), True),
                           types.StructField("end_time", types.DoubleType(), True),
                           types.StructField("elapsed_time", types.DoubleType(), True),
                           types.StructField("row_count", types.IntegerType(), True),
                           types.StructField("error", types.BooleanType(), True)
                           ])
        df_s = spark.read.option("header", "true").csv(stats_path, schema)
        df= df_s.toPandas()
        df['scale'] = data_size
        dfs.append(df)

    df_final=pd.concat(dfs, ignore_index=True)
    #General plot
    df_final.pivot(index='query_id', columns='scale', values='elapsed_time').plot(kind='bar', rot=0, figsize=(20,10), title="Query runtimes across scale factors")
    plt.tight_layout()
    plt.show()

    #Plots per queries
    grouped = df_final.groupby('query_id')
    
    nrows = int(math.ceil(len(grouped)/2.))
    fig, axs = plt.subplots(nrows,2)

    for (name, df), ax in zip(grouped, axs.flat):
        df.plot(x='scale',y='elapsed_time', ax=ax, title=str(name), figsize=(20,50))

# COMMAND ----------

get_visualization_tables_per_scale()
get_visualization_tables_per_scale_for_all()

# COMMAND ----------

data_sizes = ["1G", "2G", "3G","4G"]
dfs=[]
for i, data_size in enumerate(data_sizes):
    stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
    schema = types.StructType([types.StructField("run_id", types.IntegerType(), True), 
                           types.StructField("query_id", types.IntegerType(), True), 
                           types.StructField("start_time", types.DoubleType(), True),
                           types.StructField("end_time", types.DoubleType(), True),
                           types.StructField("elapsed_time", types.DoubleType(), True),
                           types.StructField("row_count", types.IntegerType(), True),
                           types.StructField("error", types.BooleanType(), True)
                           ])
    df_s = spark.read.option("header", "true").csv(stats_path, schema)
    df= df_s.toPandas()
    df['scale'] = data_size
    df_s.show(n=99)

# COMMAND ----------


