import pyspark
import os

from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession

# Initiate Spark
spark = SparkSession.builder.appName("tpcds-loaddata-testing").enableHiveSupport().getOrCreate();

relations = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", 
             "customer_address", "customer_demographics", "customer", "date_dim",
             "dbgen_version", "household_demographics", "income_band", "inventory", "item", 
             "promotion", "reason", "ship_mode", "store_returns", "store_sales", "store",
             "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"
            ]


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
data_dir = "{}/../data".format(ROOT_DIR)
sql_dir = "{}/queries/table/".format(ROOT_DIR)

# Create database by reading from create_db file
def createDatabase():
    with open("{}/queries/create_db.sql".format(ROOT_DIR)) as fr:
        queries = fr.readlines()
    
    for query in queries:
        spark.sql(query)
        
# Import the data from sql
def importData():
    for relation in relations:
        print("processing", relation)
        filepath = "{}{}.sql".format(sql_dir, relation)
        
        # Read queries file
        with open(filepath) as fr:
            queries = fr.read().strip("\n").replace("${path}", data_dir).replace("${name}", relation).split(";");
        
        
        for query in queries:
            if query != "":
                spark.sql(query);

createDatabase()
importData()
