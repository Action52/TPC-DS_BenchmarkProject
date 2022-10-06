import pyspark

from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession


# Initiate Spark
spark = SparkSession.builder.appName("tpcds-loaddata-testing").enableHiveSupport().getOrCreate();

# Create database by reading from create_db file
def createDatabase():
    with open("./queries/create_db.sql") as fr:
        queries = fr.readlines()
    
    for query in queries:
        spark.sql(query)
        
# Import the data from sql
def importData():
    pass

createDatabase()
