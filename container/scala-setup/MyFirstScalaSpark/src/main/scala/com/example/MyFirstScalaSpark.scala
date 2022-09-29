package com.example
import org.apache.spark.sql.SparkSession
object MyFirstScalaSpark {
def main(args: Array[String]) {
val SPARK_HOME = sys.env("SPARK_HOME")
val logFile = s"${SPARK_HOME}/README.md"
val spark = SparkSession.builder
.appName("MyFirstScalaSpark")
.getOrCreate()
val logData = spark.read.textFile(logFile).cache()
val numAs = logData.filter(line => line.contains("a")).count()
val numBs = logData.filter(line => line.contains("b")).count()
println(s"Lines with a: $numAs, Lines with b: $numBs")
spark.stop()
}
}
