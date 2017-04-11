import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext 
from pyspark.sql import HiveContext
from pyspark.sql import Row
from operator import itemgetter
from math import sin, cos, sqrt, atan2, radians
import sys
import argparse
import logging
import subprocess
import pyspark.sql.types as pst
from datetime import datetime 

def main():
    conf = SparkConf().setAppName('Hive Status Initialization')
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)   

    field = [pst.StructField("id", pst.IntegerType(), True), 
         pst.StructField("time", pst.StringType(), True), 
         pst.StructField("key", pst.StringType(), True), 
         pst.StructField("year", pst.StringType(), True),
         pst.StructField("month", pst.StringType(), True),
         pst.StructField("day", pst.StringType(), True),
         pst.StructField("hour", pst.StringType(), True),
         pst.StructField("num_executors", pst.StringType(), True),
         pst.StructField("executor_cores", pst.StringType(), True),
         pst.StructField("executor_mem",pst.StringType(),True)]
    schema = pst.StructType(field)
    
    timestamp = datetime.now()
    df = sqlContext.createDataFrame([(0,str(timestamp),'initilizing','2017','03','28','00','32','1','4g')],schema = schema)
    df.write.mode("overwrite").format("com.databricks.spark.avro").save('/prod/ard/hivestatus')

    sc.stop()


if __name__ == '__main__':
    main()

