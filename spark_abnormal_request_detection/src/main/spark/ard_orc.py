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

def main():
    conf = SparkConf().setAppName('ScienceCoreExtension_orc')
    sc = SparkContext(conf = conf)

    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc) 

    """ Add arguments in the command to specify the information of the data to process
     such as country, prod_type, dt, fill and loc_score"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--year", help="year")
    parser.add_argument("--month", help="month")
    parser.add_argument("--day", help="day")
    parser.add_argument("--hour", help="hour")
    parser.add_argument("--avro_partitions",help="avro_partitions")
        
    """Parse the arguments """
    args = parser.parse_args()
    if args.country:
        country = args.country
    if args.logtype:
        logtype = args.logtype
    if args.year:
        year = args.year
    if args.month:
        month = args.month
    if args.day:
        day = args.day
    if args.hour:
        hour = args.hour
    if args.avro_partitions:
        partitions_str = args.avro_partitions
        avro_partitions = partitions_str.split(',')
    
    fill_status = ['fill', 'nf']
    locscore_status = ['tll', 'pos','rest']

    for fill in fill_status:
        for loc_score in locscore_status:
            partition = '-'.join([fill, loc_score])
            if partition in avro_partitions:                
                save_as_orc(hiveContext,country, logtype, year, month, day, hour,fill,loc_score)

    sc.stop()

def save_as_orc(hiveContext,country, logtype, year, month, day, hour, fill, loc_score):

    avro_base_dir = '/data/extract'
    output_base_dir = '/tmp/ard'
    date_dir = '/'.join([country, logtype, year, month, day, hour])

    avro_path = os.path.join(avro_base_dir, date_dir, fill, loc_score)                            
    output_path = os.path.join(output_base_dir, date_dir, fill, loc_score)
    df = hiveContext.read.format("com.databricks.spark.avro").load(avro_path)
    df.write.mode("overwrite").format("orc").option("compression","zlib").mode("overwrite").save(output_path)

if __name__ == "__main__":
    main()  


