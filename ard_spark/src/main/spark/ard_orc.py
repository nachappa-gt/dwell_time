"""

This program converts science core AVRO into ORC.

Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.

@author: xiangling

"""

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import argparse


def save_as_orc(hiveContext,country, logtype, date, hour, fill, loc_score,
                input_dir, output_dir):
    """Save as ORC
    
    Will skip if the dataframe is empty.

    FIXME: this is a duplicated function (another one in ard_join).
    Remove duplication.
    """

    avro_path = os.path.join(input_dir, fill, loc_score)                            
    output_path = os.path.join(output_dir, fill, loc_score)

    # Load an empty avro file that has the full schema
    df_schema = hiveContext.read.format("com.databricks.spark.avro").load('/prod/ard/schema')
    schema = df_schema.schema

    df = hiveContext.read.format("com.databricks.spark.avro").load(avro_path, schema=schema)
    if not df.rdd.isEmpty:
        df.write.mode("overwrite").format("orc").\
            option("compression","zlib").\
            mode("overwrite").save(output_path)


def main():     

    #Add arguments in the command to specify the information of the data to process
    #such as country, prod_type, dt, fill and loc_score
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--date", help="date")
    parser.add_argument("--hour", help="hour")
    parser.add_argument("--avro_partitions",help="avro_partitions")
    parser.add_argument("--input_dir", help="input dir")
    parser.add_argument("--output_dir", help="output dir")
   
    #Parse the arguments 
    args = parser.parse_args()
    if args.country:
        country = args.country
    if args.logtype:
        logtype = args.logtype
    if args.date:
        date = args.date
    if args.hour:
        hour = args.hour
    if args.avro_partitions:
        partitions_str = args.avro_partitions
        sub_parts = [ x.split('-') for x in partitions_str.split(',') ]    
    if args.input_dir:
        input_dir = args.input_dir
    if args.output_dir:
        output_dir = args.output_dir
    
    app_name = "ard_orc {}/{}/{}/{}".format(country, logtype, date, hour)
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf = conf)

#    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)

    for p in sub_parts:
        fill, loc_score = p
        save_as_orc(hiveContext, country, logtype, date, hour, fill, loc_score,
                    input_dir, output_dir)                

    sc.stop()


if __name__ == "__main__":
    main()  


