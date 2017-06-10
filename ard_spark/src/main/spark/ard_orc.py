"""

This program converts science core AVRO into ORC.

Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.

@author: xiangling

"""

import sys
sys.path.append('/home/xad/ard/python')
sys.path.append('/home/xad/share/python')
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import argparse

DEFAULT_CONFIG_DIRS = "/home/xad/ard/config:/home/xad/share/config"
DEFAULT_CONFIG_FILE = "ard.properties"


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
    if not df.rdd.isEmpty():
        df.write.mode("overwrite").format("orc").\
            option("compression","zlib").\
            mode("overwrite").save(output_path)

def parse_arguments():
    """Parse command line arguments
    
    Add arguments in the command to specify the information of the data
    to process such as country, prod_type, dt, fill and loc_score"
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_dirs', help="Configuration search dirs",
                        default=DEFAULT_CONFIG_DIRS)
    parser.add_argument('--config', help="Configuration file",
                        default=DEFAULT_CONFIG_FILE)    
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--date", help="date")
    parser.add_argument("--hour", help="hour")
    parser.add_argument("--avro_partitions",help="avro_partitions")
    parser.add_argument("--input_dir", help="input dir")
    parser.add_argument("--output_dir", help="output dir")

    # Parse the arguments
    opt = parser.parse_args()
    return(opt)
    

def get_app_name(opt):
    """Get the application name"""
    return os.path.join('ard_orc', opt.country,
                        opt.logtype, opt.date, opt.hour)

def main():        
    #Parse the arguments 
    opt = parse_arguments()
    sub_parts = [ x.split('-') for x in opt.avro_partitions.split(',') ]    

    # Context
    app_name = get_app_name(opt)
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf = conf)
    hiveContext = HiveContext(sc)

    for p in sub_parts:
        fill, loc_score = p
        save_as_orc(hiveContext, opt.country, opt.logtype, opt.date, opt.hour, 
                    fill, loc_score,
                    opt.input_dir, opt.output_dir)                

    sc.stop()


if __name__ == "__main__":
    main()  


