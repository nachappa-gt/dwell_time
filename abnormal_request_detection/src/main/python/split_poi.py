#!/bin/env python2.7 
#

"""
Split POI data by countries.

Copyright (C) 2016. xAd, Inc.  All Rights Reserved.
"""

import argparse
import logging
import os
from argparse import RawTextHelpFormatter

# from xad.common.conf import Conf

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *


APP_NAME = 'split_poi'
DESC = """
NAME:
    split_poi.py - Split POI file by countries.

"""


def parse_arguments():
    """Parse command line arguments."""
    global opt

    parser = argparse.ArgumentParser(description=DESC,
                                     formatter_class=RawTextHelpFormatter)

    parser.add_argument("--app_name", default=APP_NAME)
    parser.add_argument("--input", help='Input path')
    parser.add_argument("--output", help='Output path')
    parser.add_argument("--input_format", help='input data format',
                        default='avro')

    parser.add_argument('-d', '--debug', action='store_true',
                        help="Turn on debugging")
    parser.add_argument('-n', '--norun', action='store_true',
                        help="No run.")

    opt = parser.parse_args()


def init_logging():
    #level = logging.DEBUG
    level = logging.INFO
    fmt = ("%(asctime)s:%(name)s %(levelname)s " +
           "[%(module)s:%(funcName)s] %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'

    logging.basicConfig(format=fmt, datefmt=datefmt, level=level)

    # IPython specific setting
    logger = logging.getLogger().setLevel(level)
    #logging.info("Logging initialized")


def create_context():
    """Create Spark and SQL contexts"""
    global sc, sqlContext
    logging.info("Creating context...")
    sparkConf = SparkConf().setAppName(opt.app_name)
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)


def run():
    """Read and split the input table"""
    global opt

    logging.info('Loading {}...'.format(opt.input))
    # Handle AVRO input
    if (opt.input_format == 'avro'):
        input = os.path.join(opt.input, '*.avro')
        df = sqlContext.read.format("com.databricks.spark.avro").load(input)
    # Handle Parquet input files
    else:
        df = sqlContext.read.load(opt.input)

    logging.info('Splitting into {}...'.format(opt.output))
    if (not opt.norun):
        df.write.partitionBy('country').parquet(opt.output)


def main():
    """Main function """
    parse_arguments()
    init_logging()
    create_context()
    run()
    logging.info("Done!")


main()
