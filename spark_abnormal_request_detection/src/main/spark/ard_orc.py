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
     

    #Add arguments in the command to specify the information of the data to process
    #such as country, prod_type, dt, fill and loc_score
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--year", help="year")
    parser.add_argument("--month", help="month")
    parser.add_argument("--day", help="day")
    parser.add_argument("--hour", help="hour")
    parser.add_argument("--avro_partitions",help="avro_partitions")
    parser.add_argument("--executor_mem",help="executor_memory")
    parser.add_argument("--executors_num",help="num_executors")
    parser.add_argument("--exe_cores",help="executor_cores")
   
    #Parse the arguments 
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
    """if args.executor_mem:
        executor_memory= args.executor_mem
    if args.executors_num:
        num_executors = args.executors_num
    if args.exe_cores:
        executor_cores = args.exe_cores"""
    
    
    fill_status = ['fill', 'nf']
    locscore_status = ['tll', 'pos','rest']

    conf = SparkConf().setAppName('ScienceCoreExtension_orc' + '/' +country + '/' + logtype  + '/' +day + '/' + hour)
    sc = SparkContext(conf = conf)

    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)

    for fill in fill_status:
        for loc_score in locscore_status:
            partition = '-'.join([fill, loc_score])
            if partition in avro_partitions:                
                save_as_orc(hiveContext,country, logtype, year, month, day, hour,fill,loc_score)                
    #addHiveStatus(sqlContext,hiveContext,country, logtype, year, month, day, hour, num_executors, executor_cores, executor_memory)

    sc.stop()

"""def addHiveStatus(country, logtype, fill, loc_score, year, month, day, hour, executor_num, core, executor_memory):
    # This function can work in ipython notebook, but doesn't work in the pipeline
    hiveContext = HiveContext(sc)
    df = hiveContext.sql('select year from xianglingmeng.hivestatus')
    rom_num = df.count() + 1
    key = '/'.join([country,logtype,fill,loc_score])
    timestamp = datetime.now()
    hiveContext.sql('insert into table xianglingmeng.hivestatus values({},"{}","{}","{}","{}","{}","{}","{}","{}","{}")'.format(row_num,timestamp,key,year,month,day,hour,executor_num,core,executor_memory))"""

"""def addHiveStatus(sqlContext,hiveContext,country, logtype, year, month, day, hour, executor_num, core, executor_memory):

    df1 = hiveContext.read.format("com.databricks.spark.avro").load('/prod/ard/hivestatus')
    schema = df.schema
    
    rom_num = df1.count() 
    key = '/'.join([country,logtype])
    timestamp = datetime.now()
    
    df2 = sqlContext.createDataFrame([(row_num,str(timestamp),key,year,month,day,hour,executor_num,core,executor_memory)],schema = schema)

    df = df1.unionAll(df2)
    df.write.mode("overwrite").format("com.databricks.spark.avro").save('/prod/ard/hivestatus')"""     

def save_as_orc(hiveContext,country, logtype, year, month, day, hour, fill, loc_score):

    avro_base_dir = '/data/extract'
    output_base_dir = '/tmp/ard'
    date_dir = '/'.join([country, logtype, year, month, day, hour])

    avro_path = os.path.join(avro_base_dir, date_dir, fill, loc_score)                            
    output_path = os.path.join(output_base_dir, date_dir, fill, loc_score)

    df_schema = hiveContext.read.format("com.databricks.spark.avro").load('/prod/ard/schema')
    schema = df_schema.schema

    df = hiveContext.read.format("com.databricks.spark.avro").load(avro_path, schema=schema)
    df.write.mode("overwrite").format("orc").option("compression","zlib").mode("overwrite").save(output_path)

"""def main():
     

    Add arguments in the command to specify the information of the data to process
     such as country, prod_type, dt, fill and loc_score
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--year", help="year")
    parser.add_argument("--month", help="month")
    parser.add_argument("--day", help="day")
    parser.add_argument("--hour", help="hour")
    parser.add_argument("--avro_partitions",help="avro_partitions")
        
    Parse the arguments
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

    conf = SparkConf().setAppName('ScienceCoreExtension_orc' + '/' +country + '/' + logtype)
    sc = SparkContext(conf = conf)

    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)
        
    udfAddFill = udf(add_fill, StringType())
    udfAddLocScore = udf(add_locscore, StringType())  

    avro_base_dir = '/data/extract'
    output_base_dir = '/tmp/ard'
    date_dir = '/'.join([country, logtype, year, month, day, hour])

    avro_path = os.path.join(avro_base_dir, date_dir, '{fill,nf}/{tll,pos,rest}')                            
    output_path = os.path.join(output_base_dir, date_dir, fill, loc_score)

    df = hiveContext.read.format("com.databricks.spark.avro").load(avro_path)
    df = df.select('r_timestamp', 'request_id', 'pub_id', 'tsrc_id', 'sp_iab_category', 'user_iab_category',
    'user_ip', 'city', 'state', 'zip', 'country', 'latitude','longitude', 'sl_adjusted_confidence',
    'sl_json', 'fp_sic', 'fp_brand', 'uid', 'uid_type', 'uid_hash_type', 'age', 'gender', 'carrier',
    'os', 'device_os_version', 'device_make', 'device_model', 'device_year', 'device_type',
    'pub_type', 'bundle', 'sp_user_age', 'sp_user_gender','int_banner','isp', 'too_freq_uid',
    'banner_size', 'request_filled', 'pub_bid_floor', 'r_s_info', 'ad_id', 'campaign_id',
    'adgroup_id', 'creative_id', 'mslocation_id', 'ad_vendor_id', 'category',
    'matched_user_iab_category', 'matched_sp_iab_category', 'adomain',
    'creative_type', 'rtb_bucket_id', 'neptune_bucket_id', 'd_s_info', 'adv_bid_rates',
    'pub_bid_rates', 'ad_returned', 'ad_impression', 'click', 'call', 'click_to_call',
    'map', 'directions', 'website', 'description', 'sms', 'moreinfo', 'review',
    'winbid', 'save_to_app', 'save_to_ph_book', 'arrival', 'checkin', 'media',
    'coupon', 'passbook', 'app_store', 'video_start', 'video_end', 'xad_revenue',
    'pub_revenue', 'is_repeated_user', 'tracking_user_agent', 'tracking_user_ip',
    'fp_matches', 'connection_type', 'geo_type', 'app_site_domain',
    'dnt', 'geo_block_id', 'event_count', 'filter_weight','parcel_ids',udfAddFill('request_filled').alias('fill'),udfAddLocScore('sl_adjusted_confidence').alias('loc_score'))
    df.write.mode("overwrite").format("orc").option("compression","zlib").mode("overwrite").partitionBy('fill','loc_score').save(output_path)        
    #save_as_orc(hiveContext,country, logtype, year, month, day, hour,fill,loc_score)

    sc.stop()

def add_fill(fill):
    if fill == 'NOT_FILLED':
        return 'nf'
    elif fill == 'FILLED':
        return 'fill'

def add_locscore(sl_score):
    if sl_score > 94:
        return 'tll'
    elif sl_score <= 94 and sl_score >=90:
        return 'pos'
    elif sl_score < 90:
        return 'rest'"""

if __name__ == "__main__":
    main()  


