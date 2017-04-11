import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext 
from pyspark.sql import HiveContext
import sys
import argparse
import logging
from pyspark.sql.functions import *

def main():
    conf = SparkConf().setAppName('ScienceCoreExtension_Join')
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
    parser.add_argument("--abd_partitions",help="abd_partitions")
    
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
    if args.abd_partitions:
        partitions_str = args.abd_partitions
        partitions = partitions_str.split(',')  

    hours = []
    hour_partitions = {}
    
    for hp in partitions:
        (hour, partition) = hp.split('*')
        hours.append(hour)
        if hour in hour_partitions:
            hour_partitions[hour].append(partition)
        else:
            partition_list = []
            partition_list.append(partition)
            hour_partitions[hour] = partition_list
    
    
    fill_status = ['fill', 'nf']
    locscore_status = ['tll', 'pos']

    for hour in hours:
        abd_partitions = hour_partitions[hour]
        for fill in fill_status:
            for loc_score in locscore_status:
                partition = '-'.join([fill, loc_score])
                if partition in abd_partitions and fill == 'fill':
                    merge_fill(hiveContext, country, logtype, year, month, day, hour, loc_score)

                if partition in abd_partitions and fill == 'nf':
                    merge_nf(hiveContext, country, logtype, year, month, day, hour, loc_score)

        save_rest_orc(hiveContext,country, logtype, year, month, day, hour, avro_partitions)
    
    sc.stop()

def merge_fill(hiveContext,country, logtype, year, month, day, hour, loc_score):

    avro_base_dir = '/data/extract'
    ard_base_dir = '/prod/ard/abnormal_req'
    output_base_dir = '/tmp/ard'
    date_path = '/'.join([country, logtype, year, month, day, hour])

    locscores = {'tll':'loc_score=95','pos':'loc_score=94'}   
    
    avro_path = os.path.join(avro_base_dir, date_path, 'fill', loc_score) 
    df_all = hiveContext.read.format("com.databricks.spark.avro").load(avro_path)
            
    ard_path = os.path.join(ard_base_dir, date_path,'fill=FILLED',locscores[loc_score]) 
    df_ard = hiveContext.read.format("orc").load(ard_path)
            
    df = df_all.join(df_ard, 'request_id','left_outer').drop('r_s_info').select('r_timestamp', 'request_id', 'pub_id', 'tsrc_id', 'sp_iab_category', 'user_iab_category',
    'user_ip', 'city', 'state', 'zip', 'country', 'latitude','longitude', 'sl_adjusted_confidence',
    'sl_json', 'fp_sic', 'fp_brand', 'uid', 'uid_type', 'uid_hash_type', 'age', 'gender', 'carrier',
    'os', 'device_os_version', 'device_make', 'device_model', 'device_year', 'device_type',
    'pub_type', 'bundle', 'sp_user_age', 'sp_user_gender','int_banner','isp', 'too_freq_uid',
    'banner_size', 'request_filled', 'pub_bid_floor', col('r_s_info1').alias('r_s_info'), 'ad_id', 'campaign_id',
    'adgroup_id', 'creative_id', 'mslocation_id', 'ad_vendor_id', 'category',
    'matched_user_iab_category', 'matched_sp_iab_category', 'adomain',
    'creative_type', 'rtb_bucket_id', 'neptune_bucket_id', 'd_s_info', 'adv_bid_rates',
    'pub_bid_rates', 'ad_returned', 'ad_impression', 'click', 'call', 'click_to_call',
    'map', 'directions', 'website', 'description', 'sms', 'moreinfo', 'review',
    'winbid', 'save_to_app', 'save_to_ph_book', 'arrival', 'checkin', 'media',
    'coupon', 'passbook', 'app_store', 'video_start', 'video_end', 'xad_revenue',
    'pub_revenue', 'is_repeated_user', 'tracking_user_agent', 'tracking_user_ip',
    'fp_matches', 'connection_type', 'geo_type', 'app_site_domain',
    'dnt', 'geo_block_id', 'event_count', 'filter_weight','parcel_ids')
            
    output_path = os.path.join(output_base_dir, date_path, 'fill', loc_score)
    df.write.mode("overwrite").format("orc").option("compression","zlib").mode("overwrite").save(output_path)

def merge_nf(hiveContext,country, logtype, year, month, day, hour, loc_score):

    avro_base_dir = '/data/extract'
    ard_base_dir = '/prod/ard/abnormal_req'
    output_base_dir = '/tmp/ard'
    date_path = '/'.join([country, logtype, year, month, day, hour])

    locscores = {'tll':'loc_score=95','pos':'loc_score=94'}   
    
    avro_path = os.path.join(avro_base_dir, date_path, 'nf', loc_score) 
    df_all = hiveContext.read.format("com.databricks.spark.avro").load(avro_path)
            
    ard_path = os.path.join(ard_base_dir, date_path,'fill=NOT_FILLED',locscores[loc_score]) 
    df_ard = hiveContext.read.format("orc").load(ard_path)
            
    df = df_all.join(df_ard, 'request_id','left_outer').drop('r_s_info').select('r_timestamp', 'request_id', 'pub_id', 'tsrc_id', 'sp_iab_category', 'user_iab_category',
    'user_ip', 'city', 'state', 'zip', 'country', 'latitude','longitude', 'sl_adjusted_confidence',
    'sl_json', 'fp_sic', 'fp_brand', 'uid', 'uid_type', 'uid_hash_type', 'age', 'gender', 'carrier',
    'os', 'device_os_version', 'device_make', 'device_model', 'device_year', 'device_type',
    'pub_type', 'bundle', 'sp_user_age', 'sp_user_gender','int_banner','isp', 'too_freq_uid',
    'banner_size', 'request_filled', 'pub_bid_floor', col('r_s_info1').alias('r_s_info'),'is_repeated_user','fp_matches', 'connection_type', 'geo_type','app_site_domain',
    'dnt', 'geo_block_id', 'event_count', 'filter_weight','parcel_ids') 
            
    output_path = os.path.join(output_base_dir, date_path, 'nf', loc_score)
    df.write.mode("overwrite").format("orc").option("compression","zlib").mode("overwrite").save(output_path)

def save_rest_orc(hiveContext,country, logtype, year, month, day, hour, avro_partitions):

    avro_base_dir = '/data/extract'
    output_base_dir = '/tmp/ard'
    date_dir = '/'.join([country, logtype, year, month, day, hour])
    
    fill_status = ['fill','nf']
    for fill in fill_status:        
        rest_partition = '-'.join([fill,'rest'])
        if rest_partition in avro_partitions:
            avro_path_re = os.path.join(avro_base_dir, date_dir, fill, 'rest')                            
            output_path = os.path.join(output_base_dir, date_dir, fill,'rest')
            df_rest = hiveContext.read.format("com.databricks.spark.avro").load(avro_path_re)
            df_rest.write.mode("overwrite").format("orc").option("compression","zlib").mode("overwrite").save(output_path)

if __name__ == "__main__":
    main()  
           



