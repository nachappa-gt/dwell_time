"""

This program joins the abnormal request data (generated by ard_process.py)
with the science core data.

Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.

@author: xiangling, victor

"""

import sys
sys.path.append('/home/xad/ard/python')
sys.path.append('/home/xad/share/python')
import argparse
import logging
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col

from xad.common.conf import Conf

DEFAULT_CONFIG_DIRS = "/home/xad/ard/config:/home/xad/share/config"
DEFAULT_CONFIG_FILE = "ard.properties"


def merge(hiveContext,country, logtype, date, hour, fill, loc_score,
          input_dir, abd_dir, output_dir):
    """Merge abnormal records with the full science_core data"""

    fill_map = {'fill':'fill=FILLED', 'nf':'fill=NOT_FILLED'}
    loc_score_map = {'tll':'loc_score=95','pos':'loc_score=94'}  
    
    ard_fill_folder = fill_map[fill]
    ard_loc_score_folder = loc_score_map[loc_score]

    avro_path = os.path.join(input_dir, fill, loc_score) 
    ard_path = os.path.join(abd_dir, ard_fill_folder, ard_loc_score_folder) 
    output_path = os.path.join(output_dir, fill, loc_score)

    logging.info("# Merging: {}/{}".format(fill,loc_score))
    logging.info("- ard path = {}".format(ard_path))
    logging.info("- avro path = {}".format(avro_path))
    logging.info("- output path = {}".format(output_path))
    
    # Use an explicit schema for two purposes:
    # 1. handle nf
    # 2. handle fp_matches changes.
    df_schema = hiveContext.read.format("com.databricks.spark.avro")\
        .load('/prod/ard/schema')
    schema = df_schema.schema

    # The full, original science core data
    df_all = hiveContext.read.format("com.databricks.spark.avro")\
        .load(avro_path, schema=schema)
            
    # Data for abnormal requests
    df_ard = hiveContext.read.format("orc").load(ard_path)
        
    # FIXME: some original r_s_info may get lost with the join.  The logic should be
    #        r_s_info = (r_s_info1 is not null) ? r_s_info1 : r_s_info.
    df = df_all.join(df_ard, 'request_id','left_outer').drop('r_s_info').select(
        # 1-10
        'r_timestamp', 'request_id', 'pub_id', 'tsrc_id', 'sp_iab_category',
        'user_iab_category', 'user_ip', 'city', 'state', 'zip',
        # 11-20
        'country', 'latitude','longitude', 'sl_adjusted_confidence', 'sl_json',
        'fp_sic', 'fp_brand', 'uid', 'uid_type', 'uid_hash_type',
        # 21-30
        'age', 'gender', 'carrier', 'os', 'device_os_version',
        'device_make', 'device_model', 'device_year', 'device_type', 'pub_type',
        # 31-40        
        'bundle', 'sp_user_age', 'sp_user_gender','int_banner', 'isp',
        'too_freq_uid', 'banner_size', 'request_filled', 'pub_bid_floor',
        col('r_s_info1').alias('r_s_info'),
        # 41-50
        'ad_id', 'campaign_id', 'adgroup_id', 'creative_id', 'mslocation_id',
        'ad_vendor_id', 'category', 'matched_user_iab_category',
        'matched_sp_iab_category', 'adomain',
        # 51-60
        'creative_type', 'rtb_bucket_id', 'neptune_bucket_id', 'd_s_info',
        'adv_bid_rates', 'pub_bid_rates', 'ad_returned', 'ad_impression',
        'click', 'call',
        # 61-70                
        'click_to_call', 'map', 'directions', 'website', 'description',
        'sms', 'moreinfo', 'review', 'winbid', 'save_to_app',
        # 71-80
        'save_to_ph_book', 'arrival', 'checkin', 'media', 'coupon',
        'passbook', 'app_store', 'video_start', 'video_end', 'xad_revenue',
        # 81-90
        'pub_revenue', 'is_repeated_user', 'tracking_user_agent',
        'tracking_user_ip','fp_matches', 'connection_type', 'geo_type',
        'app_site_domain', 'dnt', 'geo_block_id', 
        # 91-
        'event_count', 'filter_weight','parcel_ids', 'cookie_uid', 'matched_poitags',
        'location_extensions', 'isp_match', 'beacons', 'location', 'ha_poi_info'
        )

    df.write.mode("overwrite").format("orc").option("compression","zlib"). \
        mode("overwrite").save(output_path)        


def save_orc(hiveContext, country, logtype, date, hour, fill, loc_score, 
             input_dir, output_dir):
    """Save as ORC"""

    avro_path = os.path.join(input_dir, fill, loc_score)
    output_path = os.path.join(output_dir, fill, loc_score)

    logging.info("# AVRO to ORC: {}/{}".format(fill,loc_score))
    logging.info("- avro path = {}".format(avro_path))
    logging.info("- orc path = {}".format(output_path))
    
    df_schema = hiveContext.read.format("com.databricks.spark.avro").\
        load('/prod/ard/schema')
    schema = df_schema.schema

    df = hiveContext.read.format("com.databricks.spark.avro").\
        load(avro_path,schema = schema)
   
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
    parser.add_argument("--abd_partitions",help="abd_partitions")
    parser.add_argument("--executor_mem",help="executor_memory")
    parser.add_argument("--executors_num",help="num_executors")
    parser.add_argument("--exe_cores",help="executor_cores")
    parser.add_argument("--input_dir",help="science core source dir")
    parser.add_argument("--abd_dir",help="abd dir")
    parser.add_argument("--output_dir",help="output_dir")

    # Flags
    parser.add_argument('-d', '--debug', action='store_true', help="Turn on debugging")
    parser.add_argument('-n',  '--norun', action='store_true', help="No run.")

    # Parse the arguments
    opt = parser.parse_args()
    return(opt)


def init_logging(opt):
    """Initialize logging"""
    global logger

    # Config
    level = logging.DEBUG if (opt.debug) else logging.INFO
    fmt = ("%(asctime)s %(levelname)s [%(module)s.%(funcName)s] %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt, datefmt=datefmt, level=level)

    # IPython specific setting
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.debug('Initialized logger')

def load_configuration(opt):
    """Load configuration files"""
    conf = Conf()
    conf.load(opt.config, opt.config_dirs)
    return(conf)

            
def get_app_name(opt):
    """Get the application name"""
    return os.path.join('ard_join', opt.country,
                        opt.logtype, opt.date, opt.hour)
                        
def main():
    # Handle arguments
    opt = parse_arguments()
    avro_partitions = opt.avro_partitions.split(',')
    abd_partitions = opt.abd_partitions.split(',')
    
    # Logging and Config
    init_logging(opt)
#    cfg = load_configuration(opt)

    # Get the context
    appName = get_app_name(opt)
    conf = SparkConf().setAppName(appName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(conf = conf)
    hiveContext = HiveContext(sc) 

    #fill_status = re.split("\s+", cfg.get('science_core.fill.folders'))
    #locscore_status = re.split("\s+", cfg.get('science_core.sl.folders'))
    fill_status = ['fill', 'nf']
    locscore_status = ['tll', 'pos', 'rest']

    logging.info("## Running {} ... ".format(appName))
    logging.info("- fill status = {} ... ".format(fill_status))
    logging.info("- locscore status = {} ... ".format(locscore_status))
    logging.info("- abd_partitions = {} ... ".format(abd_partitions))
    logging.info("- avro_partitions = {} ... ".format(avro_partitions))

    for fill in fill_status:
        for loc_score in locscore_status:
            partition = '-'.join([fill, loc_score])
            logging.info("# Processing '{}' ...".format(partition))
            if (partition in abd_partitions):
                merge(hiveContext, opt.country, opt.logtype, opt.date, opt.hour,
                      fill, loc_score, opt.input_dir, 
                      opt.abd_dir, opt.output_dir)

            elif partition in avro_partitions:
                save_orc(hiveContext, opt.country, opt.logtype, 
                         opt.date, opt.hour, fill,
                         loc_score, opt.input_dir, opt.output_dir)
            else:
                logging.warn("*** INVALID PARTITION {} ***".format(partition))

    logging.info("## Finished ard_join!")
    sc.stop()


if __name__ == "__main__":
    main()  




