import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext 
from pyspark.sql import HiveContext
from pyspark.sql import Row
from operator import itemgetter
from math import sin, cos, sqrt, atan2, radians
import sys
import argparse

#def main(year, month, day, hour, log_type, country):
def main():
    
    conf = SparkConf().setAppName('ScienceCoreExtension')
    sc = SparkContext(conf = conf)
    
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)    

    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--year", help="year")
    parser.add_argument("--month", help="month")
    parser.add_argument("--day", help="day")
    parser.add_argument("--hour", help="hour")
    parser.add_argument("--partitions",help="partitions")
    
    args = parser.parse_args()
    if args.country:
        country = args.country
    if args.logtype:
        log_type = args.logtype
    if args.year:
        year = args.year
    if args.month:
        month = args.month
    if args.day:
        day = args.day
    if args.hour:
        hour = args.hour
    if args.partitions:
        partitions_str = args.partitions
        partitions = partitions_str.split(',')
       
          
    
    # path for tll/pos data and rest data separately
    base_dir = '/data/extract'
    date_path = '/'.join([country, log_type, year, month, day, hour])
    avro_path_tp = os.path.join(base_dir, date_path, '{fill,nf}/{tll,pos}')
    avro_path_re = os.path.join(base_dir, date_path, '{fill,nf}/{rest}')
    
    # using databricks to load avro data
    # filter data based on sl_adjusted_confidence, in order to reduce the amount of data for further process
    df_tp = hiveContext.read.format("com.databricks.spark.avro").load(avro_path_tp) 
    df = df_tp.where(df_tp.uid !='') 
    
    
    # repartition data by uid, all the request belong to the same uid will go to the same partion
    # sort rdds in each partition by uid and timestamp
    df = df.repartition('uid').sortWithinPartitions('uid','r_timestamp')
    
    
    # apply the model on each partion
    rdds = df.rdd
    list_of_requests = rdds.mapPartitions(process)
    
    
    # output from model is a list of tuples, covnert tuples back to dataframe
    df_schema  = df.schema
    df_output1 = sqlContext.createDataFrame(list_of_requests, schema = df_schema).cache()
    
    #df_output2 = df_tp.where(df_tp.sl_adjusted_confidence < 94).cache()
    
    #df_final = df_output1.unionAll(df_output2).cache()
    
    
    save_as_orc_fp(df_output1, year, month, day, hour, log_type, country,partitions)
    
    df_rest = hiveContext.read.format("com.databricks.spark.avro").load(avro_path_re).cache()
    
    save_as_orc_rest(df_rest, year, month, day, hour, log_type, country,partitions)  

    df_output1.unpersist()
    df_rest.unpersist()


  

def miles (pre, cur):
    R = 6373.0
    lat1 = float(pre[0])
    lon1 = float(pre[1])
    lat2 = float(cur[0])
    lon2 = float(cur[1])
    rad_lat1 = radians(lat1)
    rad_lon1 = radians(lon1)
    rad_lat2 = radians(lat2)
    rad_lon2 = radians(lon2)
    
    dlon = rad_lon2 - rad_lon1
    dlat = rad_lat2 - rad_lat1
    
    a = sin(dlat / 2) **2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    
    if a >0 and a < 1:
        c = 2 * atan2(sqrt(a),sqrt(1-a))
    
        distance = R * c / 1.60934
    else:
        distance = 0

    return distance    


def flat_kernel_update(pre_cluster, cur_cluster, requests):

    pre_len = len(requests[-1]) 
    total_len = pre_len + 1
    sum_lat = pre_cluster[0] * pre_len + cur_cluster[0]
    sum_lon = pre_cluster[1] * pre_len + cur_cluster[1]
    lat = sum_lat / total_len
    lon = sum_lon / total_len
    
    return (lat, lon)

def get_clusters(uid_requests):
    
    clusters = []
    requests = []
    cluster_request = {}
    try:           
        for i in range(len(uid_requests)): 
            r = uid_requests[i]             
            if int(r['sl_adjusted_confidence']) < 94:
                continue
            
            if len(clusters) == 0:

                pre_cluster = [float(r['latitude']), float(r['longitude']), int(r['r_timestamp'])] 
                clusters.append(pre_cluster)
                pre_requests = []
                pre_requests.append(i)
                requests.append(pre_requests)
                ## save the clusters and corresponding requests in separate list to modify it 
                ## more easily
        
            else:

                cur_cluster = [float(r['latitude']), float(r['longitude']), int(r['r_timestamp'])]
                flag = False

                for i in range(len(clusters)):

                    cluster = clusters[i]

                    if cur_cluster[0] == cluster[0] and cur_cluster[1] == cluster[1]:
                        requests[i].append(i)
                        clusters[i] = cur_cluster
                        flag = True
                        break

                    else:
                    ## the threshhold can be a distance and speed at the same time. 
                    ## right now only distance is used, speed will be added into the model later.
                        if miles(cluster, cur_cluster) < 2:
                        
                            weighted_cluster = flat_kernel_update(cluster,cur_cluster,requests)
                            clusters[i] = [weighted_cluster[0],weighted_cluster[1], cur_cluster[2]]
                            requests[i].append(i)
                            flag = True
                            break
                if not flag:

                    clusters.append(cur_cluster)
                    cur_requests = []
                    cur_requests.append(i)
                    requests.append(cur_requests)
                                    
        for j in range(len(clusters)):

            cluster_request[tuple(clusters[j])] = requests[j]
        
        return cluster_request

    except ValueError:
            
        return 'error'

def update_r_s_info(cluster_requests, uid_requests):
    if len(cluster_requests) == 0:
                
        for r in uid_requests:
            r['r_s_info'] = ''
                 
    elif len(cluster_requests) == 1: 
                         
        for r in uid_requests:
            if r['sl_adjusted_confidence'] >= 94:
                r['r_s_info'] = '{"abnormal_req":0}'
            else:
                r['r_s_info'] = ''

    else:
                               
        if float(len(cluster_requests[0][1])) / len(cluster_requests[1][1]) > 2.0:
                    
            for i in cluster_requests[0][1]:
                r = uid_requests[i]
                r['r_s_info'] = '{"abnormal_req":0}'   
                
            for i in range(1, len(cluster_requests)):
                for j in cluster_requests[i][1]:
                    r = uid_requests[j]
                    r['r_s_info'] = '{"abnormal_req":1}'

            for r in uid_requests:
                if r['sl_adjusted_confidence'] < 94:
                    r['r_s_info'] = ''  

        else:

            for r in uid_requests:                        
                r['r_s_info'] = ''
                
    return uid_requests

def build_tuples(dic):
    # the order of the command is the same as the schema, the order can't be changed
    t_list = []
    t_list.append(dic['r_timestamp']) 
    t_list.append(dic['request_id']) 
    t_list.append(dic['pub_id']) 
    t_list.append(dic['tsrc_id']) 
    t_list.append(dic['sp_iab_category']) 
    
    t_list.append(dic['user_iab_category']) 
    t_list.append(dic['user_ip']) 
    t_list.append(dic['city']) 
    t_list.append(dic['state'])
    t_list.append(dic['zip']) 
    t_list.append(dic['country']) 
    
    t_list.append(dic['latitude']) 
    t_list.append(dic['longitude']) 
    t_list.append(dic['sl_adjusted_confidence']) 
    t_list.append(dic['sl_json']) 
    t_list.append(dic['fp_sic']) 
    
    t_list.append(dic['fp_brand']) 
    t_list.append(dic['uid']) 
    t_list.append(dic['uid_type']) 
    t_list.append(dic['uid_hash_type']) 
    t_list.append(dic['age']) 
    
    t_list.append(dic['gender']) 
    t_list.append(dic['carrier']) 
    t_list.append(dic['os']) 
    t_list.append(dic['device_os_version']) 
    t_list.append(dic['device_make']) 
    
    t_list.append(dic['device_model']) 
    t_list.append(dic['device_year']) 
    t_list.append(dic['device_type']) 
    t_list.append(dic['pub_type']) 
    t_list.append(dic['bundle'])
    
    t_list.append(dic['sp_user_age']) 
    t_list.append(dic['sp_user_gender']) 
    t_list.append(dic['int_banner']) 
    t_list.append(dic['isp']) 
    t_list.append(dic['too_freq_uid'])
    
    t_list.append(dic['banner_size']) 
    t_list.append(dic['request_filled']) 
    t_list.append(dic['pub_bid_floor']) 
    t_list.append(dic['r_s_info']) 
    t_list.append(dic['ad_id'])
    
    t_list.append(dic['campaign_id']) 
    t_list.append(dic['adgroup_id']) 
    t_list.append(dic['creative_id']) 
    t_list.append(dic['mslocation_id']) 
    t_list.append(dic['ad_vendor_id'])
    
    t_list.append(dic['category']) 
    t_list.append(dic['matched_user_iab_category']) 
    t_list.append(dic['matched_sp_iab_category']) 
    t_list.append(dic['adomain']) 
    t_list.append(dic['creative_type'])
    
    t_list.append(dic['rtb_bucket_id']) 
    t_list.append(dic['neptune_bucket_id']) 
    t_list.append(dic['d_s_info']) 
    t_list.append(dic['adv_bid_rates']) 
    t_list.append(dic['pub_bid_rates'])
    
    t_list.append(dic['ad_returned']) 
    t_list.append(dic['ad_impression']) 
    t_list.append(dic['click']) 
    t_list.append(dic['call']) 
    t_list.append(dic['click_to_call'])
    
    t_list.append(dic['map']) 
    t_list.append(dic['directions']) 
    t_list.append(dic['website']) 
    t_list.append(dic['description']) 
    t_list.append(dic['sms'])
    
    t_list.append(dic['moreinfo']) 
    t_list.append(dic['review']) 
    t_list.append(dic['winbid']) 
    t_list.append(dic['save_to_app']) 
    t_list.append(dic['save_to_ph_book'])
    
    t_list.append(dic['arrival']) 
    t_list.append(dic['checkin']) 
    t_list.append(dic['media']) 
    t_list.append(dic['coupon']) 
    t_list.append(dic['passbook'])
                      
    t_list.append(dic['app_store']) 
    t_list.append(dic['video_start']) 
    t_list.append(dic['video_end']) 
    t_list.append(dic['xad_revenue']) 
    t_list.append(dic['pub_revenue'])
    
    t_list.append(dic['is_repeated_user']) 
    t_list.append(dic['tracking_user_agent']) 
    t_list.append(dic['tracking_user_ip']) 
      
    fp_list = []
    if dic['fp_matches']:
        for row in dic['fp_matches']:
            m_list = []
            m_list.append(row['proximity_mode'])
            m_list.append(row['fp_brand'])
            m_list.append(row['fp_sic'])
            m_list.append(row['weight'])
            m_list.append(row['hash_key'])
            m_list.append(row['is_poly'])
            m_list.append(row['block_id'])

            fp_list.append(tuple(m_list))
    else:
        fp_list.append(None)
    
    t_list.append(fp_list)
    
    t_list.append(dic['connection_type']) 
    t_list.append(dic['geo_type']) 
    t_list.append(dic['app_site_domain']) 
    t_list.append(dic['dnt']) 
    t_list.append(dic['geo_block_id'])
    
    t_list.append(dic['event_count']) 
    t_list.append(dic['filter_weight'])
    
    return tuple(t_list)


def process(iterator):
    
    try:
        count = 0
        uid_requests = []
        pre_key = ''
        process = 0
        all_requests = []
        for row in iterator: 
            cur_key = row['uid']
            
            if pre_key == '':
                
                uid_requests.append(row.asDict())
                pre_key = cur_key
                
            elif pre_key == cur_key:
                
                uid_requests.append(row.asDict())
            
            elif pre_key != cur_key:
                                
                cluster_request = get_clusters(uid_requests)
                cluster_requests = sorted(cluster_request.items(), key = lambda x:len(x[1]),reverse = True)                
                requests = update_r_s_info(cluster_requests, uid_requests)
                pre_key = cur_key
                uid_requests = []
                
                for r in requests:
                    r_tuple = build_tuples(r)
                    all_requests.append(r_tuple)
                
        return all_requests
                
    except ValueError:
        return 'error'


def save_as_orc_fp(df, year, month, day, hour, log_type, country,partitions):
    
    fill_status = {'fill':'FILLED','nf':'NOT_FILLED'}
    
    base_dir = os.path.join('/user',os.environ['USER'], 'tmp', 'ard', 'science_core_ex')
    date_dir = os.path.join(base_dir,country, log_type, year, month, day, hour )
    
    
    for fill in fill_status.keys():
        df0 = df.where(df.request_filled == fill_status[fill]).cache()
        tll_partition = '-'.join([fill,'tll'])
        pos_partition = '-'.join([fill,'pos'])
        if tll_partition in partitions:                
            path_tll = os.path.join(date_dir, fill,'tll')
            df1 = df0.where(df0.sl_adjusted_confidence > 94).write.format("orc").save(path_tll)
            
        if pos_partition in partitions:
            path_pos = os.path.join(date_dir, fill,'pos')
            df2 = df0.where(df0.sl_adjusted_confidence <= 94).write.format("orc").save(path_pos)
        df0.unpersist()
                
def save_as_orc_rest(df, year, month, day, hour, log_type, country,partitions):
    
    fill_status = {'fill':'FILLED','nf':'NOT_FILLED'}
    
    base_dir = os.path.join('/user',os.environ['USER'], 'tmp', 'ard', 'science_core_ex')
    date_dir = os.path.join(base_dir,country, log_type, year, month, day, hour )
    
    for fill in fill_status.keys(): 
        df0 = df.where(df.request_filled == fill_status[fill]).cache()
        rest_partition = '-'.join([fill,'rest'])

        if rest_partition in partitions:                
            path_rest = os.path.join(date_dir, fill,'rest')
            df1 = df0.write.format("orc").save(path_rest)
        df0.unpersist()

if __name__ == "__main__":
    main()   



















































