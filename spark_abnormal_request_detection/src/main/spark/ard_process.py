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

#def main(year, month, day, hour, log_type, country):
def main():
    
    conf = SparkConf().setAppName('ScienceCoreExtension')
    sc = SparkContext(conf = conf)
    
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)    
    
    # Add arguments in the command to specify the information of the data to process
    # such as country, prod_type, dt, fill and loc_score
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--year", help="year")
    parser.add_argument("--month", help="month")
    parser.add_argument("--day", help="day")
    parser.add_argument("--hour", help="hour")
    parser.add_argument("--partitions",help="partitions")
    
    ## Parse the arguments 
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
       
          
    
    # load tll and pos data, the model will only process this part of data
    base_dir = '/data/extract'
    date_path = '/'.join([country, log_type, year, month, day, hour])
    avro_path_tp = os.path.join(base_dir, date_path, '{fill,nf}/{tll,pos}')
    
    
    # using databricks to load avro data
    # filter data based on sl_adjusted_confidence, in order to reduce the amount of data for further process
    df_tp = hiveContext.read.format("com.databricks.spark.avro").load(avro_path_tp) 
    rdds = df_tp.where(df_tp.uid !='').repartition('uid').sortWithinPartitions('uid','r_timestamp').rdd
    
    
    # repartition data by uid, all the request belong to the same uid will go to the same partion
    # sort rdds in each partition by uid and timestamp
    
    
    # apply the model on each partion
    
    list_of_requests = rdds.mapPartitions(process)
    
    
    # output from model is a list of tuples, covnert tuples back to dataframe
    df_schema  = df_tp.schema
    df_output1 = sqlContext.createDataFrame(list_of_requests, schema = df_schema)
    
    #df_output2 = df_tp.where(df_tp.sl_adjusted_confidence < 94).cache()
    
    #df_final = df_output1.unionAll(df_output2).cache()
    
    
    save_as_orc_fp(df_output1, year, month, day, hour, log_type, country,partitions)
    df_output1.unpersist()
    
    # load and save the rest data
    avro_path_re_fill = os.path.join(base_dir, date_path, 'fill/{rest}')
    avro_path_re_nf = os.path.join(base_dir, date_path, 'nf/{rest}')
    
    if (has(avro_path_re_fill)):
        df_rest_fill = hiveContext.read.format("com.databricks.spark.avro").load(avro_path_re_fill)
        save_as_orc_rest(df_rest_fill, year, month, day, hour, log_type, country,partitions,'fill')

    if (has(avro_path_re_nf)):
        df_rest_nf = hiveContext.read.format("com.databricks.spark.avro").load(avro_path_re_nf)
        save_as_orc_rest(df_rest_nf, year, month, day, hour, log_type, country,partitions,'nf')  



def save_as_orc_fp(df, year, month, day, hour, log_type, country,partitions):
    # save spark dataframe as orc file, for tll and pos data

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

def _exec(cmd, args, norun=False):
    """Execute an HDFS command"""
    retval = True
    popen_args = ["hdfs", "dfs", cmd, args]
    popen_args = " ".join(popen_args)

    if (norun):
        logging.debug("EXEC> {} (NORUN)".format(popen_args))
    else:
        logging.debug("EXEC> {}".format(popen_args))
        try:
            p = subprocess.Popen(popen_args, shell=True, 
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            p.communicate()
            if (p.returncode == 0):
                retval = True
            else:
                retval = False 
                errstr = p.stderr.read()
                if (len(errstr) > 0):
                    logging.debug("- Error: {}".format(errstr))

        except subprocess.CalledProcessError:
            logging.error("Subprocess Error")
            retval = False

        except :
            #logging.error("Other Error")
            retval = False

    return retval 


def has(path):
    """Check the existence of specified path"""
    return _exec("-ls", path)


def save_as_orc_rest(df, year, month, day, hour, log_type, country,partitions,fill):
    
    # save spark dataframe as orc file, for rest data
    base_dir = os.path.join('/user',os.environ['USER'], 'tmp', 'ard', 'science_core_ex')
    date_dir = os.path.join(base_dir,country, log_type, year, month, day, hour )
        
    rest_partition = '-'.join([fill,'rest'])

    if rest_partition in partitions:                
        path_rest = os.path.join(date_dir, fill,'rest')
        df1 = df.write.format("orc").save(path_rest)
        
               
"""def save_as_orc_rest(df, year, month, day, hour, log_type, country,partitions):
    
    fill_status = {'fill':'FILLED','nf':'NOT_FILLED'}
    
    base_dir = os.path.join('/user',os.environ['USER'], 'tmp', 'ard', 'science_core_ex')
    date_dir = os.path.join(base_dir,country, log_type, year, month, day, hour )
    
    for fill in fill_status.keys(): 
        df0 = df.where(df.request_filled == fill_status[fill]).cache()
        rest_partition = '-'.join([fill,'rest'])

        if rest_partition in partitions:                
            path_rest = os.path.join(date_dir, fill,'rest')
            df1 = df0.write.format("orc").save(path_rest)
        df0.unpersist()"""

  

def miles (pre, cur):
    # Caculate the distance between two latitudes and longitudes
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
    # Update the latitude and longitude of the centroid of a cluster based on the weight ratio
    pre_len = len(requests[-1]) 
    total_len = pre_len + 1
    sum_lat = pre_cluster[0] * pre_len + cur_cluster[0]
    sum_lon = pre_cluster[1] * pre_len + cur_cluster[1]
    lat = sum_lat / total_len
    lon = sum_lon / total_len
    
    return (lat, lon)

def get_clusters(uid_requests):
    # get the location clusters and corresponding requests with all the requests from one uid    
    clusters = []
    requests = []
    cluster_request = {}
    try:           
        for i in range(len(uid_requests)): 
            r = uid_requests[i]             
            if int(r['sl_adjusted_confidence']) < 94:
                continue
            
            if len(clusters) == 0:
                # when there is no cluster, we initialize the first cluster with the first request location
                pre_cluster = [float(r['latitude']), float(r['longitude']), int(r['r_timestamp'])] 
                clusters.append(pre_cluster)
                pre_requests = []
                pre_requests.append(i)
                requests.append(pre_requests)
                
        
            else:

                cur_cluster = [float(r['latitude']), float(r['longitude']), int(r['r_timestamp'])]
                flag = False

                for i in range(len(clusters)):

                    cluster = clusters[i]
                    # compare the location of the current request with all the existing clusters
                    # merge the this request into one of them and update the centroid of the cluster if the distance is small enough. 
                    # Otherwise, create a new cluser 
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
        # return a dictionary, the key is the centroid of a cluster, the values are all the requests in this cluster
        return cluster_request

    except ValueError:
            
        return 'error'

def update_r_s_info(cluster_requests, uid_requests):
    # update the abnormal request tag in r_s_info field

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
    # convert the dictionary into a tuple
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
    # nf and fill have different schemas, nf has less fields than fill data
    # sometimes the fill data is missing, wo we need to check whether we have the following fields before we convert it to a tuple
    if 'ad_id' in dic.keys():    
        t_list.append(dic['ad_id'])
    
    if 'campaign_id' in dic.keys():
        t_list.append(dic['campaign_id']) 
    if 'adgroup_id' in dic.keys():
        t_list.append(dic['adgroup_id'])
    if 'creative_id' in dic.keys(): 
        t_list.append(dic['creative_id']) 
    if 'mslocation_id' in dic.keys():
        t_list.append(dic['mslocation_id'])
    if 'ad_vendor_id' in dic.keys():
        t_list.append(dic['ad_vendor_id'])

    if 'category' in dic.keys():
        t_list.append(dic['category']) 
    if 'matched_user_iab_category' in dic.keys():
        t_list.append(dic['matched_user_iab_category'])
    if 'matched_sp_iab_category' in dic.keys(): 
        t_list.append(dic['matched_sp_iab_category']) 
    if 'adomain' in dic.keys():
        t_list.append(dic['adomain']) 
    if 'creative_type' in dic.keys():
        t_list.append(dic['creative_type'])

    if 'rtb_bucket_id' in dic.keys():
        t_list.append(dic['rtb_bucket_id']) 
    if 'neptune_bucket_id' in dic.keys():
        t_list.append(dic['neptune_bucket_id'])
    if 'd_s_info' in dic.keys(): 
        t_list.append(dic['d_s_info']) 
    if 'adv_bid_rates' in dic.keys():
        t_list.append(dic['adv_bid_rates']) 
    if 'pub_bid_rates' in dic.keys():
        t_list.append(dic['pub_bid_rates'])
    
    if 'ad_returned' in dic.keys():
        t_list.append(dic['ad_returned'])
    if 'ad_impression' in dic.keys(): 
        t_list.append(dic['ad_impression']) 
    if 'click' in dic.keys():
        t_list.append(dic['click'])
    if 'call' in dic.keys(): 
        t_list.append(dic['call']) 
    if 'click_to_call' in dic.keys():
        t_list.append(dic['click_to_call'])
    
    if 'map' in dic.keys():
        t_list.append(dic['map'])
    if 'directions' in dic.keys(): 
        t_list.append(dic['directions'])
    if 'website' in dic.keys(): 
        t_list.append(dic['website'])
    if 'description' in dic.keys(): 
        t_list.append(dic['description'])
    if 'sms' in dic.keys(): 
        t_list.append(dic['sms'])
    
    if 'moreinfo' in dic.keys():
        t_list.append(dic['moreinfo']) 
    if 'review' in dic.keys():
        t_list.append(dic['review']) 
    if 'winbid' in dic.keys():
        t_list.append(dic['winbid'])
    if 'save_to_app' in dic.keys(): 
        t_list.append(dic['save_to_app']) 
    if 'save_to_ph_book' in dic.keys():
        t_list.append(dic['save_to_ph_book'])
    
    if 'arrival' in dic.keys():
        t_list.append(dic['arrival']) 
    if 'checkin' in dic.keys():
        t_list.append(dic['checkin']) 
    if 'media' in dic.keys():
        t_list.append(dic['media'])
    if 'coupon' in dic.keys(): 
        t_list.append(dic['coupon']) 
    if 'passbook' in dic.keys():
        t_list.append(dic['passbook'])

    if 'app_store' in dic.keys():                 
        t_list.append(dic['app_store']) 
    if 'video_start' in dic.keys():
        t_list.append(dic['video_start']) 
    if 'video_end' in dic.keys():
        t_list.append(dic['video_end']) 
    if 'xad_revenue' in dic.keys():
        t_list.append(dic['xad_revenue']) 
    if 'pub_revenue' in dic.keys():
        t_list.append(dic['pub_revenue'])
    
    t_list.append(dic['is_repeated_user'] ) 
    if 'tracking_user_agent' in dic.keys():   
        t_list.append(dic['tracking_user_agent']) 
    if 'tracking_user_ip' in dic.keys():  
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
    # Major function for abnormal request detection model
    try:
        count = 0
        uid_requests = []
        pre_key = ''
        process = 0
        all_requests = []
        # each row in the iterator is one request, it is a row object
        # Since they have been sorted by uid and r_timestamp already, we only need to compare the consecutive requests
        for row in iterator: 
            cur_key = row['uid']
            
            if pre_key == '':
                # convert the row into dictionary to modify the value, since row is immutable
                uid_requests.append(row.asDict())
                pre_key = cur_key
                
            elif pre_key == cur_key:
                
                uid_requests.append(row.asDict())
            
            elif pre_key != cur_key:
                # When two keys are different, it means all the requests belong to this uid have been saved to the uid_requests list
                # Next step is to process these requests with the model               
                cluster_request = get_clusters(uid_requests)
                # sort the clusters based on the number of requests in it
                cluster_requests = sorted(cluster_request.items(), key = lambda x:len(x[1]),reverse = True)  
                # update the r_s_info field              
                requests = update_r_s_info(cluster_requests, uid_requests)

                pre_key = cur_key
                uid_requests = []
                
                # convert the dictionary into a tuple, since SPARK 2.0.0 doesn't support converting dictionary to dataframe directly
                for r in requests:
                    r_tuple = build_tuples(r)
                    all_requests.append(r_tuple)
                
        return all_requests
                
    except ValueError:
        return 'error'




if __name__ == "__main__":
    main()   



















































