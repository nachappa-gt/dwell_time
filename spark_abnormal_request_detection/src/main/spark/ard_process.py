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
    
    conf = SparkConf().setAppName('ScienceCoreExtension_Model')
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
    
    """Parse the arguments """
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
    
    """Load tll and pos data, the model will only process this part of data"""
    base_dir = '/data/extract'
    date_path = '/'.join([country, log_type, year, month, day, hour])

    """Using databricks to load avro data"""
    avro_path_tp = os.path.join(base_dir, date_path, '{fill,nf}/{tll,pos}')  
    df_tp = hiveContext.read.format("com.databricks.spark.avro").load(avro_path_tp)
    df = df_tp.where((df_tp.uid !='') & (df_tp.sl_adjusted_confidence >=94))

    """ Repartition data by uid, all the request belong to the same uid will go to the same partion.
     Sort rdds in each partition by uid and timestamp"""
    df = df.select('uid', 'request_id','r_timestamp','latitude','longitude','r_s_info','sl_adjusted_confidence','request_filled')
    df = df.repartition('uid').sortWithinPartitions('uid','r_timestamp')
   
    """ Apply the model on each partion"""  
    rdds = df.rdd
    list_of_requests = rdds.mapPartitions(process)      
    
    """ Create schema for the output"""   
    field = [pst.StructField("request_id", pst.StringType(), True), pst.StructField("r_s_info1", pst.StringType(), True), pst.StructField("loc_score", pst.StringType(), True), pst.StructField("fill", pst.StringType(), True)]
    schema = pst.StructType(field)

    """ Output from model is a list of tuples, covnert tuples back to dataframe"""
    df_ab = sqlContext.createDataFrame(list_of_requests,schema = schema)

    """ Save dataframe with partitions"""
    base_dir_w = os.path.join('/user',os.environ['USER'], 'tmp', 'ard','abnormal_req')
    path_w = os.path.join(base_dir_w,country, log_type, year, month, day, hour)
    df_w = df_ab.write.format("orc").partitionBy('fill','loc_score').save(path_w)
    
    sc.stop()

def miles (pre, cur):
    """Caculate the distance between two latitudes and longitudes"""
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

def speed(pre, cur):
    
    distance = miles(pre, cur)
    if int(cur[2]) - int(pre[2]) != 0:            
        speed = distance / (int(cur[2]) - int(pre[2])) * 1000 * 60 * 60
    elif int(cur[2]) - int(pre[2]) == 0 and distance < 2:
        speed = 0
    else:
        speed = 100000000
    return speed

def flat_kernel_update(pre_cluster, cur_cluster, requests):
    """Update the latitude and longitude of the centroid of a cluster based on the weight ratio"""
    pre_len = len(requests[-1]) 
    total_len = pre_len + 1
    sum_lat = pre_cluster[0] * pre_len + cur_cluster[0]
    sum_lon = pre_cluster[1] * pre_len + cur_cluster[1]
    lat = sum_lat / total_len
    lon = sum_lon / total_len
    
    return (lat, lon)

def get_clusters(uid_requests):
    """Get the location clusters and corresponding requests with all the requests from one uid"""   
    clusters = []
    requests = []
    cluster_request = {}
    try:           
        for i in range(len(uid_requests)): 
            r = uid_requests[i]             
            
            if len(clusters) == 0:
                """When there is no cluster, we initialize the first cluster with the first request location"""
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
                    """Compare the location of the current request with all the existing clusters
                     Merge the this request into one of them and update the centroid of the cluster if the distance is small enough. 
                     Otherwise, create a new cluser""" 
                    if cur_cluster[0] == cluster[0] and cur_cluster[1] == cluster[1]:
                        requests[i].append(i)
                        clusters[i] = cur_cluster
                        flag = True
                        break

                    else:
                        if miles(cluster, cur_cluster) <= 2:                        
                            weighted_cluster = flat_kernel_update(cluster,cur_cluster,requests)
                            clusters[i] = [weighted_cluster[0],weighted_cluster[1], cur_cluster[2]]
                            requests[i].append(i)
                            flag = True
                            break
                        elif miles(cluster, cur_cluster) > 2 and speed(cluster,cur_cluster) <= 100:                        
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
            """Return a dictionary, the key is the centroid of a cluster, the values are all the requests in this cluster"""
            cluster_request[tuple(clusters[j])] = requests[j]
        
        return cluster_request

    except ValueError:            
        return 'error'

def update_r_s_info(cluster_requests, uid_requests):
    """ update the abnormal request tag in r_s_info field"""
    if len(cluster_requests) == 1:                          
        for r in uid_requests:            
            r['r_s_info'] = '{"abnormal_req":0}'

    elif len(cluster_requests) >=2:     
        if float(len(cluster_requests[0][1])) / len(cluster_requests[1][1]) > 2.0:                    
            for i in cluster_requests[0][1]:
                r = uid_requests[i]
                r['r_s_info'] = '{"abnormal_req":0}' 
                
            for i in range(1, len(cluster_requests)):
                for j in cluster_requests[i][1]:
                    r = uid_requests[j]
                    r['r_s_info'] = '{"abnormal_req":1}'

        else:
            for r in uid_requests:                        
                r['r_s_info'] = '{"abnormal_req":2}'            
                
    return uid_requests

def build_tuples_s(dic):
    """Convert the dictionary into a tuple.
    The order of the command is the same as the schema, the order can't be changed"""
    t_list = []
    t_list.append(dic['request_id']) 
    t_list.append(dic['r_s_info'])
    t_list.append(dic['sl_adjusted_confidence'])
    t_list.append(dic['request_filled'])
    
    return t_list 

def process(iterator):
    """ Major function for abnormal request detection model"""
    try:
        count = 0
        uid_requests = []
        pre_key = ''
        process = 0
        all_requests = []
        """ Each row in the iterator is one request, it is a row object
         Since they have been sorted by uid and r_timestamp already, we only need to compare the consecutive requests"""
        for row in iterator: 
            
            cur_key = row['uid']
            
            if pre_key == '':
                """Convert the row into dictionary to modify the value, since row is immutable"""               
                uid_requests.append(row.asDict())
                pre_key = cur_key
                
            elif pre_key == cur_key:                
                uid_requests.append(row.asDict())
            
            elif pre_key != cur_key :
                """When two keys are different, it means all the requests belong to this uid have been saved to the uid_requests list.
                   Next step is to process these requests with the model"""            
                cluster_request = get_clusters(uid_requests)
                """ Sort the clusters based on the number of requests in it"""
                cluster_requests = sorted(cluster_request.items(), key = lambda x:len(x[1]),reverse = True)  
                """ Update the r_s_info field"""              
                requests = update_r_s_info(cluster_requests, uid_requests)

                pre_key = cur_key
                uid_requests = []
                
                """Convert the dictionary into a tuple, since SPARK 2.0.0 doesn't support converting dictionary to dataframe directly"""
                for r in requests:
                    r_tuple = build_tuples_s(r)
                    all_requests.append(r_tuple)

        """Process the last uid"""
        if len(uid_requests) > 0:
            cluster_request = get_clusters(uid_requests)
            cluster_requests = sorted(cluster_request.items(), key = lambda x:len(x[1]),reverse = True)  
            requests = update_r_s_info(cluster_requests, uid_requests)
            requests = update_r_s_info(cluster_requests, uid_requests)                
            for r in requests:
                r_tuple = build_tuples_s(r)
                all_requests.append(r_tuple)  
        
        return all_requests
                
    except ValueError:
        return 'error'


if __name__ == "__main__":
    main()   



















































