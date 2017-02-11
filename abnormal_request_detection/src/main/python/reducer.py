from itertools import groupby
from operator import itemgetter
from math import sin, cos, sqrt, atan2, radians
import sys


## Hive transform pipeline does not accept numpy, even you just import the package and don't use it

def read_mapper_output(file,separator = '\t'):
    for line in file:
        if len(line) > 1:
            yield line.split(separator)

def get_clusters(uid_requests):
    
    clusters = []
    requests = []
    cluster_request = {}
    try:           
        for i in range(len(uid_requests)): 
            r = uid_requests[i]             
            if int(r[5]) < 94:
                continue
            
            if len(clusters) == 0:

                pre_cluster = [float(r[3]), float(r[4]), int(r[1])] 
                clusters.append(pre_cluster)
                pre_requests = []
                pre_requests.append(i)
                requests.append(pre_requests)
                ## save the clusters and corresponding requests in separate list to modify it 
                ## more easily
        
            else:

                cur_cluster = [float(r[3]), float(r[4]), int(r[1])]
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

    

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator = separator)  

    for uid, group in groupby(data,itemgetter(0)):
        try:            
            
            uid_requests = sorted(group, key = itemgetter(1))
            
            cluster_request = get_clusters(uid_requests)
            
            cluster_requests = sorted(cluster_request.items(), key = lambda x:len(x[1]),reverse = True)
            clusters = []

            for cr in cluster_requests:
                clusters.append([cr[0][0],cr[0][1],len(cr[1])])

            """for cluster in cluster_requests:

                print "%s%s%s%s%s%s" % (uid,separator, cluster[0], separator, len(cluster[1]))"""
            
                ## next step is to pick up the major location from the clusters
            if len(cluster_requests) == 0:
                
                for r in uid_requests:
                    print "%s%s%s%s%s" % (r[2], separator,'1',separator,r[6]) 

            elif len(cluster_requests) == 1: 
                         
                for r in uid_requests:
                    if int(r[5]) >= 94:
                            print "%s%s%s%s%s" % (r[2], separator,'0',separator,r[6]) 
                    else:
                            print "%s%s%s%s%s" % (r[2], separator,'1',separator,r[6])

            else:
                               
                if float(len(cluster_requests[0][1])) / len(cluster_requests[1][1]) > 2.0:
                    
                    for i in cluster_requests[0][1]:
                        r = uid_requests[i]
                        print "%s%s%s%s%s" % (r[2], separator,'0',separator,r[6])

                    for i in range(1, len(cluster_requests)):
                        for j in cluster_requests[i][1]:
                            r = uid_requests[j]
                            print "%s%s%s%s%s" % (r[2], separator,'2',separator,r[6])

                    for r in uid_requests:
                        if int(r[5]) < 94:
                            print "%s%s%s%s%s" % (r[2], separator,'1',separator,r[6])

                else:

                    for r in uid_requests:
                        print "%s%s%s%s%s" % (r[2], separator,'3',separator,r[6])

        except ValueError:
            
            print "%s%s%s" % (uid,separator,'error')

def speed(pre, cur):
    
    distance = miles(pre, cur)
    if int(cur[2]) - int(pre[2]) != 0:            
        speed = distance / (int(cur[2]) - int(pre[2])) * 1000 * 60 * 60
    elif int(cur[2]) - int(pre[2]) == 0 and distance < 2:
        speed = 0
    else:
        speed = 100000000

    return speed
               

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

if __name__ == "__main__":
    main()


