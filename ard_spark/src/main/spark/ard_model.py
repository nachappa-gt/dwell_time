"""
This program runs in pyspark.  It does the following:
1. extract location data from the original science core.
2. set abnormal_req flag for those that are abnormal.
3. Save abnormal request data to ORC.

There will be another process after this to join the abnormal data
with the science core data.

Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.

@author: xiangling, victor

"""

import sys
sys.path.append('/home/xad/ard/python')
sys.path.append('/home/xad/share/python')
import argparse
import json
import logging
import os

from math import sin, cos, sqrt, atan2, radians
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from xad.common.conf import Conf

DEFAULT_CONFIG_DIRS = "/home/xad/ard/config:/home/xad/share/config"
DEFAULT_CONFIG_FILE = "ard.properties"
SL_DELIM = "|"


# Define values for abnormal_req
class AbnormalState():
    GOOD = 0
    REMOTE_BAD = 1
    REMOTE_CHAOTIC = 2
    NEAR_BAD = 3
    NEAR_CHAOTIC = 4
    CENTROID = 5
    NON_94_IP = 6


# Visit State (for Hayoung)
class VisitState():
    VISIT_BEGIN = 1
    VISIT_CONTINUED = 2
    INVALID = 3


class Location(object):
    R = 3960.00845   # 6373.0 / 1.60934
    MAX_SPEED = 1000000000  # MPH

    """Store location and time"""
    def __init__(self, lat=0, lon=0, loc=None):
        if (loc):
            self.lat = loc.lat
            self.lon = loc.lon
        else:
            self.lat = lat
            self.lon = lon

    def __eq__(self, other):
        """Compare location up to 5 devimal points"""
        return self.key() == other.key()

    def key(self):
        """Get a concated string of latitude and logitude to 5 decimal points"""
        return "{:.05f}|{:.05f}".format(self.lat, self.lon)

    def move_to(self, other, alpha=0.9):
       """Move the location toward another at the specified rate"""
       self.lat = (1-alpha) * self.lat + alpha * other.lat
       self.lon = (1-alpha) * self.lon + alpha * other.lon

       
    def get_distance (self, other):
        """Caculate the distance between two latitudes and longitudes in miles
    
        This is a static function.        
        """    
        # Convert decimal degrees to radians
        lat1, lon1 = map(radians, [self.lat, self.lon])
        lat2, lon2 = map(radians, [other.lat, other.lon])
    
        # haversine formula 
        dlon = lon2 - lon1
        dlat = lat2 - lat1    
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    
        if a > 0 and a < 1:
            c = 2 * atan2(sqrt(a),sqrt(1-a))
            distance = Location.R * c
        else:
            distance = 0
    
        return distance 

    def get_distance_speed(self, other, dt):
        """Get the speed in miles per hour.
    
        Params:
          distance: in miles
          dt: delta time, in miliseconds     
        """           
        distance = self.get_distance(other)

        msec = abs(dt)
        if (msec > 0):        
            speed = (distance/msec) * 3600000 
            if (speed > Location.MAX_SPEED):
                speed = Location.MAX_SPEED
        else:
            speed = Location.MAX_SPEED
    
        return [distance, speed]
                


class RequestRecord(object):
    """Store record data for ARD modeling"""
    def __init__(self, row):
        """Constructor
        Param:
          row: the original Row object passed to the process.
              This is a read-only object.
        """
        # The original row
        self.row = row
        # Convert some fields
        self.timestamp = int(row['r_timestamp'])
        self.loc = Location(float(self.row['latitude']), float(self.row['longitude']))
        # States
        self.abnormal_state = None
        self.visit_state = None   # TODO For Hayoung

    def is_abnormal(self):
        """Check if the record is in a n abnormal state other than GOOD"""
        return (self.abnormal_state is not None) and \
            (self.abnormal_state != AbnormalState.GOOD)
        
    def get(self, key):
        """Retrieve an attribute that comes with the original row"""
        return self.row[key]        

    def get_latitude(self):
        return loc.lat
    
    def get_longitude(self):
        return loc.lon

    def get_centroid_key(self):
        """Combine latitude and latitude into a key"""
        return "{:.05f}{}{:.05f}".format(self.loc.lat, SL_DELIM, self.loc.lon)

    def get_user_ip(self):
        return self.row('user_ip')

    def update_r_s_info(self):
        """ update the abnormal request tag in r_s_info field"""        

        # Convert to a JSON object
        r_s_info = self.row('r_s_info')
        if (not isinstance(r_s_info, basestring)):
            r_s_info = "{}"
        try:
            j = json.loads(r_s_info)
        except ValueError:
            j = json.loads("{}")

        # Add a properties
        if (self.abnormal_state):
            j["abnormal_req"] = self.abnormal_state
        
        # Handle visit_state
        if (self.visit_state):
            # FIXME - Hayoung
            pass
        
        # Convert JSON object to a JSON string
        r_s_info = json.dumps(j)     

        return r_s_info
    
    def to_output_list(self):
        """Generate the output tuple"""
        output_list = list()
        output_list.append(self.row['request_id']) 
        output_list.append(self.update_r_s_info())
        output_list.append(self.row['sl_adjusted_confidence'])
        output_list.append(self.row['request_filled'])
        return output_list 



class RequestCluster(object):

    def __init__(self, req=None, alpha=0.9, time_threshold=300):
        """Initialize a request cluster
        
        alaph is the learning rate for the cluster "center".
        time_threshold (in seconds) is another learning factor.
        """
        self.weight = 0.0
        self.requests = list()
        self.center = None
        self.timestamp = 0   # last update
        self.alpha = alpha  # learning rate
        self.time_threshold_msec = time_threshold * 1000
        
        if (req):
            self.add(req)
        
    def add(self, req):        
        """Add the request to the cluster"""
        
        # Move the center location
        loc = req.loc
        if len(self.requests) == 0:
            # Handle the first request in the cluster
            self.center = loc
            self.weight += 1
            
        else:
            prev = self.requests[-1]  
            dt = req.timestamp - prev.timestamp

            # Dedupe based on the previous location.  No weight change
            if loc == prev.loc:
                self.center = loc

            # Move the center towards the new location
            elif (dt >= self.time_threshold_msec):       
                self.center = loc
                self.weight += 1
        
            # Move toward to the new location according to the learning rate
            else:
                self.center.move_to(loc, self.alpha)  
                self.weight += 1

        # Add the request to the reqiest list
        self.timestamp = req.timestamp
        self.requests.append(req)

    def is_nearby(self, loc, ts,
                  distance_threshold=2, speed_threshold=100):

        """Check if location 2 at time2 is close to location 1 at time 1"""
        retval = False
        dt = ts - self.timestamp
        distance, speed = self.center.get_distance_speed(loc, dt)
        if (distance <= distance_threshold or speed <= speed_threshold):
            retval = True
        return retval

    def set_abnoamrl_state(self, state):
        """Set the abnormal state for all requests in the cluster"""
        for req in self.requests:
            req.abnormal_state = state
            

class UserRequestMgr(object):
    """Request manager -- tracking all clusters associated with a user
    
    Most of them are regular cluster clusters.
    Some are special clusters to store centroids.
    """

    def __init__(self):
        self.requests = list()
        self.centroids = list()
        self.non_94_ips = list()
        self.clusters = list()
        self.active_cluster_idx = 0

    def isEmpty(self):
        """Check if the request manager contains any valid data"""
        return True if len(self.requests) == 0 else False

    def getNumClusters(self):
        return len(self.clusters)        
        
    def add_request(self, req):
        """Add a new request record associated with a user"""
        self.requests.append(req)

    def check_sl_black_list(self, req):
        """Check if the request is in the SmartLoation black list.
        
        If found, the record will be added to the corresponding
        cluster.   They will not be used for other abnormal location
        detection.
        """
        retval = False
        # Check Centroid
        centroid_key = req.get_centroid_key()
        if (centroid_key in CENTROID_SET_BROADCAST.value):
            req.abnormal_state = AbnormalState.CENTROID
            self.centroids.append(req)
            retval = True

        # Check IP
        elif (req.get_user_ip() in IP_SET_BROADCAST.value):
            req.abnormal_state = AbnormalState.NON_94_IP
            self.non_94_ips.append(req)
            retval = True
        
        return retval

        
    def _assign_cluster(self, req):
        """Assign the request to a request cluster"""

        if len(self.clusters) == 0:
            cluster = RequestCluster(req)
            self.clusters.append(cluster)
            self.active_cluster_idx = 0
            
        else:
            loc = req.loc
            ts = req.timestamp
            found = False                
            
            # Check the previous active cluster first
            # Since it is most likely that it will fall into this cluster
            cluster = self.clusters[self.active_cluster_idx]
            if (cluster.is_nearby(loc, ts)):
                cluster.add(req)
                found = True
                
            else:
                for i in range(len(self.clusters)):
                    if (i == self.active_cluster_idx):
                        # Skip the previously active cluster
                        continue
                    
                    cluster = self.clusters[i]
                    if (cluster.is_nearby(loc, ts)):
                        cluster.add(req)
                        found = True
                        self.active_cluster_idx = i
                        break
                    
            # Create a new cluster; request doesn't match existing ones
            if not found:
                self.active_cluster_idx = len(self.clusters)
                self.clusters.append(RequestCluster(req))
       

    def _tag_abnormal_requests(self):
        """Find the there is a dominate cluster and update the abnormal state
        of all requests"""
        
        # There will be no anomolies if there is only one cluster or less
        numClusters = self.getNumClusters()
        if (numClusters >= 2):
            # Sort the cluster based on weights
            self.clusters.sort(key=lambda x: x.weight, reverse=True)
            w1 = self.clusters[0].weight
            w2 = self.clusters[1].weight
            # FIXME: don't hard code
            if (w1/w2) >= 2.0:
                # Has a dominate cluster
                for i in range(1,numClusters):
                    self.clusters[i].set_abnoamrl_state(AbnormalState.REMOTE_BAD)
            elif w1 >= 5.0 or numClusters > 2:
                # Cannot identify a dominate cluster
                for cluster in self.clusters:
                    cluster.set_abnoamrl_state(AbnormalState.REMOTE_CHAOTIC)


    def identify_abnormal_requests(self):
        """Identify abnormal requests and tag centroids and invalid IPs"""
        for req in self.requests:             
            # Centroid removal
            if self.check_sl_black_list(req):
                continue          

            self._assign_cluster(req)
            
        # Identify the major cluster and tag abnormal requests
        self._tag_abnormal_requests()


    def identify_valid_visitations(self):
        """ Identify valid POI visitations
        """
        # FIXME: Hayoung
        pass    


    def get_outputs(self, retvals):
        """Convert request record to outputs."""
        for req in self.requests:
            # Check if the request has new information
            if req.abnormal_state is not None or req.visit_state is not None:
                output_record = req.to_output_list()
                retvals.append(output_record)


    def process_requests(self, retvals=None):
        """Group user requests into clusters"""

        self.identify_abnormal_requests()
        self.identify_valid_visitations()
        
        if retvals is not None:
            self.get_outputs(retvals)

    

def process_partition(iterator):
    """Process the requests data in a partition.

    The data are associated with multiple users.
    All user requests will be grouped together.
    The method needs to detect uid changes.    
    """

    logging.info("+ Process Partition ...")
    mgr = UserRequestMgr()
    retvals = []

    # Each row in the iterator is one request, it is a row object
    # Since they have been sorted by uid and r_timestamp already,
    # we only need to compare the consecutive requests.
    pre_uid = None
    for row in iterator: 

        req = RequestRecord(row)
        curr_uid = req.get("uid")
        
        if (pre_uid is None) or (prev_uid == curr_uid):
            mgr.add_request(req)            
        else:  
            # Process requests on uid changes
            mgr.process_requests(retvals)
            
            # Reset for the new uid
            mgr = UserRequestMgr()
            mgr.add_request(req)

    """Process the last uid"""
    if not mgr.isEmpty():
        mgr.process_requests(retvals)

    return retvals



def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_dirs', help="Configuration search dirs",
                        default=DEFAULT_CONFIG_DIRS)
    parser.add_argument('--config', help="Configuration file",
                        default=DEFAULT_CONFIG_FILE)    
    parser.add_argument("--country", help="country")
    parser.add_argument("--logtype", help="logtype")
    parser.add_argument("--date", help="date")
    parser.add_argument("--hour", help="hour")    
    parser.add_argument("--input_dir", help="input dir")    
    parser.add_argument("--output_dir", help="outputdir")  
    parser.add_argument("--sl_centroid_path", help="smartlocation ll centroids")  
    parser.add_argument("--sl_ip_path", help="smartlocation IP changes")  

    # Flags
    parser.add_argument('-d', '--debug', action='store_true', help="Turn on debugging")
    parser.add_argument('-n',  '--norun', action='store_true', help="No run.")

    
    # Parse the arguments
    opt = parser.parse_args()
    return(opt)


def dump_opt(opt):
    """Print some of the major arguments"""
    logging.info("# Options")
    logging.info("--config = {}".format(opt.config))
    logging.info("--config_dirs = {}".format(opt.config_dirs))
    logging.info("--input_dir = {}".format(opt.input_dir))
    logging.info("--output_dir = {}".format(opt.output_dir))
    logging.info("--sl_centroid_path = {}".format(opt.sl_centroid_path))
    logging.info("--sl_ip_path = {}".format(opt.sl_ip_path))


def init_logging():
    """Initialize logging"""
    global logger

    # Config
    level = logging.INFO
    fmt = ("%(asctime)s %(levelname)s [%(module)s.%(funcName)s] %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt, datefmt=datefmt, level=level)

    # IPython specific setting
    logger = logging.getLogger()
    logger.setLevel(level)
    logging.info('Initialized logger')

def load_configuration(opt):
    """Load configuration files"""
    conf = Conf()
    conf.load(opt.config, opt.config_dirs)
    return(conf)


def get_app_name(opt):
    """Get the application name"""
    return os.path.join('ard_process', opt.country,
                        opt.logtype, opt.date, opt.hour)

def load_smartlocation_centroids(opt, spark):
    """Load smartlocation centroids and broadcast"""
    global CENTROID_SET_BROADCAST
    centroid_path = opt.sl_centroid_path

    if (centroid_path):
        centroid_schema = StructType([\
            StructField("latitude", StringType(), False), \
            StructField("longitude", StringType(), False)])
        centroid_df = spark.read.format('com.databricks.spark.csv') \
            .options(header='false', mode="DROPMALFORMED", delimiter='\t') \
            .load(centroid_path, schema=centroid_schema)
        centroid_rdd = centroid_df.rdd.map(lambda r: r[0] + SL_DELIM + r[1])
        centroid_set = set(centroid_rdd.collect())
    else:
        logging.info("sl_centroid_path not defined")
        centroid_set = set()
    
    sc = spark.sparkContext
    CENTROID_SET_BROADCAST = sc.broadcast(centroid_set)
    logging.info("CENTROID_SET size = {}".format(len(CENTROID_SET_BROADCAST.value)))


def load_smartlocation_IPs(opt, spark):
    """Load smartlocation IP addresses and broadcast"""
    global IP_SET_BROADCAST
    ip_path = opt.sl_ip_path

    if (ip_path):
        ip_schema = StructType([\
            StructField("ip", StringType(), False), \
            StructField("count", IntegerType(), False)])
        ip_df = spark.read.format('com.databricks.spark.csv') \
            .options(header='false', mode="DROPMALFORMED", delimiter='\t') \
            .load(ip_path, schema=ip_schema)
        ip_rdd = ip_df.rdd.map(lambda r: r[0])
        ip_set = set(ip_rdd.collect())
    else:
        logging.info("sl_centroid_path not defined")
        ip_set = set()
        
    sc = spark.sparkContext
    IP_SET_BROADCAST = sc.broadcast(ip_set)
    logging.info("IP_SET size = {}".format(len(IP_SET_BROADCAST.value)))
    

def main():
 
    # Parse command line argumen
    global opt
    global spark    
    
    init_logging()
    opt = parse_arguments()
    dump_opt(opt)
    
    # Logging and Config
#    cfg = load_configuration(opt)

    # Create the Spark Session
    appName = get_app_name(opt)
    spark = SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
        
    # Load Smartlocation centroids
    logging.info("# Loading Smart Location centroids and IPs...")
    load_smartlocation_centroids(opt, spark)
    load_smartlocation_IPs(opt, spark)

    # Load avro data
    avro_path_tp = os.path.join(opt.input_dir, '{fill,nf}/{tll,pos}')  
    logging.info("Loading AVRO data {}...".format(avro_path_tp))
    df_tp = spark.read.format("com.databricks.spark.avro").load(avro_path_tp)
    df = df_tp.where((df_tp.uid !='') & (df_tp.sl_adjusted_confidence >=94))

    # Select location related fields.
    # Repartition data by uid, 
    # All the request belong to the same uid will go to the same partion.
    # Sort rdds in each partition by uid and timestamp
    logging.info("Repartioning..")
    df = df.select('uid', 'request_id','r_timestamp',
                   'latitude','longitude',
                   'user_ip', 
                   'fp_matches',
                   'r_s_info','sl_adjusted_confidence','request_filled')
    df = df.repartition('uid').sortWithinPartitions('uid','r_timestamp')

    # Apply the model on each partion
    logging.info("Map Partitions..")
    output_records = df.rdd.mapPartitions(process_partition)

    # Output from model is a list of tuples, covnert tuples back to dataframe
    logging.info("Create output dataframe..")
    field = [StructField("request_id", StringType(), True),
             StructField("r_s_info1", StringType(), True),
             StructField("loc_score", StringType(), True),
             StructField("fill", StringType(), True)]
    schema = StructType(field)

    # Save dataframe with partitions
    logging.info("Save to {}...".format(opt.output_dir))
    df_ab = spark.createDataFrame(output_records, schema=schema)
    df_ab.write.mode("overwrite").format("orc") \
        .option("compression","zlib") \
        .mode("overwrite") \
        .partitionBy('fill','loc_score') \
        .save(opt.output_dir)

    # Force the spark process to stop.
    sc.stop()
    logging.info("Done!")


if __name__ == "__main__":
    main()   

