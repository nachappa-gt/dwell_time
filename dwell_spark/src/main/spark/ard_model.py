# """
# This program runs in pyspark.  It does the following:
# 1. extract location data from the original science core.
# 2. set abnormal_req flag for those that are abnormal.
# 3. Save abnormal request data to ORC.
#
# There will be another process after this to join the abnormal data
# with the science core data.
#
# Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.
#
# @author: xiangling, victor
#
# """
#
# import sys
# sys.path.append('/home/xad/dwell_time/python')
# sys.path.append('/home/xad/share/python')
# import argparse
# import json
# import logging
# import os
#
# import operator
# import math
# import numpy as np
#
# from math import sin, cos, sqrt, atan2, radians
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#
# from xad.common.conf import Conf
#
# DEFAULT_CONFIG_DIRS = "/home/xad/dwell_time/config:/home/xad/share/config"
# DEFAULT_CONFIG_FILE = "dwell_time.properties"
# SL_DELIM = "|"
#
#
# # Define values for abnormal_req
# class AbnormalState():
#     GOOD = 0
#     REMOTE_BAD = 1
#     REMOTE_CHAOTIC = 2
#     NEAR_BAD = 3
#     NEAR_CHAOTIC = 4
#     CENTROID = 5
#     NON_94_IP = 6
#
#
# # Visit State
# class VisitState():
#     VISIT_BEGIN = 1
#     VISIT_CONTINUED = 2
#     VISIT_END = 3
#     VISIT_BEGIN_AND_END = 4
#     INVALID = 5
#
#
# class Location(object):
#     R = 3960.00845   # 6373.0 / 1.60934
#     MAX_SPEED = 1000000000  # MPH
#
#     """Store location and time"""
#     def __init__(self, lat=0, lon=0, loc=None):
#         if (loc):
#             self.lat = loc.lat
#             self.lon = loc.lon
#         else:
#             self.lat = lat
#             self.lon = lon
#
#     def __eq__(self, other):
#         """Compare location up to 5 devimal points"""
#         return self.key() == other.key()
#
#     def key(self):
#         """Get a concated string of latitude and logitude to 5 decimal points"""
#         return "{:.05f}|{:.05f}".format(self.lat, self.lon)
#
#     def move_to(self, other, alpha=0.9):
#        """Move the location toward another at the specified rate"""
#        self.lat = (1-alpha) * self.lat + alpha * other.lat
#        self.lon = (1-alpha) * self.lon + alpha * other.lon
#
#
#     def get_distance (self, other):
#         """Caculate the distance between two latitudes and longitudes in miles
#
#         This is a static function.
#         """
#         # Convert decimal degrees to radians
#         lat1, lon1 = map(radians, [self.lat, self.lon])
#         lat2, lon2 = map(radians, [other.lat, other.lon])
#
#         # haversine formula
#         dlon = lon2 - lon1
#         dlat = lat2 - lat1
#         a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
#
#         if a > 0 and a < 1:
#             c = 2 * atan2(sqrt(a),sqrt(1-a))
#             distance = Location.R * c
#         else:
#             distance = 0
#
#         return distance
#
#     def get_distance_speed(self, other, dt):
#         """Get the speed in miles per hour.
#
#         Params:
#           distance: in miles
#           dt: delta time, in miliseconds
#         """
#         distance = self.get_distance(other)
#
#         msec = abs(dt)
#         if (msec > 0):
#             speed = (distance/msec) * 3600000
#             if (speed > Location.MAX_SPEED):
#                 speed = Location.MAX_SPEED
#         else:
#             speed = Location.MAX_SPEED
#
#         return [distance, speed]
#
#
#
# class RequestRecord(object):
#     """Store record data for ARD modeling"""
#     def __init__(self, row):
#         """Constructor
#         Param:
#           row: the original Row object passed to the process.
#               This is a read-only object.
#         """
#         # The original row
#         self.row = row
#         # Convert some fields
#         self.timestamp = int(row['r_timestamp'])
#         self.loc = Location(float(self.row['latitude']), float(self.row['longitude']))
#         # States
#         self.abnormal_state = None
#         self.visit_state = None
#
#     def is_abnormal(self):
#         """Check if the record is in a n abnormal state other than GOOD"""
#         return (self.abnormal_state is not None) and \
#             (self.abnormal_state != AbnormalState.GOOD)
#
#     def get(self, key):
#         """Retrieve an attribute that comes with the original row"""
#         return self.row[key]
#
#     def get_latitude(self):
#         return loc.lat
#
#     def get_longitude(self):
#         return loc.lon
#
#     def get_centroid_key(self):
#         """Combine latitude and latitude into a key"""
#         return "{:.05f}{}{:.05f}".format(self.loc.lat, SL_DELIM, self.loc.lon)
#
#     def get_user_ip(self):
#         return self.row('user_ip')
#
#     def update_r_s_info(self):
#         """ update the abnormal request tag in r_s_info field"""
#
#         # Convert to a JSON object
#         r_s_info = self.row('r_s_info')
#         if (not isinstance(r_s_info, basestring)):
#             r_s_info = "{}"
#         try:
#             j = json.loads(r_s_info)
#         except ValueError:
#             j = json.loads("{}")
#
#         # Add a properties
#         if (self.abnormal_state):
#             j["abnormal_req"] = self.abnormal_state
#
#         # Handle visit_state
#         if (self.visit_state):
#             j["vis_info"] = self.visit_state
#         # Convert JSON object to a JSON string
#         r_s_info = json.dumps(j)
#
#         return r_s_info
#
#     def to_output_list(self):
#         """Generate the output tuple"""
#         output_list = list()
#         output_list.append(self.row['request_id'])
#         output_list.append(self.update_r_s_info())
#         output_list.append(self.row['sl_adjusted_confidence'])
#         output_list.append(self.row['request_filled'])
#         return output_list
#
#
#
# class RequestCluster(object):
#
#     def __init__(self, req=None, alpha=0.9, time_threshold=300):
#         """Initialize a request cluster
#
#         alaph is the learning rate for the cluster "center".
#         time_threshold (in seconds) is another learning factor.
#         """
#         self.weight = 0.0
#         self.requests = list()
#         self.center = None
#         self.timestamp = 0   # last update
#         self.alpha = alpha  # learning rate
#         self.time_threshold_msec = time_threshold * 1000
#
#         if (req):
#             self.add(req)
#
#     def add(self, req):
#         """Add the request to the cluster"""
#
#         # Move the center location
#         loc = req.loc
#         if len(self.requests) == 0:
#             # Handle the first request in the cluster
#             self.center = loc
#             self.weight += 1
#
#         else:
#             prev = self.requests[-1]
#             dt = req.timestamp - prev.timestamp
#
#             # Dedupe based on the previous location.  No weight change
#             if loc == prev.loc:
#                 self.center = loc
#
#             # Move the center towards the new location
#             elif (dt >= self.time_threshold_msec):
#                 self.center = loc
#                 self.weight += 1
#
#             # Move toward to the new location according to the learning rate
#             else:
#                 self.center.move_to(loc, self.alpha)
#                 self.weight += 1
#
#         # Add the request to the reqiest list
#         self.timestamp = req.timestamp
#         self.requests.append(req)
#
#     def is_nearby(self, loc, ts,
#                   distance_threshold=2, speed_threshold=100):
#
#         """Check if location 2 at time2 is close to location 1 at time 1"""
#         retval = False
#         dt = ts - self.timestamp
#         distance, speed = self.center.get_distance_speed(loc, dt)
#         if (distance <= distance_threshold or speed <= speed_threshold):
#             retval = True
#         return retval
#
#     def set_abnoamrl_state(self, state):
#         """Set the abnormal state for all requests in the cluster"""
#         for req in self.requests:
#             req.abnormal_state = state
#
#
# class UserRequestMgr(object):
#     """Request manager -- tracking all clusters associated with a user
#
#     Most of them are regular cluster clusters.
#     Some are special clusters to store centroids.
#     """
#
#     def __init__(self):
#         self.requests = list()
#         self.centroids = list()
#         self.non_94_ips = list()
#         self.clusters = list()
#         self.active_cluster_idx = 0
#
#     def isEmpty(self):
#         """Check if the request manager contains any valid data"""
#         return True if len(self.requests) == 0 else False
#
#     def getNumClusters(self):
#         return len(self.clusters)
#
#     def add_request(self, req):
#         """Add a new request record associated with a user"""
#         self.requests.append(req)
#
#     def check_sl_black_list(self, req):
#         """Check if the request is in the SmartLoation black list.
#
#         If found, the record will be added to the corresponding
#         cluster.   They will not be used for other abnormal location
#         detection.
#         """
#         retval = False
#         # Check Centroid
#         centroid_key = req.get_centroid_key()
#         if (centroid_key in CENTROID_SET_BROADCAST.value):
#             req.abnormal_state = AbnormalState.CENTROID
#             self.centroids.append(req)
#             retval = True
#
#         # Check IP
#         elif (req.get_user_ip() in IP_SET_BROADCAST.value):
#             req.abnormal_state = AbnormalState.NON_94_IP
#             self.non_94_ips.append(req)
#             retval = True
#
#         return retval
#
#
#     def _assign_cluster(self, req):
#         """Assign the request to a request cluster"""
#
#         if len(self.clusters) == 0:
#             cluster = RequestCluster(req)
#             self.clusters.append(cluster)
#             self.active_cluster_idx = 0
#
#         else:
#             loc = req.loc
#             ts = req.timestamp
#             found = False
#
#             # Check the previous active cluster first
#             # Since it is most likely that it will fall into this cluster
#             cluster = self.clusters[self.active_cluster_idx]
#             if (cluster.is_nearby(loc, ts)):
#                 cluster.add(req)
#                 found = True
#
#             else:
#                 for i in range(len(self.clusters)):
#                     if (i == self.active_cluster_idx):
#                         # Skip the previously active cluster
#                         continue
#
#                     cluster = self.clusters[i]
#                     if (cluster.is_nearby(loc, ts)):
#                         cluster.add(req)
#                         found = True
#                         self.active_cluster_idx = i
#                         break
#
#             # Create a new cluster; request doesn't match existing ones
#             if not found:
#                 self.active_cluster_idx = len(self.clusters)
#                 self.clusters.append(RequestCluster(req))
#
#
#     def _tag_abnormal_requests(self):
#         """Find the there is a dominate cluster and update the abnormal state
#         of all requests"""
#
#         # There will be no anomolies if there is only one cluster or less
#         numClusters = self.getNumClusters()
#         if (numClusters >= 2):
#             # Sort the cluster based on weights
#             self.clusters.sort(key=lambda x: x.weight, reverse=True)
#             w1 = self.clusters[0].weight
#             w2 = self.clusters[1].weight
#             # FIXME: don't hard code
#             if (w1/w2) >= 2.0:
#                 # Has a dominate cluster
#                 for i in range(1,numClusters):
#                     self.clusters[i].set_abnoamrl_state(AbnormalState.REMOTE_BAD)
#             else:
#                 # Cannot identify a dominate cluster
#                 for cluster in self.clusters:
#                     cluster.set_abnoamrl_state(AbnormalState.REMOTE_CHAOTIC)
#
#
#     def identify_abnormal_requests(self):
#         """Identify abnormal requests and tag centroids and invalid IPs"""
#         for req in self.requests:
#             # Centroid removal
#             if self.check_sl_black_list(req):
#                 continue
#
#             self._assign_cluster(req)
#
#         # Identify the major cluster and tag abnormal requests
#         self._tag_abnormal_requests()
# #
#     def identify_valid_visitations(self):
#         """ Identify valid POI visitations
#         """
#         filtered_requests = list()
#         for req in self.requests :
#             if (req.abnormal_state >= 1) :
#                 continue
#             filtered_requests.append(req) # exclued abnormal requests
#
#         # build list of pois with proximity 1
#         poi_visits = buildPoiVisits(filtered_requests)
#         if (len(poi_visits) == 0) :
#             return
#         # check validity of poi visit for each poi
#         valid_individual_visits = validPoiVisits(filtered_requests, poi_visits)
#         # check validity of multiple poi visits at the same time - overlapping visits? pick one
#         poi_visits = pickFinalValidVisits(valid_individual_visits)
#         # tag valid visits
#         self._tag_valid_visits(poi_visits)
#
#     def _tag_valid_visits(self, poi_visits):
#         """ tag each request if their visitation is classified as true(begin/continued/end) or false(invalid). If not enough info - return null"""
#         vis_info = [None] * len(self.requests)
#
#         for poi_visit in poi_visits :
#             if (poi_visit.isValid == True) :
#                 if (poi_visit.start_idx == poi_visit.end_idx) :
#                     info = [(poi_visit.poi, VisitState.VISIT_BEGIN_AND_END)]
#                     if (vis_info[poi_visit.start_idx] == None) :
#                             vis_info[poi_visit.start_idx] = info
#                     else :
#                         vis_info[poi_visit.start_idx].append(info)
#
#                 else :
#                     for i in range(poi_visit.start_idx, poi_visit.end_idx+1) :
#
#                         if (i == poi_visit.start_idx) :
#                             info = [(poi_visit.poi, VisitState.VISIT_BEGIN)]
#                         elif (i == poi_visit.end_idx) :
#                             info = [(poi_visit.poi, VisitState.VISIT_END)]
#                         else :
#                             info = [(poi_visit.poi, VisitState.VISIT_CONTINUED)]
#
#
#                         if (vis_info[i] == None) :
#                             vis_info[i] = info
#                         else :
#                             vis_info[i].append(info)
#
#             elif (poi_visit.isValid == False) :
#                 info = [(poi_visit.poi, VisitState.INVALID)]
#                 for i in range(poi_visit.start_idx, poi_visit.end_idx+1) :
#
#                     if (vis_info[i] == None) :
#                         vis_info[i] = info
#                     else :
#                         vis_info[i].append(info)
#
#         for i in range(len(self.requests)) :
#             req = self.requests[i]
#             if (vis_info[i] is not None) :
#                 #if (req.abnormal_state is not None) :
#                 req.visit_state = vis_info[i]
#
#
#
#
#     def get_outputs(self, retvals):
#         """Convert request record to outputs."""
#         for req in self.requests:
#             # Check if the request has new information
#             if req.abnormal_state is not None or req.visit_state is not None:
#                 output_record = req.to_output_list()
#                 retvals.append(output_record)
#
#
#     def process_requests(self, retvals=None):
#         """Group user requests into clusters"""
#
#         self.identify_abnormal_requests()
#         # identify valid/invalid visitations
#         #self.identify_valid_visitations()
#
#         if retvals is not None:
#             self.get_outputs(retvals)
#
#
#
# def process_partition(iterator):
#     """Process the requests data in a partition.
#
#     The data are associated with multiple users.
#     All user requests will be grouped together.
#     The method needs to detect uid changes.
#     """
#
#     logging.info("+ Process Partition ...")
#     mgr = UserRequestMgr()
#     retvals = []
#
#     # Each row in the iterator is one request, it is a row object
#     # Since they have been sorted by uid and r_timestamp already,
#     # we only need to compare the consecutive requests.
#     prev_uid = None
#     for row in iterator:
#
#         req = RequestRecord(row)
#         curr_uid = req.get("uid")
#
#         if (prev_uid is None) or (prev_uid == curr_uid):
#             mgr.add_request(req)
#         else:
#             # Process requests on uid changes
#             mgr.process_requests(retvals)
#
#             # Reset for the new uid
#             mgr = UserRequestMgr()
#             mgr.add_request(req)
#
#         prev_uid = curr_uid
#
#     """Process the last uid"""
#     if not mgr.isEmpty():
#         mgr.process_requests(retvals)
#
#     return retvals
#
#
# class PoiVisit :
#
#     def __init__(self, poi, start_time=None, end_time=None, start_idx=None,
#                  end_idx=None, latlon_list=list(), isValid=None, last_lat=None, last_lon=None) :
#         self.poi = poi
#         self.start_time = start_time
#         self.end_time = end_time
#         self.start_idx = start_idx
#         self.end_idx = end_idx
#         self.latlon_list = latlon_list
#         self.isValid = isValid
#         self.last_lat = last_lat
#         self.last_lon = last_lon
#
#     def getVisitDuration(self) :
#         duration = (self.end_time - self.start_time) / 1000.0
#         return duration
#
#     def getNumUniqLocs(self) :
#         return len(set(self.latlon_list))
#
#     def __str__(self) :
#         return "Poi:{}, start_idx:{}, end_idx:{}, isValid:{}".format(self.poi, self.start_idx, self.end_idx, self.isValid)
#
# def buildPoiVisits(requests) :
#     """ build poi visit information from requests """
#     # find all the POIs visited with proximity mode 1 from the requests(filtered requests)
#     poi_prox1_list = list()
#     for req in requests :
#         fpMatches = req.get('fp_matches')
#
#         if (fpMatches is not None) :
#             for fp in fpMatches :
#                 prox = fp['proximity_mode']
#
#                 if (prox == 1) :
#                     poi = fp['hash_key']
#
#                     if (poi not in poi_prox1_list) :
#                         poi_prox1_list.append(poi)
#
#     # build PoiVisit-visit information for each poi
#     poi_visits = list()
#     for poi in poi_prox1_list :
#         isVisitContinued = False
#
#         for i in range(len(requests)) :
#             req = requests[i]
#             fpMatches = req.get('fp_matches')
#
#             isPOIProx1 = False
#             if (fpMatches is not None) :
#                 for fp in fpMatches :
#                     if ((fp['proximity_mode'] == 1) and (fp['hash_key'] == poi)) :
#
#                         if (isVisitContinued == False) :
#                             poi_visit = PoiVisit(poi)
#                             isVisitContinued = True
#
#                         isPOIProx1 = True
#                         ts = req.get('r_timestamp')
#
#                         if (poi_visit.start_time == None) :
#                             poi_visit.start_time = ts
#                             poi_visit.start_idx = i
#                         poi_visit.end_time = ts
#                         poi_visit.end_idx = i
#
#                         poi_visit.latlon_list.append((req.get('latitude'), req.get('longitude')))
#                         poi_visit.last_lat = req.get('latitude')
#                         poi_visit.last_lon = req.get('longitude')
#             # fp_matches is empty or poi is not appeared as proximity 1 : check lat/lon - is it close enough? more like caused by noise? => include as visit
#             if ((fpMatches == None) or (~isPOIProx1)) :
#                 if (isVisitContinued == False) :
#                     continue
#
#                 cur_lat = req.get('latitude')
#                 cur_lon = req.get('longitude')
#
#                 dist = distance_meter(poi_visit.last_lat, poi_visit.last_lon, cur_lat, cur_lon)
#
#                 if (dist < 5.0) :
#                     ts = req.get('r_timestamp')
#                     poi_visit.end_time = ts
#                     poi_visit.end_idx = i
#                     poi_visit.latlon_list.append((cur_lat, cur_lon))
#                 else :
#                     poi_visits.append(poi_visit)
#                     isVisitContinued = False
#
#         # after looking at all requests
#         if ((poi_visit.start_time != None) and (isVisitContinued ==True)):
#             poi_visits.append(poi_visit)
#
#     return poi_visits
#
# def validPoiVisits(filtered_requests, poi_visits) :
#     """ check validity of each poi visit(independently)"""
#     for poi_visit in poi_visits :
#
#         prev_idx = max(0, poi_visit.start_idx-1)
#         prev_exists = False
#         prev_valid = None
#         estTimeToAdd = 0
#
#         # check the speed of movement into the poi
#         if (prev_idx < poi_visit.start_idx) :
#             prev_ts = filtered_requests[prev_idx].get('r_timestamp')
#             prev_lat = filtered_requests[prev_idx].get('latitude')
#             prev_lon = filtered_requests[prev_idx].get('longitude')
#             timeElapsed = (poi_visit.start_time - prev_ts) / 1000.0 # in seconds
#
#             prev_dist = distance_meter(prev_lat, prev_lon,
#                                        filtered_requests[poi_visit.start_idx].get('latitude'),filtered_requests[poi_visit.start_idx].get('longitude'))
#             if (timeElapsed < 10.0) :
#                 prev_exists = True
#
#                 if (timeElapsed < 1.0) :
#                     if (prev_dist > 10.0) :
#                         prev_valid = False
#                 else :
#                     speed = prev_dist / timeElapsed
#                     if (speed > 10.0) :
#                         prev_valid = False
#
#             elif (timeElapsed < 60.0) :
#                 prev_exists = True
#                 speed = prev_dist / timeElapsed
#                 if (speed > 30.0) :
#                     prev_valid = False
#             # estimated time of visiting POI between previous lat/long and the first lat/long in the POI (assume moving speed as 2m/s)
#             estTimeToAdd = estTimeToAdd + max(0, timeElapsed - prev_dist/2.0)
#
#         # check the speed of movement out from poi
#         post_idx = min(len(filtered_requests)-1, poi_visit.end_idx+1)
#         post_exists = False
#         post_valid = None
#         if (post_idx > poi_visit.end_idx) :
#             post_ts = filtered_requests[post_idx].get('r_timestamp')
#             post_lat = filtered_requests[post_idx].get('latitude')
#             post_lon = filtered_requests[post_idx].get('longitude')
#             timeElapsed = (post_ts - poi_visit.end_time) / 1000.0 # in seconds
#
#             post_dist = distance_meter(post_lat, post_lon,
#                                        filtered_requests[poi_visit.end_idx].get('latitude'), filtered_requests[poi_visit.end_idx].get('longitude'))
#             if (timeElapsed < 10.0) :
#                 post_exists = True
#
#                 if (timeElapsed < 1.0) :
#                     if (post_dist > 10.0) :
#                         post_valid = False
#                 else :
#                     speed = post_dist / timeElapsed
#                     if (speed > 10.0) :
#                         post_valid = False
#
#             elif (timeElapsed < 60.0) :
#                 post_exists = True
#                 speed = post_dist / timeElapsed
#                 if (speed > 30.0) :
#                     post_valid = False
#
#             estTimeToAdd = estTimeToAdd + max(0, timeElapsed - post_dist/2.0)
#         # check the duration of visit
#         duration = poi_visit.getVisitDuration()
#         est_duration = duration + estTimeToAdd
#
#         if ((prev_valid == False) or (post_valid == False)) :
#             poi_visit.isValid = False
#         elif ((prev_exists == True) and (post_exists == True)) :
#
#             if (est_duration < 60.0) :
#
#                 poi_visit.isValid = False
#             else :
#                 poi_visit.isValid = True
#         return poi_visits
#
#
# def pickFinalValidVisits(poi_visits) :
#     """ compare poi visits if they are overlapping - pick one which looks more reasonable """
#     poi_visits = sorted(poi_visits, key=operator.attrgetter('start_idx'))
#
#     i = 0
#     bi = 0
#     overlap_buckets = list(list()) # lisf of list - each element will be a list of poi-visit that share some overlapping time range
#     t1 = None
#     t2 = None
#
#     while (True) :
#
#         if (i == len(poi_visits)) :
#             break
#
#         poi_visit = poi_visits[i]
#         if (poi_visit.isValid) :
#             if (len(overlap_buckets) == 0) :
#                 overlap_buckets.append([poi_visit])
#                 t1 = poi_visit.start_idx
#                 t2 = poi_visit.end_idx
#
#             else :
#                 if (existsOverlap(t1, t2, poi_visit.start_idx, poi_visit.end_idx)) :
#                     overlap_buckets[bi].append(poi_visit)
#                     t2 = max(t2, poi_visit.end_idx)
#                 else :
#                     t1 = poi_visit.start_idx
#                     t2 = poi_visit.end_idx
#                     bi+=1
#                     overlap_buckets.append([poi_visit])
#         i+=1
#
#     # find one poi visit among overlapping poi visits
#     if (len(overlap_buckets) > 0) :
#         for overlaps in overlap_buckets :
#             picked_idx = None
#             durations = [(item.end_time - item.start_time) / 1000.0 for item in overlaps]
#             max_durations_idx = np.argwhere(durations == np.amax(durations))
#
#             if (len(max_durations_idx) == 1) :
#                 picked_idx = int(max_durations_idx)
#             else :
#                 numUniq_pts = [poi_visit.getNumUniqLocs() for poi_visit in overlaps]
#                 max_numUniq_idx = np.argwhere(numUniq_pts == np.amax(numUniq_pts))
#
#                 if (len(max_numUniq_idx) == 1) :
#                     picked_idx = int(max_durations_idx[int(max_numUniq_idx)])
#
#             if (picked_idx != None) :
#                 # set all the other overlapped visit as invalid
#                 for i in range(len(overlaps)) :
#                     if (i != picked_idx) :
#                         overlaps[i].isValid = False
#             else :
#                 for i in range(len(overlaps)) :
#                     overlaps[i].isValid = None # couldn't pick one based on the given information. currently set to None.
#
#     return poi_visits
#
# def existsOverlap(s1, e1, s2, e2) :
#     if (s1 is None) or (e1 is None) :
#         return False
#     return max(s1, s2) <= min(e1,e2)
#
# def distance_meter(lat1, lon1, lat2, lon2) :
#     #return distance as meter if you want km distance, remove "* 1000"
#     radius = 6371 * 1000
#
#     dLat = (lat2-lat1) * math.pi / 180
#     dLng = (lon2-lon1) * math.pi / 180
#
#     lat1 = lat1 * math.pi / 180
#     lat2 = lat2 * math.pi / 180
#
#     val = sin(dLat/2) * sin(dLat/2) + sin(dLng/2) * sin(dLng/2) * cos(lat1) * cos(lat2)
#     ang = 2 * atan2(sqrt(val), sqrt(1-val))
#     return radius * ang
#
#
# def parse_arguments():
#     """Parse command line arguments"""
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--config_dirs', help="Configuration search dirs",
#                         default=DEFAULT_CONFIG_DIRS)
#     parser.add_argument('--config', help="Configuration file",
#                         default=DEFAULT_CONFIG_FILE)
#     parser.add_argument("--country", help="country")
#     parser.add_argument("--logtype", help="logtype")
#     parser.add_argument("--date", help="date")
#     parser.add_argument("--hour", help="hour")
#     parser.add_argument("--input_dir", help="input dir")
#     parser.add_argument("--output_dir", help="outputdir")
#     parser.add_argument("--sl_centroid_path", help="smartlocation ll centroids", default="")
#     parser.add_argument("--sl_ip_path", help="smartlocation IP changes", default="")
#
#     # Flags
#     parser.add_argument('-d', '--debug', action='store_true', help="Turn on debugging")
#     parser.add_argument('-n',  '--norun', action='store_true', help="No run.")
#
#
#     # Parse the arguments
#     opt = parser.parse_args()
#     return(opt)
#
#
# def dump_opt(opt):
#     """Print some of the major arguments"""
#     logging.info("# Options")
#     logging.info("--config = {}".format(opt.config))
#     logging.info("--config_dirs = {}".format(opt.config_dirs))
#     logging.info("--input_dir = {}".format(opt.input_dir))
#     logging.info("--output_dir = {}".format(opt.output_dir))
#     logging.info("--sl_centroid_path = {}".format(opt.sl_centroid_path))
#     logging.info("--sl_ip_path = {}".format(opt.sl_ip_path))
#
#
# def init_logging():
#     """Initialize logging"""
#     global logger
#
#     # Config
#     level = logging.INFO
#     fmt = ("%(asctime)s %(levelname)s [%(module)s.%(funcName)s] %(message)s")
#     datefmt = '%Y-%m-%d %H:%M:%S'
#     logging.basicConfig(format=fmt, datefmt=datefmt, level=level)
#
#     # IPython specific setting
#     logger = logging.getLogger()
#     logger.setLevel(level)
#     logging.info('Initialized logger')
#
# def load_configuration(opt):
#     """Load configuration files"""
#     conf = Conf()
#     conf.load(opt.config, opt.config_dirs)
#     return(conf)
#
#
# def get_app_name(opt):
#     """Get the application name"""
#     return os.path.join('ard_process', opt.country,
#                         opt.logtype, opt.date, opt.hour)
#
# def load_smartlocation_centroids(opt, spark):
#     """Load smartlocation centroids and broadcast"""
#     global CENTROID_SET_BROADCAST
#     centroid_path = opt.sl_centroid_path
#
#     if (centroid_path):
#         centroid_schema = StructType([\
#             StructField("latitude", StringType(), False), \
#             StructField("longitude", StringType(), False)])
#         centroid_df = spark.read.format('com.databricks.spark.csv') \
#             .options(header='false', mode="DROPMALFORMED", delimiter='\t') \
#             .load(centroid_path, schema=centroid_schema)
#         centroid_rdd = centroid_df.rdd.map(lambda r: r[0] + SL_DELIM + r[1])
#         centroid_set = set(centroid_rdd.collect())
#     else:
#         logging.info("sl_centroid_path not defined")
#         centroid_set = set()
#
#     sc = spark.sparkContext
#     CENTROID_SET_BROADCAST = sc.broadcast(centroid_set)
#     logging.info("CENTROID_SET size = {}".format(len(CENTROID_SET_BROADCAST.value)))
#
#
# def load_smartlocation_IPs(opt, spark):
#     """Load smartlocation IP addresses and broadcast"""
#     global IP_SET_BROADCAST
#     ip_path = opt.sl_ip_path
#
#     if (ip_path):
#         ip_schema = StructType([\
#             StructField("ip", StringType(), False), \
#             StructField("count", IntegerType(), False)])
#         ip_df = spark.read.format('com.databricks.spark.csv') \
#             .options(header='false', mode="DROPMALFORMED", delimiter='\t') \
#             .load(ip_path, schema=ip_schema)
#         ip_rdd = ip_df.rdd.map(lambda r: r[0])
#         ip_set = set(ip_rdd.collect())
#     else:
#         logging.info("sl_centroid_path not defined")
#         ip_set = set()
#
#     sc = spark.sparkContext
#     IP_SET_BROADCAST = sc.broadcast(ip_set)
#     logging.info("IP_SET size = {}".format(len(IP_SET_BROADCAST.value)))
#
#
# def main():
#
#     # Parse command line argumen
#     global opt
#     global spark
#
#     init_logging()
#     opt = parse_arguments()
#     dump_opt(opt)
#
#     # Logging and Config
# #    cfg = load_configuration(opt)
#
#     # Create the Spark Session
#     appName = get_app_name(opt)
#     spark = SparkSession.builder \
#         .appName(appName) \
#         .enableHiveSupport() \
#         .getOrCreate()
#     sc = spark.sparkContext
#
#     # Load Smartlocation centroids
#     logging.info("# Loading Smart Location centroids and IPs...")
#     load_smartlocation_centroids(opt, spark)
#     load_smartlocation_IPs(opt, spark)
#
#     # Load avro data
#     avro_path_tp = os.path.join(opt.input_dir, '{fill,nf}/{tll,pos}')
#     logging.info("Loading AVRO data {}...".format(avro_path_tp))
#     df_tp = spark.read.format("com.databricks.spark.avro").load(avro_path_tp)
#     df = df_tp.where((df_tp.uid !='') & (df_tp.sl_adjusted_confidence >=94))
#
#     # Select location related fields.
#     # Repartition data by uid,
#     # All the request belong to the same uid will go to the same partion.
#     # Sort rdds in each partition by uid and timestamp
#     logging.info("Repartioning..")
#     df = df.select('uid', 'request_id','r_timestamp',
#                    'latitude','longitude',
#                    'user_ip',
#                    'fp_matches',
#                    'r_s_info','sl_adjusted_confidence','request_filled')
#     df = df.repartition('uid').sortWithinPartitions('uid','r_timestamp')
#
#     # Apply the model on each partion
#     logging.info("Map Partitions..")
#     output_records = df.rdd.mapPartitions(process_partition)
#
#     # Output from model is a list of tuples, covnert tuples back to dataframe
#     logging.info("Create output dataframe..")
#     field = [StructField("request_id", StringType(), True),
#              StructField("r_s_info1", StringType(), True),
#              StructField("loc_score", StringType(), True),
#              StructField("fill", StringType(), True)]
#     schema = StructType(field)
#
#     # Save dataframe with partitions
#     logging.info("Save to {}...".format(opt.output_dir))
#     df_ab = spark.createDataFrame(output_records, schema=schema)
#     df_ab.write.mode("overwrite").format("orc") \
#         .option("compression","zlib") \
#         .mode("overwrite") \
#         .partitionBy('fill','loc_score') \
#         .save(opt.output_dir)
#
#     # Force the spark process to stop.
#     sc.stop()
#     logging.info("Done!")
#
#
# if __name__ == "__main__":
#     main()
#
