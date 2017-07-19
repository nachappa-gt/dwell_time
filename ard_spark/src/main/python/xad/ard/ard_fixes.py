# -*- coding: utf-8 -*-
"""
Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.

@author: xiangling, victor
"""

import logging
import os
 
from baseard import BaseArd
from xad.common import hdfs

class ArdFixes(BaseArd):
    """Provided various kinds of one-time fixes"""

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseArd.__init__(self, cfg, opt)
        self.status_log = self.STATUS_L
 
    #----------------
    # Add Partitions
    #----------------
    def addPartitions(self):
        """Add Hive partitions.
        """
        logging.info('Generating Science Core orc files with Abnormal Request...')

        """ Get parameters"""
        dates = self.getDates('ard.process.window', 'yyyy/MM/dd')
        hours = self.getHours()
        regions = self.getRegions()
        #sl_levels = self.getSLLevels()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))
        #logging.info("- sl levels = {}".format(sl_levels))      
        
        # key prefix for status log
        keyPrefix = self.cfg.get('status_log_local.key.add_partition')

        """Looping through all combinations"""
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)
                              
                hour_count = 0

                # Keys for status_log
                hourly_key = os.path.join(keyPrefix, country, logtype)
                daily_key = os.path.join(hourly_key, 'DAILY')
                
                # CHECK DAILY status
                daily_status = self.status_log.getStatus(daily_key, date)
                if (daily_status and not self.FORCE):
                    logging.debug("x SKIP: found daily status {}:{}".format(daily_status, date))
                    continue;

                for hour in hours:
                    """Check hourly gen status""" 
                    logging.info("# PROCESSING: " + country + ',' + logtype +',' + date + ',' + hour)
                    
                    hourly_status = self.status_log.getStatus(hourly_key, date + "/" + hour)
                    
                    if (hourly_status is not None and hourly_status == 1 and not self.FORCE):
                        logging.debug("x SKIP: found status {}: {},{}".format(hourly_key, date, hour))
                        hour_count += 1
                        continue

                    """Check Spark job status, if completed, there should be an orc file"""
                    hourly_path = self._get_science_core_orc_path(country, logtype, date, hour)
                    subparts = self.getSubHourPartitions(hourly_path)
                    
                    if (len(subparts) == 0):
                        logging.info("x SKIP: MISSING ORC FILE {}".format(hourly_path))
                        continue
                    else:
                        # Check all the available partitions based on country, logtype, date, hour
                        #   Pass these information to hive"""
                        self.addHivePartitions(country, logtype, date, hour,
                                               subparts, hourly_path)

                    if (not self.NORUN): 
                        logging.info("Add status {}: {},{}".format(hourly_key, date, hour))           
                        self.status_log.addStatus(hourly_key, date + "/" + hour) 
                        hour_count += 1
   
                # add daily k ey
                if (hour_count == 24):
                    logging.info("Add daily status {}: {}".format(daily_key, date))           
                    self.status_log.addStatus(daily_key, date) 

    #----------------
    # Fix Repeated 
    #----------------
    def fixRepeatedFolders(self):
        """Fix repeated folders.
        
        Observed "rest/rest" and "pos/pos" for us/gb from mid May to early June.
        Use this function to fix them.
        """

        logging.info('Fixing Repeated Sub-Partition Folders...')

        """ Get parameters"""
        dates = self.getDates('ard.process.window', 'yyyy/MM/dd')
        hours = self.getHours()
        regions = self.getRegions()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))

        for date in dates:
            for region in regions:
                # Split region
                (country,logtype) = self.splitRegion(region)
                hourly_key = self.get_fixrep_status_key(country, logtype)
                daily_key = self.get_fixrep_status_key(country, logtype, True)

                # Check daily status
                daily_status = self.status_log.getStatus(daily_key, date)
                if (daily_status and not self.FORCE):
                    logging.info("x SKIP: found {} {}".format(daily_key, date))
                    continue
                
                num_hours = 0
                for hour in hours:
                    time = os.path.join(date, hour)
                    hourly_status = self.status_log.getStatus(hourly_key, time)
                    if (hourly_status and not self.FORCE):
                        logging.info("x SKIP: found {} {} {}".format(hourly_key, date, hour))
                        num_hours += 1
                        continue

                    self._fix_repeated_helper(country,logtype,date,hour)
                    
                    if not self.NORUN:
                        num_hours += 1
                        self.status_log.addStatus(hourly_key, time)
                        
                if (num_hours == 24):
                    self.status_log.addStatus(daily_key, date)

    def _fix_repeated_helper(self,country,logtype,date,hour):
        hour_path = self._get_science_core_orc_path(country, logtype, date, hour)
        subparts = self.getSubHourPartitions(hour_path)
        for p in subparts:
            if p[1] == 'tll':
                continue
            
            rep_path = os.path.join(hour_path, p[0], p[1], p[1])
            if (not hdfs.has(rep_path)):
                continue
            
            logging.info("- Fixing {}...".format(rep_path))
            tmp_path = os.path.join(hour_path, p[0], "tmp")
            dest_path = os.path.join(hour_path, p[0], p[1])
            hdfs.mv(rep_path, tmp_path, self.NORUN)
            hdfs.rmrs(dest_path, self.NORUN)
            hdfs.mv(tmp_path, dest_path, self.NORUN)

    def get_fixrep_status_key(self, country, logtype, daily=False):
        """Get status_log key for adding a S3 Partition in Hive"""
        prefix = self.cfg.get('status_log_local.key.fixrep')
        tag = self.cfg.get('status_log_local.tag.daily') if daily else ""
        if (tag):
            key = os.path.join(prefix, country, logtype, tag)
        else:
            key = os.path.join(prefix, country, logtype)
        return key
        
    
    #----------------
    # Fix Missing 
    #----------------        
    def fixMissing(self):
        """Fixing missing sub-hour partitions with empty folders
        
        The older code may missing some partition folders if they
        are empty.   This method will fill those folders and
        add them to the Hive partition.
        """
        
        logging.info('Fixing Missing Folders...')

        """ Get parameters"""
        dates = self.getDates('ard.process.window', 'yyyy/MM/dd')
        hours = self.getHours()
        regions = self.getRegions()
        #sl_levels = self.getSLLevels()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))
        #logging.info("- sl levels = {}".format(sl_levels))      

        keyPrefix = self.cfg.get('status_log_local.key.science_core_orc')
                
        """Looping through all combinations"""
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)

                # Find missing sub partitions
                day_path = self._get_science_core_orc_path(country, logtype, date)
                missing_hour_subparts = self.findMissingPartitions(day_path) 
                               
                num_hours = len(missing_hour_subparts)
                if num_hours == 0:
                    logging.info("x SKIP - NO MISSING PARTS for {} {}".format(region, date))
                    continue;
                else:
                    logging.info("## FIXING {} HOURS for {} {}...".format(num_hours, region,date))
                               
                # Status log key
                hourly_key = os.path.join(keyPrefix, country, logtype)

                for hour, missing_subparts in missing_hour_subparts:
                    # Won't proceeed unless there this hour has been processed
                    logging.info("# FIXING: " + country + ',' + logtype +',' + date + ',' + hour)
                    hourly_status = self.status_log.getStatus(hourly_key, date + "/" + hour)
                    if (hourly_status is None or hourly_status != 1):
                        logging.debug("x SKIP: missing {} {}:{}".format(hourly_key, date, hour))
                        break

                    # Get source sub-hour partitions
                    orc_path = self._get_science_core_orc_path(country, logtype, date, hour)
                    self.addMissingHDFSPartitions(orc_path, missing_subparts)

                    # Add Hive partitions
                    self.addHivePartitions(country, logtype, date, hour,
                                           missing_subparts, orc_path)


    def fixStatusLog(self):
        """Add status log for for those with data"""
        logging.info('Fixing Status Log...')

        """ Get parameters"""
        dates = self.getDates('ard.process.window', 'yyyy/MM/dd')
        hours = self.getHours()
        regions = self.getRegions()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))

        keyPrefix = self.cfg.get('status_log_local.key.science_core_orc')
        daily_tag = self.cfg.get('status_log_local.tag.daily')
                
        """Looping through all combinations"""
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)
                                               
                # Status log key
                hourly_key = os.path.join(keyPrefix, country, logtype)
                daily_key = os.path.join(hourly_key, daily_tag)

                hour_count = 0
                for hour in hours:
                    # Skip if there is already a status log
                    hourly_status = self.status_log.getStatus(hourly_key, date + "/" + hour)
                    if (hourly_status):
                        logging.info("x SKIP:  Found atatus {} {}/{}".format(hourly_key, date, hour))
                        hour_count += 1
                        continue

                    # Get source sub-hour partitions
                    orc_path = self._get_science_core_orc_path(country, logtype, date, hour)
                    if (self.FORCE or hdfs.has(orc_path)):                  
                        logging.info("+ Adding Hourly: {} {}/{}".format(hourly_key, date, hour))
                        if (not self.NORUN):              
                            self.status_log.addStatus(hourly_key, date + "/" + hour)                        
                        hour_count += 1
                    else:
                        logging.info("x SKIP: not ORC in {}".format(orc_path))
                        break

                # Touch daily status
                if (hour_count == 24):
                    daily_status = self.status_log.getStatus(daily_key, date)
                    if (not daily_status):
                        logging.info("+ Adding DAILY: {} {}".format(hourly_key, date))
                        if (not self.NORUN):
                            self.status_log.addStatus(daily_key, date)


