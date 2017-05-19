# -*- coding: utf-8 -*-
"""
Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.

@author: xiangling
"""

import logging
import os
 
from baseard import BaseArd



class AddPartition(BaseArd):
    """Add Hive partitions.
    
    The purpose of this class is to fixed Hive partition, e.g., caused wrong
    locations.   It will replace the current partitions with new ones.

    It also support status_log, with "add_partition" be the key prefix.    
    """

    def __init__(self, cfg, opt, status_log):
        """Constructor"""
        BaseArd.__init__(self, cfg, opt)
        self.status_log = status_log
 
    #------------------------
    # Processing Hourly Data
    #------------------------

    def genHourly(self):

        """Generate updated Science Core orc table with new features in it. """
        logging.info('Generating Science Core orc files with Abnormal Request...')

        """ Get parameters"""
        dates = self.getDates('ard.process.window')
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
                              
                dates = date.split('-')
                year = dates[0]
                month = dates[1]
                day = dates[2]
                # FIXME
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
                    logging.info("PROCESSING:" + country + ',' + logtype +',' + date + ',' + hour)
                    
                    hourly_status = self.status_log.getStatus(hourly_key, date + "/" + hour)
                    
                    if (hourly_status is not None and hourly_status == 1 and not self.FORCE):
                        logging.debug("x SKIP: found status {}: {} {}".format(hourly_key, date, hour))
                        hour_count += 1
                        continue

                    """Check Spark job status, if completed, there should be an orc file"""
                    hourly_path = self._get_science_core_orc_path(country, logtype, year, month, day, hour)
                    subparts = self.getSubHourPartitions(hourly_path)
                    
                    # FIXME
                    if (len(subparts) == 0):
                        logging.info("x SKIP: MISSING ORC FILE {}".format(hourly_path))
                        continue
                    else:
                        # Check all the available partitions based on country, logtype, date, hour
                        #   Pass these information to hive"""
                        self.addHivePartitions(country, logtype, date, hour,
                                               subparts, hourly_path)

                    if (not self.NORUN): 
                        logging.info("Add status {}: {} {}".format(hourly_key, date, hour))           
                        self.status_log.addStatus(hourly_key, date + "/" + hour) 
                        hour_count += 1
   
                # FIXME - add daily k ey
                if (hour_count == 24):
                    logging.info("Add daily status {}: {}".format(daily_key, date))           
                    self.status_log.addStatus(daily_key, date) 





