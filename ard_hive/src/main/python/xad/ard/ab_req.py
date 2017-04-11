# -*- coding: utf-8 -*-
"""
Copyright (C) 2016.  xAd, Inc.  All Rights Reserved.

@author: xiangling
"""

import logging
import os
import re
import sys
from string import Template    
import subprocess

from baseard import BaseArd
from datetime import datetime

from xad.common import dateutil
from xad.common import hdfs

# Huitao's library
#sys.path.append('/home/xad/sar-optimization/lib/modules/cmnfunc')
#import cmnfunc


class AbnormalRequest(BaseArd):
    """A class for downloading tables from the POI database."""

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseArd.__init__(self, cfg, opt)
 
    #-----------------------
    # Processing Hourly Data
    #-----------------------

    def genHourly(self):

        """Generate updated Science Core orc table with new features in it. """
        logging.info('Generating Science Core orc files with Abnormal Request...')

        # Get parameters
        dates = self.getDates('ard.process.window', 'yyyy/MM/dd')
        hours = self.getHours()
        regions = self.getRegions()
        #sl_levels = self.getSLLevels()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))
        #logging.info("- sl levels = {}".format(sl_levels))       

        # Looping through all combinations
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)

                # no _get_local_status function before, need to write one
                dailyStatus = self._get_local_status(country, logtype, date)
                
                if (os.path.exists(dailyStatus)):
                    logging.debug("x SKIP: found {}".format(dailyStatus))
                    continue

                # Check daily status (optional)


                for hour in hours:
                    # Check hourly gen status 

                    # Check source status
                    #hourlyStatus = self._get_local_status(country, logtype, date, hour, SUCCESS_GEN)
                    #if (os.path.exists(hourlyStatus)):
                    #    logging.debug("x SKIP: found {}".format(hourlyStatus))
                    #    continue
                    self.run_hive_cmd(country,logtype,date,hour)

                    # Touch hourly status
                    if (self._has_full_hour(hourlyStatus) and not self.NORUN):
                        self._touch_local_status(hourlyStatus)

                # Touch daily status
                if (self._has_full_day(dailyStatus) and not self.NORUN):
                    self._touch_local_status(dailyStatus)


    def run_hive_cmd(self,country,logtype,date,hour):
        
        logging.info("Running Hive Command Line......")
         
        # FIXME: Don't hard code parameters
        queue = "xianglingmeng"
        table_name = "xianglingmeng.science_core_orc"
        hql_path = self.cfg.get('hive.script.ard-gen')

        # FIXME: Need a unique name
        # FIXME: Don't hard code the connection infor
        cmd = ["beeline", "-u", "\"jdbc:hive2://ip-172-17-25-136.ec2.internal:2181,ip-172-17-25-137.ec2.internal:2181,ip-172-17-25-135.ec2.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2","--hiveconf", "tez.queue.name ==", queue, "-n", "xianglingmeng", "-f", hql_path]
        
        p = subprocess.Popen(cmd)

        # FIXME: Delete the hql file
    
    def _touch_local_status(args):
        loggint.info("Generating Local Status File......")
        dir = 'ard'+'/' + args
        cmd = 'mkdir -p '
        cmd = cmd + dir
        p = subprocess.Popen(cmd, shell = True)
        
    
   

    #-------------------
    # Helper Functions
    #-------------------

    def _getDate(self):
        """Get today's date"""
        if (self.DATE):
            date = self.DATE
        else:
            date = dateutil.today()
        return(date)

    def _getURI(self):
        """Get the URI for DB connection"""
        host = self.cfg.get('poidb.conn.host')
        port = self.cfg.get('poidb.conn.port')
        dbname = self.cfg.get('poidb.conn.dbname')
        uri = "jdbc:postgresql://{}:{}/{}".format(host, port, dbname);
        return (uri)

    def _getHDFSDir(self, entries):
        """Get the target HDFS directory"""
        prefix = self.cfg.get('poidb.data.prefix.hdfs')
        path = os.path.join(prefix, *entries)
        return(path)

    def _getHDFSTmpDir(self, date):
        """Get a temporary working directory."""
        appTmpDir = self.getHDFSUserTmpDir()
        prefix = self.cfg.get('poidb.tmp.prefix')
        date = re.sub('-', '', date)  # remove '-'
        folder = "_".join([prefix, date])
        path = os.path.join(appTmpDir, folder)
        return(path)


