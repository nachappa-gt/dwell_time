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
from xad.common import system

# Huitao's library
#sys.path.append('/home/xad/sar-optimization/lib/modules/cmnfunc')
#import cmnfunc


class AbnormalRequest(BaseArd):
    """A class for downloading tables from the POI database."""

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

        # Get parameters
        dates = self.getDates('ard.process.window', 'yyyy-MM-dd')
        hours = self.getHours()
        regions = self.getRegions()
        #sl_levels = self.getSLLevels()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))
        #logging.info("- sl levels = {}".format(sl_levels))       

        scx_key_base = self.cfg.get('status_log_local.key.science_core_x')
        daily_tag = self.cfg.get('status_log_local.tag.daily')

        # Looping through all combinations
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)

                # Check daily status (optional)
                hourly_key = '/'.join([scx_key_base, country, logtype])
                daily_key = '/'.join([hourly_key, daily_tag])

                daily_status = self.status_log.getStatus(daily_key, date)
                if (daily_status is not None and daily_status == 1 and not self.FORCE):
                    logging.debug("x SKIP: found daily status {} {}".format(daily_key, date))
                    continue

                hour_count = 0
                for hour in hours:
                    # Check hourly gen status 
                    hourly_status = self.status_log.getStatus(daily_key, date + "/" + hour)
                    if (hourly_status is not None and hourly_status == 1 and not self.FORCE):
                        logging.debug("x SKIP: found hourly status {} {}:{}".format(hourly_key, date, hour))
                        ++hour_count
                        continue

                    # Check source (science_core_hrly) status 
                    # FIXME

                    # Run the Hive command
                    self.run_spark_cmd(country,logtype,date,hour)

                    # Touch hourly status
                    if (not self.NORUN):
                        self.status_log.addStatus(hourly_key, date + "/" + hour)
                        ++hour_count

                # Touch daily status
                if (hour_count == 24):
                    self.status_log.addStatus(daily_key, date)


    def run_spark_cmd(self,country,logtype,date,hour):
        """Run Spark command to generate science_core_x"""
        
        logging.info("Running Spark Command Line......")
         
        queue = self.cfg.get('ard.default.queue')
        spark_path = self.cfg.get('spark.script.process')
        driver_memory = self.cfg.get('spark.default.driver_memory')
        executor_memory = self.cfg.get('spark.default.executor_memory')
        num_executors = self.cfg.get('spark.default.num_executors')
        packages = self.cfg.get('spark.default.databricks')
        
        dates = date.split('-')
        year = dates[0]
        month = dates[1]
        day = dates[2]
        
 
        cmd = ["SPARK_MAJOR_VERSION=2"]
        cmd += ["spark-submit"]
        cmd += ["--master", "yarn"]
        cmd += ["--queue", queue ]
        cmd += ["--driver-memory", driver_memory]
        cmd += ["--executor-memory", executor_memory]
        cmd += ["--num-executors", num_executors]
        cmd += ["--packages", packages]
        cmd += [spark_path]
        cmd += ["--country", country]
        cmd += ["--logtype", logtype]
        cmd += ["--year", year]
        cmd += ["--month", month]
        cmd += ["--day", day]
        cmd += ["--hour", hour]

        cmdStr = " ".join(cmd)

        system.execute(cmdStr, self.NORUN)
        #subprocess.Popen(cmdStr, shell=True)   

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

    def _touch_local_status(args):
        """Touch the local file for status tracking (NOT USED)"""
        loggint.info("Generating Local Status File......")
        dir = 'ard'+'/' + args
        cmd = 'mkdir -p '
        cmd = cmd + dir
        p = subprocess.Popen(cmd, shell = True)


