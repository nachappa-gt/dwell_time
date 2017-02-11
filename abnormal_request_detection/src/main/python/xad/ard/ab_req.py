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
                    self.run_hive_cmd(country,logtype,date,hour)

                    # Touch hourly status
                    if (not self.NORUN):
                        self.status_log.addStatus(hourly_key, date + "/" + hour)
                        ++hour_count

                # Touch daily status
                if (hour_count == 24):
                    self.status_log.addStatus(daily_key, date)


    def run_hive_cmd(self,country,logtype,date,hour):
        """Run Hive command to generate science_core_x"""
        
        logging.info("Running Hive Command Line......")
         
        queue = self.cfg.get('ard.default.queue')
        table_name = self.cfg.get('ard.default.tmp.schema') 
        table_name += country + hour + logtype
        join_table = self.cfg.get('ard.default.join.schema')
        join_table += country + hour + logtype 
        hql_path = self.cfg.get('hive.script.ard-gen')

        
        
        cmd = ["beeline"]
        cmd += ["-u", '"' + self.cfg.get('hiveserver.uri') + '"']
        cmd += ["--hiveconf", "tez.queue.name=" + queue]
        cmd += ["-n", 'xad']  #FIXME need to get username from system
        cmd += ["-f", hql_path]
        cmd += ["--hivevar"]
        cmd += ['"' + "ARD_MAPPER=" + "hdfs://" +self.cfg.get('hdfs.model.mapper.dir')+'"']
        cmd += ["--hivevar"]
        cmd += ['"' +"ARD_REDUCER=" + "hdfs://" +self.cfg.get('hdfs.model.reducer.dir')+'"']
        cmd += ["--hivevar", '"TMP_TABLE=' + table_name + '"' ]
        cmd += ["--hivevar", '"SCIENCE_CORE_TABLE=' + self.cfg.get('ard.input.table') + '"']
        cmd += ["--hivevar", '"JOIN_TABLE=' + join_table + '"']
        cmd += ["--hivevar", '"COUNTRY=' +"'"+ country + "'"+'"']
        cmd += ["--hivevar", '"LOGTYPE=' +"'"+ logtype+ "'" +'"']
        cmd += ["--hivevar", '"DATE=' +"'"+ date+ "'" + '"']
        cmd += ["--hivevar", '"HOUR=' + hour + '"']
        cmdStr = " ".join(cmd)

        system.execute(cmdStr, self.NORUN)   

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

