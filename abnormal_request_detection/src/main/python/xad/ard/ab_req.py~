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

	return

        # Looping through all combinations
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)

                # no _get_local_status function before, need to write one
                dailyStatus = self._get_local_status(country, logtype, date, SUCCESS_GEN)
                
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
                    self.run_hive_cmd(country,logtype,date,hour,fill,sl)

                    # Touch hourly status
                    if (self._has_full_hour(hourlyStatus) and not self.NORUN):
                        self._touch_local_status(hourlyStatus)

                # Touch daily status
                if (self._has_full_day(dailyStatus) and not self.NORUN):
                    self._touch_local_status(dailyStatus)


    def run_hive_cmd(country,logtype,date,hour,fill,sl):
        
        logging.info("Running Hive Command Line......")
         
        queue = "xianglingmeng"
        table_name = "xianglingmeng.science_core_orc"
        self.create_hql_file(table_name, country, date, logtype, hour, fill)
        output_path = "./hive.hql"
        cmd = ["beeline", "-n", "\"jdbc:hive2://ip-172-17-25-136.ec2.internal:2181,ip-172-17-25-137.ec2.internal:2181,ip-172-17-25-135.ec2.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2","--hiveconf", "tez.queue.name ==", queue, "-f", output_path]
        
        p = subprocess.popen(cmd)

    

    def create_hql_file(table_name, country, dt, prod_type, hour, fill, loc_score):

        loggint.info("Creating hql file......")
        query_template = Template("create table ${table_name} if not exists partition (cntry='${country}', dt='${dt}', prod_type='${prod_type}', hour='${hour}', fill='${fill}', loc_score='${loc_score}');")
    
        queue = "xianglingmeng";

        hive_cmd = ""
        hive_cmd += 'set tez.queue.name = ' + queue + ";\n"
        hql_path = "/hive.sql"
        hql_file = open(hql_path, 'w')
        ## we can make changes in the for loop to generate queies for different country, dt, prod_typ, hour, fill and loc_score
        for i in range(2):
            query = query_template.substitute(table_name = table_name, country = country, dt = dt, prod_type = prod_type, hour = hour, fill= fill, loc_score = loc_score)
    	    hive_cmd += query + "\n"     
    
        hql_file.write(hive_cmd)
        hql_file.close()



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


