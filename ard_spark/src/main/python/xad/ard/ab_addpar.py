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

class AddPartition(BaseArd):
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

        """ Get parameters"""
        dates = self.getDates('ard.process.window', 'yyyy-MM-dd')
        hours = self.getHours()
        regions = self.getRegions()
        #sl_levels = self.getSLLevels()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))
        #logging.info("- sl levels = {}".format(sl_levels))      
        

        """Looping through all combinations"""
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)
                              
                dates = date.split('-')
                year = dates[0]
                month = dates[1]
                day = dates[2]
                hour_count = 0
                
                base = self.cfg.get('status_log_local.key.add_partition')
                hourly_key = '/'.join([base, country, logtype])

                for hour in hours:
                    """Check hourly gen status""" 
                    logging.info("PROCESSING:" + country + ',' + logtype +',' + date + ',' + hour)

                    
                    hourly_status = self.status_log.getStatus(hourly_key, date + "/" + hour)
                    
                    if (hourly_status is not None and hourly_status == 1 and not self.FORCE):
                        logging.debug("x SKIP: found hourly status {},{} {}:{}".format(country,logtype, date, hour))
                        continue

                    """old_path = os.path.join('/data/science_core_ex_new',country, logtype, year, month, day, hour)
                    if (hdfs.has(old_path)):
                        self.mvHDFS(country, logtype, year, month, day, hour)"""

                    """Check Spark job status, if completed, there should be an orc file"""
                    orc_path = self._get_science_core_orc_path(country, logtype, year, month, day, hour)
                    
                    if (not hdfs.has(orc_path)):
                        logging.info("x SKIP: MISSING ORC FILE {}".format(orc_path))
                        continue
                    else:
                        """Check all the available partitions based on country, logtype, date, hour
                           Pass these information to hive"""
                        fill_partitions = ['fill','nf']
                        loc_score_partitions = ['tll','pos','rest']
                        for fill in fill_partitions:
                            for loc_score in loc_score_partitions:
                                success_partition_path = os.path.join(orc_path, fill, loc_score)
                                if (hdfs.has(success_partition_path)):                                    
                                    """Run the Hive command"""                                    
                                    self.run_hive_cmd(country,logtype,date,year,month,day,hour,fill,loc_score,success_partition_path)
                   
                    if (not self.NORUN): 
                        logging.info("Add Partition hourly status {},{} {}:{}".format(country,logtype, date, hour))           
                        self.status_log.addStatus(hourly_key, date + "/" + hour) 
   

    def run_hive_cmd(self,country,logtype,date,year,month,day,hour,fill,loc_score,orc_path):
        
        """Run Hive command to add partitions into hive table"""
        logging.info("Running Hive Command Line......")
        logging.info(str(datetime.now()))
        queue = self.cfg.get('ard.default.queue')
        table_name = self.cfg.get('ard.output.table')
  
        hive_query = ''
        
        hive_template = Template("\"alter table ${table_name} drop if exists partition (cntry='${country}', dt='${dt}', prod_type= '${prod_type}', hour='${hour}', fill='${fill}', loc_score='${loc_score}'); alter table ${table_name} add if not exists partition (cntry='${country}', dt='${dt}', prod_type= '${prod_type}', hour='${hour}', fill='${fill}', loc_score='${loc_score}') location '${path}';\"")      
        query = hive_template.substitute(table_name = table_name, country = country, dt = date, prod_type = logtype, hour = hour, fill= fill, loc_score = loc_score, path = orc_path)
        hive_query += query
        
        cmd = []
        cmd = ["beeline"]
        cmd += ["-u", '"' + self.cfg.get('hiveserver.uri') + '"']
        cmd += ["--hiveconf", "tez.queue.name=" + queue]
        cmd += ["-n", os.environ['USER']]  
        cmd += ["-e", hive_query]
        
        command = ' '.join(cmd)
        system.execute(command, self.NORUN)     

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

    def _get_science_core_avro_path(self, country, logtype, *entries):
        """Get path to the AVRO-based science foundation files"""
        base_dir = self.cfg.get('extract.data.prefix.hdfs')
        return os.path.join(base_dir, country, logtype, *entries)

    def _get_abd_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('abd.data.hdfs')
        return os.path.join(base_dir, country, logtype, *entries)

    def _get_science_core_orc_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('orc.data.hdfs')
        return os.path.join(base_dir, country, logtype, *entries)

    def mvHDFS(self, country, logtype, year, month, day, hour):
        """Move completed one-hour data from tmp file to data/science_core_ex"""
        tmp_base_dir = '/data/science_core_ex_new'
        output_base_dir = '/data/science_core_ex'
        date_path = '/'.join([country, logtype, year, month, day])
        hour_path = '/'.join([date_path, hour])

        tmp_path = os.path.join(tmp_base_dir, hour_path)
        output_path = os.path.join(output_base_dir, date_path)

        cmd = []
        cmd += ['hdfs dfs -mv']
        cmd += [tmp_path, output_path]
       
        cmdStr = ' '.join(cmd)
        
        mkdir = []
        mkdir += ['hdfs dfs -mkdir -p', output_path]

        mkdirCmd = ' ' .join(mkdir)
        
        system.execute(mkdirCmd, self.NORUN)
        system.execute(cmdStr, self.NORUN)



















