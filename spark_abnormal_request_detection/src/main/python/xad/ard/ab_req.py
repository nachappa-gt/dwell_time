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
                
                dates = date.split('-')
                year = dates[0]
                month = dates[1]
                day = dates[2]
                hour_count = 0
                for hour in hours:
                    # Check hourly gen status 
                    hourly_status = self.status_log.getStatus(daily_key, date + "/" + hour)
                    if (hourly_status is not None and hourly_status == 1 and not self.FORCE):
                        logging.debug("x SKIP: found hourly status {} {}:{}".format(hourly_key, date, hour))
                        ++hour_count
                        continue
                     
                    # Check source (/data/extract) status 
                    input = self._get_science_core_avro_path(country, logtype, year, month, day, hour)
                    #success_path = os.path.join(input, "fill/tll/_SUCCESS")
                    avro_partitions = []
                    if (not hdfs.has(input)):
                        logging.info("x SKIP: MISSING AVRO FILE {}".format(success_path))
                        break
                    else:
                        # Check all the available partitions based on country, logtype, date, hour
                        # Pass these information to spark 
                        fill_partitions = ['fill','nf']
                        loc_score_partitions = ['tll','pos','rest']
                        for fill in fill_partitions:
                            for loc_score in loc_score_partitions:
                                success_partition_path = os.path.join(input, fill, loc_score, "_SUCCESS")
                                if (hdfs.has(success_partition_path)):
                                    partition = '-'.join([fill,loc_score])
                                    avro_partitions.append(partition)
                    
                    # Run the Spark command
                    self.run_spark_cmd(country,logtype,year,month,day,hour,avro_partitions)

                    # Check Spark job status, if completed, there should be an orc file
                    orc_path = self._get_science_core_orc_path(country, logtype, year, month, day, hour)
                    #success_orc_path = os.path.join(orc_path, "fill/tll/_SUCCESS")
                    orc_partitions = []
                    if (not hdfs.has(orc_path)):
                        logging.info("x SKIP: MISSING ORC FILE {}".format(orc_path))
                        break
                    else:
                        # Check all the available partitions based on country, logtype, date, hour
                        # Pass these information to hive
                        fill_partitions = ['fill','nf']
                        loc_score_partitions = ['tll','pos','rest']
                        for fill in fill_partitions:
                            for loc_score in loc_score_partitions:
                                success_partition_path = os.path.join(orc_path, fill, loc_score, "_SUCCESS")
                                if (hdfs.has(success_partition_path)):
                                    # Run the Hive command
                                    self.run_hive_cmd(country,logtype,date,year,month,day,hour,fill,loc_score,orc_path)

                    # Touch hourly status
                    if (not self.NORUN):
                        self.status_log.addStatus(hourly_key, date + "/" + hour)
                        ++hour_count

                    
                    

                # Touch daily status
                if (hour_count == 24):
                    self.status_log.addStatus(daily_key, date)


    def run_spark_cmd(self,country,logtype,year,month,day,hour,avro_partitions):
        """Run Spark command to generate science_core_x"""
        
        logging.info("Running Spark Command Line... ...")
        
        # Configurations of the Spark job
        queue = self.cfg.get('ard.default.queue')
        spark_path = self.cfg.get('spark.script.process')
        driver_memory = self.cfg.get('spark.default.driver_memory')
        executor_cores = self.cfg.get('spark.default.executor_cores')
        executor_memory = self.cfg.get('spark.default.executor_memory')
        num_executors = self.cfg.get('spark.default.num_executors')
        packages = self.cfg.get('spark.default.databricks')
        
        partitions = ','.join(avro_partitions)
        
        
        # command to run Spark, abnormal request detection model is built in Spark
        cmd = ["SPARK_MAJOR_VERSION=2"]
        cmd += ["spark-submit"]
        cmd += ["--master", "yarn"]
        cmd += ["--queue", queue ]
        cmd += ["--conf", "spark.yarn.executor.memoryOverhead=3000"]
        cmd += ["--driver-memory", driver_memory]
        cmd += ["--executor-memory", executor_memory]
        cmd += ["--num-executors", num_executors]
        cmd += ["--executor-cores", executor_cores]
        cmd += ["--packages", packages]
        cmd += [spark_path]
        cmd += ["--country", country]
        cmd += ["--logtype", logtype]
        cmd += ["--year", year]
        cmd += ["--month", month]
        cmd += ["--day", day]
        cmd += ["--hour", hour]
      
        cmd += ["--partitions",partitions]

        cmdStr = " ".join(cmd)

        system.execute(cmdStr, self.NORUN)

    def run_hive_cmd(self,country,logtype,date,year,month,day,hour,fill,loc_score,orc_path):
        
        # Run Hive command to add partitions into hive table
        logging.info("Running Hive Command Line......")
        queue = self.cfg.get('ard.default.queue')
        table_name = self.cfg.get('ard.output.table')
  
        hive_query = ''
        
        hive_template = Template("\"alter table ${table_name} add partition (cntry='${country}', dt='${dt}', prod_type= '${prod_type}', hour='${hour}', fill='${fill}', loc_score='${loc_score}') location '${path}';\"")      
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
        
    """def run_hive_cmd(self,country,logtype,date,year,month,day,hour,fill,loc_score):
        #Run Hive command to generate add partitions to Hive Table
        
        logging.info("Running Hive Command Line......")
         
        queue = self.cfg.get('ard.default.queue')
        hql_path = self.cfg.get('hive.script.ard-gen-partition')
        
        base_dir = self._get_science_core_orc_path(country, logtype, year, month, day, hour)
        location_path = os.path.join(base_dir,fill,loc_score)
        country = '\'' + country + '\''
        logtype = '\'' + logtype + '\''
        date = '\'' + date + '\''
        hour = '\'' + hour + '\''
        fill = '\'' + fill + '\''
        location_path = '\'' + location_path + '\''

        cmd = ["beeline"]
        cmd += ["-u", '"' + self.cfg.get('hiveserver.uri') + '"']
        cmd += ["--hiveconf", "tez.queue.name=" + queue]
        cmd += ["-n", os.environ['USER']]  
        cmd += ["-f", hql_path]
        cmd += ["--hivevar", '"SCIENCE_CORE_TABLE=' + self.cfg.get('ard.output.table') + '"'] 
        cmd += ["--hivevar", "\"COUNTRY=" +country +"\""]
        cmd += ["--hivevar", "\"LOGTYPE=" +logtype+ "\""]
        cmd += ["--hivevar", "\"DATE=" + date + "\""]
        cmd += ["--hivevar", "\"HOUR=" + hour + "\""]
        cmd += ["--hivevar", "\"FILL=" + fill+ "\""]
        cmd += ["--hivevar", "\"LOC_SCORE=" + loc_score + "\""]
        cmd += ["--hivevar", "\"PATH=" + location_path+ "\""]

        
        cmdStr = " ".join(cmd)

        system.execute(cmdStr, self.NORUN) """
    
    """def run_hive_cmd(self,country,logtype,date,year,month,day,hour,fill,loc_score,orc_path):

        logging.info("Running Hive Command Line......")
        queue = self.cfg.get('ard.default.queue')
        table_name = self.cfg.get('ard.output.table')
  
        base_dir = self.cfg.get('proj.hive.tmp.dir')
        hql_dir = os.path.join(base_dir, country, logtype, year, month, day, hour, fill, loc_score)
        hql_path = os.path.join(hql_dir, 'hive.hql')
     
        touch_dir = ""
        touch_dir  += "mkdir -p" + " " + hql_dir
        system.execute(touch_dir, self.NORUN) 

        self.create_hql_file(table_name, country, logtype, date, year, month, day, hour, fill, loc_score, orc_path)
        
        cmd = []
        cmd = ["beeline"]
        cmd += ["-u", '"' + self.cfg.get('hiveserver.uri') + '"']
        cmd += ["--hiveconf", "tez.queue.name=" + queue]
        cmd += ["-n", os.environ['USER']]  
        cmd += ["-f", hql_path]
        
        command = ' '.join(cmd)
        system.execute(command, self.NORUN) 
        
        del_dir = ""
        del_dir  += "rm -r" + " " + hql_dir
        system.execute(touch_dir, self.NORUN)
        

    def create_hql_file(self,table_name, country, logtype, date, year, month, day, hour, fill, loc_score, orc_path):
        
        base_dir = self.cfg.get('proj.hive.tmp.dir')
        hql_dir = os.path.join(base_dir, country, logtype, year, month, day, hour, fill, loc_score)
        hql_path = os.path.join(hql_dir,'hive.hql')
        hql_file = open(hql_path, 'w')

        cmd = ""
        
        hive_template = Template("alter table ${table_name} add partition (cntry='${country}', dt='${dt}', prod_type= '${prod_type}', hour='${hour}', fill='${fill}', loc_score='${loc_score}') location '${path}';")
        
       
        query = hive_template.substitute(table_name = table_name, country = country, dt = date, prod_type = logtype, hour = hour, fill= fill, loc_score = loc_score, path = orc_path)
        cmd += query 
        cmd += "\n"
      
        hql_file.write(cmd)
        hql_file.close()"""
        
 

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

    def _get_science_core_orc_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('orc.data.hdfs')
        return os.path.join(base_dir, country, logtype, *entries)


