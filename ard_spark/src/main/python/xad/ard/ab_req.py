# -*- coding: utf-8 -*-
"""
Copyright (C) 2017.  xAd, Inc.  All Rights Reserved.

@author: xiangling
"""

import logging
import os

from baseard import BaseArd
from xad.common import hdfs
from xad.common import system

ABD_MAP = {'fill=FILLED':'fill', 'fill=NOT_FILLED':'nf',
           'loc_score=95':'tll', 'loc_score=94':'pos'}


class AbnormalRequest(BaseArd):
    """A class for downloading tables from the POI database."""

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseArd.__init__(self, cfg, opt)
        self.status_log = self.STATUS_L
    #------------------------
    # Processing Hourly Data
    #------------------------

    def genHourly(self):
        """Generate updated Science Core orc table with new features in it. 
        
        For us and gb, it will detect abnormal requests and join them
        with the original AVRO and modify r_s_info.
        For other countries, it will only conver AVRO to ORC.
        """
        logging.info('Generating Science Core orc files with Abnormal Request...')

        # Get parameters
        dates = self.getDates('ard.process.window', 'yyyy/MM/dd')
        hours = self.getHours()
        regions = self.getRegions()
        fill_folders = self.getFillFolders()
        sl_folders = self.getSLFolders()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))
        logging.info("- fill folders = {}".format(fill_folders))      
        logging.info("- sl folders = {}".format(sl_folders))   
        if (self.NOMODEL):
            logging.info("- NOMODEL")   
        if (self.NOJOIN):
            logging.info("- No JOIN (anomily detection only)")   
            self.NOHIVE = True
            self.NOSTATUS = True
            self.NOFIX = True
        if (self.NORUN):
            logging.info("- NORUN")   
        if (self.NOHIVE):
            logging.info("- NOHIVE")   
        if (self.NOSTATUS):
            logging.info("- NOSTATUS")   
        if (self.PARTIAL):
            logging.info("- PARTIAL")   

        keyPrefix = self.cfg.get('status_log_local.key.science_core_x')
        daily_tag = self.cfg.get('status_log_local.tag.daily')
                
        # Looping through all combinations
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)
                               
                # Check daily status (optional)
                hourly_key = os.path.join(keyPrefix, country, logtype)
                daily_key = os.path.join(hourly_key, daily_tag)

                daily_status = self.status_log.getStatus(daily_key, date)
                if (daily_status is not None and daily_status == 1
                        and not self.FORCE and not self.NOSTATUS):
                    logging.info("x SKIP: found daily status {} {}".format(daily_key, date))
                    continue

                hour_count = 0

                for hour in hours:
                    # Check hourly gen status
                    logging.info("PROCESSING:" + country + ',' + logtype +',' + date + ',' + hour)
                    hourly_status = self.status_log.getStatus(hourly_key, date + "/" + hour)
                    if (hourly_status is not None and
                            hourly_status == 1 and not self.FORCE):
                        logging.info("x SKIP: found hourly status {} {}:{}".format(hourly_key, date, hour))
                        hour_count += 1
                        continue

                    # Check for SL hourly update (dependency)
                    if not self.NOSL:
                        sl_hourly_key = self.getSLHourlyStatusLogKey(country)
                        sl_status = self.status_log.getStatus(sl_hourly_key, date + "/" + hour)
                        if (not sl_status and not self.FORCE):
                            logging.info("x SKIP: missing status: {} {}/{}".format(sl_hourly_key, date, hour))
                            break               
                    
                    # Get source sub-hour partitions
                    avro_path = self._get_science_core_avro_path(country, logtype, date, hour)
                    avro_subparts = self.getSubHourPartitions(avro_path, '-')
                    if len(avro_subparts) == 0:
                        logging.info("x SKIP: missing source {}: {}".format(avro_path, avro_subparts))
                        break                        

                    # Helper function
                    self._genHourlyHelper(country, logtype, date, hour, avro_subparts)
                                                                                              
                    # Touch hourly status
                    if (not self.NORUN and not self.NOSTATUS):              
                        self.status_log.addStatus(hourly_key, date + "/" + hour)                        
                        hour_count += 1

                # Touch daily status
                if (hour_count == 24 and not self.NOSTATUS):
                    self.status_log.addStatus(daily_key, date)


    def _genHourlyHelper(self, country, logtype, date, hour, avro_subparts):
        # Delete previous tmp dir              
        model_countries = set(self.cfg.get_array('ard.model.countries'))          
        tmp_path = self._get_tmp_path(country, logtype, date, hour)
        logging.info("tmp_path = '{}'".format(tmp_path))
        if hdfs.has(tmp_path):
            hdfs.rmrs(tmp_path)

        # Run the Spark command
        if country in model_countries:
            # Generate models
            if (not self.NOMODEL):
                self.run_spark_model(country,logtype, date, hour)

            # Check model outputs status, then pass it to join with orginal data
            abd_path = self._get_abd_path(country, logtype, date, hour)
            abd_subparts = self.getSubHourPartitions(abd_path, '-', ABD_MAP)
            logging.info("abd_subparts = {}".format(abd_subparts))

            # Join the Spark Dataframe and save as orc file
            if (not self.NOJOIN):
                if len(abd_subparts) > 0:
                    self.run_spark_join(country,logtype, date, hour,
                                        avro_subparts, abd_subparts)
                else:
                    self.run_spark_orc(country, logtype, date, hour,
                                       avro_subparts)
        else:
            # No detection for other countries.  Just convert to ORC
            self.run_spark_orc(country, logtype, date, hour, avro_subparts)
                                    
        # Fill empty partitions with empty ORC
        if not (self.NOFIX or self.NOJOIN or self.NORUN):
            hasFillPartition = False if (logtype == 'display_dr') else True
            self.addMissingHDFSPartitions(tmp_path, hasFillPartition)

        # FIXME - Move into run_spark_orc() or run_spark_join.
        # Use a dedicated function generate tmp path.
        if not (self.NORUN or self.NOJOIN): 
            self.mvHDFS(country, logtype, date, hour, self.PARTIAL)   

        # Add Hive partitions
        if not (self.NOHIVE or self.NOJOIN):
            orc_path = self._get_science_core_orc_path(country, logtype, date, hour)
            orc_subparts = self.getSubHourPartitions(orc_path)                    

            if (len(orc_subparts) == 0):
                logging.info("x SKIP: MISSING ORC {}".format(orc_path))
            else:
                self.addHivePartitions(country, logtype, date, hour,
                                       orc_subparts, orc_path)


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

        keyPrefix = self.cfg.get('status_log_local.key.science_core_x')
                
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

        keyPrefix = self.cfg.get('status_log_local.key.science_core_x')
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


    def run_spark_model(self, country, logtype, date, hour):
        """Run Spark model to generate abnormal request_id"""
                
        logging.info("# Running Spark Modeling Command Line... ...")
        
        """Configurations of the Spark job"""
        queue = self.cfg.get('ard.default.queue')
        spark_path = self.cfg.get('spark.script.model')
        driver_memory = self.cfg.get('spark.default.driver_memory')
        packages = self.cfg.get('spark.default.databricks')
        
        executor_cores = self._get_cfg('spark.process.executor_cores', country)
        executor_memory = self._get_cfg('spark.process.executor_memory', country)
        num_executors = self._get_cfg('spark.process.num_executors', country)

        avro_path = self._get_science_core_avro_path(country, logtype, date, hour)
        abd_path = self._get_abd_path(country, logtype, date, hour)
        
        # SL
        if not self.NOSL:
            sl_entries = self.findSLEntries(country, date, hour)
            sl_centroid_path = self.getSLCombinedHourlyLLPaths(country, sl_entries)
            sl_ip_path = self.getSLCombinedHourlyIPPaths(country, sl_entries)
        else:
            sl_centroid_path = ""
            sl_ip_path = ""


        """Command to run Spark, abnormal request detection model is built in Spark"""
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
        if self.CONFIG:
            cmd += ["--config", self.CONFIG]
        if self.CONFIG_DIRS:
            cmd += ["--config_dirs \"{}\"".format(self.CONFIG_DIRS)]
        cmd += ["--country", country]
        cmd += ["--logtype", logtype]
        cmd += ["--date", date]
        cmd += ["--hour", hour]
        cmd += ["--input_dir", avro_path]
        cmd += ["--output_dir", abd_path]
        if sl_centroid_path:
            cmd += ["--sl_centroid_path \"{}\"".format(sl_centroid_path)]
        if sl_ip_path:
            cmd += ["--sl_ip_path \"{}\"".format(sl_ip_path)]
        
        if (self.DEBUG):
            cmd += ["--debug"]
        if (self.NORUN or self.NOMODEL):
            cmd += ["--norun"]
        
        cmdStr = " ".join(cmd)
        system.execute(cmdStr, self.NORUN)


    def run_spark_join(self, country, logtype, date, hour, avro_subparts, abd_subparts):
        """Run Spark command to generate science_core_ex"""

        logging.info("# Running Spark Join Command Line... ...")
        
        # Configurations of the Spark job
        queue = self.cfg.get('ard.default.queue')
        spark_path = self.cfg.get('spark.script.join')
        driver_memory = self.cfg.get('spark.default.driver_memory')
        packages = self.cfg.get('spark.default.databricks')
        
        # FIXME: use configuration to control memory
        executor_cores = self._get_cfg('spark.join.executor_cores', country)
        executor_memory = self._get_cfg('spark.join.executor_memory', country)
        num_executors = self._get_cfg('spark.join.num_executors', country)

        avr_partition_str = ','.join(avro_subparts)
        abd_partition_str = ','.join(abd_subparts)
        
        input_dir = self._get_science_core_avro_path(country, logtype, date, hour)
        abd_dir = self._get_abd_path(country, logtype, date, hour)
        tmp_dir = self._get_tmp_path(country, logtype, date, hour)

        # Command to run Spark, abnormal request detection model is built in Spark
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
        cmd += ["--config", self.CONFIG]
        cmd += ["--config_dirs", self.CONFIG_DIRS]
        cmd += ["--country", country]
        cmd += ["--logtype", logtype]
        cmd += ["--date", date]
        cmd += ["--hour", hour]      
        cmd += ["--avro_partitions", avr_partition_str]
        cmd += ["--abd_partitions", abd_partition_str]
        cmd += ["--input_dir", input_dir]
        cmd += ["--abd_dir", abd_dir]
        cmd += ["--output_dir", tmp_dir]
        if (self.DEBUG):
            cmd += ["--debug"]
        if (self.NORUN or self.NOJOIN):
            cmd += ["--norun"]

        cmdStr = " ".join(cmd)
        system.execute(cmdStr, self.NORUN)

 
    def run_spark_orc(self,country,logtype,date,hour,avro_subparts):
        """Run Spark model to generate abnormal request_id"""
        logging.info("# Running Spark AVRO TO ORC - {} {} {} {} ...".format(
            country, logtype, date, hour))     
        logging.info(" - avro partitions =", avro_subparts)
        
        """Configurations of the Spark job"""
        queue = self.cfg.get('ard.default.queue')
        spark_path = self.cfg.get('spark.script.orc')
        driver_memory = self.cfg.get('spark.default.driver_memory')
        packages = self.cfg.get('spark.default.databricks')
        
        executor_cores = self._get_cfg('spark.orc.executor_cores', country)
        executor_memory = self._get_cfg('spark.orc.executor_memory', country)
        num_executors = self._get_cfg('spark.orc.num_executors', country)

        input_dir = self._get_science_core_avro_path(country, logtype, date, hour)
        tmp_dir = self._get_tmp_path(country, logtype, date, hour)

        """Command to run Spark, abnormal request detection model is built in Spark"""
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
        cmd += ["--config", self.CONFIG]
        cmd += ["--config_dirs", self.CONFIG_DIRS]
        cmd += ["--country", country]
        cmd += ["--logtype", logtype]
        cmd += ["--date", date]
        cmd += ["--hour", hour]    
        cmd += ["--avro_partitions", ','.join(avro_subparts)]
        cmd += ["--input_dir", input_dir]
        cmd += ["--output_dir", tmp_dir]
        if (self.DEBUG):
            cmd += ["--debug"]
        if (self.NORUN):
            cmd += ["--norun"]

        cmdStr = " ".join(cmd)
        system.execute(cmdStr, self.NORUN)



    #-------------------
    # Helper Functions
    #-------------------

        
    def mvHDFS(self, country, logtype, date, hour, partial=False):
        """Move completed data from tmp folder to target location.
        
        If partial is True, this will be a partial replacement.
        It will only update partitions in the tmp folders.
        Other existing partitions will not be affected.
        This feature is introduced to fix the "rest" folder only.
        """

        tmp_path = self._get_tmp_path(country, logtype, date, hour)
        output_dir = self._get_science_core_orc_path(country, logtype, date)
        output_path = os.path.join(output_dir, hour)

        if partial:
            subparts = self.getSubHourPartitions(tmp_path)
            for p in subparts:
                fill = p[0]
                sl = p[1]
                src = os.path.join(tmp_path, fill, sl)
                dest = os.path.join(output_path, fill, sl)
                dest_parent = os.path.join(output_path, fill)
                if not hdfs.has(dest_parent):
                    hdfs.mkdirp(dest_parent, self.NORUN)
                hdfs.mv(src, dest, self.NORUN)
            hdfs.rmrs(tmp_path)

        else:
            # Remove old output
            if (hdfs.has(output_path)):
                hdfs.rmrs(output_path)
    
            # Move tmp folder to the destination directory
            hdfs.mkdirp(output_dir, self.NORUN)
            hdfs.mv(tmp_path, output_path, self.NORUN)


    def _get_cfg(self, baseKey, country):
        """A helper function to get country-specific configuration if
        it is available.   Otherwise, get the default one"""
        altKey = baseKey
        countryKey = baseKey + "." + country
        return self.cfg.get(countryKey, altKey)
