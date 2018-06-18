# -*- coding: utf-8 -*-
"""
Copyright (C) 2018.  GroundTruth.  All Rights Reserved.

"""

import logging
import os

from base_dt import BaseDT
from xad.common import hdfs
from xad.common import system

# ABD_MAP = {'fill=FILLED':'fill', 'fill=NOT_FILLED':'nf',
#            'loc_score=95':'tll', 'loc_score=94':'pos'}


class DwellTimeMain(BaseDT):
    """A class for implementing module functions for hql and spark."""

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseDT.__init__(self, cfg, opt)
        self.status_log = self.STATUS_L


    def run_spark_cmd(self, country, date):
        """Constructing Spark command"""
                
        logging.info("# Running Spark Command Line... ...")
        
        """Configurations of the Spark job"""
        queue = self.cfg.get('dwell_time.default.queue')
        spark_path = self.cfg.get('spark.script.model')
        driver_memory = self.cfg.get('spark.default.driver_memory')
        executor_cores = self._get_cfg('spark.process.executor_cores', country)
        executor_memory = self._get_cfg('spark.process.executor_memory', country)
        num_executors = self._get_cfg('spark.process.num_executors', country)
        input_dir = self.cfg.get('spark.input.dir')
        output_dir = self.cfg.get('spark.output.dir')

        input_dir += country + '/' + date
        output_dir += country + '/' + date

        """ Constructing spark command """
        cmd = ["SPARK_MAJOR_VERSION=2"]
        cmd += ["spark-submit"]
        cmd += ["--master", "yarn"]
        cmd += ["--queue", queue ]
        cmd += ["--conf", "spark.yarn.executor.memoryOverhead=3000"]
        cmd += ["--driver-memory", driver_memory]
        cmd += ["--executor-memory", executor_memory]
        cmd += ["--num-executors", num_executors]
        cmd += ["--executor-cores", executor_cores]
        cmd += [spark_path]
        if self.CONFIG:
            cmd += ["--config", self.CONFIG]
        if self.CONFIG_DIRS:
            cmd += ["--config_dirs \"{}\"".format(self.CONFIG_DIRS)]
        cmd += ["--country", country]
        cmd += ["--date", date]
        cmd += ["--input_dir", input_dir]
        cmd += ["--output_dir", output_dir]
        
        if (self.DEBUG):
            cmd += ["--debug"]
        if (self.NORUN or self.NOMODEL):
            cmd += ["--norun"]
        
        cmdStr = " ".join(cmd)
        logging.info("Spark command: {}".format(cmdStr))
        #system.execute(cmdStr, self.NORUN)


    def run_spark_join(self, country, logtype, date, hour, avro_subparts, abd_subparts):
        """Run Spark command to generate science_core_ex"""

        logging.info("# Running Spark Join Command Line... ...")
        
        # Configurations of the Spark job
        queue = self.cfg.get('dwell_time.default.queue')
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
        queue = self.cfg.get('dwell_time.default.queue')
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


    ### -------------------------
    ### New Code
    ### -------------------------

    def processOne(self, daily=False):
        logging.info("Processing pipeline, module: 1 for (daily={})".format(daily))

        dates = self.getDates('dwell_time.process.window', 'yyyy/MM/dd')
        regions = self.getRegions()
        countries = self.getCountries()

        logging.info("- dates = {}".format(dates))
        logging.info("- countries = {}".format(countries))
        logging.info("- regions = {}".format(regions))

        logCounter = 0
        processed_logtypes = []
        for date in dates:
            for region in regions:
                # Split region
                (country, logtype) = self.splitRegion(region)

                # Status keys
                orc_daily_key = self.get_orc_status_key(country, logtype, True)
                orc_status = self.status_log.getStatus(orc_daily_key, date)
                if (not self.FORCE and orc_status is not None and orc_status == 1):
                    logging.info("Status for ORC {} found for date {}".format(orc_daily_key, date))
                    logCounter += 1
                    processed_logtypes.append(region)

        logging.info("Logtype count for country: {} is {}".format(country, len(regions)))
        logging.info("Logtype count with processed ORC Status: {}".format(logCounter))

        if (len(regions) == logCounter):
            logging.info("All logtypes processed, Starting Module One...")
            for date in dates:
                processOne_daily_key = self.get_processOne_status_key(country, True)

                if (daily):
                    loc_path = self.cfg.get('spark.input.dir')
                    tmp_path = loc_path + country + '/' + date
                    self._sub_processOne(country, date, tmp_path)
                    if hdfs.has(tmp_path):
                        self.status_log.addStatus(processOne_daily_key, date)
                    else:
                        logging.info("Check the HQL executed, tmp path missing for date: {}".format(date))
        else:
            logging.info("All logtypes aren't processed. Present are: {}".format(processed_logtypes))

    def _sub_processOne(self, country, date, tmp_path):
        print ("<<<<< HQL processing for date: {}, country:{} >>>>>".format(date, country))
        self.run_hql_cmd(country, date, tmp_path)

    def processTwo(self, daily=False):
        logging.info("Processing pipeline, module:2 for (daily={})".format(daily))

        dates = self.getDates('dwell_time.process.window', 'yyyy/MM/dd')
        countries = self.getCountries()
        logging.info("- dates = {}".format(dates))
        logging.info("- countries = {}".format(countries))
        output_path = self.cfg.get('spark.output.dir')

        for date in dates:
            for country in countries:

                # Status keys
                processTwo_daily_key = self.get_processTwo_status_key(country, True)
                processOne_daily_key = self.get_processOne_status_key(country, True)
                processOne_status = self.status_log.getStatus(processOne_daily_key, date)

                if (not self.FORCE and processOne_status is not None and processOne_status == 1):
                    logging.info("Status for processOne {} found for date {}".format(processOne_status, date))

                    if (daily):
                        self._sub_processTwo(country, date)
                        if hdfs.has(output_path):
                            self.status_log.addStatus(processTwo_daily_key, date)
                        else:
                            logging.info("Output dir: {} not present after spark run".format(output_path))

    def _sub_processTwo(self, country, date):
        logging.info("<<<<< Processing for date: {}, country:{} >>>>>".format(date, country))
        self.run_spark_cmd(country, date)


    def _get_cfg(self, baseKey, country):
        """A helper function to get country-specific configuration if
        it is available.   Otherwise, get the default one"""
        altKey = baseKey
        countryKey = baseKey + "." + country
        return self.cfg.get(countryKey, altKey)
