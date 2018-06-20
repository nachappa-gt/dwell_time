# -*- coding: utf-8 -*-
"""
Copyright (C) 2018.  GroundTruth.  All Rights Reserved.

"""

import logging
import os

from dwelltime_base import DwellTimeBase
from xad.common import hdfs
from xad.common import system


class DwellTimeMain(DwellTimeBase):
    """A class for implementing module functions for hql and spark."""

    def __init__(self, cfg, opt):
        """Constructor"""
        DwellTimeBase.__init__(self, cfg, opt)
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
        input_dir = self.cfg.get('data.hql.input.dir')
        output_dir = self.cfg.get('data.output.dir')

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
        # system.execute(cmdStr, self.NORUN)

    def prepare_dwelltime(self, daily=False):
        logging.info("Validating Processing pipeline... ")

        dates = self.getDates('dwell_time.process.window', 'yyyy/MM/dd')
        regions = self.getRegions()
        countries = self.getCountries()
        logtypeCount = len(regions)

        logging.info("- dates = {}".format(dates))
        logging.info("- countries = {}".format(countries))

        logCounter = 0
        for date in dates:
            for country in countries:
                # Get the logtypes associated with a country
                logTypes = self._get_country_logtypes(country)
                for logtype in logTypes:
                    # Get the status keys
                    orc_daily_key = self.get_orc_status_key(country, logtype, True)
                    orc_status = self.status_log.getStatus(orc_daily_key, date)
                    if (orc_status is not None and orc_status == 1):
                        logCounter += 1

                logging.info("Count of processed logtypes: {}".format(logCounter))
                if (self.FORCE and logtypeCount == logCounter):
                    self._run_prepare(date, country)

                elif (not self.FORCE):
                    prepare_daily_key = self.get_dt_prepare_status_key(country, True)
                    prepare_status = self.status_log.getStatus(prepare_daily_key, date)
                    if (prepare_status is None):
                        if (logtypeCount == logCounter):
                            logging.info("Proceeding with dwell time prepare module... ")
                            self._run_prepare(date, country)
                    else:
                        logging.info("Already processed, key found: {} ,skipped processing for date: {}".format(prepare_daily_key,date))

    def _run_prepare(self, date, country):
        logging.info("Starting Processing pipeline... ")

        prepare_daily_key = self.get_dt_prepare_status_key(country, True)
        loc_path = self.cfg.get('data.hql.output.dir')
        output_path = loc_path + country + '/' + date
        logging.info ("<<<<< HQL processing for date: {}, country:{} >>>>>".format(date, country))
        self.run_hql_cmd(country, date, output_path)
        if hdfs.has(output_path):
            self.status_log.addStatus(prepare_daily_key, date)
        else:
            logging.info("Check the HQL executed, tmp path missing for date: {}".format(date))


    def process_dwelltime(self, daily=False):
        logging.info("Processing pipeline, module:2 for (daily={})".format(daily))

        dates = self.getDates('dwell_time.process.window', 'yyyy/MM/dd')
        countries = self.getCountries()
        logging.info("- dates = {}".format(dates))
        logging.info("- countries = {}".format(countries))
        output_path = self.cfg.get('data.output.dir')

        for date in dates:
            for country in countries:

                # Status keys
                process_daily_key = self.get_dt_gen_status_key(country, True)
                prepare_daily_key = self.get_dt_prepare_status_key(country, True)
                prepare_status = self.status_log.getStatus(prepare_daily_key, date)

                if (not self.FORCE and prepare_status is not None and prepare_status == 1):
                    logging.info("Status for processOne {} found for date {}".format(prepare_status, date))

                    if (daily):
                        self._sub_processdt(country, date)
                        if hdfs.has(output_path):
                            self.status_log.addStatus(process_daily_key, date)
                        else:
                            logging.info("Output dir: {} not present after spark run".format(output_path))

                elif (self.FORCE):
                    self._sub_processdt(country, date)

    def _sub_processdt(self, country, date):
        logging.info("<<<<< Processing for date: {}, country:{} >>>>>".format(date, country))
        self.run_spark_cmd(country, date)


    def _get_cfg(self, baseKey, country):
        """A helper function to get country-specific configuration if
        it is available.   Otherwise, get the default one"""
        altKey = baseKey
        countryKey = baseKey + "." + country
        return self.cfg.get(countryKey, altKey)
