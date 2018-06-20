#
# Copyright (C) 2018. GroundTruth.  All Rights Reserved.
#

"""
Base class for dwell time module.

@author: nachappa.ap

"""

import sys
sys.path.append('/home/xad/share/python')

import logging
import re
import subprocess
import sys
import os
from datetime import datetime
from datetime import timedelta

from xad.common import dateutil
from xad.common import hourutil
from xad.common import hdfs
from xad.common.conf import Conf
from xad.common.optioncontainer import OptionContainer
from xad.common.statuslog import StatusLog
from xad.common import system


class DwellTimeBase(OptionContainer):
    """Base dwell_time class.

    Thiis class has the common attributes and functions
    shared by other dwell_time modules.
    """

    def __init__(self, cfg, opt=None):
        """Constructor"""
        # Merge options with the self
        OptionContainer.__init__(self, opt)
        # Keep a reference to options and the configuration object
        self.opt = opt
        self.cfg = cfg
        self.STATUS_L = StatusLog(cfg, prefix='status_log_local')
        self.FILL = None
        self.SL = None

    def execute(self, cmd):
        """Execute a shell command and print output to STDOUT.
        Returns the process output"""
        output = None
        logging.debug("EXE> {}".format(cmd))
        if (not self.NORUN):
            process = subprocess.Popen(cmd, shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            # Poll process for new output until finished
            while True:
                nextline = process.stdout.readline()
                if nextline == '' and process.poll() is not None:
                    break
                sys.stdout.write(nextline)
                sys.stdout.flush()

            output = process.communicate()[0]
            exitCode = process.returncode

            if (exitCode != 0):
                raise Exception(cmd, exitCode, output)

        return output

    #------------------
    # Status Log Keys
    #------------------
    def get_orc_status_key(self, country, logtype, daily=False):
        """Get status_log key for ORC generation"""
        prefix = self.cfg.get('status_log_local.key.science_core_orc')
        tag = self.cfg.get('status_log_local.tag.daily') if daily else ""
        if (tag):
            key = os.path.join(prefix, country, logtype, tag)
        else:
            key = os.path.join(prefix, country, logtype)
        return key

    def get_dt_prepare_status_key(self, country, daily=False):
        """Get status_log key for process"""
        prefix = self.cfg.get('status_log_local.key.dwell_time_prepare')
        tag = self.cfg.get('status_log_local.tag.prepare.daily') if daily else \
            self.cfg.get('status_log_local.tag.prepare')
        if (tag):
            key = os.path.join(prefix, country, tag)
        else:
            key = os.path.join(prefix, country)
        return key

    def get_dt_gen_status_key(self, country, daily=False):
        """Get status_log key for module"""
        prefix = self.cfg.get('status_log_local.key.dwell_time_process')
        tag = self.cfg.get('status_log_local.tag.process.daily') if daily else \
            self.cfg.get('status_log_local.tag.process')
        if (tag):
            key = os.path.join(prefix, country, tag)
        else:
            key = os.path.join(prefix, country)
        return key


    def run_hql_cmd(self,country,date, output_path):

        """Run Hive command to add partitions into hive table"""
        logging.info("Running Hive Command Line......")
        queue = self.cfg.get('dwell_time.default.queue')
        hive_flavor = self.cfg.get('hive.cmdline.flavor')
        hql_path = self.cfg.get('hive.script.path')
        param_date = self.getDates(date,'yyyy-MM-dd')
        query_date = "".join(str(x) for x in param_date)
        logging.info("Temp path: {}".format(output_path))
        logging.info("Date being processed: {}".format(query_date))

        cmd = []
        if (hive_flavor == 'hive'):
            hive_exec = hive_flavor
            cmd = [hive_exec,'-hiveconf', 'output_path=' + output_path]
            cmd += ['-hiveconf', 'country=' + country]
            cmd += ['-hiveconf', 'query_date=' + query_date]
            cmd += ['-f', hql_path]
            command = ' '.join(cmd)
            logging.info("HQL Query Running.. >>> {}".format(command))
            system.execute(command, self.NORUN)

        elif (hive_flavor == 'beeline'):

            cmd = ["beeline"]
            cmd += ["-u", '"' + self.cfg.get('hiveserver.uri') + '"']
            cmd += ["--hiveconf", "tez.queue.name=" + queue]
            cmd += ["-hiveconf", "output_path=" + output_path]
            cmd += ["-hiveconf", 'country=' + country]
            cmd += ['-hiveconf', 'query_date=' + query_date]
            cmd += ["-n", os.environ['USER']]
            cmd += ["-f", hql_path]

            command = ' '.join(cmd)
            logging.info("Query to run: {}".format(command))
            system.execute(command, self.NORUN)


    #------------------
    # Main Partitions
    #------------------

    def getCountries(self, key='default.countries'):
        """Get a list of countries"""
        if (self.COUNTRY):
            retval = re.split("[,\s]+", self.COUNTRY)
        else:
            retval = self.cfg.get_array(key)
        #logging.debug("Countries = {}".format(retval))
        return retval

    def _get_country_logtypes(self, country, defaultKey='default.logtypes'):
        """Get logtypes associated with a country.  The user may override
           with the command line argument. However, only those that also appear
           in the configuration will be accepted.  Currently no warning for invalid
           logtypes."""
        # This is a country-specific logtype key.
        # If it exisits in the configuration, it will override the default values.
        # Otherwise, fall back to the default.
        countryKey = "{}.{}".format(defaultKey, country)
        valid_vals = self.cfg.get_array(countryKey, defaultKey)

        # Check command-line options.
        if (self.LOGTYPE):
            user_vals = re.split("[,\s]+", self.LOGTYPE)
            retval = list(set(user_vals).intersection(valid_vals))
        else:
            retval = valid_vals
        return retval


    def makeRegion(self, country, logtype, delim='-'):
        """Combine a country and a logtype into a 'region'"""
        return country + delim + logtype


    def splitRegion(self, region, regex='-'):
        """Split a region into a country and a logtype"""
        retval = re.split(regex, region);
        return retval


    def getRegions(self, countryKey='default.countries', logtypeKey='default.logtypes'):
        """Get a list of regions (country + logtype)"""
        countries = self.getCountries(countryKey)
        regions = list()
        for c in countries:
            # Get logtypes
            if (self.LOGTYPE):
                logtypes = re.split("[,\s]+", self.LOGTYPE)
            else:
                logtypes = self._get_country_logtypes(c, logtypeKey)
            for x in logtypes:
                # Make regions
                r = self.makeRegion(c, x)
                regions.append(r)
        #logging.debug("Regions = {}".format(regions))
        return regions


    def getDates(self, key, fmt='yyyy-MM-dd'):
        """Get the dates"""
        if (self.DATE):
            dateStr = self.DATE
        else:
            dateStr = self.cfg.get(key)
        return dateutil.expand(dateStr, fmt)


    def getHours(self, daily=False):
        """Get the hours"""
        if (self.HOUR and not daily):
            hourStr = self.HOUR
        else:
            hourStr = '00:23'
        return hourutil.expand(hourStr)

#-----------
# Unit Test
#-----------

def init_logging():
    """Initialize logging"""
    fmt = ("%(asctime)s %(levelname)s [%(module)s.%(funcName)s] %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt,
                        datefmt=datefmt,
                        level=logging.DEBUG)
    # IPython specific setting
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)


def load_config(conf_file="dwell_time.properties",
                conf_path="/home/xad/dwell_time/config:/home/xad/share/config",
                dump=False):
    """Load the configuration"""
    conf = Conf()
    conf.load(conf_file, conf_path)
    if (dump):
        conf.dump()
    return conf




