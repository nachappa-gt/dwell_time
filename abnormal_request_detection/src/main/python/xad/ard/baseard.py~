#
# Copyright (C) 2016. xAd, Inc.  All Rights Reserved.
#

"""
Base class for ard.

@author: xiangling
"""

#from xad.common.conf import Conf

import getpass
import logging
import re
import subprocess
import sys
import os

from xad.common import dateutil
from xad.common import hourutil
from xad.common import hdfs
from xad.common.optioncontainer import OptionContainer

class BaseArd(OptionContainer):
    """Base ard class.
    It has the common attributes and functions
    share by other ard modules.
    """

    def __init__(self, cfg, opt):
        """Constructor"""
        # Merge options with the self
        OptionContainer.__init__(self, opt)
        # Keep a reference to options and the configuration object
        self.opt = opt
        self.cfg = cfg


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


    def getHDFSUserTmpDir(self, app='ard', tmpRoot='/tmp'):
        """Get a user-specific tmp directory, e.g. /tmp/ard-xad."""
        appUserFolder = "-".join([app, getpass.getuser()])
        path = os.path.join(tmpRoot, appUserFolder)
        return(path)


    def sqoopImport(self, uri, user, passwd, tables, dataFmt, dest):
        """Import data via sqoop"""

        # Prepare the destination folder.   Existing contents will be removed
        if (hdfs.test(dest)):
            hdfs.rmrs(dest)
        hdfs.mkdirp(dest)

        # Set the driver
        if (uri.index('postgresql') >= 0):
            driver = 'org.postgresql.Driver'
        else:
            driver = 'com.mysql.jdbc.Driver'

        # Get the DB name
        m = re.search('/(\w+)$', uri)
        if (m):
            dbname = m.group(1)
        else:
            raise Exception("Cannot find dbname from URI {}".format(uri))

        logging.debug("- dest = {}".format(dest))
        logging.debug("- driver = {}".format(driver))
        logging.debug("- dbname = {}".format(dbname))

        # Import one table at a time.
        for table in tables:
            logging.debug("Downloading table '{}' ...".format(table))

            # Get Sqoop options
            optKey = ".".join(['sqoop.import.option', dbname, table])
            tableOpt = self.cfg.get(optKey, "")
            logging.debug("- table option = {}".format(tableOpt))

            # Call a helper function
            self.sqoopImportTable(uri, driver, user, passwd, table,
                                  dataFmt, dest, tableOpt);


    def sqoopImportTable(self, uri, driver, user, passwd, table, dataFmt,
                         dest, tableOpt):
        """Sqoop one table from the database"""

        connOpt = '?dontTrackOpenResources=true'
        connOpt += '\&defaultFetchSize=10000'
        connOpt += '\&useCursorFetch=true'

        # Build the command
        cmd = 'sqoop import'
        cmd += ' -D mapreduce.job.user.classpath.first=true'
        cmd += ' -D mapred.child.java.opts=" -Duser.timezone=GMT"'
        if (self.QUEUE):
            cmd += ' -D mapred.job.queue.name={}'.format(self.QUEUE)
        cmd += " --connect {}{}".format(uri, connOpt)
        cmd += " --driver {}".format(driver)
        cmd += " --username {}".format(user)
        cmd += " --password {}".format(passwd)
        cmd += " --table {}".format(table)

        if (self.MAXMAPS and not re.search('-m\s', tableOpt)):
            cmd += " -m {}".format(self.MAXMAPS)
        if (tableOpt):
            cmd += " " + tableOpt
        cmd += " --warehouse-dir {}".format(dest)

        # Data format
        if (dataFmt == 'avro'):
            cmd += " --as-avrodatafile"
        elif (dataFmt == 'parquet'):
            cmd += " --as-parquetfile"
        elif (dataFmt == 'text'):
            cmd += " --as-textfile"

        # Run it and check the return code
        self.execute(cmd)

        # Validation
        successPath = os.path.join(dest, table, '_SUCCESS')
        #if (not self.NORUN and not hdfs.test(successPath)):
        #    raise Exception("Failed importing {}".format(table))

    def _touch_local_status(

    def hdfsMove(self, src, dest):
        """Move a HDFS folder by checking that the destination folder exists"""
        parent = os.path.dirname(dest)
        if (not hdfs.test(parent)):
            hdfs.mkdirp(parent)
        hdfs.mv(src, dest)
    
    
    def _get_local_status(self,*args):
        
        dir = self.cfg.get('proj.status.dir')
        sub_dir = '/'.join(args)
        return '/'.join([dir, sub_dir])

    def _get_science_core_avro_path(self, *args)
        
        dir = self.cfg.get('hdfs.data.dir')
        sub_dir = '/'.join(args)
        return '/'.join([dir, sub_dir])


    def _get_config_array(self, confKey, optKey):
        """Get configuration values as an array.
           Override with command line arguments if they are available"""
        #if (self.opt.):

    def getCountries(self, key='default.countries'):
        """Get a list of countries"""
        if (self.COUNTRY):
            retval = re.split("[,\s]+", self.COUNTRY)
        else:
            retval = self.cfg.get_array(key)
        #logging.debug("Countries = {}".format(retval))
        return retval

    def _get_country_logtypes(self, country, logtypeKey='default.logtypes'):
        """Get logtypes associated with a country.  The user may override
           with the command line argument. However, only those that also appear
           in the configuration will be accepted.  Currently no warning for invalid
           logtypes."""
        # This is a country-specific logtype key.
        # If it exisits in the configuration, it will override the default values.
        # Otherwise, fall back to the default.
        key = "{}.{}".format(logtypeKey, country)
        valid_vals = self.cfg.get_array(key, logtypeKey)

        # Check command-line options.
        if (self.LOGTYPE):
            user_vals = re.split("[,\s]+", self.LOGTYPE)
            retval = list(set(user_vals).intersection(valid_vals))
        else:
            retval = valid_vals
        return retval

    def makeRegion(self, country, logtype, delim='_'):
        """Combine a country and a logtype into a 'region'"""
        return country + delim + logtype

    def splitRegion(self, region, regex='_'):
        """Split a region into a country and a logtype"""
        retval = re.split(regex, region); 
        return retval

    def getRegions(self, countryKey='default.countries', logtypeKey='default.logtypes'):
        """Get a list of regions (country + logtype)"""
        countries = self.getCountries(countryKey)
        regions = list()
        for c in countries:
            # Get logtypes
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
        
    def getHours(self):
        """Get the hours"""
        if (self.HOUR):
            hourStr = self.HOUR
        else:
            hourStr = '00:23'
        return hourutil.expand(hourStr)

    def getFills(self):
        """Get the fill/nf folders"""
        if (self.FILL):
            str = self.FILL
        else:
            str = self.cfg.get('science_core.fill.folders')
        return re.split('[,\s]+', str)

    def getSLLevels(self):
        """Get the SL tll/pos/rest folders"""
        if (self.SL):
            str = self.SL
        else:
            str = self.cfg.get('science_core.sl.folders')
        return re.split('[,\s]+', str)



