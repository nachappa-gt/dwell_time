# -*- coding: utf-8 -*-
"""
Copyright (C) 2016.  xAd, Inc.  All Rights Reserved.

@author: victor
"""

import logging
import os
import re

from basexcp import BaseXcp
from datetime import datetime

from xad.common import dateutil
from xad.common import hdfs
from xad.common import hourutil
from xad.common.pigcommand import PigCommand

SUCCESS_GEN = '_GEN_'

class ScienceCore(BaseXcp):
    """A class for converting Science Core files."""

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseXcp.__init__(self, cfg, opt)

    #-------------------------
    # Generate Parquet Files
    #-------------------------
    def genParquet(self):
        """Convert Science Core AVRO files into Parquet files."""
        logging.info('Generating Science Core Parquet files...')

        # Get parameters
        # enigma.dates is L3, which means the window is 3 days
        dates = self.getDates('enigma.dates', 'yyyy/MM/dd')
        hours = self.getHours()
        regions = self.getRegions()
        fills = self.getFills()
        sl_levels = self.getSLLevels()

        logging.info("- dates = {}".format(dates))
        logging.info("- hours = [{}]".format(",".join(hours)))
        logging.info("- regions = {}".format(regions))
        logging.info("- fills = {}".format(fills))
        logging.info("- sl levels = {}".format(sl_levels))

        # Looping through all combinations
        for date in dates:
            for region in regions:
                (country,logtype) = self.splitRegion(region)

                # Check daily status
                dailyStatus = self._get_local_status(country, logtype, date, SUCCESS_GEN)
                if (os.path.exists(dailyStatus)):
                    logging.debug("x SKIP: found {}".format(dailyStatus))
                    continue

                for hour in hours:
                    # Check hourly status
                    hourlyStatus = self._get_local_status(country, logtype, date, hour, SUCCESS_GEN)
                    if (os.path.exists(hourlyStatus)):
                        logging.debug("x SKIP: found {}".format(hourlyStatus))
                        continue

                    # Check HDFS Source
                    input = self._get_science_core_avro_path(country, logtype, date, hour)
                    success_path = os.path.join(input, "fill/tll/_SUCCESS")
                    if (not hdfs.has(success_path)):
                        logging.info("x SKIP: MISSING {}".format(success_path))
                        break

                    # Dig into sub folders
                    logging.info("# Checking {}_{} {}:{}...".format(country, logtype, date, hour))
                    for fill in fills:
                        for sl in sl_levels:
                            self._gen_helper(country,logtype,date,hour,fill,sl)

                    # Touch hourly status
                    if (self._has_full_hour(hourlyStatus) and not self.NORUN):
                        self._touch_local_status(hourlyStatus)

                # Touch daily status
                if (self._has_full_day(dailyStatus) and not self.NORUN):
                    self._touch_local_status(dailyStatus)


    def _gen_helper(self, country, logtype, date, hour, fill, sl):
        """Helper function for generating Parquet files"""

        logging.debug("+ Subfolder {}/{}".format(fill, sl))

        # Check local status log
        statusPath = self._get_local_status(country, logtype, date, hour, fill, sl)
        if (os.path.exists(statusPath)):
            logging.debug("  x SKIP: found status {}".format(statusPath))
            return True

        # Other parameters
        output = self._get_science_core_parquet_path(country, logtype, date, hour, fill, sl)
        tmp = self._get_hdfs_tmp(output)
        if hdfs.has(os.path.join(output, "_SUCCESS")):
            if not self.FORCE:
                logging.debug("  x SKIP: found output {}".format(statusPath))
                return True
            else:
                hdfs.rmrs(output, norun=self.NORUN)

        # Check HDFS source
        input = self._get_science_core_avro_path(country, logtype, date, hour, fill, sl)
        files = '*.avro'

        # Pig Script
        script = self.cfg.get('pig.script.science_core_parquet')
        pig = self.createPigCommand(script,
                params = {
                    'INPUT' : os.path.join(input, files),
                    'OUTPUT' : tmp
                },
                outputs = (tmp, output)
            )
        pig.run()

        # Create parent folder if it doesn't exist
        parent = os.path.dirname(output)
        if not hdfs.has(parent):
            hdfs.mkdirp(parent, norun=self.NORUN)

        # Move tmp to output
        hdfs.mv(tmp, output, norun=self.NORUN)

        if not self.NORUN:
            self._touch_local_status(statusPath)

        return True


    #-------------------
    # Pig
    #-------------------
    def createPigCommand(self, script, params={}, properties={}, outputs=()):
        """Generate a PigCommand object using specified and default
        project parameters."""
        # Properties
        if (self.QUEUE):
            properties['mapred.job.queue.name'] = self.QUEUE

        if (properties):
            tmp = self.cfg.get('proj.tmp.dir')
            property_file = os.path.join(tmp, 'pig.properties')
        else:
            property_file = None
        
        # Parameters
        params['AVRO_JAR'] = self.cfg.get('jar.avro')
        params['JSON_SIMPLE_JAR'] = self.cfg.get('jar.json-simple')
        params['PARQUET_PIG_BUNDLE_JAR'] = self.cfg.get('jar.parquet-pig-bundle')

        # Command object
        pig = PigCommand(
                         params = params,
                         property_file = property_file,
                         properties = properties,
                         script=script,
                         outputs = outputs,
                         logfile = self.cfg.get('proj.log.pig.dir'),
                         norun = self.NORUN
                        )
        return pig

    #-------------------
    # Helper Functions
    #-------------------
    def _get_local_status(self, *args):
        """Get the path to the local status log"""
        base_dir = self.cfg.get('science_core.parquet.status.dir')
        return os.path.join(base_dir, *args)

    def _touch_local_status(self, path, isDir=False):
        """Touch the local status.  The specified path can represent
           either a directory or a file."""

        logging.debug("Touch status {}".format(path))

        if (isDir):
            os.makedirs(path)
        else:
            parent = os.path.dirname(path)
            if (not os.path.isdir(parent)):
                os.makedirs(parent)
            if (not os.path.exists(path)):
                open(path, 'w').close() # touch

    def _has_full_hour(self, hourlyStatus):
        """Has created all subfolders within the hour"""
        retval = True
        dir = os.path.dirname(hourlyStatus)
        fills = self.cfg.get_array('science_core.fill.folders')
        sls = self.cfg.get_array('science_core.sl.folders')
        for fill in fills:
            for sl in sls:
                path = os.path.join(dir, fill, sl)
                if not os.path.exists(path):
                    retval = False
                    break
        return retval

    def _has_full_day(self, dailyStatus):
        """Has created all subfolders within the hour"""
        retval = True
        dir = os.path.dirname(dailyStatus)
        hours = hourutil.expand('0:23')
        for hour in hours:
            path = os.path.join(dir, hour, SUCCESS_GEN)
            if not os.path.exists(path):
                retval = False
                break
        return retval

    def _get_science_core_avro_path(self, country, logtype, *entries):
        """Get path to the AVRO-based science foundation files"""
        base_dir = self.cfg.get('extract.data.prefix.hdfs')
        return os.path.join(base_dir, country, logtype, *entries)

    def _get_science_core_parquet_path(self, country, logtype, *entries):
        """Get path to the AVRO-based science foundation files"""
        base_dir = self.cfg.get('science_core.parquet.data.prefix.hdfs')
        return os.path.join(base_dir, country, logtype, *entries)

    def _get_hdfs_tmp(self, *args):
        """Get a temporary working directory."""
        tmpDir = self.getHDFSUserTmpDir()
        folder = '_'.join(args)
        folder = re.sub('^\/', '', folder)    # remove leading '/'
        folder = re.sub('[\/]', '_', folder)  # replace '/' with '_'
        path = os.path.join(tmpDir, 'WIP_' + folder)
        return(path)


#--- end ---

