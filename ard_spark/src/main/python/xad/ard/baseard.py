#
# Copyright (C) 2016,2017. xAd, Inc.  All Rights Reserved.
#

"""
Base class for ARD.

@author: xiangling
@aurhor: victor

"""

import sys
sys.path.append('/home/xad/share/python')

import getpass
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


class BaseArd(OptionContainer):
    """Base ard class.

    Thiis class has the common attributes and functions
    shared by other ard modules.
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

    #---------
    # Path
    #---------
    def getHDFSUserTmpDir(self, app='ard', tmpRoot='/tmp'):
        """Get a user-specific tmp directory, e.g. /tmp/ard-xad."""
        appUserFolder = "-".join([app, getpass.getuser()])
        path = os.path.join(tmpRoot, appUserFolder)
        return(path)


    def _get_science_core_avro_path(self, country, logtype, *entries):
        base_dir = self.cfg.get('hdfs.data.extract')
        return os.path.join(base_dir, country, logtype, *entries)


    def _get_science_core_orc_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('hdfs.data.orc')
        return os.path.join(base_dir, country, logtype, *entries)

    def _get_science_core_orc_s3_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('s3.data.science_core_ex')
        return os.path.join(base_dir, country, logtype, *entries)

    def _get_science_core_orc_s3n_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('s3n.data.science_core_ex')
        return os.path.join(base_dir, country, logtype, *entries)

    def _get_abd_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('hdfs.prod.abd')
        return os.path.join(base_dir, country, logtype, *entries)


    def _get_tmp_path(self, country, logtype, *entries):
        base_dir = '/tmp/ard'
        folder = os.path.join(country, logtype, *entries).replace("/", "_")
        return os.path.join(base_dir, "WIP_" + folder)


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

    def get_s3put_status_key(self, country, logtype, daily=False):
        """Get status_log key for S3 Put"""
        prefix = self.cfg.get('status_log_local.key.science_core_s3put')
        tag = self.cfg.get('status_log_local.tag.s3put.daily') if daily else\
            self.cfg.get('status_log_local.tag.s3put')
        if (tag):
            key = os.path.join(prefix, country, logtype, tag)
        else:
            key = os.path.join(prefix, country, logtype)
        return key

    def get_s3hive_status_key(self, country, logtype, daily=False):
        """Get status_log key for adding a S3 Partition in Hive"""
        prefix = self.cfg.get('status_log_local.key.science_core_s3hive')
        tag = self.cfg.get('status_log_local.tag.s3hive.daily') if daily else\
            self.cfg.get('status_log_local.tag.s3hive')
        if (tag):
            key = os.path.join(prefix, country, logtype, tag)
        else:
            key = os.path.join(prefix, country, logtype)
        return key

    ## New Code

    def get_processOne_status_key(self, country, logtype, daily=False):
        """Get status_log key for S3 Put"""
        prefix = self.cfg.get('status_log_local.key.dwell_time_processOne')
        tag = self.cfg.get('status_log_local.tag.processOne.daily') if daily else \
            self.cfg.get('status_log_local.tag.processOne')
        if (tag):
            key = os.path.join(prefix, country, logtype, tag)
        else:
            key = os.path.join(prefix, country, logtype)
        return key

    def get_processTwo_status_key(self, country, logtype, daily=False):
        """Get status_log key for S3 Put"""
        prefix = self.cfg.get('status_log_local.key.dwell_time_processTwo')
        tag = self.cfg.get('status_log_local.tag.processTwo.daily') if daily else \
            self.cfg.get('status_log_local.tag.processTwo')
        if (tag):
            key = os.path.join(prefix, country, logtype, tag)
        else:
            key = os.path.join(prefix, country, logtype)
        return key





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


    #------------------
    # Sub-Partitions
    #------------------

    def getFillFolders(self):
        """Get the fill/nf folders.  Exclude fill if hasFill is false"""
        if (self.FILL):
            str = self.FILL
        else:
            str = self.cfg.get('science_core.fill.folders')
        return re.split('[,\s]+', str)

    def getNoFillFolders(self):
        """Get the fill/nf folders.  Exclude fill if hasFill is false"""
        str = self.cfg.get('science_core.nf.folders')
        return re.split('[,\s]+', str)

    def getSLFolders(self):
        """Get the SL tll/pos/rest folders"""
        if (self.SL):
            str = self.SL
        else:
            str = self.cfg.get('science_core.sl.folders')
        return re.split('[,\s]+', str)


    def getSubHourPartitions(self, hour_path, delim=None, mapper=None, empty=False):
        """Get a list of [fill, loc_score] folders under specified path

        Find all available fill/loc_score sub-folder combinations under
        the specified hour path.   The fill folder may have "fill" and "nf".
        The loc_score folder may have "tll", "pos", and "rest".
        It use hdfs ls functions to check the availability of these folders.
        It does not check the size of the folders.

        Arg:
           hour_path Path to an hour folder. E.g.,
               "/data/science_core_ex/us/exchange/2017/05/01/00"
           delim: partition delimiter.  If Nont, output will be
               a list of lists.  Otherwise, it will be a list of strings.
           mapper: for folder name conversion.
           empty: if True, will accept empty folders (of size=0). Default False.

        Return: a list of [fill,loc_score] pair.

        Sample output:
           [['fill', 'pos'], ['fill', 'rest'], ['fill', 'tll'],
            ['nf', 'pos'], ['nf', 'rest'], ['nf', 'tll']]
        Sample output with delim = '-'
           ['fill-pos', fill-rest', fill-tll', 'nf-pos', 'nf-rest', nf-tll']
        """
        subparts = []

        outputs = hdfs.du(hour_path + "/*")
        fill_folders = set(self.getFillFolders())
        sl_folders = set(self.getSLFolders())

        # Now collect fill and loc_score entries
        for p in outputs:
            folder_size = p[0]
            entries = p[1].split('/');
            if (not empty and folder_size == 0):
                continue

            if (len(entries) > 2):
               fill = entries[-2]
               loc_score = entries[-1]

               # Apply mapping
               if (mapper):
                   if fill in mapper:
                       fill = mapper[fill]
                   if loc_score in mapper:
                       loc_score = mapper[loc_score]

               if (fill in fill_folders and loc_score in sl_folders):
                   pair = [fill,loc_score]
                   subparts.append( delim.join(pair) if delim else pair)

        return subparts


    def makeFullSubHourPartitions(self, hasFill=True, delim=None):
        """Create a complet sub partition list"""

        fill_folders = self.getFillFolders() if hasFill else self.getNoFillFolders()
        sl_folders = self.getSLFolders()
        return [ delim.join([x,y]) if delim else [x,y]
            for x in sorted(fill_folders)
            for y in sorted(sl_folders)]


    def makeAddPartitionQuery(self, table_name, cntry, prod_type, dt, hour,
                              subparts, locations):
        """Create a Hive query for adding partitions

        Create a Hive query for adding partitions.
        Because subparts is a list structure, the query may
        add multiple partitions at multiple locations.

        Args:
        - cntry: country
        - prod_type: product type
        - dt: date, in the format of yyyy-MM-dd
        - hour: in the format of 'hh'
        - subparts: a list of [fill,loc_score] pairs.

        TODO: Generalized and put in HiveCommand module.
        """

        spec = self.makePartitionSpec(cntry, prod_type, dt, hour,
                                      subparts, locations)
        query = "ALTER TABLE {} ADD IF NOT EXISTS\n  {};".format(table_name, spec)
        return query;


    def makeDropPartitionQuery(self, table_name, cntry, prod_type, dt, hour):
        """Create a Hive query for dropping partitions

        Create a Hive query for dropping partitions.
        Because subparts is a list structure, the query may
        drop multiple partitions.

        Args:
        - cntry: country
        - prod_type: product type
        - dt: date, in the format of yyyy-MM-dd
        - hour: in the format of 'hh'
        """

        spec = self.makePartitionSpec(cntry, prod_type, dt, hour)
        query = "ALTER TABLE {} DROP IF EXISTS\n  {};".format(table_name, spec)
        return query;


    def makePartitionSpec(self, cntry, prod_type, dt, hour, subparts=None,
                          locations=None):
        """Generate the partition spec

        Note that the it may be involved with multiple partitions
        delimited by commas"""

        dt = dateutil.convert(dt, 'yyyy-MM-dd')
        num_parts = len(subparts) if (subparts) else 0
        has_locations = False;
        if (locations is not None):
            if (len(locations) == num_parts):
                has_locations = True
            else:
                #logging.error("Locations does not match subparts")
                pass

        spec = ""
        if (num_parts == 0):
            # Don't need to include sub-parts for dropping
            spec += "PARTITION (cntry='{}', ".format(cntry)
            spec += "prod_type='{}', ".format(prod_type)
            spec += "dt='{}', hour='{}')".format(dt, hour)
        else:
            for i in range(num_parts):
                # Handle partition spec delimiters
                if (i > 0):
                    spec += "\n  " if has_locations else ",\n  "

                spec += "PARTITION (cntry='{}', ".format(cntry)
                spec += "prod_type='{}', ".format(prod_type)
                spec += "dt='{}', hour='{}', ".format(dt, hour)

                # Sub parts
                p = subparts[i]
                spec += "fill='{}', loc_score='{}')".format(p[0], p[1])

                # Handle locations
                if (has_locations):
                    loc = locations[i]
                    spec += "\n    LOCATION '{}'".format(loc)

        return spec;


    def runHiveQuery(self, query):
	#default to version 2
        version = self.cfg.get('hive.version') if self.cfg.get('hive.version') else '2'
	if version == '1':
		self.runHiveV1Query(query)
	else:
		self.runHiveV2Query(query)



    def runHiveV2Query(self, query):
        """Run the specified Hive query"""
        beeline = self.cfg.get('hive.command')
        uri = self.cfg.get('hiveserver.uri')
        queue = self.QUEUE if self.QUEUE else self.cfg.get('ard.default.queue')
        user = os.environ['USER']

        cmd = beeline
        cmd += " -u \"{}\"".format(uri)
        cmd += " --hiveconf tez.queue.name={}".format(queue)
        cmd += " -n {}".format(user)
        cmd += " -e \"{}\"".format(query)
        system.execute(cmd, self.NORUN)

    def runHiveV1Query(self, query):
        """Run the specified Hive query"""
        beeline = self.cfg.get('hive.command')
        queue = self.QUEUE if self.QUEUE else self.cfg.get('ard.default.queue')

        cmd = beeline
        cmd += " --hiveconf tez.queue.name={}".format(queue)
        cmd += " -e \"{}\"".format(query)
        system.execute(cmd, self.NORUN)

    def addHivePartitions(self, country, logtype, date,
                          hour, subparts, hourly_path):
        """Add Hive partitions"""

        # Drop existing partitions
        table_name =  self.cfg.get('ard.output.table');
        query = self.makeDropPartitionQuery(table_name, country, logtype,
                                            date, hour)
        # Add new partitions
        if len(subparts) > 0:
            loc = [os.path.join(hourly_path, p[0], p[1]) for p in subparts]
            addQuery = self.makeAddPartitionQuery(table_name, country, logtype,
                                                  date, hour, subparts, loc)
            query += "\n" + addQuery

        self.runHiveQuery(query)


    def addMissingHDFSPartitions(self, hour_path, hasFill=None, missing_subparts=None):
        """Padding empty partitions with empty ORC files

        Some partitions (like nf/pos) may be missing if the source
        is empty.  This function will create the partition folder
        and fill it with _SUCCESS and empty.orc.
        """
        empty_folder = self.cfg.get('hdfs.prod.empty.folder')

        if hasFill is None:
            hasFill = False if (hour_path.find('display_dr') >= 0) else True

        if not missing_subparts:
            missing_subparts = self.findMissingSubHourPartitions(hour_path, hasFill)

        for part in missing_subparts:
                fill, loc_score = part
                fill_path = os.path.join(hour_path, fill)
                loc_score_path = os.path.join(fill_path, loc_score)

                # Folder prepartion
                if hdfs.has(loc_score_path):
                    hdfs.rmrs(loc_score_path)
                elif not hdfs.has(fill_path):
                    hdfs.mkdirp(fill_path, self.NORUN)

                logging.info("Padding empty folder {}".format(loc_score_path))
                hdfs.cp(empty_folder, loc_score_path, self.NORUN)

        if (len(missing_subparts) > 0):
            logging.info("Fixed HDFS Partitions: {}".format(missing_subparts))
        return missing_subparts;


    def findMissingSubHourPartitions(self, hour_path, hasFill=None, empty=False):
        """Find missing sub-partitions under an hourly path"""

        delim = '-'
        avail_set = set(self.getSubHourPartitions(hour_path, delim, empty));

        if hasFill is None:
            hasFill = False if (hour_path.find('display_dr') >= 0) else True
        full_set = set(self.makeFullSubHourPartitions(hasFill, delim=delim))

        missing_set = full_set - avail_set

        missing_list = [ x.split(delim) for x in missing_set ]
        return missing_list


    def findMissingPartitions(self, day_path, hasFill=None):
        """Identify hours that have missing/empty partitions

        Identify all of the missing parts with one hdfs du call.
        Returns [ [hour, missint-partition-list]]
        """

        # Use du to find out the folders and sizes
        du_outputs = hdfs.du(day_path + "/*/*")
        fill_folders = set(self.getFillFolders())
        sl_folders = set(self.getSLFolders())

        # Re-organize the data into t dictionary
        valid_dict = dict();
        delim = '-'
        for x in du_outputs:
            folder_size, path = x
            entries = path.split("/")

            if (folder_size == 0):
                continue
            if (entries < 3):
                continue

            # Get hour and partitions
            hour = entries[-3]
            fill = entries[-2]
            loc_score = entries[-1]

            if (fill in fill_folders and loc_score in sl_folders):
                pair = delim.join([fill,loc_score])
                if (hour in valid_dict):
                    valid_set = valid_dict[hour]
                    valid_set.add(pair)
                else:
                    valid_dict[hour] = set([pair])

        # Use path to set the hasFill flag
        if hasFill is None:
            hasFill = False if (day_path.find('display_dr') >= 0) else True

        full_set = set(self.makeFullSubHourPartitions(hasFill, delim=delim))
        full_size = len(full_set)

        missing_parts = []
        for hour in sorted(valid_dict.keys()):
            avail_set = valid_dict[hour]
            if (len(avail_set) == full_size):
                continue
            missing_set = full_set - avail_set
            missing_list = [ x.split(delim) for x in missing_set ]
            missing_parts.append( [hour, missing_list] )

        return missing_parts


    #-------------
    # SL Related
    #-------------
    def getSLHourlyLLPath(self, country, *entries):
        base_dir = self.cfg.get('sl.output.dir')
        folder = self.cfg.get('sl.hourly.ll.folder')
        return os.path.join(base_dir, country, folder, *entries)


    def getSLHourlyIPPath(self, country, *entries):
        base_dir = self.cfg.get('sl.output.dir')
        folder = self.cfg.get('sl.hourly.ip.folder')
        return os.path.join(base_dir, country, folder, *entries)

    def getSLCombinedHourlyLLPaths(self, country, entryList):
        path = ""
        if (len(entryList) > 0):
            entryStr = "{{{}}}".format(",".join(entryList))
            base = self.getSLHourlyLLPath(country)
            path = os.path.join(base, entryStr)
        return path

    def getSLCombinedHourlyIPPaths(self, country, entryList):
        path = ""
        if (len(entryList) > 0):
            entryStr = "{{{}}}".format(",".join(entryList))
            base = self.getSLHourlyIPPath(country)
            path = os.path.join(base, entryStr)
        return path

    def getSLHourlyStatusLogKey(self, country):
        slKeyPrefix = self.cfg.get('status_log_local.key.sl.centroid')
        slHourlyTag = self.cfg.get('status_log_local.tag.sl.hourly')
        return os.path.join(slKeyPrefix, country, slHourlyTag)


    def getSLDelayHours(self, country, date, hour):
        """Get the delay in hours"""
        retval = None
        status_key = self.getSLHourlyStatusLogKey(country)
        statusTime = self.STATUS_L.getTimestamp(status_key, date+"/"+hour)

        if (statusTime):
            dataTime = datetime.strptime(date + " " + hour, "%Y/%m/%d %H")
            delta = statusTime - dataTime
            retval = delta.days * 24 + int(delta.seconds/3600.0)
            if (retval < 0):
                retval = 0

        return retval


    def findSLEntries(self, country, date, hour):
        """Find available SL entries (date/hour)

        This method will search available SL entries starting from
        the specified date/hour.   It will calculate the delay in hours
        and try to get that many entries, subjected to limitation.
        Return a list of date time in "yyyy/MM/dd/hh".
        """
        minDelay = int(self.cfg.get("sl.hourly.delay.min"))
        maxDelay = int(self.cfg.get("sl.hourly.delay.max"))
        delay = self.getSLDelayHours(country, date, hour)
        if (delay < minDelay):
            delay = minDelay
        if (delay > maxDelay):
            delay = maxDelay
        status_key = self.getSLHourlyStatusLogKey(country)

        dateTime = datetime.strptime(date + " " + hour, "%Y/%m/%d %H")
        folderList = []
        missing = 0
        maxGap = minDelay
        for delay in range(delay):
            timeStr = dateTime.strftime("%Y/%m/%d/%H")
            status = self.STATUS_L.getStatus(status_key, timeStr)
            if (status):
                logging.info(" + {}".format(timeStr))
                folderList.append(timeStr)
            else:
                missing += 1
                if (missing >= maxGap):
                    logging.warn("Reached max gap {} on delay {}".format(maxGap, delay))
                    break
            dateTime = dateTime + timedelta(hours = -1)

        return folderList


#-----------
# Unit Test
#-----------

def init_logging():
    """Initialize logging"""
#    fmt = ("[%(module)s] %(levelname)s %(message)s")
#    fmt = ("%(asctime)s:%(name)s %(levelname)s [%(funcName)s] %(message)s")
    fmt = ("%(asctime)s %(levelname)s [%(module)s.%(funcName)s] %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt,
                        datefmt=datefmt,
                        level=logging.DEBUG)
    # IPython specific setting
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)


def load_config(conf_file="ard.properties",
                conf_path="/home/xad/ard/config:/home/xad/share/config",
                dump=False):
    """Load the configuration"""
    conf = Conf()
    conf.load(conf_file, conf_path)
    if (dump):
        conf.dump()
    return conf


def _test_sub_hour_partitions(base):
    """Test getSubHourPartitions"""
    # Some ofthe paths should exist in HDFS for the testing
    paths = ['/data/science_core_ex/us/display/2017/05/09/03',
             '/data/science_core_ex/jp/euwest1/2017/05/09/09']
    for path in paths:
        part_list = base.getSubHourPartitions(path, empty=True)
        logging.info("empty=True: {} => {}".format(path, part_list))
        part_list = base.getSubHourPartitions(path, empty=False)
        logging.info("empty=False: {} => {}".format(path, part_list))

def _test_findMissingPartitions(base):
    paths = ['/data/science_core_ex/cn/cnnorth1/2017/05/09/03',
             '/data/science_core_ex/jp/euwest1/2017/05/09/09']
    for path in paths:
        missing_parts = base.findMissingPartitions(path)
        logging.info("empty=True: {} => {}".format(path, missing_parts))

def _test_queries(base):

    table_name =  base.cfg.get('ard.output.table');
    country = 'gb'
    logtype = 'display'
    date = '2017/04/25'
    hour = '16'

    hour_path = base._get_science_core_orc_path(country, logtype, date, hour)
    logging.info("hour_path = {}".format(hour_path))
    subparts = base.makeFullSubHourPartitions()
    logging.info("subparts = {}".format(subparts))

    # Drop
    dropQuery = base.makeDropPartitionQuery(table_name, country, logtype,
                                            date, hour)
    logging.info("QUERY = {}".format(dropQuery))

    # Add
    locations = [os.path.join(hour_path, p[0], p[1]) for p in subparts]
    addQuery = base.makeAddPartitionQuery(table_name, country, logtype, date,
                                          hour, subparts, locations)
    logging.info("QUERY = {}".format(addQuery))

def _test_sl(base):
    """Test smartlocation centroids"""
    country = 'us'
    date = '2017/06/01'
    hour = '07'

    deltaHours = base.getSLDelayHours(country, date, hour)
    logging.info("delta hors = {}".format(deltaHours))
    entries = base.findSLEntries(country, date, hour)
    logging.info("folders = {}".format(entries))

    llPath = base.getSLCombinedHourlyLLPaths(country, entries)
    ipPath = base.getSLCombinedHourlyIPPaths(country, entries)
    logging.info("LL Path = {}".format(llPath))
    logging.info("IP Path = {}".format(ipPath))



def main():
    '''Unit test driver'''
    init_logging();
    conf = load_config()
    base = BaseArd(conf)

    # Tests
#    _test_sub_hour_partitions(base)
#    _test_findMissingPartitions(base)
#    _test_queries(base)
    _test_sl(base)

    print ("Done!")


if (__name__ == "__main__"):
    """Unit Test"""
    main()


