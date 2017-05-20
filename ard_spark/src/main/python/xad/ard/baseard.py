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

from xad.common import dateutil
from xad.common import hourutil
from xad.common import hdfs
from xad.common.conf import Conf
from xad.common.optioncontainer import OptionContainer
from xad.common import system


FILL_FOLDERS = set(['fill', 'nf'])
LOC_SCORE_FOLDERS = set(['tll', 'pos', 'rest'])


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


    def _get_science_core_avro_path(self, country, logtype, *entries):        
        base_dir = self.cfg.get('extract.data.prefix.hdfs')        
        return os.path.join(base_dir, country, logtype, *entries)


    def _get_science_core_orc_path(self, country, logtype, *entries):
        """Get path to the ORC-based science foundation files"""
        base_dir = self.cfg.get('orc.data.hdfs')
        return os.path.join(base_dir, country, logtype, *entries)


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


    def getSubHourPartitions(self, hour_path):
        """Get a list of [fill, loc_score] folders under specified path
        
        Find all available fill/loc_score sub-folder combinations under
        the specified hour path.   The fill folder may have "fill" and "nf".
        The loc_score folder may have "tll", "pos", and "rest".
        It use hdfs ls functions to check the availability of these folders.
        It does not check the size of the folders.
        
        Arg:
           hour_path Path to an hour folder. E.g.,
               "/data/science_core_ex/us/exchange/2017/05/01/00"
        
        Return: a list of [fill,loc_score] pair.
        
        Sample output:
           [['fill', 'pos'], ['fill', 'rest'], ['fill', 'tll'],
            ['nf', 'pos'], ['nf', 'rest'], ['nf', 'tll']]
        """
        ls_outputs = hdfs.ls(hour_path + "/*", pwd=True)

        # Now collect fill and loc_score entries
        subparts = []
        for p in ls_outputs:
            entries = p.split('/');
            if (len(entries) > 2):
               loc_score = entries[-1]
               fill = entries[-2]
               if (fill in FILL_FOLDERS and loc_score in LOC_SCORE_FOLDERS):
                   subparts.append([fill, loc_score])

        return subparts                


    def makeFullSubHourPartitions(self):
        """Create a complet sub partition list"""
        return [ [x,y] for x in sorted(FILL_FOLDERS)
            for y in sorted(LOC_SCORE_FOLDERS)]

        
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


    def makeDropPartitionQuery(self, table_name, cntry, prod_type, dt, hour,
                               subparts):
        """Create a Hive query for dropping partitions
        
        Create a Hive query for dropping partitions.
        Because subparts is a list structure, the query may
        drop multiple partitions.
        
        Args:
        - cntry: country
        - prod_type: product type
        - dt: date, in the format of yyyy-MM-dd
        - hour: in the format of 'hh'
        - subparts: a list of [fill,loc_score] pairs.
        - locations: a list of locations matching subparts.
        
        TODO: Generalized and put in HiveCommand module.
        """
        
        spec = self.makePartitionSpec(cntry, prod_type, dt, hour, subparts)
        query = "ALTER TABLE {} DROP IF EXISTS\n  {};".format(table_name, spec)        
        return query;


    def makePartitionSpec(self, cntry, prod_type, dt, hour, subparts,
                          locations=None):
        """Generate the partition spec
        
        Note that the it may be involved with multiple partitions
        delimited by commas"""

        spec = ""
        dt = dateutil.convert(dt, 'yyyy-MM-dd')
        num_parts = len(subparts)
        has_locations = False;
        if (locations is not None):
            if (len(locations) == num_parts):
                has_locations = True
            else:
                #logging.error("Locations does not match subparts")  
                pass
        
        for i in range(num_parts):
            # Handle partition spec delimiters
            if (i > 0):
                spec += "\n  " if has_locations else ",\n  " 
                
            p = subparts[i]
            spec += "PARTITION (cntry='{}', ".format(cntry)
            spec += "prod_type='{}', ".format(prod_type)
            spec += "dt='{}', hour='{}', ".format(dt, hour)
            spec += "fill='{}', loc_score='{}')".format(p[0], p[1])    
            
            # Handle locations
            if (has_locations):
                loc = locations[i]
                spec += "\n    LOCATION '{}'".format(loc)    
                
        return spec;        


    def runHiveQuery(self, query):
        """Run the specified Hive query"""
        uri = self.cfg.get('hiveserver.uri')
        queue = self.QUEUE if self.QUEUE else self.cfg.get('ard.default.queue')
        user = os.environ['USER']
        
        cmd = "beeline"
        cmd += " -u \"{}\"".format(uri)
        cmd += " --hiveconf tez.queue.name={}".format(queue)
        cmd += " -n {}".format(user)  
        cmd += " -e \"{}\"".format(query)
        system.execute(cmd, self.NORUN)   
          

    def addHivePartitions(self, country, logtype, date,
                          hour, subparts, hourly_path):
        """Add Hive partitions"""
        table_name =  self.cfg.get('ard.output.table');
        locations = [os.path.join(hourly_path, p[0], p[1]) for p in subparts]
        
        # Make query
        dropQuery = self.makeDropPartitionQuery(table_name, country, logtype, date,
                                                hour, subparts)
        addQuery = self.makeAddPartitionQuery(table_name, country, logtype, date,
                                      hour, subparts, locations)
        query = dropQuery + "\n" + addQuery
        self.runHiveQuery(query)



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
    paths = ['/data/extract/us/display/2017/04/25/16',
             '/data/extract/us/display/2017/04/25/17',
             '/data/extract/us/display/2017/04/25/18'
             ]
    for path in paths:
        part_list = base.getSubHourPartitions(path)
        logging.info("{} => {}".format(path, part_list))
    
    
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
    dropQuery = base.makeDropPartitionQuery(table_name, country, logtype, date,
                                            hour, subparts)
    logging.info("QUERY = {}".format(dropQuery))
    
    # Add
    locations = [os.path.join(hour_path, p[0], p[1]) for p in subparts]
    addQuery = base.makeAddPartitionQuery(table_name, country, logtype, date,
                                          hour, subparts, locations)
    logging.info("QUERY = {}".format(addQuery))


def main():
    '''Unit test driver'''    
    init_logging();
    conf = load_config()
    base = BaseArd(conf)
    
    # Tests
#    _test_sub_hour_partitions(base)
    _test_queries(base)

    print ("Done!")


if (__name__ == "__main__"):
    """Unit Test"""
    main()


