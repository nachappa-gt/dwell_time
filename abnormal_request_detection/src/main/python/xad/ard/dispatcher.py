#
# Copyright (C) 2016. xAd, Inc.  All Rights Reserved.
#

"""
ard Command Dispatcher

@author: xiangling
"""

import datetime
import filelock
import logging
import os
import time

from xad.common.conf import Conf
from xad.common.commandkey import CommandKey
from baseard import BaseArd
from poidb import PoiDB
from science_core import ScienceCore


DEBUG = False


class Dispatcher(BaseArd):

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseArd.__init__(self, cfg, opt.__dict__)
        # Create components
        self.POI = PoiDB(cfg, opt.__dict__)
        self.SCI = ScienceCore(cfg, opt.__dict__)
        #self.logger = logging.getLogger()


    def run(self):
        """Acquire lock and dispatch commands.
        The default behavior is locking unless --nolock is specified.
        """
        if (self.NOLOCK):
            logging.debug("Lock = None!")
            self.dispatchCommands()
        else:
            # Get the lock file path
            lockDir = self.cfg.get('proj.lock.dir')
            lockFile = "lock." + self.LOCK
            path = os.path.join(lockDir, lockFile)
            logging.debug("Lock = {}".format(path))

            # Try to get the lock
            lock = filelock.FileLock(path)
            try:
                with lock.acquire(timeout=0):
                    self.dispatchCommands()
            except filelock.Timeout:
                logging.error('Failed to get lock {}'.format(path))


    def dispatchCommands(self):
        """Dispatch commands to dedicated functions"""

        # Use CommandKey object to parse commands
        ck = CommandKey(self.cfg)
        ck.activateCommands(self.CMDS)
        if (DEBUG): ck.dump()

        # Get a list of active commands ordered by their priorities
        activeKeys = ck.getActiveKeys()

        # Process each active key
        for cmdKey in activeKeys:
            beginTime = self._beginSession(cmdKey)
            # POI
            if (cmdKey == ck.get('poi-get')):
                self.poiGet()
            elif (cmdKey == ck.get('poi-split')):
                self.poiSplit()
            elif (cmdKey == ck.get('poi-clean')):
                self.poiClean()

            # Science Core Conversion
            elif (cmdKey == ck.get('scpq-gen')):
                self.scpqGen()

            # TEST
            elif (cmdKey == ck.get('test-exception')):
                self.testException()
            elif (cmdKey == ck.get('test-sleep')):
                self.testSleep()
            self._endSession(cmdKey, beginTime)

        logging.info("Done!")


    def _beginSession(self, title):
        """Print the command section title"""
        now = datetime.datetime.now()
        logging.info("# Timestamp = {}".format(now))
        line = '#' + '-' * (len(title) + 11)
        logging.info(line)
        logging.info('# {} (begin)'.format(title))
        logging.info(line)
        return now

    def _endSession(self, title, beginTime):
        """Print the end section message"""
        now = datetime.datetime.now()
        line = '#' + '-' * (len(title) + 11)
        logging.info(line)
        logging.info("# {} (end)".format(title))
        logging.info("# Timestamp = {}".format(now))
        logging.info("# Elapsed = {}".format(now - beginTime))

    #--------
    # POI DB
    #--------
    def poiGet(self):
        """Download POI tables."""
        self.POI.downloadTables()

    def poiSplit(self):
        """Split the POI tables.  This is for debugging only."""
        self.POI.splitTable()

    def poiClean(self):
        """Delete older POI DB folders"""
        self.POI.cleanFolders()


    #----------------------
    # Scienc Core Parquet
    #----------------------
    def scpqGen(self):
        """Convert Science Core to Parquet"""
        self.SCI.genParquet()

    #--------
    # TEST
    #--------
    def testException(self):
        """Test exception handling"""
        # Raise an exception
        logging.warn("Raising an exception...")
        #logging.exception("A test exception")
        raise Exception("ARD TEST on EXCEPTION")

    def testSleep(self):
        """Sleep a few seconds for lock testing"""
        duration = 5
        logging.info("Sleeping for {} seconds...".format(duration))
        time.sleep(duration)
        logging.info("Awake now!")



