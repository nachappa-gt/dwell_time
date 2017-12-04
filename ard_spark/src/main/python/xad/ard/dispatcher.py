#
# Copyright (C) 2016,2017.  xAd, Inc.  All Rights Reserved.
#

"""
ard Command Dispatcher

@author: xiangling
"""

import datetime
import filelock
import logging
import os

from xad.common.commandkey import CommandKey
from xad.ard.ard_main import ArdMain
from xad.ard.ard_regen import ArdRegen
from xad.ard.ard_fixes import ArdFixes
from baseard import BaseArd


DEBUG = False


class Dispatcher(BaseArd):

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseArd.__init__(self, cfg, opt.__dict__)
        # Create components
        self.ARD_MAIN = ArdMain(cfg, opt.__dict__)
        self.ARD_REGEN = ArdRegen(cfg, opt.__dict__)
        self.ARD_FIXES = ArdFixes(cfg, opt.__dict__)

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
            # GEN
            if (cmdKey == ck.get('gen')):
                self.ARD_MAIN.genHourly()

            # Push to S3
            elif (cmdKey == ck.get('s3pd')):
                self.ARD_MAIN.s3Push(daily=True)
            elif (cmdKey == ck.get('s3ph')):
                self.ARD_MAIN.s3Push(daily=False)
                
            # Hive Partition on S3
            elif (cmdKey == ck.get('s3hive')):
                self.ARD_MAIN.s3Hive()

            # Hive Partition on HDFS
            elif (cmdKey == ck.get('hdfshive')):
                self.ARD_MAIN.hdfsHive()


            # Cleaning
            elif (cmdKey == ck.get('clean')):
                logging.info("# ARD Cleaning... FIXME")
                
            # Monitoring
            elif (cmdKey == ck.get('mon')):
                logging.info("# ARD Monitoring... FIXME")

            # FIXES
            elif (cmdKey == ck.get('addpar')):
                self.ARD_FIXES.addPartitions()
            elif (cmdKey == ck.get('regen')):
                self.ARD_REGEN.genHourly()
            elif (cmdKey == ck.get('fixpar')):
                self.ARD_FIXES.fixMissing()
            elif (cmdKey == ck.get('fixstatus')):
                self.ARD_FIXES.fixStatusLog()
            elif (cmdKey == ck.get('fixrep')):
                self.ARD_FIXES.fixRepeatedFolders()

            # TEST
            elif (cmdKey == ck.get('test')):
                logging.info("# ARD TEST")
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


