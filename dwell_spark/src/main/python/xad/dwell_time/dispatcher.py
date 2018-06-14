#
# Copyright (C) 2018. GroundTruth. All Rights Reserved.
#

"""
dwell_time Command Dispatcher

@author: nachappa.ap
"""

import datetime
import filelock
import logging
import os

from xad.common.commandkey import CommandKey
from xad.dwell_time.dt_main import DwellTimeMain
from base_dt import BaseDT


DEBUG = False


class Dispatcher(BaseDT):

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseDT.__init__(self, cfg, opt.__dict__)
        # Create components
        self.DWELL_TIME_MAIN = DwellTimeMain(cfg, opt.__dict__)

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

            ### New code ###
            if (cmdKey == ck.get('processOne')):
                self.DWELL_TIME_MAIN.processOne(daily=True)

            elif (cmdKey == ck.get('processTwo')):
                self.DWELL_TIME_MAIN.processTwo(daily=True)

            # TEST
            elif (cmdKey == ck.get('test')):
                logging.info("# Pipeline TEST")
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


