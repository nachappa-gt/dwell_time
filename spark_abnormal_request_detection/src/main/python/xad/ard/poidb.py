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


class PoiDB(BaseXcp):
    """A class for downloading tables from the POI database."""

    def __init__(self, cfg, opt):
        """Constructor"""
        BaseXcp.__init__(self, cfg, opt)


    #------------
    # Download
    #------------

    def downloadTables(self):
        """Download tables from POI DB"""
        logging.debug('Downloading POI DB tables...')

        # Connection info
        uri = self._getURI()
        user = self.cfg.get('poidb.conn.user')
        passwd = self.cfg.get('poidb.conn.passwd')
        tables = re.split('/\s+/', self.cfg.get('poidb.tables'))
        logging.debug("- uri = {}".format(uri))
        logging.debug("- user = {}".format(user))
        logging.debug("- tables = {}".format(tables))

        # HDFS destination
        date = self._getDate()
        sqoopFmt = self.cfg.get('poidb.data.format.sqoop')
        splitFmt = self.cfg.get('poidb.data.format.split')
        outputDir = self._getHDFSDir([splitFmt, date])
        tmpDir = self._getHDFSTmpDir(date)

        logging.debug("- date = {}".format(date))
        logging.debug("- sqoop format = {}".format(sqoopFmt))
        logging.debug("- output dir = {}".format(outputDir))
        logging.debug("- tmp dir = {}".format(tmpDir))

        # Test HDFS path
        if (hdfs.test(outputDir) and not self.FORCE):
            logging.info("x SKIP => HDFS dir exists {}".format(outputDir))
            return

        # Test Tmp Folder
        if (hdfs.test(tmpDir) and not self.KEEP):
            hdfs.rmrs(tmpDir)

        # Use sqoop to download tables from the database
        self.sqoopImport(uri, user, passwd, tables, sqoopFmt, tmpDir)

        # Split table and move to the output directory
        self.splitTable('poi', tmpDir, keep=False)

        # Move to the official output directory (at the DB level)
        self.hdfsMove(tmpDir, outputDir)


    def splitTable(self, table='poi', workingDir=None, field='country',
                   keep=False):
        """Split an avro table into multiple parquet tables.
        On completion, replace the original
        """

        # Get working directory if it is not defined
        if (workingDir is None):
            date = self._getDate()
            workingDir = self._getHDFSTmpDir(date)

        input = "{}/{}".format(workingDir, table)
        output = input + "-split"
        orig = input + "-orig"
        inputFmt = self.cfg.get('poidb.data.format.sqoop')

        logging.debug("- input = {}".format(input))
        logging.debug("- output = {}".format(output))

        # Build PySpark command
        LB = '\\\n' # Line break
        env = 'PYSPARK_PYTHON={} '.format(self.cfg.get('python.cmd'))
        drmem = self.cfg.get('spark.driver.memory')
        exmem = self.cfg.get('spark.executor.memory')
        numex = self.cfg.get('spark.num.executors')
        appName = 'xcp poi-split'

        # Packages
        sv = "2.10"     # Scala version
        pkgs = list()
        pkgs.append("com.databricks:spark-csv_{}:{}".format(sv, '1.4.0'))
        pkgs.append("com.databricks:spark-avro_{}:{}".format(sv, '2.0.1'))
        script = self.cfg.get('pyspark.script.split_poi')

        # Build main commands
        cmd = env + 'spark-submit' + LB
        cmd += ' --master yarn' + LB
        if (self.QUEUE):
            cmd += " --queue {}".format(self.QUEUE) + LB
        cmd += " --driver-memory {}".format(drmem) + LB
        cmd += " --executor-memory {}".format(exmem) + LB
        cmd += " --num-executors {}".format(numex) + LB
        cmd += " --packages {}".format(",".join(pkgs))
        #cmd += " --py-files {}".format(script) + LB
        cmd += " {}".format(script) + LB

        # Application Options
        cmd += ' --app_name "{}"'.format(appName) + LB
        if (self.DEBUG): cmd += " --debug" + LB
        if (self.NORUN): cmd += " --norun" + LB
        cmd += ' --input "{}"'.format(input) + LB
        cmd += ' --output "{}"'.format(output) + LB
        cmd += ' --input_format "{}"'.format(inputFmt)

        if (not self.NORUN):
            self.execute(cmd)

        # Swap with the original
        if (not self.NORUN):
            logging.debug("Renaming split output to original")
            if (hdfs.test(orig)): hdfs.rmrs(orig)
            hdfs.mv(input, orig)
            hdfs.mv(output, input)

        if (not keep and not self.KEEP and not self.NORUN):
            logging.debug("Deleting the original")
            hdfs.rmrs(orig)

    #---------
    # Clean
    #---------

    def cleanFolders(self):
        """Delete older POI DB folders"""
        dataFmt = self.cfg.get('poidb.data.format.split')
        rootDir = self._getHDFSDir([dataFmt])

        logging.debug("- root dir = {}".format(rootDir))
        pass


    #-------------------
    # Helper Functions
    #-------------------

    def _getDate(self):
        """Get today's date"""
        if (self.DATE):
            date = self.DATE
        else:
            date = dateutil.today()
        return(date)

    def _getURI(self):
        """Get the URI for DB connection"""
        host = self.cfg.get('poidb.conn.host')
        port = self.cfg.get('poidb.conn.port')
        dbname = self.cfg.get('poidb.conn.dbname')
        uri = "jdbc:postgresql://{}:{}/{}".format(host, port, dbname);
        return (uri)

    def _getHDFSDir(self, entries):
        """Get the target HDFS directory"""
        prefix = self.cfg.get('poidb.data.prefix.hdfs')
        path = os.path.join(prefix, *entries)
        return(path)

    def _getHDFSTmpDir(self, date):
        """Get a temporary working directory."""
        appTmpDir = self.getHDFSUserTmpDir()
        prefix = self.cfg.get('poidb.tmp.prefix')
        date = re.sub('-', '', date)  # remove '-'
        folder = "_".join([prefix, date])
        path = os.path.join(appTmpDir, folder)
        return(path)



