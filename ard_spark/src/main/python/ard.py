#!/bin/env python2.7
#
# Copyright (C) 2016. xAd, Inc.  All Rights Reserved.
#

"""
Main function for ard.

@author: xiangling
"""

import sys
sys.path.append('/home/xad/ard/python')
sys.path.append('/home/xad/share/python')
import argparse
import getpass
import traceback
import logging
import re
import smtplib
import socket
from argparse import RawTextHelpFormatter
from email.mime.text import MIMEText

from xad.common.conf import Conf
from xad.ard.dispatcher import Dispatcher


# Constants
DEFAULT_CONFIG_DIRS = "/home/xad/ard/config:/home/xad/share/config"
DEFAULT_CONFIG_FILE = "ard.properties"
DEFAULT_ALERT_EMAIL = 'science-ops@xad.com'
DEFAULT_EMAIL_PRIORITY = '0'
DEBUG = False

DESC = """
NAME:
    ard.py -- FIXME ...

DESCRIPTION:
    FIXME ...

COMMANDS:
    This tool takes one or many commands.  A complete list of commands and
    alises can be found in the configuration file:

        /home/xad/ard/config/commands.properties

    Some commands are "single" commands.  Some are "composite" commands.
    The priority of the commands are determined by the value part of
    these commands in the configuration file.
    
    gen
       FIXME
    
"""
conf = None


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=DESC,
                                     formatter_class=RawTextHelpFormatter)
    # Command List - one or many.  See DESC
    parser.add_argument("cmds", metavar='cmd', nargs="+", help="command list")

    # Configuration dir and file
    parser.add_argument('--config_dirs', help="Configuration search dirs",
                        default=DEFAULT_CONFIG_DIRS)
    parser.add_argument('--config', help="Configuration file",
                        default=DEFAULT_CONFIG_FILE)
    parser.add_argument('--country', help="Countries")
    parser.add_argument('--date', help="Date(s)")
    parser.add_argument('--hour', help="Hour(s)")
    parser.add_argument('--fill', help="Fill folders")
    parser.add_argument('--sl', help="SL (loc_score) folders")
    parser.add_argument('--keep', action='store_true',
                        help="Keep old output files")
    parser.add_argument('-l', '--lock', help="Define custom lock file",
                        default="lock")
    parser.add_argument('--logtype', 
                        help="Log types (e.g. exchange, display, euwest1); aka product types")                        
    parser.add_argument('--output', help="Temporary HDFS output folders")
    parser.add_argument('-q', '--queue', help="YARN queue name")

    # Flags
    parser.add_argument('-d', '--debug', action='store_true', help="Turn on debugging")
    parser.add_argument('-f', '--force', action='store_true', help="Force run")
    parser.add_argument('-ne', '--noemail', action='store_true', help="No email.")
    parser.add_argument('-nf', '--nofix', action='store_true', help="No missing folder fix.")
    parser.add_argument('-nh', '--nohive', action='store_true', help="No Hive update.")
    parser.add_argument('-nj', '--nojoin', action='store_true', help="No Join.")
    parser.add_argument('-nl', '--nolock', action='store_true', help="No locking")
    parser.add_argument('-nm', '--nomodel', action='store_true', help="No modeling")
    parser.add_argument('-n',  '--norun', action='store_true', help="No run.")
    parser.add_argument('-ns', '--nostatus', action='store_true', help="No status update.")
    parser.add_argument('--partial', action='store_true', help="Partial replacement.")
    

    opt = parser.parse_args()
    return(opt)


def init_logging(opt):
    """Initialize logging"""
    global logger

    # Set logging level
    if (opt.debug):
        level = logging.DEBUG
    else:
        level = logging.INFO

    #fmt = ("%(asctime)s:%(name)s %(levelname)s " + "[%(module)s:%(funcName)s] %(message)s")
    #fmt = ("[%(module)s:%(funcName)s] %(levelname)s %(message)s")
    fmt = ("%(asctime)s %(levelname)s [%(module)s.%(funcName)s] %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt, datefmt=datefmt, level=level)

    # IPython specific setting
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.debug('Initialized logger')


def load_configuration(opt):
    """Load configuration files"""
    logger.debug("Loading configuration")
    conf = Conf()
    conf.load(opt.config, opt.config_dirs)
    if (DEBUG): conf.dump()
    return(conf)


def exceptionHandler(e):
    """Handle exceptions"""
    global opt, conf

    # Get parameters from the configuration if it is available.
    host = socket.gethostname();
    if (conf):
        to_addr = conf.get('alert.email')
        priority = conf.get('alert.email.priority')
        if (conf.has('local.host.id')):
            host = conf.get('local.host.id')
    else:
        to_addr = DEFAULT_ALERT_EMAIL
        priority = DEFAULT_EMAIL_PRIORITY

    if (opt):
        noemail = opt.noemail or opt.norun
    else:
        noemail = False

    # Trace Back
    tb = traceback.format_exc()

    # Get the user name and the host name
    user = getpass.getuser()
    from_addr = '{}@{}'.format(user, host)

    # Construct the email body and header
    msgArray = [str(e), "", "---", tb]
    email = MIMEText("\n".join(msgArray))
    email['Subject'] = '[ALERT] ARD ({})'.format(host)
    email['From'] = from_addr
    email['To'] = to_addr
    if (priority != '0'):
        email['X-Priority'] = priority

    logging.debug(str(e))
    print("\n" + str(email) + "\n")

    # Send the email unless --norun or --noemail options are set
    if (not noemail):
        logging.info('Sending email...')
        s = smtplib.SMTP('localhost')
        s.sendmail(email['From'],
                   re.split('[\s,]+', email['To']),
                   email.as_string())
        s.quit()
    else:
        logging.info('==> NO EMAIL!')


def main():
    """The Main Function"""
    global conf, opt
    try:
        # Parse command line arguments
        opt = parse_arguments()
        # Initiate logging
        init_logging(opt)
        # Load configuration
        conf = load_configuration(opt)
        # Create the main object
        dispatcher = Dispatcher(conf, opt)
        dispatcher.run()

    except Exception as e:
        exceptionHandler(e)

    except:
        logging.warn('Un-expected exception')
        e = sys.exc_info()
        exceptionHandler(e)


if (__name__ == "__main__"):
    main()


