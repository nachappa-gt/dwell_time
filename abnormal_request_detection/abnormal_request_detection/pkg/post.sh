#!/bin/sh -x
#
# Post deployment script
#
echo "[POST] BEGIN"

PROJ_HOME=/home/xad/user_frequency
SHARE_DIR=/home/xad/share
#LOCAL_HDFS=/home/xad/hdfs/prod/insights-etl

# Create empty directories (not supported by RPM)
mkdir -p $PROJ_HOME/log/pig
mkdir -p $PROJ_HOME/lock
mkdir -p $PROJ_HOME/data/output
mkdir -p $PROJ_HOME/data/working
mkdir -p $PROJ_HOME/tmp
#mkdir -p $LOCAL_HDFS/data/working

# Change owner (from root to xad)
XAD_GROUP=`id -g -n xad`
chown -R xad:$XAD_GROUP $PROJ_HOME
#chown -R xad:$XAD_GROUP $LOCAL_HDFS

echo "[POST] END"

