#!/bin/sh -x
#
# Post deployment script
#
echo "[POST] BEGIN"

PROJ_HOME=/home/xad/dwell_time
SHARE_DIR=/home/xad/share

# Create project directories
mkdir -p $PROJ_HOME/log
mkdir -p $PROJ_HOME/tmp
mkdir -p $PROJ_HOME/lock
mkdir -p $PROJ_HOME/status

# Change owner (from root)
XAD_GROUP=`id -g -n xad`
chown -R xad:$XAD_GROUP $PROJ_HOME
chmod g+sw $PROJ_HOME/lock
chmod g+sw $PROJ_HOME/log
chmod g+sw $PROJ_HOME/status

echo "[POST] END"
