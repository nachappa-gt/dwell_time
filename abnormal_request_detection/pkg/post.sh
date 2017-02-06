#!/bin/sh -x
#
# Post deployment script
#
echo "[POST] BEGIN"

PROJ_HOME=/home/xad/ard
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

# Checking postgresql JDBC driver
HDP_DIR=/usr/hdp/current
SQOOP_DIR=$HDP_DIR/sqoop-client/lib
#POSTGRESQL_DIR=$HDP_DIR/oozie-client/libserver
POSTGRESQL_DIR=/home/xad/ard/lib

echo "# Checking postgresql jdbc driver ..."
if [ -e $SQOOP_DIR/postgre*.jar ]; then
    echo "Found posgrequesql.jar in $SQOOP_DIR"
else
    if [ -e $POSTGRESQL_DIR/postgresql*.jar ]; then
        ln -sf $POSTGRESQL_DIR/postgresql*.jar $SQOOP_DIR/
    else
        echo "Missing postgreql.jar in $SQOOP_DIR; no source ($POSTGRESQL_DIR)"
        exit 1
    fi
fi

echo "[POST] END"
