#!/bin/sh -x

echo "[PRE] BEGIN"

# Python Modules
PIP=/opt/anaconda/bin/pip
SITE_PACKAGES=/opt/anaconda/lib/python2.7/site-packages
if [ ! -e $SITE_PACKAGES/filelock.py ]; then
    $PIP install --upgrade filelock
fi

echo "[PRE] END"
