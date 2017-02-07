#!/bin/sh -x

echo "[PRE] BEGIN"

# Python Modules
CONDA=/opt/anaconda/bin/conda
SITE_PACKAGES=/opt/anaconda/lib/python2.7/site-packages
if [ ! -e $SITE_PACKAGES/filelock.py ]; then
    $CONDA install -y --upgrade filelock
fi

echo "[PRE] END"
