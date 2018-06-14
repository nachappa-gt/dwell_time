#!/bin/sh -x
#
# Post deployment script
#
echo "[POST] BEGIN"

CRON_PATH=/etc/cron.d/dwell_time

# Change owner and mode.
# The mode 664 will prevent it from running.
# Change it to 644 to active the cronjob.
cmod 664 $CRON_PATH

echo "[WARNING] Type the following to active cron:"
echo "[WARNING] > sudo chmod 644 $CRON_PATH"
echo "[POST] END"

