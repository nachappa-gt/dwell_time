
-- Add partitions to Hive Table 
-- 
-- Copyright (C) 2017 by xAd, Inc.  All Rights Reserved.

set tez.queue.name = xianglingmeng;

ALTER TABLE ${SCIENCE_CORE_TABLE}
        ADD PARTITION (cntry=${COUNRY}, dt=${DATE}, hour=${HOUR}, prod_type=${LOGTYPE},fill=${FILL}, loc_score=${LOC_SCORE})
        LOCATION ${PATH};
