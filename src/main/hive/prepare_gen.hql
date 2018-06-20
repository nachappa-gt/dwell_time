INSERT OVERWRITE DIRECTORY '${hiveconf:output_path}' ROW FORMAT DELIMITED STORED as ORC
select uid, request_id, r_timestamp, latitude, longitude, user_ip, fp_matches, r_s_info, sl_adjusted_confidence
from science_core_orc where cntry='${hiveconf:country}' and dt='${hiveconf:query_date}'
and (loc_score='tll' or loc_score='pos') and uid != '' and sl_adjusted_confidence >=94
and fp_matches is not null;