--
-- Initialize the ARC output talbe
--
-- Copyright (C) 2017 by xAd, Inc.  All Rights Reserved.
-- 

-- Initialize the final table. Only need to run once
create external table science_core_ex (
 r_timestamp bigint, request_id string, pub_id int,
 tsrc_id bigint, sp_iab_category array<string>, user_iab_category array<string>,
 user_ip string, city string, state string,
 zip string, country string, latitude double,
 longitude double, sl_adjusted_confidence int, sl_json string,
 fp_sic string, fp_brand string, uid string,
 uid_type string, uid_hash_type string, age int,
 gender string, carrier string, os string,
 device_os_version string, device_make string, device_model string,
 device_year int, device_type string, pub_type string,
 bundle string, sp_user_age int, sp_user_gender string,
 int_banner boolean, isp string, too_freq_uid boolean,
 banner_size string, request_filled string, pub_bid_floor double,
 r_s_info string, ad_id string, campaign_id string,
 adgroup_id string, creative_id string, mslocation_id string,
 ad_vendor_id string, category string, matched_user_iab_category array<string>,
 matched_sp_iab_category array<string>, adomain string,
 creative_type string, rtb_bucket_id string, neptune_bucket_id string,
 d_s_info string, adv_bid_rates string, pub_bid_rates string,
 ad_returned int, ad_impression bigint, click bigint,
 call bigint, click_to_call bigint, maps bigint,
 directions bigint, website bigint, description bigint,
 sms bigint, moreinfo bigint, review bigint,
 winbid bigint, save_to_app bigint, save_to_ph_book bigint,
 arrival bigint, checkin bigint, media bigint,
 coupon bigint, passbook bigint, app_store bigint,
 video_start bigint, video_end bigint, xad_revenue double,
 pub_revenue double, is_repeated_user boolean, tracking_user_agent string,
 tracking_user_ip string, 
 fp_matches  array<struct<proximity_mode:int,fp_brand:string,fp_sic:string,weight:float,hash_key:string,is_poly:int,block_id:int>>,
 connection_type string, geo_type string, app_site_domain string,
 dnt boolean, geo_block_id array<int>, event_count int,
 filter_weight double)
partitioned by (cntry string, dt string, hour string, prod_type string, 
fill string, loc_score string)
row format delimited fields terminated by '\t'
stored as orc
location '/data/science_core_ex';


-- vim: ft=sql
--
