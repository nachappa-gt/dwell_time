set tez.queue.name = xianglingmeng;

add file mapper_record_level.py;
add file reducer.py;

## first step is to generate a table with uid, request_id and status of the request
create table xianglingmeng.abnormal_request as
select transform (a.*) using 'python reducer.py'
as request_id, abnormal_request
from (
select transform (*) using 'python mapper_record_level.py'
as uid string, r_time string, request_id string, lat string, lon string, loc_score string
from science_core_orc where cntry = 'us' and dt = '2017-01-10' and hour = 12) a;

## second step is to join the abnormal_request table with science_core_orc

create table xianglingmeng.orc_abnormal
as
select a.* , b.abnormal_request
from (select * from science_core_orc where cntry = 'us' and dt = '2017-01-10' and hour = 12) a
join
(select * from xianglingmeng.abnormal_request) b 
on a.request_id = b.request_id;


## finaly the dynamic partitions will be added into the table
create table xianglingmeng.science_core_orc_new
(r_timestamp bigint, request_id string, pub_id int,
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
 fp_matches  array<struct<proximity_mode:int,fp_brand:string,fp_sic:string,weight:float,hash_key:string,is_poly:int>>,
 connection_type string, geo_type string, app_site_domain string,
 dnt boolean, geo_block_id array<int>, event_count int,
 filter_weight double, abnormal_request string)
partitioned by (cntry string, dt string, hour string, prod_type string, 
fill string, loc_score string);


insert into table xianglingmeng.science_core_orc_new partition (cntry, dt, hour, prod_type, fill, loc_score)
select r_timestamp, request_id, pub_id, tsrc_id, sp_iab_category,user_iab_category,
 user_ip, city, state, zip, country, latitude,longitude, sl_adjusted_confidence, 
 sl_json, fp_sic, fp_brand, uid, uid_type, uid_hash_type, age, gender, carrier, 
 os, device_os_version, device_make, device_model, device_year, device_type, 
 pub_type, bundle, sp_user_age, sp_user_gender,int_banner,isp, too_freq_uid,
 banner_size, request_filled, pub_bid_floor, r_s_info, ad_id, campaign_id,
 adgroup_id, creative_id, mslocation_id, ad_vendor_id, category, 
 matched_user_iab_category, matched_sp_iab_category, adomain,
 creative_type, rtb_bucket_id, neptune_bucket_id, d_s_info, adv_bid_rates, 
 pub_bid_rates, ad_returned, ad_impression, click, call, click_to_call, 
 map, directions, website, description, sms, moreinfo, review,
 winbid, save_to_app, save_to_ph_book, arrival, checkin, media,
 coupon, passbook, app_store, video_start, video_end, xad_revenue,
 pub_revenue, is_repeated_user, tracking_user_agent, tracking_user_ip, 
 fp_matches, connection_type, geo_type, app_site_domain,
 dnt, geo_block_id, event_count, filter_weight, status, 
 cntry , dt, hour, prod_type, fill, loc_score 
 from xianglingmeng.orc_abnoraml;


