--
-- Hive streaming to generate new ORC tables with
-- abnoarml requests idenfieid.
--
-- Copyright (C) 2017 by xAd, Inc.  All Rights Reserved.
--
-- vim: ft=sql

-- Hard code for adding file through hdfs
-- add file hdfs:///user/xianglingmeng/ard/ard_model_files/ard_mapper_orc.py;
-- add file hdfs:///user/xianglingmeng/ard/ard_model_files/ard_reducer_orc.py;

set tez.queue.name = xianglingmeng;
add file ${ARD_MAPPER};
add file ${ARD_REDUCER};

--
-- Process science core hourly data and identify abnormal requests.
-- Save the output into a tmp table.
--
-- create external table xianglingmeng.ard_orc as
create table ${TMP_TABLE} as
select transform (a.*) using 'python ard_reducer_orc.py'
as r_timestamp, request_id, pub_id, tsrc_id, sp_iab_category, user_iab_category,
    user_ip, city, state, zip, country, latitude,longitude, sl_adjusted_confidence,
    sl_json, fp_sic, fp_brand, uid, uid_type, uid_hash_type, age, gender, carrier,
    os, device_os_version, device_make, device_model, device_year, device_type,
    pub_type, bundle, sp_user_age, sp_user_gender,int_banner,isp, too_freq_uid,
    banner_size, request_filled, pub_bid_floor, r_s_info, ad_id, campaign_id,
    adgroup_id, creative_id, mslocation_id, ad_vendor_id, category,
    matched_user_iab_category, matched_sp_iab_category, adomain,
    creative_type, rtb_bucket_id, neptune_bucket_id, d_s_info, adv_bid_rates,
    pub_bid_rates, ad_returned, ad_impression, click, call, click_to_call,
    maps, directions, website, description, sms, moreinfo, review,
    winbid, save_to_app, save_to_ph_book, arrival, checkin, media,
    coupon, passbook, app_store, video_start, video_end, xad_revenue,
    pub_revenue, is_repeated_user, tracking_user_agent, tracking_user_ip,
    fp_matches, connection_type, geo_type, app_site_domain,
    dnt, geo_block_id, event_count, filter_weight, cntry ,
    dt, hour, prod_type, fill, loc_score, abnormal_request
from (select transform (*) using 'python ard_mapper_orc.py'
    as r_timestamp, request_id, pub_id, tsrc_id, sp_iab_category, user_iab_category,
        user_ip, city, state, zip, country, latitude,longitude, sl_adjusted_confidence,
        sl_json, fp_sic, fp_brand, uid, uid_type, uid_hash_type, age, gender, carrier,
        os, device_os_version, device_make, device_model, device_year, device_type,
        pub_type, bundle, sp_user_age, sp_user_gender,int_banner,isp, too_freq_uid,
        banner_size, request_filled, pub_bid_floor, r_s_info, ad_id, campaign_id,
        adgroup_id, creative_id, mslocation_id, ad_vendor_id, category,
        matched_user_iab_category, matched_sp_iab_category, adomain,
        creative_type, rtb_bucket_id, neptune_bucket_id, d_s_info, adv_bid_rates,
        pub_bid_rates, ad_returned, ad_impression, click, call, click_to_call,
        maps, directions, website, description, sms, moreinfo, review,
        winbid, save_to_app, save_to_ph_book, arrival, checkin, media,
        coupon, passbook, app_store, video_start, video_end, xad_revenue,
        pub_revenue, is_repeated_user, tracking_user_agent, tracking_user_ip,
        fp_matches, connection_type, geo_type, app_site_domain,
        dnt, geo_block_id, event_count, filter_weight, cntry , dt, hour, prod_type, fill, loc_score
    from ${SCIENCE_CORE_TABLE}
    where cntry = ${COUNTRY} and dt = ${DATE} and hour = ${HOUR} and prod_type = ${LOGTYPE}
    distribute by uid sort by uid
) a;

-- Insert the temporary table into the final table

set hive.exec.dynamic.partition.mode=nonstrict;

insert into table xianglingmeng.ard_orc_partition
partition(cntry, dt, hour, prod_type, fill, loc_score)
select r_timestamp, request_id, pub_id, tsrc_id, sp_iab_category, user_iab_category,
    user_ip, city, state, zip, country, latitude,longitude, sl_adjusted_confidence,
    sl_json, fp_sic, fp_brand, uid, uid_type, uid_hash_type, age, gender, carrier,
    os, device_os_version, device_make, device_model, device_year, device_type,
    pub_type, bundle, sp_user_age, sp_user_gender,int_banner,isp, too_freq_uid,
    banner_size, request_filled, pub_bid_floor, r_s_info, ad_id, campaign_id,
    adgroup_id, creative_id, mslocation_id, ad_vendor_id, category,
    matched_user_iab_category, matched_sp_iab_category, adomain,
    creative_type, rtb_bucket_id, neptune_bucket_id, d_s_info, adv_bid_rates,
    pub_bid_rates, ad_returned, ad_impression, click, call, click_to_call,
    maps, directions, website, description, sms, moreinfo, review,
    winbid, save_to_app, save_to_ph_book, arrival, checkin, media,
    coupon, passbook, app_store, video_start, video_end, xad_revenue,
    pub_revenue, is_repeated_user, tracking_user_agent, tracking_user_ip,
    fp_matches, connection_type, geo_type, app_site_domain,
    dnt, geo_block_id, event_count, filter_weight,
    abnormal_request,
    cntry , dt, hour, prod_type, fill, loc_score
from ${TMP_TABLE};

-- Delete the temporary table

drop table ${TMP_TABLE};




