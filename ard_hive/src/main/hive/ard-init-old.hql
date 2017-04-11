--
-- Initialize the ARC output talbe
--
-- Copyright (C) 2017 by xAd, Inc.  All Rights Reserved.
-- 

-- Initialize the final table. Only need to run once
create external table xianglingmeng.ard_orc_partition (
    r_timestamp string, request_id string, pub_id string,
    tsrc_id string, sp_iab_category string, user_iab_category string,
    user_ip string, city string, state string,
    zip string, country string, latitude string,
    longitude string, sl_adjusted_confidence string, sl_json string,
    fp_sic string, fp_brand string, uid string,
    uid_type string, uid_hash_type string, age string,
    gender string, carrier string, os string,
    device_os_version string, device_make string, device_model string,
    device_year string, device_type string, pub_type string,
    bundle string, sp_user_age string, sp_user_gender string,
    int_banner string, isp string, too_freq_uid string,
    banner_size string, request_filled string, pub_bid_floor string,
    r_s_info string, ad_id string, campaign_id string,
    adgroup_id string, creative_id string, mslocation_id string,
    ad_vendor_id string, category string, matched_user_iab_category string,
    matched_sp_iab_category string, adomain string,
    creative_type string, rtb_bucket_id string, neptune_bucket_id string,
    d_s_info string, adv_bid_rates string, pub_bid_rates string,
    ad_returned string, ad_impression string, click string,
    call string, click_to_call string, maps string,
    directions string, website string, description string,
    sms string, moreinfo string, review string,
    winbid string, save_to_app string, save_to_ph_book string,
    arrival string, checkin string, media string,
    coupon string, passbook string, app_store string,
    video_start string, video_end string, xad_revenue string,
    pub_revenue string, is_repeated_user string, tracking_user_agent string,
    tracking_user_ip string, fp_matches string, connection_type string, geo_type string,
    app_site_domain string, dnt string, geo_block_id string, event_count string,
    filter_weight string, abnormal_request string)
partitioned by (
    cntry string, dt string, hour string, prod_type string,
    fill string, loc_score string)
row format delimited fields terminated by '\t'
location '/user/xianglingmeng/ard/ard_orc_partition';


--
-- vim: ft=sql
--
