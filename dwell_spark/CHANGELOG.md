Release 1.2.4
--------------
- Added kyro serialization to ard_join command
- Date: 2017-12-04

Release 1.2.3
--------------
- Added hdfshive command
- Date: 2017-11-20

Release 1.2.2
--------------
- Added fields location, ha_poi_info
- Date: 2017-11-17

Release 1.2.1
--------------
- Added de to modeling list.  (Need to update cronjob as well removing --nosl)
- Date: 2017-09-06

Release 1.2.0
--------------
- Added fields - location_extensions, isp_match, beacons
- Updated Hive schema to the latest as well.
- Date: 2017-08-25

Release 1.1.1
--------------
- s3hive: use s3 as hive partitions
- fixrep: fix repeated loc_score folders like rest/rest and pos/pos.
- Date: 2017-07-14

Release 1.1.0
--------------
- s3put: S3 backup
- Date: 2017-07-11

Release 1.0.6
--------------
- Fixed prev_uid
- Added --nosl to skip centroid tagging.
- Date: 2017-06-29

Release 1.0.5
--------------
- Re-factoring with classes.
- Centroid filtering.
- Date: 2017-06-21

Release 1.0.4
--------------
- Use explicit schema for fill to address schema changes for fp_matches.
- Date: 2017-06-09

Release 1.0.3
--------------
- Fix empty orc for all non-us and non-gb countries.
- Fix empty orc for us gb rest partition
- Date: 2017-06-02

Release 1.0.2
--------------
- Fixed "fixpar" on dealing with empty folders.
- Date: 2017-05-24

Release 1.0.1
--------------
- Handled missing sub-hour partitions.
- Added "fixpar" command to fix processed hours.
- Passed HDFS paths to spark scripts.
- Date: 2017-05-23

Release 1.0.0
--------------
- Make it a formal release.
- Improve addpar performance by combining multiple parttions.
- Initial cleaning.
- Date: 2017-05-19

Release 0.9.65
--------------
- Added one command addpar to change the partition location from hour scale to fill/loc_score scale
- Date: 2017-05-17

Release 0.9.64
--------------
- Fixed matched_poitags.
- Date: 2017-04-18

Release 0.9.63
--------------
- Remove hdfs output folder before moving tmp data
- Use json module to update r_s_info.
- Added matched_poitags.
- Date: 2017-04-18

Release 0.9.62
--------------
- Latest code from Xiangling.
- Date: 2017-04-11

Release 0.9.61
--------------
- Fixed sqoop jar issue in HDP-2.5.
- Fixed sqoop import avro issue in HDP-2.5.
- Fixed queue issue in HDP-2.5
- Support local.host.id
- Date 2016-12-06

Release 0.9.60
--------------
- China now supports only cnnorth1 logtype
- Removed exchange logtype
- Date 2016-12-02

Release 0.9.59
--------------
- Download country_timezone dimention table from XADCMS DB.
- Date 2016-11-05

Release 0.9.58
--------------
- Update config to pull GB request sci foundation data
- Added cron entry
- Date 2016-10-25

Release 0.9.57
--------------
- Update config to pull datarequest sci foundation data
- Added cron entry
- Date 2016-10-24

Release 0.9.56
--------------
- Added config to pull datarequest sci foundation data
- Date 2016-10-12

Release 0.9.55
--------------
## These are the process of the development of xcp project made by Victor
- Moved parquet-pig-bundle.jar to project lib.
  Let the common package to deploy in shared folder.
- Date 2016-10-11

Release 0.9.54
--------------
- Added the offset flag to support data requests.
- Date 2016-10-03

Release 0.9.53
--------------
- Convert Science Core to Parquet (for testing)
- Date 2016-09-29

Release 0.9.52
--------------
- Updated status_log db to enigma_etl_hd2
- Date 2016-09-27

Release 0.9.51
--------------
- Created xcp-cron package
- Date 2016-09-16

Release 0.9.50
--------------
- Added sci foundation new logtypes for all the countries
- Added config to pull background data
- Date 2016-08-24

Release 0.9.49
--------------
- poi-get: download brandId, flagged, reason and hide from poi.
- Date 2016-08-05

Release 0.9.48
--------------
- poi-get reverted to download avro files (parquet didn't work yet)
- Date 2016-07-29

Release 0.9.47
--------------
- Added config to pull SvlControlGroup data from S3
- Date 2016-07-26

Release 0.9.46
--------------
- Python wrapper for POI download.
- POI split.
- Date 2016-07-25

Release 0.9.46
--------------
- Added driver option to sqoop import

Release 0.9.45
--------------
- poi download
- add s3 prefix (s3n, s3a) to config
- Date 2016-06-24

Release 0.9.44
--------------
- xadcms: improved verification.
- xadcms: modified cleaning logic to keeep min number of data sets.
- xadcms: added tmp prefix
- Date 2016-06-01

Release 0.9.43
--------------
- xcp: added campaign_dimension table to daily redshift pull
- Date 2016-06-01

Release 0.9.42
--------------
- xcp: added footprints_sic table to daily redshift pull

Release 0.9.41
--------------
- xcp: added footprints_brand table to daily xadcms pull
- Date 2016-05-16

Release 0.9.40
--------------
- xcp: added postgresql as dependent 
- Date 2016-05-10

Release 0.9.39
--------------
- xcp xadcms: validate SUCCESS file.
- Date 2016-04-07

Release 0.9.38
--------------
- xcp: archive dimension files.
- Date 2016-03-30

Release 0.9.37
--------------
- xcp: Fixed simulated automic distcp by deleting the destination
  folder before moving data from the tmp folder.
- Date 2016-03-14

Release 0.9.36
--------------
- Chekced force flag on enigma log download (older logs were deledted)!
- Date 2016-03-04

Release 0.9.35
--------------
- Added "--driver" to xadcms sqoop command.
- Faked the -atomic behavor.
- Date 2016-02-17

Release 0.9.34
--------------
- Added etldb-ext.science.xad.com alias for master-db3
- for connectivity from outside AWS etldb.science.xad.com
- Date 2015-12-14

Release 0.9.33
--------------
- Changed master db to etldb.science.xad.com
- Pulling bundle_meta_data_rev table data from atlantic db
- Date 2015-12-07

Release 0.9.32
--------------
- Changed master db to master-db1.mgmt.xad.com
- Date 2015-12-02

Release 0.9.31
--------------
- Fixed sqoop on importing large 'term' table.
- Date: 2015-09-14

Release 0.9.30
--------------
- Configured cp of top_traffic_src_dimension from Redshift
- Date 2015-07-17

Release 0.9.29
--------------
- [BUG] Create /data/redshift/dimension dir if it is missing.
- [BUG] Remove header from redshift dimension download.
- Added clean-logs command to clean /app-logs
- Added clean-dir command to clean user specified --dir.
- Date 2015-06-18

Release 0.9.28
--------------
- Modified extract cleaning logic
- Date 2015-06-03

Release 0.9.27
--------------
- Added new countries: it, es
- Download traffic_src_dimension.tsv
- Date 2015-06-02

Release 0.9.26
--------------
- Changed input/output folders for extracts(foundation layer)
- Date: 2015-05-22

Release 0.9.25
--------------
- Support downloading extracts(foundation layer) from S3.

Release 0.9.24
--------------
- Support downloading rtb_hourly_summary from S3.
- Date 2015-05-07

Release 0.9.23
--------------
- Support table specific sqoop options.
- Date 2015-04-07

Release 0.9.22
--------------
- Handle null or negative meta data for campaign hourly summary.
- Fixed enigma-hourly on handling empty HDFS folders.
- Date 2015-04-01

Release 0.9.21
--------------
- Enigma daily: handle FORCE option.
- Enigma: validate logs in HDFS.
- Enabled queue
- Date 2015-03-30

Release 0.9.20
--------------
- Added comment.
- Date 2015-03-04

Release 0.9.19
--------------
- Support event level enigma-clean
- eingma-ls
- Date 2015-03-02

Release 0.9.18
--------------
- Updated enigma-clean
- Added enigma-clean-nf.
- Download campaign hourly summary.
- Clean up local status logs.
- Date 2015-02-26

Release 0.9.17
--------------
- Fixed clean related commands.
- Date 2015-02-24

Release 0.9.16
--------------
- Fixed enigma-daily script with status logs.
- Date 2015-02-12

Release 0.9.15
--------------
- Force download enigma hourly.
- Added command to display enigma status logs (MySQL)
- Date 2015-02-09

Release 0.9.14
--------------
- XCP: double check status before each hourly copy.
- Date 2014-12-05

Release 0.9.13
--------------
- XADCMS: added category table.
- Date 2014-11-07

Release 0.9.12
--------------
- AdUserProfile backup sopped since 2014-10-02.
- Allow events to be downloaded from the original S3 location (30-day retension)
- Date 2014-10-06

Release 0.9.11
--------------
- Download daily call_tracking_log from RedShift to S3 & HDFS.
- Enigma daily: check local status.
- Enigma hourly: support conditiontal daily copy for some event types.
- Enigma: added cn, fr
- Date 2014-10-03

Release 0.9.10
--------------
- Get enigma logs at hourly granularity.
- Date 2014-09-30

Release 0.9.9
--------------
- Updated remote hdfs copy for Data Platform.
- Date 2014-08-29

Release 0.9.8
--------------
- Change s3 to s3n in distcp

Release 0.9.7
--------------
- Fixed xadcms-clean (caused by date format)

Release 0.9.6
--------------
- Added REQUIRED to pkg configuration.
- Support --queue for xadcmd.

Release 0.9.5
--------------
- Support csv data format for xadcms.

Release 0.9.4
--------------
- XADCMS import based sqoop.
- Forecast ETL download based on distcp.
- Publisher ETL download (skeleton).

Release 0.9.3
--------------
- Data platform support.
- Date: 2014-04-14

Release 0.9.2
--------------
- Check hours in source S3 dir before copy.
- Date: 2014-04-04

Release 0.9.1
--------------
- Check S3 dir.
- Set tmp dir.

Release 0.9.0
--------------
- Initial version
- Date: 2014-03-22
