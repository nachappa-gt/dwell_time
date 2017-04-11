ard
===

Download tool based on Hadoop distcp that download data
from S3 and database to a Hadoop cluster.

Detect abnormal request based on location

Mapping Table
-------------

| Component | Source | HDFS |
| enigma | s3://enigma-data-backup/raw-data/camus/data | /data/enigma |
| dp | s3://dataplatform/data | /data/dataplatform |
| fetl | s3://xad-science/forecast_etl/data | /data/forecast_etl |
| xadcms | mysql | /data/xadcms |
| extract(foundation layer) | s3://enigma-data-backup/extract | /data/extract |


