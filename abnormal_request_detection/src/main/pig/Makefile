#
# Manual Test
#


# Pig Options
MR_OPTIONS = -x mapreduce
TEZ_OPTIONS = -x tez

# Parameters
AVRO_JAR = '/home/xad/share/java/avro-1.7.6.jar'
JSON_SIMPLE_JAR = '/home/xad/share/java/json-simple-1.1.jar'
PARQUET_PIG_BUNDLE_JAR = '/home/xad/xcp/lib/parquet-pig-bundle-1.6.0.jar'

REGION = us/exchange
CONF = tll
#CONF = pos
#CONF = rest
#FILL = fill
FILL = nf
FOLDER = 2016/07/22/00/$(FILL)/$(CONF)
COMP = snappy

INPUT = '/data/extract/$(REGION)/$(FOLDER)/*.avro'
OUTPUT = /user/victor/tmp/sf_$(FILL)_$(CONF)


# Local camus
sf-parquet sfp:
	@echo "# Convert Science Foundation Logs..." 
	pig $(TEZ_OPTIONS) \
	-param AVRO_JAR=$(AVRO_JAR) \
	-param JSON_SIMPLE_JAR=$(JSON_SIMPLE_JAR) \
	-param PARQUET_PIG_BUNDLE_JAR=$(PARQUET_PIG_BUNDLE_JAR) \
	-param COMP='$(COMP)' \
	-param INPUT=$(INPUT) \
	-param OUTPUT='$(OUTPUT)' \
	scienc_core_parquet.pig

read r:
	@echo "# Convert Science Foundation Logs..." 
	pig $(TEZ_OPTIONS) \
	-param PARQUET_PIG_BUNDLE_JAR=$(PARQUET_PIG_BUNDLE_JAR) \
	-param INPUT='$(OUTPUT)/gzip' \
	scienc_core_read.pig

hls:
	hdfs dfs -ls -R $(HDFS_ROOT)/

hclean:
	hdfs dfs -rm -R -skipTrash $(OUTPUT)

hdu du:
	hdfs dfs -du -h $(OUTPUT)

clean:
	rm -f *.log


