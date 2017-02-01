#
# Makefile to run Pig scripts
#

PIG = pig

ROOT_DIR = ../../..
TARGET_DIR = $(ROOT_DIR)/target/
JARS_DIR = $(ROOT_DIR)/lib_managed/jars

INSIGHTS_JAR = $(TARGET_DIR)/scala-2.10/xad-insights-etl_2.10-0.9.0.jar
JODA_TIME_JAR = $(JARS_DIR)/joda-time/joda-time/joda-time-2.2.jar
SCALAJ_TIME_JAR = $(JARS_DIR)/org.scalaj/scalaj-time_2.10.2/scalaj-time_2.10.2-0.7.jar
SCALA_LIBRARY_JAR = $(JARS_DIR)/org.scala-lang/scala-library/scala-library-2.10.4.jar


d2m:
	$(PIG) -x local \
		-param INSIGHTS_JAR='$(INSIGHTS_JAR)' \
		-param JODA_TIME_JAR='$(JODA_TIME_JAR)' \
		-param SCALAJ_TIME_JAR='$(SCALAJ_TIME_JAR)' \
		-param SCALA_LIBRARY_JAR='$(SCALA_LIBRARY_JAR)' \
		./date_to_millis.pig 

clean:
	rm -f *.log

