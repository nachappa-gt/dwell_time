# from pyspark import SparkConf, SparkContext
# from pyspark.sql import HiveContext
#
# """command to run this script"""
# """SPARK_MAJOR_VERSION=2 spark-submit --master yarn --queue dwell_time --driver-memory 2g --executor-memory 4g --num-executors 4 --packages com.databricks:spark-avro_2.11:3.2.0 create_dummy.py"""
#
# def main():
#     conf = SparkConf().setAppName('Create Empty Avro File with Schema')
#     sc = SparkContext(conf = conf)
#
#     hiveContext = HiveContext(sc)
#
#     # Load most recent data to get the latest schema, directory may change
#     input_path = '/data/extract/us/exchange/2017/04/30/03/fill/tll'
#     df = hiveContext.read.format("com.databricks.spark.avro").load(input_path)
#     df_schema = df.limit(1)
#     output_path = '/prod/dwell_time/dummy'
#     df_schema.write.mode('overwrite').format("com.databricks.spark.avro").save(output_path)
#
#     sc.stop()
#
#
# if __name__ == '__main__':
# 	main()
#
