import sys

dns_server_param = sys.argv[1]
year_param = sys.argv[2]
month_param = sys.argv[3]

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Custom-MoveSpecificToS3").getOrCreate()

HDFS_PROCESSED_PATH = f'hdfs://{dns_server_param}:8020/datalake/processed/retail/year={year_param}/month={month_param}'

S3_BUCKET_PATH = f"s3a://finalproject-9458/data/specific/{year_param}-{month_param}/"

df = spark.read.parquet(HDFS_PROCESSED_PATH)

df.write.mode("overwrite").parquet(S3_BUCKET_PATH)

spark.stop()
