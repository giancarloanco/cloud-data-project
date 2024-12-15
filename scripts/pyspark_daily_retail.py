import sys

dns_server = sys.argv[1]

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import to_date, year, month, dayofmonth

HDFS_RAW_PATH = f'hdfs://{dns_server}:8020/datalake/raw/retail'

HDFS_PROCESSED_PATH = f'hdfs://{dns_server}:8020/datalake/processed/retail/'

spark = SparkSession.builder.appName("Daily-RetailProcess").getOrCreate()

schema = StructType([
        StructField("index", IntegerType(), True),
        StructField("InvoiceNo", IntegerType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DecimalType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True),
        StructField("invoice_date", DateType(), True)
    ])

df = spark.read.csv(HDFS_RAW_PATH, header=True, schema=schema)
df = df.withColumn("date", to_date(df["invoice_date"]))

df = df.withColumn("year", year(df["date"])).withColumn("month", month(df["date"])).withColumn("day", dayofmonth(df["date"]))

df.show(5)

df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(HDFS_PROCESSED_PATH)

spark.stop()
