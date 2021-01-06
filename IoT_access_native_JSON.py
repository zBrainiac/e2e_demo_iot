import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession \
    .builder \
    .appName("IoT_access_native_JSON") \
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-2//") \
    .getOrCreate()

## Get data from geo location data dataset
print('load data from "geo location" data dataset')
start_time = time.time()

geolocation_schema = StructType() \
    .add("sensor_id", IntegerType(), True) \
    .add("city", StringType(), True) \
    .add("lat", DoubleType(), True) \
    .add("log", DoubleType(), True)

df_geolocation_data_clean = spark.read.format("csv") \
    .option("header", True) \
    .schema(geolocation_schema) \
    .load("s3a://demo-aws-2/user/mdaeppen/data_geo/geolocation_ch.csv")

df_geolocation_data_clean.printSchema()

df_geolocation_data_clean.show(n=10, truncate=False)
print(df_geolocation_data_clean.count())

print("--- %s 'geo location data cleansing' in seconds ---" % (time.time() - start_time))
