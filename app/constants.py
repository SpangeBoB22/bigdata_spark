import pathlib

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

MAIN_PATH = pathlib.Path("F:/spark_data")

PATH_TO_TRIP_DATA = MAIN_PATH / 'data'
PATH_TO_FARE_DATA = MAIN_PATH / 'fare'

PARQUET_TRIP_DATA = 'F:/parquet_data/trip'
PARQUET_FARE_DATA = 'F:/parquet_data/fare'

PARQUET_TRIP_SAMPLE = 'F:/parquet_sample/trip'
PARQUET_FARE_SAMPLE = 'F:/parquet_sample/fare'

FARE_SCHEMA = StructType([
    StructField("medallion", StringType(), True),
    StructField("hack_license", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("surcharge", FloatType(), True),
    StructField("mta_tax", FloatType(), True),
    StructField("tip_amount", FloatType(), True),
    StructField("tolls_amount", FloatType(), True),
    StructField("total_amount", FloatType(), True)
])

TRIP_SCHEMA = schema = StructType([
    StructField('medallion', StringType(), True),
    StructField('hack_license', StringType(), True),
    StructField('vendor_id', StringType(), True),
    StructField('rate_code', FloatType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_time_in_secs', IntegerType(), True),
    StructField('trip_distance', FloatType(), True),
    StructField('pickup_longitude', FloatType(), True),
    StructField('pickup_latitude', FloatType(), True),
    StructField('dropoff_longitude', FloatType(), True),
    StructField('dropoff_latitude', FloatType(), True)
])
