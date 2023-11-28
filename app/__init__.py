from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NYC Taxi") \
    .config('spark.memory.offHeap.enabled', 'true') \
    .config('spark.memory.offHeap.size', '2g') \
    .config('spark.executor.pyspark.memory', '6g') \
    .config("spark.driver.memory", "8g") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
