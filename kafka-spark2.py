from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


spark = SparkSession.builder.appName("APP").getOrCreate()

df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "twitter-data") \
      .option("startingOffsets", "latest") \
      .load()
      

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", "/amar") \
    .start()

query.awaitTermination()
