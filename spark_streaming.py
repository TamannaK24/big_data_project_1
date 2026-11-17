
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

#connect to spark
spark = SparkSession.builder \
    .appName("HackerNewsStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#kafka settings
kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

#read incoming messages from topic1
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "topic1") \
    .option("startingOffsets", "earliest") \
    .load()

#structure of our json messages
schema = StructType([
    StructField("id", LongType()),
    StructField("text", StringType()),
    StructField("time", LongType())
])

#convert kafka messages into readable columns
stories = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

#ignore messages without text
stories = stories.filter(
    col("text").isNotNull() & (col("text") != "")
)

#print messages as they arrive
query = stories.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
