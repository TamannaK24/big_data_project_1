
import os
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, udf, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

#kafka settings
kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

#start spark
spark = SparkSession.builder \
    .appName("HackerNewsNER") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#structure of the incoming messages
schema = StructType([
    StructField("id", LongType()),
    StructField("text", StringType()),
    StructField("time", LongType())
])

#read messages from topic1
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "topic1") \
    .option("startingOffsets", "earliest") \
    .load()

#convert kafka json messages into columns
stories = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

#remove empty messages
stories = stories.filter(
    col("text").isNotNull() & (col("text") != "")
)

#load spacy once per python worker
nlp = None

def get_entities(text):
    global nlp

    if nlp is None:
        nlp = spacy.load("en_core_web_sm")

    doc = nlp(text)
    entities = []

    for ent in doc.ents:
        if ent.label_ in ["PERSON", "ORG", "GPE", "LOC", "PRODUCT"]:
            entities.append(ent.text)

    return entities

#extract entities from the articles
entity_udf = udf(get_entities, ArrayType(StringType()))

entities_df = stories.withColumn(
    "entity", explode(entity_udf(col("text")))
)

#keep a running count of each entity
entity_counts = entities_df.groupBy("entity").count()

#show top entities and send counts to topic2
def process_batch(batch_df, batch_id):
    print(f"\nBatch {batch_id}: Top 10 named entities")

    batch_df.orderBy(col("count").desc()).show(10, truncate=False)

    #convert counts to json
    output_df = batch_df.select(
        col("entity").cast("string").alias("key"),
        to_json(
            struct(col("entity"), col("count"))
        ).alias("value")
    )

    #send counts to kafka topic2
    output_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", "topic2") \
        .save()

#start streaming
checkpoint = os.environ["STREAM_CHECKPOINT"]

query = entity_counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
