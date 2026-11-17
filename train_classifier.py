
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    HashingTF,
    IDF
)
from pyspark.ml.classification import LogisticRegression


#read file locations from command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "--data-path",
    default="data/ag_news_train.parquet"
)
parser.add_argument(
    "--model-path",
    default="models/ag_news_pipeline"
)
args = parser.parse_args()


#create spark session
spark = (
    SparkSession.builder
    .appName("AGNewsClassification")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


#load the full ag news training dataset
train_df = (
    spark.read.parquet(args.data_path)
    .select(
        col("text"),
        col("label").cast("double").alias("label")
    )
    .filter(col("text").isNotNull())
)

print("\nAG NEWS TRAINING DATA")
print("Total articles:", train_df.count())

#show how many articles belong to each class
train_df.groupBy("label").count().orderBy("label").show()

#split text into lowercase words
tokenizer = RegexTokenizer(
    inputCol="text",
    outputCol="words",
    pattern=r"\W+",
    toLowercase=True,
    minTokenLength=2
)

#remove common english stopwords
stopwords = StopWordsRemover(
    inputCol="words",
    outputCol="filtered_words"
)

#convert words into numerical term frequencies
hashing_tf = HashingTF(
    inputCol="filtered_words",
    outputCol="tf",
    numFeatures=16384
)

#calculate tf-idf features
idf = IDF(
    inputCol="tf",
    outputCol="features",
    minDocFreq=3
)

#train a multiclass logistic regression model
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    predictionCol="prediction",
    probabilityCol="probability",
    maxIter=20,
    regParam=0.1,
    elasticNetParam=0.0,
    family="multinomial"
)

#combine the required stages into one pipeline
pipeline = Pipeline(
    stages=[
        tokenizer,
        stopwords,
        hashing_tf,
        idf,
        lr
    ]
)

print("\nTraining Spark MLlib classifier...")
print("Pipeline: RegexTokenizer -> StopWordsRemover")
print("          -> HashingTF -> IDF -> LogisticRegression")

#fit the complete pipeline on ag news
model = pipeline.fit(train_df)

#save the model for batch and streaming predictions
model.write().overwrite().save(args.model_path)

print("\nTraining complete!")
print("Model saved to:", args.model_path)
print("Class labels:")
print("0 = World")
print("1 = Sports")
print("2 = Business")
print("3 = Sci/Tech")

spark.stop()
