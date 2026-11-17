
import argparse
import csv
import random
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, length
from pyspark.ml import PipelineModel


#read file paths from arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "--data-path",
    default="data/hackernews_messages.jsonl"
)
parser.add_argument(
    "--model-path",
    default="models/ag_news_pipeline"
)
parser.add_argument(
    "--output-dir",
    default="outputs"
)
args = parser.parse_args()

output_dir = Path(args.output_dir)
output_dir.mkdir(parents=True, exist_ok=True)

labels = {
    0: "World",
    1: "Sports",
    2: "Business",
    3: "Sci/Tech"
}

spark = (
    SparkSession.builder
    .appName("HackerNewsBatchClassification")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


#load messages collected from hacker news
messages = spark.read.json(args.data_path)

raw_count = messages.count()

#remove empty messages and duplicate text
cleaned = (
    messages
    .select("id", "text")
    .filter(col("text").isNotNull())
    .withColumn("text", trim(col("text")))
    .filter(length(col("text")) > 0)
    .dropDuplicates(["text"])
)

clean_count = cleaned.count()

print("\nDATA CLEANING")
print("Original messages:", raw_count)
print("Messages after cleaning:", clean_count)
print("Removed:", raw_count - clean_count)

if clean_count < 1000:
    print("Warning: fewer than 1000 messages remain after cleaning.")


#load the same trained pipeline model
model = PipelineModel.load(args.model_path)

#predict categories for all cleaned messages
predictions = model.transform(cleaned)

#collect results for csv export
results = []

for row in predictions.select(
    "id", "text", "prediction", "probability"
).toLocalIterator():

    predicted_id = int(row["prediction"])
    probabilities = row["probability"].toArray()

    results.append({
        "id": row["id"],
        "text": row["text"],
        "predicted_class": labels[predicted_id],
        "probability": round(
            float(probabilities[predicted_id]), 6
        )
    })


#save every prediction to a csv file
prediction_file = output_dir / "predictions.csv"

with open(
    prediction_file,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.DictWriter(
        file,
        fieldnames=[
            "id",
            "text",
            "predicted_class",
            "probability"
        ]
    )

    writer.writeheader()
    writer.writerows(results)

print("\nSaved predictions to:", prediction_file)


#count messages in each predicted category
print("\nPREDICTED CLASS COUNTS")

for label in labels.values():
    count = sum(
        1 for row in results
        if row["predicted_class"] == label
    )

    print(f"{label}: {count}")


#show five example messages for each class
print("\nFIVE EXAMPLES PER CLASS")

for label in labels.values():
    print(f"\n--- {label} ---")

    examples = [
        row for row in results
        if row["predicted_class"] == label
    ][:5]

    for row in examples:
        print("Text:", row["text"][:200])
        print("Probability:", row["probability"])
        print()


#save a reproducible random sample for manual labeling
rng = random.Random(42)
sample_size = min(50, len(results))

sample = rng.sample(results, sample_size)

label_file = output_dir / "manual_labels_50.csv"

with open(
    label_file,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    writer.writerow([
        "id",
        "text",
        "predicted_class",
        "probability",
        "manual_label"
    ])

    for row in sample:
        writer.writerow([
            row["id"],
            row["text"],
            row["predicted_class"],
            row["probability"],
            ""
        ])

print("\nSaved manual labeling file to:", label_file)
print("Sample size:", sample_size)

spark.stop()
