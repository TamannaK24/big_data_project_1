from datasets import load_dataset
from pathlib import Path

#download the full ag news training dataset
data = load_dataset("fancyzhx/ag_news", split="train")

#save as parquet so pyspark can read it
output = Path("data/ag_news_train.parquet")
output.parent.mkdir(parents=True, exist_ok=True)
data.to_parquet(str(output))

#print dataset information
print("Total training articles:", len(data))
print("Columns:", data.column_names)

labels = ["World", "Sports", "Business", "Sci/Tech"]

for i, label in enumerate(labels):
    count = sum(1 for value in data["label"] if value == i)
    print(f"{label}: {count}")

print("Saved to:", output)
