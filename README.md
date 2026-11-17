# Spark Streaming with Kafka and Text Classification with Spark MLlib

CS4371 Group Project 1, UT Dallas

This project has two parts:

- **Part 1: Real-time named entity pipeline.** A producer pulls new stories and comments from the Hacker News API and publishes them to Kafka. A PySpark Structured Streaming job extracts named entities with spaCy, keeps running counts, and sends them back to Kafka. Logstash loads the counts into Elasticsearch, and Kibana shows the top 10 entities.
- **Part 2: Text classification with Spark MLlib.** A logistic regression pipeline is trained on the AG News dataset (World, Sports, Business, Sci/Tech). It is used to classify the collected Hacker News messages in batch, and it also classifies messages live inside the same streaming job. 50 messages were labeled by hand to measure accuracy.

> **New here?** Skip to [Quick Start](#quick-start-fresh-laptop). It goes from a fresh clone to a working pipeline, one command at a time. The commands are for **Windows PowerShell**. If you're on macOS or Linux, see [macOS / Linux differences](#macos--linux-differences).

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Project Structure](#project-structure)
3. [Versions Used](#versions-used)
4. [Quick Start (fresh laptop)](#quick-start-fresh-laptop)
   - [Step 0: Install the required software](#step-0-install-the-required-software)
   - [Step 1: Clone the repo and set up Python](#step-1-clone-the-repo-and-set-up-python)
   - [Step 2: Set up Hadoop winutils (Windows only)](#step-2-set-up-hadoop-winutils-windows-only)
   - [Step 3: Start Kafka](#step-3-start-kafka)
   - [Step 4: Start Elasticsearch, Kibana, and Logstash](#step-4-start-elasticsearch-kibana-and-logstash)
   - [Step 5: Run the Hacker News producer](#step-5-run-the-hacker-news-producer-terminal-1)
   - [Step 6: Run the Spark streaming job](#step-6-run-the-spark-streaming-job-terminal-2)
   - [Step 7: Build the Kibana dashboard](#step-7-build-the-kibana-dashboard)
5. [Part 2: Classification Scripts](#part-2-classification-scripts)
6. [Checking That Everything Works](#checking-that-everything-works)
7. [Stopping and Restarting](#stopping-and-restarting)
8. [Troubleshooting](#troubleshooting)
9. [macOS / Linux differences](#macos--linux-differences)
10. [Results](#results)

---

## How It Works

```
PART 1 (streaming)
Hacker News API -> producer.py -> Kafka topic1 -> spark_streaming.py
                                                   ├─ Query 1: spaCy NER -> entity counts -> Kafka topic2 -> Logstash -> Elasticsearch -> Kibana
                                                   └─ Query 2: MLlib model -> live category counts -> console

PART 2 (batch)
data/ag_news_train.parquet -> train_classifier.py -> models/ag_news_pipeline
data/hackernews_messages.jsonl (saved by producer.py) + model -> batch_classification.py -> outputs/predictions.csv
outputs/manual_labels_50.csv (hand labeled) -> evaluate_classifier.py -> accuracy + confusion matrix
```

| Piece | File | What it does |
|---|---|---|
| Producer | `producer.py` | On start it walks back from the newest Hacker News item ID until it has saved 1,200 stories and comments. After that it checks for new items every 30 seconds. Each message (`id`, `text`, `time`) goes to Kafka `topic1`, and a copy is appended to `data/hackernews_messages.jsonl`. IDs already in that file are skipped, so restarting doesn't create duplicates. No API key is needed. |
| Streaming query 1 (NER) | `spark_streaming.py` | Reads `topic1`, runs spaCy `en_core_web_sm` on each message, and keeps entities labeled `PERSON`, `ORG`, `GPE`, `LOC`, or `PRODUCT`. It keeps a running count per entity. Every 30 seconds it prints the top 10 and writes every count to `topic2` as `{"entity": ..., "count": ...}`. |
| Streaming query 2 (classification) | `spark_streaming.py` | Reads `topic1` again, applies the trained model from `models/ag_news_pipeline`, and prints a running count of messages per category every 30 seconds. |
| Logstash | `logstash/logstash.conf` | Reads `topic2` and indexes each message into the Elasticsearch index `hackernews-entities`, using the entity name as the document ID so each entity has one up-to-date document. |
| Kibana | (built by hand) | A Lens bar chart of the top 10 entities. |
| Dataset download | `download_ag_news.py` | Downloads AG News (120,000 training articles) from Hugging Face to `data/ag_news_train.parquet`. **Already done, and the file is in the repo.** |
| Training | `train_classifier.py` | Pipeline: `RegexTokenizer` → `StopWordsRemover` → `HashingTF` → `IDF` → `LogisticRegression`. Saves to `models/ag_news_pipeline`. **Already trained, and the model is in the repo.** |
| Batch classification | `batch_classification.py` | Cleans the collected Hacker News messages, classifies them, and writes `outputs/predictions.csv` plus a random sample of 50 messages to `outputs/manual_labels_50.csv` for hand labeling. |
| Evaluation | `evaluate_classifier.py` | Reads the 50 hand-labeled messages and writes the accuracy, a confusion matrix, and misclassified examples to `outputs/`. |

---

## Project Structure

```
big_data_project_1/
├── producer.py                 # Part 1: Hacker News -> Kafka topic1 (+ saves data/hackernews_messages.jsonl)
├── spark_streaming.py          # Part 1 + 2: streaming NER -> topic2, and live classification -> console
├── docker-compose.yml          # Elasticsearch, Kibana, Logstash
├── logstash/
│   └── logstash.conf           # Logstash pipeline: Kafka topic2 -> Elasticsearch
├── download_ag_news.py         # Part 2: download the AG News training set
├── train_classifier.py         # Part 2: train the Spark MLlib pipeline
├── batch_classification.py     # Part 2: classify the collected Hacker News messages
├── evaluate_classifier.py      # Part 2: score the 50 hand-labeled messages
├── data/
│   ├── ag_news_train.parquet           # AG News training data (120k articles)
│   └── hackernews_messages.jsonl       # Hacker News messages collected by the producer
├── models/
│   └── ag_news_pipeline/       # Trained Spark MLlib PipelineModel
├── outputs/
│   ├── predictions.csv         # Predicted category for every cleaned Hacker News message
│   ├── manual_labels_50.csv    # 50 random messages with our manual labels filled in
│   ├── manual_evaluation.csv   # Same 50 rows marked Correct / Incorrect / Excluded
│   ├── confusion_matrix.csv    # Manual label vs. predicted label
│   └── evaluation_summary.txt  # Accuracy and misclassified examples
├── screenshots/                # Part 1: Kibana top 10 at 15/30/45/60 min, with the counts behind each
├── requirements.txt            # Python packages (same list as the pip install step)
├── checkpoints/                # Spark streaming state (local only, delete before your first run)
├── producer_original.py        # Older version of the producer (not used, kept for reference)
└── spark_streaming_backup.py   # Older NER-only version of the streaming job (not used)
```

---

## Versions Used

| Component | Version |
|---|---|
| OS | Windows 11 |
| Python | 3.13 |
| Java (JDK) | 21 |
| PySpark | 4.2.0 |
| spaCy + `en_core_web_sm` | 3.8 |
| kafka-python | 3.0 |
| Apache Kafka (`apache/kafka` Docker image) | 4.4.0 |
| Elasticsearch / Kibana / Logstash | 9.2.0 |
| Docker Desktop | any recent version |

---

## Quick Start (fresh laptop)

You'll use **two PowerShell windows** that stay open: one for the producer and one for the Spark job. All commands are meant to run from the **project root folder** (the folder that contains `producer.py`) unless a step says otherwise.

### Step 0: Install the required software

Install these once. If you already have them, run the check commands to confirm.

1. **Git**: https://git-scm.com/download/win
2. **Python 3.13**: https://www.python.org/downloads/ . In the installer, check **"Add python.exe to PATH"**.
3. **Java 21 (JDK)**: for example Eclipse Temurin 21 from https://adoptium.net/ . In the installer, enable **"Set JAVA_HOME variable"** and **"Add to PATH"**. Spark 4 needs Java 17 or 21.
4. **Docker Desktop**: https://www.docker.com/products/docker-desktop/ . Start it and wait until it says "Engine running". Give Docker at least 4 GB of memory (Settings → Resources) because Elasticsearch, Kibana, and Logstash all run in it.

After installing, **open a new PowerShell window** so it picks up the new PATH, then check:

```powershell
git --version
python --version
java -version
docker --version
docker compose version
```

`python --version` should print 3.13.x and `java -version` should print 21 (or 17).

### Step 1: Clone the repo and set up Python

```powershell
cd $HOME\Documents
git clone https://github.com/TamannaK24/big_data_project_1.git
cd big_data_project_1
```

Keep the folder name `big_data_project_1`. Docker Compose names its network after the folder, and Step 4 uses that name.

Create a virtual environment and activate it:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell says running scripts is disabled, run this once and then activate again:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

Your prompt should now start with `(.venv)`. Install the packages and the spaCy English model:

```powershell
python -m pip install --upgrade pip
pip install pyspark==4.2.0 numpy kafka-python requests spacy datasets
python -m spacy download en_core_web_sm
```

The same package list is in `requirements.txt`, so `pip install -r requirements.txt` works too.

(`numpy` is needed by Spark MLlib. `datasets` is only needed if you want to re-download AG News.)

**Delete the old Spark checkpoint folder.** It holds streaming state from our machine, which points at Kafka offsets that don't exist on your Kafka. If you skip this, the Spark job can fail or skip data.

```powershell
Remove-Item -Recurse -Force .\checkpoints -ErrorAction SilentlyContinue
```

### Step 2: Set up Hadoop winutils (Windows only)

On Windows, Spark needs two small Hadoop helper files (`winutils.exe` and `hadoop.dll`) to write checkpoints and save models. Download them into `C:\hadoop\bin`:

```powershell
New-Item -ItemType Directory -Force C:\hadoop\bin
Invoke-WebRequest https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe -OutFile C:\hadoop\bin\winutils.exe
Invoke-WebRequest https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll -OutFile C:\hadoop\bin\hadoop.dll
```

You only need to do this once per laptop.

#### The Spark environment block

Every PowerShell window that runs a Spark script (`spark_streaming.py`, `train_classifier.py`, `batch_classification.py`) needs these variables. **They only last for that window**, so paste this block every time you open a new window to run Spark. Run it from the project root with the venv activated:

```powershell
.\.venv\Scripts\Activate.ps1
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
$env:SPARK_LOCAL_HOSTNAME = "localhost"
$env:STREAM_CHECKPOINT = "$PWD\checkpoints\entities"
```

What each line does:
- `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` make Spark's Python workers use the venv Python, which is where spaCy is installed.
- `HADOOP_HOME` and `PATH` point Spark to `winutils.exe` and `hadoop.dll`.
- `SPARK_LOCAL_HOSTNAME` stops Spark from binding to a VPN, WSL, or Docker network adapter.
- `STREAM_CHECKPOINT` is **required** by `spark_streaming.py`. It's where the NER query saves its state, and the script fails with `KeyError: 'STREAM_CHECKPOINT'` if it isn't set.

### Step 3: Start Kafka

Make sure Docker Desktop is running. Start a single Kafka broker. The container **must** be named `kafka`, because Logstash attaches to it by name.

```powershell
docker run -d --name kafka -p 9092:9092 apache/kafka:4.4.0
```

Wait about 10 seconds for it to start, then create the two topics:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic2
```

Check that both topics exist:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

You should see `topic1` and `topic2`.

### Step 4: Start Elasticsearch, Kibana, and Logstash

Run these in order from the project root.

**4a.** Start Elasticsearch and Kibana. This also creates the Docker network `big_data_project_1_default`.

```powershell
docker compose up -d elasticsearch kibana
```

**4b.** Connect the Kafka container to that network so Logstash can reach Elasticsearch (the reason is explained below):

```powershell
docker network connect big_data_project_1_default kafka
```

If you cloned into a folder with a different name, find the right network name with `docker network ls` (it ends in `_default`) and use that instead. If Docker says the container is already connected, that's fine.

**4c.** Start Logstash:

```powershell
docker compose up -d logstash
```

**4d.** Check that everything is up:

```powershell
docker ps
curl.exe http://localhost:9200
```

`docker ps` should list `kafka`, `elasticsearch`, `kibana`, and `logstash`. The `curl.exe` command should return some JSON with `"tagline" : "You Know, for Search"`. Use `curl.exe`, not `curl`, because in PowerShell `curl` is an alias for a different command. Kibana takes 1 to 2 minutes to start. Then open http://localhost:5601 in a browser. No login is needed because security is turned off for local use.

<details>
<summary><b>Why the Logstash networking works this way</b></summary>

The Kafka container advertises itself as `localhost:9092`. Clients connect to the bootstrap address and are then told to use `localhost:9092`. That works for the producer and Spark on your machine because of the `-p 9092:9092` port mapping. Inside a normal Logstash container, though, `localhost` would mean the Logstash container itself.

To avoid changing the Kafka configuration, `docker-compose.yml` runs Logstash with `network_mode: "container:kafka"`. Logstash then shares Kafka's network, so `localhost:9092` reaches the broker. Because Logstash also uses Kafka's DNS, the Kafka container has to be on the Compose network (step 4b) for `http://elasticsearch:9200` to resolve. For the same reason, `kafka` must be running before Logstash starts.
</details>

### Step 5: Run the Hacker News producer (Terminal 1)

In your current PowerShell window (project root, venv activated):

```powershell
python producer.py
```

What you'll see:
- `Previously saved messages: 1414` (or similar). The repo already includes the messages we collected in `data/hackernews_messages.jsonl`.
- Because that file already has more than 1,200 messages, the producer skips the initial collection and goes straight to checking for new Hacker News items every 30 seconds, printing `New messages published: N`.

New items appear on Hacker News every few seconds, so `topic1` fills up quickly. **Leave this window running.**

> **Optional: collect a fresh set of 1,200 messages right away.** Stop the producer (`Ctrl+C`), move the saved file out of the way, and start it again:
> ```powershell
> Rename-Item data\hackernews_messages.jsonl hackernews_messages_ours.jsonl
> python producer.py
> ```
> Note that `batch_classification.py` reads `data/hackernews_messages.jsonl`, so it would then classify your new messages instead of ours.

### Step 6: Run the Spark streaming job (Terminal 2)

Open a **second** PowerShell window and go to the project folder:

```powershell
cd $HOME\Documents\big_data_project_1
```

Paste the [Spark environment block](#the-spark-environment-block) from Step 2:

```powershell
.\.venv\Scripts\Activate.ps1
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
$env:SPARK_LOCAL_HOSTNAME = "localhost"
$env:STREAM_CHECKPOINT = "$PWD\checkpoints\entities"
```

Start the job with the Spark Kafka connector:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 spark_streaming.py
```

The first run takes a minute or two longer because Spark downloads the Kafka connector. Then, every 30 seconds, you'll see two outputs:

1. **Named entities** (query 1). These counts are also sent to `topic2`, which feeds Elasticsearch:
   ```
   Batch 3: Top 10 named entities
   +---------+-----+
   |entity   |count|
   +---------+-----+
   |Google   |14   |
   ...
   ```
2. **Live classification counts** (query 2):
   ```
   +---------------+-----+
   |predicted_class|count|
   +---------------+-----+
   |Sci/Tech       |...  |
   |Business       |...  |
   ...
   ```

**Leave this window running.** Press `Ctrl+C` to stop it.

> Run `spark-submit` from the **project root**. The script loads the model from the relative path `models/ag_news_pipeline`.

### Step 7: Build the Kibana dashboard

After the Spark job has been running for a minute or so, check that Elasticsearch has data:

```powershell
curl.exe "http://localhost:9200/hackernews-entities/_search?size=10&sort=count:desc&pretty"
```

Then build the chart in Kibana. It's saved inside Kibana's storage, not in the repo, so it has to be created on each new machine.

1. Open http://localhost:5601.
2. Open the menu (☰), then go to **Stack Management → Data Views → Create data view**.
   - Name: `hackernews-entities`
   - Index pattern: `hackernews-entities`
   - Timestamp field: **"I don't want to use the time filter"**
   - Click **Save data view to Kibana**.
3. Open the menu (☰), then go to **Visualize Library → Create visualization → Lens**.
4. Pick the `hackernews-entities` data view in the top left, and choose a **Bar** chart.
5. Horizontal axis: **Top values** of `entity.keyword`, number of values **10**.
6. Vertical axis: **Maximum** of `count`.
7. Click **Save**, and add the chart to a new dashboard.

The counts keep updating, so click **Refresh** on the dashboard (or set auto-refresh to 30 seconds) to see the current top 10.

---

## Part 2: Classification Scripts

The trained model (`models/ag_news_pipeline`), the training data, and all outputs are already in the repo, so **you don't need to run these to see the results**. Look in `outputs/`. They're here if you want to reproduce them.

Run them in a PowerShell window where you've pasted the [Spark environment block](#the-spark-environment-block). Kafka and Docker are **not** needed for these. Use `python`, not `spark-submit`.

**1. (Optional) Re-download AG News.** The file is already in `data/`.

```powershell
python download_ag_news.py
```

Prints the article count per class (30,000 each) and saves `data/ag_news_train.parquet`.

**2. (Optional) Retrain the model.** This overwrites `models/ag_news_pipeline` and takes a few minutes.

```powershell
python train_classifier.py
```

**3. Classify the collected Hacker News messages.**

> ⚠️ **Warning:** By default this **overwrites `outputs/manual_labels_50.csv`** with a new sample whose `manual_label` column is empty, which erases our hand labels. To keep them, write the results to a different folder:

```powershell
python batch_classification.py --output-dir outputs_rerun
```

This prints how many messages were removed during cleaning (empty or duplicate text), the predicted count per class, and five examples per class. It saves `predictions.csv` and `manual_labels_50.csv` in the output folder.

**4. Evaluate against the hand labels.**

```powershell
python evaluate_classifier.py
```

This reads `outputs/manual_labels_50.csv`, where our labels are filled in, and writes `manual_evaluation.csv`, `confusion_matrix.csv`, and `evaluation_summary.txt` to `outputs/`. Messages labeled `None of these` (the message doesn't fit any of the four categories) are excluded from the accuracy.

To evaluate a fresh sample instead, fill in the `manual_label` column of `outputs_rerun/manual_labels_50.csv` with one of `World`, `Sports`, `Business`, `Sci/Tech`, or `None of these`, then run:

```powershell
python evaluate_classifier.py --input outputs_rerun/manual_labels_50.csv --output-dir outputs_rerun
```

---

## Checking That Everything Works

Run these from any PowerShell window.

```powershell
# All four containers running?
docker ps

# Messages from the producer in topic1
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning --max-messages 5

# Entity counts from Spark in topic2 (with keys)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning --max-messages 5 --property print.key=true

# Logstash indexing events (Ctrl+C to stop watching)
docker logs -f logstash

# Elasticsearch index exists and has documents
curl.exe "http://localhost:9200/_cat/indices/hackernews-entities?v"

# Top 10 entities in Elasticsearch
curl.exe "http://localhost:9200/hackernews-entities/_search?size=10&sort=count:desc&pretty"
```

---

## Stopping and Restarting

**Stop everything:**

1. Press `Ctrl+C` in the producer window and in the Spark window.
2. Stop the containers:
   ```powershell
   docker compose stop
   docker stop kafka
   ```

**Start again later.** The order matters: Kafka first, then Logstash.

```powershell
docker start kafka
docker compose up -d elasticsearch kibana
docker compose up -d logstash
```

Then repeat Step 5 (producer) and Step 6 (Spark, including the environment block). Don't run `docker run ... kafka` again, because the container already exists.

**Reset the entity counts.** Spark keeps running counts in its checkpoint. To start from zero, stop the Spark job and run:

```powershell
Remove-Item -Recurse -Force .\checkpoints
```

**Remove everything (containers and Elasticsearch data):**

```powershell
docker compose down -v
docker rm -f kafka
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `KeyError: 'STREAM_CHECKPOINT'` | You didn't paste the [Spark environment block](#the-spark-environment-block) in this window. Paste it and run `spark-submit` again. |
| `Python worker failed to connect back` or `No module named 'spacy'` | Spark is using a different Python than the venv. Activate the venv **before** setting `PYSPARK_PYTHON`, then paste the environment block again. Make sure `python -m spacy download en_core_web_sm` was run inside the venv. |
| `No module named 'numpy'` | Run `pip install numpy` inside the venv. |
| `Could not locate executable null\bin\winutils.exe`, `HADOOP_HOME ... unset`, or `UnsatisfiedLinkError ... NativeIO$Windows` | Do Step 2 again and check that `C:\hadoop\bin\winutils.exe` and `C:\hadoop\bin\hadoop.dll` exist. If it still fails, copy `hadoop.dll` into `C:\Windows\System32`. |
| `Service 'sparkDriver' could not bind` or a strange hostname or IP in the Spark logs | Make sure `$env:SPARK_LOCAL_HOSTNAME = "localhost"` is set. If that's not enough, also run `$env:SPARK_LOCAL_IP = "127.0.0.1"`. |
| `Failed to find data source: kafka` | The `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` part is missing from `spark-submit`, or its version doesn't match the installed PySpark. |
| `spark-submit` is not recognized | The venv isn't activated. Run `.\.venv\Scripts\Activate.ps1`. |
| `java` not recognized, or `JAVA_HOME is not set` | Install JDK 21 with the "Set JAVA_HOME" option and open a new PowerShell window. |
| Spark errors mentioning offsets, `failOnDataLoss`, or partitions that changed | The checkpoint is from another Kafka instance. Stop Spark, run `Remove-Item -Recurse -Force .\checkpoints`, and start it again. |
| `Path does not exist: .../models/ag_news_pipeline` | You ran `spark-submit` from the wrong folder. `cd` into the project root first. |
| Producer hangs or shows `NoBrokersAvailable` | Kafka isn't running. Check `docker ps`, and if needed run `docker start kafka`. |
| Logstash won't start, with an error mentioning `container:kafka` | Kafka must be running first. Run `docker start kafka`, then `docker compose up -d logstash`. |
| `docker logs logstash` shows `elasticsearch: Name or service not known` | Kafka isn't on the Compose network. Run `docker network connect big_data_project_1_default kafka`, then `docker compose restart logstash`. |
| You deleted and recreated the Kafka container | Reconnect it to the network (Step 4b), then run `docker compose up -d --force-recreate logstash`. Logstash was attached to the old container. |
| Elasticsearch container keeps exiting | Give Docker Desktop more memory (Settings → Resources, 4 GB or more). |
| Kibana shows "Kibana server is not ready yet" | Wait 1 to 2 minutes and refresh. |
| `evaluate_classifier.py` says `Missing or invalid manual labels` | `manual_labels_50.csv` was overwritten by `batch_classification.py`. Restore ours with `git checkout -- outputs/manual_labels_50.csv`, or fill in the labels yourself. |

---

## macOS / Linux differences

Everything is the same except for these points:

- **Skip Step 2**. winutils is only needed on Windows.
- Use `python3` if `python` doesn't point to Python 3.13.
- Activate the venv with `source .venv/bin/activate`.
- Delete folders with `rm -rf checkpoints`.
- Use plain `curl` instead of `curl.exe`.
- The Spark environment block becomes:
  ```bash
  source .venv/bin/activate
  export PYSPARK_PYTHON="$(which python)"
  export PYSPARK_DRIVER_PYTHON="$(which python)"
  export SPARK_LOCAL_HOSTNAME=localhost
  export STREAM_CHECKPOINT="$PWD/checkpoints/entities"
  ```
- On Apple Silicon Macs, the Docker images run natively, so no extra setup is needed.
- Spark 4.2 needs Java 17 or newer. If `java -version` fails or shows an older version, install a JDK (for example Java 21) and set `JAVA_HOME` before running `spark-submit`.
- Kafka, Elasticsearch, Kibana and Logstash together use about 2.5 GB of RAM, plus about 1 GB for Spark. Docker Desktop defaults to about 4 GB on an 8 GB Mac, so stop any other running containers first.
- The four container images and the Python packages need about 6 GB of free disk.
- Kibana can sit on its loading screen for a minute the first time it opens. Reload the page if it doesn't finish.

We ran the full Part 1 pipeline on an 8 GB Apple Silicon Mac with these steps for the screenshots in `screenshots/`.

---

## Results

**Part 1.** The streaming job extracts people, organizations, places, and products from live Hacker News messages, and the Kibana dashboard shows the top 10 by count.

**Part 2.** From `outputs/evaluation_summary.txt`:

| Metric | Value |
|---|---|
| Manually labeled messages | 50 |
| Excluded as "None of these" | 14 |
| Messages in the four AG News categories | 36 |
| Correct predictions | 24 |
| **Accuracy on applicable messages** | **66.67%** |

Accuracy is lower than it would be on AG News itself, because the model was trained on formal news articles and Hacker News is mostly short, informal tech discussion. Many messages don't fit any of the four categories, and the model predicts `Sci/Tech` for most of them. See `outputs/evaluation_summary.txt` and `outputs/confusion_matrix.csv` for details.
