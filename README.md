# Spark Streaming with Kafka and Text Classification with Spark MLlib

CS4371 Group Project 1, UT Dallas

This project has two parts. Part 1 builds a real-time pipeline that pulls new stories from Hacker News, extracts named entities from them with spaCy inside a PySpark Structured Streaming job, and shows the most mentioned entities in Kibana. Part 2 will train a text classifier with Spark MLlib on the AG News dataset. Part 1 is implemented and this README explains how to run it. Part 2 is not done yet (see the last section).

## Part 1 Overview

The data flows through the pipeline like this:

```
Hacker News API -> producer.py -> Kafka topic1 -> spark_streaming.py (PySpark + spaCy NER) -> Kafka topic2 -> Logstash -> Elasticsearch -> Kibana
```

1. **Producer (`producer.py`)**: Calls the Hacker News API every 30 seconds, looks at the newest 100 story IDs, and fetches each story it hasn't sent yet. For every story with a title it publishes a JSON message to `topic1` with the fields `id`, `text` (the story title), and `time`. Sent IDs are kept in memory while the script runs so the same story isn't sent twice. The Hacker News API is public, so no API key is needed.

2. **Spark streaming job (`spark_streaming.py`)**: Reads JSON messages from `topic1` using Structured Streaming. A Python UDF runs spaCy's `en_core_web_sm` model on each title and keeps entities labeled `PERSON`, `ORG`, `GPE`, `LOC`, and `PRODUCT` (people, organizations, places, and products). The entities are grouped and Spark keeps a running count for each one. Every 30 seconds the job prints the top 10 entities to the console and writes all current counts to `topic2` as JSON (`{"entity": ..., "count": ...}`), with the entity name as the Kafka message key.

3. **Logstash (`logstash/logstash.conf`)**: Reads the JSON messages from `topic2`, converts `count` to an integer, and indexes each message into the Elasticsearch index `hackernews-entities`. The document ID is set to the entity name, so each entity has one document that gets overwritten with its latest count instead of piling up duplicates.

4. **Kibana**: We made a Lens bar chart and a dashboard that show the top 10 named entities, using `entity.keyword` for the categories and Maximum of `count` for the values.

## Technologies Used

| Component | Version we used |
|---|---|
| Windows | 11 |
| Python | 3.13 |
| PySpark | 4.2.0 |
| Java | 21 |
| Apache Kafka (`apache/kafka` Docker image) | 4.4.0 |
| spaCy with `en_core_web_sm` | 3.8 |
| kafka-python | 3.0 |
| requests | 2.32 |
| Elasticsearch | 9.2.0 |
| Kibana | 9.2.0 |
| Logstash | 9.2.0 |
| Docker Desktop | |

## Project Structure

```
.
├── producer.py            # Hacker News producer, publishes to topic1
├── spark_streaming.py     # PySpark Structured Streaming + spaCy NER, publishes to topic2
├── docker-compose.yml     # Elasticsearch, Kibana, and Logstash containers
├── logstash/
│   └── logstash.conf      # Logstash pipeline: topic2 -> Elasticsearch
└── checkpoints/           # Spark streaming checkpoint data (created when the job runs)
```

## Prerequisites

You need the following installed:

- Docker Desktop (running)
- Python 3.13
- Java 21 (Spark needs `JAVA_HOME` set or `java` on the PATH)
- Hadoop `winutils.exe` and `hadoop.dll` for Windows (needed by Spark for checkpointing on Windows, see Troubleshooting)

Install the Python packages and the spaCy model. Run these from the project root in PowerShell:

```powershell
pip install pyspark==4.2.0 kafka-python requests spacy
python -m spacy download en_core_web_sm
```

Check that Java is available:

```powershell
java -version
```

## 1. Start Kafka

Kafka runs on its own container from the official Apache Kafka image, separate from Docker Compose. With the image's default settings it runs a single broker that listens on port 9092 and advertises itself as `localhost:9092`.

```powershell
docker run -d --name kafka -p 9092:9092 apache/kafka:4.4.0
```

The container name has to be `kafka`, because the Logstash service in `docker-compose.yml` attaches to it by that name.

Create the two topics:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic2
```

If you stopped the container earlier, start it again with `docker start kafka` instead of running `docker run` again.

## 2. Run the Hacker News Producer

Open a new PowerShell window in the project root and run:

```powershell
python producer.py
```

By default it connects to Kafka at `localhost:9092`. To use a different broker, set `KAFKA_BOOTSTRAP_SERVERS` first:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python producer.py
```

You should see a `Sent: <story title>` line for every story, then `Checking again in 30 seconds...`. Press `Ctrl+C` to stop it.

## 3. Run the Spark Streaming Job

Open another PowerShell window in the project root. Spark on Windows needs a few environment variables. These only last for the current PowerShell session, so set them each time you open a new window.

```powershell
# Make Spark's Python workers use the same Python that has spaCy installed
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source

# Folder that contains bin\winutils.exe and bin\hadoop.dll (change this to wherever you put it)
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

# Avoid Spark picking up the wrong network hostname on Windows
$env:SPARK_LOCAL_HOSTNAME = "localhost"

# Where Spark stores its streaming checkpoint (required, the script fails without it)
$env:STREAM_CHECKPOINT = "$PWD\checkpoints\entities"
```

Then submit the job with the Kafka connector for Spark 4.2.0 (Scala 2.13):

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 spark_streaming.py
```

The first run takes a bit longer because Spark downloads the connector. After that, every 30 seconds you should see output like `Batch N: Top 10 named entities` followed by a table of entities and counts. The same counts are written to `topic2`.

### How the checkpoint path works

`spark_streaming.py` reads the checkpoint location from the `STREAM_CHECKPOINT` environment variable (`os.environ["STREAM_CHECKPOINT"]`). There is no default value, so if the variable isn't set the script stops with a `KeyError`. The checkpoint stores the Kafka offsets that have already been processed and the running entity counts, so if you stop and restart the job with the same checkpoint it continues from where it left off and keeps the old counts.

To start over with fresh counts, stop the job and delete the checkpoint folder:

```powershell
Remove-Item -Recurse -Force .\checkpoints\entities
```

The job reads `topic1` with `startingOffsets` set to `earliest`, but Spark only uses that setting when there is no existing checkpoint. With a new checkpoint it processes everything already in `topic1`.

Spark also reads `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), same as the producer.

## 4. Start Elasticsearch, Kibana, and Logstash

### How the Logstash networking is set up

This part depends on how Kafka was started, so it is worth explaining. The Kafka container advertises itself as `localhost:9092`. A client connects to the bootstrap address and is then told to connect to `localhost:9092`. For the producer and Spark on the host this works because of the `-p 9092:9092` port mapping. But inside a normal Logstash container, `localhost` means the Logstash container itself, so it would never reach Kafka.

To get around this without changing the Kafka configuration, `docker-compose.yml` runs Logstash with:

```yaml
network_mode: "container:kafka"
```

This makes Logstash share the Kafka container's network, so `localhost:9092` in `logstash.conf` reaches the broker. The catch is that Logstash then also uses Kafka's DNS, so to resolve `http://elasticsearch:9200` the Kafka container has to be connected to the Docker Compose network. This also means the `kafka` container must already be running before the Logstash container starts.

### Startup order

Run these from the project root, with the `kafka` container already running.

Start Elasticsearch and Kibana first. This also creates the Compose network:

```powershell
docker compose up -d elasticsearch kibana
```

Connect the Kafka container to the Compose network. Docker Compose names the network after the project folder, so check the exact name first:

```powershell
docker network ls
```

Look for the network ending in `_default` that matches the project folder name. For our folder, `big_data_project_1`, it is `big_data_project_1_default`:

```powershell
docker network connect big_data_project_1_default kafka
```

You only need to do this once per Kafka container. If it's already connected, Docker will give an error saying so, which you can ignore.

Now start Logstash:

```powershell
docker compose up -d logstash
```

Elasticsearch is available at http://localhost:9200 and Kibana at http://localhost:5601. Security is turned off in `docker-compose.yml` (`xpack.security.enabled=false`), so no login is needed. This is fine for running locally but should not be used for anything public. Kibana can take a minute or two to finish starting.

Logstash prints every event it indexes (the `stdout` output with `rubydebug`), so you can watch it with:

```powershell
docker logs -f logstash
```

## 5. Verify the Pipeline

### Kafka topics

List the topics:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Read a few messages from `topic1` (story titles from the producer):

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning --max-messages 5
```

Read a few messages from `topic2` (entity counts from Spark), with the keys:

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning --max-messages 5 --property print.key=true
```

### Elasticsearch index

In PowerShell, `curl` can be an alias for `Invoke-WebRequest`, so these commands use `curl.exe` to call the real curl that ships with Windows.

Check that the index exists and has documents:

```powershell
curl.exe "http://localhost:9200/_cat/indices/hackernews-entities?v"
```

Show the 10 entities with the highest counts:

```powershell
curl.exe "http://localhost:9200/hackernews-entities/_search?size=10&sort=count:desc&pretty"
```

### Kibana dashboard

1. Open http://localhost:5601.
2. Go to **Stack Management > Data Views** and create a data view with the index pattern `hackernews-entities`. There is no time field, so choose to not use one.
3. Go to **Visualize Library**, create a **Lens** visualization, and pick the `hackernews-entities` data view.
4. Choose a bar chart. Set the horizontal axis to **Top values** of `entity.keyword` with size 10, and the vertical axis to **Maximum** of `count`.
5. Save the visualization and add it to a dashboard.

Since the counts keep updating, refresh the dashboard (or set an auto-refresh interval) to see the latest top 10.

## Troubleshooting

**`KeyError: 'STREAM_CHECKPOINT'`**
The checkpoint variable isn't set in the current PowerShell window. Set `$env:STREAM_CHECKPOINT` as shown in step 3 and run `spark-submit` again from the same window.

**`Python worker failed to connect back` or `ModuleNotFoundError: No module named 'spacy'`**
Spark is starting its Python workers with a different Python than the one where spaCy is installed. Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the same interpreter you used for `pip install`. Also make sure `python -m spacy download en_core_web_sm` was run with that same interpreter.

**`Could not locate executable null\bin\winutils.exe`, `HADOOP_HOME and hadoop.home.dir are unset`, or `UnsatisfiedLinkError ... NativeIO$Windows`**
Spark uses Hadoop's file system code for checkpoints, which needs native helpers on Windows. Download `winutils.exe` and `hadoop.dll` built for a Hadoop 3.x version, put them in a `bin` folder (for example `C:\hadoop\bin`), set `HADOOP_HOME` to the parent folder, and add `%HADOOP_HOME%\bin` to `PATH`. If it still fails, copying `hadoop.dll` into `C:\Windows\System32` is a common fix.

**Spark hostname or binding errors (for example `Service 'sparkDriver' could not bind` or the driver using a strange hostname or IP)**
This can happen on Windows when the machine has several network adapters (VPN, WSL, Docker). Set `$env:SPARK_LOCAL_HOSTNAME = "localhost"` before running `spark-submit`. If that doesn't help, also try `$env:SPARK_LOCAL_IP = "127.0.0.1"`.

**`Failed to find data source: kafka`**
The Kafka connector wasn't loaded. Make sure the `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` option is in the `spark-submit` command and that its version matches the installed PySpark version.

**`spark-submit` is not recognized**
The pip install of PySpark puts `spark-submit` in Python's `Scripts` folder. Make sure that folder is on your `PATH`, or run it from the same environment where PySpark is installed.

**Logstash fails to start with an error about `container:kafka`**
The `kafka` container has to be running before Logstash starts because Logstash uses its network. Run `docker start kafka`, then `docker compose up -d logstash`.

**Logstash can't reach Elasticsearch (`elasticsearch: Name or service not known` or connection errors in `docker logs logstash`)**
The Kafka container isn't connected to the Compose network. Run `docker network connect <project>_default kafka` (see step 4), then `docker compose restart logstash`.

**Logstash can't connect to Kafka, or the producer hangs or times out**
Check that the Kafka container is running (`docker ps`) and that port 9092 is mapped. Logstash connects through `localhost:9092` inside Kafka's network, so it only works when `network_mode: "container:kafka"` is in place. If you recreate the Kafka container, also recreate Logstash (`docker compose up -d --force-recreate logstash`) because it was attached to the old container's network, and reconnect the new Kafka container to the Compose network.

**Counts seem too high after restarting**
The producer only remembers sent story IDs while it is running, so restarting it sends the newest stories again. Spark's checkpoint also keeps the old counts. Delete the checkpoint folder (step 3) to reset the counts.

## Setup Notes

A few things aren't in the repository yet that would make this easier for someone else to reproduce:

- There is no `requirements.txt`. The packages are listed in the Prerequisites section instead.
- The Kafka container is started with `docker run` and isn't part of `docker-compose.yml`, so it has to be started and connected to the Compose network by hand (steps 1 and 4).
- The Compose network name depends on the folder name of the project, so the `docker network connect` command may need a different network name on another machine.
- The Kibana data view, Lens visualization, and dashboard are saved inside Kibana (in the Elasticsearch data volume), not exported into the repository. They need to be created again on a new machine using the steps in section 5.
- `winutils.exe` and `hadoop.dll` are not included and have to be downloaded separately.
- The `checkpoints/` folder contains local Spark state and should not be submitted or committed. There is no `.gitignore` for it yet.

## Current Status

**Done (Part 1):**

- Hacker News producer publishing new stories to Kafka `topic1`
- PySpark Structured Streaming job reading `topic1`, extracting named entities with spaCy, keeping running counts, and writing them to `topic2`
- Logstash pipeline loading the counts from `topic2` into the Elasticsearch index `hackernews-entities`
- Kibana Lens visualization and dashboard showing the top 10 named entities

**Remaining (Part 2):**

Part 2 has not been started. The plan is to use the AG News dataset and Spark MLlib to build a text classification model that sorts news articles into four categories: World, Sports, Business, and Sci/Tech.
