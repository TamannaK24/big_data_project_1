
import json
import os
import re
import time
import html
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser

import requests
from kafka import KafkaProducer


#kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC = "topic1"

#hacker news configuration
API_URL = "https://hacker-news.firebaseio.com/v0"
TARGET_MESSAGES = 1200
POLL_INTERVAL = 30
MAX_WORKERS = 8

#save messages for part 2
DATA_FILE = "data/hackernews_messages.jsonl"

os.makedirs("data", exist_ok=True)


class HTMLCleaner(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def get_text(self):
        return " ".join(self.parts)


def clean_text(raw):
    cleaner = HTMLCleaner()
    cleaner.feed(raw or "")
    text = html.unescape(cleaner.get_text())
    return re.sub(r"\s+", " ", text).strip()


def get_max_item():
    response = requests.get(f"{API_URL}/maxitem.json", timeout=15)
    response.raise_for_status()
    return int(response.json())


def get_item(item_id):
    try:
        response = requests.get(
            f"{API_URL}/item/{item_id}.json",
            timeout=10
        )
        response.raise_for_status()
        item = response.json()

        if not item:
            return None

        if item.get("deleted") or item.get("dead"):
            return None

        item_type = item.get("type")

        if item_type == "story":
            text = clean_text(item.get("title", ""))

        elif item_type == "comment":
            text = clean_text(item.get("text", ""))

        else:
            return None

        if not text:
            return None

        return {
            "id": item["id"],
            "text": text,
            "time": item.get("time", 0)
        }

    except requests.RequestException:
        return None


#load previously saved messages
seen_ids = set()

if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        for line in file:
            try:
                message = json.loads(line)
                seen_ids.add(message["id"])
            except (json.JSONDecodeError, KeyError):
                continue

print(f"Previously saved messages: {len(seen_ids)}")


#connect to kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    retries=5
)


def publish_items(item_ids):
    published = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for message in executor.map(get_item, item_ids):

            if message is None:
                continue

            if message["id"] in seen_ids:
                continue

            #send the message to kafka
            producer.send(TOPIC, value=message)

            #save a copy for later classification
            with open(DATA_FILE, "a", encoding="utf-8") as file:
                file.write(json.dumps(message) + "\n")

            seen_ids.add(message["id"])
            published += 1

            if published % 100 == 0:
                producer.flush()
                print(
                    f"Published {published} messages this batch. "
                    f"Total saved: {len(seen_ids)}"
                )

            if len(seen_ids) >= TARGET_MESSAGES:
                break

    producer.flush()
    return published


try:
    #collect recent stories and comments first
    latest_id = get_max_item()
    print(f"Latest Hacker News item ID: {latest_id}")
    print("Collecting recent stories and comments...")

    current_id = latest_id

    while len(seen_ids) < TARGET_MESSAGES and current_id > 0:
        lower_id = max(1, current_id - 199)

        item_ids = range(current_id, lower_id - 1, -1)
        count = publish_items(item_ids)

        print(
            f"Checked IDs {lower_id} to {current_id}. "
            f"Found {count} new messages. "
            f"Saved: {len(seen_ids)}"
        )

        current_id = lower_id - 1

    print("Initial collection complete!")
    print(f"Total saved messages: {len(seen_ids)}")
    print("Now checking for new Hacker News items...")

    #continue collecting new messages as they appear
    last_checked_id = latest_id

    while True:
        time.sleep(POLL_INTERVAL)

        newest_id = get_max_item()

        if newest_id > last_checked_id:
            item_ids = range(
                last_checked_id + 1,
                newest_id + 1
            )

            count = publish_items(item_ids)

            print(
                f"New messages published: {count}. "
                f"Total saved: {len(seen_ids)}"
            )

            last_checked_id = newest_id
        else:
            print("No new Hacker News items yet.")

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    producer.flush()
    producer.close()
    print(f"Total messages saved: {len(seen_ids)}")
    print(f"Data saved to: {DATA_FILE}")
