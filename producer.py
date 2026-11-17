
import requests
import json
import time
import os
from kafka import KafkaProducer

#api and kafka settings
base_url = "https://hacker-news.firebaseio.com/v0"
kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic = "topic1"

#connect to kafka
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#track stories so we don't send duplicates
seen_stories = set()

print("Starting Hacker News producer...")

try:
    while True:
        try:
            #get latest story ids
            response = requests.get(
                f"{base_url}/newstories.json", timeout=15
            )
            response.raise_for_status()
            story_ids = response.json()

            #check newest 100 stories
            for story_id in reversed(story_ids[:100]):
                if story_id in seen_stories:
                    continue

                url = f"{base_url}/item/{story_id}.json"
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                story = response.json()

                if not story or not story.get("title"):
                    continue

                #prepare data for kafka
                message = {
                    "id": story_id,
                    "text": story["title"],
                    "time": story.get("time")
                }

                #send story to topic1
                producer.send(topic, value=message).get(timeout=15)
                seen_stories.add(story_id)

                print("Sent:", message["text"])

            #check for new stories every 30 seconds
            print("Checking again in 30 seconds...")
            time.sleep(30)

        except requests.RequestException as e:
            print("API error:", e)
            time.sleep(30)

except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    producer.flush()
    producer.close()
