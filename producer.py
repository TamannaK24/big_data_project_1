
import requests

#hacker news api urls
base_url = "https://hacker-news.firebaseio.com/v0"
stories_url = f"{base_url}/newstories.json"

#get the newest story ids
response = requests.get(stories_url, timeout=15)
response.raise_for_status()
story_ids = response.json()

#print the first 10 stories
for story_id in story_ids[:10]:
    url = f"{base_url}/item/{story_id}.json"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    story = response.json()

    if story and story.get("title"):
        print(story["title"])