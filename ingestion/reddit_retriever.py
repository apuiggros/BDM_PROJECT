import json
import time
import urllib.parse
from pathlib import Path

import requests

FIGURES = ["Plato", "Nietzsche", "Kant", "Aristotle"]

OUTPUT_PATH = Path("data/samples/reddit_philosophy_mentions.json")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "BDMProject/0.1 academic demo by u/example"
}


def search_reddit(query, limit=2):
    encoded_query = urllib.parse.quote(query)
    url = f"https://www.reddit.com/search.json?q={encoded_query}&sort=new&limit={limit}"

    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()

    data = response.json()
    posts = []

    for child in data.get("data", {}).get("children", []):
        post = child.get("data", {})

        posts.append({
            "source": "reddit",
            "subreddit": post.get("subreddit"),
            "title": post.get("title"),
            "author": post.get("author"),
            "mentions": query,
            "score": post.get("score"),
            "num_comments": post.get("num_comments"),
            "url": f"https://www.reddit.com{post.get('permalink')}",
            "created_utc": post.get("created_utc")
        })

    return posts


def main():
    all_posts = []

    for figure in FIGURES:
        print(f"Searching Reddit for: {figure}")
        posts = search_reddit(figure, limit=2)
        all_posts.extend(posts)
        time.sleep(2)

    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(all_posts, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(all_posts)} Reddit posts to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()