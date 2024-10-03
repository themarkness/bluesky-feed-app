import feedparser
from atproto import Client, models
import re
import logging
import os
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import requests
from typing import List, Dict

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Bluesky credentials from environment variables
BSKY_USERNAME = os.getenv('BSKY_USERNAME')
BSKY_PASSWORD = os.getenv('BSKY_PASSWORD')

# RSS feed URLs with human-readable names
RSS_FEEDS = {
    "Review Commons": "https://labs.sciety.org/lists/by-id/f3dbc188-e891-4586-b267-c99cf3b3808e/atom.xml",
    "PREreview": "https://labs.sciety.org/lists/by-id/5c2e4b99-f5f0-4145-8c87-cadd7a41a1b1/atom.xml",
    "preLights": "https://labs.sciety.org/lists/by-id/f4b96b8b-db49-4b41-9c5b-28d66a83cd70/atom.xml",
    "Rapid Reviews Infectious Diseases": "https://labs.sciety.org/lists/by-id/f3dbc188-e891-4586-b267-c99cf3b3808e/atom.xml",
    "Arcadia Science": "https://labs.sciety.org/lists/by-id/f8459240-f79c-4bb2-bb55-b43eae25e4f6/atom.xml",
    "PCI Ecology": "https://labs.sciety.org/lists/by-id/65f661e6-73f9-43e9-9ae6-a84635afb79a/atom.xml",
    "PCI Archaeology": "https://labs.sciety.org/lists/by-id/24a60cf9-5f45-43f2-beaf-04139e6f0a0e/atom.xml",
    "PCI Evolutionary Biology": "https://labs.sciety.org/lists/by-id/3d69f9e5-6fd2-4266-9cf8-c069bca79617/atom.xml",
    "PCI Animal Science": "https://labs.sciety.org/lists/by-id/e764d90c-ffea-4b0e-a63e-d2b5236aa1ed/atom.xml",
    "PCI Zoology": "https://labs.sciety.org/lists/by-id/a4d57b30-b41c-4c9d-81f0-dccd4cd1d099/atom.xml",
    "PCI Paleontology": "https://labs.sciety.org/lists/by-id/dd9d166f-6d25-432c-a60f-6df33ca86897/atom.xml",
    "Gigascience": "https://labs.sciety.org/lists/by-id/5498e813-ddad-414d-88df-d1f84696cecd/atom.xml",
    "Gigabyte": "https://labs.sciety.org/lists/by-id/794cb0bd-f784-4b58-afde-7427faced494/atom.xml",
}

# Define UTM parameters
utm_source = "bluesky"
utm_medium = "social"
utm_campaign = "preprint_review"


# File to store the last posted entry IDs
LAST_POSTED_FILE = "last_posted_multi.json"

# Initialize Bluesky client
client = Client()
client.login(BSKY_USERNAME, BSKY_PASSWORD)

def resolve_handle_to_did(handle):
    try:
        response = client.com.atproto.identity.resolve_handle({'handle': handle})
        logging.debug(f"Resolved handle {handle} to DID: {response.did}")
        return response.did
    except Exception as e:
        logging.error(f"Error resolving handle {handle}: {str(e)}")
        return None

def parse_mentions(text: str) -> List[Dict]:
    spans = []
    mention_regex = rb"[$|\W](@([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(mention_regex, text_bytes):
        spans.append({
            "start": m.start(1),
            "end": m.end(1),
            "handle": m.group(1)[1:].decode("UTF-8")
        })
    return spans

def parse_urls(text: str) -> List[Dict]:
    spans = []
    url_regex = rb"[$|\W](https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*[-a-zA-Z0-9@%_\+~#//=])?)"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(url_regex, text_bytes):
        spans.append({
            "start": m.start(1),
            "end": m.end(1),
            "url": m.group(1).decode("UTF-8"),
        })
    return spans

def parse_facets(text: str) -> List[Dict]:
    facets = []
    for m in parse_mentions(text):
        resp = requests.get(
            "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
            params={"handle": m["handle"]},
        )
        if resp.status_code == 400:
            continue
        did = resp.json()["did"]
        facets.append({
            "index": {
                "byteStart": m["start"],
                "byteEnd": m["end"],
            },
            "features": [{"$type": "app.bsky.richtext.facet#mention", "did": did}],
        })
    for u in parse_urls(text):
        facets.append({
            "index": {
                "byteStart": u["start"],
                "byteEnd": u["end"],
            },
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": u["url"],
                }
            ],
        })
    return facets

import unicodedata

def trim_to_graphemes(text, limit=300):
    graphemes = []
    for char in text:
        grapheme = unicodedata.normalize("NFC", char)
        graphemes.append(grapheme)
        if len(graphemes) >= limit:
            break
    return ''.join(graphemes)

def post_to_bluesky(entry, feed_name, feed_url, last_posted_ids):
    # Remove HTML tags from the title
    title = re.sub(r'<[^>]+>', '', entry.title)
    link = entry.link.replace("?utm_source=sciety_labs_atom_feed", "")
    
    # Trim content to fit within 300 graphemes
    utm_link = f"{link}?utm_source={utm_source}&utm_medium={utm_medium}&utm_campaign={utm_campaign}"
    
    mentions = {
        "PREreview": "prereview.bsky.social",
        "Gigabyte": "gigabytejournal.bsky.social",
        "Gigascience": "gigascience.bsky.social",
        "PCI Archaeology": "pciarchaeology.bsky.social",
        "PCI Animal Science": "pci-animsci.bsky.social",
        "Arcadia Science": "arcadiascience.bsky.social",
    }
    
    mention_handle = mentions.get(feed_name, feed_name)
    content = f"💬 New preprint evaluation by @{mention_handle} of\n\n{title}\n\n{utm_link}\n\n#PreprintEvaluation"
    
    # Ensure the content does not exceed 300 graphemes
    content = trim_to_graphemes(content, limit=300)
    
    # Parse facets from the content
    facets = parse_facets(content)
    
    # Extract the first line of the abstract
    abstract = entry.get('summary', '')
    abstract = re.sub(r'<[^>]+>', '', abstract)  # Remove any HTML tags
    first_line = abstract.split('.')[0].strip()  # Get the first sentence
    description = first_line[:300] if first_line else "Read the review on Sciety.org"  # Truncate if too long
    
    # Create external link embed
    external = models.AppBskyEmbedExternal.Main(
        external=models.AppBskyEmbedExternal.External(
            title=title,
            description=description,
            uri=utm_link
        )
    )
    
    # Log the content and facets before sending the post
    logging.info(f"Attempting to post content: {content}")
    logging.info(f"Facets: {json.dumps(facets, indent=2)}")

    # Send post with facets and external embed
    try:
        response = client.send_post(text=content, facets=facets, embed=external)
        logging.info(f"Posted to Bluesky: {feed_name} - {title}")
        logging.debug(f"Post response: {response}")
    except Exception as e:
        logging.error(f"Error posting to Bluesky: {str(e)}")
        return last_posted_ids
    
    # Update last_posted_ids
    if feed_url not in last_posted_ids:
        last_posted_ids[feed_url] = []
    elif isinstance(last_posted_ids[feed_url], str):
        last_posted_ids[feed_url] = [last_posted_ids[feed_url]]
    last_posted_ids[feed_url].append(entry.id)

    return last_posted_ids


def get_new_entries(feed_url, last_posted_ids, start_date, end_date):
    feed = feedparser.parse(feed_url)
    new_entries = []
    
    for entry in feed.entries:
        pub_date = datetime(*entry.published_parsed[:6])
        
        # Check if the entry is within the date range
        if start_date <= pub_date <= end_date:
            # Check if we've already posted this entry
            if entry.id not in last_posted_ids.get(feed_url, []):
                new_entries.append(entry)
        elif pub_date < start_date:
            # Stop checking if we've reached entries older than the start date
            break
    
    return new_entries[::-1]  # Reverse to post oldest first

def get_last_posted_ids():
    try:
        with open(LAST_POSTED_FILE, 'r') as f:
            data = json.load(f)
        # Ensure all values are lists
        for key in data:
            if not isinstance(data[key], list):
                data[key] = [data[key]]
        return data
    except FileNotFoundError:
        return {}

def save_last_posted_ids(last_posted_ids):
    with open(LAST_POSTED_FILE, 'w') as f:
        json.dump(last_posted_ids, f)
    logging.info("Saved last posted IDs")

def main():
    last_posted_ids = get_last_posted_ids()
    
    # Define the date range (e.g., last 7 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    for feed_name, feed_url in RSS_FEEDS.items():
        try:
            logging.info(f"Processing feed: {feed_name}")
            new_entries = get_new_entries(feed_url, last_posted_ids, start_date, end_date)
            logging.info(f"Found {len(new_entries)} new entries")
            
            if new_entries:
                for entry in new_entries:
                    logging.info(f"Attempting to post: {entry.title}")
                    last_posted_ids = post_to_bluesky(entry, feed_name, feed_url, last_posted_ids)
                    
                    # Add a delay between posts to avoid rate limiting
                    time.sleep(5)
                
                logging.info(f"Posted {len(new_entries)} new entries from {feed_name}")
            else:
                logging.info(f"No new entries to post from {feed_name}")
        except Exception as e:
            logging.error(f"Error processing {feed_name}: {str(e)}")
    
    save_last_posted_ids(last_posted_ids)

if __name__ == "__main__":
    logging.info("Multi-feed bot started")
    main()
    logging.info("Multi-feed bot finished")