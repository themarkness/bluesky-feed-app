import feedparser
from atproto import Client, models
from atproto_client.utils import TextBuilder
import logging
import os
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
from typing import List, Dict

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bluesky_bot.log"),
        logging.StreamHandler()
    ]
)

# Bluesky credentials
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

# Mapping feed names to Bluesky handles
FEED_HANDLES = {
    "PREreview": "prereview.bsky.social",
    "Gigabyte": "gigabytejournal.bsky.social",
    "Gigascience": "gigascience.bsky.social",
    "PCI Archaeology": "pciarchaeology.bsky.social",
    "PCI Animal Science": "pci-animsci.bsky.social",
    "Arcadia Science": "arcadiascience.bsky.social",
    "preLights": "prelights.bsky.social",
    "Review Commons": "reviewcommons.org",
}

UTM_PARAMS = "?utm_source=bluesky&utm_medium=social&utm_campaign=preprint_review"
LAST_POSTED_FILE = "last_posted_multi.json"

def get_client() -> Client:
    client = Client()
    client.login(BSKY_USERNAME, BSKY_PASSWORD)
    return client

def load_last_posted_ids() -> Dict[str, List[str]]:
    if not os.path.exists(LAST_POSTED_FILE):
        return {}
    try:
        with open(LAST_POSTED_FILE, 'r') as f:
            data = json.load(f)
            # Ensure compatibility with both dict and list structures just in case
            processed_data = {}
            for k, v in data.items():
                processed_data[k] = v if isinstance(v, list) else [v]
            return processed_data
    except (json.JSONDecodeError, IOError):
        logging.warning("Failed to load last posted IDs, starting fresh.")
        return {}

def save_last_posted_ids(data: Dict[str, List[str]]):
    try:
        with open(LAST_POSTED_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        logging.error(f"Failed to save last posted IDs: {e}")

def get_new_entries(feed_url: str, posted_ids: List[str], max_age_days: int = 7) -> List:
    feed = feedparser.parse(feed_url)
    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    new_entries = []

    if feed.bozo:
        logging.warning(f"Feed error for {feed_url}: {feed.bozo_exception}")
        return []

    for entry in feed.entries:
        try:
            # Handle different time struct formats if necessary, though parsed usually works
            pub_date = datetime(*entry.published_parsed[:6])
            
            if pub_date >= cutoff_date:
                if entry.id not in posted_ids:
                    new_entries.append(entry)
        except (AttributeError, TypeError):
            continue
            
    # Return reversed to post oldest first
    return new_entries[::-1]

def create_post_text_builder(entry, feed_name: str) -> TextBuilder:
    # Clean title
    title = entry.title
    # Truncate title if too long (leave room for other text)
    if len(title) > 200:
        title = title[:197] + "..."
    
    # Clean link
    base_link = entry.link.split('?')[0]  # Remove existing params
    final_link = f"{base_link}{UTM_PARAMS}"
    
    # Handle Mention
    handle = FEED_HANDLES.get(feed_name)
    
    tb = TextBuilder()
    tb.text("💬 New preprint evaluation by ")
    
    if handle:
        tb.mention(f"@{handle}", handle)
    else:
        tb.text(feed_name)
        
    tb.text(" of\n\n")
    tb.text(title)
    tb.text("\n\n")
    # Use short text for the link to save characters
    tb.link("Read more", final_link)
    tb.text("\n\n#PreprintEvaluation")
    
    return tb

def get_external_embed(entry) -> models.AppBskyEmbedExternal.Main:
    title = entry.title
    base_link = entry.link.split('?')[0]
    final_link = f"{base_link}{UTM_PARAMS}"
    
    # Extract summary/abstract
    summary = getattr(entry, 'summary', '')
    # Simple HTML strip (regex is usually enough for simple feeds)
    import re
    clean_summary = re.sub(r'<[^>]+>', '', summary)
    description = clean_summary[:300] if clean_summary else "Read the review on Sciety.org"
    
    return models.AppBskyEmbedExternal.Main(
        external=models.AppBskyEmbedExternal.External(
            title=title,
            description=description,
            uri=final_link
        )
    )

def main():
    if not BSKY_USERNAME or not BSKY_PASSWORD:
        logging.error("Missing environment variables BSKY_USERNAME or BSKY_PASSWORD")
        return

    try:
        client = get_client()
    except Exception as e:
        logging.error(f"Failed to login: {e}")
        return

    last_posted = load_last_posted_ids()
    
    for feed_name, feed_url in RSS_FEEDS.items():
        logging.info(f"Checking {feed_name}...")
        
        current_posted_ids = last_posted.get(feed_url, [])
        # Only keep the last 50 IDs to prevent unlimited growth
        if len(current_posted_ids) > 50:
            current_posted_ids = current_posted_ids[-50:]
            
        entries = get_new_entries(feed_url, current_posted_ids)
        
        if not entries:
            logging.info(f"No new entries for {feed_name}")
            continue
            
        logging.info(f"Found {len(entries)} new entries for {feed_name}")
        
        for entry in entries:
            try:
                tb = create_post_text_builder(entry, feed_name)
                embed = get_external_embed(entry)
                
                # Check grapheme limit logic implicitly handled by atproto SDK validation usually,
                # but good to be aware. TextBuilder manages facets.
                
                client.send_post(tb, embed=embed)
                logging.info(f"Posted: {entry.title}")
                
                current_posted_ids.append(entry.id)
                time.sleep(5) # Rate limit niceness
                
            except Exception as e:
                logging.error(f"Failed to post {entry.title}: {e}")
        
        last_posted[feed_url] = current_posted_ids
        
    save_last_posted_ids(last_posted)

if __name__ == "__main__":
    logging.info("Starting Bluesky Bot...")
    main()
    logging.info("Job complete.")