import os
import urllib.request
import json
import logging
import re
from pathlib import Path
from unidecode import unidecode

# Configuration
M3U_PERMANENT_DIR = "m3u_permanent"
SOURCES = [
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Hollywood/Movies.m3u",
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Worldwide/Movies.m3u"
]
SOURCE_GROUPS = {
    SOURCES[0]: "Hollywood",
    SOURCES[1]: "Worldwide"
}
FINAL_M3U = "./VOD/master_vod.m3u"
METADATA_JSON = "video_metadata.json"
DOWNLOAD_TIMEOUT = 15
DEFAULT_LOGO = "https://via.placeholder.com/150"

# Setup logging
logging.basicConfig(filename='generate.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_dirs():
    """Create permanent M3U directory."""
    os.makedirs(M3U_PERMANENT_DIR, exist_ok=True)
    logging.info(f"Created directory: {M3U_PERMANENT_DIR}")

def fetch_m3u_urls(m3u_url):
    """Fetch video URLs, titles, logos, and groups."""
    try:
        req = urllib.request.Request(m3u_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=DOWNLOAD_TIMEOUT) as response:
            content = response.read().decode('utf-8')
            urls = []
            lines = content.splitlines()
            i = 0
            group = SOURCE_GROUPS.get(m3u_url, "Unknown")
            while i < len(lines):
                if lines[i].startswith('#EXTGRP'):
                    group = lines[i].split(':', 1)[1].strip() if ':' in lines[i] else group
                elif lines[i].startswith('#EXTINF'):
                    try:
                        title = lines[i].split(',', 1)[1].strip() if ',' in lines[i] else f"Video_{len(urls)+1}"
                        logo_match = re.search(r'tvg-logo\s*=\s*"([^"]+)"', lines[i])
                        logo = logo_match.group(1) if logo_match else DEFAULT_LOGO
                        english_title = unidecode(title)
                        i += 1
                        if i < len(lines) and lines[i].strip() and not lines[i].startswith('#'):
                            url = lines[i].strip()
                            if not url.lower().endswith(('.mp4', '.mkv')):
                                logging.warning(f"Skipping unsupported URL: {url}")
                                continue
                            urls.append((url, english_title, logo, group))
                    except IndexError:
                        logging.warning(f"Invalid #EXTINF at line {i+1}")
                i += 1
            logging.info(f"Fetched {len(urls)} URLs from {m3u_url}: {urls[:3]}...")
            return urls
    except urllib.error.URLError as e:
        logging.error(f"Error fetching {m3u_url}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error fetching {m3u_url}: {e}")
        return []

def save_metadata(metadata):
    """Save video metadata."""
    try:
        with open(METADATA_JSON, 'w') as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"Saved metadata to {METADATA_JSON}")
    except Exception as e:
        logging.error(f"Error saving {METADATA_JSON}: {e}")

def write_master_m3u(videos):
    """Write VOD/master_vod.m3u with raw URLs."""
    try:
        with open(FINAL_M3U, 'w') as f:
            f.write("#EXTM3U\n")
            for video in videos:
                f.write(f"#EXTINF:-1 tvg-logo=\"{video['logo']}\" group-title=\"{video['group']}\",{video['title']}\n")
                f.write(f"{video['url']}\n")
        logging.info(f"Wrote {len(videos)} videos to {FINAL_M3U}")
    except Exception as e:
        logging.error(f"Error writing {FINAL_M3U}: {e}")

def main():
    create_dirs()
    all_videos = []
    metadata = {}

    for source_idx, source_url in enumerate(SOURCES):
        video_urls = fetch_m3u_urls(source_url)
        for idx, (url, title, logo, group) in enumerate(video_urls):
            video_id = f"video_{source_idx+1}_{idx+1}"
            video_data = {
                "url": url,
                "title": title,
                "logo": logo,
                "group": group,
                "source": f"source_{source_idx+1}"
            }
            all_videos.append(video_data)
            metadata[video_id] = video_data

    write_master_m3u(all_videos)
    save_metadata(metadata)

if __name__ == "__main__":
    main()
