import os
import subprocess
import urllib.request
import time
import json
import logging
import re
from urllib.parse import urljoin
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.error
from unidecode import unidecode

# Configuration
OUTPUT_DIR = "hls_output"
M3U_PERMANENT_DIR = "m3u_permanent"
SOURCES = [
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Hollywood/Movies.m3u",
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Worldwide/Movies.m3u"
]
SOURCE_GROUPS = {
    SOURCES[0]: "Hollywood",
    SOURCES[1]: "Worldwide"
}
BASE_GITHUB_URL = "https://raw.githubusercontent.com/bugsfreeweb/EnjoyDailyLife/main/hls_output"
FINAL_M3U = "master.m3u"
CONVERTED_LOG = "converted_videos.json"
FAILED_LOG = "failed_urls.json"
MAX_VIDEOS_PER_SOURCE = 50
DOWNLOAD_TIMEOUT = 15
MAX_WORKERS = 4
DEFAULT_LOGO = "https://via.placeholder.com/150"

# Setup logging
logging.basicConfig(filename='conversion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_dirs():
    """Create output and permanent M3U directories."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(M3U_PERMANENT_DIR, exist_ok=True)
    logging.info(f"Created directories: {OUTPUT_DIR}, {M3U_PERMANENT_DIR}")

def load_converted_videos():
    """Load converted videos."""
    try:
        if os.path.exists(CONVERTED_LOG):
            with open(CONVERTED_LOG, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logging.error(f"Error loading {CONVERTED_LOG}: {e}")
        return {}

def save_converted_videos(converted_videos):
    """Save converted videos."""
    try:
        with open(CONVERTED_LOG, 'w') as f:
            json.dump(converted_videos, f, indent=2)
        logging.info(f"Saved converted videos to {CONVERTED_LOG}")
    except Exception as e:
        logging.error(f"Error saving {CONVERTED_LOG}: {e}")

def load_failed_urls():
    """Load failed URLs."""
    try:
        if os.path.exists(FAILED_LOG):
            with open(FAILED_LOG, 'r') as f:
                return json.load(f)
        return []
    except Exception:
        return []

def save_failed_urls(failed_urls):
    """Save failed URLs."""
    try:
        with open(FAILED_LOG, 'w') as f:
            json.dump(failed_urls, f, indent=2)
        logging.info(f"Saved failed URLs to {FAILED_LOG}")
    except Exception as e:
        logging.error(f"Error saving {FAILED_LOG}: {e}")

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
            logging.info(f"Fetched {len(urls)} URLs from {m3u_url}: {urls[:5]}...")
            return urls[:MAX_VIDEOS_PER_SOURCE]
    except urllib.error.URLError as e:
        logging.error(f"Error fetching {m3u_url}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error fetching {m3u_url}: {e}")
        return []

def download_video(url, output_path, retries=4):
    """Download video with retries."""
    logging.info(f"Attempting to download: {url}")
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            urllib.request.urlretrieve(req, output_path)
            logging.info(f"Downloaded {url} to {output_path}")
            return True
        except urllib.error.URLError as e:
            logging.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt < retries:
                time.sleep(0.5)
        except Exception as e:
            logging.warning(f"Unexpected error on attempt {attempt+1} for {url}: {e}")
            if attempt < retries:
                time.sleep(0.5)
    logging.error(f"Failed to download {url} after {retries+1} attempts")
    return False

def convert_to_hls(input_file, output_folder):
    """Convert MP4/MKV to HLS."""
    try:
        os.makedirs(output_folder, exist_ok=True)
        output_m3u8 = os.path.join(output_folder, "playlist.m3u8")
        cmd = [
            "timeout", "20s",  # Faster timeout
            "ffmpeg", "-i", input_file,
            "-c:v", "copy", "-c:a", "copy",
            "-f", "hls",
            "-hls_time", "10",
            "-hls_list_size", "0",
            "-hls_segment_filename", os.path.join(output_folder, "segment_%03d.ts"),
            output_m3u8
        ]
        logging.info(f"Converting {input_file}")
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"Converted {input_file} to HLS at {output_m3u8}")
        return output_m3u8
    except subprocess.CalledProcessError as e:
        logging.error(f"FFmpeg error converting {input_file}: {e.stderr.decode()}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error converting {input_file}: {e}")
        return None

def create_permanent_m3u(video_url, m3u8_file, video_id, title, logo, group):
    """Create a permanent M3U file."""
    try:
        relative_path = os.path.relpath(m3u8_file, OUTPUT_DIR)
        github_m3u8_url = f"{BASE_GITHUB_URL}/{relative_path.replace(os.sep, '/')}"
        permanent_m3u = os.path.join(M3U_PERMANENT_DIR, f"video_{video_id}.m3u")
        with open(permanent_m3u, 'w') as f:
            f.write(f"#EXTM3U\n#EXTINF:-1 tvg-logo=\"{logo}\" group-title=\"{group}\",{title}\n{github_m3u8_url}\n")
        logging.info(f"Created permanent M3U: {permanent_m3u}")
        return permanent_m3u, github_m3u8_url
    except Exception as e:
        logging.error(f"Error creating permanent M3U for {video_url}: {e}")
        return None, None

def write_partial_master_m3u(master_m3u_content):
    """Write master.m3u incrementally."""
    try:
        with open(FINAL_M3U, "w") as f:
            f.write("\n".join(master_m3u_content))
        logging.info(f"Updated partial master.m3u with {(len(master_m3u_content)-1)//2} videos")
    except Exception as e:
        logging.error(f"Error writing partial {FINAL_M3U}: {e}")

def process_video(video_url, title, logo, group, source_idx, source_name, converted_videos, video_id_counter, master_m3u_content, failed_urls):
    """Process a single video."""
    ext = "mp4" if video_url.lower().endswith(".mp4") else "mkv"
    temp_file = f"temp_{source_idx}_{int(time.time())}.{ext}"
    source_output_dir = os.path.join(OUTPUT_DIR, source_name)
    video_id = len(converted_videos) + video_id_counter + 1
    output_folder = os.path.join(source_output_dir, f"video_{video_id}")

    if video_url in converted_videos:
        logging.info(f"Skipping successfully converted video: {video_url}")
        return None

    try:
        if download_video(video_url, temp_file):
            m3u8_file = convert_to_hls(temp_file, output_folder)
            if m3u8_file:
                permanent_m3u, github_m3u8_url = create_permanent_m3u(video_url, m3u8_file, video_id, title, logo, group)
                if github_m3u8_url:
                    result = {
                        "url": video_url,
                        "m3u8_file": m3u8_file,
                        "video_id": video_id,
                        "source": source_name,
                        "github_m3u8_url": github_m3u8_url,
                        "title": title,
                        "logo": logo,
                        "group": group
                    }
                    master_m3u_content.append(f"#EXTINF:-1 tvg-logo=\"{logo}\" group-title=\"{group}\",{title}")
                    master_m3u_content.append(github_m3u8_url)
                    write_partial_master_m3u(master_m3u_content)
                    return result
            else:
                failed_urls.append({"url": video_url, "reason": "Conversion failed"})
        else:
            failed_urls.append({"url": video_url, "reason": "Download failed"})
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logging.info(f"Cleaned up temporary file: {temp_file}")
        except Exception as e:
            logging.warning(f"Failed to delete {temp_file}: {e}")

    return None

def main():
    create_dirs()
    converted_videos = load_converted_videos()
    failed_urls = load_failed_urls()
    master_m3u_content = ["#EXTM3U"]
    logging.info("Starting conversion process")

    # Include existing videos
    existing_count = 0
    for video_url, info in converted_videos.items():
        m3u8_file = info.get("m3u8_file")
        video_id = info.get("video_id")
        source_name = info.get("source")
        title = info.get("title", f"Video_{video_id}")
        logo = info.get("logo", DEFAULT_LOGO)
        group = info.get("group", "Unknown")
        github_m3u8_url = info.get("github_m3u8_url")
        if m3u8_file and os.path.exists(m3u8_file):
            permanent_m3u, github_m3u8_url = create_permanent_m3u(video_url, m3u8_file, video_id, title, logo, group)
            if github_m3u8_url:
                master_m3u_content.append(f"#EXTINF:-1 tvg-logo=\"{logo}\" group-title=\"{group}\",{title}")
                master_m3u_content.append(github_m3u8_url)
                existing_count += 1
        elif github_m3u8_url:
            master_m3u_content.append(f"#EXTINF:-1 tvg-logo=\"{logo}\" group-title=\"{group}\",{title}")
            master_m3u_content.append(github_m3u8_url)
            existing_count += 1
            logging.warning(f"Retaining video without m3u8: {video_url}")
    logging.info(f"Included {existing_count} existing videos in master.m3u")
    write_partial_master_m3u(master_m3u_content)

    video_id_counter = 0
    new_videos = []

    # Process each source M3U
    for source_idx, source_url in enumerate(SOURCES):
        source_name = f"source_{source_idx + 1}"
        video_urls = fetch_m3u_urls(source_url)

        new_urls = [(url, title, logo, group) for url, title, logo, group in video_urls
                    if url not in converted_videos]
        logging.info(f"Processing {len(new_urls)} new URLs for {source_name}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(process_video, url, title, logo, group, source_idx, source_name, converted_videos, video_id_counter, master_m3u_content, failed_urls): url
                for url, title, logo, group in new_urls
            }
            for future in as_completed(future_to_url):
                result = future.result()
                if result:
                    new_videos.append(result)
                    video_id_counter += 1

    # Update converted videos
    for video in new_videos:
        converted_videos[video["url"]] = {
            "m3u8_file": video["m3u8_file"],
            "video_id": video["video_id"],
            "source": video["source"],
            "github_m3u8_url": video["github_m3u8_url"],
            "title": video["title"],
            "logo": video["logo"],
            "group": video["group"]
        }

    logging.info(f"Added {len(new_videos)} new videos to master.m3u")
    save_converted_videos(converted_videos)
    save_failed_urls(failed_urls)
    write_partial_master_m3u(master_m3u_content)

if __name__ == "__main__":
    main()
