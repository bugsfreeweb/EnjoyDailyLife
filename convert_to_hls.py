import os
import subprocess
import urllib.request
import time
import json
import logging
from urllib.parse import urljoin
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.error

# Configuration
OUTPUT_DIR = "hls_output"
M3U_PERMANENT_DIR = "m3u_permanent"
SOURCES = [
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Hollywood/Movies.m3u",
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Worldwide/Movies.m3u"
]
BASE_GITHUB_URL = "https://raw.githubusercontent.com/bugsfreeweb/EnjoyDailyLife/main/hls_output"
FINAL_M3U = "master.m3u"
CONVERTED_LOG = "converted_videos.json"
MAX_VIDEOS_PER_SOURCE = 5  # Limit videos per source per run
DOWNLOAD_TIMEOUT = 30  # Seconds
MAX_WORKERS = 4  # Concurrent downloads

# Setup logging
logging.basicConfig(filename='conversion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_dirs():
    """Create output and permanent M3U directories."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(M3U_PERMANENT_DIR, exist_ok=True)
    logging.info(f"Created directories: {OUTPUT_DIR}, {M3U_PERMANENT_DIR}")

def load_converted_videos():
    """Load previously converted videos from JSON."""
    try:
        if os.path.exists(CONVERTED_LOG):
            with open(CONVERTED_LOG, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logging.error(f"Error loading {CONVERTED_LOG}: {e}")
        return {}

def save_converted_videos(converted_videos):
    """Save converted videos to JSON."""
    try:
        with open(CONVERTED_LOG, 'w') as f:
            json.dump(converted_videos, f, indent=2)
        logging.info(f"Saved converted videos to {CONVERTED_LOG}")
    except Exception as e:
        logging.error(f"Error saving {CONVERTED_LOG}: {e}")

def fetch_m3u_urls(m3u_url):
    """Fetch video URLs from an M3U playlist."""
    try:
        req = urllib.request.Request(m3u_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=DOWNLOAD_TIMEOUT) as response:
            content = response.read().decode('utf-8')
            urls = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith('#')]
            logging.info(f"Fetched {len(urls)} URLs from {m3u_url}")
            return urls[:MAX_VIDEOS_PER_SOURCE]  # Limit URLs
    except Exception as e:
        logging.error(f"Error fetching {m3u_url}: {e}")
        return []

def download_video(url, output_path):
    """Download video from URL."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        urllib.request.urlretrieve(url, output_path)  # Removed context parameter
        logging.info(f"Downloaded {url} to {output_path}")
        return True
    except urllib.error.URLError as e:
        logging.error(f"Failed to download {url}: {e}")
        return False

def convert_to_hls(input_file, output_folder):
    """Convert MP4/MKV to HLS using ffmpeg."""
    try:
        os.makedirs(output_folder, exist_ok=True)
        output_m3u8 = os.path.join(output_folder, "playlist.m3u8")
        cmd = [
            "ffmpeg", "-i", input_file,
            "-c:v", "copy", "-c:a", "copy",
            "-f", "hls",
            "-hls_time", "10",
            "-hls_list_size", "0",
            "-hls_segment_filename", os.path.join(output_folder, "segment_%03d.ts"),
            output_m3u8
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"Converted {input_file} to HLS at {output_m3u8}")
        return output_m3u8
    except Exception as e:
        logging.error(f"Error converting {input_file}: {e}")
        return None

def create_permanent_m3u(video_url, m3u8_file, video_id):
    """Create a permanent M3U file for a video."""
    try:
        relative_path = os.path.relpath(m3u8_file, OUTPUT_DIR)
        github_m3u8_url = f"{BASE_GITHUB_URL}/{relative_path.replace(os.sep, '/')}"
        permanent_m3u = os.path.join(M3U_PERMANENT_DIR, f"video_{video_id}.m3u")
        with open(permanent_m3u, 'w') as f:
            f.write(f"#EXTM3U\n#EXTINF:-1,Video_{video_id}\n{github_m3u8_url}\n")
        logging.info(f"Created permanent M3U: {permanent_m3u}")
        return permanent_m3u, github_m3u8_url
    except Exception as e:
        logging.error(f"Error creating permanent M3U for {video_url}: {e}")
        return None, None

def process_video(video_url, source_idx, source_name, converted_videos, video_id_counter):
    """Process a single video (download and convert)."""
    temp_file = f"temp_{source_idx}_{int(time.time())}.mp4"  # Default to mp4 for simplicity
    source_output_dir = os.path.join(OUTPUT_DIR, source_name)
    video_id = len(converted_videos) + video_id_counter + 1
    output_folder = os.path.join(source_output_dir, f"video_{video_id}")

    # Download
    if download_video(video_url, temp_file):
        # Convert to HLS
        m3u8_file = convert_to_hls(temp_file, output_folder)
        try:
            os.remove(temp_file)
            logging.info(f"Cleaned up temporary file: {temp_file}")
        except Exception as e:
            logging.warning(f"Failed to delete {temp_file}: {e}")

        if m3u8_file:
            permanent_m3u, github_m3u8_url = create_permanent_m3u(video_url, m3u8_file, video_id)
            if github_m3u8_url:
                return {
                    "url": video_url,
                    "m3u8_file": m3u8_file,
                    "video_id": video_id,
                    "source": source_name,
                    "github_m3u8_url": github_m3u8_url
                }
    return None

def main():
    create_dirs()
    converted_videos = load_converted_videos()
    processed_urls = set(converted_videos.keys())  # Track all processed URLs
    master_m3u_content = ["#EXTM3U"]
    logging.info("Starting conversion process")

    # Include existing videos in master M3U
    for video_url, info in converted_videos.items():
        m3u8_file = info.get("m3u8_file")
        video_id = info.get("video_id")
        source_name = info.get("source")
        if m3u8_file and os.path.exists(m3u8_file):
            permanent_m3u, github_m3u8_url = create_permanent_m3u(video_url, m3u8_file, video_id)
            if github_m3u8_url:
                master_m3u_content.append(f"#EXTINF:-1,{source_name}_video_{video_id}")
                master_m3u_content.append(github_m3u8_url)

    video_id_counter = 0
    new_videos = []

    # Process each source M3U
    for source_idx, source_url in enumerate(SOURCES):
        source_name = f"source_{source_idx + 1}"
        video_urls = fetch_m3u_urls(source_url)

        # Filter new URLs
        new_urls = [url for url in video_urls if url not in processed_urls]
        logging.info(f"Found {len(new_urls)} new URLs for {source_name}")

        # Download and process concurrently
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(process_video, url, source_idx, source_name, converted_videos, video_id_counter): url
                for url in new_urls
            }
            for future in as_completed(future_to_url):
                result = future.result()
                if result:
                    new_videos.append(result)
                    processed_urls.add(result["url"])
                    video_id_counter += 1

    # Update converted videos
    for video in new_videos:
        converted_videos[video["url"]] = {
            "m3u8_file": video["m3u8_file"],
            "video_id": video["video_id"],
            "source": video["source"]
        }
        master_m3u_content.append(f"#EXTINF:-1,{video['source']}_video_{video['video_id']}")
        master_m3u_content.append(video["github_m3u8_url"])

    # Save updated converted videos
    save_converted_videos(converted_videos)

    # Write master M3U file
    try:
        with open(FINAL_M3U, "w") as f:
            f.write("\n".join(master_m3u_content))
        logging.info(f"Master M3U written to {FINAL_M3U}")
    except Exception as e:
        logging.error(f"Error writing {FINAL_M3U}: {e}")

if __name__ == "__main__":
    main()
