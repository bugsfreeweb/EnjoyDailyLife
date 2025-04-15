import os
import subprocess
import urllib.request
import time
from urllib.parse import urljoin
from pathlib import Path
import logging

# Configuration
OUTPUT_DIR = "hls_output"
SOURCES = [
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Hollywood/Movies.m3u",
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Worldwide/Movies.m3u"
]
BASE_GITHUB_URL = "https://raw.githubusercontent.com/bugsfreeweb/EnjoyDailyLife/main/hls_output"
FINAL_M3U = "master.m3u"

# Setup logging
logging.basicConfig(filename='conversion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_dirs():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Created output directory: {OUTPUT_DIR}")

def fetch_m3u_urls(m3u_url):
    """Fetch video URLs from an M3U playlist."""
    try:
        with urllib.request.urlopen(m3u_url) as response:
            content = response.read().decode('utf-8')
            urls = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith('#')]
            logging.info(f"Fetched {len(urls)} URLs from {m3u_url}")
            return urls
    except Exception as e:
        logging.error(f"Error fetching {m3u_url}: {e}")
        return []

def download_video(url, output_path, retries=3):
    """Download video from URL with retries."""
    for attempt in range(retries):
        try:
            urllib.request.urlretrieve(url, output_path)
            logging.info(f"Downloaded {url} to {output_path}")
            return True
        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(2)
    logging.error(f"Failed to download {url} after {retries} attempts")
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

def main():
    create_dirs()
    processed_urls = set()  # Track processed URLs to avoid duplicates
    master_m3u_content = ["#EXTM3U"]
    logging.info("Starting conversion process")

    # Process each source M3U
    for source_idx, source_url in enumerate(SOURCES):
        source_name = f"source_{source_idx + 1}"
        source_output_dir = os.path.join(OUTPUT_DIR, source_name)
        video_urls = fetch_m3u_urls(source_url)

        for video_url in video_urls:
            if video_url in processed_urls:
                logging.info(f"Skipping duplicate: {video_url}")
                continue
            processed_urls.add(video_url)

            # Determine file extension
            ext = "mp4" if video_url.lower().endswith(".mp4") else "mkv" if video_url.lower().endswith(".mkv") else None
            if not ext:
                logging.warning(f"Unsupported format: {video_url}")
                continue

            # Download video
            temp_file = f"temp_{source_idx}_{int(time.time())}.{ext}"
            if not download_video(video_url, temp_file):
                continue

            # Convert to HLS
            output_folder = os.path.join(source_output_dir, f"video_{len(processed_urls)}")
            m3u8_file = convert_to_hls(temp_file, output_folder)
            try:
                os.remove(temp_file)  # Clean up
                logging.info(f"Cleaned up temporary file: {temp_file}")
            except Exception as e:
                logging.warning(f"Failed to delete {temp_file}: {e}")

            if m3u8_file:
                # Generate GitHub raw URL for M3U8
                relative_path = os.path.relpath(m3u8_file, OUTPUT_DIR)
                github_m3u8_url = f"{BASE_GITHUB_URL}/{source_name}/{relative_path.replace(os.sep, '/')}"
                master_m3u_content.append(f"#EXTINF:-1,{source_name}_video_{len(processed_urls)}")
                master_m3u_content.append(github_m3u8_url)
                logging.info(f"Added {github_m3u8_url} to master M3U")

    # Write master M3U file
    try:
        with open(FINAL_M3U, "w") as f:
            f.write("\n".join(master_m3u_content))
        logging.info(f"Master M3U written to {FINAL_M3U}")
    except Exception as e:
        logging.error(f"Error writing {FINAL_M3U}: {e}")

if __name__ == "__main__":
    main()
