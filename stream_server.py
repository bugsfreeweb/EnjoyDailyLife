import os
import subprocess
import urllib.request
import json
import logging
from flask import Flask, Response, send_file
from pathlib import Path
from unidecode import unidecode
import urllib.error

app = Flask(__name__)

# Configuration
OUTPUT_DIR = "hls_output"
METADATA_JSON = "video_metadata.json"
DOWNLOAD_TIMEOUT = 15
MAX_CACHE_SIZE_MB = 1000  # 1 GB

# Setup logging
logging.basicConfig(filename='stream.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_metadata():
    """Load video metadata."""
    try:
        if os.path.exists(METADATA_JSON):
            with open(METADATA_JSON, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logging.error(f"Error loading {METADATA_JSON}: {e}")
        return {}

def download_video(url, output_path):
    """Download video."""
    logging.info(f"Downloading: {url}")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        urllib.request.urlretrieve(req, output_path)
        logging.info(f"Downloaded {url} to {output_path}")
        return True
    except urllib.error.URLError as e:
        logging.error(f"Failed to download {url}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error downloading {url}: {e}")
        return False

def convert_to_hls(input_file, output_folder, video_id):
    """Convert to HLS."""
    try:
        os.makedirs(output_folder, exist_ok=True)
        output_m3u8 = os.path.join(output_folder, "playlist.m3u8")
        cmd = [
            "ffmpeg", "-i", input_file,
            "-c:v", "libx264", "-b:v", "1M",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "hls",
            "-hls_time", "4",
            "-hls_list_size", "0",
            "-hls_segment_filename", os.path.join(output_folder, "segment_%03d.ts"),
            output_m3u8
        ]
        logging.info(f"Converting {input_file} for {video_id}")
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"Converted to HLS: {output_m3u8}")
        return output_m3u8
    except subprocess.CalledProcessError as e:
        logging.error(f"FFmpeg error for {video_id}: {e.stderr.decode()}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error converting {video_id}: {e}")
        return None

def clean_cache():
    """Delete oldest HLS folders if cache exceeds size."""
    try:
        total_size = 0
        folders = []
        for root, dirs, files in os.walk(OUTPUT_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path) / (1024 * 1024)  # MB
            for dir in dirs:
                folders.append(os.path.join(root, dir))
        if total_size > MAX_CACHE_SIZE_MB:
            folders.sort(key=lambda x: os.path.getmtime(x))
            for folder in folders[:len(folders)//2]:
                for root, dirs, files in os.walk(folder, topdown=False):
                    for file in files:
                        os.remove(os.path.join(root, file))
                    for dir in dirs:
                        os.rmdir(os.path.join(root, dir))
                os.rmdir(folder)
                logging.info(f"Deleted old cache: {folder}")
    except Exception as e:
        logging.warning(f"Error cleaning cache: {e}")

@app.route('/stream/<video_id>')
def stream(video_id):
    """Stream HLS for video_id."""
    metadata = load_metadata()
    if video_id not in metadata:
        return "Video not found", 404

    video_data = metadata[video_id]
    url = video_data['url']
    output_folder = os.path.join(OUTPUT_DIR, video_id)
    m3u8_file = os.path.join(output_folder, "playlist.m3u8")

    if os.path.exists(m3u8_file):
        logging.info(f"Serving cached HLS: {video_id}")
        return send_file(m3u8_file)

    temp_file = f"temp_{video_id}_{int(time.time())}.mp4"
    try:
        if download_video(url, temp_file):
            m3u8_file = convert_to_hls(temp_file, output_folder, video_id)
            if m3u8_file:
                clean_cache()
                logging.info(f"Streaming new HLS: {video_id}")
                return send_file(m3u8_file)
        return "Failed to process video", 500
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            logging.info(f"Cleaned temp file: {temp_file}")

@app.route('/stream/<video_id>/<path:segment>')
def serve_segment(video_id, segment):
    """Serve HLS segments."""
    segment_path = os.path.join(OUTPUT_DIR, video_id, segment)
    if os.path.exists(segment_path):
        return send_file(segment_path)
    return "Segment not found", 404

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
