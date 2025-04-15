import os
import subprocess
import urllib.request
import re
import time
from urllib.parse import urljoin
from pathlib import Path

# Configuration
OUTPUT_DIR = "hls_output"
SOURCES = [
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Hollywood/Movies.m3u",
    "https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/Movies/Worldwide/Movies.m3u"
]  # Replace with your M3U URLs
BASE_GITHUB_URL = "https://raw.githubusercontent.com/bugsfreeweb/EnjoyDailyLife/main/hls_output"
FINAL_M3U = "master.m3u"

def create_dirs():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_m3u_urls(m3u_url):
    """Fetch video URLs from an M3U playlist."""
    try:
        with urllib.request.urlopen(m3u_url) as response:
            content = response.read().decode('utf-8')
            # Extract URLs (basic parsing, assumes URLs are on separate lines)
            urls = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith('#')]
            return urls
    except Exception as e:
        print(f"Error fetching {m3u_url}: {e}")
        return []

def download_video(url, output_path):
    """Download video from URL."""
    try:
        urllib.request.urlretrieve(url, output_path)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
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
        return output_m3u8
    except Exception as e:
        print(f"Error converting {input_file}: {e}")
        return None

def main():
    create_dirs()
    processed_urls = set()  # Track processed URLs to avoid duplicates
    master_m3u_content = ["#EXTM3U"]

    # Process each source M3U
    for source_idx, source_url in enumerate(SOURCES):
        source_name = f"source_{source_idx + 1}"
        source_output_dir = os.path.join(OUTPUT_DIR, source_name)
        video_urls = fetch_m3u_urls(source_url)

        for video_url in video_urls:
            if video_url in processed_urls:
                print(f"Skipping duplicate: {video_url}")
                continue
            processed_urls.add(video_url)

            # Determine file extension
            ext = "mp4" if video_url.lower().endswith(".mp4") else "mkv" if video_url.lower().endswith(".mkv") else None
            if not ext:
                print(f"Unsupported format: {video_url}")
                continue

            # Download video
            temp_file = f"temp_{source_idx}_{int(time.time())}.{ext}"
            if not download_video(video_url, temp_file):
                continue

            # Convert to HLS
            output_folder = os.path.join(source_output_dir, f"video_{len(processed_urls)}")
            m3u8_file = convert_to_hls(temp_file, output_folder)
            os.remove(temp_file)  # Clean up

            if m3u8_file:
                # Generate GitHub raw URL for M3U8
                relative_path = os.path.relpath(m3u8_file, OUTPUT_DIR)
                github_m3u8_url = f"{BASE_GITHUB_URL}/{source_name}/{relative_path.replace(os.sep, '/')}"
                master_m3u_content.append(f"#EXTINF:-1,{source_name}_video_{len(processed_urls)}")
                master_m3u_content.append(github_m3u8_url)

    # Write master M3U file
    with open(FINAL_M3U, "w") as f:
        f.write("\n".join(master_m3u_content))
    print(f"Master M3U written to {FINAL_M3U}")

if __name__ == "__main__":
    main()
