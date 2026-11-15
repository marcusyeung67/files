"""
ScratchTube v6 Server
---------------------
- Resolution: 112x63 @ 14 FPS (RGB24: 21,168 bytes per frame)
- High-speed single-pass FFmpeg conversion with ffprobe stream validation
- Pure Python instant in-memory MP3 audio seeking (< 1ms vs ~2.5s FFmpeg spawn)
- Dedicated downloads directory with LRU cache pruning and temporary file cleanup
- Strict search filtering excluding livestreams, private, deleted, and unplayable videos
- Production Waitress WSGI server (mandatory)
"""

import bisect
import glob
import gzip
import io
import json
import os
import re
import shutil
import subprocess
import struct
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import requests
import yt_dlp
from flask import Flask, Response, abort, jsonify, request, send_file, stream_with_context
from PIL import Image

# ---------------------------------------------------------
# MANDATORY SERVER ENGINE CHECK (Waitress)
# ---------------------------------------------------------
try:
    from waitress import serve
except ImportError:
    print("=" * 64)
    print("[FATAL] Waitress is required to run ScratchTube v6.")
    print("Please install Waitress by running:")
    print("    pip install waitress")
    print("=" * 64)
    sys.exit(1)

app = Flask(__name__)

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
DOWNLOADS_DIR = r"C:\Users\marcu\OneDrive\Desktop\ScratchTube\downloads"
DELIMITER = "‡"
THUMBNAIL_HEIGHT = 96

# ScratchTube v6 Video Specification
VIDEO_WIDTH = 128
VIDEO_HEIGHT = 72
VIDEO_TARGET_FPS = 14
FRAME_SIZE = VIDEO_WIDTH * VIDEO_HEIGHT * 3  # exactly 27,648 bytes per RGB24 frame
HEX_FRAME_SIZE = FRAME_SIZE * 2              # exactly 55,296 hex characters per frame

# Transfer Chunks: 100 frames per response batch
VIDEO_FRAMES_IN_RESPONSE = 100

MAX_SEARCH_DURATION = 600      # seconds (max video duration in search results)
MAX_VIDEO_DURATION = 600       # seconds (refuse to process longer videos)
DOWNLOAD_TIMEOUT = 360         # seconds
SEARCH_RESULTS = 3
MAX_CACHED_VIDEOS = 12         # LRU retention count
MAX_CACHE_BYTES = 500 * 1024 * 1024  # 500 MB maximum cache size on disk
SEARCH_CACHE_SECONDS = 300

FMT = f"v7-{VIDEO_WIDTH}x{VIDEO_HEIGHT}@{VIDEO_TARGET_FPS}"
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
CHUNK_RE = re.compile(r"^vid_start_(\d+(?:\.\d+)?)_([A-Za-z0-9_-]{11})$")
INIT_RE = re.compile(r"^vid_([A-Za-z0-9_-]{11})$")

COOKIES_FROM_BROWSER = os.environ.get("YT_COOKIES_BROWSER")
COOKIES_FILE = os.environ.get("YT_COOKIES_FILE")

def _js_runtime():
    for name in ("deno", "node", "bun"):
        if shutil.which(name):
            return name
    return None

JS_RUNTIME = _js_runtime()
CLIENT_ATTEMPTS = ([None, ["web_safari"], ["mweb"], ["tv"], ["ios"], ["android_vr"]] if JS_RUNTIME
                   else [["android_vr"], ["ios"], ["tv"], ["mweb"], ["web_safari"], None])
_good_client = {"v": "unset"}

def ordered_clients():
    g = _good_client["v"]
    if g == "unset" or g not in CLIENT_ATTEMPTS:
        return list(CLIENT_ATTEMPTS)
    return [g] + [c for c in CLIENT_ATTEMPTS if c != g]

# ---------------------------------------------------------
# STATE & CACHING
# ---------------------------------------------------------
state_lock = threading.Lock()
active_video_downloads = {}    # video_id -> start_time
failed_downloads = {}          # video_id -> error text
video_meta = {}                # video_id -> metadata dict
download_order = []            # list of video_ids ordered by LRU access

chunk_cache = OrderedDict()     # (video_id, start_frame) -> gzip'd JSON body
chunk_lock = threading.Lock()
CHUNK_CACHE_BYTES = 256 * 1024 * 1024
_chunk_bytes = 0

audio_index_cache = OrderedDict()  # video_id -> (audio_bytes, list of (timestamp, byte_offset))
audio_lock = threading.Lock()
_audio_index_bytes = 0
AUDIO_INDEX_CACHE_BYTES = 128 * 1024 * 1024

search_lock = threading.Lock()
search_cache = OrderedDict()       # query -> (timestamp, result_string)
SEARCH_CACHE_MAX_ENTRIES = 64

_prefetch_lock = threading.Lock()
_prefetching = set()
_prefetch_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="scratchtube-prefetch")

http = requests.Session()

# Valid PCM WAV silence for Scratch's synthesis decoder.
# The old bridge returned MP3 bytes while Scratch's audio engine was parsing a WAV response.
def make_silent_wav(duration=0.10, sample_rate=44100, channels=1):
    frame_count = max(1, int(duration * sample_rate))
    pcm = b"\\x00" * (frame_count * channels * 2)
    byte_rate = sample_rate * channels * 2
    block_align = channels * 2
    header = (
        b"RIFF" + struct.pack("<I", 36 + len(pcm)) + b"WAVE"
        + b"fmt " + struct.pack("<IHHIIHH", 16, 1, channels, sample_rate,
                                byte_rate, block_align, 16)
        + b"data" + struct.pack("<I", len(pcm))
    )
    return header + pcm

SILENT_WAV_BYTES = make_silent_wav()

# ---------------------------------------------------------
# PATH HELPERS
# ---------------------------------------------------------
def frames_path(video_id):
    return os.path.join(DOWNLOADS_DIR, f"{video_id}_frames.rgb")

def audio_path(video_id):
    return os.path.join(DOWNLOADS_DIR, f"{video_id}_audio.mp3")

def meta_path(video_id):
    return os.path.join(DOWNLOADS_DIR, f"{video_id}_meta.json")

# ---------------------------------------------------------
# CACHE MANAGEMENT & STARTUP
# ---------------------------------------------------------
def init_dl():
    """Scan and reuse valid existing v6 cache items at startup; clean temp files."""
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    
    # Clean up any leftover temporary files from interrupted runs
    for temp_pattern in ("*.part", "*.part.mp3", "*.tmp", "*_src.*", "*.ytdl"):
        for f in glob.glob(os.path.join(DOWNLOADS_DIR, temp_pattern)):
            try:
                os.remove(f)
            except OSError:
                pass

    found = []
    for mf in glob.glob(os.path.join(DOWNLOADS_DIR, "*_meta.json")):
        vid = os.path.basename(mf)[:-len("_meta.json")]
        if not VIDEO_ID_RE.match(vid):
            continue
        try:
            with open(mf, "r", encoding="utf-8") as f:
                m = json.load(f)
            fp, ap = frames_path(vid), audio_path(vid)
            if (m.get("fmt") == FMT and os.path.isfile(fp) and os.path.isfile(ap)):
                f_size = os.path.getsize(fp)
                if f_size >= FRAME_SIZE and f_size % FRAME_SIZE == 0 and os.path.getsize(ap) > 1024:
                    video_meta[vid] = m
                    found.append((os.path.getmtime(mf), vid))
                    # Pre-load audio frame index into memory
                    get_or_load_audio_index(vid)
        except Exception as e:
            print(f"[WARN] Bad cache entry {mf}: {e}")

    download_order.extend(vid for _, vid in sorted(found))
    print(f"[DEBUG] Cache directory: {DOWNLOADS_DIR} ({len(found)} ready videos reused)")

def drop_chunks(video_id):
    global _chunk_bytes
    with chunk_lock:
        keys_to_pop = [k for k in chunk_cache if k[0] == video_id]
        for k in keys_to_pop:
            _chunk_bytes -= len(chunk_cache.pop(k))

def cleanup_cache(keep_id=None):
    """LRU cleanup: prune oldest cached videos beyond count or byte limit."""
    with state_lock:
        if keep_id:
            if keep_id in download_order:
                download_order.remove(keep_id)
            download_order.append(keep_id)
            
        def total_cache_size():
            tot = 0
            for vid in download_order:
                for p in (frames_path(vid), audio_path(vid), meta_path(vid)):
                    if os.path.isfile(p):
                        tot += os.path.getsize(p)
            return tot

        while len(download_order) > MAX_CACHED_VIDEOS or total_cache_size() > MAX_CACHE_BYTES:
            if not download_order:
                break
            old_id = download_order.pop(0)
            if old_id == keep_id and download_order:
                # If keep_id is at index 0, rotate it
                download_order.append(old_id)
                old_id = download_order.pop(0)
            
            video_meta.pop(old_id, None)
            drop_chunks(old_id)
            global _audio_index_bytes
            with audio_lock:
                old_audio = audio_index_cache.pop(old_id, None)
                if old_audio:
                    _audio_index_bytes -= len(old_audio[0])
            for f in glob.glob(os.path.join(DOWNLOADS_DIR, f"{old_id}_*")):
                try:
                    os.remove(f)
                except OSError:
                    pass
            print(f"[DEBUG] Pruned cached video {old_id} from disk")

# ---------------------------------------------------------
# MP3 PARSING & IN-MEMORY AUDIO SLICING (< 1ms)
# ---------------------------------------------------------
def parse_mp3_frames(filepath):
    """Parse MPEG1 Layer 3 frames and build a list of (timestamp, byte_offset) tuples."""
    try:
        with open(filepath, "rb") as f:
            data = f.read()
    except OSError:
        return b"", []

    offset = 0
    if data[:3] == b"ID3":
        size = 0
        for b in data[6:10]:
            size = (size << 7) | (b & 0x7F)
        offset = 10 + size

    frames = []
    bitrates = [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0]
    samplerates = [44100, 48000, 32000, 0]

    cur_time = 0.0
    data_len = len(data)
    while offset < data_len - 4:
        b0, b1, b2, _ = data[offset:offset+4]
        if b0 == 0xFF and (b1 & 0xE0) == 0xE0:
            version = (b1 >> 3) & 3
            layer = (b1 >> 1) & 3
            if version == 3 and layer == 1:  # MPEG 1 Layer 3
                br_idx = (b2 >> 4) & 0xF
                sr_idx = (b2 >> 2) & 3
                padding = (b2 >> 1) & 1
                if 0 < br_idx < 15 and sr_idx < 3:
                    br = bitrates[br_idx] * 1000
                    sr = samplerates[sr_idx]
                    frame_len = (144 * br // sr) + padding
                    if frame_len >= 24 and offset + frame_len <= data_len:
                        frames.append((cur_time, offset))
                        cur_time += 1152.0 / sr
                        offset += frame_len
                        continue
        offset += 1
    return data, frames

def get_or_load_audio_index(video_id):
    global _audio_index_bytes
    with audio_lock:
        cached = audio_index_cache.get(video_id)
        if cached is not None:
            audio_index_cache.move_to_end(video_id)
            return cached
    
    ap = audio_path(video_id)
    if not os.path.isfile(ap):
        return None, []
    data, frames = parse_mp3_frames(ap)
    if data and frames:
        value = (data, frames)
        with audio_lock:
            previous = audio_index_cache.pop(video_id, None)
            if previous:
                _audio_index_bytes -= len(previous[0])
            audio_index_cache[video_id] = value
            _audio_index_bytes += len(data)
            while _audio_index_bytes > AUDIO_INDEX_CACHE_BYTES and len(audio_index_cache) > 1:
                _old_id, old_value = audio_index_cache.popitem(last=False)
                _audio_index_bytes -= len(old_value[0])
        return data, frames
    return data, []

def slice_audio_at(video_id, target_time):
    """Instant in-memory slice of MP3 starting at target_time in seconds."""
    data, frames = get_or_load_audio_index(video_id)
    if not data or not frames:
        # Fallback to serving whole audio file if index unavailable
        ap = audio_path(video_id)
        if os.path.isfile(ap):
            with open(ap, "rb") as f:
                return f.read()
        return None

    target_time = max(0.0, float(target_time))
    # Tuples are already ordered by timestamp; avoid allocating a second list per seek.
    idx = bisect.bisect_left(frames, (target_time, -1))
    if idx >= len(frames):
        return b""
    byte_offset = frames[idx][1]
    return data[byte_offset:]

# ---------------------------------------------------------
# CORS & COMPRESSION
# ---------------------------------------------------------
@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    if (response.status_code == 200
            and response.mimetype == "application/json"
            and not response.direct_passthrough
            and not response.headers.get("Content-Encoding")
            and "gzip" in request.headers.get("Accept-Encoding", "")):
        data = response.get_data()
        if len(data) > 2048:
            response.set_data(gzip.compress(data, compresslevel=1))
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Vary"] = "Accept-Encoding"
    return response

# ---------------------------------------------------------
# FORMAT HELPERS
# ---------------------------------------------------------
def clean(s, limit=None):
    s = "" if s is None else str(s)
    s = s.replace(DELIMITER, " ")
    s = re.sub(r"[\r\n\t]+", " ", s).strip()
    return s[:limit] if limit else s

def format_views(v):
    if v is None:
        return "N/A"
    try:
        v = float(v)
    except (ValueError, TypeError):
        return "N/A"
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1000:
        return f"{v/1000:.1f}K"
    return str(int(v))

def format_likes(v):
    if v is None:
        return "N/A"
    try:
        v = float(v)
    except (ValueError, TypeError):
        return "N/A"
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1000:
        return f"{v/1000:.1f}K"
    return str(int(v))

def format_upload_date(d):
    d = d or ""
    return f"{d[4:6]}.{d[6:8]}.{d[2:4]}" if len(d) == 8 and d.isdigit() else ""

def format_duration(d):
    try:
        d = int(d)
        return f"{d//60}:{d%60:02d}"
    except (ValueError, TypeError):
        return "0:00"

# ---------------------------------------------------------
# YT-DLP HELPERS
# ---------------------------------------------------------
def base_opts(client=None):
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "nocheckcertificate": False,
        "socket_timeout": 15,
        "retries": 3,
        "fragment_retries": 3,
    }
    if COOKIES_FROM_BROWSER:
        opts["cookiesfrombrowser"] = (COOKIES_FROM_BROWSER,)
    if COOKIES_FILE:
        opts["cookiefile"] = COOKIES_FILE
    if client:
        opts["extractor_args"] = {"youtube": {"player_client": client}}
    return opts

# ---------------------------------------------------------
# SEARCH FILTERING (Strict & Playable Only)
# ---------------------------------------------------------
def is_playable_entry(entry):
    """Exclude livestreams, upcoming, private, deleted, and unplayable videos."""
    if not entry or not isinstance(entry, dict):
        return False
    vid = entry.get("id")
    if not vid or not VIDEO_ID_RE.match(vid):
        return False

    # Check title for deleted / private markers
    title = entry.get("title") or ""
    if not title or title.strip() in ("[Private video]", "[Deleted video]", "Private video", "Deleted video"):
        return False

    # Check live status
    live_status = entry.get("live_status")
    if live_status in ("is_live", "is_upcoming", "post_live"):
        return False
    if entry.get("is_live") is True:
        return False

    # Check availability & restrictions
    availability = entry.get("availability")
    if availability in ("private", "premium_only", "subscriber_only", "needs_auth"):
        return False

    # Check duration (must have a valid positive duration within limit)
    dur = entry.get("duration")
    if dur is None or not isinstance(dur, (int, float)):
        return False
    if dur <= 0 or dur > MAX_SEARCH_DURATION:
        return False

    return True

def search_videos(query, num_results=20):
    opts = base_opts()
    # Keep search requests comfortably below Scratch's 10-second translate timeout.
    opts.update({
        "ignoreerrors": True,
        "extract_flat": "in_playlist",
        "socket_timeout": 7,
        "retries": 1,
        "fragment_retries": 1,
    })
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch{num_results}:{query}", download=False)
    except Exception as e:
        print(f"[ERROR] Search failed: {e}")
        return {}
    if not info:
        return {}
    entries = [e for e in (info.get("entries") or []) if is_playable_entry(e)]
    return {"entries": entries}

def extract_thumbnail(video_id):
    """Retrieve YouTube thumbnail and resize to the existing 96px height."""
    url = f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"
    r = http.get(url, timeout=8)
    r.raise_for_status()
    img = Image.open(io.BytesIO(r.content)).convert("RGB")
    w, h = img.size
    if w <= 0 or h <= 0:
        raise ValueError("Invalid thumbnail dimensions")
    new_w = max(1, int(THUMBNAIL_HEIGHT * (w / h)))
    img = img.resize((new_w, THUMBNAIL_HEIGHT))
    return img.tobytes().hex().upper(), new_w

# ---------------------------------------------------------
# MEDIA PROBING & CONVERSION PIPELINE
# ---------------------------------------------------------
def probe_media_file(filepath):
    """Use ffprobe to identify actual video and audio streams."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_streams", "-show_format",
        "-of", "json", filepath
    ]
    try:
        res = subprocess.check_output(cmd, timeout=30)
        info = json.loads(res.decode("utf-8", errors="ignore"))
    except Exception as e:
        raise RuntimeError(f"ffprobe failed on {filepath}: {e}")

    streams = info.get("streams", [])
    v_idx, a_idx = None, None
    for s in streams:
        c_type = s.get("codec_type")
        is_pic = s.get("disposition", {}).get("attached_pic", 0) != 0
        if c_type == "video" and not is_pic:
            if v_idx is None:
                v_idx = s.get("index")
        elif c_type == "audio":
            if a_idx is None:
                a_idx = s.get("index")

    duration = float(info.get("format", {}).get("duration", 0) or 0)
    return v_idx, a_idx, duration

def fetch_source(video_id):
    """Download optimal source format (up to 360p/480p). Returns (info, filepath)."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    last_err = None
    for client in ordered_clients():
        stats = {"bytes": 0}

        def hook(d, stats=stats):
            if d.get("status") == "finished":
                stats["bytes"] += d.get("total_bytes") or d.get("downloaded_bytes") or 0

        opts = base_opts(client)
        opts.update({
            # Prefer 360p then 480p: fast download, minimal CPU downscaling to 128x72
            "format": "bv*[height<=360]+ba/b[height<=360]/bv*[height<=480]+ba/b[height<=480]/bv*+ba/b",
            "concurrent_fragment_downloads": 8,
            "merge_output_format": "mkv",
            "progress_hooks": [hook],
            "outtmpl": os.path.join(DOWNLOADS_DIR, f"{video_id}_src.%(ext)s"),
        })
        try:
            t0 = time.time()
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    raise RuntimeError("No metadata returned by yt-dlp")
                dur = info.get("duration") or 0
                if dur > MAX_VIDEO_DURATION:
                    raise ValueError(f"Video too long ({int(dur)}s > {MAX_VIDEO_DURATION}s)")
                t1 = time.time()
                info = ydl.process_ie_result(info, download=True)
                t2 = time.time()

            path = None
            if info.get("requested_downloads"):
                cand = info["requested_downloads"][0].get("filepath")
                if cand and os.path.isfile(cand) and not cand.endswith((".part", ".ytdl")):
                    path = cand
            if not path or not os.path.isfile(path):
                # Look for downloaded file, excluding unfinished parts
                candidates = [
                    f for f in glob.glob(os.path.join(DOWNLOADS_DIR, f"{video_id}_src.*"))
                    if not f.endswith((".part", ".ytdl")) and os.path.getsize(f) > 1024
                ]
                path = candidates[0] if candidates else None

            if not path:
                raise RuntimeError("Download completed but media file was not found")

            mb = (stats["bytes"] or os.path.getsize(path)) / 1e6
            duration = max(t2 - t1, 0.01)
            print(f"[TIMING] {video_id}: info {t1 - t0:.1f}s, download {t2 - t1:.1f}s "
                  f"({mb:.1f} MB, {mb / duration:.2f} MB/s) via client={client or 'default'}")
            _good_client["v"] = client
            return info, path
        except ValueError:
            raise
        except Exception as e:
            last_err = e
            print(f"[WARN] Download attempt failed (client={client or 'default'}): {str(e)[:160]}")
    raise RuntimeError(f"All download attempts failed: {last_err}")

def run_conversion(src, video_id, duration_hint=0):
    """
    Single-pass FFmpeg conversion:
    - Explicitly maps verified video and audio streams
    - Scales video to 128x72 @ 14 FPS RGB24 (27,648 bytes per frame)
    - Encodes MP3 44.1kHz stereo audio
    - Handles audio-less or unusual stream layouts seamlessly
    """
    v_idx, a_idx, probed_dur = probe_media_file(src)
    if v_idx is None:
        raise RuntimeError("Downloaded media contains no valid video stream")

    duration = probed_dur if probed_dur > 0 else duration_hint
    # Direct scale to the exact 128x72 16:9 target. Scaling directly avoids rounding
    # edge cases from force_original_aspect_ratio and guarantees fixed-size RGB frames.
    vf = (
        f"fps={VIDEO_TARGET_FPS},"
        f"scale={VIDEO_WIDTH}:{VIDEO_HEIGHT}:flags=lanczos,"
        f"unsharp=5:5:0.35:3:3:0.0,setsar=1"
    )

    tmp_v = frames_path(video_id) + ".part"
    tmp_a = audio_path(video_id) + ".part.mp3"

    cmd = ["ffmpeg", "-y", "-v", "error", "-nostdin", "-i", src]
    if a_idx is not None:
        # Source has audio: map video to tmp_v and audio to tmp_a in one pass
        cmd.extend([
            "-map", f"0:{v_idx}",
            "-vf", vf,
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            tmp_v,
            "-map", f"0:{a_idx}",
            "-vn",
            "-ac", "2",
            "-ar", "44100",
            "-c:a", "libmp3lame",
            "-b:a", "128k",
            tmp_a
        ])
    else:
        # Silent source: generate synthetic silent audio matching duration
        cmd.extend([
            "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
            "-map", f"0:{v_idx}",
            "-vf", vf,
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            tmp_v,
            "-map", "1:a",
            "-t", f"{duration:.3f}",
            "-ac", "2",
            "-ar", "44100",
            "-c:a", "libmp3lame",
            "-b:a", "128k",
            tmp_a
        ])

    t0 = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        stderr = proc.communicate(timeout=300)[1].decode(errors="ignore")[-400:]
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        raise RuntimeError("ffmpeg conversion timed out")

    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg conversion failed: {stderr}")

    if not (os.path.isfile(tmp_v) and os.path.getsize(tmp_v) >= FRAME_SIZE and os.path.isfile(tmp_a)):
        raise RuntimeError("ffmpeg conversion produced incomplete or missing files")

    # Atomic rename so ready files appear cleanly
    os.replace(tmp_v, frames_path(video_id))
    os.replace(tmp_a, audio_path(video_id))
    print(f"[TIMING] {video_id}: FFmpeg conversion took {time.time() - t0:.1f}s")
    return duration

def reuse_existing(video_id):
    """Reuse cached frames and audio only when they match the v7 128x72 @ 14 FPS format."""
    fp, ap = frames_path(video_id), audio_path(video_id)
    if not (os.path.isfile(fp) and os.path.isfile(ap) and os.path.getsize(ap) > 1024):
        return None
    size = os.path.getsize(fp)
    if size < FRAME_SIZE or size % FRAME_SIZE != 0:
        return None
    for client in ordered_clients():
        try:
            o = base_opts(client)
            o["skip_download"] = True
            with yt_dlp.YoutubeDL(o) as ydl:
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            if info:
                dur = info.get("duration") or (size / FRAME_SIZE / VIDEO_TARGET_FPS)
                if abs(size / FRAME_SIZE / VIDEO_TARGET_FPS - dur) <= 2.5:
                    print(f"[DEBUG] Reusing cached frames/audio for {video_id}")
                    return info
            return None
        except Exception as e:
            print(f"[WARN] Cache check metadata attempt failed (client={client or 'default'}): {str(e)[:120]}")
    return None

def download_worker(video_id, started):
    src = None
    job_t0 = time.time()
    try:
        info = reuse_existing(video_id)
        if info is None:
            info, src = fetch_source(video_id)
            duration_hint = info.get("duration") or 0
            run_conversion(src, video_id, duration_hint)

        fp, ap = frames_path(video_id), audio_path(video_id)
        if not (os.path.isfile(fp) and os.path.getsize(fp) >= FRAME_SIZE and os.path.isfile(ap)):
            raise RuntimeError("Conversion validation failed: frames or audio missing")

        total_frames = os.path.getsize(fp) // FRAME_SIZE
        duration = info.get("duration") or (total_frames / VIDEO_TARGET_FPS)

        meta = {
            "fmt": FMT,
            "title": clean(info.get("title"), 200),
            "channel": clean(info.get("uploader") or info.get("channel"), 100),
            "views": format_views(info.get("view_count")),
            "likes": format_likes(info.get("like_count")),
            "upload": format_upload_date(info.get("upload_date")),
            "duration": f"{float(duration):.2f}",
            "frames": total_frames,
        }

        tmp_meta = meta_path(video_id) + ".tmp"
        with open(tmp_meta, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False)
        os.replace(tmp_meta, meta_path(video_id))

        with state_lock:
            video_meta[video_id] = meta

        # Pre-parse audio frames for zero-latency seeking
        get_or_load_audio_index(video_id)

        # Pre-build chunk 0 (frames 1-100) and chunk 1 (frames 101-200) into RAM
        chunk_body(video_id, 1)
        prefetch(video_id, 1 + VIDEO_FRAMES_IN_RESPONSE)

        cleanup_cache(keep_id=video_id)
        print(f"[DEBUG] Ready: {video_id} '{meta['title']}' ({total_frames} frames, job took {time.time() - job_t0:.1f}s)")
    except Exception as e:
        print(f"[ERROR] Processing failed for {video_id}: {e}")
        with state_lock:
            failed_downloads[video_id] = str(e)
        # Clean up failed temporary artifacts
        for p in (frames_path(video_id) + ".part", audio_path(video_id) + ".part.mp3", meta_path(video_id) + ".tmp"):
            if os.path.isfile(p):
                try:
                    os.remove(p)
                except OSError:
                    pass
    finally:
        if src and os.path.isfile(src):
            try:
                os.remove(src)
            except OSError:
                pass
        with state_lock:
            if active_video_downloads.get(video_id) == started:
                active_video_downloads.pop(video_id, None)

# ---------------------------------------------------------
# FRAME EXTRACTION & CHUNK CACHING
# ---------------------------------------------------------
def extract_frames(video_id, start_index):
    """
    Extract up to VIDEO_FRAMES_IN_RESPONSE frames starting at 0-based start_index.
    Returns (hex_string, frame_count).
    Every frame is exactly 27,648 bytes (55,296 hex chars).
    """
    start_index = max(0, int(start_index))
    fp = frames_path(video_id)
    if not os.path.isfile(fp):
        return "", 0

    with open(fp, "rb") as f:
        f.seek(start_index * FRAME_SIZE)
        raw = f.read(VIDEO_FRAMES_IN_RESPONSE * FRAME_SIZE)

    valid_bytes = (len(raw) // FRAME_SIZE) * FRAME_SIZE
    raw = raw[:valid_bytes]
    count = valid_bytes // FRAME_SIZE
    if count == 0:
        return "", 0
    return raw.hex().upper(), count

def pack(result):
    return gzip.compress(
        json.dumps({"result": result}, separators=(",", ":")).encode("ascii"),
        compresslevel=1,
        mtime=0
    )

def cache_get(key):
    with chunk_lock:
        body = chunk_cache.get(key)
        if body is not None:
            chunk_cache.move_to_end(key)
        return body

def cache_put(key, body):
    global _chunk_bytes
    with chunk_lock:
        if key in chunk_cache:
            _chunk_bytes -= len(chunk_cache.pop(key))
        chunk_cache[key] = body
        _chunk_bytes += len(body)
        while _chunk_bytes > CHUNK_CACHE_BYTES and len(chunk_cache) > 1:
            _chunk_bytes -= len(chunk_cache.popitem(last=False)[1])

def chunk_body(video_id, start_frame):
    """Return gzip'd JSON for the chunk starting at 1-based frame start_frame."""
    key = (video_id, start_frame)
    body = cache_get(key)
    if body is None:
        frames, n = extract_frames(video_id, start_frame - 1)
        body = pack(f"{n}{DELIMITER}{frames}" if n else "end")
        cache_put(key, body)
    return body

def prefetch(video_id, start_frame):
    """Pre-build a chunk using a bounded pool and deduplicate concurrent requests."""
    key = (video_id, start_frame)
    with _prefetch_lock:
        if key in _prefetching or cache_get(key) is not None:
            return
        _prefetching.add(key)

    def run():
        try:
            fp = frames_path(video_id)
            if os.path.isfile(fp) and (start_frame - 1) * FRAME_SIZE < os.path.getsize(fp):
                chunk_body(video_id, start_frame)
        except Exception as exc:
            print(f"[WARN] Prefetch failed for {video_id} frame {start_frame}: {exc}")
        finally:
            with _prefetch_lock:
                _prefetching.discard(key)

    try:
        _prefetch_pool.submit(run)
    except RuntimeError:
        with _prefetch_lock:
            _prefetching.discard(key)

def respond_gz(body):
    if "gzip" in request.headers.get("Accept-Encoding", "").lower():
        r = Response(body, mimetype="application/json")
        r.headers["Content-Encoding"] = "gzip"
        r.headers["Content-Length"] = str(len(body))
        r.headers["Vary"] = "Accept-Encoding"
        return r
    return Response(gzip.decompress(body), mimetype="application/json")

# ---------------------------------------------------------
# TRANSLATE ROUTE (Scratch Translate Bridge)
# ---------------------------------------------------------
def handle_text(raw):
    if raw.startswith("HTTP "):
        raw = raw[5:]

    if raw == "try":
        return "success"

    # Frame chunks request: vid_start_{frame}_{vid}
    if raw.startswith("vid_start_"):
        start_str, video_id = raw[10:].split("_", 1)
        if not VIDEO_ID_RE.match(video_id) or not os.path.isfile(frames_path(video_id)):
            return "error"
        start_frame = int(float(start_str))
        frames, frame_count = extract_frames(video_id, start_frame - 1)
        if frame_count == 0:
            return "end"
        return f"{frame_count}{DELIMITER}{frames}"

    # Initial video request: vid_{vid}
    if raw.startswith("vid_"):
        video_id = raw[4:]
        if not VIDEO_ID_RE.match(video_id):
            return "error"

        with state_lock:
            if video_id in active_video_downloads:
                if time.time() - active_video_downloads[video_id] > DOWNLOAD_TIMEOUT:
                    active_video_downloads.pop(video_id, None)
                    print(f"[ERROR] Download of {video_id} timed out")
                    return "error"
                return "processing"

            ready = (
                video_id in video_meta
                and os.path.isfile(frames_path(video_id))
                and os.path.isfile(audio_path(video_id))
            )
            if not ready:
                if video_id in failed_downloads:
                    failed_downloads.pop(video_id)
                    return "error"
                started = time.time()
                active_video_downloads[video_id] = started
                threading.Thread(target=download_worker, args=(video_id, started), daemon=True).start()
                return "processing"

            meta = video_meta[video_id]

        frames, frame_count = extract_frames(video_id, 0)
        return (
            f"{VIDEO_WIDTH}{DELIMITER}"
            f"{VIDEO_HEIGHT}{DELIMITER}"
            f"{meta['duration']}{DELIMITER}"
            f"{VIDEO_TARGET_FPS}{DELIMITER}"
            f"1{DELIMITER}"
            f"{meta['title']}{DELIMITER}"
            f"{meta['channel']}{DELIMITER}"
            f"{meta['views']}{DELIMITER}"
            f"{meta['likes']}{DELIMITER}"
            f"{meta['upload']}{DELIMITER}"
            f"{frame_count}{DELIMITER}"
            f"{frames}"
        )

    # Search request: search_{query} or popular
    if raw.startswith("search_") or raw.startswith("popular"):
        query = raw[7:] if raw.startswith("search_") else "popular videos"
        query = clean(query, 120)
        if not query:
            return ""
        with search_lock:
            cached = search_cache.get(query)
            if cached and time.time() - cached[0] < SEARCH_CACHE_SECONDS:
                search_cache.move_to_end(query)
                return cached[1]

        entries = search_videos(query, 12).get("entries", [])
        picked = []
        for entry in entries:
            picked.append(entry)
            if len(picked) >= SEARCH_RESULTS:
                break

        def get_thumb(entry):
            try:
                return extract_thumbnail(entry["id"])
            except Exception as e:
                print(f"[WARN] Thumbnail retrieval failed for {entry['id']}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=SEARCH_RESULTS) as pool:
            thumbs = list(pool.map(get_thumb, picked))

        final = ""
        for entry, thumb in zip(picked, thumbs):
            if thumb is None:
                continue
            thumb_hex, thumb_w = thumb
            final += (
                f"{entry['id']}{DELIMITER}"
                f"{clean(entry.get('title'), 200)}{DELIMITER}"
                f"{clean(entry.get('description'), 300)}{DELIMITER}"
                f"{clean(entry.get('uploader') or entry.get('channel'), 100)}{DELIMITER}"
                f"{format_views(entry.get('view_count'))}{DELIMITER}"
                f"{format_duration(entry.get('duration', 0))}{DELIMITER}"
                f"{thumb_w}{DELIMITER}"
                f"{THUMBNAIL_HEIGHT}{DELIMITER}"
                f"{thumb_hex}{DELIMITER}"
            )
        if final:
            with search_lock:
                search_cache[query] = (time.time(), final)
                search_cache.move_to_end(query)
                while len(search_cache) > SEARCH_CACHE_MAX_ENTRIES:
                    search_cache.popitem(last=False)
        return final

    return "error"

@app.route('/translate', methods=['GET'])
def translate():
    raw = request.args.get("text", "")
    if raw.startswith("HTTP "):
        raw = raw[5:]
    init_id = None
    try:
        m = CHUNK_RE.match(raw)
        if m and m.group(2) in video_meta and os.path.isfile(frames_path(m.group(2))):
            vid, start = m.group(2), int(float(m.group(1)))
            if start >= 1:
                body = chunk_body(vid, start)
                prefetch(vid, start + VIDEO_FRAMES_IN_RESPONSE)
                return respond_gz(body)

        m = INIT_RE.match(raw)
        if m:
            init_id = m.group(1)
            body = cache_get((init_id, 0)) if init_id in video_meta and init_id not in active_video_downloads else None
            if body is not None:
                prefetch(init_id, 1 + VIDEO_FRAMES_IN_RESPONSE)
                return respond_gz(body)

        result = handle_text(raw)
    except Exception as e:
        print(f"[ERROR] /translate exception: {e}")
        result = "error"

    if init_id and len(result) > 1000:
        body = pack(result)
        cache_put((init_id, 0), body)
        prefetch(init_id, 1 + VIDEO_FRAMES_IN_RESPONSE)
        return respond_gz(body)

    body = pack(result)
    return respond_gz(body)

# ---------------------------------------------------------
# AUDIO ROUTE (Scratch TTS Synth Bridge)
# ---------------------------------------------------------
@app.route('/synth', methods=['GET'])
def synth():
    raw = request.args.get("text", "")
    if raw.startswith("HTTP "):
        raw = raw[5:]

    # Instant Audio Stop: returns 50ms silence to cut off Scratch TTS
    if raw == "audio_stop":
        return Response(SILENT_WAV_BYTES, mimetype="audio/wav")

    # In-Memory Sliced Audio: audio_start_{seconds}_{video_id}
    if raw.startswith("audio_start_"):
        try:
            t_str, vid = raw[12:].split("_", 1)
            t = max(0.0, float(t_str))
        except ValueError:
            abort(400)
        if not VIDEO_ID_RE.match(vid) or not os.path.isfile(audio_path(vid)):
            abort(404)

        sliced_data = slice_audio_at(vid, t)
        if sliced_data is None:
            abort(404)
        return Response(sliced_data, mimetype="audio/mpeg")

    # Full Audio: audio_{video_id}
    if raw.startswith("audio_"):
        vid = raw[6:]
        if not VIDEO_ID_RE.match(vid) or not os.path.isfile(audio_path(vid)):
            abort(404)
        return send_file(audio_path(vid), mimetype="audio/mpeg")

    abort(404)

# ---------------------------------------------------------
# TAILSCALE INTEGRATION
# ---------------------------------------------------------
TAILSCALE_CLI  = r"C:\Program Files\Tailscale\tailscale.exe"
TAILSCALE_APP  = r"C:\Program Files\Tailscale\tailscale-ipn.exe"
FUNNEL_PORT    = 8080
_NO_WINDOW     = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _ts_cli():
    if os.path.exists(TAILSCALE_CLI):
        return TAILSCALE_CLI
    return shutil.which("tailscale")


def _ts(args, timeout=15):
    """Run a SHORT tailscale command. Returns (returncode, combined output)."""
    r = subprocess.run([_ts_cli()] + args, capture_output=True, text=True, timeout=timeout,
                       creationflags=_NO_WINDOW)
    return r.returncode, ((r.stdout or "") + (r.stderr or "")).strip()


def _ts_app_running() -> bool:
    """Return True if tailscale-ipn.exe is already in the process list."""
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq tailscale-ipn.exe", "/NH"],
            text=True, stderr=subprocess.DEVNULL, creationflags=_NO_WINDOW
        )
        return "tailscale-ipn.exe" in out
    except Exception:
        return False


def _funnel_url():
    """The public https URL if Funnel is already forwarding FUNNEL_PORT, else None."""
    try:
        _code, out = _ts(["funnel", "status"], 10)
    except Exception:
        return None
    m = re.search(r"(https://\S+?)\s+\(Funnel on\)", out)
    if m and f":{FUNNEL_PORT}" in out:
        return m.group(1)
    return None


def _run_funnel():
    """Make sure Funnel is forwarding FUNNEL_PORT.

    `tailscale funnel 8080` is a FOREGROUND command: the Funnel only exists while that process keeps
    running, so killing it after a timeout (the old code) switched the Funnel off again and the Scratch
    project could not connect. `--bg` stores the Funnel inside Tailscale and returns immediately."""
    try:
        # 1. wait for the Tailscale daemon (it can take a few seconds right after login/startup)
        for _ in range(20):
            try:
                if _ts(["status"], 5)[0] == 0:
                    break
            except Exception:
                pass
            time.sleep(1)

        url = _funnel_url()
        if url:
            print(f"[TAILSCALE] Funnel already on -> {url}")
            return

        # 2. start it in the background (stays up after this program exits, until `tailscale funnel reset`)
        proc = subprocess.Popen([_ts_cli(), "funnel", "--bg", str(FUNNEL_PORT)],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                creationflags=_NO_WINDOW)
        lines = []

        def _reader():
            for line in proc.stdout:
                line = line.rstrip()
                if line.strip():
                    lines.append(line)
                    print(f"[TAILSCALE] {line}")
        threading.Thread(target=_reader, daemon=True).start()
        try:
            proc.wait(timeout=25)
        except subprocess.TimeoutExpired:
            # First time ever: Tailscale prints a link and waits until you allow Funnel for this device.
            # Do NOT kill it - open the link above, approve, and it finishes by itself.
            print("[TAILSCALE] Still waiting. If a login.tailscale.com link was printed above, open it and "
                  "enable Funnel; the server keeps running meanwhile.")
            proc.wait()
        time.sleep(0.3)
        text = "\n".join(lines).lower()
        if proc.returncode not in (0, None) and ("not defined" in text or "unknown flag" in text):
            print("[TAILSCALE] Old Tailscale version without --bg: using `funnel 8080 on`")
            _ts(["funnel", str(FUNNEL_PORT), "on"], 30)

        url = _funnel_url()
        if url:
            print(f"[TAILSCALE] Funnel is ON -> {url}   (publicly reachable)")
        else:
            print("[TAILSCALE] Funnel did not start. Run  tailscale funnel status  to see why "
                  "(Funnel must be allowed for this device in the Tailscale admin console).")
    except Exception as e:
        print(f"[TAILSCALE] Funnel error: {e}")


def start_tailscale():
    """Launch the Tailscale tray app (if not running) then make sure Funnel is on for FUNNEL_PORT."""
    if not _ts_cli():
        print("[TAILSCALE] tailscale.exe not found - skipping Funnel setup")
        return

    if _ts_app_running():
        print("[TAILSCALE] Tailscale app already running")
    elif os.path.exists(TAILSCALE_APP):
        print("[TAILSCALE] Starting Tailscale app...")
        try:
            subprocess.Popen(
                [TAILSCALE_APP],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            )
            time.sleep(3)
        except Exception as e:
            print(f"[TAILSCALE] Could not start app: {e}")

    # runs in a background thread so server startup is never blocked
    print(f"[TAILSCALE] Making sure Funnel is on for port {FUNNEL_PORT} (background)...")
    threading.Thread(target=_run_funnel, daemon=True, name="tailscale-funnel").start()


def stop_tailscale():
    """Reset Funnel on shutdown so the port is no longer publicly exposed."""
    if not _ts_cli():
        return
    try:
        _ts(["funnel", "reset"], 10)
        print("[TAILSCALE] Funnel reset - port no longer publicly exposed")
    except Exception:
        pass


# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        print("[FATAL] ffmpeg and ffprobe must be installed and in PATH")
        sys.exit(1)

    print("=" * 64)
    print("ScratchTube v7 Server Initializing")
    print("=" * 64)
    print(f"[DEBUG] Video Profile: {VIDEO_WIDTH}x{VIDEO_HEIGHT} @ {VIDEO_TARGET_FPS} FPS (16:9)")
    print(f"[DEBUG] Frame Size: {FRAME_SIZE} bytes (RGB24), Chunk: {VIDEO_FRAMES_IN_RESPONSE} frames")
    print(f"[DEBUG] Downloads Directory: {DOWNLOADS_DIR}")
    print(f"[DEBUG] yt-dlp version: {yt_dlp.version.__version__}")
    print(f"[DEBUG] JavaScript runtime: {JS_RUNTIME or 'None (throttling may occur)'}")

    init_dl()
    start_tailscale()

    print("[DEBUG] Starting Waitress WSGI server on 0.0.0.0:8080...")
    try:
        serve(app, host="0.0.0.0", port=8080, threads=32, connection_limit=300, channel_timeout=90)
    finally:
        stop_tailscale()
