#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 kaggle_notebook.py — WZML-X Telegram Bot Runner for Kaggle
 WZFIX BUILD: v19.4.0-r19  (block list / do-not-allow)
================================================================================
 A single-cell Kaggle notebook script that:

   1. Clones WZML-X (wzv3 branch) from GitHub
   2. Reads config.env from a Kaggle dataset
   3. Applies 9 source patches + 2 inline sed patches
   4. Installs system packages (aria2, ffmpeg, etc.) and Python deps
   5. Downloads cloudflared, starts a quick tunnel on port 8080
   6. Syncs the tunnel URL to a Cloudflare Worker
   7. Injects the Worker URL as BASE_URL into config.env
   8. Sends Telegram + ntfy.sh notifications (start, stream_ready, stop, crash)
   9. Runs the bot via `python -m bot` (no Docker)
  10. Self-terminates after 9.5–10.0 hours (graceful SIGINT)
  11. Random 10–120 s startup delay for fingerprint variation
  12. Cleans up old downloads on startup and shutdown
 13. Round 1 (v15.7): per-user daily bandwidth quota (default 15 GB,
     owner-adjustable), download library with /find, /usage, and DB
     stats/cleanup commands

 Designed for Kaggle's /kaggle/working/ (~73 GB temp disk, 30 GB RAM).
 Kaggle kills notebooks at 12 h; we self-terminate at ~9.5–10 h to stay safe.
================================================================================
"""

# ============================================================================
# SECTION 0 — IMPORTS & CONSTANTS
# ============================================================================

import os
import sys
import re
import json
import time
import random
import signal
import shutil
import socket
import base64
import subprocess
import urllib.request
import urllib.parse
import urllib.error
import threading
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# IST timezone (UTC+5:30)
# ---------------------------------------------------------------------------
IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
KAGGLE_WORKING = "/kaggle/working"
WZMLX_DIR = os.path.join(KAGGLE_WORKING, "WZML-X")
CONFIG_SRC = "/kaggle/input/wzmlx-config/config.env"
CONFIG_DST = os.path.join(WZMLX_DIR, "config.env")
CLOUDFLARED_BIN = os.path.join(KAGGLE_WORKING, "cloudflared")
PATCH_TMP_DIR = os.path.join(KAGGLE_WORKING, "_patches")
DOWNLOAD_DIR_DEFAULT = os.path.join(KAGGLE_WORKING, "downloads")

# ---------------------------------------------------------------------------
# Cloudflared download URL (latest release, linux-amd64)
# ---------------------------------------------------------------------------
CLOUDFLARED_URL = (
    "https://github.com/cloudflare/cloudflared/releases/latest/download/"
    "cloudflared-linux-amd64"
)

# ---------------------------------------------------------------------------
# Tunnel capture: regex for https://*.trycloudflare.com
# ---------------------------------------------------------------------------
TUNNEL_URL_RE = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")

# ---------------------------------------------------------------------------
# Self-termination window (seconds): 9.5 h to 10.0 h
# ---------------------------------------------------------------------------
MIN_RUNTIME = int(9.5 * 3600)   # 34200 s
MAX_RUNTIME = int(10.0 * 3600)  # 36000 s

# ---------------------------------------------------------------------------
# Startup delay window (seconds): 10 s to 120 s
# ---------------------------------------------------------------------------
MIN_STARTUP_DELAY = 10
MAX_STARTUP_DELAY = 120

# ---------------------------------------------------------------------------
# Bot process handle (global so signal handlers can reach it)
# ---------------------------------------------------------------------------
BOT_PROCESS = None
TUNNEL_PROCESS = None
SHUTDOWN_EVENT = threading.Event()
NOTIFIED_STREAM_READY = False


def _r19_start_watchdog():
    """WZFIX_R19 (v15.82): a `kaggle kernels push` while a session is
    already running does NOT restart it - the pushed version sits in
    the repo while the old bot keeps serving. This daemon thread
    polls the repo WZFIX BUILD marker; when it stops
    matching THIS build (2 consecutive mismatches), the whole
    session exits so the new version's run can take over. Network
    errors never trigger an exit."""
    import urllib.request

    ver = "v19.4.0-r19"

    def _r19_poll():
        import time as _r19t

        _r19_misses = 0
        while not SHUTDOWN_EVENT.is_set():
            _r19t.sleep(120)
            if SHUTDOWN_EVENT.is_set():
                break
            try:
                _r19url = (
                    "https://raw.githubusercontent.com/vot1122/"
                    "Kaggle-auto/main/kaggle_notebook.py?t="
                    + str(int(_r19t.time()))
                )
                _r19req = urllib.request.Request(
                    _r19url, headers={"User-Agent": "WZFIX-R19-Watchdog"}
                )
                with urllib.request.urlopen(_r19req, timeout=20) as _r19r:
                    _r19head = _r19r.read(4096).decode("utf-8", "replace")
                _r19m = ""
                for _r19ln in _r19head.splitlines():
                    if "WZFIX BUILD:" in _r19ln:
                        _r19parts = _r19ln.split(":", 1)[1].split()
                        if _r19parts:
                            _r19m = _r19parts[0]
                        break
                if not _r19m or _r19m == ver:
                    _r19_misses = 0
                    continue
                _r19_misses += 1
                log(
                    f"r19 watchdog: repo build {_r19m} != running {ver} "
                    f"({_r19_misses}/2)"
                )
                if _r19_misses >= 2:
                    log(
                        f"r19 watchdog: new build {_r19m} in repo - "
                        "exiting so its run takes over",
                        "WARN",
                    )
                    try:
                        notify(
                            parse_config(CONFIG_SRC),
                            "stop",
                            f"Restarting for new build {_r19m}",
                        )
                    except Exception:
                        pass
                    os._exit(0)
            except Exception:
                _r19_misses = 0

    _r19th = threading.Thread(target=_r19_poll, daemon=True)
    _r19th.start()


# ============================================================================
# SECTION 1 — LOGGING HELPER
# ============================================================================

_LOG_RING = []  # WZFIX r25c29: last 800 kernel log lines (diagnostics)


def log(msg, level="INFO"):
    """Print a timestamped log line in IST."""
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    _LOG_RING.append(line)  # WZFIX r25c29
    del _LOG_RING[:-800]
    print(line, flush=True)


def now_ist_str():
    """Return a human-readable IST timestamp."""
    return datetime.now(IST).strftime("%d %b %Y, %I:%M:%S %p IST")


# ============================================================================
# SECTION 2 — CONFIG PARSING
# ============================================================================

def parse_config(config_path):
    """
    Read a WZML-X config.env file and return a dict of key→value pairs.

    The config file uses ``KEY = "value"`` or ``KEY = value`` syntax.
    Lines starting with ``#`` are comments. Blank lines are skipped.
    The sentinel ``_____REMOVE_THIS_LINE_____=True`` is ignored.
    """
    config = {}
    if not os.path.isfile(config_path):
        log(f"Config file not found: {config_path}", "WARN")
        return config
    with open(config_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "REMOVE_THIS_LINE" in line:
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1].strip()
            if key:
                config[key] = value
    return config


# ============================================================================
# SECTION 2.5 — ERROR DIAGNOSIS ENGINE
# ============================================================================
# Every error signature below is translated into its actual cause and the fix,
# so the log explains WHY something failed instead of just showing the error.

DIAGNOSES = [
    ("AUTH_KEY_UNREGISTERED",
     "CAUSE: Telegram no longer recognizes this session string - it was revoked "
     "(logging out, regenerating a new session, or Telegram invalidating it). "
     "FIX: run /exportsession on a working bot with the same account, copy the "
     "new string from Saved Messages, replace USER_SESSION_STRING in the Kaggle "
     "dataset config.env, save the dataset, re-run."),
    ("AUTH_KEY_DUPLICATED",
     "CAUSE: the same session string is being used by two running instances at "
     "the same time (e.g. the Actions bot and the Kaggle bot both running). "
     "FIX: stop one of the two instances, or give each its own session string."),
    ("terminated by other getUpdates request",
     "CAUSE: another running instance is polling the same bot token "
     "(two bots, one token). FIX: stop the other instance (pause the Actions "
     "workflow or stop the Kaggle kernel) - one token, one instance."),
    ("API_ID_INVALID",
     "CAUSE: TELEGRAM_API / TELEGRAM_HASH values are wrong or belong to a "
     "different app. FIX: verify both values at my.telegram.org."),
    ("PHONE_CODE_INVALID",
     "CAUSE: OTP entered without spaces. FIX: enter the login code with "
     "spaces between digits, e.g. '1 2 3 4 5'."),
    ("error code: 1010",
     "CAUSE: Cloudflare's browser-integrity check blocked the request before it "
     "reached the Worker (non-browser User-Agent). "
     "FIX: already handled - the notebook sends a browser User-Agent. If it "
     "still appears, the Worker's security settings are blocking API traffic."),
    ("unauthorized",
     "CAUSE (Worker): the WORKER_SECRET in the dataset config.env does not "
     "match the WORKER_SECRET variable set in the Cloudflare Worker's settings. "
     "FIX: compare both values and make them identical."),
    ("ModuleNotFoundError",
     "CAUSE: a Python module that exists only in the official Docker image was "
     "missing. FIX: report the module name - it needs a shim or pip install "
     "added to the notebook."),
    ("ACCESS_TOKEN_INVALID",
     "COSMETIC: the Telegraph token is invalid, so the bot cannot create its "
     "log page. Everything else works; replace TELEGRAPH_TOKEN if you want it."),
    ("FloodWait",
     "CAUSE: Telegram rate limit hit. The bot waits it out automatically - "
     "no action needed unless it happens constantly."),
    ("ECONNREFUSED",
     "CAUSE: a local service the bot expects is not running. If it mentions "
     "port 6800 it is the aria2 daemon, 8090/8080 qBittorrent/WebUI, 8091 "
     "the stream server. FIX: report it - the notebook will start it."),
    ("latin-1",
     "CAUSE: a non-ASCII character (usually an em-dash) in a header the bot "
     "encodes as latin-1. FIX: already handled by the notebook's sanitizer."),
    ("ServerSelectionTimeoutError",
     "CAUSE: cannot reach MongoDB. FIX: check DATABASE_URL in the dataset "
     "config.env and the cluster's network access list."),
    ("OperationFailure",
     "CAUSE: MongoDB rejected the credentials. FIX: check the username/password "
     "inside DATABASE_URL."),
]


def explain_errors(text):
    """Return human-readable cause+fix for every known signature in text."""
    hits = []
    for key, explanation in DIAGNOSES:
        if key in text:
            hits.append((key, explanation))
    return hits


def log_diagnosis(text, seen=None):
    """Log explanations for known error signatures found in text.

    `seen` is a set of keys already explained (pass a persistent set to avoid
    repeating the same diagnosis for every matching line).
    """
    for key, explanation in explain_errors(text):
        if seen is not None:
            if key in seen:
                continue
            seen.add(key)
        log(f"DIAGNOSIS >> {explanation}", "WARN")


# ============================================================================
# SECTION 3 — NOTIFICATION (Telegram + ntfy.sh)
# ============================================================================

def send_telegram(bot_token, chat_id, text):
    """
    Send a message via the Telegram Bot API using urllib.

    Uses sendMessage endpoint. Text is sent as-is (HTML parse mode is
    intentionally avoided to prevent parsing errors with arbitrary content).
    """
    if not bot_token or not chat_id:
        log("Telegram: missing bot_token or chat_id, skipping", "WARN")
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                return True
            log(f"Telegram response: {resp.status}", "WARN")
            return False
    except Exception as e:
        log(f"Telegram notification failed: {e}", "ERROR")
        return False


def send_ntfy(topic, title, message, tags=None):
    """
    Send a notification via ntfy.sh using urllib.

    The topic acts as the pub/sub channel — anyone subscribed to it receives
    the message. No authentication required.
    """
    if not topic:
        log("ntfy: missing topic, skipping", "WARN")
        return False
    url = f"https://ntfy.sh/{topic}"
    # HTTP headers must be latin-1 safe (em dash in titles crashes urllib)
    safe_title = title.encode("latin-1", "replace").decode("latin-1")
    headers = {
        "Title": safe_title,
        "Priority": "default",
    }
    if tags:
        headers["Tags"] = tags.encode("latin-1", "replace").decode("latin-1")
    data = message.encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 201, 202):
                return True
            log(f"ntfy response: {resp.status}", "WARN")
            return False
    except Exception as e:
        log(f"ntfy notification failed: {e}", "ERROR")
        return False


def notify(config, event, extra=""):
    """
    Dispatch a notification to both Telegram and ntfy.sh.

    Events: start, stream_ready, stop, crash

    Builds a human-readable message with the event type, IST timestamp,
    and any extra context (e.g. the tunnel URL).
    """
    event_emoji = {
        "start": "🟢",
        "stream_ready": "🌐",
        "stop": "🔴",
        "crash": "💥",
    }
    emoji = event_emoji.get(event, "ℹ️")
    bot_token = config.get("BOT_TOKEN", "")
    owner_id = config.get("OWNER_ID", "")
    ntfy_topic = config.get("NTFY_TOPIC", "")

    header = f"{emoji} WZML-X Kaggle — {event.upper()}"
    timestamp = now_ist_str()
    body_parts = [header, f"Time: {timestamp}"]
    if extra:
        body_parts.append(extra)
    text = "\n".join(body_parts)

    if bot_token and owner_id:
        send_telegram(bot_token, owner_id, text)
    else:
        log("Telegram skipped: BOT_TOKEN or OWNER_ID not set", "WARN")

    ntfy_title = f"WZML-X — {event}"
    ntfy_tags = event
    if ntfy_topic:
        send_ntfy(ntfy_topic, ntfy_title, text, tags=ntfy_tags)
    else:
        log("ntfy skipped: NTFY_TOPIC not set", "WARN")


# ============================================================================
# SECTION 4 — EMBEDDED PATCH SCRIPTS (base64-encoded)
# ============================================================================
# Each patch is a standalone Python script that takes a file path as argv[1]
# and modifies that file in-place. The scripts are base64-encoded here to
# avoid quoting issues, decoded at runtime, written to temp files, and run
# via subprocess against the corresponding WZML-X source file.
# ============================================================================

# Each entry: (patch_name, target_file_relative_to_WZMLX, base64_encoded_script)
# The script is decoded at runtime and run via subprocess against the target.
PATCH_DATA = [
    ('patch_cm.py',
     'bot/core/config_manager.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9jbS5weSAtIFByZXZlbnQgTW9uZ29EQiBmcm9tIG92ZXJyaWRpbmcgQkFTRV9VUkwgd2l0aCB0cnljbG91ZGZsYXJlIFVSTHMKCk1vZGlmaWVzIGNvbmZpZ19tYW5hZ2VyLnB5IGluLXBsYWNlLiBXaGVuIHRoZSBib3QgbG9hZHMgY29uZmlnIGZyb20gTW9uZ29EQgoodmlhIGxvYWRfZGljdCksIGEgc3RhbGUgQkFTRV9VUkwgY29udGFpbmluZyAidHJ5Y2xvdWRmbGFyZS5jb20iIGZyb20gYQpwcmV2aW91cyBydW4gd291bGQgb3ZlcnJpZGUgdGhlIHN0YWJsZSBXb3JrZXIgVVJMIGluamVjdGVkIGJ5IHRoaXMgbm90ZWJvb2suCgpUaGlzIHBhdGNoIGFkZHMgYSBndWFyZDogaWYgdGhlIEJBU0VfVVJMIHZhbHVlIGZyb20gTW9uZ29EQiBjb250YWlucwoidHJ5Y2xvdWRmbGFyZS5jb20iLCBpdCBpcyBza2lwcGVkIChub3QgbG9hZGVkKSwgcHJlc2VydmluZyB0aGUgV29ya2VyIFVSTC4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCiMgVGFyZ2V0IHRoZSBsb2FkX2RpY3QgbWV0aG9kIC0tIGFkZCBhIGd1YXJkIGZvciBCQVNFX1VSTApvbGRfYmxvY2sgPSAoCiAgICAnICAgIGRlZiBsb2FkX2RpY3QoY2xzLCBjb25maWdfZGljdCk6XG4nCiAgICAnICAgICAgICBmb3Iga2V5LCB2YWx1ZSBpbiBjb25maWdfZGljdC5pdGVtcygpOlxuJwogICAgJyAgICAgICAgICAgIGlmIGhhc2F0dHIoY2xzLCBrZXkpOlxuJwogICAgJyAgICAgICAgICAgICAgICBpZiBrZXkgPT0gIkRFRkFVTFRfVVBMT0FEIiBhbmQgdmFsdWUgIT0gImdkIjpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gInJjIlxuJwogICAgJyAgICAgICAgICAgICAgICBlbGlmIGtleSBpbiBbXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiQkFTRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIlJDTE9ORV9TRVJWRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIklOREVYX1VSTCIsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiU0VBUkNIX0FQSV9MSU5LIixcbicKICAgICcgICAgICAgICAgICAgICAgXTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIGlmIHZhbHVlOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gdmFsdWUuc3RyaXAoIi8iKScKKQpuZXdfYmxvY2sgPSAoCiAgICAnICAgIGRlZiBsb2FkX2RpY3QoY2xzLCBjb25maWdfZGljdCk6XG4nCiAgICAnICAgICAgICBmb3Iga2V5LCB2YWx1ZSBpbiBjb25maWdfZGljdC5pdGVtcygpOlxuJwogICAgJyAgICAgICAgICAgIGlmIGhhc2F0dHIoY2xzLCBrZXkpOlxuJwogICAgJyAgICAgICAgICAgICAgICBpZiBrZXkgPT0gIkRFRkFVTFRfVVBMT0FEIiBhbmQgdmFsdWUgIT0gImdkIjpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gInJjIlxuJwogICAgJyAgICAgICAgICAgICAgICBlbGlmIGtleSBpbiBbXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiQkFTRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIlJDTE9ORV9TRVJWRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIklOREVYX1VSTCIsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiU0VBUkNIX0FQSV9MSU5LIixcbicKICAgICcgICAgICAgICAgICAgICAgXTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIGlmIHZhbHVlOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gdmFsdWUuc3RyaXAoIi8iKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIyBHdWFyZDogbmV2ZXIgbGV0IE1vbmdvREIgb3ZlcnJpZGUgQkFTRV9VUkwgd2l0aCBhXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAjIHRyeWNsb3VkZmxhcmUuY29tIHF1aWNrLXR1bm5lbCBVUkwuIFRoZSBub3RlYm9va1xuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIyBpbmplY3RzIGEgc3RhYmxlIFdvcmtlciBVUkw7IE1vbmdvREIgbWF5IGNhcnJ5IGFcbicKICAgICcgICAgICAgICAgICAgICAgICAgICMgc3RhbGUgdHJ5Y2xvdWRmbGFyZSBVUkwgZnJvbSBhIHByZXZpb3VzIHJ1bi5cbicKICAgICcgICAgICAgICAgICAgICAgICAgIGlmIChcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICBrZXkgPT0gIkJBU0VfVVJMIlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIGFuZCBpc2luc3RhbmNlKHZhbHVlLCBzdHIpXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgYW5kICJ0cnljbG91ZGZsYXJlLmNvbSIgaW4gdmFsdWVcbicKICAgICcgICAgICAgICAgICAgICAgICAgICk6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgaW1wb3J0IGxvZ2dpbmdcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICBsb2dnaW5nLmdldExvZ2dlcihfX25hbWVfXykud2FybmluZyhcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgImNvbmZpZ19tYW5hZ2VyOiBza2lwcGluZyBzdGFsZSB0cnljbG91ZGZsYXJlICJcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgIkJBU0VfVVJMIGZyb20gTW9uZ29EQjogJXMiLCB2YWx1ZVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIClcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICBjb250aW51ZScKKQoKaWYgb2xkX2Jsb2NrIGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZF9ibG9jaywgbmV3X2Jsb2NrLCAxKQogICAgcHJpbnQoInBhdGNoX2NtOiBhZGRlZCB0cnljbG91ZGZsYXJlIGd1YXJkIHRvIGxvYWRfZGljdCIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfY206IFdBUk5JTkcgLS0gbG9hZF9kaWN0IHRhcmdldCBub3QgZm91bmQgKGFscmVhZHkgcGF0Y2hlZD8pIikKCiMgQWxzbyBwYXRjaCBsb2FkX2NvbmZpZygpIGZvciB0aGUgc2FtZSBwcm90ZWN0aW9uCm9sZF9sb2FkX2NvbmZpZyA9ICgKICAgICcgICAgQGNsYXNzbWV0aG9kXG4nCiAgICAnICAgIGRlZiBsb2FkX2NvbmZpZyhjbHMpOlxuJwogICAgJyAgICAgICAgdHJ5OlxuJwogICAgJyAgICAgICAgICAgIHNldHRpbmdzID0gaW1wb3J0X21vZHVsZSgiY29uZmlnIilcbicKICAgICcgICAgICAgIGV4Y2VwdCBNb2R1bGVOb3RGb3VuZEVycm9yOlxuJwogICAgJyAgICAgICAgICAgIHJldHVyblxuJwogICAgJyAgICAgICAgZm9yIGF0dHIgaW4gZGlyKHNldHRpbmdzKTpcbicKICAgICcgICAgICAgICAgICBpZiBoYXNhdHRyKGNscywgYXR0cik6XG4nCiAgICAnICAgICAgICAgICAgICAgIHZhbHVlID0gZ2V0YXR0cihzZXR0aW5ncywgYXR0cilcbicKICAgICcgICAgICAgICAgICAgICAgaWYgbm90IHZhbHVlOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgY29udGludWUnCikKbmV3X2xvYWRfY29uZmlnID0gKAogICAgJyAgICBAY2xhc3NtZXRob2RcbicKICAgICcgICAgZGVmIGxvYWRfY29uZmlnKGNscyk6XG4nCiAgICAnICAgICAgICB0cnk6XG4nCiAgICAnICAgICAgICAgICAgc2V0dGluZ3MgPSBpbXBvcnRfbW9kdWxlKCJjb25maWciKVxuJwogICAgJyAgICAgICAgZXhjZXB0IE1vZHVsZU5vdEZvdW5kRXJyb3I6XG4nCiAgICAnICAgICAgICAgICAgcmV0dXJuXG4nCiAgICAnICAgICAgICBmb3IgYXR0ciBpbiBkaXIoc2V0dGluZ3MpOlxuJwogICAgJyAgICAgICAgICAgIGlmIGhhc2F0dHIoY2xzLCBhdHRyKTpcbicKICAgICcgICAgICAgICAgICAgICAgdmFsdWUgPSBnZXRhdHRyKHNldHRpbmdzLCBhdHRyKVxuJwogICAgJyAgICAgICAgICAgICAgICBpZiBub3QgdmFsdWU6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBjb250aW51ZVxuJwogICAgJyAgICAgICAgICAgICAgICAjIEd1YXJkOiBza2lwIHRyeWNsb3VkZmxhcmUgQkFTRV9VUkwgZnJvbSBjb25maWcucHlcbicKICAgICcgICAgICAgICAgICAgICAgaWYgKFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgYXR0ciA9PSAiQkFTRV9VUkwiXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBhbmQgaXNpbnN0YW5jZSh2YWx1ZSwgc3RyKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgYW5kICJ0cnljbG91ZGZsYXJlLmNvbSIgaW4gdmFsdWVcbicKICAgICcgICAgICAgICAgICAgICAgKTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIGltcG9ydCBsb2dnaW5nXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBsb2dnaW5nLmdldExvZ2dlcihfX25hbWVfXykud2FybmluZyhcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAiY29uZmlnX21hbmFnZXI6IHNraXBwaW5nIHRyeWNsb3VkZmxhcmUgIlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICJCQVNFX1VSTCBmcm9tIGNvbmZpZy5weTogJXMiLCB2YWx1ZVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgY29udGludWUnCikKCmlmIG9sZF9sb2FkX2NvbmZpZyBpbiBzcmM6CiAgICBzcmMgPSBzcmMucmVwbGFjZShvbGRfbG9hZF9jb25maWcsIG5ld19sb2FkX2NvbmZpZywgMSkKICAgIHByaW50KCJwYXRjaF9jbTogYWRkZWQgdHJ5Y2xvdWRmbGFyZSBndWFyZCB0byBsb2FkX2NvbmZpZyIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfY206IGxvYWRfY29uZmlnIHRhcmdldCBub3QgZm91bmQgKGFscmVhZHkgcGF0Y2hlZD8pIikKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKHNyYykKcHJpbnQoInBhdGNoX2NtOiBkb25lIikK"),
    ('patch_r25c16.py',
     'bot/helper/telegram_helper/tg_stream.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMTYucHkgLSBwZXItY2hhdCB1c2VyLWFjY291bnQgc3RpY2tpbmVzcyAoa2l0LWJhc2VkKS4KCnIyNWMxMi9yMjVjMTUgd2VyZSBidWlsdCBvbiBwYXRjaDdfdXNlcidzIFVzZXJTdHJlYW0gY2xhc3MgLSBidXQgdGhhdApwYXRjaCBpcyBpbiB0aGUgbm90ZWJvb2sncyByZXRpcmVkIHNldCAoc3VwZXJzZWRlZCBieSB0aGUgV1pNTC1YLUJvdApraXQpLCBzbyAnY2xhc3MgVXNlclN0cmVhbScgbmV2ZXIgZXhpc3RlZCBpbiB0Z19zdHJlYW0gYW5kIGJvdGgKcGF0Y2hlcyBzaWxlbnRseSBza2lwcGVkLiBSZXN1bHQ6IHBsYXlsaXN0cyBzZXJ2ZWQgdmlhIHRoZSBib3QKY2xpZW50cyBmYWlsZWQgd2l0aCBTdHJlYW1Hb25lIGFuZCBubyBmYWxsYmFjayBldmVyIHJhbi4KCnIyNWMxNiB1c2VzIHRoZSBraXQncyBvd24gdXNlci1zZXNzaW9uIGZ1bmN0aW9ucyBpbnN0ZWFkCihib3QvaGVscGVyL3VzZXJfc3RyZWFtX21vZHVsZS5weSAtIGluc3RhbGxlZCBieQphcHBseV91c2VycmVwb19wYXRjaGVzKCkgcmlnaHQgQUZURVIgdGhpcyBwYXRjaCBydW5zLCBzbyB0aGUgaW1wb3J0CmlzIGRvbmUgbGF6aWx5IGF0IHJlcXVlc3QgdGltZSk6CiAgLSBvcGVuX3N0cmVhbV91c2VyIC8gcHJvYmVfdXNlcjogc3RyZWFtIGFuZCBwcm9iZSB2aWEgdGhlIHVzZXIncwogICAgcmVhbCBzZXNzaW9uIChVU0VSX1NFU1NJT05fU1RSSU5HKS4KT25jZSBhIGNoYXQgbmVlZGVkIHRoZSB1c2VyIGFjY291bnQgKGhlbHBlciBib3RzIGNhbid0IHJlYWQgdGhlCm1haW4gYm90J3MgcGxheWxpc3QgcG9zdHMpLCBldmVyeSBsYXRlciBzdHJlYW0gb3IgcHJvYmUgZm9yIHRoYXQKY2hhdCBpcyBzZXJ2ZWQgYnkgdGhlIHVzZXIgYWNjb3VudCBkaXJlY3RseS4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMTYiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMTY6IGFscmVhZHkgYXBwbGllZCIpCiAgICBzeXMuZXhpdCgwKQoKc3JjID0gc3JjICsgIiIiCgojIFdaRklYIHIyNWMxNjogcGVyLWNoYXQgdXNlci1hY2NvdW50IHN0aWNraW5lc3MgdmlhIHRoZSBXWk1MLVgtQm90CiMga2l0J3MgdXNlciBzZXNzaW9uIChwYXRjaDcncyBVc2VyU3RyZWFtIGlzIHJldGlyZWQgLSB0aGlzIG5vIGxvbmdlcgojIGRlcGVuZHMgb24gaXQpLiBUaGUga2l0IGluc3RhbGxzIHVzZXJfc3RyZWFtX21vZHVsZS5weSBhZnRlciB0aGlzCiMgcGF0Y2ggcnVucywgc28gaW1wb3J0IGxhemlseSBhdCByZXF1ZXN0IHRpbWUuCl9vcmlnX29wZW5fc3RyZWFtXzI1YzE2ID0gb3Blbl9zdHJlYW0KX1VTRVJfUFJFRl8yNUMxNiA9IHNldCgpCgoKYXN5bmMgZGVmIF93el91c2VyX29wZW5fMjVjMTYoY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXI9Tm9uZSk6CiAgICBmcm9tIC4udXNlcl9zdHJlYW1fbW9kdWxlIGltcG9ydCBvcGVuX3N0cmVhbV91c2VyCgogICAgcmV0dXJuIGF3YWl0IG9wZW5fc3RyZWFtX3VzZXIoY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXI9dmlld2VyKQoKCmFzeW5jIGRlZiBfd3pfdXNlcl9wcm9iZV8yNWMxNihjaGF0X2lkLCBtc2dfaWQpOgogICAgZnJvbSAuLnVzZXJfc3RyZWFtX21vZHVsZSBpbXBvcnQgcHJvYmVfdXNlcgoKICAgIHJldHVybiBhd2FpdCBwcm9iZV91c2VyKGNoYXRfaWQsIG1zZ19pZCkKCgphc3luYyBkZWYgb3Blbl9zdHJlYW0oY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXI9Tm9uZSk6CiAgICAjIHIyNWMxNjogb25jZSBhIGNoYXQgbmVlZGVkIHRoZSB1c2VyIGFjY291bnQgKGl0cyBzb25ncyBhcmUKICAgICMgcG9zdGVkIGJ5IHRoZSBtYWluIGJvdCwgdW5yZWFkYWJsZSBieSB0aGUgaGVscGVyIGJvdHMpLCBldmVyeQogICAgIyBsYXRlciBzdHJlYW0gZnJvbSB0aGF0IGNoYXQgaXMgc2VydmVkIGJ5IHRoZSB1c2VyIGFjY291bnQKICAgICMgZGlyZWN0bHkgLSBubyBmYWlsZWQgYm90IGF0dGVtcHRzLCBubyBwZXItc29uZyBkZWxheQogICAgaWYgY2hhdF9pZCBpbiBfVVNFUl9QUkVGXzI1QzE2OgogICAgICAgIHJldHVybiBhd2FpdCBfd3pfdXNlcl9vcGVuXzI1YzE2KGNoYXRfaWQsIG1zZ19pZCwga2luZCwgdmlld2VyKQogICAgdHJ5OgogICAgICAgIHJldHVybiBhd2FpdCBfb3JpZ19vcGVuX3N0cmVhbV8yNWMxNigKICAgICAgICAgICAgY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXIKICAgICAgICApCiAgICBleGNlcHQgU3RyZWFtR29uZToKICAgICAgICBzdCA9IGF3YWl0IF93el91c2VyX29wZW5fMjVjMTYoY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXIpCiAgICAgICAgX1VTRVJfUFJFRl8yNUMxNi5hZGQoY2hhdF9pZCkKICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgZiJyMjVjMTY6IGNoYXQge2NoYXRfaWR9IHNlcnZlZCB2aWEgdXNlciBhY2NvdW50IC0gIgogICAgICAgICAgICBmInByZWZlcnJpbmcgdGhlIHVzZXIgYWNjb3VudCBmb3IgdGhpcyBjaGF0IG5vdyIKICAgICAgICApCiAgICAgICAgcmV0dXJuIHN0CgoKX29yaWdfcHJvYmVfMjVjMTYgPSBwcm9iZQoKCmFzeW5jIGRlZiBwcm9iZShjaGF0X2lkLCBtc2dfaWQpOgogICAgIyByMjVjMTY6IHBsYXlsaXN0IHBhZ2UgcHJvYmVzIGV2ZXJ5IHNvbmcgLSBzYW1lIHBlci1jaGF0CiAgICAjIHVzZXItYWNjb3VudCBwcmVmZXJlbmNlCiAgICBpZiBjaGF0X2lkIGluIF9VU0VSX1BSRUZfMjVDMTY6CiAgICAgICAgcmV0dXJuIGF3YWl0IF93el91c2VyX3Byb2JlXzI1YzE2KGNoYXRfaWQsIG1zZ19pZCkKICAgIHRyeToKICAgICAgICByZXR1cm4gYXdhaXQgX29yaWdfcHJvYmVfMjVjMTYoY2hhdF9pZCwgbXNnX2lkKQogICAgZXhjZXB0IFN0cmVhbUdvbmU6CiAgICAgICAgb3V0ID0gYXdhaXQgX3d6X3VzZXJfcHJvYmVfMjVjMTYoY2hhdF9pZCwgbXNnX2lkKQogICAgICAgIF9VU0VSX1BSRUZfMjVDMTYuYWRkKGNoYXRfaWQpCiAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAgICAgIGYicjI1YzE2OiBwcm9iZSB2aWEgdXNlciBhY2NvdW50IGZvciBjaGF0IHtjaGF0X2lkfSAtICIKICAgICAgICAgICAgZiJwcmVmZXJyaW5nIHRoZSB1c2VyIGFjY291bnQgZm9yIHRoaXMgY2hhdCBub3ciCiAgICAgICAgKQogICAgICAgIHJldHVybiBvdXQKIiIiCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaF9yMjVjMTY6IGtpdC1iYXNlZCBzdGlja3kgdXNlci1hY2NvdW50IGZhbGxiYWNrIGluc3RhbGxlZCIpCg=="),
    ('patch_r25c17_db.py',
     'bot/helper/ext_utils/db_handler.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMTdfZGIucHkgLSBwbGF5bGlzdCBtZXRhZGF0YSBpbiB0aGUgcGxheWxpc3QgZG9jdW1lbnRzLgoKZ2V0X3BsYXlsaXN0KCkgZ2FpbnMgYSAibWV0YSIgZmllbGQgKHt0b2tlbjoge24sIGEsIGMsIGR9fSkgYW5kIGEKc2V0X3BsYXlsaXN0X21ldGEoKSBoZWxwZXIgd3JpdGVzIGl0LiBUaGUgcGFnZS9BUEkgcmVuZGVyIGNsZWFuCm5hbWVzLCBjb3ZlcnMgYW5kIGR1cmF0aW9ucyBmcm9tIHRoZXJlLgoiIiIKaW1wb3J0IHN5cwoKcGF0aCA9IHN5cy5hcmd2WzFdCndpdGggb3BlbihwYXRoLCAiciIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBzcmMgPSBmLnJlYWQoKQoKaWYgInIyNWMxNyIgaW4gc3JjOgogICAgcHJpbnQoInBhdGNoX3IyNWMxN19kYjogYWxyZWFkeSBhcHBsaWVkIikKICAgIHN5cy5leGl0KDApCgojIDEpIGdldF9wbGF5bGlzdDogcmV0dXJuIG1ldGEgdG9vCm9sZF9nZXQgPSAnJycgICAgICAgIHJldHVybiB7CiAgICAgICAgICAgICJuYW1lIjogZG9jLmdldCgibmFtZSIpIG9yICIiLAogICAgICAgICAgICAiaXRlbXMiOiBkb2MuZ2V0KCJpdGVtcyIpIG9yIFtdLAogICAgICAgICAgICAicHVybCI6IGRvYy5nZXQoInB1cmwiKSwKICAgICAgICAgICAgInBjaWQiOiBkb2MuZ2V0KCJwY2lkIiksCiAgICAgICAgICAgICJwbWlkIjogZG9jLmdldCgicG1pZCIpLAogICAgICAgIH0nJycKbmV3X2dldCA9ICcnJyAgICAgICAgcmV0dXJuIHsKICAgICAgICAgICAgIm5hbWUiOiBkb2MuZ2V0KCJuYW1lIikgb3IgIiIsCiAgICAgICAgICAgICJpdGVtcyI6IGRvYy5nZXQoIml0ZW1zIikgb3IgW10sCiAgICAgICAgICAgICJwdXJsIjogZG9jLmdldCgicHVybCIpLAogICAgICAgICAgICAicGNpZCI6IGRvYy5nZXQoInBjaWQiKSwKICAgICAgICAgICAgInBtaWQiOiBkb2MuZ2V0KCJwbWlkIiksCiAgICAgICAgICAgICMgV1pGSVggcjI1YzE3OiBwZXItc29uZyBkaXNwbGF5IG1ldGFkYXRhCiAgICAgICAgICAgICJtZXRhIjogZG9jLmdldCgibWV0YSIpIG9yIHt9LAogICAgICAgIH0nJycKYXNzZXJ0IHNyYy5jb3VudChvbGRfZ2V0KSA9PSAxLCAiZ2V0X3BsYXlsaXN0IHJldHVybiBhbmNob3IiCnNyYyA9IHNyYy5yZXBsYWNlKG9sZF9nZXQsIG5ld19nZXQsIDEpCgojIDIpIHNldF9wbGF5bGlzdF9tZXRhIGhlbHBlciwgcmlnaHQgYmVmb3JlIGdldF9wbGF5bGlzdApvbGRfZGVmID0gJycnICAgIGFzeW5jIGRlZiBnZXRfcGxheWxpc3Qoc2VsZiwgdG9rZW4pOicnJwpuZXdfZGVmID0gJycnICAgIGFzeW5jIGRlZiBzZXRfcGxheWxpc3RfbWV0YShzZWxmLCB0b2tlbiwgbWV0YSk6CiAgICAgICAgIiIiV1pGSVggcjI1YzE3OiBwZXItc29uZyBkaXNwbGF5IG1ldGFkYXRhIGZvciBhIHBsYXlsaXN0LiIiIgogICAgICAgIGlmIHNlbGYuX3JldHVybjoKICAgICAgICAgICAgcmV0dXJuCiAgICAgICAgYXdhaXQgc2VsZi5kYi5wbGF5bGlzdHNbX3BhcnQoKV0udXBkYXRlX29uZSgKICAgICAgICAgICAgeyJfaWQiOiB0b2tlbn0sCiAgICAgICAgICAgIHsiJHNldCI6IHsibWV0YSI6IGRpY3QobWV0YSBvciB7fSl9fSwKICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgKQoKICAgIGFzeW5jIGRlZiBnZXRfcGxheWxpc3Qoc2VsZiwgdG9rZW4pOicnJwphc3NlcnQgc3JjLmNvdW50KG9sZF9kZWYpID09IDEsICJnZXRfcGxheWxpc3QgZGVmIGFuY2hvciIKc3JjID0gc3JjLnJlcGxhY2Uob2xkX2RlZiwgbmV3X2RlZiwgMSkKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKHNyYykKcHJpbnQoInBhdGNoX3IyNWMxN19kYjogZ2V0X3BsYXlsaXN0IG1ldGEgKyBzZXRfcGxheWxpc3RfbWV0YSBhZGRlZCIpCg=="),
    ('patch_r25c17_api.py',
     'bot/core/stream_server.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMTdfYXBpLnB5IC0gY2xlYW4gbmFtZXMvY292ZXJzL2R1cmF0aW9ucyBpbiB0aGUgcGxheWxpc3QKSlNPTi4gX3BsYXlsaXN0X2JvZHkgbWVyZ2VzIHRoZSBzdG9yZWQgcGVyLXNvbmcgbWV0YWRhdGEgKHdyaXR0ZW4gYXQKcGxheWxpc3QtY3JlYXRpb24gdGltZSkgb3ZlciB0aGUgcmF3IHl0LWRscCBmaWxlIG5hbWVzLgoiIiIKaW1wb3J0IHN5cwoKcGF0aCA9IHN5cy5hcmd2WzFdCndpdGggb3BlbihwYXRoLCAiciIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBzcmMgPSBmLnJlYWQoKQoKaWYgInIyNWMxNyIgaW4gc3JjOgogICAgcHJpbnQoInBhdGNoX3IyNWMxN19hcGk6IGFscmVhZHkgYXBwbGllZCIpCiAgICBzeXMuZXhpdCgwKQoKb2xkX2l0ZW1zID0gJycnICAgICAgICBtaW1lID0gaW5mby5nZXQoIm1pbWUiKSBvciAiIgogICAgICAgIGl0ZW1zLmFwcGVuZCgKICAgICAgICAgICAgewogICAgICAgICAgICAgICAgInRva2VuIjogdG9rLAogICAgICAgICAgICAgICAgIm5hbWUiOiBpbmZvLmdldCgibmFtZSIpIG9yICJVbnRpdGxlZCIsCiAgICAgICAgICAgICAgICAic2l6ZSI6IGluZm8uZ2V0KCJzaXplIikgb3IgMCwKICAgICAgICAgICAgICAgICJtaW1lIjogbWltZSwKICAgICAgICAgICAgICAgICJwbGF5YWJsZSI6IG1pbWUuc3RhcnRzd2l0aChfUExBWUFCTEUpLAogICAgICAgICAgICB9CiAgICAgICAgKScnJwpuZXdfaXRlbXMgPSAnJycgICAgICAgIG1pbWUgPSBpbmZvLmdldCgibWltZSIpIG9yICIiCiAgICAgICAgIyBXWkZJWCByMjVjMTc6IHN0b3JlZCBtZXRhZGF0YSB3aW5zIG92ZXIgdGhlIHJhdyB5dC1kbHAKICAgICAgICAjIGZpbGUgbmFtZSAoY2xlYW4gdGl0bGUsIGFydGlzdCwgY292ZXIsIGR1cmF0aW9uKQogICAgICAgIG0gPSBtZXRhLmdldCh0b2spIG9yIHt9CiAgICAgICAgaXRlbXMuYXBwZW5kKAogICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAidG9rZW4iOiB0b2ssCiAgICAgICAgICAgICAgICAibmFtZSI6IG0uZ2V0KCJuIikgb3IgaW5mby5nZXQoIm5hbWUiKSBvciAiVW50aXRsZWQiLAogICAgICAgICAgICAgICAgImFydGlzdCI6IG0uZ2V0KCJhIikgb3IgIiIsCiAgICAgICAgICAgICAgICAiY292ZXIiOiBtLmdldCgiYyIpIG9yICIiLAogICAgICAgICAgICAgICAgImR1ciI6IG0uZ2V0KCJkIikgb3IgMCwKICAgICAgICAgICAgICAgICJzaXplIjogaW5mby5nZXQoInNpemUiKSBvciAwLAogICAgICAgICAgICAgICAgIm1pbWUiOiBtaW1lLAogICAgICAgICAgICAgICAgInBsYXlhYmxlIjogbWltZS5zdGFydHN3aXRoKF9QTEFZQUJMRSksCiAgICAgICAgICAgIH0KICAgICAgICApJycnCmFzc2VydCBzcmMuY291bnQob2xkX2l0ZW1zKSA9PSAxLCAiX3BsYXlsaXN0X2JvZHkgaXRlbXMgYW5jaG9yIgpzcmMgPSBzcmMucmVwbGFjZShvbGRfaXRlbXMsIG5ld19pdGVtcywgMSkKCm9sZF9oZWFkID0gJycnICAgIGl0ZW1zID0gW10KICAgIGZvciB0b2sgaW4gZG9jWyJpdGVtcyJdOicnJwpuZXdfaGVhZCA9ICcnJyAgICAjIFdaRklYIHIyNWMxNzogcGVyLXNvbmcgZGlzcGxheSBtZXRhZGF0YSAocG9wdWxhcml0eS1vcmRlcmVkCiAgICAjIGF0IGNyZWF0aW9uIHRpbWUsIHdpdGggY2xlYW4gbmFtZXMsIGNvdmVycyBhbmQgZHVyYXRpb25zKQogICAgbWV0YSA9IGRvYy5nZXQoIm1ldGEiKSBvciB7fQogICAgaXRlbXMgPSBbXQogICAgZm9yIHRvayBpbiBkb2NbIml0ZW1zIl06JycnCmFzc2VydCBzcmMuY291bnQob2xkX2hlYWQpID09IDEsICJfcGxheWxpc3RfYm9keSBoZWFkIGFuY2hvciIKc3JjID0gc3JjLnJlcGxhY2Uob2xkX2hlYWQsIG5ld19oZWFkLCAxKQoKd2l0aCBvcGVuKHBhdGgsICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoc3JjKQpwcmludCgicGF0Y2hfcjI1YzE3X2FwaTogcGxheWxpc3QgSlNPTiBjYXJyaWVzIGNsZWFuIG5hbWVzICsgY292ZXJzIikK"),
    ('patch_r25c18.py',
     'bot/helper/telegram_helper/tg_stream.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMTgucHkgLSB1c2VyLWFjY291bnQtT05MWSBjaGF0cyAobm8gYm90IHBoYXNlIGF0IGFsbCkuCgpUaGUgcGxheWxpc3Qgc29uZ3MgbGl2ZSBpbiB0aGUgbG9nIGNoYXQsIHdoaWNoIHRoZSBoZWxwZXIgYm90cwpjYW5ub3QgcmVhZC4gRm9yIGNoYXRzIGluIHRoZSB1c2VyLW9ubHkgc2V0ICh0aGUgTEVFQ0hfTE9HX0NIQVQsCnBsdXMgYW55dGhpbmcgaW4gV1pGSVhfVVNFUl9PTkxZX0NIQVRTKSwgb3Blbl9zdHJlYW0gYW5kIHByb2JlIGdvClNUUkFJR0hUIHRvIHRoZSB1c2VyIGFjY291bnQgdmlhIHRoZSBraXQncyB1c2VyX3N0cmVhbV9tb2R1bGUgLQp6ZXJvIGJvdCBhdHRlbXB0cywgemVybyBTdHJlYW1Hb25lIHJldHJpZXMsIGZhc3RlciBwYWdlcy4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMTgiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMTg6IGFscmVhZHkgYXBwbGllZCIpCiAgICBzeXMuZXhpdCgwKQoKc3JjICs9ICcnJwoKIyBXWkZJWCByMjVjMTg6IHVzZXItYWNjb3VudC1PTkxZIGNoYXRzIC0gbm8gYm90IHBoYXNlIGF0IGFsbApfV1pfVVNFUl9PTkxZXzI1QzE4ID0gc2V0KCkKX1daX1VPX1NFRURFRF8yNUMxOCA9IEZhbHNlCgoKZGVmIF93el9zZWVkX3VzZXJfb25seV8yNWMxOCgpOgogICAgZ2xvYmFsIF9XWl9VT19TRUVERURfMjVDMTgKICAgIGlmIF9XWl9VT19TRUVERURfMjVDMTg6CiAgICAgICAgcmV0dXJuCiAgICBfV1pfVU9fU0VFREVEXzI1QzE4ID0gVHJ1ZQogICAgaW1wb3J0IG9zIGFzIF9vczE3CgogICAgdHJ5OgogICAgICAgIGZvciBfYyBpbiAoX29zMTcuZW52aXJvbi5nZXQoIldaRklYX1VTRVJfT05MWV9DSEFUUyIpIG9yICIiKS5zcGxpdCgiLCIpOgogICAgICAgICAgICBfYyA9IF9jLnN0cmlwKCkKICAgICAgICAgICAgaWYgX2MubHN0cmlwKCItIikuaXNkaWdpdCgpOgogICAgICAgICAgICAgICAgX1daX1VTRVJfT05MWV8yNUMxOC5hZGQoaW50KF9jKSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgdHJ5OgogICAgICAgIF9sYyA9IGdldGF0dHIoQ29uZmlnLCAiTEVFQ0hfTE9HX0NIQVQiLCBOb25lKQogICAgICAgIGlmIF9sYzoKICAgICAgICAgICAgX1daX1VTRVJfT05MWV8yNUMxOC5hZGQoaW50KF9sYykpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIGlmIF9XWl9VU0VSX09OTFlfMjVDMTg6CiAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAgICAgICJyMjVjMTg6IHVzZXItYWNjb3VudC1vbmx5IGNoYXRzOiAiCiAgICAgICAgICAgICsgIiwgIi5qb2luKHN0cihjKSBmb3IgYyBpbiBzb3J0ZWQoX1daX1VTRVJfT05MWV8yNUMxOCkpCiAgICAgICAgKQoKCmFzeW5jIGRlZiBfd3pfdXNlcl9vcGVuXzI1YzE4KGNoYXRfaWQsIG1zZ19pZCwga2luZCwgdmlld2VyPU5vbmUpOgogICAgZnJvbSAuLnVzZXJfc3RyZWFtX21vZHVsZSBpbXBvcnQgb3Blbl9zdHJlYW1fdXNlcgoKICAgIHJldHVybiBhd2FpdCBvcGVuX3N0cmVhbV91c2VyKGNoYXRfaWQsIG1zZ19pZCwga2luZCwgdmlld2VyPXZpZXdlcikKCgphc3luYyBkZWYgX3d6X3VzZXJfcHJvYmVfMjVjMTgoY2hhdF9pZCwgbXNnX2lkKToKICAgIGZyb20gLi51c2VyX3N0cmVhbV9tb2R1bGUgaW1wb3J0IHByb2JlX3VzZXIKCiAgICByZXR1cm4gYXdhaXQgcHJvYmVfdXNlcihjaGF0X2lkLCBtc2dfaWQpCgoKX29yaWdfb3Blbl9zdHJlYW1fMjVjMTggPSBvcGVuX3N0cmVhbQoKCmFzeW5jIGRlZiBvcGVuX3N0cmVhbShjaGF0X2lkLCBtc2dfaWQsIGtpbmQsIHZpZXdlcj1Ob25lKToKICAgIF93el9zZWVkX3VzZXJfb25seV8yNWMxOCgpCiAgICBpZiBjaGF0X2lkIGluIF9XWl9VU0VSX09OTFlfMjVDMTg6CiAgICAgICAgcmV0dXJuIGF3YWl0IF93el91c2VyX29wZW5fMjVjMTgoY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXIpCiAgICByZXR1cm4gYXdhaXQgX29yaWdfb3Blbl9zdHJlYW1fMjVjMTgoY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXIpCgoKX29yaWdfcHJvYmVfMjVjMTggPSBwcm9iZQoKCmFzeW5jIGRlZiBwcm9iZShjaGF0X2lkLCBtc2dfaWQpOgogICAgX3d6X3NlZWRfdXNlcl9vbmx5XzI1YzE4KCkKICAgIGlmIGNoYXRfaWQgaW4gX1daX1VTRVJfT05MWV8yNUMxODoKICAgICAgICByZXR1cm4gYXdhaXQgX3d6X3VzZXJfcHJvYmVfMjVjMTgoY2hhdF9pZCwgbXNnX2lkKQogICAgcmV0dXJuIGF3YWl0IF9vcmlnX3Byb2JlXzI1YzE4KGNoYXRfaWQsIG1zZ19pZCkKJycnCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaF9yMjVjMTg6IHVzZXItYWNjb3VudC1vbmx5IGNoYXRzIGluc3RhbGxlZCIpCg=="),
    ('patch_r25c18_hdl.py',
     'bot/core/handlers.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMThfaGRsLnB5IC0gL3NldHBhc3MgY29tbWFuZCAob3duZXIvc3VkbykuCgpTZXRzIHRoZSBHTE9CQUwgU1RSRUFNX1BBU1MgbGl2ZTogdGhlIHN0cmVhbSBzZXJ2ZXIgcmVhZHMgaXQgZm9yCmV2ZXJ5IGF1dGggcmVxdWVzdCwgYW5kIHRoZSBjaGFuZ2UgcGVyc2lzdHMgdmlhIHRoZSBzYXZlZCBjb25maWcgc28KaXQgc3Vydml2ZXMgcmVib290cy4gVXNhZ2U6IC9zZXRwYXNzIDxuZXctcGFzc3dvcmQ+IChubyBzcGFjZXMpLgoiIiIKaW1wb3J0IGJhc2U2NAppbXBvcnQgb3MKaW1wb3J0IHN5cwoKTU9EVUxFID0gYmFzZTY0LmI2NGRlY29kZSgKICAgICJabkp2YlNBdUxpNWpiM0psTG1OdmJtWnBaMTl0WVc1aFoyVnlJR2x0Y0c5eWRDQkRiMjVtYVdjS1puSnZiU0F1TGk0Z2FXMXdiM0owSUV4UFIwZEZVZ29LQ21GemVXNWpJR1JsWmlCM2VtWnBlRjl6WlhSd1lYTnpLR05zYVdWdWRDd2diV1Z6YzJGblpTazZDaUFnSUNBaUlpSlhXa1pKV0NCeU1qVmpNVGc2SUhObGRDQjBhR1VnWjJ4dlltRnNJRk5VVWtWQlRWOVFRVk5USUd4cGRtVWdLeUJ3WlhKemFYTjBJR2wwTGlJaUlnb2dJQ0FnWVhKbmN5QTlJQ2h0WlhOellXZGxMblJsZUhRZ2IzSWdJaUlwTG5Od2JHbDBLQ2tLSUNBZ0lHbG1JR3hsYmloaGNtZHpLU0E4SURJZ2IzSWdZWEpuYzFzeFhTNXNiM2RsY2lncElHbHVJQ2dpYUdWc2NDSXNJQ0kvSWlrNkNpQWdJQ0FnSUNBZ1kzVnlJRDBnWjJWMFlYUjBjaWhEYjI1bWFXY3NJQ0pUVkZKRlFVMWZVRUZUVXlJc0lDSWlLU0J2Y2lBaUlnb2dJQ0FnSUNBZ0lITjBZWFJsSUQwZ0tBb2dJQ0FnSUNBZ0lDQWdJQ0FpYzJWMElDZ2lJQ3NnYzNSeUtHeGxiaWhqZFhJcEtTQXJJQ0lnWTJoaGNuTXBJZ29nSUNBZ0lDQWdJQ0FnSUNCcFppQmpkWElLSUNBZ0lDQWdJQ0FnSUNBZ1pXeHpaU0FpVGs5VUlITmxkQ0F0SUhOMGNtVmhiWE1nWVhKbElHOXdaVzRpQ2lBZ0lDQWdJQ0FnS1FvZ0lDQWdJQ0FnSUhKbGRIVnliaUJoZDJGcGRDQnRaWE56WVdkbExuSmxjR3g1WDNSbGVIUW9DaUFnSUNBZ0lDQWdJQ0FnSUNJOFlqNVRkSEpsWVcwZ2NHRnpjM2R2Y21ROEwySStYRzVjYmlJS0lDQWdJQ0FnSUNBZ0lDQWdJa04xY25KbGJuUTZJQ0lnS3lCemRHRjBaU0FySUNKY2JseHVJZ29nSUNBZ0lDQWdJQ0FnSUNBaVBHTnZaR1UrTDNObGRIQmhjM01nUEc1bGR5MXdZWE56ZDI5eVpENDhMMk52WkdVK0lDMGdjMlYwSUdsMFhHNGlDaUFnSUNBZ0lDQWdJQ0FnSUNJb2JtOGdjM0JoWTJWek95QmhjSEJzYVdWeklIUnZJRzVsZHlCemFXZHVMV2x1Y3lCcGJXMWxaR2xoZEdWc2VTQmhibVJjYmlJS0lDQWdJQ0FnSUNBZ0lDQWdJbk4xY25acGRtVnpJSEpsWW05dmRITXBJZ29nSUNBZ0lDQWdJQ2tLSUNBZ0lIQjNJRDBnWVhKbmMxc3hYUzV6ZEhKcGNDZ3BDaUFnSUNCcFppQnNaVzRvY0hjcElEd2dNem9LSUNBZ0lDQWdJQ0J5WlhSMWNtNGdZWGRoYVhRZ2JXVnpjMkZuWlM1eVpYQnNlVjkwWlhoMEtBb2dJQ0FnSUNBZ0lDQWdJQ0FpVUd4bFlYTmxJSFZ6WlNCaGRDQnNaV0Z6ZENBeklHTm9ZWEpoWTNSbGNuTXVJZ29nSUNBZ0lDQWdJQ2tLSUNBZ0lIUnllVG9LSUNBZ0lDQWdJQ0JEYjI1bWFXY3VVMVJTUlVGTlgxQkJVMU1nUFNCd2R3b2dJQ0FnWlhoalpYQjBJRVY0WTJWd2RHbHZiaUJoY3lCbE9nb2dJQ0FnSUNBZ0lISmxkSFZ5YmlCaGQyRnBkQ0J0WlhOellXZGxMbkpsY0d4NVgzUmxlSFFvWmlKY2RUSTNOR01nUTI5dVptbG5JR1Z5Y205eU9pQjdaWDBpS1FvZ0lDQWdjMkYyWldRZ1BTQkdZV3h6WlFvZ0lDQWdkSEo1T2dvZ0lDQWdJQ0FnSUdaeWIyMGdMaTR1YUdWc2NHVnlMbVY0ZEY5MWRHbHNjeTVrWWw5b1lXNWtiR1Z5SUdsdGNHOXlkQ0JrWVhSaFltRnpaUW9LSUNBZ0lDQWdJQ0JoZDJGcGRDQmtZWFJoWW1GelpTNTFjR1JoZEdWZlkyOXVabWxuS0hzaVUxUlNSVUZOWDFCQlUxTWlPaUJ3ZDMwcENpQWdJQ0FnSUNBZ2MyRjJaV1FnUFNCVWNuVmxDaUFnSUNCbGVHTmxjSFFnUlhoalpYQjBhVzl1SUdGeklHVTZDaUFnSUNBZ0lDQWdURTlIUjBWU0xuZGhjbTVwYm1jb1ppSnlNalZqTVRnZ2MyVjBjR0Z6Y3pvZ2MyRjJaU0JtWVdsc1pXUTZJSHRsZlNJcENpQWdJQ0IwZUhRZ1BTQW9DaUFnSUNBZ0lDQWdJbHgxTWpjd05TQlRWRkpGUVUxZlVFRlRVeUIxY0dSaGRHVmtMaUJPWlhjZ2NHRnpjM2R2Y21RZ2MybG5iaTFwYm5NZ2RYTmxJR2wwSUNJS0lDQWdJQ0FnSUNBaWFXMXRaV1JwWVhSbGJIazdJR1JsZG1salpYTWdZV3h5WldGa2VTQnphV2R1WldRZ2FXNGdhMlZsY0NCM2IzSnJhVzVuSUhWdWRHbHNJQ0lLSUNBZ0lDQWdJQ0FpZEdobGFYSWdkRzlyWlc0Z1pYaHdhWEpsY3lBb01qUWdhQ2t1SWdvZ0lDQWdLUW9nSUNBZ2FXWWdibTkwSUhOaGRtVmtPZ29nSUNBZ0lDQWdJSFI0ZENBclBTQWlYRzVjZFRJMllUQmNkV1psTUdZZ1EyOTFiR1FnYm05MElIQmxjbk5wYzNRZ2FYUWdMU0JwZENCdFlYa2djbVZ6WlhRZ2IyNGdkR2hsSUc1bGVIUWdjbVZpYjI5MExpSUtJQ0FnSUhKbGRIVnliaUJoZDJGcGRDQnRaWE56WVdkbExuSmxjR3g1WDNSbGVIUW9kSGgwS1FvPSIKKS5kZWNvZGUoInV0Zi04IikKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJ3emZpeF9zZXRwYXNzIiBpbiBzcmM6CiAgICBwcmludCgicGF0Y2hfcjI1YzE4X2hkbDogYWxyZWFkeSBhcHBsaWVkIikKICAgIHN5cy5leGl0KDApCgojIGhhbmRsZXJzLnB5IHNpdHMgYXQgPHJlcG8+L2JvdC9jb3JlL2hhbmRsZXJzLnB5IC0+IDxyZXBvPi9ib3QvaGVscGVyL3d6Zml4Cnd6Zml4X2RpciA9IG9zLnBhdGguam9pbigKICAgIG9zLnBhdGguZGlybmFtZShvcy5wYXRoLmRpcm5hbWUob3MucGF0aC5hYnNwYXRoKHBhdGgpKSksCiAgICAiaGVscGVyIiwKICAgICJ3emZpeCIsCikKb3MubWFrZWRpcnMod3pmaXhfZGlyLCBleGlzdF9vaz1UcnVlKQp3aXRoIG9wZW4ob3MucGF0aC5qb2luKHd6Zml4X2RpciwgInIyNWMxOF9zZXRwYXNzLnB5IiksICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoTU9EVUxFKQoKc3JjICs9ICgKICAgICJcbiAgICAjIFdaRklYIHIyNWMxOCBzZXRwYXNzICh2MTUuODMuMTgpXG4iCiAgICAiICAgIGZyb20gLi5oZWxwZXIud3pmaXgucjI1YzE4X3NldHBhc3MgaW1wb3J0IHd6Zml4X3NldHBhc3NcbiIKICAgICIgICAgVGdDbGllbnQuYm90LmFkZF9oYW5kbGVyKFxuIgogICAgIiAgICAgICAgTWVzc2FnZUhhbmRsZXIoXG4iCiAgICAiICAgICAgICAgICAgd3pmaXhfc2V0cGFzcyxcbiIKICAgICIgICAgICAgICAgICBmaWx0ZXJzPWNvbW1hbmQoXCJzZXRwYXNzXCIsIGNhc2Vfc2Vuc2l0aXZlPVRydWUpXG4iCiAgICAiICAgICAgICAgICAgJiBDdXN0b21GaWx0ZXJzLnN1ZG8sXG4iCiAgICAiICAgICAgICApXG4iCiAgICAiICAgIClcbiIKKQoKd2l0aCBvcGVuKHBhdGgsICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoc3JjKQpwcmludCgicGF0Y2hfcjI1YzE4X2hkbDogL3NldHBhc3MgcmVnaXN0ZXJlZCAobW9kdWxlICsgaGFuZGxlcikiKQo="),
    ('patch_r25c19_api2.py',
     'bot/core/stream_server.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMTlfYXBpMi5weSAtIC9fcGxheWxpc3RzIEpTT04gaW5kZXggb24gdGhlIHN0cmVhbSBzZXJ2ZXIuCgpSZWFkLW9ubHkgbGlzdGluZyBvZiBldmVyeSBzdG9yZWQgcGxheWxpc3QgKHRva2VuLCBuYW1lLCBzb25nCmNvdW50LCB1cCB0byA0IGNvdmVyIHRodW1ibmFpbHMpLiBHYXRlZCBieSB0aGUgZ2xvYmFsIFNUUkVBTV9QQVNTCihubyBnYXRlIHdoZW4gbm9uZSBpcyBzZXQpLiBQb3dlcnMgdGhlIC9wbGF5bGlzdHMgbGlicmFyeSBwYWdlIGFuZAp0aGUgaG9tZXBhZ2UncyByZWNlbnQtcGxheWxpc3RzIHN0cmlwLgoiIiIKaW1wb3J0IHN5cwoKcGF0aCA9IHN5cy5hcmd2WzFdCndpdGggb3BlbihwYXRoLCAiciIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBzcmMgPSBmLnJlYWQoKQoKaWYgInIyNWMxOSIgaW4gc3JjOgogICAgcHJpbnQoInBhdGNoX3IyNWMxOV9hcGkyOiBhbHJlYWR5IGFwcGxpZWQiKQogICAgc3lzLmV4aXQoMCkKCiMgMSkgdGhlIGhhbmRsZXIsIGFwcGVuZGVkIGF0IG1vZHVsZSBsZXZlbApzcmMgKz0gJycnCgphc3luYyBkZWYgX3BsYXlsaXN0cyhyZXF1ZXN0KToKICAgICIiIldaRklYIHIyNWMxOTogcGxheWxpc3QgbGlicmFyeSBpbmRleCAoSlNPTiwgcmVhZC1vbmx5KS4iIiIKICAgIGlmIG5vdCBfdXNfY2hlY2tfYXV0aChyZXF1ZXN0KToKICAgICAgICByYWlzZSB3ZWIuSFRUUFVuYXV0aG9yaXplZCgKICAgICAgICAgICAgdGV4dD0iYXV0aGVudGljYXRlIGZpcnN0IiwKICAgICAgICAgICAgaGVhZGVycz17IlgtU3RyZWFtLUF1dGgtUmVxdWlyZWQiOiAiMSJ9LAogICAgICAgICkKICAgIHRyeToKICAgICAgICBmcm9tIC4uZXh0X3V0aWxzLmRiX2hhbmRsZXIgaW1wb3J0IF9wYXJ0IGFzIF93el9wYXJ0MjUKCiAgICAgICAgY29sbCA9IGRhdGFiYXNlLmRiLnBsYXlsaXN0c1tfd3pfcGFydDI1KCldCiAgICAgICAgb3V0ID0gW10KICAgICAgICBhc3luYyBmb3IgZCBpbiBjb2xsLmZpbmQoe30sIHsibmFtZSI6IDEsICJpdGVtcyI6IDEsICJtZXRhIjogMX0pOgogICAgICAgICAgICBtZXRhID0gZC5nZXQoIm1ldGEiKSBvciB7fQogICAgICAgICAgICBjb3ZlcnMgPSBbXQogICAgICAgICAgICBmb3IgdCBpbiAoZC5nZXQoIml0ZW1zIikgb3IgW10pOgogICAgICAgICAgICAgICAgYyA9IChtZXRhLmdldCh0KSBvciB7fSkuZ2V0KCJjIikKICAgICAgICAgICAgICAgIGlmIGMgYW5kIGMgbm90IGluIGNvdmVyczoKICAgICAgICAgICAgICAgICAgICBjb3ZlcnMuYXBwZW5kKGMpCiAgICAgICAgICAgICAgICBpZiBsZW4oY292ZXJzKSA+PSA0OgogICAgICAgICAgICAgICAgICAgIGJyZWFrCiAgICAgICAgICAgIG91dC5hcHBlbmQoCiAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgInRva2VuIjogZFsiX2lkIl0sCiAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiBkLmdldCgibmFtZSIpIG9yICJQbGF5bGlzdCIsCiAgICAgICAgICAgICAgICAgICAgImNvdW50IjogbGVuKGQuZ2V0KCJpdGVtcyIpIG9yIFtdKSwKICAgICAgICAgICAgICAgICAgICAiY292ZXJzIjogY292ZXJzLAogICAgICAgICAgICAgICAgfQogICAgICAgICAgICApCiAgICAgICAgb3V0LnNvcnQoa2V5PWxhbWJkYSB4OiB4WyJuYW1lIl0ubG93ZXIoKSkKICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJwbGF5bGlzdHMiOiBvdXR9KQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmInIyNWMxOTogcGxheWxpc3RzIGluZGV4IGZhaWxlZDoge2V9IikKICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJwbGF5bGlzdHMiOiBbXX0pCicnJwoKIyAyKSByZWdpc3RlciB0aGUgcm91dGUgbmV4dCB0byAvX3BpbmcKb2xkX3J0ID0gJyAgICBhcHAucm91dGVyLmFkZF9yb3V0ZSgiR0VUIiwgIi9fcGluZyIsIF9waW5nKScKbmV3X3J0ID0gKCcgICAgYXBwLnJvdXRlci5hZGRfcm91dGUoIkdFVCIsICIvX3BpbmciLCBfcGluZylcbicKICAgICAgICAgICcgICAgIyBXWkZJWCByMjVjMTk6IHBsYXlsaXN0IGxpYnJhcnkgaW5kZXhcbicKICAgICAgICAgICcgICAgYXBwLnJvdXRlci5hZGRfcm91dGUoIkdFVCIsICIvX3BsYXlsaXN0cyIsIF9wbGF5bGlzdHMpJykKYXNzZXJ0IHNyYy5jb3VudChvbGRfcnQpID09IDEsICJwaW5nIHJvdXRlIGFuY2hvciIKc3JjID0gc3JjLnJlcGxhY2Uob2xkX3J0LCBuZXdfcnQsIDEpCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaF9yMjVjMTlfYXBpMjogL19wbGF5bGlzdHMgaW5kZXggcm91dGUgYWRkZWQiKQo="),
    ('patch_r25c19_ws.py',
     'web/wserver.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMTlfd3MucHkgLSAvcGxheWxpc3RzIGxpYnJhcnkgcGFnZSArIC9hcGkvcGxheWxpc3RzIHByb3h5Cm9uIHRoZSBGYXN0QVBJIHdlYiBzZXJ2ZXIuIEFsc28gd3JpdGVzIHRoZSBwbGF5bGlzdHMuaHRtbCB0ZW1wbGF0ZQooYXMgYSBzaWJsaW5nIG9mIHRoZSBleGlzdGluZyB0ZW1wbGF0ZXMpLgoiIiIKaW1wb3J0IGJhc2U2NAppbXBvcnQgb3MKaW1wb3J0IHN5cwoKUEFHRSA9IGJhc2U2NC5iNjRkZWNvZGUoCiAgICAiUENGRVQwTlVXVkJGSUdoMGJXdytDanhvZEcxc0lHeGhibWM5SW1WdUlqNEtQR2hsWVdRK0NqeHRaWFJoSUdOb1lYSnpaWFE5SWxWVVJpMDRJajRLUEcxbGRHRWdibUZ0WlQwaWRtbGxkM0J2Y25RaUlHTnZiblJsYm5ROUluZHBaSFJvUFdSbGRtbGpaUzEzYVdSMGFDd2dhVzVwZEdsaGJDMXpZMkZzWlQweExqQWlQZ284YldWMFlTQnVZVzFsUFNKeVpXWmxjbkpsY2lJZ1kyOXVkR1Z1ZEQwaWJtOHRjbVZtWlhKeVpYSWlQZ284YldWMFlTQnVZVzFsUFNKMGFHVnRaUzFqYjJ4dmNpSWdZMjl1ZEdWdWREMGlJekF3TURBd015SStDangwYVhSc1pUNVFiR0Y1YkdsemRITWd3cmNnVjFwTlRDMVlQQzkwYVhSc1pUNEtQSE4wZVd4bFBnbzZjbTl2ZEh0amIyeHZjaTF6WTJobGJXVTZaR0Z5YXpzdExXSm5PaU13TURBd01ETTdMUzF6T2lNd05EQTBNRUk3TFMxek1qb2pNRGN3TnpCR095MHRiR2x1WlRweVoySmhLRFF5TERrMUxESXdPQ3d1TWpncE95MHRkSGc2STBZMVJqZEdSanN0TFcxMU9pTkRNME5DUkVZN0xTMWhZem9qTTBRNE4wWkdmUW9xZTJKdmVDMXphWHBwYm1jNlltOXlaR1Z5TFdKdmVEdHRZWEpuYVc0Nk1EdHdZV1JrYVc1bk9qQjlDbUp2WkhsN1ltRmphMmR5YjNWdVpEcDJZWElvTFMxaVp5azdZMjlzYjNJNmRtRnlLQzB0ZEhncE8yWnZiblF0Wm1GdGFXeDVPaWROYjI1MGMyVnljbUYwSnl4emVYTjBaVzB0ZFdrc2MyRnVjeTF6WlhKcFpqdHRhVzR0YUdWcFoyaDBPakV3TUdSMmFEc0tZbUZqYTJkeWIzVnVaQzFwYldGblpUcHlZV1JwWVd3dFozSmhaR2xsYm5Rb01URXdNSEI0SURVd01IQjRJR0YwSURnMUpTQXRNVEFsTEhKblltRW9OakVzTVRNMUxESTFOU3d1TVRZcExIUnlZVzV6Y0dGeVpXNTBJRFl3SlNsOUNpNTNjbUZ3ZTIxaGVDMTNhV1IwYURvNU9EQndlRHR0WVhKbmFXNDZNQ0JoZFhSdk8zQmhaR1JwYm1jNlkyeGhiWEFvTVhKbGJTd3pkbmNzTW5KbGJTbDlDbWd4ZTJadmJuUXRjMmw2WlRwamJHRnRjQ2d4TGpaeVpXMHNOWFozTERJdU5uSmxiU2s3YldGeVoybHVPaTR6Y21WdElEQWdMalJ5WlcxOUNpNXJhVzVrZTJadmJuUXRjMmw2WlRvdU56SnlaVzA3YkdWMGRHVnlMWE53WVdOcGJtYzZMakl5WlcwN2RHVjRkQzEwY21GdWMyWnZjbTA2ZFhCd1pYSmpZWE5sTzJOdmJHOXlPblpoY2lndExXRmpLVHRtYjI1MExYZGxhV2RvZERvM01EQjlDaTV6ZFdKN1kyOXNiM0k2ZG1GeUtDMHRiWFVwTzJadmJuUXRjMmw2WlRvdU9USnlaVzA3YldGeVoybHVMV0p2ZEhSdmJUb3hMalp5WlcxOUNpNW5jbWxrZTJScGMzQnNZWGs2WjNKcFpEdG5jbWxrTFhSbGJYQnNZWFJsTFdOdmJIVnRibk02Y21Wd1pXRjBLR0YxZEc4dFptbHNiQ3h0YVc1dFlYZ29NakV3Y0hnc01XWnlLU2s3WjJGd09qRnlaVzE5Q21FdVkyRnlaSHRrYVhOd2JHRjVPbUpzYjJOck8ySmhZMnRuY205MWJtUTZkbUZ5S0MwdGN5azdZbTl5WkdWeU9qRndlQ0J6YjJ4cFpDQjJZWElvTFMxc2FXNWxLVHRpYjNKa1pYSXRjbUZrYVhWek9qRTBjSGc3YjNabGNtWnNiM2M2YUdsa1pHVnVPd3AwWlhoMExXUmxZMjl5WVhScGIyNDZibTl1WlR0amIyeHZjanBwYm1obGNtbDBPM1J5WVc1emFYUnBiMjQ2ZEhKaGJuTm1iM0p0SUM0eE1uTWdaV0Z6WlN4aWIzSmtaWEl0WTI5c2IzSWdMakV5Y3lCbFlYTmxmUXBoTG1OaGNtUTZhRzkyWlhKN2RISmhibk5tYjNKdE9uUnlZVzV6YkdGMFpWa29MVE53ZUNrN1ltOXlaR1Z5TFdOdmJHOXlPblpoY2lndExXRmpLWDBLTG1OdmRtVnljM3RrYVhOd2JHRjVPbWR5YVdRN1ozSnBaQzEwWlcxd2JHRjBaUzFqYjJ4MWJXNXpPakZtY2lBeFpuSTdZWE53WldOMExYSmhkR2x2T2pFN1ltRmphMmR5YjNWdVpEcHNhVzVsWVhJdFozSmhaR2xsYm5Rb01UTTFaR1ZuTENNd1lURmhOR1FzSXpBMk1UVXpSaWw5Q2k1amIzWmxjbk1nYVcxbmUzZHBaSFJvT2pFd01DVTdhR1ZwWjJoME9qRXdNQ1U3YjJKcVpXTjBMV1pwZERwamIzWmxjbjBLTG1OdmRtVnljeTV6YjJ4dmUyZHlhV1F0ZEdWdGNHeGhkR1V0WTI5c2RXMXVjem94Wm5KOUNpNWpiM1psY25NdWNHaDdaR2x6Y0d4aGVUcG5jbWxrTzNCc1lXTmxMV2wwWlcxek9tTmxiblJsY2p0amIyeHZjanAyWVhJb0xTMWhZeWs3Wm05dWRDMXphWHBsT2pJdU5ISmxiWDBLTG1sdVptOTdjR0ZrWkdsdVp6b3VPSEpsYlNBdU9UVnlaVzBnTGprMWNtVnRmUW91Ym0xN1ptOXVkQzEzWldsbmFIUTZOekF3TzNkb2FYUmxMWE53WVdObE9tNXZkM0poY0R0dmRtVnlabXh2ZHpwb2FXUmtaVzQ3ZEdWNGRDMXZkbVZ5Wm14dmR6cGxiR3hwY0hOcGMzMEtMbU4wZTJOdmJHOXlPblpoY2lndExXMTFLVHRtYjI1MExYTnBlbVU2TGpneWNtVnRPMjFoY21kcGJpMTBiM0E2TGpFMWNtVnRmUW91Ym05MFpYdGliM0prWlhJNk1YQjRJSE52Ykdsa0lIWmhjaWd0TFd4cGJtVXBPMkpoWTJ0bmNtOTFibVE2ZG1GeUtDMHRjeWs3WW05eVpHVnlMWEpoWkdsMWN6b3hNbkI0TzNCaFpHUnBibWM2TW5KbGJUdDBaWGgwTFdGc2FXZHVPbU5sYm5SbGNqdGpiMnh2Y2pwMllYSW9MUzF0ZFNsOUNpNXphMlZzZTJobGFXZG9kRG95TmpCd2VEdGliM0prWlhJdGNtRmthWFZ6T2pFMGNIZzdZbUZqYTJkeWIzVnVaRHBzYVc1bFlYSXRaM0poWkdsbGJuUW9PVEJrWldjc2RtRnlLQzB0Y3lrZ01qVWxMSFpoY2lndExYTXlLU0ExTUNVc2RtRnlLQzB0Y3lrZ056VWxLVHNLWW1GamEyZHliM1Z1WkMxemFYcGxPakl3TUNVZ01UQXdKVHRoYm1sdFlYUnBiMjQ2YzJnZ01TNDBjeUJwYm1acGJtbDBaU0JzYVc1bFlYSjlDa0JyWlhsbWNtRnRaWE1nYzJoN2RHOTdZbUZqYTJkeWIzVnVaQzF3YjNOcGRHbHZiam90TWpBd0pTQXdmWDBLTG1kaGRHVjdjRzl6YVhScGIyNDZabWw0WldRN2FXNXpaWFE2TUR0a2FYTndiR0Y1T21keWFXUTdjR3hoWTJVdGFYUmxiWE02WTJWdWRHVnlPMkpoWTJ0bmNtOTFibVE2Y21kaVlTZ3dMREFzTXl3dU9DazdlaTFwYm1SbGVEb3hNSDBLTG1kaGRHVWdMbUp2ZUh0aVlXTnJaM0p2ZFc1a09uWmhjaWd0TFhNeUtUdGliM0prWlhJNk1YQjRJSE52Ykdsa0lIWmhjaWd0TFdGaktUdGliM0prWlhJdGNtRmthWFZ6T2pFMGNIZzdjR0ZrWkdsdVp6b3hMalp5WlcwN2JXRjRMWGRwWkhSb09qTTBNSEI0TzNkcFpIUm9Pamt5SlgwS0xtZGhkR1VnYURON2JXRnlaMmx1TFdKdmRIUnZiVG91T0hKbGJYMEtMbWRoZEdVZ2FXNXdkWFI3ZDJsa2RHZzZNVEF3SlR0d1lXUmthVzVuT2k0M2NtVnRJQzQ1Y21WdE8ySnZjbVJsY2kxeVlXUnBkWE02TVRCd2VEdGliM0prWlhJNk1YQjRJSE52Ykdsa0lIWmhjaWd0TFd4cGJtVXBPd3BpWVdOclozSnZkVzVrT25aaGNpZ3RMV0puS1R0amIyeHZjanAyWVhJb0xTMTBlQ2s3Wm05dWREbzFNREFnTGprMWNtVnRJQ2ROYjI1MGMyVnljbUYwSnl4ellXNXpMWE5sY21sbU8yOTFkR3hwYm1VNmJtOXVaVHR0WVhKbmFXNHRZbTkwZEc5dE9pNDRjbVZ0ZlFvdVoyRjBaU0JpZFhSMGIyNTdkMmxrZEdnNk1UQXdKVHR3WVdSa2FXNW5PaTQzY21WdE8ySnZjbVJsY2pvd08ySnZjbVJsY2kxeVlXUnBkWE02TVRCd2VEdGlZV05yWjNKdmRXNWtPblpoY2lndExXRmpLVHRqYjJ4dmNqb2pabVptT3dwbWIyNTBPamN3TUNBdU9UVnlaVzBnSjAxdmJuUnpaWEp5WVhRbkxITmhibk10YzJWeWFXWTdZM1Z5YzI5eU9uQnZhVzUwWlhKOUNqd3ZjM1I1YkdVK0Nqd3ZhR1ZoWkQ0S1BHSnZaSGsrQ2p4dFlXbHVJR05zWVhOelBTSjNjbUZ3SWo0S1BHUnBkaUJqYkdGemN6MGlhMmx1WkNJK1RHbGljbUZ5ZVR3dlpHbDJQZ284YURFK1dXOTFjaUJ3YkdGNWJHbHpkSE04TDJneFBnbzhaR2wySUdOc1lYTnpQU0p6ZFdJaUlHbGtQU0p6ZFdJaVBreHZZV1JwYm1maWdLWThMMlJwZGo0S1BHUnBkaUJqYkdGemN6MGlaM0pwWkNJZ2FXUTlJbWR5YVdRaVBqd3ZaR2wyUGdvOEwyMWhhVzQrQ2p4elkzSnBjSFErQ2lobWRXNWpkR2x2YmlncGV3b2lkWE5sSUhOMGNtbGpkQ0k3Q25aaGNpQmhkWFJvVkc5clBTSWlPM1J5ZVh0aGRYUm9WRzlyUFhObGMzTnBiMjVUZEc5eVlXZGxMbWRsZEVsMFpXMG9JbmQ2VUd4QmRYUm9JaWw4ZkNJaU8zMWpZWFJqYUNobEtYdDlDbVoxYm1OMGFXOXVJR1ZzS0drcGUzSmxkSFZ5YmlCa2IyTjFiV1Z1ZEM1blpYUkZiR1Z0Wlc1MFFubEpaQ2hwS1R0OUNtWjFibU4wYVc5dUlHZHlhV1FvS1h0eVpYUjFjbTRnWld3b0ltZHlhV1FpS1R0OUNtWjFibU4wYVc5dUlHRndhU2dwZTNKbGRIVnliaUFpTDJGd2FTOXdiR0Y1YkdsemRITWlLeWhoZFhSb1ZHOXJQeUkvWVhWMGFEMGlLMlZ1WTI5a1pWVlNTVU52YlhCdmJtVnVkQ2hoZFhSb1ZHOXJLVG9pSWlrN2ZRcG1kVzVqZEdsdmJpQnphMlZzWlhSdmJpZ3BlMlp2Y2loMllYSWdhVDB3TzJrOE5qdHBLeXNwZTNaaGNpQmtQV1J2WTNWdFpXNTBMbU55WldGMFpVVnNaVzFsYm5Rb0ltUnBkaUlwTzJRdVkyeGhjM05PWVcxbFBTSnphMlZzSWp0bmNtbGtLQ2t1WVhCd1pXNWtRMmhwYkdRb1pDazdmWDBLWm5WdVkzUnBiMjRnWTJGeVpDaHdLWHNLZG1GeUlHRTlaRzlqZFcxbGJuUXVZM0psWVhSbFJXeGxiV1Z1ZENnaVlTSXBPMkV1WTJ4aGMzTk9ZVzFsUFNKallYSmtJanRoTG1oeVpXWTlJaTl3YkdGNWJHbHpkQzhpSzJWdVkyOWtaVlZTU1VOdmJYQnZibVZ1ZENod0xuUnZhMlZ1S1RzS2RtRnlJR052ZGoxa2IyTjFiV1Z1ZEM1amNtVmhkR1ZGYkdWdFpXNTBLQ0prYVhZaUtUc0tkbUZ5SUdOelBYQXVZMjkyWlhKemZIeGJYVHNLYVdZb1kzTXViR1Z1WjNSb1BUMDlNU2w3WTI5MkxtTnNZWE56VG1GdFpUMGlZMjkyWlhKeklITnZiRzhpTzMwS1pXeHpaU0JwWmloamN5NXNaVzVuZEdncGUyTnZkaTVqYkdGemMwNWhiV1U5SW1OdmRtVnljeUk3ZlFwbGJITmxlMk52ZGk1amJHRnpjMDVoYldVOUltTnZkbVZ5Y3lCd2FDSTdZMjkyTG5SbGVIUkRiMjUwWlc1MFBTTGltYXNpTzMwS1ptOXlLSFpoY2lCcFBUQTdhVHhOWVhSb0xtMXBiaWhqY3k1c1pXNW5kR2dzTkNrN2FTc3JLWHNLZG1GeUlHbHRQV1J2WTNWdFpXNTBMbU55WldGMFpVVnNaVzFsYm5Rb0ltbHRaeUlwTzJsdExuTnlZejFqYzF0cFhUdHBiUzVoYkhROUlpSTdhVzB1Ykc5aFpHbHVaejBpYkdGNmVTSTdDbWx0TG05dVpYSnliM0k5S0daMWJtTjBhVzl1S0hncGUzSmxkSFZ5YmlCbWRXNWpkR2x2YmlncGUzZ3VjM1I1YkdVdVpHbHpjR3hoZVQwaWJtOXVaU0k3ZlR0OUtTaHBiU2s3Q21OdmRpNWhjSEJsYm1SRGFHbHNaQ2hwYlNrN2ZRcGhMbUZ3Y0dWdVpFTm9hV3hrS0dOdmRpazdDblpoY2lCcGJtWnZQV1J2WTNWdFpXNTBMbU55WldGMFpVVnNaVzFsYm5Rb0ltUnBkaUlwTzJsdVptOHVZMnhoYzNOT1lXMWxQU0pwYm1adklqc0tkbUZ5SUc0OVpHOWpkVzFsYm5RdVkzSmxZWFJsUld4bGJXVnVkQ2dpWkdsMklpazdiaTVqYkdGemMwNWhiV1U5SW01dElqdHVMblJsZUhSRGIyNTBaVzUwUFhBdWJtRnRaWHg4SWxCc1lYbHNhWE4wSWpzS2RtRnlJR005Wkc5amRXMWxiblF1WTNKbFlYUmxSV3hsYldWdWRDZ2laR2wySWlrN1l5NWpiR0Z6YzA1aGJXVTlJbU4wSWp0akxuUmxlSFJEYjI1MFpXNTBQWEF1WTI5MWJuUXJLSEF1WTI5MWJuUTlQVDB4UHlJZ2MyOXVaeUk2SWlCemIyNW5jeUlwT3dwcGJtWnZMbUZ3Y0dWdVpFTm9hV3hrS0c0cE8ybHVabTh1WVhCd1pXNWtRMmhwYkdRb1l5azdZUzVoY0hCbGJtUkRhR2xzWkNocGJtWnZLVHNLY21WMGRYSnVJR0U3ZlFwbWRXNWpkR2x2YmlCeVpXNWtaWElvWkNsN0NtZHlhV1FvS1M1cGJtNWxja2hVVFV3OUlpSTdDblpoY2lCd2N6MG9aQ1ltWkM1d2JHRjViR2x6ZEhNcGZIeGJYVHNLWld3b0luTjFZaUlwTG5SbGVIUkRiMjUwWlc1MFBYQnpMbXhsYm1kMGFEOXdjeTVzWlc1bmRHZ3JLSEJ6TG14bGJtZDBhRDA5UFRFL0lpQndiR0Y1YkdsemRDSTZJaUJ3YkdGNWJHbHpkSE1pS1RvaUlqc0thV1lvSVhCekxteGxibWQwYUNsN0NuWmhjaUJ1ZEQxa2IyTjFiV1Z1ZEM1amNtVmhkR1ZGYkdWdFpXNTBLQ0prYVhZaUtUdHVkQzVqYkdGemMwNWhiV1U5SW01dmRHVWlPd3B1ZEM1MFpYaDBRMjl1ZEdWdWREMGlUbThnY0d4aGVXeHBjM1J6SUhsbGRDRGlnSlFnYzJWdVpDQmhiaUJoY25ScGMzUWdiR2x1YXlCMGJ5QjBhR1VnWW05MElHRnVaQ0IwWVhBZ1EzSmxZWFJsSUhCc1lYbHNhWE4wTGlJN0NtZHlhV1FvS1M1eVpYQnNZV05sVjJsMGFDaHVkQ2s3Y21WMGRYSnVPMzBLZG1GeUlHWTlaRzlqZFcxbGJuUXVZM0psWVhSbFJHOWpkVzFsYm5SR2NtRm5iV1Z1ZENncE93cG1iM0lvZG1GeUlHazlNRHRwUEhCekxteGxibWQwYUR0cEt5c3BaaTVoY0hCbGJtUkRhR2xzWkNoallYSmtLSEJ6VzJsZEtTazdDbWR5YVdRb0tTNWhjSEJsYm1SRGFHbHNaQ2htS1R0OUNtWjFibU4wYVc5dUlHZGhkR1VvS1hzS2RtRnlJR2M5Wkc5amRXMWxiblF1WTNKbFlYUmxSV3hsYldWdWRDZ2laR2wySWlrN1p5NWpiR0Z6YzA1aGJXVTlJbWRoZEdVaU93cG5MbWx1Ym1WeVNGUk5URDBuUEdScGRpQmpiR0Z6Y3owaVltOTRJajQ4YURNK1RHbGljbUZ5ZVNCcGN5QndZWE56ZDI5eVpDMXdjbTkwWldOMFpXUThMMmd6UGljckNpYzhhVzV3ZFhRZ2FXUTlJbWR3ZHlJZ2RIbHdaVDBpY0dGemMzZHZjbVFpSUhCc1lXTmxhRzlzWkdWeVBTSlRkSEpsWVcwZ2NHRnpjM2R2Y21RaUlHRjFkRzltYjJOMWN6NG5Ld29uUEdKMWRIUnZiaUJwWkQwaVoyOXJJajVWYm14dlkyczhMMkoxZEhSdmJqNDhMMlJwZGo0bk93cGtiMk4xYldWdWRDNWliMlI1TG1Gd2NHVnVaRU5vYVd4a0tHY3BPd3BuTG5GMVpYSjVVMlZzWldOMGIzSW9JaU5uYjJzaUtTNXZibU5zYVdOclBXWjFibU4wYVc5dUtDbDdDblpoY2lCd2R6MW5MbkYxWlhKNVUyVnNaV04wYjNJb0lpTm5jSGNpS1M1MllXeDFaVHNLWm1WMFkyZ29JaTloY0drdmMzUnlaV0Z0WDJGMWRHZ2lMSHR0WlhSb2IyUTZJbEJQVTFRaUxHaGxZV1JsY25NNmV5SkRiMjUwWlc1MExWUjVjR1VpT2lKaGNIQnNhV05oZEdsdmJpOXFjMjl1SW4wc0NtSnZaSGs2U2xOUFRpNXpkSEpwYm1kcFpua29lM0JoYzNOM2IzSmtPbkIzZlNsOUtTNTBhR1Z1S0daMWJtTjBhVzl1S0hJcGUzSmxkSFZ5YmlCeUxtcHpiMjRvS1R0OUtTNTBhR1Z1S0daMWJtTjBhVzl1S0dRcGV3cHBaaWhrSmlaa0xuUnZhMlZ1S1h0aGRYUm9WRzlyUFdRdWRHOXJaVzQ3ZEhKNWUzTmxjM05wYjI1VGRHOXlZV2RsTG5ObGRFbDBaVzBvSW5kNlVHeEJkWFJvSWl4aGRYUm9WRzlyS1R0OVkyRjBZMmdvWlNsN2ZRcG5MbkpsYlc5MlpTZ3BPMnh2WVdRb0tUdDlDbVZzYzJWN1p5NXhkV1Z5ZVZObGJHVmpkRzl5S0NJalozQjNJaWt1ZG1Gc2RXVTlJaUk3Wnk1eGRXVnllVk5sYkdWamRHOXlLQ0lqWjNCM0lpa3VjR3hoWTJWb2IyeGtaWEk5SWxkeWIyNW5JSEJoYzNOM2IzSmtJanQ5ZlNrS0xtTmhkR05vS0daMWJtTjBhVzl1S0NsN2ZTazdmVHQ5Q21aMWJtTjBhVzl1SUd4dllXUW9LWHNLWm1WMFkyZ29ZWEJwS0Nrc2UyTmhZMmhsT2lKdWJ5MXpkRzl5WlNKOUtTNTBhR1Z1S0daMWJtTjBhVzl1S0hJcGV3cHBaaWh5TG5OMFlYUjFjejA5UFRRd01TbDdaMkYwWlNncE8zSmxkSFZ5YmlCdWRXeHNPMzBLY21WMGRYSnVJSEl1YjJzL2NpNXFjMjl1S0NrNmJuVnNiRHQ5S1M1MGFHVnVLR1oxYm1OMGFXOXVLR1FwZTJsbUtHUXBjbVZ1WkdWeUtHUXBPMzBwQ2k1allYUmphQ2htZFc1amRHbHZiaWdwZXdwbGJDZ2ljM1ZpSWlrdWRHVjRkRU52Ym5SbGJuUTlJa052ZFd4a0lHNXZkQ0JzYjJGa0lIUm9aU0JzYVdKeVlYSjVJT0tBbENCMGFHVWdjM1J5WldGdElITmxjblpsY2lCdFlYa2dZbVVnY21WemRHRnlkR2x1Wnk0aU8zMHBPMzBLYzJ0bGJHVjBiMjRvS1R0c2IyRmtLQ2s3Q24wcEtDazdDand2YzJOeWFYQjBQZ284TDJKdlpIaytDand2YUhSdGJENEsiCikuZGVjb2RlKCJ1dGYtOCIpCgpwYXRoID0gc3lzLmFyZ3ZbMV0Kd2l0aCBvcGVuKHBhdGgsICJyIiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIHNyYyA9IGYucmVhZCgpCgppZiAicjI1YzE5IiBpbiBzcmM6CiAgICBwcmludCgicGF0Y2hfcjI1YzE5X3dzOiBhbHJlYWR5IGFwcGxpZWQiKQogICAgc3lzLmV4aXQoMCkKCiMgd3JpdGUgdGhlIHRlbXBsYXRlIG5leHQgdG8gdGhlIGV4aXN0aW5nIG9uZXMKdHBsX2RpciA9IG9zLnBhdGguam9pbihvcy5wYXRoLmRpcm5hbWUob3MucGF0aC5hYnNwYXRoKHBhdGgpKSwgInRlbXBsYXRlcyIpCm9zLm1ha2VkaXJzKHRwbF9kaXIsIGV4aXN0X29rPVRydWUpCndpdGggb3Blbihvcy5wYXRoLmpvaW4odHBsX2RpciwgInBsYXlsaXN0cy5odG1sIiksICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoUEFHRSkKCm9sZCA9ICdAYXBwLmdldCgiL3BsYXlsaXN0L3t0b2tlbn0iLCByZXNwb25zZV9jbGFzcz1IVE1MUmVzcG9uc2UpXG5hc3luYyBkZWYgcGxheWxpc3RfcGFnZSh0b2tlbjogc3RyLCByZXF1ZXN0OiBSZXF1ZXN0KTonCm5ldyA9ICcjIFdaRklYIHIyNWMxOTogcGxheWxpc3QgbGlicmFyeVxuQGFwcC5nZXQoIi9wbGF5bGlzdHMiLCByZXNwb25zZV9jbGFzcz1IVE1MUmVzcG9uc2UpXG5hc3luYyBkZWYgcGxheWxpc3RzX3BhZ2UocmVxdWVzdDogUmVxdWVzdCk6XG4gICAgcmVzcG9uc2UgPSB0ZW1wbGF0ZXMuVGVtcGxhdGVSZXNwb25zZShyZXF1ZXN0LCAicGxheWxpc3RzLmh0bWwiKVxuICAgIHJlc3BvbnNlLmhlYWRlcnNbIkNhY2hlLUNvbnRyb2wiXSA9ICJuby1jYWNoZSwgbm8tc3RvcmUsIG11c3QtcmV2YWxpZGF0ZSJcbiAgICByZXR1cm4gcmVzcG9uc2VcblxuXG5AYXBwLmdldCgiL2FwaS9wbGF5bGlzdHMiKVxuYXN5bmMgZGVmIHBsYXlsaXN0c19hcGkocmVxdWVzdDogUmVxdWVzdCk6XG4gICAgcGFyYW1zID0ge31cbiAgICBpZiByZXF1ZXN0LnF1ZXJ5X3BhcmFtcy5nZXQoImF1dGgiKTpcbiAgICAgICAgcGFyYW1zWyJhdXRoIl0gPSByZXF1ZXN0LnF1ZXJ5X3BhcmFtcy5nZXQoImF1dGgiKVxuICAgIHRyeTpcbiAgICAgICAgYXN5bmMgd2l0aCBodHRwX3Nlc3Npb24uZ2V0KGYie1NUUkVBTV9CQVNFfS9fcGxheWxpc3RzIiwgcGFyYW1zPXBhcmFtcykgYXMgdXBzdHJlYW06XG4gICAgICAgICAgICBib2R5ID0gYXdhaXQgdXBzdHJlYW0ucmVhZCgpXG4gICAgICAgICAgICBzdGF0dXMgPSB1cHN0cmVhbS5zdGF0dXNcbiAgICBleGNlcHQgQ2xpZW50RXJyb3IgYXMgZTpcbiAgICAgICAgcmFpc2UgX3N0cmVhbV9vZmZsaW5lKCkgZnJvbSBlXG4gICAgcmV0dXJuIFJlc3BvbnNlKFxuICAgICAgICBjb250ZW50PWJvZHksXG4gICAgICAgIHN0YXR1c19jb2RlPXN0YXR1cyxcbiAgICAgICAgbWVkaWFfdHlwZT0iYXBwbGljYXRpb24vanNvbiIsXG4gICAgICAgIGhlYWRlcnM9eyJDYWNoZS1Db250cm9sIjogIm5vLXN0b3JlIiwgIlJlZmVycmVyLVBvbGljeSI6ICJuby1yZWZlcnJlciJ9LFxuICAgIClcblxuXG5AYXBwLmdldCgiL3BsYXlsaXN0L3t0b2tlbn0iLCByZXNwb25zZV9jbGFzcz1IVE1MUmVzcG9uc2UpXG5hc3luYyBkZWYgcGxheWxpc3RfcGFnZSh0b2tlbjogc3RyLCByZXF1ZXN0OiBSZXF1ZXN0KTonCmFzc2VydCBzcmMuY291bnQob2xkKSA9PSAxLCAicGxheWxpc3QgcGFnZSByb3V0ZSBhbmNob3IiCnNyYyA9IHNyYy5yZXBsYWNlKG9sZCwgbmV3LCAxKQoKd2l0aCBvcGVuKHBhdGgsICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoc3JjKQpwcmludCgicGF0Y2hfcjI1YzE5X3dzOiAvcGxheWxpc3RzIHBhZ2UgKyAvYXBpL3BsYXlsaXN0cyBwcm94eSBhZGRlZCIpCg=="),
    ('patch_r25c26_msrv.py',
     'bot/core/stream_server.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMjZfbXNydi5weSAtIG9ubGluZSBtdXNpYyBvbiB0aGUgc3RyZWFtIHNlcnZlci4KCkFkZHMgdGhyZWUgZW5kcG9pbnRzIChhbGwgU1RSRUFNX1BBU1MtZ2F0ZWQgYnkgdGhlIGV4aXN0aW5nCl91c19jaGVja19hdXRoKToKICAvX211c2ljc2VhcmNoP3E9ICAgSmlvU2Fhdm4gc2VhcmNoLmdldFJlc3VsdHMgLT4gY2xlYW4gSlNPTiBsaXN0CiAgL19tdXNpY3N0cmVhbT9xPSAgIHl0LWRscCByZXNvbHZlcyB0aGUgYmVzdCBZb3VUdWJlIG1hdGNoLCB0aGVuIHRoZQogICAgICAgICAgICAgICAgICAgICBhdWRpbyBieXRlcyBhcmUgcHJveGllZCB0aHJvdWdoIChSYW5nZSBzdXBwb3J0ZWQsCiAgICAgICAgICAgICAgICAgICAgIHNvIHNlZWtpbmcgd29ya3MpOyBvcHRpb25hbCAmZGw9MSArICZuYW1lPSBzZXQgYW4KICAgICAgICAgICAgICAgICAgICAgYXR0YWNobWVudCBDb250ZW50LURpc3Bvc2l0aW9uLgpSZXNvbHV0aW9uIHJlc3VsdHMgYXJlIGNhY2hlZCB+OTAgbWluOyBzZWFyY2hlcyB+MTAgbWluOyBjb25jdXJyZW50CnJlc29sdXRpb25zIGZvciB0aGUgc2FtZSBxdWVyeSBzaGFyZSBvbmUgbG9jay4KIiIiCmltcG9ydCBzeXMKCkhBTkRMRVIgPSAnJycKCiMgLS0tLS0tLS0tLS0tLS0tLS0tIFdaRklYIHIyNWMyNjogb25saW5lIG11c2ljIC0tLS0tLS0tLS0tLS0tLS0tLQppbXBvcnQgYXN5bmNpbyBhcyBfYWlvMjYKaW1wb3J0IGh0bWwgYXMgX2h0bWwyNgppbXBvcnQganNvbiBhcyBfanNvbjI2CmltcG9ydCB0aW1lIGFzIF90aW1lMjYKaW1wb3J0IHVybGxpYi5wYXJzZSBhcyBfdXAyNgppbXBvcnQgdXJsbGliLnJlcXVlc3QgYXMgX3VyMjYKCmltcG9ydCBhaW9odHRwIGFzIF9haW9odHRwMjYKCl9XWjI2X1JFU09MVkVEID0ge30KX1daMjZfUkxPQ0sgPSB7fQpfV1oyNl9SU0VNID0gTm9uZQpfV1oyNl9TRUFSQ0hFRCA9IHt9Cl9XWjI2X0hTRVNTID0gTm9uZQoKCmRlZiBfd3oyNl9rZXkocSk6CiAgICByZXR1cm4gIiAiLmpvaW4oKHEgb3IgIiIpLmxvd2VyKCkuc3BsaXQoKSlbOjIyMF0KCgpkZWYgX3d6MjZfc2Fhdm5fc2VhcmNoKHEsIGxpbWl0KToKICAgIGFwaSA9ICgKICAgICAgICAiaHR0cHM6Ly93d3cuamlvc2Fhdm4uY29tL2FwaS5waHA/X19jYWxsPXNlYXJjaC5nZXRSZXN1bHRzIgogICAgICAgICImcT0iICsgX3VwMjYucXVvdGUocSkgKwogICAgICAgICImX2Zvcm1hdD1qc29uJl9tYXJrZXI9MCZhcGlfdmVyc2lvbj00JmN0eD13ZWI2ZG90MCZuPSIKICAgICAgICArIHN0cihsaW1pdCkgKyAiJnA9MSIKICAgICkKICAgIHJlcSA9IF91cjI2LlJlcXVlc3QoYXBpLCBoZWFkZXJzPXsiVXNlci1BZ2VudCI6ICJNb3ppbGxhLzUuMCJ9KQogICAgd2l0aCBfdXIyNi51cmxvcGVuKHJlcSwgdGltZW91dD0zMCkgYXMgcjoKICAgICAgICBkYXRhID0gX2pzb24yNi5sb2FkcyhyLnJlYWQoKS5kZWNvZGUoKSkKICAgIHJlcyA9IGRhdGEuZ2V0KCJyZXN1bHRzIikKICAgIGlmIGlzaW5zdGFuY2UocmVzLCBkaWN0KToKICAgICAgICByZXMgPSByZXMuZ2V0KCJzb25ncyIpIG9yIFtdCiAgICBvdXQgPSBbXQogICAgZm9yIHMgaW4gcmVzIG9yIFtdOgogICAgICAgIHRyeToKICAgICAgICAgICAgbWkgPSBzLmdldCgibW9yZV9pbmZvIikgb3Ige30KICAgICAgICAgICAgaWYgaXNpbnN0YW5jZShtaSwgc3RyKToKICAgICAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgICAgICBtaSA9IF9qc29uMjYubG9hZHMobWkpCiAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAgIG1pID0ge30KICAgICAgICAgICAgdGl0bGUgPSBfaHRtbDI2LnVuZXNjYXBlKHMuZ2V0KCJ0aXRsZSIpIG9yICIiKQogICAgICAgICAgICBhcnRpc3RzID0gKChtaS5nZXQoImFydGlzdE1hcCIpIG9yIHt9KS5nZXQoInByaW1hcnlfYXJ0aXN0cyIpKSBvciBbXQogICAgICAgICAgICBuYW1lcyA9IFtdCiAgICAgICAgICAgIGZvciBhIGluIGFydGlzdHM6CiAgICAgICAgICAgICAgICBuID0gX2h0bWwyNi51bmVzY2FwZShhLmdldCgibmFtZSIpIG9yICIiKQogICAgICAgICAgICAgICAgaWYgbiBhbmQgbiBub3QgaW4gbmFtZXM6CiAgICAgICAgICAgICAgICAgICAgbmFtZXMuYXBwZW5kKG4pCiAgICAgICAgICAgIG91dC5hcHBlbmQoCiAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgImlkIjogcy5nZXQoImlkIikgb3IgIiIsCiAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiB0aXRsZSwKICAgICAgICAgICAgICAgICAgICAiYXJ0aXN0IjogIiwgIi5qb2luKG5hbWVzWzoyXSksCiAgICAgICAgICAgICAgICAgICAgImFsYnVtIjogX2h0bWwyNi51bmVzY2FwZShtaS5nZXQoImFsYnVtIikgb3IgIiIpLAogICAgICAgICAgICAgICAgICAgICJkdXIiOiBpbnQoZmxvYXQobWkuZ2V0KCJkdXJhdGlvbiIpIG9yIDApKSwKICAgICAgICAgICAgICAgICAgICAiY292ZXIiOiAocy5nZXQoImltYWdlIikgb3IgIiIpLnJlcGxhY2UoIi81MHg1MC8iLCAiLzE1MHgxNTAvIiksCiAgICAgICAgICAgICAgICAgICAgImxhbmciOiBtaS5nZXQoImxhbmd1YWdlIikgb3IgIiIsCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBjb250aW51ZQogICAgcmV0dXJuIG91dAoKCmFzeW5jIGRlZiBfbXVzaWMyNl9zZWFyY2gocmVxdWVzdCk6CiAgICBpZiBub3QgX3VzX2NoZWNrX2F1dGgocmVxdWVzdCk6CiAgICAgICAgcmFpc2Ugd2ViLkhUVFBVbmF1dGhvcml6ZWQoCiAgICAgICAgICAgIHRleHQ9ImF1dGhlbnRpY2F0ZSBmaXJzdCIsCiAgICAgICAgICAgIGhlYWRlcnM9eyJYLVN0cmVhbS1BdXRoLVJlcXVpcmVkIjogIjEifSwKICAgICAgICApCiAgICBxID0gKHJlcXVlc3QucXVlcnkuZ2V0KCJxIikgb3IgIiIpLnN0cmlwKCkKICAgIGlmIG5vdCBxOgogICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSh7InJlc3VsdHMiOiBbXX0pCiAgICBrZXkgPSBfd3oyNl9rZXkocSkKICAgIG5vdyA9IF90aW1lMjYudGltZSgpCiAgICBoaXQgPSBfV1oyNl9TRUFSQ0hFRC5nZXQoa2V5KQogICAgaWYgaGl0IGFuZCBub3cgLSBoaXRbMF0gPCA2MDA6CiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsicmVzdWx0cyI6IGhpdFsxXX0pCiAgICB0cnk6CiAgICAgICAgcmVzID0gYXdhaXQgX2FpbzI2LnRvX3RocmVhZChfd3oyNl9zYWF2bl9zZWFyY2gsIHEsIDMwKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcigicjI1YzI2OiBzYWF2biBzZWFyY2ggZmFpbGVkOiAlcyIsIGUpCiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsicmVzdWx0cyI6IFtdLCAiZXJyb3IiOiBzdHIoZSlbOjE2MF19KQogICAgX1daMjZfU0VBUkNIRURba2V5XSA9IChub3csIHJlcykKICAgIGlmIGxlbihfV1oyNl9TRUFSQ0hFRCkgPiAxMjA6CiAgICAgICAgZm9yIGsgaW4gbGlzdChfV1oyNl9TRUFSQ0hFRCk6CiAgICAgICAgICAgIGlmIG5vdyAtIF9XWjI2X1NFQVJDSEVEW2tdWzBdID4gNjAwOgogICAgICAgICAgICAgICAgZGVsIF9XWjI2X1NFQVJDSEVEW2tdCiAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJyZXN1bHRzIjogcmVzfSkKCgpkZWYgX3d6MjZfeXRfcmVzb2x2ZShxKToKICAgIGZyb20geXRfZGxwIGltcG9ydCBZb3V0dWJlREwKCiAgICBvcHRzID0gewogICAgICAgICJmb3JtYXQiOiAiYmVzdGF1ZGlvL2Jlc3QiLAogICAgICAgICJxdWlldCI6IFRydWUsCiAgICAgICAgIm5vX3dhcm5pbmdzIjogVHJ1ZSwKICAgICAgICAibm9wbGF5bGlzdCI6IFRydWUsCiAgICAgICAgInNvY2tldF90aW1lb3V0IjogMzAsCiAgICB9CiAgICB3aXRoIFlvdXR1YmVETChvcHRzKSBhcyB5OgogICAgICAgIGluZm8gPSB5LmV4dHJhY3RfaW5mbygieXRzZWFyY2gxOiIgKyBxLCBkb3dubG9hZD1GYWxzZSkKICAgICAgICBpZiBpbmZvIGlzIE5vbmU6CiAgICAgICAgICAgIHJhaXNlIFJ1bnRpbWVFcnJvcigibm8gcmVzdWx0IikKICAgICAgICBpZiAiZW50cmllcyIgaW4gaW5mbzoKICAgICAgICAgICAgZW50cyA9IFtlIGZvciBlIGluIChpbmZvLmdldCgiZW50cmllcyIpIG9yIFtdKSBpZiBlXQogICAgICAgICAgICBpZiBub3QgZW50czoKICAgICAgICAgICAgICAgIHJhaXNlIFJ1bnRpbWVFcnJvcigibm8gc2VhcmNoIHJlc3VsdCIpCiAgICAgICAgICAgIGluZm8gPSBlbnRzWzBdCiAgICAgICAgdXJsID0gaW5mby5nZXQoInVybCIpCiAgICAgICAgaWYgbm90IHVybDoKICAgICAgICAgICAgcmFpc2UgUnVudGltZUVycm9yKCJubyBzdHJlYW0gdXJsIikKICAgICAgICByZXR1cm4gdXJsCgoKYXN5bmMgZGVmIF93ejI2X3Jlc29sdmUocSk6CiAgICBnbG9iYWwgX1daMjZfUlNFTQogICAga2V5ID0gX3d6MjZfa2V5KHEpCiAgICBub3cgPSBfdGltZTI2LnRpbWUoKQogICAgaGl0ID0gX1daMjZfUkVTT0xWRUQuZ2V0KGtleSkKICAgIGlmIGhpdCBhbmQgaGl0WzFdID4gbm93OgogICAgICAgIHJldHVybiBoaXRbMF0KICAgIGxvY2sgPSBfV1oyNl9STE9DSy5zZXRkZWZhdWx0KGtleSwgX2FpbzI2LkxvY2soKSkKICAgIGFzeW5jIHdpdGggbG9jazoKICAgICAgICBoaXQgPSBfV1oyNl9SRVNPTFZFRC5nZXQoa2V5KQogICAgICAgIGlmIGhpdCBhbmQgaGl0WzFdID4gbm93OgogICAgICAgICAgICByZXR1cm4gaGl0WzBdCiAgICAgICAgaWYgX1daMjZfUlNFTSBpcyBOb25lOgogICAgICAgICAgICBfV1oyNl9SU0VNID0gX2FpbzI2LlNlbWFwaG9yZSgyKQogICAgICAgIGFzeW5jIHdpdGggX1daMjZfUlNFTToKICAgICAgICAgICAgdXJsID0gYXdhaXQgX2FpbzI2LnRvX3RocmVhZChfd3oyNl95dF9yZXNvbHZlLCBxKQogICAgICAgIF9XWjI2X1JFU09MVkVEW2tleV0gPSAodXJsLCBub3cgKyA1NDAwKQogICAgICAgIGlmIGxlbihfV1oyNl9SRVNPTFZFRCkgPiAyNDA6CiAgICAgICAgICAgIGZvciBrIGluIGxpc3QoX1daMjZfUkVTT0xWRUQpOgogICAgICAgICAgICAgICAgaWYgX1daMjZfUkVTT0xWRURba11bMV0gPCBub3c6CiAgICAgICAgICAgICAgICAgICAgZGVsIF9XWjI2X1JFU09MVkVEW2tdCiAgICAgICAgcmV0dXJuIHVybAoKCmRlZiBfd3oyNl9oc2VzcygpOgogICAgZ2xvYmFsIF9XWjI2X0hTRVNTCiAgICBpZiBfV1oyNl9IU0VTUyBpcyBOb25lIG9yIF9XWjI2X0hTRVNTLmNsb3NlZDoKICAgICAgICBfV1oyNl9IU0VTUyA9IF9haW9odHRwMjYuQ2xpZW50U2Vzc2lvbigKICAgICAgICAgICAgYXV0b19kZWNvbXByZXNzPUZhbHNlLAogICAgICAgICAgICB0aW1lb3V0PV9haW9odHRwMjYuQ2xpZW50VGltZW91dCh0b3RhbD1Ob25lLCBzb2NrX2Nvbm5lY3Q9MzAsIHNvY2tfcmVhZD0xODApLAogICAgICAgICkKICAgIHJldHVybiBfV1oyNl9IU0VTUwoKCmFzeW5jIGRlZiBfbXVzaWMyNl9zdHJlYW0ocmVxdWVzdCk6CiAgICBpZiBub3QgX3VzX2NoZWNrX2F1dGgocmVxdWVzdCk6CiAgICAgICAgcmFpc2Ugd2ViLkhUVFBVbmF1dGhvcml6ZWQoCiAgICAgICAgICAgIHRleHQ9ImF1dGhlbnRpY2F0ZSBmaXJzdCIsCiAgICAgICAgICAgIGhlYWRlcnM9eyJYLVN0cmVhbS1BdXRoLVJlcXVpcmVkIjogIjEifSwKICAgICAgICApCiAgICBxID0gKHJlcXVlc3QucXVlcnkuZ2V0KCJxIikgb3IgIiIpLnN0cmlwKCkKICAgIGlmIG5vdCBxOgogICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSh7ImVycm9yIjogIm1pc3NpbmcgcSJ9LCBzdGF0dXM9NDAwKQogICAgdHJ5OgogICAgICAgIHVybCA9IGF3YWl0IF93ejI2X3Jlc29sdmUocSkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICBMT0dHRVIuZXJyb3IoInIyNWMyNjogcmVzb2x2ZSBmYWlsZWQgZm9yICVyOiAlcyIsIHEsIGUpCiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKAogICAgICAgICAgICB7ImVycm9yIjogInJlc29sdmUgZmFpbGVkOiAiICsgc3RyKGUpWzoxNjBdfSwgc3RhdHVzPTUwMgogICAgICAgICkKICAgIGhkcnMgPSB7IlVzZXItQWdlbnQiOiAiTW96aWxsYS81LjAifQogICAgcm5nID0gcmVxdWVzdC5oZWFkZXJzLmdldCgiUmFuZ2UiKQogICAgaWYgcm5nOgogICAgICAgIGhkcnNbIlJhbmdlIl0gPSBybmcKICAgIHNlcyA9IF93ejI2X2hzZXNzKCkKICAgIHRyeToKICAgICAgICB1cCA9IGF3YWl0IHNlcy5nZXQodXJsLCBoZWFkZXJzPWhkcnMsIGFsbG93X3JlZGlyZWN0cz1UcnVlKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcigicjI1YzI2OiB1cHN0cmVhbSBmYWlsZWQ6ICVzIiwgZSkKICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJ1cHN0cmVhbSBmYWlsZWQifSwgc3RhdHVzPTUwMikKICAgIG91dCA9IHt9CiAgICBmb3IgaywgdiBpbiB1cC5oZWFkZXJzLml0ZW1zKCk6CiAgICAgICAgaWYgay5sb3dlcigpIGluICgKICAgICAgICAgICAgImNvbnRlbnQtdHlwZSIsCiAgICAgICAgICAgICJjb250ZW50LWxlbmd0aCIsCiAgICAgICAgICAgICJjb250ZW50LXJhbmdlIiwKICAgICAgICAgICAgImFjY2VwdC1yYW5nZXMiLAogICAgICAgICAgICAiZXRhZyIsCiAgICAgICAgICAgICJsYXN0LW1vZGlmaWVkIiwKICAgICAgICApOgogICAgICAgICAgICBvdXRba10gPSB2CiAgICBvdXQuc2V0ZGVmYXVsdCgiQWNjZXB0LVJhbmdlcyIsICJieXRlcyIpCiAgICBvdXRbIkNhY2hlLUNvbnRyb2wiXSA9ICJuby1zdG9yZSIKICAgIG91dFsiUmVmZXJyZXItUG9saWN5Il0gPSAibm8tcmVmZXJyZXIiCiAgICBvdXRbIlgtQ29udGVudC1UeXBlLU9wdGlvbnMiXSA9ICJub3NuaWZmIgogICAgaWYgcmVxdWVzdC5xdWVyeS5nZXQoImRsIik6CiAgICAgICAgZm5hbWUgPSBfdXAyNi51bnF1b3RlKHJlcXVlc3QucXVlcnkuZ2V0KCJuYW1lIikgb3IgInNvbmciKQogICAgICAgIGZuYW1lID0gIiIuam9pbihjIGZvciBjIGluIGZuYW1lIGlmIGMgbm90IGluICdcXFxcLzoqPyI8PnxcXHJcXG5cXHQnKS5zdHJpcCgpIG9yICJzb25nIgogICAgICAgIG91dFsiQ29udGVudC1EaXNwb3NpdGlvbiJdID0gImF0dGFjaG1lbnQ7IGZpbGVuYW1lKj1VVEYtOCcnIiArIF91cDI2LnF1b3RlKGZuYW1lKQogICAgcmVzcCA9IHdlYi5TdHJlYW1SZXNwb25zZShzdGF0dXM9dXAuc3RhdHVzLCBoZWFkZXJzPW91dCkKICAgIHRyeToKICAgICAgICBhd2FpdCByZXNwLnByZXBhcmUocmVxdWVzdCkKICAgICAgICBhc3luYyBmb3IgY2h1bmsgaW4gdXAuY29udGVudC5pdGVyX2NodW5rZWQoMjYyMTQ0KToKICAgICAgICAgICAgYXdhaXQgcmVzcC53cml0ZShjaHVuaykKICAgIGV4Y2VwdCBDb25uZWN0aW9uUmVzZXRFcnJvcjoKICAgICAgICBwYXNzCiAgICBleGNlcHQgR2VuZXJhdG9yRXhpdDoKICAgICAgICByYWlzZQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcigicjI1YzI2OiBwdW1wIGZhaWxlZDogJXMiLCBlKQogICAgZmluYWxseToKICAgICAgICB1cC5yZWxlYXNlKCkKICAgIHJldHVybiByZXNwCicnJwoKUk9VVEVTX09MRCA9ICcgICAgYXBwLnJvdXRlci5hZGRfcm91dGUoIkdFVCIsICIvX3BsYXlsaXN0cyIsIF9wbGF5bGlzdHMpJwpST1VURVNfTkVXID0gKAogICAgJyAgICBhcHAucm91dGVyLmFkZF9yb3V0ZSgiR0VUIiwgIi9fcGxheWxpc3RzIiwgX3BsYXlsaXN0cylcbicKICAgICIgICAgIyBXWkZJWCByMjVjMjY6IG9ubGluZSBtdXNpY1xuIgogICAgJyAgICBhcHAucm91dGVyLmFkZF9yb3V0ZSgiR0VUIiwgIi9fbXVzaWNzZWFyY2giLCBfbXVzaWMyNl9zZWFyY2gpXG4nCiAgICAnICAgIGFwcC5yb3V0ZXIuYWRkX3JvdXRlKCJHRVQiLCAiL19tdXNpY3N0cmVhbSIsIF9tdXNpYzI2X3N0cmVhbSknCikKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMjYiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMjZfbXNydjogYWxyZWFkeSBhcHBsaWVkIikKICAgIHN5cy5leGl0KDApCgppZiAiX211c2ljMjZfc2VhcmNoIiBpbiBzcmM6CiAgICBwcmludCgicGF0Y2hfcjI1YzI2X21zcnY6IGhhbmRsZXIgYWxyZWFkeSBwcmVzZW50IChwYXJ0aWFsKSIpCiAgICBzeXMuZXhpdCgwKQoKc3JjICs9IEhBTkRMRVIKYXNzZXJ0IHNyYy5jb3VudChST1VURVNfT0xEKSA9PSAxLCAicGxheWxpc3RzIHJvdXRlIGFuY2hvciIKc3JjID0gc3JjLnJlcGxhY2UoUk9VVEVTX09MRCwgUk9VVEVTX05FVywgMSkKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKHNyYykKcHJpbnQoInBhdGNoX3IyNWMyNl9tc3J2OiAvX211c2ljc2VhcmNoICsgL19tdXNpY3N0cmVhbSBhZGRlZCIpCg=="),
    ('patch_r25c26_mws.py',
     'web/wserver.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMjZfbXdzLnB5IC0gb25saW5lIG11c2ljIG9uIHRoZSBGYXN0QVBJIHdlYiBzZXJ2ZXIuCgpTZXJ2ZXMgdGhlIC9tdXNpYyBwYWdlIGFuZCBwcm94aWVzIHRoZSBtdXNpYyBlbmRwb2ludHMgdG8gdGhlIHN0cmVhbQpzZXJ2ZXI6CiAgL211c2ljICAgICAgICAgICAgICAgLT4gbXVzaWMuaHRtbCAod3JpdHRlbiBuZXh0IHRvIHRoZSB0ZW1wbGF0ZXMpCiAgL2FwaS9tdXNpYy9zZWFyY2ggICAgLT4gL19tdXNpY3NlYXJjaCAgKEpTT04pCiAgL2FwaS9tdXNpYy9zdHJlYW0gICAgLT4gL19tdXNpY3N0cmVhbSAgKGJ5dGUgcHJveHkgd2l0aCBSYW5nZQogICAgICAgICAgICAgICAgICAgICAgICAgcGFzcy10aHJvdWdoLCBzbyB0aGUgYnJvd3NlciBhdWRpbyBlbGVtZW50IGNhbgogICAgICAgICAgICAgICAgICAgICAgICAgc2VlazsgdGhlIHdvcmtlciBzZXRzIENvbnRlbnQtRGlzcG9zaXRpb24gZm9yCiAgICAgICAgICAgICAgICAgICAgICAgICBkb3dubG9hZHMgdmlhICZkbD0xKQoiIiIKaW1wb3J0IGJhc2U2NAppbXBvcnQgZ3ppcAppbXBvcnQgb3MKaW1wb3J0IHN5cwoKUEFHRSA9IGd6aXAuZGVjb21wcmVzcyhiYXNlNjQuYjY0ZGVjb2RlKAogICAgIkg0c0lBSjRrdW1vQy85VjkyNUxiU0piWWUzOUZDaFBxSWlVU3hVdXhycXJxVWVzUzA3dWpibzFLUFJjckZCc29Ja21pQ3dSSUFHUlZ0VVlSczM3dzNiRU94M2czTm1JZDgySTciCiAgICAiL0xMMmd5TjIvYno3Si8wRjh3ays1MlRpbHNnRXdTcEpQYTV1VlpHSnZKNDhlZTU1OE9qZTAyK2V2UDdOeTJkc2xzejlzODhlM2V0MjJhLyt4Zk92ZnMyaXdXZzgyRDltWWVCNyIKICAgICJBV2Z6VmV5TjJRKy8rejM3TXk4OGQ1eDF3T1k4Y1Z3bmNUcnNOK0hxOWVxQ3M0WHYzRnc0NDB2VzdVSmYyQ1h6bldCNmF2SEFPdnNNU3Jqam5uM0c0T2NSTm1iam1SUEZQRG0xIgogICAgInZuMzl2SHRvRlI4RnpweWZXbXVQWHkzQ0tMSFlPQXdTSGtEVks4OU5acWN1WDN0ajNxVXZIZVlGWHVJNWZqY2VPejQvN2R1OURrdGJkaWRlY2pvTzF6elNkQi94Q1k4aWVKUjMiCiAgICAiSDRUZHJMVGFJSm54T2UrT1F6OHN0dmxKRDMrR2FmM0VTM3grOW8yQTJ3dUMyei85by96d3M5WEZvMTFSUVZTR1NwY3M0djZwNVVGL0ZoWGl6d3htY1dvaGVJKzl1VFBsdS9GNiIKICAgICIrdkI2N25mdUQ1L0FSd1lmZy9oMFo1WWtpK1BkM2F1cksvdHFhSWZSZEhjQVU4SEtPd1NDTDhQcjA1MGU2N0Yrai83dDNCOCtneDRpUGs2WWdPUU9scklaOTZhelJINkpvTTFnIgogICAgIllJOTIyTVR6L2RPZCs0UGg4OUh6ZytmUGQzWkY4NFdUekpoN3V2T0M5WS9zUGJZM3NQZlpFemJvMlQwMkdrRUJOTjVqKy92NDZjZytaUHVIOWhDZUR3L2crVUhQN3JPOW9UMWciCiAgICAiKzdCUGJHOEVuL1lPb2RJVHRuZGdqMFRwYUNqYTc0dXZzajM4MmNmMjBNa0lQc0ZJSTNiWWcwL0RrVDFNNXhxRUFkOWhjUktGbDV4bVBueDZpRE9YUmQxMDBYMjduNVhoUm8yZCIKICAgICJ4ZWxPRks0Q3QxVDhYZWdGYWJsY1BBSVhQbG1WRFZ3QVZNTUF1Z0owRmR1SHV4UEQ5a3dBVTJKN0dvWlRuenNMTDdiSDRmd1c3ZVBFU2J3eE5XYmpLSXpqTVBLbVhsRG9TRUdmIgogICAgInV2RjN4M0U4K0dMaXpEMy81dlFGVmdDa2Q1TGpLOENEbis3MWVpY2orTGNQL3c3ZzMyR3Y5N21zK3BwZkpxdElWQ3RVK2R6MVlqeitwL0dWczhqUm1CWVdKemMrajJlY0orbWkiCiAgICAicWVRc3EzVWNoV0hDM21YZjhZY09HWnhvUEhISHpIV2l5NVBTODI3M1luck01TkZUSDhXcmFPS01PVDdmZy8rK05EenZEckRHQWZ6M1hLMkJtMy9Nb3VtRjA5b2JkTmpScUFQNCIKICAgICJmZGhoOXVDd3JhdExYUTBlajU0LzdhbVBFMzZkd0VOeGhOU0g4MVhDWFhqNlpQamt5NmVWcDg1NERCUUdIZ3NzMWorbW9VZGZIajAxVm9qRENYYlMyKytQaHBVNjBUSHJEeGJYIgogICAgIkZSQTVRWHpNZG5MTTJPbXdyck5ZK0x3YjM4UUpuM2VZK050ZGVmQVJhbmVobmpkUis1RjRBVjBKeklGdXlwMXU2T1g5WjluSEJ3cUtYSVRYM2RqNzNnc0FFUzdDeU9WUkY0cksiCiAgICAiRTVnN0VSeVJZNmJzeXNKeFhXclgwNDcwWnVhNUxnL2VLZ05tYTBFcXcrNTVjK1F3VHBCbys3Z0kzUnQxd3NBYnAwUk1qdG5haVZxSXd3bzJFZHFuVHhGemxPZDRsTHZpS0thMSIKICAgICJFR3BLcmJrWGRBVkpQMGFxNzY1bko0YUpkSW03QUtZN0xuTFFLZjRGbEduMW9kbmltbzNvdDVPd3c5RjkxdTMzN25mRW1kanZkMWgvaUlkaUJML3MvbjY3dzVJSUpySndJbWpPIgogICAgIjludjMyNTNTa0VRTmxFR09xUGU5ZlRsRzd6NHpEZEE3VkFZWWplNjN0V0MzblZYa0tIQmZoREVJQ0NHZ3djUzc1cTRDQzRFNU9LY1ZZUHlvZDErQnVPY25ITGJrd2w5Rk1PUEYiCiAgICAidFFMcmNPR012UVIydzFiSTBBSjRCN1RzOGpWTU9CWklVNjd4ZmRjTFhINTl6TG9EODFwc3A2OHNoM2pZTWNCdGZWWHVMOTN5NmhPZkl3WG85Z2ZxZ3lSY1lIbFBMUytpcXJwdCIKICAgICJZeThhK3h4M2JBaFlBZjg2RWhNRnVWRlJvYlJUK09NRWdIUmlQMXc0NjBtZkRmWmp4cDJZQXp5NjRTb0JnVzZDTWgyTWdiQVBuSVRYd1dkZ2dFL1BDSi9LazBnODBBQ0NBRFRzIgogICAgIm1VOVFIWHpnR01CZ0NueTZnNjBoTkdERHdhMGc5Tk5MZmpPSlFIYU5VMUMvVTFhbkZGQWh6bTBTUnZOajhkR0gzbHRINnlzNGtZUDFyTTFJekc2QkJEVlNwdjIreVJRR3Q1NUMiCiAgICAiOXhEbjBEMHF6YUhKRk95cnlGa29vOHlkNjY1RWxFTWtRQWEyd1p4VkVocDR4OWgzNW90V1AwSmVPQ1R3MlB2d3BhMnREcXdwU1VKWVQvK2dORnB4bW9CckpvNHo4Ymt5UmNmMyIKICAgICJwa0VYVUdBT3RBWHhpa2ZsQ2xNSE1KZG1wRnRiUGg5N3IxUkZuWkFOOVMvMUoyeHdxTUl0UFdIVkp3cVpyVHpIQmFvMHNqS1ZDOEFGVjVtTGhpTktxT2xZNTVXY0lFcTF4bkVjIgogICAgImV4Yk9lUVZoQ0d5Q2tsYXhvc1M0U2FyVERRL2lDakJiKzdDeUtlYkowWEVBUWFEcmdub1FTYXBRRHljeC8rTVpxcjA2c1Jwa3djbEVMMlB0UG1BeGQ2THhqRDNZelRzVlJSZE8iCiAgICAiOUFuUXN5b2taZEo2VzRkU2dNRWdQc1NoNzdteVBzcmo3VnJzT3pvNlV2RXZPOVcwT1hBcUJwc09qbjFnUERjWnZJNG40WGdWQTZGSlpsNVFFVjlwVGlYRWtSeDBRNjlBL1JlciIKICAgICJwRWE0cklvYUthd1V4QUpXSWpTZGFvc21rdWd4Y0xnZXdLcUh3REtMbzVKY2dFaDV2OUhLam84QnE4WjhGdnF1Q1lFM25MUDBKSTFNeHp3ZkVmUmk3Y1pVWUZWRnpNcHVHVS9ZIgogICAgInRqZzRNdUdnZ0RvUUNNRFRVUzNReDZzb3hvbElNWFE3YWp1ZWVZdTQrV0VYWjNsdlZKMHVWTzBpOXoxbStIc0RJOW8zSGlpY2oyR1htcHoreGlSbEkzWnRzNGxEMmlEN3lMQ0oiCiAgICAiZUhUc2c4UGJiYUlLbk9iRS9qYWtoN2pDR0ZsUHJQQ0ZjVEs3SlUrNEFHa1c5MHFIU1ljYmFlOWhEZTNGT2MwR2R4SVRCSjhHOFhMRE1IYTh1cmdWZ2RJS0FzWCtmUzlPbWtPVyIKICAgICJ6cG5yUldLVGpuRVNxM21nQSsxZ3RBbTJBK09jb3ZES05LVnA1Q2w2TlpZQTM1Z3ZVSGJ2aWdtaGZqMkE4OXFmUkNSQ2FlUW9NY3ZxbWRrb1R4U0lwemgzVlNSU0RxL1lvbWdiIgogICAgInVsa2xPZ1ZGenFqRFRSeVhmd3ZpNjFEb2NOQkxNak5VQmltUDRDa3h0TU42Y1h1VFppZTdWL0E5Q3VjYXhTcXpWaWpNemFSMC9hWjFXTEY0RkdaaFZPQ3ljZnExNDFURmp2Y20iCiAgICAiek5OU3VHMkZ4VExKVTFpRk1xQ05tQTBZdFhISWdwVzFmVGM2U3lmTVRtYXIrWVZlNHhvTlRCcFg5WW1DN2YyS2tsdGNDWUxDaVFyMnYrSEk1ZE1PKzBuUDZUdDdiaWUxSHF2RyIKICAgICJyNHZ2Z09TZ2t3MXB6bnBiTVlPV08vZFVsUTdObDNMQnZUbzRvUi90ZzJza0dpbW00WHo0TXZaREk5SDJ5Q0hZM1RBdkl1UThjUFdiM0I5dHF6YlR4QUtWRW9oVy9TYXJySWpVIgogICAgIis2cHllaldEdVhlQkJvNUpqNmpLZVlnWEV4OU9NQk5tZFkxdW0xZmh2ZytpcHhmWExNZXNobDc0NGZqeWcrcmxuMzV4NHlUKzREamRIeGw1T3JxZEw1TGdReWlURzBGZHkxb2wiCiAgICAiNmczM1RSU3UrbVNUNlY0OWUxVWhoZlRNT3ZnVnNlT29TaG0yTU14SVFHOHBxQmZOek9nWElYZEkvZ3NkSTdYRDJSTm5iWWVCY2J6UjVGQnZDQUpxOWpFb0dlSGp3QjZaTnJtLyIKICAgICJiektPd253OFBWTWNic0NMUVIzalV5enpSaG1PTHk5WVgyK0ROOC8zT0VobVhWRFBmTGMxYUN1VHI0aDhJSnJIamJvYWJ1NnFZVTk3bTN2cUQrSk44aWVDNWwxWkp1emRMM3YvIgogICAgIjBQU2prUkl6aDR5NlE0cUlPYXB0WGVXS2VpRXlDQlArRVl3STdWc29HWm15TXRDVEZEcEplb3BVUzJVcnk3MjlIcnpKL3FkcTVHWVdFMTl5WDVsRnVuWDdtMmk2SG54MWt1dFIiCiAgICAiandUWDhoWUJ0YnpmcVdnSWlGZVZtZ2NWYjF6Qld5NDR3UUN4dVd6TlZLaEZQTVBncjc2OUYrZWVPakhSVFlkSnRHemdJaXZNS3ZkdmQybHF2UWFuWWZjQmhlc0JLMElqYU5HbyIKICAgICJVelh6MS9yUGhVT2twL1ducWhaVWlTczlneHQ4MU52QS9mWTZEUDd2OTRIeEhhbTBHZ1RVaTBzdjZXSWpOd29YM1pManZsOVZZNXRYRkNoSm51QUdoRUkxUkFnM0lYa0xnR3NMIgogICAgIlB5RzYzTnBzN1BqamxxejFrUEZnM1lxZENlODZFWGNBSkRGUDVPbnFNQXc3VUliUmErdjlRZSsrcnFMY3Y2d1JCaklKYnFZL3M0QUU5a3ExS2VpSDdCbmRGZnhTNDNpOXV6UjciCiAgICAic05FNnVGZERqTkpaSlY3RnhVZTBFZmJWYytCdnNKcnp5QnZEVXAyTGxROFVCZ3BpczNCNFVOVWROc3JEYWFEQzNyWmFYV2tSeDc0VEo0S2hxL3RWWUNWMElnMHV3d2hqZGsxSCIKICAgICJQZ0pCSVBIVy9HU3pCcGt4NUlwMzl5NmIzc2dHTHRaZ1I0N25keXFsR0JwcVhKOXpBZWQ1bGZCYlU3WE1Bck9CbFpWZEJZYkoxK2hoV3ZtL3Y5ZXU3Vk96OUlaK3JIcURoK3orIgogICAgIk1nZ3Z0b1JzNmd3Y0dsV0E0Ylo2WG5GRk9tZkhkVGVlT1M0cTRCZ0tEY2lwalRFN2FqZEFBUjBKL0hXck8rcVp3dEVXTlVielcxUEFJeU41bytIc3NVYkhUQzJJZTBZTDR0Ny8iCiAgICAiaHhaRXNkNWcwY2lDYUNSY2pVaE1OcFE5dnEzOXlhQm1Gdm5JL3Y2V3NTRStUekRLRUUxVVF1cm9heldLQXVLdUZnc2VqVXVzLzFPWXVwU29xM0lRbmg3UXlSYUFyb2ZTcDdUaiIKICAgICI1Zk4zUHBxaGN2Q2pXaXJIaWYvQmFkcFFIejFRRzZVQTA3aFlnY3ozYWN5WEpjOXdyenJkRExGTkZMYjZaQk5ucTdXWTN0RzhxWWZrcDdKUDVpTWF6Wk5HbW1ub1orNVVZcXh1IgogICAgIjR6YTdZNWhQQ1V1R1JrWk5vVWZYZnhKNCs2bk44cHVPdFFETkowTEUzUWRzdWVJckRncjRQSXc0bzN0SzVTaVhKVXFQZkJ1YkNDbnZ0UUZrTk1jZUtQYjBQeWlzYllOTjVHRFAiCiAgICAiY04yZ3QrMXRnNklkUVBaQ051Y2FLNEJZZWRVUVlQTHVxNU1vaDNlVWVrYitWTkRVNWdUM0g4bnVkRERhd3U1MHNKWGRhYkRYME82a3E3akI3bVNLY2NqOEhuaXZobjcxMUtXaiIKICAgICJISmJaWUE4cjk0UHVHR2xrTUUzMW01cW1oakhRcEF0djNMM2czM3M4YXRrREJEeGV2b09UM0Rjd0FpMEc1VnU4YnpMU1VlaHFUOXJpOXJjMnhWV1FHazVMQmEvdmFrcGI0czNwIgogICAgIkR5NXhWUzFXR1ZUNmVmd3Bxd25MRlBPYURUOUNySjFXVjZ1T2JTOS9CQjdhLzFGWnFCWUluMHBzVzJyaUVsT3RvWHR6WEhkZHBpY3dTWjYwdzd1ZHRBOFJpampZeDl1SHhYakUiCiAgICAiWnRibWJVSVI2UUFkYklwRXJGcFdHbGtrbG5jTGkxTzd1bVBBVzJWLzdPQldvYkczTk1BZmpyWnlwVlptNjgybmhudUVSdVZoYjdEdExhY04xcTQ3MnRPcU83QU1kS1Q1VHlLVSIKICAgICJTMDdRdVV2NDlNR1BHN0FsbCtDdW9nKzhpRzNPUVVrRytnakc1cjVaU3BDdXl6bzFkeTVDUEgzbm9ra0k0bFp3TzZvM2xPNmJydjVJUVpHbWRpWk1CeDNEVTNzZGJ2VElxRmZoIgogICAgIjI5c0xISnJvQ3BWQlZDaE53Y0s5WnhRc3FwN0ZJamZXeExNVnJoY2QzZUY2MFphaFlqcmMrMjRWSjk3a3BpdXo3SHhneTJHNnk5WDlOVjFMTEFMT2ZHdWoySy91a2w3cS9Pb2IiCiAgICAiN3dETEhreW1NVlBBVW5NYlZkbjJNUVdKcUdUc29JSTdtenBxaExHTklaY0dNOG13ZzJadms1bmt5SERZYVRuMlJYaDl4NHNERFNHdUh0dDk0L25yYThNWU11L0lzR29ybGsrTyIKICAgICJCdmRybG5wN1hVeU5vVEQ3R0dtZ091eXVSRWJsSE9OZ1lMZ1R0OW5YZUpkUXZWdmxXYUhyb3hzSlljMFYydWFYMWdpa1dsZEdVNWcyNGpwYk9YUnZhL3krSS84b1gyWjM0bHNZIgogICAgIkJTc1c1MFplZXhGcHQ5Q0ZyK1JtcEUwbTNYMlFBMEFnMkVkcWRiUy94WVgxVGNhOHFnUlFRSUM2MjhLWmZuUzRoYnhValp0QVhHSERYaG83a1p1dVIrMFRjMklOTlp2S0o1VE8iCiAgICAiZnpybnJ1ZXdWbUUrKzBoVTFaQm56YTNHR3BzQlNsUUdjMEV1UFl5cUFvbk9MS0NwcHNROUY2NkUySzZ2bVdVcEkxVjlWK2JJa05wUXRIcHhVaDJrNmhqT29kTGZ2RnlqUTdkbyIKICAgICJScXVkNHJEUkZNME93eUlzRG10aGNiaHBJSE84aU1ycks4ZmtmUjFDTHloQlpkeU51THNhYzdjN0Q5TkFQZnplVmtQeEtVbFNwd25LRitLWDYrOWw0dTlIdXpLSjM2TmRrZGJ6IgogICAgInMwZVk4VXdtK0hPOU5jYTl4dkdwUmZtNG5MN0ZVSjN0aWxOOWFpWFJpbHRuajNhaG9xbkpvSzZKVE0ySkd5Z2JJUzJ4OHB5Q2xHd1VVRjArQmNKZWVFZ1ZNSTJtZklvcGJpeVoiCiAgICAiVTlNeTU5UzBzcHlhVmlHbnBuYWFGZUErS3VUY3RLaVZ6TGtwdmtUUUorYmN0RVFlUzB1bUM3VFlycVlybVgvVCtoUEt2MmxWWmtrTWh0YUN5R1NsT1RrdG1jdlFLbWZrdERBaiIKICAgICJwNlZtNUxTSXRWcVZqSnhwdVFLY1I3aEo2aTR2bkF4RktIMlFWVXJOQ2syZ2d0TEdTUnRnR3AwMEdlZXVkZmJEdi9wUDdHZFE5R2pYS1NEYXJzQzBzL3pZRlhFNVN6Umk2U2FtIgogICAgIndady8vdUgzLzFFN0t5RnhlN0R2UzRzbE53dWVkbTZ4UXJxVVUrdGM1TytKdzJBYWQyQ0V4SXNUL09CZnJPYnhENy83SHhieHJuRUlySTBuMEVrNG1ZZ2l5bFdqRENxcHNWek0iCiAgICAiTkxSb0F2aFh6RUE4dDg3RW9JOTJ4ZmNpZFBManFvS0drb3lJRHVGd2dKVDVSQlFJZUp4Vm1zb1VGTlJBbWdYazJmTzlOY3hsQVNKVm9wNjk0b0I0STBhTUIzdzl1ZEdkMHRsQSIKICAgICJMZ1dvNFEwbTdabkMvZzUwWi9Ec0hBQ010enRDRUlmeDVnWk9RbHk3VDFNa2QvS015SURCM0puSDRubWFMeG5US1R0dWR4Snhiais2aU02MForaDFkTU51d2xYRUpzNGEvbERPIgogICAgIk45cFRGb0lVUXR0TTJZbnRSN3NMOVRoazFGV2VEZ0ZBU1hCM2tYcW1sQlFUUHhmdWcyQUNaeFY2aU1NRVBQcFFSL1BLK0M5QzFkVnRvZkI3N0MxNXNvS3owVHZ1OVRBejhweWIiCiAgICAidDQ5aWpzVVU1TWNvOVBFVWdDVE4weW41emdYbW5EMkhZUzIwbUpMZ0R2UmF0OW1GdmpIMnVzU1BUQldScElrNTBLY21UVEJJV2pTaFQ3b211cUljUWs5TkVGSjN1REFvU2g4cSIKICAgICJ6TkhGa1o0OVNvbE4zUytlaU0rT0Q4d29PMzNtUTdRUXpZTEZZMEJwZ0RIZXdqKzF2bG53UUVUUzZDQmRwTUxqYk5nVkhPQXpIYTJydEVsa205YzRHQkRpMy8yK1VUTkhObnRNIgogICAgIkI4WTYrenk0aUJjbld1cXEyWUFpcVVwODNiSWtkYVJEY1Q1YlRUSm80SmVKejFrcmJpdDRLUjdBRXY3Yi82eFNTMjNITHlPK3pqckdMMTY0aWtFTVZIcE9uMERYZi9XL21uWU4iCiAgICAiSjk3S2hTQXZ5TWVCSjJ3WFZKWlZqS3RBaFUwZEQyc0FDYUlxTU9oZi8wUERRYjhHSlM0YkI3K3dWcUQwVFZWZ0hYL2ZzTXRYZkpIMUNKKzVBMzFHU3AraW5QanJYemJzOWtVWSIKICAgICI4YXhmL0VJNTM0SG94RVM2NHdYbmJvZkZQdWVMRGx1amlzZ0ZmeTBNVzJvR2EvcjMvMXMvdUE0Qnk4ejNXbExmSjM0WTU5T2liNUowbDBjdVBUbjc0ZS8raTVFN3E0eTZpUGtpIgogICAgIkJFd01MVCtieEhZS2Y1RTF4Y2VtVElMQ0dGUnFOUnVlL1FMcENURGdZUzFnbGhJeXl4UXlSY21rQktmTllLak1ET01kWlBmMDhXeExtTTNQYTJFbXpQMnlLcURLdWZoNlY4Q1YiCiAgICAic0s0eEFPY2ZBWUR6SElDNHZwOFRESTEwZGw3bFdoV2lqczVFNit4bEpsbmhLVFJ5Z3hLRnhwcXE0TnIvNTc5cGZpQnZOZE56cEE5MFV5OXFOazJzcjA3emozLzREMy83a2VmNSIKICAgICJpbCtCc0FSRWZ4SkdWMDdrZ203TDRrWXovaEwyb2Q5VHAvekR2L25QL2Q1SG52TXZpZWcya2dXQVBnczBoQSsvSW9HMFRPdFhpUW56NlJIeWpYK1hyVVlvWTFwNW5leVUxQTFvIgogICAgIm0xT2VqV21oeXhZRlVUVDlnTDZMV2kwSEZSY1Vhdmk4ZHZ3Vnh5OUhaU0l1RnFpWGtqNFlGSitHVjRFZk9pNERoU2NtamFMUnRqLzFLMXYrOS8rSG5UdHIzbWpYVFlUVVdibGUiCiAgICAiU0NPZzFXZ1JjWnlhdENJQUhPaHhXamNlUjk0aXlmdHNUVmFCVUJKYnFwM1hRa2tHMURCdm5GaGwwOWJhSVhQdTdIVjR5VTZaWmFtK2d4djJydkE4NW5FTUE1d25ZZVJNdVQzbCIKICAgICJ5VmNKbjdlc3ErOWYrbytobHRWbXYvMHQ5c0xlczdHVGdDYlpRcU5jd1lDV2o0a0xQV1V1Nk4xelVHYXhyMmMreDQ5ZjNuemx0bkQ5N2VwVXlYVUt6ZDY4N2FBWEJUNTEreUNGIgogICAgIlNJbnpGUFJFUCtZZEZnazU2RlQxQVdFWHdtN3dDN25hc2s4Z0JTRDNXeDdPTytMSktncU1rL1RhSjhyU3NoNG04d1RrWDQyaDBadXcxajB2Zms1NUViQUtBQ3htajFpdnplTHEiCiAgICAiaFBFSGkxODR5Y3llK0dFWXRWUm5VcnF1dVZJTmlObCtUMU5YcmdrREVLMWpDMzVEMWZ0UUZlWUFSTzhMaHFmMEdHRFR6aDhaODR5WGdSYVA5VXVXSTU0RC9nVlQ2UEwwbEFVciIKICAgICIzOGV4Y0tpNGJjTitvUldudGZ2bTgwZG4xczdiM1drbjc3YzExblZhNk5qNi9DZTRqckdONzFSNkVycjhjZExxNGZTdEU2dTYvdmZOVm9PVUFFU3c2S1oyVGExV2JLTUJRcUE5IgogICAgImpja0VURzFwcUJBUGlrdVU2NE5TR3lBeWJ6V2NEeGxSdm8zOFZ0eGhycStiRTJMQkN2RjYxMWw0dS9UV3FsM1I3SXZscVVVQnAyT0F6cmV2dm5vU3poZEFVZ0xBMGNJeU5jaUMiCiAgICAiMklxRHJkaEQ2UGR6MXovdGZ5NWVCNlh2cmdnTzdGcGlVUW9NMkhIV0pRQmxSWVJxaHBFbDRjbUd4Kytta2RPNlJvUmZOUU16VVFjRTg3SXhpS21KR2NUTEgyVjFwYSs3RDFnWCIKICAgICJmZ0NKaUhDTEw0WElsM1JwengvLzhpLytISmQzOWYyTGliT09yUTU3OWV4SlZpUXNsVkQ0aTJMcEV0REg0N0Z5MURLSWZvY3NySFVKU0lzVVZYQVVPZWsvTy8vbWEzdUI3MEZyIgogICAgIitlSFk4VlcrY2luWUNSSUx3VmxjbGJHa0JCckxEVHY2WFF4TUdjZGY1K09YUm92VDBUcGlRakhSS1c5eTAxcTMyeFZPcHVGbENDbUFoVmdvZ2JBRERFckR2UVQ4OHJvRVJVUGQiCiAgICAiSlZUT2EvN0NWRFZiSnJBVVowMjBLZ1VLVHN1T3d6a3ZDQWFUNG5QYmM0RVdBMWZIRDU5L0RnVjBka1VSZmp4QlltbUNheEpPcHo1UEI5V2VGSTg0TXN4aUFzTDFWMmlNQkFwUSIKICAgICJMcmpUM0JpK3RVUi91RHgyZG9vOFZRQmg0WHRBZWoyNlFBUE1IWVFoS2w4RjhjeWJKSzEzSUhFZDAxQWRNaWNmeXpGU0Q4SnhScTZrSzRFSzhBTmc5U3JDYi9Dbkk2S2E4UnQ5IgogICAgIjZOQUxBUEVyL3Ezd25YU21OQk9mQjlOa3hzN1lzSmRPV2hhZFlsRzFwY0JxaVd4WXZ4a1hXYXppMlN2Q1FoTmJTekZVZnJMRlJhM0NOa1dGYmJyWGl0U05pdFNOYXA5b2w1NzIiCiAgICAiL3lleEIrbGtDdHZRWmtvaGJvUnBIK1RwbEMyYWI0WGd2RnBPazU1LytsdmRoZXZDTGx5emV3RHRwUjdPb24wS1pSTXZFcld5NVIrMldhbmtsQjJhbHA1U0pxcXYzV2M0NXRHciIKICAgICIzTWZWcXMrb1hLQXZUcHkwNWpHSVNuTWpoUWw5dDZoRUlDdTZPZWMrSHdOdGIxa2loc295ckJuYXRyRURFTTNtZ0NzdGcxeGRHbUFNMGxUQ3BRN1Fza0J6MC9YdTJxUmlmazBIIgogICAgImdWbGlGcnBxR05QelJIanpvQ0tzVlZNcEhSb2pER3huc1FCNFBxRUVocTVtWk9CbHI3MDVEMWRKV1JzVWdwd3RjblYvRGRKRm14WFd6ZDRqakpIQkR2Wjd2WFlqVWFMd3ZsTzkiCiAgICAiTElHdXRGUFVwTWhuSmw3N05MNlVSY0tIQllXVW8wZVVrVThKaWlpdmppZ2luNUdHT2FMZkxPMEpYV2pZKzlPOENIMUdtbFprOGsvblJPWi9hRWUraTdTUS9CaFkrSW92MGpLMCIKICAgICJyN2QxYWlTL3hQdENVdWZVNk0xYWJGcERBNUo0bmdOZlQvUmlENm5Udnd4OW8xeWNxWThnMGdESlhjT0pCWHFGczBWYlM5c21nd29NSkRXdXRicWppbTZ1UDMvNHZzTWJtRVdyIgogICAgIjNXd3B5dkNhcVpQT2J3dGZBYlF0cnVJTDZPMlk5ZXlqYWl2YUJESkR0Wlh6MGxvVG0rbGhjK3VQZi9qOXYwWnRFZ3BCbWJaSHN1emZraktMOWl2ZFFhMlJCck5ONk9SZ2JOZmIiCiAgICAiTlJSSUlvek9YejU3OXZRY0RSWTkrd0F2OEdJQ1BMckphK090aWJjZEZpKytjcStoUXYra1podkllcXZkQ0FIVDlEQyt3bERhVXpucUcrcjZyUUdpd2lCY0FXbXBLWUczajVEcyIKICAgICIvL1BmSUNETFQwSFpoZUtHNUR5QWNiNlprRmxGaTlPcERRZndtWXc4T1RQcW05Ui9CSEJRdG5yUVI0eDBDVUd2WmcvZ3FCVDc2a0pmR2l4SXB4Q2d6QWdUREI0K1BLbXpOd1FhIgogICAgIm80SkpML01BU3YxbUVNSk5GREt4RVVnZTRqYlNhUkp2aTJ0cnl3R3JVeE5tTWsvUDNGRENvRzdlZUc5TkJ6YU94bWo2SzlvZmlPVHBUbmhHTXd6UEpDTHJKNE1rTjhmbmxvSDQiCiAgICAiTFJCRkZqWWR3M2I2UWVGMlFuYXdNajhKcFJ6aUxua3VFMmRCb0diT0ZKMitCdkVVYzBBU0UwZW5qZTI0YnN0YUxTeERSYUFaanhNZ0VoZEFwVnBXd1g4RnhNTWlXT2xhbG1SeCIKICAgICJIU3NuVytzTG5qZ3RvMXoxd29rdVl4T2tsaGlYMEJhQkNhK291Z255MFRQZkxFYnRZTWprRzN3ZGQ5Yzd0WFlBb1JHcGQ2eTNPeVpwR25wRFRlQ1piOGZqS1BUOXI0SWsvS1hIIgogICAgInJ3b0dnTW96VUFCb2w0QmE0OTFQSHFPZDRZTFBuTFdIb2ZKV1BBL0RaR1lKamJSV0pjZWZsZGRVekN5QjJXU3ZiVmtVZjNvdTZsck1BeUlBRTVzNkFCOHlUTnhyNFRGRGxUTWoiCiAgICAiWUcrZzVHM2JmQzcxTWdMK1pGM2J4Vkh0OUozenNGTUJ2Mkl2OE5rTFdkWjZaL2JIb0NQbnVEQ25nc0d5WTJ5VktsMkZaa1dEWmsxRG9ac1YyMkVKTlNzR0g5YVBmUlZHbDZWTyIKICAgICJST3oyRit6Tk93YjBxUG9JR0tuM1BZOEJVZnFqM2pYOEEreEIvd3dVaUhmSWY3ZmdVOENldDhERjNyelZEcTBqQTF1dyt3eWpFUGQwT1l1UkdpbnNWbEk3akNKeGtjMys4TmYvIgogICAgIlFQTEtEMy8xZnpYNkFvbW5CYW9rTERBdEswUTZJL21uamtLQkJHdHFKUjBsS0Q4YUdwWW5uRHBXUUM0WVNBbnJYNllTMWw5YVpnVktMQlFVZElxeFJtVG93ZjVrRDJBZlVURkIiCiAgICAiMVlXZUdmakRHRnNnb0ZvSjIwVjc0Z09LUTlhKzBJZk9yUlRTVFFJRWFodzJoWExiRkltTEFpMk1BWExOZlVzdkFhQkNJbHZnZFp2NkJsV3loTXFLQWxGMEZDVTYwZlJwcFdvbSIKICAgICJNTHNrOWlNZ3NMV0xkckJXZmlES0ZBaUJMaXVXQzdHVmhZRjFKdjNZUU5FTXNFU2hVb2FvcVVLbFFucE96TzFsckZwTkJ6a1IwbElZdzdZaDlvekxIV2t5RUpRV2orZWQ1aVFpIgogICAgIkJOdFNEQnFmbEVzRm44KzBRTTJlaThYRjNOQU9BMXowMDhqWXNYalh6Mmx4ajNNYWZwNTZXU3oyVC8vSVNoVFdESXNnSUNaQ0VqbDAxNjdaRWhHcFdORVRFRGtlb21pT0E0Y1QiCiAgICAiOHVrVTVWRWpjWDhJY25hUTRSVjhmRlNXOXpOa0M0SzM1QytDVmEwV05ObmpmQlI4S21CQS9xTm1BdmxHZ2FtV3krdlJYb2d6Qm82ZHFtWG5pZERMU2tmcVhwSDRrd29yVTVBUSIKICAgICJQUlhGR3h6cDFZV2FlRklZUElQVnUwWUpwMGpYNjVkYUpkam90QzdMN1Jzbm5TdElUWFl1MWZOcUVWWW9jSThVcFNqWHFZSzJUaEdHWTZrc3YxOXN0TUVXSmhZTnlzRXp6UG1HIgogICAgIjNKVUhJR1JiR08rMFdvQmN4b0hGcmp6MXhTcUdWb2lmTGpTUWU5V3dGVTUybTFGRXlPbzIwNHFpTUxLSy9uZ1RFaFd4TzFNZDI2a3lkazRLSk5CSnowODFNVUFvSndDcG5xTnIiCiAgICAiTEY3NVdsT3RoaEhLT1NsNzAzQTlWNDZYNENsVFZxUWRCeU5haFg3NFdWV1VxM1k5OXIzeFpXTlFVY3lIc015YWtCYndMejBubW5NaG5NaEZJdkpPcjBjMzFwdXI3cjdzbUJRRyIKICAgICJxcWhXeW5kaFg4WEk3WFpUSU9YNGcwREpBWUJGWGZMYWFRZWhXT24yclhlaVNsakl2dklGNi9hQkJ0ZlJtWXgvYlVGeDFCVUljYjRwZ1BJWXAzdnk0NG5RY3F2OW90amV0TnNzIgogICAgIlVpcWxnY1RVNzdPaHFYZUN1NGlIdlQzZ2k2d2tSYW1OVEtOc2twSE9pOXRhWlNpdXVHMnlsdW04eWhxVGdnNDAwbEo5RjVURUJDcW51VG5mWUFiTExlOVFTOXJjeGZGNWh6M1kiCiAgICAiYUJZQUVOaUxDSkVtcTNPU2YwUUsxN09NSi81ZHFhYmFKY3FlR0xHcGEyNHlBK29BSmx3R1ZYQlJrQ21BSysxTHBjSDBXbVorcVhxZUNuWWRmdms2YkFIVW9kdGZtMEFka1FRTyIKICAgICJJaG82WDc3RWk1WEFHcDVRbTFkOG5CZ3RsYVNKa3JGNTdnV3Rma2QrZHE0eE9VQTZLRkN1aU5URU5taXJrVkF4MisydGxHUkRSSlpKUk5NSlp6alhCOHc5YWFvQnQwU0RmaytFIgogICAgIjBEWFhoQnMwZkYvakdCZmJvQkZZUkpJTU43d0tTcWVIYitTdEpna3pkOTNwbFMrSk90eE9rY2NRY0FuMEJ4Vy9mRVlVWVpTMlh1Zk4yWHQ5RDZ0RnFiMXBWMnRkamZuRi9RQkEiCiAgICAiSk1taUhvVDRCRUNJZjlxMzdnVG9MUWlQQzUzS2RmS1pvVC9qcHRiUGFGTnIvVlRlYXlpRnlISzljQUx1NjF6VmRHTkhrbHh4ZTZmZFlUTHp0U3dWdDFxdytGVjRsVVlnazRGZCIKICAgICJ2eTNacnBhczd3WXF0TVNsUFV1SnZyaHJvd3ZsRU5Wc0x3QW8vT3oxaTU5cklyYXBYalpGUGRwTkltZGFEV3A0S3I4K2g2Y1UzYUNad1NTTVdDdU43QUt0ejFQRUh5aDUrTERPIgogICAgIlhiZU10byttRUZFczVaQUt6RDVvR2F1bWZNckxmZUJldThZYVV2WitLTFBDeXdEb0pZSmE1VGtFbGlnczZ3enBlQ1JJbWNmRTY2am1NZUVwRGdsL2JNZFB4RGJUTnd3R0ZLVEEiCiAgICAiOHAzdmJ5eXpBU3YxNmdsRFY1dGFDeE5XK1lsNWl2T05XOFhta2dNQUUveVZaQjhvVVppM1A5amM1ekpRdGhyaERJVTZrNkJYZEdiVURPczBHTlpSaG5Vc0tqUU1XM1NHNkFlZSIKICAgICJsNEoxbGdHQnExVGsxT0NINnpaQVNWY0pOTUpzbkliWnVLNXBKYXA5Mk11dHcrWVRWbHdIbkFHRVg3a1EwSzFhT0s4V3VhNEJDQVhOMUhPdlRUUWxuVTVEYmFmZ1lvYys5WjVmIgogICAgIndVT01CSU9JcTQyKzI5YlNaRGhGQWx2ZTZxaVJpVElsOE1XbTJKZU9GeUJiT2I4SnhpWVJkYm5FNEVIWm9lclFYYW9lWFdIT3JmSHBRbmVnN2k2WFJvOXU1VkhCb1NzeUhUYngiCiAgICAiM2I2dllhSml0UnBFMEhFbHNVME51VktJWktuMWtOb1EvcWY4bzAwbVFRQk9EUzRRSmExNDExSmJMcHJ6TnJLZXJCODFmakd3NnNKVjROeHBHQkFzSnZja2J1QkdqWUFmQWpMUyIKICAgICJaV1F0OERkRUY0alhBaWhlRW8xdEhycUprOGRwRXFibitENVl4VFlsZXRMRlpFaFJUZjlNR3hNcjViYXErRzhBd1JqdEhUVXdVS1pXc2sxVVo2YzhOZ3VSN3l2YXNrdzhVV2RmIgogICAgInlMWkxvMnd2Tnh0dThxVXE3ZVV5R3JaVUpmRDgvVEk2QVR5Ny9TMUY0UHcyZUxzakgrYXl1THhWTHA4MEVNRVJJbmdwWEx0MzJWQWZCRW56M3JUSW1DMmxPYTRXMXJnZHVqWlkiCiAgICAic1FrbGRkT3MxdGdHYjBVNmllM013ZWtZN2VKNjVQV05mRXYxRnNwNVEwVEhQdFFjanRuaXQyOWJpcXhzYk5tVkVhQXQ4VUVhWUVXb1pjYkJTdEZ6ZXN1anZJbitZYXl5WlJ0UyIKICAgICIwYnBWZmQ1bC9kNm0wRmpkaEovNmQ1aHN3ZEpEQVZpbENJWFU1L1IxU0dtc1dNcU5hOTBwRXkvaTZXM3dncXU5Z1hraHpyTWM2S2diUFg1TTE1Rmx4cFRYSGtYQzQ4VTJXU0pRIgogICAgIlFITmQrZnpuejU2OUZCSEVIZFlmZGRnUS91NzMzcDZZcm9tR0MwcWlnRGNuZEhBclRFWXY2V1VUUkRpT2ZlNUVYNkgwdG5iOHdxT1R5anIwWnVSV210S2hyU0Vsb1ZhOEtUVlMiCiAgICAiL0hHWUNjTFNUNXVXbTkwWmFXOGtTT2tJdDhXL3dxNjFzcy9wMmFVOVM4K3VkcnFpeXB1MDVkczBrdjFkWVFldFF2WU1ocG5xYXRFM2I2Y0xKczEyL1Nrb2tuWVFYclhRU0Z1WiIKICAgICJ4UU5NaGF1N2IyYllTbUplK24wc0lnZ0cxYWRJdE5uZWlXZ3ZiY3Jwdkx1RmVac2xZbXIwU01EUnFDN1d3dWxPRHFwTmtRMTZwYTlvYmlsRXN0TmFkc1YyZ0tpakpBRVFqKy9MIgogICAgIngxQ3YzK3YxYXNKNGFnOFV2djZ2bUExQWt3Z2cxdWdPSGRPZ3FjTS94MTRSdVZQQk5vd2RtbnNWN05HUldKSC9WUWJycWtUMkZyRTdXMGZmb2plUjhQWm5UdUQ2aGNpTE1qTVgiCiAgICAib1FGRVFsbzFCb2JtZzhod2pZODhpc3lmSnE1bHFhTVYvZmtmYmt4MHV0ZU1KMTM3SDI0ODlLSWtZV2t3dDQ1T2tLT040aXh0Y3VtZ3BITlBacTdJQWx4U1oxMWJLemJsTFU4KyIKICAgICJSc1F4SEl0TGZuTVJZcjRpNVVSa2xzTXFlNE1telgxcDNFNmNhQXA2R2F4NDk2dXZYMzc3K3Jldm4vMzY5ZU5Yeng3dkFpMkJZNTdXZ0QvVHIvTk1HR2J2bStnV014d1F6N1BPIgogICAgIk1iK2VSZnVPam1XYzdGTStjVlkrZWlBcW1HNk80ZUkyckV4MCtUaUt3cXRYbUdIWU11TlQ4NTUrRHJUV01oK0VSaDBCU1FMQUZBcSt2dXZVRm1xUEwrODZ4MWp0OFp4NlRNUE8iCiAgICAidCtvclV2dDZKZm9Tc2VoYmRUVlh1M3FSTDFSR1cyelo0MUx0OFJkV3FuUXVDeHFuTlBnVVZNNnNwTms0RTNXYzUwWThKK3NNcklUeUVEY2Y0Vms4ZGhiaTlKU25YRktaMzI5ayIKICAgICJyM1NKVjBUSW9yNmt1OU9icHpZcTZrbkdtOWwxTGhkSEp5ODZOdWFhVm0rYW9jMURXOWxOODNhaC9IMnJwRFBiM0xWMnRITkkwVzY3YTloTzZScTJVNzZHZlZnblVjR3hocWE0IgogICAgIlFkbnFNWTVTTEUwa1N4Z01ObDNnemlZRWxBMGg3SFhZT0xtdUM1RFowbCtydUdyMW50ckdUdHBJdWhpaDZzc0l6bUNVM0xTc2JoY0RaYlA0R3cvMEZyeFVPNUtoSi9QWVVuRjgiCiAgICAiRzc5cmxXWE5wOHFOL3RscWZtSHBLMllPVyszVHpRNWMwc0dyWHR2WTVLN1YrTjMwUzU5N3QwaGxBSTNLUzRjQ3d6MmY5SnJDbGlOUU13VzhkSTFFUHdwZnhuNlliSGFPVnNrbiIKICAgICJOU3dQSk1vTUl3WHpXNHdTekpVZ2dibWxyVlRXd3dvRURBNjBaUUpTY2FQRjFOdE5xZ1p6ZzQvUWlXNnhRa2M1MzQ3TzVleW9vZFJ2OHV3dFdkb1drWlNsSXpLM1NCOTBuRHVmIgogICAgIjM2WlpUNzRNUTU4N1Fkdkc5eCswNkVhSUNWV0x5eVpBTktqblJPMU5wd3FhbUU0VnZhQm1hNlRIVmdvWW9jU0FpQk5uWFRPQ3pEK3BpOVp4MWpiZWdjVHVaUzE5cGRKRXZIRVkiCiAgICAiWENTVVBvcnNBVmxpS2VTanFGb2JtQ2VOcG9UUC8rMS9ONHlZMGdvTCtnNHhyNytoWGhQckhJWGQ2UzBmYXh1dFBjZzNuQ25wYVNhYlR6R1psY0dubjhLcGVva3loVkNOWlFvNCIKICAgICJyQlFKTWFzYVhmL0pXbUVVZGxFSStLeUpta2dJVklvVGNOYW0wRkwvVnRqaitwdVJ4L1gxdU9QNitzclp0cWZ5bzc1YU1jQnRoOTVWazc0b1piL3c2cGg5NVpVMGd6MzQzOUsrIgogICAgImQwV3E1VS93MVczcXkxY0c5dDdXNzE3UnZac25lekZOZjhDRzYvN0EyczNMNWdlczMyY2ovSzg3S2o1NHNZZXZyaG5OWURHN1orSVZManRha0RRK0NIcWtWNFgyazJaSTVmb2IiCiAgICAiU1NPMk1hY2xqQnFLb3NXN2VTWkpWQVExNnBNVVBQWkJmOEk4QlZiRHFNVm8rL0NReUJnZGd1YzV2ZnhreUJ4eTEwQVJLZlJFaGtBUktjalVVQ0NTRnFpYjB2RnF3ZUlxbHhHTiIKICAgICJOckV2OERnV1VpanpKU0MrQjRqckdmNklWTW83aG1DeTkxc2d4N2w0ZjR2ZzZpQXpyQzQ2Nld0K2dMaDJHTDNWNW9YZTUwWFhmek8yaEtwMitnb2QzUTBROFdoamxDMTJPdHVlIgogICAgIjg4L0tGQk5mU3pNejlUNm82WDQyMFBZK1VIZ3dnVXMzaStJWm5nME1VVjhBNWJxREVUY0puTldIdEt2QnREQ1NaYTZxU011ckMzM1Y4cXJpWjM2anFMdDB3MHNRYVpzeUNpREsiCiAgICAiWlplOTZ2TVMzU0xPV1lsOXBaYzJtV3FXZ1pMaVB5a1JxZDk3eGlQT2JvQldHVHJSTGQxRWhiYTdyNHN4N051REFGdVZJVUN4OEo4aWpMMjRzYlVjb1JMY1NYWVViQTJFT1RPbiIKICAgICJOTUk4V202RFlNL2RCNHk0ZlRjSnU1UzZDTk1qenJnQXNtS215L3R0SWpHWUZpbU45RW5rL3ptL0VVWk1URCtUZmFGc2t2Q3RYZXZzVEExSW1ZK0FySkpra2xPTFV1NE5uSUtpIgogICAgIkNZeXp1aGZoREtLQ0RZMnlYK0tTTjAvR0kzODlYWjM3Q2xDallJVHFVQXlMT1pvZUVQSnJDaHF1SHdSMnF2QXlBZm5PTmllbTNSS1hVRFRiUllQSXhQWUNEV1BLVjJ1YWtQbU8iCiAgICAiSXY0b01jNk50QWtkSFNDSWJpTytsWEo4bXJpdzVML0ZkOTZaU0cweEF5a1p1SldrR1RXeEVPT04zRnNiTTF3WXNKWUVGSXk2eXpvWElpV2R2Slh5bGQxakxWTkRmR3VnVlZkNyIKICAgICJvOFpXcUZwaUg4dTZxcmVOa2FtRW54WnlVOVlNS0V6eDRsV0Uyb3kxZGFpY28wQVJtUy9NZ2Y2MDYwQzZtOGtLdFZHaXB2T0JyOUtra0hoeitvMWlxdUcySXZKYTRtVDVOK0tGIgogICAgIlZpN2RYM1lXTEFtTGlleXk5TVB3Vkg2eTZuSmVGSkpNVndaTXJUS1lBTjZpRnpENjRacTdnanBaSXVGMFJ4b3pUR09ZMHdSbHdyZWk2TmU5cmRMMGNzb2Q5dENJQWpzZjVVV1YiCiAgICAidFNOdTljSks4ZWFYblMzMW9peVh2NENHS2YydWVQeWFZc2V5U0VNcU8rZkxhdlJmaHF1Rmc2ZUxkRWVWY1NsOS9OclhWK1RrMjh3cHhmeXdyNGNQc3pucFk4NzB1TktVb3U5diIKICAgICIxTzNqeTlzSjZ2R2xvcjljY3Q4NmFZRHlKUTNsc2hIUm1YRE0xRkY4TTBVSDJhRXpubUY2dXlEczRtc2RPRjZ2c1VIT0NKUmM3VWFaQmoxclRyS0t5WXF4MThNMHEyd0t5MGR6IgogICAgIlVUTEQxMlJqbXNGbm1HcWxaZUVEMTJxZjFDYnpBYkVzdkFUaExySy9pOUhzWkJUbXFqTjE2MmFLdUlKQ1hvWXJEY1JPanRKVUdzQWpzc3VJbCszb2JzWG13aVhmcE54dFM4U0kiCiAgICAiY24wZHlnUTM4VWFpaFQ4V0VLNVVoWnVneFpHUSs0ZmYvUjI5SFNRZTQ5bDhpSzZpLzVybDBYRzl5WVNqUk15dXdzaU5DL1NsUmdqWVBsQ3h3cGhvVWNnbDVPeVdwWW5sVElyWSIKICAgICJFL0lNK2RwbXJXQmF5VWxUcTU5SXB6R2xERWIxSkk2ZEtSYzJ1UlJkYTFkNEM4emFldU1seXhKSmp4b3hyTmVnS2RCN1pXQk8wUnJVQURZSEZnOThDRk9pSWk4Qm5NaVNKeUhqIgogICAgIngxQkhoODFESW1EMWJLVmVuTWZGVGNNNzVpNVpwcGZZVTFIdjFreWkrSDRFWGFpR1FUN1UzVFJZYWhlMVpTQmNIbk5EbHhscmI3azNCa0lUUUd3R3hrYUJ1ZjR5QmptSitFVWEiCiAgICAieU44UWZHbUNsZytKRTNUZElJMmNnU21aTHFObXljRlAyYkF0NTI2TXVTbENCa05zRHFvUk1xYUlLQ1FqOWNGUWdtRWFGajdkWHJaUUFrMndlMHRYeTBoN0xzSnJKRDNEczVmdyIKICAgICJGWGtCM1Z2eklpSS9ReVA1MlJGdmRSUnZ1RjljcFM4M1hNaE9MRVp2TDV1RlBwRC8welNYVy9hMHB0dkNTeE9uNGFWMTltMkFsNEx6bDBtYWlKVTVGbXVxQlp2aUVma0pEdGEyIgogICAgInc0QklWNk0wSzVUWTV3cXFham9EaUVqY05YaUhTVVFUYndVVFdzUmY0Q3U3NEhpWXBZZzVUMmFoQ3hMY3kyL09YOWVrUThiWDNQSW9QZ1pVdHFUQzNuME51Mk5CVTd3VzVvM0oiCiAgICAieTdlTE1oY0lnZWFPRUpMSDZqdXUzbVc3ZUl5cmY5ODI2TWs2MFRJWCs0UzhkN0tOWUtmRVdDZmhKUVplMWhzUjhqZFJ5Z2IxUmdSeGkwTjVhV1ZjZVdsbGgyV3ZWMnQ4eDBNZyIKICAgICJuZm5OTVhkakNTVmkxMjVvREtsVHUydk9TWTdhQnNXcVNmc0NkY0JlZmhXaG9wdVJoNXBKbTdETmtKMXdVOUloUmFUWitQS2gzREtUbDc5dnA5OGU3YWJ2V0FWNkJZZm43RFA0IgogICAgIk1Fdm0vdGxuL3crVHNrOFBQdHdBQUE9PSIKKSkuZGVjb2RlKCJ1dGYtOCIpCgpST1VURVMgPSAnJycjIFdaRklYIHIyNWMyNjogb25saW5lIG11c2ljIHBhZ2UgKyBwcm94aWVzCkBhcHAuZ2V0KCIvbXVzaWMiLCByZXNwb25zZV9jbGFzcz1IVE1MUmVzcG9uc2UpCmFzeW5jIGRlZiBtdXNpY19wYWdlKHJlcXVlc3Q6IFJlcXVlc3QpOgogICAgcmVzcG9uc2UgPSB0ZW1wbGF0ZXMuVGVtcGxhdGVSZXNwb25zZShyZXF1ZXN0LCAibXVzaWMuaHRtbCIpCiAgICByZXNwb25zZS5oZWFkZXJzWyJDYWNoZS1Db250cm9sIl0gPSAibm8tY2FjaGUsIG5vLXN0b3JlLCBtdXN0LXJldmFsaWRhdGUiCiAgICByZXR1cm4gcmVzcG9uc2UKCgpAYXBwLmdldCgiL2FwaS9tdXNpYy9zZWFyY2giKQphc3luYyBkZWYgbXVzaWNfc2VhcmNoKHJlcXVlc3Q6IFJlcXVlc3QpOgogICAgcGFyYW1zID0ge30KICAgIGlmIHJlcXVlc3QucXVlcnlfcGFyYW1zLmdldCgicSIpOgogICAgICAgIHBhcmFtc1sicSJdID0gcmVxdWVzdC5xdWVyeV9wYXJhbXMuZ2V0KCJxIikKICAgIGlmIHJlcXVlc3QucXVlcnlfcGFyYW1zLmdldCgiYXV0aCIpOgogICAgICAgIHBhcmFtc1siYXV0aCJdID0gcmVxdWVzdC5xdWVyeV9wYXJhbXMuZ2V0KCJhdXRoIikKICAgIHRyeToKICAgICAgICBhc3luYyB3aXRoIGh0dHBfc2Vzc2lvbi5nZXQoCiAgICAgICAgICAgIGYie1NUUkVBTV9CQVNFfS9fbXVzaWNzZWFyY2giLCBwYXJhbXM9cGFyYW1zCiAgICAgICAgKSBhcyB1cHN0cmVhbToKICAgICAgICAgICAgYm9keSA9IGF3YWl0IHVwc3RyZWFtLnJlYWQoKQogICAgICAgICAgICBzdGF0dXMgPSB1cHN0cmVhbS5zdGF0dXMKICAgIGV4Y2VwdCBDbGllbnRFcnJvciBhcyBlOgogICAgICAgIHJhaXNlIF9zdHJlYW1fb2ZmbGluZSgpIGZyb20gZQogICAgcmV0dXJuIFJlc3BvbnNlKAogICAgICAgIGNvbnRlbnQ9Ym9keSwKICAgICAgICBzdGF0dXNfY29kZT1zdGF0dXMsCiAgICAgICAgbWVkaWFfdHlwZT0iYXBwbGljYXRpb24vanNvbiIsCiAgICAgICAgaGVhZGVycz17IkNhY2hlLUNvbnRyb2wiOiAibm8tc3RvcmUiLCAiUmVmZXJyZXItUG9saWN5IjogIm5vLXJlZmVycmVyIn0sCiAgICApCgoKQGFwcC5hcGlfcm91dGUoIi9hcGkvbXVzaWMvc3RyZWFtIiwgbWV0aG9kcz1bIkdFVCIsICJIRUFEIl0pCmFzeW5jIGRlZiBtdXNpY19zdHJlYW0ocmVxdWVzdDogUmVxdWVzdCk6CiAgICBwYXJhbXMgPSB7InEiOiByZXF1ZXN0LnF1ZXJ5X3BhcmFtcy5nZXQoInEiKSBvciAiIn0KICAgIGZvciBrIGluICgiYXV0aCIsICJkbCIsICJuYW1lIik6CiAgICAgICAgaWYgcmVxdWVzdC5xdWVyeV9wYXJhbXMuZ2V0KGspOgogICAgICAgICAgICBwYXJhbXNba10gPSByZXF1ZXN0LnF1ZXJ5X3BhcmFtcy5nZXQoaykKICAgIGlmIG5vdCBwYXJhbXNbInEiXToKICAgICAgICByYWlzZSBIVFRQRXhjZXB0aW9uKHN0YXR1c19jb2RlPTQwMCwgZGV0YWlsPSJtaXNzaW5nIHEiKQogICAgaGVhZGVycyA9IHt9CiAgICBybmcgPSByZXF1ZXN0LmhlYWRlcnMuZ2V0KCJyYW5nZSIpCiAgICBpZiBybmc6CiAgICAgICAgaGVhZGVyc1siUmFuZ2UiXSA9IHJuZwogICAgdHJ5OgogICAgICAgIHVwc3RyZWFtID0gYXdhaXQgaHR0cF9zZXNzaW9uLmdldCgKICAgICAgICAgICAgZiJ7U1RSRUFNX0JBU0V9L19tdXNpY3N0cmVhbSIsCiAgICAgICAgICAgIHBhcmFtcz1wYXJhbXMsCiAgICAgICAgICAgIGhlYWRlcnM9aGVhZGVycywKICAgICAgICAgICAgYWxsb3dfcmVkaXJlY3RzPUZhbHNlLAogICAgICAgICkKICAgIGV4Y2VwdCBDbGllbnRFcnJvciBhcyBlOgogICAgICAgIHJhaXNlIF9zdHJlYW1fb2ZmbGluZSgpIGZyb20gZQogICAgb3V0ID0ge2s6IHYgZm9yIGssIHYgaW4gdXBzdHJlYW0uaGVhZGVycy5pdGVtcygpIGlmIGsubG93ZXIoKSBub3QgaW4gX0hPUH0KICAgIG91dC5zZXRkZWZhdWx0KCJBY2NlcHQtUmFuZ2VzIiwgImJ5dGVzIikKICAgIG91dFsiQ2FjaGUtQ29udHJvbCJdID0gIm5vLXN0b3JlIgogICAgb3V0WyJSZWZlcnJlci1Qb2xpY3kiXSA9ICJuby1yZWZlcnJlciIKCiAgICBpZiByZXF1ZXN0Lm1ldGhvZCA9PSAiSEVBRCIgb3IgdXBzdHJlYW0uc3RhdHVzIGluICgyMDQsIDMwNCwgNDE2KSBvciB1cHN0cmVhbS5zdGF0dXMgPj0gNDAwOgogICAgICAgIGJvZHkgPSBhd2FpdCB1cHN0cmVhbS5yZWFkKCkKICAgICAgICB1cHN0cmVhbS5yZWxlYXNlKCkKICAgICAgICByZXR1cm4gUmVzcG9uc2UoCiAgICAgICAgICAgIGNvbnRlbnQ9YiIiIGlmIHJlcXVlc3QubWV0aG9kID09ICJIRUFEIiBlbHNlIGJvZHksCiAgICAgICAgICAgIHN0YXR1c19jb2RlPXVwc3RyZWFtLnN0YXR1cywKICAgICAgICAgICAgaGVhZGVycz1vdXQsCiAgICAgICAgKQoKICAgIGFzeW5jIGRlZiBfbXB1bXAoKToKICAgICAgICB0cnk6CiAgICAgICAgICAgIGFzeW5jIGZvciBjaHVuayBpbiB1cHN0cmVhbS5jb250ZW50Lml0ZXJfY2h1bmtlZCgyNjIxNDQpOgogICAgICAgICAgICAgICAgeWllbGQgY2h1bmsKICAgICAgICBmaW5hbGx5OgogICAgICAgICAgICB1cHN0cmVhbS5yZWxlYXNlKCkKCiAgICByZXR1cm4gU3RyZWFtaW5nUmVzcG9uc2UoX21wdW1wKCksIHN0YXR1c19jb2RlPXVwc3RyZWFtLnN0YXR1cywgaGVhZGVycz1vdXQpCgoKQGFwcC5nZXQoIi9wbGF5bGlzdC97dG9rZW59IiwgcmVzcG9uc2VfY2xhc3M9SFRNTFJlc3BvbnNlKQphc3luYyBkZWYgcGxheWxpc3RfcGFnZSh0b2tlbjogc3RyLCByZXF1ZXN0OiBSZXF1ZXN0KTonJycKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMjYiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMjZfbXdzOiBhbHJlYWR5IGFwcGxpZWQiKQogICAgc3lzLmV4aXQoMCkKCnRwbF9kaXIgPSBvcy5wYXRoLmpvaW4ob3MucGF0aC5kaXJuYW1lKG9zLnBhdGguYWJzcGF0aChwYXRoKSksICJ0ZW1wbGF0ZXMiKQpvcy5tYWtlZGlycyh0cGxfZGlyLCBleGlzdF9vaz1UcnVlKQp3aXRoIG9wZW4ob3MucGF0aC5qb2luKHRwbF9kaXIsICJtdXNpYy5odG1sIiksICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoUEFHRSkKCm9sZCA9ICdAYXBwLmdldCgiL3BsYXlsaXN0L3t0b2tlbn0iLCByZXNwb25zZV9jbGFzcz1IVE1MUmVzcG9uc2UpXG5hc3luYyBkZWYgcGxheWxpc3RfcGFnZSh0b2tlbjogc3RyLCByZXF1ZXN0OiBSZXF1ZXN0KTonCmFzc2VydCBzcmMuY291bnQob2xkKSA9PSAxLCAicGxheWxpc3QgcGFnZSByb3V0ZSBhbmNob3IiCnNyYyA9IHNyYy5yZXBsYWNlKG9sZCwgUk9VVEVTLCAxKQoKd2l0aCBvcGVuKHBhdGgsICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoc3JjKQpwcmludCgicGF0Y2hfcjI1YzI2X213czogL211c2ljIHBhZ2UgKyBtdXNpYyBwcm94aWVzIGFkZGVkIikK"),
    ('patch_r25c27_pl.py',
     'web/templates/playlists.html',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMjdfcGwgLSBsaWJyYXJ5IHYyLjEuCgpyMjVjMjcgZGFzaGJvYXJkIGF1dGggcm91bmQ6IHRoZSBzdHJlYW0gcGFzc3dvcmQgaXMgbm93IGFza2VkCk9OQ0UgYW5kIHNoYXJlZCBldmVyeXdoZXJlLiBSb290IGNhdXNlcyBmaXhlZDogKDEpIHRoZSBwbGF5ZXIKYXV0aCBwcm9iZSBzZW50IG5vIHRva2VuLCBzbyBpdCByZXR1cm5lZCA0MDEgYW5kIHJlLXByb21wdGVkCm9uIGV2ZXJ5IHBsYXk7ICgyKSBhbGwgcGFnZXMga2VwdCB0aGUgdG9rZW4gaW4gc2Vzc2lvblN0b3JhZ2UsCndoaWNoIGRpZXMgd2l0aCB0aGUgdGFiLiBUaGUgdG9rZW4gbm93IGxpdmVzIGluIGxvY2FsU3RvcmFnZQp1bmRlciB3em1sX3N0cmVhbV9hdXRoICh0aGUgc2FtZSBrZXkgdGhlIGtpdCBnYXRlIHVzZXMpIGZvcgoyNGgsIHNoYXJlZCBhY3Jvc3MgSG9tZSAvIFBsYXlsaXN0cyAvIFBsYXlsaXN0IC8gT25saW5lIG11c2ljLgpTdXBlcnNlZGVzIHRoZSBwcmV2aW91cyB0ZW1wbGF0ZSB3cml0ZSBmb3IgdGhpcyBmaWxlLgoiIiIKaW1wb3J0IGJhc2U2NAppbXBvcnQgZ3ppcAppbXBvcnQgc3lzCgpQQUdFID0gZ3ppcC5kZWNvbXByZXNzKGJhc2U2NC5iNjRkZWNvZGUoCiAgICAiSDRzSUFDdzR1bW9DLzYwOHkzTGpScEwzL29veUhMSUFOd0dDVDFHVUtEL2tWdGdiM2U2TzZmYk9lQnlPaVNKUUpHRGhaYUFvU3QyakNQL0Fubll1ZTlyYlh2WUQ5cjZmNGkvWVQ5ak1Ba0FDaFFMSWxrZHFTZ0FxS3lzcjM1a285ZVVuMzd5K2Z2ZmpteGZFNDJGdzllenlFOU1rZi83cnpYZC9JZWx3NGd6UDVpVHdseWxOSDhqZDBCb1Frd1N4UTRPM1BFN3BtaEVlMzdLb1IxNUVuS1ZrVFRuckVaZG0zakttcVVzaWVrZE1FM0FpYWhMUWFMM1FXS1JkUFlNbmpMcFh6d2g4WFlhTVUrSjROTTBZWDJnL3ZMc3haMXAxS0tJaFcyaDNQdHNtY2NvMTRzU3dXQVNnVzkvbDNzSmxkNzdEVEhIVEkzN2tjNThHWmdZMHNzWEFzbnVrbkdtdWZMNXc0anVXS3RDbmJNWFNGSWIyNktQWTNEMXRUdUFlQzVucHhFRmNuZk9walYrakVwNzdQR0JYYndMNkVQZ1p6OGovL2crdzl0Vkw4eStYL1h3b0J3djg2SmFrTEZob1BtRFN4RVA4OG1EOWhlWlNUdWQrQ056dVozZnI1L2RoMERzWlhjTWxnY3NvVzV4Nm5DZnpmbis3M1ZyYmtSV242LzRRaUVEZ1U3SDVyK1A3eGFsTmJES3d4ZWYwWlBRQ01LVE00U1RuNFNrK0pSN3oxeDR2YmxLWU14eGFrMU95OG9OZ2NYb3lITjFNYnM1dWJrNzcrZlNFY28rNGk5TlhaSEJ1amNsNGFFM0pOUm5hbGswbUUzZ0FrOGRrT3NXcmMydEdwak5yQk9Pak14Zy9zMEdSeGlOclNLWWdJVEtld05WNEJrRFhaSHhtVGZLbmsxRStmNXJmRnZQaDF4VG5BNUlKWE1GS0V6S3o0V28wc1VZbHJWRWNzVk9TOFJTVVUxQSsrbWFHbEJlUHpITFRBMnV3ZXdZeVlBNU5GcWRwdkluYzJ1TmZZajhxbnhlYlIrYkNsZFlRWUFKY2pTTkFCWXFhaXcrbGs0RjRWcUFqbWJXTzQzWEFhT0pubGhPSFQ1aWZjY3A5UjB3bVRocG5XWno2YXorcUlKTFVwMnY5dnBObHd5OVdOUFNEaDhVckJBQjFwM3krQlQzNGNtemJGeFA0VE9GekJwK1piWDlXZ0w1anQzeVQ1bUFWa005Y1AwdEEyUmZabGlaN05SWWJ5L2hEd0RLUE1WNXVXank1MmtITjB6am01TVB1SHIrRWVZRXRvNjNOd2JHa3R4ZTFjZE5jcnVla01EcDVLTU9STVh4LzNSZ1o0dEFaZk4vSVF5anZPVW5YUzZxUGh6MXlQdW1CU3M5NnhCck9EQm1XM3dPYTNDamtvWEFEUTllajY2Ky9hUXhSQjRaeWhXelFSU01nK25RdmlOTWVNV21TQk16TUhqTE93aDdKZjVzYkh5NEIyZ1E0ZnlYaktjUUFxSEpCQVpvNjBnTllIcC90TGorWEpMS003ODNNZis5SHdQZGxuTG9zTmVGUm5ZQ1FwcUNSYzJMWEh5ZlVkY1U4VzduU1Q1N3Z1aXo2V1Zwd3R4YzBhdktKSDZJcnB4Rlg0bGpHN29OTU1IVnUxOEoyNStTT3BqcXFqQ1JKb1dYbEtMK1hSdEZ1ekZ6dlN4amttUVFWK3BHWis4ODV1bGozenJ0b0ljTVVyaHgwakxvWXFOYjRHMktIUG9CcHlUMlppSitVazlua2hKZ0QrNlNYYStOMDBDT0RFYXJqQkg1WWc2blJJendGUWhLYXduUXl0VStNWG0xSllYclNJdWNDKzNoYXJHR2ZrTFlGN0ptMHdHUnlZaWlaYnRGTlNpV3VKM0VHY1RnR0pWajU5OHlWZUpIckRkSzBBWDJmMkNjU3gvMEFzZ25RcjJDVEFzV0pMSkU0b1k3UFFScVdaUE1KT0dxWWFiSTdJRGpMVmFZTzhkNzBJNWVCM1pyRDlyMVlkQ0J0UndTTU9mRHRibHZIVjRxOE9SS3dGVHczQjBONWdNY0pQcmZsNTFWRmxjWG0rS2tUTUpUWUNMUUNQcjFDRTZranEwRk5TdmhGSTFDNFhCWXVXRGtma09FMEk0eG1ESGhoeGhzT09kTUsweWJBajN5UElJbnI0czJ3aFRkMksyOGFJMmsrb0dDQ1lNN0licmVlTHQ2QUNjQml3SnRQSjErZmYzTno4N0djR1pMUjhFbWMrZktXUGF4U1NBdXprc1VmcEYxSkQ4UkRwRzBWcCtFOHZ3d0F1MzUrdHdVckhONTVCaEVackE0cHlrUWkrL0VZRW9aUEpzR2NJUTNtZVkyR1kwaXd0aWxOcEZWQ2VtOFdDakt3WjJES0xaR0MwQTJQVzhLRkU5QXdBU0tHS1FiQUVWS0hsNFlTSEtJUjV6RnNDQ2gzZElRanp3bUw3dlNNcnBnSm1rQkJ0bEJvRkhBOWd1Nmx4YS9kZ3ErUTlpT0NBY1EvOE4vV0dXS1hqWjZqLzhuUVFTSHAxbkFvZzNCMno4MEs0emRKd2xJSGRLNGpKb0dWSzJMU3RyQXZ6TTFVOUhzREZmSDFTRlpFVnhYNmZKY2w4MmVDK1JQQmZIRmpxQ1ZwalpEbE5ySEdOZVpVMlpwdGxxbzByeVFwM0xSVFk1MDNlWjZ2dkpQN29HTmxIc2RCMXBaZnJBSW1hZWVhZ2pPeXBvMFZhZUN2SXhQY1FnZ3h4bUVZY2lTS0FaV0oxakFuK1BNQXZjTjJUakdhT2xCTDM4dUNERENFRFpvWlNHbHEwNGFsZFd6ejRIWnlQa3dhZkdpbVZ0blFVSVY2b0FqeWpTd09mTGNBeERUYjZNd0t6cy9QNVQzc1BJS2doUXlPNEpzZkpSdmVrUkUyTTRTU1lpbDVoV2lRbHdiTkdZZlRSOHh4d0NUT2tlcldESExuSnF2cFVNZXU1bk1RcU1POE9BQjZqemFvT3NMQWhQS1JIOHlZLzRoWXU3bnowVUlYVEZSeGVJb2NuZzA3V2R3bHhFMmFJWjFGQ3FsbTJEcjEzVGIzZ1dPUzJjQVRFNndxd2JDS25hSk5pT1ZkeWhKR3VZNEJ6OFIyUlE4dEYrS2tQaHhNa251SS9xdlVNQlFHQ0ZHNFRlR3A1V0M3cllXeVpSQTd0d2N0OTU5bXVJT3BMRUpzdWEyQ2VEc25lWW1uQ0lndWMrSzB5TVZhTGN5UFBLaFR1VFFkUTJsUmErekNLdFpIZVNiWEs4a1RPT3JQb1pMMXFCdHY5MDliODhNVmRka1BDVVMxSEE0bWM2OEZHUFlpMkY0RVdNZ3dNcU5EYW5NUDJTTm5hNHJFN0VmZEhDY3Q5dE9kTGV4M2lwbldZSVlWSUtES3l6NGJDQlQvcklseEtMa3R1Q0FGb3pRT0Zibmxya2lUSEduNzlnYk5TcTlDUm1zU3UxdG8wTGxRVTZ2VVNheG9FR2QvMk1yQml2RWo2VWlXTUllYlF0RWI5Rlp0RXEyTXBwWGV3R2ppc2pVVU5qWWQwTEdMRjlQQlpIVFQ0dGp6UFZoZ3R6TEQycW50d2tUOGNLMnUrdXJScWxyMU5VZmk1Uys0KzVVUG93SnZKL0dKZDd3TVJDRHN5bUFLK3hBS0w5b2IreC9XYk5LZWJ3NDdra21vRFdYdTdtUFZMSTlWay8wdk5aSW9WT1hveXN4ZWNOMkRUWXJ5UWdTeFpuSjVoS3ZkZzdBZzhKUE16MXJrd0orY3BjOWFzM1JSMzF1RFdUdERZcWkyUHp3NUZCMFoxL2JCYXR5YWJ6VDNJTmduc3VWT0xXdGhrREM5M09KZ1hkSW41cUFsTGJ0bGdjU0MwcWlHczRsTTc4SG8yK1ZYem0zaFZrcFdnVVhzT2t1UTgyRm5yako0MW1pZ1ZKcWFoYjJBMFNzc3Z4SklNdzhydEd6ZldjbEpPaFI1TXUrWWZrYUZubjBEMGhSRTJjZjRmbngvK1RHTlRORkphRVM0UCtLb2FvMnVmWGdlb1orU2svQmRMM05nWDdSdngycVdqMzhnd1Zma0ZnZlViMmRQQSt0TTRSVjI3YUhSdUZHekZpUG53NU9PL1htanAzWTVwRUpjcWl3YUM2bEt5ZFlvdUk4RW9rdWtLbHRrenRscXkzNmkzM3ZDUzQ2eVNwMDh0WWFTR1Ryclp1aHlBM0RSRXpsNlZORittTVVOdmpVVXZPRGJwNnZWU3NXeXN5Tllka3h0K1dYSVhKOFNQUkZIRGpJelplN0dZYTRaeHJuM3llOE40RllOdGVqSjExLzdLSXRCeVF0M0o4TzFCZkEwQjU3aitQQlBiaWNkMGNnYk5Fdjl0c1phVmJsS2lpMFhrRUVCNEhhbFdETTV4U3FvS0Y1UjFOdlNxbFZvZDViRUdXYnJ5M05uNk02Nk1pWmxWNk05RVR5aWFONGJ6R2dzWE5EWjdKQVBhblpmbWo2bzhrcmxBR09VbFcyck9UVWluNXluMjFQandJSVdHRnFYTktpRDZtZ09keStJT290cDFVdlIyaUdBZ29iTGZuR1E0YktmSDJwNmRvbXZvWXRERHE1L2h6MzBMRnRvNGpVcEhXaUVwajQxOC94OG9mRjB3N1NyeXo0QXRrMFpIcHdTVWo4cTU2QmhhUHRqRlpmSW1tS280RlJsTkQrSWtkRGQ3TkptdEt0WG04eDN5TGViSld3UUFLUTV0RGhkMHRldXZvMURkdG1uYlFCSmVmcEpLOWNBTVduN1ExRWRVME1rUWJ0NkhXSEVJZUt1Qm4zWmg5MVViaXVjdzNjMzJ0WEwvT1JhaFZVQ3podGMvUmlEcmlSN0d1Q1JFaysyV1dyRWQvT0xxNWN4Ulp2Ni9iZi9rbEZXcG9pM0RQbWs0aklYbkxUTGdDNGh6eStYS1h1NzJ0WC8vZWMvL3EzaHV5L3o3QU54L3FvUi9wQ3djbzVHS20zZ2hmWldQTnp2REVqVmhCZHpZaWo2R1lkNThXb2xhMEJmRUNPckJRdndoTmlPd3J4WlhIQkRYSWt6YkxBbTNsdzFhWTRUOUUxZ2ZNRUdvQ0syMWE3K3hOQUVML3Y1ME1FNTlMMTI5Ulg1L2JkL0ozODllazRZWjBETksvZ0pIaXRhWitxSm9OUmllMVZsYXBVb0p2TDV2c1ZWemZUNmFIdmxpU1luOVpNS1NuMjFpUnhCbTI1SWprbmJaQXpQbVBrTzErcGVDSHdWQ3N4N0Y5K1NCZEUwdWQzNW9BcnRPL2pxNFV4cnpmaDNFSWQxYmZzK0RQNEdxekVhL2cxaE5hT0JBci8rL25lU3NTd0RnaFVZM2dSZmlaa0lKVlAxU0J6S1FlMTB6RkNrREFMMzgrWXRrUGJUenhmMTVHWEhIUmJvUGs1TUdkK2tFWEZqWnhPQ211RHFMd0tHbDE4L2ZPY0N6SVdFZTRjQjVhSlhVQURHWEZidFUyamlONlNTbjFZVEdMUStBRlNkMTNPaWwyeitnbWhmNFBWQ0V5K1huZGhsUC96cHUydXdNSWpDRVMvaERESUhUclcrT3E5UmcwVS9nM1JZU2RJcVRvbU9mUFNCamZZRi9Mb2tVL2oxL0xrS3VtUzZDOEE3VmpvZ2U4NEtidW9hNks5TTJDNjFzNFRXZnc5MU4rb2ZFcWFwSVhPbVd6UkpXT1JlZTM3ZzZxNEM2ZU14KzArb2VIZXU0K0ZhMWFiNm54TVh2RmNhK2hHSUF3SlQyY01nQ2RzN2NuRTJsM3plZjZiaWh5ZVlkeFJ6RVkwVnNHak52WkxOWXJydWtjL0phQUJTRnhCNGFQa2FoUDhWQjkwMHlBa1pUVzFaMzZvcTFkYlc5VEpRVjBEcXdVY2oweUVnR3A0WSsrZTZqaVBqYWJHRXNRTWJ6RTRNUXp0S3diQW8wSk1lOFZYY0ZUNm5RMTJvU2xsb1hWRndBVTBGaGNFY0FYYkcxRzh4bThRU3A4bVZTNGtjeThvWWY1UEdJSEgrb0d0UTEyczk4b3B5endLdDBIMFF6UmlTdEttZE15ak1WRVRqVHAzNDd1Tk5RMHpNWUY1U05zWEJENkpMa3dIOUZkR2RyTkFlc2xnc3lNREFKU1Z1NVRpd1JhOWdHZ3NnT3RRUXRhSm9tNjMyQzIxMEpGNkxqZU9FblBmN0JCMlpVSmhyWWdtTHc1Q1F0RGtVUklIMXluVitTQjVYL2YwLy9sczc1Q2hxdG5tYjIrWXQyT1pPNER2ZTlNZ1kzUHh0dHpmMHd3NlorK0c2alhvL3RMTFVnYmxPOXRQdHp4ZDRUd09lUjJhOENmS1VFQjhFOVAyRDFvb0ZkRHhOWVQrTGFscHdYNGxadFdTQjNCZHNMeXB0eEk4MUhpejZDUDhNM1E4NzJGMTF5VXJBUjRXSlZTY0JraFliRUs4OVB0cDZjRlpkOGZDSnBsNGkrbmo4VVIxNUZHb3FtTG9XVm5TM0xFWmFDSEkrbmlCSE1qTVZhcWRCa0FQbXhkSGhsNWZDZldDK0lYSlpEUk9LUEt0dFpYSlZpdEV4UUk3UzM5WTBDS1lvZ0FxMXBVZUZIenpGL3ZDdlB0dnFiZkhuVjNTdU9pWnV2MnFHSlpMNVBOVTBMRWlUUTBnMGVQd3kzckwwbW1aTWI5SE9FR0lLb0VFc29rUXBFS21CYVlyVytPYXRsUVcrdzNUYlVEdnpIQ2NJQXNzUkEyZFppTHRpeGJSSGxoVkQxdWxlczRBQ2taSXpESE0wWmZxeU9vYVczT0g5OXl1TG91YW90WmVGNXNBQ0VBZE5KS2J5UUwxZ2tjajVFYmlvYjkrOWVxbW9PMHFlWlY2OFJmdTBlMlFGRlVMVE1MNHBibTlnVkZpSWNWektoVnVyWjF4cVJ3cHNBVTM1N0RQWVdKcis1UDljWTNWTlF5enhldVQxQ3VBTldNQTJ4SjlwK1pGS0hmS0RESFJkdHd4TW1mSlZldm5HalJhZkt3YWZQei9HenlxU1pseFlxUWE2NkRvWWtwc29STEFRcXB1elRFblVGL3R4ZENuN203MVRLVE95M0xIc2l4MTFlVGd2bHNic00xNFJ6T0txSzFRUkhPVVRVdUFBUzZGbVVNajZTSlVVaGFYdW9qNjQxbTU1b3kwM2EyT3BDalZxMmllNy9YV2xGZ0xGRXlvdEJLNUZyWmd6clJWVW92ajdlTTl1OHNBNCtmMjNmMEQxRHRrWkJXK2JjcXlHeEorVjhaaHdUNXhZSXRnTDV6UWgxNExFM1h6citQb3U0aTE3eWQzUE1RYUFNc2o3WW9hVk44WmdPeXNhWkFxenJFU05velFLM3l1MUJwajF4MHRwWFJjUW90ZFVVRlUxUGExMmprUkg3OUlibFoxSTRrT3VEU1BiT0hYTkpBV0JPNXk1bDMyQU9DWFBsWnc5cmZUKzFzbTI3UDZWV09UK24yanlrUDBvOXY1V3NPdXNZNEhpVFp4WUliN1ZybjZJOE1qZ1pUOS9YclM3VHBzYjMzRVRHOTQxUFZrcm1mbnJocVVQYjBYZkxVNTE3VlBjRHVpWDY3N0FQOWg1Q2FySWdJKzZkc3NlWEhBMFVOM3Q0eHk3NndvSjdNNkNTWG1zRkgrTERNRlNzUjVzemdDUitzNnRLaXc5SGtlMFFCSkhBZzJxYm11UHI2cDh5UlpBMnpqUWtxTUkxV2JjOGZTOEdWVnQ0UFZhMWhLdnNCajNZaGM4K3B2WGI5OEJKTDZXZ0NKdkRsbUNWbmdROHgwb2tRWWdhR0crSTE0aTlYK0J6RklqajcxV3hDam1PZm1YdDYrL3Q3QjFHYTM5MVlQK1lhZHNjOXpsb3pwMlBJTEw5VmhVU1Z6U1N0YVNXcmkyTHRJVEdjNDFPcmFLc2krOGY5NDc2SUN0TjBxTENSZWQ0S0xiV3UrcFpxMDkxVjZKSGJmUjBSQnRxbGpLUWlqRGNmdFlUZXBHTzFHUFhjWDlJVVBMYzJwMXREdG1mdlc4T1dENWN3cUZ5TjdUZEJEZHBoR0NSL1VtdWRJSUg0OXkvam52Vk8xVFlVT2kzd3RtQTVKeFBEYkhjR3Rtc0QybXRTaG5xN09CSEp4VHZzbUV1eG5iQXlRN2p6d1hwVUpIbXlDNGFObjRUdWRGSjdsVWZjaXV4S1JuaHkwSExTTFhmR09mUTEyMFdaNkt5NjJpYWsyUXJ1Tk40QkxJVUFTYlJVWlIvcmNRbUhqZ2ZXNExrSU9rK05vM3BBOWt5WUErNEJWa0k5RzZMY3Q0N0F6dFpUM1lEQklpS3FMUmxUbUNoS2RTQXpZbk94Nk4xcXhqOXI0ZkwvMkZsMlNnajBaNWQ5a3ZYLzlBNEJRdmdQR05zUGcvTmY0ZkJZdmtlR1JEQUFBPSIKKSkuZGVjb2RlKCJ1dGYtOCIpCgpwYXRoID0gc3lzLmFyZ3ZbMV0Kd2l0aCBvcGVuKHBhdGgsICJyIiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIHNyYyA9IGYucmVhZCgpCgppZiAicjI1YzI3IiBpbiBzcmM6CiAgICBwcmludCgicGF0Y2hfcjI1YzI3X3BsOiBhbHJlYWR5IGFwcGxpZWQiKQogICAgc3lzLmV4aXQoMCkKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKFBBR0UpCnByaW50KCJwYXRjaF9yMjVjMjdfcGw6IGxpYnJhcnkgdjIuMSB3cml0dGVuIikK"),
    ('patch_r25c27_mtpl.py',
     'web/templates/music.html',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMjdfbXRwbCAtIG11c2ljIHYxLjEuCgpyMjVjMjcgZGFzaGJvYXJkIGF1dGggcm91bmQ6IHRoZSBzdHJlYW0gcGFzc3dvcmQgaXMgbm93IGFza2VkCk9OQ0UgYW5kIHNoYXJlZCBldmVyeXdoZXJlLiBSb290IGNhdXNlcyBmaXhlZDogKDEpIHRoZSBwbGF5ZXIKYXV0aCBwcm9iZSBzZW50IG5vIHRva2VuLCBzbyBpdCByZXR1cm5lZCA0MDEgYW5kIHJlLXByb21wdGVkCm9uIGV2ZXJ5IHBsYXk7ICgyKSBhbGwgcGFnZXMga2VwdCB0aGUgdG9rZW4gaW4gc2Vzc2lvblN0b3JhZ2UsCndoaWNoIGRpZXMgd2l0aCB0aGUgdGFiLiBUaGUgdG9rZW4gbm93IGxpdmVzIGluIGxvY2FsU3RvcmFnZQp1bmRlciB3em1sX3N0cmVhbV9hdXRoICh0aGUgc2FtZSBrZXkgdGhlIGtpdCBnYXRlIHVzZXMpIGZvcgoyNGgsIHNoYXJlZCBhY3Jvc3MgSG9tZSAvIFBsYXlsaXN0cyAvIFBsYXlsaXN0IC8gT25saW5lIG11c2ljLgpTdXBlcnNlZGVzIHRoZSBwcmV2aW91cyB0ZW1wbGF0ZSB3cml0ZSBmb3IgdGhpcyBmaWxlLgoiIiIKaW1wb3J0IGJhc2U2NAppbXBvcnQgZ3ppcAppbXBvcnQgc3lzCgpQQUdFID0gZ3ppcC5kZWNvbXByZXNzKGJhc2U2NC5iNjRkZWNvZGUoCiAgICAiSDRzSUFDdzR1bW9DLzlWOTI0N2JTSmJnZTMxRm1BMVhTcmJFMUNXVlYyZFd1M3hCMTB5N3l1MTA5V1VObzhFVVF4SXJLVklpS1dWbXVRMzA3TXZlTWNDaWR3WUR6S0pmZGhmN01yc1BDOHpzODh5ZjFCZjBKK3c1SjRLM1lBUkZaZHF1M3F4eXBoU002NGtUNXg2SGorNDkvZWJKNjkrOGZNWm15ZHcvKyt6UnZXNlgvZXBmUGYvcTF5d2FqTWFEZzJNMlg4WGVtSzM3ZHA5MTJZSkhzUmNuUEVpWTY4U3ppOUNKWE9hc2tsbUh4VE1uNGk1THdrc2VkRmpnckpudkJaY3g2M2FoVyt5ZCtVNHdQYlY0WUoxOUJpWGNjYzgrWS9EemFNNFRoNDJoZWN5VFUrdmIxOCs3aDFieFVlRE0rYW0xOXZqVklvd1NpNDNEQUdkd2FsMTViakk3ZGZuYUcvTXVmZWt3TC9BU3ovRzc4ZGp4K1duZjduVlkyckk3OFpMVGNiam1rYWI3aUU5NEZNR2p2UHNnN0dhbDFRYkpqTTk1ZHh6NlliSE5UM3I0TTB6ckoxN2k4N052QWdBRlp5OElrdi84VC9MRHoxWVhqM1pGQlZFWjRjVWk3cDlhSHZSblVTSCt6R0FXcDVickpNNnhOM2VtZkRkZVR4OWV6LzNPL2VFVCtNamdZeENmN3N5U1pIRzh1M3QxZFdWZkRlMHdtdTRPWUNwWWVZZEE4R1Y0ZmJyVFl6M1c3OUcvbmZ2RFo5QkR4TWNKRTVEY3dWSTI0OTUwbHNndkViUVpET3pSRHB0NHZuKzZjMzh3ZkQ1NmZ2RDgrYzZ1YUw1d2tobHpUM2Rlc1A2UnZjZjJCdlkrZThJR1BidkhSaU1vZ01aN2JIOGZQeDNaaDJ6LzBCN0M4K0VCUEQvb0FVN3REZTBCMjRkOVluc2orTFIzQ0pXZXNMMERleVJLUjBQUmZsOThsZTNoeno2MmgwNUc4QWxHR3JIREhud2FqdXhoT3RjZ0RQZ09pNU1JVUpKbVBueDZpRE9YUmQxMDBZRGFXUmx1MU5oWm5PNUU0U3B3UzhYZmhWNlFsc3ZGSTNEaGsxWFp3QVZBTlF5Z0swQlhzWDI0T3pGc3p3UXdKYmFuWVRqMXViUHdZbnNjem0vUlBrNmN4QnRUWXphT3dqZ09JMi9xQllXT0ZQU3BHMzkzSE1lREx5Yk8zUE52VGw5Z0JVQjZKem0rQWp6NDZWNnZkektDZi92dzd3RCtIZlo2bjh1cXIvbGxzb3BFdFVLVnoxMHZYdmpPeldsODVTeHlOS2FGeGNtTnorTVo1MG02YUNvNXkyb2RSMkdZc0hmWmQveWhRd1luR2svY01SQ2U2UEtrOUx6YnZaZ2VNM24wMUVmeEtwbzRZNDdQOStDL0x3M1B1d09zY1FEL1BWZHI0T1lmczJoNjRiVDJCaDEyTk9vQWZoOTJtRDA0Yk92cVVsZUR4NlBuVDN2cTQ0UmZKL0JRSENIMTRYeVZjQmVlUGhrKytmSnA1YWt6SGdPRmdjY0NpL1dQYWVqUmwwZFBqUlhpY0lLZDlQYjdvMkdsVG5UTStvUEZkUVZFVGhBZnM1MGNNM1k2ck9zc0ZqN3Z4amZBRGVaQS9lbHZkK1hCUjZqZGhYcmVSTzFINGdWMEpUQUh1aWwzdXFHWDk1OWxIeDhvS0hJUlhuZGo3M3N2QUVTNENDT1hSMTBvS2s5ZzdrUndSSTZac2lzTHgzV3BYVTg3MHB1WjU3bzhlS3NNbUswRnFReTc1ODJSd3poQm91M2pJblJ2MUFrNzQ4c3BFWk5qdG5haUZ1S3dnazJFOXVsVHhCemxPUjdscmppS2FTMkVtbEpyN2dWZFFkS1BrZXE3NjltSllTSmQ0aTZBNlk2TEhIU0tmd0ZsV24xb3RyaG1JL3J0Sk94d2RKOTErNzM3SFhFbTl2c2QxaC9pb1JqQkw3dS8zKzZ3SklLSkxFQWdBRUZodjNlLzNTa05TZFJBR2VTSWV0L2JsMlAwN2pQVEFMMURaWURSNkg1YkMzYmJXVVdPQXZkRkdJT0FFQUlhVEx4cjdpcXdFSmlEYzFvQnhvOTY5eFdJZTM3Q1lVc3UvRlVFTTE1Y0s3QU9GODdZUzJBM2JJVU1MWUIzUU1zdVg4T0VZNEUwNVJyZmQ3M0E1ZGZIckRzd3I4VjIrc3B5aUljZEE5eldWK1grMGkydlB2RTVVb0J1ZjZBK1NNSUZsdmZVOGlLcXF0czI5cUt4ejNISGhvQVY4SzhqTVZHUUd4VVZTanVGUDA0QVNDZjJ3NFd6bnZUWllEOW0zSWs1d0tNYnJoSVE2Q1lvMDhFWUNQdkFTWGdkZkFZRytQU004S2s4aWNRRERTQUlRTU9lK1FUVndRZU9BUXltd0tjNzJCcENBelljM0FwQ1A3M2tONU1JWk5jNEJmVTdaWFZLQVJYaTNDWmhORDhXSDMzb3ZYVzB2b0lUT1ZqUDJvekU3QlpJVUNObDJ1K2JUR0Z3NnlsMEQzRU8zYVBTSEpwTXdiNktuSVV5eXR5NTdrcEVPVVFDWkdBYnFPaUVCdDR4OXAzNW90V1BrQmNPQ1R6MlBueHBhNnNEYTBxU0VOYlRQeWlOVnB3bTRKcUo0MHg4cmt6UjhiMXAwQVVVbUFOdFFiemlVYm5DMUFITXBSbnAxcGJQeDk0clZWRW5aRVA5Uy8wSkd4eXFjRXRQV1BXSlFtWXJ6M0dCS28yc1RPVUNjTUZWNXFMaGlCSnFPdFo1SlNlSVVxMXhITWVlaFhOZVFSZ0NtNkNrVmF3b01XNlM2blREZzdnQ3pOWStyR3lLZVhKMEhFQVE2THFnSGtTU0t0VERTY3ovZUlacXIwNnNCbGx3TXRITFdMc1BXTXlkYUR4akQzYnpUa1hSaFJOOUF2U3NDa21adE43V29SUmdNSWdQY2VoN3JxeVA4bmk3RnZ1T2pvNVUvTXRPTlcwT25JckJwb05qSHhqUFRRYXY0MGs0WHNWQWFKS1pGMVRFVjVwVENYRWtCOTNRSzFEL3hTcXBFUzZyb2tZS0t3V3hnSlVJVGFmYW9va2tlZ3djcmdldzZpR3d6T0tvSkJjZ1V0NXZ0TExqWThDcU1aK0Z2bXRDNEEzbkxEMUpJOU14ejBjRXZWaTdNUlZZVlJHenNsdkdFN1l0RG81TU9DaWdEZ1FDOEhSVUMvVHhLb3B4SWxJTTNZN2FqbWZlSW01KzJNVlozaHRWcHd0VnU4aDlqeG4rM3NDSTlvMEhDdWRqMktVbXA3OHhTZG1JWGR0czRwQTJ5RDR5YkNJZUhmdmc4SGFicUFLbk9iRy9EZWtocmpCRzFoTXJmR0djekc3SkV5NUFtc1c5MG1IUzRVYmFlMWhEZTNGT3M4R2R4QVRCcDBHODNEQ01IYTh1YmtXZ3RJSkFzWC9maTVQbWtLVno1bnFSMktSam5NUnFIdWhBT3hodGd1M0FPS2NvdkRKTmFScDVpbDZOSmNBMzVndVUzYnRpUXFoZkQrQzg5aWNSaVZBYU9Vck1zbnBtTnNvVEJlSXB6bDBWaVpUREs3WW8yb1p1Vm9sT1FaRXo2bkFUeCtYZmd2ZzZGRG9jOUpMTURKVkJ5aU40U2d6dHNGN2MzcVRaeWU0VmZJL0N1VWF4eXF3VkNuTXpLVjIvYVIxV0xCNkZXUmdWdUd5Y2Z1MDRWYkhqdlFuenRCUnVXMkd4VFBJVVZxRU1hQ05tQTBadEhMSmdaVzNmamM3U0NiT1QyV3Arb2RlNFJnT1R4bFY5b21CN3Y2TGtGbGVDb0hDaWd2MXZPSEw1dE1OKzBuUDZ6cDdiU2EzSHF2SHI0anNnT2Voa1E1cXozbGJNb09YT1BWV2xRL09sWEhDdkRrN29SL3ZnR29sR2ltazRINzZNL2RCSXREMXlDSFkzeklzSU9ROWMvU2IzUjl1cXpUU3hRS1VFb2xXL3lTb3JJdlcrcXB4ZXpXRHVYYUNCWTlJanFuSWU0c1hFaHhQTWhGbGRvOXZtVmJqdmcranB4VFhMTWF1aEYzNDR2dnlnZXZtblg5dzRpVDg0VHZkSFJwNk9idWVMSlBnUXl1UkdVTmV5Vm9sNnczMFRoYXMrMldTNlY4OWVWVWdoUGJNT2ZrWHNPS3BTaGkwTU14TFFXd3JxUlRNeitrWElIWkwvUXNkSTdYRDJ4Rm5iWVdBY2J6UTUxQnVDZ0pwOURFcEcrRGl3UjZaTjd1K2JqS013SDAvUEZJY2I4R0pReC9nVXk3eFJodVBMQzliWDIrRE44ejBPa2xrWDFEUGZiUTNheXVRckloK0k1bkdqcm9hYnUyclkwOTdtbnZxRGVKUDhpYUI1VjVZSmUvZkwzajgwL1dpa3hNd2hvKzZRSW1LT2FsdFh1YUplaUF6Q2hIOEVJMEw3RmtwR3Bxd005Q1NGVHBLZUl0VlMyY3B5YjY4SGI3TC9xUnE1bWNYRWw5eFhacEZ1M2Y0bW1xNEhYNTNrZXRRandiVzhSVUF0NzNjcUdnTGlWYVhtUWNVYlYvQ1dDMDR3UUd3dVd6TVZhaEhQTVBpcmIrL0Z1YWRPVEhUVFlSSXRHN2pJQ3JQSy9kdGRtbHF2d1duWWZZRE03d1pZRVJwQmkwYWRxcG0vMW44dUhDSTlyVDlWdGFCS1hPa1ozT0NqM2didXQ5ZGg4SCsvRDR6dlNLWFZJS0JlWEhwSkZ4dTVVYmpvbGh6My9hb2EyN3lpUUVueUJEY2dGS29oUXJnSnlWc0FYRnY0Q2RIbDFtWmp4eCszWksySGpBZnJWdXhNZU5lSnVBTWdpWGtpVDFlSFlkaUJNb3hlVys4UGV2ZDFGZVgrWlkwd2tFbHdNLzJaQlNTd1Y2cE5RVDlreitpdTRKY2F4K3ZkcGRtRGpkYkJ2UnBpbE00cThTb3VQcUtOc0srZUEzK0QxWnhIM2hpVzZseXNmS0F3VUJDYmhjT0RxdTZ3VVI1T0F4WDJ0dFhxU29zNDlwMDRFUXhkM2E4Q0s2RVRhWEFaUm5BU2pFYytBa0VnOGRiOFpMTUdtVEhraW5mM0xwdmV5QVl1MW1CSGp1ZDNLcVVZR21wY24zTUI1M21WOEZ0VHRjd0NzNEdWbFYwRmhzblg2R0ZhK2IrLzE2N3RVN1AwaG42c2VvT0g3UDR5Q0MrMmhHenFEQndhVllEaHRucGVjVVU2WjhkMU41NDVMaXJnR0FvTnlLbU5NVHRxTjBBQkhRbjhkYXM3NnBuQzBSWTFSdk5iVThBakkzbWo0ZXl4UnNkTUxZaDdSZ3ZpM3YrSEZrU3gzbURSeUlKb0pGeU5TRXcybEQyK3JmM0pvR1lXK2NqKy9wYXhJVDVQTU1vUVRWUkM2dWhyTllvQzRxNFdDeDZOUzZ6L1U1aTZsS2lyY2hDZUh0REpGb0N1aDlLbnRPUGw4M2MrbXFGeThLTmFLc2VKLzhGcDJsQWZQVkFicFFEVHVGaUJ6UGRwekpjbHozQ3ZPdDBNc1UwVXR2cGtFMmVydFpqZTBieXBoK1Nuc2svbUl4ck5rMGFhYWVobjdsUmlyRzdqTnJ0am1FOEpTNFpHUmsyaFI5ZC9Gbmo3cWMzeW00NjFBTTBuUXNUZEIyeTU0aXNPQ3ZnOGpEaWplMHJsS0pjbFNvOThHNXNJS2UrMUFXUTB4eDRvOXZRL0tLeHRnMDNrWU05dzNhQzM3VzJEb2gxQTlrSTI1eG9yZ0ZoNTFSQmc4dTZya3lpSGQ1UjZSdjVVME5UbUJQY2Z5ZTUwTU5yQzduU3dsZDFwc05mUTdxU3J1TUh1WklweHlQd2VlSytHZnZYVXBhTWNsdGxnRHl2M2crNFlhV1F3VGZXYm1xYUdNZENrQzIvY3ZlRGZlenhxMlFNRVBGNitnNVBjTnpBQ0xRYmxXN3h2TXRKUjZHcFAydUwydHpiRlZaQWFUa3NGcis5cVNsdml6ZWtQTG5GVkxWWVpWUHA1L0NtckNjc1U4NW9OUDBLc25WWlhxNDV0TDM4RUh0ci9VVm1vRmdpZlNteGJhdUlTVTYyaGUzTmNkMTJtSnpCSm5yVER1NTIwRHhHS09OakgyNGZGZU1SbTF1WnRRaEhwQUIxc2lrU3NXbFlhV1NTV2R3dUxVN3U2WThCYlpYL3M0RmFoc2JjMHdCK090bktsVm1icnphZUdlNFJHNVdGdnNPMHRwdzNXcmp2YTA2bzdzQXgwcFBuUElwUkxUdEM1Uy9qMHdZOGJzQ1dYNEs2aUQ3eUliYzVCU1FiNkNNYm12bGxLa0s3TE9qVjNMa0k4ZmVlaVNRamlWbkE3cWplVTdwdXUva2hCa2FaMkprd0hIY05UZXgxdTlNaW9WK0hiMndzY211Z0tsVUZVS0UzQndyMW5GQ3lxbnNVaU45YkVzeFd1RngzZDRYclJscUZpT3R6N2JoVW4zdVNtSzdQc2ZHRExZYnJMMWYwMVhVc3NBczU4YTZQWXIrNlNYdXI4Nmh2dkFNc2VUS1l4VThCU2N4dFYyZll4Qlltb1pPeWdnanViT21xRXNZMGhsd1l6eWJDRFptK1RtZVRJY05ocE9mWkZlSDNIaXdNTklhNGUyMzNqK2V0cnd4Z3k3OGl3YWl1V1Q0NEc5MnVXZW50ZFRJMmhNUHNZYWFBNjdLNUVSdVVjNDJCZ3VCTzMyZGQ0bDFDOVcrVlpvZXVqR3dsaHpSWGE1cGZXQ0tSYVYwWlRtRGJpT2xzNWRHOXIvTDRqL3loZlpuZmlXeGdGS3hiblJsNTdFV20zMElXdjVHYWtUU2JkZlpBRFFDRFlSMnAxdEwvRmhmVk54cnlxQkZCQWdMcmJ3cGwrZExpRnZGU05tMEJjWWNOZUdqdVJtNjVIN1JOellnMDFtOG9ubE01L091ZXU1N0JXWVQ3N1NGVFZrR2ZOcmNZYW13RktWQVp6UVM0OWpLb0NpYzRzb0ttbXhEMFhyb1RZcnErWlpTa2pWWDFYNXNpUTJsQzBlbkZTSGFUcUdNNmgwdCs4WEtORHQyaEdxNTNpc05FVXpRN0RJaXdPYTJGeHVHa2djN3lJeXVzcngrUjlIVUl2S0VGbDNJMjR1eHB6dHpzUDAwQTkvTjVXUS9FcFNWS25DY29YNHBmcjcyWGk3MGU3TW9uZm8xMlIxdk96UjVqeFRDYjRjNzAxeHIzRzhhbEYrYmljdnNWUW5lMktVMzFxSmRHS1cyZVBkcUdpcWNtZ3JvbE16WWtiS0JzaExiSHluSUtVYkJSUVhUNEZ3bDU0U0JVd2phWjhpaWx1TEpsVDB6TG4xTFN5bkpwV0lhZW1kcG9WNEQ0cTVOeTBxSlhNdVNtK1JOQW41dHkwUkI1TFM2WUx0Tml1cGl1WmY5UDZNOHEvYVZWbVNReUcxb0xJWktVNU9TMlp5OUFxWitTME1DT25wV2JrdElpMVdwV01uR201QXB4SHVFbnFMaStjREVVb2ZaQlZTczBLVGFDQzBzWkpHMkFhblRRWjU2NTE5alA0K21qWGFWUWJ5VExheW1QcjdHWDZzZFJXbkJzZW5lWEh0WGdHc2dRbGxtNUJHb3o3MHgvLzhKKzBxeEdTdWdmNHNyUlljclBnYWVjV0s2UlpPYlhPUmQ2Zk9BeW1jUWRHU0hESzhNRy9XTTNqSDM3L1B5emllZU1RV0NKUG9KTndNaEZGbE9OR0dWUlNjYm1ZYVdqUkJQQ3ZtSUY0YnAySlFSL3RpdTlGNk9USFhBVU5KU2NSSGNLaEF1bjBpU2dROERpck5KV3BLNmlCTkNmSU0rdDdhNWpMQWtTeFJEMnp4UUh4Sm8wWUQrU0I1RVozdW1jRHVSU2dvamVZN0djSyt6dlFuZDJ6Y3dBdzNnb0pRWXpHR3g4NENYRmQveSs4OE54eDFrR0hybWFnYkltWXo1MTVMSjcvSmx5OVhsMXc5c1B2LzhBY3R6dUpPTGNmWFVSbjJyUDNPcnBoTitFcVloTm5EWDhvVnh6dEtRdEJlcUZ0cHF6RzlxUGRoWHFNTXFvc1Q1VUFvQ1RVdTBoMVV3cU11YU1MOTBndzhiTUtQY1JoQWg1OXFLT1ZaZndYSWU3cXRsRFlQdmFXUEZuQjJlZ2Q5M3FZVVhuT3pkdEhzY3BpQ3ZKakZQcDRDa0FDNSttVWZPY0NjOVdldzdBV1dscEo0QWM2cjl2c1F0OFlzMTNpWTZhS1NBckZIT2hUa3lZWVhDMmEwQ2RkRTExUkRxR25KZ2lwTzF3WUZLVVdGZWJvR2tuUEhxWFNwdTRYVDhSbnh3Y21scDArOHlGYWlHYkI0akdnTk1BWWIrK2ZXdDhzZUNBaWNIU1FMbEx2Y1Ric0NnN3dtWTdXVmRva3NzMXJITXc2ZzRQVHFKa2ptejJtQTJPZGZSNWN4SXNUTFhYVmJFQ1JWQ1crYmxtU090S2hPSit0SmhrMDhNdkU1NndWdHhXOEZBOWdDZi90ZjFhcHBiYmpseEZmWngzakZ5OWN4U0ErS2oyblQ2RHJ2LzVmVGJ1R0UyL2x3cE1YNU9QQUU3WUxxczRxeGxXZ29xZU9oeldBQkZFVkdQUnYvckhob0YrRDhwZU5nMTlZSzFENnBpcXdqbjlvMk9VcnZzaDZoTS9jZ1Q0anBVOVJUdnoxcnhwMit5S01lTll2ZnFGYzhVQjBZaUxkOFlKenQ4TmluL05GaDYxUnRlU0N2eGFHTFRXRE5mMkgvNjBmWEllQVplWjdMYW52RXorTTgyblJOMG02eXlPWG5wejk4UGYveGNpZFZVWmR4SHdST2lhR2xwOU40ajZGemNpYTRtTlRKa0hoRHlxMW1nM1Bmb0gwQkJqd3NCWXdTd21aWlFxWm9tUlNndE5tTUZSbWhnS2Y3SjQrbm0wSnMvbDVMY3lFbTBCV0JWUTVGMS92Q3JnUzFqVUc0UHdqQUhDZUF4RFg5M09Db1pIT3pxdGNxMExVMFFrcGhIRWhXZUVwTkhLREVvWEdtcXJnMnYrWHYyMStJRzgxMDNPa0QzVERMMm8yVGF5dlR2TlBmL3lQZi9lUjUvbUtYNEd3QkVSL0VrWlgrRzZRZm8vRmpXYjhKZXhEdjZkTytZZC8rNS83dlk4ODUxOFMwVzBrQ3dCOUZtZ0lIMzVGQW1tWjFxOFNFK2JUSStRYi96NWJqVkRHdFBJNjJUZXBHOUJTcHp3YjAwSlhMd3FpYURJQ1BSbTFZUTZxTVNqaThIbnQrQ3VPWDQ3S1JGd3NVQzhsZlRBb1BnMnZBajkwWEFZS1Qwd2FSYU50ZitwWHR2d2YvZzg3ZDlhODBhNmJDS216Y3IyUVJrQnIweUxpT0RWcGZRQTQwT08wYmp5T3ZFV1M5OW1hckFLaEpMWlUrN0NGa2d5b1lkNDRzY29tc2JWRFp1RFo2L0NTblRMTFVuME9Oem9iVzFiZkQ4ZU9mNTZFa1RQbDlwUW5YeVY4M3JLdXZwLzd2eFZLMzIreHJ0WFdJc3Z2ZnNkaUhzY3dZVTBQTC8zSDFCSnJxYk42ejhaT0FscHFDdzJGQmFOZXZoNEU0aWx6UWFlZmc2S00vVDd6T1g3ODh1WXJ0NFd3YlZmQlFPNWNhUGJtYlFjOU8vQ3AyOGZYQXdscDloUjBVRC9tSFJZSkdldFU5VXRoRjhJbThRc0p5YktmSXQwYzdyYzhuSGZFazFVVUdDZnB0VStVcFdVOVRPWUp5TmFhamZFbXJIWFBpNTlUcmdhc2dpQm1qMWl2emVMcWhQRUhpMTg0eWN5ZStHRVl0VlFIVjdxdXVWSU5DT1YrVDFOWHJnbURJcTFqQzM1RDFmdFFGZVlBQlBVTGhoVGdHR0RUemg4WmM1K1hnUmFQOVV1V0k1NERiZ2RUNlBMMGxBVXIzOGV4Y0tpNGJjTitvWVdvdGZ2bTgwZG4xczdiM1drbjc3YzExblZhNk5qNi9DZTRqckdONzNsNkVycjhjZExxNGZTdEU2dTYvdmZOVm9OVUJzUzc2S1oyVGExV2JLTnhReHdCR3BNSm1OclNDQ0llRkpjbzF3ZWxOa0JrM21vNEh6cXIzMForSys0dzE5Zk5DYkZnaFhpOTZ5eThYWHEzMXE1bzlzWHkxS0lnMkRGQTU5dFhYejBKNXdzZ1Z3SGdhR0daR21SQmJNWEJWdXdoOVB1NTY1LzJQeGV2cU5KM1Z3UUhkaTJ4S0FVRzdEanJFb0N5SWtJMXc4aVNpR1hENDNmVHlHbGRJOEt2bW9HWnFBT0NlZGtZeE5URURPTGxqN0s2MHRmZEI2d0xQNEJFUk1URmwwSTBUcnEwNTQ5LytkdS94T1ZkZmY5aTRxeGpxOE5lUFh1U0ZRa3JLQlQrb2xpNkJQVHhlS3djdFF5aTN5RjdiRjBDMGlKRkpXNlZUdm92enIvNTJsN2d1OWxhV2k1MUtWZ0xFZ3ZCWmR3VGhiR2tCQnJMRFR2NlhRd01IOGRmNStPWFJvdlQwVHBpUWpIUktXOXkwMXEzMitxQTd6VzhEQ0VGc0JBTEpSQjJnRUZwdUplQVgxNlhvR2lvdTRUS2VjMWZtS3BteXdTVzRxeUpWcVZBd1duWmNUam5CYUZqVW54dWV5N1E0bE00aS9EaDg4K2hnTTZ1S01LUEowZ3NUWEJOd3VuVTUrbWcycFBpRVVlR1dVeEFjUDhLRFoxQUFjb0ZkNW9id3plcDZBK1h4ODVPa2FjS0lDeDhEMGl2UjVkNmdMbURvRVhscXlDZWVaT2s5UTZrdVdNYXFrT202bU01UnVxZE9NN0lsWFJUVUFGK0FLeGVSZmdOL25SRXBEVitvdzhkZWlraGZzVy9GYjZUenBSbTR2Tmdtc3pZR1J2MjBrbkxvbE1zcXJZVVdDMlJEZXMzNHlLTFZUeDdSVmhvWW1zcGhzcFB0cmc4VnRpbXFMQk45MXFSdWxHUnVsSHRFKzNTMC83L0xQWWduVXhoRzlwTUtjU05NTzJEUEoyeVJmT3RFSnhYeTJuUzgwOS9xN3R3WGRpRmEzWVBvTDNVdzFtMFQ2RnM0a1dpVnJiOHd6WXJsWnl5UTlQU1U4cEU5Ylg3RE1jOGVwWDd6MXIxV1o0TDlNV0prOVk4QmxGcGJxUXdvZThXbFFoa1JUZm4zT2Rqb08wdFM4UjFXWVkxUTlzMmRnQ2kyUnh3cFdXUXEwc0RqRUdhU3JqVUFWb1dhSVc2M2wyYjFOZXY2U0F3Uzh4Q1Z3M2pqSjRJVHlGVWhMVnFLcVZEWTlTRDdTd1dBTThubEZUUjFZd012T3kxTitmaEtpbHJta0tRczBYKzhLOUJ1bWl6d3JyWmU0UXhNdGpCZnEvWGJpUktaSjVEa3l5QmJycFQxS1RJSHlkZVJUVytsRVhDUHdhRmxEZElsSkcvQ29vbzE0OG9JbitVaGptaVR5N3RDZDF6MlB2VHZBajlVWnBXNUU1STUwU3VCV2hIZnBHMGtId2tXUGlLTDlJeXROMjNkV29rdjhRN1RGTG5iS0tUWTdNMU5DQ0o1em53OWFSbFVzNWYrcjhNZmFOY25LbVBJTklBeVYzRGlRVjZoYk5GTzA3YkptTU5EQ1Excm5XN1hqZlhuejk4QitNTnpLTFZicllVWlhqTjFFbm50NFVmQXRvV1YvRUY5SGJNZXZaUnRSVnRBcG00MnNwNWFhMkp6ZlN3dWZXblAvN2gzNkEyQ1lXZ1ROc2pXZmJ2U0psRjI1anVvTlpJZzlrbWRISXdWcVhCdXBPQ01EcC8rZXpaMDNNMFdQVHNBN3hVakVuNTZIYXhqVGM1M25aWXZQakt2WVlLL1pPYWJTRExzSFlqQkV6VHcvZ0t3M3RQNWFodnFPdTNCb2dLWTNNRnBLV21CTjQrUXJML0wzK0xnQ3cvQldVWGlodVM4d0RHK1daQ1poVXRUcWMySE1Cbk12TGt6S2h2VXY4UndFSFo2a0VmTWZvbUJMMmFQWUNqVXV5ckMzMXBzQ0NkUW9BeUkwd3dlUGp3cE03ZUVHaU1DaWE5ekFNbzladEJDRGRSeU1SR0lIbUkyMGluU2J3dHJxMHRCNnhPVFpqSlBEMXpRd21EdW5uanZUVWQyRGdhUTYyUy9ZRkludTZFWnpURDhFd2lzbjR5U0hKemZHNFppTjhDVVdSaDB6RnNweDhVYmlka0J5dnp3VkFhSk82U1Z6UnhGZ1JxNWt6Um9Xd1FUekV2SlRGeGRBalpqdXUyck5YQ01sUUVtdkU0QVNKeEFWU3FaUlY4WTBBOExJS1ZybVZKRnRleGNySzd2dUNKMHpMS1ZTK2M2REkyUVdxSk1ROXRFZlR3aXFxYklCODk4ODFpMUE2R2NiN0JWNFIzdlZOckJ4QWFrWHJIZXJ0amtxYWhOOVFFbnZsMlBJNUMzLzhxU01KZmV2eXFZQUNvUEFNRmdIWUpxRFhlUitVeDJoa3UrTXhaZXhpK2I4WHpNRXhtbHRCSWExVnkvRmw1VGNYTUVwaE45dHFXUlRHeDU2S3V4YndBWDBMdlRSMkFEeGttN3JYd21LSEttUkd3TjFEeXRtMCtsM29aQVgreXJ1M2lxRGErSHg1M0FYWXE0RmZzQlQ1N0ljdGE3OHkrSG5RU0hSZm1WREJZZG95dFVxV3IwS3hvMEt4cEtIU3pZanNzb1diRmdNajZzYS9DNkxMVWlZZ24vNEs5ZWNlQUhsVWZBU1AxdnVjeElFcC8xTHVHZjRBOTZQdUJBdkZlKys4V2ZBclk4eGE0Mkp1MzJxRjFaR0FMZHA5aEZPS2VMbzh5VWlPRjNVcHFoeEVxTHJMWkgvN21IMGxlK2VHdi82OUdYeUR4dEVDVmhBV21aWVZJWnlULzFGRW9rR0JOcmFTakJPVkhROFB5aEZQSENzZ0ZBeWxoL2V0VXd2b3J5NnhBaVlXQ2drNXgzNGdNUGRpZjdBSHNJeW9tcUxyUU13Ti9HR01MQkZRcllidG9UM3hBc2RIYWx3elJ1WlZDdWttQVFJM0RwdkJ5bTZLRFVhQ0ZNVUN1dVcvcEpRQlVTR1FMdkFKVTM2QktsbEJaVVNDS2pxSkVKNW8rclZUTkJHYVh4SDRFQkxaMjBRN1d5ZzlFbVFJaDBHWEZjaUcyc2pCb3o2UWZHeWlhQVpZb1ZNcndOMVdvVkVqUGlibTlqSU9yNlNBblFsb0tZOWcyeEo1eHVTTk5Wb1RTNHZHODA1eEU5R0ZiaWtIamszS3A0UE9aRnFqWmM3RzRtQnZhWWZDTWZob1pPeGJ2SHpvdDduRk93ODlUTDR2Ri92bWZXSW5DbW1FUkJNUkVTQ0tIN3RvMVd5S2lJQ3Q2QWlMSFF4VE5jZUJ3UWo2ZG9qeHFKTzRQUWM0T01yeUNqNC9LOG42R2JFSHdsdnhGc0tyVmdpWjduSStDVHdVTXlIL1VUQ0RmS0REVmNuazkyZ3R4eHNDeFU3WHNQQkY2V2VsSTNTc1NmMUpoWlZvVW9xZWkyS3BYT0tzTE5mR2tNSGdHcTNlTkVrNlJydGN2dFVxdzBXbGRsdHMzVGpwWGtKcnNYS3JuMVNLc1VPQWVLVXBScmxNRmJaMGlETWRTV1g2LzJHaURMVXdzR3BTRFo1aUhEcmtyRDBESXRqQ1dhclVBdVl3RGkxMTU2c3RlREswUVAxMW9JUGVxWVN1YzdEYWppSERZYmFZVlJXRmtGZjN4SmlRcVluZW1PclpUWmV5Y0ZFaWdrNTZmYW1LQVVFNEFVajFIMTFpODhyV21XZzBqbEhOUzlxYmhlcTRjTDhGVHBxeElPdzVHeXdyOThMT3FLRmZ0ZXV4NzQ4dkdvS0tZRDJHWk5TRXQ0Rjk2VGpUblFqaVJpMFRrblY2UGJxdzNWOTE5MlRFcERGUlJyWlR2d3I2S1VlSHRwa0RLOFFlQmtnTUFpN3JrdGRNT1FuSFk3VnZ2UkpXd2tIM2xDOWJ0QXcydW96TVovOXFDNHFnckVPSjhVd0RsTVU3MzVNY1RvZVZXKzBXeHZXbTNXYVJVU2dPSnFkOW5RMVB2QkhjUmEzdDd3QmRaU1lwU0c1bEcyU1FqblJlM3RjcFF6SExiWkMzVGVaVTFKZ1VkYUtTbCtpNG9pVWxkVG5OenZzRU1sbHZlb1phMHVZdmo4dzU3c05Fc0FDQ3dGeEVpVFZibkpQK0lGSzVuR1UvOHUxSk50VXVVUFRFYVZOZmNaQWJVQVV5NERLcmdvZ0JXQUZmYWwwcUQ2VlhSL0ZMMVBCWHNPdnp5ZGRnQ3FFTzN2emFCT2lJSkhFUTBkTDU4aVpjOWdUVThvVGF2K0RneFdpcEpFeVZqODl3TFd2Mk8vT3hjWThLQ2RGQ2dYQkdwaVczUVZpT2hZcmJiV3luSmhvZ3NrNGltRTg1d3JnK1llOUpVQTI2SkJ2MmVDS0ZycmdrM2FQaSt4akV1dGtFanNJakVIVzU0RlpST0Q5L0lXMDBTWnU2NjB5dGZFblc0blNLUEllQVM2QThxZnZtTUtNSW9iYjNPbTdQMytoNVdpMUo3MDY3V3VocnpaQUlCZ0VpU1JUMEk4UW1BRVArMGI5MEowRnNRSGhjNmxldmtNME4veGsydG45R20xdnFwdk5kUUNwRjVlK0VFM05lNXF1azJrQ1M1NG1aUXU4TmtObTVaS203TVlQR3I4Q3FOUUNZRHUzNWJzbDB0V2Q4TlZHaUpTM3VXRW4xeGowY1h5aUdxMlY0QVVQalo2eGMvMTBTRFU3MXNpbnEwbTBUT3RCclU4RlIrZlE1UEticEJNNE5KR0xGV0d0a0ZXcCtuaUQ5UTh2QmhuYnR1R1cwZlRTR2lXTW9oRlpnUjBUSldUZm1VbC92QXZYYU5OYVRzL1ZCbWhSY04wRXNFdGNwekNDeFJXTllaMHZGSWtES1BpVmRkeldQQ1V4d1MvdGlPbjRodHBtOFlEQ2hJZ2VVNzM5OVlaZ05XNnRVVGhxNDJ0UlltclBJVDh4VG5HN2VLelNVSEFDYjRLOGsrVUtJd2IzK3d1Yzlsb0d3MXdoa0tkU1pCcitqTXFCbldhVENzb3d6cldGUm9HTGJvRE5FUFBDOEY2eXdEQWxlcHlLbkJEOWR0Z0pLdUVtaUVHVUlOczNGZDAwcFUrN0NYVzRmTko2eTREamdEQ0w5eUlhQmJ0WEJlTFhKZEF4QUttcW5uWHB0b1NqcWRodHBPd2NVT2Zlbzl2NEtIR0FrR0VWY2JmYmV0cGNsd2lnUzJ2TlZSSXhObFN1Q0xUYkV2SFM5QXRuSitFNHhOSXVweWljR0Rza1BWb2J0VVBickNuRnZqMDRYdVFOMWRMbzBlM2NxamdrTlhaRjlzNHJ0OVg4TkV4V28xaUtEalNtS2JHbktsRU1sUzZ5RzFJZnhQK1VlYlRJSUFuQnBjSUVwYThhNmx0bHcwNTIxa1BWay9hdnhpWU5XRnE4QzUwekFnV0V6dVNkekFqUm9CUHdSa3BJdk9XdUJ2aUM0UXJ5cFF2Q1FhMnp4MEV5ZVAwOFJRei9FZHRZcHRTdlNraThtUW9wcittVFltVnNwdFZmSGZBSUl4Mmp0cVlLQk1yV1NicU01T2VXd1dJdDlYdEdXWjFLTE92cEJ0bDBiWlhtNDIzT1JMVmRyTFpUUnNxVXJnK1R0dmRBSjRkck5jaXNENVRmTjJSejdNWlhGNVkxMCthU0NDSTBUd3dybDI3N0toUGdpUzVyMXBrVEZiU25OY0xheHhPM1J0c0dJVFN1cW1XYTJ4RGQ2S1ZCWGJtWVBUTWRyRjljanJHL21XNmkyVTg0YUlqbjJvZVNXenhXL2Z0aFJaMmRpeUt5TkFXK0tETk1DS1VNdU1nNVdpNS9TV1IzbkwvY05ZWmNzMnBLSjFxL3E4eS9xOVRhR3h1Z2svOWU4dzJZS2xod0t3U2hFS3FjL3A2NUJTWkxHVUc5ZTZVeVpleE5PYjVnVlhld1B6UXB4blVOQlJOM3I4bUs0ankyd3NyejJLaE1lTGJiSkVvSURtdXZMNXo1ODlleWtpaUR1c1ArcXdJZnpkNzcwOU1WMFREUmVVb0FGdlR1amdWcGlNWHRMTEpvaHdIUHZjaWI1QzZXM3QrSVZISjVWMTZNM0lyVFJkUkZ0RFNrS3RlRk5xcFBqak1NdUVwWjgyTFRlN005TGVTSkRTRVc2TGY0VmRhMldmMDdOTGU1YWVYZTEwUlpVM2FjdTNhU1Q3dThJT1dvWE1IQXl6NE5XaWI5NU9GMHlhN2ZwVFVDVHRJTHhxb1pHMk1vc0htSjVYZDkvTXNKWEV2UFQ3V0VRUURLcFBrV2l6dlJQUlh0cVUwM2wzQy9NMlM4VFU2SkdBbzFGZHJJWFRuUnhVbXlJYjlFcGYwZHhTaUdTbnRleUs3UUJSUjBrQ0lCN2ZsNCtoWHIvWDY5V0U4ZFFlS0h3bFlURWJnQ1lSUUt6UkhUcW1RVk9IZjQ2OUluS25nbTBZT3pUM0t0aWpJN0VpSjYwTTFsV0o3QzFpZDdhT3ZrVnZJdUh0ejV6QTlRdVJGMlZtTGtJRGlJUzBhZ3dNelFlUjRSb2ZlUlNabTAxY3kxSkhLL3J6UDl5WTZIU3ZHVSs2OWovY2VPaEZTY0xTWUc0ZG5TQkhHOFZaMnVUU1FVbm5uc3hja1FXNHBNNjZ0bFpzeWx1ZWZJeUlZemdXbC96bUlzUmNTTXFKeUN5SFZmWUdUWnI3MHJpZE9ORVU5REpZOGU1WFg3Lzg5dlh2WGovNzlldkhyNTQ5M2dWYUFzYzhyUUYvcGwvbm1URE0zamZSTFdZNElKNW5uV1B1UG92MkhSM0xPTm1uZk9Lc2ZQUkFWRERkSE1QRmJWaVo2UEp4RklWWHJ6RHJzV1hHcCtZOS9SeG9yV1UrQ0kwNkFwSUVnQ2tVZkgzWHFTM1VIbC9lZFk2eDJ1TTU5WmlHblcvVlY2VDI5VXIwSldMUnQrcHFybmIxSWwrb2pMYllzc2VsMnVNdnJGVHBYQlkwVG1ud0thaWNXVW16Y1NicU9NK05lRTdXR1ZnSjVUaHVQc0t6ZU93c3hPa3BUN21rTXIvZnlGN3BFcStJa0VWOVNYZW5OMDl0Vk5TVGpEZXo2MXd1ams1ZWRHek1hSzNlTkVPYmg3YXltK1lFUS9uN1ZrbG50cmxyN1dqbmtLTGRkdGV3bmRJMWJLZDhEZnV3VHFLQ1l3MU5jWU95MVdNY3BWaWFTSll3R0d5NndKMU5DQ2diUXRqcnNIRnlYUmNnczZXL1ZuSFY2ajIxaloyMGtYUXhRdFdYRVp6QktMbHBXZDB1QnNwbThUY2U2QzE0cVhZa1EwL21zYVhpK0RaKzF5ckxtaytWRy8yejFmekMwbGZNSExiYXA1c2R1S1NEVjcyMnNjbGRxL0c3NlpjKzkyNlJ5Z0FhbFpjT0JZWjdQdWsxaFMxSG9HWUtlT2thaVg0VXZvejlNTm5zSEsyU1QycFlIa2lVR1VZSzVyY1lKWmdyUVFKelMxdXBySWNWQ0JnY2FNc0VwT0pHaTZtM20xUU41Z1lmb1JQZFlvV09jcjRkbmN2WlVVT3AzK1RaVzdLMExTSXBTMGRrYnBFKzZEaDNQcjlOczU1OEdZWStkNEsyamU5a2FOR05FQk9xRnBkTmdHaFF6NG5hbTA0Vk5ER2RLbnBwenRaSWo2MFVNRUtKQVJFbnpycG1CSm5iVWhldDQ2eHR2QU9KM2N0YStrcWxpWGpqTUxoSUtIMFUyUU95eEZMSVIxRzFOakJQR2swSm4vKzcvMjRZTWFVVkZ2UWQ0anNERFBXYVdPY283RTV2K1ZqYmFPMUJ2dUZNU1U4ejJYeUt5YXdNUHYwVVR0VkxsQ21FYWl4VHdHR2xTSWhaMWVqNlQ5WUtvN0NMUXNCblRkUkVRcUJTbklDek5vV1crcmZDSHRmZmpEeXVyOGNkMTlkWHpyWTlsUi8xMVlvQmJqdjAvcHowNVMzN2hkZlo3Q3V2eVJuc3dmK1c5bDB3VWkxL2dxK1RVMThJTTdEM3RuNGZqTzU5UWRuTGN2b0RObHozQjladVhqWS9ZUDArRytGLzNWSHh3WXM5ZkozT2FBYUwyVDBUcjVYWjBZS2s4VUhRSTcwcXRKODBReXJYMzBnYXNZMDVMV0hVVUJRdDNzMHpTYUlpcUZHZnBPQ3hEL29UNWltd0drWXRSdHVIaDBURzZCQTh6K25sSjBQbWtMc0dpa2loSnpJRWlraEJwb1lDa2JSQTNaU09Wd3NXVjdtTWFMU0pmWUhIc1pDZW1TOEI4VDFBWE0vd1I2UnAzakVFazczZkFqbk94YnRoQkZjSG1XRjEwVWxmSVFURXRjUG9qVGt2OUQ0dnV2NmJzU1ZVdGRQWDgraHVnSWhIRzZOc3NkUFo5cHgvVnFhWStNcWJtYW4zUVUzM3M0RzI5NEhDZ3dsY3Vsa1V6L0JzWUlqNkFpalhIWXk0U2VDc1BxUmREYWFGa1N4elZVVmFYbDNvcTVaWEZUL3pHMFhkcFJ0ZWdramJsRkVBVVM2NzdGV2ZsK2dXY2M1SzdDdTlFTXBVc3d5VUZQOUppVWo5M2pNZWNYWUR0TXJRaVc3cEppcTAzWDFkakdIZkhnVFlxZ3dCaW9YL0ZHSHN4WTJ0NVFpVjRFNnlvMkJySU15Wk9hVVI1dEZ5R3dSNzdqNWd4TzI3U2RpbDFFV1lIbkhHQlpBVk0xM2VieE9Kd2JSSWFhUlBJdjh2K1kwd1ltTDZtZXdMWlpPRWIrMWFaMmRxUU1wOEJHU1ZKSk9jV3BSeWIrQVVGRTFnbk5XOUNHY1FGV3hvbFAwU2w3eDVNaDc1NitucTNGZUFHZ1VqVklkaVdNelI5SUNRWDFQUWNQMGdzRk9GRnhYSTk4RTVNZTJXdUlTaTJTNGFSQ2EyRjJnWVU3NWEwNFRNZHhUeFI0bHhicVJONk9nQVFYUWI4YTJVNDlQRWhTWC9MYjVQejBScWl4bEl5Y0N0Sk0yb2lZVVliK1RlMnBqaHdvQzFKS0JnMUYzV3VSQXA2ZVN0bEsvc0htdVpHdUliQ2EyNjJoczF0a0xWRXZ0WTFsVzliWXhNSmZ5MGtKdXlaa0JoaWhldk9kUm1ySzFENVJ3RmlzaDhZUTcwcDEwSDB0MU1WcWlORWpXZEQzeGhKNFhFbTlOdkZGTU50eFdSMXhJbnk3OFJMOHR5NmY2eXMyQkpXRXhrbDZVZmhxZnlrMVdYODZLUVpMb3lZR3FWd1FUd0ZyM2MwUS9YM0JYVXlSSUpwenZTbUdFYXc1d21LQk8rRlVXLzdrMllwaGRmN3JDSFJoVFkrU2d2d2F3ZGNhdVhZWXEzeXV4c3FSZGx1ZndGTkV6cGQ4WGoxeFE3bGtVYVV0azVYMWFqL3pKY0xSdzhYYVE3cW94TDZlUFh2cjRpSjk5bVRpbm1oMzA5ZkpqTlNSOXpwc2VWcGhSOWY2TnVIMS9lVGxDUEx4WDk1Wkw3MWtrRGxDOXBLSmVOaU02RVk2YU80cHNwT3NnT25mRU0wOXNGWVJkZjY4RHhlbzBOY2thZzVHbzN5alRvV1hPU1ZVeFdqTDBlcGxsbFUxZyttb3VTR2I2Nkc5TU1Qc05VS3kwTEg3aFcrNlEybVErSVplRWxDSGVSL1YyTVppZWpNRmVkcVZzM1U4UVZGUEl5WEdrZ2RuS1VwdElBSHBGZFJyeHNSM2NyTmhjdStTYmxibHNpUnBUcjYxQW11SWszRWkzOHNZQndwU3JjQkMyT2hOdy8vUDd2NmUwZzhSalA1a04wRmYzWExJK082MDBtSENWaWRoVkdibHlnTHpWQ3dQYUJpaFhHUkl0Q0xpRm50eXhOTEdkU3hKNlFaOGhYUW1zRjAwcE9tbHI5UkRxTktXVXdxaWR4N0V5NXNNbWw2RnE3d2x0ZzF0WWJMMW1XU0hyVWlHRzlCazJCM2lzRGM0cldvQWF3T2JCNDRFT1lFaFY1Q2VCRWxqd0pHVCtHT2pwc0hoSUJxMmNyOWVJOExtNGEzakYzeVRLOXhKNktlcmRtRXNYM0kraENOUXp5b2U2bXdWSzdxQzBENGZLWUc3ck1XSHZMdlRFUW1nQmlNekEyQ3N6MWx6SElTY1F2MGtEK2h1QkxFN1I4U0p5ZzZ3WnA1QXhNeVhRWk5Vc09mc3FHYlRsM1k4eE5FVElZWW5OUWpaQXhSVVFoR2FrUGhoSU0wN0R3NmZheWhSSm9ndDFidWxwRzJuTVJYaVBwR1o2OWhLL0lDK2plbWhjUitSa2F5YytPZUdNa2lkelR4Vlg2NHNTRjdNUmk5UGF5V2VnRCtUOU5jN2xsVDJ1NkxieVFjUnBlV21mZkJuZ3BPSDlScFlsWW1XT3hwbHF3S1I2Um4rQWlHcC82ZGEwZGJGMDkrNXJ4WUhGdGMwVFkrMmFUcGs3Q2dMcHBsQnVHc2hGZFFWVVRCRVRTSjcwbGsrUks4U3F6NHFzZ096V2l6NXduczlBRnNmUGxOK2V2YTNJNDQzdC9lUlFmdy9tenBKV2greHBReW9LbWVKZk5HNU5yY2hjRlJaQmN6UjNoOWgrckwrWjZsNkhlTWE3K2ZkdWczT3ZrNFZ4V0ZVTHF5VGJTcUJJWW5vU1hHQzFhYi9uSVg4VXBHOVJiUHVyZlNhRzh0YlBEc2pmRE5iNmVJbERQL05LYnUzR3pFcDF1TjdUajFGa01OaDN4TEpXWmRYS3I5Z1hDaHIzOEtrSWRQYU5zTlpNMjRad2hzZUttZkVtS05MYnh2VW01VVNrdmY5OU92ejNhVFY4OUM2UVdqdERaWi9CaGxzejlzOC8rSDRKK3VHMlkzUUFBIgopKS5kZWNvZGUoInV0Zi04IikKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMjciIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMjdfbXRwbDogYWxyZWFkeSBhcHBsaWVkIikKICAgIHN5cy5leGl0KDApCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShQQUdFKQpwcmludCgicGF0Y2hfcjI1YzI3X210cGw6IG11c2ljIHYxLjEgd3JpdHRlbiIpCg=="),
    ('patch_r25c27_home.py',
     'web/templates/landing.html',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMjdfaG9tZSAtIGhvbWVwYWdlIHYyLjIuCgpyMjVjMjcgZGFzaGJvYXJkIGF1dGggcm91bmQ6IHRoZSBzdHJlYW0gcGFzc3dvcmQgaXMgbm93IGFza2VkCk9OQ0UgYW5kIHNoYXJlZCBldmVyeXdoZXJlLiBSb290IGNhdXNlcyBmaXhlZDogKDEpIHRoZSBwbGF5ZXIKYXV0aCBwcm9iZSBzZW50IG5vIHRva2VuLCBzbyBpdCByZXR1cm5lZCA0MDEgYW5kIHJlLXByb21wdGVkCm9uIGV2ZXJ5IHBsYXk7ICgyKSBhbGwgcGFnZXMga2VwdCB0aGUgdG9rZW4gaW4gc2Vzc2lvblN0b3JhZ2UsCndoaWNoIGRpZXMgd2l0aCB0aGUgdGFiLiBUaGUgdG9rZW4gbm93IGxpdmVzIGluIGxvY2FsU3RvcmFnZQp1bmRlciB3em1sX3N0cmVhbV9hdXRoICh0aGUgc2FtZSBrZXkgdGhlIGtpdCBnYXRlIHVzZXMpIGZvcgoyNGgsIHNoYXJlZCBhY3Jvc3MgSG9tZSAvIFBsYXlsaXN0cyAvIFBsYXlsaXN0IC8gT25saW5lIG11c2ljLgpTdXBlcnNlZGVzIHRoZSBwcmV2aW91cyB0ZW1wbGF0ZSB3cml0ZSBmb3IgdGhpcyBmaWxlLgoiIiIKaW1wb3J0IGJhc2U2NAppbXBvcnQgZ3ppcAppbXBvcnQgc3lzCgpQQUdFID0gZ3ppcC5kZWNvbXByZXNzKGJhc2U2NC5iNjRkZWNvZGUoCiAgICAiSDRzSUFDdzR1bW9DLzlVOTI1TGp4blh2K3hVdHFGWkRlZ2tNQ1E0NUhPNlFzdlpteTZYMWJyenJXMVFxVlJOb2toQkJBRzRBNUl6a3JmSlRLazk1Y0Z5dWNsNVNlY2xEVW5sS2xmT2VUOUVYNUJOeVRqZEEzQm9BT2JPUzdkbWRHUkI5TzMzdTUvUmxyajk0OXVycDIxKy9mazdXMGRhZFA3aitRTmZKTC8vK3hhZS9JdHdjV2VibGxLejlMUXZvaXBHZGFaaEVKN0huK3RhR1JQNkdlU1JnUEhUQ2lObkU4UWk4cCs2YnlPZFkyM1UyakZEWEpkZzJKTG9PbmVNWXhLWGVhcVl4VDVzL2dEZU0ydk1IQkw2dXR5eWl4RnBUSHJKb3B2Mzg3UXQ5b3VXTFBMcGxNMjNuc0gzZzgwZ2psdTlGeklPcWU4ZU8xak9iN1J5TDZlSkRENkJ4SW9lNmVnZ1FzZG5BNlBkSTJsSmZPdEhNOG5lTUs3cm5iTWs0aDZLc2U4L1hEMityRGFJMTJ6TGQ4bDAvMytiRFBuNE4wL3FSRTdscy9qSU9IWXY4T0Y2US8vMGZ3UEhMei9SZlhaL0xva3EzTmdzdDdnU1I0M3U1Ym4vdHgxeWczUGVvUzZpdEx6bGpaQ3Y2WFVPLzMvN3VEekR6TUtKZVJDaVBnREFrY09tdEN3OGhXWEovUzk0eWw2MDQzZlpJR0hGR3Q0NjNJdFN6aWUzdmdhelVEb252d1l0Ykl0RnBGR1ljY0I4R2oyNW5tcithUnJjQnl4T0JMVUluWWczMWNhSzVCZ2RzMURkUkkrRVQ0S2xiUklTWWR3OEFaZ2RVQUJJT0lMdU90eUdjdVRQTmdiYWFlSWxmYTZBbUlKaEdkT3BzZ1RmUHc5M3EwYzNXN1QwY1BvVkhBbzllT0R0YlIxRXdQVC9mNy9mR2ZtajRmSFZ1QWtteDhwbGdwU2YremV5c1QvcGswQmZmWncrSHo2RUh6cXlJU0k0OHc3ZGt6WnpWT2tvK2NHaGptc2JvakN3ZDE1MmRQVFNITDBZdkxsKzhPRHVYelFNYXJZazlPM3RKQmxmR0Jia3dqVEY1U3N5KzBTZWpFYnlBeGhka1BNYW5LMk5DeGhOakNPWERTeWkvN0JzRGNqRUVHUjBEdjVPTEVUeGRUS0RTVTNKeGFZemsyOUZRdGgvTGowbDcrRFhHOXRESkNKNWdwQkdaOU9GcE9ES0dLYXdlSVBvTTJRWUVYMEErZkRaQnlKTlhlanJwZ1RFNHZBTWFNSXNHc3pQdXg1NWRlUDJWNzNqcCsyVHlpRng0cWhJd0FLejZIblFGWWkvSmg5UUpnVHhMNElyUVdQbit5bVUwY0VMRDhyZDNhQThpRXptV2FFd3M3b2VoejUyVjQrVTZLckZQMC9qblZoaWFIeTlCdU56YjJVdXNBTXFEUnRNOThNRVBML3I5eHlQNEhzUDNKWHhQK3YyUGtxcHYyU2FLdWF5V3EvS1I3WVFveGJOd1Q0T01qY1hFd3VqV1plR2FzU2lkdEhnelA5U2FjdCtQeURlSHovZ2xsQlZvUnRSY1UySlR2bmxjS05mMXhXcEtFaFZXTGdwanZxUVd3L0lMK1Bla3BsdzNzY1lsL0h0UnJvSEVueEsrV3RET2hka2pWNk1lOFBla1J3eHowaTNYamRoTkJCMUpHU2tYYm1Pd08xRDZkUGoweWJOS0tiVXNVQmRRTE5sVVhTekFIRDI1ZWxaYklmU1gyRWwvUEJnTkszVkM2b1ZUY3BiUitLeEhkQm9FTHRQRFc3Q0txR2JGYnoxMjRCRnE2MURQV1piN1NTZ01YVWtlZ0c2S25iYjA4dTdCNGZFSEpXSXYvQnM5ZEw0R1BUK0ZaMjR6cnNPcklnQmJ5b0hacDZSZmZCMVEyeGJ0K3NxUlBsODd0czI4TDBvREh1YUMrb0o4NEd6UjVvSkJVdmF4OE8zYk1zRFUycXlFV3BpU0hlVWQ1TVlTWHdnR1RrdVJSVXJsS0pTNkZLcTBGbUt0VkF0c255NlY4eFQxdDcxYlA2NEJSQmQyQW5pVzJ1aFRyUEEzOEVabkFNMkNHeklTUDJsRUpxT0hSQi8wSC9Za2Q0OEhQVElZSW51UDRJY3hHSGQ3Sk9JQVNFQTVOQ2ZqL3NOdXJ6Q2trT3ZTSUZlaTk0dHhNa2IvSWFrYm9EOHBEVEFhUGV3cTBXN1FtTk1TM2dNZjdEZFkyaW1vK3h0bWwzQWhPUWRoaW9IalIvMkhKWXc3YnNTQUpBczM1Z0J4Y0ZQQ3RSOVF5NG1BR2taSm9RUmdCYUNsem5ZQWNDaVpwbGpqYTkzeGJIWXpCY3pXejhXZ2c5SjBoRFdhQXQ1MisySi9LY21ySlM1RFVkY0hacmtnOGdOODN5Ky96N05xbVd5V3d5MlhJY1dHd0JYdzNVczRVZXFWTWlzVUtJVmYxQU9tay9Td1FkYWpBVEhISVdFMFpJQVAzWThqY1BTVzZPV2lrdzBZOUdqRW12QmoxdUNuWDR1ZlNnbVhCUXBFQ0FRTnpYb0phc0lQaUFFTVZzS1BicDZNSVJNZ3VCT0dmcmhodDB2d2lTRktTVkQ5VFdsMnBSZmlKY0syOVBsMktoOWQ2TDF6dGR1RFJBSWV1a1FFSGgzd2hVWWxzTjhkQTRKNVp4RDBDY0tnWHhWZ09BWUVZODlwVUJwbFMyLzBoRkd1SmlEVk5XYUQwRGp5YTJ5SDVkSnRBRENZSEszaEVJSER4NjZ5T3BpbUtQSmhQZ0M0MWNGNjVCRmgzcTRUMGlYVGdSRW9rQmJDdzZSZWo2Q21VYXU0OHg4Z1Z3SUxjdktEODJ5UytLN0dYaTFkVnBvZ2RaMlZwd01EYlVFeklWY3lYcXl3b3NEM3hpWEFxY0xNWVRhbW1MMWFPaEVnQStwdjFQSTV2Q2hqUFpYUGFrbEpTVitWeTNHQ1pRMWJBV1hCTVJ6OHBzMmVKbGhUR2Q1OUFpQjZ0OVZTOEVYQWtrTHNNYXBneldVUm1vSVFiUVd5anRFMzY3QVdRRVJTNFZXQmM2bkVxd3g1SUxQam9RT3EzNVhhbDJXc0hqaDlEQVo2TUd3anlsV1ZMRDY2cVdPTUlrM29vdFpmcWFMbm9vekJnc0lWRG5hUHdQOEIrQW5HcEt5SUpHUkFDeGcwOUYzSGxrMHVvWUZwbXFqSDBDbS9VUHRlSDA3R0RQejhraU83WjR1TkE5SUpZTmdReU9zRnQyQlFkUXVPcWxpaHUyRlhRcHBFV2laMXdqSnBJVXZGb2NsajhzTUxhck5Kdng0aXcxOHVLMjYzNkQvQmxrQ3NjTklHWmovNVVZdlpwVVZIZE5ReW1nSUhCWkNYazh2QjVhQk9NNjRaOXd0cWNlT29oVjVLcTNGcHRndXJhWmFyb0dldTU0eFVIQVNNVzJDZkc3ejVnL1UvVnJIa0pyWWUzRXR2eWJsS2UyVWFBMkd2THRGZURZMXgxV0lKSlhJSUgwQ2RxVzJqZ1dvT0xHVEpTT1JwNmpKcXF5TDBGR1FSNW5hYlZLbXBNRUFId3oyOGFETlBBekUvTlhCV1JJKzNsOUljVGlyallVMGQzWXNwd1o4dHRySWVtRVhrMVFGelQ2MXVWSzFScWg3N3ArcnpnMEVRbUtoZ04xUDV3TWZFdUJJTVVxdjByWmlIeUFwSm5LUVFNSnRaUGsvODRHcjBKTVF2aWVzT29naXhhT0lwOTVMSUxYdFRpL2twdFNKbng4cU9hU2JlMHRzMHJpNjd0WDBZQVFlZm5iZUgvRW1RVktNZ2w4dldFYVpyVFBDWDlVRnFaVVFrNDdFd1JPZlliSUIzdGZiREpqMnJqUFBON3VrS3JtcU1aWFhrNmpiNGxITnRnOUtzZFpzeEZ4b1dySU44VXlONUsrNlVrZ1g0UmdlQkN6QWdRUk1ZYnpGTnhsbkFhTlJCMXd5WFlIcVlnQUU5MVJtTXdONERiRXZlN1I2blQwNVFHUWg2SzdjbE9jdTdrVVNoRkFZWDlUcEJ5dnRBMkpjR2tJM2RlN0JsQTRWYU84cVNKakJzN21HV2pNdEo4OWpqY25oUTlXL0hkL0FvaXJ6TUdVcGJiaEdzeU5iY0NaUlJiNG05SmcyMGdpN1dkNHdvRndBNE10T2RtTDRkcUxYNVhsaG9NR29iaURheFNaTkRsM0RLNU9JRUxqM0M3cWxBVk9ySVNqK2dFeGd2a3FUUzJZbDZVS283MTk5UGlkU0VkWFVPZXJLcUZvOWpFSndnRHFUZlRPc1RNaG4vVkpFZVd0eDMzUVhscWVzWXJSMnZKZzZ4S0xmck1MSEEvUUdQLytJNk44WEhsTWlGaXBOOXA0U1BIUS9DSlNjNnpxOGFwWDVWUGdETTNqZGdVODJoaWhUZnJ6djZzQnBKRitMTkdpZXFTa0ZEN0lPNHYyMEhMc1h2a3JJTEEyWkZ1a0F3VktubkNLUXVjRjIycmpJYzJXelZJeC8yNllCZTJMMTArZTJZcVJqQU51VmthVDNReCtERzJhN1UyWVpCdjV3MXlDMG5sVXI4eFZlSUMvQjdVQS9zMkRFakcvV1dwVW9ZZUcreHBqam5SSzA4YURBd0NaU090eXhqT3ZOejBDV1RVVzlUN0p2MDVHMVZwdXFZUktJeHFTckMvUnJRSVB3SWhtSmREVG1QMEF4WkZlYTZUaEE2WVRQQm9udTVTdU02YXk4V1Z1cDlSZkJ3bHVCVHg1d1ZQSnUxYVlTc0F0TGR6UDl3MUpvK3FLVXV3dmErNHdiWFBRUU81cmc1Y0xocUR4d21qYkIvLzRIRHVEWndrQkVER1JqVlpFL2VGcW5OVGs5bG8rcm5yUTRvajdFd0RUYkxyRTNxQ2xRYlM2YytBUWxzMk9vUGo1cEp1UjQyNURldjJ0M3RZWFAzd1gza1grRUFsOUtMb3liTStRRnJ5NHNwWExMQ2VvbFI0NExYNTM4TEducHloMWp1cUt4SVc2cmw0cjJKWGxNZWJ5QldJbzFSVlJVZTRVV0NKRVdPUlYxZHhINmdkUExqcUlsNXQ0VE9wRWE4a3YyNjczdlY4OFEwNzErSDhoemVKNWVVSU5LSVhib29LeE94dERxb2JqQktYRVd6MzY4amU2SFh4UWt4VlZsRG1jY0JEakxwdlVkbHBSakg4WUk0cW5VTXBVdDRWWmR3ejhqWlY2L1pIY2NhNzJjVEdTNE10dWZvL1RpU2V4cXJ3cDlqQVJIUHQrTnR1dlN0T0x5TDdWWDB1SWlCTnp4bFoyMExHd3IwM3lOTFgyS0JpenBwekMrTTNHMWRwS3BSUTkvL2pzM2orTjdtY1hDL3RWUGxhbnpGWUpSVzlnZm1uVFlFREVmM01LVFNqbGF3ZGF5TnhLVnJmMCtjaU94OXZpbG5jMW53M1MxU21PWjNHR3NnNkg4RHNVWUZaTU83YTRoNWNvcjR4QXh4VmNBdUdtZFNpUTRPNi9mRFpQMStPR3BVQVZlalpsVDlSZUlERUJlNndJMlFqNkE3SDVkNTh3SWppLzZxMkc0bzJhNTF4MEk5WDhwSnZhK2xqM3NFblJLUSs1SDlzbzNzNDZheDc3d3M4ejdXWFJJUTNzdXl5N0o2aENWaEtwRWVPNElIMDhVUFdWK1JRcmxQT1BSVkhFYk84bFpQRHFoTmljZzg2Z3NXN1ZrNXR5aU14ZUMweU9rMG5xbFBVeUFXallpdTdzc1c0OXBGNHcxekcvUkpPY3QvMVJkSi9xS2FnV0QyWWVrZFFJUDc4eW8xTHlzN3duTW5OaVE2SU94NnFFakc1M2FPaDJ1QUNsVk9tTzBXbDRDZTRCMnJOM0xMcm8vWXg1MERPenVFb1F2WSs4ZnNIZDh5MjZHa0U0aFRxcUhPbVIxYnpOYTN2dXhKZnU3Q3dJVyt4TG1BNHZFVG1jMHV2Vk1rUWtzNHJJWTk1YXhrdFVaK0p2anoranc1dUhaOUxvOEVQN2pHczBISm9UYmIyZUhHdkRDY2FlTGtDaDFvaEhLSDZqSi9QOU1pSGpOdGZuME9GZXVhbUUxTmtoT28xUEhTUmlpSFduYU9UaHhVQmsyV2xJSWl5UldLQ25oME5DbkY3ZHhhY281VXF6OUhxaDNPa1dxNWM2UWE0YjdMWnBxelhTVVFROVNPWi8zS0IyWUxvNHRqcDlpUlJtN0ZUM2thVXhNZEprZFE1UWNPdGZBSXFpYVBkV3JKNFRxTm5DdTZUWTZqYW45RngxRzFDcFRKMWk0WGoycDdURXVQcUdySnlUK3RlRUJWd3dPcVd2bUFxaWJrVDZzY1VFM2ZsNUJ6amZRck13QW1WaElPRUx2b3RlekVOOVNIMG9ZR3VMRlhJNDZkUGtrdW5SZnEySDZFREN0NmtnVnAvYmMzVUdLdG1iVUJLL2Z0Ny80OXJWUWFWY29XNC9OTUUxempRaEhJYURvRzdnd3VjM1pPbEhDZnNEWlBENUdEVmtUZmNzOFdZb2NONHprQnpBUm5NQytlMkRhdUYzeitxblJvRzBBYmxCb0c2YUM0UVZhYnYyR2VEVm9uUGRjT21qUmFNMUlBQlU4dXJCaitQcHlDVDNmK2lLUHhRb2N6bXl4dXlYNU5vN05Rb2JSZ0ZNQmhEK1FIT04rQ29UMXhJajhVblNmcnN1U05PRHVQY1Nnb0RBUmp3ZjE5Q09yQjU0ZVQ5QVNqeCtKaCtzcHdQL1VCQ1dFUDlDT3FTMEU5d0VSUWozOHJvaXJwcHdlK2l6eVM3SUJNVHpxZkh6WS9hZk52Ly9qbmo3eEZHRHdtcjNETlFORGtVSHg5VHR1NkZ0c05EeDBMYWtLbmYvcVB0Rk9SVFpGVVByVzNEeUd3MStZL3prWDNsUjVLM0FYY0xYbTNnWjNGemtVcFY4bGpYcU4rNW9DYzhsdTU0N0dCNjdFY0pDLzNacGYwK1RxSFd4UTdCTEFnTDlyOGRZWmZVYW9Ra2FPR2V1TjdxNFpoUlBFOWgzaUNDcVp1Z0x5azNYT2NYK0JOR25Yai9DejJQTHlWWWhFN3JxMGNTRW40WENjeStTbkhrczlQd2EwNTZOUmFnREU5cnBLdXhWemN1K0VtM09LRWVOSEpoZ0Z3QzBWdG9YSVBSQWQrWTNpcFJRUVFTODJENmtMZXZRRmhTUmdDcTl0Q09SMnVWU0VoNkVTOFZnU1BhVEJEYVRvVW1KZFpkekhuMTN1TjRQVWNZQnlTRVRTNVAyVHR1NkFCWjlxYjR2aEZxU2dYbHNaSmNzcGlvQi81NlVEeXJUYi91WmdGWUVaOHp0TXM4N1BVZ3Byc3haUmtrL3MyZnlrK3Q1Rk5idkZURVc1dHpuOVcyZ0FLZHNaVTZxV3FzdndGT0dmaUFwdHYvK0gzYmJwSUNWTitLdHI4Rk8yVko4ZUxaSU5IbVE1cjh6QVkzbXN4Znc2bTZSWjM3SzFTbzdSR3Y2TTgzVHlVWW4rR0NuR2xPb29xbFdvT1NQUy8vSnNDTFJuQXcvbW5OWGZUQUpqRG1sWkIyZkNMV3ozQXpxSlZUcjBBVUVrRytSU2tMVVF0S0dSdXlTSndpTVMxUkFIbFFFYm05c0FPMjNIQXNEZGJPWnBZemNtY0JCZlBPd1IrQUlxQk85RnQxVFRYY01KOWNQakhQN2ZnOEpQRWE4cDdYUTNZKzd1WXhhd0h3V2k4WE9KNWE4Q2x6SEhETzVleGdFVE9sdkdlNkFvRFVSSUdqTms5c3FRN0h5Yk53RHNKSVJxMjFxSnA2SFBwU3FGN1ZJdENKSW00TVVwY2ppUlF5U2owa05MN0xKU2VGSkwwZTBIcWYvMTNDMUtmcGRjZk5lTHlSNXd1Q0NVaHlCaWdFbm10QngvWEZBTjNCeTBDb01xVkl0eERGa1U4N0VIbnNzd05CWGJFYXpFaUdpUWVLVjR6UTkxYVZQNG1waTZ3bnJpdWlXSVVoeXd2L0xiRXAvd2UwUGQvLy9xSDM3Zmc3elYzZGpSaVNHbWJMV25zUnMwaUxVeE1LQVZzd2RaNCtwQm01aERtQjVpTWlqZFdrYzYxNWR0c0Rub3p3cHJYNStKajF3QXZPbEY3WVMwYWczamhRaVFHTnBhRm9lZ2ZOSUx3TmI4ZkJQN3pQN1lnOEcwU2VFajEwOHRISG8yWXpCeU5wUzltOUJQSGYwUHB6anRMc0p0R05BUnJKV0dOdkdSTE1MQWNvcGNvVjNpd1k1bVZyY2ZsSVJENnZzUVgrTytmV3RDWGp6eHkxbHhHS0dsZnVMVkdtNHZJSnpIcExYWW5VWHUzRWxNZ3VzQzFybENZb0M5ajhBeWNTS1FOZmNtcEtlcWxwanlFaUtJTVBNaTM4WUtoME5jaVZ0eVlsMVo4S1M4MzJ6cGVIQXBGQXNySklDOHlyZndicWRpemk5c2N6M0pqc0hCSGsrUVVqd1E5R1l6UUNxNUpQbHByYzArS2tWMlRWeUlXY2x1OEVxeHpCUGNBdWQrOGZmNmFERnJZcCtoaU5ITEZTMUFnaVdrVE9RZkF6VUZEQ2VHcThWU1FKMURyYTArQkw2TE1JbWgzbHFBVGNXQzIrV2FSRExra3Q2WGdOZUxpYllJRDZXdkpodElESzNoZDZEU0VNcUtwT2xWU25hQzJDMGtjRUlaZWJLMkVJQ2EvTDN3TjIwd2VZQ2lmNzJuRWxGQTZlYndtekpGUEQ0blFyeTZ4SkoyRkxMa2swRnlMSjdDbnpzNnh3WGR3Yi9FdVRLQ3N1eEV1Q2NZekZMbjJCTmZoRG5rWHNmeFhWQmFmaUZjVkxURVg3eFVhQWRrTFFtMjgxaEorMGV6YVM2bmtiUmE0L3UwV2d6dkVEYTFtQWpNSEdXd2p4N1NZY0ZIaWtHR3NRSGpzNFdXWDFZVU5DTHZwem5GdmlSV0hrYjkxdmtZUEZpV0QrTXZNdHFRWEFLNUE1dU9GdVBidmplTUNPTS9ZMXZmZVBEdVhGM3lxczlVUjVTdTg1UFRMaFV1OWpTYnY4dk44TkZBQXJlZG45NDJtMTRUU09jcE9ZWVRFRUhzUWFrSzA3ZlB3TVhMTWcybzhJMTFNdFBwNEJxVW5ySmJ0V3pFaWo4cDRVN2oyTWJlWUZFL0JmTUFtRUNuZ2dwVFBTN0dQa2dXU0xRYXA0ZmI5Q3JYenlleUlybVJ1REpuZDlXUGJxTTJSejU4QTlvV21sZGpBNjFPei9QY3l5U2I5b1RicExRRkwxb1hPY1pFblhmQ1IxNHZPOHhzbWNuZmZqcVlTRzVqSWlzSGl1dmwwb1ZUb0lLRXl2dGV6STZieWxHQnVwMFZuR1h0U09qcmQwa3FhQnR3b0dvQ0pMRzJKb2h4UDhhM2YraHN5STVwVzNtTi9xMXFUTzlUUDM4UnJBS3Q5R3JGdFI5dC92WFcvbENybVM2eXJkWlhjK2R2Zmd2Y2RoZ0N3b29mWDdpZWlKZFlxUS9XT1dCUXNBZW5nYW1OK2lWS3MwS1JJWUc3SHdYTE9vcGg3QjA3RVFaNjdEQitmM0g1cVE1M0g1UzZTdytuZ0VJbDdZQjRKVVJkSGxSL2hGaktFT0k5M01TcWFwbzZHQ3g4dWdOMkRjUzBJUmRtVTRMMitJTitjYWVSZDF3Qktlams2OGE0Q3ZRbkEzQUFVZnd5L3ZnSjlCQlNkRWc5OHdSSW1LajJ1VlQwaWtjVlVab2dWdWZUVHJTNmxPa3ZTK1VEWmdZaHQ4RTRZbVNTRGZwYlVEUldyc1llS1F2dytBMFkxcUcxM05IKzVWSTJJWHlsRXVMZ0UwMkUzMFZPNXd3SDVNYUZEN0cwOE1FbGFmUThpcVZ4cER1SmEwMFlpdVZyMlRvazhINmU4Tm9BUHZ1UXNESHdaNnN4bU00TEx1OVZlanNKVkNVK1J2NEp3WDZLcVJ6N3dQUVcrbW5BRk1INU1OT2tyWXBTaUFjZklqOHVsK0t6dVRvazQyZGVycko5WHRYMGdlbllDTzZsb2dNUUNxb1FsVXdodnltaTdPajRUUUtHeUxRTzFxNmYrOHNqNjc2QjJ5RTRkdUlHTmFnZlhoSUxYMmhnTUJGam9zZ2JkZlJjT1AyVXE3NERQeXVxdmFJSWVWZTgzNklnWVJLNGxkQ3ZLTUowTHVwQ2ZPUXZsbEZLTlNRTW5sdzJIc1RxcGVRRU8vQmlmWjVxNG5RL1RNRC8vMmFkUC9TM0lIMjdzU2VxaFp0UzBicDNLVlpMdEtEMmM1MVp1SkpvSVpmNmlQMmlxbnhJZ3R5YlVQVXB4Wm5sTjhNYUp4L2JrT2VjKzcyZ3JjQ2p0T3YycDFscnRnS2cxMXgyTVVNWklEOXJ3Yk5maFRWZ293QzVVSVI5OVJHemp3QkxDQS9qOGl3Yk5ueTJPbHJuOURYZzgzcW9UaEFiNHN5dGdXTlJIS0FFMXFCUUc4RkM3aWNaaVlMbFVlb0syYURJODlXUkUzTWh3ZDFiZXFKVnRuT09nVWFHZUkrckFyMnR5bUFkOGZQU29tM1R4YUFZRm56dGZRQ1FSQTdpQWtINERhdFV6bEQzTjhWSXE0STNPU3hxdERiRi9wU05Mem5GL1VWZjg2b0xvYWhzMElRa3RSSTBhOU9NRXhEMHAwazNKTGM0MU5KQnVjTDVGVSswbCtKbFErZUFNV2lKTDhpejUrQUpLOFhlbjI0TG1qVVR6QnRBc1pyOTF2SXpMZW1RQ1B1VUdjVjdQUUlMaGlTREc1b3ZIamRWb0ZlREVnZTFvdEVrclVPbGQvQlR6UXNDWFl0ZWQxbFFkUTA2c2VWREk1eldxTnpERW4rWG9OZ051K2JzRzBDSFkxOW82UUpZUDBnc1NHdlJBS3J6V1FkSlJUNE9XaHFaRkpDUjk0ZTBORGFnUVhrS2h3OXF1Mm5yNXBoYUdZSzA5Rm9WbC9mR24vOVFlTjZqMEF4ZGFrZ3V0UEJkYUdSZGVBQmRhelZ5WUl0clpOaEFLZHdoMm15MldzelZDanVCWTRlZldGNjExSVVaU0JKdUtpdWhBQ0UrYmFDNzkrcmE5QWJBbm1rdzBKSm5WdWNtRmdRVS9pOXdZWWp1b2tlekx4bkhFM2o1QVAvenZkcHh0eThTUmZEUUltR2MvWFlQRDI5emdYWVBvNVR1QlRsc0VReXdHM0ZtMHNIV1JJL0dOMWp5a3Q3MzdnTjYyT0p5MzFSb3JGd1VpTUVSbUcyMTNhdXBiWUxXaXU4TnFSU1ZoYlJvTUtwZGhsWFlWM05qMFVXZ2lkR2FGM1JUUmxMU2dyU1RLODRTM1BhVzJGVFdhaFFMSFF0dUd5bWd3Qy9WcDl4VGZSVmpuUW52c3NLWUx0UDJ0anJJaVltSjFLZzQxZU9KM01IUXJtYkdWeXlwZFFaWFVyMjcweVZwY3pKYlk4RFFmOFYzdEJlcGkzUzZOcGtyeG1uRHhmK1RESU5TMm4rTWZPTUI4QXFaNE81cmxPdFpHNjVIbThGSTRJL3ZFaWNKdFhWMWpSMTFWZUNBOTVIMDl5dkpoWFQ0RjJLc2gwWlpGYTk4R21Yajk2czFicmFlc0kzY1RoMU5RMkZxQ1EvMHQvbGtvYUlaL0JjV3hSSHI1SEdNVUNQblVuZUEyL3luNXladFhQeFgzcTNrclozbmIrZWF3R1dDS0NGQkVNT3FjWFJZZXliam9zYUtlM2NTVlNaUWp2YWdHQTUybFdwUEs5WklxY3JYRmpHeFltNUh0cFQwajZLVjBhdTI2Y2NaL1NnWFFsRnM1OEduR1hDMGVRRlk3dDZWUUpGWTRycElmdGcwZXE0M1VtWmFxeUpVK1oyQlVoV3ZEYm5HaHJDQmVTbDJFRkdjR1ZKZHE1emtlN2dLMWt3bXVFTk5PQXlqZ2l5U2Zycy9UMVlUcmMzbDBCYyt5aUQrcTkvOG54bWJ1Wlc4QUFBPT0iCikpLmRlY29kZSgidXRmLTgiKQoKcGF0aCA9IHN5cy5hcmd2WzFdCndpdGggb3BlbihwYXRoLCAiciIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBzcmMgPSBmLnJlYWQoKQoKaWYgInIyNWMyNyIgaW4gc3JjOgogICAgcHJpbnQoInBhdGNoX3IyNWMyN19ob21lOiBhbHJlYWR5IGFwcGxpZWQiKQogICAgc3lzLmV4aXQoMCkKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKFBBR0UpCnByaW50KCJwYXRjaF9yMjVjMjdfaG9tZTogaG9tZXBhZ2UgdjIuMiB3cml0dGVuIikK"),
    ('patch_r25c28.py',
     'bot/core/startup.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMjgucHkg4oCUIHdlYiBzZXJ2ZXIgcmVzaWxpZW5jZSAoZ3VuaWNvcm4gd2F0Y2hkb2cpLgoKVGhlIGd1bmljb3JuIHdzZXJ2ZXIgKHBvcnQgODA4MCwgYmVoaW5kIHRoZSBjbG91ZGZsYXJlZCB0dW5uZWwpIHdhcyBmb3VuZApkZWFkIG1pbnV0ZXMgYWZ0ZXIgYm9vdCBvbiAyOC1TZXAtMjAyNiAoc2l0ZSA1MDIgd2hpbGUgdGhlIGJvdCBsaXZlZCBvbikuCldaTUwtWCBsYXVuY2hlcyBndW5pY29ybiBmaXJlLWFuZC1mb3JnZXQgdmlhIGNtZF9leGVjLCBzbyBpdHMgY3Jhc2ggb3V0cHV0CmlzIGRpc2NhcmRlZCBhbmQgbm90aGluZyByZXN0YXJ0cyBpdC4gVGhpcyBwYXRjaDoKICAxLiBSZWRpcmVjdHMgZ3VuaWNvcm4gb3V0cHV0IHRvIC9rYWdnbGUvd29ya2luZy93c2VydmVyLmxvZwogIDIuIEFkZHMgYSB3YXRjaGRvZyB0aHJlYWQ6IGV2ZXJ5IDMwcyBwcm9iZXMgaHR0cDovLzEyNy4wLjAuMTpQT1JULzsKICAgICBhZnRlciAyIGNvbnNlY3V0aXZlIGZhaWx1cmVzIGl0IGxvZ3MgdGhlIGd1bmljb3JuIGxvZyB0YWlsICgrIHRoZQogICAgIGxpc3RlbmluZyBzb2NrZXRzKSBhbmQgcmVzdGFydHMgZ3VuaWNvcm4gKDQtbWludXRlIGNvb2xkb3duKS4KU3VwZXJzZWRlczogbm9uZS4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJSMjVDMjgiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMjg6IGFscmVhZHkgYXBwbGllZCIpCiAgICBzeXMuZXhpdCgwKQoKT0xEID0gKAogICAgJyAgICAgICAgZW52ID0gZiJXRUJfQUNDRVNTX1BBU1NXT1JEPXthY2Nlc3NfcHdkfSAiXG4nCiAgICAnICAgICAgICBib3RfbG9vcC5jcmVhdGVfdGFzayhjbWRfZXhlYyhcbicKICAgICcgICAgICAgICAgICBmIntlbnZ9Z3VuaWNvcm4gLWsgdXZpY29ybi53b3JrZXJzLlV2aWNvcm5Xb3JrZXIgLXcgMSB3ZWIud3NlcnZlcjphcHAgLS1iaW5kIDAuMC4wLjA6e1BPUlR9IixcbicKICAgICcgICAgICAgICAgICBzaGVsbD1UcnVlLFxuJwogICAgJyAgICAgICAgKSlcbicKKQoKTkVXID0gKAogICAgJyAgICAgICAgZW52ID0gZiJXRUJfQUNDRVNTX1BBU1NXT1JEPXthY2Nlc3NfcHdkfSAiXG4nCiAgICAnICAgICAgICBib3RfbG9vcC5jcmVhdGVfdGFzayhjbWRfZXhlYyhcbicKICAgICcgICAgICAgICAgICBmIntlbnZ9Z3VuaWNvcm4gLWsgdXZpY29ybi53b3JrZXJzLlV2aWNvcm5Xb3JrZXIgLXcgMSB3ZWIud3NlcnZlcjphcHAiXG4nCiAgICAnICAgICAgICAgICAgZiIgLS1iaW5kIDAuMC4wLjA6e1BPUlR9ID4+IC9rYWdnbGUvd29ya2luZy93c2VydmVyLmxvZyAyPiYxIixcbicKICAgICcgICAgICAgICAgICBzaGVsbD1UcnVlLFxuJwogICAgJyAgICAgICAgKSlcbicKICAgICcgICAgICAgICMgV1pGSVggUjI1QzI4OiBndW5pY29ybiBydW5zIGZpcmUtYW5kLWZvcmdldCBhYm92ZSwgc28gYSBjcmFzaFxuJwogICAgJyAgICAgICAgIyBpcyBzaWxlbnQgYW5kIHRoZSBzaXRlIDUwMnMgZm9yZXZlci4gUHJvYmUgKyByZXN0YXJ0ICsgbG9nIHdoeS5cbicKICAgICcgICAgICAgIGltcG9ydCB0aHJlYWRpbmcgYXMgX3IyOHRoXG4nCiAgICAnXG4nCiAgICAnICAgICAgICBkZWYgX3IyOF93ZWJfd2F0Y2hkb2coKTogICMgV1pGSVggUjI1QzI4XG4nCiAgICAnICAgICAgICAgICAgaW1wb3J0IHRpbWUgYXMgX3RcbicKICAgICcgICAgICAgICAgICBpbXBvcnQgdXJsbGliLnJlcXVlc3QgYXMgX3VcbicKICAgICcgICAgICAgICAgICBpbXBvcnQgc3VicHJvY2VzcyBhcyBfc3BcbicKICAgICcgICAgICAgICAgICBmcm9tIGFzeW5jaW8gaW1wb3J0IHJ1bl9jb3JvdXRpbmVfdGhyZWFkc2FmZSBhcyBfcmNcbicKICAgICcgICAgICAgICAgICBfY21kID0gKFxuJwogICAgJyAgICAgICAgICAgICAgICBmIntlbnZ9Z3VuaWNvcm4gLWsgdXZpY29ybi53b3JrZXJzLlV2aWNvcm5Xb3JrZXIgLXcgMSAiXG4nCiAgICAnICAgICAgICAgICAgICAgIGYid2ViLndzZXJ2ZXI6YXBwIC0tYmluZCAwLjAuMC4wOntQT1JUfSAiXG4nCiAgICAnICAgICAgICAgICAgICAgIGYiPj4gL2thZ2dsZS93b3JraW5nL3dzZXJ2ZXIubG9nIDI+JjEiXG4nCiAgICAnICAgICAgICAgICAgKVxuJwogICAgJyAgICAgICAgICAgIF9mYWlscyA9IDBcbicKICAgICcgICAgICAgICAgICBfbGFzdCA9IDAuMFxuJwogICAgJyAgICAgICAgICAgIHdoaWxlIFRydWU6XG4nCiAgICAnICAgICAgICAgICAgICAgIF9vayA9IEZhbHNlXG4nCiAgICAnICAgICAgICAgICAgICAgIHRyeTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIHdpdGggX3UudXJsb3BlbihmImh0dHA6Ly8xMjcuMC4wLjE6e1BPUlR9LyIsIHRpbWVvdXQ9NSkgYXMgX3I6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgX29rID0gX3Iuc3RhdHVzIDwgNTAwXG4nCiAgICAnICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBfb2sgPSBGYWxzZVxuJwogICAgJyAgICAgICAgICAgICAgICBpZiBfb2s6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBpZiBfZmFpbHMgPj0gMjpcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICBMT0dHRVIuaW5mbygicjI1YzI4OiB3ZWIgc2VydmVyIGJhY2sgdXAiKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgX2ZhaWxzID0gMFxuJwogICAgJyAgICAgICAgICAgICAgICBlbHNlOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgX2ZhaWxzICs9IDFcbicKICAgICcgICAgICAgICAgICAgICAgICAgIGlmIF9mYWlscyA+PSAyIGFuZCAoX3QudGltZSgpIC0gX2xhc3QpID4gMjQwOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIF9sYXN0ID0gX3QudGltZSgpXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICAgICBmInIyNWMyODogd2ViIHNlcnZlciBvbiBwb3J0IHtQT1JUfSBET1dOIlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICAgICAiIChndW5pY29ybikgLSBsb2cgdGFpbDoiXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIHRyeTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgd2l0aCBvcGVuKFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIi9rYWdnbGUvd29ya2luZy93c2VydmVyLmxvZyIsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiciIsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBlcnJvcnM9InJlcGxhY2UiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICAgICApIGFzIF9mOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZm9yIF9sIGluIF9mLnJlYWQoKS5zcGxpdGxpbmVzKClbLTI1Ol06XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgX2wuc3RyaXAoKTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYid3NlcnZlcnwge19sfSIpXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBfZTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYicjI1YzI4OiB3c2VydmVyIGxvZyB1bnJlYWRhYmxlOiB7X2V9IilcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICB0cnk6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9zcyA9IF9zcC5ydW4oXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBbInNzIiwgIi1sdG4iXSxcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNhcHR1cmVfb3V0cHV0PVRydWUsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0ZXh0PVRydWUsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aW1lb3V0PTEwLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICAgICApXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgIGZvciBfbCBpbiAoX3NzLnN0ZG91dCBvciAiIikuc3BsaXRsaW5lcygpOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgZiI6e1BPUlR9ICIgaW4gX2wgb3IgX2wucnN0cmlwKCkuZW5kc3dpdGgoZiI6e1BPUlR9Iik6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYic3N8IHtfbC5zdHJpcCgpfSIpXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjpcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgcGFzc1xuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIExPR0dFUi5lcnJvcigicjI1YzI4OiByZXN0YXJ0aW5nIGd1bmljb3JuIilcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICB0cnk6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9yYyhjbWRfZXhlYyhfY21kLCBzaGVsbD1UcnVlKSwgYm90X2xvb3ApXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBfZTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYicjI1YzI4OiBndW5pY29ybiByZXN0YXJ0IGZhaWxlZDoge19lfSIpXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgX2ZhaWxzID0gMFxuJwogICAgJyAgICAgICAgICAgICAgICBfdC5zbGVlcCgzMClcbicKICAgICdcbicKICAgICcgICAgICAgIF9yMjh0aC5UaHJlYWQodGFyZ2V0PV9yMjhfd2ViX3dhdGNoZG9nLCBkYWVtb249VHJ1ZSkuc3RhcnQoKSAgIyBXWkZJWCBSMjVDMjhcbicKKQoKaWYgc3JjLmNvdW50KE9MRCkgIT0gMToKICAgIHByaW50KCJwYXRjaF9yMjVjMjg6IGd1bmljb3JuIGxhdW5jaCBhbmNob3Igbm90IGZvdW5kIG9yIGFtYmlndW91cyIpCiAgICBzeXMuZXhpdCgxKQoKc3JjID0gc3JjLnJlcGxhY2UoT0xELCBORVcpCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaF9yMjVjMjg6IGd1bmljb3JuIGxvZ2dpbmcgKyB3ZWIgd2F0Y2hkb2cgYWRkZWQiKQo="),
    ('patch_r25c29_tpl.py',
     'web/templates/player.html',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMjlfdHBsLnB5IC0gcGxheWVyIHBhZ2UgdjUuMSAoY3JlYXRlLWlmLW1pc3NpbmcgZml4KS4KCnIyNWMyN190cGwgZXhwZWN0ZWQgd2ViL3RlbXBsYXRlcy9wbGF5ZXIuaHRtbCB0byBhbHJlYWR5IGV4aXN0LCBidXQgb24gYQpmcmVzaCBXWk1MLVggY2xvbmUgbm90aGluZyBjcmVhdGVzIGl0IChpdHMgb3JpZ2luYWwgY3JlYXRvciB3YXMgcmV0aXJlZAp3aXRoIHRoZSByMjcgcGF5bG9hZCksIHNvIHNpbmNlIHYxNS44My4yNyB0aGUgcGxheWVyIHRlbXBsYXRlIHNpbGVudGx5Cm5ldmVyIGRlcGxveWVkOiAiVEFSR0VUIE5PVCBGT1VORCIuIFNhbWUgcGFnZSBhcyBwbGF5ZXIgdjUsIGJ1dCB0aGlzCnBhdGNoIHdyaXRlcyBpdCB1bmNvbmRpdGlvbmFsbHkgKHRoZSBub3RlYm9vaydzIGFwcGx5X3BhdGNoZXMgZ3JhbnRzCnRoaXMgZW50cnkgYSBjcmVhdGUtaWYtbWlzc2luZyBleGVtcHRpb24pLgoiIiIKaW1wb3J0IGJhc2U2NAppbXBvcnQgZ3ppcAppbXBvcnQgc3lzCgpQQUdFID0gZ3ppcC5kZWNvbXByZXNzKGJhc2U2NC5iNjRkZWNvZGUoCiAgICAiSDRzSUFDdzR1bW9DLytWOWE1UGN5SEhnOS8wVlJjaGNvcGZkbU82ZTd1RzhhVDZsdlYwdWFRNGxyVFE3VnFBYjZHbHcwQUFHUU05alNVYkk5K0hlRjc2d2ZYSTR3aGYrNG5OY1hNVGR4WDJ4N3JQOFQvWVgrQ2RjWmhiZXFDcWdlNGE3Y21nbGttaWdLcXNxS3lzck15c3phLy9PMDVkUDN2emkxVE0yanhmdTRTZjdkM285OXZOZlB2LzhheFlPeDlQaGcxMFd1T2ExSGJLTE1lc3hjeG5QV1JENkU1dEZ0bWRGTFBiUGJLL0xYSDlxdWtleEg1cW5OclBNYUQ3eHpkQ2k0bDNtbVJjc2lrTW5ZTDBldElBTk1kZjBUZzgwMjlNT1A0RTN0bWtkZnNMZ3YvMkZIWnRzT2pmRHlJNFB0SisrZWQ3YjFvcWZQSE5oSDJnWGpuMForR0dzc2FudnhiWUhSUzhkSzU0ZldQYUZNN1Y3OUtQTEhNK0pIZFB0UmRBNSsyQmc5THNzcmRtYk9mSEIxTCt3UXdINDBKN1pZUWlmY3ZDZTM4dmUxaXZFYzN0aDk2YSs2eGZyL0tpUC8yMm01V01uZHUzRFY0Qk4xNGxpOXJ2ZkFwNWZmTm43ZW4rRGYrR2xYTWM3WTZIdEhtZ09BTkxvSmY0M2grWVBOTXVNelYxbkFWamVpQzVPNzE4dDNPN2R6U2Z3eU9EUml3N3V6ZU00Mk4zWXVMeThOQzQzRFQ4ODNSaENIN0R3UFJyN1kvL3E0RjZmOWRtZ1QzL3UzZDE4QmhCQ2V4b3pqc0o3K0piTmJlZDBIaWMvUXFnekhCcmplMnptdU83QnZidkR6ZWZqNXcrZVA3KzN3YXNISnBDRmRYRHZCUnZzR0NNMkdocGI3QWtiOW8wK0c0L2hCVlFlc2EwdGZOb3h0dG5XdHJFSjN6Y2Z3UGNIZldQQVJwdkdrRzNCQkxIUkdKNUcyMURvQ1JzOU1NYjg3WGlUMTkvaVA1UDY4TThXMWdjZ1kzaUNsc1pzdXc5UG0yTmpNKzJyNTN2MlBTUS9JRlRxK2ViVGJleDU4cXFYRG5wZ0RMSjNNQWYyMUF3TzdvWCswck5LcjkvNmpwZStUd2FQeUlVbnJUYUJBV0RWOXdBVTBDbWZQcHlkQ0tabkJpUVNHYWUrZityYVp1QkV4dFJmckZFL2lzM1ltVkpsTmczOUtQSkQ1OVR4Q29BcTVLTnFmMk1hUmNPSE0zUGh1TmNITDdBQVVMc1o3MTRDSGZ6eHFOL2ZHOE9mTGZqekFQNXM5L3VmSmtYZjJHZnhNdVRGQ2tVK3Rad0lPY2RCZEdrR09SblR3S0w0MnJXanVXM0g2YURweldGV2FqZjAvWmk5eTM3amY3UzZZQ25qVXRzRkhoT2U3WlcrOTNxVDAxMldyTG5xcDJnWnpzeXBqZDlIOEwvSGt1KzlJWlo0QVA5N1hpMkJrNy9Md3RPSnFZK0dYYll6N2dKOWIzZVpNZHp1aU1vU3FPR2o4Zk9uL2VybjJMNks0U05mUXRXUGkyVnNXL0QxeWVhVHgwOXJYODNwRkZnTGZPWlVMUDVNVFk4Zjd6eVZGb2o4R1FMcGJ3M0dtN1V5NFM0YkRJT3JHb3BNTDlwbDkzTEt1TmRsUFRNSVhMc1hYVWV4dmVpeXgwaHlMOHpwRWYxK0RpVzc3TjZSZmVyYjdLZWZRL0hYL3NTUC9TNURVRDBBNHN5cWpTUkVBKzF3c29KSzVSWjVVNzJsSTRieTRaUHM4Yk1LL1V6OHExN2tmT3Q0UUNVVFA3VHNzQWV2eWgxWW1DR3NuMTFXbWJMQXRDeXExeGUydFBFWjN5djd1d3cyQWpaM0xNdjJtQm5EbGplQjJXU0xKYkI3MDcwMHJ5UGdzUjdEWFllWjNuV3hqV1RnTEZ5Nk5wdllybi9KZEN3SDc3b3NBRTRXZFlFbmg0QzhpVGs5czBJLzZMRFBOaklJeDd6Ums4cVlNM1FpRjJSM25BVnVmYVlYQzRkQiszSzVmZ1JjeFhVblpzajN0bDEyWVlaNlN0K2Q1TmZrdENPRU4vR3Q2K29jUU45UGlYbnVpaXBueXp6OWlpdWw4aDFaVjQrem5yUVVFa0tsMU1MeGVud0wyOFZkenJxWTcwazYwcVBkZExmMGxSaVZhYUhvY0lyL3dwTFJCd0FtdUdKait0dU0yZmI0THVzTituZTduQ2RzRGJwc3NJbE1ZUXgvR1lPdFRwZkZJWFFzTUVPb3pyYjZkenZkeGtaMkNQcG9LMm1qZjVmSkd1aHZWeG9ZaisrS3A4RzRETTJnTWc4TDg0cHZmTHRzWjdzZnlGWUJ5bSsrWkNsTVhYTVI2SU1RMS8zbXhTWDBDeDQ3d3NLd3p1TFlYOEJNakV0dGxaZFFML3VQQ0wzNHUwRG9CbjJUVVBuTXRTdERPVFVEUVZjSHhuYTlzNmJybkhvOUI3aEx4Q0gxUU1DdDBCNitSWFR1TXZ4YmhMVjhyTVlXdENHZUVScURRZEpuWlNqSnBDUTlSblFCWXJjSXZVUDRVZWx5U3VFdGl5ZGNENGx0Q1dNY2JGY252cmc4Y1kzRHVzL3BmM05zMmFkZDJEVE1nVG15dXVudWdaVGRUYmRWWVlQUUVwQno1THVPVmVBZnRhTEFuZWVtNVY4aTNRMUhVSU5XQWRFK3lPMzgvOFo0WEtublQ5NkNmSVRTUEtBQkVWcWZNYzc5VkRQaGVMTXFUZkdLZ3pwWFNXWm9PSktSY2dMenpQR3NLa3hrWGJBRmdSeGhQQmlXcUFQL2MrMDRodW1CQlQybEZXWU1oOVVpeUE5N3RPcG5mZ2hFdGd3Q081eWFrYTNnb2FsTUlPS2psd2tCb1Vnbkg4dDhJQnBJbVFjbmExRFVDQjl4UXFQR0RpMURvbEVRNnV2TGtQYVdqSFViL2JHWU4xRlZvQlNnaUJvaUw0SHNlcFBRTnM5Z3A4ZC9ldmhHTlZta3lyMlQ0NUNFTXZuWWpKMzZiRmJaUVFNem1NUmV0Q0piTXg3VUdwVXlxR0tEMEpTc0pjY2o5TmNiTERGSHBLZnFRdU05cXM5RnlnTDZTbGEwczdOVDVVWFpSc09YQ3BCQ0hUaE9BUkV2ekFCK2xjc0QwMlVZNFhRR29MN1YrazRMeW9rZEgrZ3FXMXl3aHc4alpzUGE2cUltQ1pYeU40SkZhWUdxQmhJcXdaQ3pHMEQ5cmptTm5RdTdNZ09GTlUzR0N0M1llZENSd2pDQ0VLU1dzRm02NG10ZkxHSDlhRGFiTmJhd094ZHNVaHdkdUxad2xYcDJGTUc2SGd3Vi9UMmQrMUdzNksxUWhCcDJWdWRxTFhjY1lmK0VZMjNxcFdyVXZ0ZHlna2dmRTIvV2drR3ZPSjFsMFlwTVR4TFJpcjYxNWtIRWF5d0hMVWRFOWRDUDVjSVRjWVhoV01yOFF0QnpKQzJlaGs1Ri9NSTNvQklzNEh0TTFqWm9ENWpIR0pSVk5waUZKSzNtZjRsNnNsTmpJWTJNTGVkRHROTXdFaDJWekl6UFZiZ0tCNnFUYlVHMFg0bmJGQ2pDOFdCcmNXSTVyOHZKRVZiYk9HVjJSY0lydmk5eXhyR0lENW9lc0F3T2VXWmE5azhEWm16eWNnQXpua3NLdzFob3VoTXhBc1M4cUNPbEZlRVNMZkJPZWtUcStJWGVHNVRrM3dLZ1B6NnpyMmVodWJDanRKOFY5aGI2aThvcmtqUlJMb3V2YTF1WnZBdmJOUW04MEF1cTVxdmFHU2picVUvN2g1V3dWdWRGbVEyc0JTZFM4Rk5jMGtZOFh5NG1ZdDFtWExNdHBkSmUvVXRWWStuZmhzWnl1eG9FRFRlWS84c1lxMmd3RFV3WDNnTk5LQmhrYTJXRHk4b2dLYWkzQTJQaFZCV25ndDdWVnhFZEhxTzAzOEJheXJTanV0elpzai8yZWVUNjhjMWtiYkVoSXRPUHhzSFZHdVRxTFZiVWR2c0szWEdyWC9sNk9ZZStreHByWTAvcWxoSmNaRE1YTlgxdXRSUnNjSGtSMjNXZElISWl4WERNVUliamlldFB6L2JXMSt1MlIzWGw4dnNlbmJVTTExSk40Yk5qd3IvZWNnRml3QlIySm5PeWRJRnB3SXRJTWVSdDllcTAzQWFDWG9PRkpGUzJ1UzNqbHZVdkZXNDU3dDlkblN0bE82MnhMUmVTa2tJbENVa2dOeWt4SnR5QVpmdDhvMTYwTGQ5NERad0prRlkvdHQ1UmJkYno2ZkRpblZEaXRleVp1WFJqbVhKaW55K0I0MzBMK0ptWVljUkFhY0pEbEhRZ2lNR2lqbUtmZnd4K1NweCthSXhsOURmWWtobjdvRCtPZU8vZmJDRFpvV3AvVjlKdFFkQzJ6eWRzd0Ntd0IremFYOGFBaHhuNlBkankvdTU2OGJ3M25UdXVwUTg3bGM3WDVITFEzcUpXb0RhYlFiV0VOR3FHTkJoR1RiSTlvdVpkV2Q3dTN5MGZ3UXo2L2JzQ0NUeWQ5VkYxaGlyaSsxaFp1NzQzZjVDc2dPak1kdTBZQ1A4K2JDZ3hkTDFJOFBpbWRwTFozcXd1ay9JN2E2aXdtU3BjdDNiU3RrWUxyYzdoaXpOTm81a1AxN1VuVjR5cWhseVNwSWFDVlRiT1ltMmNrVXJkZEZxM1JnM3JXb3c3bGZDKzB5Zlp2VHcvd08zdmRtdWFHZEpjcmVTRDB0RmY1WGlUYit0RHBIUWtkeWtuaWViUUs1RE9SMUhHUVpLT05pMDBYdk5kczJaYjZGWGdwL3RyajdyV2I3ZFNDb2FyeENzTjlneVpPV3RTa3d2elZtZk9sVzFWajEvUUo2TFNrWkJQZXMxNnplbXY4dnBiWU1FV0N0TGpmc09XUHVveStQOEFOblpqcDhyZFFiQ2VuRGx4THozbjcyWG1WbmNaNm9PNlVhRjlRVTZvc1IrMDRoMVZ5eGMvd2FGVEI1QkYrRGtxSGVDd3FlbE85YVRVZldaN0YzcGt6dXllR2RvbW9DU3k0MlRGZGhrZVNuYjJXcGh2QnNQKzNVNHJNLzF3V3lXQkFSRVl5NkNWeGFndll3YTJmU1k0UTcrNVp2bWc2ZENvcW5zS2V4VTdDMXZFVDllVS94L1U3WnVOK2tZaThZeEdxMnFqcFVIc3VtWVVjeEdnT2wrRjNZVldwQmdhek9YMFRMcmtReEFkOE9SbHIxbnp6YmJ3bXRweGswbVhXb0RyWXpCQzAzRzd0YmVUNWF6K0V0MGNwWU0ySjdESWw3RzlOcXZMN0ZVTnUxNzVBRTh5b3FhekZWSnc4citNd2FpamhBbjRXQlhrY0NpbTNiNnlJUUdPV3g2MnRRSi81dm1URmFjd2dUdllsS29zbTZ1cXpNVVJsVStUNmc0YWZiYWRPbWRVMU5PZFRndGFFekhnci9YZXVDM1ByeC9FMXJDYTZOd2kzQ3BhVDA1ZkI2RDFTRWd2VUJ4WnJiMFQ3RWpaUERXbmNoVWFqNlFtNXRHL1FITTZINjhYdExJQUN4bTRFSm9ScjJBaWxMdkdmTjhHd0x6LzVrY3pjUTUvVUJQbk5IWnZmVFZ0am9XT01TcXl3MjVNbG5Hc1BMS3ZuNXRJdkZ0V21RQjBjYXJQUUNKUTlXVnJ1LzZsMFNDcU9uKytCUnV1U3RBU1kxcG9GUlY2TTdUYTJmc3l1MmplWXQwam84bE1MSVd6dTJ2T1lrSHZLZlJubDJuYTNrcmJlU3J4MTFnMjN6NXJzOWw2QTAxM0Nla21jUk1wWVZXMExVekh1NFdEMGh1NE9kVVczNlowNTYzMGV3VlBxSEZuNVE1WEd6Yjhta21saVlSSXJhOU5aeWlaNXVJV01ONFdldFpsVytCMnY3KzM5cXE1OE4zdjQwaTJpY0ZqTjc1SEJxL2t0a1VDbEpJZmR0anhnbVVzWGpEYlc5VVpUZWFoL2NFTjJzZU1xOStML1U2SnJ2UThaVXQ2T3JpMUtoTnJvaGFPbXU5dGc1cWZpU2Q1S0IzeWNPVWhwMU0zekF4LzFNa0JEMlRieElpQlRYU3c3OXk2WktCZUN4S1BsQVROVWplNGtnaTdJdnNxS3BVbUtQWVN2OTYzeXloMlp0ZTl5SFpuNmxNTm1FQ3hqNVgwTEZYS0x2RTBGUm9rbU4xU0MrM2RTVCtXSjJuV3VSVWRlZnMzY3loRHQ5VFk5OTNvMXZlUkxZa0JsbHM0UkhUVnhyOCs3NjhSM3lTb1lIdXNwdXVhNDQxYUZWYVpDYVhISWVzTG5sbmdROTJJbXAyVGJEV2RrMndCRFFFeGJTRm4ydGxxNmV1dFdnU3RBZzRTUjk4dCtwdi9HQmgxTmZubTlDZHlKUzVhMlFZWUJqVVNoVUdwWWxGV29Kb2FnWWhGcEp2SGNDUklIS2hDT0xadkVNSnhXNkVQQlNUY1F1Z0RRVnRSeEdwa0VXV2t5aG4xM0xIc200UURBNmMzdytrY0tISkZaOEZLbE9mdExCVkJaTkdLRHN3dHd4RGIwL1NJRTdYaWhDN0ZuMGlPWDB2UTlwY3h6MFBRUURlU3VHbms1MzNPSWVUckxKM0Zrck9BWWx5N3V5UUJ6bjNYc3NOMUhTN1NNK3p2OThBK1V5RHE1clJzaitvTkdsd3JCRVp1b2FPRGlMWkxKL1lQOEhDY1JXYThER0ZUQmI0eWt2b3Jqc1p0b2pwNGFEdTZYUW1rMjRTTjl1d0w2RmVrVW9hU05BU3JUQTBkK3N2OUk3YWI5djE4cDl0YXlUdWlyWE5FdmR4TlBPQXpsakJRc09RVWkwYWRvYTdtcExVT0h4dHNTZGtZajFObkE0SGNVTWdTTUJyV1ZrZ2puMGhIUE4rOExYZXZuWFd3MjlZd25ZRzRXc2Z3Vmo5TFN6alJRR20vL0VnbWxrRjdDNHNZQmUxTkg2TDZzVG1wQldJSXlhV3NEYnBtRUJGRDVVOXIrYWhuWGFqNmtPVDc5aVpGanUrdHQxSGw4SGRuVGlqeFYybVZ3MFFlUDVFdXVZWjFkVGF4YnV6bkxXWW1zaEN3bEozSXVZblI1MXJUU0Jhc3ZZVUNDUGtZaVNXUWtoUnJSOHVGL1gwcnJudzViOG1WMXZFcVN1dURXMVJhQnlORmtIeGRGdjBZcmdrZlJVZVZ6UG5IMGtVVHVWMWtoeTVxbzZQMXRGSEpZSXhUL3lORjZ4Y2I4ZnlWWGF3NmF6TEMyRGRyMGVMZnc3cVVHNU1lZkJSalVodDJXTi9pcSt0U1lEMVM3R2ZOVzBSbENlTHEyMnl4QkFzQzNmYld4ZVVQNXZWUjl1U20zR2tEa0s2V29SK2F4ZWdxaVdNM0ZEUlhvYnpHMDZDaWFyQlR6NXVVNjEyYnEraFFaVFZ5S0Y1R09CYkRISWlGcFZGdGtqTC9qOW9YdnJ4NmcySDFBNjJiM3FCZmZWOWFINVU4WkZNbm5JSUVaOFpzYzN3WC8zVExMS3FTMjZ3V2YxQ0lLN0JDWnhZUDJIQkxIS1VFMnhCZzBETmw4VW9jUDBNSmZ2cFMvTlMrSk9LNEFCR0VvTTIrUEVXY0NqOWIvYnZRV0FVL3dEQld4ZENRYlE3WHdsQWhFQ05CZFl0SURCSFQxWGZJcFg5NE1jKzlJR3V1REIvYWRHRzRkaGQ2MjlpSDNrNnBENTEyc1NFZ2lBY1Iyd0R1SGNid0R5YTdoWlpQVDJHZWlzeURGMXN4MDlKNHZaTWdiT3NqeEc3MWhwMFZsY09WeEtPaFdKUXN5a2EzazJ1Sm43MHFBMmdSZ3lzZXc2OGF2NHBOL0o2azZzSEQxUjdSNzd1UGIrcHVVbEpYcHhuSlFYeWk5S21KUm1YWmJpTnFYOVN6bW4yRXFQS1B2M29sSWI5dC9ZbDRIclJiZGVHb1lubEZUNGZWczFYTnpBcy9kS3B4c1BEMmgvZmNTa2xLNm5PL09WclpGZWw3bXp6QVlBTXJIYysyWjlLNlV1ZFpWVVdZVHV3eDA2ZG1hRVVkdml0WGM0NFpXT1RpeG5uQVFqdXd6VmpIM0Y5bzdYZTdlQ2dKS284KzJLTEVxSU5aV0kwOXBOMWRmb0pYNkp3cVU1bkFCYjhwN0V5VVVmcDdPZ3FvVVdpRFhpZUp3R25JRjlZS29hdWs4OXFVaGJNMjg1eDJyRmcxOFFJWHZWYW5BdHRpZDI2NUVXUFk2bUJzczVvSlY5bDNiN2xRK1FHMGhFSVp2YnFLQXJJY1dIV0xmOG9zNjFuNnpDakE0Q0ZLYlZkUGpGSW01SDdManRmVFNkVThmY2hJTTZ6RTR5cUIxcFB5ckVjUGlWYXF6TVJjbWZpdHNkUWhaTURGc0hGVGhzSzZLTmVRbXJoUmNLMGphTldZMU14ZlRIcVExbDhGU1cwWFIxMnNXTCtqZkhGL2pINktFK3prZHFueFdMYjdUcUZaakZQQmpZSGRaK2RMZTJsRFJkdURkNDVINlJPSzI3RWsvSzZWQ0o0SHEwM1hEVlpyVGg5bmJHMnBiYVcxZUwxYVd1M0JtbG0xUDdhQnRIcmtYakpVU1NKVG9yT2FoU0dWVWFXeFkvVXZKZnl1NEV4ZEpqVk9XNEhwMlc2SnBzN1JVbTNmM0gxRTRTVXlxdkxFL0ZSZ0pMSGw5bGMxNVFxVGdnMlY0ZzRmZVQzL2hDejFWN1VUNVEyeUJGbVFrT0w3eW03eVlMeENkcE1ISy9udllONy9WZzQ4b29JTjJVMmFUbkhRdzRYLzFhOE9IVmRsbHY5bnUzYVp5QTJUSkVzU29BemFCc052UnNDZko4NjBON0cvZGV4UU40YUllTXgxTUlaWjZDZ0k2S2FaVWM3eDRyWmI5OHF2VzVGeVo2WDhOSTBwN3RmZy9WcmY3NmpCbDBicFo4L2JOczUvZ01DcXdROGFWeVZFd3ZjVlFuVXV5RjJlN3JXOTYxM1ZSVGI5eE5PZkp6SGFYam1IVWFrYnQ1SFBIR083MktpWTFMeGQ4cUJWVXBsbmFzK0tDU0phQ1lMbk44czVYUVYxdzZTV3Rma3h2TzgxbitwNDdXUjUxRnRuY1NvNS9KU0dUSSthY2t2WDFNMkdwQjAzVEF0U240SHp4Y29abnBPS25vaW4vMTZrSWs0NmVLTjdaQVRKdDM2QUlheWJjVmcraUZVV2tOcFBaTGpMdElVZjJocWoreU5sTGlJTC92VUhFcEMzL3RBRTVIeG5HZkpybWZobXVuV2JDUUUvcmp6TUNlYW1BdkhpSXlTSEdzakY0U3c0VVo3RllzRnRvSzQ1YVJQRnRkcWxWMnNHR2laZHFqdEUzbDZZNGZqM1BjNHd3OEV0UkJrbXZJN0RQRXdzUTEzcGQwRkdqc1lJN3M3cTJrcXpSMEhkYmJJZ0JZeWtXc2xJN2dYZEZ4NmgzdEl0WWJkd1NWcWFVU0JMRTNTck9helNlYTdQcnl6YXZZZzRlVWhGRWE0aUk4bGdJTDBSTVlFZ1M4SzBrazk4MDlMYW5mblRaZFM3Y0NLbkhvU1NlWmtNMnpTVmxPNzVzeG5aSTV2RWFsbVMrRDllMkpaak1qMmdHK1NqWG1oYnk2bHQ5UlorZWtTTXZ6dlZiT1hrKzFoT1dNNnY2TUlUQWNGNytVbDF4ZUd3NWRWQmFiY0xOdW10RWNVb2xxRkxHcFJvdDdoOEcyL3JVbm5laVRSWndTV1FsVnp0a290UEJCSjJ1RERkZW92WjZzL2tzK3JkMFNYaHpiL3ErYUZEYXh6WWUreE1SVERUMGtSa2xGMjVkZ0RjTE9BTHgxbkxmTjFLazJodXJQVTVSdE9wengvMDdGU08wdk1QdFNQMG9ycGYyL2JVbTJLMVpYa2kwVnRzcEo3Tk1WL0pnK1kxS3MzQ3FQUzlVdnRmeWRzUXBhTXI0bUpiaVl2dGRnZ1hIWWRXRHZvMmE3NzhSWFpjZ21tWjBkd3pMMjdkNU41OGZhM0VETjdvQXAzMjJMQUFHQ2hRbHNwK1U4dlVsUFFpRlBtTGlGcHBOcjUwMlk4bU85T2h0ZDFaTzU5VTdZUzV4WldNaGVqVEViZTlicS91cE5IeWxrZ2hZbTdiRUM5elhNZ2FOS1pxSzFKNjFBOFRNbjY4OC9UNWM2VjNsL0RxVmNGaHdQNUdGRis3OXVFbit4dDRCSEg0eVNmN0U5KzZQdVFmTGVjQ2J6R0lvZ010dFpwb3pMR0t2emdUUDl6ZmdMTDFXaFI1WkE0MGhxYXNIaTk3b01YaDB0YlVWWWFxS3J3T2NhT2tFcTRuN1RBYjNENWlOUG1VSUxqd2xVb0FGV1MxMDZXbUhiNVlSczZVL1dRNUFieEFnVW9kazgxQklqM1FOclREbi9nTGUzL0RsQlZBOW9JaVpxUWR2a29mRmNVWDJLeDIrSklVSlVhL1NxWDNOMkFFaFo4UlB4NU4rNDgzWXZONTRVL0puSlFiUTd0NFVwNDJ0YVRDRS81c3V2R0JKcWxabUJ5OCtMMkN5V29Sdk1jOUgzVmhra3NWNWdQZS9CdThnbEE3L083WC93QUVPRkJEeGt2SGsxNi93TWRER2ZBaTJjWmVKT2d3RlV0Mnpyd2tTeTV4VGhyQk1XZ3N2ZzVzK0VwbG9hTy8rY2RQdlVrVTdESDhpaEVaK3h2OFcrdEc2TzdrcEltaitYSUcyMEt0bGIvL0gwa3JTWUViTmZMVWZlUzYxU2IybzR0VHZuTWZhSU10TGRtbitUTTZIVC8ycnc0MHpPbzJITUgvTlRTeHVBY2FNbXFOUlhIb253RXM0RmpJU1o4Z3kwbmY5aEtZUTJPVXZVS3FucHJCZ1VZY3N2VDZyZTk0Mlh2UmlnL01lTTVnRkM4R1E3WjVNUmhxRy9tN3hRTlFvdGtZLzljYkZ6KzhnRDczamZFY0JnTnZOMkNvaHdrK24vcVhudXViMWkzTW5Cblc1dTJmLys2di9qcWJPUGd1YjBGQXVwVlgwRzIreWcvejNhSkkyVm5XUE42aDVGRzRnRjF6WXJ0cHZTd2JFWFgzUDlkN3hrMFdDSk1YVFFlWi9pcGtMenJRanVnbDdLN2VhUVNMV0NPWlkrcURFbXZIVU1lZnphcU1kNE42STJjeEZHOG00akhsK2FCUU1jQTVkUlIvQ0dqOGtXcU9CZkJ5WU05aHc2aXV5Yi81Nyt4NUdta2hCaXVhMU1oMllScHo1UE40cFdUT2p1aVJybUVGVk9JUFFULzlnSGo5aGVrdW9WUUFtKzdoS3ovQWd5QkdXLzcrQmkvUldOWDhGbERDdnZ2MVg3SmZ0cTVqTFVQdDhFdVlYUnQ1dWFnUzBpa09zZksyakYyTWcwbkcvRE5nTGhYY3BoajRNVVkrYlBBcjE1RUhJYnY5QndtcWxhc3pQckpGUE8rSXo0VjA2VGNBZmVJQ3NWZGgwa3NKdUtLTWtRRkIvMUxjdXNyeVJYWHhWM1o0UkFrSHdKK0lVOUpDUXJxQnRVY1FsZndpVDN2QUFmSGZyK2kza0d0UTcvT1NiNjZnMjYvcDhhRlFPaW9qNzlRdk52TmpYN0NSc2llZ0pUamUwbTQxRzE0SjRGYzFnRWV4R2Naa1hhaURLOHFOVmJ6a0dBbGE0QUxMRUNiNmpKTzliYW1Ra1ZaNStnZS8vK1k3YjQ2M0ZyTitsVS9OMXpVUyt0di9LcDFxMlVMQ2F3NDVTUDZVcVMvWjR1RkFVTGxJRlkyU0ZoU21DbEFveEplUXhOSmJ1NnBiSWQxRXhya0NzdG4rYnIrL3Y0RXY1YnNqWGRXVHNCTCtHUHJJT2lQUWJ1Mnd3aGlnV1EyZEJ1aVlIYWlwUVhMSEc2ZmFDZFhMV1lJRmVHaFRZWmF0TVhwcVV3V3ZJZUpWNk9td1dXQ3FvUFNwREtWVkNpbnlndEMvMUZycFRjRnFlcE9YeUJaZThDaTB6V3pEZXhuWUhuY0RGMDFOY2Z1WVpzMEt0dy9WbGhOa090WmZ0YXFXNkZqUTB4ajNta1J5Rm5JNXdRUVVKYm5ZVmNoeFJEOUhnVzFiR1RwUXBVTExBb3Y0NnhJeDA2dkR3VC85ZGJOSXgwRzd0aDNrd2hYK29sdnpxb3VrOEFFRTR2LzBOeTNCLzhSMmMraGYyTmNUM3d5QnQ4MUJpSnN1NDRqcER6dmxocWpDNGNPMnZaL2pHa3M3ejVWQXBrY1ZtS242aUFwalM4Q3ZRdnNpUnpqOGNQd2w5RGFvUUU2L0FPZy8vOTh0UVQrR3FSdjB0WXpyblRrNWh2QWJHOENtV1c0bWUyMVBmYy9DMXY3ZFh3ejZiWWRDK25scUhBQ0dYU0lrRUNRRGN4a2gxdkFzcERvK0xPR0h2QWhKSXkwYmZYNXB5Y2I0M0E4dmtRanF3eXgrU1VjNjZOOXYyZVJYOWxXdUplQVBwbnVWNFZBUm1Lci8xUkxrNjhMU2VFMWh3RXdQS3pENWU5SVMvNndsMkJma2FKYkF4Ujkwd1UrSXFkMkIrL0JsM1dVUkxya3V1OEFUVHBzcmpZVm1TOVZnVFAveC83Ulh0Z3JNQjZBM01aOFh5N2pRVy9wUjZnaSt3ZEgvQjhYb3ViTE01WkxRQkRXSk0wOXNIYjB6Y05QRm93dVE2MURzc2tFRzZ4dDllRTdVcTc2eFUyNzBaNFFVclhtb1FqRnA4c1QxbzN4TTlDdTUrTGZjVE9sTG95QlZNcnNXY016RGRYalR5YlBNcWt2aEUwbEovdGhXZUNMWCtDcEM1cHVIZjRMYjV2NEdQS2tRYzU1ZzVqekZqRWp2NU4vYXlaUEZudVZhR1g4OFhCRm5peU1senJnWFNGSVVWc1VSLzNsVHhKVVdXR3NFTGo0Q0FoYzVBbkY4WHhJT3BTdDZVUmZPYXJJTCt1MXBuRFNJNDJER0hMSk5kZG5iNVNKZ3BuZDlPYmZSTUNjUmhNcURYNlJhN0lKQVZoVVFGTi9XWWs3dGgwSm1SQmJQbllnRm1VbDl4YjVMVFpVcUkyWE5HdGxJMlVtYTFmUWd3ZzFlWHRRRjQ1SWFnVGJJR25rZXBTSlVJMmxlNVUxOXZRWmhjbjBCTSs4S2NCbUhoL3V4ZGJoL05yRU9qMUIrMk4vQVJ4QXE2TlVYL0Nlb0ZoYVZDd29DQjMrNUFSRFVZTC83Ti84bEFacjgvSXNLVUZRYjJlLytMd29OYldGK1ZlN21xd3BFRDRVSDZHWXE5dUhTYUF2NnFOcTc5R1NpWGZYWGxlbzgrY2d1ODJjekJrT25sWXIvK2w1cmtDOHFJUEc4dW0zZFA2blV4UWh2d015VTlzVnp2clcwZy9Rc21sWmdjU0RBYW5oc2IxdEF6eXRneUMrUGNhdDdXeGcvcnNBNHJWaFUyOEo1V0lGRERBZ1htZ1FBdkNrdkpCbS9NSmVXNDlPeXhWTmhJRVEwQ1NWMkxZQkxudzlyUllmeW9yeHNOQTJkb0dDQzFtZExqOXQ5OUtyM25ZWTZRUlNIempTdTNEbDVZWWJzemNzdm5uM0ZEcGp1K2xOeVRqRFFwdVdaQzl1SUF0ZUpkVzFENnhqYzMxaC83UHV1YlhvZEkvQURhT2Y5ZTZacG5UcE04aVlCbU1jblhUd2VtYi94eitCWDljYkxPTHdXZVNGbTViRkQ3bEhzaCthcGJaemE4ZWNBVmRjdXYxMjR2NExSMk9iaVYxaFc2d2dQczZCcmtSMUZNQjRCaEZmdUk2ckpCMUR4ckdHQWh1bWM2ZWh1K2FFMk5ITUFQYk9BVUJlMkZ5UE1aNjZOajQrdlA3ZDBuR0lCT3N5aHNzNVFWQWZUTGlncW9UVk9VSXZmRnE2b3gwMW9ncHAwZjdtaUlwbXhSUDFjenA0cEs2TEZURkNQTHQxV1ZDTWJtR2lFVDVaS3hKQmhVVlR2YVVPOXArSjYzSmFrcXBsWW0wUjF1VUZKV1RteE9ZbHFrOEZOV1ptYjVFU3pRdHErYWxiSWxDQ3NDVnVsdWliYVUwUTFTVkZYMVNTZFhWUVRyVHJLbW1RY0V0VUVsVjFaRWRWK1VUMVVkWlVWU1JjVzFFUVhkMFU5VklNRnRYQTdVaThUVjBJRlV5TGIzZ0FZNlJTVGhNRVBFMzVFc2VsWkU1eGljOWhsNURZT1RNdTJrcklvU0dFODdBR2JtVzQxU3dtQ1RXU1p0RUEzU1kwR3YvdDdHRlRYSnpIbGQ3OWxBNUpUNEdHSWdrb3hnazdPd25QZ0toYk01NVFkSE1ET01ORHFmcFJaandJempPelB2VmhYUWFPSkpuN2UxN3BzMEtmbmZoMHF6V0VLOURsc3NMRXUyMmRldVQvRDJlelVnVGd6cGp2UmMwcStxMTkwMktlZkFzeERCbzNDL0J0azZZQW1qbURMOVU3aHUzS0RLWHNScHB1NDdlcU9CZDhCQy9FeTlLUjBBNFgyS2k2Z0dZelpBbERXRWN3UGR2OU8xditJVUJXeGZleC9SQlJRbjFGNC9RTEVBbVBtK242b1J4MHhZaGVWWWlDSWJmVTdZbkFSdTF1NzNZOVBPdzBZb3dLMVhRMysxckZyb0JJOHhLbGxlSEYyQjk1R01uL1lFZ29tMTdFZDZaNFVDZkFsYVUvVHhFTmFvaGlqUFFhYTByNmd2MS9RM3o5K3JJRnM0NGl4ZFRsMzBIYnRzY01ENlBod2hQVGh3Q0EyY1VKQjZPWnY5NWh6LzM1MThnb1kwQjBZc21mRS9uTU1SZFdISFJpN2gwUFhHS0psZWV5Y3RNTEJNblQxMkQ4VDRXQmpnMEptQjl1N21hN051R2dWc1ZNZkgwMDhvd1Z0UFBTWHAzUDQxeGJCQVBreXhNdUY4YVNHb1FNdmF2Q25aZ3pzQ0ZnVVZHSkhiMTQvZS9UaVY2OGVIUjNKaHF0dDhKWTNjSERRWHh6b1E0UjhNTkNFRWg1UVJpb3JBbWw4aXM4SEdzV1NUbjNML3Vucno1LzRpd0I0bGhlbjVUcWNlbHBoelhKL0tzZGIxbVhML1QzcExrcmNKQTNvVGd6czMvbldGdldiTmhTZ1dTY21rb3dUWjMrQkZKeXVrQ2tXUkhENDc4WTNHOTlZOTYvZ3p6Y2JHd2FzckZpZmRrVHRrQ3N2dERNMWdJbWo4NVFPVlhXbzE3bWl2NkU2ckNGQ0hjRUczRjBWZjJ4b0FxWWhYU2hUcFpjOWJHYnA5UUY1MnRqN1BNMG83S0p4YlUvTGNJcWhKRi9ZMTRCUkJRM284TmxBYlNsUmhyRDc3NGx0d1h1VHkzLzhTd2VXOHBmK3BSMCtNU05iN3hSUUU5M2ZPTzNpc29ZaW9iUFFsVk9lam1kemwwVUxkQUJKZlB1WlIrblY2ZFFCb0FTMDdsQXdjSzQySXRpUG5TdDRZOGFmQ0hmWkNETzlQZnZaczllL1NGUUlIWFpDZzJtSkFOdUR6Z0ZDVEpnZzc5VEZLRzE4WFFXVnNwQTkzblRLVUFnNW9FeWJMaXh2NnhwUERDODlUSXlQcFdJLzZFZ25ZSXE2NWxkUVhieU40UzRDbXdQSHJwQUdDemcyOUVXdytYNHhNdUhQNVAwTTNyNzNnMlgwM2o4OWZYOXBYcnczNGNYaTdPTDlJaGk5djdRbmk4NGZiVGpkTm9DUGovLzBtNU9Uejc0NVNlZXdxWUt1UDl3OS90UE95V2NQTzk5TTRCa0VMV2ZxbU81Nzl4cFU5T2doL3hkK2s3Ny8vc0t4YlAvOWJKbllkZC9QcmZlanMvZnpjL2dRTFUwWFZrejRQbktCcnF6M1VRQWNkeGtBV0lML1RXZmoxR25YcCtqZHNQdWg1UUQrOVBpYjZKdmxzRC9ZcEw5SDczZTd4c245OThLM2YwUXdaU0JGeEY1WVd1MjJlQ1N2VjBDUUVSRkpWamN4WHVEaU91NlZPbmFDNjAwdU5hRXJKOGorVC9tcVF1cUxkT0hlK1JsYm1HK1JuMXl6Y09uYXU4bXlNeGxRTFVaeU1OMG5oZHh4NFVjSEJjNFlKQU1iU1YrRURxZ1Y4Y3NpL0JsZkdyZ1FJMWd1dUpzQ3g4YVZqU0s0YjFrVGxNbHBYWG0rRUJhNkw0WThxaTRpWUpkejN5MnN5Y3FLUy9jR2ovWUdleEVacnUyZHh2TzlCcWxKTERKeGxlWDRwUDUxQmhqUnlVREVGUTBVaWp5U2d6cFl5NEFGT2RmelZVOWRBVEdIK0d0SEluUGFua1dtSmcxNEhVcG13T3MwVmRNMmI5cUdwb2Z3RHpZdDNzQ29uOWFWWkNnbG1GY2M1aFVmemhYQ2hKcDhPRmVkUFNsNDhrS1RpTndGUVJKaDhRbEJpWEtJVzNGU0U4WWc2MzQyR3lpVllSdnZQdXhKQzJZak1mbElUSUNjdHdxLzVXZ3FtU3lnZHI0ZVlVYVBBY2l4ZVhMUzJWUFdSYW9LaWtOc2FpeXpJdG1vQXVzMmFaR29JZ1RIL1JNUW00TGpERnlQRFU0cVcrOWVJMnlPdEdNQWY0THdpejlScmNSZGZxQ0c4dUdUMWIrUVpjSW1nNUczeE96aytPT0puRHhLTTNlR1d6anZhSWVMYmttblR6RGpBd0pDOXBoQ3pEN3VwUzJlN1NsNlJzdWVDc0x3T1F3Z3ZlSVBVdnVtdHVNV3FmVXoxamUyUUVLY3dNNS90cWNjdDhlTlI3S2xWaHJxaEJQcHBFS2trNVpFT2hGUzZhU1JTcWt5Y3ZFRFZpQzRTVUp4aytOZ1VxUzVGaFEvcWE1cWhGNG1WV29IY2R4bVJSU0dVdW1qRWJrTzdOMkRqb0h1cjdxR0VsMm5FWjc0djkwY1hyL0xlbVdZelVzTEo1cHpSdDdWenUydkk4NjFzU0U1Yk01QVFRSC9wQjNzRC9JZDVWdE9qdDl5N3Y4dGNYL2F1YjQ5TVN6Y1VuRmV2aVhPa2IxSCtsT3FMUWo0K2FPZi9lcUxaNy9Bb3hPMFJqMDNMd3pVTE9qRVJtREJOeTlFYko1TWRPbkhmM1gwOGl1RFRGOWlxMWZTSWpla3ZmdkFMV29Bc2VsVUpQUXZuN21SWVAwbUhYdnB1ZGNLWTZRZnhpOUFDY2FCWXJTSGFBQ0ZNbko3SFFXVThNNFRuSGEyTmpNSTNPdWYrYTR1VTVrcjlzTE14aWNnWFhOZ2NDOHZzc3ptejBWcjRVTUF1QXVjY2FkZW5jek9Ca1lMUCtFSllIRDN1ZUFMR1N0cS8veDNmL1Z2MFFnR0wvY0J4RGg1OSsvSk1JYWVXNklseURGWVFsdFVOWE4yYzNObFJ6bmRNaXd1SFNIKzZLQ2hNaUp1eVRiSWQ4RENFWHozbTMra0FYejM1LzlQWUljZzY3OUJMaERvTUdQd0s3dDB6ZmVnMTRtdFdUQnF0UDNMYWlXMlpUVFpTaXFXZTV6YW9nK1FVWE9VLytzVTVYOG1NWjBRbVBPbEhWN3pLQncveEEwWTFBYlA5d09Rd0FVc0JZbk55aEZrTFhtNE9Na2NYVlpBWFJJSDhjYmhkZ2VKblR1WTBua01kRmhISndnTE5HMjhhNEh0aWlyUUZwK2NHOGcyR3p3RU5DaDgycUNnQzF3WDBNWjlwdDNWeEl3VWovR1NHcGcrVDExQmpKQkpQdXdKekxVZFZ2UHlaUWpITFRUYlUrSFprbzJERGkwckE5SDFpUUc2aEY3Y3hEc0ZySFhhOXhyUElDc0VoTGI0V0xRMG53cUxXcEpEQjFTdERuRlRKL01kN2lYdzVrUTJUSDR5V1FHZlY4UE5LZHVUNkVWOVY4cEFjVXVRSEZadTZpbzJBTjE5eUhSaE1iU1Y3UXJ0anBueU5VMmFTQTJiS1pRdUc0ejdFcm1Cam1hbXVOTDQ2YWdSaFFSbXVwZSs0QTVhMlc0a2tTTnNGOTFGS3pYUTkxRGNiSFkrRXlmSHhYb1RucmtsLzNlL1pULy81WXN2ZTE4cjhCQk1HdzZCTVVCQ2dZNWcycVFqZWw0aUw3MmNJWWtwUkxKZ1d0MmRrQ1R2NDFMQjRmZ3pPcHNvMmhDVTB0MTlwbnRlUnRId3VGK3FtNU81NTUwMFNwWVBPVDZYQVExbE4rOEoxdVkyV29GQmZTMlJyMkMvNEtKUHBvUTRhb05DaURYdjh6cG8yYkRNR09UODJIREVuY29MMWpleUpPY3g3bVlPN1V6eW1TTWh5L1ZwSjhzZ2xyWW1YVFBzY3l5aUlpVDgzaUZBaHVONWR2aVROeSsrUkJMSTI4Y0p1MVBhM3VWYXhrTjJyK1QrYVo5cmgvdk80ZjZHSS9tSGU0SGVrM0NOK215UlU5dlJ0VGZWSmR4VW8vUmxSL3hVV1VQOTJUTXZuRk9UYjlYQ2pvc1B2RFBkSnExdUZDRlRkbXFNOGptaTQ0YURNaHV2b0F1cE9KMVpIQ2wvTGVFT0NqRk5qQkxWSGdKaytEaEpjcUpuWFBleGMxcmdaQ0liWEdSZTJLLzhTQ2R1MnRsckpTc21CL21ZMDBLWG5zMnFwd2NZNmgzWlVPVG1TZm4wa2ZFcmpLVTd6cmd2MjNFa2s0NjVPM0IxRTJlOVpDL3cyNHZrblM0bklkby9kcXViQmQ4d3U5SmFmRnZkWlpLZFZsSFJuU3dYUUdiSkpxUnM0ZElQejNZSlNRL1pNV2hrNFpSKzhlUEdDSUFBanE3Z0QzQWtkRTJHRjg0Q2RJMk50NEVOdFB3QnJTVEhZaTcrUVhUY3Q3b0tRc2tCcEZxYzNKWXJZdXNsTXpqbjZwbEoxK25JeE9kRUdVbFBJYUNHb3JXM1NHeTVzUXprelQxNGQ0ZzllTnZycVhhUnM3S2ZCVDFpT2g5L29hTzBxcitsSFZteEdjU0xnRGQrL1Baa0wvazNlWEdXdkRqREYxQnVieDIyVWpCWXMwSHlCdTFrNlRZbEdWdGFpaDdLcUVsNlZYbkpqU3NybkJYWFprUjJxc1NGSVVmR25WSVhwNVR4Rklhcm1qaXZZZUpLc0hyeU9jUXVrTndFSGZSa1JxeGt5TjRxNktrYnRpVUlRby82UDBnRTllb0lrbXh5Yy8veXNTbm1SeE16TEVoMHBnVnkvRElRaVY1WUVEYm1SM0VjT3BObERHSmZJUmdLajd0bzJ4WFZSSHRNcTVrRVdMYXNuOXh5VkpSUWRMVjFxRDY0MEY3QVpycjIrQ2pjUzFBeDlZdHNOM0NKazUwWExVTWJYY0gxNlVTMmJRU2hQN0c1UTNxK3U1WUZEaERKejBSNVVHbFBTT3VqU2pxZHBHSlNLcUdJbEF6MDVOUkw3bEJaSHdwT1J1czdGSFdoSnlDZHpIMExmdjdrMmFPbnNEbDNqSGh1ZTRVZ0FqbVBoa0hCbklFb3U0ellIZURuby80Z0dSdk9sWEpvWmY5K21lNHZNRmR5SXBJRkFEUlNaTUdNWEhZaExjTk53d0phd1NQYXVJUmhYRHJBbmk0eG1lY2lpSFh0aUhyR0FxQi9FSmlzWFpWQ2RTZTRiRTBXUmRJd0EyZWppSUd1UWlmSlp2clZ5Nk0zQ2dFUFl5ZnRFTVM0ZDB4TDFQdmVHeERqTktpS0ZuS0hSNGRzdkkxUUdQOGdCNFRaRkhmNVNVTkVKbVZuZHEyL3l4R0NXUHZRa2NpQk5USWNGcHc2d3FHQnJSTUxxcFcwVkVZT1JMWkZKa0crVnB0TzAzSWl0V1NMdTcyQnZVS3VXVWhNT3pyTEdGNjZ2dVFIWXR4bzlTNmxTTk8xUXlESW40ZStkNXFoSDZtN1FHOGZaUE5BL1NvSEZaWHFpU1IyYWEyRU1YeG95WmRSLy8wY003am9UaGVkUzQrNHM3dE0xRUJ4dlU4V050cndDeEtDMms4bHVqUlJDczRiUUFMUmk3NzBCeVEvb0Fja0w0REdSTmxXSkxEZ1pCNzcxTkxEekdrZjFDYkJ2cFc3OU90cHhRTjA5a2VUZ0RtVVZPSnFCNEJIVEpkMmEyNzNST2ZmekkyR2szNlY2Z1EwVUlrbkVJd3NPektUZktQa0pYcG5WUTA4S0p3T0FSSG9LcXNtSFpoeml1dWtEMlhTVzFHdDVLRUxpZEFtbUoraTBVSk1VU0hGZVBCZ2o0cDk3UjdtN3o1RzViL25IR2ozMERRSmYrNXBKL2NreWlUQ1FrY2ZQS2lZaHI3cmZ1N0ZQcWFTVTVxbUJPV0I5NUlqR1BCeHZKZkxqbUlOUFV2bTVvV0RlWE8xYU9IN3dKWndkYTVuVURySHFQQU9ON2U5dGozU3dkdnFFSHhIazhsZXQzUVdWbzUzc01oNFpMRjlPbE9WTVFpc0ExdEdqOFVkTExsSmVtMWhYZXh6VFRleGZsY01CdElEbWVLNmFyUzNKeEpMem5wSzZ6bUhsUy9zdEN6R2plcXJUcWRra256dkdVeXBKYlhURlk5RzFhUXBtQzh5dDRnWC9ncUx2TTAyS3B2bkQxSkhTT1g4Y1AyeU91djV4dVYxbVdTM3BqMjZncmhCc1NwUXQ4aVdtdFY5Snp4VHB6UDBQZVdVSG1QZ21EazhNV1orK013czRkRVV6WnlKaXVrenZHZ1lGVG04RDF6WE1HM1VNZ0EyaHI2VzhwRGpJcHBNMGhMNDVNclhXM0Y1eU5oK1hiT1RHUTlGWFVmT1pGVjduZlR3b05ERGpPRDNXb1BHeVFQSVM2ZHRlY3JCdEVJRk93ejljQVdNRjhjak9HSk80RFhqRW45WG93L1N5L1N5UkVlQzIvVnl6NC82V0thZ1NwdzFqaVUxS1ZKVUduSEJNRWFBaGVXaFVKWUlDK1Z6bFI5VzZFaFdiczJTOG9rSyt4UnFlMk1NSG5MaE9MTk41NXdtc2VFbGFBWXhFMTFEa04rSk9WQ3RleGpQdTNiMzZtdzI2VVJ2a1BSQ3huV3o0MkkxLzIwMUJISXRXbmNJZVp4cmF2bVgrVjVWekE1UkxRNDJjMlJDVzg2QVR2NzZMUXdSQWx0WGRZVG9oYlR1QUxNUTNIU3pvb1ArdTJ4em5YRmloRzdtWmNiaGRXNWxoT1F6ZDVOVkl2VHQ0L0c4N3lpaU56Mmxod1VEeU1qSzdKWENmV0hDUkt3b1dmbkZrbFdRRkxWczdBaXJ5elN0S2hZUVpoMEhsS3dOTFE0SmxFb2xlZmc3VHlQVVVhQTFNUnNMTmdoMElvTVZqT1dBWUN5Z0JQam5MRTBZS1F2MndscHZmQjJxUVhOZnl6aEdTRjQ0NXZRTXUvd1lmWGlCbXA1UW5kZWdhTWtVTSs0SlI2Y0pDOGZUUVE3aXp5YUpXMm1qSU8ySDVLYUdmbDhoOXd5VGhaM0lWQk14MDVMdVAwS1pHSHY3R2JQMjJ2cmc2YnlDeWtsTjdJdlhvcUpLb09RVElSQnYrT1dJbG4vcGxkYWYzYmpSeTZURFBKK0IyQVVySVI1QVpFSStrdmgwbjh3eGhSNWRrSENSMUw3SXE3TVBrbkR3b0ZSZk5xM0svQXZwZjRtWmp0dWd4U2pFTDRCQy9LZXpOcEFsOHQxbElKSkxCTEhyaWUxUU5xbnFIalhWRm5mbFEvM1dYaUZkQVE5cFIxT3JMOUE3bHRvVVlCc1Vhb1M1S2g2QjdIejVKU3dnclNOVGFvdk1SVkNnaC9rcDlwaU56QjlIK05TZW1VczMxanN5VWJiYSttdU15bTlxSHZpY0pXeitmdHZtYTlKL3RsL2NZRzc0ZUVDY0J3Wk9RZVdmZi9YcXAyL2V2M24yOVp0SHI1ODlTdUxLMHhMd3orbFhlWFIxMHlSTktVSUE4VVFwN0RUWitoU01YZXdvVERvTWJXVzZYSzZYVHhFT01Cc3Nxc0ZFMmhTSVJjSnpDbGttTjBoSlR3R1lsSVlWQVhzYUlyanc0aXR0N1U0R1ZWaXZ0TFg3RlZWaEhYRllTVVRBU3JEQ0txelhIQllQRTFnSjFLSUs2Z1VIUlpMb2lyQm1WVmpQYjB5MWRMaWh6SzZWM0d5ajh2V2NkQUNHUVNuNTlGYlpHRVNETzYwTzdzZUVLSGtXTHJvcnBiTXFEcytyemZ3Sk5jTTlaU21SYVh0WUR3a1d2SW5teml6K3dyN21TeTM3dnFGSlUxNUloNVhrQnUza251UjMycFpkRi9QUG9xa1p5Qm5nQ2wyVk83MVQyc2NVdmZJU21INjNtWVJxbTgwYUhycnl3eVdKa3lhNm50Qm05UlBUczl5Q0thOXNKQ3h2QTBLejRHcU5KUGEvajl4S2t2U1U1L3lydDFibXh6ZHREZTA0a3BZcXU4aE5XMExoT3ZaTHpiUTkvQ2RKSDJXZ096ekVHdDlXaE1TT1dLYks2KzYxZG1GZGVWVG9KSTVwODF0WmVoc09WdHJKb0MxT1RtNWxaRE4rSDhEdERReWtXL0hKb0Z6Z1hYT3NhemdpNTlacWh4djJWRjZTRkFTUVdXZmZGYXlLeEhuYld4QzFnZGJtY0Q5eFY2ZzdNdEpXa3J2RTVSUEZuZGV3di9oVThzNlErWU44a0I0YlhHd3lZQWVoR2UyeTlFN1ZidVdlQkVwNTBxWHN1eGlxaWJkZmRaTzB3TExEQnA0eWxZTlQ1MDFOcm5HdFh1NmJtNkh5S0FqS2ppVzFXY2crY3JkR0RpSXh0K1NYNVg2T0h2Rm9MTVNEM0h2b3JrZHc4dXc2OXloanp0M2hBNTVLNmw1SFlzekpXcWlFczBuUEhscENFZS8ySDFiS1FjYURSZ3FPVytYc1pCUktJWXAzUDNyMTdOblRJM1RTN3hzUHhsMkdsL2thUTN3dzhFcmZFNlNUejhtTGY3Q25DT1ZPZkU5RUo1cURMQmJuTlEvRk1ZZlZON3dUeDlTU0lGWkFrYU9VN3V2cGxJL2FoRmpYUzIzd0UyQ3krLy9UWDZQbHYvd1ZpQUJlS3gwcW12dTA5Z2xIZ20rZFB5VG1mOTVCYVk0Z3VRTlFUY2JqZ1dtdzZGODRTSHo5aEFVOGlncy8zamlVMGhpM2ExSHFXTHJOU0prN2xxNUZFcVJXUGZyeTJiTlhuTnd3b3JUTE51SGZyZjZKakRQRWZrQ3c5RVVrakpBdURtU3ZPQkN4dFNJYkhEbko0YVdLbjZPSjdNSjBDNS8yNmpnUXNIUStSb0hUTlFqTklxZHJYcnh5Sm94WFFXbnl3a2xjYStuNktQSEFDRDJ4YjBZeFBiWnlCYUlkTThHdTQ0blR2V1g0RnpsbjViakgrcElDTkIxUFlaMGJubitwSTRQRnd1d3ptUFYrdjcvWGpGZnkxQmNqdFRoUHVEdW5jOWtzNzFDQ1lHNlFUM3ZaSy9SU3JxTlRKZTdHSkpla2xHaHJWcHlLbmJ6SVRhbzhzVVdYYnZqQnZCOERXRHFlT2xOUkNtVm1XdmJLT0NvSm5ySjREcWtBbWFYaEtBckZGeGhQTWdCVTR6VUlOSTdPYWg2NXBSTnBEcUJGNzdsYVhGenRpSkFXaVhQazhSalZVVjZzUEF4MWlROWROcEtHUDdad3Ftenl3WkwzUUpCVW1JaCtnNi9ZVHJlYW1aaC92cHQ4aG5LRHZyVG5DamFJbkdHMUJNUUpuaVROY1c1WTVKMDhQSjNZRHdiT3c0TjZqODkyU3I0aDk2dCtERFNVdGJmNUhMQ2VQYWViUGUyU3lvU0F2TWh4V3ZNa3pWZVRzNTdpeURHenVDWnpiaXZzQTFXb1RiS0VYQUtnaXdqWEY0S1VSaXljRjErWk1EKzFxTlhyK2hjRmcyRDJRMzAybHFSc0h2WjM2YjRWZnNxZVh2S0VTVm94MjJOeU1SSHZlUVRpZHp0RVpiYS9kVEdGbDZhb1ZJbmFuT1hwbG4yNm1VbnFLUVRxVXBMNWlhNG9tWWYyVE9JZWxGa2xTSXZzTk5vRHFaaitMZzI4cmlUeXdHT25WMG5XVER5NkROMWQ2b3M0MXVHRCtvd29ieFRRR1hCM0NQUmpxcjgyTGpGUDhSdmdUTTBERUZUU29ZdEpnSXc4bTE3SlJwY3dxQzhkN3d3MDBzQ3hyVHRrVE9pdVZ2K0pIMXpEQnUrNE5nV2FxSHpoeEk3Q0JBWjd2NW9PS2lkcGZoV1lpcVFUMGhNNHNuRFRBd3Y4eUVtamtjYlR3VTVIWkh4NDlmS29tS3Z0bFIrcGNyVUJ5Q1B6d241VDUrUzVSSnptVmZERGFaUGpCR1ZES09aeVVJVzlnRkJaa29JbEIrTFVMcEVubE8vbFBkNUhXVURocWxFWUd0UmNKUUJFR0QrVjRMVmJDeWlUbTN2SmU3REw0dDJpWUNBSkZnQUpJbzUyc2FjU2UrQ3FCa0d4V1NJNjQvZklTL2xiSU5Sd2M1RXZhSkc4TDBFVnozK0h3Rm80RVBJb3hBTDlCSVpEbVFwMUVJeVNYS2Y3Yk5oWGs1UjU2bGZFc0lLUzFTTllFUWZXeVVRM1NRU05VcGZuQy9LVjRDb2lKUlBnMWZBUyt6YldHWTFQRmZ2dTEzK2I1SEZQOFpLbjRzQk0zTnhBOTkydi94dm1Nc2VTbUxnTFJ0cVJwdjhITEIxeTExVjhCSmx5TkNLcGt1bGErbjBmc0lQeFh2QjRuODZaeTBRTXIrbEtEZnc0NXgzQXNoMU5sdGtvYkRZUU5xRHR4ejVnemZlNG9OSEc5eWtNV3AwZGtoVWFzQ3EyUTZ0dDBlbzh0VmhFcFdvUWw4cWRvQTFISG5EUlhqOFdPL0laOFkyVXBnL3RmTFlhWnZBcnhReldaMHZVQURCakhKRy9qUFhHMmwwMkhOZlhkM2w4VWpleGlRM2JqcjMwTUlDcGRvNlk3b2lGYU5LNjZ5a2RFbXd3Zmw5ZHVtUERPdUYzUUpTdWdLQWN5TUxObklOUlg5Z2tkcUVndzE1U3U1UEJhZkxNZlU2OWpSVEJGaEhGRVNJcTdqUDBzMElFUmNud3RqdXFVeEdvS2NnS1d6anZjQ21Cbk5KSHY2L0lZa041cHdGS2VrUUVqOGRuSnp6cmhqeVBodGZPS25qdFRZOXNjV0xXMzZ0Y2JEQm9tTXM3ZDNEd29YT2k4TXVaS25Pd1RlZG5LcWVjYVlkTlpTbE5WWTNMZzl6eTZaYzRUamRjQ3hoSTltSjhuN01HajFzRVZ1QmNXTC90aHUxUkdzSDhHSVh1eWFBZGtkK0VuT3lUZkFuVmNxbEpTQyt6WDNmWklwS3REZCsxaXNpcHppYkIwQ1FDTnRUdElJRFVWcS93TTg4YW1JWTJ5RlVKb25UTmNvUm1CWXVUQ0xsTmdqckNleUVxVnJaOHdWZ1ZFNFJaSFF4Z1ZyWm5QWms3cmlWTVVTcmRKOGdGQTJWWEFQV1ZiOEdPVnhnMzdoa0xrZytIV3cwYlIzNjNtUlBhVHhNK2lMYzBkUm5QcVNtZUoxT0JSRk1UeHVDaGhRRnJaZGRBeWN3TkMxS1M3bXg4WXh5YnZXLzd2WjJUZDhQdStNTWZiVGpjazVYZlhjRnY2N2dQMDJFc2drMU4xR0xLMXBQczlKbWNLYWtnbnhsVE9DQ3BFNnR5MHN6U3BKbmxTZHR1bUMyaVgvZnhNcExrL2k1WWYvalFVWTJKZEFmbE1sdDZjMTBDa1ZRV1hySXgyMFBXQ2JFOG1xWDVLWHFYSkxDUCszalBXelhybEN3SlJCN0o4RENCdUN0T1VMYWliSnZKdGUrcUNHM0lHSk52a0hKUDBkaHVGOVNRZG9TbjJhaWdYaTBkcTZoQVlpdC9ta2s1WkJPMkxkcnhLYWRycWVuR3N3MVU0VW8xVkR0Rnd6bE1rMXFodnR2RHlYTW1wOVRsbkNndWlpQmtOOTc4VUdLRlRzeVRBU0ROWm5tSGViN2hsYkNPd1N0Yys4MlMrMjZzaFh1TjdpMUtMMFFyOTZnTEdyRDZYRW1PR3RVWllJR2pJWEYzMlk2MEZjbWRPRml0VGF4NVN5c2tTak5QM1JzWTFsdm1yNVRKNGxubXl1d3ExVE82STFVY2NBS0Y4YmFGWW5xQUxwc1UzSGxNNElFVHNYVzN3c210cTRaUUh5WEt2cjZKNTRwUTdhRXZxVkt4WnM5aXFQekl2Y2xzdHN3M2lyUG5uTWhqM200NmppZDRDdjE3aWVMNTB4dGkrT1BuZTIxRDZESmxucTVnQ3lsVER2SlppZktlalJEVXhVZ3ZMRDlSZmlGQUZ5Z2NVRktycmVta2VCdFVLZ0sycG5Ib1lqQUV4VmxnYnVQc1J4WW5RYjlNRjUvVjV1S1FUaUN5RUM5K1NvbFpoYXV2MGlIaHpiZEM0empKUlZCRTNSNm5oSXozUWZsYzF5YytLTHZoK1N2eks1cmFnbjNjd1pObzlmVjM4N1BXb3lOOVh6azZLQ0VQTW5XcE81Ui9JSG1XYkdmaTlkZ2sxVWd1S0hIYkRnOTBHcmRoZUpZckd4MnNsMHdqY2gzdmJKY3R6RE9ib2RqTUxtMDJSOTBiejdveG1SNGptVWQwcXlISjJpdUVUUlZGTkNrNnViNWdYOGl5RHhlOHJFVUpvRzlnMjhZU041ZjNzZ05wR2dTcEsvSjBxcW9RZ1BhRXcxTndaWGdscDF0ejRpb1NBcldaTlFXU1YzTmt0NjRVcnV4U3B3WFBqMjBkODJaMktlbWx5b0lLaTAvRDRpSmpnOWQ0Uk9PVjdsdVFYVlUrVjFnNTVrTlJ5L09LQ1FqSHNpYzluNVRCRmliMnJmcFZJWUpFQXlzYUx1YWRwaEpCUzIvU005Z1dZOStUWFAydXVvWlViaTZPVnJmRUVlOHRXK093YXhLWC9uU2pMb3czNnF6cGl3OTdIQjFjZHJuR0pwV1IraXhSNWNYSGNHbktqUlhISFpiSGpIdDRlcEthclh6VXY3bnE3ZmtVZENlRWsrM1Q2Sm5QVThjNGZXRkpIbWtCWlYrRmZtQ0g4Yld1OVhwNGlKUmJkUHFZNjJMVVplUGtCSFVSYWRXMW5XM2k1Tk5WdmdwQ2R1MVFFZzJpdkVkMmNhckFJbnlWV3YwWHB4VnI3bnk1bUdqeXdqeE5JSFJIWGdSRU5FWDJaU3lSV2dDZ2xHdCtlNjBvYWRsVFB5MXFvcGloS0h1VHRHWVpIMUl4T2J3ZlJuVUZaVEF2NHpLWWE4ckMxUXg3Zi9NL0ZlVnhoRWtVejg4ZDJIV0NlV2ZGemJTMDhnSGNxczVUTjhKUGE5eXNncGZ5a0lRWStTQmVmd3RuallNUHFGUWVCTHlRN0pWcE9NT0tMVkMxeW9MRVY1SlcrSlZLYTh3SXIxaHVLTG1mU2VMVXRWaWpGVzlSYnNGYmFNSkM5VXZmQ2xlYVNTNk40NWdxVGovdmY2ZE5VVzhoT1Ewend6V0dhVmEySXpPVUlERmFUaVFXQTI2SmhjSHlXM1U2V0RTeERSVGVLdXBaZU9sSlZnazloYkxYcW1wNHMwNmgzdVE2dHFQQ0YvRmd5OU9GbFpNTGVuLzNXeVpiTmtYczAzeTBLR2VHd28yNFdBU3F5SGJZbVhtaG1NdkpNbzdGY1RaUXo4RHJoWEFtazFMaVFxVTVoeGNrZ3VBOXVNZWVIeTYrc0s4SmtaMFRPZ2pBN0FJU0Z5cHFzTXJ4L3J1azBTeE02bm5pYUNJcEo3anp3alVuNUVXUVZjMHVzSlBwY3dpb2xWbEhLcHZZMEpQWUQxQmtNay9KdFZxbEhGTUlXaEYzY2s4RndqT2FuaTJVeEcyVy9ON2orMWZ5cXg3SVdYWjZGRHFFSmxjVTF4eENFV2JMQUo2TU9rUmVGR25QeFIwcnV1dTBUVVpiWGhJQVg3WWtMSGZWYzJyTExaTzVKZEl0b0ZEcE5KdElLa2taTFNwY3BzM1VDS04xaFp4ZkRDSmJCcW1oZ3VVS1FDc0FxdlhSQXFZWWFGR0p2cmNmWFp3eXlxQjNvQTIyTk5COU1XTVRmNzV3N012SC90V0IxbWQ5Tmh6Qi96WEt1M2NBT3J4bmF3eW9EdEIzb0NYZWZrOThGOFRZNUcwdmdUazBSdGtyMS9Ic3FSa2NhQlFEWG5xTnJEbDdYN2o3NW9EZmZITzRINEQ2d3F3RDdjVmd5RFl2QmtOdEkzKzNlTUFHQXpiRy8vWEd4UTh2b005OVl6eUh3V3pneFlrWHA0ZjNtZ2pUY3FYS0VGazBWOTEyb1ZhWk9OSFlLUzVXWmE5Lys1ZXlnbGtVS3RtOUphWEt4QlA2THVaYWdlYnQ2ZG5FdjlJYWx5amFYUGRrbm10aEt3c0V0KzdycUw0S2JlcG9DVU4vQWJvaEVGT1R3TDhHdlNWanMwZ0dLWXF0NVhyYzlhVVVxaUpBakw4SUFCOVBuUWkxYlp3VElRTkxqQkJOOXFiY250ZmdBVUEyTXUzTjNJbkkza1pISC9Cc0w0TDRXbkViamZZVkxEUzhUdGFKSTF4NmRnU0x3MmJtaGVtNFpEM0E0S2VGSDlxR2RpdEc3ZGlQVFpjSG5RUGtwMlJYN0s5OWNpUUxic0UyN21mMjVSTVM0aVJKQ1dtejRUMHAxc0FibGNVVlBzZzloRktxNGY4MlhUdU1wc281M1J4ZDliN2pBQ1ExOEY2TVdvV1M0OHo5eXZWdENqY01tUXM5UjJGeTBTK3lmeTRSMCt2a2RpdXA5ejFIWnFFdVN1SDhiVWNtKzVGWmRaclliK2RrQ3BJVm16bGhGRXVqT0VxWFB1SUZqelhDZVNzbm5GUWRpSTdmbmhocDNwR3N3ZktYUFRZQjlueTJ4OXJlM2tGZ0VONThtaGlQNkUyZWtlU2JEZjBiNjM3bml2NytabU1EV09sR2N0UG5CcDQyUXIzbUs3VUpmM2JvRnhPTFNZemQzRHUyNmVnMnliTGcxZkRveWZHWXVBZVRLcFVhU0VFYk9PbVNDNm53TGtISkVkZ3lmQm5FY3RmUGV6K0tqL3d3Wm42QW04RXg1WDArMEtDVy9Mb1hEcktUZ0RZc0owSTJoMGFRT3lucFlrSnFDU2RPU3VCTlJkRHVpeXdMSmpZcFBUN01Takl0OEFOdFpVbWM1eDRDS0xpN0JwUlRaL1VMWkVLYmJtMUZTVnhmSmFZbjluMDNha0ZOY2llRkpsRytFTFNsb01QWmtCTWkvRnVseE5td3paS2VEZk0xTFQreHc0eEpVeTVvNnRVTGltZVlHV2RucTVNRUU3eVoyOHJUUDg0ZjFyb0R2V0tzbzBYOWltejNiVDAxeEJjNVZMVW1BbnpFODJDdEMxc1JKWkk2WFBDWWtOMGtXQ1RxMHBMb3N0UFFzVWdGNlBJN2picDR6a3pKTHM2Y0lHcDAxeWpUbGVUZzVaeEM3ZE13RTF5M2FhZ0lUeExQMDkwYUlNUXVkUGpILzlLL3RNTW5acVFteHJWRElRckgzcUtBQ01VNStKeXEzamxQWENTa1ZJY2hoZFVqNm15VXhlRVpkQnoxY3FhZmQraW1oeWFJdHdhb2VHRjJBeWk1M1FQUkFiTUpTdjVMejczbTE0U1NmM2pGOE5RNTZTU29VM2pFcWlKT01QMCsyaXNBU090dEN5ODNhdzduSVJJNnE1UFFtWnFiM1VsNmUxYnNMZDRSWXpwZWxQUzMwK0Y5RURsdmlydThTR1N2bUtKVVpMRVVpdzZyV3FuMVpMQUhCK1Z4RUozU3QwNTZBc2xMb2xnTStnWUtodVVLRkR1Q1JiU1dtbDloUTVQRkk0UmhGZ1prUk1ETGJGMjJ0WmYzYy9OYjZYWU9NQ1Z1bXczNVdOS042TDVaV1BBRmkwcjFTMkhCS2YxL0RSSWFiTHlGRmpNc0pLQW0wa1lta2tiMlZzalVtSG0ydEJlRGJvNjM4cEFTQlkzaW5vV0lUUXVzUEs1Vis1LzFzOWdCNmxleHk3STBEV0lsQnkvenFKbUNuaVkvbjhOWHNnbDEyakVZN0hxSnV5RDR5aEZES0xiRkNud2xzTEpFTndNaFVCM01TZUs2akx0UTdmeCtUZnFaM2VXU0VscEQ0czBQTFhQVlRPZE9jRVBuNG1UcmtlNHNsS1NsVlVvelpTK2ZteGVVcUx0ZHlqbVZzTjNhTFRodjl1Ykl1Wk04TnVLbmREeVFWRm9WVjN4R1pVY09jcUMzZ3JXRXRrVTRtNXZlYWZORmZnVkZrWEREN3o3NlpFMU5NWVhXckN3MnFJWk4rbHVhYzh2SDRDR2VicXUrbTZJTThJaDk5K3UvWkw4a1VhQzJiUWg1TGxSeWZVQWRpSXRrTENIYkZXakFTeGVEUXJIVHRVZ2lZZXBMMURSZVNwUlhqczZzaEREakIrSDF4MUFFZGpmcThVQlRZcldZNVphdWhoWG1zY3phUEpYeTNUb3BZNlVMbUY0WkMxWmVSNUNrL1Jlelg5aktKaFhoN2hSbjRMdmYvRDNoL2J2Zi9JTzJPalVTMXJvY1Vydjcxc3EvTWd3U3NscXZ4ZVIrZzlZYWJkTEtuYlNaVmRLZVlSN2pRZjhtNmJ3K1luYngxdXhyOHZ6UytyaWp1TjFVNGlLREE1a1JXR0I2dGl0SzBYQitHWnJLdkRkVVFKVEM5anlhODJ5YThxcFVRbGozdFg4cE1yUFNON3pTdVNGb3QzVGZzOHpNOFNYeEMyVUg2YnhLWnRoTUFNamR4Sk1DalFkV3N0SGVvbVM3enBrUUlTbGN6N1A0dk9MWGRJNSt0dEtpSWs5YWhhK0g5OHhkMDVQUWUxWnhTdkEwZWNFeVgwLzdSZGI0dlkvalRYdGIzcS9Va1duTlNSak5OVEpINGN6d08rMFVmSFdkNlo0eTkra2FsRkh4Nnp0ZktNWnc3cTFKZlY2bEVkazBRMEhCb2VCeFJmMHY2ZnlLM3Bwcjl0YXM5TmJVcEFVbHZTM2FCMlZvTCtxajUxNm5WVEZUUWVuV21tdXc0Z2Q3anBLc3JLUnNjdWl3TlBWVVRGL3hrMUlwa3lrNWNUNXpPNjBLaWwyZEJRVVg3WXBaa21JRkN3a0Y5Y2tOUEFpd3BZVFdNb2dvbHc2a1hKZDJLSDQ0ZVI1S3l0UU1KTUtTSDZTN1pBdDdDVzNyUjlmZVZKYVI1aHpQTGxLQWxYTlBBL2VnWTl4cWVzNkJkZytUYktFREE3c25QL2dFY0o5K0NrQ05hQnI2cnZ1NUYvc284dWIybHRvbi9SMmJnR0IvQnBRNHRUR0p0b2JvYnJLL3lHVVlQbGhKNU9zZFBpMU5HVVpFUWtDeFpxTVE0Q01iMXU5VG5kTFpDOWZzcEhHR1dZVzZTb2FrQ2ZzWWtLenZOZTcwR1p4cUVpTlBsWkFLMTdoZ0g0ZkJrSUwyajNTZGhYcFRielZQUHBCdGN0R1lkSjVRUmxiSWlVVnBWZkFaYXpjZUp2TlNBdDlFWGhIUHdhbW0wTVhNaGk1RThTUFBXWkNPQVNMbHdtNlIrWjYzV2JIWExRUHBSa2Z5ZnJ2eUlpYVZTdjZDNU0zaXFTbGVBcmZ1M0ZTR21Kb1N4YjJ1amJDaGVQT2NrYWVsQWhjQ2FwQm1EU3ZjT0ppRW1oS1VUdm15UEc3UUwxRDFYdHRVSWw3d0NNUUFwVHFjZ1cycllwODMzL2FkOTc0Q05KbU5salZyR1pieEJINElqQnhkK2ZDWXpiWmpTaGh6WnVPOUVoamZIY3pSRTNCQ01YbUx3TVFMcStzYU5GWS9vc29LVFRNckpOS0crY2NHWlhweEpGT21zVHFPdmFuOUx3WEtibHBmVG0xWTRzWHJseitueTJwcVpIcWMzdlBUNVE2Z2VKa1J2MUZMTytrS1M5T05PTjN5WFM3aW90ejQwc1drdUppMEV6TnI4cHZjOEVLRUNEK2taaVpoL1F1ZlBMWi9SamRUU0pxZzdQeFE2SXYwaHZobzdvZnhkQmxIMmttcGZFVnp6M25uWkFueURFL1FyNHIyNVQ2ZERCTjBFVEt6clRsc3lCTko2U3JXVURvdzBVUTVIazJ1b2RNbE1PWmtUWGtmYWxaYWdqZWF2R3g1dXlac0hJY254NE1UK1VDS29pT0FVS25uRmhsYU1xam9ucFQ5Nk1zU1htWFROT1hUTkVVemlwWExUMVA1SkJWdHpGRGxlSHBDcnRHU2RjaExkTlJwdCs1TWdHTURnaHh2cVVoWFZzV0xEQ3ZpTUpTVUg1UkFZREtUNXF0U2RkbjFkY2o3cFNzaFk0Q05nazVlOHZzVmR2SjJXd3M4R2VOZVgrWXBNTi9WeEo0V21HNHJwb2lHMGFaRzR4ekpoQnYxanRQMmdqbkV3QTFzOCtTOGszU2tVN3BvSlplUVJEZXZ0RDQ5V0xRVWJiQ1JDc3g4UmxhdksrOFBTVUlmNnpLYW9rQzVKcjZhNzh1NFFmK3lXMTZhYzJWWjE1NjVjS2JNbktLT0Q3elk5ZEZJN3k5SUtDU0hXV2FHc2RvTGsxeGg5ZkIwSXRWTTZKdE1MNUZuZmMvd2x6NGtTQlJub2VCakFOUmhhM3UzQnJBM1ZJS3MyWThydGhwTTRuV01OdGNEamZ4YmU0UmpzYUdtNUdoWFpqZFRMa1pJdTdMNmJSa2xaK2Nvbk1JeWswOGdmRzlJL2tXSEJaNTl5ZWp1VmVGOUo1aGtJL1NqNkdYb25OSWRncHJwK2Q3MXdsOUdtcmk0bjZicWJkN1QxSGNIcEJLUFRPcWJtdDZGR2FteVdrd05pampFckI4R0QyS0V4K0ZvVDlua0ZSV0gxVTlTNEJVME5MUlVqVndaVm1oZWNnekMrTHQ0UEFyL0g0N3dUMGZkRnFMcGlud1dzUHBUTXpiMVVtMnlkYWxCVE93c3RxWkxQNDZtd0dJVUdhdEtFbVhBSmNvQUpFb3JreWNEaktzYWRWcGNlMGpIYzhmQkNYb0wwQk5hc3VEWEpQczFQR20rOW5CeFZUdzJEd0VZZXVsMTJjSXJIa1NuNzV2aFJTWWw3YjdpcWRYWlEvaXpDd3NVRS9rc1BMeG1aWEhWREFUVU1qUTdodXd6Wmd4MzhKNkVVM3djYnovQTJDcDhIQXhHQ0d3NEhyZm9VaklwMkxYUDhOSmVkTG5CSmc0QitoQmxjdnl4ejR3SDI5QmZZd3M2VEg2U05IcHpFbEhaSGpUZndaYTMxUTJTWng0MWVKaFRCRjAzWHlBUEtyQ1hrczl4Z3QyVFBVVm0yUTlLdlFBaHRhSVpKQm9zREFvUFJ4ODBmWnErRzJUdkpwUDAzZkNrTFpvWFY4TVNLZUdvY0ZpY21vWWxjc3EvdFNBR0pBVUVmaC9CMEt3M1Q0R0w4N25aNXVKTFFrbldzMEVYWDl3SHdoaURzRWVvS1g2REYrazNRbEh4Rzd4SXZxbHZyY3k5Z1Yya3dBY3RPL25aQWRJZTlpQjVndmI0azdvNTVkZnBSTmRnZjZTTGZtZ3NGUEd0WTNNNDZaU09xbHY1aUQyUWZzUk9aUjg3MnNwSmxFdmJNZ3BjNktlUFdiYjVoc04wejJkUFhyNCs2bVRtT0dieFJIeXBTUGJaaGloZVNMWmRVcXFuMnUwejBLNE10QmdRUHpLSHY1WFh6Wk5rUUZldmllYWNoeVIvNWNkelBPV1BmVHJEUTFXSm9wUXgxeVJHS0MrY0tNTHZHSUNjUlM2SDlzd09iVzhxRGpzV2lTRVZyN0VzTWQxMlZZTzM4VFpCYmNNTW5JMjBPY3E0RGEzNWx2M1QxNStqSXozZ0VZUUNQckl1WGh0dFR1ZjJMdE04dnhmRlpFTDlVSGZLNXhjQjVvZ1BWV0VjbUVqTmpKY1JiU3VqUGpEL2VJNEdNQlNnbnVFYzZ0b3A1a0JRQ0lsM1FnUFRLOWJxVFV4TEhhek5RdU50SkU1QkloMFdEN1FYZkJWY3o2Z0lVRThENTluU3kyTE1WZUhwbjhkc1lWNnp1WGxoZzd3YU9DR3NIS0J2MUUxU0Fyb004ZktHSmh0RWJzdlozNGltb1JQRWg1L3NiMkMyeE1OUDRHRWVMOXpEVC80L1AwSkFxNE4vQVFBPSIKKSkuZGVjb2RlKCJ1dGYtOCIpCgpwYXRoID0gc3lzLmFyZ3ZbMV0KdHJ5OgogICAgd2l0aCBvcGVuKHBhdGgsICJyIiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgICAgICBzcmMgPSBmLnJlYWQoKQpleGNlcHQgRmlsZU5vdEZvdW5kRXJyb3I6CiAgICBzcmMgPSAiIgoKaWYgInIyNWMyOV90cGwiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMjlfdHBsOiBhbHJlYWR5IGFwcGxpZWQiKQogICAgc3lzLmV4aXQoMCkKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKFBBR0UpCnByaW50KCJwYXRjaF9yMjVjMjlfdHBsOiBwbGF5ZXIgdjUuMSB3cml0dGVuIChjcmVhdGUtaWYtbWlzc2luZykiKQo="),
    ('patch_r25c30_diag.py',
     'web/wserver.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMzBfZGlhZy5weSAtIGRpYWdub3N0aWNzIGVuZHBvaW50IG9uIHRoZSB3ZWIgc2VydmVyLgoKQWRkcyBHRVQgL19kaWFnL2xvZ3MgKGF1dGg6IFgtRGlhZy1LZXkgaGVhZGVyIG9yID9rZXk9IHF1ZXJ5IHBhcmFtKQp0aGF0IHJldHVybnMgdGhlIHIyNWMyOSBzaGlwcGVyJ3MgYnVuZGxlLmpzb24gcGx1cyBhIGZyZXNoIHdzZXJ2ZXIubG9nCnRhaWwgYW5kIGxpdmUgcHJvY2Vzcy9zb2NrZXQgc3RhdGUuCgpXaHk6IHRoZSBrZXJuZWwgaGFzIE5PIEthZ2dsZSBBUEkgY3JlZGVudGlhbHMsIHNvIHRoZSByMjVjMjkgZGF0YXNldApwdXNoIGNhbiBuZXZlciBzdWNjZWVkIChwZXJtaXNzaW9uIHdhbGwpLiBUaGlzIGVuZHBvaW50IHNlcnZlcyB0aGUKc2FtZSBidW5kbGUgb3ZlciB0aGUgcHVibGljIHdvcmtlciBVUkwgaW5zdGVhZCwgd2hpY2ggZmV0Y2gtbG9ncy55bWwKY2FuIHJlYWNoLgoiIiIKaW1wb3J0IHN5cwoKX1daMzBfS0VZID0gInd6Zml4X3BJQmhDdHFnaHk3NVliS2pBcXFNNExPeE9ldmk2SFJQLUFFeSIKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMzAiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMzBfZGlhZzogYWxyZWFkeSBhcHBsaWVkIikKICAgIHN5cy5leGl0KDApCgpCTE9DSyA9ICcnJwojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQojIFdaRklYIHIyNWMzMDogZGlhZ25vc3RpY3MgZW5kcG9pbnQgKGxvZ3Mgc2VydmVkIG92ZXIgdGhlIHdzZXJ2ZXIpCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCl9XWjMwX0RJQUdfS0VZID0gInd6Zml4X3BJQmhDdHFnaHk3NVliS2pBcXFNNExPeE9ldmk2SFJQLUFFeSIKCgpAYXBwLmdldCgiL19kaWFnL2xvZ3MiKQphc3luYyBkZWYgd3ozMF9kaWFnX2xvZ3MocmVxdWVzdDogUmVxdWVzdCk6CiAgICAiIiJGdWxsIGRpYWdub3N0aWNzIGJ1bmRsZSBmb3IgcmVtb3RlIGZldGNoaW5nIChyMjVjMzApLiIiIgogICAgaW1wb3J0IGpzb24gYXMgX2ozMAogICAgaW1wb3J0IHN1YnByb2Nlc3MgYXMgX3NwMzAKICAgIGltcG9ydCB0aW1lIGFzIF90MzAKCiAgICBkZWYgX2szMCgpOgogICAgICAgIHJldHVybiAoCiAgICAgICAgICAgIHJlcXVlc3QuaGVhZGVycy5nZXQoIngtZGlhZy1rZXkiKQogICAgICAgICAgICBvciByZXF1ZXN0LnF1ZXJ5X3BhcmFtcy5nZXQoImtleSIpCiAgICAgICAgICAgIG9yICIiCiAgICAgICAgKQoKICAgIGlmIF9rMzAoKSAhPSBfV1ozMF9ESUFHX0tFWToKICAgICAgICByZXR1cm4gUmVzcG9uc2UoCiAgICAgICAgICAgIGNvbnRlbnQ9J3siZXJyb3IiOiAidW5hdXRob3JpemVkIn0nLAogICAgICAgICAgICBzdGF0dXNfY29kZT00MDEsCiAgICAgICAgICAgIG1lZGlhX3R5cGU9ImFwcGxpY2F0aW9uL2pzb24iLAogICAgICAgICkKCiAgICBkZWYgX3RhaWwzMChwLCBuKToKICAgICAgICB0cnk6CiAgICAgICAgICAgIHdpdGggb3BlbihwLCAiciIsIGVycm9ycz0icmVwbGFjZSIpIGFzIGY6CiAgICAgICAgICAgICAgICByZXR1cm4gIlxcbiIuam9pbihmLnJlYWQoKS5zcGxpdGxpbmVzKClbLW46XSkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgIHJldHVybiAiKHVucmVhZGFibGU6ICIgKyByZXByKGUpICsgIikiCgogICAgb3V0ID0geyJ0cyI6IF90MzAudGltZSgpLCAiZGlhZ192ZXJzaW9uIjogInIyNWMzMCJ9CiAgICB0cnk6CiAgICAgICAgd2l0aCBvcGVuKAogICAgICAgICAgICAiL2thZ2dsZS93b3JraW5nL3d6bWwtbG9ncy9idW5kbGUuanNvbiIsICJyIiwgZXJyb3JzPSJyZXBsYWNlIgogICAgICAgICkgYXMgZjoKICAgICAgICAgICAgb3V0WyJidW5kbGUiXSA9IF9qMzAubG9hZHMoZi5yZWFkKCkpCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgb3V0WyJidW5kbGUiXSA9IHsiZXJyb3IiOiByZXByKGUpfQogICAgb3V0WyJ3c2VydmVyX2xvZyJdID0gX3RhaWwzMCgiL2thZ2dsZS93b3JraW5nL3dzZXJ2ZXIubG9nIiwgNDAwKQogICAgb3V0WyJib3RfbG9nIl0gPSBfdGFpbDMwKCJsb2cudHh0IiwgMjAwKQogICAgdHJ5OgogICAgICAgIF9wcyA9IF9zcDMwLnJ1bigKICAgICAgICAgICAgWyJwcyIsICItZW8iLCAicGlkLGV0aW1lcyxjbWQiXSwKICAgICAgICAgICAgY2FwdHVyZV9vdXRwdXQ9VHJ1ZSwgdGV4dD1UcnVlLCB0aW1lb3V0PTE1LAogICAgICAgICkuc3Rkb3V0CiAgICAgICAgb3V0WyJwcm9jZXNzZXMiXSA9ICJcXG4iLmpvaW4oCiAgICAgICAgICAgIGwgZm9yIGwgaW4gX3BzLnNwbGl0bGluZXMoKQogICAgICAgICAgICBpZiAoImd1bmljb3JuIiBpbiBsIG9yICJjbG91ZGZsYXJlZCIgaW4gbCkKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgb3V0WyJwcm9jZXNzZXMiXSA9IHJlcHIoZSkKICAgIHRyeToKICAgICAgICBfc3MgPSBfc3AzMC5ydW4oCiAgICAgICAgICAgIFsic3MiLCAiLWx0biJdLCBjYXB0dXJlX291dHB1dD1UcnVlLCB0ZXh0PVRydWUsIHRpbWVvdXQ9MTUKICAgICAgICApLnN0ZG91dAogICAgICAgIG91dFsic29ja2V0cyJdID0gIlxcbiIuam9pbigKICAgICAgICAgICAgbCBmb3IgbCBpbiBfc3Muc3BsaXRsaW5lcygpCiAgICAgICAgICAgIGlmICI6ODA4MCAiIGluIGwgb3IgIjo4MDkxICIgaW4gbCBvciAiOjQ0MTYgIiBpbiBsCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIG91dFsic29ja2V0cyJdID0gcmVwcihlKQogICAgcmV0dXJuIFJlc3BvbnNlKAogICAgICAgIGNvbnRlbnQ9X2ozMC5kdW1wcyhvdXQsIGVuc3VyZV9hc2NpaT1GYWxzZSksCiAgICAgICAgbWVkaWFfdHlwZT0iYXBwbGljYXRpb24vanNvbiIsCiAgICApCgoKJycnCgpvbGQgPSAnQGFwcC5nZXQoIi9wbGF5bGlzdC97dG9rZW59IiwgcmVzcG9uc2VfY2xhc3M9SFRNTFJlc3BvbnNlKVxuYXN5bmMgZGVmIHBsYXlsaXN0X3BhZ2UodG9rZW46IHN0ciwgcmVxdWVzdDogUmVxdWVzdCk6JwpuZXcgPSBCTE9DSyArICJcbiIgKyBvbGQKYXNzZXJ0IHNyYy5jb3VudChvbGQpID09IDEsICJwbGF5bGlzdCBwYWdlIHJvdXRlIGFuY2hvciIKc3JjID0gc3JjLnJlcGxhY2Uob2xkLCBuZXcsIDEpCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaF9yMjVjMzBfZGlhZzogL19kaWFnL2xvZ3MgZW5kcG9pbnQgYWRkZWQgdG8gd3NlcnZlciIpCg=="),

    ('patch_r25c32_ct.py',
     'web/wserver.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMzJfY3QucHkgLSBmaXggdGhlIENsaWVudFRpbWVvdXQgTmFtZUVycm9yIGluIHdzZXJ2ZXIucHkuCgpUaGUgRmFzdEFQSSBhcHAncyBsaWZlc3BhbiBzdGFydHVwIGNhbGxzIENsaWVudFRpbWVvdXQoLi4uKSB3aXRob3V0CmltcG9ydGluZyBpdCwgc28gZ3VuaWNvcm4ncyB3b3JrZXIgZGllcyBhdCBib290IChleGl0IGNvZGUgMywgbWFzdGVyCnNodXRzIGRvd24pIGFuZCB0aGUgc2l0ZSA1MDJzIGZvcmV2ZXIuIFByZXBlbmQgYSBndWFyZGVkIGltcG9ydC4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMzIiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMzJfY3Q6IGFscmVhZHkgYXBwbGllZCIpCiAgICBzeXMuZXhpdCgwKQoKSEVBRCA9ICgKICAgICJ0cnk6XG4iCiAgICAiICAgIGZyb20gYWlvaHR0cCBpbXBvcnQgQ2xpZW50VGltZW91dCAgIyBXWkZJWCByMjVjMzJcbiIKICAgICJleGNlcHQgSW1wb3J0RXJyb3I6ICAjIHZlcnkgb2xkIGFpb2h0dHBcbiIKICAgICIgICAgZnJvbSBhaW9odHRwLmNsaWVudCBpbXBvcnQgQ2xpZW50VGltZW91dCAgIyBXWkZJWCByMjVjMzJcbiIKKQpzcmMgPSBIRUFEICsgc3JjCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaF9yMjVjMzJfY3Q6IENsaWVudFRpbWVvdXQgaW1wb3J0IHByZXBlbmRlZCB0byB3c2VydmVyLnB5IikK"),

    ('patch_r25c33_al.py',
     'bot/core/startup.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9yMjVjMzNfYWwucHkgLSBndW5pY29ybiBhY2Nlc3MgbG9nIG9uIChyZXF1ZXN0ICsgc3RhdHVzIGxpbmVzKS4KCkV2ZXJ5IEhUVFAgcmVxdWVzdCB0byB0aGUgd2ViIHNlcnZlciBub3cgbGFuZHMgaW4gd3NlcnZlci5sb2cgd2l0aCBpdHMKc3RhdHVzIGNvZGUsIHNvIHJlbW90ZSBsb2cgcHVsbHMgKC9fZGlhZy9sb2dzKSBzaG93IGV4YWN0bHkgd2hhdCB0aGUKYnJvd3NlciBhc2tlZCBmb3IgYW5kIHdoYXQgdGhlIHNlcnZlciBhbnN3ZXJlZC4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCmlmICJyMjVjMzMiIGluIHNyYzoKICAgIHByaW50KCJwYXRjaF9yMjVjMzNfYWw6IGFscmVhZHkgYXBwbGllZCIpCiAgICBzeXMuZXhpdCgwKQoKbzEgPSAnZiIgLS1iaW5kIDAuMC4wLjA6e1BPUlR9ID4+IC9rYWdnbGUvd29ya2luZy93c2VydmVyLmxvZyAyPiYxIiwnCm4xID0gJ2YiIC0tYmluZCAwLjAuMC4wOntQT1JUfSAtLWFjY2Vzcy1sb2dmaWxlIC0gPj4gL2thZ2dsZS93b3JraW5nL3dzZXJ2ZXIubG9nIDI+JjEiLCcKbzIgPSAnZiJ3ZWIud3NlcnZlcjphcHAgLS1iaW5kIDAuMC4wLjA6e1BPUlR9ICInCm4yID0gJ2Yid2ViLndzZXJ2ZXI6YXBwIC0tYmluZCAwLjAuMC4wOntQT1JUfSAtLWFjY2Vzcy1sb2dmaWxlIC0gIicKYzEsIGMyID0gc3JjLmNvdW50KG8xKSwgc3JjLmNvdW50KG8yKQpzcmMgPSBzcmMucmVwbGFjZShvMSwgbjEsIDEpCnNyYyA9IHNyYy5yZXBsYWNlKG8yLCBuMiwgMSkKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKHNyYykKcHJpbnQoZiJwYXRjaF9yMjVjMzNfYWw6IGFjY2VzcyBsb2cgb24gKGJvb3QgbGluZToge2MxfSwgd2F0Y2hkb2cgbGluZToge2MyfSkiKQo="),

]


# ============================================================================
# SECTION 5 — PATCH APPLICATION
# ============================================================================

def write_patch_scripts():
    """Decode and write all embedded patch scripts to PATCH_TMP_DIR."""
    os.makedirs(PATCH_TMP_DIR, exist_ok=True)
    for name, _target, b64_content in PATCH_DATA:
        patch_path = os.path.join(PATCH_TMP_DIR, name)
        script_content = base64.b64decode(b64_content).decode("utf-8")
        with open(patch_path, "w", encoding="utf-8") as f:
            f.write(script_content)
        os.chmod(patch_path, 0o755)
    log(f"Wrote {len(PATCH_DATA)} patch scripts to {PATCH_TMP_DIR}")


def apply_patches():
    """Run each patch script against its target file via subprocess."""
    log("=" * 60)
    log("Applying source patches")
    log("=" * 60)
    # These patches are superseded by the WZML-X-Bot patch kit (the battle-
    # tested patch set from the user's GitHub Actions deployment), which is
    # applied right after this step by apply_userrepo_patches().
    retired = {
        "patch_db.py", "patch_tstream.py", "patch_sserv.py", "patch_tmon.py",
        "patch7_user.py", "patch8_sserv.py", "patch9_html.py", "patch10_ws.py",
        "patch_r25c27_tpl.py",  # WZFIX r25c29: superseded (never applied on fresh clones)
    }
    creating = {"patch_r25c29_tpl.py"}  # WZFIX r25c29: write-if-missing
    for name, target_rel, _b64 in PATCH_DATA:
        if name in retired:
            log(f"  {name}: superseded by WZML-X-Bot patch kit — skipping")
            continue
        patch_path = os.path.join(PATCH_TMP_DIR, name)
        target_path = os.path.join(WZMLX_DIR, target_rel)
        if name not in creating and not os.path.isfile(target_path):
            log(f"  {name}: TARGET NOT FOUND — {target_path}", "ERROR")
            continue
        try:
            result = subprocess.run(
                [sys.executable, patch_path, target_path],
                capture_output=True,
                text=True,
                timeout=30,
            )
            for line in result.stdout.strip().split("\n"):
                if line:
                    log(f"  {name}: {line}")
            if result.stderr.strip():
                for line in result.stderr.strip().split("\n"):
                    log(f"  {name} STDERR: {line}", "WARN")
            if result.returncode != 0:
                log(f"  {name}: exited with code {result.returncode}", "WARN")
        except Exception as e:
            log(f"  {name}: FAILED — {e}", "ERROR")
    log("All patches applied")


# ============================================================================
# WZML-X-BOT PATCH KIT (user stream + UI + stream authentication)
# ============================================================================
# The patch kit is downloaded at runtime from the user's own repository
# (hackaking20/WZML-X-Bot) — the same patch set the working GitHub Actions
# deployment applies, in the same order. Keeping it runtime-fetched means any
# future tweak to that repo flows into the Kaggle bot automatically.

AUTH_BANNER_HTML = """<style>
#wzml-auth-gate{position:fixed;inset:0;z-index:2147483647;background:rgba(4,6,12,.94);backdrop-filter:blur(10px);display:flex;align-items:center;justify-content:center;font-family:system-ui,-apple-system,sans-serif;color:#e8ecf7}
#wzml-auth-gate .wag-box{background:var(--surface,#10141f);border:1px solid var(--line,#2a3350);border-radius:14px;padding:34px 30px;width:min(92vw,380px);text-align:center;box-shadow:0 20px 60px rgba(0,0,0,.55)}
#wzml-auth-gate .wag-ico{font-size:36px;margin-bottom:10px}
#wzml-auth-gate h3{color:var(--text,#e8ecf7);font-size:18px;margin:0 0 8px;font-weight:600}
#wzml-auth-gate p{color:var(--muted,#8b94ad);font-size:13px;margin:0 0 20px;line-height:1.5}
#wzml-auth-gate input{width:100%;padding:12px 14px;border-radius:9px;border:1px solid #2a3350;background:var(--bg,#0a0d16);color:var(--text,#e8ecf7);font-size:14px;outline:none;margin-bottom:12px;box-sizing:border-box}
#wzml-auth-gate input:focus{border-color:var(--accent-2,#5b9dff)}
#wzml-auth-gate button{width:100%;padding:12px;border:none;border-radius:9px;background:linear-gradient(135deg,var(--accent,#5b9dff),var(--accent-2,#7d6bff));color:#fff;font-size:14px;font-weight:600;cursor:pointer}
#wzml-auth-gate .wag-err{color:#ff6b6b;font-size:12px;margin-top:10px;display:none}
</style>
<script>
(function(){
  var qs = new URLSearchParams(location.search);
  var tok = qs.get('auth') || '';
  try { if(!tok) tok = localStorage.getItem('wzml_stream_auth') || ''; } catch(e){}
  function addAuth(u){
    try{
      if(!tok || !u) return u;
      if(u.charAt(0) === '#') return u;
      if(u.indexOf('auth=') >= 0) return u;
      if(u.indexOf('/api/stream_auth') >= 0) return u;
      return u + (u.indexOf('?') >= 0 ? '&' : '?') + 'auth=' + encodeURIComponent(tok);
    }catch(e){ return u; }
  }
  function rewrite(root){
    try{
      var els = root.querySelectorAll('video, audio, source, track, a[href]');
      for(var i=0;i<els.length;i++){
        var el = els[i];
        if(el.getAttribute('data-wzauth') === '1') continue;
        var s = el.getAttribute('src');
        if(s !== null){
          var ns = addAuth(s);
          if(ns !== s) el.setAttribute('src', ns);
        }
        var h = el.getAttribute('href');
        if(h !== null){
          var nh = addAuth(h);
          if(nh !== h) el.setAttribute('href', nh);
        }
        el.setAttribute('data-wzauth', '1');
      }
    }catch(e){}
  }
  function armRewrites(){
    if(!tok) return;
    rewrite(document);
    try{
      new MutationObserver(function(){ rewrite(document); })
        .observe(document.documentElement, {subtree:true, childList:true});
    }catch(e){}
  }
  fetch('/api/stream_auth', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: '{}'
  }).then(function(r){
    return r.json().catch(function(){ return {}; });
  }).then(function(j){
    var passSet = !(j && j.error === 'STREAM_PASS not set');
    if(!passSet){ armRewrites(); return; }
    if(tok){ armRewrites(); return; }
    showBanner();
  }).catch(function(){ armRewrites(); });
  function showBanner(){
    if(document.getElementById('wzml-auth-gate')) return;
    var d = document.createElement('div');
    d.id = 'wzml-auth-gate';
    d.innerHTML = '<div class="wag-box">' +
      '<div class="wag-ico">🔒</div>' +
      '<h3>Authenticate first</h3>' +
      '<p>This stream is protected. Enter the stream password to continue.</p>' +
      '<input id="wag-pass" type="password" placeholder="Stream password" autocomplete="current-password">' +
      '<button id="wag-go">Continue</button>' +
      '<div class="wag-err" id="wag-err">Wrong password — try again.</div>' +
      '</div>';
    (document.body || document.documentElement).appendChild(d);
    function submit(){
      var p = document.getElementById('wag-pass').value;
      document.getElementById('wag-err').style.display = 'none';
      fetch('/api/stream_auth', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({password: p})
      }).then(function(r){ return r.json().catch(function(){return {};}); }).then(function(j){
        if(j && j.token){
          try{ localStorage.setItem('wzml_stream_auth', j.token); }catch(e){}
          var u = new URL(location.href);
          u.searchParams.set('auth', j.token);
          location.replace(u.toString());
        } else {
          document.getElementById('wag-err').style.display = 'block';
        }
      }).catch(function(){
        document.getElementById('wag-err').style.display = 'block';
      });
    }
    document.getElementById('wag-go').addEventListener('click', submit);
    document.getElementById('wag-pass').addEventListener('keydown', function(ev){
      if(ev.key === 'Enter') submit();
    });
    setTimeout(function(){ try{ document.getElementById('wag-pass').focus(); }catch(e){} }, 50);
  }
})();
</script>"""




# ─── v15.7 Round 1 modules (quota engine + owner commands) ────────────

# bot/helper/wzfix/r1_core.py — per-user daily bandwidth quota + download
# library + DB stats/cleanup. Fail-open by design.
WZFIX_R1_CORE_B64 = (
    "IyBXWkZJWCBSb3VuZCAxICh2MTUuNykg4oCUIHBlci11c2VyIGJhbmR3aWR0aCBxdW90YSArIGRvd25sb2FkIGxpYnJhcnkuCiMK"
    "IyBDb2xsZWN0aW9ucyAoYWxsIHBhcnRpdGlvbmVkIHBlci1ib3QgbGlrZSB1cHN0cmVhbTogZGIuPG5hbWU+LjxwYXJ0aXRpb24+"
    "KToKIyAgIHd6Zml4X3VzZXJzICAg4oCUIG9uZSBkb2MgcGVyIHVzZXI6IHVzZXJuYW1lLCBkaXNwbGF5IG5hbWUsIGN1c3RvbSBj"
    "YXAsCiMgICAgICAgICAgICAgICAgICAgbGlmZXRpbWUgdXNhZ2UuIChubyBUVEwg4oCUIGl0IGlzIHRoZSB1c2VyIHJlZ2lzdHJ5"
    "KQojICAgd3pmaXhfcXVvdGEgICDigJQgb25lIGRvYyBwZXIgdXNlciBwZXIgSVNUIGRheTogeyJfaWQiOiAiPHVpZD46PFlZWVkt"
    "TU0tREQ+IiwKIyAgICAgICAgICAgICAgICAgICAidXNlZCI6IGJ5dGVzLCAiZXhwaXJlQXQiOiArN2QgVFRMfQojICAgd3pmaXhf"
    "bGlicmFyeSDigJQgb25lIGRvYyBwZXIgY29tcGxldGVkIHRhc2s6IG5hbWUsIHNpemUsIHVzZXIsIGRhdGUsCiMgICAgICAgICAg"
    "ICAgICAgICAgdGVsZWdyYW0gcGFydCBsaW5rcyBbKGNoYXRfaWQsIG1zZ19pZCksIC4uLl0sIGNsb3VkIGxpbmtzLgojICAgICAg"
    "ICAgICAgICAgICAgIDkwLWRheSBUVEwga2VlcHMgaXQgc21hbGwuCiMgICB3emZpeF9jb25maWcgIOKAlCB7Il9pZCI6ICJnbG9i"
    "YWwiLCAiYm90X2NhcF9nYiI6IGZsb2F0fSDigJQgZ2xvYmFsIGRlZmF1bHQgY2FwLgojCiMgRW5mb3JjZW1lbnQgcG9saWN5Ogoj"
    "ICAgKiBPV05FUiBhbmQgU1VETyB1c2VycyBhcmUgZXhlbXB0IChtYXRjaGVzIHVwc3RyZWFtIGxpbWl0IGJlaGF2aW91cikuCiMg"
    "ICAqIEEgdGFzayBpcyBibG9ja2VkIHdoZW4gdGhlIHVzZXIgaGFzIGFscmVhZHkgdXNlZCA+PSBjYXAsIG9yIHdoZW4gdGhlCiMg"
    "ICAgIHRhc2sgc2l6ZSB3b3VsZCBvdmVyc2hvb3QgdGhlIHJlbWFpbmluZyBhbGxvd2FuY2UgYnkgPiAxMDAgTUIuCiMgICAqIENo"
    "YXJnaW5nIGhhcHBlbnMgYXQgdXBsb2FkIGNvbXBsZXRpb24gKGFjdHVhbCBzaXplIG9mIHRoZSBmaW5pc2hlZCB0YXNrKSwKIyAg"
    "ICAgc28gdW5rbm93bi1zaXplIGRvd25sb2FkcyBzdGlsbCBjb3VudCBvbmNlIGNvbXBsZXRlLgojICAgKiBSRVNFUlZBVElPTlM6"
    "IHRoZSBtb21lbnQgYSB0YXNrIHBhc3NlcyB0aGUgc2l6ZSBjaGVjayBpdHMgYmFuZHdpZHRoIGlzCiMgICAgIGhlbGQgYXRvbWlj"
    "YWxseSAoJGluYyBvbiB0aGUgZGF5IGRvYywgdmVyaWZpZWQgc2VydmVyLXNpZGUgaW4gb25lIG9wKSwKIyAgICAgc28gc2ltdWx0"
    "YW5lb3VzIHRhc2tzIGNhbiBuZXZlciBlYWNoIHBhc3MgYWdhaW5zdCBhIHN0YWxlIG51bWJlci4KIyAgICAgUmVsZWFzZWQgb24g"
    "Y29tcGxldGlvbiwgZXJyb3Igb3IgY2FuY2VsOyByZXNldCBvbiBib290LgojICAgKiBFdmVyeSBjb2RlIHBhdGggZmFpbHMgT1BF"
    "TjogYSBxdW90YSBidWcgbXVzdCBuZXZlciBicmVhayBhIGRvd25sb2FkLgoKZnJvbSBkYXRldGltZSBpbXBvcnQgZGF0ZXRpbWUs"
    "IHRpbWVkZWx0YSwgdGltZXpvbmUKZnJvbSByZSBpbXBvcnQgZXNjYXBlIGFzIF9yZXNjYXBlCmZyb20gdGltZSBpbXBvcnQgdGlt"
    "ZQpmcm9tIHV1aWQgaW1wb3J0IHV1aWQ0Cgpmcm9tIHB5bW9uZ28gaW1wb3J0IFJldHVybkRvY3VtZW50Cgpmcm9tIC4uLiBpbXBv"
    "cnQgTE9HR0VSCmZyb20gLi4uY29yZS5jb25maWdfbWFuYWdlciBpbXBvcnQgQ29uZmlnCgpJU1QgPSB0aW1lem9uZSh0aW1lZGVs"
    "dGEoaG91cnM9NSwgbWludXRlcz0zMCkpClVUQyA9IHRpbWV6b25lLnV0YwpHQiA9IDEwMjQqKjMKTUIgPSAxMDI0KioyCldaRklY"
    "X0dSQUNFX01CID0gMTAwICAgICAgICAgICMgYWxsb3dlZCBvdmVyc2hvb3QgYmV5b25kIHRoZSBkYWlseSBjYXAKV1pGSVhfQldf"
    "RkFDVE9SID0gMiAgICAgICAgICAgIyBhIHRhc2sncyBiYW5kd2lkdGggPSBkb3dubG9hZCArIHVwbG9hZCAoMnggZmlsZSBzaXpl"
    "KQpXWkZJWF9ERUZBVUxUX0NBUF9HQiA9IDE1ICAgICAjIHVzZWQgd2hlbiBubyBnbG9iYWwgb3ZlcnJpZGUgaXMgc3RvcmVkClda"
    "RklYX0xJQl9UVExfREFZUyA9IDkwCldaRklYX1FVT1RBX1RUTF9EQVlTID0gNwpXWkZJWF9BVExBU19GUkVFID0gNTEyICogTUIg"
    "ICMgTW9uZ29EQiBNMCBmcmVlIHRpZXIgKDUxMiBNQiwgbm90IEdCISkKCl9vd25lcl91c2VybmFtZSA9IE5vbmUKX3JlYWR5ID0g"
    "RmFsc2UKCgpkZWYgX3BhcnQoKToKICAgIGZyb20gLi4uY29yZS50Z19jbGllbnQgaW1wb3J0IFRnQ2xpZW50CgogICAgaWYgVGdD"
    "bGllbnQuUEFSVElUSU9OOgogICAgICAgIHJldHVybiBUZ0NsaWVudC5QQVJUSVRJT04KICAgIGZyb20gLi4uY29yZS50Z19jbGll"
    "bnQgaW1wb3J0IGRiX3BhcnRpdGlvbl9pZAoKICAgIHJldHVybiBkYl9wYXJ0aXRpb25faWQoQ29uZmlnLkJPVF9UT0tFTi5zcGxp"
    "dCgiOiIsIDEpWzBdKQoKCmRlZiBfZGIoKToKICAgIGZyb20gLi5leHRfdXRpbHMuZGJfaGFuZGxlciBpbXBvcnQgZGF0YWJhc2UK"
    "CiAgICByZXR1cm4gZGF0YWJhc2UuZGIKCgpkZWYgX2RheV9pc3QoKToKICAgIHJldHVybiBkYXRldGltZS5ub3coSVNUKS5zdHJm"
    "dGltZSgiJVktJW0tJWQiKQoKCmRlZiBfbmljZV9zaXplKG4pOgogICAgdHJ5OgogICAgICAgIGZyb20gLi5leHRfdXRpbHMuc3Rh"
    "dHVzX3V0aWxzIGltcG9ydCBnZXRfcmVhZGFibGVfZmlsZV9zaXplCgogICAgICAgIHJldHVybiBnZXRfcmVhZGFibGVfZmlsZV9z"
    "aXplKGludChuKSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIGYie259IEIiCgoKYXN5bmMgZGVmIGVuc3Vy"
    "ZV9yZWFkeSgpOgogICAgIiIiQ3JlYXRlIGluZGV4ZXMgb25jZSBwZXIgcHJvY2Vzcy4gUmV0dXJucyBGYWxzZSB3aGVuIERCIGlz"
    "IHVudXNhYmxlLiIiIgogICAgZ2xvYmFsIF9yZWFkeQogICAgaWYgX3JlYWR5OgogICAgICAgIHJldHVybiBUcnVlCiAgICB0cnk6"
    "CiAgICAgICAgZGIgPSBfZGIoKQogICAgICAgIGlmIGRiIGlzIE5vbmU6CiAgICAgICAgICAgIHJldHVybiBGYWxzZQogICAgICAg"
    "IHBhcnQgPSBfcGFydCgpCiAgICAgICAgYXdhaXQgZGIud3pmaXhfcXVvdGFbcGFydF0uY3JlYXRlX2luZGV4KCJleHBpcmVBdCIs"
    "IGV4cGlyZUFmdGVyU2Vjb25kcz0wKQogICAgICAgIGF3YWl0IGRiLnd6Zml4X2xpYnJhcnlbcGFydF0uY3JlYXRlX2luZGV4KCJl"
    "eHBpcmVBdCIsIGV4cGlyZUFmdGVyU2Vjb25kcz0wKQogICAgICAgIGF3YWl0IGRiLnd6Zml4X2xpYnJhcnlbcGFydF0uY3JlYXRl"
    "X2luZGV4KCJuYW1lIikKICAgICAgICBhd2FpdCBkYi53emZpeF9saWJyYXJ5W3BhcnRdLmNyZWF0ZV9pbmRleCgidXNlcl9pZCIp"
    "CiAgICAgICAgYXdhaXQgZGIud3pmaXhfdXNlcnNbcGFydF0uY3JlYXRlX2luZGV4KCJsYXN0X3VzZWQiKQogICAgICAgICMgYSBm"
    "cmVzaCBwcm9jZXNzIGhhcyBubyBsaXZlIHRhc2tzOiByZXNldCBldmVyeSByZXNlcnZhdGlvbiBob2xkIHNvCiAgICAgICAgIyBh"
    "IGJvdCByZXN0YXJ0IGNhbiBuZXZlciBsZWF2ZSBhIHVzZXIncyBhbGxvd2FuY2UgZnJvemVuCiAgICAgICAgYXdhaXQgZGIud3pm"
    "aXhfcXVvdGFbcGFydF0udXBkYXRlX21hbnkoe30sIHsiJHNldCI6IHsicmVzZXJ2ZWQiOiAwfX0pCiAgICAgICAgYXdhaXQgZGIu"
    "d3pmaXhfcHJlaG9sZFtwYXJ0XS5kZWxldGVfbWFueSh7fSkKICAgICAgICBfcmVhZHkgPSBUcnVlCiAgICAgICAgcmV0dXJuIFRy"
    "dWUKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICBMT0dHRVIuZXJyb3IoZiJXWkZJWCBlbnN1cmVfcmVhZHkgZmFp"
    "bGVkOiB7ZX0iKQogICAgICAgIHJldHVybiBGYWxzZQoKCmFzeW5jIGRlZiBnZXRfb3duZXJfdXNlcm5hbWUoKToKICAgICIiIlJl"
    "c29sdmUgdGhlIG93bmVyJ3MgQHVzZXJuYW1lIG9uY2UgKGZvciBxdW90YSBtZXNzYWdlcykuIiIiCiAgICBnbG9iYWwgX293bmVy"
    "X3VzZXJuYW1lCiAgICBpZiBfb3duZXJfdXNlcm5hbWU6CiAgICAgICAgcmV0dXJuIF9vd25lcl91c2VybmFtZQogICAgdHJ5Ogog"
    "ICAgICAgIGZyb20gLi4uY29yZS50Z19jbGllbnQgaW1wb3J0IFRnQ2xpZW50CgogICAgICAgIHUgPSBhd2FpdCBUZ0NsaWVudC5i"
    "b3QuZ2V0X3VzZXJzKENvbmZpZy5PV05FUl9JRCkKICAgICAgICBfb3duZXJfdXNlcm5hbWUgPSBnZXRhdHRyKHUsICJ1c2VybmFt"
    "ZSIsIE5vbmUpIG9yICIiCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIF9vd25lcl91c2VybmFtZSA9ICIiCiAgICByZXR1"
    "cm4gX293bmVyX3VzZXJuYW1lIG9yIE5vbmUKCgphc3luYyBkZWYgYWRtaW5fbG9nKGV2ZW50LCB0ZXh0KToKICAgICIiIldaRklY"
    "IGV2ZXJ5dGhpbmctbG9nOiBldmVyeSBldmVudCB3b3J0aCByZWNvcmRpbmcgZ29lcyB0bwogICAgQURNSU5fTE9HX0NIQVQgKGZh"
    "bGxiYWNrIExPR19DSEFUIHdoZW4gdW5zZXQpLiBUaGUgb3duZXIncyBETSBvbmx5CiAgICByZWNlaXZlcyBzdGFydHVwL3N0b3As"
    "IHNlbnQgYnkgdGhlIG5vdGVib29rIHJ1bm5lci4iIiIKICAgIHRyeToKICAgICAgICBmcm9tIC4uLmNvcmUuY29uZmlnX21hbmFn"
    "ZXIgaW1wb3J0IENvbmZpZwogICAgICAgIGZyb20gLi4uY29yZS50Z19jbGllbnQgaW1wb3J0IFRnQ2xpZW50CgogICAgICAgIGNo"
    "YXQgPSBzdHIoZ2V0YXR0cihDb25maWcsICJBRE1JTl9MT0dfQ0hBVCIsICIiKSBvciAiIikuc3RyaXAoKQogICAgICAgIGlmIG5v"
    "dCBjaGF0OgogICAgICAgICAgICBjaGF0ID0gc3RyKGdldGF0dHIoQ29uZmlnLCAiTE9HX0NIQVQiLCAiIikgb3IgIiIpLnN0cmlw"
    "KCkKICAgICAgICBpZiBub3QgY2hhdDoKICAgICAgICAgICAgcmV0dXJuCiAgICAgICAgIyBweXJvZ3JhbSB0cmVhdHMgc3RyaW5n"
    "IGNoYXQgaWRzIGFzIEB1c2VybmFtZXMg4oCUIG51bWVyaWMgaWRzCiAgICAgICAgIyBNVVNUIGJlIGludCBvciB0aGUgc2VuZCBm"
    "YWlscyB3aXRoIHBlZXItbm90LWZvdW5kCiAgICAgICAgaWYgY2hhdC5sc3RyaXAoIi0iKS5pc2RpZ2l0KCk6CiAgICAgICAgICAg"
    "IGNoYXQgPSBpbnQoY2hhdCkKICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Quc2VuZF9tZXNzYWdlKAogICAgICAgICAgICBjaGF0"
    "X2lkPWNoYXQsCiAgICAgICAgICAgIHRleHQ9ZiJ7ZXZlbnR9XG57dGV4dH0iWzozOTAwXSwKICAgICAgICAgICAgZGlzYWJsZV93"
    "ZWJfcGFnZV9wcmV2aWV3PVRydWUsCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBfYWxfZToKICAgICAgICBMT0dH"
    "RVIuZXJyb3IoZiJXWkZJWCBhZG1pbl9sb2cgc2VuZCBmYWlsZWQ6IHtfYWxfZX0iKQoKCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACiMgUXVvdGEKIyDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgX2dldF9nbG9i"
    "YWxfY2FwX2diKCk6CiAgICB0cnk6CiAgICAgICAgZG9jID0gYXdhaXQgX2RiKCkud3pmaXhfY29uZmlnW19wYXJ0KCldLmZpbmRf"
    "b25lKHsiX2lkIjogImdsb2JhbCJ9KQogICAgICAgIGlmIGRvYyBhbmQgZG9jLmdldCgiYm90X2NhcF9nYiIpOgogICAgICAgICAg"
    "ICByZXR1cm4gZmxvYXQoZG9jWyJib3RfY2FwX2diIl0pCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJl"
    "dHVybiBXWkZJWF9ERUZBVUxUX0NBUF9HQgoKCmFzeW5jIGRlZiBzZXRfZ2xvYmFsX2NhcF9nYihnYik6CiAgICBhd2FpdCBlbnN1"
    "cmVfcmVhZHkoKQogICAgYXdhaXQgX2RiKCkud3pmaXhfY29uZmlnW19wYXJ0KCldLnVwZGF0ZV9vbmUoCiAgICAgICAgeyJfaWQi"
    "OiAiZ2xvYmFsIn0sIHsiJHNldCI6IHsiYm90X2NhcF9nYiI6IGZsb2F0KGdiKX19LCB1cHNlcnQ9VHJ1ZQogICAgKQoKCmFzeW5j"
    "IGRlZiBnZXRfdXNlcl9kb2ModXNlcl9pZCk6CiAgICB0cnk6CiAgICAgICAgcmV0dXJuIGF3YWl0IF9kYigpLnd6Zml4X3VzZXJz"
    "W19wYXJ0KCldLmZpbmRfb25lKHsiX2lkIjogdXNlcl9pZH0pIG9yIHt9CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJl"
    "dHVybiB7fQoKCmFzeW5jIGRlZiBnZXRfY2FwX2J5dGVzKHVzZXJfaWQpOgogICAgZG9jID0gYXdhaXQgZ2V0X3VzZXJfZG9jKHVz"
    "ZXJfaWQpCiAgICBpZiBkb2MuZ2V0KCJjYXBfZ2IiKSBpcyBub3QgTm9uZToKICAgICAgICByZXR1cm4gZmxvYXQoZG9jWyJjYXBf"
    "Z2IiXSkgKiBHQgogICAgcmV0dXJuIGF3YWl0IF9nZXRfZ2xvYmFsX2NhcF9nYigpICogR0IKCgphc3luYyBkZWYgZ2V0X3VzYWdl"
    "KHVzZXJfaWQsIGRheT1Ob25lKToKICAgIGRheSA9IGRheSBvciBfZGF5X2lzdCgpCiAgICB0cnk6CiAgICAgICAgZG9jID0gYXdh"
    "aXQgX2RiKCkud3pmaXhfcXVvdGFbX3BhcnQoKV0uZmluZF9vbmUoeyJfaWQiOiBmInt1c2VyX2lkfTp7ZGF5fSJ9KQogICAgICAg"
    "IHJldHVybiBpbnQoZG9jLmdldCgidXNlZCIsIDApKSBpZiBkb2MgZWxzZSAwCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAg"
    "IHJldHVybiAwCgoKYXN5bmMgZGVmIHJlc2V0X3VzYWdlKHVzZXJfaWQsIGRheT1Ob25lKToKICAgIGRheSA9IGRheSBvciBfZGF5"
    "X2lzdCgpCiAgICBhd2FpdCBfZGIoKS53emZpeF9xdW90YVtfcGFydCgpXS5kZWxldGVfb25lKHsiX2lkIjogZiJ7dXNlcl9pZH06"
    "e2RheX0ifSkKCgphc3luYyBkZWYgdXNlcl9leGlzdHModXNlcl9pZCk6CiAgICB0cnk6CiAgICAgICAgcmV0dXJuICgKICAgICAg"
    "ICAgICAgYXdhaXQgX2RiKCkud3pmaXhfdXNlcnNbX3BhcnQoKV0uZmluZF9vbmUoeyJfaWQiOiB1c2VyX2lkfSwgeyJfaWQiOiAx"
    "fSkKICAgICAgICAgICAgaXMgbm90IE5vbmUKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBG"
    "YWxzZQoKCmFzeW5jIGRlZiBkZWxldGVfdXNlcih1c2VyX2lkKToKICAgICIiIlJlbW92ZSBhIHVzZXIncyByZWdpc3RyeSBkb2Mg"
    "KyBldmVyeSBxdW90YSByb3cuIFJldHVybnMgKHJlZywgcm93cykuIiIiCiAgICBhd2FpdCBlbnN1cmVfcmVhZHkoKQogICAgZGIg"
    "PSBfZGIoKQogICAgcGFydCA9IF9wYXJ0KCkKICAgIHJlZyA9IDAKICAgIHJvd3MgPSAwCiAgICB0cnk6CiAgICAgICAgciA9IGF3"
    "YWl0IGRiLnd6Zml4X3VzZXJzW3BhcnRdLmRlbGV0ZV9vbmUoeyJfaWQiOiB1c2VyX2lkfSkKICAgICAgICByZWcgPSByLmRlbGV0"
    "ZWRfY291bnQKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgdHJ5OgogICAgICAgIHIgPSBhd2FpdCBkYi53"
    "emZpeF9xdW90YVtwYXJ0XS5kZWxldGVfbWFueSh7Il9pZCI6IHsiJHJlZ2V4IjogZiJee3VzZXJfaWR9OiJ9fSkKICAgICAgICBy"
    "b3dzID0gci5kZWxldGVkX2NvdW50CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiByZWcsIHJv"
    "d3MKCgphc3luYyBkZWYgc2V0X3VzZXJfY2FwKHVzZXJfaWQsIGdiKToKICAgICIiIlNldCBhIHBlci11c2VyIGNhcC4gZ2I9Tm9u"
    "ZSByZW1vdmVzIHRoZSBvdmVycmlkZSAoYmFjayB0byBnbG9iYWwpLiIiIgogICAgYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgIGlm"
    "IGdiIGlzIE5vbmU6CiAgICAgICAgYXdhaXQgX2RiKCkud3pmaXhfdXNlcnNbX3BhcnQoKV0udXBkYXRlX29uZSgKICAgICAgICAg"
    "ICAgeyJfaWQiOiB1c2VyX2lkfSwgeyIkdW5zZXQiOiB7ImNhcF9nYiI6ICIifX0sIHVwc2VydD1UcnVlCiAgICAgICAgKQogICAg"
    "ZWxzZToKICAgICAgICBhd2FpdCBfZGIoKS53emZpeF91c2Vyc1tfcGFydCgpXS51cGRhdGVfb25lKAogICAgICAgICAgICB7Il9p"
    "ZCI6IHVzZXJfaWR9LCB7IiRzZXQiOiB7ImNhcF9nYiI6IGZsb2F0KGdiKX19LCB1cHNlcnQ9VHJ1ZQogICAgICAgICkKCgphc3lu"
    "YyBkZWYgZ2V0X3VzZXJfbXVzaWModXNlcl9pZCk6CiAgICAiIiJQZXItdXNlciBtdXNpYyBwbGF5bGlzdCBsaW1pdCAoc29uZ3Mp"
    "OyBOb25lID0gdXNlIHRoZSBkZWZhdWx0LiIiIgogICAgdHJ5OgogICAgICAgIGRvYyA9IGF3YWl0IGdldF91c2VyX2RvYyh1c2Vy"
    "X2lkKQogICAgICAgIGlmIGRvYy5nZXQoIm11c2ljX21heCIpIGlzIG5vdCBOb25lOgogICAgICAgICAgICByZXR1cm4gbWF4KDEs"
    "IG1pbig1MDAsIGludChkb2NbIm11c2ljX21heCJdKSkpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJl"
    "dHVybiBOb25lCgoKYXN5bmMgZGVmIHNldF91c2VyX211c2ljKHVzZXJfaWQsIG4pOgogICAgIiIiU2V0IHRoZSBwZXItdXNlciBt"
    "dXNpYyBsaW1pdCAoMS01MDApLiBuPU5vbmUgcmVtb3ZlcyB0aGUKICAgIG92ZXJyaWRlIHNvIHRoZSB1c2VyIGZhbGxzIGJhY2sg"
    "dG8gdGhlIGdsb2JhbCBkZWZhdWx0LiIiIgogICAgYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgIGlmIG4gaXMgTm9uZToKICAgICAg"
    "ICBhd2FpdCBfZGIoKS53emZpeF91c2Vyc1tfcGFydCgpXS51cGRhdGVfb25lKAogICAgICAgICAgICB7Il9pZCI6IHVzZXJfaWR9"
    "LCB7IiR1bnNldCI6IHsibXVzaWNfbWF4IjogIiJ9fSwgdXBzZXJ0PVRydWUKICAgICAgICApCiAgICBlbHNlOgogICAgICAgIGF3"
    "YWl0IF9kYigpLnd6Zml4X3VzZXJzW19wYXJ0KCldLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogdXNlcl9pZH0sCiAg"
    "ICAgICAgICAgIHsiJHNldCI6IHsibXVzaWNfbWF4IjogbWF4KDEsIG1pbig1MDAsIGludChuKSkpfX0sCiAgICAgICAgICAgIHVw"
    "c2VydD1UcnVlLAogICAgICAgICkKCgphc3luYyBkZWYgX2dldF9nbG9iYWxfbXVzaWMoKToKICAgICIiIkdsb2JhbCBtdXNpYyBs"
    "aW1pdDogREIgb3ZlcnJpZGUg4oaSIFdaRklYX01VU0lDX01BWCBlbnYg4oaSIDEwLiIiIgogICAgdHJ5OgogICAgICAgIGRvYyA9"
    "IGF3YWl0IF9kYigpLnd6Zml4X2NvbmZpZ1tfcGFydCgpXS5maW5kX29uZSh7Il9pZCI6ICJnbG9iYWwifSkKICAgICAgICBpZiBk"
    "b2MgYW5kIGRvYy5nZXQoIm11c2ljX21heCIpOgogICAgICAgICAgICByZXR1cm4gbWF4KDEsIG1pbig1MDAsIGludChkb2NbIm11"
    "c2ljX21heCJdKSkpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHRyeToKICAgICAgICBpbXBvcnQgb3MK"
    "CiAgICAgICAgcmV0dXJuIG1heCgxLCBtaW4oNTAwLCBpbnQob3MuZW52aXJvbi5nZXQoIldaRklYX01VU0lDX01BWCIsICIxMCIp"
    "KSkpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAxMAoKCmFzeW5jIGRlZiBzZXRfZ2xvYmFsX211c2ljKG4p"
    "OgogICAgYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgIGF3YWl0IF9kYigpLnd6Zml4X2NvbmZpZ1tfcGFydCgpXS51cGRhdGVfb25l"
    "KAogICAgICAgIHsiX2lkIjogImdsb2JhbCJ9LAogICAgICAgIHsiJHNldCI6IHsibXVzaWNfbWF4IjogbWF4KDEsIG1pbig1MDAs"
    "IGludChuKSkpfX0sCiAgICAgICAgdXBzZXJ0PVRydWUsCiAgICApCgoKYXN5bmMgZGVmIF9xdW90YV9ibG9ja19tc2codXNlZCwg"
    "Y2FwLCBzaXplLCBhdF9saW1pdCwgcmVzZXJ2ZWQ9MCk6CiAgICAiIiJUaGUgT05FIHVzZXItZmFjaW5nIGJsb2NrIG1lc3NhZ2Ug"
    "KGRldGFpbHMgZ28gdG8gYWRtaW5fbG9nKS4KICAgIFRoZSB1c2VybmFtZSBpcyBBTExPV0FOQ0VfT1dORVIgKC9icykgd2hlbiBz"
    "ZXQsIGVsc2UgdGhlIGJvdCBvd25lci4iIiIKICAgIF93aG8gPSAiIgogICAgdHJ5OgogICAgICAgIGZyb20gLi4uY29yZS5jb25m"
    "aWdfbWFuYWdlciBpbXBvcnQgQ29uZmlnCgogICAgICAgIF93aG8gPSBzdHIoZ2V0YXR0cihDb25maWcsICJBTExPV0FOQ0VfT1dO"
    "RVIiLCAiIikgb3IgIiIpLnN0cmlwKCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgaWYgX3dobzoKICAg"
    "ICAgICBpZiBub3QgX3doby5zdGFydHN3aXRoKCJAIik6CiAgICAgICAgICAgIF93aG8gPSBmIkB7X3dob30iCiAgICBlbHNlOgog"
    "ICAgICAgIG93bmVyID0gYXdhaXQgZ2V0X293bmVyX3VzZXJuYW1lKCkKICAgICAgICBfd2hvID0gZiJAe293bmVyfSIgaWYgb3du"
    "ZXIgZWxzZSAidGhlIG93bmVyIgogICAgcmV0dXJuICgKICAgICAgICAi8J+aqyA8Yj5Tb3JyeSwgeW91IGRvbid0IGhhdmUgZW5v"
    "dWdoIGJhbmR3aWR0aCBhbGxvd2FuY2UuPC9iPlxuIgogICAgICAgIGYi4pSWIFBsZWFzZSBidXkgbW9yZSBiYW5kd2lkdGggZnJv"
    "bSA8Yj57X3dob308L2I+IgogICAgKQoKCmFzeW5jIGRlZiByZXNlcnZlZF90b2RheSh1c2VyX2lkLCBkYXk9Tm9uZSk6CiAgICAi"
    "IiJUb3RhbCBiYW5kd2lkdGggY3VycmVudGx5IGhlbGQgYnkgdGhpcyB1c2VyJ3MgcnVubmluZyB0YXNrcwogICAgKGF0b21pYyBj"
    "b3VudGVyIG9uIHRvZGF5J3MgcXVvdGEgZG9jdW1lbnQpLiIiIgogICAgZGF5ID0gZGF5IG9yIF9kYXlfaXN0KCkKICAgIHRyeToK"
    "ICAgICAgICBkb2MgPSBhd2FpdCBfZGIoKS53emZpeF9xdW90YVtfcGFydCgpXS5maW5kX29uZSgKICAgICAgICAgICAgeyJfaWQi"
    "OiBmInt1c2VyX2lkfTp7ZGF5fSJ9LCB7InJlc2VydmVkIjogMX0KICAgICAgICApCiAgICAgICAgcmV0dXJuIGludCgoZG9jIG9y"
    "IHt9KS5nZXQoInJlc2VydmVkIikgb3IgMCkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICBMT0dHRVIuZXJyb3Io"
    "ZiJXWkZJWCByZXNlcnZlZF90b2RheSBmYWlsZWQgKHRyZWF0ZWQgYXMgMCk6IHtlfSIpCiAgICAgICAgcmV0dXJuIDAKCgphc3lu"
    "YyBkZWYgX3RyeV9ob2xkKHVzZXJfaWQsIGJ3LCBjYXA9Tm9uZSk6CiAgICAiIiJBdG9taWMgYmFuZHdpZHRoIGhvbGQ6IG9uZSBz"
    "ZXJ2ZXItc2lkZSAkaW5jICsgdmVyaWZ5ICsgcm9sbGJhY2suCiAgICBSZXR1cm5zIChyZWFzb24sIHFpZCkg4oCUIHJlYXNvbiBp"
    "cyBOb25lIHdoZW4gdGhlIGhvbGQgd2FzIGdyYW50ZWQuIiIiCiAgICBpZiBidyA8PSAwOgogICAgICAgIHJldHVybiBOb25lLCBO"
    "b25lCiAgICB0cnk6CiAgICAgICAgaWYgY2FwIGlzIE5vbmU6CiAgICAgICAgICAgIGNhcCA9IGF3YWl0IGdldF9jYXBfYnl0ZXMo"
    "dXNlcl9pZCkKICAgICAgICBxaWQgPSBmInt1c2VyX2lkfTp7X2RheV9pc3QoKX0iCiAgICAgICAgY29sID0gX2RiKCkud3pmaXhf"
    "cXVvdGFbX3BhcnQoKV0KICAgICAgICBiZWZvcmUgPSBhd2FpdCBjb2wuZmluZF9vbmVfYW5kX3VwZGF0ZSgKICAgICAgICAgICAg"
    "eyJfaWQiOiBxaWR9LAogICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAiJGluYyI6IHsicmVzZXJ2ZWQiOiBpbnQoYncpfSwK"
    "ICAgICAgICAgICAgICAgICIkc2V0T25JbnNlcnQiOiB7CiAgICAgICAgICAgICAgICAgICAgInVzZWQiOiAwLAogICAgICAgICAg"
    "ICAgICAgICAgICJleHBpcmVBdCI6IGRhdGV0aW1lLmZyb210aW1lc3RhbXAoCiAgICAgICAgICAgICAgICAgICAgICAgIHRpbWUo"
    "KSArIFdaRklYX1FVT1RBX1RUTF9EQVlTICogODY0MDAsIFVUQwogICAgICAgICAgICAgICAgICAgICksCiAgICAgICAgICAgICAg"
    "ICB9LAogICAgICAgICAgICB9LAogICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICAgICAgICAgcmV0dXJuX2RvY3VtZW50PVJl"
    "dHVybkRvY3VtZW50LkJFRk9SRSwKICAgICAgICApCiAgICAgICAgdXNlZCA9IGludCgoYmVmb3JlIG9yIHt9KS5nZXQoInVzZWQi"
    "KSBvciAwKQogICAgICAgIHJlc19iID0gaW50KChiZWZvcmUgb3Ige30pLmdldCgicmVzZXJ2ZWQiKSBvciAwKQogICAgICAgIGlm"
    "IHVzZWQgKyByZXNfYiA+PSBjYXA6CiAgICAgICAgICAgIHJlYXNvbiA9ICJhdF9saW1pdCIKICAgICAgICBlbGlmIHVzZWQgKyBy"
    "ZXNfYiArIGJ3ID4gY2FwICsgV1pGSVhfR1JBQ0VfTUIgKiBNQjoKICAgICAgICAgICAgcmVhc29uID0gIm92ZXJzaG9vdCIKICAg"
    "ICAgICBlbHNlOgogICAgICAgICAgICByZWFzb24gPSBOb25lCiAgICAgICAgaWYgcmVhc29uOgogICAgICAgICAgICB0cnk6CiAg"
    "ICAgICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgICAgICB7Il9pZCI6IHFpZH0sIHsiJGlu"
    "YyI6IHsicmVzZXJ2ZWQiOiAtaW50KGJ3KX19CiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24g"
    "YXMgZToKICAgICAgICAgICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHJlc2VydmUgcm9sbGJhY2sgZmFpbGVkIChoZWFscyBv"
    "biBib290KToge2V9IikKICAgICAgICAgICAgcmV0dXJuIHJlYXNvbiwgcWlkCiAgICAgICAgcmV0dXJuIE5vbmUsIHFpZAogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIF90cnlfaG9sZCBmYWlsZWQgKGFsbG93"
    "ZWQpOiB7ZX0iKQogICAgICAgIHJldHVybiBOb25lLCBOb25lCgoKYXN5bmMgZGVmIHJlc2VydmUobGlzdGVuZXIsIGJ3LCBjYXA9"
    "Tm9uZSk6CiAgICAiIiJIb2xkIGJhbmR3aWR0aCBmb3IgYSBydW5uaW5nIHRhc2sgKGF0b21pYyDigJQgc2VlIF90cnlfaG9sZCku"
    "IiIiCiAgICBpZiBidyA8PSAwOgogICAgICAgIHJldHVybiBOb25lCiAgICB0cnk6CiAgICAgICAgaWYgZ2V0YXR0cihsaXN0ZW5l"
    "ciwgIl93emZpeF9yZXN2IiwgTm9uZSk6CiAgICAgICAgICAgIGF3YWl0IHJlbGVhc2VfcmVzZXJ2ZShsaXN0ZW5lcikgICMgcmUt"
    "ZXZhbHVhdGlvbiByZXBsYWNlcyB0aGUgaG9sZAogICAgICAgIHJlYXNvbiwgcWlkID0gYXdhaXQgX3RyeV9ob2xkKGxpc3RlbmVy"
    "LnVzZXJfaWQsIGJ3LCBjYXApCiAgICAgICAgaWYgcmVhc29uOgogICAgICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAg"
    "ICAgIGYiV1pGSVggcmVzZXJ2ZTogdXNlcj17bGlzdGVuZXIudXNlcl9pZH0gaG9sZD17Ynd9IHJlamVjdGVkICIKICAgICAgICAg"
    "ICAgICAgIGYiKHtyZWFzb259KSBjYXA9e2NhcH0iCiAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuIHJlYXNvbgogICAg"
    "ICAgIGxpc3RlbmVyLl93emZpeF9yZXN2ID0gKHFpZCwgaW50KGJ3KSkKICAgICAgICByZXR1cm4gTm9uZQogICAgZXhjZXB0IEV4"
    "Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHJlc2VydmUgZmFpbGVkIChhbGxvd2VkKToge2V9IikK"
    "ICAgICAgICByZXR1cm4gTm9uZQoKCmFzeW5jIGRlZiByZXNlcnZlX3ByZShtaWQsIHVzZXJfaWQsIHNpemUpOgogICAgIiIiUHJl"
    "LWRvd25sb2FkIGhvbGQsIHRha2VuIGluIHByZV90YXNrX2NoZWNrIEJFRk9SRSBhbnkgZG93bmxvYWRlcgogICAgc3RhcnRzIChr"
    "ZXllZCBieSB0aGUgY29tbWFuZCBtZXNzYWdlIGlkKS4gVGhlIHRhc2sgYWRvcHRzIGl0IGxhdGVyIGluCiAgICBsaW1pdF9jaGVj"
    "a2VyIHZpYSBfYWRvcHRfcHJlLCBzbyB0aGUgYmFuZHdpZHRoIGlzIGhlbGQgZnJvbSB0aGUgdmVyeQogICAgZmlyc3QgbW9tZW50"
    "IOKAlCBzaW11bHRhbmVvdXMgbGlua3MgY2FuIG5ldmVyIGVhY2ggcGFzcyBhZ2FpbnN0IGEKICAgIHN0YWxlIG51bWJlci4gUmV0"
    "dXJucyAocmVhc29uLCBxaWQpIG9yIHJhaXNlcyBub3RoaW5nLiIiIgogICAgdHJ5OgogICAgICAgIGF3YWl0IGVuc3VyZV9yZWFk"
    "eSgpCiAgICAgICAgYncgPSBXWkZJWF9CV19GQUNUT1IgKiBpbnQoc2l6ZSkKICAgICAgICBpZiBidyA8PSAwOgogICAgICAgICAg"
    "ICByZXR1cm4gTm9uZSwgTm9uZQogICAgICAgICMgZHJvcCBhIHN0YWxlIHByZS1ob2xkIGZvciB0aGlzIG1lc3NhZ2UgZmlyc3Qg"
    "KGVkaXRlZCBjb21tYW5kKQogICAgICAgIHRyeToKICAgICAgICAgICAgYXdhaXQgX2RiKCkud3pmaXhfcHJlaG9sZFtfcGFydCgp"
    "XS5kZWxldGVfb25lKHsiX2lkIjogZiJwcmU6e21pZH0ifSkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBw"
    "YXNzCiAgICAgICAgcmVhc29uLCBxaWQgPSBhd2FpdCBfdHJ5X2hvbGQodXNlcl9pZCwgYncpCiAgICAgICAgaWYgcmVhc29uOgog"
    "ICAgICAgICAgICByZXR1cm4gcmVhc29uLCBxaWQKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IF9kYigpLnd6Zml4X3By"
    "ZWhvbGRbX3BhcnQoKV0uaW5zZXJ0X29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogZiJwcmU6e21pZH0iLCAicWlkIjogcWlk"
    "LCAiYnciOiBpbnQoYncpLAogICAgICAgICAgICAgICAgICJ1aWQiOiB1c2VyX2lkLCAidHMiOiB0aW1lKCl9CiAgICAgICAgICAg"
    "ICkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHByZWhvbGQg"
    "d3JpdGUgZmFpbGVkOiB7ZX0iKQogICAgICAgIHJldHVybiBOb25lLCBxaWQKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAg"
    "ICAgICBMT0dHRVIuZXJyb3IoZiJXWkZJWCByZXNlcnZlX3ByZSBmYWlsZWQgKGFsbG93ZWQpOiB7ZX0iKQogICAgICAgIHJldHVy"
    "biBOb25lLCBOb25lCgoKYXN5bmMgZGVmIF9hZG9wdF9wcmUobGlzdGVuZXIpOgogICAgIiIiQnJpbmcgdGhpcyB0YXNrJ3MgcHJl"
    "LWRvd25sb2FkIGhvbGQgKGlmIGFueSkgb250byB0aGUgbGlzdGVuZXIgc28KICAgIHRoZSBub3JtYWwgcmVsZWFzZSBwYXRocyBm"
    "cmVlIGl0LiIiIgogICAgdHJ5OgogICAgICAgIGRvYyA9IGF3YWl0IF9kYigpLnd6Zml4X3ByZWhvbGRbX3BhcnQoKV0uZmluZF9v"
    "bmVfYW5kX2RlbGV0ZSgKICAgICAgICAgICAgeyJfaWQiOiBmInByZTp7Z2V0YXR0cihsaXN0ZW5lciwgJ21pZCcsIDApfSJ9CiAg"
    "ICAgICAgKQogICAgICAgIGlmIG5vdCBkb2M6CiAgICAgICAgICAgIHJldHVybgogICAgICAgIHFpZCA9IGRvYy5nZXQoInFpZCIp"
    "CiAgICAgICAgYncgPSBpbnQoZG9jLmdldCgiYnciKSBvciAwKQogICAgICAgIGlmIG5vdCBxaWQgb3IgYncgPD0gMDoKICAgICAg"
    "ICAgICAgcmV0dXJuCiAgICAgICAgaWYgZ2V0YXR0cihsaXN0ZW5lciwgIl93emZpeF9yZXN2IiwgTm9uZSk6CiAgICAgICAgICAg"
    "ICMgYWxyZWFkeSBob2xkcyAobGltaXRfY2hlY2tlciByZS1jaGVjayk6IHJlZnVuZCB0aGUgcHJlLWhvbGQKICAgICAgICAgICAg"
    "YXdhaXQgX2RiKCkud3pmaXhfcXVvdGFbX3BhcnQoKV0udXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogcWlkfSwg"
    "eyIkaW5jIjogeyJyZXNlcnZlZCI6IC1id319CiAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuCiAgICAgICAgbGlzdGVu"
    "ZXIuX3d6Zml4X3Jlc3YgPSAocWlkLCBidykKICAgICAgICBMT0dHRVIuaW5mbyhmIldaRklYIHByZWhvbGQgYWRvcHRlZDoge2J3"
    "fSBieXRlcyBmb3Ige3FpZH0iKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKYXN5bmMgZGVmIHJlbGVhc2Vf"
    "cmVzZXJ2ZShsaXN0ZW5lcik6CiAgICAiIiJGcmVlIHRoaXMgdGFzaydzIHJlc2VydmF0aW9uIChjb21wbGV0aW9uLCBlcnJvciBv"
    "ciBjYW5jZWwpLiIiIgogICAgYXdhaXQgX2Fkb3B0X3ByZShsaXN0ZW5lcikKICAgIHJlc3YgPSBnZXRhdHRyKGxpc3RlbmVyLCAi"
    "X3d6Zml4X3Jlc3YiLCBOb25lKQogICAgaWYgbm90IHJlc3Y6CiAgICAgICAgcmV0dXJuCiAgICAjIGNsZWFyIHN5bmNocm9ub3Vz"
    "bHkgRklSU1Qgc28gYSBkdXBsaWNhdGUgcmVsZWFzZSBjYW5ub3QgZG91YmxlLXJlZnVuZAogICAgbGlzdGVuZXIuX3d6Zml4X3Jl"
    "c3YgPSBOb25lCiAgICB0cnk6CiAgICAgICAgcWlkLCBidyA9IHJlc3YKICAgICAgICBhd2FpdCBfZGIoKS53emZpeF9xdW90YVtf"
    "cGFydCgpXS51cGRhdGVfb25lKAogICAgICAgICAgICB7Il9pZCI6IHFpZH0sIHsiJGluYyI6IHsicmVzZXJ2ZWQiOiAtYnd9fQog"
    "ICAgICAgICkKICAgICAgICBMT0dHRVIuaW5mbyhmIldaRklYIHJlc2VydmU6IHJlbGVhc2VkIHtfbmljZV9zaXplKGJ3KX0gZnJv"
    "bSB7cWlkfSIpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKCgpkZWYgX2lzX2V4ZW1wdCh1c2VyX2lkLCB1c2Vy"
    "X2RpY3Q9Tm9uZSk6CiAgICBpZiB1c2VyX2lkID09IENvbmZpZy5PV05FUl9JRDoKICAgICAgICByZXR1cm4gVHJ1ZQogICAgaWYg"
    "dXNlcl9kaWN0IGFuZCB1c2VyX2RpY3QuZ2V0KCJTVURPIik6CiAgICAgICAgcmV0dXJuIFRydWUKICAgICMgUlNTIHRhc2tzIHJ1"
    "biB1bmRlciB0aGUgUlNTIGNoYXQgaWQg4oCUIG5ldmVyIHF1b3RhLWJsb2NrIHRoZSBmZWVkcwogICAgdHJ5OgogICAgICAgIGlm"
    "IENvbmZpZy5SU1NfQ0hBVCBhbmQgdXNlcl9pZCA9PSBpbnQoQ29uZmlnLlJTU19DSEFUKToKICAgICAgICAgICAgcmV0dXJuIFRy"
    "dWUKICAgIGV4Y2VwdCAoVHlwZUVycm9yLCBWYWx1ZUVycm9yKToKICAgICAgICBwYXNzCiAgICByZXR1cm4gRmFsc2UKCgphc3lu"
    "YyBkZWYgcXVvdGFfY2hlY2sobGlzdGVuZXIpOgogICAgIiIiU2l6ZS1hd2FyZSBjaGVjayBjYWxsZWQgZnJvbSBsaW1pdF9jaGVj"
    "a2VyIChzaXplIGlzIGtub3duIHRoZXJlKS4iIiIKICAgIHRyeToKICAgICAgICBpZiBub3QgYXdhaXQgZW5zdXJlX3JlYWR5KCk6"
    "CiAgICAgICAgICAgIExPR0dFUi53YXJuaW5nKCJXWkZJWCBxdW90YTogZGIgbm90IHJlYWR5IC0+IGFsbG93aW5nIHRhc2siKQog"
    "ICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIGlmIF9pc19leGVtcHQobGlzdGVuZXIudXNlcl9pZCwgbGlzdGVuZXIudXNl"
    "cl9kaWN0KToKICAgICAgICAgICAgTE9HR0VSLmluZm8oZiJXWkZJWCBxdW90YTogdXNlciB7bGlzdGVuZXIudXNlcl9pZH0gZXhl"
    "bXB0IChvd25lci9zdWRvL3JzcykiKQogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIHVzZXJfaWQgPSBsaXN0ZW5lci51"
    "c2VyX2lkCiAgICAgICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1c2VyX2lkKQogICAgICAgIHVzZWQgPSBhd2FpdCBnZXRf"
    "dXNhZ2UodXNlcl9pZCkKICAgICAgICAjIGFkb3B0IHRoZSBwcmUtZG93bmxvYWQgaG9sZCB0YWtlbiBpbiBwcmVfdGFza19jaGVj"
    "ayAodjE1LjkpOgogICAgICAgICMgdGhlIGJhbmR3aWR0aCB3YXMgYWxyZWFkeSByZXNlcnZlZCBiZWZvcmUgdGhlIHRhc2sgc3Rh"
    "cnRlZAogICAgICAgIGF3YWl0IF9hZG9wdF9wcmUobGlzdGVuZXIpCiAgICAgICAgaWYgZ2V0YXR0cihsaXN0ZW5lciwgIl93emZp"
    "eF9yZXN2IiwgTm9uZSk6CiAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgZiJXWkZJWCBxdW90YTogdXNl"
    "cj17dXNlcl9pZH0gLT4gYWxsb3cgKHByZS1oZWxkIHtsaXN0ZW5lci5zaXplIG9yIDB9KSIKICAgICAgICAgICAgKQogICAgICAg"
    "ICAgICByZXR1cm4gTm9uZQogICAgICAgICMgcmUtZXZhbHVhdGlvbiAoZS5nLiBxYml0IG1ldGFkYXRhIHVwZGF0ZXMpOiBkcm9w"
    "IG91ciBvd24gb2xkIGhvbGQKICAgICAgICAjIGZpcnN0IHNvIHRoaXMgdGFzayBpcyBub3QgY291bnRlZCBhZ2FpbnN0IGl0c2Vs"
    "ZiB0d2ljZQogICAgICAgIGlmIGdldGF0dHIobGlzdGVuZXIsICJfd3pmaXhfcmVzdiIsIE5vbmUpOgogICAgICAgICAgICBhd2Fp"
    "dCByZWxlYXNlX3Jlc2VydmUobGlzdGVuZXIpCiAgICAgICAgc2l6ZSA9IFdaRklYX0JXX0ZBQ1RPUiAqIGludChsaXN0ZW5lci5z"
    "aXplIG9yIDApCiAgICAgICAgIyBBVE9NSUM6IHRoZSBob2xkIGFuZCB0aGUgdmVyZGljdCBoYXBwZW4gaW4gb25lIHNlcnZlci1z"
    "aWRlCiAgICAgICAgIyBvcGVyYXRpb24g4oCUIHNpbXVsdGFuZW91cyB0YXNrcyBzZXJpYWxpemUsIGV4YWN0bHkgb25lIGNhbiB3"
    "aW4KICAgICAgICByZWFzb24gPSBhd2FpdCByZXNlcnZlKGxpc3RlbmVyLCBzaXplLCBjYXApCiAgICAgICAgaWYgcmVhc29uOgog"
    "ICAgICAgICAgICByZXNlcnZlZCA9IGF3YWl0IHJlc2VydmVkX3RvZGF5KHVzZXJfaWQpCiAgICAgICAgICAgIExPR0dFUi5pbmZv"
    "KAogICAgICAgICAgICAgICAgZiJXWkZJWCBxdW90YTogdXNlcj17dXNlcl9pZH0gc2l6ZT17c2l6ZX0gdXNlZD17dXNlZH0gIgog"
    "ICAgICAgICAgICAgICAgZiJyZXNlcnZlZD17cmVzZXJ2ZWR9IGNhcD17Y2FwfSAtPiBCTE9DSyAoe3JlYXNvbn0pIgogICAgICAg"
    "ICAgICApCiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIF9ubSA9IGxpc3RlbmVyLm5hbWUoKQogICAgICAgICAgICBl"
    "eGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgX25tID0gc3RyKGdldGF0dHIobGlzdGVuZXIsICJuYW1lIiwgIiIpIG9y"
    "ICI/IikKICAgICAgICAgICAgYXdhaXQgYWRtaW5fbG9nKAogICAgICAgICAgICAgICAgIvCfmqsgPGI+RG93bmxvYWQgcmVqZWN0"
    "ZWQ8L2I+IiwKICAgICAgICAgICAgICAgIGYi4pSPIDxiPlVzZXI8L2I+IOKGkiA8Y29kZT57dXNlcl9pZH08L2NvZGU+XG4iCiAg"
    "ICAgICAgICAgICAgICBmIuKUoCA8Yj5GaWxlPC9iPiDihpIge3N0cihfbm0pWzoxMjBdfVxuIgogICAgICAgICAgICAgICAgZiLi"
    "lKAgPGI+U2l6ZTwvYj4g4oaSIHtfbmljZV9zaXplKGludChsaXN0ZW5lci5zaXplIG9yIDApKX0gIgogICAgICAgICAgICAgICAg"
    "ZiIoY29zdCB7X25pY2Vfc2l6ZShzaXplKX0pXG4iCiAgICAgICAgICAgICAgICBmIuKUoCA8Yj5BbGxvd2FuY2U8L2I+IOKGkiB7"
    "X25pY2Vfc2l6ZShtYXgoY2FwIC0gdXNlZCAtIHJlc2VydmVkLCAwKSl9IGxlZnQiCiAgICAgICAgICAgICAgICBmIiAvIHtfbmlj"
    "ZV9zaXplKGNhcCl9XG4iCiAgICAgICAgICAgICAgICBmIuKUliBSZWFzb24g4oaSIHtyZWFzb259ICh1c2VkIHtfbmljZV9zaXpl"
    "KHVzZWQpfSwgIgogICAgICAgICAgICAgICAgZiJydW5uaW5nIHtfbmljZV9zaXplKHJlc2VydmVkKX0pIiwKICAgICAgICAgICAg"
    "KQogICAgICAgICAgICByZXR1cm4gYXdhaXQgX3F1b3RhX2Jsb2NrX21zZygKICAgICAgICAgICAgICAgIHVzZWQsIGNhcCwgc2l6"
    "ZSwgYXRfbGltaXQ9KHJlYXNvbiA9PSAiYXRfbGltaXQiKSwgcmVzZXJ2ZWQ9cmVzZXJ2ZWQKICAgICAgICAgICAgKQogICAgICAg"
    "IExPR0dFUi5pbmZvKAogICAgICAgICAgICBmIldaRklYIHF1b3RhOiB1c2VyPXt1c2VyX2lkfSBzaXplPXtzaXplfSB1c2VkPXt1"
    "c2VkfSAiCiAgICAgICAgICAgIGYiY2FwPXtjYXB9IC0+IGFsbG93IChob2xkaW5nIHtzaXplfSkiCiAgICAgICAgKQogICAgICAg"
    "IHRyeToKICAgICAgICAgICAgX25tMiA9IGxpc3RlbmVyLm5hbWUoKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "ICAgIF9ubTIgPSBzdHIoZ2V0YXR0cihsaXN0ZW5lciwgIm5hbWUiLCAiIikgb3IgIj8iKQogICAgICAgIGF3YWl0IGFkbWluX2xv"
    "ZygKICAgICAgICAgICAgIuKsh++4jyA8Yj5Eb3dubG9hZCBzdGFydGVkPC9iPiIsCiAgICAgICAgICAgIGYi4pSPIDxiPlVzZXI8"
    "L2I+IOKGkiA8Y29kZT57dXNlcl9pZH08L2NvZGU+XG4iCiAgICAgICAgICAgIGYi4pSgIDxiPkZpbGU8L2I+IOKGkiB7c3RyKF9u"
    "bTIpWzoxMjBdfVxuIgogICAgICAgICAgICBmIuKUliA8Yj5CYW5kd2lkdGggaGVsZDwvYj4g4oaSIHtfbmljZV9zaXplKHNpemUp"
    "fSIsCiAgICAgICAgKQogICAgICAgIHJldHVybiBOb25lCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VS"
    "LmVycm9yKGYiV1pGSVggcXVvdGFfY2hlY2sgZmFpbGVkIChhbGxvd2VkKToge2V9IikKICAgICAgICByZXR1cm4gTm9uZQoKCl9V"
    "UkxfUkUgPSBOb25lCgoKZGVmIF9leHRyYWN0X2xpbmtzKHRleHQpOgogICAgIiIiQWxsIGh0dHAocykgbGlua3MgaW4gYSBtZXNz"
    "YWdlIHRleHQgKG5vIG1hZ25ldHMsIC50b3JyZW50IGZpbGVzKS4iIiIKICAgIGdsb2JhbCBfVVJMX1JFCiAgICBpZiBfVVJMX1JF"
    "IGlzIE5vbmU6CiAgICAgICAgZnJvbSByZSBpbXBvcnQgY29tcGlsZSBhcyBfYwogICAgICAgIF9VUkxfUkUgPSBfYyhyImh0dHBz"
    "PzovL1teXHN8XSsiKQogICAgcmV0dXJuIFsKICAgICAgICB1IGZvciB1IGluIF9VUkxfUkUuZmluZGFsbCh0ZXh0IG9yICIiKQog"
    "ICAgICAgIGlmIG5vdCB1Lmxvd2VyKCkuZW5kc3dpdGgoIi50b3JyZW50IikKICAgIF0KCgphc3luYyBkZWYgX3Byb2JlX3NpemVf"
    "YXJpYTIobGluayk6CiAgICAiIiJVbml2ZXJzYWwgc2l6ZSBwcm9iZTogbGV0IGFyaWEyIElUU0VMRiBjb25uZWN0IHRvIHRoZSBs"
    "aW5rLCByZWFkCiAgICB0aGUgcmVhbCB0b3RhbCBsZW5ndGgsIHRoZW4gdGhyb3cgdGhlIHByb2JlIGF3YXkuIFNhbWUgZW5naW5l"
    "LCBzYW1lCiAgICByZWRpcmVjdCBoYW5kbGluZyB0aGUgcmVhbCBkb3dubG9hZCB3b3VsZCB1c2Ug4oCUIHdvcmtzIGZvciBkeW5h"
    "bWljCiAgICBwYWdlcyBhbmQgb2RkIHNlcnZlcnMgd2hlcmUgSEVBRC9yYW5nZWQtR0VUIHNlZSBub3RoaW5nLiIiIgogICAgZ2lk"
    "ID0gTm9uZQogICAgbGFzdCA9IE5vbmUKICAgIHRyeToKICAgICAgICBpbXBvcnQgdGVtcGZpbGUgYXMgX3RmCgogICAgICAgIGZy"
    "b20gLi4uY29yZS50b3JyZW50X21hbmFnZXIgaW1wb3J0IFRvcnJlbnRNYW5hZ2VyCgogICAgICAgIF9kaXIgPSBfdGYubWtkdGVt"
    "cChwcmVmaXg9Ind6Zml4cHJvYmVfIikKICAgICAgICBnaWQgPSBhd2FpdCBUb3JyZW50TWFuYWdlci5hcmlhMi5hZGRVcmkoCiAg"
    "ICAgICAgICAgIHVyaXM9W2xpbmtdLAogICAgICAgICAgICBvcHRpb25zPXsKICAgICAgICAgICAgICAgICJkaXIiOiBfZGlyLAog"
    "ICAgICAgICAgICAgICAgImFsbG93LW92ZXJ3cml0ZSI6ICJ0cnVlIiwKICAgICAgICAgICAgICAgICJhdXRvLWZpbGUtcmVuYW1p"
    "bmciOiAiZmFsc2UiLAogICAgICAgICAgICAgICAgImNvbnRpbnVlIjogImZhbHNlIiwKICAgICAgICAgICAgICAgICJtYXgtZG93"
    "bmxvYWQtbGltaXQiOiAiMUsiLAogICAgICAgICAgICB9LAogICAgICAgICAgICBwb3NpdGlvbj0wLAogICAgICAgICkKICAgICAg"
    "ICBmcm9tIGFzeW5jaW8gaW1wb3J0IHNsZWVwIGFzIF9hc2wKCiAgICAgICAgZm9yIF8gaW4gcmFuZ2UoMjQpOiAgIyB1cCB0byB+"
    "MTJzCiAgICAgICAgICAgIGF3YWl0IF9hc2woMC41KQogICAgICAgICAgICBsYXN0ID0gYXdhaXQgVG9ycmVudE1hbmFnZXIuYXJp"
    "YTIudGVsbFN0YXR1cyhnaWQpCiAgICAgICAgICAgIHRvdGFsID0gaW50KGxhc3QuZ2V0KCJ0b3RhbExlbmd0aCIpIG9yIDApCiAg"
    "ICAgICAgICAgIGlmIHRvdGFsID4gMDoKICAgICAgICAgICAgICAgIHJldHVybiB0b3RhbAogICAgICAgICAgICBpZiBsYXN0Lmdl"
    "dCgic3RhdHVzIikgaW4gKCJlcnJvciIsICJjb21wbGV0ZSIpIG9yIGxhc3QuZ2V0KCJlcnJvckNvZGUiKToKICAgICAgICAgICAg"
    "ICAgIHJldHVybiAwCiAgICAgICAgcmV0dXJuIDAKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIDAKICAgIGZp"
    "bmFsbHk6CiAgICAgICAgaWYgZ2lkOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBmcm9tIC4uLmNvcmUudG9ycmVu"
    "dF9tYW5hZ2VyIGltcG9ydCBUb3JyZW50TWFuYWdlcgoKICAgICAgICAgICAgICAgIGlmIGxhc3QgaXMgbm90IE5vbmU6CiAgICAg"
    "ICAgICAgICAgICAgICAgYXdhaXQgVG9ycmVudE1hbmFnZXIuYXJpYTJfcmVtb3ZlKGxhc3QpCiAgICAgICAgICAgICAgICBlbHNl"
    "OgogICAgICAgICAgICAgICAgICAgIGF3YWl0IFRvcnJlbnRNYW5hZ2VyLmFyaWEyLmZvcmNlUmVtb3ZlKGdpZCkKICAgICAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKCgpfUFJPQkVfU0tJUCA9IE5vbmUKCgpkZWYgX3Byb2Jl"
    "X3NraXAobGluayk6CiAgICAiIiJUcnVlIGZvciBob3N0cyB3aG9zZSBodHRwIHJlc3BvbnNlIGlzIGFuIGVuY3J5cHRlZCBhcHAg"
    "cGFnZSwgbm90CiAgICB0aGUgZmlsZSDigJQgcHJvYmluZyB0aGVtIHlpZWxkcyB0aGUgcGFnZSBzaXplIChhIGZldyBLQiksIHdo"
    "aWNoIHRoZQogICAgcHJlLWNoZWNrIHdvdWxkIHdyb25nbHkgdHJ1c3QgKG1lZ2EubnogYW5zd2VyZWQgNC4xMEtCIGZvciBhIDk0"
    "NE1CCiAgICBmaWxlKS4gVGhlc2UgYWxsIGhhdmUgZGVkaWNhdGVkIGRvd25sb2FkZXJzIHRoYXQga25vdyB0aGUgcmVhbCBzaXpl"
    "LgogICAgIiIiCiAgICBnbG9iYWwgX1BST0JFX1NLSVAKICAgIGlmIF9QUk9CRV9TS0lQIGlzIE5vbmU6CiAgICAgICAgZnJvbSBy"
    "ZSBpbXBvcnQgY29tcGlsZSBhcyBfYwoKICAgICAgICBfUFJPQkVfU0tJUCA9IF9jKAogICAgICAgICAgICByIig/aSkoPzpefC8v"
    "fFwuKSg/Om1lZ2FcLm56fG1lZ2FcLmNvXC5uenxtZWdhXC5pb3wiCiAgICAgICAgICAgIHIiZHJpdmVcLmdvb2dsZVwuY29tfGRv"
    "Y3NcLmdvb2dsZVwuY29tfHlvdXR1YmVcLmNvbXwiCiAgICAgICAgICAgIHIieW91dHVcLmJlfHlvdXR1YmUtbm9jb29raWVcLmNv"
    "bXx0XC5tZXx0ZWxlZ3JhbVwubWUpLyIKICAgICAgICApCiAgICByZXR1cm4gYm9vbChfUFJPQkVfU0tJUC5zZWFyY2gobGluayBv"
    "ciAiIikpCgoKYXN5bmMgZGVmIF9wcm9iZV9zaXplKGxpbmssIGhlYWRlcj1Ob25lKToKICAgICIiIkxlYXJuIGEgZG93bmxvYWQn"
    "cyBzaXplIGJlZm9yZSBpdCBzdGFydHMgKDAgPSB1bmtub3duKS4KCiAgICBhcmNoaXZlLm9yZyBwYWdlIGxpbmtzICgvY29tcHJl"
    "c3MvLCAvZGV0YWlscy8sIC9kb3dubG9hZC8pIG5ldmVyCiAgICBhbnN3ZXIgSEVBRCB3aXRoIHRoZSByZWFsIHNpemUg4oCUIGFz"
    "ayB0aGVpciBtZXRhZGF0YSBBUEkgaW5zdGVhZC4KICAgIEVuY3J5cHRlZC1hcHAgaG9zdHMgKG1lZ2EsIGRyaXZlLCB5b3V0dWJl"
    "KSBhcmUgc2tpcHBlZCBlbnRpcmVseSDigJQKICAgIHRoZWlyIGRlZGljYXRlZCBkb3dubG9hZGVycyBjaGVjayB0aGUgcmVhbCBz"
    "aXplIHRoZW1zZWx2ZXMuCiAgICAiIiIKICAgIHRyeToKICAgICAgICBpZiBfcHJvYmVfc2tpcChsaW5rKToKICAgICAgICAgICAg"
    "cmV0dXJuIDAKCiAgICAgICAgZnJvbSB1cmxsaWIucGFyc2UgaW1wb3J0IHVybHBhcnNlCgogICAgICAgIF9ob3N0ID0gKHVybHBh"
    "cnNlKGxpbmspLmhvc3RuYW1lIG9yICIiKS5sb3dlcigpCiAgICAgICAgaWYgX2hvc3QuZW5kc3dpdGgoImFyY2hpdmUub3JnIik6"
    "CiAgICAgICAgICAgIF9wYXJ0cyA9IFtwIGZvciBwIGluICh1cmxwYXJzZShsaW5rKS5wYXRoIG9yICIiKS5zcGxpdCgiLyIpIGlm"
    "IHBdCiAgICAgICAgICAgIF9pZGVudCwgX2FmdGVyLCBfZmlsZSA9IE5vbmUsIE5vbmUsIE5vbmUKICAgICAgICAgICAgZm9yIF9p"
    "LCBfcCBpbiBlbnVtZXJhdGUoX3BhcnRzKToKICAgICAgICAgICAgICAgIGlmIF9wIGluICgiY29tcHJlc3MiLCAiZGV0YWlscyIs"
    "ICJkb3dubG9hZCIsICJzdHJlYW0iKToKICAgICAgICAgICAgICAgICAgICBpZiBfaSArIDEgPCBsZW4oX3BhcnRzKToKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgX2lkZW50ID0gX3BhcnRzW19pICsgMV0KICAgICAgICAgICAgICAgICAgICAgICAgaWYgX2kgKyAy"
    "IDwgbGVuKF9wYXJ0cyk6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfZmlsZSA9ICIvIi5qb2luKF9wYXJ0c1tfaSArIDI6"
    "XSkKICAgICAgICAgICAgICAgICAgICBicmVhawogICAgICAgICAgICBpZiBfaWRlbnQ6CiAgICAgICAgICAgICAgICBmcm9tIGFp"
    "b2h0dHAgaW1wb3J0IENsaWVudFNlc3Npb24sIENsaWVudFRpbWVvdXQKCiAgICAgICAgICAgICAgICBhc3luYyB3aXRoIENsaWVu"
    "dFNlc3Npb24oCiAgICAgICAgICAgICAgICAgICAgdGltZW91dD1DbGllbnRUaW1lb3V0KHRvdGFsPTgpLAogICAgICAgICAgICAg"
    "ICAgICAgIGhlYWRlcnM9eyJVc2VyLUFnZW50IjogIldaTUwtWC8xNS4xMSJ9LAogICAgICAgICAgICAgICAgKSBhcyBfczoKICAg"
    "ICAgICAgICAgICAgICAgICBhc3luYyB3aXRoIF9zLmdldCgKICAgICAgICAgICAgICAgICAgICAgICAgZiJodHRwczovL2FyY2hp"
    "dmUub3JnL21ldGFkYXRhL3tfaWRlbnR9IgogICAgICAgICAgICAgICAgICAgICkgYXMgX3I6CiAgICAgICAgICAgICAgICAgICAg"
    "ICAgIGlmIF9yLnN0YXR1cyA8IDQwMDoKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9qcyA9IGF3YWl0IF9yLmpzb24oY29u"
    "dGVudF90eXBlPU5vbmUpCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfZmlsZXMgPSAoX2pzIG9yIHt9KS5nZXQoImZpbGVz"
    "Iikgb3IgW10KICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIF9maWxlOgogICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgIGZvciBfZiBpbiBfZmlsZXM6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIF9mLmdldCgibmFtZSIp"
    "ID09IF9maWxlOgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgIF9zeiA9IGludChfZi5nZXQoInNpemUiKSBvciAwKQogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgZXhjZXB0IChUeXBlRXJyb3IsIFZhbHVlRXJyb3IpOgogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgIF9zeiA9IDAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIF9z"
    "eiA+IDA6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIF9zegogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgX3RvdGFsID0gMAogICAgICAgICAgICAgICAgICAgICAgICAgICAgZm9yIF9mIGluIF9maWxlczoKICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIF90b3Rh"
    "bCArPSBpbnQoX2YuZ2V0KCJzaXplIikgb3IgMCkKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBleGNlcHQgKFR5cGVF"
    "cnJvciwgVmFsdWVFcnJvcik6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgIGlmIF90b3RhbCA+IDA6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIF90b3RhbAog"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICB0cnk6CiAgICAgICAgZnJvbSBhaW9odHRwIGltcG9ydCBDbGll"
    "bnRTZXNzaW9uLCBDbGllbnRUaW1lb3V0CgogICAgICAgIGFzeW5jIHdpdGggQ2xpZW50U2Vzc2lvbigKICAgICAgICAgICAgdGlt"
    "ZW91dD1DbGllbnRUaW1lb3V0KHRvdGFsPTgpLAogICAgICAgICAgICBoZWFkZXJzPXsiVXNlci1BZ2VudCI6ICJXWk1MLVgvMTUu"
    "OSJ9LAogICAgICAgICkgYXMgczoKICAgICAgICAgICAgYXN5bmMgd2l0aCBzLmhlYWQobGluaywgYWxsb3dfcmVkaXJlY3RzPVRy"
    "dWUpIGFzIHI6CiAgICAgICAgICAgICAgICBpZiByLnN0YXR1cyA8IDQwMDoKICAgICAgICAgICAgICAgICAgICBfY3QgPSAoci5o"
    "ZWFkZXJzLmdldCgiQ29udGVudC1UeXBlIikgb3IgIiIpLmxvd2VyKCkKICAgICAgICAgICAgICAgICAgICBpZiAidGV4dC9odG1s"
    "IiBpbiBfY3Qgb3IgInhodG1sIiBpbiBfY3Q6CiAgICAgICAgICAgICAgICAgICAgICAgIHBhc3MgICMgYSB3ZWIgUEFHRSwgbm90"
    "IHRoZSBmaWxlIOKAlCBuZXZlciB0cnVzdCBpdAogICAgICAgICAgICAgICAgICAgIGVsc2U6CiAgICAgICAgICAgICAgICAgICAg"
    "ICAgIGNsID0gci5oZWFkZXJzLmdldCgiQ29udGVudC1MZW5ndGgiKQogICAgICAgICAgICAgICAgICAgICAgICBpZiBjbCBhbmQg"
    "Y2wuaXNkaWdpdCgpOgogICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGludChjbCkKICAgICAgICAgICAgIyBzb21l"
    "IHNlcnZlcnMgcmVqZWN0IEhFQUQg4oCUIGEgMS1ieXRlIHJhbmdlZCBHRVQgc3RpbGwgcmVwb3J0cwogICAgICAgICAgICAjIHRo"
    "ZSB0b3RhbCBzaXplIGluIENvbnRlbnQtUmFuZ2UKICAgICAgICAgICAgYXN5bmMgd2l0aCBzLmdldCgKICAgICAgICAgICAgICAg"
    "IGxpbmssIGFsbG93X3JlZGlyZWN0cz1UcnVlLCBoZWFkZXJzPXsiUmFuZ2UiOiAiYnl0ZXM9MC0wIn0KICAgICAgICAgICAgKSBh"
    "cyByOgogICAgICAgICAgICAgICAgX2N0MiA9IChyLmhlYWRlcnMuZ2V0KCJDb250ZW50LVR5cGUiKSBvciAiIikubG93ZXIoKQog"
    "ICAgICAgICAgICAgICAgaWYgInRleHQvaHRtbCIgaW4gX2N0MiBvciAieGh0bWwiIGluIF9jdDI6CiAgICAgICAgICAgICAgICAg"
    "ICAgcGFzcyAgIyBwYWdlLCBub3QgZmlsZSDigJQgcHJvYmUgZGVlcGVyCiAgICAgICAgICAgICAgICBlbHNlOgogICAgICAgICAg"
    "ICAgICAgICAgIGNyID0gci5oZWFkZXJzLmdldCgiQ29udGVudC1SYW5nZSIsICIiKQogICAgICAgICAgICAgICAgICAgIGlmIGNy"
    "IGFuZCAiLyIgaW4gY3I6CiAgICAgICAgICAgICAgICAgICAgICAgIHRvdGFsID0gY3IucnNwbGl0KCIvIiwgMSlbMV0KICAgICAg"
    "ICAgICAgICAgICAgICAgICAgaWYgdG90YWwuaXNkaWdpdCgpOgogICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGlu"
    "dCh0b3RhbCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgIyBsYXN0IHJlc29ydDogYXNrIGFyaWEyIGl0"
    "c2VsZiAodW5pdmVyc2FsIGNoZWNrZXIpCiAgICByZXR1cm4gYXdhaXQgX3Byb2JlX3NpemVfYXJpYTIobGluaykKCgphc3luYyBk"
    "ZWYgcHJlY2hlY2sobWVzc2FnZSk6CiAgICAiIiJQcmUtZG93bmxvYWQgYWxsb3dhbmNlIGNoZWNrICh2MTUuOSksIGNhbGxlZCBm"
    "cm9tIHByZV90YXNrX2NoZWNrCiAgICBCRUZPUkUgYW55IGRvd25sb2FkZXIgc3RhcnRzLgoKICAgIEZvciBwbGFpbiBzaW5nbGUg"
    "aHR0cChzKSBsaW5rcyB0aGUgZmlsZSBzaXplIGlzIHByb2JlZCB3aXRoIGEgSEVBRAogICAgcmVxdWVzdDsgaWYgdGhlIGFsbG93"
    "YW5jZSBjYW5ub3QgY292ZXIgaXQgdGhlIHRhc2sgaXMgc3RvcHBlZCByaWdodAogICAgaGVyZSB3aXRoIGEgY2xlYXIgbWVzc2Fn"
    "ZSwgYW5kIHRoZSBiYW5kd2lkdGggaXMgaGVsZCBhdG9taWNhbGx5IHRoZQogICAgbW9tZW50IGl0IHBhc3Nlcywgc28gc2ltdWx0"
    "YW5lb3VzIGNvbW1hbmRzIGNhbiBuZXZlciBlYWNoIHNsaXAgcGFzdAogICAgdGhlIHNhbWUgc3RhbGUgbnVtYmVyLiBSZXR1cm5z"
    "IGEgYmxvY2sgbWVzc2FnZSwgb3IgTm9uZSB0byBwcm9jZWVkLgogICAgIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbV91c2VyID0g"
    "Z2V0YXR0cihtZXNzYWdlLCAiZnJvbV91c2VyIiwgTm9uZSkKICAgICAgICBzZW5kZXJfY2hhdCA9IGdldGF0dHIobWVzc2FnZSwg"
    "InNlbmRlcl9jaGF0IiwgTm9uZSkKICAgICAgICB3aG8gPSBmcm9tX3VzZXIgb3Igc2VuZGVyX2NoYXQKICAgICAgICB1c2VyX2lk"
    "ID0gd2hvLmlkIGlmIHdobyBlbHNlIDAKICAgICAgICBfY2hhdCA9IGdldGF0dHIobWVzc2FnZSwgImNoYXQiLCBOb25lKQogICAg"
    "ICAgIF9jdHlwZSA9IGdldGF0dHIoX2NoYXQsICJ0eXBlIiwgTm9uZSkKICAgICAgICBfY25hbWUgPSBzdHIoZ2V0YXR0cihfY2hh"
    "dCwgInRpdGxlIiwgTm9uZSkgb3IgZ2V0YXR0cihfY2hhdCwgInVzZXJuYW1lIiwgTm9uZSkgb3IgIiIpCiAgICAgICAgdHJ5Ogog"
    "ICAgICAgICAgICBfY3R5cGUgPSBzdHIoX2N0eXBlKS5zcGxpdCgiLiIpWy0xXQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAg"
    "ICAgICAgICAgIF9jdHlwZSA9IHN0cihfY3R5cGUpCiAgICAgICAgdXNlcl9kaWN0ID0gX2dldF91c2VyX2RpY3QodXNlcl9pZCkK"
    "ICAgICAgICBpZiBfaXNfZXhlbXB0KHVzZXJfaWQsIHVzZXJfZGljdCk6CiAgICAgICAgICAgIGF3YWl0IGFkbWluX2xvZygKICAg"
    "ICAgICAgICAgICAgICLwn5GRIDxiPkV4ZW1wdCB0YXNrIOKAlCBubyBxdW90YSBjaGVjazwvYj4iLAogICAgICAgICAgICAgICAg"
    "ZiLilI8gPGI+VXNlcjwvYj4g4oaSIDxjb2RlPnt1c2VyX2lkfTwvY29kZT4iCiAgICAgICAgICAgICAgICBmIntmJyAoQHt3aG8u"
    "dXNlcm5hbWV9KScgaWYgZ2V0YXR0cih3aG8sICd1c2VybmFtZScsIE5vbmUpIGVsc2UgJyd9XG4iCiAgICAgICAgICAgICAgICBm"
    "IuKUoCA8Yj5XaGVyZTwvYj4g4oaSIHtfY3R5cGV9IHtfY25hbWVbOjYwXX1cbiIKICAgICAgICAgICAgICAgIGYi4pSWIE93bmVy"
    "L3N1ZG8gYXJlIGV4ZW1wdCBieSBkZXNpZ24iLAogICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAg"
    "aWYgbm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIHRleHQgPSBnZXRhdHRy"
    "KG1lc3NhZ2UsICJ0ZXh0IiwgIiIpIG9yICIiCiAgICAgICAgaWYgX2NhcCA6PSBnZXRhdHRyKG1lc3NhZ2UsICJjYXB0aW9uIiwg"
    "Tm9uZSk6CiAgICAgICAgICAgIHRleHQgKz0gIlxuIiArIF9jYXAKICAgICAgICAjIGxpbmtzIGNhbiBhbHNvIGFycml2ZSB2aWEg"
    "YSByZXBsaWVkLXRvIG1lc3NhZ2UgKC9sIGFzIHJlcGx5KToKICAgICAgICAjIHNjYW4gaXQgdG9vLCBvdGhlcndpc2UgdGhlIHBy"
    "ZS1jaGVjayBjYW5ub3Qgc2VlIHRoZSBzaXplIGF0IGFsbAogICAgICAgIF9yZXAgPSBnZXRhdHRyKG1lc3NhZ2UsICJyZXBseV90"
    "b19tZXNzYWdlIiwgTm9uZSkKICAgICAgICBpZiBfcmVwIGlzIG5vdCBOb25lOgogICAgICAgICAgICBfcnQgPSAoZ2V0YXR0cihf"
    "cmVwLCAidGV4dCIsIE5vbmUpIG9yIGdldGF0dHIoX3JlcCwgImNhcHRpb24iLCBOb25lKSBvciAiIikKICAgICAgICAgICAgaWYg"
    "X3J0OgogICAgICAgICAgICAgICAgdGV4dCArPSAiXG4iICsgX3J0CiAgICAgICAgbGlua3MgPSBfZXh0cmFjdF9saW5rcyh0ZXh0"
    "KQogICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICBmIldaRklYIHByZWNoZWNrOiBjaGF0PXtfY3R5cGV9IHVzZXI9e3Vz"
    "ZXJfaWR9ICIKICAgICAgICAgICAgZiJsaW5rcz17bGVuKGxpbmtzKX0iCiAgICAgICAgKQogICAgICAgICMgb25seSBzaW5nbGUg"
    "cGxhaW4gbGlua3MgY2FuIGJlIHByZS1jaGVja2VkIHJlbGlhYmx5OyBidWxrIGFuZAogICAgICAgICMgbm9uLWh0dHAgc291cmNl"
    "cyBmYWxsIGJhY2sgdG8gdGhlIHNpemUgY2hlY2sgaW5zaWRlIHRoZSBkb3dubG9hZAogICAgICAgIGlmIGxlbihsaW5rcykgIT0g"
    "MToKICAgICAgICAgICAgYXdhaXQgYWRtaW5fbG9nKAogICAgICAgICAgICAgICAgIuKaoO+4jyA8Yj5UYXNrIHdpdGhvdXQgcHJl"
    "LWNoZWNrYWJsZSBsaW5rPC9iPiIsCiAgICAgICAgICAgICAgICBmIuKUjyA8Yj5Vc2VyPC9iPiDihpIgPGNvZGU+e3VzZXJfaWR9"
    "PC9jb2RlPlxuIgogICAgICAgICAgICAgICAgZiLilKAgPGI+V2hlcmU8L2I+IOKGkiB7X2N0eXBlfSB7X2NuYW1lWzo2MF19XG4i"
    "CiAgICAgICAgICAgICAgICBmIuKUoCA8Yj5MaW5rcyBzZWVuPC9iPiDihpIge2xlbihsaW5rcyl9XG4iCiAgICAgICAgICAgICAg"
    "ICBmIuKUliBGYWxscyBiYWNrIHRvIHRoZSBpbi1kb3dubG9hZCBjaGVjayDigJQgdGhlIGRvd25sb2FkICIKICAgICAgICAgICAg"
    "ICAgIGYiaXMgc3RpbGwgaGVsZCBhbmQgdmVyaWZpZWQiLAogICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVybiBOb25lCgog"
    "ICAgICAgIGZyb20gLi4uaGVscGVyLnRlbGVncmFtX2hlbHBlci5tZXNzYWdlX3V0aWxzIGltcG9ydCAoCiAgICAgICAgICAgIHNl"
    "bmRfbWVzc2FnZSBhcyBfc2VuZCwKICAgICAgICAgICAgZWRpdF9tZXNzYWdlIGFzIF9lZGl0LAogICAgICAgICkKCiAgICAgICAg"
    "bWlkID0gbWVzc2FnZS5pZAogICAgICAgICMgdjE1LjI3OiB0aGUgY2hlY2tpbmcgbWVzc2FnZSBpcyBCQUNLIGFuZCBhbHdheXMg"
    "c2hvd24g4oCUIGl0IGtlZXBzCiAgICAgICAgIyB1c2VycyBwYXRpZW50IHdoaWxlIHRoZSBwcm9iZSBydW5zLiBJdCBpcyBzZW50"
    "IElNTUVESUFURUxZIGFuZAogICAgICAgICMgdGhlbiBFRElURUQgaW50byB0aGUgZmluYWwgcmVzdWx0IChhbGxvdyAvIGJsb2Nr"
    "KSDigJQgb25lIHNtb290aAogICAgICAgICMgbWVzc2FnZSwgbm8gZmxhc2gsIG5vIGRlbGV0ZS1nYXAtbmV3LW1lc3NhZ2UgamFu"
    "ay4KICAgICAgICBjaGVja2luZyA9IE5vbmUKICAgICAgICB0cnk6CiAgICAgICAgICAgIGNoZWNraW5nID0gYXdhaXQgX3NlbmQo"
    "CiAgICAgICAgICAgICAgICBtZXNzYWdlLCAi8J+UjSA8Yj5DaGVja2luZyBiYW5kd2lkdGggYWxsb3dhbmNl4oCmPC9iPiIKICAg"
    "ICAgICAgICAgKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIGNoZWNraW5nID0gTm9uZQoKICAgICAgICBz"
    "aXplID0gYXdhaXQgX3Byb2JlX3NpemUobGlua3NbMF0pCiAgICAgICAgaWYgc2l6ZSA8PSAwOgogICAgICAgICAgICAjIHYxNS4y"
    "ODogcHJvYmUtc2tpcHBlZCBzb3VyY2VzIChZb3VUdWJlLCBtZWdhLCBnZHJpdmUsIHQubWUpCiAgICAgICAgICAgICMga2VlcCBm"
    "ZWVkYmFjayBvbiBzY3JlZW4g4oCUIHRoZSBleHRyYWN0b3IgY2FuIHRha2UgMTBzKyB0bwogICAgICAgICAgICAjIGZldGNoIG1l"
    "dGFkYXRhIGFuZCB0aGUgdXNlciBzaG91bGQgc2VlIHNvbWV0aGluZyBtZWFud2hpbGUuCiAgICAgICAgICAgICMgVGhlIGluLWRv"
    "d25sb2FkIGNoZWNrIHN0aWxsIGVuZm9yY2VzIHRoZSBxdW90YS4KICAgICAgICAgICAgaWYgY2hlY2tpbmcgaXMgbm90IE5vbmU6"
    "CiAgICAgICAgICAgICAgICAjIHYxNS4zMDogc3Rhc2ggaXQgc28gYSBsYXRlciBvdmVyLXF1b3RhIHJlZnVzYWwgY2FuCiAgICAg"
    "ICAgICAgICAgICAjIG1vcnBoIFRISVMgbWVzc2FnZSBpbnN0ZWFkIG9mIHNlbmRpbmcgYSBuZXcgb25lCiAgICAgICAgICAgICAg"
    "ICB0cnk6CiAgICAgICAgICAgICAgICAgICAgbWVzc2FnZS5fd3pmaXhfY2hlY2tpbmcgPSBjaGVja2luZwogICAgICAgICAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgICAgICBwYXNzCiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAg"
    "ICAgICAgICAgICAgYXdhaXQgX2VkaXQoCiAgICAgICAgICAgICAgICAgICAgICAgIGNoZWNraW5nLAogICAgICAgICAgICAgICAg"
    "ICAgICAgICAi4pyFIDxiPkNoZWNrZWQuPC9iPiIsCiAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgIF9k"
    "ZWxfYWZ0ZXIoY2hlY2tpbmcpCiAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAgIHBh"
    "c3MKICAgICAgICAgICAgcmV0dXJuIE5vbmUgICMgc2l6ZSB1bmtub3duIC0+IGNoZWNrZWQgZHVyaW5nIHRoZSBkb3dubG9hZAoK"
    "ICAgICAgICBjYXAgPSBhd2FpdCBnZXRfY2FwX2J5dGVzKHVzZXJfaWQpCiAgICAgICAgdXNlZCA9IGF3YWl0IGdldF91c2FnZSh1"
    "c2VyX2lkKQogICAgICAgIHJlc2VydmVkID0gYXdhaXQgcmVzZXJ2ZWRfdG9kYXkodXNlcl9pZCkKICAgICAgICBidyA9IFdaRklY"
    "X0JXX0ZBQ1RPUiAqIHNpemUKICAgICAgICByZWFzb24sIF8gPSBhd2FpdCByZXNlcnZlX3ByZShtaWQsIHVzZXJfaWQsIHNpemUp"
    "CgogICAgICAgIGlmIHJlYXNvbiBpcyBOb25lOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBtZXNzYWdlLl93emZp"
    "eF9oZWxkID0gVHJ1ZQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAg"
    "ICB0cnk6CiAgICAgICAgICAgICAgICBfb2tfdGV4dCA9ICgKICAgICAgICAgICAgICAgICAgICBmIuKchSA8Yj5DaGVja2VkIOKA"
    "lCBhbGxvd2FuY2UgT0suPC9iPiAiCiAgICAgICAgICAgICAgICAgICAgZiJEb3dubG9hZCB3aWxsIHN0YXJ0IHNob3J0bHkgIgog"
    "ICAgICAgICAgICAgICAgICAgIGYiKH57X25pY2Vfc2l6ZShidyl9IGluY2wuIHVwbG9hZCkiCiAgICAgICAgICAgICAgICApCiAg"
    "ICAgICAgICAgICAgICBpZiBjaGVja2luZyBpcyBub3QgTm9uZToKICAgICAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAg"
    "ICAgICAgICAgICAgIGF3YWl0IF9lZGl0KGNoZWNraW5nLCBfb2tfdGV4dCkKICAgICAgICAgICAgICAgICAgICAgICAgX2RlbF9h"
    "ZnRlcihjaGVja2luZykKICAgICAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAgICAg"
    "ICBwYXNzCiAgICAgICAgICAgICAgICBlbHNlOgogICAgICAgICAgICAgICAgICAgICMgZmFzdCBjaGVjazogbm8gY2hlY2tpbmcg"
    "bWVzc2FnZSBldmVyIHNob3dlZCDigJQgc2VuZAogICAgICAgICAgICAgICAgICAgICMgdGhlIGNvbmZpcm1hdGlvbiBkaXJlY3Rs"
    "eSAoc3RhYmxlLCBuZXZlciBmbGFzaGVzKQogICAgICAgICAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgICAgICAgICAg"
    "YXdhaXQgX3NlbmQobWVzc2FnZSwgX29rX3RleHQpCiAgICAgICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgcGFzcwog"
    "ICAgICAgICAgICByZXR1cm4gTm9uZQoKICAgICAgICBmcm9tIGh0bWwgaW1wb3J0IGVzY2FwZSBhcyBfX2VzYwoKICAgICAgICBs"
    "ZWZ0ID0gbWF4KGNhcCAtIHVzZWQgLSByZXNlcnZlZCwgMCkKICAgICAgICBhd2FpdCBhZG1pbl9sb2coCiAgICAgICAgICAgICLw"
    "n5qrIDxiPkRvd25sb2FkIHJlamVjdGVkIChwcmUtY2hlY2spPC9iPiIsCiAgICAgICAgICAgIGYi4pSPIDxiPlVzZXI8L2I+IOKG"
    "kiA8Y29kZT57dXNlcl9pZH08L2NvZGU+XG4iCiAgICAgICAgICAgIGYi4pSgIDxiPkZpbGU8L2I+IOKGkiB7bGlua3NbMF1bOjEy"
    "MF19XG4iCiAgICAgICAgICAgIGYi4pSgIDxiPlNpemU8L2I+IOKGkiB7X25pY2Vfc2l6ZShzaXplKX0gKGNvc3Qge19uaWNlX3Np"
    "emUoYncpfSlcbiIKICAgICAgICAgICAgZiLilKAgPGI+QWxsb3dhbmNlPC9iPiDihpIge19uaWNlX3NpemUobGVmdCl9IGxlZnQg"
    "LyB7X25pY2Vfc2l6ZShjYXApfVxuIgogICAgICAgICAgICBmIuKUoCA8Yj5Vc2VkIHRvZGF5PC9iPiDihpIge19uaWNlX3NpemUo"
    "dXNlZCl9ICgre19uaWNlX3NpemUocmVzZXJ2ZWQpfSBydW5uaW5nKVxuIgogICAgICAgICAgICBmIuKUliA8Yj5NZXNzYWdlPC9i"
    "PiDihpIge19fZXNjKF9tc2dfdGV4dChtZXNzYWdlKSlbOjEwMDBdIG9yICcobm8gdGV4dCknfSIsCiAgICAgICAgKQogICAgICAg"
    "IF9ibGsgPSBhd2FpdCBfcXVvdGFfYmxvY2tfbXNnKAogICAgICAgICAgICB1c2VkLCBjYXAsIHNpemUsIGF0X2xpbWl0PUZhbHNl"
    "LCByZXNlcnZlZD1yZXNlcnZlZAogICAgICAgICkKICAgICAgICAjIHYxNS4yNzogdGhlIGNoZWNraW5nIG1lc3NhZ2UgQkVDT01F"
    "UyB0aGUgYmxvY2sgbWVzc2FnZSDigJQgdGhlCiAgICAgICAgIyB1c2VyIHdhdGNoZWQgIvCflI0gQ2hlY2tpbmfigKYiIGFuZCBu"
    "b3cgc2VlcyBpdCB0dXJuIGludG8gdGhlIPCfmqsKICAgICAgICAjIG1lc3NhZ2UgaW4gcGxhY2UuIE5vIGZsYXNoLCBubyBnYXAu"
    "ICJfX1daRklYX0hBTkRMRURfXyIgdGVsbHMKICAgICAgICAjIHRoZSB0YXNrIHBpcGVsaW5lIHRoZSByZXBseSB3YXMgYWxyZWFk"
    "eSBzZW50LCBzbyBpdCBtdXN0CiAgICAgICAgIyBhYm9ydCB0aGUgdGFzayBzaWxlbnRseSBpbnN0ZWFkIG9mIHNlbmRpbmcgYW5v"
    "dGhlciBtZXNzYWdlLgogICAgICAgIHRyeToKICAgICAgICAgICAgaWYgY2hlY2tpbmcgaXMgbm90IE5vbmU6CiAgICAgICAgICAg"
    "ICAgICBhd2FpdCBfZWRpdChjaGVja2luZywgX2JsaykKICAgICAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgICAgICBm"
    "cm9tIC4uLmhlbHBlci50ZWxlZ3JhbV9oZWxwZXIubWVzc2FnZV91dGlscyBpbXBvcnQgKAogICAgICAgICAgICAgICAgICAgICAg"
    "ICBkZWxldGVfbWVzc2FnZSBhcyBfZGVsLAogICAgICAgICAgICAgICAgICAgICkKCiAgICAgICAgICAgICAgICAgICAgYXdhaXQg"
    "X2RlbChtZXNzYWdlKQogICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgICAgICBwYXNzCiAg"
    "ICAgICAgICAgICAgICByZXR1cm4gIl9fV1pGSVhfSEFORExFRF9fIgogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "ICAgIHBhc3MKICAgICAgICByZXR1cm4gX2JsawogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJv"
    "cihmIldaRklYIHByZWNoZWNrIGZhaWxlZCAoYWxsb3dlZCk6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUKCgpkZWYgX2dldF91"
    "c2VyX2RpY3QodXNlcl9pZCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi4gaW1wb3J0IHVzZXJfZGF0YQoKICAgICAgICByZXR1"
    "cm4gdXNlcl9kYXRhLmdldCh1c2VyX2lkLCB7fSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIHt9CgoKYXN5"
    "bmMgZGVmIF9kZWxfbXNnX2xhdGVyKG0pOgogICAgZnJvbSBhc3luY2lvIGltcG9ydCBzbGVlcCBhcyBfc2wKCiAgICBhd2FpdCBf"
    "c2woNikKICAgIHRyeToKICAgICAgICBpZiBnZXRhdHRyKG0sICJfd3pmaXhfa2VlcCIsIEZhbHNlKToKICAgICAgICAgICAgcmV0"
    "dXJuCiAgICAgICAgZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1faGVscGVyLm1lc3NhZ2VfdXRpbHMgaW1wb3J0IGRlbGV0ZV9tZXNz"
    "YWdlIGFzIF9kZWwKCiAgICAgICAgYXdhaXQgX2RlbChtKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKZGVm"
    "IF9kZWxfYWZ0ZXIobSk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi4gaW1wb3J0IGJvdF9sb29wCgogICAgICAgIGJvdF9sb29w"
    "LmNyZWF0ZV90YXNrKF9kZWxfbXNnX2xhdGVyKG0pKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKZGVmIF9t"
    "c2dfdGV4dChtZXNzYWdlKToKICAgICIiIkZ1bGwgdXNlciBtZXNzYWdlIGZvciBsb2dnaW5nOiBjb21tYW5kIHRleHQgcGx1cyBh"
    "bnkgcmVwbHkKICAgIGNvbnRleHQgKHRoZSBmaWxlL25hbWUgYmVpbmcgcmVwbGllZCB0byksIG5vdGhpbmcgdHJ1bmNhdGVkIGF3"
    "YXkuIiIiCiAgICB0cnk6CiAgICAgICAgX3QgPSAobWVzc2FnZS50ZXh0IG9yIG1lc3NhZ2UuY2FwdGlvbiBvciAiIikuc3RyaXAo"
    "KQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBfdCA9ICIiCiAgICB0cnk6CiAgICAgICAgX3J0ID0gZ2V0YXR0cihtZXNz"
    "YWdlLCAicmVwbHlfdG9fbWVzc2FnZSIsIE5vbmUpCiAgICAgICAgaWYgX3J0IGlzIG5vdCBOb25lOgogICAgICAgICAgICBfcnRf"
    "dHh0ID0gKF9ydC50ZXh0IG9yIF9ydC5jYXB0aW9uIG9yICIiKS5zdHJpcCgpCiAgICAgICAgICAgIGlmIF9ydF90eHQ6CiAgICAg"
    "ICAgICAgICAgICBfdCA9IChfdCArIGYiXG7ihqkgcmVwbGllcyB0bzoge19ydF90eHRbOjQwMF19Iikuc3RyaXAoKQogICAgZXhj"
    "ZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gX3QKCgphc3luYyBkZWYgb3Zlcl9saW1pdF9tc2codXNlcl9p"
    "ZCwgdXNlcl9kaWN0PU5vbmUpOgogICAgIiIiQmx1bnQgcHJlLXRhc2sgY2hlY2sgY2FsbGVkIGZyb20gcHJlX3Rhc2tfY2hlY2sg"
    "KG5vIHNpemUga25vd24geWV0KS4iIiIKICAgIHRyeToKICAgICAgICBpZiBfaXNfZXhlbXB0KHVzZXJfaWQsIHVzZXJfZGljdCk6"
    "CiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAgaWYgbm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgICAgICBM"
    "T0dHRVIud2FybmluZygiV1pGSVggcHJlLWNoZWNrOiBkYiBub3QgcmVhZHkgLT4gYWxsb3dpbmcgdGFzayIpCiAgICAgICAgICAg"
    "IHJldHVybiBOb25lCiAgICAgICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1c2VyX2lkKQogICAgICAgIHVzZWQgPSBhd2Fp"
    "dCBnZXRfdXNhZ2UodXNlcl9pZCkKICAgICAgICByZXNlcnZlZCA9IGF3YWl0IHJlc2VydmVkX3RvZGF5KHVzZXJfaWQpCiAgICAg"
    "ICAgaWYgdXNlZCArIHJlc2VydmVkID49IGNhcDoKICAgICAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAgICAgICAgICBmIlda"
    "RklYIHByZS1jaGVjazogdXNlcj17dXNlcl9pZH0gdXNlZD17dXNlZH0gIgogICAgICAgICAgICAgICAgZiJyZXNlcnZlZD17cmVz"
    "ZXJ2ZWR9IGNhcD17Y2FwfSAtPiBCTE9DSyIKICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4gYXdhaXQgX3F1b3RhX2Js"
    "b2NrX21zZyh1c2VkLCBjYXAsIDAsIGF0X2xpbWl0PVRydWUsIHJlc2VydmVkPXJlc2VydmVkKQogICAgICAgIHJldHVybiBOb25l"
    "CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBOb25lCgoKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKIyBSZWNvcmRpbmcgKGNoYXJnZSArIGxpYnJhcnkpCiMg"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5"
    "bmMgZGVmIG92ZXJfbGltaXRfcmVwbHkobWVzc2FnZSwgdXNlcl9pZCwgYmxvY2tfbXNnKToKICAgICIiInYxNS4yODogdGhlIG92"
    "ZXItcXVvdGEgYmxvY2sgKG5vIHByZS1jaGVja2FibGUgbGluaywgb3IgYQogICAgcHJvYmUtc2tpcHBlZCBzb3VyY2UgbGlrZSBZ"
    "b3VUdWJlKSBub3cgZ2V0cyB0aGUgU0FNRSB0cmVhdG1lbnQgYXMgdGhlCiAgICBwcmUtY2hlY2sgYmxvY2s6IGxvZ2dlZCB0byB0"
    "aGUgYWRtaW4gZ3JvdXAsIE9ORSBjbGVhbiBtZXNzYWdlLCBhbmQgdGhlCiAgICBjb21tYW5kIG1lc3NhZ2UgcmVtb3ZlZC4gUmV0"
    "dXJucyAiX19XWkZJWF9IQU5ETEVEX18iIHdoZW4gdGhlIHJlcGx5CiAgICB3YXMgZGVsaXZlcmVkLCBzbyB0aGUgdGFzayBwaXBl"
    "bGluZSBjYW4gYWJvcnQgc2lsZW50bHkuIiIiCiAgICB0cnk6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBfY2hhdCA9IGdldGF0"
    "dHIobWVzc2FnZSwgImNoYXQiLCBOb25lKQogICAgICAgICAgICBfY3R5cGUgPSBzdHIoZ2V0YXR0cihfY2hhdCwgInR5cGUiLCAi"
    "PyIpKQogICAgICAgICAgICBfY25hbWUgPSBzdHIoCiAgICAgICAgICAgICAgICBnZXRhdHRyKF9jaGF0LCAidGl0bGUiLCAiIikK"
    "ICAgICAgICAgICAgICAgIG9yIGdldGF0dHIoX2NoYXQsICJmaXJzdF9uYW1lIiwgIiIpCiAgICAgICAgICAgICAgICBvciAiPyIK"
    "ICAgICAgICAgICAgKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIF9jdHlwZSwgX2NuYW1lID0gIj8iLCAi"
    "PyIKICAgICAgICBmcm9tIGh0bWwgaW1wb3J0IGVzY2FwZSBhcyBfZXNjCgogICAgICAgIHRyeToKICAgICAgICAgICAgX2xncyA9"
    "IF9leHRyYWN0X2xpbmtzKG1lc3NhZ2UpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgX2xncyA9IFtdCiAg"
    "ICAgICAgYXdhaXQgYWRtaW5fbG9nKAogICAgICAgICAgICAi8J+aqyA8Yj5Eb3dubG9hZCByZWplY3RlZCAob3ZlciBxdW90YSk8"
    "L2I+IiwKICAgICAgICAgICAgZiLilI8gPGI+VXNlcjwvYj4g4oaSIDxjb2RlPnt1c2VyX2lkfTwvY29kZT5cbiIKICAgICAgICAg"
    "ICAgZiLilKAgPGI+V2hlcmU8L2I+IOKGkiB7X2N0eXBlfSB7X2NuYW1lWzo2MF19XG4iCiAgICAgICAgICAgIGYi4pSgIDxiPlJl"
    "YXNvbjwvYj4g4oaSIGRhaWx5IGFsbG93YW5jZSBleGhhdXN0ZWQgYmVmb3JlIHRoZSB0YXNrXG4iCiAgICAgICAgICAgIGYi4pSg"
    "IDxiPkxpbmtzPC9iPiDihpIge19lc2MoJywgJy5qb2luKF9sZ3MpWzo5MDBdKSBpZiBfbGdzIGVsc2UgJ25vbmUgZm91bmQnfVxu"
    "IgogICAgICAgICAgICBmIuKUliA8Yj5NZXNzYWdlPC9iPiDihpIge19lc2MoX21zZ190ZXh0KG1lc3NhZ2UpKVs6MTAwMF0gb3Ig"
    "JyhubyB0ZXh0KSd9IiwKICAgICAgICApCiAgICAgICAgZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1faGVscGVyLm1lc3NhZ2VfdXRp"
    "bHMgaW1wb3J0ICgKICAgICAgICAgICAgc2VuZF9tZXNzYWdlIGFzIF9zZW5kLAogICAgICAgICAgICBkZWxldGVfbWVzc2FnZSBh"
    "cyBfZGVsLAogICAgICAgICAgICBlZGl0X21lc3NhZ2UgYXMgX2VkaXQsCiAgICAgICAgKQoKICAgICAgICAjIHYxNS4zMDogaWYg"
    "dGhlIHByb2JlLXNraXAgcGF0aCBsZWZ0IGEgIuKchSBDaGVja2VkLiBTdGFydGluZ+KApiIKICAgICAgICAjIG1lc3NhZ2Ugb24g"
    "c2NyZWVuLCBtb3JwaCBJVCBpbnRvIHRoZSByZWZ1c2FsIGluc3RlYWQgb2YKICAgICAgICAjIHNlbmRpbmcgYSBuZXcgb25lIOKA"
    "lCBvbmUgbWVzc2FnZSBvbiBzY3JlZW4sIGFuZCB0aGUgNnMKICAgICAgICAjIGNsZWFudXAgdGltZXIgaXMgZGlzYXJtZWQgdmlh"
    "IHRoZSBfd3pmaXhfa2VlcCBmbGFnLgogICAgICAgIF9jaGVja2luZyA9IE5vbmUKICAgICAgICB0cnk6CiAgICAgICAgICAgIF9j"
    "aGVja2luZyA9IGdldGF0dHIobWVzc2FnZSwgIl93emZpeF9jaGVja2luZyIsIE5vbmUpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlv"
    "bjoKICAgICAgICAgICAgX2NoZWNraW5nID0gTm9uZQogICAgICAgIGlmIF9jaGVja2luZyBpcyBub3QgTm9uZToKICAgICAgICAg"
    "ICAgdHJ5OgogICAgICAgICAgICAgICAgX2NoZWNraW5nLl93emZpeF9rZWVwID0gVHJ1ZQogICAgICAgICAgICAgICAgYXdhaXQg"
    "X2VkaXQoX2NoZWNraW5nLCBibG9ja19tc2cpCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBf"
    "Y2hlY2tpbmcgPSBOb25lCiAgICAgICAgaWYgX2NoZWNraW5nIGlzIE5vbmU6CiAgICAgICAgICAgIGF3YWl0IF9zZW5kKG1lc3Nh"
    "Z2UsIGJsb2NrX21zZykKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IF9kZWwobWVzc2FnZSkKICAgICAgICBleGNlcHQg"
    "RXhjZXB0aW9uOgogICAgICAgICAgICBwYXNzCiAgICAgICAgcmV0dXJuICJfX1daRklYX0hBTkRMRURfXyIKICAgIGV4Y2VwdCBF"
    "eGNlcHRpb24gYXMgZToKICAgICAgICBMT0dHRVIuZXJyb3IoZiJXWkZJWCBvdmVyX2xpbWl0X3JlcGx5IGZhaWxlZDoge2V9IikK"
    "ICAgICAgICByZXR1cm4gYmxvY2tfbXNnICAjIGZhbGwgYmFjayB0byB0aGUgd3JhcHBlZCBwYXRoCgoKYXN5bmMgZGVmIGNoYXJn"
    "ZShsaXN0ZW5lcik6CiAgICAiIiJDaGFyZ2UgdGhlIGZpbmlzaGVkIHRhc2sncyBhY3R1YWwgc2l6ZTsgcmVmcmVzaCB0aGUgdXNl"
    "ciByZWdpc3RyeS4iIiIKICAgIHRyeToKICAgICAgICBpZiBub3QgYXdhaXQgZW5zdXJlX3JlYWR5KCk6CiAgICAgICAgICAgIHJl"
    "dHVybgogICAgICAgIHVzZXJfaWQgPSBsaXN0ZW5lci51c2VyX2lkCiAgICAgICAgc2l6ZSA9IFdaRklYX0JXX0ZBQ1RPUiAqIGlu"
    "dChsaXN0ZW5lci5zaXplIG9yIDApCiAgICAgICAgdHJ5OgogICAgICAgICAgICBfY24gPSBsaXN0ZW5lci5uYW1lKCkKICAgICAg"
    "ICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBfY24gPSBzdHIoZ2V0YXR0cihsaXN0ZW5lciwgIm5hbWUiLCAiIikgb3Ig"
    "Ij8iKQogICAgICAgIGF3YWl0IGFkbWluX2xvZygKICAgICAgICAgICAgIuKchSA8Yj5Eb3dubG9hZCBjb21wbGV0ZWQ8L2I+IiwK"
    "ICAgICAgICAgICAgZiLilI8gPGI+VXNlcjwvYj4g4oaSIDxjb2RlPnt1c2VyX2lkfTwvY29kZT5cbiIKICAgICAgICAgICAgZiLi"
    "lKAgPGI+RmlsZTwvYj4g4oaSIHtzdHIoX2NuKVs6MTIwXX1cbiIKICAgICAgICAgICAgZiLilJYgPGI+Q2hhcmdlZDwvYj4g4oaS"
    "IHtfbmljZV9zaXplKHNpemUpfSIsCiAgICAgICAgKQogICAgICAgIG5vdyA9IHRpbWUoKQogICAgICAgIGRiID0gX2RiKCkKICAg"
    "ICAgICBwYXJ0ID0gX3BhcnQoKQogICAgICAgIGlmIHNpemUgPiAwOgogICAgICAgICAgICBhd2FpdCBkYi53emZpeF9xdW90YVtw"
    "YXJ0XS51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBmInt1c2VyX2lkfTp7X2RheV9pc3QoKX0ifSwKICAgICAg"
    "ICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAiJGluYyI6IHsidXNlZCI6IHNpemV9LAogICAgICAgICAgICAgICAgICAg"
    "ICIkc2V0IjogewogICAgICAgICAgICAgICAgICAgICAgICAiZXhwaXJlQXQiOiBkYXRldGltZS5mcm9tdGltZXN0YW1wKAogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgbm93ICsgV1pGSVhfUVVPVEFfVFRMX0RBWVMgKiA4NjQwMCwgVVRDCiAgICAgICAgICAg"
    "ICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICB9LAogICAgICAgICAgICAgICAgfSwKICAgICAgICAgICAgICAgIHVw"
    "c2VydD1UcnVlLAogICAgICAgICAgICApCiAgICAgICAgICAgIGRvYyA9IGF3YWl0IGRiLnd6Zml4X3F1b3RhW3BhcnRdLmZpbmRf"
    "b25lKHsiX2lkIjogZiJ7dXNlcl9pZH06e19kYXlfaXN0KCl9In0pCiAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAg"
    "ICAgICAgZiJXWkZJWCBjaGFyZ2U6IHVzZXI9e3VzZXJfaWR9IHRhc2tfc2l6ZT17c2l6ZX0gIgogICAgICAgICAgICAgICAgZiIt"
    "PiB0b2RheV90b3RhbD17aW50KGRvYy5nZXQoJ3VzZWQnLCAwKSkgaWYgZG9jIGVsc2UgJz8nfSAiCiAgICAgICAgICAgICAgICBm"
    "IihkYXk9e19kYXlfaXN0KCl9KSIKICAgICAgICAgICAgKQogICAgICAgIHVuYW1lID0gIiIKICAgICAgICBuYW1lID0gIiIKICAg"
    "ICAgICB0cnk6CiAgICAgICAgICAgIHVuYW1lID0gZ2V0YXR0cihsaXN0ZW5lci51c2VyLCAidXNlcm5hbWUiLCAiIikgb3IgIiIK"
    "ICAgICAgICAgICAgbmFtZSA9IChnZXRhdHRyKGxpc3RlbmVyLnVzZXIsICJmaXJzdF9uYW1lIiwgIiIpIG9yICIiKS5zdHJpcCgp"
    "CiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwogICAgICAgIGF3YWl0IGRiLnd6Zml4X3VzZXJzW3Bh"
    "cnRdLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogdXNlcl9pZH0sCiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAg"
    "ICIkc2V0IjogeyJ1bmFtZSI6IHVuYW1lLCAibmFtZSI6IG5hbWUsICJsYXN0X3VzZWQiOiBub3d9LAogICAgICAgICAgICAgICAg"
    "IiRpbmMiOiB7InRvdGFsX3VzZWQiOiBzaXplLCAidGFza3MiOiAxfSwKICAgICAgICAgICAgfSwKICAgICAgICAgICAgdXBzZXJ0"
    "PVRydWUsCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIGNo"
    "YXJnZSBmYWlsZWQgKGlnbm9yZWQpOiB7ZX0iKQoKCmFzeW5jIGRlZiByZWNvcmRfdGFzayhsaXN0ZW5lciwgbGluaywgZmlsZXMs"
    "IG1pbWVfdHlwZSwgcmNsb25lX3BhdGg9IiIsIGRpcl9pZD0iIik6CiAgICAiIiJDYWxsZWQgZnJvbSBUYXNrTGlzdGVuZXIub25f"
    "dXBsb2FkX2NvbXBsZXRlIGZvciBldmVyeSBmaW5pc2hlZCB0YXNrLiIiIgogICAgIyBmcmVlIHRoaXMgdGFzaydzIGJhbmR3aWR0"
    "aCBob2xkIGJlZm9yZSBjaGFyZ2luZyB0aGUgYWN0dWFsIGFtb3VudAogICAgYXdhaXQgcmVsZWFzZV9yZXNlcnZlKGxpc3RlbmVy"
    "KQogICAgIyBpZGVtcG90ZW5jeTogYSBkdXBsaWNhdGUgY29tcGxldGlvbiBmb3IgdGhlIHNhbWUgdGFzayAoc2FtZSBjb21tYW5k"
    "CiAgICAjIG1lc3NhZ2UsIG5hbWUgYW5kIHNpemUpIG11c3QgbmV2ZXIgY2hhcmdlIHR3aWNlCiAgICB0cnk6CiAgICAgICAga2V5"
    "ID0gewogICAgICAgICAgICAibWlkIjogaW50KGxpc3RlbmVyLm1pZCBvciAwKSwKICAgICAgICAgICAgIm5hbWUiOiBzdHIobGlz"
    "dGVuZXIubmFtZSBvciAiIiksCiAgICAgICAgICAgICJzaXplIjogaW50KGxpc3RlbmVyLnNpemUgb3IgMCksCiAgICAgICAgfQog"
    "ICAgICAgIGR1cCA9IGF3YWl0IF9kYigpLnd6Zml4X2xpYnJhcnlbX3BhcnQoKV0uZmluZF9vbmUoa2V5KQogICAgICAgIGlmIGR1"
    "cDoKICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoCiAgICAgICAgICAgICAgICBmIldaRklYIGNoYXJnZTogRFVQTElDQVRFIGNv"
    "bXBsZXRpb24gZm9yIG1pZD17a2V5WydtaWQnXX0gIgogICAgICAgICAgICAgICAgZiIne2tleVsnbmFtZSddfScgc2l6ZT17a2V5"
    "WydzaXplJ119IC0+IG5vdCBjaGFyZ2VkIGFnYWluIgogICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVybgogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBhd2FpdCBjaGFyZ2UobGlzdGVuZXIpCiAgICB0cnk6CiAgICAgICAgaWYgbm90"
    "IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgICAgICByZXR1cm4KICAgICAgICBub3cgPSB0aW1lKCkKICAgICAgICBwYXJ0"
    "cyA9IFtdCiAgICAgICAgdGdfbGlua3MgPSBbXQogICAgICAgIHBhcnRfbmFtZXMgPSBbXQogICAgICAgIGlmIGlzaW5zdGFuY2Uo"
    "ZmlsZXMsIGRpY3QpOgogICAgICAgICAgICBmb3IgbCwgbiBpbiBmaWxlcy5pdGVtcygpOgogICAgICAgICAgICAgICAgbCA9IHN0"
    "cihsKQogICAgICAgICAgICAgICAgdGdfbGlua3MuYXBwZW5kKGwpCiAgICAgICAgICAgICAgICBwYXJ0X25hbWVzLmFwcGVuZChz"
    "dHIobikpCiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgdGFpbCA9IGwucnN0cmlwKCIvIikuc3BsaXQo"
    "Ii8iKVstMjpdCiAgICAgICAgICAgICAgICAgICAgY2lkLCBtaWQgPSB0YWlsWzBdLCB0YWlsWzFdCiAgICAgICAgICAgICAgICAg"
    "ICAgaWYgY2lkLmlzZGlnaXQoKSBhbmQgbWlkLmlzZGlnaXQoKToKICAgICAgICAgICAgICAgICAgICAgICAgcGFydHMuYXBwZW5k"
    "KFtpbnQoZiItMTAwe2NpZH0iKSwgaW50KG1pZCldKQogICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAg"
    "ICAgICAgICAgICBjb250aW51ZQogICAgICAgIHRyeToKICAgICAgICAgICAgbW9kZSA9IGYie2xpc3RlbmVyLm1vZGVbMF19IOKG"
    "kiB7bGlzdGVuZXIubW9kZVsxXX0iCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgbW9kZSA9ICIiCiAgICAg"
    "ICAgZG9jID0gewogICAgICAgICAgICAiX2lkIjogdXVpZDQoKS5oZXgsCiAgICAgICAgICAgICJtaWQiOiBpbnQobGlzdGVuZXIu"
    "bWlkIG9yIDApLAogICAgICAgICAgICAibmFtZSI6IGxpc3RlbmVyLm5hbWUsCiAgICAgICAgICAgICJzaXplIjogaW50KGxpc3Rl"
    "bmVyLnNpemUgb3IgMCksCiAgICAgICAgICAgICJ1c2VyX2lkIjogbGlzdGVuZXIudXNlcl9pZCwKICAgICAgICAgICAgInVuYW1l"
    "IjogZ2V0YXR0cihsaXN0ZW5lci51c2VyLCAidXNlcm5hbWUiLCAiIikgb3IgIiIsCiAgICAgICAgICAgICJkYXRlIjogbm93LAog"
    "ICAgICAgICAgICAiZXhwaXJlQXQiOiBkYXRldGltZS5mcm9tdGltZXN0YW1wKAogICAgICAgICAgICAgICAgbm93ICsgV1pGSVhf"
    "TElCX1RUTF9EQVlTICogODY0MDAsIFVUQwogICAgICAgICAgICApLAogICAgICAgICAgICAiaXNfbGVlY2giOiBib29sKGxpc3Rl"
    "bmVyLmlzX2xlZWNoKSwKICAgICAgICAgICAgIm1vZGUiOiBtb2RlLAogICAgICAgICAgICAicGFydHMiOiBwYXJ0cywKICAgICAg"
    "ICAgICAgInRnX2xpbmtzIjogdGdfbGlua3MsCiAgICAgICAgICAgICJwYXJ0X25hbWVzIjogcGFydF9uYW1lcywKICAgICAgICAg"
    "ICAgImNsb3VkX2xpbmsiOiBsaW5rIGlmIGlzaW5zdGFuY2UobGluaywgc3RyKSBlbHNlICIiLAogICAgICAgICAgICAicmNsb25l"
    "X3BhdGgiOiByY2xvbmVfcGF0aCBvciAiIiwKICAgICAgICAgICAgImRpcl9pZCI6IGRpcl9pZCBvciAiIiwKICAgICAgICB9CiAg"
    "ICAgICAgYXdhaXQgX2RiKCkud3pmaXhfbGlicmFyeVtfcGFydCgpXS5pbnNlcnRfb25lKGRvYykKICAgICAgICBMT0dHRVIuaW5m"
    "bygKICAgICAgICAgICAgZiJXWkZJWCBsaWJyYXJ5OiByZWNvcmRlZCAne2RvY1snbmFtZSddfScgc2l6ZT17ZG9jWydzaXplJ119"
    "ICIKICAgICAgICAgICAgZiJ1c2VyPXtkb2NbJ3VzZXJfaWQnXX0gdGdfcGFydHM9e2xlbihkb2NbJ3BhcnRzJ10pfSIKICAgICAg"
    "ICApCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggcmVjb3JkX3Rhc2sgZmFp"
    "bGVkIChpZ25vcmVkKToge2V9IikKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgAojIExpYnJhcnkgc2VhcmNoCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIGZpbmQocXVlcnksIHVzZXJfaWQsIGFsbF91c2Vy"
    "cz1GYWxzZSwgbGltaXQ9Nik6CiAgICBpZiBub3QgYXdhaXQgZW5zdXJlX3JlYWR5KCk6CiAgICAgICAgcmV0dXJuIFtdCiAgICBx"
    "ID0geyJuYW1lIjogeyIkcmVnZXgiOiBfcmVzY2FwZShxdWVyeSksICIkb3B0aW9ucyI6ICJpIn19CiAgICBpZiBub3QgYWxsX3Vz"
    "ZXJzOgogICAgICAgIHFbInVzZXJfaWQiXSA9IHVzZXJfaWQKICAgIHRyeToKICAgICAgICBjdXJzb3IgPSAoCiAgICAgICAgICAg"
    "IF9kYigpLnd6Zml4X2xpYnJhcnlbX3BhcnQoKV0uZmluZChxKS5zb3J0KCJkYXRlIiwgLTEpLmxpbWl0KGludChsaW1pdCkpCiAg"
    "ICAgICAgKQogICAgICAgIHJldHVybiBbZCBhc3luYyBmb3IgZCBpbiBjdXJzb3JdCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6"
    "CiAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggZmluZCBmYWlsZWQ6IHtlfSIpCiAgICAgICAgcmV0dXJuIFtdCgoKIyDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKIyBEQiBzdGF0"
    "cyAvIGNsZWFudXAKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIAKCgphc3luYyBkZWYgZGJzdGF0cygpOgogICAgIiIid3ptbHggYnJlYWtkb3duICsgYWNjb3VudC13aWRlIChhbGwg"
    "ZGF0YWJhc2VzKSBzaXplcy4iIiIKICAgIG91dCA9IHsidG90YWwiOiAwLCAiY29scyI6IFtdLCAiZGJzIjogW10sICJhY2NvdW50"
    "X3RvdGFsIjogMCwgImRiX2Vycm9yIjogIiIsICJlcnJvciI6ICIifQogICAgdHJ5OgogICAgICAgIGRiID0gX2RiKCkKICAgICAg"
    "ICBpZiBkYiBpcyBOb25lOgogICAgICAgICAgICBvdXRbImVycm9yIl0gPSAiZGF0YWJhc2Ugbm90IGNvbm5lY3RlZCIKICAgICAg"
    "ICAgICAgcmV0dXJuIG91dAogICAgICAgIHN0ID0gYXdhaXQgZGIuY29tbWFuZCh7ImRiU3RhdHMiOiAxLCAic2NhbGUiOiAxMDI0"
    "fSkKICAgICAgICBvdXRbInRvdGFsIl0gPSBpbnQoc3QuZ2V0KCJkYXRhU2l6ZSIpIG9yIDApICogMTAyNAogICAgZXhjZXB0IEV4"
    "Y2VwdGlvbiBhcyBlOgogICAgICAgIG91dFsiZXJyb3IiXSA9IHN0cihlKQogICAgdHJ5OgogICAgICAgIG5hbWVzID0gYXdhaXQg"
    "X2RiKCkubGlzdF9jb2xsZWN0aW9uX25hbWVzKCkKICAgICAgICBwYXJ0ID0gX3BhcnQoKQogICAgICAgIGFnZyA9IHt9CiAgICAg"
    "ICAgZm9yIG5hbWUgaW4gbmFtZXM6CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGNzID0gYXdhaXQgX2RiKCkuY29t"
    "bWFuZCh7ImNvbGxTdGF0cyI6IG5hbWV9KQogICAgICAgICAgICAgICAgaWYgIi4iIGluIG5hbWU6CiAgICAgICAgICAgICAgICAg"
    "ICAgYmFzZSwgc3VmZml4ID0gbmFtZS5zcGxpdCgiLiIsIDEpCiAgICAgICAgICAgICAgICAgICAgIyBhIHBhcnRpdGlvbiBzdWZm"
    "aXggZXF1YWwgdG8gb3VycyA9IHRoaXMgYm90OyBvdGhlciBib3RzCiAgICAgICAgICAgICAgICAgICAgIyBzaGFyaW5nIHRoaXMg"
    "QXRsYXMgYWNjb3VudCBnZXQgdGhlaXIgb3duIGF0dHJpYnV0aW9uCiAgICAgICAgICAgICAgICAgICAgb3duZXIgPSAic2VsZiIg"
    "aWYgc3VmZml4ID09IHBhcnQgZWxzZSAib3RoZXIiCiAgICAgICAgICAgICAgICBlbHNlOgogICAgICAgICAgICAgICAgICAgIGJh"
    "c2UsIG93bmVyID0gbmFtZSwgInNoYXJlZCIKICAgICAgICAgICAgICAgIGEgPSBhZ2cuc2V0ZGVmYXVsdCgKICAgICAgICAgICAg"
    "ICAgICAgICBiYXNlLAogICAgICAgICAgICAgICAgICAgIHsiZG9jcyI6IDAsICJzaXplIjogMCwgInNlbGYiOiAwLCAib3RoZXIi"
    "OiAwLCAic2hhcmVkIjogMCwgIm4iOiAwfSwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIHN6ID0gaW50KGNzLmdl"
    "dCgic2l6ZSIpIG9yIDApCiAgICAgICAgICAgICAgICBhWyJkb2NzIl0gKz0gaW50KGNzLmdldCgiY291bnQiKSBvciAwKQogICAg"
    "ICAgICAgICAgICAgYVsic2l6ZSJdICs9IHN6CiAgICAgICAgICAgICAgICBhW293bmVyXSArPSBzegogICAgICAgICAgICAgICAg"
    "YVsibiJdICs9IDEKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAg"
    "b3V0WyJjb2xzIl0gPSBzb3J0ZWQoYWdnLml0ZW1zKCksIGtleT1sYW1iZGEga3Y6IC1rdlsxXVsic2l6ZSJdKQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIG91dFsiZXJyb3IiXSA9IHN0cihlKQogICAgIyBhY2NvdW50LXdpZGU6IGV2ZXJ5IGRh"
    "dGFiYXNlIG9uIHRoaXMgQXRsYXMgY2x1c3RlciAodGhlIGZyZWUtdGllcgogICAgIyA1MTIgTUIgaXMgc2hhcmVkIGFjcm9zcyBB"
    "TEwgb2YgdGhlbSwgbm90IGp1c3Qgd3ptbHgpCiAgICB0cnk6CiAgICAgICAgYWRtaW5fZGIgPSBfZGIoKS5jbGllbnQuZ2V0X2Rh"
    "dGFiYXNlKCJhZG1pbiIpCiAgICAgICAgbGQgPSBhd2FpdCBhZG1pbl9kYi5jb21tYW5kKHsibGlzdERhdGFiYXNlcyI6IDF9KQog"
    "ICAgICAgIG91dFsiZGJzIl0gPSBzb3J0ZWQoCiAgICAgICAgICAgICgKICAgICAgICAgICAgICAgIChzdHIoZC5nZXQoIm5hbWUi"
    "KSksIGludChkLmdldCgic2l6ZU9uRGlzayIpIG9yIDApKQogICAgICAgICAgICAgICAgZm9yIGQgaW4gbGQuZ2V0KCJkYXRhYmFz"
    "ZXMiLCBbXSkKICAgICAgICAgICAgKSwKICAgICAgICAgICAga2V5PWxhbWJkYSB0OiAtdFsxXSwKICAgICAgICApCiAgICAgICAg"
    "b3V0WyJhY2NvdW50X3RvdGFsIl0gPSBpbnQobGQuZ2V0KCJ0b3RhbFNpemUiKSBvciAwKSBvciBzdW0oCiAgICAgICAgICAgIHMg"
    "Zm9yIF8sIHMgaW4gb3V0WyJkYnMiXQogICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICBvdXRbImRi"
    "X2Vycm9yIl0gPSBzdHIoZSkKICAgIHJldHVybiBvdXQKCgphc3luYyBkZWYgZGJjbGVhbigpOgogICAgIiIiU2FmZSBwdXJnZXMg"
    "b25seS4gUmV0dXJucyB7bGFiZWw6IGZyZWVkX2RvY19jb3VudH0uIiIiCiAgICBmcmVlZCA9IHt9CiAgICBkYiA9IF9kYigpCiAg"
    "ICBwYXJ0ID0gX3BhcnQoKQogICAgdHJ5OgogICAgICAgIHIgPSBhd2FpdCBkYi5zdHJlYW1zW3BhcnRdLmRlbGV0ZV9tYW55KHsi"
    "ZXhwIjogeyIkbHQiOiBpbnQodGltZSgpKX19KQogICAgICAgIGZyZWVkWyJleHBpcmVkIHN0cmVhbSB0b2tlbnMiXSA9IHIuZGVs"
    "ZXRlZF9jb3VudAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICB0cnk6CiAgICAgICAgciA9IGF3YWl0IGRi"
    "LnRhc2tzW3BhcnRdLmRlbGV0ZV9tYW55KHt9KQogICAgICAgIGZyZWVkWyJpbmNvbXBsZXRlLXRhc2sgcmVzdW1lIHJlY29yZHMi"
    "XSA9IHIuZGVsZXRlZF9jb3VudAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICB0cnk6CiAgICAgICAgciA9"
    "IGF3YWl0IGRiLnd6Zml4X3F1b3RhW3BhcnRdLmRlbGV0ZV9tYW55KAogICAgICAgICAgICB7ImV4cGlyZUF0IjogeyIkbHQiOiBk"
    "YXRldGltZS5ub3coVVRDKX19CiAgICAgICAgKQogICAgICAgIGZyZWVkWyJzdGFsZSB3emZpeCBxdW90YSByb3dzIl0gPSByLmRl"
    "bGV0ZWRfY291bnQKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIGZyZWVkCgoKdHJ5OgogICAg"
    "ZnJvbSAudmVyc2lvbnMgaW1wb3J0IFdaRklYX0JVSUxELCBXWkZJWF9EQVRFLCBXWkZJWF9CQVNFCgogICAgV1pGSVhfQlVJTERf"
    "SU5GTyA9IGYie1daRklYX0JVSUxEfSAoe1daRklYX0RBVEV9KSIKZXhjZXB0IEV4Y2VwdGlvbjoKICAgIFdaRklYX0JVSUxEID0g"
    "V1pGSVhfQlVJTERfSU5GTyA9ICI/IgogICAgV1pGSVhfREFURSA9IFdaRklYX0JBU0UgPSAiPyIKCgphc3luYyBkZWYgYm9vdF9y"
    "ZXBvcnQoKToKICAgICIiIkRlbGF5ZWQgYm9vdCBsb2c6IHZlcnNpb25zLCB0b3RhbCBEQiBzaXplLCB3YXJuaW5nIHBhc3QgODAl"
    "LiIiIgogICAgdHJ5OgogICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgc2xlZXAKCiAgICAgICAgYXdhaXQgc2xlZXAoNDUpCiAg"
    "ICAgICAgdHJ5OgogICAgICAgICAgICBpbXBvcnQgeXRfZGxwIGFzIF95ZAoKICAgICAgICAgICAgX3l2ID0gX3lkLnZlcnNpb24u"
    "X192ZXJzaW9uX18KICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBfeXYgPSAidW5rbm93biIKICAgICAgICBM"
    "T0dHRVIuaW5mbygKICAgICAgICAgICAgZiJXWkZJWCBCVUlMRCB7V1pGSVhfQlVJTER9IHJ1bm5pbmcgfCBiYXNlOiB7V1pGSVhf"
    "QkFTRX0gfCAiCiAgICAgICAgICAgIGYieXQtZGxwIHtfeXZ9IHwge1daRklYX0RBVEV9IgogICAgICAgICkKICAgICAgICBzdCA9"
    "IGF3YWl0IGRic3RhdHMoKQogICAgICAgIGlmIHN0LmdldCgiZXJyb3IiKSBhbmQgbm90IHN0LmdldCgiY29scyIpOgogICAgICAg"
    "ICAgICByZXR1cm4KICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgZiJXWkZJWCBEQjoge19uaWNlX3NpemUoc3RbJ3Rv"
    "dGFsJ10pfSBhY3Jvc3MgIgogICAgICAgICAgICBmIntsZW4oc3RbJ2NvbHMnXSl9IGNvbGxlY3Rpb24gZ3JvdXBzIChmcmVlIHRp"
    "ZXIgNTEyIE1CKSIKICAgICAgICApCiAgICAgICAgaWYgc3RbInRvdGFsIl0gPiAwLjggKiBXWkZJWF9BVExBU19GUkVFOgogICAg"
    "ICAgICAgICBMT0dHRVIud2FybmluZygKICAgICAgICAgICAgICAgICJXWkZJWCBEQjogdXNhZ2UgaXMgYWJvdmUgODAlIG9mIHRo"
    "ZSBBdGxhcyBmcmVlIHRpZXIgKDUxMiBNQikhICIKICAgICAgICAgICAgICAgICJSdW4gL2Ric3RhdHMgYW5kIC9kYmNsZWFuIHRv"
    "IHJlY2xhaW0gc3BhY2UuIgogICAgICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKCgphc3luYyBk"
    "ZWYgZGlhZyh1c2VyX2lkLCBwcm9iZV9nYj1Ob25lKToKICAgICIiIkNvbXBsZXRlIHF1b3RhIHN0YXRlIGZvciBvbmUgdXNlciwg"
    "Zm9yIGxpdmUgZGVidWdnaW5nLiIiIgogICAgb3V0ID0ge30KICAgIG91dFsicGFydGl0aW9uIl0gPSBfcGFydCgpCiAgICBvdXRb"
    "ImRheSJdID0gX2RheV9pc3QoKQogICAgb3V0WyJkYl9yZWFkeSJdID0gYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgIG91dFsidXNl"
    "cl9kb2MiXSA9IGF3YWl0IGdldF91c2VyX2RvYyh1c2VyX2lkKQogICAgb3V0WyJnbG9iYWxfY2FwX2diIl0gPSBhd2FpdCBfZ2V0"
    "X2dsb2JhbF9jYXBfZ2IoKQogICAgb3V0WyJjYXBfYnl0ZXMiXSA9IGF3YWl0IGdldF9jYXBfYnl0ZXModXNlcl9pZCkKICAgIG91"
    "dFsicXVvdGFfZG9jIl0gPSBOb25lCiAgICB0cnk6CiAgICAgICAgb3V0WyJxdW90YV9kb2MiXSA9IGF3YWl0IF9kYigpLnd6Zml4"
    "X3F1b3RhW19wYXJ0KCldLmZpbmRfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYie3VzZXJfaWR9OntfZGF5X2lzdCgpfSJ9CiAg"
    "ICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBvdXRbInJlc2VydmVkIl0gPSBhd2FpdCByZXNl"
    "cnZlZF90b2RheSh1c2VyX2lkKQogICAgaWYgcHJvYmVfZ2IgaXMgbm90IE5vbmU6CiAgICAgICAgcHJvYmUgPSBwcm9iZV9nYiAq"
    "IEdCCiAgICAgICAgdXNlZCA9IGF3YWl0IGdldF91c2FnZSh1c2VyX2lkKQogICAgICAgIGNhcCA9IG91dFsiY2FwX2J5dGVzIl0K"
    "ICAgICAgICBvdXRbInByb2JlIl0gPSB7CiAgICAgICAgICAgICJzaXplIjogcHJvYmUsCiAgICAgICAgICAgICJ1c2VkIjogdXNl"
    "ZCwKICAgICAgICAgICAgInJlc2VydmVkIjogb3V0WyJyZXNlcnZlZCJdLAogICAgICAgICAgICAiY2FwIjogY2FwLAogICAgICAg"
    "ICAgICAid291bGRfYmxvY2siOiAoCiAgICAgICAgICAgICAgICB1c2VkICsgb3V0WyJyZXNlcnZlZCJdID49IGNhcAogICAgICAg"
    "ICAgICAgICAgb3IgdXNlZCArIG91dFsicmVzZXJ2ZWQiXSArIHByb2JlID4gY2FwICsgV1pGSVhfR1JBQ0VfTUIgKiBNQgogICAg"
    "ICAgICAgICApLAogICAgICAgIH0KICAgIHJldHVybiBvdXQKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIE93bmVyLWZhY2luZyBzdW1tYXJpZXMKIyDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgdG9kYXlf"
    "cm93cygpOgogICAgIiIiQWxsIG9mIHRvZGF5J3MgcXVvdGEgcm93czogW3sidXNlcl9pZCIsInVzZWQifSwgLi4uXSBsYXJnZXN0"
    "IGZpcnN0LiIiIgogICAgdHJ5OgogICAgICAgIGRheSA9IF9kYXlfaXN0KCkKICAgICAgICBjdXJzb3IgPSBfZGIoKS53emZpeF9x"
    "dW90YVtfcGFydCgpXS5maW5kKHsiX2lkIjogeyIkcmVnZXgiOiBmIjp7ZGF5fSQifX0pCiAgICAgICAgcm93cyA9IFtdCiAgICAg"
    "ICAgYXN5bmMgZm9yIGQgaW4gY3Vyc29yOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICB1aWQgPSBpbnQoc3RyKGRb"
    "Il9pZCJdKS5yc3BsaXQoIjoiLCAxKVswXSkKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGNv"
    "bnRpbnVlCiAgICAgICAgICAgIHJvd3MuYXBwZW5kKHsidXNlcl9pZCI6IHVpZCwgInVzZWQiOiBpbnQoZC5nZXQoInVzZWQiKSBv"
    "ciAwKX0pCiAgICAgICAgcm93cy5zb3J0KGtleT1sYW1iZGEgcjogLXJbInVzZWQiXSkKICAgICAgICByZXR1cm4gcm93cwogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gW10KCgphc3luYyBkZWYgYWxsX3VzZXJzKCk6CiAgICB0cnk6CiAgICAg"
    "ICAgY3Vyc29yID0gX2RiKCkud3pmaXhfdXNlcnNbX3BhcnQoKV0uZmluZCgpLnNvcnQoImxhc3RfdXNlZCIsIC0xKQogICAgICAg"
    "IHJldHVybiBbZCBhc3luYyBmb3IgZCBpbiBjdXJzb3JdCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBbXQo="
)
# bot/helper/wzfix/r3_music.py — music app integrations (Spotify / JioSaavn / Apple Music)

WZFIX_R3_MUSIC_B64 = (
    "IyBXWkZJWCBSb3VuZCAzIOKAlCBtdXNpYyBhcHAgaW50ZWdyYXRpb25zICh2MTUuNzQpLiBTcG90"
    "aWZ5IC8gSmlvU2Fhdm4gLwojIEFwcGxlIE11c2ljIGxpbmtzIGFyZSByZXNvbHZlZCB0byBhIHl0"
    "c2VhcmNoIHF1ZXJ5IGFuZCByb3V0ZWQgdGhyb3VnaCB0aGUKIyB5dC1kbHAgZW5naW5lLCBzbyB0"
    "aGUgcXVvdGEsIHRoZSBjaGVja2luZyBtZXNzYWdlLCB0aGUgYWRtaW4gbG9ncyBhbmQgdGhlCiMg"
    "dXBsb2FkIHR1bmluZyBhbGwgYXBwbHkgdG8gbXVzaWMgZG93bmxvYWRzIHVuY2hhbmdlZC4gVHJh"
    "Y2sgbGlua3MgcmVzb2x2ZQojIHRvIHRoZSBleGFjdCBzb25nOyBhIFNwb3RpZnkgYXJ0aXN0IGxp"
    "bmsgZmFucyBvdXQgaW50byBvbmUgdGFzayBwZXIKIyB0b3AgdHJhY2sgKGV4YWN0IFNwb3RpZnkg"
    "bGlzdCwgY2xlYW4gbmFtZXMsIG9uZSBzb25nIGVhY2gpLiBBbGJ1bSAvCiMgcGxheWxpc3QgbGlu"
    "a3MgYmVjb21lIGEgc2luZ2xlIHl0c2VhcmNoIHBsYXlsaXN0IHRhc2suCiMgTm8gQVBJIGtleXMg"
    "YXJlIHVzZWQ6IHRoZSBzb3VyY2UgcGFnZXMgYXJlIGZldGNoZWQgd2l0aCBhIGJyb3dzZXIgdXNl"
    "cgojIGFnZW50LCBTcG90aWZ5J3Mgb0VtYmVkIGVuZHBvaW50IHN1cHBsaWVzIGFydGlzdC9hbGJ1"
    "bSBuYW1lcywgYW5kIHRoZQojIFVSTCBzbHVnIGlzIHRoZSBhbHdheXMtYXZhaWxhYmxlIGZhbGxi"
    "YWNrLgoKaW1wb3J0IGFzeW5jaW8KaW1wb3J0IGpzb24KaW1wb3J0IG9zCmltcG9ydCByZQpmcm9t"
    "IGh0bWwgaW1wb3J0IHVuZXNjYXBlCgp0cnk6CiAgICBmcm9tIC5yMV9jb3JlIGltcG9ydCBfZ2V0"
    "X2dsb2JhbF9tdXNpYywgYWRtaW5fbG9nLCBnZXRfdXNlcl9tdXNpYwpleGNlcHQgRXhjZXB0aW9u"
    "OgogICAgYWRtaW5fbG9nID0gTm9uZQogICAgZ2V0X3VzZXJfbXVzaWMgPSBOb25lCiAgICBfZ2V0"
    "X2dsb2JhbF9tdXNpYyA9IE5vbmUKCnRyeToKICAgIGZyb20gbG9nZ2luZyBpbXBvcnQgZ2V0TG9n"
    "Z2VyCgogICAgX0xPRyA9IGdldExvZ2dlcihfX25hbWVfXykKZXhjZXB0IEV4Y2VwdGlvbjoKICAg"
    "IF9MT0cgPSBOb25lCgoKZGVmIF9sb2cobXNnKToKICAgIHRyeToKICAgICAgICBpZiBfTE9HIGlz"
    "IG5vdCBOb25lOgogICAgICAgICAgICBfTE9HLmluZm8obXNnKQogICAgZXhjZXB0IEV4Y2VwdGlv"
    "bjoKICAgICAgICBwYXNzCgoKX1VBID0gewogICAgIlVzZXItQWdlbnQiOiAoCiAgICAgICAgIk1v"
    "emlsbGEvNS4wIChXaW5kb3dzIE5UIDEwLjA7IFdpbjY0OyB4NjQpIEFwcGxlV2ViS2l0LzUzNy4z"
    "NiAiCiAgICAgICAgIihLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzEyNC4wLjAuMCBTYWZhcmkv"
    "NTM3LjM2IgogICAgKSwKICAgICJBY2NlcHQtTGFuZ3VhZ2UiOiAiZW4tVVMsZW47cT0wLjkiLAp9"
    "CgpfVVJMX1JFID0gcmUuY29tcGlsZShyImh0dHBzPzovL1xTKyIsIHJlLkkpCgpfTVVTSUNfSE9T"
    "VFMgPSAoCiAgICAib3Blbi5zcG90aWZ5LmNvbSIsCiAgICAic3BvdGlmeS5saW5rIiwKICAgICJq"
    "aW9zYWF2bi5jb20iLAogICAgInNhYXZuLmNvbSIsCiAgICAibXVzaWMuYXBwbGUuY29tIiwKKQoK"
    "CmNsYXNzIE11c2ljVW5zdXBwb3J0ZWQoRXhjZXB0aW9uKToKICAgICIiIkEgbXVzaWMgbGluayB0"
    "aGUgYm90IHJlY29nbmlzZXMgYnV0IGNhbm5vdCBoYW5kbGUuIiIiCgoKZGVmIF9pc19vd25lcih1"
    "c2VyX2lkKToKICAgICIiIlRoZSBib3Qgb3duZXIocykg4oCUIHRoZWlyIGRlZmF1bHQgbXVzaWMg"
    "bGltaXQgaXMgdW5saW1pdGVkLgogICAgT1dORVJfSUQgbWF5IGJlIGFuIGludCwgYSBsaXN0IG9m"
    "IGludHMsIG9yIGEgbnVtZXJpYyBzdHJpbmcuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi5j"
    "b3JlLmNvbmZpZ19tYW5hZ2VyIGltcG9ydCBDb25maWcKCiAgICAgICAgaWYgbm90IHVzZXJfaWQ6"
    "CiAgICAgICAgICAgIHJldHVybiBGYWxzZQogICAgICAgIG8gPSBnZXRhdHRyKENvbmZpZywgIk9X"
    "TkVSX0lEIiwgTm9uZSkKICAgICAgICBpZiBub3QgbzoKICAgICAgICAgICAgcmV0dXJuIEZhbHNl"
    "CiAgICAgICAgaWYgaXNpbnN0YW5jZShvLCAobGlzdCwgdHVwbGUsIHNldCkpOgogICAgICAgICAg"
    "ICByZXR1cm4gYW55KAogICAgICAgICAgICAgICAgc3RyKHVzZXJfaWQpID09IHN0cih4KQogICAg"
    "ICAgICAgICAgICAgZm9yIHggaW4gbwogICAgICAgICAgICAgICAgaWYgc3RyKHgpLnN0cmlwKCkK"
    "ICAgICAgICAgICAgKQogICAgICAgIHJldHVybiBzdHIodXNlcl9pZCkgPT0gc3RyKG8pCiAgICBl"
    "eGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBGYWxzZQoKCmRlZiBfbXVzaWNfbWF4KCk6"
    "CiAgICAiIiJIYXJkIGRlZmF1bHQ6IDEwIHNvbmdzIChXWkZJWF9NVVNJQ19NQVggZW52LCBjbGFt"
    "cGVkIDHigJM1MDApLiIiIgogICAgdHJ5OgogICAgICAgIHJldHVybiBtYXgoMSwgbWluKDUwMCwg"
    "aW50KG9zLmVudmlyb24uZ2V0KCJXWkZJWF9NVVNJQ19NQVgiLCAiMTAiKSkpKQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gMTAKCgphc3luYyBkZWYgX211c2ljX21heF9mb3Io"
    "dXNlcl9pZD0wKToKICAgICIiIkVmZmVjdGl2ZSBsaW1pdCBmb3IgdGhpcyB1c2VyOiB0aGVpciBv"
    "dmVycmlkZSDihpIgb3duZXIgdW5saW1pdGVkCiAgICDihpIgZ2xvYmFsIGRlZmF1bHQgKERCLCBz"
    "ZXR0YWJsZSBvbiB0aGUgYWRtaW4gd2ViIHBhZ2UpIOKGkiBlbnYg4oaSIDEwLiIiIgogICAgdHJ5"
    "OgogICAgICAgIGlmIHVzZXJfaWQgYW5kIGdldF91c2VyX211c2ljIGlzIG5vdCBOb25lOgogICAg"
    "ICAgICAgICB2ID0gYXdhaXQgZ2V0X3VzZXJfbXVzaWModXNlcl9pZCkKICAgICAgICAgICAgaWYg"
    "djoKICAgICAgICAgICAgICAgIHJldHVybiB2CiAgICAgICAgaWYgdXNlcl9pZCBhbmQgX2lzX293"
    "bmVyKHVzZXJfaWQpOgogICAgICAgICAgICByZXR1cm4gNTAwCiAgICAgICAgaWYgX2dldF9nbG9i"
    "YWxfbXVzaWMgaXMgbm90IE5vbmU6CiAgICAgICAgICAgIGcgPSBhd2FpdCBfZ2V0X2dsb2JhbF9t"
    "dXNpYygpCiAgICAgICAgICAgIGlmIGc6CiAgICAgICAgICAgICAgICByZXR1cm4gZwogICAgZXhj"
    "ZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gX211c2ljX21heCgpCgoKZGVm"
    "IF9ob3N0X29mKHVybCk6CiAgICB0cnk6CiAgICAgICAgbSA9IHJlLm1hdGNoKHIiaHR0cHM/Oi8v"
    "KFteL10rKS8iLCB1cmwgKyAiLyIpCiAgICAgICAgcmV0dXJuIChtLmdyb3VwKDEpIGlmIG0gZWxz"
    "ZSAiIikubG93ZXIoKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gIiIKCgpk"
    "ZWYgaXNfbXVzaWNfdXJsKHVybCk6CiAgICBoID0gX2hvc3Rfb2YodXJsKQogICAgaWYgbm90IGg6"
    "CiAgICAgICAgcmV0dXJuIEZhbHNlCiAgICBmb3IgbWggaW4gX01VU0lDX0hPU1RTOgogICAgICAg"
    "IGlmIGggPT0gbWggb3IgaC5lbmRzd2l0aCgiLiIgKyBtaCk6CiAgICAgICAgICAgIHJldHVybiBU"
    "cnVlCiAgICByZXR1cm4gRmFsc2UKCgpkZWYgX3NsdWdfcXVlcnkodXJsKToKICAgICIiIkJlc3Qt"
    "ZWZmb3J0IHNlYXJjaCB0ZXJtcyBzdHJhaWdodCBmcm9tIHRoZSBVUkwgc2x1ZyDigJQgdGhlCiAg"
    "ICBhbHdheXMtYXZhaWxhYmxlIGZhbGxiYWNrIHdoZW4gdGhlIHBhZ2UgY2Fubm90IGJlIGZldGNo"
    "ZWQuIiIiCiAgICB0cnk6CiAgICAgICAgcGF0aCA9IHVybC5zcGxpdCgiPyIpWzBdLnNwbGl0KCIj"
    "IilbMF0KICAgICAgICBwYXJ0cyA9IFtwIGZvciBwIGluIHBhdGguc3BsaXQoIi8iKSBpZiBwXQog"
    "ICAgICAgIGZvciBzZWcgaW4gcmV2ZXJzZWQocGFydHMpOgogICAgICAgICAgICBzID0gc2VnLnN0"
    "cmlwKCkKICAgICAgICAgICAgaWYgbm90IHMgb3Igbm90IHJlLnNlYXJjaChyIlthLXpBLVpdIiwg"
    "cyk6CiAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICAjIHNraXAgb3BhcXVlIGlk"
    "czogc3BvdGlmeSB0cmFjayBpZHMsIGppb3NhYXZuL2FwcGxlIG51bWVyaWMgaWRzCiAgICAgICAg"
    "ICAgIGlmIHJlLmZ1bGxtYXRjaChyIlswLTlhLXpBLVpdezEwLH0iLCBzKToKICAgICAgICAgICAg"
    "ICAgIGNvbnRpbnVlCiAgICAgICAgICAgIHJldHVybiBzLnJlcGxhY2UoIi0iLCAiICIpLnJlcGxh"
    "Y2UoIl8iLCAiICIpLnN0cmlwKCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwog"
    "ICAgcmV0dXJuIE5vbmUKCgphc3luYyBkZWYgX2ZldGNoKHVybCwgdGltZW91dD0xNS4wKToKICAg"
    "ICIiIkdFVCB0aGUgcGFnZSBmb2xsb3dpbmcgcmVkaXJlY3RzOyByZXR1cm5zIChmaW5hbF91cmws"
    "IGh0bWwpLgogICAgUmV0cmllcyBvbmNlIOKAlCB0cmFuc2llbnQgbmV0d29yayBibGlwcyBhcmUg"
    "Y29tbW9uIG9uIEthZ2dsZS4iIiIKICAgIGZyb20gYWlvaHR0cCBpbXBvcnQgQ2xpZW50U2Vzc2lv"
    "biwgQ2xpZW50VGltZW91dAoKICAgIF9lcnIgPSBOb25lCiAgICBmb3IgX2F0dGVtcHQgaW4gcmFu"
    "Z2UoMik6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBhc3luYyB3aXRoIENsaWVudFNlc3Npb24o"
    "CiAgICAgICAgICAgICAgICB0aW1lb3V0PUNsaWVudFRpbWVvdXQodG90YWw9dGltZW91dCksCiAg"
    "ICAgICAgICAgICAgICBoZWFkZXJzPV9VQSwKICAgICAgICAgICAgICAgIHRydXN0X2Vudj1UcnVl"
    "LAogICAgICAgICAgICApIGFzIF9zOgogICAgICAgICAgICAgICAgYXN5bmMgd2l0aCBfcy5nZXQo"
    "dXJsKSBhcyBfcjoKICAgICAgICAgICAgICAgICAgICByZXR1cm4gc3RyKF9yLnVybCksIGF3YWl0"
    "IF9yLnRleHQoKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAgICAgX2Vy"
    "ciA9IGUKICAgICAgICAgICAgaWYgX2F0dGVtcHQ6CiAgICAgICAgICAgICAgICByYWlzZQogICAg"
    "cmFpc2UgX2VycgoKCmFzeW5jIGRlZiBfb2VtYmVkX3RpdGxlKHVybCk6CiAgICAiIiJTcG90aWZ5"
    "IG9FbWJlZCDigJQgdGhlIG5hbWUgb2YgYSB0cmFjay9hcnRpc3QvYWxidW0vcGxheWxpc3QsIG5v"
    "IGtleS4iIiIKICAgIHRyeToKICAgICAgICBpbXBvcnQganNvbiBhcyBfanNvbgoKICAgICAgICBm"
    "aW5hbCwgaHRtbCA9IGF3YWl0IF9mZXRjaChmImh0dHBzOi8vb3Blbi5zcG90aWZ5LmNvbS9vZW1i"
    "ZWQ/dXJsPXt1cmx9IikKICAgICAgICBqID0gX2pzb24ubG9hZHMoaHRtbCkKICAgICAgICBpZiBq"
    "LmdldCgidGl0bGUiKToKICAgICAgICAgICAgcmV0dXJuIF9jbGVhbihqWyJ0aXRsZSJdKQogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gTm9uZQoKCmFzeW5jIGRl"
    "ZiBfb2VtYmVkX3RpdGxlX2FsdCh1cmwpOgogICAgIiIiVGhlIHNhbWUgb0VtYmVkIHZpYSB0aGUg"
    "YWx0ZXJuYXRlIGVtYmVkLnNwb3RpZnkuY29tIGhvc3QuIiIiCiAgICB0cnk6CiAgICAgICAgaW1w"
    "b3J0IGpzb24gYXMgX2pzb24KCiAgICAgICAgZmluYWwsIGh0bWwgPSBhd2FpdCBfZmV0Y2goCiAg"
    "ICAgICAgICAgIGYiaHR0cHM6Ly9lbWJlZC5zcG90aWZ5LmNvbS9vZW1iZWQvP3VybD17dXJsfSIK"
    "ICAgICAgICApCiAgICAgICAgaiA9IF9qc29uLmxvYWRzKGh0bWwpCiAgICAgICAgaWYgai5nZXQo"
    "InRpdGxlIik6CiAgICAgICAgICAgIHJldHVybiBfY2xlYW4oalsidGl0bGUiXSkKICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIE5vbmUKCgphc3luYyBkZWYgX2Vt"
    "YmVkX2VudGl0eSh1cmwpOgogICAgIiIiU3BvdGlmeSdzIFNTUiBlbWJlZCBwYWdlIOKAlCB0aGUg"
    "dHJhY2svYWxidW0vYXJ0aXN0L3BsYXlsaXN0IG5hbWUKICAgIGFuZCBhcnRpc3QgZnJvbSBpdHMg"
    "X19ORVhUX0RBVEFfXyBKU09OLiBXb3JrcyBldmVuIHdoZW4gdGhlIG1haW4KICAgIHBhZ2Ugc2Vy"
    "dmVzIHRoZSBKUyBzaGVsbCwgYW5kIHVubGlrZSBvRW1iZWQgaXQgaGFzIHRoZSBhcnRpc3QgdG9v"
    "LiIiIgogICAgdHJ5OgogICAgICAgIGltcG9ydCBqc29uIGFzIF9qc29uCgogICAgICAgIF9mcCA9"
    "IHVybC5zcGxpdCgiPyIpWzBdLnJzdHJpcCgiLyIpCiAgICAgICAgZXAgPSByZS5zdWIoCiAgICAg"
    "ICAgICAgIHIiXmh0dHBzPzovL29wZW5cLnNwb3RpZnlcLmNvbS8oPzppbnRsLVthLXotXSsvKT8i"
    "LAogICAgICAgICAgICAiaHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL2VtYmVkLyIsCiAgICAgICAg"
    "ICAgIF9mcCwKICAgICAgICApCiAgICAgICAgZmluYWwsIGh0bWwgPSBhd2FpdCBfZmV0Y2goZXAp"
    "CiAgICAgICAgbSA9IHJlLnNlYXJjaCgKICAgICAgICAgICAgcic8c2NyaXB0IGlkPSJfX05FWFRf"
    "REFUQV9fIltePl0qPiguKj8pPC9zY3JpcHQ+JywgaHRtbCwgcmUuUwogICAgICAgICkKICAgICAg"
    "ICBpZiBtOgogICAgICAgICAgICBqID0gX2pzb24ubG9hZHMobS5ncm91cCgxKSkKICAgICAgICAg"
    "ICAgZW50ID0gKAogICAgICAgICAgICAgICAgai5nZXQoInByb3BzIiwge30pLmdldCgicGFnZVBy"
    "b3BzIiwge30pCiAgICAgICAgICAgICAgICAuZ2V0KCJzdGF0ZSIsIHt9KS5nZXQoImRhdGEiLCB7"
    "fSkuZ2V0KCJlbnRpdHkiLCB7fSkKICAgICAgICAgICAgKQogICAgICAgICAgICBuYW1lID0gX2Ns"
    "ZWFuKHN0cihlbnQuZ2V0KCJuYW1lIikgb3IgIiIpKQogICAgICAgICAgICBhcnRpc3RzID0gWwog"
    "ICAgICAgICAgICAgICAgc3RyKGEuZ2V0KCJuYW1lIikgb3IgIiIpCiAgICAgICAgICAgICAgICBm"
    "b3IgYSBpbiAoZW50LmdldCgiYXJ0aXN0cyIpIG9yIFtdKQogICAgICAgICAgICBdCiAgICAgICAg"
    "ICAgIGFydGlzdHMgPSBbYSBmb3IgYSBpbiBhcnRpc3RzIGlmIGEgYW5kIGEubG93ZXIoKSAhPSBu"
    "YW1lLmxvd2VyKCldCiAgICAgICAgICAgIGlmIG5hbWU6CiAgICAgICAgICAgICAgICByZXR1cm4g"
    "bmFtZSwgKGFydGlzdHNbMF0gaWYgYXJ0aXN0cyBlbHNlIE5vbmUpCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiBOb25lLCBOb25lCgoKZGVmIF9jbGVhbih0ZXh0"
    "KToKICAgIHQgPSB1bmVzY2FwZSh0ZXh0IG9yICIiKS5zdHJpcCgpCiAgICB0ID0gcmUuc3ViKHIi"
    "XHMrIiwgIiAiLCB0KQogICAgcmV0dXJuIHQuc3RyaXAoIiAt4oCT4oCUfCIpCgoKX1NIRUxMX1dP"
    "UkRTID0gKAogICAgInNwb3RpZnkiLCAid2ViIHBsYXllciIsICJsb2dpbiIsICJzaWduIHVwIiwg"
    "ImVycm9yIiwgImFjY2VzcyBkZW5pZWQiLAogICAgImp1c3QgYSBtb21lbnQiLCAibm90IGF2YWls"
    "YWJsZSIsICJwYWdlIG5vdCBmb3VuZCIsICJhdHRlbnRpb24gcmVxdWlyZWQiLAopCgoKZGVmIF9w"
    "bGF1c2libGUodCk6CiAgICAiIiJBIHBhZ2UgdGl0bGUgdGhhdCBjYW4gcGxhdXNpYmx5IGJlIGEg"
    "c29uZy9hcnRpc3QgbmFtZSDigJQgbm90IHRoZQogICAgSlMtc2hlbGwgLyBibG9jay1wYWdlIGdh"
    "cmJhZ2UgZGF0YWNlbnRlciBJUHMgc29tZXRpbWVzIGdldCBzZXJ2ZWQuIiIiCiAgICBpZiBub3Qg"
    "dCBvciBsZW4odCkgPCA0OgogICAgICAgIHJldHVybiBGYWxzZQogICAgbG93ID0gdC5sb3dlcigp"
    "CiAgICByZXR1cm4gbm90IGFueSh3IGluIGxvdyBmb3IgdyBpbiBfU0hFTExfV09SRFMpCgoKYXN5"
    "bmMgZGVmIF9yZXNvbHZlX3Nwb3RpZnkodXJsLCBtbWF4PTEwKToKICAgIGZpbmFsLCBodG1sID0g"
    "dXJsLCAiIgogICAgdHJ5OgogICAgICAgIGZpbmFsLCBodG1sID0gYXdhaXQgX2ZldGNoKHVybCkK"
    "ICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgX2ZwID0gZmluYWwuc3BsaXQo"
    "Ij8iKVswXQogICAgX3VwID0gdXJsLnNwbGl0KCI/IilbMF0KICAgIGlmICIvdHJhY2svIiBpbiBf"
    "ZnAgb3IgIi90cmFjay8iIGluIF91cDoKICAgICAgICAjIGV4YWN0IHNpbmdsZSBzb25nIOKAlCB0"
    "aGUgU1NSIGVtYmVkIHBhZ2UgZmlyc3QgKGl0IGNhcnJpZXMgdGhlCiAgICAgICAgIyBhcnRpc3Qg"
    "dG9vKSwgdGhlbiBvRW1iZWQgb24gYm90aCBpdHMgaG9zdHMsIHRoZW4gdGhlIHBhZ2UKICAgICAg"
    "ICAjIHRpdGxlLiBUaGUgSFRNTCBwYWdlIGlzIG9mdGVuIGEgSlMgc2hlbGwgb24gZGF0YWNlbnRl"
    "ciBJUHMsCiAgICAgICAgIyBzbyBub3RoaW5nIGJlbG93IHRydXN0cyBpdCBibGluZGx5LgogICAg"
    "ICAgIG5hbWUsIGFydGlzdCA9IGF3YWl0IF9lbWJlZF9lbnRpdHkodXJsKQogICAgICAgIGlmIG5v"
    "dCBuYW1lOgogICAgICAgICAgICBuYW1lID0gYXdhaXQgX29lbWJlZF90aXRsZShfdXApCiAgICAg"
    "ICAgaWYgbm90IG5hbWU6CiAgICAgICAgICAgIG5hbWUgPSBhd2FpdCBfb2VtYmVkX3RpdGxlX2Fs"
    "dChfdXApCiAgICAgICAgaWYgbm90IG5hbWU6CiAgICAgICAgICAgIG0gPSByZS5zZWFyY2gociI8"
    "dGl0bGU+KFtePF0rKTwvdGl0bGU+IiwgaHRtbCkKICAgICAgICAgICAgaWYgbToKICAgICAgICAg"
    "ICAgICAgIHQgPSBfY2xlYW4obS5ncm91cCgxKSkKICAgICAgICAgICAgICAgIHQgPSByZS5zdWIo"
    "ciJccypcfFxzKlNwb3RpZnlccyokIiwgIiIsIHQpLnN0cmlwKCkKICAgICAgICAgICAgICAgIG1t"
    "ID0gcmUubWF0Y2gociIoLis/KVxzKlst4oCT4oCUXVxzKnNvbmcgYW5kIGx5cmljcyBieVxzKigu"
    "KykkIiwgdCkKICAgICAgICAgICAgICAgIGlmIG1tOgogICAgICAgICAgICAgICAgICAgIHJldHVy"
    "biAoCiAgICAgICAgICAgICAgICAgICAgICAgIGYieXRzZWFyY2g1OntfY2xlYW4obW0uZ3JvdXAo"
    "MikpfSAtICIKICAgICAgICAgICAgICAgICAgICAgICAgZiJ7X2NsZWFuKG1tLmdyb3VwKDEpKX0g"
    "YXVkaW8iCiAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgaWYgX3BsYXVzaWJs"
    "ZSh0KToKICAgICAgICAgICAgICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaDU6e3R9IGF1ZGlvIgog"
    "ICAgICAgIGlmIG5hbWU6CiAgICAgICAgICAgIGlmIGFydGlzdDoKICAgICAgICAgICAgICAgIHJl"
    "dHVybiBmInl0c2VhcmNoNTp7YXJ0aXN0fSAtIHtuYW1lfSBhdWRpbyIKICAgICAgICAgICAgcmV0"
    "dXJuIGYieXRzZWFyY2g1OntuYW1lfSBhdWRpbyIKICAgICAgICByYWlzZSBNdXNpY1Vuc3VwcG9y"
    "dGVkKAogICAgICAgICAgICAiQ291bGRuJ3QgcmVhZCB0aGlzIFNwb3RpZnkgdHJhY2sgKG5ldHdv"
    "cmsgb3IgYmxvY2spIOKAlCAiCiAgICAgICAgICAgICJ0cnkgYWdhaW4gaW4gYSBtaW51dGUsIG9y"
    "IHNlbmQgYSBZb3VUdWJlIGxpbmsiCiAgICAgICAgKQogICAgaWYgYW55KHAgaW4gKF9mcCArIF91"
    "cCkgZm9yIHAgaW4gKCIvYXJ0aXN0LyIsICIvYWxidW0vIiwgIi9wbGF5bGlzdC8iKSk6CiAgICAg"
    "ICAgbmFtZSwgX2FyID0gYXdhaXQgX2VtYmVkX2VudGl0eSh1cmwpCiAgICAgICAgaWYgbm90IG5h"
    "bWU6CiAgICAgICAgICAgIG5hbWUgPSBhd2FpdCBfb2VtYmVkX3RpdGxlKGZpbmFsIG9yIHVybCkK"
    "ICAgICAgICBpZiBub3QgbmFtZToKICAgICAgICAgICAgbmFtZSA9IGF3YWl0IF9vZW1iZWRfdGl0"
    "bGVfYWx0KGZpbmFsIG9yIHVybCkKICAgICAgICBpZiBub3QgbmFtZToKICAgICAgICAgICAgbSA9"
    "IHJlLnNlYXJjaChyIjx0aXRsZT4oW148XSspPC90aXRsZT4iLCBodG1sKQogICAgICAgICAgICBp"
    "ZiBtOgogICAgICAgICAgICAgICAgdCA9IF9jbGVhbihtLmdyb3VwKDEpKQogICAgICAgICAgICAg"
    "ICAgdCA9IHJlLnN1YihyIlxzKlst4oCTfF1ccypTcG90aWZ5XHMqJCIsICIiLCB0KS5zdHJpcCgp"
    "CiAgICAgICAgICAgICAgICB0ID0gcmUuc3BsaXQociJccypbLeKAk11ccypTb25ncyIsIHQpWzBd"
    "LnN0cmlwKCkKICAgICAgICAgICAgICAgIG5hbWUgPSB0IGlmIF9wbGF1c2libGUodCkgZWxzZSBO"
    "b25lCiAgICAgICAgaWYgbmFtZToKICAgICAgICAgICAgaWYgIi9hcnRpc3QvIiBpbiAoX2ZwICsg"
    "X3VwKToKICAgICAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1lfSBzb25n"
    "cyBhdWRpbyIKICAgICAgICAgICAgaWYgIi9hbGJ1bS8iIGluIChfZnAgKyBfdXApOgogICAgICAg"
    "ICAgICAgICAgcmV0dXJuIGYieXRzZWFyY2h7bW1heH06e25hbWV9IGZ1bGwgYWxidW0gYXVkaW8i"
    "CiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1lfSBwbGF5bGlzdCBhdWRp"
    "byIKICAgICAgICByYWlzZSBNdXNpY1Vuc3VwcG9ydGVkKAogICAgICAgICAgICAiQ291bGRuJ3Qg"
    "cmVhZCB0aGlzIFNwb3RpZnkgcGFnZSDigJQgdHJ5IGFnYWluIGluIGEgbWludXRlLCAiCiAgICAg"
    "ICAgICAgICJvciBzZW5kIGEgc2luZ2xlIHRyYWNrIGxpbmsiCiAgICAgICAgKQogICAgcmFpc2Ug"
    "TXVzaWNVbnN1cHBvcnRlZCgKICAgICAgICAiVGhpcyBTcG90aWZ5IGxpbmsgaXNuJ3QgYSB0cmFj"
    "aywgYXJ0aXN0LCBhbGJ1bSBvciBwbGF5bGlzdCIKICAgICkKCgojIG5ldmVyIGFjY2VwdCB0aGVz"
    "ZSBhcyBzb25nIHRpdGxlIG9yIGFydGlzdCAoc2l0ZSBjaHJvbWUgLyBuYXYgaXRlbXMpCl9TQUFW"
    "Tl9TVE9QID0gewogICAgImppb3NhYXZuIiwgInNhYXZuIiwgInNvbmciLCAic29uZ3MiLCAiaG9t"
    "ZSIsICJtdXNpYyIsICJhbGJ1bSIsCiAgICAiYXJ0aXN0IiwgInBsYXlsaXN0IiwgImRvd25sb2Fk"
    "IiwKfQoKCiMgbmV2ZXIgYWNjZXB0IHRoZXNlIGFzIHNvbmcgdGl0bGUgb3IgYXJ0aXN0IChzaXRl"
    "IGNocm9tZSAvIG5hdiBpdGVtcykKX1NBQVZOX1NUT1AgPSB7CiAgICAiamlvc2Fhdm4iLCAic2Fh"
    "dm4iLCAic29uZyIsICJzb25ncyIsICJob21lIiwgIm11c2ljIiwgImFsYnVtIiwKICAgICJhcnRp"
    "c3QiLCAicGxheWxpc3QiLCAiZG93bmxvYWQiLAp9CgoKIyBuZXZlciBhY2NlcHQgdGhlc2UgYXMg"
    "c29uZyB0aXRsZSBvciBhcnRpc3QgKHNpdGUgY2hyb21lIC8gbmF2IGl0ZW1zKQpfU0FBVk5fU1RP"
    "UCA9IHsKICAgICJqaW9zYWF2biIsICJzYWF2biIsICJzb25nIiwgInNvbmdzIiwgImhvbWUiLCAi"
    "bXVzaWMiLCAiYWxidW0iLAogICAgImFydGlzdCIsICJwbGF5bGlzdCIsICJkb3dubG9hZCIsCn0K"
    "Cgphc3luYyBkZWYgX3Jlc29sdmVfamlvc2Fhdm4odXJsLCBtbWF4PTEwKToKICAgIGZpbmFsLCBo"
    "dG1sID0gdXJsLCAiIgogICAgdHJ5OgogICAgICAgIGZpbmFsLCBodG1sID0gYXdhaXQgX2ZldGNo"
    "KHVybCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgX2ZwID0gZmluYWwu"
    "c3BsaXQoIj8iKVswXQogICAgX3VwID0gdXJsLnNwbGl0KCI/IilbMF0KICAgIGlmICIvc29uZy8i"
    "IGluIF9mcCBvciAiL3NvbmcvIiBpbiBfdXA6CiAgICAgICAgdGl0bGUgPSBhcnRpc3QgPSBOb25l"
    "CiAgICAgICAgIyBKaW9TYWF2bidzIG93biB3ZWIgQVBJIOKAlCBzdGFibGUgc3RydWN0dXJlZCBk"
    "YXRhIGZvciB0aGUgdG9rZW4KICAgICAgICAjICh0aGUgSFRNTCBwYWdlIHZhcmlhbnQgdGhlIGZl"
    "dGNoZXIgZ2V0cyBpcyBpbmNvbnNpc3RlbnQpCiAgICAgICAgbSA9IHJlLnNlYXJjaChyIi9zb25n"
    "L1thLXowLTktXSsvKFtBLVphLXowLTlfLFwtXSspIiwgX3VwIG9yIF9mcCkKICAgICAgICBpZiBt"
    "OgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBfYXBpID0gKAogICAgICAgICAgICAg"
    "ICAgICAgICJodHRwczovL3d3dy5qaW9zYWF2bi5jb20vYXBpLnBocD9fX2NhbGw9d2ViYXBpLmdl"
    "dCIKICAgICAgICAgICAgICAgICAgICBmIiZ0b2tlbj17bS5ncm91cCgxKX0mdHlwZT1zb25nJmN0"
    "eD13ZWI2ZG90MCIKICAgICAgICAgICAgICAgICAgICAiJmFwaV92ZXJzaW9uPTQmX2Zvcm1hdD1q"
    "c29uIgogICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgXywgX2ogPSBhd2FpdCBfZmV0"
    "Y2goX2FwaSkKICAgICAgICAgICAgICAgIG1tID0gcmUuc2VhcmNoKHInInN1YnRpdGxlIlxzKjpc"
    "cyoiKFteIl0rKSInLCBfaikKICAgICAgICAgICAgICAgIGlmIG1tOgogICAgICAgICAgICAgICAg"
    "ICAgIF9wYXJ0cyA9IFsKICAgICAgICAgICAgICAgICAgICAgICAgcC5zdHJpcCgpCiAgICAgICAg"
    "ICAgICAgICAgICAgICAgIGZvciBwIGluIF9jbGVhbihtbS5ncm91cCgxKSkuc3BsaXQoIiAtICIp"
    "CiAgICAgICAgICAgICAgICAgICAgICAgIGlmIHAuc3RyaXAoKQogICAgICAgICAgICAgICAgICAg"
    "IF0KICAgICAgICAgICAgICAgICAgICBpZiBsZW4oX3BhcnRzKSA+PSAyOgogICAgICAgICAgICAg"
    "ICAgICAgICAgICBhcnRpc3QgPSBfcGFydHNbMF0KICAgICAgICAgICAgICAgICAgICAgICAgdGl0"
    "bGUgPSAiIC0gIi5qb2luKF9wYXJ0c1sxOl0pCiAgICAgICAgICAgICAgICAgICAgZWxpZiBfcGFy"
    "dHM6CiAgICAgICAgICAgICAgICAgICAgICAgIHRpdGxlID0gX3BhcnRzWzBdCiAgICAgICAgICAg"
    "ICAgICBpZiBub3QgYXJ0aXN0OgogICAgICAgICAgICAgICAgICAgIG1tID0gcmUuc2VhcmNoKAog"
    "ICAgICAgICAgICAgICAgICAgICAgICByJyJwcmltYXJ5X2FydGlzdHMiOlxbXHtbXn1dKj8ibmFt"
    "ZSI6IihbXiJdKykiJywgX2oKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAg"
    "ICAgaWYgbW06CiAgICAgICAgICAgICAgICAgICAgICAgIGFydGlzdCA9IF9jbGVhbihtbS5ncm91"
    "cCgxKSkKICAgICAgICAgICAgICAgIGlmIG5vdCB0aXRsZToKICAgICAgICAgICAgICAgICAgICBt"
    "bSA9IHJlLnNlYXJjaChyJyJ0aXRsZSJccyo6XHMqIihbXiJdKykiJywgX2opCiAgICAgICAgICAg"
    "ICAgICAgICAgaWYgbW0gYW5kIF9jbGVhbihtbS5ncm91cCgxKSkubG93ZXIoKSBub3QgaW4gX1NB"
    "QVZOX1NUT1A6CiAgICAgICAgICAgICAgICAgICAgICAgIHRpdGxlID0gX2NsZWFuKG1tLmdyb3Vw"
    "KDEpKQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgcGFzcwog"
    "ICAgICAgICMgb2c6dGl0bGUgLyA8dGl0bGU+IEZJUlNUIOKAlCBKaW9TYWF2bidzIHN0YWJsZSBw"
    "YXR0ZXJuCiAgICAgICAgIyAiU29uZyAtIEFydGlzdCAtIERvd25sb2FkIG9yIExpc3RlbiBGcmVl"
    "IC0gSmlvU2Fhdm4iIChvciB0aGUgb2xkZXIKICAgICAgICAjICJTb25nIC0gU29uZyBEb3dubG9h"
    "ZCBmcm9tIEFsYnVtIEAgSmlvU2Fhdm4iKQogICAgICAgIGZvciBwYXQgaW4gKAogICAgICAgICAg"
    "ICByJ3Byb3BlcnR5PSJvZzp0aXRsZSJccytjb250ZW50PSIoW14iXSspIicsCiAgICAgICAgICAg"
    "IHIiPHRpdGxlPihbXjxdKyk8L3RpdGxlPiIsCiAgICAgICAgKToKICAgICAgICAgICAgbSA9IHJl"
    "LnNlYXJjaChwYXQsIGh0bWwpCiAgICAgICAgICAgIGlmIG5vdCBtOgogICAgICAgICAgICAgICAg"
    "Y29udGludWUKICAgICAgICAgICAgdCA9IF9jbGVhbihtLmdyb3VwKDEpKQogICAgICAgICAgICB0"
    "ID0gcmUuc3BsaXQociJccypbLeKAk+KAlF1ccypTb25nIERvd25sb2FkCCIsIHQpWzBdCiAgICAg"
    "ICAgICAgIHNlZyA9IHJlLnNwbGl0KAogICAgICAgICAgICAgICAgciJccypbLeKAk+KAlF1ccypE"
    "b3dubG9hZCBvciBMaXN0ZW4gRnJlZQgiLCB0CiAgICAgICAgICAgIClbMF0KICAgICAgICAgICAg"
    "c2VnID0gcmUuc3BsaXQociJccypAXHMqSmlvU2Fhdm4IIiwgc2VnKVswXS5zdHJpcCgpCiAgICAg"
    "ICAgICAgIHNlZyA9IHJlLnN1YihyIlxzKlx8XHMqSmlvU2Fhdm5ccyokIiwgIiIsIHNlZykuc3Ry"
    "aXAoKQogICAgICAgICAgICBpZiBub3Qgc2VnOgogICAgICAgICAgICAgICAgY29udGludWUKICAg"
    "ICAgICAgICAgcGFydHMgPSBbCiAgICAgICAgICAgICAgICBwLnN0cmlwKCkKICAgICAgICAgICAg"
    "ICAgIGZvciBwIGluIHJlLnNwbGl0KHIiXHMqWy3igJPigJRdXHMqIiwgc2VnKQogICAgICAgICAg"
    "ICAgICAgaWYgcC5zdHJpcCgpCiAgICAgICAgICAgIF0KICAgICAgICAgICAgaWYgbGVuKHBhcnRz"
    "KSA+PSAyOgogICAgICAgICAgICAgICAgIyAiSXNocSAtIEppb1NhYXZuIiAoc2hvcnQgb2c6dGl0"
    "bGUgdmFyaWFudCkgY2FycmllcyB0aGUKICAgICAgICAgICAgICAgICMgc2l0ZSBuYW1lIGFzIHRo"
    "ZSBsYXN0IHNlZ21lbnQg4oCUIG5ldmVyIGFuIGFydGlzdAogICAgICAgICAgICAgICAgX2NhbmRf"
    "dCwgX2NhbmRfYSA9IHBhcnRzWzBdLCBwYXJ0c1stMV0KICAgICAgICAgICAgICAgIGlmIF9jYW5k"
    "X3QubG93ZXIoKSBub3QgaW4gX1NBQVZOX1NUT1A6CiAgICAgICAgICAgICAgICAgICAgdGl0bGUg"
    "PSBfY2FuZF90CiAgICAgICAgICAgICAgICBpZiBfY2FuZF9hLmxvd2VyKCkgbm90IGluIF9TQUFW"
    "Tl9TVE9QOgogICAgICAgICAgICAgICAgICAgIGFydGlzdCA9IF9jYW5kX2EKICAgICAgICAgICAg"
    "ICAgIGlmIHRpdGxlIG9yIGFydGlzdDoKICAgICAgICAgICAgICAgICAgICBicmVhawogICAgICAg"
    "ICAgICBlbGlmIG5vdCB0aXRsZToKICAgICAgICAgICAgICAgIHRpdGxlID0gc2VnCiAgICAgICAg"
    "IyBtZXRhIGRlc2NyaXB0aW9uOiAiSXNocSBpcyBhIFB1bmphYmkgbGFuZ3VhZ2Ugc29uZyBhbmQg"
    "aXMgc3VuZyBieQogICAgICAgICMgQW1yaW5kZXIgR2lsbC4iIOKAlCBwcmVzZW50IGluIGV2ZXJ5"
    "IHBhZ2UgdmFyaWFudCBmb3IgU0VPCiAgICAgICAgaWYgbm90ICh0aXRsZSBhbmQgYXJ0aXN0KToK"
    "ICAgICAgICAgICAgbSA9IHJlLnNlYXJjaCgKICAgICAgICAgICAgICAgIHInPG1ldGFccysoPzpu"
    "YW1lPSJkZXNjcmlwdGlvbiJ8cHJvcGVydHk9Im9nOmRlc2NyaXB0aW9uIiknCiAgICAgICAgICAg"
    "ICAgICByJ1xzK2NvbnRlbnQ9IihbXiJdKykiJywKICAgICAgICAgICAgICAgIGh0bWwsCiAgICAg"
    "ICAgICAgICkgb3IgcmUuc2VhcmNoKAogICAgICAgICAgICAgICAgcidwcm9wZXJ0eT0ib2c6ZGVz"
    "Y3JpcHRpb24iXHMrY29udGVudD0iKFteIl0rKSInLCBodG1sCiAgICAgICAgICAgICkKICAgICAg"
    "ICAgICAgaWYgbToKICAgICAgICAgICAgICAgIGQgPSBfY2xlYW4obS5ncm91cCgxKSkKICAgICAg"
    "ICAgICAgICAgIG1tID0gcmUuc2VhcmNoKAogICAgICAgICAgICAgICAgICAgIHIiKFteLl17Miw4"
    "MH0/KVxzK2lzXHMrKD86YXxhbilccytbXHdcc117MCw0MH1zb25nIgogICAgICAgICAgICAgICAg"
    "ICAgIHIiKD86XHMrYW5kfFxzK3doaWNoKVxzK2lzXHMrc3VuZ1xzK2J5XHMrKFteLl0rKSIsCiAg"
    "ICAgICAgICAgICAgICAgICAgZCwgcmUuSSwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAg"
    "ICAgIGlmIG1tOgogICAgICAgICAgICAgICAgICAgIHRpdGxlID0gdGl0bGUgb3IgX2NsZWFuKG1t"
    "Lmdyb3VwKDEpKQogICAgICAgICAgICAgICAgICAgIGFydGlzdCA9IGFydGlzdCBvciBfY2xlYW4o"
    "bW0uZ3JvdXAoMikpCiAgICAgICAgIyBzdHJ1Y3R1cmVkIEpTT04gZmFsbGJhY2sg4oCUIHNwZWNp"
    "ZmljIGtleXMgb25seSAoYSBnZW5lcmljICJ0aXRsZSIKICAgICAgICAjIG1hdGNoZXMgbmF2L21l"
    "bnUgaXRlbXMgbGlrZSAiSG9tZSIgaW4gdGhlIHBhZ2UgSlNPTikKICAgICAgICBpZiBub3QgKHRp"
    "dGxlIGFuZCBhcnRpc3QpOgogICAgICAgICAgICBtID0gcmUuc2VhcmNoKHInInNvbmdfdGl0bGUi"
    "XHMqOlxzKiIoW14iXSspIicsIGh0bWwpCiAgICAgICAgICAgIGlmIG06CiAgICAgICAgICAgICAg"
    "ICB0aXRsZSA9IHRpdGxlIG9yIF9jbGVhbihtLmdyb3VwKDEpKQogICAgICAgICAgICBtID0gKAog"
    "ICAgICAgICAgICAgICAgcmUuc2VhcmNoKHInInByaW1hcnlfYXJ0aXN0cyJccyo6XHMqIihbXiJd"
    "KikiJywgaHRtbCkKICAgICAgICAgICAgICAgIG9yIHJlLnNlYXJjaChyJyJzaW5nZXJzIlxzKjpc"
    "cyoiKFteIl0qKSInLCBodG1sKQogICAgICAgICAgICAgICAgb3IgcmUuc2VhcmNoKHInImFydGlz"
    "dCJccyo6XHMqIihbXiJdKikiJywgaHRtbCkKICAgICAgICAgICAgKQogICAgICAgICAgICBpZiBt"
    "IGFuZCBfY2xlYW4obS5ncm91cCgxKSkubG93ZXIoKSBub3QgaW4gX1NBQVZOX1NUT1A6CiAgICAg"
    "ICAgICAgICAgICBhcnRpc3QgPSBhcnRpc3Qgb3IgX2NsZWFuKG0uZ3JvdXAoMSkpCiAgICAgICAg"
    "IyBzdHJpcCBwYXJlbnRoZXRpY2FsIGp1bmsgZnJvbSB0aGUgc29uZyBuYW1lICgiSXNocSAoRnVs"
    "bCBTb25nKSIgLT4gIklzaHEiKQogICAgICAgIGlmIHRpdGxlOgogICAgICAgICAgICB0aXRsZSA9"
    "IHJlLnN1YigKICAgICAgICAgICAgICAgIHIiXHMqXChbXildKig/OmZ1bGxccypzb25nfG9mZmlj"
    "aWFsfGx5cmljYWx8YXVkaW98dmlkZW98aGQpW14pXSpcKVxzKiQiLAogICAgICAgICAgICAgICAg"
    "IiIsIHRpdGxlLCBmbGFncz1yZS5JLAogICAgICAgICAgICApLnN0cmlwKCkKICAgICAgICAjIHNs"
    "dWcgZmFsbGJhY2s6IC9zb25nL2lzaHEvSUQKICAgICAgICBpZiBub3QgdGl0bGU6CiAgICAgICAg"
    "ICAgIG0gPSByZS5zZWFyY2gociIvc29uZy8oW2EtejAtOS1dKykvIiwgX3VwIG9yIF9mcCkKICAg"
    "ICAgICAgICAgaWYgbSBhbmQgbS5ncm91cCgxKSBub3QgaW4gKCJzb25nIiwpOgogICAgICAgICAg"
    "ICAgICAgdGl0bGUgPSBtLmdyb3VwKDEpLnJlcGxhY2UoIi0iLCAiICIpLnN0cmlwKCkudGl0bGUo"
    "KQogICAgICAgIGlmIHRpdGxlIGFuZCBhcnRpc3Q6CiAgICAgICAgICAgIHJldHVybiBmInl0c2Vh"
    "cmNoNTp7YXJ0aXN0fSAtIHt0aXRsZX0gYXVkaW8iCiAgICAgICAgaWYgdGl0bGU6CiAgICAgICAg"
    "ICAgIHJldHVybiBmInl0c2VhcmNoNTp7dGl0bGV9IGF1ZGlvIgogICAgICAgIHJldHVybiBOb25l"
    "CiAgICAjIGFydGlzdCAvIGFsYnVtIC8gZmVhdHVyZWQgcGFnZXMKICAgIG5hbWUgPSBOb25lCiAg"
    "ICBtID0gcmUuc2VhcmNoKHIncHJvcGVydHk9Im9nOnRpdGxlIlxzK2NvbnRlbnQ9IihbXiJdKyki"
    "JywgaHRtbCkKICAgIGlmIG06CiAgICAgICAgbmFtZSA9IF9jbGVhbihtLmdyb3VwKDEpKQogICAg"
    "aWYgbm90IG5hbWU6CiAgICAgICAgbSA9IHJlLnNlYXJjaChyIjx0aXRsZT4oW148XSspPC90aXRs"
    "ZT4iLCBodG1sKQogICAgICAgIGlmIG06CiAgICAgICAgICAgIHQgPSBfY2xlYW4obS5ncm91cCgx"
    "KSkucmVwbGFjZSgiQCBKaW9TYWF2biIsICIiKQogICAgICAgICAgICB0ID0gcmUuc3BsaXQociJc"
    "cypbLeKAk+KAlF1ccyooPzpTb25nc3xBbGJ1bXN8QWxidW0pIiwgdClbMF0uc3RyaXAoKQogICAg"
    "ICAgICAgICBuYW1lID0gdCBvciBOb25lCiAgICBpZiBub3QgbmFtZToKICAgICAgICBuYW1lID0g"
    "X3NsdWdfcXVlcnkoZmluYWwpCiAgICBpZiBuYW1lOgogICAgICAgIGlmICIvYXJ0aXN0LyIgaW4g"
    "KF9mcCArIF91cCk6CiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1lfSBz"
    "b25ncyBhdWRpbyIKICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaHttbWF4fTp7bmFtZX0gYXVkaW8i"
    "CiAgICByYWlzZSBNdXNpY1Vuc3VwcG9ydGVkKCJDb3VsZG4ndCByZWFkIHRoaXMgSmlvU2Fhdm4g"
    "bGluayIpCgoKCgoKCmFzeW5jIGRlZiBfcmVzb2x2ZV9hcHBsZSh1cmwsIG1tYXg9MTApOgogICAg"
    "ZmluYWwsIGh0bWwgPSB1cmwsICIiCiAgICB0cnk6CiAgICAgICAgZmluYWwsIGh0bWwgPSBhd2Fp"
    "dCBfZmV0Y2godXJsKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBfZnAg"
    "PSBmaW5hbC5zcGxpdCgiPyIpWzBdCiAgICBfdXAgPSB1cmwuc3BsaXQoIj8iKVswXQogICAgaWYg"
    "Ii9zb25nLyIgaW4gX2ZwIG9yICgiL2FsYnVtLyIgaW4gX2ZwIGFuZCAiP2k9IiBpbiBmaW5hbCk6"
    "CiAgICAgICAgIyBpVHVuZXMgbG9va3VwIEFQSSDigJQgc3RhYmxlIHN0cnVjdHVyZWQgZGF0YSBm"
    "b3IgdGhlIG51bWVyaWMgaWQKICAgICAgICAjICh0aGUgSFRNTCBvZzp0aXRsZSB2YXJpYW50IHNl"
    "cnZlZCB0byB0aGUgZmV0Y2hlciBpcyBpbmNvbnNpc3RlbnQpCiAgICAgICAgbSA9ICgKICAgICAg"
    "ICAgICAgcmUuc2VhcmNoKHIiL3NvbmcvW14vXSsvKFxkKykiLCBfZnAgb3IgX3VwKQogICAgICAg"
    "ICAgICBvciByZS5zZWFyY2gociJbPyZdaT0oXGQrKSIsIGZpbmFsKQogICAgICAgICkKICAgICAg"
    "ICBpZiBtOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBfYXBpID0gKAogICAgICAg"
    "ICAgICAgICAgICAgICJodHRwczovL2l0dW5lcy5hcHBsZS5jb20vbG9va3VwP2lkPSIKICAgICAg"
    "ICAgICAgICAgICAgICBmInttLmdyb3VwKDEpfSZlbnRpdHk9c29uZyIKICAgICAgICAgICAgICAg"
    "ICkKICAgICAgICAgICAgICAgIF8sIF9qID0gYXdhaXQgX2ZldGNoKF9hcGkpCiAgICAgICAgICAg"
    "ICAgICBtbSA9IHJlLnNlYXJjaChyJyJhcnRpc3ROYW1lIlxzKjpccyoiKFteIl0rKSInLCBfaikK"
    "ICAgICAgICAgICAgICAgIF9hciA9IF9jbGVhbihtbS5ncm91cCgxKSkgaWYgbW0gZWxzZSBOb25l"
    "CiAgICAgICAgICAgICAgICBtbSA9IHJlLnNlYXJjaChyJyJ0cmFja05hbWUiXHMqOlxzKiIoW14i"
    "XSspIicsIF9qKQogICAgICAgICAgICAgICAgX3RuID0gX2NsZWFuKG1tLmdyb3VwKDEpKSBpZiBt"
    "bSBlbHNlIE5vbmUKICAgICAgICAgICAgICAgIGlmIF90biBhbmQgX2FyOgogICAgICAgICAgICAg"
    "ICAgICAgIHJldHVybiBmInl0c2VhcmNoNTp7X2FyfSAtIHtfdG59IGF1ZGlvIgogICAgICAgICAg"
    "ICAgICAgaWYgX3RuOgogICAgICAgICAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoNTp7X3Ru"
    "fSBhdWRpbyIKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBh"
    "c3MKICAgICAgICBtID0gcmUuc2VhcmNoKHIncHJvcGVydHk9Im9nOnRpdGxlIlxzK2NvbnRlbnQ9"
    "IihbXiJdKykiJywgaHRtbCkKICAgICAgICBpZiBtIGFuZCBfY2xlYW4obS5ncm91cCgxKSk6CiAg"
    "ICAgICAgICAgIHQgPSBfY2xlYW4obS5ncm91cCgxKSkKICAgICAgICAgICAgdCA9IHJlLnN1Yihy"
    "IlxzKlst4oCT4oCUXVxzKkFwcGxlIE11c2ljXHMqJCIsICIiLCB0KS5zdHJpcCgpCiAgICAgICAg"
    "ICAgIGlmIHQ6CiAgICAgICAgICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaDU6e3R9IGF1ZGlvIgog"
    "ICAgICAgIG0gPSByZS5zZWFyY2gociI8dGl0bGU+KFtePF0rKTwvdGl0bGU+IiwgaHRtbCkKICAg"
    "ICAgICBpZiBtOgogICAgICAgICAgICB0ID0gX2NsZWFuKG0uZ3JvdXAoMSkpCiAgICAgICAgICAg"
    "IHQgPSB0LnJlcGxhY2UoIm9uIEFwcGxlIE11c2ljIiwgIiIpLnN0cmlwKCkKICAgICAgICAgICAg"
    "dCA9IHJlLnNwbGl0KHIiXHMqWy3igJPigJRdXHMqKD86U2luZ2xlfFNvbmd8RVApCCIsIHQpWzBd"
    "LnN0cmlwKCkKICAgICAgICAgICAgaWYgdDoKICAgICAgICAgICAgICAgIHJldHVybiBmInl0c2Vh"
    "cmNoNTp7dH0gYXVkaW8iCiAgICAgICAgbmFtZSA9IF9zbHVnX3F1ZXJ5KGZpbmFsKQogICAgICAg"
    "IGlmIG5hbWU6CiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoNTp7bmFtZX0gYXVkaW8iCiAg"
    "ICAgICAgcmV0dXJuIE5vbmUKICAgICMgYXJ0aXN0IC8gYWxidW0gcGFnZXMKICAgIG5hbWUgPSBO"
    "b25lCiAgICBtID0gcmUuc2VhcmNoKHIncHJvcGVydHk9Im9nOnRpdGxlIlxzK2NvbnRlbnQ9Iihb"
    "XiJdKykiJywgaHRtbCkKICAgIGlmIG06CiAgICAgICAgbmFtZSA9IF9jbGVhbihtLmdyb3VwKDEp"
    "KS5yZXBsYWNlKCJvbiBBcHBsZSBNdXNpYyIsICIiKS5zdHJpcCgpCiAgICBpZiBub3QgbmFtZToK"
    "ICAgICAgICBtID0gcmUuc2VhcmNoKHIiPHRpdGxlPihbXjxdKyk8L3RpdGxlPiIsIGh0bWwpCiAg"
    "ICAgICAgaWYgbToKICAgICAgICAgICAgdCA9IF9jbGVhbihtLmdyb3VwKDEpKS5yZXBsYWNlKCJv"
    "biBBcHBsZSBNdXNpYyIsICIiKQogICAgICAgICAgICB0ID0gcmUuc3BsaXQociJccypbLeKAk11c"
    "cyooPzpTb25nc3xBbGJ1bXxBcnRpc3QpIiwgdClbMF0uc3RyaXAoKQogICAgICAgICAgICBuYW1l"
    "ID0gdCBvciBOb25lCiAgICBpZiBub3QgbmFtZToKICAgICAgICBuYW1lID0gX3NsdWdfcXVlcnko"
    "ZmluYWwpCiAgICBpZiBuYW1lOgogICAgICAgIGlmICIvYXJ0aXN0LyIgaW4gKF9mcCArIF91cCk6"
    "CiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1lfSBzb25ncyBhdWRpbyIK"
    "ICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaHttbWF4fTp7bmFtZX0gYXVkaW8iCiAgICByYWlzZSBN"
    "dXNpY1Vuc3VwcG9ydGVkKCJDb3VsZG4ndCByZWFkIHRoaXMgQXBwbGUgTXVzaWMgbGluayIpCgoK"
    "CmRlZiBfc3BvdGlmeV9jcmVkcygpOgogICAgIiIiVGhlIG93bmVyJ3Mgb3duIFNwb3RpZnkgYXBw"
    "IGNyZWRlbnRpYWxzIChjb25maWcuZW52IOKGkiBlbnYpLiIiIgogICAgY2lkID0gKG9zLmVudmly"
    "b24uZ2V0KCJTUE9USUZZX0NMSUVOVF9JRCIpIG9yICIiKS5zdHJpcCgpCiAgICBjcyA9IChvcy5l"
    "bnZpcm9uLmdldCgiU1BPVElGWV9DTElFTlRfU0VDUkVUIikgb3IgIiIpLnN0cmlwKCkKICAgIGlm"
    "IGNpZCBhbmQgY3MgYW5kICJQTEFDRUhPTERFUiIgbm90IGluIGNpZCArIGNzOgogICAgICAgIHJl"
    "dHVybiBjaWQsIGNzCiAgICByZXR1cm4gTm9uZQoKCl9TUF9UT0tFTiA9IHsidCI6IDAuMCwgInYi"
    "OiBOb25lLCAia2luZCI6ICIifQoKCmRlZiBfYXBpX2dldCh1cmwsIHRva2VuLCB0aW1lb3V0PTE1"
    "LjAsIHJldHJpZXM9MSk6CiAgICAiIiJBIHBsYWluIEdFVCB3aXRoIGEgYmVhcmVyIHRva2VuIChy"
    "dW4gaW4gYSB0aHJlYWQpLgogICAgV1pGSVggcjEyICh2MTUuNzEpOiBvbmUgcmV0cnkgd2l0aCBi"
    "YWNrb2ZmIG9uIEhUVFAgNDI5IOKAlCB0aGUKICAgIGFub255bW91cyB3ZWItcGxheWVyIHRva2Vu"
    "IGlzIHF1b3RhLWxpbWl0ZWQgKFFVT1RBX0VYQ0VFREVEKSBhbmQKICAgIGEgc2luZ2xlIHJldHJ5"
    "IGFmdGVyIGEgcGF1c2UgdXN1YWxseSBjbGVhcnMgaXQuIiIiCiAgICBpbXBvcnQgdGltZSBhcyBf"
    "dAogICAgaW1wb3J0IHVybGxpYi5lcnJvcgogICAgaW1wb3J0IHVybGxpYi5yZXF1ZXN0CgogICAg"
    "bGFzdCA9IE5vbmUKICAgIGZvciBhdHRlbXB0IGluIHJhbmdlKHJldHJpZXMgKyAxKToKICAgICAg"
    "ICByZXEgPSB1cmxsaWIucmVxdWVzdC5SZXF1ZXN0KAogICAgICAgICAgICB1cmwsCiAgICAgICAg"
    "ICAgIGhlYWRlcnM9ewogICAgICAgICAgICAgICAgIkF1dGhvcml6YXRpb24iOiBmIkJlYXJlciB7"
    "dG9rZW59IiwKICAgICAgICAgICAgICAgICJBY2NlcHQiOiAiYXBwbGljYXRpb24vanNvbiIsCiAg"
    "ICAgICAgICAgICAgICAiYXBwLXBsYXRmb3JtIjogIldlYlBsYXllciIsCiAgICAgICAgICAgICAg"
    "ICAiVXNlci1BZ2VudCI6ICgKICAgICAgICAgICAgICAgICAgICAiTW96aWxsYS81LjAgKFdpbmRv"
    "d3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgIgogICAgICAgICAgICAgICAgICAgICJBcHBsZVdlYktp"
    "dC81MzcuMzYiCiAgICAgICAgICAgICAgICApLAogICAgICAgICAgICB9LAogICAgICAgICkKICAg"
    "ICAgICB0cnk6CiAgICAgICAgICAgIHdpdGggdXJsbGliLnJlcXVlc3QudXJsb3BlbihyZXEsIHRp"
    "bWVvdXQ9dGltZW91dCkgYXMgcjoKICAgICAgICAgICAgICAgIHJldHVybiBqc29uLmxvYWRzKHIu"
    "cmVhZCgpLmRlY29kZSgidXRmLTgiLCAicmVwbGFjZSIpKQogICAgICAgIGV4Y2VwdCB1cmxsaWIu"
    "ZXJyb3IuSFRUUEVycm9yIGFzIGU6CiAgICAgICAgICAgIGxhc3QgPSBlCiAgICAgICAgICAgIGlm"
    "IGUuY29kZSA9PSA0MjkgYW5kIGF0dGVtcHQgPCByZXRyaWVzOgogICAgICAgICAgICAgICAgX3Qu"
    "c2xlZXAoMTAuMCAqIChhdHRlbXB0ICsgMSkpCiAgICAgICAgICAgICAgICBjb250aW51ZQogICAg"
    "ICAgICAgICByYWlzZQogICAgcmFpc2UgbGFzdAoKCmRlZiBfanNvbl9nZXQodXJsLCB0aW1lb3V0"
    "PTE1LjApOgogICAgIiIiV1pGSVggcjEzICh2MTUuNzIpOiBhIHBsYWluIEpTT04gR0VUIHdpdGgg"
    "YSBicm93c2VyIHVzZXIgYWdlbnQKICAgIChydW4gaW4gYSB0aHJlYWQpIOKAlCBmb3IgdGhlIEpp"
    "b1NhYXZuIGFuZCBEZWV6ZXIgcHVibGljIEFQSXMsIHdoaWNoCiAgICBuZWVkIG5vIGF1dGhlbnRp"
    "Y2F0aW9uIGF0IGFsbC4iIiIKICAgIGltcG9ydCB1cmxsaWIucmVxdWVzdAoKICAgIHJlcSA9IHVy"
    "bGxpYi5yZXF1ZXN0LlJlcXVlc3QoCiAgICAgICAgdXJsLAogICAgICAgIGhlYWRlcnM9ewogICAg"
    "ICAgICAgICAiQWNjZXB0IjogImFwcGxpY2F0aW9uL2pzb24iLAogICAgICAgICAgICAiVXNlci1B"
    "Z2VudCI6ICgKICAgICAgICAgICAgICAgICJNb3ppbGxhLzUuMCAoV2luZG93cyBOVCAxMC4wOyBX"
    "aW42NDsgeDY0KSAiCiAgICAgICAgICAgICAgICAiQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwg"
    "bGlrZSBHZWNrbykgIgogICAgICAgICAgICAgICAgIkNocm9tZS8xMjQuMCBTYWZhcmkvNTM3LjM2"
    "IgogICAgICAgICAgICApLAogICAgICAgIH0sCiAgICApCiAgICB3aXRoIHVybGxpYi5yZXF1ZXN0"
    "LnVybG9wZW4ocmVxLCB0aW1lb3V0PXRpbWVvdXQpIGFzIHI6CiAgICAgICAgcmV0dXJuIGpzb24u"
    "bG9hZHMoci5yZWFkKCkuZGVjb2RlKCJ1dGYtOCIsICJyZXBsYWNlIikpCgoKZGVmIF9hcGlfdG9r"
    "ZW4oY3JlZHMpOgogICAgIiIiQ2xpZW50LWNyZWRlbnRpYWxzIHRva2VuIGZvciB0aGUgb3duZXIn"
    "cyBvd24gU3BvdGlmeSBhcHAuIiIiCiAgICBpbXBvcnQgYmFzZTY0CiAgICBpbXBvcnQgdXJsbGli"
    "LnJlcXVlc3QKCiAgICBjaWQsIGNzID0gY3JlZHMKICAgIHJlcSA9IHVybGxpYi5yZXF1ZXN0LlJl"
    "cXVlc3QoCiAgICAgICAgImh0dHBzOi8vYWNjb3VudHMuc3BvdGlmeS5jb20vYXBpL3Rva2VuIiwK"
    "ICAgICAgICBkYXRhPSJncmFudF90eXBlPWNsaWVudF9jcmVkZW50aWFscyIuZW5jb2RlKCksCiAg"
    "ICAgICAgaGVhZGVycz17CiAgICAgICAgICAgICJBdXRob3JpemF0aW9uIjogIkJhc2ljICIKICAg"
    "ICAgICAgICAgKyBiYXNlNjQuYjY0ZW5jb2RlKGYie2NpZH06e2NzfSIuZW5jb2RlKCkpLmRlY29k"
    "ZSgpLAogICAgICAgICAgICAiQ29udGVudC1UeXBlIjogImFwcGxpY2F0aW9uL3gtd3d3LWZvcm0t"
    "dXJsZW5jb2RlZCIsCiAgICAgICAgfSwKICAgICkKICAgIHdpdGggdXJsbGliLnJlcXVlc3QudXJs"
    "b3BlbihyZXEsIHRpbWVvdXQ9MTUpIGFzIHI6CiAgICAgICAgcmV0dXJuIGpzb24ubG9hZHMoci5y"
    "ZWFkKCkuZGVjb2RlKCJ1dGYtOCIsICJyZXBsYWNlIikpWyJhY2Nlc3NfdG9rZW4iXQoKCl9GRUFU"
    "X1BBVCA9IHJlLmNvbXBpbGUoCiAgICByIlxzKlsoXFtdKD86ZmVhdFwuP3xmdFwuP3xmZWF0dXJp"
    "bmd8d2l0aClcYlteKVxdXSpbKVxdXSIsIHJlLkkKKQpfTUVUQV9QQVQgPSByZS5jb21waWxlKAog"
    "ICAgciJccypbKFxbXSg/Om9mZmljaWFsfGx5cmljfGx5cmljYWx8YXVkaW98dmlkZW98ZnVsbHx2"
    "aXN1YWwiCiAgICByInxhY291c3RpY3xyZW1peHxsaXZlfGluc3RydW1lbnRhbHxzcGVkfHNsb3dl"
    "ZHxyZXZlcmJ8a2FyYW9rZSkiCiAgICByIlteKVxdXSpbKVxdXSIsCiAgICByZS5JLAopCgoKX1ZB"
    "UklBTlRfV09SRFMgPSBmcm96ZW5zZXQoCiAgICAib2ZmaWNpYWwgdmlkZW8gYXVkaW8gbHlyaWNz"
    "IGx5cmljIGx5cmljYWwgOGQgMTZkIGhkIGhxIDRrIGJhc3MgIgogICAgImJvb3N0ZWQgc2xvd2Vk"
    "IHJldmVyYiBzcGVkIHNwZWVkdXAgdmlzdWFsaXplciB2aXN1YWwga2FyYW9rZSAiCiAgICAiaW5z"
    "dHJ1bWVudGFsIHJlbWl4IGxpdmUgY292ZXIgdmVyc2lvbiB0cmlidXRlIGZ1bGwiCiAgICAuc3Bs"
    "aXQoKQopCgoKZGVmIF9ub3JtX2tleShuYW1lKToKICAgICIiIldaRklYIHIxMiAodjE1LjcxKTog"
    "bm9ybWFsaXplZCBkZWR1cGUga2V5IOKAlCAnVGVtcG9yYXJ5IFB5YXInLAogICAgJ1RlbXBvcmFy"
    "eSBQeWFyIChmZWF0LiBYKScgYW5kICdUZW1wb3JhcnkgUHlhciBbT2ZmaWNpYWwgVmlkZW9dJwog"
    "ICAgbXVzdCBjb3VudCBhcyBPTkUgc29uZywgbm90IHRocmVlLiIiIgogICAgcyA9IF9GRUFUX1BB"
    "VC5zdWIoIiAiLCBuYW1lIG9yICIiKQogICAgcyA9IF9NRVRBX1BBVC5zdWIoIiAiLCBzKQogICAg"
    "cyA9IHJlLnN1YihyIig/OnNwZWR8c3BlZWQpWyAtXT91cCIsICIgIiwgcywgZmxhZ3M9cmUuSSkK"
    "ICAgIHRva3MgPSByZS5zdWIociJbXjAtOWEtel0rIiwgIiAiLCBzLmxvd2VyKCkpLnNwbGl0KCkK"
    "ICAgICMgV1pGSVhfUjE4OiB2YXJpYW50IG1hcmtlcnMgYWxzbyBhcHBlYXIgdW5icmFja2V0ZWQs"
    "IGFmdGVyIGEKICAgICMgZGFzaCAoIlNvbmcgLSA4RCBBdWRpbyIpIG9yIGJhcmUgKCJTb25nIE9m"
    "ZmljaWFsIFZpZGVvIikg4oCUCiAgICAjIGRyb3AgZXZlcnkgdmFyaWFudCB3b3JkIHNvIHRoZXkg"
    "YWxsIGZvbGQgaW50byB0aGUgYmFzZSBzb25nCiAgICB0b2tzID0gW3QgZm9yIHQgaW4gdG9rcyBp"
    "ZiB0IG5vdCBpbiBfVkFSSUFOVF9XT1JEU10KICAgIHJldHVybiAiICIuam9pbih0b2tzKQoKCl9D"
    "QVRBTE9HX05PVEUgPSAiIgoKCmFzeW5jIGRlZiBfc3BvdGlmeV90b2tlbihhbm9uX2Zyb209Tm9u"
    "ZSk6CiAgICAiIiJBIGNhdGFsb2ctQVBJIHRva2VuOiB0aGUgb3duZXIncyBhcHAgY3JlZGVudGlh"
    "bHMgd2hlbiBjb25maWd1cmVkCiAgICAoYmVzdCksIGVsc2UgdGhlIGFub255bW91cyB3ZWItcGxh"
    "eWVyIHRva2VuIChzY3JhcGVkIGZyb20gYW4gZW1iZWQKICAgIHBhZ2UncyBfX05FWFRfREFUQV9f"
    "KS4gQ2FjaGVkIH41MCBtaW51dGVzLiIiIgogICAgaW1wb3J0IHRpbWUgYXMgX3RpbWUKCiAgICBu"
    "b3cgPSBfdGltZS50aW1lKCkKICAgIGlmIF9TUF9UT0tFTlsidiJdIGFuZCBub3cgLSBfU1BfVE9L"
    "RU5bInQiXSA8IDMwMDA6CiAgICAgICAgcmV0dXJuIF9TUF9UT0tFTlsidiJdCiAgICB0b2sgPSBO"
    "b25lCiAgICBjcmVkcyA9IF9zcG90aWZ5X2NyZWRzKCkKICAgIGlmIGNyZWRzOgogICAgICAgIHRy"
    "eToKICAgICAgICAgICAgdG9rID0gYXdhaXQgYXN5bmNpby50b190aHJlYWQoX2FwaV90b2tlbiwg"
    "Y3JlZHMpCiAgICAgICAgICAgIGlmIHRvazoKICAgICAgICAgICAgICAgIF9TUF9UT0tFTi51cGRh"
    "dGUodD1ub3csIHY9dG9rLCBraW5kPSJhcHAiKQogICAgICAgICAgICAgICAgX2xvZygiV1pGSVgg"
    "bXVzaWM6IGNhdGFsb2cgdG9rZW4gZnJvbSBhcHAgY3JlZGVudGlhbHMiKQogICAgICAgICAgICAg"
    "ICAgcmV0dXJuIHRvawogICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAgICAg"
    "X2xvZyhmIldaRklYIG11c2ljOiBhcHAgdG9rZW4gZmFpbGVkOiB7ZX0iKQogICAgaWYgYW5vbl9m"
    "cm9tIGFuZCBpc2luc3RhbmNlKGFub25fZnJvbSwgZGljdCk6CiAgICAgICAgdHJ5OgogICAgICAg"
    "ICAgICB0b2sgPSAoCiAgICAgICAgICAgICAgICBhbm9uX2Zyb20uZ2V0KCJwcm9wcyIsIHt9KQog"
    "ICAgICAgICAgICAgICAgLmdldCgicGFnZVByb3BzIiwge30pCiAgICAgICAgICAgICAgICAuZ2V0"
    "KCJzdGF0ZSIsIHt9KQogICAgICAgICAgICAgICAgLmdldCgic2V0dGluZ3MiLCB7fSkKICAgICAg"
    "ICAgICAgICAgIC5nZXQoInNlc3Npb24iLCB7fSkKICAgICAgICAgICAgICAgIC5nZXQoImFjY2Vz"
    "c1Rva2VuIikKICAgICAgICAgICAgKQogICAgICAgICAgICBpZiB0b2s6CiAgICAgICAgICAgICAg"
    "ICBfU1BfVE9LRU4udXBkYXRlKHQ9bm93LCB2PXRvaywga2luZD0iYW5vbiIpCiAgICAgICAgICAg"
    "ICAgICBfbG9nKCJXWkZJWCBtdXNpYzogYW5vbnltb3VzIGNhdGFsb2cgdG9rZW4gKGVtYmVkKSIp"
    "CiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgdG9rID0gTm9uZQogICAgcmV0"
    "dXJuIHRvawoKCmFzeW5jIGRlZiBfY2F0YWxvZ190cmFja3MoYXJ0aXN0X2lkLCBuYW1lLCBvdXQs"
    "IGxpbWl0LCBhbm9uX2Zyb209Tm9uZSk6CiAgICAiIiJFeHRlbmQgdGhlIHRvcC10cmFja3MgbGlz"
    "dCB3aXRoIHRoZSBhcnRpc3QncyBkaXNjb2dyYXBoeToKICAgIGFsYnVtcyArIHNpbmdsZXMsIG5l"
    "d2VzdCBmaXJzdCwgdW50aWwgdGhlIGxpbWl0IGlzIHJlYWNoZWQuCiAgICBTdG9wcyBjbGVhbmx5"
    "IG9uIHF1b3RhIGVycm9ycyDigJQgdGhlIHRvcCB0cmFja3Mgc3RpbGwgd29yay4iIiIKICAgIGds"
    "b2JhbCBfQ0FUQUxPR19OT1RFCiAgICB0b2sgPSBhd2FpdCBfc3BvdGlmeV90b2tlbihhbm9uX2Zy"
    "b20pCiAgICBpZiBub3QgdG9rOgogICAgICAgIF9sb2coIldaRklYIG11c2ljOiBubyBjYXRhbG9n"
    "IHRva2VuIOKAlCB0b3AgdHJhY2tzIG9ubHkiKQogICAgICAgIF9DQVRBTE9HX05PVEUgPSAoCiAg"
    "ICAgICAgICAgICJcdTI2YTBcdWZlMGYgZGlzY29ncmFwaHkgdW5hdmFpbGFibGUg4oCUIG5vIFNw"
    "b3RpZnkgY2F0YWxvZyIKICAgICAgICAgICAgIiB0b2tlbi4gQWRkIFNQT1RJRllfQ0xJRU5UX0lE"
    "ICsgU1BPVElGWV9DTElFTlRfU0VDUkVUIHRvIgogICAgICAgICAgICAiIGNvbmZpZy5lbnYgZm9y"
    "IHRoZSBmdWxsIGxpc3QgKHRvcCB0cmFja3Mgb25seSBmb3Igbm93KS4iCiAgICAgICAgKQogICAg"
    "ICAgIHJldHVybiBvdXQKICAgIHNlZW4gPSB7X25vcm1fa2V5KF9uKSBmb3IgXywgX24gaW4gb3V0"
    "fQogICAgYWxidW1zLCB1cmwgPSBbXSwgKAogICAgICAgICJodHRwczovL2FwaS5zcG90aWZ5LmNv"
    "bS92MS9hcnRpc3RzLyIKICAgICAgICBmInthcnRpc3RfaWR9L2FsYnVtcz9pbmNsdWRlX2dyb3Vw"
    "cz1hbGJ1bSxzaW5nbGUmbGltaXQ9NTAiCiAgICApCiAgICBmb3IgXyBpbiByYW5nZSgyKToKICAg"
    "ICAgICB0cnk6CiAgICAgICAgICAgICMgcjI1Yzc6IHRocmVlIDQyOSByZXRyaWVzICh0aGUgYW5v"
    "biB0b2tlbiBpcyBxdW90YS1saW1pdGVkKQogICAgICAgICAgICByID0gYXdhaXQgYXN5bmNpby50"
    "b190aHJlYWQoX2FwaV9nZXQsIHVybCwgdG9rLCAxNS4wLCAzKQogICAgICAgIGV4Y2VwdCBFeGNl"
    "cHRpb24gYXMgZToKICAgICAgICAgICAgX2xvZyhmIldaRklYIG11c2ljOiBhbGJ1bXMgbGlzdCBm"
    "YWlsZWQ6IHtlfSIpCiAgICAgICAgICAgIF9DQVRBTE9HX05PVEUgPSAoCiAgICAgICAgICAgICAg"
    "ICBmIlx1MjZhMFx1ZmUwZiBkaXNjb2dyYXBoeSB1bmF2YWlsYWJsZSAoe2V9KSDigJQiCiAgICAg"
    "ICAgICAgICAgICAiIHRvcCB0cmFja3Mgb25seSIKICAgICAgICAgICAgKQogICAgICAgICAgICBy"
    "ZXR1cm4gb3V0CiAgICAgICAgYWxidW1zLmV4dGVuZChyLmdldCgiaXRlbXMiKSBvciBbXSkKICAg"
    "ICAgICB1cmwgPSByLmdldCgibmV4dCIpCiAgICAgICAgaWYgbm90IHVybDoKICAgICAgICAgICAg"
    "YnJlYWsKICAgICAgICBhd2FpdCBhc3luY2lvLnNsZWVwKDEuMCkKICAgIF9sb2coCiAgICAgICAg"
    "ZiJXWkZJWCBtdXNpYzoge2xlbihhbGJ1bXMpfSBhbGJ1bShzKSBmb3Ige25hbWV9ICIKICAgICAg"
    "ICAiKG5ld2VzdCBmaXJzdCkiCiAgICApCiAgICBmb3IgYWxiIGluIGFsYnVtczoKICAgICAgICBp"
    "ZiBsZW4ob3V0KSA+PSBsaW1pdDoKICAgICAgICAgICAgYnJlYWsKICAgICAgICBhd2FpdCBhc3lu"
    "Y2lvLnNsZWVwKDEuMikKICAgICAgICB0cnk6CiAgICAgICAgICAgIHRyID0gYXdhaXQgYXN5bmNp"
    "by50b190aHJlYWQoCiAgICAgICAgICAgICAgICBfYXBpX2dldCwKICAgICAgICAgICAgICAgIGYi"
    "aHR0cHM6Ly9hcGkuc3BvdGlmeS5jb20vdjEvYWxidW1zL3thbGJbJ2lkJ119IgogICAgICAgICAg"
    "ICAgICAgIi90cmFja3M/bGltaXQ9NTAiLAogICAgICAgICAgICAgICAgdG9rLAogICAgICAgICAg"
    "ICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICBfbG9nKGYiV1pG"
    "SVggbXVzaWM6IGFsYnVtIHRyYWNrcyBmYWlsZWQ6IHtlfSIpCiAgICAgICAgICAgIGJyZWFrCiAg"
    "ICAgICAgZm9yIHQgaW4gdHIuZ2V0KCJpdGVtcyIpIG9yIFtdOgogICAgICAgICAgICBfdCA9IHN0"
    "cih0LmdldCgibmFtZSIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAgICAgIGlmIG5vdCBfdDoKICAg"
    "ICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIF9tYWluID0gc3RyKAogICAgICAgICAg"
    "ICAgICAgKHQuZ2V0KCJhcnRpc3RzIikgb3IgW3t9XSlbMF0uZ2V0KCJuYW1lIikgb3IgbmFtZQog"
    "ICAgICAgICAgICApLnN0cmlwKCkKICAgICAgICAgICAgY2xlYW4gPSBmIntfbWFpbn0gLSB7X3R9"
    "IgogICAgICAgICAgICBpZiBfbm9ybV9rZXkoY2xlYW4pIGluIHNlZW46CiAgICAgICAgICAgICAg"
    "ICBjb250aW51ZQogICAgICAgICAgICBzZWVuLmFkZChfbm9ybV9rZXkoY2xlYW4pKQogICAgICAg"
    "ICAgICBvdXQuYXBwZW5kKChmInl0c2VhcmNoNTp7Y2xlYW59IGF1ZGlvIiwgY2xlYW4pKQogICAg"
    "cmV0dXJuIG91dAoKCmFzeW5jIGRlZiBfc2Fhdm5fYXJ0aXN0X3RyYWNrcyhuYW1lLCBvdXQsIGxp"
    "bWl0KToKICAgICIiIldaRklYIHIxNCAodjE1LjczKTogSmlvU2Fhdm4gZnVsbC1kaXNjb2dyYXBo"
    "eSBmYWxsYmFjay4KCiAgICBXaGVuIFNwb3RpZnkncyBjYXRhbG9nIEFQSSBpcyB1bmF2YWlsYWJs"
    "ZSAobm8gYXBwIGNyZWRlbnRpYWxzIGFuZAogICAgdGhlIGFub255bW91cyB0b2tlbiBxdW90YS1i"
    "bG9ja2VkKSwgbG9vayB0aGUgYXJ0aXN0IHVwIG9uIEppb1NhYXZuCiAgICBieSB0aGUgZXhhY3Qg"
    "bmFtZSB0YWtlbiBmcm9tIHRoZSBTcG90aWZ5IGVtYmVkIHBhZ2UgYW5kIGV4dGVuZCB0aGUKICAg"
    "IGxpc3Qgd2l0aCB0aGUgYXJ0aXN0J3Mgc29uZ3M6IHBhZ2VkIHNlYXJjaCByZXN1bHRzCiAgICAo"
    "c2VhcmNoLmdldFJlc3VsdHMsIDQwIHBlciBwYWdlKSwga2VlcGluZyBvbmx5IHNvbmdzIHdob3Nl"
    "CiAgICBQUklNQVJZIGFydGlzdCBtYXRjaGVzIOKAlCBtb3N0LXBsYXllZCBmaXJzdC4gTm8gYXV0"
    "aGVudGljYXRpb24sIG5vCiAgICBrZXlzLiBBZGRzIG5vdGhpbmcgd2hlbiBKaW9TYWF2biBoYXMg"
    "bm8gYXJ0aXN0IHdpdGggdGhhdCBleGFjdAogICAgbmFtZSAoaW50ZXJuYXRpb25hbCBhcnRpc3Rz"
    "IOKAlCBEZWV6ZXIgdGFrZXMgb3ZlciBuZXh0KS4KICAgICIiIgogICAgZ2xvYmFsIF9DQVRBTE9H"
    "X05PVEUKICAgIGlmIG5vdCBuYW1lIG9yIGxlbihvdXQpID49IGxpbWl0OgogICAgICAgIHJldHVy"
    "biBvdXQKICAgIGltcG9ydCB1cmxsaWIucGFyc2UKCiAgICBkZWYgX3ByaW0ocyk6CiAgICAgICAg"
    "cmV0dXJuICgocy5nZXQoIm1vcmVfaW5mbyIpIG9yIHt9KS5nZXQoImFydGlzdE1hcCIpIG9yIHt9"
    "KS5nZXQoCiAgICAgICAgICAgICJwcmltYXJ5X2FydGlzdHMiCiAgICAgICAgKSBvciBbXQoKICAg"
    "IHEgPSB1cmxsaWIucGFyc2UucXVvdGUobmFtZSkKICAgICMgcGFnZSAxIGRvdWJsZXMgYXMgdGhl"
    "IGFydGlzdC1pZCBzZWFyY2gKICAgIHIgPSBhd2FpdCBhc3luY2lvLnRvX3RocmVhZCgKICAgICAg"
    "ICBfanNvbl9nZXQsCiAgICAgICAgImh0dHBzOi8vd3d3Lmppb3NhYXZuLmNvbS9hcGkucGhwP19f"
    "Y2FsbD1zZWFyY2guZ2V0UmVzdWx0cyIKICAgICAgICBmIiZxPXtxfSZfZm9ybWF0PWpzb24mX21h"
    "cmtlcj0wJmFwaV92ZXJzaW9uPTQmY3R4PXdlYjZkb3QwIgogICAgICAgICImbj00MCZwPTEiLAog"
    "ICAgKQogICAgY291bnRzID0ge30KICAgIGZvciBzIGluIHIuZ2V0KCJyZXN1bHRzIikgb3IgW106"
    "CiAgICAgICAgZm9yIHBhIGluIF9wcmltKHMpOgogICAgICAgICAgICBpZiAocGEuZ2V0KCJuYW1l"
    "Iikgb3IgIiIpLnN0cmlwKCkubG93ZXIoKSA9PSBuYW1lLnN0cmlwKCkubG93ZXIoKToKICAgICAg"
    "ICAgICAgICAgIF9pZCA9IHN0cihwYS5nZXQoImlkIikgb3IgIiIpCiAgICAgICAgICAgICAgICBp"
    "ZiBfaWQ6CiAgICAgICAgICAgICAgICAgICAgY291bnRzW19pZF0gPSBjb3VudHMuZ2V0KF9pZCwg"
    "MCkgKyAxCiAgICBpZiBub3QgY291bnRzOgogICAgICAgIF9sb2coZiJXWkZJWCBtdXNpYzogc2Fh"
    "dm4gaGFzIG5vIGFydGlzdCAne25hbWV9JyDigJQgc2tpcCIpCiAgICAgICAgcmV0dXJuIG91dAog"
    "ICAgYWlkID0gbWF4KGNvdW50cywga2V5PWNvdW50cy5nZXQpCiAgICBfbG9nKAogICAgICAgIGYi"
    "V1pGSVggbXVzaWM6IHNhYXZuIGFydGlzdCAne25hbWV9JyBpZCB7YWlkfSAiCiAgICAgICAgZiIo"
    "cGFnZSAxOiB7bGVuKHIuZ2V0KCdyZXN1bHRzJykgb3IgW10pfSByZXN1bHRzLCAiCiAgICAgICAg"
    "ZiJ0b3RhbCB7ci5nZXQoJ3RvdGFsJyl9KSIKICAgICkKICAgIHNlZW4gPSB7X25vcm1fa2V5KF9u"
    "KSBmb3IgXywgX24gaW4gb3V0fQogICAgYWRkZWQgPSAwCgogICAgZGVmIF90YWtlKHNvbmdzKToK"
    "ICAgICAgICBub25sb2NhbCBhZGRlZAogICAgICAgIGZvciBzIGluIHNvbmdzOgogICAgICAgICAg"
    "ICBpZiBsZW4ob3V0KSA+PSBsaW1pdDoKICAgICAgICAgICAgICAgIHJldHVybgogICAgICAgICAg"
    "ICBfdCA9IHN0cihzLmdldCgidGl0bGUiKSBvciAiIikuc3RyaXAoKQogICAgICAgICAgICBpZiBu"
    "b3QgX3Q6CiAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICBpZiBub3QgYW55KHN0"
    "cihwLmdldCgiaWQiKSBvciAiIikgPT0gYWlkIGZvciBwIGluIF9wcmltKHMpKToKICAgICAgICAg"
    "ICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIGNsZWFuID0gZiJ7bmFtZX0gLSB7X3R9IgogICAg"
    "ICAgICAgICBpZiBfbm9ybV9rZXkoY2xlYW4pIGluIHNlZW46CiAgICAgICAgICAgICAgICBjb250"
    "aW51ZQogICAgICAgICAgICBzZWVuLmFkZChfbm9ybV9rZXkoY2xlYW4pKQogICAgICAgICAgICBv"
    "dXQuYXBwZW5kKChmInl0c2VhcmNoNTp7Y2xlYW59IGF1ZGlvIiwgY2xlYW4pKQogICAgICAgICAg"
    "ICBhZGRlZCArPSAxCgogICAgIyBwYWdlLTEgc29uZ3MgZmlyc3QgKGhpZ2hlc3QgcmVsZXZhbmNl"
    "KSwgdGhlbiBtb3JlIHBhZ2VzIHVudGlsIHRoZQogICAgIyBkaXNjb2dyYXBoeSBpcyBjb3ZlcmVk"
    "IChjYXA6IDUgcGFnZXMgPSAyMDAgc2VhcmNoIHJlc3VsdHMpCiAgICBfdGFrZShyLmdldCgicmVz"
    "dWx0cyIpIG9yIFtdKQogICAgcGFnZSA9IDIKICAgIHdoaWxlIGxlbihvdXQpIDwgbGltaXQgYW5k"
    "IHBhZ2UgPD0gNToKICAgICAgICBhd2FpdCBhc3luY2lvLnNsZWVwKDAuNikKICAgICAgICByMiA9"
    "IGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgICAgICBfanNvbl9nZXQsCiAgICAgICAg"
    "ICAgICJodHRwczovL3d3dy5qaW9zYWF2bi5jb20vYXBpLnBocD9fX2NhbGw9c2VhcmNoLmdldFJl"
    "c3VsdHMiCiAgICAgICAgICAgIGYiJnE9e3F9Jl9mb3JtYXQ9anNvbiZfbWFya2VyPTAmYXBpX3Zl"
    "cnNpb249NCZjdHg9d2ViNmRvdDAiCiAgICAgICAgICAgIGYiJm49NDAmcD17cGFnZX0iLAogICAg"
    "ICAgICkKICAgICAgICBfcmVzID0gcjIuZ2V0KCJyZXN1bHRzIikgb3IgW10KICAgICAgICBpZiBu"
    "b3QgX3JlczoKICAgICAgICAgICAgYnJlYWsKICAgICAgICBfdGFrZShfcmVzKQogICAgICAgIHBh"
    "Z2UgKz0gMQogICAgaWYgYWRkZWQ6CiAgICAgICAgX2xvZygKICAgICAgICAgICAgZiJXWkZJWCBt"
    "dXNpYzogc2Fhdm4gZmFsbGJhY2sgK3thZGRlZH0gc29uZyhzKSBmb3Ige25hbWV9ICIKICAgICAg"
    "ICAgICAgZiIoYXJ0aXN0IGlkIHthaWR9LCB7cGFnZSAtIDF9IHBhZ2UocyksIGxpc3Qgbm93IHts"
    "ZW4ob3V0KX0pIgogICAgICAgICkKICAgICAgICBfQ0FUQUxPR19OT1RFID0gKAogICAgICAgICAg"
    "ICBmIlxVMDAwMWY0YzIgZnVsbCBkaXNjb2dyYXBoeSB2aWEgSmlvU2Fhdm4g4oCUIFNwb3RpZnkg"
    "Y2F0YWxvZyIKICAgICAgICAgICAgZiIgd2FzIHVuYXZhaWxhYmxlICh7bGVuKG91dCl9IHNvbmdz"
    "KSIKICAgICAgICApCiAgICBlbHNlOgogICAgICAgIF9sb2coCiAgICAgICAgICAgIGYiV1pGSVgg"
    "bXVzaWM6IHNhYXZuIGZvdW5kIGFydGlzdCBidXQgbm8gbmV3IHNvbmdzICIKICAgICAgICAgICAg"
    "ZiIobGlzdCBzdGF5cyB7bGVuKG91dCl9KSIKICAgICAgICApCiAgICByZXR1cm4gb3V0CgoKYXN5"
    "bmMgZGVmIF9kZWV6ZXJfYXJ0aXN0X3RyYWNrcyhuYW1lLCBvdXQsIGxpbWl0LCBtYXhfYWxidW1z"
    "PTEwMCk6CiAgICAiIiJXWkZJWCByMTMgKHYxNS43Mik6IERlZXplciBjYXRhbG9nIGZhbGxiYWNr"
    "IChpbnRlcm5hdGlvbmFsCiAgICBhcnRpc3RzKSDigJQgcHVibGljIEFQSSwgbm8ga2V5cy4gVG9w"
    "IHRyYWNrcyBmaXJzdCwgdGhlbiB0aGUgbmV3ZXN0CiAgICBhbGJ1bXMsIGNhcHBlZCBhdCBtYXhf"
    "YWxidW1zIHJlcXVlc3RzIHNvIGEgaHVnZSBkaXNjb2dyYXBoeSBjYW5ub3QKICAgIHN0YWxsIHRo"
    "ZSByZXNvbHZlIHN0YWdlIGZvciBtaW51dGVzLiIiIgogICAgZ2xvYmFsIF9DQVRBTE9HX05PVEUK"
    "ICAgIGlmIG5vdCBuYW1lIG9yIGxlbihvdXQpID49IGxpbWl0OgogICAgICAgIHJldHVybiBvdXQK"
    "ICAgIGltcG9ydCB1cmxsaWIucGFyc2UKCiAgICBxID0gdXJsbGliLnBhcnNlLnF1b3RlKG5hbWUp"
    "CiAgICByID0gYXdhaXQgYXN5bmNpby50b190aHJlYWQoCiAgICAgICAgX2pzb25fZ2V0LAogICAg"
    "ICAgIGYiaHR0cHM6Ly9hcGkuZGVlemVyLmNvbS9zZWFyY2gvYXJ0aXN0P3E9e3F9JmxpbWl0PTEw"
    "IiwKICAgICkKICAgIGJlc3QgPSBOb25lCiAgICBmb3IgYSBpbiByLmdldCgiZGF0YSIpIG9yIFtd"
    "OgogICAgICAgIGlmIChhLmdldCgibmFtZSIpIG9yICIiKS5zdHJpcCgpLmxvd2VyKCkgIT0gbmFt"
    "ZS5zdHJpcCgpLmxvd2VyKCk6CiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgdHJ5OgogICAg"
    "ICAgICAgICBfZmFucyA9IGludChhLmdldCgibmJfZmFuIikgb3IgMCkKICAgICAgICBleGNlcHQg"
    "RXhjZXB0aW9uOgogICAgICAgICAgICBfZmFucyA9IDAKICAgICAgICBpZiBiZXN0IGlzIE5vbmUg"
    "b3IgX2ZhbnMgPiBiZXN0WzFdOgogICAgICAgICAgICBiZXN0ID0gKGEsIF9mYW5zKQogICAgaWYg"
    "bm90IGJlc3Q6CiAgICAgICAgX2xvZyhmIldaRklYIG11c2ljOiBkZWV6ZXIgaGFzIG5vIGFydGlz"
    "dCAne25hbWV9JyDigJQgc2tpcCIpCiAgICAgICAgcmV0dXJuIG91dAogICAgYWlkID0gYmVzdFsw"
    "XVsiaWQiXQogICAgc2VlbiA9IHtfbm9ybV9rZXkoX24pIGZvciBfLCBfbiBpbiBvdXR9CiAgICBh"
    "ZGRlZCA9IDAKICAgIHRvcCA9IGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgIF9qc29u"
    "X2dldCwgZiJodHRwczovL2FwaS5kZWV6ZXIuY29tL2FydGlzdC97YWlkfS90b3A/bGltaXQ9NTAi"
    "CiAgICApCiAgICBmb3IgdCBpbiB0b3AuZ2V0KCJkYXRhIikgb3IgW106CiAgICAgICAgaWYgbGVu"
    "KG91dCkgPj0gbGltaXQ6CiAgICAgICAgICAgIGJyZWFrCiAgICAgICAgX2FydCA9IHN0cigodC5n"
    "ZXQoImFydGlzdCIpIG9yIHt9KS5nZXQoIm5hbWUiKSBvciBuYW1lKS5zdHJpcCgpCiAgICAgICAg"
    "X3QgPSBzdHIodC5nZXQoInRpdGxlIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICBpZiBub3QgX3Q6"
    "CiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgY2xlYW4gPSBmIntfYXJ0fSAtIHtfdH0iCiAg"
    "ICAgICAgaWYgX25vcm1fa2V5KGNsZWFuKSBpbiBzZWVuOgogICAgICAgICAgICBjb250aW51ZQog"
    "ICAgICAgIHNlZW4uYWRkKF9ub3JtX2tleShjbGVhbikpCiAgICAgICAgb3V0LmFwcGVuZCgoZiJ5"
    "dHNlYXJjaDU6e2NsZWFufSBhdWRpbyIsIGNsZWFuKSkKICAgICAgICBhZGRlZCArPSAxCiAgICBh"
    "bGJ1bXMgPSBhd2FpdCBhc3luY2lvLnRvX3RocmVhZCgKICAgICAgICBfanNvbl9nZXQsCiAgICAg"
    "ICAgZiJodHRwczovL2FwaS5kZWV6ZXIuY29tL2FydGlzdC97YWlkfS9hbGJ1bXM/bGltaXQ9MTAw"
    "IiwKICAgICkKICAgIGZvciBhbGIgaW4gYWxidW1zLmdldCgiZGF0YSIpIG9yIFtdOgogICAgICAg"
    "IGlmIGxlbihvdXQpID49IGxpbWl0IG9yIG5vdCBtYXhfYWxidW1zOgogICAgICAgICAgICBicmVh"
    "awogICAgICAgIG1heF9hbGJ1bXMgLT0gMQogICAgICAgIGF3YWl0IGFzeW5jaW8uc2xlZXAoMS4w"
    "KQogICAgICAgIHRyeToKICAgICAgICAgICAgdHIgPSBhd2FpdCBhc3luY2lvLnRvX3RocmVhZCgK"
    "ICAgICAgICAgICAgICAgIF9qc29uX2dldCwKICAgICAgICAgICAgICAgIGYiaHR0cHM6Ly9hcGku"
    "ZGVlemVyLmNvbS9hbGJ1bS97YWxiWydpZCddfS90cmFja3MiLAogICAgICAgICAgICApCiAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgY29udGludWUKICAgICAgICBmb3IgdCBp"
    "biB0ci5nZXQoImRhdGEiKSBvciBbXToKICAgICAgICAgICAgaWYgbGVuKG91dCkgPj0gbGltaXQ6"
    "CiAgICAgICAgICAgICAgICBicmVhawogICAgICAgICAgICBfYXJ0ID0gc3RyKAogICAgICAgICAg"
    "ICAgICAgKHQuZ2V0KCJhcnRpc3QiKSBvciB7fSkuZ2V0KCJuYW1lIikgb3IgbmFtZQogICAgICAg"
    "ICAgICApLnN0cmlwKCkKICAgICAgICAgICAgX3QgPSBzdHIodC5nZXQoInRpdGxlIikgb3IgIiIp"
    "LnN0cmlwKCkKICAgICAgICAgICAgaWYgbm90IF90OgogICAgICAgICAgICAgICAgY29udGludWUK"
    "ICAgICAgICAgICAgY2xlYW4gPSBmIntfYXJ0fSAtIHtfdH0iCiAgICAgICAgICAgIGlmIF9ub3Jt"
    "X2tleShjbGVhbikgaW4gc2VlbjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAg"
    "IHNlZW4uYWRkKF9ub3JtX2tleShjbGVhbikpCiAgICAgICAgICAgIG91dC5hcHBlbmQoKGYieXRz"
    "ZWFyY2g1OntjbGVhbn0gYXVkaW8iLCBjbGVhbikpCiAgICAgICAgICAgIGFkZGVkICs9IDEKICAg"
    "IGlmIGFkZGVkOgogICAgICAgIF9sb2coCiAgICAgICAgICAgIGYiV1pGSVggbXVzaWM6IGRlZXpl"
    "ciBmYWxsYmFjayAre2FkZGVkfSBzb25nKHMpIGZvciB7bmFtZX0iCiAgICAgICAgICAgIGYiIChh"
    "cnRpc3QgaWQge2FpZH0pIgogICAgICAgICkKICAgICAgICBfQ0FUQUxPR19OT1RFID0gKAogICAg"
    "ICAgICAgICBmIlxVMDAwMWY0YzIgZnVsbCBkaXNjb2dyYXBoeSB2aWEgRGVlemVyIOKAlCBTcG90"
    "aWZ5IGNhdGFsb2ciCiAgICAgICAgICAgIGYiIHdhcyB1bmF2YWlsYWJsZSAoe2xlbihvdXQpfSBz"
    "b25ncykiCiAgICAgICAgKQogICAgcmV0dXJuIG91dAoKCmFzeW5jIGRlZiBfc3BvdGlmeV9hcnRp"
    "c3RfdHJhY2tzKHVybCwgbGltaXQ9MTApOgogICAgIiIiU3BvdGlmeSBhcnRpc3Qg4oaSIHNtYXJ0"
    "IHRyYWNrIGxpc3QuIFRvcCB0cmFja3MgZmlyc3QgKFNwb3RpZnkncwogICAgb3duIHBvcHVsYXJp"
    "dHkgb3JkZXIpLCB0aGVuIHRoZSBkaXNjb2dyYXBoeSBuZXdlc3QtYWxidW0tZmlyc3Qg4oCUCiAg"
    "ICBzbyBhbnkgbGltaXQgZ2l2ZXMgdGhlIGJlc3QgcG9zc2libGUgbWl4LiBGYWxscyBiYWNrIHRv"
    "IHRvcC0xMAogICAgKHRoZSBlbWJlZCBwYWdlKSB3aGVuZXZlciB0aGUgY2F0YWxvZyBBUEkgaXMg"
    "dW5hdmFpbGFibGUuIiIiCiAgICBnbG9iYWwgX0NBVEFMT0dfTk9URQogICAgX0NBVEFMT0dfTk9U"
    "RSA9ICIiCiAgICBtID0gcmUuc2VhcmNoKHIiLyg/OmludGwtW2Etei1dKy8pP2FydGlzdC8oW0Et"
    "WmEtejAtOV0rKSIsIHVybCkKICAgIGlmIG5vdCBtOgogICAgICAgIHJldHVybiBOb25lCiAgICB0"
    "cnk6CiAgICAgICAgZmluYWwsIGh0bWwgPSBhd2FpdCBfZmV0Y2goCiAgICAgICAgICAgIGYiaHR0"
    "cHM6Ly9vcGVuLnNwb3RpZnkuY29tL2VtYmVkL2FydGlzdC97bS5ncm91cCgxKX0iCiAgICAgICAg"
    "KQogICAgICAgIG1tID0gcmUuc2VhcmNoKAogICAgICAgICAgICByJzxzY3JpcHQgaWQ9Il9fTkVY"
    "VF9EQVRBX18iIHR5cGU9ImFwcGxpY2F0aW9uL2pzb24iPicKICAgICAgICAgICAgciIoLio/KTwv"
    "c2NyaXB0PiIsCiAgICAgICAgICAgIGh0bWwsCiAgICAgICAgICAgIHJlLlMsCiAgICAgICAgKQog"
    "ICAgICAgIGlmIG5vdCBtbToKICAgICAgICAgICAgcmV0dXJuIE5vbmUKICAgICAgICBkID0ganNv"
    "bi5sb2FkcyhtbS5ncm91cCgxKSkKICAgICAgICBlbnQgPSAoCiAgICAgICAgICAgIGQuZ2V0KCJw"
    "cm9wcyIsIHt9KQogICAgICAgICAgICAuZ2V0KCJwYWdlUHJvcHMiLCB7fSkKICAgICAgICAgICAg"
    "LmdldCgic3RhdGUiLCB7fSkKICAgICAgICAgICAgLmdldCgiZGF0YSIsIHt9KQogICAgICAgICAg"
    "ICAuZ2V0KCJlbnRpdHkiLCB7fSkKICAgICAgICApCiAgICAgICAgaWYgZW50LmdldCgidHlwZSIp"
    "ICE9ICJhcnRpc3QiOgogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIG5hbWUgPSBzdHIo"
    "ZW50LmdldCgibmFtZSIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAgdG9wID0gW10KICAgICAgICBm"
    "b3IgdCBpbiBlbnQuZ2V0KCJ0cmFja0xpc3QiKSBvciBbXToKICAgICAgICAgICAgX3QgPSBzdHIo"
    "dC5nZXQoInRpdGxlIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICAgICAgaWYgbm90IF90OgogICAg"
    "ICAgICAgICAgICAgY29udGludWUKICAgICAgICAgICAgX3N1YiA9IHN0cih0LmdldCgic3VidGl0"
    "bGUiKSBvciAiIikuc3RyaXAoKQogICAgICAgICAgICBfbWFpbiA9IChfc3ViLnNwbGl0KCIsIilb"
    "MF0uc3RyaXAoKSBvciBuYW1lKS5zdHJpcCgpCiAgICAgICAgICAgIGlmIG5vdCBfbWFpbjoKICAg"
    "ICAgICAgICAgICAgIF9tYWluID0gbmFtZSBvciAiYXJ0aXN0IgogICAgICAgICAgICB0b3AuYXBw"
    "ZW5kKAogICAgICAgICAgICAgICAgKGYieXRzZWFyY2g1OntfbWFpbn0gLSB7X3R9IGF1ZGlvIiwg"
    "ZiJ7X21haW59IC0ge190fSIpCiAgICAgICAgICAgICkKICAgICAgICBpZiBub3QgdG9wOgogICAg"
    "ICAgICAgICByZXR1cm4gTm9uZQogICAgICAgICMgV1pGSVggcjEyICh2MTUuNzEpOiBub3JtYWxp"
    "emVkIGRlZHVwZSBJTlNJREUgdGhlIHRvcCBsaXN0IOKAlAogICAgICAgICMgdGhlIGVtYmVkIHRy"
    "YWNrTGlzdCBjYW4gcmVwZWF0IGEgc29uZyB3aXRoIGEgZGlmZmVyZW50CiAgICAgICAgIyBzdWZm"
    "aXggKChmZWF0LiBYKSwgW09mZmljaWFsIFZpZGVvXSwgLi4uKSBhbmQgdGhlIGZhbi1vdXQKICAg"
    "ICAgICAjIG11c3Qgbm90IGRvd25sb2FkIGl0IHR3aWNlLgogICAgICAgIF9kZWQgPSB7fQogICAg"
    "ICAgIGZvciBfcSwgX24yIGluIHRvcDoKICAgICAgICAgICAgX2syID0gX25vcm1fa2V5KF9uMikK"
    "ICAgICAgICAgICAgaWYgX2syIG5vdCBpbiBfZGVkOgogICAgICAgICAgICAgICAgX2RlZFtfazJd"
    "ID0gKF9xLCBfbjIpCiAgICAgICAgdG9wID0gbGlzdChfZGVkLnZhbHVlcygpKQogICAgICAgIG91"
    "dCA9IGxpc3QodG9wKQogICAgICAgIGlmIGxpbWl0ID4gbGVuKG91dCk6CiAgICAgICAgICAgIHRy"
    "eToKICAgICAgICAgICAgICAgIG91dCA9IGF3YWl0IF9jYXRhbG9nX3RyYWNrcygKICAgICAgICAg"
    "ICAgICAgICAgICBtLmdyb3VwKDEpLCBuYW1lLCBvdXQsIGxpbWl0LCBkCiAgICAgICAgICAgICAg"
    "ICApCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAgICAgICAgIF9s"
    "b2coZiJXWkZJWCBtdXNpYzogZGlzY29ncmFwaHkgZmFpbGVkOiB7ZX0iKQogICAgICAgICMgV1pG"
    "SVggcjEzICh2MTUuNzIpOiBTcG90aWZ5IGNhdGFsb2cgdW5hdmFpbGFibGUgb3IgaW5jb21wbGV0"
    "ZQogICAgICAgICMgKGFub255bW91cyB0b2tlbiBxdW90YS1ibG9ja2VkIC8gbm8gYXBwIGNyZWRl"
    "bnRpYWxzKSDigJQgZXh0ZW5kCiAgICAgICAgIyB0aGUgbGlzdCBmcm9tIEppb1NhYXZuIChyZWdp"
    "b25hbCkgZmlyc3QsIHRoZW4gRGVlemVyCiAgICAgICAgIyAoaW50ZXJuYXRpb25hbCkuIEJvdGgg"
    "YXJlIGZyZWUgcHVibGljIEFQSXMgd2l0aCBubyBrZXlzLgogICAgICAgIGlmIGxpbWl0ID4gbGVu"
    "KG91dCk6CiAgICAgICAgICAgIF9wcmUgPSBsZW4ob3V0KQogICAgICAgICAgICB0cnk6CiAgICAg"
    "ICAgICAgICAgICBvdXQgPSBhd2FpdCBfc2Fhdm5fYXJ0aXN0X3RyYWNrcyhuYW1lLCBvdXQsIGxp"
    "bWl0KQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgICAgICBf"
    "bG9nKGYiV1pGSVggbXVzaWM6IHNhYXZuIGZhbGxiYWNrIGZhaWxlZDoge2V9IikKICAgICAgICAg"
    "ICAgaWYgbGltaXQgPiBsZW4ob3V0KSBhbmQgbGVuKG91dCkgPT0gX3ByZToKICAgICAgICAgICAg"
    "ICAgIHRyeToKICAgICAgICAgICAgICAgICAgICBvdXQgPSBhd2FpdCBfZGVlemVyX2FydGlzdF90"
    "cmFja3MobmFtZSwgb3V0LCBsaW1pdCkKICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24g"
    "YXMgZToKICAgICAgICAgICAgICAgICAgICBfbG9nKGYiV1pGSVggbXVzaWM6IGRlZXplciBmYWxs"
    "YmFjayBmYWlsZWQ6IHtlfSIpCiAgICAgICAgX2xvZygKICAgICAgICAgICAgZiJXWkZJWCBtdXNp"
    "YzogYXJ0aXN0IHtuYW1lfSDihpIge2xlbihvdXQpfSB0cmFjayhzKSAiCiAgICAgICAgICAgIGYi"
    "KHRvcCB7bGVuKHRvcCl9ICsgZGlzY29ncmFwaHkpIgogICAgICAgICkKICAgICAgICByZXR1cm4g"
    "KG5hbWUsIG91dFs6IG1heCgxLCBpbnQobGltaXQpKV0pCiAgICBleGNlcHQgRXhjZXB0aW9uIGFz"
    "IGU6CiAgICAgICAgX2xvZyhmIldaRklYIG11c2ljOiBhcnRpc3QgcGFnZSBmYWlsZWQ6IHtlfSIp"
    "CiAgICAgICAgcmV0dXJuIE5vbmUKCgpfQ0ZHX0xPR0dFRCA9IEZhbHNlCl9GQU5PVVRfU1RBR0dF"
    "UiA9IDEuMApfRkFOT1VUX1dBVENIID0gMTAuMAoKCmRlZiBfdGFza19kaWN0X3JlZigpOgogICAg"
    "dHJ5OgogICAgICAgIGZyb20gLi4uIGltcG9ydCB0YXNrX2RpY3QKCiAgICAgICAgcmV0dXJuIHRh"
    "c2tfZGljdAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gTm9uZQoKCmRlZiBf"
    "ZmFub3V0X2pvYnMoKToKICAgICIiIldaRklYIHIxNSAodjE1Ljc0KTogY29uY3VycmVudC1kb3du"
    "bG9hZCB0YXJnZXQgZm9yIGFydGlzdAogICAgYmF0Y2hlcy4gVGhlIGNlaWxpbmcgaXMgWW91VHVi"
    "ZSdzIHBlci1JUCB0aHJvdHRsaW5nIChub3QgUkFNKSAtLQogICAgNC04IGlzIHRoZSB1bmF1dGhl"
    "bnRpY2F0ZWQgc3dlZXQgc3BvdDsgMTYgd2l0aCBjb29raWVzIGlzIGEKICAgIHJlYXNvbmFibGUg"
    "ZGVmYXVsdC4gVHVuZSB2aWEgdGhlIFdaRklYX0ZBTk9VVF9KT0JTIGVudiAoMS00MCkuIiIiCiAg"
    "ICB0cnk6CiAgICAgICAgaW1wb3J0IG9zIGFzIF9vcwoKICAgICAgICBfaiA9IGludChfb3MuZW52"
    "aXJvbi5nZXQoIldaRklYX0ZBTk9VVF9KT0JTIiwgIjYiKSkKICAgIGV4Y2VwdCBFeGNlcHRpb246"
    "CiAgICAgICAgX2ogPSAxNgogICAgcmV0dXJuIG1heCgxLCBtaW4oX2osIDQwKSkKCgpkZWYgX2Fj"
    "dGl2ZV9kbF9jb3VudCgpOgogICAgdHJ5OgogICAgICAgIGZyb20gLi4uIGltcG9ydCBub25fcXVl"
    "dWVkX2RsLCBxdWV1ZWRfZGwKCiAgICAgICAgcmV0dXJuIGxlbihub25fcXVldWVkX2RsKSArIGxl"
    "bihxdWV1ZWRfZGwpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBOb25lCgoK"
    "ZGVmIF9zYW5pdGl6ZV9mb2xkZXIobmFtZSk6CiAgICBfZiA9IHJlLnN1YihyIlteQS1aYS16MC05"
    "IF8tXSIsICIiLCBzdHIobmFtZSBvciAiIikpWzo0OF0KICAgIHJldHVybiAoX2Yuc3RyaXAoKS5y"
    "ZXBsYWNlKCIgIiwgIl8iKSBvciAiYXJ0aXN0IikKCgphc3luYyBkZWYgX2FydGlzdF9mYW5vdXQo"
    "Y2xpZW50LCBtZXNzYWdlLCBhcnRpc3QsIHRyYWNrcywgaXNfbGVlY2gpOgogICAgIiIiT25lIHRh"
    "c2sgcGVyIHRvcCB0cmFjaywgYWxsIG1lcmdlZCBpbnRvIG9uZSBmb2xkZXI6IGV2ZXJ5CiAgICBm"
    "aW5pc2hlZCBzb25nIG1vdmVzIGludG8gdGhlIGxhc3QgdGFzaydzIGRpcmVjdG9yeSwgc28gdGhl"
    "IGZpbmFsCiAgICB1cGxvYWQgZGVsaXZlcnMgZXZlcnl0aGluZyB0b2dldGhlciDigJQgb25lIHVw"
    "bG9hZCBwYXNzLCBubyB6aXAuCiAgICBGbGFncyBmcm9tIHRoZSB1c2VyJ3MgY29tbWFuZCAoLW4s"
    "IC4uLikgcGFzcyB0aHJvdWdoIHRvIGV2ZXJ5IHRhc2s7CiAgICAteiBpcyBzdHJpcHBlZCAoaXQg"
    "d291bGQgdXBsb2FkIHRoZSB3aG9sZSBiYXRjaCBhIHNlY29uZCB0aW1lKS4iIiIKICAgIGltcG9y"
    "dCBjb3B5IGFzIF9jb3B5CgogICAgZnJvbSAuLi5tb2R1bGVzLnl0ZGxwIGltcG9ydCBZdERscAoK"
    "ICAgIF90b2tzID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3BsaXQoKQogICAgIyBXWkZJWCByMTQg"
    "KHYxNS43Myk6IC16IHdvdWxkIHppcCB0aGUgd2hvbGUgYmF0Y2ggQUZURVIgZXZlcnkgc29uZwog"
    "ICAgIyB3YXMgYWxyZWFkeSB1cGxvYWRlZCDigJQgYSBmdWxsIHNlY29uZCB1cGxvYWQgcGFzcy4g"
    "TXVzaWMgYmF0Y2hlcwogICAgIyBuZXZlciB6aXAsIHNvIHRoZSBmbGFnIGlzIHN0cmlwcGVkIGZy"
    "b20gZXZlcnkgY2xvbmUuCiAgICBfZmxhZ3MgPSAiICIuam9pbigKICAgICAgICB0CiAgICAgICAg"
    "Zm9yIHQgaW4gX3Rva3NbMTpdCiAgICAgICAgaWYgbm90IHQuc3RhcnRzd2l0aCgiaHR0cCIpIGFu"
    "ZCB0Lmxvd2VyKCkgbm90IGluICgiLXoiLCAiLXppcCIpCiAgICApLnN0cmlwKCkKICAgIGlmIGFu"
    "eSh0Lmxvd2VyKCkgaW4gKCIteiIsICItemlwIikgZm9yIHQgaW4gX3Rva3NbMTpdKToKICAgICAg"
    "ICBfbG9nKCJXWkZJWCByMTQ6IC16IHN0cmlwcGVkIGZyb20gdGhlIG11c2ljIGJhdGNoIChvbmUg"
    "dXBsb2FkIHBhc3MpIikKICAgIGdsb2JhbCBfQ0ZHX0xPR0dFRAogICAgaWYgbm90IF9DRkdfTE9H"
    "R0VEOgogICAgICAgIF9DRkdfTE9HR0VEID0gVHJ1ZQogICAgICAgIHRyeToKICAgICAgICAgICAg"
    "ZnJvbSAuLi4gaW1wb3J0IENvbmZpZyBhcyBfQwoKICAgICAgICAgICAgX2xvZygKICAgICAgICAg"
    "ICAgICAgICJXWkZJWCByMTQ6IHVwbG9hZCBjb25maWcg4oCUICIKICAgICAgICAgICAgICAgIGYi"
    "TEVFQ0hfTE9HX0NIQVQ9e2dldGF0dHIoX0MsICdMRUVDSF9MT0dfQ0hBVCcsIE5vbmUpIXJ9LCAi"
    "CiAgICAgICAgICAgICAgICBmIkJPVF9QTT17Z2V0YXR0cihfQywgJ0JPVF9QTScsIE5vbmUpIXJ9"
    "LCAiCiAgICAgICAgICAgICAgICBmIk1FRElBX1NUT1JFPXtnZXRhdHRyKF9DLCAnTUVESUFfU1RP"
    "UkUnLCBOb25lKSFyfSwgIgogICAgICAgICAgICAgICAgZiJVU0VfSFlQRVI9e2dldGF0dHIoX0Ms"
    "ICdVU0VfSFlQRVInLCBOb25lKSFyfSwgIgogICAgICAgICAgICAgICAgZiJ1c2VyX3Nlc3Npb249"
    "e2Jvb2woZ2V0YXR0cihfQywgJ1VTRVJfU0VTU0lPTl9TVFJJTkcnLCAnJykpfSIKICAgICAgICAg"
    "ICAgKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIHBhc3MKICAgIF9mb2xk"
    "ZXIgPSBfc2FuaXRpemVfZm9sZGVyKGFydGlzdCkKICAgIF9uID0gbGVuKHRyYWNrcykKICAgICMg"
    "cHJlLXNlZWRlZCBzbyBldmVyeSBjbG9uZSByZWdpc3RlcnMgaW50byB0aGlzIHNoYXJlZCBkaWN0"
    "CiAgICAjICh0b3RhbCA9IGFsbCB0cmFja3M7IGVhY2ggY2xvbmUgc2VsZi1yZWdpc3RlcnMgd2l0"
    "aCAtaSAxKQogICAgX3NoYXJlZCA9IHsKICAgICAgICBmIi97X2ZvbGRlcn0iOiB7CiAgICAgICAg"
    "ICAgICJ0b3RhbCI6IF9uLAogICAgICAgICAgICAidGFza3MiOiBzZXQoKSwKICAgICAgICAgICAg"
    "IyBXWkZJWCByMTEgKHYxNS43MCk6IGZhbi1vdXQgbWFya2VyIOKAlCB0YXNrX2xpc3RlbmVyIHVz"
    "ZXMKICAgICAgICAgICAgIyBpdCB0byBrZWVwIHRoZSBiYXRjaCBxdWlldCBhbmQgdG8gY29sbGVj"
    "dCB0aGUgdXBsb2FkZWQKICAgICAgICAgICAgIyBzb25ncyBmb3IgdGhlIHBsYXlsaXN0IHByb21w"
    "dAogICAgICAgICAgICAiX3d6Zml4IjogewogICAgICAgICAgICAgICAgImFydGlzdCI6IGFydGlz"
    "dCwKICAgICAgICAgICAgICAgICJmYWlsZWQiOiBbXSwKICAgICAgICAgICAgICAgICJwbCI6IFtd"
    "LAogICAgICAgICAgICAgICAgIyBXWkZJWCByMTUgKHYxNS43NCk6IGRldGVybWluaXN0aWMgbWVy"
    "Z2Ug4oCUIGV2ZXJ5IGNsb25lCiAgICAgICAgICAgICAgICAjIG1vdmVzIGl0cyBzb25nIGludG8g"
    "dGhlIGxlYWRlcidzIGZvbGRlcjsgdGhlIHRhc2sKICAgICAgICAgICAgICAgICMgdGhhdCBmaW5p"
    "c2hlcyBsYXN0IHVwbG9hZHMgZXZlcnl0aGluZyBleGFjdGx5IG9uY2UKICAgICAgICAgICAgICAg"
    "ICJ0b3RhbCI6IF9uLAogICAgICAgICAgICAgICAgImRvbmUiOiBzZXQoKSwKICAgICAgICAgICAg"
    "ICAgICJsZWFkZXJfbWlkIjogTm9uZSwKICAgICAgICAgICAgICAgICJtb2RlIjogInNvbmciLAog"
    "ICAgICAgICAgICB9LAogICAgICAgIH0KICAgIH0KICAgIF9taWRzID0gW10KCiAgICBub3RlID0g"
    "Tm9uZQogICAgX2xpbmVzID0gIlxuIi5qb2luKAogICAgICAgIGYie2l9LiB7X24yfSIgZm9yIGks"
    "IChfLCBfbjIpIGluIGVudW1lcmF0ZSh0cmFja3MsIDEpCiAgICApCiAgICB0cnk6CiAgICAgICAg"
    "ZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1faGVscGVyLm1lc3NhZ2VfdXRpbHMgaW1wb3J0IHNlbmRf"
    "bWVzc2FnZQoKICAgICAgICBfZXh0cmEgPSAoCiAgICAgICAgICAgIGYiXG57X0NBVEFMT0dfTk9U"
    "RX1cbiIgaWYgX0NBVEFMT0dfTk9URSBlbHNlICIiCiAgICAgICAgKQogICAgICAgIG5vdGUgPSBh"
    "d2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgIG1lc3NhZ2UsCiAgICAgICAgICAgIGYi8J+O"
    "pyA8Yj57YXJ0aXN0fTwvYj4g4oCUIFNwb3RpZnkgdG9wIHtfbn0gdHJhY2socylcblxuIgogICAg"
    "ICAgICAgICBmIntfbGluZXN9XG57X2V4dHJhfVxuIgogICAgICAgICAgICAi8J+TpiBlYWNoIHNv"
    "bmcgaXMgc2VudCBhcyBzb29uIGFzIGl0IGZpbmlzaGVzICIKICAgICAgICAgICAgIndpdGggPGNv"
    "ZGU+LXo8L2NvZGU+IHlvdSBnZXQgb25lIHppcCBpbnN0ZWFkIiwKICAgICAgICApCiAgICBleGNl"
    "cHQgRXhjZXB0aW9uOgogICAgICAgIG5vdGUgPSBOb25lCiAgICBfYmFzZSA9IGludChnZXRhdHRy"
    "KG1lc3NhZ2UsICJpZCIsIDApIG9yIDApCiAgICAjIFdaRklYIHIxNSAodjE1Ljc0KTogdGhlIGZp"
    "cnN0IGNsb25lJ3MgZGlyIGlzIHRoZSBtZXJnZSB0YXJnZXQKICAgIF9zaGFyZWRbZiIve19mb2xk"
    "ZXJ9Il1bIl93emZpeCJdWyJsZWFkZXJfbWlkIl0gPSBfYmFzZSArIDEwMDAwMAogICAgaWYgIi16"
    "IiBpbiBfZmxhZ3M6CiAgICAgICAgX3NoYXJlZFtmIi97X2ZvbGRlcn0iXVsiX3d6Zml4Il1bIm1v"
    "ZGUiXSA9ICJ6aXAiCiAgICAjIFdaRklYIHIyNDogc3RyYXkgd29yZHMgdHlwZWQgYWZ0ZXIgdGhl"
    "IGFydGlzdCBsaW5rIHdlcmUKICAgICMgYmVpbmcgZ2x1ZWQgb250byBldmVyeSBZb3VUdWJlIHNl"
    "YXJjaC4gS2VlcCByZWFsIGZsYWdzIGFuZAogICAgIyB0aGVpciB2YWx1ZXMsIGRyb3AgdGhlIHJl"
    "c3Qgd2l0aCBhIG5vdGUgaW4gdGhlIGxvZy4KICAgIF92YWxmMjQgPSB7Ii1uIiwgIi1zIiwgIi1z"
    "ZCIsICItdGwiLCAiLXVsIiwgIi11cCJ9CiAgICBfa2VlcDI0ID0gW10KICAgIF9kcm9wMjQgPSBb"
    "XQogICAgX3Rva3MyNCA9IChfZmxhZ3Mgb3IgIiIpLnNwbGl0KCkKICAgIF9pMjQgPSAwCiAgICB3"
    "aGlsZSBfaTI0IDwgbGVuKF90b2tzMjQpOgogICAgICAgIF90MjQgPSBfdG9rczI0W19pMjRdCiAg"
    "ICAgICAgaWYgX3QyNCBpbiBfdmFsZjI0IGFuZCBfaTI0ICsgMSA8IGxlbihfdG9rczI0KToKICAg"
    "ICAgICAgICAgX2tlZXAyNC5hcHBlbmQoX3QyNCkKICAgICAgICAgICAgX2tlZXAyNC5hcHBlbmQo"
    "X3Rva3MyNFtfaTI0ICsgMV0pCiAgICAgICAgICAgIF9pMjQgKz0gMgogICAgICAgICAgICBjb250"
    "aW51ZQogICAgICAgIGlmIF90MjQuc3RhcnRzd2l0aCgiLSIpOgogICAgICAgICAgICBfa2VlcDI0"
    "LmFwcGVuZChfdDI0KQogICAgICAgIGVsc2U6CiAgICAgICAgICAgIF9kcm9wMjQuYXBwZW5kKF90"
    "MjQpCiAgICAgICAgX2kyNCArPSAxCiAgICBpZiBfZHJvcDI0OgogICAgICAgIF9sb2coIldaRklY"
    "IG11c2ljOiBpZ25vcmluZyBleHRyYSB3b3JkczogIiArICIgIi5qb2luKF9kcm9wMjQpKQogICAg"
    "ICAgIF9mbGFncyA9ICIgIi5qb2luKF9rZWVwMjQpCiAgICBfam9icyA9IF9mYW5vdXRfam9icygp"
    "CiAgICBfbG9nKAogICAgICAgIGYiV1pGSVggcjE1OiBmYW4tb3V0IHRhcmdldCB7X2pvYnN9IGNv"
    "bmN1cnJlbnQgZG93bmxvYWQocykgIgogICAgICAgICIoV1pGSVhfRkFOT1VUX0pPQlMpIgogICAg"
    "KQogICAgIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0KICAgICMgV1pGSVggcjI1YyAodjE1LjgzKTogUGhhc2UgQiDigJQg"
    "YSBiYXRjaCB3aXRoIG1vcmUgdGhhbiA2IHNvbmdzIGlzCiAgICAjIHNwbGl0IGFjcm9zcyBzaG9y"
    "dC1saXZlZCB3b3JrZXIgc2Vzc2lvbnMsIGVhY2ggZG93bmxvYWRpbmcgfjYKICAgICMgc29uZ3Mg"
    "d2l0aCA2IGxhbmVzIG9uIGl0cyBvd24gSVAuIFdvcmtlcnMgdXBsb2FkIHN0cmFpZ2h0IHRvIHRo"
    "ZQogICAgIyBjaGF0IGFuZCBleGl0OyBvbmx5IHRoaXMgbWFpbiBzZXNzaW9uIGtlZXBzIHJ1bm5p"
    "bmcuIERpc2FibGUgd2l0aAogICAgIyBXWkZJWF9QSEFTRUI9MC4gLXogKG9uZSB6aXApIGFsd2F5"
    "cyBzdGF5cyBzaW5nbGUtc2Vzc2lvbi4KICAgIF9yMjVfd29ya2VycyA9IFtdCiAgICBfcjI1X3Rv"
    "dGFsID0gX24KICAgIF9jaGF0MjUgPSAiIgogICAgX3RvazI1ID0gIiIKICAgIF93emZpeF9wYjI1"
    "ID0gTm9uZQogICAgaWYgKAogICAgICAgIF9uID4gNgogICAgICAgIGFuZCAiLXoiIG5vdCBpbiAo"
    "X2ZsYWdzIG9yICIiKQogICAgICAgIGFuZCBvcy5lbnZpcm9uLmdldCgiV1pGSVhfUEhBU0VCIiwg"
    "IjEiKSAhPSAiMCIKICAgICk6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBpbXBvcnQgYmFzZTY0"
    "IGFzIF9iNjRfMjUKCiAgICAgICAgICAgIGZyb20gLnIyNV93b3JrZXIgaW1wb3J0ICgKICAgICAg"
    "ICAgICAgICAgIGNvbGxlY3RfY29va2llIGFzIF9jYzI1LAogICAgICAgICAgICAgICAgZGlzcGF0"
    "Y2hfd29ya2VyIGFzIF9kdzI1LAogICAgICAgICAgICAgICAgd29ya2VyX3N0YXR1cyBhcyBfd3N0"
    "MjUsCiAgICAgICAgICAgICkKCiAgICAgICAgICAgICMgcjI1Yzg6IHVubGltaXRlZCBzY2FsaW5n"
    "IOKAlCBvbmUgd29ya2VyIHBlciA2IHNvbmdzCiAgICAgICAgICAgICMgKFdaRklYX1dPUktFUl9N"
    "QVggY2FwcyBpdDsgS2FnZ2xlIHJlamVjdHMgcHVzaGVzIGJleW9uZAogICAgICAgICAgICAjIGl0"
    "cyBjb25jdXJyZW5jeSBsaW1pdCBhbmQgdGhlIGxlYWRlciBhYnNvcmJzIHRob3NlKQogICAgICAg"
    "ICAgICBfd21heDI1ID0gaW50KG9zLmVudmlyb24uZ2V0KCJXWkZJWF9XT1JLRVJfTUFYIiwgIjEy"
    "IikpCiAgICAgICAgICAgIF9zMjUgPSBtaW4oLSgtX24gLy8gNiksIDEgKyBfd21heDI1KQogICAg"
    "ICAgICAgICBpZiBfczI1ID4gMToKICAgICAgICAgICAgICAgICMgV1pGSVggcjI1YzYgKHYxNS44"
    "My42KTogd29ya2VycyBkZWxpdmVyIGludG8gdGhlIGxvZwogICAgICAgICAgICAgICAgIyBjaGF0"
    "IGZyb20gdGhlIGJvdCBzZXR0aW5ncyAoTW9uZ29EQi1iYWNrZWQgQ29uZmlnKSDigJQKICAgICAg"
    "ICAgICAgICAgICMgdGhlIGNvcnJlY3QgY2xhc3MgbGl2ZXMgaW4gYm90L2NvcmUvY29uZmlnX21h"
    "bmFnZXIKICAgICAgICAgICAgICAgICMgKHRoZSBvbGQgYGZyb20gLi4uY29uZmlnIGltcG9ydGAg"
    "ZmFpbGVkIHNpbGVudGx5LAogICAgICAgICAgICAgICAgIyB3aGljaCBpcyB3aHkgdGhlIGxvZyBj"
    "aGF0IGFsd2F5cyBjYW1lIHVwIGVtcHR5KS4KICAgICAgICAgICAgICAgIF9jaGF0MjUgPSAiIgog"
    "ICAgICAgICAgICAgICAgX3RvazI1ID0gIiIKICAgICAgICAgICAgICAgIHRyeToKICAgICAgICAg"
    "ICAgICAgICAgICBmcm9tIC4uLmNvcmUuY29uZmlnX21hbmFnZXIgaW1wb3J0IENvbmZpZyBhcyBf"
    "QzI1CgogICAgICAgICAgICAgICAgICAgIF9jaGF0MjUgPSBzdHIoCiAgICAgICAgICAgICAgICAg"
    "ICAgICAgIGdldGF0dHIoX0MyNSwgIkxFRUNIX0xPR19DSEFUIiwgIiIpCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgIG9yIGdldGF0dHIoX0MyNSwgIkxFRUNIX0RVTVBfQ0hBVCIsICIiKQogICAgICAg"
    "ICAgICAgICAgICAgICAgICBvciAiIgogICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAg"
    "ICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAg"
    "ICAgICAgaWYgbm90IF9jaGF0MjU6CiAgICAgICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAg"
    "ICAgICAgICAgICAgICBfY2hhdDI1ID0gc3RyKG1lc3NhZ2UuY2hhdC5pZCkKICAgICAgICAgICAg"
    "ICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAgICAgICBwYXNzCiAg"
    "ICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgZnJvbSAuLi5jb3JlLmNvbmZp"
    "Z19tYW5hZ2VyIGltcG9ydCBDb25maWcgYXMgX0MyNQoKICAgICAgICAgICAgICAgICAgICBfdG9r"
    "MjUgPSBzdHIoZ2V0YXR0cihfQzI1LCAiQk9UX1RPS0VOIiwgIiIpIG9yICIiKQogICAgICAgICAg"
    "ICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgICAgICBwYXNzCiAgICAgICAg"
    "ICAgICAgICBfY2syNSA9IGF3YWl0IF9jYzI1KF91c2VyX29mKG1lc3NhZ2UpKQogICAgICAgICAg"
    "ICAgICAgIyByMjVjODogcnVuIGlkIGZvciB0aGUgTW9uZ28gc3RhdHVzIGNoYW5uZWwgKGFsbnVt"
    "CiAgICAgICAgICAgICAgICAjIG9ubHkg4oCUIGl0IGJlY29tZXMgYSAkcmVnZXggcHJlZml4KQog"
    "ICAgICAgICAgICAgICAgaW1wb3J0IHRpbWUgYXMgX3QyNXIKCiAgICAgICAgICAgICAgICBfcnVu"
    "aWQyNSA9ICgKICAgICAgICAgICAgICAgICAgICAiIi5qb2luKGNoIGZvciBjaCBpbiBhcnRpc3Qg"
    "aWYgY2guaXNhbG51bSgpKVs6MTZdCiAgICAgICAgICAgICAgICAgICAgKyBzdHIoaW50KF90MjVy"
    "LnRpbWUoKSkpCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICBfam9iczI1ID0gW10K"
    "ICAgICAgICAgICAgICAgIGZvciBfazI1IGluIHJhbmdlKDEsIF9zMjUpOgogICAgICAgICAgICAg"
    "ICAgICAgIF9zbDI1ID0gdHJhY2tzW19rMjU6Ol9zMjVdCiAgICAgICAgICAgICAgICAgICAgaWYg"
    "X3NsMjU6CiAgICAgICAgICAgICAgICAgICAgICAgIF9qb2JzMjUuYXBwZW5kKChfazI1LCBfc2wy"
    "NSkpCiAgICAgICAgICAgICAgICAjIHIyNWMxMDogS2FnZ2xlIHN0YXJ0cyBvbmx5IH41IHNlc3Np"
    "b25zIGF0IGEgdGltZQogICAgICAgICAgICAgICAgIyAobGVhZGVyICsgNCB3b3JrZXJzKTsgcHVz"
    "aGVzIGJleW9uZCB0aGF0IHNpdAogICAgICAgICAgICAgICAgIyB1bnN0YXJ0ZWQgZm9yZXZlciAo"
    "dGhlIGxvZ180IHJ1bidzIHc1Li53MTAgbmV2ZXIKICAgICAgICAgICAgICAgICMgY2FtZSB1cCku"
    "IFdvcmtlcnMgYXJlIHRoZXJlZm9yZSBkaXNwYXRjaGVkIGluIFdBVkVTOgogICAgICAgICAgICAg"
    "ICAgIyB0aGUgbmV4dCBrZXJuZWwgZ29lcyBvdXQgb25seSB3aGVuIGEgc2xvdCBmcmVlcy4KICAg"
    "ICAgICAgICAgICAgIF93Y29uYzI1ID0gbWF4KAogICAgICAgICAgICAgICAgICAgIDEsIGludChv"
    "cy5lbnZpcm9uLmdldCgiV1pGSVhfV09SS0VSX0NPTkMiLCAiNCIpKQogICAgICAgICAgICAgICAg"
    "KQogICAgICAgICAgICAgICAgX2xvZygKICAgICAgICAgICAgICAgICAgICBmIldaRklYIHIyNWM6"
    "IFBoYXNlIEIg4oCUIHtfcjI1X3RvdGFsfSBzb25ncyAtPiAiCiAgICAgICAgICAgICAgICAgICAg"
    "ZiJ7bGVuKF9qb2JzMjUpfSB3b3JrZXIocykgaW4gd2F2ZXMgb2YgIgogICAgICAgICAgICAgICAg"
    "ICAgIGYie193Y29uYzI1fSArIGxlYWRlciBzbGljZSIKICAgICAgICAgICAgICAgICkKCiAgICAg"
    "ICAgICAgICAgICBhc3luYyBkZWYgX3B1c2hfd29ya2VyMjUoX2syNSwgX3NsMjUpOgogICAgICAg"
    "ICAgICAgICAgICAgICIiIkRpc3BhdGNoIG9uZSB3b3JrZXIga2VybmVsOiBvayAvIGJ1c3kgLyBm"
    "YWlsLiIiIgogICAgICAgICAgICAgICAgICAgIGlmIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKF93"
    "c3QyNSwgX2syNSkgPT0gInJ1biI6CiAgICAgICAgICAgICAgICAgICAgICAgIF9sb2coCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICBmIldaRklYIHIyNWM6IHdvcmtlciB7X2syNX0gc3RpbGwg"
    "YnVzeSAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBmImZyb20gYSBwcmV2aW91cyBiYXRj"
    "aCIKICAgICAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgICAgICByZXR1"
    "cm4gImJ1c3kiCiAgICAgICAgICAgICAgICAgICAgX2pvYjI1ID0gewogICAgICAgICAgICAgICAg"
    "ICAgICAgICAiYXJ0aXN0IjogYXJ0aXN0LAogICAgICAgICAgICAgICAgICAgICAgICAid29ya2Vy"
    "IjogX2syNSwKICAgICAgICAgICAgICAgICAgICAgICAgInRpdGxlcyI6IFtfdDI1IGZvciBfLCBf"
    "dDI1IGluIF9zbDI1XSwKICAgICAgICAgICAgICAgICAgICAgICAgImNoYXRfaWQiOiBfY2hhdDI1"
    "LAogICAgICAgICAgICAgICAgICAgICAgICAiYm90X3Rva2VuIjogX3RvazI1LAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAiY29va2llX2I2NCI6ICgKICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "IF9iNjRfMjUuYjY0ZW5jb2RlKF9jazI1KS5kZWNvZGUoKQogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgaWYgX2NrMjUKICAgICAgICAgICAgICAgICAgICAgICAgICAgIGVsc2UgIiIKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgKSwKICAgICAgICAgICAgICAgICAgICAgICAgInJ1bl9pZCI6IF9y"
    "dW5pZDI1LAogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICBfb2syNSwg"
    "X21zZzI1ID0gYXdhaXQgYXN5bmNpby50b190aHJlYWQoCiAgICAgICAgICAgICAgICAgICAgICAg"
    "IF9kdzI1LCBfazI1LCBfam9iMjUKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAg"
    "ICAgICAgaWYgX29rMjU6CiAgICAgICAgICAgICAgICAgICAgICAgIF9yMjVfd29ya2Vycy5hcHBl"
    "bmQoX2syNSkKICAgICAgICAgICAgICAgICAgICAgICAgX2xvZygKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgIGYiV1pGSVggcjI1Yzogd29ya2VyIHtfazI1fSBwdXNoZWQgIgogICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgZiIoe2xlbihfc2wyNSl9IHNvbmdzKSIKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gIm9rIgogICAgICAgICAg"
    "ICAgICAgICAgIF9sb2coCiAgICAgICAgICAgICAgICAgICAgICAgIGYiV1pGSVggcjI1Yzogd29y"
    "a2VyIHtfazI1fSBwdXNoIEZBSUxFRCAiCiAgICAgICAgICAgICAgICAgICAgICAgIGYiKHtfbXNn"
    "MjV9KSIKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgcmV0dXJuICJm"
    "YWlsIgoKICAgICAgICAgICAgICAgIF9wZW5kaW5nMjUgPSBsaXN0KF9qb2JzMjUpCiAgICAgICAg"
    "ICAgICAgICBfYWJzb3JiMjUgPSBbXQogICAgICAgICAgICAgICAgd2hpbGUgKAogICAgICAgICAg"
    "ICAgICAgICAgIF9wZW5kaW5nMjUgYW5kIGxlbihfcjI1X3dvcmtlcnMpIDwgX3djb25jMjUKICAg"
    "ICAgICAgICAgICAgICk6CiAgICAgICAgICAgICAgICAgICAgX2syNSwgX3NsMjUgPSBfcGVuZGlu"
    "ZzI1LnBvcCgwKQogICAgICAgICAgICAgICAgICAgIGlmIGF3YWl0IF9wdXNoX3dvcmtlcjI1KF9r"
    "MjUsIF9zbDI1KSAhPSAib2siOgogICAgICAgICAgICAgICAgICAgICAgICAjIGJlZm9yZSB0aGUg"
    "bGVhZGVyIHN0YXJ0cywgdW5kZWxpdmVyYWJsZQogICAgICAgICAgICAgICAgICAgICAgICAjIHNv"
    "bmdzIHNpbXBseSBqb2luIHRoZSBsZWFkZXIgc2xpY2UKICAgICAgICAgICAgICAgICAgICAgICAg"
    "X2Fic29yYjI1LmV4dGVuZCgKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF90MjUgZm9yIF8s"
    "IF90MjUgaW4gX3NsMjUKICAgICAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAg"
    "aWYgX3IyNV93b3JrZXJzOgogICAgICAgICAgICAgICAgICAgIF90cmFja3MyNV9mdWxsID0gdHJh"
    "Y2tzCiAgICAgICAgICAgICAgICAgICAgX2xzbGljZTI1ID0gWwogICAgICAgICAgICAgICAgICAg"
    "ICAgICBfYzI1dCBmb3IgXywgX2MyNXQgaW4gdHJhY2tzWzA6Ol9zMjVdCiAgICAgICAgICAgICAg"
    "ICAgICAgXSArIF9hYnNvcmIyNQogICAgICAgICAgICAgICAgICAgIHRyYWNrcyA9IHRyYWNrc1sw"
    "OjpfczI1XQogICAgICAgICAgICAgICAgICAgIF9uID0gbGVuKHRyYWNrcykKICAgICAgICAgICAg"
    "ICAgICAgICBfbG9nKAogICAgICAgICAgICAgICAgICAgICAgICBmIldaRklYIHIyNWM6IFBoYXNl"
    "IEIgYWN0aXZlIOKAlCBsZWFkZXIgc2xpY2UgIgogICAgICAgICAgICAgICAgICAgICAgICBmIntf"
    "bn0sIHdvcmtlcnMge19yMjVfd29ya2Vyc30sIHRvdGFsICIKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgZiJ7X3IyNV90b3RhbH0iCiAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAg"
    "ICAgICMgV1pGSVggcjI1Yzg6IHRoZSBsZWFkZXIgc2xpY2UgZ29lcyB0aHJvdWdoIHRoZQogICAg"
    "ICAgICAgICAgICAgICAgICMgU0FNRSB3b3JrZXIgcGF0aCBhcyB0aGUgd29ya2VycyAoY2xlYW4g"
    "YXVkaW8tb25seQogICAgICAgICAgICAgICAgICAgICMgYnVyc3QsIG5vIHBlci1zb25nIHRhc2sg"
    "bWVzc2FnZXMpIOKAlCBydW4gYXMgYQogICAgICAgICAgICAgICAgICAgICMgYmFja2dyb3VuZCB0"
    "YXNrIHNvIHRoZSBjYXRhbG9nIGNhcmQgYmVsb3cgY2FuIGJlCiAgICAgICAgICAgICAgICAgICAg"
    "IyBlZGl0ZWQgd2hpbGUgaXQgd29ya3MuCiAgICAgICAgICAgICAgICAgICAgdHJ5OgogICAgICAg"
    "ICAgICAgICAgICAgICAgICBmcm9tIC5yMjVfd29ya2VyIGltcG9ydCBydW5fd29ya2VyX2pvYiBh"
    "cyBfcncyNQoKICAgICAgICAgICAgICAgICAgICAgICAgX2x0YXNrMjUgPSBhc3luY2lvLmNyZWF0"
    "ZV90YXNrKAogICAgICAgICAgICAgICAgICAgICAgICAgICAgX3J3MjUoCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAi"
    "YXJ0aXN0IjogYXJ0aXN0LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAid29y"
    "a2VyIjogMCwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInRpdGxlcyI6IGxp"
    "c3QoX2xzbGljZTI1KSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImNoYXRf"
    "aWQiOiBfY2hhdDI1LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiYm90X3Rv"
    "a2VuIjogX3RvazI1LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiY29va2ll"
    "X2I2NCI6ICgKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9iNjRfMjUu"
    "YjY0ZW5jb2RlKF9jazI1KS5kZWNvZGUoKQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgaWYgX2NrMjUKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "IGVsc2UgIiIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgKSwKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgInJ1bl9pZCI6IF9ydW5pZDI1LAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgICkKICAg"
    "ICAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgICAgICB0cmFja3MgPSBb"
    "XQogICAgICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgX2UyNToKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgX2xvZygKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJXWkZJWCBy"
    "MjVjODogbGVhZGVyIHdvcmtlci1wYXRoIHNldHVwICIKICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgIGYiZmFpbGVkICh7X2UyNX0pIOKAlCBjbG9uZSBmYW4tb3V0IGZhbGxiYWNrIgogICAgICAg"
    "ICAgICAgICAgICAgICAgICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBfZTI1OgogICAg"
    "ICAgICAgICBfbG9nKAogICAgICAgICAgICAgICAgZiJXWkZJWCByMjVjOiBQaGFzZSBCIGRpc3Bh"
    "dGNoIGVycm9yICh7X2UyNX0pIOKAlCAiCiAgICAgICAgICAgICAgICAic2luZ2xlLXNlc3Npb24g"
    "ZmFsbGJhY2siCiAgICAgICAgICAgICkKICAgIF9pbmZsaWdodDI0ID0gc2V0KCkKICAgIGZvciBp"
    "LCAocXVlcnksIGNsZWFuKSBpbiBlbnVtZXJhdGUodHJhY2tzKToKICAgICAgICB0cnk6CiAgICAg"
    "ICAgICAgICMgV1pGSVggcjE1OiBsYXVuY2ggdGhlIG5leHQgY2xvbmUgb25seSB3aGVuIGEgZG93"
    "bmxvYWQKICAgICAgICAgICAgIyBzbG90IGlzIGZyZWUg4oCUIHNlbGYtcmVndWxhdGluZywgbm8g"
    "cXVldWUgYm90dGxlbmVjayBhbmQKICAgICAgICAgICAgIyBubyBwZXItSVAgaGFtbWVyaW5nIGJl"
    "eW9uZCB0aGUgdGFyZ2V0CiAgICAgICAgICAgIHdoaWxlIFRydWU6CiAgICAgICAgICAgICAgICBp"
    "ZiBsZW4oX2luZmxpZ2h0MjQpIDwgX2pvYnM6CiAgICAgICAgICAgICAgICAgICAgYnJlYWsKICAg"
    "ICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8uc2xlZXAoMC41KQogICAgICAgICAgICBtMiA9IF9j"
    "b3B5LmNvcHkobWVzc2FnZSkKICAgICAgICAgICAgIyBweXJvZ3JhbSdzIE1lc3NhZ2UgY29weSBk"
    "cm9wcyB0aGUgY2xpZW50IGJpbmRpbmcg4oCUCiAgICAgICAgICAgICMgcmUtYXR0YWNoIGl0IG9y"
    "IGV2ZXJ5IHJlcGx5IG9uIHRoZSBjbG9uZSBmYWlscwogICAgICAgICAgICBtMi5fY2xpZW50ID0g"
    "KAogICAgICAgICAgICAgICAgZ2V0YXR0cihtZXNzYWdlLCAiX2NsaWVudCIsIE5vbmUpIG9yIGNs"
    "aWVudAogICAgICAgICAgICApCiAgICAgICAgICAgIG0yLmlkID0gX2Jhc2UgKyAxMDAwMDAgKyBp"
    "CiAgICAgICAgICAgICMgLWYgKGZvcmNlIHJ1bikgYnlwYXNzZXMgdGhlIGJvdCdzIGRvd25sb2Fk"
    "IHF1ZXVlOiB0aGUKICAgICAgICAgICAgIyByMyBsYXVuY2hlciBhYm92ZSBpcyB0aGUgb25seSBj"
    "b25jdXJyZW5jeSBsaW1pdAogICAgICAgICAgICBtMi50ZXh0ID0gKAogICAgICAgICAgICAgICAg"
    "ZiIveWwge3F1ZXJ5fSB7X2ZsYWdzfSAtbSB7X2ZvbGRlcn0gLWkgMSAtZiIKICAgICAgICAgICAg"
    "KS5zdHJpcCgpCiAgICAgICAgICAgIG0yLnRleHQgPSBtMi50ZXh0LnJlcGxhY2UoIiAgIiwgIiAi"
    "KQogICAgICAgICAgICBtMi5jYXB0aW9uID0gTm9uZQogICAgICAgICAgICBtMi5yZXBseV90b19t"
    "ZXNzYWdlID0gTm9uZQogICAgICAgICAgICBtMi5fd3pmaXhfbXVzaWNfdGl0bGUgPSBjbGVhbgog"
    "ICAgICAgICAgICBtMi5fd3pmaXhfcXVlcnkyNSA9IHF1ZXJ5CiAgICAgICAgICAgIF9taWRzLmFw"
    "cGVuZChtMi5pZCkKCiAgICAgICAgICAgIGFzeW5jIGRlZiBfZ28oX209bTIpOgogICAgICAgICAg"
    "ICAgICAgdHJ5OgogICAgICAgICAgICAgICAgICAgIGF3YWl0IFl0RGxwKAogICAgICAgICAgICAg"
    "ICAgICAgICAgICBjbGllbnQsCiAgICAgICAgICAgICAgICAgICAgICAgIF9tLAogICAgICAgICAg"
    "ICAgICAgICAgICAgICBpc19sZWVjaD1ib29sKGlzX2xlZWNoKSwKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgc2FtZV9kaXI9X3NoYXJlZCwKICAgICAgICAgICAgICAgICAgICApLm5ld19ldmVudCgp"
    "CiAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgICAgICAg"
    "ICAgIyBXWkZJWCByMjViOiBvbmUgcmV0cnkgd2l0aCBkaWZmZXJlbnQgc2VhcmNoCiAgICAgICAg"
    "ICAgICAgICAgICAgIyB3b3JkaW5nIGJlZm9yZSBkZWNsYXJpbmcgdGhlIHNvbmcgdW5hdmFpbGFi"
    "bGUg4oCUCiAgICAgICAgICAgICAgICAgICAgIyBtYW55ICJ1bmF2YWlsYWJsZSIgdHJhY2tzIGFy"
    "ZSBqdXN0IGJhZCBoaXRzCiAgICAgICAgICAgICAgICAgICAgaWYgbm90IGdldGF0dHIoX20sICJf"
    "d3pmaXhfcmV0cnkyNSIsIEZhbHNlKToKICAgICAgICAgICAgICAgICAgICAgICAgdHJ5OgogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgX20zID0gX2NvcHkuY29weShfbSkKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgIF9tMy5pZCA9IF9tLmlkICsgMTAwMDAwCiAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICBfcTI1ID0gZ2V0YXR0cihfbSwgIl93emZpeF9xdWVyeTI1IiwgIiIpCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICBpZiBfcTI1OgogICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIF9tMy50ZXh0ID0gX20udGV4dC5yZXBsYWNlKAogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICBfcTI1LCBfcTI1ICsgIiBhdWRpbyIsIDEKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfbTMuX3d6Zml4X3Jl"
    "dHJ5MjUgPSBUcnVlCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfbWlkcy5hcHBlbmQoX20z"
    "LmlkKQogICAgICAgICAgICAgICAgICAgICAgICAgICAgX2luZmxpZ2h0MjQuYWRkKF9tMy5pZCkK"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9sb2coCiAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgIldaRklYIHIyNWI6IHJldHJ5IHdpdGggYXVkaW8tdmFyaWFudCAiCiAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgZiJzZWFyY2g6IHtfbTMudGV4dFs6ODBdfSIKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFzeW5j"
    "aW8uY3JlYXRlX3Rhc2soX2dvKF9tMykpCiAgICAgICAgICAgICAgICAgICAgICAgIGV4Y2VwdCBF"
    "eGNlcHRpb246CiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfd3pfcjI0X2ZhaWwoX3NoYXJl"
    "ZCwgX2ZvbGRlciwgX20sIGUpCiAgICAgICAgICAgICAgICAgICAgZWxzZToKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgX3d6X3IyNF9mYWlsKF9zaGFyZWQsIF9mb2xkZXIsIF9tLCBlKQogICAgICAg"
    "ICAgICAgICAgZmluYWxseToKICAgICAgICAgICAgICAgICAgICBfaW5mbGlnaHQyNC5kaXNjYXJk"
    "KF9tLmlkKQoKICAgICAgICAgICAgX2luZmxpZ2h0MjQuYWRkKG0yLmlkKQogICAgICAgICAgICBh"
    "c3luY2lvLmNyZWF0ZV90YXNrKF9nbygpKQogICAgICAgICAgICBhd2FpdCBhc3luY2lvLnNsZWVw"
    "KDAuMjUpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICBfbG9nKGYi"
    "V1pGSVggbXVzaWM6IGFydGlzdCBmYW4tb3V0IGRpc3BhdGNoIGZhaWxlZDoge2V9IikKICAgICAg"
    "ICAgICAgX2luZmxpZ2h0MjQucG9wKF9iYXNlICsgMTAwMDAwICsgaSwgTm9uZSkKICAgIF9sb2co"
    "CiAgICAgICAgZiJXWkZJWCBtdXNpYzogYXJ0aXN0IGZhbi1vdXQ6IHtfbn0gdHJhY2socykgcXVl"
    "dWVkIGZvciAiCiAgICAgICAgZiJ7YXJ0aXN0fSAoZm9sZGVyIHtfZm9sZGVyfSwgZmxhZ3MgJ3tf"
    "ZmxhZ3N9JykiCiAgICApCiAgICBpZiBfcjI1X3dvcmtlcnMgYW5kIG5vdCBfbWlkcyBhbmQgbm90"
    "ZSBpcyBub3QgTm9uZToKCiAgICAgICAgIyBXWkZJWCByMjVjODogUGhhc2UgQiB3aXRoIHRoZSBs"
    "ZWFkZXIgc2xpY2Ugb24gdGhlIHdvcmtlcgogICAgICAgICMgcGF0aCDigJQgbm8gY2xvbmUgd2F0"
    "Y2hlciByYW4uIFRoZSBjYXRhbG9nIGNhcmQgKHRoZSBudW1iZXJlZAogICAgICAgICMgbGlzdCBu"
    "b3RlKSBpcyBPTkUgbWVzc2FnZSwgZWRpdGVkIHdpdGggbGl2ZSBtYXJrcyB3aGlsZSB0aGUKICAg"
    "ICAgICAjIGxlYWRlciBzbGljZSBhbmQgdGhlIHdvcmtlcnMgcnVuOiDinIUgZGVsaXZlcmVkLCDi"
    "nYwgY3Jvc3NlZAogICAgICAgICMgb3V0ID0gZmFpbGVkIGFmdGVyIGFsbCByZXRyaWVzLiBQcm9n"
    "cmVzcyBjb21lcyB0aHJvdWdoIHRoZQogICAgICAgICMgTW9uZ28gc3RhdHVzIGNoYW5uZWwsIHNv"
    "IHRoZXJlIGFyZSBubyBwZXItd29ya2VyIG1lc3NhZ2VzLgogICAgICAgIGZyb20gLnIyNV93b3Jr"
    "ZXIgaW1wb3J0ICgKICAgICAgICAgICAgY2xlYW51cF9zdGF0dXMgYXMgX2NsbjI1LAogICAgICAg"
    "ICAgICBmZXRjaF9zdGF0dXMgYXMgX2YyNXMsCiAgICAgICAgICAgIHJlcG9ydF9zdGF0dXMgYXMg"
    "X3JwdDI1LAogICAgICAgICkKCiAgICAgICAgYXN5bmMgZGVmIF9lZGl0X2NhcmQyNShfc3VtbWFy"
    "eTI1PU5vbmUpOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBfc3QyNXggPSBhd2Fp"
    "dCBhc3luY2lvLnRvX3RocmVhZChfZjI1cywgX3J1bmlkMjUpCiAgICAgICAgICAgIGV4Y2VwdCBF"
    "eGNlcHRpb246CiAgICAgICAgICAgICAgICBfc3QyNXggPSB7fQogICAgICAgICAgICBfc29uZ3My"
    "NSA9IF9zdDI1eC5nZXQoInNvbmdzIikgb3Ige30KICAgICAgICAgICAgX29rbjI1ID0gMAogICAg"
    "ICAgICAgICBfbG5zMjUgPSBbXQogICAgICAgICAgICBmb3IgX2kyNSwgKF9xMjUsIF9jMjV0KSBp"
    "biBlbnVtZXJhdGUoX3RyYWNrczI1X2Z1bGwsIDEpOgogICAgICAgICAgICAgICAgX2QyNSA9IF9z"
    "b25nczI1LmdldChfYzI1dCkKICAgICAgICAgICAgICAgIGlmIF9kMjUgYW5kIF9kMjUuZ2V0KCJz"
    "dCIpID09ICJvayI6CiAgICAgICAgICAgICAgICAgICAgX2xuczI1LmFwcGVuZChmIntfaTI1fS4g"
    "4pyFIHtfYzI1dFs6NTVdfSIpCiAgICAgICAgICAgICAgICAgICAgX29rbjI1ICs9IDEKICAgICAg"
    "ICAgICAgICAgIGVsaWYgX2QyNSBhbmQgX2QyNS5nZXQoInN0IikgPT0gImZhaWwiOgogICAgICAg"
    "ICAgICAgICAgICAgICMgcjI1YzEzOiBzaG93IFdIWSBuZXh0IHRvIHRoZSBzb25nCiAgICAgICAg"
    "ICAgICAgICAgICAgX3IyNXggPSAoX2QyNS5nZXQoInIiKSBvciAiIikuc3RyaXAoKVs6NDBdCiAg"
    "ICAgICAgICAgICAgICAgICAgX2xuczI1LmFwcGVuZCgKICAgICAgICAgICAgICAgICAgICAgICAg"
    "ZiJ7X2kyNX0uIOKdjCA8cz57X2MyNXRbOjU1XX08L3M+IgogICAgICAgICAgICAgICAgICAgICAg"
    "ICArIChmIiDigJQge19yMjV4fSIgaWYgX3IyNXggZWxzZSAiIikKICAgICAgICAgICAgICAgICAg"
    "ICApCiAgICAgICAgICAgICAgICBlbGlmIF9kMjUgYW5kIF9kMjUuZ2V0KCJzdCIpID09ICJ1cCI6"
    "CiAgICAgICAgICAgICAgICAgICAgX2xuczI1LmFwcGVuZCgKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgZiJ7X2kyNX0uIOKPqyB7X2MyNXRbOjU1XX0gKHVwbG9hZGluZykiCiAgICAgICAgICAgICAg"
    "ICAgICAgKQogICAgICAgICAgICAgICAgZWxpZiBfZDI1IGFuZCBfZDI1LmdldCgic3QiKSA9PSAi"
    "d3IiOgogICAgICAgICAgICAgICAgICAgICMgd2FpdGluZyAvIHJldHJ5aW5nIOKAlCB3aXRoIHRo"
    "ZSByZWFzb24KICAgICAgICAgICAgICAgICAgICBfcjI1eCA9IChfZDI1LmdldCgiciIpIG9yICIi"
    "KS5zdHJpcCgpWzo0MF0KICAgICAgICAgICAgICAgICAgICBfbG5zMjUuYXBwZW5kKAogICAgICAg"
    "ICAgICAgICAgICAgICAgICBmIntfaTI1fS4g4o+zIHtfYzI1dFs6NTVdfSIKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgZiIg4oCUIHtfcjI1eCBvciAnd2FpdGluZyd9IgogICAgICAgICAgICAgICAg"
    "ICAgICkKICAgICAgICAgICAgICAgIGVsaWYgX2QyNSBhbmQgX2QyNS5nZXQoInN0IikgPT0gImRs"
    "IjoKICAgICAgICAgICAgICAgICAgICBfbG5zMjUuYXBwZW5kKAogICAgICAgICAgICAgICAgICAg"
    "ICAgICBmIntfaTI1fS4g4qyHIHtfYzI1dFs6NTVdfSAoZG93bmxvYWRpbmcpIgogICAgICAgICAg"
    "ICAgICAgICAgICkKICAgICAgICAgICAgICAgIGVsc2U6CiAgICAgICAgICAgICAgICAgICAgX2xu"
    "czI1LmFwcGVuZChmIntfaTI1fS4ge19jMjV0Wzo1NV19IikKICAgICAgICAgICAgX3dsMjUgPSAi"
    "IgogICAgICAgICAgICBpZiBfcjI1X3dvcmtlcnM6CiAgICAgICAgICAgICAgICBfcHMyNSA9IFtd"
    "CiAgICAgICAgICAgICAgICBmb3IgX3cyNSBpbiBfcjI1X3dvcmtlcnM6CiAgICAgICAgICAgICAg"
    "ICAgICAgX21kMjUgPSBfc3QyNXguZ2V0KGYibWV0YXtfdzI1fSIpIG9yIHt9CiAgICAgICAgICAg"
    "ICAgICAgICAgX3BzMjUuYXBwZW5kKAogICAgICAgICAgICAgICAgICAgICAgICBmInd7X3cyNX0g"
    "e19tZDI1LmdldCgnZCcsIDApfS8iCiAgICAgICAgICAgICAgICAgICAgICAgIGYie19tZDI1Lmdl"
    "dCgnbicsICc/Jyl9IHtfbWQyNS5nZXQoJ3BoJykgb3IgJ+KApid9IgogICAgICAgICAgICAgICAg"
    "ICAgICkKICAgICAgICAgICAgICAgIF93bDI1ID0gIlxu4pqZICIgKyAiIMK3ICIuam9pbihfcHMy"
    "NSkKICAgICAgICAgICAgX3R4dDI1ID0gKAogICAgICAgICAgICAgICAgZiLwn46nIDxiPnthcnRp"
    "c3R9PC9iPiDigJQge19yMjVfdG90YWx9IHNvbmcocykiCiAgICAgICAgICAgICAgICBmIiDCtyDi"
    "nIUge19va24yNX0iCiAgICAgICAgICAgICAgICBmIntfd2wyNX1cblxuIgogICAgICAgICAgICAg"
    "ICAgKyAiXG4iLmpvaW4oX2xuczI1KQogICAgICAgICAgICApCiAgICAgICAgICAgIGlmIF9zdW1t"
    "YXJ5MjU6CiAgICAgICAgICAgICAgICBfdHh0MjUgKz0gZiJcblxue19zdW1tYXJ5MjV9IgogICAg"
    "ICAgICAgICAjIDQwOTYgaXMgdGhlIGhhcmQgbWVzc2FnZSBsaW1pdCDigJQgY29sbGFwc2UgdGhl"
    "IHBlbmRpbmcKICAgICAgICAgICAgIyB0YWlsIGlmIHRoZSBjYXRhbG9nIGFsb25lIHdvdWxkIG92"
    "ZXJmbG93CiAgICAgICAgICAgIGlmIGxlbihfdHh0MjUpID4gNDAwMDoKICAgICAgICAgICAgICAg"
    "IF9tYXJrZWQyNSA9IFsKICAgICAgICAgICAgICAgICAgICBsCiAgICAgICAgICAgICAgICAgICAg"
    "Zm9yIGwgaW4gX2xuczI1CiAgICAgICAgICAgICAgICAgICAgaWYgbC5zcGxpdCgiLiAiLCAxKVst"
    "MV0uc3RhcnRzd2l0aCgKICAgICAgICAgICAgICAgICAgICAgICAgKCLinIUiLCAi4p2MIiwgIuKP"
    "qyIsICLij7MiLCAi4qyHIikKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICBd"
    "CiAgICAgICAgICAgICAgICBfcGVuZDI1ID0gbGVuKF9sbnMyNSkgLSBsZW4oX21hcmtlZDI1KQog"
    "ICAgICAgICAgICAgICAgX3R4dDI1ID0gKAogICAgICAgICAgICAgICAgICAgIGYi8J+OpyA8Yj57"
    "YXJ0aXN0fTwvYj4g4oCUIHtfcjI1X3RvdGFsfSBzb25nKHMpIgogICAgICAgICAgICAgICAgICAg"
    "IGYiIMK3IOKchSB7X29rbjI1fSIKICAgICAgICAgICAgICAgICAgICBmIntfd2wyNX1cblxuIgog"
    "ICAgICAgICAgICAgICAgICAgICsgIlxuIi5qb2luKF9tYXJrZWQyNVs6NzBdKQogICAgICAgICAg"
    "ICAgICAgICAgICsgKGYiXG7igKYgKCt7X3BlbmQyNX0gcGVuZGluZykiIGlmIF9wZW5kMjUgZWxz"
    "ZSAiIikKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIGlmIF9zdW1tYXJ5MjU6CiAg"
    "ICAgICAgICAgICAgICAgICAgX3R4dDI1ICs9IGYiXG5cbntfc3VtbWFyeTI1fSIKICAgICAgICAg"
    "ICAgdHJ5OgogICAgICAgICAgICAgICAgYXdhaXQgbm90ZS5lZGl0X3RleHQoX3R4dDI1Wzo0MDkw"
    "XSkKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKCiAg"
    "ICAgICAgYXN5bmMgZGVmIF93YXRjaF9wYigpOgogICAgICAgICAgICBpbXBvcnQgdGltZSBhcyBf"
    "dGltZQoKICAgICAgICAgICAgIyAxLiB3YWl0IGZvciB0aGUgbGVhZGVyIHNsaWNlIChlZGl0aW5n"
    "IHRoZSBjYXJkIHdoaWxlIGl0CiAgICAgICAgICAgICMgICAgZG93bmxvYWRzL3NlbmRzKQogICAg"
    "ICAgICAgICBfbHJlcyA9IE5vbmUKICAgICAgICAgICAgX2xkZWFkMjUgPSA0MDAgICMgfjgwIG1p"
    "biBzYWZldHkgY2FwIG9uIHRoZSBsZWFkZXIgd2FpdAogICAgICAgICAgICB3aGlsZSBfbHJlcyBp"
    "cyBOb25lOgogICAgICAgICAgICAgICAgX2xkZWFkMjUgLT0gMQogICAgICAgICAgICAgICAgaWYg"
    "X2xkZWFkMjUgPD0gMDoKICAgICAgICAgICAgICAgICAgICBfbG9nKCJXWkZJWCByMjVjODogbGVh"
    "ZGVyIHdhaXQgdGltZWQgb3V0IikKICAgICAgICAgICAgICAgICAgICBfbHJlcyA9IHsib2siOiBb"
    "XSwgImZhaWwiOiBbXSwgImlkcyI6IFtdfQogICAgICAgICAgICAgICAgICAgIGJyZWFrCiAgICAg"
    "ICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgX2xyZXMgPSBhd2FpdCBhc3luY2lv"
    "LndhaXRfZm9yKAogICAgICAgICAgICAgICAgICAgICAgICBhc3luY2lvLnNoaWVsZChfbHRhc2sy"
    "NSksIHRpbWVvdXQ9MTIKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICBleGNl"
    "cHQgYXN5bmNpby5UaW1lb3V0RXJyb3I6CiAgICAgICAgICAgICAgICAgICAgYXdhaXQgX2VkaXRf"
    "Y2FyZDI1KCkKICAgICAgICAgICAgICAgIGV4Y2VwdCBhc3luY2lvLkNhbmNlbGxlZEVycm9yOgog"
    "ICAgICAgICAgICAgICAgICAgIHJhaXNlCiAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9u"
    "IGFzIF9lMjU6CiAgICAgICAgICAgICAgICAgICAgX2xvZygKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgIldaRklYIHIyNWM4OiBsZWFkZXIgd29ya2VyLXBhdGggY3Jhc2hlZCAiCiAgICAgICAgICAg"
    "ICAgICAgICAgICAgIGYiKHtfZTI1fSkg4oCUIG1hcmtpbmcgc2xpY2UgZmFpbGVkIgogICAgICAg"
    "ICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICBfbHJlcyA9IHsKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgIm9rIjogW10sCiAgICAgICAgICAgICAgICAgICAgICAgICJmYWlsIjogWwog"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgKF90MjUsICJsZWFkZXIgcGF0aCBjcmFzaGVkIikK"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgIGZvciBfdDI1IGluIF9sc2xpY2UyNQogICAgICAg"
    "ICAgICAgICAgICAgICAgICBdLAogICAgICAgICAgICAgICAgICAgICAgICAiaWRzIjogW10sCiAg"
    "ICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICBfd3pmaXhfcGIyNSA9IF9scmVzCgogICAg"
    "ICAgICAgICAjIDIuIHN3ZWVwIHRoZSBsZWFkZXIncyBmYWlsZWQgc29uZ3MgdGhyb3VnaCBKaW9T"
    "YWF2bgogICAgICAgICAgICBfb2tMID0gbGlzdChfbHJlcy5nZXQoIm9rIikgb3IgW10pCiAgICAg"
    "ICAgICAgIF9mYWlsID0gW3R1cGxlKHgpIGZvciB4IGluIChfbHJlcy5nZXQoImZhaWwiKSBvciBb"
    "XSldCiAgICAgICAgICAgIF93aHkyNSA9IHt0OiByIGZvciB0LCByIGluIF9mYWlsfQogICAgICAg"
    "ICAgICBpZiBfZmFpbDoKICAgICAgICAgICAgICAgIF9sb2coCiAgICAgICAgICAgICAgICAgICAg"
    "ZiJXWkZJWCByMjViOiBzd2VlcCDigJQge2xlbihfZmFpbCl9IGZhaWxlZCAiCiAgICAgICAgICAg"
    "ICAgICAgICAgZiJzb25nKHMpLCB0cnlpbmcgSmlvU2Fhdm4iCiAgICAgICAgICAgICAgICApCiAg"
    "ICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgZnJvbSAucjI1X3dvcmtlciBp"
    "bXBvcnQgKAogICAgICAgICAgICAgICAgICAgICAgICBzYWF2bl9mZXRjaF9hbmRfc2VuZCBhcyBf"
    "c2ZzMjUsCiAgICAgICAgICAgICAgICAgICAgKQoKICAgICAgICAgICAgICAgICAgICBmb3IgX3Qy"
    "NSwgX3IyNSBpbiBsaXN0KF9mYWlsKVs6MTJdOgogICAgICAgICAgICAgICAgICAgICAgICBfb2sy"
    "NSwgX3JzMjUgPSBhd2FpdCBfc2ZzMjUoCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfdDI1"
    "LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgYXJ0aXN0LAogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgY2hhdF9pZD1fY2hhdDI1LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgYm90"
    "X3Rva2VuPV90b2syNSwKICAgICAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAg"
    "ICAgICAgICBpZiBfb2syNToKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9mYWlsID0gW3gg"
    "Zm9yIHggaW4gX2ZhaWwgaWYgeFswXSAhPSBfdDI1XQogICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgX29rTC5hcHBlbmQoX3QyNSkKICAgICAgICAgICAgICAgICAgICAgICAgICAgIGF3YWl0IGFz"
    "eW5jaW8udG9fdGhyZWFkKAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9ycHQyNSwg"
    "X3J1bmlkMjUsIDAsIF90MjUsIFRydWUKICAgICAgICAgICAgICAgICAgICAgICAgICAgICkKICAg"
    "ICAgICAgICAgICAgICAgICAgICAgZWxpZiBfcnMyNToKICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgIF93aHkyNVtfdDI1XSA9IF9yczI1CiAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9u"
    "IGFzIF9lMjU6CiAgICAgICAgICAgICAgICAgICAgX2xvZyhmIldaRklYIHIyNWI6IHN3ZWVwIGVy"
    "cm9yOiB7X2UyNX0iKQogICAgICAgICAgICBfbG4yNSA9IGxlbihfb2tMKSArIGxlbihfZmFpbCkK"
    "CiAgICAgICAgICAgICMgMy4gd29ya2VyIHdhdmVzIChyMjVjMTApOiBLYWdnbGUgcnVucyBvbmx5"
    "IH41IHNlc3Npb25zCiAgICAgICAgICAgICMgICAgYXQgYSB0aW1lIChsZWFkZXIgKyBXWkZJWF9X"
    "T1JLRVJfQ09OQykuIERpc3BhdGNoIHRoZQogICAgICAgICAgICAjICAgIG5leHQgd29ya2VyIHdo"
    "ZW4gYSBzbG90IGZyZWVzOyB0aGUgY2FyZCBrZWVwcwogICAgICAgICAgICAjICAgIHVwZGF0aW5n"
    "IHRocm91Z2hvdXQuIEJ1c3kvZmFpbGVkIHB1c2hlcyBhcmUgcmV0cmllZCBhCiAgICAgICAgICAg"
    "ICMgICAgZmV3IHRpbWVzLCB0aGVuIHRoZWlyIHNvbmdzIGFyZSBtYXJrZWQgZmFpbGVkLgogICAg"
    "ICAgICAgICBfc3QyNSA9IHt9CiAgICAgICAgICAgIF90cmllczI1ID0ge30KICAgICAgICAgICAg"
    "X2hhcmQyNSA9IF90aW1lLnRpbWUoKSArIDU0MDAKICAgICAgICAgICAgd2hpbGUgX3RpbWUudGlt"
    "ZSgpIDwgX2hhcmQyNToKICAgICAgICAgICAgICAgIF9zdDI1ID0gYXdhaXQgX3IyNV9zdGF0ZXMo"
    "X3IyNV93b3JrZXJzKQogICAgICAgICAgICAgICAgX25ydW4yNSA9IHN1bSgKICAgICAgICAgICAg"
    "ICAgICAgICAxIGZvciB2IGluIF9zdDI1LnZhbHVlcygpIGlmIHYgPT0gInJ1biIKICAgICAgICAg"
    "ICAgICAgICkKICAgICAgICAgICAgICAgIF9kZWZlcjI1ID0gW10KICAgICAgICAgICAgICAgIHdo"
    "aWxlIF9wZW5kaW5nMjUgYW5kIF9ucnVuMjUgPCBfd2NvbmMyNToKICAgICAgICAgICAgICAgICAg"
    "ICBfazI1LCBfc2wyNSA9IF9wZW5kaW5nMjUucG9wKDApCiAgICAgICAgICAgICAgICAgICAgX3Iy"
    "NXggPSBhd2FpdCBfcHVzaF93b3JrZXIyNShfazI1LCBfc2wyNSkKICAgICAgICAgICAgICAgICAg"
    "ICBpZiBfcjI1eCA9PSAib2siOgogICAgICAgICAgICAgICAgICAgICAgICBfc3QyNVtfazI1XSA9"
    "ICJydW4iCiAgICAgICAgICAgICAgICAgICAgICAgIF9ucnVuMjUgKz0gMQogICAgICAgICAgICAg"
    "ICAgICAgICAgICBfdHJpZXMyNS5wb3AoX2syNSwgTm9uZSkKICAgICAgICAgICAgICAgICAgICBl"
    "bHNlOgogICAgICAgICAgICAgICAgICAgICAgICBfdHJpZXMyNVtfazI1XSA9IF90cmllczI1Lmdl"
    "dChfazI1LCAwKSArIDEKICAgICAgICAgICAgICAgICAgICAgICAgX2NhcDI1ID0gNiBpZiBfcjI1"
    "eCA9PSAiYnVzeSIgZWxzZSAzCiAgICAgICAgICAgICAgICAgICAgICAgIGlmIF90cmllczI1W19r"
    "MjVdID49IF9jYXAyNToKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF93aHkyNXggPSAoCiAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIndvcmtlciBidXN5IgogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgIGlmIF9yMjV4ID09ICJidXN5IgogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgIGVsc2UgIndvcmtlciBwdXNoIGZhaWxlZCIKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9sb2coCiAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgZiJXWkZJWCByMjVjOiB3b3JrZXIge19rMjV9ICIKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICBmIntfd2h5MjV4fSB7X3RyaWVzMjVbX2syNV19eCDi"
    "gJQgIgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYibWFya2luZyB7bGVuKF9zbDI1"
    "KX0gc29uZyhzKSAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZiJmYWlsZWQiCiAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBm"
    "b3IgX3F0MjUsIF90MjUgaW4gX3NsMjU6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "YXdhaXQgYXN5bmNpby50b190aHJlYWQoCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgIF9ycHQyNSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX3J1bmlkMjUs"
    "CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9rMjUsCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgIF90MjUsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIEZhbHNlLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBfd2h5MjV4"
    "LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgZWxzZToKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9kZWZlcjI1LmFwcGVuZCgoX2sy"
    "NSwgX3NsMjUpKQogICAgICAgICAgICAgICAgX3BlbmRpbmcyNS5leHRlbmQoX2RlZmVyMjUpCiAg"
    "ICAgICAgICAgICAgICBhd2FpdCBfZWRpdF9jYXJkMjUoKQogICAgICAgICAgICAgICAgX2xvZygK"
    "ICAgICAgICAgICAgICAgICAgICAiV1pGSVggcjI1Yzogd29ya2VycyDigJQgIgogICAgICAgICAg"
    "ICAgICAgICAgICsgIiAiLmpvaW4oZiJ3e2t9Ont2fSIgZm9yIGssIHYgaW4gX3N0MjUuaXRlbXMo"
    "KSkKICAgICAgICAgICAgICAgICAgICArICgKICAgICAgICAgICAgICAgICAgICAgICAgZiIgKCt7"
    "bGVuKF9wZW5kaW5nMjUpfSBxdWV1ZWQpIgogICAgICAgICAgICAgICAgICAgICAgICBpZiBfcGVu"
    "ZGluZzI1CiAgICAgICAgICAgICAgICAgICAgICAgIGVsc2UgIiIKICAgICAgICAgICAgICAgICAg"
    "ICApCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICBpZiBub3QgX3BlbmRpbmcyNSBh"
    "bmQgYWxsKAogICAgICAgICAgICAgICAgICAgIHYgaW4gKCJkb25lIiwgImVycm9yIikgZm9yIHYg"
    "aW4gX3N0MjUudmFsdWVzKCkKICAgICAgICAgICAgICAgICk6CiAgICAgICAgICAgICAgICAgICAg"
    "YnJlYWsKICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8uc2xlZXAoMTUpCiAgICAgICAgICAg"
    "IF93czI1ID0gIiAiLmpvaW4oCiAgICAgICAgICAgICAgICBmInd7a306IgogICAgICAgICAgICAg"
    "ICAgKyAoCiAgICAgICAgICAgICAgICAgICAgIuKchSIKICAgICAgICAgICAgICAgICAgICBpZiB2"
    "ID09ICJkb25lIgogICAgICAgICAgICAgICAgICAgIGVsc2UgKCLinYwiIGlmIHYgPT0gImVycm9y"
    "IiBlbHNlIHYpCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICBmb3IgaywgdiBpbiBf"
    "c3QyNS5pdGVtcygpCiAgICAgICAgICAgICkKCiAgICAgICAgICAgICMgNC4gZmluYWwgY2FyZCAr"
    "IGJhdGNoIHN1bW1hcnkKICAgICAgICAgICAgX3N1bTI1ID0gKAogICAgICAgICAgICAgICAgZiLi"
    "nIUgPGI+ZG9uZTwvYj4g4oCUIGxlYWRlciB7X2xuMjV9IHNvbmcocyksICIKICAgICAgICAgICAg"
    "ICAgIGYid29ya2Vyczoge193czI1fSIKICAgICAgICAgICAgKQogICAgICAgICAgICBpZiBfZmFp"
    "bDoKICAgICAgICAgICAgICAgIF9zdW0yNSArPSAoCiAgICAgICAgICAgICAgICAgICAgZiJcbuKd"
    "jCB1bmF2YWlsYWJsZSAoe2xlbihfZmFpbCl9KTpcbiIKICAgICAgICAgICAgICAgICAgICArICJc"
    "biIuam9pbigKICAgICAgICAgICAgICAgICAgICAgICAgZiLigKIgPHM+e3RbOjYwXX08L3M+IOKA"
    "lCAiCiAgICAgICAgICAgICAgICAgICAgICAgIGYie3N0cihfd2h5MjUuZ2V0KHQsICdub3QgZm91"
    "bmQnKSlbOjgwXX0iCiAgICAgICAgICAgICAgICAgICAgICAgIGZvciB0LCBfIGluIF9mYWlsWzox"
    "MF0KICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgIGF3"
    "YWl0IF9lZGl0X2NhcmQyNShfc3VtMjUpCgogICAgICAgICAgICAjIDUuIHBsYXlsaXN0IHByb21w"
    "dCDigJQgZXZlcnkgZGVsaXZlcmVkIHNvbmcncyBtZXNzYWdlIGlkCiAgICAgICAgICAgICMgICAg"
    "aXMgaW4gdGhlIHN0YXR1cyBkb2NzLCBzbyB0aGUgcGxheWxpc3QgY292ZXJzIHRoZQogICAgICAg"
    "ICAgICAjICAgIHdob2xlIGJhdGNoIChsZWFkZXIgQU5EIHdvcmtlcnMpCiAgICAgICAgICAgIF9w"
    "bF9pdGVtcyA9IFtdCiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIF9zdDI1eCA9IGF3"
    "YWl0IGFzeW5jaW8udG9fdGhyZWFkKF9mMjVzLCBfcnVuaWQyNSkKICAgICAgICAgICAgICAgIGZv"
    "ciBfZDI1IGluIChfc3QyNXguZ2V0KCJzb25ncyIpIG9yIHt9KS52YWx1ZXMoKToKICAgICAgICAg"
    "ICAgICAgICAgICBfbWlkMjUgPSBfZDI1LmdldCgibSIpCiAgICAgICAgICAgICAgICAgICAgaWYg"
    "X2QyNS5nZXQoInN0IikgPT0gIm9rIiBhbmQgX21pZDI1OgogICAgICAgICAgICAgICAgICAgICAg"
    "ICBfcGxfaXRlbXMuYXBwZW5kKChfY2hhdDI1LCBfbWlkMjUpKQogICAgICAgICAgICBleGNlcHQg"
    "RXhjZXB0aW9uOgogICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAgICBfbG9nKAogICAgICAg"
    "ICAgICAgICAgZiJXWkZJWCByMjVjMTE6IHBsYXlsaXN0IOKAlCB7bGVuKF9wbF9pdGVtcyl9IHNv"
    "bmcgIgogICAgICAgICAgICAgICAgZiJpZChzKSBmcm9tIHRoZSBzdGF0dXMgY2hhbm5lbCIKICAg"
    "ICAgICAgICAgKQogICAgICAgICAgICBpZiBub3QgX3BsX2l0ZW1zOgogICAgICAgICAgICAgICAg"
    "X3BsX2l0ZW1zID0gWwogICAgICAgICAgICAgICAgICAgIHR1cGxlKHgpIGZvciB4IGluIChfbHJl"
    "cy5nZXQoImlkcyIpIG9yIFtdKQogICAgICAgICAgICAgICAgXQogICAgICAgICAgICBpZiBfcGxf"
    "aXRlbXM6CiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgYXdhaXQgX3Bs"
    "YXlsaXN0X3Byb21wdChtZXNzYWdlLCBhcnRpc3QsIF9wbF9pdGVtcykKICAgICAgICAgICAgICAg"
    "IGV4Y2VwdCBFeGNlcHRpb24gYXMgX2UyNToKICAgICAgICAgICAgICAgICAgICBfbG9nKAogICAg"
    "ICAgICAgICAgICAgICAgICAgICAiV1pGSVggcjE1OiBwbGF5bGlzdCBwcm9tcHQgZmFpbGVkOiAi"
    "CiAgICAgICAgICAgICAgICAgICAgICAgIGYie19lMjV9IgogICAgICAgICAgICAgICAgICAgICkK"
    "ICAgICAgICAgICAgYXdhaXQgYXN5bmNpby50b190aHJlYWQoX2NsbjI1LCBfcnVuaWQyNSkKICAg"
    "ICAgICAgICAgX2xvZygiV1pGSVggcjI1Yzg6IGJhdGNoIGNvbXBsZXRlIOKAlCBjYXRhbG9nIGNh"
    "cmQgZmluYWwiKQoKICAgICAgICBhc3luY2lvLmNyZWF0ZV90YXNrKF93YXRjaF9wYigpKQoKICAg"
    "IGlmIG5vdGUgaXMgbm90IE5vbmUgYW5kIF9taWRzOgoKICAgICAgICBhc3luYyBkZWYgX3dhdGNo"
    "KCk6CiAgICAgICAgICAgIGltcG9ydCB0aW1lIGFzIF90aW1lCgogICAgICAgICAgICBfdDAgPSBf"
    "dGltZS50aW1lKCkKICAgICAgICAgICAgX3N0YXJ0ZWQgPSBGYWxzZQogICAgICAgICAgICBfbG9n"
    "KGYiV1pGSVggcjE1OiB3YXRjaGVyIHN0YXJ0ZWQgZm9yIHthcnRpc3R9ICh7X259IHNvbmdzKSIp"
    "CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIHdoaWxlIF90aW1lLnRpbWUoKSAtIF90"
    "MCA8IDM2MDA6CiAgICAgICAgICAgICAgICAgICAgYXdhaXQgYXN5bmNpby5zbGVlcChfRkFOT1VU"
    "X1dBVENIKQogICAgICAgICAgICAgICAgICAgIF90ZCA9IF90YXNrX2RpY3RfcmVmKCkKICAgICAg"
    "ICAgICAgICAgICAgICBpZiBfdGQgaXMgTm9uZToKICAgICAgICAgICAgICAgICAgICAgICAgX2xv"
    "ZygKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJXWkZJWCByMTU6IHdhdGNoZXIg4oCUIHRh"
    "c2sgZGljdCB1bmF2YWlsYWJsZSwgIgogICAgICAgICAgICAgICAgICAgICAgICAgICAgInN0b3Ag"
    "KHByb21wdCBza2lwcGVkKSIKICAgICAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAg"
    "ICAgICAgICAgICByZXR1cm4KICAgICAgICAgICAgICAgICAgICBfbGVmdCA9IFttIGZvciBtIGlu"
    "IF9taWRzIGlmIG0gaW4gX3RkXQogICAgICAgICAgICAgICAgICAgIGlmIF9sZWZ0OgogICAgICAg"
    "ICAgICAgICAgICAgICAgICBfc3RhcnRlZCA9IFRydWUKICAgICAgICAgICAgICAgICAgICBfd3og"
    "PSBfc2hhcmVkW2YiL3tfZm9sZGVyfSJdLmdldCgiX3d6Zml4Iikgb3Ige30KICAgICAgICAgICAg"
    "ICAgICAgICBfZmFpbGVkID0gX3d6LmdldCgiZmFpbGVkIikgb3IgW10KICAgICAgICAgICAgICAg"
    "ICAgICBfbWVyZ2VkID0gbGVuKF93ei5nZXQoImRvbmUiKSBvciBbXSkKICAgICAgICAgICAgICAg"
    "ICAgICBfZmwgPSAoCiAgICAgICAgICAgICAgICAgICAgICAgIGYiXG7inYwgdW5hdmFpbGFibGUg"
    "KHtsZW4oX2ZhaWxlZCl9KTogIgogICAgICAgICAgICAgICAgICAgICAgICArICIsICIuam9pbihf"
    "ZmFpbGVkWzo4XSkKICAgICAgICAgICAgICAgICAgICAgICAgaWYgX2ZhaWxlZAogICAgICAgICAg"
    "ICAgICAgICAgICAgICBlbHNlICIiCiAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAg"
    "ICAgICAgIF9kb25lID0gbGVuKF9taWRzKSAtIGxlbihfbGVmdCkKICAgICAgICAgICAgICAgICAg"
    "ICBfbG9nKAogICAgICAgICAgICAgICAgICAgICAgICBmIldaRklYIHIxNTogd2F0Y2gg4oCUIHtf"
    "ZG9uZX0ve2xlbihfbWlkcyl9IGZpbmlzaGVkLCAiCiAgICAgICAgICAgICAgICAgICAgICAgIGYi"
    "e19tZXJnZWR9IG1lcmdlZCwgIgogICAgICAgICAgICAgICAgICAgICAgICBmIntsZW4oX3d6Lmdl"
    "dCgncGwnKSBvciBbXSl9IHVwbG9hZGVkLCAiCiAgICAgICAgICAgICAgICAgICAgICAgIGYie2xl"
    "bihfZmFpbGVkKX0gZmFpbGVkLCAiCiAgICAgICAgICAgICAgICAgICAgICAgIGYiYWN0aXZlPXts"
    "ZW4oX2xlZnQpfSIKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgIyBX"
    "WkZJWCByMTU6IG9ubHkgY2FsbCB0aGUgYmF0Y2ggY29tcGxldGUgb25jZSB0aGUKICAgICAgICAg"
    "ICAgICAgICAgICAjIHRhc2tzIHdlcmUgYWN0dWFsbHkgU0VFTiBpbiB0aGUgdGFzayBkaWN0IOKA"
    "lAogICAgICAgICAgICAgICAgICAgICMgYmVmb3JlIHRoZSBmaXJzdCBkb3dubG9hZCBzdGFydHMg"
    "dGhlIGNsb25lcyBhcmUKICAgICAgICAgICAgICAgICAgICAjIG1lcmVseSBxdWV1ZWQgYW5kIG11"
    "c3Qgbm90IGNvdW50IGFzIGZpbmlzaGVkCiAgICAgICAgICAgICAgICAgICAgaWYgX3N0YXJ0ZWQg"
    "YW5kIG5vdCBfbGVmdDoKICAgICAgICAgICAgICAgICAgICAgICAgIyBXWkZJWCByMjVjOiB3YWl0"
    "IGZvciB0aGUgd29ya2VyIHNlc3Npb25zIHRvCiAgICAgICAgICAgICAgICAgICAgICAgICMgZmlu"
    "aXNoIGJlZm9yZSB0aGUgZmluYWwgc3VtbWFyeQogICAgICAgICAgICAgICAgICAgICAgICBpZiBf"
    "cjI1X3dvcmtlcnM6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfbG9nKAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICJXWkZJWCByMjVjOiBsZWFkZXIgc2xpY2UgZG9uZSDigJQg"
    "IgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYid2FpdGluZyBmb3Igd29ya2VycyB7"
    "X3IyNV93b3JrZXJzfSIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgIF93dDI1ID0gX3RpbWUudGltZSgpCiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICB3aGlsZSBfdGltZS50aW1lKCkgLSBfd3QyNSA8IDE1MDA6CiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgX3N0MjUgPSBhd2FpdCBfcjI1X3N0YXRlcyhfcjI1X3dvcmtlcnMp"
    "CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX2xvZygKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgIldaRklYIHIyNWM6IHdvcmtlcnMg4oCUICIKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgKyAiICIuam9pbigKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgIGYid3trfTp7dn0iCiAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICBmb3IgaywgdiBpbiBfc3QyNS5pdGVtcygpCiAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICApCiAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgYWxsKAogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICB2ICE9ICJydW4iIGZvciB2IGluIF9zdDI1LnZhbHVlcygpCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgKToKICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgYnJlYWsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhd2FpdCBhc3lu"
    "Y2lvLnNsZWVwKDE1KQogICAgICAgICAgICAgICAgICAgICAgICAjIFdaRklYIHIyNWI6IGxhc3Qt"
    "Y2hhbmNlIHN3ZWVwIOKAlCBldmVyeSBzb25nCiAgICAgICAgICAgICAgICAgICAgICAgICMgdGhh"
    "dCBmYWlsZWQgb24gWW91VHViZSBnZXRzIG9uZSBKaW9TYWF2bgogICAgICAgICAgICAgICAgICAg"
    "ICAgICAjIGF0dGVtcHQsIHNlbnQgdG8gdGhlIGNoYXQgdmlhIHRoZSBCb3QgQVBJCiAgICAgICAg"
    "ICAgICAgICAgICAgICAgIF93aHkyNSA9IF93ei5nZXQoImZhaWxlZF93aHkiKSBvciB7fQogICAg"
    "ICAgICAgICAgICAgICAgICAgICBpZiBfZmFpbGVkOgogICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgX2xvZygKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmIldaRklYIHIyNWI6IHN3"
    "ZWVwIOKAlCB7bGVuKF9mYWlsZWQpfSAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ImZhaWxlZCBzb25nKHMpLCB0cnlpbmcgSmlvU2Fhdm4iCiAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgZnJvbSAucjI1X3dvcmtlciBpbXBvcnQgKAogICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICBzYWF2bl9mZXRjaF9hbmRfc2VuZCBhcyBfc2ZzMjUsCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgKQoKICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICBmb3IgX3QyNSBpbiBsaXN0KF9mYWlsZWQpWzoxMl06CiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgIF9vazI1LCBfcnMyNSA9IGF3YWl0IF9zZnMyNSgKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgIF90MjUsCiAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICBhcnRpc3QsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICBjaGF0X2lkPV9jaGF0MjUsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICBib3RfdG9rZW49X3RvazI1LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIF9vazI1OgogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX2ZhaWxlZCA9IFsKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB4CiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgZm9yIHggaW4gX2ZhaWxlZAogICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIHggIT0gX3QyNQogICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgXQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgX3d6WyJmYWlsZWQiXSA9IF9mYWlsZWQKICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgZWxzZToKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "IF93aHkyNVtfdDI1XSA9IF9yczI1CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX3d6"
    "WyJmYWlsZWRfd2h5Il0gPSBfd2h5MjUKICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb24gYXMgX2UyNToKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBfbG9n"
    "KGYiV1pGSVggcjI1Yjogc3dlZXAgZXJyb3I6IHtfZTI1fSIpCiAgICAgICAgICAgICAgICAgICAg"
    "ICAgIF9mbDI1ID0gX2ZsCiAgICAgICAgICAgICAgICAgICAgICAgIGlmIF9mYWlsZWQgYW5kIF93"
    "aHkyNToKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9mbDI1ID0gKAogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICJcbuKdjCB1bmF2YWlsYWJsZSAoIgogICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgIGYie2xlbihfZmFpbGVkKX0pOlxuIgogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICsgIlxuIi5qb2luKAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICBmIuKAoiB7dFs6NjBdfSDigJQgIgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICBmIntzdHIoX3doeTI1LmdldCh0LCAnbm90IGZvdW5kJykpWzo4MF19IgogICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICBmb3IgdCBpbiBfZmFpbGVkWzoxMF0KICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgICAgICApCiAg"
    "ICAgICAgICAgICAgICAgICAgICAgIGlmIF9yMjVfd29ya2VyczoKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgIF9zdDI1ID0gYXdhaXQgX3IyNV9zdGF0ZXMoX3IyNV93b3JrZXJzKQogICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgX3dzMjUgPSAiICIuam9pbigKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICBmInd7a306IgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICsg"
    "KAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAi4pyFIgogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICBpZiB2ID09ICJkb25lIgogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICBlbHNlICgi4p2MIiBpZiB2ID09ICJlcnJvciIgZWxzZSB2KQogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICBmb3IgaywgdiBpbiBfc3QyNS5pdGVtcygpCiAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICApCiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgYXdhaXQgbm90ZS5lZGl0X3RleHQoCiAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgIGYi8J+OpyA8Yj57YXJ0aXN0fTwvYj4g4oCUIOKchSBsZWFkZXIgIgogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmIntfbiAtIGxlbihfZmFpbGVkKX0ve19u"
    "fSBzb25ncyAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYiZGVsaXZlcmVk"
    "XG7impkgd29ya2Vyczoge193czI1fSIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgZiJ7X2ZsMjV9XG4iCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICIod29y"
    "a2VyIHN1bW1hcmllcyBhcmUgcG9zdGVkIGluIGNoYXQpIgogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246"
    "CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAgICAgICAgICAg"
    "ICAgICBlbHNlOgogICAgICAgICAgICAgICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgIGF3YWl0IG5vdGUuZWRpdF90ZXh0KAogICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICBmIvCfjqcgPGI+e2FydGlzdH08L2I+IOKAlCDinIUgIgogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmIntfbiAtIGxlbihfZmFpbGVkKX0ve19u"
    "fSBzb25ncyAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYiZGVsaXZlcmVk"
    "e19mbDI1fSIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIHBhc3MKICAgICAgICAgICAgICAgICAgICAgICAgIyB0aGUgbGVhZGVyIGZpbGxzIHRo"
    "ZSBwbGF5bGlzdCBsaXN0IHdoaWxlIGl0CiAgICAgICAgICAgICAgICAgICAgICAgICMgdXBsb2Fk"
    "cyDigJQgcmUtY2hlY2sgYmVmb3JlIGdpdmluZyB1cAogICAgICAgICAgICAgICAgICAgICAgICBf"
    "cGxfaXRlbXMgPSBOb25lCiAgICAgICAgICAgICAgICAgICAgICAgICMgV1pGSVhfUjE3OiB0aGUg"
    "bGVhZGVyIHVwbG9hZHMgdGhlIHdob2xlCiAgICAgICAgICAgICAgICAgICAgICAgICMgYmF0Y2gg"
    "aW4gT05FIHRhc2sgYW5kIHRoYXQgdGFrZXMgZmFyIGxvbmdlcgogICAgICAgICAgICAgICAgICAg"
    "ICAgICAjIHRoYW4gMiBtaW51dGVzIC0gcG9sbCBmb3IgdXAgdG8gMzAgbWludXRlcwogICAgICAg"
    "ICAgICAgICAgICAgICAgICBmb3IgX3JldHJ5IGluIHJhbmdlKDYwKToKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgIF93eiA9ICgKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBfc2hh"
    "cmVkW2YiL3tfZm9sZGVyfSJdLmdldCgiX3d6Zml4Iikgb3Ige30KICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9wbF9pdGVtcyA9IF93ei5n"
    "ZXQoInBsIikgb3IgW10KICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmIF9wbF9pdGVtczoK"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBicmVhawogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgaWYgX3JldHJ5ICUgNSA9PSAwOgogICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgIF9sb2coCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJXWkZJWCByMTc6"
    "IHBsYXlsaXN0IHN0aWxsIGVtcHR5IOKAlCAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIGYicmV0cnkge19yZXRyeSArIDF9LzYwICh3YWl0aW5nIDMwcykiCiAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgICAgICAgICAgYXdhaXQg"
    "YXN5bmNpby5zbGVlcCgzMCkKICAgICAgICAgICAgICAgICAgICAgICAgaWYgX3BsX2l0ZW1zOgog"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIGF3YWl0IF9wbGF5bGlzdF9wcm9tcHQoCiAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgIG1lc3NhZ2UsIGFydGlzdCwgX3BsX2l0ZW1zCiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlv"
    "biBhcyBfZToKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBfbG9nKAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAiV1pGSVggcjE1OiBwbGF5bGlzdCBwcm9tcHQgZmFp"
    "bGVkOiAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYie19lfSIKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgIGVsc2U6"
    "CiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfbG9nKAogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICJXWkZJWCByMTU6IG5vIHBsYXlsaXN0IGl0ZW1zIGFmdGVyICIKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAicmV0cmllcyDigJQgcHJvbXB0IHNraXBwZWQiCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybgog"
    "ICAgICAgICAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgICAgICAgICAgIyBXWkZJWCBy"
    "MTQgKHYxNS43Myk6IGEgdmlzdWFsIHByb2dyZXNzIGJhciDigJQKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIyBvbmUgYmxvY2sgcGVyIDEwJSBvZiB0aGUgYmF0Y2gKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgX2ZpbGwgPSBpbnQocm91bmQoMTAgKiBfZG9uZSAvIG1heChsZW4oX21pZHMpLCAxKSkp"
    "CiAgICAgICAgICAgICAgICAgICAgICAgIF9iYXIgPSAi4pawIiAqIF9maWxsICsgIuKWsSIgKiAo"
    "MTAgLSBfZmlsbCkKICAgICAgICAgICAgICAgICAgICAgICAgX3dsbjI1ID0gIiIKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgaWYgX3IyNV93b3JrZXJzOgogICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgX3dsbjI1ID0gKAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYiXG7impkgUGhh"
    "c2UgQiDigJQge2xlbihfcjI1X3dvcmtlcnMpfSAiCiAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIndvcmtlciBzZXNzaW9uKHMpIGFsc28gZG93bmxvYWRpbmciCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgIGF3YWl0IG5vdGUuZWRpdF90"
    "ZXh0KAogICAgICAgICAgICAgICAgICAgICAgICAgICAgZiLwn46nIDxiPnthcnRpc3R9PC9iPlxu"
    "IgogICAgICAgICAgICAgICAgICAgICAgICAgICAgZiJ7X2Jhcn0ge19kb25lfS97bGVuKF9taWRz"
    "KX0gc29uZ3MgIgogICAgICAgICAgICAgICAgICAgICAgICAgICAgZiJmaW5pc2hlZHtfZmx9e193"
    "bG4yNX1cbiIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICLwn461IGVhY2ggc29uZyBpcyBz"
    "ZW50IGFzIHNvb24gYXMgaXQgZmluaXNoZXMiCiAgICAgICAgICAgICAgICAgICAgICAgICkKICAg"
    "ICAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAgICAg"
    "ICBwYXNzCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgX2U6CiAgICAgICAgICAgICAg"
    "ICBfbG9nKGYiV1pGSVggcjE1OiB3YXRjaGVyIGVycm9yOiB7X2V9IikKCiAgICAgICAgYXN5bmNp"
    "by5jcmVhdGVfdGFzayhfd2F0Y2goKSkKICAgIHJldHVybiBUcnVlCgoKYXN5bmMgZGVmIHJlc29s"
    "dmVfbXVzaWModXJsLCB1c2VyX2lkPTApOgogICAgIiIiUmV0dXJucyAoeXRzZWFyY2hfc3BlYywg"
    "c291cmNlX2xhYmVsKS4gVGhlIHNwZWMgaXMgYSByZWFkeSB5dHNlYXJjaAogICAgdGVybSDigJQg"
    "b25lIGV4YWN0IHNvbmcgZm9yIHRyYWNrIGxpbmtzLCBhIGNhcHBlZCBwbGF5bGlzdCBzZWFyY2gg"
    "Zm9yCiAgICBhcnRpc3QgLyBhbGJ1bSAvIHBsYXlsaXN0IGxpbmtzLiBSYWlzZXMgTXVzaWNVbnN1"
    "cHBvcnRlZCBmb3IgbGlua3MKICAgIHRoYXQgY2FuIG5ldmVyIHdvcms7IHJldHVybnMgTm9uZSB3"
    "aGVuIG5vdGhpbmcgY291bGQgYmUgcmVhZC4iIiIKICAgIGggPSBfaG9zdF9vZih1cmwpCiAgICBp"
    "ZiAic3BvdGlmeS4iIGluIGg6CiAgICAgICAgcmV0dXJuIGF3YWl0IF9yZXNvbHZlX3Nwb3RpZnko"
    "dXJsLCBhd2FpdCBfbXVzaWNfbWF4X2Zvcih1c2VyX2lkKSksICJTcG90aWZ5IgogICAgaWYgInNh"
    "YXZuLiIgaW4gaDoKICAgICAgICByZXR1cm4gYXdhaXQgX3Jlc29sdmVfamlvc2Fhdm4odXJsLCBh"
    "d2FpdCBfbXVzaWNfbWF4X2Zvcih1c2VyX2lkKSksICJKaW9TYWF2biIKICAgIGlmICJhcHBsZS5j"
    "b20iIGluIGg6CiAgICAgICAgcmV0dXJuIGF3YWl0IF9yZXNvbHZlX2FwcGxlKHVybCwgYXdhaXQg"
    "X211c2ljX21heF9mb3IodXNlcl9pZCkpLCAiQXBwbGUgTXVzaWMiCiAgICByZXR1cm4gTm9uZSwg"
    "Tm9uZQoKCmRlZiBfdXNlcl9vZihtZXNzYWdlKToKICAgIHRyeToKICAgICAgICByZXR1cm4gZ2V0"
    "YXR0cihnZXRhdHRyKG1lc3NhZ2UsICJmcm9tX3VzZXIiLCBOb25lKSwgImlkIiwgMCkKICAgIGV4"
    "Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIDAKCgphc3luYyBkZWYgX3JlZnVzZShtZXNz"
    "YWdlLCByZWFzb24pOgogICAgIiIiT25lIGNsZWFuIHJlZnVzYWwgbWVzc2FnZSBmb3IgdW5zdXBw"
    "b3J0ZWQgbXVzaWMgbGlua3MuIiIiCiAgICB0cnk6CiAgICAgICAgaWYgYWRtaW5fbG9nIGlzIG5v"
    "dCBOb25lOgogICAgICAgICAgICBmcm9tIGh0bWwgaW1wb3J0IGVzY2FwZSBhcyBfZXNjCgogICAg"
    "ICAgICAgICBhd2FpdCBhZG1pbl9sb2coCiAgICAgICAgICAgICAgICAi8J+OtSA8Yj5NdXNpYyBs"
    "aW5rIHJlamVjdGVkPC9iPiIsCiAgICAgICAgICAgICAgICBmIuKUjyA8Yj5Vc2VyPC9iPiDihpIg"
    "PGNvZGU+e191c2VyX29mKG1lc3NhZ2UpfTwvY29kZT5cbiIKICAgICAgICAgICAgICAgIGYi4pSg"
    "IDxiPlJlYXNvbjwvYj4g4oaSIHtfZXNjKHJlYXNvbil9XG4iCiAgICAgICAgICAgICAgICBmIuKU"
    "liA8Yj5NZXNzYWdlPC9iPiDihpIgIgogICAgICAgICAgICAgICAgZiJ7X2VzYygobWVzc2FnZS50"
    "ZXh0IG9yIG1lc3NhZ2UuY2FwdGlvbiBvciAnJylbOjEwMDBdKSBvciAnKG5vIHRleHQpJ30iLAog"
    "ICAgICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHRyeToK"
    "ICAgICAgICBmcm9tIC4uLmhlbHBlci50ZWxlZ3JhbV9oZWxwZXIubWVzc2FnZV91dGlscyBpbXBv"
    "cnQgKAogICAgICAgICAgICBzZW5kX21lc3NhZ2UsCiAgICAgICAgICAgIGRlbGV0ZV9tZXNzYWdl"
    "LAogICAgICAgICkKCiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIGYi4pqg77iP"
    "IHtyZWFzb259IikKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IGRlbGV0ZV9tZXNzYWdl"
    "KG1lc3NhZ2UpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIF9sb2coZiJXWkZJWCBtdXNpYyByZWZ1c2Ug"
    "ZmFpbGVkOiB7ZX0iKQoKCmFzeW5jIGRlZiBwcmVfcmVzb2x2ZShtZXNzYWdlLCBjbGllbnQ9Tm9u"
    "ZSwgaXNfbGVlY2g9RmFsc2UsIGlzX3l0ZGw9RmFsc2UpOgogICAgIiIiQ2FsbGVkIGF0IHRoZSB2"
    "ZXJ5IHRvcCBvZiBuZXdfZXZlbnQgZm9yIC9sLWZhbWlseSBhbmQgeXRkbC1mYW1pbHkKICAgIGNv"
    "bW1hbmRzLiBSZXR1cm5zICJfX1daRklYX01VU0lDX0RPTkVfXyIgd2hlbiB0aGUgY2FsbGVyIG11"
    "c3QgcmV0dXJuCiAgICAobGluayByZWZ1c2VkLCBvciB0aGUgdGFzayB3YXMgcmUtZGlzcGF0Y2hl"
    "ZCB0aHJvdWdoIHRoZSB5dGRsCiAgICBlbmdpbmUpOyBOb25lIHdoZW4gdGhlIGZsb3cgc2hvdWxk"
    "IGNvbnRpbnVlIOKAlCBubyBtdXNpYyBsaW5rIGZvdW5kLAogICAgb3IgdGhlIHRleHQgd2FzIHJl"
    "d3JpdHRlbiBpbiBwbGFjZSBmb3IgdGhlIHl0ZGwgZW5naW5lLiBOb24tbXVzaWMKICAgIGNvbW1h"
    "bmRzIGZhaWwgb3BlbjsgYSBtdXNpYyBsaW5rIGlzIG5ldmVyIGxlZnQgYXMgYSByYXcgVVJMIOKA"
    "lCBpdCBpcwogICAgZWl0aGVyIHJld3JpdHRlbiBvciBjbGVhbmx5IHJlZnVzZWQuIiIiCiAgICBf"
    "c2Vlbl9tdXNpYyA9IEZhbHNlCiAgICB0cnk6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBmcm9t"
    "IC5yNF9tdXNpY19jbWRzIGltcG9ydCBfc2V0dGluZyBhcyBfd3pfc2V0dGluZwoKICAgICAgICAg"
    "ICAgaWYgbm90IGF3YWl0IF93el9zZXR0aW5nKCJtdXNpY19vbiIpOgogICAgICAgICAgICAgICAg"
    "cmV0dXJuIE5vbmUKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBwYXNzCiAg"
    "ICAgICAgdGV4dCA9IG1lc3NhZ2UudGV4dCBvciBnZXRhdHRyKG1lc3NhZ2UsICJjYXB0aW9uIiwg"
    "Tm9uZSkgb3IgIiIKICAgICAgICBfcmVwbHkgPSBnZXRhdHRyKG1lc3NhZ2UsICJyZXBseV90b19t"
    "ZXNzYWdlIiwgTm9uZSkKICAgICAgICBfcnRleHQgPSAoX3JlcGx5LnRleHQgb3IgX3JlcGx5LmNh"
    "cHRpb24gb3IgIiIpIGlmIF9yZXBseSBlbHNlICIiCiAgICAgICAgaWYgImh0dHAiIG5vdCBpbiAo"
    "dGV4dCArICIgIiArIF9ydGV4dCkubG93ZXIoKToKICAgICAgICAgICAgcmV0dXJuIE5vbmUKICAg"
    "ICAgICBtdXNpY191cmwgPSBOb25lCiAgICAgICAgX3NyYyA9ICJ0ZXh0IgogICAgICAgIGZvciB1"
    "IGluIF9VUkxfUkUuZmluZGFsbCh0ZXh0KToKICAgICAgICAgICAgaWYgaXNfbXVzaWNfdXJsKHUp"
    "OgogICAgICAgICAgICAgICAgbXVzaWNfdXJsID0gdS5yc3RyaXAoIikuLF0+XCInIikKICAgICAg"
    "ICAgICAgICAgIGJyZWFrCiAgICAgICAgaWYgbXVzaWNfdXJsIGlzIE5vbmU6CiAgICAgICAgICAg"
    "IGZvciB1IGluIF9VUkxfUkUuZmluZGFsbChfcnRleHQpOgogICAgICAgICAgICAgICAgaWYgaXNf"
    "bXVzaWNfdXJsKHUpOgogICAgICAgICAgICAgICAgICAgIG11c2ljX3VybCA9IHUKICAgICAgICAg"
    "ICAgICAgICAgICBfc3JjID0gInJlcGx5IgogICAgICAgICAgICAgICAgICAgIGJyZWFrCiAgICAg"
    "ICAgaWYgbXVzaWNfdXJsIGlzIE5vbmU6CiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAg"
    "X3NlZW5fbXVzaWMgPSBUcnVlCiAgICAgICAgX2xvZyhmIldaRklYIG11c2ljOiByZXNvbHZpbmcg"
    "e211c2ljX3VybFs6MTIwXX0gKGZyb20ge19zcmN9KSIpCgogICAgICAgICMgdjE1LjQ3OiBhIFNw"
    "b3RpZnkgYXJ0aXN0IGxpbmsgZmFucyBvdXQgaW50byBvbmUgdGFzayBwZXIKICAgICAgICAjIHRv"
    "cCB0cmFjayDigJQgdGhlIGV4YWN0IFNwb3RpZnkgbGlzdCwgbm90IGEgc2VhcmNoIGd1ZXNzCiAg"
    "ICAgICAgaWYgInNwb3RpZnkuIiBpbiBfaG9zdF9vZihtdXNpY191cmwpIGFuZCAiL2FydGlzdC8i"
    "IGluICgKICAgICAgICAgICAgbXVzaWNfdXJsLnNwbGl0KCI/IilbMF0KICAgICAgICApOgogICAg"
    "ICAgICAgICBfYXJ0ID0gTm9uZQogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBfbGlt"
    "ID0gYXdhaXQgX211c2ljX21heF9mb3IoX3VzZXJfb2YobWVzc2FnZSkpCiAgICAgICAgICAgICAg"
    "ICBfYXJ0ID0gYXdhaXQgX3Nwb3RpZnlfYXJ0aXN0X3RyYWNrcyhtdXNpY191cmwsIF9saW0pCiAg"
    "ICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAgICAgICAgIF9sb2coZiJX"
    "WkZJWCBtdXNpYzogYXJ0aXN0IHJlc29sdmUgZmFpbGVkOiB7ZX0iKQogICAgICAgICAgICBpZiBf"
    "YXJ0OgogICAgICAgICAgICAgICAgX2FuYW1lLCBfdHJhY2tzID0gX2FydAogICAgICAgICAgICAg"
    "ICAgX2xvZygKICAgICAgICAgICAgICAgICAgICBmIldaRklYIG11c2ljOiBhcnRpc3Qge19hbmFt"
    "ZX0g4oaSIHtsZW4oX3RyYWNrcyl9ICIKICAgICAgICAgICAgICAgICAgICAidHJhY2socykgKHNt"
    "YXJ0IG9yZGVyKSIKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIGlmIGF3YWl0IF9h"
    "cnRpc3RfZmFub3V0KAogICAgICAgICAgICAgICAgICAgIGNsaWVudCwgbWVzc2FnZSwgX2FuYW1l"
    "LCBfdHJhY2tzLCBpc19sZWVjaAogICAgICAgICAgICAgICAgKToKICAgICAgICAgICAgICAgICAg"
    "ICB0cnk6CiAgICAgICAgICAgICAgICAgICAgICAgIGlmIGFkbWluX2xvZyBpcyBub3QgTm9uZToK"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgIGZyb20gaHRtbCBpbXBvcnQgZXNjYXBlIGFzIF9l"
    "c2MKCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBhd2FpdCBhZG1pbl9sb2coCiAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgIvCfjrUgPGI+QXJ0aXN0IGJhdGNoPC9iPiIsCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgZiLilI8gPGI+QXJ0aXN0PC9iPiDihpIge19lc2Mo"
    "X2FuYW1lKX1cbiIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmIuKUoCA8Yj5UcmFj"
    "a3M8L2I+IOKGkiB7bGVuKF90cmFja3MpfVxuIgogICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgIGYi4pSWIDxiPkxpbms8L2I+IOKGkiAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgZiJ7X2VzYyhtdXNpY191cmxbOjkwMF0pfSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICApCiAgICAgICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgcGFzcwogICAgICAgICAgICAgICAgICAgIHJldHVybiAiX19XWkZJWF9NVVNJQ19E"
    "T05FX18iCiAgICAgICAgICAgICMgYXJ0aXN0IHBhZ2UgdW5yZWFkYWJsZSDihpIgZmFsbCB0aHJv"
    "dWdoIHRvIHRoZSBzaW5nbGUKICAgICAgICAgICAgIyBzZWFyY2ggYmVsb3cgKGJlc3QtcGljayBr"
    "ZWVwcyBpdCB0byBvbmUgc29uZykKCiAgICAgICAgdHJ5OgogICAgICAgICAgICB5dHEsIHNvdXJj"
    "ZSA9IGF3YWl0IHJlc29sdmVfbXVzaWMobXVzaWNfdXJsLCBfdXNlcl9vZihtZXNzYWdlKSkKICAg"
    "ICAgICBleGNlcHQgTXVzaWNVbnN1cHBvcnRlZCBhcyBlOgogICAgICAgICAgICBhd2FpdCBfcmVm"
    "dXNlKG1lc3NhZ2UsIHN0cihlKSkKICAgICAgICAgICAgcmV0dXJuICJfX1daRklYX01VU0lDX0RP"
    "TkVfXyIKICAgICAgICBpZiBub3QgeXRxOgogICAgICAgICAgICBhd2FpdCBfcmVmdXNlKAogICAg"
    "ICAgICAgICAgICAgbWVzc2FnZSwKICAgICAgICAgICAgICAgICJDb3VsZG4ndCByZWFkIHRoaXMg"
    "bXVzaWMgbGluayDigJQgdHJ5IGFnYWluIGluIGEgbWludXRlLCAiCiAgICAgICAgICAgICAgICAi"
    "b3Igc2VuZCBhIFlvdVR1YmUgbGluayIsCiAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJu"
    "ICJfX1daRklYX01VU0lDX0RPTkVfXyIKICAgICAgICBpZiBhbnkoX3cgaW4gc3RyKHl0cSkubG93"
    "ZXIoKSBmb3IgX3cgaW4KICAgICAgICAgICAgICAgKCJzcG90aWZ5IiwgIndlYiBwbGF5ZXIiLCAi"
    "YXBwbGUgbXVzaWMiLCAiamlvc2Fhdm4uY29tIikpOgogICAgICAgICAgICBfbG9nKGYiV1pGSVgg"
    "bXVzaWM6IHJlZnVzaW5nIHN1c3BpY2lvdXMgcXVlcnk6IHt5dHFbOjEyMF19IikKICAgICAgICAg"
    "ICAgYXdhaXQgX3JlZnVzZSgKICAgICAgICAgICAgICAgIG1lc3NhZ2UsCiAgICAgICAgICAgICAg"
    "ICAiU3BvdGlmeSBzZXJ2ZWQgYSBwbGFjZWhvbGRlciBwYWdlIGluc3RlYWQgb2YgdGhlIHNvbmcg"
    "4oCUICIKICAgICAgICAgICAgICAgICJ0cnkgYWdhaW4gaW4gYSBtaW51dGUsIG9yIHNlbmQgYSBZ"
    "b3VUdWJlIGxpbmsiLAogICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVybiAiX19XWkZJWF9N"
    "VVNJQ19ET05FX18iCiAgICAgICAgdHJ5OgogICAgICAgICAgICBpZiBhZG1pbl9sb2cgaXMgbm90"
    "IE5vbmU6CiAgICAgICAgICAgICAgICBmcm9tIGh0bWwgaW1wb3J0IGVzY2FwZSBhcyBfZXNjCgog"
    "ICAgICAgICAgICAgICAgYXdhaXQgYWRtaW5fbG9nKAogICAgICAgICAgICAgICAgICAgICLwn461"
    "IDxiPk11c2ljIGxpbmsgcmVzb2x2ZWQ8L2I+IiwKICAgICAgICAgICAgICAgICAgICBmIuKUjyA8"
    "Yj5Vc2VyPC9iPiDihpIgPGNvZGU+e191c2VyX29mKG1lc3NhZ2UpfTwvY29kZT5cbiIKICAgICAg"
    "ICAgICAgICAgICAgICBmIuKUoCA8Yj5Tb3VyY2U8L2I+IOKGkiB7c291cmNlfVxuIgogICAgICAg"
    "ICAgICAgICAgICAgIGYi4pSgIDxiPkxpbms8L2I+IOKGkiB7X2VzYyhtdXNpY191cmxbOjkwMF0p"
    "fVxuIgogICAgICAgICAgICAgICAgICAgIGYi4pSgIDxiPlNlYXJjaDwvYj4g4oaSIHtfZXNjKHl0"
    "cVs6NTAwXSl9XG4iCiAgICAgICAgICAgICAgICAgICAgZiLilJYgPGI+TWVzc2FnZTwvYj4g4oaS"
    "ICIKICAgICAgICAgICAgICAgICAgICBmIntfZXNjKChtZXNzYWdlLnRleHQgb3IgbWVzc2FnZS5j"
    "YXB0aW9uIG9yICcnKVs6MTAwMF0pIG9yICcobm8gdGV4dCknfSIsCiAgICAgICAgICAgICAgICAp"
    "CiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwogICAgICAgIHRyeToK"
    "ICAgICAgICAgICAgX3QgPSByZS5zdWIociJeeXRzZWFyY2hcZCs6IiwgIiIsIHN0cih5dHEpKQog"
    "ICAgICAgICAgICBmb3IgX3N1ZiBpbiAoIiBmdWxsIGFsYnVtIGF1ZGlvIiwgIiBzb25ncyBhdWRp"
    "byIsCiAgICAgICAgICAgICAgICAgICAgICAgICAiIHBsYXlsaXN0IGF1ZGlvIiwgIiBhdWRpbyIp"
    "OgogICAgICAgICAgICAgICAgaWYgX3QuZW5kc3dpdGgoX3N1Zik6CiAgICAgICAgICAgICAgICAg"
    "ICAgX3QgPSBfdFs6IC1sZW4oX3N1ZildCiAgICAgICAgICAgICAgICAgICAgYnJlYWsKICAgICAg"
    "ICAgICAgX3QgPSBfdC5zdHJpcCgiIC18IikKICAgICAgICAgICAgaWYgX3Q6CiAgICAgICAgICAg"
    "ICAgICBtZXNzYWdlLl93emZpeF9tdXNpY190aXRsZSA9IF90CiAgICAgICAgZXhjZXB0IEV4Y2Vw"
    "dGlvbjoKICAgICAgICAgICAgcGFzcwogICAgICAgIG1lc3NhZ2UuX3d6Zml4X211c2ljX25vdGUg"
    "PSBmIntzb3VyY2V9OiB7bXVzaWNfdXJsfSDihpIge3l0cX0iCiAgICAgICAgaWYgaXNfeXRkbDoK"
    "ICAgICAgICAgICAgIyBhbHJlYWR5IGluIHRoZSB5dGRsIGVuZ2luZSDigJQgcmV3cml0ZSB0aGUg"
    "bGluayBpbiBwbGFjZTsKICAgICAgICAgICAgIyB0aGUgeXRkbCBtb2R1bGUgYWxzbyByZS1yZWFk"
    "cyB0aGUgcmVwbHkgbWVzc2FnZSwgc28gdGhlCiAgICAgICAgICAgICMgcmV3cml0ZSBtdXN0IGxh"
    "bmQgd2hlcmV2ZXIgdGhlIGxpbmsgY2FtZSBmcm9tCiAgICAgICAgICAgIHRyeToKICAgICAgICAg"
    "ICAgICAgIGlmIG1lc3NhZ2UudGV4dDoKICAgICAgICAgICAgICAgICAgICBtZXNzYWdlLnRleHQg"
    "PSBtZXNzYWdlLnRleHQucmVwbGFjZShtdXNpY191cmwsIHl0cSkKICAgICAgICAgICAgICAgIGlm"
    "IGdldGF0dHIobWVzc2FnZSwgImNhcHRpb24iLCBOb25lKToKICAgICAgICAgICAgICAgICAgICBt"
    "ZXNzYWdlLmNhcHRpb24gPSBtZXNzYWdlLmNhcHRpb24ucmVwbGFjZSgKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgbXVzaWNfdXJsLCB5dHEKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAg"
    "ICAgICBpZiBfc3JjID09ICJyZXBseSIgYW5kIF9yZXBseSBpcyBub3QgTm9uZToKICAgICAgICAg"
    "ICAgICAgICAgICBpZiBfcmVwbHkudGV4dDoKICAgICAgICAgICAgICAgICAgICAgICAgX3JlcGx5"
    "LnRleHQgPSBfcmVwbHkudGV4dC5yZXBsYWNlKG11c2ljX3VybCwgeXRxKQogICAgICAgICAgICAg"
    "ICAgICAgIGlmIF9yZXBseS5jYXB0aW9uOgogICAgICAgICAgICAgICAgICAgICAgICBfcmVwbHku"
    "Y2FwdGlvbiA9IF9yZXBseS5jYXB0aW9uLnJlcGxhY2UoCiAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICBtdXNpY191cmwsIHl0cQogICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAg"
    "IGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBwYXNzCiAgICAgICAgICAgIHJldHVy"
    "biBOb25lCiAgICAgICAgIyBtaXJyb3IgY29tbWFuZCAoL2wgZmFtaWx5KSDigJQgcmUtZGlzcGF0"
    "Y2ggdGhyb3VnaCB0aGUgeXRkbAogICAgICAgICMgZW5naW5lIHdpdGggYSBjbGVhbiAveWwgY29t"
    "bWFuZCBzbyBubyAvbCBhcmcgc3ludGF4IGxlYWtzCiAgICAgICAgdHJ5OgogICAgICAgICAgICBt"
    "ZXNzYWdlLnRleHQgPSBmIi95bCB7eXRxfSIKICAgICAgICAgICAgbWVzc2FnZS5jYXB0aW9uID0g"
    "Tm9uZQogICAgICAgICAgICBmcm9tIC4uLm1vZHVsZXMueXRkbHAgaW1wb3J0IFl0RGxwCgogICAg"
    "ICAgICAgICBhd2FpdCBZdERscChjbGllbnQsIG1lc3NhZ2UsIGlzX2xlZWNoPWJvb2woaXNfbGVl"
    "Y2gpKS5uZXdfZXZlbnQoKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAg"
    "ICAgX2xvZyhmIldaRklYIG11c2ljIHJlLWRpc3BhdGNoIGZhaWxlZDoge2V9IikKICAgICAgICAg"
    "ICAgdHJ5OgogICAgICAgICAgICAgICAgYXdhaXQgX3JlZnVzZSgKICAgICAgICAgICAgICAgICAg"
    "ICBtZXNzYWdlLAogICAgICAgICAgICAgICAgICAgICJNdXNpYyBsaW5rIGNvdWxkbid0IGJlIHN0"
    "YXJ0ZWQg4oCUIHRyeSBhZ2FpbiBpbiBhICIKICAgICAgICAgICAgICAgICAgICAibWludXRlLCBv"
    "ciBzZW5kIGEgWW91VHViZSBsaW5rIiwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgZXhj"
    "ZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAgICAgcmV0dXJuICJf"
    "X1daRklYX01VU0lDX0RPTkVfXyIKICAgICAgICByZXR1cm4gIl9fV1pGSVhfTVVTSUNfRE9ORV9f"
    "IgogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIF9sb2coZiJXWkZJWCBtdXNpYyBw"
    "cmVfcmVzb2x2ZSBmYWlsZWQ6IHtlfSIpCiAgICAgICAgaWYgX3NlZW5fbXVzaWM6CiAgICAgICAg"
    "ICAgICMgbmV2ZXIgbGV0IGEgcmF3IG11c2ljIGxpbmsgZmFsbCB0aHJvdWdoIHRvIHl0LWRscAog"
    "ICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBhd2FpdCBfcmVmdXNlKAogICAgICAgICAg"
    "ICAgICAgICAgIG1lc3NhZ2UsCiAgICAgICAgICAgICAgICAgICAgIk11c2ljIGxpbmsgY291bGRu"
    "J3QgYmUgcHJvY2Vzc2VkIOKAlCB0cnkgYWdhaW4gaW4gIgogICAgICAgICAgICAgICAgICAgICJh"
    "IG1pbnV0ZSwgb3Igc2VuZCBhIFlvdVR1YmUgbGluayIsCiAgICAgICAgICAgICAgICApCiAgICAg"
    "ICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBwYXNzCiAgICAgICAgICAg"
    "IHJldHVybiAiX19XWkZJWF9NVVNJQ19ET05FX18iCiAgICAgICAgcmV0dXJuIE5vbmUKCgoKIyAt"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tCiMgV1pGSVggcjExICh2MTUuNzApOiBvbmUtdGFwIHBsYXlsaXN0IGZvciBh"
    "biBhcnRpc3QgYmF0Y2gKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCgpfUExfU1RBVEUgPSB7fQoKCmFzeW5jIGRl"
    "ZiBfcGxheWxpc3RfcHJvbXB0KG1lc3NhZ2UsIGFydGlzdCwgaXRlbXMpOgogICAgIiIiT25lIG1l"
    "c3NhZ2Ugd2l0aCBhIENyZWF0ZS1wbGF5bGlzdCBidXR0b24gYmVuZWF0aCB0aGUgYmF0Y2guIiIi"
    "CiAgICBmcm9tIHNlY3JldHMgaW1wb3J0IHRva2VuX2hleAoKICAgIGZyb20gcHlyb2dyYW0udHlw"
    "ZXMgaW1wb3J0IElubGluZUtleWJvYXJkQnV0dG9uLCBJbmxpbmVLZXlib2FyZE1hcmt1cAoKICAg"
    "IGZyb20gLi4uaGVscGVyLnRlbGVncmFtX2hlbHBlci5tZXNzYWdlX3V0aWxzIGltcG9ydCBzZW5k"
    "X21lc3NhZ2UKCiAgICBrZXkgPSB0b2tlbl9oZXgoNikKICAgIF9QTF9TVEFURVtrZXldID0gewog"
    "ICAgICAgICJ0aXRsZSI6IHN0cihhcnRpc3QpWzo2MF0sCiAgICAgICAgIml0ZW1zIjogbGlzdChp"
    "dGVtcylbOjUwMF0sCiAgICB9CiAgICBpZiBsZW4oX1BMX1NUQVRFKSA+IDIwOgogICAgICAgIGZv"
    "ciBfayBpbiBsaXN0KF9QTF9TVEFURSlbOi0yMF06CiAgICAgICAgICAgIF9QTF9TVEFURS5wb3Ao"
    "X2ssIE5vbmUpCiAgICBrYiA9IElubGluZUtleWJvYXJkTWFya3VwKAogICAgICAgIFsKICAgICAg"
    "ICAgICAgWwogICAgICAgICAgICAgICAgSW5saW5lS2V5Ym9hcmRCdXR0b24oCiAgICAgICAgICAg"
    "ICAgICAgICAgIvCfjrUgQ3JlYXRlIHBsYXlsaXN0IiwKICAgICAgICAgICAgICAgICAgICBjYWxs"
    "YmFja19kYXRhPWYid3pmeHBsOntrZXl9IiwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAg"
    "XQogICAgICAgIF0KICAgICkKICAgIGF3YWl0IHNlbmRfbWVzc2FnZSgKICAgICAgICBtZXNzYWdl"
    "LAogICAgICAgIGYi8J+OpyA8Yj57YXJ0aXN0fTwvYj4g4oCUIGFsbCBzb25ncyBkZWxpdmVyZWQu"
    "XG4iCiAgICAgICAgIldhbnQgb25lIHBsYXlsaXN0IHBhZ2Ugd2l0aCBldmVyeSBzdHJlYW0/IiwK"
    "ICAgICAgICBrYiwKICAgICkKICAgIF9sb2coCiAgICAgICAgZiJXWkZJWCByMTE6IHBsYXlsaXN0"
    "IHByb21wdCBmb3Ige2FydGlzdH0gIgogICAgICAgIGYiKHtsZW4oaXRlbXMpfSBzb25ncykiCiAg"
    "ICApCgoKYXN5bmMgZGVmIHd6Zml4X3BsX2dvKGNsaWVudCwgY2IpOgogICAgIiIiVGhlIGJ1dHRv"
    "bjogYnVpbGQgb25lIHBsYXlsaXN0IHBhZ2UgZnJvbSB0aGUgZXhhY3QgYmF0Y2ggc29uZ3MuIiIi"
    "CiAgICBhd2FpdCBjYi5hbnN3ZXIoKQogICAga2V5ID0gKGNiLmRhdGEgb3IgIiIpLnNwbGl0KCI6"
    "IiwgMSlbLTFdCiAgICBzdCA9IF9QTF9TVEFURS5wb3Aoa2V5LCBOb25lKQogICAgaWYgbm90IHN0"
    "OgogICAgICAgIHRyeToKICAgICAgICAgICAgYXdhaXQgY2IubWVzc2FnZS5lZGl0X3RleHQoCiAg"
    "ICAgICAgICAgICAgICAi4p2MIFByb21wdCBleHBpcmVkIOKAlCBzZW5kIHRoZSBhcnRpc3QgbGlu"
    "ayBhZ2FpbiIKICAgICAgICAgICAgKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "ICAgIHBhc3MKICAgICAgICByZXR1cm4KICAgIHRyeToKICAgICAgICBhd2FpdCBjYi5tZXNzYWdl"
    "LmVkaXRfdGV4dCgi4o+zIEJ1aWxkaW5nIHRoZSBwbGF5bGlzdC4uLiIpCiAgICAgICAgIyBXWkZJ"
    "WCByMjVjNzogYm90L19faW5pdF9fLnB5IGV4cG9ydHMgbmVpdGhlciBDb25maWcgbm9yCiAgICAg"
    "ICAgIyBkYXRhYmFzZSDigJQgaW1wb3J0IHRoZW0gZnJvbSB0aGVpciByZWFsIG1vZHVsZXMuCiAg"
    "ICAgICAgZnJvbSAuLi5jb3JlLmNvbmZpZ19tYW5hZ2VyIGltcG9ydCBDb25maWcKICAgICAgICAj"
    "IHIyNWMxMTogZXh0X3V0aWxzIGxpdmVzIGF0IGJvdC9oZWxwZXIvZXh0X3V0aWxzICh2ZXJpZmll"
    "ZAogICAgICAgICMgYWdhaW5zdCB0aGUgV1pNTC1YIHd6djMgc291cmNlIOKAlCBgZnJvbSAuLi5l"
    "eHRfdXRpbHNgIHdhcwogICAgICAgICMgd2h5IHRoZSBidXR0b24gZGllZCB3aXRoICJObyBtb2R1"
    "bGUgbmFtZWQgJ2JvdC5leHRfdXRpbHMnIikKICAgICAgICBmcm9tIC4uZXh0X3V0aWxzLmRiX2hh"
    "bmRsZXIgaW1wb3J0IGRhdGFiYXNlCiAgICAgICAgZnJvbSAuLi5tb2R1bGVzLnN0cmVhbSBpbXBv"
    "cnQgKAogICAgICAgICAgICBfcmVzZXJ2ZV9wbGF5bGlzdCwKICAgICAgICAgICAgZ2VuX3N0cmVh"
    "bV9saW5rLAogICAgICAgICkKCiAgICAgICAgcGwgPSBhd2FpdCBfcmVzZXJ2ZV9wbGF5bGlzdCgp"
    "CiAgICAgICAgaWYgbm90IHBsOgogICAgICAgICAgICBhd2FpdCBjYi5tZXNzYWdlLmVkaXRfdGV4"
    "dCgKICAgICAgICAgICAgICAgICLinYwgQ291bGQgbm90IGFsbG9jYXRlIGEgcGxheWxpc3Qg4oCU"
    "IHRyeSAvc3RyZWFtIC1wbCIKICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4KICAgICAg"
    "ICB0b2tzID0gW10KICAgICAgICBmb3IgY2lkLCBtaWQgaW4gc3RbIml0ZW1zIl06CiAgICAgICAg"
    "ICAgIHNsID0gYXdhaXQgZ2VuX3N0cmVhbV9saW5rKGNpZCwgbWlkKQogICAgICAgICAgICBpZiBz"
    "bDoKICAgICAgICAgICAgICAgIHRva3MuYXBwZW5kKHNsWzBdLnJzcGxpdCgiLyIsIDEpWy0xXSkK"
    "ICAgICAgICBpZiBub3QgdG9rczoKICAgICAgICAgICAgYXdhaXQgY2IubWVzc2FnZS5lZGl0X3Rl"
    "eHQoIuKdjCBObyBwbGF5YWJsZSBzb25ncyBmb3VuZCIpCiAgICAgICAgICAgIHJldHVybgogICAg"
    "ICAgIGF3YWl0IGRhdGFiYXNlLmFkZF9wbGF5bGlzdCgKICAgICAgICAgICAgcGwsIHN0WyJ0aXRs"
    "ZSJdLCB0b2tzLCBOb25lLCBOb25lCiAgICAgICAgKQogICAgICAgICMgV1pGSVggcjI1YzE3OiBw"
    "ZXItc29uZyBtZXRhZGF0YSAoY2xlYW4gbmFtZSwgYXJ0aXN0LCBjb3ZlciwKICAgICAgICAjIGR1"
    "cmF0aW9uKSAtIHRoZSBwbGF5bGlzdCBwYWdlIGFuZCBBUEkgcmVuZGVyIHRoZXNlIGluc3RlYWQK"
    "ICAgICAgICAjIG9mIHRoZSByYXcgeXQtZGxwIGZpbGUgbmFtZXMKICAgICAgICB0cnk6CiAgICAg"
    "ICAgICAgIF9wZW5kMTcgPSBfV1pfUExfUEVORElORy5wb3Aoc3RyKHN0LmdldCgidGl0bGUiKSlb"
    "OjYwXSwgTm9uZSkgb3Ige30KICAgICAgICAgICAgX210MTcgPSB7fQogICAgICAgICAgICBmb3Ig"
    "X2kxNywgX3NsMTcgaW4gZW51bWVyYXRlKHRva3MpOgogICAgICAgICAgICAgICAgX3JvdzE3ID0g"
    "X3BlbmQxNy5nZXQoX2kxNykgb3Ige30KICAgICAgICAgICAgICAgIGlmIG5vdCBfcm93MTcuZ2V0"
    "KCJuIik6CiAgICAgICAgICAgICAgICAgICAgY29udGludWUKICAgICAgICAgICAgICAgIF9tdDE3"
    "W19zbDE3XSA9IHsKICAgICAgICAgICAgICAgICAgICAibiI6IHN0cihfcm93MTcuZ2V0KCJuIikp"
    "WzoxMjBdLAogICAgICAgICAgICAgICAgICAgICJhIjogc3RyKF9yb3cxNy5nZXQoImEiKSBvciBz"
    "dC5nZXQoInRpdGxlIikgb3IgIiIpWzo2MF0sCiAgICAgICAgICAgICAgICAgICAgImMiOiBzdHIo"
    "X3JvdzE3LmdldCgiYyIpIG9yICIiKVs6MzAwXSwKICAgICAgICAgICAgICAgICAgICAiZCI6IGlu"
    "dChfcm93MTcuZ2V0KCJkIikgb3IgMCksCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgIGlm"
    "IF9tdDE3OgogICAgICAgICAgICAgICAgYXdhaXQgZGF0YWJhc2Uuc2V0X3BsYXlsaXN0X21ldGEo"
    "cGwsIF9tdDE3KQogICAgICAgICAgICAgICAgX2xvZygKICAgICAgICAgICAgICAgICAgICAiV1pG"
    "SVggcjI1YzE3OiBwbGF5bGlzdCBtZXRhZGF0YSBhdHRhY2hlZCAiCiAgICAgICAgICAgICAgICAg"
    "ICAgZiIoe2xlbihfbXQxNyl9IHNvbmcocykpIgogICAgICAgICAgICAgICAgKQogICAgICAgIGV4"
    "Y2VwdCBFeGNlcHRpb24gYXMgX2UxNzoKICAgICAgICAgICAgX2xvZyhmIldaRklYIHIyNWMxNzog"
    "cGxheWxpc3QgbWV0YWRhdGEgZmFpbGVkOiB7X2UxN30iKQogICAgICAgIGJhc2UgPSAoQ29uZmln"
    "LkJBU0VfVVJMIG9yICIiKS5yc3RyaXAoIi8iKQogICAgICAgIHBhZ2UgPSBmIntiYXNlfS9wbGF5"
    "bGlzdC97cGx9IgogICAgICAgIGF3YWl0IGNiLm1lc3NhZ2UuZWRpdF90ZXh0KAogICAgICAgICAg"
    "ICBmIvCfjrUgPGI+UGxheWxpc3QgcmVhZHk8L2I+IOKAlCB7bGVuKHRva3MpfSBzb25nc1xuIgog"
    "ICAgICAgICAgICBmJzxhIGhyZWY9IntwYWdlfSI+e3BhZ2V9PC9hPicKICAgICAgICApCiAgICAg"
    "ICAgX2xvZygKICAgICAgICAgICAgZiJXWkZJWCByMTE6IHBsYXlsaXN0IGNyZWF0ZWQgKHtsZW4o"
    "dG9rcyl9IHNvbmdzKSAiCiAgICAgICAgICAgIGYiLT4gL3BsYXlsaXN0L3twbH0iCiAgICAgICAg"
    "KQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIF9sb2coZiJXWkZJWCByMTE6IHBs"
    "YXlsaXN0IGZhaWxlZDoge2V9IikKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IGNiLm1l"
    "c3NhZ2UuZWRpdF90ZXh0KAogICAgICAgICAgICAgICAgZiLinYwgUGxheWxpc3QgZmFpbGVkOiB7"
    "c3RyKGUpWzoyMDBdfSIKICAgICAgICAgICAgKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAg"
    "ICAgICAgICAgIHBhc3MKCgojIFdaRklYIHIyNCBoZWxwZXJzICh2MTUuODIpCmRlZiBfd3pfcjI0"
    "X2ZhaWwoX3NoYXJlZCwgX2ZvbGRlciwgX20sIF9lKToKICAgIF9sb2coZiJXWkZJWCBtdXNpYzog"
    "YXJ0aXN0IHRyYWNrIGZhaWxlZDoge19lfSIpCiAgICB0cnk6CiAgICAgICAgX3d6ID0gX3NoYXJl"
    "ZC5nZXQoZiIve19mb2xkZXJ9Iiwge30pLmdldCgiX3d6Zml4IikKICAgICAgICBpZiBfd3ogaXMg"
    "bm90IE5vbmU6CiAgICAgICAgICAgIF90ID0gZ2V0YXR0cihfbSwgIl93emZpeF9tdXNpY190aXRs"
    "ZSIsICIiKSBvciAic29uZyIKICAgICAgICAgICAgX2ZsID0gX3d6LnNldGRlZmF1bHQoImZhaWxl"
    "ZCIsIFtdKQogICAgICAgICAgICBpZiBfdCBub3QgaW4gX2ZsOgogICAgICAgICAgICAgICAgX2Zs"
    "LmFwcGVuZChfdCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwoKCiMgV1pGSVgg"
    "cjI1YyAodjE1LjgzKTogcG9sbCB0aGUgd29ya2VyIGtlcm5lbCBzZXNzaW9ucyB0aHJvdWdoIHRo"
    "ZQojIEthZ2dsZSBBUEkgc28gdGhlIGJhdGNoIG5vdGUgY2FuIHNob3cgdGhlaXIgc3RhdGUuCmFz"
    "eW5jIGRlZiBfcjI1X3N0YXRlcyh3b3JrZXJzKToKICAgIG91dCA9IHt9CiAgICB0cnk6CiAgICAg"
    "ICAgZnJvbSAucjI1X3dvcmtlciBpbXBvcnQgd29ya2VyX3N0YXR1cyBhcyBfd3MyNQoKICAgICAg"
    "ICBmb3IgayBpbiB3b3JrZXJzOgogICAgICAgICAgICBvdXRba10gPSBhd2FpdCBhc3luY2lvLnRv"
    "X3RocmVhZChfd3MyNSwgaykKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICBfbG9n"
    "KGYiV1pGSVggcjI1Yzogd29ya2VyIHN0YXR1cyBwb2xsIGZhaWxlZDoge2V9IikKICAgIHJldHVy"
    "biBvdXQKCgojIFdaRklYIHIyNWMxNzogbGl2ZSBwb3B1bGFyaXR5IG9yZGVyICsgcGVyLXNvbmcg"
    "bWV0YWRhdGEKX1daX1BMX01FVEEgPSB7fQpfV1pfUExfUEVORElORyA9IHt9CgoKZGVmIF93el90"
    "aXRsZV9wYXJ0XzI1YzE3KGFydGlzdCwgbmFtZSk6CiAgICAiIiJTdHJpcCB0aGUgbGVhZGluZyAn"
    "QXJ0aXN0IC0gJyBmcm9tIGEgZGlzcGxheSBuYW1lLiIiIgogICAgX2EgPSAoYXJ0aXN0IG9yICIi"
    "KS5zdHJpcCgpLmxvd2VyKCkKICAgIF9uID0gKG5hbWUgb3IgIiIpLnN0cmlwKCkKICAgIGlmIF9h"
    "IGFuZCBfbi5sb3dlcigpLnN0YXJ0c3dpdGgoX2EgKyAiIC0gIik6CiAgICAgICAgcmV0dXJuIF9u"
    "W2xlbihfYSkgKyAzOl0uc3RyaXAoKQogICAgaWYgX2EgYW5kIF9uLmxvd2VyKCkuc3RhcnRzd2l0"
    "aChfYSArICItIik6CiAgICAgICAgcmV0dXJuIF9uW2xlbihfYSkgKyAxOl0uc3RyaXAoKQogICAg"
    "Zm9yIF9zZXAgaW4gKCIgLSAiLCAiIOKAkyAiLCAiIOKAlCAiKToKICAgICAgICBpZiBfc2VwIGlu"
    "IF9uOgogICAgICAgICAgICByZXR1cm4gX24uc3BsaXQoX3NlcCwgMSlbMV0uc3RyaXAoKQogICAg"
    "cmV0dXJuIF9uCgoKYXN5bmMgZGVmIF93el9wb3Bfc29ydF8yNWMxNyhhcnRpc3QsIHRyYWNrcyk6"
    "CiAgICAiIiJMaXZlIEppb1NhYXZuIHJhbmtpbmcgZm9yIGFuIGFydGlzdCdzIHNvbmdzLgoKICAg"
    "IHNlYXJjaC5nZXRSZXN1bHRzIHdpdGggdGhlIGFydGlzdCBuYW1lICh0aGUgZXhhY3QgY2FsbAog"
    "ICAgX3NhYXZuX2FydGlzdF90cmFja3MgbWFrZXMgLSB3b3JrcyBmcm9tIEthZ2dsZSk6IGFydGlz"
    "dCBzZWFyY2gKICAgIHJlc3VsdHMgY29tZSBiYWNrIHBsYXktcmFua2VkLCBzbyBwb3NpdGlvbiA9"
    "IHRyZW5kaW5nIHJhbmsuCiAgICBSZXR1cm5zIChzb3J0ZWRfdHJhY2tzLCBtZXRhKSB3aXRoIG1l"
    "dGEga2V5ZWQgYnkgdGhlIG5vcm1hbGl6ZWQKICAgIHNvbmcgdGl0bGUuIE5vIGtleSAvIGF1dGgg"
    "bmVlZGVkLgogICAgIiIiCiAgICBpbXBvcnQgaHRtbCBhcyBfaHRtbDE3CiAgICBpbXBvcnQgdXJs"
    "bGliLnBhcnNlIGFzIF91cDE3CgogICAgaWYgbm90IHRyYWNrczoKICAgICAgICByZXR1cm4gbGlz"
    "dCh0cmFja3MpLCB7fQogICAgcSA9IF91cDE3LnF1b3RlKHN0cihhcnRpc3QpLnN0cmlwKCkpCiAg"
    "ICBtZXRhID0ge30KICAgIHJhbmsgPSAwCiAgICBmb3IgcGFnZSBpbiByYW5nZSgxLCA1KToKICAg"
    "ICAgICByID0gYXdhaXQgYXN5bmNpby50b190aHJlYWQoCiAgICAgICAgICAgIF9qc29uX2dldCwK"
    "ICAgICAgICAgICAgImh0dHBzOi8vd3d3Lmppb3NhYXZuLmNvbS9hcGkucGhwP19fY2FsbD1zZWFy"
    "Y2guZ2V0UmVzdWx0cyIKICAgICAgICAgICAgZiImcT17cX0mX2Zvcm1hdD1qc29uJl9tYXJrZXI9"
    "MCZhcGlfdmVyc2lvbj00JmN0eD13ZWI2ZG90MCIKICAgICAgICAgICAgZiImbj00MCZwPXtwYWdl"
    "fSIsCiAgICAgICAgKQogICAgICAgIHJlcyA9IHIuZ2V0KCJyZXN1bHRzIikgb3IgW10KICAgICAg"
    "ICBpZiBub3QgcmVzOgogICAgICAgICAgICBicmVhawogICAgICAgIHRvb2sgPSBGYWxzZQogICAg"
    "ICAgIGZvciBzIGluIHJlczoKICAgICAgICAgICAgbWkgPSBzLmdldCgibW9yZV9pbmZvIikgb3Ig"
    "e30KICAgICAgICAgICAgcHJpbSA9ICgobWkuZ2V0KCJhcnRpc3RNYXAiKSBvciB7fSkuZ2V0KCJw"
    "cmltYXJ5X2FydGlzdHMiKSBvciBbXSkKICAgICAgICAgICAgaWYgbm90IGFueSgKICAgICAgICAg"
    "ICAgICAgIChwLmdldCgibmFtZSIpIG9yICIiKS5zdHJpcCgpLmxvd2VyKCkKICAgICAgICAgICAg"
    "ICAgID09IHN0cihhcnRpc3QpLnN0cmlwKCkubG93ZXIoKQogICAgICAgICAgICAgICAgZm9yIHAg"
    "aW4gcHJpbQogICAgICAgICAgICApOgogICAgICAgICAgICAgICAgY29udGludWUKICAgICAgICAg"
    "ICAgdCA9IF9odG1sMTcudW5lc2NhcGUoc3RyKHMuZ2V0KCJ0aXRsZSIpIG9yICIiKSkuc3RyaXAo"
    "KQogICAgICAgICAgICBpZiBub3QgdDoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAg"
    "ICAgIGsgPSBfbm9ybV9rZXkoX3d6X3RpdGxlX3BhcnRfMjVjMTcoYXJ0aXN0LCB0KSkgb3IgX25v"
    "cm1fa2V5KHQpCiAgICAgICAgICAgIGlmIG5vdCBrIG9yIGsgaW4gbWV0YToKICAgICAgICAgICAg"
    "ICAgIGNvbnRpbnVlCiAgICAgICAgICAgIGltZyA9IHN0cihzLmdldCgiaW1hZ2UiKSBvciAiIikK"
    "ICAgICAgICAgICAgaWYgIjE1MHgxNTAiIGluIGltZzoKICAgICAgICAgICAgICAgIGltZyA9IGlt"
    "Zy5yZXBsYWNlKCIxNTB4MTUwIiwgIjUwMHg1MDAiKQogICAgICAgICAgICB0cnk6CiAgICAgICAg"
    "ICAgICAgICBkdXIgPSBpbnQoZmxvYXQobWkuZ2V0KCJkdXJhdGlvbiIpIG9yIDApKQogICAgICAg"
    "ICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgZHVyID0gMAogICAgICAgICAg"
    "ICBtZXRhW2tdID0gewogICAgICAgICAgICAgICAgInJhbmsiOiByYW5rLAogICAgICAgICAgICAg"
    "ICAgIm4iOiBmInthcnRpc3R9IC0ge3R9IiwKICAgICAgICAgICAgICAgICJhIjogc3RyKGFydGlz"
    "dCksCiAgICAgICAgICAgICAgICAiYyI6IGltZywKICAgICAgICAgICAgICAgICJkIjogZHVyLAog"
    "ICAgICAgICAgICAgICAgInNrIjogc3RyKHMuZ2V0KCJpZCIpIG9yICIiKSBvciBrLAogICAgICAg"
    "ICAgICB9CiAgICAgICAgICAgIHJhbmsgKz0gMQogICAgICAgICAgICB0b29rID0gVHJ1ZQogICAg"
    "ICAgIGlmIG5vdCB0b29rIGFuZCBwYWdlID49IDI6CiAgICAgICAgICAgIGJyZWFrCiAgICAgICAg"
    "aWYgcmFuayA+PSAxMjA6CiAgICAgICAgICAgIGJyZWFrCiAgICAgICAgYXdhaXQgYXN5bmNpby5z"
    "bGVlcCgwLjUpCiAgICBpZiBub3QgbWV0YToKICAgICAgICByZXR1cm4gbGlzdCh0cmFja3MpLCB7"
    "fQoKICAgIG1hdGNoZWQgPSBbXQogICAgdW5tYXRjaGVkID0gW10KICAgIHNlZW5fc2sgPSBzZXQo"
    "KQogICAgc2Vlbl9rID0gc2V0KCkKICAgIGZvciBpLCAoeXEsIG5hbWUpIGluIGVudW1lcmF0ZSh0"
    "cmFja3MpOgogICAgICAgIHRwID0gX3d6X3RpdGxlX3BhcnRfMjVjMTcoYXJ0aXN0LCBuYW1lKQog"
    "ICAgICAgIGsgPSBfbm9ybV9rZXkodHApIG9yIF9ub3JtX2tleShuYW1lKQogICAgICAgIG0gPSBt"
    "ZXRhLmdldChrKSBvciBtZXRhLmdldChfbm9ybV9rZXkobmFtZSkpCiAgICAgICAgaWYgbToKICAg"
    "ICAgICAgICAgaWYgbVsic2siXSBpbiBzZWVuX3NrIG9yIGsgaW4gc2Vlbl9rOgogICAgICAgICAg"
    "ICAgICAgY29udGludWUgICMgZHVwbGljYXRlIG9mIGFuIGFscmVhZHktcmFua2VkIHNvbmcKICAg"
    "ICAgICAgICAgc2Vlbl9zay5hZGQobVsic2siXSkKICAgICAgICAgICAgc2Vlbl9rLmFkZChrKQog"
    "ICAgICAgICAgICBtYXRjaGVkLmFwcGVuZCgoKHlxLCBuYW1lKSwgbVsicmFuayJdLCBpKSkKICAg"
    "ICAgICBlbHNlOgogICAgICAgICAgICBpZiBrIGluIHNlZW5fazoKICAgICAgICAgICAgICAgIGNv"
    "bnRpbnVlCiAgICAgICAgICAgIHNlZW5fay5hZGQoaykKICAgICAgICAgICAgdW5tYXRjaGVkLmFw"
    "cGVuZCgoKHlxLCBuYW1lKSwgaSkpCiAgICBtYXRjaGVkLnNvcnQoa2V5PWxhbWJkYSB4OiAoeFsx"
    "XSwgeFsyXSkpCiAgICBvdXQgPSBbdCBmb3IgdCwgXywgXyBpbiBtYXRjaGVkXSArIFt0IGZvciB0"
    "LCBfIGluIHVubWF0Y2hlZF0KICAgIHJldHVybiBvdXQsIG1ldGEKCgpfb3JpZ19hcnRpc3RfZmFu"
    "b3V0XzI1YzE3ID0gX2FydGlzdF9mYW5vdXQKCgphc3luYyBkZWYgX2FydGlzdF9mYW5vdXQoY2xp"
    "ZW50LCBtZXNzYWdlLCBhcnRpc3QsIHRyYWNrcywgaXNfbGVlY2gpOgogICAgIiIicjI1YzE3OiB0"
    "cmVuZGluZyBvcmRlciBiZWZvcmUgYW55dGhpbmcgZG93bmxvYWRzLiIiIgogICAgX21ldGExNyA9"
    "IHt9CiAgICB0cnk6CiAgICAgICAgdHJhY2tzLCBfbWV0YTE3ID0gYXdhaXQgX3d6X3BvcF9zb3J0"
    "XzI1YzE3KGFydGlzdCwgbGlzdCh0cmFja3Mgb3IgW10pKQogICAgICAgIGlmIF9tZXRhMTc6CiAg"
    "ICAgICAgICAgIF9XWl9QTF9NRVRBW3N0cihhcnRpc3QpWzo2MF1dID0gX21ldGExNwogICAgICAg"
    "ICAgICB3aGlsZSBsZW4oX1daX1BMX01FVEEpID4gNjoKICAgICAgICAgICAgICAgIF9XWl9QTF9N"
    "RVRBLnBvcChuZXh0KGl0ZXIoX1daX1BMX01FVEEpKSwgTm9uZSkKICAgICAgICAgICAgX2xvZygK"
    "ICAgICAgICAgICAgICAgICJXWkZJWCByMjVjMTc6IHBvcHVsYXJpdHkgb3JkZXIgYXBwbGllZCAo"
    "IgogICAgICAgICAgICAgICAgZiJ7bGVuKF9tZXRhMTcpfSBzb25nKHMpIHJhbmtlZCBsaXZlIHZp"
    "YSBKaW9TYWF2bikiCiAgICAgICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgX2UxNzoK"
    "ICAgICAgICBfbG9nKGYiV1pGSVggcjI1YzE3OiBwb3B1bGFyaXR5IHNvcnQgZmFpbGVkOiB7X2Ux"
    "N30gLSBrZWVwaW5nIG9yZGVyIikKICAgIHJldHVybiBhd2FpdCBfb3JpZ19hcnRpc3RfZmFub3V0"
    "XzI1YzE3KAogICAgICAgIGNsaWVudCwgbWVzc2FnZSwgYXJ0aXN0LCB0cmFja3MsIGlzX2xlZWNo"
    "CiAgICApCgoKX29yaWdfcGxheWxpc3RfcHJvbXB0XzI1YzE3ID0gX3BsYXlsaXN0X3Byb21wdAoK"
    "CmFzeW5jIGRlZiBfcGxheWxpc3RfcHJvbXB0KG1lc3NhZ2UsIGFydGlzdCwgaXRlbXMpOgogICAg"
    "IiIicjI1YzE3OiBwb3B1bGFyaXR5LW9yZGVyZWQsIGRlLWR1cGxpY2F0ZWQgcGxheWxpc3QgaXRl"
    "bXMgd2l0aAogICAgcGVyLXNvbmcgbWV0YWRhdGEgKGNsZWFuIG5hbWVzLCBjb3ZlcnMsIGR1cmF0"
    "aW9ucykuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSAucjI1X3dvcmtlciBpbXBvcnQgTUlEX1RJ"
    "VExFXzI1CgogICAgICAgIG1ldGEgPSBfV1pfUExfTUVUQS5nZXQoc3RyKGFydGlzdClbOjYwXSkg"
    "b3Ige30KICAgICAgICByb3dzID0gW10KICAgICAgICBmb3IgaSwgKGNpZCwgbWlkKSBpbiBlbnVt"
    "ZXJhdGUoaXRlbXMpOgogICAgICAgICAgICB0ID0gTUlEX1RJVExFXzI1LmdldChzdHIobWlkKSkg"
    "b3IgIiIKICAgICAgICAgICAgayA9IF9ub3JtX2tleShfd3pfdGl0bGVfcGFydF8yNWMxNyhhcnRp"
    "c3QsIHQpKSBpZiB0IGVsc2UgIiIKICAgICAgICAgICAgbSA9IG1ldGEuZ2V0KGspIGlmIGsgZWxz"
    "ZSBOb25lCiAgICAgICAgICAgIHJvd3MuYXBwZW5kKAogICAgICAgICAgICAgICAgeyJjaWQiOiBj"
    "aWQsICJtaWQiOiBtaWQsICJrIjogaywgIm0iOiBtLAogICAgICAgICAgICAgICAgICJyYW5rIjog"
    "KG0gb3Ige30pLmdldCgicmFuayIsIDkwMDAgKyBpKSwgImkiOiBpfQogICAgICAgICAgICApCiAg"
    "ICAgICAgcm93cy5zb3J0KGtleT1sYW1iZGEgcjogKHJbInJhbmsiXSwgclsiaSJdKSkKICAgICAg"
    "ICBzZWVuX2sgPSBzZXQoKQogICAgICAgIHNlZW5fc2sgPSBzZXQoKQogICAgICAgIGRlZCA9IFtd"
    "CiAgICAgICAgZm9yIHIgaW4gcm93czoKICAgICAgICAgICAgc2sgPSAoclsibSJdIG9yIHt9KS5n"
    "ZXQoInNrIikKICAgICAgICAgICAgaWYgclsiayJdIGFuZCByWyJrIl0gaW4gc2Vlbl9rOgogICAg"
    "ICAgICAgICAgICAgY29udGludWUKICAgICAgICAgICAgaWYgc2sgYW5kIHNrIGluIHNlZW5fc2s6"
    "CiAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICBpZiByWyJrIl06CiAgICAgICAg"
    "ICAgICAgICBzZWVuX2suYWRkKHJbImsiXSkKICAgICAgICAgICAgaWYgc2s6CiAgICAgICAgICAg"
    "ICAgICBzZWVuX3NrLmFkZChzaykKICAgICAgICAgICAgZGVkLmFwcGVuZChyKQogICAgICAgIF9X"
    "Wl9QTF9QRU5ESU5HW3N0cihhcnRpc3QpWzo2MF1dID0gewogICAgICAgICAgICBpOiAoclsibSJd"
    "IG9yIHt9KSBmb3IgaSwgciBpbiBlbnVtZXJhdGUoZGVkKQogICAgICAgIH0KICAgICAgICB3aGls"
    "ZSBsZW4oX1daX1BMX1BFTkRJTkcpID4gNjoKICAgICAgICAgICAgX1daX1BMX1BFTkRJTkcucG9w"
    "KG5leHQoaXRlcihfV1pfUExfUEVORElORykpLCBOb25lKQogICAgICAgIF9kdXAxNyA9IGxlbihp"
    "dGVtcykgLSBsZW4oZGVkKQogICAgICAgIF9sb2coCiAgICAgICAgICAgICJXWkZJWCByMjVjMTc6"
    "IHBsYXlsaXN0IG9yZGVyIC0gdHJlbmRpbmcgcmFuayIKICAgICAgICAgICAgKyAoZiIsIHtfZHVw"
    "MTd9IGR1cGxpY2F0ZShzKSByZW1vdmVkIiBpZiBfZHVwMTcgZWxzZSAiIikKICAgICAgICApCiAg"
    "ICAgICAgcmV0dXJuIGF3YWl0IF9vcmlnX3BsYXlsaXN0X3Byb21wdF8yNWMxNygKICAgICAgICAg"
    "ICAgbWVzc2FnZSwgYXJ0aXN0LCBbKHJbImNpZCJdLCByWyJtaWQiXSkgZm9yIHIgaW4gZGVkXQog"
    "ICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgX2UxNzoKICAgICAgICBfbG9nKGYiV1pG"
    "SVggcjI1YzE3OiBwbGF5bGlzdCBtZXRhIGZhaWxlZDoge19lMTd9IikKICAgICAgICByZXR1cm4g"
    "YXdhaXQgX29yaWdfcGxheWxpc3RfcHJvbXB0XzI1YzE3KG1lc3NhZ2UsIGFydGlzdCwgaXRlbXMp"
    "Cg=="
)
WZFIX_R25_WORKER_B64 = (
    "IyBXWkZJWCBSb3VuZCAyNSDigJQgUGhhc2UgQiB3b3JrZXIgKyBmYWxsYmFjayBsYWRkZXIgKHYx"
    "NS44MykuCiMKIyBUaHJlZSByb2xlcywgb25lIGZpbGU6CiMgICAxLiBIZWFkbGVzcyB3b3JrZXIg"
    "am9iIChlbnYgV1pGSVhfSk9CX0I2NCk6IGEgS2FnZ2xlIHdvcmtlciBzZXNzaW9uCiMgICAgICBk"
    "b3dubG9hZHMgaXRzIHNsaWNlIG9mIGFuIGFydGlzdCBiYXRjaCAoNiBsYW5lcyBhdCBhIHRpbWUp"
    "IGFuZAojICAgICAgc2VuZHMgZXZlcnkgZmluaXNoZWQgc29uZyBzdHJhaWdodCB0byB0aGUgbG9n"
    "IGNoYXQgdGhyb3VnaCB0aGUKIyAgICAgIEJvdCBIVFRQIEFQSS4gV2hlbiB0aGUgc2xpY2UgaXMg"
    "ZG9uZSB0aGUgam9iIGV4aXRzIGFuZCB0aGUKIyAgICAgIHdvcmtlciBzZXNzaW9uIGVuZHMgYnkg"
    "aXRzZWxmIOKAlCBub3RoaW5nIGlkbGVzIG9uIEthZ2dsZS4KIyAgIDIuIHNhYXZuX2ZldGNoX2Fu"
    "ZF9zZW5kKCk6IHRoZSBsYXN0LWNoYW5jZSBKaW9TYWF2biBhdHRlbXB0IGZvciBhCiMgICAgICBz"
    "b25nIHRoYXQgZmFpbGVkIG9uIFlvdVR1YmUg4oCUIHVzZWQgYnkgdGhlIGxlYWRlcidzIHN3ZWVw"
    "IGFmdGVyCiMgICAgICB0aGUgYmF0Y2ggZmluaXNoZXMuCiMgICAzLiBkaXNwYXRjaF93b3JrZXIo"
    "KSAvIHdvcmtlcl9zdGF0dXMoKTogbGVhZGVyLXNpZGUgaGVscGVycyB0aGF0CiMgICAgICBwdXNo"
    "IHdvcmtlciBrZXJuZWxzIHRocm91Z2ggdGhlIEthZ2dsZSBBUEkgYW5kIHBvbGwgdGhlbS4KIwoj"
    "IEV2ZXJ5IGFjdGl2aXR5IGlzIGxvZ2dlZCAoa2VybmVsIGxvZykgYW5kIHRoZSBpbXBvcnRhbnQg"
    "c3RlcHMgYXJlCiMgbWlycm9yZWQgaW50byB0aGUgY2hhdCBzbyBwcm9ncmVzcyBpcyBhbHdheXMg"
    "dmlzaWJsZS4KCmltcG9ydCBhc3luY2lvCmltcG9ydCBiYXNlNjQKaW1wb3J0IGh0bWwKaW1wb3J0"
    "IGpzb24KaW1wb3J0IG9zCmltcG9ydCByZQppbXBvcnQgc2h1dGlsCmltcG9ydCBzdWJwcm9jZXNz"
    "CmltcG9ydCBzeXMKaW1wb3J0IHRlbXBmaWxlCmltcG9ydCB0aW1lCmltcG9ydCB1cmxsaWIucGFy"
    "c2UKaW1wb3J0IHVybGxpYi5yZXF1ZXN0CgpfSEVSRSA9IG9zLnBhdGguZGlybmFtZShvcy5wYXRo"
    "LmFic3BhdGgoX19maWxlX18pKQpXWk1MWF9ESVIgPSBvcy5wYXRoLmRpcm5hbWUob3MucGF0aC5k"
    "aXJuYW1lKG9zLnBhdGguZGlybmFtZShfSEVSRSkpKQoKIyBUaGUgd29ya2VyIGJvb3RzdHJhcCBk"
    "b3dubG9hZHMgdGhlIG1haW4gbm90ZWJvb2sgZnJvbSBEcml2ZSDigJQgdGhlCiMgc2FtZSBzb3Vy"
    "Y2Ugb2YgdHJ1dGggdGhlIGRlcGxveSB3b3JrZmxvdyB1c2VzLgpOT1RFQk9PS19EUklWRV9VUkwg"
    "PSAoCiAgICAiaHR0cHM6Ly9kcml2ZS51c2VyY29udGVudC5nb29nbGUuY29tL2Rvd25sb2FkIgog"
    "ICAgIj9pZD0xU180MWw0cmJaN19mRHdORFRDZkVVSEQwQnoyYUUyaU0mZXhwb3J0PWRvd25sb2Fk"
    "JmNvbmZpcm09dCIKKQpDT05GSUdfREFUQVNFVCA9ICJkam9zaGk3L3d6bWx4LWNvbmZpZyIKCldP"
    "UktFUl9CT09UX1RNUEwgPSAnJycjIS91c3IvYmluL2VudiBweXRob24zCiMgV1pGSVggcjI1YyDi"
    "gJQgUGhhc2UgQiB3b3JrZXIgYm9vdHN0cmFwIChhdXRvLWdlbmVyYXRlZCBwZXIgam9iKS4KIyBE"
    "b3dubG9hZHMgdGhlIG1haW4gbm90ZWJvb2sgZnJvbSBEcml2ZSAoc291cmNlIG9mIHRydXRoKSBh"
    "bmQgcnVucyBpdAojIGluIHdvcmtlciBtb2RlIHdpdGggdGhpcyBqb2IgZW1iZWRkZWQuIE5vdGhp"
    "bmcgZWxzZSBsaXZlcyBoZXJlLgppbXBvcnQgYmFzZTY0CmltcG9ydCBvcwppbXBvcnQgcnVucHkK"
    "aW1wb3J0IHVybGxpYi5yZXF1ZXN0Cgpvcy5lbnZpcm9uWyJXWkZJWF9XT1JLRVIiXSA9ICIxIgpv"
    "cy5lbnZpcm9uWyJXWkZJWF9KT0JfQjY0Il0gPSAlcgoKVVJMID0gJXIKcmVxID0gdXJsbGliLnJl"
    "cXVlc3QuUmVxdWVzdChVUkwsIGhlYWRlcnM9eyJVc2VyLUFnZW50IjogIk1vemlsbGEvNS4wIn0p"
    "CndpdGggdXJsbGliLnJlcXVlc3QudXJsb3BlbihyZXEsIHRpbWVvdXQ9NjAwKSBhcyByOgogICAg"
    "c3JjID0gci5yZWFkKCkKZHN0ID0gb3MucGF0aC5qb2luKCIva2FnZ2xlL3dvcmtpbmciLCAia2Fn"
    "Z2xlX25vdGVib29rLnB5IikKd2l0aCBvcGVuKGRzdCwgIndiIikgYXMgZjoKICAgIGYud3JpdGUo"
    "c3JjKQpwcmludCgicjI1IGJvb3RzdHJhcDogbm90ZWJvb2sgJSVkIGJ5dGVzIiAlJSBsZW4oc3Jj"
    "KSwgZmx1c2g9VHJ1ZSkKcnVucHkucnVuX3BhdGgoZHN0LCBydW5fbmFtZT0iX19tYWluX18iKQon"
    "JycKCgojIHIyNWMxNzogbWVzc2FnZSBpZCAtPiBzb25nIHRpdGxlIGZvciBwbGF5bGlzdCBtZXRh"
    "ZGF0YS9vcmRlcmluZwpNSURfVElUTEVfMjUgPSB7fQoKCmRlZiBfbG9nKG1zZyk6CiAgICBwcmlu"
    "dChmIltyMjUge3RpbWUuc3RyZnRpbWUoJyVIOiVNOiVTJyl9XSB7bXNnfSIsIGZsdXNoPVRydWUp"
    "CgoKZGVmIF9jZmcoKToKICAgICIiIlBhcnNlIFdaTUwtWCBjb25maWcuZW52IChLRVk9InZhbHVl"
    "IiBsaW5lcykgaW50byBhIGRpY3QuIiIiCiAgICBvdXQgPSB7fQogICAgdHJ5OgogICAgICAgIHdp"
    "dGggb3Blbihvcy5wYXRoLmpvaW4oV1pNTFhfRElSLCAiY29uZmlnLmVudiIpKSBhcyBmOgogICAg"
    "ICAgICAgICBmb3IgbGluZSBpbiBmOgogICAgICAgICAgICAgICAgbGluZSA9IGxpbmUuc3RyaXAo"
    "KQogICAgICAgICAgICAgICAgaWYgbm90IGxpbmUgb3IgbGluZS5zdGFydHN3aXRoKCIjIikgb3Ig"
    "Ij0iIG5vdCBpbiBsaW5lOgogICAgICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAg"
    "ICAgICBrLCB2ID0gbGluZS5zcGxpdCgiPSIsIDEpCiAgICAgICAgICAgICAgICBvdXRbay5zdHJp"
    "cCgpXSA9IHYuc3RyaXAoKS5zdHJpcCgnIicpLnN0cmlwKCInIikKICAgIGV4Y2VwdCBFeGNlcHRp"
    "b246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIG91dAoKCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KIyBU"
    "ZWxlZ3JhbSBCb3QgSFRUUCBBUEkgKG11bHRpcGFydCwgZmxvb2QtYXdhcmUpCiMgLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0KCmRlZiBfdGcodG9rZW4sIG1ldGhvZCwgZmllbGRzPU5vbmUsIHRpbWVvdXQ9MzAw"
    "KToKICAgICIiIlBPU1QgdG8gdGhlIEJvdCBBUEkuIFJldHVybnMgdGhlIHBhcnNlZCBKU09OIG9y"
    "IE5vbmUuIiIiCiAgICB1cmwgPSBmImh0dHBzOi8vYXBpLnRlbGVncmFtLm9yZy9ib3R7dG9rZW59"
    "L3ttZXRob2R9IgogICAgYm5kID0gZiItLS0td3pmaXhyMjV7dGltZS50aW1lKCl9IgogICAgcGFy"
    "dHMgPSBbXQoKICAgIGRlZiBfZmllbGQobmFtZSwgdmFsdWUpOgogICAgICAgIHBhcnRzLmFwcGVu"
    "ZCgKICAgICAgICAgICAgKAogICAgICAgICAgICAgICAgZiItLXtibmR9XHJcbkNvbnRlbnQtRGlz"
    "cG9zaXRpb246IGZvcm0tZGF0YTsgIgogICAgICAgICAgICAgICAgZiduYW1lPSJ7bmFtZX0iXHJc"
    "blxyXG57dmFsdWV9XHJcbicKICAgICAgICAgICAgKS5lbmNvZGUoKQogICAgICAgICkKCiAgICBm"
    "b3IgaywgdiBpbiAoZmllbGRzIG9yIHt9KS5pdGVtcygpOgogICAgICAgIF9maWVsZChrLCB2KQog"
    "ICAgYm9keSA9IGIiIi5qb2luKHBhcnRzKSArIGYiLS17Ym5kfS0tXHJcbiIuZW5jb2RlKCkKICAg"
    "IGZvciBfdHJ5IGluIHJhbmdlKDYpOgogICAgICAgIHRyeToKICAgICAgICAgICAgcmVxID0gdXJs"
    "bGliLnJlcXVlc3QuUmVxdWVzdCgKICAgICAgICAgICAgICAgIHVybCwKICAgICAgICAgICAgICAg"
    "IGRhdGE9Ym9keSwKICAgICAgICAgICAgICAgIGhlYWRlcnM9ewogICAgICAgICAgICAgICAgICAg"
    "ICJDb250ZW50LVR5cGUiOiBmIm11bHRpcGFydC9mb3JtLWRhdGE7IGJvdW5kYXJ5PXtibmR9IiwK"
    "ICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICkKICAgICAgICAgICAgd2l0aCB1cmxsaWIu"
    "cmVxdWVzdC51cmxvcGVuKHJlcSwgdGltZW91dD10aW1lb3V0KSBhcyByOgogICAgICAgICAgICAg"
    "ICAgcmV0dXJuIGpzb24ubG9hZHMoci5yZWFkKCkuZGVjb2RlKCkpCiAgICAgICAgZXhjZXB0IHVy"
    "bGxpYi5lcnJvci5IVFRQRXJyb3IgYXMgZToKICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAg"
    "ICAgZXJyID0ganNvbi5sb2FkcyhlLnJlYWQoKS5kZWNvZGUoKSkKICAgICAgICAgICAgZXhjZXB0"
    "IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGVyciA9IHt9CiAgICAgICAgICAgIGlmIGVyci5n"
    "ZXQoImVycm9yX2NvZGUiKSA9PSA0Mjk6CiAgICAgICAgICAgICAgICB3YWl0ID0gKAogICAgICAg"
    "ICAgICAgICAgICAgIGVyci5nZXQoInBhcmFtZXRlcnMiLCB7fSkuZ2V0KCJyZXRyeV9hZnRlciIs"
    "IDUpICsgMQogICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgX2xvZyhmInRnIHttZXRo"
    "b2R9OiBmbG9vZCDigJQgd2FpdGluZyB7d2FpdH1zIikKICAgICAgICAgICAgICAgIHRpbWUuc2xl"
    "ZXAod2FpdCkKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIGlmIGUuY29kZSBp"
    "biAoNTAwLCA1MDIsIDUwMyk6CiAgICAgICAgICAgICAgICB0aW1lLnNsZWVwKDUpCiAgICAgICAg"
    "ICAgICAgICBjb250aW51ZQogICAgICAgICAgICBfbG9nKGYidGcge21ldGhvZH06IEhUVFAge2Uu"
    "Y29kZX0ge3N0cihlcnIpWzoxNjBdfSIpCiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICBfbG9nKGYidGcge21ldGhvZH06IHtl"
    "fSIpCiAgICAgICAgICAgIHRpbWUuc2xlZXAoMykKICAgIHJldHVybiBOb25lCgoKZGVmIF9zZW5k"
    "X2F1ZGlvKHRva2VuLCBjaGF0LCBwYXRoLCBjYXB0aW9uKToKICAgICIiInNlbmRBdWRpbyB3aXRo"
    "IHRoZSBmaWxlIHN0cmVhbWVkIGZyb20gZGlzay4iIiIKICAgIHVybCA9IGYiaHR0cHM6Ly9hcGku"
    "dGVsZWdyYW0ub3JnL2JvdHt0b2tlbn0vc2VuZEF1ZGlvIgogICAgYm5kID0gZiItLS0td3pmaXhy"
    "MjV7dGltZS50aW1lKCl9IgogICAgcHJlID0gWwogICAgICAgICgKICAgICAgICAgICAgZiItLXti"
    "bmR9XHJcbkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgIgogICAgICAgICAgICBmJ25h"
    "bWU9ImNoYXRfaWQiXHJcblxyXG57Y2hhdH1cclxuJwogICAgICAgICkuZW5jb2RlKCksCiAgICAg"
    "ICAgKAogICAgICAgICAgICBmIi0te2JuZH1cclxuQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1k"
    "YXRhOyAiCiAgICAgICAgICAgIGYnbmFtZT0iY2FwdGlvbiJcclxuXHJcbntjYXB0aW9ufVxyXG4n"
    "CiAgICAgICAgKS5lbmNvZGUoKSwKICAgIF0KICAgIGZuYW1lID0gb3MucGF0aC5iYXNlbmFtZShw"
    "YXRoKQogICAgd2l0aCBvcGVuKHBhdGgsICJyYiIpIGFzIGY6CiAgICAgICAgZGF0YSA9IGYucmVh"
    "ZCgpCiAgICBwcmUuYXBwZW5kKAogICAgICAgICgKICAgICAgICAgICAgZiItLXtibmR9XHJcbkNv"
    "bnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgIgogICAgICAgICAgICBmJ25hbWU9ImF1ZGlv"
    "IjsgZmlsZW5hbWU9IntmbmFtZX0iXHJcbicKICAgICAgICAgICAgZiJDb250ZW50LVR5cGU6IGF1"
    "ZGlvL21wZWdcclxuXHJcbiIKICAgICAgICApLmVuY29kZSgpCiAgICApCiAgICBib2R5ID0gYiIi"
    "LmpvaW4ocHJlKSArIGRhdGEgKyBmIlxyXG4tLXtibmR9LS1cclxuIi5lbmNvZGUoKQogICAgIyBX"
    "WkZJWCByMjVjNyAodjE1LjgzLjcpOiBzZW5kQXVkaW8gaXMgZmxvb2QtYXdhcmUg4oCUIGFsbCB3"
    "b3JrZXIKICAgICMgc2Vzc2lvbnMgKGFuZCB0aGUgbGVhZGVyKSBzaGFyZSBvbmUgYm90IHRva2Vu"
    "LCBzbyBidXJzdHMgY29sbGlkZTsKICAgICMgYmFjayBvZmYgZm9yIHJldHJ5X2FmdGVyIGFuZCBy"
    "ZXRyeSBpbnN0ZWFkIG9mIGxvc2luZyB0aGUgc29uZy4KICAgIGZvciBfdHJ5IGluIHJhbmdlKDYp"
    "OgogICAgICAgIHRyeToKICAgICAgICAgICAgcmVxID0gdXJsbGliLnJlcXVlc3QuUmVxdWVzdCgK"
    "ICAgICAgICAgICAgICAgIHVybCwKICAgICAgICAgICAgICAgIGRhdGE9Ym9keSwKICAgICAgICAg"
    "ICAgICAgIGhlYWRlcnM9ewogICAgICAgICAgICAgICAgICAgICJDb250ZW50LVR5cGUiOiBmIm11"
    "bHRpcGFydC9mb3JtLWRhdGE7IGJvdW5kYXJ5PXtibmR9IgogICAgICAgICAgICAgICAgfSwKICAg"
    "ICAgICAgICAgKQogICAgICAgICAgICB3aXRoIHVybGxpYi5yZXF1ZXN0LnVybG9wZW4ocmVxLCB0"
    "aW1lb3V0PTMwMCkgYXMgcjoKICAgICAgICAgICAgICAgIHJldHVybiBqc29uLmxvYWRzKHIucmVh"
    "ZCgpLmRlY29kZSgpKQogICAgICAgIGV4Y2VwdCB1cmxsaWIuZXJyb3IuSFRUUEVycm9yIGFzIGU6"
    "CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGVyciA9IGpzb24ubG9hZHMoZS5yZWFk"
    "KCkuZGVjb2RlKCkpCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAg"
    "ICBlcnIgPSB7fQogICAgICAgICAgICBpZiBlcnIuZ2V0KCJlcnJvcl9jb2RlIikgPT0gNDI5IG9y"
    "IGUuY29kZSA9PSA0Mjk6CiAgICAgICAgICAgICAgICB3YWl0ID0gZXJyLmdldCgicGFyYW1ldGVy"
    "cyIsIHt9KS5nZXQoInJldHJ5X2FmdGVyIiwgNSkgKyAxCiAgICAgICAgICAgICAgICBfbG9nKGYi"
    "c2VuZEF1ZGlvOiBmbG9vZCDigJQgd2FpdGluZyB7d2FpdH1zIikKICAgICAgICAgICAgICAgIHRp"
    "bWUuc2xlZXAod2FpdCkKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIGlmIGUu"
    "Y29kZSBpbiAoNTAwLCA1MDIsIDUwMyk6CiAgICAgICAgICAgICAgICB0aW1lLnNsZWVwKDUpCiAg"
    "ICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgICAgICByYWlzZQogICAgICAgIGV4Y2VwdCBF"
    "eGNlcHRpb246CiAgICAgICAgICAgIGlmIF90cnkgPj0gNToKICAgICAgICAgICAgICAgIHJhaXNl"
    "CiAgICAgICAgICAgIHRpbWUuc2xlZXAoMykKICAgIHJldHVybiBOb25lCgoKIyAtLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLQojIERvd25sb2FkIGxhZGRlciAoWW91VHViZSAtPiB3b3JkaW5nIHZhcmlhbnQgLT4g"
    "SmlvU2Fhdm4pCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KCl9TQl9SRU1PVkUgPSBbCiAgICAic3BvbnNv"
    "ciIsCiAgICAic2VsZnByb21vIiwKICAgICJpbnRybyIsCiAgICAicHJldmlldyIsCiAgICAibXVz"
    "aWNfb2ZmdG9waWMiLApdCgoKZGVmIF95ZGxfb3B0cyhjb29raWVmaWxlLCBvdXRkaXIpOgogICAg"
    "b3B0cyA9IHsKICAgICAgICAiZm9ybWF0IjogImJhW2V4dD1tNGFdL2JhL2IiLAogICAgICAgICJv"
    "dXR0bXBsIjogb3MucGF0aC5qb2luKG91dGRpciwgIiUodGl0bGUpcy4lKGV4dClzIiksCiAgICAg"
    "ICAgInF1aWV0IjogVHJ1ZSwKICAgICAgICAibm9fd2FybmluZ3MiOiBUcnVlLAogICAgICAgICJu"
    "b3BsYXlsaXN0IjogVHJ1ZSwKICAgICAgICAicmV0cmllcyI6IDMsCiAgICAgICAgInNsZWVwX2lu"
    "dGVydmFsIjogMSwKICAgICAgICAibWF4X3NsZWVwX2ludGVydmFsIjogMywKICAgICAgICAic3Bv"
    "bnNvcmJsb2NrX3JlbW92ZSI6IF9TQl9SRU1PVkUsCiAgICAgICAgIm92ZXJ3cml0ZXMiOiBUcnVl"
    "LAogICAgICAgICJzb2NrZXRfdGltZW91dCI6IDMwLAogICAgfQogICAgaWYgY29va2llZmlsZSBh"
    "bmQgb3MucGF0aC5pc2ZpbGUoY29va2llZmlsZSk6CiAgICAgICAgb3B0c1siY29va2llZmlsZSJd"
    "ID0gY29va2llZmlsZQogICAgcmV0dXJuIG9wdHMKCgpkZWYgX3l0ZGxfZmV0Y2goc3BlYywgY29v"
    "a2llZmlsZSwgd29ya2Rpcik6CiAgICAiIiJSdW4geXQtZGxwIChtb2R1bGUgQVBJKSBvbiBvbmUg"
    "c3BlYy4gUmV0dXJucyAoZmlsZXBhdGgsIGluZm8pLiIiIgogICAgZnJvbSB5dF9kbHAgaW1wb3J0"
    "IFlvdXR1YmVETAoKICAgIHdpdGggWW91dHViZURMKF95ZGxfb3B0cyhjb29raWVmaWxlLCB3b3Jr"
    "ZGlyKSkgYXMgeToKICAgICAgICBpbmZvID0geS5leHRyYWN0X2luZm8oc3BlYywgZG93bmxvYWQ9"
    "VHJ1ZSkKICAgICAgICBpZiBpbmZvIGlzIE5vbmU6CiAgICAgICAgICAgIHJhaXNlIFJ1bnRpbWVF"
    "cnJvcigibm8gcmVzdWx0IikKICAgICAgICBpZiAiZW50cmllcyIgaW4gaW5mbzoKICAgICAgICAg"
    "ICAgZW50cyA9IFtlIGZvciBlIGluIChpbmZvLmdldCgiZW50cmllcyIpIG9yIFtdKSBpZiBlXQog"
    "ICAgICAgICAgICBpZiBub3QgZW50czoKICAgICAgICAgICAgICAgIHJhaXNlIFJ1bnRpbWVFcnJv"
    "cigibm8gc2VhcmNoIHJlc3VsdCIpCiAgICAgICAgICAgIGluZm8gPSBlbnRzWzBdCiAgICAgICAg"
    "cGF0aCA9IChpbmZvLmdldCgicmVxdWVzdGVkX2Rvd25sb2FkcyIpIG9yIFt7fV0pWzBdLmdldCgi"
    "ZmlsZXBhdGgiKQogICAgICAgIGlmIG5vdCBwYXRoIG9yIG5vdCBvcy5wYXRoLmlzZmlsZShwYXRo"
    "KToKICAgICAgICAgICAgcmFpc2UgUnVudGltZUVycm9yKCJubyBmaWxlIHByb2R1Y2VkIikKICAg"
    "ICAgICByZXR1cm4gcGF0aCwgaW5mbwoKCmRlZiBfc2Fhdm5fc2VhcmNoX3VybChhcnRpc3QsIHRp"
    "dGxlKToKICAgICIiIkppb1NhYXZuIHdlYi1BUEkgc2VhcmNoIC0+IGZpcnN0IHNvbmcgcGFnZSBV"
    "UkwgKG9yIE5vbmUpLiIiIgogICAgcSA9IHVybGxpYi5wYXJzZS5xdW90ZShmInthcnRpc3R9IHt0"
    "aXRsZX0iKQogICAgYXBpID0gKAogICAgICAgICJodHRwczovL3d3dy5qaW9zYWF2bi5jb20vYXBp"
    "LnBocD9fX2NhbGw9c2VhcmNoLmdldFJlc3VsdHMiCiAgICAgICAgZiImcT17cX0mX2Zvcm1hdD1q"
    "c29uJl9tYXJrZXI9MCZhcGlfdmVyc2lvbj00JmN0eD13ZWI2ZG90MCZuPTUmcD0xIgogICAgKQog"
    "ICAgcmVxID0gdXJsbGliLnJlcXVlc3QuUmVxdWVzdCgKICAgICAgICBhcGksIGhlYWRlcnM9eyJV"
    "c2VyLUFnZW50IjogIk1vemlsbGEvNS4wIn0KICAgICkKICAgIHdpdGggdXJsbGliLnJlcXVlc3Qu"
    "dXJsb3BlbihyZXEsIHRpbWVvdXQ9MzApIGFzIHI6CiAgICAgICAgZGF0YSA9IGpzb24ubG9hZHMo"
    "ci5yZWFkKCkuZGVjb2RlKCkpCiAgICByZXMgPSBkYXRhLmdldCgicmVzdWx0cyIpCiAgICBpZiBp"
    "c2luc3RhbmNlKHJlcywgZGljdCk6CiAgICAgICAgcmVzID0gcmVzLmdldCgic29uZ3MiKSBvciBb"
    "XQogICAgZm9yIHMgaW4gcmVzIG9yIFtdOgogICAgICAgIHUgPSBodG1sLnVuZXNjYXBlKAogICAg"
    "ICAgICAgICBzLmdldCgicGVybWFfdXJsIikgb3Igcy5nZXQoInBlcm1hVXJsIikgb3IgIiIKICAg"
    "ICAgICApCiAgICAgICAgaWYgdToKICAgICAgICAgICAgcmV0dXJuIHUKICAgIHJldHVybiBOb25l"
    "CgoKY2xhc3MgX0xhZGRlckZhaWwoRXhjZXB0aW9uKToKICAgIHBhc3MKCgpkZWYgX2xhZGRlcihh"
    "cnRpc3QsIHRpdGxlLCBjb29raWVmaWxlLCB3b3JrZGlyKToKICAgICIiIk9uZSBzb25nIHRocm91"
    "Z2ggdGhlIGZ1bGwgZmFsbGJhY2sgbGFkZGVyLgoKICAgIFJldHVybnMgKGZpbGVwYXRoLCBzb3Vy"
    "Y2UpLiBSYWlzZXMgX0xhZGRlckZhaWwgd2l0aCBldmVyeQogICAgY29sbGVjdGVkIHJlYXNvbiB3"
    "aGVuIGFsbCBwbGF0Zm9ybXMgZmFpbC4iIiIKICAgIGVycnMgPSBbXQogICAgX2xvZyhmImxhZGRl"
    "cjogJ3t0aXRsZX0nIOKAlCBZb3VUdWJlIikKICAgIGZvciBzdWZmaXggaW4gKCIiLCAiIGF1ZGlv"
    "Iik6CiAgICAgICAgc3BlYyA9IGYieXRzZWFyY2gxOnthcnRpc3R9IC0ge3RpdGxlfXtzdWZmaXh9"
    "Ii5zdHJpcCgpCiAgICAgICAgdHJ5OgogICAgICAgICAgICBwLCBfID0gX3l0ZGxfZmV0Y2goc3Bl"
    "YywgY29va2llZmlsZSwgd29ya2RpcikKICAgICAgICAgICAgcmV0dXJuIHAsICJZb3VUdWJlIgog"
    "ICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAgICAgZXJycy5hcHBlbmQoZiJ5"
    "dHsnKCthdWRpbyknIGlmIHN1ZmZpeCBlbHNlICcnfToge3N0cihlKVs6MTAwXX0iKQogICAgICAg"
    "ICAgICBfbG9nKGYibGFkZGVyOiAne3RpdGxlfScgeXR7c3VmZml4IXJ9IG1pc3Mg4oCUIHtzdHIo"
    "ZSlbOjEwMF19IikKICAgIF9sb2coZiJsYWRkZXI6ICd7dGl0bGV9JyDigJQgSmlvU2Fhdm4iKQog"
    "ICAgdHJ5OgogICAgICAgIHUgPSBfc2Fhdm5fc2VhcmNoX3VybChhcnRpc3QsIHRpdGxlKQogICAg"
    "ICAgIGlmIG5vdCB1OgogICAgICAgICAgICByYWlzZSBSdW50aW1lRXJyb3IoIm5vIEppb1NhYXZu"
    "IG1hdGNoIikKICAgICAgICBwLCBfID0gX3l0ZGxfZmV0Y2godSwgY29va2llZmlsZSwgd29ya2Rp"
    "cikKICAgICAgICByZXR1cm4gcCwgIkppb1NhYXZuIgogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBl"
    "OgogICAgICAgIGVycnMuYXBwZW5kKGYiamlvc2Fhdm46IHtzdHIoZSlbOjEwMF19IikKICAgIHJh"
    "aXNlIF9MYWRkZXJGYWlsKCI7ICIuam9pbihlcnJzKSkKCgpkZWYgX2Vuc3VyZV9wb3Rfc2VydmVy"
    "KCk6CiAgICAiIiJCZXN0LWVmZm9ydCBzdGFydCBvZiB0aGUgYmd1dGlsIFBPIHNlcnZlciAoaWYg"
    "bm90IHJ1bm5pbmcpLiIiIgogICAgdHJ5OgogICAgICAgIHdpdGggdXJsbGliLnJlcXVlc3QudXJs"
    "b3BlbigKICAgICAgICAgICAgImh0dHA6Ly8xMjcuMC4wLjE6NDQxNi9waW5nIiwgdGltZW91dD0z"
    "CiAgICAgICAgKSBhcyByOgogICAgICAgICAgICBpZiByLnN0YXR1cyA9PSAyMDA6CiAgICAgICAg"
    "ICAgICAgICByZXR1cm4KICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgdHJ5"
    "OgogICAgICAgIGt3ID0gb3MuZW52aXJvbi5nZXQoIktBR0dMRV9XT1JLSU5HIikgb3IgIi9rYWdn"
    "bGUvd29ya2luZyIKICAgICAgICBiZyA9IG9zLnBhdGguam9pbihrdywgImJpbiIsICJiZ3V0aWwt"
    "cG90IikKICAgICAgICBpZiBvcy5wYXRoLmlzZmlsZShiZyk6CiAgICAgICAgICAgIHN1YnByb2Nl"
    "c3MuUG9wZW4oCiAgICAgICAgICAgICAgICBbYmcsICJzZXJ2ZXIiLCAiLS1ob3N0IiwgIjEyNy4w"
    "LjAuMSIsICItLXBvcnQiLCAiNDQxNiJdLAogICAgICAgICAgICAgICAgc3Rkb3V0PXN1YnByb2Nl"
    "c3MuREVWTlVMTCwKICAgICAgICAgICAgICAgIHN0ZGVycj1zdWJwcm9jZXNzLkRFVk5VTEwsCiAg"
    "ICAgICAgICAgICkKICAgICAgICAgICAgX2xvZygiUE8gdG9rZW4gc2VydmVyIHN0YXJ0ZWQgKHdh"
    "cyBkb3duKSIpCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgX2xvZyhmIlBPIHNl"
    "cnZlciBzdGFydCBmYWlsZWQ6IHtlfSIpCgoKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQojIFJvbGUgMSDi"
    "gJQgaGVhZGxlc3Mgd29ya2VyIGpvYgojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCgojIC0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tCiMgV1pGSVggcjI1YzggKHYxNS44My44KTogdGhlIE1vbmdvIHN0YXR1cyBjaGFubmVs"
    "LiBXb3JrZXJzIHJlcG9ydCBldmVyeQojIHNvbmcncyBmaW5hbCBzdGF0ZSAoKyBhIHNtYWxsIGhl"
    "YXJ0YmVhdCkgaW50byBhIHNjcmF0Y2ggY29sbGVjdGlvbjsgdGhlCiMgbGVhZGVyIHJlYWRzIGl0"
    "IHRvIG1hcmsgdGhlIGNhdGFsb2cgY2FyZCAob25lIHNpbmdsZSB0ZXh0IGZvciB0aGUgd2hvbGUK"
    "IyBiYXRjaCDigJQgbm8gcGVyLXdvcmtlciBjaGF0IG1lc3NhZ2VzKS4gRXZlcnl0aGluZyBoZXJl"
    "IGlzIGJlc3QtZWZmb3J0CiMgYW5kIHNpbGVudDogbm8gTW9uZ28gLT4gdGhlIHdvcmtlciBmYWxs"
    "cyBiYWNrIHRvIGNoYXQgc3RhdHVzIG1lc3NhZ2VzLgpfTU9OR09fQ09MTCA9IE5vbmUKCiMgcjI1"
    "YzEzOiB3YWl0IGJldHdlZW4gZG93bmxvYWQgLyB1cGxvYWQgcmV0cnkgYXR0ZW1wdHMgKHNlY29u"
    "ZHMpCl9ETF9XQUlUMjUgPSAxNQpfVVBfV0FJVDI1ID0gMTAKCgpkZWYgX21vbmdvX2NvbGwoKToK"
    "ICAgICIiIkxhenkgY29ubmVjdGlvbiB0byB0aGUgc3RhdHVzIGNvbGxlY3Rpb24gKG9yIE5vbmUp"
    "LiIiIgogICAgZ2xvYmFsIF9NT05HT19DT0xMCiAgICBpZiBfTU9OR09fQ09MTCBpcyBub3QgTm9u"
    "ZToKICAgICAgICAjIE5COiBhIHB5bW9uZ28gQ29sbGVjdGlvbiBoYXMgbm8gdHJ1dGggdmFsdWUg"
    "4oCUIGNvbXBhcmUgd2l0aAogICAgICAgICMgYGlzYCwgbmV2ZXIgYm9vbC10ZXN0IGl0IChyMjVj"
    "OCBzaGlwcGVkIGBvciBOb25lYCBoZXJlIGFuZAogICAgICAgICMgaXQgY3Jhc2hlZCB0aGUgbGVh"
    "ZGVyIHNsaWNlICsgZXZlcnkgd29ya2VyIGF0IHN0YXJ0dXApCiAgICAgICAgcmV0dXJuIE5vbmUg"
    "aWYgX01PTkdPX0NPTEwgaXMgRmFsc2UgZWxzZSBfTU9OR09fQ09MTAogICAgdXJsID0gKAogICAg"
    "ICAgIF9jZmcoKS5nZXQoIkRBVEFCQVNFX1VSTCIpCiAgICAgICAgb3Igb3MuZW52aXJvbi5nZXQo"
    "IkRBVEFCQVNFX1VSTCIpCiAgICAgICAgb3IgIiIKICAgICkuc3RyaXAoKQogICAgaWYgbm90IHVy"
    "bDoKICAgICAgICBfTU9OR09fQ09MTCA9IEZhbHNlCiAgICAgICAgcmV0dXJuIE5vbmUKICAgIHRy"
    "eToKICAgICAgICBpbXBvcnQgcHltb25nbwogICAgZXhjZXB0IEltcG9ydEVycm9yOgogICAgICAg"
    "IHRyeToKICAgICAgICAgICAgc3VicHJvY2Vzcy5ydW4oCiAgICAgICAgICAgICAgICBbc3lzLmV4"
    "ZWN1dGFibGUsICItbSIsICJwaXAiLCAiaW5zdGFsbCIsICItcSIsICJweW1vbmdvIl0sCiAgICAg"
    "ICAgICAgICAgICB0aW1lb3V0PTI0MCwKICAgICAgICAgICAgICAgIGNoZWNrPUZhbHNlLAogICAg"
    "ICAgICAgICAgICAgY2FwdHVyZV9vdXRwdXQ9VHJ1ZSwKICAgICAgICAgICAgKQogICAgICAgICAg"
    "ICBpbXBvcnQgcHltb25nbwogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIF9s"
    "b2coInIyNWM4OiBweW1vbmdvIHVuYXZhaWxhYmxlIC0gY2hhdCBzdGF0dXMgZmFsbGJhY2siKQog"
    "ICAgICAgICAgICBfTU9OR09fQ09MTCA9IEZhbHNlCiAgICAgICAgICAgIHJldHVybiBOb25lCiAg"
    "ICB0cnk6CiAgICAgICAgX2MgPSBweW1vbmdvLk1vbmdvQ2xpZW50KHVybCwgc2VydmVyU2VsZWN0"
    "aW9uVGltZW91dE1TPTgwMDApCiAgICAgICAgX2Muc2VydmVyX2luZm8oKQogICAgICAgIF9NT05H"
    "T19DT0xMID0gX2NbInd6Zml4X3IyNSJdWyJzdGF0dXMiXQogICAgICAgIHJldHVybiBfTU9OR09f"
    "Q09MTAogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIF9sb2coZiJyMjVjODogbW9u"
    "Z28gc3RhdHVzIGNoYW5uZWwgdW5hdmFpbGFibGUgLSB7ZX0iKQogICAgICAgIF9NT05HT19DT0xM"
    "ID0gRmFsc2UKICAgICAgICByZXR1cm4gTm9uZQoKCmRlZiByZXBvcnRfc3RhdHVzKHJ1bl9pZCwg"
    "d2lkeCwgdGl0bGUsIG9rLCByZWFzb249IiIsIG1pZD1Ob25lKToKICAgICIiIk9uZSBzb25nJ3Mg"
    "ZmluYWwgc3RhdGUgKHNpbGVudCwgdXBzZXJ0KS4iIiIKICAgIGlmIG5vdCBydW5faWQ6CiAgICAg"
    "ICAgcmV0dXJuCiAgICBjb2xsID0gX21vbmdvX2NvbGwoKQogICAgaWYgY29sbCBpcyBOb25lOgog"
    "ICAgICAgIHJldHVybgogICAgdHJ5OgogICAgICAgICMgcjI1YzExOiBvbmx5IHNldCAibSIgd2hl"
    "biBhIHJlYWwgbWVzc2FnZSBpZCBpcyBwYXNzZWQg4oCUCiAgICAgICAgIyB0aGUgZmluYWwgY29u"
    "c2lzdGVuY3kgcGFzcyBydW5zIHdpdGhvdXQgb25lIGFuZCB1c2VkIHRvCiAgICAgICAgIyB3aXBl"
    "IGl0LCB3aGljaCBlbXB0aWVkIHRoZSBwbGF5bGlzdCBhdCB0aGUgbGFzdCBtb21lbnQKICAgICAg"
    "ICBfc2V0MjUgPSB7CiAgICAgICAgICAgICJzdCI6ICJvayIgaWYgb2sgZWxzZSAiZmFpbCIsCiAg"
    "ICAgICAgICAgICJyIjogKHJlYXNvbiBvciAiIilbOjEyMF0sCiAgICAgICAgICAgICJ3Ijogd2lk"
    "eCwKICAgICAgICAgICAgInQiOiB0aW1lLnRpbWUoKSwKICAgICAgICB9CiAgICAgICAgaWYgbWlk"
    "IGlzIG5vdCBOb25lOgogICAgICAgICAgICBfc2V0MjVbIm0iXSA9IG1pZAogICAgICAgICMgcjI1"
    "YzE3OiByZW1lbWJlciBtaWQgLT4gdGl0bGUgc28gdGhlIHBsYXlsaXN0IHBhZ2UgY2FuCiAgICAg"
    "ICAgIyBvcmRlciBzb25ncyBieSBwb3B1bGFyaXR5IGFuZCBhdHRhY2ggY2xlYW4gbmFtZXMvY292"
    "ZXJzCiAgICAgICAgaWYgbWlkIGlzIG5vdCBOb25lIGFuZCB0aXRsZToKICAgICAgICAgICAgTUlE"
    "X1RJVExFXzI1W3N0cihtaWQpXSA9IHRpdGxlCiAgICAgICAgY29sbC51cGRhdGVfb25lKAogICAg"
    "ICAgICAgICB7Il9pZCI6IGYie3J1bl9pZH18e3RpdGxlfSJ9LAogICAgICAgICAgICB7IiRzZXQi"
    "OiBfc2V0MjV9LAogICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICAgICApCiAgICBleGNlcHQg"
    "RXhjZXB0aW9uOgogICAgICAgIHBhc3MKCgpkZWYgcmVwb3J0X3N0YWdlKHJ1bl9pZCwgd2lkeCwg"
    "dGl0bGUsIHN0YWdlLCByZWFzb249IiIpOgogICAgIiIicjI1YzEzOiBwZXItc29uZyBMSVZFIHN0"
    "YWdlIOKAlCBkbCAvIHVwIC8gd3IgKHdhaXRpbmcsIHJldHJ5aW5nKS4KCiAgICBOZXZlciB0b3Vj"
    "aGVzIHRoZSBzdG9yZWQgbWVzc2FnZSBpZCwgc28gdGhlIHBsYXlsaXN0IHByb21wdCBpcwogICAg"
    "c2FmZS4gVGhlIGxlYWRlcidzIGNhdGFsb2cgY2FyZCByZW5kZXJzIHRoZXNlIGFzIHBlbmRpbmcg"
    "c3RhdGVzOgogICAgImRvd25sb2FkaW5nIiwgInVwbG9hZGluZyIsICJ3YWl0aW5nIChyZWFzb24p"
    "Ii4KICAgICIiIgogICAgaWYgbm90IHJ1bl9pZDoKICAgICAgICByZXR1cm4KICAgIGNvbGwgPSBf"
    "bW9uZ29fY29sbCgpCiAgICBpZiBjb2xsIGlzIE5vbmU6CiAgICAgICAgcmV0dXJuCiAgICB0cnk6"
    "CiAgICAgICAgY29sbC51cGRhdGVfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYie3J1bl9pZH18"
    "e3RpdGxlfSJ9LAogICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAiJHNldCI6IHsKICAgICAg"
    "ICAgICAgICAgICAgICAic3QiOiBzdGFnZSwKICAgICAgICAgICAgICAgICAgICAiciI6IChyZWFz"
    "b24gb3IgIiIpWzoxMjBdLAogICAgICAgICAgICAgICAgICAgICJ3Ijogd2lkeCwKICAgICAgICAg"
    "ICAgICAgICAgICAidCI6IHRpbWUudGltZSgpLAogICAgICAgICAgICAgICAgfQogICAgICAgICAg"
    "ICB9LAogICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uOgogICAgICAgIHBhc3MKCgpkZWYgcmVwb3J0X21ldGEocnVuX2lkLCB3aWR4LCBkb25lLCB0"
    "b3RhbCwgcGhhc2UpOgogICAgIiIiV29ya2VyIGhlYXJ0YmVhdCAoc2lsZW50LCB1cHNlcnQpLiIi"
    "IgogICAgaWYgbm90IHJ1bl9pZDoKICAgICAgICByZXR1cm4KICAgIGNvbGwgPSBfbW9uZ29fY29s"
    "bCgpCiAgICBpZiBjb2xsIGlzIE5vbmU6CiAgICAgICAgcmV0dXJuCiAgICB0cnk6CiAgICAgICAg"
    "Y29sbC51cGRhdGVfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYie3J1bl9pZH18bWV0YXt3aWR4"
    "fSJ9LAogICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAiJHNldCI6IHsKICAgICAgICAgICAg"
    "ICAgICAgICAiZCI6IGRvbmUsCiAgICAgICAgICAgICAgICAgICAgIm4iOiB0b3RhbCwKICAgICAg"
    "ICAgICAgICAgICAgICAicGgiOiBwaGFzZSwKICAgICAgICAgICAgICAgICAgICAidCI6IHRpbWUu"
    "dGltZSgpLAogICAgICAgICAgICAgICAgfQogICAgICAgICAgICB9LAogICAgICAgICAgICB1cHNl"
    "cnQ9VHJ1ZSwKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKCgpk"
    "ZWYgZmV0Y2hfc3RhdHVzKHJ1bl9pZCk6CiAgICAiIiJBbGwgZG9jcyBmb3IgYSBydW46IHsnc29u"
    "Z3MnOiB7dGl0bGU6IGRvY30sICdtZXRhTic6IGRvY30uIiIiCiAgICBvdXQgPSB7fQogICAgdHJ5"
    "OgogICAgICAgIGNvbGwgPSBfbW9uZ29fY29sbCgpCiAgICAgICAgaWYgY29sbCBpcyBOb25lIG9y"
    "IG5vdCBydW5faWQ6CiAgICAgICAgICAgIHJldHVybiBvdXQKICAgICAgICBwYXQgPSAiXiIgKyBy"
    "dW5faWQgKyAiXFx8IgogICAgICAgIGZvciBkIGluIGNvbGwuZmluZCh7Il9pZCI6IHsiJHJlZ2V4"
    "IjogcGF0fX0pOgogICAgICAgICAgICBrID0gZFsiX2lkIl0uc3BsaXQoInwiLCAxKVstMV0KICAg"
    "ICAgICAgICAgaWYgay5zdGFydHN3aXRoKCJtZXRhIik6CiAgICAgICAgICAgICAgICBvdXRba10g"
    "PSBkCiAgICAgICAgICAgIGVsc2U6CiAgICAgICAgICAgICAgICBvdXQuc2V0ZGVmYXVsdCgic29u"
    "Z3MiLCB7fSlba10gPSBkCiAgICAgICAgICAgICAgICAjIHIyNWMxNzogbGVhZGVyLXNpZGUgbWlk"
    "IC0+IHRpdGxlIG1hcCAod29ya2VyIHNvbmdzCiAgICAgICAgICAgICAgICAjIGxhbmQgaGVyZSB2"
    "aWEgTW9uZ287IHRoZSBsZWFkZXIncyBvd24gc2xpY2UgdmlhCiAgICAgICAgICAgICAgICAjIHJl"
    "cG9ydF9zdGF0dXMgaW4gdGhpcyBwcm9jZXNzKQogICAgICAgICAgICAgICAgaWYgZC5nZXQoIm0i"
    "KToKICAgICAgICAgICAgICAgICAgICBNSURfVElUTEVfMjVbc3RyKGRbIm0iXSldID0gawogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gb3V0CgoKZGVmIGNsZWFu"
    "dXBfc3RhdHVzKHJ1bl9pZCk6CiAgICAiIiJEcm9wIHRoaXMgcnVuJ3MgZG9jcyBhZnRlciB0aGUg"
    "YmF0Y2ggc3VtbWFyeSAoc2lsZW50KS4iIiIKICAgIGlmIG5vdCBydW5faWQ6CiAgICAgICAgcmV0"
    "dXJuCiAgICB0cnk6CiAgICAgICAgY29sbCA9IF9tb25nb19jb2xsKCkKICAgICAgICBpZiBjb2xs"
    "IGlzIG5vdCBOb25lOgogICAgICAgICAgICBjb2xsLmRlbGV0ZV9tYW55KHsiX2lkIjogeyIkcmVn"
    "ZXgiOiBmIl57cnVuX2lkfVxcfCJ9fSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFz"
    "cwoKCmFzeW5jIGRlZiBydW5fd29ya2VyX2pvYihqb2I9Tm9uZSk6CiAgICAiIiJSdW4gb25lIHNs"
    "aWNlLiBXb3JrZXJzIGdldCB0aGVpciBqb2IgZnJvbSBXWkZJWF9KT0JfQjY0OyB0aGUKICAgIGxl"
    "YWRlciAoUGhhc2UgQikgY2FuIHBhc3MgdGhlIGpvYiBkaWN0IGRpcmVjdGx5LiIiIgogICAgaWYg"
    "am9iIGlzIE5vbmU6CiAgICAgICAgam9iID0ganNvbi5sb2FkcygKICAgICAgICAgICAgYmFzZTY0"
    "LmI2NGRlY29kZShvcy5lbnZpcm9uWyJXWkZJWF9KT0JfQjY0Il0pLmRlY29kZSgpCiAgICAgICAg"
    "KQogICAgYXJ0aXN0ID0gam9iLmdldCgiYXJ0aXN0Iikgb3IgIm11c2ljIgogICAgdGl0bGVzID0g"
    "am9iLmdldCgidGl0bGVzIikgb3IgW10KICAgIHdpZHggPSBqb2IuZ2V0KCJ3b3JrZXIiKSBvciAw"
    "CiAgICBjZmcgPSBfY2ZnKCkKICAgIHRva2VuID0gam9iLmdldCgiYm90X3Rva2VuIikgb3IgY2Zn"
    "LmdldCgiQk9UX1RPS0VOIikgb3IgIiIKICAgIGNoYXQgPSBzdHIoam9iLmdldCgiY2hhdF9pZCIp"
    "IG9yIGNmZy5nZXQoIkxFRUNIX0xPR19DSEFUIikgb3IgIiIpCiAgICBydW5faWQgPSBqb2IuZ2V0"
    "KCJydW5faWQiKSBvciAiIgogICAgd29ya2RpciA9IHRlbXBmaWxlLm1rZHRlbXAocHJlZml4PWYi"
    "d3pmaXhfd3t3aWR4fV8iKQogICAgY29va2llZmlsZSA9IE5vbmUKICAgIGlmIGpvYi5nZXQoImNv"
    "b2tpZV9iNjQiKToKICAgICAgICBjb29raWVmaWxlID0gb3MucGF0aC5qb2luKHdvcmtkaXIsICJj"
    "b29raWVzLnR4dCIpCiAgICAgICAgd2l0aCBvcGVuKGNvb2tpZWZpbGUsICJ3YiIpIGFzIGY6CiAg"
    "ICAgICAgICAgIGYud3JpdGUoYmFzZTY0LmI2NGRlY29kZShqb2JbImNvb2tpZV9iNjQiXSkpCiAg"
    "ICAgICAgX2xvZyhmIndvcmtlciB7d2lkeH06IGNvb2tpZXMgaW1wb3J0ZWQgKHtvcy5wYXRoLmdl"
    "dHNpemUoY29va2llZmlsZSl9IGJ5dGVzKSIpCiAgICBlbHNlOgogICAgICAgIF9sb2coIndvcmtl"
    "ciB7MH06IG5vIGNvb2tpZXMgaW4gam9iIOKAlCBydW5uaW5nIHdpdGhvdXQiLmZvcm1hdCh3aWR4"
    "KSkKICAgIF9lbnN1cmVfcG90X3NlcnZlcigpCiAgICBfbG9nKAogICAgICAgIGYid29ya2VyIHt3"
    "aWR4fToge2xlbih0aXRsZXMpfSBzb25nKHMpIGZvciAne2FydGlzdH0nICIKICAgICAgICBmIi0+"
    "IGNoYXQge2NoYXR9IgogICAgKQoKICAgICMgcjI1Yzg6IHdpdGggdGhlIE1vbmdvIHN0YXR1cyBj"
    "aGFubmVsIGFsaXZlIHRoZSB3b3JrZXIgcG9zdHMgTk8KICAgICMgY2hhdCBtZXNzYWdlcyBhdCBh"
    "bGwg4oCUIHRoZSBsZWFkZXIncyBjYXRhbG9nIGNhcmQgKG9uZSBzaW5nbGUKICAgICMgdGV4dCkg"
    "c2hvd3MgcHJvZ3Jlc3MuIFdpdGhvdXQgTW9uZ28sIGZhbGwgYmFjayB0byB0aGUgb2xkCiAgICAj"
    "IHNlbGYtZWRpdGluZyBzdGF0dXMgbGluZS4KICAgIF9tYzI1ID0gYXdhaXQgYXN5bmNpby50b190"
    "aHJlYWQoX21vbmdvX2NvbGwpCiAgICBfbWdvMjUgPSBib29sKHJ1bl9pZCkgYW5kIF9tYzI1IGlz"
    "IG5vdCBOb25lCiAgICBpZiBfbWdvMjU6CiAgICAgICAgYXdhaXQgYXN5bmNpby50b190aHJlYWQo"
    "CiAgICAgICAgICAgIHJlcG9ydF9tZXRhLCBydW5faWQsIHdpZHgsIDAsIGxlbih0aXRsZXMpLCAi"
    "ZGwiCiAgICAgICAgKQogICAgc3RhdHVzX21pZCA9IE5vbmUKICAgIGlmIHRva2VuIGFuZCBjaGF0"
    "IGFuZCBub3QgX21nbzI1OgogICAgICAgIHIgPSBfdGcoCiAgICAgICAgICAgIHRva2VuLAogICAg"
    "ICAgICAgICAic2VuZE1lc3NhZ2UiLAogICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAiY2hh"
    "dF9pZCI6IGNoYXQsCiAgICAgICAgICAgICAgICAidGV4dCI6IGYi4pqZ77iPIDxiPndvcmtlciB7"
    "d2lkeH08L2I+OiBzdGFydGluZyDigJQge2xlbih0aXRsZXMpfSBzb25nKHMpIGZvciB7YXJ0aXN0"
    "fSIsCiAgICAgICAgICAgIH0sCiAgICAgICAgKQogICAgICAgIHRyeToKICAgICAgICAgICAgc3Rh"
    "dHVzX21pZCA9IChyLmdldCgicmVzdWx0Iikgb3Ige30pLmdldCgibWVzc2FnZV9pZCIpCiAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgc3RhdHVzX21pZCA9IE5vbmUKCiAgICAj"
    "IFdaRklYIHIyNWMxNCAodjE1LjgzLjE0KTogb3JkZXJlZCBwaXBlbGluZS4gRG93bmxvYWRzIHN0"
    "aWxsCiAgICAjIHJ1biBpbiBwYXJhbGxlbCwgYnV0IHVwbG9hZHMgYXJlIFNFUVVFTlRJQUwgaW4g"
    "dGhlIG9yaWdpbmFsCiAgICAjIHNsaWNlIG9yZGVyIOKAlCBhIHNvbmcgZ29lcyBvdXQgYXMgc29v"
    "biBhcyBpdCBpcyByZWFkeSBBTkQgZXZlcnkKICAgICMgc29uZyBiZWZvcmUgaXQgaGFzIGJlZW4g"
    "c2VudCAodXNlciBwcmVmZXJzIG9yZGVyZWQgYnVyc3RzIG92ZXIKICAgICMgcmF3IHNwZWVkKS4g"
    "UmV0cmllcyB3aXRoIHdhaXQgYW5kIHRoZSBsaXZlIHBlci1zb25nIGNhcmQKICAgICMgc3RhdHVz"
    "ICjirIcgZG93bmxvYWRpbmcgwrcg4o+rIHVwbG9hZGluZyDCtyDij7Mgd2FpdGluZyAocmVhc29u"
    "KSDCtwogICAgIyDinIUgdXBsb2FkZWQgwrcg4p2MIGZhaWxlZCAocmVhc29uKSkgYXJlIGtlcHQg"
    "ZnJvbSByMjVjMTMuCiAgICBfZGxfZG9uZTI1ID0ge30KICAgIF9kbF9ldnQyNSA9IHt0OiBhc3lu"
    "Y2lvLkV2ZW50KCkgZm9yIHQgaW4gdGl0bGVzfQogICAgb2sgPSBbXQogICAgZmFpbCA9IFtdCiAg"
    "ICBzZW50X2lkcyA9IFtdCiAgICBfZGxzZW0yNSA9IGFzeW5jaW8uU2VtYXBob3JlKGludChqb2Iu"
    "Z2V0KCJjb25jdXJyZW5jeSIpIG9yIDYpKQoKICAgIGRlZiBfZmJfbGluZTI1KCk6CiAgICAgICAg"
    "IyBub24tTW9uZ28gZmFsbGJhY2s6IG9uZSBlZGl0ZWQgc3RhdHVzIGxpbmUgd2l0aCBjb3VudHMK"
    "ICAgICAgICBpZiBzdGF0dXNfbWlkIGFuZCB0b2tlbiBhbmQgY2hhdDoKICAgICAgICAgICAgX3Rn"
    "KAogICAgICAgICAgICAgICAgdG9rZW4sCiAgICAgICAgICAgICAgICAiZWRpdE1lc3NhZ2VUZXh0"
    "IiwKICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAiY2hhdF9pZCI6IGNoYXQs"
    "CiAgICAgICAgICAgICAgICAgICAgIm1lc3NhZ2VfaWQiOiBzdGF0dXNfbWlkLAogICAgICAgICAg"
    "ICAgICAgICAgICJ0ZXh0IjogKAogICAgICAgICAgICAgICAgICAgICAgICBmIuKame+4jyA8Yj53"
    "b3JrZXIge3dpZHh9PC9iPjogIgogICAgICAgICAgICAgICAgICAgICAgICBmIuKchSB7bGVuKG9r"
    "KX0g4oCiIOKdjCB7bGVuKGZhaWwpfSAiCiAgICAgICAgICAgICAgICAgICAgICAgIGYiLyB7bGVu"
    "KHRpdGxlcyl9IgogICAgICAgICAgICAgICAgICAgICksCiAgICAgICAgICAgICAgICB9LAogICAg"
    "ICAgICAgICApCgogICAgYXN5bmMgZGVmIF9kbDI1KHRpdGxlKToKICAgICAgICBhc3luYyB3aXRo"
    "IF9kbHNlbTI1OgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBpZiBfbWdvMjU6CiAg"
    "ICAgICAgICAgICAgICAgICAgYXdhaXQgYXN5bmNpby50b190aHJlYWQoCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgIHJlcG9ydF9zdGFnZSwgcnVuX2lkLCB3aWR4LCB0aXRsZSwgImRsIgogICAgICAg"
    "ICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIF9wMjUgPSBOb25lCiAgICAgICAgICAgICAg"
    "ICBfc3JjMjUgPSAiIgogICAgICAgICAgICAgICAgZm9yIF9hdHQyNSBpbiByYW5nZSgzKToKICAg"
    "ICAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgICAgIF9wMjUsIF9zcmMy"
    "NSA9IGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "X2xhZGRlciwgYXJ0aXN0LCB0aXRsZSwgY29va2llZmlsZSwKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIHdvcmtkaXIsCiAgICAgICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgYnJlYWsKICAgICAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6"
    "CiAgICAgICAgICAgICAgICAgICAgICAgIF9yc24yNSA9IHN0cihlKVs6MTYwXQogICAgICAgICAg"
    "ICAgICAgICAgICAgICBpZiBfYXR0MjUgPj0gMjoKICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "IGZhaWwuYXBwZW5kKCh0aXRsZSwgX3JzbjI1KSkKICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "IF9sb2coCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZiJ3b3JrZXIge3dpZHh9OiAn"
    "e3RpdGxlfScgRkFJTEVEICIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmImFmdGVy"
    "IDMgdHJpZXMg4oCUIHtfcnNuMjV9IgogICAgICAgICAgICAgICAgICAgICAgICAgICAgKQogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgaWYgX21nbzI1OgogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICByZXBvcnRfc3RhdHVzLCBydW5faWQsIHdpZHgsCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgIHRpdGxlLCBGYWxzZSwgX3JzbjI1LAogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICAgICAgICAgIF9mYl9saW5l"
    "MjUoKQogICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuCiAgICAgICAgICAgICAgICAg"
    "ICAgICAgIGlmIF9tZ28yNToKICAgICAgICAgICAgICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5j"
    "aW8udG9fdGhyZWFkKAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcG9ydF9zdGFn"
    "ZSwgcnVuX2lkLCB3aWR4LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRpdGxlLCAi"
    "d3IiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYicmV0cnkge19hdHQyNSArIDJ9"
    "LzMg4oCUIHtfcnNuMjV9IiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICkKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgX2xvZygKICAgICAgICAgICAgICAgICAgICAgICAgICAgIGYid29ya2Vy"
    "IHt3aWR4fTogJ3t0aXRsZX0nIGRsIGZhaWxlZCAiCiAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ICBmIih7X3JzbjI1fSkg4oCUIHdhaXRpbmcgMTVzLCAiCiAgICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICBmInJldHJ5IHtfYXR0MjUgKyAyfS8zIgogICAgICAgICAgICAgICAgICAgICAgICApCiAg"
    "ICAgICAgICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8uc2xlZXAoX0RMX1dBSVQyNSkKICAg"
    "ICAgICAgICAgICAgIF9sb2coCiAgICAgICAgICAgICAgICAgICAgZiJ3b3JrZXIge3dpZHh9OiAn"
    "e3RpdGxlfScgcmVhZHkgdmlhIHtfc3JjMjV9IgogICAgICAgICAgICAgICAgKQogICAgICAgICAg"
    "ICAgICAgX2RsX2RvbmUyNVt0aXRsZV0gPSAoIm9rIiwgX3AyNSkKICAgICAgICAgICAgZmluYWxs"
    "eToKICAgICAgICAgICAgICAgIF9kbF9kb25lMjUuc2V0ZGVmYXVsdCh0aXRsZSwgKCJmYWlsIiwg"
    "Tm9uZSkpCiAgICAgICAgICAgICAgICBfZGxfZXZ0MjVbdGl0bGVdLnNldCgpCgogICAgYXN5bmMg"
    "ZGVmIF91cDI1KCk6CiAgICAgICAgIyByMjVjMTY6IGV2ZXJ5IGRvd25sb2FkIGluIHRoaXMgc2xp"
    "Y2UgZmluaXNoZXMgQkVGT1JFIHRoZQogICAgICAgICMgZmlyc3QgdXBsb2FkIOKAlCB0aGVuIHNv"
    "bmdzIGFyZSBzZW50IG9uZSBieSBvbmUgaW4gdGhlCiAgICAgICAgIyBvcmlnaW5hbCBvcmRlcgog"
    "ICAgICAgIGF3YWl0IGFzeW5jaW8uZ2F0aGVyKCpbX2RsX2V2dDI1W3RdLndhaXQoKSBmb3IgdCBp"
    "biB0aXRsZXNdKQogICAgICAgIF9zZW50X2FueTI1ID0gRmFsc2UKICAgICAgICBmb3IgX3QyNSBp"
    "biB0aXRsZXM6CiAgICAgICAgICAgIGF3YWl0IF9kbF9ldnQyNVtfdDI1XS53YWl0KCkKICAgICAg"
    "ICAgICAgX3IyNXggPSBfZGxfZG9uZTI1LmdldChfdDI1KQogICAgICAgICAgICBpZiBub3QgX3Iy"
    "NXggb3IgX3IyNXhbMF0gIT0gIm9rIjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlICAjIGZhaWxl"
    "ZCBpbiBkb3dubG9hZCDigJQgYWxyZWFkeSByZXBvcnRlZAogICAgICAgICAgICBfcDI1ID0gX3Iy"
    "NXhbMV0KICAgICAgICAgICAgaWYgX3NlbnRfYW55MjU6CiAgICAgICAgICAgICAgICBhd2FpdCBh"
    "c3luY2lvLnNsZWVwKDEuMikKICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgaWYgX21n"
    "bzI1OgogICAgICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAg"
    "ICAgICAgICAgICAgICAgICByZXBvcnRfc3RhZ2UsIHJ1bl9pZCwgd2lkeCwgX3QyNSwgInVwIgog"
    "ICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIF9yZXNwMjUgPSBOb25lCiAgICAg"
    "ICAgICAgICAgICAjIHIyNWMxNzogY2xlYW4gVGVsZWdyYW0gZmlsZW5hbWUg4oCUIHRoZSByYXcg"
    "eXQtZGxwCiAgICAgICAgICAgICAgICAjIG5hbWUgKEF1a2FhdF9PZmZpY2lhbF9WaWRlb18uLi4p"
    "IGJlY29tZXMKICAgICAgICAgICAgICAgICMgIkFydGlzdCAtIFRpdGxlLm1wMyIKICAgICAgICAg"
    "ICAgICAgIF9wMjVjID0gX3AyNQogICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAg"
    "ICAgIF9jZm4yNSA9IHJlLnN1YigKICAgICAgICAgICAgICAgICAgICAgICAgcidbXFwvOio/Ijw+"
    "fFx4MDAtXHgxZl0rJywgIiIsCiAgICAgICAgICAgICAgICAgICAgICAgIGYie2FydGlzdH0gLSB7"
    "X3QyNX0iLAogICAgICAgICAgICAgICAgICAgICkuc3RyaXAoKSBvciBfdDI1CiAgICAgICAgICAg"
    "ICAgICAgICAgX3AyNWMgPSBvcy5wYXRoLmpvaW4oCiAgICAgICAgICAgICAgICAgICAgICAgIG9z"
    "LnBhdGguZGlybmFtZShfcDI1KSwKICAgICAgICAgICAgICAgICAgICAgICAgX2NmbjI1WzoxMjBd"
    "ICsgIi5tcDMiLAogICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICBvcy5y"
    "ZXBsYWNlKF9wMjUsIF9wMjVjKQogICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAg"
    "ICAgICAgICAgICAgICAgICBfcDI1YyA9IF9wMjUKICAgICAgICAgICAgICAgIGZvciBfYXR0MjUg"
    "aW4gcmFuZ2UoMyk6CiAgICAgICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgICAg"
    "ICAgICBfcmVzcDI1ID0gYXdhaXQgYXN5bmNpby50b190aHJlYWQoCiAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICBfc2VuZF9hdWRpbywKICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRva2Vu"
    "LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgY2hhdCwKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgIF9wMjVjLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgZiLwn46nIHthcnRpc3R9"
    "IOKAlCB7X3QyNX0iLAogICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAg"
    "ICAgICAgIGJyZWFrCiAgICAgICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgog"
    "ICAgICAgICAgICAgICAgICAgICAgICBfcnNuMjUgPSBmInNlbmQ6IHtzdHIoZSlbOjEwMF19Igog"
    "ICAgICAgICAgICAgICAgICAgICAgICBpZiBfYXR0MjUgPj0gMjoKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgIHJhaXNlCiAgICAgICAgICAgICAgICAgICAgICAgIGlmIF9tZ28yNToKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgIHJlcG9ydF9zdGFnZSwgcnVuX2lkLCB3aWR4LAogICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgIF90MjUsICJ3ciIsCiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgZiJyZXRyeSB7X2F0dDI1ICsgMn0vMyDigJQge19yc24yNX0iLAogICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgICAgICBfbG9nKAogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgZiJ3b3JrZXIge3dpZHh9OiAne190MjV9JyBzZW5kIGZh"
    "aWxlZCAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBmIih7X3JzbjI1fSkg4oCUIHdhaXRp"
    "bmcgMTBzLCAiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICBmInJldHJ5IHtfYXR0MjUgKyAy"
    "fS8zIgogICAgICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICAgICAgICAgIGF3"
    "YWl0IGFzeW5jaW8uc2xlZXAoX1VQX1dBSVQyNSkKICAgICAgICAgICAgICAgIF9zZW50X2FueTI1"
    "ID0gVHJ1ZQogICAgICAgICAgICAgICAgb2suYXBwZW5kKF90MjUpCiAgICAgICAgICAgICAgICBf"
    "bWlkMjUgPSBOb25lCiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgX21p"
    "ZDI1ID0gX3Jlc3AyNVsicmVzdWx0Il1bIm1lc3NhZ2VfaWQiXQogICAgICAgICAgICAgICAgICAg"
    "IHNlbnRfaWRzLmFwcGVuZCgoY2hhdCwgX21pZDI1KSkKICAgICAgICAgICAgICAgIGV4Y2VwdCBF"
    "eGNlcHRpb246CiAgICAgICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAgICAgICAgaWYgX21n"
    "bzI1OgogICAgICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAg"
    "ICAgICAgICAgICAgICAgICByZXBvcnRfc3RhdHVzLCBydW5faWQsIHdpZHgsIF90MjUsCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgIFRydWUsICIiLCBfbWlkMjUsCiAgICAgICAgICAgICAgICAgICAg"
    "KQogICAgICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgICAg"
    "ICAgICAgICAgICAgICByZXBvcnRfbWV0YSwgcnVuX2lkLCB3aWR4LAogICAgICAgICAgICAgICAg"
    "ICAgICAgICBsZW4ob2spLCBsZW4odGl0bGVzKSwgInNlbmQiLAogICAgICAgICAgICAgICAgICAg"
    "ICkKICAgICAgICAgICAgICAgIF9sb2coCiAgICAgICAgICAgICAgICAgICAgZiJ3b3JrZXIge3dp"
    "ZHh9OiAne190MjV9JyBTRU5UICIKICAgICAgICAgICAgICAgICAgICBmIih7bGVuKG9rKX0gZG9u"
    "ZSkiCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToK"
    "ICAgICAgICAgICAgICAgIF9yc24yNSA9IGYic2VuZDoge3N0cihlKVs6MTAwXX0iCiAgICAgICAg"
    "ICAgICAgICBmYWlsLmFwcGVuZCgoX3QyNSwgX3JzbjI1KSkKICAgICAgICAgICAgICAgIGlmIF9t"
    "Z28yNToKICAgICAgICAgICAgICAgICAgICBhd2FpdCBhc3luY2lvLnRvX3RocmVhZCgKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgcmVwb3J0X3N0YXR1cywgcnVuX2lkLCB3aWR4LCBfdDI1LAogICAg"
    "ICAgICAgICAgICAgICAgICAgICBGYWxzZSwgX3JzbjI1LAogICAgICAgICAgICAgICAgICAgICkK"
    "ICAgICAgICAgICAgICAgIF9sb2coCiAgICAgICAgICAgICAgICAgICAgZiJ3b3JrZXIge3dpZHh9"
    "OiAne190MjV9JyBTRU5EIEZBSUxFRCDigJQge2V9IgogICAgICAgICAgICAgICAgKQogICAgICAg"
    "ICAgICBmaW5hbGx5OgogICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgICAgIG9z"
    "LnJlbW92ZShfcDI1YykKICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "ICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgICAgICAgICBvcy5yZW1vdmUoX3AyNSkK"
    "ICAgICAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAg"
    "ICAgICBwYXNzCiAgICAgICAgICAgICAgICBfZmJfbGluZTI1KCkKCiAgICAjIHIyNWMxNjogZG93"
    "bmxvYWRzIG9mIGFsbCB3b3JrZXJzIHN0YXJ0IHRvZ2V0aGVyIGltbWVkaWF0ZWx5OwogICAgIyBl"
    "YWNoIHdvcmtlciB1cGxvYWRzIG9ubHkgYWZ0ZXIgQUxMIGl0cyBkb3dubG9hZHMgZmluaXNoZWQK"
    "ICAgICMgKHN0YWdnZXIgYmV0d2VlbiB3b3JrZXJzIGtlcHQgb25seSBmb3IgdGhlIHVwbG9hZCBw"
    "aGFzZSBzbwogICAgIyBzaW11bHRhbmVvdXMgZmluaXNoZXMgZG9uJ3QgaGl0IHRoZSBCb3QgQVBJ"
    "IGluIHRoZSBzYW1lIHNlY29uZCkKICAgIGF3YWl0IGFzeW5jaW8uZ2F0aGVyKCpbX2RsMjUodCkg"
    "Zm9yIHQgaW4gdGl0bGVzXSkKICAgIGF3YWl0IGFzeW5jaW8uc2xlZXAobWluKCh3aWR4IG9yIDAp"
    "ICogMiwgOCkpCiAgICBhd2FpdCBfdXAyNSgpCgogICAgaWYgdG9rZW4gYW5kIGNoYXQgYW5kIG5v"
    "dCBfbWdvMjU6CiAgICAgICAgX3R4dCA9ICgKICAgICAgICAgICAgZiLinIUgPGI+d29ya2VyIHt3"
    "aWR4fTwvYj4gZG9uZSDigJQge2xlbihvayl9L3tsZW4odGl0bGVzKX0gc2VudCIKICAgICAgICAp"
    "CiAgICAgICAgaWYgZmFpbDoKICAgICAgICAgICAgX3R4dCArPSAoCiAgICAgICAgICAgICAgICBm"
    "Ilxu4p2MIHVuYXZhaWxhYmxlICh7bGVuKGZhaWwpfSk6XG4iCiAgICAgICAgICAgICAgICArICJc"
    "biIuam9pbigKICAgICAgICAgICAgICAgICAgICBmIuKAoiB7dFs6NjBdfSDigJQge3N0cihyKVs6"
    "ODBdfSIgZm9yIHQsIHIgaW4gZmFpbFs6MTBdCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAg"
    "ICkKICAgICAgICBfdGcodG9rZW4sICJzZW5kTWVzc2FnZSIsIHsiY2hhdF9pZCI6IGNoYXQsICJ0"
    "ZXh0IjogX3R4dH0pCiAgICBfbG9nKAogICAgICAgIGYid29ya2VyIHt3aWR4fTogRklOSVNIRUQg"
    "4oCUIHtsZW4ob2spfSBzZW50LCAiCiAgICAgICAgZiJ7bGVuKGZhaWwpfSBmYWlsZWQiCiAgICAp"
    "CiAgICBzaHV0aWwucm10cmVlKHdvcmtkaXIsIGlnbm9yZV9lcnJvcnM9VHJ1ZSkKICAgIGlmIF9t"
    "Z28yNToKICAgICAgICAjIGZpbmFsIGNvbnNpc3RlbmN5IHBhc3MgKHRoZSBsYWRkZXIvc3dlZXAg"
    "bWF5IGhhdmUgZmxpcHBlZAogICAgICAgICMgc3RhdGVzIGFmdGVyIHRoZSByZWFsLXRpbWUgcmVw"
    "b3J0cykgKyBhIGRvbmUgaGVhcnRiZWF0CiAgICAgICAgZm9yIF90MjUgaW4gb2s6CiAgICAgICAg"
    "ICAgIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgICAgICAgICAgcmVwb3J0X3N0YXR1"
    "cywgcnVuX2lkLCB3aWR4LCBfdDI1LCBUcnVlCiAgICAgICAgICAgICkKICAgICAgICBmb3IgX3Qy"
    "NSwgX3IyNSBpbiBmYWlsOgogICAgICAgICAgICBhd2FpdCBhc3luY2lvLnRvX3RocmVhZCgKICAg"
    "ICAgICAgICAgICAgIHJlcG9ydF9zdGF0dXMsIHJ1bl9pZCwgd2lkeCwgX3QyNSwgRmFsc2UsIF9y"
    "MjUKICAgICAgICAgICAgKQogICAgICAgIGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAg"
    "ICAgICByZXBvcnRfbWV0YSwgcnVuX2lkLCB3aWR4LCBsZW4ob2spLCBsZW4odGl0bGVzKSwgImRv"
    "bmUiCiAgICAgICAgKQogICAgcmV0dXJuIHsKICAgICAgICAicmMiOiAwIGlmIG9rIGVsc2UgKDEg"
    "aWYgZmFpbCBlbHNlIDApLAogICAgICAgICJvayI6IG9rLAogICAgICAgICJmYWlsIjogZmFpbCwK"
    "ICAgICAgICAiaWRzIjogc2VudF9pZHMsCiAgICB9CgoKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQojIFJv"
    "bGUgMiDigJQgbGVhZGVyLXNpZGUgSmlvU2Fhdm4gc3dlZXAKIyAtLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQoK"
    "YXN5bmMgZGVmIHNhYXZuX2ZldGNoX2FuZF9zZW5kKAogICAgdGl0bGUsIGFydGlzdD0iIiwgY2hh"
    "dF9pZD1Ob25lLCBib3RfdG9rZW49Tm9uZSwgY29va2llX2I2ND1Ob25lCik6CiAgICAiIiJMYXN0"
    "LWNoYW5jZSBhdHRlbXB0IGZvciBvbmUgc29uZyB0aHJvdWdoIEppb1NhYXZuLCBzZW50IHRvIHRo"
    "ZQogICAgY2hhdCB2aWEgdGhlIEJvdCBBUEkuIFJldHVybnMgKG9rLCByZWFzb24pLiIiIgogICAg"
    "Y2ZnID0gX2NmZygpCiAgICB0b2tlbiA9IGJvdF90b2tlbiBvciBjZmcuZ2V0KCJCT1RfVE9LRU4i"
    "KSBvciAiIgogICAgY2hhdCA9IHN0cihjaGF0X2lkIG9yIGNmZy5nZXQoIkxFRUNIX0xPR19DSEFU"
    "Iikgb3IgIiIpCiAgICBpZiBub3QgdG9rZW4gb3Igbm90IGNoYXQ6CiAgICAgICAgcmV0dXJuIEZh"
    "bHNlLCAibm8gYm90IHRva2VuIC8gY2hhdCBjb25maWd1cmVkIgogICAgd29ya2RpciA9IHRlbXBm"
    "aWxlLm1rZHRlbXAocHJlZml4PSJ3emZpeF9zd2VlcF8iKQogICAgdHJ5OgogICAgICAgIGNvb2tp"
    "ZWZpbGUgPSBOb25lCiAgICAgICAgaWYgY29va2llX2I2NDoKICAgICAgICAgICAgY29va2llZmls"
    "ZSA9IG9zLnBhdGguam9pbih3b3JrZGlyLCAiY29va2llcy50eHQiKQogICAgICAgICAgICB3aXRo"
    "IG9wZW4oY29va2llZmlsZSwgIndiIikgYXMgZjoKICAgICAgICAgICAgICAgIGYud3JpdGUoYmFz"
    "ZTY0LmI2NGRlY29kZShjb29raWVfYjY0KSkKICAgICAgICB0cnk6CiAgICAgICAgICAgIHUgPSBh"
    "d2FpdCBhc3luY2lvLnRvX3RocmVhZChfc2Fhdm5fc2VhcmNoX3VybCwgYXJ0aXN0LCB0aXRsZSkK"
    "ICAgICAgICAgICAgaWYgbm90IHU6CiAgICAgICAgICAgICAgICByZXR1cm4gRmFsc2UsICJubyBK"
    "aW9TYWF2biBtYXRjaCIKICAgICAgICAgICAgcCwgXyA9IGF3YWl0IGFzeW5jaW8udG9fdGhyZWFk"
    "KAogICAgICAgICAgICAgICAgX3l0ZGxfZmV0Y2gsIHUsIGNvb2tpZWZpbGUsIHdvcmtkaXIKICAg"
    "ICAgICAgICAgKQogICAgICAgICAgICBhd2FpdCBhc3luY2lvLnRvX3RocmVhZCgKICAgICAgICAg"
    "ICAgICAgIF9zZW5kX2F1ZGlvLCB0b2tlbiwgY2hhdCwgcCwgZiLwn46nIHthcnRpc3R9IOKAlCB7"
    "dGl0bGV9IgogICAgICAgICAgICApCiAgICAgICAgICAgIF9sb2coZiJzd2VlcDogJ3t0aXRsZX0n"
    "IHNlbnQgdmlhIEppb1NhYXZuIikKICAgICAgICAgICAgcmV0dXJuIFRydWUsICJKaW9TYWF2biIK"
    "ICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgIHJldHVybiBGYWxzZSwg"
    "ZiJqaW9zYWF2bjoge3N0cihlKVs6MTIwXX0iCiAgICBmaW5hbGx5OgogICAgICAgIHNodXRpbC5y"
    "bXRyZWUod29ya2RpciwgaWdub3JlX2Vycm9ycz1UcnVlKQoKCmFzeW5jIGRlZiBjb2xsZWN0X2Nv"
    "b2tpZSh1c2VyX2lkPTApOgogICAgIiIiVGhlIHVzZXIncyBZb3VUdWJlIGNvb2tpZXMgYXMgYnl0"
    "ZXMgKG9yIGIiIikuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSAuLmV4dF91dGlscy5ib3RfdXRp"
    "bHMgaW1wb3J0IGdldF91c2VyX2RjdAoKICAgICAgICB1ZCA9IGF3YWl0IGdldF91c2VyX2RjdCh1"
    "c2VyX2lkKQogICAgICAgIHAgPSB1ZC5nZXQoIlVTRVJfQ09PS0lFX0ZJTEUiKSBvciAiIgogICAg"
    "ICAgIGZvciBjYW5kIGluICgKICAgICAgICAgICAgcCwKICAgICAgICAgICAgb3MucGF0aC5qb2lu"
    "KG9zLmdldGN3ZCgpLCBwKSwKICAgICAgICAgICAgb3MucGF0aC5qb2luKFdaTUxYX0RJUiwgcCks"
    "CiAgICAgICAgKToKICAgICAgICAgICAgaWYgY2FuZCBhbmQgb3MucGF0aC5pc2ZpbGUoY2FuZCk6"
    "CiAgICAgICAgICAgICAgICB3aXRoIG9wZW4oY2FuZCwgInJiIikgYXMgZjoKICAgICAgICAgICAg"
    "ICAgICAgICByZXR1cm4gZi5yZWFkKCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFz"
    "cwogICAgZm9yIGNhbmQgaW4gKAogICAgICAgIG9zLnBhdGguam9pbihXWk1MWF9ESVIsICJjb29r"
    "aWVzLnR4dCIpLAogICAgICAgICJjb29raWVzLnR4dCIsCiAgICApOgogICAgICAgIGlmIG9zLnBh"
    "dGguaXNmaWxlKGNhbmQpOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICB3aXRoIG9w"
    "ZW4oY2FuZCwgInJiIikgYXMgZjoKICAgICAgICAgICAgICAgICAgICByZXR1cm4gZi5yZWFkKCkK"
    "ICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKICAgIHJl"
    "dHVybiBiIiIKCgojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCiMgUm9sZSAzIOKAlCBLYWdnbGUgQVBJIGRp"
    "c3BhdGNoICsgc3RhdHVzCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KCmRlZiBfa2FnZ2xlX2NyZWRzKCk6"
    "CiAgICBjZmcgPSBfY2ZnKCkKICAgIHVzZXIgPSBjZmcuZ2V0KCJLQUdHTEVfVVNFUk5BTUUiKSBv"
    "ciAiIgogICAga2V5ID0gY2ZnLmdldCgiS0FHR0xFX0tFWSIpIG9yIGNmZy5nZXQoIktBR0dMRV9B"
    "UElfS0VZIikgb3IgIiIKICAgIGlmIHVzZXIgYW5kIGtleToKICAgICAgICByZXR1cm4gdXNlciwg"
    "a2V5CiAgICByZXR1cm4gTm9uZQoKCmRlZiBkaXNwYXRjaF93b3JrZXIoaW5kZXgsIGpvYik6CiAg"
    "ICAiIiJQdXNoIG9uZSB3b3JrZXIga2VybmVsIChzdGFydHMgaXRzIHNlc3Npb24pLiAob2ssIG1l"
    "c3NhZ2UpLiIiIgogICAgY3JlZHMgPSBfa2FnZ2xlX2NyZWRzKCkKICAgIGlmIG5vdCBjcmVkczoK"
    "ICAgICAgICByZXR1cm4gRmFsc2UsICJubyBLQUdHTEVfVVNFUk5BTUUvS0FHR0xFX0tFWSBpbiBj"
    "b25maWcuZW52IgogICAgdXNlciwga2V5ID0gY3JlZHMKICAgIGJhc2UgPSBvcy5lbnZpcm9uLmdl"
    "dCgiS0FHR0xFX1dPUktJTkciKSBvciAiL2thZ2dsZS93b3JraW5nIgogICAgaWYgbm90IG9zLnBh"
    "dGguaXNkaXIoYmFzZSk6CiAgICAgICAgYmFzZSA9IHRlbXBmaWxlLmdldHRlbXBkaXIoKQogICAg"
    "ZCA9IHRlbXBmaWxlLm1rZHRlbXAocHJlZml4PWYid3pmaXhfd3tpbmRleH1fIiwgZGlyPWJhc2Up"
    "CiAgICB0cnk6CiAgICAgICAgd2l0aCBvcGVuKAogICAgICAgICAgICBvcy5wYXRoLmpvaW4oZCwg"
    "ImthZ2dsZS5qc29uIiksICJ3IgogICAgICAgICkgYXMgZjoKICAgICAgICAgICAganNvbi5kdW1w"
    "KHsidXNlcm5hbWUiOiB1c2VyLCAia2V5Ijoga2V5fSwgZikKICAgICAgICB3aXRoIG9wZW4ob3Mu"
    "cGF0aC5qb2luKGQsICJrZXJuZWwtbWV0YWRhdGEuanNvbiIpLCAidyIpIGFzIGY6CiAgICAgICAg"
    "ICAgIGpzb24uZHVtcCgKICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAiaWQi"
    "OiBmInt1c2VyfS93em1sLXdvcmtlci17aW5kZXh9IiwKICAgICAgICAgICAgICAgICAgICAidGl0"
    "bGUiOiBmInd6bWwtd29ya2VyLXtpbmRleH0iLAogICAgICAgICAgICAgICAgICAgICJjb2RlX2Zp"
    "bGUiOiAid29ya2VyX2Jvb3QucHkiLAogICAgICAgICAgICAgICAgICAgICJsYW5ndWFnZSI6ICJw"
    "eXRob24iLAogICAgICAgICAgICAgICAgICAgICJrZXJuZWxfdHlwZSI6ICJzY3JpcHQiLAogICAg"
    "ICAgICAgICAgICAgICAgICJpc19wcml2YXRlIjogInRydWUiLAogICAgICAgICAgICAgICAgICAg"
    "ICJlbmFibGVfZ3B1IjogImZhbHNlIiwKICAgICAgICAgICAgICAgICAgICAiZW5hYmxlX3RwdSI6"
    "ICJmYWxzZSIsCiAgICAgICAgICAgICAgICAgICAgImVuYWJsZV9pbnRlcm5ldCI6ICJ0cnVlIiwK"
    "ICAgICAgICAgICAgICAgICAgICAiZGF0YXNldF9zb3VyY2VzIjogW0NPTkZJR19EQVRBU0VUXSwK"
    "ICAgICAgICAgICAgICAgICAgICAiY29tcGV0aXRpb25fc291cmNlcyI6IFtdLAogICAgICAgICAg"
    "ICAgICAgICAgICJrZXJuZWxfc291cmNlcyI6IFtdLAogICAgICAgICAgICAgICAgICAgICJtb2Rl"
    "bF9zb3VyY2VzIjogW10sCiAgICAgICAgICAgICAgICB9LAogICAgICAgICAgICAgICAgZiwKICAg"
    "ICAgICAgICAgICAgIGluZGVudD0yLAogICAgICAgICAgICApCiAgICAgICAgam9iX2I2NCA9IGJh"
    "c2U2NC5iNjRlbmNvZGUoCiAgICAgICAgICAgIGpzb24uZHVtcHMoam9iKS5lbmNvZGUoKQogICAg"
    "ICAgICkuZGVjb2RlKCkKICAgICAgICB3aXRoIG9wZW4ob3MucGF0aC5qb2luKGQsICJ3b3JrZXJf"
    "Ym9vdC5weSIpLCAidyIpIGFzIGY6CiAgICAgICAgICAgIGYud3JpdGUoV09SS0VSX0JPT1RfVE1Q"
    "TCAlIChqb2JfYjY0LCBOT1RFQk9PS19EUklWRV9VUkwpKQogICAgICAgIGVudiA9IGRpY3Qob3Mu"
    "ZW52aXJvbikKICAgICAgICBlbnZbIktBR0dMRV9DT05GSUdfRElSIl0gPSBkCiAgICAgICAgZW52"
    "WyJLQUdHTEVfVVNFUk5BTUUiXSA9IHVzZXIKICAgICAgICBlbnZbIktBR0dMRV9LRVkiXSA9IGtl"
    "eQogICAgICAgICMga2FnZ2xlIDEuNi4xNyBoYXMgbm8gX19tYWluX18ucHksIHNvIGBweXRob24g"
    "LW0ga2FnZ2xlYAogICAgICAgICMgY2Fubm90IHJ1biBpdCAtIGxvY2F0ZSB0aGUgY29uc29sZSBz"
    "Y3JpcHQgaW5zdGVhZC4KICAgICAgICBleGUgPSBzaHV0aWwud2hpY2goImthZ2dsZSIpCiAgICAg"
    "ICAgaWYgbm90IGV4ZToKICAgICAgICAgICAgZm9yIHBkaXIgaW4gKAogICAgICAgICAgICAgICAg"
    "b3MucGF0aC5qb2luKHN5cy5wcmVmaXgsICJiaW4iKSwKICAgICAgICAgICAgICAgIG9zLnBhdGgu"
    "ZXhwYW5kdXNlcigifi8ubG9jYWwvYmluIiksCiAgICAgICAgICAgICAgICAiL3Vzci9sb2NhbC9i"
    "aW4iLAogICAgICAgICAgICAgICAgb3MucGF0aC5qb2luKHN5cy5wcmVmaXgsICJsb2NhbCIsICJi"
    "aW4iKSwKICAgICAgICAgICAgKToKICAgICAgICAgICAgICAgIGNhbmQgPSBvcy5wYXRoLmpvaW4o"
    "cGRpciwgImthZ2dsZSIpCiAgICAgICAgICAgICAgICBpZiBvcy5wYXRoLmlzZmlsZShjYW5kKToK"
    "ICAgICAgICAgICAgICAgICAgICBleGUgPSBjYW5kCiAgICAgICAgICAgICAgICAgICAgYnJlYWsK"
    "ICAgICAgICBpZiBleGU6CiAgICAgICAgICAgIGNtZCA9IFtzeXMuZXhlY3V0YWJsZSwgZXhlLCAi"
    "a2VybmVscyIsICJwdXNoIiwgIi1wIiwgZF0KICAgICAgICBlbHNlOgogICAgICAgICAgICBjbWQg"
    "PSBbc3lzLmV4ZWN1dGFibGUsICItbSIsICJrYWdnbGUiLCAia2VybmVscyIsICJwdXNoIiwgIi1w"
    "IiwgZF0KICAgICAgICBwID0gc3VicHJvY2Vzcy5ydW4oCiAgICAgICAgICAgIGNtZCwKICAgICAg"
    "ICAgICAgY2FwdHVyZV9vdXRwdXQ9VHJ1ZSwKICAgICAgICAgICAgdGV4dD1UcnVlLAogICAgICAg"
    "ICAgICB0aW1lb3V0PTI0MCwKICAgICAgICAgICAgZW52PWVudiwKICAgICAgICApCiAgICAgICAg"
    "b3V0ID0gKHAuc3Rkb3V0IG9yICIiKSArIChwLnN0ZGVyciBvciAiIikKICAgICAgICBpZiBwLnJl"
    "dHVybmNvZGUgPT0gMDoKICAgICAgICAgICAgcmV0dXJuIFRydWUsIG91dC5zdHJpcCgpWy0yMDA6"
    "XQogICAgICAgIHJldHVybiBGYWxzZSwgb3V0LnN0cmlwKClbLTIwMDpdIG9yIGYicmM9e3AucmV0"
    "dXJuY29kZX0iCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgcmV0dXJuIEZhbHNl"
    "LCBzdHIoZSlbOjIwMF0KICAgIGZpbmFsbHk6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBzaHV0"
    "aWwucm10cmVlKGQsIGlnbm9yZV9lcnJvcnM9VHJ1ZSkKICAgICAgICBleGNlcHQgRXhjZXB0aW9u"
    "OgogICAgICAgICAgICBwYXNzCgoKX1NUQVRVU19NQVAgPSB7CiAgICAicnVubmluZyI6ICJydW4i"
    "LAogICAgInF1ZXVlZCI6ICJydW4iLAogICAgImNvbXBsZXRlIjogImRvbmUiLAogICAgImVycm9y"
    "IjogImVycm9yIiwKICAgICJjYW5jZWxhY2tub3dsZWRnZWQiOiAiZXJyb3IiLAogICAgImNhbmNl"
    "bGFja25vd2xlZGdlZHNjb3BlIjogImVycm9yIiwKfQoKCmRlZiB3b3JrZXJfc3RhdHVzKGluZGV4"
    "KToKICAgICIiIldvcmtlciBrZXJuZWwgcnVuIHN0YXR1czogcnVuIC8gZG9uZSAvIGVycm9yIC8g"
    "dW5rbm93bi4iIiIKICAgIGNyZWRzID0gX2thZ2dsZV9jcmVkcygpCiAgICBpZiBub3QgY3JlZHM6"
    "CiAgICAgICAgcmV0dXJuICJ1bmtub3duIgogICAgdXNlciwga2V5ID0gY3JlZHMKICAgIHVybCA9"
    "ICgKICAgICAgICAiaHR0cHM6Ly93d3cua2FnZ2xlLmNvbS9hcGkvdjEva2VybmVscy9zdGF0dXMi"
    "CiAgICAgICAgZiI/dXNlck5hbWU9e3VzZXJ9Jmtlcm5lbFNsdWc9d3ptbC13b3JrZXIte2luZGV4"
    "fSIKICAgICkKICAgIHJlcSA9IHVybGxpYi5yZXF1ZXN0LlJlcXVlc3QodXJsKQogICAgaW1wb3J0"
    "IGJhc2U2NCBhcyBfYjY0CgogICAgdG9rID0gX2I2NC5iNjRlbmNvZGUoZiJ7dXNlcn06e2tleX0i"
    "LmVuY29kZSgpKS5kZWNvZGUoKQogICAgcmVxLmFkZF9oZWFkZXIoIkF1dGhvcml6YXRpb24iLCBm"
    "IkJhc2ljIHt0b2t9IikKICAgIHRyeToKICAgICAgICB3aXRoIHVybGxpYi5yZXF1ZXN0LnVybG9w"
    "ZW4ocmVxLCB0aW1lb3V0PTIwKSBhcyByOgogICAgICAgICAgICBkYXRhID0ganNvbi5sb2Fkcyhy"
    "LnJlYWQoKS5kZWNvZGUoKSkKICAgICAgICBzdCA9IChkYXRhLmdldCgic3RhdHVzIikgb3IgIiIp"
    "Lmxvd2VyKCkKICAgICAgICByZXR1cm4gX1NUQVRVU19NQVAuZ2V0KHN0LCAiZG9uZSIgaWYgc3Qg"
    "ZWxzZSAidW5rbm93biIpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAidW5r"
    "bm93biIKCgpkZWYgcGlja19pZGxlX3dvcmtlcihtYXhfd29ya2Vycz00KToKICAgICIiIkZpcnN0"
    "IGlkbGUgd29ya2VyIGluZGV4IChvciBOb25lIGlmIGFsbCBidXN5IC8gbm8gY3JlZHMpLiIiIgog"
    "ICAgaWYgbm90IF9rYWdnbGVfY3JlZHMoKToKICAgICAgICByZXR1cm4gTm9uZQogICAgZm9yIGkg"
    "aW4gcmFuZ2UoMSwgbWF4X3dvcmtlcnMgKyAxKToKICAgICAgICBpZiB3b3JrZXJfc3RhdHVzKGkp"
    "ICE9ICJydW4iOgogICAgICAgICAgICByZXR1cm4gaQogICAgcmV0dXJuIE5vbmUKCgppZiBfX25h"
    "bWVfXyA9PSAiX19tYWluX18iOgogICAgX3IyNW0gPSBhc3luY2lvLnJ1bihydW5fd29ya2VyX2pv"
    "YigpKQogICAgc3lzLmV4aXQoX3IyNW1bInJjIl0gaWYgaXNpbnN0YW5jZShfcjI1bSwgZGljdCkg"
    "ZWxzZSAwKQo="
)
WZFIX_R4_CMDS_B64 = (
    "IiIiV1pGSVggUm91bmQgNCAodjE1LjQ2KSDigJQgc2hvcnQgbXVzaWMgY29tbWFuZHMgKyBseXJpY3Mgc2VhcmNoLgoKL2RsIDxs"
    "aW5rIG9yIHJlcGx5PiAgICBsZWVjaCB0aGUgc29uZyBhcyBhbiBtcDMgKHl0LWRscCBlbmdpbmUpCi9kICA8bGluayBvciByZXBs"
    "eT4gICAgdXBsb2FkIHRoZSBzb25nIHRvIHRoZSBkcml2ZSAoeXQtZGxwIGVuZ2luZSkKL3kgIDxsaW5rIG9yIHJlcGx5PiAgICBz"
    "YW1lIGFzIC9kIOKAlCBtdXNpYyBsaW5rIC0+IHVwbG9hZGVkCi9sZCA8bHlyaWNzL2tleXdvcmRzPiAgZmluZCB0aGUgc29uZyBi"
    "eSBpdHMgbHlyaWNzLCBjb25maXJtLCB0aGVuIGxlZWNoCgpMaW5rcyBhcmUgbmV2ZXIgYWxsb3dlZCBpbiAvbGQg4oCUIGx5cmlj"
    "cyBvciBrZXl3b3JkcyBvbmx5LiBUaGUgc2VhcmNoCnRyaWVzIExSQ0xJQiBmaXJzdCAoY2xlYW4gYXJ0aXN0ICsgdGl0bGUgZnJv"
    "bSBseXJpY3MsIG5vIEFQSSBrZXkpLApmYWxscyBiYWNrIHRvIGEgcGxhaW4gWW91VHViZSBzZWFyY2guIFRvZ2dsZXMgbGl2ZSBv"
    "biB0aGUgd2ViIGRhc2hib2FyZC4KIiIiCgppbXBvcnQgYXN5bmNpbwoKZnJvbSBib3QgaW1wb3J0IExPR0dFUgoKX2xvZyA9IExP"
    "R0dFUi5pbmZvIGlmIExPR0dFUiBlbHNlIHByaW50CgpfTERfQ0FDSEUgPSB7fQpfTERfRE9ORSA9IHNldCgpCl9MRF9TVEFURSA9"
    "IHt9Cl9TRVRfQ0FDSEUgPSB7InQiOiAwLjAsICJ2Ijoge319CgoKYXN5bmMgZGVmIGdldF9zZXR0aW5ncyhmb3JjZT1GYWxzZSk6"
    "CiAgICAiIiJUaGUgdGhyZWUgb3duZXIgdG9nZ2xlcyAoY2FjaGVkIDYwIHMpOiBtdXNpYyBsaW5rcywgL2xkLCBhbGlhc2VzLiIi"
    "IgogICAgaW1wb3J0IHRpbWUgYXMgX3RpbWUKCiAgICBub3cgPSBfdGltZS50aW1lKCkKICAgIGlmIG5vdCBmb3JjZSBhbmQgbm93"
    "IC0gX1NFVF9DQUNIRVsidCJdIDwgNjAgYW5kIF9TRVRfQ0FDSEVbInYiXToKICAgICAgICByZXR1cm4gX1NFVF9DQUNIRVsidiJd"
    "CiAgICB2YWxzID0geyJtdXNpY19vbiI6IFRydWUsICJsZF9vbiI6IFRydWUsICJhbGlhc2VzX29uIjogVHJ1ZX0KICAgIHRyeToK"
    "ICAgICAgICBmcm9tIC5yMV9jb3JlIGltcG9ydCBfZGIsIF9wYXJ0CgogICAgICAgIGRvYyA9IGF3YWl0IF9kYigpLnd6Zml4X2Nv"
    "bmZpZ1tfcGFydCgpXS5maW5kX29uZSh7Il9pZCI6ICJnbG9iYWwifSkKICAgICAgICBpZiBkb2M6CiAgICAgICAgICAgIGZvciBr"
    "IGluIHZhbHM6CiAgICAgICAgICAgICAgICBpZiBrIGluIGRvYzoKICAgICAgICAgICAgICAgICAgICB2YWxzW2tdID0gYm9vbChk"
    "b2Nba10pCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIF9TRVRfQ0FDSEVbInQiXSA9IG5vdwogICAgX1NF"
    "VF9DQUNIRVsidiJdID0gdmFscwogICAgcmV0dXJuIHZhbHMKCgphc3luYyBkZWYgX3NldHRpbmcobmFtZSk6CiAgICByZXR1cm4g"
    "KGF3YWl0IGdldF9zZXR0aW5ncygpKS5nZXQobmFtZSwgVHJ1ZSkKCgphc3luYyBkZWYgc2V0X3NldHRpbmcobmFtZSwgdmFsdWUp"
    "OgogICAgZnJvbSAucjFfY29yZSBpbXBvcnQgX2RiLCBfcGFydAoKICAgIGF3YWl0IF9kYigpLnd6Zml4X2NvbmZpZ1tfcGFydCgp"
    "XS51cGRhdGVfb25lKAogICAgICAgIHsiX2lkIjogImdsb2JhbCJ9LCB7IiRzZXQiOiB7bmFtZTogYm9vbCh2YWx1ZSl9fSwgdXBz"
    "ZXJ0PVRydWUKICAgICkKICAgIF9TRVRfQ0FDSEVbInQiXSA9IDAuMAoKCmFzeW5jIGRlZiBfcnVuX2FsaWFzKGNsaWVudCwgbWVz"
    "c2FnZSwgY21kLCBsZWVjaCk6CiAgICB0ZXh0ID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3RyaXAoKQogICAgcGFydHMgPSB0ZXh0"
    "LnNwbGl0KE5vbmUsIDEpCiAgICBhcmcgPSBwYXJ0c1sxXS5zdHJpcCgpIGlmIGxlbihwYXJ0cykgPiAxIGVsc2UgIiIKICAgIGlm"
    "IG5vdCBhcmcgYW5kIG5vdCBnZXRhdHRyKG1lc3NhZ2UsICJyZXBseV90b19tZXNzYWdlIiwgTm9uZSk6CiAgICAgICAgYXdhaXQg"
    "bWVzc2FnZS5yZXBseV90ZXh0KAogICAgICAgICAgICAiU2VuZCBhIG11c2ljIGxpbmsgd2l0aCB0aGUgY29tbWFuZCDigJQgZS5n"
    "LiAiCiAgICAgICAgICAgIGYie3BhcnRzWzBdfSA8c3BvdGlmeSAvIGppb3NhYXZuIC8gYXBwbGUgbGluaz4sIG9yIHJlcGx5IHRv"
    "IG9uZSIKICAgICAgICApCiAgICAgICAgcmV0dXJuCiAgICBfbG9nKGYiV1pGSVggbXVzaWM6IHtwYXJ0c1swXX0g4oaSIHtjbWR9"
    "IHthcmdbOjgwXX0iKQogICAgbWVzc2FnZS50ZXh0ID0gZiJ7Y21kfSB7YXJnfSIuc3RyaXAoKQogICAgdHJ5OgogICAgICAgIGZy"
    "b20gYm90Lm1vZHVsZXMueXRkbHAgaW1wb3J0IHl0ZGxfbGVlY2gsIHl0ZGwKCiAgICAgICAgaWYgbGVlY2g6CiAgICAgICAgICAg"
    "IGF3YWl0IHl0ZGxfbGVlY2goY2xpZW50LCBtZXNzYWdlKQogICAgICAgIGVsc2U6CiAgICAgICAgICAgIGF3YWl0IHl0ZGwoY2xp"
    "ZW50LCBtZXNzYWdlKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIF9sb2coZiJXWkZJWCBtdXNpYzogYWxpYXMg"
    "ZGlzcGF0Y2ggZmFpbGVkOiB7ZX0iKQogICAgICAgIHRyeToKICAgICAgICAgICAgYXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KAog"
    "ICAgICAgICAgICAgICAgIuKaoO+4jyBDb3VsZG4ndCBzdGFydCB0aGUgdGFzayDigJQgdHJ5IGFnYWluIGluIGEgbWludXRlIgog"
    "ICAgICAgICAgICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwoKCmFzeW5jIGRlZiBtdXNpY19k"
    "bChjbGllbnQsIG1lc3NhZ2UpOgogICAgIiIiL2RsIDxsaW5rPiDigJQgbGVlY2ggdGhlIHNvbmcuIiIiCiAgICBpZiBub3QgYXdh"
    "aXQgX3NldHRpbmcoImFsaWFzZXNfb24iKToKICAgICAgICBhd2FpdCBtZXNzYWdlLnJlcGx5X3RleHQoIlNob3J0IGNvbW1hbmRz"
    "IGFyZSBkaXNhYmxlZCBieSB0aGUgb3duZXIuIikKICAgICAgICByZXR1cm4KICAgIGF3YWl0IF9ydW5fYWxpYXMoY2xpZW50LCBt"
    "ZXNzYWdlLCAiL3lsIiwgVHJ1ZSkKCgphc3luYyBkZWYgbXVzaWNfZChjbGllbnQsIG1lc3NhZ2UpOgogICAgIiIiL2QgPGxpbms+"
    "IOKAlCB1cGxvYWQgdGhlIHNvbmcgdG8gdGhlIGRyaXZlLiIiIgogICAgaWYgbm90IGF3YWl0IF9zZXR0aW5nKCJhbGlhc2VzX29u"
    "Iik6CiAgICAgICAgYXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KCJTaG9ydCBjb21tYW5kcyBhcmUgZGlzYWJsZWQgYnkgdGhlIG93"
    "bmVyLiIpCiAgICAgICAgcmV0dXJuCiAgICBhd2FpdCBfcnVuX2FsaWFzKGNsaWVudCwgbWVzc2FnZSwgIi95dGRsIiwgRmFsc2Up"
    "CgoKYXN5bmMgZGVmIG11c2ljX3koY2xpZW50LCBtZXNzYWdlKToKICAgICIiIi95IDxtdXNpYyBsaW5rPiDigJQgc2FtZSBhcyAv"
    "ZCwgdXBsb2FkZWQgdG8gdGhlIGRyaXZlLiIiIgogICAgaWYgbm90IGF3YWl0IF9zZXR0aW5nKCJhbGlhc2VzX29uIik6CiAgICAg"
    "ICAgYXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KCJTaG9ydCBjb21tYW5kcyBhcmUgZGlzYWJsZWQgYnkgdGhlIG93bmVyLiIpCiAg"
    "ICAgICAgcmV0dXJuCiAgICBhd2FpdCBfcnVuX2FsaWFzKGNsaWVudCwgbWVzc2FnZSwgIi95dGRsIiwgRmFsc2UpCgphc3luYyBk"
    "ZWYgbXVzaWNfbWwoY2xpZW50LCBtZXNzYWdlKToKICAgICIiIi9tbCA8bXVzaWMgbGluaz4g4oCUIG1pcnJvci1sZWVjaCBtdXNp"
    "Yzogc29uZyhzKSArIHppcCBzZW50CiAgICBzdHJhaWdodCB0byB0aGUgZHJpdmUgKHNhbWUgZW5naW5lIGFzIC95LCAteiBmcmll"
    "bmRseSkuIiIiCiAgICBpZiBub3QgYXdhaXQgX3NldHRpbmcoImFsaWFzZXNfb24iKToKICAgICAgICBhd2FpdCBtZXNzYWdlLnJl"
    "cGx5X3RleHQoIlNob3J0IGNvbW1hbmRzIGFyZSBkaXNhYmxlZCBieSB0aGUgb3duZXIuIikKICAgICAgICByZXR1cm4KICAgIGF3"
    "YWl0IF9ydW5fYWxpYXMoY2xpZW50LCBtZXNzYWdlLCAiL3l0ZGwiLCBGYWxzZSkKCgpkZWYgX2Nvb2tpZV9maWxlKCk6CiAgICAi"
    "IiJUaGUgc2FtZSBjb29raWUgdGhlIHl0LWRscCBlbmdpbmUgdXNlcyDigJQgZGF0YWNlbnRlciBJUHMgZ2V0CiAgICBib3QtY2hl"
    "Y2tlZCBieSBZb3VUdWJlIHdpdGhvdXQgaXQuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSBvcyBpbXBvcnQgcGF0aCBhcyBfcAoK"
    "ICAgICAgICBmcm9tIGJvdC5oZWxwZXIubWlycm9yX2xlZWNoX3V0aWxzLmRvd25sb2FkX3V0aWxzLnl0X2RscF9kb3dubG9hZCBp"
    "bXBvcnQgKAogICAgICAgICAgICBnZXRfY29va2llX2ZpbGUsCiAgICAgICAgKQoKICAgICAgICBmID0gZ2V0X2Nvb2tpZV9maWxl"
    "KHt9KQogICAgICAgIHJldHVybiBmIGlmIF9wLmV4aXN0cyhmKSBlbHNlIE5vbmUKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAg"
    "ICAgcmV0dXJuIE5vbmUKCgpkZWYgX3JhbmtfZW50cyhlbnRzKToKICAgIGdvb2QsIHJlc3QgPSBbXSwgW10KICAgIGZvciBlIGlu"
    "IGVudHM6CiAgICAgICAgZCA9IGUuZ2V0KCJkdXJhdGlvbiIpIG9yIDAKICAgICAgICB0ID0gc3RyKGUuZ2V0KCJ0aXRsZSIpIG9y"
    "ICIiKS5sb3dlcigpCiAgICAgICAgaWYgKAogICAgICAgICAgICA3NSA8PSBkIDw9IDkwMAogICAgICAgICAgICBhbmQgbm90IGFu"
    "eSgKICAgICAgICAgICAgICAgIHcgaW4gdCBmb3IgdyBpbiAoInRlYXNlciIsICJ0cmFpbGVyIiwgInByb21vIiwgInNuaXBwZXQi"
    "KQogICAgICAgICAgICApCiAgICAgICAgKToKICAgICAgICAgICAgZ29vZC5hcHBlbmQoZSkKICAgICAgICBlbHNlOgogICAgICAg"
    "ICAgICByZXN0LmFwcGVuZChlKQogICAgcmV0dXJuIFsKICAgICAgICAoCiAgICAgICAgICAgIHN0cihlLmdldCgidGl0bGUiKSBv"
    "ciAiVW5rbm93biIpLAogICAgICAgICAgICBzdHIoZS5nZXQoImlkIikpLAogICAgICAgICAgICBlLmdldCgiZHVyYXRpb24iKSBv"
    "ciAwLAogICAgICAgICkKICAgICAgICBmb3IgZSBpbiAoZ29vZCArIHJlc3QpCiAgICBdCgoKZGVmIF95dF9zZWFyY2gocSwgbj0x"
    "MCk6CiAgICBmcm9tIHl0X2RscCBpbXBvcnQgWW91dHViZURMCgogICAgY2YgPSBfY29va2llX2ZpbGUoKQogICAgYmFzZSA9IHsK"
    "ICAgICAgICAicXVpZXQiOiBUcnVlLAogICAgICAgICJza2lwX2Rvd25sb2FkIjogVHJ1ZSwKICAgICAgICAibm9jaGVja2NlcnRp"
    "ZmljYXRlIjogVHJ1ZSwKICAgICAgICAiY29va2llZmlsZSI6IGNmLAogICAgICAgICJyZXRyaWVzIjogMywKICAgICAgICAicmV0"
    "cnlfc2xlZXBfZnVuY3Rpb25zIjogewogICAgICAgICAgICAiaHR0cCI6IGxhbWJkYSBuOiAyLAogICAgICAgICAgICAiZXh0cmFj"
    "dG9yIjogbGFtYmRhIG46IDIsCiAgICAgICAgfSwKICAgIH0KICAgICMgYXR0ZW1wdCAxICJmbGF0IjogZmFzdCBIVE1MIHNjcmFw"
    "ZSAoYm90LWNoZWNrZWQgb24gc29tZSBJUHMsCiAgICAjIHJldHVybnMgMCBlbnRyaWVzIHNpbGVudGx5KS4gYXR0ZW1wdCAyICJm"
    "dWxsIjogdGhlIHNhbWUKICAgICMgZXh0cmFjdGlvbiBwYXRoIGFzIHRoZSBtYWluIGRvd25sb2FkIGVuZ2luZSwgd2hpY2ggd29y"
    "a3Mgb24KICAgICMgZGF0YWNlbnRlciBJUHMgKHNsb3dlciwgc28gY2FwcGVkIGF0IDUgcmVzdWx0cykuCiAgICBmb3IgdGFnLCBl"
    "eHRyYSwgY291bnQgaW4gKAogICAgICAgICgiZmxhdCIsIHsiZXh0cmFjdF9mbGF0IjogImluX3BsYXlsaXN0In0sIG4pLAogICAg"
    "ICAgICgiZnVsbCIsIHt9LCBtaW4obiwgNSkpLAogICAgKToKICAgICAgICBvcHRzID0gZGljdChiYXNlKQogICAgICAgIG9wdHMu"
    "dXBkYXRlKGV4dHJhKQogICAgICAgIG9wdHNbInBsYXlsaXN0X2l0ZW1zIl0gPSBmIjE6e2NvdW50fSIKICAgICAgICB0cnk6CiAg"
    "ICAgICAgICAgIHdpdGggWW91dHViZURMKG9wdHMpIGFzIHlkbDoKICAgICAgICAgICAgICAgIHIgPSB5ZGwuZXh0cmFjdF9pbmZv"
    "KAogICAgICAgICAgICAgICAgICAgIGYieXRzZWFyY2h7Y291bnR9OntxfSIsIGRvd25sb2FkPUZhbHNlCiAgICAgICAgICAgICAg"
    "ICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICBfbG9nKGYiV1pGSVggL2xkOiB5dHNlYXJjaFt7"
    "dGFnfV0gZmFpbGVkOiB7ZX0iKQogICAgICAgICAgICBjb250aW51ZQogICAgICAgIGVudHMgPSBbZSBmb3IgZSBpbiAoci5nZXQo"
    "ImVudHJpZXMiKSBvciBbXSkgaWYgZV0KICAgICAgICBfbG9nKAogICAgICAgICAgICBmIldaRklYIC9sZDogeXRzZWFyY2hbe3Rh"
    "Z31dIHJldHVybmVkIHtsZW4oZW50cyl9IGVudHJpZXMiCiAgICAgICAgICAgIGYiIChjb29raWVzPXsneWVzJyBpZiBjZiBlbHNl"
    "ICdubyd9KSIKICAgICAgICApCiAgICAgICAgaWYgZW50czoKICAgICAgICAgICAgcmV0dXJuIF9yYW5rX2VudHMoZW50cykKICAg"
    "IHJldHVybiBbXQoKCmFzeW5jIGRlZiBfZmluZF9zb25ncyhxdWVyeSk6CiAgICAiIiJMUkNMSUIgZmlyc3QgKGNsZWFuIGFydGlz"
    "dCArIHRpdGxlIGZyb20gbHlyaWNzKSwgdGhlbiBZb3VUdWJlLgoKICAgIFJldHVybnMgKGhpdHMsIGNsZWFuLCBscmMpOiB1cCB0"
    "byAxMCAodGl0bGUsIHZpZCwgZHVyKSB0dXBsZXMsIHRoZQogICAgY2xlYW4gTFJDTElCIG5hbWUgZm9yIHRoZSB0b3AgaGl0LCBh"
    "bmQgdGhlIHJlbWFpbmluZyBMUkNMSUIKICAgIGFsdGVybmF0ZXMgZm9yIGxhdGVyIHJvdW5kcy4KICAgICIiIgogICAgcSA9IHF1"
    "ZXJ5CiAgICBjbGVhbiA9IE5vbmUKICAgIGxyYyA9IFtdCiAgICB0cnk6CiAgICAgICAgaW1wb3J0IGFpb2h0dHAKCiAgICAgICAg"
    "YXN5bmMgd2l0aCBhaW9odHRwLkNsaWVudFNlc3Npb24oCiAgICAgICAgICAgIHRpbWVvdXQ9YWlvaHR0cC5DbGllbnRUaW1lb3V0"
    "KHRvdGFsPTgpLCB0cnVzdF9lbnY9VHJ1ZQogICAgICAgICkgYXMgX3M6CiAgICAgICAgICAgIGFzeW5jIHdpdGggX3MuZ2V0KAog"
    "ICAgICAgICAgICAgICAgImh0dHBzOi8vbHJjbGliLm5ldC9hcGkvc2VhcmNoIiwgcGFyYW1zPXsicSI6IHF1ZXJ5WzozMDBdfQog"
    "ICAgICAgICAgICApIGFzIF9yOgogICAgICAgICAgICAgICAgaWYgX3Iuc3RhdHVzID09IDIwMDoKICAgICAgICAgICAgICAgICAg"
    "ICBoaXRzID0gYXdhaXQgX3IuanNvbigpCiAgICAgICAgICAgICAgICAgICAgZm9yIGggaW4gaGl0c1s6OF06CiAgICAgICAgICAg"
    "ICAgICAgICAgICAgIF9hID0gc3RyKGguZ2V0KCJhcnRpc3ROYW1lIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICAgICAgICAgICAg"
    "ICAgICAgX3QgPSBzdHIoaC5nZXQoInRyYWNrTmFtZSIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAgICAgICAgICAgICAgICAgIGlm"
    "IF9hIG9yIF90OgogICAgICAgICAgICAgICAgICAgICAgICAgICAgbHJjLmFwcGVuZChmIntfYX0gLSB7X3R9Ii5zdHJpcCgiIC0i"
    "KSkKICAgICAgICAgICAgICAgICAgICBpZiBscmM6CiAgICAgICAgICAgICAgICAgICAgICAgIGNsZWFuID0gbHJjWzBdCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgIHEgPSBmIntjbGVhbn0gYXVkaW8iCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MK"
    "ICAgIHRyeToKICAgICAgICBoaXRzID0gYXdhaXQgYXN5bmNpby50b190aHJlYWQoX3l0X3NlYXJjaCwgcSwgMTApCiAgICBleGNl"
    "cHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgX2xvZyhmIldaRklYIC9sZDogeW91dHViZSBzZWFyY2ggZmFpbGVkOiB7ZX0iKQog"
    "ICAgICAgIGhpdHMgPSBbXQogICAgICAgIGlmIGxlbihxLnNwbGl0KCkpID4gODoKICAgICAgICAgICAgdHJ5OgogICAgICAgICAg"
    "ICAgICAgX2xvZygiV1pGSVggL2xkOiByZXRyeWluZyB3aXRoIHNob3J0ZXIgcXVlcnkiKQogICAgICAgICAgICAgICAgaGl0cyA9"
    "IGF3YWl0IGFzeW5jaW8udG9fdGhyZWFkKAogICAgICAgICAgICAgICAgICAgIF95dF9zZWFyY2gsICIgIi5qb2luKHEuc3BsaXQo"
    "KVs6OF0pLCAxMAogICAgICAgICAgICAgICAgKQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGUyOgogICAgICAgICAg"
    "ICAgICAgX2xvZyhmIldaRklYIC9sZDogcmV0cnkgZmFpbGVkIHRvbzoge2UyfSIpCiAgICByZXR1cm4gKGhpdHMsIGNsZWFuLCBs"
    "cmNbMTpdIGlmIGNsZWFuIGVsc2UgbHJjKQoKCmFzeW5jIGRlZiBfc2VhcmNoX21vcmUoc3QpOgogICAgIiIiUm91bmQgMis6IExS"
    "Q0xJQiBhbHRlcm5hdGVzIGZpcnN0LCB0aGVuIHF1ZXJ5IHZhcmlhbnRzLiIiIgogICAgbmV3ID0gW10KICAgIHNlZW4gPSB7aFsx"
    "XSBmb3IgaCBpbiBzdFsiaGl0cyJdfQogICAgbHJjID0gc3QuZ2V0KCJscmMiKSBvciBbXQogICAgd2hpbGUgbHJjIGFuZCBsZW4o"
    "bmV3KSA8IDU6CiAgICAgICAgbmFtZSA9IGxyYy5wb3AoMCkKICAgICAgICB0cnk6CiAgICAgICAgICAgIGZvdW5kID0gYXdhaXQg"
    "YXN5bmNpby50b190aHJlYWQoX3l0X3NlYXJjaCwgZiJ7bmFtZX0gYXVkaW8iLCAxKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246"
    "CiAgICAgICAgICAgIGZvdW5kID0gW10KICAgICAgICBmb3IgaCBpbiBmb3VuZDoKICAgICAgICAgICAgaWYgaFsxXSBub3QgaW4g"
    "c2VlbjoKICAgICAgICAgICAgICAgIHNlZW4uYWRkKGhbMV0pCiAgICAgICAgICAgICAgICBuZXcuYXBwZW5kKGgpCiAgICBpZiBu"
    "b3QgbmV3OgogICAgICAgIHdvcmRzID0gc3RbInEiXS5zcGxpdCgpCiAgICAgICAgdmFyaWFudHMgPSBbCiAgICAgICAgICAgICIg"
    "Ii5qb2luKHdvcmRzWzo4XSksCiAgICAgICAgICAgICIgIi5qb2luKHdvcmRzWy04Ol0pLAogICAgICAgICAgICAiICIuam9pbih3"
    "b3Jkc1syOjEwXSksCiAgICAgICAgXQogICAgICAgIHYgPSB2YXJpYW50c1tzdFsicm91bmRzIl0gJSBsZW4odmFyaWFudHMpXQog"
    "ICAgICAgIHN0WyJyb3VuZHMiXSArPSAxCiAgICAgICAgdHJ5OgogICAgICAgICAgICBfbG9nKGYiV1pGSVggL2xkOiBzZWFyY2hp"
    "bmcgbW9yZSAoe3ZbOjUwXX0pIikKICAgICAgICAgICAgZm9yIGggaW4gYXdhaXQgYXN5bmNpby50b190aHJlYWQoX3l0X3NlYXJj"
    "aCwgdiwgMTApOgogICAgICAgICAgICAgICAgaWYgaFsxXSBub3QgaW4gc2VlbjoKICAgICAgICAgICAgICAgICAgICBzZWVuLmFk"
    "ZChoWzFdKQogICAgICAgICAgICAgICAgICAgIG5ldy5hcHBlbmQoaCkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAg"
    "ICAgICAgICAgIF9sb2coZiJXWkZJWCAvbGQ6IG1vcmUgc2VhcmNoIGZhaWxlZDoge2V9IikKICAgIHJldHVybiBuZXcKCgpkZWYg"
    "X2xkX3BhZ2Vfa2Ioc3QpOgogICAgZnJvbSBweXJvZ3JhbS50eXBlcyBpbXBvcnQgSW5saW5lS2V5Ym9hcmRNYXJrdXAsIElubGlu"
    "ZUtleWJvYXJkQnV0dG9uCgogICAgY3VyID0gc3RbImN1cnNvciJdCiAgICBwYWdlID0gc3RbImhpdHMiXVtjdXIgOiBjdXIgKyA1"
    "XQogICAgcm93cyA9IFtdCiAgICBmb3IgaSwgKHRpdGxlLCB2aWQsIGR1cikgaW4gZW51bWVyYXRlKHBhZ2UpOgogICAgICAgIG0s"
    "IHMgPSBkaXZtb2QoaW50KGR1ciBvciAwKSwgNjApCiAgICAgICAgbGFiZWwgPSBmIuKWtiB7Y3VyICsgaSArIDF9LiB7dGl0bGVb"
    "OjM4XX0gwrcge219OntzOjAyZH0iCiAgICAgICAgcm93cy5hcHBlbmQoCiAgICAgICAgICAgIFsKICAgICAgICAgICAgICAgIElu"
    "bGluZUtleWJvYXJkQnV0dG9uKAogICAgICAgICAgICAgICAgICAgIGxhYmVsLCBjYWxsYmFja19kYXRhPWYid3pmaXhsZHA6e2N1"
    "ciArIGl9IgogICAgICAgICAgICAgICAgKQogICAgICAgICAgICBdCiAgICAgICAgKQogICAgcm93cy5hcHBlbmQoCiAgICAgICAg"
    "WwogICAgICAgICAgICBJbmxpbmVLZXlib2FyZEJ1dHRvbigi8J+UjSBNb3JlIiwgY2FsbGJhY2tfZGF0YT0id3pmaXhsZG0iKSwK"
    "ICAgICAgICAgICAgSW5saW5lS2V5Ym9hcmRCdXR0b24oIuKdjCBDYW5jZWwiLCBjYWxsYmFja19kYXRhPSJ3emZpeGxkbiIpLAog"
    "ICAgICAgIF0KICAgICkKICAgIHJldHVybiBJbmxpbmVLZXlib2FyZE1hcmt1cChyb3dzKQoKCmFzeW5jIGRlZiBfbGRfZWRpdCht"
    "c2csIHRleHQsIG1hcmt1cD1Ob25lKToKICAgIHRyeToKICAgICAgICBhd2FpdCBtc2cuZWRpdF90ZXh0KHRleHQsIHJlcGx5X21h"
    "cmt1cD1tYXJrdXApCiAgICAgICAgcmV0dXJuIFRydWUKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIEZhbHNl"
    "CgoKYXN5bmMgZGVmIG11c2ljX2xkKGNsaWVudCwgbWVzc2FnZSk6CiAgICAiIiIvbGQgPGx5cmljcyBvciBrZXl3b3Jkcz4g4oCU"
    "IGZpbmQgdGhlIHNvbmcsIHBpY2ssIHRoZW4gbGVlY2guIiIiCiAgICBpZiBub3QgYXdhaXQgX3NldHRpbmcoImxkX29uIik6CiAg"
    "ICAgICAgYXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KCJMeXJpY3Mgc2VhcmNoIGlzIGRpc2FibGVkIGJ5IHRoZSBvd25lci4iKQog"
    "ICAgICAgIHJldHVybgogICAgdGV4dCA9IChtZXNzYWdlLnRleHQgb3IgIiIpLnN0cmlwKCkKICAgIHBhcnRzID0gdGV4dC5zcGxp"
    "dChOb25lLCAxKQogICAgcXVlcnkgPSBwYXJ0c1sxXS5zdHJpcCgpIGlmIGxlbihwYXJ0cykgPiAxIGVsc2UgIiIKICAgIGlmIG5v"
    "dCBxdWVyeToKICAgICAgICBhd2FpdCBtZXNzYWdlLnJlcGx5X3RleHQoCiAgICAgICAgICAgICJTZW5kIHNvbWUgbHlyaWNzIG9y"
    "IGtleXdvcmRzIOKAlCBlLmcuIC9sZCB0ZXJpIG1pdHRpIG1laW4iCiAgICAgICAgKQogICAgICAgIHJldHVybgogICAgaWYgImh0"
    "dHAiIGluIHF1ZXJ5Lmxvd2VyKCkgb3IgIjovLyIgaW4gcXVlcnkgb3IgInd3dy4iIGluIHF1ZXJ5Lmxvd2VyKCk6CiAgICAgICAg"
    "YXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KAogICAgICAgICAgICAi4pqg77iPIC9sZCBvbmx5IHdvcmtzIHdpdGggbHlyaWNzIG9y"
    "IGtleXdvcmRzIOKAlCBubyBsaW5rcyAiCiAgICAgICAgICAgICJhbGxvd2VkLiBVc2UgL2RsIG9yIC95bCBmb3IgbGlua3MuIgog"
    "ICAgICAgICkKICAgICAgICByZXR1cm4KICAgIF9sb2coZiJXWkZJWCAvbGQ6IHNlYXJjaGluZyB7cXVlcnlbOjEwMF19IikKICAg"
    "IHRyeToKICAgICAgICBub3RlID0gYXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KCLwn5SOIFNlYXJjaGluZyBmb3IgdGhlIHNvbmcu"
    "Li4iKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIF9sb2coZiJXWkZJWCAvbGQ6IGNhbm5vdCByZXBseToge2V9"
    "IikKICAgICAgICByZXR1cm4KICAgIGhpdHMsIGNsZWFuLCBscmMgPSBhd2FpdCBfZmluZF9zb25ncyhxdWVyeSkKICAgIF9sb2co"
    "CiAgICAgICAgZiJXWkZJWCAvbGQ6IHtsZW4oaGl0cyl9IG1hdGNoKGVzKSIKICAgICAgICArIChmIiwgdG9wIOKGkiB7aGl0c1sw"
    "XVswXVs6NjBdfSIgaWYgaGl0cyBlbHNlICIiKQogICAgKQogICAgaWYgbm90IGhpdHM6CiAgICAgICAgYXdhaXQgX2xkX2VkaXQo"
    "CiAgICAgICAgICAgIG5vdGUsICLinYwgTm8gbWF0Y2hlcyBmb3VuZCDigJQgdHJ5IHdpdGggZGlmZmVyZW50IGtleXdvcmRzIgog"
    "ICAgICAgICkKICAgICAgICByZXR1cm4KICAgIGZyb20gcHlyb2dyYW0udHlwZXMgaW1wb3J0IElubGluZUtleWJvYXJkTWFya3Vw"
    "LCBJbmxpbmVLZXlib2FyZEJ1dHRvbgoKICAgIHRpdGxlLCB2aWQsIGR1ciA9IGhpdHNbMF0KICAgIF9MRF9DQUNIRVt2aWRdID0g"
    "Y2xlYW4gb3IgdGl0bGUKICAgIGlmIGxlbihfTERfQ0FDSEUpID4gMTAwOgogICAgICAgIF9MRF9DQUNIRS5jbGVhcigpCiAgICBt"
    "LCBzID0gZGl2bW9kKGludChkdXIgb3IgMCksIDYwKQogICAgb2sgPSBhd2FpdCBfbGRfZWRpdCgKICAgICAgICBub3RlLAogICAg"
    "ICAgIGYiRGlkIHlvdSBtZWFuOlxu8J+OtSA8Yj57Y2xlYW4gb3IgdGl0bGV9PC9iPlxu4o+xIHttfTp7czowMmR9IiwKICAgICAg"
    "ICBJbmxpbmVLZXlib2FyZE1hcmt1cCgKICAgICAgICAgICAgWwogICAgICAgICAgICAgICAgWwogICAgICAgICAgICAgICAgICAg"
    "IElubGluZUtleWJvYXJkQnV0dG9uKAogICAgICAgICAgICAgICAgICAgICAgICAi4pyFIFllcyIsIGNhbGxiYWNrX2RhdGE9ZiJ3"
    "emZpeGxkeTp7dmlkfSIKICAgICAgICAgICAgICAgICAgICApLAogICAgICAgICAgICAgICAgICAgIElubGluZUtleWJvYXJkQnV0"
    "dG9uKAogICAgICAgICAgICAgICAgICAgICAgICAi8J+UhCBSZXZpc2UiLCBjYWxsYmFja19kYXRhPSJ3emZpeGxkciIKICAgICAg"
    "ICAgICAgICAgICAgICApLAogICAgICAgICAgICAgICAgICAgIElubGluZUtleWJvYXJkQnV0dG9uKCLinYwgTm8iLCBjYWxsYmFj"
    "a19kYXRhPSJ3emZpeGxkbiIpLAogICAgICAgICAgICAgICAgXQogICAgICAgICAgICBdCiAgICAgICAgKSwKICAgICkKICAgIGlm"
    "IG9rOgogICAgICAgIF9MRF9TVEFURVttZXNzYWdlLmlkXSA9IHsKICAgICAgICAgICAgImhpdHMiOiBoaXRzLAogICAgICAgICAg"
    "ICAiY3Vyc29yIjogMCwKICAgICAgICAgICAgInEiOiBxdWVyeSwKICAgICAgICAgICAgImxyYyI6IGxyYywKICAgICAgICAgICAg"
    "InJvdW5kcyI6IDAsCiAgICAgICAgfQogICAgICAgIGlmIGxlbihfTERfU1RBVEUpID4gNTA6CiAgICAgICAgICAgIF9MRF9TVEFU"
    "RS5jbGVhcigpCiAgICBlbHNlOgogICAgICAgIF9sb2coIldaRklYIC9sZDogY29uZmlybSBtZXNzYWdlIGNvdWxkIG5vdCBiZSBz"
    "aG93biIpCgoKZGVmIF9sZF9vcmlnKGNiKToKICAgICIiIlRoZSBvcmlnaW5hbCAvbGQgbWVzc2FnZSB0aGlzIGJ1dHRvbiBiZWxv"
    "bmdzIHRvIChieSByZXBseSkuIiIiCiAgICBvcmlnID0gZ2V0YXR0cihjYi5tZXNzYWdlLCAicmVwbHlfdG9fbWVzc2FnZSIsIE5v"
    "bmUpCiAgICBpZiBvcmlnIGlzIE5vbmU6CiAgICAgICAgcmV0dXJuIE5vbmUsIE5vbmUKICAgIHJldHVybiBvcmlnLCBfTERfU1RB"
    "VEUuZ2V0KGdldGF0dHIob3JpZywgImlkIiwgTm9uZSkpCgoKYXN5bmMgZGVmIF9sZF9kb3dubG9hZChjbGllbnQsIGNiLCB2aWQs"
    "IHRpdGxlKToKICAgIG9yaWcgPSBnZXRhdHRyKGNiLm1lc3NhZ2UsICJyZXBseV90b19tZXNzYWdlIiwgTm9uZSkKICAgIGlmIG9y"
    "aWcgaXMgTm9uZToKICAgICAgICBhd2FpdCBfbGRfZWRpdCgKICAgICAgICAgICAgY2IubWVzc2FnZSwgIuKdjCBPcmlnaW5hbCAv"
    "bGQgY29tbWFuZCBub3QgZm91bmQg4oCUIHNlbmQgaXQgYWdhaW4iCiAgICAgICAgKQogICAgICAgIHJldHVybgogICAgaWYgdmlk"
    "IGluIF9MRF9ET05FOgogICAgICAgIHJldHVybgogICAgX0xEX0RPTkUuYWRkKHZpZCkKICAgIGlmIGxlbihfTERfRE9ORSkgPiAy"
    "MDA6CiAgICAgICAgX0xEX0RPTkUuY2xlYXIoKQogICAgX2xvZyhmIldaRklYIC9sZDogY29uZmlybWVkIHt2aWR9IikKICAgIG9y"
    "aWcudGV4dCA9IGYiL3lsIGh0dHBzOi8vd3d3LnlvdXR1YmUuY29tL3dhdGNoP3Y9e3ZpZH0iCiAgICBvcmlnLl93emZpeF9sZCA9"
    "IFRydWUKICAgIGlmIHRpdGxlOgogICAgICAgIG9yaWcuX3d6Zml4X211c2ljX3RpdGxlID0gdGl0bGUKICAgIGF3YWl0IF9sZF9l"
    "ZGl0KGNiLm1lc3NhZ2UsICLirIfvuI8gU3RhcnRpbmcgdGhlIGRvd25sb2FkLi4uIikKICAgIHRyeToKICAgICAgICBmcm9tIGJv"
    "dC5tb2R1bGVzLnl0ZGxwIGltcG9ydCB5dGRsX2xlZWNoCgogICAgICAgIGF3YWl0IHl0ZGxfbGVlY2goY2xpZW50LCBvcmlnKQog"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIF9sb2coZiJXWkZJWCAvbGQ6IGRvd25sb2FkIGZhaWxlZDoge2V9IikK"
    "ICAgICAgICBhd2FpdCBfbGRfZWRpdCgKICAgICAgICAgICAgY2IubWVzc2FnZSwgIuKaoO+4jyBDb3VsZG4ndCBzdGFydCB0aGUg"
    "ZG93bmxvYWQg4oCUIHRyeSBhZ2FpbiIKICAgICAgICApCgoKYXN5bmMgZGVmIG11c2ljX2xkX3llcyhjbGllbnQsIGNiKToKICAg"
    "IGF3YWl0IGNiLmFuc3dlcigpCiAgICB2aWQgPSBzdHIoY2IuZGF0YSkuc3BsaXQoIjoiLCAxKVsxXQogICAgYXdhaXQgX2xkX2Rv"
    "d25sb2FkKGNsaWVudCwgY2IsIHZpZCwgX0xEX0NBQ0hFLmdldCh2aWQpKQoKCmFzeW5jIGRlZiBtdXNpY19sZF9yZXZpc2UoY2xp"
    "ZW50LCBjYik6CiAgICBhd2FpdCBjYi5hbnN3ZXIoKQogICAgb3JpZywgc3QgPSBfbGRfb3JpZyhjYikKICAgIGlmIG5vdCBzdDoK"
    "ICAgICAgICBhd2FpdCBfbGRfZWRpdCgKICAgICAgICAgICAgY2IubWVzc2FnZSwgIuKdjCBTZWFyY2ggZXhwaXJlZCDigJQgc2Vu"
    "ZCAvbGQgYWdhaW4iCiAgICAgICAgKQogICAgICAgIHJldHVybgogICAgX2xvZygiV1pGSVggL2xkOiByZXZpc2Ug4oaSIHNob3dp"
    "bmcgdG9wIG1hdGNoZXMiKQogICAgYXdhaXQgX2xkX2VkaXQoCiAgICAgICAgY2IubWVzc2FnZSwKICAgICAgICAi8J+OpyA8Yj5N"
    "YXRjaGVzIGZvciB5b3VyIGx5cmljczwvYj5cblxuVGFwIG9uZSB0byBkb3dubG9hZDoiLAogICAgICAgIF9sZF9wYWdlX2tiKHN0"
    "KSwKICAgICkKCgphc3luYyBkZWYgbXVzaWNfbGRfcGljayhjbGllbnQsIGNiKToKICAgIGF3YWl0IGNiLmFuc3dlcigpCiAgICB0"
    "cnk6CiAgICAgICAgaSA9IGludChzdHIoY2IuZGF0YSkuc3BsaXQoIjoiLCAxKVsxXSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAg"
    "ICAgICAgcmV0dXJuCiAgICBvcmlnLCBzdCA9IF9sZF9vcmlnKGNiKQogICAgaWYgbm90IHN0IG9yIGkgPj0gbGVuKHN0WyJoaXRz"
    "Il0pOgogICAgICAgIGF3YWl0IF9sZF9lZGl0KAogICAgICAgICAgICBjYi5tZXNzYWdlLCAi4p2MIFRoYXQgcGljayBleHBpcmVk"
    "IOKAlCBzZW5kIC9sZCBhZ2FpbiIKICAgICAgICApCiAgICAgICAgcmV0dXJuCiAgICB0aXRsZSwgdmlkLCBkdXIgPSBzdFsiaGl0"
    "cyJdW2ldCiAgICBfTERfQ0FDSEVbdmlkXSA9IF9MRF9DQUNIRS5nZXQodmlkKSBvciB0aXRsZQogICAgYXdhaXQgX2xkX2Rvd25s"
    "b2FkKGNsaWVudCwgY2IsIHZpZCwgX0xEX0NBQ0hFLmdldCh2aWQpKQoKCmFzeW5jIGRlZiBtdXNpY19sZF9tb3JlKGNsaWVudCwg"
    "Y2IpOgogICAgYXdhaXQgY2IuYW5zd2VyKCkKICAgIG9yaWcsIHN0ID0gX2xkX29yaWcoY2IpCiAgICBpZiBub3Qgc3Q6CiAgICAg"
    "ICAgYXdhaXQgX2xkX2VkaXQoCiAgICAgICAgICAgIGNiLm1lc3NhZ2UsICLinYwgU2VhcmNoIGV4cGlyZWQg4oCUIHNlbmQgL2xk"
    "IGFnYWluIgogICAgICAgICkKICAgICAgICByZXR1cm4KICAgIHN0WyJjdXJzb3IiXSArPSA1CiAgICBpZiBzdFsiY3Vyc29yIl0g"
    "Pj0gMjA6CiAgICAgICAgX0xEX1NUQVRFLnBvcChnZXRhdHRyKG9yaWcsICJpZCIsIE5vbmUpLCBOb25lKQogICAgICAgIGF3YWl0"
    "IF9sZF9lZGl0KAogICAgICAgICAgICBjYi5tZXNzYWdlLCAi4p2MIE5vIG1hdGNoZXMgZm91bmQg4oCUIHRyeSB3aXRoIGRpZmZl"
    "cmVudCBrZXl3b3JkcyIKICAgICAgICApCiAgICAgICAgcmV0dXJuCiAgICBpZiBsZW4oc3RbImhpdHMiXSkgPCBzdFsiY3Vyc29y"
    "Il0gKyAxOgogICAgICAgIGF3YWl0IF9sZF9lZGl0KGNiLm1lc3NhZ2UsICLwn5SOIFNlYXJjaGluZyBtb3JlLi4uIikKICAgICAg"
    "ICBuZXcgPSBhd2FpdCBfc2VhcmNoX21vcmUoc3QpCiAgICAgICAgaWYgbmV3OgogICAgICAgICAgICBzdFsiaGl0cyJdLmV4dGVu"
    "ZChuZXcpCiAgICBpZiBub3Qgc3RbImhpdHMiXVtzdFsiY3Vyc29yIl0gOiBzdFsiY3Vyc29yIl0gKyA1XToKICAgICAgICBfTERf"
    "U1RBVEUucG9wKGdldGF0dHIob3JpZywgImlkIiwgTm9uZSksIE5vbmUpCiAgICAgICAgYXdhaXQgX2xkX2VkaXQoCiAgICAgICAg"
    "ICAgIGNiLm1lc3NhZ2UsICLinYwgTm8gbWF0Y2hlcyBmb3VuZCDigJQgdHJ5IHdpdGggZGlmZmVyZW50IGtleXdvcmRzIgogICAg"
    "ICAgICkKICAgICAgICByZXR1cm4KICAgIGF3YWl0IF9sZF9lZGl0KAogICAgICAgIGNiLm1lc3NhZ2UsCiAgICAgICAgIvCfjqcg"
    "PGI+TWF0Y2hlcyBmb3IgeW91ciBseXJpY3M8L2I+XG5cblRhcCBvbmUgdG8gZG93bmxvYWQ6IiwKICAgICAgICBfbGRfcGFnZV9r"
    "YihzdCksCiAgICApCgoKYXN5bmMgZGVmIG11c2ljX2xkX25vKGNsaWVudCwgY2IpOgogICAgYXdhaXQgY2IuYW5zd2VyKCkKICAg"
    "IG9yaWcsIHN0ID0gX2xkX29yaWcoY2IpCiAgICB0eHQgPSAoZ2V0YXR0cihjYi5tZXNzYWdlLCAidGV4dCIsICIiKSBvciAiIikK"
    "ICAgIGlmICJNYXRjaGVzIGZvciB5b3VyIGx5cmljcyIgaW4gdHh0OgogICAgICAgICMgQ2FuY2VsIGZyb20gdGhlIGxpc3Qgdmll"
    "dyDigJQgZW5kIHRoZSBzZWFyY2gKICAgICAgICBpZiBvcmlnIGlzIG5vdCBOb25lOgogICAgICAgICAgICBfTERfU1RBVEUucG9w"
    "KGdldGF0dHIob3JpZywgImlkIiwgTm9uZSksIE5vbmUpCiAgICAgICAgYXdhaXQgX2xkX2VkaXQoCiAgICAgICAgICAgIGNiLm1l"
    "c3NhZ2UsICLinYwgU2VhcmNoIGNhbmNlbGxlZCDigJQgc2VuZCAvbGQgYWdhaW4gYW55dGltZSIKICAgICAgICApCiAgICAgICAg"
    "cmV0dXJuCiAgICBpZiBub3Qgc3Q6CiAgICAgICAgYXdhaXQgX2xkX2VkaXQoCiAgICAgICAgICAgIGNiLm1lc3NhZ2UsICLinYwg"
    "U2VhcmNoIGV4cGlyZWQg4oCUIHNlbmQgL2xkIGFnYWluIgogICAgICAgICkKICAgICAgICByZXR1cm4KICAgIGlmIGxlbihzdFsi"
    "aGl0cyJdKSA8IDI6CiAgICAgICAgX0xEX1NUQVRFLnBvcChnZXRhdHRyKG9yaWcsICJpZCIsIE5vbmUpLCBOb25lKQogICAgICAg"
    "IGF3YWl0IF9sZF9lZGl0KAogICAgICAgICAgICBjYi5tZXNzYWdlLCAi4p2MIE5vIG90aGVyIG1hdGNoZXMgZm91bmQg4oCUIHRy"
    "eSBkaWZmZXJlbnQga2V5d29yZHMiCiAgICAgICAgKQogICAgICAgIHJldHVybgogICAgIyAiTm8iIG9uIHRoZSBjb25maXJtIGNh"
    "cmQg4oaSIHNob3cgdGhlIE9USEVSIG1hdGNoZXMKICAgIHN0WyJoaXRzIl0gPSBzdFsiaGl0cyJdWzE6XQogICAgc3RbImN1cnNv"
    "ciJdID0gMAogICAgX2xvZygiV1pGSVggL2xkOiBubyDihpIgc2hvd2luZyBvdGhlciBtYXRjaGVzIikKICAgIGF3YWl0IF9sZF9l"
    "ZGl0KAogICAgICAgIGNiLm1lc3NhZ2UsCiAgICAgICAgIvCfjqcgPGI+TWF0Y2hlcyBmb3IgeW91ciBseXJpY3M8L2I+XG5cblRh"
    "cCBvbmUgdG8gZG93bmxvYWQ6IiwKICAgICAgICBfbGRfcGFnZV9rYihzdCksCiAgICApCg=="
)




# bot/modules/wzfix_admin.py — Telegram commands: /find /usage /qusers
# /setcap /addcap /resetcap /botcap /dbstats /dbclean
# WZFIX Round 5 (v15.82) — per-link stream passwords.
WZFIX_R5_STREAMPASS_B64 = (
    "IiIiV1pGSVggUm91bmQgNSAodjE1LjYyKSDigJQgcGVyLWxpbmsgc3RyZWFtIHBhc3N3b3Jkcy4KCkV2ZXJ5IHN0cmVhbSBsaW5r"
    "ICgvc3RyZWFtLzx0b2tlbj4sIC9kbC88dG9rZW4+KSBjYW4gY2FycnkgaXRzIG93bgpwYXNzd29yZCwgcmVwbGFjaW5nIHRoZSBz"
    "aW5nbGUgZ2xvYmFsIFNUUkVBTV9QQVNTIGZvciB0aGF0IG9uZSBsaW5rLgpQYXNzd29yZHMgbGl2ZSBpbiBNb25nb0RCICh3emZp"
    "eF9zdHJlYW1wYXNzKSBzbyB0aGV5IHN1cnZpdmUgcmVzdGFydHMKYW5kIGFyZSBzaGFyZWQgbGl2ZSBiZXR3ZWVuIHRoZSBUZWxl"
    "Z3JhbSBjb21tYW5kIGFuZCB0aGUgc3RyZWFtCnNlcnZlciAoc2FtZSBwcm9jZXNzKS4KCkJlaGF2aW9yOgotIGxpbmsgV0lUSCBh"
    "IGN1c3RvbSBwYXNzd29yZCDihpIgdGhhdCBwYXNzd29yZCB1bmxvY2tzIGl0OyB0aGUgZ2xvYmFsCiAgU1RSRUFNX1BBU1MgZG9l"
    "cyBOT1QuIENoZWNrZWQgb24gZXZlcnkgcGFnZS9tZXRhL2RhdGEgcmVxdWVzdC4KLSBsaW5rIFdJVEhPVVQg4oaSIGNvbXBsZXRl"
    "bHkgdW5jaGFuZ2VkIGxlZ2FjeSBiZWhhdmlvci4KCk93bmVyL3N1ZG8gY29tbWFuZDoKICAvc3RyZWFtcGFzcyBzZXQgPHVybC1v"
    "ci10b2tlbj4gPHBhc3N3b3JkPgogIC9zdHJlYW1wYXNzIGRlbCA8dXJsLW9yLXRva2VuPgogIC9zdHJlYW1wYXNzIGxpc3QKCkZh"
    "aWwtb3BlbjogaWYgTW9uZ29EQiBpcyB1bnJlYWNoYWJsZSB0aGUgZ2F0ZSBsZXRzIHJlcXVlc3RzIHRocm91Z2gg4oCUCmEgc3Rv"
    "cmFnZSBvdXRhZ2UgbXVzdCBuZXZlciB0YWtlIHN0cmVhbXMgZG93bi4KIiIiCgppbXBvcnQgcmUKaW1wb3J0IHRpbWUKCmZyb20g"
    "Ym90IGltcG9ydCBMT0dHRVIKCl9sb2cgPSBMT0dHRVIuaW5mbyBpZiBMT0dHRVIgZWxzZSBwcmludAoKX1RPS0VOX1JFID0gcmUu"
    "Y29tcGlsZShyIl5bQS1aYS16MC05Xy1dezQsMTI4fSQiKQpfTUFYX1BBU1MgPSA2NAoKCmRlZiBfZGIoKToKICAgIGZyb20gLi5l"
    "eHRfdXRpbHMuZGJfaGFuZGxlciBpbXBvcnQgZGF0YWJhc2UKCiAgICByZXR1cm4gZGF0YWJhc2UuZGIKCgpkZWYgX3BhcnQoKToK"
    "ICAgIGZyb20gLi4uY29yZS50Z19jbGllbnQgaW1wb3J0IGRiX3BhcnRpdGlvbl9pZAogICAgZnJvbSAuLi5jb3JlLmNvbmZpZ19t"
    "YW5hZ2VyIGltcG9ydCBDb25maWcKCiAgICByZXR1cm4gZGJfcGFydGl0aW9uX2lkKENvbmZpZy5CT1RfVE9LRU4uc3BsaXQoIjoi"
    "LCAxKVswXSkKCgpkZWYgZXh0cmFjdF90b2tlbih0ZXh0KToKICAgICIiIlRva2VuIGZyb20gYSBwYXN0ZWQgVVJMICgvc3RyZWFt"
    "L1RPS0VOLCAvZGwvVE9LRU4/eCkgb3IgYSBiYXJlIHRva2VuLiIiIgogICAgdCA9ICh0ZXh0IG9yICIiKS5zdHJpcCgpCiAgICBp"
    "ZiBub3QgdDoKICAgICAgICByZXR1cm4gIiIKICAgIHQgPSB0LnNwbGl0KCI/IiwgMSlbMF0uc3BsaXQoIiMiLCAxKVswXQogICAg"
    "aWYgIi8iIGluIHQ6CiAgICAgICAgdCA9IHQucnN0cmlwKCIvIikuc3BsaXQoIi8iKVstMV0KICAgIHJldHVybiB0IGlmIF9UT0tF"
    "Tl9SRS5tYXRjaCh0KSBlbHNlICIiCgoKZGVmIHBhdGhfdG9rZW4ocmVxdWVzdCk6CiAgICAiIiJTdHJlYW0gdG9rZW4gZnJvbSBh"
    "biBhaW9odHRwIHJlcXVlc3QgcGF0aCAoL19zdHJlYW0vVE9LRU4gZXRjLikuIiIiCiAgICB0cnk6CiAgICAgICAgc2VnID0gW3Ag"
    "Zm9yIHAgaW4gcmVxdWVzdC5wYXRoLnNwbGl0KCIvIikgaWYgcF0KICAgICAgICB0b2sgPSBzZWdbLTFdIGlmIHNlZyBlbHNlICIi"
    "CiAgICAgICAgcmV0dXJuIHRvayBpZiBfVE9LRU5fUkUubWF0Y2godG9rKSBlbHNlICIiCiAgICBleGNlcHQgRXhjZXB0aW9uOgog"
    "ICAgICAgIHJldHVybiAiIgoKCmFzeW5jIGRlZiBnZXRfbGlua19wYXNzKHRva2VuKToKICAgICIiIkN1c3RvbSBwYXNzd29yZCBm"
    "b3IgdGhpcyBsaW5rLCBvciBOb25lIHdoZW4gaXQgaGFzIG5vbmUuIiIiCiAgICB0cnk6CiAgICAgICAgZG9jID0gYXdhaXQgX2Ri"
    "KCkud3pmaXhfc3RyZWFtcGFzc1tfcGFydCgpXS5maW5kX29uZSh7Il9pZCI6IHRva2VufSkKICAgICAgICBwID0gKGRvYyBvciB7"
    "fSkuZ2V0KCJwYXNzIikgb3IgIiIKICAgICAgICByZXR1cm4gcCBvciBOb25lCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAg"
    "ICAgICAgX2xvZyhmIldaRklYIHI1OiBnZXRfbGlua19wYXNzIGZhaWxlZDoge2V9IikKICAgICAgICByZXR1cm4gTm9uZQoKCmFz"
    "eW5jIGRlZiBzZXRfbGlua19wYXNzKHRva2VuLCBwYXNzd29yZCwgYnkpOgogICAgYXdhaXQgX2RiKCkud3pmaXhfc3RyZWFtcGFz"
    "c1tfcGFydCgpXS51cGRhdGVfb25lKAogICAgICAgIHsiX2lkIjogdG9rZW59LAogICAgICAgIHsiJHNldCI6IHsicGFzcyI6IHBh"
    "c3N3b3JkLCAiYnkiOiBieSwgImF0IjogdGltZS50aW1lKCl9fSwKICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICkKCgphc3luYyBk"
    "ZWYgZGVsX2xpbmtfcGFzcyh0b2tlbik6CiAgICB0cnk6CiAgICAgICAgciA9IGF3YWl0IF9kYigpLnd6Zml4X3N0cmVhbXBhc3Nb"
    "X3BhcnQoKV0uZGVsZXRlX29uZSh7Il9pZCI6IHRva2VufSkKICAgICAgICByZXR1cm4gci5kZWxldGVkX2NvdW50ID4gMAogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gRmFsc2UKCgphc3luYyBkZWYgYWxsX2xpbmtfcGFzc2VzKCk6CiAgICBj"
    "dXIgPSAoCiAgICAgICAgX2RiKCkud3pmaXhfc3RyZWFtcGFzc1tfcGFydCgpXQogICAgICAgIC5maW5kKHt9LCB7Il9pZCI6IDEs"
    "ICJwYXNzIjogMSwgImF0IjogMX0pCiAgICAgICAgLnNvcnQoImF0IiwgLTEpCiAgICAgICAgLmxpbWl0KDUwKQogICAgKQogICAg"
    "cmV0dXJuIFtkIGFzeW5jIGZvciBkIGluIGN1cl0KCgpkZWYgdmVyaWZ5X2xpbmtfdG9rZW4odG9rZW5fdmFsdWUsIHBhc3N3b3Jk"
    "KToKICAgICIiIkhNQUMgY2hlY2sgb2YgYSBzdWJtaXR0ZWQgYXV0aCB0b2tlbiBhZ2FpbnN0IGEgbGluayBwYXNzd29yZC4iIiIK"
    "ICAgIHRyeToKICAgICAgICBmcm9tIC4udXNlcl9zdHJlYW1fbW9kdWxlIGltcG9ydCBfdmVyaWZ5X3Rva2VuCgogICAgICAgIHJl"
    "dHVybiBfdmVyaWZ5X3Rva2VuKHRva2VuX3ZhbHVlIG9yICIiLCBwYXNzd29yZCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAg"
    "ICAgcmV0dXJuIEZhbHNlCgoKYXN5bmMgZGVmIHNlcnZlX29rKHJlcXVlc3QpOgogICAgIiIiUGVyLWxpbmsgZ2F0ZSBmb3IgdGhl"
    "IHN0cmVhbSBzZXJ2ZXIuIFRydWUgPSBhbGxvdy4iIiIKICAgIHRyeToKICAgICAgICB0b2sgPSBwYXRoX3Rva2VuKHJlcXVlc3Qp"
    "CiAgICAgICAgaWYgbm90IHRvazoKICAgICAgICAgICAgcmV0dXJuIFRydWUKICAgICAgICBscCA9IGF3YWl0IGdldF9saW5rX3Bh"
    "c3ModG9rKQogICAgICAgIGlmIGxwIGlzIE5vbmU6CiAgICAgICAgICAgIHJldHVybiBUcnVlCiAgICAgICAgcmV0dXJuIHZlcmlm"
    "eV9saW5rX3Rva2VuKHJlcXVlc3QucXVlcnkuZ2V0KCJhdXRoIiksIGxwKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBy"
    "ZXR1cm4gVHJ1ZQoKCiMg4pSA4pSA4pSAIG93bmVyIGNvbW1hbmQg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgpfSEVMUCA9ICgKICAgICI8Yj5TdHJlYW0tbGluayBwYXNzd29y"
    "ZHM8L2I+XG5cbiIKICAgICIvc3RyZWFtcGFzcyBzZXQgPGNvZGU+PGxpbms+PC9jb2RlPiA8Y29kZT48cGFzc3dvcmQ+PC9jb2Rl"
    "PiIKICAgICIg4oCUIHByb3RlY3Qgb25lIHN0cmVhbSBsaW5rXG4iCiAgICAiL3N0cmVhbXBhc3MgZGVsIDxjb2RlPjxsaW5rPjwv"
    "Y29kZT4g4oCUIHJlbW92ZSBpdCIKICAgICIgKGZhbGxzIGJhY2sgdG8gdGhlIGdsb2JhbCBwYXNzd29yZClcbiIKICAgICIvc3Ry"
    "ZWFtcGFzcyBsaXN0IOKAlCBsaW5rcyB3aXRoIHRoZWlyIG93biBwYXNzd29yZFxuXG4iCiAgICAiUGFzdGUgdGhlIGZ1bGwgc3Ry"
    "ZWFtIGxpbmsgb3IganVzdCBpdHMgdG9rZW4uIEEgbGluayB3aXRoIGl0cyBvd24gIgogICAgInBhc3N3b3JkIG5vIGxvbmdlciBh"
    "Y2NlcHRzIHRoZSBnbG9iYWwgb25lLiIKKQoKCmFzeW5jIGRlZiB3emZpeF9zdHJlYW1wYXNzKGNsaWVudCwgbWVzc2FnZSk6CiAg"
    "ICBhcmdzID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3BsaXQoKQogICAgc3ViID0gKGFyZ3NbMV0ubG93ZXIoKSBpZiBsZW4oYXJn"
    "cykgPiAxIGVsc2UgImhlbHAiKQoKICAgIGlmIHN1YiBpbiAoImhlbHAiLCAic3RhcnQiKToKICAgICAgICByZXR1cm4gYXdhaXQg"
    "bWVzc2FnZS5yZXBseV90ZXh0KF9IRUxQKQoKICAgIGlmIHN1YiA9PSAibGlzdCI6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBy"
    "b3dzID0gYXdhaXQgYWxsX2xpbmtfcGFzc2VzKCkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgIHJl"
    "dHVybiBhd2FpdCBtZXNzYWdlLnJlcGx5X3RleHQoZiLinYwgREIgZXJyb3I6IHtlfSIpCiAgICAgICAgaWYgbm90IHJvd3M6CiAg"
    "ICAgICAgICAgIHJldHVybiBhd2FpdCBtZXNzYWdlLnJlcGx5X3RleHQoCiAgICAgICAgICAgICAgICAiTm8gbGlua3MgaGF2ZSBh"
    "IGN1c3RvbSBwYXNzd29yZCDigJQgIgogICAgICAgICAgICAgICAgImV2ZXJ5dGhpbmcgdXNlcyB0aGUgZ2xvYmFsIFNUUkVBTV9Q"
    "QVNTLiIKICAgICAgICAgICAgKQogICAgICAgIG91dCA9IFsiPGI+Q3VzdG9tIHN0cmVhbSBwYXNzd29yZHM8L2I+XG4iXQogICAg"
    "ICAgIGZvciBkIGluIHJvd3M6CiAgICAgICAgICAgIG91dC5hcHBlbmQoCiAgICAgICAgICAgICAgICBmIuKAoiA8Y29kZT57ZFsn"
    "X2lkJ119PC9jb2RlPiDihpIgPGNvZGU+e2RbJ3Bhc3MnXX08L2NvZGU+IgogICAgICAgICAgICApCiAgICAgICAgb3V0LmFwcGVu"
    "ZChmIlxue2xlbihyb3dzKX0gbGluayhzKSIpCiAgICAgICAgcmV0dXJuIGF3YWl0IG1lc3NhZ2UucmVwbHlfdGV4dCgiXG4iLmpv"
    "aW4ob3V0KSkKCiAgICBpZiBzdWIgPT0gInNldCI6CiAgICAgICAgaWYgbGVuKGFyZ3MpIDwgNDoKICAgICAgICAgICAgcmV0dXJu"
    "IGF3YWl0IG1lc3NhZ2UucmVwbHlfdGV4dCgKICAgICAgICAgICAgICAgICJVc2FnZTogL3N0cmVhbXBhc3Mgc2V0IDxsaW5rPiA8"
    "cGFzc3dvcmQ+IgogICAgICAgICAgICApCiAgICAgICAgdG9rID0gZXh0cmFjdF90b2tlbihhcmdzWzJdKQogICAgICAgIHB3ID0g"
    "YXJnc1szXQogICAgICAgIGlmIG5vdCB0b2s6CiAgICAgICAgICAgIHJldHVybiBhd2FpdCBtZXNzYWdlLnJlcGx5X3RleHQoCiAg"
    "ICAgICAgICAgICAgICAi4p2MIFRoYXQgZG9lc24ndCBsb29rIGxpa2UgYSBzdHJlYW0gbGluayBvciB0b2tlbi4iCiAgICAgICAg"
    "ICAgICkKICAgICAgICBpZiBsZW4ocHcpID4gX01BWF9QQVNTOgogICAgICAgICAgICByZXR1cm4gYXdhaXQgbWVzc2FnZS5yZXBs"
    "eV90ZXh0KAogICAgICAgICAgICAgICAgZiLinYwgUGFzc3dvcmQgdG9vIGxvbmcgKG1heCB7X01BWF9QQVNTfSBjaGFycywgbm8g"
    "c3BhY2VzKS4iCiAgICAgICAgICAgICkKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IHNldF9saW5rX3Bhc3ModG9rLCBw"
    "dywgbWVzc2FnZS5mcm9tX3VzZXIuaWQpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICByZXR1cm4g"
    "YXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KGYi4p2MIERCIGVycm9yOiB7ZX0iKQogICAgICAgIF9sb2coZiJXWkZJWCByNTogc3Ry"
    "ZWFtIHBhc3Mgc2V0IGZvciB7dG9rfSIpCiAgICAgICAgcmV0dXJuIGF3YWl0IG1lc3NhZ2UucmVwbHlfdGV4dCgKICAgICAgICAg"
    "ICAgZiLinIUgTG9ja2VkLlxuPGNvZGU+e3Rva308L2NvZGU+IG5vdyBuZWVkcyB0aGUgcGFzc3dvcmQgIgogICAgICAgICAgICBm"
    "Ijxjb2RlPntwd308L2NvZGU+IOKAlCB0aGUgZ2xvYmFsIHBhc3N3b3JkIG5vIGxvbmdlciBvcGVucyBpdC4gIgogICAgICAgICAg"
    "ICAiQXBwbGllcyBpbW1lZGlhdGVseTsgZGVsZXRlIHRoZSBtZXNzYWdlIHRvIGhpZGUgdGhlIHBhc3N3b3JkLiIKICAgICAgICAp"
    "CgogICAgaWYgc3ViIGluICgiZGVsIiwgImRlbGV0ZSIsICJybSIpOgogICAgICAgIGlmIGxlbihhcmdzKSA8IDM6CiAgICAgICAg"
    "ICAgIHJldHVybiBhd2FpdCBtZXNzYWdlLnJlcGx5X3RleHQoIlVzYWdlOiAvc3RyZWFtcGFzcyBkZWwgPGxpbms+IikKICAgICAg"
    "ICB0b2sgPSBleHRyYWN0X3Rva2VuKGFyZ3NbMl0pCiAgICAgICAgaWYgbm90IHRvazoKICAgICAgICAgICAgcmV0dXJuIGF3YWl0"
    "IG1lc3NhZ2UucmVwbHlfdGV4dCgKICAgICAgICAgICAgICAgICLinYwgVGhhdCBkb2Vzbid0IGxvb2sgbGlrZSBhIHN0cmVhbSBs"
    "aW5rIG9yIHRva2VuLiIKICAgICAgICAgICAgKQogICAgICAgIHJlbW92ZWQgPSBhd2FpdCBkZWxfbGlua19wYXNzKHRvaykKICAg"
    "ICAgICByZXR1cm4gYXdhaXQgbWVzc2FnZS5yZXBseV90ZXh0KAogICAgICAgICAgICBmIuKchSBSZW1vdmVkIOKAlCA8Y29kZT57"
    "dG9rfTwvY29kZT4gaXMgYmFjayB0byB0aGUgZ2xvYmFsIHBhc3N3b3JkLiIKICAgICAgICAgICAgaWYgcmVtb3ZlZAogICAgICAg"
    "ICAgICBlbHNlIGYi4oS577iPIDxjb2RlPnt0b2t9PC9jb2RlPiBoYWQgbm8gY3VzdG9tIHBhc3N3b3JkLiIKICAgICAgICApCgog"
    "ICAgcmV0dXJuIGF3YWl0IG1lc3NhZ2UucmVwbHlfdGV4dChfSEVMUCkK"
)

WZFIX_ADMIN_B64 = (
    "IyBXWkZJWCBSb3VuZCAxICh2MTUuNykg4oCUIG93bmVyICYgdXNlciBjb21tYW5kcy4KIwojICAgL2ZpbmQgPHF1ZXJ5PiAgICAg"
    "ICAgc2VhcmNoIHlvdXIgZG93bmxvYWQgbGlicmFyeSAob3duZXI6IC9maW5kIC1hIDxxPiA9IGFsbCB1c2VycykKIyAgIC91c2Fn"
    "ZSAgICAgICAgICAgICAgIHlvdXIgYmFuZHdpZHRoIHVzYWdlIHRvZGF5IChvd25lciBhbHNvIHNlZXMgZXZlcnlvbmUpCiMgICAv"
    "cXVzZXJzICAgICAgICAgICAgICBvd25lcjogdXNlciByZWdpc3RyeSB3aXRoIHVzYWdlICsgY2FwcwojICAgL3NldGNhcCA8aWQ+"
    "IDxHQj4gICAgb3duZXI6IHNldCBhIHBlci11c2VyIGNhcCAoMCA9IGJhY2sgdG8gZ2xvYmFsIGRlZmF1bHQpCiMgICAvYWRkY2Fw"
    "IDxpZD4gPEdCPiAgICBvd25lcjogcmFpc2UgYSB1c2VyJ3MgY2FwIGJ5IEdCCiMgICAvcmVzZXRjYXAgPGlkPiAgICAgICBvd25l"
    "cjogcmVzZXQgYSB1c2VyJ3MgdXNhZ2UgZm9yIHRvZGF5CiMgICAvYm90Y2FwIDxHQj4gICAgICAgICBvd25lcjogZ2xvYmFsIGRl"
    "ZmF1bHQgY2FwCiMgICAvZGJzdGF0cyAgICAgICAgICAgICBvd25lcjogTW9uZ29EQiBjb2xsZWN0aW9uIHNpemVzCiMgICAvZGJj"
    "bGVhbiAgICAgICAgICAgICBvd25lcjogcHVyZ2UgZXhwaXJlZCB0b2tlbnMgLyBzdGFsZSByZWNvcmRzIChjb25maXJtIGJ1dHRv"
    "bikKCmZyb20gZGF0ZXRpbWUgaW1wb3J0IGRhdGV0aW1lCmZyb20gdGltZSBpbXBvcnQgdGltZQpmcm9tIGh0bWwgaW1wb3J0IGVz"
    "Y2FwZQoKZnJvbSAuLiBpbXBvcnQgYm90X2xvb3AsIHVzZXJfZGF0YQpmcm9tIC4uY29yZS5jb25maWdfbWFuYWdlciBpbXBvcnQg"
    "Q29uZmlnCmZyb20gLi5oZWxwZXIuZXh0X3V0aWxzLnN0YXR1c191dGlscyBpbXBvcnQgZ2V0X3JlYWRhYmxlX2ZpbGVfc2l6ZQpm"
    "cm9tIC4uaGVscGVyLnRlbGVncmFtX2hlbHBlci5idXR0b25fYnVpbGQgaW1wb3J0IEJ1dHRvbk1ha2VyCmZyb20gLi5oZWxwZXIu"
    "dGVsZWdyYW1faGVscGVyLm1lc3NhZ2VfdXRpbHMgaW1wb3J0ICgKICAgIGF1dG9fZGVsZXRlX21lc3NhZ2UsCiAgICBzZW5kX21l"
    "c3NhZ2UsCikKZnJvbSAuLmhlbHBlci53emZpeC5yMV9jb3JlIGltcG9ydCAoCiAgICBJU1QsCiAgICBhbGxfdXNlcnMsCiAgICBk"
    "YmNsZWFuLAogICAgZGJzdGF0cywKICAgIGRlbGV0ZV91c2VyLAogICAgZmluZCwKICAgIGdldF9jYXBfYnl0ZXMsCiAgICBnZXRf"
    "dXNhZ2UsCiAgICByZXNldF91c2FnZSwKICAgIHNldF9nbG9iYWxfY2FwX2diLAogICAgc2V0X3VzZXJfY2FwLAogICAgdG9kYXlf"
    "cm93cywKICAgIHVzZXJfZXhpc3RzLAogICAgcmVzZXJ2ZWRfdG9kYXksCiAgICBib290X3JlcG9ydCwKKQpmcm9tIC4uaGVscGVy"
    "Lnd6Zml4LnIyX3dlYiBpbXBvcnQgKAogICAgX2FsbG93X2FsbCwKICAgIF9hbGxvd19pcCwKICAgIF9saXN0X2JhbnMsCiAgICBn"
    "ZXRfYWRtaW5fcGFzcywKICAgIHNldF9hZG1pbl9wYXNzLAogICAgc3RhcnRfbG9vcHMsCikKCgphc3luYyBkZWYgX2RiX29rKCk6"
    "CiAgICBmcm9tIC4uaGVscGVyLnd6Zml4LnIxX2NvcmUgaW1wb3J0IGVuc3VyZV9yZWFkeQoKICAgIHJldHVybiBhd2FpdCBlbnN1"
    "cmVfcmVhZHkoKQoKCl9EQl9ET1dOID0gIuKdjCBEYXRhYmFzZSBub3QgY29ubmVjdGVkIOKAlCBzZXQgREFUQUJBU0VfVVJMIGFu"
    "ZCB0cnkgYWdhaW4uIgoKCmRlZiBfcihuKToKICAgIHRyeToKICAgICAgICByZXR1cm4gZ2V0X3JlYWRhYmxlX2ZpbGVfc2l6ZShp"
    "bnQobikpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBzdHIobikKCgpkZWYgX2lzX293bmVyKG1lc3NhZ2Up"
    "OgogICAgdSA9IG1lc3NhZ2UuZnJvbV91c2VyIG9yIG1lc3NhZ2Uuc2VuZGVyX2NoYXQKICAgIHJldHVybiB1IGFuZCB1LmlkID09"
    "IENvbmZpZy5PV05FUl9JRAoKCmRlZiBfdGFyZ2V0X3VzZXIobWVzc2FnZSwgYXJncyk6CiAgICAiIiJSZXBseS10byB1c2VyLCBl"
    "bHNlIGZpcnN0IG51bWVyaWMgYXJnLCBlbHNlIE5vbmUuIiIiCiAgICBpZiBtZXNzYWdlLnJlcGx5X3RvX21lc3NhZ2UgYW5kIG1l"
    "c3NhZ2UucmVwbHlfdG9fbWVzc2FnZS5mcm9tX3VzZXI6CiAgICAgICAgcmV0dXJuIG1lc3NhZ2UucmVwbHlfdG9fbWVzc2FnZS5m"
    "cm9tX3VzZXIuaWQKICAgIGZvciBhIGluIGFyZ3M6CiAgICAgICAgaWYgYS5sc3RyaXAoIi0iKS5pc2RpZ2l0KCk6CiAgICAgICAg"
    "ICAgIHJldHVybiBpbnQoYSkKICAgIHJldHVybiBOb25lCgoKZGVmIF9wYXJzZV90YXJnZXRfYW5kX2diKG1lc3NhZ2UsIGFyZ3Mp"
    "OgogICAgIiIiUmVzb2x2ZSB0YXJnZXQgYW5kIEdCIGZyb20gZGlmZmVyZW50IGFyZ3MuCgogICAgUmVwbHkgZm9ybTogIC9zZXRj"
    "YXAgNSAgICAgICAodGFyZ2V0ID0gcmVwbGllZCB1c2VyLCBnYiA9IDUpCiAgICBJZCBmb3JtOiAgICAgL3NldGNhcCAxMjMgNSAg"
    "ICh0YXJnZXQgPSAxMjMsIGdiID0gNSDigJQgTk9UIDEyMyEpCiAgICAiIiIKICAgIHRhcmdldCA9IE5vbmUKICAgIHJlc3QgPSBh"
    "cmdzCiAgICBpZiBtZXNzYWdlLnJlcGx5X3RvX21lc3NhZ2UgYW5kIG1lc3NhZ2UucmVwbHlfdG9fbWVzc2FnZS5mcm9tX3VzZXI6"
    "CiAgICAgICAgdGFyZ2V0ID0gbWVzc2FnZS5yZXBseV90b19tZXNzYWdlLmZyb21fdXNlci5pZAogICAgZWxzZToKICAgICAgICBm"
    "b3IgaSwgYSBpbiBlbnVtZXJhdGUoYXJncyk6CiAgICAgICAgICAgIGlmIGEubHN0cmlwKCItIikuaXNkaWdpdCgpOgogICAgICAg"
    "ICAgICAgICAgdGFyZ2V0ID0gaW50KGEpCiAgICAgICAgICAgICAgICByZXN0ID0gYXJnc1tpICsgMSA6XQogICAgICAgICAgICAg"
    "ICAgYnJlYWsKICAgIGdiID0gTm9uZQogICAgZm9yIGEgaW4gcmVzdDoKICAgICAgICBpZiBhLnJlcGxhY2UoIi4iLCAiIiwgMSku"
    "aXNkaWdpdCgpOgogICAgICAgICAgICBnYiA9IGZsb2F0KGEpCiAgICAgICAgICAgIGJyZWFrCiAgICByZXR1cm4gdGFyZ2V0LCBn"
    "YgoKCmRlZiBfZm10X2diKGdiKToKICAgICIiIkh1bWFuLWZyaWVuZGx5IEdCIGxhYmVsIChuZXZlciBzY2llbnRpZmljIG5vdGF0"
    "aW9uKS4iIiIKICAgIHJldHVybiBmIntnYjouMmZ9Ii5yc3RyaXAoIjAiKS5yc3RyaXAoIi4iKSArICIgR0IiCgoKYXN5bmMgZGVm"
    "IF9iYWRfdGFyZ2V0KG1lc3NhZ2UsIHVpZCk6CiAgICAiIiJUcnVlIHdoZW4gdGhlIHJlc29sdmVkIGlkIGNhbid0IGJlIGEgcmVh"
    "bCBUZWxlZ3JhbSB1c2VyIChwaGFudG9tIGd1YXJkKS4iIiIKICAgIGlmIG5vdCAoMCA8IHVpZCA8IDEwMDAwMCk6CiAgICAgICAg"
    "cmV0dXJuIEZhbHNlCiAgICBpZiBhd2FpdCB1c2VyX2V4aXN0cyh1aWQpOgogICAgICAgIHJldHVybiBGYWxzZQogICAgYXdhaXQg"
    "c2VuZF9tZXNzYWdlKAogICAgICAgIG1lc3NhZ2UsCiAgICAgICAgIuKdkyBUaGF0IGRvZXNuJ3QgbG9vayBsaWtlIGEgVGVsZWdy"
    "YW0gdXNlciBJRCDigJQgaXQgd291bGQgY3JlYXRlIGEgIgogICAgICAgICJwaGFudG9tIHJlZ2lzdHJ5IGVudHJ5LiBSZXBseSB0"
    "byB0aGUgdXNlcidzIG1lc3NhZ2UgaW5zdGVhZCwgb3IgdXNlICIKICAgICAgICAidGhlaXIgbnVtZXJpYyBJRCAoc2VlIC9xdXNl"
    "cnMpLiIsCiAgICApCiAgICByZXR1cm4gVHJ1ZQoKCmRlZiBfZm10X2RheSgpOgogICAgcmV0dXJuIGRhdGV0aW1lLm5vdyhJU1Qp"
    "LnN0cmZ0aW1lKCIlZCAlYiAlWSIpCgoKYXN5bmMgZGVmIF9zdGFydF9ib290X3JlcG9ydCgpOgogICAgYXdhaXQgYm9vdF9yZXBv"
    "cnQoKQoKCmJvdF9sb29wLmNyZWF0ZV90YXNrKF9zdGFydF9ib290X3JlcG9ydCgpKQp0cnk6CiAgICBzdGFydF9sb29wcygpICAj"
    "IHIyOiBkYWlseSB1c2FnZSByZXBvcnQgYXQgMjM6NTcgSVNUCmV4Y2VwdCBFeGNlcHRpb246CiAgICBwYXNzCgoKIyDilIDilIAg"
    "L2ZpbmQg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIHd6Zml4X2ZpbmQoY2xp"
    "ZW50LCBtZXNzYWdlKToKICAgIGFyZ3MgPSAobWVzc2FnZS50ZXh0IG9yICIiKS5zcGxpdChtYXhzcGxpdD0xKQogICAgcXVlcnkg"
    "PSBhcmdzWzFdLnN0cmlwKCkgaWYgbGVuKGFyZ3MpID4gMSBlbHNlICIiCiAgICBvd25lciA9IF9pc19vd25lcihtZXNzYWdlKQog"
    "ICAgYWxsX3VzZXJzID0gRmFsc2UKICAgIGlmIG93bmVyIGFuZCBxdWVyeS5zdGFydHN3aXRoKCItYSAiKToKICAgICAgICBhbGxf"
    "dXNlcnMgPSBUcnVlCiAgICAgICAgcXVlcnkgPSBxdWVyeVszOl0uc3RyaXAoKQogICAgaWYgbm90IGF3YWl0IF9kYl9vaygpOgog"
    "ICAgICAgIGF3YWl0IGF1dG9fZGVsZXRlX21lc3NhZ2UoYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIF9EQl9ET1dOKSkKICAg"
    "ICAgICByZXR1cm4KICAgIGlmIG5vdCBxdWVyeToKICAgICAgICBhd2FpdCBhdXRvX2RlbGV0ZV9tZXNzYWdlKAogICAgICAgICAg"
    "ICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgICAgICBtZXNzYWdlLAogICAgICAgICAgICAgICAgIvCflI4gPGI+V1pG"
    "SVggTGlicmFyeTwvYj5cbuKUglxuIgogICAgICAgICAgICAgICAgIuKUoCA8Y29kZT4vZmluZCBpbmNlcHRpb248L2NvZGU+IOKA"
    "lCBzZWFyY2ggeW91ciBkb3dubG9hZHNcbiIKICAgICAgICAgICAgICAgICLilJYgPGNvZGU+L2ZpbmQgLWEgcXVlcnk8L2NvZGU+"
    "IOKAlCBvd25lcjogc2VhcmNoIGV2ZXJ5IHVzZXIncyBkb3dubG9hZHNcbiIKICAgICAgICAgICAgICAgICLilINcbjxpPlNob3dz"
    "IHNpemUsIGRhdGUsIFRlbGVncmFtIHBvc3QgbGlua3MgZm9yIGFsbCBwYXJ0cywgIgogICAgICAgICAgICAgICAgImFuZCBTdHJl"
    "YW0gLyBEb3dubG9hZCBidXR0b25zLjwvaT4iLAogICAgICAgICAgICApCiAgICAgICAgKQogICAgICAgIHJldHVybgogICAgdWlk"
    "ID0gKG1lc3NhZ2UuZnJvbV91c2VyIG9yIG1lc3NhZ2Uuc2VuZGVyX2NoYXQpLmlkCiAgICBkb2NzID0gYXdhaXQgZmluZChxdWVy"
    "eSwgdWlkLCBhbGxfdXNlcnMpCiAgICBpZiBub3QgZG9jczoKICAgICAgICBhd2FpdCBhdXRvX2RlbGV0ZV9tZXNzYWdlKAogICAg"
    "ICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgICAgICBtZXNzYWdlLAogICAgICAgICAgICAgICAgZiLwn5iV"
    "IE5vIGxpYnJhcnkgcmVzdWx0cyBmb3IgPGNvZGU+e2VzY2FwZShxdWVyeSl9PC9jb2RlPi4iCiAgICAgICAgICAgICAgICArICgi"
    "IiBpZiBhbGxfdXNlcnMgZWxzZSAiIChsYXN0IDkwIGRheXMpIiksCiAgICAgICAgICAgICkKICAgICAgICApCiAgICAgICAgcmV0"
    "dXJuCiAgICBmb3IgaSwgZCBpbiBlbnVtZXJhdGUoZG9jcywgMSk6CiAgICAgICAgYnRuID0gQnV0dG9uTWFrZXIoKQogICAgICAg"
    "IHBhcnRzID0gZC5nZXQoInBhcnRzIikgb3IgW10KICAgICAgICBpZiBwYXJ0czoKICAgICAgICAgICAgdHJ5OgogICAgICAgICAg"
    "ICAgICAgZnJvbSAuc3RyZWFtIGltcG9ydCBnZW5fc3RyZWFtX2xpbmsKCiAgICAgICAgICAgICAgICBzbCA9IGF3YWl0IGdlbl9z"
    "dHJlYW1fbGluayhwYXJ0c1swXVswXSwgcGFydHNbMF1bMV0pCiAgICAgICAgICAgICAgICBpZiBzbDoKICAgICAgICAgICAgICAg"
    "ICAgICBidG4udXJsX2J1dHRvbigi4pa2IFN0cmVhbSIsIHNsWzBdKQogICAgICAgICAgICAgICAgICAgIGJ0bi51cmxfYnV0dG9u"
    "KCLirIcgRG93bmxvYWQiLCBzbFsxXSkKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MK"
    "ICAgICAgICB3aGVuID0gZGF0ZXRpbWUuZnJvbXRpbWVzdGFtcChkLmdldCgiZGF0ZSIpIG9yIDAsIElTVCkuc3RyZnRpbWUoCiAg"
    "ICAgICAgICAgICIlZC8lbS8leSAlSDolTSIKICAgICAgICApCiAgICAgICAgdW5hbWUgPSBkLmdldCgidW5hbWUiKSBvciAiIgog"
    "ICAgICAgIHdobyA9IGYiQHtlc2NhcGUodW5hbWUpfSIgaWYgdW5hbWUgZWxzZSBmIjxjb2RlPntkLmdldCgndXNlcl9pZCcpfTwv"
    "Y29kZT4iCiAgICAgICAgbXNnID0gKAogICAgICAgICAgICBmIvCflI4gPGI+UmVzdWx0IHtpfS97bGVuKGRvY3MpfTwvYj5cbiIK"
    "ICAgICAgICAgICAgZiI8Yj57ZXNjYXBlKGQuZ2V0KCduYW1lJykgb3IgJz8nKX08L2I+XG4iCiAgICAgICAgICAgIGYi4pSgIDxi"
    "PlNpemU8L2I+IOKGkiB7X3IoZC5nZXQoJ3NpemUnKSBvciAwKX1cbiIKICAgICAgICAgICAgZiLilKAgPGI+V2hlbjwvYj4g4oaS"
    "IHt3aGVufSBJU1RcbiIKICAgICAgICAgICAgZiLilKAgPGI+Qnk8L2I+IOKGkiB7d2hvfSIKICAgICAgICApCiAgICAgICAgbGlu"
    "a3MgPSBkLmdldCgidGdfbGlua3MiKSBvciBbXQogICAgICAgIGlmIGxlbihsaW5rcykgPT0gMToKICAgICAgICAgICAgbXNnICs9"
    "IGYiXG7ilJYgPGI+UG9zdDwvYj4g4oaSIDxhIGhyZWY9J3tsaW5rc1swXX0nPm9wZW4gaW4gVGVsZWdyYW08L2E+IgogICAgICAg"
    "IGVsaWYgbGlua3M6CiAgICAgICAgICAgIG1zZyArPSAiXG7ilJYgPGI+UGFydHM8L2I+IOKGkiAiICsgIiB8ICIuam9pbigKICAg"
    "ICAgICAgICAgICAgIGYiPGEgaHJlZj0ne2x9Jz5QYXJ0IHtqfTwvYT4iIGZvciBqLCBsIGluIGVudW1lcmF0ZShsaW5rcywgMSkK"
    "ICAgICAgICAgICAgKQogICAgICAgIGVsaWYgZC5nZXQoImNsb3VkX2xpbmsiKToKICAgICAgICAgICAgbXNnICs9IGYiXG7ilJYg"
    "PGI+Q2xvdWQ8L2I+IOKGkiA8YSBocmVmPSd7ZFsnY2xvdWRfbGluayddfSc+b3BlbjwvYT4iCiAgICAgICAgZWxpZiBkLmdldCgi"
    "cmNsb25lX3BhdGgiKToKICAgICAgICAgICAgbXNnICs9IGYiXG7ilJYgPGI+UGF0aDwvYj4g4oaSIDxjb2RlPntlc2NhcGUoZFsn"
    "cmNsb25lX3BhdGgnXSl9PC9jb2RlPiIKICAgICAgICB0cnk6CiAgICAgICAgICAgIG5fYnRucyA9IGxlbihidG4uYnV0dG9ucy5n"
    "ZXQoImRlZmF1bHQiKSBvciBbXSkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBuX2J0bnMgPSAxCiAgICAg"
    "ICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIG1zZywgYnRuLmJ1aWxkX21lbnUoMikgaWYgbl9idG5zIGVsc2UgTm9uZSkK"
    "CgojIOKUgOKUgCAvdXNhZ2Ug4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIHd6"
    "Zml4X3VzYWdlKGNsaWVudCwgbWVzc2FnZSk6CiAgICB1aWQgPSAobWVzc2FnZS5mcm9tX3VzZXIgb3IgbWVzc2FnZS5zZW5kZXJf"
    "Y2hhdCkuaWQKICAgIHVzZWQgPSBhd2FpdCBnZXRfdXNhZ2UodWlkKQogICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1aWQp"
    "CiAgICByZXNlcnZlZCA9IGF3YWl0IHJlc2VydmVkX3RvZGF5KHVpZCkKICAgIGhlbGQgPSBmIiA8aT4oK3tfcihyZXNlcnZlZCl9"
    "IGluIHJ1bm5pbmcgdGFza3MpPC9pPiIgaWYgcmVzZXJ2ZWQgZWxzZSAiIgogICAgbXNnID0gKAogICAgICAgIGYi8J+TiiA8Yj5C"
    "YW5kd2lkdGgg4oCUIHtfZm10X2RheSgpfSAoSVNUKTwvYj5cbuKUglxuIgogICAgICAgIGYi4pSgIDxiPllvdTwvYj4g4oaSIHtf"
    "cih1c2VkKX17aGVsZH0gLyB7X3IoY2FwKX1cbiIKICAgICAgICBmIuKUliBSZXNldHMgYXQgMDA6MDAgSVNUIgogICAgKQogICAg"
    "aWYgX2lzX293bmVyKG1lc3NhZ2UpOgogICAgICAgIHJvd3MgPSBhd2FpdCB0b2RheV9yb3dzKCkKICAgICAgICBpZiByb3dzOgog"
    "ICAgICAgICAgICB1ZG9jcyA9IGF3YWl0IGFsbF91c2VycygpCiAgICAgICAgICAgIHVuYW1lcyA9IHt1WyJfaWQiXTogdS5nZXQo"
    "InVuYW1lIikgb3IgIiIgZm9yIHUgaW4gdWRvY3N9CiAgICAgICAgICAgIG1zZyArPSAiXG7ilINcbvCfkaUgPGI+RXZlcnlvbmUg"
    "dG9kYXk6PC9iPiIKICAgICAgICAgICAgZm9yIHJvdyBpbiByb3dzWzoxNV06CiAgICAgICAgICAgICAgICBpZiByb3dbInVzZXJf"
    "aWQiXSA9PSB1aWQ6CiAgICAgICAgICAgICAgICAgICAgY29udGludWUKICAgICAgICAgICAgICAgIHVuID0gdW5hbWVzLmdldChy"
    "b3dbInVzZXJfaWQiXSwgIiIpCiAgICAgICAgICAgICAgICB3aG8gPSBmIkB7ZXNjYXBlKHVuKX0iIGlmIHVuIGVsc2UgZiI8Y29k"
    "ZT57cm93Wyd1c2VyX2lkJ119PC9jb2RlPiIKICAgICAgICAgICAgICAgIG1zZyArPSBmIlxu4pSgIHt3aG99IOKGkiB7X3Iocm93"
    "Wyd1c2VkJ10pfSIKICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBtc2cpCgoKIyDilIDilIAgL3F1c2VycyDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgd3pmaXhfcXVzZXJzKGNsaWVudCwgbWVzc2FnZSk6"
    "CiAgICBpZiBub3QgYXdhaXQgX2RiX29rKCk6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIF9EQl9ET1dOKQog"
    "ICAgICAgIHJldHVybgogICAgdXNlcnMgPSBhd2FpdCBhbGxfdXNlcnMoKQogICAgaWYgbm90IHVzZXJzOgogICAgICAgIGF3YWl0"
    "IHNlbmRfbWVzc2FnZShtZXNzYWdlLCAiTm8gdXNlcnMgcmVjb3JkZWQgeWV0IOKAlCB0aGV5IGFwcGVhciBhZnRlciB0aGVpciBm"
    "aXJzdCB0YXNrLiIpCiAgICAgICAgcmV0dXJuCiAgICByb3dzID0gYXdhaXQgdG9kYXlfcm93cygpCiAgICB0b2RheSA9IHtyWyJ1"
    "c2VyX2lkIl06IHJbInVzZWQiXSBmb3IgciBpbiByb3dzfQogICAgbXNnID0gZiLwn5GlIDxiPlVzZXIgcmVnaXN0cnk8L2I+IOKA"
    "lCB7X2ZtdF9kYXkoKX0gKElTVClcbuKUgiIKICAgIGZvciB1IGluIHVzZXJzWzoyNV06CiAgICAgICAgdWlkID0gdVsiX2lkIl0K"
    "ICAgICAgICB1biA9IHUuZ2V0KCJ1bmFtZSIpIG9yICIiCiAgICAgICAgd2hvID0gZiJAe2VzY2FwZSh1bil9IiBpZiB1biBlbHNl"
    "IGVzY2FwZSh1LmdldCgibmFtZSIpIG9yIHN0cih1aWQpKQogICAgICAgIGNhcCA9IHUuZ2V0KCJjYXBfZ2IiKQogICAgICAgIGNh"
    "cF9zID0gZiJ7Y2FwOmd9IEdCIiBpZiBjYXAgaXMgbm90IE5vbmUgZWxzZSAiZGVmYXVsdCIKICAgICAgICBtc2cgKz0gKAogICAg"
    "ICAgICAgICBmIlxu4pSgIDxiPntlc2NhcGUod2hvKX08L2I+XG4iCiAgICAgICAgICAgIGYi4pSDICAgSUQgPGNvZGU+e3VpZH08"
    "L2NvZGU+IMK3IHRvZGF5IHtfcih0b2RheS5nZXQodWlkLCAwKSl9IMK3ICIKICAgICAgICAgICAgZiJsaWZldGltZSB7X3IodS5n"
    "ZXQoJ3RvdGFsX3VzZWQnKSBvciAwKX0gwrcgY2FwIHtjYXBfc30gwrcgIgogICAgICAgICAgICBmInRhc2tzIHt1LmdldCgndGFz"
    "a3MnKSBvciAwfSIKICAgICAgICApCiAgICBpZiBsZW4odXNlcnMpID4gMjU6CiAgICAgICAgbXNnICs9IGYiXG7ilIMg4oCmYW5k"
    "IHtsZW4odXNlcnMpIC0gMjV9IG1vcmUiCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgbXNnKQoKCiMg4pSA4pSAIGNh"
    "cCBjb21tYW5kcyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgd3pmaXhfc2V0Y2FwKGNsaWVudCwgbWVzc2Fn"
    "ZSk6CiAgICBpZiBub3QgYXdhaXQgX2RiX29rKCk6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIF9EQl9ET1dO"
    "KQogICAgICAgIHJldHVybgogICAgYXJncyA9IChtZXNzYWdlLnRleHQgb3IgIiIpLnNwbGl0KClbMTpdCiAgICB1aWQsIGdiID0g"
    "X3BhcnNlX3RhcmdldF9hbmRfZ2IobWVzc2FnZSwgYXJncykKICAgIGlmIG5vdCB1aWQ6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNz"
    "YWdlKG1lc3NhZ2UsICJVc2FnZTogPGNvZGU+L3NldGNhcCA8dXNlcl9pZD4gPEdCPjwvY29kZT4gKG9yIHJlcGx5IHRvIGEgdXNl"
    "cikuIDAgPSBiYWNrIHRvIGdsb2JhbCBkZWZhdWx0LiIpCiAgICAgICAgcmV0dXJuCiAgICBpZiBhd2FpdCBfYmFkX3RhcmdldCht"
    "ZXNzYWdlLCB1aWQpOgogICAgICAgIHJldHVybgogICAgaWYgZ2IgaXMgTm9uZToKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2Uo"
    "bWVzc2FnZSwgIlVzYWdlOiA8Y29kZT4vc2V0Y2FwIDx1c2VyX2lkPiA8R0I+PC9jb2RlPiDigJQgZ2l2ZSBhIG51bWJlciBpbiBH"
    "Qi4iKQogICAgICAgIHJldHVybgogICAgYXdhaXQgc2V0X3VzZXJfY2FwKHVpZCwgZ2IgaWYgZ2IgPiAwIGVsc2UgTm9uZSkKICAg"
    "IGxhYmVsID0gX2ZtdF9nYihnYikgaWYgZ2IgYW5kIGdiID4gMCBlbHNlICJnbG9iYWwgZGVmYXVsdCIKICAgIGF3YWl0IHNlbmRf"
    "bWVzc2FnZShtZXNzYWdlLCBmIuKchSBDYXAgZm9yIDxjb2RlPnt1aWR9PC9jb2RlPiBzZXQgdG8gPGI+e2xhYmVsfTwvYj4uIikK"
    "Cgphc3luYyBkZWYgd3pmaXhfYWRkY2FwKGNsaWVudCwgbWVzc2FnZSk6CiAgICBpZiBub3QgYXdhaXQgX2RiX29rKCk6CiAgICAg"
    "ICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIF9EQl9ET1dOKQogICAgICAgIHJldHVybgogICAgYXJncyA9IChtZXNzYWdl"
    "LnRleHQgb3IgIiIpLnNwbGl0KClbMTpdCiAgICB1aWQsIGdiID0gX3BhcnNlX3RhcmdldF9hbmRfZ2IobWVzc2FnZSwgYXJncykK"
    "ICAgIGlmIG5vdCB1aWQ6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsICJVc2FnZTogPGNvZGU+L2FkZGNhcCA8"
    "dXNlcl9pZD4gPEdCPjwvY29kZT4gKG9yIHJlcGx5IHRvIGEgdXNlcikuIikKICAgICAgICByZXR1cm4KICAgIGlmIGF3YWl0IF9i"
    "YWRfdGFyZ2V0KG1lc3NhZ2UsIHVpZCk6CiAgICAgICAgcmV0dXJuCiAgICBpZiBnYiBpcyBOb25lIG9yIGdiIDw9IDA6CiAgICAg"
    "ICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsICJVc2FnZTogPGNvZGU+L2FkZGNhcCA8dXNlcl9pZD4gPEdCPjwvY29kZT4g"
    "4oCUIGhvdyBtYW55IEdCIHRvIGFkZC4iKQogICAgICAgIHJldHVybgogICAgdWRvYyA9IGF3YWl0IGdldF91c2VyX2RvY19sb2Nh"
    "bCh1aWQpCiAgICBjdXIgPSB1ZG9jLmdldCgiY2FwX2diIikKICAgIGZyb20gLi5oZWxwZXIud3pmaXgucjFfY29yZSBpbXBvcnQg"
    "X2dldF9nbG9iYWxfY2FwX2diCgogICAgYmFzZSA9IGZsb2F0KGN1cikgaWYgY3VyIGlzIG5vdCBOb25lIGVsc2UgYXdhaXQgX2dl"
    "dF9nbG9iYWxfY2FwX2diKCkKICAgIGF3YWl0IHNldF91c2VyX2NhcCh1aWQsIGJhc2UgKyBnYikKICAgIGF3YWl0IHNlbmRfbWVz"
    "c2FnZSgKICAgICAgICBtZXNzYWdlLAogICAgICAgIGYi4pyFIENhcCBmb3IgPGNvZGU+e3VpZH08L2NvZGU+IHJhaXNlZCB0byA8"
    "Yj57X2ZtdF9nYihiYXNlICsgZ2IpfTwvYj4vZGF5LiIsCiAgICApCgoKYXN5bmMgZGVmIHd6Zml4X2RlZHVjdGNhcChjbGllbnQs"
    "IG1lc3NhZ2UpOgogICAgIiIiT3Bwb3NpdGUgb2YgL2FkZGNhcDogbG93ZXIgYSB1c2VyJ3MgY2FwIGJ5IEdCIChmbG9vciAwID0g"
    "YmxvY2tlZCkuIiIiCiAgICBpZiBub3QgYXdhaXQgX2RiX29rKCk6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2Us"
    "IF9EQl9ET1dOKQogICAgICAgIHJldHVybgogICAgYXJncyA9IChtZXNzYWdlLnRleHQgb3IgIiIpLnNwbGl0KClbMTpdCiAgICB1"
    "aWQsIGdiID0gX3BhcnNlX3RhcmdldF9hbmRfZ2IobWVzc2FnZSwgYXJncykKICAgIGlmIG5vdCB1aWQ6CiAgICAgICAgYXdhaXQg"
    "c2VuZF9tZXNzYWdlKG1lc3NhZ2UsICJVc2FnZTogPGNvZGU+L2RlZHVjdGNhcCA8dXNlcl9pZD4gPEdCPjwvY29kZT4gKG9yIHJl"
    "cGx5IHRvIGEgdXNlcikuIikKICAgICAgICByZXR1cm4KICAgIGlmIGF3YWl0IF9iYWRfdGFyZ2V0KG1lc3NhZ2UsIHVpZCk6CiAg"
    "ICAgICAgcmV0dXJuCiAgICBpZiBnYiBpcyBOb25lIG9yIGdiIDw9IDA6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3Nh"
    "Z2UsICJVc2FnZTogPGNvZGU+L2RlZHVjdGNhcCA8dXNlcl9pZD4gPEdCPjwvY29kZT4g4oCUIGhvdyBtYW55IEdCIHRvIHN1YnRy"
    "YWN0LiIpCiAgICAgICAgcmV0dXJuCiAgICB1ZG9jID0gYXdhaXQgZ2V0X3VzZXJfZG9jX2xvY2FsKHVpZCkKICAgIGN1ciA9IHVk"
    "b2MuZ2V0KCJjYXBfZ2IiKQogICAgZnJvbSAuLmhlbHBlci53emZpeC5yMV9jb3JlIGltcG9ydCBfZ2V0X2dsb2JhbF9jYXBfZ2IK"
    "CiAgICBiYXNlID0gZmxvYXQoY3VyKSBpZiBjdXIgaXMgbm90IE5vbmUgZWxzZSBhd2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKQog"
    "ICAgbmV3X2NhcCA9IG1heChiYXNlIC0gZ2IsIDApCiAgICBhd2FpdCBzZXRfdXNlcl9jYXAodWlkLCBuZXdfY2FwKQogICAgaWYg"
    "bmV3X2NhcCA8PSAwOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZSgKICAgICAgICAgICAgbWVzc2FnZSwKICAgICAgICAgICAg"
    "ZiLim5QgQ2FwIGZvciA8Y29kZT57dWlkfTwvY29kZT4gaXMgbm93IDxiPjAgR0I8L2I+IOKAlCBmdWxseSBibG9ja2VkLiAiCiAg"
    "ICAgICAgICAgIGYiVXNlIDxjb2RlPi9zZXRjYXAge3VpZH0gMDwvY29kZT4gdG8gcmV0dXJuIHRoZW0gdG8gdGhlIGdsb2JhbCBk"
    "ZWZhdWx0LiIsCiAgICAgICAgKQogICAgZWxzZToKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgIG1lc3Nh"
    "Z2UsCiAgICAgICAgICAgIGYi4pyFIENhcCBmb3IgPGNvZGU+e3VpZH08L2NvZGU+IGxvd2VyZWQgdG8gPGI+e19mbXRfZ2IobmV3"
    "X2NhcCl9PC9iPi9kYXkuIiwKICAgICAgICApCgoKYXN5bmMgZGVmIHd6Zml4X2RlbHVzZXIoY2xpZW50LCBtZXNzYWdlKToKICAg"
    "ICIiIlBlcm1hbmVudGx5IHJlbW92ZSBhIHVzZXIgZnJvbSB0aGUgcmVnaXN0cnkgKCsgdGhlaXIgdXNhZ2UgaGlzdG9yeSkuIiIi"
    "CiAgICBpZiBub3QgYXdhaXQgX2RiX29rKCk6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIF9EQl9ET1dOKQog"
    "ICAgICAgIHJldHVybgogICAgYXJncyA9IChtZXNzYWdlLnRleHQgb3IgIiIpLnNwbGl0KClbMTpdCiAgICB1aWQgPSBfdGFyZ2V0"
    "X3VzZXIobWVzc2FnZSwgYXJncykKICAgIGlmIG5vdCB1aWQ6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsICJV"
    "c2FnZTogPGNvZGU+L2RlbHVzZXIgPHVzZXJfaWQ+PC9jb2RlPiAob3IgcmVwbHkgdG8gYSB1c2VyKS4iKQogICAgICAgIHJldHVy"
    "bgogICAgcmVnLCByb3dzID0gYXdhaXQgZGVsZXRlX3VzZXIodWlkKQogICAgaWYgbm90IHJlZyBhbmQgbm90IHJvd3M6CiAgICAg"
    "ICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIGYi8J+ktyBVc2VyIDxjb2RlPnt1aWR9PC9jb2RlPiB3YXNuJ3QgaW4gdGhl"
    "IHJlZ2lzdHJ5LiIpCiAgICAgICAgcmV0dXJuCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgbWVzc2FnZSwKICAgICAg"
    "ICBmIvCfl5EgUmVtb3ZlZCB1c2VyIDxjb2RlPnt1aWR9PC9jb2RlPiDigJQgcmVnaXN0cnkgZW50cnkgZGVsZXRlZCwgIgogICAg"
    "ICAgIGYie3Jvd3N9IHVzYWdlIHJvdyhzKSBjbGVhcmVkLiBUaGVpciAvZmluZCBkb3dubG9hZCBoaXN0b3J5IGlzIGtlcHQgIgog"
    "ICAgICAgIGYiKGV4cGlyZXMgbmF0dXJhbGx5IGFmdGVyIDkwIGRheXMpLiIsCiAgICApCgoKYXN5bmMgZGVmIGdldF91c2VyX2Rv"
    "Y19sb2NhbCh1aWQpOgogICAgZnJvbSAuLmhlbHBlci53emZpeC5yMV9jb3JlIGltcG9ydCBnZXRfdXNlcl9kb2MKCiAgICByZXR1"
    "cm4gYXdhaXQgZ2V0X3VzZXJfZG9jKHVpZCkKCgphc3luYyBkZWYgd3pmaXhfcmVzZXRjYXAoY2xpZW50LCBtZXNzYWdlKToKICAg"
    "IGlmIG5vdCBhd2FpdCBfZGJfb2soKToKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgX0RCX0RPV04pCiAgICAg"
    "ICAgcmV0dXJuCiAgICBhcmdzID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3BsaXQoKVsxOl0KICAgIHVpZCA9IF90YXJnZXRfdXNl"
    "cihtZXNzYWdlLCBhcmdzKQogICAgaWYgbm90IHVpZDoKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgIlVzYWdl"
    "OiA8Y29kZT4vcmVzZXRjYXAgPHVzZXJfaWQ+PC9jb2RlPiAob3IgcmVwbHkgdG8gYSB1c2VyKS4iKQogICAgICAgIHJldHVybgog"
    "ICAgYXdhaXQgcmVzZXRfdXNhZ2UodWlkKQogICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIGYi4pm777iPIFRvZGF5J3Mg"
    "dXNhZ2UgZm9yIDxjb2RlPnt1aWR9PC9jb2RlPiByZXNldCB0byAwLiIpCgoKYXN5bmMgZGVmIHd6Zml4X2JvdGNhcChjbGllbnQs"
    "IG1lc3NhZ2UpOgogICAgaWYgbm90IGF3YWl0IF9kYl9vaygpOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBf"
    "REJfRE9XTikKICAgICAgICByZXR1cm4KICAgIGFyZ3MgPSAobWVzc2FnZS50ZXh0IG9yICIiKS5zcGxpdCgpWzE6XQogICAgaWYg"
    "bm90IGFyZ3Mgb3Igbm90IGFyZ3NbMF0ucmVwbGFjZSgiLiIsICIiLCAxKS5pc2RpZ2l0KCk6CiAgICAgICAgYXdhaXQgc2VuZF9t"
    "ZXNzYWdlKG1lc3NhZ2UsICJVc2FnZTogPGNvZGU+L2JvdGNhcCA8R0I+PC9jb2RlPiDigJQgZ2xvYmFsIGRlZmF1bHQgZGFpbHkg"
    "Y2FwIGZvciBldmVyeSB1c2VyLiIpCiAgICAgICAgcmV0dXJuCiAgICBnYiA9IGZsb2F0KGFyZ3NbMF0pCiAgICBpZiBnYiA8PSAw"
    "OgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCAiQ2FwIG11c3QgYmUgPiAwIEdCLiIpCiAgICAgICAgcmV0dXJu"
    "CiAgICBhd2FpdCBzZXRfZ2xvYmFsX2NhcF9nYihnYikKICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBmIuKchSBHbG9i"
    "YWwgZGFpbHkgY2FwIHNldCB0byA8Yj57Z2I6Z30gR0I8L2I+IChvd25lciAmIHN1ZG8gc3RheSB1bmxpbWl0ZWQpLiIpCgoKYXN5"
    "bmMgZGVmIHd6Zml4X2RpYWcoY2xpZW50LCBtZXNzYWdlKToKICAgICIiIk93bmVyLW9ubHkgbGl2ZSBkdW1wIG9mIGV2ZXJ5dGhp"
    "bmcgcXVvdGEtcmVsYXRlZCBmb3Igb25lIHVzZXIuIiIiCiAgICBhcmdzID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3BsaXQoKVsx"
    "Ol0KICAgIHVpZCA9IF90YXJnZXRfdXNlcihtZXNzYWdlLCBhcmdzKQogICAgaWYgbm90IHVpZDoKICAgICAgICBhd2FpdCBzZW5k"
    "X21lc3NhZ2UoCiAgICAgICAgICAgIG1lc3NhZ2UsCiAgICAgICAgICAgICJVc2FnZTogPGNvZGU+L3d6Zml4ZGlhZyA8dXNlcl9p"
    "ZD48L2NvZGU+IOKAlCBzaG93cyBwYXJ0aXRpb24sIGNhcCwgIgogICAgICAgICAgICAidG9kYXkncyB1c2FnZSBkb2MgYW5kIGEg"
    "c2ltdWxhdGVkIDIgR0IgdmVyZGljdC4iLAogICAgICAgICkKICAgICAgICByZXR1cm4KICAgIGZyb20gLi5oZWxwZXIud3pmaXgu"
    "cjFfY29yZSBpbXBvcnQgZGlhZwoKICAgIGQgPSBhd2FpdCBkaWFnKHVpZCwgcHJvYmVfZ2I9MikKICAgIHFkID0gZC5nZXQoInF1"
    "b3RhX2RvYyIpIG9yIHt9CiAgICB1ZG9jID0gZC5nZXQoInVzZXJfZG9jIikgb3Ige30KICAgIHByb2JlID0gZC5nZXQoInByb2Jl"
    "Iikgb3Ige30KICAgIGNhcF9jdXN0b20gPSB1ZG9jLmdldCgiY2FwX2diIikKICAgIGNhcF9zcmMgPSAoCiAgICAgICAgZiJjdXN0"
    "b20ge19mbXRfZ2IoY2FwX2N1c3RvbSl9IgogICAgICAgIGlmIGNhcF9jdXN0b20gaXMgbm90IE5vbmUKICAgICAgICBlbHNlIGYi"
    "Z2xvYmFsIGRlZmF1bHQge19mbXRfZ2IoZC5nZXQoJ2dsb2JhbF9jYXBfZ2InKSBvciAwKX0iCiAgICApCiAgICBwcm9iZV90eHQg"
    "PSAoCiAgICAgICAgIndvdWxkIDxiPkJMT0NLPC9iPiBhIDIgR0IgdGFzayIgaWYgcHJvYmUuZ2V0KCJ3b3VsZF9ibG9jayIpCiAg"
    "ICAgICAgZWxzZSAid291bGQgPGI+QUxMT1c8L2I+IGEgMiBHQiB0YXNrIgogICAgKQogICAgbXNnID0gKAogICAgICAgIGYi8J+U"
    "pyA8Yj5XWkZJWCBkaWFnPC9iPiDigJQgdXNlciA8Y29kZT57dWlkfTwvY29kZT5cbiIKICAgICAgICBmIuKUglxuIgogICAgICAg"
    "IGYi4pSgIDxiPkRCIHBhcnRpdGlvbjwvYj4g4oaSIDxjb2RlPntkLmdldCgncGFydGl0aW9uJyl9PC9jb2RlPlxuIgogICAgICAg"
    "IGYi4pSgIDxiPkRheSAoSVNUKTwvYj4g4oaSIHtkLmdldCgnZGF5Jyl9XG4iCiAgICAgICAgZiLilKAgPGI+REIgcmVhZHk8L2I+"
    "IOKGkiB7ZC5nZXQoJ2RiX3JlYWR5Jyl9XG4iCiAgICAgICAgZiLilKAgPGI+Q2FwPC9iPiDihpIge19yKGQuZ2V0KCdjYXBfYnl0"
    "ZXMnKSBvciAwKX0gKHtjYXBfc3JjfSlcbiIKICAgICAgICBmIuKUoCA8Yj5SZXNlcnZlZCAocnVubmluZyB0YXNrcyk8L2I+IOKG"
    "kiB7X3IoZC5nZXQoJ3Jlc2VydmVkJykgb3IgMCl9XG4iCiAgICAgICAgZiLilKAgPGI+UXVvdGEgZG9jPC9iPiDihpIgPGNvZGU+"
    "e2VzY2FwZShzdHIocWQpKVs6MzAwXX08L2NvZGU+XG4iCiAgICAgICAgZiLilKAgPGI+U2ltdWxhdGlvbjwvYj4g4oaSIHtwcm9i"
    "ZV90eHR9ICIKICAgICAgICBmIih1c2VkIHtfcihwcm9iZS5nZXQoJ3VzZWQnKSBvciAwKX0gLyBjYXAge19yKHByb2JlLmdldCgn"
    "Y2FwJykgb3IgMCl9KVxuIgogICAgICAgIGYi4pSgIDxiPlVzZXIgZG9jPC9iPiDihpIgPGNvZGU+e2VzY2FwZShzdHIodWRvYykp"
    "WzozMDBdfTwvY29kZT4iCiAgICApCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgbXNnKQoKCiMg4pSA4pSAIGRiIHRv"
    "b2xzIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiB3emZpeF9kYnN0YXRzKGNsaWVudCwg"
    "bWVzc2FnZSk6CiAgICBzdCA9IGF3YWl0IGRic3RhdHMoKQogICAgaWYgc3QuZ2V0KCJlcnJvciIpIGFuZCBub3Qgc3QuZ2V0KCJj"
    "b2xzIik6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIGYiREIgc3RhdHMgZmFpbGVkOiA8Y29kZT57ZXNjYXBl"
    "KHN0WydlcnJvciddKX08L2NvZGU+IikKICAgICAgICByZXR1cm4KICAgIGlmIHN0LmdldCgiYWNjb3VudF90b3RhbCIpOgogICAg"
    "ICAgIHBjdCA9IChzdFsiYWNjb3VudF90b3RhbCJdIC8gKDUxMiAqIDEwMjQqKjIpKSAqIDEwMAogICAgICAgIG1zZyA9ICgKICAg"
    "ICAgICAgICAgZiLwn5eEIDxiPk1vbmdvREIgdXNhZ2U8L2I+IOKAlCB7X2ZtdF9kYXkoKX1cbuKUglxuIgogICAgICAgICAgICBm"
    "IuKUoCA8Yj5BY2NvdW50IHRvdGFsIChhbGwgZGF0YWJhc2VzKTwvYj4g4oaSIHtfcihzdFsnYWNjb3VudF90b3RhbCddKX0gIgog"
    "ICAgICAgICAgICBmIih7cGN0Oi4xZn0lIG9mIHRoZSA1MTIgTUIgZnJlZSB0aWVyKVxuIgogICAgICAgICAgICBmIuKUoCA8Yj5U"
    "aGlzIGJvdCdzIGRhdGFiYXNlICh3em1seCk8L2I+IOKGkiB7X3Ioc3RbJ3RvdGFsJ10pfVxuIgogICAgICAgICAgICArICgi4pSg"
    "IOKaoO+4jyA8Yj5BY2NvdW50IGFib3ZlIDgwJSE8L2I+XG4iIGlmIHBjdCA+IDgwIGVsc2UgIiIpCiAgICAgICAgICAgICsgIuKU"
    "g1xuPGI+RGF0YWJhc2VzIGJ5IHNpemU8L2I+ICh0aGUgNTEyIE1CIGNvdmVycyBBTEwgb2YgdGhlbSk6IgogICAgICAgICkKICAg"
    "ICAgICBmb3IgbmFtZSwgc3ogaW4gc3RbImRicyJdWzoxMF06CiAgICAgICAgICAgIHRhZyA9ICIg4oaQIHRoaXMgYm90J3MiIGlm"
    "IG5hbWUgPT0gInd6bWx4IiBlbHNlICIiCiAgICAgICAgICAgIG1zZyArPSBmIlxu4pSgIDxjb2RlPntlc2NhcGUobmFtZSl9PC9j"
    "b2RlPiDihpIge19yKHN6KX17dGFnfSIKICAgICAgICBtc2cgKz0gIlxu4pSDXG48Yj53em1seCBjb2xsZWN0aW9uczwvYj4gKHNl"
    "bGYgPSB0aGlzIGJvdCwgb3RoZXIgPSBvdGhlciBib3RzKToiCiAgICBlbHNlOgogICAgICAgIHBjdCA9IChzdFsidG90YWwiXSAv"
    "ICg1MTIgKiAxMDI0KioyKSkgKiAxMDAgaWYgc3RbInRvdGFsIl0gZWxzZSAwCiAgICAgICAgbXNnID0gKAogICAgICAgICAgICBm"
    "IvCfl4QgPGI+TW9uZ29EQiB1c2FnZTwvYj4g4oCUIHtfZm10X2RheSgpfVxu4pSCXG4iCiAgICAgICAgICAgIGYi4pSgIDxiPlRv"
    "dGFsPC9iPiDihpIge19yKHN0Wyd0b3RhbCddKX0gKHtwY3Q6LjFmfSUgb2YgdGhlIDUxMiBNQiBmcmVlIHRpZXIpXG4iCiAgICAg"
    "ICAgICAgICIoYWNjb3VudC13aWRlIHZpZXcgdW5hdmFpbGFibGUiCiAgICAgICAgICAgICsgKGYiOiB7ZXNjYXBlKHN0WydkYl9l"
    "cnJvciddWzo4MF0pfSIgaWYgc3QuZ2V0KCJkYl9lcnJvciIpIGVsc2UgIiIpCiAgICAgICAgICAgICsgIilcbiIKICAgICAgICAg"
    "ICAgKyAoIuKUoCDimqDvuI8gPGI+QWJvdmUgODAlIOKAlCBydW4gL2RiY2xlYW48L2I+XG4iIGlmIHBjdCA+IDgwIGVsc2UgIiIp"
    "CiAgICAgICAgICAgICsgIuKUg1xuPGI+VG9wIGNvbGxlY3Rpb25zPC9iPiAoc2VsZiA9IHRoaXMgYm90LCBvdGhlciA9IGJvdHMg"
    "c2hhcmluZyB0aGUgYWNjb3VudCk6IgogICAgICAgICkKICAgIGZvciBuYW1lLCBhIGluIHN0WyJjb2xzIl1bOjEyXToKICAgICAg"
    "ICBpZiBhWyJzaXplIl0gPD0gMDoKICAgICAgICAgICAgY29udGludWUKICAgICAgICBtc2cgKz0gZiJcbuKUoCA8Y29kZT57ZXNj"
    "YXBlKG5hbWUpfTwvY29kZT4g4oaSIHtfcihhWydzaXplJ10pfSDCtyB7YVsnZG9jcyddfSBkb2NzIgogICAgICAgIGJpdHMgPSBb"
    "XQogICAgICAgIGlmIGEuZ2V0KCJzZWxmIik6CiAgICAgICAgICAgIGJpdHMuYXBwZW5kKGYidGhpcyBib3Qge19yKGFbJ3NlbGYn"
    "XSl9IikKICAgICAgICBpZiBhLmdldCgib3RoZXIiKToKICAgICAgICAgICAgYml0cy5hcHBlbmQoZiJvdGhlciBib3RzIHtfcihh"
    "WydvdGhlciddKX0iKQogICAgICAgIGlmIGEuZ2V0KCJzaGFyZWQiKToKICAgICAgICAgICAgYml0cy5hcHBlbmQoZiJzaGFyZWQg"
    "e19yKGFbJ3NoYXJlZCddKX0iKQogICAgICAgIGlmIGJpdHM6CiAgICAgICAgICAgIG1zZyArPSAiIMK3ICIgKyAiIMK3ICIuam9p"
    "bihiaXRzKQogICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIG1zZykKCgphc3luYyBkZWYgd3pmaXhfZGJjbGVhbihjbGll"
    "bnQsIG1lc3NhZ2UpOgogICAgYnRuID0gQnV0dG9uTWFrZXIoKQogICAgYnRuLmRhdGFfYnV0dG9uKCLwn6e5IFllcywgY2xlYW4i"
    "LCAid3pmaXhjbGVhbiIpCiAgICBidG4uZGF0YV9idXR0b24oIuKcliBDYW5jZWwiLCAid3pmaXhjYW5jZWwiKQogICAgc3QgPSBh"
    "d2FpdCBkYnN0YXRzKCkKICAgIGF3YWl0IHNlbmRfbWVzc2FnZSgKICAgICAgICBtZXNzYWdlLAogICAgICAgICLwn6e5IDxiPkRC"
    "IGNsZWFudXA8L2I+IOKAlCB3aWxsIHB1cmdlOlxuIgogICAgICAgICLilKAgZXhwaXJlZCBzdHJlYW0gdG9rZW5zXG4iCiAgICAg"
    "ICAgIuKUoCBpbmNvbXBsZXRlLXRhc2sgcmVzdW1lIHJlY29yZHNcbiIKICAgICAgICAi4pSgIHN0YWxlIHd6Zml4IHJvd3NcbiIK"
    "ICAgICAgICBmIuKUg1xu4pSWIEN1cnJlbnQgdXNhZ2U6IDxiPntfcihzdFsndG90YWwnXSl9PC9iPlxuIgogICAgICAgICJQcm9j"
    "ZWVkPyIsCiAgICAgICAgYnRuLmJ1aWxkX21lbnUoMiksCiAgICApCgoKYXN5bmMgZGVmIHd6Zml4X2NsZWFuX2NiKGNsaWVudCwg"
    "cXVlcnkpOgogICAgaWYgbm90IF9pc19zdWRvX29yX293bmVyKHF1ZXJ5LmZyb21fdXNlci5pZCk6CiAgICAgICAgYXdhaXQgcXVl"
    "cnkuYW5zd2VyKCJPd25lciAvIHN1ZG8gb25seS4iLCBzaG93X2FsZXJ0PVRydWUpCiAgICAgICAgcmV0dXJuCiAgICBpZiBub3Qg"
    "YXdhaXQgX2RiX29rKCk6CiAgICAgICAgYXdhaXQgcXVlcnkuYW5zd2VyKCJEYXRhYmFzZSBub3QgY29ubmVjdGVkLiIsIHNob3df"
    "YWxlcnQ9VHJ1ZSkKICAgICAgICByZXR1cm4KICAgIGZyZWVkID0gYXdhaXQgZGJjbGVhbigpCiAgICBib2R5ID0gIlxuIi5qb2lu"
    "KGYi4pSgIHtrfSDihpIge3Z9IiBmb3IgaywgdiBpbiBmcmVlZC5pdGVtcygpKSBvciAi4pSgIG5vdGhpbmcgdG8gcHVyZ2UiCiAg"
    "ICB0cnk6CiAgICAgICAgYXdhaXQgcXVlcnkuYW5zd2VyKCkKICAgICAgICBhd2FpdCBxdWVyeS5lZGl0X21lc3NhZ2VfdGV4dCgK"
    "ICAgICAgICAgICAgZiLwn6e5IDxiPkRCIGNsZWFudXAgZG9uZTwvYj5cbuKUglxue2JvZHl9XG7ilINcbuKUliBSdW4gL2Ric3Rh"
    "dHMgdG8gc2VlIHRoZSBuZXcgc2l6ZS4iCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKZGVm"
    "IF9pc19zdWRvX29yX293bmVyKHVpZCk6CiAgICBpZiB1aWQgPT0gQ29uZmlnLk9XTkVSX0lEOgogICAgICAgIHJldHVybiBUcnVl"
    "CiAgICByZXR1cm4gYm9vbCh1c2VyX2RhdGEuZ2V0KHVpZCwge30pLmdldCgiU1VETyIpKQoKCmFzeW5jIGRlZiB3emZpeF9jYW5j"
    "ZWxfY2IoY2xpZW50LCBxdWVyeSk6CiAgICB0cnk6CiAgICAgICAgYXdhaXQgcXVlcnkuYW5zd2VyKCkKICAgICAgICBhd2FpdCBx"
    "dWVyeS5lZGl0X21lc3NhZ2VfdGV4dCgi4pyWIENsZWFudXAgY2FuY2VsbGVkLiIpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAg"
    "ICAgIHBhc3MKCgphc3luYyBkZWYgX2Rhc2hfdXJsKCk6CiAgICAiIiJUaGUgZGFzaGJvYXJkIFVSTCBidWlsdCBmcm9tIHRoZSBs"
    "aXZlIEJBU0VfVVJMICh3b3JrZXIvdHVubmVsKS4iIiIKICAgIHRyeToKICAgICAgICBiID0gKGdldGF0dHIoQ29uZmlnLCAiQkFT"
    "RV9VUkwiLCAiIikgb3IgIiIpLnN0cmlwKCkucnN0cmlwKCIvIikKICAgICAgICBpZiBiOgogICAgICAgICAgICByZXR1cm4gZiJ7"
    "Yn0vd3phZG1pbiIKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuICJ5b3VyIHdvcmtlciBsaW5r"
    "ICsgL3d6YWRtaW4iCgoKIyDilIDilIAgL2FkbWlucGFzcyDigJQgd2ViIGRhc2hib2FyZCBwYXNzd29yZCAodjE1LjgpIOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoK"
    "CmFzeW5jIGRlZiB3emZpeF9hZG1pbnBhc3MoY2xpZW50LCBtZXNzYWdlKToKICAgICIiIlNob3cgb3Igc2V0IHRoZSB3ZWIgZGFz"
    "aGJvYXJkIHBhc3N3b3JkIChvd25lci9zdWRvIG9ubHkpLiIiIgogICAgaWYgbm90IGF3YWl0IF9kYl9vaygpOgogICAgICAgIGF3"
    "YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikKICAgICAgICByZXR1cm4KICAgIGFyZ3MgPSAobWVzc2FnZS50ZXh0"
    "IG9yICIiKS5zcGxpdCgpWzE6XQogICAgaWYgYXJnczoKICAgICAgICBuZXcgPSBhcmdzWzBdLnN0cmlwKCkKICAgICAgICBpZiBs"
    "ZW4obmV3KSA8IDQ6CiAgICAgICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZSgKICAgICAgICAgICAgICAgIG1lc3NhZ2UsCiAgICAg"
    "ICAgICAgICAgICAiUGljayBhdCBsZWFzdCA0IGNoYXJhY3RlcnM6IDxjb2RlPi9hZG1pbnBhc3MgbXlzZWNyZXQ8L2NvZGU+IiwK"
    "ICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4KICAgICAgICBhd2FpdCBzZXRfYWRtaW5fcGFzcyhuZXcpCiAgICAgICAg"
    "YXdhaXQgc2VuZF9tZXNzYWdlKAogICAgICAgICAgICBtZXNzYWdlLAogICAgICAgICAgICAi4pyFIDxiPkRhc2hib2FyZCBwYXNz"
    "d29yZCB1cGRhdGVkLjwvYj4gT3BlbiB5b3VyIHdvcmtlciBsaW5rICsgIgogICAgICAgICAgICAiPGNvZGU+L3d6YWRtaW48L2Nv"
    "ZGU+IGFuZCB1c2UgdGhlIG5ldyBwYXNzd29yZC4iLAogICAgICAgICkKICAgICAgICByZXR1cm4KICAgIHB3ID0gYXdhaXQgZ2V0"
    "X2FkbWluX3Bhc3MoKQogICAgaWYgbm90IHB3OgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZSgKICAgICAgICAgICAgbWVzc2Fn"
    "ZSwKICAgICAgICAgICAgIuKaoO+4jyBDb3VsZCBub3QgcmVhZCB0aGUgZGFzaGJvYXJkIHBhc3N3b3JkIOKAlCBpcyBNb25nb0RC"
    "IHJlYWNoYWJsZT8iLAogICAgICAgICkKICAgICAgICByZXR1cm4KICAgIGF3YWl0IHNlbmRfbWVzc2FnZSgKICAgICAgICBtZXNz"
    "YWdlLAogICAgICAgICLwn5SQIDxiPldlYiBEYXNoYm9hcmQ8L2I+XG7ilIJcbiIKICAgICAgICBmIuKUoCA8Yj5QYXNzd29yZDwv"
    "Yj4g4oaSIDxjb2RlPntwd308L2NvZGU+XG4iCiAgICAgICAgZiLilKAgPGI+VVJMPC9iPiDihpIge2F3YWl0IF9kYXNoX3VybCgp"
    "fVxuIgogICAgICAgICLilJYgU2VwYXJhdGUgZnJvbSBTVFJFQU1fUEFTUy4gQ2hhbmdlOiAvYWRtaW5wYXNzIE5FV1BBU1MiLAog"
    "ICAgKQoKCiMg4pSA4pSAIC9hbGxvdywgL2JhbnMsIC9sb2NrZGFzaCDigJQgZGFzaGJvYXJkIGFjY2VzcyBjb250cm9sICh2MTUu"
    "MTApIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiB3emZpeF9hbGxvdyhjbGllbnQsIG1lc3NhZ2Up"
    "OgogICAgIiIiL2FsbG93IDxpcD4gW3JldHJpZXNdIHwgL2FsbG93IGFsbCDigJQgdW5iYW4gZGFzaGJvYXJkIHZpc2l0b3JzLgoK"
    "ICAgIEdyYW50cyB0aGUgSVAgTiBleHRyYSBsb2dpbiBhdHRlbXB0cyAoZGVmYXVsdCAzKS4gSWYgdGhleSBhcmUgYWxsCiAgICB3"
    "cm9uZywgdGhlIGJhbiBlc2NhbGF0ZXMgdG8gdGhlIE5FWFQgc3RhZ2UgKGxvbmdlciB0aGFuIGJlZm9yZSkg4oCUCiAgICB0aGVp"
    "ciBwcmV2aW91cyB0aWVyIGlzIGtlcHQuCiAgICAiIiIKICAgIGlmIG5vdCBhd2FpdCBfZGJfb2soKToKICAgICAgICByZXR1cm4g"
    "YXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIF9EQl9ET1dOKQogICAgYXJncyA9IChtZXNzYWdlLnRleHQgb3IgIiIpLnNwbGl0"
    "KClbMTpdCiAgICBpZiBub3QgYXJnczoKICAgICAgICByZXR1cm4gYXdhaXQgc2VuZF9tZXNzYWdlKAogICAgICAgICAgICBtZXNz"
    "YWdlLAogICAgICAgICAgICAiVXNhZ2U6XG4iCiAgICAgICAgICAgICLilI8gPGNvZGU+L2FsbG93IDxpcD4gW3JldHJpZXNdPC9j"
    "b2RlPiDigJQgdW5iYW4gb25lIElQXG4iCiAgICAgICAgICAgICLilKMgPGNvZGU+L2FsbG93IGFsbDwvY29kZT4g4oCUIGNsZWFy"
    "IGV2ZXJ5IGJhbiArIGxvY2tvdXRcbiIKICAgICAgICAgICAgIuKUliByZXRyaWVzID0gZXh0cmEgYXR0ZW1wdHMgYmVmb3JlIHRo"
    "ZSBuZXh0LXN0YWdlIGJhbiAiCiAgICAgICAgICAgICIoZGVmYXVsdCAzKSIsCiAgICAgICAgKQogICAgaWYgYXJnc1swXS5sb3dl"
    "cigpID09ICJhbGwiOgogICAgICAgIG4gPSBhd2FpdCBfYWxsb3dfYWxsKCkKICAgICAgICByZXR1cm4gYXdhaXQgc2VuZF9tZXNz"
    "YWdlKAogICAgICAgICAgICBtZXNzYWdlLAogICAgICAgICAgICBmIuKchSBDbGVhcmVkIHtufSBiYW4vbG9jayByZWNvcmRzIOKA"
    "lCBldmVyeW9uZSBzdGFydHMgZnJlc2guIiwKICAgICAgICApCiAgICBpcCA9IGFyZ3NbMF0uc3RyaXAoKQogICAgdHJ5OgogICAg"
    "ICAgIGV4dHJhID0gaW50KGFyZ3NbMV0pIGlmIGxlbihhcmdzKSA+IDEgZWxzZSAzCiAgICBleGNlcHQgKFR5cGVFcnJvciwgVmFs"
    "dWVFcnJvcik6CiAgICAgICAgZXh0cmEgPSAzCiAgICBleHRyYSA9IG1heCgwLCBtaW4oZXh0cmEsIDIwKSkKICAgIG9rID0gYXdh"
    "aXQgX2FsbG93X2lwKGlwLCBleHRyYSkKICAgIGlmIG9rOgogICAgICAgIHJldHVybiBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAg"
    "ICAgICAgIG1lc3NhZ2UsCiAgICAgICAgICAgIGYi4pyFIFVuYmFubmVkIDxjb2RlPntpcH08L2NvZGU+IChzdWJuZXQgdG9vKSDi"
    "gJQgIgogICAgICAgICAgICBmIjxiPntleHRyYX08L2I+IGF0dGVtcHQocykgZ3JhbnRlZC5cbiIKICAgICAgICAgICAgIuKUliBp"
    "ZiB0aGV5IGFyZSBhbGwgd3JvbmcsIHRoZSBiYW4gZXNjYWxhdGVzIHRvIHRoZSBuZXh0IHN0YWdlLiIsCiAgICAgICAgKQogICAg"
    "cmV0dXJuIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCAi4pqg77iPIENvdWxkIG5vdCB1bmJhbiDigJQgaXMgTW9uZ29EQiBy"
    "ZWFjaGFibGU/IikKCgphc3luYyBkZWYgd3pmaXhfYmFucyhjbGllbnQsIG1lc3NhZ2UpOgogICAgIiIiL2JhbnMg4oCUIGxpc3Qg"
    "Y3VycmVudCBkYXNoYm9hcmQgYmFucywgbG9ja291dHMgYW5kIC9hbGxvdyBncmFudHMuIiIiCiAgICBpZiBub3QgYXdhaXQgX2Ri"
    "X29rKCk6CiAgICAgICAgcmV0dXJuIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikKICAgIHJvd3MgPSBhd2Fp"
    "dCBfbGlzdF9iYW5zKCkKICAgIGlmIG5vdCByb3dzOgogICAgICAgIHJldHVybiBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwg"
    "IuKchSBObyBiYW5zIG9yIGxvY2tvdXRzIG9uIHJlY29yZC4iKQogICAgZnJvbSBodG1sIGltcG9ydCBlc2NhcGUgYXMgX2VzYwoK"
    "ICAgIGxpbmVzID0gWyLwn5uhIDxiPkRhc2hib2FyZCBiYW5zPC9iPiIsICIiXQogICAgZm9yIHIgaW4gcm93c1s6MjVdOgogICAg"
    "ICAgIGsgPSByLmdldCgia2luZCIpCiAgICAgICAgaWYgayA9PSAiYmFuIjoKICAgICAgICAgICAgc3ViID0gIiAoc3VibmV0KSIg"
    "aWYgc3RyKHJbImlkIl0pLnN0YXJ0c3dpdGgoInN1YjoiKSBlbHNlICIiCiAgICAgICAgICAgIGxpbmVzLmFwcGVuZChmIuKUjyA8"
    "Y29kZT57X2VzYyhzdHIoclsnaWQnXSkpfTwvY29kZT57c3VifSDigJQgcGVybWFuZW50IGJhbiIpCiAgICAgICAgZWxpZiBrID09"
    "ICJwZXJtLWxvY2siOgogICAgICAgICAgICBsaW5lcy5hcHBlbmQoZiLilI8gPGNvZGU+e19lc2Moc3RyKHJbJ2lkJ10pKX08L2Nv"
    "ZGU+IOKAlCBwZXJtYW5lbnQgbG9ja291dCIpCiAgICAgICAgZWxpZiBrID09ICJsb2Nrb3V0IjoKICAgICAgICAgICAgZnJvbSBk"
    "YXRldGltZSBpbXBvcnQgdGltZWRlbHRhIGFzIF90ZAoKICAgICAgICAgICAgbGVmdCA9IF90ZChzZWNvbmRzPWludChyLmdldCgi"
    "dW50aWwiLCAwKSAtIHRpbWUoKSkpCiAgICAgICAgICAgIGxpbmVzLmFwcGVuZCgKICAgICAgICAgICAgICAgIGYi4pSPIDxjb2Rl"
    "PntfZXNjKHN0cihyWydpZCddKSl9PC9jb2RlPiDigJQgbG9ja2VkLCB7bGVmdH0gbGVmdCAiCiAgICAgICAgICAgICAgICBmIih0"
    "aWVyIHtyLmdldCgndGllcicsIDApfSkiCiAgICAgICAgICAgICkKICAgICAgICBlbGlmIGsgPT0gImFsbG93ZWQiOgogICAgICAg"
    "ICAgICBsaW5lcy5hcHBlbmQoCiAgICAgICAgICAgICAgICBmIuKUjyA8Y29kZT57X2VzYyhzdHIoclsnaWQnXSkpfTwvY29kZT4g"
    "4oCUIGFsbG93ZWQ6ICIKICAgICAgICAgICAgICAgIGYie3IuZ2V0KCdleHRyYScsIDApfSBhdHRlbXB0KHMpIGxlZnQgKHRpZXIg"
    "e3IuZ2V0KCd0aWVyJywgMCl9KSIKICAgICAgICAgICAgKQogICAgaWYgbGVuKHJvd3MpID4gMjU6CiAgICAgICAgbGluZXMuYXBw"
    "ZW5kKGYi4pSWIOKApmFuZCB7bGVuKHJvd3MpIC0gMjV9IG1vcmUiKQogICAgbGluZXMuYXBwZW5kKCIiKQogICAgbGluZXMuYXBw"
    "ZW5kKCLilJYgPGNvZGU+L2FsbG93IDxpcD4gW25dPC9jb2RlPiB0byB1bmJhbiIpCiAgICByZXR1cm4gYXdhaXQgc2VuZF9tZXNz"
    "YWdlKG1lc3NhZ2UsICJcbiIuam9pbihsaW5lcylbOjM5MDBdKQoKCmFzeW5jIGRlZiB3emZpeF9sb2NrZGFzaChjbGllbnQsIG1l"
    "c3NhZ2UpOgogICAgIiIiL2xvY2tkYXNoIOKAlCBsb2NrIG9yIHVubG9jayB0aGUgd2ViIGRhc2hib2FyZCBlbnRpcmVseS4iIiIK"
    "ICAgIG5ld192YWwgPSBub3QgYm9vbChnZXRhdHRyKENvbmZpZywgIkFETUlOX0RBU0hCT0FSRF9MT0NLRUQiLCBGYWxzZSkpCiAg"
    "ICB0cnk6CiAgICAgICAgQ29uZmlnLnNldCgiQURNSU5fREFTSEJPQVJEX0xPQ0tFRCIsIG5ld192YWwpCiAgICBleGNlcHQgRXhj"
    "ZXB0aW9uOgogICAgICAgIHNldGF0dHIoQ29uZmlnLCAiQURNSU5fREFTSEJPQVJEX0xPQ0tFRCIsIG5ld192YWwpCiAgICB0cnk6"
    "CiAgICAgICAgZnJvbSAuLmhlbHBlci5leHRfdXRpbHMuZGJfaGFuZGxlciBpbXBvcnQgZGF0YWJhc2UKCiAgICAgICAgYXdhaXQg"
    "ZGF0YWJhc2UudXBkYXRlX2NvbmZpZyh7IkFETUlOX0RBU0hCT0FSRF9MT0NLRUQiOiBuZXdfdmFsfSkKICAgICAgICBzYXZlZCA9"
    "ICJzYXZlZCIKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgc2F2ZWQgPSAidGhpcyByZXN0YXJ0IG9ubHkiCiAgICBpZiBu"
    "ZXdfdmFsOgogICAgICAgIHJldHVybiBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgIG1lc3NhZ2UsCiAgICAgICAgICAg"
    "ICLwn5SSIDxiPkRhc2hib2FyZCBMT0NLRUQuPC9iPiBFdmVyeSBsb2dpbiBpcyByZWZ1c2VkICgiICsgc2F2ZWQgKyAiKS5cbiIK"
    "ICAgICAgICAgICAgIuKUliAvbG9ja2Rhc2ggYWdhaW4gdG8gdW5sb2NrLiBUZWxlZ3JhbSBrZWVwcyB3b3JraW5nLiIsCiAgICAg"
    "ICAgKQogICAgcmV0dXJuIGF3YWl0IHNlbmRfbWVzc2FnZSgKICAgICAgICBtZXNzYWdlLAogICAgICAgICLwn5STIDxiPkRhc2hi"
    "b2FyZCB1bmxvY2tlZC48L2I+IExvZ2lucyB3b3JrIGFnYWluICgiICsgc2F2ZWQgKyAiKS4iLAogICAgKQo="
)

WZFIX_WEB_B64 = (
    "IyBXWkZJWCBSb3VuZCAyICh2MTUuOCkg4oCUIG93bmVyIHdlYiBkYXNoYm9hcmQuCiMKIyBTZXJ2ZWQgYnkgdGhlIGJvdCdzIG93"
    "biBhaW9odHRwIHN0cmVhbSBzZXJ2ZXIgKHRoZSBvbmUgdGhhdCBhbHJlYWR5IHJ1bnMKIyBvbiAxMjcuMC4wLjE6ODA5MSksIHNv"
    "IGl0IGhhcyBkaXJlY3QgYWNjZXNzIHRvIHRhc2tfZGljdCAobGl2ZSB0YXNrcyArCiMga2lsbCksIE1vbmdvREIgKHd6Zml4Xyog"
    "Y29sbGVjdGlvbnMpIGFuZCB0aGUgYm90IGl0c2VsZiAocmVwb3J0cykuCiMgUmVhY2hhYmxlIGZyb20gb3V0c2lkZSB0aHJvdWdo"
    "IHRoZSBleGlzdGluZyBjaGFpbjoKIyAgICAgaHR0cHM6Ly88d29ya2VyPi93emFkbWluIC0+IGNsb3VkZmxhcmVkIC0+IHdzZXJ2"
    "ZXI6ODA4MCAtPiBoZXJlCiMKIyBBdXRoOiBhIHNlcGFyYXRlIEFETUlOX1BBU1MgKE5PVCB0aGUgc3RyZWFtIHBhc3N3b3JkKSBz"
    "dG9yZWQgaW4KIyB3emZpeF9jb25maWc7IHNlc3Npb25zIGFyZSBITUFDLXNpZ25lZCB0b2tlbnMgaW4gYW4gSHR0cE9ubHkgY29v"
    "a2llLgojIExvZ2luIGlzIHJhdGUtbGltaXRlZCBwZXIgSVAgKDUgZmFpbHMgLT4gMTAgbWludXRlIGxvY2spLgojCiMgRXZlcnl0"
    "aGluZyBmYWlscyBzYWZlOiBhbnkgZGFzaGJvYXJkIGVycm9yIG11c3QgbmV2ZXIgYWZmZWN0IGRvd25sb2Fkcy4KCmZyb20gYXN5"
    "bmNpbyBpbXBvcnQgc2xlZXAgYXMgYWlvc2xlZXAKZnJvbSBkYXRldGltZSBpbXBvcnQgZGF0ZXRpbWUsIHRpbWVkZWx0YSwgdGlt"
    "ZXpvbmUKZnJvbSBoYXNobGliIGltcG9ydCBzaGEyNTYKZnJvbSBobWFjIGltcG9ydCBjb21wYXJlX2RpZ2VzdCwgbmV3IGFzIGht"
    "YWNfbmV3CmZyb20gc2VjcmV0cyBpbXBvcnQgdG9rZW5faGV4LCB0b2tlbl91cmxzYWZlCmZyb20gdGltZSBpbXBvcnQgdGltZQoK"
    "ZnJvbSBhaW9odHRwIGltcG9ydCB3ZWIKCmZyb20gLnIxX2NvcmUgaW1wb3J0ICgKICAgIF9kYXlfaXN0LAogICAgX2RiLAogICAg"
    "X2dldF9nbG9iYWxfY2FwX2diLAogICAgX25pY2Vfc2l6ZSwKICAgIF9wYXJ0LAogICAgYWxsX3VzZXJzLAogICAgZGVsZXRlX3Vz"
    "ZXIsCiAgICBlbnN1cmVfcmVhZHksCiAgICBmaW5kLAogICAgZ2V0X2NhcF9ieXRlcywKICAgIGdldF91c2VyX2RvYywKICAgIGdl"
    "dF91c2FnZSwKICAgIHJlc2VydmVkX3RvZGF5LAogICAgcmVzZXRfdXNhZ2UsCiAgICBfZ2V0X2dsb2JhbF9tdXNpYywKICAgIHNl"
    "dF9nbG9iYWxfY2FwX2diLAogICAgc2V0X2dsb2JhbF9tdXNpYywKICAgIHNldF91c2VyX2NhcCwKICAgIHNldF91c2VyX211c2lj"
    "LAogICAgdG9kYXlfcm93cywKKQoKZGVmIF9mbXRfZ2IoZ2IpOgogICAgIiIiQ29tcGFjdCBHQiBsYWJlbCwgbmV2ZXIgc2NpZW50"
    "aWZpYyBub3RhdGlvbi4iIiIKICAgIHRyeToKICAgICAgICBnYiA9IGZsb2F0KGdiIG9yIDApCiAgICBleGNlcHQgKFR5cGVFcnJv"
    "ciwgVmFsdWVFcnJvcik6CiAgICAgICAgcmV0dXJuICIwIEdCIgogICAgaWYgZ2IgPj0gMTAyNDoKICAgICAgICByZXR1cm4gZiJ7"
    "Z2IgLyAxMDI0Oi4yZn0gVEIiCiAgICBpZiBnYiA+PSAxMDoKICAgICAgICByZXR1cm4gZiJ7Z2I6LjBmfSBHQiIKICAgIGlmIGdi"
    "ID49IDE6CiAgICAgICAgcmV0dXJuIGYie2diOi4xZn0gR0IiCiAgICBpZiBnYiA+IDA6CiAgICAgICAgcmV0dXJuIGYie2diICog"
    "MTAyNDouMGZ9IE1CIgogICAgcmV0dXJuICIwIEdCIgoKCklTVCA9IHRpbWV6b25lKHRpbWVkZWx0YShob3Vycz01LCBtaW51dGVz"
    "PTMwKSkKU0VTU0lPTl9IID0gMTIgICAgICAgICAgICMgZGFzaGJvYXJkIGxvZ2luIGxhc3RzIDEyIGhvdXJzCkxPR0lOX01BWF9G"
    "QUlMUyA9IDMgICAgICAjIHdyb25nIHBhc3N3b3JkcyBiZWZvcmUgYSBsb2Nrb3V0ICh2MTUuOSkKIyBlc2NhbGF0aW5nIGxvY2tv"
    "dXRzOiAxc3QgLT4gMjRoLCAybmQgLT4gNzJoLCAzcmQgLT4gN2QsIDR0aCAtPiAzMGQsCiMgNXRoIC0+IHBlcm1hbmVudCAocGVy"
    "IElQLCBwZXJzaXN0ZWQgaW4gdGhlIERCKQpMT0NLX1RJRVJTX1MgPSAoODY0MDAsIDI1OTIwMCwgNjA0ODAwLCAyNTkyMDAwLCAt"
    "MSkKUkVQT1JUX01BWF9DSEFSUyA9IDUwICogMTAyNApJUF9JTkZPX1RUTCA9IDcgKiA4NjQwMCAgIyBjYWNoZSBJUCBpbnRlbGxp"
    "Z2VuY2UgZm9yIGEgd2VlawoKIyBPbmx5IHJlYWwsIGtub3duIGJyb3dzZXJzIGFyZSBhbGxvd2VkIChzcG9vZmVkL3Vua25vd24g"
    "VUFzIGFyZSBibG9ja2VkKQpfS05PV05fVUEgPSAoCiAgICAibW96aWxsYS81LjAiLCAgIyBiYXNlIGZvciBmaXJlZm94ICsgZ2Vj"
    "a28gd2Vidmlld3MKICAgICJjaHJvbWUvIiwKICAgICJjcmlvcy8iLCAgICAgICAgIyBjaHJvbWUgaW9zCiAgICAiZWRnYS8iLCAi"
    "ZWRnaW9zLyIsICJlZGcvIiwKICAgICJmaXJlZm94LyIsCiAgICAiZnhpb3MvIiwgICAgICAgICMgZmlyZWZveCBpb3MKICAgICJz"
    "YWZhcmkvIiwKICAgICJzYW1zdW5nYnJvd3Nlci8iLAogICAgIm9wZXJhLyIsICJvcHQvIiwKICAgICJvcHIvIiwKICAgICJ2aXZv"
    "YnJvd3Nlci8iLCAiaGV5dGFicm93c2VyLyIsICJoZXl0YXBicm93c2VyLyIsCiAgICAiaHVhd2VpYnJvd3Nlci8iLCAiaGJicm93"
    "c2VyLyIsCiAgICAibWlicm93c2VyLyIsICJtaXVpIiwgICMgeGlhb21pCiAgICAicXVhcmsvIiwgInVjYnJvd3Nlci8iLCAidWJy"
    "b3dzZXIvIiwKICAgICJ5YWJyb3dzZXIvIiwgInlhbmRleCIsCiAgICAiZHVja2R1Y2tnby8iLAogICAgImJyYXZlLyIsICJ2aXZh"
    "bGRpLyIsCiAgICAiaW5zdGFncmFtIiwgInNuYXBjaGF0IiwgIndoYXRzYXBwIiwgICMgaW4tYXBwIHdlYnZpZXdzCiAgICAiZmJh"
    "biIsICJmYmF2IiwgImZiX2lhYiIsICAjIGZhY2Vib29rCiAgICAidGVsZWdyYW0iLCAiZGlzY29yZCIsCiAgICAia2Fpb3MiLAog"
    "ICAgIndlY2hhdCIsICJsaW5lLyIsCikKX0JBRF9VQSA9ICgKICAgICJweXRob24iLCAiY3VybC8iLCAid2dldCIsICJva2h0dHAi"
    "LCAiamF2YS8iLCAiZ28taHR0cCIsICJnb2xhbmciLAogICAgIm5vZGUiLCAic2NyYXB5IiwgImJvdC8iLCAiY3Jhd2xlciIsICJz"
    "cGlkZXIiLCAiaHR0cGNsaWVudCIsCiAgICAibGlid3d3IiwgImF4aW9zIiwgInBvc3RtYW4iLCAiaGVhZGxlc3MiLCAicGhhbnRv"
    "bSIsICJzZWxlbml1bSIsCiAgICAicHVwcGV0ZWVyIiwgInBsYXl3cmlnaHQiLCAicmVxdWVzdHMiLCAiYWlvaHR0cCIsICJodHRw"
    "eCIsCikKCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSACiMgTG9naW4gaGFyZGVuaW5nICh2MTUuOSk6IFVBIGFsbG93bGlzdCwgcGVyc2lzdGVudCBlc2NhbGF0aW5nIGxvY2tvdXRz"
    "LAojIHN1Ym5ldCBiYW5zLCBkYXRhY2VudGVyL1ZQTiBJUCBpbnRlbGxpZ2VuY2UgKGlwLWFwaS5jb20sIGNhY2hlZCkKIyDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgojIOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIEFETUlOX1BB"
    "U1MgKyBzZXNzaW9uIHRva2VucwojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiBfZ2V0X211c2ljX3NldHRpbmdzKCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAu"
    "cjRfbXVzaWNfY21kcyBpbXBvcnQgZ2V0X3NldHRpbmdzCgogICAgICAgIHJldHVybiBhd2FpdCBnZXRfc2V0dGluZ3MoZm9yY2U9"
    "VHJ1ZSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIHsibXVzaWNfb24iOiBUcnVlLCAibGRfb24iOiBUcnVl"
    "LCAiYWxpYXNlc19vbiI6IFRydWV9CgoKYXN5bmMgZGVmIGdldF9hZG1pbl9wYXNzKCk6CiAgICAiIiJDdXJyZW50IEFETUlOX1BB"
    "U1MgKENvbmZpZy5BRE1JTl9QQVNTIGZyb20gL2JzIHdpbnMsIHRoZW4gREIpLgoKICAgIFByaW9yaXR5OiAvYnMtc2V0IEFETUlO"
    "X1BBU1MgKGxpdmUgY29uZmlnLCBpbmNsdWRlcyBjb25maWcuZW52KSDihpIKICAgIERCLXN0b3JlZCB2YWx1ZSAoYXV0by1nZW5l"
    "cmF0ZWQgb3Igc2V0IHZpYSAvYWRtaW5wYXNzKS4KICAgICIiIgogICAgIyAvYnMtc2V0IEFETUlOX1BBU1Mgd2lucyAoc2FtZSBw"
    "YXR0ZXJuIGFzIFNUUkVBTV9QQVNTKQogICAgdHJ5OgogICAgICAgIGZyb20gLi4uY29yZS5jb25maWdfbWFuYWdlciBpbXBvcnQg"
    "Q29uZmlnCgogICAgICAgIF9icyA9IHN0cihnZXRhdHRyKENvbmZpZywgIkFETUlOX1BBU1MiLCAiIikgb3IgIiIpLnN0cmlwKCkK"
    "ICAgICAgICBpZiBfYnM6CiAgICAgICAgICAgIHJldHVybiBfYnMKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwog"
    "ICAgdHJ5OgogICAgICAgIGNvbCA9IF9kYigpLnd6Zml4X2NvbmZpZ1tfcGFydCgpXQogICAgICAgIGRvYyA9IGF3YWl0IGNvbC5m"
    "aW5kX29uZSh7Il9pZCI6ICJhZG1pbiJ9KQogICAgICAgIGlmIGRvYyBhbmQgZG9jLmdldCgicGFzcyIpOgogICAgICAgICAgICBy"
    "ZXR1cm4gc3RyKGRvY1sicGFzcyJdKQogICAgICAgIGZyb20gb3MgaW1wb3J0IGdldGVudgoKICAgICAgICBwdyA9IGdldGVudigi"
    "V1pGSVhfQURNSU5fUEFTUyIsICIiKS5zdHJpcCgpIG9yIHRva2VuX3VybHNhZmUoNikKICAgICAgICBhd2FpdCBjb2wudXBkYXRl"
    "X29uZSh7Il9pZCI6ICJhZG1pbiJ9LCB7IiRzZXQiOiB7InBhc3MiOiBwd319LCB1cHNlcnQ9VHJ1ZSkKICAgICAgICByZXR1cm4g"
    "cHcKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuICIiCgoKYXN5bmMgZGVmIHNldF9hZG1pbl9wYXNzKG5ld19w"
    "YXNzKToKICAgIGNvbCA9IF9kYigpLnd6Zml4X2NvbmZpZ1tfcGFydCgpXQogICAgYXdhaXQgY29sLnVwZGF0ZV9vbmUoCiAgICAg"
    "ICAgeyJfaWQiOiAiYWRtaW4ifSwgeyIkc2V0IjogeyJwYXNzIjogc3RyKG5ld19wYXNzKS5zdHJpcCgpfX0sIHVwc2VydD1UcnVl"
    "CiAgICApCgoKYXN5bmMgZGVmIF9hZG1pbl9zZWNyZXQoKToKICAgICIiIlJhbmRvbSBwZXItYm90IHNpZ25pbmcgc2VjcmV0IChj"
    "cmVhdGVkIG9uY2UsIHN0b3JlZCBpbiB3emZpeF9jb25maWcpLiIiIgogICAgdHJ5OgogICAgICAgIGNvbCA9IF9kYigpLnd6Zml4"
    "X2NvbmZpZ1tfcGFydCgpXQogICAgICAgIGRvYyA9IGF3YWl0IGNvbC5maW5kX29uZSh7Il9pZCI6ICJhZG1pbl9zZWNyZXQifSkK"
    "ICAgICAgICBpZiBkb2MgYW5kIGRvYy5nZXQoInNlY3JldCIpOgogICAgICAgICAgICByZXR1cm4gc3RyKGRvY1sic2VjcmV0Il0p"
    "CiAgICAgICAgc2VjcmV0ID0gdG9rZW5faGV4KDMyKQogICAgICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAgICAgICAgICB7"
    "Il9pZCI6ICJhZG1pbl9zZWNyZXQifSwgeyIkc2V0IjogeyJzZWNyZXQiOiBzZWNyZXR9fSwgdXBzZXJ0PVRydWUKICAgICAgICAp"
    "CiAgICAgICAgcmV0dXJuIHNlY3JldAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gInd6Zml4LW5vLXNlY3Jl"
    "dCIKCgphc3luYyBkZWYgX3Nlc3Npb25fdG9rZW4oKToKICAgIHNlY3JldCA9IGF3YWl0IF9hZG1pbl9zZWNyZXQoKQogICAgZXhw"
    "ID0gaW50KHRpbWUoKSkgKyBTRVNTSU9OX0ggKiAzNjAwCiAgICBzaWcgPSBobWFjX25ldyhzZWNyZXQuZW5jb2RlKCksIGYiYWRt"
    "aW46e2V4cH0iLmVuY29kZSgpLCBzaGEyNTYpLmhleGRpZ2VzdCgpCiAgICByZXR1cm4gZiJ7ZXhwfS57c2lnfSIKCgphc3luYyBk"
    "ZWYgX2NoZWNrX3Nlc3Npb24ocmVxdWVzdCk6CiAgICB0cnk6CiAgICAgICAgdG9rID0gcmVxdWVzdC5jb29raWVzLmdldCgid3ph"
    "ZG1pbiIsICIiKQogICAgICAgIGV4cCwgXywgc2lnID0gdG9rLnBhcnRpdGlvbigiLiIpCiAgICAgICAgc2VjcmV0ID0gYXdhaXQg"
    "X2FkbWluX3NlY3JldCgpCiAgICAgICAgZ29vZCA9IGhtYWNfbmV3KHNlY3JldC5lbmNvZGUoKSwgZiJhZG1pbjp7ZXhwfSIuZW5j"
    "b2RlKCksIHNoYTI1NikuaGV4ZGlnZXN0KCkKICAgICAgICBpZiBub3QgdG9rIG9yIG5vdCBjb21wYXJlX2RpZ2VzdChzaWcsIGdv"
    "b2QpOgogICAgICAgICAgICByZXR1cm4gRmFsc2UKICAgICAgICByZXR1cm4gaW50KGV4cCkgPiB0aW1lKCkKICAgIGV4Y2VwdCBF"
    "eGNlcHRpb246CiAgICAgICAgcmV0dXJuIEZhbHNlCgoKZGVmIF9jbGllbnRfaXAocmVxdWVzdCk6CiAgICBmd2QgPSByZXF1ZXN0"
    "LmhlYWRlcnMuZ2V0KCJYLUZvcndhcmRlZC1Gb3IiLCAiIikKICAgIGlmIGZ3ZDoKICAgICAgICByZXR1cm4gZndkLnNwbGl0KCIs"
    "IilbMF0uc3RyaXAoKVs6NjRdCiAgICB0cnk6CiAgICAgICAgcmV0dXJuIChyZXF1ZXN0LnJlbW90ZSBvciAiPyIpWzo2NF0KICAg"
    "IGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuICI/IgoKCmRlZiBfdWFfb2socmVxdWVzdCk6CiAgICB1YSA9IChyZXF1"
    "ZXN0LmhlYWRlcnMuZ2V0KCJVc2VyLUFnZW50Iikgb3IgIiIpLnN0cmlwKCkubG93ZXIoKQogICAgaWYgbm90IHVhIG9yIGxlbih1"
    "YSkgPCAxMDoKICAgICAgICByZXR1cm4gRmFsc2UKICAgIGlmIGFueShiIGluIHVhIGZvciBiIGluIF9CQURfVUEpOgogICAgICAg"
    "IHJldHVybiBGYWxzZQogICAgcmV0dXJuIGFueShrIGluIHVhIGZvciBrIGluIF9LTk9XTl9VQSkKCgpkZWYgX3N1Ym5ldChpcCk6"
    "CiAgICAiIiJJUHY0IC8yNCBvciBJUHY2IC82NCBwcmVmaXggZm9yIHN1Ym5ldC1sZXZlbCBiYW5zLiIiIgogICAgdHJ5OgogICAg"
    "ICAgIGlmICI6IiBpbiBpcDoKICAgICAgICAgICAgcmV0dXJuICIvIi5qb2luKGlwLnNwbGl0KCI6IilbOjRdKSArICI6Oi82NCIK"
    "ICAgICAgICBwYXJ0cyA9IGlwLnNwbGl0KCIuIikKICAgICAgICBpZiBsZW4ocGFydHMpID09IDQ6CiAgICAgICAgICAgIHJldHVy"
    "biAiLiIuam9pbihwYXJ0c1s6M10pICsgIi4wLzI0IgogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1"
    "cm4gaXAKCgpkZWYgX25vcm1faXAoaXApOgogICAgaXAgPSAoaXAgb3IgIiIpLnN0cmlwKCkKICAgIGlmIGlwLnN0YXJ0c3dpdGgo"
    "Ijo6ZmZmZjoiKToKICAgICAgICBpcCA9IGlwWzc6XQogICAgcmV0dXJuIGlwWzo0NV0KCgphc3luYyBkZWYgX2lzX2Jhbm5lZChp"
    "cCk6CiAgICAiIiJJUCBvciBzdWJuZXQgYmFuIGNoZWNrIChwZXJzaXN0ZW50LCBEQi1iYWNrZWQpLiIiIgogICAgdHJ5OgogICAg"
    "ICAgIGNvbCA9IF9kYigpLnd6Zml4X2JhbnNbX3BhcnQoKV0KICAgICAgICBpZiBhd2FpdCBjb2wuZmluZF9vbmUoeyJfaWQiOiBf"
    "bm9ybV9pcChpcCl9KToKICAgICAgICAgICAgcmV0dXJuIFRydWUKICAgICAgICBpZiBhd2FpdCBjb2wuZmluZF9vbmUoeyJfaWQi"
    "OiAic3ViOiIgKyBfc3VibmV0KF9ub3JtX2lwKGlwKSl9KToKICAgICAgICAgICAgcmV0dXJuIFRydWUKICAgIGV4Y2VwdCBFeGNl"
    "cHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIEZhbHNlCgoKYXN5bmMgZGVmIF9iYW5faXAoaXAsIHN1Ym5ldF90b289VHJ1"
    "ZSk6CiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCkud3pmaXhfYmFuc1tfcGFydCgpXQogICAgICAgIGF3YWl0IGNvbC51cGRh"
    "dGVfb25lKAogICAgICAgICAgICB7Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgIHsiJHNldCI6IHsicGVybSI6IFRy"
    "dWUsICJ0cyI6IHRpbWUoKX19LAogICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICAgICApCiAgICAgICAgaWYgc3VibmV0X3Rv"
    "bzoKICAgICAgICAgICAgYXdhaXQgY29sLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgICAgICB7Il9pZCI6ICJzdWI6IiArIF9zdWJu"
    "ZXQoX25vcm1faXAoaXApKX0sCiAgICAgICAgICAgICAgICB7IiRzZXQiOiB7InBlcm0iOiBUcnVlLCAidHMiOiB0aW1lKCl9fSwK"
    "ICAgICAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBh"
    "c3MKCgphc3luYyBkZWYgX3ByaW9yX2ZhaWxzKGlwKToKICAgICIiIkZhaWxlZC1hdHRlbXB0IGNvdW50IGN1cnJlbnRseSBvbiBy"
    "ZWNvcmQgZm9yIHRoaXMgSVAuIiIiCiAgICB0cnk6CiAgICAgICAgZG9jID0gYXdhaXQgX2RiKCkud3pmaXhfbG9ja3NbX3BhcnQo"
    "KV0uZmluZF9vbmUoeyJfaWQiOiBfbm9ybV9pcChpcCl9KQogICAgICAgIHJldHVybiBpbnQoKGRvYyBvciB7fSkuZ2V0KCJmYWls"
    "cyIpIG9yIDApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAwCgoKYXN5bmMgZGVmIF9hbGxvd19pcChpcCwg"
    "ZXh0cmE9Myk6CiAgICAiIiJPd25lciB1bmJhbm5lZCBhbiBJUDogY2xlYXIgYmFucyArIGxvY2tvdXQsIGdyYW50IE4gcmV0cnkg"
    "YXR0ZW1wdHMuCgogICAgVGhlIHRpZXIgaXMgS0VQVCwgc28gYnVybmluZyB0aGUgcmV0cmllcyBlc2NhbGF0ZXMgdG8gdGhlIG5l"
    "eHQgc3RhZ2UKICAgIChsb25nZXIgdGhhbiB0aGUgcHJldmlvdXMgYmFuKS4KICAgICIiIgogICAgdHJ5OgogICAgICAgIGlwID0g"
    "X25vcm1faXAoc3RyKGlwKSkKICAgICAgICBiY29sID0gX2RiKCkud3pmaXhfYmFuc1tfcGFydCgpXQogICAgICAgIGF3YWl0IGJj"
    "b2wuZGVsZXRlX29uZSh7Il9pZCI6IGlwfSkKICAgICAgICBhd2FpdCBiY29sLmRlbGV0ZV9vbmUoeyJfaWQiOiAic3ViOiIgKyBf"
    "c3VibmV0KGlwKX0pCiAgICAgICAgbGNvbCA9IF9kYigpLnd6Zml4X2xvY2tzW19wYXJ0KCldCiAgICAgICAgYXdhaXQgbGNvbC51"
    "cGRhdGVfb25lKAogICAgICAgICAgICB7Il9pZCI6IGlwfSwKICAgICAgICAgICAgeyIkc2V0IjogeyJmYWlscyI6IDAsICJleHRy"
    "YSI6IGludChleHRyYSksICJ1bnRpbCI6IDAsICJwZXJtIjogRmFsc2V9fSwKICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAg"
    "ICAgKQogICAgICAgIHJldHVybiBUcnVlCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBGYWxzZQoKCmFzeW5j"
    "IGRlZiBfYWxsb3dfYWxsKCk6CiAgICAiIiJDbGVhciBldmVyeSBiYW4sIHN1Ym5ldCBiYW4gYW5kIGxvY2tvdXQuIiIiCiAgICB0"
    "cnk6CiAgICAgICAgbiA9IDAKICAgICAgICBmb3IgY29sbCBpbiAoInd6Zml4X2JhbnMiLCAid3pmaXhfbG9ja3MiKToKICAgICAg"
    "ICAgICAgciA9IGF3YWl0IF9kYigpW2NvbGxdW19wYXJ0KCldLmRlbGV0ZV9tYW55KHt9KQogICAgICAgICAgICBuICs9IGludChn"
    "ZXRhdHRyKHIsICJkZWxldGVkX2NvdW50IiwgMCkgb3IgMCkKICAgICAgICByZXR1cm4gbgogICAgZXhjZXB0IEV4Y2VwdGlvbjoK"
    "ICAgICAgICByZXR1cm4gMAoKCmFzeW5jIGRlZiBfbGlzdF9iYW5zKCk6CiAgICAiIiJBbGwgYmFucyBhbmQgbG9ja291dHMsIGZv"
    "ciB0aGUgL2JhbnMgY29tbWFuZC4iIiIKICAgIG91dCA9IFtdCiAgICB0cnk6CiAgICAgICAgYXN5bmMgZm9yIGQgaW4gX2RiKCku"
    "d3pmaXhfYmFuc1tfcGFydCgpXS5maW5kKHt9LCBsaW1pdD01MCk6CiAgICAgICAgICAgIG91dC5hcHBlbmQoCiAgICAgICAgICAg"
    "ICAgICB7CiAgICAgICAgICAgICAgICAgICAgImtpbmQiOiAiYmFuIiwKICAgICAgICAgICAgICAgICAgICAiaWQiOiBkLmdldCgi"
    "X2lkIiwgIj8iKSwKICAgICAgICAgICAgICAgICAgICAidHMiOiBkLmdldCgidHMiLCAwKSwKICAgICAgICAgICAgICAgIH0KICAg"
    "ICAgICAgICAgKQogICAgICAgIGFzeW5jIGZvciBkIGluIF9kYigpLnd6Zml4X2xvY2tzW19wYXJ0KCldLmZpbmQoe30sIGxpbWl0"
    "PTUwKToKICAgICAgICAgICAgaWYgZC5nZXQoInBlcm0iKToKICAgICAgICAgICAgICAgIG91dC5hcHBlbmQoeyJraW5kIjogInBl"
    "cm0tbG9jayIsICJpZCI6IGQuZ2V0KCJfaWQiLCAiPyIpLCAidHMiOiAwfSkKICAgICAgICAgICAgZWxpZiAoZC5nZXQoInVudGls"
    "Iikgb3IgMCkgPiB0aW1lKCk6CiAgICAgICAgICAgICAgICBvdXQuYXBwZW5kKAogICAgICAgICAgICAgICAgICAgIHsKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgImtpbmQiOiAibG9ja291dCIsCiAgICAgICAgICAgICAgICAgICAgICAgICJpZCI6IGQuZ2V0KCJf"
    "aWQiLCAiPyIpLAogICAgICAgICAgICAgICAgICAgICAgICAidW50aWwiOiBkLmdldCgidW50aWwiKSwKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgInRpZXIiOiBkLmdldCgidGllciIsIDApLAogICAgICAgICAgICAgICAgICAgICAgICAiZXh0cmEiOiBkLmdldCgi"
    "ZXh0cmEiLCAwKSwKICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgIGVsaWYgZC5nZXQo"
    "ImV4dHJhIik6CiAgICAgICAgICAgICAgICBvdXQuYXBwZW5kKAogICAgICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgImtpbmQiOiAiYWxsb3dlZCIsCiAgICAgICAgICAgICAgICAgICAgICAgICJpZCI6IGQuZ2V0KCJfaWQiLCAiPyIp"
    "LAogICAgICAgICAgICAgICAgICAgICAgICAiZXh0cmEiOiBkLmdldCgiZXh0cmEiKSwKICAgICAgICAgICAgICAgICAgICAgICAg"
    "InRpZXIiOiBkLmdldCgidGllciIsIDApLAogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICkKICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIG91dAoKCmRlZiBfY2hhdF9pbnQoY2hhdCk6CiAgICAiIiJweXJv"
    "Z3JhbSB0cmVhdHMgc3RyaW5nIGNoYXQgaWRzIGFzIEB1c2VybmFtZXMg4oCUIG51bWVyaWMgaWRzCiAgICBNVVNUIGJlIGludCBv"
    "ciB0aGUgc2VuZCBmYWlscyB3aXRoIHBlZXItbm90LWZvdW5kLiIiIgogICAgY2hhdCA9IHN0cihjaGF0IG9yICIiKS5zdHJpcCgp"
    "CiAgICBpZiBub3QgY2hhdDoKICAgICAgICByZXR1cm4gIiIKICAgIGlmIGNoYXQubHN0cmlwKCItIikuaXNkaWdpdCgpOgogICAg"
    "ICAgIHJldHVybiBpbnQoY2hhdCkKICAgIHJldHVybiBjaGF0CgoKYXN5bmMgZGVmIF9sb2dpbl9hbGVydChpcCwgcmVxdWVzdCwg"
    "ZGV2LCBzdWJtaXR0ZWQsIHRpdGxlKToKICAgICIiIkRhc2hib2FyZCBsb2dpbiBldmVudCAtPiBMT0dfQ0hBVCAodG9nZ2xlOiBB"
    "RE1JTl9MT0dJTl9BTEVSVFMgdmlhIC9icykuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi5jb3JlLmNvbmZpZ19tYW5hZ2Vy"
    "IGltcG9ydCBDb25maWcKICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVudAoKICAgICAgICBpZiBu"
    "b3QgZ2V0YXR0cihDb25maWcsICJBRE1JTl9MT0dJTl9BTEVSVFMiLCBUcnVlKToKICAgICAgICAgICAgcmV0dXJuCiAgICAgICAg"
    "Y2hhdCA9IHN0cihnZXRhdHRyKENvbmZpZywgIkFETUlOX0xPR19DSEFUIiwgIiIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAgaWYg"
    "bm90IGNoYXQ6CiAgICAgICAgICAgIGNoYXQgPSBzdHIoZ2V0YXR0cihDb25maWcsICJMT0dfQ0hBVCIsICIiKSBvciAiIikuc3Ry"
    "aXAoKQogICAgICAgIGlmIG5vdCBjaGF0OgogICAgICAgICAgICByZXR1cm4KICAgICAgICBoZHJzID0gW10KICAgICAgICBmb3Ig"
    "ayBpbiAoCiAgICAgICAgICAgICJVc2VyLUFnZW50IiwgIkFjY2VwdC1MYW5ndWFnZSIsICJSZWZlcmVyIiwgIlNlYy1DaC1VYSIs"
    "CiAgICAgICAgICAgICJTZWMtQ2gtVWEtUGxhdGZvcm0iLCAiU2VjLUNoLVVhLU1vYmlsZSIsICJTZWMtRmV0Y2gtU2l0ZSIsCiAg"
    "ICAgICAgICAgICJTZWMtRmV0Y2gtTW9kZSIsICJTZWMtRmV0Y2gtRGVzdCIsCiAgICAgICAgKToKICAgICAgICAgICAgdHJ5Ogog"
    "ICAgICAgICAgICAgICAgdiA9IHJlcXVlc3QuaGVhZGVycy5nZXQoaykKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAg"
    "ICAgICAgICAgICAgIHYgPSBOb25lCiAgICAgICAgICAgIGlmIHY6CiAgICAgICAgICAgICAgICBoZHJzLmFwcGVuZChmIuKUoCB7"
    "a306IHtzdHIodilbOjEyMF19IikKICAgICAgICBkZXZfbCA9IFtdCiAgICAgICAgZm9yIGssIGxibCBpbiAoCiAgICAgICAgICAg"
    "ICgicGxhdGZvcm0iLCAiUGxhdGZvcm0iKSwgKCJsYW5nIiwgIkxhbmd1YWdlIiksCiAgICAgICAgICAgICgibGFuZ3MiLCAiTGFu"
    "Z3VhZ2VzIiksICgidHoiLCAiVGltZXpvbmUiKSwKICAgICAgICAgICAgKCJzY3JlZW4iLCAiU2NyZWVuIiksICgiZHByIiwgIlBp"
    "eGVsIHJhdGlvIiksCiAgICAgICAgICAgICgibWVtIiwgIkRldmljZSBtZW1vcnkiKSwgKCJjb3JlcyIsICJDUFUgY29yZXMiKSwK"
    "ICAgICAgICAgICAgKCJ0b3VjaCIsICJUb3VjaCBwb2ludHMiKSwgKCJjb29raWVzIiwgIkNvb2tpZXMiKSwKICAgICAgICAgICAg"
    "KCJ3ZWJkcml2ZXIiLCAiQXV0b21hdGlvbiIpLCAoIm5ldCIsICJOZXR3b3JrIiksCiAgICAgICAgKToKICAgICAgICAgICAgdiA9"
    "IGRldi5nZXQoaykKICAgICAgICAgICAgaWYgdiBpcyBub3QgTm9uZSBhbmQgdiAhPSAiIjoKICAgICAgICAgICAgICAgIGRldl9s"
    "LmFwcGVuZChmIuKUoCB7bGJsfToge3N0cih2KVs6ODBdfSIpCiAgICAgICAgbGluZXMgPSBbCiAgICAgICAgICAgICLwn5uhIDxi"
    "PldaRklYIGRhc2hib2FyZDwvYj4iLAogICAgICAgICAgICB0aXRsZSwKICAgICAgICAgICAgIiIsCiAgICAgICAgICAgIGYi4pSP"
    "IDxiPklQPC9iPiDihpIgPGNvZGU+e2lwfTwvY29kZT4iLAogICAgICAgICAgICBmIuKUoyA8Yj5UcmllZDwvYj4g4oaSIDxjb2Rl"
    "PntzdHIoc3VibWl0dGVkKVs6NDhdfTwvY29kZT4iLAogICAgICAgIF0KICAgICAgICBpZiBkZXZfbDoKICAgICAgICAgICAgbGlu"
    "ZXMgKz0gWyLilKMgPGI+RGV2aWNlPC9iPiJdICsgZGV2X2wKICAgICAgICBpZiBoZHJzOgogICAgICAgICAgICBsaW5lcyArPSBb"
    "IuKUoyA8Yj5IZWFkZXJzPC9iPiJdICsgaGRyc1s6OV0KICAgICAgICBsaW5lcy5hcHBlbmQoIuKUliB2aWEgL2JzIEFETUlOX0xP"
    "R0lOX0FMRVJUUyB0byB0b2dnbGUiKQogICAgICAgIGF3YWl0IFRnQ2xpZW50LmJvdC5zZW5kX21lc3NhZ2UoCiAgICAgICAgICAg"
    "IGNoYXRfaWQ9X2NoYXRfaW50KGNoYXQpLAogICAgICAgICAgICB0ZXh0PSJcbiIuam9pbihsaW5lcylbOjM5MDBdLAogICAgICAg"
    "ICAgICBkaXNhYmxlX3dlYl9wYWdlX3ByZXZpZXc9VHJ1ZSwKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAg"
    "IHBhc3MKCgphc3luYyBkZWYgX2lwX2ludGVsKGlwKToKICAgICIiIkRhdGFjZW50ZXIvVlBOIGRldGVjdGlvbiB2aWEgaXAtYXBp"
    "LmNvbSAoZnJlZSB0aWVyLCBjYWNoZWQgNyBkYXlzKS4KICAgIFJldHVybnMgVHJ1ZSB3aGVuIHRoZSBJUCBsb29rcyBsaWtlIGEg"
    "c2VydmVyL3Byb3h5ICgtPiBiYW5uZWQpLiIiIgogICAgaXAgPSBfbm9ybV9pcChpcCkKICAgIGlmIG5vdCBpcCBvciBpcCBpbiAo"
    "IjEyNy4wLjAuMSIsICI6OjEiLCAiPyIsICJsb2NhbGhvc3QiKToKICAgICAgICByZXR1cm4gRmFsc2UKICAgIGlmIG5vdCBpcC5y"
    "ZXBsYWNlKCIuIiwgIiIpLmlzZGlnaXQoKTogICMgaXB2NiBvciB1bmtub3duOiBza2lwIGxvb2t1cAogICAgICAgIHJldHVybiBG"
    "YWxzZQogICAgdHJ5OgogICAgICAgIGNvbCA9IF9kYigpLnd6Zml4X2lwaW5mb1tfcGFydCgpXQogICAgICAgIGNhY2hlZCA9IGF3"
    "YWl0IGNvbC5maW5kX29uZSh7Il9pZCI6IGlwfSkKICAgICAgICBpZiBjYWNoZWQgYW5kIHRpbWUoKSAtIGNhY2hlZC5nZXQoInRz"
    "IiwgMCkgPCBJUF9JTkZPX1RUTDoKICAgICAgICAgICAgcmV0dXJuIGJvb2woY2FjaGVkLmdldCgiYmFkIikpCiAgICAgICAgaW1w"
    "b3J0IGpzb24gYXMgX2pzb24KICAgICAgICBmcm9tIHVybGxpYi5yZXF1ZXN0IGltcG9ydCB1cmxvcGVuLCBSZXF1ZXN0CgogICAg"
    "ICAgIHVybCA9ICgKICAgICAgICAgICAgImh0dHA6Ly9pcC1hcGkuY29tL2pzb24vIiArIGlwCiAgICAgICAgICAgICsgIj9maWVs"
    "ZHM9c3RhdHVzLHByb3h5LGhvc3RpbmcsbW9iaWxlLGlzcCIKICAgICAgICApCiAgICAgICAgcmVxID0gUmVxdWVzdCh1cmwsIGhl"
    "YWRlcnM9eyJVc2VyLUFnZW50IjogInd6Zml4LWRhc2hib2FyZCJ9KQogICAgICAgIHdpdGggdXJsb3BlbihyZXEsIHRpbWVvdXQ9"
    "OCkgYXMgcjoKICAgICAgICAgICAgZGF0YSA9IF9qc29uLmxvYWRzKHIucmVhZCgpLmRlY29kZSgidXRmLTgiLCAicmVwbGFjZSIp"
    "KQogICAgICAgIGJhZCA9IGRhdGEuZ2V0KCJzdGF0dXMiKSA9PSAic3VjY2VzcyIgYW5kICgKICAgICAgICAgICAgZGF0YS5nZXQo"
    "Imhvc3RpbmciKSBvciBkYXRhLmdldCgicHJveHkiKQogICAgICAgICkKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IGNv"
    "bC51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBpcH0sCiAgICAgICAgICAgICAgICB7IiRzZXQiOiB7ImJhZCI6"
    "IGJvb2woYmFkKSwgImlzcCI6IGRhdGEuZ2V0KCJpc3AiLCAiIiksCiAgICAgICAgICAgICAgICAgICAgICAgICAgInRzIjogdGlt"
    "ZSgpfX0sCiAgICAgICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICAgICAgICAgKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246"
    "CiAgICAgICAgICAgIHBhc3MKICAgICAgICBpZiBiYWQ6CiAgICAgICAgICAgIGF3YWl0IF9iYW5faXAoaXApCiAgICAgICAgICAg"
    "IHRyeToKICAgICAgICAgICAgICAgIGZyb20gLi4uIGltcG9ydCBMT0dHRVIKCiAgICAgICAgICAgICAgICBMT0dHRVIuZXJyb3Io"
    "CiAgICAgICAgICAgICAgICAgICAgZiJXWkZJWCBkYXNoYm9hcmQ6IGRhdGFjZW50ZXIvcHJveHkgSVAgYmFubmVkOiB7aXB9ICIK"
    "ICAgICAgICAgICAgICAgICAgICBmIih7ZGF0YS5nZXQoJ2lzcCcsICc/Jyl9KSIKICAgICAgICAgICAgICAgICkKICAgICAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICByZXR1cm4gYm9vbChiYWQpCiAgICBleGNl"
    "cHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBGYWxzZSAgIyBpbnRlbCB1bmF2YWlsYWJsZSAtPiBkbyBub3QgYmxvY2sKCgph"
    "c3luYyBkZWYgX2xvY2tvdXRfc3RhdGUoaXApOgogICAgIiIiUGVyc2lzdGVudCB0aWVyLWJhc2VkIGxvY2tvdXQuIFJldHVybnMg"
    "c2Vjb25kcyByZW1haW5pbmcgKDAgPSBvcGVuKS4iIiIKICAgIHRyeToKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9sb2Nrc1tf"
    "cGFydCgpXQogICAgICAgIGRvYyA9IGF3YWl0IGNvbC5maW5kX29uZSh7Il9pZCI6IF9ub3JtX2lwKGlwKX0pCiAgICAgICAgaWYg"
    "bm90IGRvYzoKICAgICAgICAgICAgcmV0dXJuIDAKICAgICAgICBpZiBkb2MuZ2V0KCJwZXJtIik6CiAgICAgICAgICAgIHJldHVy"
    "biAtMQogICAgICAgIHVudGlsID0gZG9jLmdldCgidW50aWwiKSBvciAwCiAgICAgICAgcmV0dXJuIG1heCh1bnRpbCAtIHRpbWUo"
    "KSwgMCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIDAKCgphc3luYyBkZWYgX3JlY29yZF9mYWlsKGlwKToK"
    "ICAgICIiIjMgZmFpbHMgLT4gZXNjYWxhdGluZyBsb2Nrb3V0OiAyNGgsIDcyaCwgN2QsIDMwZCwgcGVybWFuZW50LiIiIgogICAg"
    "dHJ5OgogICAgICAgIGZyb20gcHltb25nbyBpbXBvcnQgUmV0dXJuRG9jdW1lbnQgYXMgX1JECgogICAgICAgIGNvbCA9IF9kYigp"
    "Lnd6Zml4X2xvY2tzW19wYXJ0KCldCiAgICAgICAgZG9jID0gYXdhaXQgY29sLmZpbmRfb25lX2FuZF91cGRhdGUoCiAgICAgICAg"
    "ICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAgeyIkaW5jIjogeyJmYWlscyI6IDF9LCAiJHNldCI6IHsidHMi"
    "OiB0aW1lKCl9fSwKICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgICAgIHJldHVybl9kb2N1bWVudD1fUkQuQUZURVIs"
    "CiAgICAgICAgKQogICAgICAgIGZhaWxzID0gaW50KChkb2Mgb3Ige30pLmdldCgiZmFpbHMiKSBvciAwKQogICAgICAgIGV4dHJh"
    "ID0gaW50KChkb2Mgb3Ige30pLmdldCgiZXh0cmEiKSBvciAwKQogICAgICAgIGlmIGV4dHJhID4gMDoKICAgICAgICAgICAgIyBy"
    "ZXRyaWVzIGdyYW50ZWQgYnkgL2FsbG93OiBlYWNoIGZhaWwgY29uc3VtZXMgb25lOyB0aGUgbGFzdAogICAgICAgICAgICAjIG9u"
    "ZSBlc2NhbGF0ZXMgc3RyYWlnaHQgdG8gdGhlIE5FWFQgdGllciAoa2VwdCBhZnRlciAvYWxsb3cpCiAgICAgICAgICAgIGxlZnQg"
    "PSBleHRyYSAtIDEKICAgICAgICAgICAgdGllciA9IChpbnQoKGRvYyBvciB7fSkuZ2V0KCJ0aWVyIikgb3IgMCkpICsgMQogICAg"
    "ICAgICAgICBzZWNzID0gTE9DS19USUVSU19TW21pbih0aWVyIC0gMSwgbGVuKExPQ0tfVElFUlNfUykgLSAxKV0KICAgICAgICAg"
    "ICAgaWYgbGVmdCA8PSAwOgogICAgICAgICAgICAgICAgaWYgc2VjcyA8IDA6CiAgICAgICAgICAgICAgICAgICAgYXdhaXQgY29s"
    "LnVwZGF0ZV9vbmUoCiAgICAgICAgICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgeyIkc2V0IjogeyJwZXJtIjogVHJ1ZSwgImZhaWxzIjogMCwgInRpZXIiOiB0aWVyLCAiZXh0cmEiOiAwfX0sCiAg"
    "ICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgIHJldHVybiAtMSwgdGllcgogICAgICAgICAgICAgICAgYXdh"
    "aXQgY29sLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgICAgICAgICAgeyJfaWQiOiBfbm9ybV9pcChpcCl9LAogICAgICAgICAgICAg"
    "ICAgICAgIHsiJHNldCI6IHsidW50aWwiOiB0aW1lKCkgKyBzZWNzLCAiZmFpbHMiOiAwLCAidGllciI6IHRpZXIsCiAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICJleHRyYSI6IDB9fSwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIHJldHVy"
    "biBzZWNzLCB0aWVyCiAgICAgICAgICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBfbm9y"
    "bV9pcChpcCl9LAogICAgICAgICAgICAgICAgeyIkc2V0IjogeyJmYWlscyI6IDAsICJleHRyYSI6IGxlZnR9fSwKICAgICAgICAg"
    "ICAgKQogICAgICAgICAgICByZXR1cm4gMCwgbGVmdAogICAgICAgIGlmIGZhaWxzIDwgTE9HSU5fTUFYX0ZBSUxTOgogICAgICAg"
    "ICAgICByZXR1cm4gMCwgZmFpbHMKICAgICAgICB0aWVyID0gKGludCgoZG9jIG9yIHt9KS5nZXQoInRpZXIiKSBvciAwKSkgKyAx"
    "CiAgICAgICAgc2VjcyA9IExPQ0tfVElFUlNfU1ttaW4odGllciAtIDEsIGxlbihMT0NLX1RJRVJTX1MpIC0gMSldCiAgICAgICAg"
    "aWYgc2VjcyA8IDA6CiAgICAgICAgICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBfbm9y"
    "bV9pcChpcCl9LAogICAgICAgICAgICAgICAgeyIkc2V0IjogeyJwZXJtIjogVHJ1ZSwgImZhaWxzIjogMCwgInRpZXIiOiB0aWVy"
    "fX0sCiAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuIC0xLCB0aWVyCiAgICAgICAgYXdhaXQgY29sLnVwZGF0ZV9vbmUo"
    "CiAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAgeyIkc2V0IjogeyJ1bnRpbCI6IHRpbWUoKSAr"
    "IHNlY3MsICJmYWlscyI6IDAsICJ0aWVyIjogdGllcn19LAogICAgICAgICkKICAgICAgICByZXR1cm4gc2VjcywgdGllcgogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gMCwgMAoKCmRlZiBfcmVjb3JkX29rKGlwKToKICAgIHRyeToKICAgICAg"
    "ICBmcm9tIGFzeW5jaW8gaW1wb3J0IGdldF9ldmVudF9sb29wCgogICAgICAgIGNvbCA9IF9kYigpLnd6Zml4X2xvY2tzW19wYXJ0"
    "KCldCiAgICAgICAgZ2V0X2V2ZW50X2xvb3AoKS5jcmVhdGVfdGFzaygKICAgICAgICAgICAgY29sLnVwZGF0ZV9vbmUoCiAgICAg"
    "ICAgICAgICAgICB7Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgICAgICB7IiRzZXQiOiB7ImZhaWxzIjogMCwgInRp"
    "ZXIiOiAwLCAidW50aWwiOiAwfX0sCiAgICAgICAgICAgICkKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAg"
    "IHBhc3MKCgpkZWYgX2xvY2tfdHh0KHNlY3MpOgogICAgaWYgc2VjcyA8IDA6CiAgICAgICAgcmV0dXJuICJwZXJtYW5lbnRseSBi"
    "YW5uZWQiCiAgICBpZiBzZWNzID49IDg2NDAwOgogICAgICAgIHJldHVybiBmImxvY2tlZCBmb3Ige3NlY3MgLy8gODY0MDB9IGRh"
    "eShzKSIKICAgIHJldHVybiBmImxvY2tlZCBmb3Ige3NlY3MgLy8gMzYwMCArIDF9IGhvdXIocykiCgoKIyDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKIyBTdGF0ZSBidWlsZGluZyAo"
    "dXNlcnMsIGxpdmUgdGFza3MsIGdsb2JhbHMpCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKZGVmIF90YXNrc19zbmFwc2hvdCgpOgogICAgb3V0ID0gW10KICAgIHRyeToKICAg"
    "ICAgICBmcm9tIC4uLiBpbXBvcnQgdGFza19kaWN0CgogICAgICAgIGRlZiBzYWZlKGZuLCBkZWZhdWx0PSIiKToKICAgICAgICAg"
    "ICAgdHJ5OgogICAgICAgICAgICAgICAgdiA9IGZuKCkKICAgICAgICAgICAgICAgIHJldHVybiB2IGlmIGlzaW5zdGFuY2Uodiwg"
    "KGludCwgZmxvYXQsIHN0cikpIGVsc2UgZGVmYXVsdAogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAg"
    "ICAgcmV0dXJuIGRlZmF1bHQKCiAgICAgICAgZm9yIG1pZCwgdCBpbiBsaXN0KHRhc2tfZGljdC5pdGVtcygpKToKICAgICAgICAg"
    "ICAgdHJ5OgogICAgICAgICAgICAgICAgbHN0ID0gZ2V0YXR0cih0LCAibGlzdGVuZXIiLCBOb25lKQogICAgICAgICAgICAgICAg"
    "b3V0LmFwcGVuZCgKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJtaWQiOiBtaWQsCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICJnaWQiOiBzYWZlKGxhbWJkYTogdC5naWQoKSwgIiIpLAogICAgICAgICAgICAgICAgICAgICAg"
    "ICAibmFtZSI6IHN0cihzYWZlKGxhbWJkYTogdC5uYW1lKCksICJ0YXNrIikpWzo5MF0sCiAgICAgICAgICAgICAgICAgICAgICAg"
    "ICJ1aWQiOiBpbnQoc2FmZShsYW1iZGE6IGxzdC51c2VyX2lkLCAwKSBvciAwKSwKICAgICAgICAgICAgICAgICAgICAgICAgInRh"
    "ZyI6IHN0cihzYWZlKGxhbWJkYTogbHN0LnRhZywgIiIpIG9yICIiKSwKICAgICAgICAgICAgICAgICAgICAgICAgInNpemUiOiBp"
    "bnQoc2FmZShsYW1iZGE6IHQuc2l6ZSgpLCAwKSBvciAwKSwKICAgICAgICAgICAgICAgICAgICAgICAgInByb2MiOiBpbnQoc2Fm"
    "ZShsYW1iZGE6IHQucHJvY2Vzc2VkX2J5dGVzKCksIDApIG9yIDApLAogICAgICAgICAgICAgICAgICAgICAgICAicGN0Ijogc2Fm"
    "ZShsYW1iZGE6IGZsb2F0KHN0cih0LnByb2dyZXNzKCkpLnJzdHJpcCgiJSIpKSwgMC4wKSwKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgInNwZWVkIjogc3RyKHNhZmUobGFtYmRhOiB0LnNwZWVkKCksICIiKSksCiAgICAgICAgICAgICAgICAgICAgICAgICJldGEi"
    "OiBzdHIoc2FmZShsYW1iZGE6IHQuZXRhKCksICIiKSksCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgKQog"
    "ICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgY29udGludWUKICAgICAgICBvdXQuc29ydChrZXk9"
    "bGFtYmRhIHg6IC14WyJzaXplIl0pCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiBvdXQKCgph"
    "c3luYyBkZWYgX2JvdF9pbmZvKCk6CiAgICBvdXQgPSB7Im5hbWUiOiAiIiwgInVuYW1lIjogIiJ9CiAgICB0cnk6CiAgICAgICAg"
    "ZnJvbSAuLi5jb3JlLnRnX2NsaWVudCBpbXBvcnQgVGdDbGllbnQKCiAgICAgICAgbWUgPSBhd2FpdCBUZ0NsaWVudC5ib3QuZ2V0"
    "X21lKCkKICAgICAgICBvdXRbIm5hbWUiXSA9IG1lLmZpcnN0X25hbWUgb3IgIiIKICAgICAgICBvdXRbInVuYW1lIl0gPSBtZS51"
    "c2VybmFtZSBvciAiIgogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gb3V0CgoKYXN5bmMgZGVm"
    "IF9zdGF0ZSgpOgogICAgcm93cyA9IGF3YWl0IHRvZGF5X3Jvd3MoKQogICAgdXNlZF9ieSA9IHtyWyJ1c2VyX2lkIl06IGludChy"
    "LmdldCgidXNlZCIpIG9yIDApIGZvciByIGluIHJvd3N9CiAgICBkb2NzID0gYXdhaXQgYWxsX3VzZXJzKCkKICAgIHVzZXJzID0g"
    "W10KICAgIGZvciBkIGluIGRvY3M6CiAgICAgICAgdHJ5OgogICAgICAgICAgICB1aWQgPSBpbnQoZFsiX2lkIl0pCiAgICAgICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgY29udGludWUKICAgICAgICBjYXAgPSBhd2FpdCBnZXRfY2FwX2J5dGVzKHVp"
    "ZCkKICAgICAgICB1c2VkID0gdXNlZF9ieS5nZXQodWlkLCAwKQogICAgICAgIHJlcyA9IGF3YWl0IHJlc2VydmVkX3RvZGF5KHVp"
    "ZCkKICAgICAgICB1c2Vycy5hcHBlbmQoCiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICJ1aWQiOiB1aWQsCiAgICAgICAg"
    "ICAgICAgICAidW5hbWUiOiBkLmdldCgidW5hbWUiKSBvciAiIiwKICAgICAgICAgICAgICAgICJuYW1lIjogZC5nZXQoIm5hbWUi"
    "KSBvciAiIiwKICAgICAgICAgICAgICAgICJjYXBfZ2IiOiBkLmdldCgiY2FwX2diIiksCiAgICAgICAgICAgICAgICAiY2FwIjog"
    "Y2FwLAogICAgICAgICAgICAgICAgInVzZWQiOiB1c2VkLAogICAgICAgICAgICAgICAgInJlc2VydmVkIjogcmVzLAogICAgICAg"
    "ICAgICAgICAgInBjdCI6IG1pbigxMDAuMCwgcm91bmQoMTAwLjAgKiAodXNlZCArIHJlcykgLyBjYXAsIDEpKSBpZiBjYXAgZWxz"
    "ZSAwLjAsCiAgICAgICAgICAgICAgICAiYmFubmVkIjogZC5nZXQoImNhcF9nYiIpID09IDAsCiAgICAgICAgICAgICAgICAibXVz"
    "aWNfbWF4IjogZC5nZXQoIm11c2ljX21heCIpLAogICAgICAgICAgICAgICAgInRhc2tzIjogaW50KGQuZ2V0KCJ0YXNrcyIpIG9y"
    "IDApLAogICAgICAgICAgICAgICAgInRvdGFsIjogaW50KGQuZ2V0KCJ0b3RhbF91c2VkIikgb3IgMCksCiAgICAgICAgICAgIH0K"
    "ICAgICAgICApCiAgICB1c2Vycy5zb3J0KGtleT1sYW1iZGEgdTogLSh1WyJ1c2VkIl0gKyB1WyJyZXNlcnZlZCJdKSkKICAgIGFj"
    "Y2VzcyA9IFtdCiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi4gaW1wb3J0IHN1ZG9fdXNlcnMgYXMgX3N1ZG9fY2ZnCiAgICAgICAg"
    "ZnJvbSAuLi4gaW1wb3J0IHVzZXJfZGF0YSBhcyBfdWQKCiAgICAgICAgZnJvbSAucjFfY29yZSBpbXBvcnQgX2RiLCBfcGFydAoK"
    "ICAgICAgICBjdXIgPSBfZGIoKS53emZpeF9zdGFydHVzZXJzW19wYXJ0KCldLmZpbmQoe30pLnNvcnQoImxhc3QiLCAtMSkubGlt"
    "aXQoNTApCiAgICAgICAgYXN5bmMgZm9yIGQgaW4gY3VyOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICB1aWQgPSBp"
    "bnQoZFsiX2lkIl0pCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAg"
    "ICAgICB1ZCA9IF91ZC5nZXQodWlkKSBvciB7fQogICAgICAgICAgICBhY2Nlc3MuYXBwZW5kKAogICAgICAgICAgICAgICAgewog"
    "ICAgICAgICAgICAgICAgICAgICJ1aWQiOiB1aWQsCiAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiBkLmdldCgibmFtZSIpIG9y"
    "ICIiLAogICAgICAgICAgICAgICAgICAgICJ1bmFtZSI6IGQuZ2V0KCJ1bmFtZSIpIG9yICIiLAogICAgICAgICAgICAgICAgICAg"
    "ICJzdGFydHMiOiBpbnQoZC5nZXQoInN0YXJ0cyIpIG9yIDApLAogICAgICAgICAgICAgICAgICAgICJsYXN0IjogZC5nZXQoImxh"
    "c3QiKSBvciAwLAogICAgICAgICAgICAgICAgICAgICJhdXRoIjogYm9vbCh1ZC5nZXQoIkFVVEgiKSksCiAgICAgICAgICAgICAg"
    "ICAgICAgInN1ZG8iOiBib29sKHVkLmdldCgiU1VETyIpKSBvciB1aWQgaW4gX3N1ZG9fY2ZnLAogICAgICAgICAgICAgICAgICAg"
    "ICJibCI6IGJvb2wodWQuZ2V0KCJCTEFDS0xJU1QiKSksCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICkKICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIHsKICAgICAgICAib2siOiBUcnVlLAogICAgICAgICJhY2Nlc3Mi"
    "OiBhY2Nlc3MsCiAgICAgICAgImRheSI6IF9kYXlfaXN0KCksCiAgICAgICAgImJvdCI6IGF3YWl0IF9ib3RfaW5mbygpLAogICAg"
    "ICAgICJ1c2VycyI6IHVzZXJzLAogICAgICAgICJ0YXNrcyI6IF90YXNrc19zbmFwc2hvdCgpLAogICAgICAgICJnbG9iYWxfY2Fw"
    "X2diIjogYXdhaXQgX2dldF9nbG9iYWxfY2FwX2diKCksCiAgICAgICAgImdsb2JhbF9tdXNpYyI6IGF3YWl0IF9nZXRfZ2xvYmFs"
    "X211c2ljKCksCiAgICAgICAgIm11c2ljX3NldHRpbmdzIjogYXdhaXQgX2dldF9tdXNpY19zZXR0aW5ncygpLAogICAgICAgICJ0"
    "b3RhbHMiOiB7CiAgICAgICAgICAgICJ1c2VkIjogc3VtKHVbInVzZWQiXSBmb3IgdSBpbiB1c2VycyksCiAgICAgICAgICAgICJy"
    "ZXNlcnZlZCI6IHN1bSh1WyJyZXNlcnZlZCJdIGZvciB1IGluIHVzZXJzKSwKICAgICAgICAgICAgInVzZXJzIjogbGVuKHVzZXJz"
    "KSwKICAgICAgICAgICAgInRhc2tzIjogbGVuKF90YXNrc19zbmFwc2hvdCgpKSwKICAgICAgICB9LAogICAgfQoKCiMg4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACiMgQWN0aW9ucwoj"
    "IOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFz"
    "eW5jIGRlZiBfa2lsbF90YXNrKG1pZCk6CiAgICBmcm9tIC4uLiBpbXBvcnQgdGFza19kaWN0CgogICAgdHJ5OgogICAgICAgIHQg"
    "PSB0YXNrX2RpY3QuZ2V0KGludChtaWQpKQogICAgICAgIGlmIHQgaXMgTm9uZToKICAgICAgICAgICAgcmV0dXJuIEZhbHNlLCAi"
    "dGFzayBub3QgZm91bmQiCiAgICAgICAgb2JqID0gdC50YXNrKCkKICAgICAgICBhd2FpdCBvYmouY2FuY2VsX3Rhc2soKQogICAg"
    "ICAgIHJldHVybiBUcnVlLCAiY2FuY2VsbGVkIgogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIHJldHVybiBGYWxz"
    "ZSwgc3RyKGUpWzoxMjBdCgoKYXN5bmMgZGVmIF9kZWR1Y3RfY2FwKHVpZCwgZ2IpOgogICAgIiIiTG93ZXIgYSB1c2VyJ3MgY2Fw"
    "IGJ5IEdCIChmbG9vciAwID0gZnVsbHkgYmxvY2tlZCkg4oCUIHNhbWUgYXMKICAgIFRlbGVncmFtIC9kZWR1Y3RjYXAuIiIiCiAg"
    "ICB0cnk6CiAgICAgICAgYW10ID0gZmxvYXQoZ2IpCiAgICAgICAgdWRvYyA9IGF3YWl0IGdldF91c2VyX2RvYyh1aWQpCiAgICAg"
    "ICAgY3VyID0gdWRvYy5nZXQoImNhcF9nYiIpCiAgICAgICAgYmFzZSA9IGZsb2F0KGN1cikgaWYgY3VyIGlzIG5vdCBOb25lIGVs"
    "c2UgYXdhaXQgX2dldF9nbG9iYWxfY2FwX2diKCkKICAgICAgICBuZXdfY2FwID0gbWF4KGJhc2UgLSBhbXQsIDAuMCkKICAgICAg"
    "ICBhd2FpdCBzZXRfdXNlcl9jYXAodWlkLCBuZXdfY2FwKQogICAgICAgIGlmIG5ld19jYXAgPD0gMDoKICAgICAgICAgICAgcmV0"
    "dXJuIFRydWUsIGYiY2FwIGZvciB7dWlkfSDihpIgMCBHQiAoYmxvY2tlZCkiCiAgICAgICAgcmV0dXJuIFRydWUsIGYiY2FwIGZv"
    "ciB7dWlkfSDihpIge19mbXRfZ2IobmV3X2NhcCl9IgogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIHJldHVybiBG"
    "YWxzZSwgc3RyKGUpWzoxMjBdCgoKYXN5bmMgZGVmIGJ1aWxkX3JlcG9ydCgpOgogICAgIiIiRGFpbHkgdXNhZ2UgdGV4dCBmb3Ig"
    "dGhlIGxvZyBjaGF0LiIiIgogICAgcm93cyA9IGF3YWl0IHRvZGF5X3Jvd3MoKQogICAgZG9jcyA9IHtpbnQoZFsiX2lkIl0pOiBk"
    "IGZvciBkIGluIGF3YWl0IGFsbF91c2VycygpfQogICAgbGluZXMgPSBbZiLwn5OKIDxiPldaRklYIGRhaWx5IHJlcG9ydCDigJQg"
    "e19kYXlfaXN0KCl9PC9iPiIsICLilIIiXQogICAgdG90YWwgPSAwCiAgICBmb3IgciBpbiByb3dzWzoyNV06CiAgICAgICAgdWlk"
    "ID0gclsidXNlcl9pZCJdCiAgICAgICAgZCA9IGRvY3MuZ2V0KHVpZCwge30pCiAgICAgICAgdGFnID0gZC5nZXQoInVuYW1lIikg"
    "b3IgZC5nZXQoIm5hbWUiKSBvciBzdHIodWlkKQogICAgICAgIGNhcF9nYiA9IGQuZ2V0KCJjYXBfZ2IiKQogICAgICAgIGNhcF90"
    "eHQgPSBmIntfZm10X2diKGNhcF9nYil9IiBpZiBjYXBfZ2IgZWxzZSAiZGVmYXVsdCIKICAgICAgICBsaW5lcy5hcHBlbmQoZiLi"
    "lKAgPGI+e3RhZ308L2I+IOKGkiB7X25pY2Vfc2l6ZShyWyd1c2VkJ10pfSAvIHtjYXBfdHh0fSIpCiAgICAgICAgdG90YWwgKz0g"
    "ci5nZXQoInVzZWQiKSBvciAwCiAgICBpZiBub3Qgcm93czoKICAgICAgICBsaW5lcy5hcHBlbmQoIuKUliBubyB1c2FnZSB0b2Rh"
    "eSIpCiAgICBlbHNlOgogICAgICAgIGxpbmVzWzFdID0gZiLilIIgdG90YWwgPGI+e19uaWNlX3NpemUodG90YWwpfTwvYj4gwrcg"
    "e2xlbihyb3dzKX0gdXNlcihzKSIKICAgICAgICBsaW5lcy5hcHBlbmQoIuKUliByZXNldHMgYXQgMDA6MDAgSVNUIikKICAgIHJl"
    "dHVybiAiXG4iLmpvaW4obGluZXMpWzpSRVBPUlRfTUFYX0NIQVJTXQoKCmFzeW5jIGRlZiBzZW5kX3JlcG9ydCgpOgogICAgdHJ5"
    "OgogICAgICAgIGZyb20gLi4uY29yZS5jb25maWdfbWFuYWdlciBpbXBvcnQgQ29uZmlnCiAgICAgICAgZnJvbSAuLi5jb3JlLnRn"
    "X2NsaWVudCBpbXBvcnQgVGdDbGllbnQKCiAgICAgICAgY2hhdCA9IHN0cihnZXRhdHRyKENvbmZpZywgIkxPR19DSEFUIiwgIiIp"
    "IG9yICIiKS5zdHJpcCgpCiAgICAgICAgaWYgbm90IGNoYXQ6CiAgICAgICAgICAgICMgTE9HX0NIQVQgbm90IHNldCAtPiBkZWxp"
    "dmVyIHRvIHRoZSBvd25lcidzIERNIGluc3RlYWQKICAgICAgICAgICAgY2hhdCA9IHN0cihnZXRhdHRyKENvbmZpZywgIk9XTkVS"
    "X0lEIiwgIiIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAgICAgIGlmIG5vdCBjaGF0OgogICAgICAgICAgICAgICAgcmV0dXJuIEZh"
    "bHNlLCAiTE9HX0NIQVQgYW5kIE9XTkVSX0lEIG5vdCBzZXQiCiAgICAgICAgdGV4dCA9IGF3YWl0IGJ1aWxkX3JlcG9ydCgpCiAg"
    "ICAgICAgYXdhaXQgVGdDbGllbnQuYm90LnNlbmRfbWVzc2FnZSgKICAgICAgICAgICAgY2hhdF9pZD1fY2hhdF9pbnQoY2hhdCks"
    "IHRleHQ9dGV4dCwgZGlzYWJsZV93ZWJfcGFnZV9wcmV2aWV3PVRydWUKICAgICAgICApCiAgICAgICAgcmV0dXJuIFRydWUsICJy"
    "ZXBvcnQgc2VudCIKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICByZXR1cm4gRmFsc2UsIHN0cihlKVs6MTIwXQoK"
    "CmFzeW5jIGRlZiBkYWlseV9yZXBvcnRfbG9vcCgpOgogICAgIiIiU2VuZCB0aGUgdXNhZ2UgcmVwb3J0IHRvIHRoZSBsb2cgY2hh"
    "dCBhdCAyMzo1NyBJU1QgZXZlcnkgZGF5LiIiIgogICAgd2hpbGUgVHJ1ZToKICAgICAgICB0cnk6CiAgICAgICAgICAgIG5vdyA9"
    "IGRhdGV0aW1lLm5vdyhJU1QpCiAgICAgICAgICAgIHRhcmdldCA9IG5vdy5yZXBsYWNlKGhvdXI9MjMsIG1pbnV0ZT01Nywgc2Vj"
    "b25kPTAsIG1pY3Jvc2Vjb25kPTApCiAgICAgICAgICAgIGlmIHRhcmdldCA8PSBub3c6CiAgICAgICAgICAgICAgICB0YXJnZXQg"
    "Kz0gdGltZWRlbHRhKGRheXM9MSkKICAgICAgICAgICAgYXdhaXQgYWlvc2xlZXAoKHRhcmdldCAtIG5vdykudG90YWxfc2Vjb25k"
    "cygpICsgNSkKICAgICAgICAgICAgb2ssIG1zZyA9IGF3YWl0IHNlbmRfcmVwb3J0KCkKICAgICAgICAgICAgaWYgbm90IG9rOgog"
    "ICAgICAgICAgICAgICAgZnJvbSAuLi4gaW1wb3J0IExPR0dFUgoKICAgICAgICAgICAgICAgIExPR0dFUi53YXJuaW5nKGYiV1pG"
    "SVggZGFpbHkgcmVwb3J0IGZhaWxlZDoge21zZ30iKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIGF3YWl0"
    "IGFpb3NsZWVwKDM2MDApCgoKIyBjb21tYW5kIGRlc2NyaXB0aW9ucyBmb3IgdGhlIFRlbGVncmFtICIvIiBtZW51ICh2MTUuOSkK"
    "X0NNRF9ERVNDID0gewogICAgInN0YXJ0IjogIlN0YXJ0IHRoZSBib3QiLCAiaGVscCI6ICJDb21tYW5kIGhlbHAiLAogICAgImxv"
    "Z2luIjogIkxvZ2luIHRva2VuIiwgInBpbmciOiAiQm90IGxhdGVuY3kiLAogICAgIm1pcnJvciI6ICJNaXJyb3IgYSBsaW5rIHRv"
    "IGNsb3VkIiwgIm0iOiAiTWlycm9yIGEgbGluayAoc2hvcnQpIiwKICAgICJsZWVjaCI6ICJEb3dubG9hZCAmIHVwbG9hZCB0byBU"
    "ZWxlZ3JhbSIsICJsIjogIkxlZWNoIGEgbGluayAoc2hvcnQpIiwKICAgICJxYm1pcnJvciI6ICJNaXJyb3IgdmlhIHFCaXR0b3Jy"
    "ZW50IiwgInFtIjogInFCaXQgbWlycm9yIChzaG9ydCkiLAogICAgInFibGVlY2giOiAiTGVlY2ggdmlhIHFCaXR0b3JyZW50Iiwg"
    "InFsIjogInFCaXQgbGVlY2ggKHNob3J0KSIsCiAgICAieXRkbCI6ICJZVC1ETFAgbWlycm9yIiwgInkiOiAiWVQtRExQIG1pcnJv"
    "ciAoc2hvcnQpIiwKICAgICJ5dGRsbGVlY2giOiAiWVQtRExQIGxlZWNoIiwgInlsIjogIllULURMUCBsZWVjaCAoc2hvcnQpIiwK"
    "ICAgICJqZG1pcnJvciI6ICJKRG93bmxvYWRlciBtaXJyb3IiLCAiam0iOiAiSkRvd25sb2FkZXIgbWlycm9yIChzaG9ydCkiLAog"
    "ICAgImpkbGVlY2giOiAiSkRvd25sb2FkZXIgbGVlY2giLCAiamwiOiAiSkRvd25sb2FkZXIgbGVlY2ggKHNob3J0KSIsCiAgICAi"
    "bnpibWlycm9yIjogIk5aQiBtaXJyb3IiLCAibm0iOiAiTlpCIG1pcnJvciAoc2hvcnQpIiwKICAgICJuemJsZWVjaCI6ICJOWkIg"
    "bGVlY2giLCAibmwiOiAiTlpCIGxlZWNoIChzaG9ydCkiLAogICAgInNlZWRybGluayI6ICJTZWVkciBsaW5rIHRyYW5zZmVyIiwg"
    "InNsaW5rIjogIlNlZWRyIGxpbmsgKHNob3J0KSIsCiAgICAic3JsaW5rIjogIlNlZWRyIGxpbmsgKHNob3J0KSIsCiAgICAiY2xv"
    "bmUiOiAiQ2xvbmUgY2xvdWQgdHJhbnNmZXJzIiwgImNsIjogIkNsb25lIChzaG9ydCkiLAogICAgImNvdW50IjogIkNvdW50IGNs"
    "b3VkIGZpbGVzL2ZvbGRlcnMiLCAiZGVsIjogIkRlbGV0ZSBjbG91ZCBmaWxlcyIsCiAgICAibGlzdCI6ICJMaXN0IGNsb3VkIGZp"
    "bGVzIiwgInNlYXJjaCI6ICJTZWFyY2ggZmlsZXMiLAogICAgInVzZXJzIjogIkF1dGhvcml6ZWQgdXNlcnMgbGlzdCIsCiAgICAi"
    "Y2FuY2VsIjogIkNhbmNlbCBhIHRhc2siLCAiYyI6ICJDYW5jZWwgYSB0YXNrIChzaG9ydCkiLAogICAgImNhbmNlbGFsbCI6ICJD"
    "YW5jZWwgdGFza3MgaW4gYnVsayIsICJjYWxsIjogIkNhbmNlbCBhbGwgKHNob3J0KSIsCiAgICAiZm9yY2VzdGFydCI6ICJGb3Jj"
    "ZSBzdGFydCBhIHF1ZXVlZCB0YXNrIiwgImZzIjogIkZvcmNlIHN0YXJ0IChzaG9ydCkiLAogICAgInN0YXR1cyI6ICJBY3RpdmUg"
    "dGFzayBzdGF0dXMiLCAicyI6ICJTdGF0dXMgKHNob3J0KSIsCiAgICAic3RhdHVzYWxsIjogIlN0YXR1cyBvZiBhbGwgdXNlcnMi"
    "LAogICAgInN0cmVhbSI6ICJTdHJlYW0gbGluayBmb3IgYSBmaWxlIiwgInNsIjogIlN0cmVhbSBsaW5rIChzaG9ydCkiLAogICAg"
    "InJlc3RhcnQiOiAiUmVzdGFydCB0aGUgYm90IiwgInIiOiAiUmVzdGFydCAoc2hvcnQpIiwKICAgICJyZXN0YXJ0YWxsIjogIlJl"
    "c3RhcnQgYWxsIGJvdHMiLCAicmVzdGFydHNlcyI6ICJSZXN0YXJ0IHNlc3Npb25zIiwKICAgICJicm9hZGNhc3QiOiAiQnJvYWRj"
    "YXN0IGEgbWVzc2FnZSIsICJiYyI6ICJCcm9hZGNhc3QgKHNob3J0KSIsCiAgICAic3RhdHMiOiAiU2VydmVyIHN0YXRzIiwgInN0"
    "IjogIlN0YXRzIChzaG9ydCkiLAogICAgImxvZyI6ICJCb3QgbG9nIGZpbGUiLCAic2hlbGwiOiAiUnVuIGEgc2hlbGwgY29tbWFu"
    "ZCIsCiAgICAiYWV4ZWMiOiAiUnVuIGFzeW5jIHB5dGhvbiIsICJleGVjIjogIlJ1biBweXRob24iLAogICAgImNsZWFybG9jYWxz"
    "IjogIkNsZWFyIHN0b3JlZCB2YXJzIiwKICAgICJyc3MiOiAiUlNTIGZlZWQgbWFuYWdlciIsCiAgICAiYWRkaW1hZ2UiOiAiQWRk"
    "IGEgY3VzdG9tIGltYWdlIiwgImFpIjogIkFkZCBpbWFnZSAoc2hvcnQpIiwKICAgICJpbWFnZXMiOiAiTGlzdCBjdXN0b20gaW1h"
    "Z2VzIiwgImltZyI6ICJJbWFnZXMgKHNob3J0KSIsCiAgICAiYXV0aG9yaXplIjogIkF1dGhvcml6ZSBhIHVzZXIiLCAiYSI6ICJB"
    "dXRob3JpemUgKHNob3J0KSIsCiAgICAidW5hdXRob3JpemUiOiAiVW5hdXRob3JpemUgYSB1c2VyIiwgInVhIjogIlVuYXV0aG9y"
    "aXplIChzaG9ydCkiLAogICAgImFkZHN1ZG8iOiAiR3JhbnQgc3VkbyBhY2Nlc3MiLCAiYXMiOiAiQWRkIHN1ZG8gKHNob3J0KSIs"
    "CiAgICAicm1zdWRvIjogIlJldm9rZSBzdWRvIGFjY2VzcyIsICJycyI6ICJSbSBzdWRvIChzaG9ydCkiLAogICAgImJsYWNrbGlz"
    "dCI6ICJCbGFja2xpc3QgYSB1c2VyIiwgImJsIjogIkJsYWNrbGlzdCAoc2hvcnQpIiwKICAgICJybWJsYWNrbGlzdCI6ICJVbi1i"
    "bGFja2xpc3QgYSB1c2VyIiwgInJibCI6ICJSbSBibGFja2xpc3QgKHNob3J0KSIsCiAgICAiYnNldHRpbmciOiAiQm90IHNldHRp"
    "bmdzIiwgImJzIjogIkJvdCBzZXR0aW5ncyAoc2hvcnQpIiwKICAgICJ1c2V0dGluZyI6ICJVc2VyIHNldHRpbmdzIiwgInVzIjog"
    "IlVzZXIgc2V0dGluZ3MgKHNob3J0KSIsCiAgICAic2VsZWN0IjogIlNlbGVjdCB0b3JyZW50IGZpbGVzIiwgInNlbCI6ICJTZWxl"
    "Y3QgKHNob3J0KSIsCiAgICAiY2F0ZWdvcnkiOiAiQ2F0ZWdvcnkgc2VsZWN0b3IiLCAiY3RzZWwiOiAiQ2F0ZWdvcnkgKHNob3J0"
    "KSIsCiAgICAiZ2RjbGVhbiI6ICJDbGVhbiBjbG91ZCBkcml2ZSIsICJnZGMiOiAiR0RDbGVhbiAoc2hvcnQpIiwKICAgICJwbHVn"
    "aW5zIjogIk1hbmFnZSBwbHVnaW5zIiwKICAgICJtZW1vcnkiOiAiU2hvdyBib3QgbWVtb3J5IiwgIm1lbSI6ICJNZW1vcnkgKHNo"
    "b3J0KSIsCiAgICAidXBob3N0ZXIiOiAiVXBsb2FkIGZyb20gVVJMIiwgInVwIjogIlVwbG9hZCAoc2hvcnQpIiwKICAgICJmaW5k"
    "IjogIlNlYXJjaCB5b3VyIGRvd25sb2FkcyIsICJ1c2FnZSI6ICJZb3VyIGJhbmR3aWR0aCB1c2FnZSIsCn0KX09XTkVSX0RFU0Mg"
    "PSB7CiAgICAicXVzZXJzIjogIlVzZXJzICsgdXNhZ2Ugb3ZlcnZpZXciLCAic2V0Y2FwIjogIlNldCBhIHVzZXIgY2FwIiwKICAg"
    "ICJhZGRjYXAiOiAiUmFpc2UgYSB1c2VyIGNhcCIsICJkZWR1Y3RjYXAiOiAiTG93ZXIgYSB1c2VyIGNhcCIsCiAgICAiZGVsdXNl"
    "ciI6ICJSZW1vdmUgYSB1c2VyIiwgInJlc2V0Y2FwIjogIlJlc2V0IHRvZGF5J3MgdXNhZ2UiLAogICAgImJvdGNhcCI6ICJEZWZh"
    "dWx0IGNhcCBmb3IgYWxsIiwgImRic3RhdHMiOiAiTW9uZ29EQiBzaXplcyIsCiAgICAiZGJjbGVhbiI6ICJDbGVhbiBleHBpcmVk"
    "IERCIHJvd3MiLCAid3pmaXhkaWFnIjogIlF1b3RhIGRpYWdub3N0aWNzIiwKICAgICJhZG1pbnBhc3MiOiAiV2ViIGRhc2hib2Fy"
    "ZCBwYXNzd29yZCIsICJhbGxvdyI6ICJVbmJhbiBhIGRhc2hib2FyZCBJUCIsCiAgICAiYmFucyI6ICJMaXN0IGRhc2hib2FyZCBi"
    "YW5zIiwgImxvY2tkYXNoIjogIkxvY2svdW5sb2NrIGRhc2hib2FyZCIsCiAgICAic3RyZWFtcGFzcyI6ICJQZXItbGluayBzdHJl"
    "YW0gcGFzc3dvcmRzIiwKfQoKCmFzeW5jIGRlZiBzZXRfYm90X2NvbW1hbmRzKCk6CiAgICAiIiJQdWJsaXNoIHRoZSBmdWxsIGNv"
    "bW1hbmQgbGlzdCB0byB0aGUgVGVsZWdyYW0gIi8iIG1lbnUg4oCUIGRlZmF1bHQKICAgIHNjb3BlIGZvciB1c2Vycywgb3duZXIg"
    "c2NvcGUgd2l0aCBldmVyeSBhZG1pbiBjb21tYW5kLiIiIgogICAgdHJ5OgogICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgc2xl"
    "ZXAKCiAgICAgICAgYXdhaXQgc2xlZXAoOTApICAjIHdhaXQgZm9yIHRoZSBib3QgdG8gYmUgdXAKICAgICAgICBmcm9tIHB5cm9n"
    "cmFtLnR5cGVzIGltcG9ydCBCb3RDb21tYW5kLCBCb3RDb21tYW5kU2NvcGVDaGF0LCBCb3RDb21tYW5kU2NvcGVEZWZhdWx0Cgog"
    "ICAgICAgIGZyb20gLi4uIGltcG9ydCBMT0dHRVIKICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVu"
    "dCwgQ29uZmlnCgogICAgICAgICMgZ2F0aGVyIGV2ZXJ5IGNvbW1hbmQgbmFtZSBrbm93biB0byB0aGUgYm90CiAgICAgICAgdHJ5"
    "OgogICAgICAgICAgICBmcm9tIC4udGVsZWdyYW1faGVscGVyLmJvdF9jb21tYW5kcyBpbXBvcnQgQm90Q29tbWFuZHMKCiAgICAg"
    "ICAgICAgIGFsbF9uYW1lcyA9IHNldCgpCiAgICAgICAgICAgIGZvciBfdiBpbiBCb3RDb21tYW5kcy5nZXRfY29tbWFuZHMoKS52"
    "YWx1ZXMoKToKICAgICAgICAgICAgICAgIG5hbWVzID0gX3YgaWYgaXNpbnN0YW5jZShfdiwgbGlzdCkgZWxzZSBbX3ZdCiAgICAg"
    "ICAgICAgICAgICBhbGxfbmFtZXMudXBkYXRlKG5hbWVzKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIGFs"
    "bF9uYW1lcyA9IHNldCgpCiAgICAgICAgYWxsX25hbWVzLnVwZGF0ZShfT1dORVJfREVTQykKICAgICAgICBhbGxfbmFtZXMudXBk"
    "YXRlKF9DTURfREVTQykKCiAgICAgICAgdXNlcl9jbWRzLCBvd25lcl9jbWRzID0gW10sIFtdCiAgICAgICAgZm9yIG5hbWUgaW4g"
    "c29ydGVkKGFsbF9uYW1lcyk6CiAgICAgICAgICAgIGlmIG5hbWUgaW4gKCJzaGVsbCIsICJhZXhlYyIsICJleGVjIiwgImNsZWFy"
    "bG9jYWxzIik6CiAgICAgICAgICAgICAgICBjb250aW51ZSAgIyBrZWVwIHRoZSBkYW5nZXJvdXMgb25lcyBvdXQgb2YgdGhlIG1l"
    "bnUKICAgICAgICAgICAgZGVzYyA9IF9DTURfREVTQy5nZXQobmFtZSkgb3IgX09XTkVSX0RFU0MuZ2V0KG5hbWUpIG9yICJXWk1M"
    "LVggY29tbWFuZCIKICAgICAgICAgICAgb3duZXJfY21kcy5hcHBlbmQoQm90Q29tbWFuZChjb21tYW5kPW5hbWUsIGRlc2NyaXB0"
    "aW9uPWRlc2MpKQogICAgICAgICAgICBpZiBuYW1lIG5vdCBpbiBfT1dORVJfREVTQyBhbmQgbm90IG5hbWUuc3RhcnRzd2l0aCgi"
    "d3oiKToKICAgICAgICAgICAgICAgIHVzZXJfY21kcy5hcHBlbmQoQm90Q29tbWFuZChjb21tYW5kPW5hbWUsIGRlc2NyaXB0aW9u"
    "PWRlc2MpKQoKICAgICAgICBhc3luYyBkZWYgX3NldF9jbWRzX2h0dHAoY21kcywgc2NvcGU9Tm9uZSk6CiAgICAgICAgICAgICIi"
    "InNldE15Q29tbWFuZHMgdmlhIHRoZSByYXcgQm90IEFQSSDigJQgc29tZSBjbGllbnQgZm9ya3MKICAgICAgICAgICAgKHd6Z3Jh"
    "bSkgbGFjayBDbGllbnQuc2V0X215X2NvbW1hbmRzIGVudGlyZWx5LiIiIgogICAgICAgICAgICBpbXBvcnQganNvbiBhcyBfanNv"
    "bgoKICAgICAgICAgICAgZnJvbSBhaW9odHRwIGltcG9ydCBDbGllbnRTZXNzaW9uCgogICAgICAgICAgICB0b2tlbiA9IHN0cihn"
    "ZXRhdHRyKENvbmZpZywgIkJPVF9UT0tFTiIsICIiKSBvciAiIikuc3RyaXAoKQogICAgICAgICAgICBpZiBub3QgdG9rZW46CiAg"
    "ICAgICAgICAgICAgICByYWlzZSBSdW50aW1lRXJyb3IoIkJPVF9UT0tFTiBub3Qgc2V0IikKICAgICAgICAgICAgYm9keSA9IHsK"
    "ICAgICAgICAgICAgICAgICJjb21tYW5kcyI6IFsKICAgICAgICAgICAgICAgICAgICB7ImNvbW1hbmQiOiBjLmNvbW1hbmQsICJk"
    "ZXNjcmlwdGlvbiI6IGMuZGVzY3JpcHRpb259CiAgICAgICAgICAgICAgICAgICAgZm9yIGMgaW4gY21kcwogICAgICAgICAgICAg"
    "ICAgXQogICAgICAgICAgICB9CiAgICAgICAgICAgIGlmIHNjb3BlIGlzIG5vdCBOb25lOgogICAgICAgICAgICAgICAgYm9keVsi"
    "c2NvcGUiXSA9IHNjb3BlCiAgICAgICAgICAgIGFzeW5jIHdpdGggQ2xpZW50U2Vzc2lvbigpIGFzIF9zOgogICAgICAgICAgICAg"
    "ICAgYXN5bmMgd2l0aCBfcy5wb3N0KAogICAgICAgICAgICAgICAgICAgIGYiaHR0cHM6Ly9hcGkudGVsZWdyYW0ub3JnL2JvdHt0"
    "b2tlbn0vc2V0TXlDb21tYW5kcyIsCiAgICAgICAgICAgICAgICAgICAganNvbj1ib2R5LAogICAgICAgICAgICAgICAgKSBhcyBf"
    "cjoKICAgICAgICAgICAgICAgICAgICBfaiA9IGF3YWl0IF9yLmpzb24oY29udGVudF90eXBlPU5vbmUpCiAgICAgICAgICAgICAg"
    "ICAgICAgaWYgbm90IF9qLmdldCgib2siKToKICAgICAgICAgICAgICAgICAgICAgICAgcmFpc2UgUnVudGltZUVycm9yKHN0cihf"
    "ai5nZXQoImRlc2NyaXB0aW9uIikpWzoxMjBdKQoKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IFRnQ2xpZW50LmJvdC5z"
    "ZXRfbXlfY29tbWFuZHMoCiAgICAgICAgICAgICAgICB1c2VyX2NtZHMgb3IgW0JvdENvbW1hbmQoInN0YXJ0IiwgIlN0YXJ0IHRo"
    "ZSBib3QiKV0KICAgICAgICAgICAgKQogICAgICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Quc2V0X215X2NvbW1hbmRzKAogICAg"
    "ICAgICAgICAgICAgb3duZXJfY21kcywKICAgICAgICAgICAgICAgIHNjb3BlPUJvdENvbW1hbmRTY29wZUNoYXQoY2hhdF9pZD1D"
    "b25maWcuT1dORVJfSUQpLAogICAgICAgICAgICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBfZToKICAgICAgICAgICAg"
    "IyBjbGllbnQgbWV0aG9kIG1pc3NpbmcvYnJva2VuIC0+IHJhdyBCb3QgQVBJCiAgICAgICAgICAgIGF3YWl0IF9zZXRfY21kc19o"
    "dHRwKAogICAgICAgICAgICAgICAgdXNlcl9jbWRzIG9yIFtCb3RDb21tYW5kKCJzdGFydCIsICJTdGFydCB0aGUgYm90IildCiAg"
    "ICAgICAgICAgICkKICAgICAgICAgICAgYXdhaXQgX3NldF9jbWRzX2h0dHAoCiAgICAgICAgICAgICAgICBvd25lcl9jbWRzLCBz"
    "Y29wZT17InR5cGUiOiAiY2hhdCIsICJjaGF0X2lkIjogQ29uZmlnLk9XTkVSX0lEfQogICAgICAgICAgICApCiAgICAgICAgTE9H"
    "R0VSLmluZm8oCiAgICAgICAgICAgIGYiV1pGSVg6IGNvbW1hbmQgbWVudSBzZXQgKHtsZW4odXNlcl9jbWRzKX0gdXNlciwgIgog"
    "ICAgICAgICAgICBmIntsZW4ob3duZXJfY21kcyl9IG93bmVyIGNvbW1hbmRzKSIKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uIGFzIGU6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBmcm9tIC4uLiBpbXBvcnQgTE9HR0VSCgogICAgICAgICAgICBMT0dH"
    "RVIud2FybmluZyhmIldaRklYIHNldF9ib3RfY29tbWFuZHMgZmFpbGVkOiB7ZX0iKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246"
    "CiAgICAgICAgICAgIHBhc3MKCgphc3luYyBkZWYgX2xvZ19zdGFydHVwKCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAucjFfY29y"
    "ZSBpbXBvcnQgYWRtaW5fbG9nCgogICAgICAgIGF3YWl0IGFkbWluX2xvZygKICAgICAgICAgICAgIvCfn6IgPGI+Qm90IHN0YXJ0"
    "ZWQ8L2I+IiwKICAgICAgICAgICAgIuKUjyBXWk1MLVggKyBXWkZJWCBhcmUgdXBcbuKUliBTZXNzaW9uIGxvY2sgYW5kIHdhdGNo"
    "ZG9nIGFjdGl2ZSIsCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKZGVmIHN0YXJ0X2xvb3Bz"
    "KCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi4gaW1wb3J0IGJvdF9sb29wCgogICAgICAgIGJvdF9sb29wLmNyZWF0ZV90YXNr"
    "KGRhaWx5X3JlcG9ydF9sb29wKCkpCiAgICAgICAgYm90X2xvb3AuY3JlYXRlX3Rhc2soc2V0X2JvdF9jb21tYW5kcygpKQogICAg"
    "ICAgIGJvdF9sb29wLmNyZWF0ZV90YXNrKF9sb2dfc3RhcnR1cCgpKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNz"
    "CgoKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAK"
    "IyBIVFRQIGhhbmRsZXJzIChyZWdpc3RlcmVkIG9uIHRoZSBzdHJlYW0gc2VydmVyJ3MgYWlvaHR0cCBhcHApCiMg4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIHd6"
    "YWRtaW5fcGFnZShyZXF1ZXN0KToKICAgIGlwID0gX25vcm1faXAoX2NsaWVudF9pcChyZXF1ZXN0KSkKICAgIGlmIG5vdCBfdWFf"
    "b2socmVxdWVzdCkgb3IgYXdhaXQgX2lzX2Jhbm5lZChpcCk6CiAgICAgICAgcmV0dXJuIHdlYi5SZXNwb25zZShzdGF0dXM9NDAz"
    "LCB0ZXh0PSJmb3JiaWRkZW4iKQogICAgaWYgbm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgIHJldHVybiB3ZWIuUmVz"
    "cG9uc2UoCiAgICAgICAgICAgIHRleHQ9IjxoMz5EYXNoYm9hcmQgdW5hdmFpbGFibGU6IGRhdGFiYXNlIG5vdCByZWFjaGFibGUu"
    "PC9oMz4iLAogICAgICAgICAgICBjb250ZW50X3R5cGU9InRleHQvaHRtbCIsCiAgICAgICAgICAgIHN0YXR1cz01MDMsCiAgICAg"
    "ICAgKQogICAgcmV0dXJuIHdlYi5SZXNwb25zZSh0ZXh0PV9QQUdFLCBjb250ZW50X3R5cGU9InRleHQvaHRtbCIpCgoKYXN5bmMg"
    "ZGVmIHd6YWRtaW5fYXBpKHJlcXVlc3QpOgogICAgcGF0aCA9IHJlcXVlc3QucGF0aAoKICAgIGlmIHBhdGguZW5kc3dpdGgoIi9h"
    "cGkvdGFrZW92ZXIiKToKICAgICAgICAjIFNlc3Npb24gdGFrZW92ZXI6IHRoZSBORVhUIEthZ2dsZSBub3RlYm9vayBydW4gcHJv"
    "dmVzIGl0IGhvbGRzCiAgICAgICAgIyB0aGUgY3VycmVudCBpbnN0YW5jZSB0b2tlbiAoc3RvcmVkIGluIE1vbmdvREIgbmV4dCB0"
    "byB0aGUgc2Vzc2lvbgogICAgICAgICMgbG9jaykgYW5kIGFza3MgdGhpcyBpbnN0YW5jZSB0byBzdG9wLiBUb2tlbiBhdXRoIG9u"
    "bHkg4oCUIG5vCiAgICAgICAgIyBzZXNzaW9uL0lQL1VBIGNoZWNrcywgYmVjYXVzZSB0aGUgY2FsbGVyIGlzIHRoZSBub3RlYm9v"
    "ayBpdHNlbGYKICAgICAgICAjICh3aGljaCBydW5zIG9uIGEgZGF0YWNlbnRlciBJUCBhbmQgd291bGQgdHJpcCB0aGUgaW50ZWwg"
    "ZmlsdGVyKS4KICAgICAgICB0cnk6CiAgICAgICAgICAgIGJvZHkgPSBhd2FpdCByZXF1ZXN0Lmpzb24oKQogICAgICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgICAgIGJvZHkgPSB7fQogICAgICAgIHRvayA9IHN0cihib2R5LmdldCgidG9rIiwgIiIpKS5z"
    "dHJpcCgpCiAgICAgICAgZ29vZCA9ICIiCiAgICAgICAgdHJ5OgogICAgICAgICAgICBpbXBvcnQgb3MgYXMgX29zCgogICAgICAg"
    "ICAgICBmcm9tIHB5bW9uZ28gaW1wb3J0IE1vbmdvQ2xpZW50CgogICAgICAgICAgICBfdXJsID0gX29zLmVudmlyb24uZ2V0KCJE"
    "QVRBQkFTRV9VUkwiLCAiIikKICAgICAgICAgICAgaWYgX3VybDoKICAgICAgICAgICAgICAgIF9jbCA9IE1vbmdvQ2xpZW50KF91"
    "cmwsIHNlcnZlclNlbGVjdGlvblRpbWVvdXRNUz04MDAwKQogICAgICAgICAgICAgICAgX2RvYyA9IF9jbFsid3ptbF9rYWdnbGUi"
    "XVsic2Vzc2lvbl9sb2NrIl0uZmluZF9vbmUoCiAgICAgICAgICAgICAgICAgICAgeyJfaWQiOiAiaW5zdGFuY2UifSkKICAgICAg"
    "ICAgICAgICAgIF9jbC5jbG9zZSgpCiAgICAgICAgICAgICAgICBnb29kID0gc3RyKChfZG9jIG9yIHt9KS5nZXQoInRvayIsICIi"
    "KSkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBnb29kID0gIiIKICAgICAgICBpZiBub3QgZ29vZCBvciBu"
    "b3QgdG9rIG9yIHRvayAhPSBnb29kOgogICAgICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJmb3Ji"
    "aWRkZW4ifSwgc3RhdHVzPTQwMykKCiAgICAgICAgaW1wb3J0IGFzeW5jaW8gYXMgX2FpbwoKICAgICAgICBhc3luYyBkZWYgX3d6"
    "Zml4X2RpZSgpOgogICAgICAgICAgICBhd2FpdCBfYWlvLnNsZWVwKDEuMCkgICMgbGV0IHRoZSByZXNwb25zZSBmbHVzaCBmaXJz"
    "dAogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBmcm9tIC4uLmNvcmUuY29uZmlnX21hbmFnZXIgaW1wb3J0IENvbmZp"
    "ZwogICAgICAgICAgICAgICAgZnJvbSAuLi5jb3JlLnRnX2NsaWVudCBpbXBvcnQgVGdDbGllbnQKCiAgICAgICAgICAgICAgICBj"
    "aGF0ID0gc3RyKGdldGF0dHIoQ29uZmlnLCAiTE9HX0NIQVQiLCAiIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICAgICAgICAgIGlm"
    "IGNoYXQ6CiAgICAgICAgICAgICAgICAgICAgYXdhaXQgVGdDbGllbnQuYm90LnNlbmRfbWVzc2FnZSgKICAgICAgICAgICAgICAg"
    "ICAgICAgICAgY2hhdF9pZD1fY2hhdF9pbnQoY2hhdCksCiAgICAgICAgICAgICAgICAgICAgICAgIHRleHQ9IvCflIQgPGI+V1pG"
    "SVg6PC9iPiBhIG5ld2VyIG5vdGVib29rIHNlc3Npb24gaXMgIgogICAgICAgICAgICAgICAgICAgICAgICAidGFraW5nIG92ZXIg"
    "4oCUIHN0b3BwaW5nIHRoaXMgaW5zdGFuY2UuIiwKICAgICAgICAgICAgICAgICAgICAgICAgZGlzYWJsZV93ZWJfcGFnZV9wcmV2"
    "aWV3PVRydWUsCiAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgZnJvbSAucjFfY29yZSBpbXBvcnQgYWRtaW5f"
    "bG9nCgogICAgICAgICAgICAgICAgYXdhaXQgYWRtaW5fbG9nKAogICAgICAgICAgICAgICAgICAgICLwn5S0IDxiPkJvdCBzdG9w"
    "cGluZzwvYj4iLAogICAgICAgICAgICAgICAgICAgICLilI8gQSBuZXdlciBub3RlYm9vayBzZXNzaW9uIHRvb2sgb3ZlclxuIgog"
    "ICAgICAgICAgICAgICAgICAgICLilJYgVGhpcyBpbnN0YW5jZSBpcyBzaHV0dGluZyBkb3duIiwKICAgICAgICAgICAgICAgICkK"
    "ICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAgICAgaW1wb3J0IG9zIGFz"
    "IF9vczIKICAgICAgICAgICAgaW1wb3J0IHNpZ25hbCBhcyBfc2lnCgogICAgICAgICAgICBfb3MyLmtpbGwoX29zMi5nZXRwaWQo"
    "KSwgX3NpZy5TSUdJTlQpCgogICAgICAgIF9haW8uZ2V0X2V2ZW50X2xvb3AoKS5jcmVhdGVfdGFzayhfd3pmaXhfZGllKCkpCiAg"
    "ICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKAogICAgICAgICAgICB7Im9rIjogVHJ1ZSwgIm1zZyI6ICJ0YWtlb3ZlciBh"
    "Y2NlcHRlZCDigJQgc2h1dHRpbmcgZG93biJ9CiAgICAgICAgKQoKICAgIGlmIHBhdGguZW5kc3dpdGgoIi9sb2dpbiIpOgogICAg"
    "ICAgIGlwID0gX25vcm1faXAoX2NsaWVudF9pcChyZXF1ZXN0KSkKICAgICAgICBpZiBub3QgX3VhX29rKHJlcXVlc3QpOgogICAg"
    "ICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJmb3JiaWRkZW4ifSwgc3RhdHVzPTQwMykKICAgICAg"
    "ICBpZiBhd2FpdCBfaXNfYmFubmVkKGlwKToKICAgICAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAi"
    "YmFubmVkIn0sIHN0YXR1cz00MDMpCiAgICAgICAgaWYgYXdhaXQgX2lwX2ludGVsKGlwKToKICAgICAgICAgICAgcmV0dXJuIHdl"
    "Yi5qc29uX3Jlc3BvbnNlKAogICAgICAgICAgICAgICAgeyJlcnJvciI6ICJkYXRhY2VudGVyL3Byb3h5IElQcyBhcmUgbm90IGFs"
    "bG93ZWQifSwgc3RhdHVzPTQwMwogICAgICAgICAgICApCiAgICAgICAgX3JlbSA9IGF3YWl0IF9sb2Nrb3V0X3N0YXRlKGlwKQog"
    "ICAgICAgIGlmIF9yZW06CiAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSgKICAgICAgICAgICAgICAgIHsiZXJy"
    "b3IiOiBmInRvbyBtYW55IGF0dGVtcHRzIOKAlCB7X2xvY2tfdHh0KF9yZW0pfSJ9LAogICAgICAgICAgICAgICAgc3RhdHVzPTQy"
    "OSwKICAgICAgICAgICAgKQogICAgICAgIHRyeToKICAgICAgICAgICAgYm9keSA9IGF3YWl0IHJlcXVlc3QuanNvbigpCiAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgYm9keSA9IHt9CiAgICAgICAgc3VibWl0dGVkID0gc3RyKGJvZHkuZ2V0"
    "KCJwYXNzIiwgIiIpKQogICAgICAgIGRldiA9IGJvZHkuZ2V0KCJkZXYiKQogICAgICAgIGlmIG5vdCBpc2luc3RhbmNlKGRldiwg"
    "ZGljdCk6CiAgICAgICAgICAgIGRldiA9IHt9CiAgICAgICAgIyBhYnNvbHV0ZS1jb250cm9sIHN3aXRjaDogL2xvY2tkYXNoIGlu"
    "IFRlbGVncmFtCiAgICAgICAgX2xvY2tlZCA9IEZhbHNlCiAgICAgICAgdHJ5OgogICAgICAgICAgICBmcm9tIC4uLmNvcmUuY29u"
    "ZmlnX21hbmFnZXIgaW1wb3J0IENvbmZpZyBhcyBfTENmZwoKICAgICAgICAgICAgX2xvY2tlZCA9IGJvb2woZ2V0YXR0cihfTENm"
    "ZywgIkFETUlOX0RBU0hCT0FSRF9MT0NLRUQiLCBGYWxzZSkpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAg"
    "cGFzcwogICAgICAgIGlmIF9sb2NrZWQ6CiAgICAgICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgZ2V0X2V2ZW50X2xvb3AgYXMg"
    "X2dlbAoKICAgICAgICAgICAgX2dlbCgpLmNyZWF0ZV90YXNrKAogICAgICAgICAgICAgICAgX2xvZ2luX2FsZXJ0KGlwLCByZXF1"
    "ZXN0LCBkZXYsIHN1Ym1pdHRlZCwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAi8J+UkiBkYXNoYm9hcmQgaXMgTE9DS0VE"
    "IOKAlCBsb2dpbiByZWZ1c2VkIikKICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoCiAg"
    "ICAgICAgICAgICAgICB7ImVycm9yIjogImRhc2hib2FyZCBsb2NrZWQgYnkgb3duZXIg4oCUIC9sb2NrZGFzaCB0byB1bmxvY2si"
    "fSwKICAgICAgICAgICAgICAgIHN0YXR1cz00MDMsCiAgICAgICAgICAgICkKICAgICAgICByZWFsID0gYXdhaXQgZ2V0X2FkbWlu"
    "X3Bhc3MoKQogICAgICAgIGlmIG5vdCByZWFsOgogICAgICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6"
    "ICJhZG1pbiBwYXNzIHVuYXZhaWxhYmxlIn0sIHN0YXR1cz01MDMpCiAgICAgICAgaWYgbm90IHN1Ym1pdHRlZCBvciBub3QgY29t"
    "cGFyZV9kaWdlc3Qoc3VibWl0dGVkLCByZWFsKToKICAgICAgICAgICAgX3NlY3MsIF90aWVyID0gYXdhaXQgX3JlY29yZF9mYWls"
    "KGlwKQogICAgICAgICAgICBfbm90ZSA9IGYiIOKAlCBMT0NLRUQgT1VUIHtfbG9ja190eHQoX3NlY3MpfSIgaWYgX3NlY3MgZWxz"
    "ZSAiIgogICAgICAgICAgICBmcm9tIGFzeW5jaW8gaW1wb3J0IGdldF9ldmVudF9sb29wIGFzIF9nZWwKCiAgICAgICAgICAgIF9n"
    "ZWwoKS5jcmVhdGVfdGFzaygKICAgICAgICAgICAgICAgIF9sb2dpbl9hbGVydChpcCwgcmVxdWVzdCwgZGV2LCBzdWJtaXR0ZWQs"
    "CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZiLinYwgd3JvbmcgcGFzc3dvcmR7X25vdGV9IikKICAgICAgICAgICAgKQog"
    "ICAgICAgICAgICBpZiBfc2VjczoKICAgICAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSgKICAgICAgICAgICAg"
    "ICAgICAgICB7ImVycm9yIjogZiJ3cm9uZyBwYXNzd29yZCDigJQge19sb2NrX3R4dChfc2Vjcyl9In0sCiAgICAgICAgICAgICAg"
    "ICAgICAgc3RhdHVzPTQyOSwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsi"
    "ZXJyb3IiOiAid3JvbmcgcGFzc3dvcmQifSwgc3RhdHVzPTQwMSkKICAgICAgICBfcHJpb3IgPSBhd2FpdCBfcHJpb3JfZmFpbHMo"
    "aXApCiAgICAgICAgX3JlY29yZF9vayhpcCkKICAgICAgICBpZiBfcHJpb3IgPiAwOgogICAgICAgICAgICBmcm9tIGFzeW5jaW8g"
    "aW1wb3J0IGdldF9ldmVudF9sb29wIGFzIF9nZWwKCiAgICAgICAgICAgIF9nZWwoKS5jcmVhdGVfdGFzaygKICAgICAgICAgICAg"
    "ICAgIF9sb2dpbl9hbGVydChpcCwgcmVxdWVzdCwgZGV2LCBzdWJtaXR0ZWQsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "ZiLinIUgbG9naW4gT0sgYWZ0ZXIge19wcmlvcn0gZmFpbGVkIGF0dGVtcHQocykiKQogICAgICAgICAgICApCiAgICAgICAgZWxz"
    "ZToKICAgICAgICAgICAgZnJvbSBhc3luY2lvIGltcG9ydCBnZXRfZXZlbnRfbG9vcCBhcyBfZ2VsCgogICAgICAgICAgICBfZ2Vs"
    "KCkuY3JlYXRlX3Rhc2soCiAgICAgICAgICAgICAgICBfbG9naW5fYWxlcnQoaXAsIHJlcXVlc3QsIGRldiwgIvCflJIiLCAi4pyF"
    "IGxvZ2luIHN1Y2Nlc3NmdWwiKQogICAgICAgICAgICApCiAgICAgICAgdG9rID0gYXdhaXQgX3Nlc3Npb25fdG9rZW4oKQogICAg"
    "ICAgIHJlc3AgPSB3ZWIuanNvbl9yZXNwb25zZSh7Im9rIjogVHJ1ZX0pCiAgICAgICAgcmVzcC5zZXRfY29va2llKAogICAgICAg"
    "ICAgICAid3phZG1pbiIsIHRvaywgbWF4X2FnZT1TRVNTSU9OX0ggKiAzNjAwLCBodHRwb25seT1UcnVlLCBzYW1lc2l0ZT0iTGF4"
    "IgogICAgICAgICkKICAgICAgICByZXR1cm4gcmVzcAoKICAgIGlmIG5vdCBhd2FpdCBfY2hlY2tfc2Vzc2lvbihyZXF1ZXN0KToK"
    "ICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJ1bmF1dGhvcml6ZWQifSwgc3RhdHVzPTQwMSkKICAg"
    "IGlmIG5vdCBfdWFfb2socmVxdWVzdCkgb3IgYXdhaXQgX2lzX2Jhbm5lZChfbm9ybV9pcChfY2xpZW50X2lwKHJlcXVlc3QpKSk6"
    "CiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAiZm9yYmlkZGVuIn0sIHN0YXR1cz00MDMpCgogICAg"
    "aWYgcGF0aC5lbmRzd2l0aCgiL2xvZ291dCIpOgogICAgICAgIHJlc3AgPSB3ZWIuanNvbl9yZXNwb25zZSh7Im9rIjogVHJ1ZX0p"
    "CiAgICAgICAgcmVzcC5kZWxfY29va2llKCJ3emFkbWluIikKICAgICAgICByZXR1cm4gcmVzcAoKICAgIGlmIHBhdGguZW5kc3dp"
    "dGgoIi9zdGF0ZSIpOgogICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZShhd2FpdCBfc3RhdGUoKSkKCiAgICBpZiBwYXRo"
    "LmVuZHN3aXRoKCIvaGlzdG9yeSIpOgogICAgICAgIHRyeToKICAgICAgICAgICAgdWlkID0gaW50KHJlcXVlc3QucXVlcnkuZ2V0"
    "KCJ1aWQiLCAiMCIpKQogICAgICAgIGV4Y2VwdCBWYWx1ZUVycm9yOgogICAgICAgICAgICB1aWQgPSAwCiAgICAgICAgcSA9IChy"
    "ZXF1ZXN0LnF1ZXJ5LmdldCgicSIpIG9yICIiKS5zdHJpcCgpWzo2MF0KICAgICAgICBkb2NzID0gYXdhaXQgZmluZChxLCB1aWQs"
    "IGFsbF91c2Vycz1GYWxzZSwgbGltaXQ9MjUpIGlmIHVpZCBlbHNlIFtdCiAgICAgICAgb3V0ID0gW10KICAgICAgICBmb3IgZCBp"
    "biBkb2NzOgogICAgICAgICAgICB0Z19saW5rcyA9IGQuZ2V0KCJ0Z19saW5rcyIpIG9yIFtdCiAgICAgICAgICAgIG91dC5hcHBl"
    "bmQoCiAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiBzdHIoZC5nZXQoIm5hbWUiLCAiIikpWzo5"
    "MF0sCiAgICAgICAgICAgICAgICAgICAgInNpemUiOiBpbnQoZC5nZXQoInNpemUiKSBvciAwKSwKICAgICAgICAgICAgICAgICAg"
    "ICAiZGF0ZSI6IHN0cihkLmdldCgiZGF0ZSIpIG9yICIiKVs6MTZdLAogICAgICAgICAgICAgICAgICAgICJ0ZyI6IHN0cih0Z19s"
    "aW5rc1swXSkgaWYgdGdfbGlua3MgZWxzZSAiIiwKICAgICAgICAgICAgICAgICAgICAiY2xvdWQiOiBzdHIoZC5nZXQoImNsb3Vk"
    "X2xpbmsiKSBvciAiIiksCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICkKICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVz"
    "cG9uc2UoeyJvayI6IFRydWUsICJpdGVtcyI6IG91dH0pCgogICAgaWYgcGF0aC5lbmRzd2l0aCgiL2FjdGlvbiIpOgogICAgICAg"
    "IHRyeToKICAgICAgICAgICAgYm9keSA9IGF3YWl0IHJlcXVlc3QuanNvbigpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAg"
    "ICAgICAgICAgYm9keSA9IHt9CiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKGF3YWl0IF9hY3Rpb24oYm9keSkpCgog"
    "ICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAidW5rbm93biBlbmRwb2ludCJ9LCBzdGF0dXM9NDA0KQoKCmFz"
    "eW5jIGRlZiBfYWN0aW9uX2xvZyh3aGF0LCBkZXRhaWwpOgogICAgIiIiRGFzaGJvYXJkIGFjdGlvbiAtPiB0aGUgYWRtaW4gbG9n"
    "cyBncm91cC4iIiIKICAgIHRyeToKICAgICAgICBmcm9tIC5yMV9jb3JlIGltcG9ydCBhZG1pbl9sb2cKCiAgICAgICAgYXdhaXQg"
    "YWRtaW5fbG9nKAogICAgICAgICAgICAi8J+OmyA8Yj5EYXNoYm9hcmQgYWN0aW9uPC9iPiIsCiAgICAgICAgICAgIGYi4pSPIDxi"
    "PkFjdGlvbjwvYj4g4oaSIHt3aGF0fVxuIgogICAgICAgICAgICArICIiLmpvaW4oZiLilKAge2t9IOKGkiB7dn1cbiIgZm9yIGss"
    "IHYgaW4gZGV0YWlsLml0ZW1zKCkpCiAgICAgICAgICAgICsgIuKUliB2aWEgL3d6YWRtaW4iLAogICAgICAgICkKICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgcGFzcwoKCmFzeW5jIGRlZiBfYWN0aW9uKGEpOgogICAgYWN0ID0gc3RyKGEuZ2V0KCJhY3Rp"
    "b24iLCAiIikpLnN0cmlwKCkKICAgIHRyeToKICAgICAgICB1aWQgPSBpbnQoYS5nZXQoInVpZCIsIDApIG9yIDApCiAgICBleGNl"
    "cHQgKFR5cGVFcnJvciwgVmFsdWVFcnJvcik6CiAgICAgICAgdWlkID0gMAoKICAgIHRyeToKICAgICAgICBpZiBhY3QgPT0gInNl"
    "dGNhcCI6CiAgICAgICAgICAgIGdiID0gZmxvYXQoYS5nZXQoImdiIiwgMCkgb3IgMCkKICAgICAgICAgICAgYXdhaXQgc2V0X3Vz"
    "ZXJfY2FwKHVpZCwgZ2IgaWYgZ2IgPiAwIGVsc2UgTm9uZSkKICAgICAgICAgICAgYXdhaXQgX2FjdGlvbl9sb2coIlNFVCBDQVAi"
    "LCB7InVpZCI6IHVpZCwgImdiIjogYS5nZXQoImdiIiwgIiIpfSkKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNn"
    "IjogZiJjYXAgZm9yIHt1aWR9IOKGkiB7X2ZtdF9nYihnYikgaWYgZ2IgPiAwIGVsc2UgJ2RlZmF1bHQnfSJ9CiAgICAgICAgaWYg"
    "YWN0ID09ICJzZXRtdXNpYyI6CiAgICAgICAgICAgIG4gPSBpbnQoZmxvYXQoYS5nZXQoIm4iLCAwKSBvciAwKSkKICAgICAgICAg"
    "ICAgYXdhaXQgc2V0X3VzZXJfbXVzaWModWlkLCBuIGlmIG4gPiAwIGVsc2UgTm9uZSkKICAgICAgICAgICAgYXdhaXQgX2FjdGlv"
    "bl9sb2coIlNFVCBNVVNJQyBMSU1JVCIsIHsidWlkIjogdWlkLCAibiI6IGEuZ2V0KCJuIiwgIiIpfSkKICAgICAgICAgICAgcmV0"
    "dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJtdXNpYyBsaW1pdCBmb3Ige3VpZH0g4oaSIHtuIGlmIG4gPiAwIGVsc2UgJ2RlZmF1"
    "bHQnfSJ9CiAgICAgICAgaWYgYWN0ID09ICJnbXVzaWMiOgogICAgICAgICAgICBuID0gaW50KGZsb2F0KGEuZ2V0KCJuIiwgMTAp"
    "IG9yIDEwKSkKICAgICAgICAgICAgaWYgbiA8IDE6CiAgICAgICAgICAgICAgICBuID0gMQogICAgICAgICAgICBuID0gbWluKDUw"
    "MCwgbikKICAgICAgICAgICAgYXdhaXQgc2V0X2dsb2JhbF9tdXNpYyhuKQogICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygi"
    "U0VUIEdMT0JBTCBNVVNJQyIsIHsibiI6IG59KQogICAgICAgICAgICByZXR1cm4geyJvayI6IFRydWUsICJtc2ciOiBmImRlZmF1"
    "bHQgbXVzaWMgbGltaXQg4oaSIHtufSBzb25ncyJ9CiAgICAgICAgaWYgYWN0ID09ICJtc2V0IjoKICAgICAgICAgICAgayA9IHN0"
    "cihhLmdldCgiayIsICIiKSkKICAgICAgICAgICAgdiA9IGJvb2woaW50KGEuZ2V0KCJ2IiwgMCkgb3IgMCkpCiAgICAgICAgICAg"
    "IGlmIGsgaW4gKCJtdXNpY19vbiIsICJsZF9vbiIsICJhbGlhc2VzX29uIik6CiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAg"
    "ICAgICAgICAgICAgZnJvbSAucjRfbXVzaWNfY21kcyBpbXBvcnQgc2V0X3NldHRpbmcKCiAgICAgICAgICAgICAgICAgICAgYXdh"
    "aXQgc2V0X3NldHRpbmcoaywgdikKICAgICAgICAgICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygiU0VUIE1VU0lDIFNFVFRJ"
    "TkciLCB7ImsiOiBrLCAidiI6IHZ9KQogICAgICAgICAgICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYie2t9"
    "IOKGkiB7J29uJyBpZiB2IGVsc2UgJ29mZid9In0KICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAg"
    "ICAgICAgICAgICAgICByZXR1cm4geyJvayI6IEZhbHNlLCAibXNnIjogZiJzZXR0aW5nIGZhaWxlZDoge2V9In0KICAgICAgICAg"
    "ICAgcmV0dXJuIHsib2siOiBGYWxzZSwgIm1zZyI6ICJ1bmtub3duIHNldHRpbmcifQogICAgICAgIGlmIGFjdCA9PSAiYmFuIjoK"
    "ICAgICAgICAgICAgYXdhaXQgc2V0X3VzZXJfY2FwKHVpZCwgMCkKICAgICAgICAgICAgYXdhaXQgX2FjdGlvbl9sb2coIkJBTiBV"
    "U0VSIiwgeyJ1aWQiOiB1aWR9KQogICAgICAgICAgICByZXR1cm4geyJvayI6IFRydWUsICJtc2ciOiBmInVzZXIge3VpZH0gYmxv"
    "Y2tlZCAoY2FwIDApIn0KICAgICAgICBpZiBhY3QgPT0gInVuYmFuIjoKICAgICAgICAgICAgYXdhaXQgc2V0X3VzZXJfY2FwKHVp"
    "ZCwgTm9uZSkKICAgICAgICAgICAgYXdhaXQgX2FjdGlvbl9sb2coIlVOQkFOIFVTRVIiLCB7InVpZCI6IHVpZH0pCiAgICAgICAg"
    "ICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYidXNlciB7dWlkfSBiYWNrIHRvIGdsb2JhbCBkZWZhdWx0In0KICAgICAg"
    "ICBpZiBhY3QgPT0gInJlc2V0Y2FwIjoKICAgICAgICAgICAgYXdhaXQgcmVzZXRfdXNhZ2UodWlkKQogICAgICAgICAgICBhd2Fp"
    "dCBfYWN0aW9uX2xvZygiUkVTRVQgREFZIFVTQUdFIiwgeyJ1aWQiOiB1aWR9KQogICAgICAgICAgICByZXR1cm4geyJvayI6IFRy"
    "dWUsICJtc2ciOiBmInRvZGF5J3MgdXNhZ2UgcmVzZXQgZm9yIHt1aWR9In0KICAgICAgICBpZiBhY3QgPT0gImRlZHVjdGNhcCI6"
    "CiAgICAgICAgICAgIGdiID0gZmxvYXQoYS5nZXQoImdiIiwgMCkgb3IgMCkKICAgICAgICAgICAgb2ssIG1zZyA9IGF3YWl0IF9k"
    "ZWR1Y3RfY2FwKHVpZCwgZ2IpCiAgICAgICAgICAgIHJldHVybiB7Im9rIjogb2ssICJtc2ciOiBtc2d9CiAgICAgICAgaWYgYWN0"
    "ID09ICJkZWx1c2VyIjoKICAgICAgICAgICAgaWYgbm90IHVpZDoKICAgICAgICAgICAgICAgIHJldHVybiB7Im9rIjogRmFsc2Us"
    "ICJtc2ciOiAibm8gdXNlciBpZCJ9CiAgICAgICAgICAgIGF3YWl0IGRlbGV0ZV91c2VyKHVpZCkKICAgICAgICAgICAgYXdhaXQg"
    "X2FjdGlvbl9sb2coIlJFTU9WRSBVU0VSIiwgeyJ1aWQiOiB1aWR9KQogICAgICAgICAgICByZXR1cm4geyJvayI6IFRydWUsICJt"
    "c2ciOiBmInVzZXIge3VpZH0gcmVtb3ZlZCJ9CiAgICAgICAgaWYgYWN0ID09ICJib3RjYXAiOgogICAgICAgICAgICBnYiA9IGZs"
    "b2F0KGEuZ2V0KCJnYiIsIDApIG9yIDApCiAgICAgICAgICAgIGlmIGdiIDw9IDA6CiAgICAgICAgICAgICAgICByZXR1cm4geyJv"
    "ayI6IEZhbHNlLCAibXNnIjogImdpdmUgYSBwb3NpdGl2ZSBHQiB2YWx1ZSJ9CiAgICAgICAgICAgIGF3YWl0IHNldF9nbG9iYWxf"
    "Y2FwX2diKGdiKQogICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygiU0VUIERFRkFVTFQgQ0FQIiwgeyJnYiI6IGdifSkKICAg"
    "ICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJkZWZhdWx0IGNhcCDihpIge19mbXRfZ2IoZ2IpfSJ9CiAgICAg"
    "ICAgaWYgYWN0ID09ICJraWxsIjoKICAgICAgICAgICAgYXdhaXQgX2FjdGlvbl9sb2coIktJTEwgVEFTSyIsIHsibWlkIjogYS5n"
    "ZXQoIm1pZCIsICIiKX0pCiAgICAgICAgICAgIG9rLCBtc2cgPSBhd2FpdCBfa2lsbF90YXNrKGEuZ2V0KCJtaWQiKSkKICAgICAg"
    "ICAgICAgcmV0dXJuIHsib2siOiBvaywgIm1zZyI6IG1zZ30KICAgICAgICBpZiBhY3QgPT0gImtpbGxhbGwiOgogICAgICAgICAg"
    "ICBmcm9tIC4uLiBpbXBvcnQgdGFza19kaWN0CgogICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygiS0lMTCBBTEwgVEFTS1Mi"
    "LCB7fSkKICAgICAgICAgICAgbiA9IDAKICAgICAgICAgICAgZm9yIG1pZCwgdCBpbiBsaXN0KHRhc2tfZGljdC5pdGVtcygpKToK"
    "ICAgICAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgICAgICBhd2FpdCB0LnRhc2soKS5jYW5jZWxfdGFzaygpCiAgICAg"
    "ICAgICAgICAgICAgICAgbiArPSAxCiAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAg"
    "IHBhc3MKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJ7bn0gdGFzayhzKSBjYW5jZWxsZWQifQogICAg"
    "ICAgIGlmIGFjdCA9PSAicmVwb3J0IjoKICAgICAgICAgICAgb2ssIG1zZyA9IGF3YWl0IHNlbmRfcmVwb3J0KCkKICAgICAgICAg"
    "ICAgcmV0dXJuIHsib2siOiBvaywgIm1zZyI6IG1zZ30KICAgICAgICBpZiBhY3QgaW4gKCJ1c2VyYXV0aCIsICJ1c2Vyc3VkbyIs"
    "ICJ1c2VyYmwiKToKICAgICAgICAgICAgaWYgbm90IHVpZDoKICAgICAgICAgICAgICAgIHJldHVybiB7Im9rIjogRmFsc2UsICJt"
    "c2ciOiAibm8gdXNlciBpZCJ9CiAgICAgICAgICAgIGZyb20gLi4uIGltcG9ydCB1c2VyX2RhdGEgYXMgX3VkCiAgICAgICAgICAg"
    "IGZyb20gLi4uaGVscGVyLmV4dF91dGlscy5ib3RfdXRpbHMgaW1wb3J0IHVwZGF0ZV91c2VyX2xkYXRhCiAgICAgICAgICAgIGZy"
    "b20gLi4uaGVscGVyLmV4dF91dGlscy5kYl9oYW5kbGVyIGltcG9ydCBkYXRhYmFzZQoKICAgICAgICAgICAgZmxhZyA9IHsKICAg"
    "ICAgICAgICAgICAgICJ1c2VyYXV0aCI6ICJBVVRIIiwKICAgICAgICAgICAgICAgICJ1c2Vyc3VkbyI6ICJTVURPIiwKICAgICAg"
    "ICAgICAgICAgICJ1c2VyYmwiOiAiQkxBQ0tMSVNUIiwKICAgICAgICAgICAgfVthY3RdCiAgICAgICAgICAgIGxhYmVsID0gZmxh"
    "Zy50aXRsZSgpCiAgICAgICAgICAgIHYgPSBib29sKGludChhLmdldCgidiIsIDApIG9yIDApKQogICAgICAgICAgICB1cGRhdGVf"
    "dXNlcl9sZGF0YSh1aWQsIGZsYWcsIHYpCiAgICAgICAgICAgIGF3YWl0IGRhdGFiYXNlLnVwZGF0ZV91c2VyX2RhdGEodWlkKQog"
    "ICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygKICAgICAgICAgICAgICAgICgiR1JBTlQgIiBpZiB2IGVsc2UgIlJFVk9LRSAi"
    "KSArIGxhYmVsLCB7InVpZCI6IHVpZH0KICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4gewogICAgICAgICAgICAgICAg"
    "Im9rIjogVHJ1ZSwKICAgICAgICAgICAgICAgICJtc2ciOiBmInVzZXIge3VpZH06IHtsYWJlbC5sb3dlcigpfSB7J29uJyBpZiB2"
    "IGVsc2UgJ29mZid9IiwKICAgICAgICAgICAgfQogICAgICAgIGlmIGFjdCA9PSAic3BzZXQiOgogICAgICAgICAgICBmcm9tIC5y"
    "NV9zdHJlYW1wYXNzIGltcG9ydCBleHRyYWN0X3Rva2VuLCBzZXRfbGlua19wYXNzCgogICAgICAgICAgICB0b2sgPSBleHRyYWN0"
    "X3Rva2VuKHN0cihhLmdldCgibGluayIsICIiKSBvciAiIikpCiAgICAgICAgICAgIHB3ID0gc3RyKGEuZ2V0KCJwYXNzIiwgIiIp"
    "IG9yICIiKS5zdHJpcCgpCiAgICAgICAgICAgIGlmIG5vdCB0b2s6CiAgICAgICAgICAgICAgICByZXR1cm4geyJvayI6IEZhbHNl"
    "LCAibXNnIjogIm5vdCBhIHN0cmVhbSBsaW5rIG9yIHRva2VuIn0KICAgICAgICAgICAgaWYgbm90IHB3IG9yIGxlbihwdykgPiA2"
    "NCBvciAiICIgaW4gcHc6CiAgICAgICAgICAgICAgICByZXR1cm4geyJvayI6IEZhbHNlLCAibXNnIjogInBhc3N3b3JkIDEtNjQg"
    "Y2hhcnMsIG5vIHNwYWNlcyJ9CiAgICAgICAgICAgIGF3YWl0IHNldF9saW5rX3Bhc3ModG9rLCBwdywgMCkKICAgICAgICAgICAg"
    "YXdhaXQgX2FjdGlvbl9sb2coIkxPQ0sgU1RSRUFNIExJTksiLCB7ImxpbmsiOiB0b2t9KQogICAgICAgICAgICByZXR1cm4geyJv"
    "ayI6IFRydWUsICJtc2ciOiBmInt0b2t9IG5vdyBuZWVkcyBpdHMgb3duIHBhc3N3b3JkIn0KICAgICAgICBpZiBhY3QgPT0gInNw"
    "ZGVsIjoKICAgICAgICAgICAgZnJvbSAucjVfc3RyZWFtcGFzcyBpbXBvcnQgZXh0cmFjdF90b2tlbiwgZGVsX2xpbmtfcGFzcwoK"
    "ICAgICAgICAgICAgdG9rID0gZXh0cmFjdF90b2tlbihzdHIoYS5nZXQoImxpbmsiLCAiIikgb3IgIiIpKQogICAgICAgICAgICBp"
    "ZiBub3QgdG9rOgogICAgICAgICAgICAgICAgcmV0dXJuIHsib2siOiBGYWxzZSwgIm1zZyI6ICJub3QgYSBzdHJlYW0gbGluayBv"
    "ciB0b2tlbiJ9CiAgICAgICAgICAgIHJlbW92ZWQgPSBhd2FpdCBkZWxfbGlua19wYXNzKHRvaykKICAgICAgICAgICAgYXdhaXQg"
    "X2FjdGlvbl9sb2coIlVOTE9DSyBTVFJFQU0gTElOSyIsIHsibGluayI6IHRva30pCiAgICAgICAgICAgIHJldHVybiB7Im9rIjog"
    "VHJ1ZSwgIm1zZyI6ICJyZW1vdmVkIOKAlCBiYWNrIHRvIHRoZSBnbG9iYWwgcGFzc3dvcmQiIGlmIHJlbW92ZWQgZWxzZSAiaGFk"
    "IG5vIGN1c3RvbSBwYXNzd29yZCJ9CiAgICAgICAgaWYgYWN0ID09ICJzcGxpc3QiOgogICAgICAgICAgICBmcm9tIC5yNV9zdHJl"
    "YW1wYXNzIGltcG9ydCBhbGxfbGlua19wYXNzZXMKCiAgICAgICAgICAgIHJvd3MgPSBhd2FpdCBhbGxfbGlua19wYXNzZXMoKQog"
    "ICAgICAgICAgICBtc2cgPSAiOyAiLmpvaW4oZFsiX2lkIl0gKyAiIOKGkiAiICsgZFsicGFzcyJdIGZvciBkIGluIHJvd3MpCiAg"
    "ICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IG1zZyBvciAibm8gY3VzdG9tIHN0cmVhbSBwYXNzd29yZHMgc2V0"
    "In0KICAgICAgICByZXR1cm4geyJvayI6IEZhbHNlLCAibXNnIjogZiJ1bmtub3duIGFjdGlvbjoge2FjdH0ifQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIHJldHVybiB7Im9rIjogRmFsc2UsICJtc2ciOiBzdHIoZSlbOjE2MF19CgoKIyDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKIyBUaGUgcGFn"
    "ZSDigJQgNyB0aGVtZXMgKG1hdGNoaW5nIHRoZSBzdHJlYW0gcGxheWVyKSwgbW9iaWxlLWZpcnN0CiMg4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgpfUEFHRSA9ICIiIjwhZG9jdHlw"
    "ZSBodG1sPgo8aHRtbCBsYW5nPSJlbiIgZGF0YS10aGVtZT0ib255eCI+CjxoZWFkPgo8bWV0YSBjaGFyc2V0PSJ1dGYtOCI+Cjxt"
    "ZXRhIG5hbWU9InZpZXdwb3J0IiBjb250ZW50PSJ3aWR0aD1kZXZpY2Utd2lkdGgsaW5pdGlhbC1zY2FsZT0xLHZpZXdwb3J0LWZp"
    "dD1jb3ZlciI+CjxtZXRhIG5hbWU9InJvYm90cyIgY29udGVudD0ibm9pbmRleCI+Cjx0aXRsZT5XWk1MLVggQ29udHJvbDwvdGl0"
    "bGU+CjxzdHlsZT4KOnJvb3R7LS1yOjE2cHg7LS1zYW5zOi1hcHBsZS1zeXN0ZW0sQmxpbmtNYWNTeXN0ZW1Gb250LCdTZWdvZSBV"
    "SScsUm9ib3RvLHNhbnMtc2VyaWZ9CltkYXRhLXRoZW1lPSJvbnl4Il17LS1iZzojMDUwNTA4Oy0tc3VyZmFjZTojMGEwYTEyOy0t"
    "c3VyZmFjZS0yOiMwZTBlMTg7LS1saW5lOnJnYmEoMTM5LDkyLDI0NiwuMjIpOy0tdGV4dDojZjRmMmZmOy0tbXV0ZWQ6I2I2YjBk"
    "NDstLWFjY2VudDojOGI1Y2Y2Oy0tYWNjZW50LTI6I2E3OGJmYTstLWFjY2VudC1zb2Z0OiMxYTEwMzM7LS1kYW5nZXI6I2Y4NzE3"
    "MTstLW9rOiM0YWRlODB9CltkYXRhLXRoZW1lPSJhcXVhIl17LS1iZzojMDMxNDFiOy0tc3VyZmFjZTojMDUxZTI4Oy0tc3VyZmFj"
    "ZS0yOiMwNzI0MmY7LS1saW5lOnJnYmEoMzQsMjExLDIzOCwuMjIpOy0tdGV4dDojZWFmY2ZmOy0tbXV0ZWQ6I2EzYzZkMTstLWFj"
    "Y2VudDojMjJkM2VlOy0tYWNjZW50LTI6IzY3ZThmOTstLWFjY2VudC1zb2Z0OiMwNjI0MzA7LS1kYW5nZXI6I2Y4NzE3MTstLW9r"
    "OiM0YWRlODB9CltkYXRhLXRoZW1lPSJlbWJlciJdey0tYmc6IzEyMGEwNTstLXN1cmZhY2U6IzFhMGYwNzstLXN1cmZhY2UtMjoj"
    "MjExNDBhOy0tbGluZTpyZ2JhKDI0NSwxNTgsMTEsLjIyKTstLXRleHQ6I2ZkZjNlNzstLW11dGVkOiNkMGI4YTA7LS1hY2NlbnQ6"
    "I2Y1OWUwYjstLWFjY2VudC0yOiNmYmJmMjQ7LS1hY2NlbnQtc29mdDojMmExYTA2Oy0tZGFuZ2VyOiNmODcxNzE7LS1vazojNGFk"
    "ZTgwfQpbZGF0YS10aGVtZT0iZGFyayJdey0tYmc6IzA0MDQwYjstLXN1cmZhY2U6IzA4MDgxMjstLXN1cmZhY2UtMjojMGUwZTFh"
    "Oy0tbGluZTpyZ2JhKDYxLDEzNSwyNTUsLjE4KTstLXRleHQ6I2Y1ZjdmZjstLW11dGVkOiNjM2NiZGY7LS1hY2NlbnQ6IzNkODdm"
    "ZjstLWFjY2VudC0yOiM1YjlkZmY7LS1hY2NlbnQtc29mdDojMGQxYjNhOy0tZGFuZ2VyOiNmZjZiNmI7LS1vazojNTFlOThjfQpb"
    "ZGF0YS10aGVtZT0ibGlnaHQiXXstLWJnOiNmMmY1ZmI7LS1zdXJmYWNlOiNmZmZmZmY7LS1zdXJmYWNlLTI6I2VlZjFmODstLWxp"
    "bmU6cmdiYSgxNSwyMyw0MiwuMTIpOy0tdGV4dDojMGYxNzJhOy0tbXV0ZWQ6IzVhNjQ3ODstLWFjY2VudDojMjU2M2ViOy0tYWNj"
    "ZW50LTI6IzNiODJmNjstLWFjY2VudC1zb2Z0OiNkYmVhZmU7LS1kYW5nZXI6I2RjMjYyNjstLW9rOiMxNmEzNGF9CltkYXRhLXRo"
    "ZW1lPSJ2aWJyYW50Il17LS1iZzojMGEwNjEyOy0tc3VyZmFjZTojMTMwYjIwOy0tc3VyZmFjZS0yOiMxYjExMzA7LS1saW5lOnJn"
    "YmEoMTY4LDg1LDI0NywuMjUpOy0tdGV4dDojZmFmNWZmOy0tbXV0ZWQ6I2M0YjVmZDstLWFjY2VudDojYTg1NWY3Oy0tYWNjZW50"
    "LTI6I2Q5NDZlZjstLWFjY2VudC1zb2Z0OiMyZTEwNjU7LS1kYW5nZXI6I2ZiNzE4NTstLW9rOiMzNGQzOTl9CltkYXRhLXRoZW1l"
    "PSJibG9zc29tIl17LS1iZzojMTYwYTEwOy0tc3VyZmFjZTojMjAxMDFiOy0tc3VyZmFjZS0yOiMyYTE2MjI7LS1saW5lOnJnYmEo"
    "MjQ0LDExNCwxODIsLjI1KTstLXRleHQ6I2ZkZjJmODstLW11dGVkOiNmOWE4ZDQ7LS1hY2NlbnQ6I2Y0NzJiNjstLWFjY2VudC0y"
    "OiNmZGE0YWY7LS1hY2NlbnQtc29mdDojNGEwNDRlOy0tZGFuZ2VyOiNmODcxNzE7LS1vazojNmVlN2I3fQoqe2JveC1zaXppbmc6"
    "Ym9yZGVyLWJveDttYXJnaW46MDtwYWRkaW5nOjB9CmJvZHl7YmFja2dyb3VuZDp2YXIoLS1iZyk7Y29sb3I6dmFyKC0tdGV4dCk7"
    "Zm9udDoxNXB4LzEuNDUgdmFyKC0tc2Fucyk7bWluLWhlaWdodDoxMDB2aDtwYWRkaW5nLWJvdHRvbTo0MHB4Oy13ZWJraXQtdGFw"
    "LWhpZ2hsaWdodC1jb2xvcjp0cmFuc3BhcmVudH0KYm9keTo6YmVmb3Jle2NvbnRlbnQ6IiI7cG9zaXRpb246Zml4ZWQ7aW5zZXQ6"
    "LTIwJTt6LWluZGV4OjA7cG9pbnRlci1ldmVudHM6bm9uZTtiYWNrZ3JvdW5kOnJhZGlhbC1ncmFkaWVudCgzNiUgMzAlIGF0IDE4"
    "JSAxMiUsY29sb3ItbWl4KGluIHNyZ2IsdmFyKC0tYWNjZW50KSAxNiUsdHJhbnNwYXJlbnQpLHRyYW5zcGFyZW50IDcwJSkscmFk"
    "aWFsLWdyYWRpZW50KDMyJSAyOCUgYXQgODIlIDIwJSxjb2xvci1taXgoaW4gc3JnYix2YXIoLS1hY2NlbnQpIDEwJSx0cmFuc3Bh"
    "cmVudCksdHJhbnNwYXJlbnQgNzIlKX0KLndyYXB7cG9zaXRpb246cmVsYXRpdmU7ei1pbmRleDoxO21heC13aWR0aDo2ODBweDtt"
    "YXJnaW46MCBhdXRvO3BhZGRpbmc6MCAxNHB4fQoudG9wYmFye3Bvc2l0aW9uOnN0aWNreTt0b3A6MDt6LWluZGV4OjEwO2Rpc3Bs"
    "YXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjhweDtwYWRkaW5nOjE0cHggMnB4O2JhY2tncm91bmQ6Y29sb3ItbWl4KGlu"
    "IHNyZ2IsdmFyKC0tYmcpIDg4JSx0cmFuc3BhcmVudCk7YmFja2Ryb3AtZmlsdGVyOmJsdXIoMTRweCk7Ym9yZGVyLWJvdHRvbTox"
    "cHggc29saWQgdmFyKC0tbGluZSl9Ci5sb2dve2ZvbnQtc2l6ZToxOHB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzot"
    "LjAyZW07ZmxleDoxfQoubG9nbyBie2NvbG9yOnZhcigtLWFjY2VudCl9Ci5jaGlwe2ZvbnQtc2l6ZToxMnB4O2ZvbnQtd2VpZ2h0"
    "OjYwMDtjb2xvcjp2YXIoLS1tdXRlZCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjk5OXB4O3Bh"
    "ZGRpbmc6NHB4IDEwcHg7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlKX0KaDJ7Zm9udC1zaXplOjEzcHg7dGV4dC10cmFuc2Zvcm06"
    "dXBwZXJjYXNlO2xldHRlci1zcGFjaW5nOi4wOGVtO2NvbG9yOnZhcigtLW11dGVkKTttYXJnaW46MjJweCAycHggMTBweH0KLmNh"
    "cmR7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlKTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yYWRpdXM6dmFy"
    "KC0tcik7cGFkZGluZzoxNHB4O21hcmdpbi1ib3R0b206MTJweH0KLnN0YXRze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNv"
    "bHVtbnM6cmVwZWF0KDIsMWZyKTtnYXA6MTBweDttYXJnaW4tdG9wOjE0cHh9Ci5zdGF0e2JhY2tncm91bmQ6dmFyKC0tc3VyZmFj"
    "ZSk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMnB4fQouc3RhdCAudntm"
    "b250LXNpemU6MTlweDtmb250LXdlaWdodDo4MDB9Ci5zdGF0IC5re2ZvbnQtc2l6ZToxMXB4O2NvbG9yOnZhcigtLW11dGVkKTt0"
    "ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2U7bGV0dGVyLXNwYWNpbmc6LjA2ZW07bWFyZ2luLXRvcDoycHh9Ci5iYXJ7aGVpZ2h0Ojhw"
    "eDtib3JkZXItcmFkaXVzOjk5cHg7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlLTIpO292ZXJmbG93OmhpZGRlbjttYXJnaW46MTBw"
    "eCAwIDZweH0KLmJhciBpe2Rpc3BsYXk6YmxvY2s7aGVpZ2h0OjEwMCU7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6bGlu"
    "ZWFyLWdyYWRpZW50KDkwZGVnLHZhcigtLWFjY2VudCksdmFyKC0tYWNjZW50LTIpKTt0cmFuc2l0aW9uOndpZHRoIC41cyBlYXNl"
    "fQoubXV0ZWR7Y29sb3I6dmFyKC0tbXV0ZWQpO2ZvbnQtc2l6ZToxMi41cHh9Ci5yb3d7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1z"
    "OmNlbnRlcjtnYXA6OHB4fQoudXNyLW5hbWV7Zm9udC13ZWlnaHQ6NzAwO2ZvbnQtc2l6ZToxNXB4fQouYnRue2JvcmRlcjoxcHgg"
    "c29saWQgdmFyKC0tbGluZSk7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlLTIpO2NvbG9yOnZhcigtLXRleHQpO2JvcmRlci1yYWRp"
    "dXM6MTBweDtwYWRkaW5nOjdweCAxMXB4O2ZvbnQ6NjAwIDEyLjVweCB2YXIoLS1zYW5zKTtjdXJzb3I6cG9pbnRlcjt0cmFuc2l0"
    "aW9uOmJvcmRlci1jb2xvciAuMTVzLHRyYW5zZm9ybSAuMXN9Ci5idG46YWN0aXZle3RyYW5zZm9ybTpzY2FsZSguOTYpfQouYnRu"
    "LnByaXtiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCgxMzVkZWcsdmFyKC0tYWNjZW50KSx2YXIoLS1hY2NlbnQtMikpO2JvcmRl"
    "ci1jb2xvcjp0cmFuc3BhcmVudDtjb2xvcjojZmZmfQouYnRuLmRuZ3tjb2xvcjp2YXIoLS1kYW5nZXIpO2JvcmRlci1jb2xvcjpj"
    "b2xvci1taXgoaW4gc3JnYix2YXIoLS1kYW5nZXIpIDQ1JSx0cmFuc3BhcmVudCl9Ci5idG5ze2Rpc3BsYXk6ZmxleDtmbGV4LXdy"
    "YXA6d3JhcDtnYXA6N3B4O21hcmdpbi10b3A6MTBweH0KaW5wdXRbdHlwZT1wYXNzd29yZF17d2lkdGg6MTAwJTtiYWNrZ3JvdW5k"
    "OnZhcigtLXN1cmZhY2UtMik7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjEycHg7Y29sb3I6dmFy"
    "KC0tdGV4dCk7cGFkZGluZzoxM3B4IDE0cHg7Zm9udDoxNXB4IHZhcigtLXNhbnMpfQppbnB1dFt0eXBlPXBhc3N3b3JkXTpmb2N1"
    "c3tvdXRsaW5lOm5vbmU7Ym9yZGVyLWNvbG9yOnZhcigtLWFjY2VudC0yKX0KLmxvZ2luLWJveHttYXgtd2lkdGg6MzYwcHg7bWFy"
    "Z2luOjE2dmggYXV0byAwO3RleHQtYWxpZ246Y2VudGVyfQoudGFza3tkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dh"
    "cDoxMHB4fQoudGFzayAubm17ZmxleDoxO21pbi13aWR0aDowO2ZvbnQtd2VpZ2h0OjYwMDtmb250LXNpemU6MTMuNXB4O3doaXRl"
    "LXNwYWNlOm5vd3JhcDtvdmVyZmxvdzpoaWRkZW47dGV4dC1vdmVyZmxvdzplbGxpcHNpc30KLmhpZGRlbntkaXNwbGF5Om5vbmV9"
    "CiN0b2FzdHtwb3NpdGlvbjpmaXhlZDtsZWZ0OjUwJTtib3R0b206MjZweDt0cmFuc2Zvcm06dHJhbnNsYXRlWCgtNTAlKSB0cmFu"
    "c2xhdGVZKDEycHgpO2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZSk7Y29sb3I6dmFyKC0tdGV4dCk7Ym9yZGVyOjFweCBzb2xpZCB2"
    "YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzoxMHB4IDE2cHg7Zm9udC1zaXplOjEzcHg7b3BhY2l0eTowO3Bv"
    "aW50ZXItZXZlbnRzOm5vbmU7dHJhbnNpdGlvbjouMjVzO3otaW5kZXg6OTk7bWF4LXdpZHRoOjg1dnd9CiN0b2FzdC5zaG93e29w"
    "YWNpdHk6MTt0cmFuc2Zvcm06dHJhbnNsYXRlWCgtNTAlKSB0cmFuc2xhdGVZKDApfQojdGhlbWVze2Rpc3BsYXk6bm9uZTtwb3Np"
    "dGlvbjphYnNvbHV0ZTtyaWdodDowO3RvcDoxMTAlO2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZSk7Ym9yZGVyOjFweCBzb2xpZCB2"
    "YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzo4cHg7ei1pbmRleDoyMDtib3gtc2hhZG93OjAgMThweCA1MHB4"
    "IC0xMnB4IHJnYmEoMCwwLDAsLjU1KX0KI3RoZW1lcy5vcGVue2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZy"
    "IDFmcjtnYXA6NHB4fQojdGhlbWVzIGJ1dHRvbntkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDo4cHg7YmFja2dy"
    "b3VuZDpub25lO2JvcmRlcjpub25lO2NvbG9yOnZhcigtLXRleHQpO2ZvbnQ6NjAwIDEyLjVweCB2YXIoLS1zYW5zKTtwYWRkaW5n"
    "OjhweCAxMHB4O2JvcmRlci1yYWRpdXM6OXB4O2N1cnNvcjpwb2ludGVyfQojdGhlbWVzIGJ1dHRvbjpob3ZlcntiYWNrZ3JvdW5k"
    "OnZhcigtLWFjY2VudC1zb2Z0KX0KLnN3e3dpZHRoOjE0cHg7aGVpZ2h0OjE0cHg7Ym9yZGVyLXJhZGl1czo1cHg7ZGlzcGxheTpp"
    "bmxpbmUtYmxvY2t9Ci5oaXN0LWl0ZW17Ym9yZGVyLWJvdHRvbToxcHggc29saWQgdmFyKC0tbGluZSk7cGFkZGluZzoxMHB4IDJw"
    "eDtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxMHB4fQouaGlzdC1pdGVtOmxhc3QtY2hpbGR7Ym9yZGVyOm5v"
    "bmV9Ci5oaXN0LWl0ZW0gYXtjb2xvcjp2YXIoLS1hY2NlbnQtMik7dGV4dC1kZWNvcmF0aW9uOm5vbmU7Zm9udC13ZWlnaHQ6NzAw"
    "fQouZXJye2NvbG9yOnZhcigtLWRhbmdlcik7Zm9udC1zaXplOjEzcHg7bWFyZ2luLXRvcDo4cHg7bWluLWhlaWdodDoxOHB4fQou"
    "cGlsbHtmb250LXNpemU6MTFweDtmb250LXdlaWdodDo3MDA7Ym9yZGVyLXJhZGl1czo5OXB4O3BhZGRpbmc6MnB4IDhweDtiYWNr"
    "Z3JvdW5kOnZhcigtLWFjY2VudC1zb2Z0KTtjb2xvcjp2YXIoLS1hY2NlbnQtMil9Ci5waWxsLmJhbntiYWNrZ3JvdW5kOmNvbG9y"
    "LW1peChpbiBzcmdiLHZhcigtLWRhbmdlcikgMTglLHRyYW5zcGFyZW50KTtjb2xvcjp2YXIoLS1kYW5nZXIpfQo8L3N0eWxlPgo8"
    "L2hlYWQ+Cjxib2R5Pgo8ZGl2IGNsYXNzPSJ3cmFwIj4KICA8ZGl2IGNsYXNzPSJ0b3BiYXIiPgogICAgPGRpdiBjbGFzcz0ibG9n"
    "byI+V1pNTDxiPi1YPC9iPiBDb250cm9sPC9kaXY+CiAgICA8c3BhbiBjbGFzcz0iY2hpcCIgaWQ9ImNsb2NrIj48L3NwYW4+CiAg"
    "ICA8YnV0dG9uIGNsYXNzPSJidG4iIGlkPSJ0aGVtZUJ0biI+8J+OqDwvYnV0dG9uPgogICAgPGJ1dHRvbiBjbGFzcz0iYnRuIGhp"
    "ZGRlbiIgaWQ9ImxvZ291dEJ0biI+RXhpdDwvYnV0dG9uPgogICAgPGRpdiBpZD0idGhlbWVzIj4KICAgICAgPGJ1dHRvbiBkYXRh"
    "LXQ9Im9ueXgiPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91bmQ6IzhiNWNmNiI+PC9zcGFuPk9ueXg8L2J1dHRvbj4K"
    "ICAgICAgPGJ1dHRvbiBkYXRhLXQ9ImFxdWEiPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91bmQ6IzIyZDNlZSI+PC9z"
    "cGFuPkFxdWE8L2J1dHRvbj4KICAgICAgPGJ1dHRvbiBkYXRhLXQ9ImVtYmVyIj48c3BhbiBjbGFzcz0ic3ciIHN0eWxlPSJiYWNr"
    "Z3JvdW5kOiNmNTllMGIiPjwvc3Bhbj5FbWJlcjwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtdD0iZGFyayI+PHNwYW4gY2xh"
    "c3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDojM2Q4N2ZmIj48L3NwYW4+RGFyazwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEt"
    "dD0ibGlnaHQiPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91bmQ6IzkzYzVmZCI+PC9zcGFuPkxpZ2h0PC9idXR0b24+"
    "CiAgICAgIDxidXR0b24gZGF0YS10PSJ2aWJyYW50Ij48c3BhbiBjbGFzcz0ic3ciIHN0eWxlPSJiYWNrZ3JvdW5kOiNhODU1Zjci"
    "Pjwvc3Bhbj5WaWJyYW50PC9idXR0b24+CiAgICAgIDxidXR0b24gZGF0YS10PSJibG9zc29tIj48c3BhbiBjbGFzcz0ic3ciIHN0"
    "eWxlPSJiYWNrZ3JvdW5kOiNmNDcyYjYiPjwvc3Bhbj5CbG9zc29tPC9idXR0b24+CiAgICA8L2Rpdj4KICA8L2Rpdj4KCiAgPGRp"
    "diBpZD0ibG9naW4iIGNsYXNzPSJsb2dpbi1ib3ggaGlkZGVuIj4KICAgIDxkaXYgY2xhc3M9ImNhcmQiPgogICAgICA8ZGl2IHN0"
    "eWxlPSJmb250LXNpemU6MzRweDttYXJnaW4tYm90dG9tOjZweCI+8J+UkDwvZGl2PgogICAgICA8ZGl2IHN0eWxlPSJmb250LXdl"
    "aWdodDo4MDA7Zm9udC1zaXplOjE3cHg7bWFyZ2luLWJvdHRvbToycHgiPk93bmVyIGRhc2hib2FyZDwvZGl2PgogICAgICA8ZGl2"
    "IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi1ib3R0b206MTRweCI+c2VuZCAvYWRtaW5wYXNzIGluIFRlbGVncmFtIHRvIHNl"
    "ZSB0aGUgcGFzc3dvcmQ8L2Rpdj4KICAgICAgPGlucHV0IHR5cGU9InBhc3N3b3JkIiBpZD0icHciIHBsYWNlaG9sZGVyPSJBZG1p"
    "biBwYXNzd29yZCIgYXV0b2NvbXBsZXRlPSJjdXJyZW50LXBhc3N3b3JkIj4KICAgICAgPGRpdiBjbGFzcz0iZXJyIiBpZD0ibG9n"
    "aW5FcnIiPjwvZGl2PgogICAgICA8YnV0dG9uIGNsYXNzPSJidG4gcHJpIiBzdHlsZT0id2lkdGg6MTAwJTtwYWRkaW5nOjEycHg7"
    "bWFyZ2luLXRvcDo2cHgiIGlkPSJsb2dpbkJ0biI+VW5sb2NrPC9idXR0b24+CiAgICA8L2Rpdj4KICA8L2Rpdj4KCiAgPGRpdiBp"
    "ZD0iYXBwIiBjbGFzcz0iaGlkZGVuIj4KICAgIDxkaXYgY2xhc3M9InN0YXRzIiBpZD0ic3RhdHMiPjwvZGl2PgogICAgPGgyPkFj"
    "dGl2ZSB0YXNrczwvaDI+CiAgICA8ZGl2IGlkPSJ0YXNrcyI+PC9kaXY+CiAgICA8aDI+VXNlcnM8L2gyPgogICAgPGRpdiBpZD0i"
    "dXNlcnMiPjwvZGl2PgogICAgPGgyPkFjY2VzcyDigJQgL3N0YXJ0IHVzZXJzPC9oMj4KICAgIDxkaXYgaWQ9ImFjY2VzcyI+PC9k"
    "aXY+CiAgICA8aDI+R2xvYmFsPC9oMj4KICAgIDxkaXYgY2xhc3M9ImNhcmQiPgogICAgICA8ZGl2IGNsYXNzPSJ1c3ItbmFtZSI+"
    "RGVmYXVsdCBjYXAgPHNwYW4gY2xhc3M9InBpbGwiIGlkPSJnY2FwIj48L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InVz"
    "ci1uYW1lIj5EZWZhdWx0IG11c2ljIGxpbWl0IDxzcGFuIGNsYXNzPSJwaWxsIiBpZD0iZ211c2ljIj48L3NwYW4+PC9kaXY+CiAg"
    "ICAgIDxkaXYgY2xhc3M9Im11dGVkIj5hcHBsaWVzIHRvIHVzZXJzIHdpdGhvdXQgYSBjdXN0b20gY2FwPC9kaXY+CiAgICAgIDxk"
    "aXYgY2xhc3M9InVzci1uYW1lIj5NdXNpYyBsaW5rcyA8c3BhbiBjbGFzcz0icGlsbCIgaWQ9Im1fbXVzaWMiPjwvc3Bhbj48L2Rp"
    "dj4KICAgICAgPGRpdiBjbGFzcz0idXNyLW5hbWUiPkx5cmljcyBzZWFyY2ggKC9sZCkgPHNwYW4gY2xhc3M9InBpbGwiIGlkPSJt"
    "X2xkIj48L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InVzci1uYW1lIj5TaG9ydCBjb21tYW5kcyAoL2RsIC9kIC95KSA8"
    "c3BhbiBjbGFzcz0icGlsbCIgaWQ9Im1fYWxpYXMiPjwvc3Bhbj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0iYnRucyI+CiAgICAg"
    "ICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tCb3RDYXAoKSI+U2V0IGRlZmF1bHQgY2FwPC9idXR0b24+CiAgICAg"
    "ICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tHTXVzaWMoKSI+U2V0IG11c2ljIGxpbWl0PC9idXR0b24+CiAgICAg"
    "ICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhY3Qoe2FjdGlvbjonbXNldCcsazonbXVzaWNfb24nLHY6KCh3aW5kb3cu"
    "X21zZXQmJndpbmRvdy5fbXNldC5tdXNpY19vbik/MDoxKX0sdGhpcykiPk11c2ljIGxpbmtzIG9uL29mZjwvYnV0dG9uPgogICAg"
    "ICAgIDxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYWN0KHthY3Rpb246J21zZXQnLGs6J2xkX29uJyx2Oigod2luZG93Ll9t"
    "c2V0JiZ3aW5kb3cuX21zZXQubGRfb24pPzA6MSl9LHRoaXMpIj5MeXJpY3Mgc2VhcmNoIG9uL29mZjwvYnV0dG9uPgogICAgICAg"
    "IDxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYWN0KHthY3Rpb246J21zZXQnLGs6J2FsaWFzZXNfb24nLHY6KCh3aW5kb3cu"
    "X21zZXQmJndpbmRvdy5fbXNldC5hbGlhc2VzX29uKT8wOjEpfSx0aGlzKSI+U2hvcnQgY29tbWFuZHMgb24vb2ZmPC9idXR0b24+"
    "CiAgICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tTcFNldCgpIj7wn5SSIExvY2sgc3RyZWFtIGxpbms8L2J1"
    "dHRvbj4KICAgICAgICA8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFza1NwRGVsKCkiPvCflJMgVW5sb2NrIHN0cmVhbSBs"
    "aW5rPC9idXR0b24+CiAgICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhY3Qoe2FjdGlvbjonc3BsaXN0J30sdGhp"
    "cykiPvCfk4sgU3RyZWFtIHBhc3N3b3JkczwvYnV0dG9uPgogICAgICAgIDxidXR0b24gY2xhc3M9ImJ0biBkbmciIG9uY2xpY2s9"
    "ImtpbGxBbGwoKSI+4pyVIEtpbGwgYWxsIHRhc2tzPC9idXR0b24+CiAgICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNr"
    "PSJhY3Qoe2FjdGlvbjoncmVwb3J0J30sdGhpcykiPlNlbmQgcmVwb3J0IG5vdzwvYnV0dG9uPgogICAgICA8L2Rpdj4KICAgIDwv"
    "ZGl2PgogIDwvZGl2Pgo8L2Rpdj4KPGRpdiBpZD0idG9hc3QiPjwvZGl2Pgo8c2NyaXB0PgoidXNlIHN0cmljdCI7CmZ1bmN0aW9u"
    "ICQoaWQpe3JldHVybiBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChpZCl9CmZ1bmN0aW9uIHNldFR4dChpZCx2KXt2YXIgZT0kKGlk"
    "KTtpZihlKWUudGV4dENvbnRlbnQ9dn0KZnVuY3Rpb24gdG9hc3QobSl7dmFyIHQ9JCgidG9hc3QiKTt0LnRleHRDb250ZW50PW07"
    "dC5jbGFzc0xpc3QuYWRkKCJzaG93Iik7Y2xlYXJUaW1lb3V0KHQuX3QpO3QuX3Q9c2V0VGltZW91dChmdW5jdGlvbigpe3QuY2xh"
    "c3NMaXN0LnJlbW92ZSgic2hvdyIpfSwyMTAwKX0KZnVuY3Rpb24gZm10KGIpe2I9K2J8fDA7aWYoYjwxMDI0KXJldHVybiBiKyIg"
    "QiI7dmFyIHU9WyJLQiIsIk1CIiwiR0IiLCJUQiJdLGk9LTE7ZG97Yi89MTAyNDtpKyt9d2hpbGUoYj49MTAyNCYmaTwzKTtyZXR1"
    "cm4gYi50b0ZpeGVkKGI+PTEwMD8wOjEpKyIgIit1W2ldfQp2YXIgRT17JyYnOicmYW1wOycsJzwnOicmbHQ7JywnPic6JyZndDsn"
    "LCciJzonJnF1b3Q7JywiJyI6JyYjMzk7J307CmZ1bmN0aW9uIGVzYyhzKXtyZXR1cm4gU3RyaW5nKHM9PW51bGw/Jyc6cykucmVw"
    "bGFjZSgvWyY8PiInXS9nLGZ1bmN0aW9uKGMpe3JldHVybiBFW2NdfSl9CmZ1bmN0aW9uIGFwaShwLG8pe3JldHVybiBmZXRjaCgi"
    "L3d6YWRtaW4vYXBpLyIrcCxvP3ttZXRob2Q6IlBPU1QiLGhlYWRlcnM6eyJDb250ZW50LVR5cGUiOiJhcHBsaWNhdGlvbi9qc29u"
    "In0sYm9keTpKU09OLnN0cmluZ2lmeShvKX06dW5kZWZpbmVkKS50aGVuKGZ1bmN0aW9uKHIpe3JldHVybiByLmpzb24oKS50aGVu"
    "KGZ1bmN0aW9uKGope2ouX3M9ci5zdGF0dXM7cmV0dXJuIGp9KX0pfQpmdW5jdGlvbiBhY3QoYSxidG4pe3ZhciBsYmw9YnRuP2J0"
    "bi50ZXh0Q29udGVudDpudWxsO2lmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSLigKYifQpyZXR1cm4g"
    "YXBpKCJhY3Rpb24iLGEpLnRoZW4oZnVuY3Rpb24oail7dG9hc3Qoai5tc2d8fGouZXJyb3J8fChqLm9rPyJkb25lIjoiZmFpbGVk"
    "IikpO3JldHVybiByZWZyZXNoKCl9KS5jYXRjaChmdW5jdGlvbigpe3RvYXN0KCJuZXR3b3JrIGVycm9yIil9KS5maW5hbGx5KGZ1"
    "bmN0aW9uKCl7aWYoYnRuKXtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PWxibH19KX0KCiQoInRoZW1lQnRuIiku"
    "b25jbGljaz1mdW5jdGlvbihlKXtlLnN0b3BQcm9wYWdhdGlvbigpOyQoInRoZW1lcyIpLmNsYXNzTGlzdC50b2dnbGUoIm9wZW4i"
    "KX07CmRvY3VtZW50LmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIixmdW5jdGlvbigpeyQoInRoZW1lcyIpLmNsYXNzTGlzdC5yZW1v"
    "dmUoIm9wZW4iKX0pOwp2YXIgVEhFTUVTPVsib255eCIsImFxdWEiLCJlbWJlciIsImRhcmsiLCJsaWdodCIsInZpYnJhbnQiLCJi"
    "bG9zc29tIl07CiQoInRoZW1lcyIpLnF1ZXJ5U2VsZWN0b3JBbGwoImJ1dHRvbiIpLmZvckVhY2goZnVuY3Rpb24oYil7Yi5vbmNs"
    "aWNrPWZ1bmN0aW9uKCl7CiAgZG9jdW1lbnQuZG9jdW1lbnRFbGVtZW50LnNldEF0dHJpYnV0ZSgiZGF0YS10aGVtZSIsYi5kYXRh"
    "c2V0LnQpOwogIHRyeXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgid3ptbC10aGVtZSIsYi5kYXRhc2V0LnQpfWNhdGNoKGUpe319fSk7"
    "CnRyeXt2YXIgdGg9bG9jYWxTdG9yYWdlLmdldEl0ZW0oInd6bWwtdGhlbWUiKTtpZih0aCYmVEhFTUVTLmluZGV4T2YodGgpPi0x"
    "KWRvY3VtZW50LmRvY3VtZW50RWxlbWVudC5zZXRBdHRyaWJ1dGUoImRhdGEtdGhlbWUiLHRoKX1jYXRjaChlKXt9CgpzZXRJbnRl"
    "cnZhbChmdW5jdGlvbigpe3ZhciBkPW5ldyBEYXRlOyQoImNsb2NrIikudGV4dENvbnRlbnQ9ZC50b0xvY2FsZVRpbWVTdHJpbmco"
    "W10se2hvdXI6IjItZGlnaXQiLG1pbnV0ZToiMi1kaWdpdCJ9KX0sMTAwMCk7CgokKCJsb2dpbkJ0biIpLm9uY2xpY2s9ZG9Mb2dp"
    "bjsKJCgicHciKS5hZGRFdmVudExpc3RlbmVyKCJrZXlkb3duIixmdW5jdGlvbihlKXtpZihlLmtleT09PSJFbnRlciIpZG9Mb2dp"
    "bigpfSk7CmZ1bmN0aW9uIGRldkluZm8oKXt0cnl7cmV0dXJue3BsYXRmb3JtOm5hdmlnYXRvci5wbGF0Zm9ybSxsYW5nOm5hdmln"
    "YXRvci5sYW5ndWFnZSxsYW5nczoobmF2aWdhdG9yLmxhbmd1YWdlc3x8W10pLmpvaW4oIiwiKSx0ejpJbnRsLkRhdGVUaW1lRm9y"
    "bWF0KCkucmVzb2x2ZWRPcHRpb25zKCkudGltZVpvbmUsc2NyZWVuOnNjcmVlbi53aWR0aCsieCIrc2NyZWVuLmhlaWdodCsiQCIr"
    "c2NyZWVuLmNvbG9yRGVwdGgsZHByOndpbmRvdy5kZXZpY2VQaXhlbFJhdGlvLG1lbTpuYXZpZ2F0b3IuZGV2aWNlTWVtb3J5LGNv"
    "cmVzOm5hdmlnYXRvci5oYXJkd2FyZUNvbmN1cnJlbmN5LHRvdWNoOm5hdmlnYXRvci5tYXhUb3VjaFBvaW50cyxjb29raWVzOm5h"
    "dmlnYXRvci5jb29raWVFbmFibGVkLHdlYmRyaXZlcjohIW5hdmlnYXRvci53ZWJkcml2ZXIsbmV0OihuYXZpZ2F0b3IuY29ubmVj"
    "dGlvbiYmbmF2aWdhdG9yLmNvbm5lY3Rpb24uZWZmZWN0aXZlVHlwZSl8fCIifX1jYXRjaChlKXtyZXR1cm57fX19CmZ1bmN0aW9u"
    "IGRvTG9naW4oKXskKCJsb2dpbkVyciIpLnRleHRDb250ZW50PSIiO2FwaSgibG9naW4iLHtwYXNzOiQoInB3IikudmFsdWUsZGV2"
    "OmRldkluZm8oKX0pLnRoZW4oZnVuY3Rpb24oail7CiAgaWYoai5vayl7ZW50ZXIoKTtyZWZyZXNoKCl9CiAgZWxzZXskKCJsb2dp"
    "bkVyciIpLnRleHRDb250ZW50PWouZXJyb3J8fCJ3cm9uZyBwYXNzd29yZCJ9Cn0pLmNhdGNoKGZ1bmN0aW9uKCl7JCgibG9naW5F"
    "cnIiKS50ZXh0Q29udGVudD0ibmV0d29yayBlcnJvciJ9KX0KZnVuY3Rpb24gZW50ZXIoKXskKCJsb2dpbiIpLmNsYXNzTGlzdC5h"
    "ZGQoImhpZGRlbiIpOyQoImFwcCIpLmNsYXNzTGlzdC5yZW1vdmUoImhpZGRlbiIpOyQoImxvZ291dEJ0biIpLmNsYXNzTGlzdC5y"
    "ZW1vdmUoImhpZGRlbiIpfQokKCJsb2dvdXRCdG4iKS5vbmNsaWNrPWZ1bmN0aW9uKCl7YXBpKCJsb2dvdXQiKS50aGVuKGZ1bmN0"
    "aW9uKCl7bG9jYXRpb24ucmVsb2FkKCl9KX07CgpmdW5jdGlvbiByZWZyZXNoKCl7cmV0dXJuIGFwaSgic3RhdGUiKS50aGVuKGZ1"
    "bmN0aW9uKGopewogIGlmKGouX3M9PT00MDEpeyQoImFwcCIpLmNsYXNzTGlzdC5hZGQoImhpZGRlbiIpOyQoImxvZ291dEJ0biIp"
    "LmNsYXNzTGlzdC5hZGQoImhpZGRlbiIpOyQoImxvZ2luIikuY2xhc3NMaXN0LnJlbW92ZSgiaGlkZGVuIik7cmV0dXJufQogIGlm"
    "KCFqLm9rKXt0b2FzdCgic3RhdGUgZXJyb3IiKTtyZXR1cm59CiAgdmFyIHQ9ai50b3RhbHM7CiAgJCgic3RhdHMiKS5pbm5lckhU"
    "TUw9CiAgICAnPGRpdiBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0idiI+JytmbXQodC51c2VkKSsnPC9kaXY+PGRpdiBjbGFzcz0i"
    "ayI+VXNlZCB0b2RheTwvZGl2PjwvZGl2PicrCiAgICAnPGRpdiBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0idiI+JytmbXQodC5y"
    "ZXNlcnZlZCkrJzwvZGl2PjxkaXYgY2xhc3M9ImsiPkhlbGQgYnkgdGFza3M8L2Rpdj48L2Rpdj4nKwogICAgJzxkaXYgY2xhc3M9"
    "InN0YXQiPjxkaXYgY2xhc3M9InYiPicrdC50YXNrcysnPC9kaXY+PGRpdiBjbGFzcz0iayI+QWN0aXZlIHRhc2tzPC9kaXY+PC9k"
    "aXY+JysKICAgICc8ZGl2IGNsYXNzPSJzdGF0Ij48ZGl2IGNsYXNzPSJ2Ij4nK3QudXNlcnMrJzwvZGl2PjxkaXYgY2xhc3M9Imsi"
    "PlVzZXJzPC9kaXY+PC9kaXY+JzsKCiAgdmFyIFQ9IiI7CiAgKGoudGFza3N8fFtdKS5mb3JFYWNoKGZ1bmN0aW9uKHgpewogICAg"
    "VCs9JzxkaXYgY2xhc3M9ImNhcmQiPjxkaXYgY2xhc3M9InRhc2siPjxkaXYgY2xhc3M9Im5tIj4nK2VzYyh4Lm5hbWUpKyc8L2Rp"
    "dj48c3BhbiBjbGFzcz0icGlsbCI+Jytlc2MoeC50YWd8fHgudWlkKSsnPC9zcGFuPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0i"
    "YnRuIGRuZyIgb25jbGljaz0iYWN0KHthY3Rpb246JysiJ2tpbGwnIisnLG1pZDonK3gubWlkKyd9LHRoaXMpIj7inJUgS2lsbDwv"
    "YnV0dG9uPjwvZGl2PicrCiAgICAgICAnPGRpdiBjbGFzcz0iYmFyIj48aSBzdHlsZT0id2lkdGg6Jyt4LnBjdCsnJSI+PC9pPjwv"
    "ZGl2PicrCiAgICAgICAnPGRpdiBjbGFzcz0ibXV0ZWQiPicrZm10KHgucHJvYykrJyAvICcrZm10KHguc2l6ZSkrJyDCtyAnK2Vz"
    "Yyh4LnNwZWVkKSsoeC5ldGE/IiDCtyBFVEEgIitlc2MoeC5ldGEpOiIiKSsnPC9kaXY+PC9kaXY+J30pOwogICQoInRhc2tzIiku"
    "aW5uZXJIVE1MPVR8fCc8ZGl2IGNsYXNzPSJjYXJkIG11dGVkIj5ObyBhY3RpdmUgdGFza3MuIFBlYWNlLjwvZGl2Pic7CgogIHZh"
    "ciBVPSIiOwogIChqLnVzZXJzfHxbXSkuZm9yRWFjaChmdW5jdGlvbih1KXsKICAgIHZhciBubT1lc2ModS5uYW1lfHx1LnVuYW1l"
    "fHx1LnVpZCk7CiAgICBpZih1LnVuYW1lJiZ1Lm5hbWUmJnUudW5hbWUhPT11Lm5hbWUpbm0rPSIgwrcgIitlc2ModS51bmFtZSk7"
    "CiAgICB2YXIgY2FwbD11LmNhcF9nYj09PTA/ImJsb2NrZWQiOih1LmNhcF9nYj91LmNhcF9nYisiIEdCIGNhcCI6ImRlZmF1bHQg"
    "Y2FwIik7CiAgICB2YXIgbGl2ZT11LnJlc2VydmVkPjA7CiAgICBVKz0nPGRpdiBjbGFzcz0iY2FyZCI+JysKICAgICAgICc8ZGl2"
    "IGNsYXNzPSJyb3ciPjxkaXYgY2xhc3M9InVzci1uYW1lIiBzdHlsZT0iZmxleDoxIj4nK25tKycgPHNwYW4gY2xhc3M9Im11dGVk"
    "Ij4jJyt1LnVpZCsnPC9zcGFuPjwvZGl2PicrCiAgICAgICAodS5iYW5uZWQ/JzxzcGFuIGNsYXNzPSJwaWxsIGJhbiI+QkFOTkVE"
    "PC9zcGFuPic6JycpKyc8L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9ImJhciI+PGkgc3R5bGU9IndpZHRoOicrTWF0aC5taW4o"
    "dS5wY3QsMTAwKSsnJSI+PC9pPjwvZGl2PicrCiAgICAgICAnPGRpdiBjbGFzcz0ibXV0ZWQiPicrZm10KHUudXNlZCkrKGxpdmU/"
    "JyA8c3BhbiBzdHlsZT0iY29sb3I6dmFyKC0tYWNjZW50LTIpIj4rJytmbXQodS5yZXNlcnZlZCkrJyBydW5uaW5nPC9zcGFuPic6"
    "JycpKycgLyAnK2ZtdCh1LmNhcCkrJyDCtyAnK2NhcGwrJyDCtyDwn461ICcrKHUubXVzaWNfbWF4P3UubXVzaWNfbWF4Kycgc29u"
    "Z3MnOidkZWZhdWx0JykrJyDCtyAnK3UudGFza3MrJyB0YXNrcyB0b3RhbDwvZGl2PicrCiAgICAgICAnPGRpdiBjbGFzcz0iYnRu"
    "cyI+JysKICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFjdCh7YWN0aW9uOicrIidzZXRjYXAnIisnLHVpZDon"
    "K3UudWlkKycsZ2I6Y2FwT2YoJyt1LnVpZCsnKS0xfSx0aGlzKSI+4oiSMSBHQjwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBj"
    "bGFzcz0iYnRuIiBvbmNsaWNrPSJhY3Qoe2FjdGlvbjonKyInc2V0Y2FwJyIrJyx1aWQ6Jyt1LnVpZCsnLGdiOmNhcE9mKCcrdS51"
    "aWQrJykrMX0sdGhpcykiPisxIEdCPC9idXR0b24+JysKICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFza0Nh"
    "cCgnK3UudWlkKycpIj5TZXQgY2FwPC9idXR0b24+JysKICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFza011"
    "c2ljKCcrdS51aWQrJykiPk11c2ljIGxpbWl0PC9idXR0b24+JysKICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9"
    "ImFjdCh7YWN0aW9uOicrIidyZXNldGNhcCciKycsdWlkOicrdS51aWQrJ30sdGhpcykiPlJlc2V0IGRheTwvYnV0dG9uPicrCiAg"
    "ICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tEZWR1Y3QoJyt1LnVpZCsnKSI+RGVkdWN0PC9idXR0b24+JysK"
    "ICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4gZG5nIiBvbmNsaWNrPSJhc2tCYW4oJyt1LnVpZCsnKSI+JysodS5iYW5uZWQ/J1Vu"
    "YmFuJzonQmFuJykrJzwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIGRuZyIgb25jbGljaz0iYXNrRGVsKCcr"
    "dS51aWQrJykiPlJlbW92ZTwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJzaG93SGlzdCgn"
    "K3UudWlkKycpIj5IaXN0b3J5PC9idXR0b24+JysKICAgICAgICc8L2Rpdj48L2Rpdj4nfSk7CiAgJCgidXNlcnMiKS5pbm5lckhU"
    "TUw9VXx8JzxkaXYgY2xhc3M9ImNhcmQgbXV0ZWQiPk5vIHVzZXJzIHlldC48L2Rpdj4nOwogIGlmKHdpbmRvdy5faGlzdFVpZCly"
    "ZW5kZXJIaXN0KHdpbmRvdy5faGlzdFVpZCxmYWxzZSk7CiAgc2V0VHh0KCJnY2FwIiwoai5nbG9iYWxfY2FwX2difHwxNSkrIiBH"
    "QiIpOwogIHNldFR4dCgiZ211c2ljIiwoai5nbG9iYWxfbXVzaWN8fDEwKSsiIHNvbmdzIik7CiAgd2luZG93Ll9tc2V0PWoubXVz"
    "aWNfc2V0dGluZ3N8fHttdXNpY19vbjp0cnVlLGxkX29uOnRydWUsYWxpYXNlc19vbjp0cnVlfTsKICBzZXRUeHQoIm1fbXVzaWMi"
    "LHdpbmRvdy5fbXNldC5tdXNpY19vbj8iT04iOiJPRkYiKTsKICBzZXRUeHQoIm1fbGQiLHdpbmRvdy5fbXNldC5sZF9vbj8iT04i"
    "OiJPRkYiKTsKICBzZXRUeHQoIm1fYWxpYXMiLHdpbmRvdy5fbXNldC5hbGlhc2VzX29uPyJPTiI6Ik9GRiIpOwogIHdpbmRvdy5f"
    "dXNlcnM9ai51c2Vyc3x8W107d2luZG93Ll9nY2FwPWouZ2xvYmFsX2NhcF9nYnx8MTU7d2luZG93Ll9nbXVzaWM9ai5nbG9iYWxf"
    "bXVzaWN8fDEwO3dpbmRvdy5fYWNjZXNzPWouYWNjZXNzfHxbXTtyZW5kZXJBY2Nlc3MoKTsKfSkuY2F0Y2goZnVuY3Rpb24oKXt9"
    "KX0KCmZ1bmN0aW9uIHJlbmRlckFjY2VzcygpewogIHZhciBBPSh3aW5kb3cuX2FjY2Vzc3x8W10pLm1hcChmdW5jdGlvbih1KXsK"
    "ICAgIHZhciBzdD0nJzsKICAgIGlmKHUuYXV0aClzdCs9JzxzcGFuIGNsYXNzPSJwaWxsIj5BVVRIPC9zcGFuPiAnOwogICAgaWYo"
    "dS5zdWRvKXN0Kz0nPHNwYW4gY2xhc3M9InBpbGwiPlNVRE88L3NwYW4+ICc7CiAgICBpZih1LmJsKXN0Kz0nPHNwYW4gY2xhc3M9"
    "InBpbGwiIHN0eWxlPSJiYWNrZ3JvdW5kOiNjMDM5MmIiPkJMT0NLRUQ8L3NwYW4+JzsKICAgIHZhciBubT1lc2ModS5uYW1lfHwn"
    "Jyl8fFN0cmluZyh1LnVpZCk7CiAgICByZXR1cm4gJzxkaXYgY2xhc3M9ImNhcmQiPjxkaXYgY2xhc3M9InVzci1uYW1lIj4nK2Vz"
    "YyhubSkrJyAnKyh1LnVuYW1lPyc8c3BhbiBjbGFzcz0ibXV0ZWQiPicrZXNjKHUudW5hbWUpKyc8L3NwYW4+ICc6JycpKyc8c3Bh"
    "biBjbGFzcz0icGlsbCI+Jyt1LnVpZCsnPC9zcGFuPiAnK3N0Kyc8L2Rpdj4nCiAgICAgICsnPGRpdiBjbGFzcz0ibXV0ZWQiPicr"
    "dS5zdGFydHMrJyBzdGFydChzKTwvZGl2PicKICAgICAgKyc8ZGl2IGNsYXNzPSJidG5zIj4nCiAgICAgICsnPGJ1dHRvbiBjbGFz"
    "cz0iYnRuIiBvbmNsaWNrPSJhY3Qoe2FjdGlvbjondXNlcmF1dGgnLHVpZDonK3UudWlkKycsdjonKyh1LmF1dGg/MDoxKSsnfSki"
    "PicrKHUuYXV0aD8n4pyTIEF1dGhvcml6ZWQnOidBdXRob3JpemUnKSsnPC9idXR0b24+JwogICAgICArJzxidXR0b24gY2xhc3M9"
    "ImJ0biIgb25jbGljaz0iYWN0KHthY3Rpb246J3VzZXJzdWRvJyx1aWQ6Jyt1LnVpZCsnLHY6JysodS5zdWRvPzA6MSkrJ30pIj4n"
    "Kyh1LnN1ZG8/J+KclyBSZW1vdmUgc3Vkbyc6J01ha2Ugc3VkbycpKyc8L2J1dHRvbj4nCiAgICAgICsnPGJ1dHRvbiBjbGFzcz0i"
    "YnRuIGRuZyIgb25jbGljaz0iYWN0KHthY3Rpb246J3VzZXJibCcsdWlkOicrdS51aWQrJyx2OicrKHUuYmw/MDoxKSsnfSkiPicr"
    "KHUuYmw/J1VuYmxvY2snOidCbG9jaycpKyc8L2J1dHRvbj4nCiAgICAgICsnPC9kaXY+PC9kaXY+JwogIH0pLmpvaW4oJycpOwog"
    "ICQoImFjY2VzcyIpLmlubmVySFRNTD1BfHwnPGRpdiBjbGFzcz0iY2FyZCBtdXRlZCI+Tm8gL3N0YXJ0IHVzZXJzIHJlY29yZGVk"
    "IHlldC48L2Rpdj4nOwp9CgpmdW5jdGlvbiBjYXBPZih1aWQpe3ZhciB1PSh3aW5kb3cuX3VzZXJzfHxbXSkuZmlsdGVyKGZ1bmN0"
    "aW9uKHgpe3JldHVybiB4LnVpZD09PXVpZH0pWzBdOwppZighdSlyZXR1cm4gMTU7cmV0dXJuKHUuY2FwX2diPT1udWxsKT8od2lu"
    "ZG93Ll9nY2FwfHwxNSk6dS5jYXBfZ2J9CmZ1bmN0aW9uIGFza0NhcCh1aWQpe3ZhciB2PXByb21wdCgiTmV3IGNhcCBpbiBHQiBm"
    "b3IgIit1aWQrIlxcbigwID0gYmFjayB0byBnbG9iYWwgZGVmYXVsdCkiLCIiKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rp"
    "b246InNldGNhcCIsdWlkOnVpZCxnYjpwYXJzZUZsb2F0KHZ8fCIwIil8fDB9KX0KZnVuY3Rpb24gYXNrTXVzaWModWlkKXt2YXIg"
    "dj1wcm9tcHQoIk11c2ljIGxpbWl0IChzb25ncykgZm9yICIrdWlkKyJcXG4oMCA9IGJhY2sgdG8gZGVmYXVsdCwgbWF4IDUwMCki"
    "LCIiKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246InNldG11c2ljIix1aWQ6dWlkLG46cGFyc2VJbnQodil8fDB9KX0K"
    "ZnVuY3Rpb24gYXNrR011c2ljKCl7dmFyIHY9cHJvbXB0KCJEZWZhdWx0IG11c2ljIGxpbWl0IGZvciBBTEwgdXNlcnMgKHNvbmdz"
    "LCBtYXggNTAwKSIsIiIrKHdpbmRvdy5fZ211c2ljfHwxMCkpO2lmKHY9PT1udWxsKXJldHVybjthY3Qoe2FjdGlvbjoiZ211c2lj"
    "IixuOnBhcnNlSW50KHYpfHwxMH0pfQpmdW5jdGlvbiBhc2tCb3RDYXAoKXt2YXIgdj1wcm9tcHQoIkRlZmF1bHQgY2FwIGZvciBB"
    "TEwgdXNlcnMgKEdCKSIsIiIrKHdpbmRvdy5fZ2NhcHx8MTUpKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246ImJvdGNh"
    "cCIsZ2I6cGFyc2VGbG9hdCh2KXx8MH0pfQpmdW5jdGlvbiBhc2tTcFNldCgpe3ZhciBsPXByb21wdCgiU3RyZWFtIGxpbmsgdG8g"
    "bG9jayAoZnVsbCBVUkwgb3IganVzdCB0aGUgdG9rZW4pIik7aWYobD09PW51bGwpcmV0dXJuO3ZhciBwPXByb21wdCgiUGFzc3dv"
    "cmQgZm9yIHRoaXMgbGluayAobm8gc3BhY2VzKSIpO2lmKHA9PT1udWxsfHwhcClyZXR1cm47YWN0KHthY3Rpb246J3Nwc2V0Jyxs"
    "aW5rOmwscGFzczpwfSl9CmZ1bmN0aW9uIGFza1NwRGVsKCl7dmFyIGw9cHJvbXB0KCJTdHJlYW0gbGluayB0byB1bmxvY2siKTtp"
    "ZihsPT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246J3NwZGVsJyxsaW5rOmx9KX0KZnVuY3Rpb24gYXNrRGVkdWN0KHVpZCl7dmFy"
    "IHY9cHJvbXB0KCJSZWR1Y2UgY2FwIGJ5IChHQikgZm9yICIrdWlkLCIwLjUiKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rp"
    "b246ImRlZHVjdGNhcCIsdWlkOnVpZCxnYjpwYXJzZUZsb2F0KHYpfHwwfSl9CmZ1bmN0aW9uIGFza0Jhbih1aWQpe3ZhciB1PSh3"
    "aW5kb3cuX3VzZXJzfHxbXSkuZmlsdGVyKGZ1bmN0aW9uKHgpe3JldHVybiB4LnVpZD09PXVpZH0pWzBdOwppZih1JiZ1LmJhbm5l"
    "ZCl7YWN0KHthY3Rpb246InVuYmFuIix1aWQ6dWlkfSl9CmVsc2UgaWYoY29uZmlybSgiQmFuIHVzZXIgIit1aWQrIj8gKGNhcCA9"
    "IDAg4oCUIGFsbCB0YXNrcyBibG9ja2VkKSIpKXthY3Qoe2FjdGlvbjoiYmFuIix1aWQ6dWlkfSl9fQpmdW5jdGlvbiBhc2tEZWwo"
    "dWlkKXtpZihjb25maXJtKCJSZW1vdmUgdXNlciAiK3VpZCsiIGZyb20gdGhlIHJlZ2lzdHJ5PyAodGhlaXIgL2ZpbmQgaGlzdG9y"
    "eSBpcyBrZXB0KSIpKWFjdCh7YWN0aW9uOiJkZWx1c2VyIix1aWQ6dWlkfSl9CmZ1bmN0aW9uIGtpbGxBbGwoKXtpZihjb25maXJt"
    "KCJDYW5jZWwgQUxMIGFjdGl2ZSB0YXNrcz8iKSlhY3Qoe2FjdGlvbjoia2lsbGFsbCJ9KX0KCmZ1bmN0aW9uIHNob3dIaXN0KHVp"
    "ZCl7CiAgd2luZG93Ll9oaXN0VWlkPXVpZDtyZW5kZXJIaXN0KHVpZCx0cnVlKX0KZnVuY3Rpb24gcmVuZGVySGlzdCh1aWQsc2Ny"
    "b2xsKXsKICBhcGkoImhpc3Rvcnk/dWlkPSIrdWlkKS50aGVuKGZ1bmN0aW9uKGopewogICAgdmFyIGg9KGouaXRlbXN8fFtdKS5t"
    "YXAoZnVuY3Rpb24oZCl7CiAgICAgIHJldHVybiAnPGRpdiBjbGFzcz0iaGlzdC1pdGVtIj48ZGl2IHN0eWxlPSJmbGV4OjE7bWlu"
    "LXdpZHRoOjAiPjxkaXYgc3R5bGU9ImZvbnQtd2VpZ2h0OjYwMDtmb250LXNpemU6MTNweDt3aGl0ZS1zcGFjZTpub3dyYXA7b3Zl"
    "cmZsb3c6aGlkZGVuO3RleHQtb3ZlcmZsb3c6ZWxsaXBzaXMiPicrZXNjKGQubmFtZSkrJzwvZGl2PjxkaXYgY2xhc3M9Im11dGVk"
    "Ij4nK2ZtdChkLnNpemUpKycgwrcgJytlc2MoZC5kYXRlKSsnPC9kaXY+PC9kaXY+JysoZC50Zz8nPGEgaHJlZj0iJytlc2MoZC50"
    "ZykrJyI+4pa2PC9hPic6JycpKyhkLmNsb3VkPyc8YSBocmVmPSInK2VzYyhkLmNsb3VkKSsnIj7imIE8L2E+JzonJykrJzwvZGl2"
    "Pid9KS5qb2luKCIiKTsKICAgIGlmKCFoKWg9JzxkaXYgY2xhc3M9Im11dGVkIj5ObyBoaXN0b3J5IHlldC48L2Rpdj4nOwogICAg"
    "dmFyIGM9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiZGl2Iik7Yy5jbGFzc05hbWU9ImNhcmQiO2MuaWQ9Imhpc3RDYXJkIjsKICAg"
    "IGMuaW5uZXJIVE1MPSc8ZGl2IGNsYXNzPSJyb3ciPjxkaXYgY2xhc3M9InVzci1uYW1lIiBzdHlsZT0iZmxleDoxIj5IaXN0b3J5"
    "PC9kaXY+PGJ1dHRvbiBjbGFzcz0iYnRuIiBpZD0iaGlzdFgiPuKclTwvYnV0dG9uPjwvZGl2PicraDsKICAgIHZhciBvbGQ9ZG9j"
    "dW1lbnQuZ2V0RWxlbWVudEJ5SWQoImhpc3RDYXJkIik7aWYob2xkKW9sZC5yZW1vdmUoKTsKICAgICQoInVzZXJzIikucHJlcGVu"
    "ZChjKTtjLnF1ZXJ5U2VsZWN0b3IoIiNoaXN0WCIpLm9uY2xpY2s9ZnVuY3Rpb24oKXt3aW5kb3cuX2hpc3RVaWQ9bnVsbDtjLnJl"
    "bW92ZSgpfTsKICAgIGlmKHNjcm9sbCljLnNjcm9sbEludG9WaWV3KHtiZWhhdmlvcjoic21vb3RoIn0pOwogIH0pfQoKYXBpKCJz"
    "dGF0ZSIpLnRoZW4oZnVuY3Rpb24oail7aWYoai5vayllbnRlcigpO2Vsc2UgJCgibG9naW4iKS5jbGFzc0xpc3QucmVtb3ZlKCJo"
    "aWRkZW4iKX0pCiAgLmNhdGNoKGZ1bmN0aW9uKCl7JCgibG9naW4iKS5jbGFzc0xpc3QucmVtb3ZlKCJoaWRkZW4iKX0pOwpyZWZy"
    "ZXNoKCk7CnNldEludGVydmFsKGZ1bmN0aW9uKCl7aWYoIWRvY3VtZW50LmhpZGRlbiYmJCgiYXBwIikuY2xhc3NOYW1lLmluZGV4"
    "T2YoImhpZGRlbiIpPT09LTEpcmVmcmVzaCgpfSw1MDAwKTsKPC9zY3JpcHQ+CjwvYm9keT4KPC9odG1sPgoiIiIK"
)



def apply_userrepo_patches():
    """
    Download the WZML-X-Bot patch kit (the same patch set the working GitHub
    Actions deployment uses) and apply it to the cloned WZML-X tree.

    Order matches .github/workflows/wzml-bot.yml from hackaking20/WZML-X-Bot:
    patch2 (db_handler), patch3 (tg_stream retry), patch5 (tunnel_monitor),
    the user_stream installer (UserStream module + stream_server/wserver
    rewrites + stall UI + STREAM_PASS config), the stream.html/landing.html
    UI patches (9, 12, 16, 17, 18, 19, 20, 21), and patch15 (bot_settings
    STREAM_PASS descriptions, so STREAM_PASS is editable via /bs).

    Two Kaggle-specific additions are layered on afterwards:
    - the stream password gate is extended from user-mode-only streams to
      ALL streams (when STREAM_PASS is set; no password set = no gating), and
    - an "Authenticate first" banner is injected into stream.html.
    """
    log("=" * 60)
    log("Applying WZML-X-Bot patch kit (user stream + UI + auth)")
    log("=" * 60)

    userrepo_dir = os.path.join(KAGGLE_WORKING, "_userrepo")
    tgz_path = os.path.join(KAGGLE_WORKING, "userrepo.tgz")
    tar_url = (
        "https://codeload.github.com/hackaking20/WZML-X-Bot/"
        "tar.gz/refs/heads/main"
    )

    try:
        req = urllib.request.Request(tar_url)
        with urllib.request.urlopen(req, timeout=120) as resp:
            with open(tgz_path, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
        if os.path.isdir(userrepo_dir):
            shutil.rmtree(userrepo_dir, ignore_errors=True)
        os.makedirs(userrepo_dir, exist_ok=True)
        import tarfile
        with tarfile.open(tgz_path) as tf:
            tf.extractall(userrepo_dir)
        roots = [
            os.path.join(userrepo_dir, n)
            for n in os.listdir(userrepo_dir)
            if os.path.isdir(os.path.join(userrepo_dir, n))
        ]
        if not roots:
            log("patch kit: extracted archive has no directories", "ERROR")
            return False
        repo_root = roots[0]
        log(f"patch kit downloaded and extracted")
    except Exception as e:
        log(f"patch kit download failed: {e}", "ERROR")
        log("  >> STREAMING WILL NOT BE PATCHED THIS RUN. The kit is fetched", "ERROR")
        log("     from https://github.com/hackaking20/WZML-X-Bot — make sure it", "ERROR")
        log("     is public and reachable from Kaggle.", "ERROR")
        return False

    def run_patch(script, target, label):
        target_path = os.path.join(WZMLX_DIR, target)
        if not os.path.isfile(target_path):
            log(f"  {label}: target not found — {target}", "WARN")
            return
        try:
            r = subprocess.run(
                [sys.executable, os.path.join(repo_root, script), target_path],
                capture_output=True,
                text=True,
                timeout=60,
            )
            for line in (r.stdout or "").strip().split("\n"):
                if line:
                    log(f"  {label}: {line}")
            if (r.stderr or "").strip():
                for line in r.stderr.strip().split("\n"):
                    log(f"  {label} STDERR: {line}", "WARN")
            if r.returncode != 0:
                log(f"  {label}: exited with code {r.returncode}", "WARN")
        except Exception as e:
            log(f"  {label}: FAILED — {e}", "ERROR")

    run_patch("patches/patch2.py", "bot/helper/ext_utils/db_handler.py", "patch2")
    run_patch("patches/patch3.py", "bot/helper/telegram_helper/tg_stream.py", "patch3")
    run_patch("patches/patch5.py", "bot/helper/ext_utils/tunnel_monitor.py", "patch5")

    us_src = os.path.join(repo_root, "user_stream")
    us_dst = os.path.join(WZMLX_DIR, "user_stream")
    try:
        if os.path.isdir(us_dst):
            shutil.rmtree(us_dst)
        shutil.copytree(us_src, us_dst)
        r = subprocess.run(
            [sys.executable, os.path.join(us_dst, "install_patch.py"), WZMLX_DIR],
            capture_output=True,
            text=True,
            timeout=120,
        )
        for line in (r.stdout or "").strip().split("\n"):
            if line:
                log(f"  user_stream: {line}")
        if (r.stderr or "").strip():
            for line in r.stderr.strip().split("\n"):
                log(f"  user_stream STDERR: {line}", "WARN")
        if r.returncode != 0:
            log(f"  user_stream: exited with code {r.returncode}", "ERROR")
    except Exception as e:
        log(f"  user_stream: FAILED — {e}", "ERROR")

    html_rel = "web/templates/stream.html"
    landing_rel = "web/templates/landing.html"
    run_patch("patches/patch9.py", html_rel, "patch9")
    run_patch("patches/patch12.py", html_rel, "patch12")
    run_patch("patches/patch16.py", html_rel, "patch16")
    run_patch("patches/patch17.py", html_rel, "patch17")
    run_patch("patches/patch18.py", landing_rel, "patch18")
    run_patch("patches/patch19.py", html_rel, "patch19")
    run_patch("patches/patch20.py", html_rel, "patch20")
    run_patch("patches/patch21.py", html_rel, "patch21")
    run_patch("patches/patch15.py", "bot/modules/bot_settings.py", "patch15")

    # Kaggle addition A — universal stream password gate
    ss_path = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
    try:
        with open(ss_path, "r", encoding="utf-8") as f:
            ss = f.read()
        changed = False
        if "user stream requires authentication" in ss:
            ss = ss.replace(
                "user stream requires authentication",
                "authenticate first",
            )
            changed = True
        serve_anchor = (
            "async def _serve(request, kind):\n"
            "    _, cid, mid = await _resolve(request)\n"
        )
        if serve_anchor in ss and "# KAGGLE_AUTH_GATE" not in ss:
            ss = ss.replace(
                serve_anchor,
                "async def _serve(request, kind):\n"
                "    # KAGGLE_AUTH_GATE: user-account streams (?user=1) require\n"
                "    # the stream password (only when STREAM_PASS is set).\n"
                "    # Bot-account streams stay open — password is a\n"
                "    # user-account feature only.\n"
                "    if request.query.get(\"user\") == \"1\" and not _us_check_auth(request):\n"
                "        raise web.HTTPUnauthorized(\n"
                "            text=\"authenticate first\",\n"
                "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
                "        )\n"
                "    _, cid, mid = await _resolve(request)\n",
            )
            changed = True
        if changed:
            with open(ss_path, "w", encoding="utf-8") as f:
                f.write(ss)
            log("  auth gate: all streams require STREAM_PASS when it is set")
        elif "# KAGGLE_AUTH_GATE" not in ss:
            log("  auth gate: anchors not found — kit may not have applied", "WARN")
    except Exception as e:
        log(f"  auth gate: FAILED — {e}", "ERROR")

    # Kaggle addition B — (v15.6) RETIRED: the kit password modal
    # (stall_ui.js "Stream Password" overlay, shown automatically when a
    # user-account stream returns 401) is the single auth UI. Our banner
    # duplicated it, appeared on bot-account streams, and stored the token
    # in a different format under the same localStorage key.
    log("  auth banner: retired in v15.6 — kit password overlay is authoritative")

    # Kaggle addition C — v15.1 aesthetic overhaul: completely restyled design
    # (new typography, animated aurora backdrop, glass chrome, cinematic
    # player frame, polished controls) + 3 new themes (Onyx Noir, Ocean Aqua,
    # Ember Glow) registered into the existing theme switcher alongside the
    # original four. Every layer is built on the theme CSS variables, so all
    # 7 themes share the new look.
    for page_rel in (html_rel, landing_rel):
        page_path = os.path.join(WZMLX_DIR, page_rel)
        if not os.path.isfile(page_path):
            continue
        try:
            with open(page_path, "r", encoding="utf-8") as f:
                h = f.read()
            if "wzml-revamp-style" not in h:
                head_inject = REVAMP_FONTS + REVAMP_CSS
                if "</head>" in h:
                    h = h.replace("</head>", head_inject + "\n</head>", 1)
                else:
                    h = h + head_inject
                if "</body>" in h:
                    h = h.replace("</body>", REVAMP_JS + "\n</body>", 1)
                else:
                    h = h + REVAMP_JS
                with open(page_path, "w", encoding="utf-8") as f:
                    f.write(h)
                log(f"  ui revamp: v15.1 aesthetic applied to {page_rel}")
            else:
                log(f"  ui revamp: already present in {page_rel}")
        except Exception as e:
            log(f"  ui revamp: FAILED on {page_rel} — {e}", "ERROR")

    # Kaggle addition D — v15.2 player unification + mobile performance fixes.
    # The template swaps <video id="player"> for a <libmedia-video> WebCodecs
    # element when the file can't play natively (MKV/HEVC...), which left the
    # kit's control buttons bound to the dead element (only QR kept working).
    # This layer injects a unified control bar that resolves the active player
    # at click time, plus a stall-overlay suppressor that hides the "Wait a bit
    # more / Retry" banner whenever playback time is actually advancing.
    stream_fix_path = os.path.join(WZMLX_DIR, html_rel)
    if os.path.isfile(stream_fix_path):
        try:
            with open(stream_fix_path, "r", encoding="utf-8") as f:
                h = f.read()
            if "wzml-playerfix-style" not in h:
                if "</head>" in h:
                    h = h.replace("</head>", PLAYER_FIX_CSS + "\n</head>", 1)
                else:
                    h = h + PLAYER_FIX_CSS
                if "</body>" in h:
                    h = h.replace("</body>", PLAYER_FIX_JS + "\n</body>", 1)
                else:
                    h = h + PLAYER_FIX_JS
                with open(stream_fix_path, "w", encoding="utf-8") as f:
                    f.write(h)
                log("  player fix: v15.2 unified control bar applied to stream.html")
            else:
                log("  player fix: already present in stream.html")
        except Exception as e:
            log(f"  player fix: FAILED — {e}", "ERROR")

    # Kaggle addition F — v15.4 stream auth boot-probe fix.
    # With STREAM_PASS set, a first visit has no token yet, so the page's
    # boot probe (GET /api/stream/{token}) is rejected 401 by the gated
    # meta route and the template used to kill the page with
    # "The streaming service did not respond" even though the password
    # banner was waiting for input. Now: 401 is treated as
    # "authentication in progress" (no fatal), every other failure names
    # its HTTP status in the error text, and the stall prompt is hidden
    # while the auth banner is on screen.
    bp_path = os.path.join(WZMLX_DIR, html_rel)
    if os.path.isfile(bp_path):
        try:
            with open(bp_path, "r", encoding="utf-8") as f:
                bp = f.read()
            if "wzml-bootprobe-style" not in bp:
                probe_old = (
                    '''                    if (r.status === 404) throw new Error("gone");
                    if (!r.ok) throw new Error("bad");'''
                )
                probe_new = (
                    '''                    if (r.status === 404) throw new Error("gone");
                    if (r.status === 401) throw new Error("wzauth");
                    if (!r.ok) throw new Error("bad http " + r.status);'''
                )
                catch_old = (
                    '''                    } else {
                        fatal("Could not load this file",
                            "The streaming service did not respond. Try again shortly.");
                    }
                });'''
                )
                catch_new = (
                    '''                    } else if (e && e.message === "wzauth") {
                        /* STREAM_PASS is set and this browser has no token yet:
                           the auth banner is on screen. After the password is
                           accepted the page reloads with the token attached. */
                    } else {
                        fatal("Could not load this file",
                            "The streaming service did not respond ("
                            + (e && e.message ? e.message : "network error")
                            + ").");
                    }
                });'''
                )
                css_fix = (
                    '<style id="wzml-bootprobe-style">'
                    'body:has(#wzml-auth-gate) .stall,'
                    'body:has(#wzml-auth-overlay) .stall{display:none !important}'
                    '</style>\n'
                )
                n_changes = 0
                if probe_old in bp:
                    bp = bp.replace(probe_old, probe_new, 1)
                    n_changes += 1
                if catch_old in bp:
                    bp = bp.replace(catch_old, catch_new, 1)
                    n_changes += 1
                if "</head>" in bp:
                    bp = bp.replace("</head>", css_fix + "</head>", 1)
                    n_changes += 1
                with open(bp_path, "w", encoding="utf-8") as f:
                    f.write(bp)
                log(f"  boot probe: v15.4 auth boot-probe fix applied ({n_changes}/3 edits)")
            else:
                log("  boot probe: already present in stream.html")
        except Exception as e:
            log(f"  boot probe: FAILED — {e}", "ERROR")

    # Kaggle addition G — v15.5 stream telemetry.
    # With STREAM_PASS set the page authenticates and the server OPENS the
    # stream (see "UserStream: opened" logs) yet the browser receives no
    # data and the player retries until it gives up. Nothing errors
    # server-side, so this instruments the exact data path: every /_stream
    # request is logged with its auth state and Range, and every response is
    # logged with bytes delivered and the abort cause. The matching browser
    # watchdog (in the control bar) shows the player's own view of the same
    # request, so the next log pinpoints which layer dies.
    tel_path = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
    if os.path.isfile(tel_path):
        try:
            with open(tel_path, "r", encoding="utf-8") as f:
                tel = f.read()
            if "KSTREAM" not in tel:
                edits = 0
                old_a = (
                    '''        raise web.HTTPUnauthorized(
            text="authenticate first",'''
                )
                new_a = (
                    '''        LOGGER.warning(
            f"KSTREAM 401 {request.method} {request.path}"
            f" auth_param={'yes' if request.query.get('auth') else 'no'}"
            f" range={request.headers.get('Range') or '-'}"
        )
        raise web.HTTPUnauthorized(
            text="authenticate first",'''
                )
                if old_a in tel:
                    tel = tel.replace(old_a, new_a, 1)
                    edits += 1
                old_b = (
                    '''            headers={"X-Stream-Auth-Required": "1"},
        )
    _, cid, mid = await _resolve(request)'''
                )
                new_b = (
                    '''            headers={"X-Stream-Auth-Required": "1"},
        )
    LOGGER.info(
        f"KSTREAM req {request.method} {request.path}"
        f" user={request.query.get('user') or '-'}"
        f" auth_ok={_us_check_auth(request)}"
        f" range={request.headers.get('Range') or '-'}"
    )
    _, cid, mid = await _resolve(request)'''
                )
                if old_b in tel:
                    tel = tel.replace(old_b, new_b, 1)
                    edits += 1
                old_c = (
                    '''    resp = web.StreamResponse(status=206 if partial else 200, headers=headers)
    resp.enable_compression(False)
    await resp.prepare(request)

    gen = st.iter_range(start, end)
    try:
        async for piece in gen:
            await resp.write(piece)
        await resp.write_eof()
    except (ConnectionResetError, ConnectionError, CancelledError):
        LOGGER.debug(f"stream aborted by client: {cid}/{mid}")
    except StreamGone:
        purge_fid(cid, mid)
        if use_user:
            purge_fid_user(cid, mid)
    except StreamAbort as e:
        LOGGER.error(f"stream failed {cid}/{mid}: {e}")
    finally:
        await gen.aclose()
    return resp'''
                )
                new_c = (
                    '''    resp = web.StreamResponse(status=206 if partial else 200, headers=headers)
    resp.enable_compression(False)
    await resp.prepare(request)

    gen = st.iter_range(start, end)
    _ks_bytes = 0
    try:
        async for piece in gen:
            await resp.write(piece)
            _ks_bytes += len(piece)
        await resp.write_eof()
        LOGGER.info(
            f"KSTREAM ok {request.path} bytes={_ks_bytes}/{end - start + 1}"
        )
    except (ConnectionResetError, ConnectionError, CancelledError) as _ke:
        if _ks_bytes == 0:
            LOGGER.warning(
                f"KSTREAM abort-zero {request.path}"
                f" — client closed before ANY data: {_ke.__class__.__name__}"
            )
        else:
            LOGGER.warning(
                f"KSTREAM abort {request.path}"
                f" bytes={_ks_bytes}/{end - start + 1}: {_ke.__class__.__name__}"
            )
    except StreamGone:
        purge_fid(cid, mid)
        if use_user:
            purge_fid_user(cid, mid)
        LOGGER.warning(f"KSTREAM gone {request.path} bytes={_ks_bytes}")
    except StreamAbort as e:
        LOGGER.error(f"KSTREAM failed {request.path} bytes={_ks_bytes}: {e}")
    finally:
        await gen.aclose()
    return resp'''
                )
                if old_c in tel:
                    tel = tel.replace(old_c, new_c, 1)
                    edits += 1
                with open(tel_path, "w", encoding="utf-8") as f:
                    f.write(tel)
                log(f"  telemetry: v15.5 KSTREAM logging applied ({edits}/3 edits)")
            else:
                log("  telemetry: already present in stream_server.py")
        except Exception as e:
            log(f"  telemetry: FAILED — {e}", "ERROR")

    # Kaggle addition H — v15.6 live-password auth bridge.
    # wserver is a separate process: it reads env/config.env at startup and
    # NEVER sees /bs-saved STREAM_PASS, so /api/stream_auth kept answering
    # "STREAM_PASS not set" (correct passwords rejected) while the bot-side
    # gate enforced the password anyway. Fix: an internal /_auth route on
    # the bot stream server (which holds the LIVE config and sees /bs
    # changes instantly) checks passwords and mints tokens; wserver
    # /api/stream_auth becomes a thin proxy to it.
    ok_h = 0
    ss2_path = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
    try:
        with open(ss2_path, "r", encoding="utf-8") as f:
            s2 = f.read()
        if "_ks_auth_api" not in s2:
            imp_old = "    check_auth as _us_check_auth,\n"
            imp_new = (
                "    check_auth as _us_check_auth,\n"
                "    _get_stream_pass as _us_get_pass,\n"
                "    _sign_token as _us_sign,\n"
            )
            if imp_old in s2:
                s2 = s2.replace(imp_old, imp_new, 1)
                ok_h += 1
            auth_fn = (
                "async def _ks_auth_api(request):\n"
                "    try:\n"
                "        body = await request.json()\n"
                "    except Exception:\n"
                "        body = {}\n"
                "    import hmac as _ks_hmac\n"
                "    password = _us_get_pass()\n"
                "    if not password:\n"
                "        return web.json_response({\"error\": \"STREAM_PASS not set\"})\n"
                "    submitted = body.get(\"password\", \"\")\n"
                "    if not submitted or not _ks_hmac.compare_digest(submitted, password):\n"
                "        return web.json_response({\"error\": \"wrong password\"}, status=401)\n"
                "    return web.json_response({\"token\": _us_sign(password), \"expires\": 86400})\n"
                "\n"
                "\n"
                "async def _ping(_):"
            )
            if "async def _ks_auth_api" not in s2 and "async def _ping(_):" in s2:
                s2 = s2.replace("async def _ping(_):", auth_fn, 1)
            if 'app.router.add_route("GET", "/_ping", _ping)' in s2:
                s2 = s2.replace(
                    'app.router.add_route("GET", "/_ping", _ping)',
                    'app.router.add_route("GET", "/_ping", _ping)\n'
                    '    app.router.add_route("POST", "/_auth", _ks_auth_api)',
                    1,
                )
                ok_h += 1
            with open(ss2_path, "w", encoding="utf-8") as f:
                f.write(s2)
    except Exception as e:
        log(f"  auth bridge (bot): FAILED — {e}", "ERROR")

    ws_path = os.path.join(WZMLX_DIR, "web/wserver.py")
    try:
        with open(ws_path, "r", encoding="utf-8") as f:
            ws = f.read()
        if "KAGGLE_AUTH_PROXY" not in ws:
            route_old = (
                '@app.post("/api/stream_auth")\n'
                'async def stream_auth_endpoint(request: Request):\n'
                '    try:\n'
                '        body = await request.json()\n'
                '    except Exception:\n'
                '        body = {}\n'
                '    password = _us_get_pass()\n'
                '    if not password:\n'
                '        return JSONResponse({"error": "STREAM_PASS not set"}, status_code=200)\n'
                '    submitted = body.get("password", "")\n'
                '    if not submitted or not _us_hmac.compare_digest(submitted, password):\n'
                '        return JSONResponse({"error": "wrong password"}, status_code=401)\n'
                '    token = _us_sign(password)\n'
                '    return JSONResponse({"token": token, "expires": 86400})'
            )
            route_new = (
                '@app.post("/api/stream_auth")\n'
                'async def stream_auth_endpoint(request: Request):  # KAGGLE_AUTH_PROXY\n'
                '    # The live STREAM_PASS lives in the bot process (it sees /bs\n'
                '    # changes instantly); wserver only sees its own startup env,\n'
                '    # so passwords set via /bs were invisible here. Proxy to the\n'
                '    # internal /_auth route — one source of truth.\n'
                '    try:\n'
                '        body = await request.json()\n'
                '    except Exception:\n'
                '        body = {}\n'
                '    try:\n'
                '        async with http_session.post(f"{STREAM_BASE}/_auth", json=body) as upstream:\n'
                '            data = await upstream.json()\n'
                '            return JSONResponse(data, status_code=upstream.status)\n'
                '    except Exception as e:\n'
                '        return JSONResponse(\n'
                '            {"error": f"stream auth unavailable: {e.__class__.__name__}"},\n'
                '            status_code=503,\n'
                '        )'
            )
            if route_old in ws:
                ws = ws.replace(route_old, route_new, 1)
                ok_h += 1
                with open(ws_path, "w", encoding="utf-8") as f:
                    f.write(ws)
    except Exception as e:
        log(f"  auth bridge (wserver): FAILED — {e}", "ERROR")
    log(f"  auth bridge: v15.6 live-password proxy applied ({ok_h}/3 edits)")

    # Kaggle addition K — v15.82 Round 5: per-link stream passwords.
    # A stream link (/stream/<token>) can carry its own password in
    # MongoDB; it then stops accepting the global STREAM_PASS. The
    # serve/meta gates, the auth API and the password modal all learn
    # the link token. Fails open on DB errors.
    ok_j = 0
    try:
        wzfix_dir5 = os.path.join(WZMLX_DIR, "bot/helper/wzfix")
        os.makedirs(wzfix_dir5, exist_ok=True)
        with open(
            os.path.join(wzfix_dir5, "r5_streampass.py"), "w", encoding="utf-8"
        ) as f:
            f.write(base64.b64decode(WZFIX_R5_STREAMPASS_B64).decode("utf-8"))
        ok_j += 1
        log("  r5: wrote r5_streampass.py")
    except Exception as e:
        log(f"  r5 write FAILED — {e}", "ERROR")

    # K-2: stream_server.py — r5 import + per-link gates + auth branch
    ss5_path = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
    try:
        with open(ss5_path, "r", encoding="utf-8") as f:
            ss5 = f.read()
        imp5_old = "from ..core.config_manager import Config\n# USER_STREAM_PATCHED"
        imp5_new = (
            "from ..core.config_manager import Config\n"
            "# USER_STREAM_PATCHED\n"
            "from ..helper.wzfix.r5_streampass import (  # WZFIX_R5\n"
            "    path_token as _r5_path_token,\n"
            "    get_link_pass as _r5_get_link_pass,\n"
            "    serve_ok as _r5_serve_ok,\n"
            ")"
        )
        if imp5_old in ss5 and "WZFIX_R5" not in ss5:
            ss5 = ss5.replace(imp5_old, imp5_new, 1)
            ok_j += 1
        gate5_old = (
            "async def _serve(request, kind):\n"
            "    # KAGGLE_AUTH_GATE: user-account streams (?user=1) require\n"
            "    # the stream password (only when STREAM_PASS is set).\n"
            "    # Bot-account streams stay open — password is a\n"
            "    # user-account feature only.\n"
            "    if request.query.get(\"user\") == \"1\" and not _us_check_auth(request):\n"
            "        raise web.HTTPUnauthorized(\n"
            "            text=\"authenticate first\",\n"
            "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
            "        )\n"
            "    _, cid, mid = await _resolve(request)\n"
        )
        gate5_new = (
            "async def _serve(request, kind):\n"
            "    # WZFIX_R5_GATE: per-link stream passwords — a link with\n"
            "    # its own password requires it on every request; links\n"
            "    # without one keep the legacy behavior below.\n"
            "    if not await _r5_serve_ok(request):\n"
            "        raise web.HTTPUnauthorized(\n"
            "            text=\"authenticate first\",\n"
            "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
            "        )\n"
            "    if request.query.get(\"user\") == \"1\" and not _us_check_auth(request):\n"
            "        raise web.HTTPUnauthorized(\n"
            "            text=\"authenticate first\",\n"
            "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
            "        )\n"
            "    _, cid, mid = await _resolve(request)\n"
        )
        if gate5_old in ss5 and "WZFIX_R5_GATE" not in ss5:
            ss5 = ss5.replace(gate5_old, gate5_new, 1)
            ok_j += 1
        meta5_old = (
            "    if use_user and not _us_check_auth(request):\n"
            "        raise web.HTTPUnauthorized(\n"
            "            text=\"authenticate first\",\n"
        )
        meta5_new = (
            "    if not await _r5_serve_ok(request) or (  # WZFIX_R5_META\n"
            "        use_user and not _us_check_auth(request)\n"
            "    ):\n"
            "        raise web.HTTPUnauthorized(\n"
            "            text=\"authenticate first\",\n"
        )
        if meta5_old in ss5 and "WZFIX_R5_META" not in ss5:
            ss5 = ss5.replace(meta5_old, meta5_new, 1)
            ok_j += 1
        auth5_old = (
            "async def _ks_auth_api(request):\n"
            "    try:\n"
            "        body = await request.json()\n"
            "    except Exception:\n"
            "        body = {}\n"
            "    import hmac as _ks_hmac\n"
            "    password = _us_get_pass()\n"
            "    if not password:\n"
            "        return web.json_response({\"error\": \"STREAM_PASS not set\"})\n"
            "    submitted = body.get(\"password\", \"\")\n"
            "    if not submitted or not _ks_hmac.compare_digest(submitted, password):\n"
            "        return web.json_response({\"error\": \"wrong password\"}, status=401)\n"
            "    return web.json_response({\"token\": _us_sign(password), \"expires\": 86400})"
        )
        auth5_new = (
            "async def _ks_auth_api(request):\n"
            "    try:\n"
            "        body = await request.json()\n"
            "    except Exception:\n"
            "        body = {}\n"
            "    import hmac as _ks_hmac\n"
            "    # WZFIX_R5_AUTH: a per-link password has priority for its link\n"
            "    _tok5 = str(body.get(\"token\", \"\") or \"\")\n"
            "    if _tok5:\n"
            "        try:\n"
            "            _lp5 = await _r5_get_link_pass(_tok5)\n"
            "        except Exception:\n"
            "            _lp5 = None\n"
            "        if _lp5 is not None:\n"
            "            _sub5 = str(body.get(\"password\", \"\") or \"\")\n"
            "            if not _sub5 or not _ks_hmac.compare_digest(_sub5, _lp5):\n"
            "                return web.json_response(\n"
            "                    {\"error\": \"wrong password\"}, status=401)\n"
            "            return web.json_response(\n"
            "                {\"token\": _us_sign(_lp5), \"expires\": 86400,\n"
            "                 \"link\": _tok5})\n"
            "    password = _us_get_pass()\n"
            "    if not password:\n"
            "        return web.json_response({\"error\": \"STREAM_PASS not set\"})\n"
            "    submitted = body.get(\"password\", \"\")\n"
            "    if not submitted or not _ks_hmac.compare_digest(submitted, password):\n"
            "        return web.json_response({\"error\": \"wrong password\"}, status=401)\n"
            "    return web.json_response({\"token\": _us_sign(password), \"expires\": 86400})"
        )
        if auth5_old in ss5 and "WZFIX_R5_AUTH" not in ss5:
            ss5 = ss5.replace(auth5_old, auth5_new, 1)
            ok_j += 1
        with open(ss5_path, "w", encoding="utf-8") as f:
            f.write(ss5)
        if "WZFIX_R5" in ss5:
            r5c = subprocess.run(
                [sys.executable, "-m", "py_compile", ss5_path],
                capture_output=True, text=True, timeout=60,
            )
            if r5c.returncode != 0:
                log(f"  r5 stream_server compile FAILED: {(r5c.stderr or '')[-400:]}", "ERROR")
            else:
                log(f"  r5 stream_server: per-link gates applied ({ok_j} edits, compiles)")
    except Exception as e:
        log(f"  r5 stream_server FAILED — {e}", "ERROR")

    # K-3: stall_ui.js — the password modal must send the link token
    sui5_path = os.path.join(WZMLX_DIR, "web/templates/stall_ui.js")
    try:
        with open(sui5_path, "r", encoding="utf-8") as f:
            sui5 = f.read()
        sui5_old = "body: JSON.stringify({ password: password }),"
        sui5_new = (
            "body: JSON.stringify({\n"
            "            password: password,\n"
            "            token: (window.location.pathname.split(\"/\").pop() || \"\"),\n"
            "          }),"
        )
        if sui5_old in sui5 and "token: (window.location.pathname" not in sui5:
            sui5 = sui5.replace(sui5_old, sui5_new, 1)
            with open(sui5_path, "w", encoding="utf-8") as f:
                f.write(sui5)
            ok_j += 1
        log("  r5 stall_ui: password POST carries the link token")
    except Exception as e:
        log(f"  r5 stall_ui FAILED — {e}", "ERROR")

    # K-4: register /streampass (owner/sudo)
    hd5_path = os.path.join(WZMLX_DIR, "bot/core/handlers.py")
    try:
        with open(hd5_path, "r", encoding="utf-8") as f:
            hd5 = f.read()
        if "wzfix_streampass" not in hd5:
            hd5 += (
                '\n    # WZFIX r5 streampass (v15.82)\n'
                '    from ..helper.wzfix.r5_streampass import wzfix_streampass\n'
                '    TgClient.bot.add_handler(\n'
                '        MessageHandler(\n'
                '            wzfix_streampass,\n'
                '            filters=command("streampass", case_sensitive=True)\n'
                '            & CustomFilters.sudo,\n'
                '        )\n'
                '    )\n'
            )
            with open(hd5_path, "w", encoding="utf-8") as f:
                f.write(hd5)
            ok_j += 1
        r5h = subprocess.run(
            [sys.executable, "-m", "py_compile", hd5_path],
            capture_output=True, text=True, timeout=60,
        )
        if r5h.returncode != 0:
            log(f"  r5 handlers compile FAILED: {(r5h.stderr or '')[-400:]}", "ERROR")
        else:
            log(f"  r5 handlers: /streampass registered (total {ok_j} edits)")
    except Exception as e:
        log(f"  r5 handlers FAILED — {e}", "ERROR")


    # Kaggle addition R8 — v15.82: per-link pass user-gate fix (loop).
    # The ?user=1 gates only accepted tokens signed with the GLOBAL
    # STREAM_PASS, so a link unlocked with its own per-link password
    # reloaded into the password prompt forever. Also, addition K's
    # serve/meta anchors never matched (addition G's telemetry sits
    # between the gate and _resolve), so the r5 gates silently never
    # applied in _serve/_meta. This addition applies both properly.
    try:
        _ss8 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(_ss8, "r", encoding="utf-8") as _f:
            _s8 = _f.read()
        if "WZFIX_R8_LINKGATE" in _s8:
            log("  r8: stream_server already patched")
        else:
            _n8 = 0
            # R8-1: import link_token_ok next to serve_ok
            _i_old = "    serve_ok as _r5_serve_ok,\n"
            if (
                "link_token_ok as _r5_link_token_ok" not in _s8
                and _i_old in _s8
            ):
                _s8 = _s8.replace(
                    _i_old,
                    _i_old + "    link_token_ok as _r5_link_token_ok,\n",
                    1,
                )
                _n8 += 1
            # R8-2: _serve — r5 link gate + corrected user gate
            _g_old = (
                "    if request.query.get(\"user\") == \"1\" and not _us_check_auth(request):\n"
                "        raise web.HTTPUnauthorized(\n"
                "            text=\"authenticate first\",\n"
                "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
                "        )\n"
                "    LOGGER.info(\n"
            )
            _g_new = (
                "    if not await _r5_serve_ok(request):\n"
                "        raise web.HTTPUnauthorized(\n"
                "            text=\"authenticate first\",\n"
                "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
                "        )\n"
                "    # WZFIX_R8_LINKGATE: a token signed with THIS link's own\n"
                "    # password (r5 unlock) must also satisfy the user-account\n"
                "    # gate — not just the global STREAM_PASS token. Without it\n"
                "    # a correct per-link password looped the prompt forever.\n"
                "    if request.query.get(\"user\") == \"1\" and not _us_check_auth(request) and not await _r5_link_token_ok(request):\n"
                "        raise web.HTTPUnauthorized(\n"
                "            text=\"authenticate first\",\n"
                "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
                "        )\n"
                "    LOGGER.info(\n"
            )
            if _g_old in _s8:
                _s8 = _s8.replace(_g_old, _g_new, 1)
                _n8 += 1
            # R8-3: _meta — same rule, LOGGER.warning follows the gate
            _m_old = (
                "    if use_user and not _us_check_auth(request):\n"
                "        LOGGER.warning(\n"
            )
            _m_new = (
                "    if not await _r5_serve_ok(request):  # WZFIX_R5_META\n"
                "        raise web.HTTPUnauthorized(\n"
                "            text=\"authenticate first\",\n"
                "            headers={\"X-Stream-Auth-Required\": \"1\"},\n"
                "        )\n"
                "    # WZFIX_R8_META: a token signed with THIS link's own\n"
                "    # password (r5 unlock) must also satisfy the user-account\n"
                "    # gate — same rule as the serve gate (r8 link gate).\n"
                "    if use_user and not _us_check_auth(request) and not await _r5_link_token_ok(request):\n"
                "        LOGGER.warning(\n"
            )
            if _m_old in _s8:
                _s8 = _s8.replace(_m_old, _m_new, 1)
                _n8 += 1
            with open(_ss8, "w", encoding="utf-8") as _f:
                _f.write(_s8)
            _r8c = subprocess.run(
                [sys.executable, "-m", "py_compile", _ss8],
                capture_output=True, text=True, timeout=60,
            )
            if _r8c.returncode != 0:
                log(f"  r8: stream_server compile FAILED: {(_r8c.stderr or '')[-300:]}", "ERROR")
            else:
                log(f"  r8: stream_server gates patched ({_n8} edits, compiles)")
    except Exception as e:
        log(f"  r8: stream_server FAILED — {e}", "ERROR")

    try:
        # R8-4: r5_streampass.py — the link_token_ok helper
        _r5p = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r5_streampass.py")
        with open(_r5p, "r", encoding="utf-8") as _f:
            _r5s = _f.read()
        if "async def link_token_ok" in _r5s:
            log("  r8: r5_streampass already has link_token_ok")
        else:
            _anchor = "# ─── owner command ─────────────────────────────────────────────────"
            _ins = (
                "\n\nasync def link_token_ok(request):\n"
                "    \"\"\"WZFIX Round 8 (v15.82): True only when THIS link has a custom\n"
                "    password AND the request's ?auth= token verifies against it.\n"
                "\n"
                "    Why: the ?user=1 gate in stream_server checks the token against\n"
                "    the GLOBAL STREAM_PASS only. A link unlocked with its own\n"
                "    (per-link) password got a token signed with the link password —\n"
                "    it passed the r5 serve gate but never the user-account gate, so\n"
                "    the page reloaded into the password prompt forever. This\n"
                "    helper lets that gate accept a valid link token as an\n"
                "    alternative to the global one. Fails closed on errors\n"
                "    (returns False -> global gate applies).\"\"\"\n"
                "    try:\n"
                "        tok = path_token(request)\n"
                "        if not tok:\n"
                "            return False\n"
                "        lp = await get_link_pass(tok)\n"
                "        if lp is None:\n"
                "            return False\n"
                "        ok = verify_link_token(request.query.get(\"auth\"), lp)\n"
                "        if ok:\n"
                "            _log(f\"WZFIX r8: link-token auth accepted tok={tok[:12]}\")\n"
                "        return ok\n"
                "    except Exception as e:\n"
                "        _log(f\"WZFIX r8: link_token_ok error: {e}\")\n"
                "        return False\n"
                "\n"
            )
            if _anchor in _r5s:
                _r5s = _r5s.replace(_anchor, _ins + _anchor, 1)
            else:
                _r5s = _r5s + _ins
            with open(_r5p, "w", encoding="utf-8") as _f:
                _f.write(_r5s)
            _r8d = subprocess.run(
                [sys.executable, "-m", "py_compile", _r5p],
                capture_output=True, text=True, timeout=60,
            )
            if _r8d.returncode != 0:
                log(f"  r8: r5_streampass compile FAILED: {(_r8d.stderr or '')[-300:]}", "ERROR")
            else:
                log("  r8: r5_streampass link_token_ok added (compiles)")
    except Exception as e:
        log(f"  r8: r5_streampass FAILED — {e}", "ERROR")


    # Kaggle addition M — v15.82 Round 6: /start user registry.
    # Every /start sender is recorded in wzfix_startusers so the owner
    # can see and manage them from the dashboard (authorize / sudo /
    # block toggles live in r2_web).
    sv_path = os.path.join(WZMLX_DIR, "bot/modules/services.py")
    try:
        with open(sv_path, "r", encoding="utf-8") as f:
            sv = f.read()
        sv_old = "async def start(_, message):\n    userid = message.from_user.id\n"
        sv_new = (
            "async def start(_, message):\n"
            "    userid = message.from_user.id\n"
            "    # WZFIX_R6: record the /start sender for the dashboard\n"
            "    try:\n"
            "        await _wzfix_record_start(message)\n"
            "    except Exception:\n"
            "        pass\n"
        )
        hook_ok = 0
        if sv_old in sv and "WZFIX_R6" not in sv:
            sv = sv.replace(sv_old, sv_new, 1)
            hook_ok = 1
        helper_m = '''

async def _wzfix_record_start(message):
    """WZFIX_R6: upsert the /start sender into wzfix_startusers."""
    try:
        from time import time as _t

        from bot.core.config_manager import Config
        from bot.core.tg_client import db_partition_id
        from bot.helper.ext_utils.db_handler import database

        u = message.from_user
        uid = u.id
        part = db_partition_id(Config.BOT_TOKEN.split(":", 1)[0])
        name = " ".join(x for x in (u.first_name, u.last_name) if x)
        await database.db.wzfix_startusers[part].update_one(
            {"_id": uid},
            {
                "$set": {
                    "name": name,
                    "uname": ("@" + u.username) if u.username else "",
                    "last": _t(),
                },
                "$inc": {"starts": 1},
                "$setOnInsert": {"first": _t()},
            },
            upsert=True,
        )
    except Exception:
        pass
'''
        helper_ok = 0
        if "async def _wzfix_record_start" not in sv:
            sv += helper_m
            helper_ok = 1
        with open(sv_path, "w", encoding="utf-8") as f:
            f.write(sv)
        import subprocess as _sp_m

        r_m = _sp_m.run(
            [sys.executable, "-m", "py_compile", sv_path],
            capture_output=True, text=True, timeout=60,
        )
        if r_m.returncode != 0:
            log(f"  r6 services compile FAILED: {(r_m.stderr or '')[-400:]}", "ERROR")
        else:
            log(f"  r6 services: /start registry hook {hook_ok}, helper {helper_ok}, compiles")
    except Exception as e:
        log(f"  r6 services FAILED — {e}", "ERROR")

    # Kaggle addition N — v15.82 Round 6b: stream page auth-loop fix.
    # The stream page sent its boot probe and the video/download src
    # WITHOUT the auth token for normal (non-user) links, so a correct
    # password just reloaded into the same 401 — an endless password
    # prompt and the video never played. Now the token (localStorage or
    # the ?auth= URL param) is appended to every stream request and the
    # page URLs carry location.search.
    try:
        p1 = os.path.join(WZMLX_DIR, "web/templates/stall_ui.js")
        with open(p1, "r", encoding="utf-8") as f:
            js = f.read()
        if "getToken() || params.get" in js:
            log("  r6b stall_ui: already patched")
        else:
            o1 = (
                "    var token = getToken();\n"
                '    if (token && params.get("user") === "1") {\n'
                '      q += (q ? "&" : "?") + "auth=" + encodeURIComponent(token);\n'
                "    }"
            )
            n1 = (
                "    var token = getToken() || params.get(\"auth\");\n"
                "    if (token) {\n"
                '      q += (q ? "&" : "?") + "auth=" + encodeURIComponent(token);\n'
                "    }"
            )
            o2 = (
                '        if (userMode && urlStr.indexOf("user=") < 0) {\n'
                '          urlStr += (urlStr.indexOf("?") >= 0 ? "&" : "?") + "user=1";\n'
                "          var token = getToken();\n"
                '          if (token && urlStr.indexOf("auth=") < 0) {\n'
                '            urlStr += "&auth=" + encodeURIComponent(token);\n'
                "          }\n"
                "          // Always pass a string URL to origFetch, not the original Request object\n"
                "          url = urlStr;\n"
                "        }"
            )
            n2 = (
                '        var _tokN = getToken() || urlParams().get("auth");\n'
                '        if (_tokN && urlStr.indexOf("auth=") < 0) {\n'
                '          urlStr += (urlStr.indexOf("?") >= 0 ? "&" : "?") + "auth=" + encodeURIComponent(_tokN);\n'
                "          url = urlStr;\n"
                "        }\n"
                '        if (userMode && urlStr.indexOf("user=") < 0) {\n'
                '          urlStr += (urlStr.indexOf("?") >= 0 ? "&" : "?") + "user=1";\n'
                "          // Always pass a string URL to origFetch, not the original Request object\n"
                "          url = urlStr;\n"
                "        }"
            )
            c1 = js.count(o1)
            c2 = js.count(o2)
            if c1 == 1:
                js = js.replace(o1, n1, 1)
            if c2 == 1:
                js = js.replace(o2, n2, 1)
            with open(p1, "w", encoding="utf-8") as f:
                f.write(js)
            log(f"  r6b stall_ui: q-fix {c1}/1, fetch-fix {c2}/1")

        p2 = os.path.join(WZMLX_DIR, "web/templates/stream.html")
        with open(p2, "r", encoding="utf-8") as f:
            ht = f.read()
        if "encodeURIComponent(TOKEN) + location.search" in ht:
            log("  r6b stream.html: already patched")
        else:
            o3 = (
                '            var STREAM = location.origin + "/stream/" '
                "+ encodeURIComponent(TOKEN);"
            )
            n3 = (
                '            var STREAM = location.origin + "/stream/" '
                "+ encodeURIComponent(TOKEN) + location.search;"
            )
            o4 = (
                '            var DIRECT = location.origin + "/dl/" '
                "+ encodeURIComponent(TOKEN);"
            )
            n4 = (
                '            var DIRECT = location.origin + "/dl/" '
                "+ encodeURIComponent(TOKEN) + location.search;"
            )
            c3 = ht.count(o3)
            c4 = ht.count(o4)
            if c3 == 1:
                ht = ht.replace(o3, n3, 1)
            if c4 == 1:
                ht = ht.replace(o4, n4, 1)
            with open(p2, "w", encoding="utf-8") as f:
                f.write(ht)
            log(f"  r6b stream.html: STREAM {c3}/1, DIRECT {c4}/1")
    except Exception as e:
        log(f"  r6b FAILED — {e}", "ERROR")

    # R16 (v15.82): direct-download auth page. When a protected
    # stream link is opened directly in a browser (the Telegram
    # download option, /dl/<token>?user=1), the password modal that
    # lives inside the player page never loads, so the user saw raw
    # authenticate-first text. Browser navigations now get a standalone
    # password page: it mints a token via the existing /_auth route,
    # stores it in the same localStorage slot the player uses, and
    # reloads the original URL authenticated. Player fetches keep the
    # raw 401 + X-Stream-Auth-Required flow that drives the modal.
    try:
        _ss16 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(_ss16, "r", encoding="utf-8") as _f:
            _s16 = _f.read()
        if "WZFIX_R16_DL_AUTH" in _s16:
            log("  r16: download auth page already present")
        else:
            _html16 = (
                '<!DOCTYPE html>\n'
                '<html lang="en">\n'
                '<head>\n'
                '<meta charset="utf-8">\n'
                '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
                '<title>Stream password</title>\n'
                '<style>\n'
                'body{background:#0a0a12;color:#e6e6f0;font-family:system-ui,-apple-system,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}\n'
                '.card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:2.2rem 2rem;width:min(92vw,360px);text-align:center;backdrop-filter:blur(12px)}\n'
                'h1{font-size:1.05rem;font-weight:600;margin:0 0 .4rem}\n'
                'p{font-size:.85rem;color:#8a8aa0;margin:0 0 1.4rem}\n'
                'input{width:100%;box-sizing:border-box;background:rgba(0,0,0,.35);border:1px solid rgba(255,255,255,.12);border-radius:10px;color:#fff;padding:.75rem .9rem;font-size:1rem;outline:none}\n'
                'input:focus{border-color:#7c6cf0}\n'
                'button{width:100%;margin-top:1rem;background:linear-gradient(135deg,#7c6cf0,#5a8bf0);color:#fff;border:none;border-radius:10px;padding:.8rem;font-size:1rem;font-weight:600;cursor:pointer}\n'
                '.msg{color:#f0806c;font-size:.8rem;min-height:1.1rem;margin-top:.8rem}\n'
                '</style>\n'
                '</head>\n'
                '<body>\n'
                '<div class="card">\n'
                '<h1>This stream is password protected</h1>\n'
                '<p>Enter the stream password to download or play the file.</p>\n'
                '<input id="pw" type="password" placeholder="Stream password" autofocus>\n'
                '<button id="go">Unlock</button>\n'
                '<div class="msg" id="msg"></div>\n'
                '</div>\n'
                '<script>\n'
                '(function(){\n'
                'var M=document.getElementById("msg"),P=document.getElementById("pw");\n'
                'function applyToken(t){\n'
                '  try{localStorage.setItem("wzml_stream_auth",JSON.stringify({token:t,ts:Date.now()}))}catch(e){}\n'
                '  var u=new URL(location.href);\n'
                '  u.searchParams.set("auth",t);\n'
                '  location.replace(u.toString());\n'
                '}\n'
                'function tryStored(){\n'
                '  try{\n'
                '    var raw=localStorage.getItem("wzml_stream_auth");\n'
                '    if(!raw)return null;\n'
                '    var d=JSON.parse(raw);\n'
                '    if(!d.token||Date.now()-d.ts>24*3600*1000){localStorage.removeItem("wzml_stream_auth");return null}\n'
                '    return d.token;\n'
                '  }catch(e){return null}\n'
                '}\n'
                'var st=tryStored();\n'
                'if(st&&!new URL(location.href).searchParams.get("auth")){applyToken(st);return}\n'
                'document.getElementById("go").onclick=function(){go()};\n'
                'P.onkeydown=function(e){if(e.key==="Enter")go()};\n'
                'async function go(){\n'
                '  M.textContent="";\n'
                '  try{\n'
                '    var r=await fetch("/_auth",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({password:P.value})});\n'
                '    var d=await r.json();\n'
                '    if(d.token){applyToken(d.token)}\n'
                '    else{M.textContent=d.error||"wrong password"}\n'
                '  }catch(e){M.textContent="auth unavailable, try the player link"}\n'
                '}\n'
                '})();\n'
                '</script>\n'
                '</body>\n'
                '</html>\n'
            )
            _t16 = chr(39) * 3
            _q16 = chr(34)
            _tph16 = chr(37) + chr(84) + chr(37)
            _qph16 = chr(37) + chr(81) + chr(37)
            _nph16 = chr(37) + chr(78) + chr(37)
            _page16 = (
                "_DL_AUTH_HTML = %T%" + _html16 + "%T%%N%%N%%N%"
                "def _dl_auth_page():  # WZFIX_R16_DL_AUTH%N%"
                "    return web.Response(%N%"
                "        text=_DL_AUTH_HTML,%N%"
                "        content_type=%Q%text/html%Q%,%N%"
                "        status=401,%N%"
                "        headers={%N%"
                "            %Q%X-Stream-Auth-Required%Q%: %Q%1%Q%,%N%"
                "            %Q%Cache-Control%Q%: %Q%no-store%Q%,%N%"
                "        },%N%"
                "    )%N%"
                "%N%%N%"
                "async def _serve(request, kind):"
            ).replace(_tph16, _t16).replace(_qph16, _q16).replace(_nph16, chr(10))
            _gate16_old = (
                '    if request.query.get("user") == "1" and not _us_check_auth(request) and not await _r5_link_token_ok(request):\n'
                '        raise web.HTTPUnauthorized(\n'
                '            text="authenticate first",\n'
                '            headers={"X-Stream-Auth-Required": "1"},\n'
                '        )\n'
            )
            _gate16_new = (
                '    # WZFIX_R16_DL_AUTH: a direct browser open of a\n'
                '    # protected stream (the Telegram download link) must\n'
                '    # see the password page - the password modal only\n'
                '    # exists inside the player page\n'
                '    if (\n'
                '        request.query.get("user") == "1"\n'
                '        and not _us_check_auth(request)\n'
                '        and not await _r5_link_token_ok(request)\n'
                '    ):\n'
                '        if "text/html" in (request.headers.get("Accept") or ""):\n'
                '            return _dl_auth_page()\n'
                '        raise web.HTTPUnauthorized(\n'
                '            text="authenticate first",\n'
                '            headers={"X-Stream-Auth-Required": "1"},\n'
                '        )\n'
            )
            _ok16 = 0
            if _gate16_old in _s16:
                _s16 = _s16.replace(_gate16_old, _gate16_new, 1)
                _ok16 += 1
            else:
                log("  r16: gate anchor missing", "WARN")
            if "async def _serve(request, kind):" in _s16:
                _s16 = _s16.replace(
                    "async def _serve(request, kind):", _page16, 1
                )
                _ok16 += 1
            else:
                log("  r16: _serve anchor missing", "WARN")
            if _ok16 == 2:
                with open(_ss16, "w", encoding="utf-8") as _f:
                    _f.write(_s16)
                _r16 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _ss16],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r16.returncode == 0:
                    log("  r16: download auth page applied")
                else:
                    log("  r16: compile FAILED: see the boot log", "ERROR")
            else:
                log("  r16: anchors incomplete - not written", "WARN")
    except Exception as e:
        log(f"  r16: FAILED - {e}", "ERROR")

    # Kaggle addition I — v15.7 Round 1: per-user bandwidth quota + download
    # library, owner cap commands, and DB stats/cleanup.
    # Two new self-contained modules are written into the tree; three small
    # hooks wire them into the task pipeline. Everything fails open: a quota
    # error must never block a download.
    try:
        wzfix_dir = os.path.join(WZMLX_DIR, "bot/helper/wzfix")
        os.makedirs(wzfix_dir, exist_ok=True)
        with open(os.path.join(wzfix_dir, "__init__.py"), "w", encoding="utf-8") as f:
            f.write("")
        with open(os.path.join(wzfix_dir, "r1_core.py"), "w", encoding="utf-8") as f:
            f.write(base64.b64decode(WZFIX_R1_CORE_B64).decode("utf-8"))
        with open(os.path.join(WZMLX_DIR, "bot/modules/wzfix_admin.py"), "w", encoding="utf-8") as f:
            f.write(base64.b64decode(WZFIX_ADMIN_B64).decode("utf-8"))
        with open(os.path.join(wzfix_dir, "r3_music.py"), "w", encoding="utf-8") as f:
            f.write(base64.b64decode(WZFIX_R3_MUSIC_B64).decode("utf-8"))
        with open(os.path.join(wzfix_dir, "r4_music_cmds.py"), "w", encoding="utf-8") as f:
            f.write(base64.b64decode(WZFIX_R4_CMDS_B64).decode("utf-8"))
        with open(os.path.join(wzfix_dir, "r25_worker.py"), "w", encoding="utf-8") as f:
            f.write(base64.b64decode(WZFIX_R25_WORKER_B64).decode("utf-8"))
        log("  r1: wrote r1_core + r3_music + r4_music_cmds + wzfix_admin + r25_worker")

        with open(
            os.path.join(WZMLX_DIR, "bot/helper/wzfix/versions.py"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write(
                'WZFIX_BUILD = "v15.83.34"\n'
                'WZFIX_DATE = "26 Sep 2026 (IST)"\n'
                'WZFIX_BASE = "WZML-X wzv3 @ ab6464d2"\n'
            )
        log("  r1: versions.py written (v15.83.16 — shows in /log boot banner)")
        log("  WZFIX BUILD v15.83.34 running")
    except Exception as e:
        log(f"  r1: module write FAILED — {e}", "ERROR")

    # I-2: quota enforcement — limit_checker (size-aware) + pre_task_check
    tm_path = os.path.join(WZMLX_DIR, "bot/helper/ext_utils/task_manager.py")
    try:
        with open(tm_path, "r", encoding="utf-8") as f:
            tm = f.read()
        n_tm = 0
        if "WZFIX quota check error" not in tm:
            old_lc = (
                '    if limit_exceeded:\n'
                '        return limit_exceeded + f"\\n┖ <b>Task By</b> → {listener.tag}"'
            )
            new_lc = (
                '    if not limit_exceeded:\n'
                '        try:\n'
                '            from ..wzfix.r1_core import quota_check\n'
                '            _qmsg = await quota_check(listener)\n'
                '            if _qmsg:\n'
                '                limit_exceeded = _qmsg\n'
                '        except Exception as _qe:\n'
                '            LOGGER.error(f"WZFIX quota check error: {_qe}")\n\n'
            ) + old_lc
            if old_lc in tm:
                tm = tm.replace(old_lc, new_lc, 1)
                n_tm += 1
            else:
                log("  r1: limit_checker anchor not found", "WARN")
        if "WZFIX pre-download allowance check" not in tm:
            old_pt = (
                '    if Config.RSS_CHAT and user_id == int(Config.RSS_CHAT):\n'
                '        return None, None'
            )
            new_pt = (
                '    try:  # WZFIX pre-download allowance check (v15.9)\n'
                '        from ..wzfix.r1_core import precheck, over_limit_msg\n'
                '        _qmsg = await precheck(message)\n'
                '        if _qmsg == "__WZFIX_HANDLED__":\n'
                '            # v15.27: precheck already replied in-place (the\n'
                '            # checking message was EDITED into the block\n'
                '            # message) — abort the task, send nothing more\n'
                '            return "__WZFIX_HANDLED__", None\n'
                '        if not _qmsg and not getattr(message, "_wzfix_held", False):\n'
                '            _qmsg = await over_limit_msg(user_id, user_dict)\n'
                '            if _qmsg:\n'
                '                # v15.28: over-quota without a pre-checkable\n'
                '                # link gets the ONE clean message + an admin\n'
                '                # log entry, not a wrapped Task-Checks card\n'
                '                from ..wzfix.r1_core import over_limit_reply\n'
                '                _qmsg = await over_limit_reply(message, user_id, _qmsg)\n'
                '                if _qmsg == "__WZFIX_HANDLED__":\n'
                '                    return "__WZFIX_HANDLED__", None\n'
                '        if _qmsg:\n'
                '            msg.append(_qmsg)\n'
                '    except Exception as _wz_e:\n'
                '        LOGGER.error(f"WZFIX pre-check failed (allowed): {_wz_e}")\n'
                '        pass\n\n'
            ) + old_pt
            if old_pt in tm:
                tm = tm.replace(old_pt, new_pt, 1)
                n_tm += 1
            else:
                log("  r1: pre_task_check anchor not found", "WARN")
        if n_tm:
            with open(tm_path, "w", encoding="utf-8") as f:
                f.write(tm)
            r = subprocess.run(
                [sys.executable, "-m", "py_compile", tm_path],
                capture_output=True, text=True, timeout=60,
            )
            if r.returncode == 0:
                log(f"  r1: task_manager patched ({n_tm}/2 hooks)")
            else:
                log(f"  r1: task_manager FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r1: task_manager patch FAILED — {e}", "ERROR")

    # I-3: record finished tasks (charge quota + library index)
    tl_path = os.path.join(WZMLX_DIR, "bot/helper/listeners/task_listener.py")
    try:
        with open(tl_path, "r", encoding="utf-8") as f:
            tl = f.read()
        if "WZFIX record failed" not in tl:
            old_oc = (
                '    async def on_upload_complete(\n'
                '        self, link, files, folders, mime_type, rclone_path="", dir_id=""\n'
                '    ):\n'
                '        if ('
            )
            new_oc = (
                '    async def on_upload_complete(\n'
                '        self, link, files, folders, mime_type, rclone_path="", dir_id=""\n'
                '    ):\n'
                '        try:\n'
                '            from ..wzfix.r1_core import record_task\n'
                '            await record_task(self, link, files, mime_type, rclone_path, dir_id)\n'
                '        except Exception as _we:\n'
                '            LOGGER.error(f"WZFIX record failed: {_we}")\n'
                '        if ('
            )
            if old_oc in tl:
                tl = tl.replace(old_oc, new_oc, 1)
                with open(tl_path, "w", encoding="utf-8") as f:
                    f.write(tl)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", tl_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r1: task_listener patched (1 hook)")
                else:
                    log(f"  r1: task_listener FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r1: on_upload_complete anchor not found", "WARN")
    except Exception as e:
        log(f"  r1: task_listener patch FAILED — {e}", "ERROR")

    # I-3b: reservations — free a task's bandwidth hold when it fails or is
    # cancelled (completion releases it from record_task itself)
    try:
        with open(tl_path, "r", encoding="utf-8") as f:
            tl3 = f.read()
        n_rel = 0
        if "WZFIX release on error" not in tl3:
            for _sig in (
                "    async def on_download_error(self, error, button=None, is_limit=False):\n        async with task_dict_lock:",
                "    async def on_upload_error(self, error):\n        async with task_dict_lock:",
            ):
                if _sig in tl3:
                    _def_line = _sig.split("\n", 1)[0] + "\n"
                    _rest = _sig.split("\n", 1)[1]
                    _rel = (
                        "        try:  # WZFIX release on error\n"
                        "            from ..wzfix.r1_core import release_reserve\n"
                        "            await release_reserve(self)\n"
                        "        except Exception:\n"
                        "            pass\n"
                    )
                    tl3 = tl3.replace(_sig, _def_line + _rel + _rest, 1)
                    n_rel += 1
                else:
                    log("  r1: release anchor not found: " + _sig.split("(")[0].strip(), "WARN")
            if n_rel:
                with open(tl_path, "w", encoding="utf-8") as f:
                    f.write(tl3)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", tl_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log(f"  r1: reservation release hooks patched ({n_rel}/2)")
                else:
                    log(f"  r1: release hooks FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r1: release hooks FAILED — {e}", "ERROR")

    # I-4: register the new commands at the end of add_handlers()
    hd_path = os.path.join(WZMLX_DIR, "bot/core/handlers.py")
    try:
        with open(hd_path, "r", encoding="utf-8") as f:
            hd = f.read()
        if "from ..modules.wzfix_admin import" not in hd:
            hd += (
                '\n    # WZFIX Round 1 (v15.7) — quota, library, db tools\n'
                '    from ..modules.wzfix_admin import (\n'
                '        wzfix_find,\n'
                '        wzfix_usage,\n'
                '        wzfix_qusers,\n'
                '        wzfix_setcap,\n'
                '        wzfix_addcap,\n'
                '        wzfix_deductcap,\n'
                '        wzfix_deluser,\n'
                '        wzfix_resetcap,\n'
                '        wzfix_botcap,\n'
                '        wzfix_dbstats,\n'
                '        wzfix_dbclean,\n'
                '        wzfix_diag,\n'
                '        wzfix_clean_cb,\n'
                '        wzfix_cancel_cb,\n'
                '        wzfix_adminpass,\n'
                '        wzfix_allow,\n'
                '        wzfix_bans,\n'
                '        wzfix_lockdash,\n'
                '    )\n'
                '    TgClient.bot.add_handler(\n'
                '        MessageHandler(\n'
                '            wzfix_find,\n'
                '            filters=command("find", case_sensitive=True)\n'
                '            & CustomFilters.authorized,\n'
                '        )\n'
                '    )\n'
                '    TgClient.bot.add_handler(\n'
                '        MessageHandler(\n'
                '            wzfix_usage,\n'
                '            filters=command("usage", case_sensitive=True)\n'
                '            & CustomFilters.authorized,\n'
                '        )\n'
                '    )\n'
                '    for _fn, _cmd in (\n'
                '        (wzfix_qusers, "qusers"),\n'
                '        (wzfix_setcap, "setcap"),\n'
                '        (wzfix_addcap, "addcap"),\n'
                '        (wzfix_deductcap, "deductcap"),\n'
                '        (wzfix_deluser, "deluser"),\n'
                '        (wzfix_resetcap, "resetcap"),\n'
                '        (wzfix_botcap, "botcap"),\n'
                '        (wzfix_dbstats, "dbstats"),\n'
                '        (wzfix_dbclean, "dbclean"),\n'
                '        (wzfix_diag, "wzfixdiag"),\n'
                '        (wzfix_adminpass, "adminpass"),\n'
                '        (wzfix_allow, "allow"),\n'
                '        (wzfix_bans, "bans"),\n'
                '        (wzfix_lockdash, "lockdash"),\n'
                '    ):\n'
                '        TgClient.bot.add_handler(\n'
                '            MessageHandler(\n'
                '                _fn, filters=command(_cmd, case_sensitive=True) & CustomFilters.sudo\n'
                '            )\n'
                '        )\n'
                '    TgClient.bot.add_handler(\n'
                '        CallbackQueryHandler(wzfix_clean_cb, filters=regex("^wzfixclean$"))\n'
                '    )\n'
                '    TgClient.bot.add_handler(\n'
                '        CallbackQueryHandler(wzfix_cancel_cb, filters=regex("^wzfixcancel$"))\n'
                '    )\n'
                '    # WZFIX r11 (v15.82): artist-batch playlist button\n'
                '    from ..helper.wzfix.r3_music import wzfix_pl_go\n'
                '    TgClient.bot.add_handler(\n'
                '        CallbackQueryHandler(wzfix_pl_go, filters=regex("^wzfxpl:"))\n'
                '    )\n'
            )
            with open(hd_path, "w", encoding="utf-8") as f:
                f.write(hd)
            r = subprocess.run(
                [sys.executable, "-m", "py_compile", hd_path],
                capture_output=True, text=True, timeout=60,
            )
            if r.returncode == 0:
                log("  r1: commands registered (find usage qusers setcap addcap deductcap deluser resetcap botcap dbstats dbclean wzfixdiag adminpass allow bans lockdash)")
            else:
                log(f"  r1: handlers FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r1: handlers patch FAILED — {e}", "ERROR")

    # I-5: compile-check the new modules themselves
    try:
        for _p in ("bot/helper/wzfix/r1_core.py", "bot/modules/wzfix_admin.py"):
            _fp = os.path.join(WZMLX_DIR, _p)
            r = subprocess.run(
                [sys.executable, "-m", "py_compile", _fp],
                capture_output=True, text=True, timeout=60,
            )
            if r.returncode != 0:
                log(f"  r1: {_p} FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r1: module compile check FAILED — {e}", "ERROR")

    # Kaggle addition J — v15.8 Round 2 Phase A: owner web dashboard
    # (served at /wzadmin from the bot's own stream server, so it reads
    # task_dict + DB directly) + wserver /wzadmin proxy + heartbeat watchdog
    # (alerts LOG_CHAT if the bot stops answering /_ping for 30+ minutes).
    try:
        _r2 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r2_web.py")
        with open(_r2, "w", encoding="utf-8") as f:
            f.write(base64.b64decode(WZFIX_WEB_B64).decode("utf-8"))
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", _r2],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode == 0:
            log("  r2: r2_web.py written (dashboard, reports, kill buttons)")
        else:
            log(f"  r2: r2_web.py FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r2: r2_web.py FAILED — {e}", "ERROR")

    # Kaggle addition O — v15.82 Round 9: dashboard reliability fixes
    # (login form always visible — no more black page, connection banner
    # with auto-retry, no-flicker section updates, history diff,
    # no-store headers) + stream auth loop-breaker with diagnostics +
    # full logging (auth attempts, gate denials, page serves, log-group
    # alerts on wrong passwords). Idempotent: markers are checked.
    def _r9_rep(src, old, new, tag):
        c = src.count(old)
        if c != 1:
            raise AssertionError("%s: anchor x%d" % (tag, c))
        return src.replace(old, new)

    try:
        # ---- O-A: r2_web.py (dashboard) ----
        _o_r2 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r2_web.py")
        with open(_o_r2, "r", encoding="utf-8") as _f:
            _src = _f.read()
        if 'id="connBar"' in _src:
            log("  r9: r2_web already patched")
        else:
            _src = _r9_rep(_src, '<div id="login" class="login-box hidden">',
                          '<div id="login" class="login-box">', "O-1")
            _src = _r9_rep(_src, '<div id="toast"></div>',
                          '<div id="toast"></div>\n<div id="connBar" class="hidden"></div>', "O-2a")
            _src = _r9_rep(_src, '.hidden{display:none}',
                          '.hidden{display:none}\n#connBar{position:fixed;left:0;right:0;bottom:0;'
                          'padding:9px 14px;background:#c0392b;color:#fff;font-size:13px;'
                          'text-align:center;z-index:9999}', "O-2b")
            _src = _r9_rep(_src, 'function toast(m){',
                          'function connBar(m){var b=$("connBar");if(!b)return;'
                          'if(m){b.textContent=m;b.classList.remove("hidden")}'
                          'else{b.classList.add("hidden")}}\n'
                          'window.onerror=function(msg,src,ln){connBar("Page error: "+msg+" (line "+ln+")");return false};\n'
                          'function toast(m){', "O-2c")
            _src = _r9_rep(_src,
                          'function api(p,o){return fetch("/wzadmin/api/"+p,o?{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(o)}:undefined).then(function(r){return r.json().then(function(j){j._s=r.status;return j})})}',
                          'function api(p,o){return fetch("/wzadmin/api/"+p,o?{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(o)}:undefined)'
                          '.then(function(r){connBar(null);return r.json().then(function(j){j._s=r.status;return j})})'
                          '.catch(function(e){connBar("\\u26a0 Server unreachable \\u2014 retrying\\u2026");throw e})}', "O-2d")
            _src = _r9_rep(_src, 'function refresh(){return api("state").then(function(j){\n  if(j._s===401){',
                          'window._lastState=null;window._sec={};\n'
                          'function setSec(id,html){if(window._sec[id]===html)return;'
                          'window._sec[id]=html;var e=$(id);if(e&&e.innerHTML!==html)e.innerHTML=html}\n'
                          'function refresh(){return api("state").then(function(j){\n'
                          '  var _sj=JSON.stringify(j);if(_sj===window._lastState)return;'
                          'window._lastState=_sj;\n'
                          '  if(j._s===401){window._lastState=null;', "O-3a")
            _src = _r9_rep(_src, '  $("stats").innerHTML=', '  setSec("stats",', "O-3b1")
            _src = _r9_rep(_src,
                          "'<div class=\"stat\"><div class=\"v\">'+t.users+'</div><div class=\"k\">Users</div></div>';",
                          "'<div class=\"stat\"><div class=\"v\">'+t.users+'</div><div class=\"k\">Users</div></div>');", "O-3b1b")
            _src = _r9_rep(_src,
                          '  $("tasks").innerHTML=T||\'<div class="card muted">No active tasks. Peace.</div>\';',
                          '  setSec("tasks",T||\'<div class="card muted">No active tasks. Peace.</div>\');', "O-3b2")
            _src = _r9_rep(_src,
                          '  $("users").innerHTML=U||\'<div class="card muted">No users yet.</div>\';',
                          '  setSec("users",U||\'<div class="card muted">No users yet.</div>\');', "O-3b3")
            _src = _r9_rep(_src,
                          '  $("access").innerHTML=A||\'<div class="card muted">No /start users recorded yet.</div>\';',
                          '  setSec("access",A||\'<div class="card muted">No /start users recorded yet.</div>\');', "O-3b4")
            _src = _r9_rep(_src,
                          'function renderHist(uid,scroll){\n  api("history?uid="+uid).then(function(j){',
                          'function renderHist(uid,scroll){\n  api("history?uid="+uid).then(function(j){\n'
                          '    var _hj=uid+":"+JSON.stringify(j.items||[]);'
                          'if(_hj===window._histLast)return;window._histLast=_hj;', "O-3c")
            _src = _r9_rep(_src, '    return web.Response(text=_PAGE, content_type="text/html")',
                          '    return web.Response(\n'
                          '        text=_PAGE, content_type="text/html",\n'
                          '        headers={"Cache-Control": "no-store, must-revalidate"},\n'
                          '    )', "O-4a")
            _src = _r9_rep(_src,
                          '    if path.endswith("/state"):\n        return web.json_response(await _state())',
                          '    if path.endswith("/state"):\n'
                          '        _r = web.json_response(await _state())\n'
                          '        _r.headers["Cache-Control"] = "no-store"\n'
                          '        return _r', "O-4b")
            # O-5: fix addition-M quoting regression (black page root cause)
            for _actn in ("userauth", "usersudo", "userbl"):
                _src = _r9_rep(
                    _src,
                    "act({action:'" + _actn + "',uid:'+u.uid+',v:'",
                    "act({action:\\\\'" + _actn + "\\\\',uid:'+u.uid+',v:'",
                    "O-5-" + _actn,
                )
            with open(_o_r2, "w", encoding="utf-8") as _f:
                _f.write(_src)
            _r = subprocess.run(
                [sys.executable, "-m", "py_compile", _o_r2],
                capture_output=True, text=True, timeout=60,
            )
            if _r.returncode == 0:
                log("  r9: r2_web patched (black-page fix, no-flicker, banner)")
            else:
                log(f"  r9: r2_web FAILED compile — {(_r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r9: r2_web patch FAILED — {e}", "ERROR")

    try:
        # ---- O-B: stall_ui.js (loop-breaker + diagnostics) ----
        _o_sui = os.path.join(WZMLX_DIR, "web/templates/stall_ui.js")
        with open(_o_sui, "r", encoding="utf-8") as _f:
            _js = _f.read()
        if "_loopInc" in _js:
            log("  r9: stall_ui already patched")
        else:
            _js = _r9_rep(_js, "  function getToken() {",
                          "  // r8 loop-breaker helpers\n"
                          "  function _loopCount() {\n"
                          "    try { return parseInt(sessionStorage.getItem(\"wzml_auth_loop\") || \"0\") || 0; }\n"
                          "    catch (e) { return window._wzLoop || 0; }\n"
                          "  }\n"
                          "  function _loopInc() {\n"
                          "    var n = _loopCount() + 1;\n"
                          "    try { sessionStorage.setItem(\"wzml_auth_loop\", String(n)); } catch (e) { window._wzLoop = n; }\n"
                          "    return n;\n"
                          "  }\n"
                          "  function _loopReset() {\n"
                          "    try { sessionStorage.removeItem(\"wzml_auth_loop\"); } catch (e) {}\n"
                          "    window._wzLoop = 0;\n"
                          "  }\n"
                          "  function _diagText() {\n"
                          "    return \"token=\" + (window.location.pathname.split(\"/\").pop() || \"\") +\n"
                          "      \" urlAuth=\" + (urlParams().get(\"auth\") ? \"yes\" : \"no\") +\n"
                          "      \" stored=\" + (getToken() ? \"yes\" : \"no\") +\n"
                          "      \" loops=\" + _loopCount() +\n"
                          "      \" ua=\" + (navigator.userAgent || \"\").slice(0, 80);\n"
                          "  }\n"
                          "  function _probeStatus(cb) {\n"
                          "    try {\n"
                          "      var t = window.location.pathname.split(\"/\").pop() || \"\";\n"
                          "      fetch(\"/api/stream/\" + t).then(function (r) { cb(r.status); })\n"
                          "        .catch(function () { cb(\"network\"); });\n"
                          "    } catch (e) { cb(\"n/a\"); }\n"
                          "  }\n"
                          "\n"
                          "  function getToken() {", "P-0")
            _js = _r9_rep(_js,
                          "        if (resp.ok && data.token) {\n"
                          "          storeToken(data.token);\n"
                          "          document.body.removeChild(overlay);\n"
                          "          // Reload with the token\n"
                          "          var sp = urlParams();\n"
                          "          sp.set(\"auth\", data.token);\n"
                          "          window.location.search = sp.toString();\n"
                          "        } else {",
                          "        if (resp.ok && data.token) {\n"
                          "          storeToken(data.token);\n"
                          "          var _n = _loopInc();\n"
                          "          if (_n >= 3) {\n"
                          "            // r8 loop-breaker: accepted 3x but still blocked -> diagnostics\n"
                          "            document.body.removeChild(overlay);\n"
                          "            _probeStatus(function (st) {\n"
                          "              alert(\"Password accepted but the stream is still blocked.\\n\\n\" +\n"
                          "                \"This is usually a server restart in progress, or an expired/changed link.\\n\" +\n"
                          "                \"Diagnostics: \" + _diagText() + \" probe=\" + st + \"\\n\\n\" +\n"
                          "                \"Tap OK to retry from a clean state.\");\n"
                          "              _loopReset();\n"
                          "              try { localStorage.removeItem(\"wzml_stream_auth\"); } catch (e) {}\n"
                          "              window.location.href = window.location.pathname;\n"
                          "            });\n"
                          "            return;\n"
                          "          }\n"
                          "          document.body.removeChild(overlay);\n"
                          "          // Reload with the token\n"
                          "          var sp = urlParams();\n"
                          "          sp.set(\"auth\", data.token);\n"
                          "          window.location.search = sp.toString();\n"
                          "        } else {", "P-1")
            _js = _r9_rep(_js,
                          "      return origFetch.call(this, url, opts).then(function (resp) {\n"
                          "        // Intercept 401 responses for stream endpoints\n"
                          "        if (\n"
                          "          resp.status === 401 &&",
                          "      return origFetch.call(this, url, opts).then(function (resp) {\n"
                          "        // r8: a successful stream request clears the auth-loop counter\n"
                          "        if (resp.status === 200 && urlStr.indexOf(\"/api/stream/\") >= 0) {\n"
                          "          _loopReset();\n"
                          "        }\n"
                          "        // Intercept 401 responses for stream endpoints\n"
                          "        if (\n"
                          "          resp.status === 401 &&", "P-2")
            with open(_o_sui, "w", encoding="utf-8") as _f:
                _f.write(_js)
            _r = subprocess.run(
                ["node", "--check", _o_sui],
                capture_output=True, text=True, timeout=60,
            )
            if _r.returncode == 0:
                log("  r9: stall_ui patched (loop-breaker, diagnostics)")
            else:
                log(f"  r9: stall_ui FAILED check — {(_r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r9: stall_ui patch FAILED — {e}", "ERROR")

    try:
        # ---- O-C: stream_server.py (auth logging + log-group alerts) ----
        _o_ss = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(_o_ss, "r", encoding="utf-8") as _f:
            _py = _f.read()
        if "_r8_group" in _py:
            log("  r9: stream_server already patched")
        else:
            _py = _r9_rep(_py, "async def _ks_auth_api(request):",
                          "def _r8_group(title, text):\n"
                          "    \"\"\"r8: fire-and-forget alert to the admin log group.\"\"\"\n"
                          "    try:\n"
                          "        from asyncio import get_event_loop as _gel\n"
                          "\n"
                          "        async def _send():\n"
                          "            try:\n"
                          "                from bot.helper.wzfix.r1_core import admin_log\n"
                          "\n"
                          "                await admin_log(title, text)\n"
                          "            except Exception:\n"
                          "                pass\n"
                          "\n"
                          "        _gel().create_task(_send())\n"
                          "    except Exception:\n"
                          "        pass\n"
                          "\n"
                          "\n"
                          "async def _ks_auth_api(request):", "P-3a")
            _py = _r9_rep(_py,
                          "    _tok5 = str(body.get(\"token\", \"\") or \"\")\n"
                          "    if _tok5:\n"
                          "        try:\n"
                          "            _lp5 = await _r5_get_link_pass(_tok5)\n"
                          "        except Exception:\n"
                          "            _lp5 = None\n"
                          "        if _lp5 is not None:\n"
                          "            _sub5 = str(body.get(\"password\", \"\") or \"\")\n"
                          "            if not _sub5 or not _ks_hmac.compare_digest(_sub5, _lp5):\n"
                          "                return web.json_response(\n"
                          "                    {\"error\": \"wrong password\"}, status=401)\n"
                          "            return web.json_response(\n"
                          "                {\"token\": _us_sign(_lp5), \"expires\": 86400,\n"
                          "                 \"link\": _tok5})",
                          "    _tok5 = str(body.get(\"token\", \"\") or \"\")\n"
                          "    _ip8 = request.remote or \"?\"\n"
                          "    if _tok5:\n"
                          "        try:\n"
                          "            _lp5 = await _r5_get_link_pass(_tok5)\n"
                          "        except Exception:\n"
                          "            _lp5 = None\n"
                          "        if _lp5 is not None:\n"
                          "            _sub5 = str(body.get(\"password\", \"\") or \"\")\n"
                          "            if not _sub5 or not _ks_hmac.compare_digest(_sub5, _lp5):\n"
                          "                LOGGER.info(\n"
                          "                    f\"WZFIX stream auth FAIL: token={_tok5[:10]} ip={_ip8}\")\n"
                          "                _r8_group(\n"
                          "                    \"\\u274c Stream password\",\n"
                          "                    f\"Wrong password for link <code>{_tok5[:12]}</code> \"\n"
                          "                    f\"from <code>{_ip8}</code>\",\n"
                          "                )\n"
                          "                return web.json_response(\n"
                          "                    {\"error\": \"wrong password\"}, status=401)\n"
                          "            LOGGER.info(\n"
                          "                f\"WZFIX stream auth OK: token={_tok5[:10]} ip={_ip8}\")\n"
                          "            return web.json_response(\n"
                          "                {\"token\": _us_sign(_lp5), \"expires\": 86400,\n"
                          "                 \"link\": _tok5})", "P-3b")
            _py = _r9_rep(_py,
                          "    password = _us_get_pass()\n"
                          "    if not password:\n"
                          "        return web.json_response({\"error\": \"STREAM_PASS not set\"})\n"
                          "    submitted = body.get(\"password\", \"\")\n"
                          "    if not submitted or not _ks_hmac.compare_digest(submitted, password):\n"
                          "        return web.json_response({\"error\": \"wrong password\"}, status=401)\n"
                          "    return web.json_response({\"token\": _us_sign(password), \"expires\": 86400})",
                          "    password = _us_get_pass()\n"
                          "    if not password:\n"
                          "        LOGGER.info(f\"WZFIX stream auth: no pass set ip={_ip8}\")\n"
                          "        return web.json_response({\"error\": \"STREAM_PASS not set\"})\n"
                          "    submitted = body.get(\"password\", \"\")\n"
                          "    if not submitted or not _ks_hmac.compare_digest(submitted, password):\n"
                          "        LOGGER.info(\n"
                          "            f\"WZFIX stream auth FAIL (global): ip={_ip8} tok={_tok5[:10]}\")\n"
                          "        _r8_group(\n"
                          "            \"\\u274c Stream password\",\n"
                          "            f\"Wrong global password from <code>{_ip8}</code>\",\n"
                          "        )\n"
                          "        return web.json_response({\"error\": \"wrong password\"}, status=401)\n"
                          "    LOGGER.info(f\"WZFIX stream auth OK (global): ip={_ip8} tok={_tok5[:10]}\")\n"
                          "    return web.json_response({\"token\": _us_sign(password), \"expires\": 86400})", "P-3c")
            with open(_o_ss, "w", encoding="utf-8") as _f:
                _f.write(_py)
            _r = subprocess.run(
                [sys.executable, "-m", "py_compile", _o_ss],
                capture_output=True, text=True, timeout=60,
            )
            if _r.returncode == 0:
                log("  r9: stream_server patched (auth logging + group alerts)")
            else:
                log(f"  r9: stream_server FAILED compile — {(_r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r9: stream_server patch FAILED — {e}", "ERROR")

    try:
        # ---- O-D: r5_streampass.py (gate denial logging) ----
        _o_r5 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r5_streampass.py")
        with open(_o_r5, "r", encoding="utf-8") as _f:
            _r5s = _f.read()
        if "WZFIX stream gate DENIED" in _r5s:
            log("  r9: r5 gate already patched")
        else:
            _r5s = _r9_rep(_r5s,
                           "async def serve_ok(request):\n"
                           "    \"\"\"Per-link gate for the stream server. True = allow.\"\"\"\n"
                           "    try:\n"
                           "        tok = path_token(request)\n"
                           "        if not tok:\n"
                           "            return True\n"
                           "        lp = await get_link_pass(tok)\n"
                           "        if lp is None:\n"
                           "            return True\n"
                           "        return verify_link_token(request.query.get(\"auth\"), lp)\n"
                           "    except Exception:\n"
                           "        return True",
                           "async def serve_ok(request):\n"
                           "    \"\"\"Per-link gate for the stream server. True = allow.\"\"\"\n"
                           "    import logging as _r8log\n"
                           "\n"
                           "    try:\n"
                           "        tok = path_token(request)\n"
                           "        if not tok:\n"
                           "            return True\n"
                           "        lp = await get_link_pass(tok)\n"
                           "        if lp is None:\n"
                           "            return True\n"
                           "        ok = verify_link_token(request.query.get(\"auth\"), lp)\n"
                           "        if not ok:\n"
                           "            _r8log.getLogger(__name__).info(\n"
                           "                \"WZFIX stream gate DENIED: token=%s ip=%s auth=%s\",\n"
                           "                str(tok)[:10],\n"
                           "                getattr(request, \"remote\", \"?\"),\n"
                           "                \"present\" if request.query.get(\"auth\") else \"missing\",\n"
                           "            )\n"
                           "        return ok\n"
                           "    except Exception:\n"
                           "        return True", "P-3d")
            with open(_o_r5, "w", encoding="utf-8") as _f:
                _f.write(_r5s)
            _r = subprocess.run(
                [sys.executable, "-m", "py_compile", _o_r5],
                capture_output=True, text=True, timeout=60,
            )
            if _r.returncode == 0:
                log("  r9: r5 gate patched (denial logging)")
            else:
                log(f"  r9: r5 gate FAILED compile — {(_r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r9: r5 gate patch FAILED — {e}", "ERROR")

    try:
        # ---- O-E: wserver.py (page-serve logging) ----
        _o_ws = os.path.join(WZMLX_DIR, "web/wserver.py")
        with open(_o_ws, "r", encoding="utf-8") as _f:
            _ws = _f.read()
        if "WZFIX xstrm page" in _ws:
            log("  r9: wserver already patched")
        else:
            _ws = _r9_rep(_ws,
                          'async def xstrm_page(token: str, request: Request):\n'
                          '    if not _SAFE_TOKEN.match(token or ""):\n'
                          '        raise HTTPException(status_code=404, detail="Unknown link")',
                          'async def xstrm_page(token: str, request: Request):\n'
                          '    if not _SAFE_TOKEN.match(token or ""):\n'
                          '        raise HTTPException(status_code=404, detail="Unknown link")\n'
                          '    # r8: log every stream page serve\n'
                          '    LOGGER.info(\n'
                          '        f"WZFIX xstrm page: token={str(token)[:10]} ip={request.client.host if request.client else \'?\'}"\n'
                          '    )', "P-3e")
            with open(_o_ws, "w", encoding="utf-8") as _f:
                _f.write(_ws)
            _r = subprocess.run(
                [sys.executable, "-m", "py_compile", _o_ws],
                capture_output=True, text=True, timeout=60,
            )
            if _r.returncode == 0:
                log("  r9: wserver patched (page-serve logging)")
            else:
                log(f"  r9: wserver FAILED compile — {(_r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r9: wserver patch FAILED — {e}", "ERROR")

    # J-31: silent cmd-message fallback (v15.82) — the uploader's
    # "Deleted Cmd Message! Don't delete the cmd message again!" warning
    # fires for EVERY fan-out clone (clones carry fake message ids the
    # chat never had), so artist batches spam it. Fall back silently to
    # the original command message instead.
    try:
        _p31 = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/upload_utils/"
            "telegram_uploader.py",
        )
        with open(_p31, "r", encoding="utf-8") as f:
            _t31 = f.read()
        if "WZFIX r14 silent cmd fallback" not in _t31:
            _old31 = r"""            if self._sent_msg is None or self._sent_msg.chat is None:
                try:
                    self._sent_msg = await _call_with_flood_retry(
                        self._listener.client.send_message,
                        chat_id=self._listener.message.chat.id,
                        text="Deleted Cmd Message! Don't delete the cmd message again!",
                        disable_web_page_preview=True,
                        disable_notification=True,
                    )
                except Exception:
                    self._sent_msg = self._listener.message"""
            _new31 = r"""            if self._sent_msg is None or self._sent_msg.chat is None:
                # WZFIX r14 silent cmd fallback: fan-out clones carry
                # fake message ids, so this lookup always fails — reply
                # to the original command message instead of spamming
                # the chat with a "Deleted Cmd Message!" warning
                LOGGER.info(
                    "WZFIX r14: upload replies to the original "
                    "cmd message (clone/deleted cmd)"
                )
                self._sent_msg = self._listener.message"""
            if _old31 in _t31:
                _t31 = _t31.replace(_old31, _new31, 1)
                with open(_p31, "w", encoding="utf-8") as f:
                    f.write(_t31)
                _r31 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p31],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r31.returncode == 0:
                    log("  r2: J-31 silent cmd fallback applied")
                else:
                    log(
                        f"  r2: J-31 compile FAILED — "
                        f"{(_r31.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-31 anchor missing (uploader)", "WARN")
        else:
            log("  r2: J-31 already applied")
    except Exception as e:
        log(f"  r2: J-31 patch FAILED — {e}", "ERROR")

    # J-32: deterministic fan-out merge (v15.82) — artist-batch clones
    # move their song into the leader's folder and finish quietly;
    # only the task that finishes last uploads, so every song reaches
    # the chat exactly once (this replaces the fragile upstream
    # same_dir race that produced duplicate uploads)
    try:
        _p32 = os.path.join(
            WZMLX_DIR, "bot/helper/listeners/task_listener.py"
        )
        with open(_p32, "r", encoding="utf-8") as f:
            _t32 = f.read()
        if "WZFIX r15 deterministic merge" not in _t32:
            _old32 = (
                "        multi_links = False\n"
                "        if (\n"
            )
            _new32 = r"""        multi_links = False
        # WZFIX r15 deterministic merge: music fan-out clones move
        # their song into the leader folder; the LAST task to finish
        # uploads everything exactly once
        try:
            _wz15 = (self.same_dir or {}).get(self.folder_name, {}).get(
                "_wzfix"
            )
        except Exception:
            _wz15 = None
        if _wz15 is not None and _wz15.get("leader_mid"):
            async with same_directory_lock:
                import os as _os15

                _sp15 = _os15.path.normpath(
                    f"{self.dir}{self.folder_name}"
                )
                if _os15.path.isdir(_sp15):
                    _dp15 = _os15.path.normpath(
                        f"{DOWNLOAD_DIR}{_wz15['leader_mid']}"
                        f"{self.folder_name}"
                    )
                    if _dp15 != _sp15:
                        await move_and_merge(_sp15, _dp15, self.mid)
                        LOGGER.info(
                            "WZFIX r15: merged a song into the leader "
                            "folder"
                        )
                _wz15.setdefault("done", set()).add(self.mid)
                if len(_wz15["done"]) >= _wz15.get("total", 10**9):
                    LOGGER.info(
                        "WZFIX r15: all songs merged — single upload "
                        "from the leader folder"
                    )
                    self.dir = f"{DOWNLOAD_DIR}{_wz15['leader_mid']}"
                else:
                    await self.on_upload_error(
                        f"{self.name} Downloaded!\n\nWaiting for other "
                        "tasks to finish..."
                    )
                    return
        if (
"""
            if _old32 in _t32:
                _t32 = _t32.replace(_old32, _new32, 1)
                with open(_p32, "w", encoding="utf-8") as f:
                    f.write(_t32)
                _r32 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p32],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r32.returncode == 0:
                    log("  r2: J-32 deterministic fan-out merge applied")
                else:
                    log(
                        f"  r2: J-32 compile FAILED — "
                        f"{(_r32.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-32 anchor missing (task_listener)", "WARN")
        else:
            log("  r2: J-32 already applied")
    except Exception as e:
        log(f"  r2: J-32 patch FAILED — {e}", "ERROR")

    # J-33: single-copy DM (v15.82) — with BOT_PM on and the batch
    # running in the user's DM, every song was sent to the DM twice
    # (upload reply + PM copy). Music batches keep just the reply.
    try:
        _p33 = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/upload_utils/"
            "telegram_uploader.py",
        )
        with open(_p33, "r", encoding="utf-8") as f:
            _t33 = f.read()
        if "WZFIX r15 single-copy DM" not in _t33:
            _old33 = (
                "        await self._user_settings()\n"
                "        res = await self._msg_to_reply()\n"
            )
            _new33 = (
                "        await self._user_settings()\n"
                "        # WZFIX r15 single-copy DM: music batches skip\n"
                "        # the BOT_PM duplicate (the reply copy is\n"
                "        # already in the user's chat)\n"
                "        if getattr(self._listener, "
                '"_wzfix_music", False):\n'
                "            self._bot_pm = False\n"
                "        res = await self._msg_to_reply()\n"
            )
            if _old33 in _t33:
                _t33 = _t33.replace(_old33, _new33, 1)
                with open(_p33, "w", encoding="utf-8") as f:
                    f.write(_t33)
                _r33 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p33],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r33.returncode == 0:
                    log("  r2: J-33 single-copy DM applied")
                else:
                    log(
                        f"  r2: J-33 compile FAILED — "
                        f"{(_r33.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-33 anchor missing (uploader)", "WARN")
        else:
            log("  r2: J-33 already applied")
    except Exception as e:
        log(f"  r2: J-33 patch FAILED — {e}", "ERROR")

    # J-34: m4a passthrough (v15.82) — every song was transcoded to
    # mp3 (aac -> mp3 re-encode: CPU-bound and quality-losing). Music
    # now downloads YouTube's native m4a audio and copies it into the
    # container (no transcode): faster, lighter and better quality.
    try:
        _p34 = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/"
            "yt_dlp_download.py",
        )
        with open(_p34, "r", encoding="utf-8") as f:
            _t34 = f.read()
        if "WZFIX r15 m4a passthrough" not in _t34:
            _old34 = (
                '        if qual.startswith("ba/b-"):\n'
                '            audio_info = qual.split("-")\n'
            )
            _new34 = (
                '        # WZFIX r15 m4a passthrough: format strings may'
                ' carry\n'
                '        # a preferred-audio prefix (ba[ext=m4a]/...)'
                ' before\n'
                '        # the ba/b-<codec>-<rate> tail\n'
                '        if "/ba/b-" in qual or '
                'qual.startswith("ba/b-"):\n'
                '            _pfx34, _sfx34 = qual.split("/b-", 1)\n'
                '            audio_info = _sfx34.split("-")\n'
                '            qual = f"{_pfx34}/b"\n'
            )
            if _old34 in _t34:
                _t34 = _t34.replace(_old34, _new34, 1)
                with open(_p34, "w", encoding="utf-8") as f:
                    f.write(_t34)
                _r34 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p34],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r34.returncode == 0:
                    log("  r2: J-34 m4a passthrough applied")
                else:
                    log(
                        f"  r2: J-34 compile FAILED — "
                        f"{(_r34.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-34 anchor missing (yt_dlp_download)", "WARN")
        else:
            log("  r2: J-34 already applied")
    except Exception as e:
        log(f"  r2: J-34 patch FAILED — {e}", "ERROR")

    # J-1: register /wzadmin routes on the bot stream server (in-process)
    try:
        ss3_path = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(ss3_path, "r", encoding="utf-8") as f:
            s3 = f.read()
        if "wzadmin" not in s3:
            _anchor_dl = '    app.router.add_route("*", "/_dl/{token}", _dl)'
            _routes = (
                '    # KAGGLE_R2: owner dashboard (v15.8)\n'
                '    try:\n'
                '        from ..helper.wzfix.r2_web import wzadmin_page, wzadmin_api\n'
                '        app.router.add_route("GET", "/wzadmin", wzadmin_page)\n'
                '        app.router.add_route("*", "/wzadmin/{path:.*}", wzadmin_api)\n'
                '    except Exception as e:\n'
                '        LOGGER.error(f"r2 dashboard routes: {e}")\n'
            )
            if _anchor_dl in s3:
                s3 = s3.replace(_anchor_dl, _anchor_dl + "\n" + _routes, 1)
                with open(ss3_path, "w", encoding="utf-8") as f:
                    f.write(s3)
                log("  r2: /wzadmin routes registered on the stream server")
            else:
                log("  r2: stream server /_dl anchor not found", "WARN")
        else:
            log("  r2: stream server already has /wzadmin")
    except Exception as e:
        log(f"  r2: stream server patch FAILED — {e}", "ERROR")

    # J-2: wserver — proxy /wzadmin to the bot + heartbeat watchdog
    try:
        ws3_path = os.path.join(WZMLX_DIR, "web/wserver.py")
        with open(ws3_path, "r", encoding="utf-8") as f:
            w3 = f.read()
        if "KAGGLE_R2_WSERVER" not in w3:
            _life_anchor = (
                "@asynccontextmanager\n"
                "async def lifespan(app: FastAPI):"
            )
            _sess_anchor = "    http_session = ClientSession(auto_decompress=True)"
            _wd_fn = (
                "\n\nasync def _wz_watchdog():  # KAGGLE_R2_WSERVER\n"
                "    # Alerts LOG_CHAT via the Bot API when the bot stream\n"
                "    # server has not answered /_ping for 30+ minutes\n"
                "    # (wserver outlives a hung bot process).\n"
                "    import asyncio as _aio\n"
                "    import time as _t\n"
                "    from importlib import import_module as _im\n"
                "    try:\n"
                "        _cfg = _im(\"config\")\n"
                "    except ModuleNotFoundError:\n"
                "        _cfg = None\n"
                "    _token = environ.get(\"BOT_TOKEN\", \"\") or (\n"
                "        getattr(_cfg, \"BOT_TOKEN\", \"\") if _cfg else \"\"\n"
                "    )\n"
                "    _chat = environ.get(\"LOG_CHAT\", \"\") or (\n"
                "        getattr(_cfg, \"LOG_CHAT\", \"\") if _cfg else \"\"\n"
                "    )\n"
                "    if not _token or not _chat:\n"
                "        print(\"[wz-watchdog] disabled: BOT_TOKEN or LOG_CHAT missing\")\n"
                "        return\n"
                "    _fail_since = None\n"
                "    _last_alert = 0.0\n"
                "    _api = \"https://api.telegram.org/bot\" + _token + \"/sendMessage\"\n"
                "    while True:\n"
                "        try:\n"
                "            async with http_session.get(f\"{STREAM_BASE}/_ping\") as _r:\n"
                "                _ok = _r.status == 200\n"
                "        except Exception:\n"
                "            _ok = False\n"
                "        _now = _t.time()\n"
                "        if _ok:\n"
                "            if _fail_since is not None:\n"
                "                _fail_since = None\n"
                "                try:\n"
                "                    await http_session.post(\n"
                "                        _api,\n"
                "                        json={\"chat_id\": _chat, \"text\": \"\\u2705 WZML-X heartbeat: the bot is back.\"},\n"
                "                    )\n"
                "                except Exception:\n"
                "                    pass\n"
                "            await _aio.sleep(300)\n"
                "            continue\n"
                "        if _fail_since is None:\n"
                "            _fail_since = _now\n"
                "        _mins = (_now - _fail_since) / 60.0\n"
                "        if _mins >= 30.0 and (_now - _last_alert) >= 3600.0:\n"
                "            _last_alert = _now\n"
                "            try:\n"
                "                await http_session.post(\n"
                "                    _api,\n"
                "                    json={\n"
                "                        \"chat_id\": _chat,\n"
                "                        \"text\": (\n"
                "                            \"\\u26a0\\ufe0f WZML-X heartbeat: the bot has been \"\n"
                "                            f\"unreachable for {int(_mins)} minutes \"\n"
                "                            \"(stream server not answering /_ping).\"\n"
                "                        ),\n"
                "                    },\n"
                "                )\n"
                "            except Exception:\n"
                "                pass\n"
                "        await _aio.sleep(300)\n"
                "\n\n"
            )
            _wd_start = (
                "\n    from asyncio import create_task as _wzct\n"
                "    app.state.wz_watchdog = _wzct(_wz_watchdog())  # KAGGLE_R2_WSERVER"
            )
            _proxy = (
                '\n\n@app.api_route("/wzadmin", methods=["GET"])  # KAGGLE_R2_WSERVER\n'
                '@app.api_route("/wzadmin/{path:path}", methods=["GET", "POST"])\n'
                "async def wzadmin_proxy(request: Request):\n"
                '    from fastapi.responses import Response as _Resp\n'
                '    _target = f"{STREAM_BASE}{request.url.path}"\n'
                "    if request.url.query:\n"
                '        _target += f"?{request.url.query}"\n'
                "    _fwd = {\n"
                "        k: v for k, v in request.headers.items()\n"
                '        if k.lower() not in ("host", "content-length", "accept-encoding")\n'
                "    }\n"
                "    _body = await request.body()\n"
                "    try:\n"
                "        async with http_session.request(\n"
                "            request.method, _target, headers=_fwd, data=_body\n"
                "        ) as _up:\n"
                "            _raw = await _up.read()\n"
                "            return _Resp(\n"
                "                content=_raw,\n"
                "                status_code=_up.status,\n"
                "                headers={\n"
                "                    k: v for k, v in _up.headers.items()\n"
                "                    if k.lower()\n"
                '                    not in (\n'
                '                        "transfer-encoding",\n'
                '                        "content-length",\n'
                '                        "content-encoding",\n'
                '                        "connection",\n'
                "                    )\n"
                "                },\n"
                '                media_type=_up.headers.get("content-type"),\n'
                "            )\n"
                "    except Exception as _e:\n"
                "        return JSONResponse(\n"
                '            {"error": f"wzadmin upstream unreachable: {_e.__class__.__name__}"},\n'
                "            status_code=502,\n"
                "        )\n"
            )
            _ok_j2 = 0
            if _life_anchor in w3:
                w3 = w3.replace(_life_anchor, _wd_fn + _life_anchor, 1)
                _ok_j2 += 1
            if _sess_anchor in w3:
                w3 = w3.replace(
                    _sess_anchor, _sess_anchor + _wd_start, 1
                )
                _ok_j2 += 1
            _bridge_end = (
                "        return JSONResponse(\n"
                '            {"error": f"stream auth unavailable: {e.__class__.__name__}"},\n'
                "            status_code=503,\n"
                "        )"
            )
            if _bridge_end in w3:
                w3 = w3.replace(_bridge_end, _bridge_end + _proxy, 1)
                _ok_j2 += 1
            else:
                _home_anchor = '@app.get("/", response_class=HTMLResponse)'
                if _home_anchor in w3:
                    w3 = w3.replace(
                        _home_anchor, _proxy.strip("\n") + "\n\n" + _home_anchor, 1
                    )
                    _ok_j2 += 1
            with open(ws3_path, "w", encoding="utf-8") as f:
                f.write(w3)
            r = subprocess.run(
                [sys.executable, "-m", "py_compile", ws3_path],
                capture_output=True, text=True, timeout=60,
            )
            if r.returncode == 0:
                log(f"  r2: wserver patched (proxy + watchdog, {_ok_j2}/3 edits)")
            else:
                log(f"  r2: wserver FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
        else:
            log("  r2: wserver already patched")
    except Exception as e:
        log(f"  r2: wserver patch FAILED — {e}", "ERROR")


    # J-3: landing page — add the Owner Dashboard link alongside the others
    try:
        lp_path = os.path.join(WZMLX_DIR, "web/templates/landing.html")
        with open(lp_path, "r", encoding="utf-8") as f:
            lp = f.read()
        if "/wzadmin" not in lp:
            _nav_close = "            </a>\n        </nav>"
            _dash = (
                "            </a>\n"
                '            <a href="/wzadmin" class="button">\n'
                '                <span class="ico"><svg aria-hidden="true"><use href="#i-book"/></svg></span> Owner Dashboard\n'
                '                <span class="arrow"><svg aria-hidden="true"><use href="#i-arrow"/></svg></span>\n'
                "            </a>\n"
                "        </nav>"
            )
            if _nav_close in lp:
                lp = lp.replace(_nav_close, _dash, 1)
                with open(lp_path, "w", encoding="utf-8") as f:
                    f.write(lp)
                log("  r2: landing page shows the dashboard link")
            else:
                log("  r2: landing page nav anchor not found", "WARN")
        else:
            log("  r2: landing page already patched")
    except Exception as e:
        log(f"  r2: landing page patch FAILED — {e}", "ERROR")

    # J-4: aria2 size-check callback hardening. Upstream checks the size
    # ~3s after the download already started, and silently skips the whole
    # check when the task is not registered in task_dict yet (a race right
    # after addUri). Wait for the registration with bounded retries so the
    # quota check always runs.
    try:
        a2_path = os.path.join(WZMLX_DIR, "bot/helper/listeners/aria2_listener.py")
        with open(a2_path, "r", encoding="utf-8") as f:
            a2 = f.read()
        if "WZFIX a2 check retry" not in a2:
            old_cb = (
                "    await sleep(2)\n"
                "    if task := await get_task_by_gid(gid):\n"
                "        download = await api.tellStatus(gid)"
            )
            new_cb = (
                "    await sleep(2)\n"
                "    task = None  # WZFIX a2 check retry (upstream race)\n"
                "    for _ in range(10):\n"
                "        task = await get_task_by_gid(gid)\n"
                "        if task:\n"
                "            break\n"
                "        await sleep(1)\n"
                "    if task:\n"
                "        download = await api.tellStatus(gid)"
            )
            if old_cb in a2:
                a2 = a2.replace(old_cb, new_cb, 1)
                with open(a2_path, "w", encoding="utf-8") as f:
                    f.write(a2)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", a2_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: aria2 size-check callback hardened (retry)")
                else:
                    log(f"  r2: aria2 hardening FAILED compile — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: aria2 callback anchor not found", "WARN")
        else:
            log("  r2: aria2 callback already hardened")
    except Exception as e:
        log(f"  r2: aria2 hardening FAILED — {e}", "ERROR")

    # J-5: ADMIN_PASS as a /bs-editable config variable (same pattern the
    # kit used for STREAM_PASS): a Config attribute plus a bot_settings
    # description, so it shows up and can be changed live via /bs.
    # Priority at runtime: /bs-set ADMIN_PASS > DB value (/adminpass).
    try:
        cm_path = os.path.join(WZMLX_DIR, "bot/core/config_manager.py")
        with open(cm_path, "r", encoding="utf-8") as f:
            cm = f.read()
        if '    ADMIN_PASS = ""' not in cm:
            mark = '    BASE_URL = ""\n'
            if mark in cm:
                cm = cm.replace(mark, mark + '    ADMIN_PASS = ""\n', 1)
                with open(cm_path, "w", encoding="utf-8") as f:
                    f.write(cm)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", cm_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: ADMIN_PASS added to config_manager")
                else:
                    log(f"  r2: config_manager compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: config_manager BASE_URL anchor not found", "WARN")
        else:
            log("  r2: ADMIN_PASS already in config_manager")
    except Exception as e:
        log(f"  r2: ADMIN_PASS config patch FAILED — {e}", "ERROR")

    try:
        bs_path = os.path.join(WZMLX_DIR, "bot/modules/bot_settings.py")
        with open(bs_path, "r", encoding="utf-8") as f:
            bs = f.read()
        if '"ADMIN_PASS"' not in bs:
            mark = '    "STREAM_TOKENS": "Bot tokens dedicated to /stream and /dl. If set, streaming uses these and is isolated from mirror/leech load. Falls back to HELPER_TOKENS.",\n'
            if mark in bs:
                add = ('    "ADMIN_PASS": "Password for the owner web dashboard '
                       '(/wzadmin on your worker/tunnel link). If empty, an '
                       'auto-generated password is used — see it with '
                       '/adminpass in Telegram. Separate from STREAM_PASS.",\n')
                bs = bs.replace(mark, mark + add, 1)
                with open(bs_path, "w", encoding="utf-8") as f:
                    f.write(bs)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", bs_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: ADMIN_PASS added to /bs settings list")
                else:
                    log(f"  r2: bot_settings compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: bot_settings STREAM_TOKENS anchor not found", "WARN")
        else:
            log("  r2: ADMIN_PASS already in /bs settings list")
    except Exception as e:
        log(f"  r2: ADMIN_PASS /bs patch FAILED — {e}", "ERROR")

    # J-6: dashboard access-control wiring.
    # (a) STREAM_PASS must stay a string: WZML's /bs editor turns any
    #     all-digit value into an int, which silently broke stream login
    #     ("11" became 11). Coerce at every read.
    try:
        usm_path = os.path.join(WZMLX_DIR, "bot/helper/user_stream_module.py")
        with open(usm_path, "r", encoding="utf-8") as f:
            usm = f.read()
        _sp_old = 'return getattr(Config, "STREAM_PASS", "") or ""'
        _sp_new = 'return str(getattr(Config, "STREAM_PASS", "") or "")'
        if _sp_old in usm:
            usm = usm.replace(_sp_old, _sp_new, 1)
            with open(usm_path, "w", encoding="utf-8") as f:
                f.write(usm)
            log("  r2: STREAM_PASS coerced to str (int /bs value fix)")
        elif _sp_new in usm:
            log("  r2: STREAM_PASS str coercion already present")
        else:
            log("  r2: STREAM_PASS anchor not found in user_stream_module", "WARN")
    except Exception as e:
        log(f"  r2: STREAM_PASS coercion FAILED — {e}", "ERROR")

    # (b) keep STREAM_PASS / ADMIN_PASS as strings in future /bs edits
    try:
        bs_path = os.path.join(WZMLX_DIR, "bot/modules/bot_settings.py")
        with open(bs_path, "r", encoding="utf-8") as f:
            bs = f.read()
        if 'elif key == "ADMIN_PASS":' not in bs:
            _lp = '    elif key == "LOGIN_PASS":\n        value = str(value)\n'
            _add = (
                _lp
                + '    elif key == "STREAM_PASS":\n        value = str(value)\n'
                + '    elif key == "ADMIN_PASS":\n        value = str(value)\n'
            )
            if _lp in bs:
                bs = bs.replace(_lp, _add, 1)
                with open(bs_path, "w", encoding="utf-8") as f:
                    f.write(bs)
                log("  r2: /bs keeps STREAM_PASS/ADMIN_PASS as strings")
            else:
                log("  r2: LOGIN_PASS anchor not found in bot_settings", "WARN")
        else:
            log("  r2: /bs string branches already present")
    except Exception as e:
        log(f"  r2: /bs string patch FAILED — {e}", "ERROR")

    # (c) new Config vars: login alerts toggle + dashboard lock
    try:
        cm_path = os.path.join(WZMLX_DIR, "bot/core/config_manager.py")
        with open(cm_path, "r", encoding="utf-8") as f:
            cm = f.read()
        if "ADMIN_LOGIN_ALERTS" not in cm:
            mark = '    ADMIN_PASS = ""\n'
            if mark in cm:
                cm = cm.replace(
                    mark,
                    mark + '    ADMIN_LOGIN_ALERTS = True\n'
                           '    ADMIN_DASHBOARD_LOCKED = False\n',
                    1,
                )
                with open(cm_path, "w", encoding="utf-8") as f:
                    f.write(cm)
                log("  r2: ADMIN_LOGIN_ALERTS + ADMIN_DASHBOARD_LOCKED added")
            else:
                log("  r2: ADMIN_PASS anchor missing in config_manager", "WARN")
        else:
            log("  r2: alert/lock config vars already present")
    except Exception as e:
        log(f"  r2: alert/lock config patch FAILED — {e}", "ERROR")

    # (d) /bs descriptions + On/Off toggles for both new vars
    try:
        bs_path = os.path.join(WZMLX_DIR, "bot/modules/bot_settings.py")
        with open(bs_path, "r", encoding="utf-8") as f:
            bs = f.read()
        if '"ADMIN_LOGIN_ALERTS"' not in bs:
            mark = '    "ADMIN_PASS": "Password for the owner web dashboard'
            if mark in bs:
                _i = bs.index(mark)
                _j = bs.index("\n", _i) + 1
                _add = (
                    '    "ADMIN_LOGIN_ALERTS": "Send every dashboard login '
                    'attempt (failed, banned, succeeded-after-fails) to '
                    'LOG_CHAT with IP, device fingerprint and headers.",\n'
                    '    "ADMIN_DASHBOARD_LOCKED": "Emergency lock — refuse '
                    'ALL dashboard logins (toggle via /lockdash too). '
                    'Telegram keeps working.",\n'
                )
                bs = bs[:_j] + _add + bs[_j:]
                with open(bs_path, "w", encoding="utf-8") as f:
                    f.write(bs)
                log("  r2: alert/lock vars described in /bs")
            else:
                log("  r2: ADMIN_PASS description anchor missing", "WARN")
        _oo = 'ONOFF_VARS = [\n'
        if _oo in bs and '"ADMIN_LOGIN_ALERTS"' not in bs.split(_oo, 1)[1][:600]:
            bs = bs.replace(_oo, _oo + '    "ADMIN_LOGIN_ALERTS",\n    "ADMIN_DASHBOARD_LOCKED",\n', 1)
            with open(bs_path, "w", encoding="utf-8") as f:
                f.write(bs)
            log("  r2: alert/lock vars added to On/Off settings")
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", bs_path],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            log(f"  r2: bot_settings compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r2: /bs description patch FAILED — {e}", "ERROR")

    # J-8: hold-the-download + admin logs group.
    # (a) every non-torrent aria2 download is BORN CAPPED at 1 KB/s; the
    #     size check releases it to full speed (or removes it — only a
    #     few KB ever leak, even on fast sites).
    try:
        a2d_path = os.path.join(
            WZMLX_DIR, "bot/helper/mirror_leech_utils/download_utils/aria2_download.py"
        )
        with open(a2d_path, "r", encoding="utf-8") as f:
            a2d = f.read()
        if "WZFIX hold" not in a2d:
            mark = '    a2c_opt = {"dir": dpath}\n'
            add = (
                mark
                + '    # WZFIX hold: start capped at 1 KB/s; the size check\n'
                + '    # releases it (or removes it) — bandwidth cannot leak.\n'
                + '    if not (listener.link.startswith("magnet:") or listener.link.endswith(".torrent")):\n'
                + '        a2c_opt["max-download-limit"] = "1K"\n'
            )
            if mark in a2d:
                a2d = a2d.replace(mark, add, 1)
                with open(a2d_path, "w", encoding="utf-8") as f:
                    f.write(a2d)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", a2d_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: downloads born held at 1 KB/s (check releases)")
                else:
                    log(f"  r2: hold patch compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: a2c_opt anchor not found", "WARN")
        else:
            log("  r2: hold patch already applied")
    except Exception as e:
        log(f"  r2: hold patch FAILED — {e}", "ERROR")

    # (b) callback: release the hold when allowed; when the quota blocks,
    #     show ONE clean message, delete the task status message, and put
    #     the full details in the admin logs group.
    try:
        a2l_path = os.path.join(WZMLX_DIR, "bot/helper/listeners/aria2_listener.py")
        with open(a2l_path, "r", encoding="utf-8") as f:
            a2l = f.read()
        if "WZFIX hold-release" not in a2l:
            old_cb = (
                '        mmsg = await limit_checker(task.listener)\n'
                '        if mmsg:\n'
                '            await TorrentManager.aria2_remove(download)\n'
                '            await task.listener.on_download_error(mmsg, is_limit=True)\n'
                '            return\n'
            )
            new_cb = (
                '        mmsg = await limit_checker(task.listener)\n'
                '        if mmsg:\n'
                '            if mmsg.startswith("\U0001F6AB"):  # WZFIX quota block\n'
                '                # remove the task FIRST so the WZML error\n'
                '                # handlers find nothing and stay silent — no\n'
                '                # "Download Stopped" card, no status edits\n'
                '                try:\n'
                '                    async with task_dict_lock:\n'
                '                        task_dict.pop(task.listener.mid, None)\n'
                '                except Exception:\n'
                '                    pass\n'
                '                await TorrentManager.aria2_remove(download)\n'
                '                try:\n'
                '                    from ..telegram_helper.message_utils import send_message, delete_message\n'
                '                    await send_message(task.listener.message, mmsg)\n'
                '                    await delete_message(task.listener.message)\n'
                '                except Exception:\n'
                '                    pass\n'
                '                try:\n'
                '                    from ..ext_utils.task_manager import start_from_queued\n'
                '                    await start_from_queued()\n'
                '                except Exception:\n'
                '                    pass\n'
                '                return\n'
                '            await TorrentManager.aria2_remove(download)\n'
                '            await task.listener.on_download_error(mmsg, is_limit=True)\n'
                '            return\n'
                '        try:  # WZFIX hold-release: uncap the held download\n'
                '            await TorrentManager.aria2.changeOption(gid, {"max-download-limit": "0"})\n'
                '        except Exception as _wzre:\n'
                '            LOGGER.error(f"WZFIX hold-release failed for {gid}: {_wzre}")\n'
            )
            if old_cb in a2l:
                a2l = a2l.replace(old_cb, new_cb, 1)
                with open(a2l_path, "w", encoding="utf-8") as f:
                    f.write(a2l)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", a2l_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: hold-release + clean block flow patched")
                else:
                    log(f"  r2: hold-release compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: limit_checker callback anchor not found", "WARN")
        else:
            log("  r2: hold-release already applied")
    except Exception as e:
        log(f"  r2: hold-release patch FAILED — {e}", "ERROR")

    # (c) ADMIN_LOG_CHAT: the dedicated everything-logs group (/bs var)
    try:
        cm_path = os.path.join(WZMLX_DIR, "bot/core/config_manager.py")
        with open(cm_path, "r", encoding="utf-8") as f:
            cm = f.read()
        if "ADMIN_LOG_CHAT" not in cm:
            mark = '    ADMIN_LOGIN_ALERTS = True\n'
            if mark in cm:
                cm = cm.replace(
                    mark, mark + '    ADMIN_LOG_CHAT = ""\n', 1)
                with open(cm_path, "w", encoding="utf-8") as f:
                    f.write(cm)
                log("  r2: ADMIN_LOG_CHAT config var added")
            else:
                log("  r2: ADMIN_LOGIN_ALERTS anchor missing", "WARN")
        else:
            log("  r2: ADMIN_LOG_CHAT already present")
    except Exception as e:
        log(f"  r2: ADMIN_LOG_CHAT config patch FAILED — {e}", "ERROR")

    try:
        bs_path = os.path.join(WZMLX_DIR, "bot/modules/bot_settings.py")
        with open(bs_path, "r", encoding="utf-8") as f:
            bs = f.read()
        if '"ADMIN_LOG_CHAT"' not in bs:
            mark = '    "ADMIN_LOGIN_ALERTS": "Send every dashboard login'
            if mark in bs:
                _i = bs.index(mark)
                _j = bs.index("\n", _i) + 1
                _add = (
                    '    "ADMIN_LOG_CHAT": "Dedicated group for the WZFIX '
                    'everything-log: every login (success + fail), download '
                    'start/reject/complete, dashboard actions. Add the bot '
                    'to the group with message permission and set the chat '
                    'id here. Owner DM only receives startup/stop.",\n'
                )
                bs = bs[:_j] + _add + bs[_j:]
                with open(bs_path, "w", encoding="utf-8") as f:
                    f.write(bs)
                log("  r2: ADMIN_LOG_CHAT described in /bs")
            else:
                log("  r2: ADMIN_LOGIN_ALERTS description anchor missing", "WARN")
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", bs_path],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            log(f"  r2: bot_settings compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r2: ADMIN_LOG_CHAT /bs patch FAILED — {e}", "ERROR")

    # J-9: ALLOWANCE_OWNER + STREAM_PASS in the /bs menu.
    try:
        cm_path = os.path.join(WZMLX_DIR, "bot/core/config_manager.py")
        with open(cm_path, "r", encoding="utf-8") as f:
            cm = f.read()
        if "ALLOWANCE_OWNER" not in cm:
            mark = '    ADMIN_LOG_CHAT = ""\n'
            if mark in cm:
                cm = cm.replace(
                    mark, mark + '    ALLOWANCE_OWNER = ""\n', 1)
                with open(cm_path, "w", encoding="utf-8") as f:
                    f.write(cm)
                log("  r2: ALLOWANCE_OWNER config var added")
            else:
                log("  r2: ADMIN_LOG_CHAT anchor missing in config_manager", "WARN")
        else:
            log("  r2: ALLOWANCE_OWNER already present")
    except Exception as e:
        log(f"  r2: ALLOWANCE_OWNER config patch FAILED — {e}", "ERROR")

    try:
        bs_path = os.path.join(WZMLX_DIR, "bot/modules/bot_settings.py")
        with open(bs_path, "r", encoding="utf-8") as f:
            bs = f.read()
        if '"ALLOWANCE_OWNER"' not in bs:
            mark = '    "ADMIN_DASHBOARD_LOCKED": "Emergency lock'
            if mark in bs:
                _i = bs.index(mark)
                _j = bs.index("\n", _i) + 1
                _add = (
                    '    "ALLOWANCE_OWNER": "Username shown to users in the '
                    'buy more bandwidth message (e.g. your @username or a '
                    'support account). Leave empty to use the bot owner '
                    'username automatically. The @ is added '
                    'automatically.",\n'
                )
                bs = bs[:_j] + _add + bs[_j:]
                log("  r2: ALLOWANCE_OWNER described in /bs")
            else:
                log("  r2: ADMIN_DASHBOARD_LOCKED anchor missing", "WARN")
        if '    "STREAM_PASS":' not in bs:
            mark2 = '    "SUDO_USERS": '
            if mark2 in bs:
                _i2 = bs.index(mark2)
                _add2 = (
                    '    "STREAM_PASS": "Password for the user stream '
                    'links (/stream password gate). Separate from ADMIN_PASS. '
                    'Default: 12345 if empty.",\n    '
                )
                bs = bs[:_i2] + _add2 + bs[_i2:]
                log("  r2: STREAM_PASS added to /bs menu")
            else:
                log("  r2: SUDO_USERS anchor missing", "WARN")
        if 'elif key == "ALLOWANCE_OWNER":' not in bs:
            mark3 = ('    elif key == "ADMIN_PASS":\n'
                     '        value = str(value)')
            if mark3 in bs:
                bs = bs.replace(
                    mark3,
                    mark3
                    + '\n    elif key == "ALLOWANCE_OWNER":\n'
                      '        value = str(value).strip() if value else ""',
                    1)
                log("  r2: ALLOWANCE_OWNER str-guard added")
            else:
                log("  r2: ADMIN_PASS guard anchor missing", "WARN")
        with open(bs_path, "w", encoding="utf-8") as f:
            f.write(bs)
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", bs_path],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            log(f"  r2: bot_settings compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
    except Exception as e:
        log(f"  r2: ALLOWANCE_OWNER /bs patch FAILED — {e}", "ERROR")

    # J-10: telegram + nzb downloads must pass the bandwidth check too —
    # they never touch aria2c, so the callback backstop cannot see them.
    try:
        tg_path = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/telegram_download.py",
        )
        with open(tg_path, "r", encoding="utf-8") as f:
            tg = f.read()
        if "WZFIX tg limit check" not in tg:
            mark = "        if media is not None:\n"
            add = (
                mark
                + "            # WZFIX tg limit check: telegram files never touch\n"
                + "            # aria2c, so the callback backstop cannot see them.\n"
                + "            # The exact size is known from the media itself.\n"
                + "            try:\n"
                + "                if not self._listener.size and getattr(media, \"file_size\", 0):\n"
                + "                    self._listener.size = media.file_size\n"
                + "                from ...ext_utils.task_manager import limit_checker\n"
                + "                _wz = await limit_checker(self._listener)\n"
                + "                if _wz:\n"
                + "                    if _wz.startswith(\"\\U0001F6AB\"):\n"
                + "                        from contextlib import suppress as _wzsup\n"
                + "                        from ...telegram_helper.message_utils import send_message\n"
                + "                        with _wzsup(Exception):\n"
                + "                            await send_message(self._listener.message, _wz)\n"
                + "                        return\n"
                + "                    await self._listener.on_download_error(_wz, is_limit=True)\n"
                + "                    return\n"
                + "            except Exception as _wz_e:\n"
                + "                LOGGER.error(f\"WZFIX tg limit check failed (allowed): {_wz_e}\")\n"
            )
            if mark in tg:
                tg = tg.replace(mark, add, 1)
                with open(tg_path, "w", encoding="utf-8") as f:
                    f.write(tg)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", tg_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: telegram downloads now bandwidth-checked")
                else:
                    log(f"  r2: tg patch compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: telegram anchor not found", "WARN")
        else:
            log("  r2: tg limit check already applied")
    except Exception as e:
        log(f"  r2: tg patch FAILED — {e}", "ERROR")

    try:
        nzb_path = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/nzb_downloader.py",
        )
        with open(nzb_path, "r", encoding="utf-8") as f:
            nz = f.read()
        if "WZFIX nzb limit check" not in nz:
            mark = "    use_par2_lock = listener.extract and sab_par2_lock.throttled\n"
            add = (
                "    # WZFIX nzb limit check: nzb never touches aria2c\n"
                "    try:\n"
                "        from ...ext_utils.task_manager import limit_checker\n"
                "        _wz = await limit_checker(listener)\n"
                "        if _wz:\n"
                "            if _wz.startswith(\"\\U0001F6AB\"):\n"
                "                from contextlib import suppress as _wzsup\n"
                "                from ...telegram_helper.message_utils import send_message\n"
                "                with _wzsup(Exception):\n"
                "                    await send_message(listener.message, _wz)\n"
                "                return\n"
                "            await listener.on_download_error(_wz, is_limit=True)\n"
                "            return\n"
                "    except Exception as _wz_e:\n"
                "        LOGGER.error(f\"WZFIX nzb limit check failed (allowed): {_wz_e}\")\n"
                + mark
            )
            if mark in nz:
                nz = nz.replace(mark, add, 1)
                with open(nzb_path, "w", encoding="utf-8") as f:
                    f.write(nz)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", nzb_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: nzb downloads now bandwidth-checked")
                else:
                    log(f"  r2: nzb patch compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: nzb anchor not found", "WARN")
        else:
            log("  r2: nzb limit check already applied")
    except Exception as e:
        log(f"  r2: nzb patch FAILED — {e}", "ERROR")

    # J-11: qbit torrents get the same treatment as aria2 — born held at
    # 1 KB/s until the size check releases or removes them.
    try:
        qb_path = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/qbit_download.py",
        )
        with open(qb_path, "r", encoding="utf-8") as f:
            qb = f.read()
        if "WZFIX qbit hold" not in qb:
            mark = "        tor_info = tor_info[0]\n        listener.name = tor_info.name\n"
            add = (
                "        tor_info = tor_info[0]\n"
                "        # WZFIX qbit hold: cap the torrent at 1 KB/s until the\n"
                "        # size check releases it (or removes it) — bandwidth cannot leak\n"
                "        try:\n"
                "            await TorrentManager.qbittorrent.torrents.set_download_limit(\n"
                "                [tor_info.hash], 1024\n"
                "            )\n"
                "        except Exception as _wz_e:\n"
                "            LOGGER.error(f\"WZFIX qbit hold failed: {_wz_e}\")\n"
                "        listener.name = tor_info.name\n"
            )
            if mark in qb:
                qb = qb.replace(mark, add, 1)
                with open(qb_path, "w", encoding="utf-8") as f:
                    f.write(qb)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", qb_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: qbit torrents born held at 1 KB/s")
                else:
                    log(f"  r2: qbit hold compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: qbit add anchor not found", "WARN")
        else:
            log("  r2: qbit hold already applied")
    except Exception as e:
        log(f"  r2: qbit hold patch FAILED — {e}", "ERROR")

    try:
        ql_path = os.path.join(WZMLX_DIR, "bot/helper/listeners/qbit_listener.py")
        with open(ql_path, "r", encoding="utf-8") as f:
            ql = f.read()
        if "WZFIX qbit hold-release" not in ql:
            old_sc = (
                "        task.listener.size = tor.size\n"
                "        mmsg = await limit_checker(task.listener)\n"
                "        if mmsg:\n"
                "            await _on_download_error(mmsg, tor, is_limit=True)\n"
            )
            new_sc = (
                "        task.listener.size = tor.size\n"
                "        mmsg = await limit_checker(task.listener)\n"
                "        if mmsg:\n"
                "            if mmsg.startswith(\"\\U0001F6AB\"):  # WZFIX qbit hold-release: quota block\n"
                "                # silent removal — pop the task FIRST so the\n"
                "                # WZML error card never fires, then ONE message\n"
                "                from contextlib import suppress as _wzsup\n"
                "                try:\n"
                "                    async with task_dict_lock:\n"
                "                        task_dict.pop(task.listener.mid, None)\n"
                "                except Exception:\n"
                "                    pass\n"
                "                with _wzsup(Exception):\n"
                "                    from ..telegram_helper.message_utils import send_message, delete_message\n"
                "                    await send_message(task.listener.message, mmsg)\n"
                "                with _wzsup(Exception):\n"
                "                    await delete_message(task.listener.message)\n"
                "                with _wzsup(Exception):\n"
                "                    await TorrentManager.qbittorrent.torrents.stop([tor.hash])\n"
                "                    await sleep(0.3)\n"
                "                    await _remove_torrent(tor.hash, tor.tags[0])\n"
                "                with _wzsup(Exception):\n"
                "                    from ..ext_utils.task_manager import start_from_queued\n"
                "                    await start_from_queued()\n"
                "                return\n"
                "            await _on_download_error(mmsg, tor, is_limit=True)\n"
                "            return\n"
                "        # WZFIX qbit hold-release: uncap the checked torrent\n"
                "        try:\n"
                "            await TorrentManager.qbittorrent.torrents.set_download_limit(\n"
                "                [tor.hash], 0\n"
                "            )\n"
                "        except Exception as _wzre:\n"
                "            LOGGER.error(f\"WZFIX qbit hold-release failed for {tor.hash}: {_wzre}\")\n"
            )
            if old_sc in ql:
                ql = ql.replace(old_sc, new_sc, 1)
                with open(ql_path, "w", encoding="utf-8") as f:
                    f.write(ql)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", ql_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: qbit hold-release + clean block flow patched")
                else:
                    log(f"  r2: qbit listener compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: qbit _size_check anchor not found", "WARN")
        else:
            log("  r2: qbit hold-release already applied")
    except Exception as e:
        log(f"  r2: qbit listener patch FAILED — {e}", "ERROR")

    # J-12: owner/sudo tasks never reach the quota hook (the sudo branch
    # above it returns early), so the exempt log must live INSIDE that
    # branch — otherwise exempt tasks are invisible in the logs group.
    try:
        tm_path = os.path.join(WZMLX_DIR, "bot/helper/ext_utils/task_manager.py")
        with open(tm_path, "r", encoding="utf-8") as f:
            tm = f.read()
        if "WZFIX exempt task log" not in tm:
            mark = '    if await CustomFilters.sudo("", message):\n'
            add = (
                mark
                + '        # WZFIX exempt task log: owner/sudo never reach the\n'
                + '        # quota hook below, so log the exemption right here\n'
                + '        try:\n'
                + '            from ..wzfix.r1_core import admin_log\n'
                + '\n'
                + '            _fu = getattr(message, "from_user", None) or getattr(\n'
                + '                message, "sender_chat", None\n'
                + '            )\n'
                + '            _ch = getattr(message, "chat", None)\n'
                + '            _ct = str(getattr(_ch, "type", ""))\n'
                + '            try:\n'
                + '                _ct = _ct.split(".")[-1]\n'
                + '            except Exception:\n'
                + '                pass\n'
                + '            _cn = str(\n'
                + '                getattr(_ch, "title", None)\n'
                + '                or getattr(_ch, "username", None)\n'
                + '                or ""\n'
                + '            )\n'
                + '            _who = f"<code>{_fu.id if _fu else 0}</code>"\n'
                + '            _un = getattr(_fu, "username", None) if _fu else None\n'
                + '            if _un:\n'
                + '                _who += f" (@{_un})"\n'
                + '            await admin_log(\n'
                + '                "\\U0001F451 <b>Exempt task — no quota check</b>",\n'
                + '                f"┏ <b>User</b> → {_who}\\n"\n'
                + '                f"┠ <b>Where</b> → {_ct} {_cn[:60]}\\n"\n'
                + '                f"┖ Owner/sudo are exempt by design",\n'
                + '            )\n'
                + '        except Exception:\n'
                + '            pass\n'
            )
            if mark in tm:
                tm = tm.replace(mark, add, 1)
                with open(tm_path, "w", encoding="utf-8") as f:
                    f.write(tm)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", tm_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: exempt tasks now logged in the sudo branch")
                else:
                    log(f"  r2: exempt log compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: sudo branch anchor not found", "WARN")
        else:
            log("  r2: exempt task log already applied")
    except Exception as e:
        log(f"  r2: exempt log patch FAILED — {e}", "ERROR")

    # J-15: uploads stuck forever + zombie tasks blocking the queue.
    # Telegram FloodWait makes the upload retry loops sleep for HOURS
    # (task never dies) -> the zombie counts against QUEUE_ALL=3 ->
    # every new task silently queues forever -> "bot not accepting
    # files". Cap the flood sleep: give up after 900s with a loud error.
    try:
        tu_path = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/upload_utils/telegram_uploader.py",
        )
        with open(tu_path, "r", encoding="utf-8") as f:
            tu = f.read()
        if "WZFIX flood cap" not in tu:
            old_tu = (
                "        except (FloodWait, FloodPremiumWait) as f:\n"
                "            LOGGER.warning(f\"FloodWait {f.value}s, retrying {method.__name__}\")\n"
                "            await sleep(f.value + 1)\n"
            )
            new_tu = (
                "        except (FloodWait, FloodPremiumWait) as f:\n"
                "            if f.value > 900:  # WZFIX flood cap\n"
                "                LOGGER.error(\n"
                "                    f\"WZFIX flood cap: FloodWait {f.value}s too long, aborting {method.__name__}\"\n"
                "                )\n"
                "                raise\n"
                "            LOGGER.warning(f\"FloodWait {f.value}s, retrying {method.__name__}\")\n"
                "            await sleep(f.value + 1)\n"
            )
            if old_tu in tu:
                tu = tu.replace(old_tu, new_tu, 1)
                with open(tu_path, "w", encoding="utf-8") as f:
                    f.write(tu)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", tu_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: upload flood-wait capped at 900s")
                else:
                    log(f"  r2: uploader patch compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: uploader flood anchor not found", "WARN")
        else:
            log("  r2: uploader flood cap already applied")

        hu_path = os.path.join(WZMLX_DIR, "bot/helper/ext_utils/hyperul_utils.py")
        with open(hu_path, "r", encoding="utf-8") as f:
            hu = f.read()
        if "WZFIX flood cap" not in hu:
            old_hu = (
                "            except (FloodWait, FloodPremiumWait) as f:\n"
                "                LOGGER.warning(f\"HypertgUL flood {f.value}s on {self._up_file}\")\n"
                "                await sleep(f.value + 1)\n"
            )
            new_hu = (
                "            except (FloodWait, FloodPremiumWait) as f:\n"
                "                if f.value > 900:  # WZFIX flood cap\n"
                "                    LOGGER.error(\n"
                "                        f\"WZFIX flood cap: FloodWait {f.value}s too long, aborting {self._up_file}\"\n"
                "                    )\n"
                "                    raise\n"
                "                LOGGER.warning(f\"HypertgUL flood {f.value}s on {self._up_file}\")\n"
                "                await sleep(f.value + 1)\n"
            )
            if old_hu in hu:
                hu = hu.replace(old_hu, new_hu, 1)
                with open(hu_path, "w", encoding="utf-8") as f:
                    f.write(hu)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", hu_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: hyper upload flood-wait capped at 900s")
                else:
                    log(f"  r2: hyperul patch compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: hyperul flood anchor not found", "WARN")
        else:
            log("  r2: hyperul flood cap already applied")
    except Exception as e:
        log(f"  r2: flood cap patch FAILED — {e}", "ERROR")

    # J-16: /log — send only the log TAIL as the document (the full file
    # can take 10-15s+ to upload), tail-read the disp/web views instead of
    # loading the whole file into memory, and move the blocking paste
    # request into a thread so it stops freezing the whole bot.
    try:
        sv_path = os.path.join(WZMLX_DIR, "bot/modules/services.py")
        with open(sv_path, "r", encoding="utf-8") as f:
            sv = f.read()
        if "WZFIX log tail" not in sv:
            old_sv1 = (
                "    await send_file(message, \"log.txt\", buttons=buttons.build_menu(2))\n"
            )
            new_sv1 = (
                "    # WZFIX log tail: the full log.txt can be huge and takes\n"
                "    # 10-15s+ to upload — send only the last 500KB.\n"
                "    from os import stat as _wzst, path as _wzpp\n"
                "\n"
                "    _wzsend = \"log.txt\"\n"
                "    try:\n"
                "        if _wzpp.exists(\"log.txt\") and _wzst(\"log.txt\").st_size > 500_000:\n"
                "            async with aiopen(\"log.txt\", \"rb\") as _wzf:\n"
                "                await _wzf.seek(_wzst(\"log.txt\").st_size - 500_000)\n"
                "                _wzdata = await _wzf.read()\n"
                "            with open(\"wzlog_tail.txt\", \"wb\") as _wzw:\n"
                "                _wzw.write(_wzdata)\n"
                "            _wzsend = \"wzlog_tail.txt\"\n"
                "    except Exception:\n"
                "        _wzsend = \"log.txt\"\n"
                "    await send_file(message, _wzsend, buttons=buttons.build_menu(2))\n"
            )
            ok1 = old_sv1 in sv
            if ok1:
                sv = sv.replace(old_sv1, new_sv1, 1)

            old_sv2 = (
                "        async with aiopen(\"log.txt\", \"r\") as f:\n"
                "            content = await f.read()\n"
                "\n"
                "        def parse(line):\n"
            )
            new_sv2 = (
                "        from os import stat as _wzst2, path as _wzpp2\n"
                "\n"
                "        _wzsz = _wzst2(\"log.txt\").st_size if _wzpp2.exists(\"log.txt\") else 0\n"
                "        async with aiopen(\"log.txt\", \"r\") as f:\n"
                "            if _wzsz > 200_000:\n"
                "                await f.seek(_wzsz - 200_000)\n"
                "                await f.readline()\n"
                "            content = await f.read()\n"
                "\n"
                "        def parse(line):\n"
            )
            ok2 = old_sv2 in sv
            if ok2:
                sv = sv.replace(old_sv2, new_sv2, 1)

            old_sv3 = (
                "        async with aiopen(\"log.txt\", \"r\") as f:\n"
                "            content = await f.read()\n"
                "\n"
                "        data = (\n"
            )
            new_sv3 = (
                "        from os import stat as _wzst3, path as _wzpp3\n"
                "\n"
                "        _wzsz3 = _wzst3(\"log.txt\").st_size if _wzpp3.exists(\"log.txt\") else 0\n"
                "        async with aiopen(\"log.txt\", \"r\") as f:\n"
                "            if _wzsz3 > 100_000:\n"
                "                await f.seek(_wzsz3 - 100_000)\n"
                "                await f.readline()\n"
                "            content = await f.read()\n"
                "\n"
                "        data = (\n"
            )
            ok3 = old_sv3 in sv
            if ok3:
                sv = sv.replace(old_sv3, new_sv3, 1)

            old_sv4 = (
                "        resp = cget(\"POST\", \"https://spaceb.in/\", headers=headers, data=data)\n"
            )
            new_sv4 = (
                "        # WZFIX: this HTTP call is BLOCKING — it freezes the\n"
                "        # whole bot for its whole duration. Run it in a thread.\n"
                "        import asyncio as _wzaio\n"
                "\n"
                "        resp = await _wzaio.get_running_loop().run_in_executor(\n"
                "            None,\n"
                "            lambda: cget(\"POST\", \"https://spaceb.in/\", headers=headers, data=data),\n"
                "        )\n"
            )
            ok4 = old_sv4 in sv
            if ok4:
                sv = sv.replace(old_sv4, new_sv4, 1)

            if ok1 and ok2 and ok3 and ok4:
                with open(sv_path, "w", encoding="utf-8") as f:
                    f.write(sv)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", sv_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: /log sends tail doc, paste no longer blocks")
                else:
                    log(f"  r2: services patch compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log(
                    f"  r2: services anchors missing (tail={ok1} disp={ok2} web={ok3} post={ok4})",
                    "WARN",
                )
        else:
            log("  r2: /log tail already applied")
    except Exception as e:
        log(f"  r2: /log patch FAILED — {e}", "ERROR")

    # J-19: the three task-launch call sites must recognise the sentinel
    # "__WZFIX_HANDLED__" returned by pre_task_check when precheck has
    # already replied in-place (checking message edited into the block
    # message) — abort the task without sending anything further.
    try:
        for _mod_rel in (
            "bot/modules/mirror_leech.py",
            "bot/modules/ytdlp.py",
            "bot/modules/clone.py",
        ):
            _mp = os.path.join(WZMLX_DIR, _mod_rel)
            with open(_mp, "r", encoding="utf-8") as f:
                _m = f.read()
            _mb = os.path.basename(_mod_rel)
            if "__WZFIX_HANDLED__" in _m:
                log(f"  r2: {_mb} sentinel already applied")
                continue
            _old_m = (
                "        check_msg, check_button = await pre_task_check(self.message)\n"
                "        if check_msg:\n"
            )
            _new_m = (
                "        check_msg, check_button = await pre_task_check(self.message)\n"
                "        if check_msg == \"__WZFIX_HANDLED__\":\n"
                "            # WZFIX: the allowance check already replied in-place\n"
                "            # (checking message edited into the block message) —\n"
                "            # abort the task without sending anything further\n"
                "            await delete_links(self.message)\n"
                "            return\n"
                "        if check_msg:\n"
            )
            if _old_m in _m:
                _m = _m.replace(_old_m, _new_m, 1)
                with open(_mp, "w", encoding="utf-8") as f:
                    f.write(_m)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _mp],
                    capture_output=True, text=True, timeout=60,
                )
                if _r.returncode == 0:
                    log(f"  r2: {_mb} recognises the WZFIX sentinel")
                else:
                    log(f"  r2: {_mb} patch compile FAILED — {(_r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log(f"  r2: {_mb} anchor not found", "WARN")
    except Exception as e:
        log(f"  r2: J-19 patch FAILED — {e}", "ERROR")

    # J-21: user-stored ytdl options often carry extractor_args like
    # player_client: "web_safari,web_embedded,-tv_downgraded" as ONE
    # comma-string — yt-dlp treats the whole thing as a single client
    # name, warns "Skipping unsupported client" and silently falls back
    # to defaults. Split comma strings into a proper list at BOTH places
    # options are built so the intended clients actually get used.
    try:
        _wz_pc_fix = (
            "        # WZFIX player_client sanitize\n"
            "        try:\n"
            "            _ea = options.get(\"extractor_args\") or {}\n"
            "            _yt = _ea.get(\"youtube\") or {}\n"
            "            _pc = _yt.get(\"player_client\")\n"
            "            if isinstance(_pc, (str, list)):\n"
            "                _parts = _pc if isinstance(_pc, list) else [_pc]\n"
            "                _split = []\n"
            "                for _c in _parts:\n"
            "                    _split.extend(\n"
            "                        [p.strip() for p in str(_c).split(\",\") if p.strip()]\n"
            "                    )\n"
            "                if _split:\n"
            "                    _yt[\"player_client\"] = _split\n"
            "                    _ea[\"youtube\"] = _yt\n"
            "                    options[\"extractor_args\"] = _ea\n"
            "        except Exception:\n"
            "            pass\n"
        )
        for _rel, _anchor in (
            (
                "bot/modules/ytdlp.py",
                '        options["playlist_items"] = "0"\n',
            ),
            (
                "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py",
                "    async def add_download(self, path, qual, playlist, options):\n",
            ),
        ):
            _fp = os.path.join(WZMLX_DIR, _rel)
            with open(_fp, "r", encoding="utf-8") as f:
                _t = f.read()
            if "WZFIX player_client" in _t:
                log(f"  r2: {os.path.basename(_rel)} player_client already sanitized")
                continue
            if _anchor in _t:
                _t = _t.replace(_anchor, _anchor + _wz_pc_fix, 1)
                with open(_fp, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _fp],
                    capture_output=True, text=True, timeout=60,
                )
                if _r.returncode == 0:
                    log(f"  r2: {os.path.basename(_rel)} player_client sanitized")
                else:
                    log(f"  r2: {os.path.basename(_rel)} sanitize FAILED — {(_r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log(f"  r2: {os.path.basename(_rel)} anchor not found", "WARN")
    except Exception as e:
        log(f"  r2: J-21 patch FAILED — {e}", "ERROR")

    # J-22: music integrations (v15.32) — Spotify / JioSaavn / Apple
    # Music links resolve to ytsearch queries and route through the
    # yt-dlp engine. Fails open on every error.
    try:
        _j22_hook_yt = (
            "        # WZFIX music integration (v15.32): Spotify / JioSaavn /\n"
            "        # Apple Music links become ytsearch queries before parsing\n"
            "        from ..helper.wzfix.r3_music import pre_resolve\n"
            "        if await pre_resolve(self.message, self.client,\n"
            "                             is_ytdl=True, is_leech=self.is_leech):\n"
            "            return\n"
        )
        _j22_hook_ml = (
            "        # WZFIX music integration (v15.32): Spotify / JioSaavn /\n"
            "        # Apple Music links are resolved and re-dispatched through\n"
            "        # the yt-dlp engine — quota, checks and logs all apply\n"
            "        from ..helper.wzfix.r3_music import pre_resolve\n"
            "        if await pre_resolve(self.message, self.client,\n"
            "                             is_leech=self.is_leech):\n"
            "            return\n"
        )
        for _rel, _hook, _anchor, _prefix in (
            (
                "bot/modules/ytdlp.py",
                _j22_hook_yt,
                '        text = self.message.text.split("\\n")\n',
                '    async def new_event(self):\n',
            ),
            (
                "bot/modules/mirror_leech.py",
                _j22_hook_ml,
                '        text = self.message.text.split("\\n")\n',
                "",
            ),
        ):
            _fp = os.path.join(WZMLX_DIR, _rel)
            with open(_fp, "r", encoding="utf-8") as f:
                _t = f.read()
            if "WZFIX music integration" in _t:
                log(f"  r2: {os.path.basename(_rel)} music hook already applied")
                continue
            if _anchor in _t:
                # ytdlp: the anchor line must directly follow the def line;
                # mirror: the anchor sits after the enable/disable checks
                _old = (_prefix + _anchor) if _prefix else _anchor
                _new = (_prefix + _hook + _anchor) if _prefix else (_hook + _anchor)
                if _old in _t:
                    _t = _t.replace(_old, _new, 1)
                else:
                    _t = _t.replace(_anchor, _new, 1)
                with open(_fp, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _fp],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r.returncode == 0:
                    log(f"  r2: {os.path.basename(_rel)} music hook applied")
                else:
                    log(f"  r2: {os.path.basename(_rel)} music hook FAILED — {(_r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log(f"  r2: {os.path.basename(_rel)} music hook anchor missing", "WARN")
    except Exception as e:
        log(f"  r2: J-22 patch FAILED — {e}", "ERROR")

    # J-23: music searches (v15.36) — ytsearch queries are not URLs;
    # the is_url gate in ytdlp.py must accept them
    try:
        _p = os.path.join(WZMLX_DIR, "bot/modules/ytdlp.py")
        with open(_p, "r", encoding="utf-8") as f:
            _t = f.read()
        if "WZFIX ytsearch gate" not in _t:
            _old = "        if not is_url(self.link):\n"
            _new = (
                "        # WZFIX ytsearch gate: music resolutions rewrite the\n"
                "        # link to a ytsearch query, which is not a URL\n"
                "        if not (is_url(self.link) or\n"
                '                self.link.startswith("ytsearch")):\n'
            )
            if _old in _t:
                _t = _t.replace(_old, _new, 1)
                with open(_p, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r.returncode == 0:
                    log("  r2: ytdlp.py ytsearch gate applied")
                else:
                    log(f"  r2: ytdlp.py ytsearch gate FAILED — {(_r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: ytdlp.py is_url anchor missing", "WARN")
        else:
            log("  r2: ytdlp.py ytsearch gate already applied")
    except Exception as e:
        log(f"  r2: J-23 patch FAILED — {e}", "ERROR")

    # J-30: quiet music fan-out (v15.82) — artist batches keep the chat
    # clean: no per-song "Downloaded! Waiting..." or per-song error
    # messages (the artist note tracks progress), a compact file list
    # with one line per song, and the uploaded songs collected for the
    # playlist prompt
    try:
        _p30 = os.path.join(
            WZMLX_DIR, "bot/helper/listeners/task_listener.py"
        )
        with open(_p30, "r", encoding="utf-8") as f:
            _t30 = f.read()
        if "WZFIX r11 quiet fan-out" not in _t30:
            _old30a = (
                '        await send_message(self.message, '
                'f"{self.tag} {escape(str(error))}")'
            )
            _new30a = r'''        # WZFIX r11 quiet fan-out (v15.82): artist batches track
        # progress in one note — no per-song messages
        _wzfix_quiet = False
        try:
            _wzfix_quiet = bool(
                (self.same_dir or {})
                .get(self.folder_name, {})
                .get("_wzfix")
            )
        except Exception:
            pass
        if not _wzfix_quiet:
            await send_message(
                self.message, f"{self.tag} {escape(str(error))}"
            )'''
            _old30b = (
                '        await send_message(self.message, msg, button)'
            )
            _new30b = r'''        # WZFIX r11 quiet fan-out (v15.82): failed songs are
        # folded into the artist note
        _wzfix_quiet = False
        try:
            _wzfix_sd = (self.same_dir or {}).get(self.folder_name) or {}
            if _wzfix_sd.get("_wzfix") is not None:
                _wzfix_quiet = True
                # WZFIX r15: a failed song still counts as "done" so
                # the leader upload can trigger without it
                _wzfix_sd["_wzfix"].setdefault("done", set()).add(
                    self.mid
                )
                _wzfix_sd["_wzfix"].setdefault("failed", []).append(
                    str(
                        getattr(
                            self.message, "_wzfix_music_title", ""
                        )
                        or self.name
                        or "a song"
                    )[:80]
                )
        except Exception:
            pass
        if not _wzfix_quiet:
            await send_message(self.message, msg, button)'''
            _old30c = "                    if Config.MEDIA_STORE and ("
            _new30c = r'''                    # WZFIX r11 quiet fan-out (v15.82): one compact
                    # line per song, and every uploaded song recorded
                    # for the playlist prompt
                    try:
                        _wzfix_music = bool(
                            (self.same_dir or {})
                            .get(self.folder_name, {})
                            .get("_wzfix")
                        )
                    except Exception:
                        _wzfix_music = False
                    if _wzfix_music:
                        try:
                            _parts30 = link.split("/")[-2:]
                            if len(_parts30) == 2:
                                _cid, _mid30 = _parts30
                                if _cid.isdigit():
                                    _cid = f"-100{_cid}"
                                from ...modules.stream import (
                                    gen_stream_link as _gsl30,
                                )

                                _sl30 = await _gsl30(_cid, _mid30)
                                if _sl30:
                                    fmsg += (
                                        f" ┖ <a href='{_sl30[0]}'>Stream</a>"
                                        f" | <a href='{_sl30[1]}'>Download</a>"
                                    )
                                try:
                                    (self.same_dir or {})[
                                        self.folder_name
                                    ]["_wzfix"].setdefault("pl", []).append(
                                        (_cid, _mid30)
                                    )
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        fmsg += "\n"
                        if len(fmsg.encode() + msg.encode()) > 4000:
                            await send_message(log_chat, msg + fmsg)
                            await sleep(1)
                            fmsg = ""
                        continue
                    if Config.MEDIA_STORE and ('''
            _ok30 = True
            for _o30, _n30 in (
                (_old30a, _new30a),
                (_old30b, _new30b),
                (_old30c, _new30c),
            ):
                if _o30 in _t30:
                    _t30 = _t30.replace(_o30, _n30, 1)
                else:
                    log(f"  r2: J-30 anchor missing: {_o30[:50]!r}", "WARN")
                    _ok30 = False
            if _ok30:
                with open(_p30, "w", encoding="utf-8") as f:
                    f.write(_t30)
                _r30 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p30],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r30.returncode == 0:
                    log("  r2: J-30 quiet fan-out applied")
                else:
                    log(
                        f"  r2: J-30 FAILED — "
                        f"{(_r30.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-30 anchors missing — NOT applied", "ERROR")
        else:
            log("  r2: J-30 quiet fan-out already applied")
    except Exception as e:
        log(f"  r2: J-30 patch FAILED — {e}", "ERROR")

    # J-24: ytsearch queries must not be mistaken for rclone remote
    # paths (ytsearch:Artist - Song audio looks like remote:path)
    try:
        _lp = os.path.join(WZMLX_DIR, "bot/helper/ext_utils/links_utils.py")
        with open(_lp, "r", encoding="utf-8") as f:
            _t = f.read()
        if "(?!(magnet:|mtp:|sa:|tp:|ytsearch))" in _t:
            log("  r2: links_utils.py ytsearch rclone exclude already applied")
        elif "(?!(magnet:|mtp:|sa:|tp:))" in _t:
            _old = "(?!(magnet:|mtp:|sa:|tp:))"
            _new = "(?!(magnet:|mtp:|sa:|tp:|ytsearch))"
            if _old in _t:
                _t = _t.replace(_old, _new, 1)
                with open(_lp, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _lp],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r.returncode == 0:
                    log("  r2: links_utils.py ytsearch rclone exclude applied")
                else:
                    log(f"  r2: links_utils.py rclone exclude FAILED — {(_r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: links_utils.py rclone anchor missing", "WARN")
        else:
            log("  r2: links_utils.py ytsearch rclone exclude already applied")
    except Exception as e:
        log(f"  r2: J-24 patch FAILED — {e}", "ERROR")

    # J-25: music best-pick (v15.38) — track searches fetch the top 5
    # and skip shorts/teasers so a full-length version is downloaded
    try:
        _p = os.path.join(WZMLX_DIR, "bot/modules/ytdlp.py")
        with open(_p, "r", encoding="utf-8") as f:
            _t = f.read()
        if "WZFIX music best-pick" not in _t:
            _old_a = r'''        options["playlist_items"] = "0"
'''
            _new_a = r'''        options["playlist_items"] = "0"
        # WZFIX music best-pick (v15.38): track searches need the
        # top 5 entries so the best-pick step can skip shorts/teasers
        if str(self.link).startswith("ytsearch"):
            options["playlist_items"] = "1:5"
'''
            _old_b = r'''        finally:
            await self.run_multi(input_list, YtDlp)

        if not qual:
'''
            _new_b = r'''        finally:
            await self.run_multi(input_list, YtDlp)

        # WZFIX music best-pick (v15.38): from the top 5 results skip
        # shorts/teasers (under 75 s or teaser-titled) and download the
        # first full-length take as a single file — acoustic/unplugged
        # versions pass; the flag enables SponsorBlock in the downloader
        if str(self.link).startswith("ytsearch") or getattr(
            self.message, "_wzfix_ld", False
        ):
            self._wzfix_music = True
            self.name = (
                getattr(self.message, "_wzfix_music_title", "") or self.name
            )
            if not self.select:
                qual = "ba[ext=m4a]/ba/b-m4a-5"
            if str(self.link).startswith("ytsearch"):
                try:
                    _ents = [e for e in (result.get("entries") or []) if e]
                    _pick = None
                    _fb = None
                    for _e in _ents:
                        _d = _e.get("duration") or 0
                        _tt = str(_e.get("title") or "").lower()
                        if _fb is None and _d and _d <= 900:
                            _fb = _e
                        if 75 <= _d <= 900 and not any(
                            w in _tt for w in ("teaser", "trailer", "promo", "snippet")
                        ):
                            _pick = _e
                            break
                    if _pick is None:
                        _pick = _fb or (_ents[0] if _ents else None)
                    if _pick is not None:
                        _u = _pick.get("webpage_url") or _pick.get("url")
                        if _u is None and _pick.get("id"):
                            _u = "https://www.youtube.com/watch?v=" + str(_pick["id"])
                        if _u:
                            self.link = _u
                            result = _pick
                except Exception:
                    pass

        if not qual:
'''
            _ok = True
            if _old_a in _t:
                _t = _t.replace(_old_a, _new_a, 1)
            else:
                log("  r2: ytdlp.py playlist_items anchor missing", "WARN")
                _ok = False
            if _old_b in _t:
                _t = _t.replace(_old_b, _new_b, 1)
            else:
                log("  r2: ytdlp.py best-pick anchor missing", "WARN")
                _ok = False
            if _ok:
                with open(_p, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r.returncode == 0:
                    log("  r2: ytdlp.py music best-pick applied")
                else:
                    log(f"  r2: ytdlp.py music best-pick FAILED — {(_r.stderr or '').strip()[:200]}", "ERROR")
        else:
            log("  r2: ytdlp.py music best-pick already applied")
    except Exception as e:
        log(f"  r2: J-25 patch FAILED — {e}", "ERROR")

    # J-26: music quality (v15.38) — SponsorBlock (the crowd-sourced
    # ad/intro skipper) + shorts/teaser skipping inside music playlists
    try:
        _p = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py",
        )
        with open(_p, "r", encoding="utf-8") as f:
            _t = f.read()
        if "sponsorblock_remove" not in _t:
            _old = r'''        if playlist:
            self.opts["ignoreerrors"] = True
            self.is_playlist = True
'''
            _new = r'''        # WZFIX music quality (v15.38): SponsorBlock — the crowd-sourced
        # ad/intro skipper — plus shorts/teaser skipping inside music
        # playlists (artist/album searches)
        try:
            if getattr(self._listener, "_wzfix_music", False) or str(
                self._listener.link
            ).startswith("ytsearch"):
                options.setdefault(
                    "sponsorblock_remove",
                    ["sponsor", "selfpromo", "intro", "preview", "music_offtopic"],
                )
                if playlist:

                    def _wzfix_music_filter(info, *, incomplete=False):
                        if incomplete:
                            return None
                        _d = info.get("duration")
                        if _d is not None and _d < 75:
                            return "WZFIX: skipping short/teaser"
                        return None

                    options["match_filter"] = _wzfix_music_filter
        except Exception:
            pass
        if playlist:
            self.opts["ignoreerrors"] = True
            self.is_playlist = True
'''
            if _old in _t:
                _t = _t.replace(_old, _new, 1)
                with open(_p, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r.returncode == 0:
                    log("  r2: yt_dlp_download.py SponsorBlock + shorts filter applied")
                else:
                    log(f"  r2: yt_dlp_download.py SponsorBlock FAILED — {(_r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: yt_dlp_download.py playlist anchor missing", "WARN")
        else:
            log("  r2: yt_dlp_download.py SponsorBlock already applied")
    except Exception as e:
        log(f"  r2: J-26 patch FAILED — {e}", "ERROR")
    # J-27: music gate (v15.42) — a raw music link must never reach
    # yt-dlp's DRM refusal; resolve or stop cleanly instead
    try:
        _fp = os.path.join(WZMLX_DIR, "bot/modules/ytdlp.py")
        with open(_fp, "r", encoding="utf-8") as f:
            _t = f.read()
        if "WZFIX music gate" in _t:
            log("  r2: ytdlp.py music gate already applied")
        else:
            _anchor = '        if "mdisk.me" in self.link:'
            _gate = (
                "        # WZFIX music gate (v15.42): a raw music link must\n"
                "        # never reach yt-dlp's DRM refusal — resolve or stop\n"
                "        try:\n"
                "            from ..helper.wzfix.r3_music import (\n"
                "                is_music_url,\n"
                "                pre_resolve,\n"
                "            )\n"
                "\n"
                "            if is_music_url(self.link):\n"
                "                if await pre_resolve(\n"
                "                    self.message, self.client,\n"
                "                    is_ytdl=True, is_leech=self.is_leech\n"
                "                ):\n"
                "                    return\n"
                "                _rt = self.message.reply_to_message\n"
                "                if _rt is not None and _rt.text:\n"
                "                    self.link = _rt.text.split(\"\\n\", 1)[0].strip()\n"
                "                elif self.message.text:\n"
                "                    _tk = self.message.text.split(\"\\n\")[0].split(\" \")\n"
                "                    self.link = \" \".join(_tk[1:]) if len(_tk) > 1 else \"\"\n"
                "                if not (self.link and self.link.startswith(\"ytsearch\")):\n"
                "                    await send_message(\n"
                "                        self.message,\n"
                "                        \"⚠️ Music link couldn't be resolved - send /yl <link> again\",\n"
                "                    )\n"
                "                    await self.remove_from_same_dir()\n"
                "                    await delete_links(self.message)\n"
                "                    return\n"
                "        except Exception:\n"
                "            pass\n"
                "\n"
            )
            if _anchor in _t:
                _t = _t.replace(_anchor, _gate + _anchor, 1)
                with open(_fp, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _fp],
                    capture_output=True, text=True, timeout=60,
                )
                if _r.returncode == 0:
                    log("  r2: ytdlp.py music gate applied (v15.42)")
                else:
                    log(
                        f"  r2: ytdlp.py music gate FAILED — "
                        f"{(_r.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: ytdlp.py music gate anchor missing", "WARN")
    except Exception as e:
        log(f"  r2: J-27 patch FAILED — {e}", "ERROR")
    # J-28: music commands (v15.43) — /dl /d /y aliases and /ld lyrics
    # search with yes/no confirmation, registered into the bot handlers
    try:
        _hd = os.path.join(WZMLX_DIR, "bot/core/handlers.py")
        with open(_hd, "r", encoding="utf-8") as f:
            _t = f.read()
        if "WZFIX r4 music commands" in _t:
            log("  r2: r4 music commands already registered")
        else:
            _reg = (
                "\n\n    # WZFIX r4 music commands (v15.45)\n"
                "    try:\n"
                "        from bot.helper.wzfix.r4_music_cmds import (\n"
                "            music_dl,\n"
                "            music_d,\n"
                "            music_y,\n"
                "            music_ml,\n"
                "            music_ld,\n"
                "            music_ld_yes,\n"
                "            music_ld_no,\n"
                "            music_ld_revise,\n"
                "            music_ld_pick,\n"
                "            music_ld_more,\n"
                "        )\n"
                "\n"
                "        TgClient.bot.add_handler(\n"
                "            MessageHandler(\n"
                "                music_dl,\n"
                "                filters=command(\"dl\", case_sensitive=True)\n"
                "                & CustomFilters.authorized,\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            MessageHandler(\n"
                "                music_d,\n"
                "                filters=command(\"d\", case_sensitive=True)\n"
                "                & CustomFilters.authorized,\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            MessageHandler(\n"
                "                music_y,\n"
                "                filters=command(\"y\", case_sensitive=True)\n"
                "                & CustomFilters.authorized,\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            MessageHandler(\n"
                "                music_ml,\n"
                "                filters=command(\"ml\", case_sensitive=True)\n"
                "                & CustomFilters.authorized,\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            MessageHandler(\n"
                "                music_ld,\n"
                "                filters=command(\"ld\", case_sensitive=True)\n"
                "                & CustomFilters.authorized,\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            CallbackQueryHandler(\n"
                "                music_ld_yes, filters=regex(r\"^wzfixldy:\")\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            CallbackQueryHandler(\n"
                "                music_ld_no, filters=regex(r\"^wzfixldn$\")\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            CallbackQueryHandler(\n"
                "                music_ld_revise,\n"
                "                filters=regex(r\"^wzfixldr$\"),\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            CallbackQueryHandler(\n"
                "                music_ld_pick,\n"
                "                filters=regex(r\"^wzfixldp:\\d+$\"),\n"
                "            )\n"
                "        )\n"
                "        TgClient.bot.add_handler(\n"
                "            CallbackQueryHandler(\n"
                "                music_ld_more,\n"
                "                filters=regex(r\"^wzfixldm$\"),\n"
                "            )\n"
                "        )\n"
                "    except Exception as e:\n"
                "        print(f\"WZFIX r4 command registration failed: {e}\")\n"
            )
            with open(_hd, "a", encoding="utf-8") as f:
                f.write(_reg)
            _r = subprocess.run(
                [sys.executable, "-m", "py_compile", _hd],
                capture_output=True, text=True, timeout=60,
            )
            if _r.returncode == 0:
                log("  r2: r4 music commands registered (/dl /d /y /ld)")
            else:
                log(
                    f"  r2: r4 command registration FAILED — "
                    f"{(_r.stderr or '').strip()[:200]}",
                    "ERROR",
                )
    except Exception as e:
        log(f"  r2: J-28 patch FAILED — {e}", "ERROR")

    # J-29: music keep-chat (v15.82) — hyper uploads of music zips go to
    # LEECH_LOG_CHAT, hiding the delivered zip from the user's chat;
    # music files must stay in the chat where they were requested
    try:
        _p = os.path.join(
            WZMLX_DIR, "bot/helper/ext_utils/hyperul_utils.py"
        )
        with open(_p, "r", encoding="utf-8") as f:
            _t = f.read()
        if "WZFIX music keep-chat" not in _t:
            _old = (
                "            use_hyper = Config.USE_HYPER and self.clients"
                " and up_size > 10 * 1024 * 1024"
            )
            _new = (
                "            # WZFIX music keep-chat (v15.82): the hyper"
                " pool routes\n"
                "            # >10MB files to LEECH_LOG_CHAT, which hides"
                " the delivered\n"
                "            # zip from the user's chat — music files stay"
                " in the\n"
                "            # requesting chat\n"
                "            use_hyper = (\n"
                "                Config.USE_HYPER\n"
                "                and self.clients\n"
                "                and up_size > 10 * 1024 * 1024\n"
                "                and not getattr(\n"
                "                    self._listener, \"_wzfix_music\", False\n"
                "                )\n"
                "            )"
            )
            if _old in _t:
                _t = _t.replace(_old, _new, 1)
                with open(_p, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p],
                    capture_output=True, text=True, timeout=60,
                )
                if _r.returncode == 0:
                    log("  r2: hyperul_utils.py music keep-chat applied")
                else:
                    log(
                        f"  r2: hyperul_utils.py keep-chat FAILED — "
                        f"{(_r.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: hyperul_utils.py use_hyper anchor missing", "WARN")
        else:
            log("  r2: hyperul_utils.py music keep-chat already applied")
    except Exception as e:
        log(f"  r2: J-29 patch FAILED — {e}", "ERROR")



    # J-13: ytdl (artists/playlists/videos) — the block message must be
    # the ONE clean message, not the old "Limit Breached" card.
    try:
        yd_path = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py",
        )
        with open(yd_path, "r", encoding="utf-8") as f:
            yd = f.read()
        if "WZFIX ytdl quota block" not in yd:
            old_yc = (
                "        if limit_exceeded := await limit_checker(self._listener, self.playlist_count):\n"
                "            await self._listener.on_download_error(limit_exceeded, is_limit=True)\n"
                "            return\n"
            )
            new_yc = (
                "        if limit_exceeded := await limit_checker(self._listener, self.playlist_count):\n"
                "            if limit_exceeded.startswith(\"\\U0001F6AB\"):  # WZFIX ytdl quota block\n"
                "                # ONE clean message, no error card, no status edits\n"
                "                from contextlib import suppress as _wzsup\n"
                "                try:\n"
                "                    async with task_dict_lock:\n"
                "                        task_dict.pop(self._listener.mid, None)\n"
                "                except Exception:\n"
                "                    pass\n"
                "                with _wzsup(Exception):\n"
                "                    from ...telegram_helper.message_utils import send_message, delete_message\n"
                "                    await send_message(self._listener.message, limit_exceeded)\n"
                "                with _wzsup(Exception):\n"
                "                    await delete_message(self._listener.message)\n"
                "                return\n"
                "            await self._listener.on_download_error(limit_exceeded, is_limit=True)\n"
                "            return\n"
            )
            if old_yc in yd:
                yd = yd.replace(old_yc, new_yc, 1)
                with open(yd_path, "w", encoding="utf-8") as f:
                    f.write(yd)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", yd_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: ytdl blocks show the ONE clean message")
                else:
                    log(f"  r2: ytdl patch compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: ytdl limit_checker anchor not found", "WARN")
        else:
            log("  r2: ytdl quota block already applied")
    except Exception as e:
        log(f"  r2: ytdl patch FAILED — {e}", "ERROR")

    # J-14: sabnzbdapi must survive ANY niquests version. Kaggle base
    # images can carry a preinstalled niquests without the vendored
    # `packages` submodule — the bot crashed at import on exactly that
    # (ModuleNotFoundError: niquests.packages). Fall back to plain
    # urllib3, then to a no-op, so the bot starts regardless.
    try:
        sb_path = os.path.join(WZMLX_DIR, "sabnzbdapi", "requests.py")
        with open(sb_path, "r", encoding="utf-8") as f:
            sb = f.read()
        if "WZFIX niquests compat" not in sb:
            old_sb = (
                "from niquests.packages.urllib3 import disable_warnings\n"
                "from niquests.packages.urllib3.exceptions import InsecureRequestWarning\n"
            )
            new_sb = (
                "# WZFIX niquests compat: some niquests builds do not\n"
                "# vendor the packages submodule — fall back to urllib3\n"
                "try:\n"
                "    from niquests.packages.urllib3 import disable_warnings\n"
                "    from niquests.packages.urllib3.exceptions import InsecureRequestWarning\n"
                "except ImportError:\n"
                "    try:\n"
                "        from urllib3 import disable_warnings\n"
                "        from urllib3.exceptions import InsecureRequestWarning\n"
                "    except ImportError:\n"
                "        def disable_warnings(*_a, **_k):\n"
                "            pass\n"
                "\n"
                "        class InsecureRequestWarning(Warning):\n"
                "            pass\n"
            )
            if old_sb in sb:
                sb = sb.replace(old_sb, new_sb, 1)
                with open(sb_path, "w", encoding="utf-8") as f:
                    f.write(sb)
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", sb_path],
                    capture_output=True, text=True, timeout=60,
                )
                if r.returncode == 0:
                    log("  r2: sabnzbdapi niquests-compat patched")
                else:
                    log(f"  r2: niquests compat compile FAILED — {(r.stderr or '').strip()[:200]}", "ERROR")
            else:
                log("  r2: sabnzbdapi anchor not found", "WARN")
        else:
            log("  r2: niquests compat already applied")
    except Exception as e:
        log(f"  r2: niquests compat patch FAILED — {e}", "ERROR")

    # J-14b: two more files import niquests.packages directly —
    # shortener_utils.py and direct_link_generator.py — patch them too
    try:
        su_path = os.path.join(
            WZMLX_DIR, "bot/helper/ext_utils/shortener_utils.py"
        )
        with open(su_path, "r", encoding="utf-8") as f:
            su = f.read()
        if "WZFIX niquests compat" not in su:
            old_su = "from niquests.packages.urllib3 import disable_warnings\n"
            new_su = (
                "# WZFIX niquests compat\n"
                "try:\n"
                "    from niquests.packages.urllib3 import disable_warnings\n"
                "except ImportError:\n"
                "    try:\n"
                "        from urllib3 import disable_warnings\n"
                "    except ImportError:\n"
                "        def disable_warnings(*_a, **_k):\n"
                "            pass\n"
            )
            if old_su in su:
                su = su.replace(old_su, new_su, 1)
                with open(su_path, "w", encoding="utf-8") as f:
                    f.write(su)
                log("  r2: shortener_utils niquests-compat patched")
            else:
                log("  r2: shortener_utils anchor not found", "WARN")
        else:
            log("  r2: shortener_utils already compatible")
    except Exception as e:
        log(f"  r2: shortener_utils patch FAILED — {e}", "ERROR")

    try:
        dl_path = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/direct_link_generator.py",
        )
        with open(dl_path, "r", encoding="utf-8") as f:
            dl = f.read()
        if "WZFIX niquests compat" not in dl:
            old_dl = "from niquests.packages.urllib3.util.retry import Retry\n"
            new_dl = (
                "# WZFIX niquests compat\n"
                "try:\n"
                "    from niquests.packages.urllib3.util.retry import Retry\n"
                "except ImportError:\n"
                "    from urllib3.util.retry import Retry\n"
            )
            if old_dl in dl:
                dl = dl.replace(old_dl, new_dl, 1)
                with open(dl_path, "w", encoding="utf-8") as f:
                    f.write(dl)
                log("  r2: direct_link_generator niquests-compat patched")
            else:
                log("  r2: direct_link_generator anchor not found", "WARN")
        else:
            log("  r2: direct_link_generator already compatible")
    except Exception as e:
        log(f"  r2: direct_link_generator patch FAILED — {e}", "ERROR")
    # R17 (v15.82): three fixes reported after R16 — the download
    # password page never showed (wserver's /dl/ proxy strips the
    # Accept header, so the gate now keys on the download kind; the
    # page also posted to the internal-only /_auth and now uses the
    # public /api/stream_auth route), the playlist card never
    # appeared (the watcher gave up after 2 minutes while the single
    # leader upload takes much longer — now 30 minutes), and the log
    # channel flooded with one exempt message per song (admin notes
    # are now aggregated into one message per burst of tasks).
    try:
        _nl17 = chr(10)
        _nph17 = chr(37) + chr(78) + chr(37)
        _kph17 = chr(37) + chr(75) + chr(37)
        _bs17 = chr(92) + "n"
        _bk17 = chr(92) + "U0001F451"
        _ss17 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(_ss17, "r", encoding="utf-8") as _f:
            _s17 = _f.read()
        if "WZFIX_R17" in _s17:
            log("  r17: download page gate already present")
        else:
            _g17o = (
                '        if "text/html" in (request.headers.get("Accept") or ""):\n'
                '            return _dl_auth_page()\n'
            ).replace(_nph17, _bs17).replace(_kph17, _bk17)
            _g17n = (
                '        # WZFIX_R17: the public /dl/ route is proxied by wserver\n'
                '        # with a fresh header set - Accept never reaches here, so\n'
                '        # key on the download kind instead. Player fetches use the\n'
                '        # playback kind and keep the raw 401 that drives the\n'
                '        # in-page password modal.\n'
                '        if kind == "download":\n'
                '            return _dl_auth_page()\n'
            ).replace(_nph17, _bs17)
            _ok17 = 0
            if _g17o in _s17:
                _s17 = _s17.replace(_g17o, _g17n, 1)
                _ok17 += 1
            else:
                log("  r17: gate anchor missing", "WARN")
            _fa17 = "await fetch(" + chr(34) + "/_auth" + chr(34)
            _fb17 = "await fetch(" + chr(34) + "/api/stream_auth" + chr(34)
            if _fa17 in _s17:
                _s17 = _s17.replace(_fa17, _fb17, 1)
                _ok17 += 1
            else:
                log("  r17: page auth route already public", "WARN")
            if _ok17 == 2:
                with open(_ss17, "w", encoding="utf-8") as _f:
                    _f.write(_s17)
                _r17 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _ss17],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r17.returncode == 0:
                    log("  r17: download page gate fixed (kind + public auth)")
                else:
                    log("  r17: stream_server compile FAILED", "ERROR")
            else:
                log("  r17: stream_server edits incomplete - not written", "WARN")
        _r3p17 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r3_music.py")
        with open(_r3p17, "r", encoding="utf-8") as _f:
            _r317 = _f.read()
        if "WZFIX_R17" in _r317:
            log("  r17: playlist wait already extended")
        else:
            _w17o = (
                '                        _pl_items = None\n'
                '                        for _retry in range(6):\n'
                '                            _pl_items = _wz.get("pl") or []\n'
                '                            if _pl_items:\n'
                '                                break\n'
                '                            _log(\n'
                '                                "WZFIX r15: playlist empty — retry "\n'
                '                                f"{_retry + 1}/6 (waiting 20s)"\n'
                '                            )\n'
                '                            await asyncio.sleep(20)\n'
                '                            _wz = (\n'
                '                                _shared[f"/{_folder}"].get("_wzfix") or {}\n'
                '                            )\n'
            ).replace(_nph17, _bs17)
            _w17n = (
                '                        _pl_items = None\n'
                '                        # WZFIX_R17: the leader uploads the whole\n'
                '                        # batch in ONE task and that takes far longer\n'
                '                        # than 2 minutes - poll for up to 30 minutes\n'
                '                        for _retry in range(60):\n'
                '                            _wz = (\n'
                '                                _shared[f"/{_folder}"].get("_wzfix") or {}\n'
                '                            )\n'
                '                            _pl_items = _wz.get("pl") or []\n'
                '                            if _pl_items:\n'
                '                                break\n'
                '                            if _retry % 5 == 0:\n'
                '                                _log(\n'
                '                                    "WZFIX r17: playlist still empty — "\n'
                '                                    f"retry {_retry + 1}/60 (waiting 30s)"\n'
                '                                )\n'
                '                            await asyncio.sleep(30)\n'
            ).replace(_nph17, _bs17)
            if _w17o in _r317:
                _r317 = _r317.replace(_w17o, _w17n, 1)
                with open(_r3p17, "w", encoding="utf-8") as _f:
                    _f.write(_r317)
                _r17 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _r3p17],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r17.returncode == 0:
                    log("  r17: playlist wait extended (30 min)")
                else:
                    log("  r17: r3_music compile FAILED", "ERROR")
            else:
                log("  r17: watcher anchor missing", "WARN")
        _tm17 = os.path.join(WZMLX_DIR, "bot/helper/ext_utils/task_manager.py")
        with open(_tm17, "r", encoding="utf-8") as _f:
            _t17 = _f.read()
        if "agg_note(" in _t17:
            log("  r17: exempt notes already aggregated")
        else:
            _t17o = (
                '            await admin_log(\n'
                '                "%K% <b>Exempt task — no quota check</b>",\n'
                '                f"┏ <b>User</b> → {_who}%N%"\n'
                '                f"┠ <b>Where</b> → {_ct} {_cn[:60]}%N%"\n'
                '                f"┖ Owner/sudo are exempt by design",\n'
                '            )\n'
            ).replace(_nph17, _bs17).replace(_kph17, _bk17)
            _t17n = (
                '            from ..wzfix.r1_core import agg_note\n'
                '\n'
                '            _nl17 = chr(10)\n'
                '            await agg_note(\n'
                '                f"exempt:{_fu.id if _fu else 0}",\n'
                '                "👑 <b>Exempt tasks — no quota check</b>",\n'
                '                "┏ <b>User</b> → " + _who + _nl17\n'
                '                + "┠ <b>Where</b> → " + _ct + " " + _cn[:60] + _nl17\n'
                '                + "┖ Owner/sudo are exempt — no bandwidth charged",\n'
                '            )\n'
            ).replace(_nph17, _bs17)
            if _t17o in _t17:
                _t17 = _t17.replace(_t17o, _t17n, 1)
                with open(_tm17, "w", encoding="utf-8") as _f:
                    _f.write(_t17)
                _r17 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _tm17],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r17.returncode == 0:
                    log("  r17: exempt notes aggregated")
                else:
                    log("  r17: task_manager compile FAILED", "ERROR")
            else:
                log("  r17: exempt anchor missing", "WARN")
        _rc17 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r1_core.py")
        with open(_rc17, "r", encoding="utf-8") as _f:
            _rcs17 = _f.read()
        if "def agg_note" in _rcs17:
            log("  r17: r1_core aggregator already present")
        else:
            _rc17o = (
                '            await admin_log(\n'
                '                "⚠️ <b>Task without pre-checkable link</b>",\n'
                '                f"┏ <b>User</b> → <code>{user_id}</code>%N%"\n'
                '                f"┠ <b>Where</b> → {_ctype} {_cname[:60]}%N%"\n'
                '                f"┠ <b>Links seen</b> → {len(links)}%N%"\n'
                '                f"┖ Falls back to the in-download check — the download "\n'
                '                f"is still held and verified",\n'
                '            )\n'
                '            return None\n'
            ).replace(_nph17, _bs17)
            _rc17n = (
                '            await agg_note(\n'
                '                f"nopre:{user_id}",\n'
                '                "⚠️ <b>Tasks without pre-checkable link</b>",\n'
                '                "┏ <b>User</b> → <code>" + str(user_id) + "</code>" + _NL17\n'
                '                + "┠ <b>Where</b> → " + _ctype + " " + _cname[:60] + _NL17\n'
                '                + "┠ <b>Links seen</b> → " + str(len(links)) + _NL17\n'
                '                + "┖ Falls back to the in-download check — still verified",\n'
                '            )\n'
                '            return None\n'
            ).replace(_nph17, _bs17)
            if _rc17o in _rcs17:
                _rcs17 = _rcs17.replace(_rc17o, _rc17n, 1)
                log("  r17: precheck notes aggregated")
            else:
                log("  r17: precheck anchor missing", "WARN")
            _rcs17 = _rcs17 + _nl17 + _nl17 + (
                '\n'
                '\n'
                '# WZFIX_R17 (v15.82): aggregated admin notes. A 70-song fan-out fired\n'
                '# 70 separate "Exempt task" channel messages; agg_note counts a burst\n'
                '# of tasks and flushes ONE message ~45s after the last event.\n'
                '\n'
                '_AGG17 = {}\n'
                '_NL17 = chr(10)\n'
                '\n'
                '\n'
                'async def agg_note(key, title, body):\n'
                '    try:\n'
                '        import asyncio as _aio17\n'
                '\n'
                '        _st = _AGG17.get(key)\n'
                '        if _st is None:\n'
                '            _st = _AGG17[key] = {"n": 0}\n'
                '        _st["n"] += 1\n'
                '        _t = _st.get("t")\n'
                '        if _t is not None and not _t.done():\n'
                '            _t.cancel()\n'
                '        _st["ttl"] = title\n'
                '        _st["bd"] = body\n'
                '\n'
                '        async def _fl17():\n'
                '            try:\n'
                '                await _aio17.sleep(45)\n'
                '            except _aio17.CancelledError:\n'
                '                return\n'
                '            _it = _AGG17.pop(key, None)\n'
                '            if not _it:\n'
                '                return\n'
                '            try:\n'
                '                await admin_log(\n'
                '                    _it.get("ttl") or "WZFIX note",\n'
                '                    "┠ <b>Tasks</b> → " + str(_it["n"]) + _NL17\n'
                '                    + (_it.get("bd") or ""),\n'
                '                )\n'
                '            except Exception:\n'
                '                pass\n'
                '\n'
                '        _st["t"] = _aio17.get_running_loop().create_task(_fl17())\n'
                '    except Exception as _e17:\n'
                '        LOGGER.error(f"WZFIX agg_note failed: {_e17}")\n'
            ).replace(_nph17, _bs17)
            with open(_rc17, "w", encoding="utf-8") as _f:
                _f.write(_rcs17)
            _r17 = subprocess.run(
                [sys.executable, "-m", "py_compile", _rc17],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if _r17.returncode == 0:
                log("  r17: admin note aggregator added")
            else:
                log("  r17: r1_core compile FAILED", "ERROR")
    except Exception as e:
        log(f"  r17: FAILED — {e}", "ERROR")

    # R18 (v15.82): the raw-401 came from the r5 per-link gate
    # that fires BEFORE the user gate R16/R17 patched - it now
    # serves the password page for download-kind requests too,
    # and the page posts the link token so per-link passwords
    # verify (returning a link-signed token). Also: the artist
    # catalogue dedup now folds variant titles (8D, Official
    # Video, Slowed/Reverb...) so a full-catalogue batch no
    # longer downloads the same song twice.
    try:
        _ss18 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(_ss18, "r", encoding="utf-8") as _f:
            _s18 = _f.read()
        if "WZFIX_R18" in _s18:
            log("  r18: download page already on the r5 gate")
        else:
            _g18o = (
                '    if not await _r5_serve_ok(request):\n'
                '        raise web.HTTPUnauthorized(\n'
                '            text="authenticate first",\n'
                '            headers={"X-Stream-Auth-Required": "1"},\n'
                '        )\n'
            )
            _g18n = (
                '    if not await _r5_serve_ok(request):\n'
                '        # WZFIX_R18: this per-link gate fires BEFORE the user gate\n'
                '        # below - a download-kind open (the Telegram download\n'
                '        # button) must see the password page here, not raw text\n'
                '        if kind == "download":\n'
                '            return _dl_auth_page()\n'
                '        raise web.HTTPUnauthorized(\n'
                '            text="authenticate first",\n'
                '            headers={"X-Stream-Auth-Required": "1"},\n'
                '        )\n'
            )
            _ok18 = 0
            if _g18o in _s18:
                _s18 = _s18.replace(_g18o, _g18n, 1)
                _ok18 += 1
            else:
                log("  r18: r5 gate anchor missing", "WARN")
            _fa18 = "body:JSON.stringify({password:P.value})"
            _fb18 = (
                "body:JSON.stringify({password:P.value,token:"
                + "location.pathname.split(" + chr(34) + "/" + chr(34)
                + ").filter(Boolean).pop()||" + chr(34) + chr(34)
            )
            if _fa18 in _s18:
                _s18 = _s18.replace(_fa18, _fb18, 1)
                _ok18 += 1
            else:
                log("  r18: page body already posts the token", "WARN")
            if _ok18 == 2:
                with open(_ss18, "w", encoding="utf-8") as _f:
                    _f.write(_s18)
                _r18 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _ss18],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r18.returncode == 0:
                    log("  r18: download page on the r5 gate + token in the page POST")
                else:
                    log("  r18: stream_server compile FAILED", "ERROR")
            else:
                log("  r18: stream_server edits incomplete - not written", "WARN")
        _r3p18 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r3_music.py")
        with open(_r3p18, "r", encoding="utf-8") as _f:
            _r318 = _f.read()
        if "WZFIX_R18" in _r318:
            log("  r18: catalogue dedup already folded")
        else:
            _n18a = (
                'def _norm_key(name):\n'
            )
            _n18p = (
                '_VARIANT_WORDS = frozenset(\n'
                '    "official video audio lyrics lyric lyrical 8d 16d hd hq 4k bass "\n'
                '    "boosted slowed reverb sped speedup visualizer visual karaoke "\n'
                '    "instrumental remix live cover version tribute full"\n'
                '    .split()\n'
                ')\n'
                '\n'
                '\n'
                'def _norm_key(name):\n'
            )
            _n18o = (
                '    s = _FEAT_PAT.sub(" ", name or "")\n'
                '    s = _META_PAT.sub(" ", s)\n'
                '    s = re.sub(r"[^0-9a-z]+", " ", s.lower())\n'
                '    return " ".join(s.split())\n'
            )
            _n18n = (
                '    s = _FEAT_PAT.sub(" ", name or "")\n'
                '    s = _META_PAT.sub(" ", s)\n'
                '    s = re.sub(r"(?:sped|speed)[ -]?up", " ", s, flags=re.I)\n'
                '    toks = re.sub(r"[^0-9a-z]+", " ", s.lower()).split()\n'
                '    # WZFIX_R18: variant markers also appear unbracketed, after a\n'
                '    # dash ("Song - 8D Audio") or bare ("Song Official Video") —\n'
                '    # drop every variant word so they all fold into the base song\n'
                '    toks = [t for t in toks if t not in _VARIANT_WORDS]\n'
                '    return " ".join(toks)\n'
            )
            _ok18b = 0
            if _n18a in _r318 and _r318.count(_n18a) == 1:
                _r318 = _r318.replace(_n18a, _n18p, 1)
                _ok18b += 1
            else:
                log("  r18: _norm_key def anchor missing", "WARN")
            if _n18o in _r318 and _r318.count(_n18o) == 1:
                _r318 = _r318.replace(_n18o, _n18n, 1)
                _ok18b += 1
            else:
                log("  r18: _norm_key body anchor missing", "WARN")
            if _ok18b == 2:
                with open(_r3p18, "w", encoding="utf-8") as _f:
                    _f.write(_r318)
                _r18 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _r3p18],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r18.returncode == 0:
                    log("  r18: catalogue dedup folds variant titles (8D, Official Video, ...)")
                else:
                    log("  r18: r3_music compile FAILED", "ERROR")
            else:
                log("  r18: dedup anchors incomplete - not written", "WARN")
    except Exception as e:
        log(f"  r18: FAILED — {e}", "ERROR")

    # R20 (v15.82): the /dl/ route serves kind="bulk", not "download" -
    # the R17/R18 gates never matched, so the password page never
    # showed for direct download links. Both gates now accept bulk.
    try:
        _ss20 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(_ss20, "r", encoding="utf-8") as _f:
            _s20 = _f.read()
        if "WZFIX_R20" in _s20:
            log("  r20: bulk-kind download page already applied")
        else:
            _a20 = (
                "        if kind == " + chr(34) + "download" + chr(34) + ":" + chr(10)
            )
            _b20 = (
                "        # WZFIX_R20: the /dl/ route passes kind=" + chr(34) + "bulk" + chr(34) + " - accept it" + chr(10)
                + "        if kind in (" + chr(34) + "download" + chr(34) + ", " + chr(34) + "bulk" + chr(34) + "):" + chr(10)
            )
            _n20 = _s20.count(_a20)
            if _n20 == 2:
                _s20 = _s20.replace(_a20, _b20)
                with open(_ss20, "w", encoding="utf-8") as _f:
                    _f.write(_s20)
                _r20 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _ss20],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r20.returncode == 0:
                    log("  r20: download page now serves the bulk kind (both gates)")
                else:
                    log("  r20: stream_server compile FAILED", "ERROR")
            else:
                log(f"  r20: expected 2 gates, found {_n20} - not written", "WARN")
    except Exception as e:
        log(f"  r20: FAILED — {e}", "ERROR")
    # R21 (v15.82): the download password page fetch() was never
    # closed - a JS syntax error killed the whole script, so the
    # Unlock button did nothing after entering the right password.
    try:
        _ss21 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")
        with open(_ss21, "r", encoding="utf-8") as _f:
            _s21 = _f.read()
        if "WZFIX_R21" in _s21:
            log("  r21: download page fetch fix already applied")
        else:
            _a21 = (
                "body:JSON.stringify({password:P.value,token:location.pathname.split(" + chr(34) + "/" + chr(34) + ").filter(Boolean).pop()||" + chr(34) + chr(34) + "});" + chr(10)
            )
            _b21 = (
                "body:JSON.stringify({password:P.value,token:location.pathname.split(" + chr(34) + "/" + chr(34) + ").filter(Boolean).pop()||" + chr(34) + chr(34) + "})});" + chr(10)
            )
            _n21 = _s21.count(_a21)
            if _n21 == 1:
                _s21 = _s21.replace(_a21, _b21)
                with open(_ss21, "w", encoding="utf-8") as _f:
                    _f.write(_s21)
                _r21 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _ss21],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r21.returncode == 0:
                    log("  r21: download page fetch closed (Unlock button works again)")
                else:
                    log("  r21: stream_server compile FAILED", "ERROR")
            else:
                log(f"  r21: expected 1 anchor, found {_n21} - not written", "WARN")
    except Exception as e:
        log(f"  r21: FAILED - {e}", "ERROR")
    # R22 (v15.82): the per-task botpm check spammed a Task Checks
    # card for every song in a batch (24 at once) because Telegram
    # rate-limits send_chat_action right after a restart and any
    # error counted as not-started. Flood/timeout errors now count
    # as started, and a genuine not-started note is returned at most
    # once per user per 30 minutes.
    try:
        _ss22 = os.path.join(WZMLX_DIR, "bot/helper/telegram_helper/tg_utils.py")
        with open(_ss22, "r", encoding="utf-8") as _f:
            _s22 = _f.read()
        if "WZFIX_R22" in _s22:
            log("  r22: botpm check dedupe already applied")
        else:
            _a22 = (
                "async def check_botpm(message, button=None):" + chr(10)
                + "    try:" + chr(10)
                + "        await TgClient.bot.send_chat_action(message.from_user.id, ChatAction.TYPING)" + chr(10)
                + "        return None, button" + chr(10)
                + "    except Exception:" + chr(10)
                + "        if button is None:" + chr(10)
                + "            button = ButtonMaker()" + chr(10)
                + "        _msg = " + chr(34) + "┠ <i>Bot isn" + chr(39) + "t Started in PM or Inbox (Private)</i>" + chr(34) + "" + chr(10)
                + "        button.url_button(" + chr(10)
                + "            " + chr(34) + "Start Bot Now" + chr(34) + ", f" + chr(34) + "https://t.me/{TgClient.BNAME}?start=start" + chr(34) + ", " + chr(34) + "header" + chr(34) + "" + chr(10)
                + "        )" + chr(10)
                + "        return _msg, button" + chr(10)
            )
            _b22 = (
                "_WZFIX_BOTPM_LAST = {}" + chr(10)
                + "" + chr(10)
                + "" + chr(10)
                + "async def check_botpm(message, button=None):" + chr(10)
                + "    # WZFIX_R22: flood/timeout = started; note at most once per user per 30 min" + chr(10)
                + "    try:" + chr(10)
                + "        await TgClient.bot.send_chat_action(message.from_user.id, ChatAction.TYPING)" + chr(10)
                + "        return None, button" + chr(10)
                + "    except Exception as e:" + chr(10)
                + "        _e = str(e).upper()" + chr(10)
                + "        if " + chr(34) + "FLOOD" + chr(34) + " in _e or " + chr(34) + "TOO MANY REQUESTS" + chr(34) + " in _e or " + chr(34) + "TIMEOUT" + chr(34) + " in _e or " + chr(34) + "SLOW MODE" + chr(34) + " in _e:" + chr(10)
                + "            return None, button" + chr(10)
                + "        try:" + chr(10)
                + "            _uid = message.from_user.id" + chr(10)
                + "        except Exception:" + chr(10)
                + "            _uid = 0" + chr(10)
                + "        from time import time as _r22t" + chr(10)
                + "        if _WZFIX_BOTPM_LAST.get(_uid, 0) and _r22t() - _WZFIX_BOTPM_LAST[_uid] < 1800:" + chr(10)
                + "            return None, button" + chr(10)
                + "        _WZFIX_BOTPM_LAST[_uid] = _r22t()" + chr(10)
                + "        if button is None:" + chr(10)
                + "            button = ButtonMaker()" + chr(10)
                + "        _msg = " + chr(34) + "┠ <i>Bot isn" + chr(39) + "t Started in PM or Inbox (Private)</i>" + chr(34) + "" + chr(10)
                + "        button.url_button(" + chr(10)
                + "            " + chr(34) + "Start Bot Now" + chr(34) + ", f" + chr(34) + "https://t.me/{TgClient.BNAME}?start=start" + chr(34) + ", " + chr(34) + "header" + chr(34) + "" + chr(10)
                + "        )" + chr(10)
                + "        return _msg, button" + chr(10)
            )
            _n22 = _s22.count(_a22)
            if _n22 == 1:
                _s22 = _s22.replace(_a22, _b22)
                with open(_ss22, "w", encoding="utf-8") as _f:
                    _f.write(_s22)
                _r22 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _ss22],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r22.returncode == 0:
                    log("  r22: botpm check deduped (no more per-task spam)")
                else:
                    log("  r22: tg_utils compile FAILED", "ERROR")
            else:
                log(f"  r22: expected 1 anchor, found {_n22} - not written", "WARN")
    except Exception as e:
        log(f"  r22: FAILED - {e}", "ERROR")
    # R23 (v15.82) PHASE-A: YouTube rate limits per IP - the fix is
    # the current anti-throttle stack, not more hammering. Installs:
    # deno (JS runtime yt-dlp now requires for n/sig challenges), the
    # bgutil PO-token server (proof-of-origin tokens so datacenter IPs
    # pass the bot check), and its version-matched yt-dlp plugin. The
    # server runs on 127.0.0.1:4416 with a keepalive thread. Binaries
    # persist in KAGGLE_WORKING so reboots skip the downloads.
    try:
        import threading as _th23
        import urllib.request as _ur23
        import zipfile as _zf23

        _bin23 = os.path.join(KAGGLE_WORKING, "bin")
        os.makedirs(_bin23, exist_ok=True)

        def _dl23(url, dest):
            _req = _ur23.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with _ur23.urlopen(_req, timeout=180) as _r, open(dest, "wb") as _f:
                while True:
                    _chunk = _r.read(1 << 20)
                    if not _chunk:
                        break
                    _f.write(_chunk)

        _bg23 = os.path.join(_bin23, "bgutil-pot")
        if not os.path.isfile(_bg23):
            _dl23("https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/download/v0.8.1/bgutil-pot-linux-x86_64", _bg23)
            os.chmod(_bg23, 0o755)

        _dn23 = os.path.join(_bin23, "deno")
        if not os.path.isfile(_dn23):
            _z23 = _bg23 + ".deno.zip"
            _dl23("https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip", _z23)
            with _zf23.ZipFile(_z23) as _z:
                with _z.open("deno") as _s, open(_dn23, "wb") as _f:
                    _f.write(_s.read())
            os.chmod(_dn23, 0o755)
            os.remove(_z23)

        _plug23 = os.path.join(WZMLX_DIR, "yt_dlp_plugins")
        if not os.path.isdir(_plug23):
            _p23 = _bg23 + ".plugin.zip"
            _dl23("https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/download/v0.8.1/bgutil-ytdlp-pot-provider-rs.zip", _p23)
            with _zf23.ZipFile(_p23) as _z:
                _z.extractall(WZMLX_DIR)
            os.remove(_p23)

        def _ping23():
            try:
                with _ur23.urlopen("http://127.0.0.1:4416/ping", timeout=5) as _r:
                    return _r.status == 200
            except Exception:
                return False

        def _pot23_start():
            _lf23 = open(os.path.join(KAGGLE_WORKING, "potserver.log"), "a")
            subprocess.Popen(
                [_bg23, "server", "--host", "127.0.0.1", "--port", "4416"],
                stdout=_lf23,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

        if not _ping23():
            _pot23_start()
            import time as _t23

            for _i23 in range(15):
                if _ping23():
                    break
                _t23.sleep(1)

        def _pot23_keepalive():
            import time as _t23

            while True:
                _t23.sleep(60)
                if not _ping23():
                    _pot23_start()

        _th23.Thread(target=_pot23_keepalive, daemon=True).start()
        log("  r23: PO token server (4416) + deno + bgutil plugin ready")
    except Exception as e:
        log(f"  r23: FAILED - {e}", "ERROR")

    # R23b: politeness - 1s between YouTube API requests (fewer 429s
    # means fewer restarts means higher real throughput)
    try:
        _ss23 = os.path.join(WZMLX_DIR, "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py")
        with open(_ss23, "r", encoding="utf-8") as _f:
            _s23 = _f.read()
        if "sleep_requests" not in _s23:
            _old23 = "            " + chr(34) + "trim_file_name" + chr(34) + ": 220," + chr(10)
            if _s23.count(_old23) == 1:
                _new23 = "            " + chr(34) + "sleep_requests" + chr(34) + ": 1," + chr(10)
                _s23 = _s23.replace(_old23, _old23 + _new23)
                with open(_ss23, "w", encoding="utf-8") as _f:
                    _f.write(_s23)
                log("  r23b: yt-dlp politeness set (sleep_requests=1)")
            else:
                log("  r23b: trim_file_name anchor missing", "WARN")
        else:
            log("  r23b: politeness already set")
    except Exception as e:
        log(f"  r23b: FAILED - {e}", "ERROR")

    # R23c: fan-out default 16 -> 6. Research-backed sweet spot for
    # concurrent YouTube downloads per IP is 4-8; 16 was inviting 429
    # spirals. Still tunable via WZFIX_FANOUT_JOBS.
    try:
        _sm23 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r3_music.py")
        with open(_sm23, "r", encoding="utf-8") as _f:
            _m23 = _f.read()
        _old23m = "_os.environ.get(" + chr(34) + "WZFIX_FANOUT_JOBS" + chr(34) + ", " + chr(34) + "16" + chr(34) + ")"
        _new23m = "_os.environ.get(" + chr(34) + "WZFIX_FANOUT_JOBS" + chr(34) + ", " + chr(34) + "6" + chr(34) + ")"
        if _old23m in _m23:
            _m23 = _m23.replace(_old23m, _new23m)
            with open(_sm23, "w", encoding="utf-8") as _f:
                _f.write(_m23)
            log("  r23c: fan-out default 16 -> 6 (per-IP sweet spot)")
        else:
            log("  r23c: fan-out default already 6")
    except Exception as e:
        log(f"  r23c: FAILED - {e}", "ERROR")
    # R24 (v15.82): artist-batch repair. The v15.74 m4a passthrough
    # parsed the music quality tail (b-m4a-5) as three dash-parts but
    # it only has two, so every music task died with
    # "list index out of range" before downloading anything. Also:
    # per-song streaming uploads (with -z the one-zip merge stays),
    # failed songs are now counted, the launch gate holds even when
    # tasks fail fast, and stray words after the artist link are no
    # longer glued onto every YouTube search.
    try:
        _p24a = os.path.join(WZMLX_DIR, "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py")
        with open(_p24a, "r", encoding="utf-8") as _f:
            _s24a = _f.read()
        if "r24a" in _s24a:
            log("  r24a: passthrough already fixed")
        else:
            _old24a = '            qual = f"{_pfx34}/b"\n            qual = audio_info[0]\n            audio_format = audio_info[1]\n            rate = audio_info[2]\n'
            if _s24a.count(_old24a) == 1:
                _new24a = '            qual = f"{_pfx34}/b"\n            audio_format = audio_info[0] if audio_info else "m4a"\n            rate = audio_info[1] if len(audio_info) > 1 else "5"\n'
                _s24a = _s24a.replace(_old24a, _new24a, 1)
                with open(_p24a, "w", encoding="utf-8") as _f:
                    _f.write(_s24a)
                log("  r24a: m4a passthrough crash fixed")
            else:
                log("  r24a: anchor missing", "WARN")
    except Exception as e:
        log(f"  r24a: FAILED - {e}", "ERROR")

    try:
        _p24b = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r3_music.py")
        with open(_p24b, "r", encoding="utf-8") as _f:
            _s24b = _f.read()
        if "_wz_r24_fail" in _s24b:
            log("  r24b: launcher already hardened")
        else:
            _ok24 = 0
            for _o24, _n24 in [
                ('                "leader_mid": None,\n', '                "leader_mid": None,\n                "mode": "song",\n'),
                ('    _shared[f"/{_folder}"]["_wzfix"]["leader_mid"] = _base + 100000\n', '    _shared[f"/{_folder}"]["_wzfix"]["leader_mid"] = _base + 100000\n    if "-z" in _flags:\n        _shared[f"/{_folder}"]["_wzfix"]["mode"] = "zip"\n'),
                ('    _jobs = _fanout_jobs()\n', '    # WZFIX r24: stray words typed after the artist link were\n    # being glued onto every YouTube search. Keep real flags and\n    # their values, drop the rest with a note in the log.\n    _valf24 = {"-n", "-s", "-sd", "-tl", "-ul", "-up"}\n    _keep24 = []\n    _drop24 = []\n    _toks24 = (_flags or "").split()\n    _i24 = 0\n    while _i24 < len(_toks24):\n        _t24 = _toks24[_i24]\n        if _t24 in _valf24 and _i24 + 1 < len(_toks24):\n            _keep24.append(_t24)\n            _keep24.append(_toks24[_i24 + 1])\n            _i24 += 2\n            continue\n        if _t24.startswith("-"):\n            _keep24.append(_t24)\n        else:\n            _drop24.append(_t24)\n        _i24 += 1\n    if _drop24:\n        _log("WZFIX music: ignoring extra words: " + " ".join(_drop24))\n        _flags = " ".join(_keep24)\n    _jobs = _fanout_jobs()\n'),
                ('    for i, (query, clean) in enumerate(tracks):\n', '    _inflight24 = set()\n    for i, (query, clean) in enumerate(tracks):\n'),
                ('            while True:\n                _ac = _active_dl_count()\n                if _ac is None or _ac < _jobs:\n                    break\n                await asyncio.sleep(0.5)\n', '            while True:\n                if len(_inflight24) < _jobs:\n                    break\n                await asyncio.sleep(0.5)\n'),
                ('            asyncio.create_task(_go())\n', '            _inflight24.add(m2.id)\n            asyncio.create_task(_go())\n'),
                ('                    _log(f"WZFIX music: artist track failed: {e}")\n', '                    _wz_r24_fail(_shared, _folder, _m, e)\n                finally:\n                    _inflight24.discard(_m.id)\n'),
                ('        except Exception as e:\n            _log(f"WZFIX music: artist fan-out dispatch failed: {e}")\n', '        except Exception as e:\n            _log(f"WZFIX music: artist fan-out dispatch failed: {e}")\n            _inflight24.pop(_base + 100000 + i, None)\n'),
                ('            "📦 <b>all songs are sent together</b> when every one "\n            "finishes — one zip with <code>-z</code>",\n', '            "📦 each song is sent as soon as it finishes "\n            "with <code>-z</code> you get one zip instead",\n'),
            ]:
                if _s24b.count(_o24) == 1:
                    _s24b = _s24b.replace(_o24, _n24, 1)
                    _ok24 += 1
                else:
                    log("  r24b: one anchor missing (skip)", "WARN")
            _s24b = _s24b + '\n\n# WZFIX r24 helpers (v15.82)\ndef _wz_r24_fail(_shared, _folder, _m, _e):\n    _log(f"WZFIX music: artist track failed: {_e}")\n    try:\n        _wz = _shared.get(f"/{_folder}", {}).get("_wzfix")\n        if _wz is not None:\n            _t = getattr(_m, "_wzfix_music_title", "") or "song"\n            _fl = _wz.setdefault("failed", [])\n            if _t not in _fl:\n                _fl.append(_t)\n    except Exception:\n        pass\n'
            with open(_p24b, "w", encoding="utf-8") as _f:
                _f.write(_s24b)
            log(f"  r24b: launcher hardened ({_ok24}/9 anchors)")
    except Exception as e:
        log(f"  r24b: FAILED - {e}", "ERROR")

    try:
        _p24c = os.path.join(WZMLX_DIR, "bot/helper/listeners/task_listener.py")
        with open(_p24c, "r", encoding="utf-8") as _f:
            _s24c = _f.read()
        if "r24 song mode" in _s24c:
            log("  r24c: song mode already applied")
        else:
            _old24c = '        if _wz15 is not None and _wz15.get("leader_mid"):\n'
            if _s24c.count(_old24c) == 1:
                _new24c = '        if _wz15 is not None and _wz15.get("leader_mid"):\n            if _wz15.get("mode") != "zip":\n                _wz15.setdefault("done", set()).add(self.mid)\n        if (\n            _wz15 is not None\n            and _wz15.get("leader_mid")\n            and _wz15.get("mode") == "zip"\n        ):\n'
                _s24c = _s24c.replace(_old24c, _new24c, 1)
                _s24c = _s24c.replace(
                    "WZFIX r15 deterministic merge: music fan-out clones",
                    "WZFIX r24 song mode + r15 deterministic merge: music fan-out clones",
                    1,
                )
                with open(_p24c, "w", encoding="utf-8") as _f:
                    _f.write(_s24c)
                log("  r24c: per-song streaming uploads (zip only with -z)")
            else:
                log("  r24c: anchor missing", "WARN")
    except Exception as e:
        log(f"  r24c: FAILED - {e}", "ERROR")

    try:
        import py_compile

        for _p24 in [
            "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py",
            "bot/helper/wzfix/r3_music.py",
            "bot/helper/listeners/task_listener.py",
        ]:
            _r24 = subprocess.run(
                [sys.executable, "-m", "py_compile", os.path.join(WZMLX_DIR, _p24)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if _r24.returncode != 0:
                log(f"  r24: compile FAILED for {_p24}", "ERROR")
        log("  r24: v15.82 artist-batch repair applied")
    except Exception as e:
        log(f"  r24: FAILED - {e}", "ERROR")
    # R25 (v15.83): Phase B + song-mode upload fix + fallback ladder
    try:
        _p25a = os.path.join(
            WZMLX_DIR, "bot/helper/listeners/task_listener.py"
        )
        with open(_p25a, "r", encoding="utf-8") as _f:
            _s25a = _f.read()
        if "WZFIX r25a" in _s25a:
            log("  r25a: song-mode upload fix already applied")
        else:
            _o25a = (
                '            if _wz15.get("mode") != "zip":\n'
                '                _wz15.setdefault("done", set())'
                '.add(self.mid)\n'
            )
            _n25a = r'''            if _wz15.get("mode") != "zip":
                _wz15.setdefault("done", set()).add(self.mid)
                # WZFIX r25a (v15.83): the v15.82 bug — in song mode the
                # clone fell into the upstream same_dir handler which
                # moved the song away and returned WITHOUT uploading
                # (every song "merged", nothing delivered). Deregister
                # so this clone uploads its own song through the
                # normal path; a retry success also clears the entry.
                _t25a = getattr(
                    getattr(self, "message", None),
                    "_wzfix_music_title",
                    "",
                )
                if _t25a and _t25a in (_wz15.get("failed") or []):
                    try:
                        _wz15["failed"].remove(_t25a)
                    except Exception:
                        pass
                try:
                    async with same_directory_lock:
                        _sd25 = (self.same_dir or {}).get(
                            self.folder_name
                        )
                        if _sd25 and self.mid in _sd25.get(
                            "tasks", set()
                        ):
                            _sd25["tasks"].discard(self.mid)
                            if _sd25.get("total", 0) > 0:
                                _sd25["total"] -= 1
                            LOGGER.info(
                                "WZFIX r25a: song-mode clone uploads "
                                "its own song (deregistered)"
                            )
                except Exception:
                    pass
'''
            if _s25a.count(_o25a) == 1:
                _s25a = _s25a.replace(_o25a, _n25a, 1)
                with open(_p25a, "w", encoding="utf-8") as _f:
                    _f.write(_s25a)
                _r25a = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p25a],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r25a.returncode == 0:
                    log(
                        "  r25a: song-mode clones upload their own "
                        "song (v15.82 zero-upload bug fixed)"
                    )
                else:
                    log(
                        "  r25a: compile FAILED — "
                        f"{(_r25a.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log(
                    "  r25a: anchor missing "
                    f"(count={_s25a.count(_o25a)})",
                    "WARN",
                )
    except Exception as e:
        log(f"  r25a: FAILED - {e}", "ERROR")

    # r25: kit verify + compile
    try:
        _k25 = os.path.join(
            WZMLX_DIR, "bot/helper/wzfix/r25_worker.py"
        )
        if os.path.isfile(_k25):
            log("  r25: Phase B worker kit written")
        else:
            log("  r25: worker kit MISSING", "ERROR")
        for _p25 in [
            "bot/helper/wzfix/r3_music.py",
            "bot/helper/wzfix/r25_worker.py",
        ]:
            _r25 = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "py_compile",
                    os.path.join(WZMLX_DIR, _p25),
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if _r25.returncode != 0:
                log(f"  r25: compile FAILED for {_p25}", "ERROR")
        log(
            "  r25: Phase B ready (workers for 6+ song batches, "
            "JioSaavn fallback, per-song upload fix)"
        )
    except Exception as e:
        log(f"  r25: FAILED - {e}", "ERROR")

    # Kaggle addition R17 — Round 17 (v15.84): web downloader (remote payload).
    # The bot becomes a downloader site: paste a link on the web page, the
    # bot's yt-dlp engine grabs it and the finished file downloads straight
    # to the user's device. Frontend: GET /webdl on the worker URL + GitHub
    # Pages (vot1122.github.io/ytwebdownload). The module + route patches
    # live outside the notebook (kernel source must stay < 1 MB): the
    # payload is Drive-hosted and sha256-pinned, same as _real_deploy_patch.
    # NOTE: no bare "import urllib/hashlib" here — a local import would make
    # "urllib" function-local and break the patch-kit download above (the
    # v15.84.0 bug: UnboundLocalError -> whole kit skipped). Module-level
    # urllib.request is used as-is; hashlib comes in via __import__.
    # Fails safe - on any error the bot boots without /webdl.
    try:
        _r17_url = (
            "https://drive.usercontent.google.com/download?"
            "id=16VVAlGx2m0YsLtb0A0Tv1oZIf5EXIkbl&export=download&confirm=t"
        )
        _r17_sha = "31cdae09710901998e5ffbd0a38a42171a7640c49a626b366192c7431e1f33b3"
        _r17 = urllib.request.urlopen(_r17_url, timeout=60).read()
        if __import__("hashlib").sha256(_r17).hexdigest() != _r17_sha:
            raise ValueError("payload sha mismatch")
        with open(os.path.join(os.getcwd(), "_r17_round.py"), "wb") as f:
            f.write(_r17)
        _r = subprocess.run(
            [sys.executable, "_r17_round.py", WZMLX_DIR],
            capture_output=True, text=True, timeout=180,
        )
        for _ln in (_r.stdout or "").splitlines():
            log(f"  r17: {_ln}")
        if _r.returncode != 0:
            log(
                f"  r17: payload FAILED - {(_r.stderr or '')[-300:]}",
                "ERROR",
            )
    except Exception as e:
        log(f"  r17: FAILED - {e}", "ERROR")

    log("WZML-X-Bot patch kit applied")
    return True




# ─── v15.1 aesthetic overhaul assets ────────────────────────────────────

REVAMP_FONTS = '<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap" rel="stylesheet">'

REVAMP_CSS = '<style id="wzml-revamp-style">\n' + '/* == WZML-X v15.1 — Aesthetic Overhaul == */\n\n/* -- New theme: Onyx Noir (dark, violet) -- */\n[data-theme="onyx"]{\n  --bg:#050508; --surface:#0a0a12; --surface-2:#0e0e18;\n  --line:rgba(139,92,246,.22); --line-2:#8b5cf6;\n  --text:#f4f2ff; --muted:#b6b0d4;\n  --accent:#8b5cf6; --accent-2:#a78bfa; --accent-soft:#1a1033;\n  --t-glyph:#0a0618; --t-veil-grid:rgba(139,92,246,.12);\n  --t-veil-top:rgba(139,92,246,.08); --t-veil-bot:rgba(16,8,38,.5);\n  --t-spot-grid:rgba(196,181,253,.14);\n  --t-card-bg:linear-gradient(180deg,rgba(12,10,20,.92),rgba(6,5,12,.94));\n  --t-card-border:rgba(139,92,246,.5);\n  --line-strong:rgba(139,92,246,.45); --deep:#5b21b6; --mid:#7c3aed;\n  --light:#8b5cf6; --pale:#a78bfa; color-scheme:dark;\n  --t-body-before-top:rgba(139,92,246,.16); --t-body-before-bot:rgba(46,16,101,.38);\n  --rev-glow-1:rgba(139,92,246,.18); --rev-glow-2:rgba(167,139,250,.12);\n  --rev-glow-3:rgba(59,7,100,.22);\n}\n\n/* -- New theme: Ocean Aqua (dark, teal) -- */\n[data-theme="aqua"]{\n  --bg:#03141b; --surface:#051e28; --surface-2:#07242f;\n  --line:rgba(34,211,238,.22); --line-2:#22d3ee;\n  --text:#eafcff; --muted:#a3c6d1;\n  --accent:#22d3ee; --accent-2:#67e8f9; --accent-soft:#062430;\n  --t-glyph:#04141b; --t-veil-grid:rgba(34,211,238,.12);\n  --t-veil-top:rgba(34,211,238,.08); --t-veil-bot:rgba(4,26,35,.5);\n  --t-spot-grid:rgba(165,243,252,.14);\n  --t-card-bg:linear-gradient(180deg,rgba(4,22,30,.92),rgba(2,14,19,.94));\n  --t-card-border:rgba(34,211,238,.45);\n  --line-strong:rgba(34,211,238,.42); --deep:#0e7490; --mid:#0891b2;\n  --light:#22d3ee; --pale:#67e8f9; color-scheme:dark;\n  --t-body-before-top:rgba(34,211,238,.14); --t-body-before-bot:rgba(8,74,96,.4);\n  --rev-glow-1:rgba(34,211,238,.14); --rev-glow-2:rgba(103,232,249,.1);\n  --rev-glow-3:rgba(8,145,178,.2);\n}\n\n/* -- New theme: Ember Glow (dark, warm amber) -- */\n[data-theme="ember"]{\n  --bg:#120a05; --surface:#1a0f07; --surface-2:#21140a;\n  --line:rgba(245,158,11,.22); --line-2:#f59e0b;\n  --text:#fdf3e7; --muted:#d0b8a0;\n  --accent:#f59e0b; --accent-2:#fbbf24; --accent-soft:#2a1a06;\n  --t-glyph:#170d05; --t-veil-grid:rgba(245,158,11,.1);\n  --t-veil-top:rgba(245,158,11,.07); --t-veil-bot:rgba(28,14,5,.5);\n  --t-spot-grid:rgba(253,230,138,.12);\n  --t-card-bg:linear-gradient(180deg,rgba(26,15,7,.92),rgba(16,9,4,.94));\n  --t-card-border:rgba(245,158,11,.45);\n  --line-strong:rgba(245,158,11,.42); --deep:#b45309; --mid:#d97706;\n  --light:#f59e0b; --pale:#fbbf24; color-scheme:dark;\n  --t-body-before-top:rgba(245,158,11,.12); --t-body-before-bot:rgba(120,53,15,.35);\n  --rev-glow-1:rgba(245,158,11,.14); --rev-glow-2:rgba(251,191,36,.1);\n  --rev-glow-3:rgba(180,83,9,.2);\n}\n\n/* -- Aurora glow colors for the original four themes -- */\n[data-theme="dark"]{\n  --rev-glow-1:rgba(61,135,255,.14); --rev-glow-2:rgba(91,157,255,.1);\n  --rev-glow-3:rgba(26,74,176,.18);\n}\n[data-theme="light"]{\n  --rev-glow-1:rgba(61,135,255,.16); --rev-glow-2:rgba(147,197,253,.2);\n  --rev-glow-3:rgba(196,181,253,.16);\n}\n[data-theme="vibrant"]{\n  --rev-glow-1:rgba(168,85,247,.18); --rev-glow-2:rgba(236,72,153,.14);\n  --rev-glow-3:rgba(124,58,237,.18);\n}\n[data-theme="blossom"]{\n  --rev-glow-1:rgba(244,114,182,.18); --rev-glow-2:rgba(251,207,232,.22);\n  --rev-glow-3:rgba(196,181,253,.18);\n}\n\n/* -- Typography & tokens -- */\n:root{\n  --sans:\'Inter\',\'SF Pro Text\',-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,sans-serif;\n  --display:\'Space Grotesk\',\'SF Pro Display\',-apple-system,BlinkMacSystemFont,sans-serif;\n  --r:16px; --r-frame:22px;\n}\nbody{-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}\nh1,h2,h3,h4,.wordmark,.logo,.tagline,.subtitle{font-family:var(--display);letter-spacing:-.02em}\n\n/* -- Soft aurora backdrop (static: no blur filter, no animation --\n   keeps phones cool and video playback smooth) -- */\nbody::before{\n  content:"";position:fixed;inset:-20%;z-index:0;pointer-events:none;\n  background:\n    radial-gradient(36% 30% at 18% 12%,var(--rev-glow-1),transparent 70%),\n    radial-gradient(32% 28% at 82% 20%,var(--rev-glow-2),transparent 72%),\n    radial-gradient(38% 34% at 50% 94%,var(--rev-glow-3),transparent 76%);\n  opacity:.8;\n}\n\n/* -- Glass chrome -- */\n.topbar{ border-bottom:1px solid var(--line); }\n@media (hover:hover){\n  .topbar{\n    background:color-mix(in srgb,var(--surface) 74%,transparent);\n    -webkit-backdrop-filter:blur(16px) saturate(1.25);\n    backdrop-filter:blur(16px) saturate(1.25);\n  }\n  @supports not (background:color-mix(in srgb,red 50%,blue)){\n    .topbar{background:var(--surface)}\n  }\n}\n@media (hover:none){\n  .topbar{background:var(--surface)}\n}\n\n/* -- Cinematic player frame -- */\nvideo-player{\n  display:block;border-radius:var(--r-frame,22px);overflow:hidden;\n}\nvideo-player,media-controller{\n  box-shadow:0 34px 90px -24px rgba(0,0,0,.7),0 0 0 1px var(--line),0 0 110px -36px var(--accent);\n}\nvideo#player{border-radius:inherit;background:#000}\n\n/* -- Buttons -- */\n.btn,.button{\n  border-radius:999px;font-weight:600;\n  transition:transform .18s ease,box-shadow .18s ease,filter .18s ease;\n}\n.btn:hover,.button:hover{transform:translateY(-1px)}\n.btn:active,.button:active{transform:translateY(0)}\n.btn.primary{\n  background:linear-gradient(135deg,var(--accent),var(--accent-2));\n  border:1px solid color-mix(in srgb,var(--accent) 55%,transparent);\n  box-shadow:0 8px 24px -8px var(--accent);\n}\n.btn.primary:hover{box-shadow:0 12px 30px -8px var(--accent-2);filter:saturate(1.12)}\n\n/* -- Info cards -- */\n.info-row{border-radius:12px;transition:background .16s ease}\n.info-row:hover{background:color-mix(in srgb,var(--accent) 8%,transparent)}\n.key{letter-spacing:.05em}\n\n/* -- Theme switcher polish + new swatches -- */\n.theme-panel{border-radius:14px;box-shadow:0 18px 50px -12px rgba(0,0,0,.55)}\n.theme-option{border-radius:10px}\n.theme-option:hover{background:color-mix(in srgb,var(--accent) 13%,transparent)}\n.sw-onyx{background:linear-gradient(135deg,#0b0b12,#8b5cf6)}\n.sw-aqua{background:linear-gradient(135deg,#04202e,#22d3ee)}\n.sw-ember{background:linear-gradient(135deg,#1c0e05,#f59e0b)}\n\n/* -- Toasts / stall box -- */\n.toast,.stall-box{border-radius:12px;box-shadow:0 14px 40px -10px rgba(0,0,0,.55)}\n\n/* -- Inputs -- */\ninput[type=text],input[type=password],input[type=search],select{\n  border-radius:10px;border:1px solid var(--line);background:var(--surface-2);\n  color:var(--text);transition:border-color .15s ease;\n}\ninput:focus{border-color:var(--accent-2)}\n\n/* -- Scrollbars & selection -- */\n*{scrollbar-width:thin;scrollbar-color:var(--line-2) transparent}\n::-webkit-scrollbar{width:8px;height:8px}\n::-webkit-scrollbar-thumb{background:var(--line-2);border-radius:8px}\n::-webkit-scrollbar-track{background:transparent}\n::selection{background:var(--accent);color:#fff}\n\n/* -- Focus rings -- */\n:focus-visible{outline:2px solid var(--accent-2);outline-offset:2px}\n\n/* -- Wordmark gradient -- */\n.wordmark,.logo{letter-spacing:-.02em}\n' + '\n</style>'

REVAMP_JS = '<script>\n' + "(function(){\n  function ready(fn){\n    if(document.readyState !== 'loading'){ fn(); }\n    else{ document.addEventListener('DOMContentLoaded', fn); }\n  }\n  ready(function(){\n    var panel = document.getElementById('themePanel');\n    if(!panel || panel.getAttribute('data-revamp') === '1'){ return; }\n    panel.setAttribute('data-revamp', '1');\n    var themes = [\n      {id:'onyx', label:'Onyx Noir', color:'#8b5cf6'},\n      {id:'aqua', label:'Ocean Aqua', color:'#22d3ee'},\n      {id:'ember', label:'Ember Glow', color:'#f59e0b'}\n    ];\n    themes.forEach(function(t){\n      var b = document.createElement('button');\n      b.className = 'theme-option';\n      b.dataset.t = t.id;\n      var s = document.createElement('span');\n      s.className = 'theme-swatch sw-' + t.id;\n      b.appendChild(s);\n      b.appendChild(document.createTextNode(' ' + t.label));\n      b.addEventListener('click', function(){\n        document.documentElement.setAttribute('data-theme', t.id);\n        try{ localStorage.setItem('wzml-theme', t.id); }catch(e){}\n        document.querySelectorAll('.theme-option').forEach(function(o){\n          o.classList.toggle('active', o.dataset.t === t.id);\n        });\n        var btn = document.getElementById('themeBtn');\n        if(btn){ btn.style.background = t.color; btn.style.borderColor = t.color; }\n      });\n      panel.appendChild(b);\n    });\n  });\n})();\n" + '\n</script>'

PLAYER_FIX_CSS = '<style id="wzml-playerfix-style">\n' + '/* == WZML-X v15.2 — unified control bar == */\n#wzfixBar{\n  display:flex;flex-wrap:wrap;gap:8px;justify-content:center;align-items:center;\n  margin-top:12px;padding:10px 12px;border-radius:14px;\n  background:var(--t-card-bg,linear-gradient(180deg,rgba(1,1,22,.9),rgba(0,0,12,.92)));\n  border:1px solid var(--t-card-border,var(--line));\n  max-width:min(92vw,760px);\n}\n#wzfixBar .fx{\n  appearance:none;border:1px solid var(--line);border-radius:999px;\n  background:var(--surface-2,#07070F);color:var(--text,#F5F7FF);\n  font:600 13px/1 var(--sans,inherit);padding:9px 14px;cursor:pointer;\n  transition:transform .15s ease,box-shadow .15s ease,border-color .15s ease;\n}\n#wzfixBar .fx:hover{\n  transform:translateY(-1px);border-color:var(--accent-2,var(--accent));\n  box-shadow:0 6px 18px -8px var(--accent);\n}\n#wzfixBar .fx:active{transform:translateY(0)}\n#wzfixBar .fx.primary{\n  background:linear-gradient(135deg,var(--accent,#3D87FF),var(--accent-2,#5B9DFF));\n  border-color:transparent;color:#fff;\n}\n#wzfixBar .fx.on{\n  border-color:var(--accent);color:var(--accent-2,var(--accent));\n  box-shadow:0 0 0 1px var(--accent) inset;\n}\n#wzfixBar .volwrap{\n  display:flex;align-items:center;gap:8px;padding:4px 12px;\n  border:1px solid var(--line);border-radius:999px;\n}\n#wzfixBar input[type=range]{\n  width:110px;accent-color:var(--accent,#3D87FF);\n}\n#wzfixBar .volpct{\n  font:600 12px/1 var(--mono,monospace);color:var(--muted,#C3CBDF);\n  min-width:3.2em;text-align:right;\n}\n#wzfixQr{\n  display:none;position:fixed;inset:0;z-index:80;place-items:center;\n  background:rgba(0,0,3,.7);\n}\n#wzfixQr.open{display:grid}\n#wzfixQr .box{\n  background:var(--surface,#04040B);border:1px solid var(--line);\n  border-radius:16px;padding:18px;text-align:center;\n}\n#wzfixQr img{border-radius:10px;background:#fff;padding:8px}\n#wzfixQr p{color:var(--muted,#C3CBDF);font-size:12px;margin:10px 0 0}\n#wzfixQr button{\n  margin-top:12px;border:1px solid var(--line);border-radius:999px;\n  background:var(--surface-2);color:var(--text);padding:8px 18px;cursor:pointer;\n}\n#wzfixToast{\n  position:fixed;left:50%;bottom:26px;transform:translateX(-50%) translateY(12px);\n  background:var(--surface,#04040B);color:var(--text,#F5F7FF);\n  border:1px solid var(--line);border-radius:12px;padding:10px 16px;\n  font-size:13px;opacity:0;pointer-events:none;z-index:90;\n  transition:opacity .2s ease,transform .2s ease;\n  box-shadow:0 12px 34px -10px rgba(0,0,0,.6);\n}\n#wzfixToast.show{opacity:1;transform:translateX(-50%) translateY(0)}\n' + '\n</style>'

PLAYER_FIX_JS = '<script>\n(function(){\n  "use strict";\n\n  // Resolve the ACTIVE player at call time. When a file cannot be played\n  // natively (MKV/HEVC etc.) the template replaces <video id="player"> with a\n  // <libmedia-video> WebCodecs element. Everything below resolves the player\n  // dynamically so the controls keep working after that swap.\n  function P(){\n    return document.querySelector("libmedia-video")\n        || document.getElementById("player");\n  }\n  function isNative(){ var p = P(); return !!p && p.tagName === "VIDEO"; }\n\n  function ready(fn){\n    if(document.readyState !== "loading"){ fn(); }\n    else{ document.addEventListener("DOMContentLoaded", fn); }\n  }\n\n  // ── mini toast ──\n  var tEl = null, tTimer = null;\n  function toast(msg){\n    if(!tEl){\n      tEl = document.createElement("div"); tEl.id = "wzfixToast";\n      document.body.appendChild(tEl);\n    }\n    tEl.textContent = msg; tEl.classList.add("show");\n    clearTimeout(tTimer);\n    tTimer = setTimeout(function(){ tEl.classList.remove("show"); }, 1900);\n  }\n\n  // ── stall-overlay suppressor ──────────────────────────────────────────\n  // The template\'s stall prompt (Wait a bit more / Retry) is driven by\n  // waiting/playing events; with the advanced decoder + a slow link those\n  // fire even while frames are actually advancing, so the banner used to\n  // cover a playing video. Whenever playback time is really progressing,\n  // drop the stalled class — a genuine stall (no advancing time) still\n  // shows it. Capture-phase listeners on document catch events from BOTH\n  // the native video and a later-swapped libmedia element.\n  var lastEl = null, lastT = -1;\n  document.addEventListener("timeupdate", function(e){\n    var t = e.target;\n    if(!t || typeof t.currentTime !== "number"){ return; }\n    var fresh = (t !== lastEl);\n    var advancing = fresh ? true : (t.currentTime > lastT + 0.05);\n    lastEl = t; lastT = t.currentTime;\n    if(advancing || !t.paused){\n      var f = document.getElementById("frame");\n      if(f && f.classList.contains("stalled")){ f.classList.remove("stalled"); }\n    }\n  }, true);\n  document.addEventListener("playing", function(){\n    var f = document.getElementById("frame");\n    if(f){ f.classList.remove("stalled"); }\n  }, true);\n\n  // ── control actions (all player-agnostic) ─────────────────────────────\n  var SPEEDS = [0.5, 1, 1.5, 2], spIdx = 1;\n\n  function cycleSpeed(){\n    var p = P(); if(!p){ return; }\n    spIdx = (spIdx + 1) % SPEEDS.length;\n    try{ p.playbackRate = SPEEDS[spIdx]; }catch(err){}\n    toast("Speed: " + SPEEDS[spIdx] + "x");\n  }\n  function skip(sec){\n    var p = P(); if(!p){ return; }\n    try{\n      var d = (isFinite(p.duration) && p.duration > 0) ? p.duration : Infinity;\n      p.currentTime = Math.max(0, Math.min(d, (p.currentTime || 0) + sec));\n    }catch(err){ toast("Seek not available"); }\n  }\n  function snapshot(){\n    var p = P(); if(!p){ return; }\n    if(!isNative()){ toast("Snapshot: not available for this format"); return; }\n    try{\n      var c = document.createElement("canvas");\n      c.width = p.videoWidth || 640; c.height = p.videoHeight || 360;\n      c.getContext("2d").drawImage(p, 0, 0, c.width, c.height);\n      c.toBlob(function(blob){\n        if(!blob){ toast("Snapshot failed"); return; }\n        var url = URL.createObjectURL(blob);\n        var a = document.createElement("a");\n        a.href = url; a.download = "snapshot-" + Date.now() + ".png";\n        document.body.appendChild(a); a.click(); document.body.removeChild(a);\n        setTimeout(function(){ URL.revokeObjectURL(url); }, 1200);\n        toast("Snapshot saved");\n      }, "image/png");\n    }catch(err){ toast("Snapshot failed"); }\n  }\n  function copyLink(){\n    var url = location.href;\n    if(navigator.clipboard && navigator.clipboard.writeText){\n      navigator.clipboard.writeText(url).then(\n        function(){ toast("Link copied!"); },\n        function(){ fallbackCopy(url); });\n    } else { fallbackCopy(url); }\n  }\n  function fallbackCopy(url){\n    var ta = document.createElement("textarea");\n    ta.value = url; ta.style.cssText = "position:fixed;opacity:0";\n    document.body.appendChild(ta); ta.select();\n    try{ document.execCommand("copy"); toast("Link copied!"); }\n    catch(err){ toast("Copy failed"); }\n    document.body.removeChild(ta);\n  }\n  function shareLink(){\n    if(navigator.share){\n      navigator.share({ title: document.title || "Stream", url: location.href })\n        .catch(function(){});\n    } else { copyLink(); }\n  }\n  function toggleMute(){\n    var p = P(); if(!p){ return; }\n    try{\n      p.muted = !p.muted;\n      toast(p.muted ? "Muted" : "Unmuted");\n      syncMute();\n    }catch(err){ toast("Mute not available"); }\n  }\n  function toggleLoop(btn){\n    var p = P(); if(!p){ return; }\n    try{\n      p.loop = !p.loop;\n      if(p.loop !== btn.classList.contains("on")){ p.loop = !p.loop; toast("Loop not available"); return; }\n      btn.classList.toggle("on", !!p.loop);\n      toast(p.loop ? "Loop on" : "Loop off");\n    }catch(err){ toast("Loop not available"); }\n  }\n  function togglePip(){\n    var p = P();\n    if(!p || p.tagName !== "VIDEO" || !p.requestPictureInPicture){\n      toast("PiP: not available for this format"); return;\n    }\n    if(document.pictureInPictureElement){ document.exitPictureInPicture(); }\n    else{ p.requestPictureInPicture().catch(function(){ toast("PiP failed"); }); }\n  }\n  function toggleFull(){\n    var target = document.getElementById("frame") || P();\n    if(!target){ return; }\n    if(document.fullscreenElement){ document.exitFullscreen(); }\n    else if(target.requestFullscreen){ target.requestFullscreen().catch(function(){}); }\n    else if(target.webkitRequestFullscreen){ target.webkitRequestFullscreen(); }\n  }\n  function toggleTheater(btn){\n    document.body.classList.toggle("wzml-theater");\n    btn.classList.toggle("on", document.body.classList.contains("wzml-theater"));\n  }\n\n  // ── QR modal (reuses the kit\'s one when present) ──\n  var qrModal = document.getElementById("qrModal");\n  var qrCanvas = document.getElementById("qrCanvas");\n  var myQr = null;\n  function ensureQr(){\n    if(qrModal && qrCanvas){ return; }\n    if(myQr){ return; }\n    myQr = document.createElement("div");\n    myQr.id = "wzfixQr";\n    myQr.innerHTML = \'<div class="box"><img alt="QR"><p>Scan to open this stream</p><button>Close</button></div>\';\n    document.body.appendChild(myQr);\n    myQr.querySelector("button").onclick = function(){ myQr.classList.remove("open"); };\n    myQr.addEventListener("click", function(e){ if(e.target === myQr){ myQr.classList.remove("open"); } });\n    qrModal = myQr; qrCanvas = myQr.querySelector("img");\n  }\n  function showQr(){\n    ensureQr();\n    var url = "https://api.qrserver.com/v1/create-qr-code/?size=200x200&data="\n      + encodeURIComponent(location.href);\n    if(qrCanvas.tagName === "IMG"){\n      qrCanvas.src = url;\n    } else {\n      qrCanvas.innerHTML = "";\n      var img = document.createElement("img");\n      img.width = 200; img.height = 200; img.alt = "QR code";\n      img.src = url;\n      qrCanvas.appendChild(img);\n    }\n    qrModal.classList.add("open");\n  }\n\n  // ── volume sync ──\n  function syncMute(){\n    var p = P();\n    var b = document.getElementById("fxMute");\n    var s = document.getElementById("fxVol");\n    if(p && s){ try{ s.value = Math.round((p.volume || 0) * 100); }catch(err){} }\n    if(b && p){ try{ b.classList.toggle("on", !!p.muted); }catch(err){} }\n  }\n  document.addEventListener("volumechange", syncMute, true);\n\n  // ── build the bar ──\n  function buildBar(){\n    var root = document.getElementById("root");\n    if(!root || document.getElementById("wzfixBar")){ return; }\n\n    var bar = document.createElement("div");\n    bar.id = "wzfixBar";\n\n    function btn(label, title, fn){\n      var b = document.createElement("button");\n      b.className = "fx"; b.type = "button";\n      b.textContent = label; b.title = title;\n      b.addEventListener("click", fn);\n      bar.appendChild(b);\n      return b;\n    }\n\n    btn("⏪ 10s", "Back 10 seconds", function(){ skip(-10); });\n    var sp = btn("1x", "Playback speed (S)", cycleSpeed); sp.classList.add("primary");\n    btn("10s ⏩", "Forward 10 seconds", function(){ skip(10); });\n\n    var mute = btn("🔊", "Mute (M)", toggleMute); mute.id = "fxMute";\n\n    var wrap = document.createElement("div");\n    wrap.className = "volwrap";\n    var slider = document.createElement("input");\n    slider.type = "range"; slider.min = "0"; slider.max = "100"; slider.value = "100";\n    slider.id = "fxVol"; slider.title = "Volume";\n    slider.addEventListener("input", function(){\n      var p = P(); if(!p){ return; }\n      try{ p.volume = slider.value / 100; }catch(err){}\n    });\n    var pct = document.createElement("span");\n    pct.className = "volpct"; pct.textContent = "100%";\n    slider.addEventListener("input", function(){ pct.textContent = slider.value + "%"; });\n    wrap.appendChild(slider); wrap.appendChild(pct);\n    bar.appendChild(wrap);\n\n    btn("🖼 Snapshot", "Save current frame (D)", snapshot);\n    btn("🔗 Copy", "Copy stream link (C)", copyLink);\n    btn("📱 QR", "Show QR code", showQr);\n    btn("↗ Share", "Share link", shareLink);\n    btn("⧉ PiP", "Picture in picture", togglePip);\n    btn("⛶ Full", "Fullscreen (F)", toggleFull);\n    var theater = btn("🎭 Theater", "Theater mode", function(){ toggleTheater(theater); });\n    var loop = btn("🔁 Loop", "Loop playback", function(){ toggleLoop(loop); });\n\n    root.appendChild(bar);\n    syncMute();\n\n    // hide the kit\'s broken bar (its buttons were bound to the original\n    // <video> element, which gets replaced by the advanced decoder)\n    var old = document.getElementById("extrasBar");\n    if(old){ old.style.display = "none"; }\n    console.log("[wzfix] unified control bar ready");\n  }\n\n  // keep the speed button label in sync across swaps\n  document.addEventListener("ratechange", function(){\n    var p = P(); if(!p){ return; }\n    var lbl = barSpeedLabel();\n    if(lbl){ try{ lbl.textContent = (p.playbackRate || 1) + "x"; }catch(err){} }\n  }, true);\n  function barSpeedLabel(){\n    var bar = document.getElementById("wzfixBar");\n    return bar ? bar.querySelectorAll(".fx")[1] : null;\n  }\n\n  // ── keyboard shortcuts (player-agnostic) ──\n  document.addEventListener("keydown", function(e){\n    if(e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA"){ return; }\n    var p = P(); if(!p){ return; }\n    var k = e.key;\n    try{\n      if(k === " " || k === "k"){\n        e.preventDefault();\n        if(p.paused){ p.play(); } else { p.pause(); }\n      } else if(k === "ArrowLeft"){ e.preventDefault(); skip(-10); }\n      else if(k === "ArrowRight"){ e.preventDefault(); skip(10); }\n      else if(k === "ArrowUp"){ e.preventDefault(); p.volume = Math.min(1, p.volume + .1); syncMute(); }\n      else if(k === "ArrowDown"){ e.preventDefault(); p.volume = Math.max(0, p.volume - .1); syncMute(); }\n      else if(k === "m"){ e.preventDefault(); toggleMute(); }\n      else if(k === "f"){ e.preventDefault(); toggleFull(); }\n      else if(k === "s"){ e.preventDefault(); cycleSpeed(); }\n      else if(k === "d"){ e.preventDefault(); snapshot(); }\n      else if(k === "c"){ e.preventDefault(); copyLink(); }\n    }catch(err){}\n  });\n\n  ready(function(){ setTimeout(buildBar, 350); });\n})();\n\n\n\n// ── stream watchdog (v15.5): the player\'s own view of the data request ──\n// If playback has not started ~22s in, fetch a tiny range from the exact\n// URL the player is using and show the result — status, bytes, latency.\n// This runs alongside the server\'s KSTREAM logs and names the failing layer.\n(function(){\n  function diag(text){\n    try{ console.log(\'[wzfix]\', text); }catch(e){}\n    var d = document.createElement(\'div\');\n    d.id = \'wzfixDiag\';\n    d.textContent = text;\n    d.style.cssText = \'position:fixed;left:12px;bottom:64px;z-index:95;\'\n      + \'background:rgba(4,6,12,.94);color:#e8ecf7;\'\n      + \'border:1px solid rgba(93,157,255,.45);border-radius:10px;\'\n      + \'padding:10px 14px;font:12px/1.5 system-ui,sans-serif;max-width:82vw\';\n    var old = document.getElementById(\'wzfixDiag\');\n    if(old){ old.remove(); }\n    (document.body || document.documentElement).appendChild(d);\n    setTimeout(function(){ if(d.parentNode){ d.remove(); } }, 45000);\n  }\n  function check(){\n    var p = document.querySelector(\'libmedia-video\') || document.getElementById(\'player\');\n    if(!p){ return; }\n    var playing = false;\n    try{ playing = !p.paused && p.currentTime > 0.5; }catch(e){}\n    if(playing){ return; }\n    var url = \'\';\n    try{ url = p.currentSrc || p.src || \'\'; }catch(e){}\n    if(!url || url.indexOf(\'/stream/\') === -1){\n      url = location.origin + \'/stream/\'\n        + encodeURIComponent(location.pathname.split(\'/\').pop() || \'\')\n        + location.search;\n    }\n    var t0 = Date.now();\n    fetch(url, { headers: { Range: \'bytes=0-1023\' }, cache: \'no-store\' })\n      .then(function(r){\n        return r.arrayBuffer().then(function(b){\n          diag(\'Stream check: HTTP \' + r.status + \' · \'\n            + b.byteLength + \' bytes · \' + (Date.now() - t0) + \'ms\'\n            + \' · \' + new Date().toLocaleTimeString());\n        });\n      })\n      .catch(function(e){\n        diag(\'Stream check FAILED: \'\n          + (e && e.message ? e.message : String(e))\n          + \' · \' + new Date().toLocaleTimeString());\n      });\n  }\n  function ready(fn){\n    if(document.readyState !== \'loading\'){ fn(); }\n    else{ document.addEventListener(\'DOMContentLoaded\', fn); }\n  }\n  ready(function(){\n    setTimeout(check, 22000);\n    setTimeout(check, 60000);\n  });\n})();\n\n</script>'


def apply_sed_patches():
    """
    Apply the two inline sed patches from the original workflow:

    Patch 1 (yt_dlp): broader exception catching + socket timeout in
        yt_dlp_download.py. The _download method only catches DownloadError;
        we broaden it to catch Exception and add a socket timeout.

    Patch 2 (broadcast): change `for uid in await database.get_pm_uids():`
        to `for uid in (await database.get_pm_uids() or []):` as a
        belt-and-suspenders fix alongside patch_db.py.
    """
    log("=" * 60)
    log("Applying inline sed patches")
    log("=" * 60)

    # --- Patch 1: yt_dlp_download.py — broader exception + socket timeout ---
    ytdlp_path = os.path.join(
        WZMLX_DIR,
        "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py",
    )
    if os.path.isfile(ytdlp_path):
        with open(ytdlp_path, "r", encoding="utf-8") as f:
            content = f.read()
        modified = False

        # Broaden the DownloadError catch to also catch Exception
        old_dl = (
            "                try:\n"
            "                    ydl.download([self._listener.link])\n"
            "                except DownloadError as e:\n"
            "                    if not self._listener.is_cancelled:\n"
            "                        self._on_download_error(str(e))\n"
            "                    return"
        )
        new_dl = (
            "                try:\n"
            "                    ydl.download([self._listener.link])\n"
            "                except (DownloadError, Exception) as e:\n"
            "                    if not self._listener.is_cancelled:\n"
            "                        self._on_download_error(str(e))\n"
            "                    return"
        )
        if old_dl in content:
            content = content.replace(old_dl, new_dl, 1)
            modified = True
            log("  sed yt_dlp: broadened DownloadError -> (DownloadError, Exception)")
        else:
            log("  sed yt_dlp: DownloadError target not found (already patched?)", "WARN")

        # Add socket timeout to extract_info call
        old_extract = (
            "            try:\n"
            "                result = ydl.extract_info(self._listener.link, download=False)\n"
            "                if result is None:\n"
            '                    raise ValueError("Info result is None")\n'
            "            except Exception as e:\n"
            "                return self._on_download_error(str(e))"
        )
        new_extract = (
            "            try:\n"
            "                import socket\n"
            "                old_timeout = socket.getdefaulttimeout()\n"
            "                socket.setdefaulttimeout(120)\n"
            "                result = ydl.extract_info(self._listener.link, download=False)\n"
            "                socket.setdefaulttimeout(old_timeout)\n"
            "                if result is None:\n"
            '                    raise ValueError("Info result is None")\n'
            "            except Exception as e:\n"
            "                return self._on_download_error(str(e))"
        )
        if old_extract in content:
            content = content.replace(old_extract, new_extract, 1)
            modified = True
            log("  sed yt_dlp: added 120s socket timeout to extract_info")
        else:
            log("  sed yt_dlp: extract_info target not found (already patched?)", "WARN")

        if modified:
            with open(ytdlp_path, "w", encoding="utf-8") as f:
                f.write(content)
    else:
        log(f"  sed yt_dlp: file not found — {ytdlp_path}", "ERROR")

    # --- Patch 2: broadcast.py — `for uid in (await ... or []):` ---
    bc_path = os.path.join(WZMLX_DIR, "bot/modules/broadcast.py")
    if os.path.isfile(bc_path):
        with open(bc_path, "r", encoding="utf-8") as f:
            content = f.read()
        old_bc = "    for uid in await database.get_pm_uids():"
        new_bc = "    for uid in (await database.get_pm_uids() or []):"
        if old_bc in content:
            content = content.replace(old_bc, new_bc, 1)
            with open(bc_path, "w", encoding="utf-8") as f:
                f.write(content)
            log("  sed broadcast: patched get_pm_uids iteration with `or []` guard")
        else:
            log("  sed broadcast: target not found (already patched?)", "WARN")
    else:
        log(f"  sed broadcast: file not found — {bc_path}", "ERROR")

    log("Inline sed patches complete")


# ============================================================================
# SECTION 6 — SYSTEM PACKAGES & PYTHON DEPS
# ============================================================================

def _port_open(host, port, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def setup_wzml_services(config):
    """
    Provide what the WZML-X Docker base image normally supplies:

    1. `wz_bin` module: config_manager does `from wz_bin import bin_name`.
       In Docker it is a compiled helper baked into the base image
       (mysterysd/wzmlx:wzadv). On Kaggle we write a pure-Python shim
       with the standard binary names.
    2. An aria2c RPC daemon on localhost:6800 (same flags as the
       upstream setpkgs.sh). Without it TorrentManager.initiate()
       raises and the bot exits.
    3. qbittorrent-nox availability check (spawned via BinConfig.QBIT_NAME;
       its profile comes from the repo's configs/qbittorrent/).
    """
    log("=" * 60)
    log("Setting up WZML-X service prerequisites")
    log("=" * 60)

    # (a) wz_bin shim
    shim_path = os.path.join(WZMLX_DIR, "wz_bin.py")
    try:
        with open(shim_path, "w") as f:
            f.write(
                "def bin_name(i):\n"
                '    names = ["aria2c", "qbittorrent-nox", "ffmpeg", "rclone", "sabnzbd"]\n'
                "    return names[i] if isinstance(i, int) and 0 <= i < len(names) else names[0]\n"
            )
        log("wz_bin.py shim written (aria2c/qbittorrent-nox/ffmpeg/rclone/sabnzbd)")
    except Exception as e:
        log(f"Failed to write wz_bin shim: {e}", "ERROR")

    # (a2) mega SDK stub module
    # `bot/.../mega_upload.py` and `mega_listener.py` import the compiled
    # MEGA SDK bindings (`from mega import MegaApi, ...`) unconditionally at
    # module load time. The SDK is baked into the Docker base image but has no
    # prebuilt wheel on PyPI (building it needs swig + 15+ min). The bot's own
    # code is already defensive (checks `MegaCancelToken is None`, and
    # add_mega_upload bails out unless MEGA credentials are configured), so a
    # stub module is safe: it satisfies the imports and only raises if someone
    # actually attempts a MEGA transfer.
    try:
        mega_pkg_dir = os.path.join(WZMLX_DIR, "mega")
        os.makedirs(mega_pkg_dir, exist_ok=True)
        with open(os.path.join(mega_pkg_dir, "__init__.py"), "w") as f:
            f.write(
                '''"""Stub for the compiled MEGA SDK Python bindings.

The real module is only present in the official Docker image.
This stub satisfies import-time usage; actual MEGA transfers
are disabled in this deployment.
"""

class _MegaUnavailable:
    def __init__(self, *args, **kwargs):
        raise RuntimeError(
            "MEGA SDK is not available in this deployment; "
            "MEGA transfers are disabled."
        )

class MegaApi(_MegaUnavailable):
    pass

class MegaCancelToken:
    @staticmethod
    def createInstance():
        return None

class MegaError(Exception):
    pass

class MegaListener:
    pass

class MegaRequest:
    pass

class MegaTransfer:
    pass

class MegaUploadOptions:
    PATH = 0
''')
        log("mega SDK stub module written (MEGA transfers disabled)")
    except Exception as e:
        log(f"Failed to write mega stub: {e}", "ERROR")

    # (a3) Install both shims into site-packages as well.
    # The bot's self-restart (/restart command, private-file updates)
    # spawns `python -m bot` with a working directory where the WZML-X
    # folder is not importable, so `from wz_bin import bin_name` crashed
    # the restarted process. Site-packages makes the shims global.
    try:
        import site as _site
        _sp_dir = _site.getsitepackages()[0]
        if os.path.isfile(shim_path):
            shutil.copy2(shim_path, os.path.join(_sp_dir, "wz_bin.py"))
        if os.path.isdir(mega_pkg_dir):
            _sp_mega = os.path.join(_sp_dir, "mega")
            os.makedirs(_sp_mega, exist_ok=True)
            shutil.copy2(
                os.path.join(mega_pkg_dir, "__init__.py"),
                os.path.join(_sp_mega, "__init__.py"),
            )
        log(f"wz_bin + mega shims also installed into site-packages")
    except Exception as e:
        log(f"site-packages shim install failed (non-fatal): {e}", "WARN")

    # (a4) deno JavaScript runtime for yt-dlp (EJS challenge solving).
    # yt-dlp needs a JS runtime to solve YouTube signature/n challenges;
    # without it formats go missing and downloads die with
    # "The page needs to be reloaded". deno is a single static binary and
    # is the runtime yt-dlp looks for by default.
    try:
        _deno_ok = subprocess.run(
            ["deno", "--version"], capture_output=True, text=True
        )
        if _deno_ok.returncode == 0:
            log("deno already installed for yt-dlp challenges")
        else:
            raise FileNotFoundError("deno present but not runnable")
    except Exception:
        try:
            _deno_url = (
                "https://github.com/denoland/deno/releases/latest/download/"
                "deno-x86_64-unknown-linux-gnu.zip"
            )
            _req = urllib.request.Request(_deno_url)
            with urllib.request.urlopen(_req, timeout=120) as _resp:
                with open("/tmp/deno.zip", "wb") as _f:
                    while True:
                        _chunk = _resp.read(65536)
                        if not _chunk:
                            break
                        _f.write(_chunk)
            import zipfile as _zf
            with _zf.ZipFile("/tmp/deno.zip") as _z:
                _z.extractall("/tmp/deno_bin")
            _installed = False
            for _cand in ("/usr/local/bin", "/usr/bin",
                          os.path.expanduser("~/.local/bin")):
                if os.path.isdir(_cand) and os.access(_cand, os.W_OK):
                    shutil.copy2("/tmp/deno_bin/deno", os.path.join(_cand, "deno"))
                    os.chmod(os.path.join(_cand, "deno"), 0o755)
                    log(f"deno installed to {_cand} (yt-dlp JS challenges enabled)")
                    _installed = True
                    break
            if not _installed:
                log("no writable bin dir for deno", "WARN")
        except Exception as e:
            log(f"deno install failed (yt-dlp challenges may fail): {e}", "WARN")

    # (b) aria2c RPC daemon on :6800
    if _port_open("127.0.0.1", 6800):
        log("aria2c RPC daemon already listening on :6800")
    else:
        dl_dir = config.get("DOWNLOAD_DIR", "") or DOWNLOAD_DIR_DEFAULT
        if not dl_dir.endswith("/"):
            dl_dir += "/"
        cmd = [
            "aria2c",
            "--daemon=true",
            "--enable-rpc=true",
            "--rpc-listen-all=true",
            "--rpc-max-request-size=1024M",
            "--max-concurrent-downloads=1000",
            "--max-connection-per-server=16",
            "--split=16",
            "--min-split-size=32M",
            "--optimize-concurrent-downloads=true",
            "--continue=true",
            "--auto-file-renaming=true",
            "--allow-overwrite=true",
            "--force-save=false",
            "--content-disposition-default-utf8=true",
            "--user-agent=Wget/1.12",
            "--http-accept-gzip=true",
            "--max-tries=20",
            "--max-file-not-found=0",
            f"--dir={dl_dir}",
        ]
        try:
            trackers = subprocess.run(
                [
                    "curl", "-Ns", "--max-time", "20",
                    "https://cdn.jsdelivr.net/gh/ngosang/trackerslist@master/trackers_all.txt",
                ],
                capture_output=True, text=True, timeout=30,
            )
            if trackers.returncode == 0 and trackers.stdout.strip():
                tlist = ",".join(trackers.stdout.split())[:4000]
                if tlist:
                    cmd.append(f"--bt-tracker={tlist}")
        except Exception:
            pass
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            # aria2c daemonizes (forks) before binding the RPC port, so the
            # first port probe can race ahead of the daemon. Retry briefly.
            _up = False
            for _ in range(6):
                if _port_open("127.0.0.1", 6800):
                    _up = True
                    break
                time.sleep(1.0)
            if _up:
                log("aria2c RPC daemon started on :6800")
            else:
                msg = (r.stderr or r.stdout or "").strip()[:300]
                log(f"aria2c daemon not reachable after retries (rc={r.returncode}): {msg}", "WARN")
        except Exception as e:
            log(f"aria2c daemon failed to start: {e}", "ERROR")

    # (c) qbittorrent-nox check
    if shutil.which("qbittorrent-nox"):
        log("qbittorrent-nox available")
    else:
        log("qbittorrent-nox MISSING - qBittorrent downloads will fail", "WARN")


def install_system_packages():
    """
    Install system packages required by WZML-X on Kaggle.

    Kaggle runs Debian-based containers. We use apt-get with
    --no-install-recommends to keep the install lean.
    Packages: aria2, ffmpeg, mediainfo, p7zip-full, p7zip-rar, rar, unrar,
    zip, unzip, wget, curl, jq.
    """
    log("=" * 60)
    log("Installing system packages")
    log("=" * 60)

    packages = [
        "aria2", "ffmpeg", "mediainfo", "p7zip-full", "p7zip-rar",
        "rar", "unrar", "zip", "unzip", "wget", "curl", "jq",
        "qbittorrent-nox", "rclone",
    ]

    # Check which are already installed
    missing = []
    for pkg in packages:
        binary_map = {
            "p7zip-full": "7z", "p7zip-rar": "7z",
        }
        binary = binary_map.get(pkg, pkg)
        if shutil.which(binary):
            log(f"  {pkg}: already installed")
        else:
            missing.append(pkg)

    if not missing:
        log("All system packages already present")
        return

    # Attempt apt-get update
    try:
        subprocess.run(
            ["apt-get", "update", "-qq"],
            check=True, timeout=120, capture_output=True,
        )
    except Exception as e:
        log(f"apt-get update failed: {e} (trying conda fallback)", "WARN")

    # Install via apt-get
    install_cmd = ["apt-get", "install", "-y", "--no-install-recommends"] + missing
    try:
        result = subprocess.run(
            install_cmd, timeout=300, capture_output=True, text=True
        )
        if result.returncode == 0:
            log(f"Installed via apt-get: {', '.join(missing)}")
        else:
            log(f"apt-get install failed (code {result.returncode}), trying conda", "WARN")
            _install_via_conda(missing)
    except Exception as e:
        log(f"apt-get install failed: {e}, trying conda", "WARN")
        _install_via_conda(missing)

    # Verify critical binaries
    for pkg in ["ffmpeg", "aria2c", "7z", "jq", "qbittorrent-nox", "rclone"]:
        if shutil.which(pkg):
            log(f"  OK: {pkg} on PATH")
        else:
            log(f"  MISSING: {pkg} NOT on PATH", "WARN")


def _install_via_conda(packages):
    """Fallback: install packages via conda (Kaggle has conda)."""
    conda_map = {
        "aria2": "aria2", "ffmpeg": "ffmpeg", "mediainfo": "mediainfo",
        "p7zip-full": "p7zip", "p7zip-rar": "p7zip", "rar": "rar",
        "unrar": "unrar", "zip": "zip", "unzip": "unzip",
        "wget": "wget", "curl": "curl", "jq": "jq",
    }
    conda_pkgs = list(set(conda_map.get(p, p) for p in packages))
    try:
        subprocess.run(
            ["conda", "install", "-y", "-c", "conda-forge"] + conda_pkgs,
            timeout=300, capture_output=True, text=True, check=True,
        )
        log(f"Installed via conda: {', '.join(conda_pkgs)}")
    except Exception as e:
        log(f"conda install also failed: {e}", "ERROR")
        log("Some system packages may be missing — bot may not work fully", "WARN")


def install_python_deps():
    """Install Python dependencies from WZML-X requirements.txt."""
    log("=" * 60)
    log("Installing Python dependencies")
    log("=" * 60)

    req_path = os.path.join(WZMLX_DIR, "requirements.txt")
    if not os.path.isfile(req_path):
        log(f"requirements.txt not found at {req_path}", "ERROR")
        return

    try:
        if os.environ.get("WZFIX_WORKER") == "1":
            # WZFIX r25c: workers only need the upgraded yt-dlp —
            # the full bot requirements would add ~4 minutes to
            # every worker boot
            log("Worker session — skipping the full requirements install")
            result = None
        else:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "--no-input", "-r", req_path],
                timeout=600, capture_output=True, text=True,
            )
        if result is None or result.returncode == 0:
            log("Python dependencies installed successfully")
        else:
            log(f"pip install exited with code {result.returncode}", "WARN")
            stderr_lines = result.stderr.strip().split("\n")[-5:]
            for line in stderr_lines:
                log(f"  pip STDERR: {line}", "WARN")
    except subprocess.TimeoutExpired:
        log("pip install timed out (600s)", "ERROR")
    except Exception as e:
        log(f"pip install failed: {e}", "ERROR")

    # WZFIX r25c (v15.83): the leader pushes worker kernels through
    # the Kaggle API at batch time — the CLI must be present.
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "kaggle==1.6.17"],
            timeout=180,
            capture_output=True,
            text=True,
        )
        log("r25c: kaggle CLI ready (worker dispatch)")
    except Exception as e:
        log(f"r25c: kaggle CLI install failed: {e}", "WARN")

    # WZFIX r23 (v15.82): Kaggle preinstalls an old yt-dlp and
    # plain pip -r sees it as satisfied - force the upgrade.
    # Current yt-dlp + curl-cffi impersonation is the anti-429 stack.
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-input", "--upgrade",
             "yt-dlp[default,curl-cffi]"],
            timeout=600, capture_output=True, text=True,
        )
        import yt_dlp as _ytd23
        log(f"r23: yt-dlp now at {_ytd23.version.__version__}")
    except Exception as e:
        log(f"r23: yt-dlp upgrade failed: {e}", "WARN")
    # Ensure critical packages are importable
    critical = ["pyrogram", "aiohttp", "fastapi", "uvicorn", "pymongo", "yt_dlp"]
    for pkg in critical:
        try:
            __import__(pkg.replace("-", "_"))
            log(f"  OK: {pkg} importable")
        except ImportError:
            log(f"  MISSING: {pkg} NOT importable — installing individually", "WARN")
            try:
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "--no-input", pkg],
                    timeout=120, capture_output=True,
                )
            except Exception:
                pass


# ============================================================================
# SECTION 7 — CLOUDFLARE TUNNEL
# ============================================================================

def download_cloudflared():
    """Download the cloudflared binary to /kaggle/working/cloudflared."""
    log("Downloading cloudflared binary...")
    if os.path.isfile(CLOUDFLARED_BIN) and os.access(CLOUDFLARED_BIN, os.X_OK):
        log("cloudflared already downloaded")
        return True

    try:
        req = urllib.request.Request(CLOUDFLARED_URL)
        with urllib.request.urlopen(req, timeout=120) as resp:
            with open(CLOUDFLARED_BIN, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
        os.chmod(CLOUDFLARED_BIN, 0o755)
        log(f"cloudflared downloaded to {CLOUDFLARED_BIN}")
        return True
    except Exception as e:
        log(f"Failed to download cloudflared: {e}", "ERROR")
        # Try wget as fallback
        try:
            subprocess.run(
                ["wget", "-q", "-O", CLOUDFLARED_BIN, CLOUDFLARED_URL],
                timeout=120, check=True,
            )
            os.chmod(CLOUDFLARED_BIN, 0o755)
            log("cloudflared downloaded via wget fallback")
            return True
        except Exception as e2:
            log(f"wget fallback also failed: {e2}", "ERROR")
            return False


def start_cloudflared_tunnel(port=8080):
    """
    Start cloudflared quick tunnel on the given port.

    Runs `cloudflared tunnel --url http://localhost:{port} --no-autoupdate`
    as a subprocess. Captures the trycloudflare.com URL from stdout/stderr
    by regex. Waits up to 60 seconds for the URL to appear.

    Returns the tunnel URL string, or None on failure.
    """
    global TUNNEL_PROCESS
    log(f"Starting cloudflared quick tunnel on port {port}...")

    cmd = [
        CLOUDFLARED_BIN,
        "tunnel",
        "--url", f"http://localhost:{port}",
        "--no-autoupdate",
    ]

    # cloudflared prints the tunnel URL to stderr (its logs go there)
    TUNNEL_PROCESS = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    tunnel_url = None
    found_url = threading.Event()

    def read_stream(stream, label):
        nonlocal tunnel_url
        try:
            for line in stream:
                log(f"  cloudflared {label}: {line.rstrip()}", "DEBUG")
                match = TUNNEL_URL_RE.search(line)
                if match and not tunnel_url:
                    tunnel_url = match.group(0)
                    found_url.set()
                    return
        except Exception:
            pass

    stderr_thread = threading.Thread(target=read_stream, args=(TUNNEL_PROCESS.stderr, "stderr"), daemon=True)
    stdout_thread = threading.Thread(target=read_stream, args=(TUNNEL_PROCESS.stdout, "stdout"), daemon=True)
    stderr_thread.start()
    stdout_thread.start()

    # Wait for URL or timeout (60s)
    if found_url.wait(timeout=60):
        log(f"Tunnel URL captured: {tunnel_url}")
        return tunnel_url
    else:
        if TUNNEL_PROCESS.poll() is not None:
            log("cloudflared process exited prematurely", "ERROR")
        else:
            log("Timed out waiting for tunnel URL (60s)", "ERROR")
        return None


# ============================================================================
# SECTION 8 — CLOUDFLARE WORKER SYNC
# ============================================================================

def sync_to_worker(config, tunnel_url):
    """
    POST the tunnel URL to the Cloudflare Worker.

    Sends a POST request to {WORKER_URL}/update-tunnel?bot={BOT_ID} with
    header X-Tunnel-Secret: {WORKER_SECRET} and JSON body {"url": tunnel_url}.

    The Worker stores this URL and serves it as a stable redirect, so clients
    always connect to the same Worker URL regardless of which trycloudflare
    tunnel is active.

    Returns True on success, False on failure.
    """
    worker_url = config.get("WORKER_URL", "").strip().strip("/")
    worker_secret = config.get("WORKER_SECRET", "")
    bot_id = config.get("BOT_ID", "")

    if not worker_url:
        log("WORKER_URL not set — skipping Worker sync", "WARN")
        return False

    endpoint = f"{worker_url}/update-tunnel"
    params = {}
    if bot_id:
        params["bot"] = bot_id
    if params:
        endpoint += "?" + urllib.parse.urlencode(params)

    body = json.dumps({"url": tunnel_url}).encode("utf-8")

    req = urllib.request.Request(endpoint, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Tunnel-Secret", worker_secret)
    # Cloudflare's Browser Integrity Check on workers.dev rejects the default
    # python-urllib User-Agent with "error code: 1010" before the request
    # ever reaches the Worker. Present a normal browser UA instead.
    req.add_header(
        "User-Agent",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    )
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 201, 202, 204):
                log(f"Worker sync successful — tunnel URL sent to {worker_url}")
                try:
                    resp_body = resp.read().decode("utf-8", "replace")
                    if resp_body:
                        log(f"  Worker response: {resp_body[:200]}")
                except Exception:
                    pass
                return True
            else:
                log(f"Worker sync failed: HTTP {resp.status}", "WARN")
                return False
    except urllib.error.HTTPError as e:
        log(f"Worker sync HTTP error: {e.code} {e.reason}", "ERROR")
        err_text = f"{e.code} {e.reason}"
        try:
            err_body = e.read().decode("utf-8", "replace")
            log(f"  Worker error body: {err_body[:300]}", "ERROR")
            err_text += " " + err_body
        except Exception:
            pass
        log_diagnosis(err_text, None)
        if e.code == 401:
            log("  >> The Worker itself rejected the secret. Compare WORKER_SECRET in", "ERROR")
            log("     the dataset config.env with the Worker's settings in Cloudflare.", "ERROR")
        elif e.code == 404:
            log("  >> The Worker exists but has no /update-tunnel route - wrong Worker", "ERROR")
            log("     (is WORKER_URL pointing at the right deployment?).", "ERROR")
        elif e.code == 403:
            log("  >> Blocked before reaching the Worker (Cloudflare bot check).", "ERROR")
        return False
    except Exception as e:
        log(f"Worker sync failed: {e}", "ERROR")
        return False


# ============================================================================
# SECTION 9 — CONFIG INJECTION (BASE_URL)
# ============================================================================

def inject_base_url(config_path, worker_url):
    """
    Inject the Worker URL as BASE_URL into config.env.

    Sets or replaces the BASE_URL line in config.env with the Worker URL.
    Also sets BASE_URL_PORT to empty (the Worker handles routing).
    """
    if not os.path.isfile(config_path):
        log(f"config.env not found at {config_path} for BASE_URL injection", "ERROR")
        return

    with open(config_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = []
    base_url_found = False
    base_url_port_found = False

    for line in lines:
        stripped = line.strip()

        # Handle BASE_URL (but not BASE_URL_PORT)
        if stripped.startswith("BASE_URL") and "=" in stripped and not stripped.startswith("BASE_URL_PORT"):
            indent = line[:len(line) - len(line.lstrip())]
            new_lines.append(f'{indent}BASE_URL = "{worker_url}"\n')
            base_url_found = True
            continue

        # Handle commented BASE_URL (e.g. "# BASE_URL = ..." or "#BASE_URL = ...")
        uncommented = stripped.lstrip("#").strip()
        if uncommented.startswith("BASE_URL") and "=" in uncommented and not uncommented.startswith("BASE_URL_PORT"):
            if stripped.startswith("#"):
                indent = line[:len(line) - len(line.lstrip())]
                new_lines.append(f'{indent}BASE_URL = "{worker_url}"\n')
                base_url_found = True
                continue

        # Handle BASE_URL_PORT (commented or not)
        port_uncommented = stripped.lstrip("#").strip()
        if port_uncommented.startswith("BASE_URL_PORT") and "=" in port_uncommented:
            indent = line[:len(line) - len(line.lstrip())]
            new_lines.append(f'{indent}BASE_URL_PORT = ""\n')
            base_url_port_found = True
            continue

        new_lines.append(line)

    if not base_url_found:
        new_lines.append(f'\nBASE_URL = "{worker_url}"\n')
    if not base_url_port_found:
        new_lines.append(f'BASE_URL_PORT = ""\n')

    with open(config_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    log(f"Injected BASE_URL = {worker_url} into config.env")


# ============================================================================
# SECTION 10 — DOWNLOAD CLEANUP
# ============================================================================

def cleanup_downloads(download_dir):
    """Remove old download files to free disk space."""
    if not os.path.isdir(download_dir):
        return
    log(f"Cleaning up old downloads in {download_dir}...")
    removed = 0
    freed_bytes = 0
    try:
        for entry in os.listdir(download_dir):
            entry_path = os.path.join(download_dir, entry)
            try:
                if os.path.isfile(entry_path):
                    size = os.path.getsize(entry_path)
                    os.remove(entry_path)
                    removed += 1
                    freed_bytes += size
                elif os.path.isdir(entry_path):
                    dir_size = sum(
                        os.path.getsize(os.path.join(dp, f))
                        for dp, _, fns in os.walk(entry_path)
                        for f in fns
                    )
                    shutil.rmtree(entry_path, ignore_errors=True)
                    removed += 1
                    freed_bytes += dir_size
            except Exception as e:
                log(f"  Could not remove {entry}: {e}", "WARN")
    except Exception as e:
        log(f"Cleanup error: {e}", "WARN")

    if removed:
        freed_mb = freed_bytes / (1024 * 1024)
        log(f"Removed {removed} items, freed {freed_mb:.1f} MB")
    else:
        log("No old downloads to clean")


# ============================================================================
# SECTION 11 — SELF-TERMINATION TIMER
# ============================================================================

def self_termination_timer():
    """
    Background thread that fires after a random 9.5–10.0 hours.

    Sends SIGINT to the bot process for graceful shutdown, waits up to
    30 seconds, then sends SIGTERM/SIGKILL if still alive. Sets
    SHUTDOWN_EVENT so the main thread knows to proceed with cleanup.

    Uses the global BOT_PROCESS (set in main() after the bot starts).
    """
    runtime = random.randint(MIN_RUNTIME, MAX_RUNTIME)
    hours = runtime / 3600
    log(f"Self-termination timer set: {hours:.1f}h ({runtime}s)")

    # Sleep until it's time to terminate, checking SHUTDOWN_EVENT each second
    for _ in range(runtime):
        if SHUTDOWN_EVENT.is_set():
            return
        time.sleep(1)

    log("=" * 60)
    log(f"Self-termination timer fired after {hours:.1f}h — initiating graceful shutdown")
    log("=" * 60)

    if BOT_PROCESS is not None and BOT_PROCESS.poll() is None:
        try:
            BOT_PROCESS.send_signal(signal.SIGINT)
            log("Sent SIGINT to bot process, waiting up to 30s...")
            try:
                BOT_PROCESS.wait(timeout=30)
                log("Bot process exited gracefully (SIGINT)")
            except subprocess.TimeoutExpired:
                log("Bot didn't exit in 30s, sending SIGTERM...", "WARN")
                BOT_PROCESS.terminate()
                try:
                    BOT_PROCESS.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    log("Bot still alive, sending SIGKILL", "ERROR")
                    BOT_PROCESS.kill()
        except Exception as e:
            log(f"Error during self-termination: {e}", "ERROR")

    SHUTDOWN_EVENT.set()


# ============================================================================
# SECTION 11.5 — SESSION LOCK (single active instance per notebook)
# ============================================================================
# Kaggle does not expose a public API to stop a previous session of a kernel,
# and two simultaneous sessions would run two bots on the same tokens
# (Telegram update-stealing conflicts). Instead we use a lease in the shared
# MongoDB: every session generates its own ID, claims the lock document, and
# a background thread verifies ownership every 45 seconds. When a NEWER
# session claims the lock, any older session of this notebook detects it and
# terminates itself gracefully. This keeps exactly one bot running at any time.

SESSION_ID = f"{int(time.time())}-{random.randint(100000, 999999)}"
_SESSION_LOCK_DB = "wzml_kaggle"
_SESSION_LOCK_COL = "session_lock"


def stop_previous_instance(tunnel_url, database_url):
    """
    Take over from a still-running previous notebook session.

    The previous run registered its tunnel URL + a random token in the
    session-lock collection. We ping that web UI ~every 1.5s; once it
    answers 3 times in a row (confirmed alive), we send the token to its
    /wzadmin/api/takeover endpoint, which stops that bot gracefully
    (its notebook then shuts down too). Everything stays well inside a
    10-second budget, then this run proceeds no matter what.
    """
    if not database_url:
        return
    import json as _json
    import urllib.error
    import urllib.request

    try:
        from pymongo import MongoClient

        cl = MongoClient(database_url, serverSelectionTimeoutMS=8000)
        col = cl[_SESSION_LOCK_DB][_SESSION_LOCK_COL]
        old = col.find_one({"_id": "instance"})
    except Exception as e:
        log(f"Takeover: could not read the instance registry ({e})", "WARN")
        try:
            cl.close()
        except Exception:
            pass
        return

    try:
        if old and old.get("url") and old.get("sid") != SESSION_ID:
            url = str(old["url"]).rstrip("/")
            log(f"Takeover: previous instance found — pinging {url}")
            ok = fails = 0
            stopped = False
            for _ in range(6):  # ~9s of pinging at most
                try:
                    urllib.request.urlopen(url + "/wzadmin", timeout=2)
                    ok += 1
                    fails = 0
                except urllib.error.HTTPError:
                    # any HTTP answer (401/403/404…) still proves the
                    # web server is alive
                    ok += 1
                    fails = 0
                except Exception:
                    ok = 0
                    fails += 1
                if ok >= 3:
                    log("Takeover: previous instance alive (3/3) — sending stop")
                    try:
                        req = urllib.request.Request(
                            url + "/wzadmin/api/takeover",
                            data=_json.dumps(
                                {"tok": str(old.get("tok", ""))}
                            ).encode(),
                            headers={"Content-Type": "application/json"},
                            method="POST",
                        )
                        with urllib.request.urlopen(req, timeout=5) as r:
                            stopped = r.status == 200
                    except Exception as e:
                        log(f"Takeover: stop call failed ({e})", "WARN")
                    break
                if fails >= 2:
                    log("Takeover: previous instance not responding — nothing to stop")
                    break
                time.sleep(1.5)
            if stopped:
                # wait until the old tunnel actually goes dark
                for _ in range(5):
                    try:
                        urllib.request.urlopen(url + "/wzadmin", timeout=2)
                    except Exception:
                        break
                    time.sleep(1)
                log("Takeover: previous notebook stopped")
        # register ourselves (fresh token, our own tunnel URL)
        import secrets as _secrets

        col.replace_one(
            {"_id": "instance"},
            {
                "_id": "instance",
                "url": (tunnel_url or "").strip(),
                "sid": SESSION_ID,
                "tok": _secrets.token_hex(16),
                "ts": int(time.time()),
            },
            upsert=True,
        )
        log("Takeover: this session registered in the instance registry")
    except Exception as e:
        log(f"Takeover: failed ({e}) — continuing", "WARN")
    finally:
        try:
            cl.close()
        except Exception:
            pass


def acquire_session_lock(database_url):
    """Claim the session lock document in MongoDB. Returns True if claimed."""
    if not database_url:
        log("Session lock: no DATABASE_URL — duplicate-session protection disabled", "WARN")
        return False
    try:
        from pymongo import MongoClient
        client = MongoClient(database_url, serverSelectionTimeoutMS=15000)
        col = client[_SESSION_LOCK_DB][_SESSION_LOCK_COL]
        col.replace_one(
            {"_id": "lock"},
            {"_id": "lock", "owner": SESSION_ID, "ts": int(time.time())},
            upsert=True,
        )
        client.close()
        log(f"Session lock claimed ({SESSION_ID}) — older sessions of this notebook will terminate")
        return True
    except Exception as e:
        log(f"Session lock: could not claim ({e}) — continuing without it", "WARN")
        return False


def session_lock_monitor(config):
    """
    Background thread: verify every 15s that this session still owns the lock.
    If a newer session has taken over, shut this session down gracefully.
    Requires 3 consecutive mismatches before acting (tolerates transient DB
    errors) and never terminates while the DB is merely unreachable.
    """
    database_url = config.get("DATABASE_URL", "")
    mismatches = 0
    while not SHUTDOWN_EVENT.is_set():
        time.sleep(15)
        if SHUTDOWN_EVENT.is_set():
            return
        try:
            from pymongo import MongoClient
            client = MongoClient(database_url, serverSelectionTimeoutMS=15000)
            doc = client[_SESSION_LOCK_DB][_SESSION_LOCK_COL].find_one({"_id": "lock"})
            client.close()
        except Exception:
            # DB unreachable — do NOT kill the bot on a transient outage
            mismatches = 0
            continue
        if doc is not None and doc.get("owner") != SESSION_ID:
            mismatches += 1
            log(f"Session lock: newer session detected ({mismatches}/3) — {doc.get('owner')}", "WARN")
            if mismatches >= 3:
                log("=" * 60)
                log("A newer session of this notebook has started — terminating this")
                log("one so only a single bot instance runs. No action needed.")
                log("=" * 60)
                try:
                    if BOT_PROCESS is not None and BOT_PROCESS.poll() is None:
                        BOT_PROCESS.send_signal(signal.SIGINT)
                        try:
                            BOT_PROCESS.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            BOT_PROCESS.terminate()
                            try:
                                BOT_PROCESS.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                BOT_PROCESS.kill()
                except Exception:
                    pass
                SHUTDOWN_EVENT.set()
                try:
                    notify(config, "stop", "New session started — this session terminated (single-instance lock)")
                except Exception:
                    pass
                os._exit(0)
        else:
            mismatches = 0


# ============================================================================
# SECTION 12 — SIGNAL HANDLERS
# ============================================================================

def handle_signal(signum, frame):
    """Handle SIGINT/SIGTERM — forward to the bot process."""
    global BOT_PROCESS
    log(f"Received signal {signum} — forwarding to bot process")
    SHUTDOWN_EVENT.set()
    if BOT_PROCESS is not None and BOT_PROCESS.poll() is None:
        try:
            BOT_PROCESS.send_signal(signum)
        except Exception:
            pass


# ============================================================================
# SECTION 13 — MAIN ORCHESTRATION
# ============================================================================

def _run_worker_branch(config):
    """WZFIX r25c (v15.83): Phase B worker session — same engine stack
    (cloned WZML-X, all patches, upgraded yt-dlp, PO token server,
    cookies from the job payload) but headless: no Telegram poller, no
    tunnel, no session lock. Runs the job slice, then exits so the
    worker session ends by itself."""
    log("=" * 60)
    log("WZFIX r25c: WORKER session — starting job runner")
    log("=" * 60)
    try:
        import subprocess as _sp25

        env25 = dict(os.environ)
        env25["PYTHONPATH"] = WZMLX_DIR
        runner25 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r25_worker.py")
        p25 = _sp25.run([sys.executable, runner25], cwd=WZMLX_DIR, env=env25)
        log(f"WZFIX r25c: worker job finished rc={p25.returncode}")
        os._exit(0 if p25.returncode == 0 else 1)
    except Exception as e:
        log(f"WZFIX r25c: worker branch failed: {e}", "ERROR")
        os._exit(1)


def main():
    """
    Main entry point — orchestrates the full Kaggle notebook workflow.
    """
    global BOT_PROCESS, NOTIFIED_STREAM_READY, CONFIG_SRC

    config = {}
    bot_proc = None
    tunnel_url = None
    # safe default: if we exit before Step 3 assigns the real one, the
    # finally block's cleanup_downloads() still has a valid path
    download_dir = DOWNLOAD_DIR_DEFAULT

    # ------------------------------------------------------------------
    # Step 0: Random startup delay (10–120 s) for fingerprint variation
    # ------------------------------------------------------------------
    _WORKER25 = os.environ.get("WZFIX_WORKER") == "1"
    if _WORKER25:
        delay = 0
        log("Worker session — skipping startup delay")
    else:
        delay = random.randint(MIN_STARTUP_DELAY, MAX_STARTUP_DELAY)
        log(f"Startup delay: {delay}s (fingerprint variation)")
        time.sleep(delay)

    # ------------------------------------------------------------------
    # Step 1: Parse config
    # ------------------------------------------------------------------
    log("=" * 60)
    log("WZML-X Kaggle Runner — Starting")
    log("=" * 60)

    if not os.path.isfile(CONFIG_SRC):
        # WZFIX r25c5 (v15.83.5): kernels created through the API mount
        # datasets under /kaggle/input/datasets/<owner>/<slug>/ instead of
        # the flat UI layout — resolve the real config path once, up front
        # (also affects every later use through the module global).
        import glob as _g25

        for _c25 in (
            "/kaggle/input/datasets/djoshi7/wzmlx-config/config.env",
            "/kaggle/input/datasets/wzmlx-config/config.env",
        ) + tuple(
            _g25.glob(
                "/kaggle/input/**/wzmlx-config/config.env", recursive=True
            )
        ):
            if os.path.isfile(_c25):
                CONFIG_SRC = _c25
                log(f"r25c5: worker config resolved to {CONFIG_SRC}")
                break
    if not os.path.isfile(CONFIG_SRC):
        log(f"Config file not found: {CONFIG_SRC}", "ERROR")
        log("Make sure you've added the wzmlx-config dataset to your Kaggle notebook", "ERROR")
        return

    config = parse_config(CONFIG_SRC)
    log(f"Config parsed: {len(config)} keys")

    # Validate critical keys
    required = ["BOT_TOKEN", "OWNER_ID", "TELEGRAM_API", "TELEGRAM_HASH"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        log(f"Missing required config keys: {', '.join(missing)}", "ERROR")
        return

    # Check for sentinel line
    if config.get("_____REMOVE_THIS_LINE_____"):
        log("WARNING: config.env still has the REMOVE_THIS_LINE sentinel!", "WARN")

    # Send start notification (not for Phase B workers)
    if not _WORKER25:
        start_msg = (
            f"Bot starting up on Kaggle\n"
            f"Startup delay was {delay}s\n"
            f"Config keys loaded: {len(config)}"
        )
        notify(config, "start", start_msg)

    # ------------------------------------------------------------------
    # Step 2: Clone WZML-X
    # ------------------------------------------------------------------
    log("=" * 60)
    log("Cloning WZML-X repository (wzv3 branch)")
    log("=" * 60)

    if os.path.isdir(WZMLX_DIR):
        log(f"Removing existing {WZMLX_DIR}")
        shutil.rmtree(WZMLX_DIR, ignore_errors=True)

    try:
        subprocess.run(
            [
                "git", "clone", "--depth", "1", "-b", "wzv3",
                "https://github.com/SilentDemonSD/WZML-X.git",
                WZMLX_DIR,
            ],
            check=True, timeout=120, capture_output=True, text=True,
        )
        # WZFIX r25c33: pin to the last-known-good wzv3 commit. Upstream
        # drifted to ab6464d2 on 28 Sep and broke the wserver lifespan
        # (ClientTimeout), moved bot.ext_utils (playlists 500s) and
        # invalidated patch anchors. All patches are validated on 6cc2760.
        _pin = "6cc2760ab1c95a8e05661f10b8ee0b2f499dbe39"
        _ok = False
        _pr = subprocess.run(
            ["git", "-C", WZMLX_DIR, "fetch", "--depth", "1", "origin", _pin],
            capture_output=True, text=True, timeout=180,
        )
        if _pr.returncode == 0:
            subprocess.run(
                ["git", "-C", WZMLX_DIR, "checkout", "--detach", "FETCH_HEAD"],
                check=True, timeout=60, capture_output=True, text=True,
            )
            _ok = True
            log(f"WZML-X pinned to {_pin[:8]} via git fetch (r25c34)")
        else:
            # git servers refuse non-tip SHAs - fetch the tarball instead
            try:
                _tgz = (
                    "https://codeload.github.com/SilentDemonSD/WZML-X/"
                    "tar.gz/" + _pin
                )
                subprocess.run(
                    ["curl", "-sSL", "-o", "/tmp/_wz.tgz", _tgz],
                    check=True, timeout=300, capture_output=True, text=True,
                )
                shutil.rmtree(WZMLX_DIR, ignore_errors=True)
                os.makedirs(WZMLX_DIR, exist_ok=True)
                subprocess.run(
                    [
                        "tar", "-xzf", "/tmp/_wz.tgz",
                        "-C", WZMLX_DIR, "--strip-components=1",
                    ],
                    check=True, timeout=120, capture_output=True, text=True,
                )
                _ok = True
                log(f"WZML-X pinned to {_pin[:8]} via tarball (r25c34)")
            except Exception as _te:
                log(
                    f"WZFIX r25c34: pin failed ({_te!r}) - staying on wzv3 tip",
                    "WARN",
                )
        log("WZML-X cloned successfully")
        log("WZML-X cloned successfully")
    except Exception as e:
        log(f"Failed to clone WZML-X: {e}", "ERROR")
        notify(config, "crash", f"Git clone failed: {e}")
        return

    # ------------------------------------------------------------------
    # Step 3: Copy config.env into the repo
    # ------------------------------------------------------------------
    log("Copying config.env into WZML-X directory")
    shutil.copy2(CONFIG_SRC, CONFIG_DST)

    # Clean up old downloads on startup
    download_dir = config.get("DOWNLOAD_DIR", DOWNLOAD_DIR_DEFAULT)
    if not download_dir:
        download_dir = DOWNLOAD_DIR_DEFAULT
    if not download_dir.endswith("/"):
        download_dir += "/"
    os.makedirs(download_dir, exist_ok=True)
    cleanup_downloads(download_dir)

    # ------------------------------------------------------------------
    # Step 4: Write & apply patches
    # ------------------------------------------------------------------
    write_patch_scripts()
    apply_patches()
    apply_sed_patches()
    apply_userrepo_patches()

    # ------------------------------------------------------------------
    # Step 5: Install system packages and Python deps
    # ------------------------------------------------------------------
    install_system_packages()
    install_python_deps()

    # WZFIX niquests guard v2: WZML-X needs a MODERN niquests — the code
    # uses AsyncSession(headers=...) AND the vendored `packages`
    # submodule. Kaggle base images keep rotating in stale preinstalled
    # niquests builds that pip treats as "already satisfied" because
    # requirements.txt pins no version (two different crashes so far).
    # Enforce a known-good minimum on every run.
    try:
        import importlib as _imp
        import importlib.util as _ilu
        import inspect as _insp

        def _niquests_ok():
            import sys as _sys

            # drop any cached niquests so the check reads the REAL
            # on-disk state (pip may have just replaced it)
            for _m in [
                m
                for m in _sys.modules
                if m == "niquests" or m.startswith("niquests.")
            ]:
                del _sys.modules[_m]
            _imp.invalidate_caches()
            if _ilu.find_spec("niquests") is None:
                return False
            if _ilu.find_spec("niquests.packages") is None:
                return False
            try:
                from niquests import AsyncSession as _AS

                return "headers" in _insp.signature(_AS.__init__).parameters
            except Exception:
                return False

        # (1) always ensure the minimum (no-op when already current)
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "niquests>=3.21.1"],
            capture_output=True, text=True, timeout=300,
        )
        if not _niquests_ok():
            # (2) stale preinstalled build is shadowing — force it away
            log("niquests stale/broken — forcing reinstall >=3.21.1", "WARN")
            subprocess.run(
                [sys.executable, "-m", "pip", "install",
                 "--upgrade", "--force-reinstall", "--no-deps",
                 "niquests>=3.21.1"],
                capture_output=True, text=True, timeout=300,
            )
            if not _niquests_ok():
                log("niquests still bad — import fallbacks will carry it", "WARN")
            else:
                log("niquests fixed by forced reinstall")
        else:
            log("niquests OK (modern build verified)")
    except Exception as _nq_e:
        log(f"niquests guard failed: {_nq_e}", "WARN")

    # WZFIX wzgram upload-engine patch (J-17): the deployed pyrogram
    # fork (wzgram 3.1.2) retries a dying upload part SIXTEEN times
    # with 60 s timeouts and up-to-300 s flood sleeps — a stalled
    # connection leaves the task "stuck at Upload, 0 B/s" for 2+
    # HOURS before it finally fails (and the whole time it occupies
    # one of the QUEUE_ALL slots, so new tasks silently queue behind
    # it). Cap the retries so a dead upload fails within minutes and
    # frees its slot: 5 attempts, flood sleep max 120 s, stall
    # watchdog 10 min.
    try:
        try:
            import importlib.metadata as _imd

            _wz_ver = _imd.version("wzgram")
        except Exception:
            _wz_ver = "?"
        if _wz_ver != "3.1.2":
            log(
                f"wzgram version {_wz_ver} != tested 3.1.2 — "
                "patch anchors/behavior may differ",
                "WARN",
            )
        import importlib.util as _ilu

        _sf_spec = _ilu.find_spec("pyrogram")
        _sf_path = None
        if _sf_spec and _sf_spec.origin:
            _sf_dir = os.path.dirname(_sf_spec.origin)
            _cand = os.path.join(
                _sf_dir, "methods", "advanced", "save_file.py"
            )
            if os.path.isfile(_cand):
                _sf_path = _cand
        if not _sf_path:
            for _cand_base in (
                "/usr/local/lib/python3.12/dist-packages",
                "/usr/local/lib/python3.11/dist-packages",
                "/usr/lib/python3/dist-packages",
            ):
                _cand = os.path.join(
                    _cand_base,
                    "pyrogram",
                    "methods",
                    "advanced",
                    "save_file.py",
                )
                if os.path.isfile(_cand):
                    _sf_path = _cand
                    break
        if _sf_path:
            with open(_sf_path, "r", encoding="utf-8") as f:
                _sf = f.read()
            if "WZFIX upload cap" not in _sf:
                _pairs = [
                    ("MAX_RETRIES = 16",
                     "MAX_RETRIES = 5  # WZFIX upload cap"),
                    ("STALL_TIMEOUT = 900",
                     "STALL_TIMEOUT = 600  # WZFIX upload cap"),
                    ("delay = min(int(part), 300)",
                     "delay = min(int(part), 120)  # WZFIX upload cap"),
                ]
                _changed = False
                for _old, _new in _pairs:
                    if _old in _sf:
                        _sf = _sf.replace(_old, _new, 1)
                        _changed = True
                    else:
                        log(f"wzgram: anchor not found: {_old!r}", "WARN")
                if _changed:
                    with open(_sf_path, "w", encoding="utf-8") as f:
                        f.write(_sf)
                    _r = subprocess.run(
                        [sys.executable, "-m", "py_compile", _sf_path],
                        capture_output=True, text=True, timeout=60,
                    )
                    if _r.returncode == 0:
                        log("wzgram upload engine capped (5 attempts)")
                    else:
                        log(
                            f"wzgram patch compile FAILED — "
                            f"{(_r.stderr or '').strip()[:200]}",
                            "ERROR",
                        )
            else:
                log("wzgram upload cap already applied")
        else:
            log("wzgram save_file.py not found — upload cap skipped", "WARN")
    except Exception as _wz_e:
        log(f"wzgram patch failed: {_wz_e}", "WARN")

    # WZFIX yt-dlp freshness guard (J-20): requirements.txt does not pin
    # yt-dlp, so the bot runs whatever the Kaggle image preinstalled.
    # YouTube changes its extraction constantly and stale builds walk
    # slow client-fallback chains (the "Skipping unsupported client"
    # warnings, 15s+ metadata delays before a download starts). Upgrade
    # to the latest release on every run.
    try:
        _ry = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-U", "yt-dlp"],
            capture_output=True, text=True, timeout=300,
        )
        if _ry.returncode == 0:
            _vy = subprocess.run(
                [sys.executable, "-m", "pip", "show", "yt-dlp"],
                capture_output=True, text=True, timeout=60,
            ).stdout
            for _ln in _vy.splitlines():
                if _ln.startswith("Version:"):
                    log(f"yt-dlp fresh: {_ln.split(':', 1)[1].strip()}")
                    break
        else:
            log(f"yt-dlp upgrade failed — {(_ry.stderr or '').strip()[:160]}", "WARN")
    except Exception as _ye:
        log(f"yt-dlp guard failed: {_ye}", "WARN")
    setup_wzml_services(config)

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Step 5.5: WZFIX r25c — Phase B worker branch (headless)
    # ------------------------------------------------------------------
    if _WORKER25:
        _run_worker_branch(config)
        return

    # Step 6: Download cloudflared and start tunnel
    # ------------------------------------------------------------------
    log("=" * 60)
    log("Setting up Cloudflare tunnel")
    log("=" * 60)

    if not download_cloudflared():
        log("cloudflared download failed — bot will run without tunnel", "WARN")
    else:
        tunnel_url = start_cloudflared_tunnel(port=8080)

        if tunnel_url:
            log(f"Tunnel is live: {tunnel_url}")

            # Sync tunnel URL to Cloudflare Worker
            worker_synced = sync_to_worker(config, tunnel_url)

            # Determine the BASE_URL to inject
            worker_url = config.get("WORKER_URL", "").strip().strip("/")
            if worker_synced and worker_url:
                base_url_to_inject = worker_url
                log(f"Using Worker URL as BASE_URL: {base_url_to_inject}")
            else:
                base_url_to_inject = tunnel_url
                log(f"Using tunnel URL as BASE_URL: {base_url_to_inject}", "WARN")

            # Inject BASE_URL into config.env
            inject_base_url(CONFIG_DST, base_url_to_inject)

            # Send stream_ready notification with the URL
            stream_msg = (
                f"Stream is ready!\n"
                f"Tunnel: {tunnel_url}\n"
                f"Base URL: {base_url_to_inject}\n"
                f"Dashboard: {base_url_to_inject}/wzadmin\n"
                f"Worker synced: {'yes' if worker_synced else 'no'}"
            )
            notify(config, "stream_ready", stream_msg)
            NOTIFIED_STREAM_READY = True
        else:
            log("Tunnel setup failed — continuing without tunnel", "WARN")
            notify(config, "stream_ready", "Tunnel setup failed — running without web UI")

    # ------------------------------------------------------------------
    # Step 6.5: Take over from a previous still-running notebook session
    # (pings its web UI, stops it after 3 confirmations — see
    # stop_previous_instance above)
    # ------------------------------------------------------------------
    stop_previous_instance(tunnel_url, config.get("DATABASE_URL", ""))

    # ------------------------------------------------------------------
    # Step 6.6: WZFIX r25c28 - tunnel watchdog: if the public tunnel
    # goes dark, restart cloudflared and re-register with the worker.
    # ------------------------------------------------------------------
    def _r28_tunnel_watchdog():  # WZFIX r25c28
        import time as _t
        import urllib.request as _u
        _turl = tunnel_url
        _fails = 0
        _last = 0.0
        while True:
            _ok = False
            if _turl:
                try:
                    with _u.urlopen(_turl + "/health", timeout=10) as _r:
                        _ok = _r.status < 500
                except Exception:
                    _ok = False
            if _ok:
                if _fails >= 3:
                    log("r25c28: tunnel healthy again")
                _fails = 0
            else:
                _fails += 1
                if _fails >= 3 and (_t.time() - _last) > 300:
                    _last = _t.time()
                    log(f"r25c28: public tunnel {_turl} failing - diagnosing", "ERROR")
                    try:
                        with _u.urlopen("http://127.0.0.1:8080/", timeout=5) as _r:
                            log(
                                f"r25c28: local web on 8080 answers ({_r.status})"
                                " - tunnel side is the problem"
                            )
                    except Exception as _e:
                        log(
                            f"r25c28: local web on 8080 also dead"
                            f" ({_e.__class__.__name__}) - wserver side is the problem",
                            "ERROR",
                        )
                    try:
                        if TUNNEL_PROCESS is not None and TUNNEL_PROCESS.poll() is None:
                            TUNNEL_PROCESS.terminate()
                    except Exception:
                        pass
                    _t.sleep(3)
                    _new = start_cloudflared_tunnel(port=8080)
                    if _new:
                        _turl = _new
                        log(f"r25c28: new tunnel {_new}")
                        if sync_to_worker(config, _new):
                            log("r25c28: worker re-registered")
                    else:
                        log("r25c28: cloudflared restart FAILED", "ERROR")
                    _fails = 0
            _t.sleep(60)

    threading.Thread(target=_r28_tunnel_watchdog, daemon=True).start()  # WZFIX r25c28

    # ------------------------------------------------------------------
    # Step 6.7: WZFIX r25c29 - log shipper: push a full diagnostics
    # bundle (kernel log ring + gunicorn log + bot log + process and
    # socket state) to the private Kaggle dataset djoshi7/wzmlx-logs
    # every 2 minutes, so logs can be fetched from outside.
    # ------------------------------------------------------------------
    def _r29_log_shipper():  # WZFIX r25c29
        import time as _t
        import json as _json
        import subprocess as _sp
        logdir = os.path.join(KAGGLE_WORKING, "wzml-logs")
        os.makedirs(logdir, exist_ok=True)
        last_err = None

        def _tail(path, n):
            try:
                with open(path, "r", errors="replace") as f:
                    return "\n".join(f.read().splitlines()[-n:])
            except Exception as _e:
                return "(unreadable: " + repr(_e) + ")"

        while True:
            _t.sleep(60)
            try:
                ps = ""
                try:
                    _psout = _sp.run(
                        ["ps", "-eo", "pid,etimes,cmd"],
                        capture_output=True, text=True, timeout=15).stdout
                    ps = "\n".join(
                        _l for _l in _psout.splitlines()
                        if ("gunicorn" in _l or "cloudflared" in _l
                            or ("python" in _l and " bot" in _l)))
                except Exception:
                    ps = "(ps failed)"
                ss = ""
                try:
                    _ssout = _sp.run(
                        ["ss", "-ltn"], capture_output=True,
                        text=True, timeout=15).stdout
                    ss = "\n".join(
                        _l for _l in _ssout.splitlines()
                        if ":8080 " in _l or ":8091 " in _l or ":4416 " in _l)
                except Exception:
                    ss = "(ss failed)"
                bundle = {
                    "ts": now_ist_str(),
                    "version": "v15.83.34",
                    "tunnel": tunnel_url,
                    "kernel_log": "\n".join(_LOG_RING[-400:]),
                    "wserver_log": _tail(os.path.join(KAGGLE_WORKING, "wserver.log"), 400),
                    "bot_log": _tail(os.path.join(WZMLX_DIR, "log.txt"), 400),
                    "processes": ps,
                    "sockets": ss,
                    "shipper_note": last_err or "ok",
                }
                with open(os.path.join(logdir, "bundle.json"), "w", encoding="utf-8") as f:
                    _json.dump(bundle, f, ensure_ascii=False, indent=1)
                meta_path = os.path.join(logdir, "dataset-metadata.json")
                if not os.path.isfile(meta_path):
                    with open(meta_path, "w") as f:
                        _json.dump({
                            "title": "wzmlx-logs",
                            "id": "djoshi7/wzmlx-logs",
                            "licenses": [{"name": "CC0-1.0"}],
                            "private": True,
                        }, f)
                if (
                    os.path.isfile(os.path.expanduser("~/.kaggle/kaggle.json"))
                    or (os.environ.get("KAGGLE_USERNAME")
                        and os.environ.get("KAGGLE_KEY"))
                ):
                    # WZFIX r25c30: only try the dataset push when kernel-side
                    # credentials exist (plain sessions have none - that is
                    # why wzmlx-logs never appeared; logs are served via
                    # wserver /_diag/logs instead now)
                    _r = _sp.run(
                        [sys.executable, "-m", "kaggle", "datasets", "version",
                         "-p", logdir, "-m", "auto", "--dir-mode", "zip"],
                        capture_output=True, text=True, timeout=240)
                    if _r.returncode != 0:
                        _r2 = _sp.run(
                            [sys.executable, "-m", "kaggle", "datasets", "create",
                             "-p", logdir],
                            capture_output=True, text=True, timeout=240)
                        if _r2.returncode != 0:
                            last_err = ("create/version failed: "
                                        + ((_r2.stdout or "") + (_r2.stderr or ""))[-300:])
                        else:
                            last_err = None
                    else:
                        last_err = None
                else:
                    last_err = "no kaggle creds in kernel; logs served via /_diag/logs"
            except Exception as _e:
                last_err = repr(_e)

    threading.Thread(target=_r29_log_shipper, daemon=True).start()  # WZFIX r25c29

    # ------------------------------------------------------------------
    # Step 6.8: WZFIX r25c31 - fallback web server. While gunicorn is
    # dead, the notebook serves /_diag/logs and a maintenance page on
    # port 8080 itself, so the tunnel/worker path stays alive and logs
    # stay fetchable from outside. Every ~6.5 min the port is released
    # for 90 s so gunicorn restarts (r25c28 watchdog) can rebind.
    # ------------------------------------------------------------------
    def _r31_fallback_web():  # WZFIX r25c31
        import http.server as _hs31
        import socket as _sk31
        import threading as _th31
        import time as _t31

        _KEY31 = "__WZFIX_DIAG_KEY__"

        def _ring31():
            try:
                return "\n".join(list(_LOG_RING)[-400:])
            except Exception:
                return "(ring unavailable)"

        def _tail31(path, n):
            try:
                with open(path, "r", errors="replace") as f:
                    return "\n".join(f.read().splitlines()[-n:])
            except Exception as e:
                return "(unreadable: " + repr(e) + ")"

        def _alive31():
            try:
                s = _sk31.create_connection(("127.0.0.1", 8080), timeout=2)
                s.close()
                return True
            except Exception:
                return False

        class _H31(_hs31.BaseHTTPRequestHandler):
            server_version = "wzfix-fallback/31"

            def _send31(self, code, body, ctype="application/json"):
                data = body.encode("utf-8", "replace")
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                import json as _j31
                import subprocess as _sp31
                import time as _t31b

                path = self.path.split("?")[0]
                key = None
                if "?" in self.path:
                    for part in self.path.split("?", 1)[1].split("&"):
                        if part.startswith("key="):
                            key = part[4:]
                if key is None:
                    key = self.headers.get("X-Diag-Key")
                if path == "/_diag/logs":
                    if key != _KEY31:
                        self._send31(401, '{"error": "unauthorized"}')
                        return
                    ps = ""
                    try:
                        ps = _sp31.run(
                            ["ps", "-eo", "pid,etimes,cmd"],
                            capture_output=True, text=True, timeout=15,
                        ).stdout
                        ps = "\n".join(
                            l for l in ps.splitlines()
                            if ("gunicorn" in l or "cloudflared" in l)
                        )
                    except Exception as e:
                        ps = repr(e)
                    out = {
                        "ts": _t31b.time(),
                        "diag_version": "r25c31-fallback",
                        "kernel_log": _ring31(),
                        "wserver_log": _tail31(
                            os.path.join(KAGGLE_WORKING, "wserver.log"), 400),
                        "bot_log": _tail31(
                            os.path.join(WZMLX_DIR, "log.txt"), 300),
                        "processes": ps,
                        "note": "gunicorn down - notebook fallback active",
                    }
                    self._send31(200, _j31.dumps(out, ensure_ascii=False))
                elif path == "/health":
                    self._send31(
                        200,
                        '{"bot_responding": false, "wzfix": "fallback"}',
                    )
                else:
                    self._send31(
                        200,
                        "<html><head><meta name='viewport' content='width=device-"
                        "width,initial-scale=1'></head><body style='font-family:"
                        "sans-serif;background:#111;color:#eee;padding:24px;"
                        "max-width:480px;margin:auto'>"
                        "<h2>🎵 WZML web player</h2>"
                        "<p>The web server is down and recovering. Logs are "
                        "being collected automatically and the bot (Telegram) "
                        "is unaffected.</p></body></html>",
                        "text/html",
                    )

            def log_message(self, fmt, *args):
                return

        log("r25c31: fallback web server armed (watching port 8080)")
        _t31.sleep(300)  # WZFIX r25c32: boot grace - let the bot start gunicorn before the fallback may ever bind 8080
        _hold_until = 0.0
        while True:
            try:
                if _t31.time() < _hold_until:
                    _t31.sleep(10)
                    continue
                if _alive31():
                    _t31.sleep(10)
                    continue
                try:
                    srv = _hs31.ThreadingHTTPServer(("0.0.0.0", 8080), _H31)
                except Exception as _e:
                    log(f"r25c31: fallback bind failed: {_e!r}", "WARN")
                    _t31.sleep(20)
                    continue
                srv.daemon_threads = True
                _th31.Thread(target=srv.serve_forever, daemon=True).start()
                log("r25c31: fallback SERVING on 8080 (gunicorn down)")
                _t31.sleep(300)
                srv.shutdown()
                srv.server_close()
                log("r25c31: releasing 8080 for gunicorn restart window")
                _hold_until = _t31.time() + 90
            except Exception as _e:
                log(f"r25c31: fallback loop error: {_e!r}", "WARN")
                _t31.sleep(20)

    threading.Thread(target=_r31_fallback_web, daemon=True).start()  # WZFIX r25c31

    # ------------------------------------------------------------------
    # Step 7: Claim the session lock + start the self-termination timer
    # ------------------------------------------------------------------
    if acquire_session_lock(config.get("DATABASE_URL", "")):
        lock_thread = threading.Thread(
            target=session_lock_monitor,
            args=(config,),
            daemon=True,
        )
        lock_thread.start()
    timer_thread = threading.Thread(target=self_termination_timer, daemon=True)
    timer_thread.start()

    # ------------------------------------------------------------------
    # Step 8: Start the bot via `python -m bot`
    # ------------------------------------------------------------------
    log("=" * 60)
    log("Starting WZML-X bot (python -m bot)")
    log("=" * 60)

    env = os.environ.copy()
    env["PYTHONPATH"] = WZMLX_DIR + os.pathsep + env.get("PYTHONPATH", "")
    # WZFIX r23 (v15.82): deno (JS runtime for yt-dlp challenges)
    # lives in KAGGLE_WORKING/bin - put it on the bot PATH
    env["PATH"] = os.path.join(KAGGLE_WORKING, "bin") + os.pathsep + env.get("PATH", "")

    # Export config.env keys as environment variables for the bot process.
    # In the official Docker deployment, config.env is loaded via --env-file,
    # which exports every key into the environment. WZML-X's Config.load_env()
    # reads os.environ, and its load_config() only imports a config.py module
    # (which we do not have). Without this export, TELEGRAM_API / TELEGRAM_HASH
    # / BOT_TOKEN etc. never reach the bot and pyrogram dies with
    # "The API key is required for new authorizations".
    # We re-parse the copy in the WZML-X dir so the injected BASE_URL wins.
    try:
        launch_cfg = parse_config(CONFIG_DST) or parse_config(CONFIG_SRC)
    except Exception:
        launch_cfg = {}
    exported = 0
    for _k, _v in launch_cfg.items():
        if _k and _v is not None and str(_v).strip():
            env[_k] = str(_v).strip()
            exported += 1
    log(f"Exported {exported} config keys into bot environment")

    # WZFIX upload speed (J-18): the wzgram engine parallelizes file
    # uploads across multiple Telegram connections. Defaults are
    # conservative (8 connections for bots, 12 for users) — Kaggle's
    # per-connection throughput to Telegram DCs is ~1-3 MB/s, so the
    # connection pool is the real speed lever. Raise the pools within
    # the engine's own hard cap (POOL_SIZE=20). config.env can still
    # override any of these by defining the variable itself.
    for _wzk, _wzv in (
        ("WZGRAM_UPLOAD_POOL_BOT", "16"),
        ("WZGRAM_UPLOAD_RATE_BOT", "200"),
        ("WZGRAM_UPLOAD_POOL_USER", "16"),
        ("WZGRAM_UPLOAD_RATE_USER", "200"),
    ):
        if _wzk not in env:
            env[_wzk] = _wzv
    log("Upload engine tuned: 16 parallel connections, 200 parts/s")

    # ------------------------------------------------------------------
    # Pre-flight: validate BOT_TOKEN and USER_SESSION_STRING directly
    # against Telegram BEFORE starting the bot, so config problems are
    # reported with a clear, actionable message instead of a bot crash.
    # ------------------------------------------------------------------
    try:
        pf_script = os.path.join(WZMLX_DIR, "_preflight_check.py")
        with open(pf_script, "w") as f:
            f.write(
                '''import asyncio, json, os, urllib.request

def check_bot_token():
    tok = (os.environ.get("BOT_TOKEN") or "").strip()
    if not tok:
        return {"status": "not_set"}
    try:
        with urllib.request.urlopen(
            f"https://api.telegram.org/bot{tok}/getMe", timeout=20
        ) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
            u = data.get("result", {}).get("username", "?")
            return {"status": "ok", "bot": "@" + str(u)}
    except Exception as e:
        return {"status": "failed", "error": str(e)[:250]}

async def check_session():
    s = (os.environ.get("USER_SESSION_STRING") or "").strip()
    if not s:
        return {"status": "not_set"}
    try:
        from pyrogram import Client
        api_id = int(os.environ.get("TELEGRAM_API", "0") or 0)
        api_hash = os.environ.get("TELEGRAM_HASH", "")
        c = Client(
            "preflight",
            api_id=api_id,
            api_hash=api_hash,
            session_string=s,
            in_memory=True,
        )
        await c.start()
        me = await c.get_me()
        uname = "@" + me.username if me.username else str(me.id)
        await c.stop()
        return {"status": "ok", "user": uname}
    except Exception as e:
        return {"status": "failed", "error": str(e)[:250]}

async def main():
    out = {"bot_token": check_bot_token(), "user_session": await check_session()}
    print("PREFLIGHT_JSON:" + json.dumps(out))

asyncio.run(main())
''')
        pf = subprocess.run(
            [sys.executable, pf_script],
            env=env,
            cwd=WZMLX_DIR,
            capture_output=True,
            text=True,
            timeout=180,
        )
        pf_data = None
        for line in (pf.stdout or "").splitlines():
            if line.startswith("PREFLIGHT_JSON:"):
                pf_data = json.loads(line[len("PREFLIGHT_JSON:"):])
        if pf_data is None:
            log(f"Pre-flight check did not produce a result: {(pf.stderr or pf.stdout or '')[:200]}", "WARN")
        else:
            bt = pf_data.get("bot_token", {})
            us = pf_data.get("user_session", {})
            if bt.get("status") == "ok":
                log(f"Pre-flight: BOT_TOKEN valid ({bt.get('bot')})")
            elif bt.get("status") == "not_set":
                log("Pre-flight: BOT_TOKEN not set", "WARN")
            else:
                log(f"Pre-flight: BOT_TOKEN REJECTED by Telegram: {bt.get('error')}", "ERROR")
                log_diagnosis(str(bt.get("error", "")), None)
                if "Unauthor" in str(bt.get("error", "")):
                    log("  >> The BOT_TOKEN in the dataset config.env is revoked or wrong.", "ERROR")
                log("  -> The BOT_TOKEN in the dataset config.env is invalid. Get a fresh", "ERROR")
                log("     token from @BotFather and update the dataset, then re-run.", "ERROR")
            if us.get("status") == "ok":
                log(f"Pre-flight: USER_SESSION_STRING valid ({us.get('user')})")
            elif us.get("status") == "not_set":
                log("Pre-flight: USER_SESSION_STRING not set (streaming via user client disabled)", "WARN")
            else:
                log(f"Pre-flight: USER_SESSION_STRING REJECTED by Telegram: {us.get('error')}", "ERROR")
                log_diagnosis(str(us.get("error", "")), None)
                log("  -> The string in the dataset config.env is stale/revoked.", "ERROR")
                log("  -> Regenerate: send /exportsession to the running Actions bot", "ERROR")
                log("     (sugarly), copy the NEW string from Saved Messages, replace", "ERROR")
                log("     USER_SESSION_STRING in the Kaggle dataset config.env, save the", "ERROR")
                log("     dataset, then re-run this workflow.", "ERROR")
    except Exception as e:
        log(f"Pre-flight check failed to run: {e}", "WARN")

    try:
        # WZFIX_R19: start the version watchdog BEFORE the bot - a
        # kaggle push alone never restarts a running session, so the
        # running bot must exit itself when the repo build changes
        try:
            _r19_start_watchdog()
        except Exception as _r19e:
            log(f"r19 watchdog failed to start: {_r19e}", "WARN")
        BOT_PROCESS = subprocess.Popen(
            [sys.executable, "-m", "bot"],
            cwd=WZMLX_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        bot_proc = BOT_PROCESS
        log(f"Bot process started (PID {bot_proc.pid})")

        # Stream bot output to our log in real-time, with live diagnosis
        _diag_seen = set()
        _bot_lines = []
        for line in bot_proc.stdout:
            line = line.rstrip()
            if line:
                print(f"[bot] {line}", flush=True)
                _bot_lines.append(line)
                if any(s in line for s in ("ERROR", "Error", "Traceback", "Telegram says",
                                           "ModuleNotFoundError", "Exception")):
                    log_diagnosis(line, _diag_seen)

        # Wait for the process to finish
        exit_code = bot_proc.wait()
        log(f"Bot process exited with code {exit_code}")

        if exit_code == 0 or SHUTDOWN_EVENT.is_set():
            notify(config, "stop", f"Bot stopped gracefully (exit code {exit_code})")
        else:
            notify(config, "crash", f"Bot crashed with exit code {exit_code}")
            log("Bot process crashed — check logs above", "ERROR")
            # Show the tail of the crash and explain every known signature
            tail = "\n".join(_bot_lines[-25:])
            log("=" * 60)
            log("CRASH ANALYSIS — last lines before exit:")
            for _l in _bot_lines[-25:]:
                if _l.strip():
                    log(f"  {_l}")
            log_diagnosis("\n".join(_bot_lines), None)
            # Point at the final exception line explicitly
            for _l in reversed(_bot_lines):
                if _l.strip().endswith(("Error", "Exception")) or ": " in _l and _l.lstrip().startswith(("AttributeError", "RuntimeError", "ValueError", "KeyError", "TypeError")):
                    log(f"Most likely crash point >> {_l.strip()}", "ERROR")
                    break
            log("=" * 60)

    except KeyboardInterrupt:
        log("Received KeyboardInterrupt — shutting down")
        notify(config, "stop", "Bot stopped via KeyboardInterrupt")
    except Exception as e:
        log(f"Error running bot: {e}", "ERROR")
        notify(config, "crash", f"Bot runner error: {e}")
    finally:
        # ------------------------------------------------------------------
        # Step 9: Cleanup
        # ------------------------------------------------------------------
        SHUTDOWN_EVENT.set()

        log("=" * 60)
        log("Cleaning up")
        log("=" * 60)

        # Stop cloudflared tunnel
        global TUNNEL_PROCESS
        if TUNNEL_PROCESS is not None and TUNNEL_PROCESS.poll() is None:
            try:
                TUNNEL_PROCESS.terminate()
                TUNNEL_PROCESS.wait(timeout=10)
                log("cloudflared tunnel stopped")
            except Exception:
                try:
                    TUNNEL_PROCESS.kill()
                except Exception:
                    pass

        # Clean up downloads on shutdown
        cleanup_downloads(download_dir)

        log("WZML-X Kaggle runner — shutdown complete")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user")
        SHUTDOWN_EVENT.set()
    except Exception as e:
        log(f"Fatal error: {e}", "ERROR")
        try:
            config = parse_config(CONFIG_SRC)
            notify(config, "crash", f"Fatal error: {e}")
        except Exception:
            pass
        sys.exit(1)

    log("Notebook script complete")
