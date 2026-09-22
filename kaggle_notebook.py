#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 kaggle_notebook.py — WZML-X Telegram Bot Runner for Kaggle
 WZFIX BUILD: v15.37  (ytsearch rclone-exclude fix)
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


# ============================================================================
# SECTION 1 — LOGGING HELPER
# ============================================================================

def log(msg, level="INFO"):
    """Print a timestamped log line in IST."""
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


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
    ('patch_db.py', 'bot/helper/ext_utils/db_handler.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9kYi5weSDigJQgRml4IGdldF9wbV91aWRzIHJldHVybmluZyBOb25lIC0+IHJldHVybiBbXQoKTW9kaWZpZXMgZGJfaGFuZGxlci5weSBpbi1wbGFjZS4gVGhlIGdldF9wbV91aWRzKCkgbWV0aG9kIGhhcyBhIGJhcmUKYHJldHVybmAgd2hlbiBzZWxmLl9yZXR1cm4gaXMgVHJ1ZSAoZGF0YWJhc2UgaW4gc3R1YiBtb2RlKS4gVGhpcyBjYXVzZXMKVHlwZUVycm9yIHdoZW4gY2FsbGVycyBpdGVyYXRlIG92ZXIgdGhlIHJlc3VsdC4gV2UgcmVwbGFjZSB0aGUgYmFyZSByZXR1cm4Kd2l0aCBgcmV0dXJuIFtdYC4KIiIiCmltcG9ydCBzeXMKaW1wb3J0IHJlCgpwYXRoID0gc3lzLmFyZ3ZbMV0Kd2l0aCBvcGVuKHBhdGgsICJyIiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIHNyYyA9IGYucmVhZCgpCgojIFRhcmdldCB0aGUgZ2V0X3BtX3VpZHMgbWV0aG9kIHNwZWNpZmljYWxseQpvbGQgPSAoCiAgICAnICAgIGFzeW5jIGRlZiBnZXRfcG1fdWlkcyhzZWxmKTpcbicKICAgICcgICAgICAgIGlmIHNlbGYuX3JldHVybjpcbicKICAgICcgICAgICAgICAgICByZXR1cm5cbicKICAgICcgICAgICAgIHJldHVybiBbZG9jWyJfaWQiXSBhc3luYyBmb3IgZG9jIGluIHNlbGYuZGIucG1fdXNlcnNbX3BhcnQoKV0uZmluZCh7fSldJwopCm5ldyA9ICgKICAgICcgICAgYXN5bmMgZGVmIGdldF9wbV91aWRzKHNlbGYpOlxuJwogICAgJyAgICAgICAgaWYgc2VsZi5fcmV0dXJuOlxuJwogICAgJyAgICAgICAgICAgIHJldHVybiBbXVxuJwogICAgJyAgICAgICAgdHJ5OlxuJwogICAgJyAgICAgICAgICAgIHJldHVybiBbZG9jWyJfaWQiXSBhc3luYyBmb3IgZG9jIGluIHNlbGYuZGIucG1fdXNlcnNbX3BhcnQoKV0uZmluZCh7fSldXG4nCiAgICAnICAgICAgICBleGNlcHQgRXhjZXB0aW9uOlxuJwogICAgJyAgICAgICAgICAgIHJldHVybiBbXScKKQoKaWYgb2xkIGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZCwgbmV3LCAxKQogICAgcHJpbnQoInBhdGNoX2RiOiByZXBsYWNlZCBnZXRfcG1fdWlkcyBiYXJlIHJldHVybiB3aXRoIHJldHVybiBbXSIpCmVsc2U6CiAgICAjIEZhbGxiYWNrOiByZWdleC1iYXNlZCByZXBsYWNlbWVudCBmb3IgdGhlIGJhcmUgcmV0dXJuIGluc2lkZSBnZXRfcG1fdWlkcwogICAgcGF0dGVybiA9IHJlLmNvbXBpbGUoCiAgICAgICAgcicoYXN5bmMgZGVmIGdldF9wbV91aWRzXChzZWxmXCk6XHMqXG5ccyppZiBzZWxmXC5fcmV0dXJuOlxzKlxuXHMqKXJldHVyblxzKlxuJwogICAgKQogICAgbWF0Y2ggPSBwYXR0ZXJuLnNlYXJjaChzcmMpCiAgICBpZiBtYXRjaDoKICAgICAgICBzcmMgPSBwYXR0ZXJuLnN1YihyJ1wxcmV0dXJuIFtdXG4nLCBzcmMsIGNvdW50PTEpCiAgICAgICAgcHJpbnQoInBhdGNoX2RiOiByZWdleC1iYXNlZCByZXBsYWNlbWVudCBhcHBsaWVkIikKICAgIGVsc2U6CiAgICAgICAgcHJpbnQoInBhdGNoX2RiOiBXQVJOSU5HIC0gdGFyZ2V0IG5vdCBmb3VuZCAoYWxyZWFkeSBwYXRjaGVkPykiKQoKd2l0aCBvcGVuKHBhdGgsICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoc3JjKQpwcmludCgicGF0Y2hfZGI6IGRvbmUiKQo="),

    ('patch_tstream.py', 'bot/core/stream_server.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF90c3RyZWFtLnB5IC0gRml4IHN0cmVhbSAiZmlsZSBpcyBnb25lIiB3aXRoIDN4IHJldHJ5ICsgZGlhZ25vc3RpYyBsb2dnaW5nCgpNb2RpZmllcyBzdHJlYW1fc2VydmVyLnB5IGluLXBsYWNlLiBUaGUgX3NlcnZlKCkgZnVuY3Rpb24gcmFpc2VzCkhUVFBOb3RGb3VuZCgiZmlsZSBpcyBnb25lIikgaW1tZWRpYXRlbHkgd2hlbiBTdHJlYW1Hb25lIGlzIGNhdWdodC4KVGhpcyBwYXRjaCB3cmFwcyB0aGUgb3Blbl9zdHJlYW0oKSBjYWxsIGFuZCB0aGUgcHJvYmUoKSBjYWxscyBpbiBhCnJldHJ5IGxvb3AgKDMgYXR0ZW1wdHMpIHdpdGggZGlhZ25vc3RpYyBsb2dnaW5nLCBzbyB0cmFuc2llbnQgZmlsZS1pZApleHBpcnkgb3IgREMgbWlncmF0aW9uIGlzc3VlcyBkb24ndCBpbW1lZGlhdGVseSBmYWlsIHRoZSBzdHJlYW0uCiIiIgppbXBvcnQgc3lzCgpwYXRoID0gc3lzLmFyZ3ZbMV0Kd2l0aCBvcGVuKHBhdGgsICJyIiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIHNyYyA9IGYucmVhZCgpCgojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQojIDEpIEluamVjdCBhIHJldHJ5IGhlbHBlciBmdW5jdGlvbiBhZnRlciB0aGUgX3Jlc29sdmUgZnVuY3Rpb24KIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KcmV0cnlfaGVscGVyID0gJycnCgphc3luYyBkZWYgX3JldHJ5X29wZW5fc3RyZWFtKGNpZCwgbWlkLCBraW5kLCB2aWV3ZXIsIG1heF9yZXRyaWVzPTMpOgogICAgIiIiUmV0cnkgb3Blbl9zdHJlYW0gdXAgdG8gbWF4X3JldHJpZXMgdGltZXMgd2l0aCBkaWFnbm9zdGljIGxvZ2dpbmcuIiIiCiAgICBpbXBvcnQgYXN5bmNpbwogICAgbGFzdF9leGMgPSBOb25lCiAgICBmb3IgYXR0ZW1wdCBpbiByYW5nZSgxLCBtYXhfcmV0cmllcyArIDEpOgogICAgICAgIHRyeToKICAgICAgICAgICAgc3QgPSBhd2FpdCBvcGVuX3N0cmVhbShjaWQsIG1pZCwga2luZCwgdmlld2VyPXZpZXdlcikKICAgICAgICAgICAgaWYgYXR0ZW1wdCA+IDE6CiAgICAgICAgICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgICAgICAgICBmInN0cmVhbV9yZXRyeTogc3VjY2VlZGVkIG9uIGF0dGVtcHQge2F0dGVtcHR9L3ttYXhfcmV0cmllc30gIgogICAgICAgICAgICAgICAgICAgIGYiZm9yIHtjaWR9L3ttaWR9IgogICAgICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4gc3QKICAgICAgICBleGNlcHQgU3RyZWFtR29uZSBhcyBlOgogICAgICAgICAgICBsYXN0X2V4YyA9IGUKICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoCiAgICAgICAgICAgICAgICBmInN0cmVhbV9yZXRyeTogU3RyZWFtR29uZSBhdHRlbXB0IHthdHRlbXB0fS97bWF4X3JldHJpZXN9ICIKICAgICAgICAgICAgICAgIGYiZm9yIHtjaWR9L3ttaWR9OiB7ZX0iCiAgICAgICAgICAgICkKICAgICAgICAgICAgaWYgYXR0ZW1wdCA8IG1heF9yZXRyaWVzOgogICAgICAgICAgICAgICAgcHVyZ2VfZmlkKGNpZCwgbWlkKQogICAgICAgICAgICAgICAgYXdhaXQgYXN5bmNpby5zbGVlcCgxLjAgKiBhdHRlbXB0KQogICAgICAgIGV4Y2VwdCBOb0NsaWVudEF2YWlsYWJsZSBhcyBlOgogICAgICAgICAgICBsYXN0X2V4YyA9IGUKICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoCiAgICAgICAgICAgICAgICBmInN0cmVhbV9yZXRyeTogTm9DbGllbnRBdmFpbGFibGUgYXR0ZW1wdCB7YXR0ZW1wdH0ve21heF9yZXRyaWVzfSAiCiAgICAgICAgICAgICAgICBmImZvciB7Y2lkfS97bWlkfToge2V9IgogICAgICAgICAgICApCiAgICAgICAgICAgIGlmIGF0dGVtcHQgPCBtYXhfcmV0cmllczoKICAgICAgICAgICAgICAgIGF3YWl0IGFzeW5jaW8uc2xlZXAoMi4wICogYXR0ZW1wdCkKICAgICAgICBleGNlcHQgU3RyZWFtQWJvcnQgYXMgZToKICAgICAgICAgICAgbGFzdF9leGMgPSBlCiAgICAgICAgICAgIExPR0dFUi53YXJuaW5nKAogICAgICAgICAgICAgICAgZiJzdHJlYW1fcmV0cnk6IFN0cmVhbUFib3J0IGF0dGVtcHQge2F0dGVtcHR9L3ttYXhfcmV0cmllc30gIgogICAgICAgICAgICAgICAgZiJmb3Ige2NpZH0ve21pZH06IHtlfSIKICAgICAgICAgICAgKQogICAgICAgICAgICBpZiBhdHRlbXB0IDwgbWF4X3JldHJpZXM6CiAgICAgICAgICAgICAgICBhd2FpdCBhc3luY2lvLnNsZWVwKDEuMCAqIGF0dGVtcHQpCiAgICByYWlzZSBsYXN0X2V4YwoKCmFzeW5jIGRlZiBfcmV0cnlfcHJvYmUoY2lkLCBtaWQsIG1heF9yZXRyaWVzPTMpOgogICAgIiIiUmV0cnkgcHJvYmUgdXAgdG8gbWF4X3JldHJpZXMgdGltZXMgd2l0aCBkaWFnbm9zdGljIGxvZ2dpbmcuIiIiCiAgICBpbXBvcnQgYXN5bmNpbwogICAgbGFzdF9leGMgPSBOb25lCiAgICBmb3IgYXR0ZW1wdCBpbiByYW5nZSgxLCBtYXhfcmV0cmllcyArIDEpOgogICAgICAgIHRyeToKICAgICAgICAgICAgaW5mbyA9IGF3YWl0IHByb2JlKGNpZCwgbWlkKQogICAgICAgICAgICBpZiBhdHRlbXB0ID4gMToKICAgICAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgICAgIGYicHJvYmVfcmV0cnk6IHN1Y2NlZWRlZCBvbiBhdHRlbXB0IHthdHRlbXB0fS97bWF4X3JldHJpZXN9ICIKICAgICAgICAgICAgICAgICAgICBmImZvciB7Y2lkfS97bWlkfSIKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuIGluZm8KICAgICAgICBleGNlcHQgU3RyZWFtR29uZSBhcyBlOgogICAgICAgICAgICBsYXN0X2V4YyA9IGUKICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoCiAgICAgICAgICAgICAgICBmInByb2JlX3JldHJ5OiBTdHJlYW1Hb25lIGF0dGVtcHQge2F0dGVtcHR9L3ttYXhfcmV0cmllc30gIgogICAgICAgICAgICAgICAgZiJmb3Ige2NpZH0ve21pZH06IHtlfSIKICAgICAgICAgICAgKQogICAgICAgICAgICBpZiBhdHRlbXB0IDwgbWF4X3JldHJpZXM6CiAgICAgICAgICAgICAgICBwdXJnZV9maWQoY2lkLCBtaWQpCiAgICAgICAgICAgICAgICBhd2FpdCBhc3luY2lvLnNsZWVwKDEuMCAqIGF0dGVtcHQpCiAgICAgICAgZXhjZXB0IE5vQ2xpZW50QXZhaWxhYmxlIGFzIGU6CiAgICAgICAgICAgIGxhc3RfZXhjID0gZQogICAgICAgICAgICBMT0dHRVIud2FybmluZygKICAgICAgICAgICAgICAgIGYicHJvYmVfcmV0cnk6IE5vQ2xpZW50QXZhaWxhYmxlIGF0dGVtcHQge2F0dGVtcHR9L3ttYXhfcmV0cmllc30gIgogICAgICAgICAgICAgICAgZiJmb3Ige2NpZH0ve21pZH06IHtlfSIKICAgICAgICAgICAgKQogICAgICAgICAgICBpZiBhdHRlbXB0IDwgbWF4X3JldHJpZXM6CiAgICAgICAgICAgICAgICBhd2FpdCBhc3luY2lvLnNsZWVwKDIuMCAqIGF0dGVtcHQpCiAgICByYWlzZSBsYXN0X2V4YwonJycKCiMgSW5zZXJ0IGFmdGVyIHRoZSBfcmVzb2x2ZSBmdW5jdGlvbiBkZWZpbml0aW9uCnJlc29sdmVfZW5kID0gJyAgICByZXR1cm4gdG9rZW4sIGZvdW5kWzBdLCBmb3VuZFsxXVxuJwppZiByZXNvbHZlX2VuZCBpbiBzcmMgYW5kICdfcmV0cnlfb3Blbl9zdHJlYW0nIG5vdCBpbiBzcmM6CiAgICBzcmMgPSBzcmMucmVwbGFjZShyZXNvbHZlX2VuZCwgcmVzb2x2ZV9lbmQgKyByZXRyeV9oZWxwZXIsIDEpCiAgICBwcmludCgicGF0Y2hfdHN0cmVhbTogaW5qZWN0ZWQgcmV0cnkgaGVscGVyIGZ1bmN0aW9ucyIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfdHN0cmVhbTogcmV0cnkgaGVscGVyIGFscmVhZHkgcHJlc2VudCBvciBfcmVzb2x2ZSBub3QgZm91bmQiKQoKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KIyAyKSBSZXBsYWNlIGRpcmVjdCBwcm9iZSgpIGNhbGxzIGluIF9zZXJ2ZSB3aXRoIF9yZXRyeV9wcm9iZSgpCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCm9sZF9oZWFkX3Byb2JlID0gKAogICAgJyAgICBpZiByZXF1ZXN0Lm1ldGhvZCA9PSAiSEVBRCI6XG4nCiAgICAnICAgICAgICB0cnk6XG4nCiAgICAnICAgICAgICAgICAgaW5mbyA9IGF3YWl0IHByb2JlKGNpZCwgbWlkKVxuJwogICAgJyAgICAgICAgZXhjZXB0IFN0cmVhbUdvbmU6XG4nCiAgICAnICAgICAgICAgICAgcHVyZ2VfZmlkKGNpZCwgbWlkKVxuJwogICAgJyAgICAgICAgICAgIHJhaXNlIHdlYi5IVFRQTm90Rm91bmQodGV4dD0iZmlsZSBpcyBnb25lIikgZnJvbSBOb25lXG4nCiAgICAnICAgICAgICBleGNlcHQgTm9DbGllbnRBdmFpbGFibGUgYXMgZTpcbicKICAgICcgICAgICAgICAgICByYWlzZSB3ZWIuSFRUUFNlcnZpY2VVbmF2YWlsYWJsZSh0ZXh0PXN0cihlKSkgZnJvbSBOb25lJwopCm5ld19oZWFkX3Byb2JlID0gKAogICAgJyAgICBpZiByZXF1ZXN0Lm1ldGhvZCA9PSAiSEVBRCI6XG4nCiAgICAnICAgICAgICB0cnk6XG4nCiAgICAnICAgICAgICAgICAgaW5mbyA9IGF3YWl0IF9yZXRyeV9wcm9iZShjaWQsIG1pZClcbicKICAgICcgICAgICAgIGV4Y2VwdCBTdHJlYW1Hb25lOlxuJwogICAgJyAgICAgICAgICAgIHB1cmdlX2ZpZChjaWQsIG1pZClcbicKICAgICcgICAgICAgICAgICBMT0dHRVIuZXJyb3IoZiJzdHJlYW1faGVhZDogZmlsZSBpcyBnb25lIGFmdGVyIHJldHJpZXM6IHtjaWR9L3ttaWR9IilcbicKICAgICcgICAgICAgICAgICByYWlzZSB3ZWIuSFRUUE5vdEZvdW5kKHRleHQ9ImZpbGUgaXMgZ29uZSIpIGZyb20gTm9uZVxuJwogICAgJyAgICAgICAgZXhjZXB0IE5vQ2xpZW50QXZhaWxhYmxlIGFzIGU6XG4nCiAgICAnICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYic3RyZWFtX2hlYWQ6IG5vIGNsaWVudCBhdmFpbGFibGU6IHtjaWR9L3ttaWR9OiB7ZX0iKVxuJwogICAgJyAgICAgICAgICAgIHJhaXNlIHdlYi5IVFRQU2VydmljZVVuYXZhaWxhYmxlKHRleHQ9c3RyKGUpKSBmcm9tIE5vbmUnCikKCmlmIG9sZF9oZWFkX3Byb2JlIGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZF9oZWFkX3Byb2JlLCBuZXdfaGVhZF9wcm9iZSwgMSkKICAgIHByaW50KCJwYXRjaF90c3RyZWFtOiBwYXRjaGVkIEhFQUQgcHJvYmUgd2l0aCByZXRyeSIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfdHN0cmVhbTogSEVBRCBwcm9iZSB0YXJnZXQgbm90IGZvdW5kIChhbHJlYWR5IHBhdGNoZWQ/KSIpCgojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQojIDMpIFJlcGxhY2UgZGlyZWN0IG9wZW5fc3RyZWFtKCkgY2FsbCBpbiBfc2VydmUgd2l0aCBfcmV0cnlfb3Blbl9zdHJlYW0oKQojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQpvbGRfb3BlbiA9ICgKICAgICcgICAgdHJ5OlxuJwogICAgJyAgICAgICAgc3QgPSBhd2FpdCBvcGVuX3N0cmVhbShjaWQsIG1pZCwga2luZCwgdmlld2VyPXZpZXdlcilcbicKICAgICcgICAgZXhjZXB0IFN0cmVhbUdvbmU6XG4nCiAgICAnICAgICAgICBwdXJnZV9maWQoY2lkLCBtaWQpXG4nCiAgICAnICAgICAgICByYWlzZSB3ZWIuSFRUUE5vdEZvdW5kKHRleHQ9ImZpbGUgaXMgZ29uZSIpIGZyb20gTm9uZVxuJwogICAgJyAgICBleGNlcHQgTm9DbGllbnRBdmFpbGFibGUgYXMgZTpcbicKICAgICcgICAgICAgIHJhaXNlIHdlYi5IVFRQU2VydmljZVVuYXZhaWxhYmxlKHRleHQ9c3RyKGUpLCBoZWFkZXJzPXsiUmV0cnktQWZ0ZXIiOiAiMTAifSlcbicKICAgICcgICAgZXhjZXB0IFN0cmVhbUFib3J0IGFzIGU6JwopCm5ld19vcGVuID0gKAogICAgJyAgICB0cnk6XG4nCiAgICAnICAgICAgICBzdCA9IGF3YWl0IF9yZXRyeV9vcGVuX3N0cmVhbShjaWQsIG1pZCwga2luZCwgdmlld2VyKVxuJwogICAgJyAgICBleGNlcHQgU3RyZWFtR29uZTpcbicKICAgICcgICAgICAgIHB1cmdlX2ZpZChjaWQsIG1pZClcbicKICAgICcgICAgICAgIExPR0dFUi5lcnJvcihmInN0cmVhbV9zZXJ2ZTogZmlsZSBpcyBnb25lIGFmdGVyIHJldHJpZXM6IHtjaWR9L3ttaWR9IilcbicKICAgICcgICAgICAgIHJhaXNlIHdlYi5IVFRQTm90Rm91bmQodGV4dD0iZmlsZSBpcyBnb25lIikgZnJvbSBOb25lXG4nCiAgICAnICAgIGV4Y2VwdCBOb0NsaWVudEF2YWlsYWJsZSBhcyBlOlxuJwogICAgJyAgICAgICAgTE9HR0VSLmVycm9yKGYic3RyZWFtX3NlcnZlOiBubyBjbGllbnQgYWZ0ZXIgcmV0cmllczoge2NpZH0ve21pZH06IHtlfSIpXG4nCiAgICAnICAgICAgICByYWlzZSB3ZWIuSFRUUFNlcnZpY2VVbmF2YWlsYWJsZSh0ZXh0PXN0cihlKSwgaGVhZGVycz17IlJldHJ5LUFmdGVyIjogIjEwIn0pXG4nCiAgICAnICAgIGV4Y2VwdCBTdHJlYW1BYm9ydCBhcyBlOicKKQoKaWYgb2xkX29wZW4gaW4gc3JjOgogICAgc3JjID0gc3JjLnJlcGxhY2Uob2xkX29wZW4sIG5ld19vcGVuLCAxKQogICAgcHJpbnQoInBhdGNoX3RzdHJlYW06IHBhdGNoZWQgb3Blbl9zdHJlYW0gd2l0aCByZXRyeSIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfdHN0cmVhbTogb3Blbl9zdHJlYW0gdGFyZ2V0IG5vdCBmb3VuZCAoYWxyZWFkeSBwYXRjaGVkPykiKQoKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KIyA0KSBSZXBsYWNlIHByb2JlKCkgaW4gX21ldGEgd2l0aCBfcmV0cnlfcHJvYmUoKQojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQpvbGRfbWV0YV9wcm9iZSA9ICgKICAgICcgICAgdHJ5OlxuJwogICAgJyAgICAgICAgaW5mbyA9IGF3YWl0IHByb2JlKGNpZCwgbWlkKVxuJwogICAgJyAgICBleGNlcHQgU3RyZWFtR29uZTpcbicKICAgICcgICAgICAgIHB1cmdlX2ZpZChjaWQsIG1pZClcbicKICAgICcgICAgICAgIHJhaXNlIHdlYi5IVFRQTm90Rm91bmQodGV4dD0iZmlsZSBpcyBnb25lIikgZnJvbSBOb25lXG4nCiAgICAnICAgIGV4Y2VwdCBOb0NsaWVudEF2YWlsYWJsZSBhcyBlOlxuJwogICAgJyAgICAgICAgcmFpc2Ugd2ViLkhUVFBTZXJ2aWNlVW5hdmFpbGFibGUodGV4dD1zdHIoZSkpIGZyb20gTm9uZScKKQpuZXdfbWV0YV9wcm9iZSA9ICgKICAgICcgICAgdHJ5OlxuJwogICAgJyAgICAgICAgaW5mbyA9IGF3YWl0IF9yZXRyeV9wcm9iZShjaWQsIG1pZClcbicKICAgICcgICAgZXhjZXB0IFN0cmVhbUdvbmU6XG4nCiAgICAnICAgICAgICBwdXJnZV9maWQoY2lkLCBtaWQpXG4nCiAgICAnICAgICAgICBMT0dHRVIuZXJyb3IoZiJzdHJlYW1fbWV0YTogZmlsZSBpcyBnb25lIGFmdGVyIHJldHJpZXM6IHtjaWR9L3ttaWR9IilcbicKICAgICcgICAgICAgIHJhaXNlIHdlYi5IVFRQTm90Rm91bmQodGV4dD0iZmlsZSBpcyBnb25lIikgZnJvbSBOb25lXG4nCiAgICAnICAgIGV4Y2VwdCBOb0NsaWVudEF2YWlsYWJsZSBhcyBlOlxuJwogICAgJyAgICAgICAgTE9HR0VSLmVycm9yKGYic3RyZWFtX21ldGE6IG5vIGNsaWVudCBhZnRlciByZXRyaWVzOiB7Y2lkfS97bWlkfToge2V9IilcbicKICAgICcgICAgICAgIHJhaXNlIHdlYi5IVFRQU2VydmljZVVuYXZhaWxhYmxlKHRleHQ9c3RyKGUpKSBmcm9tIE5vbmUnCikKCmlmIG9sZF9tZXRhX3Byb2JlIGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZF9tZXRhX3Byb2JlLCBuZXdfbWV0YV9wcm9iZSwgMSkKICAgIHByaW50KCJwYXRjaF90c3RyZWFtOiBwYXRjaGVkIF9tZXRhIHByb2JlIHdpdGggcmV0cnkiKQplbHNlOgogICAgcHJpbnQoInBhdGNoX3RzdHJlYW06IF9tZXRhIHByb2JlIHRhcmdldCBub3QgZm91bmQgKGFscmVhZHkgcGF0Y2hlZD8pIikKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKHNyYykKcHJpbnQoInBhdGNoX3RzdHJlYW06IGRvbmUiKQo="),

    ('patch_sserv.py', 'bot/core/stream_server.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9zc2Vydi5weSAtIEFkZCBkaWFnbm9zdGljIGxvZ2dpbmcgdG8gc3RyZWFtX3NlcnZlci5weQoKTW9kaWZpZXMgc3RyZWFtX3NlcnZlci5weSBpbi1wbGFjZS4gQWRkcyBsb2dnaW5nIHRvIHRoZSBfc2VydmUsIF9tZXRhLAphbmQgX3RyYWNrcyByZXF1ZXN0IGhhbmRsZXJzIHNvIGVhY2ggcmVxdWVzdCBpcyBsb2dnZWQgd2l0aCBtZXRob2QsIHBhdGgsCmFuZCBjbGllbnQgaW5mby4gVGhpcyBhaWRzIGluIGRpYWdub3Npbmcgc3RyZWFtIHBsYXliYWNrIGZhaWx1cmVzLgoiIiIKaW1wb3J0IHN5cwoKcGF0aCA9IHN5cy5hcmd2WzFdCndpdGggb3BlbihwYXRoLCAiciIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBzcmMgPSBmLnJlYWQoKQoKIyBBZGQgYSBkaWFnbm9zdGljIGxvZ2dpbmcgaGVscGVyIGFmdGVyIF9yZXNvbHZlCmxvZ19oZWxwZXIgPSAnJycKCmRlZiBfbG9nX3JlcXVlc3QocmVxdWVzdCwgd2hhdCk6CiAgICAiIiJMb2cgYSBzdHJlYW0gc2VydmVyIHJlcXVlc3QgZm9yIGRpYWdub3N0aWNzLiIiIgogICAgdG9rZW4gPSByZXF1ZXN0Lm1hdGNoX2luZm8uZ2V0KCJ0b2tlbiIsICIiKQogICAgdmlld2VyID0gcmVxdWVzdC5oZWFkZXJzLmdldCgiWC1WaWV3ZXIiKSBvciByZXF1ZXN0LnJlbW90ZSBvciAidW5rbm93biIKICAgIExPR0dFUi5pbmZvKAogICAgICAgIGYic3RyZWFtX3NlcnZlIFt7d2hhdH1dOiB7cmVxdWVzdC5tZXRob2R9IHtyZXF1ZXN0LnBhdGh9ICIKICAgICAgICBmInRva2VuPXt0b2tlbls6OF19Li4uIHZpZXdlcj17dmlld2VyfSIKICAgICkKJycnCgpyZXNvbHZlX2VuZCA9ICcgICAgcmV0dXJuIHRva2VuLCBmb3VuZFswXSwgZm91bmRbMV1cbicKaWYgcmVzb2x2ZV9lbmQgaW4gc3JjIGFuZCAnX2xvZ19yZXF1ZXN0JyBub3QgaW4gc3JjOgogICAgc3JjID0gc3JjLnJlcGxhY2UocmVzb2x2ZV9lbmQsIHJlc29sdmVfZW5kICsgbG9nX2hlbHBlciwgMSkKICAgIHByaW50KCJwYXRjaF9zc2VydjogaW5qZWN0ZWQgX2xvZ19yZXF1ZXN0IGhlbHBlciIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfc3NlcnY6IF9sb2dfcmVxdWVzdCBhbHJlYWR5IHByZXNlbnQgb3IgX3Jlc29sdmUgbm90IGZvdW5kIikKCiMgQWRkIGxvZ2dpbmcgY2FsbHMgYXQgdGhlIHN0YXJ0IG9mIF9zZXJ2ZSwgX21ldGEsIF90cmFja3MKcGF0Y2hlcyA9IFsKICAgICgKICAgICAgICAnYXN5bmMgZGVmIF9tZXRhKHJlcXVlc3QpOlxuICAgIHRva2VuLCBjaWQsIG1pZCA9IGF3YWl0IF9yZXNvbHZlKHJlcXVlc3QpXG4nLAogICAgICAgICdhc3luYyBkZWYgX21ldGEocmVxdWVzdCk6XG4gICAgX2xvZ19yZXF1ZXN0KHJlcXVlc3QsICJtZXRhIilcbiAgICB0b2tlbiwgY2lkLCBtaWQgPSBhd2FpdCBfcmVzb2x2ZShyZXF1ZXN0KVxuJywKICAgICksCiAgICAoCiAgICAgICAgJ2FzeW5jIGRlZiBfc2VydmUocmVxdWVzdCwga2luZCk6XG4gICAgXywgY2lkLCBtaWQgPSBhd2FpdCBfcmVzb2x2ZShyZXF1ZXN0KVxuJywKICAgICAgICAnYXN5bmMgZGVmIF9zZXJ2ZShyZXF1ZXN0LCBraW5kKTpcbiAgICBfbG9nX3JlcXVlc3QocmVxdWVzdCwga2luZClcbiAgICBfLCBjaWQsIG1pZCA9IGF3YWl0IF9yZXNvbHZlKHJlcXVlc3QpXG4nLAogICAgKSwKICAgICgKICAgICAgICAnYXN5bmMgZGVmIF90cmFja3MocmVxdWVzdCk6XG4gICAgXywgY2lkLCBtaWQgPSBhd2FpdCBfcmVzb2x2ZShyZXF1ZXN0KVxuJywKICAgICAgICAnYXN5bmMgZGVmIF90cmFja3MocmVxdWVzdCk6XG4gICAgX2xvZ19yZXF1ZXN0KHJlcXVlc3QsICJ0cmFja3MiKVxuICAgIF8sIGNpZCwgbWlkID0gYXdhaXQgX3Jlc29sdmUocmVxdWVzdClcbicsCiAgICApLApdCgpmb3Igb2xkLCBuZXcgaW4gcGF0Y2hlczoKICAgIGlmIG9sZCBpbiBzcmM6CiAgICAgICAgc3JjID0gc3JjLnJlcGxhY2Uob2xkLCBuZXcsIDEpCiAgICAgICAgZnVuY19uYW1lID0gbmV3LnNwbGl0KCIoIilbMF0uc3RyaXAoKS5zcGxpdCgpWy0xXQogICAgICAgIHByaW50KGYicGF0Y2hfc3NlcnY6IGFkZGVkIGxvZ2dpbmcgdG8ge2Z1bmNfbmFtZX0iKQogICAgZWxzZToKICAgICAgICBmdW5jX25hbWUgPSBvbGQuc3BsaXQoIigiKVswXS5zdHJpcCgpLnNwbGl0KClbLTFdCiAgICAgICAgcHJpbnQoZiJwYXRjaF9zc2VydjogdGFyZ2V0IG5vdCBmb3VuZCBmb3Ige2Z1bmNfbmFtZX0iKQoKIyBBZGQgbG9nZ2luZyB0byBzdGFydF9zdHJlYW1fc2VydmVyCm9sZF9zdGFydCA9ICcgICAgICAgIExPR0dFUi5pbmZvKGYiU3RyZWFtIHNlcnZlciBsaXN0ZW5pbmcgb24gMTI3LjAuMC4xOntwb3J0fSIpJwpuZXdfc3RhcnQgPSAoCiAgICAnICAgICAgICBMT0dHRVIuaW5mbyhmIlN0cmVhbSBzZXJ2ZXIgbGlzdGVuaW5nIG9uIDEyNy4wLjAuMTp7cG9ydH0iKVxuJwogICAgJyAgICAgICAgTE9HR0VSLmluZm8oInN0cmVhbV9zZXJ2ZTogZGlhZ25vc3RpYyBsb2dnaW5nIGVuYWJsZWQgKHBhdGNoX3NzZXJ2KSIpJwopCmlmIG9sZF9zdGFydCBpbiBzcmMgYW5kICdwYXRjaF9zc2Vydicgbm90IGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZF9zdGFydCwgbmV3X3N0YXJ0LCAxKQogICAgcHJpbnQoInBhdGNoX3NzZXJ2OiBhZGRlZCBzdGFydHVwIGRpYWdub3N0aWMgbG9nIikKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKHNyYykKcHJpbnQoInBhdGNoX3NzZXJ2OiBkb25lIikK"),

    ('patch_tmon.py', 'bot/helper/ext_utils/tunnel_monitor.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF90bW9uLnB5IC0gRnVsbCByZXBsYWNlbWVudCBvZiB0dW5uZWxfbW9uaXRvci5weQoKUmVwbGFjZXMgdHVubmVsX21vbml0b3IucHkgZW50aXJlbHkuIFRoZSBvcmlnaW5hbCBibGluZGx5IHNldHMgQ29uZmlnLkJBU0VfVVJMCnRvIHdoYXRldmVyIHR1bm5lbCBVUkwgaXQgcmVhZHMgZnJvbSBhIGZpbGUsIHdoaWNoIGNsb2JiZXJzIHRoZSBzdGFibGUKQ2xvdWRmbGFyZSBXb3JrZXIgVVJMIHdpdGggdGhlIGVwaGVtZXJhbCB0cnljbG91ZGZsYXJlLmNvbSBVUkwuCgpUaGUgbmV3IHZlcnNpb246CiAgLSBSZWFkcyB0aGUgdHVubmVsIFVSTCBmcm9tIFRVTk5FTF9VUkxfRklMRSAoZm9yIGRpYWdub3N0aWMgcHVycG9zZXMpCiAgLSBEb2VzIE5PVCBvdmVycmlkZSBDb25maWcuQkFTRV9VUkwgaWYgaXQncyBhbHJlYWR5IHNldCB0byBhIFdvcmtlciBVUkwKICAtIE9ubHkgdXBkYXRlcyBCQVNFX1VSTCBpZiB0aGUgY3VycmVudCB2YWx1ZSBpcyBlbXB0eSBvciBpcyBhIHN0YWxlCiAgICB0cnljbG91ZGZsYXJlLmNvbSBVUkwgdGhhdCBkaWZmZXJzIGZyb20gdGhlIG5ldyBvbmUKICAtIExvZ3MgYWxsIGRlY2lzaW9ucyBmb3IgZGlhZ25vc3RpY3MKIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQoKbmV3X2NvbnRlbnQgPSAnJydmcm9tIGFzeW5jaW8gaW1wb3J0IHNsZWVwCmZyb20gb3MgaW1wb3J0IGVudmlyb24KCmZyb20gYWlvZmlsZXMgaW1wb3J0IG9wZW4gYXMgYWlvcGVuCmZyb20gYWlvZmlsZXMub3MgaW1wb3J0IHBhdGggYXMgYWlvcGF0aAoKZnJvbSAuLi4gaW1wb3J0IExPR0dFUiwgYm90X2xvb3AKZnJvbSAuLi5jb3JlLmNvbmZpZ19tYW5hZ2VyIGltcG9ydCBDb25maWcKCgpUVU5ORUxfVVJMX0ZJTEUgPSBlbnZpcm9uLmdldCgiVFVOTkVMX1VSTF9GSUxFIiwgIi9kYXRhL3R1bm5lbF91cmwudHh0IikKCgpkZWYgX2lzX3dvcmtlcl91cmwodXJsKToKICAgICIiIkNoZWNrIGlmIGEgVVJMIHBvaW50cyB0byB0aGUgQ2xvdWRmbGFyZSBXb3JrZXIgKHN0YWJsZSBVUkwpLiIiIgogICAgaWYgbm90IHVybDoKICAgICAgICByZXR1cm4gRmFsc2UKICAgICMgV29ya2VyIFVSTHMgYXJlIGN1c3RvbSBkb21haW5zLCBOT1QgdHJ5Y2xvdWRmbGFyZS5jb20KICAgIHJldHVybiAidHJ5Y2xvdWRmbGFyZS5jb20iIG5vdCBpbiB1cmwgYW5kIHVybC5zdGFydHN3aXRoKCJodHRwczovLyIpCgoKZGVmIF9pc190cnljbG91ZGZsYXJlKHVybCk6CiAgICAiIiJDaGVjayBpZiBhIFVSTCBpcyBhIHRyeWNsb3VkZmxhcmUuY29tIHF1aWNrIHR1bm5lbCBVUkwuIiIiCiAgICBpZiBub3QgdXJsOgogICAgICAgIHJldHVybiBGYWxzZQogICAgcmV0dXJuICJ0cnljbG91ZGZsYXJlLmNvbSIgaW4gdXJsCgoKYXN5bmMgZGVmIF9yZWFkX3R1bm5lbF91cmwoKToKICAgIHRyeToKICAgICAgICBpZiBub3QgYXdhaXQgYWlvcGF0aC5pc2ZpbGUoVFVOTkVMX1VSTF9GSUxFKToKICAgICAgICAgICAgcmV0dXJuIE5vbmUKICAgICAgICBhc3luYyB3aXRoIGFpb3BlbihUVU5ORUxfVVJMX0ZJTEUsICJyIikgYXMgZjoKICAgICAgICAgICAgdXJsID0gKGF3YWl0IGYucmVhZCgpKS5zdHJpcCgpCiAgICAgICAgcmV0dXJuIHVybCBvciBOb25lCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VSLndhcm5pbmcoZiJ0dW5uZWxfbW9uaXRvcjogcmVhZCBmYWlsZWQ6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUKCgphc3luYyBkZWYgX3R1bm5lbF9tb25pdG9yX2xvb3AoKToKICAgIExPR0dFUi5pbmZvKCJ0dW5uZWxfbW9uaXRvcjogc3RhcnRlZCAoV29ya2VyIFVSTCBwcm90ZWN0aW9uIGVuYWJsZWQpIikKICAgIHdoaWxlIFRydWU6CiAgICAgICAgdHJ5OgogICAgICAgICAgICB0dW5uZWxfdXJsID0gYXdhaXQgX3JlYWRfdHVubmVsX3VybCgpCiAgICAgICAgICAgIGN1cnJlbnRfYmFzZSA9IHN0cihDb25maWcuQkFTRV9VUkwgb3IgIiIpLnN0cmlwKCkKCiAgICAgICAgICAgIGlmIG5vdCB0dW5uZWxfdXJsOgogICAgICAgICAgICAgICAgYXdhaXQgc2xlZXAoMTApCiAgICAgICAgICAgICAgICBjb250aW51ZQoKICAgICAgICAgICAgIyBJZiBjdXJyZW50IEJBU0VfVVJMIGlzIGEgV29ya2VyIFVSTCwgTkVWRVIgb3ZlcnJpZGUgaXQKICAgICAgICAgICAgaWYgX2lzX3dvcmtlcl91cmwoY3VycmVudF9iYXNlKToKICAgICAgICAgICAgICAgICMgT25seSBsb2cgaWYgdGhlIHR1bm5lbCBVUkwgZGlmZmVycyAoZm9yIGRpYWdub3N0aWNzKQogICAgICAgICAgICAgICAgaWYgdHVubmVsX3VybCAhPSBjdXJyZW50X2Jhc2U6CiAgICAgICAgICAgICAgICAgICAgTE9HR0VSLmRlYnVnKAogICAgICAgICAgICAgICAgICAgICAgICBmInR1bm5lbF9tb25pdG9yOiBwcm90ZWN0aW5nIFdvcmtlciBVUkwgIgogICAgICAgICAgICAgICAgICAgICAgICBmIntjdXJyZW50X2Jhc2V9ICh0dW5uZWw9e3R1bm5lbF91cmx9KSIKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICBhd2FpdCBzbGVlcCgzMCkKICAgICAgICAgICAgICAgIGNvbnRpbnVlCgogICAgICAgICAgICAjIElmIGN1cnJlbnQgQkFTRV9VUkwgaXMgZW1wdHkgb3IgYSBzdGFsZSB0cnljbG91ZGZsYXJlIFVSTCwKICAgICAgICAgICAgIyB3ZSBjYW4gdXBkYXRlIGl0IC0tIGJ1dCBwcmVmZXIgdGhlIFdvcmtlciBVUkwgaWYgYXZhaWxhYmxlCiAgICAgICAgICAgIGlmIG5vdCBjdXJyZW50X2Jhc2Ugb3IgX2lzX3RyeWNsb3VkZmxhcmUoY3VycmVudF9iYXNlKToKICAgICAgICAgICAgICAgIGlmIHR1bm5lbF91cmwgIT0gY3VycmVudF9iYXNlOgogICAgICAgICAgICAgICAgICAgICMgT25seSB1cGRhdGUgaWYgdGhlIG5ldyBVUkwgaXMgZGlmZmVyZW50CiAgICAgICAgICAgICAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAgICAgICAgICAgICAgICAgIGYidHVubmVsX21vbml0b3I6IHVwZGF0aW5nIEJBU0VfVVJMICIKICAgICAgICAgICAgICAgICAgICAgICAgZiJvbGQ9e2N1cnJlbnRfYmFzZSBvciAiKGVtcHR5KSJ9IG5ldz17dHVubmVsX3VybH0iCiAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgICAgIENvbmZpZy5CQVNFX1VSTCA9IHR1bm5lbF91cmwKICAgICAgICAgICAgICAgIGF3YWl0IHNsZWVwKDE1KQogICAgICAgICAgICAgICAgY29udGludWUKCiAgICAgICAgICAgICMgSWYgY3VycmVudCBCQVNFX1VSTCBpcyBzb21ldGhpbmcgZWxzZSBlbnRpcmVseSwgbGVhdmUgaXQgYWxvbmUKICAgICAgICAgICAgYXdhaXQgc2xlZXAoMzApCgogICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYidHVubmVsX21vbml0b3I6IHtlfSIpCiAgICAgICAgICAgIGF3YWl0IHNsZWVwKDEwKQoKCmFzeW5jIGRlZiBhcHBseV90dW5uZWxfdXJsX29uY2UoKToKICAgIHR1bm5lbF91cmwgPSBhd2FpdCBfcmVhZF90dW5uZWxfdXJsKCkKICAgIGlmIHR1bm5lbF91cmw6CiAgICAgICAgY3VycmVudF9iYXNlID0gc3RyKENvbmZpZy5CQVNFX1VSTCBvciAiIikuc3RyaXAoKQogICAgICAgIGlmIF9pc193b3JrZXJfdXJsKGN1cnJlbnRfYmFzZSk6CiAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgZiJ0dW5uZWxfbW9uaXRvcjoga2VlcGluZyBXb3JrZXIgVVJMIHtjdXJyZW50X2Jhc2V9ICIKICAgICAgICAgICAgICAgIGYiKHR1bm5lbD17dHVubmVsX3VybH0pIgogICAgICAgICAgICApCiAgICAgICAgZWxpZiBub3QgY3VycmVudF9iYXNlIG9yIF9pc190cnljbG91ZGZsYXJlKGN1cnJlbnRfYmFzZSk6CiAgICAgICAgICAgIENvbmZpZy5CQVNFX1VSTCA9IHR1bm5lbF91cmwKICAgICAgICAgICAgTE9HR0VSLmluZm8oZiJ0dW5uZWxfbW9uaXRvcjogaW5pdGlhbCBCQVNFX1VSTCA9IHt0dW5uZWxfdXJsfSIpCiAgICByZXR1cm4gQ29uZmlnLkJBU0VfVVJMCgoKZGVmIHN0YXJ0X3R1bm5lbF9tb25pdG9yKCk6CiAgICBib3RfbG9vcC5jcmVhdGVfdGFzayhfdHVubmVsX21vbml0b3JfbG9vcCgpKQogICAgTE9HR0VSLmluZm8oInR1bm5lbF9tb25pdG9yOiBiYWNrZ3JvdW5kIG1vbml0b3Igc3RhcnRlZCAoV29ya2VyLXByb3RlY3RlZCkiKQonJycKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKG5ld19jb250ZW50KQpwcmludCgicGF0Y2hfdG1vbjogdHVubmVsX21vbml0b3IucHkgZnVsbHkgcmVwbGFjZWQgKFdvcmtlciBVUkwgcHJvdGVjdGlvbikiKQo="),

    ('patch_cm.py', 'bot/core/config_manager.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaF9jbS5weSAtIFByZXZlbnQgTW9uZ29EQiBmcm9tIG92ZXJyaWRpbmcgQkFTRV9VUkwgd2l0aCB0cnljbG91ZGZsYXJlIFVSTHMKCk1vZGlmaWVzIGNvbmZpZ19tYW5hZ2VyLnB5IGluLXBsYWNlLiBXaGVuIHRoZSBib3QgbG9hZHMgY29uZmlnIGZyb20gTW9uZ29EQgoodmlhIGxvYWRfZGljdCksIGEgc3RhbGUgQkFTRV9VUkwgY29udGFpbmluZyAidHJ5Y2xvdWRmbGFyZS5jb20iIGZyb20gYQpwcmV2aW91cyBydW4gd291bGQgb3ZlcnJpZGUgdGhlIHN0YWJsZSBXb3JrZXIgVVJMIGluamVjdGVkIGJ5IHRoaXMgbm90ZWJvb2suCgpUaGlzIHBhdGNoIGFkZHMgYSBndWFyZDogaWYgdGhlIEJBU0VfVVJMIHZhbHVlIGZyb20gTW9uZ29EQiBjb250YWlucwoidHJ5Y2xvdWRmbGFyZS5jb20iLCBpdCBpcyBza2lwcGVkIChub3QgbG9hZGVkKSwgcHJlc2VydmluZyB0aGUgV29ya2VyIFVSTC4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCiMgVGFyZ2V0IHRoZSBsb2FkX2RpY3QgbWV0aG9kIC0tIGFkZCBhIGd1YXJkIGZvciBCQVNFX1VSTApvbGRfYmxvY2sgPSAoCiAgICAnICAgIGRlZiBsb2FkX2RpY3QoY2xzLCBjb25maWdfZGljdCk6XG4nCiAgICAnICAgICAgICBmb3Iga2V5LCB2YWx1ZSBpbiBjb25maWdfZGljdC5pdGVtcygpOlxuJwogICAgJyAgICAgICAgICAgIGlmIGhhc2F0dHIoY2xzLCBrZXkpOlxuJwogICAgJyAgICAgICAgICAgICAgICBpZiBrZXkgPT0gIkRFRkFVTFRfVVBMT0FEIiBhbmQgdmFsdWUgIT0gImdkIjpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gInJjIlxuJwogICAgJyAgICAgICAgICAgICAgICBlbGlmIGtleSBpbiBbXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiQkFTRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIlJDTE9ORV9TRVJWRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIklOREVYX1VSTCIsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiU0VBUkNIX0FQSV9MSU5LIixcbicKICAgICcgICAgICAgICAgICAgICAgXTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIGlmIHZhbHVlOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gdmFsdWUuc3RyaXAoIi8iKScKKQpuZXdfYmxvY2sgPSAoCiAgICAnICAgIGRlZiBsb2FkX2RpY3QoY2xzLCBjb25maWdfZGljdCk6XG4nCiAgICAnICAgICAgICBmb3Iga2V5LCB2YWx1ZSBpbiBjb25maWdfZGljdC5pdGVtcygpOlxuJwogICAgJyAgICAgICAgICAgIGlmIGhhc2F0dHIoY2xzLCBrZXkpOlxuJwogICAgJyAgICAgICAgICAgICAgICBpZiBrZXkgPT0gIkRFRkFVTFRfVVBMT0FEIiBhbmQgdmFsdWUgIT0gImdkIjpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gInJjIlxuJwogICAgJyAgICAgICAgICAgICAgICBlbGlmIGtleSBpbiBbXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiQkFTRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIlJDTE9ORV9TRVJWRV9VUkwiLFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIklOREVYX1VSTCIsXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAiU0VBUkNIX0FQSV9MSU5LIixcbicKICAgICcgICAgICAgICAgICAgICAgXTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIGlmIHZhbHVlOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIHZhbHVlID0gdmFsdWUuc3RyaXAoIi8iKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIyBHdWFyZDogbmV2ZXIgbGV0IE1vbmdvREIgb3ZlcnJpZGUgQkFTRV9VUkwgd2l0aCBhXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAjIHRyeWNsb3VkZmxhcmUuY29tIHF1aWNrLXR1bm5lbCBVUkwuIFRoZSBub3RlYm9va1xuJwogICAgJyAgICAgICAgICAgICAgICAgICAgIyBpbmplY3RzIGEgc3RhYmxlIFdvcmtlciBVUkw7IE1vbmdvREIgbWF5IGNhcnJ5IGFcbicKICAgICcgICAgICAgICAgICAgICAgICAgICMgc3RhbGUgdHJ5Y2xvdWRmbGFyZSBVUkwgZnJvbSBhIHByZXZpb3VzIHJ1bi5cbicKICAgICcgICAgICAgICAgICAgICAgICAgIGlmIChcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICBrZXkgPT0gIkJBU0VfVVJMIlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIGFuZCBpc2luc3RhbmNlKHZhbHVlLCBzdHIpXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgYW5kICJ0cnljbG91ZGZsYXJlLmNvbSIgaW4gdmFsdWVcbicKICAgICcgICAgICAgICAgICAgICAgICAgICk6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICAgICAgaW1wb3J0IGxvZ2dpbmdcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICBsb2dnaW5nLmdldExvZ2dlcihfX25hbWVfXykud2FybmluZyhcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgImNvbmZpZ19tYW5hZ2VyOiBza2lwcGluZyBzdGFsZSB0cnljbG91ZGZsYXJlICJcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAgICAgIkJBU0VfVVJMIGZyb20gTW9uZ29EQjogJXMiLCB2YWx1ZVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgIClcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICBjb250aW51ZScKKQoKaWYgb2xkX2Jsb2NrIGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZF9ibG9jaywgbmV3X2Jsb2NrLCAxKQogICAgcHJpbnQoInBhdGNoX2NtOiBhZGRlZCB0cnljbG91ZGZsYXJlIGd1YXJkIHRvIGxvYWRfZGljdCIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfY206IFdBUk5JTkcgLS0gbG9hZF9kaWN0IHRhcmdldCBub3QgZm91bmQgKGFscmVhZHkgcGF0Y2hlZD8pIikKCiMgQWxzbyBwYXRjaCBsb2FkX2NvbmZpZygpIGZvciB0aGUgc2FtZSBwcm90ZWN0aW9uCm9sZF9sb2FkX2NvbmZpZyA9ICgKICAgICcgICAgQGNsYXNzbWV0aG9kXG4nCiAgICAnICAgIGRlZiBsb2FkX2NvbmZpZyhjbHMpOlxuJwogICAgJyAgICAgICAgdHJ5OlxuJwogICAgJyAgICAgICAgICAgIHNldHRpbmdzID0gaW1wb3J0X21vZHVsZSgiY29uZmlnIilcbicKICAgICcgICAgICAgIGV4Y2VwdCBNb2R1bGVOb3RGb3VuZEVycm9yOlxuJwogICAgJyAgICAgICAgICAgIHJldHVyblxuJwogICAgJyAgICAgICAgZm9yIGF0dHIgaW4gZGlyKHNldHRpbmdzKTpcbicKICAgICcgICAgICAgICAgICBpZiBoYXNhdHRyKGNscywgYXR0cik6XG4nCiAgICAnICAgICAgICAgICAgICAgIHZhbHVlID0gZ2V0YXR0cihzZXR0aW5ncywgYXR0cilcbicKICAgICcgICAgICAgICAgICAgICAgaWYgbm90IHZhbHVlOlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgY29udGludWUnCikKbmV3X2xvYWRfY29uZmlnID0gKAogICAgJyAgICBAY2xhc3NtZXRob2RcbicKICAgICcgICAgZGVmIGxvYWRfY29uZmlnKGNscyk6XG4nCiAgICAnICAgICAgICB0cnk6XG4nCiAgICAnICAgICAgICAgICAgc2V0dGluZ3MgPSBpbXBvcnRfbW9kdWxlKCJjb25maWciKVxuJwogICAgJyAgICAgICAgZXhjZXB0IE1vZHVsZU5vdEZvdW5kRXJyb3I6XG4nCiAgICAnICAgICAgICAgICAgcmV0dXJuXG4nCiAgICAnICAgICAgICBmb3IgYXR0ciBpbiBkaXIoc2V0dGluZ3MpOlxuJwogICAgJyAgICAgICAgICAgIGlmIGhhc2F0dHIoY2xzLCBhdHRyKTpcbicKICAgICcgICAgICAgICAgICAgICAgdmFsdWUgPSBnZXRhdHRyKHNldHRpbmdzLCBhdHRyKVxuJwogICAgJyAgICAgICAgICAgICAgICBpZiBub3QgdmFsdWU6XG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBjb250aW51ZVxuJwogICAgJyAgICAgICAgICAgICAgICAjIEd1YXJkOiBza2lwIHRyeWNsb3VkZmxhcmUgQkFTRV9VUkwgZnJvbSBjb25maWcucHlcbicKICAgICcgICAgICAgICAgICAgICAgaWYgKFxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgYXR0ciA9PSAiQkFTRV9VUkwiXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBhbmQgaXNpbnN0YW5jZSh2YWx1ZSwgc3RyKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgYW5kICJ0cnljbG91ZGZsYXJlLmNvbSIgaW4gdmFsdWVcbicKICAgICcgICAgICAgICAgICAgICAgKTpcbicKICAgICcgICAgICAgICAgICAgICAgICAgIGltcG9ydCBsb2dnaW5nXG4nCiAgICAnICAgICAgICAgICAgICAgICAgICBsb2dnaW5nLmdldExvZ2dlcihfX25hbWVfXykud2FybmluZyhcbicKICAgICcgICAgICAgICAgICAgICAgICAgICAgICAiY29uZmlnX21hbmFnZXI6IHNraXBwaW5nIHRyeWNsb3VkZmxhcmUgIlxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgICAgICJCQVNFX1VSTCBmcm9tIGNvbmZpZy5weTogJXMiLCB2YWx1ZVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgKVxuJwogICAgJyAgICAgICAgICAgICAgICAgICAgY29udGludWUnCikKCmlmIG9sZF9sb2FkX2NvbmZpZyBpbiBzcmM6CiAgICBzcmMgPSBzcmMucmVwbGFjZShvbGRfbG9hZF9jb25maWcsIG5ld19sb2FkX2NvbmZpZywgMSkKICAgIHByaW50KCJwYXRjaF9jbTogYWRkZWQgdHJ5Y2xvdWRmbGFyZSBndWFyZCB0byBsb2FkX2NvbmZpZyIpCmVsc2U6CiAgICBwcmludCgicGF0Y2hfY206IGxvYWRfY29uZmlnIHRhcmdldCBub3QgZm91bmQgKGFscmVhZHkgcGF0Y2hlZD8pIikKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKHNyYykKcHJpbnQoInBhdGNoX2NtOiBkb25lIikK"),

    ('patch7_user.py', 'bot/helper/telegram_helper/tg_stream.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaDdfdXNlci5weSAtIFVzZXJTdHJlYW0gY2xhc3M6IHVzZXIgYWNjb3VudCBzdHJlYW0gZmFsbGJhY2sgd2l0aCByZXRyeQoKTW9kaWZpZXMgdGdfc3RyZWFtLnB5IGluLXBsYWNlLiBXaGVuIGFsbCBib3Qgc3RyZWFtIGNsaWVudHMgYXJlIGJ1c3kgb3IKdW5hdmFpbGFibGUgKE5vQ2xpZW50QXZhaWxhYmxlKSwgdGhlIHN0cmVhbSBmYWxscyBiYWNrIHRvIHRoZSB1c2VyJ3MKUHlyb2dyYW0gc2Vzc2lvbiAoVVNFUl9TRVNTSU9OX1NUUklORykgdG8gc2VydmUgdGhlIGZpbGUuIFRoaXMgY2xhc3MKd3JhcHMgdGhlIHVzZXIgY2xpZW50IHdpdGggcmV0cnkgbG9naWMgZm9yIEZpbGVSZWZlcmVuY2VFeHBpcmVkIGFuZApGaWxlTWlncmF0ZSBlcnJvcnMuCgpBbHNvIG1vZGlmaWVzIG9wZW5fc3RyZWFtKCkgYW5kIHByb2JlKCkgdG8gYXR0ZW1wdCB0aGUgVXNlclN0cmVhbSBmYWxsYmFjawp3aGVuIE5vQ2xpZW50QXZhaWxhYmxlIGlzIHJhaXNlZC4KIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgc3JjID0gZi5yZWFkKCkKCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCiMgMSkgQWRkIHRoZSBVc2VyU3RyZWFtIGNsYXNzIGJlZm9yZSB0aGUgb3Blbl9zdHJlYW0gZnVuY3Rpb24KIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KdXNlcl9zdHJlYW1fY2xhc3MgPSAnJycKCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCiMgVXNlclN0cmVhbTogdXNlciBhY2NvdW50IHN0cmVhbSBmYWxsYmFjayAocGF0Y2g3X3VzZXIpCiMgVXNlZCB3aGVuIGFsbCBib3Qgc3RyZWFtIGNsaWVudHMgYXJlIHVuYXZhaWxhYmxlIChOb0NsaWVudEF2YWlsYWJsZSkuCiMgUmVxdWlyZXMgVVNFUl9TRVNTSU9OX1NUUklORyB0byBiZSBjb25maWd1cmVkLgojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQoKX3VzZXJfY2xpZW50ID0gTm9uZQpfdXNlcl9jbGllbnRfbG9jayA9IE5vbmUKCgpkZWYgX2dldF91c2VyX3N0cmVhbV9jbGllbnQoKToKICAgICIiIkxhemlseSBpbml0aWFsaXplIGEgUHlyb2dyYW0gdXNlciBjbGllbnQgZnJvbSBVU0VSX1NFU1NJT05fU1RSSU5HLiIiIgogICAgZ2xvYmFsIF91c2VyX2NsaWVudCwgX3VzZXJfY2xpZW50X2xvY2sKICAgIGlmIF91c2VyX2NsaWVudCBpcyBub3QgTm9uZToKICAgICAgICByZXR1cm4gX3VzZXJfY2xpZW50CiAgICBzZXNzaW9uX3N0cmluZyA9IGdldGF0dHIoQ29uZmlnLCAiVVNFUl9TRVNTSU9OX1NUUklORyIsICIiKSBvciAiIgogICAgaWYgbm90IHNlc3Npb25fc3RyaW5nOgogICAgICAgIHJldHVybiBOb25lCiAgICBhcGlfaWQgPSBzdHIoZ2V0YXR0cihDb25maWcsICJURUxFR1JBTV9BUEkiLCAiIikgb3IgIiIpCiAgICBhcGlfaGFzaCA9IHN0cihnZXRhdHRyKENvbmZpZywgIlRFTEVHUkFNX0hBU0giLCAiIikgb3IgIiIpCiAgICBpZiBub3QgYXBpX2lkIG9yIG5vdCBhcGlfaGFzaDoKICAgICAgICByZXR1cm4gTm9uZQogICAgdHJ5OgogICAgICAgIGZyb20gcHlyb2dyYW0gaW1wb3J0IENsaWVudAogICAgICAgIF91c2VyX2NsaWVudF9sb2NrID0gX3VzZXJfY2xpZW50X2xvY2sgb3IgX19pbXBvcnRfXygiYXN5bmNpbyIpLkxvY2soKQogICAgICAgIF91c2VyX2NsaWVudCA9IENsaWVudCgKICAgICAgICAgICAgInd6bWx4X3VzZXJfc3RyZWFtIiwKICAgICAgICAgICAgYXBpX2lkPWFwaV9pZCwKICAgICAgICAgICAgYXBpX2hhc2g9YXBpX2hhc2gsCiAgICAgICAgICAgIHNlc3Npb25fc3RyaW5nPXNlc3Npb25fc3RyaW5nLAogICAgICAgICAgICBub191cGRhdGVzPVRydWUsCiAgICAgICAgICAgIGluX21lbW9yeT1UcnVlLAogICAgICAgICkKICAgICAgICBMT0dHRVIuaW5mbygiVXNlclN0cmVhbTogdXNlciBjbGllbnQgaW5pdGlhbGl6ZWQgZm9yIHN0cmVhbSBmYWxsYmFjayIpCiAgICAgICAgcmV0dXJuIF91c2VyX2NsaWVudAogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi53YXJuaW5nKGYiVXNlclN0cmVhbTogZmFpbGVkIHRvIGluaXQgdXNlciBjbGllbnQ6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUKCgphc3luYyBkZWYgX3N0YXJ0X3VzZXJfY2xpZW50KCk6CiAgICAiIiJTdGFydCB0aGUgdXNlciBjbGllbnQgaWYgbm90IGFscmVhZHkgc3RhcnRlZC4iIiIKICAgIGdsb2JhbCBfdXNlcl9jbGllbnQKICAgIGNsaWVudCA9IF9nZXRfdXNlcl9zdHJlYW1fY2xpZW50KCkKICAgIGlmIGNsaWVudCBpcyBOb25lOgogICAgICAgIHJldHVybiBOb25lCiAgICBpZiBub3QgY2xpZW50LmlzX2Nvbm5lY3RlZDoKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IGNsaWVudC5zdGFydCgpCiAgICAgICAgICAgIExPR0dFUi5pbmZvKCJVc2VyU3RyZWFtOiB1c2VyIGNsaWVudCBzdGFydGVkIikKICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgIExPR0dFUi53YXJuaW5nKGYiVXNlclN0cmVhbTogZmFpbGVkIHRvIHN0YXJ0IHVzZXIgY2xpZW50OiB7ZX0iKQogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgcmV0dXJuIGNsaWVudAoKCmNsYXNzIFVzZXJTdHJlYW06CiAgICAiIiJTdHJlYW0gdXNpbmcgdGhlIHVzZXIncyBQeXJvZ3JhbSBzZXNzaW9uIGFzIGZhbGxiYWNrLgoKICAgIEltcGxlbWVudHMgYSBtaW5pbWFsIGludGVyZmFjZSBjb21wYXRpYmxlIHdpdGggSHlwZXJ0Z1N0cmVhbTogb3BlbigpLAogICAgaXRlcl9yYW5nZShzdGFydCwgZW5kKSwgc2l6ZSwgbWltZSwgbmFtZSwgdW5pcXVlX2lkLCBfcmVsZWFzZSgpLgogICAgSW5jbHVkZXMgcmV0cnkgbG9naWMgZm9yIEZpbGVSZWZlcmVuY2VFeHBpcmVkIGFuZCBGaWxlTWlncmF0ZSBlcnJvcnMuCiAgICAiIiIKCiAgICBkZWYgX19pbml0X18oc2VsZiwgY2hhdF9pZCwgbXNnX2lkLCB2aWV3ZXI9Tm9uZSk6CiAgICAgICAgc2VsZi5jaGF0X2lkID0gY2hhdF9pZAogICAgICAgIHNlbGYubXNnX2lkID0gbXNnX2lkCiAgICAgICAgc2VsZi52aWV3ZXIgPSB2aWV3ZXIKICAgICAgICBzZWxmLnNpemUgPSAwCiAgICAgICAgc2VsZi5taW1lID0gIiIKICAgICAgICBzZWxmLm5hbWUgPSAiIgogICAgICAgIHNlbGYudW5pcXVlX2lkID0gIiIKICAgICAgICBzZWxmLl9jbGllbnQgPSBOb25lCiAgICAgICAgc2VsZi5fbXNnID0gTm9uZQogICAgICAgIHNlbGYuX21lZGlhID0gTm9uZQogICAgICAgIHNlbGYuX3JlbGVhc2VkID0gRmFsc2UKICAgICAgICBzZWxmLl9tYXhfcmV0cmllcyA9IDMKCiAgICBhc3luYyBkZWYgb3BlbihzZWxmKToKICAgICAgICAiIiJPcGVuIHRoZSBzdHJlYW0gdmlhIHRoZSB1c2VyIGNsaWVudCB3aXRoIHJldHJ5LiIiIgogICAgICAgIGltcG9ydCBhc3luY2lvCiAgICAgICAgbGFzdF9leGMgPSBOb25lCiAgICAgICAgZm9yIGF0dGVtcHQgaW4gcmFuZ2UoMSwgc2VsZi5fbWF4X3JldHJpZXMgKyAxKToKICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgY2xpZW50ID0gYXdhaXQgX3N0YXJ0X3VzZXJfY2xpZW50KCkKICAgICAgICAgICAgICAgIGlmIGNsaWVudCBpcyBOb25lOgogICAgICAgICAgICAgICAgICAgIHJhaXNlIE5vQ2xpZW50QXZhaWxhYmxlKCJ1c2VyIHNlc3Npb24gbm90IGF2YWlsYWJsZSIpCiAgICAgICAgICAgICAgICBzZWxmLl9jbGllbnQgPSBjbGllbnQKICAgICAgICAgICAgICAgIHNlbGYuX21zZyA9IGF3YWl0IGNsaWVudC5nZXRfbWVzc2FnZXMoc2VsZi5jaGF0X2lkLCBzZWxmLm1zZ19pZCkKICAgICAgICAgICAgICAgIGlmIHNlbGYuX21zZyBpcyBOb25lIG9yIGdldGF0dHIoc2VsZi5fbXNnLCAiZW1wdHkiLCBGYWxzZSk6CiAgICAgICAgICAgICAgICAgICAgcmFpc2UgU3RyZWFtR29uZShmIm1zZyB7c2VsZi5tc2dfaWR9IG1pc3NpbmcgZnJvbSB7c2VsZi5jaGF0X2lkfSIpCiAgICAgICAgICAgICAgICBmcm9tIC50Z190cmFuc2ZlciBpbXBvcnQgbWVkaWFfb2YKICAgICAgICAgICAgICAgIHNlbGYuX21lZGlhID0gbWVkaWFfb2Yoc2VsZi5fbXNnKQogICAgICAgICAgICAgICAgaWYgc2VsZi5fbWVkaWEgaXMgTm9uZToKICAgICAgICAgICAgICAgICAgICByYWlzZSBTdHJlYW1Hb25lKGYibXNnIHtzZWxmLm1zZ19pZH0gaGFzIG5vIG1lZGlhIikKICAgICAgICAgICAgICAgIHNlbGYuc2l6ZSA9IGludChnZXRhdHRyKHNlbGYuX21lZGlhLCAiZmlsZV9zaXplIiwgMCkgb3IgMCkKICAgICAgICAgICAgICAgIHNlbGYubWltZSA9IGdldGF0dHIoc2VsZi5fbWVkaWEsICJtaW1lX3R5cGUiLCAiIikgb3IgIiIKICAgICAgICAgICAgICAgIHNlbGYubmFtZSA9IGdldGF0dHIoc2VsZi5fbWVkaWEsICJmaWxlX25hbWUiLCAiIikgb3IgIiIKICAgICAgICAgICAgICAgIHNlbGYudW5pcXVlX2lkID0gZ2V0YXR0cihzZWxmLl9tZWRpYSwgImZpbGVfdW5pcXVlX2lkIiwgIiIpIG9yICIiCiAgICAgICAgICAgICAgICBpZiBub3Qgc2VsZi5zaXplOgogICAgICAgICAgICAgICAgICAgIHJhaXNlIFN0cmVhbUdvbmUoInVzZXIgc3RyZWFtOiBtZWRpYSBoYXMgbm8gc2l6ZSIpCiAgICAgICAgICAgICAgICBpZiBhdHRlbXB0ID4gMToKICAgICAgICAgICAgICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgICAgICAgICAgICAgZiJVc2VyU3RyZWFtOiBvcGVuZWQgb24gYXR0ZW1wdCB7YXR0ZW1wdH0gIgogICAgICAgICAgICAgICAgICAgICAgICBmImZvciB7c2VsZi5jaGF0X2lkfS97c2VsZi5tc2dfaWR9IgogICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIHJldHVybiBzZWxmCiAgICAgICAgICAgIGV4Y2VwdCBTdHJlYW1Hb25lOgogICAgICAgICAgICAgICAgcmFpc2UKICAgICAgICAgICAgZXhjZXB0IE5vQ2xpZW50QXZhaWxhYmxlOgogICAgICAgICAgICAgICAgcmFpc2UKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICAgICAgbGFzdF9leGMgPSBlCiAgICAgICAgICAgICAgICBMT0dHRVIud2FybmluZygKICAgICAgICAgICAgICAgICAgICBmIlVzZXJTdHJlYW06IG9wZW4gYXR0ZW1wdCB7YXR0ZW1wdH0ve3NlbGYuX21heF9yZXRyaWVzfSAiCiAgICAgICAgICAgICAgICAgICAgZiJmYWlsZWQgZm9yIHtzZWxmLmNoYXRfaWR9L3tzZWxmLm1zZ19pZH06IHtlfSIKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIGlmIGF0dGVtcHQgPCBzZWxmLl9tYXhfcmV0cmllczoKICAgICAgICAgICAgICAgICAgICBhd2FpdCBhc3luY2lvLnNsZWVwKDEuMCAqIGF0dGVtcHQpCiAgICAgICAgcmFpc2UgU3RyZWFtQWJvcnQoZiJVc2VyU3RyZWFtOiBmYWlsZWQgYWZ0ZXIge3NlbGYuX21heF9yZXRyaWVzfSByZXRyaWVzOiB7bGFzdF9leGN9IikKCiAgICBhc3luYyBkZWYgaXRlcl9yYW5nZShzZWxmLCBzdGFydCwgZW5kKToKICAgICAgICAiIiJZaWVsZCBieXRlIGNodW5rcyBmcm9tIFtzdGFydCwgZW5kXSBpbmNsdXNpdmUgdXNpbmcgaXRlcl9kb3dubG9hZC4iIiIKICAgICAgICBpZiBzZWxmLl9tc2cgaXMgTm9uZSBvciBzZWxmLl9tZWRpYSBpcyBOb25lOgogICAgICAgICAgICByYWlzZSBTdHJlYW1BYm9ydCgiVXNlclN0cmVhbTogbm90IG9wZW5lZCIpCiAgICAgICAgb2Zmc2V0ID0gc3RhcnQKICAgICAgICByZW1haW5pbmcgPSBlbmQgLSBzdGFydCArIDEKICAgICAgICB0cnk6CiAgICAgICAgICAgIGFzeW5jIGZvciBjaHVuayBpbiBzZWxmLl9jbGllbnQuaXRlcl9kb3dubG9hZCgKICAgICAgICAgICAgICAgIHNlbGYuX21lZGlhLAogICAgICAgICAgICAgICAgb2Zmc2V0PW9mZnNldCwKICAgICAgICAgICAgICAgIGxpbWl0PXJlbWFpbmluZywKICAgICAgICAgICAgICAgIGNodW5rX3NpemU9MjU2ICogMTAyNCwKICAgICAgICAgICAgKToKICAgICAgICAgICAgICAgIGlmIG5vdCBjaHVuazoKICAgICAgICAgICAgICAgICAgICBicmVhawogICAgICAgICAgICAgICAgeWllbGQgY2h1bmsKICAgICAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgICAgIExPR0dFUi5lcnJvcihmIlVzZXJTdHJlYW06IGl0ZXJfcmFuZ2UgZXJyb3IgZm9yIHtzZWxmLmNoYXRfaWR9L3tzZWxmLm1zZ19pZH06IHtlfSIpCiAgICAgICAgICAgIHJhaXNlIFN0cmVhbUFib3J0KHN0cihlKSkKCiAgICBhc3luYyBkZWYgX3JlbGVhc2Uoc2VsZik6CiAgICAgICAgIiIiUmVsZWFzZSByZXNvdXJjZXMgKHVzZXIgY2xpZW50IHN0YXlzIHJ1bm5pbmcgZm9yIHJldXNlKS4iIiIKICAgICAgICBzZWxmLl9yZWxlYXNlZCA9IFRydWUKJycnCgojIEluc2VydCBiZWZvcmUgdGhlIG9wZW5fc3RyZWFtIGZ1bmN0aW9uCm9wZW5fc3RyZWFtX21hcmtlciA9ICdhc3luYyBkZWYgb3Blbl9zdHJlYW0oY2hhdF9pZCwgbXNnX2lkLCBraW5kLCB2aWV3ZXI9Tm9uZSk6JwppZiBvcGVuX3N0cmVhbV9tYXJrZXIgaW4gc3JjIGFuZCAnY2xhc3MgVXNlclN0cmVhbScgbm90IGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9wZW5fc3RyZWFtX21hcmtlciwgdXNlcl9zdHJlYW1fY2xhc3MgKyAnXG4nICsgb3Blbl9zdHJlYW1fbWFya2VyLCAxKQogICAgcHJpbnQoInBhdGNoN191c2VyOiBpbmplY3RlZCBVc2VyU3RyZWFtIGNsYXNzIikKZWxzZToKICAgIHByaW50KCJwYXRjaDdfdXNlcjogVXNlclN0cmVhbSBhbHJlYWR5IHByZXNlbnQgb3Igb3Blbl9zdHJlYW0gbm90IGZvdW5kIikKCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCiMgMikgTW9kaWZ5IG9wZW5fc3RyZWFtKCkgdG8gdHJ5IFVzZXJTdHJlYW0gZmFsbGJhY2sgb24gTm9DbGllbnRBdmFpbGFibGUKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0Kb2xkX29wZW5fc3RyZWFtID0gKAogICAgJ2FzeW5jIGRlZiBvcGVuX3N0cmVhbShjaGF0X2lkLCBtc2dfaWQsIGtpbmQsIHZpZXdlcj1Ob25lKTpcbicKICAgICcgICAgcmV0dXJuIGF3YWl0IEh5cGVydGdTdHJlYW0oY2hhdF9pZCwgbXNnX2lkLCBwcm9maWxlKGtpbmQpLCB2aWV3ZXI9dmlld2VyKS5vcGVuKCknCikKbmV3X29wZW5fc3RyZWFtID0gKAogICAgJ2FzeW5jIGRlZiBvcGVuX3N0cmVhbShjaGF0X2lkLCBtc2dfaWQsIGtpbmQsIHZpZXdlcj1Ob25lKTpcbicKICAgICcgICAgdHJ5OlxuJwogICAgJyAgICAgICAgcmV0dXJuIGF3YWl0IEh5cGVydGdTdHJlYW0oY2hhdF9pZCwgbXNnX2lkLCBwcm9maWxlKGtpbmQpLCB2aWV3ZXI9dmlld2VyKS5vcGVuKClcbicKICAgICcgICAgZXhjZXB0IE5vQ2xpZW50QXZhaWxhYmxlOlxuJwogICAgJyAgICAgICAgIyBGYWxsYmFjayB0byB1c2VyIGFjY291bnQgc3RyZWFtIChwYXRjaDdfdXNlcilcbicKICAgICcgICAgICAgIExPR0dFUi5pbmZvKFxuJwogICAgJyAgICAgICAgICAgIGYib3Blbl9zdHJlYW06IGJvdCBjbGllbnRzIHVuYXZhaWxhYmxlLCB0cnlpbmcgVXNlclN0cmVhbSAiXG4nCiAgICAnICAgICAgICAgICAgZiJmb3Ige2NoYXRfaWR9L3ttc2dfaWR9IlxuJwogICAgJyAgICAgICAgKVxuJwogICAgJyAgICAgICAgdXMgPSBVc2VyU3RyZWFtKGNoYXRfaWQsIG1zZ19pZCwgdmlld2VyPXZpZXdlcilcbicKICAgICcgICAgICAgIHJldHVybiBhd2FpdCB1cy5vcGVuKCknCikKCmlmIG9sZF9vcGVuX3N0cmVhbSBpbiBzcmM6CiAgICBzcmMgPSBzcmMucmVwbGFjZShvbGRfb3Blbl9zdHJlYW0sIG5ld19vcGVuX3N0cmVhbSwgMSkKICAgIHByaW50KCJwYXRjaDdfdXNlcjogYWRkZWQgVXNlclN0cmVhbSBmYWxsYmFjayB0byBvcGVuX3N0cmVhbSIpCmVsc2U6CiAgICBwcmludCgicGF0Y2g3X3VzZXI6IG9wZW5fc3RyZWFtIHRhcmdldCBub3QgZm91bmQgKGFscmVhZHkgcGF0Y2hlZD8pIikKCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCiMgMykgTW9kaWZ5IHByb2JlKCkgdG8gdHJ5IFVzZXJTdHJlYW0gZmFsbGJhY2sgb24gTm9DbGllbnRBdmFpbGFibGUKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0Kb2xkX3Byb2JlID0gKAogICAgJ2FzeW5jIGRlZiBwcm9iZShjaGF0X2lkLCBtc2dfaWQpOlxuJwogICAgJyAgICBjbGllbnRzLCBfLCBfID0gUE9PTC5yZXNvbHZlKClcbicKICAgICcgICAgaWYgbm90IGNsaWVudHM6XG4nCiAgICAnICAgICAgICByYWlzZSBOb0NsaWVudEF2YWlsYWJsZSgibm8gc3RyZWFtIG9yIGhlbHBlciBib3RzIGFyZSBydW5uaW5nIilcbicKICAgICcgICAgUE9PTC5lbnN1cmVfcG9vbChjbGllbnRzKVxuJwogICAgJyAgICBjaSA9IG5leHQoaXRlcihjbGllbnRzKSlcbicKICAgICcgICAgZmlkID0gYXdhaXQgZ2V0X2ZpZChjaSwgY2xpZW50c1tjaV0sIGNoYXRfaWQsIG1zZ19pZClcbicKICAgICcgICAgcmV0dXJuIHtcbicKICAgICcgICAgICAgICJuYW1lIjogZ2V0YXR0cihmaWQsICJmaWxlX25hbWUiLCAiIikgb3IgIiIsXG4nCiAgICAnICAgICAgICAic2l6ZSI6IGludChnZXRhdHRyKGZpZCwgImZpbGVfc2l6ZSIsIDApIG9yIDApLFxuJwogICAgJyAgICAgICAgIm1pbWUiOiBnZXRhdHRyKGZpZCwgIm1pbWVfdHlwZSIsICIiKSBvciAiIixcbicKICAgICcgICAgICAgICJ1bmlxdWVfaWQiOiBnZXRhdHRyKGZpZCwgInVuaXF1ZV9pZCIsICIiKSBvciAiIixcbicKICAgICcgICAgfScKKQpuZXdfcHJvYmUgPSAoCiAgICAnYXN5bmMgZGVmIHByb2JlKGNoYXRfaWQsIG1zZ19pZCk6XG4nCiAgICAnICAgIHRyeTpcbicKICAgICcgICAgICAgIGNsaWVudHMsIF8sIF8gPSBQT09MLnJlc29sdmUoKVxuJwogICAgJyAgICAgICAgaWYgbm90IGNsaWVudHM6XG4nCiAgICAnICAgICAgICAgICAgcmFpc2UgTm9DbGllbnRBdmFpbGFibGUoIm5vIHN0cmVhbSBvciBoZWxwZXIgYm90cyBhcmUgcnVubmluZyIpXG4nCiAgICAnICAgICAgICBQT09MLmVuc3VyZV9wb29sKGNsaWVudHMpXG4nCiAgICAnICAgICAgICBjaSA9IG5leHQoaXRlcihjbGllbnRzKSlcbicKICAgICcgICAgICAgIGZpZCA9IGF3YWl0IGdldF9maWQoY2ksIGNsaWVudHNbY2ldLCBjaGF0X2lkLCBtc2dfaWQpXG4nCiAgICAnICAgICAgICByZXR1cm4ge1xuJwogICAgJyAgICAgICAgICAgICJuYW1lIjogZ2V0YXR0cihmaWQsICJmaWxlX25hbWUiLCAiIikgb3IgIiIsXG4nCiAgICAnICAgICAgICAgICAgInNpemUiOiBpbnQoZ2V0YXR0cihmaWQsICJmaWxlX3NpemUiLCAwKSBvciAwKSxcbicKICAgICcgICAgICAgICAgICAibWltZSI6IGdldGF0dHIoZmlkLCAibWltZV90eXBlIiwgIiIpIG9yICIiLFxuJwogICAgJyAgICAgICAgICAgICJ1bmlxdWVfaWQiOiBnZXRhdHRyKGZpZCwgInVuaXF1ZV9pZCIsICIiKSBvciAiIixcbicKICAgICcgICAgICAgIH1cbicKICAgICcgICAgZXhjZXB0IE5vQ2xpZW50QXZhaWxhYmxlOlxuJwogICAgJyAgICAgICAgIyBGYWxsYmFjazogcHJvYmUgdmlhIHVzZXIgYWNjb3VudCBzdHJlYW0gKHBhdGNoN191c2VyKVxuJwogICAgJyAgICAgICAgTE9HR0VSLmluZm8oZiJwcm9iZTogYm90IGNsaWVudHMgdW5hdmFpbGFibGUsIHRyeWluZyBVc2VyU3RyZWFtIGZvciB7Y2hhdF9pZH0ve21zZ19pZH0iKVxuJwogICAgJyAgICAgICAgdXMgPSBVc2VyU3RyZWFtKGNoYXRfaWQsIG1zZ19pZClcbicKICAgICcgICAgICAgIGF3YWl0IHVzLm9wZW4oKVxuJwogICAgJyAgICAgICAgdHJ5OlxuJwogICAgJyAgICAgICAgICAgIHJldHVybiB7XG4nCiAgICAnICAgICAgICAgICAgICAgICJuYW1lIjogdXMubmFtZSBvciAiIixcbicKICAgICcgICAgICAgICAgICAgICAgInNpemUiOiBpbnQodXMuc2l6ZSBvciAwKSxcbicKICAgICcgICAgICAgICAgICAgICAgIm1pbWUiOiB1cy5taW1lIG9yICIiLFxuJwogICAgJyAgICAgICAgICAgICAgICAidW5pcXVlX2lkIjogdXMudW5pcXVlX2lkIG9yICIiLFxuJwogICAgJyAgICAgICAgICAgIH1cbicKICAgICcgICAgICAgIGZpbmFsbHk6XG4nCiAgICAnICAgICAgICAgICAgYXdhaXQgdXMuX3JlbGVhc2UoKScKKQoKaWYgb2xkX3Byb2JlIGluIHNyYzoKICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZF9wcm9iZSwgbmV3X3Byb2JlLCAxKQogICAgcHJpbnQoInBhdGNoN191c2VyOiBhZGRlZCBVc2VyU3RyZWFtIGZhbGxiYWNrIHRvIHByb2JlIikKZWxzZToKICAgIHByaW50KCJwYXRjaDdfdXNlcjogcHJvYmUgdGFyZ2V0IG5vdCBmb3VuZCAoYWxyZWFkeSBwYXRjaGVkPykiKQoKd2l0aCBvcGVuKHBhdGgsICJ3IiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIGYud3JpdGUoc3JjKQpwcmludCgicGF0Y2g3X3VzZXI6IGRvbmUiKQo="),

    ('patch8_sserv.py', 'bot/core/stream_server.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaDhfc3NlcnYucHkgLSBTdHJlYW0gc2VydmVyID91c2VyPTEgcGFyYW0gKyBYLVN0cmVhbS1SZXRyeSBoZWFkZXIKCk1vZGlmaWVzIHN0cmVhbV9zZXJ2ZXIucHkgaW4tcGxhY2UuIEFkZHM6CiAgMS4gQWNjZXB0ID91c2VyPTEgcXVlcnkgcGFyYW0gLS0gd2hlbiBwcmVzZW50LCBmb3JjZXMgdGhlIHN0cmVhbSB0byB1c2UKICAgICB0aGUgdXNlciBhY2NvdW50IChVc2VyU3RyZWFtKSBpbnN0ZWFkIG9mIGJvdCBjbGllbnRzLgogIDIuIFgtU3RyZWFtLVJldHJ5IHJlc3BvbnNlIGhlYWRlciAtLSBpbmRpY2F0ZXMgaG93IG1hbnkgcmV0cmllcyB3ZXJlCiAgICAgbmVlZGVkIHRvIG9wZW4gdGhlIHN0cmVhbSAoMCA9IGZpcnN0IHRyeSBzdWNjZWVkZWQpLgoiIiIKaW1wb3J0IHN5cwoKcGF0aCA9IHN5cy5hcmd2WzFdCndpdGggb3BlbihwYXRoLCAiciIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBzcmMgPSBmLnJlYWQoKQoKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KIyAxKSBNb2RpZnkgX3NlcnZlIHRvIGNoZWNrIGZvciA/dXNlcj0xIHBhcmFtCiMgICAgVHJ5IHdpdGggdGhlIF9sb2dfcmVxdWVzdCBsaW5lIChpZiBwYXRjaF9zc2VydiB3YXMgYXBwbGllZCksIGVsc2Ugd2l0aG91dAojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQp2YXJpYW50cyA9IFsKICAgICgKICAgICAgICAnYXN5bmMgZGVmIF9zZXJ2ZShyZXF1ZXN0LCBraW5kKTpcbicKICAgICAgICAnICAgIF9sb2dfcmVxdWVzdChyZXF1ZXN0LCBraW5kKVxuJwogICAgICAgICcgICAgXywgY2lkLCBtaWQgPSBhd2FpdCBfcmVzb2x2ZShyZXF1ZXN0KVxuJwogICAgICAgICcgICAgaW5saW5lID0ga2luZCA9PSAicGxheWJhY2siXG4nCiAgICAgICAgJ1xuJwogICAgICAgICcgICAgdmlld2VyID0gcmVxdWVzdC5oZWFkZXJzLmdldCgiWC1WaWV3ZXIiKSBvciByZXF1ZXN0LnJlbW90ZScsCiAgICAgICAgJ2FzeW5jIGRlZiBfc2VydmUocmVxdWVzdCwga2luZCk6XG4nCiAgICAgICAgJyAgICBfbG9nX3JlcXVlc3QocmVxdWVzdCwga2luZClcbicKICAgICAgICAnICAgIF8sIGNpZCwgbWlkID0gYXdhaXQgX3Jlc29sdmUocmVxdWVzdClcbicKICAgICAgICAnICAgIGlubGluZSA9IGtpbmQgPT0gInBsYXliYWNrIlxuJwogICAgICAgICdcbicKICAgICAgICAnICAgIHZpZXdlciA9IHJlcXVlc3QuaGVhZGVycy5nZXQoIlgtVmlld2VyIikgb3IgcmVxdWVzdC5yZW1vdGVcbicKICAgICAgICAnICAgICMgQ2hlY2sgZm9yID91c2VyPTEgcGFyYW0gdG8gZm9yY2UgdXNlciBhY2NvdW50IHN0cmVhbSAocGF0Y2g4X3NzZXJ2KVxuJwogICAgICAgICcgICAgZm9yY2VfdXNlciA9IHJlcXVlc3QucXVlcnkuZ2V0KCJ1c2VyIiwgIiIpID09ICIxIicKICAgICksCiAgICAoCiAgICAgICAgJ2FzeW5jIGRlZiBfc2VydmUocmVxdWVzdCwga2luZCk6XG4nCiAgICAgICAgJyAgICBfLCBjaWQsIG1pZCA9IGF3YWl0IF9yZXNvbHZlKHJlcXVlc3QpXG4nCiAgICAgICAgJyAgICBpbmxpbmUgPSBraW5kID09ICJwbGF5YmFjayJcbicKICAgICAgICAnXG4nCiAgICAgICAgJyAgICB2aWV3ZXIgPSByZXF1ZXN0LmhlYWRlcnMuZ2V0KCJYLVZpZXdlciIpIG9yIHJlcXVlc3QucmVtb3RlJywKICAgICAgICAnYXN5bmMgZGVmIF9zZXJ2ZShyZXF1ZXN0LCBraW5kKTpcbicKICAgICAgICAnICAgIF8sIGNpZCwgbWlkID0gYXdhaXQgX3Jlc29sdmUocmVxdWVzdClcbicKICAgICAgICAnICAgIGlubGluZSA9IGtpbmQgPT0gInBsYXliYWNrIlxuJwogICAgICAgICdcbicKICAgICAgICAnICAgIHZpZXdlciA9IHJlcXVlc3QuaGVhZGVycy5nZXQoIlgtVmlld2VyIikgb3IgcmVxdWVzdC5yZW1vdGVcbicKICAgICAgICAnICAgICMgQ2hlY2sgZm9yID91c2VyPTEgcGFyYW0gdG8gZm9yY2UgdXNlciBhY2NvdW50IHN0cmVhbSAocGF0Y2g4X3NzZXJ2KVxuJwogICAgICAgICcgICAgZm9yY2VfdXNlciA9IHJlcXVlc3QucXVlcnkuZ2V0KCJ1c2VyIiwgIiIpID09ICIxIicKICAgICksCl0KCnBhdGNoZWRfc2VydmUgPSBGYWxzZQpmb3Igb2xkX3NlcnZlLCBuZXdfc2VydmUgaW4gdmFyaWFudHM6CiAgICBpZiBvbGRfc2VydmUgaW4gc3JjIGFuZCAnZm9yY2VfdXNlcicgbm90IGluIHNyYzoKICAgICAgICBzcmMgPSBzcmMucmVwbGFjZShvbGRfc2VydmUsIG5ld19zZXJ2ZSwgMSkKICAgICAgICBwcmludCgicGF0Y2g4X3NzZXJ2OiBhZGRlZCA/dXNlcj0xIHBhcmFtIGRldGVjdGlvbiB0byBfc2VydmUiKQogICAgICAgIHBhdGNoZWRfc2VydmUgPSBUcnVlCiAgICAgICAgYnJlYWsKCmlmIG5vdCBwYXRjaGVkX3NlcnZlIGFuZCAnZm9yY2VfdXNlcicgbm90IGluIHNyYzoKICAgIHByaW50KCJwYXRjaDhfc3NlcnY6IFdBUk5JTkcgLS0gX3NlcnZlIHN0YXJ0IG5vdCBmb3VuZCIpCgojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQojIDIpIE1vZGlmeSB0aGUgc3RyZWFtIG9wZW5pbmcgdG8gdXNlIGZvcmNlX3VzZXIgd2hlbiA/dXNlcj0xCiMgICAgSGFuZGxlIGJvdGggX3JldHJ5X29wZW5fc3RyZWFtIGFuZCBvcmlnaW5hbCBvcGVuX3N0cmVhbSBjYWxsIHZhcmlhbnRzCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCm9wZW5fdmFyaWFudHMgPSBbCiAgICAoCiAgICAgICAgJyAgICAgICAgc3QgPSBhd2FpdCBfcmV0cnlfb3Blbl9zdHJlYW0oY2lkLCBtaWQsIGtpbmQsIHZpZXdlciknLAogICAgICAgICcgICAgICAgIGlmIGZvcmNlX3VzZXI6XG4nCiAgICAgICAgJyAgICAgICAgICAgICMgRm9yY2UgdXNlciBhY2NvdW50IHN0cmVhbSAocGF0Y2g4X3NzZXJ2KVxuJwogICAgICAgICcgICAgICAgICAgICBMT0dHRVIuaW5mbyhmInN0cmVhbV9zZXJ2ZTogZm9yY2luZyBVc2VyU3RyZWFtIGZvciB7Y2lkfS97bWlkfSAoP3VzZXI9MSkiKVxuJwogICAgICAgICcgICAgICAgICAgICBmcm9tIC4uaGVscGVyLnRlbGVncmFtX2hlbHBlci50Z19zdHJlYW0gaW1wb3J0IFVzZXJTdHJlYW1cbicKICAgICAgICAnICAgICAgICAgICAgc3QgPSBVc2VyU3RyZWFtKGNpZCwgbWlkLCB2aWV3ZXI9dmlld2VyKVxuJwogICAgICAgICcgICAgICAgICAgICBhd2FpdCBzdC5vcGVuKClcbicKICAgICAgICAnICAgICAgICBlbHNlOlxuJwogICAgICAgICcgICAgICAgICAgICBzdCA9IGF3YWl0IF9yZXRyeV9vcGVuX3N0cmVhbShjaWQsIG1pZCwga2luZCwgdmlld2VyKScKICAgICksCiAgICAoCiAgICAgICAgJyAgICAgICAgc3QgPSBhd2FpdCBvcGVuX3N0cmVhbShjaWQsIG1pZCwga2luZCwgdmlld2VyPXZpZXdlciknLAogICAgICAgICcgICAgICAgIGlmIGZvcmNlX3VzZXI6XG4nCiAgICAgICAgJyAgICAgICAgICAgIExPR0dFUi5pbmZvKGYic3RyZWFtX3NlcnZlOiBmb3JjaW5nIFVzZXJTdHJlYW0gZm9yIHtjaWR9L3ttaWR9ICg/dXNlcj0xKSIpXG4nCiAgICAgICAgJyAgICAgICAgICAgIGZyb20gLi5oZWxwZXIudGVsZWdyYW1faGVscGVyLnRnX3N0cmVhbSBpbXBvcnQgVXNlclN0cmVhbVxuJwogICAgICAgICcgICAgICAgICAgICBzdCA9IFVzZXJTdHJlYW0oY2lkLCBtaWQsIHZpZXdlcj12aWV3ZXIpXG4nCiAgICAgICAgJyAgICAgICAgICAgIGF3YWl0IHN0Lm9wZW4oKVxuJwogICAgICAgICcgICAgICAgIGVsc2U6XG4nCiAgICAgICAgJyAgICAgICAgICAgIHN0ID0gYXdhaXQgb3Blbl9zdHJlYW0oY2lkLCBtaWQsIGtpbmQsIHZpZXdlcj12aWV3ZXIpJwogICAgKSwKXQoKcGF0Y2hlZF9vcGVuID0gRmFsc2UKZm9yIG9sZF9vcGVuLCBuZXdfb3BlbiBpbiBvcGVuX3ZhcmlhbnRzOgogICAgaWYgb2xkX29wZW4gaW4gc3JjIGFuZCAnZm9yY2VfdXNlcicgbm90IGluIHNyYy5zcGxpdChvbGRfb3BlbilbMV1bOjIwMF0gaWYgb2xkX29wZW4gaW4gc3JjIGVsc2UgRmFsc2U6CiAgICAgICAgc3JjID0gc3JjLnJlcGxhY2Uob2xkX29wZW4sIG5ld19vcGVuLCAxKQogICAgICAgIHByaW50KCJwYXRjaDhfc3NlcnY6IGFkZGVkIGZvcmNlZCBVc2VyU3RyZWFtIHBhdGgiKQogICAgICAgIHBhdGNoZWRfb3BlbiA9IFRydWUKICAgICAgICBicmVhawogICAgZWxpZiBvbGRfb3BlbiBpbiBzcmMgYW5kICdVc2VyU3RyZWFtJyBub3QgaW4gc3JjOgogICAgICAgIHNyYyA9IHNyYy5yZXBsYWNlKG9sZF9vcGVuLCBuZXdfb3BlbiwgMSkKICAgICAgICBwcmludCgicGF0Y2g4X3NzZXJ2OiBhZGRlZCBmb3JjZWQgVXNlclN0cmVhbSBwYXRoIikKICAgICAgICBwYXRjaGVkX29wZW4gPSBUcnVlCiAgICAgICAgYnJlYWsKCmlmIG5vdCBwYXRjaGVkX29wZW46CiAgICBwcmludCgicGF0Y2g4X3NzZXJ2OiBvcGVuX3N0cmVhbS9fcmV0cnlfb3Blbl9zdHJlYW0gY2FsbCBub3QgZm91bmQiKQoKIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0KIyAzKSBBZGQgWC1TdHJlYW0tUmV0cnkgaGVhZGVyIHRvIHRoZSBTdHJlYW1SZXNwb25zZSBoZWFkZXJzCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCm9sZF9oZWFkZXJzID0gKAogICAgJyAgICBoZWFkZXJzID0ge1xuJwogICAgJyAgICAgICAgIkNvbnRlbnQtVHlwZSI6IHN0Lm1pbWUgb3IgImFwcGxpY2F0aW9uL29jdGV0LXN0cmVhbSIsXG4nCiAgICAnICAgICAgICAiQ29udGVudC1MZW5ndGgiOiBzdHIoZW5kIC0gc3RhcnQgKyAxKSxcbicKICAgICcgICAgICAgICJBY2NlcHQtUmFuZ2VzIjogImJ5dGVzIixcbicKICAgICcgICAgICAgICJDb250ZW50LURpc3Bvc2l0aW9uIjogX2Rpc3Bvc2l0aW9uKHN0Lm5hbWUsIGlubGluZSksXG4nCiAgICAnICAgICAgICAiQ2FjaGUtQ29udHJvbCI6ICJwcml2YXRlLCBtYXgtYWdlPTg2NDAwLCBpbW11dGFibGUiLFxuJwogICAgJyAgICB9JwopCm5ld19oZWFkZXJzID0gKAogICAgJyAgICBoZWFkZXJzID0ge1xuJwogICAgJyAgICAgICAgIkNvbnRlbnQtVHlwZSI6IHN0Lm1pbWUgb3IgImFwcGxpY2F0aW9uL29jdGV0LXN0cmVhbSIsXG4nCiAgICAnICAgICAgICAiQ29udGVudC1MZW5ndGgiOiBzdHIoZW5kIC0gc3RhcnQgKyAxKSxcbicKICAgICcgICAgICAgICJBY2NlcHQtUmFuZ2VzIjogImJ5dGVzIixcbicKICAgICcgICAgICAgICJDb250ZW50LURpc3Bvc2l0aW9uIjogX2Rpc3Bvc2l0aW9uKHN0Lm5hbWUsIGlubGluZSksXG4nCiAgICAnICAgICAgICAiQ2FjaGUtQ29udHJvbCI6ICJwcml2YXRlLCBtYXgtYWdlPTg2NDAwLCBpbW11dGFibGUiLFxuJwogICAgJyAgICAgICAgIyBYLVN0cmVhbS1SZXRyeTogaW5kaWNhdGVzIHN0cmVhbSBvcGVuZWQgc3VjY2Vzc2Z1bGx5IChwYXRjaDhfc3NlcnYpXG4nCiAgICAnICAgICAgICAiWC1TdHJlYW0tUmV0cnkiOiAiMCIsXG4nCiAgICAnICAgIH0nCikKCmlmIG9sZF9oZWFkZXJzIGluIHNyYyBhbmQgJ1gtU3RyZWFtLVJldHJ5JyBub3QgaW4gc3JjOgogICAgc3JjID0gc3JjLnJlcGxhY2Uob2xkX2hlYWRlcnMsIG5ld19oZWFkZXJzLCAxKQogICAgcHJpbnQoInBhdGNoOF9zc2VydjogYWRkZWQgWC1TdHJlYW0tUmV0cnkgaGVhZGVyIikKZWxzZToKICAgIHByaW50KCJwYXRjaDhfc3NlcnY6IFgtU3RyZWFtLVJldHJ5IGFscmVhZHkgcHJlc2VudCBvciBoZWFkZXJzIG5vdCBmb3VuZCIpCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaDhfc3NlcnY6IGRvbmUiKQo="),

    ('patch9_html.py', 'web/templates/stream.html',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaDlfaHRtbC5weSAtIFN0cmVhbSBIVE1MIHNtYXJ0IHN0YWxsIGRldGVjdGlvbiArIFVSTFNlYXJjaFBhcmFtcyArIHJldHJ5IGJ1dHRvbnMKCk1vZGlmaWVzIHN0cmVhbS5odG1sIGluLXBsYWNlLiBJbmplY3RzIGEgPHNjcmlwdD4gYmxvY2sgYmVmb3JlIDwvYm9keT4gdGhhdDoKICAxLiBVc2VzIFVSTFNlYXJjaFBhcmFtcyB0byBwYXJzZSBxdWVyeSBwYXJhbXMgKD91c2VyPTEsID9yZXRyeT1OKQogIDIuIERldGVjdHMgcGxheWJhY2sgc3RhbGxzIChidWZmZXJpbmcgc3RhdGUgPiAxMCBzZWNvbmRzKQogIDMuIFNob3dzIHJldHJ5IGJ1dHRvbnMgd2hlbiBwbGF5YmFjayBmYWlscyBvciBzdGFsbHMKICA0LiBPZmZlcnMgIlJldHJ5IHdpdGggVXNlciBBY2NvdW50IiBidXR0b24gdGhhdCBhZGRzID91c2VyPTEKIiIiCmltcG9ydCBzeXMKCnBhdGggPSBzeXMuYXJndlsxXQp3aXRoIG9wZW4ocGF0aCwgInIiLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgaHRtbCA9IGYucmVhZCgpCgojIFRoZSBKYXZhU2NyaXB0IHRvIGluamVjdApzdGFsbF9zY3JpcHQgPSAiIiIKPCEtLSBwYXRjaDlfaHRtbDogc21hcnQgc3RhbGwgZGV0ZWN0aW9uICsgcmV0cnkgYnV0dG9ucyAtLT4KPHNjcmlwdD4KKGZ1bmN0aW9uKCkgewogICAgInVzZSBzdHJpY3QiOwoKICAgIC8vIFBhcnNlIHF1ZXJ5IHBhcmFtcyB1c2luZyBVUkxTZWFyY2hQYXJhbXMKICAgIGNvbnN0IHBhcmFtcyA9IG5ldyBVUkxTZWFyY2hQYXJhbXMod2luZG93LmxvY2F0aW9uLnNlYXJjaCk7CiAgICBjb25zdCB1c2VyTW9kZSA9IHBhcmFtcy5nZXQoInVzZXIiKSA9PT0gIjEiOwogICAgY29uc3QgcmV0cnlDb3VudCA9IHBhcnNlSW50KHBhcmFtcy5nZXQoInJldHJ5IikgfHwgIjAiLCAxMCk7CgogICAgLy8gRmluZCB0aGUgdmlkZW8vYXVkaW8gZWxlbWVudAogICAgY29uc3QgbWVkaWEgPSBkb2N1bWVudC5xdWVyeVNlbGVjdG9yKCJ2aWRlbywgYXVkaW8iKTsKICAgIGlmICghbWVkaWEpIHJldHVybjsKCiAgICAvLyBTdGFsbCBkZXRlY3Rpb24gc3RhdGUKICAgIGxldCBzdGFsbFN0YXJ0ID0gbnVsbDsKICAgIGxldCBzdGFsbFRpbWVyID0gbnVsbDsKICAgIGxldCBsYXN0VGltZSA9IDA7CiAgICBsZXQgbGFzdFByb2dyZXNzID0gRGF0ZS5ub3coKTsKCiAgICAvLyBDcmVhdGUgcmV0cnkgYnV0dG9uIGNvbnRhaW5lcgogICAgY29uc3QgcmV0cnlDb250YWluZXIgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KCJkaXYiKTsKICAgIHJldHJ5Q29udGFpbmVyLmlkID0gInd6bWx4LXJldHJ5LWNvbnRhaW5lciI7CiAgICByZXRyeUNvbnRhaW5lci5zdHlsZS5jc3NUZXh0ID0gWwogICAgICAgICJwb3NpdGlvbjpmaXhlZCIsCiAgICAgICAgImJvdHRvbTo4MHB4IiwKICAgICAgICAibGVmdDo1MCUiLAogICAgICAgICJ0cmFuc2Zvcm06dHJhbnNsYXRlWCgtNTAlKSIsCiAgICAgICAgInotaW5kZXg6OTk5OSIsCiAgICAgICAgImRpc3BsYXk6bm9uZSIsCiAgICAgICAgImdhcDoxMHB4IiwKICAgICAgICAiZmxleC1kaXJlY3Rpb246cm93IiwKICAgICAgICAiZmxleC13cmFwOndyYXAiLAogICAgICAgICJqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyIgogICAgXS5qb2luKCI7Iik7CiAgICBkb2N1bWVudC5ib2R5LmFwcGVuZENoaWxkKHJldHJ5Q29udGFpbmVyKTsKCiAgICBmdW5jdGlvbiBjcmVhdGVCdXR0b24odGV4dCwgb25DbGljaywgY29sb3IpIHsKICAgICAgICB2YXIgYnRuID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgiYnV0dG9uIik7CiAgICAgICAgYnRuLnRleHRDb250ZW50ID0gdGV4dDsKICAgICAgICBidG4uc3R5bGUuY3NzVGV4dCA9IFsKICAgICAgICAgICAgInBhZGRpbmc6MTBweCAyMHB4IiwKICAgICAgICAgICAgImJvcmRlcjpub25lIiwKICAgICAgICAgICAgImJvcmRlci1yYWRpdXM6OHB4IiwKICAgICAgICAgICAgImZvbnQtc2l6ZToxNHB4IiwKICAgICAgICAgICAgImZvbnQtd2VpZ2h0OjYwMCIsCiAgICAgICAgICAgICJjdXJzb3I6cG9pbnRlciIsCiAgICAgICAgICAgICJiYWNrZ3JvdW5kOiIgKyAoY29sb3IgfHwgIiMzRDg3RkYiKSwKICAgICAgICAgICAgImNvbG9yOiNmZmYiLAogICAgICAgICAgICAiYm94LXNoYWRvdzowIDJweCA4cHggcmdiYSgwLDAsMCwwLjMpIiwKICAgICAgICAgICAgInRyYW5zaXRpb246b3BhY2l0eSAwLjJzIgogICAgICAgIF0uam9pbigiOyIpOwogICAgICAgIGJ0bi5vbm1vdXNlZW50ZXIgPSBmdW5jdGlvbigpIHsgdGhpcy5zdHlsZS5vcGFjaXR5ID0gIjAuODUiOyB9OwogICAgICAgIGJ0bi5vbm1vdXNlbGVhdmUgPSBmdW5jdGlvbigpIHsgdGhpcy5zdHlsZS5vcGFjaXR5ID0gIjEiOyB9OwogICAgICAgIGJ0bi5vbmNsaWNrID0gb25DbGljazsKICAgICAgICByZXR1cm4gYnRuOwogICAgfQoKICAgIGZ1bmN0aW9uIHNob3dSZXRyeUJ1dHRvbnMoKSB7CiAgICAgICAgcmV0cnlDb250YWluZXIuaW5uZXJIVE1MID0gIiI7CiAgICAgICAgcmV0cnlDb250YWluZXIuc3R5bGUuZGlzcGxheSA9ICJmbGV4IjsKCiAgICAgICAgLy8gUmV0cnkgYnV0dG9uIChzYW1lIFVSTCkKICAgICAgICByZXRyeUNvbnRhaW5lci5hcHBlbmRDaGlsZChjcmVhdGVCdXR0b24oIlJldHJ5IiwgZnVuY3Rpb24oKSB7CiAgICAgICAgICAgIG1lZGlhLmxvYWQoKTsKICAgICAgICAgICAgbWVkaWEucGxheSgpLmNhdGNoKGZ1bmN0aW9uKCkge30pOwogICAgICAgICAgICByZXRyeUNvbnRhaW5lci5zdHlsZS5kaXNwbGF5ID0gIm5vbmUiOwogICAgICAgIH0pKTsKCiAgICAgICAgLy8gUmV0cnkgd2l0aCB1c2VyIGFjY291bnQgKD91c2VyPTEpCiAgICAgICAgaWYgKCF1c2VyTW9kZSkgewogICAgICAgICAgICByZXRyeUNvbnRhaW5lci5hcHBlbmRDaGlsZChjcmVhdGVCdXR0b24oIlJldHJ5IHdpdGggVXNlciBBY2NvdW50IiwgZnVuY3Rpb24oKSB7CiAgICAgICAgICAgICAgICB2YXIgdXJsID0gbmV3IFVSTCh3aW5kb3cubG9jYXRpb24uaHJlZik7CiAgICAgICAgICAgICAgICB1cmwuc2VhcmNoUGFyYW1zLnNldCgidXNlciIsICIxIik7CiAgICAgICAgICAgICAgICB1cmwuc2VhcmNoUGFyYW1zLnNldCgicmV0cnkiLCBTdHJpbmcocmV0cnlDb3VudCArIDEpKTsKICAgICAgICAgICAgICAgIHdpbmRvdy5sb2NhdGlvbi5ocmVmID0gdXJsLnRvU3RyaW5nKCk7CiAgICAgICAgICAgIH0sICIjNUI5REZGIikpOwogICAgICAgIH0KCiAgICAgICAgLy8gUmVsb2FkIHBhZ2UgYnV0dG9uCiAgICAgICAgcmV0cnlDb250YWluZXIuYXBwZW5kQ2hpbGQoY3JlYXRlQnV0dG9uKCJSZWxvYWQgUGFnZSIsIGZ1bmN0aW9uKCkgewogICAgICAgICAgICB3aW5kb3cubG9jYXRpb24ucmVsb2FkKCk7CiAgICAgICAgfSwgIiM2NjYiKSk7CiAgICB9CgogICAgZnVuY3Rpb24gaGlkZVJldHJ5QnV0dG9ucygpIHsKICAgICAgICByZXRyeUNvbnRhaW5lci5zdHlsZS5kaXNwbGF5ID0gIm5vbmUiOwogICAgfQoKICAgIC8vIERldGVjdCBzdGFsbGluZzogbWVkaWEgaXMgcGxheWluZyBidXQgbm90IHByb2dyZXNzaW5nCiAgICBmdW5jdGlvbiBjaGVja1N0YWxsKCkgewogICAgICAgIGlmIChtZWRpYS5yZWFkeVN0YXRlIDwgMykgewogICAgICAgICAgICAvLyBCVUZGRVJJTkcKICAgICAgICAgICAgaWYgKHN0YWxsU3RhcnQgPT09IG51bGwpIHsKICAgICAgICAgICAgICAgIHN0YWxsU3RhcnQgPSBEYXRlLm5vdygpOwogICAgICAgICAgICB9CiAgICAgICAgICAgIHZhciBzdGFsbER1cmF0aW9uID0gKERhdGUubm93KCkgLSBzdGFsbFN0YXJ0KSAvIDEwMDA7CiAgICAgICAgICAgIGlmIChzdGFsbER1cmF0aW9uID4gMTApIHsKICAgICAgICAgICAgICAgIGNvbnNvbGUud2FybigiW1daTUwtWF0gUGxheWJhY2sgc3RhbGxlZCBmb3IgIiArIHN0YWxsRHVyYXRpb24udG9GaXhlZCgxKSArICJzIik7CiAgICAgICAgICAgICAgICBzaG93UmV0cnlCdXR0b25zKCk7CiAgICAgICAgICAgIH0KICAgICAgICB9IGVsc2UgewogICAgICAgICAgICAvLyBQTEFZSU5HCiAgICAgICAgICAgIGlmIChzdGFsbFN0YXJ0ICE9PSBudWxsKSB7CiAgICAgICAgICAgICAgICBjb25zb2xlLmxvZygiW1daTUwtWF0gUGxheWJhY2sgcmVzdW1lZCBhZnRlciAiICsgKChEYXRlLm5vdygpIC0gc3RhbGxTdGFydCkgLyAxMDAwKS50b0ZpeGVkKDEpICsgInMgc3RhbGwiKTsKICAgICAgICAgICAgfQogICAgICAgICAgICBzdGFsbFN0YXJ0ID0gbnVsbDsKICAgICAgICB9CiAgICB9CgogICAgLy8gTW9uaXRvciBwbGF5YmFjayBwcm9ncmVzcwogICAgc2V0SW50ZXJ2YWwoY2hlY2tTdGFsbCwgMjAwMCk7CgogICAgLy8gTGlzdGVuIGZvciBlcnJvcnMKICAgIG1lZGlhLmFkZEV2ZW50TGlzdGVuZXIoImVycm9yIiwgZnVuY3Rpb24oZSkgewogICAgICAgIGNvbnNvbGUuZXJyb3IoIltXWk1MLVhdIE1lZGlhIGVycm9yOiIsIG1lZGlhLmVycm9yKTsKICAgICAgICBzaG93UmV0cnlCdXR0b25zKCk7CiAgICB9KTsKCiAgICBtZWRpYS5hZGRFdmVudExpc3RlbmVyKCJzdGFsbGVkIiwgZnVuY3Rpb24oKSB7CiAgICAgICAgY29uc29sZS53YXJuKCJbV1pNTC1YXSBNZWRpYSBzdGFsbGVkIGV2ZW50Iik7CiAgICB9KTsKCiAgICBtZWRpYS5hZGRFdmVudExpc3RlbmVyKCJ3YWl0aW5nIiwgZnVuY3Rpb24oKSB7CiAgICAgICAgY29uc29sZS5sb2coIltXWk1MLVhdIE1lZGlhIHdhaXRpbmcgKGJ1ZmZlcmluZykiKTsKICAgIH0pOwoKICAgIG1lZGlhLmFkZEV2ZW50TGlzdGVuZXIoInBsYXlpbmciLCBmdW5jdGlvbigpIHsKICAgICAgICBjb25zb2xlLmxvZygiW1daTUwtWF0gTWVkaWEgcGxheWluZyIpOwogICAgICAgIGhpZGVSZXRyeUJ1dHRvbnMoKTsKICAgIH0pOwoKICAgIG1lZGlhLmFkZEV2ZW50TGlzdGVuZXIoImNhbnBsYXkiLCBmdW5jdGlvbigpIHsKICAgICAgICBjb25zb2xlLmxvZygiW1daTUwtWF0gTWVkaWEgY2FuIHBsYXkiKTsKICAgIH0pOwoKICAgIC8vIElmIHJldHJ5IGNvdW50IGlzIGhpZ2gsIGF1dG8tc2hvdyByZXRyeSBidXR0b25zCiAgICBpZiAocmV0cnlDb3VudCA+IDApIHsKICAgICAgICBjb25zb2xlLmxvZygiW1daTUwtWF0gUmV0cnkgYXR0ZW1wdCAjIiArIHJldHJ5Q291bnQpOwogICAgfQoKICAgIC8vIExvZyB1c2VyIG1vZGUKICAgIGlmICh1c2VyTW9kZSkgewogICAgICAgIGNvbnNvbGUubG9nKCJbV1pNTC1YXSBVc2VyIGFjY291bnQgc3RyZWFtIG1vZGUgKD91c2VyPTEpIik7CiAgICB9CgogICAgY29uc29sZS5sb2coIltXWk1MLVhdIFNtYXJ0IHN0YWxsIGRldGVjdGlvbiBsb2FkZWQgKHBhdGNoOV9odG1sKSIpOwp9KSgpOwo8L3NjcmlwdD4KPCEtLSAvcGF0Y2g5X2h0bWwgLS0+CiIiIgoKIyBJbmplY3QgYmVmb3JlIDwvYm9keT4gb3IgYXBwZW5kIGF0IGVuZAppZiAiPC9ib2R5PiIgaW4gaHRtbCBhbmQgInBhdGNoOV9odG1sIiBub3QgaW4gaHRtbDoKICAgIGh0bWwgPSBodG1sLnJlcGxhY2UoIjwvYm9keT4iLCBzdGFsbF9zY3JpcHQgKyAiXG48L2JvZHk+IiwgMSkKICAgIHByaW50KCJwYXRjaDlfaHRtbDogaW5qZWN0ZWQgc3RhbGwgZGV0ZWN0aW9uIHNjcmlwdCBiZWZvcmUgPC9ib2R5PiIpCmVsaWYgInBhdGNoOV9odG1sIiBub3QgaW4gaHRtbDoKICAgIGh0bWwgPSBodG1sICsgIlxuIiArIHN0YWxsX3NjcmlwdAogICAgcHJpbnQoInBhdGNoOV9odG1sOiBhcHBlbmRlZCBzdGFsbCBkZXRlY3Rpb24gc2NyaXB0IikKZWxzZToKICAgIHByaW50KCJwYXRjaDlfaHRtbDogYWxyZWFkeSBwYXRjaGVkIikKCndpdGggb3BlbihwYXRoLCAidyIsIGVuY29kaW5nPSJ1dGYtOCIpIGFzIGY6CiAgICBmLndyaXRlKGh0bWwpCnByaW50KCJwYXRjaDlfaHRtbDogZG9uZSIpCg=="),

    ('patch10_ws.py', 'web/wserver.py',
     "IyEvdXNyL2Jpbi9lbnYgcHl0aG9uMwoiIiJwYXRjaDEwX3dzLnB5IC0gd3NlcnZlci5weTogZm9yd2FyZCA/dXNlcj0xIHRocm91Z2ggRmFzdEFQSSBwcm94eQoKTW9kaWZpZXMgd3NlcnZlci5weSBpbi1wbGFjZS4gVGhlIHN0cmVhbV9wcm94eSgpIGZ1bmN0aW9uIGZvcndhcmRzIHJlcXVlc3RzCnRvIHRoZSB1cHN0cmVhbSBzdHJlYW0gc2VydmVyIChTVFJFQU1fQkFTRSkuIFRoaXMgcGF0Y2ggZW5zdXJlcyB0aGUgP3VzZXI9MQpxdWVyeSBwYXJhbSBpcyBmb3J3YXJkZWQgdG8gdGhlIHVwc3RyZWFtLCBlbmFibGluZyB0aGUgdXNlciBhY2NvdW50IHN0cmVhbQpwYXRoIHRocm91Z2ggdGhlIEZhc3RBUEkgcHJveHkuCiIiIgppbXBvcnQgc3lzCgpwYXRoID0gc3lzLmFyZ3ZbMV0Kd2l0aCBvcGVuKHBhdGgsICJyIiwgZW5jb2Rpbmc9InV0Zi04IikgYXMgZjoKICAgIHNyYyA9IGYucmVhZCgpCgojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQojIDEpIE1vZGlmeSBzdHJlYW1fcHJveHkgdG8gZm9yd2FyZCA/dXNlcj0xIHBhcmFtCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCm9sZF9wcm94eV9wYXJhbXMgPSAoCiAgICAnICAgIHRyeTpcbicKICAgICcgICAgICAgIHVwc3RyZWFtID0gYXdhaXQgaHR0cF9zZXNzaW9uLnJlcXVlc3QoXG4nCiAgICAnICAgICAgICAgICAgcmVxdWVzdC5tZXRob2QsXG4nCiAgICAnICAgICAgICAgICAgZiJ7U1RSRUFNX0JBU0V9e3Vwc3RyZWFtX3BhdGh9L3t0b2tlbn0iLFxuJwogICAgJyAgICAgICAgICAgIGhlYWRlcnM9aGVhZGVycyxcbicKICAgICcgICAgICAgICAgICBwYXJhbXM9cGFyYW1zIG9yIE5vbmUsXG4nCiAgICAnICAgICAgICAgICAgYWxsb3dfcmVkaXJlY3RzPUZhbHNlLFxuJwogICAgJyAgICAgICAgKScKKQpuZXdfcHJveHlfcGFyYW1zID0gKAogICAgJyAgICAjIEZvcndhcmQgP3VzZXI9MSBwYXJhbSB0byB1cHN0cmVhbSBzdHJlYW0gc2VydmVyIChwYXRjaDEwX3dzKVxuJwogICAgJyAgICBmb3J3YXJkX3BhcmFtcyA9IGRpY3QocGFyYW1zIG9yIHt9KVxuJwogICAgJyAgICB1c2VyX3BhcmFtID0gcmVxdWVzdC5xdWVyeV9wYXJhbXMuZ2V0KCJ1c2VyIilcbicKICAgICcgICAgaWYgdXNlcl9wYXJhbSBpcyBub3QgTm9uZSBhbmQgInVzZXIiIG5vdCBpbiBmb3J3YXJkX3BhcmFtczpcbicKICAgICcgICAgICAgIGZvcndhcmRfcGFyYW1zWyJ1c2VyIl0gPSB1c2VyX3BhcmFtXG4nCiAgICAnXG4nCiAgICAnICAgIHRyeTpcbicKICAgICcgICAgICAgIHVwc3RyZWFtID0gYXdhaXQgaHR0cF9zZXNzaW9uLnJlcXVlc3QoXG4nCiAgICAnICAgICAgICAgICAgcmVxdWVzdC5tZXRob2QsXG4nCiAgICAnICAgICAgICAgICAgZiJ7U1RSRUFNX0JBU0V9e3Vwc3RyZWFtX3BhdGh9L3t0b2tlbn0iLFxuJwogICAgJyAgICAgICAgICAgIGhlYWRlcnM9aGVhZGVycyxcbicKICAgICcgICAgICAgICAgICBwYXJhbXM9Zm9yd2FyZF9wYXJhbXMgb3IgTm9uZSxcbicKICAgICcgICAgICAgICAgICBhbGxvd19yZWRpcmVjdHM9RmFsc2UsXG4nCiAgICAnICAgICAgICApJwopCgppZiBvbGRfcHJveHlfcGFyYW1zIGluIHNyYyBhbmQgInBhdGNoMTBfd3MiIG5vdCBpbiBzcmM6CiAgICBzcmMgPSBzcmMucmVwbGFjZShvbGRfcHJveHlfcGFyYW1zLCBuZXdfcHJveHlfcGFyYW1zLCAxKQogICAgcHJpbnQoInBhdGNoMTBfd3M6IGFkZGVkID91c2VyPTEgZm9yd2FyZGluZyB0byBzdHJlYW1fcHJveHkiKQplbHNlOgogICAgcHJpbnQoInBhdGNoMTBfd3M6IHRhcmdldCBub3QgZm91bmQgb3IgYWxyZWFkeSBwYXRjaGVkIikKCiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tCiMgMikgQWRkIFgtU3RyZWFtLVJldHJ5IGhlYWRlciBwYXNzdGhyb3VnaCBmcm9tIHVwc3RyZWFtIHRvIGNsaWVudAojIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLQpvbGRfb3V0X2hlYWRlcnMgPSAoCiAgICAnICAgIG91dCA9IHtcbicKICAgICcgICAgICAgIGs6IHYgZm9yIGssIHYgaW4gdXBzdHJlYW0uaGVhZGVycy5pdGVtcygpIGlmIGsubG93ZXIoKSBub3QgaW4gX0hPUFxuJwogICAgJyAgICB9XG4nCiAgICAnICAgIG91dC5zZXRkZWZhdWx0KCJBY2NlcHQtUmFuZ2VzIiwgImJ5dGVzIilcbicKICAgICcgICAgb3V0LnNldGRlZmF1bHQoIkNhY2hlLUNvbnRyb2wiLCAicHJpdmF0ZSwgbWF4LWFnZT04NjQwMCwgaW1tdXRhYmxlIilcbicKICAgICcgICAgb3V0WyJSZWZlcnJlci1Qb2xpY3kiXSA9ICJuby1yZWZlcnJlciJcbicKICAgICcgICAgb3V0WyJYLUNvbnRlbnQtVHlwZS1PcHRpb25zIl0gPSAibm9zbmlmZiInCikKbmV3X291dF9oZWFkZXJzID0gKAogICAgJyAgICBvdXQgPSB7XG4nCiAgICAnICAgICAgICBrOiB2IGZvciBrLCB2IGluIHVwc3RyZWFtLmhlYWRlcnMuaXRlbXMoKSBpZiBrLmxvd2VyKCkgbm90IGluIF9IT1BcbicKICAgICcgICAgfVxuJwogICAgJyAgICBvdXQuc2V0ZGVmYXVsdCgiQWNjZXB0LVJhbmdlcyIsICJieXRlcyIpXG4nCiAgICAnICAgIG91dC5zZXRkZWZhdWx0KCJDYWNoZS1Db250cm9sIiwgInByaXZhdGUsIG1heC1hZ2U9ODY0MDAsIGltbXV0YWJsZSIpXG4nCiAgICAnICAgIG91dFsiUmVmZXJyZXItUG9saWN5Il0gPSAibm8tcmVmZXJyZXIiXG4nCiAgICAnICAgIG91dFsiWC1Db250ZW50LVR5cGUtT3B0aW9ucyJdID0gIm5vc25pZmYiXG4nCiAgICAnICAgICMgRm9yd2FyZCBYLVN0cmVhbS1SZXRyeSBoZWFkZXIgZnJvbSB1cHN0cmVhbSAocGF0Y2gxMF93cylcbicKICAgICcgICAgaWYgIlgtU3RyZWFtLVJldHJ5IiBub3QgaW4gb3V0IGFuZCAieC1zdHJlYW0tcmV0cnkiIG5vdCBpbiB7ay5sb3dlcigpIGZvciBrIGluIG91dH06XG4nCiAgICAnICAgICAgICB1cHN0cmVhbV9yZXRyeSA9IHVwc3RyZWFtLmhlYWRlcnMuZ2V0KCJYLVN0cmVhbS1SZXRyeSIpXG4nCiAgICAnICAgICAgICBpZiB1cHN0cmVhbV9yZXRyeTpcbicKICAgICcgICAgICAgICAgICBvdXRbIlgtU3RyZWFtLVJldHJ5Il0gPSB1cHN0cmVhbV9yZXRyeScKKQoKaWYgb2xkX291dF9oZWFkZXJzIGluIHNyYyBhbmQgIlgtU3RyZWFtLVJldHJ5IiBub3QgaW4gc3JjOgogICAgc3JjID0gc3JjLnJlcGxhY2Uob2xkX291dF9oZWFkZXJzLCBuZXdfb3V0X2hlYWRlcnMsIDEpCiAgICBwcmludCgicGF0Y2gxMF93czogYWRkZWQgWC1TdHJlYW0tUmV0cnkgaGVhZGVyIHBhc3N0aHJvdWdoIikKZWxzZToKICAgIHByaW50KCJwYXRjaDEwX3dzOiBvdXRfaGVhZGVycyB0YXJnZXQgbm90IGZvdW5kIG9yIGFscmVhZHkgcGF0Y2hlZCIpCgp3aXRoIG9wZW4ocGF0aCwgInciLCBlbmNvZGluZz0idXRmLTgiKSBhcyBmOgogICAgZi53cml0ZShzcmMpCnByaW50KCJwYXRjaDEwX3dzOiBkb25lIikK"),

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
    }
    for name, target_rel, _b64 in PATCH_DATA:
        if name in retired:
            log(f"  {name}: superseded by WZML-X-Bot patch kit — skipping")
            continue
        patch_path = os.path.join(PATCH_TMP_DIR, name)
        target_path = os.path.join(WZMLX_DIR, target_rel)
        if not os.path.isfile(target_path):
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
    "bGV0ZWRfY291bnQKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIGZyZWVkCgoKYXN5bmMgZGVm"
    "IGJvb3RfcmVwb3J0KCk6CiAgICAiIiJEZWxheWVkIGJvb3QgbG9nOiB0b3RhbCBEQiBzaXplICsgd2FybmluZyBwYXN0IDgwJSBv"
    "ZiB0aGUgNTEyIE1CIHRpZXIuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSBhc3luY2lvIGltcG9ydCBzbGVlcAoKICAgICAgICBh"
    "d2FpdCBzbGVlcCg0NSkKICAgICAgICBzdCA9IGF3YWl0IGRic3RhdHMoKQogICAgICAgIGlmIHN0LmdldCgiZXJyb3IiKSBhbmQg"
    "bm90IHN0LmdldCgiY29scyIpOgogICAgICAgICAgICByZXR1cm4KICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgZiJX"
    "WkZJWCBEQjoge19uaWNlX3NpemUoc3RbJ3RvdGFsJ10pfSBhY3Jvc3MgIgogICAgICAgICAgICBmIntsZW4oc3RbJ2NvbHMnXSl9"
    "IGNvbGxlY3Rpb24gZ3JvdXBzIChmcmVlIHRpZXIgNTEyIE1CKSIKICAgICAgICApCiAgICAgICAgaWYgc3RbInRvdGFsIl0gPiAw"
    "LjggKiBXWkZJWF9BVExBU19GUkVFOgogICAgICAgICAgICBMT0dHRVIud2FybmluZygKICAgICAgICAgICAgICAgICJXWkZJWCBE"
    "QjogdXNhZ2UgaXMgYWJvdmUgODAlIG9mIHRoZSBBdGxhcyBmcmVlIHRpZXIgKDUxMiBNQikhICIKICAgICAgICAgICAgICAgICJS"
    "dW4gL2Ric3RhdHMgYW5kIC9kYmNsZWFuIHRvIHJlY2xhaW0gc3BhY2UuIgogICAgICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uOgogICAgICAgIHBhc3MKCgphc3luYyBkZWYgZGlhZyh1c2VyX2lkLCBwcm9iZV9nYj1Ob25lKToKICAgICIiIkNvbXBsZXRl"
    "IHF1b3RhIHN0YXRlIGZvciBvbmUgdXNlciwgZm9yIGxpdmUgZGVidWdnaW5nLiIiIgogICAgb3V0ID0ge30KICAgIG91dFsicGFy"
    "dGl0aW9uIl0gPSBfcGFydCgpCiAgICBvdXRbImRheSJdID0gX2RheV9pc3QoKQogICAgb3V0WyJkYl9yZWFkeSJdID0gYXdhaXQg"
    "ZW5zdXJlX3JlYWR5KCkKICAgIG91dFsidXNlcl9kb2MiXSA9IGF3YWl0IGdldF91c2VyX2RvYyh1c2VyX2lkKQogICAgb3V0WyJn"
    "bG9iYWxfY2FwX2diIl0gPSBhd2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKQogICAgb3V0WyJjYXBfYnl0ZXMiXSA9IGF3YWl0IGdl"
    "dF9jYXBfYnl0ZXModXNlcl9pZCkKICAgIG91dFsicXVvdGFfZG9jIl0gPSBOb25lCiAgICB0cnk6CiAgICAgICAgb3V0WyJxdW90"
    "YV9kb2MiXSA9IGF3YWl0IF9kYigpLnd6Zml4X3F1b3RhW19wYXJ0KCldLmZpbmRfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYi"
    "e3VzZXJfaWR9OntfZGF5X2lzdCgpfSJ9CiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBv"
    "dXRbInJlc2VydmVkIl0gPSBhd2FpdCByZXNlcnZlZF90b2RheSh1c2VyX2lkKQogICAgaWYgcHJvYmVfZ2IgaXMgbm90IE5vbmU6"
    "CiAgICAgICAgcHJvYmUgPSBwcm9iZV9nYiAqIEdCCiAgICAgICAgdXNlZCA9IGF3YWl0IGdldF91c2FnZSh1c2VyX2lkKQogICAg"
    "ICAgIGNhcCA9IG91dFsiY2FwX2J5dGVzIl0KICAgICAgICBvdXRbInByb2JlIl0gPSB7CiAgICAgICAgICAgICJzaXplIjogcHJv"
    "YmUsCiAgICAgICAgICAgICJ1c2VkIjogdXNlZCwKICAgICAgICAgICAgInJlc2VydmVkIjogb3V0WyJyZXNlcnZlZCJdLAogICAg"
    "ICAgICAgICAiY2FwIjogY2FwLAogICAgICAgICAgICAid291bGRfYmxvY2siOiAoCiAgICAgICAgICAgICAgICB1c2VkICsgb3V0"
    "WyJyZXNlcnZlZCJdID49IGNhcAogICAgICAgICAgICAgICAgb3IgdXNlZCArIG91dFsicmVzZXJ2ZWQiXSArIHByb2JlID4gY2Fw"
    "ICsgV1pGSVhfR1JBQ0VfTUIgKiBNQgogICAgICAgICAgICApLAogICAgICAgIH0KICAgIHJldHVybiBvdXQKCgojIOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIE93bmVyLWZhY2lu"
    "ZyBzdW1tYXJpZXMKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIAKCgphc3luYyBkZWYgdG9kYXlfcm93cygpOgogICAgIiIiQWxsIG9mIHRvZGF5J3MgcXVvdGEgcm93czogW3sidXNl"
    "cl9pZCIsInVzZWQifSwgLi4uXSBsYXJnZXN0IGZpcnN0LiIiIgogICAgdHJ5OgogICAgICAgIGRheSA9IF9kYXlfaXN0KCkKICAg"
    "ICAgICBjdXJzb3IgPSBfZGIoKS53emZpeF9xdW90YVtfcGFydCgpXS5maW5kKHsiX2lkIjogeyIkcmVnZXgiOiBmIjp7ZGF5fSQi"
    "fX0pCiAgICAgICAgcm93cyA9IFtdCiAgICAgICAgYXN5bmMgZm9yIGQgaW4gY3Vyc29yOgogICAgICAgICAgICB0cnk6CiAgICAg"
    "ICAgICAgICAgICB1aWQgPSBpbnQoc3RyKGRbIl9pZCJdKS5yc3BsaXQoIjoiLCAxKVswXSkKICAgICAgICAgICAgZXhjZXB0IEV4"
    "Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIHJvd3MuYXBwZW5kKHsidXNlcl9pZCI6IHVpZCwg"
    "InVzZWQiOiBpbnQoZC5nZXQoInVzZWQiKSBvciAwKX0pCiAgICAgICAgcm93cy5zb3J0KGtleT1sYW1iZGEgcjogLXJbInVzZWQi"
    "XSkKICAgICAgICByZXR1cm4gcm93cwogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gW10KCgphc3luYyBkZWYg"
    "YWxsX3VzZXJzKCk6CiAgICB0cnk6CiAgICAgICAgY3Vyc29yID0gX2RiKCkud3pmaXhfdXNlcnNbX3BhcnQoKV0uZmluZCgpLnNv"
    "cnQoImxhc3RfdXNlZCIsIC0xKQogICAgICAgIHJldHVybiBbZCBhc3luYyBmb3IgZCBpbiBjdXJzb3JdCiAgICBleGNlcHQgRXhj"
    "ZXB0aW9uOgogICAgICAgIHJldHVybiBbXQo="
)
# bot/helper/wzfix/r3_music.py — music app integrations (Spotify / JioSaavn / Apple Music)

WZFIX_R3_MUSIC_B64 = (
    "IyBXWkZJWCBSb3VuZCAzIOKAlCBtdXNpYyBhcHAgaW50ZWdyYXRpb25zICh2MTUuMzIvMzMpLiBTcG90aWZ5IC8gSmlvU2Fhdm4g"
    "LwojIEFwcGxlIE11c2ljIGxpbmtzIGFyZSByZXNvbHZlZCB0byBhIHl0c2VhcmNoIHF1ZXJ5IGFuZCByb3V0ZWQgdGhyb3VnaCB0"
    "aGUKIyB5dC1kbHAgZW5naW5lLCBzbyB0aGUgcXVvdGEsIHRoZSBjaGVja2luZyBtZXNzYWdlLCB0aGUgYWRtaW4gbG9ncyBhbmQg"
    "dGhlCiMgdXBsb2FkIHR1bmluZyBhbGwgYXBwbHkgdG8gbXVzaWMgZG93bmxvYWRzIHVuY2hhbmdlZC4gVHJhY2sgbGlua3MgcmVz"
    "b2x2ZQojIHRvIHRoZSBleGFjdCBzb25nOyBhcnRpc3QgLyBhbGJ1bSAvIHBsYXlsaXN0IGxpbmtzIGJlY29tZSBhIHNpbmdsZQoj"
    "IHl0c2VhcmNoIHBsYXlsaXN0IHRhc2sgKG9uZSBzdGF0dXMgY2FyZCwgcGxheWxpc3QgcXVvdGEgY2hlY2sgYXBwbGllcykuCiMg"
    "Tm8gQVBJIGtleXMgYXJlIHVzZWQ6IHRoZSBzb3VyY2UgcGFnZXMgYXJlIGZldGNoZWQgd2l0aCBhIGJyb3dzZXIgdXNlcgojIGFn"
    "ZW50LCBTcG90aWZ5J3Mgb0VtYmVkIGVuZHBvaW50IHN1cHBsaWVzIGFydGlzdC9hbGJ1bSBuYW1lcywgYW5kIHRoZQojIFVSTCBz"
    "bHVnIGlzIHRoZSBhbHdheXMtYXZhaWxhYmxlIGZhbGxiYWNrLgoKaW1wb3J0IG9zCmltcG9ydCByZQpmcm9tIGh0bWwgaW1wb3J0"
    "IHVuZXNjYXBlCgp0cnk6CiAgICBmcm9tIC5yMV9jb3JlIGltcG9ydCBfZ2V0X2dsb2JhbF9tdXNpYywgYWRtaW5fbG9nLCBnZXRf"
    "dXNlcl9tdXNpYwpleGNlcHQgRXhjZXB0aW9uOgogICAgYWRtaW5fbG9nID0gTm9uZQogICAgZ2V0X3VzZXJfbXVzaWMgPSBOb25l"
    "CiAgICBfZ2V0X2dsb2JhbF9tdXNpYyA9IE5vbmUKCnRyeToKICAgIGZyb20gbG9nZ2luZyBpbXBvcnQgZ2V0TG9nZ2VyCgogICAg"
    "X0xPRyA9IGdldExvZ2dlcihfX25hbWVfXykKZXhjZXB0IEV4Y2VwdGlvbjoKICAgIF9MT0cgPSBOb25lCgoKZGVmIF9sb2cobXNn"
    "KToKICAgIHRyeToKICAgICAgICBpZiBfTE9HIGlzIG5vdCBOb25lOgogICAgICAgICAgICBfTE9HLmluZm8obXNnKQogICAgZXhj"
    "ZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKX1VBID0gewogICAgIlVzZXItQWdlbnQiOiAoCiAgICAgICAgIk1vemlsbGEv"
    "NS4wIChXaW5kb3dzIE5UIDEwLjA7IFdpbjY0OyB4NjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAiCiAgICAgICAgIihLSFRNTCwgbGlr"
    "ZSBHZWNrbykgQ2hyb21lLzEyNC4wLjAuMCBTYWZhcmkvNTM3LjM2IgogICAgKSwKICAgICJBY2NlcHQtTGFuZ3VhZ2UiOiAiZW4t"
    "VVMsZW47cT0wLjkiLAp9CgpfVVJMX1JFID0gcmUuY29tcGlsZShyImh0dHBzPzovL1xTKyIsIHJlLkkpCgpfTVVTSUNfSE9TVFMg"
    "PSAoCiAgICAib3Blbi5zcG90aWZ5LmNvbSIsCiAgICAic3BvdGlmeS5saW5rIiwKICAgICJqaW9zYWF2bi5jb20iLAogICAgInNh"
    "YXZuLmNvbSIsCiAgICAibXVzaWMuYXBwbGUuY29tIiwKKQoKCmNsYXNzIE11c2ljVW5zdXBwb3J0ZWQoRXhjZXB0aW9uKToKICAg"
    "ICIiIkEgbXVzaWMgbGluayB0aGUgYm90IHJlY29nbmlzZXMgYnV0IGNhbm5vdCBoYW5kbGUuIiIiCgoKZGVmIF9tdXNpY19tYXgo"
    "KToKICAgICIiIkhhcmQgZGVmYXVsdDogMTAgc29uZ3MgKFdaRklYX01VU0lDX01BWCBlbnYsIGNsYW1wZWQgMeKAkzUwMCkuIiIi"
    "CiAgICB0cnk6CiAgICAgICAgcmV0dXJuIG1heCgxLCBtaW4oNTAwLCBpbnQob3MuZW52aXJvbi5nZXQoIldaRklYX01VU0lDX01B"
    "WCIsICIxMCIpKSkpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAxMAoKCmFzeW5jIGRlZiBfbXVzaWNfbWF4"
    "X2Zvcih1c2VyX2lkPTApOgogICAgIiIiRWZmZWN0aXZlIGxpbWl0IGZvciB0aGlzIHVzZXI6IHRoZWlyIG92ZXJyaWRlIOKGkiBn"
    "bG9iYWwgZGVmYXVsdAogICAgKERCLCBzZXR0YWJsZSBvbiB0aGUgYWRtaW4gd2ViIHBhZ2UpIOKGkiBlbnYg4oaSIDEwLiBDbGFt"
    "cGVkIDHigJM1MDAuIiIiCiAgICB0cnk6CiAgICAgICAgaWYgdXNlcl9pZCBhbmQgZ2V0X3VzZXJfbXVzaWMgaXMgbm90IE5vbmU6"
    "CiAgICAgICAgICAgIHYgPSBhd2FpdCBnZXRfdXNlcl9tdXNpYyh1c2VyX2lkKQogICAgICAgICAgICBpZiB2OgogICAgICAgICAg"
    "ICAgICAgcmV0dXJuIHYKICAgICAgICBpZiBfZ2V0X2dsb2JhbF9tdXNpYyBpcyBub3QgTm9uZToKICAgICAgICAgICAgZyA9IGF3"
    "YWl0IF9nZXRfZ2xvYmFsX211c2ljKCkKICAgICAgICAgICAgaWYgZzoKICAgICAgICAgICAgICAgIHJldHVybiBnCiAgICBleGNl"
    "cHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiBfbXVzaWNfbWF4KCkKCgpkZWYgX2hvc3Rfb2YodXJsKToKICAg"
    "IHRyeToKICAgICAgICBtID0gcmUubWF0Y2gociJodHRwcz86Ly8oW14vXSspLyIsIHVybCArICIvIikKICAgICAgICByZXR1cm4g"
    "KG0uZ3JvdXAoMSkgaWYgbSBlbHNlICIiKS5sb3dlcigpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAiIgoK"
    "CmRlZiBpc19tdXNpY191cmwodXJsKToKICAgIGggPSBfaG9zdF9vZih1cmwpCiAgICBpZiBub3QgaDoKICAgICAgICByZXR1cm4g"
    "RmFsc2UKICAgIGZvciBtaCBpbiBfTVVTSUNfSE9TVFM6CiAgICAgICAgaWYgaCA9PSBtaCBvciBoLmVuZHN3aXRoKCIuIiArIG1o"
    "KToKICAgICAgICAgICAgcmV0dXJuIFRydWUKICAgIHJldHVybiBGYWxzZQoKCmRlZiBfc2x1Z19xdWVyeSh1cmwpOgogICAgIiIi"
    "QmVzdC1lZmZvcnQgc2VhcmNoIHRlcm1zIHN0cmFpZ2h0IGZyb20gdGhlIFVSTCBzbHVnIOKAlCB0aGUKICAgIGFsd2F5cy1hdmFp"
    "bGFibGUgZmFsbGJhY2sgd2hlbiB0aGUgcGFnZSBjYW5ub3QgYmUgZmV0Y2hlZC4iIiIKICAgIHRyeToKICAgICAgICBwYXRoID0g"
    "dXJsLnNwbGl0KCI/IilbMF0uc3BsaXQoIiMiKVswXQogICAgICAgIHBhcnRzID0gW3AgZm9yIHAgaW4gcGF0aC5zcGxpdCgiLyIp"
    "IGlmIHBdCiAgICAgICAgZm9yIHNlZyBpbiByZXZlcnNlZChwYXJ0cyk6CiAgICAgICAgICAgIHMgPSBzZWcuc3RyaXAoKQogICAg"
    "ICAgICAgICBpZiBub3QgcyBvciBub3QgcmUuc2VhcmNoKHIiW2EtekEtWl0iLCBzKToKICAgICAgICAgICAgICAgIGNvbnRpbnVl"
    "CiAgICAgICAgICAgICMgc2tpcCBvcGFxdWUgaWRzOiBzcG90aWZ5IHRyYWNrIGlkcywgamlvc2Fhdm4vYXBwbGUgbnVtZXJpYyBp"
    "ZHMKICAgICAgICAgICAgaWYgcmUuZnVsbG1hdGNoKHIiWzAtOWEtekEtWl17MTAsfSIsIHMpOgogICAgICAgICAgICAgICAgY29u"
    "dGludWUKICAgICAgICAgICAgcmV0dXJuIHMucmVwbGFjZSgiLSIsICIgIikucmVwbGFjZSgiXyIsICIgIikuc3RyaXAoKQogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gTm9uZQoKCmFzeW5jIGRlZiBfZmV0Y2godXJsLCB0aW1l"
    "b3V0PTE1LjApOgogICAgIiIiR0VUIHRoZSBwYWdlIGZvbGxvd2luZyByZWRpcmVjdHM7IHJldHVybnMgKGZpbmFsX3VybCwgaHRt"
    "bCkuCiAgICBSZXRyaWVzIG9uY2Ug4oCUIHRyYW5zaWVudCBuZXR3b3JrIGJsaXBzIGFyZSBjb21tb24gb24gS2FnZ2xlLiIiIgog"
    "ICAgZnJvbSBhaW9odHRwIGltcG9ydCBDbGllbnRTZXNzaW9uLCBDbGllbnRUaW1lb3V0CgogICAgX2VyciA9IE5vbmUKICAgIGZv"
    "ciBfYXR0ZW1wdCBpbiByYW5nZSgyKToKICAgICAgICB0cnk6CiAgICAgICAgICAgIGFzeW5jIHdpdGggQ2xpZW50U2Vzc2lvbigK"
    "ICAgICAgICAgICAgICAgIHRpbWVvdXQ9Q2xpZW50VGltZW91dCh0b3RhbD10aW1lb3V0KSwKICAgICAgICAgICAgICAgIGhlYWRl"
    "cnM9X1VBLAogICAgICAgICAgICAgICAgdHJ1c3RfZW52PVRydWUsCiAgICAgICAgICAgICkgYXMgX3M6CiAgICAgICAgICAgICAg"
    "ICBhc3luYyB3aXRoIF9zLmdldCh1cmwpIGFzIF9yOgogICAgICAgICAgICAgICAgICAgIHJldHVybiBzdHIoX3IudXJsKSwgYXdh"
    "aXQgX3IudGV4dCgpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICBfZXJyID0gZQogICAgICAgICAg"
    "ICBpZiBfYXR0ZW1wdDoKICAgICAgICAgICAgICAgIHJhaXNlCiAgICByYWlzZSBfZXJyCgoKYXN5bmMgZGVmIF9vZW1iZWRfdGl0"
    "bGUodXJsKToKICAgICIiIlNwb3RpZnkgb0VtYmVkIOKAlCB0aGUgbmFtZSBvZiBhIHRyYWNrL2FydGlzdC9hbGJ1bS9wbGF5bGlz"
    "dCwgbm8ga2V5LiIiIgogICAgdHJ5OgogICAgICAgIGltcG9ydCBqc29uIGFzIF9qc29uCgogICAgICAgIGZpbmFsLCBodG1sID0g"
    "YXdhaXQgX2ZldGNoKGYiaHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL29lbWJlZD91cmw9e3VybH0iKQogICAgICAgIGogPSBfanNv"
    "bi5sb2FkcyhodG1sKQogICAgICAgIGlmIGouZ2V0KCJ0aXRsZSIpOgogICAgICAgICAgICByZXR1cm4gX2NsZWFuKGpbInRpdGxl"
    "Il0pCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiBOb25lCgoKZGVmIF9jbGVhbih0ZXh0KToK"
    "ICAgIHQgPSB1bmVzY2FwZSh0ZXh0IG9yICIiKS5zdHJpcCgpCiAgICB0ID0gcmUuc3ViKHIiXHMrIiwgIiAiLCB0KQogICAgcmV0"
    "dXJuIHQuc3RyaXAoIiAt4oCT4oCUfCIpCgoKYXN5bmMgZGVmIF9yZXNvbHZlX3Nwb3RpZnkodXJsLCBtbWF4PTEwKToKICAgIGZp"
    "bmFsLCBodG1sID0gdXJsLCAiIgogICAgdHJ5OgogICAgICAgIGZpbmFsLCBodG1sID0gYXdhaXQgX2ZldGNoKHVybCkKICAgIGV4"
    "Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgX2ZwID0gZmluYWwuc3BsaXQoIj8iKVswXQogICAgX3VwID0gdXJsLnNw"
    "bGl0KCI/IilbMF0KICAgIGlmICIvdHJhY2svIiBpbiBfZnAgb3IgIi90cmFjay8iIGluIF91cDoKICAgICAgICAjIGV4YWN0IHNp"
    "bmdsZSBzb25nCiAgICAgICAgbSA9IHJlLnNlYXJjaChyIjx0aXRsZT4oW148XSspPC90aXRsZT4iLCBodG1sKQogICAgICAgIGlm"
    "IG06CiAgICAgICAgICAgIHQgPSBfY2xlYW4obS5ncm91cCgxKSkKICAgICAgICAgICAgdCA9IHJlLnN1YihyIlxzKlx8XHMqU3Bv"
    "dGlmeVxzKiQiLCAiIiwgdCkuc3RyaXAoKQogICAgICAgICAgICBtbSA9IHJlLm1hdGNoKHIiKC4rPylccypbLeKAk+KAlF1ccypz"
    "b25nIGFuZCBseXJpY3MgYnlccyooLispJCIsIHQpCiAgICAgICAgICAgIGlmIG1tOgogICAgICAgICAgICAgICAgcmV0dXJuICgK"
    "ICAgICAgICAgICAgICAgICAgICBmInl0c2VhcmNoOntfY2xlYW4obW0uZ3JvdXAoMikpfSAtICIKICAgICAgICAgICAgICAgICAg"
    "ICBmIntfY2xlYW4obW0uZ3JvdXAoMSkpfSBhdWRpbyIKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgaWYgdDoKICAgICAg"
    "ICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoOnt0fSBhdWRpbyIKICAgICAgICBuYW1lID0gYXdhaXQgX29lbWJlZF90aXRsZShm"
    "aW5hbCBvciB1cmwpCiAgICAgICAgaWYgbmFtZToKICAgICAgICAgICAgcmV0dXJuIGYieXRzZWFyY2g6e25hbWV9IGF1ZGlvIgog"
    "ICAgICAgIHJhaXNlIE11c2ljVW5zdXBwb3J0ZWQoCiAgICAgICAgICAgICJDb3VsZG4ndCByZWFkIHRoaXMgU3BvdGlmeSB0cmFj"
    "ayAobmV0d29yayBvciBibG9jaykg4oCUICIKICAgICAgICAgICAgInRyeSBhZ2FpbiBpbiBhIG1pbnV0ZSwgb3Igc2VuZCBhIFlv"
    "dVR1YmUgbGluayIKICAgICAgICApCiAgICBpZiBhbnkocCBpbiAoX2ZwICsgX3VwKSBmb3IgcCBpbiAoIi9hcnRpc3QvIiwgIi9h"
    "bGJ1bS8iLCAiL3BsYXlsaXN0LyIpKToKICAgICAgICBuYW1lID0gYXdhaXQgX29lbWJlZF90aXRsZShmaW5hbCBvciB1cmwpCiAg"
    "ICAgICAgaWYgbm90IG5hbWU6CiAgICAgICAgICAgIG0gPSByZS5zZWFyY2gociI8dGl0bGU+KFtePF0rKTwvdGl0bGU+IiwgaHRt"
    "bCkKICAgICAgICAgICAgaWYgbToKICAgICAgICAgICAgICAgIHQgPSBfY2xlYW4obS5ncm91cCgxKSkKICAgICAgICAgICAgICAg"
    "IHQgPSByZS5zdWIociJccypbLeKAk3xdXHMqU3BvdGlmeVxzKiQiLCAiIiwgdCkuc3RyaXAoKQogICAgICAgICAgICAgICAgdCA9"
    "IHJlLnNwbGl0KHIiXHMqWy3igJNdXHMqU29uZ3MiLCB0KVswXS5zdHJpcCgpCiAgICAgICAgICAgICAgICBuYW1lID0gdCBvciBO"
    "b25lCiAgICAgICAgaWYgbmFtZToKICAgICAgICAgICAgaWYgIi9hcnRpc3QvIiBpbiAoX2ZwICsgX3VwKToKICAgICAgICAgICAg"
    "ICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1lfSBzb25ncyBhdWRpbyIKICAgICAgICAgICAgaWYgIi9hbGJ1bS8iIGlu"
    "IChfZnAgKyBfdXApOgogICAgICAgICAgICAgICAgcmV0dXJuIGYieXRzZWFyY2h7bW1heH06e25hbWV9IGZ1bGwgYWxidW0gYXVk"
    "aW8iCiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1lfSBwbGF5bGlzdCBhdWRpbyIKICAgICAgICByYWlz"
    "ZSBNdXNpY1Vuc3VwcG9ydGVkKAogICAgICAgICAgICAiQ291bGRuJ3QgcmVhZCB0aGlzIFNwb3RpZnkgcGFnZSDigJQgdHJ5IGFn"
    "YWluIGluIGEgbWludXRlLCAiCiAgICAgICAgICAgICJvciBzZW5kIGEgc2luZ2xlIHRyYWNrIGxpbmsiCiAgICAgICAgKQogICAg"
    "cmFpc2UgTXVzaWNVbnN1cHBvcnRlZCgKICAgICAgICAiVGhpcyBTcG90aWZ5IGxpbmsgaXNuJ3QgYSB0cmFjaywgYXJ0aXN0LCBh"
    "bGJ1bSBvciBwbGF5bGlzdCIKICAgICkKCgphc3luYyBkZWYgX3Jlc29sdmVfamlvc2Fhdm4odXJsLCBtbWF4PTEwKToKICAgIGZp"
    "bmFsLCBodG1sID0gdXJsLCAiIgogICAgdHJ5OgogICAgICAgIGZpbmFsLCBodG1sID0gYXdhaXQgX2ZldGNoKHVybCkKICAgIGV4"
    "Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgX2ZwID0gZmluYWwuc3BsaXQoIj8iKVswXQogICAgX3VwID0gdXJsLnNw"
    "bGl0KCI/IilbMF0KICAgIGlmICIvc29uZy8iIGluIF9mcCBvciAiL3NvbmcvIiBpbiBfdXA6CiAgICAgICAgbSA9IHJlLnNlYXJj"
    "aChyJ3Byb3BlcnR5PSJvZzp0aXRsZSJccytjb250ZW50PSIoW14iXSspIicsIGh0bWwpCiAgICAgICAgaWYgbSBhbmQgX2NsZWFu"
    "KG0uZ3JvdXAoMSkpOgogICAgICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaDp7X2NsZWFuKG0uZ3JvdXAoMSkpfSBhdWRpbyIKICAg"
    "ICAgICBtID0gcmUuc2VhcmNoKHIiPHRpdGxlPihbXjxdKyk8L3RpdGxlPiIsIGh0bWwpCiAgICAgICAgaWYgbToKICAgICAgICAg"
    "ICAgdCA9IF9jbGVhbihtLmdyb3VwKDEpKQogICAgICAgICAgICB0ID0gcmUuc3BsaXQociJccypbLeKAk+KAlF1ccypTb25nIERv"
    "d25sb2FkXGIiLCB0KVswXQogICAgICAgICAgICB0ID0gdC5zcGxpdCgiQCBKaW9TYWF2biIpWzBdCiAgICAgICAgICAgIGlmIHQ6"
    "CiAgICAgICAgICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaDp7dH0gYXVkaW8iCiAgICAgICAgdGl0bGUgPSBhcnRpc3QgPSBOb25l"
    "CiAgICAgICAgbSA9IHJlLnNlYXJjaChyJyJzb25nX3RpdGxlIlxzKjpccyoiKFteIl0rKSInLCBodG1sKSBvciByZS5zZWFyY2go"
    "CiAgICAgICAgICAgIHInInRpdGxlIlxzKjpccyoiKFteIl0rKSInLCBodG1sCiAgICAgICAgKQogICAgICAgIGlmIG06CiAgICAg"
    "ICAgICAgIHRpdGxlID0gX2NsZWFuKG0uZ3JvdXAoMSkpCiAgICAgICAgbSA9ICgKICAgICAgICAgICAgcmUuc2VhcmNoKHInInBy"
    "aW1hcnlfYXJ0aXN0cyJccyo6XHMqIihbXiJdKikiJywgaHRtbCkKICAgICAgICAgICAgb3IgcmUuc2VhcmNoKHInInNpbmdlcnMi"
    "XHMqOlxzKiIoW14iXSopIicsIGh0bWwpCiAgICAgICAgICAgIG9yIHJlLnNlYXJjaChyJyJhcnRpc3QiXHMqOlxzKiIoW14iXSop"
    "IicsIGh0bWwpCiAgICAgICAgKQogICAgICAgIGlmIG06CiAgICAgICAgICAgIGFydGlzdCA9IF9jbGVhbihtLmdyb3VwKDEpKQog"
    "ICAgICAgIGlmIHRpdGxlIGFuZCBhcnRpc3Q6CiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoOnthcnRpc3R9IC0ge3RpdGxl"
    "fSBhdWRpbyIKICAgICAgICBpZiB0aXRsZToKICAgICAgICAgICAgcmV0dXJuIGYieXRzZWFyY2g6e3RpdGxlfSBhdWRpbyIKICAg"
    "ICAgICBuYW1lID0gX3NsdWdfcXVlcnkoZmluYWwpCiAgICAgICAgaWYgbmFtZToKICAgICAgICAgICAgcmV0dXJuIGYieXRzZWFy"
    "Y2g6e25hbWV9IGF1ZGlvIgogICAgICAgIHJldHVybiBOb25lCiAgICAjIGFydGlzdCAvIGFsYnVtIC8gZmVhdHVyZWQgcGFnZXMK"
    "ICAgIG5hbWUgPSBOb25lCiAgICBtID0gcmUuc2VhcmNoKHIncHJvcGVydHk9Im9nOnRpdGxlIlxzK2NvbnRlbnQ9IihbXiJdKyki"
    "JywgaHRtbCkKICAgIGlmIG06CiAgICAgICAgbmFtZSA9IF9jbGVhbihtLmdyb3VwKDEpKQogICAgaWYgbm90IG5hbWU6CiAgICAg"
    "ICAgbSA9IHJlLnNlYXJjaChyIjx0aXRsZT4oW148XSspPC90aXRsZT4iLCBodG1sKQogICAgICAgIGlmIG06CiAgICAgICAgICAg"
    "IHQgPSBfY2xlYW4obS5ncm91cCgxKSkucmVwbGFjZSgiQCBKaW9TYWF2biIsICIiKQogICAgICAgICAgICB0ID0gcmUuc3BsaXQo"
    "ciJccypbLeKAk11ccyooPzpTb25nc3xBbGJ1bXN8QWxidW0pIiwgdClbMF0uc3RyaXAoKQogICAgICAgICAgICBuYW1lID0gdCBv"
    "ciBOb25lCiAgICBpZiBub3QgbmFtZToKICAgICAgICBuYW1lID0gX3NsdWdfcXVlcnkoZmluYWwpCiAgICBpZiBuYW1lOgogICAg"
    "ICAgIGlmICIvYXJ0aXN0LyIgaW4gKF9mcCArIF91cCk6CiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1l"
    "fSBzb25ncyBhdWRpbyIKICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaHttbWF4fTp7bmFtZX0gYXVkaW8iCiAgICByYWlzZSBNdXNp"
    "Y1Vuc3VwcG9ydGVkKCJDb3VsZG4ndCByZWFkIHRoaXMgSmlvU2Fhdm4gbGluayIpCgoKYXN5bmMgZGVmIF9yZXNvbHZlX2FwcGxl"
    "KHVybCwgbW1heD0xMCk6CiAgICBmaW5hbCwgaHRtbCA9IHVybCwgIiIKICAgIHRyeToKICAgICAgICBmaW5hbCwgaHRtbCA9IGF3"
    "YWl0IF9mZXRjaCh1cmwpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIF9mcCA9IGZpbmFsLnNwbGl0KCI/"
    "IilbMF0KICAgIF91cCA9IHVybC5zcGxpdCgiPyIpWzBdCiAgICBpZiAiL3NvbmcvIiBpbiBfZnAgb3IgKCIvYWxidW0vIiBpbiBf"
    "ZnAgYW5kICI/aT0iIGluIGZpbmFsKToKICAgICAgICBtID0gcmUuc2VhcmNoKHIncHJvcGVydHk9Im9nOnRpdGxlIlxzK2NvbnRl"
    "bnQ9IihbXiJdKykiJywgaHRtbCkKICAgICAgICBpZiBtIGFuZCBfY2xlYW4obS5ncm91cCgxKSk6CiAgICAgICAgICAgIHJldHVy"
    "biBmInl0c2VhcmNoOntfY2xlYW4obS5ncm91cCgxKSl9IGF1ZGlvIgogICAgICAgIG0gPSByZS5zZWFyY2gociI8dGl0bGU+KFte"
    "PF0rKTwvdGl0bGU+IiwgaHRtbCkKICAgICAgICBpZiBtOgogICAgICAgICAgICB0ID0gX2NsZWFuKG0uZ3JvdXAoMSkpCiAgICAg"
    "ICAgICAgIHQgPSB0LnJlcGxhY2UoIm9uIEFwcGxlIE11c2ljIiwgIiIpLnN0cmlwKCkKICAgICAgICAgICAgdCA9IHJlLnNwbGl0"
    "KHIiXHMqWy3igJPigJRdXHMqKD86U2luZ2xlfFNvbmd8RVApXGIiLCB0KVswXS5zdHJpcCgpCiAgICAgICAgICAgIGlmIHQ6CiAg"
    "ICAgICAgICAgICAgICByZXR1cm4gZiJ5dHNlYXJjaDp7dH0gYXVkaW8iCiAgICAgICAgbmFtZSA9IF9zbHVnX3F1ZXJ5KGZpbmFs"
    "KQogICAgICAgIGlmIG5hbWU6CiAgICAgICAgICAgIHJldHVybiBmInl0c2VhcmNoOntuYW1lfSBhdWRpbyIKICAgICAgICByZXR1"
    "cm4gTm9uZQogICAgIyBhcnRpc3QgLyBhbGJ1bSBwYWdlcwogICAgbmFtZSA9IE5vbmUKICAgIG0gPSByZS5zZWFyY2gocidwcm9w"
    "ZXJ0eT0ib2c6dGl0bGUiXHMrY29udGVudD0iKFteIl0rKSInLCBodG1sKQogICAgaWYgbToKICAgICAgICBuYW1lID0gX2NsZWFu"
    "KG0uZ3JvdXAoMSkpLnJlcGxhY2UoIm9uIEFwcGxlIE11c2ljIiwgIiIpLnN0cmlwKCkKICAgIGlmIG5vdCBuYW1lOgogICAgICAg"
    "IG0gPSByZS5zZWFyY2gociI8dGl0bGU+KFtePF0rKTwvdGl0bGU+IiwgaHRtbCkKICAgICAgICBpZiBtOgogICAgICAgICAgICB0"
    "ID0gX2NsZWFuKG0uZ3JvdXAoMSkpLnJlcGxhY2UoIm9uIEFwcGxlIE11c2ljIiwgIiIpCiAgICAgICAgICAgIHQgPSByZS5zcGxp"
    "dChyIlxzKlst4oCTXVxzKig/OlNvbmdzfEFsYnVtfEFydGlzdCkiLCB0KVswXS5zdHJpcCgpCiAgICAgICAgICAgIG5hbWUgPSB0"
    "IG9yIE5vbmUKICAgIGlmIG5vdCBuYW1lOgogICAgICAgIG5hbWUgPSBfc2x1Z19xdWVyeShmaW5hbCkKICAgIGlmIG5hbWU6CiAg"
    "ICAgICAgaWYgIi9hcnRpc3QvIiBpbiAoX2ZwICsgX3VwKToKICAgICAgICAgICAgcmV0dXJuIGYieXRzZWFyY2h7bW1heH06e25h"
    "bWV9IHNvbmdzIGF1ZGlvIgogICAgICAgIHJldHVybiBmInl0c2VhcmNoe21tYXh9OntuYW1lfSBhdWRpbyIKICAgIHJhaXNlIE11"
    "c2ljVW5zdXBwb3J0ZWQoIkNvdWxkbid0IHJlYWQgdGhpcyBBcHBsZSBNdXNpYyBsaW5rIikKCgphc3luYyBkZWYgcmVzb2x2ZV9t"
    "dXNpYyh1cmwsIHVzZXJfaWQ9MCk6CiAgICAiIiJSZXR1cm5zICh5dHNlYXJjaF9zcGVjLCBzb3VyY2VfbGFiZWwpLiBUaGUgc3Bl"
    "YyBpcyBhIHJlYWR5IHl0c2VhcmNoCiAgICB0ZXJtIOKAlCBvbmUgZXhhY3Qgc29uZyBmb3IgdHJhY2sgbGlua3MsIGEgY2FwcGVk"
    "IHBsYXlsaXN0IHNlYXJjaCBmb3IKICAgIGFydGlzdCAvIGFsYnVtIC8gcGxheWxpc3QgbGlua3MuIFJhaXNlcyBNdXNpY1Vuc3Vw"
    "cG9ydGVkIGZvciBsaW5rcwogICAgdGhhdCBjYW4gbmV2ZXIgd29yazsgcmV0dXJucyBOb25lIHdoZW4gbm90aGluZyBjb3VsZCBi"
    "ZSByZWFkLiIiIgogICAgaCA9IF9ob3N0X29mKHVybCkKICAgIGlmICJzcG90aWZ5LiIgaW4gaDoKICAgICAgICByZXR1cm4gYXdh"
    "aXQgX3Jlc29sdmVfc3BvdGlmeSh1cmwsIGF3YWl0IF9tdXNpY19tYXhfZm9yKHVzZXJfaWQpKSwgIlNwb3RpZnkiCiAgICBpZiAi"
    "c2Fhdm4uIiBpbiBoOgogICAgICAgIHJldHVybiBhd2FpdCBfcmVzb2x2ZV9qaW9zYWF2bih1cmwsIGF3YWl0IF9tdXNpY19tYXhf"
    "Zm9yKHVzZXJfaWQpKSwgIkppb1NhYXZuIgogICAgaWYgImFwcGxlLmNvbSIgaW4gaDoKICAgICAgICByZXR1cm4gYXdhaXQgX3Jl"
    "c29sdmVfYXBwbGUodXJsLCBhd2FpdCBfbXVzaWNfbWF4X2Zvcih1c2VyX2lkKSksICJBcHBsZSBNdXNpYyIKICAgIHJldHVybiBO"
    "b25lLCBOb25lCgoKZGVmIF91c2VyX29mKG1lc3NhZ2UpOgogICAgdHJ5OgogICAgICAgIHJldHVybiBnZXRhdHRyKGdldGF0dHIo"
    "bWVzc2FnZSwgImZyb21fdXNlciIsIE5vbmUpLCAiaWQiLCAwKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4g"
    "MAoKCmFzeW5jIGRlZiBfcmVmdXNlKG1lc3NhZ2UsIHJlYXNvbik6CiAgICAiIiJPbmUgY2xlYW4gcmVmdXNhbCBtZXNzYWdlIGZv"
    "ciB1bnN1cHBvcnRlZCBtdXNpYyBsaW5rcy4iIiIKICAgIHRyeToKICAgICAgICBpZiBhZG1pbl9sb2cgaXMgbm90IE5vbmU6CiAg"
    "ICAgICAgICAgIGZyb20gaHRtbCBpbXBvcnQgZXNjYXBlIGFzIF9lc2MKCiAgICAgICAgICAgIGF3YWl0IGFkbWluX2xvZygKICAg"
    "ICAgICAgICAgICAgICLwn461IDxiPk11c2ljIGxpbmsgcmVqZWN0ZWQ8L2I+IiwKICAgICAgICAgICAgICAgIGYi4pSPIDxiPlVz"
    "ZXI8L2I+IOKGkiA8Y29kZT57X3VzZXJfb2YobWVzc2FnZSl9PC9jb2RlPlxuIgogICAgICAgICAgICAgICAgZiLilKAgPGI+UmVh"
    "c29uPC9iPiDihpIge19lc2MocmVhc29uKX1cbiIKICAgICAgICAgICAgICAgIGYi4pSWIDxiPk1lc3NhZ2U8L2I+IOKGkiAiCiAg"
    "ICAgICAgICAgICAgICBmIntfZXNjKChtZXNzYWdlLnRleHQgb3IgbWVzc2FnZS5jYXB0aW9uIG9yICcnKVs6MTAwMF0pIG9yICco"
    "bm8gdGV4dCknfSIsCiAgICAgICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgdHJ5OgogICAg"
    "ICAgIGZyb20gLi4uaGVscGVyLnRlbGVncmFtX2hlbHBlci5tZXNzYWdlX3V0aWxzIGltcG9ydCAoCiAgICAgICAgICAgIHNlbmRf"
    "bWVzc2FnZSwKICAgICAgICAgICAgZGVsZXRlX21lc3NhZ2UsCiAgICAgICAgKQoKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2Uo"
    "bWVzc2FnZSwgZiLimqDvuI8ge3JlYXNvbn0iKQogICAgICAgIHRyeToKICAgICAgICAgICAgYXdhaXQgZGVsZXRlX21lc3NhZ2Uo"
    "bWVzc2FnZSkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBwYXNzCiAgICBleGNlcHQgRXhjZXB0aW9uIGFz"
    "IGU6CiAgICAgICAgX2xvZyhmIldaRklYIG11c2ljIHJlZnVzZSBmYWlsZWQ6IHtlfSIpCgoKYXN5bmMgZGVmIHByZV9yZXNvbHZl"
    "KG1lc3NhZ2UsIGNsaWVudD1Ob25lLCBpc19sZWVjaD1GYWxzZSwgaXNfeXRkbD1GYWxzZSk6CiAgICAiIiJDYWxsZWQgYXQgdGhl"
    "IHZlcnkgdG9wIG9mIG5ld19ldmVudCBmb3IgL2wtZmFtaWx5IGFuZCB5dGRsLWZhbWlseQogICAgY29tbWFuZHMuIFJldHVybnMg"
    "Il9fV1pGSVhfTVVTSUNfRE9ORV9fIiB3aGVuIHRoZSBjYWxsZXIgbXVzdCByZXR1cm4KICAgIChsaW5rIHJlZnVzZWQsIG9yIHRo"
    "ZSB0YXNrIHdhcyByZS1kaXNwYXRjaGVkIHRocm91Z2ggdGhlIHl0ZGwKICAgIGVuZ2luZSk7IE5vbmUgd2hlbiB0aGUgZmxvdyBz"
    "aG91bGQgY29udGludWUg4oCUIG5vIG11c2ljIGxpbmsgZm91bmQsCiAgICBvciB0aGUgdGV4dCB3YXMgcmV3cml0dGVuIGluIHBs"
    "YWNlIGZvciB0aGUgeXRkbCBlbmdpbmUuIEZhaWxzIG9wZW46CiAgICBhbnkgaW50ZXJuYWwgZXJyb3IgbGVhdmVzIHRoZSBvcmln"
    "aW5hbCBjb21tYW5kIHVudG91Y2hlZC4iIiIKICAgIHRyeToKICAgICAgICB0ZXh0ID0gbWVzc2FnZS50ZXh0IG9yIG1lc3NhZ2Uu"
    "Y2FwdGlvbiBvciAiIgogICAgICAgIGlmICJodHRwIiBub3QgaW4gdGV4dC5sb3dlcigpOgogICAgICAgICAgICByZXR1cm4gTm9u"
    "ZQogICAgICAgIG11c2ljX3VybCA9IE5vbmUKICAgICAgICBmb3IgdSBpbiBfVVJMX1JFLmZpbmRhbGwodGV4dCk6CiAgICAgICAg"
    "ICAgIGlmIGlzX211c2ljX3VybCh1KToKICAgICAgICAgICAgICAgIG11c2ljX3VybCA9IHUucnN0cmlwKCIpLixdPlwiJyIpCiAg"
    "ICAgICAgICAgICAgICBicmVhawogICAgICAgIGlmIG11c2ljX3VybCBpcyBOb25lOgogICAgICAgICAgICByZXR1cm4gTm9uZQog"
    "ICAgICAgIF9sb2coZiJXWkZJWCBtdXNpYzogcmVzb2x2aW5nIHttdXNpY191cmxbOjEyMF19IikKCiAgICAgICAgdHJ5OgogICAg"
    "ICAgICAgICB5dHEsIHNvdXJjZSA9IGF3YWl0IHJlc29sdmVfbXVzaWMobXVzaWNfdXJsLCBfdXNlcl9vZihtZXNzYWdlKSkKICAg"
    "ICAgICBleGNlcHQgTXVzaWNVbnN1cHBvcnRlZCBhcyBlOgogICAgICAgICAgICBhd2FpdCBfcmVmdXNlKG1lc3NhZ2UsIHN0cihl"
    "KSkKICAgICAgICAgICAgcmV0dXJuICJfX1daRklYX01VU0lDX0RPTkVfXyIKICAgICAgICBpZiBub3QgeXRxOgogICAgICAgICAg"
    "ICBhd2FpdCBfcmVmdXNlKAogICAgICAgICAgICAgICAgbWVzc2FnZSwKICAgICAgICAgICAgICAgICJDb3VsZG4ndCByZWFkIHRo"
    "aXMgbXVzaWMgbGluayDigJQgdHJ5IGFnYWluIGluIGEgbWludXRlLCAiCiAgICAgICAgICAgICAgICAib3Igc2VuZCBhIFlvdVR1"
    "YmUgbGluayIsCiAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuICJfX1daRklYX01VU0lDX0RPTkVfXyIKICAgICAgICB0"
    "cnk6CiAgICAgICAgICAgIGlmIGFkbWluX2xvZyBpcyBub3QgTm9uZToKICAgICAgICAgICAgICAgIGZyb20gaHRtbCBpbXBvcnQg"
    "ZXNjYXBlIGFzIF9lc2MKCiAgICAgICAgICAgICAgICBhd2FpdCBhZG1pbl9sb2coCiAgICAgICAgICAgICAgICAgICAgIvCfjrUg"
    "PGI+TXVzaWMgbGluayByZXNvbHZlZDwvYj4iLAogICAgICAgICAgICAgICAgICAgIGYi4pSPIDxiPlVzZXI8L2I+IOKGkiA8Y29k"
    "ZT57X3VzZXJfb2YobWVzc2FnZSl9PC9jb2RlPlxuIgogICAgICAgICAgICAgICAgICAgIGYi4pSgIDxiPlNvdXJjZTwvYj4g4oaS"
    "IHtzb3VyY2V9XG4iCiAgICAgICAgICAgICAgICAgICAgZiLilKAgPGI+TGluazwvYj4g4oaSIHtfZXNjKG11c2ljX3VybFs6OTAw"
    "XSl9XG4iCiAgICAgICAgICAgICAgICAgICAgZiLilKAgPGI+U2VhcmNoPC9iPiDihpIge19lc2MoeXRxWzo1MDBdKX1cbiIKICAg"
    "ICAgICAgICAgICAgICAgICBmIuKUliA8Yj5NZXNzYWdlPC9iPiDihpIgIgogICAgICAgICAgICAgICAgICAgIGYie19lc2MoKG1l"
    "c3NhZ2UudGV4dCBvciBtZXNzYWdlLmNhcHRpb24gb3IgJycpWzoxMDAwXSkgb3IgJyhubyB0ZXh0KSd9IiwKICAgICAgICAgICAg"
    "ICAgICkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBwYXNzCiAgICAgICAgbWVzc2FnZS5fd3pmaXhfbXVz"
    "aWNfbm90ZSA9IGYie3NvdXJjZX06IHttdXNpY191cmx9IOKGkiB7eXRxfSIKICAgICAgICBpZiBpc195dGRsOgogICAgICAgICAg"
    "ICAjIGFscmVhZHkgaW4gdGhlIHl0ZGwgZW5naW5lIOKAlCByZXdyaXRlIHRoZSBsaW5rIGluIHBsYWNlCiAgICAgICAgICAgIHRy"
    "eToKICAgICAgICAgICAgICAgIGlmIG1lc3NhZ2UudGV4dDoKICAgICAgICAgICAgICAgICAgICBtZXNzYWdlLnRleHQgPSBtZXNz"
    "YWdlLnRleHQucmVwbGFjZShtdXNpY191cmwsIHl0cSkKICAgICAgICAgICAgICAgIGlmIG1lc3NhZ2UuY2FwdGlvbjoKICAgICAg"
    "ICAgICAgICAgICAgICBtZXNzYWdlLmNhcHRpb24gPSBtZXNzYWdlLmNhcHRpb24ucmVwbGFjZShtdXNpY191cmwsIHl0cSkKICAg"
    "ICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAgICAgcmV0dXJuIE5vbmUKICAg"
    "ICAgICAjIG1pcnJvciBjb21tYW5kICgvbCBmYW1pbHkpIOKAlCByZS1kaXNwYXRjaCB0aHJvdWdoIHRoZSB5dGRsCiAgICAgICAg"
    "IyBlbmdpbmUgd2l0aCBhIGNsZWFuIC95bCBjb21tYW5kIHNvIG5vIC9sIGFyZyBzeW50YXggbGVha3MKICAgICAgICB0cnk6CiAg"
    "ICAgICAgICAgIG1lc3NhZ2UudGV4dCA9IGYiL3lsIHt5dHF9IgogICAgICAgICAgICBtZXNzYWdlLmNhcHRpb24gPSBOb25lCiAg"
    "ICAgICAgICAgIGZyb20gLi4ubW9kdWxlcy55dGRscCBpbXBvcnQgWXREbHAKCiAgICAgICAgICAgIGF3YWl0IFl0RGxwKGNsaWVu"
    "dCwgbWVzc2FnZSwgaXNfbGVlY2g9Ym9vbChpc19sZWVjaCkpLm5ld19ldmVudCgpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBh"
    "cyBlOgogICAgICAgICAgICBfbG9nKGYiV1pGSVggbXVzaWMgcmUtZGlzcGF0Y2ggZmFpbGVkOiB7ZX0iKQogICAgICAgICAgICBy"
    "ZXR1cm4gTm9uZQogICAgICAgIHJldHVybiAiX19XWkZJWF9NVVNJQ19ET05FX18iCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6"
    "CiAgICAgICAgX2xvZyhmIldaRklYIG11c2ljIHByZV9yZXNvbHZlIGZhaWxlZDoge2V9IikKICAgICAgICByZXR1cm4gTm9uZQo="
)



# bot/modules/wzfix_admin.py — Telegram commands: /find /usage /qusers
# /setcap /addcap /resetcap /botcap /dbstats /dbclean
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
    "gOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiBnZXRfYWRtaW5fcGFzcygpOgogICAgIiIiQ3VycmVudCBBRE1JTl9QQVNTIChD"
    "b25maWcuQURNSU5fUEFTUyBmcm9tIC9icyB3aW5zLCB0aGVuIERCKS4KCiAgICBQcmlvcml0eTogL2JzLXNldCBBRE1JTl9QQVNT"
    "IChsaXZlIGNvbmZpZywgaW5jbHVkZXMgY29uZmlnLmVudikg4oaSCiAgICBEQi1zdG9yZWQgdmFsdWUgKGF1dG8tZ2VuZXJhdGVk"
    "IG9yIHNldCB2aWEgL2FkbWlucGFzcykuCiAgICAiIiIKICAgICMgL2JzLXNldCBBRE1JTl9QQVNTIHdpbnMgKHNhbWUgcGF0dGVy"
    "biBhcyBTVFJFQU1fUEFTUykKICAgIHRyeToKICAgICAgICBmcm9tIC4uLmNvcmUuY29uZmlnX21hbmFnZXIgaW1wb3J0IENvbmZp"
    "ZwoKICAgICAgICBfYnMgPSBzdHIoZ2V0YXR0cihDb25maWcsICJBRE1JTl9QQVNTIiwgIiIpIG9yICIiKS5zdHJpcCgpCiAgICAg"
    "ICAgaWYgX2JzOgogICAgICAgICAgICByZXR1cm4gX2JzCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHRy"
    "eToKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9jb25maWdbX3BhcnQoKV0KICAgICAgICBkb2MgPSBhd2FpdCBjb2wuZmluZF9v"
    "bmUoeyJfaWQiOiAiYWRtaW4ifSkKICAgICAgICBpZiBkb2MgYW5kIGRvYy5nZXQoInBhc3MiKToKICAgICAgICAgICAgcmV0dXJu"
    "IHN0cihkb2NbInBhc3MiXSkKICAgICAgICBmcm9tIG9zIGltcG9ydCBnZXRlbnYKCiAgICAgICAgcHcgPSBnZXRlbnYoIldaRklY"
    "X0FETUlOX1BBU1MiLCAiIikuc3RyaXAoKSBvciB0b2tlbl91cmxzYWZlKDYpCiAgICAgICAgYXdhaXQgY29sLnVwZGF0ZV9vbmUo"
    "eyJfaWQiOiAiYWRtaW4ifSwgeyIkc2V0IjogeyJwYXNzIjogcHd9fSwgdXBzZXJ0PVRydWUpCiAgICAgICAgcmV0dXJuIHB3CiAg"
    "ICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAiIgoKCmFzeW5jIGRlZiBzZXRfYWRtaW5fcGFzcyhuZXdfcGFzcyk6"
    "CiAgICBjb2wgPSBfZGIoKS53emZpeF9jb25maWdbX3BhcnQoKV0KICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAgICAgIHsi"
    "X2lkIjogImFkbWluIn0sIHsiJHNldCI6IHsicGFzcyI6IHN0cihuZXdfcGFzcykuc3RyaXAoKX19LCB1cHNlcnQ9VHJ1ZQogICAg"
    "KQoKCmFzeW5jIGRlZiBfYWRtaW5fc2VjcmV0KCk6CiAgICAiIiJSYW5kb20gcGVyLWJvdCBzaWduaW5nIHNlY3JldCAoY3JlYXRl"
    "ZCBvbmNlLCBzdG9yZWQgaW4gd3pmaXhfY29uZmlnKS4iIiIKICAgIHRyeToKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9jb25m"
    "aWdbX3BhcnQoKV0KICAgICAgICBkb2MgPSBhd2FpdCBjb2wuZmluZF9vbmUoeyJfaWQiOiAiYWRtaW5fc2VjcmV0In0pCiAgICAg"
    "ICAgaWYgZG9jIGFuZCBkb2MuZ2V0KCJzZWNyZXQiKToKICAgICAgICAgICAgcmV0dXJuIHN0cihkb2NbInNlY3JldCJdKQogICAg"
    "ICAgIHNlY3JldCA9IHRva2VuX2hleCgzMikKICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgeyJfaWQi"
    "OiAiYWRtaW5fc2VjcmV0In0sIHsiJHNldCI6IHsic2VjcmV0Ijogc2VjcmV0fX0sIHVwc2VydD1UcnVlCiAgICAgICAgKQogICAg"
    "ICAgIHJldHVybiBzZWNyZXQKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuICJ3emZpeC1uby1zZWNyZXQiCgoK"
    "YXN5bmMgZGVmIF9zZXNzaW9uX3Rva2VuKCk6CiAgICBzZWNyZXQgPSBhd2FpdCBfYWRtaW5fc2VjcmV0KCkKICAgIGV4cCA9IGlu"
    "dCh0aW1lKCkpICsgU0VTU0lPTl9IICogMzYwMAogICAgc2lnID0gaG1hY19uZXcoc2VjcmV0LmVuY29kZSgpLCBmImFkbWluOntl"
    "eHB9Ii5lbmNvZGUoKSwgc2hhMjU2KS5oZXhkaWdlc3QoKQogICAgcmV0dXJuIGYie2V4cH0ue3NpZ30iCgoKYXN5bmMgZGVmIF9j"
    "aGVja19zZXNzaW9uKHJlcXVlc3QpOgogICAgdHJ5OgogICAgICAgIHRvayA9IHJlcXVlc3QuY29va2llcy5nZXQoInd6YWRtaW4i"
    "LCAiIikKICAgICAgICBleHAsIF8sIHNpZyA9IHRvay5wYXJ0aXRpb24oIi4iKQogICAgICAgIHNlY3JldCA9IGF3YWl0IF9hZG1p"
    "bl9zZWNyZXQoKQogICAgICAgIGdvb2QgPSBobWFjX25ldyhzZWNyZXQuZW5jb2RlKCksIGYiYWRtaW46e2V4cH0iLmVuY29kZSgp"
    "LCBzaGEyNTYpLmhleGRpZ2VzdCgpCiAgICAgICAgaWYgbm90IHRvayBvciBub3QgY29tcGFyZV9kaWdlc3Qoc2lnLCBnb29kKToK"
    "ICAgICAgICAgICAgcmV0dXJuIEZhbHNlCiAgICAgICAgcmV0dXJuIGludChleHApID4gdGltZSgpCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uOgogICAgICAgIHJldHVybiBGYWxzZQoKCmRlZiBfY2xpZW50X2lwKHJlcXVlc3QpOgogICAgZndkID0gcmVxdWVzdC5oZWFk"
    "ZXJzLmdldCgiWC1Gb3J3YXJkZWQtRm9yIiwgIiIpCiAgICBpZiBmd2Q6CiAgICAgICAgcmV0dXJuIGZ3ZC5zcGxpdCgiLCIpWzBd"
    "LnN0cmlwKClbOjY0XQogICAgdHJ5OgogICAgICAgIHJldHVybiAocmVxdWVzdC5yZW1vdGUgb3IgIj8iKVs6NjRdCiAgICBleGNl"
    "cHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAiPyIKCgpkZWYgX3VhX29rKHJlcXVlc3QpOgogICAgdWEgPSAocmVxdWVzdC5o"
    "ZWFkZXJzLmdldCgiVXNlci1BZ2VudCIpIG9yICIiKS5zdHJpcCgpLmxvd2VyKCkKICAgIGlmIG5vdCB1YSBvciBsZW4odWEpIDwg"
    "MTA6CiAgICAgICAgcmV0dXJuIEZhbHNlCiAgICBpZiBhbnkoYiBpbiB1YSBmb3IgYiBpbiBfQkFEX1VBKToKICAgICAgICByZXR1"
    "cm4gRmFsc2UKICAgIHJldHVybiBhbnkoayBpbiB1YSBmb3IgayBpbiBfS05PV05fVUEpCgoKZGVmIF9zdWJuZXQoaXApOgogICAg"
    "IiIiSVB2NCAvMjQgb3IgSVB2NiAvNjQgcHJlZml4IGZvciBzdWJuZXQtbGV2ZWwgYmFucy4iIiIKICAgIHRyeToKICAgICAgICBp"
    "ZiAiOiIgaW4gaXA6CiAgICAgICAgICAgIHJldHVybiAiLyIuam9pbihpcC5zcGxpdCgiOiIpWzo0XSkgKyAiOjovNjQiCiAgICAg"
    "ICAgcGFydHMgPSBpcC5zcGxpdCgiLiIpCiAgICAgICAgaWYgbGVuKHBhcnRzKSA9PSA0OgogICAgICAgICAgICByZXR1cm4gIi4i"
    "LmpvaW4ocGFydHNbOjNdKSArICIuMC8yNCIKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIGlw"
    "CgoKZGVmIF9ub3JtX2lwKGlwKToKICAgIGlwID0gKGlwIG9yICIiKS5zdHJpcCgpCiAgICBpZiBpcC5zdGFydHN3aXRoKCI6OmZm"
    "ZmY6Iik6CiAgICAgICAgaXAgPSBpcFs3Ol0KICAgIHJldHVybiBpcFs6NDVdCgoKYXN5bmMgZGVmIF9pc19iYW5uZWQoaXApOgog"
    "ICAgIiIiSVAgb3Igc3VibmV0IGJhbiBjaGVjayAocGVyc2lzdGVudCwgREItYmFja2VkKS4iIiIKICAgIHRyeToKICAgICAgICBj"
    "b2wgPSBfZGIoKS53emZpeF9iYW5zW19wYXJ0KCldCiAgICAgICAgaWYgYXdhaXQgY29sLmZpbmRfb25lKHsiX2lkIjogX25vcm1f"
    "aXAoaXApfSk6CiAgICAgICAgICAgIHJldHVybiBUcnVlCiAgICAgICAgaWYgYXdhaXQgY29sLmZpbmRfb25lKHsiX2lkIjogInN1"
    "YjoiICsgX3N1Ym5ldChfbm9ybV9pcChpcCkpfSk6CiAgICAgICAgICAgIHJldHVybiBUcnVlCiAgICBleGNlcHQgRXhjZXB0aW9u"
    "OgogICAgICAgIHBhc3MKICAgIHJldHVybiBGYWxzZQoKCmFzeW5jIGRlZiBfYmFuX2lwKGlwLCBzdWJuZXRfdG9vPVRydWUpOgog"
    "ICAgdHJ5OgogICAgICAgIGNvbCA9IF9kYigpLnd6Zml4X2JhbnNbX3BhcnQoKV0KICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29u"
    "ZSgKICAgICAgICAgICAgeyJfaWQiOiBfbm9ybV9pcChpcCl9LAogICAgICAgICAgICB7IiRzZXQiOiB7InBlcm0iOiBUcnVlLCAi"
    "dHMiOiB0aW1lKCl9fSwKICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgKQogICAgICAgIGlmIHN1Ym5ldF90b286CiAg"
    "ICAgICAgICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiAic3ViOiIgKyBfc3VibmV0KF9u"
    "b3JtX2lwKGlwKSl9LAogICAgICAgICAgICAgICAgeyIkc2V0IjogeyJwZXJtIjogVHJ1ZSwgInRzIjogdGltZSgpfX0sCiAgICAg"
    "ICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoK"
    "YXN5bmMgZGVmIF9wcmlvcl9mYWlscyhpcCk6CiAgICAiIiJGYWlsZWQtYXR0ZW1wdCBjb3VudCBjdXJyZW50bHkgb24gcmVjb3Jk"
    "IGZvciB0aGlzIElQLiIiIgogICAgdHJ5OgogICAgICAgIGRvYyA9IGF3YWl0IF9kYigpLnd6Zml4X2xvY2tzW19wYXJ0KCldLmZp"
    "bmRfb25lKHsiX2lkIjogX25vcm1faXAoaXApfSkKICAgICAgICByZXR1cm4gaW50KChkb2Mgb3Ige30pLmdldCgiZmFpbHMiKSBv"
    "ciAwKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gMAoKCmFzeW5jIGRlZiBfYWxsb3dfaXAoaXAsIGV4dHJh"
    "PTMpOgogICAgIiIiT3duZXIgdW5iYW5uZWQgYW4gSVA6IGNsZWFyIGJhbnMgKyBsb2Nrb3V0LCBncmFudCBOIHJldHJ5IGF0dGVt"
    "cHRzLgoKICAgIFRoZSB0aWVyIGlzIEtFUFQsIHNvIGJ1cm5pbmcgdGhlIHJldHJpZXMgZXNjYWxhdGVzIHRvIHRoZSBuZXh0IHN0"
    "YWdlCiAgICAobG9uZ2VyIHRoYW4gdGhlIHByZXZpb3VzIGJhbikuCiAgICAiIiIKICAgIHRyeToKICAgICAgICBpcCA9IF9ub3Jt"
    "X2lwKHN0cihpcCkpCiAgICAgICAgYmNvbCA9IF9kYigpLnd6Zml4X2JhbnNbX3BhcnQoKV0KICAgICAgICBhd2FpdCBiY29sLmRl"
    "bGV0ZV9vbmUoeyJfaWQiOiBpcH0pCiAgICAgICAgYXdhaXQgYmNvbC5kZWxldGVfb25lKHsiX2lkIjogInN1YjoiICsgX3N1Ym5l"
    "dChpcCl9KQogICAgICAgIGxjb2wgPSBfZGIoKS53emZpeF9sb2Nrc1tfcGFydCgpXQogICAgICAgIGF3YWl0IGxjb2wudXBkYXRl"
    "X29uZSgKICAgICAgICAgICAgeyJfaWQiOiBpcH0sCiAgICAgICAgICAgIHsiJHNldCI6IHsiZmFpbHMiOiAwLCAiZXh0cmEiOiBp"
    "bnQoZXh0cmEpLCAidW50aWwiOiAwLCAicGVybSI6IEZhbHNlfX0sCiAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICkK"
    "ICAgICAgICByZXR1cm4gVHJ1ZQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gRmFsc2UKCgphc3luYyBkZWYg"
    "X2FsbG93X2FsbCgpOgogICAgIiIiQ2xlYXIgZXZlcnkgYmFuLCBzdWJuZXQgYmFuIGFuZCBsb2Nrb3V0LiIiIgogICAgdHJ5Ogog"
    "ICAgICAgIG4gPSAwCiAgICAgICAgZm9yIGNvbGwgaW4gKCJ3emZpeF9iYW5zIiwgInd6Zml4X2xvY2tzIik6CiAgICAgICAgICAg"
    "IHIgPSBhd2FpdCBfZGIoKVtjb2xsXVtfcGFydCgpXS5kZWxldGVfbWFueSh7fSkKICAgICAgICAgICAgbiArPSBpbnQoZ2V0YXR0"
    "cihyLCAiZGVsZXRlZF9jb3VudCIsIDApIG9yIDApCiAgICAgICAgcmV0dXJuIG4KICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAg"
    "ICAgcmV0dXJuIDAKCgphc3luYyBkZWYgX2xpc3RfYmFucygpOgogICAgIiIiQWxsIGJhbnMgYW5kIGxvY2tvdXRzLCBmb3IgdGhl"
    "IC9iYW5zIGNvbW1hbmQuIiIiCiAgICBvdXQgPSBbXQogICAgdHJ5OgogICAgICAgIGFzeW5jIGZvciBkIGluIF9kYigpLnd6Zml4"
    "X2JhbnNbX3BhcnQoKV0uZmluZCh7fSwgbGltaXQ9NTApOgogICAgICAgICAgICBvdXQuYXBwZW5kKAogICAgICAgICAgICAgICAg"
    "ewogICAgICAgICAgICAgICAgICAgICJraW5kIjogImJhbiIsCiAgICAgICAgICAgICAgICAgICAgImlkIjogZC5nZXQoIl9pZCIs"
    "ICI/IiksCiAgICAgICAgICAgICAgICAgICAgInRzIjogZC5nZXQoInRzIiwgMCksCiAgICAgICAgICAgICAgICB9CiAgICAgICAg"
    "ICAgICkKICAgICAgICBhc3luYyBmb3IgZCBpbiBfZGIoKS53emZpeF9sb2Nrc1tfcGFydCgpXS5maW5kKHt9LCBsaW1pdD01MCk6"
    "CiAgICAgICAgICAgIGlmIGQuZ2V0KCJwZXJtIik6CiAgICAgICAgICAgICAgICBvdXQuYXBwZW5kKHsia2luZCI6ICJwZXJtLWxv"
    "Y2siLCAiaWQiOiBkLmdldCgiX2lkIiwgIj8iKSwgInRzIjogMH0pCiAgICAgICAgICAgIGVsaWYgKGQuZ2V0KCJ1bnRpbCIpIG9y"
    "IDApID4gdGltZSgpOgogICAgICAgICAgICAgICAgb3V0LmFwcGVuZCgKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAg"
    "ICAgICAgICAgICAgICJraW5kIjogImxvY2tvdXQiLAogICAgICAgICAgICAgICAgICAgICAgICAiaWQiOiBkLmdldCgiX2lkIiwg"
    "Ij8iKSwKICAgICAgICAgICAgICAgICAgICAgICAgInVudGlsIjogZC5nZXQoInVudGlsIiksCiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICJ0aWVyIjogZC5nZXQoInRpZXIiLCAwKSwKICAgICAgICAgICAgICAgICAgICAgICAgImV4dHJhIjogZC5nZXQoImV4dHJh"
    "IiwgMCksCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgKQogICAgICAgICAgICBlbGlmIGQuZ2V0KCJleHRy"
    "YSIpOgogICAgICAgICAgICAgICAgb3V0LmFwcGVuZCgKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICJraW5kIjogImFsbG93ZWQiLAogICAgICAgICAgICAgICAgICAgICAgICAiaWQiOiBkLmdldCgiX2lkIiwgIj8iKSwKICAg"
    "ICAgICAgICAgICAgICAgICAgICAgImV4dHJhIjogZC5nZXQoImV4dHJhIiksCiAgICAgICAgICAgICAgICAgICAgICAgICJ0aWVy"
    "IjogZC5nZXQoInRpZXIiLCAwKSwKICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICApCiAgICBleGNlcHQgRXhj"
    "ZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiBvdXQKCgpkZWYgX2NoYXRfaW50KGNoYXQpOgogICAgIiIicHlyb2dyYW0g"
    "dHJlYXRzIHN0cmluZyBjaGF0IGlkcyBhcyBAdXNlcm5hbWVzIOKAlCBudW1lcmljIGlkcwogICAgTVVTVCBiZSBpbnQgb3IgdGhl"
    "IHNlbmQgZmFpbHMgd2l0aCBwZWVyLW5vdC1mb3VuZC4iIiIKICAgIGNoYXQgPSBzdHIoY2hhdCBvciAiIikuc3RyaXAoKQogICAg"
    "aWYgbm90IGNoYXQ6CiAgICAgICAgcmV0dXJuICIiCiAgICBpZiBjaGF0LmxzdHJpcCgiLSIpLmlzZGlnaXQoKToKICAgICAgICBy"
    "ZXR1cm4gaW50KGNoYXQpCiAgICByZXR1cm4gY2hhdAoKCmFzeW5jIGRlZiBfbG9naW5fYWxlcnQoaXAsIHJlcXVlc3QsIGRldiwg"
    "c3VibWl0dGVkLCB0aXRsZSk6CiAgICAiIiJEYXNoYm9hcmQgbG9naW4gZXZlbnQgLT4gTE9HX0NIQVQgKHRvZ2dsZTogQURNSU5f"
    "TE9HSU5fQUxFUlRTIHZpYSAvYnMpLiIiIgogICAgdHJ5OgogICAgICAgIGZyb20gLi4uY29yZS5jb25maWdfbWFuYWdlciBpbXBv"
    "cnQgQ29uZmlnCiAgICAgICAgZnJvbSAuLi5jb3JlLnRnX2NsaWVudCBpbXBvcnQgVGdDbGllbnQKCiAgICAgICAgaWYgbm90IGdl"
    "dGF0dHIoQ29uZmlnLCAiQURNSU5fTE9HSU5fQUxFUlRTIiwgVHJ1ZSk6CiAgICAgICAgICAgIHJldHVybgogICAgICAgIGNoYXQg"
    "PSBzdHIoZ2V0YXR0cihDb25maWcsICJBRE1JTl9MT0dfQ0hBVCIsICIiKSBvciAiIikuc3RyaXAoKQogICAgICAgIGlmIG5vdCBj"
    "aGF0OgogICAgICAgICAgICBjaGF0ID0gc3RyKGdldGF0dHIoQ29uZmlnLCAiTE9HX0NIQVQiLCAiIikgb3IgIiIpLnN0cmlwKCkK"
    "ICAgICAgICBpZiBub3QgY2hhdDoKICAgICAgICAgICAgcmV0dXJuCiAgICAgICAgaGRycyA9IFtdCiAgICAgICAgZm9yIGsgaW4g"
    "KAogICAgICAgICAgICAiVXNlci1BZ2VudCIsICJBY2NlcHQtTGFuZ3VhZ2UiLCAiUmVmZXJlciIsICJTZWMtQ2gtVWEiLAogICAg"
    "ICAgICAgICAiU2VjLUNoLVVhLVBsYXRmb3JtIiwgIlNlYy1DaC1VYS1Nb2JpbGUiLCAiU2VjLUZldGNoLVNpdGUiLAogICAgICAg"
    "ICAgICAiU2VjLUZldGNoLU1vZGUiLCAiU2VjLUZldGNoLURlc3QiLAogICAgICAgICk6CiAgICAgICAgICAgIHRyeToKICAgICAg"
    "ICAgICAgICAgIHYgPSByZXF1ZXN0LmhlYWRlcnMuZ2V0KGspCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "ICAgICAgICB2ID0gTm9uZQogICAgICAgICAgICBpZiB2OgogICAgICAgICAgICAgICAgaGRycy5hcHBlbmQoZiLilKAge2t9OiB7"
    "c3RyKHYpWzoxMjBdfSIpCiAgICAgICAgZGV2X2wgPSBbXQogICAgICAgIGZvciBrLCBsYmwgaW4gKAogICAgICAgICAgICAoInBs"
    "YXRmb3JtIiwgIlBsYXRmb3JtIiksICgibGFuZyIsICJMYW5ndWFnZSIpLAogICAgICAgICAgICAoImxhbmdzIiwgIkxhbmd1YWdl"
    "cyIpLCAoInR6IiwgIlRpbWV6b25lIiksCiAgICAgICAgICAgICgic2NyZWVuIiwgIlNjcmVlbiIpLCAoImRwciIsICJQaXhlbCBy"
    "YXRpbyIpLAogICAgICAgICAgICAoIm1lbSIsICJEZXZpY2UgbWVtb3J5IiksICgiY29yZXMiLCAiQ1BVIGNvcmVzIiksCiAgICAg"
    "ICAgICAgICgidG91Y2giLCAiVG91Y2ggcG9pbnRzIiksICgiY29va2llcyIsICJDb29raWVzIiksCiAgICAgICAgICAgICgid2Vi"
    "ZHJpdmVyIiwgIkF1dG9tYXRpb24iKSwgKCJuZXQiLCAiTmV0d29yayIpLAogICAgICAgICk6CiAgICAgICAgICAgIHYgPSBkZXYu"
    "Z2V0KGspCiAgICAgICAgICAgIGlmIHYgaXMgbm90IE5vbmUgYW5kIHYgIT0gIiI6CiAgICAgICAgICAgICAgICBkZXZfbC5hcHBl"
    "bmQoZiLilKAge2xibH06IHtzdHIodilbOjgwXX0iKQogICAgICAgIGxpbmVzID0gWwogICAgICAgICAgICAi8J+boSA8Yj5XWkZJ"
    "WCBkYXNoYm9hcmQ8L2I+IiwKICAgICAgICAgICAgdGl0bGUsCiAgICAgICAgICAgICIiLAogICAgICAgICAgICBmIuKUjyA8Yj5J"
    "UDwvYj4g4oaSIDxjb2RlPntpcH08L2NvZGU+IiwKICAgICAgICAgICAgZiLilKMgPGI+VHJpZWQ8L2I+IOKGkiA8Y29kZT57c3Ry"
    "KHN1Ym1pdHRlZClbOjQ4XX08L2NvZGU+IiwKICAgICAgICBdCiAgICAgICAgaWYgZGV2X2w6CiAgICAgICAgICAgIGxpbmVzICs9"
    "IFsi4pSjIDxiPkRldmljZTwvYj4iXSArIGRldl9sCiAgICAgICAgaWYgaGRyczoKICAgICAgICAgICAgbGluZXMgKz0gWyLilKMg"
    "PGI+SGVhZGVyczwvYj4iXSArIGhkcnNbOjldCiAgICAgICAgbGluZXMuYXBwZW5kKCLilJYgdmlhIC9icyBBRE1JTl9MT0dJTl9B"
    "TEVSVFMgdG8gdG9nZ2xlIikKICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Quc2VuZF9tZXNzYWdlKAogICAgICAgICAgICBjaGF0"
    "X2lkPV9jaGF0X2ludChjaGF0KSwKICAgICAgICAgICAgdGV4dD0iXG4iLmpvaW4obGluZXMpWzozOTAwXSwKICAgICAgICAgICAg"
    "ZGlzYWJsZV93ZWJfcGFnZV9wcmV2aWV3PVRydWUsCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNz"
    "CgoKYXN5bmMgZGVmIF9pcF9pbnRlbChpcCk6CiAgICAiIiJEYXRhY2VudGVyL1ZQTiBkZXRlY3Rpb24gdmlhIGlwLWFwaS5jb20g"
    "KGZyZWUgdGllciwgY2FjaGVkIDcgZGF5cykuCiAgICBSZXR1cm5zIFRydWUgd2hlbiB0aGUgSVAgbG9va3MgbGlrZSBhIHNlcnZl"
    "ci9wcm94eSAoLT4gYmFubmVkKS4iIiIKICAgIGlwID0gX25vcm1faXAoaXApCiAgICBpZiBub3QgaXAgb3IgaXAgaW4gKCIxMjcu"
    "MC4wLjEiLCAiOjoxIiwgIj8iLCAibG9jYWxob3N0Iik6CiAgICAgICAgcmV0dXJuIEZhbHNlCiAgICBpZiBub3QgaXAucmVwbGFj"
    "ZSgiLiIsICIiKS5pc2RpZ2l0KCk6ICAjIGlwdjYgb3IgdW5rbm93bjogc2tpcCBsb29rdXAKICAgICAgICByZXR1cm4gRmFsc2UK"
    "ICAgIHRyeToKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9pcGluZm9bX3BhcnQoKV0KICAgICAgICBjYWNoZWQgPSBhd2FpdCBj"
    "b2wuZmluZF9vbmUoeyJfaWQiOiBpcH0pCiAgICAgICAgaWYgY2FjaGVkIGFuZCB0aW1lKCkgLSBjYWNoZWQuZ2V0KCJ0cyIsIDAp"
    "IDwgSVBfSU5GT19UVEw6CiAgICAgICAgICAgIHJldHVybiBib29sKGNhY2hlZC5nZXQoImJhZCIpKQogICAgICAgIGltcG9ydCBq"
    "c29uIGFzIF9qc29uCiAgICAgICAgZnJvbSB1cmxsaWIucmVxdWVzdCBpbXBvcnQgdXJsb3BlbiwgUmVxdWVzdAoKICAgICAgICB1"
    "cmwgPSAoCiAgICAgICAgICAgICJodHRwOi8vaXAtYXBpLmNvbS9qc29uLyIgKyBpcAogICAgICAgICAgICArICI/ZmllbGRzPXN0"
    "YXR1cyxwcm94eSxob3N0aW5nLG1vYmlsZSxpc3AiCiAgICAgICAgKQogICAgICAgIHJlcSA9IFJlcXVlc3QodXJsLCBoZWFkZXJz"
    "PXsiVXNlci1BZ2VudCI6ICJ3emZpeC1kYXNoYm9hcmQifSkKICAgICAgICB3aXRoIHVybG9wZW4ocmVxLCB0aW1lb3V0PTgpIGFz"
    "IHI6CiAgICAgICAgICAgIGRhdGEgPSBfanNvbi5sb2FkcyhyLnJlYWQoKS5kZWNvZGUoInV0Zi04IiwgInJlcGxhY2UiKSkKICAg"
    "ICAgICBiYWQgPSBkYXRhLmdldCgic3RhdHVzIikgPT0gInN1Y2Nlc3MiIGFuZCAoCiAgICAgICAgICAgIGRhdGEuZ2V0KCJob3N0"
    "aW5nIikgb3IgZGF0YS5nZXQoInByb3h5IikKICAgICAgICApCiAgICAgICAgdHJ5OgogICAgICAgICAgICBhd2FpdCBjb2wudXBk"
    "YXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogaXB9LAogICAgICAgICAgICAgICAgeyIkc2V0IjogeyJiYWQiOiBib29s"
    "KGJhZCksICJpc3AiOiBkYXRhLmdldCgiaXNwIiwgIiIpLAogICAgICAgICAgICAgICAgICAgICAgICAgICJ0cyI6IHRpbWUoKX19"
    "LAogICAgICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgICAgICkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAg"
    "ICAgICAgICBwYXNzCiAgICAgICAgaWYgYmFkOgogICAgICAgICAgICBhd2FpdCBfYmFuX2lwKGlwKQogICAgICAgICAgICB0cnk6"
    "CiAgICAgICAgICAgICAgICBmcm9tIC4uLiBpbXBvcnQgTE9HR0VSCgogICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKAogICAg"
    "ICAgICAgICAgICAgICAgIGYiV1pGSVggZGFzaGJvYXJkOiBkYXRhY2VudGVyL3Byb3h5IElQIGJhbm5lZDoge2lwfSAiCiAgICAg"
    "ICAgICAgICAgICAgICAgZiIoe2RhdGEuZ2V0KCdpc3AnLCAnPycpfSkiCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgIGV4"
    "Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBwYXNzCiAgICAgICAgcmV0dXJuIGJvb2woYmFkKQogICAgZXhjZXB0IEV4"
    "Y2VwdGlvbjoKICAgICAgICByZXR1cm4gRmFsc2UgICMgaW50ZWwgdW5hdmFpbGFibGUgLT4gZG8gbm90IGJsb2NrCgoKYXN5bmMg"
    "ZGVmIF9sb2Nrb3V0X3N0YXRlKGlwKToKICAgICIiIlBlcnNpc3RlbnQgdGllci1iYXNlZCBsb2Nrb3V0LiBSZXR1cm5zIHNlY29u"
    "ZHMgcmVtYWluaW5nICgwID0gb3BlbikuIiIiCiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCkud3pmaXhfbG9ja3NbX3BhcnQo"
    "KV0KICAgICAgICBkb2MgPSBhd2FpdCBjb2wuZmluZF9vbmUoeyJfaWQiOiBfbm9ybV9pcChpcCl9KQogICAgICAgIGlmIG5vdCBk"
    "b2M6CiAgICAgICAgICAgIHJldHVybiAwCiAgICAgICAgaWYgZG9jLmdldCgicGVybSIpOgogICAgICAgICAgICByZXR1cm4gLTEK"
    "ICAgICAgICB1bnRpbCA9IGRvYy5nZXQoInVudGlsIikgb3IgMAogICAgICAgIHJldHVybiBtYXgodW50aWwgLSB0aW1lKCksIDAp"
    "CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAwCgoKYXN5bmMgZGVmIF9yZWNvcmRfZmFpbChpcCk6CiAgICAi"
    "IiIzIGZhaWxzIC0+IGVzY2FsYXRpbmcgbG9ja291dDogMjRoLCA3MmgsIDdkLCAzMGQsIHBlcm1hbmVudC4iIiIKICAgIHRyeToK"
    "ICAgICAgICBmcm9tIHB5bW9uZ28gaW1wb3J0IFJldHVybkRvY3VtZW50IGFzIF9SRAoKICAgICAgICBjb2wgPSBfZGIoKS53emZp"
    "eF9sb2Nrc1tfcGFydCgpXQogICAgICAgIGRvYyA9IGF3YWl0IGNvbC5maW5kX29uZV9hbmRfdXBkYXRlKAogICAgICAgICAgICB7"
    "Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgIHsiJGluYyI6IHsiZmFpbHMiOiAxfSwgIiRzZXQiOiB7InRzIjogdGlt"
    "ZSgpfX0sCiAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICAgICByZXR1cm5fZG9jdW1lbnQ9X1JELkFGVEVSLAogICAg"
    "ICAgICkKICAgICAgICBmYWlscyA9IGludCgoZG9jIG9yIHt9KS5nZXQoImZhaWxzIikgb3IgMCkKICAgICAgICBleHRyYSA9IGlu"
    "dCgoZG9jIG9yIHt9KS5nZXQoImV4dHJhIikgb3IgMCkKICAgICAgICBpZiBleHRyYSA+IDA6CiAgICAgICAgICAgICMgcmV0cmll"
    "cyBncmFudGVkIGJ5IC9hbGxvdzogZWFjaCBmYWlsIGNvbnN1bWVzIG9uZTsgdGhlIGxhc3QKICAgICAgICAgICAgIyBvbmUgZXNj"
    "YWxhdGVzIHN0cmFpZ2h0IHRvIHRoZSBORVhUIHRpZXIgKGtlcHQgYWZ0ZXIgL2FsbG93KQogICAgICAgICAgICBsZWZ0ID0gZXh0"
    "cmEgLSAxCiAgICAgICAgICAgIHRpZXIgPSAoaW50KChkb2Mgb3Ige30pLmdldCgidGllciIpIG9yIDApKSArIDEKICAgICAgICAg"
    "ICAgc2VjcyA9IExPQ0tfVElFUlNfU1ttaW4odGllciAtIDEsIGxlbihMT0NLX1RJRVJTX1MpIC0gMSldCiAgICAgICAgICAgIGlm"
    "IGxlZnQgPD0gMDoKICAgICAgICAgICAgICAgIGlmIHNlY3MgPCAwOgogICAgICAgICAgICAgICAgICAgIGF3YWl0IGNvbC51cGRh"
    "dGVfb25lKAogICAgICAgICAgICAgICAgICAgICAgICB7Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgICAgICAgICAg"
    "ICAgIHsiJHNldCI6IHsicGVybSI6IFRydWUsICJmYWlscyI6IDAsICJ0aWVyIjogdGllciwgImV4dHJhIjogMH19LAogICAgICAg"
    "ICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICByZXR1cm4gLTEsIHRpZXIKICAgICAgICAgICAgICAgIGF3YWl0IGNv"
    "bC51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAgICAgICAg"
    "ICB7IiRzZXQiOiB7InVudGlsIjogdGltZSgpICsgc2VjcywgImZhaWxzIjogMCwgInRpZXIiOiB0aWVyLAogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAiZXh0cmEiOiAwfX0sCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICByZXR1cm4gc2Vj"
    "cywgdGllcgogICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAo"
    "aXApfSwKICAgICAgICAgICAgICAgIHsiJHNldCI6IHsiZmFpbHMiOiAwLCAiZXh0cmEiOiBsZWZ0fX0sCiAgICAgICAgICAgICkK"
    "ICAgICAgICAgICAgcmV0dXJuIDAsIGxlZnQKICAgICAgICBpZiBmYWlscyA8IExPR0lOX01BWF9GQUlMUzoKICAgICAgICAgICAg"
    "cmV0dXJuIDAsIGZhaWxzCiAgICAgICAgdGllciA9IChpbnQoKGRvYyBvciB7fSkuZ2V0KCJ0aWVyIikgb3IgMCkpICsgMQogICAg"
    "ICAgIHNlY3MgPSBMT0NLX1RJRVJTX1NbbWluKHRpZXIgLSAxLCBsZW4oTE9DS19USUVSU19TKSAtIDEpXQogICAgICAgIGlmIHNl"
    "Y3MgPCAwOgogICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAo"
    "aXApfSwKICAgICAgICAgICAgICAgIHsiJHNldCI6IHsicGVybSI6IFRydWUsICJmYWlscyI6IDAsICJ0aWVyIjogdGllcn19LAog"
    "ICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVybiAtMSwgdGllcgogICAgICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAg"
    "ICAgICAgICB7Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgIHsiJHNldCI6IHsidW50aWwiOiB0aW1lKCkgKyBzZWNz"
    "LCAiZmFpbHMiOiAwLCAidGllciI6IHRpZXJ9fSwKICAgICAgICApCiAgICAgICAgcmV0dXJuIHNlY3MsIHRpZXIKICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIDAsIDAKCgpkZWYgX3JlY29yZF9vayhpcCk6CiAgICB0cnk6CiAgICAgICAgZnJv"
    "bSBhc3luY2lvIGltcG9ydCBnZXRfZXZlbnRfbG9vcAoKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9sb2Nrc1tfcGFydCgpXQog"
    "ICAgICAgIGdldF9ldmVudF9sb29wKCkuY3JlYXRlX3Rhc2soCiAgICAgICAgICAgIGNvbC51cGRhdGVfb25lKAogICAgICAgICAg"
    "ICAgICAgeyJfaWQiOiBfbm9ybV9pcChpcCl9LAogICAgICAgICAgICAgICAgeyIkc2V0IjogeyJmYWlscyI6IDAsICJ0aWVyIjog"
    "MCwgInVudGlsIjogMH19LAogICAgICAgICAgICApCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNz"
    "CgoKZGVmIF9sb2NrX3R4dChzZWNzKToKICAgIGlmIHNlY3MgPCAwOgogICAgICAgIHJldHVybiAicGVybWFuZW50bHkgYmFubmVk"
    "IgogICAgaWYgc2VjcyA+PSA4NjQwMDoKICAgICAgICByZXR1cm4gZiJsb2NrZWQgZm9yIHtzZWNzIC8vIDg2NDAwfSBkYXkocyki"
    "CiAgICByZXR1cm4gZiJsb2NrZWQgZm9yIHtzZWNzIC8vIDM2MDAgKyAxfSBob3VyKHMpIgoKCiMg4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACiMgU3RhdGUgYnVpbGRpbmcgKHVzZXJz"
    "LCBsaXZlIHRhc2tzLCBnbG9iYWxzKQojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgAoKCmRlZiBfdGFza3Nfc25hcHNob3QoKToKICAgIG91dCA9IFtdCiAgICB0cnk6CiAgICAgICAg"
    "ZnJvbSAuLi4gaW1wb3J0IHRhc2tfZGljdAoKICAgICAgICBkZWYgc2FmZShmbiwgZGVmYXVsdD0iIik6CiAgICAgICAgICAgIHRy"
    "eToKICAgICAgICAgICAgICAgIHYgPSBmbigpCiAgICAgICAgICAgICAgICByZXR1cm4gdiBpZiBpc2luc3RhbmNlKHYsIChpbnQs"
    "IGZsb2F0LCBzdHIpKSBlbHNlIGRlZmF1bHQKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHJl"
    "dHVybiBkZWZhdWx0CgogICAgICAgIGZvciBtaWQsIHQgaW4gbGlzdCh0YXNrX2RpY3QuaXRlbXMoKSk6CiAgICAgICAgICAgIHRy"
    "eToKICAgICAgICAgICAgICAgIGxzdCA9IGdldGF0dHIodCwgImxpc3RlbmVyIiwgTm9uZSkKICAgICAgICAgICAgICAgIG91dC5h"
    "cHBlbmQoCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAibWlkIjogbWlkLAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAiZ2lkIjogc2FmZShsYW1iZGE6IHQuZ2lkKCksICIiKSwKICAgICAgICAgICAgICAgICAgICAgICAgIm5h"
    "bWUiOiBzdHIoc2FmZShsYW1iZGE6IHQubmFtZSgpLCAidGFzayIpKVs6OTBdLAogICAgICAgICAgICAgICAgICAgICAgICAidWlk"
    "IjogaW50KHNhZmUobGFtYmRhOiBsc3QudXNlcl9pZCwgMCkgb3IgMCksCiAgICAgICAgICAgICAgICAgICAgICAgICJ0YWciOiBz"
    "dHIoc2FmZShsYW1iZGE6IGxzdC50YWcsICIiKSBvciAiIiksCiAgICAgICAgICAgICAgICAgICAgICAgICJzaXplIjogaW50KHNh"
    "ZmUobGFtYmRhOiB0LnNpemUoKSwgMCkgb3IgMCksCiAgICAgICAgICAgICAgICAgICAgICAgICJwcm9jIjogaW50KHNhZmUobGFt"
    "YmRhOiB0LnByb2Nlc3NlZF9ieXRlcygpLCAwKSBvciAwKSwKICAgICAgICAgICAgICAgICAgICAgICAgInBjdCI6IHNhZmUobGFt"
    "YmRhOiBmbG9hdChzdHIodC5wcm9ncmVzcygpKS5yc3RyaXAoIiUiKSksIDAuMCksCiAgICAgICAgICAgICAgICAgICAgICAgICJz"
    "cGVlZCI6IHN0cihzYWZlKGxhbWJkYTogdC5zcGVlZCgpLCAiIikpLAogICAgICAgICAgICAgICAgICAgICAgICAiZXRhIjogc3Ry"
    "KHNhZmUobGFtYmRhOiB0LmV0YSgpLCAiIikpLAogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICkKICAgICAg"
    "ICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgb3V0LnNvcnQoa2V5PWxhbWJk"
    "YSB4OiAteFsic2l6ZSJdKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gb3V0CgoKYXN5bmMg"
    "ZGVmIF9ib3RfaW5mbygpOgogICAgb3V0ID0geyJuYW1lIjogIiIsICJ1bmFtZSI6ICIifQogICAgdHJ5OgogICAgICAgIGZyb20g"
    "Li4uY29yZS50Z19jbGllbnQgaW1wb3J0IFRnQ2xpZW50CgogICAgICAgIG1lID0gYXdhaXQgVGdDbGllbnQuYm90LmdldF9tZSgp"
    "CiAgICAgICAgb3V0WyJuYW1lIl0gPSBtZS5maXJzdF9uYW1lIG9yICIiCiAgICAgICAgb3V0WyJ1bmFtZSJdID0gbWUudXNlcm5h"
    "bWUgb3IgIiIKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIG91dAoKCmFzeW5jIGRlZiBfc3Rh"
    "dGUoKToKICAgIHJvd3MgPSBhd2FpdCB0b2RheV9yb3dzKCkKICAgIHVzZWRfYnkgPSB7clsidXNlcl9pZCJdOiBpbnQoci5nZXQo"
    "InVzZWQiKSBvciAwKSBmb3IgciBpbiByb3dzfQogICAgZG9jcyA9IGF3YWl0IGFsbF91c2VycygpCiAgICB1c2VycyA9IFtdCiAg"
    "ICBmb3IgZCBpbiBkb2NzOgogICAgICAgIHRyeToKICAgICAgICAgICAgdWlkID0gaW50KGRbIl9pZCJdKQogICAgICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1aWQpCiAg"
    "ICAgICAgdXNlZCA9IHVzZWRfYnkuZ2V0KHVpZCwgMCkKICAgICAgICByZXMgPSBhd2FpdCByZXNlcnZlZF90b2RheSh1aWQpCiAg"
    "ICAgICAgdXNlcnMuYXBwZW5kKAogICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAidWlkIjogdWlkLAogICAgICAgICAgICAg"
    "ICAgInVuYW1lIjogZC5nZXQoInVuYW1lIikgb3IgIiIsCiAgICAgICAgICAgICAgICAibmFtZSI6IGQuZ2V0KCJuYW1lIikgb3Ig"
    "IiIsCiAgICAgICAgICAgICAgICAiY2FwX2diIjogZC5nZXQoImNhcF9nYiIpLAogICAgICAgICAgICAgICAgImNhcCI6IGNhcCwK"
    "ICAgICAgICAgICAgICAgICJ1c2VkIjogdXNlZCwKICAgICAgICAgICAgICAgICJyZXNlcnZlZCI6IHJlcywKICAgICAgICAgICAg"
    "ICAgICJwY3QiOiBtaW4oMTAwLjAsIHJvdW5kKDEwMC4wICogKHVzZWQgKyByZXMpIC8gY2FwLCAxKSkgaWYgY2FwIGVsc2UgMC4w"
    "LAogICAgICAgICAgICAgICAgImJhbm5lZCI6IGQuZ2V0KCJjYXBfZ2IiKSA9PSAwLAogICAgICAgICAgICAgICAgIm11c2ljX21h"
    "eCI6IGQuZ2V0KCJtdXNpY19tYXgiKSwKICAgICAgICAgICAgICAgICJ0YXNrcyI6IGludChkLmdldCgidGFza3MiKSBvciAwKSwK"
    "ICAgICAgICAgICAgICAgICJ0b3RhbCI6IGludChkLmdldCgidG90YWxfdXNlZCIpIG9yIDApLAogICAgICAgICAgICB9CiAgICAg"
    "ICAgKQogICAgdXNlcnMuc29ydChrZXk9bGFtYmRhIHU6IC0odVsidXNlZCJdICsgdVsicmVzZXJ2ZWQiXSkpCiAgICByZXR1cm4g"
    "ewogICAgICAgICJvayI6IFRydWUsCiAgICAgICAgImRheSI6IF9kYXlfaXN0KCksCiAgICAgICAgImJvdCI6IGF3YWl0IF9ib3Rf"
    "aW5mbygpLAogICAgICAgICJ1c2VycyI6IHVzZXJzLAogICAgICAgICJ0YXNrcyI6IF90YXNrc19zbmFwc2hvdCgpLAogICAgICAg"
    "ICJnbG9iYWxfY2FwX2diIjogYXdhaXQgX2dldF9nbG9iYWxfY2FwX2diKCksCiAgICAgICAgImdsb2JhbF9tdXNpYyI6IGF3YWl0"
    "IF9nZXRfZ2xvYmFsX211c2ljKCksCiAgICAgICAgInRvdGFscyI6IHsKICAgICAgICAgICAgInVzZWQiOiBzdW0odVsidXNlZCJd"
    "IGZvciB1IGluIHVzZXJzKSwKICAgICAgICAgICAgInJlc2VydmVkIjogc3VtKHVbInJlc2VydmVkIl0gZm9yIHUgaW4gdXNlcnMp"
    "LAogICAgICAgICAgICAidXNlcnMiOiBsZW4odXNlcnMpLAogICAgICAgICAgICAidGFza3MiOiBsZW4oX3Rhc2tzX3NuYXBzaG90"
    "KCkpLAogICAgICAgIH0sCiAgICB9CgoKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIAKIyBBY3Rpb25zCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIF9raWxsX3Rhc2sobWlkKToKICAgIGZyb20gLi4uIGltcG9y"
    "dCB0YXNrX2RpY3QKCiAgICB0cnk6CiAgICAgICAgdCA9IHRhc2tfZGljdC5nZXQoaW50KG1pZCkpCiAgICAgICAgaWYgdCBpcyBO"
    "b25lOgogICAgICAgICAgICByZXR1cm4gRmFsc2UsICJ0YXNrIG5vdCBmb3VuZCIKICAgICAgICBvYmogPSB0LnRhc2soKQogICAg"
    "ICAgIGF3YWl0IG9iai5jYW5jZWxfdGFzaygpCiAgICAgICAgcmV0dXJuIFRydWUsICJjYW5jZWxsZWQiCiAgICBleGNlcHQgRXhj"
    "ZXB0aW9uIGFzIGU6CiAgICAgICAgcmV0dXJuIEZhbHNlLCBzdHIoZSlbOjEyMF0KCgphc3luYyBkZWYgX2RlZHVjdF9jYXAodWlk"
    "LCBnYik6CiAgICAiIiJMb3dlciBhIHVzZXIncyBjYXAgYnkgR0IgKGZsb29yIDAgPSBmdWxseSBibG9ja2VkKSDigJQgc2FtZSBh"
    "cwogICAgVGVsZWdyYW0gL2RlZHVjdGNhcC4iIiIKICAgIHRyeToKICAgICAgICBhbXQgPSBmbG9hdChnYikKICAgICAgICB1ZG9j"
    "ID0gYXdhaXQgZ2V0X3VzZXJfZG9jKHVpZCkKICAgICAgICBjdXIgPSB1ZG9jLmdldCgiY2FwX2diIikKICAgICAgICBiYXNlID0g"
    "ZmxvYXQoY3VyKSBpZiBjdXIgaXMgbm90IE5vbmUgZWxzZSBhd2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKQogICAgICAgIG5ld19j"
    "YXAgPSBtYXgoYmFzZSAtIGFtdCwgMC4wKQogICAgICAgIGF3YWl0IHNldF91c2VyX2NhcCh1aWQsIG5ld19jYXApCiAgICAgICAg"
    "aWYgbmV3X2NhcCA8PSAwOgogICAgICAgICAgICByZXR1cm4gVHJ1ZSwgZiJjYXAgZm9yIHt1aWR9IOKGkiAwIEdCIChibG9ja2Vk"
    "KSIKICAgICAgICByZXR1cm4gVHJ1ZSwgZiJjYXAgZm9yIHt1aWR9IOKGkiB7X2ZtdF9nYihuZXdfY2FwKX0iCiAgICBleGNlcHQg"
    "RXhjZXB0aW9uIGFzIGU6CiAgICAgICAgcmV0dXJuIEZhbHNlLCBzdHIoZSlbOjEyMF0KCgphc3luYyBkZWYgYnVpbGRfcmVwb3J0"
    "KCk6CiAgICAiIiJEYWlseSB1c2FnZSB0ZXh0IGZvciB0aGUgbG9nIGNoYXQuIiIiCiAgICByb3dzID0gYXdhaXQgdG9kYXlfcm93"
    "cygpCiAgICBkb2NzID0ge2ludChkWyJfaWQiXSk6IGQgZm9yIGQgaW4gYXdhaXQgYWxsX3VzZXJzKCl9CiAgICBsaW5lcyA9IFtm"
    "IvCfk4ogPGI+V1pGSVggZGFpbHkgcmVwb3J0IOKAlCB7X2RheV9pc3QoKX08L2I+IiwgIuKUgiJdCiAgICB0b3RhbCA9IDAKICAg"
    "IGZvciByIGluIHJvd3NbOjI1XToKICAgICAgICB1aWQgPSByWyJ1c2VyX2lkIl0KICAgICAgICBkID0gZG9jcy5nZXQodWlkLCB7"
    "fSkKICAgICAgICB0YWcgPSBkLmdldCgidW5hbWUiKSBvciBkLmdldCgibmFtZSIpIG9yIHN0cih1aWQpCiAgICAgICAgY2FwX2di"
    "ID0gZC5nZXQoImNhcF9nYiIpCiAgICAgICAgY2FwX3R4dCA9IGYie19mbXRfZ2IoY2FwX2diKX0iIGlmIGNhcF9nYiBlbHNlICJk"
    "ZWZhdWx0IgogICAgICAgIGxpbmVzLmFwcGVuZChmIuKUoCA8Yj57dGFnfTwvYj4g4oaSIHtfbmljZV9zaXplKHJbJ3VzZWQnXSl9"
    "IC8ge2NhcF90eHR9IikKICAgICAgICB0b3RhbCArPSByLmdldCgidXNlZCIpIG9yIDAKICAgIGlmIG5vdCByb3dzOgogICAgICAg"
    "IGxpbmVzLmFwcGVuZCgi4pSWIG5vIHVzYWdlIHRvZGF5IikKICAgIGVsc2U6CiAgICAgICAgbGluZXNbMV0gPSBmIuKUgiB0b3Rh"
    "bCA8Yj57X25pY2Vfc2l6ZSh0b3RhbCl9PC9iPiDCtyB7bGVuKHJvd3MpfSB1c2VyKHMpIgogICAgICAgIGxpbmVzLmFwcGVuZCgi"
    "4pSWIHJlc2V0cyBhdCAwMDowMCBJU1QiKQogICAgcmV0dXJuICJcbiIuam9pbihsaW5lcylbOlJFUE9SVF9NQVhfQ0hBUlNdCgoK"
    "YXN5bmMgZGVmIHNlbmRfcmVwb3J0KCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi5jb3JlLmNvbmZpZ19tYW5hZ2VyIGltcG9y"
    "dCBDb25maWcKICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVudAoKICAgICAgICBjaGF0ID0gc3Ry"
    "KGdldGF0dHIoQ29uZmlnLCAiTE9HX0NIQVQiLCAiIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICBpZiBub3QgY2hhdDoKICAgICAg"
    "ICAgICAgIyBMT0dfQ0hBVCBub3Qgc2V0IC0+IGRlbGl2ZXIgdG8gdGhlIG93bmVyJ3MgRE0gaW5zdGVhZAogICAgICAgICAgICBj"
    "aGF0ID0gc3RyKGdldGF0dHIoQ29uZmlnLCAiT1dORVJfSUQiLCAiIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICAgICAgaWYgbm90"
    "IGNoYXQ6CiAgICAgICAgICAgICAgICByZXR1cm4gRmFsc2UsICJMT0dfQ0hBVCBhbmQgT1dORVJfSUQgbm90IHNldCIKICAgICAg"
    "ICB0ZXh0ID0gYXdhaXQgYnVpbGRfcmVwb3J0KCkKICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Quc2VuZF9tZXNzYWdlKAogICAg"
    "ICAgICAgICBjaGF0X2lkPV9jaGF0X2ludChjaGF0KSwgdGV4dD10ZXh0LCBkaXNhYmxlX3dlYl9wYWdlX3ByZXZpZXc9VHJ1ZQog"
    "ICAgICAgICkKICAgICAgICByZXR1cm4gVHJ1ZSwgInJlcG9ydCBzZW50IgogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAg"
    "ICAgIHJldHVybiBGYWxzZSwgc3RyKGUpWzoxMjBdCgoKYXN5bmMgZGVmIGRhaWx5X3JlcG9ydF9sb29wKCk6CiAgICAiIiJTZW5k"
    "IHRoZSB1c2FnZSByZXBvcnQgdG8gdGhlIGxvZyBjaGF0IGF0IDIzOjU3IElTVCBldmVyeSBkYXkuIiIiCiAgICB3aGlsZSBUcnVl"
    "OgogICAgICAgIHRyeToKICAgICAgICAgICAgbm93ID0gZGF0ZXRpbWUubm93KElTVCkKICAgICAgICAgICAgdGFyZ2V0ID0gbm93"
    "LnJlcGxhY2UoaG91cj0yMywgbWludXRlPTU3LCBzZWNvbmQ9MCwgbWljcm9zZWNvbmQ9MCkKICAgICAgICAgICAgaWYgdGFyZ2V0"
    "IDw9IG5vdzoKICAgICAgICAgICAgICAgIHRhcmdldCArPSB0aW1lZGVsdGEoZGF5cz0xKQogICAgICAgICAgICBhd2FpdCBhaW9z"
    "bGVlcCgodGFyZ2V0IC0gbm93KS50b3RhbF9zZWNvbmRzKCkgKyA1KQogICAgICAgICAgICBvaywgbXNnID0gYXdhaXQgc2VuZF9y"
    "ZXBvcnQoKQogICAgICAgICAgICBpZiBub3Qgb2s6CiAgICAgICAgICAgICAgICBmcm9tIC4uLiBpbXBvcnQgTE9HR0VSCgogICAg"
    "ICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoZiJXWkZJWCBkYWlseSByZXBvcnQgZmFpbGVkOiB7bXNnfSIpCiAgICAgICAgZXhj"
    "ZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgYXdhaXQgYWlvc2xlZXAoMzYwMCkKCgojIGNvbW1hbmQgZGVzY3JpcHRpb25zIGZv"
    "ciB0aGUgVGVsZWdyYW0gIi8iIG1lbnUgKHYxNS45KQpfQ01EX0RFU0MgPSB7CiAgICAic3RhcnQiOiAiU3RhcnQgdGhlIGJvdCIs"
    "ICJoZWxwIjogIkNvbW1hbmQgaGVscCIsCiAgICAibG9naW4iOiAiTG9naW4gdG9rZW4iLCAicGluZyI6ICJCb3QgbGF0ZW5jeSIs"
    "CiAgICAibWlycm9yIjogIk1pcnJvciBhIGxpbmsgdG8gY2xvdWQiLCAibSI6ICJNaXJyb3IgYSBsaW5rIChzaG9ydCkiLAogICAg"
    "ImxlZWNoIjogIkRvd25sb2FkICYgdXBsb2FkIHRvIFRlbGVncmFtIiwgImwiOiAiTGVlY2ggYSBsaW5rIChzaG9ydCkiLAogICAg"
    "InFibWlycm9yIjogIk1pcnJvciB2aWEgcUJpdHRvcnJlbnQiLCAicW0iOiAicUJpdCBtaXJyb3IgKHNob3J0KSIsCiAgICAicWJs"
    "ZWVjaCI6ICJMZWVjaCB2aWEgcUJpdHRvcnJlbnQiLCAicWwiOiAicUJpdCBsZWVjaCAoc2hvcnQpIiwKICAgICJ5dGRsIjogIllU"
    "LURMUCBtaXJyb3IiLCAieSI6ICJZVC1ETFAgbWlycm9yIChzaG9ydCkiLAogICAgInl0ZGxsZWVjaCI6ICJZVC1ETFAgbGVlY2gi"
    "LCAieWwiOiAiWVQtRExQIGxlZWNoIChzaG9ydCkiLAogICAgImpkbWlycm9yIjogIkpEb3dubG9hZGVyIG1pcnJvciIsICJqbSI6"
    "ICJKRG93bmxvYWRlciBtaXJyb3IgKHNob3J0KSIsCiAgICAiamRsZWVjaCI6ICJKRG93bmxvYWRlciBsZWVjaCIsICJqbCI6ICJK"
    "RG93bmxvYWRlciBsZWVjaCAoc2hvcnQpIiwKICAgICJuemJtaXJyb3IiOiAiTlpCIG1pcnJvciIsICJubSI6ICJOWkIgbWlycm9y"
    "IChzaG9ydCkiLAogICAgIm56YmxlZWNoIjogIk5aQiBsZWVjaCIsICJubCI6ICJOWkIgbGVlY2ggKHNob3J0KSIsCiAgICAic2Vl"
    "ZHJsaW5rIjogIlNlZWRyIGxpbmsgdHJhbnNmZXIiLCAic2xpbmsiOiAiU2VlZHIgbGluayAoc2hvcnQpIiwKICAgICJzcmxpbmsi"
    "OiAiU2VlZHIgbGluayAoc2hvcnQpIiwKICAgICJjbG9uZSI6ICJDbG9uZSBjbG91ZCB0cmFuc2ZlcnMiLCAiY2wiOiAiQ2xvbmUg"
    "KHNob3J0KSIsCiAgICAiY291bnQiOiAiQ291bnQgY2xvdWQgZmlsZXMvZm9sZGVycyIsICJkZWwiOiAiRGVsZXRlIGNsb3VkIGZp"
    "bGVzIiwKICAgICJsaXN0IjogIkxpc3QgY2xvdWQgZmlsZXMiLCAic2VhcmNoIjogIlNlYXJjaCBmaWxlcyIsCiAgICAidXNlcnMi"
    "OiAiQXV0aG9yaXplZCB1c2VycyBsaXN0IiwKICAgICJjYW5jZWwiOiAiQ2FuY2VsIGEgdGFzayIsICJjIjogIkNhbmNlbCBhIHRh"
    "c2sgKHNob3J0KSIsCiAgICAiY2FuY2VsYWxsIjogIkNhbmNlbCB0YXNrcyBpbiBidWxrIiwgImNhbGwiOiAiQ2FuY2VsIGFsbCAo"
    "c2hvcnQpIiwKICAgICJmb3JjZXN0YXJ0IjogIkZvcmNlIHN0YXJ0IGEgcXVldWVkIHRhc2siLCAiZnMiOiAiRm9yY2Ugc3RhcnQg"
    "KHNob3J0KSIsCiAgICAic3RhdHVzIjogIkFjdGl2ZSB0YXNrIHN0YXR1cyIsICJzIjogIlN0YXR1cyAoc2hvcnQpIiwKICAgICJz"
    "dGF0dXNhbGwiOiAiU3RhdHVzIG9mIGFsbCB1c2VycyIsCiAgICAic3RyZWFtIjogIlN0cmVhbSBsaW5rIGZvciBhIGZpbGUiLCAi"
    "c2wiOiAiU3RyZWFtIGxpbmsgKHNob3J0KSIsCiAgICAicmVzdGFydCI6ICJSZXN0YXJ0IHRoZSBib3QiLCAiciI6ICJSZXN0YXJ0"
    "IChzaG9ydCkiLAogICAgInJlc3RhcnRhbGwiOiAiUmVzdGFydCBhbGwgYm90cyIsICJyZXN0YXJ0c2VzIjogIlJlc3RhcnQgc2Vz"
    "c2lvbnMiLAogICAgImJyb2FkY2FzdCI6ICJCcm9hZGNhc3QgYSBtZXNzYWdlIiwgImJjIjogIkJyb2FkY2FzdCAoc2hvcnQpIiwK"
    "ICAgICJzdGF0cyI6ICJTZXJ2ZXIgc3RhdHMiLCAic3QiOiAiU3RhdHMgKHNob3J0KSIsCiAgICAibG9nIjogIkJvdCBsb2cgZmls"
    "ZSIsICJzaGVsbCI6ICJSdW4gYSBzaGVsbCBjb21tYW5kIiwKICAgICJhZXhlYyI6ICJSdW4gYXN5bmMgcHl0aG9uIiwgImV4ZWMi"
    "OiAiUnVuIHB5dGhvbiIsCiAgICAiY2xlYXJsb2NhbHMiOiAiQ2xlYXIgc3RvcmVkIHZhcnMiLAogICAgInJzcyI6ICJSU1MgZmVl"
    "ZCBtYW5hZ2VyIiwKICAgICJhZGRpbWFnZSI6ICJBZGQgYSBjdXN0b20gaW1hZ2UiLCAiYWkiOiAiQWRkIGltYWdlIChzaG9ydCki"
    "LAogICAgImltYWdlcyI6ICJMaXN0IGN1c3RvbSBpbWFnZXMiLCAiaW1nIjogIkltYWdlcyAoc2hvcnQpIiwKICAgICJhdXRob3Jp"
    "emUiOiAiQXV0aG9yaXplIGEgdXNlciIsICJhIjogIkF1dGhvcml6ZSAoc2hvcnQpIiwKICAgICJ1bmF1dGhvcml6ZSI6ICJVbmF1"
    "dGhvcml6ZSBhIHVzZXIiLCAidWEiOiAiVW5hdXRob3JpemUgKHNob3J0KSIsCiAgICAiYWRkc3VkbyI6ICJHcmFudCBzdWRvIGFj"
    "Y2VzcyIsICJhcyI6ICJBZGQgc3VkbyAoc2hvcnQpIiwKICAgICJybXN1ZG8iOiAiUmV2b2tlIHN1ZG8gYWNjZXNzIiwgInJzIjog"
    "IlJtIHN1ZG8gKHNob3J0KSIsCiAgICAiYmxhY2tsaXN0IjogIkJsYWNrbGlzdCBhIHVzZXIiLCAiYmwiOiAiQmxhY2tsaXN0IChz"
    "aG9ydCkiLAogICAgInJtYmxhY2tsaXN0IjogIlVuLWJsYWNrbGlzdCBhIHVzZXIiLCAicmJsIjogIlJtIGJsYWNrbGlzdCAoc2hv"
    "cnQpIiwKICAgICJic2V0dGluZyI6ICJCb3Qgc2V0dGluZ3MiLCAiYnMiOiAiQm90IHNldHRpbmdzIChzaG9ydCkiLAogICAgInVz"
    "ZXR0aW5nIjogIlVzZXIgc2V0dGluZ3MiLCAidXMiOiAiVXNlciBzZXR0aW5ncyAoc2hvcnQpIiwKICAgICJzZWxlY3QiOiAiU2Vs"
    "ZWN0IHRvcnJlbnQgZmlsZXMiLCAic2VsIjogIlNlbGVjdCAoc2hvcnQpIiwKICAgICJjYXRlZ29yeSI6ICJDYXRlZ29yeSBzZWxl"
    "Y3RvciIsICJjdHNlbCI6ICJDYXRlZ29yeSAoc2hvcnQpIiwKICAgICJnZGNsZWFuIjogIkNsZWFuIGNsb3VkIGRyaXZlIiwgImdk"
    "YyI6ICJHRENsZWFuIChzaG9ydCkiLAogICAgInBsdWdpbnMiOiAiTWFuYWdlIHBsdWdpbnMiLAogICAgIm1lbW9yeSI6ICJTaG93"
    "IGJvdCBtZW1vcnkiLCAibWVtIjogIk1lbW9yeSAoc2hvcnQpIiwKICAgICJ1cGhvc3RlciI6ICJVcGxvYWQgZnJvbSBVUkwiLCAi"
    "dXAiOiAiVXBsb2FkIChzaG9ydCkiLAogICAgImZpbmQiOiAiU2VhcmNoIHlvdXIgZG93bmxvYWRzIiwgInVzYWdlIjogIllvdXIg"
    "YmFuZHdpZHRoIHVzYWdlIiwKfQpfT1dORVJfREVTQyA9IHsKICAgICJxdXNlcnMiOiAiVXNlcnMgKyB1c2FnZSBvdmVydmlldyIs"
    "ICJzZXRjYXAiOiAiU2V0IGEgdXNlciBjYXAiLAogICAgImFkZGNhcCI6ICJSYWlzZSBhIHVzZXIgY2FwIiwgImRlZHVjdGNhcCI6"
    "ICJMb3dlciBhIHVzZXIgY2FwIiwKICAgICJkZWx1c2VyIjogIlJlbW92ZSBhIHVzZXIiLCAicmVzZXRjYXAiOiAiUmVzZXQgdG9k"
    "YXkncyB1c2FnZSIsCiAgICAiYm90Y2FwIjogIkRlZmF1bHQgY2FwIGZvciBhbGwiLCAiZGJzdGF0cyI6ICJNb25nb0RCIHNpemVz"
    "IiwKICAgICJkYmNsZWFuIjogIkNsZWFuIGV4cGlyZWQgREIgcm93cyIsICJ3emZpeGRpYWciOiAiUXVvdGEgZGlhZ25vc3RpY3Mi"
    "LAogICAgImFkbWlucGFzcyI6ICJXZWIgZGFzaGJvYXJkIHBhc3N3b3JkIiwgImFsbG93IjogIlVuYmFuIGEgZGFzaGJvYXJkIElQ"
    "IiwKICAgICJiYW5zIjogIkxpc3QgZGFzaGJvYXJkIGJhbnMiLCAibG9ja2Rhc2giOiAiTG9jay91bmxvY2sgZGFzaGJvYXJkIiwK"
    "fQoKCmFzeW5jIGRlZiBzZXRfYm90X2NvbW1hbmRzKCk6CiAgICAiIiJQdWJsaXNoIHRoZSBmdWxsIGNvbW1hbmQgbGlzdCB0byB0"
    "aGUgVGVsZWdyYW0gIi8iIG1lbnUg4oCUIGRlZmF1bHQKICAgIHNjb3BlIGZvciB1c2Vycywgb3duZXIgc2NvcGUgd2l0aCBldmVy"
    "eSBhZG1pbiBjb21tYW5kLiIiIgogICAgdHJ5OgogICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgc2xlZXAKCiAgICAgICAgYXdh"
    "aXQgc2xlZXAoOTApICAjIHdhaXQgZm9yIHRoZSBib3QgdG8gYmUgdXAKICAgICAgICBmcm9tIHB5cm9ncmFtLnR5cGVzIGltcG9y"
    "dCBCb3RDb21tYW5kLCBCb3RDb21tYW5kU2NvcGVDaGF0LCBCb3RDb21tYW5kU2NvcGVEZWZhdWx0CgogICAgICAgIGZyb20gLi4u"
    "IGltcG9ydCBMT0dHRVIKICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVudCwgQ29uZmlnCgogICAg"
    "ICAgICMgZ2F0aGVyIGV2ZXJ5IGNvbW1hbmQgbmFtZSBrbm93biB0byB0aGUgYm90CiAgICAgICAgdHJ5OgogICAgICAgICAgICBm"
    "cm9tIC4udGVsZWdyYW1faGVscGVyLmJvdF9jb21tYW5kcyBpbXBvcnQgQm90Q29tbWFuZHMKCiAgICAgICAgICAgIGFsbF9uYW1l"
    "cyA9IHNldCgpCiAgICAgICAgICAgIGZvciBfdiBpbiBCb3RDb21tYW5kcy5nZXRfY29tbWFuZHMoKS52YWx1ZXMoKToKICAgICAg"
    "ICAgICAgICAgIG5hbWVzID0gX3YgaWYgaXNpbnN0YW5jZShfdiwgbGlzdCkgZWxzZSBbX3ZdCiAgICAgICAgICAgICAgICBhbGxf"
    "bmFtZXMudXBkYXRlKG5hbWVzKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIGFsbF9uYW1lcyA9IHNldCgp"
    "CiAgICAgICAgYWxsX25hbWVzLnVwZGF0ZShfT1dORVJfREVTQykKICAgICAgICBhbGxfbmFtZXMudXBkYXRlKF9DTURfREVTQykK"
    "CiAgICAgICAgdXNlcl9jbWRzLCBvd25lcl9jbWRzID0gW10sIFtdCiAgICAgICAgZm9yIG5hbWUgaW4gc29ydGVkKGFsbF9uYW1l"
    "cyk6CiAgICAgICAgICAgIGlmIG5hbWUgaW4gKCJzaGVsbCIsICJhZXhlYyIsICJleGVjIiwgImNsZWFybG9jYWxzIik6CiAgICAg"
    "ICAgICAgICAgICBjb250aW51ZSAgIyBrZWVwIHRoZSBkYW5nZXJvdXMgb25lcyBvdXQgb2YgdGhlIG1lbnUKICAgICAgICAgICAg"
    "ZGVzYyA9IF9DTURfREVTQy5nZXQobmFtZSkgb3IgX09XTkVSX0RFU0MuZ2V0KG5hbWUpIG9yICJXWk1MLVggY29tbWFuZCIKICAg"
    "ICAgICAgICAgb3duZXJfY21kcy5hcHBlbmQoQm90Q29tbWFuZChjb21tYW5kPW5hbWUsIGRlc2NyaXB0aW9uPWRlc2MpKQogICAg"
    "ICAgICAgICBpZiBuYW1lIG5vdCBpbiBfT1dORVJfREVTQyBhbmQgbm90IG5hbWUuc3RhcnRzd2l0aCgid3oiKToKICAgICAgICAg"
    "ICAgICAgIHVzZXJfY21kcy5hcHBlbmQoQm90Q29tbWFuZChjb21tYW5kPW5hbWUsIGRlc2NyaXB0aW9uPWRlc2MpKQoKICAgICAg"
    "ICBhc3luYyBkZWYgX3NldF9jbWRzX2h0dHAoY21kcywgc2NvcGU9Tm9uZSk6CiAgICAgICAgICAgICIiInNldE15Q29tbWFuZHMg"
    "dmlhIHRoZSByYXcgQm90IEFQSSDigJQgc29tZSBjbGllbnQgZm9ya3MKICAgICAgICAgICAgKHd6Z3JhbSkgbGFjayBDbGllbnQu"
    "c2V0X215X2NvbW1hbmRzIGVudGlyZWx5LiIiIgogICAgICAgICAgICBpbXBvcnQganNvbiBhcyBfanNvbgoKICAgICAgICAgICAg"
    "ZnJvbSBhaW9odHRwIGltcG9ydCBDbGllbnRTZXNzaW9uCgogICAgICAgICAgICB0b2tlbiA9IHN0cihnZXRhdHRyKENvbmZpZywg"
    "IkJPVF9UT0tFTiIsICIiKSBvciAiIikuc3RyaXAoKQogICAgICAgICAgICBpZiBub3QgdG9rZW46CiAgICAgICAgICAgICAgICBy"
    "YWlzZSBSdW50aW1lRXJyb3IoIkJPVF9UT0tFTiBub3Qgc2V0IikKICAgICAgICAgICAgYm9keSA9IHsKICAgICAgICAgICAgICAg"
    "ICJjb21tYW5kcyI6IFsKICAgICAgICAgICAgICAgICAgICB7ImNvbW1hbmQiOiBjLmNvbW1hbmQsICJkZXNjcmlwdGlvbiI6IGMu"
    "ZGVzY3JpcHRpb259CiAgICAgICAgICAgICAgICAgICAgZm9yIGMgaW4gY21kcwogICAgICAgICAgICAgICAgXQogICAgICAgICAg"
    "ICB9CiAgICAgICAgICAgIGlmIHNjb3BlIGlzIG5vdCBOb25lOgogICAgICAgICAgICAgICAgYm9keVsic2NvcGUiXSA9IHNjb3Bl"
    "CiAgICAgICAgICAgIGFzeW5jIHdpdGggQ2xpZW50U2Vzc2lvbigpIGFzIF9zOgogICAgICAgICAgICAgICAgYXN5bmMgd2l0aCBf"
    "cy5wb3N0KAogICAgICAgICAgICAgICAgICAgIGYiaHR0cHM6Ly9hcGkudGVsZWdyYW0ub3JnL2JvdHt0b2tlbn0vc2V0TXlDb21t"
    "YW5kcyIsCiAgICAgICAgICAgICAgICAgICAganNvbj1ib2R5LAogICAgICAgICAgICAgICAgKSBhcyBfcjoKICAgICAgICAgICAg"
    "ICAgICAgICBfaiA9IGF3YWl0IF9yLmpzb24oY29udGVudF90eXBlPU5vbmUpCiAgICAgICAgICAgICAgICAgICAgaWYgbm90IF9q"
    "LmdldCgib2siKToKICAgICAgICAgICAgICAgICAgICAgICAgcmFpc2UgUnVudGltZUVycm9yKHN0cihfai5nZXQoImRlc2NyaXB0"
    "aW9uIikpWzoxMjBdKQoKICAgICAgICB0cnk6CiAgICAgICAgICAgIGF3YWl0IFRnQ2xpZW50LmJvdC5zZXRfbXlfY29tbWFuZHMo"
    "CiAgICAgICAgICAgICAgICB1c2VyX2NtZHMgb3IgW0JvdENvbW1hbmQoInN0YXJ0IiwgIlN0YXJ0IHRoZSBib3QiKV0KICAgICAg"
    "ICAgICAgKQogICAgICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Quc2V0X215X2NvbW1hbmRzKAogICAgICAgICAgICAgICAgb3du"
    "ZXJfY21kcywKICAgICAgICAgICAgICAgIHNjb3BlPUJvdENvbW1hbmRTY29wZUNoYXQoY2hhdF9pZD1Db25maWcuT1dORVJfSUQp"
    "LAogICAgICAgICAgICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBfZToKICAgICAgICAgICAgIyBjbGllbnQgbWV0aG9k"
    "IG1pc3NpbmcvYnJva2VuIC0+IHJhdyBCb3QgQVBJCiAgICAgICAgICAgIGF3YWl0IF9zZXRfY21kc19odHRwKAogICAgICAgICAg"
    "ICAgICAgdXNlcl9jbWRzIG9yIFtCb3RDb21tYW5kKCJzdGFydCIsICJTdGFydCB0aGUgYm90IildCiAgICAgICAgICAgICkKICAg"
    "ICAgICAgICAgYXdhaXQgX3NldF9jbWRzX2h0dHAoCiAgICAgICAgICAgICAgICBvd25lcl9jbWRzLCBzY29wZT17InR5cGUiOiAi"
    "Y2hhdCIsICJjaGF0X2lkIjogQ29uZmlnLk9XTkVSX0lEfQogICAgICAgICAgICApCiAgICAgICAgTE9HR0VSLmluZm8oCiAgICAg"
    "ICAgICAgIGYiV1pGSVg6IGNvbW1hbmQgbWVudSBzZXQgKHtsZW4odXNlcl9jbWRzKX0gdXNlciwgIgogICAgICAgICAgICBmInts"
    "ZW4ob3duZXJfY21kcyl9IG93bmVyIGNvbW1hbmRzKSIKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAg"
    "ICAgdHJ5OgogICAgICAgICAgICBmcm9tIC4uLiBpbXBvcnQgTE9HR0VSCgogICAgICAgICAgICBMT0dHRVIud2FybmluZyhmIlda"
    "RklYIHNldF9ib3RfY29tbWFuZHMgZmFpbGVkOiB7ZX0iKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIHBh"
    "c3MKCgphc3luYyBkZWYgX2xvZ19zdGFydHVwKCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAucjFfY29yZSBpbXBvcnQgYWRtaW5f"
    "bG9nCgogICAgICAgIGF3YWl0IGFkbWluX2xvZygKICAgICAgICAgICAgIvCfn6IgPGI+Qm90IHN0YXJ0ZWQ8L2I+IiwKICAgICAg"
    "ICAgICAgIuKUjyBXWk1MLVggKyBXWkZJWCBhcmUgdXBcbuKUliBTZXNzaW9uIGxvY2sgYW5kIHdhdGNoZG9nIGFjdGl2ZSIsCiAg"
    "ICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKZGVmIHN0YXJ0X2xvb3BzKCk6CiAgICB0cnk6CiAg"
    "ICAgICAgZnJvbSAuLi4gaW1wb3J0IGJvdF9sb29wCgogICAgICAgIGJvdF9sb29wLmNyZWF0ZV90YXNrKGRhaWx5X3JlcG9ydF9s"
    "b29wKCkpCiAgICAgICAgYm90X2xvb3AuY3JlYXRlX3Rhc2soc2V0X2JvdF9jb21tYW5kcygpKQogICAgICAgIGJvdF9sb29wLmNy"
    "ZWF0ZV90YXNrKF9sb2dfc3RhcnR1cCgpKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKIyDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKIyBIVFRQIGhhbmRsZXJz"
    "IChyZWdpc3RlcmVkIG9uIHRoZSBzdHJlYW0gc2VydmVyJ3MgYWlvaHR0cCBhcHApCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIHd6YWRtaW5fcGFnZShyZXF1"
    "ZXN0KToKICAgIGlwID0gX25vcm1faXAoX2NsaWVudF9pcChyZXF1ZXN0KSkKICAgIGlmIG5vdCBfdWFfb2socmVxdWVzdCkgb3Ig"
    "YXdhaXQgX2lzX2Jhbm5lZChpcCk6CiAgICAgICAgcmV0dXJuIHdlYi5SZXNwb25zZShzdGF0dXM9NDAzLCB0ZXh0PSJmb3JiaWRk"
    "ZW4iKQogICAgaWYgbm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgIHJldHVybiB3ZWIuUmVzcG9uc2UoCiAgICAgICAg"
    "ICAgIHRleHQ9IjxoMz5EYXNoYm9hcmQgdW5hdmFpbGFibGU6IGRhdGFiYXNlIG5vdCByZWFjaGFibGUuPC9oMz4iLAogICAgICAg"
    "ICAgICBjb250ZW50X3R5cGU9InRleHQvaHRtbCIsCiAgICAgICAgICAgIHN0YXR1cz01MDMsCiAgICAgICAgKQogICAgcmV0dXJu"
    "IHdlYi5SZXNwb25zZSh0ZXh0PV9QQUdFLCBjb250ZW50X3R5cGU9InRleHQvaHRtbCIpCgoKYXN5bmMgZGVmIHd6YWRtaW5fYXBp"
    "KHJlcXVlc3QpOgogICAgcGF0aCA9IHJlcXVlc3QucGF0aAoKICAgIGlmIHBhdGguZW5kc3dpdGgoIi9hcGkvdGFrZW92ZXIiKToK"
    "ICAgICAgICAjIFNlc3Npb24gdGFrZW92ZXI6IHRoZSBORVhUIEthZ2dsZSBub3RlYm9vayBydW4gcHJvdmVzIGl0IGhvbGRzCiAg"
    "ICAgICAgIyB0aGUgY3VycmVudCBpbnN0YW5jZSB0b2tlbiAoc3RvcmVkIGluIE1vbmdvREIgbmV4dCB0byB0aGUgc2Vzc2lvbgog"
    "ICAgICAgICMgbG9jaykgYW5kIGFza3MgdGhpcyBpbnN0YW5jZSB0byBzdG9wLiBUb2tlbiBhdXRoIG9ubHkg4oCUIG5vCiAgICAg"
    "ICAgIyBzZXNzaW9uL0lQL1VBIGNoZWNrcywgYmVjYXVzZSB0aGUgY2FsbGVyIGlzIHRoZSBub3RlYm9vayBpdHNlbGYKICAgICAg"
    "ICAjICh3aGljaCBydW5zIG9uIGEgZGF0YWNlbnRlciBJUCBhbmQgd291bGQgdHJpcCB0aGUgaW50ZWwgZmlsdGVyKS4KICAgICAg"
    "ICB0cnk6CiAgICAgICAgICAgIGJvZHkgPSBhd2FpdCByZXF1ZXN0Lmpzb24oKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAg"
    "ICAgICAgICAgIGJvZHkgPSB7fQogICAgICAgIHRvayA9IHN0cihib2R5LmdldCgidG9rIiwgIiIpKS5zdHJpcCgpCiAgICAgICAg"
    "Z29vZCA9ICIiCiAgICAgICAgdHJ5OgogICAgICAgICAgICBpbXBvcnQgb3MgYXMgX29zCgogICAgICAgICAgICBmcm9tIHB5bW9u"
    "Z28gaW1wb3J0IE1vbmdvQ2xpZW50CgogICAgICAgICAgICBfdXJsID0gX29zLmVudmlyb24uZ2V0KCJEQVRBQkFTRV9VUkwiLCAi"
    "IikKICAgICAgICAgICAgaWYgX3VybDoKICAgICAgICAgICAgICAgIF9jbCA9IE1vbmdvQ2xpZW50KF91cmwsIHNlcnZlclNlbGVj"
    "dGlvblRpbWVvdXRNUz04MDAwKQogICAgICAgICAgICAgICAgX2RvYyA9IF9jbFsid3ptbF9rYWdnbGUiXVsic2Vzc2lvbl9sb2Nr"
    "Il0uZmluZF9vbmUoCiAgICAgICAgICAgICAgICAgICAgeyJfaWQiOiAiaW5zdGFuY2UifSkKICAgICAgICAgICAgICAgIF9jbC5j"
    "bG9zZSgpCiAgICAgICAgICAgICAgICBnb29kID0gc3RyKChfZG9jIG9yIHt9KS5nZXQoInRvayIsICIiKSkKICAgICAgICBleGNl"
    "cHQgRXhjZXB0aW9uOgogICAgICAgICAgICBnb29kID0gIiIKICAgICAgICBpZiBub3QgZ29vZCBvciBub3QgdG9rIG9yIHRvayAh"
    "PSBnb29kOgogICAgICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJmb3JiaWRkZW4ifSwgc3RhdHVz"
    "PTQwMykKCiAgICAgICAgaW1wb3J0IGFzeW5jaW8gYXMgX2FpbwoKICAgICAgICBhc3luYyBkZWYgX3d6Zml4X2RpZSgpOgogICAg"
    "ICAgICAgICBhd2FpdCBfYWlvLnNsZWVwKDEuMCkgICMgbGV0IHRoZSByZXNwb25zZSBmbHVzaCBmaXJzdAogICAgICAgICAgICB0"
    "cnk6CiAgICAgICAgICAgICAgICBmcm9tIC4uLmNvcmUuY29uZmlnX21hbmFnZXIgaW1wb3J0IENvbmZpZwogICAgICAgICAgICAg"
    "ICAgZnJvbSAuLi5jb3JlLnRnX2NsaWVudCBpbXBvcnQgVGdDbGllbnQKCiAgICAgICAgICAgICAgICBjaGF0ID0gc3RyKGdldGF0"
    "dHIoQ29uZmlnLCAiTE9HX0NIQVQiLCAiIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICAgICAgICAgIGlmIGNoYXQ6CiAgICAgICAg"
    "ICAgICAgICAgICAgYXdhaXQgVGdDbGllbnQuYm90LnNlbmRfbWVzc2FnZSgKICAgICAgICAgICAgICAgICAgICAgICAgY2hhdF9p"
    "ZD1fY2hhdF9pbnQoY2hhdCksCiAgICAgICAgICAgICAgICAgICAgICAgIHRleHQ9IvCflIQgPGI+V1pGSVg6PC9iPiBhIG5ld2Vy"
    "IG5vdGVib29rIHNlc3Npb24gaXMgIgogICAgICAgICAgICAgICAgICAgICAgICAidGFraW5nIG92ZXIg4oCUIHN0b3BwaW5nIHRo"
    "aXMgaW5zdGFuY2UuIiwKICAgICAgICAgICAgICAgICAgICAgICAgZGlzYWJsZV93ZWJfcGFnZV9wcmV2aWV3PVRydWUsCiAgICAg"
    "ICAgICAgICAgICAgICAgKQogICAgICAgICAgICAgICAgZnJvbSAucjFfY29yZSBpbXBvcnQgYWRtaW5fbG9nCgogICAgICAgICAg"
    "ICAgICAgYXdhaXQgYWRtaW5fbG9nKAogICAgICAgICAgICAgICAgICAgICLwn5S0IDxiPkJvdCBzdG9wcGluZzwvYj4iLAogICAg"
    "ICAgICAgICAgICAgICAgICLilI8gQSBuZXdlciBub3RlYm9vayBzZXNzaW9uIHRvb2sgb3ZlclxuIgogICAgICAgICAgICAgICAg"
    "ICAgICLilJYgVGhpcyBpbnN0YW5jZSBpcyBzaHV0dGluZyBkb3duIiwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgZXhj"
    "ZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAgICAgaW1wb3J0IG9zIGFzIF9vczIKICAgICAgICAg"
    "ICAgaW1wb3J0IHNpZ25hbCBhcyBfc2lnCgogICAgICAgICAgICBfb3MyLmtpbGwoX29zMi5nZXRwaWQoKSwgX3NpZy5TSUdJTlQp"
    "CgogICAgICAgIF9haW8uZ2V0X2V2ZW50X2xvb3AoKS5jcmVhdGVfdGFzayhfd3pmaXhfZGllKCkpCiAgICAgICAgcmV0dXJuIHdl"
    "Yi5qc29uX3Jlc3BvbnNlKAogICAgICAgICAgICB7Im9rIjogVHJ1ZSwgIm1zZyI6ICJ0YWtlb3ZlciBhY2NlcHRlZCDigJQgc2h1"
    "dHRpbmcgZG93biJ9CiAgICAgICAgKQoKICAgIGlmIHBhdGguZW5kc3dpdGgoIi9sb2dpbiIpOgogICAgICAgIGlwID0gX25vcm1f"
    "aXAoX2NsaWVudF9pcChyZXF1ZXN0KSkKICAgICAgICBpZiBub3QgX3VhX29rKHJlcXVlc3QpOgogICAgICAgICAgICByZXR1cm4g"
    "d2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJmb3JiaWRkZW4ifSwgc3RhdHVzPTQwMykKICAgICAgICBpZiBhd2FpdCBfaXNf"
    "YmFubmVkKGlwKToKICAgICAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAiYmFubmVkIn0sIHN0YXR1"
    "cz00MDMpCiAgICAgICAgaWYgYXdhaXQgX2lwX2ludGVsKGlwKToKICAgICAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNl"
    "KAogICAgICAgICAgICAgICAgeyJlcnJvciI6ICJkYXRhY2VudGVyL3Byb3h5IElQcyBhcmUgbm90IGFsbG93ZWQifSwgc3RhdHVz"
    "PTQwMwogICAgICAgICAgICApCiAgICAgICAgX3JlbSA9IGF3YWl0IF9sb2Nrb3V0X3N0YXRlKGlwKQogICAgICAgIGlmIF9yZW06"
    "CiAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSgKICAgICAgICAgICAgICAgIHsiZXJyb3IiOiBmInRvbyBtYW55"
    "IGF0dGVtcHRzIOKAlCB7X2xvY2tfdHh0KF9yZW0pfSJ9LAogICAgICAgICAgICAgICAgc3RhdHVzPTQyOSwKICAgICAgICAgICAg"
    "KQogICAgICAgIHRyeToKICAgICAgICAgICAgYm9keSA9IGF3YWl0IHJlcXVlc3QuanNvbigpCiAgICAgICAgZXhjZXB0IEV4Y2Vw"
    "dGlvbjoKICAgICAgICAgICAgYm9keSA9IHt9CiAgICAgICAgc3VibWl0dGVkID0gc3RyKGJvZHkuZ2V0KCJwYXNzIiwgIiIpKQog"
    "ICAgICAgIGRldiA9IGJvZHkuZ2V0KCJkZXYiKQogICAgICAgIGlmIG5vdCBpc2luc3RhbmNlKGRldiwgZGljdCk6CiAgICAgICAg"
    "ICAgIGRldiA9IHt9CiAgICAgICAgIyBhYnNvbHV0ZS1jb250cm9sIHN3aXRjaDogL2xvY2tkYXNoIGluIFRlbGVncmFtCiAgICAg"
    "ICAgX2xvY2tlZCA9IEZhbHNlCiAgICAgICAgdHJ5OgogICAgICAgICAgICBmcm9tIC4uLmNvcmUuY29uZmlnX21hbmFnZXIgaW1w"
    "b3J0IENvbmZpZyBhcyBfTENmZwoKICAgICAgICAgICAgX2xvY2tlZCA9IGJvb2woZ2V0YXR0cihfTENmZywgIkFETUlOX0RBU0hC"
    "T0FSRF9MT0NLRUQiLCBGYWxzZSkpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwogICAgICAgIGlm"
    "IF9sb2NrZWQ6CiAgICAgICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgZ2V0X2V2ZW50X2xvb3AgYXMgX2dlbAoKICAgICAgICAg"
    "ICAgX2dlbCgpLmNyZWF0ZV90YXNrKAogICAgICAgICAgICAgICAgX2xvZ2luX2FsZXJ0KGlwLCByZXF1ZXN0LCBkZXYsIHN1Ym1p"
    "dHRlZCwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAi8J+UkiBkYXNoYm9hcmQgaXMgTE9DS0VEIOKAlCBsb2dpbiByZWZ1"
    "c2VkIikKICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoCiAgICAgICAgICAgICAgICB7"
    "ImVycm9yIjogImRhc2hib2FyZCBsb2NrZWQgYnkgb3duZXIg4oCUIC9sb2NrZGFzaCB0byB1bmxvY2sifSwKICAgICAgICAgICAg"
    "ICAgIHN0YXR1cz00MDMsCiAgICAgICAgICAgICkKICAgICAgICByZWFsID0gYXdhaXQgZ2V0X2FkbWluX3Bhc3MoKQogICAgICAg"
    "IGlmIG5vdCByZWFsOgogICAgICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJhZG1pbiBwYXNzIHVu"
    "YXZhaWxhYmxlIn0sIHN0YXR1cz01MDMpCiAgICAgICAgaWYgbm90IHN1Ym1pdHRlZCBvciBub3QgY29tcGFyZV9kaWdlc3Qoc3Vi"
    "bWl0dGVkLCByZWFsKToKICAgICAgICAgICAgX3NlY3MsIF90aWVyID0gYXdhaXQgX3JlY29yZF9mYWlsKGlwKQogICAgICAgICAg"
    "ICBfbm90ZSA9IGYiIOKAlCBMT0NLRUQgT1VUIHtfbG9ja190eHQoX3NlY3MpfSIgaWYgX3NlY3MgZWxzZSAiIgogICAgICAgICAg"
    "ICBmcm9tIGFzeW5jaW8gaW1wb3J0IGdldF9ldmVudF9sb29wIGFzIF9nZWwKCiAgICAgICAgICAgIF9nZWwoKS5jcmVhdGVfdGFz"
    "aygKICAgICAgICAgICAgICAgIF9sb2dpbl9hbGVydChpcCwgcmVxdWVzdCwgZGV2LCBzdWJtaXR0ZWQsCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgZiLinYwgd3JvbmcgcGFzc3dvcmR7X25vdGV9IikKICAgICAgICAgICAgKQogICAgICAgICAgICBpZiBf"
    "c2VjczoKICAgICAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSgKICAgICAgICAgICAgICAgICAgICB7ImVycm9y"
    "IjogZiJ3cm9uZyBwYXNzd29yZCDigJQge19sb2NrX3R4dChfc2Vjcyl9In0sCiAgICAgICAgICAgICAgICAgICAgc3RhdHVzPTQy"
    "OSwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAid3Jvbmcg"
    "cGFzc3dvcmQifSwgc3RhdHVzPTQwMSkKICAgICAgICBfcHJpb3IgPSBhd2FpdCBfcHJpb3JfZmFpbHMoaXApCiAgICAgICAgX3Jl"
    "Y29yZF9vayhpcCkKICAgICAgICBpZiBfcHJpb3IgPiAwOgogICAgICAgICAgICBmcm9tIGFzeW5jaW8gaW1wb3J0IGdldF9ldmVu"
    "dF9sb29wIGFzIF9nZWwKCiAgICAgICAgICAgIF9nZWwoKS5jcmVhdGVfdGFzaygKICAgICAgICAgICAgICAgIF9sb2dpbl9hbGVy"
    "dChpcCwgcmVxdWVzdCwgZGV2LCBzdWJtaXR0ZWQsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZiLinIUgbG9naW4gT0sg"
    "YWZ0ZXIge19wcmlvcn0gZmFpbGVkIGF0dGVtcHQocykiKQogICAgICAgICAgICApCiAgICAgICAgZWxzZToKICAgICAgICAgICAg"
    "ZnJvbSBhc3luY2lvIGltcG9ydCBnZXRfZXZlbnRfbG9vcCBhcyBfZ2VsCgogICAgICAgICAgICBfZ2VsKCkuY3JlYXRlX3Rhc2so"
    "CiAgICAgICAgICAgICAgICBfbG9naW5fYWxlcnQoaXAsIHJlcXVlc3QsIGRldiwgIvCflJIiLCAi4pyFIGxvZ2luIHN1Y2Nlc3Nm"
    "dWwiKQogICAgICAgICAgICApCiAgICAgICAgdG9rID0gYXdhaXQgX3Nlc3Npb25fdG9rZW4oKQogICAgICAgIHJlc3AgPSB3ZWIu"
    "anNvbl9yZXNwb25zZSh7Im9rIjogVHJ1ZX0pCiAgICAgICAgcmVzcC5zZXRfY29va2llKAogICAgICAgICAgICAid3phZG1pbiIs"
    "IHRvaywgbWF4X2FnZT1TRVNTSU9OX0ggKiAzNjAwLCBodHRwb25seT1UcnVlLCBzYW1lc2l0ZT0iTGF4IgogICAgICAgICkKICAg"
    "ICAgICByZXR1cm4gcmVzcAoKICAgIGlmIG5vdCBhd2FpdCBfY2hlY2tfc2Vzc2lvbihyZXF1ZXN0KToKICAgICAgICByZXR1cm4g"
    "d2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJ1bmF1dGhvcml6ZWQifSwgc3RhdHVzPTQwMSkKICAgIGlmIG5vdCBfdWFfb2so"
    "cmVxdWVzdCkgb3IgYXdhaXQgX2lzX2Jhbm5lZChfbm9ybV9pcChfY2xpZW50X2lwKHJlcXVlc3QpKSk6CiAgICAgICAgcmV0dXJu"
    "IHdlYi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAiZm9yYmlkZGVuIn0sIHN0YXR1cz00MDMpCgogICAgaWYgcGF0aC5lbmRzd2l0"
    "aCgiL2xvZ291dCIpOgogICAgICAgIHJlc3AgPSB3ZWIuanNvbl9yZXNwb25zZSh7Im9rIjogVHJ1ZX0pCiAgICAgICAgcmVzcC5k"
    "ZWxfY29va2llKCJ3emFkbWluIikKICAgICAgICByZXR1cm4gcmVzcAoKICAgIGlmIHBhdGguZW5kc3dpdGgoIi9zdGF0ZSIpOgog"
    "ICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZShhd2FpdCBfc3RhdGUoKSkKCiAgICBpZiBwYXRoLmVuZHN3aXRoKCIvaGlz"
    "dG9yeSIpOgogICAgICAgIHRyeToKICAgICAgICAgICAgdWlkID0gaW50KHJlcXVlc3QucXVlcnkuZ2V0KCJ1aWQiLCAiMCIpKQog"
    "ICAgICAgIGV4Y2VwdCBWYWx1ZUVycm9yOgogICAgICAgICAgICB1aWQgPSAwCiAgICAgICAgcSA9IChyZXF1ZXN0LnF1ZXJ5Lmdl"
    "dCgicSIpIG9yICIiKS5zdHJpcCgpWzo2MF0KICAgICAgICBkb2NzID0gYXdhaXQgZmluZChxLCB1aWQsIGFsbF91c2Vycz1GYWxz"
    "ZSwgbGltaXQ9MjUpIGlmIHVpZCBlbHNlIFtdCiAgICAgICAgb3V0ID0gW10KICAgICAgICBmb3IgZCBpbiBkb2NzOgogICAgICAg"
    "ICAgICB0Z19saW5rcyA9IGQuZ2V0KCJ0Z19saW5rcyIpIG9yIFtdCiAgICAgICAgICAgIG91dC5hcHBlbmQoCiAgICAgICAgICAg"
    "ICAgICB7CiAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiBzdHIoZC5nZXQoIm5hbWUiLCAiIikpWzo5MF0sCiAgICAgICAgICAg"
    "ICAgICAgICAgInNpemUiOiBpbnQoZC5nZXQoInNpemUiKSBvciAwKSwKICAgICAgICAgICAgICAgICAgICAiZGF0ZSI6IHN0cihk"
    "LmdldCgiZGF0ZSIpIG9yICIiKVs6MTZdLAogICAgICAgICAgICAgICAgICAgICJ0ZyI6IHN0cih0Z19saW5rc1swXSkgaWYgdGdf"
    "bGlua3MgZWxzZSAiIiwKICAgICAgICAgICAgICAgICAgICAiY2xvdWQiOiBzdHIoZC5nZXQoImNsb3VkX2xpbmsiKSBvciAiIiks"
    "CiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICkKICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJvayI6IFRy"
    "dWUsICJpdGVtcyI6IG91dH0pCgogICAgaWYgcGF0aC5lbmRzd2l0aCgiL2FjdGlvbiIpOgogICAgICAgIHRyeToKICAgICAgICAg"
    "ICAgYm9keSA9IGF3YWl0IHJlcXVlc3QuanNvbigpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgYm9keSA9"
    "IHt9CiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKGF3YWl0IF9hY3Rpb24oYm9keSkpCgogICAgcmV0dXJuIHdlYi5q"
    "c29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAidW5rbm93biBlbmRwb2ludCJ9LCBzdGF0dXM9NDA0KQoKCmFzeW5jIGRlZiBfYWN0aW9u"
    "X2xvZyh3aGF0LCBkZXRhaWwpOgogICAgIiIiRGFzaGJvYXJkIGFjdGlvbiAtPiB0aGUgYWRtaW4gbG9ncyBncm91cC4iIiIKICAg"
    "IHRyeToKICAgICAgICBmcm9tIC5yMV9jb3JlIGltcG9ydCBhZG1pbl9sb2cKCiAgICAgICAgYXdhaXQgYWRtaW5fbG9nKAogICAg"
    "ICAgICAgICAi8J+OmyA8Yj5EYXNoYm9hcmQgYWN0aW9uPC9iPiIsCiAgICAgICAgICAgIGYi4pSPIDxiPkFjdGlvbjwvYj4g4oaS"
    "IHt3aGF0fVxuIgogICAgICAgICAgICArICIiLmpvaW4oZiLilKAge2t9IOKGkiB7dn1cbiIgZm9yIGssIHYgaW4gZGV0YWlsLml0"
    "ZW1zKCkpCiAgICAgICAgICAgICsgIuKUliB2aWEgL3d6YWRtaW4iLAogICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAg"
    "ICAgICAgcGFzcwoKCmFzeW5jIGRlZiBfYWN0aW9uKGEpOgogICAgYWN0ID0gc3RyKGEuZ2V0KCJhY3Rpb24iLCAiIikpLnN0cmlw"
    "KCkKICAgIHRyeToKICAgICAgICB1aWQgPSBpbnQoYS5nZXQoInVpZCIsIDApIG9yIDApCiAgICBleGNlcHQgKFR5cGVFcnJvciwg"
    "VmFsdWVFcnJvcik6CiAgICAgICAgdWlkID0gMAoKICAgIHRyeToKICAgICAgICBpZiBhY3QgPT0gInNldGNhcCI6CiAgICAgICAg"
    "ICAgIGdiID0gZmxvYXQoYS5nZXQoImdiIiwgMCkgb3IgMCkKICAgICAgICAgICAgYXdhaXQgc2V0X3VzZXJfY2FwKHVpZCwgZ2Ig"
    "aWYgZ2IgPiAwIGVsc2UgTm9uZSkKICAgICAgICAgICAgYXdhaXQgX2FjdGlvbl9sb2coIlNFVCBDQVAiLCB7InVpZCI6IHVpZCwg"
    "ImdiIjogYS5nZXQoImdiIiwgIiIpfSkKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJjYXAgZm9yIHt1"
    "aWR9IOKGkiB7X2ZtdF9nYihnYikgaWYgZ2IgPiAwIGVsc2UgJ2RlZmF1bHQnfSJ9CiAgICAgICAgaWYgYWN0ID09ICJzZXRtdXNp"
    "YyI6CiAgICAgICAgICAgIG4gPSBpbnQoZmxvYXQoYS5nZXQoIm4iLCAwKSBvciAwKSkKICAgICAgICAgICAgYXdhaXQgc2V0X3Vz"
    "ZXJfbXVzaWModWlkLCBuIGlmIG4gPiAwIGVsc2UgTm9uZSkKICAgICAgICAgICAgYXdhaXQgX2FjdGlvbl9sb2coIlNFVCBNVVNJ"
    "QyBMSU1JVCIsIHsidWlkIjogdWlkLCAibiI6IGEuZ2V0KCJuIiwgIiIpfSkKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVl"
    "LCAibXNnIjogZiJtdXNpYyBsaW1pdCBmb3Ige3VpZH0g4oaSIHtuIGlmIG4gPiAwIGVsc2UgJ2RlZmF1bHQnfSJ9CiAgICAgICAg"
    "aWYgYWN0ID09ICJnbXVzaWMiOgogICAgICAgICAgICBuID0gaW50KGZsb2F0KGEuZ2V0KCJuIiwgMTApIG9yIDEwKSkKICAgICAg"
    "ICAgICAgaWYgbiA8IDE6CiAgICAgICAgICAgICAgICBuID0gMQogICAgICAgICAgICBuID0gbWluKDUwMCwgbikKICAgICAgICAg"
    "ICAgYXdhaXQgc2V0X2dsb2JhbF9tdXNpYyhuKQogICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygiU0VUIEdMT0JBTCBNVVNJ"
    "QyIsIHsibiI6IG59KQogICAgICAgICAgICByZXR1cm4geyJvayI6IFRydWUsICJtc2ciOiBmImRlZmF1bHQgbXVzaWMgbGltaXQg"
    "4oaSIHtufSBzb25ncyJ9CiAgICAgICAgaWYgYWN0ID09ICJiYW4iOgogICAgICAgICAgICBhd2FpdCBzZXRfdXNlcl9jYXAodWlk"
    "LCAwKQogICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygiQkFOIFVTRVIiLCB7InVpZCI6IHVpZH0pCiAgICAgICAgICAgIHJl"
    "dHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYidXNlciB7dWlkfSBibG9ja2VkIChjYXAgMCkifQogICAgICAgIGlmIGFjdCA9PSAi"
    "dW5iYW4iOgogICAgICAgICAgICBhd2FpdCBzZXRfdXNlcl9jYXAodWlkLCBOb25lKQogICAgICAgICAgICBhd2FpdCBfYWN0aW9u"
    "X2xvZygiVU5CQU4gVVNFUiIsIHsidWlkIjogdWlkfSkKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJ1"
    "c2VyIHt1aWR9IGJhY2sgdG8gZ2xvYmFsIGRlZmF1bHQifQogICAgICAgIGlmIGFjdCA9PSAicmVzZXRjYXAiOgogICAgICAgICAg"
    "ICBhd2FpdCByZXNldF91c2FnZSh1aWQpCiAgICAgICAgICAgIGF3YWl0IF9hY3Rpb25fbG9nKCJSRVNFVCBEQVkgVVNBR0UiLCB7"
    "InVpZCI6IHVpZH0pCiAgICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYidG9kYXkncyB1c2FnZSByZXNldCBm"
    "b3Ige3VpZH0ifQogICAgICAgIGlmIGFjdCA9PSAiZGVkdWN0Y2FwIjoKICAgICAgICAgICAgZ2IgPSBmbG9hdChhLmdldCgiZ2Ii"
    "LCAwKSBvciAwKQogICAgICAgICAgICBvaywgbXNnID0gYXdhaXQgX2RlZHVjdF9jYXAodWlkLCBnYikKICAgICAgICAgICAgcmV0"
    "dXJuIHsib2siOiBvaywgIm1zZyI6IG1zZ30KICAgICAgICBpZiBhY3QgPT0gImRlbHVzZXIiOgogICAgICAgICAgICBpZiBub3Qg"
    "dWlkOgogICAgICAgICAgICAgICAgcmV0dXJuIHsib2siOiBGYWxzZSwgIm1zZyI6ICJubyB1c2VyIGlkIn0KICAgICAgICAgICAg"
    "YXdhaXQgZGVsZXRlX3VzZXIodWlkKQogICAgICAgICAgICBhd2FpdCBfYWN0aW9uX2xvZygiUkVNT1ZFIFVTRVIiLCB7InVpZCI6"
    "IHVpZH0pCiAgICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYidXNlciB7dWlkfSByZW1vdmVkIn0KICAgICAg"
    "ICBpZiBhY3QgPT0gImJvdGNhcCI6CiAgICAgICAgICAgIGdiID0gZmxvYXQoYS5nZXQoImdiIiwgMCkgb3IgMCkKICAgICAgICAg"
    "ICAgaWYgZ2IgPD0gMDoKICAgICAgICAgICAgICAgIHJldHVybiB7Im9rIjogRmFsc2UsICJtc2ciOiAiZ2l2ZSBhIHBvc2l0aXZl"
    "IEdCIHZhbHVlIn0KICAgICAgICAgICAgYXdhaXQgc2V0X2dsb2JhbF9jYXBfZ2IoZ2IpCiAgICAgICAgICAgIGF3YWl0IF9hY3Rp"
    "b25fbG9nKCJTRVQgREVGQVVMVCBDQVAiLCB7ImdiIjogZ2J9KQogICAgICAgICAgICByZXR1cm4geyJvayI6IFRydWUsICJtc2ci"
    "OiBmImRlZmF1bHQgY2FwIOKGkiB7X2ZtdF9nYihnYil9In0KICAgICAgICBpZiBhY3QgPT0gImtpbGwiOgogICAgICAgICAgICBh"
    "d2FpdCBfYWN0aW9uX2xvZygiS0lMTCBUQVNLIiwgeyJtaWQiOiBhLmdldCgibWlkIiwgIiIpfSkKICAgICAgICAgICAgb2ssIG1z"
    "ZyA9IGF3YWl0IF9raWxsX3Rhc2soYS5nZXQoIm1pZCIpKQogICAgICAgICAgICByZXR1cm4geyJvayI6IG9rLCAibXNnIjogbXNn"
    "fQogICAgICAgIGlmIGFjdCA9PSAia2lsbGFsbCI6CiAgICAgICAgICAgIGZyb20gLi4uIGltcG9ydCB0YXNrX2RpY3QKCiAgICAg"
    "ICAgICAgIGF3YWl0IF9hY3Rpb25fbG9nKCJLSUxMIEFMTCBUQVNLUyIsIHt9KQogICAgICAgICAgICBuID0gMAogICAgICAgICAg"
    "ICBmb3IgbWlkLCB0IGluIGxpc3QodGFza19kaWN0Lml0ZW1zKCkpOgogICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAg"
    "ICAgICAgIGF3YWl0IHQudGFzaygpLmNhbmNlbF90YXNrKCkKICAgICAgICAgICAgICAgICAgICBuICs9IDEKICAgICAgICAgICAg"
    "ICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAgICByZXR1cm4geyJvayI6IFRy"
    "dWUsICJtc2ciOiBmIntufSB0YXNrKHMpIGNhbmNlbGxlZCJ9CiAgICAgICAgaWYgYWN0ID09ICJyZXBvcnQiOgogICAgICAgICAg"
    "ICBvaywgbXNnID0gYXdhaXQgc2VuZF9yZXBvcnQoKQogICAgICAgICAgICByZXR1cm4geyJvayI6IG9rLCAibXNnIjogbXNnfQog"
    "ICAgICAgIHJldHVybiB7Im9rIjogRmFsc2UsICJtc2ciOiBmInVua25vd24gYWN0aW9uOiB7YWN0fSJ9CiAgICBleGNlcHQgRXhj"
    "ZXB0aW9uIGFzIGU6CiAgICAgICAgcmV0dXJuIHsib2siOiBGYWxzZSwgIm1zZyI6IHN0cihlKVs6MTYwXX0KCgojIOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIFRoZSBwYWdlIOKA"
    "lCA3IHRoZW1lcyAobWF0Y2hpbmcgdGhlIHN0cmVhbSBwbGF5ZXIpLCBtb2JpbGUtZmlyc3QKIyDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCl9QQUdFID0gIiIiPCFkb2N0eXBlIGh0"
    "bWw+CjxodG1sIGxhbmc9ImVuIiBkYXRhLXRoZW1lPSJvbnl4Ij4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEg"
    "bmFtZT0idmlld3BvcnQiIGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0aCxpbml0aWFsLXNjYWxlPTEsdmlld3BvcnQtZml0PWNv"
    "dmVyIj4KPG1ldGEgbmFtZT0icm9ib3RzIiBjb250ZW50PSJub2luZGV4Ij4KPHRpdGxlPldaTUwtWCBDb250cm9sPC90aXRsZT4K"
    "PHN0eWxlPgo6cm9vdHstLXI6MTZweDstLXNhbnM6LWFwcGxlLXN5c3RlbSxCbGlua01hY1N5c3RlbUZvbnQsJ1NlZ29lIFVJJyxS"
    "b2JvdG8sc2Fucy1zZXJpZn0KW2RhdGEtdGhlbWU9Im9ueXgiXXstLWJnOiMwNTA1MDg7LS1zdXJmYWNlOiMwYTBhMTI7LS1zdXJm"
    "YWNlLTI6IzBlMGUxODstLWxpbmU6cmdiYSgxMzksOTIsMjQ2LC4yMik7LS10ZXh0OiNmNGYyZmY7LS1tdXRlZDojYjZiMGQ0Oy0t"
    "YWNjZW50OiM4YjVjZjY7LS1hY2NlbnQtMjojYTc4YmZhOy0tYWNjZW50LXNvZnQ6IzFhMTAzMzstLWRhbmdlcjojZjg3MTcxOy0t"
    "b2s6IzRhZGU4MH0KW2RhdGEtdGhlbWU9ImFxdWEiXXstLWJnOiMwMzE0MWI7LS1zdXJmYWNlOiMwNTFlMjg7LS1zdXJmYWNlLTI6"
    "IzA3MjQyZjstLWxpbmU6cmdiYSgzNCwyMTEsMjM4LC4yMik7LS10ZXh0OiNlYWZjZmY7LS1tdXRlZDojYTNjNmQxOy0tYWNjZW50"
    "OiMyMmQzZWU7LS1hY2NlbnQtMjojNjdlOGY5Oy0tYWNjZW50LXNvZnQ6IzA2MjQzMDstLWRhbmdlcjojZjg3MTcxOy0tb2s6IzRh"
    "ZGU4MH0KW2RhdGEtdGhlbWU9ImVtYmVyIl17LS1iZzojMTIwYTA1Oy0tc3VyZmFjZTojMWEwZjA3Oy0tc3VyZmFjZS0yOiMyMTE0"
    "MGE7LS1saW5lOnJnYmEoMjQ1LDE1OCwxMSwuMjIpOy0tdGV4dDojZmRmM2U3Oy0tbXV0ZWQ6I2QwYjhhMDstLWFjY2VudDojZjU5"
    "ZTBiOy0tYWNjZW50LTI6I2ZiYmYyNDstLWFjY2VudC1zb2Z0OiMyYTFhMDY7LS1kYW5nZXI6I2Y4NzE3MTstLW9rOiM0YWRlODB9"
    "CltkYXRhLXRoZW1lPSJkYXJrIl17LS1iZzojMDQwNDBiOy0tc3VyZmFjZTojMDgwODEyOy0tc3VyZmFjZS0yOiMwZTBlMWE7LS1s"
    "aW5lOnJnYmEoNjEsMTM1LDI1NSwuMTgpOy0tdGV4dDojZjVmN2ZmOy0tbXV0ZWQ6I2MzY2JkZjstLWFjY2VudDojM2Q4N2ZmOy0t"
    "YWNjZW50LTI6IzViOWRmZjstLWFjY2VudC1zb2Z0OiMwZDFiM2E7LS1kYW5nZXI6I2ZmNmI2YjstLW9rOiM1MWU5OGN9CltkYXRh"
    "LXRoZW1lPSJsaWdodCJdey0tYmc6I2YyZjVmYjstLXN1cmZhY2U6I2ZmZmZmZjstLXN1cmZhY2UtMjojZWVmMWY4Oy0tbGluZTpy"
    "Z2JhKDE1LDIzLDQyLC4xMik7LS10ZXh0OiMwZjE3MmE7LS1tdXRlZDojNWE2NDc4Oy0tYWNjZW50OiMyNTYzZWI7LS1hY2NlbnQt"
    "MjojM2I4MmY2Oy0tYWNjZW50LXNvZnQ6I2RiZWFmZTstLWRhbmdlcjojZGMyNjI2Oy0tb2s6IzE2YTM0YX0KW2RhdGEtdGhlbWU9"
    "InZpYnJhbnQiXXstLWJnOiMwYTA2MTI7LS1zdXJmYWNlOiMxMzBiMjA7LS1zdXJmYWNlLTI6IzFiMTEzMDstLWxpbmU6cmdiYSgx"
    "NjgsODUsMjQ3LC4yNSk7LS10ZXh0OiNmYWY1ZmY7LS1tdXRlZDojYzRiNWZkOy0tYWNjZW50OiNhODU1Zjc7LS1hY2NlbnQtMjoj"
    "ZDk0NmVmOy0tYWNjZW50LXNvZnQ6IzJlMTA2NTstLWRhbmdlcjojZmI3MTg1Oy0tb2s6IzM0ZDM5OX0KW2RhdGEtdGhlbWU9ImJs"
    "b3Nzb20iXXstLWJnOiMxNjBhMTA7LS1zdXJmYWNlOiMyMDEwMWI7LS1zdXJmYWNlLTI6IzJhMTYyMjstLWxpbmU6cmdiYSgyNDQs"
    "MTE0LDE4MiwuMjUpOy0tdGV4dDojZmRmMmY4Oy0tbXV0ZWQ6I2Y5YThkNDstLWFjY2VudDojZjQ3MmI2Oy0tYWNjZW50LTI6I2Zk"
    "YTRhZjstLWFjY2VudC1zb2Z0OiM0YTA0NGU7LS1kYW5nZXI6I2Y4NzE3MTstLW9rOiM2ZWU3Yjd9Cip7Ym94LXNpemluZzpib3Jk"
    "ZXItYm94O21hcmdpbjowO3BhZGRpbmc6MH0KYm9keXtiYWNrZ3JvdW5kOnZhcigtLWJnKTtjb2xvcjp2YXIoLS10ZXh0KTtmb250"
    "OjE1cHgvMS40NSB2YXIoLS1zYW5zKTttaW4taGVpZ2h0OjEwMHZoO3BhZGRpbmctYm90dG9tOjQwcHg7LXdlYmtpdC10YXAtaGln"
    "aGxpZ2h0LWNvbG9yOnRyYW5zcGFyZW50fQpib2R5OjpiZWZvcmV7Y29udGVudDoiIjtwb3NpdGlvbjpmaXhlZDtpbnNldDotMjAl"
    "O3otaW5kZXg6MDtwb2ludGVyLWV2ZW50czpub25lO2JhY2tncm91bmQ6cmFkaWFsLWdyYWRpZW50KDM2JSAzMCUgYXQgMTglIDEy"
    "JSxjb2xvci1taXgoaW4gc3JnYix2YXIoLS1hY2NlbnQpIDE2JSx0cmFuc3BhcmVudCksdHJhbnNwYXJlbnQgNzAlKSxyYWRpYWwt"
    "Z3JhZGllbnQoMzIlIDI4JSBhdCA4MiUgMjAlLGNvbG9yLW1peChpbiBzcmdiLHZhcigtLWFjY2VudCkgMTAlLHRyYW5zcGFyZW50"
    "KSx0cmFuc3BhcmVudCA3MiUpfQoud3JhcHtwb3NpdGlvbjpyZWxhdGl2ZTt6LWluZGV4OjE7bWF4LXdpZHRoOjY4MHB4O21hcmdp"
    "bjowIGF1dG87cGFkZGluZzowIDE0cHh9Ci50b3BiYXJ7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6MTA7ZGlzcGxheTpm"
    "bGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6OHB4O3BhZGRpbmc6MTRweCAycHg7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3Jn"
    "Yix2YXIoLS1iZykgODglLHRyYW5zcGFyZW50KTtiYWNrZHJvcC1maWx0ZXI6Ymx1cigxNHB4KTtib3JkZXItYm90dG9tOjFweCBz"
    "b2xpZCB2YXIoLS1saW5lKX0KLmxvZ297Zm9udC1zaXplOjE4cHg7Zm9udC13ZWlnaHQ6ODAwO2xldHRlci1zcGFjaW5nOi0uMDJl"
    "bTtmbGV4OjF9Ci5sb2dvIGJ7Y29sb3I6dmFyKC0tYWNjZW50KX0KLmNoaXB7Zm9udC1zaXplOjEycHg7Zm9udC13ZWlnaHQ6NjAw"
    "O2NvbG9yOnZhcigtLW11dGVkKTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yYWRpdXM6OTk5cHg7cGFkZGlu"
    "Zzo0cHggMTBweDtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UpfQpoMntmb250LXNpemU6MTNweDt0ZXh0LXRyYW5zZm9ybTp1cHBl"
    "cmNhc2U7bGV0dGVyLXNwYWNpbmc6LjA4ZW07Y29sb3I6dmFyKC0tbXV0ZWQpO21hcmdpbjoyMnB4IDJweCAxMHB4fQouY2FyZHti"
    "YWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czp2YXIoLS1y"
    "KTtwYWRkaW5nOjE0cHg7bWFyZ2luLWJvdHRvbToxMnB4fQouc3RhdHN7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1u"
    "czpyZXBlYXQoMiwxZnIpO2dhcDoxMHB4O21hcmdpbi10b3A6MTRweH0KLnN0YXR7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlKTti"
    "b3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHh9Ci5zdGF0IC52e2ZvbnQt"
    "c2l6ZToxOXB4O2ZvbnQtd2VpZ2h0OjgwMH0KLnN0YXQgLmt7Zm9udC1zaXplOjExcHg7Y29sb3I6dmFyKC0tbXV0ZWQpO3RleHQt"
    "dHJhbnNmb3JtOnVwcGVyY2FzZTtsZXR0ZXItc3BhY2luZzouMDZlbTttYXJnaW4tdG9wOjJweH0KLmJhcntoZWlnaHQ6OHB4O2Jv"
    "cmRlci1yYWRpdXM6OTlweDtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UtMik7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbjoxMHB4IDAg"
    "NnB4fQouYmFyIGl7ZGlzcGxheTpibG9jaztoZWlnaHQ6MTAwJTtib3JkZXItcmFkaXVzOjk5cHg7YmFja2dyb3VuZDpsaW5lYXIt"
    "Z3JhZGllbnQoOTBkZWcsdmFyKC0tYWNjZW50KSx2YXIoLS1hY2NlbnQtMikpO3RyYW5zaXRpb246d2lkdGggLjVzIGVhc2V9Ci5t"
    "dXRlZHtjb2xvcjp2YXIoLS1tdXRlZCk7Zm9udC1zaXplOjEyLjVweH0KLnJvd3tkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2Vu"
    "dGVyO2dhcDo4cHh9Ci51c3ItbmFtZXtmb250LXdlaWdodDo3MDA7Zm9udC1zaXplOjE1cHh9Ci5idG57Ym9yZGVyOjFweCBzb2xp"
    "ZCB2YXIoLS1saW5lKTtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UtMik7Y29sb3I6dmFyKC0tdGV4dCk7Ym9yZGVyLXJhZGl1czox"
    "MHB4O3BhZGRpbmc6N3B4IDExcHg7Zm9udDo2MDAgMTIuNXB4IHZhcigtLXNhbnMpO2N1cnNvcjpwb2ludGVyO3RyYW5zaXRpb246"
    "Ym9yZGVyLWNvbG9yIC4xNXMsdHJhbnNmb3JtIC4xc30KLmJ0bjphY3RpdmV7dHJhbnNmb3JtOnNjYWxlKC45Nil9Ci5idG4ucHJp"
    "e2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDEzNWRlZyx2YXIoLS1hY2NlbnQpLHZhcigtLWFjY2VudC0yKSk7Ym9yZGVyLWNv"
    "bG9yOnRyYW5zcGFyZW50O2NvbG9yOiNmZmZ9Ci5idG4uZG5ne2NvbG9yOnZhcigtLWRhbmdlcik7Ym9yZGVyLWNvbG9yOmNvbG9y"
    "LW1peChpbiBzcmdiLHZhcigtLWRhbmdlcikgNDUlLHRyYW5zcGFyZW50KX0KLmJ0bnN7ZGlzcGxheTpmbGV4O2ZsZXgtd3JhcDp3"
    "cmFwO2dhcDo3cHg7bWFyZ2luLXRvcDoxMHB4fQppbnB1dFt0eXBlPXBhc3N3b3JkXXt3aWR0aDoxMDAlO2JhY2tncm91bmQ6dmFy"
    "KC0tc3VyZmFjZS0yKTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yYWRpdXM6MTJweDtjb2xvcjp2YXIoLS10"
    "ZXh0KTtwYWRkaW5nOjEzcHggMTRweDtmb250OjE1cHggdmFyKC0tc2Fucyl9CmlucHV0W3R5cGU9cGFzc3dvcmRdOmZvY3Vze291"
    "dGxpbmU6bm9uZTtib3JkZXItY29sb3I6dmFyKC0tYWNjZW50LTIpfQoubG9naW4tYm94e21heC13aWR0aDozNjBweDttYXJnaW46"
    "MTZ2aCBhdXRvIDA7dGV4dC1hbGlnbjpjZW50ZXJ9Ci50YXNre2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjEw"
    "cHh9Ci50YXNrIC5ubXtmbGV4OjE7bWluLXdpZHRoOjA7Zm9udC13ZWlnaHQ6NjAwO2ZvbnQtc2l6ZToxMy41cHg7d2hpdGUtc3Bh"
    "Y2U6bm93cmFwO292ZXJmbG93OmhpZGRlbjt0ZXh0LW92ZXJmbG93OmVsbGlwc2lzfQouaGlkZGVue2Rpc3BsYXk6bm9uZX0KI3Rv"
    "YXN0e3Bvc2l0aW9uOmZpeGVkO2xlZnQ6NTAlO2JvdHRvbToyNnB4O3RyYW5zZm9ybTp0cmFuc2xhdGVYKC01MCUpIHRyYW5zbGF0"
    "ZVkoMTJweCk7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlKTtjb2xvcjp2YXIoLS10ZXh0KTtib3JkZXI6MXB4IHNvbGlkIHZhcigt"
    "LWxpbmUpO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHggMTZweDtmb250LXNpemU6MTNweDtvcGFjaXR5OjA7cG9pbnRl"
    "ci1ldmVudHM6bm9uZTt0cmFuc2l0aW9uOi4yNXM7ei1pbmRleDo5OTttYXgtd2lkdGg6ODV2d30KI3RvYXN0LnNob3d7b3BhY2l0"
    "eToxO3RyYW5zZm9ybTp0cmFuc2xhdGVYKC01MCUpIHRyYW5zbGF0ZVkoMCl9CiN0aGVtZXN7ZGlzcGxheTpub25lO3Bvc2l0aW9u"
    "OmFic29sdXRlO3JpZ2h0OjA7dG9wOjExMCU7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlKTtib3JkZXI6MXB4IHNvbGlkIHZhcigt"
    "LWxpbmUpO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjhweDt6LWluZGV4OjIwO2JveC1zaGFkb3c6MCAxOHB4IDUwcHggLTEy"
    "cHggcmdiYSgwLDAsMCwuNTUpfQojdGhlbWVzLm9wZW57ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZy"
    "O2dhcDo0cHh9CiN0aGVtZXMgYnV0dG9ue2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjhweDtiYWNrZ3JvdW5k"
    "Om5vbmU7Ym9yZGVyOm5vbmU7Y29sb3I6dmFyKC0tdGV4dCk7Zm9udDo2MDAgMTIuNXB4IHZhcigtLXNhbnMpO3BhZGRpbmc6OHB4"
    "IDEwcHg7Ym9yZGVyLXJhZGl1czo5cHg7Y3Vyc29yOnBvaW50ZXJ9CiN0aGVtZXMgYnV0dG9uOmhvdmVye2JhY2tncm91bmQ6dmFy"
    "KC0tYWNjZW50LXNvZnQpfQouc3d7d2lkdGg6MTRweDtoZWlnaHQ6MTRweDtib3JkZXItcmFkaXVzOjVweDtkaXNwbGF5OmlubGlu"
    "ZS1ibG9ja30KLmhpc3QtaXRlbXtib3JkZXItYm90dG9tOjFweCBzb2xpZCB2YXIoLS1saW5lKTtwYWRkaW5nOjEwcHggMnB4O2Rp"
    "c3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjEwcHh9Ci5oaXN0LWl0ZW06bGFzdC1jaGlsZHtib3JkZXI6bm9uZX0K"
    "Lmhpc3QtaXRlbSBhe2NvbG9yOnZhcigtLWFjY2VudC0yKTt0ZXh0LWRlY29yYXRpb246bm9uZTtmb250LXdlaWdodDo3MDB9Ci5l"
    "cnJ7Y29sb3I6dmFyKC0tZGFuZ2VyKTtmb250LXNpemU6MTNweDttYXJnaW4tdG9wOjhweDttaW4taGVpZ2h0OjE4cHh9Ci5waWxs"
    "e2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjcwMDtib3JkZXItcmFkaXVzOjk5cHg7cGFkZGluZzoycHggOHB4O2JhY2tncm91"
    "bmQ6dmFyKC0tYWNjZW50LXNvZnQpO2NvbG9yOnZhcigtLWFjY2VudC0yKX0KLnBpbGwuYmFue2JhY2tncm91bmQ6Y29sb3ItbWl4"
    "KGluIHNyZ2IsdmFyKC0tZGFuZ2VyKSAxOCUsdHJhbnNwYXJlbnQpO2NvbG9yOnZhcigtLWRhbmdlcil9Cjwvc3R5bGU+CjwvaGVh"
    "ZD4KPGJvZHk+CjxkaXYgY2xhc3M9IndyYXAiPgogIDxkaXYgY2xhc3M9InRvcGJhciI+CiAgICA8ZGl2IGNsYXNzPSJsb2dvIj5X"
    "Wk1MPGI+LVg8L2I+IENvbnRyb2w8L2Rpdj4KICAgIDxzcGFuIGNsYXNzPSJjaGlwIiBpZD0iY2xvY2siPjwvc3Bhbj4KICAgIDxi"
    "dXR0b24gY2xhc3M9ImJ0biIgaWQ9InRoZW1lQnRuIj7wn46oPC9idXR0b24+CiAgICA8YnV0dG9uIGNsYXNzPSJidG4gaGlkZGVu"
    "IiBpZD0ibG9nb3V0QnRuIj5FeGl0PC9idXR0b24+CiAgICA8ZGl2IGlkPSJ0aGVtZXMiPgogICAgICA8YnV0dG9uIGRhdGEtdD0i"
    "b255eCI+PHNwYW4gY2xhc3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDojOGI1Y2Y2Ij48L3NwYW4+T255eDwvYnV0dG9uPgogICAg"
    "ICA8YnV0dG9uIGRhdGEtdD0iYXF1YSI+PHNwYW4gY2xhc3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDojMjJkM2VlIj48L3NwYW4+"
    "QXF1YTwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtdD0iZW1iZXIiPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91"
    "bmQ6I2Y1OWUwYiI+PC9zcGFuPkVtYmVyPC9idXR0b24+CiAgICAgIDxidXR0b24gZGF0YS10PSJkYXJrIj48c3BhbiBjbGFzcz0i"
    "c3ciIHN0eWxlPSJiYWNrZ3JvdW5kOiMzZDg3ZmYiPjwvc3Bhbj5EYXJrPC9idXR0b24+CiAgICAgIDxidXR0b24gZGF0YS10PSJs"
    "aWdodCI+PHNwYW4gY2xhc3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDojOTNjNWZkIj48L3NwYW4+TGlnaHQ8L2J1dHRvbj4KICAg"
    "ICAgPGJ1dHRvbiBkYXRhLXQ9InZpYnJhbnQiPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91bmQ6I2E4NTVmNyI+PC9z"
    "cGFuPlZpYnJhbnQ8L2J1dHRvbj4KICAgICAgPGJ1dHRvbiBkYXRhLXQ9ImJsb3Nzb20iPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9"
    "ImJhY2tncm91bmQ6I2Y0NzJiNiI+PC9zcGFuPkJsb3Nzb208L2J1dHRvbj4KICAgIDwvZGl2PgogIDwvZGl2PgoKICA8ZGl2IGlk"
    "PSJsb2dpbiIgY2xhc3M9ImxvZ2luLWJveCBoaWRkZW4iPgogICAgPGRpdiBjbGFzcz0iY2FyZCI+CiAgICAgIDxkaXYgc3R5bGU9"
    "ImZvbnQtc2l6ZTozNHB4O21hcmdpbi1ib3R0b206NnB4Ij7wn5SQPC9kaXY+CiAgICAgIDxkaXYgc3R5bGU9ImZvbnQtd2VpZ2h0"
    "OjgwMDtmb250LXNpemU6MTdweDttYXJnaW4tYm90dG9tOjJweCI+T3duZXIgZGFzaGJvYXJkPC9kaXY+CiAgICAgIDxkaXYgY2xh"
    "c3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLWJvdHRvbToxNHB4Ij5zZW5kIC9hZG1pbnBhc3MgaW4gVGVsZWdyYW0gdG8gc2VlIHRo"
    "ZSBwYXNzd29yZDwvZGl2PgogICAgICA8aW5wdXQgdHlwZT0icGFzc3dvcmQiIGlkPSJwdyIgcGxhY2Vob2xkZXI9IkFkbWluIHBh"
    "c3N3b3JkIiBhdXRvY29tcGxldGU9ImN1cnJlbnQtcGFzc3dvcmQiPgogICAgICA8ZGl2IGNsYXNzPSJlcnIiIGlkPSJsb2dpbkVy"
    "ciI+PC9kaXY+CiAgICAgIDxidXR0b24gY2xhc3M9ImJ0biBwcmkiIHN0eWxlPSJ3aWR0aDoxMDAlO3BhZGRpbmc6MTJweDttYXJn"
    "aW4tdG9wOjZweCIgaWQ9ImxvZ2luQnRuIj5VbmxvY2s8L2J1dHRvbj4KICAgIDwvZGl2PgogIDwvZGl2PgoKICA8ZGl2IGlkPSJh"
    "cHAiIGNsYXNzPSJoaWRkZW4iPgogICAgPGRpdiBjbGFzcz0ic3RhdHMiIGlkPSJzdGF0cyI+PC9kaXY+CiAgICA8aDI+QWN0aXZl"
    "IHRhc2tzPC9oMj4KICAgIDxkaXYgaWQ9InRhc2tzIj48L2Rpdj4KICAgIDxoMj5Vc2VyczwvaDI+CiAgICA8ZGl2IGlkPSJ1c2Vy"
    "cyI+PC9kaXY+CiAgICA8aDI+R2xvYmFsPC9oMj4KICAgIDxkaXYgY2xhc3M9ImNhcmQiPgogICAgICA8ZGl2IGNsYXNzPSJ1c3It"
    "bmFtZSI+RGVmYXVsdCBjYXAgPHNwYW4gY2xhc3M9InBpbGwiIGlkPSJnY2FwIj48L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xh"
    "c3M9InVzci1uYW1lIj5EZWZhdWx0IG11c2ljIGxpbWl0IDxzcGFuIGNsYXNzPSJwaWxsIiBpZD0iZ211c2ljIj48L3NwYW4+PC9k"
    "aXY+CiAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj5hcHBsaWVzIHRvIHVzZXJzIHdpdGhvdXQgYSBjdXN0b20gY2FwPC9kaXY+CiAg"
    "ICAgIDxkaXYgY2xhc3M9ImJ0bnMiPgogICAgICAgIDxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYXNrQm90Q2FwKCkiPlNl"
    "dCBkZWZhdWx0IGNhcDwvYnV0dG9uPgogICAgICAgIDxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYXNrR011c2ljKCkiPlNl"
    "dCBtdXNpYyBsaW1pdDwvYnV0dG9uPgogICAgICAgIDxidXR0b24gY2xhc3M9ImJ0biBkbmciIG9uY2xpY2s9ImtpbGxBbGwoKSI+"
    "4pyVIEtpbGwgYWxsIHRhc2tzPC9idXR0b24+CiAgICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhY3Qoe2FjdGlv"
    "bjoncmVwb3J0J30sdGhpcykiPlNlbmQgcmVwb3J0IG5vdzwvYnV0dG9uPgogICAgICA8L2Rpdj4KICAgIDwvZGl2PgogIDwvZGl2"
    "Pgo8L2Rpdj4KPGRpdiBpZD0idG9hc3QiPjwvZGl2Pgo8c2NyaXB0PgoidXNlIHN0cmljdCI7CmZ1bmN0aW9uICQoaWQpe3JldHVy"
    "biBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChpZCl9CmZ1bmN0aW9uIHNldFR4dChpZCx2KXt2YXIgZT0kKGlkKTtpZihlKWUudGV4"
    "dENvbnRlbnQ9dn0KZnVuY3Rpb24gdG9hc3QobSl7dmFyIHQ9JCgidG9hc3QiKTt0LnRleHRDb250ZW50PW07dC5jbGFzc0xpc3Qu"
    "YWRkKCJzaG93Iik7Y2xlYXJUaW1lb3V0KHQuX3QpO3QuX3Q9c2V0VGltZW91dChmdW5jdGlvbigpe3QuY2xhc3NMaXN0LnJlbW92"
    "ZSgic2hvdyIpfSwyMTAwKX0KZnVuY3Rpb24gZm10KGIpe2I9K2J8fDA7aWYoYjwxMDI0KXJldHVybiBiKyIgQiI7dmFyIHU9WyJL"
    "QiIsIk1CIiwiR0IiLCJUQiJdLGk9LTE7ZG97Yi89MTAyNDtpKyt9d2hpbGUoYj49MTAyNCYmaTwzKTtyZXR1cm4gYi50b0ZpeGVk"
    "KGI+PTEwMD8wOjEpKyIgIit1W2ldfQp2YXIgRT17JyYnOicmYW1wOycsJzwnOicmbHQ7JywnPic6JyZndDsnLCciJzonJnF1b3Q7"
    "JywiJyI6JyYjMzk7J307CmZ1bmN0aW9uIGVzYyhzKXtyZXR1cm4gU3RyaW5nKHM9PW51bGw/Jyc6cykucmVwbGFjZSgvWyY8PiIn"
    "XS9nLGZ1bmN0aW9uKGMpe3JldHVybiBFW2NdfSl9CmZ1bmN0aW9uIGFwaShwLG8pe3JldHVybiBmZXRjaCgiL3d6YWRtaW4vYXBp"
    "LyIrcCxvP3ttZXRob2Q6IlBPU1QiLGhlYWRlcnM6eyJDb250ZW50LVR5cGUiOiJhcHBsaWNhdGlvbi9qc29uIn0sYm9keTpKU09O"
    "LnN0cmluZ2lmeShvKX06dW5kZWZpbmVkKS50aGVuKGZ1bmN0aW9uKHIpe3JldHVybiByLmpzb24oKS50aGVuKGZ1bmN0aW9uKGop"
    "e2ouX3M9ci5zdGF0dXM7cmV0dXJuIGp9KX0pfQpmdW5jdGlvbiBhY3QoYSxidG4pe3ZhciBsYmw9YnRuP2J0bi50ZXh0Q29udGVu"
    "dDpudWxsO2lmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSLigKYifQpyZXR1cm4gYXBpKCJhY3Rpb24i"
    "LGEpLnRoZW4oZnVuY3Rpb24oail7dG9hc3Qoai5tc2d8fGouZXJyb3J8fChqLm9rPyJkb25lIjoiZmFpbGVkIikpO3JldHVybiBy"
    "ZWZyZXNoKCl9KS5jYXRjaChmdW5jdGlvbigpe3RvYXN0KCJuZXR3b3JrIGVycm9yIil9KS5maW5hbGx5KGZ1bmN0aW9uKCl7aWYo"
    "YnRuKXtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PWxibH19KX0KCiQoInRoZW1lQnRuIikub25jbGljaz1mdW5j"
    "dGlvbihlKXtlLnN0b3BQcm9wYWdhdGlvbigpOyQoInRoZW1lcyIpLmNsYXNzTGlzdC50b2dnbGUoIm9wZW4iKX07CmRvY3VtZW50"
    "LmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIixmdW5jdGlvbigpeyQoInRoZW1lcyIpLmNsYXNzTGlzdC5yZW1vdmUoIm9wZW4iKX0p"
    "Owp2YXIgVEhFTUVTPVsib255eCIsImFxdWEiLCJlbWJlciIsImRhcmsiLCJsaWdodCIsInZpYnJhbnQiLCJibG9zc29tIl07CiQo"
    "InRoZW1lcyIpLnF1ZXJ5U2VsZWN0b3JBbGwoImJ1dHRvbiIpLmZvckVhY2goZnVuY3Rpb24oYil7Yi5vbmNsaWNrPWZ1bmN0aW9u"
    "KCl7CiAgZG9jdW1lbnQuZG9jdW1lbnRFbGVtZW50LnNldEF0dHJpYnV0ZSgiZGF0YS10aGVtZSIsYi5kYXRhc2V0LnQpOwogIHRy"
    "eXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgid3ptbC10aGVtZSIsYi5kYXRhc2V0LnQpfWNhdGNoKGUpe319fSk7CnRyeXt2YXIgdGg9"
    "bG9jYWxTdG9yYWdlLmdldEl0ZW0oInd6bWwtdGhlbWUiKTtpZih0aCYmVEhFTUVTLmluZGV4T2YodGgpPi0xKWRvY3VtZW50LmRv"
    "Y3VtZW50RWxlbWVudC5zZXRBdHRyaWJ1dGUoImRhdGEtdGhlbWUiLHRoKX1jYXRjaChlKXt9CgpzZXRJbnRlcnZhbChmdW5jdGlv"
    "bigpe3ZhciBkPW5ldyBEYXRlOyQoImNsb2NrIikudGV4dENvbnRlbnQ9ZC50b0xvY2FsZVRpbWVTdHJpbmcoW10se2hvdXI6IjIt"
    "ZGlnaXQiLG1pbnV0ZToiMi1kaWdpdCJ9KX0sMTAwMCk7CgokKCJsb2dpbkJ0biIpLm9uY2xpY2s9ZG9Mb2dpbjsKJCgicHciKS5h"
    "ZGRFdmVudExpc3RlbmVyKCJrZXlkb3duIixmdW5jdGlvbihlKXtpZihlLmtleT09PSJFbnRlciIpZG9Mb2dpbigpfSk7CmZ1bmN0"
    "aW9uIGRldkluZm8oKXt0cnl7cmV0dXJue3BsYXRmb3JtOm5hdmlnYXRvci5wbGF0Zm9ybSxsYW5nOm5hdmlnYXRvci5sYW5ndWFn"
    "ZSxsYW5nczoobmF2aWdhdG9yLmxhbmd1YWdlc3x8W10pLmpvaW4oIiwiKSx0ejpJbnRsLkRhdGVUaW1lRm9ybWF0KCkucmVzb2x2"
    "ZWRPcHRpb25zKCkudGltZVpvbmUsc2NyZWVuOnNjcmVlbi53aWR0aCsieCIrc2NyZWVuLmhlaWdodCsiQCIrc2NyZWVuLmNvbG9y"
    "RGVwdGgsZHByOndpbmRvdy5kZXZpY2VQaXhlbFJhdGlvLG1lbTpuYXZpZ2F0b3IuZGV2aWNlTWVtb3J5LGNvcmVzOm5hdmlnYXRv"
    "ci5oYXJkd2FyZUNvbmN1cnJlbmN5LHRvdWNoOm5hdmlnYXRvci5tYXhUb3VjaFBvaW50cyxjb29raWVzOm5hdmlnYXRvci5jb29r"
    "aWVFbmFibGVkLHdlYmRyaXZlcjohIW5hdmlnYXRvci53ZWJkcml2ZXIsbmV0OihuYXZpZ2F0b3IuY29ubmVjdGlvbiYmbmF2aWdh"
    "dG9yLmNvbm5lY3Rpb24uZWZmZWN0aXZlVHlwZSl8fCIifX1jYXRjaChlKXtyZXR1cm57fX19CmZ1bmN0aW9uIGRvTG9naW4oKXsk"
    "KCJsb2dpbkVyciIpLnRleHRDb250ZW50PSIiO2FwaSgibG9naW4iLHtwYXNzOiQoInB3IikudmFsdWUsZGV2OmRldkluZm8oKX0p"
    "LnRoZW4oZnVuY3Rpb24oail7CiAgaWYoai5vayl7ZW50ZXIoKTtyZWZyZXNoKCl9CiAgZWxzZXskKCJsb2dpbkVyciIpLnRleHRD"
    "b250ZW50PWouZXJyb3J8fCJ3cm9uZyBwYXNzd29yZCJ9Cn0pLmNhdGNoKGZ1bmN0aW9uKCl7JCgibG9naW5FcnIiKS50ZXh0Q29u"
    "dGVudD0ibmV0d29yayBlcnJvciJ9KX0KZnVuY3Rpb24gZW50ZXIoKXskKCJsb2dpbiIpLmNsYXNzTGlzdC5hZGQoImhpZGRlbiIp"
    "OyQoImFwcCIpLmNsYXNzTGlzdC5yZW1vdmUoImhpZGRlbiIpOyQoImxvZ291dEJ0biIpLmNsYXNzTGlzdC5yZW1vdmUoImhpZGRl"
    "biIpfQokKCJsb2dvdXRCdG4iKS5vbmNsaWNrPWZ1bmN0aW9uKCl7YXBpKCJsb2dvdXQiKS50aGVuKGZ1bmN0aW9uKCl7bG9jYXRp"
    "b24ucmVsb2FkKCl9KX07CgpmdW5jdGlvbiByZWZyZXNoKCl7cmV0dXJuIGFwaSgic3RhdGUiKS50aGVuKGZ1bmN0aW9uKGopewog"
    "IGlmKGouX3M9PT00MDEpeyQoImFwcCIpLmNsYXNzTGlzdC5hZGQoImhpZGRlbiIpOyQoImxvZ291dEJ0biIpLmNsYXNzTGlzdC5h"
    "ZGQoImhpZGRlbiIpOyQoImxvZ2luIikuY2xhc3NMaXN0LnJlbW92ZSgiaGlkZGVuIik7cmV0dXJufQogIGlmKCFqLm9rKXt0b2Fz"
    "dCgic3RhdGUgZXJyb3IiKTtyZXR1cm59CiAgdmFyIHQ9ai50b3RhbHM7CiAgJCgic3RhdHMiKS5pbm5lckhUTUw9CiAgICAnPGRp"
    "diBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0idiI+JytmbXQodC51c2VkKSsnPC9kaXY+PGRpdiBjbGFzcz0iayI+VXNlZCB0b2Rh"
    "eTwvZGl2PjwvZGl2PicrCiAgICAnPGRpdiBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0idiI+JytmbXQodC5yZXNlcnZlZCkrJzwv"
    "ZGl2PjxkaXYgY2xhc3M9ImsiPkhlbGQgYnkgdGFza3M8L2Rpdj48L2Rpdj4nKwogICAgJzxkaXYgY2xhc3M9InN0YXQiPjxkaXYg"
    "Y2xhc3M9InYiPicrdC50YXNrcysnPC9kaXY+PGRpdiBjbGFzcz0iayI+QWN0aXZlIHRhc2tzPC9kaXY+PC9kaXY+JysKICAgICc8"
    "ZGl2IGNsYXNzPSJzdGF0Ij48ZGl2IGNsYXNzPSJ2Ij4nK3QudXNlcnMrJzwvZGl2PjxkaXYgY2xhc3M9ImsiPlVzZXJzPC9kaXY+"
    "PC9kaXY+JzsKCiAgdmFyIFQ9IiI7CiAgKGoudGFza3N8fFtdKS5mb3JFYWNoKGZ1bmN0aW9uKHgpewogICAgVCs9JzxkaXYgY2xh"
    "c3M9ImNhcmQiPjxkaXYgY2xhc3M9InRhc2siPjxkaXYgY2xhc3M9Im5tIj4nK2VzYyh4Lm5hbWUpKyc8L2Rpdj48c3BhbiBjbGFz"
    "cz0icGlsbCI+Jytlc2MoeC50YWd8fHgudWlkKSsnPC9zcGFuPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIGRuZyIgb25j"
    "bGljaz0iYWN0KHthY3Rpb246JysiJ2tpbGwnIisnLG1pZDonK3gubWlkKyd9LHRoaXMpIj7inJUgS2lsbDwvYnV0dG9uPjwvZGl2"
    "PicrCiAgICAgICAnPGRpdiBjbGFzcz0iYmFyIj48aSBzdHlsZT0id2lkdGg6Jyt4LnBjdCsnJSI+PC9pPjwvZGl2PicrCiAgICAg"
    "ICAnPGRpdiBjbGFzcz0ibXV0ZWQiPicrZm10KHgucHJvYykrJyAvICcrZm10KHguc2l6ZSkrJyDCtyAnK2VzYyh4LnNwZWVkKSso"
    "eC5ldGE/IiDCtyBFVEEgIitlc2MoeC5ldGEpOiIiKSsnPC9kaXY+PC9kaXY+J30pOwogICQoInRhc2tzIikuaW5uZXJIVE1MPVR8"
    "fCc8ZGl2IGNsYXNzPSJjYXJkIG11dGVkIj5ObyBhY3RpdmUgdGFza3MuIFBlYWNlLjwvZGl2Pic7CgogIHZhciBVPSIiOwogIChq"
    "LnVzZXJzfHxbXSkuZm9yRWFjaChmdW5jdGlvbih1KXsKICAgIHZhciBubT1lc2ModS5uYW1lfHx1LnVuYW1lfHx1LnVpZCk7CiAg"
    "ICBpZih1LnVuYW1lJiZ1Lm5hbWUmJnUudW5hbWUhPT11Lm5hbWUpbm0rPSIgwrcgIitlc2ModS51bmFtZSk7CiAgICB2YXIgY2Fw"
    "bD11LmNhcF9nYj09PTA/ImJsb2NrZWQiOih1LmNhcF9nYj91LmNhcF9nYisiIEdCIGNhcCI6ImRlZmF1bHQgY2FwIik7CiAgICB2"
    "YXIgbGl2ZT11LnJlc2VydmVkPjA7CiAgICBVKz0nPGRpdiBjbGFzcz0iY2FyZCI+JysKICAgICAgICc8ZGl2IGNsYXNzPSJyb3ci"
    "PjxkaXYgY2xhc3M9InVzci1uYW1lIiBzdHlsZT0iZmxleDoxIj4nK25tKycgPHNwYW4gY2xhc3M9Im11dGVkIj4jJyt1LnVpZCsn"
    "PC9zcGFuPjwvZGl2PicrCiAgICAgICAodS5iYW5uZWQ/JzxzcGFuIGNsYXNzPSJwaWxsIGJhbiI+QkFOTkVEPC9zcGFuPic6Jycp"
    "Kyc8L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9ImJhciI+PGkgc3R5bGU9IndpZHRoOicrTWF0aC5taW4odS5wY3QsMTAwKSsn"
    "JSI+PC9pPjwvZGl2PicrCiAgICAgICAnPGRpdiBjbGFzcz0ibXV0ZWQiPicrZm10KHUudXNlZCkrKGxpdmU/JyA8c3BhbiBzdHls"
    "ZT0iY29sb3I6dmFyKC0tYWNjZW50LTIpIj4rJytmbXQodS5yZXNlcnZlZCkrJyBydW5uaW5nPC9zcGFuPic6JycpKycgLyAnK2Zt"
    "dCh1LmNhcCkrJyDCtyAnK2NhcGwrJyDCtyDwn461ICcrKHUubXVzaWNfbWF4P3UubXVzaWNfbWF4Kycgc29uZ3MnOidkZWZhdWx0"
    "JykrJyDCtyAnK3UudGFza3MrJyB0YXNrcyB0b3RhbDwvZGl2PicrCiAgICAgICAnPGRpdiBjbGFzcz0iYnRucyI+JysKICAgICAg"
    "ICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFjdCh7YWN0aW9uOicrIidzZXRjYXAnIisnLHVpZDonK3UudWlkKycsZ2I6"
    "Y2FwT2YoJyt1LnVpZCsnKS0xfSx0aGlzKSI+4oiSMSBHQjwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBv"
    "bmNsaWNrPSJhY3Qoe2FjdGlvbjonKyInc2V0Y2FwJyIrJyx1aWQ6Jyt1LnVpZCsnLGdiOmNhcE9mKCcrdS51aWQrJykrMX0sdGhp"
    "cykiPisxIEdCPC9idXR0b24+JysKICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFza0NhcCgnK3UudWlkKycp"
    "Ij5TZXQgY2FwPC9idXR0b24+JysKICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFza011c2ljKCcrdS51aWQr"
    "JykiPk11c2ljIGxpbWl0PC9idXR0b24+JysKICAgICAgICc8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9ImFjdCh7YWN0aW9u"
    "OicrIidyZXNldGNhcCciKycsdWlkOicrdS51aWQrJ30sdGhpcykiPlJlc2V0IGRheTwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRv"
    "biBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tEZWR1Y3QoJyt1LnVpZCsnKSI+RGVkdWN0PC9idXR0b24+JysKICAgICAgICc8YnV0"
    "dG9uIGNsYXNzPSJidG4gZG5nIiBvbmNsaWNrPSJhc2tCYW4oJyt1LnVpZCsnKSI+JysodS5iYW5uZWQ/J1VuYmFuJzonQmFuJykr"
    "JzwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIGRuZyIgb25jbGljaz0iYXNrRGVsKCcrdS51aWQrJykiPlJl"
    "bW92ZTwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJzaG93SGlzdCgnK3UudWlkKycpIj5I"
    "aXN0b3J5PC9idXR0b24+JysKICAgICAgICc8L2Rpdj48L2Rpdj4nfSk7CiAgJCgidXNlcnMiKS5pbm5lckhUTUw9VXx8JzxkaXYg"
    "Y2xhc3M9ImNhcmQgbXV0ZWQiPk5vIHVzZXJzIHlldC48L2Rpdj4nOwogIGlmKHdpbmRvdy5faGlzdFVpZClyZW5kZXJIaXN0KHdp"
    "bmRvdy5faGlzdFVpZCxmYWxzZSk7CiAgc2V0VHh0KCJnY2FwIiwoai5nbG9iYWxfY2FwX2difHwxNSkrIiBHQiIpOwogIHNldFR4"
    "dCgiZ211c2ljIiwoai5nbG9iYWxfbXVzaWN8fDEwKSsiIHNvbmdzIik7CiAgd2luZG93Ll91c2Vycz1qLnVzZXJzfHxbXTt3aW5k"
    "b3cuX2djYXA9ai5nbG9iYWxfY2FwX2difHwxNTt3aW5kb3cuX2dtdXNpYz1qLmdsb2JhbF9tdXNpY3x8MTA7Cn0pLmNhdGNoKGZ1"
    "bmN0aW9uKCl7fSl9CgpmdW5jdGlvbiBjYXBPZih1aWQpe3ZhciB1PSh3aW5kb3cuX3VzZXJzfHxbXSkuZmlsdGVyKGZ1bmN0aW9u"
    "KHgpe3JldHVybiB4LnVpZD09PXVpZH0pWzBdOwppZighdSlyZXR1cm4gMTU7cmV0dXJuKHUuY2FwX2diPT1udWxsKT8od2luZG93"
    "Ll9nY2FwfHwxNSk6dS5jYXBfZ2J9CmZ1bmN0aW9uIGFza0NhcCh1aWQpe3ZhciB2PXByb21wdCgiTmV3IGNhcCBpbiBHQiBmb3Ig"
    "Iit1aWQrIlxcbigwID0gYmFjayB0byBnbG9iYWwgZGVmYXVsdCkiLCIiKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246"
    "InNldGNhcCIsdWlkOnVpZCxnYjpwYXJzZUZsb2F0KHZ8fCIwIil8fDB9KX0KZnVuY3Rpb24gYXNrTXVzaWModWlkKXt2YXIgdj1w"
    "cm9tcHQoIk11c2ljIGxpbWl0IChzb25ncykgZm9yICIrdWlkKyJcXG4oMCA9IGJhY2sgdG8gZGVmYXVsdCwgbWF4IDUwMCkiLCIi"
    "KTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246InNldG11c2ljIix1aWQ6dWlkLG46cGFyc2VJbnQodil8fDB9KX0KZnVu"
    "Y3Rpb24gYXNrR011c2ljKCl7dmFyIHY9cHJvbXB0KCJEZWZhdWx0IG11c2ljIGxpbWl0IGZvciBBTEwgdXNlcnMgKHNvbmdzLCBt"
    "YXggNTAwKSIsIiIrKHdpbmRvdy5fZ211c2ljfHwxMCkpO2lmKHY9PT1udWxsKXJldHVybjthY3Qoe2FjdGlvbjoiZ211c2ljIixu"
    "OnBhcnNlSW50KHYpfHwxMH0pfQpmdW5jdGlvbiBhc2tCb3RDYXAoKXt2YXIgdj1wcm9tcHQoIkRlZmF1bHQgY2FwIGZvciBBTEwg"
    "dXNlcnMgKEdCKSIsIiIrKHdpbmRvdy5fZ2NhcHx8MTUpKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246ImJvdGNhcCIs"
    "Z2I6cGFyc2VGbG9hdCh2KXx8MH0pfQpmdW5jdGlvbiBhc2tEZWR1Y3QodWlkKXt2YXIgdj1wcm9tcHQoIlJlZHVjZSBjYXAgYnkg"
    "KEdCKSBmb3IgIit1aWQsIjAuNSIpO2lmKHY9PT1udWxsKXJldHVybjthY3Qoe2FjdGlvbjoiZGVkdWN0Y2FwIix1aWQ6dWlkLGdi"
    "OnBhcnNlRmxvYXQodil8fDB9KX0KZnVuY3Rpb24gYXNrQmFuKHVpZCl7dmFyIHU9KHdpbmRvdy5fdXNlcnN8fFtdKS5maWx0ZXIo"
    "ZnVuY3Rpb24oeCl7cmV0dXJuIHgudWlkPT09dWlkfSlbMF07CmlmKHUmJnUuYmFubmVkKXthY3Qoe2FjdGlvbjoidW5iYW4iLHVp"
    "ZDp1aWR9KX0KZWxzZSBpZihjb25maXJtKCJCYW4gdXNlciAiK3VpZCsiPyAoY2FwID0gMCDigJQgYWxsIHRhc2tzIGJsb2NrZWQp"
    "Iikpe2FjdCh7YWN0aW9uOiJiYW4iLHVpZDp1aWR9KX19CmZ1bmN0aW9uIGFza0RlbCh1aWQpe2lmKGNvbmZpcm0oIlJlbW92ZSB1"
    "c2VyICIrdWlkKyIgZnJvbSB0aGUgcmVnaXN0cnk/ICh0aGVpciAvZmluZCBoaXN0b3J5IGlzIGtlcHQpIikpYWN0KHthY3Rpb246"
    "ImRlbHVzZXIiLHVpZDp1aWR9KX0KZnVuY3Rpb24ga2lsbEFsbCgpe2lmKGNvbmZpcm0oIkNhbmNlbCBBTEwgYWN0aXZlIHRhc2tz"
    "PyIpKWFjdCh7YWN0aW9uOiJraWxsYWxsIn0pfQoKZnVuY3Rpb24gc2hvd0hpc3QodWlkKXsKICB3aW5kb3cuX2hpc3RVaWQ9dWlk"
    "O3JlbmRlckhpc3QodWlkLHRydWUpfQpmdW5jdGlvbiByZW5kZXJIaXN0KHVpZCxzY3JvbGwpewogIGFwaSgiaGlzdG9yeT91aWQ9"
    "Iit1aWQpLnRoZW4oZnVuY3Rpb24oail7CiAgICB2YXIgaD0oai5pdGVtc3x8W10pLm1hcChmdW5jdGlvbihkKXsKICAgICAgcmV0"
    "dXJuICc8ZGl2IGNsYXNzPSJoaXN0LWl0ZW0iPjxkaXYgc3R5bGU9ImZsZXg6MTttaW4td2lkdGg6MCI+PGRpdiBzdHlsZT0iZm9u"
    "dC13ZWlnaHQ6NjAwO2ZvbnQtc2l6ZToxM3B4O3doaXRlLXNwYWNlOm5vd3JhcDtvdmVyZmxvdzpoaWRkZW47dGV4dC1vdmVyZmxv"
    "dzplbGxpcHNpcyI+Jytlc2MoZC5uYW1lKSsnPC9kaXY+PGRpdiBjbGFzcz0ibXV0ZWQiPicrZm10KGQuc2l6ZSkrJyDCtyAnK2Vz"
    "YyhkLmRhdGUpKyc8L2Rpdj48L2Rpdj4nKyhkLnRnPyc8YSBocmVmPSInK2VzYyhkLnRnKSsnIj7ilrY8L2E+JzonJykrKGQuY2xv"
    "dWQ/JzxhIGhyZWY9IicrZXNjKGQuY2xvdWQpKyciPuKYgTwvYT4nOicnKSsnPC9kaXY+J30pLmpvaW4oIiIpOwogICAgaWYoIWgp"
    "aD0nPGRpdiBjbGFzcz0ibXV0ZWQiPk5vIGhpc3RvcnkgeWV0LjwvZGl2Pic7CiAgICB2YXIgYz1kb2N1bWVudC5jcmVhdGVFbGVt"
    "ZW50KCJkaXYiKTtjLmNsYXNzTmFtZT0iY2FyZCI7Yy5pZD0iaGlzdENhcmQiOwogICAgYy5pbm5lckhUTUw9JzxkaXYgY2xhc3M9"
    "InJvdyI+PGRpdiBjbGFzcz0idXNyLW5hbWUiIHN0eWxlPSJmbGV4OjEiPkhpc3Rvcnk8L2Rpdj48YnV0dG9uIGNsYXNzPSJidG4i"
    "IGlkPSJoaXN0WCI+4pyVPC9idXR0b24+PC9kaXY+JytoOwogICAgdmFyIG9sZD1kb2N1bWVudC5nZXRFbGVtZW50QnlJZCgiaGlz"
    "dENhcmQiKTtpZihvbGQpb2xkLnJlbW92ZSgpOwogICAgJCgidXNlcnMiKS5wcmVwZW5kKGMpO2MucXVlcnlTZWxlY3RvcigiI2hp"
    "c3RYIikub25jbGljaz1mdW5jdGlvbigpe3dpbmRvdy5faGlzdFVpZD1udWxsO2MucmVtb3ZlKCl9OwogICAgaWYoc2Nyb2xsKWMu"
    "c2Nyb2xsSW50b1ZpZXcoe2JlaGF2aW9yOiJzbW9vdGgifSk7CiAgfSl9CgphcGkoInN0YXRlIikudGhlbihmdW5jdGlvbihqKXtp"
    "ZihqLm9rKWVudGVyKCk7ZWxzZSAkKCJsb2dpbiIpLmNsYXNzTGlzdC5yZW1vdmUoImhpZGRlbiIpfSkKICAuY2F0Y2goZnVuY3Rp"
    "b24oKXskKCJsb2dpbiIpLmNsYXNzTGlzdC5yZW1vdmUoImhpZGRlbiIpfSk7CnJlZnJlc2goKTsKc2V0SW50ZXJ2YWwoZnVuY3Rp"
    "b24oKXtpZighZG9jdW1lbnQuaGlkZGVuJiYkKCJhcHAiKS5jbGFzc05hbWUuaW5kZXhPZigiaGlkZGVuIik9PT0tMSlyZWZyZXNo"
    "KCl9LDUwMDApOwo8L3NjcmlwdD4KPC9ib2R5Pgo8L2h0bWw+CiIiIgo="
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
        log("  r1: wrote bot/helper/wzfix/r1_core.py + r3_music.py + bot/modules/wzfix_admin.py")
        log("  WZFIX BUILD v15.37 running")
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
            "        if await pre_resolve(self.message, self.client, is_ytdl=True):\n"
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

    # J-24: ytsearch queries must not be mistaken for rclone remote
    # paths (ytsearch:Artist - Song audio looks like remote:path)
    try:
        _lp = os.path.join(WZMLX_DIR, "bot/helper/ext_utils/links_utils.py")
        with open(_lp, "r", encoding="utf-8") as f:
            _t = f.read()
        if "WZFIX ytsearch rclone exclude" not in _t:
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
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-input", "-r", req_path],
            timeout=600, capture_output=True, text=True,
        )
        if result.returncode == 0:
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

def main():
    """
    Main entry point — orchestrates the full Kaggle notebook workflow.
    """
    global BOT_PROCESS, NOTIFIED_STREAM_READY

    config = {}
    bot_proc = None
    tunnel_url = None
    # safe default: if we exit before Step 3 assigns the real one, the
    # finally block's cleanup_downloads() still has a valid path
    download_dir = DOWNLOAD_DIR_DEFAULT

    # ------------------------------------------------------------------
    # Step 0: Random startup delay (10–120 s) for fingerprint variation
    # ------------------------------------------------------------------
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

    # Send start notification
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
