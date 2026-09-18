#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 kaggle_notebook.py — WZML-X Telegram Bot Runner for Kaggle
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
    "cm4gX293bmVyX3VzZXJuYW1lIG9yIE5vbmUKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIFF1b3RhCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIF9nZXRfZ2xvYmFsX2NhcF9nYigpOgogICAgdHJ5Ogog"
    "ICAgICAgIGRvYyA9IGF3YWl0IF9kYigpLnd6Zml4X2NvbmZpZ1tfcGFydCgpXS5maW5kX29uZSh7Il9pZCI6ICJnbG9iYWwifSkK"
    "ICAgICAgICBpZiBkb2MgYW5kIGRvYy5nZXQoImJvdF9jYXBfZ2IiKToKICAgICAgICAgICAgcmV0dXJuIGZsb2F0KGRvY1siYm90"
    "X2NhcF9nYiJdKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gV1pGSVhfREVGQVVMVF9DQVBf"
    "R0IKCgphc3luYyBkZWYgc2V0X2dsb2JhbF9jYXBfZ2IoZ2IpOgogICAgYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgIGF3YWl0IF9k"
    "YigpLnd6Zml4X2NvbmZpZ1tfcGFydCgpXS51cGRhdGVfb25lKAogICAgICAgIHsiX2lkIjogImdsb2JhbCJ9LCB7IiRzZXQiOiB7"
    "ImJvdF9jYXBfZ2IiOiBmbG9hdChnYil9fSwgdXBzZXJ0PVRydWUKICAgICkKCgphc3luYyBkZWYgZ2V0X3VzZXJfZG9jKHVzZXJf"
    "aWQpOgogICAgdHJ5OgogICAgICAgIHJldHVybiBhd2FpdCBfZGIoKS53emZpeF91c2Vyc1tfcGFydCgpXS5maW5kX29uZSh7Il9p"
    "ZCI6IHVzZXJfaWR9KSBvciB7fQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4ge30KCgphc3luYyBkZWYgZ2V0"
    "X2NhcF9ieXRlcyh1c2VyX2lkKToKICAgIGRvYyA9IGF3YWl0IGdldF91c2VyX2RvYyh1c2VyX2lkKQogICAgaWYgZG9jLmdldCgi"
    "Y2FwX2diIikgaXMgbm90IE5vbmU6CiAgICAgICAgcmV0dXJuIGZsb2F0KGRvY1siY2FwX2diIl0pICogR0IKICAgIHJldHVybiBh"
    "d2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKSAqIEdCCgoKYXN5bmMgZGVmIGdldF91c2FnZSh1c2VyX2lkLCBkYXk9Tm9uZSk6CiAg"
    "ICBkYXkgPSBkYXkgb3IgX2RheV9pc3QoKQogICAgdHJ5OgogICAgICAgIGRvYyA9IGF3YWl0IF9kYigpLnd6Zml4X3F1b3RhW19w"
    "YXJ0KCldLmZpbmRfb25lKHsiX2lkIjogZiJ7dXNlcl9pZH06e2RheX0ifSkKICAgICAgICByZXR1cm4gaW50KGRvYy5nZXQoInVz"
    "ZWQiLCAwKSkgaWYgZG9jIGVsc2UgMAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gMAoKCmFzeW5jIGRlZiBy"
    "ZXNldF91c2FnZSh1c2VyX2lkLCBkYXk9Tm9uZSk6CiAgICBkYXkgPSBkYXkgb3IgX2RheV9pc3QoKQogICAgYXdhaXQgX2RiKCku"
    "d3pmaXhfcXVvdGFbX3BhcnQoKV0uZGVsZXRlX29uZSh7Il9pZCI6IGYie3VzZXJfaWR9OntkYXl9In0pCgoKYXN5bmMgZGVmIHVz"
    "ZXJfZXhpc3RzKHVzZXJfaWQpOgogICAgdHJ5OgogICAgICAgIHJldHVybiAoCiAgICAgICAgICAgIGF3YWl0IF9kYigpLnd6Zml4"
    "X3VzZXJzW19wYXJ0KCldLmZpbmRfb25lKHsiX2lkIjogdXNlcl9pZH0sIHsiX2lkIjogMX0pCiAgICAgICAgICAgIGlzIG5vdCBO"
    "b25lCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gRmFsc2UKCgphc3luYyBkZWYgZGVsZXRl"
    "X3VzZXIodXNlcl9pZCk6CiAgICAiIiJSZW1vdmUgYSB1c2VyJ3MgcmVnaXN0cnkgZG9jICsgZXZlcnkgcXVvdGEgcm93LiBSZXR1"
    "cm5zIChyZWcsIHJvd3MpLiIiIgogICAgYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgIGRiID0gX2RiKCkKICAgIHBhcnQgPSBfcGFy"
    "dCgpCiAgICByZWcgPSAwCiAgICByb3dzID0gMAogICAgdHJ5OgogICAgICAgIHIgPSBhd2FpdCBkYi53emZpeF91c2Vyc1twYXJ0"
    "XS5kZWxldGVfb25lKHsiX2lkIjogdXNlcl9pZH0pCiAgICAgICAgcmVnID0gci5kZWxldGVkX2NvdW50CiAgICBleGNlcHQgRXhj"
    "ZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHRyeToKICAgICAgICByID0gYXdhaXQgZGIud3pmaXhfcXVvdGFbcGFydF0uZGVsZXRl"
    "X21hbnkoeyJfaWQiOiB7IiRyZWdleCI6IGYiXnt1c2VyX2lkfToifX0pCiAgICAgICAgcm93cyA9IHIuZGVsZXRlZF9jb3VudAog"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gcmVnLCByb3dzCgoKYXN5bmMgZGVmIHNldF91c2Vy"
    "X2NhcCh1c2VyX2lkLCBnYik6CiAgICAiIiJTZXQgYSBwZXItdXNlciBjYXAuIGdiPU5vbmUgcmVtb3ZlcyB0aGUgb3ZlcnJpZGUg"
    "KGJhY2sgdG8gZ2xvYmFsKS4iIiIKICAgIGF3YWl0IGVuc3VyZV9yZWFkeSgpCiAgICBpZiBnYiBpcyBOb25lOgogICAgICAgIGF3"
    "YWl0IF9kYigpLnd6Zml4X3VzZXJzW19wYXJ0KCldLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogdXNlcl9pZH0sIHsi"
    "JHVuc2V0IjogeyJjYXBfZ2IiOiAiIn19LCB1cHNlcnQ9VHJ1ZQogICAgICAgICkKICAgIGVsc2U6CiAgICAgICAgYXdhaXQgX2Ri"
    "KCkud3pmaXhfdXNlcnNbX3BhcnQoKV0udXBkYXRlX29uZSgKICAgICAgICAgICAgeyJfaWQiOiB1c2VyX2lkfSwgeyIkc2V0Ijog"
    "eyJjYXBfZ2IiOiBmbG9hdChnYil9fSwgdXBzZXJ0PVRydWUKICAgICAgICApCgoKYXN5bmMgZGVmIF9xdW90YV9ibG9ja19tc2co"
    "dXNlZCwgY2FwLCBzaXplLCBhdF9saW1pdCwgcmVzZXJ2ZWQ9MCk6CiAgICBvd25lciA9IGF3YWl0IGdldF9vd25lcl91c2VybmFt"
    "ZSgpCiAgICB1c2VkX2xpbmUgPSBmIuKUoCA8Yj5Vc2VkIHRvZGF5PC9iPiDihpIge19uaWNlX3NpemUodXNlZCl9IgogICAgaWYg"
    "cmVzZXJ2ZWQ6CiAgICAgICAgdXNlZF9saW5lICs9IGYiIDxpPigre19uaWNlX3NpemUocmVzZXJ2ZWQpfSBoZWxkIGJ5IHJ1bm5p"
    "bmcgdGFza3MpPC9pPiIKICAgIHVzZWRfbGluZSArPSBmIiAvIHtfbmljZV9zaXplKGNhcCl9IgogICAgbGluZXMgPSBbCiAgICAg"
    "ICAgIvCfmqsgPGI+RGFpbHkgYmFuZHdpZHRoIGxpbWl0IHJlYWNoZWQ8L2I+IiwKICAgICAgICAi4pSCIiwKICAgICAgICB1c2Vk"
    "X2xpbmUsCiAgICBdCiAgICBpZiBzaXplIGFuZCBub3QgYXRfbGltaXQ6CiAgICAgICAgbGluZXMuYXBwZW5kKAogICAgICAgICAg"
    "ICBmIuKUoCA8Yj5UaGlzIHRhc2s8L2I+IOKGkiB7X25pY2Vfc2l6ZShzaXplKX0gYmFuZHdpZHRoIChkb3dubG9hZCArIHVwbG9h"
    "ZCkg4oCUIGJpZ2dlciB0aGFuIHlvdXIgcmVtYWluaW5nIGFsbG93YW5jZSIKICAgICAgICApCiAgICBsaW5lcy5hcHBlbmQoIuKU"
    "gyIpCiAgICB3aG8gPSBmIkB7b3duZXJ9IiBpZiBvd25lciBlbHNlICJ0aGUgYm90IGFkbWluIgogICAgbGluZXMuYXBwZW5kKAog"
    "ICAgICAgIGYi4pSWIENvbnRhY3QgPGI+e3dob308L2I+IHRvIGluY3JlYXNlIHlvdXIgbGltaXQsIG9yIHdhaXQgdW50aWwgdG9t"
    "b3Jyb3cgKHJlc2V0cyAwMDowMCBJU1QpLiIKICAgICkKICAgIHJldHVybiAiXG4iLmpvaW4obGluZXMpCgoKYXN5bmMgZGVmIHJl"
    "c2VydmVkX3RvZGF5KHVzZXJfaWQsIGRheT1Ob25lKToKICAgICIiIlRvdGFsIGJhbmR3aWR0aCBjdXJyZW50bHkgaGVsZCBieSB0"
    "aGlzIHVzZXIncyBydW5uaW5nIHRhc2tzCiAgICAoYXRvbWljIGNvdW50ZXIgb24gdG9kYXkncyBxdW90YSBkb2N1bWVudCkuIiIi"
    "CiAgICBkYXkgPSBkYXkgb3IgX2RheV9pc3QoKQogICAgdHJ5OgogICAgICAgIGRvYyA9IGF3YWl0IF9kYigpLnd6Zml4X3F1b3Rh"
    "W19wYXJ0KCldLmZpbmRfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYie3VzZXJfaWR9OntkYXl9In0sIHsicmVzZXJ2ZWQiOiAx"
    "fQogICAgICAgICkKICAgICAgICByZXR1cm4gaW50KChkb2Mgb3Ige30pLmdldCgicmVzZXJ2ZWQiKSBvciAwKQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHJlc2VydmVkX3RvZGF5IGZhaWxlZCAodHJlYXRl"
    "ZCBhcyAwKToge2V9IikKICAgICAgICByZXR1cm4gMAoKCmFzeW5jIGRlZiBfdHJ5X2hvbGQodXNlcl9pZCwgYncsIGNhcD1Ob25l"
    "KToKICAgICIiIkF0b21pYyBiYW5kd2lkdGggaG9sZDogb25lIHNlcnZlci1zaWRlICRpbmMgKyB2ZXJpZnkgKyByb2xsYmFjay4K"
    "ICAgIFJldHVybnMgKHJlYXNvbiwgcWlkKSDigJQgcmVhc29uIGlzIE5vbmUgd2hlbiB0aGUgaG9sZCB3YXMgZ3JhbnRlZC4iIiIK"
    "ICAgIGlmIGJ3IDw9IDA6CiAgICAgICAgcmV0dXJuIE5vbmUsIE5vbmUKICAgIHRyeToKICAgICAgICBpZiBjYXAgaXMgTm9uZToK"
    "ICAgICAgICAgICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1c2VyX2lkKQogICAgICAgIHFpZCA9IGYie3VzZXJfaWR9Ontf"
    "ZGF5X2lzdCgpfSIKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9xdW90YVtfcGFydCgpXQogICAgICAgIGJlZm9yZSA9IGF3YWl0"
    "IGNvbC5maW5kX29uZV9hbmRfdXBkYXRlKAogICAgICAgICAgICB7Il9pZCI6IHFpZH0sCiAgICAgICAgICAgIHsKICAgICAgICAg"
    "ICAgICAgICIkaW5jIjogeyJyZXNlcnZlZCI6IGludChidyl9LAogICAgICAgICAgICAgICAgIiRzZXRPbkluc2VydCI6IHsKICAg"
    "ICAgICAgICAgICAgICAgICAidXNlZCI6IDAsCiAgICAgICAgICAgICAgICAgICAgImV4cGlyZUF0IjogZGF0ZXRpbWUuZnJvbXRp"
    "bWVzdGFtcCgKICAgICAgICAgICAgICAgICAgICAgICAgdGltZSgpICsgV1pGSVhfUVVPVEFfVFRMX0RBWVMgKiA4NjQwMCwgVVRD"
    "CiAgICAgICAgICAgICAgICAgICAgKSwKICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgIH0sCiAgICAgICAgICAgIHVwc2Vy"
    "dD1UcnVlLAogICAgICAgICAgICByZXR1cm5fZG9jdW1lbnQ9UmV0dXJuRG9jdW1lbnQuQkVGT1JFLAogICAgICAgICkKICAgICAg"
    "ICB1c2VkID0gaW50KChiZWZvcmUgb3Ige30pLmdldCgidXNlZCIpIG9yIDApCiAgICAgICAgcmVzX2IgPSBpbnQoKGJlZm9yZSBv"
    "ciB7fSkuZ2V0KCJyZXNlcnZlZCIpIG9yIDApCiAgICAgICAgaWYgdXNlZCArIHJlc19iID49IGNhcDoKICAgICAgICAgICAgcmVh"
    "c29uID0gImF0X2xpbWl0IgogICAgICAgIGVsaWYgdXNlZCArIHJlc19iICsgYncgPiBjYXAgKyBXWkZJWF9HUkFDRV9NQiAqIE1C"
    "OgogICAgICAgICAgICByZWFzb24gPSAib3ZlcnNob290IgogICAgICAgIGVsc2U6CiAgICAgICAgICAgIHJlYXNvbiA9IE5vbmUK"
    "ICAgICAgICBpZiByZWFzb246CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAog"
    "ICAgICAgICAgICAgICAgICAgIHsiX2lkIjogcWlkfSwgeyIkaW5jIjogeyJyZXNlcnZlZCI6IC1pbnQoYncpfX0KICAgICAgICAg"
    "ICAgICAgICkKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYi"
    "V1pGSVggcmVzZXJ2ZSByb2xsYmFjayBmYWlsZWQgKGhlYWxzIG9uIGJvb3QpOiB7ZX0iKQogICAgICAgICAgICByZXR1cm4gcmVh"
    "c29uLCBxaWQKICAgICAgICByZXR1cm4gTm9uZSwgcWlkCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VS"
    "LmVycm9yKGYiV1pGSVggX3RyeV9ob2xkIGZhaWxlZCAoYWxsb3dlZCk6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUsIE5vbmUK"
    "Cgphc3luYyBkZWYgcmVzZXJ2ZShsaXN0ZW5lciwgYncsIGNhcD1Ob25lKToKICAgICIiIkhvbGQgYmFuZHdpZHRoIGZvciBhIHJ1"
    "bm5pbmcgdGFzayAoYXRvbWljIOKAlCBzZWUgX3RyeV9ob2xkKS4iIiIKICAgIGlmIGJ3IDw9IDA6CiAgICAgICAgcmV0dXJuIE5v"
    "bmUKICAgIHRyeToKICAgICAgICBpZiBnZXRhdHRyKGxpc3RlbmVyLCAiX3d6Zml4X3Jlc3YiLCBOb25lKToKICAgICAgICAgICAg"
    "YXdhaXQgcmVsZWFzZV9yZXNlcnZlKGxpc3RlbmVyKSAgIyByZS1ldmFsdWF0aW9uIHJlcGxhY2VzIHRoZSBob2xkCiAgICAgICAg"
    "cmVhc29uLCBxaWQgPSBhd2FpdCBfdHJ5X2hvbGQobGlzdGVuZXIudXNlcl9pZCwgYncsIGNhcCkKICAgICAgICBpZiByZWFzb246"
    "CiAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgZiJXWkZJWCByZXNlcnZlOiB1c2VyPXtsaXN0ZW5lci51"
    "c2VyX2lkfSBob2xkPXtid30gcmVqZWN0ZWQgIgogICAgICAgICAgICAgICAgZiIoe3JlYXNvbn0pIGNhcD17Y2FwfSIKICAgICAg"
    "ICAgICAgKQogICAgICAgICAgICByZXR1cm4gcmVhc29uCiAgICAgICAgbGlzdGVuZXIuX3d6Zml4X3Jlc3YgPSAocWlkLCBpbnQo"
    "YncpKQogICAgICAgIHJldHVybiBOb25lCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VSLmVycm9yKGYi"
    "V1pGSVggcmVzZXJ2ZSBmYWlsZWQgKGFsbG93ZWQpOiB7ZX0iKQogICAgICAgIHJldHVybiBOb25lCgoKYXN5bmMgZGVmIHJlc2Vy"
    "dmVfcHJlKG1pZCwgdXNlcl9pZCwgc2l6ZSk6CiAgICAiIiJQcmUtZG93bmxvYWQgaG9sZCwgdGFrZW4gaW4gcHJlX3Rhc2tfY2hl"
    "Y2sgQkVGT1JFIGFueSBkb3dubG9hZGVyCiAgICBzdGFydHMgKGtleWVkIGJ5IHRoZSBjb21tYW5kIG1lc3NhZ2UgaWQpLiBUaGUg"
    "dGFzayBhZG9wdHMgaXQgbGF0ZXIgaW4KICAgIGxpbWl0X2NoZWNrZXIgdmlhIF9hZG9wdF9wcmUsIHNvIHRoZSBiYW5kd2lkdGgg"
    "aXMgaGVsZCBmcm9tIHRoZSB2ZXJ5CiAgICBmaXJzdCBtb21lbnQg4oCUIHNpbXVsdGFuZW91cyBsaW5rcyBjYW4gbmV2ZXIgZWFj"
    "aCBwYXNzIGFnYWluc3QgYQogICAgc3RhbGUgbnVtYmVyLiBSZXR1cm5zIChyZWFzb24sIHFpZCkgb3IgcmFpc2VzIG5vdGhpbmcu"
    "IiIiCiAgICB0cnk6CiAgICAgICAgYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgICAgICBidyA9IFdaRklYX0JXX0ZBQ1RPUiAqIGlu"
    "dChzaXplKQogICAgICAgIGlmIGJ3IDw9IDA6CiAgICAgICAgICAgIHJldHVybiBOb25lLCBOb25lCiAgICAgICAgIyBkcm9wIGEg"
    "c3RhbGUgcHJlLWhvbGQgZm9yIHRoaXMgbWVzc2FnZSBmaXJzdCAoZWRpdGVkIGNvbW1hbmQpCiAgICAgICAgdHJ5OgogICAgICAg"
    "ICAgICBhd2FpdCBfZGIoKS53emZpeF9wcmVob2xkW19wYXJ0KCldLmRlbGV0ZV9vbmUoeyJfaWQiOiBmInByZTp7bWlkfSJ9KQog"
    "ICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIHBhc3MKICAgICAgICByZWFzb24sIHFpZCA9IGF3YWl0IF90cnlf"
    "aG9sZCh1c2VyX2lkLCBidykKICAgICAgICBpZiByZWFzb246CiAgICAgICAgICAgIHJldHVybiByZWFzb24sIHFpZAogICAgICAg"
    "IHRyeToKICAgICAgICAgICAgYXdhaXQgX2RiKCkud3pmaXhfcHJlaG9sZFtfcGFydCgpXS5pbnNlcnRfb25lKAogICAgICAgICAg"
    "ICAgICAgeyJfaWQiOiBmInByZTp7bWlkfSIsICJxaWQiOiBxaWQsICJidyI6IGludChidyksCiAgICAgICAgICAgICAgICAgInVp"
    "ZCI6IHVzZXJfaWQsICJ0cyI6IHRpbWUoKX0KICAgICAgICAgICAgKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAg"
    "ICAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggcHJlaG9sZCB3cml0ZSBmYWlsZWQ6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5v"
    "bmUsIHFpZAogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHJlc2VydmVfcHJl"
    "IGZhaWxlZCAoYWxsb3dlZCk6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUsIE5vbmUKCgphc3luYyBkZWYgX2Fkb3B0X3ByZShs"
    "aXN0ZW5lcik6CiAgICAiIiJCcmluZyB0aGlzIHRhc2sncyBwcmUtZG93bmxvYWQgaG9sZCAoaWYgYW55KSBvbnRvIHRoZSBsaXN0"
    "ZW5lciBzbwogICAgdGhlIG5vcm1hbCByZWxlYXNlIHBhdGhzIGZyZWUgaXQuIiIiCiAgICB0cnk6CiAgICAgICAgZG9jID0gYXdh"
    "aXQgX2RiKCkud3pmaXhfcHJlaG9sZFtfcGFydCgpXS5maW5kX29uZV9hbmRfZGVsZXRlKAogICAgICAgICAgICB7Il9pZCI6IGYi"
    "cHJlOntnZXRhdHRyKGxpc3RlbmVyLCAnbWlkJywgMCl9In0KICAgICAgICApCiAgICAgICAgaWYgbm90IGRvYzoKICAgICAgICAg"
    "ICAgcmV0dXJuCiAgICAgICAgcWlkID0gZG9jLmdldCgicWlkIikKICAgICAgICBidyA9IGludChkb2MuZ2V0KCJidyIpIG9yIDAp"
    "CiAgICAgICAgaWYgbm90IHFpZCBvciBidyA8PSAwOgogICAgICAgICAgICByZXR1cm4KICAgICAgICBpZiBnZXRhdHRyKGxpc3Rl"
    "bmVyLCAiX3d6Zml4X3Jlc3YiLCBOb25lKToKICAgICAgICAgICAgIyBhbHJlYWR5IGhvbGRzIChsaW1pdF9jaGVja2VyIHJlLWNo"
    "ZWNrKTogcmVmdW5kIHRoZSBwcmUtaG9sZAogICAgICAgICAgICBhd2FpdCBfZGIoKS53emZpeF9xdW90YVtfcGFydCgpXS51cGRh"
    "dGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBxaWR9LCB7IiRpbmMiOiB7InJlc2VydmVkIjogLWJ3fX0KICAgICAgICAg"
    "ICAgKQogICAgICAgICAgICByZXR1cm4KICAgICAgICBsaXN0ZW5lci5fd3pmaXhfcmVzdiA9IChxaWQsIGJ3KQogICAgICAgIExP"
    "R0dFUi5pbmZvKGYiV1pGSVggcHJlaG9sZCBhZG9wdGVkOiB7Ynd9IGJ5dGVzIGZvciB7cWlkfSIpCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uOgogICAgICAgIHBhc3MKCgphc3luYyBkZWYgcmVsZWFzZV9yZXNlcnZlKGxpc3RlbmVyKToKICAgICIiIkZyZWUgdGhpcyB0"
    "YXNrJ3MgcmVzZXJ2YXRpb24gKGNvbXBsZXRpb24sIGVycm9yIG9yIGNhbmNlbCkuIiIiCiAgICBhd2FpdCBfYWRvcHRfcHJlKGxp"
    "c3RlbmVyKQogICAgcmVzdiA9IGdldGF0dHIobGlzdGVuZXIsICJfd3pmaXhfcmVzdiIsIE5vbmUpCiAgICBpZiBub3QgcmVzdjoK"
    "ICAgICAgICByZXR1cm4KICAgICMgY2xlYXIgc3luY2hyb25vdXNseSBGSVJTVCBzbyBhIGR1cGxpY2F0ZSByZWxlYXNlIGNhbm5v"
    "dCBkb3VibGUtcmVmdW5kCiAgICBsaXN0ZW5lci5fd3pmaXhfcmVzdiA9IE5vbmUKICAgIHRyeToKICAgICAgICBxaWQsIGJ3ID0g"
    "cmVzdgogICAgICAgIGF3YWl0IF9kYigpLnd6Zml4X3F1b3RhW19wYXJ0KCldLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lk"
    "IjogcWlkfSwgeyIkaW5jIjogeyJyZXNlcnZlZCI6IC1id319CiAgICAgICAgKQogICAgICAgIExPR0dFUi5pbmZvKGYiV1pGSVgg"
    "cmVzZXJ2ZTogcmVsZWFzZWQge19uaWNlX3NpemUoYncpfSBmcm9tIHtxaWR9IikKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAg"
    "ICAgcGFzcwoKCmRlZiBfaXNfZXhlbXB0KHVzZXJfaWQsIHVzZXJfZGljdD1Ob25lKToKICAgIGlmIHVzZXJfaWQgPT0gQ29uZmln"
    "Lk9XTkVSX0lEOgogICAgICAgIHJldHVybiBUcnVlCiAgICBpZiB1c2VyX2RpY3QgYW5kIHVzZXJfZGljdC5nZXQoIlNVRE8iKToK"
    "ICAgICAgICByZXR1cm4gVHJ1ZQogICAgIyBSU1MgdGFza3MgcnVuIHVuZGVyIHRoZSBSU1MgY2hhdCBpZCDigJQgbmV2ZXIgcXVv"
    "dGEtYmxvY2sgdGhlIGZlZWRzCiAgICB0cnk6CiAgICAgICAgaWYgQ29uZmlnLlJTU19DSEFUIGFuZCB1c2VyX2lkID09IGludChD"
    "b25maWcuUlNTX0NIQVQpOgogICAgICAgICAgICByZXR1cm4gVHJ1ZQogICAgZXhjZXB0IChUeXBlRXJyb3IsIFZhbHVlRXJyb3Ip"
    "OgogICAgICAgIHBhc3MKICAgIHJldHVybiBGYWxzZQoKCmFzeW5jIGRlZiBxdW90YV9jaGVjayhsaXN0ZW5lcik6CiAgICAiIiJT"
    "aXplLWF3YXJlIGNoZWNrIGNhbGxlZCBmcm9tIGxpbWl0X2NoZWNrZXIgKHNpemUgaXMga25vd24gdGhlcmUpLiIiIgogICAgdHJ5"
    "OgogICAgICAgIGlmIG5vdCBhd2FpdCBlbnN1cmVfcmVhZHkoKToKICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoIldaRklYIHF1"
    "b3RhOiBkYiBub3QgcmVhZHkgLT4gYWxsb3dpbmcgdGFzayIpCiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAgaWYgX2lz"
    "X2V4ZW1wdChsaXN0ZW5lci51c2VyX2lkLCBsaXN0ZW5lci51c2VyX2RpY3QpOgogICAgICAgICAgICBMT0dHRVIuaW5mbyhmIlda"
    "RklYIHF1b3RhOiB1c2VyIHtsaXN0ZW5lci51c2VyX2lkfSBleGVtcHQgKG93bmVyL3N1ZG8vcnNzKSIpCiAgICAgICAgICAgIHJl"
    "dHVybiBOb25lCiAgICAgICAgdXNlcl9pZCA9IGxpc3RlbmVyLnVzZXJfaWQKICAgICAgICBjYXAgPSBhd2FpdCBnZXRfY2FwX2J5"
    "dGVzKHVzZXJfaWQpCiAgICAgICAgdXNlZCA9IGF3YWl0IGdldF91c2FnZSh1c2VyX2lkKQogICAgICAgICMgYWRvcHQgdGhlIHBy"
    "ZS1kb3dubG9hZCBob2xkIHRha2VuIGluIHByZV90YXNrX2NoZWNrICh2MTUuOSk6CiAgICAgICAgIyB0aGUgYmFuZHdpZHRoIHdh"
    "cyBhbHJlYWR5IHJlc2VydmVkIGJlZm9yZSB0aGUgdGFzayBzdGFydGVkCiAgICAgICAgYXdhaXQgX2Fkb3B0X3ByZShsaXN0ZW5l"
    "cikKICAgICAgICBpZiBnZXRhdHRyKGxpc3RlbmVyLCAiX3d6Zml4X3Jlc3YiLCBOb25lKToKICAgICAgICAgICAgTE9HR0VSLmlu"
    "Zm8oCiAgICAgICAgICAgICAgICBmIldaRklYIHF1b3RhOiB1c2VyPXt1c2VyX2lkfSAtPiBhbGxvdyAocHJlLWhlbGQge2xpc3Rl"
    "bmVyLnNpemUgb3IgMH0pIgogICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAgIyByZS1ldmFsdWF0"
    "aW9uIChlLmcuIHFiaXQgbWV0YWRhdGEgdXBkYXRlcyk6IGRyb3Agb3VyIG93biBvbGQgaG9sZAogICAgICAgICMgZmlyc3Qgc28g"
    "dGhpcyB0YXNrIGlzIG5vdCBjb3VudGVkIGFnYWluc3QgaXRzZWxmIHR3aWNlCiAgICAgICAgaWYgZ2V0YXR0cihsaXN0ZW5lciwg"
    "Il93emZpeF9yZXN2IiwgTm9uZSk6CiAgICAgICAgICAgIGF3YWl0IHJlbGVhc2VfcmVzZXJ2ZShsaXN0ZW5lcikKICAgICAgICBz"
    "aXplID0gV1pGSVhfQldfRkFDVE9SICogaW50KGxpc3RlbmVyLnNpemUgb3IgMCkKICAgICAgICAjIEFUT01JQzogdGhlIGhvbGQg"
    "YW5kIHRoZSB2ZXJkaWN0IGhhcHBlbiBpbiBvbmUgc2VydmVyLXNpZGUKICAgICAgICAjIG9wZXJhdGlvbiDigJQgc2ltdWx0YW5l"
    "b3VzIHRhc2tzIHNlcmlhbGl6ZSwgZXhhY3RseSBvbmUgY2FuIHdpbgogICAgICAgIHJlYXNvbiA9IGF3YWl0IHJlc2VydmUobGlz"
    "dGVuZXIsIHNpemUsIGNhcCkKICAgICAgICBpZiByZWFzb246CiAgICAgICAgICAgIHJlc2VydmVkID0gYXdhaXQgcmVzZXJ2ZWRf"
    "dG9kYXkodXNlcl9pZCkKICAgICAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAgICAgICAgICBmIldaRklYIHF1b3RhOiB1c2Vy"
    "PXt1c2VyX2lkfSBzaXplPXtzaXplfSB1c2VkPXt1c2VkfSAiCiAgICAgICAgICAgICAgICBmInJlc2VydmVkPXtyZXNlcnZlZH0g"
    "Y2FwPXtjYXB9IC0+IEJMT0NLICh7cmVhc29ufSkiCiAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuIGF3YWl0IF9xdW90"
    "YV9ibG9ja19tc2coCiAgICAgICAgICAgICAgICB1c2VkLCBjYXAsIHNpemUsIGF0X2xpbWl0PShyZWFzb24gPT0gImF0X2xpbWl0"
    "IiksIHJlc2VydmVkPXJlc2VydmVkCiAgICAgICAgICAgICkKICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgZiJXWkZJ"
    "WCBxdW90YTogdXNlcj17dXNlcl9pZH0gc2l6ZT17c2l6ZX0gdXNlZD17dXNlZH0gIgogICAgICAgICAgICBmImNhcD17Y2FwfSAt"
    "PiBhbGxvdyAoaG9sZGluZyB7c2l6ZX0pIgogICAgICAgICkKICAgICAgICByZXR1cm4gTm9uZQogICAgZXhjZXB0IEV4Y2VwdGlv"
    "biBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHF1b3RhX2NoZWNrIGZhaWxlZCAoYWxsb3dlZCk6IHtlfSIpCiAg"
    "ICAgICAgcmV0dXJuIE5vbmUKCgpfVVJMX1JFID0gTm9uZQoKCmRlZiBfZXh0cmFjdF9saW5rcyh0ZXh0KToKICAgICIiIkFsbCBo"
    "dHRwKHMpIGxpbmtzIGluIGEgbWVzc2FnZSB0ZXh0IChubyBtYWduZXRzLCAudG9ycmVudCBmaWxlcykuIiIiCiAgICBnbG9iYWwg"
    "X1VSTF9SRQogICAgaWYgX1VSTF9SRSBpcyBOb25lOgogICAgICAgIGZyb20gcmUgaW1wb3J0IGNvbXBpbGUgYXMgX2MKICAgICAg"
    "ICBfVVJMX1JFID0gX2MociJodHRwcz86Ly9bXlxzfF0rIikKICAgIHJldHVybiBbCiAgICAgICAgdSBmb3IgdSBpbiBfVVJMX1JF"
    "LmZpbmRhbGwodGV4dCBvciAiIikKICAgICAgICBpZiBub3QgdS5sb3dlcigpLmVuZHN3aXRoKCIudG9ycmVudCIpCiAgICBdCgoK"
    "YXN5bmMgZGVmIF9wcm9iZV9zaXplKGxpbmssIGhlYWRlcj1Ob25lKToKICAgICIiIkhFQUQgcmVxdWVzdCB0byBsZWFybiBhIGRv"
    "d25sb2FkJ3Mgc2l6ZSBiZWZvcmUgaXQgc3RhcnRzICgwID0gdW5rbm93bikuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSBhaW9o"
    "dHRwIGltcG9ydCBDbGllbnRTZXNzaW9uLCBDbGllbnRUaW1lb3V0CgogICAgICAgIGFzeW5jIHdpdGggQ2xpZW50U2Vzc2lvbigK"
    "ICAgICAgICAgICAgdGltZW91dD1DbGllbnRUaW1lb3V0KHRvdGFsPTgpLAogICAgICAgICAgICBoZWFkZXJzPXsiVXNlci1BZ2Vu"
    "dCI6ICJXWk1MLVgvMTUuOSJ9LAogICAgICAgICkgYXMgczoKICAgICAgICAgICAgYXN5bmMgd2l0aCBzLmhlYWQobGluaywgYWxs"
    "b3dfcmVkaXJlY3RzPVRydWUpIGFzIHI6CiAgICAgICAgICAgICAgICBpZiByLnN0YXR1cyA8IDQwMDoKICAgICAgICAgICAgICAg"
    "ICAgICBjbCA9IHIuaGVhZGVycy5nZXQoIkNvbnRlbnQtTGVuZ3RoIikKICAgICAgICAgICAgICAgIGlmIGNsIGFuZCBjbC5pc2Rp"
    "Z2l0KCk6CiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGludChjbCkKICAgICAgICAjIHNvbWUgc2VydmVycyByZWplY3QgSEVB"
    "RCAtIGEgMS1ieXRlIHJhbmdlZCBHRVQgc3RpbGwgcmVwb3J0cwogICAgICAgICMgdGhlIHRvdGFsIHNpemUgaW4gQ29udGVudC1S"
    "YW5nZQogICAgICAgIGFzeW5jIHdpdGggcy5nZXQoCiAgICAgICAgICAgIGxpbmssIGFsbG93X3JlZGlyZWN0cz1UcnVlLCBoZWFk"
    "ZXJzPXsiUmFuZ2UiOiAiYnl0ZXM9MC0wIn0KICAgICAgICApIGFzIHI6CiAgICAgICAgICAgIGNyID0gci5oZWFkZXJzLmdldCgi"
    "Q29udGVudC1SYW5nZSIsICIiKQogICAgICAgICAgICBpZiBjciBhbmQgIi8iIGluIGNyOgogICAgICAgICAgICAgICAgdG90YWwg"
    "PSBjci5yc3BsaXQoIi8iLCAxKVsxXQogICAgICAgICAgICAgICAgaWYgdG90YWwuaXNkaWdpdCgpOgogICAgICAgICAgICAgICAg"
    "ICAgIHJldHVybiBpbnQodG90YWwpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiAwCgoKYXN5"
    "bmMgZGVmIHByZWNoZWNrKG1lc3NhZ2UpOgogICAgIiIiUHJlLWRvd25sb2FkIGFsbG93YW5jZSBjaGVjayAodjE1LjkpLCBjYWxs"
    "ZWQgZnJvbSBwcmVfdGFza19jaGVjawogICAgQkVGT1JFIGFueSBkb3dubG9hZGVyIHN0YXJ0cy4KCiAgICBGb3IgcGxhaW4gc2lu"
    "Z2xlIGh0dHAocykgbGlua3MgdGhlIGZpbGUgc2l6ZSBpcyBwcm9iZWQgd2l0aCBhIEhFQUQKICAgIHJlcXVlc3Q7IGlmIHRoZSBh"
    "bGxvd2FuY2UgY2Fubm90IGNvdmVyIGl0IHRoZSB0YXNrIGlzIHN0b3BwZWQgcmlnaHQKICAgIGhlcmUgd2l0aCBhIGNsZWFyIG1l"
    "c3NhZ2UsIGFuZCB0aGUgYmFuZHdpZHRoIGlzIGhlbGQgYXRvbWljYWxseSB0aGUKICAgIG1vbWVudCBpdCBwYXNzZXMsIHNvIHNp"
    "bXVsdGFuZW91cyBjb21tYW5kcyBjYW4gbmV2ZXIgZWFjaCBzbGlwIHBhc3QKICAgIHRoZSBzYW1lIHN0YWxlIG51bWJlci4gUmV0"
    "dXJucyBhIGJsb2NrIG1lc3NhZ2UsIG9yIE5vbmUgdG8gcHJvY2VlZC4KICAgICIiIgogICAgdHJ5OgogICAgICAgIGZyb21fdXNl"
    "ciA9IGdldGF0dHIobWVzc2FnZSwgImZyb21fdXNlciIsIE5vbmUpCiAgICAgICAgc2VuZGVyX2NoYXQgPSBnZXRhdHRyKG1lc3Nh"
    "Z2UsICJzZW5kZXJfY2hhdCIsIE5vbmUpCiAgICAgICAgd2hvID0gZnJvbV91c2VyIG9yIHNlbmRlcl9jaGF0CiAgICAgICAgdXNl"
    "cl9pZCA9IHdoby5pZCBpZiB3aG8gZWxzZSAwCiAgICAgICAgdXNlcl9kaWN0ID0gX2dldF91c2VyX2RpY3QodXNlcl9pZCkKICAg"
    "ICAgICBpZiBfaXNfZXhlbXB0KHVzZXJfaWQsIHVzZXJfZGljdCk6CiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAgaWYg"
    "bm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIHRleHQgPSBnZXRhdHRyKG1l"
    "c3NhZ2UsICJ0ZXh0IiwgIiIpIG9yICIiCiAgICAgICAgbGlua3MgPSBfZXh0cmFjdF9saW5rcyh0ZXh0KQogICAgICAgICMgb25s"
    "eSBzaW5nbGUgcGxhaW4gbGlua3MgY2FuIGJlIHByZS1jaGVja2VkIHJlbGlhYmx5OyBidWxrIGFuZAogICAgICAgICMgbm9uLWh0"
    "dHAgc291cmNlcyBmYWxsIGJhY2sgdG8gdGhlIHNpemUgY2hlY2sgaW5zaWRlIHRoZSBkb3dubG9hZAogICAgICAgIGlmIGxlbihs"
    "aW5rcykgIT0gMToKICAgICAgICAgICAgcmV0dXJuIE5vbmUKCiAgICAgICAgZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1faGVscGVy"
    "Lm1lc3NhZ2VfdXRpbHMgaW1wb3J0ICgKICAgICAgICAgICAgc2VuZF9tZXNzYWdlIGFzIF9zZW5kLAogICAgICAgICAgICBlZGl0"
    "X21lc3NhZ2UgYXMgX2VkaXQsCiAgICAgICAgKQoKICAgICAgICBtaWQgPSBtZXNzYWdlLmlkCiAgICAgICAgY2hlY2tpbmcgPSBO"
    "b25lCiAgICAgICAgdHJ5OgogICAgICAgICAgICBjaGVja2luZyA9IGF3YWl0IF9zZW5kKAogICAgICAgICAgICAgICAgbWVzc2Fn"
    "ZSwgIvCflI0gPGI+Q2hlY2tpbmcgYmFuZHdpZHRoIGFsbG93YW5jZeKApjwvYj4iCiAgICAgICAgICAgICkKICAgICAgICBleGNl"
    "cHQgRXhjZXB0aW9uOgogICAgICAgICAgICBjaGVja2luZyA9IE5vbmUKCiAgICAgICAgc2l6ZSA9IGF3YWl0IF9wcm9iZV9zaXpl"
    "KGxpbmtzWzBdKQogICAgICAgIGlmIHNpemUgPD0gMDoKICAgICAgICAgICAgaWYgY2hlY2tpbmcgaXMgbm90IE5vbmU6CiAgICAg"
    "ICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1faGVscGVyLm1lc3NhZ2Vf"
    "dXRpbHMgaW1wb3J0ICgKICAgICAgICAgICAgICAgICAgICAgICAgZGVsZXRlX21lc3NhZ2UgYXMgX2RlbCwKICAgICAgICAgICAg"
    "ICAgICAgICApCgogICAgICAgICAgICAgICAgICAgIGF3YWl0IF9kZWwoY2hlY2tpbmcpCiAgICAgICAgICAgICAgICBleGNlcHQg"
    "RXhjZXB0aW9uOgogICAgICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAgICAgcmV0dXJuIE5vbmUgICMgc2l6ZSB1bmtub3du"
    "IC0+IGNoZWNrZWQgZHVyaW5nIHRoZSBkb3dubG9hZAoKICAgICAgICBjYXAgPSBhd2FpdCBnZXRfY2FwX2J5dGVzKHVzZXJfaWQp"
    "CiAgICAgICAgdXNlZCA9IGF3YWl0IGdldF91c2FnZSh1c2VyX2lkKQogICAgICAgIHJlc2VydmVkID0gYXdhaXQgcmVzZXJ2ZWRf"
    "dG9kYXkodXNlcl9pZCkKICAgICAgICBidyA9IFdaRklYX0JXX0ZBQ1RPUiAqIHNpemUKICAgICAgICByZWFzb24sIF8gPSBhd2Fp"
    "dCByZXNlcnZlX3ByZShtaWQsIHVzZXJfaWQsIHNpemUpCgogICAgICAgIGlmIHJlYXNvbiBpcyBOb25lOgogICAgICAgICAgICB0"
    "cnk6CiAgICAgICAgICAgICAgICBtZXNzYWdlLl93emZpeF9oZWxkID0gVHJ1ZQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9u"
    "OgogICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAgICBpZiBjaGVja2luZyBpcyBub3QgTm9uZToKICAgICAgICAgICAgICAg"
    "IHRyeToKICAgICAgICAgICAgICAgICAgICBmcm9tIGFzeW5jaW8gaW1wb3J0IHNsZWVwIGFzIF9zbAoKICAgICAgICAgICAgICAg"
    "ICAgICBhd2FpdCBfZWRpdCgKICAgICAgICAgICAgICAgICAgICAgICAgY2hlY2tpbmcsCiAgICAgICAgICAgICAgICAgICAgICAg"
    "IGYi4pyFIDxiPkFsbG93YW5jZSBzdWZmaWNpZW50PC9iPiDigJQgc3RhcnRpbmcgZG93bmxvYWQgIgogICAgICAgICAgICAgICAg"
    "ICAgICAgICBmIih+e19uaWNlX3NpemUoYncpfSBpbmNsLiB1cGxvYWQpIiwKICAgICAgICAgICAgICAgICAgICApCiAgICAgICAg"
    "ICAgICAgICAgICAgX2RlbF9hZnRlcihjaGVja2luZykKICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "ICAgICAgICAgICAgcGFzcwogICAgICAgICAgICByZXR1cm4gTm9uZQoKICAgICAgICBpZiBjaGVja2luZyBpcyBub3QgTm9uZToK"
    "ICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1faGVscGVyLm1lc3NhZ2VfdXRp"
    "bHMgaW1wb3J0ICgKICAgICAgICAgICAgICAgICAgICBkZWxldGVfbWVzc2FnZSBhcyBfZGVsLAogICAgICAgICAgICAgICAgKQoK"
    "ICAgICAgICAgICAgICAgIGF3YWl0IF9kZWwoY2hlY2tpbmcpCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "ICAgICAgICBwYXNzCiAgICAgICAgbGVmdCA9IG1heChjYXAgLSB1c2VkIC0gcmVzZXJ2ZWQsIDApCiAgICAgICAgb3duZXIgPSBh"
    "d2FpdCBnZXRfb3duZXJfdXNlcm5hbWUoKQogICAgICAgIGlmIGxlZnQgPD0gMDoKICAgICAgICAgICAgcmV0dXJuIGF3YWl0IF9x"
    "dW90YV9ibG9ja19tc2coCiAgICAgICAgICAgICAgICB1c2VkLCBjYXAsIDAsIGF0X2xpbWl0PVRydWUsIHJlc2VydmVkPXJlc2Vy"
    "dmVkCiAgICAgICAgICAgICkKICAgICAgICByZXR1cm4gKAogICAgICAgICAgICAi8J+aqyA8Yj5Ob3QgZW5vdWdoIGJhbmR3aWR0"
    "aCBhbGxvd2FuY2U8L2I+XG7ilIJcbiIKICAgICAgICAgICAgZiLilKAgPGI+RmlsZTwvYj4g4oaSIHtfbmljZV9zaXplKHNpemUp"
    "fSAiCiAgICAgICAgICAgIGYiPGk+KG5lZWRzIH57X25pY2Vfc2l6ZShidyl9IHdpdGggdXBsb2FkKTwvaT5cbiIKICAgICAgICAg"
    "ICAgZiLilKAgPGI+QWxsb3dhbmNlIGxlZnQgdG9kYXk8L2I+IOKGkiB7X25pY2Vfc2l6ZShsZWZ0KX1cbiIKICAgICAgICAgICAg"
    "KyAoCiAgICAgICAgICAgICAgICBmIuKUoCA8Yj5Zb3VyIGRhaWx5IGNhcDwvYj4g4oaSIHtfbmljZV9zaXplKGNhcCl9XG4iCiAg"
    "ICAgICAgICAgICAgICBpZiB1c2VkICsgcmVzZXJ2ZWQgPD0gMAogICAgICAgICAgICAgICAgZWxzZSAiIgogICAgICAgICAgICAp"
    "CiAgICAgICAgICAgICsgZiLilJYgQ29udGFjdCBAe293bmVyfSB0byBpbmNyZWFzZSB5b3VyIGxpbWl0LCBvciB3YWl0IHVudGls"
    "ICIKICAgICAgICAgICAgZiJ0b21vcnJvdyAocmVzZXRzIDAwOjAwIElTVCkuIgogICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRp"
    "b24gYXMgZToKICAgICAgICBMT0dHRVIuZXJyb3IoZiJXWkZJWCBwcmVjaGVjayBmYWlsZWQgKGFsbG93ZWQpOiB7ZX0iKQogICAg"
    "ICAgIHJldHVybiBOb25lCgoKZGVmIF9nZXRfdXNlcl9kaWN0KHVzZXJfaWQpOgogICAgdHJ5OgogICAgICAgIGZyb20gLi4uIGlt"
    "cG9ydCB1c2VyX2RhdGEKCiAgICAgICAgcmV0dXJuIHVzZXJfZGF0YS5nZXQodXNlcl9pZCwge30pCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uOgogICAgICAgIHJldHVybiB7fQoKCmFzeW5jIGRlZiBfZGVsX21zZ19sYXRlcihtKToKICAgIGZyb20gYXN5bmNpbyBpbXBv"
    "cnQgc2xlZXAgYXMgX3NsCgogICAgYXdhaXQgX3NsKDYpCiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1f"
    "aGVscGVyLm1lc3NhZ2VfdXRpbHMgaW1wb3J0IGRlbGV0ZV9tZXNzYWdlIGFzIF9kZWwKCiAgICAgICAgYXdhaXQgX2RlbChtKQog"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKZGVmIF9kZWxfYWZ0ZXIobSk6CiAgICB0cnk6CiAgICAgICAgZnJv"
    "bSAuLi4gaW1wb3J0IGJvdF9sb29wCgogICAgICAgIGJvdF9sb29wLmNyZWF0ZV90YXNrKF9kZWxfbXNnX2xhdGVyKG0pKQogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKYXN5bmMgZGVmIG92ZXJfbGltaXRfbXNnKHVzZXJfaWQsIHVzZXJfZGlj"
    "dD1Ob25lKToKICAgICIiIkJsdW50IHByZS10YXNrIGNoZWNrIGNhbGxlZCBmcm9tIHByZV90YXNrX2NoZWNrIChubyBzaXplIGtu"
    "b3duIHlldCkuIiIiCiAgICB0cnk6CiAgICAgICAgaWYgX2lzX2V4ZW1wdCh1c2VyX2lkLCB1c2VyX2RpY3QpOgogICAgICAgICAg"
    "ICByZXR1cm4gTm9uZQogICAgICAgIGlmIG5vdCBhd2FpdCBlbnN1cmVfcmVhZHkoKToKICAgICAgICAgICAgTE9HR0VSLndhcm5p"
    "bmcoIldaRklYIHByZS1jaGVjazogZGIgbm90IHJlYWR5IC0+IGFsbG93aW5nIHRhc2siKQogICAgICAgICAgICByZXR1cm4gTm9u"
    "ZQogICAgICAgIGNhcCA9IGF3YWl0IGdldF9jYXBfYnl0ZXModXNlcl9pZCkKICAgICAgICB1c2VkID0gYXdhaXQgZ2V0X3VzYWdl"
    "KHVzZXJfaWQpCiAgICAgICAgcmVzZXJ2ZWQgPSBhd2FpdCByZXNlcnZlZF90b2RheSh1c2VyX2lkKQogICAgICAgIGlmIHVzZWQg"
    "KyByZXNlcnZlZCA+PSBjYXA6CiAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgZiJXWkZJWCBwcmUtY2hl"
    "Y2s6IHVzZXI9e3VzZXJfaWR9IHVzZWQ9e3VzZWR9ICIKICAgICAgICAgICAgICAgIGYicmVzZXJ2ZWQ9e3Jlc2VydmVkfSBjYXA9"
    "e2NhcH0gLT4gQkxPQ0siCiAgICAgICAgICAgICkKICAgICAgICAgICAgcmV0dXJuIGF3YWl0IF9xdW90YV9ibG9ja19tc2codXNl"
    "ZCwgY2FwLCAwLCBhdF9saW1pdD1UcnVlLCByZXNlcnZlZD1yZXNlcnZlZCkKICAgICAgICByZXR1cm4gTm9uZQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gTm9uZQoKCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACiMgUmVjb3JkaW5nIChjaGFyZ2UgKyBsaWJyYXJ5KQojIOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiBjaGFy"
    "Z2UobGlzdGVuZXIpOgogICAgIiIiQ2hhcmdlIHRoZSBmaW5pc2hlZCB0YXNrJ3MgYWN0dWFsIHNpemU7IHJlZnJlc2ggdGhlIHVz"
    "ZXIgcmVnaXN0cnkuIiIiCiAgICB0cnk6CiAgICAgICAgaWYgbm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgICAgICBy"
    "ZXR1cm4KICAgICAgICB1c2VyX2lkID0gbGlzdGVuZXIudXNlcl9pZAogICAgICAgIHNpemUgPSBXWkZJWF9CV19GQUNUT1IgKiBp"
    "bnQobGlzdGVuZXIuc2l6ZSBvciAwKQogICAgICAgIG5vdyA9IHRpbWUoKQogICAgICAgIGRiID0gX2RiKCkKICAgICAgICBwYXJ0"
    "ID0gX3BhcnQoKQogICAgICAgIGlmIHNpemUgPiAwOgogICAgICAgICAgICBhd2FpdCBkYi53emZpeF9xdW90YVtwYXJ0XS51cGRh"
    "dGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBmInt1c2VyX2lkfTp7X2RheV9pc3QoKX0ifSwKICAgICAgICAgICAgICAg"
    "IHsKICAgICAgICAgICAgICAgICAgICAiJGluYyI6IHsidXNlZCI6IHNpemV9LAogICAgICAgICAgICAgICAgICAgICIkc2V0Ijog"
    "ewogICAgICAgICAgICAgICAgICAgICAgICAiZXhwaXJlQXQiOiBkYXRldGltZS5mcm9tdGltZXN0YW1wKAogICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgbm93ICsgV1pGSVhfUVVPVEFfVFRMX0RBWVMgKiA4NjQwMCwgVVRDCiAgICAgICAgICAgICAgICAgICAg"
    "ICAgICkKICAgICAgICAgICAgICAgICAgICB9LAogICAgICAgICAgICAgICAgfSwKICAgICAgICAgICAgICAgIHVwc2VydD1UcnVl"
    "LAogICAgICAgICAgICApCiAgICAgICAgICAgIGRvYyA9IGF3YWl0IGRiLnd6Zml4X3F1b3RhW3BhcnRdLmZpbmRfb25lKHsiX2lk"
    "IjogZiJ7dXNlcl9pZH06e19kYXlfaXN0KCl9In0pCiAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgZiJX"
    "WkZJWCBjaGFyZ2U6IHVzZXI9e3VzZXJfaWR9IHRhc2tfc2l6ZT17c2l6ZX0gIgogICAgICAgICAgICAgICAgZiItPiB0b2RheV90"
    "b3RhbD17aW50KGRvYy5nZXQoJ3VzZWQnLCAwKSkgaWYgZG9jIGVsc2UgJz8nfSAiCiAgICAgICAgICAgICAgICBmIihkYXk9e19k"
    "YXlfaXN0KCl9KSIKICAgICAgICAgICAgKQogICAgICAgIHVuYW1lID0gIiIKICAgICAgICBuYW1lID0gIiIKICAgICAgICB0cnk6"
    "CiAgICAgICAgICAgIHVuYW1lID0gZ2V0YXR0cihsaXN0ZW5lci51c2VyLCAidXNlcm5hbWUiLCAiIikgb3IgIiIKICAgICAgICAg"
    "ICAgbmFtZSA9IChnZXRhdHRyKGxpc3RlbmVyLnVzZXIsICJmaXJzdF9uYW1lIiwgIiIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwogICAgICAgIGF3YWl0IGRiLnd6Zml4X3VzZXJzW3BhcnRdLnVwZGF0"
    "ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogdXNlcl9pZH0sCiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICIkc2V0Ijog"
    "eyJ1bmFtZSI6IHVuYW1lLCAibmFtZSI6IG5hbWUsICJsYXN0X3VzZWQiOiBub3d9LAogICAgICAgICAgICAgICAgIiRpbmMiOiB7"
    "InRvdGFsX3VzZWQiOiBzaXplLCAidGFza3MiOiAxfSwKICAgICAgICAgICAgfSwKICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAg"
    "ICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIGNoYXJnZSBmYWls"
    "ZWQgKGlnbm9yZWQpOiB7ZX0iKQoKCmFzeW5jIGRlZiByZWNvcmRfdGFzayhsaXN0ZW5lciwgbGluaywgZmlsZXMsIG1pbWVfdHlw"
    "ZSwgcmNsb25lX3BhdGg9IiIsIGRpcl9pZD0iIik6CiAgICAiIiJDYWxsZWQgZnJvbSBUYXNrTGlzdGVuZXIub25fdXBsb2FkX2Nv"
    "bXBsZXRlIGZvciBldmVyeSBmaW5pc2hlZCB0YXNrLiIiIgogICAgIyBmcmVlIHRoaXMgdGFzaydzIGJhbmR3aWR0aCBob2xkIGJl"
    "Zm9yZSBjaGFyZ2luZyB0aGUgYWN0dWFsIGFtb3VudAogICAgYXdhaXQgcmVsZWFzZV9yZXNlcnZlKGxpc3RlbmVyKQogICAgIyBp"
    "ZGVtcG90ZW5jeTogYSBkdXBsaWNhdGUgY29tcGxldGlvbiBmb3IgdGhlIHNhbWUgdGFzayAoc2FtZSBjb21tYW5kCiAgICAjIG1l"
    "c3NhZ2UsIG5hbWUgYW5kIHNpemUpIG11c3QgbmV2ZXIgY2hhcmdlIHR3aWNlCiAgICB0cnk6CiAgICAgICAga2V5ID0gewogICAg"
    "ICAgICAgICAibWlkIjogaW50KGxpc3RlbmVyLm1pZCBvciAwKSwKICAgICAgICAgICAgIm5hbWUiOiBzdHIobGlzdGVuZXIubmFt"
    "ZSBvciAiIiksCiAgICAgICAgICAgICJzaXplIjogaW50KGxpc3RlbmVyLnNpemUgb3IgMCksCiAgICAgICAgfQogICAgICAgIGR1"
    "cCA9IGF3YWl0IF9kYigpLnd6Zml4X2xpYnJhcnlbX3BhcnQoKV0uZmluZF9vbmUoa2V5KQogICAgICAgIGlmIGR1cDoKICAgICAg"
    "ICAgICAgTE9HR0VSLndhcm5pbmcoCiAgICAgICAgICAgICAgICBmIldaRklYIGNoYXJnZTogRFVQTElDQVRFIGNvbXBsZXRpb24g"
    "Zm9yIG1pZD17a2V5WydtaWQnXX0gIgogICAgICAgICAgICAgICAgZiIne2tleVsnbmFtZSddfScgc2l6ZT17a2V5WydzaXplJ119"
    "IC0+IG5vdCBjaGFyZ2VkIGFnYWluIgogICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVybgogICAgZXhjZXB0IEV4Y2VwdGlv"
    "bjoKICAgICAgICBwYXNzCiAgICBhd2FpdCBjaGFyZ2UobGlzdGVuZXIpCiAgICB0cnk6CiAgICAgICAgaWYgbm90IGF3YWl0IGVu"
    "c3VyZV9yZWFkeSgpOgogICAgICAgICAgICByZXR1cm4KICAgICAgICBub3cgPSB0aW1lKCkKICAgICAgICBwYXJ0cyA9IFtdCiAg"
    "ICAgICAgdGdfbGlua3MgPSBbXQogICAgICAgIHBhcnRfbmFtZXMgPSBbXQogICAgICAgIGlmIGlzaW5zdGFuY2UoZmlsZXMsIGRp"
    "Y3QpOgogICAgICAgICAgICBmb3IgbCwgbiBpbiBmaWxlcy5pdGVtcygpOgogICAgICAgICAgICAgICAgbCA9IHN0cihsKQogICAg"
    "ICAgICAgICAgICAgdGdfbGlua3MuYXBwZW5kKGwpCiAgICAgICAgICAgICAgICBwYXJ0X25hbWVzLmFwcGVuZChzdHIobikpCiAg"
    "ICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgdGFpbCA9IGwucnN0cmlwKCIvIikuc3BsaXQoIi8iKVstMjpd"
    "CiAgICAgICAgICAgICAgICAgICAgY2lkLCBtaWQgPSB0YWlsWzBdLCB0YWlsWzFdCiAgICAgICAgICAgICAgICAgICAgaWYgY2lk"
    "LmlzZGlnaXQoKSBhbmQgbWlkLmlzZGlnaXQoKToKICAgICAgICAgICAgICAgICAgICAgICAgcGFydHMuYXBwZW5kKFtpbnQoZiIt"
    "MTAwe2NpZH0iKSwgaW50KG1pZCldKQogICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgICAg"
    "ICBjb250aW51ZQogICAgICAgIHRyeToKICAgICAgICAgICAgbW9kZSA9IGYie2xpc3RlbmVyLm1vZGVbMF19IOKGkiB7bGlzdGVu"
    "ZXIubW9kZVsxXX0iCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgbW9kZSA9ICIiCiAgICAgICAgZG9jID0g"
    "ewogICAgICAgICAgICAiX2lkIjogdXVpZDQoKS5oZXgsCiAgICAgICAgICAgICJtaWQiOiBpbnQobGlzdGVuZXIubWlkIG9yIDAp"
    "LAogICAgICAgICAgICAibmFtZSI6IGxpc3RlbmVyLm5hbWUsCiAgICAgICAgICAgICJzaXplIjogaW50KGxpc3RlbmVyLnNpemUg"
    "b3IgMCksCiAgICAgICAgICAgICJ1c2VyX2lkIjogbGlzdGVuZXIudXNlcl9pZCwKICAgICAgICAgICAgInVuYW1lIjogZ2V0YXR0"
    "cihsaXN0ZW5lci51c2VyLCAidXNlcm5hbWUiLCAiIikgb3IgIiIsCiAgICAgICAgICAgICJkYXRlIjogbm93LAogICAgICAgICAg"
    "ICAiZXhwaXJlQXQiOiBkYXRldGltZS5mcm9tdGltZXN0YW1wKAogICAgICAgICAgICAgICAgbm93ICsgV1pGSVhfTElCX1RUTF9E"
    "QVlTICogODY0MDAsIFVUQwogICAgICAgICAgICApLAogICAgICAgICAgICAiaXNfbGVlY2giOiBib29sKGxpc3RlbmVyLmlzX2xl"
    "ZWNoKSwKICAgICAgICAgICAgIm1vZGUiOiBtb2RlLAogICAgICAgICAgICAicGFydHMiOiBwYXJ0cywKICAgICAgICAgICAgInRn"
    "X2xpbmtzIjogdGdfbGlua3MsCiAgICAgICAgICAgICJwYXJ0X25hbWVzIjogcGFydF9uYW1lcywKICAgICAgICAgICAgImNsb3Vk"
    "X2xpbmsiOiBsaW5rIGlmIGlzaW5zdGFuY2UobGluaywgc3RyKSBlbHNlICIiLAogICAgICAgICAgICAicmNsb25lX3BhdGgiOiBy"
    "Y2xvbmVfcGF0aCBvciAiIiwKICAgICAgICAgICAgImRpcl9pZCI6IGRpcl9pZCBvciAiIiwKICAgICAgICB9CiAgICAgICAgYXdh"
    "aXQgX2RiKCkud3pmaXhfbGlicmFyeVtfcGFydCgpXS5pbnNlcnRfb25lKGRvYykKICAgICAgICBMT0dHRVIuaW5mbygKICAgICAg"
    "ICAgICAgZiJXWkZJWCBsaWJyYXJ5OiByZWNvcmRlZCAne2RvY1snbmFtZSddfScgc2l6ZT17ZG9jWydzaXplJ119ICIKICAgICAg"
    "ICAgICAgZiJ1c2VyPXtkb2NbJ3VzZXJfaWQnXX0gdGdfcGFydHM9e2xlbihkb2NbJ3BhcnRzJ10pfSIKICAgICAgICApCiAgICBl"
    "eGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggcmVjb3JkX3Rhc2sgZmFpbGVkIChpZ25v"
    "cmVkKToge2V9IikKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgAojIExpYnJhcnkgc2VhcmNoCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIGZpbmQocXVlcnksIHVzZXJfaWQsIGFsbF91c2Vycz1GYWxzZSwg"
    "bGltaXQ9Nik6CiAgICBpZiBub3QgYXdhaXQgZW5zdXJlX3JlYWR5KCk6CiAgICAgICAgcmV0dXJuIFtdCiAgICBxID0geyJuYW1l"
    "IjogeyIkcmVnZXgiOiBfcmVzY2FwZShxdWVyeSksICIkb3B0aW9ucyI6ICJpIn19CiAgICBpZiBub3QgYWxsX3VzZXJzOgogICAg"
    "ICAgIHFbInVzZXJfaWQiXSA9IHVzZXJfaWQKICAgIHRyeToKICAgICAgICBjdXJzb3IgPSAoCiAgICAgICAgICAgIF9kYigpLnd6"
    "Zml4X2xpYnJhcnlbX3BhcnQoKV0uZmluZChxKS5zb3J0KCJkYXRlIiwgLTEpLmxpbWl0KGludChsaW1pdCkpCiAgICAgICAgKQog"
    "ICAgICAgIHJldHVybiBbZCBhc3luYyBmb3IgZCBpbiBjdXJzb3JdCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAg"
    "TE9HR0VSLmVycm9yKGYiV1pGSVggZmluZCBmYWlsZWQ6IHtlfSIpCiAgICAgICAgcmV0dXJuIFtdCgoKIyDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKIyBEQiBzdGF0cyAvIGNsZWFu"
    "dXAKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAK"
    "Cgphc3luYyBkZWYgZGJzdGF0cygpOgogICAgIiIid3ptbHggYnJlYWtkb3duICsgYWNjb3VudC13aWRlIChhbGwgZGF0YWJhc2Vz"
    "KSBzaXplcy4iIiIKICAgIG91dCA9IHsidG90YWwiOiAwLCAiY29scyI6IFtdLCAiZGJzIjogW10sICJhY2NvdW50X3RvdGFsIjog"
    "MCwgImRiX2Vycm9yIjogIiIsICJlcnJvciI6ICIifQogICAgdHJ5OgogICAgICAgIGRiID0gX2RiKCkKICAgICAgICBpZiBkYiBp"
    "cyBOb25lOgogICAgICAgICAgICBvdXRbImVycm9yIl0gPSAiZGF0YWJhc2Ugbm90IGNvbm5lY3RlZCIKICAgICAgICAgICAgcmV0"
    "dXJuIG91dAogICAgICAgIHN0ID0gYXdhaXQgZGIuY29tbWFuZCh7ImRiU3RhdHMiOiAxLCAic2NhbGUiOiAxMDI0fSkKICAgICAg"
    "ICBvdXRbInRvdGFsIl0gPSBpbnQoc3QuZ2V0KCJkYXRhU2l6ZSIpIG9yIDApICogMTAyNAogICAgZXhjZXB0IEV4Y2VwdGlvbiBh"
    "cyBlOgogICAgICAgIG91dFsiZXJyb3IiXSA9IHN0cihlKQogICAgdHJ5OgogICAgICAgIG5hbWVzID0gYXdhaXQgX2RiKCkubGlz"
    "dF9jb2xsZWN0aW9uX25hbWVzKCkKICAgICAgICBwYXJ0ID0gX3BhcnQoKQogICAgICAgIGFnZyA9IHt9CiAgICAgICAgZm9yIG5h"
    "bWUgaW4gbmFtZXM6CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGNzID0gYXdhaXQgX2RiKCkuY29tbWFuZCh7ImNv"
    "bGxTdGF0cyI6IG5hbWV9KQogICAgICAgICAgICAgICAgaWYgIi4iIGluIG5hbWU6CiAgICAgICAgICAgICAgICAgICAgYmFzZSwg"
    "c3VmZml4ID0gbmFtZS5zcGxpdCgiLiIsIDEpCiAgICAgICAgICAgICAgICAgICAgIyBhIHBhcnRpdGlvbiBzdWZmaXggZXF1YWwg"
    "dG8gb3VycyA9IHRoaXMgYm90OyBvdGhlciBib3RzCiAgICAgICAgICAgICAgICAgICAgIyBzaGFyaW5nIHRoaXMgQXRsYXMgYWNj"
    "b3VudCBnZXQgdGhlaXIgb3duIGF0dHJpYnV0aW9uCiAgICAgICAgICAgICAgICAgICAgb3duZXIgPSAic2VsZiIgaWYgc3VmZml4"
    "ID09IHBhcnQgZWxzZSAib3RoZXIiCiAgICAgICAgICAgICAgICBlbHNlOgogICAgICAgICAgICAgICAgICAgIGJhc2UsIG93bmVy"
    "ID0gbmFtZSwgInNoYXJlZCIKICAgICAgICAgICAgICAgIGEgPSBhZ2cuc2V0ZGVmYXVsdCgKICAgICAgICAgICAgICAgICAgICBi"
    "YXNlLAogICAgICAgICAgICAgICAgICAgIHsiZG9jcyI6IDAsICJzaXplIjogMCwgInNlbGYiOiAwLCAib3RoZXIiOiAwLCAic2hh"
    "cmVkIjogMCwgIm4iOiAwfSwKICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgIHN6ID0gaW50KGNzLmdldCgic2l6ZSIp"
    "IG9yIDApCiAgICAgICAgICAgICAgICBhWyJkb2NzIl0gKz0gaW50KGNzLmdldCgiY291bnQiKSBvciAwKQogICAgICAgICAgICAg"
    "ICAgYVsic2l6ZSJdICs9IHN6CiAgICAgICAgICAgICAgICBhW293bmVyXSArPSBzegogICAgICAgICAgICAgICAgYVsibiJdICs9"
    "IDEKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgb3V0WyJjb2xz"
    "Il0gPSBzb3J0ZWQoYWdnLml0ZW1zKCksIGtleT1sYW1iZGEga3Y6IC1rdlsxXVsic2l6ZSJdKQogICAgZXhjZXB0IEV4Y2VwdGlv"
    "biBhcyBlOgogICAgICAgIG91dFsiZXJyb3IiXSA9IHN0cihlKQogICAgIyBhY2NvdW50LXdpZGU6IGV2ZXJ5IGRhdGFiYXNlIG9u"
    "IHRoaXMgQXRsYXMgY2x1c3RlciAodGhlIGZyZWUtdGllcgogICAgIyA1MTIgTUIgaXMgc2hhcmVkIGFjcm9zcyBBTEwgb2YgdGhl"
    "bSwgbm90IGp1c3Qgd3ptbHgpCiAgICB0cnk6CiAgICAgICAgYWRtaW5fZGIgPSBfZGIoKS5jbGllbnQuZ2V0X2RhdGFiYXNlKCJh"
    "ZG1pbiIpCiAgICAgICAgbGQgPSBhd2FpdCBhZG1pbl9kYi5jb21tYW5kKHsibGlzdERhdGFiYXNlcyI6IDF9KQogICAgICAgIG91"
    "dFsiZGJzIl0gPSBzb3J0ZWQoCiAgICAgICAgICAgICgKICAgICAgICAgICAgICAgIChzdHIoZC5nZXQoIm5hbWUiKSksIGludChk"
    "LmdldCgic2l6ZU9uRGlzayIpIG9yIDApKQogICAgICAgICAgICAgICAgZm9yIGQgaW4gbGQuZ2V0KCJkYXRhYmFzZXMiLCBbXSkK"
    "ICAgICAgICAgICAgKSwKICAgICAgICAgICAga2V5PWxhbWJkYSB0OiAtdFsxXSwKICAgICAgICApCiAgICAgICAgb3V0WyJhY2Nv"
    "dW50X3RvdGFsIl0gPSBpbnQobGQuZ2V0KCJ0b3RhbFNpemUiKSBvciAwKSBvciBzdW0oCiAgICAgICAgICAgIHMgZm9yIF8sIHMg"
    "aW4gb3V0WyJkYnMiXQogICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICBvdXRbImRiX2Vycm9yIl0g"
    "PSBzdHIoZSkKICAgIHJldHVybiBvdXQKCgphc3luYyBkZWYgZGJjbGVhbigpOgogICAgIiIiU2FmZSBwdXJnZXMgb25seS4gUmV0"
    "dXJucyB7bGFiZWw6IGZyZWVkX2RvY19jb3VudH0uIiIiCiAgICBmcmVlZCA9IHt9CiAgICBkYiA9IF9kYigpCiAgICBwYXJ0ID0g"
    "X3BhcnQoKQogICAgdHJ5OgogICAgICAgIHIgPSBhd2FpdCBkYi5zdHJlYW1zW3BhcnRdLmRlbGV0ZV9tYW55KHsiZXhwIjogeyIk"
    "bHQiOiBpbnQodGltZSgpKX19KQogICAgICAgIGZyZWVkWyJleHBpcmVkIHN0cmVhbSB0b2tlbnMiXSA9IHIuZGVsZXRlZF9jb3Vu"
    "dAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICB0cnk6CiAgICAgICAgciA9IGF3YWl0IGRiLnRhc2tzW3Bh"
    "cnRdLmRlbGV0ZV9tYW55KHt9KQogICAgICAgIGZyZWVkWyJpbmNvbXBsZXRlLXRhc2sgcmVzdW1lIHJlY29yZHMiXSA9IHIuZGVs"
    "ZXRlZF9jb3VudAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICB0cnk6CiAgICAgICAgciA9IGF3YWl0IGRi"
    "Lnd6Zml4X3F1b3RhW3BhcnRdLmRlbGV0ZV9tYW55KAogICAgICAgICAgICB7ImV4cGlyZUF0IjogeyIkbHQiOiBkYXRldGltZS5u"
    "b3coVVRDKX19CiAgICAgICAgKQogICAgICAgIGZyZWVkWyJzdGFsZSB3emZpeCBxdW90YSByb3dzIl0gPSByLmRlbGV0ZWRfY291"
    "bnQKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIGZyZWVkCgoKYXN5bmMgZGVmIGJvb3RfcmVw"
    "b3J0KCk6CiAgICAiIiJEZWxheWVkIGJvb3QgbG9nOiB0b3RhbCBEQiBzaXplICsgd2FybmluZyBwYXN0IDgwJSBvZiB0aGUgNTEy"
    "IE1CIHRpZXIuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSBhc3luY2lvIGltcG9ydCBzbGVlcAoKICAgICAgICBhd2FpdCBzbGVl"
    "cCg0NSkKICAgICAgICBzdCA9IGF3YWl0IGRic3RhdHMoKQogICAgICAgIGlmIHN0LmdldCgiZXJyb3IiKSBhbmQgbm90IHN0Lmdl"
    "dCgiY29scyIpOgogICAgICAgICAgICByZXR1cm4KICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgZiJXWkZJWCBEQjog"
    "e19uaWNlX3NpemUoc3RbJ3RvdGFsJ10pfSBhY3Jvc3MgIgogICAgICAgICAgICBmIntsZW4oc3RbJ2NvbHMnXSl9IGNvbGxlY3Rp"
    "b24gZ3JvdXBzIChmcmVlIHRpZXIgNTEyIE1CKSIKICAgICAgICApCiAgICAgICAgaWYgc3RbInRvdGFsIl0gPiAwLjggKiBXWkZJ"
    "WF9BVExBU19GUkVFOgogICAgICAgICAgICBMT0dHRVIud2FybmluZygKICAgICAgICAgICAgICAgICJXWkZJWCBEQjogdXNhZ2Ug"
    "aXMgYWJvdmUgODAlIG9mIHRoZSBBdGxhcyBmcmVlIHRpZXIgKDUxMiBNQikhICIKICAgICAgICAgICAgICAgICJSdW4gL2Ric3Rh"
    "dHMgYW5kIC9kYmNsZWFuIHRvIHJlY2xhaW0gc3BhY2UuIgogICAgICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAg"
    "ICAgIHBhc3MKCgphc3luYyBkZWYgZGlhZyh1c2VyX2lkLCBwcm9iZV9nYj1Ob25lKToKICAgICIiIkNvbXBsZXRlIHF1b3RhIHN0"
    "YXRlIGZvciBvbmUgdXNlciwgZm9yIGxpdmUgZGVidWdnaW5nLiIiIgogICAgb3V0ID0ge30KICAgIG91dFsicGFydGl0aW9uIl0g"
    "PSBfcGFydCgpCiAgICBvdXRbImRheSJdID0gX2RheV9pc3QoKQogICAgb3V0WyJkYl9yZWFkeSJdID0gYXdhaXQgZW5zdXJlX3Jl"
    "YWR5KCkKICAgIG91dFsidXNlcl9kb2MiXSA9IGF3YWl0IGdldF91c2VyX2RvYyh1c2VyX2lkKQogICAgb3V0WyJnbG9iYWxfY2Fw"
    "X2diIl0gPSBhd2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKQogICAgb3V0WyJjYXBfYnl0ZXMiXSA9IGF3YWl0IGdldF9jYXBfYnl0"
    "ZXModXNlcl9pZCkKICAgIG91dFsicXVvdGFfZG9jIl0gPSBOb25lCiAgICB0cnk6CiAgICAgICAgb3V0WyJxdW90YV9kb2MiXSA9"
    "IGF3YWl0IF9kYigpLnd6Zml4X3F1b3RhW19wYXJ0KCldLmZpbmRfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYie3VzZXJfaWR9"
    "OntfZGF5X2lzdCgpfSJ9CiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBvdXRbInJlc2Vy"
    "dmVkIl0gPSBhd2FpdCByZXNlcnZlZF90b2RheSh1c2VyX2lkKQogICAgaWYgcHJvYmVfZ2IgaXMgbm90IE5vbmU6CiAgICAgICAg"
    "cHJvYmUgPSBwcm9iZV9nYiAqIEdCCiAgICAgICAgdXNlZCA9IGF3YWl0IGdldF91c2FnZSh1c2VyX2lkKQogICAgICAgIGNhcCA9"
    "IG91dFsiY2FwX2J5dGVzIl0KICAgICAgICBvdXRbInByb2JlIl0gPSB7CiAgICAgICAgICAgICJzaXplIjogcHJvYmUsCiAgICAg"
    "ICAgICAgICJ1c2VkIjogdXNlZCwKICAgICAgICAgICAgInJlc2VydmVkIjogb3V0WyJyZXNlcnZlZCJdLAogICAgICAgICAgICAi"
    "Y2FwIjogY2FwLAogICAgICAgICAgICAid291bGRfYmxvY2siOiAoCiAgICAgICAgICAgICAgICB1c2VkICsgb3V0WyJyZXNlcnZl"
    "ZCJdID49IGNhcAogICAgICAgICAgICAgICAgb3IgdXNlZCArIG91dFsicmVzZXJ2ZWQiXSArIHByb2JlID4gY2FwICsgV1pGSVhf"
    "R1JBQ0VfTUIgKiBNQgogICAgICAgICAgICApLAogICAgICAgIH0KICAgIHJldHVybiBvdXQKCgojIOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIE93bmVyLWZhY2luZyBzdW1tYXJp"
    "ZXMKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAK"
    "Cgphc3luYyBkZWYgdG9kYXlfcm93cygpOgogICAgIiIiQWxsIG9mIHRvZGF5J3MgcXVvdGEgcm93czogW3sidXNlcl9pZCIsInVz"
    "ZWQifSwgLi4uXSBsYXJnZXN0IGZpcnN0LiIiIgogICAgdHJ5OgogICAgICAgIGRheSA9IF9kYXlfaXN0KCkKICAgICAgICBjdXJz"
    "b3IgPSBfZGIoKS53emZpeF9xdW90YVtfcGFydCgpXS5maW5kKHsiX2lkIjogeyIkcmVnZXgiOiBmIjp7ZGF5fSQifX0pCiAgICAg"
    "ICAgcm93cyA9IFtdCiAgICAgICAgYXN5bmMgZm9yIGQgaW4gY3Vyc29yOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAg"
    "ICB1aWQgPSBpbnQoc3RyKGRbIl9pZCJdKS5yc3BsaXQoIjoiLCAxKVswXSkKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoK"
    "ICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgIHJvd3MuYXBwZW5kKHsidXNlcl9pZCI6IHVpZCwgInVzZWQiOiBp"
    "bnQoZC5nZXQoInVzZWQiKSBvciAwKX0pCiAgICAgICAgcm93cy5zb3J0KGtleT1sYW1iZGEgcjogLXJbInVzZWQiXSkKICAgICAg"
    "ICByZXR1cm4gcm93cwogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gW10KCgphc3luYyBkZWYgYWxsX3VzZXJz"
    "KCk6CiAgICB0cnk6CiAgICAgICAgY3Vyc29yID0gX2RiKCkud3pmaXhfdXNlcnNbX3BhcnQoKV0uZmluZCgpLnNvcnQoImxhc3Rf"
    "dXNlZCIsIC0xKQogICAgICAgIHJldHVybiBbZCBhc3luYyBmb3IgZCBpbiBjdXJzb3JdCiAgICBleGNlcHQgRXhjZXB0aW9uOgog"
    "ICAgICAgIHJldHVybiBbXQo="
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
    "bikKCmZyb20gZGF0ZXRpbWUgaW1wb3J0IGRhdGV0aW1lCmZyb20gaHRtbCBpbXBvcnQgZXNjYXBlCgpmcm9tIC4uIGltcG9ydCBi"
    "b3RfbG9vcCwgdXNlcl9kYXRhCmZyb20gLi5jb3JlLmNvbmZpZ19tYW5hZ2VyIGltcG9ydCBDb25maWcKZnJvbSAuLmhlbHBlci5l"
    "eHRfdXRpbHMuc3RhdHVzX3V0aWxzIGltcG9ydCBnZXRfcmVhZGFibGVfZmlsZV9zaXplCmZyb20gLi5oZWxwZXIudGVsZWdyYW1f"
    "aGVscGVyLmJ1dHRvbl9idWlsZCBpbXBvcnQgQnV0dG9uTWFrZXIKZnJvbSAuLmhlbHBlci50ZWxlZ3JhbV9oZWxwZXIubWVzc2Fn"
    "ZV91dGlscyBpbXBvcnQgKAogICAgYXV0b19kZWxldGVfbWVzc2FnZSwKICAgIHNlbmRfbWVzc2FnZSwKKQpmcm9tIC4uaGVscGVy"
    "Lnd6Zml4LnIxX2NvcmUgaW1wb3J0ICgKICAgIElTVCwKICAgIGFsbF91c2VycywKICAgIGRiY2xlYW4sCiAgICBkYnN0YXRzLAog"
    "ICAgZGVsZXRlX3VzZXIsCiAgICBmaW5kLAogICAgZ2V0X2NhcF9ieXRlcywKICAgIGdldF91c2FnZSwKICAgIHJlc2V0X3VzYWdl"
    "LAogICAgc2V0X2dsb2JhbF9jYXBfZ2IsCiAgICBzZXRfdXNlcl9jYXAsCiAgICB0b2RheV9yb3dzLAogICAgdXNlcl9leGlzdHMs"
    "CiAgICByZXNlcnZlZF90b2RheSwKICAgIGJvb3RfcmVwb3J0LAopCmZyb20gLi5oZWxwZXIud3pmaXgucjJfd2ViIGltcG9ydCBn"
    "ZXRfYWRtaW5fcGFzcywgc2V0X2FkbWluX3Bhc3MsIHN0YXJ0X2xvb3BzCgoKYXN5bmMgZGVmIF9kYl9vaygpOgogICAgZnJvbSAu"
    "LmhlbHBlci53emZpeC5yMV9jb3JlIGltcG9ydCBlbnN1cmVfcmVhZHkKCiAgICByZXR1cm4gYXdhaXQgZW5zdXJlX3JlYWR5KCkK"
    "CgpfREJfRE9XTiA9ICLinYwgRGF0YWJhc2Ugbm90IGNvbm5lY3RlZCDigJQgc2V0IERBVEFCQVNFX1VSTCBhbmQgdHJ5IGFnYWlu"
    "LiIKCgpkZWYgX3Iobik6CiAgICB0cnk6CiAgICAgICAgcmV0dXJuIGdldF9yZWFkYWJsZV9maWxlX3NpemUoaW50KG4pKQogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gc3RyKG4pCgoKZGVmIF9pc19vd25lcihtZXNzYWdlKToKICAgIHUgPSBt"
    "ZXNzYWdlLmZyb21fdXNlciBvciBtZXNzYWdlLnNlbmRlcl9jaGF0CiAgICByZXR1cm4gdSBhbmQgdS5pZCA9PSBDb25maWcuT1dO"
    "RVJfSUQKCgpkZWYgX3RhcmdldF91c2VyKG1lc3NhZ2UsIGFyZ3MpOgogICAgIiIiUmVwbHktdG8gdXNlciwgZWxzZSBmaXJzdCBu"
    "dW1lcmljIGFyZywgZWxzZSBOb25lLiIiIgogICAgaWYgbWVzc2FnZS5yZXBseV90b19tZXNzYWdlIGFuZCBtZXNzYWdlLnJlcGx5"
    "X3RvX21lc3NhZ2UuZnJvbV91c2VyOgogICAgICAgIHJldHVybiBtZXNzYWdlLnJlcGx5X3RvX21lc3NhZ2UuZnJvbV91c2VyLmlk"
    "CiAgICBmb3IgYSBpbiBhcmdzOgogICAgICAgIGlmIGEubHN0cmlwKCItIikuaXNkaWdpdCgpOgogICAgICAgICAgICByZXR1cm4g"
    "aW50KGEpCiAgICByZXR1cm4gTm9uZQoKCmRlZiBfcGFyc2VfdGFyZ2V0X2FuZF9nYihtZXNzYWdlLCBhcmdzKToKICAgICIiIlJl"
    "c29sdmUgdGFyZ2V0IGFuZCBHQiBmcm9tIGRpZmZlcmVudCBhcmdzLgoKICAgIFJlcGx5IGZvcm06ICAvc2V0Y2FwIDUgICAgICAg"
    "KHRhcmdldCA9IHJlcGxpZWQgdXNlciwgZ2IgPSA1KQogICAgSWQgZm9ybTogICAgIC9zZXRjYXAgMTIzIDUgICAodGFyZ2V0ID0g"
    "MTIzLCBnYiA9IDUg4oCUIE5PVCAxMjMhKQogICAgIiIiCiAgICB0YXJnZXQgPSBOb25lCiAgICByZXN0ID0gYXJncwogICAgaWYg"
    "bWVzc2FnZS5yZXBseV90b19tZXNzYWdlIGFuZCBtZXNzYWdlLnJlcGx5X3RvX21lc3NhZ2UuZnJvbV91c2VyOgogICAgICAgIHRh"
    "cmdldCA9IG1lc3NhZ2UucmVwbHlfdG9fbWVzc2FnZS5mcm9tX3VzZXIuaWQKICAgIGVsc2U6CiAgICAgICAgZm9yIGksIGEgaW4g"
    "ZW51bWVyYXRlKGFyZ3MpOgogICAgICAgICAgICBpZiBhLmxzdHJpcCgiLSIpLmlzZGlnaXQoKToKICAgICAgICAgICAgICAgIHRh"
    "cmdldCA9IGludChhKQogICAgICAgICAgICAgICAgcmVzdCA9IGFyZ3NbaSArIDEgOl0KICAgICAgICAgICAgICAgIGJyZWFrCiAg"
    "ICBnYiA9IE5vbmUKICAgIGZvciBhIGluIHJlc3Q6CiAgICAgICAgaWYgYS5yZXBsYWNlKCIuIiwgIiIsIDEpLmlzZGlnaXQoKToK"
    "ICAgICAgICAgICAgZ2IgPSBmbG9hdChhKQogICAgICAgICAgICBicmVhawogICAgcmV0dXJuIHRhcmdldCwgZ2IKCgpkZWYgX2Zt"
    "dF9nYihnYik6CiAgICAiIiJIdW1hbi1mcmllbmRseSBHQiBsYWJlbCAobmV2ZXIgc2NpZW50aWZpYyBub3RhdGlvbikuIiIiCiAg"
    "ICByZXR1cm4gZiJ7Z2I6LjJmfSIucnN0cmlwKCIwIikucnN0cmlwKCIuIikgKyAiIEdCIgoKCmFzeW5jIGRlZiBfYmFkX3Rhcmdl"
    "dChtZXNzYWdlLCB1aWQpOgogICAgIiIiVHJ1ZSB3aGVuIHRoZSByZXNvbHZlZCBpZCBjYW4ndCBiZSBhIHJlYWwgVGVsZWdyYW0g"
    "dXNlciAocGhhbnRvbSBndWFyZCkuIiIiCiAgICBpZiBub3QgKDAgPCB1aWQgPCAxMDAwMDApOgogICAgICAgIHJldHVybiBGYWxz"
    "ZQogICAgaWYgYXdhaXQgdXNlcl9leGlzdHModWlkKToKICAgICAgICByZXR1cm4gRmFsc2UKICAgIGF3YWl0IHNlbmRfbWVzc2Fn"
    "ZSgKICAgICAgICBtZXNzYWdlLAogICAgICAgICLinZMgVGhhdCBkb2Vzbid0IGxvb2sgbGlrZSBhIFRlbGVncmFtIHVzZXIgSUQg"
    "4oCUIGl0IHdvdWxkIGNyZWF0ZSBhICIKICAgICAgICAicGhhbnRvbSByZWdpc3RyeSBlbnRyeS4gUmVwbHkgdG8gdGhlIHVzZXIn"
    "cyBtZXNzYWdlIGluc3RlYWQsIG9yIHVzZSAiCiAgICAgICAgInRoZWlyIG51bWVyaWMgSUQgKHNlZSAvcXVzZXJzKS4iLAogICAg"
    "KQogICAgcmV0dXJuIFRydWUKCgpkZWYgX2ZtdF9kYXkoKToKICAgIHJldHVybiBkYXRldGltZS5ub3coSVNUKS5zdHJmdGltZSgi"
    "JWQgJWIgJVkiKQoKCmFzeW5jIGRlZiBfc3RhcnRfYm9vdF9yZXBvcnQoKToKICAgIGF3YWl0IGJvb3RfcmVwb3J0KCkKCgpib3Rf"
    "bG9vcC5jcmVhdGVfdGFzayhfc3RhcnRfYm9vdF9yZXBvcnQoKSkKdHJ5OgogICAgc3RhcnRfbG9vcHMoKSAgIyByMjogZGFpbHkg"
    "dXNhZ2UgcmVwb3J0IGF0IDIzOjU3IElTVApleGNlcHQgRXhjZXB0aW9uOgogICAgcGFzcwoKCiMg4pSA4pSAIC9maW5kIOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiB3emZpeF9maW5kKGNsaWVudCwgbWVzc2Fn"
    "ZSk6CiAgICBhcmdzID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3BsaXQobWF4c3BsaXQ9MSkKICAgIHF1ZXJ5ID0gYXJnc1sxXS5z"
    "dHJpcCgpIGlmIGxlbihhcmdzKSA+IDEgZWxzZSAiIgogICAgb3duZXIgPSBfaXNfb3duZXIobWVzc2FnZSkKICAgIGFsbF91c2Vy"
    "cyA9IEZhbHNlCiAgICBpZiBvd25lciBhbmQgcXVlcnkuc3RhcnRzd2l0aCgiLWEgIik6CiAgICAgICAgYWxsX3VzZXJzID0gVHJ1"
    "ZQogICAgICAgIHF1ZXJ5ID0gcXVlcnlbMzpdLnN0cmlwKCkKICAgIGlmIG5vdCBhd2FpdCBfZGJfb2soKToKICAgICAgICBhd2Fp"
    "dCBhdXRvX2RlbGV0ZV9tZXNzYWdlKGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikpCiAgICAgICAgcmV0dXJu"
    "CiAgICBpZiBub3QgcXVlcnk6CiAgICAgICAgYXdhaXQgYXV0b19kZWxldGVfbWVzc2FnZSgKICAgICAgICAgICAgYXdhaXQgc2Vu"
    "ZF9tZXNzYWdlKAogICAgICAgICAgICAgICAgbWVzc2FnZSwKICAgICAgICAgICAgICAgICLwn5SOIDxiPldaRklYIExpYnJhcnk8"
    "L2I+XG7ilIJcbiIKICAgICAgICAgICAgICAgICLilKAgPGNvZGU+L2ZpbmQgaW5jZXB0aW9uPC9jb2RlPiDigJQgc2VhcmNoIHlv"
    "dXIgZG93bmxvYWRzXG4iCiAgICAgICAgICAgICAgICAi4pSWIDxjb2RlPi9maW5kIC1hIHF1ZXJ5PC9jb2RlPiDigJQgb3duZXI6"
    "IHNlYXJjaCBldmVyeSB1c2VyJ3MgZG93bmxvYWRzXG4iCiAgICAgICAgICAgICAgICAi4pSDXG48aT5TaG93cyBzaXplLCBkYXRl"
    "LCBUZWxlZ3JhbSBwb3N0IGxpbmtzIGZvciBhbGwgcGFydHMsICIKICAgICAgICAgICAgICAgICJhbmQgU3RyZWFtIC8gRG93bmxv"
    "YWQgYnV0dG9ucy48L2k+IiwKICAgICAgICAgICAgKQogICAgICAgICkKICAgICAgICByZXR1cm4KICAgIHVpZCA9IChtZXNzYWdl"
    "LmZyb21fdXNlciBvciBtZXNzYWdlLnNlbmRlcl9jaGF0KS5pZAogICAgZG9jcyA9IGF3YWl0IGZpbmQocXVlcnksIHVpZCwgYWxs"
    "X3VzZXJzKQogICAgaWYgbm90IGRvY3M6CiAgICAgICAgYXdhaXQgYXV0b19kZWxldGVfbWVzc2FnZSgKICAgICAgICAgICAgYXdh"
    "aXQgc2VuZF9tZXNzYWdlKAogICAgICAgICAgICAgICAgbWVzc2FnZSwKICAgICAgICAgICAgICAgIGYi8J+YlSBObyBsaWJyYXJ5"
    "IHJlc3VsdHMgZm9yIDxjb2RlPntlc2NhcGUocXVlcnkpfTwvY29kZT4uIgogICAgICAgICAgICAgICAgKyAoIiIgaWYgYWxsX3Vz"
    "ZXJzIGVsc2UgIiAobGFzdCA5MCBkYXlzKSIpLAogICAgICAgICAgICApCiAgICAgICAgKQogICAgICAgIHJldHVybgogICAgZm9y"
    "IGksIGQgaW4gZW51bWVyYXRlKGRvY3MsIDEpOgogICAgICAgIGJ0biA9IEJ1dHRvbk1ha2VyKCkKICAgICAgICBwYXJ0cyA9IGQu"
    "Z2V0KCJwYXJ0cyIpIG9yIFtdCiAgICAgICAgaWYgcGFydHM6CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGZyb20g"
    "LnN0cmVhbSBpbXBvcnQgZ2VuX3N0cmVhbV9saW5rCgogICAgICAgICAgICAgICAgc2wgPSBhd2FpdCBnZW5fc3RyZWFtX2xpbmso"
    "cGFydHNbMF1bMF0sIHBhcnRzWzBdWzFdKQogICAgICAgICAgICAgICAgaWYgc2w6CiAgICAgICAgICAgICAgICAgICAgYnRuLnVy"
    "bF9idXR0b24oIuKWtiBTdHJlYW0iLCBzbFswXSkKICAgICAgICAgICAgICAgICAgICBidG4udXJsX2J1dHRvbigi4qyHIERvd25s"
    "b2FkIiwgc2xbMV0pCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBwYXNzCiAgICAgICAgd2hl"
    "biA9IGRhdGV0aW1lLmZyb210aW1lc3RhbXAoZC5nZXQoImRhdGUiKSBvciAwLCBJU1QpLnN0cmZ0aW1lKAogICAgICAgICAgICAi"
    "JWQvJW0vJXkgJUg6JU0iCiAgICAgICAgKQogICAgICAgIHVuYW1lID0gZC5nZXQoInVuYW1lIikgb3IgIiIKICAgICAgICB3aG8g"
    "PSBmIkB7ZXNjYXBlKHVuYW1lKX0iIGlmIHVuYW1lIGVsc2UgZiI8Y29kZT57ZC5nZXQoJ3VzZXJfaWQnKX08L2NvZGU+IgogICAg"
    "ICAgIG1zZyA9ICgKICAgICAgICAgICAgZiLwn5SOIDxiPlJlc3VsdCB7aX0ve2xlbihkb2NzKX08L2I+XG4iCiAgICAgICAgICAg"
    "IGYiPGI+e2VzY2FwZShkLmdldCgnbmFtZScpIG9yICc/Jyl9PC9iPlxuIgogICAgICAgICAgICBmIuKUoCA8Yj5TaXplPC9iPiDi"
    "hpIge19yKGQuZ2V0KCdzaXplJykgb3IgMCl9XG4iCiAgICAgICAgICAgIGYi4pSgIDxiPldoZW48L2I+IOKGkiB7d2hlbn0gSVNU"
    "XG4iCiAgICAgICAgICAgIGYi4pSgIDxiPkJ5PC9iPiDihpIge3dob30iCiAgICAgICAgKQogICAgICAgIGxpbmtzID0gZC5nZXQo"
    "InRnX2xpbmtzIikgb3IgW10KICAgICAgICBpZiBsZW4obGlua3MpID09IDE6CiAgICAgICAgICAgIG1zZyArPSBmIlxu4pSWIDxi"
    "PlBvc3Q8L2I+IOKGkiA8YSBocmVmPSd7bGlua3NbMF19Jz5vcGVuIGluIFRlbGVncmFtPC9hPiIKICAgICAgICBlbGlmIGxpbmtz"
    "OgogICAgICAgICAgICBtc2cgKz0gIlxu4pSWIDxiPlBhcnRzPC9iPiDihpIgIiArICIgfCAiLmpvaW4oCiAgICAgICAgICAgICAg"
    "ICBmIjxhIGhyZWY9J3tsfSc+UGFydCB7an08L2E+IiBmb3IgaiwgbCBpbiBlbnVtZXJhdGUobGlua3MsIDEpCiAgICAgICAgICAg"
    "ICkKICAgICAgICBlbGlmIGQuZ2V0KCJjbG91ZF9saW5rIik6CiAgICAgICAgICAgIG1zZyArPSBmIlxu4pSWIDxiPkNsb3VkPC9i"
    "PiDihpIgPGEgaHJlZj0ne2RbJ2Nsb3VkX2xpbmsnXX0nPm9wZW48L2E+IgogICAgICAgIGVsaWYgZC5nZXQoInJjbG9uZV9wYXRo"
    "Iik6CiAgICAgICAgICAgIG1zZyArPSBmIlxu4pSWIDxiPlBhdGg8L2I+IOKGkiA8Y29kZT57ZXNjYXBlKGRbJ3JjbG9uZV9wYXRo"
    "J10pfTwvY29kZT4iCiAgICAgICAgdHJ5OgogICAgICAgICAgICBuX2J0bnMgPSBsZW4oYnRuLmJ1dHRvbnMuZ2V0KCJkZWZhdWx0"
    "Iikgb3IgW10pCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgbl9idG5zID0gMQogICAgICAgIGF3YWl0IHNl"
    "bmRfbWVzc2FnZShtZXNzYWdlLCBtc2csIGJ0bi5idWlsZF9tZW51KDIpIGlmIG5fYnRucyBlbHNlIE5vbmUpCgoKIyDilIDilIAg"
    "L3VzYWdlIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiB3emZpeF91c2FnZShj"
    "bGllbnQsIG1lc3NhZ2UpOgogICAgdWlkID0gKG1lc3NhZ2UuZnJvbV91c2VyIG9yIG1lc3NhZ2Uuc2VuZGVyX2NoYXQpLmlkCiAg"
    "ICB1c2VkID0gYXdhaXQgZ2V0X3VzYWdlKHVpZCkKICAgIGNhcCA9IGF3YWl0IGdldF9jYXBfYnl0ZXModWlkKQogICAgcmVzZXJ2"
    "ZWQgPSBhd2FpdCByZXNlcnZlZF90b2RheSh1aWQpCiAgICBoZWxkID0gZiIgPGk+KCt7X3IocmVzZXJ2ZWQpfSBpbiBydW5uaW5n"
    "IHRhc2tzKTwvaT4iIGlmIHJlc2VydmVkIGVsc2UgIiIKICAgIG1zZyA9ICgKICAgICAgICBmIvCfk4ogPGI+QmFuZHdpZHRoIOKA"
    "lCB7X2ZtdF9kYXkoKX0gKElTVCk8L2I+XG7ilIJcbiIKICAgICAgICBmIuKUoCA8Yj5Zb3U8L2I+IOKGkiB7X3IodXNlZCl9e2hl"
    "bGR9IC8ge19yKGNhcCl9XG4iCiAgICAgICAgZiLilJYgUmVzZXRzIGF0IDAwOjAwIElTVCIKICAgICkKICAgIGlmIF9pc19vd25l"
    "cihtZXNzYWdlKToKICAgICAgICByb3dzID0gYXdhaXQgdG9kYXlfcm93cygpCiAgICAgICAgaWYgcm93czoKICAgICAgICAgICAg"
    "dWRvY3MgPSBhd2FpdCBhbGxfdXNlcnMoKQogICAgICAgICAgICB1bmFtZXMgPSB7dVsiX2lkIl06IHUuZ2V0KCJ1bmFtZSIpIG9y"
    "ICIiIGZvciB1IGluIHVkb2NzfQogICAgICAgICAgICBtc2cgKz0gIlxu4pSDXG7wn5GlIDxiPkV2ZXJ5b25lIHRvZGF5OjwvYj4i"
    "CiAgICAgICAgICAgIGZvciByb3cgaW4gcm93c1s6MTVdOgogICAgICAgICAgICAgICAgaWYgcm93WyJ1c2VyX2lkIl0gPT0gdWlk"
    "OgogICAgICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAgICAgICB1biA9IHVuYW1lcy5nZXQocm93WyJ1c2VyX2lk"
    "Il0sICIiKQogICAgICAgICAgICAgICAgd2hvID0gZiJAe2VzY2FwZSh1bil9IiBpZiB1biBlbHNlIGYiPGNvZGU+e3Jvd1sndXNl"
    "cl9pZCddfTwvY29kZT4iCiAgICAgICAgICAgICAgICBtc2cgKz0gZiJcbuKUoCB7d2hvfSDihpIge19yKHJvd1sndXNlZCddKX0i"
    "CiAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgbXNnKQoKCiMg4pSA4pSAIC9xdXNlcnMg4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIHd6Zml4X3F1c2VycyhjbGllbnQsIG1lc3NhZ2UpOgogICAgaWYgbm90"
    "IGF3YWl0IF9kYl9vaygpOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikKICAgICAgICByZXR1"
    "cm4KICAgIHVzZXJzID0gYXdhaXQgYWxsX3VzZXJzKCkKICAgIGlmIG5vdCB1c2VyczoKICAgICAgICBhd2FpdCBzZW5kX21lc3Nh"
    "Z2UobWVzc2FnZSwgIk5vIHVzZXJzIHJlY29yZGVkIHlldCDigJQgdGhleSBhcHBlYXIgYWZ0ZXIgdGhlaXIgZmlyc3QgdGFzay4i"
    "KQogICAgICAgIHJldHVybgogICAgcm93cyA9IGF3YWl0IHRvZGF5X3Jvd3MoKQogICAgdG9kYXkgPSB7clsidXNlcl9pZCJdOiBy"
    "WyJ1c2VkIl0gZm9yIHIgaW4gcm93c30KICAgIG1zZyA9IGYi8J+RpSA8Yj5Vc2VyIHJlZ2lzdHJ5PC9iPiDigJQge19mbXRfZGF5"
    "KCl9IChJU1QpXG7ilIIiCiAgICBmb3IgdSBpbiB1c2Vyc1s6MjVdOgogICAgICAgIHVpZCA9IHVbIl9pZCJdCiAgICAgICAgdW4g"
    "PSB1LmdldCgidW5hbWUiKSBvciAiIgogICAgICAgIHdobyA9IGYiQHtlc2NhcGUodW4pfSIgaWYgdW4gZWxzZSBlc2NhcGUodS5n"
    "ZXQoIm5hbWUiKSBvciBzdHIodWlkKSkKICAgICAgICBjYXAgPSB1LmdldCgiY2FwX2diIikKICAgICAgICBjYXBfcyA9IGYie2Nh"
    "cDpnfSBHQiIgaWYgY2FwIGlzIG5vdCBOb25lIGVsc2UgImRlZmF1bHQiCiAgICAgICAgbXNnICs9ICgKICAgICAgICAgICAgZiJc"
    "buKUoCA8Yj57ZXNjYXBlKHdobyl9PC9iPlxuIgogICAgICAgICAgICBmIuKUgyAgIElEIDxjb2RlPnt1aWR9PC9jb2RlPiDCtyB0"
    "b2RheSB7X3IodG9kYXkuZ2V0KHVpZCwgMCkpfSDCtyAiCiAgICAgICAgICAgIGYibGlmZXRpbWUge19yKHUuZ2V0KCd0b3RhbF91"
    "c2VkJykgb3IgMCl9IMK3IGNhcCB7Y2FwX3N9IMK3ICIKICAgICAgICAgICAgZiJ0YXNrcyB7dS5nZXQoJ3Rhc2tzJykgb3IgMH0i"
    "CiAgICAgICAgKQogICAgaWYgbGVuKHVzZXJzKSA+IDI1OgogICAgICAgIG1zZyArPSBmIlxu4pSDIOKApmFuZCB7bGVuKHVzZXJz"
    "KSAtIDI1fSBtb3JlIgogICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIG1zZykKCgojIOKUgOKUgCBjYXAgY29tbWFuZHMg"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIHd6Zml4X3NldGNhcChjbGllbnQsIG1lc3NhZ2UpOgogICAgaWYg"
    "bm90IGF3YWl0IF9kYl9vaygpOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikKICAgICAgICBy"
    "ZXR1cm4KICAgIGFyZ3MgPSAobWVzc2FnZS50ZXh0IG9yICIiKS5zcGxpdCgpWzE6XQogICAgdWlkLCBnYiA9IF9wYXJzZV90YXJn"
    "ZXRfYW5kX2diKG1lc3NhZ2UsIGFyZ3MpCiAgICBpZiBub3QgdWlkOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdl"
    "LCAiVXNhZ2U6IDxjb2RlPi9zZXRjYXAgPHVzZXJfaWQ+IDxHQj48L2NvZGU+IChvciByZXBseSB0byBhIHVzZXIpLiAwID0gYmFj"
    "ayB0byBnbG9iYWwgZGVmYXVsdC4iKQogICAgICAgIHJldHVybgogICAgaWYgYXdhaXQgX2JhZF90YXJnZXQobWVzc2FnZSwgdWlk"
    "KToKICAgICAgICByZXR1cm4KICAgIGlmIGdiIGlzIE5vbmU6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsICJV"
    "c2FnZTogPGNvZGU+L3NldGNhcCA8dXNlcl9pZD4gPEdCPjwvY29kZT4g4oCUIGdpdmUgYSBudW1iZXIgaW4gR0IuIikKICAgICAg"
    "ICByZXR1cm4KICAgIGF3YWl0IHNldF91c2VyX2NhcCh1aWQsIGdiIGlmIGdiID4gMCBlbHNlIE5vbmUpCiAgICBsYWJlbCA9IF9m"
    "bXRfZ2IoZ2IpIGlmIGdiIGFuZCBnYiA+IDAgZWxzZSAiZ2xvYmFsIGRlZmF1bHQiCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVz"
    "c2FnZSwgZiLinIUgQ2FwIGZvciA8Y29kZT57dWlkfTwvY29kZT4gc2V0IHRvIDxiPntsYWJlbH08L2I+LiIpCgoKYXN5bmMgZGVm"
    "IHd6Zml4X2FkZGNhcChjbGllbnQsIG1lc3NhZ2UpOgogICAgaWYgbm90IGF3YWl0IF9kYl9vaygpOgogICAgICAgIGF3YWl0IHNl"
    "bmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikKICAgICAgICByZXR1cm4KICAgIGFyZ3MgPSAobWVzc2FnZS50ZXh0IG9yICIi"
    "KS5zcGxpdCgpWzE6XQogICAgdWlkLCBnYiA9IF9wYXJzZV90YXJnZXRfYW5kX2diKG1lc3NhZ2UsIGFyZ3MpCiAgICBpZiBub3Qg"
    "dWlkOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCAiVXNhZ2U6IDxjb2RlPi9hZGRjYXAgPHVzZXJfaWQ+IDxH"
    "Qj48L2NvZGU+IChvciByZXBseSB0byBhIHVzZXIpLiIpCiAgICAgICAgcmV0dXJuCiAgICBpZiBhd2FpdCBfYmFkX3RhcmdldCht"
    "ZXNzYWdlLCB1aWQpOgogICAgICAgIHJldHVybgogICAgaWYgZ2IgaXMgTm9uZSBvciBnYiA8PSAwOgogICAgICAgIGF3YWl0IHNl"
    "bmRfbWVzc2FnZShtZXNzYWdlLCAiVXNhZ2U6IDxjb2RlPi9hZGRjYXAgPHVzZXJfaWQ+IDxHQj48L2NvZGU+IOKAlCBob3cgbWFu"
    "eSBHQiB0byBhZGQuIikKICAgICAgICByZXR1cm4KICAgIHVkb2MgPSBhd2FpdCBnZXRfdXNlcl9kb2NfbG9jYWwodWlkKQogICAg"
    "Y3VyID0gdWRvYy5nZXQoImNhcF9nYiIpCiAgICBmcm9tIC4uaGVscGVyLnd6Zml4LnIxX2NvcmUgaW1wb3J0IF9nZXRfZ2xvYmFs"
    "X2NhcF9nYgoKICAgIGJhc2UgPSBmbG9hdChjdXIpIGlmIGN1ciBpcyBub3QgTm9uZSBlbHNlIGF3YWl0IF9nZXRfZ2xvYmFsX2Nh"
    "cF9nYigpCiAgICBhd2FpdCBzZXRfdXNlcl9jYXAodWlkLCBiYXNlICsgZ2IpCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAg"
    "ICAgbWVzc2FnZSwKICAgICAgICBmIuKchSBDYXAgZm9yIDxjb2RlPnt1aWR9PC9jb2RlPiByYWlzZWQgdG8gPGI+e19mbXRfZ2Io"
    "YmFzZSArIGdiKX08L2I+L2RheS4iLAogICAgKQoKCmFzeW5jIGRlZiB3emZpeF9kZWR1Y3RjYXAoY2xpZW50LCBtZXNzYWdlKToK"
    "ICAgICIiIk9wcG9zaXRlIG9mIC9hZGRjYXA6IGxvd2VyIGEgdXNlcidzIGNhcCBieSBHQiAoZmxvb3IgMCA9IGJsb2NrZWQpLiIi"
    "IgogICAgaWYgbm90IGF3YWl0IF9kYl9vaygpOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikK"
    "ICAgICAgICByZXR1cm4KICAgIGFyZ3MgPSAobWVzc2FnZS50ZXh0IG9yICIiKS5zcGxpdCgpWzE6XQogICAgdWlkLCBnYiA9IF9w"
    "YXJzZV90YXJnZXRfYW5kX2diKG1lc3NhZ2UsIGFyZ3MpCiAgICBpZiBub3QgdWlkOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2Fn"
    "ZShtZXNzYWdlLCAiVXNhZ2U6IDxjb2RlPi9kZWR1Y3RjYXAgPHVzZXJfaWQ+IDxHQj48L2NvZGU+IChvciByZXBseSB0byBhIHVz"
    "ZXIpLiIpCiAgICAgICAgcmV0dXJuCiAgICBpZiBhd2FpdCBfYmFkX3RhcmdldChtZXNzYWdlLCB1aWQpOgogICAgICAgIHJldHVy"
    "bgogICAgaWYgZ2IgaXMgTm9uZSBvciBnYiA8PSAwOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCAiVXNhZ2U6"
    "IDxjb2RlPi9kZWR1Y3RjYXAgPHVzZXJfaWQ+IDxHQj48L2NvZGU+IOKAlCBob3cgbWFueSBHQiB0byBzdWJ0cmFjdC4iKQogICAg"
    "ICAgIHJldHVybgogICAgdWRvYyA9IGF3YWl0IGdldF91c2VyX2RvY19sb2NhbCh1aWQpCiAgICBjdXIgPSB1ZG9jLmdldCgiY2Fw"
    "X2diIikKICAgIGZyb20gLi5oZWxwZXIud3pmaXgucjFfY29yZSBpbXBvcnQgX2dldF9nbG9iYWxfY2FwX2diCgogICAgYmFzZSA9"
    "IGZsb2F0KGN1cikgaWYgY3VyIGlzIG5vdCBOb25lIGVsc2UgYXdhaXQgX2dldF9nbG9iYWxfY2FwX2diKCkKICAgIG5ld19jYXAg"
    "PSBtYXgoYmFzZSAtIGdiLCAwKQogICAgYXdhaXQgc2V0X3VzZXJfY2FwKHVpZCwgbmV3X2NhcCkKICAgIGlmIG5ld19jYXAgPD0g"
    "MDoKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgIG1lc3NhZ2UsCiAgICAgICAgICAgIGYi4puUIENhcCBm"
    "b3IgPGNvZGU+e3VpZH08L2NvZGU+IGlzIG5vdyA8Yj4wIEdCPC9iPiDigJQgZnVsbHkgYmxvY2tlZC4gIgogICAgICAgICAgICBm"
    "IlVzZSA8Y29kZT4vc2V0Y2FwIHt1aWR9IDA8L2NvZGU+IHRvIHJldHVybiB0aGVtIHRvIHRoZSBnbG9iYWwgZGVmYXVsdC4iLAog"
    "ICAgICAgICkKICAgIGVsc2U6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKAogICAgICAgICAgICBtZXNzYWdlLAogICAgICAg"
    "ICAgICBmIuKchSBDYXAgZm9yIDxjb2RlPnt1aWR9PC9jb2RlPiBsb3dlcmVkIHRvIDxiPntfZm10X2diKG5ld19jYXApfTwvYj4v"
    "ZGF5LiIsCiAgICAgICAgKQoKCmFzeW5jIGRlZiB3emZpeF9kZWx1c2VyKGNsaWVudCwgbWVzc2FnZSk6CiAgICAiIiJQZXJtYW5l"
    "bnRseSByZW1vdmUgYSB1c2VyIGZyb20gdGhlIHJlZ2lzdHJ5ICgrIHRoZWlyIHVzYWdlIGhpc3RvcnkpLiIiIgogICAgaWYgbm90"
    "IGF3YWl0IF9kYl9vaygpOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBfREJfRE9XTikKICAgICAgICByZXR1"
    "cm4KICAgIGFyZ3MgPSAobWVzc2FnZS50ZXh0IG9yICIiKS5zcGxpdCgpWzE6XQogICAgdWlkID0gX3RhcmdldF91c2VyKG1lc3Nh"
    "Z2UsIGFyZ3MpCiAgICBpZiBub3QgdWlkOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCAiVXNhZ2U6IDxjb2Rl"
    "Pi9kZWx1c2VyIDx1c2VyX2lkPjwvY29kZT4gKG9yIHJlcGx5IHRvIGEgdXNlcikuIikKICAgICAgICByZXR1cm4KICAgIHJlZywg"
    "cm93cyA9IGF3YWl0IGRlbGV0ZV91c2VyKHVpZCkKICAgIGlmIG5vdCByZWcgYW5kIG5vdCByb3dzOgogICAgICAgIGF3YWl0IHNl"
    "bmRfbWVzc2FnZShtZXNzYWdlLCBmIvCfpLcgVXNlciA8Y29kZT57dWlkfTwvY29kZT4gd2Fzbid0IGluIHRoZSByZWdpc3RyeS4i"
    "KQogICAgICAgIHJldHVybgogICAgYXdhaXQgc2VuZF9tZXNzYWdlKAogICAgICAgIG1lc3NhZ2UsCiAgICAgICAgZiLwn5eRIFJl"
    "bW92ZWQgdXNlciA8Y29kZT57dWlkfTwvY29kZT4g4oCUIHJlZ2lzdHJ5IGVudHJ5IGRlbGV0ZWQsICIKICAgICAgICBmIntyb3dz"
    "fSB1c2FnZSByb3cocykgY2xlYXJlZC4gVGhlaXIgL2ZpbmQgZG93bmxvYWQgaGlzdG9yeSBpcyBrZXB0ICIKICAgICAgICBmIihl"
    "eHBpcmVzIG5hdHVyYWxseSBhZnRlciA5MCBkYXlzKS4iLAogICAgKQoKCmFzeW5jIGRlZiBnZXRfdXNlcl9kb2NfbG9jYWwodWlk"
    "KToKICAgIGZyb20gLi5oZWxwZXIud3pmaXgucjFfY29yZSBpbXBvcnQgZ2V0X3VzZXJfZG9jCgogICAgcmV0dXJuIGF3YWl0IGdl"
    "dF91c2VyX2RvYyh1aWQpCgoKYXN5bmMgZGVmIHd6Zml4X3Jlc2V0Y2FwKGNsaWVudCwgbWVzc2FnZSk6CiAgICBpZiBub3QgYXdh"
    "aXQgX2RiX29rKCk6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIF9EQl9ET1dOKQogICAgICAgIHJldHVybgog"
    "ICAgYXJncyA9IChtZXNzYWdlLnRleHQgb3IgIiIpLnNwbGl0KClbMTpdCiAgICB1aWQgPSBfdGFyZ2V0X3VzZXIobWVzc2FnZSwg"
    "YXJncykKICAgIGlmIG5vdCB1aWQ6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsICJVc2FnZTogPGNvZGU+L3Jl"
    "c2V0Y2FwIDx1c2VyX2lkPjwvY29kZT4gKG9yIHJlcGx5IHRvIGEgdXNlcikuIikKICAgICAgICByZXR1cm4KICAgIGF3YWl0IHJl"
    "c2V0X3VzYWdlKHVpZCkKICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBmIuKZu++4jyBUb2RheSdzIHVzYWdlIGZvciA8"
    "Y29kZT57dWlkfTwvY29kZT4gcmVzZXQgdG8gMC4iKQoKCmFzeW5jIGRlZiB3emZpeF9ib3RjYXAoY2xpZW50LCBtZXNzYWdlKToK"
    "ICAgIGlmIG5vdCBhd2FpdCBfZGJfb2soKToKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgX0RCX0RPV04pCiAg"
    "ICAgICAgcmV0dXJuCiAgICBhcmdzID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3BsaXQoKVsxOl0KICAgIGlmIG5vdCBhcmdzIG9y"
    "IG5vdCBhcmdzWzBdLnJlcGxhY2UoIi4iLCAiIiwgMSkuaXNkaWdpdCgpOgogICAgICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNz"
    "YWdlLCAiVXNhZ2U6IDxjb2RlPi9ib3RjYXAgPEdCPjwvY29kZT4g4oCUIGdsb2JhbCBkZWZhdWx0IGRhaWx5IGNhcCBmb3IgZXZl"
    "cnkgdXNlci4iKQogICAgICAgIHJldHVybgogICAgZ2IgPSBmbG9hdChhcmdzWzBdKQogICAgaWYgZ2IgPD0gMDoKICAgICAgICBh"
    "d2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgIkNhcCBtdXN0IGJlID4gMCBHQi4iKQogICAgICAgIHJldHVybgogICAgYXdhaXQg"
    "c2V0X2dsb2JhbF9jYXBfZ2IoZ2IpCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UobWVzc2FnZSwgZiLinIUgR2xvYmFsIGRhaWx5IGNh"
    "cCBzZXQgdG8gPGI+e2diOmd9IEdCPC9iPiAob3duZXIgJiBzdWRvIHN0YXkgdW5saW1pdGVkKS4iKQoKCmFzeW5jIGRlZiB3emZp"
    "eF9kaWFnKGNsaWVudCwgbWVzc2FnZSk6CiAgICAiIiJPd25lci1vbmx5IGxpdmUgZHVtcCBvZiBldmVyeXRoaW5nIHF1b3RhLXJl"
    "bGF0ZWQgZm9yIG9uZSB1c2VyLiIiIgogICAgYXJncyA9IChtZXNzYWdlLnRleHQgb3IgIiIpLnNwbGl0KClbMTpdCiAgICB1aWQg"
    "PSBfdGFyZ2V0X3VzZXIobWVzc2FnZSwgYXJncykKICAgIGlmIG5vdCB1aWQ6CiAgICAgICAgYXdhaXQgc2VuZF9tZXNzYWdlKAog"
    "ICAgICAgICAgICBtZXNzYWdlLAogICAgICAgICAgICAiVXNhZ2U6IDxjb2RlPi93emZpeGRpYWcgPHVzZXJfaWQ+PC9jb2RlPiDi"
    "gJQgc2hvd3MgcGFydGl0aW9uLCBjYXAsICIKICAgICAgICAgICAgInRvZGF5J3MgdXNhZ2UgZG9jIGFuZCBhIHNpbXVsYXRlZCAy"
    "IEdCIHZlcmRpY3QuIiwKICAgICAgICApCiAgICAgICAgcmV0dXJuCiAgICBmcm9tIC4uaGVscGVyLnd6Zml4LnIxX2NvcmUgaW1w"
    "b3J0IGRpYWcKCiAgICBkID0gYXdhaXQgZGlhZyh1aWQsIHByb2JlX2diPTIpCiAgICBxZCA9IGQuZ2V0KCJxdW90YV9kb2MiKSBv"
    "ciB7fQogICAgdWRvYyA9IGQuZ2V0KCJ1c2VyX2RvYyIpIG9yIHt9CiAgICBwcm9iZSA9IGQuZ2V0KCJwcm9iZSIpIG9yIHt9CiAg"
    "ICBjYXBfY3VzdG9tID0gdWRvYy5nZXQoImNhcF9nYiIpCiAgICBjYXBfc3JjID0gKAogICAgICAgIGYiY3VzdG9tIHtfZm10X2di"
    "KGNhcF9jdXN0b20pfSIKICAgICAgICBpZiBjYXBfY3VzdG9tIGlzIG5vdCBOb25lCiAgICAgICAgZWxzZSBmImdsb2JhbCBkZWZh"
    "dWx0IHtfZm10X2diKGQuZ2V0KCdnbG9iYWxfY2FwX2diJykgb3IgMCl9IgogICAgKQogICAgcHJvYmVfdHh0ID0gKAogICAgICAg"
    "ICJ3b3VsZCA8Yj5CTE9DSzwvYj4gYSAyIEdCIHRhc2siIGlmIHByb2JlLmdldCgid291bGRfYmxvY2siKQogICAgICAgIGVsc2Ug"
    "IndvdWxkIDxiPkFMTE9XPC9iPiBhIDIgR0IgdGFzayIKICAgICkKICAgIG1zZyA9ICgKICAgICAgICBmIvCflKcgPGI+V1pGSVgg"
    "ZGlhZzwvYj4g4oCUIHVzZXIgPGNvZGU+e3VpZH08L2NvZGU+XG4iCiAgICAgICAgZiLilIJcbiIKICAgICAgICBmIuKUoCA8Yj5E"
    "QiBwYXJ0aXRpb248L2I+IOKGkiA8Y29kZT57ZC5nZXQoJ3BhcnRpdGlvbicpfTwvY29kZT5cbiIKICAgICAgICBmIuKUoCA8Yj5E"
    "YXkgKElTVCk8L2I+IOKGkiB7ZC5nZXQoJ2RheScpfVxuIgogICAgICAgIGYi4pSgIDxiPkRCIHJlYWR5PC9iPiDihpIge2QuZ2V0"
    "KCdkYl9yZWFkeScpfVxuIgogICAgICAgIGYi4pSgIDxiPkNhcDwvYj4g4oaSIHtfcihkLmdldCgnY2FwX2J5dGVzJykgb3IgMCl9"
    "ICh7Y2FwX3NyY30pXG4iCiAgICAgICAgZiLilKAgPGI+UmVzZXJ2ZWQgKHJ1bm5pbmcgdGFza3MpPC9iPiDihpIge19yKGQuZ2V0"
    "KCdyZXNlcnZlZCcpIG9yIDApfVxuIgogICAgICAgIGYi4pSgIDxiPlF1b3RhIGRvYzwvYj4g4oaSIDxjb2RlPntlc2NhcGUoc3Ry"
    "KHFkKSlbOjMwMF19PC9jb2RlPlxuIgogICAgICAgIGYi4pSgIDxiPlNpbXVsYXRpb248L2I+IOKGkiB7cHJvYmVfdHh0fSAiCiAg"
    "ICAgICAgZiIodXNlZCB7X3IocHJvYmUuZ2V0KCd1c2VkJykgb3IgMCl9IC8gY2FwIHtfcihwcm9iZS5nZXQoJ2NhcCcpIG9yIDAp"
    "fSlcbiIKICAgICAgICBmIuKUoCA8Yj5Vc2VyIGRvYzwvYj4g4oaSIDxjb2RlPntlc2NhcGUoc3RyKHVkb2MpKVs6MzAwXX08L2Nv"
    "ZGU+IgogICAgKQogICAgYXdhaXQgc2VuZF9tZXNzYWdlKG1lc3NhZ2UsIG1zZykKCgojIOKUgOKUgCBkYiB0b29scyDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgd3pmaXhfZGJzdGF0cyhjbGllbnQsIG1lc3NhZ2UpOgog"
    "ICAgc3QgPSBhd2FpdCBkYnN0YXRzKCkKICAgIGlmIHN0LmdldCgiZXJyb3IiKSBhbmQgbm90IHN0LmdldCgiY29scyIpOgogICAg"
    "ICAgIGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBmIkRCIHN0YXRzIGZhaWxlZDogPGNvZGU+e2VzY2FwZShzdFsnZXJyb3In"
    "XSl9PC9jb2RlPiIpCiAgICAgICAgcmV0dXJuCiAgICBpZiBzdC5nZXQoImFjY291bnRfdG90YWwiKToKICAgICAgICBwY3QgPSAo"
    "c3RbImFjY291bnRfdG90YWwiXSAvICg1MTIgKiAxMDI0KioyKSkgKiAxMDAKICAgICAgICBtc2cgPSAoCiAgICAgICAgICAgIGYi"
    "8J+XhCA8Yj5Nb25nb0RCIHVzYWdlPC9iPiDigJQge19mbXRfZGF5KCl9XG7ilIJcbiIKICAgICAgICAgICAgZiLilKAgPGI+QWNj"
    "b3VudCB0b3RhbCAoYWxsIGRhdGFiYXNlcyk8L2I+IOKGkiB7X3Ioc3RbJ2FjY291bnRfdG90YWwnXSl9ICIKICAgICAgICAgICAg"
    "ZiIoe3BjdDouMWZ9JSBvZiB0aGUgNTEyIE1CIGZyZWUgdGllcilcbiIKICAgICAgICAgICAgZiLilKAgPGI+VGhpcyBib3QncyBk"
    "YXRhYmFzZSAod3ptbHgpPC9iPiDihpIge19yKHN0Wyd0b3RhbCddKX1cbiIKICAgICAgICAgICAgKyAoIuKUoCDimqDvuI8gPGI+"
    "QWNjb3VudCBhYm92ZSA4MCUhPC9iPlxuIiBpZiBwY3QgPiA4MCBlbHNlICIiKQogICAgICAgICAgICArICLilINcbjxiPkRhdGFi"
    "YXNlcyBieSBzaXplPC9iPiAodGhlIDUxMiBNQiBjb3ZlcnMgQUxMIG9mIHRoZW0pOiIKICAgICAgICApCiAgICAgICAgZm9yIG5h"
    "bWUsIHN6IGluIHN0WyJkYnMiXVs6MTBdOgogICAgICAgICAgICB0YWcgPSAiIOKGkCB0aGlzIGJvdCdzIiBpZiBuYW1lID09ICJ3"
    "em1seCIgZWxzZSAiIgogICAgICAgICAgICBtc2cgKz0gZiJcbuKUoCA8Y29kZT57ZXNjYXBlKG5hbWUpfTwvY29kZT4g4oaSIHtf"
    "cihzeil9e3RhZ30iCiAgICAgICAgbXNnICs9ICJcbuKUg1xuPGI+d3ptbHggY29sbGVjdGlvbnM8L2I+IChzZWxmID0gdGhpcyBi"
    "b3QsIG90aGVyID0gb3RoZXIgYm90cyk6IgogICAgZWxzZToKICAgICAgICBwY3QgPSAoc3RbInRvdGFsIl0gLyAoNTEyICogMTAy"
    "NCoqMikpICogMTAwIGlmIHN0WyJ0b3RhbCJdIGVsc2UgMAogICAgICAgIG1zZyA9ICgKICAgICAgICAgICAgZiLwn5eEIDxiPk1v"
    "bmdvREIgdXNhZ2U8L2I+IOKAlCB7X2ZtdF9kYXkoKX1cbuKUglxuIgogICAgICAgICAgICBmIuKUoCA8Yj5Ub3RhbDwvYj4g4oaS"
    "IHtfcihzdFsndG90YWwnXSl9ICh7cGN0Oi4xZn0lIG9mIHRoZSA1MTIgTUIgZnJlZSB0aWVyKVxuIgogICAgICAgICAgICAiKGFj"
    "Y291bnQtd2lkZSB2aWV3IHVuYXZhaWxhYmxlIgogICAgICAgICAgICArIChmIjoge2VzY2FwZShzdFsnZGJfZXJyb3InXVs6ODBd"
    "KX0iIGlmIHN0LmdldCgiZGJfZXJyb3IiKSBlbHNlICIiKQogICAgICAgICAgICArICIpXG4iCiAgICAgICAgICAgICsgKCLilKAg"
    "4pqg77iPIDxiPkFib3ZlIDgwJSDigJQgcnVuIC9kYmNsZWFuPC9iPlxuIiBpZiBwY3QgPiA4MCBlbHNlICIiKQogICAgICAgICAg"
    "ICArICLilINcbjxiPlRvcCBjb2xsZWN0aW9uczwvYj4gKHNlbGYgPSB0aGlzIGJvdCwgb3RoZXIgPSBib3RzIHNoYXJpbmcgdGhl"
    "IGFjY291bnQpOiIKICAgICAgICApCiAgICBmb3IgbmFtZSwgYSBpbiBzdFsiY29scyJdWzoxMl06CiAgICAgICAgaWYgYVsic2l6"
    "ZSJdIDw9IDA6CiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgbXNnICs9IGYiXG7ilKAgPGNvZGU+e2VzY2FwZShuYW1lKX08"
    "L2NvZGU+IOKGkiB7X3IoYVsnc2l6ZSddKX0gwrcge2FbJ2RvY3MnXX0gZG9jcyIKICAgICAgICBiaXRzID0gW10KICAgICAgICBp"
    "ZiBhLmdldCgic2VsZiIpOgogICAgICAgICAgICBiaXRzLmFwcGVuZChmInRoaXMgYm90IHtfcihhWydzZWxmJ10pfSIpCiAgICAg"
    "ICAgaWYgYS5nZXQoIm90aGVyIik6CiAgICAgICAgICAgIGJpdHMuYXBwZW5kKGYib3RoZXIgYm90cyB7X3IoYVsnb3RoZXInXSl9"
    "IikKICAgICAgICBpZiBhLmdldCgic2hhcmVkIik6CiAgICAgICAgICAgIGJpdHMuYXBwZW5kKGYic2hhcmVkIHtfcihhWydzaGFy"
    "ZWQnXSl9IikKICAgICAgICBpZiBiaXRzOgogICAgICAgICAgICBtc2cgKz0gIiDCtyAiICsgIiDCtyAiLmpvaW4oYml0cykKICAg"
    "IGF3YWl0IHNlbmRfbWVzc2FnZShtZXNzYWdlLCBtc2cpCgoKYXN5bmMgZGVmIHd6Zml4X2RiY2xlYW4oY2xpZW50LCBtZXNzYWdl"
    "KToKICAgIGJ0biA9IEJ1dHRvbk1ha2VyKCkKICAgIGJ0bi5kYXRhX2J1dHRvbigi8J+nuSBZZXMsIGNsZWFuIiwgInd6Zml4Y2xl"
    "YW4iKQogICAgYnRuLmRhdGFfYnV0dG9uKCLinJYgQ2FuY2VsIiwgInd6Zml4Y2FuY2VsIikKICAgIHN0ID0gYXdhaXQgZGJzdGF0"
    "cygpCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgbWVzc2FnZSwKICAgICAgICAi8J+nuSA8Yj5EQiBjbGVhbnVwPC9i"
    "PiDigJQgd2lsbCBwdXJnZTpcbiIKICAgICAgICAi4pSgIGV4cGlyZWQgc3RyZWFtIHRva2Vuc1xuIgogICAgICAgICLilKAgaW5j"
    "b21wbGV0ZS10YXNrIHJlc3VtZSByZWNvcmRzXG4iCiAgICAgICAgIuKUoCBzdGFsZSB3emZpeCByb3dzXG4iCiAgICAgICAgZiLi"
    "lINcbuKUliBDdXJyZW50IHVzYWdlOiA8Yj57X3Ioc3RbJ3RvdGFsJ10pfTwvYj5cbiIKICAgICAgICAiUHJvY2VlZD8iLAogICAg"
    "ICAgIGJ0bi5idWlsZF9tZW51KDIpLAogICAgKQoKCmFzeW5jIGRlZiB3emZpeF9jbGVhbl9jYihjbGllbnQsIHF1ZXJ5KToKICAg"
    "IGlmIG5vdCBfaXNfc3Vkb19vcl9vd25lcihxdWVyeS5mcm9tX3VzZXIuaWQpOgogICAgICAgIGF3YWl0IHF1ZXJ5LmFuc3dlcigi"
    "T3duZXIgLyBzdWRvIG9ubHkuIiwgc2hvd19hbGVydD1UcnVlKQogICAgICAgIHJldHVybgogICAgaWYgbm90IGF3YWl0IF9kYl9v"
    "aygpOgogICAgICAgIGF3YWl0IHF1ZXJ5LmFuc3dlcigiRGF0YWJhc2Ugbm90IGNvbm5lY3RlZC4iLCBzaG93X2FsZXJ0PVRydWUp"
    "CiAgICAgICAgcmV0dXJuCiAgICBmcmVlZCA9IGF3YWl0IGRiY2xlYW4oKQogICAgYm9keSA9ICJcbiIuam9pbihmIuKUoCB7a30g"
    "4oaSIHt2fSIgZm9yIGssIHYgaW4gZnJlZWQuaXRlbXMoKSkgb3IgIuKUoCBub3RoaW5nIHRvIHB1cmdlIgogICAgdHJ5OgogICAg"
    "ICAgIGF3YWl0IHF1ZXJ5LmFuc3dlcigpCiAgICAgICAgYXdhaXQgcXVlcnkuZWRpdF9tZXNzYWdlX3RleHQoCiAgICAgICAgICAg"
    "IGYi8J+nuSA8Yj5EQiBjbGVhbnVwIGRvbmU8L2I+XG7ilIJcbntib2R5fVxu4pSDXG7ilJYgUnVuIC9kYnN0YXRzIHRvIHNlZSB0"
    "aGUgbmV3IHNpemUuIgogICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwoKCmRlZiBfaXNfc3Vkb19v"
    "cl9vd25lcih1aWQpOgogICAgaWYgdWlkID09IENvbmZpZy5PV05FUl9JRDoKICAgICAgICByZXR1cm4gVHJ1ZQogICAgcmV0dXJu"
    "IGJvb2wodXNlcl9kYXRhLmdldCh1aWQsIHt9KS5nZXQoIlNVRE8iKSkKCgphc3luYyBkZWYgd3pmaXhfY2FuY2VsX2NiKGNsaWVu"
    "dCwgcXVlcnkpOgogICAgdHJ5OgogICAgICAgIGF3YWl0IHF1ZXJ5LmFuc3dlcigpCiAgICAgICAgYXdhaXQgcXVlcnkuZWRpdF9t"
    "ZXNzYWdlX3RleHQoIuKcliBDbGVhbnVwIGNhbmNlbGxlZC4iKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoK"
    "YXN5bmMgZGVmIF9kYXNoX3VybCgpOgogICAgIiIiVGhlIGRhc2hib2FyZCBVUkwgYnVpbHQgZnJvbSB0aGUgbGl2ZSBCQVNFX1VS"
    "TCAod29ya2VyL3R1bm5lbCkuIiIiCiAgICB0cnk6CiAgICAgICAgYiA9IChnZXRhdHRyKENvbmZpZywgIkJBU0VfVVJMIiwgIiIp"
    "IG9yICIiKS5zdHJpcCgpLnJzdHJpcCgiLyIpCiAgICAgICAgaWYgYjoKICAgICAgICAgICAgcmV0dXJuIGYie2J9L3d6YWRtaW4i"
    "CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJldHVybiAieW91ciB3b3JrZXIgbGluayArIC93emFkbWlu"
    "IgoKCiMg4pSA4pSAIC9hZG1pbnBhc3Mg4oCUIHdlYiBkYXNoYm9hcmQgcGFzc3dvcmQgKHYxNS44KSDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYg"
    "d3pmaXhfYWRtaW5wYXNzKGNsaWVudCwgbWVzc2FnZSk6CiAgICAiIiJTaG93IG9yIHNldCB0aGUgd2ViIGRhc2hib2FyZCBwYXNz"
    "d29yZCAob3duZXIvc3VkbyBvbmx5KS4iIiIKICAgIGlmIG5vdCBhd2FpdCBfZGJfb2soKToKICAgICAgICBhd2FpdCBzZW5kX21l"
    "c3NhZ2UobWVzc2FnZSwgX0RCX0RPV04pCiAgICAgICAgcmV0dXJuCiAgICBhcmdzID0gKG1lc3NhZ2UudGV4dCBvciAiIikuc3Bs"
    "aXQoKVsxOl0KICAgIGlmIGFyZ3M6CiAgICAgICAgbmV3ID0gYXJnc1swXS5zdHJpcCgpCiAgICAgICAgaWYgbGVuKG5ldykgPCA0"
    "OgogICAgICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgICAgICBtZXNzYWdlLAogICAgICAgICAgICAgICAg"
    "IlBpY2sgYXQgbGVhc3QgNCBjaGFyYWN0ZXJzOiA8Y29kZT4vYWRtaW5wYXNzIG15c2VjcmV0PC9jb2RlPiIsCiAgICAgICAgICAg"
    "ICkKICAgICAgICAgICAgcmV0dXJuCiAgICAgICAgYXdhaXQgc2V0X2FkbWluX3Bhc3MobmV3KQogICAgICAgIGF3YWl0IHNlbmRf"
    "bWVzc2FnZSgKICAgICAgICAgICAgbWVzc2FnZSwKICAgICAgICAgICAgIuKchSA8Yj5EYXNoYm9hcmQgcGFzc3dvcmQgdXBkYXRl"
    "ZC48L2I+IE9wZW4geW91ciB3b3JrZXIgbGluayArICIKICAgICAgICAgICAgIjxjb2RlPi93emFkbWluPC9jb2RlPiBhbmQgdXNl"
    "IHRoZSBuZXcgcGFzc3dvcmQuIiwKICAgICAgICApCiAgICAgICAgcmV0dXJuCiAgICBwdyA9IGF3YWl0IGdldF9hZG1pbl9wYXNz"
    "KCkKICAgIGlmIG5vdCBwdzoKICAgICAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgICAgIG1lc3NhZ2UsCiAgICAgICAg"
    "ICAgICLimqDvuI8gQ291bGQgbm90IHJlYWQgdGhlIGRhc2hib2FyZCBwYXNzd29yZCDigJQgaXMgTW9uZ29EQiByZWFjaGFibGU/"
    "IiwKICAgICAgICApCiAgICAgICAgcmV0dXJuCiAgICBhd2FpdCBzZW5kX21lc3NhZ2UoCiAgICAgICAgbWVzc2FnZSwKICAgICAg"
    "ICAi8J+UkCA8Yj5XZWIgRGFzaGJvYXJkPC9iPlxu4pSCXG4iCiAgICAgICAgZiLilKAgPGI+UGFzc3dvcmQ8L2I+IOKGkiA8Y29k"
    "ZT57cHd9PC9jb2RlPlxuIgogICAgICAgIGYi4pSgIDxiPlVSTDwvYj4g4oaSIHthd2FpdCBfZGFzaF91cmwoKX1cbiIKICAgICAg"
    "ICAi4pSWIFNlcGFyYXRlIGZyb20gU1RSRUFNX1BBU1MuIENoYW5nZTogL2FkbWlucGFzcyBORVdQQVNTIiwKICAgICkK"
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
    "dF91c2FnZSwKICAgIHJlc2VydmVkX3RvZGF5LAogICAgcmVzZXRfdXNhZ2UsCiAgICBzZXRfZ2xvYmFsX2NhcF9nYiwKICAgIHNl"
    "dF91c2VyX2NhcCwKICAgIHRvZGF5X3Jvd3MsCikKCmRlZiBfZm10X2diKGdiKToKICAgICIiIkNvbXBhY3QgR0IgbGFiZWwsIG5l"
    "dmVyIHNjaWVudGlmaWMgbm90YXRpb24uIiIiCiAgICB0cnk6CiAgICAgICAgZ2IgPSBmbG9hdChnYiBvciAwKQogICAgZXhjZXB0"
    "IChUeXBlRXJyb3IsIFZhbHVlRXJyb3IpOgogICAgICAgIHJldHVybiAiMCBHQiIKICAgIGlmIGdiID49IDEwMjQ6CiAgICAgICAg"
    "cmV0dXJuIGYie2diIC8gMTAyNDouMmZ9IFRCIgogICAgaWYgZ2IgPj0gMTA6CiAgICAgICAgcmV0dXJuIGYie2diOi4wZn0gR0Ii"
    "CiAgICBpZiBnYiA+PSAxOgogICAgICAgIHJldHVybiBmIntnYjouMWZ9IEdCIgogICAgaWYgZ2IgPiAwOgogICAgICAgIHJldHVy"
    "biBmIntnYiAqIDEwMjQ6LjBmfSBNQiIKICAgIHJldHVybiAiMCBHQiIKCgpJU1QgPSB0aW1lem9uZSh0aW1lZGVsdGEoaG91cnM9"
    "NSwgbWludXRlcz0zMCkpClNFU1NJT05fSCA9IDEyICAgICAgICAgICAjIGRhc2hib2FyZCBsb2dpbiBsYXN0cyAxMiBob3VycwpM"
    "T0dJTl9NQVhfRkFJTFMgPSAzICAgICAgIyB3cm9uZyBwYXNzd29yZHMgYmVmb3JlIGEgbG9ja291dCAodjE1LjkpCiMgZXNjYWxh"
    "dGluZyBsb2Nrb3V0czogMXN0IC0+IDI0aCwgMm5kIC0+IDcyaCwgM3JkIC0+IDdkLCA0dGggLT4gMzBkLAojIDV0aCAtPiBwZXJt"
    "YW5lbnQgKHBlciBJUCwgcGVyc2lzdGVkIGluIHRoZSBEQikKTE9DS19USUVSU19TID0gKDg2NDAwLCAyNTkyMDAsIDYwNDgwMCwg"
    "MjU5MjAwMCwgLTEpClJFUE9SVF9NQVhfQ0hBUlMgPSA1MCAqIDEwMjQKSVBfSU5GT19UVEwgPSA3ICogODY0MDAgICMgY2FjaGUg"
    "SVAgaW50ZWxsaWdlbmNlIGZvciBhIHdlZWsKCiMgT25seSByZWFsLCBrbm93biBicm93c2VycyBhcmUgYWxsb3dlZCAoc3Bvb2Zl"
    "ZC91bmtub3duIFVBcyBhcmUgYmxvY2tlZCkKX0tOT1dOX1VBID0gKAogICAgIm1vemlsbGEvNS4wIiwgICMgYmFzZSBmb3IgZmly"
    "ZWZveCArIGdlY2tvIHdlYnZpZXdzCiAgICAiY2hyb21lLyIsCiAgICAiY3Jpb3MvIiwgICAgICAgICMgY2hyb21lIGlvcwogICAg"
    "ImVkZ2EvIiwgImVkZ2lvcy8iLCAiZWRnLyIsCiAgICAiZmlyZWZveC8iLAogICAgImZ4aW9zLyIsICAgICAgICAjIGZpcmVmb3gg"
    "aW9zCiAgICAic2FmYXJpLyIsCiAgICAic2Ftc3VuZ2Jyb3dzZXIvIiwKICAgICJvcGVyYS8iLCAib3B0LyIsCiAgICAib3ByLyIs"
    "CiAgICAidml2b2Jyb3dzZXIvIiwgImhleXRhYnJvd3Nlci8iLCAiaGV5dGFwYnJvd3Nlci8iLAogICAgImh1YXdlaWJyb3dzZXIv"
    "IiwgImhiYnJvd3Nlci8iLAogICAgIm1pYnJvd3Nlci8iLCAibWl1aSIsICAjIHhpYW9taQogICAgInF1YXJrLyIsICJ1Y2Jyb3dz"
    "ZXIvIiwgInVicm93c2VyLyIsCiAgICAieWFicm93c2VyLyIsICJ5YW5kZXgiLAogICAgImR1Y2tkdWNrZ28vIiwKICAgICJicmF2"
    "ZS8iLCAidml2YWxkaS8iLAogICAgImluc3RhZ3JhbSIsICJzbmFwY2hhdCIsICJ3aGF0c2FwcCIsICAjIGluLWFwcCB3ZWJ2aWV3"
    "cwogICAgImZiYW4iLCAiZmJhdiIsICJmYl9pYWIiLCAgIyBmYWNlYm9vawogICAgInRlbGVncmFtIiwgImRpc2NvcmQiLAogICAg"
    "ImthaW9zIiwKICAgICJ3ZWNoYXQiLCAibGluZS8iLAopCl9CQURfVUEgPSAoCiAgICAicHl0aG9uIiwgImN1cmwvIiwgIndnZXQi"
    "LCAib2todHRwIiwgImphdmEvIiwgImdvLWh0dHAiLCAiZ29sYW5nIiwKICAgICJub2RlIiwgInNjcmFweSIsICJib3QvIiwgImNy"
    "YXdsZXIiLCAic3BpZGVyIiwgImh0dHBjbGllbnQiLAogICAgImxpYnd3dyIsICJheGlvcyIsICJwb3N0bWFuIiwgImhlYWRsZXNz"
    "IiwgInBoYW50b20iLCAic2VsZW5pdW0iLAogICAgInB1cHBldGVlciIsICJwbGF5d3JpZ2h0IiwgInJlcXVlc3RzIiwgImFpb2h0"
    "dHAiLCAiaHR0cHgiLAopCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgAojIExvZ2luIGhhcmRlbmluZyAodjE1LjkpOiBVQSBhbGxvd2xpc3QsIHBlcnNpc3RlbnQgZXNjYWxhdGlu"
    "ZyBsb2Nrb3V0cywKIyBzdWJuZXQgYmFucywgZGF0YWNlbnRlci9WUE4gSVAgaW50ZWxsaWdlbmNlIChpcC1hcGkuY29tLCBjYWNo"
    "ZWQpCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "CgoKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAK"
    "IyBBRE1JTl9QQVNTICsgc2Vzc2lvbiB0b2tlbnMKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgZ2V0X2FkbWluX3Bhc3MoKToKICAgICIiIkN1cnJlbnQgQURN"
    "SU5fUEFTUyAoQ29uZmlnLkFETUlOX1BBU1MgZnJvbSAvYnMgd2lucywgdGhlbiBEQikuCgogICAgUHJpb3JpdHk6IC9icy1zZXQg"
    "QURNSU5fUEFTUyAobGl2ZSBjb25maWcsIGluY2x1ZGVzIGNvbmZpZy5lbnYpIOKGkgogICAgREItc3RvcmVkIHZhbHVlIChhdXRv"
    "LWdlbmVyYXRlZCBvciBzZXQgdmlhIC9hZG1pbnBhc3MpLgogICAgIiIiCiAgICAjIC9icy1zZXQgQURNSU5fUEFTUyB3aW5zIChz"
    "YW1lIHBhdHRlcm4gYXMgU1RSRUFNX1BBU1MpCiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi5jb3JlLmNvbmZpZ19tYW5hZ2VyIGlt"
    "cG9ydCBDb25maWcKCiAgICAgICAgX2JzID0gKGdldGF0dHIoQ29uZmlnLCAiQURNSU5fUEFTUyIsICIiKSBvciAiIikuc3RyaXAo"
    "KQogICAgICAgIGlmIF9iczoKICAgICAgICAgICAgcmV0dXJuIF9icwogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNz"
    "CiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCkud3pmaXhfY29uZmlnW19wYXJ0KCldCiAgICAgICAgZG9jID0gYXdhaXQgY29s"
    "LmZpbmRfb25lKHsiX2lkIjogImFkbWluIn0pCiAgICAgICAgaWYgZG9jIGFuZCBkb2MuZ2V0KCJwYXNzIik6CiAgICAgICAgICAg"
    "IHJldHVybiBzdHIoZG9jWyJwYXNzIl0pCiAgICAgICAgZnJvbSBvcyBpbXBvcnQgZ2V0ZW52CgogICAgICAgIHB3ID0gZ2V0ZW52"
    "KCJXWkZJWF9BRE1JTl9QQVNTIiwgIiIpLnN0cmlwKCkgb3IgdG9rZW5fdXJsc2FmZSg2KQogICAgICAgIGF3YWl0IGNvbC51cGRh"
    "dGVfb25lKHsiX2lkIjogImFkbWluIn0sIHsiJHNldCI6IHsicGFzcyI6IHB3fX0sIHVwc2VydD1UcnVlKQogICAgICAgIHJldHVy"
    "biBwdwogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gIiIKCgphc3luYyBkZWYgc2V0X2FkbWluX3Bhc3MobmV3"
    "X3Bhc3MpOgogICAgY29sID0gX2RiKCkud3pmaXhfY29uZmlnW19wYXJ0KCldCiAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAg"
    "ICAgICB7Il9pZCI6ICJhZG1pbiJ9LCB7IiRzZXQiOiB7InBhc3MiOiBzdHIobmV3X3Bhc3MpLnN0cmlwKCl9fSwgdXBzZXJ0PVRy"
    "dWUKICAgICkKCgphc3luYyBkZWYgX2FkbWluX3NlY3JldCgpOgogICAgIiIiUmFuZG9tIHBlci1ib3Qgc2lnbmluZyBzZWNyZXQg"
    "KGNyZWF0ZWQgb25jZSwgc3RvcmVkIGluIHd6Zml4X2NvbmZpZykuIiIiCiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCkud3pm"
    "aXhfY29uZmlnW19wYXJ0KCldCiAgICAgICAgZG9jID0gYXdhaXQgY29sLmZpbmRfb25lKHsiX2lkIjogImFkbWluX3NlY3JldCJ9"
    "KQogICAgICAgIGlmIGRvYyBhbmQgZG9jLmdldCgic2VjcmV0Iik6CiAgICAgICAgICAgIHJldHVybiBzdHIoZG9jWyJzZWNyZXQi"
    "XSkKICAgICAgICBzZWNyZXQgPSB0b2tlbl9oZXgoMzIpCiAgICAgICAgYXdhaXQgY29sLnVwZGF0ZV9vbmUoCiAgICAgICAgICAg"
    "IHsiX2lkIjogImFkbWluX3NlY3JldCJ9LCB7IiRzZXQiOiB7InNlY3JldCI6IHNlY3JldH19LCB1cHNlcnQ9VHJ1ZQogICAgICAg"
    "ICkKICAgICAgICByZXR1cm4gc2VjcmV0CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAid3pmaXgtbm8tc2Vj"
    "cmV0IgoKCmFzeW5jIGRlZiBfc2Vzc2lvbl90b2tlbigpOgogICAgc2VjcmV0ID0gYXdhaXQgX2FkbWluX3NlY3JldCgpCiAgICBl"
    "eHAgPSBpbnQodGltZSgpKSArIFNFU1NJT05fSCAqIDM2MDAKICAgIHNpZyA9IGhtYWNfbmV3KHNlY3JldC5lbmNvZGUoKSwgZiJh"
    "ZG1pbjp7ZXhwfSIuZW5jb2RlKCksIHNoYTI1NikuaGV4ZGlnZXN0KCkKICAgIHJldHVybiBmIntleHB9LntzaWd9IgoKCmFzeW5j"
    "IGRlZiBfY2hlY2tfc2Vzc2lvbihyZXF1ZXN0KToKICAgIHRyeToKICAgICAgICB0b2sgPSByZXF1ZXN0LmNvb2tpZXMuZ2V0KCJ3"
    "emFkbWluIiwgIiIpCiAgICAgICAgZXhwLCBfLCBzaWcgPSB0b2sucGFydGl0aW9uKCIuIikKICAgICAgICBzZWNyZXQgPSBhd2Fp"
    "dCBfYWRtaW5fc2VjcmV0KCkKICAgICAgICBnb29kID0gaG1hY19uZXcoc2VjcmV0LmVuY29kZSgpLCBmImFkbWluOntleHB9Ii5l"
    "bmNvZGUoKSwgc2hhMjU2KS5oZXhkaWdlc3QoKQogICAgICAgIGlmIG5vdCB0b2sgb3Igbm90IGNvbXBhcmVfZGlnZXN0KHNpZywg"
    "Z29vZCk6CiAgICAgICAgICAgIHJldHVybiBGYWxzZQogICAgICAgIHJldHVybiBpbnQoZXhwKSA+IHRpbWUoKQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gRmFsc2UKCgpkZWYgX2NsaWVudF9pcChyZXF1ZXN0KToKICAgIGZ3ZCA9IHJlcXVl"
    "c3QuaGVhZGVycy5nZXQoIlgtRm9yd2FyZGVkLUZvciIsICIiKQogICAgaWYgZndkOgogICAgICAgIHJldHVybiBmd2Quc3BsaXQo"
    "IiwiKVswXS5zdHJpcCgpWzo2NF0KICAgIHRyeToKICAgICAgICByZXR1cm4gKHJlcXVlc3QucmVtb3RlIG9yICI/IilbOjY0XQog"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gIj8iCgoKZGVmIF91YV9vayhyZXF1ZXN0KToKICAgIHVhID0gKHJl"
    "cXVlc3QuaGVhZGVycy5nZXQoIlVzZXItQWdlbnQiKSBvciAiIikuc3RyaXAoKS5sb3dlcigpCiAgICBpZiBub3QgdWEgb3IgbGVu"
    "KHVhKSA8IDEwOgogICAgICAgIHJldHVybiBGYWxzZQogICAgaWYgYW55KGIgaW4gdWEgZm9yIGIgaW4gX0JBRF9VQSk6CiAgICAg"
    "ICAgcmV0dXJuIEZhbHNlCiAgICByZXR1cm4gYW55KGsgaW4gdWEgZm9yIGsgaW4gX0tOT1dOX1VBKQoKCmRlZiBfc3VibmV0KGlw"
    "KToKICAgICIiIklQdjQgLzI0IG9yIElQdjYgLzY0IHByZWZpeCBmb3Igc3VibmV0LWxldmVsIGJhbnMuIiIiCiAgICB0cnk6CiAg"
    "ICAgICAgaWYgIjoiIGluIGlwOgogICAgICAgICAgICByZXR1cm4gIi8iLmpvaW4oaXAuc3BsaXQoIjoiKVs6NF0pICsgIjo6LzY0"
    "IgogICAgICAgIHBhcnRzID0gaXAuc3BsaXQoIi4iKQogICAgICAgIGlmIGxlbihwYXJ0cykgPT0gNDoKICAgICAgICAgICAgcmV0"
    "dXJuICIuIi5qb2luKHBhcnRzWzozXSkgKyAiLjAvMjQiCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAgIHJl"
    "dHVybiBpcAoKCmRlZiBfbm9ybV9pcChpcCk6CiAgICBpcCA9IChpcCBvciAiIikuc3RyaXAoKQogICAgaWYgaXAuc3RhcnRzd2l0"
    "aCgiOjpmZmZmOiIpOgogICAgICAgIGlwID0gaXBbNzpdCiAgICByZXR1cm4gaXBbOjQ1XQoKCmFzeW5jIGRlZiBfaXNfYmFubmVk"
    "KGlwKToKICAgICIiIklQIG9yIHN1Ym5ldCBiYW4gY2hlY2sgKHBlcnNpc3RlbnQsIERCLWJhY2tlZCkuIiIiCiAgICB0cnk6CiAg"
    "ICAgICAgY29sID0gX2RiKCkud3pmaXhfYmFuc1tfcGFydCgpXQogICAgICAgIGlmIGF3YWl0IGNvbC5maW5kX29uZSh7Il9pZCI6"
    "IF9ub3JtX2lwKGlwKX0pOgogICAgICAgICAgICByZXR1cm4gVHJ1ZQogICAgICAgIGlmIGF3YWl0IGNvbC5maW5kX29uZSh7Il9p"
    "ZCI6ICJzdWI6IiArIF9zdWJuZXQoX25vcm1faXAoaXApKX0pOgogICAgICAgICAgICByZXR1cm4gVHJ1ZQogICAgZXhjZXB0IEV4"
    "Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gRmFsc2UKCgphc3luYyBkZWYgX2Jhbl9pcChpcCwgc3VibmV0X3Rvbz1U"
    "cnVlKToKICAgIHRyeToKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9iYW5zW19wYXJ0KCldCiAgICAgICAgYXdhaXQgY29sLnVw"
    "ZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAgeyIkc2V0IjogeyJwZXJtIjog"
    "VHJ1ZSwgInRzIjogdGltZSgpfX0sCiAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICkKICAgICAgICBpZiBzdWJuZXRf"
    "dG9vOgogICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogInN1YjoiICsgX3N1"
    "Ym5ldChfbm9ybV9pcChpcCkpfSwKICAgICAgICAgICAgICAgIHsiJHNldCI6IHsicGVybSI6IFRydWUsICJ0cyI6IHRpbWUoKX19"
    "LAogICAgICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAg"
    "cGFzcwoKCmFzeW5jIGRlZiBfaXBfaW50ZWwoaXApOgogICAgIiIiRGF0YWNlbnRlci9WUE4gZGV0ZWN0aW9uIHZpYSBpcC1hcGku"
    "Y29tIChmcmVlIHRpZXIsIGNhY2hlZCA3IGRheXMpLgogICAgUmV0dXJucyBUcnVlIHdoZW4gdGhlIElQIGxvb2tzIGxpa2UgYSBz"
    "ZXJ2ZXIvcHJveHkgKC0+IGJhbm5lZCkuIiIiCiAgICBpcCA9IF9ub3JtX2lwKGlwKQogICAgaWYgbm90IGlwIG9yIGlwIGluICgi"
    "MTI3LjAuMC4xIiwgIjo6MSIsICI/IiwgImxvY2FsaG9zdCIpOgogICAgICAgIHJldHVybiBGYWxzZQogICAgaWYgbm90IGlwLnJl"
    "cGxhY2UoIi4iLCAiIikuaXNkaWdpdCgpOiAgIyBpcHY2IG9yIHVua25vd246IHNraXAgbG9va3VwCiAgICAgICAgcmV0dXJuIEZh"
    "bHNlCiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCkud3pmaXhfaXBpbmZvW19wYXJ0KCldCiAgICAgICAgY2FjaGVkID0gYXdh"
    "aXQgY29sLmZpbmRfb25lKHsiX2lkIjogaXB9KQogICAgICAgIGlmIGNhY2hlZCBhbmQgdGltZSgpIC0gY2FjaGVkLmdldCgidHMi"
    "LCAwKSA8IElQX0lORk9fVFRMOgogICAgICAgICAgICByZXR1cm4gYm9vbChjYWNoZWQuZ2V0KCJiYWQiKSkKICAgICAgICBpbXBv"
    "cnQganNvbiBhcyBfanNvbgogICAgICAgIGZyb20gdXJsbGliLnJlcXVlc3QgaW1wb3J0IHVybG9wZW4sIFJlcXVlc3QKCiAgICAg"
    "ICAgdXJsID0gKAogICAgICAgICAgICAiaHR0cDovL2lwLWFwaS5jb20vanNvbi8iICsgaXAKICAgICAgICAgICAgKyAiP2ZpZWxk"
    "cz1zdGF0dXMscHJveHksaG9zdGluZyxtb2JpbGUsaXNwIgogICAgICAgICkKICAgICAgICByZXEgPSBSZXF1ZXN0KHVybCwgaGVh"
    "ZGVycz17IlVzZXItQWdlbnQiOiAid3pmaXgtZGFzaGJvYXJkIn0pCiAgICAgICAgd2l0aCB1cmxvcGVuKHJlcSwgdGltZW91dD04"
    "KSBhcyByOgogICAgICAgICAgICBkYXRhID0gX2pzb24ubG9hZHMoci5yZWFkKCkuZGVjb2RlKCJ1dGYtOCIsICJyZXBsYWNlIikp"
    "CiAgICAgICAgYmFkID0gZGF0YS5nZXQoInN0YXR1cyIpID09ICJzdWNjZXNzIiBhbmQgKAogICAgICAgICAgICBkYXRhLmdldCgi"
    "aG9zdGluZyIpIG9yIGRhdGEuZ2V0KCJwcm94eSIpCiAgICAgICAgKQogICAgICAgIHRyeToKICAgICAgICAgICAgYXdhaXQgY29s"
    "LnVwZGF0ZV9vbmUoCiAgICAgICAgICAgICAgICB7Il9pZCI6IGlwfSwKICAgICAgICAgICAgICAgIHsiJHNldCI6IHsiYmFkIjog"
    "Ym9vbChiYWQpLCAiaXNwIjogZGF0YS5nZXQoImlzcCIsICIiKSwKICAgICAgICAgICAgICAgICAgICAgICAgICAidHMiOiB0aW1l"
    "KCl9fSwKICAgICAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICAgICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoK"
    "ICAgICAgICAgICAgcGFzcwogICAgICAgIGlmIGJhZDoKICAgICAgICAgICAgYXdhaXQgX2Jhbl9pcChpcCkKICAgICAgICAgICAg"
    "dHJ5OgogICAgICAgICAgICAgICAgZnJvbSAuLi4gaW1wb3J0IExPR0dFUgoKICAgICAgICAgICAgICAgIExPR0dFUi5lcnJvcigK"
    "ICAgICAgICAgICAgICAgICAgICBmIldaRklYIGRhc2hib2FyZDogZGF0YWNlbnRlci9wcm94eSBJUCBiYW5uZWQ6IHtpcH0gIgog"
    "ICAgICAgICAgICAgICAgICAgIGYiKHtkYXRhLmdldCgnaXNwJywgJz8nKX0pIgogICAgICAgICAgICAgICAgKQogICAgICAgICAg"
    "ICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAgICAgcGFzcwogICAgICAgIHJldHVybiBib29sKGJhZCkKICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIEZhbHNlICAjIGludGVsIHVuYXZhaWxhYmxlIC0+IGRvIG5vdCBibG9jawoKCmFz"
    "eW5jIGRlZiBfbG9ja291dF9zdGF0ZShpcCk6CiAgICAiIiJQZXJzaXN0ZW50IHRpZXItYmFzZWQgbG9ja291dC4gUmV0dXJucyBz"
    "ZWNvbmRzIHJlbWFpbmluZyAoMCA9IG9wZW4pLiIiIgogICAgdHJ5OgogICAgICAgIGNvbCA9IF9kYigpLnd6Zml4X2xvY2tzW19w"
    "YXJ0KCldCiAgICAgICAgZG9jID0gYXdhaXQgY29sLmZpbmRfb25lKHsiX2lkIjogX25vcm1faXAoaXApfSkKICAgICAgICBpZiBu"
    "b3QgZG9jOgogICAgICAgICAgICByZXR1cm4gMAogICAgICAgIGlmIGRvYy5nZXQoInBlcm0iKToKICAgICAgICAgICAgcmV0dXJu"
    "IC0xCiAgICAgICAgdW50aWwgPSBkb2MuZ2V0KCJ1bnRpbCIpIG9yIDAKICAgICAgICByZXR1cm4gbWF4KHVudGlsIC0gdGltZSgp"
    "LCAwKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gMAoKCmFzeW5jIGRlZiBfcmVjb3JkX2ZhaWwoaXApOgog"
    "ICAgIiIiMyBmYWlscyAtPiBlc2NhbGF0aW5nIGxvY2tvdXQ6IDI0aCwgNzJoLCA3ZCwgMzBkLCBwZXJtYW5lbnQuIiIiCiAgICB0"
    "cnk6CiAgICAgICAgZnJvbSBweW1vbmdvIGltcG9ydCBSZXR1cm5Eb2N1bWVudCBhcyBfUkQKCiAgICAgICAgY29sID0gX2RiKCku"
    "d3pmaXhfbG9ja3NbX3BhcnQoKV0KICAgICAgICBkb2MgPSBhd2FpdCBjb2wuZmluZF9vbmVfYW5kX3VwZGF0ZSgKICAgICAgICAg"
    "ICAgeyJfaWQiOiBfbm9ybV9pcChpcCl9LAogICAgICAgICAgICB7IiRpbmMiOiB7ImZhaWxzIjogMX0sICIkc2V0IjogeyJ0cyI6"
    "IHRpbWUoKX19LAogICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwKICAgICAgICAgICAgcmV0dXJuX2RvY3VtZW50PV9SRC5BRlRFUiwK"
    "ICAgICAgICApCiAgICAgICAgZmFpbHMgPSBpbnQoKGRvYyBvciB7fSkuZ2V0KCJmYWlscyIpIG9yIDApCiAgICAgICAgaWYgZmFp"
    "bHMgPCBMT0dJTl9NQVhfRkFJTFM6CiAgICAgICAgICAgIHJldHVybiAwLCBmYWlscwogICAgICAgIHRpZXIgPSAoaW50KChkb2Mg"
    "b3Ige30pLmdldCgidGllciIpIG9yIDApKSArIDEKICAgICAgICBzZWNzID0gTE9DS19USUVSU19TW21pbih0aWVyIC0gMSwgbGVu"
    "KExPQ0tfVElFUlNfUykgLSAxKV0KICAgICAgICBpZiBzZWNzIDwgMDoKICAgICAgICAgICAgYXdhaXQgY29sLnVwZGF0ZV9vbmUo"
    "CiAgICAgICAgICAgICAgICB7Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgICAgICB7IiRzZXQiOiB7InBlcm0iOiBU"
    "cnVlLCAiZmFpbHMiOiAwLCAidGllciI6IHRpZXJ9fSwKICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4gLTEsIHRpZXIK"
    "ICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgeyJfaWQiOiBfbm9ybV9pcChpcCl9LAogICAgICAgICAg"
    "ICB7IiRzZXQiOiB7InVudGlsIjogdGltZSgpICsgc2VjcywgImZhaWxzIjogMCwgInRpZXIiOiB0aWVyfX0sCiAgICAgICAgKQog"
    "ICAgICAgIHJldHVybiBzZWNzLCB0aWVyCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAwLCAwCgoKZGVmIF9y"
    "ZWNvcmRfb2soaXApOgogICAgdHJ5OgogICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgZ2V0X2V2ZW50X2xvb3AKCiAgICAgICAg"
    "Y29sID0gX2RiKCkud3pmaXhfbG9ja3NbX3BhcnQoKV0KICAgICAgICBnZXRfZXZlbnRfbG9vcCgpLmNyZWF0ZV90YXNrKAogICAg"
    "ICAgICAgICBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAg"
    "ICAgIHsiJHNldCI6IHsiZmFpbHMiOiAwLCAidGllciI6IDAsICJ1bnRpbCI6IDB9fSwKICAgICAgICAgICAgKQogICAgICAgICkK"
    "ICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwoKCmRlZiBfbG9ja190eHQoc2Vjcyk6CiAgICBpZiBzZWNzIDwgMDoK"
    "ICAgICAgICByZXR1cm4gInBlcm1hbmVudGx5IGJhbm5lZCIKICAgIGlmIHNlY3MgPj0gODY0MDA6CiAgICAgICAgcmV0dXJuIGYi"
    "bG9ja2VkIGZvciB7c2VjcyAvLyA4NjQwMH0gZGF5KHMpIgogICAgcmV0dXJuIGYibG9ja2VkIGZvciB7c2VjcyAvLyAzNjAwICsg"
    "MX0gaG91cihzKSIKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgAojIFN0YXRlIGJ1aWxkaW5nICh1c2VycywgbGl2ZSB0YXNrcywgZ2xvYmFscykKIyDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgpkZWYgX3Rhc2tzX3NuYXBzaG90"
    "KCk6CiAgICBvdXQgPSBbXQogICAgdHJ5OgogICAgICAgIGZyb20gLi4uIGltcG9ydCB0YXNrX2RpY3QKCiAgICAgICAgZGVmIHNh"
    "ZmUoZm4sIGRlZmF1bHQ9IiIpOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICB2ID0gZm4oKQogICAgICAgICAgICAg"
    "ICAgcmV0dXJuIHYgaWYgaXNpbnN0YW5jZSh2LCAoaW50LCBmbG9hdCwgc3RyKSkgZWxzZSBkZWZhdWx0CiAgICAgICAgICAgIGV4"
    "Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICByZXR1cm4gZGVmYXVsdAoKICAgICAgICBmb3IgbWlkLCB0IGluIGxpc3Qo"
    "dGFza19kaWN0Lml0ZW1zKCkpOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICBsc3QgPSBnZXRhdHRyKHQsICJsaXN0"
    "ZW5lciIsIE5vbmUpCiAgICAgICAgICAgICAgICBvdXQuYXBwZW5kKAogICAgICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgIm1pZCI6IG1pZCwKICAgICAgICAgICAgICAgICAgICAgICAgImdpZCI6IHNhZmUobGFtYmRhOiB0LmdpZCgp"
    "LCAiIiksCiAgICAgICAgICAgICAgICAgICAgICAgICJuYW1lIjogc3RyKHNhZmUobGFtYmRhOiB0Lm5hbWUoKSwgInRhc2siKSlb"
    "OjkwXSwKICAgICAgICAgICAgICAgICAgICAgICAgInVpZCI6IGludChzYWZlKGxhbWJkYTogbHN0LnVzZXJfaWQsIDApIG9yIDAp"
    "LAogICAgICAgICAgICAgICAgICAgICAgICAidGFnIjogc3RyKHNhZmUobGFtYmRhOiBsc3QudGFnLCAiIikgb3IgIiIpLAogICAg"
    "ICAgICAgICAgICAgICAgICAgICAic2l6ZSI6IGludChzYWZlKGxhbWJkYTogdC5zaXplKCksIDApIG9yIDApLAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAicHJvYyI6IGludChzYWZlKGxhbWJkYTogdC5wcm9jZXNzZWRfYnl0ZXMoKSwgMCkgb3IgMCksCiAgICAg"
    "ICAgICAgICAgICAgICAgICAgICJwY3QiOiBzYWZlKGxhbWJkYTogZmxvYXQoc3RyKHQucHJvZ3Jlc3MoKSkucnN0cmlwKCIlIikp"
    "LCAwLjApLAogICAgICAgICAgICAgICAgICAgICAgICAic3BlZWQiOiBzdHIoc2FmZShsYW1iZGE6IHQuc3BlZWQoKSwgIiIpKSwK"
    "ICAgICAgICAgICAgICAgICAgICAgICAgImV0YSI6IHN0cihzYWZlKGxhbWJkYTogdC5ldGEoKSwgIiIpKSwKICAgICAgICAgICAg"
    "ICAgICAgICB9CiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBj"
    "b250aW51ZQogICAgICAgIG91dC5zb3J0KGtleT1sYW1iZGEgeDogLXhbInNpemUiXSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAg"
    "ICAgICAgcGFzcwogICAgcmV0dXJuIG91dAoKCmFzeW5jIGRlZiBfYm90X2luZm8oKToKICAgIG91dCA9IHsibmFtZSI6ICIiLCAi"
    "dW5hbWUiOiAiIn0KICAgIHRyeToKICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVudAoKICAgICAg"
    "ICBtZSA9IGF3YWl0IFRnQ2xpZW50LmJvdC5nZXRfbWUoKQogICAgICAgIG91dFsibmFtZSJdID0gbWUuZmlyc3RfbmFtZSBvciAi"
    "IgogICAgICAgIG91dFsidW5hbWUiXSA9IG1lLnVzZXJuYW1lIG9yICIiCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBh"
    "c3MKICAgIHJldHVybiBvdXQKCgphc3luYyBkZWYgX3N0YXRlKCk6CiAgICByb3dzID0gYXdhaXQgdG9kYXlfcm93cygpCiAgICB1"
    "c2VkX2J5ID0ge3JbInVzZXJfaWQiXTogaW50KHIuZ2V0KCJ1c2VkIikgb3IgMCkgZm9yIHIgaW4gcm93c30KICAgIGRvY3MgPSBh"
    "d2FpdCBhbGxfdXNlcnMoKQogICAgdXNlcnMgPSBbXQogICAgZm9yIGQgaW4gZG9jczoKICAgICAgICB0cnk6CiAgICAgICAgICAg"
    "IHVpZCA9IGludChkWyJfaWQiXSkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBjb250aW51ZQogICAgICAg"
    "IGNhcCA9IGF3YWl0IGdldF9jYXBfYnl0ZXModWlkKQogICAgICAgIHVzZWQgPSB1c2VkX2J5LmdldCh1aWQsIDApCiAgICAgICAg"
    "cmVzID0gYXdhaXQgcmVzZXJ2ZWRfdG9kYXkodWlkKQogICAgICAgIHVzZXJzLmFwcGVuZCgKICAgICAgICAgICAgewogICAgICAg"
    "ICAgICAgICAgInVpZCI6IHVpZCwKICAgICAgICAgICAgICAgICJ1bmFtZSI6IGQuZ2V0KCJ1bmFtZSIpIG9yICIiLAogICAgICAg"
    "ICAgICAgICAgIm5hbWUiOiBkLmdldCgibmFtZSIpIG9yICIiLAogICAgICAgICAgICAgICAgImNhcF9nYiI6IGQuZ2V0KCJjYXBf"
    "Z2IiKSwKICAgICAgICAgICAgICAgICJjYXAiOiBjYXAsCiAgICAgICAgICAgICAgICAidXNlZCI6IHVzZWQsCiAgICAgICAgICAg"
    "ICAgICAicmVzZXJ2ZWQiOiByZXMsCiAgICAgICAgICAgICAgICAicGN0IjogbWluKDEwMC4wLCByb3VuZCgxMDAuMCAqICh1c2Vk"
    "ICsgcmVzKSAvIGNhcCwgMSkpIGlmIGNhcCBlbHNlIDAuMCwKICAgICAgICAgICAgICAgICJiYW5uZWQiOiBkLmdldCgiY2FwX2di"
    "IikgPT0gMCwKICAgICAgICAgICAgICAgICJ0YXNrcyI6IGludChkLmdldCgidGFza3MiKSBvciAwKSwKICAgICAgICAgICAgICAg"
    "ICJ0b3RhbCI6IGludChkLmdldCgidG90YWxfdXNlZCIpIG9yIDApLAogICAgICAgICAgICB9CiAgICAgICAgKQogICAgdXNlcnMu"
    "c29ydChrZXk9bGFtYmRhIHU6IC0odVsidXNlZCJdICsgdVsicmVzZXJ2ZWQiXSkpCiAgICByZXR1cm4gewogICAgICAgICJvayI6"
    "IFRydWUsCiAgICAgICAgImRheSI6IF9kYXlfaXN0KCksCiAgICAgICAgImJvdCI6IGF3YWl0IF9ib3RfaW5mbygpLAogICAgICAg"
    "ICJ1c2VycyI6IHVzZXJzLAogICAgICAgICJ0YXNrcyI6IF90YXNrc19zbmFwc2hvdCgpLAogICAgICAgICJnbG9iYWxfY2FwX2di"
    "IjogYXdhaXQgX2dldF9nbG9iYWxfY2FwX2diKCksCiAgICAgICAgInRvdGFscyI6IHsKICAgICAgICAgICAgInVzZWQiOiBzdW0o"
    "dVsidXNlZCJdIGZvciB1IGluIHVzZXJzKSwKICAgICAgICAgICAgInJlc2VydmVkIjogc3VtKHVbInJlc2VydmVkIl0gZm9yIHUg"
    "aW4gdXNlcnMpLAogICAgICAgICAgICAidXNlcnMiOiBsZW4odXNlcnMpLAogICAgICAgICAgICAidGFza3MiOiBsZW4oX3Rhc2tz"
    "X3NuYXBzaG90KCkpLAogICAgICAgIH0sCiAgICB9CgoKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKIyBBY3Rpb25zCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVmIF9raWxsX3Rhc2sobWlkKToKICAgIGZyb20g"
    "Li4uIGltcG9ydCB0YXNrX2RpY3QKCiAgICB0cnk6CiAgICAgICAgdCA9IHRhc2tfZGljdC5nZXQoaW50KG1pZCkpCiAgICAgICAg"
    "aWYgdCBpcyBOb25lOgogICAgICAgICAgICByZXR1cm4gRmFsc2UsICJ0YXNrIG5vdCBmb3VuZCIKICAgICAgICBvYmogPSB0LnRh"
    "c2soKQogICAgICAgIGF3YWl0IG9iai5jYW5jZWxfdGFzaygpCiAgICAgICAgcmV0dXJuIFRydWUsICJjYW5jZWxsZWQiCiAgICBl"
    "eGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgcmV0dXJuIEZhbHNlLCBzdHIoZSlbOjEyMF0KCgphc3luYyBkZWYgX2RlZHVj"
    "dF9jYXAodWlkLCBnYik6CiAgICAiIiJMb3dlciBhIHVzZXIncyBjYXAgYnkgR0IgKGZsb29yIDAgPSBmdWxseSBibG9ja2VkKSDi"
    "gJQgc2FtZSBhcwogICAgVGVsZWdyYW0gL2RlZHVjdGNhcC4iIiIKICAgIHRyeToKICAgICAgICBhbXQgPSBmbG9hdChnYikKICAg"
    "ICAgICB1ZG9jID0gYXdhaXQgZ2V0X3VzZXJfZG9jKHVpZCkKICAgICAgICBjdXIgPSB1ZG9jLmdldCgiY2FwX2diIikKICAgICAg"
    "ICBiYXNlID0gZmxvYXQoY3VyKSBpZiBjdXIgaXMgbm90IE5vbmUgZWxzZSBhd2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKQogICAg"
    "ICAgIG5ld19jYXAgPSBtYXgoYmFzZSAtIGFtdCwgMC4wKQogICAgICAgIGF3YWl0IHNldF91c2VyX2NhcCh1aWQsIG5ld19jYXAp"
    "CiAgICAgICAgaWYgbmV3X2NhcCA8PSAwOgogICAgICAgICAgICByZXR1cm4gVHJ1ZSwgZiJjYXAgZm9yIHt1aWR9IOKGkiAwIEdC"
    "IChibG9ja2VkKSIKICAgICAgICByZXR1cm4gVHJ1ZSwgZiJjYXAgZm9yIHt1aWR9IOKGkiB7X2ZtdF9nYihuZXdfY2FwKX0iCiAg"
    "ICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgcmV0dXJuIEZhbHNlLCBzdHIoZSlbOjEyMF0KCgphc3luYyBkZWYgYnVp"
    "bGRfcmVwb3J0KCk6CiAgICAiIiJEYWlseSB1c2FnZSB0ZXh0IGZvciB0aGUgbG9nIGNoYXQuIiIiCiAgICByb3dzID0gYXdhaXQg"
    "dG9kYXlfcm93cygpCiAgICBkb2NzID0ge2ludChkWyJfaWQiXSk6IGQgZm9yIGQgaW4gYXdhaXQgYWxsX3VzZXJzKCl9CiAgICBs"
    "aW5lcyA9IFtmIvCfk4ogPGI+V1pGSVggZGFpbHkgcmVwb3J0IOKAlCB7X2RheV9pc3QoKX08L2I+IiwgIuKUgiJdCiAgICB0b3Rh"
    "bCA9IDAKICAgIGZvciByIGluIHJvd3NbOjI1XToKICAgICAgICB1aWQgPSByWyJ1c2VyX2lkIl0KICAgICAgICBkID0gZG9jcy5n"
    "ZXQodWlkLCB7fSkKICAgICAgICB0YWcgPSBkLmdldCgidW5hbWUiKSBvciBkLmdldCgibmFtZSIpIG9yIHN0cih1aWQpCiAgICAg"
    "ICAgY2FwX2diID0gZC5nZXQoImNhcF9nYiIpCiAgICAgICAgY2FwX3R4dCA9IGYie19mbXRfZ2IoY2FwX2diKX0iIGlmIGNhcF9n"
    "YiBlbHNlICJkZWZhdWx0IgogICAgICAgIGxpbmVzLmFwcGVuZChmIuKUoCA8Yj57dGFnfTwvYj4g4oaSIHtfbmljZV9zaXplKHJb"
    "J3VzZWQnXSl9IC8ge2NhcF90eHR9IikKICAgICAgICB0b3RhbCArPSByLmdldCgidXNlZCIpIG9yIDAKICAgIGlmIG5vdCByb3dz"
    "OgogICAgICAgIGxpbmVzLmFwcGVuZCgi4pSWIG5vIHVzYWdlIHRvZGF5IikKICAgIGVsc2U6CiAgICAgICAgbGluZXNbMV0gPSBm"
    "IuKUgiB0b3RhbCA8Yj57X25pY2Vfc2l6ZSh0b3RhbCl9PC9iPiDCtyB7bGVuKHJvd3MpfSB1c2VyKHMpIgogICAgICAgIGxpbmVz"
    "LmFwcGVuZCgi4pSWIHJlc2V0cyBhdCAwMDowMCBJU1QiKQogICAgcmV0dXJuICJcbiIuam9pbihsaW5lcylbOlJFUE9SVF9NQVhf"
    "Q0hBUlNdCgoKYXN5bmMgZGVmIHNlbmRfcmVwb3J0KCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi5jb3JlLmNvbmZpZ19tYW5h"
    "Z2VyIGltcG9ydCBDb25maWcKICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVudAoKICAgICAgICBj"
    "aGF0ID0gc3RyKGdldGF0dHIoQ29uZmlnLCAiTE9HX0NIQVQiLCAiIikgb3IgIiIpLnN0cmlwKCkKICAgICAgICBpZiBub3QgY2hh"
    "dDoKICAgICAgICAgICAgcmV0dXJuIEZhbHNlLCAiTE9HX0NIQVQgbm90IHNldCIKICAgICAgICB0ZXh0ID0gYXdhaXQgYnVpbGRf"
    "cmVwb3J0KCkKICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Quc2VuZF9tZXNzYWdlKAogICAgICAgICAgICBjaGF0X2lkPWNoYXQs"
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
    "ZCBwYXNzd29yZCIsCn0KCgphc3luYyBkZWYgc2V0X2JvdF9jb21tYW5kcygpOgogICAgIiIiUHVibGlzaCB0aGUgZnVsbCBjb21t"
    "YW5kIGxpc3QgdG8gdGhlIFRlbGVncmFtICIvIiBtZW51IOKAlCBkZWZhdWx0CiAgICBzY29wZSBmb3IgdXNlcnMsIG93bmVyIHNj"
    "b3BlIHdpdGggZXZlcnkgYWRtaW4gY29tbWFuZC4iIiIKICAgIHRyeToKICAgICAgICBmcm9tIGFzeW5jaW8gaW1wb3J0IHNsZWVw"
    "CgogICAgICAgIGF3YWl0IHNsZWVwKDkwKSAgIyB3YWl0IGZvciB0aGUgYm90IHRvIGJlIHVwCiAgICAgICAgZnJvbSBweXJvZ3Jh"
    "bS50eXBlcyBpbXBvcnQgQm90Q29tbWFuZCwgQm90Q29tbWFuZFNjb3BlQ2hhdCwgQm90Q29tbWFuZFNjb3BlRGVmYXVsdAoKICAg"
    "ICAgICBmcm9tIC4uLiBpbXBvcnQgTE9HR0VSCiAgICAgICAgZnJvbSAuLi5jb3JlLnRnX2NsaWVudCBpbXBvcnQgVGdDbGllbnQs"
    "IENvbmZpZwoKICAgICAgICAjIGdhdGhlciBldmVyeSBjb21tYW5kIG5hbWUga25vd24gdG8gdGhlIGJvdAogICAgICAgIHRyeToK"
    "ICAgICAgICAgICAgZnJvbSAuLnRlbGVncmFtX2hlbHBlci5ib3RfY29tbWFuZHMgaW1wb3J0IEJvdENvbW1hbmRzCgogICAgICAg"
    "ICAgICBhbGxfbmFtZXMgPSBzZXQoKQogICAgICAgICAgICBmb3IgX3YgaW4gQm90Q29tbWFuZHMuZ2V0X2NvbW1hbmRzKCkudmFs"
    "dWVzKCk6CiAgICAgICAgICAgICAgICBuYW1lcyA9IF92IGlmIGlzaW5zdGFuY2UoX3YsIGxpc3QpIGVsc2UgW192XQogICAgICAg"
    "ICAgICAgICAgYWxsX25hbWVzLnVwZGF0ZShuYW1lcykKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBhbGxf"
    "bmFtZXMgPSBzZXQoKQogICAgICAgIGFsbF9uYW1lcy51cGRhdGUoX09XTkVSX0RFU0MpCiAgICAgICAgYWxsX25hbWVzLnVwZGF0"
    "ZShfQ01EX0RFU0MpCgogICAgICAgIHVzZXJfY21kcywgb3duZXJfY21kcyA9IFtdLCBbXQogICAgICAgIGZvciBuYW1lIGluIHNv"
    "cnRlZChhbGxfbmFtZXMpOgogICAgICAgICAgICBpZiBuYW1lIGluICgic2hlbGwiLCAiYWV4ZWMiLCAiZXhlYyIsICJjbGVhcmxv"
    "Y2FscyIpOgogICAgICAgICAgICAgICAgY29udGludWUgICMga2VlcCB0aGUgZGFuZ2Vyb3VzIG9uZXMgb3V0IG9mIHRoZSBtZW51"
    "CiAgICAgICAgICAgIGRlc2MgPSBfQ01EX0RFU0MuZ2V0KG5hbWUpIG9yIF9PV05FUl9ERVNDLmdldChuYW1lKSBvciAiV1pNTC1Y"
    "IGNvbW1hbmQiCiAgICAgICAgICAgIG93bmVyX2NtZHMuYXBwZW5kKEJvdENvbW1hbmQoY29tbWFuZD1uYW1lLCBkZXNjcmlwdGlv"
    "bj1kZXNjKSkKICAgICAgICAgICAgaWYgbmFtZSBub3QgaW4gX09XTkVSX0RFU0MgYW5kIG5vdCBuYW1lLnN0YXJ0c3dpdGgoInd6"
    "Iik6CiAgICAgICAgICAgICAgICB1c2VyX2NtZHMuYXBwZW5kKEJvdENvbW1hbmQoY29tbWFuZD1uYW1lLCBkZXNjcmlwdGlvbj1k"
    "ZXNjKSkKCiAgICAgICAgYXdhaXQgVGdDbGllbnQuYm90LnNldF9teV9jb21tYW5kcygKICAgICAgICAgICAgdXNlcl9jbWRzIG9y"
    "IFtCb3RDb21tYW5kKCJzdGFydCIsICJTdGFydCB0aGUgYm90IildCiAgICAgICAgKQogICAgICAgIGF3YWl0IFRnQ2xpZW50LmJv"
    "dC5zZXRfbXlfY29tbWFuZHMoCiAgICAgICAgICAgIG93bmVyX2NtZHMsCiAgICAgICAgICAgIHNjb3BlPUJvdENvbW1hbmRTY29w"
    "ZUNoYXQoY2hhdF9pZD1Db25maWcuT1dORVJfSUQpLAogICAgICAgICkKICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAg"
    "ZiJXWkZJWDogY29tbWFuZCBtZW51IHNldCAoe2xlbih1c2VyX2NtZHMpfSB1c2VyLCAiCiAgICAgICAgICAgIGYie2xlbihvd25l"
    "cl9jbWRzKX0gb3duZXIgY29tbWFuZHMpIgogICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICB0cnk6"
    "CiAgICAgICAgICAgIGZyb20gLi4uIGltcG9ydCBMT0dHRVIKCiAgICAgICAgICAgIExPR0dFUi53YXJuaW5nKGYiV1pGSVggc2V0"
    "X2JvdF9jb21tYW5kcyBmYWlsZWQ6IHtlfSIpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwoKCmRl"
    "ZiBzdGFydF9sb29wcygpOgogICAgdHJ5OgogICAgICAgIGZyb20gLi4uIGltcG9ydCBib3RfbG9vcAoKICAgICAgICBib3RfbG9v"
    "cC5jcmVhdGVfdGFzayhkYWlseV9yZXBvcnRfbG9vcCgpKQogICAgICAgIGJvdF9sb29wLmNyZWF0ZV90YXNrKHNldF9ib3RfY29t"
    "bWFuZHMoKSkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwoKCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACiMgSFRUUCBoYW5kbGVycyAocmVnaXN0ZXJlZCBvbiB0"
    "aGUgc3RyZWFtIHNlcnZlcidzIGFpb2h0dHAgYXBwKQojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiB3emFkbWluX3BhZ2UocmVxdWVzdCk6CiAgICBpcCA9IF9u"
    "b3JtX2lwKF9jbGllbnRfaXAocmVxdWVzdCkpCiAgICBpZiBub3QgX3VhX29rKHJlcXVlc3QpIG9yIGF3YWl0IF9pc19iYW5uZWQo"
    "aXApOgogICAgICAgIHJldHVybiB3ZWIuUmVzcG9uc2Uoc3RhdHVzPTQwMywgdGV4dD0iZm9yYmlkZGVuIikKICAgIGlmIG5vdCBh"
    "d2FpdCBlbnN1cmVfcmVhZHkoKToKICAgICAgICByZXR1cm4gd2ViLlJlc3BvbnNlKAogICAgICAgICAgICB0ZXh0PSI8aDM+RGFz"
    "aGJvYXJkIHVuYXZhaWxhYmxlOiBkYXRhYmFzZSBub3QgcmVhY2hhYmxlLjwvaDM+IiwKICAgICAgICAgICAgY29udGVudF90eXBl"
    "PSJ0ZXh0L2h0bWwiLAogICAgICAgICAgICBzdGF0dXM9NTAzLAogICAgICAgICkKICAgIHJldHVybiB3ZWIuUmVzcG9uc2UodGV4"
    "dD1fUEFHRSwgY29udGVudF90eXBlPSJ0ZXh0L2h0bWwiKQoKCmFzeW5jIGRlZiB3emFkbWluX2FwaShyZXF1ZXN0KToKICAgIHBh"
    "dGggPSByZXF1ZXN0LnBhdGgKCiAgICBpZiBwYXRoLmVuZHN3aXRoKCIvbG9naW4iKToKICAgICAgICBpcCA9IF9ub3JtX2lwKF9j"
    "bGllbnRfaXAocmVxdWVzdCkpCiAgICAgICAgaWYgbm90IF91YV9vayhyZXF1ZXN0KToKICAgICAgICAgICAgcmV0dXJuIHdlYi5q"
    "c29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAiZm9yYmlkZGVuIn0sIHN0YXR1cz00MDMpCiAgICAgICAgaWYgYXdhaXQgX2lzX2Jhbm5l"
    "ZChpcCk6CiAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSh7ImVycm9yIjogImJhbm5lZCJ9LCBzdGF0dXM9NDAz"
    "KQogICAgICAgIGlmIGF3YWl0IF9pcF9pbnRlbChpcCk6CiAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSgKICAg"
    "ICAgICAgICAgICAgIHsiZXJyb3IiOiAiZGF0YWNlbnRlci9wcm94eSBJUHMgYXJlIG5vdCBhbGxvd2VkIn0sIHN0YXR1cz00MDMK"
    "ICAgICAgICAgICAgKQogICAgICAgIF9yZW0gPSBhd2FpdCBfbG9ja291dF9zdGF0ZShpcCkKICAgICAgICBpZiBfcmVtOgogICAg"
    "ICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoCiAgICAgICAgICAgICAgICB7ImVycm9yIjogZiJ0b28gbWFueSBhdHRl"
    "bXB0cyDigJQge19sb2NrX3R4dChfcmVtKX0ifSwKICAgICAgICAgICAgICAgIHN0YXR1cz00MjksCiAgICAgICAgICAgICkKICAg"
    "ICAgICB0cnk6CiAgICAgICAgICAgIGJvZHkgPSBhd2FpdCByZXF1ZXN0Lmpzb24oKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246"
    "CiAgICAgICAgICAgIGJvZHkgPSB7fQogICAgICAgIHN1Ym1pdHRlZCA9IHN0cihib2R5LmdldCgicGFzcyIsICIiKSkKICAgICAg"
    "ICByZWFsID0gYXdhaXQgZ2V0X2FkbWluX3Bhc3MoKQogICAgICAgIGlmIG5vdCByZWFsOgogICAgICAgICAgICByZXR1cm4gd2Vi"
    "Lmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJhZG1pbiBwYXNzIHVuYXZhaWxhYmxlIn0sIHN0YXR1cz01MDMpCiAgICAgICAgaWYg"
    "bm90IHN1Ym1pdHRlZCBvciBub3QgY29tcGFyZV9kaWdlc3Qoc3VibWl0dGVkLCByZWFsKToKICAgICAgICAgICAgX3NlY3MsIF90"
    "aWVyID0gYXdhaXQgX3JlY29yZF9mYWlsKGlwKQogICAgICAgICAgICBpZiBfc2VjczoKICAgICAgICAgICAgICAgIHJldHVybiB3"
    "ZWIuanNvbl9yZXNwb25zZSgKICAgICAgICAgICAgICAgICAgICB7ImVycm9yIjogZiJ3cm9uZyBwYXNzd29yZCDigJQge19sb2Nr"
    "X3R4dChfc2Vjcyl9In0sCiAgICAgICAgICAgICAgICAgICAgc3RhdHVzPTQyOSwKICAgICAgICAgICAgICAgICkKICAgICAgICAg"
    "ICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAid3JvbmcgcGFzc3dvcmQifSwgc3RhdHVzPTQwMSkKICAgICAg"
    "ICBfcmVjb3JkX29rKGlwKQogICAgICAgIHRvayA9IGF3YWl0IF9zZXNzaW9uX3Rva2VuKCkKICAgICAgICByZXNwID0gd2ViLmpz"
    "b25fcmVzcG9uc2UoeyJvayI6IFRydWV9KQogICAgICAgIHJlc3Auc2V0X2Nvb2tpZSgKICAgICAgICAgICAgInd6YWRtaW4iLCB0"
    "b2ssIG1heF9hZ2U9U0VTU0lPTl9IICogMzYwMCwgaHR0cG9ubHk9VHJ1ZSwgc2FtZXNpdGU9IkxheCIKICAgICAgICApCiAgICAg"
    "ICAgcmV0dXJuIHJlc3AKCiAgICBpZiBub3QgYXdhaXQgX2NoZWNrX3Nlc3Npb24ocmVxdWVzdCk6CiAgICAgICAgcmV0dXJuIHdl"
    "Yi5qc29uX3Jlc3BvbnNlKHsiZXJyb3IiOiAidW5hdXRob3JpemVkIn0sIHN0YXR1cz00MDEpCiAgICBpZiBub3QgX3VhX29rKHJl"
    "cXVlc3QpIG9yIGF3YWl0IF9pc19iYW5uZWQoX25vcm1faXAoX2NsaWVudF9pcChyZXF1ZXN0KSkpOgogICAgICAgIHJldHVybiB3"
    "ZWIuanNvbl9yZXNwb25zZSh7ImVycm9yIjogImZvcmJpZGRlbiJ9LCBzdGF0dXM9NDAzKQoKICAgIGlmIHBhdGguZW5kc3dpdGgo"
    "Ii9sb2dvdXQiKToKICAgICAgICByZXNwID0gd2ViLmpzb25fcmVzcG9uc2UoeyJvayI6IFRydWV9KQogICAgICAgIHJlc3AuZGVs"
    "X2Nvb2tpZSgid3phZG1pbiIpCiAgICAgICAgcmV0dXJuIHJlc3AKCiAgICBpZiBwYXRoLmVuZHN3aXRoKCIvc3RhdGUiKToKICAg"
    "ICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoYXdhaXQgX3N0YXRlKCkpCgogICAgaWYgcGF0aC5lbmRzd2l0aCgiL2hpc3Rv"
    "cnkiKToKICAgICAgICB0cnk6CiAgICAgICAgICAgIHVpZCA9IGludChyZXF1ZXN0LnF1ZXJ5LmdldCgidWlkIiwgIjAiKSkKICAg"
    "ICAgICBleGNlcHQgVmFsdWVFcnJvcjoKICAgICAgICAgICAgdWlkID0gMAogICAgICAgIHEgPSAocmVxdWVzdC5xdWVyeS5nZXQo"
    "InEiKSBvciAiIikuc3RyaXAoKVs6NjBdCiAgICAgICAgZG9jcyA9IGF3YWl0IGZpbmQocSwgdWlkLCBhbGxfdXNlcnM9RmFsc2Us"
    "IGxpbWl0PTI1KSBpZiB1aWQgZWxzZSBbXQogICAgICAgIG91dCA9IFtdCiAgICAgICAgZm9yIGQgaW4gZG9jczoKICAgICAgICAg"
    "ICAgdGdfbGlua3MgPSBkLmdldCgidGdfbGlua3MiKSBvciBbXQogICAgICAgICAgICBvdXQuYXBwZW5kKAogICAgICAgICAgICAg"
    "ICAgewogICAgICAgICAgICAgICAgICAgICJuYW1lIjogc3RyKGQuZ2V0KCJuYW1lIiwgIiIpKVs6OTBdLAogICAgICAgICAgICAg"
    "ICAgICAgICJzaXplIjogaW50KGQuZ2V0KCJzaXplIikgb3IgMCksCiAgICAgICAgICAgICAgICAgICAgImRhdGUiOiBzdHIoZC5n"
    "ZXQoImRhdGUiKSBvciAiIilbOjE2XSwKICAgICAgICAgICAgICAgICAgICAidGciOiBzdHIodGdfbGlua3NbMF0pIGlmIHRnX2xp"
    "bmtzIGVsc2UgIiIsCiAgICAgICAgICAgICAgICAgICAgImNsb3VkIjogc3RyKGQuZ2V0KCJjbG91ZF9saW5rIikgb3IgIiIpLAog"
    "ICAgICAgICAgICAgICAgfQogICAgICAgICAgICApCiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKHsib2siOiBUcnVl"
    "LCAiaXRlbXMiOiBvdXR9KQoKICAgIGlmIHBhdGguZW5kc3dpdGgoIi9hY3Rpb24iKToKICAgICAgICB0cnk6CiAgICAgICAgICAg"
    "IGJvZHkgPSBhd2FpdCByZXF1ZXN0Lmpzb24oKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIGJvZHkgPSB7"
    "fQogICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZShhd2FpdCBfYWN0aW9uKGJvZHkpKQoKICAgIHJldHVybiB3ZWIuanNv"
    "bl9yZXNwb25zZSh7ImVycm9yIjogInVua25vd24gZW5kcG9pbnQifSwgc3RhdHVzPTQwNCkKCgphc3luYyBkZWYgX2FjdGlvbihh"
    "KToKICAgIGFjdCA9IHN0cihhLmdldCgiYWN0aW9uIiwgIiIpKS5zdHJpcCgpCiAgICB0cnk6CiAgICAgICAgdWlkID0gaW50KGEu"
    "Z2V0KCJ1aWQiLCAwKSBvciAwKQogICAgZXhjZXB0IChUeXBlRXJyb3IsIFZhbHVlRXJyb3IpOgogICAgICAgIHVpZCA9IDAKCiAg"
    "ICB0cnk6CiAgICAgICAgaWYgYWN0ID09ICJzZXRjYXAiOgogICAgICAgICAgICBnYiA9IGZsb2F0KGEuZ2V0KCJnYiIsIDApIG9y"
    "IDApCiAgICAgICAgICAgIGF3YWl0IHNldF91c2VyX2NhcCh1aWQsIGdiIGlmIGdiID4gMCBlbHNlIE5vbmUpCiAgICAgICAgICAg"
    "IHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYiY2FwIGZvciB7dWlkfSDihpIge19mbXRfZ2IoZ2IpIGlmIGdiID4gMCBlbHNl"
    "ICdkZWZhdWx0J30ifQogICAgICAgIGlmIGFjdCA9PSAiYmFuIjoKICAgICAgICAgICAgYXdhaXQgc2V0X3VzZXJfY2FwKHVpZCwg"
    "MCkKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJ1c2VyIHt1aWR9IGJsb2NrZWQgKGNhcCAwKSJ9CiAg"
    "ICAgICAgaWYgYWN0ID09ICJ1bmJhbiI6CiAgICAgICAgICAgIGF3YWl0IHNldF91c2VyX2NhcCh1aWQsIE5vbmUpCiAgICAgICAg"
    "ICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYidXNlciB7dWlkfSBiYWNrIHRvIGdsb2JhbCBkZWZhdWx0In0KICAgICAg"
    "ICBpZiBhY3QgPT0gInJlc2V0Y2FwIjoKICAgICAgICAgICAgYXdhaXQgcmVzZXRfdXNhZ2UodWlkKQogICAgICAgICAgICByZXR1"
    "cm4geyJvayI6IFRydWUsICJtc2ciOiBmInRvZGF5J3MgdXNhZ2UgcmVzZXQgZm9yIHt1aWR9In0KICAgICAgICBpZiBhY3QgPT0g"
    "ImRlZHVjdGNhcCI6CiAgICAgICAgICAgIGdiID0gZmxvYXQoYS5nZXQoImdiIiwgMCkgb3IgMCkKICAgICAgICAgICAgb2ssIG1z"
    "ZyA9IGF3YWl0IF9kZWR1Y3RfY2FwKHVpZCwgZ2IpCiAgICAgICAgICAgIHJldHVybiB7Im9rIjogb2ssICJtc2ciOiBtc2d9CiAg"
    "ICAgICAgaWYgYWN0ID09ICJkZWx1c2VyIjoKICAgICAgICAgICAgaWYgbm90IHVpZDoKICAgICAgICAgICAgICAgIHJldHVybiB7"
    "Im9rIjogRmFsc2UsICJtc2ciOiAibm8gdXNlciBpZCJ9CiAgICAgICAgICAgIGF3YWl0IGRlbGV0ZV91c2VyKHVpZCkKICAgICAg"
    "ICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJ1c2VyIHt1aWR9IHJlbW92ZWQifQogICAgICAgIGlmIGFjdCA9PSAi"
    "Ym90Y2FwIjoKICAgICAgICAgICAgZ2IgPSBmbG9hdChhLmdldCgiZ2IiLCAwKSBvciAwKQogICAgICAgICAgICBpZiBnYiA8PSAw"
    "OgogICAgICAgICAgICAgICAgcmV0dXJuIHsib2siOiBGYWxzZSwgIm1zZyI6ICJnaXZlIGEgcG9zaXRpdmUgR0IgdmFsdWUifQog"
    "ICAgICAgICAgICBhd2FpdCBzZXRfZ2xvYmFsX2NhcF9nYihnYikKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBUcnVlLCAibXNn"
    "IjogZiJkZWZhdWx0IGNhcCDihpIge19mbXRfZ2IoZ2IpfSJ9CiAgICAgICAgaWYgYWN0ID09ICJraWxsIjoKICAgICAgICAgICAg"
    "b2ssIG1zZyA9IGF3YWl0IF9raWxsX3Rhc2soYS5nZXQoIm1pZCIpKQogICAgICAgICAgICByZXR1cm4geyJvayI6IG9rLCAibXNn"
    "IjogbXNnfQogICAgICAgIGlmIGFjdCA9PSAia2lsbGFsbCI6CiAgICAgICAgICAgIGZyb20gLi4uIGltcG9ydCB0YXNrX2RpY3QK"
    "CiAgICAgICAgICAgIG4gPSAwCiAgICAgICAgICAgIGZvciBtaWQsIHQgaW4gbGlzdCh0YXNrX2RpY3QuaXRlbXMoKSk6CiAgICAg"
    "ICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgYXdhaXQgdC50YXNrKCkuY2FuY2VsX3Rhc2soKQogICAgICAgICAg"
    "ICAgICAgICAgIG4gKz0gMQogICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgICAgICBwYXNz"
    "CiAgICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYie259IHRhc2socykgY2FuY2VsbGVkIn0KICAgICAgICBp"
    "ZiBhY3QgPT0gInJlcG9ydCI6CiAgICAgICAgICAgIG9rLCBtc2cgPSBhd2FpdCBzZW5kX3JlcG9ydCgpCiAgICAgICAgICAgIHJl"
    "dHVybiB7Im9rIjogb2ssICJtc2ciOiBtc2d9CiAgICAgICAgcmV0dXJuIHsib2siOiBGYWxzZSwgIm1zZyI6IGYidW5rbm93biBh"
    "Y3Rpb246IHthY3R9In0KICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICByZXR1cm4geyJvayI6IEZhbHNlLCAibXNn"
    "Ijogc3RyKGUpWzoxNjBdfQoKCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSACiMgVGhlIHBhZ2Ug4oCUIDcgdGhlbWVzIChtYXRjaGluZyB0aGUgc3RyZWFtIHBsYXllciksIG1vYmls"
    "ZS1maXJzdAojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgAoKX1BBR0UgPSAiIiI8IWRvY3R5cGUgaHRtbD4KPGh0bWwgbGFuZz0iZW4iIGRhdGEtdGhlbWU9Im9ueXgiPgo8aGVhZD4K"
    "PG1ldGEgY2hhcnNldD0idXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGlu"
    "aXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJyb2JvdHMiIGNvbnRlbnQ9Im5vaW5kZXgiPgo8"
    "dGl0bGU+V1pNTC1YIENvbnRyb2w8L3RpdGxlPgo8c3R5bGU+Cjpyb290ey0tcjoxNnB4Oy0tc2FuczotYXBwbGUtc3lzdGVtLEJs"
    "aW5rTWFjU3lzdGVtRm9udCwnU2Vnb2UgVUknLFJvYm90byxzYW5zLXNlcmlmfQpbZGF0YS10aGVtZT0ib255eCJdey0tYmc6IzA1"
    "MDUwODstLXN1cmZhY2U6IzBhMGExMjstLXN1cmZhY2UtMjojMGUwZTE4Oy0tbGluZTpyZ2JhKDEzOSw5MiwyNDYsLjIyKTstLXRl"
    "eHQ6I2Y0ZjJmZjstLW11dGVkOiNiNmIwZDQ7LS1hY2NlbnQ6IzhiNWNmNjstLWFjY2VudC0yOiNhNzhiZmE7LS1hY2NlbnQtc29m"
    "dDojMWExMDMzOy0tZGFuZ2VyOiNmODcxNzE7LS1vazojNGFkZTgwfQpbZGF0YS10aGVtZT0iYXF1YSJdey0tYmc6IzAzMTQxYjst"
    "LXN1cmZhY2U6IzA1MWUyODstLXN1cmZhY2UtMjojMDcyNDJmOy0tbGluZTpyZ2JhKDM0LDIxMSwyMzgsLjIyKTstLXRleHQ6I2Vh"
    "ZmNmZjstLW11dGVkOiNhM2M2ZDE7LS1hY2NlbnQ6IzIyZDNlZTstLWFjY2VudC0yOiM2N2U4Zjk7LS1hY2NlbnQtc29mdDojMDYy"
    "NDMwOy0tZGFuZ2VyOiNmODcxNzE7LS1vazojNGFkZTgwfQpbZGF0YS10aGVtZT0iZW1iZXIiXXstLWJnOiMxMjBhMDU7LS1zdXJm"
    "YWNlOiMxYTBmMDc7LS1zdXJmYWNlLTI6IzIxMTQwYTstLWxpbmU6cmdiYSgyNDUsMTU4LDExLC4yMik7LS10ZXh0OiNmZGYzZTc7"
    "LS1tdXRlZDojZDBiOGEwOy0tYWNjZW50OiNmNTllMGI7LS1hY2NlbnQtMjojZmJiZjI0Oy0tYWNjZW50LXNvZnQ6IzJhMWEwNjst"
    "LWRhbmdlcjojZjg3MTcxOy0tb2s6IzRhZGU4MH0KW2RhdGEtdGhlbWU9ImRhcmsiXXstLWJnOiMwNDA0MGI7LS1zdXJmYWNlOiMw"
    "ODA4MTI7LS1zdXJmYWNlLTI6IzBlMGUxYTstLWxpbmU6cmdiYSg2MSwxMzUsMjU1LC4xOCk7LS10ZXh0OiNmNWY3ZmY7LS1tdXRl"
    "ZDojYzNjYmRmOy0tYWNjZW50OiMzZDg3ZmY7LS1hY2NlbnQtMjojNWI5ZGZmOy0tYWNjZW50LXNvZnQ6IzBkMWIzYTstLWRhbmdl"
    "cjojZmY2YjZiOy0tb2s6IzUxZTk4Y30KW2RhdGEtdGhlbWU9ImxpZ2h0Il17LS1iZzojZjJmNWZiOy0tc3VyZmFjZTojZmZmZmZm"
    "Oy0tc3VyZmFjZS0yOiNlZWYxZjg7LS1saW5lOnJnYmEoMTUsMjMsNDIsLjEyKTstLXRleHQ6IzBmMTcyYTstLW11dGVkOiM1YTY0"
    "Nzg7LS1hY2NlbnQ6IzI1NjNlYjstLWFjY2VudC0yOiMzYjgyZjY7LS1hY2NlbnQtc29mdDojZGJlYWZlOy0tZGFuZ2VyOiNkYzI2"
    "MjY7LS1vazojMTZhMzRhfQpbZGF0YS10aGVtZT0idmlicmFudCJdey0tYmc6IzBhMDYxMjstLXN1cmZhY2U6IzEzMGIyMDstLXN1"
    "cmZhY2UtMjojMWIxMTMwOy0tbGluZTpyZ2JhKDE2OCw4NSwyNDcsLjI1KTstLXRleHQ6I2ZhZjVmZjstLW11dGVkOiNjNGI1ZmQ7"
    "LS1hY2NlbnQ6I2E4NTVmNzstLWFjY2VudC0yOiNkOTQ2ZWY7LS1hY2NlbnQtc29mdDojMmUxMDY1Oy0tZGFuZ2VyOiNmYjcxODU7"
    "LS1vazojMzRkMzk5fQpbZGF0YS10aGVtZT0iYmxvc3NvbSJdey0tYmc6IzE2MGExMDstLXN1cmZhY2U6IzIwMTAxYjstLXN1cmZh"
    "Y2UtMjojMmExNjIyOy0tbGluZTpyZ2JhKDI0NCwxMTQsMTgyLC4yNSk7LS10ZXh0OiNmZGYyZjg7LS1tdXRlZDojZjlhOGQ0Oy0t"
    "YWNjZW50OiNmNDcyYjY7LS1hY2NlbnQtMjojZmRhNGFmOy0tYWNjZW50LXNvZnQ6IzRhMDQ0ZTstLWRhbmdlcjojZjg3MTcxOy0t"
    "b2s6IzZlZTdiN30KKntib3gtc2l6aW5nOmJvcmRlci1ib3g7bWFyZ2luOjA7cGFkZGluZzowfQpib2R5e2JhY2tncm91bmQ6dmFy"
    "KC0tYmcpO2NvbG9yOnZhcigtLXRleHQpO2ZvbnQ6MTVweC8xLjQ1IHZhcigtLXNhbnMpO21pbi1oZWlnaHQ6MTAwdmg7cGFkZGlu"
    "Zy1ib3R0b206NDBweDstd2Via2l0LXRhcC1oaWdobGlnaHQtY29sb3I6dHJhbnNwYXJlbnR9CmJvZHk6OmJlZm9yZXtjb250ZW50"
    "OiIiO3Bvc2l0aW9uOmZpeGVkO2luc2V0Oi0yMCU7ei1pbmRleDowO3BvaW50ZXItZXZlbnRzOm5vbmU7YmFja2dyb3VuZDpyYWRp"
    "YWwtZ3JhZGllbnQoMzYlIDMwJSBhdCAxOCUgMTIlLGNvbG9yLW1peChpbiBzcmdiLHZhcigtLWFjY2VudCkgMTYlLHRyYW5zcGFy"
    "ZW50KSx0cmFuc3BhcmVudCA3MCUpLHJhZGlhbC1ncmFkaWVudCgzMiUgMjglIGF0IDgyJSAyMCUsY29sb3ItbWl4KGluIHNyZ2Is"
    "dmFyKC0tYWNjZW50KSAxMCUsdHJhbnNwYXJlbnQpLHRyYW5zcGFyZW50IDcyJSl9Ci53cmFwe3Bvc2l0aW9uOnJlbGF0aXZlO3ot"
    "aW5kZXg6MTttYXgtd2lkdGg6NjgwcHg7bWFyZ2luOjAgYXV0bztwYWRkaW5nOjAgMTRweH0KLnRvcGJhcntwb3NpdGlvbjpzdGlj"
    "a3k7dG9wOjA7ei1pbmRleDoxMDtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDo4cHg7cGFkZGluZzoxNHB4IDJw"
    "eDtiYWNrZ3JvdW5kOmNvbG9yLW1peChpbiBzcmdiLHZhcigtLWJnKSA4OCUsdHJhbnNwYXJlbnQpO2JhY2tkcm9wLWZpbHRlcjpi"
    "bHVyKDE0cHgpO2JvcmRlci1ib3R0b206MXB4IHNvbGlkIHZhcigtLWxpbmUpfQoubG9nb3tmb250LXNpemU6MThweDtmb250LXdl"
    "aWdodDo4MDA7bGV0dGVyLXNwYWNpbmc6LS4wMmVtO2ZsZXg6MX0KLmxvZ28gYntjb2xvcjp2YXIoLS1hY2NlbnQpfQouY2hpcHtm"
    "b250LXNpemU6MTJweDtmb250LXdlaWdodDo2MDA7Y29sb3I6dmFyKC0tbXV0ZWQpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGlu"
    "ZSk7Ym9yZGVyLXJhZGl1czo5OTlweDtwYWRkaW5nOjRweCAxMHB4O2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZSl9Cmgye2ZvbnQt"
    "c2l6ZToxM3B4O3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTtsZXR0ZXItc3BhY2luZzouMDhlbTtjb2xvcjp2YXIoLS1tdXRlZCk7"
    "bWFyZ2luOjIycHggMnB4IDEwcHh9Ci5jYXJke2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZSk7Ym9yZGVyOjFweCBzb2xpZCB2YXIo"
    "LS1saW5lKTtib3JkZXItcmFkaXVzOnZhcigtLXIpO3BhZGRpbmc6MTRweDttYXJnaW4tYm90dG9tOjEycHh9Ci5zdGF0c3tkaXNw"
    "bGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCgyLDFmcik7Z2FwOjEwcHg7bWFyZ2luLXRvcDoxNHB4fQouc3Rh"
    "dHtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czoxNHB4"
    "O3BhZGRpbmc6MTJweH0KLnN0YXQgLnZ7Zm9udC1zaXplOjE5cHg7Zm9udC13ZWlnaHQ6ODAwfQouc3RhdCAua3tmb250LXNpemU6"
    "MTFweDtjb2xvcjp2YXIoLS1tdXRlZCk7dGV4dC10cmFuc2Zvcm06dXBwZXJjYXNlO2xldHRlci1zcGFjaW5nOi4wNmVtO21hcmdp"
    "bi10b3A6MnB4fQouYmFye2hlaWdodDo4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZS0yKTtv"
    "dmVyZmxvdzpoaWRkZW47bWFyZ2luOjEwcHggMCA2cHh9Ci5iYXIgaXtkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JvcmRlci1y"
    "YWRpdXM6OTlweDtiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCg5MGRlZyx2YXIoLS1hY2NlbnQpLHZhcigtLWFjY2VudC0yKSk7"
    "dHJhbnNpdGlvbjp3aWR0aCAuNXMgZWFzZX0KLm11dGVke2NvbG9yOnZhcigtLW11dGVkKTtmb250LXNpemU6MTIuNXB4fQoucm93"
    "e2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjhweH0KLnVzci1uYW1le2ZvbnQtd2VpZ2h0OjcwMDtmb250LXNp"
    "emU6MTVweH0KLmJ0bntib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZS0yKTtjb2xv"
    "cjp2YXIoLS10ZXh0KTtib3JkZXItcmFkaXVzOjEwcHg7cGFkZGluZzo3cHggMTFweDtmb250OjYwMCAxMi41cHggdmFyKC0tc2Fu"
    "cyk7Y3Vyc29yOnBvaW50ZXI7dHJhbnNpdGlvbjpib3JkZXItY29sb3IgLjE1cyx0cmFuc2Zvcm0gLjFzfQouYnRuOmFjdGl2ZXt0"
    "cmFuc2Zvcm06c2NhbGUoLjk2KX0KLmJ0bi5wcml7YmFja2dyb3VuZDpsaW5lYXItZ3JhZGllbnQoMTM1ZGVnLHZhcigtLWFjY2Vu"
    "dCksdmFyKC0tYWNjZW50LTIpKTtib3JkZXItY29sb3I6dHJhbnNwYXJlbnQ7Y29sb3I6I2ZmZn0KLmJ0bi5kbmd7Y29sb3I6dmFy"
    "KC0tZGFuZ2VyKTtib3JkZXItY29sb3I6Y29sb3ItbWl4KGluIHNyZ2IsdmFyKC0tZGFuZ2VyKSA0NSUsdHJhbnNwYXJlbnQpfQou"
    "YnRuc3tkaXNwbGF5OmZsZXg7ZmxleC13cmFwOndyYXA7Z2FwOjdweDttYXJnaW4tdG9wOjEwcHh9CmlucHV0W3R5cGU9cGFzc3dv"
    "cmRde3dpZHRoOjEwMCU7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlLTIpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9y"
    "ZGVyLXJhZGl1czoxMnB4O2NvbG9yOnZhcigtLXRleHQpO3BhZGRpbmc6MTNweCAxNHB4O2ZvbnQ6MTVweCB2YXIoLS1zYW5zKX0K"
    "aW5wdXRbdHlwZT1wYXNzd29yZF06Zm9jdXN7b3V0bGluZTpub25lO2JvcmRlci1jb2xvcjp2YXIoLS1hY2NlbnQtMil9Ci5sb2dp"
    "bi1ib3h7bWF4LXdpZHRoOjM2MHB4O21hcmdpbjoxNnZoIGF1dG8gMDt0ZXh0LWFsaWduOmNlbnRlcn0KLnRhc2t7ZGlzcGxheTpm"
    "bGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTBweH0KLnRhc2sgLm5te2ZsZXg6MTttaW4td2lkdGg6MDtmb250LXdlaWdodDo2"
    "MDA7Zm9udC1zaXplOjEzLjVweDt3aGl0ZS1zcGFjZTpub3dyYXA7b3ZlcmZsb3c6aGlkZGVuO3RleHQtb3ZlcmZsb3c6ZWxsaXBz"
    "aXN9Ci5oaWRkZW57ZGlzcGxheTpub25lfQojdG9hc3R7cG9zaXRpb246Zml4ZWQ7bGVmdDo1MCU7Ym90dG9tOjI2cHg7dHJhbnNm"
    "b3JtOnRyYW5zbGF0ZVgoLTUwJSkgdHJhbnNsYXRlWSgxMnB4KTtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UpO2NvbG9yOnZhcigt"
    "LXRleHQpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweCAxNnB4O2Zv"
    "bnQtc2l6ZToxM3B4O29wYWNpdHk6MDtwb2ludGVyLWV2ZW50czpub25lO3RyYW5zaXRpb246LjI1czt6LWluZGV4Ojk5O21heC13"
    "aWR0aDo4NXZ3fQojdG9hc3Quc2hvd3tvcGFjaXR5OjE7dHJhbnNmb3JtOnRyYW5zbGF0ZVgoLTUwJSkgdHJhbnNsYXRlWSgwKX0K"
    "I3RoZW1lc3tkaXNwbGF5Om5vbmU7cG9zaXRpb246YWJzb2x1dGU7cmlnaHQ6MDt0b3A6MTEwJTtiYWNrZ3JvdW5kOnZhcigtLXN1"
    "cmZhY2UpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6OHB4O3otaW5kZXg6"
    "MjA7Ym94LXNoYWRvdzowIDE4cHggNTBweCAtMTJweCByZ2JhKDAsMCwwLC41NSl9CiN0aGVtZXMub3BlbntkaXNwbGF5OmdyaWQ7"
    "Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnI7Z2FwOjRweH0KI3RoZW1lcyBidXR0b257ZGlzcGxheTpmbGV4O2FsaWduLWl0"
    "ZW1zOmNlbnRlcjtnYXA6OHB4O2JhY2tncm91bmQ6bm9uZTtib3JkZXI6bm9uZTtjb2xvcjp2YXIoLS10ZXh0KTtmb250OjYwMCAx"
    "Mi41cHggdmFyKC0tc2Fucyk7cGFkZGluZzo4cHggMTBweDtib3JkZXItcmFkaXVzOjlweDtjdXJzb3I6cG9pbnRlcn0KI3RoZW1l"
    "cyBidXR0b246aG92ZXJ7YmFja2dyb3VuZDp2YXIoLS1hY2NlbnQtc29mdCl9Ci5zd3t3aWR0aDoxNHB4O2hlaWdodDoxNHB4O2Jv"
    "cmRlci1yYWRpdXM6NXB4O2Rpc3BsYXk6aW5saW5lLWJsb2NrfQouaGlzdC1pdGVte2JvcmRlci1ib3R0b206MXB4IHNvbGlkIHZh"
    "cigtLWxpbmUpO3BhZGRpbmc6MTBweCAycHg7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTBweH0KLmhpc3Qt"
    "aXRlbTpsYXN0LWNoaWxke2JvcmRlcjpub25lfQouaGlzdC1pdGVtIGF7Y29sb3I6dmFyKC0tYWNjZW50LTIpO3RleHQtZGVjb3Jh"
    "dGlvbjpub25lO2ZvbnQtd2VpZ2h0OjcwMH0KLmVycntjb2xvcjp2YXIoLS1kYW5nZXIpO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10"
    "b3A6OHB4O21pbi1oZWlnaHQ6MThweH0KLnBpbGx7Zm9udC1zaXplOjExcHg7Zm9udC13ZWlnaHQ6NzAwO2JvcmRlci1yYWRpdXM6"
    "OTlweDtwYWRkaW5nOjJweCA4cHg7YmFja2dyb3VuZDp2YXIoLS1hY2NlbnQtc29mdCk7Y29sb3I6dmFyKC0tYWNjZW50LTIpfQou"
    "cGlsbC5iYW57YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYix2YXIoLS1kYW5nZXIpIDE4JSx0cmFuc3BhcmVudCk7Y29sb3I6"
    "dmFyKC0tZGFuZ2VyKX0KPC9zdHlsZT4KPC9oZWFkPgo8Ym9keT4KPGRpdiBjbGFzcz0id3JhcCI+CiAgPGRpdiBjbGFzcz0idG9w"
    "YmFyIj4KICAgIDxkaXYgY2xhc3M9ImxvZ28iPldaTUw8Yj4tWDwvYj4gQ29udHJvbDwvZGl2PgogICAgPHNwYW4gY2xhc3M9ImNo"
    "aXAiIGlkPSJjbG9jayI+PC9zcGFuPgogICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBpZD0idGhlbWVCdG4iPvCfjqg8L2J1dHRvbj4K"
    "ICAgIDxidXR0b24gY2xhc3M9ImJ0biBoaWRkZW4iIGlkPSJsb2dvdXRCdG4iPkV4aXQ8L2J1dHRvbj4KICAgIDxkaXYgaWQ9InRo"
    "ZW1lcyI+CiAgICAgIDxidXR0b24gZGF0YS10PSJvbnl4Ij48c3BhbiBjbGFzcz0ic3ciIHN0eWxlPSJiYWNrZ3JvdW5kOiM4YjVj"
    "ZjYiPjwvc3Bhbj5Pbnl4PC9idXR0b24+CiAgICAgIDxidXR0b24gZGF0YS10PSJhcXVhIj48c3BhbiBjbGFzcz0ic3ciIHN0eWxl"
    "PSJiYWNrZ3JvdW5kOiMyMmQzZWUiPjwvc3Bhbj5BcXVhPC9idXR0b24+CiAgICAgIDxidXR0b24gZGF0YS10PSJlbWJlciI+PHNw"
    "YW4gY2xhc3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDojZjU5ZTBiIj48L3NwYW4+RW1iZXI8L2J1dHRvbj4KICAgICAgPGJ1dHRv"
    "biBkYXRhLXQ9ImRhcmsiPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91bmQ6IzNkODdmZiI+PC9zcGFuPkRhcms8L2J1"
    "dHRvbj4KICAgICAgPGJ1dHRvbiBkYXRhLXQ9ImxpZ2h0Ij48c3BhbiBjbGFzcz0ic3ciIHN0eWxlPSJiYWNrZ3JvdW5kOiM5M2M1"
    "ZmQiPjwvc3Bhbj5MaWdodDwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtdD0idmlicmFudCI+PHNwYW4gY2xhc3M9InN3IiBz"
    "dHlsZT0iYmFja2dyb3VuZDojYTg1NWY3Ij48L3NwYW4+VmlicmFudDwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtdD0iYmxv"
    "c3NvbSI+PHNwYW4gY2xhc3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDojZjQ3MmI2Ij48L3NwYW4+Qmxvc3NvbTwvYnV0dG9uPgog"
    "ICAgPC9kaXY+CiAgPC9kaXY+CgogIDxkaXYgaWQ9ImxvZ2luIiBjbGFzcz0ibG9naW4tYm94IGhpZGRlbiI+CiAgICA8ZGl2IGNs"
    "YXNzPSJjYXJkIj4KICAgICAgPGRpdiBzdHlsZT0iZm9udC1zaXplOjM0cHg7bWFyZ2luLWJvdHRvbTo2cHgiPvCflJA8L2Rpdj4K"
    "ICAgICAgPGRpdiBzdHlsZT0iZm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi1ib3R0b206MnB4Ij5Pd25lciBk"
    "YXNoYm9hcmQ8L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tYm90dG9tOjE0cHgiPnNlbmQgL2Fk"
    "bWlucGFzcyBpbiBUZWxlZ3JhbSB0byBzZWUgdGhlIHBhc3N3b3JkPC9kaXY+CiAgICAgIDxpbnB1dCB0eXBlPSJwYXNzd29yZCIg"
    "aWQ9InB3IiBwbGFjZWhvbGRlcj0iQWRtaW4gcGFzc3dvcmQiIGF1dG9jb21wbGV0ZT0iY3VycmVudC1wYXNzd29yZCI+CiAgICAg"
    "IDxkaXYgY2xhc3M9ImVyciIgaWQ9ImxvZ2luRXJyIj48L2Rpdj4KICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIHByaSIgc3R5bGU9"
    "IndpZHRoOjEwMCU7cGFkZGluZzoxMnB4O21hcmdpbi10b3A6NnB4IiBpZD0ibG9naW5CdG4iPlVubG9jazwvYnV0dG9uPgogICAg"
    "PC9kaXY+CiAgPC9kaXY+CgogIDxkaXYgaWQ9ImFwcCIgY2xhc3M9ImhpZGRlbiI+CiAgICA8ZGl2IGNsYXNzPSJzdGF0cyIgaWQ9"
    "InN0YXRzIj48L2Rpdj4KICAgIDxoMj5BY3RpdmUgdGFza3M8L2gyPgogICAgPGRpdiBpZD0idGFza3MiPjwvZGl2PgogICAgPGgy"
    "PlVzZXJzPC9oMj4KICAgIDxkaXYgaWQ9InVzZXJzIj48L2Rpdj4KICAgIDxoMj5HbG9iYWw8L2gyPgogICAgPGRpdiBjbGFzcz0i"
    "Y2FyZCI+CiAgICAgIDxkaXYgY2xhc3M9InVzci1uYW1lIj5EZWZhdWx0IGNhcCA8c3BhbiBjbGFzcz0icGlsbCIgaWQ9ImdjYXAi"
    "Pjwvc3Bhbj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPmFwcGxpZXMgdG8gdXNlcnMgd2l0aG91dCBhIGN1c3RvbSBj"
    "YXA8L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0iYnRucyI+CiAgICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tC"
    "b3RDYXAoKSI+U2V0IGRlZmF1bHQgY2FwPC9idXR0b24+CiAgICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIGRuZyIgb25jbGljaz0i"
    "a2lsbEFsbCgpIj7inJUgS2lsbCBhbGwgdGFza3M8L2J1dHRvbj4KICAgICAgICA8YnV0dG9uIGNsYXNzPSJidG4iIG9uY2xpY2s9"
    "ImFjdCh7YWN0aW9uOidyZXBvcnQnfSx0aGlzKSI+U2VuZCByZXBvcnQgbm93PC9idXR0b24+CiAgICAgIDwvZGl2PgogICAgPC9k"
    "aXY+CiAgPC9kaXY+CjwvZGl2Pgo8ZGl2IGlkPSJ0b2FzdCI+PC9kaXY+CjxzY3JpcHQ+CiJ1c2Ugc3RyaWN0IjsKZnVuY3Rpb24g"
    "JChpZCl7cmV0dXJuIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKGlkKX0KZnVuY3Rpb24gdG9hc3QobSl7dmFyIHQ9JCgidG9hc3Qi"
    "KTt0LnRleHRDb250ZW50PW07dC5jbGFzc0xpc3QuYWRkKCJzaG93Iik7Y2xlYXJUaW1lb3V0KHQuX3QpO3QuX3Q9c2V0VGltZW91"
    "dChmdW5jdGlvbigpe3QuY2xhc3NMaXN0LnJlbW92ZSgic2hvdyIpfSwyMTAwKX0KZnVuY3Rpb24gZm10KGIpe2I9K2J8fDA7aWYo"
    "YjwxMDI0KXJldHVybiBiKyIgQiI7dmFyIHU9WyJLQiIsIk1CIiwiR0IiLCJUQiJdLGk9LTE7ZG97Yi89MTAyNDtpKyt9d2hpbGUo"
    "Yj49MTAyNCYmaTwzKTtyZXR1cm4gYi50b0ZpeGVkKGI+PTEwMD8wOjEpKyIgIit1W2ldfQp2YXIgRT17JyYnOicmYW1wOycsJzwn"
    "OicmbHQ7JywnPic6JyZndDsnLCciJzonJnF1b3Q7JywiJyI6JyYjMzk7J307CmZ1bmN0aW9uIGVzYyhzKXtyZXR1cm4gU3RyaW5n"
    "KHM9PW51bGw/Jyc6cykucmVwbGFjZSgvWyY8PiInXS9nLGZ1bmN0aW9uKGMpe3JldHVybiBFW2NdfSl9CmZ1bmN0aW9uIGFwaShw"
    "LG8pe3JldHVybiBmZXRjaCgiL3d6YWRtaW4vYXBpLyIrcCxvP3ttZXRob2Q6IlBPU1QiLGhlYWRlcnM6eyJDb250ZW50LVR5cGUi"
    "OiJhcHBsaWNhdGlvbi9qc29uIn0sYm9keTpKU09OLnN0cmluZ2lmeShvKX06dW5kZWZpbmVkKS50aGVuKGZ1bmN0aW9uKHIpe3Jl"
    "dHVybiByLmpzb24oKS50aGVuKGZ1bmN0aW9uKGope2ouX3M9ci5zdGF0dXM7cmV0dXJuIGp9KX0pfQpmdW5jdGlvbiBhY3QoYSxi"
    "dG4pe3ZhciBsYmw9YnRuP2J0bi50ZXh0Q29udGVudDpudWxsO2lmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250"
    "ZW50PSLigKYifQpyZXR1cm4gYXBpKCJhY3Rpb24iLGEpLnRoZW4oZnVuY3Rpb24oail7dG9hc3Qoai5tc2d8fGouZXJyb3J8fChq"
    "Lm9rPyJkb25lIjoiZmFpbGVkIikpO3JldHVybiByZWZyZXNoKCl9KS5jYXRjaChmdW5jdGlvbigpe3RvYXN0KCJuZXR3b3JrIGVy"
    "cm9yIil9KS5maW5hbGx5KGZ1bmN0aW9uKCl7aWYoYnRuKXtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PWxibH19"
    "KX0KCiQoInRoZW1lQnRuIikub25jbGljaz1mdW5jdGlvbihlKXtlLnN0b3BQcm9wYWdhdGlvbigpOyQoInRoZW1lcyIpLmNsYXNz"
    "TGlzdC50b2dnbGUoIm9wZW4iKX07CmRvY3VtZW50LmFkZEV2ZW50TGlzdGVuZXIoImNsaWNrIixmdW5jdGlvbigpeyQoInRoZW1l"
    "cyIpLmNsYXNzTGlzdC5yZW1vdmUoIm9wZW4iKX0pOwp2YXIgVEhFTUVTPVsib255eCIsImFxdWEiLCJlbWJlciIsImRhcmsiLCJs"
    "aWdodCIsInZpYnJhbnQiLCJibG9zc29tIl07CiQoInRoZW1lcyIpLnF1ZXJ5U2VsZWN0b3JBbGwoImJ1dHRvbiIpLmZvckVhY2go"
    "ZnVuY3Rpb24oYil7Yi5vbmNsaWNrPWZ1bmN0aW9uKCl7CiAgZG9jdW1lbnQuZG9jdW1lbnRFbGVtZW50LnNldEF0dHJpYnV0ZSgi"
    "ZGF0YS10aGVtZSIsYi5kYXRhc2V0LnQpOwogIHRyeXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgid3ptbC10aGVtZSIsYi5kYXRhc2V0"
    "LnQpfWNhdGNoKGUpe319fSk7CnRyeXt2YXIgdGg9bG9jYWxTdG9yYWdlLmdldEl0ZW0oInd6bWwtdGhlbWUiKTtpZih0aCYmVEhF"
    "TUVTLmluZGV4T2YodGgpPi0xKWRvY3VtZW50LmRvY3VtZW50RWxlbWVudC5zZXRBdHRyaWJ1dGUoImRhdGEtdGhlbWUiLHRoKX1j"
    "YXRjaChlKXt9CgpzZXRJbnRlcnZhbChmdW5jdGlvbigpe3ZhciBkPW5ldyBEYXRlOyQoImNsb2NrIikudGV4dENvbnRlbnQ9ZC50"
    "b0xvY2FsZVRpbWVTdHJpbmcoW10se2hvdXI6IjItZGlnaXQiLG1pbnV0ZToiMi1kaWdpdCJ9KX0sMTAwMCk7CgokKCJsb2dpbkJ0"
    "biIpLm9uY2xpY2s9ZG9Mb2dpbjsKJCgicHciKS5hZGRFdmVudExpc3RlbmVyKCJrZXlkb3duIixmdW5jdGlvbihlKXtpZihlLmtl"
    "eT09PSJFbnRlciIpZG9Mb2dpbigpfSk7CmZ1bmN0aW9uIGRvTG9naW4oKXskKCJsb2dpbkVyciIpLnRleHRDb250ZW50PSIiO2Fw"
    "aSgibG9naW4iLHtwYXNzOiQoInB3IikudmFsdWV9KS50aGVuKGZ1bmN0aW9uKGopewogIGlmKGoub2spe2VudGVyKCk7cmVmcmVz"
    "aCgpfQogIGVsc2V7JCgibG9naW5FcnIiKS50ZXh0Q29udGVudD1qLmVycm9yfHwid3JvbmcgcGFzc3dvcmQifQp9KS5jYXRjaChm"
    "dW5jdGlvbigpeyQoImxvZ2luRXJyIikudGV4dENvbnRlbnQ9Im5ldHdvcmsgZXJyb3IifSl9CmZ1bmN0aW9uIGVudGVyKCl7JCgi"
    "bG9naW4iKS5jbGFzc0xpc3QuYWRkKCJoaWRkZW4iKTskKCJhcHAiKS5jbGFzc0xpc3QucmVtb3ZlKCJoaWRkZW4iKTskKCJsb2dv"
    "dXRCdG4iKS5jbGFzc0xpc3QucmVtb3ZlKCJoaWRkZW4iKX0KJCgibG9nb3V0QnRuIikub25jbGljaz1mdW5jdGlvbigpe2FwaSgi"
    "bG9nb3V0IikudGhlbihmdW5jdGlvbigpe2xvY2F0aW9uLnJlbG9hZCgpfSl9OwoKZnVuY3Rpb24gcmVmcmVzaCgpe3JldHVybiBh"
    "cGkoInN0YXRlIikudGhlbihmdW5jdGlvbihqKXsKICBpZihqLl9zPT09NDAxKXskKCJhcHAiKS5jbGFzc0xpc3QuYWRkKCJoaWRk"
    "ZW4iKTskKCJsb2dvdXRCdG4iKS5jbGFzc0xpc3QuYWRkKCJoaWRkZW4iKTskKCJsb2dpbiIpLmNsYXNzTGlzdC5yZW1vdmUoImhp"
    "ZGRlbiIpO3JldHVybn0KICBpZighai5vayl7dG9hc3QoInN0YXRlIGVycm9yIik7cmV0dXJufQogIHZhciB0PWoudG90YWxzOwog"
    "ICQoInN0YXRzIikuaW5uZXJIVE1MPQogICAgJzxkaXYgY2xhc3M9InN0YXQiPjxkaXYgY2xhc3M9InYiPicrZm10KHQudXNlZCkr"
    "JzwvZGl2PjxkaXYgY2xhc3M9ImsiPlVzZWQgdG9kYXk8L2Rpdj48L2Rpdj4nKwogICAgJzxkaXYgY2xhc3M9InN0YXQiPjxkaXYg"
    "Y2xhc3M9InYiPicrZm10KHQucmVzZXJ2ZWQpKyc8L2Rpdj48ZGl2IGNsYXNzPSJrIj5IZWxkIGJ5IHRhc2tzPC9kaXY+PC9kaXY+"
    "JysKICAgICc8ZGl2IGNsYXNzPSJzdGF0Ij48ZGl2IGNsYXNzPSJ2Ij4nK3QudGFza3MrJzwvZGl2PjxkaXYgY2xhc3M9ImsiPkFj"
    "dGl2ZSB0YXNrczwvZGl2PjwvZGl2PicrCiAgICAnPGRpdiBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0idiI+Jyt0LnVzZXJzKyc8"
    "L2Rpdj48ZGl2IGNsYXNzPSJrIj5Vc2VyczwvZGl2PjwvZGl2Pic7CgogIHZhciBUPSIiOwogIChqLnRhc2tzfHxbXSkuZm9yRWFj"
    "aChmdW5jdGlvbih4KXsKICAgIFQrPSc8ZGl2IGNsYXNzPSJjYXJkIj48ZGl2IGNsYXNzPSJ0YXNrIj48ZGl2IGNsYXNzPSJubSI+"
    "Jytlc2MoeC5uYW1lKSsnPC9kaXY+PHNwYW4gY2xhc3M9InBpbGwiPicrZXNjKHgudGFnfHx4LnVpZCkrJzwvc3Bhbj4nKwogICAg"
    "ICAgJzxidXR0b24gY2xhc3M9ImJ0biBkbmciIG9uY2xpY2s9ImFjdCh7YWN0aW9uOicrIidraWxsJyIrJyxtaWQ6Jyt4Lm1pZCsn"
    "fSx0aGlzKSI+4pyVIEtpbGw8L2J1dHRvbj48L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9ImJhciI+PGkgc3R5bGU9IndpZHRo"
    "OicreC5wY3QrJyUiPjwvaT48L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9Im11dGVkIj4nK2ZtdCh4LnByb2MpKycgLyAnK2Zt"
    "dCh4LnNpemUpKycgwrcgJytlc2MoeC5zcGVlZCkrKHguZXRhPyIgwrcgRVRBICIrZXNjKHguZXRhKToiIikrJzwvZGl2PjwvZGl2"
    "Pid9KTsKICAkKCJ0YXNrcyIpLmlubmVySFRNTD1UfHwnPGRpdiBjbGFzcz0iY2FyZCBtdXRlZCI+Tm8gYWN0aXZlIHRhc2tzLiBQ"
    "ZWFjZS48L2Rpdj4nOwoKICB2YXIgVT0iIjsKICAoai51c2Vyc3x8W10pLmZvckVhY2goZnVuY3Rpb24odSl7CiAgICB2YXIgbm09"
    "ZXNjKHUubmFtZXx8dS51bmFtZXx8dS51aWQpOwogICAgaWYodS51bmFtZSYmdS5uYW1lJiZ1LnVuYW1lIT09dS5uYW1lKW5tKz0i"
    "IMK3ICIrZXNjKHUudW5hbWUpOwogICAgdmFyIGNhcGw9dS5jYXBfZ2I9PT0wPyJibG9ja2VkIjoodS5jYXBfZ2I/dS5jYXBfZ2Ir"
    "IiBHQiBjYXAiOiJkZWZhdWx0IGNhcCIpOwogICAgdmFyIGxpdmU9dS5yZXNlcnZlZD4wOwogICAgVSs9JzxkaXYgY2xhc3M9ImNh"
    "cmQiPicrCiAgICAgICAnPGRpdiBjbGFzcz0icm93Ij48ZGl2IGNsYXNzPSJ1c3ItbmFtZSIgc3R5bGU9ImZsZXg6MSI+JytubSsn"
    "IDxzcGFuIGNsYXNzPSJtdXRlZCI+IycrdS51aWQrJzwvc3Bhbj48L2Rpdj4nKwogICAgICAgKHUuYmFubmVkPyc8c3BhbiBjbGFz"
    "cz0icGlsbCBiYW4iPkJBTk5FRDwvc3Bhbj4nOicnKSsnPC9kaXY+JysKICAgICAgICc8ZGl2IGNsYXNzPSJiYXIiPjxpIHN0eWxl"
    "PSJ3aWR0aDonK01hdGgubWluKHUucGN0LDEwMCkrJyUiPjwvaT48L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9Im11dGVkIj4n"
    "K2ZtdCh1LnVzZWQpKyhsaXZlPycgPHNwYW4gc3R5bGU9ImNvbG9yOnZhcigtLWFjY2VudC0yKSI+KycrZm10KHUucmVzZXJ2ZWQp"
    "KycgcnVubmluZzwvc3Bhbj4nOicnKSsnIC8gJytmbXQodS5jYXApKycgwrcgJytjYXBsKycgwrcgJyt1LnRhc2tzKycgdGFza3Mg"
    "dG90YWw8L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9ImJ0bnMiPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNs"
    "aWNrPSJhY3Qoe2FjdGlvbjonKyInc2V0Y2FwJyIrJyx1aWQ6Jyt1LnVpZCsnLGdiOmNhcE9mKCcrdS51aWQrJyktMX0sdGhpcyki"
    "PuKIkjEgR0I8L2J1dHRvbj4nKwogICAgICAgJzxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYWN0KHthY3Rpb246JysiJ3Nl"
    "dGNhcCciKycsdWlkOicrdS51aWQrJyxnYjpjYXBPZignK3UudWlkKycpKzF9LHRoaXMpIj4rMSBHQjwvYnV0dG9uPicrCiAgICAg"
    "ICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tDYXAoJyt1LnVpZCsnKSI+U2V0IGNhcDwvYnV0dG9uPicrCiAgICAg"
    "ICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhY3Qoe2FjdGlvbjonKyIncmVzZXRjYXAnIisnLHVpZDonK3UudWlkKyd9"
    "LHRoaXMpIj5SZXNldCBkYXk8L2J1dHRvbj4nKwogICAgICAgJzxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYXNrRGVkdWN0"
    "KCcrdS51aWQrJykiPkRlZHVjdDwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIGRuZyIgb25jbGljaz0iYXNr"
    "QmFuKCcrdS51aWQrJykiPicrKHUuYmFubmVkPydVbmJhbic6J0JhbicpKyc8L2J1dHRvbj4nKwogICAgICAgJzxidXR0b24gY2xh"
    "c3M9ImJ0biBkbmciIG9uY2xpY2s9ImFza0RlbCgnK3UudWlkKycpIj5SZW1vdmU8L2J1dHRvbj4nKwogICAgICAgJzxidXR0b24g"
    "Y2xhc3M9ImJ0biIgb25jbGljaz0ic2hvd0hpc3QoJyt1LnVpZCsnKSI+SGlzdG9yeTwvYnV0dG9uPicrCiAgICAgICAnPC9kaXY+"
    "PC9kaXY+J30pOwogICQoInVzZXJzIikuaW5uZXJIVE1MPVV8fCc8ZGl2IGNsYXNzPSJjYXJkIG11dGVkIj5ObyB1c2VycyB5ZXQu"
    "PC9kaXY+JzsKICAkKCJnY2FwIikudGV4dENvbnRlbnQ9KGouZ2xvYmFsX2NhcF9nYnx8MTUpKyIgR0IiOwogIHdpbmRvdy5fdXNl"
    "cnM9ai51c2Vyc3x8W107d2luZG93Ll9nY2FwPWouZ2xvYmFsX2NhcF9nYnx8MTU7Cn0pLmNhdGNoKGZ1bmN0aW9uKCl7fSl9Cgpm"
    "dW5jdGlvbiBjYXBPZih1aWQpe3ZhciB1PSh3aW5kb3cuX3VzZXJzfHxbXSkuZmlsdGVyKGZ1bmN0aW9uKHgpe3JldHVybiB4LnVp"
    "ZD09PXVpZH0pWzBdOwppZighdSlyZXR1cm4gMTU7cmV0dXJuKHUuY2FwX2diPT1udWxsKT8od2luZG93Ll9nY2FwfHwxNSk6dS5j"
    "YXBfZ2J9CmZ1bmN0aW9uIGFza0NhcCh1aWQpe3ZhciB2PXByb21wdCgiTmV3IGNhcCBpbiBHQiBmb3IgIit1aWQrIlxcbigwID0g"
    "YmFjayB0byBnbG9iYWwgZGVmYXVsdCkiLCIiKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246InNldGNhcCIsdWlkOnVp"
    "ZCxnYjpwYXJzZUZsb2F0KHZ8fCIwIil8fDB9KX0KZnVuY3Rpb24gYXNrQm90Q2FwKCl7dmFyIHY9cHJvbXB0KCJEZWZhdWx0IGNh"
    "cCBmb3IgQUxMIHVzZXJzIChHQikiLCIiKyh3aW5kb3cuX2djYXB8fDE1KSk7aWYodj09PW51bGwpcmV0dXJuO2FjdCh7YWN0aW9u"
    "OiJib3RjYXAiLGdiOnBhcnNlRmxvYXQodil8fDB9KX0KZnVuY3Rpb24gYXNrRGVkdWN0KHVpZCl7dmFyIHY9cHJvbXB0KCJSZWR1"
    "Y2UgY2FwIGJ5IChHQikgZm9yICIrdWlkLCIwLjUiKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246ImRlZHVjdGNhcCIs"
    "dWlkOnVpZCxnYjpwYXJzZUZsb2F0KHYpfHwwfSl9CmZ1bmN0aW9uIGFza0Jhbih1aWQpe3ZhciB1PSh3aW5kb3cuX3VzZXJzfHxb"
    "XSkuZmlsdGVyKGZ1bmN0aW9uKHgpe3JldHVybiB4LnVpZD09PXVpZH0pWzBdOwppZih1JiZ1LmJhbm5lZCl7YWN0KHthY3Rpb246"
    "InVuYmFuIix1aWQ6dWlkfSl9CmVsc2UgaWYoY29uZmlybSgiQmFuIHVzZXIgIit1aWQrIj8gKGNhcCA9IDAg4oCUIGFsbCB0YXNr"
    "cyBibG9ja2VkKSIpKXthY3Qoe2FjdGlvbjoiYmFuIix1aWQ6dWlkfSl9fQpmdW5jdGlvbiBhc2tEZWwodWlkKXtpZihjb25maXJt"
    "KCJSZW1vdmUgdXNlciAiK3VpZCsiIGZyb20gdGhlIHJlZ2lzdHJ5PyAodGhlaXIgL2ZpbmQgaGlzdG9yeSBpcyBrZXB0KSIpKWFj"
    "dCh7YWN0aW9uOiJkZWx1c2VyIix1aWQ6dWlkfSl9CmZ1bmN0aW9uIGtpbGxBbGwoKXtpZihjb25maXJtKCJDYW5jZWwgQUxMIGFj"
    "dGl2ZSB0YXNrcz8iKSlhY3Qoe2FjdGlvbjoia2lsbGFsbCJ9KX0KCmZ1bmN0aW9uIHNob3dIaXN0KHVpZCl7CiAgYXBpKCJoaXN0"
    "b3J5P3VpZD0iK3VpZCkudGhlbihmdW5jdGlvbihqKXsKICAgIHZhciBoPShqLml0ZW1zfHxbXSkubWFwKGZ1bmN0aW9uKGQpewog"
    "ICAgICByZXR1cm4gJzxkaXYgY2xhc3M9Imhpc3QtaXRlbSI+PGRpdiBzdHlsZT0iZmxleDoxO21pbi13aWR0aDowIj48ZGl2IHN0"
    "eWxlPSJmb250LXdlaWdodDo2MDA7Zm9udC1zaXplOjEzcHg7d2hpdGUtc3BhY2U6bm93cmFwO292ZXJmbG93OmhpZGRlbjt0ZXh0"
    "LW92ZXJmbG93OmVsbGlwc2lzIj4nK2VzYyhkLm5hbWUpKyc8L2Rpdj48ZGl2IGNsYXNzPSJtdXRlZCI+JytmbXQoZC5zaXplKSsn"
    "IMK3ICcrZXNjKGQuZGF0ZSkrJzwvZGl2PjwvZGl2PicrKGQudGc/JzxhIGhyZWY9IicrZXNjKGQudGcpKyciPuKWtjwvYT4nOicn"
    "KSsoZC5jbG91ZD8nPGEgaHJlZj0iJytlc2MoZC5jbG91ZCkrJyI+4piBPC9hPic6JycpKyc8L2Rpdj4nfSkuam9pbigiIik7CiAg"
    "ICBpZighaCloPSc8ZGl2IGNsYXNzPSJtdXRlZCI+Tm8gaGlzdG9yeSB5ZXQuPC9kaXY+JzsKICAgIHZhciBjPWRvY3VtZW50LmNy"
    "ZWF0ZUVsZW1lbnQoImRpdiIpO2MuY2xhc3NOYW1lPSJjYXJkIjsKICAgIGMuaW5uZXJIVE1MPSc8ZGl2IGNsYXNzPSJyb3ciPjxk"
    "aXYgY2xhc3M9InVzci1uYW1lIiBzdHlsZT0iZmxleDoxIj5IaXN0b3J5PC9kaXY+PGJ1dHRvbiBjbGFzcz0iYnRuIiBpZD0iaGlz"
    "dFgiPuKclTwvYnV0dG9uPjwvZGl2PicraDsKICAgICQoInVzZXJzIikucHJlcGVuZChjKTtjLnF1ZXJ5U2VsZWN0b3IoIiNoaXN0"
    "WCIpLm9uY2xpY2s9ZnVuY3Rpb24oKXtjLnJlbW92ZSgpfTsKICAgIGMuc2Nyb2xsSW50b1ZpZXcoe2JlaGF2aW9yOiJzbW9vdGgi"
    "fSk7CiAgfSl9CgphcGkoInN0YXRlIikudGhlbihmdW5jdGlvbihqKXtpZihqLm9rKWVudGVyKCk7ZWxzZSAkKCJsb2dpbiIpLmNs"
    "YXNzTGlzdC5yZW1vdmUoImhpZGRlbiIpfSkKICAuY2F0Y2goZnVuY3Rpb24oKXskKCJsb2dpbiIpLmNsYXNzTGlzdC5yZW1vdmUo"
    "ImhpZGRlbiIpfSk7CnJlZnJlc2goKTsKc2V0SW50ZXJ2YWwoZnVuY3Rpb24oKXtpZighZG9jdW1lbnQuaGlkZGVuJiYkKCJhcHAi"
    "KS5jbGFzc05hbWUuaW5kZXhPZigiaGlkZGVuIik9PT0tMSlyZWZyZXNoKCl9LDUwMDApOwo8L3NjcmlwdD4KPC9ib2R5Pgo8L2h0"
    "bWw+CiIiIgo="
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
        log("  r1: wrote bot/helper/wzfix/r1_core.py + bot/modules/wzfix_admin.py")
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
                '        if not _qmsg and not getattr(message, "_wzfix_held", False):\n'
                '            _qmsg = await over_limit_msg(user_id, user_dict)\n'
                '        if _qmsg:\n'
                '            msg.append(_qmsg)\n'
                '    except Exception:\n'
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
                log("  r1: commands registered (find usage qusers setcap addcap deductcap deluser resetcap botcap dbstats dbclean wzfixdiag adminpass)")
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
    Background thread: verify every 45s that this session still owns the lock.
    If a newer session has taken over, shut this session down gracefully.
    Requires 3 consecutive mismatches before acting (tolerates transient DB
    errors) and never terminates while the DB is merely unreachable.
    """
    database_url = config.get("DATABASE_URL", "")
    mismatches = 0
    while not SHUTDOWN_EVENT.is_set():
        time.sleep(45)
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
