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
    "gyIpCiAgICB3aG8gPSBmIkB7b3duZXJ9IiBpZiBvd25lciBlbHNlICJ0aGUgb3duZXIiCiAgICBsaW5lcy5hcHBlbmQoCiAgICAg"
    "ICAgIuKUliBOb3QgZW5vdWdoIGJhbmR3aWR0aCB0b2RheSDigJQgdHJ5IGFnYWluIHRvbW9ycm93IChyZXNldHMgMDA6MDAgIgog"
    "ICAgICAgIGYiSVNUKSBvciBhc2sgPGI+e3dob308L2I+IGZvciBtb3JlIGJhbmR3aWR0aCIKICAgICkKICAgIHJldHVybiAiXG4i"
    "LmpvaW4obGluZXMpCgoKYXN5bmMgZGVmIHJlc2VydmVkX3RvZGF5KHVzZXJfaWQsIGRheT1Ob25lKToKICAgICIiIlRvdGFsIGJh"
    "bmR3aWR0aCBjdXJyZW50bHkgaGVsZCBieSB0aGlzIHVzZXIncyBydW5uaW5nIHRhc2tzCiAgICAoYXRvbWljIGNvdW50ZXIgb24g"
    "dG9kYXkncyBxdW90YSBkb2N1bWVudCkuIiIiCiAgICBkYXkgPSBkYXkgb3IgX2RheV9pc3QoKQogICAgdHJ5OgogICAgICAgIGRv"
    "YyA9IGF3YWl0IF9kYigpLnd6Zml4X3F1b3RhW19wYXJ0KCldLmZpbmRfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYie3VzZXJf"
    "aWR9OntkYXl9In0sIHsicmVzZXJ2ZWQiOiAxfQogICAgICAgICkKICAgICAgICByZXR1cm4gaW50KChkb2Mgb3Ige30pLmdldCgi"
    "cmVzZXJ2ZWQiKSBvciAwKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHJl"
    "c2VydmVkX3RvZGF5IGZhaWxlZCAodHJlYXRlZCBhcyAwKToge2V9IikKICAgICAgICByZXR1cm4gMAoKCmFzeW5jIGRlZiBfdHJ5"
    "X2hvbGQodXNlcl9pZCwgYncsIGNhcD1Ob25lKToKICAgICIiIkF0b21pYyBiYW5kd2lkdGggaG9sZDogb25lIHNlcnZlci1zaWRl"
    "ICRpbmMgKyB2ZXJpZnkgKyByb2xsYmFjay4KICAgIFJldHVybnMgKHJlYXNvbiwgcWlkKSDigJQgcmVhc29uIGlzIE5vbmUgd2hl"
    "biB0aGUgaG9sZCB3YXMgZ3JhbnRlZC4iIiIKICAgIGlmIGJ3IDw9IDA6CiAgICAgICAgcmV0dXJuIE5vbmUsIE5vbmUKICAgIHRy"
    "eToKICAgICAgICBpZiBjYXAgaXMgTm9uZToKICAgICAgICAgICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1c2VyX2lkKQog"
    "ICAgICAgIHFpZCA9IGYie3VzZXJfaWR9OntfZGF5X2lzdCgpfSIKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9xdW90YVtfcGFy"
    "dCgpXQogICAgICAgIGJlZm9yZSA9IGF3YWl0IGNvbC5maW5kX29uZV9hbmRfdXBkYXRlKAogICAgICAgICAgICB7Il9pZCI6IHFp"
    "ZH0sCiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICIkaW5jIjogeyJyZXNlcnZlZCI6IGludChidyl9LAogICAgICAgICAg"
    "ICAgICAgIiRzZXRPbkluc2VydCI6IHsKICAgICAgICAgICAgICAgICAgICAidXNlZCI6IDAsCiAgICAgICAgICAgICAgICAgICAg"
    "ImV4cGlyZUF0IjogZGF0ZXRpbWUuZnJvbXRpbWVzdGFtcCgKICAgICAgICAgICAgICAgICAgICAgICAgdGltZSgpICsgV1pGSVhf"
    "UVVPVEFfVFRMX0RBWVMgKiA4NjQwMCwgVVRDCiAgICAgICAgICAgICAgICAgICAgKSwKICAgICAgICAgICAgICAgIH0sCiAgICAg"
    "ICAgICAgIH0sCiAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICAgICByZXR1cm5fZG9jdW1lbnQ9UmV0dXJuRG9jdW1l"
    "bnQuQkVGT1JFLAogICAgICAgICkKICAgICAgICB1c2VkID0gaW50KChiZWZvcmUgb3Ige30pLmdldCgidXNlZCIpIG9yIDApCiAg"
    "ICAgICAgcmVzX2IgPSBpbnQoKGJlZm9yZSBvciB7fSkuZ2V0KCJyZXNlcnZlZCIpIG9yIDApCiAgICAgICAgaWYgdXNlZCArIHJl"
    "c19iID49IGNhcDoKICAgICAgICAgICAgcmVhc29uID0gImF0X2xpbWl0IgogICAgICAgIGVsaWYgdXNlZCArIHJlc19iICsgYncg"
    "PiBjYXAgKyBXWkZJWF9HUkFDRV9NQiAqIE1COgogICAgICAgICAgICByZWFzb24gPSAib3ZlcnNob290IgogICAgICAgIGVsc2U6"
    "CiAgICAgICAgICAgIHJlYXNvbiA9IE5vbmUKICAgICAgICBpZiByZWFzb246CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAg"
    "ICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgICAgIHsiX2lkIjogcWlkfSwgeyIkaW5jIjogeyJyZXNl"
    "cnZlZCI6IC1pbnQoYncpfX0KICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAg"
    "ICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggcmVzZXJ2ZSByb2xsYmFjayBmYWlsZWQgKGhlYWxzIG9uIGJvb3QpOiB7"
    "ZX0iKQogICAgICAgICAgICByZXR1cm4gcmVhc29uLCBxaWQKICAgICAgICByZXR1cm4gTm9uZSwgcWlkCiAgICBleGNlcHQgRXhj"
    "ZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggX3RyeV9ob2xkIGZhaWxlZCAoYWxsb3dlZCk6IHtlfSIp"
    "CiAgICAgICAgcmV0dXJuIE5vbmUsIE5vbmUKCgphc3luYyBkZWYgcmVzZXJ2ZShsaXN0ZW5lciwgYncsIGNhcD1Ob25lKToKICAg"
    "ICIiIkhvbGQgYmFuZHdpZHRoIGZvciBhIHJ1bm5pbmcgdGFzayAoYXRvbWljIOKAlCBzZWUgX3RyeV9ob2xkKS4iIiIKICAgIGlm"
    "IGJ3IDw9IDA6CiAgICAgICAgcmV0dXJuIE5vbmUKICAgIHRyeToKICAgICAgICBpZiBnZXRhdHRyKGxpc3RlbmVyLCAiX3d6Zml4"
    "X3Jlc3YiLCBOb25lKToKICAgICAgICAgICAgYXdhaXQgcmVsZWFzZV9yZXNlcnZlKGxpc3RlbmVyKSAgIyByZS1ldmFsdWF0aW9u"
    "IHJlcGxhY2VzIHRoZSBob2xkCiAgICAgICAgcmVhc29uLCBxaWQgPSBhd2FpdCBfdHJ5X2hvbGQobGlzdGVuZXIudXNlcl9pZCwg"
    "YncsIGNhcCkKICAgICAgICBpZiByZWFzb246CiAgICAgICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgZiJXWkZJ"
    "WCByZXNlcnZlOiB1c2VyPXtsaXN0ZW5lci51c2VyX2lkfSBob2xkPXtid30gcmVqZWN0ZWQgIgogICAgICAgICAgICAgICAgZiIo"
    "e3JlYXNvbn0pIGNhcD17Y2FwfSIKICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4gcmVhc29uCiAgICAgICAgbGlzdGVu"
    "ZXIuX3d6Zml4X3Jlc3YgPSAocWlkLCBpbnQoYncpKQogICAgICAgIHJldHVybiBOb25lCiAgICBleGNlcHQgRXhjZXB0aW9uIGFz"
    "IGU6CiAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggcmVzZXJ2ZSBmYWlsZWQgKGFsbG93ZWQpOiB7ZX0iKQogICAgICAgIHJl"
    "dHVybiBOb25lCgoKYXN5bmMgZGVmIHJlc2VydmVfcHJlKG1pZCwgdXNlcl9pZCwgc2l6ZSk6CiAgICAiIiJQcmUtZG93bmxvYWQg"
    "aG9sZCwgdGFrZW4gaW4gcHJlX3Rhc2tfY2hlY2sgQkVGT1JFIGFueSBkb3dubG9hZGVyCiAgICBzdGFydHMgKGtleWVkIGJ5IHRo"
    "ZSBjb21tYW5kIG1lc3NhZ2UgaWQpLiBUaGUgdGFzayBhZG9wdHMgaXQgbGF0ZXIgaW4KICAgIGxpbWl0X2NoZWNrZXIgdmlhIF9h"
    "ZG9wdF9wcmUsIHNvIHRoZSBiYW5kd2lkdGggaXMgaGVsZCBmcm9tIHRoZSB2ZXJ5CiAgICBmaXJzdCBtb21lbnQg4oCUIHNpbXVs"
    "dGFuZW91cyBsaW5rcyBjYW4gbmV2ZXIgZWFjaCBwYXNzIGFnYWluc3QgYQogICAgc3RhbGUgbnVtYmVyLiBSZXR1cm5zIChyZWFz"
    "b24sIHFpZCkgb3IgcmFpc2VzIG5vdGhpbmcuIiIiCiAgICB0cnk6CiAgICAgICAgYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgICAg"
    "ICBidyA9IFdaRklYX0JXX0ZBQ1RPUiAqIGludChzaXplKQogICAgICAgIGlmIGJ3IDw9IDA6CiAgICAgICAgICAgIHJldHVybiBO"
    "b25lLCBOb25lCiAgICAgICAgIyBkcm9wIGEgc3RhbGUgcHJlLWhvbGQgZm9yIHRoaXMgbWVzc2FnZSBmaXJzdCAoZWRpdGVkIGNv"
    "bW1hbmQpCiAgICAgICAgdHJ5OgogICAgICAgICAgICBhd2FpdCBfZGIoKS53emZpeF9wcmVob2xkW19wYXJ0KCldLmRlbGV0ZV9v"
    "bmUoeyJfaWQiOiBmInByZTp7bWlkfSJ9KQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIHBhc3MKICAgICAg"
    "ICByZWFzb24sIHFpZCA9IGF3YWl0IF90cnlfaG9sZCh1c2VyX2lkLCBidykKICAgICAgICBpZiByZWFzb246CiAgICAgICAgICAg"
    "IHJldHVybiByZWFzb24sIHFpZAogICAgICAgIHRyeToKICAgICAgICAgICAgYXdhaXQgX2RiKCkud3pmaXhfcHJlaG9sZFtfcGFy"
    "dCgpXS5pbnNlcnRfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBmInByZTp7bWlkfSIsICJxaWQiOiBxaWQsICJidyI6IGlu"
    "dChidyksCiAgICAgICAgICAgICAgICAgInVpZCI6IHVzZXJfaWQsICJ0cyI6IHRpbWUoKX0KICAgICAgICAgICAgKQogICAgICAg"
    "IGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggcHJlaG9sZCB3cml0ZSBmYWls"
    "ZWQ6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUsIHFpZAogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dF"
    "Ui5lcnJvcihmIldaRklYIHJlc2VydmVfcHJlIGZhaWxlZCAoYWxsb3dlZCk6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUsIE5v"
    "bmUKCgphc3luYyBkZWYgX2Fkb3B0X3ByZShsaXN0ZW5lcik6CiAgICAiIiJCcmluZyB0aGlzIHRhc2sncyBwcmUtZG93bmxvYWQg"
    "aG9sZCAoaWYgYW55KSBvbnRvIHRoZSBsaXN0ZW5lciBzbwogICAgdGhlIG5vcm1hbCByZWxlYXNlIHBhdGhzIGZyZWUgaXQuIiIi"
    "CiAgICB0cnk6CiAgICAgICAgZG9jID0gYXdhaXQgX2RiKCkud3pmaXhfcHJlaG9sZFtfcGFydCgpXS5maW5kX29uZV9hbmRfZGVs"
    "ZXRlKAogICAgICAgICAgICB7Il9pZCI6IGYicHJlOntnZXRhdHRyKGxpc3RlbmVyLCAnbWlkJywgMCl9In0KICAgICAgICApCiAg"
    "ICAgICAgaWYgbm90IGRvYzoKICAgICAgICAgICAgcmV0dXJuCiAgICAgICAgcWlkID0gZG9jLmdldCgicWlkIikKICAgICAgICBi"
    "dyA9IGludChkb2MuZ2V0KCJidyIpIG9yIDApCiAgICAgICAgaWYgbm90IHFpZCBvciBidyA8PSAwOgogICAgICAgICAgICByZXR1"
    "cm4KICAgICAgICBpZiBnZXRhdHRyKGxpc3RlbmVyLCAiX3d6Zml4X3Jlc3YiLCBOb25lKToKICAgICAgICAgICAgIyBhbHJlYWR5"
    "IGhvbGRzIChsaW1pdF9jaGVja2VyIHJlLWNoZWNrKTogcmVmdW5kIHRoZSBwcmUtaG9sZAogICAgICAgICAgICBhd2FpdCBfZGIo"
    "KS53emZpeF9xdW90YVtfcGFydCgpXS51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBxaWR9LCB7IiRpbmMiOiB7"
    "InJlc2VydmVkIjogLWJ3fX0KICAgICAgICAgICAgKQogICAgICAgICAgICByZXR1cm4KICAgICAgICBsaXN0ZW5lci5fd3pmaXhf"
    "cmVzdiA9IChxaWQsIGJ3KQogICAgICAgIExPR0dFUi5pbmZvKGYiV1pGSVggcHJlaG9sZCBhZG9wdGVkOiB7Ynd9IGJ5dGVzIGZv"
    "ciB7cWlkfSIpCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKCgphc3luYyBkZWYgcmVsZWFzZV9yZXNlcnZlKGxp"
    "c3RlbmVyKToKICAgICIiIkZyZWUgdGhpcyB0YXNrJ3MgcmVzZXJ2YXRpb24gKGNvbXBsZXRpb24sIGVycm9yIG9yIGNhbmNlbCku"
    "IiIiCiAgICBhd2FpdCBfYWRvcHRfcHJlKGxpc3RlbmVyKQogICAgcmVzdiA9IGdldGF0dHIobGlzdGVuZXIsICJfd3pmaXhfcmVz"
    "diIsIE5vbmUpCiAgICBpZiBub3QgcmVzdjoKICAgICAgICByZXR1cm4KICAgICMgY2xlYXIgc3luY2hyb25vdXNseSBGSVJTVCBz"
    "byBhIGR1cGxpY2F0ZSByZWxlYXNlIGNhbm5vdCBkb3VibGUtcmVmdW5kCiAgICBsaXN0ZW5lci5fd3pmaXhfcmVzdiA9IE5vbmUK"
    "ICAgIHRyeToKICAgICAgICBxaWQsIGJ3ID0gcmVzdgogICAgICAgIGF3YWl0IF9kYigpLnd6Zml4X3F1b3RhW19wYXJ0KCldLnVw"
    "ZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogcWlkfSwgeyIkaW5jIjogeyJyZXNlcnZlZCI6IC1id319CiAgICAgICAgKQog"
    "ICAgICAgIExPR0dFUi5pbmZvKGYiV1pGSVggcmVzZXJ2ZTogcmVsZWFzZWQge19uaWNlX3NpemUoYncpfSBmcm9tIHtxaWR9IikK"
    "ICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwoKCmRlZiBfaXNfZXhlbXB0KHVzZXJfaWQsIHVzZXJfZGljdD1Ob25l"
    "KToKICAgIGlmIHVzZXJfaWQgPT0gQ29uZmlnLk9XTkVSX0lEOgogICAgICAgIHJldHVybiBUcnVlCiAgICBpZiB1c2VyX2RpY3Qg"
    "YW5kIHVzZXJfZGljdC5nZXQoIlNVRE8iKToKICAgICAgICByZXR1cm4gVHJ1ZQogICAgIyBSU1MgdGFza3MgcnVuIHVuZGVyIHRo"
    "ZSBSU1MgY2hhdCBpZCDigJQgbmV2ZXIgcXVvdGEtYmxvY2sgdGhlIGZlZWRzCiAgICB0cnk6CiAgICAgICAgaWYgQ29uZmlnLlJT"
    "U19DSEFUIGFuZCB1c2VyX2lkID09IGludChDb25maWcuUlNTX0NIQVQpOgogICAgICAgICAgICByZXR1cm4gVHJ1ZQogICAgZXhj"
    "ZXB0IChUeXBlRXJyb3IsIFZhbHVlRXJyb3IpOgogICAgICAgIHBhc3MKICAgIHJldHVybiBGYWxzZQoKCmFzeW5jIGRlZiBxdW90"
    "YV9jaGVjayhsaXN0ZW5lcik6CiAgICAiIiJTaXplLWF3YXJlIGNoZWNrIGNhbGxlZCBmcm9tIGxpbWl0X2NoZWNrZXIgKHNpemUg"
    "aXMga25vd24gdGhlcmUpLiIiIgogICAgdHJ5OgogICAgICAgIGlmIG5vdCBhd2FpdCBlbnN1cmVfcmVhZHkoKToKICAgICAgICAg"
    "ICAgTE9HR0VSLndhcm5pbmcoIldaRklYIHF1b3RhOiBkYiBub3QgcmVhZHkgLT4gYWxsb3dpbmcgdGFzayIpCiAgICAgICAgICAg"
    "IHJldHVybiBOb25lCiAgICAgICAgaWYgX2lzX2V4ZW1wdChsaXN0ZW5lci51c2VyX2lkLCBsaXN0ZW5lci51c2VyX2RpY3QpOgog"
    "ICAgICAgICAgICBMT0dHRVIuaW5mbyhmIldaRklYIHF1b3RhOiB1c2VyIHtsaXN0ZW5lci51c2VyX2lkfSBleGVtcHQgKG93bmVy"
    "L3N1ZG8vcnNzKSIpCiAgICAgICAgICAgIHJldHVybiBOb25lCiAgICAgICAgdXNlcl9pZCA9IGxpc3RlbmVyLnVzZXJfaWQKICAg"
    "ICAgICBjYXAgPSBhd2FpdCBnZXRfY2FwX2J5dGVzKHVzZXJfaWQpCiAgICAgICAgdXNlZCA9IGF3YWl0IGdldF91c2FnZSh1c2Vy"
    "X2lkKQogICAgICAgICMgYWRvcHQgdGhlIHByZS1kb3dubG9hZCBob2xkIHRha2VuIGluIHByZV90YXNrX2NoZWNrICh2MTUuOSk6"
    "CiAgICAgICAgIyB0aGUgYmFuZHdpZHRoIHdhcyBhbHJlYWR5IHJlc2VydmVkIGJlZm9yZSB0aGUgdGFzayBzdGFydGVkCiAgICAg"
    "ICAgYXdhaXQgX2Fkb3B0X3ByZShsaXN0ZW5lcikKICAgICAgICBpZiBnZXRhdHRyKGxpc3RlbmVyLCAiX3d6Zml4X3Jlc3YiLCBO"
    "b25lKToKICAgICAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAgICAgICAgICBmIldaRklYIHF1b3RhOiB1c2VyPXt1c2VyX2lk"
    "fSAtPiBhbGxvdyAocHJlLWhlbGQge2xpc3RlbmVyLnNpemUgb3IgMH0pIgogICAgICAgICAgICApCiAgICAgICAgICAgIHJldHVy"
    "biBOb25lCiAgICAgICAgIyByZS1ldmFsdWF0aW9uIChlLmcuIHFiaXQgbWV0YWRhdGEgdXBkYXRlcyk6IGRyb3Agb3VyIG93biBv"
    "bGQgaG9sZAogICAgICAgICMgZmlyc3Qgc28gdGhpcyB0YXNrIGlzIG5vdCBjb3VudGVkIGFnYWluc3QgaXRzZWxmIHR3aWNlCiAg"
    "ICAgICAgaWYgZ2V0YXR0cihsaXN0ZW5lciwgIl93emZpeF9yZXN2IiwgTm9uZSk6CiAgICAgICAgICAgIGF3YWl0IHJlbGVhc2Vf"
    "cmVzZXJ2ZShsaXN0ZW5lcikKICAgICAgICBzaXplID0gV1pGSVhfQldfRkFDVE9SICogaW50KGxpc3RlbmVyLnNpemUgb3IgMCkK"
    "ICAgICAgICAjIEFUT01JQzogdGhlIGhvbGQgYW5kIHRoZSB2ZXJkaWN0IGhhcHBlbiBpbiBvbmUgc2VydmVyLXNpZGUKICAgICAg"
    "ICAjIG9wZXJhdGlvbiDigJQgc2ltdWx0YW5lb3VzIHRhc2tzIHNlcmlhbGl6ZSwgZXhhY3RseSBvbmUgY2FuIHdpbgogICAgICAg"
    "IHJlYXNvbiA9IGF3YWl0IHJlc2VydmUobGlzdGVuZXIsIHNpemUsIGNhcCkKICAgICAgICBpZiByZWFzb246CiAgICAgICAgICAg"
    "IHJlc2VydmVkID0gYXdhaXQgcmVzZXJ2ZWRfdG9kYXkodXNlcl9pZCkKICAgICAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAg"
    "ICAgICAgICBmIldaRklYIHF1b3RhOiB1c2VyPXt1c2VyX2lkfSBzaXplPXtzaXplfSB1c2VkPXt1c2VkfSAiCiAgICAgICAgICAg"
    "ICAgICBmInJlc2VydmVkPXtyZXNlcnZlZH0gY2FwPXtjYXB9IC0+IEJMT0NLICh7cmVhc29ufSkiCiAgICAgICAgICAgICkKICAg"
    "ICAgICAgICAgcmV0dXJuIGF3YWl0IF9xdW90YV9ibG9ja19tc2coCiAgICAgICAgICAgICAgICB1c2VkLCBjYXAsIHNpemUsIGF0"
    "X2xpbWl0PShyZWFzb24gPT0gImF0X2xpbWl0IiksIHJlc2VydmVkPXJlc2VydmVkCiAgICAgICAgICAgICkKICAgICAgICBMT0dH"
    "RVIuaW5mbygKICAgICAgICAgICAgZiJXWkZJWCBxdW90YTogdXNlcj17dXNlcl9pZH0gc2l6ZT17c2l6ZX0gdXNlZD17dXNlZH0g"
    "IgogICAgICAgICAgICBmImNhcD17Y2FwfSAtPiBhbGxvdyAoaG9sZGluZyB7c2l6ZX0pIgogICAgICAgICkKICAgICAgICByZXR1"
    "cm4gTm9uZQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIHF1b3RhX2NoZWNr"
    "IGZhaWxlZCAoYWxsb3dlZCk6IHtlfSIpCiAgICAgICAgcmV0dXJuIE5vbmUKCgpfVVJMX1JFID0gTm9uZQoKCmRlZiBfZXh0cmFj"
    "dF9saW5rcyh0ZXh0KToKICAgICIiIkFsbCBodHRwKHMpIGxpbmtzIGluIGEgbWVzc2FnZSB0ZXh0IChubyBtYWduZXRzLCAudG9y"
    "cmVudCBmaWxlcykuIiIiCiAgICBnbG9iYWwgX1VSTF9SRQogICAgaWYgX1VSTF9SRSBpcyBOb25lOgogICAgICAgIGZyb20gcmUg"
    "aW1wb3J0IGNvbXBpbGUgYXMgX2MKICAgICAgICBfVVJMX1JFID0gX2MociJodHRwcz86Ly9bXlxzfF0rIikKICAgIHJldHVybiBb"
    "CiAgICAgICAgdSBmb3IgdSBpbiBfVVJMX1JFLmZpbmRhbGwodGV4dCBvciAiIikKICAgICAgICBpZiBub3QgdS5sb3dlcigpLmVu"
    "ZHN3aXRoKCIudG9ycmVudCIpCiAgICBdCgoKYXN5bmMgZGVmIF9wcm9iZV9zaXplX2FyaWEyKGxpbmspOgogICAgIiIiVW5pdmVy"
    "c2FsIHNpemUgcHJvYmU6IGxldCBhcmlhMiBJVFNFTEYgY29ubmVjdCB0byB0aGUgbGluaywgcmVhZAogICAgdGhlIHJlYWwgdG90"
    "YWwgbGVuZ3RoLCB0aGVuIHRocm93IHRoZSBwcm9iZSBhd2F5LiBTYW1lIGVuZ2luZSwgc2FtZQogICAgcmVkaXJlY3QgaGFuZGxp"
    "bmcgdGhlIHJlYWwgZG93bmxvYWQgd291bGQgdXNlIOKAlCB3b3JrcyBmb3IgZHluYW1pYwogICAgcGFnZXMgYW5kIG9kZCBzZXJ2"
    "ZXJzIHdoZXJlIEhFQUQvcmFuZ2VkLUdFVCBzZWUgbm90aGluZy4iIiIKICAgIGdpZCA9IE5vbmUKICAgIGxhc3QgPSBOb25lCiAg"
    "ICB0cnk6CiAgICAgICAgaW1wb3J0IHRlbXBmaWxlIGFzIF90ZgoKICAgICAgICBmcm9tIC4uLmNvcmUudG9ycmVudF9tYW5hZ2Vy"
    "IGltcG9ydCBUb3JyZW50TWFuYWdlcgoKICAgICAgICBfZGlyID0gX3RmLm1rZHRlbXAocHJlZml4PSJ3emZpeHByb2JlXyIpCiAg"
    "ICAgICAgZ2lkID0gYXdhaXQgVG9ycmVudE1hbmFnZXIuYXJpYTIuYWRkVXJpKAogICAgICAgICAgICB1cmlzPVtsaW5rXSwKICAg"
    "ICAgICAgICAgb3B0aW9ucz17CiAgICAgICAgICAgICAgICAiZGlyIjogX2RpciwKICAgICAgICAgICAgICAgICJhbGxvdy1vdmVy"
    "d3JpdGUiOiAidHJ1ZSIsCiAgICAgICAgICAgICAgICAiYXV0by1maWxlLXJlbmFtaW5nIjogImZhbHNlIiwKICAgICAgICAgICAg"
    "ICAgICJjb250aW51ZSI6ICJmYWxzZSIsCiAgICAgICAgICAgICAgICAibWF4LWRvd25sb2FkLWxpbWl0IjogIjI1NksiLAogICAg"
    "ICAgICAgICB9LAogICAgICAgICAgICBwb3NpdGlvbj0wLAogICAgICAgICkKICAgICAgICBmcm9tIGFzeW5jaW8gaW1wb3J0IHNs"
    "ZWVwIGFzIF9hc2wKCiAgICAgICAgZm9yIF8gaW4gcmFuZ2UoMTgpOiAgIyB1cCB0byB+OXMKICAgICAgICAgICAgYXdhaXQgX2Fz"
    "bCgwLjUpCiAgICAgICAgICAgIGxhc3QgPSBhd2FpdCBUb3JyZW50TWFuYWdlci5hcmlhMi50ZWxsU3RhdHVzKGdpZCkKICAgICAg"
    "ICAgICAgdG90YWwgPSBpbnQobGFzdC5nZXQoInRvdGFsTGVuZ3RoIikgb3IgMCkKICAgICAgICAgICAgaWYgdG90YWwgPiAwOgog"
    "ICAgICAgICAgICAgICAgcmV0dXJuIHRvdGFsCiAgICAgICAgICAgIGlmIGxhc3QuZ2V0KCJzdGF0dXMiKSBpbiAoImVycm9yIiwg"
    "ImNvbXBsZXRlIikgb3IgbGFzdC5nZXQoImVycm9yQ29kZSIpOgogICAgICAgICAgICAgICAgcmV0dXJuIDAKICAgICAgICByZXR1"
    "cm4gMAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gMAogICAgZmluYWxseToKICAgICAgICBpZiBnaWQ6CiAg"
    "ICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGZyb20gLi4uY29yZS50b3JyZW50X21hbmFnZXIgaW1wb3J0IFRvcnJlbnRN"
    "YW5hZ2VyCgogICAgICAgICAgICAgICAgaWYgbGFzdCBpcyBub3QgTm9uZToKICAgICAgICAgICAgICAgICAgICBhd2FpdCBUb3Jy"
    "ZW50TWFuYWdlci5hcmlhMl9yZW1vdmUobGFzdCkKICAgICAgICAgICAgICAgIGVsc2U6CiAgICAgICAgICAgICAgICAgICAgYXdh"
    "aXQgVG9ycmVudE1hbmFnZXIuYXJpYTIuZm9yY2VSZW1vdmUoZ2lkKQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAg"
    "ICAgICAgICAgICAgcGFzcwoKCmFzeW5jIGRlZiBfcHJvYmVfc2l6ZShsaW5rLCBoZWFkZXI9Tm9uZSk6CiAgICAiIiJMZWFybiBh"
    "IGRvd25sb2FkJ3Mgc2l6ZSBiZWZvcmUgaXQgc3RhcnRzICgwID0gdW5rbm93bikuCgogICAgYXJjaGl2ZS5vcmcgcGFnZSBsaW5r"
    "cyAoL2NvbXByZXNzLywgL2RldGFpbHMvLCAvZG93bmxvYWQvKSBuZXZlcgogICAgYW5zd2VyIEhFQUQgd2l0aCB0aGUgcmVhbCBz"
    "aXplIOKAlCBhc2sgdGhlaXIgbWV0YWRhdGEgQVBJIGluc3RlYWQuCiAgICAiIiIKICAgIHRyeToKICAgICAgICBmcm9tIHVybGxp"
    "Yi5wYXJzZSBpbXBvcnQgdXJscGFyc2UKCiAgICAgICAgX2hvc3QgPSAodXJscGFyc2UobGluaykuaG9zdG5hbWUgb3IgIiIpLmxv"
    "d2VyKCkKICAgICAgICBpZiBfaG9zdC5lbmRzd2l0aCgiYXJjaGl2ZS5vcmciKToKICAgICAgICAgICAgX3BhcnRzID0gW3AgZm9y"
    "IHAgaW4gKHVybHBhcnNlKGxpbmspLnBhdGggb3IgIiIpLnNwbGl0KCIvIikgaWYgcF0KICAgICAgICAgICAgX2lkZW50LCBfYWZ0"
    "ZXIsIF9maWxlID0gTm9uZSwgTm9uZSwgTm9uZQogICAgICAgICAgICBmb3IgX2ksIF9wIGluIGVudW1lcmF0ZShfcGFydHMpOgog"
    "ICAgICAgICAgICAgICAgaWYgX3AgaW4gKCJjb21wcmVzcyIsICJkZXRhaWxzIiwgImRvd25sb2FkIiwgInN0cmVhbSIpOgogICAg"
    "ICAgICAgICAgICAgICAgIGlmIF9pICsgMSA8IGxlbihfcGFydHMpOgogICAgICAgICAgICAgICAgICAgICAgICBfaWRlbnQgPSBf"
    "cGFydHNbX2kgKyAxXQogICAgICAgICAgICAgICAgICAgICAgICBpZiBfaSArIDIgPCBsZW4oX3BhcnRzKToKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgIF9maWxlID0gIi8iLmpvaW4oX3BhcnRzW19pICsgMjpdKQogICAgICAgICAgICAgICAgICAgIGJyZWFr"
    "CiAgICAgICAgICAgIGlmIF9pZGVudDoKICAgICAgICAgICAgICAgIGZyb20gYWlvaHR0cCBpbXBvcnQgQ2xpZW50U2Vzc2lvbiwg"
    "Q2xpZW50VGltZW91dAoKICAgICAgICAgICAgICAgIGFzeW5jIHdpdGggQ2xpZW50U2Vzc2lvbigKICAgICAgICAgICAgICAgICAg"
    "ICB0aW1lb3V0PUNsaWVudFRpbWVvdXQodG90YWw9OCksCiAgICAgICAgICAgICAgICAgICAgaGVhZGVycz17IlVzZXItQWdlbnQi"
    "OiAiV1pNTC1YLzE1LjExIn0sCiAgICAgICAgICAgICAgICApIGFzIF9zOgogICAgICAgICAgICAgICAgICAgIGFzeW5jIHdpdGgg"
    "X3MuZ2V0KAogICAgICAgICAgICAgICAgICAgICAgICBmImh0dHBzOi8vYXJjaGl2ZS5vcmcvbWV0YWRhdGEve19pZGVudH0iCiAg"
    "ICAgICAgICAgICAgICAgICAgKSBhcyBfcjoKICAgICAgICAgICAgICAgICAgICAgICAgaWYgX3Iuc3RhdHVzIDwgNDAwOgogICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgX2pzID0gYXdhaXQgX3IuanNvbihjb250ZW50X3R5cGU9Tm9uZSkKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgIF9maWxlcyA9IChfanMgb3Ige30pLmdldCgiZmlsZXMiKSBvciBbXQogICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgaWYgX2ZpbGU6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZm9yIF9mIGluIF9maWxlczoKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgX2YuZ2V0KCJuYW1lIikgPT0gX2ZpbGU6CiAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX3N6"
    "ID0gaW50KF9mLmdldCgic2l6ZSIpIG9yIDApCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBleGNlcHQg"
    "KFR5cGVFcnJvciwgVmFsdWVFcnJvcik6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX3N6ID0g"
    "MAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgX3N6ID4gMDoKICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gX3N6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfdG90YWwgPSAwCiAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICBmb3IgX2YgaW4gX2ZpbGVzOgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg"
    "IHRyeToKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX3RvdGFsICs9IGludChfZi5nZXQoInNpemUiKSBvciAw"
    "KQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4Y2VwdCAoVHlwZUVycm9yLCBWYWx1ZUVycm9yKToKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgcGFzcwogICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgX3RvdGFsID4gMDoK"
    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gX3RvdGFsCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAg"
    "IHBhc3MKICAgIHRyeToKICAgICAgICBmcm9tIGFpb2h0dHAgaW1wb3J0IENsaWVudFNlc3Npb24sIENsaWVudFRpbWVvdXQKCiAg"
    "ICAgICAgYXN5bmMgd2l0aCBDbGllbnRTZXNzaW9uKAogICAgICAgICAgICB0aW1lb3V0PUNsaWVudFRpbWVvdXQodG90YWw9OCks"
    "CiAgICAgICAgICAgIGhlYWRlcnM9eyJVc2VyLUFnZW50IjogIldaTUwtWC8xNS45In0sCiAgICAgICAgKSBhcyBzOgogICAgICAg"
    "ICAgICBhc3luYyB3aXRoIHMuaGVhZChsaW5rLCBhbGxvd19yZWRpcmVjdHM9VHJ1ZSkgYXMgcjoKICAgICAgICAgICAgICAgIGlm"
    "IHIuc3RhdHVzIDwgNDAwOgogICAgICAgICAgICAgICAgICAgIF9jdCA9IChyLmhlYWRlcnMuZ2V0KCJDb250ZW50LVR5cGUiKSBv"
    "ciAiIikubG93ZXIoKQogICAgICAgICAgICAgICAgICAgIGlmICJ0ZXh0L2h0bWwiIGluIF9jdCBvciAieGh0bWwiIGluIF9jdDoK"
    "ICAgICAgICAgICAgICAgICAgICAgICAgcGFzcyAgIyBhIHdlYiBQQUdFLCBub3QgdGhlIGZpbGUg4oCUIG5ldmVyIHRydXN0IGl0"
    "CiAgICAgICAgICAgICAgICAgICAgZWxzZToKICAgICAgICAgICAgICAgICAgICAgICAgY2wgPSByLmhlYWRlcnMuZ2V0KCJDb250"
    "ZW50LUxlbmd0aCIpCiAgICAgICAgICAgICAgICAgICAgICAgIGlmIGNsIGFuZCBjbC5pc2RpZ2l0KCk6CiAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICByZXR1cm4gaW50KGNsKQogICAgICAgICAgICAjIHNvbWUgc2VydmVycyByZWplY3QgSEVBRCDigJQgYSAx"
    "LWJ5dGUgcmFuZ2VkIEdFVCBzdGlsbCByZXBvcnRzCiAgICAgICAgICAgICMgdGhlIHRvdGFsIHNpemUgaW4gQ29udGVudC1SYW5n"
    "ZQogICAgICAgICAgICBhc3luYyB3aXRoIHMuZ2V0KAogICAgICAgICAgICAgICAgbGluaywgYWxsb3dfcmVkaXJlY3RzPVRydWUs"
    "IGhlYWRlcnM9eyJSYW5nZSI6ICJieXRlcz0wLTAifQogICAgICAgICAgICApIGFzIHI6CiAgICAgICAgICAgICAgICBfY3QyID0g"
    "KHIuaGVhZGVycy5nZXQoIkNvbnRlbnQtVHlwZSIpIG9yICIiKS5sb3dlcigpCiAgICAgICAgICAgICAgICBpZiAidGV4dC9odG1s"
    "IiBpbiBfY3QyIG9yICJ4aHRtbCIgaW4gX2N0MjoKICAgICAgICAgICAgICAgICAgICBwYXNzICAjIHBhZ2UsIG5vdCBmaWxlIOKA"
    "lCBwcm9iZSBkZWVwZXIKICAgICAgICAgICAgICAgIGVsc2U6CiAgICAgICAgICAgICAgICAgICAgY3IgPSByLmhlYWRlcnMuZ2V0"
    "KCJDb250ZW50LVJhbmdlIiwgIiIpCiAgICAgICAgICAgICAgICAgICAgaWYgY3IgYW5kICIvIiBpbiBjcjoKICAgICAgICAgICAg"
    "ICAgICAgICAgICAgdG90YWwgPSBjci5yc3BsaXQoIi8iLCAxKVsxXQogICAgICAgICAgICAgICAgICAgICAgICBpZiB0b3RhbC5p"
    "c2RpZ2l0KCk6CiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gaW50KHRvdGFsKQogICAgZXhjZXB0IEV4Y2VwdGlv"
    "bjoKICAgICAgICBwYXNzCiAgICAjIGxhc3QgcmVzb3J0OiBhc2sgYXJpYTIgaXRzZWxmICh1bml2ZXJzYWwgY2hlY2tlcikKICAg"
    "IHJldHVybiBhd2FpdCBfcHJvYmVfc2l6ZV9hcmlhMihsaW5rKQoKCmFzeW5jIGRlZiBwcmVjaGVjayhtZXNzYWdlKToKICAgICIi"
    "IlByZS1kb3dubG9hZCBhbGxvd2FuY2UgY2hlY2sgKHYxNS45KSwgY2FsbGVkIGZyb20gcHJlX3Rhc2tfY2hlY2sKICAgIEJFRk9S"
    "RSBhbnkgZG93bmxvYWRlciBzdGFydHMuCgogICAgRm9yIHBsYWluIHNpbmdsZSBodHRwKHMpIGxpbmtzIHRoZSBmaWxlIHNpemUg"
    "aXMgcHJvYmVkIHdpdGggYSBIRUFECiAgICByZXF1ZXN0OyBpZiB0aGUgYWxsb3dhbmNlIGNhbm5vdCBjb3ZlciBpdCB0aGUgdGFz"
    "ayBpcyBzdG9wcGVkIHJpZ2h0CiAgICBoZXJlIHdpdGggYSBjbGVhciBtZXNzYWdlLCBhbmQgdGhlIGJhbmR3aWR0aCBpcyBoZWxk"
    "IGF0b21pY2FsbHkgdGhlCiAgICBtb21lbnQgaXQgcGFzc2VzLCBzbyBzaW11bHRhbmVvdXMgY29tbWFuZHMgY2FuIG5ldmVyIGVh"
    "Y2ggc2xpcCBwYXN0CiAgICB0aGUgc2FtZSBzdGFsZSBudW1iZXIuIFJldHVybnMgYSBibG9jayBtZXNzYWdlLCBvciBOb25lIHRv"
    "IHByb2NlZWQuCiAgICAiIiIKICAgIHRyeToKICAgICAgICBmcm9tX3VzZXIgPSBnZXRhdHRyKG1lc3NhZ2UsICJmcm9tX3VzZXIi"
    "LCBOb25lKQogICAgICAgIHNlbmRlcl9jaGF0ID0gZ2V0YXR0cihtZXNzYWdlLCAic2VuZGVyX2NoYXQiLCBOb25lKQogICAgICAg"
    "IHdobyA9IGZyb21fdXNlciBvciBzZW5kZXJfY2hhdAogICAgICAgIHVzZXJfaWQgPSB3aG8uaWQgaWYgd2hvIGVsc2UgMAogICAg"
    "ICAgIHVzZXJfZGljdCA9IF9nZXRfdXNlcl9kaWN0KHVzZXJfaWQpCiAgICAgICAgaWYgX2lzX2V4ZW1wdCh1c2VyX2lkLCB1c2Vy"
    "X2RpY3QpOgogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIGlmIG5vdCBhd2FpdCBlbnN1cmVfcmVhZHkoKToKICAgICAg"
    "ICAgICAgcmV0dXJuIE5vbmUKICAgICAgICB0ZXh0ID0gZ2V0YXR0cihtZXNzYWdlLCAidGV4dCIsICIiKSBvciAiIgogICAgICAg"
    "IGxpbmtzID0gX2V4dHJhY3RfbGlua3ModGV4dCkKICAgICAgICAjIG9ubHkgc2luZ2xlIHBsYWluIGxpbmtzIGNhbiBiZSBwcmUt"
    "Y2hlY2tlZCByZWxpYWJseTsgYnVsayBhbmQKICAgICAgICAjIG5vbi1odHRwIHNvdXJjZXMgZmFsbCBiYWNrIHRvIHRoZSBzaXpl"
    "IGNoZWNrIGluc2lkZSB0aGUgZG93bmxvYWQKICAgICAgICBpZiBsZW4obGlua3MpICE9IDE6CiAgICAgICAgICAgIHJldHVybiBO"
    "b25lCgogICAgICAgIGZyb20gLi4uaGVscGVyLnRlbGVncmFtX2hlbHBlci5tZXNzYWdlX3V0aWxzIGltcG9ydCAoCiAgICAgICAg"
    "ICAgIHNlbmRfbWVzc2FnZSBhcyBfc2VuZCwKICAgICAgICAgICAgZWRpdF9tZXNzYWdlIGFzIF9lZGl0LAogICAgICAgICkKCiAg"
    "ICAgICAgbWlkID0gbWVzc2FnZS5pZAogICAgICAgIGNoZWNraW5nID0gTm9uZQogICAgICAgIHRyeToKICAgICAgICAgICAgY2hl"
    "Y2tpbmcgPSBhd2FpdCBfc2VuZCgKICAgICAgICAgICAgICAgIG1lc3NhZ2UsICLwn5SNIDxiPkNoZWNraW5nIGJhbmR3aWR0aCBh"
    "bGxvd2FuY2XigKY8L2I+IgogICAgICAgICAgICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgY2hlY2tp"
    "bmcgPSBOb25lCgogICAgICAgIHNpemUgPSBhd2FpdCBfcHJvYmVfc2l6ZShsaW5rc1swXSkKICAgICAgICBpZiBzaXplIDw9IDA6"
    "CiAgICAgICAgICAgIGlmIGNoZWNraW5nIGlzIG5vdCBOb25lOgogICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAg"
    "ICAgIGZyb20gLi4uaGVscGVyLnRlbGVncmFtX2hlbHBlci5tZXNzYWdlX3V0aWxzIGltcG9ydCAoCiAgICAgICAgICAgICAgICAg"
    "ICAgICAgIGRlbGV0ZV9tZXNzYWdlIGFzIF9kZWwsCiAgICAgICAgICAgICAgICAgICAgKQoKICAgICAgICAgICAgICAgICAgICBh"
    "d2FpdCBfZGVsKGNoZWNraW5nKQogICAgICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgICAgICBw"
    "YXNzCiAgICAgICAgICAgIHJldHVybiBOb25lICAjIHNpemUgdW5rbm93biAtPiBjaGVja2VkIGR1cmluZyB0aGUgZG93bmxvYWQK"
    "CiAgICAgICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1c2VyX2lkKQogICAgICAgIHVzZWQgPSBhd2FpdCBnZXRfdXNhZ2Uo"
    "dXNlcl9pZCkKICAgICAgICByZXNlcnZlZCA9IGF3YWl0IHJlc2VydmVkX3RvZGF5KHVzZXJfaWQpCiAgICAgICAgYncgPSBXWkZJ"
    "WF9CV19GQUNUT1IgKiBzaXplCiAgICAgICAgcmVhc29uLCBfID0gYXdhaXQgcmVzZXJ2ZV9wcmUobWlkLCB1c2VyX2lkLCBzaXpl"
    "KQoKICAgICAgICBpZiByZWFzb24gaXMgTm9uZToKICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgbWVzc2FnZS5fd3pm"
    "aXhfaGVsZCA9IFRydWUKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAg"
    "ICAgaWYgY2hlY2tpbmcgaXMgbm90IE5vbmU6CiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAgICAgZnJvbSBh"
    "c3luY2lvIGltcG9ydCBzbGVlcCBhcyBfc2wKCiAgICAgICAgICAgICAgICAgICAgYXdhaXQgX2VkaXQoCiAgICAgICAgICAgICAg"
    "ICAgICAgICAgIGNoZWNraW5nLAogICAgICAgICAgICAgICAgICAgICAgICBmIuKchSA8Yj5DaGVja2VkIOKAlCBhbGxvd2FuY2Ug"
    "T0suPC9iPiAiCiAgICAgICAgICAgICAgICAgICAgICAgIGYiRG93bmxvYWQgd2lsbCBzdGFydCBzaG9ydGx5ICIKICAgICAgICAg"
    "ICAgICAgICAgICAgICAgZiIofntfbmljZV9zaXplKGJ3KX0gaW5jbC4gdXBsb2FkKSIsCiAgICAgICAgICAgICAgICAgICAgKQog"
    "ICAgICAgICAgICAgICAgICAgIF9kZWxfYWZ0ZXIoY2hlY2tpbmcpCiAgICAgICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgog"
    "ICAgICAgICAgICAgICAgICAgIHBhc3MKICAgICAgICAgICAgcmV0dXJuIE5vbmUKCiAgICAgICAgaWYgY2hlY2tpbmcgaXMgbm90"
    "IE5vbmU6CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGZyb20gLi4uaGVscGVyLnRlbGVncmFtX2hlbHBlci5tZXNz"
    "YWdlX3V0aWxzIGltcG9ydCAoCiAgICAgICAgICAgICAgICAgICAgZGVsZXRlX21lc3NhZ2UgYXMgX2RlbCwKICAgICAgICAgICAg"
    "ICAgICkKCiAgICAgICAgICAgICAgICBhd2FpdCBfZGVsKGNoZWNraW5nKQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgog"
    "ICAgICAgICAgICAgICAgcGFzcwogICAgICAgIGxlZnQgPSBtYXgoY2FwIC0gdXNlZCAtIHJlc2VydmVkLCAwKQogICAgICAgIG93"
    "bmVyID0gYXdhaXQgZ2V0X293bmVyX3VzZXJuYW1lKCkKICAgICAgICBpZiBsZWZ0IDw9IDA6CiAgICAgICAgICAgIHJldHVybiBh"
    "d2FpdCBfcXVvdGFfYmxvY2tfbXNnKAogICAgICAgICAgICAgICAgdXNlZCwgY2FwLCAwLCBhdF9saW1pdD1UcnVlLCByZXNlcnZl"
    "ZD1yZXNlcnZlZAogICAgICAgICAgICApCiAgICAgICAgd2hvID0gZiJAe293bmVyfSIgaWYgb3duZXIgZWxzZSAidGhlIG93bmVy"
    "IgogICAgICAgIGlmIHVzZWQgKyByZXNlcnZlZCA8PSAwOgogICAgICAgICAgICAjIHRoZSB3aG9sZSBkYWlseSBjYXAgY2Fubm90"
    "IGNvdmVyIHRoaXMgZmlsZQogICAgICAgICAgICByZXR1cm4gKAogICAgICAgICAgICAgICAgIvCfmqsgPGI+RmlsZSB0b28gYmln"
    "IGZvciB5b3VyIGJhbmR3aWR0aCBjYXA8L2I+XG7ilIJcbiIKICAgICAgICAgICAgICAgIGYi4pSgIDxiPkZpbGU8L2I+IOKGkiB7"
    "X25pY2Vfc2l6ZShzaXplKX0gIgogICAgICAgICAgICAgICAgZiI8aT4obmVlZHMgfntfbmljZV9zaXplKGJ3KX0gd2l0aCB1cGxv"
    "YWQpPC9pPlxuIgogICAgICAgICAgICAgICAgZiLilKAgPGI+WW91ciBkYWlseSBjYXA8L2I+IOKGkiB7X25pY2Vfc2l6ZShjYXAp"
    "fVxuIgogICAgICAgICAgICAgICAgZiLilJYgQnV5IG1vcmUgYmFuZHdpZHRoIGZyb20gdGhlIG93bmVyIDxiPnt3aG99PC9iPiIK"
    "ICAgICAgICAgICAgKQogICAgICAgIHJldHVybiAoCiAgICAgICAgICAgICLwn5qrIDxiPk5vdCBlbm91Z2ggYmFuZHdpZHRoPC9i"
    "Plxu4pSCXG4iCiAgICAgICAgICAgIGYi4pSgIDxiPkZpbGU8L2I+IOKGkiB7X25pY2Vfc2l6ZShzaXplKX0gIgogICAgICAgICAg"
    "ICBmIjxpPihuZWVkcyB+e19uaWNlX3NpemUoYncpfSB3aXRoIHVwbG9hZCk8L2k+XG4iCiAgICAgICAgICAgIGYi4pSgIDxiPkFs"
    "bG93YW5jZSBsZWZ0IHRvZGF5PC9iPiDihpIge19uaWNlX3NpemUobGVmdCl9XG4iCiAgICAgICAgICAgIGYi4pSWIFRyeSBhZ2Fp"
    "biB0b21vcnJvdyAocmVzZXRzIDAwOjAwIElTVCkgb3IgYXNrICIKICAgICAgICAgICAgZiI8Yj57d2hvfTwvYj4gZm9yIG1vcmUg"
    "YmFuZHdpZHRoIgogICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICBMT0dHRVIuZXJyb3IoZiJXWkZJ"
    "WCBwcmVjaGVjayBmYWlsZWQgKGFsbG93ZWQpOiB7ZX0iKQogICAgICAgIHJldHVybiBOb25lCgoKZGVmIF9nZXRfdXNlcl9kaWN0"
    "KHVzZXJfaWQpOgogICAgdHJ5OgogICAgICAgIGZyb20gLi4uIGltcG9ydCB1c2VyX2RhdGEKCiAgICAgICAgcmV0dXJuIHVzZXJf"
    "ZGF0YS5nZXQodXNlcl9pZCwge30pCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiB7fQoKCmFzeW5jIGRlZiBf"
    "ZGVsX21zZ19sYXRlcihtKToKICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgc2xlZXAgYXMgX3NsCgogICAgYXdhaXQgX3NsKDYpCiAg"
    "ICB0cnk6CiAgICAgICAgZnJvbSAuLi5oZWxwZXIudGVsZWdyYW1faGVscGVyLm1lc3NhZ2VfdXRpbHMgaW1wb3J0IGRlbGV0ZV9t"
    "ZXNzYWdlIGFzIF9kZWwKCiAgICAgICAgYXdhaXQgX2RlbChtKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoK"
    "ZGVmIF9kZWxfYWZ0ZXIobSk6CiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi4gaW1wb3J0IGJvdF9sb29wCgogICAgICAgIGJvdF9s"
    "b29wLmNyZWF0ZV90YXNrKF9kZWxfbXNnX2xhdGVyKG0pKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoKYXN5"
    "bmMgZGVmIG92ZXJfbGltaXRfbXNnKHVzZXJfaWQsIHVzZXJfZGljdD1Ob25lKToKICAgICIiIkJsdW50IHByZS10YXNrIGNoZWNr"
    "IGNhbGxlZCBmcm9tIHByZV90YXNrX2NoZWNrIChubyBzaXplIGtub3duIHlldCkuIiIiCiAgICB0cnk6CiAgICAgICAgaWYgX2lz"
    "X2V4ZW1wdCh1c2VyX2lkLCB1c2VyX2RpY3QpOgogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIGlmIG5vdCBhd2FpdCBl"
    "bnN1cmVfcmVhZHkoKToKICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoIldaRklYIHByZS1jaGVjazogZGIgbm90IHJlYWR5IC0+"
    "IGFsbG93aW5nIHRhc2siKQogICAgICAgICAgICByZXR1cm4gTm9uZQogICAgICAgIGNhcCA9IGF3YWl0IGdldF9jYXBfYnl0ZXMo"
    "dXNlcl9pZCkKICAgICAgICB1c2VkID0gYXdhaXQgZ2V0X3VzYWdlKHVzZXJfaWQpCiAgICAgICAgcmVzZXJ2ZWQgPSBhd2FpdCBy"
    "ZXNlcnZlZF90b2RheSh1c2VyX2lkKQogICAgICAgIGlmIHVzZWQgKyByZXNlcnZlZCA+PSBjYXA6CiAgICAgICAgICAgIExPR0dF"
    "Ui5pbmZvKAogICAgICAgICAgICAgICAgZiJXWkZJWCBwcmUtY2hlY2s6IHVzZXI9e3VzZXJfaWR9IHVzZWQ9e3VzZWR9ICIKICAg"
    "ICAgICAgICAgICAgIGYicmVzZXJ2ZWQ9e3Jlc2VydmVkfSBjYXA9e2NhcH0gLT4gQkxPQ0siCiAgICAgICAgICAgICkKICAgICAg"
    "ICAgICAgcmV0dXJuIGF3YWl0IF9xdW90YV9ibG9ja19tc2codXNlZCwgY2FwLCAwLCBhdF9saW1pdD1UcnVlLCByZXNlcnZlZD1y"
    "ZXNlcnZlZCkKICAgICAgICByZXR1cm4gTm9uZQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gTm9uZQoKCiMg"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACiMgUmVj"
    "b3JkaW5nIChjaGFyZ2UgKyBsaWJyYXJ5KQojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgAoKCmFzeW5jIGRlZiBjaGFyZ2UobGlzdGVuZXIpOgogICAgIiIiQ2hhcmdlIHRoZSBmaW5p"
    "c2hlZCB0YXNrJ3MgYWN0dWFsIHNpemU7IHJlZnJlc2ggdGhlIHVzZXIgcmVnaXN0cnkuIiIiCiAgICB0cnk6CiAgICAgICAgaWYg"
    "bm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgICAgICByZXR1cm4KICAgICAgICB1c2VyX2lkID0gbGlzdGVuZXIudXNl"
    "cl9pZAogICAgICAgIHNpemUgPSBXWkZJWF9CV19GQUNUT1IgKiBpbnQobGlzdGVuZXIuc2l6ZSBvciAwKQogICAgICAgIG5vdyA9"
    "IHRpbWUoKQogICAgICAgIGRiID0gX2RiKCkKICAgICAgICBwYXJ0ID0gX3BhcnQoKQogICAgICAgIGlmIHNpemUgPiAwOgogICAg"
    "ICAgICAgICBhd2FpdCBkYi53emZpeF9xdW90YVtwYXJ0XS51cGRhdGVfb25lKAogICAgICAgICAgICAgICAgeyJfaWQiOiBmInt1"
    "c2VyX2lkfTp7X2RheV9pc3QoKX0ifSwKICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAiJGluYyI6IHsidXNl"
    "ZCI6IHNpemV9LAogICAgICAgICAgICAgICAgICAgICIkc2V0IjogewogICAgICAgICAgICAgICAgICAgICAgICAiZXhwaXJlQXQi"
    "OiBkYXRldGltZS5mcm9tdGltZXN0YW1wKAogICAgICAgICAgICAgICAgICAgICAgICAgICAgbm93ICsgV1pGSVhfUVVPVEFfVFRM"
    "X0RBWVMgKiA4NjQwMCwgVVRDCiAgICAgICAgICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICB9LAogICAgICAg"
    "ICAgICAgICAgfSwKICAgICAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICAgICApCiAgICAgICAgICAgIGRvYyA9IGF3"
    "YWl0IGRiLnd6Zml4X3F1b3RhW3BhcnRdLmZpbmRfb25lKHsiX2lkIjogZiJ7dXNlcl9pZH06e19kYXlfaXN0KCl9In0pCiAgICAg"
    "ICAgICAgIExPR0dFUi5pbmZvKAogICAgICAgICAgICAgICAgZiJXWkZJWCBjaGFyZ2U6IHVzZXI9e3VzZXJfaWR9IHRhc2tfc2l6"
    "ZT17c2l6ZX0gIgogICAgICAgICAgICAgICAgZiItPiB0b2RheV90b3RhbD17aW50KGRvYy5nZXQoJ3VzZWQnLCAwKSkgaWYgZG9j"
    "IGVsc2UgJz8nfSAiCiAgICAgICAgICAgICAgICBmIihkYXk9e19kYXlfaXN0KCl9KSIKICAgICAgICAgICAgKQogICAgICAgIHVu"
    "YW1lID0gIiIKICAgICAgICBuYW1lID0gIiIKICAgICAgICB0cnk6CiAgICAgICAgICAgIHVuYW1lID0gZ2V0YXR0cihsaXN0ZW5l"
    "ci51c2VyLCAidXNlcm5hbWUiLCAiIikgb3IgIiIKICAgICAgICAgICAgbmFtZSA9IChnZXRhdHRyKGxpc3RlbmVyLnVzZXIsICJm"
    "aXJzdF9uYW1lIiwgIiIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgcGFzcwog"
    "ICAgICAgIGF3YWl0IGRiLnd6Zml4X3VzZXJzW3BhcnRdLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogdXNlcl9pZH0s"
    "CiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICIkc2V0IjogeyJ1bmFtZSI6IHVuYW1lLCAibmFtZSI6IG5hbWUsICJsYXN0"
    "X3VzZWQiOiBub3d9LAogICAgICAgICAgICAgICAgIiRpbmMiOiB7InRvdGFsX3VzZWQiOiBzaXplLCAidGFza3MiOiAxfSwKICAg"
    "ICAgICAgICAgfSwKICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgog"
    "ICAgICAgIExPR0dFUi5lcnJvcihmIldaRklYIGNoYXJnZSBmYWlsZWQgKGlnbm9yZWQpOiB7ZX0iKQoKCmFzeW5jIGRlZiByZWNv"
    "cmRfdGFzayhsaXN0ZW5lciwgbGluaywgZmlsZXMsIG1pbWVfdHlwZSwgcmNsb25lX3BhdGg9IiIsIGRpcl9pZD0iIik6CiAgICAi"
    "IiJDYWxsZWQgZnJvbSBUYXNrTGlzdGVuZXIub25fdXBsb2FkX2NvbXBsZXRlIGZvciBldmVyeSBmaW5pc2hlZCB0YXNrLiIiIgog"
    "ICAgIyBmcmVlIHRoaXMgdGFzaydzIGJhbmR3aWR0aCBob2xkIGJlZm9yZSBjaGFyZ2luZyB0aGUgYWN0dWFsIGFtb3VudAogICAg"
    "YXdhaXQgcmVsZWFzZV9yZXNlcnZlKGxpc3RlbmVyKQogICAgIyBpZGVtcG90ZW5jeTogYSBkdXBsaWNhdGUgY29tcGxldGlvbiBm"
    "b3IgdGhlIHNhbWUgdGFzayAoc2FtZSBjb21tYW5kCiAgICAjIG1lc3NhZ2UsIG5hbWUgYW5kIHNpemUpIG11c3QgbmV2ZXIgY2hh"
    "cmdlIHR3aWNlCiAgICB0cnk6CiAgICAgICAga2V5ID0gewogICAgICAgICAgICAibWlkIjogaW50KGxpc3RlbmVyLm1pZCBvciAw"
    "KSwKICAgICAgICAgICAgIm5hbWUiOiBzdHIobGlzdGVuZXIubmFtZSBvciAiIiksCiAgICAgICAgICAgICJzaXplIjogaW50KGxp"
    "c3RlbmVyLnNpemUgb3IgMCksCiAgICAgICAgfQogICAgICAgIGR1cCA9IGF3YWl0IF9kYigpLnd6Zml4X2xpYnJhcnlbX3BhcnQo"
    "KV0uZmluZF9vbmUoa2V5KQogICAgICAgIGlmIGR1cDoKICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoCiAgICAgICAgICAgICAg"
    "ICBmIldaRklYIGNoYXJnZTogRFVQTElDQVRFIGNvbXBsZXRpb24gZm9yIG1pZD17a2V5WydtaWQnXX0gIgogICAgICAgICAgICAg"
    "ICAgZiIne2tleVsnbmFtZSddfScgc2l6ZT17a2V5WydzaXplJ119IC0+IG5vdCBjaGFyZ2VkIGFnYWluIgogICAgICAgICAgICAp"
    "CiAgICAgICAgICAgIHJldHVybgogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBhd2FpdCBjaGFyZ2UobGlz"
    "dGVuZXIpCiAgICB0cnk6CiAgICAgICAgaWYgbm90IGF3YWl0IGVuc3VyZV9yZWFkeSgpOgogICAgICAgICAgICByZXR1cm4KICAg"
    "ICAgICBub3cgPSB0aW1lKCkKICAgICAgICBwYXJ0cyA9IFtdCiAgICAgICAgdGdfbGlua3MgPSBbXQogICAgICAgIHBhcnRfbmFt"
    "ZXMgPSBbXQogICAgICAgIGlmIGlzaW5zdGFuY2UoZmlsZXMsIGRpY3QpOgogICAgICAgICAgICBmb3IgbCwgbiBpbiBmaWxlcy5p"
    "dGVtcygpOgogICAgICAgICAgICAgICAgbCA9IHN0cihsKQogICAgICAgICAgICAgICAgdGdfbGlua3MuYXBwZW5kKGwpCiAgICAg"
    "ICAgICAgICAgICBwYXJ0X25hbWVzLmFwcGVuZChzdHIobikpCiAgICAgICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICAg"
    "ICAgdGFpbCA9IGwucnN0cmlwKCIvIikuc3BsaXQoIi8iKVstMjpdCiAgICAgICAgICAgICAgICAgICAgY2lkLCBtaWQgPSB0YWls"
    "WzBdLCB0YWlsWzFdCiAgICAgICAgICAgICAgICAgICAgaWYgY2lkLmlzZGlnaXQoKSBhbmQgbWlkLmlzZGlnaXQoKToKICAgICAg"
    "ICAgICAgICAgICAgICAgICAgcGFydHMuYXBwZW5kKFtpbnQoZiItMTAwe2NpZH0iKSwgaW50KG1pZCldKQogICAgICAgICAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgICAgICBjb250aW51ZQogICAgICAgIHRyeToKICAgICAgICAgICAg"
    "bW9kZSA9IGYie2xpc3RlbmVyLm1vZGVbMF19IOKGkiB7bGlzdGVuZXIubW9kZVsxXX0iCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlv"
    "bjoKICAgICAgICAgICAgbW9kZSA9ICIiCiAgICAgICAgZG9jID0gewogICAgICAgICAgICAiX2lkIjogdXVpZDQoKS5oZXgsCiAg"
    "ICAgICAgICAgICJtaWQiOiBpbnQobGlzdGVuZXIubWlkIG9yIDApLAogICAgICAgICAgICAibmFtZSI6IGxpc3RlbmVyLm5hbWUs"
    "CiAgICAgICAgICAgICJzaXplIjogaW50KGxpc3RlbmVyLnNpemUgb3IgMCksCiAgICAgICAgICAgICJ1c2VyX2lkIjogbGlzdGVu"
    "ZXIudXNlcl9pZCwKICAgICAgICAgICAgInVuYW1lIjogZ2V0YXR0cihsaXN0ZW5lci51c2VyLCAidXNlcm5hbWUiLCAiIikgb3Ig"
    "IiIsCiAgICAgICAgICAgICJkYXRlIjogbm93LAogICAgICAgICAgICAiZXhwaXJlQXQiOiBkYXRldGltZS5mcm9tdGltZXN0YW1w"
    "KAogICAgICAgICAgICAgICAgbm93ICsgV1pGSVhfTElCX1RUTF9EQVlTICogODY0MDAsIFVUQwogICAgICAgICAgICApLAogICAg"
    "ICAgICAgICAiaXNfbGVlY2giOiBib29sKGxpc3RlbmVyLmlzX2xlZWNoKSwKICAgICAgICAgICAgIm1vZGUiOiBtb2RlLAogICAg"
    "ICAgICAgICAicGFydHMiOiBwYXJ0cywKICAgICAgICAgICAgInRnX2xpbmtzIjogdGdfbGlua3MsCiAgICAgICAgICAgICJwYXJ0"
    "X25hbWVzIjogcGFydF9uYW1lcywKICAgICAgICAgICAgImNsb3VkX2xpbmsiOiBsaW5rIGlmIGlzaW5zdGFuY2UobGluaywgc3Ry"
    "KSBlbHNlICIiLAogICAgICAgICAgICAicmNsb25lX3BhdGgiOiByY2xvbmVfcGF0aCBvciAiIiwKICAgICAgICAgICAgImRpcl9p"
    "ZCI6IGRpcl9pZCBvciAiIiwKICAgICAgICB9CiAgICAgICAgYXdhaXQgX2RiKCkud3pmaXhfbGlicmFyeVtfcGFydCgpXS5pbnNl"
    "cnRfb25lKGRvYykKICAgICAgICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgZiJXWkZJWCBsaWJyYXJ5OiByZWNvcmRlZCAne2Rv"
    "Y1snbmFtZSddfScgc2l6ZT17ZG9jWydzaXplJ119ICIKICAgICAgICAgICAgZiJ1c2VyPXtkb2NbJ3VzZXJfaWQnXX0gdGdfcGFy"
    "dHM9e2xlbihkb2NbJ3BhcnRzJ10pfSIKICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VS"
    "LmVycm9yKGYiV1pGSVggcmVjb3JkX3Rhc2sgZmFpbGVkIChpZ25vcmVkKToge2V9IikKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIExpYnJhcnkgc2VhcmNoCiMg4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACgoKYXN5bmMgZGVm"
    "IGZpbmQocXVlcnksIHVzZXJfaWQsIGFsbF91c2Vycz1GYWxzZSwgbGltaXQ9Nik6CiAgICBpZiBub3QgYXdhaXQgZW5zdXJlX3Jl"
    "YWR5KCk6CiAgICAgICAgcmV0dXJuIFtdCiAgICBxID0geyJuYW1lIjogeyIkcmVnZXgiOiBfcmVzY2FwZShxdWVyeSksICIkb3B0"
    "aW9ucyI6ICJpIn19CiAgICBpZiBub3QgYWxsX3VzZXJzOgogICAgICAgIHFbInVzZXJfaWQiXSA9IHVzZXJfaWQKICAgIHRyeToK"
    "ICAgICAgICBjdXJzb3IgPSAoCiAgICAgICAgICAgIF9kYigpLnd6Zml4X2xpYnJhcnlbX3BhcnQoKV0uZmluZChxKS5zb3J0KCJk"
    "YXRlIiwgLTEpLmxpbWl0KGludChsaW1pdCkpCiAgICAgICAgKQogICAgICAgIHJldHVybiBbZCBhc3luYyBmb3IgZCBpbiBjdXJz"
    "b3JdCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgTE9HR0VSLmVycm9yKGYiV1pGSVggZmluZCBmYWlsZWQ6IHtl"
    "fSIpCiAgICAgICAgcmV0dXJuIFtdCgoKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIAKIyBEQiBzdGF0cyAvIGNsZWFudXAKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgZGJzdGF0cygpOgogICAgIiIid3ptbHgg"
    "YnJlYWtkb3duICsgYWNjb3VudC13aWRlIChhbGwgZGF0YWJhc2VzKSBzaXplcy4iIiIKICAgIG91dCA9IHsidG90YWwiOiAwLCAi"
    "Y29scyI6IFtdLCAiZGJzIjogW10sICJhY2NvdW50X3RvdGFsIjogMCwgImRiX2Vycm9yIjogIiIsICJlcnJvciI6ICIifQogICAg"
    "dHJ5OgogICAgICAgIGRiID0gX2RiKCkKICAgICAgICBpZiBkYiBpcyBOb25lOgogICAgICAgICAgICBvdXRbImVycm9yIl0gPSAi"
    "ZGF0YWJhc2Ugbm90IGNvbm5lY3RlZCIKICAgICAgICAgICAgcmV0dXJuIG91dAogICAgICAgIHN0ID0gYXdhaXQgZGIuY29tbWFu"
    "ZCh7ImRiU3RhdHMiOiAxLCAic2NhbGUiOiAxMDI0fSkKICAgICAgICBvdXRbInRvdGFsIl0gPSBpbnQoc3QuZ2V0KCJkYXRhU2l6"
    "ZSIpIG9yIDApICogMTAyNAogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIG91dFsiZXJyb3IiXSA9IHN0cihlKQog"
    "ICAgdHJ5OgogICAgICAgIG5hbWVzID0gYXdhaXQgX2RiKCkubGlzdF9jb2xsZWN0aW9uX25hbWVzKCkKICAgICAgICBwYXJ0ID0g"
    "X3BhcnQoKQogICAgICAgIGFnZyA9IHt9CiAgICAgICAgZm9yIG5hbWUgaW4gbmFtZXM6CiAgICAgICAgICAgIHRyeToKICAgICAg"
    "ICAgICAgICAgIGNzID0gYXdhaXQgX2RiKCkuY29tbWFuZCh7ImNvbGxTdGF0cyI6IG5hbWV9KQogICAgICAgICAgICAgICAgaWYg"
    "Ii4iIGluIG5hbWU6CiAgICAgICAgICAgICAgICAgICAgYmFzZSwgc3VmZml4ID0gbmFtZS5zcGxpdCgiLiIsIDEpCiAgICAgICAg"
    "ICAgICAgICAgICAgIyBhIHBhcnRpdGlvbiBzdWZmaXggZXF1YWwgdG8gb3VycyA9IHRoaXMgYm90OyBvdGhlciBib3RzCiAgICAg"
    "ICAgICAgICAgICAgICAgIyBzaGFyaW5nIHRoaXMgQXRsYXMgYWNjb3VudCBnZXQgdGhlaXIgb3duIGF0dHJpYnV0aW9uCiAgICAg"
    "ICAgICAgICAgICAgICAgb3duZXIgPSAic2VsZiIgaWYgc3VmZml4ID09IHBhcnQgZWxzZSAib3RoZXIiCiAgICAgICAgICAgICAg"
    "ICBlbHNlOgogICAgICAgICAgICAgICAgICAgIGJhc2UsIG93bmVyID0gbmFtZSwgInNoYXJlZCIKICAgICAgICAgICAgICAgIGEg"
    "PSBhZ2cuc2V0ZGVmYXVsdCgKICAgICAgICAgICAgICAgICAgICBiYXNlLAogICAgICAgICAgICAgICAgICAgIHsiZG9jcyI6IDAs"
    "ICJzaXplIjogMCwgInNlbGYiOiAwLCAib3RoZXIiOiAwLCAic2hhcmVkIjogMCwgIm4iOiAwfSwKICAgICAgICAgICAgICAgICkK"
    "ICAgICAgICAgICAgICAgIHN6ID0gaW50KGNzLmdldCgic2l6ZSIpIG9yIDApCiAgICAgICAgICAgICAgICBhWyJkb2NzIl0gKz0g"
    "aW50KGNzLmdldCgiY291bnQiKSBvciAwKQogICAgICAgICAgICAgICAgYVsic2l6ZSJdICs9IHN6CiAgICAgICAgICAgICAgICBh"
    "W293bmVyXSArPSBzegogICAgICAgICAgICAgICAgYVsibiJdICs9IDEKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAg"
    "ICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgb3V0WyJjb2xzIl0gPSBzb3J0ZWQoYWdnLml0ZW1zKCksIGtleT1sYW1iZGEg"
    "a3Y6IC1rdlsxXVsic2l6ZSJdKQogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIG91dFsiZXJyb3IiXSA9IHN0cihl"
    "KQogICAgIyBhY2NvdW50LXdpZGU6IGV2ZXJ5IGRhdGFiYXNlIG9uIHRoaXMgQXRsYXMgY2x1c3RlciAodGhlIGZyZWUtdGllcgog"
    "ICAgIyA1MTIgTUIgaXMgc2hhcmVkIGFjcm9zcyBBTEwgb2YgdGhlbSwgbm90IGp1c3Qgd3ptbHgpCiAgICB0cnk6CiAgICAgICAg"
    "YWRtaW5fZGIgPSBfZGIoKS5jbGllbnQuZ2V0X2RhdGFiYXNlKCJhZG1pbiIpCiAgICAgICAgbGQgPSBhd2FpdCBhZG1pbl9kYi5j"
    "b21tYW5kKHsibGlzdERhdGFiYXNlcyI6IDF9KQogICAgICAgIG91dFsiZGJzIl0gPSBzb3J0ZWQoCiAgICAgICAgICAgICgKICAg"
    "ICAgICAgICAgICAgIChzdHIoZC5nZXQoIm5hbWUiKSksIGludChkLmdldCgic2l6ZU9uRGlzayIpIG9yIDApKQogICAgICAgICAg"
    "ICAgICAgZm9yIGQgaW4gbGQuZ2V0KCJkYXRhYmFzZXMiLCBbXSkKICAgICAgICAgICAgKSwKICAgICAgICAgICAga2V5PWxhbWJk"
    "YSB0OiAtdFsxXSwKICAgICAgICApCiAgICAgICAgb3V0WyJhY2NvdW50X3RvdGFsIl0gPSBpbnQobGQuZ2V0KCJ0b3RhbFNpemUi"
    "KSBvciAwKSBvciBzdW0oCiAgICAgICAgICAgIHMgZm9yIF8sIHMgaW4gb3V0WyJkYnMiXQogICAgICAgICkKICAgIGV4Y2VwdCBF"
    "eGNlcHRpb24gYXMgZToKICAgICAgICBvdXRbImRiX2Vycm9yIl0gPSBzdHIoZSkKICAgIHJldHVybiBvdXQKCgphc3luYyBkZWYg"
    "ZGJjbGVhbigpOgogICAgIiIiU2FmZSBwdXJnZXMgb25seS4gUmV0dXJucyB7bGFiZWw6IGZyZWVkX2RvY19jb3VudH0uIiIiCiAg"
    "ICBmcmVlZCA9IHt9CiAgICBkYiA9IF9kYigpCiAgICBwYXJ0ID0gX3BhcnQoKQogICAgdHJ5OgogICAgICAgIHIgPSBhd2FpdCBk"
    "Yi5zdHJlYW1zW3BhcnRdLmRlbGV0ZV9tYW55KHsiZXhwIjogeyIkbHQiOiBpbnQodGltZSgpKX19KQogICAgICAgIGZyZWVkWyJl"
    "eHBpcmVkIHN0cmVhbSB0b2tlbnMiXSA9IHIuZGVsZXRlZF9jb3VudAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNz"
    "CiAgICB0cnk6CiAgICAgICAgciA9IGF3YWl0IGRiLnRhc2tzW3BhcnRdLmRlbGV0ZV9tYW55KHt9KQogICAgICAgIGZyZWVkWyJp"
    "bmNvbXBsZXRlLXRhc2sgcmVzdW1lIHJlY29yZHMiXSA9IHIuZGVsZXRlZF9jb3VudAogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAg"
    "ICAgICBwYXNzCiAgICB0cnk6CiAgICAgICAgciA9IGF3YWl0IGRiLnd6Zml4X3F1b3RhW3BhcnRdLmRlbGV0ZV9tYW55KAogICAg"
    "ICAgICAgICB7ImV4cGlyZUF0IjogeyIkbHQiOiBkYXRldGltZS5ub3coVVRDKX19CiAgICAgICAgKQogICAgICAgIGZyZWVkWyJz"
    "dGFsZSB3emZpeCBxdW90YSByb3dzIl0gPSByLmRlbGV0ZWRfY291bnQKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFz"
    "cwogICAgcmV0dXJuIGZyZWVkCgoKYXN5bmMgZGVmIGJvb3RfcmVwb3J0KCk6CiAgICAiIiJEZWxheWVkIGJvb3QgbG9nOiB0b3Rh"
    "bCBEQiBzaXplICsgd2FybmluZyBwYXN0IDgwJSBvZiB0aGUgNTEyIE1CIHRpZXIuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSBh"
    "c3luY2lvIGltcG9ydCBzbGVlcAoKICAgICAgICBhd2FpdCBzbGVlcCg0NSkKICAgICAgICBzdCA9IGF3YWl0IGRic3RhdHMoKQog"
    "ICAgICAgIGlmIHN0LmdldCgiZXJyb3IiKSBhbmQgbm90IHN0LmdldCgiY29scyIpOgogICAgICAgICAgICByZXR1cm4KICAgICAg"
    "ICBMT0dHRVIuaW5mbygKICAgICAgICAgICAgZiJXWkZJWCBEQjoge19uaWNlX3NpemUoc3RbJ3RvdGFsJ10pfSBhY3Jvc3MgIgog"
    "ICAgICAgICAgICBmIntsZW4oc3RbJ2NvbHMnXSl9IGNvbGxlY3Rpb24gZ3JvdXBzIChmcmVlIHRpZXIgNTEyIE1CKSIKICAgICAg"
    "ICApCiAgICAgICAgaWYgc3RbInRvdGFsIl0gPiAwLjggKiBXWkZJWF9BVExBU19GUkVFOgogICAgICAgICAgICBMT0dHRVIud2Fy"
    "bmluZygKICAgICAgICAgICAgICAgICJXWkZJWCBEQjogdXNhZ2UgaXMgYWJvdmUgODAlIG9mIHRoZSBBdGxhcyBmcmVlIHRpZXIg"
    "KDUxMiBNQikhICIKICAgICAgICAgICAgICAgICJSdW4gL2Ric3RhdHMgYW5kIC9kYmNsZWFuIHRvIHJlY2xhaW0gc3BhY2UuIgog"
    "ICAgICAgICAgICApCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKCgphc3luYyBkZWYgZGlhZyh1c2VyX2lkLCBw"
    "cm9iZV9nYj1Ob25lKToKICAgICIiIkNvbXBsZXRlIHF1b3RhIHN0YXRlIGZvciBvbmUgdXNlciwgZm9yIGxpdmUgZGVidWdnaW5n"
    "LiIiIgogICAgb3V0ID0ge30KICAgIG91dFsicGFydGl0aW9uIl0gPSBfcGFydCgpCiAgICBvdXRbImRheSJdID0gX2RheV9pc3Qo"
    "KQogICAgb3V0WyJkYl9yZWFkeSJdID0gYXdhaXQgZW5zdXJlX3JlYWR5KCkKICAgIG91dFsidXNlcl9kb2MiXSA9IGF3YWl0IGdl"
    "dF91c2VyX2RvYyh1c2VyX2lkKQogICAgb3V0WyJnbG9iYWxfY2FwX2diIl0gPSBhd2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKQog"
    "ICAgb3V0WyJjYXBfYnl0ZXMiXSA9IGF3YWl0IGdldF9jYXBfYnl0ZXModXNlcl9pZCkKICAgIG91dFsicXVvdGFfZG9jIl0gPSBO"
    "b25lCiAgICB0cnk6CiAgICAgICAgb3V0WyJxdW90YV9kb2MiXSA9IGF3YWl0IF9kYigpLnd6Zml4X3F1b3RhW19wYXJ0KCldLmZp"
    "bmRfb25lKAogICAgICAgICAgICB7Il9pZCI6IGYie3VzZXJfaWR9OntfZGF5X2lzdCgpfSJ9CiAgICAgICAgKQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICBvdXRbInJlc2VydmVkIl0gPSBhd2FpdCByZXNlcnZlZF90b2RheSh1c2VyX2lk"
    "KQogICAgaWYgcHJvYmVfZ2IgaXMgbm90IE5vbmU6CiAgICAgICAgcHJvYmUgPSBwcm9iZV9nYiAqIEdCCiAgICAgICAgdXNlZCA9"
    "IGF3YWl0IGdldF91c2FnZSh1c2VyX2lkKQogICAgICAgIGNhcCA9IG91dFsiY2FwX2J5dGVzIl0KICAgICAgICBvdXRbInByb2Jl"
    "Il0gPSB7CiAgICAgICAgICAgICJzaXplIjogcHJvYmUsCiAgICAgICAgICAgICJ1c2VkIjogdXNlZCwKICAgICAgICAgICAgInJl"
    "c2VydmVkIjogb3V0WyJyZXNlcnZlZCJdLAogICAgICAgICAgICAiY2FwIjogY2FwLAogICAgICAgICAgICAid291bGRfYmxvY2si"
    "OiAoCiAgICAgICAgICAgICAgICB1c2VkICsgb3V0WyJyZXNlcnZlZCJdID49IGNhcAogICAgICAgICAgICAgICAgb3IgdXNlZCAr"
    "IG91dFsicmVzZXJ2ZWQiXSArIHByb2JlID4gY2FwICsgV1pGSVhfR1JBQ0VfTUIgKiBNQgogICAgICAgICAgICApLAogICAgICAg"
    "IH0KICAgIHJldHVybiBvdXQKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgAojIE93bmVyLWZhY2luZyBzdW1tYXJpZXMKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgphc3luYyBkZWYgdG9kYXlfcm93cygpOgogICAgIiIiQWxs"
    "IG9mIHRvZGF5J3MgcXVvdGEgcm93czogW3sidXNlcl9pZCIsInVzZWQifSwgLi4uXSBsYXJnZXN0IGZpcnN0LiIiIgogICAgdHJ5"
    "OgogICAgICAgIGRheSA9IF9kYXlfaXN0KCkKICAgICAgICBjdXJzb3IgPSBfZGIoKS53emZpeF9xdW90YVtfcGFydCgpXS5maW5k"
    "KHsiX2lkIjogeyIkcmVnZXgiOiBmIjp7ZGF5fSQifX0pCiAgICAgICAgcm93cyA9IFtdCiAgICAgICAgYXN5bmMgZm9yIGQgaW4g"
    "Y3Vyc29yOgogICAgICAgICAgICB0cnk6CiAgICAgICAgICAgICAgICB1aWQgPSBpbnQoc3RyKGRbIl9pZCJdKS5yc3BsaXQoIjoi"
    "LCAxKVswXSkKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgICAg"
    "IHJvd3MuYXBwZW5kKHsidXNlcl9pZCI6IHVpZCwgInVzZWQiOiBpbnQoZC5nZXQoInVzZWQiKSBvciAwKX0pCiAgICAgICAgcm93"
    "cy5zb3J0KGtleT1sYW1iZGEgcjogLXJbInVzZWQiXSkKICAgICAgICByZXR1cm4gcm93cwogICAgZXhjZXB0IEV4Y2VwdGlvbjoK"
    "ICAgICAgICByZXR1cm4gW10KCgphc3luYyBkZWYgYWxsX3VzZXJzKCk6CiAgICB0cnk6CiAgICAgICAgY3Vyc29yID0gX2RiKCku"
    "d3pmaXhfdXNlcnNbX3BhcnQoKV0uZmluZCgpLnNvcnQoImxhc3RfdXNlZCIsIC0xKQogICAgICAgIHJldHVybiBbZCBhc3luYyBm"
    "b3IgZCBpbiBjdXJzb3JdCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiBbXQo="
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
    "cG9ydCBDb25maWcKCiAgICAgICAgX2JzID0gc3RyKGdldGF0dHIoQ29uZmlnLCAiQURNSU5fUEFTUyIsICIiKSBvciAiIikuc3Ry"
    "aXAoKQogICAgICAgIGlmIF9iczoKICAgICAgICAgICAgcmV0dXJuIF9icwogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBw"
    "YXNzCiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCkud3pmaXhfY29uZmlnW19wYXJ0KCldCiAgICAgICAgZG9jID0gYXdhaXQg"
    "Y29sLmZpbmRfb25lKHsiX2lkIjogImFkbWluIn0pCiAgICAgICAgaWYgZG9jIGFuZCBkb2MuZ2V0KCJwYXNzIik6CiAgICAgICAg"
    "ICAgIHJldHVybiBzdHIoZG9jWyJwYXNzIl0pCiAgICAgICAgZnJvbSBvcyBpbXBvcnQgZ2V0ZW52CgogICAgICAgIHB3ID0gZ2V0"
    "ZW52KCJXWkZJWF9BRE1JTl9QQVNTIiwgIiIpLnN0cmlwKCkgb3IgdG9rZW5fdXJsc2FmZSg2KQogICAgICAgIGF3YWl0IGNvbC51"
    "cGRhdGVfb25lKHsiX2lkIjogImFkbWluIn0sIHsiJHNldCI6IHsicGFzcyI6IHB3fX0sIHVwc2VydD1UcnVlKQogICAgICAgIHJl"
    "dHVybiBwdwogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gIiIKCgphc3luYyBkZWYgc2V0X2FkbWluX3Bhc3Mo"
    "bmV3X3Bhc3MpOgogICAgY29sID0gX2RiKCkud3pmaXhfY29uZmlnW19wYXJ0KCldCiAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgK"
    "ICAgICAgICB7Il9pZCI6ICJhZG1pbiJ9LCB7IiRzZXQiOiB7InBhc3MiOiBzdHIobmV3X3Bhc3MpLnN0cmlwKCl9fSwgdXBzZXJ0"
    "PVRydWUKICAgICkKCgphc3luYyBkZWYgX2FkbWluX3NlY3JldCgpOgogICAgIiIiUmFuZG9tIHBlci1ib3Qgc2lnbmluZyBzZWNy"
    "ZXQgKGNyZWF0ZWQgb25jZSwgc3RvcmVkIGluIHd6Zml4X2NvbmZpZykuIiIiCiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCku"
    "d3pmaXhfY29uZmlnW19wYXJ0KCldCiAgICAgICAgZG9jID0gYXdhaXQgY29sLmZpbmRfb25lKHsiX2lkIjogImFkbWluX3NlY3Jl"
    "dCJ9KQogICAgICAgIGlmIGRvYyBhbmQgZG9jLmdldCgic2VjcmV0Iik6CiAgICAgICAgICAgIHJldHVybiBzdHIoZG9jWyJzZWNy"
    "ZXQiXSkKICAgICAgICBzZWNyZXQgPSB0b2tlbl9oZXgoMzIpCiAgICAgICAgYXdhaXQgY29sLnVwZGF0ZV9vbmUoCiAgICAgICAg"
    "ICAgIHsiX2lkIjogImFkbWluX3NlY3JldCJ9LCB7IiRzZXQiOiB7InNlY3JldCI6IHNlY3JldH19LCB1cHNlcnQ9VHJ1ZQogICAg"
    "ICAgICkKICAgICAgICByZXR1cm4gc2VjcmV0CiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAid3pmaXgtbm8t"
    "c2VjcmV0IgoKCmFzeW5jIGRlZiBfc2Vzc2lvbl90b2tlbigpOgogICAgc2VjcmV0ID0gYXdhaXQgX2FkbWluX3NlY3JldCgpCiAg"
    "ICBleHAgPSBpbnQodGltZSgpKSArIFNFU1NJT05fSCAqIDM2MDAKICAgIHNpZyA9IGhtYWNfbmV3KHNlY3JldC5lbmNvZGUoKSwg"
    "ZiJhZG1pbjp7ZXhwfSIuZW5jb2RlKCksIHNoYTI1NikuaGV4ZGlnZXN0KCkKICAgIHJldHVybiBmIntleHB9LntzaWd9IgoKCmFz"
    "eW5jIGRlZiBfY2hlY2tfc2Vzc2lvbihyZXF1ZXN0KToKICAgIHRyeToKICAgICAgICB0b2sgPSByZXF1ZXN0LmNvb2tpZXMuZ2V0"
    "KCJ3emFkbWluIiwgIiIpCiAgICAgICAgZXhwLCBfLCBzaWcgPSB0b2sucGFydGl0aW9uKCIuIikKICAgICAgICBzZWNyZXQgPSBh"
    "d2FpdCBfYWRtaW5fc2VjcmV0KCkKICAgICAgICBnb29kID0gaG1hY19uZXcoc2VjcmV0LmVuY29kZSgpLCBmImFkbWluOntleHB9"
    "Ii5lbmNvZGUoKSwgc2hhMjU2KS5oZXhkaWdlc3QoKQogICAgICAgIGlmIG5vdCB0b2sgb3Igbm90IGNvbXBhcmVfZGlnZXN0KHNp"
    "ZywgZ29vZCk6CiAgICAgICAgICAgIHJldHVybiBGYWxzZQogICAgICAgIHJldHVybiBpbnQoZXhwKSA+IHRpbWUoKQogICAgZXhj"
    "ZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gRmFsc2UKCgpkZWYgX2NsaWVudF9pcChyZXF1ZXN0KToKICAgIGZ3ZCA9IHJl"
    "cXVlc3QuaGVhZGVycy5nZXQoIlgtRm9yd2FyZGVkLUZvciIsICIiKQogICAgaWYgZndkOgogICAgICAgIHJldHVybiBmd2Quc3Bs"
    "aXQoIiwiKVswXS5zdHJpcCgpWzo2NF0KICAgIHRyeToKICAgICAgICByZXR1cm4gKHJlcXVlc3QucmVtb3RlIG9yICI/IilbOjY0"
    "XQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICByZXR1cm4gIj8iCgoKZGVmIF91YV9vayhyZXF1ZXN0KToKICAgIHVhID0g"
    "KHJlcXVlc3QuaGVhZGVycy5nZXQoIlVzZXItQWdlbnQiKSBvciAiIikuc3RyaXAoKS5sb3dlcigpCiAgICBpZiBub3QgdWEgb3Ig"
    "bGVuKHVhKSA8IDEwOgogICAgICAgIHJldHVybiBGYWxzZQogICAgaWYgYW55KGIgaW4gdWEgZm9yIGIgaW4gX0JBRF9VQSk6CiAg"
    "ICAgICAgcmV0dXJuIEZhbHNlCiAgICByZXR1cm4gYW55KGsgaW4gdWEgZm9yIGsgaW4gX0tOT1dOX1VBKQoKCmRlZiBfc3VibmV0"
    "KGlwKToKICAgICIiIklQdjQgLzI0IG9yIElQdjYgLzY0IHByZWZpeCBmb3Igc3VibmV0LWxldmVsIGJhbnMuIiIiCiAgICB0cnk6"
    "CiAgICAgICAgaWYgIjoiIGluIGlwOgogICAgICAgICAgICByZXR1cm4gIi8iLmpvaW4oaXAuc3BsaXQoIjoiKVs6NF0pICsgIjo6"
    "LzY0IgogICAgICAgIHBhcnRzID0gaXAuc3BsaXQoIi4iKQogICAgICAgIGlmIGxlbihwYXJ0cykgPT0gNDoKICAgICAgICAgICAg"
    "cmV0dXJuICIuIi5qb2luKHBhcnRzWzozXSkgKyAiLjAvMjQiCiAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHBhc3MKICAg"
    "IHJldHVybiBpcAoKCmRlZiBfbm9ybV9pcChpcCk6CiAgICBpcCA9IChpcCBvciAiIikuc3RyaXAoKQogICAgaWYgaXAuc3RhcnRz"
    "d2l0aCgiOjpmZmZmOiIpOgogICAgICAgIGlwID0gaXBbNzpdCiAgICByZXR1cm4gaXBbOjQ1XQoKCmFzeW5jIGRlZiBfaXNfYmFu"
    "bmVkKGlwKToKICAgICIiIklQIG9yIHN1Ym5ldCBiYW4gY2hlY2sgKHBlcnNpc3RlbnQsIERCLWJhY2tlZCkuIiIiCiAgICB0cnk6"
    "CiAgICAgICAgY29sID0gX2RiKCkud3pmaXhfYmFuc1tfcGFydCgpXQogICAgICAgIGlmIGF3YWl0IGNvbC5maW5kX29uZSh7Il9p"
    "ZCI6IF9ub3JtX2lwKGlwKX0pOgogICAgICAgICAgICByZXR1cm4gVHJ1ZQogICAgICAgIGlmIGF3YWl0IGNvbC5maW5kX29uZSh7"
    "Il9pZCI6ICJzdWI6IiArIF9zdWJuZXQoX25vcm1faXAoaXApKX0pOgogICAgICAgICAgICByZXR1cm4gVHJ1ZQogICAgZXhjZXB0"
    "IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gRmFsc2UKCgphc3luYyBkZWYgX2Jhbl9pcChpcCwgc3VibmV0X3Rv"
    "bz1UcnVlKToKICAgIHRyeToKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9iYW5zW19wYXJ0KCldCiAgICAgICAgYXdhaXQgY29s"
    "LnVwZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAgeyIkc2V0IjogeyJwZXJt"
    "IjogVHJ1ZSwgInRzIjogdGltZSgpfX0sCiAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICkKICAgICAgICBpZiBzdWJu"
    "ZXRfdG9vOgogICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogInN1YjoiICsg"
    "X3N1Ym5ldChfbm9ybV9pcChpcCkpfSwKICAgICAgICAgICAgICAgIHsiJHNldCI6IHsicGVybSI6IFRydWUsICJ0cyI6IHRpbWUo"
    "KX19LAogICAgICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgICAgICkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAg"
    "ICAgcGFzcwoKCmFzeW5jIGRlZiBfcHJpb3JfZmFpbHMoaXApOgogICAgIiIiRmFpbGVkLWF0dGVtcHQgY291bnQgY3VycmVudGx5"
    "IG9uIHJlY29yZCBmb3IgdGhpcyBJUC4iIiIKICAgIHRyeToKICAgICAgICBkb2MgPSBhd2FpdCBfZGIoKS53emZpeF9sb2Nrc1tf"
    "cGFydCgpXS5maW5kX29uZSh7Il9pZCI6IF9ub3JtX2lwKGlwKX0pCiAgICAgICAgcmV0dXJuIGludCgoZG9jIG9yIHt9KS5nZXQo"
    "ImZhaWxzIikgb3IgMCkKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIDAKCgphc3luYyBkZWYgX2FsbG93X2lw"
    "KGlwLCBleHRyYT0zKToKICAgICIiIk93bmVyIHVuYmFubmVkIGFuIElQOiBjbGVhciBiYW5zICsgbG9ja291dCwgZ3JhbnQgTiBy"
    "ZXRyeSBhdHRlbXB0cy4KCiAgICBUaGUgdGllciBpcyBLRVBULCBzbyBidXJuaW5nIHRoZSByZXRyaWVzIGVzY2FsYXRlcyB0byB0"
    "aGUgbmV4dCBzdGFnZQogICAgKGxvbmdlciB0aGFuIHRoZSBwcmV2aW91cyBiYW4pLgogICAgIiIiCiAgICB0cnk6CiAgICAgICAg"
    "aXAgPSBfbm9ybV9pcChzdHIoaXApKQogICAgICAgIGJjb2wgPSBfZGIoKS53emZpeF9iYW5zW19wYXJ0KCldCiAgICAgICAgYXdh"
    "aXQgYmNvbC5kZWxldGVfb25lKHsiX2lkIjogaXB9KQogICAgICAgIGF3YWl0IGJjb2wuZGVsZXRlX29uZSh7Il9pZCI6ICJzdWI6"
    "IiArIF9zdWJuZXQoaXApfSkKICAgICAgICBsY29sID0gX2RiKCkud3pmaXhfbG9ja3NbX3BhcnQoKV0KICAgICAgICBhd2FpdCBs"
    "Y29sLnVwZGF0ZV9vbmUoCiAgICAgICAgICAgIHsiX2lkIjogaXB9LAogICAgICAgICAgICB7IiRzZXQiOiB7ImZhaWxzIjogMCwg"
    "ImV4dHJhIjogaW50KGV4dHJhKSwgInVudGlsIjogMCwgInBlcm0iOiBGYWxzZX19LAogICAgICAgICAgICB1cHNlcnQ9VHJ1ZSwK"
    "ICAgICAgICApCiAgICAgICAgcmV0dXJuIFRydWUKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcmV0dXJuIEZhbHNlCgoK"
    "YXN5bmMgZGVmIF9hbGxvd19hbGwoKToKICAgICIiIkNsZWFyIGV2ZXJ5IGJhbiwgc3VibmV0IGJhbiBhbmQgbG9ja291dC4iIiIK"
    "ICAgIHRyeToKICAgICAgICBuID0gMAogICAgICAgIGZvciBjb2xsIGluICgid3pmaXhfYmFucyIsICJ3emZpeF9sb2NrcyIpOgog"
    "ICAgICAgICAgICByID0gYXdhaXQgX2RiKClbY29sbF1bX3BhcnQoKV0uZGVsZXRlX21hbnkoe30pCiAgICAgICAgICAgIG4gKz0g"
    "aW50KGdldGF0dHIociwgImRlbGV0ZWRfY291bnQiLCAwKSBvciAwKQogICAgICAgIHJldHVybiBuCiAgICBleGNlcHQgRXhjZXB0"
    "aW9uOgogICAgICAgIHJldHVybiAwCgoKYXN5bmMgZGVmIF9saXN0X2JhbnMoKToKICAgICIiIkFsbCBiYW5zIGFuZCBsb2Nrb3V0"
    "cywgZm9yIHRoZSAvYmFucyBjb21tYW5kLiIiIgogICAgb3V0ID0gW10KICAgIHRyeToKICAgICAgICBhc3luYyBmb3IgZCBpbiBf"
    "ZGIoKS53emZpeF9iYW5zW19wYXJ0KCldLmZpbmQoe30sIGxpbWl0PTUwKToKICAgICAgICAgICAgb3V0LmFwcGVuZCgKICAgICAg"
    "ICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAia2luZCI6ICJiYW4iLAogICAgICAgICAgICAgICAgICAgICJpZCI6IGQu"
    "Z2V0KCJfaWQiLCAiPyIpLAogICAgICAgICAgICAgICAgICAgICJ0cyI6IGQuZ2V0KCJ0cyIsIDApLAogICAgICAgICAgICAgICAg"
    "fQogICAgICAgICAgICApCiAgICAgICAgYXN5bmMgZm9yIGQgaW4gX2RiKCkud3pmaXhfbG9ja3NbX3BhcnQoKV0uZmluZCh7fSwg"
    "bGltaXQ9NTApOgogICAgICAgICAgICBpZiBkLmdldCgicGVybSIpOgogICAgICAgICAgICAgICAgb3V0LmFwcGVuZCh7ImtpbmQi"
    "OiAicGVybS1sb2NrIiwgImlkIjogZC5nZXQoIl9pZCIsICI/IiksICJ0cyI6IDB9KQogICAgICAgICAgICBlbGlmIChkLmdldCgi"
    "dW50aWwiKSBvciAwKSA+IHRpbWUoKToKICAgICAgICAgICAgICAgIG91dC5hcHBlbmQoCiAgICAgICAgICAgICAgICAgICAgewog"
    "ICAgICAgICAgICAgICAgICAgICAgICAia2luZCI6ICJsb2Nrb3V0IiwKICAgICAgICAgICAgICAgICAgICAgICAgImlkIjogZC5n"
    "ZXQoIl9pZCIsICI/IiksCiAgICAgICAgICAgICAgICAgICAgICAgICJ1bnRpbCI6IGQuZ2V0KCJ1bnRpbCIpLAogICAgICAgICAg"
    "ICAgICAgICAgICAgICAidGllciI6IGQuZ2V0KCJ0aWVyIiwgMCksCiAgICAgICAgICAgICAgICAgICAgICAgICJleHRyYSI6IGQu"
    "Z2V0KCJleHRyYSIsIDApLAogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICkKICAgICAgICAgICAgZWxpZiBk"
    "LmdldCgiZXh0cmEiKToKICAgICAgICAgICAgICAgIG91dC5hcHBlbmQoCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAg"
    "ICAgICAgICAgICAgICAia2luZCI6ICJhbGxvd2VkIiwKICAgICAgICAgICAgICAgICAgICAgICAgImlkIjogZC5nZXQoIl9pZCIs"
    "ICI/IiksCiAgICAgICAgICAgICAgICAgICAgICAgICJleHRyYSI6IGQuZ2V0KCJleHRyYSIpLAogICAgICAgICAgICAgICAgICAg"
    "ICAgICAidGllciI6IGQuZ2V0KCJ0aWVyIiwgMCksCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgKQogICAg"
    "ZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gb3V0CgoKYXN5bmMgZGVmIF9sb2dpbl9hbGVydChpcCwg"
    "cmVxdWVzdCwgZGV2LCBzdWJtaXR0ZWQsIHRpdGxlKToKICAgICIiIkRhc2hib2FyZCBsb2dpbiBldmVudCAtPiBMT0dfQ0hBVCAo"
    "dG9nZ2xlOiBBRE1JTl9MT0dJTl9BTEVSVFMgdmlhIC9icykuIiIiCiAgICB0cnk6CiAgICAgICAgZnJvbSAuLi5jb3JlLmNvbmZp"
    "Z19tYW5hZ2VyIGltcG9ydCBDb25maWcKICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVudAoKICAg"
    "ICAgICBpZiBub3QgZ2V0YXR0cihDb25maWcsICJBRE1JTl9MT0dJTl9BTEVSVFMiLCBUcnVlKToKICAgICAgICAgICAgcmV0dXJu"
    "CiAgICAgICAgY2hhdCA9IHN0cihnZXRhdHRyKENvbmZpZywgIkxPR19DSEFUIiwgIiIpIG9yICIiKS5zdHJpcCgpCiAgICAgICAg"
    "aWYgbm90IGNoYXQ6CiAgICAgICAgICAgIGNoYXQgPSBzdHIoZ2V0YXR0cihDb25maWcsICJPV05FUl9JRCIsICIiKSBvciAiIiku"
    "c3RyaXAoKQogICAgICAgIGlmIG5vdCBjaGF0OgogICAgICAgICAgICByZXR1cm4KICAgICAgICBoZHJzID0gW10KICAgICAgICBm"
    "b3IgayBpbiAoCiAgICAgICAgICAgICJVc2VyLUFnZW50IiwgIkFjY2VwdC1MYW5ndWFnZSIsICJSZWZlcmVyIiwgIlNlYy1DaC1V"
    "YSIsCiAgICAgICAgICAgICJTZWMtQ2gtVWEtUGxhdGZvcm0iLCAiU2VjLUNoLVVhLU1vYmlsZSIsICJTZWMtRmV0Y2gtU2l0ZSIs"
    "CiAgICAgICAgICAgICJTZWMtRmV0Y2gtTW9kZSIsICJTZWMtRmV0Y2gtRGVzdCIsCiAgICAgICAgKToKICAgICAgICAgICAgdHJ5"
    "OgogICAgICAgICAgICAgICAgdiA9IHJlcXVlc3QuaGVhZGVycy5nZXQoaykKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoK"
    "ICAgICAgICAgICAgICAgIHYgPSBOb25lCiAgICAgICAgICAgIGlmIHY6CiAgICAgICAgICAgICAgICBoZHJzLmFwcGVuZChmIuKU"
    "oCB7a306IHtzdHIodilbOjEyMF19IikKICAgICAgICBkZXZfbCA9IFtdCiAgICAgICAgZm9yIGssIGxibCBpbiAoCiAgICAgICAg"
    "ICAgICgicGxhdGZvcm0iLCAiUGxhdGZvcm0iKSwgKCJsYW5nIiwgIkxhbmd1YWdlIiksCiAgICAgICAgICAgICgibGFuZ3MiLCAi"
    "TGFuZ3VhZ2VzIiksICgidHoiLCAiVGltZXpvbmUiKSwKICAgICAgICAgICAgKCJzY3JlZW4iLCAiU2NyZWVuIiksICgiZHByIiwg"
    "IlBpeGVsIHJhdGlvIiksCiAgICAgICAgICAgICgibWVtIiwgIkRldmljZSBtZW1vcnkiKSwgKCJjb3JlcyIsICJDUFUgY29yZXMi"
    "KSwKICAgICAgICAgICAgKCJ0b3VjaCIsICJUb3VjaCBwb2ludHMiKSwgKCJjb29raWVzIiwgIkNvb2tpZXMiKSwKICAgICAgICAg"
    "ICAgKCJ3ZWJkcml2ZXIiLCAiQXV0b21hdGlvbiIpLCAoIm5ldCIsICJOZXR3b3JrIiksCiAgICAgICAgKToKICAgICAgICAgICAg"
    "diA9IGRldi5nZXQoaykKICAgICAgICAgICAgaWYgdiBpcyBub3QgTm9uZSBhbmQgdiAhPSAiIjoKICAgICAgICAgICAgICAgIGRl"
    "dl9sLmFwcGVuZChmIuKUoCB7bGJsfToge3N0cih2KVs6ODBdfSIpCiAgICAgICAgbGluZXMgPSBbCiAgICAgICAgICAgICLwn5uh"
    "IDxiPldaRklYIGRhc2hib2FyZDwvYj4iLAogICAgICAgICAgICB0aXRsZSwKICAgICAgICAgICAgIiIsCiAgICAgICAgICAgIGYi"
    "4pSPIDxiPklQPC9iPiDihpIgPGNvZGU+e2lwfTwvY29kZT4iLAogICAgICAgICAgICBmIuKUoyA8Yj5UcmllZDwvYj4g4oaSIDxj"
    "b2RlPntzdHIoc3VibWl0dGVkKVs6NDhdfTwvY29kZT4iLAogICAgICAgIF0KICAgICAgICBpZiBkZXZfbDoKICAgICAgICAgICAg"
    "bGluZXMgKz0gWyLilKMgPGI+RGV2aWNlPC9iPiJdICsgZGV2X2wKICAgICAgICBpZiBoZHJzOgogICAgICAgICAgICBsaW5lcyAr"
    "PSBbIuKUoyA8Yj5IZWFkZXJzPC9iPiJdICsgaGRyc1s6OV0KICAgICAgICBsaW5lcy5hcHBlbmQoIuKUliB2aWEgL2JzIEFETUlO"
    "X0xPR0lOX0FMRVJUUyB0byB0b2dnbGUiKQogICAgICAgIGF3YWl0IFRnQ2xpZW50LmJvdC5zZW5kX21lc3NhZ2UoCiAgICAgICAg"
    "ICAgIGNoYXRfaWQ9Y2hhdCwKICAgICAgICAgICAgdGV4dD0iXG4iLmpvaW4obGluZXMpWzozOTAwXSwKICAgICAgICAgICAgZGlz"
    "YWJsZV93ZWJfcGFnZV9wcmV2aWV3PVRydWUsCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoK"
    "YXN5bmMgZGVmIF9pcF9pbnRlbChpcCk6CiAgICAiIiJEYXRhY2VudGVyL1ZQTiBkZXRlY3Rpb24gdmlhIGlwLWFwaS5jb20gKGZy"
    "ZWUgdGllciwgY2FjaGVkIDcgZGF5cykuCiAgICBSZXR1cm5zIFRydWUgd2hlbiB0aGUgSVAgbG9va3MgbGlrZSBhIHNlcnZlci9w"
    "cm94eSAoLT4gYmFubmVkKS4iIiIKICAgIGlwID0gX25vcm1faXAoaXApCiAgICBpZiBub3QgaXAgb3IgaXAgaW4gKCIxMjcuMC4w"
    "LjEiLCAiOjoxIiwgIj8iLCAibG9jYWxob3N0Iik6CiAgICAgICAgcmV0dXJuIEZhbHNlCiAgICBpZiBub3QgaXAucmVwbGFjZSgi"
    "LiIsICIiKS5pc2RpZ2l0KCk6ICAjIGlwdjYgb3IgdW5rbm93bjogc2tpcCBsb29rdXAKICAgICAgICByZXR1cm4gRmFsc2UKICAg"
    "IHRyeToKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9pcGluZm9bX3BhcnQoKV0KICAgICAgICBjYWNoZWQgPSBhd2FpdCBjb2wu"
    "ZmluZF9vbmUoeyJfaWQiOiBpcH0pCiAgICAgICAgaWYgY2FjaGVkIGFuZCB0aW1lKCkgLSBjYWNoZWQuZ2V0KCJ0cyIsIDApIDwg"
    "SVBfSU5GT19UVEw6CiAgICAgICAgICAgIHJldHVybiBib29sKGNhY2hlZC5nZXQoImJhZCIpKQogICAgICAgIGltcG9ydCBqc29u"
    "IGFzIF9qc29uCiAgICAgICAgZnJvbSB1cmxsaWIucmVxdWVzdCBpbXBvcnQgdXJsb3BlbiwgUmVxdWVzdAoKICAgICAgICB1cmwg"
    "PSAoCiAgICAgICAgICAgICJodHRwOi8vaXAtYXBpLmNvbS9qc29uLyIgKyBpcAogICAgICAgICAgICArICI/ZmllbGRzPXN0YXR1"
    "cyxwcm94eSxob3N0aW5nLG1vYmlsZSxpc3AiCiAgICAgICAgKQogICAgICAgIHJlcSA9IFJlcXVlc3QodXJsLCBoZWFkZXJzPXsi"
    "VXNlci1BZ2VudCI6ICJ3emZpeC1kYXNoYm9hcmQifSkKICAgICAgICB3aXRoIHVybG9wZW4ocmVxLCB0aW1lb3V0PTgpIGFzIHI6"
    "CiAgICAgICAgICAgIGRhdGEgPSBfanNvbi5sb2FkcyhyLnJlYWQoKS5kZWNvZGUoInV0Zi04IiwgInJlcGxhY2UiKSkKICAgICAg"
    "ICBiYWQgPSBkYXRhLmdldCgic3RhdHVzIikgPT0gInN1Y2Nlc3MiIGFuZCAoCiAgICAgICAgICAgIGRhdGEuZ2V0KCJob3N0aW5n"
    "Iikgb3IgZGF0YS5nZXQoInByb3h5IikKICAgICAgICApCiAgICAgICAgdHJ5OgogICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRl"
    "X29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogaXB9LAogICAgICAgICAgICAgICAgeyIkc2V0IjogeyJiYWQiOiBib29sKGJh"
    "ZCksICJpc3AiOiBkYXRhLmdldCgiaXNwIiwgIiIpLAogICAgICAgICAgICAgICAgICAgICAgICAgICJ0cyI6IHRpbWUoKX19LAog"
    "ICAgICAgICAgICAgICAgdXBzZXJ0PVRydWUsCiAgICAgICAgICAgICkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAg"
    "ICAgICBwYXNzCiAgICAgICAgaWYgYmFkOgogICAgICAgICAgICBhd2FpdCBfYmFuX2lwKGlwKQogICAgICAgICAgICB0cnk6CiAg"
    "ICAgICAgICAgICAgICBmcm9tIC4uLiBpbXBvcnQgTE9HR0VSCgogICAgICAgICAgICAgICAgTE9HR0VSLmVycm9yKAogICAgICAg"
    "ICAgICAgICAgICAgIGYiV1pGSVggZGFzaGJvYXJkOiBkYXRhY2VudGVyL3Byb3h5IElQIGJhbm5lZDoge2lwfSAiCiAgICAgICAg"
    "ICAgICAgICAgICAgZiIoe2RhdGEuZ2V0KCdpc3AnLCAnPycpfSkiCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgIGV4Y2Vw"
    "dCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICBwYXNzCiAgICAgICAgcmV0dXJuIGJvb2woYmFkKQogICAgZXhjZXB0IEV4Y2Vw"
    "dGlvbjoKICAgICAgICByZXR1cm4gRmFsc2UgICMgaW50ZWwgdW5hdmFpbGFibGUgLT4gZG8gbm90IGJsb2NrCgoKYXN5bmMgZGVm"
    "IF9sb2Nrb3V0X3N0YXRlKGlwKToKICAgICIiIlBlcnNpc3RlbnQgdGllci1iYXNlZCBsb2Nrb3V0LiBSZXR1cm5zIHNlY29uZHMg"
    "cmVtYWluaW5nICgwID0gb3BlbikuIiIiCiAgICB0cnk6CiAgICAgICAgY29sID0gX2RiKCkud3pmaXhfbG9ja3NbX3BhcnQoKV0K"
    "ICAgICAgICBkb2MgPSBhd2FpdCBjb2wuZmluZF9vbmUoeyJfaWQiOiBfbm9ybV9pcChpcCl9KQogICAgICAgIGlmIG5vdCBkb2M6"
    "CiAgICAgICAgICAgIHJldHVybiAwCiAgICAgICAgaWYgZG9jLmdldCgicGVybSIpOgogICAgICAgICAgICByZXR1cm4gLTEKICAg"
    "ICAgICB1bnRpbCA9IGRvYy5nZXQoInVudGlsIikgb3IgMAogICAgICAgIHJldHVybiBtYXgodW50aWwgLSB0aW1lKCksIDApCiAg"
    "ICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgIHJldHVybiAwCgoKYXN5bmMgZGVmIF9yZWNvcmRfZmFpbChpcCk6CiAgICAiIiIz"
    "IGZhaWxzIC0+IGVzY2FsYXRpbmcgbG9ja291dDogMjRoLCA3MmgsIDdkLCAzMGQsIHBlcm1hbmVudC4iIiIKICAgIHRyeToKICAg"
    "ICAgICBmcm9tIHB5bW9uZ28gaW1wb3J0IFJldHVybkRvY3VtZW50IGFzIF9SRAoKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9s"
    "b2Nrc1tfcGFydCgpXQogICAgICAgIGRvYyA9IGF3YWl0IGNvbC5maW5kX29uZV9hbmRfdXBkYXRlKAogICAgICAgICAgICB7Il9p"
    "ZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgIHsiJGluYyI6IHsiZmFpbHMiOiAxfSwgIiRzZXQiOiB7InRzIjogdGltZSgp"
    "fX0sCiAgICAgICAgICAgIHVwc2VydD1UcnVlLAogICAgICAgICAgICByZXR1cm5fZG9jdW1lbnQ9X1JELkFGVEVSLAogICAgICAg"
    "ICkKICAgICAgICBmYWlscyA9IGludCgoZG9jIG9yIHt9KS5nZXQoImZhaWxzIikgb3IgMCkKICAgICAgICBleHRyYSA9IGludCgo"
    "ZG9jIG9yIHt9KS5nZXQoImV4dHJhIikgb3IgMCkKICAgICAgICBpZiBleHRyYSA+IDA6CiAgICAgICAgICAgICMgcmV0cmllcyBn"
    "cmFudGVkIGJ5IC9hbGxvdzogZWFjaCBmYWlsIGNvbnN1bWVzIG9uZTsgdGhlIGxhc3QKICAgICAgICAgICAgIyBvbmUgZXNjYWxh"
    "dGVzIHN0cmFpZ2h0IHRvIHRoZSBORVhUIHRpZXIgKGtlcHQgYWZ0ZXIgL2FsbG93KQogICAgICAgICAgICBsZWZ0ID0gZXh0cmEg"
    "LSAxCiAgICAgICAgICAgIHRpZXIgPSAoaW50KChkb2Mgb3Ige30pLmdldCgidGllciIpIG9yIDApKSArIDEKICAgICAgICAgICAg"
    "c2VjcyA9IExPQ0tfVElFUlNfU1ttaW4odGllciAtIDEsIGxlbihMT0NLX1RJRVJTX1MpIC0gMSldCiAgICAgICAgICAgIGlmIGxl"
    "ZnQgPD0gMDoKICAgICAgICAgICAgICAgIGlmIHNlY3MgPCAwOgogICAgICAgICAgICAgICAgICAgIGF3YWl0IGNvbC51cGRhdGVf"
    "b25lKAogICAgICAgICAgICAgICAgICAgICAgICB7Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgICAgICAgICAgICAg"
    "IHsiJHNldCI6IHsicGVybSI6IFRydWUsICJmYWlscyI6IDAsICJ0aWVyIjogdGllciwgImV4dHJhIjogMH19LAogICAgICAgICAg"
    "ICAgICAgICAgICkKICAgICAgICAgICAgICAgICAgICByZXR1cm4gLTEsIHRpZXIKICAgICAgICAgICAgICAgIGF3YWl0IGNvbC51"
    "cGRhdGVfb25lKAogICAgICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXApfSwKICAgICAgICAgICAgICAgICAgICB7"
    "IiRzZXQiOiB7InVudGlsIjogdGltZSgpICsgc2VjcywgImZhaWxzIjogMCwgInRpZXIiOiB0aWVyLAogICAgICAgICAgICAgICAg"
    "ICAgICAgICAgICAgICAiZXh0cmEiOiAwfX0sCiAgICAgICAgICAgICAgICApCiAgICAgICAgICAgICAgICByZXR1cm4gc2Vjcywg"
    "dGllcgogICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXAp"
    "fSwKICAgICAgICAgICAgICAgIHsiJHNldCI6IHsiZmFpbHMiOiAwLCAiZXh0cmEiOiBsZWZ0fX0sCiAgICAgICAgICAgICkKICAg"
    "ICAgICAgICAgcmV0dXJuIDAsIGxlZnQKICAgICAgICBpZiBmYWlscyA8IExPR0lOX01BWF9GQUlMUzoKICAgICAgICAgICAgcmV0"
    "dXJuIDAsIGZhaWxzCiAgICAgICAgdGllciA9IChpbnQoKGRvYyBvciB7fSkuZ2V0KCJ0aWVyIikgb3IgMCkpICsgMQogICAgICAg"
    "IHNlY3MgPSBMT0NLX1RJRVJTX1NbbWluKHRpZXIgLSAxLCBsZW4oTE9DS19USUVSU19TKSAtIDEpXQogICAgICAgIGlmIHNlY3Mg"
    "PCAwOgogICAgICAgICAgICBhd2FpdCBjb2wudXBkYXRlX29uZSgKICAgICAgICAgICAgICAgIHsiX2lkIjogX25vcm1faXAoaXAp"
    "fSwKICAgICAgICAgICAgICAgIHsiJHNldCI6IHsicGVybSI6IFRydWUsICJmYWlscyI6IDAsICJ0aWVyIjogdGllcn19LAogICAg"
    "ICAgICAgICApCiAgICAgICAgICAgIHJldHVybiAtMSwgdGllcgogICAgICAgIGF3YWl0IGNvbC51cGRhdGVfb25lKAogICAgICAg"
    "ICAgICB7Il9pZCI6IF9ub3JtX2lwKGlwKX0sCiAgICAgICAgICAgIHsiJHNldCI6IHsidW50aWwiOiB0aW1lKCkgKyBzZWNzLCAi"
    "ZmFpbHMiOiAwLCAidGllciI6IHRpZXJ9fSwKICAgICAgICApCiAgICAgICAgcmV0dXJuIHNlY3MsIHRpZXIKICAgIGV4Y2VwdCBF"
    "eGNlcHRpb246CiAgICAgICAgcmV0dXJuIDAsIDAKCgpkZWYgX3JlY29yZF9vayhpcCk6CiAgICB0cnk6CiAgICAgICAgZnJvbSBh"
    "c3luY2lvIGltcG9ydCBnZXRfZXZlbnRfbG9vcAoKICAgICAgICBjb2wgPSBfZGIoKS53emZpeF9sb2Nrc1tfcGFydCgpXQogICAg"
    "ICAgIGdldF9ldmVudF9sb29wKCkuY3JlYXRlX3Rhc2soCiAgICAgICAgICAgIGNvbC51cGRhdGVfb25lKAogICAgICAgICAgICAg"
    "ICAgeyJfaWQiOiBfbm9ybV9pcChpcCl9LAogICAgICAgICAgICAgICAgeyIkc2V0IjogeyJmYWlscyI6IDAsICJ0aWVyIjogMCwg"
    "InVudGlsIjogMH19LAogICAgICAgICAgICApCiAgICAgICAgKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCgoK"
    "ZGVmIF9sb2NrX3R4dChzZWNzKToKICAgIGlmIHNlY3MgPCAwOgogICAgICAgIHJldHVybiAicGVybWFuZW50bHkgYmFubmVkIgog"
    "ICAgaWYgc2VjcyA+PSA4NjQwMDoKICAgICAgICByZXR1cm4gZiJsb2NrZWQgZm9yIHtzZWNzIC8vIDg2NDAwfSBkYXkocykiCiAg"
    "ICByZXR1cm4gZiJsb2NrZWQgZm9yIHtzZWNzIC8vIDM2MDAgKyAxfSBob3VyKHMpIgoKCiMg4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA"
    "4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACiMgU3RhdGUgYnVpbGRpbmcgKHVzZXJzLCBs"
    "aXZlIHRhc2tzLCBnbG9iYWxzKQojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgAoKCmRlZiBfdGFza3Nfc25hcHNob3QoKToKICAgIG91dCA9IFtdCiAgICB0cnk6CiAgICAgICAgZnJv"
    "bSAuLi4gaW1wb3J0IHRhc2tfZGljdAoKICAgICAgICBkZWYgc2FmZShmbiwgZGVmYXVsdD0iIik6CiAgICAgICAgICAgIHRyeToK"
    "ICAgICAgICAgICAgICAgIHYgPSBmbigpCiAgICAgICAgICAgICAgICByZXR1cm4gdiBpZiBpc2luc3RhbmNlKHYsIChpbnQsIGZs"
    "b2F0LCBzdHIpKSBlbHNlIGRlZmF1bHQKICAgICAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIHJldHVy"
    "biBkZWZhdWx0CgogICAgICAgIGZvciBtaWQsIHQgaW4gbGlzdCh0YXNrX2RpY3QuaXRlbXMoKSk6CiAgICAgICAgICAgIHRyeToK"
    "ICAgICAgICAgICAgICAgIGxzdCA9IGdldGF0dHIodCwgImxpc3RlbmVyIiwgTm9uZSkKICAgICAgICAgICAgICAgIG91dC5hcHBl"
    "bmQoCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAibWlkIjogbWlkLAogICAgICAgICAgICAg"
    "ICAgICAgICAgICAiZ2lkIjogc2FmZShsYW1iZGE6IHQuZ2lkKCksICIiKSwKICAgICAgICAgICAgICAgICAgICAgICAgIm5hbWUi"
    "OiBzdHIoc2FmZShsYW1iZGE6IHQubmFtZSgpLCAidGFzayIpKVs6OTBdLAogICAgICAgICAgICAgICAgICAgICAgICAidWlkIjog"
    "aW50KHNhZmUobGFtYmRhOiBsc3QudXNlcl9pZCwgMCkgb3IgMCksCiAgICAgICAgICAgICAgICAgICAgICAgICJ0YWciOiBzdHIo"
    "c2FmZShsYW1iZGE6IGxzdC50YWcsICIiKSBvciAiIiksCiAgICAgICAgICAgICAgICAgICAgICAgICJzaXplIjogaW50KHNhZmUo"
    "bGFtYmRhOiB0LnNpemUoKSwgMCkgb3IgMCksCiAgICAgICAgICAgICAgICAgICAgICAgICJwcm9jIjogaW50KHNhZmUobGFtYmRh"
    "OiB0LnByb2Nlc3NlZF9ieXRlcygpLCAwKSBvciAwKSwKICAgICAgICAgICAgICAgICAgICAgICAgInBjdCI6IHNhZmUobGFtYmRh"
    "OiBmbG9hdChzdHIodC5wcm9ncmVzcygpKS5yc3RyaXAoIiUiKSksIDAuMCksCiAgICAgICAgICAgICAgICAgICAgICAgICJzcGVl"
    "ZCI6IHN0cihzYWZlKGxhbWJkYTogdC5zcGVlZCgpLCAiIikpLAogICAgICAgICAgICAgICAgICAgICAgICAiZXRhIjogc3RyKHNh"
    "ZmUobGFtYmRhOiB0LmV0YSgpLCAiIikpLAogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICkKICAgICAgICAg"
    "ICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgb3V0LnNvcnQoa2V5PWxhbWJkYSB4"
    "OiAteFsic2l6ZSJdKQogICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICBwYXNzCiAgICByZXR1cm4gb3V0CgoKYXN5bmMgZGVm"
    "IF9ib3RfaW5mbygpOgogICAgb3V0ID0geyJuYW1lIjogIiIsICJ1bmFtZSI6ICIifQogICAgdHJ5OgogICAgICAgIGZyb20gLi4u"
    "Y29yZS50Z19jbGllbnQgaW1wb3J0IFRnQ2xpZW50CgogICAgICAgIG1lID0gYXdhaXQgVGdDbGllbnQuYm90LmdldF9tZSgpCiAg"
    "ICAgICAgb3V0WyJuYW1lIl0gPSBtZS5maXJzdF9uYW1lIG9yICIiCiAgICAgICAgb3V0WyJ1bmFtZSJdID0gbWUudXNlcm5hbWUg"
    "b3IgIiIKICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgcGFzcwogICAgcmV0dXJuIG91dAoKCmFzeW5jIGRlZiBfc3RhdGUo"
    "KToKICAgIHJvd3MgPSBhd2FpdCB0b2RheV9yb3dzKCkKICAgIHVzZWRfYnkgPSB7clsidXNlcl9pZCJdOiBpbnQoci5nZXQoInVz"
    "ZWQiKSBvciAwKSBmb3IgciBpbiByb3dzfQogICAgZG9jcyA9IGF3YWl0IGFsbF91c2VycygpCiAgICB1c2VycyA9IFtdCiAgICBm"
    "b3IgZCBpbiBkb2NzOgogICAgICAgIHRyeToKICAgICAgICAgICAgdWlkID0gaW50KGRbIl9pZCJdKQogICAgICAgIGV4Y2VwdCBF"
    "eGNlcHRpb246CiAgICAgICAgICAgIGNvbnRpbnVlCiAgICAgICAgY2FwID0gYXdhaXQgZ2V0X2NhcF9ieXRlcyh1aWQpCiAgICAg"
    "ICAgdXNlZCA9IHVzZWRfYnkuZ2V0KHVpZCwgMCkKICAgICAgICByZXMgPSBhd2FpdCByZXNlcnZlZF90b2RheSh1aWQpCiAgICAg"
    "ICAgdXNlcnMuYXBwZW5kKAogICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAidWlkIjogdWlkLAogICAgICAgICAgICAgICAg"
    "InVuYW1lIjogZC5nZXQoInVuYW1lIikgb3IgIiIsCiAgICAgICAgICAgICAgICAibmFtZSI6IGQuZ2V0KCJuYW1lIikgb3IgIiIs"
    "CiAgICAgICAgICAgICAgICAiY2FwX2diIjogZC5nZXQoImNhcF9nYiIpLAogICAgICAgICAgICAgICAgImNhcCI6IGNhcCwKICAg"
    "ICAgICAgICAgICAgICJ1c2VkIjogdXNlZCwKICAgICAgICAgICAgICAgICJyZXNlcnZlZCI6IHJlcywKICAgICAgICAgICAgICAg"
    "ICJwY3QiOiBtaW4oMTAwLjAsIHJvdW5kKDEwMC4wICogKHVzZWQgKyByZXMpIC8gY2FwLCAxKSkgaWYgY2FwIGVsc2UgMC4wLAog"
    "ICAgICAgICAgICAgICAgImJhbm5lZCI6IGQuZ2V0KCJjYXBfZ2IiKSA9PSAwLAogICAgICAgICAgICAgICAgInRhc2tzIjogaW50"
    "KGQuZ2V0KCJ0YXNrcyIpIG9yIDApLAogICAgICAgICAgICAgICAgInRvdGFsIjogaW50KGQuZ2V0KCJ0b3RhbF91c2VkIikgb3Ig"
    "MCksCiAgICAgICAgICAgIH0KICAgICAgICApCiAgICB1c2Vycy5zb3J0KGtleT1sYW1iZGEgdTogLSh1WyJ1c2VkIl0gKyB1WyJy"
    "ZXNlcnZlZCJdKSkKICAgIHJldHVybiB7CiAgICAgICAgIm9rIjogVHJ1ZSwKICAgICAgICAiZGF5IjogX2RheV9pc3QoKSwKICAg"
    "ICAgICAiYm90IjogYXdhaXQgX2JvdF9pbmZvKCksCiAgICAgICAgInVzZXJzIjogdXNlcnMsCiAgICAgICAgInRhc2tzIjogX3Rh"
    "c2tzX3NuYXBzaG90KCksCiAgICAgICAgImdsb2JhbF9jYXBfZ2IiOiBhd2FpdCBfZ2V0X2dsb2JhbF9jYXBfZ2IoKSwKICAgICAg"
    "ICAidG90YWxzIjogewogICAgICAgICAgICAidXNlZCI6IHN1bSh1WyJ1c2VkIl0gZm9yIHUgaW4gdXNlcnMpLAogICAgICAgICAg"
    "ICAicmVzZXJ2ZWQiOiBzdW0odVsicmVzZXJ2ZWQiXSBmb3IgdSBpbiB1c2VycyksCiAgICAgICAgICAgICJ1c2VycyI6IGxlbih1"
    "c2VycyksCiAgICAgICAgICAgICJ0YXNrcyI6IGxlbihfdGFza3Nfc25hcHNob3QoKSksCiAgICAgICAgfSwKICAgIH0KCgojIOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgAojIEFjdGlv"
    "bnMKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAK"
    "Cgphc3luYyBkZWYgX2tpbGxfdGFzayhtaWQpOgogICAgZnJvbSAuLi4gaW1wb3J0IHRhc2tfZGljdAoKICAgIHRyeToKICAgICAg"
    "ICB0ID0gdGFza19kaWN0LmdldChpbnQobWlkKSkKICAgICAgICBpZiB0IGlzIE5vbmU6CiAgICAgICAgICAgIHJldHVybiBGYWxz"
    "ZSwgInRhc2sgbm90IGZvdW5kIgogICAgICAgIG9iaiA9IHQudGFzaygpCiAgICAgICAgYXdhaXQgb2JqLmNhbmNlbF90YXNrKCkK"
    "ICAgICAgICByZXR1cm4gVHJ1ZSwgImNhbmNlbGxlZCIKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICByZXR1cm4g"
    "RmFsc2UsIHN0cihlKVs6MTIwXQoKCmFzeW5jIGRlZiBfZGVkdWN0X2NhcCh1aWQsIGdiKToKICAgICIiIkxvd2VyIGEgdXNlcidz"
    "IGNhcCBieSBHQiAoZmxvb3IgMCA9IGZ1bGx5IGJsb2NrZWQpIOKAlCBzYW1lIGFzCiAgICBUZWxlZ3JhbSAvZGVkdWN0Y2FwLiIi"
    "IgogICAgdHJ5OgogICAgICAgIGFtdCA9IGZsb2F0KGdiKQogICAgICAgIHVkb2MgPSBhd2FpdCBnZXRfdXNlcl9kb2ModWlkKQog"
    "ICAgICAgIGN1ciA9IHVkb2MuZ2V0KCJjYXBfZ2IiKQogICAgICAgIGJhc2UgPSBmbG9hdChjdXIpIGlmIGN1ciBpcyBub3QgTm9u"
    "ZSBlbHNlIGF3YWl0IF9nZXRfZ2xvYmFsX2NhcF9nYigpCiAgICAgICAgbmV3X2NhcCA9IG1heChiYXNlIC0gYW10LCAwLjApCiAg"
    "ICAgICAgYXdhaXQgc2V0X3VzZXJfY2FwKHVpZCwgbmV3X2NhcCkKICAgICAgICBpZiBuZXdfY2FwIDw9IDA6CiAgICAgICAgICAg"
    "IHJldHVybiBUcnVlLCBmImNhcCBmb3Ige3VpZH0g4oaSIDAgR0IgKGJsb2NrZWQpIgogICAgICAgIHJldHVybiBUcnVlLCBmImNh"
    "cCBmb3Ige3VpZH0g4oaSIHtfZm10X2diKG5ld19jYXApfSIKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZToKICAgICAgICByZXR1"
    "cm4gRmFsc2UsIHN0cihlKVs6MTIwXQoKCmFzeW5jIGRlZiBidWlsZF9yZXBvcnQoKToKICAgICIiIkRhaWx5IHVzYWdlIHRleHQg"
    "Zm9yIHRoZSBsb2cgY2hhdC4iIiIKICAgIHJvd3MgPSBhd2FpdCB0b2RheV9yb3dzKCkKICAgIGRvY3MgPSB7aW50KGRbIl9pZCJd"
    "KTogZCBmb3IgZCBpbiBhd2FpdCBhbGxfdXNlcnMoKX0KICAgIGxpbmVzID0gW2Yi8J+TiiA8Yj5XWkZJWCBkYWlseSByZXBvcnQg"
    "4oCUIHtfZGF5X2lzdCgpfTwvYj4iLCAi4pSCIl0KICAgIHRvdGFsID0gMAogICAgZm9yIHIgaW4gcm93c1s6MjVdOgogICAgICAg"
    "IHVpZCA9IHJbInVzZXJfaWQiXQogICAgICAgIGQgPSBkb2NzLmdldCh1aWQsIHt9KQogICAgICAgIHRhZyA9IGQuZ2V0KCJ1bmFt"
    "ZSIpIG9yIGQuZ2V0KCJuYW1lIikgb3Igc3RyKHVpZCkKICAgICAgICBjYXBfZ2IgPSBkLmdldCgiY2FwX2diIikKICAgICAgICBj"
    "YXBfdHh0ID0gZiJ7X2ZtdF9nYihjYXBfZ2IpfSIgaWYgY2FwX2diIGVsc2UgImRlZmF1bHQiCiAgICAgICAgbGluZXMuYXBwZW5k"
    "KGYi4pSgIDxiPnt0YWd9PC9iPiDihpIge19uaWNlX3NpemUoclsndXNlZCddKX0gLyB7Y2FwX3R4dH0iKQogICAgICAgIHRvdGFs"
    "ICs9IHIuZ2V0KCJ1c2VkIikgb3IgMAogICAgaWYgbm90IHJvd3M6CiAgICAgICAgbGluZXMuYXBwZW5kKCLilJYgbm8gdXNhZ2Ug"
    "dG9kYXkiKQogICAgZWxzZToKICAgICAgICBsaW5lc1sxXSA9IGYi4pSCIHRvdGFsIDxiPntfbmljZV9zaXplKHRvdGFsKX08L2I+"
    "IMK3IHtsZW4ocm93cyl9IHVzZXIocykiCiAgICAgICAgbGluZXMuYXBwZW5kKCLilJYgcmVzZXRzIGF0IDAwOjAwIElTVCIpCiAg"
    "ICByZXR1cm4gIlxuIi5qb2luKGxpbmVzKVs6UkVQT1JUX01BWF9DSEFSU10KCgphc3luYyBkZWYgc2VuZF9yZXBvcnQoKToKICAg"
    "IHRyeToKICAgICAgICBmcm9tIC4uLmNvcmUuY29uZmlnX21hbmFnZXIgaW1wb3J0IENvbmZpZwogICAgICAgIGZyb20gLi4uY29y"
    "ZS50Z19jbGllbnQgaW1wb3J0IFRnQ2xpZW50CgogICAgICAgIGNoYXQgPSBzdHIoZ2V0YXR0cihDb25maWcsICJMT0dfQ0hBVCIs"
    "ICIiKSBvciAiIikuc3RyaXAoKQogICAgICAgIGlmIG5vdCBjaGF0OgogICAgICAgICAgICAjIExPR19DSEFUIG5vdCBzZXQgLT4g"
    "ZGVsaXZlciB0byB0aGUgb3duZXIncyBETSBpbnN0ZWFkCiAgICAgICAgICAgIGNoYXQgPSBzdHIoZ2V0YXR0cihDb25maWcsICJP"
    "V05FUl9JRCIsICIiKSBvciAiIikuc3RyaXAoKQogICAgICAgICAgICBpZiBub3QgY2hhdDoKICAgICAgICAgICAgICAgIHJldHVy"
    "biBGYWxzZSwgIkxPR19DSEFUIGFuZCBPV05FUl9JRCBub3Qgc2V0IgogICAgICAgIHRleHQgPSBhd2FpdCBidWlsZF9yZXBvcnQo"
    "KQogICAgICAgIGF3YWl0IFRnQ2xpZW50LmJvdC5zZW5kX21lc3NhZ2UoCiAgICAgICAgICAgIGNoYXRfaWQ9Y2hhdCwgdGV4dD10"
    "ZXh0LCBkaXNhYmxlX3dlYl9wYWdlX3ByZXZpZXc9VHJ1ZQogICAgICAgICkKICAgICAgICByZXR1cm4gVHJ1ZSwgInJlcG9ydCBz"
    "ZW50IgogICAgZXhjZXB0IEV4Y2VwdGlvbiBhcyBlOgogICAgICAgIHJldHVybiBGYWxzZSwgc3RyKGUpWzoxMjBdCgoKYXN5bmMg"
    "ZGVmIGRhaWx5X3JlcG9ydF9sb29wKCk6CiAgICAiIiJTZW5kIHRoZSB1c2FnZSByZXBvcnQgdG8gdGhlIGxvZyBjaGF0IGF0IDIz"
    "OjU3IElTVCBldmVyeSBkYXkuIiIiCiAgICB3aGlsZSBUcnVlOgogICAgICAgIHRyeToKICAgICAgICAgICAgbm93ID0gZGF0ZXRp"
    "bWUubm93KElTVCkKICAgICAgICAgICAgdGFyZ2V0ID0gbm93LnJlcGxhY2UoaG91cj0yMywgbWludXRlPTU3LCBzZWNvbmQ9MCwg"
    "bWljcm9zZWNvbmQ9MCkKICAgICAgICAgICAgaWYgdGFyZ2V0IDw9IG5vdzoKICAgICAgICAgICAgICAgIHRhcmdldCArPSB0aW1l"
    "ZGVsdGEoZGF5cz0xKQogICAgICAgICAgICBhd2FpdCBhaW9zbGVlcCgodGFyZ2V0IC0gbm93KS50b3RhbF9zZWNvbmRzKCkgKyA1"
    "KQogICAgICAgICAgICBvaywgbXNnID0gYXdhaXQgc2VuZF9yZXBvcnQoKQogICAgICAgICAgICBpZiBub3Qgb2s6CiAgICAgICAg"
    "ICAgICAgICBmcm9tIC4uLiBpbXBvcnQgTE9HR0VSCgogICAgICAgICAgICAgICAgTE9HR0VSLndhcm5pbmcoZiJXWkZJWCBkYWls"
    "eSByZXBvcnQgZmFpbGVkOiB7bXNnfSIpCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgYXdhaXQgYWlvc2xl"
    "ZXAoMzYwMCkKCgojIGNvbW1hbmQgZGVzY3JpcHRpb25zIGZvciB0aGUgVGVsZWdyYW0gIi8iIG1lbnUgKHYxNS45KQpfQ01EX0RF"
    "U0MgPSB7CiAgICAic3RhcnQiOiAiU3RhcnQgdGhlIGJvdCIsICJoZWxwIjogIkNvbW1hbmQgaGVscCIsCiAgICAibG9naW4iOiAi"
    "TG9naW4gdG9rZW4iLCAicGluZyI6ICJCb3QgbGF0ZW5jeSIsCiAgICAibWlycm9yIjogIk1pcnJvciBhIGxpbmsgdG8gY2xvdWQi"
    "LCAibSI6ICJNaXJyb3IgYSBsaW5rIChzaG9ydCkiLAogICAgImxlZWNoIjogIkRvd25sb2FkICYgdXBsb2FkIHRvIFRlbGVncmFt"
    "IiwgImwiOiAiTGVlY2ggYSBsaW5rIChzaG9ydCkiLAogICAgInFibWlycm9yIjogIk1pcnJvciB2aWEgcUJpdHRvcnJlbnQiLCAi"
    "cW0iOiAicUJpdCBtaXJyb3IgKHNob3J0KSIsCiAgICAicWJsZWVjaCI6ICJMZWVjaCB2aWEgcUJpdHRvcnJlbnQiLCAicWwiOiAi"
    "cUJpdCBsZWVjaCAoc2hvcnQpIiwKICAgICJ5dGRsIjogIllULURMUCBtaXJyb3IiLCAieSI6ICJZVC1ETFAgbWlycm9yIChzaG9y"
    "dCkiLAogICAgInl0ZGxsZWVjaCI6ICJZVC1ETFAgbGVlY2giLCAieWwiOiAiWVQtRExQIGxlZWNoIChzaG9ydCkiLAogICAgImpk"
    "bWlycm9yIjogIkpEb3dubG9hZGVyIG1pcnJvciIsICJqbSI6ICJKRG93bmxvYWRlciBtaXJyb3IgKHNob3J0KSIsCiAgICAiamRs"
    "ZWVjaCI6ICJKRG93bmxvYWRlciBsZWVjaCIsICJqbCI6ICJKRG93bmxvYWRlciBsZWVjaCAoc2hvcnQpIiwKICAgICJuemJtaXJy"
    "b3IiOiAiTlpCIG1pcnJvciIsICJubSI6ICJOWkIgbWlycm9yIChzaG9ydCkiLAogICAgIm56YmxlZWNoIjogIk5aQiBsZWVjaCIs"
    "ICJubCI6ICJOWkIgbGVlY2ggKHNob3J0KSIsCiAgICAic2VlZHJsaW5rIjogIlNlZWRyIGxpbmsgdHJhbnNmZXIiLCAic2xpbmsi"
    "OiAiU2VlZHIgbGluayAoc2hvcnQpIiwKICAgICJzcmxpbmsiOiAiU2VlZHIgbGluayAoc2hvcnQpIiwKICAgICJjbG9uZSI6ICJD"
    "bG9uZSBjbG91ZCB0cmFuc2ZlcnMiLCAiY2wiOiAiQ2xvbmUgKHNob3J0KSIsCiAgICAiY291bnQiOiAiQ291bnQgY2xvdWQgZmls"
    "ZXMvZm9sZGVycyIsICJkZWwiOiAiRGVsZXRlIGNsb3VkIGZpbGVzIiwKICAgICJsaXN0IjogIkxpc3QgY2xvdWQgZmlsZXMiLCAi"
    "c2VhcmNoIjogIlNlYXJjaCBmaWxlcyIsCiAgICAidXNlcnMiOiAiQXV0aG9yaXplZCB1c2VycyBsaXN0IiwKICAgICJjYW5jZWwi"
    "OiAiQ2FuY2VsIGEgdGFzayIsICJjIjogIkNhbmNlbCBhIHRhc2sgKHNob3J0KSIsCiAgICAiY2FuY2VsYWxsIjogIkNhbmNlbCB0"
    "YXNrcyBpbiBidWxrIiwgImNhbGwiOiAiQ2FuY2VsIGFsbCAoc2hvcnQpIiwKICAgICJmb3JjZXN0YXJ0IjogIkZvcmNlIHN0YXJ0"
    "IGEgcXVldWVkIHRhc2siLCAiZnMiOiAiRm9yY2Ugc3RhcnQgKHNob3J0KSIsCiAgICAic3RhdHVzIjogIkFjdGl2ZSB0YXNrIHN0"
    "YXR1cyIsICJzIjogIlN0YXR1cyAoc2hvcnQpIiwKICAgICJzdGF0dXNhbGwiOiAiU3RhdHVzIG9mIGFsbCB1c2VycyIsCiAgICAi"
    "c3RyZWFtIjogIlN0cmVhbSBsaW5rIGZvciBhIGZpbGUiLCAic2wiOiAiU3RyZWFtIGxpbmsgKHNob3J0KSIsCiAgICAicmVzdGFy"
    "dCI6ICJSZXN0YXJ0IHRoZSBib3QiLCAiciI6ICJSZXN0YXJ0IChzaG9ydCkiLAogICAgInJlc3RhcnRhbGwiOiAiUmVzdGFydCBh"
    "bGwgYm90cyIsICJyZXN0YXJ0c2VzIjogIlJlc3RhcnQgc2Vzc2lvbnMiLAogICAgImJyb2FkY2FzdCI6ICJCcm9hZGNhc3QgYSBt"
    "ZXNzYWdlIiwgImJjIjogIkJyb2FkY2FzdCAoc2hvcnQpIiwKICAgICJzdGF0cyI6ICJTZXJ2ZXIgc3RhdHMiLCAic3QiOiAiU3Rh"
    "dHMgKHNob3J0KSIsCiAgICAibG9nIjogIkJvdCBsb2cgZmlsZSIsICJzaGVsbCI6ICJSdW4gYSBzaGVsbCBjb21tYW5kIiwKICAg"
    "ICJhZXhlYyI6ICJSdW4gYXN5bmMgcHl0aG9uIiwgImV4ZWMiOiAiUnVuIHB5dGhvbiIsCiAgICAiY2xlYXJsb2NhbHMiOiAiQ2xl"
    "YXIgc3RvcmVkIHZhcnMiLAogICAgInJzcyI6ICJSU1MgZmVlZCBtYW5hZ2VyIiwKICAgICJhZGRpbWFnZSI6ICJBZGQgYSBjdXN0"
    "b20gaW1hZ2UiLCAiYWkiOiAiQWRkIGltYWdlIChzaG9ydCkiLAogICAgImltYWdlcyI6ICJMaXN0IGN1c3RvbSBpbWFnZXMiLCAi"
    "aW1nIjogIkltYWdlcyAoc2hvcnQpIiwKICAgICJhdXRob3JpemUiOiAiQXV0aG9yaXplIGEgdXNlciIsICJhIjogIkF1dGhvcml6"
    "ZSAoc2hvcnQpIiwKICAgICJ1bmF1dGhvcml6ZSI6ICJVbmF1dGhvcml6ZSBhIHVzZXIiLCAidWEiOiAiVW5hdXRob3JpemUgKHNo"
    "b3J0KSIsCiAgICAiYWRkc3VkbyI6ICJHcmFudCBzdWRvIGFjY2VzcyIsICJhcyI6ICJBZGQgc3VkbyAoc2hvcnQpIiwKICAgICJy"
    "bXN1ZG8iOiAiUmV2b2tlIHN1ZG8gYWNjZXNzIiwgInJzIjogIlJtIHN1ZG8gKHNob3J0KSIsCiAgICAiYmxhY2tsaXN0IjogIkJs"
    "YWNrbGlzdCBhIHVzZXIiLCAiYmwiOiAiQmxhY2tsaXN0IChzaG9ydCkiLAogICAgInJtYmxhY2tsaXN0IjogIlVuLWJsYWNrbGlz"
    "dCBhIHVzZXIiLCAicmJsIjogIlJtIGJsYWNrbGlzdCAoc2hvcnQpIiwKICAgICJic2V0dGluZyI6ICJCb3Qgc2V0dGluZ3MiLCAi"
    "YnMiOiAiQm90IHNldHRpbmdzIChzaG9ydCkiLAogICAgInVzZXR0aW5nIjogIlVzZXIgc2V0dGluZ3MiLCAidXMiOiAiVXNlciBz"
    "ZXR0aW5ncyAoc2hvcnQpIiwKICAgICJzZWxlY3QiOiAiU2VsZWN0IHRvcnJlbnQgZmlsZXMiLCAic2VsIjogIlNlbGVjdCAoc2hv"
    "cnQpIiwKICAgICJjYXRlZ29yeSI6ICJDYXRlZ29yeSBzZWxlY3RvciIsICJjdHNlbCI6ICJDYXRlZ29yeSAoc2hvcnQpIiwKICAg"
    "ICJnZGNsZWFuIjogIkNsZWFuIGNsb3VkIGRyaXZlIiwgImdkYyI6ICJHRENsZWFuIChzaG9ydCkiLAogICAgInBsdWdpbnMiOiAi"
    "TWFuYWdlIHBsdWdpbnMiLAogICAgIm1lbW9yeSI6ICJTaG93IGJvdCBtZW1vcnkiLCAibWVtIjogIk1lbW9yeSAoc2hvcnQpIiwK"
    "ICAgICJ1cGhvc3RlciI6ICJVcGxvYWQgZnJvbSBVUkwiLCAidXAiOiAiVXBsb2FkIChzaG9ydCkiLAogICAgImZpbmQiOiAiU2Vh"
    "cmNoIHlvdXIgZG93bmxvYWRzIiwgInVzYWdlIjogIllvdXIgYmFuZHdpZHRoIHVzYWdlIiwKfQpfT1dORVJfREVTQyA9IHsKICAg"
    "ICJxdXNlcnMiOiAiVXNlcnMgKyB1c2FnZSBvdmVydmlldyIsICJzZXRjYXAiOiAiU2V0IGEgdXNlciBjYXAiLAogICAgImFkZGNh"
    "cCI6ICJSYWlzZSBhIHVzZXIgY2FwIiwgImRlZHVjdGNhcCI6ICJMb3dlciBhIHVzZXIgY2FwIiwKICAgICJkZWx1c2VyIjogIlJl"
    "bW92ZSBhIHVzZXIiLCAicmVzZXRjYXAiOiAiUmVzZXQgdG9kYXkncyB1c2FnZSIsCiAgICAiYm90Y2FwIjogIkRlZmF1bHQgY2Fw"
    "IGZvciBhbGwiLCAiZGJzdGF0cyI6ICJNb25nb0RCIHNpemVzIiwKICAgICJkYmNsZWFuIjogIkNsZWFuIGV4cGlyZWQgREIgcm93"
    "cyIsICJ3emZpeGRpYWciOiAiUXVvdGEgZGlhZ25vc3RpY3MiLAogICAgImFkbWlucGFzcyI6ICJXZWIgZGFzaGJvYXJkIHBhc3N3"
    "b3JkIiwgImFsbG93IjogIlVuYmFuIGEgZGFzaGJvYXJkIElQIiwKICAgICJiYW5zIjogIkxpc3QgZGFzaGJvYXJkIGJhbnMiLCAi"
    "bG9ja2Rhc2giOiAiTG9jay91bmxvY2sgZGFzaGJvYXJkIiwKfQoKCmFzeW5jIGRlZiBzZXRfYm90X2NvbW1hbmRzKCk6CiAgICAi"
    "IiJQdWJsaXNoIHRoZSBmdWxsIGNvbW1hbmQgbGlzdCB0byB0aGUgVGVsZWdyYW0gIi8iIG1lbnUg4oCUIGRlZmF1bHQKICAgIHNj"
    "b3BlIGZvciB1c2Vycywgb3duZXIgc2NvcGUgd2l0aCBldmVyeSBhZG1pbiBjb21tYW5kLiIiIgogICAgdHJ5OgogICAgICAgIGZy"
    "b20gYXN5bmNpbyBpbXBvcnQgc2xlZXAKCiAgICAgICAgYXdhaXQgc2xlZXAoOTApICAjIHdhaXQgZm9yIHRoZSBib3QgdG8gYmUg"
    "dXAKICAgICAgICBmcm9tIHB5cm9ncmFtLnR5cGVzIGltcG9ydCBCb3RDb21tYW5kLCBCb3RDb21tYW5kU2NvcGVDaGF0LCBCb3RD"
    "b21tYW5kU2NvcGVEZWZhdWx0CgogICAgICAgIGZyb20gLi4uIGltcG9ydCBMT0dHRVIKICAgICAgICBmcm9tIC4uLmNvcmUudGdf"
    "Y2xpZW50IGltcG9ydCBUZ0NsaWVudCwgQ29uZmlnCgogICAgICAgICMgZ2F0aGVyIGV2ZXJ5IGNvbW1hbmQgbmFtZSBrbm93biB0"
    "byB0aGUgYm90CiAgICAgICAgdHJ5OgogICAgICAgICAgICBmcm9tIC4udGVsZWdyYW1faGVscGVyLmJvdF9jb21tYW5kcyBpbXBv"
    "cnQgQm90Q29tbWFuZHMKCiAgICAgICAgICAgIGFsbF9uYW1lcyA9IHNldCgpCiAgICAgICAgICAgIGZvciBfdiBpbiBCb3RDb21t"
    "YW5kcy5nZXRfY29tbWFuZHMoKS52YWx1ZXMoKToKICAgICAgICAgICAgICAgIG5hbWVzID0gX3YgaWYgaXNpbnN0YW5jZShfdiwg"
    "bGlzdCkgZWxzZSBbX3ZdCiAgICAgICAgICAgICAgICBhbGxfbmFtZXMudXBkYXRlKG5hbWVzKQogICAgICAgIGV4Y2VwdCBFeGNl"
    "cHRpb246CiAgICAgICAgICAgIGFsbF9uYW1lcyA9IHNldCgpCiAgICAgICAgYWxsX25hbWVzLnVwZGF0ZShfT1dORVJfREVTQykK"
    "ICAgICAgICBhbGxfbmFtZXMudXBkYXRlKF9DTURfREVTQykKCiAgICAgICAgdXNlcl9jbWRzLCBvd25lcl9jbWRzID0gW10sIFtd"
    "CiAgICAgICAgZm9yIG5hbWUgaW4gc29ydGVkKGFsbF9uYW1lcyk6CiAgICAgICAgICAgIGlmIG5hbWUgaW4gKCJzaGVsbCIsICJh"
    "ZXhlYyIsICJleGVjIiwgImNsZWFybG9jYWxzIik6CiAgICAgICAgICAgICAgICBjb250aW51ZSAgIyBrZWVwIHRoZSBkYW5nZXJv"
    "dXMgb25lcyBvdXQgb2YgdGhlIG1lbnUKICAgICAgICAgICAgZGVzYyA9IF9DTURfREVTQy5nZXQobmFtZSkgb3IgX09XTkVSX0RF"
    "U0MuZ2V0KG5hbWUpIG9yICJXWk1MLVggY29tbWFuZCIKICAgICAgICAgICAgb3duZXJfY21kcy5hcHBlbmQoQm90Q29tbWFuZChj"
    "b21tYW5kPW5hbWUsIGRlc2NyaXB0aW9uPWRlc2MpKQogICAgICAgICAgICBpZiBuYW1lIG5vdCBpbiBfT1dORVJfREVTQyBhbmQg"
    "bm90IG5hbWUuc3RhcnRzd2l0aCgid3oiKToKICAgICAgICAgICAgICAgIHVzZXJfY21kcy5hcHBlbmQoQm90Q29tbWFuZChjb21t"
    "YW5kPW5hbWUsIGRlc2NyaXB0aW9uPWRlc2MpKQoKICAgICAgICBhc3luYyBkZWYgX3NldF9jbWRzX2h0dHAoY21kcywgc2NvcGU9"
    "Tm9uZSk6CiAgICAgICAgICAgICIiInNldE15Q29tbWFuZHMgdmlhIHRoZSByYXcgQm90IEFQSSDigJQgc29tZSBjbGllbnQgZm9y"
    "a3MKICAgICAgICAgICAgKHd6Z3JhbSkgbGFjayBDbGllbnQuc2V0X215X2NvbW1hbmRzIGVudGlyZWx5LiIiIgogICAgICAgICAg"
    "ICBpbXBvcnQganNvbiBhcyBfanNvbgoKICAgICAgICAgICAgZnJvbSBhaW9odHRwIGltcG9ydCBDbGllbnRTZXNzaW9uCgogICAg"
    "ICAgICAgICB0b2tlbiA9IHN0cihnZXRhdHRyKENvbmZpZywgIkJPVF9UT0tFTiIsICIiKSBvciAiIikuc3RyaXAoKQogICAgICAg"
    "ICAgICBpZiBub3QgdG9rZW46CiAgICAgICAgICAgICAgICByYWlzZSBSdW50aW1lRXJyb3IoIkJPVF9UT0tFTiBub3Qgc2V0IikK"
    "ICAgICAgICAgICAgYm9keSA9IHsKICAgICAgICAgICAgICAgICJjb21tYW5kcyI6IFsKICAgICAgICAgICAgICAgICAgICB7ImNv"
    "bW1hbmQiOiBjLmNvbW1hbmQsICJkZXNjcmlwdGlvbiI6IGMuZGVzY3JpcHRpb259CiAgICAgICAgICAgICAgICAgICAgZm9yIGMg"
    "aW4gY21kcwogICAgICAgICAgICAgICAgXQogICAgICAgICAgICB9CiAgICAgICAgICAgIGlmIHNjb3BlIGlzIG5vdCBOb25lOgog"
    "ICAgICAgICAgICAgICAgYm9keVsic2NvcGUiXSA9IHNjb3BlCiAgICAgICAgICAgIGFzeW5jIHdpdGggQ2xpZW50U2Vzc2lvbigp"
    "IGFzIF9zOgogICAgICAgICAgICAgICAgYXN5bmMgd2l0aCBfcy5wb3N0KAogICAgICAgICAgICAgICAgICAgIGYiaHR0cHM6Ly9h"
    "cGkudGVsZWdyYW0ub3JnL2JvdHt0b2tlbn0vc2V0TXlDb21tYW5kcyIsCiAgICAgICAgICAgICAgICAgICAganNvbj1ib2R5LAog"
    "ICAgICAgICAgICAgICAgKSBhcyBfcjoKICAgICAgICAgICAgICAgICAgICBfaiA9IGF3YWl0IF9yLmpzb24oY29udGVudF90eXBl"
    "PU5vbmUpCiAgICAgICAgICAgICAgICAgICAgaWYgbm90IF9qLmdldCgib2siKToKICAgICAgICAgICAgICAgICAgICAgICAgcmFp"
    "c2UgUnVudGltZUVycm9yKHN0cihfai5nZXQoImRlc2NyaXB0aW9uIikpWzoxMjBdKQoKICAgICAgICB0cnk6CiAgICAgICAgICAg"
    "IGF3YWl0IFRnQ2xpZW50LmJvdC5zZXRfbXlfY29tbWFuZHMoCiAgICAgICAgICAgICAgICB1c2VyX2NtZHMgb3IgW0JvdENvbW1h"
    "bmQoInN0YXJ0IiwgIlN0YXJ0IHRoZSBib3QiKV0KICAgICAgICAgICAgKQogICAgICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Qu"
    "c2V0X215X2NvbW1hbmRzKAogICAgICAgICAgICAgICAgb3duZXJfY21kcywKICAgICAgICAgICAgICAgIHNjb3BlPUJvdENvbW1h"
    "bmRTY29wZUNoYXQoY2hhdF9pZD1Db25maWcuT1dORVJfSUQpLAogICAgICAgICAgICApCiAgICAgICAgZXhjZXB0IEV4Y2VwdGlv"
    "biBhcyBfZToKICAgICAgICAgICAgIyBjbGllbnQgbWV0aG9kIG1pc3NpbmcvYnJva2VuIC0+IHJhdyBCb3QgQVBJCiAgICAgICAg"
    "ICAgIGF3YWl0IF9zZXRfY21kc19odHRwKAogICAgICAgICAgICAgICAgdXNlcl9jbWRzIG9yIFtCb3RDb21tYW5kKCJzdGFydCIs"
    "ICJTdGFydCB0aGUgYm90IildCiAgICAgICAgICAgICkKICAgICAgICAgICAgYXdhaXQgX3NldF9jbWRzX2h0dHAoCiAgICAgICAg"
    "ICAgICAgICBvd25lcl9jbWRzLCBzY29wZT17InR5cGUiOiAiY2hhdCIsICJjaGF0X2lkIjogQ29uZmlnLk9XTkVSX0lEfQogICAg"
    "ICAgICAgICApCiAgICAgICAgTE9HR0VSLmluZm8oCiAgICAgICAgICAgIGYiV1pGSVg6IGNvbW1hbmQgbWVudSBzZXQgKHtsZW4o"
    "dXNlcl9jbWRzKX0gdXNlciwgIgogICAgICAgICAgICBmIntsZW4ob3duZXJfY21kcyl9IG93bmVyIGNvbW1hbmRzKSIKICAgICAg"
    "ICApCiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgdHJ5OgogICAgICAgICAgICBmcm9tIC4uLiBpbXBvcnQgTE9H"
    "R0VSCgogICAgICAgICAgICBMT0dHRVIud2FybmluZyhmIldaRklYIHNldF9ib3RfY29tbWFuZHMgZmFpbGVkOiB7ZX0iKQogICAg"
    "ICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIHBhc3MKCgpkZWYgc3RhcnRfbG9vcHMoKToKICAgIHRyeToKICAgICAg"
    "ICBmcm9tIC4uLiBpbXBvcnQgYm90X2xvb3AKCiAgICAgICAgYm90X2xvb3AuY3JlYXRlX3Rhc2soZGFpbHlfcmVwb3J0X2xvb3Ao"
    "KSkKICAgICAgICBib3RfbG9vcC5jcmVhdGVfdGFzayhzZXRfYm90X2NvbW1hbmRzKCkpCiAgICBleGNlcHQgRXhjZXB0aW9uOgog"
    "ICAgICAgIHBhc3MKCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgAojIEhUVFAgaGFuZGxlcnMgKHJlZ2lzdGVyZWQgb24gdGhlIHN0cmVhbSBzZXJ2ZXIncyBhaW9odHRwIGFwcCkK"
    "IyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKCgph"
    "c3luYyBkZWYgd3phZG1pbl9wYWdlKHJlcXVlc3QpOgogICAgaXAgPSBfbm9ybV9pcChfY2xpZW50X2lwKHJlcXVlc3QpKQogICAg"
    "aWYgbm90IF91YV9vayhyZXF1ZXN0KSBvciBhd2FpdCBfaXNfYmFubmVkKGlwKToKICAgICAgICByZXR1cm4gd2ViLlJlc3BvbnNl"
    "KHN0YXR1cz00MDMsIHRleHQ9ImZvcmJpZGRlbiIpCiAgICBpZiBub3QgYXdhaXQgZW5zdXJlX3JlYWR5KCk6CiAgICAgICAgcmV0"
    "dXJuIHdlYi5SZXNwb25zZSgKICAgICAgICAgICAgdGV4dD0iPGgzPkRhc2hib2FyZCB1bmF2YWlsYWJsZTogZGF0YWJhc2Ugbm90"
    "IHJlYWNoYWJsZS48L2gzPiIsCiAgICAgICAgICAgIGNvbnRlbnRfdHlwZT0idGV4dC9odG1sIiwKICAgICAgICAgICAgc3RhdHVz"
    "PTUwMywKICAgICAgICApCiAgICByZXR1cm4gd2ViLlJlc3BvbnNlKHRleHQ9X1BBR0UsIGNvbnRlbnRfdHlwZT0idGV4dC9odG1s"
    "IikKCgphc3luYyBkZWYgd3phZG1pbl9hcGkocmVxdWVzdCk6CiAgICBwYXRoID0gcmVxdWVzdC5wYXRoCgogICAgaWYgcGF0aC5l"
    "bmRzd2l0aCgiL2FwaS90YWtlb3ZlciIpOgogICAgICAgICMgU2Vzc2lvbiB0YWtlb3ZlcjogdGhlIE5FWFQgS2FnZ2xlIG5vdGVi"
    "b29rIHJ1biBwcm92ZXMgaXQgaG9sZHMKICAgICAgICAjIHRoZSBjdXJyZW50IGluc3RhbmNlIHRva2VuIChzdG9yZWQgaW4gTW9u"
    "Z29EQiBuZXh0IHRvIHRoZSBzZXNzaW9uCiAgICAgICAgIyBsb2NrKSBhbmQgYXNrcyB0aGlzIGluc3RhbmNlIHRvIHN0b3AuIFRv"
    "a2VuIGF1dGggb25seSDigJQgbm8KICAgICAgICAjIHNlc3Npb24vSVAvVUEgY2hlY2tzLCBiZWNhdXNlIHRoZSBjYWxsZXIgaXMg"
    "dGhlIG5vdGVib29rIGl0c2VsZgogICAgICAgICMgKHdoaWNoIHJ1bnMgb24gYSBkYXRhY2VudGVyIElQIGFuZCB3b3VsZCB0cmlw"
    "IHRoZSBpbnRlbCBmaWx0ZXIpLgogICAgICAgIHRyeToKICAgICAgICAgICAgYm9keSA9IGF3YWl0IHJlcXVlc3QuanNvbigpCiAg"
    "ICAgICAgZXhjZXB0IEV4Y2VwdGlvbjoKICAgICAgICAgICAgYm9keSA9IHt9CiAgICAgICAgdG9rID0gc3RyKGJvZHkuZ2V0KCJ0"
    "b2siLCAiIikpLnN0cmlwKCkKICAgICAgICBnb29kID0gIiIKICAgICAgICB0cnk6CiAgICAgICAgICAgIGltcG9ydCBvcyBhcyBf"
    "b3MKCiAgICAgICAgICAgIGZyb20gcHltb25nbyBpbXBvcnQgTW9uZ29DbGllbnQKCiAgICAgICAgICAgIF91cmwgPSBfb3MuZW52"
    "aXJvbi5nZXQoIkRBVEFCQVNFX1VSTCIsICIiKQogICAgICAgICAgICBpZiBfdXJsOgogICAgICAgICAgICAgICAgX2NsID0gTW9u"
    "Z29DbGllbnQoX3VybCwgc2VydmVyU2VsZWN0aW9uVGltZW91dE1TPTgwMDApCiAgICAgICAgICAgICAgICBfZG9jID0gX2NsWyJ3"
    "em1sX2thZ2dsZSJdWyJzZXNzaW9uX2xvY2siXS5maW5kX29uZSgKICAgICAgICAgICAgICAgICAgICB7Il9pZCI6ICJpbnN0YW5j"
    "ZSJ9KQogICAgICAgICAgICAgICAgX2NsLmNsb3NlKCkKICAgICAgICAgICAgICAgIGdvb2QgPSBzdHIoKF9kb2Mgb3Ige30pLmdl"
    "dCgidG9rIiwgIiIpKQogICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgIGdvb2QgPSAiIgogICAgICAgIGlmIG5v"
    "dCBnb29kIG9yIG5vdCB0b2sgb3IgdG9rICE9IGdvb2Q6CiAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSh7ImVy"
    "cm9yIjogImZvcmJpZGRlbiJ9LCBzdGF0dXM9NDAzKQoKICAgICAgICBpbXBvcnQgYXN5bmNpbyBhcyBfYWlvCgogICAgICAgIGFz"
    "eW5jIGRlZiBfd3pmaXhfZGllKCk6CiAgICAgICAgICAgIGF3YWl0IF9haW8uc2xlZXAoMS4wKSAgIyBsZXQgdGhlIHJlc3BvbnNl"
    "IGZsdXNoIGZpcnN0CiAgICAgICAgICAgIHRyeToKICAgICAgICAgICAgICAgIGZyb20gLi4uY29yZS5jb25maWdfbWFuYWdlciBp"
    "bXBvcnQgQ29uZmlnCiAgICAgICAgICAgICAgICBmcm9tIC4uLmNvcmUudGdfY2xpZW50IGltcG9ydCBUZ0NsaWVudAoKICAgICAg"
    "ICAgICAgICAgIGNoYXQgPSBzdHIoZ2V0YXR0cihDb25maWcsICJMT0dfQ0hBVCIsICIiKSBvciAiIikuc3RyaXAoKQogICAgICAg"
    "ICAgICAgICAgaWYgY2hhdDoKICAgICAgICAgICAgICAgICAgICBhd2FpdCBUZ0NsaWVudC5ib3Quc2VuZF9tZXNzYWdlKAogICAg"
    "ICAgICAgICAgICAgICAgICAgICBjaGF0X2lkPWNoYXQsCiAgICAgICAgICAgICAgICAgICAgICAgIHRleHQ9IvCflIQgPGI+V1pG"
    "SVg6PC9iPiBhIG5ld2VyIG5vdGVib29rIHNlc3Npb24gaXMgIgogICAgICAgICAgICAgICAgICAgICAgICAidGFraW5nIG92ZXIg"
    "4oCUIHN0b3BwaW5nIHRoaXMgaW5zdGFuY2UuIiwKICAgICAgICAgICAgICAgICAgICAgICAgZGlzYWJsZV93ZWJfcGFnZV9wcmV2"
    "aWV3PVRydWUsCiAgICAgICAgICAgICAgICAgICAgKQogICAgICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICAg"
    "ICAgcGFzcwogICAgICAgICAgICBpbXBvcnQgb3MgYXMgX29zMgogICAgICAgICAgICBpbXBvcnQgc2lnbmFsIGFzIF9zaWcKCiAg"
    "ICAgICAgICAgIF9vczIua2lsbChfb3MyLmdldHBpZCgpLCBfc2lnLlNJR0lOVCkKCiAgICAgICAgX2Fpby5nZXRfZXZlbnRfbG9v"
    "cCgpLmNyZWF0ZV90YXNrKF93emZpeF9kaWUoKSkKICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoCiAgICAgICAgICAg"
    "IHsib2siOiBUcnVlLCAibXNnIjogInRha2VvdmVyIGFjY2VwdGVkIOKAlCBzaHV0dGluZyBkb3duIn0KICAgICAgICApCgogICAg"
    "aWYgcGF0aC5lbmRzd2l0aCgiL2xvZ2luIik6CiAgICAgICAgaXAgPSBfbm9ybV9pcChfY2xpZW50X2lwKHJlcXVlc3QpKQogICAg"
    "ICAgIGlmIG5vdCBfdWFfb2socmVxdWVzdCk6CiAgICAgICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSh7ImVycm9yIjog"
    "ImZvcmJpZGRlbiJ9LCBzdGF0dXM9NDAzKQogICAgICAgIGlmIGF3YWl0IF9pc19iYW5uZWQoaXApOgogICAgICAgICAgICByZXR1"
    "cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJiYW5uZWQifSwgc3RhdHVzPTQwMykKICAgICAgICBpZiBhd2FpdCBfaXBf"
    "aW50ZWwoaXApOgogICAgICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoCiAgICAgICAgICAgICAgICB7ImVycm9yIjog"
    "ImRhdGFjZW50ZXIvcHJveHkgSVBzIGFyZSBub3QgYWxsb3dlZCJ9LCBzdGF0dXM9NDAzCiAgICAgICAgICAgICkKICAgICAgICBf"
    "cmVtID0gYXdhaXQgX2xvY2tvdXRfc3RhdGUoaXApCiAgICAgICAgaWYgX3JlbToKICAgICAgICAgICAgcmV0dXJuIHdlYi5qc29u"
    "X3Jlc3BvbnNlKAogICAgICAgICAgICAgICAgeyJlcnJvciI6IGYidG9vIG1hbnkgYXR0ZW1wdHMg4oCUIHtfbG9ja190eHQoX3Jl"
    "bSl9In0sCiAgICAgICAgICAgICAgICBzdGF0dXM9NDI5LAogICAgICAgICAgICApCiAgICAgICAgdHJ5OgogICAgICAgICAgICBi"
    "b2R5ID0gYXdhaXQgcmVxdWVzdC5qc29uKCkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBib2R5ID0ge30K"
    "ICAgICAgICBzdWJtaXR0ZWQgPSBzdHIoYm9keS5nZXQoInBhc3MiLCAiIikpCiAgICAgICAgZGV2ID0gYm9keS5nZXQoImRldiIp"
    "CiAgICAgICAgaWYgbm90IGlzaW5zdGFuY2UoZGV2LCBkaWN0KToKICAgICAgICAgICAgZGV2ID0ge30KICAgICAgICAjIGFic29s"
    "dXRlLWNvbnRyb2wgc3dpdGNoOiAvbG9ja2Rhc2ggaW4gVGVsZWdyYW0KICAgICAgICBfbG9ja2VkID0gRmFsc2UKICAgICAgICB0"
    "cnk6CiAgICAgICAgICAgIGZyb20gLi4uY29yZS5jb25maWdfbWFuYWdlciBpbXBvcnQgQ29uZmlnIGFzIF9MQ2ZnCgogICAgICAg"
    "ICAgICBfbG9ja2VkID0gYm9vbChnZXRhdHRyKF9MQ2ZnLCAiQURNSU5fREFTSEJPQVJEX0xPQ0tFRCIsIEZhbHNlKSkKICAgICAg"
    "ICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBwYXNzCiAgICAgICAgaWYgX2xvY2tlZDoKICAgICAgICAgICAgZnJvbSBh"
    "c3luY2lvIGltcG9ydCBnZXRfZXZlbnRfbG9vcCBhcyBfZ2VsCgogICAgICAgICAgICBfZ2VsKCkuY3JlYXRlX3Rhc2soCiAgICAg"
    "ICAgICAgICAgICBfbG9naW5fYWxlcnQoaXAsIHJlcXVlc3QsIGRldiwgc3VibWl0dGVkLAogICAgICAgICAgICAgICAgICAgICAg"
    "ICAgICAgICLwn5SSIGRhc2hib2FyZCBpcyBMT0NLRUQg4oCUIGxvZ2luIHJlZnVzZWQiKQogICAgICAgICAgICApCiAgICAgICAg"
    "ICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSgKICAgICAgICAgICAgICAgIHsiZXJyb3IiOiAiZGFzaGJvYXJkIGxvY2tlZCBi"
    "eSBvd25lciDigJQgL2xvY2tkYXNoIHRvIHVubG9jayJ9LAogICAgICAgICAgICAgICAgc3RhdHVzPTQwMywKICAgICAgICAgICAg"
    "KQogICAgICAgIHJlYWwgPSBhd2FpdCBnZXRfYWRtaW5fcGFzcygpCiAgICAgICAgaWYgbm90IHJlYWw6CiAgICAgICAgICAgIHJl"
    "dHVybiB3ZWIuanNvbl9yZXNwb25zZSh7ImVycm9yIjogImFkbWluIHBhc3MgdW5hdmFpbGFibGUifSwgc3RhdHVzPTUwMykKICAg"
    "ICAgICBpZiBub3Qgc3VibWl0dGVkIG9yIG5vdCBjb21wYXJlX2RpZ2VzdChzdWJtaXR0ZWQsIHJlYWwpOgogICAgICAgICAgICBf"
    "c2VjcywgX3RpZXIgPSBhd2FpdCBfcmVjb3JkX2ZhaWwoaXApCiAgICAgICAgICAgIF9ub3RlID0gZiIg4oCUIExPQ0tFRCBPVVQg"
    "e19sb2NrX3R4dChfc2Vjcyl9IiBpZiBfc2VjcyBlbHNlICIiCiAgICAgICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgZ2V0X2V2"
    "ZW50X2xvb3AgYXMgX2dlbAoKICAgICAgICAgICAgX2dlbCgpLmNyZWF0ZV90YXNrKAogICAgICAgICAgICAgICAgX2xvZ2luX2Fs"
    "ZXJ0KGlwLCByZXF1ZXN0LCBkZXYsIHN1Ym1pdHRlZCwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmIuKdjCB3cm9uZyBw"
    "YXNzd29yZHtfbm90ZX0iKQogICAgICAgICAgICApCiAgICAgICAgICAgIGlmIF9zZWNzOgogICAgICAgICAgICAgICAgcmV0dXJu"
    "IHdlYi5qc29uX3Jlc3BvbnNlKAogICAgICAgICAgICAgICAgICAgIHsiZXJyb3IiOiBmIndyb25nIHBhc3N3b3JkIOKAlCB7X2xv"
    "Y2tfdHh0KF9zZWNzKX0ifSwKICAgICAgICAgICAgICAgICAgICBzdGF0dXM9NDI5LAogICAgICAgICAgICAgICAgKQogICAgICAg"
    "ICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJ3cm9uZyBwYXNzd29yZCJ9LCBzdGF0dXM9NDAxKQogICAg"
    "ICAgIF9wcmlvciA9IGF3YWl0IF9wcmlvcl9mYWlscyhpcCkKICAgICAgICBfcmVjb3JkX29rKGlwKQogICAgICAgIGlmIF9wcmlv"
    "ciA+IDA6CiAgICAgICAgICAgIGZyb20gYXN5bmNpbyBpbXBvcnQgZ2V0X2V2ZW50X2xvb3AgYXMgX2dlbAoKICAgICAgICAgICAg"
    "X2dlbCgpLmNyZWF0ZV90YXNrKAogICAgICAgICAgICAgICAgX2xvZ2luX2FsZXJ0KGlwLCByZXF1ZXN0LCBkZXYsIHN1Ym1pdHRl"
    "ZCwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmIuKchSBsb2dpbiBPSyBhZnRlciB7X3ByaW9yfSBmYWlsZWQgYXR0ZW1w"
    "dChzKSIpCiAgICAgICAgICAgICkKICAgICAgICB0b2sgPSBhd2FpdCBfc2Vzc2lvbl90b2tlbigpCiAgICAgICAgcmVzcCA9IHdl"
    "Yi5qc29uX3Jlc3BvbnNlKHsib2siOiBUcnVlfSkKICAgICAgICByZXNwLnNldF9jb29raWUoCiAgICAgICAgICAgICJ3emFkbWlu"
    "IiwgdG9rLCBtYXhfYWdlPVNFU1NJT05fSCAqIDM2MDAsIGh0dHBvbmx5PVRydWUsIHNhbWVzaXRlPSJMYXgiCiAgICAgICAgKQog"
    "ICAgICAgIHJldHVybiByZXNwCgogICAgaWYgbm90IGF3YWl0IF9jaGVja19zZXNzaW9uKHJlcXVlc3QpOgogICAgICAgIHJldHVy"
    "biB3ZWIuanNvbl9yZXNwb25zZSh7ImVycm9yIjogInVuYXV0aG9yaXplZCJ9LCBzdGF0dXM9NDAxKQogICAgaWYgbm90IF91YV9v"
    "ayhyZXF1ZXN0KSBvciBhd2FpdCBfaXNfYmFubmVkKF9ub3JtX2lwKF9jbGllbnRfaXAocmVxdWVzdCkpKToKICAgICAgICByZXR1"
    "cm4gd2ViLmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJmb3JiaWRkZW4ifSwgc3RhdHVzPTQwMykKCiAgICBpZiBwYXRoLmVuZHN3"
    "aXRoKCIvbG9nb3V0Iik6CiAgICAgICAgcmVzcCA9IHdlYi5qc29uX3Jlc3BvbnNlKHsib2siOiBUcnVlfSkKICAgICAgICByZXNw"
    "LmRlbF9jb29raWUoInd6YWRtaW4iKQogICAgICAgIHJldHVybiByZXNwCgogICAgaWYgcGF0aC5lbmRzd2l0aCgiL3N0YXRlIik6"
    "CiAgICAgICAgcmV0dXJuIHdlYi5qc29uX3Jlc3BvbnNlKGF3YWl0IF9zdGF0ZSgpKQoKICAgIGlmIHBhdGguZW5kc3dpdGgoIi9o"
    "aXN0b3J5Iik6CiAgICAgICAgdHJ5OgogICAgICAgICAgICB1aWQgPSBpbnQocmVxdWVzdC5xdWVyeS5nZXQoInVpZCIsICIwIikp"
    "CiAgICAgICAgZXhjZXB0IFZhbHVlRXJyb3I6CiAgICAgICAgICAgIHVpZCA9IDAKICAgICAgICBxID0gKHJlcXVlc3QucXVlcnku"
    "Z2V0KCJxIikgb3IgIiIpLnN0cmlwKClbOjYwXQogICAgICAgIGRvY3MgPSBhd2FpdCBmaW5kKHEsIHVpZCwgYWxsX3VzZXJzPUZh"
    "bHNlLCBsaW1pdD0yNSkgaWYgdWlkIGVsc2UgW10KICAgICAgICBvdXQgPSBbXQogICAgICAgIGZvciBkIGluIGRvY3M6CiAgICAg"
    "ICAgICAgIHRnX2xpbmtzID0gZC5nZXQoInRnX2xpbmtzIikgb3IgW10KICAgICAgICAgICAgb3V0LmFwcGVuZCgKICAgICAgICAg"
    "ICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAibmFtZSI6IHN0cihkLmdldCgibmFtZSIsICIiKSlbOjkwXSwKICAgICAgICAg"
    "ICAgICAgICAgICAic2l6ZSI6IGludChkLmdldCgic2l6ZSIpIG9yIDApLAogICAgICAgICAgICAgICAgICAgICJkYXRlIjogc3Ry"
    "KGQuZ2V0KCJkYXRlIikgb3IgIiIpWzoxNl0sCiAgICAgICAgICAgICAgICAgICAgInRnIjogc3RyKHRnX2xpbmtzWzBdKSBpZiB0"
    "Z19saW5rcyBlbHNlICIiLAogICAgICAgICAgICAgICAgICAgICJjbG91ZCI6IHN0cihkLmdldCgiY2xvdWRfbGluayIpIG9yICIi"
    "KSwKICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgKQogICAgICAgIHJldHVybiB3ZWIuanNvbl9yZXNwb25zZSh7Im9rIjog"
    "VHJ1ZSwgIml0ZW1zIjogb3V0fSkKCiAgICBpZiBwYXRoLmVuZHN3aXRoKCIvYWN0aW9uIik6CiAgICAgICAgdHJ5OgogICAgICAg"
    "ICAgICBib2R5ID0gYXdhaXQgcmVxdWVzdC5qc29uKCkKICAgICAgICBleGNlcHQgRXhjZXB0aW9uOgogICAgICAgICAgICBib2R5"
    "ID0ge30KICAgICAgICByZXR1cm4gd2ViLmpzb25fcmVzcG9uc2UoYXdhaXQgX2FjdGlvbihib2R5KSkKCiAgICByZXR1cm4gd2Vi"
    "Lmpzb25fcmVzcG9uc2UoeyJlcnJvciI6ICJ1bmtub3duIGVuZHBvaW50In0sIHN0YXR1cz00MDQpCgoKYXN5bmMgZGVmIF9hY3Rp"
    "b24oYSk6CiAgICBhY3QgPSBzdHIoYS5nZXQoImFjdGlvbiIsICIiKSkuc3RyaXAoKQogICAgdHJ5OgogICAgICAgIHVpZCA9IGlu"
    "dChhLmdldCgidWlkIiwgMCkgb3IgMCkKICAgIGV4Y2VwdCAoVHlwZUVycm9yLCBWYWx1ZUVycm9yKToKICAgICAgICB1aWQgPSAw"
    "CgogICAgdHJ5OgogICAgICAgIGlmIGFjdCA9PSAic2V0Y2FwIjoKICAgICAgICAgICAgZ2IgPSBmbG9hdChhLmdldCgiZ2IiLCAw"
    "KSBvciAwKQogICAgICAgICAgICBhd2FpdCBzZXRfdXNlcl9jYXAodWlkLCBnYiBpZiBnYiA+IDAgZWxzZSBOb25lKQogICAgICAg"
    "ICAgICByZXR1cm4geyJvayI6IFRydWUsICJtc2ciOiBmImNhcCBmb3Ige3VpZH0g4oaSIHtfZm10X2diKGdiKSBpZiBnYiA+IDAg"
    "ZWxzZSAnZGVmYXVsdCd9In0KICAgICAgICBpZiBhY3QgPT0gImJhbiI6CiAgICAgICAgICAgIGF3YWl0IHNldF91c2VyX2NhcCh1"
    "aWQsIDApCiAgICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYidXNlciB7dWlkfSBibG9ja2VkIChjYXAgMCki"
    "fQogICAgICAgIGlmIGFjdCA9PSAidW5iYW4iOgogICAgICAgICAgICBhd2FpdCBzZXRfdXNlcl9jYXAodWlkLCBOb25lKQogICAg"
    "ICAgICAgICByZXR1cm4geyJvayI6IFRydWUsICJtc2ciOiBmInVzZXIge3VpZH0gYmFjayB0byBnbG9iYWwgZGVmYXVsdCJ9CiAg"
    "ICAgICAgaWYgYWN0ID09ICJyZXNldGNhcCI6CiAgICAgICAgICAgIGF3YWl0IHJlc2V0X3VzYWdlKHVpZCkKICAgICAgICAgICAg"
    "cmV0dXJuIHsib2siOiBUcnVlLCAibXNnIjogZiJ0b2RheSdzIHVzYWdlIHJlc2V0IGZvciB7dWlkfSJ9CiAgICAgICAgaWYgYWN0"
    "ID09ICJkZWR1Y3RjYXAiOgogICAgICAgICAgICBnYiA9IGZsb2F0KGEuZ2V0KCJnYiIsIDApIG9yIDApCiAgICAgICAgICAgIG9r"
    "LCBtc2cgPSBhd2FpdCBfZGVkdWN0X2NhcCh1aWQsIGdiKQogICAgICAgICAgICByZXR1cm4geyJvayI6IG9rLCAibXNnIjogbXNn"
    "fQogICAgICAgIGlmIGFjdCA9PSAiZGVsdXNlciI6CiAgICAgICAgICAgIGlmIG5vdCB1aWQ6CiAgICAgICAgICAgICAgICByZXR1"
    "cm4geyJvayI6IEZhbHNlLCAibXNnIjogIm5vIHVzZXIgaWQifQogICAgICAgICAgICBhd2FpdCBkZWxldGVfdXNlcih1aWQpCiAg"
    "ICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwgIm1zZyI6IGYidXNlciB7dWlkfSByZW1vdmVkIn0KICAgICAgICBpZiBhY3Qg"
    "PT0gImJvdGNhcCI6CiAgICAgICAgICAgIGdiID0gZmxvYXQoYS5nZXQoImdiIiwgMCkgb3IgMCkKICAgICAgICAgICAgaWYgZ2Ig"
    "PD0gMDoKICAgICAgICAgICAgICAgIHJldHVybiB7Im9rIjogRmFsc2UsICJtc2ciOiAiZ2l2ZSBhIHBvc2l0aXZlIEdCIHZhbHVl"
    "In0KICAgICAgICAgICAgYXdhaXQgc2V0X2dsb2JhbF9jYXBfZ2IoZ2IpCiAgICAgICAgICAgIHJldHVybiB7Im9rIjogVHJ1ZSwg"
    "Im1zZyI6IGYiZGVmYXVsdCBjYXAg4oaSIHtfZm10X2diKGdiKX0ifQogICAgICAgIGlmIGFjdCA9PSAia2lsbCI6CiAgICAgICAg"
    "ICAgIG9rLCBtc2cgPSBhd2FpdCBfa2lsbF90YXNrKGEuZ2V0KCJtaWQiKSkKICAgICAgICAgICAgcmV0dXJuIHsib2siOiBvaywg"
    "Im1zZyI6IG1zZ30KICAgICAgICBpZiBhY3QgPT0gImtpbGxhbGwiOgogICAgICAgICAgICBmcm9tIC4uLiBpbXBvcnQgdGFza19k"
    "aWN0CgogICAgICAgICAgICBuID0gMAogICAgICAgICAgICBmb3IgbWlkLCB0IGluIGxpc3QodGFza19kaWN0Lml0ZW1zKCkpOgog"
    "ICAgICAgICAgICAgICAgdHJ5OgogICAgICAgICAgICAgICAgICAgIGF3YWl0IHQudGFzaygpLmNhbmNlbF90YXNrKCkKICAgICAg"
    "ICAgICAgICAgICAgICBuICs9IDEKICAgICAgICAgICAgICAgIGV4Y2VwdCBFeGNlcHRpb246CiAgICAgICAgICAgICAgICAgICAg"
    "cGFzcwogICAgICAgICAgICByZXR1cm4geyJvayI6IFRydWUsICJtc2ciOiBmIntufSB0YXNrKHMpIGNhbmNlbGxlZCJ9CiAgICAg"
    "ICAgaWYgYWN0ID09ICJyZXBvcnQiOgogICAgICAgICAgICBvaywgbXNnID0gYXdhaXQgc2VuZF9yZXBvcnQoKQogICAgICAgICAg"
    "ICByZXR1cm4geyJvayI6IG9rLCAibXNnIjogbXNnfQogICAgICAgIHJldHVybiB7Im9rIjogRmFsc2UsICJtc2ciOiBmInVua25v"
    "d24gYWN0aW9uOiB7YWN0fSJ9CiAgICBleGNlcHQgRXhjZXB0aW9uIGFzIGU6CiAgICAgICAgcmV0dXJuIHsib2siOiBGYWxzZSwg"
    "Im1zZyI6IHN0cihlKVs6MTYwXX0KCgojIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKU"
    "gOKUgOKUgOKUgOKUgOKUgOKUgAojIFRoZSBwYWdlIOKAlCA3IHRoZW1lcyAobWF0Y2hpbmcgdGhlIHN0cmVhbSBwbGF5ZXIpLCBt"
    "b2JpbGUtZmlyc3QKIyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDi"
    "lIDilIDilIAKCl9QQUdFID0gIiIiPCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImVuIiBkYXRhLXRoZW1lPSJvbnl4Ij4KPGhl"
    "YWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgbmFtZT0idmlld3BvcnQiIGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0"
    "aCxpbml0aWFsLXNjYWxlPTEsdmlld3BvcnQtZml0PWNvdmVyIj4KPG1ldGEgbmFtZT0icm9ib3RzIiBjb250ZW50PSJub2luZGV4"
    "Ij4KPHRpdGxlPldaTUwtWCBDb250cm9sPC90aXRsZT4KPHN0eWxlPgo6cm9vdHstLXI6MTZweDstLXNhbnM6LWFwcGxlLXN5c3Rl"
    "bSxCbGlua01hY1N5c3RlbUZvbnQsJ1NlZ29lIFVJJyxSb2JvdG8sc2Fucy1zZXJpZn0KW2RhdGEtdGhlbWU9Im9ueXgiXXstLWJn"
    "OiMwNTA1MDg7LS1zdXJmYWNlOiMwYTBhMTI7LS1zdXJmYWNlLTI6IzBlMGUxODstLWxpbmU6cmdiYSgxMzksOTIsMjQ2LC4yMik7"
    "LS10ZXh0OiNmNGYyZmY7LS1tdXRlZDojYjZiMGQ0Oy0tYWNjZW50OiM4YjVjZjY7LS1hY2NlbnQtMjojYTc4YmZhOy0tYWNjZW50"
    "LXNvZnQ6IzFhMTAzMzstLWRhbmdlcjojZjg3MTcxOy0tb2s6IzRhZGU4MH0KW2RhdGEtdGhlbWU9ImFxdWEiXXstLWJnOiMwMzE0"
    "MWI7LS1zdXJmYWNlOiMwNTFlMjg7LS1zdXJmYWNlLTI6IzA3MjQyZjstLWxpbmU6cmdiYSgzNCwyMTEsMjM4LC4yMik7LS10ZXh0"
    "OiNlYWZjZmY7LS1tdXRlZDojYTNjNmQxOy0tYWNjZW50OiMyMmQzZWU7LS1hY2NlbnQtMjojNjdlOGY5Oy0tYWNjZW50LXNvZnQ6"
    "IzA2MjQzMDstLWRhbmdlcjojZjg3MTcxOy0tb2s6IzRhZGU4MH0KW2RhdGEtdGhlbWU9ImVtYmVyIl17LS1iZzojMTIwYTA1Oy0t"
    "c3VyZmFjZTojMWEwZjA3Oy0tc3VyZmFjZS0yOiMyMTE0MGE7LS1saW5lOnJnYmEoMjQ1LDE1OCwxMSwuMjIpOy0tdGV4dDojZmRm"
    "M2U3Oy0tbXV0ZWQ6I2QwYjhhMDstLWFjY2VudDojZjU5ZTBiOy0tYWNjZW50LTI6I2ZiYmYyNDstLWFjY2VudC1zb2Z0OiMyYTFh"
    "MDY7LS1kYW5nZXI6I2Y4NzE3MTstLW9rOiM0YWRlODB9CltkYXRhLXRoZW1lPSJkYXJrIl17LS1iZzojMDQwNDBiOy0tc3VyZmFj"
    "ZTojMDgwODEyOy0tc3VyZmFjZS0yOiMwZTBlMWE7LS1saW5lOnJnYmEoNjEsMTM1LDI1NSwuMTgpOy0tdGV4dDojZjVmN2ZmOy0t"
    "bXV0ZWQ6I2MzY2JkZjstLWFjY2VudDojM2Q4N2ZmOy0tYWNjZW50LTI6IzViOWRmZjstLWFjY2VudC1zb2Z0OiMwZDFiM2E7LS1k"
    "YW5nZXI6I2ZmNmI2YjstLW9rOiM1MWU5OGN9CltkYXRhLXRoZW1lPSJsaWdodCJdey0tYmc6I2YyZjVmYjstLXN1cmZhY2U6I2Zm"
    "ZmZmZjstLXN1cmZhY2UtMjojZWVmMWY4Oy0tbGluZTpyZ2JhKDE1LDIzLDQyLC4xMik7LS10ZXh0OiMwZjE3MmE7LS1tdXRlZDoj"
    "NWE2NDc4Oy0tYWNjZW50OiMyNTYzZWI7LS1hY2NlbnQtMjojM2I4MmY2Oy0tYWNjZW50LXNvZnQ6I2RiZWFmZTstLWRhbmdlcjoj"
    "ZGMyNjI2Oy0tb2s6IzE2YTM0YX0KW2RhdGEtdGhlbWU9InZpYnJhbnQiXXstLWJnOiMwYTA2MTI7LS1zdXJmYWNlOiMxMzBiMjA7"
    "LS1zdXJmYWNlLTI6IzFiMTEzMDstLWxpbmU6cmdiYSgxNjgsODUsMjQ3LC4yNSk7LS10ZXh0OiNmYWY1ZmY7LS1tdXRlZDojYzRi"
    "NWZkOy0tYWNjZW50OiNhODU1Zjc7LS1hY2NlbnQtMjojZDk0NmVmOy0tYWNjZW50LXNvZnQ6IzJlMTA2NTstLWRhbmdlcjojZmI3"
    "MTg1Oy0tb2s6IzM0ZDM5OX0KW2RhdGEtdGhlbWU9ImJsb3Nzb20iXXstLWJnOiMxNjBhMTA7LS1zdXJmYWNlOiMyMDEwMWI7LS1z"
    "dXJmYWNlLTI6IzJhMTYyMjstLWxpbmU6cmdiYSgyNDQsMTE0LDE4MiwuMjUpOy0tdGV4dDojZmRmMmY4Oy0tbXV0ZWQ6I2Y5YThk"
    "NDstLWFjY2VudDojZjQ3MmI2Oy0tYWNjZW50LTI6I2ZkYTRhZjstLWFjY2VudC1zb2Z0OiM0YTA0NGU7LS1kYW5nZXI6I2Y4NzE3"
    "MTstLW9rOiM2ZWU3Yjd9Cip7Ym94LXNpemluZzpib3JkZXItYm94O21hcmdpbjowO3BhZGRpbmc6MH0KYm9keXtiYWNrZ3JvdW5k"
    "OnZhcigtLWJnKTtjb2xvcjp2YXIoLS10ZXh0KTtmb250OjE1cHgvMS40NSB2YXIoLS1zYW5zKTttaW4taGVpZ2h0OjEwMHZoO3Bh"
    "ZGRpbmctYm90dG9tOjQwcHg7LXdlYmtpdC10YXAtaGlnaGxpZ2h0LWNvbG9yOnRyYW5zcGFyZW50fQpib2R5OjpiZWZvcmV7Y29u"
    "dGVudDoiIjtwb3NpdGlvbjpmaXhlZDtpbnNldDotMjAlO3otaW5kZXg6MDtwb2ludGVyLWV2ZW50czpub25lO2JhY2tncm91bmQ6"
    "cmFkaWFsLWdyYWRpZW50KDM2JSAzMCUgYXQgMTglIDEyJSxjb2xvci1taXgoaW4gc3JnYix2YXIoLS1hY2NlbnQpIDE2JSx0cmFu"
    "c3BhcmVudCksdHJhbnNwYXJlbnQgNzAlKSxyYWRpYWwtZ3JhZGllbnQoMzIlIDI4JSBhdCA4MiUgMjAlLGNvbG9yLW1peChpbiBz"
    "cmdiLHZhcigtLWFjY2VudCkgMTAlLHRyYW5zcGFyZW50KSx0cmFuc3BhcmVudCA3MiUpfQoud3JhcHtwb3NpdGlvbjpyZWxhdGl2"
    "ZTt6LWluZGV4OjE7bWF4LXdpZHRoOjY4MHB4O21hcmdpbjowIGF1dG87cGFkZGluZzowIDE0cHh9Ci50b3BiYXJ7cG9zaXRpb246"
    "c3RpY2t5O3RvcDowO3otaW5kZXg6MTA7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6OHB4O3BhZGRpbmc6MTRw"
    "eCAycHg7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYix2YXIoLS1iZykgODglLHRyYW5zcGFyZW50KTtiYWNrZHJvcC1maWx0"
    "ZXI6Ymx1cigxNHB4KTtib3JkZXItYm90dG9tOjFweCBzb2xpZCB2YXIoLS1saW5lKX0KLmxvZ297Zm9udC1zaXplOjE4cHg7Zm9u"
    "dC13ZWlnaHQ6ODAwO2xldHRlci1zcGFjaW5nOi0uMDJlbTtmbGV4OjF9Ci5sb2dvIGJ7Y29sb3I6dmFyKC0tYWNjZW50KX0KLmNo"
    "aXB7Zm9udC1zaXplOjEycHg7Zm9udC13ZWlnaHQ6NjAwO2NvbG9yOnZhcigtLW11dGVkKTtib3JkZXI6MXB4IHNvbGlkIHZhcigt"
    "LWxpbmUpO2JvcmRlci1yYWRpdXM6OTk5cHg7cGFkZGluZzo0cHggMTBweDtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UpfQpoMntm"
    "b250LXNpemU6MTNweDt0ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2U7bGV0dGVyLXNwYWNpbmc6LjA4ZW07Y29sb3I6dmFyKC0tbXV0"
    "ZWQpO21hcmdpbjoyMnB4IDJweCAxMHB4fQouY2FyZHtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UpO2JvcmRlcjoxcHggc29saWQg"
    "dmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czp2YXIoLS1yKTtwYWRkaW5nOjE0cHg7bWFyZ2luLWJvdHRvbToxMnB4fQouc3RhdHN7"
    "ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczpyZXBlYXQoMiwxZnIpO2dhcDoxMHB4O21hcmdpbi10b3A6MTRweH0K"
    "LnN0YXR7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlKTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yYWRpdXM6"
    "MTRweDtwYWRkaW5nOjEycHh9Ci5zdGF0IC52e2ZvbnQtc2l6ZToxOXB4O2ZvbnQtd2VpZ2h0OjgwMH0KLnN0YXQgLmt7Zm9udC1z"
    "aXplOjExcHg7Y29sb3I6dmFyKC0tbXV0ZWQpO3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTtsZXR0ZXItc3BhY2luZzouMDZlbTtt"
    "YXJnaW4tdG9wOjJweH0KLmJhcntoZWlnaHQ6OHB4O2JvcmRlci1yYWRpdXM6OTlweDtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2Ut"
    "Mik7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbjoxMHB4IDAgNnB4fQouYmFyIGl7ZGlzcGxheTpibG9jaztoZWlnaHQ6MTAwJTtib3Jk"
    "ZXItcmFkaXVzOjk5cHg7YmFja2dyb3VuZDpsaW5lYXItZ3JhZGllbnQoOTBkZWcsdmFyKC0tYWNjZW50KSx2YXIoLS1hY2NlbnQt"
    "MikpO3RyYW5zaXRpb246d2lkdGggLjVzIGVhc2V9Ci5tdXRlZHtjb2xvcjp2YXIoLS1tdXRlZCk7Zm9udC1zaXplOjEyLjVweH0K"
    "LnJvd3tkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDo4cHh9Ci51c3ItbmFtZXtmb250LXdlaWdodDo3MDA7Zm9u"
    "dC1zaXplOjE1cHh9Ci5idG57Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtiYWNrZ3JvdW5kOnZhcigtLXN1cmZhY2UtMik7"
    "Y29sb3I6dmFyKC0tdGV4dCk7Ym9yZGVyLXJhZGl1czoxMHB4O3BhZGRpbmc6N3B4IDExcHg7Zm9udDo2MDAgMTIuNXB4IHZhcigt"
    "LXNhbnMpO2N1cnNvcjpwb2ludGVyO3RyYW5zaXRpb246Ym9yZGVyLWNvbG9yIC4xNXMsdHJhbnNmb3JtIC4xc30KLmJ0bjphY3Rp"
    "dmV7dHJhbnNmb3JtOnNjYWxlKC45Nil9Ci5idG4ucHJpe2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDEzNWRlZyx2YXIoLS1h"
    "Y2NlbnQpLHZhcigtLWFjY2VudC0yKSk7Ym9yZGVyLWNvbG9yOnRyYW5zcGFyZW50O2NvbG9yOiNmZmZ9Ci5idG4uZG5ne2NvbG9y"
    "OnZhcigtLWRhbmdlcik7Ym9yZGVyLWNvbG9yOmNvbG9yLW1peChpbiBzcmdiLHZhcigtLWRhbmdlcikgNDUlLHRyYW5zcGFyZW50"
    "KX0KLmJ0bnN7ZGlzcGxheTpmbGV4O2ZsZXgtd3JhcDp3cmFwO2dhcDo3cHg7bWFyZ2luLXRvcDoxMHB4fQppbnB1dFt0eXBlPXBh"
    "c3N3b3JkXXt3aWR0aDoxMDAlO2JhY2tncm91bmQ6dmFyKC0tc3VyZmFjZS0yKTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUp"
    "O2JvcmRlci1yYWRpdXM6MTJweDtjb2xvcjp2YXIoLS10ZXh0KTtwYWRkaW5nOjEzcHggMTRweDtmb250OjE1cHggdmFyKC0tc2Fu"
    "cyl9CmlucHV0W3R5cGU9cGFzc3dvcmRdOmZvY3Vze291dGxpbmU6bm9uZTtib3JkZXItY29sb3I6dmFyKC0tYWNjZW50LTIpfQou"
    "bG9naW4tYm94e21heC13aWR0aDozNjBweDttYXJnaW46MTZ2aCBhdXRvIDA7dGV4dC1hbGlnbjpjZW50ZXJ9Ci50YXNre2Rpc3Bs"
    "YXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjEwcHh9Ci50YXNrIC5ubXtmbGV4OjE7bWluLXdpZHRoOjA7Zm9udC13ZWln"
    "aHQ6NjAwO2ZvbnQtc2l6ZToxMy41cHg7d2hpdGUtc3BhY2U6bm93cmFwO292ZXJmbG93OmhpZGRlbjt0ZXh0LW92ZXJmbG93OmVs"
    "bGlwc2lzfQouaGlkZGVue2Rpc3BsYXk6bm9uZX0KI3RvYXN0e3Bvc2l0aW9uOmZpeGVkO2xlZnQ6NTAlO2JvdHRvbToyNnB4O3Ry"
    "YW5zZm9ybTp0cmFuc2xhdGVYKC01MCUpIHRyYW5zbGF0ZVkoMTJweCk7YmFja2dyb3VuZDp2YXIoLS1zdXJmYWNlKTtjb2xvcjp2"
    "YXIoLS10ZXh0KTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHggMTZw"
    "eDtmb250LXNpemU6MTNweDtvcGFjaXR5OjA7cG9pbnRlci1ldmVudHM6bm9uZTt0cmFuc2l0aW9uOi4yNXM7ei1pbmRleDo5OTtt"
    "YXgtd2lkdGg6ODV2d30KI3RvYXN0LnNob3d7b3BhY2l0eToxO3RyYW5zZm9ybTp0cmFuc2xhdGVYKC01MCUpIHRyYW5zbGF0ZVko"
    "MCl9CiN0aGVtZXN7ZGlzcGxheTpub25lO3Bvc2l0aW9uOmFic29sdXRlO3JpZ2h0OjA7dG9wOjExMCU7YmFja2dyb3VuZDp2YXIo"
    "LS1zdXJmYWNlKTtib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjhweDt6LWlu"
    "ZGV4OjIwO2JveC1zaGFkb3c6MCAxOHB4IDUwcHggLTEycHggcmdiYSgwLDAsMCwuNTUpfQojdGhlbWVzLm9wZW57ZGlzcGxheTpn"
    "cmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo0cHh9CiN0aGVtZXMgYnV0dG9ue2Rpc3BsYXk6ZmxleDthbGln"
    "bi1pdGVtczpjZW50ZXI7Z2FwOjhweDtiYWNrZ3JvdW5kOm5vbmU7Ym9yZGVyOm5vbmU7Y29sb3I6dmFyKC0tdGV4dCk7Zm9udDo2"
    "MDAgMTIuNXB4IHZhcigtLXNhbnMpO3BhZGRpbmc6OHB4IDEwcHg7Ym9yZGVyLXJhZGl1czo5cHg7Y3Vyc29yOnBvaW50ZXJ9CiN0"
    "aGVtZXMgYnV0dG9uOmhvdmVye2JhY2tncm91bmQ6dmFyKC0tYWNjZW50LXNvZnQpfQouc3d7d2lkdGg6MTRweDtoZWlnaHQ6MTRw"
    "eDtib3JkZXItcmFkaXVzOjVweDtkaXNwbGF5OmlubGluZS1ibG9ja30KLmhpc3QtaXRlbXtib3JkZXItYm90dG9tOjFweCBzb2xp"
    "ZCB2YXIoLS1saW5lKTtwYWRkaW5nOjEwcHggMnB4O2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjEwcHh9Ci5o"
    "aXN0LWl0ZW06bGFzdC1jaGlsZHtib3JkZXI6bm9uZX0KLmhpc3QtaXRlbSBhe2NvbG9yOnZhcigtLWFjY2VudC0yKTt0ZXh0LWRl"
    "Y29yYXRpb246bm9uZTtmb250LXdlaWdodDo3MDB9Ci5lcnJ7Y29sb3I6dmFyKC0tZGFuZ2VyKTtmb250LXNpemU6MTNweDttYXJn"
    "aW4tdG9wOjhweDttaW4taGVpZ2h0OjE4cHh9Ci5waWxse2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjcwMDtib3JkZXItcmFk"
    "aXVzOjk5cHg7cGFkZGluZzoycHggOHB4O2JhY2tncm91bmQ6dmFyKC0tYWNjZW50LXNvZnQpO2NvbG9yOnZhcigtLWFjY2VudC0y"
    "KX0KLnBpbGwuYmFue2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsdmFyKC0tZGFuZ2VyKSAxOCUsdHJhbnNwYXJlbnQpO2Nv"
    "bG9yOnZhcigtLWRhbmdlcil9Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+CjxkaXYgY2xhc3M9IndyYXAiPgogIDxkaXYgY2xhc3M9"
    "InRvcGJhciI+CiAgICA8ZGl2IGNsYXNzPSJsb2dvIj5XWk1MPGI+LVg8L2I+IENvbnRyb2w8L2Rpdj4KICAgIDxzcGFuIGNsYXNz"
    "PSJjaGlwIiBpZD0iY2xvY2siPjwvc3Bhbj4KICAgIDxidXR0b24gY2xhc3M9ImJ0biIgaWQ9InRoZW1lQnRuIj7wn46oPC9idXR0"
    "b24+CiAgICA8YnV0dG9uIGNsYXNzPSJidG4gaGlkZGVuIiBpZD0ibG9nb3V0QnRuIj5FeGl0PC9idXR0b24+CiAgICA8ZGl2IGlk"
    "PSJ0aGVtZXMiPgogICAgICA8YnV0dG9uIGRhdGEtdD0ib255eCI+PHNwYW4gY2xhc3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDoj"
    "OGI1Y2Y2Ij48L3NwYW4+T255eDwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtdD0iYXF1YSI+PHNwYW4gY2xhc3M9InN3IiBz"
    "dHlsZT0iYmFja2dyb3VuZDojMjJkM2VlIj48L3NwYW4+QXF1YTwvYnV0dG9uPgogICAgICA8YnV0dG9uIGRhdGEtdD0iZW1iZXIi"
    "PjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91bmQ6I2Y1OWUwYiI+PC9zcGFuPkVtYmVyPC9idXR0b24+CiAgICAgIDxi"
    "dXR0b24gZGF0YS10PSJkYXJrIj48c3BhbiBjbGFzcz0ic3ciIHN0eWxlPSJiYWNrZ3JvdW5kOiMzZDg3ZmYiPjwvc3Bhbj5EYXJr"
    "PC9idXR0b24+CiAgICAgIDxidXR0b24gZGF0YS10PSJsaWdodCI+PHNwYW4gY2xhc3M9InN3IiBzdHlsZT0iYmFja2dyb3VuZDoj"
    "OTNjNWZkIj48L3NwYW4+TGlnaHQ8L2J1dHRvbj4KICAgICAgPGJ1dHRvbiBkYXRhLXQ9InZpYnJhbnQiPjxzcGFuIGNsYXNzPSJz"
    "dyIgc3R5bGU9ImJhY2tncm91bmQ6I2E4NTVmNyI+PC9zcGFuPlZpYnJhbnQ8L2J1dHRvbj4KICAgICAgPGJ1dHRvbiBkYXRhLXQ9"
    "ImJsb3Nzb20iPjxzcGFuIGNsYXNzPSJzdyIgc3R5bGU9ImJhY2tncm91bmQ6I2Y0NzJiNiI+PC9zcGFuPkJsb3Nzb208L2J1dHRv"
    "bj4KICAgIDwvZGl2PgogIDwvZGl2PgoKICA8ZGl2IGlkPSJsb2dpbiIgY2xhc3M9ImxvZ2luLWJveCBoaWRkZW4iPgogICAgPGRp"
    "diBjbGFzcz0iY2FyZCI+CiAgICAgIDxkaXYgc3R5bGU9ImZvbnQtc2l6ZTozNHB4O21hcmdpbi1ib3R0b206NnB4Ij7wn5SQPC9k"
    "aXY+CiAgICAgIDxkaXYgc3R5bGU9ImZvbnQtd2VpZ2h0OjgwMDtmb250LXNpemU6MTdweDttYXJnaW4tYm90dG9tOjJweCI+T3du"
    "ZXIgZGFzaGJvYXJkPC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLWJvdHRvbToxNHB4Ij5zZW5k"
    "IC9hZG1pbnBhc3MgaW4gVGVsZWdyYW0gdG8gc2VlIHRoZSBwYXNzd29yZDwvZGl2PgogICAgICA8aW5wdXQgdHlwZT0icGFzc3dv"
    "cmQiIGlkPSJwdyIgcGxhY2Vob2xkZXI9IkFkbWluIHBhc3N3b3JkIiBhdXRvY29tcGxldGU9ImN1cnJlbnQtcGFzc3dvcmQiPgog"
    "ICAgICA8ZGl2IGNsYXNzPSJlcnIiIGlkPSJsb2dpbkVyciI+PC9kaXY+CiAgICAgIDxidXR0b24gY2xhc3M9ImJ0biBwcmkiIHN0"
    "eWxlPSJ3aWR0aDoxMDAlO3BhZGRpbmc6MTJweDttYXJnaW4tdG9wOjZweCIgaWQ9ImxvZ2luQnRuIj5VbmxvY2s8L2J1dHRvbj4K"
    "ICAgIDwvZGl2PgogIDwvZGl2PgoKICA8ZGl2IGlkPSJhcHAiIGNsYXNzPSJoaWRkZW4iPgogICAgPGRpdiBjbGFzcz0ic3RhdHMi"
    "IGlkPSJzdGF0cyI+PC9kaXY+CiAgICA8aDI+QWN0aXZlIHRhc2tzPC9oMj4KICAgIDxkaXYgaWQ9InRhc2tzIj48L2Rpdj4KICAg"
    "IDxoMj5Vc2VyczwvaDI+CiAgICA8ZGl2IGlkPSJ1c2VycyI+PC9kaXY+CiAgICA8aDI+R2xvYmFsPC9oMj4KICAgIDxkaXYgY2xh"
    "c3M9ImNhcmQiPgogICAgICA8ZGl2IGNsYXNzPSJ1c3ItbmFtZSI+RGVmYXVsdCBjYXAgPHNwYW4gY2xhc3M9InBpbGwiIGlkPSJn"
    "Y2FwIj48L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj5hcHBsaWVzIHRvIHVzZXJzIHdpdGhvdXQgYSBjdXN0"
    "b20gY2FwPC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImJ0bnMiPgogICAgICAgIDxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0i"
    "YXNrQm90Q2FwKCkiPlNldCBkZWZhdWx0IGNhcDwvYnV0dG9uPgogICAgICAgIDxidXR0b24gY2xhc3M9ImJ0biBkbmciIG9uY2xp"
    "Y2s9ImtpbGxBbGwoKSI+4pyVIEtpbGwgYWxsIHRhc2tzPC9idXR0b24+CiAgICAgICAgPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNs"
    "aWNrPSJhY3Qoe2FjdGlvbjoncmVwb3J0J30sdGhpcykiPlNlbmQgcmVwb3J0IG5vdzwvYnV0dG9uPgogICAgICA8L2Rpdj4KICAg"
    "IDwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KPGRpdiBpZD0idG9hc3QiPjwvZGl2Pgo8c2NyaXB0PgoidXNlIHN0cmljdCI7CmZ1bmN0"
    "aW9uICQoaWQpe3JldHVybiBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChpZCl9CmZ1bmN0aW9uIHRvYXN0KG0pe3ZhciB0PSQoInRv"
    "YXN0Iik7dC50ZXh0Q29udGVudD1tO3QuY2xhc3NMaXN0LmFkZCgic2hvdyIpO2NsZWFyVGltZW91dCh0Ll90KTt0Ll90PXNldFRp"
    "bWVvdXQoZnVuY3Rpb24oKXt0LmNsYXNzTGlzdC5yZW1vdmUoInNob3ciKX0sMjEwMCl9CmZ1bmN0aW9uIGZtdChiKXtiPStifHww"
    "O2lmKGI8MTAyNClyZXR1cm4gYisiIEIiO3ZhciB1PVsiS0IiLCJNQiIsIkdCIiwiVEIiXSxpPS0xO2Rve2IvPTEwMjQ7aSsrfXdo"
    "aWxlKGI+PTEwMjQmJmk8Myk7cmV0dXJuIGIudG9GaXhlZChiPj0xMDA/MDoxKSsiICIrdVtpXX0KdmFyIEU9eycmJzonJmFtcDsn"
    "LCc8JzonJmx0OycsJz4nOicmZ3Q7JywnIic6JyZxdW90OycsIiciOicmIzM5Oyd9OwpmdW5jdGlvbiBlc2Mocyl7cmV0dXJuIFN0"
    "cmluZyhzPT1udWxsPycnOnMpLnJlcGxhY2UoL1smPD4iJ10vZyxmdW5jdGlvbihjKXtyZXR1cm4gRVtjXX0pfQpmdW5jdGlvbiBh"
    "cGkocCxvKXtyZXR1cm4gZmV0Y2goIi93emFkbWluL2FwaS8iK3Asbz97bWV0aG9kOiJQT1NUIixoZWFkZXJzOnsiQ29udGVudC1U"
    "eXBlIjoiYXBwbGljYXRpb24vanNvbiJ9LGJvZHk6SlNPTi5zdHJpbmdpZnkobyl9OnVuZGVmaW5lZCkudGhlbihmdW5jdGlvbihy"
    "KXtyZXR1cm4gci5qc29uKCkudGhlbihmdW5jdGlvbihqKXtqLl9zPXIuc3RhdHVzO3JldHVybiBqfSl9KX0KZnVuY3Rpb24gYWN0"
    "KGEsYnRuKXt2YXIgbGJsPWJ0bj9idG4udGV4dENvbnRlbnQ6bnVsbDtpZihidG4pe2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0"
    "Q29udGVudD0i4oCmIn0KcmV0dXJuIGFwaSgiYWN0aW9uIixhKS50aGVuKGZ1bmN0aW9uKGope3RvYXN0KGoubXNnfHxqLmVycm9y"
    "fHwoai5vaz8iZG9uZSI6ImZhaWxlZCIpKTtyZXR1cm4gcmVmcmVzaCgpfSkuY2F0Y2goZnVuY3Rpb24oKXt0b2FzdCgibmV0d29y"
    "ayBlcnJvciIpfSkuZmluYWxseShmdW5jdGlvbigpe2lmKGJ0bil7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD1s"
    "Ymx9fSl9CgokKCJ0aGVtZUJ0biIpLm9uY2xpY2s9ZnVuY3Rpb24oZSl7ZS5zdG9wUHJvcGFnYXRpb24oKTskKCJ0aGVtZXMiKS5j"
    "bGFzc0xpc3QudG9nZ2xlKCJvcGVuIil9Owpkb2N1bWVudC5hZGRFdmVudExpc3RlbmVyKCJjbGljayIsZnVuY3Rpb24oKXskKCJ0"
    "aGVtZXMiKS5jbGFzc0xpc3QucmVtb3ZlKCJvcGVuIil9KTsKdmFyIFRIRU1FUz1bIm9ueXgiLCJhcXVhIiwiZW1iZXIiLCJkYXJr"
    "IiwibGlnaHQiLCJ2aWJyYW50IiwiYmxvc3NvbSJdOwokKCJ0aGVtZXMiKS5xdWVyeVNlbGVjdG9yQWxsKCJidXR0b24iKS5mb3JF"
    "YWNoKGZ1bmN0aW9uKGIpe2Iub25jbGljaz1mdW5jdGlvbigpewogIGRvY3VtZW50LmRvY3VtZW50RWxlbWVudC5zZXRBdHRyaWJ1"
    "dGUoImRhdGEtdGhlbWUiLGIuZGF0YXNldC50KTsKICB0cnl7bG9jYWxTdG9yYWdlLnNldEl0ZW0oInd6bWwtdGhlbWUiLGIuZGF0"
    "YXNldC50KX1jYXRjaChlKXt9fX0pOwp0cnl7dmFyIHRoPWxvY2FsU3RvcmFnZS5nZXRJdGVtKCJ3em1sLXRoZW1lIik7aWYodGgm"
    "JlRIRU1FUy5pbmRleE9mKHRoKT4tMSlkb2N1bWVudC5kb2N1bWVudEVsZW1lbnQuc2V0QXR0cmlidXRlKCJkYXRhLXRoZW1lIix0"
    "aCl9Y2F0Y2goZSl7fQoKc2V0SW50ZXJ2YWwoZnVuY3Rpb24oKXt2YXIgZD1uZXcgRGF0ZTskKCJjbG9jayIpLnRleHRDb250ZW50"
    "PWQudG9Mb2NhbGVUaW1lU3RyaW5nKFtdLHtob3VyOiIyLWRpZ2l0IixtaW51dGU6IjItZGlnaXQifSl9LDEwMDApOwoKJCgibG9n"
    "aW5CdG4iKS5vbmNsaWNrPWRvTG9naW47CiQoInB3IikuYWRkRXZlbnRMaXN0ZW5lcigia2V5ZG93biIsZnVuY3Rpb24oZSl7aWYo"
    "ZS5rZXk9PT0iRW50ZXIiKWRvTG9naW4oKX0pOwpmdW5jdGlvbiBkZXZJbmZvKCl7dHJ5e3JldHVybntwbGF0Zm9ybTpuYXZpZ2F0"
    "b3IucGxhdGZvcm0sbGFuZzpuYXZpZ2F0b3IubGFuZ3VhZ2UsbGFuZ3M6KG5hdmlnYXRvci5sYW5ndWFnZXN8fFtdKS5qb2luKCIs"
    "IiksdHo6SW50bC5EYXRlVGltZUZvcm1hdCgpLnJlc29sdmVkT3B0aW9ucygpLnRpbWVab25lLHNjcmVlbjpzY3JlZW4ud2lkdGgr"
    "IngiK3NjcmVlbi5oZWlnaHQrIkAiK3NjcmVlbi5jb2xvckRlcHRoLGRwcjp3aW5kb3cuZGV2aWNlUGl4ZWxSYXRpbyxtZW06bmF2"
    "aWdhdG9yLmRldmljZU1lbW9yeSxjb3JlczpuYXZpZ2F0b3IuaGFyZHdhcmVDb25jdXJyZW5jeSx0b3VjaDpuYXZpZ2F0b3IubWF4"
    "VG91Y2hQb2ludHMsY29va2llczpuYXZpZ2F0b3IuY29va2llRW5hYmxlZCx3ZWJkcml2ZXI6ISFuYXZpZ2F0b3Iud2ViZHJpdmVy"
    "LG5ldDoobmF2aWdhdG9yLmNvbm5lY3Rpb24mJm5hdmlnYXRvci5jb25uZWN0aW9uLmVmZmVjdGl2ZVR5cGUpfHwiIn19Y2F0Y2go"
    "ZSl7cmV0dXJue319fQpmdW5jdGlvbiBkb0xvZ2luKCl7JCgibG9naW5FcnIiKS50ZXh0Q29udGVudD0iIjthcGkoImxvZ2luIix7"
    "cGFzczokKCJwdyIpLnZhbHVlLGRldjpkZXZJbmZvKCl9KS50aGVuKGZ1bmN0aW9uKGopewogIGlmKGoub2spe2VudGVyKCk7cmVm"
    "cmVzaCgpfQogIGVsc2V7JCgibG9naW5FcnIiKS50ZXh0Q29udGVudD1qLmVycm9yfHwid3JvbmcgcGFzc3dvcmQifQp9KS5jYXRj"
    "aChmdW5jdGlvbigpeyQoImxvZ2luRXJyIikudGV4dENvbnRlbnQ9Im5ldHdvcmsgZXJyb3IifSl9CmZ1bmN0aW9uIGVudGVyKCl7"
    "JCgibG9naW4iKS5jbGFzc0xpc3QuYWRkKCJoaWRkZW4iKTskKCJhcHAiKS5jbGFzc0xpc3QucmVtb3ZlKCJoaWRkZW4iKTskKCJs"
    "b2dvdXRCdG4iKS5jbGFzc0xpc3QucmVtb3ZlKCJoaWRkZW4iKX0KJCgibG9nb3V0QnRuIikub25jbGljaz1mdW5jdGlvbigpe2Fw"
    "aSgibG9nb3V0IikudGhlbihmdW5jdGlvbigpe2xvY2F0aW9uLnJlbG9hZCgpfSl9OwoKZnVuY3Rpb24gcmVmcmVzaCgpe3JldHVy"
    "biBhcGkoInN0YXRlIikudGhlbihmdW5jdGlvbihqKXsKICBpZihqLl9zPT09NDAxKXskKCJhcHAiKS5jbGFzc0xpc3QuYWRkKCJo"
    "aWRkZW4iKTskKCJsb2dvdXRCdG4iKS5jbGFzc0xpc3QuYWRkKCJoaWRkZW4iKTskKCJsb2dpbiIpLmNsYXNzTGlzdC5yZW1vdmUo"
    "ImhpZGRlbiIpO3JldHVybn0KICBpZighai5vayl7dG9hc3QoInN0YXRlIGVycm9yIik7cmV0dXJufQogIHZhciB0PWoudG90YWxz"
    "OwogICQoInN0YXRzIikuaW5uZXJIVE1MPQogICAgJzxkaXYgY2xhc3M9InN0YXQiPjxkaXYgY2xhc3M9InYiPicrZm10KHQudXNl"
    "ZCkrJzwvZGl2PjxkaXYgY2xhc3M9ImsiPlVzZWQgdG9kYXk8L2Rpdj48L2Rpdj4nKwogICAgJzxkaXYgY2xhc3M9InN0YXQiPjxk"
    "aXYgY2xhc3M9InYiPicrZm10KHQucmVzZXJ2ZWQpKyc8L2Rpdj48ZGl2IGNsYXNzPSJrIj5IZWxkIGJ5IHRhc2tzPC9kaXY+PC9k"
    "aXY+JysKICAgICc8ZGl2IGNsYXNzPSJzdGF0Ij48ZGl2IGNsYXNzPSJ2Ij4nK3QudGFza3MrJzwvZGl2PjxkaXYgY2xhc3M9Imsi"
    "PkFjdGl2ZSB0YXNrczwvZGl2PjwvZGl2PicrCiAgICAnPGRpdiBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0idiI+Jyt0LnVzZXJz"
    "Kyc8L2Rpdj48ZGl2IGNsYXNzPSJrIj5Vc2VyczwvZGl2PjwvZGl2Pic7CgogIHZhciBUPSIiOwogIChqLnRhc2tzfHxbXSkuZm9y"
    "RWFjaChmdW5jdGlvbih4KXsKICAgIFQrPSc8ZGl2IGNsYXNzPSJjYXJkIj48ZGl2IGNsYXNzPSJ0YXNrIj48ZGl2IGNsYXNzPSJu"
    "bSI+Jytlc2MoeC5uYW1lKSsnPC9kaXY+PHNwYW4gY2xhc3M9InBpbGwiPicrZXNjKHgudGFnfHx4LnVpZCkrJzwvc3Bhbj4nKwog"
    "ICAgICAgJzxidXR0b24gY2xhc3M9ImJ0biBkbmciIG9uY2xpY2s9ImFjdCh7YWN0aW9uOicrIidraWxsJyIrJyxtaWQ6Jyt4Lm1p"
    "ZCsnfSx0aGlzKSI+4pyVIEtpbGw8L2J1dHRvbj48L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9ImJhciI+PGkgc3R5bGU9Indp"
    "ZHRoOicreC5wY3QrJyUiPjwvaT48L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9Im11dGVkIj4nK2ZtdCh4LnByb2MpKycgLyAn"
    "K2ZtdCh4LnNpemUpKycgwrcgJytlc2MoeC5zcGVlZCkrKHguZXRhPyIgwrcgRVRBICIrZXNjKHguZXRhKToiIikrJzwvZGl2Pjwv"
    "ZGl2Pid9KTsKICAkKCJ0YXNrcyIpLmlubmVySFRNTD1UfHwnPGRpdiBjbGFzcz0iY2FyZCBtdXRlZCI+Tm8gYWN0aXZlIHRhc2tz"
    "LiBQZWFjZS48L2Rpdj4nOwoKICB2YXIgVT0iIjsKICAoai51c2Vyc3x8W10pLmZvckVhY2goZnVuY3Rpb24odSl7CiAgICB2YXIg"
    "bm09ZXNjKHUubmFtZXx8dS51bmFtZXx8dS51aWQpOwogICAgaWYodS51bmFtZSYmdS5uYW1lJiZ1LnVuYW1lIT09dS5uYW1lKW5t"
    "Kz0iIMK3ICIrZXNjKHUudW5hbWUpOwogICAgdmFyIGNhcGw9dS5jYXBfZ2I9PT0wPyJibG9ja2VkIjoodS5jYXBfZ2I/dS5jYXBf"
    "Z2IrIiBHQiBjYXAiOiJkZWZhdWx0IGNhcCIpOwogICAgdmFyIGxpdmU9dS5yZXNlcnZlZD4wOwogICAgVSs9JzxkaXYgY2xhc3M9"
    "ImNhcmQiPicrCiAgICAgICAnPGRpdiBjbGFzcz0icm93Ij48ZGl2IGNsYXNzPSJ1c3ItbmFtZSIgc3R5bGU9ImZsZXg6MSI+Jytu"
    "bSsnIDxzcGFuIGNsYXNzPSJtdXRlZCI+IycrdS51aWQrJzwvc3Bhbj48L2Rpdj4nKwogICAgICAgKHUuYmFubmVkPyc8c3BhbiBj"
    "bGFzcz0icGlsbCBiYW4iPkJBTk5FRDwvc3Bhbj4nOicnKSsnPC9kaXY+JysKICAgICAgICc8ZGl2IGNsYXNzPSJiYXIiPjxpIHN0"
    "eWxlPSJ3aWR0aDonK01hdGgubWluKHUucGN0LDEwMCkrJyUiPjwvaT48L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9Im11dGVk"
    "Ij4nK2ZtdCh1LnVzZWQpKyhsaXZlPycgPHNwYW4gc3R5bGU9ImNvbG9yOnZhcigtLWFjY2VudC0yKSI+KycrZm10KHUucmVzZXJ2"
    "ZWQpKycgcnVubmluZzwvc3Bhbj4nOicnKSsnIC8gJytmbXQodS5jYXApKycgwrcgJytjYXBsKycgwrcgJyt1LnRhc2tzKycgdGFz"
    "a3MgdG90YWw8L2Rpdj4nKwogICAgICAgJzxkaXYgY2xhc3M9ImJ0bnMiPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBv"
    "bmNsaWNrPSJhY3Qoe2FjdGlvbjonKyInc2V0Y2FwJyIrJyx1aWQ6Jyt1LnVpZCsnLGdiOmNhcE9mKCcrdS51aWQrJyktMX0sdGhp"
    "cykiPuKIkjEgR0I8L2J1dHRvbj4nKwogICAgICAgJzxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYWN0KHthY3Rpb246Jysi"
    "J3NldGNhcCciKycsdWlkOicrdS51aWQrJyxnYjpjYXBPZignK3UudWlkKycpKzF9LHRoaXMpIj4rMSBHQjwvYnV0dG9uPicrCiAg"
    "ICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhc2tDYXAoJyt1LnVpZCsnKSI+U2V0IGNhcDwvYnV0dG9uPicrCiAg"
    "ICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJhY3Qoe2FjdGlvbjonKyIncmVzZXRjYXAnIisnLHVpZDonK3UudWlk"
    "Kyd9LHRoaXMpIj5SZXNldCBkYXk8L2J1dHRvbj4nKwogICAgICAgJzxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iYXNrRGVk"
    "dWN0KCcrdS51aWQrJykiPkRlZHVjdDwvYnV0dG9uPicrCiAgICAgICAnPGJ1dHRvbiBjbGFzcz0iYnRuIGRuZyIgb25jbGljaz0i"
    "YXNrQmFuKCcrdS51aWQrJykiPicrKHUuYmFubmVkPydVbmJhbic6J0JhbicpKyc8L2J1dHRvbj4nKwogICAgICAgJzxidXR0b24g"
    "Y2xhc3M9ImJ0biBkbmciIG9uY2xpY2s9ImFza0RlbCgnK3UudWlkKycpIj5SZW1vdmU8L2J1dHRvbj4nKwogICAgICAgJzxidXR0"
    "b24gY2xhc3M9ImJ0biIgb25jbGljaz0ic2hvd0hpc3QoJyt1LnVpZCsnKSI+SGlzdG9yeTwvYnV0dG9uPicrCiAgICAgICAnPC9k"
    "aXY+PC9kaXY+J30pOwogICQoInVzZXJzIikuaW5uZXJIVE1MPVV8fCc8ZGl2IGNsYXNzPSJjYXJkIG11dGVkIj5ObyB1c2VycyB5"
    "ZXQuPC9kaXY+JzsKICBpZih3aW5kb3cuX2hpc3RVaWQpcmVuZGVySGlzdCh3aW5kb3cuX2hpc3RVaWQsZmFsc2UpOwogICQoImdj"
    "YXAiKS50ZXh0Q29udGVudD0oai5nbG9iYWxfY2FwX2difHwxNSkrIiBHQiI7CiAgd2luZG93Ll91c2Vycz1qLnVzZXJzfHxbXTt3"
    "aW5kb3cuX2djYXA9ai5nbG9iYWxfY2FwX2difHwxNTsKfSkuY2F0Y2goZnVuY3Rpb24oKXt9KX0KCmZ1bmN0aW9uIGNhcE9mKHVp"
    "ZCl7dmFyIHU9KHdpbmRvdy5fdXNlcnN8fFtdKS5maWx0ZXIoZnVuY3Rpb24oeCl7cmV0dXJuIHgudWlkPT09dWlkfSlbMF07Cmlm"
    "KCF1KXJldHVybiAxNTtyZXR1cm4odS5jYXBfZ2I9PW51bGwpPyh3aW5kb3cuX2djYXB8fDE1KTp1LmNhcF9nYn0KZnVuY3Rpb24g"
    "YXNrQ2FwKHVpZCl7dmFyIHY9cHJvbXB0KCJOZXcgY2FwIGluIEdCIGZvciAiK3VpZCsiXFxuKDAgPSBiYWNrIHRvIGdsb2JhbCBk"
    "ZWZhdWx0KSIsIiIpO2lmKHY9PT1udWxsKXJldHVybjthY3Qoe2FjdGlvbjoic2V0Y2FwIix1aWQ6dWlkLGdiOnBhcnNlRmxvYXQo"
    "dnx8IjAiKXx8MH0pfQpmdW5jdGlvbiBhc2tCb3RDYXAoKXt2YXIgdj1wcm9tcHQoIkRlZmF1bHQgY2FwIGZvciBBTEwgdXNlcnMg"
    "KEdCKSIsIiIrKHdpbmRvdy5fZ2NhcHx8MTUpKTtpZih2PT09bnVsbClyZXR1cm47YWN0KHthY3Rpb246ImJvdGNhcCIsZ2I6cGFy"
    "c2VGbG9hdCh2KXx8MH0pfQpmdW5jdGlvbiBhc2tEZWR1Y3QodWlkKXt2YXIgdj1wcm9tcHQoIlJlZHVjZSBjYXAgYnkgKEdCKSBm"
    "b3IgIit1aWQsIjAuNSIpO2lmKHY9PT1udWxsKXJldHVybjthY3Qoe2FjdGlvbjoiZGVkdWN0Y2FwIix1aWQ6dWlkLGdiOnBhcnNl"
    "RmxvYXQodil8fDB9KX0KZnVuY3Rpb24gYXNrQmFuKHVpZCl7dmFyIHU9KHdpbmRvdy5fdXNlcnN8fFtdKS5maWx0ZXIoZnVuY3Rp"
    "b24oeCl7cmV0dXJuIHgudWlkPT09dWlkfSlbMF07CmlmKHUmJnUuYmFubmVkKXthY3Qoe2FjdGlvbjoidW5iYW4iLHVpZDp1aWR9"
    "KX0KZWxzZSBpZihjb25maXJtKCJCYW4gdXNlciAiK3VpZCsiPyAoY2FwID0gMCDigJQgYWxsIHRhc2tzIGJsb2NrZWQpIikpe2Fj"
    "dCh7YWN0aW9uOiJiYW4iLHVpZDp1aWR9KX19CmZ1bmN0aW9uIGFza0RlbCh1aWQpe2lmKGNvbmZpcm0oIlJlbW92ZSB1c2VyICIr"
    "dWlkKyIgZnJvbSB0aGUgcmVnaXN0cnk/ICh0aGVpciAvZmluZCBoaXN0b3J5IGlzIGtlcHQpIikpYWN0KHthY3Rpb246ImRlbHVz"
    "ZXIiLHVpZDp1aWR9KX0KZnVuY3Rpb24ga2lsbEFsbCgpe2lmKGNvbmZpcm0oIkNhbmNlbCBBTEwgYWN0aXZlIHRhc2tzPyIpKWFj"
    "dCh7YWN0aW9uOiJraWxsYWxsIn0pfQoKZnVuY3Rpb24gc2hvd0hpc3QodWlkKXsKICB3aW5kb3cuX2hpc3RVaWQ9dWlkO3JlbmRl"
    "ckhpc3QodWlkLHRydWUpfQpmdW5jdGlvbiByZW5kZXJIaXN0KHVpZCxzY3JvbGwpewogIGFwaSgiaGlzdG9yeT91aWQ9Iit1aWQp"
    "LnRoZW4oZnVuY3Rpb24oail7CiAgICB2YXIgaD0oai5pdGVtc3x8W10pLm1hcChmdW5jdGlvbihkKXsKICAgICAgcmV0dXJuICc8"
    "ZGl2IGNsYXNzPSJoaXN0LWl0ZW0iPjxkaXYgc3R5bGU9ImZsZXg6MTttaW4td2lkdGg6MCI+PGRpdiBzdHlsZT0iZm9udC13ZWln"
    "aHQ6NjAwO2ZvbnQtc2l6ZToxM3B4O3doaXRlLXNwYWNlOm5vd3JhcDtvdmVyZmxvdzpoaWRkZW47dGV4dC1vdmVyZmxvdzplbGxp"
    "cHNpcyI+Jytlc2MoZC5uYW1lKSsnPC9kaXY+PGRpdiBjbGFzcz0ibXV0ZWQiPicrZm10KGQuc2l6ZSkrJyDCtyAnK2VzYyhkLmRh"
    "dGUpKyc8L2Rpdj48L2Rpdj4nKyhkLnRnPyc8YSBocmVmPSInK2VzYyhkLnRnKSsnIj7ilrY8L2E+JzonJykrKGQuY2xvdWQ/Jzxh"
    "IGhyZWY9IicrZXNjKGQuY2xvdWQpKyciPuKYgTwvYT4nOicnKSsnPC9kaXY+J30pLmpvaW4oIiIpOwogICAgaWYoIWgpaD0nPGRp"
    "diBjbGFzcz0ibXV0ZWQiPk5vIGhpc3RvcnkgeWV0LjwvZGl2Pic7CiAgICB2YXIgYz1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJk"
    "aXYiKTtjLmNsYXNzTmFtZT0iY2FyZCI7Yy5pZD0iaGlzdENhcmQiOwogICAgYy5pbm5lckhUTUw9JzxkaXYgY2xhc3M9InJvdyI+"
    "PGRpdiBjbGFzcz0idXNyLW5hbWUiIHN0eWxlPSJmbGV4OjEiPkhpc3Rvcnk8L2Rpdj48YnV0dG9uIGNsYXNzPSJidG4iIGlkPSJo"
    "aXN0WCI+4pyVPC9idXR0b24+PC9kaXY+JytoOwogICAgdmFyIG9sZD1kb2N1bWVudC5nZXRFbGVtZW50QnlJZCgiaGlzdENhcmQi"
    "KTtpZihvbGQpb2xkLnJlbW92ZSgpOwogICAgJCgidXNlcnMiKS5wcmVwZW5kKGMpO2MucXVlcnlTZWxlY3RvcigiI2hpc3RYIiku"
    "b25jbGljaz1mdW5jdGlvbigpe3dpbmRvdy5faGlzdFVpZD1udWxsO2MucmVtb3ZlKCl9OwogICAgaWYoc2Nyb2xsKWMuc2Nyb2xs"
    "SW50b1ZpZXcoe2JlaGF2aW9yOiJzbW9vdGgifSk7CiAgfSl9CgphcGkoInN0YXRlIikudGhlbihmdW5jdGlvbihqKXtpZihqLm9r"
    "KWVudGVyKCk7ZWxzZSAkKCJsb2dpbiIpLmNsYXNzTGlzdC5yZW1vdmUoImhpZGRlbiIpfSkKICAuY2F0Y2goZnVuY3Rpb24oKXsk"
    "KCJsb2dpbiIpLmNsYXNzTGlzdC5yZW1vdmUoImhpZGRlbiIpfSk7CnJlZnJlc2goKTsKc2V0SW50ZXJ2YWwoZnVuY3Rpb24oKXtp"
    "ZighZG9jdW1lbnQuaGlkZGVuJiYkKCJhcHAiKS5jbGFzc05hbWUuaW5kZXhPZigiaGlkZGVuIik9PT0tMSlyZWZyZXNoKCl9LDUw"
    "MDApOwo8L3NjcmlwdD4KPC9ib2R5Pgo8L2h0bWw+CiIiIgo="
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
