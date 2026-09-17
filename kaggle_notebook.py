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
