#!/usr/bin/env python3
"""
WZML-X Kaggle Session Manager (v5 — Blind Push Fallback)
=========================================================
Called by GitHub Actions workflow. Handles:

  1. HUMAN-LIKE SCHEDULE: 06:00–23:00 IST active, overnight rest
  2. BLIND PUSH: No status check (avoids 'Permission denied' on private notebooks)
  3. RANDOMIZED TIMING: 0–30 min delay within windows + jitter
  4. NOTIFICATIONS: Telegram + ntfy.sh on start/stop/skip/error

USAGE:
    python kaggle_manager.py start    # Push the notebook (starts a new session)
    python kaggle_manager.py stop      # Notify stop (notebook self-terminates)
    python kaggle_manager.py status    # Try status (may fail, that's ok)
    python kaggle_manager.py ensure   # Push if in active hours, skip if not

REQUIRED ENVIRONMENT VARIABLES (GitHub Secrets):
    KAGGLE_USERNAME  - Kaggle username
    KAGGLE_KEY       - Kaggle API key
    KAGGLE_NOTEBOOK  - Notebook slug (username/notebook-title)

OPTIONAL (for workflow-level notifications):
    NTFY_TOPIC       - ntfy.sh topic
    TG_BOT_TOKEN     - Bot token
    TG_CHAT_ID       - Your Telegram user ID
"""

import os
import sys
import json
import time
import random
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

def log(msg):
    ts = now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"[{ts}] {msg}", flush=True)

# ============================================================
# SCHEDULE WINDOWS (IST)
# ============================================================
START_WINDOW_BEGIN = (6, 0)
START_WINDOW_END   = (6, 30)
STOP_WINDOW_BEGIN  = (22, 30)
STOP_WINDOW_END    = (23, 0)
MAX_JITTER_MINUTES = 5

def in_time_window(begin, end):
    now = now_ist()
    b = now.replace(hour=begin[0], minute=begin[1], second=0, microsecond=0)
    e = now.replace(hour=end[0], minute=end[1], second=0, microsecond=0)
    return b <= now <= e

def in_active_hours():
    now = now_ist()
    return 6 <= now.hour < 23

def random_delay_in_window(window_end_tuple):
    now = now_ist()
    end = now.replace(hour=window_end_tuple[0], minute=window_end_tuple[1],
                      second=0, microsecond=0)
    remaining = (end - now).total_seconds()
    if remaining <= 0:
        return
    sleep_seconds = random.uniform(0, remaining * 0.8)
    log(f"Randomizing: sleeping {sleep_seconds/60:.1f} minutes...")
    time.sleep(sleep_seconds)

# ============================================================
# NOTIFIER (workflow-level)
# ============================================================
def wf_notify(event, extra=""):
    now_str = now_ist().strftime("%H:%M:%S IST")
    ntfy_topic = os.environ.get("NTFY_TOPIC", "")
    tg_token = os.environ.get("TG_BOT_TOKEN", "")
    tg_chat = os.environ.get("TG_CHAT_ID", "")

    labels = {
        "start": ("Workflow: Starting Notebook", "green_circle"),
        "stop": ("Workflow: Stopping Notebook", "red_circle"),
        "skip": ("Workflow: Skipped", "fast_forward"),
        "error": ("Workflow: Error", "warning"),
    }
    title, tag = labels.get(event, ("Workflow", "memo"))
    message = f"{title}\nTime: {now_str}\n{extra}" if extra else f"{title}\nTime: {now_str}"

    if ntfy_topic:
        try:
            ascii_title = title.encode("ascii", "replace").decode()
            ascii_tag = tag.encode("ascii", "replace").decode()
            req = urllib.request.Request(
                f"https://ntfy.sh/{ntfy_topic}",
                data=message.encode("utf-8"),
                method="POST"
            )
            req.add_header("Title", ascii_title)
            req.add_header("Tags", ascii_tag)
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            log(f"ntfy error: {e}")

    if tg_token and tg_chat:
        try:
            data = urllib.parse.urlencode({
                "chat_id": tg_chat, "text": message, "parse_mode": "HTML"
            }).encode()
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{tg_token}/sendMessage",
                data=data, method="POST"
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            log(f"Telegram error: {e}")

# ============================================================
# KAGGLE API
# ============================================================
def setup_kaggle_credentials():
    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY")
    if not username or not key:
        log("ERROR: KAGGLE_USERNAME and KAGGLE_KEY required.")
        sys.exit(1)
    kaggle_dir = os.path.expanduser("~/.kaggle")
    os.makedirs(kaggle_dir, exist_ok=True)
    with open(os.path.join(kaggle_dir, "kaggle.json"), "w") as f:
        json.dump({"username": username, "key": key}, f)
    os.chmod(os.path.join(kaggle_dir, "kaggle.json"), 0o600)
    log(f"Kaggle credentials configured for: {username}")

def get_notebook_slug():
    slug = os.environ.get("KAGGLE_NOTEBOOK")
    if not slug:
        log("ERROR: KAGGLE_NOTEBOOK required.")
        sys.exit(1)
    return slug

def kaggle_cmd(args):
    cmd = f"kaggle {args}"
    log(f">>> {cmd}")
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.stdout:
        log(result.stdout.strip())
    if result.stderr:
        log(f"stderr: {result.stderr.strip()}")
    return result

def get_kernel_status(slug):
    """Best-effort status check. Returns 'unknown' on permission error."""
    try:
        result = kaggle_cmd(f"kernels status {slug}")
        output = (result.stdout + result.stderr).lower()
        if "running" in output:
            return "running"
        elif "complete" in output:
            return "complete"
        elif "cancelled" in output or "cancel" in output:
            return "cancelled"
        elif "error" in output:
            return "error"
        elif "pending" in output:
            return "pending"
        return "unknown"
    except Exception:
        return "unknown"

def blind_push():
    """Push the notebook without checking status first. This avoids
    the 'Permission denied' error that occurs when the notebook's
    status can't be read (private notebooks, API quirks, etc)."""
    log("Blind push (skipping status check)...")
    result = kaggle_cmd("kernels push -p .")
    if result.returncode == 0:
        log("Notebook pushed successfully. New session starting.")
        wf_notify("start", "Notebook pushed. Bot coming online.")
        return True
    else:
        log("Push failed. Retrying in 15s...")
        time.sleep(15)
        result = kaggle_cmd("kernels push -p .")
        if result.returncode == 0:
            log("Notebook pushed on retry. New session starting.")
            wf_notify("start", "Notebook pushed on retry. Bot coming online.")
            return True
        else:
            log("ERROR: Failed after retry.")
            wf_notify("error", "Failed to push notebook after retry")
            return False

# ============================================================
# ACTIONS
# ============================================================
def action_start():
    slug = get_notebook_slug()

    if not in_active_hours():
        log(f"Outside active hours. Skipping. Time: {now_ist().strftime('%H:%M')} IST")
        return

    if in_time_window(START_WINDOW_BEGIN, START_WINDOW_END):
        log("Morning start window. Adding random delay...")
        random_delay_in_window(START_WINDOW_END)
    else:
        jitter = random.randint(0, MAX_JITTER_MINUTES)
        log(f"Adding {jitter} min jitter...")
        time.sleep(jitter * 60)

    log(f"Starting: {slug}")
    wf_notify("start", f"Notebook: {slug}")
    blind_push()

def action_stop():
    slug = get_notebook_slug()

    if in_time_window(STOP_WINDOW_BEGIN, STOP_WINDOW_END):
        log("Evening stop window. Adding random delay...")
        random_delay_in_window(STOP_WINDOW_END)
    else:
        jitter = random.randint(0, MAX_JITTER_MINUTES)
        log(f"Adding {jitter} min jitter...")
        time.sleep(jitter * 60)

    log(f"Stopping: {slug}")
    wf_notify("stop", f"Notebook: {slug}")
    # Notebook self-terminates at ~9.5h; this is backup notification only
    log("Notebook will self-terminate. Stop notification sent.")

def action_status():
    slug = get_notebook_slug()
    status = get_kernel_status(slug)
    log(f"Notebook: {slug}")
    log(f"Status:   {status}")
    log(f"Time:     {now_ist().strftime('%H:%M:%S IST')}")
    log(f"Active:   {'YES' if in_active_hours() else 'NO'}")
    if status == "unknown":
        log("Note: Status check may fail if notebook is private or API is restricted.")

def action_ensure():
    active = in_active_hours()
    log(f"Active: {active} | {now_ist().strftime('%H:%M')} IST")
    if active:
        log("In active hours. Pushing notebook (blind, no status check)...")
        action_start()
    else:
        log("Outside active hours. Skipping.")
        # Try a status check for logging purposes, but don't block on it
        slug = get_notebook_slug()
        status = get_kernel_status(slug)
        if status == "running":
            log("Notebook still running outside active hours. It will self-terminate.")

def main():
    if len(sys.argv) < 2:
        print("Usage: python kaggle_manager.py <start|stop|status|ensure>")
        sys.exit(1)
    action = sys.argv[1].lower()
    setup_kaggle_credentials()
    log(f"=== Kaggle Manager: {action.upper()} ===")
    if action == "start":
        action_start()
    elif action == "stop":
        action_stop()
    elif action == "status":
        action_status()
    elif action == "ensure":
        action_ensure()
    else:
        log(f"Unknown action: {action}")
        sys.exit(1)
    log("=== Done ===")

if __name__ == "__main__":
    main()
