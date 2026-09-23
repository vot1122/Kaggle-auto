"""One-time Telegram session generator for the bot test bridge.

mode=request  : ask Telegram to send the login code to the phone number
                in the TG_TEST_PHONE repo secret. Stores only the
                phone_code_hash in bot_test/state.json (useless without
                the phone number and api credentials).
mode=complete : finish the login using the code in the TG_LOGIN_CODE
                repo secret, then save the session string into the
                PRIVATE Kaggle config dataset (TEST_SESSION=...).
                The session string is never printed and never written
                to this public repo.

Requires: cfg/config.env (downloaded from the private Kaggle dataset)
containing TELEGRAM_API / TELEGRAM_HASH, plus the KAGGLE_* env vars.
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = os.path.join(HERE, "..", "cfg", "config.env")
STATE = os.path.join(HERE, "state.json")


def cfg_val(key):
    if not os.path.exists(CFG):
        sys.exit("config.env not found -- was the dataset downloaded?")
    for ln in open(CFG, encoding="utf-8", errors="ignore"):
        s = ln.strip()
        if s.startswith(key) and "=" in s:
            v = s.split("=", 1)[1].strip().strip('"').strip("'")
            if v:
                return v
    sys.exit(f"{key} missing from config.env")


def save_session_to_dataset(session_string):
    """Write TEST_SESSION into the private config dataset and push a new
    dataset version. Never prints or logs the string itself."""
    lines = open(CFG, encoding="utf-8", errors="ignore").read().splitlines()
    out, seen = [], False
    for ln in lines:
        s = ln.strip()
        if s.startswith("TEST_SESSION=") or s.startswith("TEST_SESSION ="):
            out.append(f"TEST_SESSION = {session_string}")
            seen = True
        else:
            out.append(ln)
    if not seen:
        out.append(f"TEST_SESSION = {session_string}")
    open(CFG, "w", encoding="utf-8").write("\n".join(out) + "\n")
    cfg_dir = os.path.join(HERE, "..", "cfg")
    meta = os.path.join(cfg_dir, "dataset-metadata.json")
    if not os.path.exists(meta):
        open(meta, "w").write(json.dumps(
            {
                "title": "wzmlx-config",
                "id": "djoshi7/wzmlx-config",
                "licenses": [{"name": "CC0-1.0"}],
            },
            indent=2,
        ))
    r = subprocess.run(
        ["kaggle", "datasets", "version", "-p", cfg_dir,
         "-m", "update test session", "--dir-mode", "zip"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(r.stdout[-1500:])
        print(r.stderr[-1500:], file=sys.stderr)
        sys.exit("kaggle dataset push failed")


async def main(mode):
    api_id = int(cfg_val("TELEGRAM_API"))
    api_hash = cfg_val("TELEGRAM_HASH")
    phone = os.environ.get("TG_TEST_PHONE", "").strip()
    if not phone:
        sys.exit("TG_TEST_PHONE secret not set")

    client = TelegramClient(os.path.join(HERE, "sgen"), api_id, api_hash)
    await client.connect()

    if mode == "request":
        sent = await client.send_code_request(phone)
        json.dump({"phone_code_hash": sent.phone_code_hash}, open(STATE, "w"))
        print("CODE_SENT -- enter the code you received on Telegram")
    elif mode == "complete":
        code = os.environ.get("TG_LOGIN_CODE", "").strip()
        if not code:
            sys.exit("TG_LOGIN_CODE secret not set")
        try:
            state = json.load(open(STATE))
        except Exception:
            sys.exit("no state.json -- run mode=request first")
        try:
            await client.sign_in(
                phone=phone,
                code=code,
                phone_code_hash=state["phone_code_hash"],
                password=os.environ.get("TG_TEST_2FA", "").strip() or None,
            )
        except SessionPasswordNeededError:
            print("NEEDS_2FA -- remove 2FA or provide TG_TEST_2FA secret")
            sys.exit(2)
        me = await client.get_me()
        print(f"LOGGED_IN_AS {me.id} {me.first_name}")
        save_session_to_dataset(client.session.save())
        json.dump({"done": True}, open(STATE, "w"))
        print("SESSION_SAVED -- stored in the private config dataset only")
    await client.disconnect()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", required=True, choices=["request", "complete"])
    a = p.parse_args()
    asyncio.run(main(a.mode))
