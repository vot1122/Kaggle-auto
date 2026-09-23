"""Bot test bridge -- runs one scenario against the WZML-X bot using the
test account session (TEST_SESSION in the private Kaggle config dataset)
and writes the outcome to bot_test/last-result.md.

Scenarios:
  ping    -- send /start, capture the reply (checks bot alive + allowance)
  artist  -- send a Spotify artist link, wait for the preview card and the
             batch delivery, download a sample of the files and ffprobe
             them (codec / bitrate / duration / tags)
  ld      -- send /ld <query>, capture the lyrics card and its buttons
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time

from telethon import TelegramClient
from telethon.sessions import StringSession

BOT = "ytdownloadwebbot"
HERE = os.path.dirname(os.path.abspath(__file__))
CFG = os.path.join(HERE, "..", "cfg", "config.env")
RESULT = os.path.join(HERE, "last-result.md")
DL_DIR = os.path.join(HERE, "dl")

LOG = []


def log(m):
    LOG.append(str(m))
    print(m)


def cfg_val(key):
    if not os.path.exists(CFG):
        sys.exit("config.env not found -- was the dataset downloaded?")
    for ln in open(CFG, encoding="utf-8", errors="ignore"):
        s = ln.strip()
        if s.startswith(key) and "=" in s:
            v = s.split("=", 1)[1].strip().strip('"').strip("'")
            if v:
                return v
    return ""


def ffprobe(path):
    r = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_format", path],
        capture_output=True, text=True,
    )
    try:
        f = json.loads(r.stdout).get("format", {})
    except Exception:
        return {}
    tags = f.get("tags", {}) or {}
    return {
        "codec": f.get("format_name", ""),
        "bitrate": f.get("bit_rate", ""),
        "duration": f.get("duration", ""),
        "title": tags.get("title", tags.get("TITLE", "")),
        "artist": tags.get("artist", tags.get("ARTIST", "")),
        "file": os.path.basename(path),
    }


async def collect(client, bot, sent, cap_s, quiet_s, first_s, max_probe=10):
    """Poll the bot chat for new messages. Ends after first_s with no
    message at all, quiet_s with no new messages, or cap_s overall."""
    last_id = sent.id
    hard_end = time.time() + cap_s
    deadline = time.time() + first_s
    probed = []
    while time.time() < hard_end:
        await asyncio.sleep(8)
        try:
            got = await client.get_messages(bot, limit=20, min_id=last_id)
        except Exception as e:
            log(f"[poll-error] {e}")
            continue
        for m in sorted(got, key=lambda x: x.id):
            if m.id <= last_id:
                continue
            last_id = m.id
            if m.text:
                log(f"[text] {m.text[:400]}")
                if m.buttons:
                    labels = [b.text for row in m.buttons for b in row]
                    log(f"[buttons] {labels}")
            elif m.document or m.audio:
                fname = ""
                try:
                    for at in m.document.attributes:
                        if getattr(at, "file_name", None):
                            fname = at.file_name
                except Exception:
                    pass
                size = (m.file.size if m.file else 0) or 0
                log(f"[file] {fname} {size / 1048576:.1f}MB")
                if len(probed) < max_probe and 0 < size < 70 * 1048576:
                    try:
                        os.makedirs(DL_DIR, exist_ok=True)
                        p = await m.download_media(file=DL_DIR)
                        if p:
                            info = ffprobe(p)
                            info["file"] = os.path.basename(p)
                            probed.append(info)
                            os.remove(p)
                    except Exception as e:
                        log(f"[probe-error] {e}")
            deadline = time.time() + quiet_s
        if time.time() > deadline:
            break
    return probed


def verdict(scenario, probed):
    lines = []
    files = [l for l in LOG if l.startswith("[file]")]
    if scenario == "artist":
        lines.append(f"files received: {len(files)}")
        if not files:
            lines.append("VERDICT: FAIL -- no files delivered")
        else:
            lines.append(f"files ffprobed: {len(probed)}")
            for p in probed:
                br = int(p.get("bitrate") or 0)
                lines.append(
                    f"  {p['file']}: {p['codec']} {br // 1000}kbps "
                    f"{float(p['duration'] or 0):.0f}s "
                    f"tags={p['title']!r}/{p['artist']!r}")
            if probed:
                allmp3 = all("mp3" in (p["codec"] or "") for p in probed)
                all320 = all(int(p.get("bitrate") or 0) >= 250000
                             for p in probed)
                named = all(" - " in (p["file"] or "") for p in probed)
                lines.append(f"all mp3: {allmp3}, all ~320kbps: {all320}, "
                             f"clean names: {named}")
                lines.append(
                    "VERDICT: " + ("PASS" if (allmp3 and all320 and named)
                                  else "CHECK"))
    else:
        lines.append("VERDICT: " + ("PASS" if LOG and any(
            l.startswith("[text]") for l in LOG) else "FAIL"))
    return lines


async def main(scenario, arg):
    api_id = cfg_val("TELEGRAM_API")
    api_hash = cfg_val("TELEGRAM_HASH")
    session = cfg_val("TEST_SESSION")
    if not (api_id and api_hash and session):
        sys.exit("TELEGRAM_API / TELEGRAM_HASH / TEST_SESSION missing "
                 "from config.env")
    client = TelegramClient(
        StringSession(session), int(api_id), api_hash)
    await client.start()
    me = await client.get_me()
    log(f"[me] {me.id} {me.first_name}")
    bot = await client.get_entity(BOT)

    if scenario == "ping":
        sent = await client.send_message(bot, "/start")
        await collect(client, bot, sent, cap_s=120, quiet_s=25, first_s=60)
    elif scenario == "artist":
        url = arg or ("https://open.spotify.com/artist/"
                      "1x02ug1CLkx7mrQP9FRswh")
        log(f"[send] {url}")
        sent = await client.send_message(bot, url)
        probed = await collect(client, bot, sent, cap_s=1800,
                               quiet_s=240, first_s=180)
        LOG.extend(verdict(scenario, probed))
    elif scenario == "ld":
        q = arg or "Locked In Bhalwaan"
        log(f"[send] /ld {q}")
        sent = await client.send_message(bot, f"/ld {q}")
        await collect(client, bot, sent, cap_s=300, quiet_s=60, first_s=60)
    else:
        sys.exit(f"unknown scenario {scenario}")

    await client.disconnect()
    open(RESULT, "w", encoding="utf-8").write(
        f"# bot test: {scenario} {arg or ''}\n\n```\n"
        + "\n".join(LOG) + "\n```\n")
    print("RESULT_WRITTEN")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", required=True)
    p.add_argument("--arg", default="")
    a = p.parse_args()
    asyncio.run(main(a.scenario, a.arg))
