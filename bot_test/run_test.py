"""Bot test bridge -- runs one scenario against the WZML-X bot using the
test account session and writes the outcome to bot_test/last-result.md.

Session source (first match wins):
  1. TG_TEST_SESSION environment variable (GitHub repo secret)
  2. TEST_SESSION in the private Kaggle config dataset

Both Pyrogram/WZML-X-style session strings and Telethon-style session
strings are supported -- the format is auto-detected.

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
        return ""
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


class Msg:
    """Normalized message across Telethon / Pyrogram."""

    def __init__(self, id, text, buttons, fname, size, raw):
        self.id, self.text, self.buttons = id, text, buttons
        self.fname, self.size, self.raw = fname, size, raw


class TelethonAdapter:
    def __init__(self, api_id, api_hash, session):
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        self.c = TelegramClient(StringSession(session), api_id, api_hash)

    async def start(self):
        await self.c.start()
        return await self.c.get_me()

    async def stop(self):
        await self.c.disconnect()

    async def send(self, chat, text):
        m = await self.c.send_message(chat, text)
        return m.id

    async def fetch_new(self, chat, min_id):
        out = []
        got = await self.c.get_messages(chat, limit=20, min_id=min_id)
        for m in sorted(got, key=lambda x: x.id):
            if m.id <= min_id:
                continue
            fname, size, buttons = "", 0, []
            if m.document:
                for at in m.document.attributes:
                    if getattr(at, "file_name", None):
                        fname = at.file_name
                size = m.document.size or 0
            if m.text and m.buttons:
                buttons = [b.text for row in m.buttons for b in row]
            out.append(Msg(m.id, m.text or "", buttons, fname, size, m))
        return out

    async def download(self, raw):
        os.makedirs(DL_DIR, exist_ok=True)
        return await raw.download_media(file=DL_DIR)


class PyrogramAdapter:
    def __init__(self, api_id, api_hash, session):
        from pyrogram import Client
        self.c = Client(
            "bot_test", api_id=api_id, api_hash=api_hash,
            session_string=session, in_memory=True,
        )

    async def start(self):
        await self.c.start()
        return await self.c.get_me()

    async def stop(self):
        await self.c.stop()

    async def send(self, chat, text):
        m = await self.c.send_message(chat, text)
        return m.id

    async def fetch_new(self, chat, min_id):
        out = []
        async for m in self.c.get_chat_history(chat, limit=20):
            if m.id <= min_id:
                break
            fname, size, buttons = "", 0, []
            if m.document:
                fname = m.document.file_name or ""
                size = m.document.file_size or 0
            elif m.audio:
                fname = (m.audio.file_name or "") + ""
                size = m.audio.file_size or 0
            try:
                rm = m.reply_markup
                if rm is not None and hasattr(rm, "inline_keyboard"):
                    buttons = [b.text for row in rm.inline_keyboard
                               for b in row]
            except Exception:
                pass
            out.append(Msg(m.id, m.text or m.caption or "", buttons,
                           fname, size, m))
        out.reverse()  # newest-first -> chronological
        return out

    async def download(self, raw):
        os.makedirs(DL_DIR, exist_ok=True)
        return await self.c.download_media(raw, file_name=DL_DIR)


def make_adapter(api_id, api_hash, session):
    s = session.strip()
    if s.startswith("1"):
        log("[client] telethon")
        return TelethonAdapter(api_id, api_hash, s)
    if s.startswith("B"):
        log("[client] pyrogram")
        return PyrogramAdapter(api_id, api_hash, s)
    sys.exit(f"unknown session format: starts {s[:3]!r} len={len(s)}")


async def collect(adapter, chat, sent_id, cap_s, quiet_s, first_s,
                  max_probe=10):
    """Poll the bot chat for new messages. Ends after first_s with no
    message at all, quiet_s with no new messages, or cap_s overall."""
    last_id = sent_id
    hard_end = time.time() + cap_s
    deadline = time.time() + first_s
    probed = []
    while time.time() < hard_end:
        await asyncio.sleep(8)
        try:
            msgs = await adapter.fetch_new(chat, last_id)
        except Exception as e:
            log(f"[poll-error] {e}")
            continue
        for m in msgs:
            if m.id <= last_id:
                continue
            last_id = m.id
            if m.text:
                log(f"[text] {m.text[:400]}")
                if m.buttons:
                    log(f"[buttons] {m.buttons}")
            elif m.size:
                log(f"[file] {m.fname} {m.size / 1048576:.1f}MB")
                if len(probed) < max_probe and m.size < 70 * 1048576:
                    try:
                        p = await adapter.download(m.raw)
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
    session = os.environ.get("TG_TEST_SESSION", "").strip() \
        or cfg_val("TEST_SESSION").strip()
    if not (api_id and api_hash and session):
        sys.exit("TELEGRAM_API / TELEGRAM_HASH / TG_TEST_SESSION missing")
    adapter = make_adapter(int(api_id), api_hash, session)
    me = await adapter.start()
    log(f"[me] {me.id} {getattr(me, 'first_name', '')}")
    chat = BOT

    if scenario == "ping":
        sid = await adapter.send(chat, "/start")
        await collect(adapter, chat, sid, cap_s=120, quiet_s=25, first_s=60)
    elif scenario == "artist":
        url = arg or ("https://open.spotify.com/artist/"
                      "1x02ug1CLkx7mrQP9FRswh")
        log(f"[send] {url}")
        sid = await adapter.send(chat, url)
        probed = await collect(adapter, chat, sid, cap_s=1800,
                               quiet_s=240, first_s=180)
        LOG.extend(verdict(scenario, probed))
    elif scenario == "ld":
        q = arg or "Locked In Bhalwaan"
        log(f"[send] /ld {q}")
        sid = await adapter.send(chat, f"/ld {q}")
        await collect(adapter, chat, sid, cap_s=300, quiet_s=60, first_s=60)
    else:
        sys.exit(f"unknown scenario {scenario}")

    await adapter.stop()
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
