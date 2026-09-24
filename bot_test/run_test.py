#!/usr/bin/env python3
"""Bot test bridge -- runs one scenario against the WZML-X bot using the
test account session and writes the outcome to bot_test/last-result.md.

Session source (first match wins):
  1. TG_TEST_SESSION environment variable (GitHub repo secret)
  2. TEST_SESSION in the private Kaggle config dataset

Supported formats: WZGram checksummed WZ_ strings (parsed natively by
the wzgram library; 2-char truncation auto-repaired via CRC),
Pyrogram/WZML-X-style strings and Telethon-style strings -- auto-detected.

Scenarios:
  ping    -- send /start, capture the reply (checks bot alive + allowance)
  artist  -- send /yl + Spotify artist link, wait for the preview card and
             the batch delivery, download a sample of the files and ffprobe
             them (codec / bitrate / duration / tags); pass a flag (e.g. -z)
             as --arg to test zip delivery, or a URL to test another artist
  song    -- send a single track link, same checks as artist
  cmd     -- send arbitrary text (e.g. /log, /help), capture the reply;
             text files the bot sends (like logs) get their filtered tail
             printed into the result
  peek    -- list the test account's recent dialogs to find where delivered
             files actually landed (the hyper uploader may deliver via a
             different peer than the bot PM)
  ld      -- send /ld <query>, capture the lyrics card and its buttons
"""

import argparse
import asyncio
import json
import os
import re
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

class Msg:
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
                buttons = [
                    (b.text + ((" ⟶ " + b.url) if getattr(b, "url", None) else ""))
                    for row in m.buttons for b in row
                ]
            out.append(Msg(m.id, m.text or "", buttons, fname, size, m))
        return out

    async def download(self, raw, fname=None):
        os.makedirs(DL_DIR, exist_ok=True)
        target = os.path.join(DL_DIR, (fname or "file.bin")
                              .replace("/", "_")[:180])
        return await raw.download_media(file=target)

    async def press_button(self, chat, msg, label):
        for row in msg.raw.buttons:
            for b in row:
                if label in (b.text or ""):
                    await b.click()
                    return True
        return False

    async def get_msg(self, chat, mid):
        m = await self.c.get_messages(chat, ids=mid)
        if not m:
            return None
        buttons = []
        if m.text and m.buttons:
            buttons = [b.text for row in m.buttons for b in row]
        fname, size = "", 0
        if m.document:
            for at in m.document.attributes:
                if getattr(at, "file_name", None):
                    fname = at.file_name
            size = m.document.size or 0
        return Msg(m.id, m.text or "", buttons, fname, size, m)

class PyrogramAdapter:
    """Backed by the wzgram-provided pyrogram module (drop-in fork):
    handles both standard Pyrogram strings and native WZGram WZ_ strings."""

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
                    buttons = [
                        (b.text + ((" ⟶ " + b.url) if getattr(b, "url", None) else ""))
                        for row in rm.inline_keyboard for b in row
                    ]
            except Exception:
                pass
            out.append(Msg(m.id, m.text or m.caption or "", buttons,
                           fname, size, m))
        return out

    async def download(self, raw, fname=None):
        os.makedirs(DL_DIR, exist_ok=True)
        target = os.path.join(DL_DIR, (fname or "file.bin")
                              .replace("/", "_")[:180])
        return await self.c.download_media(raw, file_name=target)

    async def press_button(self, chat, msg, label):
        rm = msg.raw.reply_markup
        if not rm:
            return False
        for row in rm.inline_keyboard:
            for b in row:
                if label in (b.text or ""):
                    await self.c.request_callback_answer(
                        chat_id=chat, message_id=msg.raw.id,
                        callback_data=b.callback_data,
                    )
                    return True
        return False

    async def get_msg(self, chat, mid):
        m = await self.c.get_messages(chat, mid)
        if not m:
            return None
        buttons = []
        try:
            rm = m.reply_markup
            if rm is not None and hasattr(rm, "inline_keyboard"):
                buttons = [b.text for row in rm.inline_keyboard for b in row]
        except Exception:
            pass
        return Msg(m.id, m.text or m.caption or "", buttons,
                   (m.document.file_name if m.document else "") or "",
                   (m.document.file_size if m.document else 0) or 0, m)

DC_IPS = {1: "149.154.175.53", 2: "149.154.167.51",
          3: "149.154.175.100", 4: "149.154.167.91", 5: "91.108.56.130"}

def _telethon_string_from(dc, key_bytes):
    """Build a standard Telethon session string from dc + auth key."""
    import ipaddress
    from telethon.crypto import AuthKey
    from telethon.sessions import StringSession
    sess = StringSession()
    sess._dc_id = int(dc)
    sess._server_address = DC_IPS.get(int(dc), DC_IPS[2])
    sess._port = 443
    sess._auth_key = AuthKey(key_bytes)
    return sess.save()

def _b64_key(key):
    import base64
    return base64.b64decode(key + "=" * (-len(key) % 4))

async def detect_session(raw, cfg):
    """Return a Telethon-style session string from any supported format."""
    if raw.startswith("WZ_"):
        try:
            from wzgram.session import parse_wz_string
            sess = parse_wz_string(raw)
            return _telethon_string_from(sess.dc_id, sess.auth_key)
        except ImportError:
            pass
        except Exception:
            pass
    if re.match(r"^[0-9]+:[A-Za-z0-9_-]+$", raw or ""):
        return raw  # already a Telethon StringSession
    m = re.match(r"^([0-9]+):([A-Za-z0-9+/=_-]+)$", raw or "")
    if m and len(m.group(2)) > 300:
        try:
            return _telethon_string_from(m.group(1), _b64_key(m.group(2)))
        except Exception:
            return None
    return None

async def make_adapter(cfg):
    raw = os.environ.get("TG_TEST_SESSION") or cfg_val("TEST_SESSION")
    if not raw:
        sys.exit("no test session (TG_TEST_SESSION / TEST_SESSION)")
    api_id = int(os.environ.get("TG_API_ID") or cfg_val("API_ID") or 0)
    api_hash = os.environ.get("TG_API_HASH") or cfg_val("API_HASH") or ""
    if raw.startswith("WZ_"):
        from wzgram import Client as WZClient
        try:
            return WZClient("bot_test", api_id=api_id, api_hash=api_hash,
                            session_string=raw, in_memory=True)
        except TypeError:
            return WZClient(api_id, api_hash, raw)
    sess = await detect_session(raw, cfg)
    if sess is None:
        sys.exit("could not parse the test session string")
    return TelethonAdapter(api_id, api_hash, sess)

async def collect(adapter, chat, sent_id, cap_s, quiet_s, first_s,
                  max_probe=10):
    """Poll the bot chat for new messages. Ends after first_s with no
    message at all, quiet_s with no new messages, or cap_s overall.
    Audio/document uploads are classified as files (their captions are
    logged as file names, not text cards). Text files (logs) sent by the
    bot are downloaded and their filtered tail printed."""
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
            if m.size:
                log(f"[file] {m.fname} {m.size / 1048576:.1f}MB")
                if len(probed) < max_probe and m.size < 70 * 1048576:
                    try:
                        p = await adapter.download(m.raw, m.fname)
                        if p:
                            _n = p.lower()
                            if (_n.endswith((".log", ".txt", ".out"))
                                    and os.path.getsize(p) < 5 * 1048576):
                                txt = open(p, encoding="utf-8",
                                           errors="ignore").read()
                                lines = txt.splitlines()
                                keep = [l for l in lines[-3000:]
                                        if re.search(
                                            r"WZFIX|ERROR|Traceback|"
                                            r"Exception|zip|Zip|artist|"
                                            r"fan-out|upload|Upload", l)]
                                log(f"[logfile {os.path.basename(p)} "
                                    f"tail]\n" + "\n".join(keep[-40:]))
                    except Exception as e:
                        log(f"[probe-error] {e}")
            elif m.buttons:
                log(f"[card] {m.text[:300]} | {m.buttons}")
            else:
                log(f"[text] {m.text[:300]}")
        if msgs:
            deadline = time.time() + quiet_s
        elif time.time() > deadline:
            break
    return probed

async def probe_audio(probed):
    for p in probed:
        if p.lower().endswith((".mp3", ".m4a", ".ogg", ".opus", ".flac")):
            try:
                r = subprocess.run(
                    ["ffprobe", "-v", "quiet", "-print_format", "json",
                     "-show_format", "-show_streams", p],
                    capture_output=True, text=True, timeout=60)
                j = json.loads(r.stdout or "{}")
                f = j.get("format", {})
                st = (j.get("streams") or [{}])[0]
                tags = f.get("tags", {})
                br = int(f.get("bit_rate") or st.get("bit_rate") or 0)
                log(f"[ffprobe {os.path.basename(p)}] "
                    f"codec={st.get('codec_name')} "
                    f"bitrate={br // 1000}kbps dur={float(f.get('duration') or 0):.0f}s "
                    f"tags={tags.get('artist')} - {tags.get('title')}")
                return
            except Exception as e:
                log(f"[ffprobe-error] {e}")

def verdict(scenario, probed):
    if scenario == "artist":
        n = len(probed)
        log(f"VERDICT: {n} file(s) probed")

async def main(scenario, arg):
    adapter = await make_adapter(CFG)
    me = await adapter.start()
    log(f"[me] {me.id} {me.first_name}")
    chat = me.username if me.username else me.id
    log(f"[chat] {chat}")

    if scenario == "ping":
        log("[send] /start")
        sid = await adapter.send(chat, "/start")
        await collect(adapter, chat, sid, cap_s=60, quiet_s=20, first_s=30)
    elif scenario == "artist":
        q = arg or "https://open.spotify.com/artist/6punPd0Zxa2TzQUBePZFgG"
        log(f"[send] /yl {q}")
        sid = await adapter.send(chat, f"/yl {q}")
        await collect(adapter, chat, sid, cap_s=600, quiet_s=60,
                      first_s=90)
        probed = await collect(adapter, chat, sid, cap_s=300, quiet_s=45,
                               first_s=45)
        await probe_audio(probed)
        verdict(scenario, probed)
    elif scenario == "song":
        if not arg:
            sys.exit("song scenario needs --arg <track url>")
        log(f"[send] /y {arg}")
        sid = await adapter.send(chat, f"/y {arg}")
        probed = await collect(adapter, chat, sid, cap_s=600, quiet_s=60,
                               first_s=90)
        await probe_audio(probed)
    elif scenario == "cmd":
        log(f"[send] {arg}")
        sid = await adapter.send(chat, arg)
        await collect(adapter, chat, sid, cap_s=120, quiet_s=20, first_s=45)
    elif scenario == "peek":
        log("[peek] recent dialogs:")
        async for d in adapter.c.iter_dialogs(limit=12):
            log(f"  {d.id} {d.name}")
    elif scenario == "ld":
        q = arg or "Locked In Bhalwaan"
        log(f"[send] /ld {q}")
        sid = await adapter.send(chat, f"/ld {q}")
        await collect(adapter, chat, sid, cap_s=300, quiet_s=60, first_s=60)
        # press the No button on the confirm card and capture what follows
        try:
            msgs = await adapter.fetch_new(chat, sid)
            card = next(
                (m for m in msgs
                 if "Did you mean" in (m.text or "") and m.buttons),
                None,
            )
            if card is None:
                log("[ld] confirm card not found")
            else:
                log(f"[ld] card buttons: {card.buttons}")
                await adapter.press_button(chat, card, "No")
                log("[ld] pressed No, waiting for the list...")
                await asyncio.sleep(10)
                fresh = await adapter.get_msg(chat, card.id)
                if fresh is None:
                    log("[ld] card could not be re-fetched")
                else:
                    txt = fresh.text or ""
                    log(f"[ld] after No text: {txt[:500]!r}")
                    log(f"[ld] after No buttons: {fresh.buttons}")
                    if "Matches for your lyrics" in txt:
                        log("[ld] RESULT: list shown after No — OK")
                    else:
                        log("[ld] RESULT: list NOT shown after No")
        except Exception as e:
            log(f"[ld-button-error] {e}")
    else:
        sys.exit(f"unknown scenario {scenario}")

    await adapter.stop()
    open(RESULT, "w", encoding="utf-8").write(
        f"# bot test: {scenario} {arg or ''}\n\n```\n"
        + "\n".join(LOG) + "\n```\n"
    )
    print("RESULT_WRITTEN")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", required=True)
    p.add_argument("--arg", default="")
    a = p.parse_args()
    asyncio.run(main(a.scenario, a.arg))
