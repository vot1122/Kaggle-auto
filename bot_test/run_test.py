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

    async def download(self, raw, fname=None):
        os.makedirs(DL_DIR, exist_ok=True)
        target = os.path.join(DL_DIR, (fname or "file.bin")
                              .replace("/", "_")[:180])
        return await raw.download_media(file=target)


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
                    buttons = [b.text for row in rm.inline_keyboard
                               for b in row]
            except Exception:
                pass
            out.append(Msg(m.id, m.text or m.caption or "", buttons,
                           fname, size, m))
        out.reverse()  # newest-first -> chronological
        return out

    async def download(self, raw, fname=None):
        os.makedirs(DL_DIR, exist_ok=True)
        target = os.path.join(DL_DIR, (fname or "file.bin")
                              .replace("/", "_")[:180])
        return await self.c.download_media(raw, file_name=target)


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
    try:
        kb = base64.urlsafe_b64decode(key + "=" * (-len(key) % 4))
        if len(kb) == 256:
            return kb
    except Exception:
        pass
    try:
        kb = bytes.fromhex(key)
        if len(kb) == 256:
            return kb
    except Exception:
        pass
    return None


def normalize_session(s):
    """Handle prefixed/custom session formats (e.g. older WZ_ variants).
    Returns a standard Telethon or Pyrogram session string. Diagnostics
    never include auth key material."""
    s = s.strip()
    if not s.startswith("WZ_"):
        return s
    inner = s[3:].strip()
    diag = [f"payload len={len(inner)}"]
    if inner[:1] in ("1", "B"):
        log("[session] WZ_ + standard string")
        return inner
    import base64
    import binascii
    import json as _json

    def _try_decode(x):
        for cand in (x, x.replace("+", "-").replace("/", "_")):
            for pad in range(4):
                try:
                    r = base64.urlsafe_b64decode(cand + "=" * pad)
                    if len(r) >= 100:
                        return r
                except (binascii.Error, ValueError):
                    continue
        return None

    raw = None
    for cand, desc in ((inner, "as-is"), (inner[1:], "strip-first"),
                        (inner[:-1], "strip-last")):
        raw = _try_decode(cand)
        if raw:
            diag.append(f"decoded {len(raw)}B from {desc} "
                        f"head={raw[:4].hex()}")
            break
    if not raw:
        diag.append(f"not-base64 starts={inner[:6]!r}")
        sys.exit("WZ_ session not parseable (1): " + "; ".join(diag))
    if raw[:1] in (b"{", b"["):
        try:
            d = _json.loads(raw)
            if isinstance(d, list):
                d = d[0] if d and isinstance(d[0], dict) else {}
            diag.append(f"json keys={sorted(d)[:12]}")

            def _find(obj, names):
                if isinstance(obj, dict):
                    for n in names:
                        if n in obj and obj[n] not in (None, "", 0, False):
                            return obj[n]
                    for v in obj.values():
                        r = _find(v, names)
                        if r is not None:
                            return r
                return None
            dc = _find(d, ("dc", "dc_id", "DC", "datacenter", "server_dc"))
            key = _find(d, ("auth_key", "authKey", "key", "authorization"))
            if dc and isinstance(key, str) and len(key) >= 64:
                kb = _b64_key(key)
                if kb:
                    st = _telethon_string_from(int(dc), kb)
                    log(f"[session] WZ_ JSON converted (dc={dc})")
                    return st
                diag.append("auth key present but not 256B")
        except Exception as e:
            diag.append(f"json-error {e}")
    # pyrogram-style raw binary: dc(1) api_id(4) test(1) key(256)...
    if len(raw) >= 262 and raw[0] in (1, 2, 3, 4, 5):
        st = _telethon_string_from(raw[0], raw[6:262])
        log(f"[session] WZ_ binary (pyrogram layout) converted "
            f"(dc={raw[0]})")
        return st
    if len(raw) >= 263 and raw[0] in (1, 2, 3, 4, 5):
        # telethon layout: dc(1) ip(4) port(2) key(256)
        st = _telethon_string_from(raw[0], raw[7:263])
        log(f"[session] WZ_ binary (telethon layout) converted "
            f"(dc={raw[0]})")
        return st
    try:
        txt = raw.decode("utf-8")
        if txt.isprintable():
            diag.append(f"printable text starts={txt[:32]!r}")
    except Exception:
        pass
    sys.exit("WZ_ session not parseable (2): " + "; ".join(diag)
             + " -- the current WZGram format must be parsed by the "
               "wzgram library itself")


def repair_wz_session(s):
    """Recover a WZGram WZ_ string that lost 2 characters somewhere
    (truncated copy-paste). Uses the format's CRC32 checksum to find the
    unique repair. Returns the repaired string, or the original if no
    repair passes."""
    import base64
    import binascii
    import struct
    import zlib

    ALPH = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz" "0123456789-_")
    V3_CRC_PACKED_SIZE = 326
    TARGET = 435  # 326 bytes -> 435 chars (3-char tail group)
    body = s[3:]
    if len(body) == TARGET:
        return s
    if len(body) != TARGET - 2:
        return s  # some other damage; let the library report it

    def _dec(cand):
        try:
            raw = base64.urlsafe_b64decode(
                cand + "=" * (-len(cand) % 4))
        except (binascii.Error, ValueError):
            return None
        return raw

    def _crc_ok(raw):
        if len(raw) != V3_CRC_PACKED_SIZE:
            return False
        payload = raw[:-4]
        stored = struct.unpack("<I", raw[-4:])[0]
        return zlib.crc32(payload) == stored

    order = [len(body)] + list(range(len(body)))  # end first, then start
    for p in order:
        prefix, suffix = body[:p], body[p:]
        for a in ALPH:
            for b in ALPH:
                cand = prefix + a + b + suffix
                raw = _dec(cand)
                if raw and _crc_ok(raw):
                    log(f"[session] REPAIRED: 2 missing chars at pos {p} "
                        f"(restored '{a}{b}')")
                    return "WZ_" + cand
    log("[session] repair failed: no candidate passed the checksum")
    return s


def make_adapter(api_id, api_hash, session):
    s = session.strip()
    if s.startswith("WZ_"):
        s = repair_wz_session(s)
        # native WZGram (Pyrogram fork) checksummed format -- only the
        # wzgram library itself can parse it; PyrogramAdapter is backed
        # by the wzgram-provided pyrogram module.
        log("[client] wzgram (native WZ_)")
        return PyrogramAdapter(api_id, api_hash, s)
    s = normalize_session(s).strip()
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
                                            r"fan-out|upload|Upload",
                                            l)]
                                log(f"[logfile {os.path.basename(p)} "
                                    f"{len(lines)} lines, filtered "
                                    f"{len(keep)}]")
                                for l in keep[-120:]:
                                    log("  " + l[:250])
                            else:
                                info = ffprobe(p)
                                info["file"] = os.path.basename(p)
                                probed.append(info)
                            os.remove(p)
                    except Exception as e:
                        log(f"[probe-error] {e}")
            elif m.text:
                log(f"[text] {m.text[:400]}")
                if m.buttons:
                    log(f"[buttons] {m.buttons}")
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
        url = ("https://open.spotify.com/artist/"
               "1x02ug1CLkx7mrQP9FRswh")
        flags = ""
        if arg and arg.startswith("-"):
            flags = arg  # e.g. "-z" -> zip delivery
        elif arg:
            url = arg
        # bare links are ignored by design (commands only) -- use /yl
        log(f"[send] /yl {url} {flags}".rstrip())
        sid = await adapter.send(chat, f"/yl {url} {flags}".rstrip())
        probed = await collect(adapter, chat, sid, cap_s=1800,
                               quiet_s=420, first_s=180)
        LOG.extend(verdict(scenario, probed))
    elif scenario == "song":
        url = arg
        if not url:
            sys.exit("song scenario needs --arg <track url>")
        log(f"[send] /yl {url}")
        sid = await adapter.send(chat, f"/yl {url}")
        probed = await collect(adapter, chat, sid, cap_s=1200,
                               quiet_s=300, first_s=120)
        LOG.extend(verdict("artist", probed))
    elif scenario == "cmd":
        text = arg or "/help"
        log(f"[send] {text}")
        sid = await adapter.send(chat, text)
        await collect(adapter, chat, sid, cap_s=240, quiet_s=30,
                      first_s=60)
    elif scenario == "peek":
        # list the test account's recent dialogs to find where files
        # actually landed (the hyper uploader may deliver via a
        # different peer than the bot PM)
        me_id = me.id
        seen = 0
        async for d in adapter.c.get_dialogs(limit=20):
            c = d.chat
            title = getattr(c, "first_name", None) or getattr(c, "title", "") or ""
            uname = getattr(c, "username", "") or ""
            last = d.top_message if d.top_message is not None else None
            if last is None or getattr(c, "id", 0) == me_id:
                continue
            last_from = getattr(getattr(last, "from_user", None), "username", "") or ""
            fname, size = "", 0
            doc = getattr(last, "document", None)
            aud = getattr(last, "audio", None)
            if doc:
                fname = doc.file_name or ""
                size = doc.file_size or 0
            elif aud:
                fname = aud.file_name or ""
                size = aud.file_size or 0
            if fname or seen < 8:
                seen += 1
                log(f"[dialog] id={c.id} {title} @{uname} "
                    f"last=<{last.id}> by @{last_from} "
                    f"file={fname!r} {size / 1048576:.1f}MB "
                    f"text={(last.text or last.caption or '')[:60]!r}")
        await asyncio.sleep(2)
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
