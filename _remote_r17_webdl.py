# WZFIX Round 17 (v15.84) — web downloader: yt-dlp links -> user's device.
#
# Served by the bot's own aiohttp stream server (the one that already
# runs on 127.0.0.1:8091), same as /wzadmin. Reachable from outside
# through the existing chain:
#     https://<worker>/webdl -> cloudflared -> wserver:8080 -> here
#
# Frontend: the same SPA is served at GET /webdl (so it works straight
# from the worker URL) AND deployed standalone on GitHub Pages
# (ytwebdownload.github.io), which calls back over CORS. No Worker
# changes needed - CORS headers are emitted here, at the origin.
#
# Auth: a separate WEBDL_PASS (NOT the stream password, NOT the admin
# password) stored in wzfix_config; on first boot one is generated and
# sent to LOG_CHAT. Sessions are HMAC tokens carried in the
# Authorization: Bearer header (CORS-safe, no cookies) or ?auth= for
# direct link downloads. Login is rate-limited per IP (5 fails -> 10
# minute lock, in memory - the stream server process is the only user).
#
# Engine: plain yt-dlp subprocesses (the same interpreter yt-dlp the
# bot upgrades on every boot). No Telegram listener, no upload - the
# finished file is served straight to the browser with a Content-
# Disposition attachment header and Range support, so phones can
# resume broken downloads.
#
# Limits: files live in <DOWNLOAD_DIR>/webdl/<task-id>/ and are
# auto-deleted WEBDL_TTL hours after they finished (default 6). A disk
# budget (WEBDL_MAX_GB, default 8) and a concurrency cap
# (WEBDL_CONC, default 2) keep the Kaggle session healthy.
#
# Everything fails safe: any downloader error must never affect the
# bot, the stream server or the dashboard.

import asyncio
import json
import os
import re
import sys
import time
from hashlib import pbkdf2_hmac, sha256

R17_VERSION = "v19.4.0"  # block list ("do not allow") + guard parity,
# file passwords, upload progress, themes
_HISTORY = []  # last 200 finished task summaries (owner+own history)

_EVENTS = []  # last 200 webdl events (surfaced via /api/state)


def _evt(msg):
    """Append to the webdl activity log (also mirrors to the bot log)."""
    _EVENTS.append({"t": int(time.time()), "m": str(msg)[:200]})
    del _EVENTS[:-200]
    try:
        from ...core.stream_server import LOGGER
        LOGGER.info(f"webdl: {msg}")
    except Exception:
        pass
from hmac import compare_digest, new as hmac_new
from secrets import token_hex, token_urlsafe
from shutil import rmtree
from urllib.parse import quote

from aiohttp import web

from .r1_core import _db, _part, ensure_ready

# ----------------------------------------------------------------------------
# settings (wzfix_config doc "webdl" + config.env overrides via Config)
# ----------------------------------------------------------------------------

_SETTINGS_CACHE = (0.0, None)
_SESSION_H = 72  # 3 days - fewer logins on a phone
_BAD_UA = ("python-requests", "curl/", "wget", "go-http-client", "okhttp")
_KNOWN_UA = ("mozilla", "applewebkit", "chrome", "safari", "firefox", "edge",
             "opera", "samsungbrowser", "ucbrowser", "miuibrowser")


def _env(key, default=""):
    try:
        from ...core.config_manager import Config

        return str(getattr(Config, key, "") or "").strip()
    except Exception:
        from os import getenv

        return getenv(key, "").strip()


async def _settings(force=False):
    """Cached (60 s) webdl settings dict."""
    global _SETTINGS_CACHE
    now = time.time()
    if not force and _SETTINGS_CACHE[1] and now - _SETTINGS_CACHE[0] < 60:
        return _SETTINGS_CACHE[1]
    s = {"ttl": 6, "max_gb": 8, "conc": 2,
         "user_max_gb": 2.0, "user_daily": 3, "origins": [
        "https://vot1122.github.io",
        "https://ytwebdownload.github.io",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
         # v17.0.0 accounts + site meta
         "site_name": "WZ Web Downloader", "owner_contact": "",
         "guest_mode": False,
         "v_max_gb": 4.0, "v_daily": 10, "s_max_gb": 10.0, "s_daily": 30,
         "bw_global_mb": 0, "bw_user_mb": 0,
         # v18.0.0: pixeldrain big-file route + site options
         "pd_low_gb": 2.0, "pd_high_gb": 4.0,
         "maintenance": False, "announcement": "", "theme": "#7c5cff",
         # v19.0.0: role slot pools + site look
         "member_slots": 2, "guest_slots": 2, "default_theme": "midnight",
         "music": False, "notify_done": True,
         # v19.2.1: quick accounts have their own switch (they are real,
         # traceable accounts - guests can stay off)
         "quick_mode": True}
    try:
        col = _db().wzfix_config[_part()]
        doc = await col.find_one({"_id": "webdl"}) or {}
        for k in ("ttl", "max_gb", "conc", "user_max_gb", "user_daily",
                  "v_max_gb", "v_daily", "s_max_gb", "s_daily",
                  "bw_global_mb", "bw_user_mb",
                  "member_slots", "guest_slots"):
            if doc.get(k) is not None:
                try:
                    s[k] = float(doc[k])
                except (TypeError, ValueError):
                    pass
        if doc.get("site_name"):
            s["site_name"] = str(doc["site_name"])[:60]
        if doc.get("owner_contact") is not None:
            s["owner_contact"] = str(doc["owner_contact"]).strip()[:60]
        if doc.get("guest_mode") is not None:
            s["guest_mode"] = bool(doc["guest_mode"])
        if doc.get("v_daily") is not None:
            try:
                s["v_daily"] = int(float(doc["v_daily"]))
            except (TypeError, ValueError):
                pass
        if doc.get("s_daily") is not None:
            try:
                s["s_daily"] = int(float(doc["s_daily"]))
            except (TypeError, ValueError):
                pass
        if doc.get("origins"):
            v = doc["origins"]
            if isinstance(v, list) and v:
                s["origins"] = [str(x).strip() for x in v if str(x).strip()]
        # v16.3.0: owner-managed strings (YT cookies, TG group ids)
        for k in ("yt_cookies", "tg_files_chat", "tg_logs_chat"):
            if doc.get(k):
                s[k] = str(doc[k])
        # v18.0.0: pixeldrain big-file route + site options
        if doc.get("pd_key"):
            s["pd_key"] = str(doc["pd_key"])
        for k in ("pd_low_gb", "pd_high_gb"):
            if doc.get(k) is not None:
                try:
                    s[k] = float(doc[k])
                except (TypeError, ValueError):
                    pass
        if doc.get("maintenance") is not None:
            s["maintenance"] = bool(doc["maintenance"])
        if doc.get("announcement") is not None:
            s["announcement"] = str(doc["announcement"])[:300]
        if doc.get("theme"):
            _th = str(doc["theme"]).strip()
            if re.fullmatch(r"#[0-9a-fA-F]{6}", _th):
                s["theme"] = _th
        if doc.get("default_theme"):
            _dt = str(doc["default_theme"]).strip().lower()
            if _dt in _THEMES:
                s["default_theme"] = _dt
        if doc.get("music") is not None:
            s["music"] = bool(doc["music"])
        if doc.get("notify_done") is not None:
            s["notify_done"] = bool(doc["notify_done"])
        if doc.get("quick_mode") is not None:
            s["quick_mode"] = bool(doc["quick_mode"])
    except Exception:
        pass
    for k, cast in (("WEBDL_TTL", float), ("WEBDL_MAX_GB", float),
                    ("WEBDL_CONC", float)):
        v = _env(k, "")
        if v:
            try:
                s[k.replace("WEBDL_", "").lower()] = cast(v)
            except (TypeError, ValueError):
                pass
    s["conc"] = max(1, int(s["conc"]))
    _SETTINGS_CACHE = (now, s)
    return s


async def get_webdl_pass():
    """Current WEBDL_PASS (config.env // /bs wins, then DB, then generated)."""
    _bs = _env("WEBDL_PASS")
    if _bs:
        return _bs
    try:
        col = _db().wzfix_config[_part()]
        doc = await col.find_one({"_id": "webdl"})
        if doc and doc.get("pass"):
            return str(doc["pass"])
        pw = "joshi"
        await col.update_one({"_id": "webdl"}, {"$set": {"pass": pw}},
                             upsert=True)
        base = _env("BASE_URL")
        await _notify(
            "🌐 <b>WZFIX web downloader</b>\n"
            "┏ Default password active: <code>joshi</code>\n"
            + (f"┗ Site: {base}/webdl" if base else "┗ Change it via WEBDL_PASS "
               "in /bs or config.env.")
        )
        return pw
    except Exception:
        return ""


async def set_webdl_pass(new_pass):
    col = _db().wzfix_config[_part()]
    await col.update_one({"_id": "webdl"},
                         {"$set": {"pass": str(new_pass).strip()}},
                         upsert=True)


async def _webdl_secret():
    """Random per-bot signing secret (created once, stored in wzfix_config)."""
    try:
        col = _db().wzfix_config[_part()]
        doc = await col.find_one({"_id": "webdl_secret"})
        if doc and doc.get("secret"):
            return str(doc["secret"])
        secret = token_hex(32)
        await col.update_one({"_id": "webdl_secret"},
                             {"$set": {"secret": secret}}, upsert=True)
        return secret
    except Exception:
        return "wzfix-no-secret"


async def _session_token(role="o", dev=""):
    """v19.1.0: "~"-separated token. The old dot-separated format broke
    whenever a device id contained a dot (e.g. the username d.j)."""
    exp = int(time.time()) + _SESSION_H * 3600
    dev = re.sub(r"[^A-Za-z0-9_.:-]", "", str(dev or ""))[:64]
    secret = await _webdl_secret()
    sig = hmac_new(secret.encode(),
                   f"webdl:{role}:{dev}:{exp}".encode(),
                   sha256).hexdigest()
    return f"{role}~{exp}~{sig}~{dev}"


_BAN_CACHE = {}  # dev -> (ts, allowed); revocation lands within 10 s


async def _auth_info(request):
    """Validate the session token; returns {"role", "dev"} or None.

    v17.2.0: member/special tokens are re-checked against the DB on every
    request (10 s cache) - banning or removing an account kicks the user
    off the site within seconds, no refresh needed. Guests get the same
    ban check. Only the owner token skips it.
    """
    try:
        tok = (request.headers.get("Authorization", "") or "").strip()
        if tok[:7].lower() == "bearer ":
            tok = tok[7:].strip()
        if not tok:
            tok = request.query.get("auth", "")
        if "~" in tok:
            role, exp, sig, dev = tok.split("~", 3)
        else:  # legacy dot tokens (sessions issued before v19.1.0)
            role, dev, exp, sig = tok.split(".", 3)
        if role not in ("o", "g", "v", "s", "q") or (
                role in ("g", "v", "s", "q") and not dev):
            return None
        secret = await _webdl_secret()
        good = hmac_new(secret.encode(),
                        f"webdl:{role}:{dev}:{exp}".encode(),
                        sha256).hexdigest()
        if not tok or not compare_digest(sig, good):
            return None
        if int(exp) <= time.time():
            return None
        if role in ("v", "s", "q", "g"):
            if not await _still_allowed(role, dev[:64]):
                return None
        return {"role": role, "dev": dev[:64]}
    except Exception:
        return None


async def _still_allowed(role, dev):
    """DB-backed ban/exists check with a 10 s cache (revocation speed)."""
    now = time.time()
    hit = _BAN_CACHE.get(dev)
    if hit and now - hit[0] < 10:
        return hit[1]
    try:
        doc = await _users_col().find_one({"_id": dev}) or {}
        if role in ("v", "s", "q"):
            # removed account or banned -> out immediately
            ok = bool(doc) and not doc.get("banned")
        else:
            # guest: only an explicit ban blocks (missing doc = fine)
            ok = not doc.get("banned")
    except Exception:
        ok = True  # DB hiccup must not log everyone out
    _BAN_CACHE[dev] = (now, ok)
    return ok


def _maint(s, role):
    """v19.0.0: maintenance mode locks out everyone but the owner."""
    return bool(s.get("maintenance")) and role != "o"


async def _chat_id(pref_key):
    """Resolve a target chat: webdl setting first, then bot config."""
    try:
        s = await _settings()
        c = str(s.get(pref_key, "") or "").strip()
        if c:
            return int(c) if c.lstrip("-").isdigit() else c
    except Exception:
        pass
    try:
        from ...core.config_manager import Config
        chat = str(getattr(Config, "LOG_CHAT", "") or "").strip()
        if not chat:
            chat = str(getattr(Config, "ADMIN_LOG_CHAT", "") or "").strip()
        if chat and chat.lstrip("-").isdigit():
            return int(chat)
        return chat or None
    except Exception:
        return None


_TG_LAST = {"logs": "", "files": "", "err": "", "at": 0}


def _norm_chat(c):
    """Chat id from config/settings -> int when numeric (pyrogram wants int)."""
    if c is None:
        return None
    if isinstance(c, int):
        return c
    c = str(c).strip()
    if not c:
        return None
    if c.lstrip("-").isdigit():
        return int(c)
    return c


async def _notify(text):
    """Bot API message to the LOGS group - failures are RECORDED, not lost."""
    try:
        from ...core.tg_client import TgClient

        chat = _norm_chat(await _chat_id("tg_logs_chat"))
        _TG_LAST["logs"] = str(chat or "")
        if not chat:
            _TG_LAST["err"] = ("no logs group set - send the group id via "
                               "/ws (Telegram logs group)")
            _TG_LAST["at"] = int(time.time())
            return
        await TgClient.bot.send_message(chat_id=chat, text=text[:3900],
                                        disable_web_page_preview=True)
        _TG_LAST["err"] = ""
    except Exception as e:
        _TG_LAST["err"] = f"{e.__class__.__name__}: {e}"[:200]
        _TG_LAST["at"] = int(time.time())
        _evt(f"tg-notify FAIL: {_TG_LAST['err']}")


_TG_MAX = 1900 * (1 << 20)  # pyrogram bot uploads: just under 2 GB


async def _send_tg(t):
    """Send a finished webdl file to the FILES group (bypasses the tunnel)."""
    try:
        from ...core.tg_client import TgClient

        chat = _norm_chat(await _chat_id("tg_files_chat"))
        _TG_LAST["files"] = str(chat or "")
        if not chat:
            t["tg"] = "error"
            t["tg_err"] = "no files group set (use /ws)"
            return
        await TgClient.bot.send_document(
            chat_id=chat, document=t["path"],
            file_name=t.get("file") or None)
        t["tg"] = "sent"
        _evt(f"tg-sent {t['id'][:8]} file={t.get('file', '?')}")
    except Exception as e:
        t["tg"] = "error"
        t["tg_err"] = f"{e.__class__.__name__}: {e}"[:150]
        _evt(f"tg-fail {t['id'][:8]}: {t['tg_err']}")


def _users_col():
    return _db().wzfix_config[f"webdl_users_{_part()}"]


def _today_ist():
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() + 5.5 * 3600))


async def _user_doc(dev):
    """Per-device doc {n today, day, banned, limit_gb, seen}; DB-fail safe."""
    try:
        doc = await _users_col().find_one({"_id": dev}) or {}
        if str(doc.get("day", "")) != _today_ist():
            doc["day"], doc["n"] = _today_ist(), 0
        doc.setdefault("n", 0)
        return doc
    except Exception:
        return {"n": 0}


async def _user_update(dev, **fields):
    try:
        await _users_col().update_one({"_id": dev}, {"$set": fields},
                                      upsert=True)
    except Exception:
        pass


# --- v17.0.0: named accounts (owner-created regular + invite specials) ---
def _hash_pass(pw, salt_hex):
    return pbkdf2_hmac("sha256", str(pw).encode(),
                               bytes.fromhex(salt_hex), 120_000).hex()


def _new_acct(name, pw, role, fdev=""):
    salt = token_hex(16)
    return {"_id": f"u:{name}", "salt": salt,
            "pass_h": _hash_pass(pw, salt), "role": role,
            "dev": fdev, "banned": False, "n": 0, "day": _today_ist(),
            "bw": 0, "bw_day": _today_ist(), "created": int(time.time()),
            "seen": int(time.time()), "ip": "", "ua": ""}


async def _acct(name):
    """Account doc or None. Names are [a-z0-9_.-]{2,24}."""
    name = re.sub(r"[^a-z0-9_.-]", "", str(name or "").lower())[:24]
    if not 2 <= len(name) <= 24:
        return None, None
    try:
        doc = await _users_col().find_one({"_id": f"u:{name}"}) or None
    except Exception:
        doc = None
    return doc, name


async def _acct_update(name, **fields):
    try:
        await _users_col().update_one({"_id": f"u:{name}"},
                                      {"$set": fields}, upsert=True)
    except Exception:
        pass


async def _stats_doc():
    try:
        col = _db().wzfix_config[_part()]
        doc = await col.find_one({"_id": "webdl_stats"}) or {}
        if str(doc.get("day", "")) != _today_ist():
            doc["day"], doc["bw"] = _today_ist(), 0
        doc.setdefault("likes", 0)
        doc.setdefault("dl_total", 0)
        doc.setdefault("users", 0)
        return doc
    except Exception:
        return {"likes": 0, "dl_total": 0, "users": 0, "bw": 0,
                "day": _today_ist()}


async def _stats_update(**fields):
    try:
        col = _db().wzfix_config[_part()]
        await col.update_one({"_id": "webdl_stats"}, {"$set": fields},
                             upsert=True)
    except Exception:
        pass


async def _stats_inc(field, by):
    try:
        col = _db().wzfix_config[_part()]
        await col.update_one({"_id": "webdl_stats"},
                             {"$inc": {field: by}}, upsert=True)
    except Exception:
        pass


_ROLE_SUB = {"v": "member", "s": "special", "o": "owner", "g": "guest",
             "q": "quick"}
# v19.1.0: usernames are strictly letters, digits, _ and - (no dots or
# symbols). Anything else is refused AND recorded for the owner.
_NAME_RE = re.compile(r"^[a-z0-9_-]{2,24}$")
_NAME_HELP = ("usernames can only use letters, numbers, _ and - "
              "(2-24 characters) — no dots, spaces or symbols")


def _net_key(ip):
    """Network fingerprint: one guest per network, not per IP address.

    IPv4 -> /24, IPv6 -> first four hextets, so carrier NAT and sub-ips
    cannot be used to farm extra guest slots.
    """
    ip = str(ip or "").strip()
    if not ip:
        return ""
    if ":" in ip:
        return ":".join(ip.split(":")[:4])
    parts = ip.split(".")
    if len(parts) == 4:
        return ".".join(parts[:3])
    return ip


def _gen_creds():
    """v19.2.0: a readable username + password for a quick account."""
    from secrets import choice
    _al = "abcdefghjkmnpqrstuvwxyz23456789"
    name = "user_" + token_hex(3)
    pw = "".join(choice(_al) for _ in range(10))
    return name, pw


async def _net_busy(net, exclude=""):
    """Is a guest device or quick account already on this network?"""
    if not net:
        return False
    try:
        for _o in await _users_col().find({}).to_list(800):
            _oid = str(_o.get("_id", ""))
            if _oid == exclude or _o.get("banned"):
                continue
            if _oid.startswith(("ip:", "like:", "rep:", "inv:", "att:",
                                "allow:")):
                continue
            if _o.get("role") == "v" or _o.get("role") == "s":
                continue  # real members/specials are not guest-like
            if _net_key(str(_o.get("ip", "") or "")) == net:
                return True
    except Exception:
        pass
    return False


async def _record_attempt(kind, raw, request, reason):
    """v19.1.0: log what someone tried to sign up with, and why it failed."""
    try:
        _ip = _client_ip(request)
        _ua = _ua_short(request)
        _raw = str(raw or "")[:60]
        await _users_col().insert_one({
            "_id": f"att:{int(time.time() * 1000)}:{token_hex(3)}",
            "kind": kind, "name": _raw, "ip": _ip, "net": _net_key(_ip),
            "ua": _ua, "at": int(time.time()), "reason": reason})
        _evt(f"signup attempt ({reason}): {_raw!r} ip={_ip}")
    except Exception:
        pass
    try:
        await _notify(f"🛡 <b>blocked signup attempt</b>\n┣ tried: "
                      f"<code>{str(raw or '')[:60]}</code>\n┣ reason: "
                      f"{reason}\n┣ ip: <code>{_client_ip(request)}</code>\n"
                      f"┗ owner can allow this ip once in settings → guard")
    except Exception:
        pass


async def _net_blocked(ip):
    """v19.4.0: is this IP (or its whole network) blocked by the owner?"""
    ip = str(ip or "").strip()
    if not ip:
        return False
    net = _net_key(ip)
    try:
        for _key in (f"block:{ip}", f"blocknet:{net}"):
            if net and await _users_col().find_one({"_id": _key}):
                return True
    except Exception:
        pass
    return False


async def _allow_consume(ip):
    """Consume a one-time owner pass for this IP (if one is waiting)."""
    ip = str(ip or "").strip()
    if not ip:
        return False
    try:
        doc = await _users_col().find_one({"_id": f"allow:{ip}"})
        if doc and not doc.get("used"):
            await _users_col().delete_one({"_id": f"allow:{ip}"})
            _evt(f"one-time pass used by {ip}")
            return True
    except Exception:
        pass
    return False
_THEMES = ("midnight", "ocean", "sunset", "forest", "light")


def _slots_for(s, role):
    """v19.0.0: per-role concurrency pools (owner is unlimited)."""
    if role == "o":
        return 999
    if role in ("v", "s"):
        return max(1, int(s.get("member_slots", 2) or 2))
    return max(1, int(s.get("guest_slots", 2) or 2))


def _pool(role):
    """Slot pool: owner / members (v+s) / guests each have their own."""
    if role == "o":
        return "o"
    return "m" if role in ("v", "s") else "g"


def _running_pool(role):
    p = _pool(role)
    return sum(1 for t in _TASKS.values()
               if _pool(t.get("role", "g")) == p
               and t["status"] in ("queued", "downloading", "processing"))


def _role_limits(s, role):
    """(daily, max_gb) for a role; guests keep the guest knobs."""
    if role == "s":
        return int(s["s_daily"]), float(s["s_max_gb"])
    if role == "v":
        return int(s["v_daily"]), float(s["v_max_gb"])
    return int(s["user_daily"]), float(s["user_max_gb"])


def _fp_dev(fp):
    """Stable device id from a browser fingerprint (survives clearing
    site data - the same browser/hardware recomputes the same id)."""
    fp = str(fp or "").strip()[:512]
    if not fp:
        return ""
    return "fp-" + sha256(fp.encode()).hexdigest()[:20]


async def _ip_doc(ip):
    """Per-IP counter doc (anti-evasion: clearing storage keeps the count)."""
    try:
        doc = await _users_col().find_one({"_id": f"ip:{ip}"}) or {}
        if str(doc.get("day", "")) != _today_ist():
            doc["day"], doc["n"] = _today_ist(), 0
        doc.setdefault("n", 0)
        return doc
    except Exception:
        return {"n": 0}


async def _count_download(dev, ip):
    """Count one REAL download (called once per task, on first progress)."""
    try:
        u = await _user_doc(dev)
        await _user_update(dev, day=_today_ist(),
                           n=int(u.get("n", 0)) + 1)
    except Exception:
        pass
    try:
        await _users_col().update_one(
            {"_id": f"ip:{ip}"},
            {"$set": {"day": _today_ist()}, "$inc": {"n": 1}},
            upsert=True)
    except Exception:
        pass


_COOKIE_ERR_RE = re.compile(
    r"sign in|not a bot|cookies|age.?restrict|confirm you|login required",
    re.I)
_YT_403_RE = re.compile(r"403|forbidden", re.I)


async def _cookie_file():
    """Materialize the owner's YouTube cookies (sent via /ws) to disk."""
    try:
        s = await _settings()
        c = str(s.get("yt_cookies", "") or "").strip()
        if not c:
            return ""
        if not c.startswith("# Netscape"):
            c = "# Netscape HTTP Cookie File\n" + c
        p = os.path.join(os.path.dirname(_base_dir())
                         or "/tmp", "webdl_cookies.txt")
        with open(p, "w", encoding="utf-8") as f:
            f.write(c)
        return p
    except Exception:
        return ""


def _cookie_hint(err):
    if _COOKIE_ERR_RE.search(err or ""):
        return (" — YouTube wants a login: cookies missing/expired. "
                "Owner, send fresh cookies.txt via /ws.")
    return ""


def _explain_err(err, url="", role="g"):
    """Map a raw yt-dlp error to (plain-language why, is_cookie_style)."""
    e = err or ""
    cookie = bool(_COOKIE_ERR_RE.search(e))
    if not cookie and _YT_403_RE.search(e) and (
            "youtu" in (url or "") or "googlevideo" in e.lower()
            or "unable to download video data" in e.lower()):
        # yt-dlp 403 on the media CDN is in practice a cookies/PO issue
        cookie = True
    if cookie:
        if role == "o":
            return ("YouTube blocked this download — your cookies are "
                    "missing or stale: send fresh ones via /ws → YouTube "
                    "cookies", True)
        return ("YouTube blocked this download — it usually needs fresh "
                "cookies; the owner has been notified", True)
    if re.search(r"429|too many requests", e, re.I):
        return ("rate limited — the server asked too often; wait a bit "
                "and retry", False)
    if re.search(r"404|not available|removed|private|unavailable", e, re.I):
        return ("the video is gone, private, or blocked in this region",
                False)
    if re.search(r"timed? ?out|connection|network|unreachable|resolve",
                 e, re.I):
        return ("a network problem between the server and the site — a "
                "retry usually fixes it", False)
    return ("the download failed — exact error below", False)


async def _report_fail(t):
    """Detailed failure notice -> TG logs group (+ owner DM if cookie-style)."""
    try:
        raw = (t.get("err") or "")[:300]
        why = t.get("why") or ""
        url = (t.get("url") or "")[:120]
        msg = ("[webdl] download FAILED\n"
               f"why: {why or 'unknown'}\n"
               f"error: {raw}\n"
               f"url: {url}\n"
               f"dev {str(t.get('dev') or '')[:24]} · "
               f"IP {t.get('ip') or '?'} · "
               f"{str(t.get('ua') or '')[:60]}")
        await _notify(msg)
        if t.get("_cookie_fail"):
            try:
                from ...core.tg_client import TgClient
                from ...core.config_manager import Config

                await TgClient.bot.send_message(
                    chat_id=Config.OWNER_ID,
                    text=("[webdl] ⚠️ A download just failed with a "
                          "cookies/block error.\n"
                          f"error: {raw[:200]}\n"
                          f"video: {url}\n"
                          "→ fix: /ws → YouTube cookies → send a fresh "
                          "cookies.txt (the message with the token... "
                          "just cookies.txt is deleted after saving)"),
                    disable_web_page_preview=True)
            except Exception:
                pass
    except Exception:
        pass


def _ua_short(request):
    ua = (request.headers.get("User-Agent", "") or "")[:180]
    if not ua:
        return "unknown"
    os_ = ("Android" if "Android" in ua else
           "iPhone" if "iPhone" in ua else
           "iPad" if "iPad" in ua else
           "Windows" if "Windows" in ua else
           "Mac" if "Mac" in ua else
           "Linux" if "Linux" in ua else "?")
    m = re.search(r"(Edg|OPR|CriOS|FxiOS|Firefox|Chrome|Safari)/[\d.]+", ua)
    return f"{os_} · {m.group(0) if m else 'browser?'}"


async def _audit(kind, detail="", ip="", ua="", dev=""):
    """Log an event to the activity feed AND the Telegram logs group."""
    try:
        line = f"[webdl] {kind}"
        if detail:
            line += f" · {str(detail)[:120]}"
        if ip:
            line += f" · IP {ip}"
        if ua:
            line += f" · {ua}"
        if dev:
            line += f" · dev {str(dev)[:24]}"
        _evt(f"{kind}{' ' + str(detail)[:80] if detail else ''}"[:180])
        asyncio.get_event_loop().create_task(_notify(line))
    except Exception:
        pass


async def _audit_req(kind, detail="", request=None, dev=""):
    try:
        await _audit(kind, detail, _client_ip(request),
                     _ua_short(request), dev)
    except Exception:
        pass


# ----------------------------------------------------------------------------
# CORS (so the GitHub Pages site can call this API; no Worker change)
# ----------------------------------------------------------------------------

def _client_ip(request):
    fwd = request.headers.get("X-Forwarded-For", "")
    if fwd:
        return fwd.split(",")[0].strip()[:64]
    try:
        return (request.remote or "?")[:64]
    except Exception:
        return "?"


def _cors_headers(request, origins):
    hdrs = {}
    org = request.headers.get("Origin", "")
    if org and (org in origins or "*" in origins):
        hdrs["Access-Control-Allow-Origin"] = org
        hdrs["Access-Control-Allow-Credentials"] = "true"
        hdrs["Vary"] = "Origin"
    return hdrs


def _ua_ok(request):
    ua = (request.headers.get("User-Agent") or "").strip().lower()
    if not ua or len(ua) < 10 or any(b in ua for b in _BAD_UA):
        return False
    return any(k in ua for k in _KNOWN_UA)


# ----------------------------------------------------------------------------
# login rate limiting (in memory; 5 fails -> 10 min lock)
# ----------------------------------------------------------------------------

_FAILS = {}


def _ip_locked(ip):
    d = _FAILS.get(ip)
    if not d:
        return False
    if d["until"] and time.time() < d["until"]:
        return True
    if d["until"] and time.time() >= d["until"]:
        _FAILS.pop(ip, None)
        return False
    return False


def _ip_fail(ip):
    d = _FAILS.setdefault(ip, {"n": 0, "until": 0})
    d["n"] += 1
    if d["n"] >= 3:
        d["until"] = time.time() + 1800
        d["n"] = 0


# ----------------------------------------------------------------------------
# tasks: yt-dlp subprocesses + an in-memory registry
# ----------------------------------------------------------------------------

_TASKS = {}
_SEM = None
_STARTED = False
_PROG_RE = re.compile(
    r"\[download\]\s+([\d.]+)% of\s+~?\s*([\d.]+\w+)"
    r"(?:\s+at\s+([\d.]+\w+))?(?:\s+ETA\s+([\d:]+))?")


def _base_dir():
    base = _env("DOWNLOAD_DIR", "")
    if not base:
        base = os.getcwd()
    if not base.endswith("/"):
        base += "/"
    d = os.path.join(base, "webdl")
    os.makedirs(d, exist_ok=True)
    return d


def _dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def _nice(b):
    b = float(b or 0)
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if b < 1024 or u == "TiB":
            return f"{b:.1f} {u}" if u != "B" else f"{int(b)} B"
        b /= 1024
    return f"{b:.1f} TiB"


async def _hist_rows(limit=400):
    """v19.3.0: finished-download history from the DB (in-memory fallback).

    Persisted, so a kernel restart no longer wipes everyone's history.
    """
    rows = []
    try:
        rows = await _users_col().find(
            {"_id": {"$regex": "^hist:"}}).to_list(limit)
    except Exception:
        rows = []
    if not rows:
        rows = [dict(h) for h in _HISTORY]
    rows.sort(key=lambda h: int(h.get("done_at", 0) or 0))
    return rows


def _task_public(t):
    return {
        "id": t["id"],
        "title": t.get("title") or t["url"][:80],
        "url": t["url"],
        "mode": t["mode"],
        "status": t["status"],
        "pct": round(t.get("pct", 0.0), 1),
        "size": t.get("size", 0),
        "speed": t.get("speed", ""),
        "eta": t.get("eta", ""),
        "file": t.get("file", ""),
        "pd": t.get("pd", ""),
        "pd_status": t.get("pd_status", ""),
        "pd_err": t.get("pd_err", ""),
        "pd_pct": round(t.get("pd_pct", 0.0), 1),
        "locked": bool(t.get("pw_hash")),
        "sub_ready": bool(t.get("sub_path") and os.path.isfile(
            t.get("sub_path", ""))),
        "dls": int(t.get("dls", 0)),
        "error": t.get("err", ""),
        "why": t.get("why", ""),
        "tg": t.get("tg", ""),
        "tg_ok": (t.get("status") == "done" and bool(t.get("path"))
                  and os.path.isfile(t.get("path"))
                  and os.path.getsize(t["path"]) <= _TG_MAX),
        "t0": t["t0"],
        "done_at": t.get("done_at", 0),
    }


def _url_ok(u):
    u = (u or "").strip()
    if len(u) > 2000 or any(c in u for c in " \t\r\n\x00"):
        return False
    if u.startswith("magnet:?"):
        return "xt=" in u  # needs at least an xt= info hash
    return u.startswith("http://") or u.startswith("https://")


_GENERIC_FILEHOST_HINTS = (
    "mediafire", "pixeldrain", "dropbox", "drive.google.com",
    "docs.google.com", "1fichier", "file.io",
    "anonfiles", "upload.ee", "zippyshare", "rapidgator", "katfile",
    "uploaded.net", "keep.sh", "gofile", "streamtape", "doodstream",
    "filemoon", "mixdrop", "uqload", "userscloud", "krakenfiles",
    "solidfiles", "we.tl", "send.cm", "wetransfer", "odrive"
)


def _webdl_generic_host(url):
    """Return True for direct file-host URLs that often need a plain HTTP fallback."""
    try:
        host = (url or "").split("//", 1)[1].split("/", 1)[0].lower()
    except Exception:
        return False
    return any(h in host for h in _GENERIC_FILEHOST_HINTS)


def _webdl_google_drive_direct(url):
    """Normalize a Google Drive share link to a direct download endpoint."""
    try:
        host = (url or "").lower()
        if "drive.google.com" not in host and "docs.google.com" not in host:
            return url
        m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", url)
        if not m:
            m = re.search(r"/file/d/([A-Za-z0-9_-]+)", url)
        if not m:
            return url
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    except Exception:
        return url


def _webdl_mediafire_direct(url):
    """Resolve a MediaFire share page to a direct file URL when possible."""
    try:
        import requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=(20, 60), allow_redirects=True)
        final_url = resp.url
        if final_url and ("mediafire.com" in final_url.lower() and ("download" in final_url.lower() or "/file/" in final_url.lower())):
            if "/download/" in final_url.lower() or "download" in final_url.lower():
                return final_url
        text = resp.text or ""
        for pat in (
            r'https?://[^"\'\s>]+(?:download|download\.mediafire|mediafire)\.[^"\'\s>]+',
            r'"(https?://download[0-9]+\.mediafire\.com[^"\\s]+)"',
            r'"(https?://[^"\\s]*mediafire[^"\\s]*)"',
        ):
            m = re.search(pat, text, re.I)
            if m:
                cand = m.group(1)
                if cand.startswith("http"):
                    return cand
        # Some MediaFire pages include a direct link in a JSON field.
        for key in ("download_link", "direct_link", "url"):
            m = re.search(rf'"{key}"\s*:\s*"(https?://[^"\\]+)"', text, re.I)
            if m:
                cand = m.group(1)
                if "mediafire" in cand.lower():
                    return cand
        return final_url if "mediafire" in final_url.lower() else url
    except Exception:
        return url


def _webdl_dropbox_direct(url):
    """Normalize a Dropbox share URL to a direct file endpoint."""
    try:
        if "dropbox.com" not in (url or "").lower():
            return url
        if "?dl=1" in url:
            return url
        if "?" in url:
            return url + "&dl=1"
        return url + "?dl=1"
    except Exception:
        return url


def _webdl_site_direct_url(url):
    """Return a concrete direct-download URL when the host exposes a direct route."""
    u = (url or "").strip()
    if not u:
        return None
    host = u.split("//", 1)[1].split("/", 1)[0].lower() if "//" in u else ""
    if "drive.google.com" in host or "docs.google.com" in host:
        return _webdl_google_drive_direct(u)
    if "mediafire.com" in host:
        return _webdl_mediafire_direct(u)
    if "dropbox.com" in host:
        return _webdl_dropbox_direct(u)
    return None


def _webdl_generic_filename(url, fallback="download.bin"):
    try:
        path = (url or "").split("?", 1)[0].rsplit("/", 1)[1]
        if path and "." in path:
            return re.sub(r"[^A-Za-z0-9_.-]", "_", path)[:150]
    except Exception:
        pass
    return fallback


def _webdl_generic_direct(url, path, max_bytes=None):
    """Download a direct HTTP file, rejecting HTML pages and over-budget files."""
    import requests
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}
    r = requests.get(url, stream=True, timeout=(20, 600), allow_redirects=True, headers=headers)
    try:
        r.raise_for_status()
        content_type = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type or "application/xhtml" in content_type:
            raise RuntimeError("host returned a webpage, not a direct file")
        content_length = r.headers.get("Content-Length")
        if max_bytes is not None and content_length:
            try:
                if int(content_length) > max_bytes:
                    raise RuntimeError("direct file exceeds remaining disk budget")
            except ValueError:
                pass
        chunks = r.iter_content(65536)
        first = next(chunks, b"")
        if not first:
            raise RuntimeError("empty response body")
        if first.lstrip().lower().startswith((b"<!doctype html", b"<html")):
            raise RuntimeError("host returned an HTML page, not a direct file")
        if max_bytes is not None and len(first) > max_bytes:
            raise RuntimeError("direct file exceeds remaining disk budget")
        total = 0
        try:
            with open(path, "wb") as fh:
                fh.write(first)
                total = len(first)
                for chunk in chunks:
                    if not chunk:
                        continue
                    total += len(chunk)
                    if max_bytes is not None and total > max_bytes:
                        raise RuntimeError("direct file exceeds remaining disk budget")
                    fh.write(chunk)
        except Exception:
            try:
                os.remove(path)
            except OSError:
                pass
            raise
        return total
    finally:
        r.close()


# ----------------------------------------------------------------------------
# v18.0.0: pixeldrain upload route (big files, owner-configurable window)
# ----------------------------------------------------------------------------
_PD_API = "https://pixeldrain.com/api"


class _Counted:
    """File wrapper that reports upload progress into the task."""

    def __init__(self, fh, t, total):
        self.fh, self.t, self.total, self.sent = fh, t, total, 0

    def read(self, n=-1):
        b = self.fh.read(n)
        self.sent += len(b)
        if self.total:
            self.t["pd_pct"] = round(100.0 * self.sent / self.total, 1)
        return b

    def __len__(self):
        return self.total


def _pd_put_blocking(path, name, key, t):
    import requests
    url = f"{_PD_API}/file/{quote(str(name))[:180]}"
    total = os.path.getsize(path)
    with open(path, "rb") as fh:
        r = requests.put(url, data=_Counted(fh, t, total), auth=("", key),
                         timeout=(30, 3600))
    d = {}
    try:
        d = r.json()
    except Exception:
        d = {}
    if r.status_code != 200 or not d.get("id"):
        raise RuntimeError(f"pixeldrain HTTP {r.status_code}: "
                           f"{str(d.get('message') or d)[:150]}")
    return f"https://pixeldrain.com/u/{d['id']}"


async def _pd_upload(path, name, t=None):
    """Upload to pixeldrain in a worker thread, reporting progress."""
    s = await _settings()
    key = s.get("pd_key") or ""
    if not key:
        raise RuntimeError("pixeldrain not configured (owner: /ws)")
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _pd_put_blocking,
                                      path, name, key, t if t is not None
                                      else {})


async def _pd_route(t, path, size):
    """Background: upload a big file to pixeldrain, attach the link.

    Failures are honest: the direct-download button always stays, the
    owner gets the exact error via the logs group, the user sees why.
    """
    try:
        s = await _settings()
        if not s.get("pd_key"):
            t["pd_err"] = "cloud route not configured (owner: /ws)"
            return
        t["pd_status"] = "uploading"
        t["pd_pct"] = 0.0
        _evt(f"pd-upload start {t['id'][:8]} size={_nice(size)}")
        try:
            link = await _pd_upload(path, t["file"], t)
        except Exception:
            t["pd_pct"] = 0.0
            _evt(f"pd-upload retry {t['id'][:8]}")
            link = await _pd_upload(path, t["file"], t)
        t["pd"] = link
        t["pd_status"] = "done"
        t["pd_pct"] = 100.0
        _evt(f"pd-upload done {t['id'][:8]}")
        await _audit("pixeldrain upload done",
                     f"{t['file'][:50]} -> {link}",
                     ip=str(t.get("ip", "") or ""),
                     ua=str(t.get("ua", "") or ""),
                     dev=t.get("dev", ""))
    except Exception as e:
        t["pd_status"] = "error"
        t["pd_err"] = f"{e.__class__.__name__}: {e}"[:200]
        _evt(f"pd-upload FAIL {t['id'][:8]}: {t['pd_err']}")
        try:
            await _notify(f"[webdl] pixeldrain upload failed\n"
                          f"file: {t.get('file', '?')[:80]}\n"
                          f"error: {t['pd_err']}\n"
                          f"site: {t.get('url', '')[:100]}")
        except Exception:
            pass


async def _run_task(t):
    global _SEM
    s = await _settings(force=True)
    if _SEM is None:
        _SEM = asyncio.Semaphore(s["conc"])
    tdir = os.path.join(_base_dir(), t["id"])
    os.makedirs(tdir, exist_ok=True)
    log = []
    try:
        async with _SEM:
            if t["status"] == "cancel":
                return
            t["status"] = "downloading"
            _evt(f"start {t['id'][:8]} url={t.get('url', '')[:100]}")
            base = _base_dir()
            tcap_g = t.get("cap") if t.get("role") == "g" else None
            cap = s["max_gb"] * (1 << 30)
            remaining = cap - _dir_bytes(base)
            if tcap_g is not None:
                remaining = min(remaining, tcap_g * (1 << 30))
            if remaining < (1 << 20):
                t["status"] = "error"
                t["err"] = (f"disk budget full ({s['max_gb']} GB) - "
                            "delete a finished file first")
                t["why"] = "the bot's disk is full"
                return
            use_generic = t.get("url", "").startswith(("http://", "https://"))
            cmd = [
                sys.executable, "-m", "yt_dlp",
                "--newline", "--no-playlist", "--no-mtime", "--no-warnings",
                "-o", os.path.join(tdir, "%(title).120B [%(id)s].%(ext)s"),
            ]
            if use_generic:
                cmd += [
                    "--extractor-args", "generic:impersonate=chrome_110",
                    "--add-header", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                    "--retries", "8",
                    "--fragment-retries", "8",
                    "--socket-timeout", "30",
                    "--force-ipv4",
                ]
            if t["mode"] == "audio":
                cmd += ["-f", "bestaudio/best", "-x",
                        "--audio-format", "mp3", "--audio-quality", "0"]
            else:
                cmd += ["-f", t.get("fmt") or "bv*+ba/b",
                        "--merge-output-format", "mp4/mkv"]
            if t.get("subs"):
                # v18.0.0: sidecar subtitles alongside the media file
                cmd += ["--write-subs", "--write-auto-subs",
                        "--sub-langs", "en,hi,en-orig",
                        "--convert-subs", "srt"]
            # v19.0.0: no --max-filesize (it silently skips big files);
            # the live budget check below stops oversized downloads
            cmd += ["--retries", "3", "--fragment-retries", "3",
                    "--socket-timeout", "20"]
            _ck = await _cookie_file()
            if _ck:
                cmd += ["--cookies", _ck]
            cmd.append(t["url"])
            _torrent = t["url"].startswith("magnet:") or t["url"].lower().endswith(".torrent")
            if _torrent:
                # v18.0.0: magnets/torrents, same engine the bot uses.
                # aria2c handles both magnet: and http .torrent links.
                _ar = __import__("shutil").which("aria2c")
                if not _ar:
                    t["status"] = "error"
                    t["err"] = "torrent engine not available on this server"
                    t["why"] = "the bot kernel has no aria2c installed"
                    return
                cmd = [_ar, "--dir", tdir, "--seed-time=0",
                       "--console-log-level=notice", "--summary-interval=2",
                       "--max-overall-download-limit=0",
                       "--allow-overwrite=true", "--auto-file-renaming=false",
                       t["url"]]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            t["proc"] = proc
            t["pid"] = proc.pid
            _ARIA_RE = re.compile(
                r"\[#[0-9a-f]+ [\d.]+[KMGT]?iB/([\d.]+)([KMGT]?)iB\((\d+)%\)"
                r"(?:.*?DL:([\d.]+)([KMGT]?)iB)?(?:.*?ETA:([\dmhs]+\s*[\dmhs]*))?")
            while True:
                raw = await proc.stdout.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", "replace").rstrip()
                if line:
                    log.append(line)
                    del log[:-40]
                if _torrent:
                    ma = _ARIA_RE.search(line)
                    if ma:
                        if not t.get("_counted"):
                            t["_counted"] = True
                            if str(t.get("dev", "")).startswith("u:"):
                                asyncio.get_event_loop().create_task(
                                    _acct_update(t["dev"][2:], n=1,
                                                 day=_today_ist()))
                                asyncio.get_event_loop().create_task(
                                    _stats_inc("dl_total", 1))
                            elif t.get("role") == "g" and t.get("dev"):
                                asyncio.get_event_loop().create_task(
                                    _count_download(t["dev"],
                                                    str(t.get("ip", "") or "")))
                        t["pct"] = float(ma.group(3))
                        t["size"] = (ma.group(1) or "?") + (ma.group(2) or "") + "iB"
                        if ma.group(4):
                            t["speed"] = (ma.group(4) or "") + (ma.group(5) or "") + "iB/s"
                        if ma.group(6):
                            t["eta"] = ma.group(6).strip()
                        continue
                m = _PROG_RE.search(line)
                if m:
                    # v16.2.0: a download only counts once yt-dlp is
                    # actually transferring bytes - failures that never
                    # start do not eat the daily limit
                    if not t.get("_counted"):
                        t["_counted"] = True
                        if str(t.get("dev", "")).startswith("u:"):
                            asyncio.get_event_loop().create_task(
                                _acct_update(t["dev"][2:], n=1,
                                             day=_today_ist()))
                            asyncio.get_event_loop().create_task(
                                _stats_inc("dl_total", 1))
                        elif t.get("role") == "g" and t.get("dev"):
                            asyncio.get_event_loop().create_task(
                                _count_download(t["dev"],
                                               str(t.get("ip", "")) or ""))
                        asyncio.get_event_loop().create_task(_audit(
                            "download start",
                            (t.get("title") or t.get("url", ""))[:80],
                            ip=str(t.get("ip", "") or ""),
                            ua=str(t.get("ua", "") or ""),
                            dev=t.get("dev", "")))
                    t["pct"] = float(m.group(1))
                    t["size"] = m.group(2)
                    t["speed"] = m.group(3) or t.get("speed", "")
                    t["eta"] = m.group(4) or t.get("eta", "")
                    _now = time.time()
                    if _now - t.get("_bud", 0) > 3:
                        t["_bud"] = _now
                        if _dir_bytes(base) > cap or (
                                tcap_g is not None
                                and _dir_bytes(tdir) > tcap_g * (1 << 30)):
                            t["status"] = "error"
                            t["err"] = (f"stopped: download is over your "
                                        f"{s['max_gb']} GB webdl limit"
                                        if tcap_g is None else
                                        f"stopped: over the "
                                        f"{tcap_g:g} GB file limit")
                            _evt(f"budget-stop {t['id'][:8]} "
                                 f"cap={s['max_gb']}GB")
                            try:
                                proc.kill()
                            except Exception:
                                pass
                            __import__("shutil").rmtree(tdir,
                                                         ignore_errors=True)
                            return
                elif "[Merger]" in line or "[ExtractAudio]" in line:
                    t["status"] = "processing"
                elif "[download] Destination:" in line:
                    t["status"] = "downloading"
            rc = await proc.wait()
            if t["status"] == "cancel":
                return
            if rc != 0 and t.get("fmt") and not t.get("_retried"):
                # the picked format died (region-locked URL, format gone,
                # 403): retry ONCE with yt-dlp's own best choice
                t["_retried"] = True
                t["fmt"] = ""
                t["status"] = "queued"
                t["_counted"] = True  # do not double count
                _evt(f"fallback-best {t['id'][:8]}")
                asyncio.get_event_loop().create_task(_run_task(t))
                return
            if rc != 0:
                if use_generic and not t.get("_generic_fallback"):
                    t["_generic_fallback"] = True
                    t["status"] = "downloading"
                    _evt(f"fallback-generic {t['id'][:8]} url={t.get('url', '')[:120]}")
                    try:
                        resolved = _webdl_site_direct_url(t.get("url", "")) or t.get("url", "")
                        if resolved != t.get("url", ""):
                            _evt(f"generic resolved {t['id'][:8]} host={resolved[:140]}")
                        name = _webdl_generic_filename(resolved, "download.bin")
                        out = os.path.join(tdir, name)
                        _webdl_generic_direct(resolved, out,
                                              max_bytes=max(0, int(remaining)))
                        rc = 0
                        _evt(f"generic-fallback downloaded {t['id'][:8]} "
                             f"file={os.path.basename(out)}")
                    except Exception as e:
                        _evt(f"generic-fallback FAIL {t['id'][:8]}: {e}")
                if rc != 0:
                    t["status"] = "error"
                    raw_err = (log[-1] if log else f"yt-dlp exited {rc}")[:300]
                    t["err"] = raw_err + _cookie_hint("\n".join(log[-3:]))
                    why, cookieish = _explain_err(
                        "\n".join(log[-3:]), t.get("url", ""), t.get("role"))
                    t["why"] = why
                    t["_cookie_fail"] = cookieish
                    _evt(f"failed {t['id'][:8]}: {raw_err[:100]}")
                    asyncio.get_event_loop().create_task(_report_fail(t))
                    return
            # finished: pick the biggest real file in the task dir
            # (skip only yt-dlp temp/partial files - the final ext can be
            # anything, e.g. .unknown_video from the generic extractor)
            best, best_sz = "", -1
            for f in os.listdir(tdir):
                p = os.path.join(tdir, f)
                if not os.path.isfile(p) or f.startswith("."):
                    continue
                if f.endswith((".part", ".ytdl", ".temp", ".f1", ".f2",
                              ".f3", ".f4")):
                    continue
                sz = os.path.getsize(p)
                if sz > best_sz:
                    best, best_sz = p, sz
            if not best or best_sz <= 0:
                t["status"] = "error"
                t["err"] = "download produced no file"
                t["why"] = ("nothing was saved — the site probably blocked "
                            "it mid-download (a 403 in disguise)")
                _evt(f"failed {t['id'][:8]}: no file produced")
                asyncio.get_event_loop().create_task(_report_fail(t))
                return
            # v18.0.0: user-chosen filename (keep the real extension)
            if t.get("name"):
                _nm = re.sub(r"[^\w\-. ()\[\]]", "", str(t["name"]))[:100]
                if _nm and _nm not in (".", ".."):
                    _ext = os.path.splitext(best)[1]
                    _nn = _nm if _nm.lower().endswith(_ext.lower()) else (
                        os.path.splitext(_nm)[0] + _ext)
                    try:
                        os.replace(best, os.path.join(tdir, _nn))
                        best = os.path.join(tdir, _nn)
                    except Exception:
                        pass
            # v19.3.0: a finished download ALWAYS counts (small files
            # finish before any progress line appears)
            if not t.get("_counted"):
                t["_counted"] = True
                try:
                    if str(t.get("dev", "")).startswith("u:"):
                        await _acct_update(t["dev"][2:], n=1,
                                           day=_today_ist())
                        await _stats_inc("dl_total", 1)
                    elif t.get("role") == "g" and t.get("dev"):
                        await _count_download(t["dev"],
                                              str(t.get("ip", "") or ""))
                except Exception:
                    pass
            t["path"] = best
            t["file"] = os.path.basename(best)
            # v18.0.0: sidecar subtitle if the user asked and one landed
            for f in os.listdir(tdir):
                if f.endswith((".srt", ".vtt")):
                    t["sub_path"] = os.path.join(tdir, f)
                    break
            t["pct"] = 100.0
            t["size"] = _nice(best_sz)
            asyncio.get_event_loop().create_task(_audit(
                "download done",
                f"{t['file'][:60]} ({t['size']})",
                ip=str(t.get("ip", "") or ""),
                ua=str(t.get("ua", "") or ""),
                dev=t.get("dev", "")))
            t["speed"] = t["eta"] = ""
            t["status"] = "done"
            # v18.0.0: big files go to the owner's pixeldrain account
            _pl = float(s.get("pd_low_gb", 2) or 0) * (1 << 30)
            _ph = float(s.get("pd_high_gb", 4) or 0) * (1 << 30)
            if _pl < best_sz < _ph and s.get("pd_key"):
                asyncio.get_event_loop().create_task(
                    _pd_route(t, best, best_sz))
            t["done_at"] = time.time()
            _evt(f"done {t['id'][:8]} file={t.get('file', '?')} size={t.get('size', '?')}")
            if s.get("notify_done"):
                try:
                    _who = (t["dev"][2:] if str(t.get("dev", "")).startswith("u:")
                            else _ROLE_SUB.get(t.get("role", "g"), "guest"))
                    asyncio.get_event_loop().create_task(_notify(
                        f"✅ <b>webdl finished</b>\n┣ file: "
                        f"{(t.get('file') or '?')[:70]}\n┣ size: "
                        f"{t.get('size', '?')}\n┗ by: {_who}"))
                except Exception:
                    pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        t["status"] = "error"
        t["err"] = f"{e.__class__.__name__}: {e}"[:300]
        t["why"] = "internal error — exact details went to the logs group"
        _evt(f"error {t['id'][:8]}: {t['err']}")
        asyncio.get_event_loop().create_task(_report_fail(t))
    finally:
        t.pop("proc", None)
        t.pop("pid", None)
        # v18.0.0: history ring; v19.3.0: ALSO persisted to the database
        # so a kernel restart (nightly) cannot wipe it
        if t.get("status") in ("done", "error", "cancel"):
            _h = {
                "id": t["id"], "dev": t.get("dev", ""),
                "role": t.get("role", ""),
                "url": t.get("url", "")[:200],
                "title": (t.get("title") or t.get("url", ""))[:80],
                "file": t.get("file", ""), "status": t.get("status"),
                "size": t.get("size", ""), "t0": t.get("t0", 0),
                "done_at": t.get("done_at", 0) or int(time.time()),
                "dls": int(t.get("dls", 0)),
                "alive": bool(t.get("path")
                              and os.path.isfile(t.get("path", "")))}
            try:
                _HISTORY.append(_h)
                del _HISTORY[:-200]
            except Exception:
                pass
            try:
                await _users_col().update_one(
                    {"_id": f"hist:{_h['done_at']}:{_h['id']}"},
                    {"$set": dict(_h)}, upsert=True)
            except Exception:
                pass
        # v19.3.0: a cancelled task leaves the list right away (no stale
        # Cancel button that does nothing)
        if t.get("status") == "cancel":
            async def _drop(_id=t["id"]):
                await asyncio.sleep(3)
                _TASKS.pop(_id, None)
                __import__("shutil").rmtree(
                    os.path.join(_base_dir(), _id), ignore_errors=True)
            try:
                asyncio.get_event_loop().create_task(_drop())
            except Exception:
                pass


async def _cleanup_loop():
    """Delete finished files older than the TTL; enforce the disk budget."""
    while True:
        try:
            await asyncio.sleep(900)
            s = await _settings(force=True)
            base = _base_dir()
            ttl = s["ttl"] * 3600
            now = time.time()
            for tid in os.listdir(base):
                tdir = os.path.join(base, tid)
                if not os.path.isdir(tdir):
                    continue
                age = now - os.path.getmtime(tdir)
                if age > ttl:
                    rmtree(tdir, ignore_errors=True)
                    t = _TASKS.get(tid)
                    if t:
                        if t["status"] == "done":
                            t.update(status="expired", path=None, file="")
                        elif t["status"] in ("error", "cancel"):
                            _TASKS.pop(tid, None)
            # budget: oldest done files first
            cap = s["max_gb"] * (1 << 30)
            used = _dir_bytes(base)
            if used > cap:
                done = sorted(
                    (t for t in _TASKS.values() if t.get("path")),
                    key=lambda t: t.get("done_at", 0))
                for t in done:
                    if used <= cap:
                        break
                    try:
                        sz = os.path.getsize(t["path"])
                        rmtree(os.path.dirname(t["path"]), ignore_errors=True)
                        t.update(status="expired", path=None, file="")
                        used -= sz
                    except OSError:
                        pass
        except Exception:
            pass


async def _ensure_started():
    global _STARTED
    if _STARTED:
        return
    _STARTED = True
    asyncio.get_event_loop().create_task(_cleanup_loop())


# ----------------------------------------------------------------------------
# frontend (the same SPA is deployed on GitHub Pages - keep them in sync)
# ----------------------------------------------------------------------------

_PAGE = '''
<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0b0d14">
<title>Web Downloader</title>
<style>
:root{--bg:#07080d;--sf:#0d1018;--sf2:#141926;--line:#232a3d;--tx:#eef1f8;
--mut:#93a0b8;--ac:#7c5cff;--ac2:#a78bfa;--ok:#31d0aa;--err:#ff6b81;--warn:#ffb45c;
--rad:16px}
[data-theme="ocean"]{--bg:#04101a;--sf:#07192a;--sf2:#0c2337;--line:#164055;
--tx:#e8f6ff;--mut:#8fb4c9;--ac:#2bb6ff;--ac2:#67e8f9}
[data-theme="sunset"]{--bg:#150a12;--sf:#1e0f1b;--sf2:#2a1526;--line:#48203c;
--tx:#ffeee6;--mut:#c39aa8;--ac:#ff7a59;--ac2:#ffb86b}
[data-theme="forest"]{--bg:#06120d;--sf:#0a1c14;--sf2:#0f2a1e;--line:#1d4632;
--tx:#e9fff5;--mut:#93c4ac;--ac:#34d399;--ac2:#a7f3d0}
[data-theme="light"]{--bg:#f4f6fb;--sf:#ffffff;--sf2:#eef1f8;--line:#dbe1ee;
--tx:#141824;--mut:#5c6679;--ac:#6b46ff;--ac2:#8b6bff}
*{box-sizing:border-box;margin:0;-webkit-tap-highlight-color:transparent}
body{background:var(--bg);color:var(--tx);font:15px/1.55 system-ui,-apple-system,
"Segoe UI",Roboto,sans-serif;min-height:100vh;overflow-x:hidden}
body::before{content:"";position:fixed;inset:0;pointer-events:none;z-index:0;
background:radial-gradient(42% 32% at 18% -4%,color-mix(in srgb,var(--ac) 22%,transparent),transparent 70%),
radial-gradient(34% 28% at 88% 6%,color-mix(in srgb,var(--ac2) 16%,transparent),transparent 72%)}
.wrap{max-width:720px;margin:0 auto;padding:18px 15px 70px;position:relative;z-index:1}
h1{font-size:21px;letter-spacing:-.02em}
.card{background:color-mix(in srgb,var(--sf) 92%,transparent);border:1px solid var(--line);
border-radius:var(--rad);padding:15px;margin-bottom:12px;backdrop-filter:blur(8px)}
.card.tight{padding:12px}
input,select,textarea{width:100%;background:var(--sf2);border:1px solid var(--line);
color:var(--tx);border-radius:11px;padding:11px 13px;font:inherit;outline:0}
input:focus,textarea:focus{border-color:var(--ac2)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;
background:var(--sf2);border:1px solid var(--line);color:var(--tx);border-radius:999px;
padding:10px 17px;font:600 14px/1 inherit;cursor:pointer;transition:transform .12s,border-color .12s}
.btn:hover{transform:translateY(-1px);border-color:var(--ac2)}
.btn:active{transform:scale(.97)}
.btn.pri{background:linear-gradient(135deg,var(--ac),var(--ac2));border:0;color:#fff;
box-shadow:0 10px 26px -12px var(--ac)}
.btn.dng{border-color:color-mix(in srgb,var(--err) 45%,var(--line));color:var(--err)}
.btn.sm{padding:6px 12px;font-size:12px;border-radius:9px}
.btn.wide{width:100%}
.btn[disabled]{opacity:.5;cursor:default;transform:none}
.row{display:flex;gap:9px}.row>*{flex:1}
.mut{color:var(--mut);font-size:12px}
.sechead{font:700 11px/1 inherit;letter-spacing:.09em;color:var(--mut);
text-transform:uppercase;margin:18px 0 8px}
.bar{height:8px;background:var(--sf2);border-radius:99px;overflow:hidden;margin:9px 0 4px}
.bar i{display:block;height:100%;border-radius:99px;background:linear-gradient(90deg,var(--ac),var(--ac2));
transition:width .45s ease}
.bar.up i{background:linear-gradient(90deg,var(--ok),var(--ac2))}
.meta{display:flex;justify-content:space-between;color:var(--mut);font-size:12px;gap:8px}
.task{border:1px solid var(--line);border-radius:14px;padding:12px 13px;margin-bottom:9px;
background:var(--sf)}
.tname{font-weight:650;font-size:14px;word-break:break-word}
.pill{font:700 10px/1.5 inherit;padding:3px 9px;border-radius:99px;background:var(--sf2);
border:1px solid var(--line);color:var(--mut);white-space:nowrap}
.pill.ok{color:var(--ok);border-color:color-mix(in srgb,var(--ok) 40%,var(--line))}
.pill.err{color:var(--err);border-color:color-mix(in srgb,var(--err) 40%,var(--line))}
.pill.warn{color:var(--warn);border-color:color-mix(in srgb,var(--warn) 40%,var(--line))}
.pill.live{color:var(--ac2);border-color:var(--ac2);animation:pulse 1.6s infinite}
@keyframes pulse{50%{opacity:.55}}
.err{color:var(--err);font-size:13px;margin-top:7px;word-break:break-word}
.hint{color:var(--mut);font-size:12px;margin-top:7px}
.hrow{display:flex;justify-content:space-between;gap:8px;padding:8px 10px;border:1px solid var(--line);
border-radius:10px;margin-bottom:6px;background:var(--sf2);font-size:12.5px;flex-wrap:wrap;align-items:center}
.tgl{display:flex;align-items:center;gap:9px;font-size:13.5px;cursor:pointer}
.tgl input{width:auto;accent-color:var(--ac)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:9px}
@media(max-width:430px){.grid3{grid-template-columns:1fr 1fr}}
.top{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:14px}
.brand{display:flex;align-items:center;gap:9px;font-weight:700;letter-spacing:-.02em}
.logo{width:30px;height:30px;border-radius:9px;background:linear-gradient(135deg,var(--ac),var(--ac2));
display:grid;place-items:center;font-size:15px}
.iconbtn{background:var(--sf2);border:1px solid var(--line);color:var(--tx);border-radius:11px;
width:38px;height:38px;display:grid;place-items:center;cursor:pointer;font-size:16px}
.iconbtn:hover{border-color:var(--ac2)}
.hero{text-align:center;padding:22px 0 6px}
.hero h1{font-size:27px;margin-bottom:6px}
.stats{display:flex;justify-content:center;gap:9px;flex-wrap:wrap;margin:16px 0 4px}
.stat{background:var(--sf);border:1px solid var(--line);border-radius:13px;padding:9px 16px;
text-align:center;min-width:88px}
.stat b{display:block;font-size:17px}
.stat span{color:var(--mut);font-size:10.5px;letter-spacing:.04em}
.announce{display:none;border:1px solid var(--ac);border-radius:12px;padding:10px 13px;
margin-bottom:12px;font-size:13px;line-height:1.5;
background:linear-gradient(135deg,color-mix(in srgb,var(--ac) 20%,transparent),
color-mix(in srgb,var(--ac2) 10%,transparent))}
.tabs{display:flex;gap:6px;overflow-x:auto;padding-bottom:8px;margin-bottom:12px;
scrollbar-width:none;-webkit-overflow-scrolling:touch}
.tabs::-webkit-scrollbar{display:none}
.tab{white-space:nowrap;background:var(--sf2);border:1px solid var(--line);color:var(--mut);
border-radius:999px;padding:8px 14px;font:600 13px/1 inherit;cursor:pointer}
.tab.on{color:#fff;background:linear-gradient(135deg,var(--ac),var(--ac2));border-color:transparent}
.urow{display:flex;align-items:center;gap:7px;padding:9px 10px;border:1px solid var(--line);
border-radius:11px;margin-bottom:7px;background:var(--sf2);font-size:13px;flex-wrap:wrap}
.urow input{width:62px;padding:6px 8px;border-radius:8px}
.urow .dev{font-family:ui-monospace,monospace;font-size:11.5px;flex:1;min-width:100px}
.foot{text-align:center;color:var(--mut);font-size:11px;margin-top:24px;line-height:1.9}
.foot a{color:var(--ac2);text-decoration:none}
.fab{position:fixed;right:14px;bottom:16px;z-index:5;display:flex;gap:8px}
.fab .iconbtn{width:44px;height:44px;border-radius:14px;background:var(--sf);
box-shadow:0 10px 26px -14px #000}
.hide{display:none!important}
.thumb{width:100%;border-radius:12px;margin-bottom:10px;max-height:210px;object-fit:contain}
.full{min-height:100vh}
.maint{position:fixed;inset:0;z-index:40;background:var(--bg);display:grid;place-items:center;
text-align:center;padding:30px}
.maint .box{max-width:420px}
.maint h2{font-size:24px;margin-bottom:10px}
.spin{width:34px;height:34px;border-radius:50%;border:3px solid var(--line);
border-top-color:var(--ac);animation:sp .8s linear infinite;margin:0 auto 14px}
@keyframes sp{to{transform:rotate(360deg)}}
.toast{position:fixed;left:50%;bottom:22px;transform:translateX(-50%) translateY(20px);
background:var(--sf);border:1px solid var(--line);color:var(--tx);padding:11px 18px;
border-radius:999px;font-size:13.5px;z-index:60;opacity:0;transition:.25s;pointer-events:none;
box-shadow:0 14px 34px -16px #000;max-width:90vw}
.toast.on{opacity:1;transform:translateX(-50%) translateY(0)}
.chips{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.chip{background:var(--sf2);border:1px solid var(--line);border-radius:999px;padding:4px 11px;
font-size:11.5px;color:var(--mut);cursor:pointer}
.chip.on{border-color:var(--ac);color:var(--tx)}
.fmt{display:flex;justify-content:space-between;align-items:center;padding:9px 12px;
border:1px solid var(--line);border-radius:10px;margin-bottom:6px;cursor:pointer;background:var(--sf2)}
.fmt:hover{border-color:var(--ac2)}.fmt.on{border-color:var(--ac);box-shadow:0 0 0 1px var(--ac) inset}
.fmt .sz{color:var(--mut);font-size:12px;white-space:nowrap;margin-left:10px}
</style></head><body>

<!-- ===================== MAINTENANCE ===================== -->
<div class="maint hide" id="maint">
<div class="box">
<div class="spin"></div>
<h2>We'll be right back</h2>
<p class="mut" id="maintMsg">The site is under maintenance right now. Downloads
are paused for a little while — please check back soon.</p>
<div style="margin-top:18px"><button class="btn" data-act="recheck">Check again</button></div>
</div></div>

<!-- ===================== LANDING ===================== -->
<div id="s-landing" class="hide">
<div class="hero">
<div class="logo" style="width:56px;height:56px;font-size:26px;margin:0 auto 12px">⬇</div>
<h1 id="siteName">Web Downloader</h1>
<div class="mut" id="siteTag">fast downloads, straight to your device</div>
</div>
<div class="announce" id="announce"></div>
<div class="stats">
<div class="stat"><b id="stUsers">–</b><span>MEMBERS</span></div>
<div class="stat"><b id="stDls">–</b><span>DOWNLOADS</span></div>
<div class="stat"><b id="stLikes">–</b><span>LIKES</span></div>
</div>
<div style="text-align:center;margin:12px 0 16px">
<button class="btn" id="likeBtn" data-act="like">❤ Like this service</button>
</div>

<div class="card">
<div class="sechead" style="margin-top:0">Sign in</div>
<input id="user" placeholder="username" autocomplete="username">
<input id="pw" type="password" placeholder="password" autocomplete="current-password"
style="margin-top:8px">
<div class="err" id="loginErr"></div>
<div style="margin-top:11px"><button class="btn pri wide" data-act="login">Sign in</button></div>
<div class="mut" style="text-align:center;margin:10px 0 4px">— or —</div>
<button class="btn wide" data-act="invite-toggle">I have an invite code</button>
<div id="inviteBox" class="hide" style="margin-top:10px">
<input id="invCode" placeholder="invite code (WZ-XXXX)">
<input id="invUser" placeholder="pick a username" style="margin-top:8px">
<input id="invPass" type="password" placeholder="pick a password" style="margin-top:8px">
<div style="margin-top:10px"><button class="btn pri wide" data-act="invite-go">Create my account</button></div>
<div class="hint">one-time code — it makes a special account with the highest limits.<br>username: letters, numbers, _ and - only (no dots or symbols)</div>
</div>
<div id="guestRow" class="hide">
<div class="mut" style="text-align:center;margin:10px 0 4px">— or —</div>
<div id="guestOnly" class="hide">
<button class="btn wide" data-act="guest">Continue as guest</button>
<div class="hint" style="text-align:center">guests get smaller files and a few downloads a day</div>
</div>
<div id="quickOnly" class="hide" style="margin-top:10px">
<button class="btn wide" data-act="quick">⚡ Create a quick account</button>
<div class="hint" style="text-align:center">we generate a username + password for you —
same limits as guest, but it remembers you and you can keep it</div>
</div>
</div>
<div id="quickBox" class="card hide" style="border-color:var(--ac)">
<div class="tname">⚡ Your new account</div>
<div class="hint">save these now — you can sign in with them any time</div>
<div class="hrow" style="margin-top:8px"><div>username</div><b id="qkUser"></b></div>
<div class="hrow"><div>password</div><b id="qkPass"></b></div>
<div class="hrow"><div>created from</div><div class="mut" id="qkInfo"></div></div>
<div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap">
<button class="btn sm" data-act="qkcopy">📋 Copy both</button>
<button class="btn pri" data-act="qkgo">Continue →</button>
</div></div>
</div>

<div class="card">
<div class="sechead" style="margin-top:0">Limits</div>
<div class="hrow"><div>👤 Member</div><div class="mut" id="limMember">bigger files, more per day</div></div>
<div class="hrow"><div>⭐ Special</div><div class="mut" id="limSpecial">invite accounts — highest limits</div></div>
<div class="hrow"><div>🔓 Unlimited</div><div class="mut">ask the owner</div></div>
<div id="contactBox" class="hide" style="margin-top:11px">
<a class="btn pri wide" id="contactBtn" target="_blank" rel="noopener"
style="text-decoration:none">Contact the owner</a></div>
</div>
<div class="foot">
files auto-delete after a few hours · powered by the bot's own engine<br>
<a id="footVer"></a></div>
</div>

<!-- ===================== APP ===================== -->
<div id="s-app" class="hide">
<div class="top">
<div class="brand"><div class="logo">⬇</div><span id="appName">Downloader</span></div>
<div style="display:flex;gap:7px">
<button class="iconbtn" id="musicBtn" data-act="music" title="Ambient sound">♪</button>
<button class="iconbtn" data-act="theme" title="Theme">◐</button>
<button class="iconbtn hide" id="gearBtn" data-act="settings" title="Settings">⚙</button>
</div></div>
<div class="announce" id="announce2"></div>
<div class="card tight">
<div class="meta"><span id="whoami">signed in</span><span id="slotTxt"></span></div>
<div class="bar hide" id="qbar"><i id="qfill" style="width:0%"></i></div>
<div class="mut" id="quotaTxt" style="margin-top:5px"></div>
</div>
<div class="card">
<div class="row">
<input id="url" placeholder="paste any link — video, song, file, or magnet:" style="flex:6">
<button class="iconbtn" data-act="paste" title="Paste" style="flex:0 0 auto">📋</button>
</div>
<div class="err" id="urlErr"></div>
<div style="margin-top:11px;display:flex;gap:9px">
<button class="btn pri" data-act="fetch" style="flex:2">Get formats</button>
<button class="btn" data-act="quick-audio" style="flex:1">♪ MP3</button>
</div>
<div class="hint">any site the bot supports · direct files · magnet &amp; torrent links</div>
</div>
<div id="pick" class="card hide">
<img id="thumb" class="thumb hide" alt="">
<div class="tname" id="metaTitle"></div>
<div class="mut" id="metaSub"></div>
<div id="fmts" style="margin-top:11px"></div>
<div class="mut" style="margin-top:9px">format string (leave unless you know it)</div>
<input id="custom" value="bv*+ba/b" style="margin-top:5px">
<div class="grid2" style="margin-top:9px">
<div><div class="mut">save as (optional)</div><input id="fname" placeholder="name.mp4"></div>
<div><div class="mut">subtitles</div>
<label class="tgl" style="padding-top:9px"><input type="checkbox" id="subs"> also fetch</label></div>
</div>
<div style="margin-top:11px;display:flex;gap:9px">
<button class="btn pri" data-act="start" style="flex:2">⬇ Download</button>
<button class="btn" data-act="start-audio" style="flex:1">♪ audio</button>
</div>
</div>
<div class="sechead">Downloads</div>
<div id="tasks"></div>
<div class="sechead">My history</div>
<div class="card tight"><div id="hist" class="mut">nothing yet</div></div>
<div style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-top:14px">
<button class="btn sm" data-act="chgpass">Change password</button>
<button class="btn sm" data-act="report">Report a problem</button>
<button class="btn sm" data-act="signout">Sign out</button>
</div>
</div>

<!-- ===================== SETTINGS (separate page) ===================== -->
<div id="s-settings" class="hide">
<div class="top">
<div class="brand"><div class="logo">⚙</div><span>Owner settings</span></div>
<button class="btn sm" data-act="back">← Back to site</button>
</div>
<div class="tabs" id="tabs"></div>
<div id="tabBody"></div>
<div class="foot">changes apply instantly — no restart needed</div>
</div>

<div class="toast" id="toast"></div>
<div class="fab">
<button class="iconbtn hide" id="fabTop" data-act="totop" title="Top">↑</button>
</div>
<script>
(function(){
var T={api:'',tok:'',role:'',user:'',meta:{},st:{},adm:{},tab:'users',theme:'midnight'};
var QK=null;
function $(i){return document.getElementById(i)}
function ls(k,v){try{if(v===undefined)return localStorage.getItem(k);
localStorage.setItem(k,v)}catch(e){return null}}
function dev(){var d=ls('webdl_dev');if(!d){d='d'+Math.random().toString(36).slice(2,10)+
Date.now().toString(36);ls('webdl_dev',d)}return d}
function fp(){try{var c=document.createElement('canvas'),x=c.getContext('2d');
x.textBaseline='top';x.font='14px Arial';x.fillStyle='#f60';x.fillRect(0,0,120,30);
x.fillStyle='#069';x.fillText('wzfix-fp',2,15);var d=c.toDataURL();
var s=[d.length,screen.width+'x'+screen.height,screen.colorDepth,navigator.language,
(navigator.languages||[]).join(','),navigator.hardwareConcurrency||0,
(Intl.DateTimeFormat().resolvedOptions().timeZone||''),(navigator.platform||'')].join('|');
var h=0;for(var i=0;i<s.length;i++){h=((h*31)+s.charCodeAt(i))>>>0}return 'f'+h.toString(16)}
catch(e){return ''}}
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){
return '&#'+c.charCodeAt(0)+';'})}
function toast(m){var t=$('toast');t.textContent=m;t.classList.add('on');
clearTimeout(t._h);t._h=setTimeout(function(){t.classList.remove('on')},2600)}
function api(p,o){o=o||{};o.headers=o.headers||{};
if(T.tok)o.headers['Authorization']='Bearer '+T.tok;
return fetch(T.api+p,o).then(function(r){
if(r.status===401&&p.indexOf('/api/login')<0&&p.indexOf('/api/dl/')<0){
T.tok='';ls('webdl_tok','');location.reload();throw new Error('session')}
return r})}
function post(p,b){return api(p,{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify(b||{})}).then(function(r){return r.json()}).catch(function(){return {}})}
function go(scr){['s-landing','s-app','s-settings'].forEach(function(i){$(i).classList.add('hide')});
$(scr).classList.remove('hide');window.scrollTo(0,0)}
function setAnnounce(a){var t=(a||'').trim();
['announce','announce2'].forEach(function(i){var el=$(i);if(!el)return;
if(t){el.textContent=t;el.style.display='block'}else{el.style.display='none'}})}
function applyTheme(n){T.theme=n||'midnight';document.documentElement.setAttribute('data-theme',T.theme);
ls('webdl_theme',T.theme)}
var Amb={ctx:null,g:null,on:false,osc:[],iv:null,
 start:function(){try{var C=window.AudioContext||window.webkitAudioContext;if(!C)return false;
 this.ctx=this.ctx||new C();var c=this.ctx;if(c.resume)c.resume();
 this.g=c.createGain();this.g.gain.value=0;this.g.connect(c.destination);
 var ch=[[220,277.18,329.63],[196,246.94,293.66],[174.61,220,261.63],[164.81,207.65,246.94]];
 var self=this,idx=0;this.osc=[];
 for(var i=0;i<3;i++){var o=c.createOscillator();o.type='sine';
 o.frequency.value=ch[0][i];o.connect(this.g);o.start();this.osc.push(o)}
 function step(){var cc=ch[idx%ch.length];idx++;var t=c.currentTime;
 self.g.gain.setTargetAtTime(0.05,t,2);
 for(var i=0;i<3;i++){if(self.osc[i])self.osc[i].frequency.setTargetAtTime(cc[i],t,1.4)}}
 step();this.iv=setInterval(step,15000);this.on=true;return true}catch(e){return false}},
 stop:function(){try{if(this.iv)clearInterval(this.iv);this.iv=null;
 this.osc.forEach(function(o){try{o.stop()}catch(e){}});this.osc=[];
 if(this.g)this.g.gain.value=0;this.on=false}catch(e){}},
 toggle:function(){if(this.on){this.stop();ls('webdl_music','0');toast('ambient sound off');
 $('musicBtn').classList.remove('chip')}
 else{if(this.start()){ls('webdl_music','1');toast('ambient sound on')}
 else{toast('audio not available on this device')}}}}
var pollT=null;
function boot(){
T.tok=ls('webdl_tok')||'';T.role=ls('webdl_role')||'';
var sameOrigin=!location.hostname.endsWith('.github.io');
T.api=sameOrigin?'':(ls('webdl_api')||'');
if(!sameOrigin){
 fetch('backend.json',{cache:'no-store'}).then(function(r){return r.ok?r.json():null})
 .then(function(d){if(d&&d.api&&d.api.indexOf('https://')===0){T.api=d.api;
 ls('webdl_api',T.api)}view()}).catch(function(){view()})}
else view();
if(ls('webdl_music')==='1'){document.addEventListener('click',function once(){
 document.removeEventListener('click',once);Amb.start()},{once:true})}
}
function view(){
if(T.tok){go('s-app');refresh();loadHist();poll()}
else{go('s-landing');loadMeta()}
$('gearBtn').classList.toggle('hide',T.role!=='o');
}
function loadMeta(){
api('/webdl/api/meta').then(function(r){return r.json()}).then(function(d){
if(!d||!d.ok){setTimeout(loadMeta,12000);return}
T.meta=d;
$('siteName').textContent=d.name||'Web Downloader';
$('appName').textContent=d.name||'Downloader';
document.title=d.name||'Web Downloader';
$('stUsers').textContent=d.users>0?d.users:'–';
$('stDls').textContent=d.downloads>0?d.downloads:'–';
$('stLikes').textContent=d.likes>0?d.likes:'–';
$('footVer').textContent='engine '+(d.version||'');
setAnnounce(d.announcement);
if(!ls('webdl_theme'))applyTheme(d.default_theme||'midnight');
$('guestRow').classList.toggle('hide',!d.guests&&!d.quick);
$('guestOnly').classList.toggle('hide',!d.guests);
$('quickOnly').classList.toggle('hide',!d.quick);
if(d.music&&!ls('webdl_music')){ls('webdl_music','1')}
if(d.contact){$('contactBox').classList.remove('hide');
$('contactBtn').href='https://t.me/'+encodeURIComponent(d.contact)}
if(d.maintenance&&T.role!=='o')showMaint(true);
}).catch(function(){setTimeout(loadMeta,12000)})}
function showMaint(on){
$('maint').classList.toggle('hide',!on);
if(on){['s-landing','s-app','s-settings'].forEach(function(i){$(i).classList.add('hide')})}}
function refresh(){
api('/webdl/api/state').then(function(r){return r.json()}).then(function(d){
if(!d||!d.ok)return;
T.st=d;T.role=d.role||T.role;T.user=d.user||T.user;
if(d.role)ls('webdl_role',d.role);
if(d.maintenance&&T.role!=='o'){showMaint(true);return}
showMaint(false);
$('gearBtn').classList.toggle('hide',T.role!=='o');
var w=$('whoami'),q=$('quotaTxt'),b=$('qbar'),f=$('qfill');
if(d.role==='o'){w.textContent='owner — unlimited everything';q.textContent='';
b.classList.add('hide')}
else{var me=d.me||{};
 w.textContent=(d.user?('signed in as '+d.user):'guest')+
 ((me.sub)?(' · '+me.sub):'');
 var bits=[];
 if(me.daily)bits.push(me.n+' of '+me.daily+' downloads today');
 if(me.cap_gb)bits.push('files up to '+me.cap_gb+' GB');
 if(me.bw_mb)bits.push((me.bw_used_mb||0)+' of '+me.bw_mb+' MB today');
 q.textContent=bits.join(' · ');
 if(me.daily){b.classList.remove('hide');
  f.style.width=Math.min(100,Math.round(100*(me.n||0)/me.daily))+'%'}
 else b.classList.add('hide')}
var sl=d.slots||{};
$('slotTxt').textContent=(d.role==='o')?'no slot limits':
 (sl.used>=sl.total?('all '+sl.total+' busy — queued'):(sl.used+' of '+sl.total+' busy'));
renderTasks(d.tasks||[]);
}).catch(function(){})}
function renderTasks(tasks){
var h='';
tasks.forEach(function(t){
if(t.status==='cancel'||t.status==='cleared')return;  // gone, not stale
h+=taskHTML(t)});
$('tasks').innerHTML=h||'<div class="card tight mut" style="text-align:center">'+
'no downloads yet — paste a link above</div>'}
function taskHTML(t){
var st=t.status,pill='<span class="pill">'+esc(st)+'</span>';
if(st==='done'){pill='<span class="pill ok">ready</span>';
if(t.dls)pill+=' <span class="pill ok">⬇ downloaded ×'+t.dls+'</span>'}
if(st==='error')pill='<span class="pill err">failed</span>';
if(st==='downloading'||st==='processing'||st==='queued')
 pill='<span class="pill live">'+esc(st)+'</span>';
var h='<div class="task"><div style="display:flex;justify-content:space-between;'+
'gap:8px;align-items:flex-start"><div class="tname">'+esc(t.title)+'</div>'+pill+'</div>';
if(st==='downloading'||st==='processing'){
h+='<div class="bar"><i style="width:'+(t.pct||0)+'%"></i></div>'+
'<div class="meta"><span>'+esc(String(t.pct||0))+'%'+(t.speed?' · '+esc(t.speed):'')+
(t.eta?' · ETA '+esc(t.eta):'')+'</span><span>'+esc(t.size||'')+'</span></div>'}
if(st==='queued')h+='<div class="hint">waiting for a free slot…</div>';
if(t.pd_status==='uploading')h+='<div class="hint" style="margin-top:8px">☁ uploading to cloud — '+
esc(String(t.pd_pct||0))+'%</div><div class="bar up"><i style="width:'+
(t.pd_pct||0)+'%"></i></div>';
if(t.error)h+='<div class="err">'+(t.why?'<div class="mut" style="margin-bottom:3px">'+
esc(t.why)+'</div>':'')+esc(t.error)+'</div>';
if(st==='done'&&t.pd)h+='<div style="margin-top:8px"><a class="btn pri wide" href="'+
esc(t.pd)+'" target="_blank" rel="noopener" style="text-decoration:none">☁ Get from cloud</a></div>';
if(st==='done'&&t.pd_status==='error')h+='<div class="hint">cloud upload failed — use Save below</div>';
var row='';
if(st==='done'){
 if(t.locked&&T.role!=='o')row+='<button class="btn pri sm" data-act="dl" data-id="'+esc(t.id)+'">🔒 Enter password</button>';
 else row+='<a class="btn pri sm" href="'+T.api+'/webdl/dl/'+encodeURIComponent(t.id)+
  '?auth='+encodeURIComponent(T.tok)+'" download style="text-decoration:none">⬇ Save</a>';
 if(t.sub_ready)row+='<a class="btn sm" href="'+T.api+'/webdl/dl/'+encodeURIComponent(t.id)+
  '?auth='+encodeURIComponent(T.tok)+'&sub=1" download style="text-decoration:none">💬 Subs</a>';
 if(T.role==='o'){
  if(t.tg_ok){row+='<button class="btn sm" data-act="tg" data-id="'+esc(t.id)+'">'+
   (t.tg==='sent'?'✓ sent':(t.tg==='sending'?'sending…':'✈ Telegram'))+'</button>'}
  row+='<button class="btn sm" data-act="lock" data-id="'+esc(t.id)+'" data-locked="'+
   (t.locked?1:0)+'">'+(t.locked?'🔓 unlock':'🔒 lock')+'</button>'}
 row+='<button class="btn sm dng" data-act="clear" data-id="'+esc(t.id)+'">✕</button>'}
if(st==='downloading'||st==='queued'||st==='processing')
 row+='<button class="btn dng sm" data-act="cancel" data-id="'+esc(t.id)+'">Cancel</button>';
if(row)h+='<div style="margin-top:9px;display:flex;gap:7px;flex-wrap:wrap">'+row+'</div>';
return h+'</div>'}
function loadHist(){
if(!T.tok)return;
api('/webdl/api/history').then(function(r){return r.json()}).then(function(d){
if(!d||!d.ok)return;var h='';
(d.history||[]).forEach(function(x){
var dt=x.done_at?new Date(x.done_at*1000).toLocaleString():'';
var st=x.status==='done'?'<span class="pill ok">done</span>':
(x.status==='cancel'?'<span class="pill">cancelled</span>':'<span class="pill err">failed</span>');
h+='<div class="hrow"><div style="flex:1;min-width:130px">'+esc(x.title||x.url)+
'</div><div class="mut">'+esc(x.size||'')+(x.dls?' · ⬇ ×'+x.dls:'')+
' · '+esc(dt)+'</div>'+st+
(x.alive?' <a class="btn sm" href="'+T.api+'/webdl/dl/'+encodeURIComponent(x.id)+
'?auth='+encodeURIComponent(T.tok)+'" download style="text-decoration:none">again</a>':'')+
'</div>'});
$('hist').innerHTML=h||'nothing yet'}).catch(function(){})}
function poll(){clearTimeout(pollT);
var ts=(T.st&&T.st.tasks)||[];
var busy=ts.some(function(t){return ['queued','downloading','processing'].indexOf(t.status)>=0||
t.pd_status==='uploading'});
pollT=setTimeout(function(){refresh();poll()},busy?2000:7000)}
var TABS=[['users','👤 Users'],['bans','🚫 Bans'],['guests','👥 Guests'],
['guard','🛡 Guard'],
['site','🎨 Site'],['cloud','☁ Cloud'],['telegram','✈ Telegram'],
['invites','🎟 Invites'],['disk','📦 Disk'],['history','🕐 History'],['tools','🔧 Tools']];
function openSettings(){go('s-settings');loadAdm()}
function loadAdm(){
api('/webdl/api/admin/state').then(function(r){return r.json()}).then(function(d){
if(!d||!d.ok){toast('settings unavailable');return}
T.adm=d;drawTabs();drawTab()}).catch(function(){toast('backend unreachable')})}
function drawTabs(){
$('tabs').innerHTML=TABS.map(function(t){
return '<button class="tab'+(t[0]===T.tab?' on':'')+'" data-act="tab" data-tab="'+t[0]+'">'+t[1]+'</button>'}).join('')}
function drawTab(){
var a=T.adm||{},g=a.globals||{},h='';
if(T.tab==='users'){
h+='<div class="card"><div style="display:flex;justify-content:space-between;align-items:center">'+
'<div class="tname">Accounts</div><button class="btn dng sm" data-act="unbanall">Unban ALL</button></div>';
(a.accounts||[]).forEach(function(u){
h+='<div class="urow"><b>'+esc(u.name)+'</b>'+
(u.role==='s'?'<span class="pill">special</span>':'<span class="pill">member</span>')+
'<span class="mut">'+esc(u.n)+'/day · '+esc(u.bw)+' MB'+(u.ip?' · '+esc(u.ip):'')+'</span>'+
(u.banned?'<span class="pill err">banned</span>':'')+
'<span class="mut">cap GB</span><input data-day="'+esc(u.name)+'" value="'+(u.daily||'')+
'" placeholder="'+esc(u.daily||'def')+'">'+
'<span class="mut">/day</span><input data-cap="'+esc(u.name)+'" value="'+(u.cap_gb||'')+
'" placeholder="'+esc(u.cap_gb||'def')+'">'+
'<button class="btn sm" data-act="ulim" data-name="'+esc(u.name)+'">set</button>'+
'<button class="btn sm" data-act="uban" data-name="'+esc(u.name)+'" data-v="'+(u.banned?0:1)+'">'+
(u.banned?'unban':'ban')+'</button>'+
'<button class="btn sm" data-act="ures" data-name="'+esc(u.name)+'">reset device</button>'+
'<button class="btn sm dng" data-act="udel" data-name="'+esc(u.name)+'">remove</button></div>'});
h+=(a.accounts||[]).length?'':'<div class="mut">no accounts yet — add members via /ws</div>';
h+='<div class="hint">cap GB + per-day are optional overrides; blank = default</div>'+
'<div style="margin-top:11px"><button class="btn pri" data-act="adminquick">⚡ New quick account</button>'+
'<span class="mut" style="margin-left:9px">generated user+pass, guest limits, binds to their device</span></div></div>';
if((a.quick||[]).length){
h+='<div class="card"><div class="tname">⚡ Quick accounts</div>'+
'<div class="hint">generated accounts with guest limits — adjustable like any member</div>';
(a.quick||[]).forEach(function(q){
h+='<div class="urow"><b>'+esc(q.name)+'</b><span class="pill">quick</span>'+
(q.bound?'<span class="pill ok">device bound</span>':'<span class="pill warn">unbound</span>')+
'<span class="mut">'+esc(q.ip||'—')+' · '+q.n+'/day · '+esc(q.bw)+' MB</span>'+
'<button class="btn sm" data-act="ginfo">ⓘ</button>'+
'<button class="btn sm" data-act="uban" data-name="'+esc(q.name)+'" data-v="'+(q.banned?0:1)+'">'+
(q.banned?'unban':'ban')+'</button>'+
'<button class="btn sm" data-act="ures" data-name="'+esc(q.name)+'">reset device</button>'+
'<button class="btn sm dng" data-act="udel" data-name="'+esc(q.name)+'">remove</button>'+
'<div class="ginfo hide" style="flex-basis:100%">'+
'<div class="hrow"><div>bound device: '+esc(q.bound?'yes':'no')+'</div>'+
'<div>seen: '+(q.seen?esc(new Date(q.seen*1000).toLocaleString()):'never')+'</div></div>'+
'<div class="hrow"><div>ip: '+esc(q.ip||'—')+'</div><div>overrides: '+
esc((q.cap_gb||'default')+' GB / '+(q.daily||'default')+' per day')+'</div></div></div></div>'});
h+='</div>'}
h+='<div class="card"><div class="tname">Member &amp; special limits</div><div class="grid2" style="margin-top:9px">'+
'<div><div class="mut">member cap (GB)</div><input id="gVMax" value="'+esc(g.v_max_gb)+'"></div>'+
'<div><div class="mut">member /day</div><input id="gVDaily" value="'+esc(g.v_daily)+'"></div>'+
'<div><div class="mut">special cap (GB)</div><input id="gSMax" value="'+esc(g.s_max_gb)+'"></div>'+
'<div><div class="mut">special /day</div><input id="gSDaily" value="'+esc(g.s_daily)+'"></div>'+
'<div><div class="mut">member slots</div><input id="gMSlots" value="'+esc((a.slots||{}).member)+'"></div>'+
'<div><div class="mut">guest slots</div><input id="gGSlots" value="'+esc((a.slots||{}).guest)+'"></div>'+
'<div><div class="mut">guest cap (GB)</div><input id="gUserMax" value="'+esc(g.user_max_gb)+'"></div>'+
'<div><div class="mut">guest /day</div><input id="gUserDaily" value="'+esc(g.user_daily)+'"></div>'+
'<div><div class="mut">file TTL (h)</div><input id="gTtl" value="'+esc(g.ttl)+'"></div>'+
'<div><div class="mut">disk max (GB)</div><input id="gMax" value="'+esc(g.max_gb)+'"></div>'+
'<div><div class="mut">global MB/day</div><input id="gBwG" value="'+esc(g.bw_global_mb)+'"></div>'+
'<div><div class="mut">user MB/day</div><input id="gBwU" value="'+esc(g.bw_user_mb)+'"></div>'+
'</div><div style="margin-top:11px"><button class="btn pri" data-act="gsave">Save limits</button></div>'+
'<div class="hint">owner is never counted in any slot or limit</div></div>'}
if(T.tab==='bans'){
var bans=(a.accounts||[]).filter(function(u){return u.banned});
var gb=(a.users||[]).filter(function(u){return u.banned});
h+='<div class="card"><div class="tname">Banned accounts</div>';
bans.forEach(function(u){h+='<div class="urow"><b>'+esc(u.name)+'</b><span class="mut">'+
esc(u.ip||'')+'</span><button class="btn sm" data-act="uban" data-name="'+esc(u.name)+
'" data-v="1">unban</button></div>'});
h+=bans.length?'':'<div class="mut">nobody is banned</div>';
h+='</div><div class="card"><div class="tname">Banned guest devices</div>';
gb.forEach(function(u){h+='<div class="urow"><span class="dev">'+esc(String(u.dev).slice(0,18))+
'…</span><button class="btn sm" data-act="gban" data-dev="'+esc(u.dev)+'" data-v="1">unban</button>'+
'<button class="btn sm dng" data-act="gdel" data-dev="'+esc(u.dev)+'">remove</button></div>'});
h+=gb.length?'':'<div class="mut">no banned guests</div>';
h+='<div style="margin-top:11px"><button class="btn dng" data-act="unbanall">Unban everyone</button></div></div>'}
if(T.tab==='guests'){
h+='<div class="card"><div class="tname">Guest devices</div>'+
'<div class="hint">one guest per network · named guest 1, guest 2…</div>';
(a.users||[]).forEach(function(u){
var nm=u.name||('device '+String(u.dev).slice(0,10));
h+='<div class="urow"><b>'+esc(nm)+'</b>'+
(u.banned?'<span class="pill err">banned</span>':'<span class="pill ok">active</span>')+
'<span class="mut">'+esc(u.net||u.ip||'—')+' · '+u.n+'/day</span>'+
'<button class="btn sm" data-act="ginfo">ⓘ</button>'+
'<input data-glim="'+esc(u.dev)+'" value="'+(u.limit_gb||'')+'" placeholder="cap GB">'+
'<button class="btn sm" data-act="gset" data-dev="'+esc(u.dev)+'">set</button>'+
'<button class="btn sm" data-act="gban" data-dev="'+esc(u.dev)+'" data-v="'+(u.banned?0:1)+'">'+
(u.banned?'unban':'ban')+'</button>'+
'<button class="btn sm dng" data-act="gdel" data-dev="'+esc(u.dev)+'">remove</button>'+
'<div class="ginfo hide" style="flex-basis:100%">'+
'<div class="hrow"><div style="flex:1;min-width:110px">id: '+esc(u.dev)+'</div></div>'+
'<div class="hrow"><div>ip: '+esc(u.ip||'—')+'</div><div>network: '+esc(u.net||'—')+'</div></div>'+
'<div class="hrow"><div>fingerprint: '+esc(u.fp||'—')+'</div></div>'+
'<div class="hrow"><div>agent: '+esc(u.ua||'—')+'</div></div>'+
'<div class="hrow"><div>seen: '+(u.seen?esc(new Date(u.seen*1000).toLocaleString()):'—')+
'</div><div>downloads: '+u.n+'</div></div></div></div>'});
h+=(a.users||[]).length?'':'<div class="mut">no guest devices yet</div>';
h+='</div><div class="card"><div class="tname">Networks today</div>';
(a.ips||[]).forEach(function(x){h+='<div class="hrow"><div>'+esc(x.ip)+'</div><div class="mut">'+
x.n+' today</div></div>'});
h+=(a.ips||[]).length?'':'<div class="mut">no activity</div>';h+='</div>'}
if(T.tab==='site'){
var st=a.site||{};
h+='<div class="card"><div class="grid2">'+
'<div><div class="mut">site name</div><input id="sName" value="'+esc(st.name||'')+'"></div>'+
'<div><div class="mut">owner contact (telegram)</div><input id="sContact" value="'+esc(st.contact||'')+'"></div>'+
'</div><div class="mut" style="margin:9px 0 4px">announcement banner</div>'+
'<textarea id="sAnn" placeholder="e.g. magnet links now work!">'+esc(a.announcement||'')+'</textarea>'+
'<div class="mut" style="margin:9px 0 4px">default theme</div>'+
'<select id="sTheme">'+['midnight','ocean','sunset','forest','light'].map(function(x){
return '<option value="'+x+'"'+(a.default_theme===x?' selected':'')+'>'+x+'</option>'}).join('')+'</select>'+
'<div class="mut" style="margin:9px 0 4px">accent colour</div>'+
'<input id="sAccent" type="color" value="'+(a.theme||'#7c5cff')+'" style="height:44px;padding:4px">'+
'<div style="display:flex;gap:14px;margin-top:11px;flex-wrap:wrap">'+
'<label class="tgl"><input type="checkbox" id="sMaint"'+(a.maintenance?' checked':'')+
'>🛠 maintenance (owner only)</label>'+
'<label class="tgl"><input type="checkbox" id="sGuests"'+((st.guests)?' checked':'')+
'>guest downloads</label>'+
'<label class="tgl"><input type="checkbox" id="sMusic"'+(a.music?' checked':'')+
'>ambient music default</label>'+
'<label class="tgl"><input type="checkbox" id="sNotify"'+(a.notify_done?' checked':'')+
'>telegram ping on finish</label>'+
'<label class="tgl"><input type="checkbox" id="sQuick"'+(a.quick_mode?' checked':'')+
'>quick accounts</label></div>'+
'<div style="margin-top:11px"><button class="btn pri" data-act="ssave">Save site</button></div></div>'}
if(T.tab==='cloud'){
var pd=a.pixeldrain||{};
h+='<div class="card"><div class="tname">☁ Pixeldrain</div>'+
'<div class="hint">status: '+(pd.on?'<b>ON</b> — big files upload to your cloud':
'<b>OFF</b> — set the API key via /ws')+'</div>'+
'<div class="grid2" style="margin-top:9px">'+
'<div><div class="mut">upload files over (GB)</div><input id="cLow" value="'+esc(pd.low_gb)+'"></div>'+
'<div><div class="mut">up to (GB)</div><input id="cHigh" value="'+esc(pd.high_gb)+'"></div></div>'+
'<div style="margin-top:11px"><button class="btn pri" data-act="csave">Save cloud</button></div>'+
'<div class="hint">files in this range skip the direct link and go to the cloud, '+
'so they still work after the bot sleeps.</div></div>'}
if(T.tab==='telegram'){
var tg=a.tg||{};
h+='<div class="card"><div class="tname">✈ Telegram routing</div>'+
'<div class="hrow"><div>logs group</div><div class="mut">'+esc(tg.logs_set||'(not set)')+'</div></div>'+
'<div class="hrow"><div>files group</div><div class="mut">'+esc(tg.files_set||'(not set)')+'</div></div>'+
(tg.err?'<div class="err">last error: '+esc(tg.err)+'</div>':'<div class="hint">no recent send errors</div>')+
'<div class="hint">group ids are set via /ws (Telegram logs group / Telegram files group). '+
'The bot must be an admin in both groups.</div>'+
'<div style="margin-top:11px"><button class="btn pri" data-act="tgtest">Send test messages</button></div>'+
'<div id="tgOut" class="hint"></div></div>'}
if(T.tab==='guard'){
h+='<div class="card"><div style="display:flex;justify-content:space-between;align-items:center">'+
'<div class="tname">🛡 Blocked signup attempts</div>'+
'<button class="btn sm dng" data-act="attclear">Clear log</button></div>'+
'<div class="hint">what they typed, where from, and why it was refused</div>';
(a.attempts||[]).forEach(function(x){
h+='<div class="urow"><b style="color:var(--err)">'+esc(x.name||'(empty)')+'</b>'+
'<span class="pill">'+esc(x.reason||'')+'</span>'+
'<span class="mut">'+esc(x.ip||'—')+'</span>'+
'<span class="mut">'+esc(x.at?new Date(x.at*1000).toLocaleString():'')+'</span>'+
'<button class="btn sm pri" data-act="allowip" data-ip="'+esc(x.ip||'')+'">allow once</button>'+
'<button class="btn sm dng" data-act="blockip" data-ip="'+esc(x.ip||'')+'" data-net="'+esc(x.net||'')+'">⛔ do not allow</button>'+
'<div class="ginfo hide" style="flex-basis:100%">'+
'<div class="hrow"><div>network: '+esc(x.net||'—')+'</div><div>kind: '+esc(x.kind||'—')+'</div></div>'+
'<div class="hrow"><div>agent: '+esc(x.ua||'—')+'</div></div></div></div>'});
h+=(a.attempts||[]).length?'':'<div class="mut">nobody tried anything bad</div>';
h+='</div><div class="card"><div class="tname">🎫 One-time IP passes</div>'+
'<div class="hint">a pass lets that ip through ONCE (guest network rule '+
'or too-many-attempts), then it is used up.</div>';
(a.allows||[]).forEach(function(x){
h+='<div class="hrow"><div>'+esc(x.ip)+'</div><div class="mut">'+
esc(x.at?new Date(x.at*1000).toLocaleString():'')+'</div>'+
'<button class="btn sm dng" data-act="allowdel" data-ip="'+esc(x.ip)+'">revoke</button></div>'});
h+=(a.allows||[]).length?'':'<div class="mut">no passes waiting</div>';
h+='</div><div class="card"><div class="tname">⛔ Blocked networks</div>'+
'<div class="hint">these can not sign up or use guests at all '+
'(accounts that already exist can still sign in).</div>';
(a.blocks||[]).forEach(function(x){
h+='<div class="hrow"><div style="flex:1;min-width:110px">'+esc(x.net||x.ip)+'</div>'+
'<div class="mut">'+esc(x.ip||'')+'</div>'+
'<div class="mut">'+esc(x.at?new Date(x.at*1000).toLocaleString():'')+'</div>'+
'<button class="btn sm" data-act="unblock" data-net="'+esc(x.net||'')+'" data-ip="'+esc(x.ip||'')+'">unblock</button></div>'});
h+=(a.blocks||[]).length?'':'<div class="mut">nothing blocked</div>';
h+='</div>'}
if(T.tab==='invites'){
h+='<div class="card"><div class="tname">🎟 Invite codes</div>'+
'<div style="display:flex;gap:9px;margin-top:9px;align-items:center">'+
'<input id="invN" type="number" value="3" min="1" max="20" style="width:80px">'+
'<button class="btn sm pri" data-act="invmake">Generate</button></div>'+
'<div id="invOut" class="hint"></div>';
(a.invites||[]).forEach(function(x){
h+='<div class="hrow"><div><b>'+esc(x.code||'(old code)')+'</b></div>'+
(x.used?'<span class="pill err">used'+(x.used_by?' by '+esc(x.used_by):'')+'</span>':
'<span class="pill ok">free</span>')+
(x.code&&!x.used?' <button class="btn sm dng" data-act="invdel" data-code="'+esc(x.code)+
'">delete</button>':'')+'</div>'});
h+=(a.invites||[]).length?'':'<div class="mut">no codes yet</div>';h+='</div>'}
if(T.tab==='disk'){
h+='<div class="card"><div class="tname">📦 Files on disk</div>'+
'<div class="hint">'+((a.disk||[]).length)+' files · budget '+(g.max_gb||'?')+' GB</div>';
(a.disk||[]).forEach(function(x){
h+='<div class="hrow"><div style="flex:1;min-width:120px">'+esc(x.file||x.id)+
'</div><div class="mut">'+esc(x.size)+'</div><div class="mut">'+esc(x.status)+'</div>'+
'<button class="btn sm dng" data-act="diskdel" data-id="'+esc(x.id)+'">delete</button></div>'});
h+='<div style="margin-top:11px"><button class="btn dng" data-act="wipe">Delete all finished files</button></div></div>'}
if(T.tab==='history'){
h+='<div class="card"><div class="tname">🕐 Recent downloads</div><div class="hint">today: '+
(a.today_downloads||0)+' · total: '+(a.dl_total||0)+' · likes: '+(a.likes||0)+'</div>';
(a.history||[]).forEach(function(x){
var who=x.dev?(String(x.dev).indexOf('u:')===0?esc(String(x.dev).slice(2)):'guest'):'—';
h+='<div class="hrow"><div style="flex:1;min-width:110px">'+esc(x.title||x.url)+
'</div><div class="mut">'+who+'</div><div class="mut">'+esc(x.size||'')+
'</div><div class="mut">'+esc(x.status)+'</div></div>'});
h+=(a.history||[]).length?'':'<div class="mut">nothing yet</div>';h+='</div>'}
if(T.tab==='tools'){
h+='<div class="card"><div class="tname">🔧 Tools</div>'+
'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:9px">'+
'<button class="btn sm" data-act="cktest">Test YouTube cookies</button>'+
'<button class="btn sm" data-act="reload">Reload settings</button>'+
'<button class="btn sm" data-act="ownpass">Change owner password</button></div>'+
'<div id="toolOut" class="hint"></div>'+
'<div class="hint" style="margin-top:9px">cookies: '+(a.yt_cookies?'set ✓':'not set')+'</div></div>'}
$('tabBody').innerHTML=h}
function loginFail(m){$('loginErr').textContent=m||'could not sign in'}
function afterLogin(d){T.tok=d.token;T.role=d.role;T.user=d.user||'';
ls('webdl_tok',d.token);ls('webdl_role',d.role);view()}
var FMT=null;
function startDl(audio){
if(!FMT){$('urlErr').textContent='tap Get formats first';return}
var cur=$('url').value.trim();
if(FMT.url&&cur&&FMT.url!==cur){
$('urlErr').textContent='the link changed — tap Get formats again';
$('pick').classList.add('hide');FMT=null;return}
var b={url:FMT.url||cur,mode:audio?'audio':'video'};
if(!audio)b.fmt=($('custom')||{}).value||'bv*+ba/b';
b.subs=($('subs')&&$('subs').checked)?1:0;
b.name=audio?'':(($('fname')||{}).value||'');
post('/webdl/api/start',b).then(function(d){
if(d.ok){$('pick').classList.add('hide');toast('download started');refresh();poll()}
else{$('urlErr').textContent=d.error||'could not start'}})}
function busyStart(on){
try{document.querySelectorAll('[data-act="start"],[data-act="start-audio"],'+
'[data-act="fetch"],[data-act="quick-audio"]').forEach(function(b){
b.disabled=!!on})}catch(e){}}
$('url').addEventListener('input',function(){
FMT=null;$('pick').classList.add('hide');$('urlErr').textContent=''});
function doFetch(audioOnly){
var u=$('url').value.trim();$('urlErr').textContent='';
if(!u){$('urlErr').textContent='paste a link first';return}
if(audioOnly){FMT={url:u,mode:'audio'};startDl(true);return}
var mag=u.indexOf('magnet:')===0||u.slice(-8).toLowerCase()==='.torrent';
if(mag){FMT={url:u};$('pick').classList.remove('hide');$('thumb').classList.add('hide');
$('metaTitle').textContent='Torrent';$('metaSub').textContent='magnet / torrent file';
$('fmts').innerHTML='<div class="mut">downloads with the same engine as the bot — tap Download below</div>';
$('custom').value='';return}
$('urlErr').textContent='reading…';busyStart(true);
post('/webdl/api/formats',{url:u}).then(function(d){
busyStart(false);$('urlErr').textContent='';
if(!d.ok){$('urlErr').textContent=(d.why?d.why+' — ':'')+(d.error||'could not read that link');return}
FMT=d;$('pick').classList.remove('hide');
if(d.thumbnail){$('thumb').src=d.thumbnail;$('thumb').classList.remove('hide')}
else $('thumb').classList.add('hide');
$('metaTitle').textContent=d.title||u;
$('metaSub').textContent=(d.extractor||'')+(d.duration?' · '+d.duration:'');
var box=$('fmts');box.innerHTML='';var def=null;
if((d.formats||[]).some(function(i){return i.vid})){
var b=document.createElement('div');b.className='fmt';
b.innerHTML='<div>Best available (recommended)</div><span class="sz"></span>';
b.onclick=function(){box.querySelectorAll('.fmt').forEach(function(x){x.classList.remove('on')});
b.classList.add('on');$('custom').value=''};box.appendChild(b);def=b}
(d.formats||[]).forEach(function(i){
var e=document.createElement('div');e.className='fmt';
e.innerHTML='<div>'+esc(i.label)+'</div><span class="sz">'+esc(i.size||'')+'</span>';
e.onclick=function(){box.querySelectorAll('.fmt').forEach(function(x){x.classList.remove('on')});
e.classList.add('on');$('custom').value=i.fmt||''};box.appendChild(e)});
if(def)def.click()})}
var ACT={
'login':function(){$('loginErr').textContent='';
var u=$('user').value.trim(),p=$('pw').value;
var b=u?{user:u,pass:p,fp:fp(),dev:dev()}:{pass:p};
post('/webdl/api/login',b).then(function(d){
if(d.token)afterLogin(d);
else if(d.maintenance)loginFail('maintenance mode — we will be back soon');
else loginFail((d.why?d.why+' — ':'')+(d.error||'could not sign in'))})},
'quick':function(){$('loginErr').textContent='';
post('/webdl/api/quick',{fp:fp(),dev:dev()}).then(function(d){
if(d&&d.ok){QK=d;$('qkUser').textContent=d.user;$('qkPass').textContent=d.pass;
$('qkInfo').textContent=(d.ip||'')+(d.device?' · '+d.device.slice(0,18)+'…':'');
$('quickBox').classList.remove('hide');$('inviteBox').classList.add('hide');
try{if($('quickBox').scrollIntoView)$('quickBox').scrollIntoView({block:'center'})}catch(e){}}
else if(d&&d.maintenance)loginFail('maintenance mode — we will be back soon');
else loginFail((d&&d.error)||'could not create a quick account')})},
'qkcopy':function(){var t='username: '+$('qkUser').textContent+
'\npassword: '+$('qkPass').textContent;
try{navigator.clipboard.writeText(t).then(function(){toast('copied ✓')})
.catch(function(){toast($('qkUser').textContent+' / '+$('qkPass').textContent)})}
catch(e){toast($('qkUser').textContent+' / '+$('qkPass').textContent)}},
'qkgo':function(){if(QK)afterLogin(QK)},
'adminquick':function(){post('/webdl/api/admin/quick',{}).then(function(d){
if(d&&d.ok){alert('quick account ready\n\nusername: '+d.user+
'\npassword: '+d.pass+'\n\n'+(d.note||''));toast('created ✓');loadAdm()}
else toast((d&&d.error)||'failed')})},
'guest':function(){$('loginErr').textContent='';
post('/webdl/api/login',{guest:1,dev:dev(),fp:fp()}).then(function(d){
if(d.token)afterLogin(d);
else if(d.maintenance)loginFail('maintenance mode — we will be back soon');
else loginFail(d.error||'guests are off right now')})},
'invite-toggle':function(){$('inviteBox').classList.toggle('hide')},
'invite-go':function(){$('loginErr').textContent='';
var code=$('invCode').value.trim(),u=$('invUser').value.trim(),p=$('invPass').value;
if(!code)return loginFail('paste your invite code');
if(!u||u.length<2)return loginFail('pick a username (2+ characters)');
if(!p||p.length<4)return loginFail('pick a password (4+ characters)');
post('/webdl/api/login',{invite:code,user:u,pass:p,fp:fp(),dev:dev()}).then(function(d){
if(d.token)afterLogin(d);
else if(d.maintenance)loginFail('maintenance mode — we will be back soon');
else loginFail(d.error||'invalid invite code')})},
'like':function(){$('likeBtn').disabled=true;
post('/webdl/api/like',{dev:dev(),fp:fp()}).then(function(d){
if(d&&d.ok){$('likeBtn').textContent='❤ Liked';$('likeBtn').disabled=true;
$('stLikes').textContent=(parseInt($('stLikes').textContent)||0)+1}
else $('likeBtn').disabled=false})},
'paste':function(){try{navigator.clipboard.readText().then(function(t){
if(t){$('url').value=t.trim();toast('pasted')}}).catch(function(){
var t=prompt('Paste your link:');if(t)$('url').value=t.trim()})}catch(e){}},
'fetch':function(){doFetch(false)},
'quick-audio':function(){doFetch(true)},
'start':function(){startDl(false)},
'start-audio':function(){startDl(true)},
'cancel':function(el){var card=el.closest('.task');
if(card)card.style.opacity='.4';
post('/webdl/api/cancel',{id:el.dataset.id}).then(function(){
if(card)card.remove();toast('cancelled');refresh()})},
'clear':function(el){post('/webdl/api/clear',{id:el.dataset.id}).then(function(){
toast('removed');refresh();loadHist()})},
'tg':function(el){post('/webdl/api/tg',{id:el.dataset.id}).then(function(){
toast('sending to telegram');refresh()})},
'lock':function(el){var id=el.dataset.id,on=el.dataset.locked==='1';
if(on){post('/webdl/api/protect',{id:id,pw:''}).then(function(d){
toast(d.ok?'unlocked':'failed');refresh()});return}
var p=prompt('set a password for this file (4+ characters):');if(!p)return;
post('/webdl/api/protect',{id:id,pw:p}).then(function(d){
toast(d.ok?'file locked':'failed');refresh()})},
'dl':function(el){var p=prompt('this file is password-protected — enter the password:');
if(!p)return;
var url=T.api+'/webdl/dl/'+encodeURIComponent(el.dataset.id)+
'?auth='+encodeURIComponent(T.tok)+'&pw='+encodeURIComponent(p);
api(url).then(function(r){if(r.status===200)window.location=url;
else toast('wrong password')})},
'chgpass':function(){if(!T.user)return toast('guests have no password');
var o=prompt('current password:');if(!o)return;
var n=prompt('new password (4+ characters):');if(!n)return;
post('/webdl/api/pass',{old:o,new:n}).then(function(d){
toast(d.ok?'password changed ✓':(d.error||'failed'))})},
'report':function(){var m=prompt('describe the problem (goes to the owner on telegram):');
if(!m)return;post('/webdl/api/report',{msg:m,fp:fp(),dev:dev()}).then(function(d){
toast(d.ok?'sent to the owner ✓':(d.error||'failed'))})},
'signout':function(){T.tok='';T.role='';T.user='';ls('webdl_tok','');ls('webdl_role','');
location.reload()},
'theme':function(){var all=['midnight','ocean','sunset','forest','light'];
var i=all.indexOf(T.theme);applyTheme(all[(i+1)%all.length]);toast('theme: '+T.theme)},
'music':function(){Amb.toggle()},
'settings':function(){openSettings()},
'back':function(){go('s-app');refresh()},
'totop':function(){window.scrollTo({top:0,behavior:'smooth'})},
'tab':function(el){T.tab=el.dataset.tab;drawTabs();drawTab()},
'recheck':function(){loadMeta();if(T.tok)refresh()},
'gsave':function(){var g=function(i){var e=$(i);return e?e.value:0};
var b={v_max_gb:parseFloat(g('gVMax'))||0,v_daily:parseInt(g('gVDaily'))||0,
s_max_gb:parseFloat(g('gSMax'))||0,s_daily:parseInt(g('gSDaily'))||0,
user_max_gb:parseFloat(g('gUserMax'))||0,user_daily:parseInt(g('gUserDaily'))||0,
ttl:parseFloat(g('gTtl'))||0,max_gb:parseFloat(g('gMax'))||0,conc:parseInt(g('gConc'))||2,
bw_global_mb:parseFloat(g('gBwG'))||0,bw_user_mb:parseFloat(g('gBwU'))||0,
member_slots:parseInt(g('gMSlots'))||2,guest_slots:parseInt(g('gGSlots'))||2};
post('/webdl/api/admin/globals',b).then(function(){toast('limits saved ✓');loadAdm()})},
'ssave':function(){var v=function(i){var e=$(i);return e?e.value:''};
var ck=function(i){var e=$(i);return !!(e&&e.checked)};
var b={site_name:v('sName'),owner_contact:v('sContact'),announcement:v('sAnn'),
default_theme:v('sTheme')||'midnight',theme:v('sAccent'),
maintenance:ck('sMaint'),guest_mode:ck('sGuests'),music:ck('sMusic'),
notify_done:ck('sNotify'),quick_mode:ck('sQuick')};
post('/webdl/api/admin/site',b).then(function(){
toast('site saved ✓');if(b.default_theme)applyTheme(b.default_theme);loadAdm()})},
'csave':function(){var lo=$('cLow'),hi=$('cHigh');
var b={pd_low_gb:parseFloat(lo&&lo.value)||0,pd_high_gb:parseFloat(hi&&hi.value)||0};
post('/webdl/api/admin/site',b).then(function(){toast('cloud window saved ✓');loadAdm()})},
'tgtest':function(){$('tgOut').textContent='sending test messages…';
post('/webdl/api/admin/tg_test',{}).then(function(d){
var l=d.logs||{},f=d.files||{};
$('tgOut').innerHTML='logs: '+(l.error?('<span style="color:var(--err)">'+esc(l.error)+'</span>'):
'<span style="color:var(--ok)">delivered ✓</span>')+
'<br>files: '+(f.error?('<span style="color:var(--err)">'+esc(f.error)+'</span>'):
'<span style="color:var(--ok)">delivered ✓</span>')})},
'invmake':function(){var n=Math.max(1,Math.min(20,parseInt($('invN').value)||1));
post('/webdl/api/admin/invites',{make:n}).then(function(d){
$('invOut').innerHTML=d.codes&&d.codes.length?
('created: '+d.codes.map(function(c){return '<b>'+esc(c)+'</b>'}).join(', ')):'failed';
loadAdm()})},
'invdel':function(el){post('/webdl/api/admin/invites',{del:el.dataset.code}).then(function(){
toast('code deleted');loadAdm()})},
'diskdel':function(el){post('/webdl/api/admin/disk',{id:el.dataset.id}).then(function(d){
toast('freed '+(d.freed_mb||0)+' MB');loadAdm()})},
'wipe':function(){if(!confirm('delete ALL finished files?'))return;
post('/webdl/api/admin/disk',{wipe:1}).then(function(d){
toast('freed '+(d.freed_mb||0)+' MB');loadAdm()})},
'cktest':function(){$('toolOut').textContent='testing cookies… (up to 90 s)';
post('/webdl/api/admin/cookie_test',{}).then(function(d){
$('toolOut').textContent=d.ok?('✓ '+(d.msg||'works')):('✗ '+(d.error||'failed'))})},
'reload':function(){post('/webdl/api/admin/reload',{}).then(function(d){
toast(d.ok?'settings reloaded ✓':'failed')})},
'ownpass':function(){var n=prompt('new owner password (4+ characters):');if(!n)return;
post('/webdl/api/admin/pass',{new:n}).then(function(d){
toast(d.ok?'owner password changed ✓':(d.error||'failed'))})},
'unbanall':function(){if(!confirm('unban every banned account and guest?'))return;
post('/webdl/api/admin/unban_all',{}).then(function(d){
toast('unbanned '+(d.count||0));loadAdm()})},
'uban':function(el){post('/webdl/api/admin/user',{user:el.dataset.name,
banned:el.dataset.v==='1'}).then(function(){toast('updated');loadAdm()})},
'ures':function(el){post('/webdl/api/admin/user',{user:el.dataset.name,reset_dev:1})
.then(function(){toast('device reset');loadAdm()})},
'udel':function(el){if(!confirm('remove '+el.dataset.name+'?'))return;
post('/webdl/api/admin/user',{user:el.dataset.name,del:1}).then(function(){
toast('account removed');loadAdm()})},
'ulim':function(el){var n=el.dataset.name;
var d=$('tabBody').querySelector('input[data-day="'+n+'"]');
var c=$('tabBody').querySelector('input[data-cap="'+n+'"]');
var dv=(d&&d.value||'').trim(),cv=(c&&c.value||'').trim();
post('/webdl/api/admin/user',{user:n,daily:dv===''?null:(parseInt(dv)||0),
cap_gb:cv===''?null:(parseFloat(cv)||0)}).then(function(){
toast('limits updated');loadAdm()})},
'gban':function(el){post('/webdl/api/admin/user',{dev:el.dataset.dev,
banned:el.dataset.v==='1'}).then(function(){toast('updated');loadAdm()})},
'gdel':function(el){if(!confirm('remove this guest device?'))return;
post('/webdl/api/admin/user',{dev:el.dataset.dev,del:1}).then(function(){
toast('guest removed');loadAdm()})},
'allowip':function(el){var ip=el.dataset.ip;if(!ip)return toast('no ip on that row');
post('/webdl/api/admin/allow',{ip:ip}).then(function(d){
toast(d.ok?('one-time pass ready for '+ip):'failed');loadAdm()})},
'blockip':function(el){var ip=el.dataset.ip,net=el.dataset.net;
if(!ip&&!net)return toast('no ip on that row');
if(!confirm('block '+(net||ip)+' from signing up or using guests?'))return;
post('/webdl/api/admin/allow',{ip:ip||net,block:1,why:'blocked from guard'})
.then(function(d){toast(d.ok?('blocked '+(d.net||ip)):'failed');loadAdm()})},
'unblock':function(el){post('/webdl/api/admin/allow',{unblock:1,
net:el.dataset.net,ip:el.dataset.ip}).then(function(d){
toast(d.ok?'unblocked ✓':'failed');loadAdm()})},
'allowdel':function(el){post('/webdl/api/admin/allow',{ip:el.dataset.ip,del:1})
.then(function(){toast('pass revoked');loadAdm()})},
'attclear':function(){if(!confirm('clear the attempt log?'))return;
post('/webdl/api/admin/allow',{clear:1}).then(function(d){
toast('cleared '+(d.cleared||0));loadAdm()})},
'ginfo':function(el){var b=el.closest('.urow').querySelector('.ginfo');
if(b)b.classList.toggle('hide')},
'gset':function(el){var i=$('tabBody').querySelector('input[data-glim="'+el.dataset.dev+'"]');
var v=parseFloat(i&&i.value)||0;
post('/webdl/api/admin/user',{dev:el.dataset.dev,limit_gb:v}).then(function(){
toast('cap saved');loadAdm()})}};
document.addEventListener('click',function(e){
var el=e.target.closest?e.target.closest('[data-act]'):null;
if(!el)return;var a=el.getAttribute('data-act');
if(!a)return;
if(el.tagName==='A')return;
e.preventDefault();
if(ACT[a]){try{ACT[a](el,e)}catch(err){toast('error: '+err.message)}}
else toast('unknown action: '+a)},{passive:false});
document.addEventListener('keydown',function(e){
if(e.key!=='Enter')return;var t=e.target;
if(t&&(t.id==='user'||t.id==='pw'))ACT['login']();
if(t&&t.id==='invPass')ACT['invite-go']()});
$('likeBtn')&&($('likeBtn').onclick=null);
applyTheme(ls('webdl_theme')||'midnight');
boot();
})();
</script></body></html>
'''


# ----------------------------------------------------------------------------
# request handlers
# ----------------------------------------------------------------------------

async def _json(request, data, status=200):
    s = await _settings()
    return web.json_response(data, status=status,
                             headers=_cors_headers(request, s["origins"]))


async def webdl_page(request):
    if not _ua_ok(request):
        return web.Response(status=403, text="forbidden")
    return web.Response(text=_PAGE, content_type="text/html",
                        headers={"Cache-Control": "no-store"})


async def webdl_api(request):
    await _ensure_started()
    path = request.path
    s = await _settings()

    # bare /webdl/ (Starlette 307s /webdl -> /webdl/ when only the
    # {path:.*} route exists) - serve the page, not a 404
    if path == "/webdl/":
        return await webdl_page(request)

    # --- CORS preflight ---
    if request.method == "OPTIONS":
        org = request.headers.get("Origin", "")
        hdrs = {}
        if org and (org in s["origins"] or "*" in s["origins"]):
            hdrs = {
                "Access-Control-Allow-Origin": org,
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers":
                    "Authorization, Content-Type",
                "Access-Control-Max-Age": "86400",
                "Vary": "Origin",
            }
        return web.Response(status=204, headers=hdrs)

    # --- file download (token via header OR ?auth= for links) ---
    if path.startswith("/webdl/dl/"):
        info = await _auth_info(request)
        if not info:
            return await _json(request, {"error": "unauthorized"}, 401)
        tid = path.rsplit("/", 1)[-1].split("?")[0]
        t = _TASKS.get(tid)
        if t and info["role"] != "o" and t.get("dev") != info["dev"]:
            return await _json(request, {"error": "not yours"}, 403)
        if not t or t.get("status") != "done" or not t.get("path"):
            return await _json(request, {"error": "not found"}, 404)
        if not os.path.isfile(t["path"]):
            t["status"] = "expired"
            return await _json(request, {"error": "expired — download it again"},
                         404)
        # v19.0.0: per-file password (owner always passes)
        if t.get("pw_hash") and info["role"] != "o":
            _given = str(request.query.get("pw", ""))
            if not compare_digest(
                    t["pw_hash"],
                    _hash_pass(_given, t.get("pw_salt", ""))):
                return await _json(request, {"error": "password required",
                                             "locked": True}, 401)
        # v18.0.0: subtitle sidecar (?sub=1)
        if request.query.get("sub") == "1" and t.get("sub_path"):
            if os.path.isfile(t["sub_path"]):
                _sh = _cors_headers(request, s["origins"])
                _sh["Content-Disposition"] = (
                    "attachment; filename*=UTF-8''" +
                    quote(os.path.basename(t["sub_path"])))
                return web.FileResponse(t["sub_path"], headers=_sh)
        # v17.0.0: egress caps (global + per-user, daily)
        try:
            _sz = os.path.getsize(t["path"])
        except Exception:
            _sz = 0
        _st = await _stats_doc()
        if (s.get("bw_global_mb") and info["role"] != "o"
                and float(_st.get("bw", 0) or 0)
                + _sz / (1 << 20) > float(s["bw_global_mb"])):
            return await _json(request,
                               {"error": "site daily bandwidth limit "
                                "reached - try again tomorrow"}, 429)
        if s.get("bw_user_mb") and info["role"] != "o":
            if str(info["dev"]).startswith("u:"):
                try:
                    _a = (await _users_col().find_one(
                        {"_id": info["dev"]}) or {})
                    if (float(_a.get("bw", 0) or 0) + _sz / (1 << 20)
                            > float(s["bw_user_mb"])):
                        return await _json(
                            request, {"error": "your daily bandwidth "
                                       "limit reached - try again "
                                       "tomorrow"}, 429)
                except Exception:
                    pass
            elif info["role"] == "g":
                try:
                    _g = await _user_doc(info["dev"])
                    if (float(_g.get("bw", 0) or 0) + _sz / (1 << 20)
                            > float(s["bw_user_mb"])):
                        return await _json(
                            request, {"error": "your daily bandwidth "
                                       "limit reached - try again "
                                       "tomorrow"}, 429)
                except Exception:
                    pass
        fname = quote(t["file"])
        hdrs = _cors_headers(request, s["origins"])
        hdrs["Content-Disposition"] = (
            f"attachment; filename=\"dl-{t['id']}{os.path.splitext(t['file'])[1]}\"; "
            f"filename*=UTF-8''{fname}")
        hdrs["Cache-Control"] = "no-store"
        # account the served bytes (egress) after the transfer starts
        try:
            asyncio.get_event_loop().create_task(_stats_inc("bw", _sz))
            if str(info["dev"]).startswith("u:"):
                asyncio.get_event_loop().create_task(
                    _users_col().update_one(
                        {"_id": info["dev"]},
                        {"$inc": {"bw": _sz / (1 << 20)}}))
            elif info["role"] == "g":
                asyncio.get_event_loop().create_task(
                    _users_col().update_one(
                        {"_id": info["dev"]},
                        {"$inc": {"bw": _sz / (1 << 20)}}))
        except Exception:
            pass
        # v19.3.0: remember that this file was actually downloaded
        try:
            t["dls"] = int(t.get("dls", 0)) + 1
            _did = int(t.get("done_at", 0) or 0)
            if _did:
                asyncio.get_event_loop().create_task(_users_col().update_one(
                    {"_id": f"hist:{_did}:{t['id']}"},
                    {"$inc": {"dls": 1}}))
        except Exception:
            pass
        try:
            return web.FileResponse(t["path"], headers=hdrs)
        except Exception as e:
            return await _json(request, {"error": f"serve failed: {e}"}, 500)

    if not await ensure_ready():
        return await _json(request, {"error": "database not reachable"}, 503)

    # --- login ---
    if path.endswith("/api/login"):
        ip = _client_ip(request)
        if not _ua_ok(request):
            return await _json(request, {"error": "forbidden"}, 403)
        if _ip_locked(ip) and not await _allow_consume(ip):
            return await _json(request, {"error": "too many attempts — wait 10 min"},
                         429)
        try:
            body = await request.json()
        except Exception:
            body = {}
        pw = str(body.get("pass", ""))
        dev = re.sub(r"[^a-zA-Z0-9_-]", "", str(body.get("dev", "")))[:64]
        fpd = _fp_dev(body.get("fp"))
        # v19.0.0: during maintenance only the owner may pass this point
        if s.get("maintenance") and (body.get("user") or body.get("guest")
                                     or body.get("invite")):
            return await _json(request, {
                "error": "maintenance mode — we will be back soon",
                "maintenance": True}, 503)

        # v17.0.0: named accounts (owner-created + invite specials)
        if body.get("user"):
            uname = str(body.get("user", ""))
            acc, name = await _acct(uname)
            invite = str(body.get("invite", "") or "").strip()[:32]
            if invite and acc:
                return await _json(request, {
                    "error": "that username is already taken — please pick "
                             "a different one"}, 409)
            if invite and not acc:
                # invite flow: first use claims the code, picks a name
                if len(pw) < 4:
                    return await _json(request,
                                       {"error": "pick a password of at "
                                        "least 4 characters"}, 400)
                if await _net_blocked(_client_ip(request)):
                    asyncio.get_event_loop().create_task(_record_attempt(
                        "invite", str(body.get("user", "")), request,
                        "blocked network"))
                    return await _json(request, {"error": "this network is "
                                           "blocked by the owner"}, 403)
                # v19.1.0: strict usernames; bad ones are logged for the
                # owner (exactly what they typed) and refused
                _raw_name = str(body.get("user", ""))
                if not _NAME_RE.match(str(name or "")):
                    asyncio.get_event_loop().create_task(_record_attempt(
                        "invite", _raw_name, request, "special characters"))
                    return await _json(request, {"error": _NAME_HELP}, 400)
                ih = sha256(invite.encode()).hexdigest()[:40]
                try:
                    inv = await _users_col().find_one(
                        {"_id": f"inv:{ih}"}) or None
                except Exception:
                    inv = None
                if not inv or inv.get("used"):
                    return await _json(
                        request, {"error": "invalid or already-used invite "
                                           "code"}, 403)
                if not fpd:
                    return await _json(request,
                                       {"error": "device id missing"}, 400)
                _now = int(time.time())
                _ipv = _client_ip(request)
                _uav = _ua_short(request)
                await _users_col().update_one(
                    {"_id": f"inv:{ih}"},
                    {"$set": {"used": True, "used_by": name,
                              "used_at": _now, "used_ip": _ipv,
                              "used_ua": _uav, "used_fp": fpd}})
                doc = _new_acct(name, pw, "s", fdev=fpd)
                # identify the person fully at signup
                doc.update({"ip": _ipv, "ua": _uav, "fp": fpd,
                            "invite": ih[:10], "seen": _now})
                try:
                    await _users_col().insert_one(doc)
                except Exception:
                    asyncio.get_event_loop().create_task(_record_attempt(
                        "invite", _raw_name, request, "name already taken"))
                    return await _json(request, {
                        "error": "that username is already taken — please "
                                 "pick a different one"}, 409)
                asyncio.get_event_loop().create_task(
                    _stats_inc("users", 1))
                asyncio.get_event_loop().create_task(_audit_req(
                    "account created (invite)", name, request, "u:" + name))
                asyncio.get_event_loop().create_task(_notify(
                    f"🎟 <b>invite used</b>\n┣ username: <code>{name}</code>\n"
                    f"┣ ip: <code>{_ipv}</code>\n┣ device: <code>{fpd}</code>\n"
                    f"┗ agent: {_uav[:60]}"))
                tok = await _session_token("s", "u:" + name)
                return await _json(request, {"ok": True, "token": tok,
                                             "role": "s", "user": name,
                                             "ttl_h": _SESSION_H})
            if not acc:
                _ip_fail(ip)
                asyncio.get_event_loop().create_task(_audit_req(
                    "account login FAIL", name, request))
                return await _json(request,
                                   {"error": "unknown user or wrong "
                                    "password"}, 401)
            if not compare_digest(acc.get("pass_h", ""),
                                  _hash_pass(pw, acc.get("salt", ""))):
                _ip_fail(ip)
                asyncio.get_event_loop().create_task(_audit_req(
                    "account login FAIL", name, request))
                return await _json(request,
                                   {"error": "unknown user or wrong "
                                    "password"}, 401)
            if acc.get("banned"):
                asyncio.get_event_loop().create_task(_audit_req(
                    "account login DENIED (banned)", name, request,
                    "u:" + name))
                return await _json(request,
                                   {"error": "this account is banned by "
                                    "the owner"}, 403)
            bdev = str(acc.get("dev", "") or "")
            if bdev and fpd and bdev != fpd:
                asyncio.get_event_loop().create_task(_audit_req(
                    "account login BLOCKED (other device)", name,
                    request, "u:" + name))
                return await _json(
                    request,
                    {"error": "this account is locked to another device - "
                     "contact the owner to move it"}, 403)
            if not bdev and fpd:
                await _acct_update(name, dev=fpd)
            await _acct_update(name, seen=int(time.time()), ip=ip,
                               ua=_ua_short(request))
            if str(acc.get("day", "")) != _today_ist():
                await _acct_update(name, day=_today_ist(), n=0)
            if str(acc.get("bw_day", "")) != _today_ist():
                await _acct_update(name, bw_day=_today_ist(), bw=0)
            _FAILS.pop(ip, None)
            asyncio.get_event_loop().create_task(_audit_req(
                f"login ({'special' if acc.get('role') == 's' else 'user'})",
                name, request, "u:" + name))
            tok = await _session_token(str(acc.get("role", "v")),
                                       "u:" + name)
            return await _json(request, {"ok": True, "token": tok,
                                         "role": acc.get("role", "v"),
                                         "user": name, "ttl_h": _SESSION_H})

        if body.get("guest"):
            if not s.get("guest_mode"):
                return await _json(request,
                                   {"error": "guest downloads are off - "
                                    "sign in with your account"}, 403)
            fpd = _fp_dev(body.get("fp"))
            if fpd:
                dev = fpd  # fingerprint beats the random id
            if not dev:
                return await _json(request, {"error": "device id missing"},
                                   400)
            u = await _user_doc(dev)
            if u.get("banned"):
                asyncio.get_event_loop().create_task(_audit_req(
                    "guest login DENIED (banned)", "", request, dev))
                return await _json(request, {"error": "this device is "
                                   "banned by the owner"}, 403)
            if await _net_blocked(ip):  # v19.4.0: owner said do not allow
                asyncio.get_event_loop().create_task(_audit_req(
                    "guest DENIED (blocked)", _net_key(ip), request, dev))
                return await _json(request, {"error": "this network is "
                                   "blocked by the owner"}, 403)
            # v19.1.0: ONE guest per network, unless the owner granted a
            # one-time pass for this IP
            _pass_used = await _allow_consume(ip)
            _net = _net_key(ip)
            if _net and not _pass_used:
                try:
                    _others = await _users_col().find({}).to_list(800)
                    for _o in _others:
                        _oid = str(_o.get("_id", ""))
                        if _oid == dev or _o.get("banned"):
                            continue
                        if _oid.startswith(("ip:", "u:", "like:", "rep:",
                                            "inv:", "att:", "allow:")):
                            continue
                        if _net_key(str(_o.get("ip", "") or "")) == _net:
                            asyncio.get_event_loop().create_task(_audit_req(
                                "guest DENIED (network already used)",
                                _net, request, dev))
                            return await _json(request, {
                                "error": "a guest is already using this "
                                         "network — sign in with an account, "
                                         "or ask the owner to allow you "
                                         "once"}, 403)
                except Exception:
                    pass
            # every guest gets a friendly name (guest 1, guest 2…)
            _gname = str(u.get("gname") or "").strip()
            if not _gname:
                try:
                    _n = await _users_col().count_documents(
                        {"_id": {"$regex": "^fp-"}}) or 0
                except Exception:
                    _n = 0
                _gname = f"guest {_n + 1}"
            await _user_update(dev, day=_today_ist(), seen=int(time.time()),
                               ip=ip, ua=_ua_short(request),
                               gname=_gname, fp=(fpd or dev))
            asyncio.get_event_loop().create_task(_audit_req(
                "guest login", "", request, dev))
            tok = await _session_token("g", dev)
            return await _json(request, {"ok": True, "token": tok,
                                         "role": "g", "ttl_h": _SESSION_H})
        good = await get_webdl_pass()
        if not good:
            return await _json(request, {"error": "password not configured yet "
                                            "(check LOG_CHAT)"}, 503)
        if pw and compare_digest(pw, good):
            tok = await _session_token("o", "")
            _FAILS.pop(ip, None)
            asyncio.get_event_loop().create_task(_audit_req(
                "OWNER login", "", request))
            return await _json(request, {"ok": True, "token": tok,
                                         "role": "o", "ttl_h": _SESSION_H})
        _ip_fail(ip)
        # v16.3.0: a device that keeps guessing the owner password is
        # banned for the day (5+ fails) - the owner can undo in the panel
        if dev:
            try:
                u = await _user_doc(dev)
                nf = 1 if str(u.get("day", "")) != _today_ist() else (
                    int(u.get("fails", 0)) + 1)
                if nf >= 5:
                    await _user_update(dev, banned=True, fails=nf,
                                       day=_today_ist())
                    asyncio.get_event_loop().create_task(_audit_req(
                        "AUTO-BAN device", f"{nf} failed password "
                        "attempts", request, dev))
                else:
                    await _user_update(dev, fails=nf, day=_today_ist())
            except Exception:
                pass
        asyncio.get_event_loop().create_task(_audit_req(
            "owner login FAIL", "", request, dev))
        await _notify(
            f"⚠️ <b>WZFIX webdl</b>\n┏ failed login\n┣ IP: <code>{ip}</code>\n"
            f"┗ via {path}")
        _evt(f"login-fail ip={ip}")
        return await _json(request, {"error": "wrong password"}, 401)

    # --- version / self-diagnosis (no auth: nothing sensitive) ---
    if path.endswith("/api/version"):
        _cfg = []
        try:
            from ...core.config_manager import Config as _Cfg
            _cfg = sorted(k for k in vars(_Cfg) if k.startswith("WEBDL_"))
        except Exception:
            pass
        _ck = ""
        try:
            _ck = "yes" if (await _settings()).get("yt_cookies") else "no"
        except Exception:
            pass
        _pd = ""
        try:
            _pd = "yes" if (await _settings()).get("pd_key") else "no"
        except Exception:
            pass
        return await _json(request, {
            "r17": R17_VERSION,
            "yt_cookies": _ck,
            "pixeldrain": _pd,
            "webdl_vars_on_config": _cfg,
            "bs_menu_will_show": bool(_cfg),
            "pass_source": "custom (WEBDL_PASS set)" if _env("WEBDL_PASS")
                           else "default (joshi)",
        })

    # --- v17.0.0: public site meta (no auth, nothing sensitive) ---
    if path.endswith("/api/meta"):
        try:
            _st = await _stats_doc()
        except Exception:
            _st = {"likes": 0, "dl_total": 0, "users": 0}
        try:
            _nu = await _users_col().count_documents(
                {"_id": {"$regex": "^u:"}}) or 0
        except Exception:
            _nu = 0
        contact = str(s.get("owner_contact", "") or "").strip()
        return await _json(request, {
            "ok": True,
            "name": s.get("site_name", "WZ Web Downloader"),
            "announcement": str(s.get("announcement", "") or "")[:300],
            "theme": str(s.get("theme", "#7c5cff")),
            "default_theme": str(s.get("default_theme", "midnight")),
            "music": bool(s.get("music")),
            "quick": bool(s.get("quick_mode")),
            "themes": list(_THEMES),
            "maintenance": bool(s.get("maintenance")),
            "contact": contact.lstrip("@"),
            "guests": bool(s.get("guest_mode")),
            "likes": int(_st.get("likes", 0)),
            "downloads": int(_st.get("dl_total", 0)),
            "users": int(_st.get("users", 0) or _nu or 0),
            "version": R17_VERSION,
        })

    # v19.2.0: "quick account" - a real account with generated credentials,
    # guest limits, bound to the device that created it.
    if path.endswith("/api/quick"):
        if s.get("maintenance"):
            return await _json(request, {"error": "maintenance mode",
                                         "maintenance": True}, 503)
        if not _ua_ok(request):
            return await _json(request, {"error": "forbidden"}, 403)
        if not s.get("quick_mode"):
            return await _json(request, {"error": "quick accounts are off "
                                   "right now - sign in with your account"},
                               403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        ip = _client_ip(request)
        if await _net_blocked(ip):
            return await _json(request, {"error": "this network is blocked "
                                   "by the owner"}, 403)
        if _ip_locked(ip) and not await _allow_consume(ip):
            return await _json(request, {"error": "too many attempts — wait "
                                   "10 min"}, 429)
        fpd = _fp_dev(body.get("fp")) or re.sub(
            r"[^a-zA-Z0-9_-]", "", str(body.get("dev", "")))[:64]
        if not fpd:
            return await _json(request, {"error": "device id missing"}, 400)
        _net = _net_key(ip)
        if await _net_busy(_net) and not await _allow_consume(ip):
            asyncio.get_event_loop().create_task(_record_attempt(
                "quick", "(network)", request, "network already has a guest"))
            return await _json(request, {
                "error": "this network already has a guest — sign in with "
                         "your account, or ask the owner to allow you once"},
                403)
        # generate a unique name + password
        name, pw = _gen_creds()
        for _try in range(5):
            if not (await _acct(name))[0]:
                break
            name, pw = _gen_creds()
        doc = _new_acct(name, pw, "q", fdev=fpd)
        doc.update({"ip": ip, "ua": _ua_short(request), "fp": fpd,
                    "created_from": "website (quick button)"})
        try:
            await _users_col().insert_one(doc)
        except Exception:
            return await _json(request, {"error": "could not create the "
                                   "account - try again"}, 500)
        asyncio.get_event_loop().create_task(_stats_inc("users", 1))
        asyncio.get_event_loop().create_task(_audit_req(
            "quick account created", name, request, "u:" + name))
        asyncio.get_event_loop().create_task(_notify(
            f"⚡ <b>quick account</b>\n┣ user: <code>{name}</code>\n"
            f"┣ ip: <code>{ip}</code>\n┣ device: <code>{fpd}</code>\n"
            f"┗ guest limits · bound to that device"))
        tok = await _session_token("q", "u:" + name)
        return await _json(request, {"ok": True, "token": tok, "role": "q",
                                     "user": name, "pass": pw,
                                     "ip": ip, "device": fpd,
                                     "ttl_h": _SESSION_H})

    if path.endswith("/api/like"):
        if s.get("maintenance"):
            return await _json(request, {"error": "maintenance mode",
                                         "maintenance": True}, 503)
        if not _ua_ok(request):
            return await _json(request, {"error": "forbidden"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        fpd = _fp_dev(body.get("fp")) or re.sub(
            r"[^a-zA-Z0-9_-]", "", str(body.get("dev", "")))[:64]
        if not fpd:
            return await _json(request, {"error": "device id missing"}, 400)
        try:
            already = await _users_col().find_one(
                {"_id": f"like:{fpd}"})
        except Exception:
            already = None
        if already:
            return await _json(request, {"ok": True, "liked": True})
        try:
            await _users_col().insert_one({"_id": f"like:{fpd}",
                                           "day": _today_ist()})
            col = _db().wzfix_config[_part()]
            await col.update_one({"_id": "webdl_stats"},
                                 {"$inc": {"likes": 1}}, upsert=True)
        except Exception:
            pass
        return await _json(request, {"ok": True, "liked": True})

    # --- everything below needs a session ---
    info = await _auth_info(request)
    if not info:
        return await _json(request, {"error": "unauthorized"}, 401)
    role, dev = info["role"], info["dev"]
    # v19.0.0: maintenance locks every action for non-owners
    if _maint(s, role):
        _leaf = path.rsplit("/", 1)[-1]
        if _leaf not in ("state",):
            return await _json(request, {
                "error": "maintenance mode — we will be back soon",
                "maintenance": True}, 503)

    if path.endswith("/api/state"):
        used = _dir_bytes(_base_dir()) if _TASKS else 0
        mine = (lambda t: True) if role == "o" else \
            (lambda t: t.get("dev") in (None, "", dev))
        me = None
        if role in ("v", "s", "q"):
            try:
                acc = (await _users_col().find_one(
                    {"_id": dev}) or {})
                _dly, _mgb = _role_limits(s, role)
                if acc.get("daily"):
                    _dly = int(acc["daily"])
                if acc.get("cap_gb"):
                    _mgb = float(acc["cap_gb"])
                me = {"n": int(acc.get("n", 0)),
                      "daily": _dly, "banned": bool(acc.get("banned")),
                      "user": str(acc.get("_id", ""))[2:],
                      "bw_mb": float(s.get("bw_user_mb", 0) or 0),
                      "cap_gb": _mgb,
                      "bw_used_mb": round(float(acc.get("bw", 0) or 0), 1),
                      "sub": _ROLE_SUB.get(role, "member")}
            except Exception:
                me = {"n": 0, "daily": _role_limits(s, role)[0],
                      "banned": False, "user": "", "bw_mb": 0}
        if role == "g":
            try:
                u = await _user_doc(dev)
                _n = int(u.get("n", 0))
                _ipi = str(u.get("ip", "") or "")
                if _ipi:
                    _n = max(_n, int((await _ip_doc(_ipi)).get("n", 0)))
                me = {"n": _n,
                      "daily": int(s["user_daily"]),
                      "banned": bool(u.get("banned"))}
            except Exception:
                me = {"n": 0, "daily": int(s["user_daily"]), "banned": False}
        _slots_used = sum(1 for t in _TASKS.values()
                          if t["status"] in ("queued", "downloading",
                                             "processing"))
        return await _json(request, {
            "ok": True,
            "role": role,
            "user": (dev[2:] if dev.startswith("u:") else ""),
            "tg_allowed": role == "o",
            "me": me,
            "slots": {"used": _running_pool(role),
                      "total": _slots_for(s, role)},
            "maintenance": bool(s.get("maintenance")),
            "tasks": [_task_public(t) for t in _TASKS.values()
                      if mine(t)],
            "limits": {
                "ttl_h": s["ttl"],
                "max_gb": s["max_gb"],
                "conc": s["conc"],
                "used_bytes": used,
            },
            "version": R17_VERSION,
            "events": _EVENTS[-20:],
        })

    if path.endswith("/api/formats"):
        try:
            body = await request.json()
        except Exception:
            body = {}
        url = str(body.get("url", "")).strip()
        if not _url_ok(url):
            return await _json(request, {"error": "give me an http(s) link"}, 400)
        try:
            _fcmd = [sys.executable, "-m", "yt_dlp", "-J", "--no-playlist",
                     "--no-warnings", "--skip-download"]
            _ck = await _cookie_file()
            if _ck:
                _fcmd += ["--cookies", _ck]
            _fcmd.append(url)
            proc = await asyncio.create_subprocess_exec(
                *_fcmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
        except Exception as e:
            return await _json(request, {"error": f"yt-dlp failed to start: {e}"},
                         500)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=120)
        except asyncio.TimeoutError:
            proc.kill()
            return await _json(request, {"error": "reading that link took too long"},
                         504)
        if proc.returncode != 0:
            msg = (err or b"").decode("utf-8", "replace").strip().splitlines()
            _e = (msg[-1] if msg else "unreadable link")[:200]
            return await _json(request,
                         {"error": _e + _cookie_hint(_e),
                          "why": _explain_err(_e, u, "g")[0]}, 422)
        try:
            info = json.loads(out.decode("utf-8", "replace"))
        except Exception:
            return await _json(request, {"error": "unreadable metadata"}, 500)
        try:
            best_res = max(
                int(f.get("height") or 0)
                for f in (info.get("formats") or [])
                if (f.get("vcodec") or "none") != "none")
        except ValueError:
            best_res = 0
        fmts = []
        seen = set()
        for f in (info.get("formats") or []):
            vid = (f.get("vcodec") or "none") != "none"
            aud = (f.get("acodec") or "none") != "none"
            if not vid and not aud:
                continue
            fid = str(f.get("format_id", ""))
            ext = str(f.get("ext", ""))
            if vid:
                height = int(f.get("height") or 0)
                res = (f"{height}p" if height
                       else str(f.get("resolution") or "?"))
                label = f"{res} · {ext}"
                if f.get("fps"):
                    label += f" {f['fps']}fps"
                if not aud:
                    label += " (video only)"
            else:
                label = f"audio · {ext} " + (
                    f"{f.get('abr', '?')}k" if f.get("abr") else "").strip()
            size = f.get("filesize") or f.get("filesize_approx")
            key = (vid, label.split(" (")[0])
            if key in seen:
                continue
            seen.add(key)
            fmts.append({
                "id": fid, "vid": vid, "label": label,
                "size": _nice(size) if size else "",
                "note": f.get("format_note") or "",
                # for a video-only row the user's picked id needs +bestaudio
                "fmt": fid if (aud or not vid)
                else (f"{fid}+bestaudio/{fid}"),
                "best": bool(vid and aud and height and height == best_res),
            })
        vid_fmts = [f for f in fmts if f["vid"]]
        aud_fmts = [f for f in fmts if not f["vid"]]
        return await _json(request, {
            "ok": True, "url": url,
            "title": info.get("title") or "",
            "extractor": info.get("extractor_key") or "",
            "duration": info.get("duration_string") or "",
            "thumbnail": (info.get("thumbnail") or "") if str(
                info.get("thumbnail") or "").startswith("http") else "",
            "formats": (vid_fmts[:24] + aud_fmts[:6]),
        })

    if path.endswith("/api/admin/state"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            users = await _users_col().find({}).sort("seen", -1).to_list(200)
        except Exception:
            users = []
        out = []
        for u in users:
            _id = str(u.get("_id", ""))
            # v19.1.0: ONLY real guest devices. like:/rep:/inv:/att:/ip:
            # docs were being listed (and deleted) as if they were guests.
            if _id.startswith(("ip:", "u:", "like:", "rep:", "inv:",
                               "att:", "allow:", "block:", "blocknet:")):
                continue
            out.append({"dev": _id,
                        "name": str(u.get("gname", "") or ""),
                        "n": int(u.get("n", 0)),
                        "day": str(u.get("day", "")),
                        "banned": bool(u.get("banned")),
                        "limit_gb": u.get("limit_gb"),
                        "seen": int(u.get("seen", 0)),
                        "created": int(u.get("created", 0) or 0),
                        "fp": str(u.get("fp", "") or "")[:40],
                        "net": _net_key(str(u.get("ip", "") or "")),
                        "ip": str(u.get("ip", "") or "")[:45],
                        "ua": str(u.get("ua", "") or "")[:60],
                        "fails": int(u.get("fails", 0))})
        # v19.1.0: blocked signup attempts + one-time IP passes
        _att = []
        for u in users:
            if str(u.get("_id", "")).startswith("att:"):
                _att.append({"name": str(u.get("name", ""))[:60],
                             "kind": u.get("kind", ""),
                             "reason": u.get("reason", ""),
                             "ip": str(u.get("ip", "") or "")[:45],
                             "net": u.get("net", ""),
                             "ua": str(u.get("ua", "") or "")[:60],
                             "at": int(u.get("at", 0) or 0)})
        _att.sort(key=lambda x: -x["at"])
        _allows = [{"ip": str(u.get("_id", ""))[6:],
                    "at": int(u.get("at", 0) or 0)}
                   for u in users
                   if str(u.get("_id", "")).startswith("allow:")]
        # v19.4.0: blocked networks ("do not allow")
        _blocks = [{"net": str(u.get("_id", ""))[9:] or "—",
                    "ip": str(u.get("ip", "") or ""),
                    "why": str(u.get("why", "") or ""),
                    "at": int(u.get("at", 0) or 0)}
                   for u in users
                   if str(u.get("_id", "")).startswith("blocknet:")]
        _blocks += [{"net": "(ip) " + str(u.get("_id", ""))[6:],
                     "ip": str(u.get("_id", ""))[6:],
                     "why": str(u.get("why", "") or ""),
                     "at": int(u.get("at", 0) or 0)}
                    for u in users
                    if str(u.get("_id", "")).startswith("block:")]
        ips = [{"ip": str(u.get("_id", ""))[3:][:45],
                "n": int(u.get("n", 0))} for u in users
               if str(u.get("_id", "")).startswith("ip:")]
        ips.sort(key=lambda d: -d["n"])
        accts = []
        for u in users:
            _id = str(u.get("_id", ""))
            if not _id.startswith("u:"):
                continue
            accts.append({"name": _id[2:],
                          "role": u.get("role", "v"),
                          "n": int(u.get("n", 0)),
                          "banned": bool(u.get("banned")),
                          "dev": bool(u.get("dev")),
                          "seen": int(u.get("seen", 0)),
                          "ip": str(u.get("ip", "") or "")[:45],
                          "ua": str(u.get("ua", "") or "")[:40],
                          "bw": round(float(u.get("bw", 0) or 0), 1),
                          "daily": u.get("daily"),
                          "cap_gb": u.get("cap_gb"),
                          "from": str(u.get("created_from", "") or "")})
        _st = await _stats_doc()
        # v18.0.0: invites, history, disk files, today stats, pixeldrain
        _inv = []
        try:
            for d in await _users_col().find(
                    {"_id": {"$regex": "^inv:"}}).to_list(100):
                _inv.append({"code": d.get("code", ""),
                             "used": bool(d.get("used")),
                             "used_by": d.get("used_by", ""),
                             "created": int(d.get("created", 0))})
        except Exception:
            pass
        _disk = []
        for _tid, _t in _TASKS.items():
            _sz = 0
            try:
                _sz = _dir_bytes(os.path.join(_base_dir(), _tid))
            except Exception:
                pass
            if _sz:
                _disk.append({"id": _tid, "size": _nice(_sz),
                              "size_b": _sz, "file": _t.get("file", ""),
                              "status": _t.get("status", ""),
                              "t0": _t.get("t0", 0)})
        _disk.sort(key=lambda x: -x["size_b"])
        _day0 = _today_ist()
        _today_n = sum(1 for h in _HISTORY
                       if h.get("done_at")
                       and time.strftime("%Y%m%d",
                                         time.gmtime(h["done_at"] + 19800))
                       == _day0)
        _rows = await _hist_rows()
        _dbh = _rows[-50:][::-1]
        return await _json(request, {
            "ok": True,
            "history": [{"id": h.get("id", ""), "dev": h.get("dev", ""),
                         "title": h.get("title", ""), "url": h.get("url", ""),
                         "size": h.get("size", ""),
                         "status": h.get("status", ""),
                         "done_at": int(h.get("done_at", 0) or 0),
                         "dls": int(h.get("dls", 0) or 0)}
                        for h in _dbh],
            "invites": _inv,
            "disk": _disk[:60],
            "today_downloads": _today_n,
            "pixeldrain": {"on": bool(s.get("pd_key")),
                           "low_gb": s.get("pd_low_gb", 2),
                           "high_gb": s.get("pd_high_gb", 4)},
            "slots": {"member": int(s.get("member_slots", 2)),
                      "guest": int(s.get("guest_slots", 2)),
                      "running": {"member": _running_pool("v"),
                                  "guest": _running_pool("g")}},
            "music": bool(s.get("music")),
            "quick_mode": bool(s.get("quick_mode")),
            "notify_done": bool(s.get("notify_done")),
            "tg": {"logs": _TG_LAST.get("logs", ""),
                   "files": _TG_LAST.get("files", ""),
                   "err": _TG_LAST.get("err", ""),
                   "logs_set": str(s.get("tg_logs_chat", "") or ""),
                   "files_set": str(s.get("tg_files_chat", "") or "")},
            "default_theme": s.get("default_theme", "midnight"),
            "maintenance": bool(s.get("maintenance")),
            "announcement": s.get("announcement", ""),
            "theme": s.get("theme", "#7c5cff"),
            "globals": {"ttl": s["ttl"], "max_gb": s["max_gb"],
                        "conc": int(s["conc"]),
                        "user_max_gb": s["user_max_gb"],
                        "user_daily": int(s["user_daily"]),
                        "v_max_gb": s.get("v_max_gb", 4),
                        "v_daily": int(s.get("v_daily", 10)),
                        "s_max_gb": s.get("s_max_gb", 10),
                        "s_daily": int(s.get("s_daily", 30)),
                        "bw_global_mb": s.get("bw_global_mb", 0),
                        "bw_user_mb": s.get("bw_user_mb", 0)},
            "users": out,
            "accounts": accts,
            "attempts": _att[:60],
            "allows": _allows,
            "blocks": _blocks,
            "quick": [{"name": a["name"], "n": a["n"],
                       "banned": a["banned"], "ip": a["ip"],
                       "bound": a["dev"], "bw": a["bw"],
                       "seen": a["seen"], "daily": a.get("daily"),
                       "cap_gb": a.get("cap_gb")}
                      for a in accts if a["role"] == "q"],
            "ips": ips[:50],
            "likes": int(_st.get("likes", 0)),
            "dl_total": int(_st.get("dl_total", 0)),
            "site": {"name": s.get("site_name", ""),
                     "contact": s.get("owner_contact", ""),
                     "guests": bool(s.get("guest_mode")),
                     "announcement": s.get("announcement", ""),
                     "theme": s.get("theme", "#7c5cff")},
            "yt_cookies": bool(s.get("yt_cookies")),
            "events": _EVENTS[-100:],
        })

    if path.endswith("/api/admin/globals"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        upd = {}
        for k, lo, hi in (("ttl", 1, 48), ("max_gb", 1, 40),
                          ("conc", 1, 20), ("user_max_gb", 0.1, 40),
                          ("user_daily", 1, 100),
                          ("v_max_gb", 0.1, 40), ("s_max_gb", 0.1, 40),
                          ("bw_global_mb", 0, 500000),
                          ("bw_user_mb", 0, 500000),
                          ("member_slots", 1, 20), ("guest_slots", 1, 20),
                          ("v_daily", 1, 500), ("s_daily", 1, 500)):
            if k in body:
                try:
                    v = float(body[k])
                except (TypeError, ValueError):
                    continue
                if lo <= v <= hi:
                    upd[k] = int(v) if k in ("conc", "user_daily", "v_daily",
                                             "s_daily", "member_slots",
                                             "guest_slots") else v
        if upd:
            try:
                col = _db().wzfix_config[_part()]
                await col.update_one({"_id": "webdl"}, {"$set": upd},
                                     upsert=True)
            except Exception:
                pass
            global _SETTINGS_CACHE
            _SETTINGS_CACHE = (0, None)
            _evt(f"admin globals: {upd}")
        return await _json(request, {"ok": True, "updated": upd})

    if path.endswith("/api/admin/user"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        upd = {}
        if body.get("user"):
            acc, name = await _acct(body.get("user"))
            if not acc:
                return await _json(request, {"error": "no such user"}, 404)
            if body.get("del"):
                try:
                    await _users_col().delete_one({"_id": f"u:{name}"})
                except Exception:
                    pass
                _BAN_CACHE.pop(f"u:{name}", None)
                asyncio.get_event_loop().create_task(_audit(
                    "account REMOVED", name, request))
                return await _json(request, {"ok": True})
            if "banned" in body:
                upd["banned"] = bool(body["banned"])
            if body.get("reset_dev"):
                upd["dev"] = ""
                upd["fails"] = 0
            # v18.0.0: per-member daily / size overrides (null = default)
            if "daily" in body:
                try:
                    v = int(body["daily"])
                    upd["daily"] = v if 1 <= v <= 500 else None
                except (TypeError, ValueError):
                    upd["daily"] = None
            if "cap_gb" in body:
                try:
                    v = float(body["cap_gb"])
                    upd["cap_gb"] = v if 0.1 <= v <= 40 else None
                except (TypeError, ValueError):
                    upd["cap_gb"] = None
            if upd:
                await _acct_update(name, **upd)
                _BAN_CACHE.pop(f"u:{name}", None)
                _evt(f"admin account {name}: {upd}")
            return await _json(request, {"ok": True})
        d = re.sub(r"[^a-zA-Z0-9_-]", "", str(body.get("dev", "")))[:64]
        if not d:
            return await _json(request, {"error": "dev missing"}, 400)
        if body.get("del"):  # v18.0.0: remove a guest device
            try:
                await _users_col().delete_one({"_id": d})
            except Exception:
                pass
            _BAN_CACHE.pop(d, None)
            asyncio.get_event_loop().create_task(_audit(
                "guest device REMOVED", d[:16]))
            return await _json(request, {"ok": True})
        if "banned" in body:
            upd["banned"] = bool(body["banned"])
        if "limit_gb" in body:
            try:
                v = float(body["limit_gb"])
                upd["limit_gb"] = v if 0.1 <= v <= 40 else None
            except (TypeError, ValueError):
                upd["limit_gb"] = None
        if upd:
            await _user_update(d, **upd)
            _evt(f"admin user {d[:10]}: {upd}")
        return await _json(request, {"ok": True})

    if path.endswith("/api/admin/unban_all"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        n = 0
        try:
            r1 = await _users_col().update_many(
                {"banned": True}, {"$set": {"banned": False, "fails": 0}})
            n = int(getattr(r1, "modified_count", 0) or 0)
        except Exception:
            try:
                # some stacks lack modified_count - count matched instead
                n = sum(1 for u in (await _users_col().find(
                    {"banned": True}).to_list(500))
                    if (await _user_update(
                        str(u.get("_id", "")), banned=False, fails=0)
                        is None))
            except Exception:
                n = 0
        _FAILS.clear()  # drop any IP lockouts too
        _BAN_CACHE.clear()
        asyncio.get_event_loop().create_task(_audit(
            "admin UNBAN ALL", f"{n} users unbanned",
            ip=_client_ip(request), ua=_ua_short(request)))
        return await _json(request, {"ok": True, "count": n})

    if path.endswith("/api/start"):
        try:
            body = await request.json()
        except Exception:
            body = {}
        url = str(body.get("url", "")).strip()
        mode = "audio" if body.get("mode") == "audio" else "video"
        fmt = str(body.get("fmt", "") or "").strip()[:120]
        subs = bool(body.get("subs"))
        name = re.sub(r"[^\w\-. ()\[\]]", "",
                      str(body.get("name", "")))[:100].strip()
        if not _url_ok(url):
            return await _json(request, {"error": "give me a link - any "
                                   "website, direct file, or magnet"}, 400)
        if s.get("maintenance") and role != "o":
            return await _json(request, {"error": "the site is under "
                                   "maintenance - back soon"}, 503)
        # v19.0.0: per-role slot pools (owner = unlimited)
        _myslots = _slots_for(s, role)
        if role != "o" and _running_pool(role) >= _myslots:
            return await _json(request, {
                "error": f"all {_myslots} {_ROLE_SUB.get(role, 'user')} "
                         f"slot{'s' if _myslots != 1 else ''} busy — wait "
                         "for one to finish"}, 429)
        if role != "o" and _dir_bytes(_base_dir()) > s["max_gb"] * (1 << 30):
            return await _json(request, {"error": "disk budget full — clear a "
                                            "finished file first"}, 507)
        tcap = None
        if role in ("v", "s"):
            acc, name = await _acct(dev[2:] if dev.startswith("u:") else dev)
            if not acc:
                return await _json(request,
                                   {"error": "account not found - sign "
                                    "in again"}, 401)
            if acc.get("banned"):
                return await _json(request,
                                   {"error": "this account is banned by "
                                    "the owner"}, 403)
            _dly, _mgb = _role_limits(s, role)
            if acc.get("daily"):  # v18.0.0: per-member override
                _dly = int(acc["daily"])
            if acc.get("cap_gb"):
                _mgb = float(acc["cap_gb"])
            if int(acc.get("n", 0)) >= _dly:
                return await _json(request,
                                   {"error": f"daily limit reached "
                                   f"({_dly} downloads/day)"}, 429)
            if s.get("bw_user_mb"):
                try:
                    _ubw = float(acc.get("bw", 0) or 0)
                    if _ubw >= float(s["bw_user_mb"]):
                        return await _json(
                            request, {"error": "daily bandwidth limit "
                                       "reached - try again tomorrow"}, 429)
                except (TypeError, ValueError):
                    pass
            await _acct_update(name, seen=int(time.time()),
                               ip=_client_ip(request))
            tcap = _mgb
        if role == "g":
            u = await _user_doc(dev)
            if u.get("banned"):
                return await _json(request, {"error": "this device is "
                                   "banned by the owner"}, 403)
            if int(u.get("n", 0)) >= int(s["user_daily"]):
                return await _json(request, {"error":
                                   f"daily limit reached "
                                   f"({int(s['user_daily'])} downloads/day)"},
                                   429)
            # per-IP cap: clearing site data does NOT reset the count
            ip = _client_ip(request)
            ipd = await _ip_doc(ip)
            if int(ipd.get("n", 0)) >= int(s["user_daily"]) * 2:
                return await _json(request, {"error":
                                   "daily limit reached for this network"},
                                   429)
            await _user_update(dev, day=_today_ist(),
                               seen=int(time.time()), ip=ip)
            try:
                tcap = float(u.get("limit_gb") or s["user_max_gb"])
            except (TypeError, ValueError):
                tcap = s["user_max_gb"]
        tid = token_hex(6)
        t = {"id": tid, "url": url, "mode": mode, "fmt": fmt,
             "subs": subs, "name": name,
             "status": "queued", "pct": 0.0, "size": "", "speed": "",
             "eta": "", "file": "", "err": "", "t0": time.time(),
             "done_at": 0, "title": url[:80],
             "role": role, "dev": dev, "cap": tcap,
             "ip": (_client_ip(request) if role == "g" else ""),
             "ua": (_ua_short(request) if role == "g" else "")}
        _TASKS[tid] = t
        asyncio.get_event_loop().create_task(_run_task(t))
        _evt(f"start-api {tid[:8]} role={role}")
        return await _json(request, {"ok": True, "id": tid})

    if path.endswith("/api/tg"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        t = _TASKS.get(str(body.get("id", "")))
        if not t or t["status"] != "done" or not t.get("path"):
            return await _json(request, {"error": "no finished file"}, 404)
        if not os.path.isfile(t["path"]):
            return await _json(request, {"error": "expired"}, 404)
        if os.path.getsize(t["path"]) > _TG_MAX:
            return await _json(request, {"error": "too big for Telegram "
                                        "(over ~1.9 GB)"}, 413)
        if t.get("tg") != "sending":
            t["tg"] = "sending"
            asyncio.get_event_loop().create_task(_send_tg(t))
        return await _json(request, {"ok": True, "tg": t.get("tg", "")})

    if path.endswith("/api/cancel"):
        try:
            body = await request.json()
        except Exception:
            body = {}
        t = _TASKS.get(str(body.get("id", "")))
        if not t:
            return await _json(request, {"error": "no such task"}, 404)
        if role != "o" and t.get("dev") not in (None, "", dev):
            return await _json(request, {"error": "not yours"}, 403)
        if t["status"] in ("downloading", "processing", "queued"):
            t["status"] = "cancel"
            p = t.get("proc")
            if p:
                try:
                    p.terminate()
                except Exception:
                    pass
            return await _json(request, {"ok": True, "msg": "cancelling"})
        return await _json(request, {"ok": True, "msg": t["status"]})

    if path.endswith("/api/clear"):
        try:
            body = await request.json()
        except Exception:
            body = {}
        t = _TASKS.pop(str(body.get("id", "")), None)
        if not t:
            return await _json(request, {"error": "no such task"}, 404)
        rmtree(os.path.join(_base_dir(), t["id"]), ignore_errors=True)
        return await _json(request, {"ok": True})

    # ---------------- v18.0.0 user endpoints ----------------

    # own download history (re-download links while files live)
    if path.endswith("/api/history"):
        # v19.3.0: read from the database first (in-memory is only a cache,
        # a kernel restart used to wipe everyone's history)
        rows = await _hist_rows()
        mine_h = [h for h in rows
                  if role == "o" or h.get("dev") in (None, "", dev)]
        out_h = []
        for h in mine_h[-40:][::-1]:
            out_h.append({
                "id": h.get("id", ""), "title": h.get("title", ""),
                "url": h.get("url", ""), "file": h.get("file", ""),
                "status": h.get("status", ""), "size": h.get("size", ""),
                "done_at": int(h.get("done_at", 0) or 0),
                "dls": int(h.get("dls", 0) or 0),
                "alive": bool(h.get("alive"))})
        return await _json(request, {"ok": True, "history": out_h})

    # change my own password (named accounts)
    if path.endswith("/api/pass"):
        if role not in ("v", "s"):
            return await _json(request,
                               {"error": "accounts only - guests have no "
                                "password"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        oldp = str(body.get("old", ""))
        newp = str(body.get("new", ""))
        if len(newp) < 4:
            return await _json(request, {"error": "new password must be "
                                   "at least 4 characters"}, 400)
        acc, name = await _acct(dev[2:])
        if not acc:
            return await _json(request, {"error": "account not found"}, 404)
        if not compare_digest(acc.get("pass_h", ""),
                              _hash_pass(oldp, acc.get("salt", ""))):
            _ip_fail(_client_ip(request))
            return await _json(request, {"error": "wrong current "
                                   "password"}, 401)
        await _acct_update(name, pass_h=_hash_pass(newp, acc.get("salt", "")))
        asyncio.get_event_loop().create_task(_audit_req(
            "password CHANGED", name, request, dev))
        return await _json(request, {"ok": True})

    # report a problem -> owner's Telegram
    if path.endswith("/api/report"):
        if not _ua_ok(request):
            return await _json(request, {"error": "forbidden"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        msg = str(body.get("msg", "")).strip()[:500]
        if len(msg) < 3:
            return await _json(request, {"error": "tell me a bit more"},
                               400)
        fpd = _fp_dev(body.get("fp")) or dev or "anon"
        try:
            seen = await _users_col().find_one({"_id": f"rep:{fpd}"})
        except Exception:
            seen = None
        if seen and time.time() - int(seen.get("at", 0)) < 3600:
            return await _json(request, {"error": "already sent - wait "
                                   "an hour between reports"}, 429)
        try:
            await _users_col().update_one(
                {"_id": f"rep:{fpd}"},
                {"$set": {"at": int(time.time())}}, upsert=True)
        except Exception:
            pass
        asyncio.get_event_loop().create_task(_audit_req(
            "user REPORT", msg[:200], request, dev))
        asyncio.get_event_loop().create_task(_notify(
            f"📢 <b>WZFIX webdl report</b>\n┏ from: "
            f"<code>{_ROLE_SUB.get(role, '?')}"
            f"{' ' + dev[2:] if dev.startswith('u:') else ''}</code>\n"
            f"┗ {msg[:400]}"))
        return await _json(request, {"ok": True})

    # ---------------- v18.0.0 owner endpoints ----------------

    # site options: name, contact, theme, announcement, maintenance, guests
    if path.endswith("/api/admin/site"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        upd = {}
        if "site_name" in body:
            _sn = str(body["site_name"]).strip()[:60]
            if _sn:
                upd["site_name"] = _sn
        if "owner_contact" in body:
            upd["owner_contact"] = str(
                body["owner_contact"]).strip().lstrip("@")[:60]
        if "theme" in body:
            _tm = str(body["theme"]).strip()
            if re.fullmatch(r"#[0-9a-fA-F]{6}", _tm):
                upd["theme"] = _tm
        if "announcement" in body:
            upd["announcement"] = str(body["announcement"])[:300]
        if "maintenance" in body:
            upd["maintenance"] = bool(body["maintenance"])
        if "guest_mode" in body:
            upd["guest_mode"] = bool(body["guest_mode"])
        if "member_slots" in body or "guest_slots" in body:
            for k in ("member_slots", "guest_slots"):
                if k in body:
                    try:
                        v = int(float(body[k]))
                        if 1 <= v <= 20:
                            upd[k] = v
                    except (TypeError, ValueError):
                        pass
        if "default_theme" in body:
            _dt = str(body["default_theme"]).strip().lower()
            if _dt in _THEMES:
                upd["default_theme"] = _dt
        if "music" in body:
            upd["music"] = bool(body["music"])
        if "quick_mode" in body:
            upd["quick_mode"] = bool(body["quick_mode"])
        if "notify_done" in body:
            upd["notify_done"] = bool(body["notify_done"])
        if "pd_low_gb" in body or "pd_high_gb" in body:
            for k in ("pd_low_gb", "pd_high_gb"):
                if k in body:
                    try:
                        v = float(body[k])
                        if 0.1 <= v <= 40:
                            upd[k] = v
                    except (TypeError, ValueError):
                        pass
        if upd:
            try:
                col = _db().wzfix_config[_part()]
                await col.update_one({"_id": "webdl"}, {"$set": upd},
                                     upsert=True)
            except Exception:
                pass
            _SETTINGS_CACHE = (0, None)
            _evt(f"admin site: { {k: v for k, v in upd.items() if k != 'pd_key'} }")
        return await _json(request, {"ok": True, "updated": upd})

    # invite codes: batch-generate / delete / list
    if path.endswith("/api/admin/invites"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        made = []
        n = 0
        try:
            n = int(body.get("make", 0))
        except (TypeError, ValueError):
            n = 0
        if 1 <= n <= 20:
            for _ in range(n):
                code = "WZ-" + token_hex(4).upper()
                ih = sha256(code.encode()).hexdigest()[:40]
                await _users_col().insert_one(
                    {"_id": f"inv:{ih}", "code": code, "used": False,
                     "created": int(time.time())})
                made.append(code)
            _evt(f"admin invites made: {n}")
        if body.get("del"):
            # delete by stored code
            try:
                _c = str(body["del"]).strip()
                ih = sha256(_c.encode()).hexdigest()[:40]
                await _users_col().delete_one({"_id": f"inv:{ih}"})
                _evt("admin invite deleted")
            except Exception:
                pass
        return await _json(request, {"ok": True, "codes": made})

    # cookie health check
    if path.endswith("/api/admin/cookie_test"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        s2 = await _settings(force=True)
        if not s2.get("yt_cookies"):
            return await _json(request, {"ok": False,
                                         "error": "no cookies set "
                                         "(owner: /ws YT_COOKIES)"})
        _ck = await _cookie_file()
        tcmd = [sys.executable, "-m", "yt_dlp", "-J", "--no-warnings",
                "--skip-download", "--cookies", _ck,
                "https://www.youtube.com/watch?v=aqz-KE-bpKQ"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *tcmd, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            out, err = await asyncio.wait_for(proc.communicate(), timeout=90)
        except asyncio.TimeoutError:
            return await _json(request, {"ok": False,
                                         "error": "timed out (90 s)"})
        if proc.returncode != 0:
            _e = (out or err or b"").decode("utf-8", "replace")
            _l = [x for x in _e.splitlines() if x.strip()]
            asyncio.get_event_loop().create_task(_audit(
                "cookie TEST FAILED", (_l[-1] if _l else "")[:150]))
            return await _json(request, {"ok": False,
                                         "error": (_l[-1] if _l
                                                   else "unknown")[:200]})
        asyncio.get_event_loop().create_task(_audit(
            "cookie TEST PASSED", "YouTube metadata read OK"))
        return await _json(request, {"ok": True,
                                     "msg": "cookies work — YouTube "
                                     "metadata read fine"})

    # v19.0.0: put a password on a finished file (owner or its owner)
    if path.endswith("/api/protect"):
        try:
            body = await request.json()
        except Exception:
            body = {}
        t = _TASKS.get(str(body.get("id", "")))
        if not t:
            return await _json(request, {"error": "no such task"}, 404)
        if role != "o" and t.get("dev") not in (None, "", dev):
            return await _json(request, {"error": "not yours"}, 403)
        pw = str(body.get("pw", ""))
        if pw and len(pw) < 4:
            return await _json(request, {"error": "password needs 4+ "
                                   "characters"}, 400)
        if pw:
            _salt = token_hex(8)
            t["pw_salt"] = _salt
            t["pw_hash"] = _hash_pass(pw, _salt)
        else:
            t.pop("pw_hash", None)
            t.pop("pw_salt", None)
        _evt(f"{'lock' if pw else 'unlock'} {t['id'][:8]}")
        return await _json(request, {"ok": True, "locked": bool(pw)})

    # disk manager: delete one task's files / wipe all finished
    if path.endswith("/api/admin/disk"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        freed = 0
        if body.get("id"):
            t = _TASKS.get(str(body["id"]))
            if t:
                _p = t.get("path") or ""
                try:
                    freed = os.path.getsize(_p) if os.path.isfile(_p) else 0
                except Exception:
                    freed = 0
                t["status"] = "cleared"
                t["path"] = ""
                rmtree(os.path.join(_base_dir(), t["id"]),
                       ignore_errors=True)
                _evt(f"admin disk delete {t['id'][:8]}")
        elif body.get("wipe"):
            for t in list(_TASKS.values()):
                if t.get("status") in ("done", "error", "cancel"):
                    _p = t.get("path") or ""
                    try:
                        freed += os.path.getsize(_p) if os.path.isfile(
                            _p) else 0
                    except Exception:
                        pass
                    t["status"] = "cleared"
                    t["path"] = ""
                    rmtree(os.path.join(_base_dir(), t["id"]),
                           ignore_errors=True)
            _evt("admin disk wipe finished files")
        return await _json(request, {"ok": True,
                                      "freed_mb": round(freed / (1 << 20), 1)})

    # reload settings caches (no restart needed)
    if path.endswith("/api/admin/reload"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        _SETTINGS_CACHE = (0, None)
        _BAN_CACHE.clear()
        _FAILS.clear()
        _evt("admin reload (caches cleared)")
        asyncio.get_event_loop().create_task(_audit(
            "admin RELOAD", "settings + ban caches cleared"))
        return await _json(request, {"ok": True})

    # v19.2.0: owner creates a quick account to hand to someone
    if path.endswith("/api/admin/quick"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        name, pw = _gen_creds()
        for _try in range(5):
            if not (await _acct(name))[0]:
                break
            name, pw = _gen_creds()
        doc = _new_acct(name, pw, "q")  # unbound: binds on their first login
        doc.update({"created_from": "owner panel",
                    "created_at": int(time.time())})
        try:
            await _users_col().insert_one(doc)
        except Exception:
            return await _json(request, {"error": "could not create"}, 500)
        asyncio.get_event_loop().create_task(_stats_inc("users", 1))
        asyncio.get_event_loop().create_task(_audit(
            "owner made a quick account", name))
        asyncio.get_event_loop().create_task(_notify(
            f"⚡ <b>quick account (owner)</b>\n┣ user: <code>{name}</code>\n"
            f"┗ guest limits · binds to the first device that signs in"))
        return await _json(request, {"ok": True, "user": name, "pass": pw,
                                     "role": "q",
                                     "note": "hand these to the person — the "
                                             "account binds to their device "
                                             "on first sign-in"})

    # v19.1.0: one-time IP pass (blocked signups / guest networks)
    if path.endswith("/api/admin/allow"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        _ip = str(body.get("ip", "")).strip()[:45]
        if body.get("clear"):
            _n = 0
            try:
                for d in await _users_col().find(
                        {"_id": {"$regex": "^att:"}}).to_list(300):
                    await _users_col().delete_one({"_id": d["_id"]})
                    _n += 1
            except Exception:
                pass
            _evt(f"admin cleared {_n} attempts")
            return await _json(request, {"ok": True, "cleared": _n})
        # v19.4.0: "do not allow" - block the ip and its whole network
        if body.get("block"):
            _net = _net_key(_ip)
            _why = str(body.get("why", "") or "")[:80]
            for _key in ({f"block:{_ip}"} | ({f"blocknet:{_net}"} if _net
                                             else set())):
                try:
                    await _users_col().insert_one(
                        {"_id": _key, "at": int(time.time()),
                         "why": _why, "ip": _ip})
                except Exception:
                    pass
            _evt(f"admin BLOCKED {_ip} (net {_net})")
            asyncio.get_event_loop().create_task(_audit(
                "admin BLOCK (do not allow)", f"{_ip} / {_net}"))
            return await _json(request, {"ok": True, "blocked": _ip,
                                         "net": _net})
        if body.get("unblock"):
            _net = str(body.get("net", "") or _net_key(_ip)).strip()[:45]
            _gone = 0
            try:
                for _key in (f"blocknet:{_net}", f"block:{_ip}"):
                    _r = await _users_col().delete_one({"_id": _key})
                    _gone += int(getattr(_r, "deleted_count", 0) or 0)
            except Exception:
                pass
            _evt(f"admin UNBLOCKED {_net}")
            return await _json(request, {"ok": True, "removed": _gone})
        if body.get("del"):
            try:
                await _users_col().delete_one({"_id": f"allow:{_ip}"})
            except Exception:
                pass
            return await _json(request, {"ok": True})
        if not _ip:
            return await _json(request, {"error": "ip missing"}, 400)
        try:
            await _users_col().insert_one({"_id": f"allow:{_ip}",
                                           "at": int(time.time())})
        except Exception:
            pass
        _evt(f"admin one-time pass for {_ip}")
        asyncio.get_event_loop().create_task(_audit(
            "admin ALLOW IP once", _ip))
        return await _json(request, {"ok": True})

    # v19.0.0: telegram self-test - why is nothing arriving?
    if path.endswith("/api/admin/tg_test"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        out = {"ok": True, "logs": {}, "files": {}}
        try:
            from ...core.tg_client import TgClient
        except Exception as e:
            return await _json(request, {"ok": False,
                                         "error": f"bot client "
                                         f"unavailable: {e}"})
        for which, pref in (("logs", "tg_logs_chat"),
                            ("files", "tg_files_chat")):
            _c = _norm_chat(await _chat_id(pref))
            if not _c:
                out[which] = {"chat": "", "error":
                              "not set - send the group id via /ws"}
                continue
            try:
                await TgClient.bot.send_message(
                    chat_id=_c, text=(f"✅ webdl test message ({which} group)"
                                      "\nif you can read this, routing works."))
                out[which] = {"chat": str(_c), "error": ""}
            except Exception as e:
                out[which] = {"chat": str(_c),
                              "error": f"{e.__class__.__name__}: "
                                       f"{str(e)[:160]}"}
        return await _json(request, out)

    # change owner password from the panel
    if path.endswith("/api/admin/pass"):
        if role != "o":
            return await _json(request, {"error": "owner only"}, 403)
        try:
            body = await request.json()
        except Exception:
            body = {}
        newp = str(body.get("new", ""))
        if len(newp) < 4:
            return await _json(request, {"error": "at least 4 "
                                   "characters"}, 400)
        await set_webdl_pass(newp)
        asyncio.get_event_loop().create_task(_audit(
            "owner password CHANGED (panel)"))
        return await _json(request, {"ok": True})

    return await _json(request, {"error": "not found"}, 404)


def webdl_routes(app):
    """Register the web downloader on the bot stream server (r17)."""
    app.router.add_route("GET", "/webdl", webdl_page)
    app.router.add_route("*", "/webdl/{path:.*}", webdl_api)
