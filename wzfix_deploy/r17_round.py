#!/usr/bin/env python3
"""WZFIX R19 payload (v19.40) — web downloader.

Fetches r17_webdl.py from Drive (sha256-pinned), writes it into
bot/helper/wzfix/, registers the /webdl routes on the bot stream
server (same /_dl anchor the wzadmin round uses) and adds the wserver
/webdl proxy with true streaming for file downloads (the buffered
/wzadmin proxy pattern would OOM on multi-GB files and its default
300 s total timeout would cut off long phone downloads).

Remote payload pattern (same as _real_deploy_patch.py) because the
Kaggle notebook kernel source must stay under 1 MB. Idempotent: every
edit is marker-checked; on any failure exits 1 and the notebook logs
it and keeps booting (webdl is skipped, the bot is unaffected).
"""
import hashlib
import os
import subprocess
import sys
import urllib.request

WS_SRC = r'''# WZFIX R19 - /ws website settings (owner only).
# Panel for every web-downloader knob, incl. MediaFire credentials.
import os
from time import time

from pyrogram.filters import create

from ..core.config_manager import Config
from ..helper.ext_utils.bot_utils import new_task
from ..helper.telegram_helper.button_build import ButtonMaker
from ..helper.telegram_helper.message_utils import (
    delete_message,
    edit_message,
    send_message,
)

# Where the public guest site (GitHub Pages) lives. The site itself
# contains NO worker URL: it fetches backend.json at runtime, and this
# panel is what (re)writes that file when a GitHub token is set.
_GH_OWNER = "vot1122"
_GH_REPO = "ytwebdownload"
_GH_FILE = "backend.json"

WS_KEYS = [
    ("WEBDL_PASS", "Owner password (website)"),
    ("WEBDL_TTL", "Files auto-delete after (hours)"),
    ("WEBDL_MAX_GB", "Disk budget (GB)"),
    ("WEBDL_CONC", "Parallel download slots"),
    ("USER_MAX_GB", "Guest max file size (GB)"),
    ("USER_DAILY", "Guest downloads per day"),
    ("PD_KEY", "Pixeldrain API key (big-file route)"),
    ("PD_LOW", "Cloud upload from (GB)"),
    ("PD_HIGH", "Cloud upload up to (GB)"),
    ("R2_ENDPOINT", "R2 endpoint (S3 URL)"),
    ("R2_BUCKET", "R2 bucket name"),
    ("R2_KEY_ID", "R2 access key id"),
    ("R2_SECRET", "R2 secret access key"),
    ("R2_PUBLIC", "R2 public base (r2.dev)"),
    ("R2_LOW", "R2 upload from (GB)"),
    ("R2_HIGH", "R2 upload up to (GB)"),
    ("MEMBER_SLOTS", "Member parallel slots"),
    ("GUEST_SLOTS", "Guest parallel slots"),
    ("DEFAULT_THEME", "Default site theme"),
    ("MUSIC", "Ambient music (yes/no)"),
    ("NOTIFY_DONE", "Telegram ping on finish (yes/no)"),
    ("YT_COOKIES", "YouTube cookies (login-locked videos)"),
    ("TG_FILES_CHAT", "Telegram files group"),
    ("TG_LOGS_CHAT", "Telegram logs group"),
    ("SITE_BACKEND", "Website backend (worker URL)"),
    ("GH_TOKEN", "GitHub token (website auto-publish)"),
    ("SITE_NAME", "Site name"),
    ("OWNER_CONTACT", "Owner contact (Telegram username)"),
    ("GUEST_MODE", "Guest downloads (yes/no)"),
    ("V_MAX_GB", "Member max file size (GB)"),
    ("V_DAILY", "Member downloads per day"),
    ("S_MAX_GB", "Special max file size (GB)"),
    ("S_DAILY", "Special downloads per day"),
    ("BW_GLOBAL_MB", "Global bandwidth cap (MB/day)"),
    ("BW_USER_MB", "User bandwidth cap (MB/day)"),
    ("ANNOUNCE", "Site announcement (banner)"),
    ("THEME", "Theme color (#hex)"),
    ("MAINTENANCE", "Maintenance mode (yes/no)"),
    ("QUICK_MODE", "Quick accounts (yes/no)"),
    ("ADD_USER", "Add member (send: name password)"),
    ("INVITE_CODE", "Invite code (special account)"),
    ("DEL_USER", "Remove member"),
    ("RESET_DEV", "Reset member device"),
    ("BAN_USER", "Ban/unban member (send: name)"),
    ("UNBAN_ALL", "Unban ALL banned users (send yes)"),
    ("SET_LIMIT", "Member limits (send: name gb daily)"),
    ("INVITE_BATCH", "Make invite codes (send: count)"),
    ("INVITE_LIST", "Show invite codes"),
    ("COOKIE_TEST", "Test YouTube cookies"),
    ("CLEANUP", "Remove guests unseen (send: days)"),
    ("STATS", "Website stats"),
    ("DISK_WIPE", "Delete ALL finished files"),
    ("RELOAD", "Reload website settings"),
    ("TG_TEST", "Test Telegram groups"),
    ("ATTEMPTS", "Blocked signup attempts"),
    ("ALLOW_IP", "Allow an IP once (send: ip)"),
    ("BLOCK_IP", "Block an IP/network (send: ip)"),
    ("UNBLOCK_IP", "Unblock (send: ip or network)"),
    ("ALLOW_LIST", "One-time passes"),
    ("QUICK_ACCT", "Make a quick account"),
]
_DB_FIELDS = {"WEBDL_PASS": "pass", "WEBDL_TTL": "ttl", "WEBDL_MAX_GB": "max_gb",
              "WEBDL_CONC": "conc", "USER_MAX_GB": "user_max_gb",
              "USER_DAILY": "user_daily", "PD_KEY": "pd_key",
              "PD_LOW": "pd_low_gb", "PD_HIGH": "pd_high_gb",
              "R2_ENDPOINT": "r2_endpoint", "R2_BUCKET": "r2_bucket",
              "R2_KEY_ID": "r2_key_id", "R2_SECRET": "r2_secret",
              "R2_PUBLIC": "r2_public", "R2_LOW": "r2_low_gb",
              "R2_HIGH": "r2_high_gb",
              "MAINTENANCE": "maintenance", "ANNOUNCE": "announcement",
              "THEME": "theme", "MEMBER_SLOTS": "member_slots",
              "GUEST_SLOTS": "guest_slots", "DEFAULT_THEME": "default_theme",
              "MUSIC": "music", "NOTIFY_DONE": "notify_done",
              "QUICK_MODE": "quick_mode",
              "YT_COOKIES": "yt_cookies", "TG_FILES_CHAT": "tg_files_chat",
              "TG_LOGS_CHAT": "tg_logs_chat", "SITE_BACKEND": "worker_url",
              "GH_TOKEN": "gh_token", "SITE_NAME": "site_name",
              "OWNER_CONTACT": "owner_contact", "GUEST_MODE": "guest_mode",
              "V_MAX_GB": "v_max_gb", "V_DAILY": "v_daily",
              "S_MAX_GB": "s_max_gb", "S_DAILY": "s_daily",
              "BW_GLOBAL_MB": "bw_global_mb", "BW_USER_MB": "bw_user_mb"}
_RANGES = {"WEBDL_TTL": (1, 48), "WEBDL_MAX_GB": (1, 40), "WEBDL_CONC": (1, 4),
           "USER_MAX_GB": (0.1, 40), "USER_DAILY": (1, 100),
           "V_MAX_GB": (0.1, 40), "V_DAILY": (1, 500),
           "S_MAX_GB": (0.1, 40), "S_DAILY": (1, 500),
           "BW_GLOBAL_MB": (0, 500000), "BW_USER_MB": (0, 500000),
           "PD_LOW": (0.1, 40), "PD_HIGH": (0.1, 40),
           "R2_LOW": (0.1, 40), "R2_HIGH": (0, 40),
           "MEMBER_SLOTS": (1, 20), "GUEST_SLOTS": (1, 20)}
# settings that are actions, not stored values
_ACTIONS = {"ADD_USER", "INVITE_CODE", "DEL_USER", "RESET_DEV",
              "BAN_USER", "UNBAN_ALL", "SET_LIMIT", "INVITE_BATCH",
              "INVITE_LIST", "COOKIE_TEST", "CLEANUP", "STATS",
              "DISK_WIPE", "RELOAD", "TG_TEST", "ATTEMPTS", "ALLOW_IP",
              "ALLOW_LIST", "QUICK_ACCT", "BLOCK_IP", "UNBLOCK_IP"}
_DEFAULTS = {"ttl": 6, "max_gb": 8, "conc": 2, "user_max_gb": 2, "user_daily": 3}
_PENDING = {}


async def _doc():
    from ..helper.wzfix.r1_core import _db, _part
    return await _db().wzfix_config[_part()].find_one({"_id": "webdl"}) or {}


def _current(key, doc):
    if key in _ACTIONS:
        return "tap to do it"
    f = _DB_FIELDS[key]
    v = doc.get(f)
    if key == "WEBDL_PASS":
        v = str(v) if v else "joshi"
        return v if len(v) <= 24 else v[:21] + "..."
    if key in ("PD_KEY", "GH_TOKEN"):
        return "set" if v else "not set"
    if key == "PD_LOW":
        return str(v if v is not None else 2)
    if key == "PD_HIGH":
        return str(v if v is not None else 4)
    if key == "MAINTENANCE":
        return "ON" if v else "OFF"
    if key == "QUICK_MODE":
        return "ON" if (v is None or v) else "OFF"
    if key == "ANNOUNCE":
        return (str(v)[:40] + "…") if v and len(str(v)) > 40 else (
            str(v) if v else "(none)")
    if key == "THEME":
        return str(v) if v else "#7c5cff"
    if key == "YT_COOKIES":
        return f"set ({len(str(v)) // 1024} KB)" if v else "not set"
    if key in ("TG_FILES_CHAT", "TG_LOGS_CHAT"):
        return str(v) if v else "bot LOG_CHAT"
    if key == "SITE_BACKEND":
        return str(v) if v else "not set (site config file)"
    if key == "SITE_NAME":
        return str(v) if v else "WZ Web Downloader"
    if key == "OWNER_CONTACT":
        return "@" + str(v).lstrip("@") if v else "not set"
    if key == "GUEST_MODE":
        return "ON" if v else "OFF"
    if key in ("V_MAX_GB", "V_DAILY", "S_MAX_GB", "S_DAILY",
               "BW_GLOBAL_MB", "BW_USER_MB"):
        if key in ("V_MAX_GB", "S_MAX_GB"):
            _d = {"V_MAX_GB": 4, "S_MAX_GB": 10}
        elif key in ("V_DAILY", "S_DAILY"):
            _d = {"V_DAILY": 10, "S_DAILY": 30}
        else:
            _d = {"BW_GLOBAL_MB": 0, "BW_USER_MB": 0}
        return str(v if v is not None else _d[key])
    if key in ("R2_ENDPOINT", "R2_KEY_ID", "R2_BUCKET", "R2_PUBLIC"):
        return str(v) if v else "not set"
    if key == "R2_SECRET":
        return "set" if v else "not set"
    if key in ("R2_LOW", "R2_HIGH"):
        return str(v if v is not None else (0.1 if key == "R2_LOW" else 0))
    if key in _ACTIONS:
        return "tap to do it"
    if v in (None, ""):
        v = _DEFAULTS.get(f, "")
    return str(v)


def _cloud_route(doc):
    """J-22b: which cloud delivery route is live right now."""
    if all(doc.get(k) for k in ("r2_endpoint", "r2_key_id", "r2_secret",
                                "r2_bucket", "r2_public")):
        return "Cloudflare R2"
    if doc.get("pd_key"):
        return "Pixeldrain"
    return "direct link (tunnel)"


# v19.0.0: the panel is grouped into sections (40+ items in one list was
# unusable). Every key must live in exactly one section.
SECTIONS = [
    ("site", "🎨 Site &amp; look", [
        "WEBDL_PASS", "SITE_NAME", "ANNOUNCE", "THEME", "DEFAULT_THEME",
        "MUSIC", "MAINTENANCE", "GUEST_MODE", "OWNER_CONTACT"]),
    ("users", "👤 Users", [
        "ADD_USER", "DEL_USER", "RESET_DEV", "BAN_USER", "UNBAN_ALL",
        "SET_LIMIT", "QUICK_ACCT", "V_MAX_GB", "V_DAILY", "S_MAX_GB",
        "S_DAILY"]),
    ("limits", "📊 Limits &amp; slots", [
        "WEBDL_TTL", "WEBDL_MAX_GB", "WEBDL_CONC", "USER_MAX_GB",
        "USER_DAILY", "MEMBER_SLOTS", "GUEST_SLOTS", "BW_GLOBAL_MB",
        "BW_USER_MB"]),
    ("cloud", "☁ Cloud &amp; backend", [
        "PD_KEY", "PD_LOW", "PD_HIGH", "R2_ENDPOINT", "R2_BUCKET",
        "R2_KEY_ID", "R2_SECRET", "R2_PUBLIC", "R2_LOW", "R2_HIGH",
        "SITE_BACKEND", "GH_TOKEN"]),
    ("telegram", "✈ Telegram", [
        "TG_FILES_CHAT", "TG_LOGS_CHAT", "NOTIFY_DONE", "TG_TEST"]),
    ("invites", "🎟 Invite codes", [
        "INVITE_CODE", "INVITE_BATCH", "INVITE_LIST"]),
    ("guard", "🛡 Guard", [
        "ATTEMPTS", "ALLOW_IP", "BLOCK_IP", "UNBLOCK_IP", "ALLOW_LIST"]),
    ("tools", "🔧 Tools", [
        "YT_COOKIES", "COOKIE_TEST", "STATS", "CLEANUP", "DISK_WIPE",
        "RELOAD"]),
]
_SEC_OF = {k: sid for sid, _lab, ks in SECTIONS for k in ks}


def _menu_buttons(doc):
    buttons = ButtonMaker()
    for sid, label, keys in SECTIONS:
        buttons.data_button(f"{label} ({len(keys)})", f"wzset sec {sid}")
    buttons.data_button("Close", "wzset close", position="footer")
    return buttons.build_menu(2)


def _sec_buttons(sid, doc):
    keys = dict((s[0], s[2]) for s in SECTIONS).get(sid, [])
    buttons = ButtonMaker()
    for k in keys:
        lab = dict(WS_KEYS).get(k, k)
        buttons.data_button(f"{lab}: {_current(k, doc)}", f"wzset key {k}")
    buttons.data_button("« Back", "wzset back", position="footer")
    buttons.data_button("Close", "wzset close", position="footer")
    return buttons.build_menu(1)


def _sec_text(sid, doc):
    rows = []
    keys = dict((s[0], s[2]) for s in SECTIONS).get(sid, [])
    label = dict((s[0], s[1]) for s in SECTIONS).get(sid, sid)
    for k in keys:
        rows.append(f"┠ <b>{dict(WS_KEYS).get(k, k)}</b> → "
                    f"<code>{_current(k, doc)}</code>")
    return (f"⌬ <b><u>{label}</u></b> — website settings\n│\n"
            + "\n".join(rows) + "\n┖ tap an item to change it")


def _menu_text(doc, note=""):
    rows = []
    for sid, label, keys in SECTIONS:
        rows.append(f"┠ {label} — {len(keys)} settings")
    return ("⌬ <b><u>Website Settings (/ws)</u></b>\n│\n"
            + "\n".join(rows)
            + "\n┖ pick a section to open it" + note
            + f"\n\n<i>Cloud route live: <b>{_cloud_route(doc)}</b></i>"
            + "\n\n<i>Everything here applies to the website instantly. "
              "The owner is never counted in slots or limits.</i>")


@new_task
async def ws_settings(client, message):
    user = message.from_user or message.sender_chat
    if not user or user.id != Config.OWNER_ID:
        return
    doc = await _doc()
    await send_message(message, _menu_text(doc), _menu_buttons(doc))


@new_task
async def ws_callback(client, query):
    data = query.data.split(maxsplit=1)
    arg = data[1] if len(data) > 1 else ""
    chat_id = query.message.chat.id
    uid = query.from_user.id
    if uid != Config.OWNER_ID:
        await query.answer("owner only", show_alert=True)
        return
    if arg == "close":
        _PENDING.pop((chat_id, uid), None)
        await query.answer()
        await delete_message(query.message.reply_to_message or query.message)
        await delete_message(query.message)
        return
    if arg == "back":
        _PENDING.pop((chat_id, uid), None)
        doc = await _doc()
        await query.answer()
        await edit_message(query.message, _menu_text(doc), _menu_buttons(doc))
        return
    # v19.0.0: sectioned menu ("sec <id>") + item ("key <KEY>")
    _parts = arg.split(maxsplit=1)
    if _parts and _parts[0] == "sec":
        _sid = _parts[1] if len(_parts) > 1 else ""
        if _sid not in dict((s[0], s[2]) for s in SECTIONS):
            await query.answer("unknown section", show_alert=True)
            return
        doc = await _doc()
        await query.answer()
        await edit_message(query.message, _sec_text(_sid, doc),
                           _sec_buttons(_sid, doc))
        return
    if _parts and _parts[0] == "key":
        arg = _parts[1] if len(_parts) > 1 else ""
    if arg not in _DB_FIELDS and arg not in _ACTIONS:
        await query.answer("unknown setting", show_alert=True)
        return
    await query.answer()
    _PENDING[(chat_id, uid)] = {"key": arg, "t": time(),
                                "mid": query.message.id}
    doc = await _doc()
    label = dict(WS_KEYS).get(arg, arg)
    hint = ""
    if arg == "SITE_BACKEND":
        hint = ("\n\n<i>The website reads its backend from a config file - "
                "no URL is stored in the site code. Send the new worker URL "
                "and it will be saved (and published to the site if a "
                "GitHub token is set).</i>")
    elif arg == "PD_KEY":
        hint = ("\n\n<i>pixeldrain.com → register a free account → "
                "User → API keys → copy one key. Big files then upload "
                "to your pixeldrain. Stored server-side only, your "
                "message is deleted right after.</i>")
    elif arg == "SET_LIMIT":
        hint = ("\n\n<i>Send: <code>name gb daily</code> — that member "
                "gets a custom file cap and daily count. Send "
                "<code>name 0 0</code> to put them back on defaults.</i>")
    elif arg == "INVITE_BATCH":
        hint = ("\n\n<i>Send how many (1-20) — I will list the "
                "one-time codes here. Each makes one special account.</i>")
    elif arg == "COOKIE_TEST":
        hint = ("\n\n<i>Tap, then send yes — I will try reading a "
                "YouTube link with the saved cookies and tell you if "
                "they work.</i>")
    elif arg == "CLEANUP":
        hint = ("\n\n<i>Send the number of days — guest devices not "
                "seen for that long get removed.</i>")
    elif arg == "MEMBER_SLOTS":
        hint = ("\n\n<i>How many downloads members can run at once "
                "(1-20). The owner is never counted in any slot.</i>")
    elif arg == "GUEST_SLOTS":
        hint = ("\n\n<i>How many downloads guests can run at once "
                "(1-20). Owner downloads never use guest slots.</i>")
    elif arg == "DEFAULT_THEME":
        hint = ("\n\n<i>midnight, ocean, sunset, forest or light — the "
                "theme new visitors see first.</i>")
    elif arg == "QUICK_MODE":
        hint = ("\n\n<i>yes/no — the 'create a quick account' button on "
                "the site. Quick accounts are real (generated user+password, "
                "guest limits, bound to one device) and can stay ON even "
                "when guests are OFF.</i>")
    elif arg == "MUSIC":
        hint = ("\n\n<i>yes/no — gentle ambient sound on the site "
                "(visitors can always switch it off).</i>")
    elif arg == "NOTIFY_DONE":
        hint = ("\n\n<i>yes/no — send a Telegram message every time a "
                "download finishes.</i>")
    elif arg == "QUICK_ACCT":
        hint = ("\n\n<i>Send anything — I generate a username + password "
                "for a guest-like account (guest limits, adjustable) and "
                "show them here.</i>")
    elif arg == "ATTEMPTS":
        hint = ("\n\n<i>Shows everyone who tried to sign up with a bad "
                "name — exactly what they typed, their ip and why it was "
                "refused.</i>")
    elif arg == "BLOCK_IP":
        hint = ("\n\n<i>Send an ip — I block it AND its whole network from "
                "signing up or using guests. Existing accounts can still "
                "sign in.</i>")
    elif arg == "UNBLOCK_IP":
        hint = ("\n\n<i>Send the ip (or network) you blocked — it is "
                "allowed again.</i>")
    elif arg == "ALLOW_IP":
        hint = ("\n\n<i>Send an ip address — that ip gets through the "
                "network / too-many-attempts block ONCE.</i>")
    elif arg == "TG_TEST":
        hint = ("\n\n<i>Send anything — I will try a test message to "
                "both groups and tell you the exact result.</i>")
    elif arg == "DISK_WIPE":
        hint = ("\n\n<i>Send yes — every FINISHED file on the disk "
                "gets deleted at once.</i>")
    elif arg == "GH_TOKEN":
        hint = ("\n\n<i>A fine-grained GitHub token with read+write access "
                "to the ytwebdownload repo lets this panel update the "
                "website by itself. It is stored server-side only and your "
                "message with the token is deleted right after.</i>")
    elif arg == "ADD_USER":
        hint = ("\n\n<i>Send it like: <code>name password</code> — the "
                "member signs in on the website with those. Passwords are "
                "stored hashed (PBKDF2), never in plain text.</i>")
    elif arg == "INVITE_CODE":
        hint = ("\n\n<i>I will generate a one-time code for a SPECIAL "
                "account (highest limits). The member uses it on the "
                "website once and picks their own name + password.</i>")
    elif arg == "DEL_USER":
        hint = ("\n\n<i>Send the member's name to delete the account.</i>")
    elif arg == "RESET_DEV":
        hint = ("\n\n<i>Send the member's name — their account unlocks "
                "from the old device and binds to the next one they sign "
                "in with.</i>")
    elif arg == "OWNER_CONTACT":
        hint = ("\n\n<i>Send your Telegram username (like @joshi). It "
                "shows on the website as the contact button.</i>")
    elif arg == "GUEST_MODE":
        hint = ("\n\n<i>Send yes or no. Guests = anonymous downloads "
                "with the small guest limits.</i>")
    elif arg == "BW_GLOBAL_MB":
        hint = ("\n\n<i>Total download traffic the site serves per day, "
                "in MB (0 = no cap).</i>")
    elif arg == "BW_USER_MB":
        hint = ("\n\n<i>Download traffic per user per day, in MB "
                "(0 = no cap).</i>")
    buttons = ButtonMaker()
    buttons.data_button("Back", "wzset back", position="footer")
    buttons.data_button("Close", "wzset close", position="footer")
    await edit_message(
        query.message,
        "⌬ <b>Website Settings</b>\n│\n"
        f"┠ <b>{label}</b>\n"
        f"┖ Current: <code>{_current(arg, doc)}</code>\n\n"
        "<i>Send the new value in this chat within 60 seconds.</i>" + hint,
        buttons.build_menu(2))


async def _publish_backend(url):
    """Rewrite backend.json on the public site via the GitHub token."""
    tok = (await _doc()).get("gh_token")
    if not tok:
        return ("no GitHub token set - saved for the bot only. Send a "
                "token via 'GitHub token' to auto-publish to the website.")
    if not url:
        return "cleared (no publish)"
    import base64
    import json as _json
    import aiohttp
    api = (f"https://api.github.com/repos/{_GH_OWNER}/{_GH_REPO}"
           f"/contents/{_GH_FILE}")
    hdrs = {"Authorization": f"token {tok}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "wzfix-ws"}
    payload = _json.dumps({"api": url})
    try:
        async with aiohttp.ClientSession() as s:
            sha = None
            async with s.get(api, headers=hdrs,
                             timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    sha = (await r.json()).get("sha")
            data = {"message": "ws: website backend update",
                    "content": base64.b64encode(payload.encode()).decode()}
            if sha:
                data["sha"] = sha
            async with s.put(api, headers=hdrs, json=data,
                             timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status in (200, 201):
                    return "published to the website ✓"
                return (f"GitHub rejected the update "
                        f"(HTTP {r.status}) - check the token's write access")
    except Exception as e:
        return f"could not reach GitHub ({type(e).__name__})"


async def _save(key, val):
    """returns (err, note) - err set means not saved."""
    if key in _RANGES:
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "that is not a number", ""
        lo, hi = _RANGES[key]
        if not lo <= v <= hi:
            return f"value must be between {lo} and {hi}", ""
        if key in ("WEBDL_CONC", "USER_DAILY"):
            v = int(v)
    elif key == "WEBDL_PASS":
        if len(val) < 4:
            return "password too short (at least 4 characters)", ""
        v = val
    elif key == "PD_KEY":
        v = (val or "").strip()
        if v and len(v) < 10:
            return "that does not look like an API key", ""
        v = v
    elif key == "MAINTENANCE":
        v = str(val).strip().lower() in ("yes", "y", "on", "1", "true")
    elif key in ("MUSIC", "NOTIFY_DONE", "QUICK_MODE"):
        v = str(val).strip().lower() in ("yes", "y", "on", "1", "true")
    elif key == "DEFAULT_THEME":
        v = (val or "").strip().lower()
        if v not in ("midnight", "ocean", "sunset", "forest", "light"):
            return "themes: midnight, ocean, sunset, forest, light", ""
        v = v
    elif key == "ANNOUNCE":
        v = (val or "").strip()[:300]
    elif key == "THEME":
        import re as _re2
        v = (val or "").strip()
        if v and not _re2.fullmatch(r"#[0-9a-fA-F]{6}", v):
            return "send a hex color like #7c5cff (or nothing to reset)", ""
    elif key == "YT_COOKIES":
        v = (val or "").strip()
        if not v:
            pass  # allow clearing
        elif len(v) > 200_000:
            return "cookies too big (200 KB max)", ""
        elif "\t" not in v:
            return ("that does not look like cookies.txt - export it "
                    "from a browser extension and send the whole file", "")
        v = v.replace("\\t", "\t")
    elif key in ("TG_FILES_CHAT", "TG_LOGS_CHAT"):
        v = (val or "").strip().lstrip("-")
        if v and (not v.isdigit() or not 5 <= len(v) <= 15):
            return "send the numeric group id (starts with -100)", ""
        v = ("-" + v) if v else ""
    elif key == "SITE_BACKEND":
        v = (val or "").strip().rstrip("/")
        if v and not v.startswith("https://"):
            return "send a full https:// URL (or nothing to clear)", ""
        if v and not 10 <= len(v) <= 120:
            return "that does not look like a worker URL", ""
    elif key == "GH_TOKEN":
        v = (val or "").strip()
        if not v:
            pass  # allow clearing
        elif len(v) < 20:
            return "that does not look like a GitHub token", ""
    elif key in ("R2_ENDPOINT", "R2_PUBLIC"):
        v = (val or "").strip().rstrip("/")
        if v and not v.startswith("https://"):
            return "send a full https:// URL", ""
    elif key in ("R2_KEY_ID", "R2_BUCKET"):
        v = (val or "").strip()
    elif key == "R2_SECRET":
        v = (val or "").strip()
        if v and len(v) < 20:
            return "that does not look like an R2 secret key", ""
    elif key == "SITE_NAME":
        v = (val or "").strip()
        if not v:
            return "send a name (or keep the default)", ""
        if len(v) > 60:
            return "that name is too long", ""
    elif key == "OWNER_CONTACT":
        v = (val or "").strip().lstrip("@")
        if v and (len(v) > 40 or any(c.isspace() for c in v)):
            return "send a Telegram username (like @joshi)", ""
    elif key == "GUEST_MODE":
        _g = (val or "").strip().lower()
        if _g in ("yes", "y", "on", "1", "true"):
            v = True
        elif _g in ("no", "n", "off", "0", "false", ""):
            v = False
        else:
            return "send yes or no", ""
    elif key in ("V_MAX_GB", "V_DAILY", "S_MAX_GB", "S_DAILY",
                 "BW_GLOBAL_MB", "BW_USER_MB"):
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "that is not a number", ""
        lo, hi = _RANGES[key]
        if not lo <= v <= hi:
            return f"value must be between {lo} and {hi}", ""
        if key in ("V_DAILY", "S_DAILY"):
            v = int(v)
    elif key == "ADD_USER":
        import re as _re
        parts = (val or "").split()
        if len(parts) != 2:
            return "send it like: name password", ""
        name, pw = parts[0].lower(), parts[1]
        # v19.1.0: dots/symbols are not allowed in usernames
        if not _re.fullmatch(r"[a-z0-9_-]{2,24}", name):
            return ("usernames: letters, numbers, _ and - only "
                    "(2-24 chars, no dots or symbols)"), ""
        if len(pw) < 4:
            return "password too short (at least 4 characters)", ""
        from ..helper.wzfix.r1_core import _db, _part
        from ..helper.wzfix.r17_webdl import _new_acct
        col = _db().wzfix_config[f"webdl_users_{_part()}"]
        try:
            if await col.find_one({"_id": f"u:{name}"}):
                return "that name is taken", ""
            await col.insert_one(_new_acct(name, pw, "v"))
            await _db().wzfix_config[_part()].update_one(
                {"_id": "webdl_stats"}, {"$inc": {"users": 1}},
                upsert=True)
        except Exception as e:
            return f"could not save ({type(e).__name__})", ""
        return None, (f"\n\n✓ member <code>{name}</code> created — tell "
                      "them to sign in on the website")
    elif key == "INVITE_CODE":
        from hashlib import sha256 as _sha
        from secrets import token_hex as _th
        from ..helper.wzfix.r1_core import _db, _part
        code = "WZ-" + _th(4).upper()
        ih = _sha(code.encode()).hexdigest()[:40]
        try:
            await _db().wzfix_config[f"webdl_users_{_part()}"].insert_one(
                {"_id": f"inv:{ih}", "used": False,
                 "created": int(time())})
        except Exception as e:
            return f"could not save ({type(e).__name__})", ""
        return None, (f"\n\n✓ one-time invite code:\n"
                      f"<code>{code}</code>\n"
                      "The member uses it once on the website (I have an "
                      "invite code) and picks their own name + password. "
                      "Special accounts get the special limits.")
    elif key == "DEL_USER":
        import re as _re
        name = (val or "").strip().lower()
        if not _re.fullmatch(r"[a-z0-9_.-]{2,24}", name):
            return "send the member's name", ""
        from ..helper.wzfix.r1_core import _db, _part
        try:
            r = await _db().wzfix_config[f"webdl_users_{_part()}"].delete_one(
                {"_id": f"u:{name}"})
            if not r or not getattr(r, "deleted_count", 1):
                return "no account with that name", ""
            try:
                from ..helper.wzfix import r17_webdl
                r17_webdl._BAN_CACHE.pop(f"u:{name}", None)
            except Exception:
                pass
        except Exception as e:
            return f"could not delete ({type(e).__name__})", ""
        return None, ("\n\n✓ member <code>{name}</code> removed — they "
                      "are signed out of the website within seconds")
    elif key == "RESET_DEV":
        import re as _re
        name = (val or "").strip().lower()
        if not _re.fullmatch(r"[a-z0-9_.-]{2,24}", name):
            return "send the member's name", ""
        from ..helper.wzfix.r1_core import _db, _part
        try:
            await _db().wzfix_config[f"webdl_users_{_part()}"].update_one(
                {"_id": f"u:{name}"}, {"$set": {"dev": ""}})
        except Exception as e:
            return f"could not update ({type(e).__name__})", ""
        return None, (f"\n\n✓ <code>{name}</code> unlocked from the old "
                      "device — it binds to the next one they sign in with")
    elif key == "SET_LIMIT":
        import re as _re
        parts = (val or "").split()
        if len(parts) != 3:
            return "send: name gb daily", ""
        name = parts[0].lower()
        try:
            gb = float(parts[1])
            dly = int(parts[2])
        except ValueError:
            return "send: name gb daily (numbers)", ""
        from ..helper.wzfix.r1_core import _db, _part
        col = _db().wzfix_config[f"webdl_users_{_part()}"]
        try:
            acc = await col.find_one({"_id": f"u:{name}"})
            if not acc:
                return "no account with that name", ""
            if gb == 0 or dly == 0:
                await col.update_one({"_id": f"u:{name}"},
                                     {"$unset": {"cap_gb": "", "daily": ""}})
                return None, f"\n\n✓ {name} back on the default limits"
            if not 0.1 <= gb <= 40:
                return "gb must be 0.1-40 (send 0 0 to reset)", ""
            if not 1 <= dly <= 500:
                return "daily must be 1-500 (send 0 0 to reset)", ""
            await col.update_one({"_id": f"u:{name}"},
                                 {"$set": {"cap_gb": gb, "daily": dly}})
        except Exception as e:
            return f"could not update ({type(e).__name__})", ""
        return None, (f"\n\n✓ <code>{name}</code>: files up to {gb:g} GB, "
                      f"{dly} downloads/day (others stay on the defaults)")
    elif key == "INVITE_BATCH":
        try:
            n = int((val or "").strip())
        except ValueError:
            return "send how many (1-20)", ""
        if not 1 <= n <= 20:
            return "send how many (1-20)", ""
        from hashlib import sha256 as _sha
        from secrets import token_hex as _th
        from ..helper.wzfix.r1_core import _db, _part
        codes = []
        for _ in range(n):
            code = "WZ-" + _th(4).upper()
            ih = _sha(code.encode()).hexdigest()[:40]
            try:
                await _db().wzfix_config[f"webdl_users_{_part()}"].insert_one(
                    {"_id": f"inv:{ih}", "code": code, "used": False,
                     "created": int(time())})
                codes.append(code)
            except Exception:
                pass
        if not codes:
            return "could not save the codes", ""
        return None, ("\n\n✓ invite codes (one-time, special accounts):\n"
                      + "\n".join(f"<code>{c}</code>" for c in codes))
    elif key == "INVITE_LIST":
        from ..helper.wzfix.r1_core import _db, _part
        try:
            invs = await _db().wzfix_config[f"webdl_users_{_part()}"].find(
                {"_id": {"$regex": "^inv:"}}).to_list(50)
        except Exception:
            invs = []
        if not invs:
            return None, "\n\nno invite codes yet"
        lines = []
        for i in invs:
            c = i.get("code", "(old code)")
            if i.get("used"):
                lines.append(f"✗ <code>{c}</code> — used"
                            + (f" by {i.get('used_by')}" if i.get("used_by")
                               else ""))
            else:
                lines.append(f"✓ <code>{c}</code> — free")
        return None, "\n\n" + "\n".join(lines)
    elif key == "COOKIE_TEST":
        from ..helper.wzfix import r17_webdl
        s = await r17_webdl._settings(force=True)
        if not s.get("yt_cookies"):
            return "no cookies set yet (send them via YT_COOKIES)", ""
        ck = await r17_webdl._cookie_file()
        import asyncio as _aio
        import sys as _sys
        proc = await _aio.create_subprocess_exec(
            _sys.executable, "-m", "yt_dlp", "-J", "--no-warnings",
            "--skip-download", "--cookies", ck,
            "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
            stdout=_aio.subprocess.PIPE, stderr=_aio.subprocess.STDOUT)
        try:
            out, _ = await _aio.wait_for(proc.communicate(), timeout=90)
        except _aio.TimeoutError:
            proc.kill()
            return "timed out (90 s) — cookies may be stuck", ""
        if proc.returncode != 0:
            _l = [x for x in out.decode("utf-8", "replace").splitlines()
                  if x.strip()]
            return "FAILED — " + (_l[-1] if _l else "unknown")[:180], ""
        return None, "\n\n✓ cookies work — YouTube metadata read fine"
    elif key == "CLEANUP":
        try:
            days = int((val or "").strip())
        except ValueError:
            return "send the number of days (e.g. 30)", ""
        if not 1 <= days <= 365:
            return "send days between 1 and 365", ""
        from ..helper.wzfix.r1_core import _db, _part
        cut = int(time()) - days * 86400
        n = 0
        try:
            col = _db().wzfix_config[f"webdl_users_{_part()}"]
            guests = await col.find({"_id": {"$regex": "^fp-"}}).to_list(500)
            import asyncio as _aio2
            for g in guests:
                if int(g.get("seen", 0) or 0) < cut and not g.get("banned"):
                    await col.delete_one({"_id": g["_id"]})
                    n += 1
        except Exception:
            pass
        return None, f"\n\n✓ removed {n} guest devices not seen in {days} d"
    elif key == "STATS":
        from ..helper.wzfix import r17_webdl
        from ..helper.wzfix.r1_core import _db, _part
        try:
            st = await r17_webdl._stats_doc()
        except Exception:
            st = {}
        try:
            nu = await _db().wzfix_config[f"webdl_users_{_part()}"] \
                .count_documents({"_id": {"$regex": "^u:"}})
        except Exception:
            nu = 0
        try:
            used = r17_webdl._dir_bytes(r17_webdl._base_dir())
        except Exception:
            used = 0
        hist = list(getattr(r17_webdl, "_HISTORY", []))
        today = r17_webdl._today_ist()
        import time as _t
        tn = sum(1 for h in hist if h.get("done_at")
                 and _t.strftime("%Y%m%d",
                                 _t.gmtime(h["done_at"] + 19800)) == today)
        s = await r17_webdl._settings()
        return None, (
            "\n\n📊 <b>Website stats</b>\n"
            f"┣ downloads total: {int(st.get('dl_total', 0))}\n"
            f"┣ downloads today: {tn}\n"
            f"┣ accounts: {nu}\n"
            f"┣ likes: {int(st.get('likes', 0))}\n"
            f"┣ disk used: {used / (1 << 30):.1f} of "
            f"{s.get('max_gb', '?')} GB\n"
            f"┗ cloud route: "
            + ("ON" if s.get("pd_key") else "OFF"))
    elif key == "DISK_WIPE":
        if (val or "").strip().lower() not in ("yes", "y"):
            return "send yes to confirm", ""
        from ..helper.wzfix import r17_webdl
        import shutil as _sh
        n = 0
        for t in list(r17_webdl._TASKS.values()):
            if t.get("status") in ("done", "error", "cancel"):
                t["status"] = "cleared"
                t["path"] = ""
                _sh.rmtree(r17_webdl._base_dir() + "/" + t["id"],
                           ignore_errors=True)
                n += 1
        return None, f"\n\n✓ deleted files of {n} finished downloads"
    elif key == "QUICK_ACCT":
        from ..helper.wzfix import r17_webdl
        name, pw = r17_webdl._gen_creds()
        col = r17_webdl._users_col()
        for _ in range(5):
            if not (await col.find_one({"_id": f"u:{name}"})):
                break
            name, pw = r17_webdl._gen_creds()
        doc = r17_webdl._new_acct(name, pw, "q")
        doc.update({"created_from": "telegram /ws",
                    "created_at": int(time())})
        try:
            await col.insert_one(doc)
        except Exception as e:
            return f"could not create ({type(e).__name__})", ""
        try:
            await r17_webdl._stats_inc("users", 1)
        except Exception:
            pass
        return None, (
            "\n\n⚡ <b>quick account</b>\n"
            f"┣ username: <code>{name}</code>\n"
            f"┣ password: <code>{pw}</code>\n"
            "┗ guest limits · binds to the first device that signs in\n\n"
            "<i>hand these to the person — they can change the password "
            "after signing in.</i>")
    elif key == "ATTEMPTS":
        from ..helper.wzfix.r1_core import _db, _part
        try:
            rows = await _db().wzfix_config[f"webdl_users_{_part()}"] \
                .find({"_id": {"$regex": "^att:"}}).to_list(200)
        except Exception:
            rows = []
        if not rows:
            return None, "\n\nno blocked attempts — all clean"
        rows.sort(key=lambda r: -int(r.get("at", 0) or 0))
        lines = []
        for r in rows[:15]:
            lines.append(
                f"✗ <code>{str(r.get('name', ''))[:28]}</code> — "
                f"{r.get('reason', '?')} · {r.get('ip', '?')}")
        return None, ("\n\n🛡 blocked signup attempts (newest first):\n"
                      + "\n".join(lines)
                      + f"\n\n{len(rows)} total · allow an ip from the "
                        "site panel → Guard")
    elif key == "ALLOW_IP":
        ip = (val or "").strip()[:45]
        if not ip:
            return "send the ip address", ""
        from ..helper.wzfix.r1_core import _db, _part
        try:
            await _db().wzfix_config[f"webdl_users_{_part()}"].insert_one(
                {"_id": f"allow:{ip}", "at": int(time())})
        except Exception:
            pass
        return None, (f"\n\n✓ one-time pass ready for <code>{ip}</code> — "
                      "it is used up the next time they try")
    elif key == "BLOCK_IP":
        from ..helper.wzfix import r17_webdl
        from ..helper.wzfix.r1_core import _db, _part
        ip = (val or "").strip()[:45]
        if not ip:
            return "send the ip address", ""
        net = r17_webdl._net_key(ip)
        col = _db().wzfix_config[f"webdl_users_{_part()}"]
        made = []
        for k in ({f"block:{ip}"} | ({f"blocknet:{net}"} if net else set())):
            try:
                await col.insert_one({"_id": k, "at": int(time()),
                                      "ip": ip, "why": "blocked via /ws"})
                made.append(k)
            except Exception:
                pass
        return None, (f"\n\n⛔ blocked <code>{ip}</code>\n"
                      f"(whole network <code>{net}</code> too)\n"
                      "they can no longer sign up or use guests. Existing "
                      "accounts can still sign in.")
    elif key == "UNBLOCK_IP":
        from ..helper.wzfix import r17_webdl
        from ..helper.wzfix.r1_core import _db, _part
        raw = (val or "").strip()[:45]
        if not raw:
            return "send the ip or network to unblock", ""
        net = r17_webdl._net_key(raw)
        col = _db().wzfix_config[f"webdl_users_{_part()}"]
        n = 0
        for k in (f"block:{raw}", f"blocknet:{net}", f"blocknet:{raw}"):
            try:
                r = await col.delete_one({"_id": k})
                n += int(getattr(r, "deleted_count", 0) or 0)
            except Exception:
                pass
        if not n:
            return "nothing was blocked for that ip", ""
        return None, f"\n\n✓ unblocked ({n} entries removed)"
    elif key == "ALLOW_LIST":
        from ..helper.wzfix.r1_core import _db, _part
        try:
            rows = await _db().wzfix_config[f"webdl_users_{_part()}"] \
                .find({"_id": {"$regex": "^allow:"}}).to_list(100)
        except Exception:
            rows = []
        lines = [f"🎫 <code>{str(r.get('_id', ''))[6:]}</code>"
                 for r in rows]
        _bl = []
        try:
            _bl = await _db().wzfix_config[f"webdl_users_{_part()}"] \
                .find({"_id": {"$regex": "^block"}}).to_list(100)
        except Exception:
            _bl = []
        out = "\n\n🎫 waiting passes:\n" + ("\n".join(lines)
                                             if lines else "(none)")
        out += "\n\n⛔ blocked:\n" + (
            "\n".join(f"<code>{str(b.get('_id', ''))}</code>" for b in _bl)
            if _bl else "(none)")
        out += "\n\nsend an ip to ALLOW_IP / BLOCK_IP / UNBLOCK_IP"
        return None, out
    elif key == "TG_TEST":
        from ..helper.wzfix import r17_webdl
        try:
            from ...core.tg_client import TgClient
        except Exception as e:
            return f"bot client unavailable ({type(e).__name__})", ""
        lines = []
        for which, pref in (("logs", "tg_logs_chat"),
                            ("files", "tg_files_chat")):
            c = r17_webdl._norm_chat(await r17_webdl._chat_id(pref))
            if not c:
                lines.append(f"✗ {which}: not set (send the group id here)")
                continue
            try:
                await TgClient.bot.send_message(
                    chat_id=c, text=f"✅ webdl test ({which} group) — "
                                    "routing works")
                lines.append(f"✓ {which}: delivered to {c}")
            except Exception as e:
                lines.append(f"✗ {which} ({c}): "
                             f"{type(e).__name__}: {str(e)[:120]}")
        return None, "\n\n" + "\n".join(lines)
    elif key == "RELOAD":
        from ..helper.wzfix import r17_webdl
        r17_webdl._SETTINGS_CACHE = (0, None)
        r17_webdl._BAN_CACHE.clear()
        r17_webdl._FAILS.clear()
        return None, "\n\n✓ settings reloaded — the website sees changes now"
    elif key == "BAN_USER":
        import re as _re
        name = (val or "").strip().lower()
        if not _re.fullmatch(r"[a-z0-9_.-]{2,24}", name):
            return "send the member's name", ""
        from ..helper.wzfix.r1_core import _db, _part
        col = _db().wzfix_config[f"webdl_users_{_part()}"]
        try:
            acc = await col.find_one({"_id": f"u:{name}"})
            if not acc:
                return "no account with that name", ""
            newban = not bool(acc.get("banned"))
            await col.update_one({"_id": f"u:{name}"},
                                 {"$set": {"banned": newban, "fails": 0}})
            try:
                from ..helper.wzfix import r17_webdl
                r17_webdl._BAN_CACHE.pop(f"u:{name}", None)
            except Exception:
                pass
        except Exception as e:
            return f"could not update ({type(e).__name__})", ""
        return None, (f"\n\n✓ <code>{name}</code> is now "
                      f"{'BANNED' if newban else 'unbanned'} — the website "
                      "signs them out within seconds")
    elif key == "UNBAN_ALL":
        if (val or "").strip().lower() not in ("yes", "y"):
            return "send yes to confirm", ""
        from ..helper.wzfix.r1_core import _db, _part
        n = 0
        try:
            r1 = await _db().wzfix_config[f"webdl_users_{_part()}"] \
                .update_many({"banned": True},
                             {"$set": {"banned": False, "fails": 0}})
            n = int(getattr(r1, "modified_count", 0) or 0)
        except Exception as e:
            return f"could not update ({type(e).__name__})", ""
        try:
            from ..helper.wzfix import r17_webdl
            r17_webdl._BAN_CACHE.clear()
            r17_webdl._FAILS.clear()
        except Exception:
            pass
        return None, f"\n\n✓ unbanned {n} users"
    else:
        return "unknown setting", ""
    from ..helper.wzfix.r1_core import _db, _part
    await _db().wzfix_config[_part()].update_one(
        {"_id": "webdl"}, {"$set": {_DB_FIELDS[key]: v}}, upsert=True)
    try:
        from ..helper.wzfix import r17_webdl
        r17_webdl._SETTINGS_CACHE = (0, None)
    except Exception:
        pass
    note = ""
    if key == "SITE_BACKEND":
        try:
            note = "\n\nℹ " + (await _publish_backend(v))
        except Exception:
            note = "\n\n⚠ saved in bot, but publishing to the site failed"
    return None, note


async def _pending_filter(_, __, message):
    user = message.from_user or message.sender_chat
    if not user:
        return False
    pend = _PENDING.get((message.chat.id, user.id))
    if not pend or pend["t"] + 60 <= time():
        return False
    return bool(message.text or message.document
                or getattr(message, "forward_from_chat", None))


ws_text_filter = create(_pending_filter)


@new_task
async def ws_receive(client, message):
    user = message.from_user or message.sender_chat
    pend = _PENDING.pop((message.chat.id, user.id), None)
    if not pend:
        return
    key = pend["key"]
    val = (message.text or "").strip()[:200_000]
    # a forwarded message identifies a group without typing ids
    fchat = getattr(message, "forward_from_chat", None)
    if fchat is not None and key in ("TG_FILES_CHAT", "TG_LOGS_CHAT"):
        val = str(fchat.id)
    # a cookies.txt sent as a file beats pasting
    elif message.document:
        try:
            p = await client.download_media(message)
            with open(p, encoding="utf-8", errors="replace") as f:
                val = f.read(200_001).strip()
            os.remove(p)
        except Exception:
            val = ""
        if val and "\t" not in val:
            val = val.replace("\\t", "\t")
    try:
        await delete_message(message)
    except Exception:
        pass
    err, note = await _save(key, val)
    doc = await _doc()
    label = dict(WS_KEYS).get(key, key)
    _sid = _SEC_OF.get(key, "")
    try:
        msg = await client.get_messages(message.chat.id, pend["mid"])
    except Exception:
        return
    if err:
        _PENDING[(message.chat.id, user.id)] = {"key": key, "t": time(),
                                                 "mid": pend["mid"]}
        buttons = ButtonMaker()
        buttons.data_button("Back", "wzset back", position="footer")
        buttons.data_button("Close", "wzset close", position="footer")
        await edit_message(
            msg,
            f"⚠️ <b>{label}</b> not saved: {err}\n\n"
            "<i>Send the new value in this chat within 60 seconds.</i>",
            buttons.build_menu(2))
    else:
        await edit_message(msg, _menu_text(
            doc, note=f"\n\n✓ <b>{label}</b> saved{note}"),
            _menu_buttons(doc))
'''

MODULE_URL = ("https://drive.usercontent.google.com/download?"
              "id=1bV2f-VG1R4FCZoJSCyQxzSAd44aJr3Bi&export=download&confirm=t")
MODULE_SHA = "2ee85331362ffd226441e81a212dda04d86b45d61406d2e3253b4d17c8a17eac"

WZMLX = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else ".")


def die(msg):
    print(f"r17: {msg}")
    sys.exit(1)


def main():
    # 1. module
    try:
        raw = urllib.request.urlopen(MODULE_URL, timeout=60).read()
    except Exception as e:
        die(f"module fetch failed: {e}")
    if hashlib.sha256(raw).hexdigest() != MODULE_SHA:
        die("module sha256 mismatch — refusing to install")
    wzdir = os.path.join(WZMLX, "bot", "helper", "wzfix")
    os.makedirs(wzdir, exist_ok=True)
    mod = os.path.join(wzdir, "r17_webdl.py")
    with open(mod, "w", encoding="utf-8") as f:
        f.write(raw.decode("utf-8"))
    r = subprocess.run([sys.executable, "-m", "py_compile", mod],
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        die(f"r17_webdl.py compile FAILED: {(r.stderr or '')[-300:]}")
    print("r17: r17_webdl.py written (compiles)")

    # 2. stream server routes
    ss = os.path.join(WZMLX, "bot", "core", "stream_server.py")
    with open(ss, "r", encoding="utf-8") as f:
        s = f.read()
    if "webdl_routes" in s:
        print("r17: stream server already has webdl")
    else:
        anchor = '    app.router.add_route("*", "/_dl/{token}", _dl)'
        routes = (
            '    # WZFIX R17: web downloader (v15.84)\n'
            '    try:\n'
            '        from ..helper.wzfix.r17_webdl import webdl_routes\n'
            '        webdl_routes(app)\n'
            '    except Exception as e:\n'
            '        LOGGER.error(f"r17 webdl routes: {e}")\n'
        )
        if anchor in s:
            s = s.replace(anchor, anchor + "\n" + routes, 1)
            with open(ss, "w", encoding="utf-8") as f:
                f.write(s)
            print("r17: stream server routes registered (/webdl)")
        else:
            print("r17: WARN stream server /_dl anchor missing")

    # 3. wserver proxy (streamed for /webdl/dl, buffered otherwise)
    ws = os.path.join(WZMLX, "web", "wserver.py")
    with open(ws, "r", encoding="utf-8") as f:
        w = f.read()
    if "WZFIX_R17_WSERVER" in w:
        print("r17: wserver already patched")
    else:
        proxy = (
            '@app.api_route("/webdl", methods=["GET", "POST", "OPTIONS"])  # WZFIX_R17_WSERVER\n'
            '@app.api_route("/webdl/{path:path}", methods=["GET", "POST", "OPTIONS"])  # WZFIX_R17_WSERVER\n'
            "async def webdl_proxy(request: Request):\n"
            '    _target = f"{STREAM_BASE}{request.url.path}"\n'
            "    if request.url.query:\n"
            '        _target += f"?{request.url.query}"\n'
            "    _fwd = {k: v for k, v in request.headers.items()\n"
            '            if k.lower() not in ("host", "content-length", "accept-encoding")}\n'
            "    _body = await request.body()\n"
            "    try:\n"
            '        if request.url.path.startswith("/webdl/dl/"):\n'
            "            # large file: stream, no total timeout (5 min would\n"
            "            # kill long phone downloads)\n"
            '            from aiohttp import ClientTimeout\n'
            "            _sess = getattr(request.app.state, \"webdl_stream_session\", None)\n"
            "            if _sess is None or _sess.closed:\n"
            "                _sess = ClientSession(\n"
            "                    auto_decompress=True,\n"
            "                    timeout=ClientTimeout(total=None, connect=30,\n"
            "                                          sock_read=600))\n"
            "                request.app.state.webdl_stream_session = _sess\n"
            "            _up = await _sess.request(request.method, _target,\n"
            "                                      headers=_fwd, data=_body)\n\n"
            "            async def _wzdl_gen():\n"
            "                try:\n"
            "                    async for _chunk in _up.content.iter_any():\n"
            "                        yield _chunk\n"
            "                finally:\n"
            "                    _up.release()\n\n"
            "            return StreamingResponse(\n"
            "                _wzdl_gen(),\n"
            "                status_code=_up.status,\n"
            "                headers={k: v for k, v in _up.headers.items()\n"
            '                         if k.lower() not in ("transfer-encoding", "content-length",\n'
            '                                              "content-encoding", "connection")},\n'
            "            )\n"
            "        async with http_session.request(\n"
            "                request.method, _target, headers=_fwd, data=_body) as _up:\n"
            "            _raw = await _up.read()\n"
            "            return Response(\n"
            "                content=_raw, status_code=_up.status,\n"
            "                headers={k: v for k, v in _up.headers.items()\n"
            '                         if k.lower() not in ("transfer-encoding", "content-length",\n'
            '                                              "content-encoding", "connection")},\n'
            '                media_type=_up.headers.get("content-type"),\n'
            "            )\n"
            "    except Exception as _e:\n"
            '        return JSONResponse({"error": f"webdl upstream unreachable: {_e.__class__.__name__}"},\n'
            "                            status_code=502)\n"
        )
        anchor_home = '@app.get("/", response_class=HTMLResponse)'
        anchor_wza = '@app.api_route("/wzadmin", methods=["GET"])'
        if anchor_home in w:
            w = w.replace(anchor_home, proxy + "\n\n" + anchor_home, 1)
            with open(ws, "w", encoding="utf-8") as f:
                f.write(w)
            print("r17: wserver patched (/webdl proxy, streaming dl)")
        elif anchor_wza in w:
            w = w.replace(anchor_wza, proxy + "\n" + anchor_wza, 1)
            with open(ws, "w", encoding="utf-8") as f:
                f.write(w)
            print("r17: wserver patched (wzadmin anchor)")
        else:
            print("r17: WARN wserver anchors missing")
    # 4. /bs menu registration (bot_settings) so WEBDL_* is editable
    # from Telegram like STREAM_PASS: descriptions dict + string save.
    # /bs persists via database.update_config and Config is read live
    # by the module, so a change applies instantly and survives boots.
    bs = os.path.join(WZMLX, "bot", "modules", "bot_settings.py")
    with open(bs, "r", encoding="utf-8") as f:
        b = f.read()
    if "WEBDL_PASS" in b:
        print("r17: /bs already has WEBDL_PASS")
    else:
        desc_anchor = (
            '    "STREAM_PASS": "Password for the user stream links '
            '(/stream password gate). Separate from ADMIN_PASS. '
            'Default: 12345 if empty.",\n'
        )
        descs = (
            '    # WZFIX R17: web downloader vars\n'
            '    "WEBDL_PASS": "Password for the web downloader site '
            '(<worker URL>/webdl). Separate from STREAM_PASS and '
            'ADMIN_PASS. Default: joshi if empty.",\n'
            '    "WEBDL_TTL": "Web downloader: hours a finished file stays '
            'downloadable before auto-delete. Default: 6.",\n'
            '    "WEBDL_MAX_GB": "Web downloader: total disk budget for '
            'web downloads, in GB. Default: 8.",\n'
            '    "WEBDL_CONC": "Web downloader: concurrent downloads. '
            'Default: 2.",\n'
        )
        save_anchor = '    elif key == "STREAM_PASS":\n        value = str(value)\n'
        save_new = (
            '    elif key == "STREAM_PASS":\n        value = str(value)\n'
            '    elif key == "WEBDL_PASS":\n        value = str(value)\n'
        )
        _ok17 = 0
        if desc_anchor in b:
            b = b.replace(desc_anchor, desc_anchor + descs, 1)
            _ok17 += 1
        if save_anchor in b:
            b = b.replace(save_anchor, save_new, 1)
            _ok17 += 1
        with open(bs, "w", encoding="utf-8") as f:
            f.write(b)
        r = subprocess.run([sys.executable, "-m", "py_compile", bs],
                           capture_output=True, text=True, timeout=60)
        if r.returncode == 0:
            print(f"r17: /bs registered WEBDL_PASS + knobs ({_ok17}/2 edits)")
        else:
            print(f"r17: /bs compile FAILED - {(r.stderr or '')[-200:]}")
    # 5. config_manager declarations - THE one that makes /bs show the vars:
    # the "Config Variables" page enumerates Config.get_all() (class attrs),
    # so a var must be DECLARED on the class; DEFAULT_DESP only adds the
    # description shown when a var is opened. Empty/0 -> module defaults.
    cm = os.path.join(WZMLX, "bot", "core", "config_manager.py")
    with open(cm, "r", encoding="utf-8") as f:
        c = f.read()
    if "WEBDL_PASS" in c:
        print("r17: config_manager already declares WEBDL vars")
    else:
        decl_anchor = "    USENET_SERVERS = []\n"
        decls = (
            "    # WZFIX R17: web downloader vars (declared so /bs lists them;\n"
            "    # empty -> module defaults: pass joshi / ttl 6h / 8GB / conc 2)\n"
            '    WEBDL_PASS = ""\n'
            "    WEBDL_TTL = 0\n"
            "    WEBDL_MAX_GB = 0\n"
            "    WEBDL_CONC = 0\n"
        )
        if decl_anchor in c:
            c = c.replace(decl_anchor, decl_anchor + decls, 1)
            with open(cm, "w", encoding="utf-8") as f:
                f.write(c)
            r = subprocess.run([sys.executable, "-m", "py_compile", cm],
                               capture_output=True, text=True, timeout=60)
            if r.returncode == 0:
                print("r17: config_manager declares WEBDL_* -> /bs will list them")
            else:
                print(f"r17: config_manager compile FAILED - {(r.stderr or '')[-200:]}")
        else:
            print("r17: WARN config_manager anchor missing")
    # 6. /ws command - website settings panel (owner only, like /bs for
    # the website): every webdl knob + MediaFire credentials.
    wsmod = os.path.join(WZMLX, "bot", "modules", "ws_settings.py")
    with open(wsmod, "w", encoding="utf-8") as f:
        f.write(WS_SRC)
    rc = subprocess.run([sys.executable, "-m", "py_compile", wsmod],
                        capture_output=True, text=True, timeout=60)
    if rc.returncode == 0:
        print("r17: ws_settings.py written (compiles)")
    else:
        print(f"r17: ws_settings compile FAILED - {(rc.stderr or '')[-200:]}")

    mi = os.path.join(WZMLX, "bot", "modules", "__init__.py")
    with open(mi, "r", encoding="utf-8") as f:
        c = f.read()
    if "ws_settings" not in c:
        # names MUST go into __all__: handlers.py uses `from ..modules
        # import *`, which only binds names listed in __all__ (v16.1.0
        # boot crashed because the import was appended without it)
        if "__all__ = [\n" in c:
            c = c.replace(
                "__all__ = [\n",
                "__all__ = [\n    \"ws_settings\",\n    \"ws_callback\",\n"
                "    \"ws_receive\",\n    \"ws_text_filter\",\n", 1)
        c += "\nfrom .ws_settings import (\n"
        c += "    ws_settings, ws_callback, ws_receive, ws_text_filter\n)\n"
        with open(mi, "w", encoding="utf-8") as f:
            f.write(c)
        rc3 = subprocess.run([sys.executable, "-m", "py_compile", mi],
                            capture_output=True, text=True, timeout=60)
        if rc3.returncode == 0:
            print("r17: modules/__init__ wired (+__all__) for /ws")
        else:
            print(f"r17: __init__ compile FAILED - {(rc3.stderr or '')[-200:]}")
    else:
        print("r17: modules/__init__ already wired for /ws")

    hd = os.path.join(WZMLX, "bot", "core", "handlers.py")
    with open(hd, "r", encoding="utf-8") as f:
        h = f.read()
    if "WZFIX R19" not in h:
        hd_anchor = (
            '    TgClient.bot.add_handler(\n'
            '        CallbackQueryHandler(\n'
            '            edit_bot_settings, filters=regex("^botset") & CustomFilters.sudo\n'
            '        )\n'
            '    )\n'
        )
        hd_add = (
            '    # WZFIX R19: /ws website settings (owner only) - guarded\n'
            '    # so a missing ws module can never crash add_handlers\n'
            '    try:\n'
            '        TgClient.bot.add_handler(\n'
            '            MessageHandler(\n'
            '                ws_settings,\n'
            '                filters=command("ws", case_sensitive=True)\n'
            '                & CustomFilters.owner,\n'
            '            )\n'
            '        )\n'
            '        TgClient.bot.add_handler(\n'
            '            CallbackQueryHandler(ws_callback, filters=regex("^wzset"))\n'
            '        )\n'
            '        TgClient.bot.add_handler(\n'
            '            MessageHandler(ws_receive, filters=ws_text_filter), group=-1\n'
            '        )\n'
            '    except NameError:\n'
            '        print("r17: /ws NOT registered - module not imported")\n'
            '\n'
        )
        if hd_anchor in h:
            h = h.replace(hd_anchor, hd_anchor + hd_add, 1)
            with open(hd, "w", encoding="utf-8") as f:
                f.write(h)
            rc2 = subprocess.run([sys.executable, "-m", "py_compile", hd],
                                 capture_output=True, text=True, timeout=60)
            if rc2.returncode == 0:
                print("r17: /ws registered in handlers (owner only)")
            else:
                print(f"r17: handlers compile FAILED - {(rc2.stderr or '')[-200:]}")
        else:
            print("r17: WARN handlers.py anchor missing")
    else:
        print("r17: handlers already wired for /ws")

    print("r17: done")


main()
