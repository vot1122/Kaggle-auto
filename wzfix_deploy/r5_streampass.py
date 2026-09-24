"""WZFIX Round 5 (v15.62) — per-link stream passwords.

Every stream link (/stream/<token>, /dl/<token>) can carry its own
password, replacing the single global STREAM_PASS for that one link.
Passwords live in MongoDB (wzfix_streampass) so they survive restarts
and are shared live between the Telegram command and the stream
server (same process).

Behavior:
- link WITH a custom password → that password unlocks it; the global
  STREAM_PASS does NOT. Checked on every page/meta/data request.
- link WITHOUT → completely unchanged legacy behavior.

Owner/sudo command:
  /streampass set <url-or-token> <password>
  /streampass del <url-or-token>
  /streampass list

Fail-open: if MongoDB is unreachable the gate lets requests through —
a storage outage must never take streams down.
"""

import re
import time

from bot import LOGGER

_log = LOGGER.info if LOGGER else print

_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{4,128}$")
_MAX_PASS = 64


def _db():
    from ..ext_utils.db_handler import database

    return database.db


def _part():
    from ...core.tg_client import db_partition_id
    from ...core.config_manager import Config

    return db_partition_id(Config.BOT_TOKEN.split(":", 1)[0])


def extract_token(text):
    """Token from a pasted URL (/stream/TOKEN, /dl/TOKEN?x) or a bare token."""
    t = (text or "").strip()
    if not t:
        return ""
    t = t.split("?", 1)[0].split("#", 1)[0]
    if "/" in t:
        t = t.rstrip("/").split("/")[-1]
    return t if _TOKEN_RE.match(t) else ""


def path_token(request):
    """Stream token from an aiohttp request path (/_stream/TOKEN etc.)."""
    try:
        seg = [p for p in request.path.split("/") if p]
        tok = seg[-1] if seg else ""
        return tok if _TOKEN_RE.match(tok) else ""
    except Exception:
        return ""


async def get_link_pass(token):
    """Custom password for this link, or None when it has none."""
    try:
        doc = await _db().wzfix_streampass[_part()].find_one({"_id": token})
        p = (doc or {}).get("pass") or ""
        return p or None
    except Exception as e:
        _log(f"WZFIX r5: get_link_pass failed: {e}")
        return None


async def set_link_pass(token, password, by):
    await _db().wzfix_streampass[_part()].update_one(
        {"_id": token},
        {"$set": {"pass": password, "by": by, "at": time.time()}},
        upsert=True,
    )


async def del_link_pass(token):
    try:
        r = await _db().wzfix_streampass[_part()].delete_one({"_id": token})
        return r.deleted_count > 0
    except Exception:
        return False


async def all_link_passes():
    cur = (
        _db().wzfix_streampass[_part()]
        .find({}, {"_id": 1, "pass": 1, "at": 1})
        .sort("at", -1)
        .limit(50)
    )
    return [d async for d in cur]


def verify_link_token(token_value, password):
    """HMAC check of a submitted auth token against a link password."""
    try:
        from ..user_stream_module import _verify_token

        return _verify_token(token_value or "", password)
    except Exception:
        return False


async def serve_ok(request):
    """Per-link gate for the stream server. True = allow."""
    try:
        tok = path_token(request)
        if not tok:
            return True
        lp = await get_link_pass(tok)
        if lp is None:
            return True
        return verify_link_token(request.query.get("auth"), lp)
    except Exception:
        return True


# ─── owner command ─────────────────────────────────────────────────

_HELP = (
    "<b>Stream-link passwords</b>\n\n"
    "/streampass set <code><link></code> <code><password></code>"
    " — protect one stream link\n"
    "/streampass del <code><link></code> — remove it"
    " (falls back to the global password)\n"
    "/streampass list — links with their own password\n\n"
    "Paste the full stream link or just its token. A link with its own "
    "password no longer accepts the global one."
)


async def wzfix_streampass(client, message):
    args = (message.text or "").split()
    sub = (args[1].lower() if len(args) > 1 else "help")

    if sub in ("help", "start"):
        return await message.reply_text(_HELP)

    if sub == "list":
        try:
            rows = await all_link_passes()
        except Exception as e:
            return await message.reply_text(f"❌ DB error: {e}")
        if not rows:
            return await message.reply_text(
                "No links have a custom password — "
                "everything uses the global STREAM_PASS."
            )
        out = ["<b>Custom stream passwords</b>\n"]
        for d in rows:
            out.append(
                f"• <code>{d['_id']}</code> → <code>{d['pass']}</code>"
            )
        out.append(f"\n{len(rows)} link(s)")
        return await message.reply_text("\n".join(out))

    if sub == "set":
        if len(args) < 4:
            return await message.reply_text(
                "Usage: /streampass set <link> <password>"
            )
        tok = extract_token(args[2])
        pw = args[3]
        if not tok:
            return await message.reply_text(
                "❌ That doesn't look like a stream link or token."
            )
        if len(pw) > _MAX_PASS:
            return await message.reply_text(
                f"❌ Password too long (max {_MAX_PASS} chars, no spaces)."
            )
        try:
            await set_link_pass(tok, pw, message.from_user.id)
        except Exception as e:
            return await message.reply_text(f"❌ DB error: {e}")
        _log(f"WZFIX r5: stream pass set for {tok}")
        return await message.reply_text(
            f"✅ Locked.\n<code>{tok}</code> now needs the password "
            f"<code>{pw}</code> — the global password no longer opens it. "
            "Applies immediately; delete the message to hide the password."
        )

    if sub in ("del", "delete", "rm"):
        if len(args) < 3:
            return await message.reply_text("Usage: /streampass del <link>")
        tok = extract_token(args[2])
        if not tok:
            return await message.reply_text(
                "❌ That doesn't look like a stream link or token."
            )
        removed = await del_link_pass(tok)
        return await message.reply_text(
            f"✅ Removed — <code>{tok}</code> is back to the global password."
            if removed
            else f"ℹ️ <code>{tok}</code> had no custom password."
        )

    return await message.reply_text(_HELP)
