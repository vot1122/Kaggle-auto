#!/usr/bin/env python3
"""One-shot patch: v15.72 -> v15.73 (Round 14).

Research-backed fixes:
- JioSaavn full-discography fallback: paged search.getResults (40/page,
  up to 5 pages), primary-artist-id filtered — replaces the ~10-song
  topSongs-only list, so artist batches actually get the discography.
- playlist prompt hardening: the watcher re-checks the collected song
  list for up to a minute and logs every step.
- progress bar in the artist-batch note (one block per 10%).
- -z stripped from fan-out clones (a zip would upload the whole batch
  a second time).
- one-time upload-config log so /log shows the upload path.
- J-31: the uploader's "Deleted Cmd Message!" warning now falls back
  silently to the original command message (fan-out clones carry fake
  message ids, so it fired on every batch).
"""
import ast
import base64
import hashlib
import re
import sys

INPUT_SHA256 = "b8088ad7704429e3477c8ebd856e5322a059fcd2cb3d48626d22b78c908b1115"
R3_IN_SHA256 = "fc7400e8eaa6e53a19a1e087a6647935500555c123ae47e5802d2d785b67668b"
EXPECT_SHA256 = "ca83da5d170a694361752ca6bd0741f273e8b02b89f56d92a0bb785c2384c404"

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.73" in s:
    print("already v15.73 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.72 build")

# ── extract and upgrade the r3 music module ────────────────────────────
m = re.search(r"WZFIX_R3_MUSIC_B64 = \(\n(?:.*\n)*?\)\n", s)
assert m, "r3 blob block not found"
ns = {}
exec(m.group(0), ns)
r3 = base64.b64decode(ns["WZFIX_R3_MUSIC_B64"]).decode("utf-8")
if hashlib.sha256(r3.encode()).hexdigest() != R3_IN_SHA256:
    sys.exit("embedded r3 module is not the expected v15.72 build")

NEW_SAAVN = '''async def _saavn_artist_tracks(name, out, limit):
    """WZFIX r14 (v15.73): JioSaavn full-discography fallback.

    When Spotify's catalog API is unavailable (no app credentials and
    the anonymous token quota-blocked), look the artist up on JioSaavn
    by the exact name taken from the Spotify embed page and extend the
    list with the artist's songs: paged search results
    (search.getResults, 40 per page), keeping only songs whose
    PRIMARY artist matches — most-played first. No authentication, no
    keys. Adds nothing when JioSaavn has no artist with that exact
    name (international artists — Deezer takes over next).
    """
    global _CATALOG_NOTE
    if not name or len(out) >= limit:
        return out
    import urllib.parse

    def _prim(s):
        return ((s.get("more_info") or {}).get("artistMap") or {}).get(
            "primary_artists"
        ) or []

    q = urllib.parse.quote(name)
    # page 1 doubles as the artist-id search
    r = await asyncio.to_thread(
        _json_get,
        "https://www.jiosaavn.com/api.php?__call=search.getResults"
        f"&q={q}&_format=json&_marker=0&api_version=4&ctx=web6dot0"
        "&n=40&p=1",
    )
    counts = {}
    for s in r.get("results") or []:
        for pa in _prim(s):
            if (pa.get("name") or "").strip().lower() == name.strip().lower():
                _id = str(pa.get("id") or "")
                if _id:
                    counts[_id] = counts.get(_id, 0) + 1
    if not counts:
        _log(f"WZFIX music: saavn has no artist '{name}' — skip")
        return out
    aid = max(counts, key=counts.get)
    _log(
        f"WZFIX music: saavn artist '{name}' id {aid} "
        f"(page 1: {len(r.get('results') or [])} results, "
        f"total {r.get('total')})"
    )
    seen = {_norm_key(_n) for _, _n in out}
    added = 0

    def _take(songs):
        nonlocal added
        for s in songs:
            if len(out) >= limit:
                return
            _t = str(s.get("title") or "").strip()
            if not _t:
                continue
            if not any(str(p.get("id") or "") == aid for p in _prim(s)):
                continue
            clean = f"{name} - {_t}"
            if _norm_key(clean) in seen:
                continue
            seen.add(_norm_key(clean))
            out.append((f"ytsearch5:{clean} audio", clean))
            added += 1

    # page-1 songs first (highest relevance), then more pages until the
    # discography is covered (cap: 5 pages = 200 search results)
    _take(r.get("results") or [])
    page = 2
    while len(out) < limit and page <= 5:
        await asyncio.sleep(0.6)
        r2 = await asyncio.to_thread(
            _json_get,
            "https://www.jiosaavn.com/api.php?__call=search.getResults"
            f"&q={q}&_format=json&_marker=0&api_version=4&ctx=web6dot0"
            f"&n=40&p={page}",
        )
        _res = r2.get("results") or []
        if not _res:
            break
        _take(_res)
        page += 1
    if added:
        _log(
            f"WZFIX music: saavn fallback +{added} song(s) for {name} "
            f"(artist id {aid}, {page - 1} page(s), list now {len(out)})"
        )
        _CATALOG_NOTE = (
            f"\\U0001f4c2 full discography via JioSaavn — Spotify catalog"
            f" was unavailable ({len(out)} songs)"
        )
    else:
        _log(
            f"WZFIX music: saavn found artist but no new songs "
            f"(list stays {len(out)})"
        )
    return out


'''
r3 = r3.replace(
    "# WZFIX Round 3 — music app integrations (v15.72).",
    "# WZFIX Round 3 — music app integrations (v15.73).",
    1,
)
i0 = r3.index("async def _saavn_artist_tracks")
i1 = r3.index("async def _deezer_artist_tracks")
r3 = r3[:i0] + NEW_SAAVN + r3[i1:]

w0 = r3.index("    if note is not None and _mids:")
w1 = r3.index("    return True", w0)
NEW_WATCH = '''    if note is not None and _mids:

        async def _watch():
            import time as _time

            _t0 = _time.time()
            _log(f"WZFIX r14: watcher started for {artist} ({_n} songs)")
            try:
                while _time.time() - _t0 < 2400:
                    await asyncio.sleep(_FANOUT_WATCH)
                    _td = _task_dict_ref()
                    if _td is None:
                        _log(
                            "WZFIX r14: watcher — task dict unavailable, "
                            "stop (prompt skipped)"
                        )
                        return
                    _left = [m for m in _mids if m in _td]
                    # WZFIX r11 (v15.70): failed songs fold into the
                    # note; at the end a playlist prompt is offered
                    _wz = _shared[f"/{_folder}"].get("_wzfix") or {}
                    _failed = _wz.get("failed") or []
                    _fl = (
                        f"\\n❌ unavailable ({len(_failed)}): "
                        + ", ".join(_failed[:8])
                        if _failed
                        else ""
                    )
                    _done = _n - len(_left)
                    _log(
                        f"WZFIX r14: watch — {_done}/{_n} finished, "
                        f"{len(_wz.get('pl') or [])} uploaded, "
                        f"{len(_failed)} failed"
                    )
                    if not _left:
                        try:
                            await note.edit_text(
                                f"🎧 <b>{artist}</b> — ✅ "
                                f"{_n - len(_failed)}/{_n} songs "
                                f"delivered{_fl}"
                            )
                        except Exception:
                            pass
                        # WZFIX r14 (v15.73): the playlist list is
                        # written by the leader task while it finishes —
                        # re-check for up to a minute before giving up
                        _pl_items = None
                        for _retry in range(4):
                            _pl_items = _wz.get("pl") or []
                            if _pl_items:
                                break
                            _log(
                                "WZFIX r14: playlist empty — retry "
                                f"{_retry + 1}/4 (waiting 20s)"
                            )
                            await asyncio.sleep(20)
                            _wz = (
                                _shared[f"/{_folder}"].get("_wzfix") or {}
                            )
                        if _pl_items:
                            try:
                                await _playlist_prompt(
                                    message, artist, _pl_items
                                )
                            except Exception as _e:
                                _log(
                                    "WZFIX r14: playlist prompt failed: "
                                    f"{_e}"
                                )
                        else:
                            _log(
                                "WZFIX r14: no playlist items after "
                                "retries — prompt skipped"
                            )
                        return
                    try:
                        # WZFIX r14 (v15.73): a visual progress bar —
                        # one block per 10% of the batch
                        _fill = int(round(10 * _done / max(_n, 1)))
                        _bar = "▰" * _fill + "▱" * (10 - _fill)
                        await note.edit_text(
                            f"🎧 <b>{artist}</b>\\n"
                            f"{_bar} {_done}/{_n} songs finished{_fl}\\n"
                            "📦 all files are sent together at the end"
                        )
                    except Exception:
                        pass
            except Exception as _e:
                _log(f"WZFIX r14: watcher error: {_e}")

        asyncio.create_task(_watch())
'''
r3 = r3[:w0] + NEW_WATCH + r3[w1:]

OLD_FLAGS = '''    _toks = (message.text or "").split()
    _flags = " ".join(
        t for t in _toks[1:] if not t.startswith("http")
    ).strip()'''
NEW_FLAGS = '''    _toks = (message.text or "").split()
    # WZFIX r14 (v15.73): -z would zip the whole batch AFTER every song
    # was already uploaded — a full second upload pass. Music batches
    # never zip, so the flag is stripped from every clone.
    _flags = " ".join(
        t
        for t in _toks[1:]
        if not t.startswith("http") and t.lower() not in ("-z", "-zip")
    ).strip()
    if any(t.lower() in ("-z", "-zip") for t in _toks[1:]):
        _log("WZFIX r14: -z stripped from the music batch (one upload pass)")
    global _CFG_LOGGED
    if not _CFG_LOGGED:
        _CFG_LOGGED = True
        try:
            from ... import Config as _C

            _log(
                "WZFIX r14: upload config — "
                f"LEECH_LOG_CHAT={getattr(_C, 'LEECH_LOG_CHAT', None)!r}, "
                f"BOT_PM={getattr(_C, 'BOT_PM', None)!r}, "
                f"MEDIA_STORE={getattr(_C, 'MEDIA_STORE', None)!r}, "
                f"USE_HYPER={getattr(_C, 'USE_HYPER', None)!r}, "
                f"user_session={bool(getattr(_C, 'USER_SESSION_STRING', ''))}"
            )
        except Exception:
            pass'''
assert OLD_FLAGS in r3
r3 = r3.replace(OLD_FLAGS, NEW_FLAGS, 1)
r3 = r3.replace(
    "_FANOUT_STAGGER = 1.0",
    "_CFG_LOGGED = False\n_FANOUT_STAGGER = 1.0",
    1,
)
OLD_DOC = '''    finished song moves into the last task's directory, so the final
    upload delivers everything together — one zip with -z. Flags from
    the user's command (-z, -n, ...) pass through to every task.'''
NEW_DOC = '''    finished song moves into the last task's directory, so the final
    upload delivers everything together — one upload pass, no zip.
    Flags from the user's command (-n, ...) pass through to every task;
    -z is stripped (it would upload the whole batch a second time).'''
assert OLD_DOC in r3
r3 = r3.replace(OLD_DOC, NEW_DOC, 1)
r3h = hashlib.sha256(r3.encode()).hexdigest()
if r3h != "89cc7b9c8eff58c441d8aa291ada54aec0e705cc38aafa55c9bb09e82311088c":
    sys.exit(f"transformed r3 SHA MISMATCH - got {r3h}")

blob = base64.b64encode(r3.encode("utf-8")).decode("ascii")
chunks = [blob[i : i + 104] for i in range(0, len(blob), 104)]
new_block = 'WZFIX_R3_MUSIC_B64 = (\n' + "".join(
    f'    "{c}"\n' for c in chunks
) + ")\n"
s = s[: m.start()] + new_block + s[m.end() :]

# ── insert addition J-31 (silent cmd-message fallback) ────────────────
J31 = '''    # J-31: silent cmd-message fallback (v15.73) — the uploader's
    # "Deleted Cmd Message! Don't delete the cmd message again!" warning
    # fires for EVERY fan-out clone (clones carry fake message ids the
    # chat never had), so artist batches spam it. Fall back silently to
    # the original command message instead.
    try:
        _p31 = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/upload_utils/"
            "telegram_uploader.py",
        )
        with open(_p31, "r", encoding="utf-8") as f:
            _t31 = f.read()
        if "WZFIX r14 silent cmd fallback" not in _t31:
            _old31 = (
                "            if self._sent_msg is None or "
                "self._sent_msg.chat is None:\\n"
                "                try:\\n"
                "                    self._sent_msg = await "
                "_call_with_flood_retry(\\n"
                "                        self._listener.client.send_message,"
                "\\n"
                "                        chat_id="
                "self._listener.message.chat.id,\\n"
                "                        text=\"Deleted Cmd Message! "
                "Don't delete the cmd message again!\\",\\n"
                "                        disable_web_page_preview=True,"
                "\\n"
                "                        disable_notification=True,\\n"
                "                    )\\n"
                "                except Exception:\\n"
                "                    self._sent_msg = "
                "self._listener.message"
            )
            _new31 = (
                "            if self._sent_msg is None or "
                "self._sent_msg.chat is None:\\n"
                "                # WZFIX r14 silent cmd fallback (v15.73):"
                " fan-out\\n"
                "                # clones carry fake message ids, so this "
                "lookup\\n"
                "                # always fails — reply to the original "
                "command\\n"
                "                # message instead of spamming the chat "
                "with a\\n"
                "                # \"Deleted Cmd Message!\" warning\\n"
                "                LOGGER.info(\\n"
                "                    \"WZFIX r14: upload replies to the "
                "original \\"\\n"
                "                    \"cmd message (clone/deleted cmd)\""
                "\\n"
                "                )\\n"
                "                self._sent_msg = self._listener.message"
            )
            if _old31 in _t31:
                _t31 = _t31.replace(_old31, _new31, 1)
                with open(_p31, "w", encoding="utf-8") as f:
                    f.write(_t31)
                _r31 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p31],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r31.returncode == 0:
                    log("  r2: J-31 silent cmd fallback applied")
                else:
                    log(
                        f"  r2: J-31 compile FAILED — "
                        f"{(_r31.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-31 anchor missing (uploader)", "WARN")
        else:
            log("  r2: J-31 already applied")
    except Exception as e:
        log(f"  r2: J-31 patch FAILED — {e}", "ERROR")

'''
anchor = (
    "    # J-1: register /wzadmin routes on the bot stream server "
    "(in-process)\n"
)
assert s.count(anchor) == 1, "J-1 anchor count wrong"
s = s.replace(anchor, J31 + anchor, 1)

n = s.count("v15.72")
s = s.replace("v15.72", "v15.73")
assert "v15.72" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r14 full discography + quiet fallback + logs, {n} markers bumped to v15.73")
