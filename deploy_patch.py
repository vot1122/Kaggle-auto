#!/usr/bin/env python3
"""One-shot patch: v15.73 -> v15.74 (Round 15).

Speed + single-copy delivery for artist batches:
- deterministic fan-out merge (J-32): every clone moves its song into
  the leader folder and finishes quietly; only the last task uploads,
  so each song reaches the chat exactly ONCE (was 4 copies).
- single-copy DM (J-33): music batches skip the BOT_PM duplicate.
- m4a passthrough (J-34 + format change): no more aac->mp3 transcode —
  native YouTube m4a audio is copied into the container: faster,
  lighter, better quality.
- concurrency-capped launcher: clones bypass the bot's download queue
  (-f) and the r3 module keeps active downloads at WZFIX_FANOUT_JOBS
  (default 16, 1-40). The ceiling is YouTube's per-IP throttling, not
  RAM: 4-8 is the unauthenticated sweet spot, 16 with cookies is
  reasonable; tune with the env before going higher.
- watcher: the batch only counts as finished once tasks were actually
  seen running (queued tasks no longer trigger the completion branch),
  so the playlist prompt appears after the real upload.
- J-30b: failed songs also count toward merge completion.
"""
import ast
import base64
import hashlib
import re
import sys

INPUT_SHA256 = "9ae9394dea8fc729737951496307b1886884626886b38dba88c58e6eb8e288ca"
R3_IN_SHA256 = "89cc7b9c8eff58c441d8aa291ada54aec0e705cc38aafa55c9bb09e82311088c"
R3_OUT_SHA256 = "9d25181e4f51dddfd3f30949c3a578005f6715ec1d2c344a6a7ae43dd7791c44"
EXPECT_SHA256 = "fe1d9612f15d4af6e003a15ae02c3057a4b62796ea8e83cb3295cfbcf47ff67c"

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.74" in s:
    print("already v15.74 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.73 build")

# ── extract and upgrade the r3 music module ────────────────────────────
m = re.search(r"WZFIX_R3_MUSIC_B64 = \(\n(?:.*\n)*?\)\n", s)
assert m, "r3 blob block not found"
ns = {}
exec(m.group(0), ns)
r3 = base64.b64decode(ns["WZFIX_R3_MUSIC_B64"]).decode("utf-8")
if hashlib.sha256(r3.encode()).hexdigest() != R3_IN_SHA256:
    sys.exit("embedded r3 module is not the expected v15.73 build")

HELPERS = '''def _fanout_jobs():
    """WZFIX r15 (v15.74): concurrent-download target for artist
    batches. The ceiling is YouTube's per-IP throttling (not RAM) --
    4-8 is the unauthenticated sweet spot; 16 with cookies is a
    reasonable default. Tune via the WZFIX_FANOUT_JOBS env (1-40)."""
    try:
        import os as _os

        _j = int(_os.environ.get("WZFIX_FANOUT_JOBS", "16"))
    except Exception:
        _j = 16
    return max(1, min(_j, 40))


def _active_dl_count():
    try:
        from ... import non_queued_dl, queued_dl

        return len(non_queued_dl) + len(queued_dl)
    except Exception:
        return None


def _sanitize_folder(name):'''

r3 = r3.replace(
    "# WZFIX Round 3 — music app integrations (v15.73).",
    "# WZFIX Round 3 — music app integrations (v15.74).",
    1,
)
assert "def _fanout_jobs()" not in r3
r3 = r3.replace("def _sanitize_folder(name):", HELPERS, 1)

OLD_WZ = '''            "_wzfix": {"artist": artist, "failed": [], "pl": []},'''
NEW_WZ = '''            "_wzfix": {
                "artist": artist,
                "failed": [],
                "pl": [],
                # WZFIX r15 (v15.74): deterministic merge — every clone
                # moves its song into the leader's folder; the task
                # that finishes last uploads everything exactly once
                "total": _n,
                "done": set(),
                "leader_mid": None,
            },'''
assert OLD_WZ in r3
r3 = r3.replace(OLD_WZ, NEW_WZ, 1)

OLD_LAUNCH = '''    _base = int(getattr(message, "id", 0) or 0)
    for i, (query, clean) in enumerate(tracks):
        try:
            m2 = _copy.copy(message)'''
NEW_LAUNCH = '''    _base = int(getattr(message, "id", 0) or 0)
    # WZFIX r15 (v15.74): the first clone's dir is the merge target
    _shared[f"/{_folder}"]["_wzfix"]["leader_mid"] = _base + 100000
    _jobs = _fanout_jobs()
    _log(
        f"WZFIX r15: fan-out target {_jobs} concurrent download(s) "
        "(WZFIX_FANOUT_JOBS)"
    )
    for i, (query, clean) in enumerate(tracks):
        try:
            # WZFIX r15: launch the next clone only when a download
            # slot is free — self-regulating, no queue bottleneck and
            # no per-IP hammering beyond the target
            while True:
                _ac = _active_dl_count()
                if _ac is None or _ac < _jobs:
                    break
                await asyncio.sleep(0.5)
            m2 = _copy.copy(message)'''
assert OLD_LAUNCH in r3
r3 = r3.replace(OLD_LAUNCH, NEW_LAUNCH, 1)

OLD_TEXT = '''            m2.text = (
                f"/yl {query} {_flags} -m {_folder} -i 1"
            ).strip()'''
NEW_TEXT = '''            # -f (force run) bypasses the bot's download queue: the
            # r3 launcher above is the only concurrency limit
            m2.text = (
                f"/yl {query} {_flags} -m {_folder} -i 1 -f"
            ).strip()'''
assert OLD_TEXT in r3
r3 = r3.replace(OLD_TEXT, NEW_TEXT, 1)

OLD_STAG = '''            asyncio.create_task(_go())
            await asyncio.sleep(_FANOUT_STAGGER)'''
NEW_STAG = '''            asyncio.create_task(_go())
            await asyncio.sleep(0.25)'''
assert OLD_STAG in r3
r3 = r3.replace(OLD_STAG, NEW_STAG, 1)

w0 = r3.index("    if note is not None and _mids:")
w1 = r3.index("    return True", w0)
NEW_WATCH = '''    if note is not None and _mids:

        async def _watch():
            import time as _time

            _t0 = _time.time()
            _started = False
            _log(f"WZFIX r15: watcher started for {artist} ({_n} songs)")
            try:
                while _time.time() - _t0 < 3600:
                    await asyncio.sleep(_FANOUT_WATCH)
                    _td = _task_dict_ref()
                    if _td is None:
                        _log(
                            "WZFIX r15: watcher — task dict unavailable, "
                            "stop (prompt skipped)"
                        )
                        return
                    _left = [m for m in _mids if m in _td]
                    if _left:
                        _started = True
                    _wz = _shared[f"/{_folder}"].get("_wzfix") or {}
                    _failed = _wz.get("failed") or []
                    _merged = len(_wz.get("done") or [])
                    _fl = (
                        f"\\n❌ unavailable ({len(_failed)}): "
                        + ", ".join(_failed[:8])
                        if _failed
                        else ""
                    )
                    _done = _n - len(_left)
                    _log(
                        f"WZFIX r15: watch — {_done}/{_n} finished, "
                        f"{_merged} merged, "
                        f"{len(_wz.get('pl') or [])} uploaded, "
                        f"{len(_failed)} failed, "
                        f"active={len(_left)}"
                    )
                    # WZFIX r15: only call the batch complete once the
                    # tasks were actually SEEN in the task dict —
                    # before the first download starts the clones are
                    # merely queued and must not count as finished
                    if _started and not _left:
                        try:
                            await note.edit_text(
                                f"🎧 <b>{artist}</b> — ✅ "
                                f"{_n - len(_failed)}/{_n} songs "
                                f"delivered{_fl}"
                            )
                        except Exception:
                            pass
                        # the leader fills the playlist list while it
                        # uploads — re-check before giving up
                        _pl_items = None
                        for _retry in range(6):
                            _pl_items = _wz.get("pl") or []
                            if _pl_items:
                                break
                            _log(
                                "WZFIX r15: playlist empty — retry "
                                f"{_retry + 1}/6 (waiting 20s)"
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
                                    "WZFIX r15: playlist prompt failed: "
                                    f"{_e}"
                                )
                        else:
                            _log(
                                "WZFIX r15: no playlist items after "
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
                _log(f"WZFIX r15: watcher error: {_e}")

        asyncio.create_task(_watch())
'''
r3 = r3[:w0] + NEW_WATCH + r3[w1:]
r3h = hashlib.sha256(r3.encode()).hexdigest()
if r3h != R3_OUT_SHA256:
    sys.exit(f"transformed r3 SHA MISMATCH - got {r3h}")

blob = base64.b64encode(r3.encode("utf-8")).decode("ascii")
chunks = [blob[i : i + 104] for i in range(0, len(blob), 104)]
new_block = 'WZFIX_R3_MUSIC_B64 = (\n' + "".join(
    f'    "{c}"\n' for c in chunks
) + ")\n"
s = s[: m.start()] + new_block + s[m.end() :]

# ── J-28: music format m4a passthrough ────────────────────────────────
old_q = 'qual = "ba/b-mp3-320"'
new_q = 'qual = "ba[ext=m4a]/ba/b-m4a-5"'
assert s.count(old_q) == 1, f"J-28 qual anchor count {s.count(old_q)}"
s = s.replace(old_q, new_q, 1)

# ── J-30b: failed songs count toward merge completion ─────────────────
old_30b = '''            if _wzfix_sd.get("_wzfix") is not None:
                _wzfix_quiet = True
                _wzfix_sd["_wzfix"].setdefault("failed", []).append('''
new_30b = '''            if _wzfix_sd.get("_wzfix") is not None:
                _wzfix_quiet = True
                # WZFIX r15: a failed song still counts as "done" so
                # the leader upload can trigger without it
                _wzfix_sd["_wzfix"].setdefault("done", set()).add(
                    self.mid
                )
                _wzfix_sd["_wzfix"].setdefault("failed", []).append('''
assert s.count(old_30b) == 1, "J-30b anchor count"
s = s.replace(old_30b, new_30b, 1)

# ── insert additions J-32/33/34 before the J-1 anchor ────────────────
J32 = r'''    # J-32: deterministic fan-out merge (v15.74) — artist-batch clones
    # move their song into the leader's folder and finish quietly;
    # only the task that finishes last uploads, so every song reaches
    # the chat exactly once (this replaces the fragile upstream
    # same_dir race that produced duplicate uploads)
    try:
        _p32 = os.path.join(
            WZMLX_DIR, "bot/helper/listeners/task_listener.py"
        )
        with open(_p32, "r", encoding="utf-8") as f:
            _t32 = f.read()
        if "WZFIX r15 deterministic merge" not in _t32:
            _old32 = (
                "        multi_links = False\n"
                "        if (\n"
            )
            _new32 = r"""        multi_links = False
        # WZFIX r15 deterministic merge: music fan-out clones move
        # their song into the leader folder; the LAST task to finish
        # uploads everything exactly once
        try:
            _wz15 = (self.same_dir or {}).get(self.folder_name, {}).get(
                "_wzfix"
            )
        except Exception:
            _wz15 = None
        if _wz15 is not None and _wz15.get("leader_mid"):
            async with same_directory_lock:
                import os as _os15

                _sp15 = _os15.path.normpath(
                    f"{self.dir}{self.folder_name}"
                )
                if _os15.path.isdir(_sp15):
                    _dp15 = _os15.path.normpath(
                        f"{DOWNLOAD_DIR}{_wz15['leader_mid']}"
                        f"{self.folder_name}"
                    )
                    if _dp15 != _sp15:
                        await move_and_merge(_sp15, _dp15, self.mid)
                        LOGGER.info(
                            "WZFIX r15: merged a song into the leader "
                            "folder"
                        )
                _wz15.setdefault("done", set()).add(self.mid)
                if len(_wz15["done"]) >= _wz15.get("total", 10**9):
                    LOGGER.info(
                        "WZFIX r15: all songs merged — single upload "
                        "from the leader folder"
                    )
                    self.dir = f"{DOWNLOAD_DIR}{_wz15['leader_mid']}"
                else:
                    await self.on_upload_error(
                        f"{self.name} Downloaded!\n\nWaiting for other "
                        "tasks to finish..."
                    )
                    return
        if (
"""
            if _old32 in _t32:
                _t32 = _t32.replace(_old32, _new32, 1)
                with open(_p32, "w", encoding="utf-8") as f:
                    f.write(_t32)
                _r32 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p32],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r32.returncode == 0:
                    log("  r2: J-32 deterministic fan-out merge applied")
                else:
                    log(
                        f"  r2: J-32 compile FAILED — "
                        f"{(_r32.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-32 anchor missing (task_listener)", "WARN")
        else:
            log("  r2: J-32 already applied")
    except Exception as e:
        log(f"  r2: J-32 patch FAILED — {e}", "ERROR")

'''

J33 = r'''    # J-33: single-copy DM (v15.74) — with BOT_PM on and the batch
    # running in the user's DM, every song was sent to the DM twice
    # (upload reply + PM copy). Music batches keep just the reply.
    try:
        _p33 = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/upload_utils/"
            "telegram_uploader.py",
        )
        with open(_p33, "r", encoding="utf-8") as f:
            _t33 = f.read()
        if "WZFIX r15 single-copy DM" not in _t33:
            _old33 = (
                "        await self._user_settings()\n"
                "        res = await self._msg_to_reply()\n"
            )
            _new33 = (
                "        await self._user_settings()\n"
                "        # WZFIX r15 single-copy DM: music batches skip\n"
                "        # the BOT_PM duplicate (the reply copy is\n"
                "        # already in the user's chat)\n"
                "        if getattr(self._listener, "
                '"_wzfix_music", False):\n'
                "            self._bot_pm = False\n"
                "        res = await self._msg_to_reply()\n"
            )
            if _old33 in _t33:
                _t33 = _t33.replace(_old33, _new33, 1)
                with open(_p33, "w", encoding="utf-8") as f:
                    f.write(_t33)
                _r33 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p33],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r33.returncode == 0:
                    log("  r2: J-33 single-copy DM applied")
                else:
                    log(
                        f"  r2: J-33 compile FAILED — "
                        f"{(_r33.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-33 anchor missing (uploader)", "WARN")
        else:
            log("  r2: J-33 already applied")
    except Exception as e:
        log(f"  r2: J-33 patch FAILED — {e}", "ERROR")

'''

J34 = r'''    # J-34: m4a passthrough (v15.74) — every song was transcoded to
    # mp3 (aac -> mp3 re-encode: CPU-bound and quality-losing). Music
    # now downloads YouTube's native m4a audio and copies it into the
    # container (no transcode): faster, lighter and better quality.
    try:
        _p34 = os.path.join(
            WZMLX_DIR,
            "bot/helper/mirror_leech_utils/download_utils/"
            "yt_dlp_download.py",
        )
        with open(_p34, "r", encoding="utf-8") as f:
            _t34 = f.read()
        if "WZFIX r15 m4a passthrough" not in _t34:
            _old34 = (
                '        if qual.startswith("ba/b-"):\n'
                '            audio_info = qual.split("-")\n'
            )
            _new34 = (
                '        # WZFIX r15 m4a passthrough: format strings may'
                ' carry\n'
                '        # a preferred-audio prefix (ba[ext=m4a]/...)'
                ' before\n'
                '        # the ba/b-<codec>-<rate> tail\n'
                '        if "/ba/b-" in qual or '
                'qual.startswith("ba/b-"):\n'
                '            _pfx34, _sfx34 = qual.split("/b-", 1)\n'
                '            audio_info = _sfx34.split("-")\n'
                '            qual = f"{_pfx34}/b"\n'
            )
            if _old34 in _t34:
                _t34 = _t34.replace(_old34, _new34, 1)
                with open(_p34, "w", encoding="utf-8") as f:
                    f.write(_t34)
                _r34 = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p34],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if _r34.returncode == 0:
                    log("  r2: J-34 m4a passthrough applied")
                else:
                    log(
                        f"  r2: J-34 compile FAILED — "
                        f"{(_r34.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: J-34 anchor missing (yt_dlp_download)", "WARN")
        else:
            log("  r2: J-34 already applied")
    except Exception as e:
        log(f"  r2: J-34 patch FAILED — {e}", "ERROR")

'''

anchor = (
    "    # J-1: register /wzadmin routes on the bot stream server "
    "(in-process)\n"
)
assert s.count(anchor) == 1, "J-1 anchor count wrong"
s = s.replace(anchor, J32 + J33 + J34 + anchor, 1)

# ── version bump + final checks ────────────────────────────────────────
n = s.count("v15.73")
s = s.replace("v15.73", "v15.74")
assert "v15.73" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r15 single-copy + parallel + m4a, {n} markers bumped to v15.74")
