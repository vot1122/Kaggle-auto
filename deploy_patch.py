#!/usr/bin/env python3
"""One-shot patch: v15.75 -> v15.76 (Round 17).

Three fixes reported after R16 went live:
  1. download password page: the public /dl/ route is proxied by
     wserver with a fresh header set - the browser's Accept
     header never reaches the stream gate, so it now keys on
     the download kind itself. The page also posted to the
     internal-only /_auth; it now uses the public
     /api/stream_auth route (same as the player modal).
  2. playlist card under an artist batch never appeared: the
     watcher waited only 2 minutes for the leader's single
     upload of the whole batch - it now polls for 30 minutes.
  3. log channel flood: one exempt message per downloaded
     song; admin notes are now aggregated into ONE message
     per burst of tasks.
"""
import ast
import hashlib
import sys

INPUT_SHA256 = "0fc74812e816e0994e11c560229bdad5694477c03b080757a93bd96b098cb7f0"
EXPECT_SHA256 = "ec00356f52068bd91f2f00c8b5b9122293de2355037b0263ef08507126196bae"

GATE_OLD = (
    '        if "text/html" in (request.headers.get("Accept") or ""):',
    '            return _dl_auth_page()',
)

GATE_NEW = (
    '        # WZFIX_R17: the public /dl/ route is proxied by wserver',
    '        # with a fresh header set - Accept never reaches here, so',
    '        # key on the download kind instead. Player fetches use the',
    '        # playback kind and keep the raw 401 that drives the',
    '        # in-page password modal.',
    '        if kind == "download":',
    '            return _dl_auth_page()',
)

WATCH_OLD = (
    '                        _pl_items = None',
    '                        for _retry in range(6):',
    '                            _pl_items = _wz.get("pl") or []',
    '                            if _pl_items:',
    '                                break',
    '                            _log(',
    '                                "WZFIX r15: playlist empty — retry "',
    '                                f"{_retry + 1}/6 (waiting 20s)"',
    '                            )',
    '                            await asyncio.sleep(20)',
    '                            _wz = (',
    '                                _shared[f"/{_folder}"].get("_wzfix") or {}',
    '                            )',
)

WATCH_NEW = (
    '                        _pl_items = None',
    '                        # WZFIX_R17: the leader uploads the whole',
    '                        # batch in ONE task and that takes far longer',
    '                        # than 2 minutes - poll for up to 30 minutes',
    '                        for _retry in range(60):',
    '                            _wz = (',
    '                                _shared[f"/{_folder}"].get("_wzfix") or {}',
    '                            )',
    '                            _pl_items = _wz.get("pl") or []',
    '                            if _pl_items:',
    '                                break',
    '                            if _retry % 5 == 0:',
    '                                _log(',
    '                                    "WZFIX r17: playlist still empty — "',
    '                                    f"retry {_retry + 1}/60 (waiting 30s)"',
    '                                )',
    '                            await asyncio.sleep(30)',
)

TM_OLD = (
    '            await admin_log(',
    '                "%K% <b>Exempt task — no quota check</b>",',
    '                f"┏ <b>User</b> → {_who}%N%"',
    '                f"┠ <b>Where</b> → {_ct} {_cn[:60]}%N%"',
    '                f"┖ Owner/sudo are exempt by design",',
    '            )',
)

TM_NEW = (
    '            from ..wzfix.r1_core import agg_note',
    '',
    '            _nl17 = chr(10)',
    '            await agg_note(',
    '                f"exempt:{_fu.id if _fu else 0}",',
    '                "👑 <b>Exempt tasks — no quota check</b>",',
    '                "┏ <b>User</b> → " + _who + _nl17',
    '                + "┠ <b>Where</b> → " + _ct + " " + _cn[:60] + _nl17',
    '                + "┖ Owner/sudo are exempt — no bandwidth charged",',
    '            )',
)

RC_OLD = (
    '            await admin_log(',
    '                "⚠️ <b>Task without pre-checkable link</b>",',
    '                f"┏ <b>User</b> → <code>{user_id}</code>%N%"',
    '                f"┠ <b>Where</b> → {_ctype} {_cname[:60]}%N%"',
    '                f"┠ <b>Links seen</b> → {len(links)}%N%"',
    '                f"┖ Falls back to the in-download check — the download "',
    '                f"is still held and verified",',
    '            )',
    '            return None',
)

RC_NEW = (
    '            await agg_note(',
    '                f"nopre:{user_id}",',
    '                "⚠️ <b>Tasks without pre-checkable link</b>",',
    '                "┏ <b>User</b> → <code>" + str(user_id) + "</code>" + _NL17',
    '                + "┠ <b>Where</b> → " + _ctype + " " + _cname[:60] + _NL17',
    '                + "┠ <b>Links seen</b> → " + str(len(links)) + _NL17',
    '                + "┖ Falls back to the in-download check — still verified",',
    '            )',
    '            return None',
)

AGG_APPEND = (
    '',
    '',
    '# WZFIX_R17 (v15.76): aggregated admin notes. A 70-song fan-out fired',
    '# 70 separate "Exempt task" channel messages; agg_note counts a burst',
    '# of tasks and flushes ONE message ~45s after the last event.',
    '',
    '_AGG17 = {}',
    '_NL17 = chr(10)',
    '',
    '',
    'async def agg_note(key, title, body):',
    '    try:',
    '        import asyncio as _aio17',
    '',
    '        _st = _AGG17.get(key)',
    '        if _st is None:',
    '            _st = _AGG17[key] = {"n": 0}',
    '        _st["n"] += 1',
    '        _t = _st.get("t")',
    '        if _t is not None and not _t.done():',
    '            _t.cancel()',
    '        _st["ttl"] = title',
    '        _st["bd"] = body',
    '',
    '        async def _fl17():',
    '            try:',
    '                await _aio17.sleep(45)',
    '            except _aio17.CancelledError:',
    '                return',
    '            _it = _AGG17.pop(key, None)',
    '            if not _it:',
    '                return',
    '            try:',
    '                await admin_log(',
    '                    _it.get("ttl") or "WZFIX note",',
    '                    "┠ <b>Tasks</b> → " + str(_it["n"]) + _NL17',
    '                    + (_it.get("bd") or ""),',
    '                )',
    '            except Exception:',
    '                pass',
    '',
    '        _st["t"] = _aio17.get_running_loop().create_task(_fl17())',
    '    except Exception as _e17:',
    '        LOGGER.error(f"WZFIX agg_note failed: {_e17}")',
)

def _c17(lines, ind):
    out = ""
    for ln in lines:
        out = out + " " * ind + chr(39) + ln + chr(92) + "n" + chr(39) + chr(10)
    return out

R17 = (
    '    # R17 (v15.76): three fixes reported after R16 — the download\n'
    + "    # password page never showed (wserver's /dl/ proxy strips the\n"
    + '    # Accept header, so the gate now keys on the download kind; the\n'
    + '    # page also posted to the internal-only /_auth and now uses the\n'
    + '    # public /api/stream_auth route), the playlist card never\n'
    + '    # appeared (the watcher gave up after 2 minutes while the single\n'
    + '    # leader upload takes much longer — now 30 minutes), and the log\n'
    + '    # channel flooded with one exempt message per song (admin notes\n'
    + '    # are now aggregated into one message per burst of tasks).\n'
    + '    try:\n'
    + '        _nl17 = chr(10)\n'
    + '        _nph17 = chr(37) + chr(78) + chr(37)\n'
    + '        _kph17 = chr(37) + chr(75) + chr(37)\n'
    + '        _bs17 = chr(92) + "n"\n'
    + '        _bk17 = chr(92) + "U0001F451"\n'
    + '        _ss17 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")\n'
    + '        with open(_ss17, "r", encoding="utf-8") as _f:\n'
    + '            _s17 = _f.read()\n'
    + '        if "WZFIX_R17" in _s17:\n'
    + '            log("  r17: download page gate already present")\n'
    + '        else:\n'
    + '            _g17o = (\n'
    + _c17(GATE_OLD, 16)
    + '            ).replace(_nph17, _bs17).replace(_kph17, _bk17)\n'
    + '            _g17n = (\n'
    + _c17(GATE_NEW, 16)
    + '            ).replace(_nph17, _bs17)\n'
    + '            _ok17 = 0\n'
    + '            if _g17o in _s17:\n'
    + '                _s17 = _s17.replace(_g17o, _g17n, 1)\n'
    + '                _ok17 += 1\n'
    + '            else:\n'
    + '                log("  r17: gate anchor missing", "WARN")\n'
    + '            _fa17 = "await fetch(" + chr(34) + "/_auth" + chr(34)\n'
    + '            _fb17 = "await fetch(" + chr(34) + "/api/stream_auth" + chr(34)\n'
    + '            if _fa17 in _s17:\n'
    + '                _s17 = _s17.replace(_fa17, _fb17, 1)\n'
    + '                _ok17 += 1\n'
    + '            else:\n'
    + '                log("  r17: page auth route already public", "WARN")\n'
    + '            if _ok17 == 2:\n'
    + '                with open(_ss17, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_s17)\n'
    + '                _r17 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _ss17],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r17.returncode == 0:\n'
    + '                    log("  r17: download page gate fixed (kind + public auth)")\n'
    + '                else:\n'
    + '                    log("  r17: stream_server compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log("  r17: stream_server edits incomplete - not written", "WARN")\n'
    + '        _r3p17 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r3_music.py")\n'
    + '        with open(_r3p17, "r", encoding="utf-8") as _f:\n'
    + '            _r317 = _f.read()\n'
    + '        if "WZFIX_R17" in _r317:\n'
    + '            log("  r17: playlist wait already extended")\n'
    + '        else:\n'
    + '            _w17o = (\n'
    + _c17(WATCH_OLD, 16)
    + '            ).replace(_nph17, _bs17)\n'
    + '            _w17n = (\n'
    + _c17(WATCH_NEW, 16)
    + '            ).replace(_nph17, _bs17)\n'
    + '            if _w17o in _r317:\n'
    + '                _r317 = _r317.replace(_w17o, _w17n, 1)\n'
    + '                with open(_r3p17, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_r317)\n'
    + '                _r17 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _r3p17],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r17.returncode == 0:\n'
    + '                    log("  r17: playlist wait extended (30 min)")\n'
    + '                else:\n'
    + '                    log("  r17: r3_music compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log("  r17: watcher anchor missing", "WARN")\n'
    + '        _tm17 = os.path.join(WZMLX_DIR, "bot/helper/ext_utils/task_manager.py")\n'
    + '        with open(_tm17, "r", encoding="utf-8") as _f:\n'
    + '            _t17 = _f.read()\n'
    + '        if "agg_note(" in _t17:\n'
    + '            log("  r17: exempt notes already aggregated")\n'
    + '        else:\n'
    + '            _t17o = (\n'
    + _c17(TM_OLD, 16)
    + '            ).replace(_nph17, _bs17).replace(_kph17, _bk17)\n'
    + '            _t17n = (\n'
    + _c17(TM_NEW, 16)
    + '            ).replace(_nph17, _bs17)\n'
    + '            if _t17o in _t17:\n'
    + '                _t17 = _t17.replace(_t17o, _t17n, 1)\n'
    + '                with open(_tm17, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_t17)\n'
    + '                _r17 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _tm17],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r17.returncode == 0:\n'
    + '                    log("  r17: exempt notes aggregated")\n'
    + '                else:\n'
    + '                    log("  r17: task_manager compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log("  r17: exempt anchor missing", "WARN")\n'
    + '        _rc17 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r1_core.py")\n'
    + '        with open(_rc17, "r", encoding="utf-8") as _f:\n'
    + '            _rcs17 = _f.read()\n'
    + '        if "def agg_note" in _rcs17:\n'
    + '            log("  r17: r1_core aggregator already present")\n'
    + '        else:\n'
    + '            _rc17o = (\n'
    + _c17(RC_OLD, 16)
    + '            ).replace(_nph17, _bs17)\n'
    + '            _rc17n = (\n'
    + _c17(RC_NEW, 16)
    + '            ).replace(_nph17, _bs17)\n'
    + '            if _rc17o in _rcs17:\n'
    + '                _rcs17 = _rcs17.replace(_rc17o, _rc17n, 1)\n'
    + '                log("  r17: precheck notes aggregated")\n'
    + '            else:\n'
    + '                log("  r17: precheck anchor missing", "WARN")\n'
    + '            _rcs17 = _rcs17 + _nl17 + _nl17 + (\n'
    + _c17(AGG_APPEND, 16)
    + '            ).replace(_nph17, _bs17)\n'
    + '            with open(_rc17, "w", encoding="utf-8") as _f:\n'
    + '                _f.write(_rcs17)\n'
    + '            _r17 = subprocess.run(\n'
    + '                [sys.executable, "-m", "py_compile", _rc17],\n'
    + '                capture_output=True,\n'
    + '                text=True,\n'
    + '                timeout=60,\n'
    + '            )\n'
    + '            if _r17.returncode == 0:\n'
    + '                log("  r17: admin note aggregator added")\n'
    + '            else:\n'
    + '                log("  r17: r1_core compile FAILED", "ERROR")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r17: FAILED — {e}", "ERROR")\n'
    + '\n'
)

frag = R17
ast.parse("def _wrap():" + chr(10) + frag + chr(10))

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.76" in s:
    print("already v15.76 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.75 build")

anchor = (
    '    log("WZML-X-Bot patch kit applied")' + chr(10)
)
assert s.count(anchor) == 1, f"anchor count {s.count(anchor)}"
s = s.replace(anchor, frag + anchor, 1)

n = s.count("v15.75")
s = s.replace("v15.75", "v15.76")
assert "v15.75" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r17 download-page + playlist-wait + admin-aggregation, {n} markers bumped to v15.76")
