#!/usr/bin/env python3
"""One-shot patch: v15.79 -> v15.81 (Rounds 22 + 23).

R22: botpm spam fix - flood/timeout errors count as started,
genuine not-started note max once per user per 30 min.
R23: Phase-A YT throughput - force-upgrade yt-dlp, install
deno JS runtime, bgutil PO-token server + plugin, politeness,
fan-out default 16 -> 6.
"""
import ast
import hashlib
import sys

INPUT_SHA256 = "bd113d747b61aa2a6b708ce594f1ed8f6cdd515f5d0f27e61cd7b9b103c52d1b"
EXPECT_SHA256 = "69cb53ecd67b8a3045e3330ca884016531ce6ce71a1e9ebf86a2d1aa8dec1891"

R22 = (
    '    # R22 (v15.81): the per-task botpm check spammed a Task Checks\n'
    + '    # card for every song in a batch (24 at once) because Telegram\n'
    + '    # rate-limits send_chat_action right after a restart and any\n'
    + '    # error counted as not-started. Flood/timeout errors now count\n'
    + '    # as started, and a genuine not-started note is returned at most\n'
    + '    # once per user per 30 minutes.\n'
    + '    try:\n'
    + '        _ss22 = os.path.join(WZMLX_DIR, "bot/helper/telegram_helper/tg_utils.py")\n'
    + '        with open(_ss22, "r", encoding="utf-8") as _f:\n'
    + '            _s22 = _f.read()\n'
    + '        if "WZFIX_R22" in _s22:\n'
    + '            log("  r22: botpm check dedupe already applied")\n'
    + '        else:\n'
    + '            _a22 = (\n'
    + '                "async def check_botpm(message, button=None):" + chr(10)\n'
    + '                + "    try:" + chr(10)\n'
    + '                + "        await TgClient.bot.send_chat_action(message.from_user.id, ChatAction.TYPING)" + chr(10)\n'
    + '                + "        return None, button" + chr(10)\n'
    + '                + "    except Exception:" + chr(10)\n'
    + '                + "        if button is None:" + chr(10)\n'
    + '                + "            button = ButtonMaker()" + chr(10)\n'
    + '                + "        _msg = " + chr(34) + "┠ <i>Bot isn" + chr(39) + "t Started in PM or Inbox (Private)</i>" + chr(34) + "" + chr(10)\n'
    + '                + "        button.url_button(" + chr(10)\n'
    + '                + "            " + chr(34) + "Start Bot Now" + chr(34) + ", f" + chr(34) + "https://t.me/{TgClient.BNAME}?start=start" + chr(34) + ", " + chr(34) + "header" + chr(34) + "" + chr(10)\n'
    + '                + "        )" + chr(10)\n'
    + '                + "        return _msg, button" + chr(10)\n'
    + '            )\n'
    + '            _b22 = (\n'
    + '                "_WZFIX_BOTPM_LAST = {}" + chr(10)\n'
    + '                + "" + chr(10)\n'
    + '                + "" + chr(10)\n'
    + '                + "async def check_botpm(message, button=None):" + chr(10)\n'
    + '                + "    # WZFIX_R22: flood/timeout = started; note at most once per user per 30 min" + chr(10)\n'
    + '                + "    try:" + chr(10)\n'
    + '                + "        await TgClient.bot.send_chat_action(message.from_user.id, ChatAction.TYPING)" + chr(10)\n'
    + '                + "        return None, button" + chr(10)\n'
    + '                + "    except Exception as e:" + chr(10)\n'
    + '                + "        _e = str(e).upper()" + chr(10)\n'
    + '                + "        if " + chr(34) + "FLOOD" + chr(34) + " in _e or " + chr(34) + "TOO MANY REQUESTS" + chr(34) + " in _e or " + chr(34) + "TIMEOUT" + chr(34) + " in _e or " + chr(34) + "SLOW MODE" + chr(34) + " in _e:" + chr(10)\n'
    + '                + "            return None, button" + chr(10)\n'
    + '                + "        try:" + chr(10)\n'
    + '                + "            _uid = message.from_user.id" + chr(10)\n'
    + '                + "        except Exception:" + chr(10)\n'
    + '                + "            _uid = 0" + chr(10)\n'
    + '                + "        from time import time as _r22t" + chr(10)\n'
    + '                + "        if _WZFIX_BOTPM_LAST.get(_uid, 0) and _r22t() - _WZFIX_BOTPM_LAST[_uid] < 1800:" + chr(10)\n'
    + '                + "            return None, button" + chr(10)\n'
    + '                + "        _WZFIX_BOTPM_LAST[_uid] = _r22t()" + chr(10)\n'
    + '                + "        if button is None:" + chr(10)\n'
    + '                + "            button = ButtonMaker()" + chr(10)\n'
    + '                + "        _msg = " + chr(34) + "┠ <i>Bot isn" + chr(39) + "t Started in PM or Inbox (Private)</i>" + chr(34) + "" + chr(10)\n'
    + '                + "        button.url_button(" + chr(10)\n'
    + '                + "            " + chr(34) + "Start Bot Now" + chr(34) + ", f" + chr(34) + "https://t.me/{TgClient.BNAME}?start=start" + chr(34) + ", " + chr(34) + "header" + chr(34) + "" + chr(10)\n'
    + '                + "        )" + chr(10)\n'
    + '                + "        return _msg, button" + chr(10)\n'
    + '            )\n'
    + '            _n22 = _s22.count(_a22)\n'
    + '            if _n22 == 1:\n'
    + '                _s22 = _s22.replace(_a22, _b22)\n'
    + '                with open(_ss22, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_s22)\n'
    + '                _r22 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _ss22],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r22.returncode == 0:\n'
    + '                    log("  r22: botpm check deduped (no more per-task spam)")\n'
    + '                else:\n'
    + '                    log("  r22: tg_utils compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log(f"  r22: expected 1 anchor, found {_n22} - not written", "WARN")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r22: FAILED - {e}", "ERROR")\n'
)

R23 = (
    '    # R23 (v15.81) PHASE-A: YouTube rate limits per IP - the fix is\n'
    + '    # the current anti-throttle stack, not more hammering. Installs:\n'
    + '    # deno (JS runtime yt-dlp now requires for n/sig challenges), the\n'
    + '    # bgutil PO-token server (proof-of-origin tokens so datacenter IPs\n'
    + '    # pass the bot check), and its version-matched yt-dlp plugin. The\n'
    + '    # server runs on 127.0.0.1:4416 with a keepalive thread. Binaries\n'
    + '    # persist in KAGGLE_WORKING so reboots skip the downloads.\n'
    + '    try:\n'
    + '        import threading as _th23\n'
    + '        import urllib.request as _ur23\n'
    + '        import zipfile as _zf23\n'
    + '\n'
    + '        _bin23 = os.path.join(KAGGLE_WORKING, "bin")\n'
    + '        os.makedirs(_bin23, exist_ok=True)\n'
    + '\n'
    + '        def _dl23(url, dest):\n'
    + '            _req = _ur23.Request(url, headers={"User-Agent": "Mozilla/5.0"})\n'
    + '            with _ur23.urlopen(_req, timeout=180) as _r, open(dest, "wb") as _f:\n'
    + '                while True:\n'
    + '                    _chunk = _r.read(1 << 20)\n'
    + '                    if not _chunk:\n'
    + '                        break\n'
    + '                    _f.write(_chunk)\n'
    + '\n'
    + '        _bg23 = os.path.join(_bin23, "bgutil-pot")\n'
    + '        if not os.path.isfile(_bg23):\n'
    + '            _dl23("https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/download/v0.8.1/bgutil-pot-linux-x86_64", _bg23)\n'
    + '            os.chmod(_bg23, 0o755)\n'
    + '\n'
    + '        _dn23 = os.path.join(_bin23, "deno")\n'
    + '        if not os.path.isfile(_dn23):\n'
    + '            _z23 = _bg23 + ".deno.zip"\n'
    + '            _dl23("https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip", _z23)\n'
    + '            with _zf23.ZipFile(_z23) as _z:\n'
    + '                with _z.open("deno") as _s, open(_dn23, "wb") as _f:\n'
    + '                    while True:\n'
    + '                        _chunk = _s.read(1 << 20)\n'
    + '                        if not _chunk:\n'
    + '                        break\n'
    + '                    _f.write(_chunk)\n'
    + '            os.chmod(_dn23, 0o755)\n'
    + '            os.remove(_z23)\n'
    + '\n'
    + '        _plug23 = os.path.join(WZMLX_DIR, "yt_dlp_plugins")\n'
    + '        if not os.path.isdir(_plug23):\n'
    + '            _p23 = _bg23 + ".plugin.zip"\n'
    + '            _dl23("https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/download/v0.8.1/bgutil-ytdlp-pot-provider-rs.zip", _p23)\n'
    + '            with _zf23.ZipFile(_p23) as _z:\n'
    + '                _z.extractall(WZMLX_DIR)\n'
    + '            os.remove(_p23)\n'
    + '\n'
    + '        def _ping23():\n'
    + '            try:\n'
    + '                with _ur23.urlopen("http://127.0.0.1:4416/ping", timeout=5) as _r:\n'
    + '                    return _r.status == 200\n'
    + '            except Exception:\n'
    + '                return False\n'
    + '\n'
    + '        def _pot23_start():\n'
    + '            _lf23 = open(os.path.join(KAGGLE_WORKING, "potserver.log"), "a")\n'
    + '            subprocess.Popen(\n'
    + '                [_bg23, "server", "--host", "127.0.0.1", "--port", "4416"],\n'
    + '                stdout=_lf23,\n'
    + '                stderr=subprocess.STDOUT,\n'
    + '                start_new_session=True,\n'
    + '            )\n'
    + '\n'
    + '        if not _ping23():\n'
    + '            _pot23_start()\n'
    + '            import time as _t23\n'
    + '\n'
    + '            for _i23 in range(15):\n'
    + '                if _ping23():\n'
    + '                    break\n'
    + '                _t23.sleep(1)\n'
    + '\n'
    + '        def _pot23_keepalive():\n'
    + '            import time as _t23\n'
    + '\n'
    + '            while True:\n'
    + '                _t23.sleep(60)\n'
    + '                if not _ping23():\n'
    + '                    _pot23_start()\n'
    + '\n'
    + '        _th23.Thread(target=_pot23_keepalive, daemon=True).start()\n'
    + '        log("  r23: PO token server (4416) + deno + bgutil plugin ready")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r23: FAILED - {e}", "ERROR")\n'
    + '\n'
    + '    # R23b: politeness - 1s between YouTube API requests (fewer 429s\n'
    + '    # means fewer restarts means higher real throughput)\n'
    + '    try:\n'
    + '        _ss23 = os.path.join(WZMLX_DIR, "bot/helper/mirror_leech_utils/download_utils/yt_dlp_download.py")\n'
    + '        with open(_ss23, "r", encoding="utf-8") as _f:\n'
    + '            _s23 = _f.read()\n'
    + '        if "sleep_requests" not in _s23:\n'
    + '            _old23 = "            " + chr(34) + "trim_file_name" + chr(34) + ": 220," + chr(10)\n'
    + '            if _s23.count(_old23) == 1:\n'
    + '                _new23 = "            " + chr(34) + "sleep_requests" + chr(34) + ": 1," + chr(10)\n'
    + '                _s23 = _s23.replace(_old23, _old23 + _new23)\n'
    + '                with open(_ss23, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_s23)\n'
    + '                log("  r23b: yt-dlp politeness set (sleep_requests=1)")\n'
    + '            else:\n'
    + '                log("  r23b: trim_file_name anchor missing", "WARN")\n'
    + '        else:\n'
    + '            log("  r23b: politeness already set")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r23b: FAILED - {e}", "ERROR")\n'
    + '\n'
    + '    # R23c: fan-out default 16 -> 6. Research-backed sweet spot for\n'
    + '    # concurrent YouTube downloads per IP is 4-8; 16 was inviting 429\n'
    + '    # spirals. Still tunable via WZFIX_FANOUT_JOBS.\n'
    + '    try:\n'
    + '        _sm23 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r3_music.py")\n'
    + '        with open(_sm23, "r", encoding="utf-8") as _f:\n'
    + '            _m23 = _f.read()\n'
    + '        _old23m = "_os.environ.get(" + chr(34) + "WZFIX_FANOUT_JOBS" + chr(34) + ", " + chr(34) + "16" + chr(34) + ")"\n'
    + '        _new23m = "_os.environ.get(" + chr(34) + "WZFIX_FANOUT_JOBS" + chr(34) + ", " + chr(34) + "6" + chr(34) + ")"\n'
    + '        if _old23m in _m23:\n'
    + '            _m23 = _m23.replace(_old23m, _new23m)\n'
    + '            with open(_sm23, "w", encoding="utf-8") as _f:\n'
    + '                _f.write(_m23)\n'
    + '            log("  r23c: fan-out default 16 -> 6 (per-IP sweet spot)")\n'
    + '        else:\n'
    + '            log("  r23c: fan-out default already 6")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r23c: FAILED - {e}", "ERROR")\n'
)

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.81" in s:
    print("already v15.81 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.79 build")

_kit_anchor = (
    "    log(" + chr(34) + "WZML-X-Bot patch kit applied" + chr(34) + ")" + chr(10)
)
assert s.count(_kit_anchor) == 1, "kit anchor count"
s = s.replace(_kit_anchor, R22 + R23 + _kit_anchor, 1)

_pip_anchor = (
    "    # Ensure critical packages are importable" + chr(10)
)
assert s.count(_pip_anchor) == 1, "pip anchor count"
_pip_new = (
    "    # WZFIX r23 (v15.81): Kaggle preinstalls an old yt-dlp and" + chr(10)
    + "    # plain pip -r sees it as satisfied - force the upgrade." + chr(10)
    + "    # Current yt-dlp + curl-cffi impersonation is the anti-429 stack." + chr(10)
    + "    try:" + chr(10)
    + "        subprocess.run(" + chr(10)
    + "            [sys.executable, " + chr(34) + "-m" + chr(34) + ", " + chr(34) + "pip" + chr(34) + ", " + chr(34) + "install" + chr(34) + ", " + chr(34) + "--no-input" + chr(34) + ", " + chr(34) + "--upgrade" + chr(34) + "," + chr(10)
    + "             " + chr(34) + "yt-dlp[default,curl-cffi]" + chr(34) + "]," + chr(10)
    + "            timeout=600, capture_output=True, text=True," + chr(10)
    + "        )" + chr(10)
    + "        import yt_dlp as _ytd23" + chr(10)
    + "        log(f" + chr(34) + "r23: yt-dlp now at {_ytd23.version.__version__}" + chr(34) + ")" + chr(10)
    + "    except Exception as e:" + chr(10)
    + "        log(f" + chr(34) + "r23: yt-dlp upgrade failed: {e}" + chr(34) + ", " + chr(34) + "WARN" + chr(34) + ")" + chr(10)
)
s = s.replace(_pip_anchor, _pip_new + _pip_anchor, 1)

_env_old = (
    "    env = os.environ.copy()" + chr(10)
    + "    env[" + chr(34) + "PYTHONPATH" + chr(34) + "] = WZMLX_DIR + os.pathsep + env.get(" + chr(34) + "PYTHONPATH" + chr(34) + ", " + chr(34) + "" + chr(34) + ")" + chr(10)
)
_env_new = (
    "    env = os.environ.copy()" + chr(10)
    + "    env[" + chr(34) + "PYTHONPATH" + chr(34) + "] = WZMLX_DIR + os.pathsep + env.get(" + chr(34) + "PYTHONPATH" + chr(34) + ", " + chr(34) + "" + chr(34) + ")" + chr(10)
    + "    # WZFIX r23 (v15.81): deno (JS runtime for yt-dlp challenges)" + chr(10)
    + "    # lives in KAGGLE_WORKING/bin - put it on the bot PATH" + chr(10)
    + "    env[" + chr(34) + "PATH" + chr(34) + "] = os.path.join(KAGGLE_WORKING, " + chr(34) + "bin" + chr(34) + ") + os.pathsep + env.get(" + chr(34) + "PATH" + chr(34) + ", " + chr(34) + "" + chr(34) + ")" + chr(10)
)
assert s.count(_env_old) == 1, "env anchor count"
s = s.replace(_env_old, _env_new, 1)

n = s.count("v15.79")
s = s.replace("v15.79", "v15.81")
assert "v15.79" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r22 botpm fix + r23 phase-A stack, {n} markers bumped to v15.81")
