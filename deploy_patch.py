#!/usr/bin/env python3
"""One-shot patch: v15.77 -> v15.78 (Round 19).

Process fix: `kaggle kernels push` does NOT restart a running
session, so today's deploys never went live. The notebook now
gains a version watchdog that exits the session when the repo
carries a different build, letting the new version take over.
"""
import ast
import hashlib
import sys

INPUT_SHA256 = "81b910f4efc8da6b031b962d6c8d8d322350664fc3148282e7b4985f4ad4da46"
EXPECT_SHA256 = "c69e2cdd4f81f91575f6990fc9ecc1c240bc496291e891704d435ba3d153124f"

WD_ANCHOR_OLD = (
    'BOT_PROCESS = None',
    'TUNNEL_PROCESS = None',
    'SHUTDOWN_EVENT = threading.Event()',
    'NOTIFIED_STREAM_READY = False',
)

WD_ANCHOR_NEW = (
    'BOT_PROCESS = None',
    'TUNNEL_PROCESS = None',
    'SHUTDOWN_EVENT = threading.Event()',
    'NOTIFIED_STREAM_READY = False',
    '',
    '',
    'def _r19_start_watchdog():',
    '    """WZFIX_R19 (v15.78): a `kaggle kernels push` while a session is',
    '    already running does NOT restart it - the pushed version sits in',
    '    the repo while the old bot keeps serving. This daemon thread',
    '    polls the repo WZFIX BUILD marker; when it stops',
    '    matching THIS build (2 consecutive mismatches), the whole',
    "    session exits so the new version's run can take over. Network",
    '    errors never trigger an exit."""',
    '    import urllib.request',
    '',
    '    ver = "v15.78"',
    '',
    '    def _r19_poll():',
    '        import time as _r19t',
    '',
    '        _r19_misses = 0',
    '        while not SHUTDOWN_EVENT.is_set():',
    '            _r19t.sleep(120)',
    '            if SHUTDOWN_EVENT.is_set():',
    '                break',
    '            try:',
    '                _r19url = (',
    '                    "https://raw.githubusercontent.com/vot1122/"',
    '                    "Kaggle-auto/main/kaggle_notebook.py?t="',
    '                    + str(int(_r19t.time()))',
    '                )',
    '                _r19req = urllib.request.Request(',
    '                    _r19url, headers={"User-Agent": "WZFIX-R19-Watchdog"}',
    '                )',
    '                with urllib.request.urlopen(_r19req, timeout=20) as _r19r:',
    '                    _r19head = _r19r.read(4096).decode("utf-8", "replace")',
    '                _r19m = ""',
    '                for _r19ln in _r19head.splitlines():',
    '                    if "WZFIX BUILD:" in _r19ln:',
    '                        _r19parts = _r19ln.split(":", 1)[1].split()',
    '                        if _r19parts:',
    '                            _r19m = _r19parts[0]',
    '                        break',
    '                if not _r19m or _r19m == ver:',
    '                    _r19_misses = 0',
    '                    continue',
    '                _r19_misses += 1',
    '                log(',
    '                    f"r19 watchdog: repo build {_r19m} != running {ver} "',
    '                    f"({_r19_misses}/2)"',
    '                )',
    '                if _r19_misses >= 2:',
    '                    log(',
    '                        f"r19 watchdog: new build {_r19m} in repo - "',
    '                        "exiting so its run takes over",',
    '                        "WARN",',
    '                    )',
    '                    try:',
    '                        notify(',
    '                            parse_config(CONFIG_SRC),',
    '                            "stop",',
    '                            f"Restarting for new build {_r19m}",',
    '                        )',
    '                    except Exception:',
    '                        pass',
    '                    os._exit(0)',
    '            except Exception:',
    '                _r19_misses = 0',
    '',
    '    _r19th = threading.Thread(target=_r19_poll, daemon=True)',
    '    _r19th.start()',
)

POPEN_OLD = (
    '    try:',
    '        BOT_PROCESS = subprocess.Popen(',
    '            [sys.executable, "-m", "bot"],',
)

POPEN_NEW = (
    '    try:',
    '        # WZFIX_R19: start the version watchdog BEFORE the bot - a',
    '        # kaggle push alone never restarts a running session, so the',
    '        # running bot must exit itself when the repo build changes',
    '        try:',
    '            _r19_start_watchdog()',
    '        except Exception as _r19e:',
    '            log(f"r19 watchdog failed to start: {_r19e}", "WARN")',
    '        BOT_PROCESS = subprocess.Popen(',
    '            [sys.executable, "-m", "bot"],',
)

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.78" in s:
    print("already v15.78 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.77 build")

for _old, _new in ((WD_ANCHOR_OLD, WD_ANCHOR_NEW), (POPEN_OLD, POPEN_NEW)):
    _o = chr(10).join(_old)
    _n = chr(10).join(_new)
    assert s.count(_o) == 1, f"anchor count {s.count(_o)}"
    s = s.replace(_o, _n, 1)

R20 = (
    '    # R20 (v15.78): the /dl/ route serves kind="bulk", not "download" -\n'
    + '    # the R17/R18 gates never matched, so the password page never\n'
    + '    # showed for direct download links. Both gates now accept bulk.\n'
    + '    try:\n'
    + '        _ss20 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")\n'
    + '        with open(_ss20, "r", encoding="utf-8") as _f:\n'
    + '            _s20 = _f.read()\n'
    + '        if "WZFIX_R20" in _s20:\n'
    + '            log("  r20: bulk-kind download page already applied")\n'
    + '        else:\n'
    + '            _a20 = (\n'
    + '                "        if kind == " + chr(34) + "download" + chr(34) + ":" + chr(10)\n'
    + '            )\n'
    + '            _b20 = (\n'
    + '                "        # WZFIX_R20: the /dl/ route passes kind=" + chr(34) + "bulk" + chr(34) + " - accept it" + chr(10)\n'
    + '                + "        if kind in (" + chr(34) + "download" + chr(34) + ", " + chr(34) + "bulk" + chr(34) + "):" + chr(10)\n'
    + '            )\n'
    + '            _n20 = _s20.count(_a20)\n'
    + '            if _n20 == 2:\n'
    + '                _s20 = _s20.replace(_a20, _b20)\n'
    + '                with open(_ss20, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_s20)\n'
    + '                _r20 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _ss20],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r20.returncode == 0:\n'
    + '                    log("  r20: download page now serves the bulk kind (both gates)")\n'
    + '                else:\n'
    + '                    log("  r20: stream_server compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log(f"  r20: expected 2 gates, found {_n20} - not written", "WARN")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r20: FAILED — {e}", "ERROR")\n'
)

_kit_anchor = (
    "    log(" + chr(34) + "WZML-X-Bot patch kit applied" + chr(34) + ")" + chr(10)
)
assert s.count(_kit_anchor) == 1, "kit anchor count"
s = s.replace(_kit_anchor, R20 + _kit_anchor, 1)

n = s.count("v15.77")
s = s.replace("v15.77", "v15.78")
assert "v15.77" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r19 version watchdog (self-restart on deploy), {n} markers bumped to v15.78")
