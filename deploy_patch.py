#!/usr/bin/env python3
"""One-shot patch: v15.78 -> v15.79 (Round 21).

Fix: the download password page fetch() call was never closed,
so the whole page script died on a syntax error and the Unlock
button did nothing. Closes the JSON.stringify() call and the
fetch options object.
"""
import ast
import hashlib
import sys

INPUT_SHA256 = "c69e2cdd4f81f91575f6990fc9ecc1c240bc496291e891704d435ba3d153124f"
EXPECT_SHA256 = "bd113d747b61aa2a6b708ce594f1ed8f6cdd515f5d0f27e61cd7b9b103c52d1b"

R21 = (
    '    # R21 (v15.79): the download password page fetch() was never\n'
    + '    # closed - a JS syntax error killed the whole script, so the\n'
    + '    # Unlock button did nothing after entering the right password.\n'
    + '    try:\n'
    + '        _ss21 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")\n'
    + '        with open(_ss21, "r", encoding="utf-8") as _f:\n'
    + '            _s21 = _f.read()\n'
    + '        if "WZFIX_R21" in _s21:\n'
    + '            log("  r21: download page fetch fix already applied")\n'
    + '        else:\n'
    + '            _a21 = (\n'
    + '                "body:JSON.stringify({password:P.value,token:location.pathname.split(" + chr(34) + "/" + chr(34) + ").filter(Boolean).pop()||" + chr(34) + chr(34) + "});" + chr(10)\n'
    + '            )\n'
    + '            _b21 = (\n'
    + '                "body:JSON.stringify({password:P.value,token:location.pathname.split(" + chr(34) + "/" + chr(34) + ").filter(Boolean).pop()||" + chr(34) + chr(34) + "})});" + chr(10)\n'
    + '            )\n'
    + '            _n21 = _s21.count(_a21)\n'
    + '            if _n21 == 1:\n'
    + '                _s21 = _s21.replace(_a21, _b21)\n'
    + '                with open(_ss21, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_s21)\n'
    + '                _r21 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _ss21],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r21.returncode == 0:\n'
    + '                    log("  r21: download page fetch closed (Unlock button works again)")\n'
    + '                else:\n'
    + '                    log("  r21: stream_server compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log(f"  r21: expected 1 anchor, found {_n21} - not written", "WARN")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r21: FAILED - {e}", "ERROR")\n'
)

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.79" in s:
    print("already v15.79 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.78 build")

_kit_anchor = (
    "    log(" + chr(34) + "WZML-X-Bot patch kit applied" + chr(34) + ")" + chr(10)
)
assert s.count(_kit_anchor) == 1, "kit anchor count"
s = s.replace(_kit_anchor, R21 + _kit_anchor, 1)

n = s.count("v15.78")
s = s.replace("v15.78", "v15.79")
assert "v15.78" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r21 download page fetch closed, {n} markers bumped to v15.79")
