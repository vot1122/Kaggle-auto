"""One-shot patch: v15.59 -> v15.60.

J-36 dashboard music-settings import fix: r2_web.py sits inside
bot/helper/wzfix/, so `from ..helper.wzfix.r4_music_cmds import ...`
resolves to bot.helper.helper.wzfix... (nonexistent). Result: the
dashboard mset toggles (music links / lyrics / short commands) failed
with "No module named 'bot.helper.helper'" and the settings read
silently fell back to defaults. Both imports become same-package
relative imports.
"""

import ast
import base64
import os
import py_compile
import re
import sys
import tempfile

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

# ---- locate and decode the WZFIX_WEB_B64 block ----
m = re.search(r"WZFIX_WEB_B64 = \((.*?)\n\)", s, re.S)
if not m:
    sys.exit("WZFIX_WEB_B64 block not found")
b64_text = ast.literal_eval("(" + m.group(1) + ")")
code = base64.b64decode(b64_text).decode("utf-8")

# ---- fix the two wrong relative imports ----
n = code.count("from ..helper.wzfix.r4_music_cmds import")
if n != 2:
    sys.exit(f"expected 2 bad imports, found {n}")
code = code.replace(
    "from ..helper.wzfix.r4_music_cmds import",
    "from .r4_music_cmds import",
)

# compile-check the patched r2_web module
with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False,
                                 encoding="utf-8") as tf:
    tf.write(code)
    tmp = tf.name
try:
    py_compile.compile(tmp, doraise=True)
except Exception as e:
    sys.exit(f"patched r2_web compile FAILED: {e}")
os.unlink(tmp)

# ---- re-encode and rebuild the block ----
new_b64 = base64.b64encode(code.encode("utf-8")).decode("ascii")
lines = [new_b64[k:k + 100] for k in range(0, len(new_b64), 100)]
block = "WZFIX_WEB_B64 = (\n" + "".join(
    f'    "{ln}"\n' for ln in lines
) + ")"
s = s[:m.start()] + block + s[m.end():]

# ---- bump the version ----
n = s.count("v15.59")
s = s.replace("v15.59", "v15.60")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-36 dashboard r2_web import fix, {n} markers bumped to v15.60")
