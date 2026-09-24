"""One-shot patch: v15.63 -> v15.64.

J-40 Round 6 user access management:
  - dashboard: /start users registry (wzfix_startusers) with
    authorize / sudo / block toggles (r2_web REPL via v1564_web_repl)
  - services.py: record every /start sender (addition M)
"""

import ast
import base64
import os
import py_compile
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "wzfix_deploy"))
from v1564_web_repl import REPL  # noqa: E402

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

m = re.search(r"WZFIX_WEB_B64 = \((.*?)\n\)", s, re.S)
if not m:
    sys.exit("WZFIX_WEB_B64 block not found")
web = base64.b64decode(ast.literal_eval("(" + m.group(1) + ")")).decode("utf-8")

for i, (old, new) in enumerate(REPL, 1):
    n = web.count(old)
    if n != 1:
        sys.exit(f"web anchor {i}: expected 1 match, found {n}")
    web = web.replace(old, new)

with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False,
                                 encoding="utf-8") as tf:
    tf.write(web)
    tmp = tf.name
try:
    py_compile.compile(tmp, doraise=True)
except Exception as e:
    sys.exit(f"patched r2_web compile FAILED: {e}")

new_b64 = base64.b64encode(web.encode("utf-8")).decode("ascii")
lines = [new_b64[k:k + 100] for k in range(0, len(new_b64), 100)]
block = "WZFIX_WEB_B64 = (\n" + "".join(
    f'    "{ln}"\n' for ln in lines
) + ")"
s = s[:m.start()] + block + s[m.end():]

# splice addition M (services.py /start registry) into the notebook,
# placed right before addition I so it runs after K
with open(os.path.join(HERE, "wzfix_deploy", "addition_m.txt"),
          encoding="utf-8") as f:
    add_m = f.read()
anchor_i = "    # Kaggle addition I"
if "Kaggle addition M" not in s:
    if anchor_i not in s:
        sys.exit("addition-I anchor not found")
    s = s.replace(anchor_i, add_m + "\n" + anchor_i, 1)
    print("addition M spliced before addition I")

n = s.count("v15.63")
s = s.replace("v15.63", "v15.64")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-40 user access management, {n} markers bumped to v15.64")
