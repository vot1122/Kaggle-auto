"""One-shot patch: v15.64 -> v15.65.

J-41 Round 6b: stream page auth-loop fix — the boot probe and the
video/download URLs never carried the auth token for normal links, so
a correct password just reloaded into the same 401 prompt (endless
password loop, video never played). Addition N patches stall_ui.js and
stream.html at boot.
"""

import os
import py_compile
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "wzfix_deploy", "addition_n.txt"),
          encoding="utf-8") as f:
    add_n = f.read()

anchor_m = "    # Kaggle addition I"
if "Kaggle addition N" not in s:
    if anchor_m not in s:
        sys.exit("addition-I anchor not found")
    s = s.replace(anchor_m, add_n + "\n" + anchor_m, 1)
    print("addition N spliced (after M, before I)")

n = s.count("v15.64")
s = s.replace("v15.64", "v15.65")
if "v15.65" not in s:
    sys.exit("version bump failed")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-41 stream auth-loop fix, {n} markers bumped to v15.65")
