"""One-shot patch: v15.61 -> v15.62 (J-38 Round 5: per-link stream passwords).

Reads wzfix_deploy/r5_streampass.py (the new module's source) and
wzfix_deploy/addition_k.txt (the notebook patch code), embeds the module
into the notebook as WZFIX_R5_STREAMPASS_B64, splices addition K in
after the v15.6 auth bridge section, and bumps the version.
"""

import base64
import py_compile
import re
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

r5_src = open("wzfix_deploy/r5_streampass.py", encoding="utf-8").read()
addition_k = open("wzfix_deploy/addition_k.txt", encoding="utf-8").read()
r5_b64 = base64.b64encode(r5_src.encode("utf-8")).decode("ascii")
R5_LINES = [r5_b64[k:k + 100] for k in range(0, len(r5_b64), 100)]

m_admin = re.search(r"WZFIX_ADMIN_B64 = \(", s)
if not m_admin:
    sys.exit("WZFIX_ADMIN_B64 anchor not found")
if "WZFIX_R5_STREAMPASS_B64" not in s:
    block = (
        "# WZFIX Round 5 (v15.62) — per-link stream passwords.\n"
        "WZFIX_R5_STREAMPASS_B64 = (\n"
        + "".join('    "' + ln + '"\n' for ln in R5_LINES)
        + ")\n\n"
    )
    s = s[:m_admin.start()] + block + s[m_admin.start():]

ANCHOR = '    log(f"  auth bridge: v15.6 live-password proxy applied ({ok_h}/3 edits)")'
if ANCHOR not in s:
    sys.exit("auth bridge anchor not found")
if "Kaggle addition K" not in s:
    s = s.replace(ANCHOR, ANCHOR + "\n\n" + addition_k.rstrip("\n") + "\n", 1)

n = s.count("v15.61")
s = s.replace("v15.61", "v15.62")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-38 r5 per-link stream passwords, {n} markers bumped to v15.62")
