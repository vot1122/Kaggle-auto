"""One-shot patch: v15.65 -> v15.66.

J-42 Round 9: dashboard reliability + stream auth hardening.
(1) the r2_web page JS had a quoting bug from Round 6 that broke the
whole script (dashboard rendered a black page); (2) login form now
visible without JS; (3) connection banner with auto-retry; (4)
no-flicker section updates (history diff); (5) stream password
loop-breaker with on-screen diagnostics; (6) full auth logging +
log-group alerts. Addition O patches the files at boot, idempotently.
"""

import os
import py_compile
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "wzfix_deploy", "addition_o.txt"),
          encoding="utf-8") as f:
    add_o = f.read()

anchor_i = "    # Kaggle addition I"
if "Kaggle addition O" not in s:
    if anchor_i not in s:
        sys.exit("addition-I anchor not found")
    s = s.replace(anchor_i, add_o + "\n" + anchor_i, 1)
    print("addition O spliced (after N, before I)")

n = s.count("v15.65")
s = s.replace("v15.65", "v15.66")
if "v15.66" not in s:
    sys.exit("version marker missing after patch")
with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-42 dashboard+stream fixes, {n} markers bumped to v15.66")
