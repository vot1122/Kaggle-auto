"""One-shot patch: v15.66 -> v15.67.

J-43: addition O ran BEFORE the J-region r2_web.py write, so the B64
write clobbered the dashboard fixes at every boot (page stayed black).
Move addition O to AFTER the write (before J-1), bump version.
"""

import os
import py_compile
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

MARK_O = "    # Kaggle addition O"
MARK_I = "    # Kaggle addition I"
J1 = "    # J-1: register /wzadmin routes on the bot stream server (in-process)"

if "v15.67" in s:
    print("already v15.67 — no changes")
else:
    if MARK_O not in s:
        sys.exit("addition O not found")
    if MARK_I not in s or J1 not in s:
        sys.exit("anchors missing")

    o_pos = s.index(MARK_O)
    i_pos = s.index(MARK_I)
    j1_pos = s.index(J1)
    r2_write_end = s.index('log(f"  r2: r2_web.py FAILED — {e}", "ERROR")', 0, j1_pos)

    if o_pos < i_pos:
        # O is before I (wrong place) — move it after the r2 write block
        o_block = s[o_pos:i_pos].rstrip() + "\n\n"
        s = s[:o_pos] + s[i_pos:]
        # recompute anchors after the cut
        j1_pos = s.index(J1)
        s = s[:j1_pos] + o_block + s[j1_pos:]
        print("addition O moved after the r2_web write (boot-order fix)")
    else:
        print("addition O already after the write")

    n = s.count("v15.66")
    s = s.replace("v15.66", "v15.67")
    if "v15.67" not in s:
        sys.exit("version marker missing after patch")
    with open(P, "w", encoding="utf-8") as f:
        f.write(s)
    py_compile.compile(P, doraise=True)
    print(f"patch OK: J-43 boot-order fix, {n} markers bumped to v15.67")
