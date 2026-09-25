"""One-shot patch: v15.67 -> v15.68 (Round 8: per-link pass loop fix).

The ?user=1 gates in stream_server only accepted tokens signed with
the GLOBAL STREAM_PASS — a link unlocked with its own per-link password
reloaded into the password prompt forever. Splices addition R8 (which
fixes both gates and adds the link_token_ok helper), bumps the version.
"""
import py_compile
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

if "v15.68" in s:
    py_compile.compile(P, doraise=True)
    print("already v15.68 — no changes")
    sys.exit(0)
if "v15.67" not in s:
    sys.exit("no v15.67 markers found — unexpected notebook")

ANCH = "    # Kaggle addition M — v15.67 Round 6: /start user registry."
if "Kaggle addition R8" not in s:
    if ANCH not in s:
        sys.exit("addition M anchor not found")
    add = open("wzfix_deploy/addition_r8.txt", encoding="utf-8").read()
    assert "WZFIX_R8_LINKGATE" in add and "async def link_token_ok" in add
    s = s.replace(ANCH, add + "\n" + ANCH, 1)
    print("addition R8 spliced (before addition M)")

n = s.count("v15.67")
s = s.replace("v15.67", "v15.68")
s = s.replace('WZFIX_DATE = "23 Sep 2026 (IST)"', 'WZFIX_DATE = "25 Sep 2026 (IST)"')
if "v15.68" not in s:
    sys.exit("version marker missing after patch")
py_compile.compile(P, doraise=True)
with open(P, "w", encoding="utf-8") as f:
    f.write(s)
print(f"patch OK: r8 link-gate fix, {n} markers bumped to v15.68")
