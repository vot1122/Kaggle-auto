#!/usr/bin/env python3
"""One-shot patch loader: R25c10 (v15.83.10).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c10: worker waves — Kaggle only starts ~5 sessions per account
(leader + 4 workers); pushes beyond that never run, so workers are
now dispatched in waves of 4, the next kernel going out when a slot
frees. Fixes the log-4 run where w5..w10 sat unstarted and 30 songs
never downloaded.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1GEaQXRyTXLqwgDpgo9b-Tpv0Id5RVgMx&export=download&confirm=t"
EXPECT_SHA256 = "38df71f9ce8c86620a98c1d2cdc51e0a2b5f083401c4989f3c9ab7d4269ce24e"

dst = "_real_deploy_patch.py"
req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=180) as r, open(dst, "wb") as f:
    f.write(r.read())
data = open(dst, "rb").read()
h = hashlib.sha256(data).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"payload sha mismatch: {h}")
print(f"payload verified: {os.path.getsize(dst)} bytes")

g = {"__name__": "__main__", "__file__": os.path.abspath(dst)}
exec(compile(data.decode("utf-8"), dst, "exec"), g)
