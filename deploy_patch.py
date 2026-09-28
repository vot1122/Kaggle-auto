#!/usr/bin/env python3
"""One-shot patch loader: R25c22 (v15.83.22) — mobile bar fix.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c22: player bar no longer overflows on phones — secondary
controls moved to a "more" sheet; 2-line song names.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1ndnTJlDrc0bMCIdhJZzr_DeFElEYwsgN&export=download&confirm=t"
EXPECT_SHA256 = "137e820ac4c22b723d50a8cea196c2f3c272ed689dab378cb667206a83eafabf"

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
