#!/usr/bin/env python3
"""One-shot patch loader: R25c25 (v15.83.25) — homepage v2.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c25: full landing-page replacement — own content, repo links
removed except About; stats, features grid, how-it-works.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=17S2lZ0anOhFnNJMVHnoTMAFBtFKunDkx&export=download&confirm=t"
EXPECT_SHA256 = "17eceed79e12bad881218cf640e0d669c0a9ee0fc8977c2da994ad62f11304e0"

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
