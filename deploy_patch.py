#!/usr/bin/env python3
"""One-shot patch loader: R25c3 (v15.83.3).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25c3: worker kernel dispatch invokes the kaggle console script
(1.6.17 has no __main__.py, so `python -m kaggle` cannot run it).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1gKZywdsQ9zXgqWj-LzutuZbKxYK4Ce97&export=download&confirm=t"
EXPECT_SHA256 = "f09a0e5c87e911307ab2b8ff5464bd713856d1c5b595b6733cd51214bbcca93c"

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
