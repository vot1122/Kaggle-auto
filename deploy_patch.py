#!/usr/bin/env python3
"""One-shot patch loader: v15.83 -> v15.83.1 (R25b hotfix).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25b: the r19 build watchdog kept the hardcoded ver = "v15.82"
constant, so the v15.83 session killed itself minutes after boot.
This bumps it so the deployed session stays up.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1L2R573dF3D49KmVuOEGxmZ5Zm2zyA0-G&export=download&confirm=t"
EXPECT_SHA256 = "3f17c2ec79dadab971eb7c8c95ad8c5f0c6e4a08905d02accb33e28fec3a746c"

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
