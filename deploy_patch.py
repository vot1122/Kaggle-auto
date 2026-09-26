#!/usr/bin/env python3
"""One-shot patch loader: R25c5 (v15.83.5).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25c5: (1) worker config-path resolution for API-created kernels'
nested dataset mount layout; (2) workers deliver songs to the chat
the command was issued in.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=18OMvCbRXg0jjGnZs09G9iFkiO5oI1Ov1&export=download&confirm=t"
EXPECT_SHA256 = "f645fe9a165b6ebd6a99c3235d45bb66f362feccd2d0bcaa34dd34fa8a8bb09f"

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
