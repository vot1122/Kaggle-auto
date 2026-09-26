#!/usr/bin/env python3
"""One-shot patch loader: R25c8 (v15.83.8).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25c8: catalog card with live per-song marks, ONE combined worker
status via the Mongo status channel, unlimited workers (one per 6
songs), whole-batch playlist, BUILD bumped to v15.83.8 everywhere
so the stale v15.83 session retires itself.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1-hSn8NCndRoMppEl7BwTIJavJNPSWiTR&export=download&confirm=t"
EXPECT_SHA256 = "fe0b5c80baecc4e91e1af01708a8967e50f173b282bdb44c3fafece16f70ddc4"

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
