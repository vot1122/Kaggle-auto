#!/usr/bin/env python3
"""One-shot patch loader: R25c20 (v15.83.20) — hotfix.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c20: [hidden]-attribute CSS override fix — the shortcuts
overlay and the selection/resume pills now actually hide.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1MaUvMK5GpPj2Mjj4o8CAaXxD7I1F90ub&export=download&confirm=t"
EXPECT_SHA256 = "7e15ff7d35dd9b5666b1730b84ea8e08cea10f96d172c59d132433de0a3d81b8"

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
