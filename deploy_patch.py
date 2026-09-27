#!/usr/bin/env python3
"""One-shot patch loader: R25c17 (v15.83.17).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c17: live trending order (JioSaavn), duplicate removal, clean
song names + covers + durations, and a full music player page.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1CqRO6YVlj7oOgQyu3_H2B-fR_iB1HVHe&export=download&confirm=t"
EXPECT_SHA256 = "4eab7bf3c4314287401869f3b67c975631e1ed8951c82c70423f2f411c63d6e3"

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
