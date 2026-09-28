#!/usr/bin/env python3
"""One-shot patch loader: R25c29 (v15.83.29) - log everything, ship it.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c29: kernel log ring + gunicorn log + bot log + process/socket
state pushed every 2 min to the private Kaggle dataset djoshi7/wzmlx-logs
(fetch via fetch-logs.yml), plus the player.html create-if-missing fix.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "3fec774ccf47c19a81e4b6e610e2b0928978e4f360afe85baf6a9ab72ffc4bde"

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
