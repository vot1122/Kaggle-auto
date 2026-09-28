#!/usr/bin/env python3
"""One-shot patch loader: R25c33 (v15.83.33) - pin WZML-X + access logs.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c33: the upstream wzv3 branch drifted (6cc2760 -> ab6464d2)
on 28 Sep, breaking the wserver lifespan, bot.ext_utils (playlists
500s) and patch anchors. This pins the clone to the last-known-good
commit 6cc2760 and enables gunicorn access logging so request/status
lines are visible via /_diag/logs.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "29dae7bda274faa988a08edb635813e2af794ad21c5f9c6361c18f6a021d4f77"

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
