#!/usr/bin/env python3
"""One-shot patch loader: v15.82 -> v15.83 (Round 25).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25: Phase B — batches with more than 6 songs fan out across
short-lived worker Kaggle sessions (6 lanes each, own IP, cookies +
newest yt-dlp); song-mode per-song upload fix (v15.82 downloaded
everything but uploaded nothing); YouTube->JioSaavn fallback ladder
with failure reasons; live progress counts; full activity logging.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1m3Yfl9b6OP9a071Bsruxze4BUTR8Bdlb&export=download&confirm=t"
EXPECT_SHA256 = "e667512e75c9214566ad77a698380f78ba36aee3d7fe7875ddd6325cd846ee10"

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
