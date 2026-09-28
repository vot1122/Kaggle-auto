#!/usr/bin/env python3
"""One-shot patch loader: R25c21 (v15.83.21) — player v4.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c21: player v4 (cover-color theme, queue panel, grid view,
sort, favorites, +-10s skips, aurora) + playlists library v2.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1YnhRuiHh6ndlh--5PziwOcIPT7-1twYZ&export=download&confirm=t"
EXPECT_SHA256 = "2173bb45ab18205e85091fa5dd0750b0694c4667cd610758dedeea5fc83bfd9f"

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
