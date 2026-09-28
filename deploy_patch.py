#!/usr/bin/env python3
"""One-shot patch loader: R25c27 (v15.83.27) - dashboard auth.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c27: stream password asked once and shared everywhere — the
player auth probe now sends the token (was 401 -> re-prompt every
play), and all pages keep it in localStorage wzml_stream_auth
(24h, across tabs). Supersedes r25c21_pl, r25c24_tpl, r25c26_home.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "5471c6655ab4c161411640912797427713059fd503250489259f0a6520d12a82"

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
