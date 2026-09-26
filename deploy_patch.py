#!/usr/bin/env python3
"""One-shot patch loader: R25c6 (v15.83.6).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25c6: workers download the whole slice first, then send all songs
together at the end; delivery chat is LEECH_LOG_CHAT from the
MongoDB-backed bot settings (command chat as fallback).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1v008cw-Ucs-Lp3AA-sJ6ALSssdQ7j3s1&export=download&confirm=t"
EXPECT_SHA256 = "1e44bdcb7a64ba1e4b65ee324cf71bea5253ffd56751e04941b933b54541b441"

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
