#!/usr/bin/env python3
"""One-shot patch loader: R25c7 (v15.83.7).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25c7: flood-aware sendAudio (429 retry + stagger/pace), playlist
import fix, leader slice on the worker path (no task-detail
messages between songs), sweep chat/token, catalog 429 retries.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1AeqwijdDA_nTeQ0wzvehAfXmsupKbuX4&export=download&confirm=t"
EXPECT_SHA256 = "1d8ffa708c87b3837256f5853d86402205dad3f6bf811b661b0ca0068eead8ee"

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
