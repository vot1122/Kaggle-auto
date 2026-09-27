#!/usr/bin/env python3
"""One-shot patch loader: R25c11 (v15.83.11).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c11: playlist fixes — the callback's ext_utils import pointed at
bot.ext_utils (it's bot/helper/ext_utils in WZML-X wzv3), and the
workers' final consistency pass wiped the stored message ids right
before the playlist was built (why the prompt offered only 6 of 67
songs).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1iA5CpQ_iy6AgFelPFQJ1P1_np75nWvBm&export=download&confirm=t"
EXPECT_SHA256 = "c54e475aa997900086dccccddfe980118751f380b11c44aaf33685e123a2dc1c"

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
