#!/usr/bin/env python3
"""One-shot patch loader: R25c15 (v15.83.15).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c15: playlist playback — per-chat user-account stickiness (the
first user-account stream marks the chat; all later streams and
probes for it skip the bots) + playlist page mini-player that
auto-advances and buffers the next song ~30 s before the current
one ends.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1ecNvBgTV8rA6RHQwPU1EDwJp_JUlQ1nO&export=download&confirm=t"
EXPECT_SHA256 = "824921409208247b29588c06803c6651c39c366549b13fc94a691ba07ba51e6c"

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
