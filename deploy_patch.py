#!/usr/bin/env python3
"""One-shot patch loader: R25c12 (v15.83.12).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c12: wave slot-race fix + errored-worker requeue, and playlist
playback via the user account (helper bots can't read the songs
the main bot posted — the user-session fallback now covers that).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1DOh7Ufu-uE4vb9LqUYCp5L-5DqV4Lj7U&export=download&confirm=t"
EXPECT_SHA256 = "6be2eac1fb16c150163e9ba49c0fe6f4c241bb6e8a1f917a81d635f906ad988d"

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
