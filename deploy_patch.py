#!/usr/bin/env python3
"""One-shot patch loader: R25c14 (v15.83.14).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c14: uploads reverted to sequential, in the original slice
order (user prefers ordered bursts over raw speed). Downloads stay
parallel; retries-with-wait and the live per-song card status are
kept.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1Nt_GL2A0Sjsb8Fvq7V7piUUuAs8rRJjZ&export=download&confirm=t"
EXPECT_SHA256 = "033f40f9aea85a3371bed82c33f239c2febc08421aebb6f0366fb65d7485bef3"

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
