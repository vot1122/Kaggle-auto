#!/usr/bin/env python3
"""One-shot patch loader: R25c24 (v15.83.24).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c24: row relayout (actions right, duration under the name) and
majority-rule name cleaning with .mp3 stripping.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1lnEbsPH_gLNMpmAVqjFQ9WNsMg9BCVZT&export=download&confirm=t"
EXPECT_SHA256 = "d4dd81fbec7a66c8cd7ac16af9e836b192e01243a41fdcd7d57fd758b58f6d08"

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
