#!/usr/bin/env python3
"""One-shot patch loader: R25c19 (v15.83.19).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c19: player v3 (search / sleep timer / speed / resume / share),
/playlists library page, homepage status pill + recent playlists,
and dead PATCH_DATA entries dropped for the 1 MB kernel limit.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1i9enjQi93F26OGbAKchkv1Vp2F1yERlN&export=download&confirm=t"
EXPECT_SHA256 = "03a6a0f1cacb95ed95a591961ea555c4b4368d4a1f23f781d816541b3127217a"

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
