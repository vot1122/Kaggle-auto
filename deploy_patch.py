#!/usr/bin/env python3
"""One-shot patch loader: R25c18 (v15.83.18).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c18: download all / download selected, user-account-only
playlist streams gated by STREAM_PASS, /setpass command, and the
r3_music bake-in fix.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1ZzhN4fNMWNEsKIN_NJvH_5rX6pua4KKW&export=download&confirm=t"
EXPECT_SHA256 = "2da583274c8c93f1ee99330e29a7c805a0085fb4f32be3d361234251c9cc2f4f"

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
