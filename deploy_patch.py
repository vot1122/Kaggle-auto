#!/usr/bin/env python3
"""One-shot patch loader: R25c9 (v15.83.9).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c9: fixes the pymongo truth-test crash (r25c8 did
bool(collection), which killed the leader slice and every worker at
startup — zero songs delivered) and makes queued workers keep the
batch open (Kaggle runs ~5 sessions at a time).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1k4LTHst8p-l_vEUIe89NZQc9Cb84OSBJ&export=download&confirm=t"
EXPECT_SHA256 = "948e8f5fd869f1b1fbde147fabc987c9d2c1901a9b5d77ff38ecd21b3bac7094"

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
