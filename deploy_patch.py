#!/usr/bin/env python3
"""One-shot patch loader: R25c26 (v15.83.26) — online music.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c26: /music page (JioSaavn search + YouTube streaming, ad-free),
worker endpoints /_musicsearch + /_musicstream, wserver proxies, and
homepage v2.1 (inline unlock + Online music button).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=10L2u_sLs18b86r9F7PUi8PSFNJByPn77&export=download&confirm=t"
EXPECT_SHA256 = "ec50ac6d81fbb05a85eb12c65084845e0d59ac7f9e18a451af432d6a5785d069"

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
