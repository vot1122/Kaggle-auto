#!/usr/bin/env python3
"""One-shot patch loader: R25c31 (v15.83.31) - fallback web server.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c31: on top of r25c30 (diag endpoint on the wserver), the
notebook now runs a fallback web server: when gunicorn is dead it
serves /_diag/logs (same key) plus a maintenance page on port 8080
itself, releasing the port every ~6.5 min for 90 s so gunicorn
restarts can rebind. Logs stay fetchable while the site is down.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "0ae5d5d6509ef3abbb78aa37f6b4fb2f7cf7861f540bb8e15985e4b864165c84"

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
