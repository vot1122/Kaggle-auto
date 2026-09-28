#!/usr/bin/env python3
"""One-shot patch loader: R25c30 (v15.83.30) - logs served over the wserver.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c30: the kernel has no Kaggle API credentials, so the r25c29
dataset push could never work (permission wall). The shipper now
writes the bundle to disk every 60s and the wserver serves it at
GET /_diag/logs (X-Diag-Key auth) - reachable through the public
worker URL and fetched by fetch-logs.yml. Also adds the diag key
scrub step to deploy-version.yml so the key never lands in the
public repo copy of the notebook.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "42003166b1a83b14980208fd84da7c4fa754d73e234c3f3b7405b5eddffbcef5"

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
