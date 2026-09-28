#!/usr/bin/env python3
"""One-shot patch loader: R25c32 (v15.83.32) - ClientTimeout fix.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c32: the wserver lifespan used ClientTimeout without
importing it -> gunicorn died at boot with a NameError (the permanent
502s). patch_r25c32_ct.py prepends a guarded import to web/wserver.py;
the r25c31 fallback also gets a 300 s boot grace so it never races
gunicorn's first start.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "52ca3710158aa22d1bda09344b453591bbfd4ec33edc29f6e7c90f76393782fa"

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
