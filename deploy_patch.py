#!/usr/bin/env python3
"""One-shot patch loader: R25c4 (v15.83.4).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25c4: worker push metadata omits id_no - Kaggle's push API wants
the numeric kernel id there (or nothing for a new kernel); push by
slug only, like the repo's own kernel-metadata.json.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1SBLjqfVRNiV_uux7SA6IbbmQrq-DNtHL&export=download&confirm=t"
EXPECT_SHA256 = "f838c01bc24212af5ce64f6d599aba11029708874b6cc18af11d826bc6b9502b"

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
