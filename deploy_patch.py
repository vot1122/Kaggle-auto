#!/usr/bin/env python3
"""One-shot patch loader: v15.83.1 -> v15.83.2 (R25c2).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R25c2: pin the boot's kaggle CLI install to 1.6.17 — this account
uses legacy API auth and the 2.x CLI made worker pushes fail.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1AJA38vqt7x3PwXBCY4swhu6f_HrHQ4Bd&export=download&confirm=t"
EXPECT_SHA256 = "4230aa53cd473ca96c6102b9cda98a873800e886547c0b30970e2056e8adb72b"

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
