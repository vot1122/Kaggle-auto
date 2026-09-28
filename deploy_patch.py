#!/usr/bin/env python3
"""One-shot patch loader: R25c28 (v15.83.28) - web server resilience.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c28: the gunicorn wserver behind the tunnel was dying silently
minutes after boot (site 502, bot alive). Adds a gunicorn log file +
a bot-side watchdog that logs why it died and restarts it, plus a
notebook-side tunnel watchdog (restart cloudflared + re-register).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "416b3265578eeb286674ada48f4d99bd4737d6c590df7cb339739806f2e629af"

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
