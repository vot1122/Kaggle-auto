#!/usr/bin/env python3
"""One-shot patch loader: R25c16 (v15.83.16).

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c16: playlist playback FIX (r25c12/r25c15 silently skipped
because patch7_user is retired - the new fallback uses the kit's
own user-session functions, no dependency) + worker pipeline
revert (all downloads first, then uploads one by one in order,
downloads of all workers start together).
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1g7k3o_7KSX8W5fWZLgvMVoBC_HS8dcy0&export=download&confirm=t"
EXPECT_SHA256 = "80ebf9b38a9b6d62522a3f92aca1d7b585ef18d94057bfa361e60ca13c8df396"

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
