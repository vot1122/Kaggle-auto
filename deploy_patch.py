#!/usr/bin/env python3
"""One-shot patch loader: v15.81 -> v15.82 (Round 24).

The real payload is served from Drive (uploaded byte-exact, no
hand-copying); this downloads it, verifies its sha256, and runs it.

R24: artist-batch repair - the v15.74 m4a passthrough crashed every
music download (list index out of range); per-song streaming uploads;
failed-song counting; in-flight launch gate; junk-flag stripping.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1VD256SyISfBh-mjIzzn8NuoXq9vOAe8l&export=download&confirm=t"
EXPECT_SHA256 = "c6b828bd584d1ba90783fe130251bdd0b3307e260f75f5129a20a3ad1a09d83e"

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
