#!/usr/bin/env python3
"""One-shot patch loader: R25c34 (v15.83.34) - working tree pin.

The real payload is served from Drive (uploaded byte-exact); this
downloads it, verifies its sha256, and runs it.

R25c34: r25c33's pin failed (git refuses non-tip SHA fetches). Now
fetches by full SHA with a codeload tarball fallback, pinning WZML-X
to the last-known-good 6cc2760ab1c9 tree where music + playlists
worked. Access logging (r25c33) stays on.
"""
import hashlib
import os
import sys
import urllib.request

URL = "https://drive.usercontent.google.com/download?id=1eMZwOcnSKvHHZDvz3vrzsWNC2dv_dT2Q&export=download&confirm=t"
EXPECT_SHA256 = "e35ea56315bd499e60cb12d72e5e6db2cb0dd7e0f8ff3d250c068444d8bdbe58"

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
