"""v15.72 (r13) deploy: fetch + verify + run the patch payload from Google Drive.

The payload lives on Google Drive and carries the full v15.71 -> v15.72
transform with its own integrity checks.
"""
import hashlib
import sys
import urllib.request

URL = (
    "https://drive.usercontent.google.com/download"
    "?id=15eJCOfida31PA6y1TeojneQOtJej7-5G&export=download&confirm=t"
)
SHA256 = "99822ace26cd3bf925deecb679318c19ffe3560fd1a3721585d17451c36de1eb"

req = urllib.request.Request(URL, headers={"User-Agent": "deploy-patch-loader"})
data = urllib.request.urlopen(req, timeout=300).read()
if hashlib.sha256(data).hexdigest() != SHA256:
    sys.exit("r13 payload integrity FAILED (hash mismatch)")
exec(compile(data.decode("utf-8"), "deploy_patch_r13.py", "exec"), {"__name__": "__main__"})
