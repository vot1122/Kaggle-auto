"""v15.71 (r12) deploy: fetch + verify + run the patch payload from Google Drive.

The payload lives on Google Drive and carries the full v15.70 -> v15.71
transform with its own integrity checks.
"""
import hashlib
import sys
import urllib.request

URL = (
    "https://drive.usercontent.google.com/download"
    "?id=15eJCOfida31PA6y1TeojneQOtJej7-5G&export=download&confirm=t"
)
SHA256 = "212b627b318755bb181e00fc68873891d3594b6cc6a99c21a86e97ee5ad90707"

req = urllib.request.Request(URL, headers={"User-Agent": "deploy-patch-loader"})
data = urllib.request.urlopen(req, timeout=300).read()
if hashlib.sha256(data).hexdigest() != SHA256:
    sys.exit("r12 payload integrity FAILED (hash mismatch)")
exec(compile(data.decode("utf-8"), "deploy_patch_r12.py", "exec"), {"__name__": "__main__"})
