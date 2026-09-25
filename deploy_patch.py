"""v15.70 (r11) deploy: fetch + verify + run the patch payload from Google Drive.

The payload (deploy_patch_r11.py) lives on Google Drive and carries the
full v15.69 -> v15.70 transform with its own integrity checks.
"""
import hashlib
import sys
import urllib.request

URL = (
    "https://drive.usercontent.google.com/download"
    "?id=15eJCOfida31PA6y1TeojneQOtJej7-5G&export=download&confirm=t"
)
SHA256 = "8be08460ab117c587c6b25de2d2b4bd2e1bb9b7bcf653088799e70c129f77a6b"

req = urllib.request.Request(URL, headers={"User-Agent": "deploy-patch-loader"})
data = urllib.request.urlopen(req, timeout=300).read()
if hashlib.sha256(data).hexdigest() != SHA256:
    sys.exit("r11 payload integrity FAILED (hash mismatch)")
exec(compile(data.decode("utf-8"), "deploy_patch_r11.py", "exec"), {"__name__": "__main__"})
