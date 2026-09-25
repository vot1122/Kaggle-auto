#!/usr/bin/env python3
"""R14 (v15.73) deploy: verify-only.

The complete v15.73 notebook is served from Google Drive
(wzfix_deploy/notebook_gdrive.txt — source of truth). This step only
verifies the fetched notebook is already v15.73 so a stale or failed
Drive fetch can never silently deploy an old build.
"""
import sys

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.73" in s:
    print("notebook already v15.73 — deploying as-is (Drive source of truth)")
    sys.exit(0)
sys.exit(
    "notebook is NOT v15.73 — the Google Drive fetch failed and the repo "
    "copy is stale. Check the Drive file behind "
    "wzfix_deploy/notebook_gdrive.txt and re-run the workflow."
)
