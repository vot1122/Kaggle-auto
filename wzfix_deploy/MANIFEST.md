# WZFIX bot — final artifacts bundle

Snapshot of everything we changed, taken after the J-21…J-24 work.
Stored durably in two places:

- **Google Drive** folder `wzfix-bot-artifacts` (id `18j83kWclAmbPxYL1yv1RhKNJCtM_ayQg`)
- **GitHub** `vot1122/Kaggle-auto` (main)

## Files

| File | sha256 (prefix) | Role |
|---|---|---|
| `_remote_r17_webdl.py` | `2ee85331362f` | webdl module (fetched by the round from Drive) — parallel fetch, R2 route, hardened pixeldrain |
| `r17_round.py` | `e7ec063e1169` | the boot round (fetched by the notebook from Drive) — installs the module, registers routes, `/ws` R2 fields |
| `kaggle_notebook.py` | `7dd530c3d50e` | the Kaggle kernel — warm 4-tunnel pool, cloudflared `--protocol http2` + `--edge-ip-version 4`, boto3 install (scrubbed: no diag key) |
| `worker.js` | `f43d7cea0e73` | Cloudflare Worker — multi-bot router + multi-tunnel fan-out + pool reporting |
| `cf-worker.yml` | `a68a2a0553b2` | GitHub Actions workflow to pull/push the Worker via a Cloudflare token (secrets scrubbed) |
| `kaggle_manager.py` | `8b912498e8b4` | Kaggle session scheduler |
| `README_WEBDL.md` | `967194b1cfd1` | webdl notes |

Note: `kaggle_notebook.py` here is the **scrubbed** copy (no diag key). The live one
with the real diag key lives in the Drive file id `1S_41l4rbZ7_fDwNDTCfEUHD0Bz2aE2iM`.

## Deploy chain (current, Kaggle)

1. Notebook is pushed to Kaggle (by `kaggle_manager` / `deploy-version.yml`).
2. At boot the notebook fetches `r17_round.py` from Drive (pin `e7ec063e1169`).
3. The round fetches `_remote_r17_webdl.py` from Drive (pin `2ee85331362f`).
4. The Worker stores the tunnel pool and fans large downloads across it.

## Pending (next step)

Port all of this to the second account's repo where the **bot process runs on a
GitHub Actions runner** instead of Kaggle. That needs adaptation:

- No Kaggle kernel / no `kaggle_manager` push loop — a workflow that runs the bot
  directly on the runner (and re-triggers the next run before GitHub's 6 h cap).
- The tunnel-pool + cloudflared flags carry over as-is.
- The module + round are fetched by URL; those URLs (or the Drive ids) must be
  updated to wherever the artifacts live in that repo.
- The Worker + `cf-worker.yml` carry over unchanged (point `script_name` /
  account at the same Cloudflare account).
