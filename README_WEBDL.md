# WZFIX R17 — Web Downloader (v15.84)

The bot is now a downloader site. Paste a link on a web page, the bot's
yt-dlp engine grabs it server-side, and the finished file downloads
straight to your device — no Telegram chat, no upload, no Drive mirror.
The Telegram bot is untouched.

    GitHub Pages ─┐
    (vot1122.github.io/ytwebdownload)  Cloudflare Worker (unchanged)
                   │                          │
                   │        https://<worker>/webdl
                   ▼                          ▼
              browser ──── CORS ────► wserver:8080 ──► stream server:8091
                                            (new /webdl proxy,      (new r17 module
                                             streaming for files)    + yt-dlp engine)

## What was verified

- Module compiled and run live locally: login (HMAC bearer tokens),
  per-IP rate limit (5 fails → 429), CORS preflight for the Pages
  origin, formats listing (real yt-dlp -J), full download → progress
  → done → file served with attachment Content-Disposition, HTTP
  Range (206 partial content), cancel, clear, error surfacing.
- The round was executed against a copy of the pinned WZML-X tree
  (6cc2760) — all three anchors matched, stream server + wserver
  patched, all three files py_compile clean, and the round is
  idempotent (second run logs "already has webdl").
- The full 12.4k-line notebook with the round baked in py_compiles.

## Deploy (the normal flow)

1. The Google Drive source-of-truth notebook already contains R17
   (deployed via the Drive update; a backup copy
   `kaggle_notebook_backup_pre_R17.py` exists in the same Drive).
2. Run the `Deploy WZFIX Version` workflow. The r25c34 payload is a
   no-op on this notebook (it already contains 15.83.34).

## The website

- **Direct:** `https://<worker>/webdl` — always works, served by the
  bot itself.
- **GitHub Pages:** repo `ytwebdownload` →
  `https://vot1122.github.io/ytwebdownload/`. First visit: click ⚙ and
  paste your worker URL (saved in localStorage). CORS is emitted by
  the bot (allowed origins include the Pages URL).

## Config knobs (wzfix_config doc "webdl" / config.env overrides)

| Key | Default | Meaning |
|---|---|---|
| `WEBDL_PASS` | generated + TG-sent | login password |
| `WEBDL_TTL` | 6 (hours) | finished files auto-delete |
| `WEBDL_MAX_GB` | 8 | disk budget for webdl files |
| `WEBDL_CONC` | 2 | concurrent downloads |
| `WEBDL_ORIGINS` | Pages URLs + localhost | CORS allow-list |

## Honest limits (by design, this round)

- **yt-dlp links only** (YouTube, Instagram, direct links, anything
  yt-dlp supports). Torrents/aria2/qbit/mirroring are R19.
- The site only works while the bot session is awake (06:00–23:00 IST,
  plus restart windows). The page shows an offline banner and retries.
- Playlists download only the single item (`--no-playlist`), v1 scope.
- Files are on Kaggle disk — when a session dies, unfinished files are
  gone; finished ones live only until the TTL deletes them.
- Bandwidth is the trycloudflare tunnel — fine for personal use, no
  SLA, big 4K files are not fast.

## Security notes

- Own password (never the stream or admin one), bearer tokens are
  HMAC-signed with a per-bot secret from wzfix_config, 72 h expiry.
- Login attempts are rate-limited per IP and wrong passwords are
  reported to LOG_CHAT (same style as the dashboard alerts).
- URLs are restricted to http(s); yt-dlp flags come only from the
  server side; the frontend can only pick a format id.
- UA filtering on the page + login, like wzadmin.
