# WZFIX R17 — Web Downloader (v15.84)

The bot is now a downloader site. Paste a link on a web page, the bot's
yt-dlp engine grabs it server-side, and the finished file downloads
straight to your device — no Telegram chat, no upload, no Drive mirror.
The Telegram bot is untouched.

## Architecture (remote payload — the 1 MB kernel cap)

The Kaggle kernel source must stay under 1 MB. The notebook fetches
`wzfix_deploy/r17_round.py` from raw GitHub first and falls back to its
SHA-verified Google Drive copy. That round fetches `_remote_r17_webdl.py`
from raw GitHub first, then falls back to its SHA-verified Drive copy.
Both mirrors must contain byte-identical copies for the pinned hash to pass.

| Repository source | Installed/Drive filename | Google Drive backup ID |
|---|---|---|
| `wzfix_deploy/r17_round.py` | `r17_round.py` | `16VVAlGx2m0YsLtb0A0Tv1oZIf5EXIkbl` |
| `_remote_r17_webdl.py` | `r17_webdl.py` | `1bV2f-VG1R4FCZoJSCyQxzSAd44aJr3Bi` |

Update the contents of those existing Drive files (do not create new files,
which would have different IDs). In Drive, use **Manage versions → Upload new
version**. Commit/push the two repository sources before the next Kaggle boot
so the GitHub copies are available; the notebook changes also need to be
included in the Kaggle notebook source.

    GitHub Pages ─┐
    (vot1122.github.io/ytwebdownload)  Cloudflare Worker (unchanged)
                   │                          │
                   │        https://<worker>/webdl
                   ▼                          ▼
              browser ──── CORS ────► wserver:8080 ──► stream server:8091
                                            (new /webdl proxy,      (new r17 module
                                             streaming for files)    + yt-dlp engine)

**v15.84.1 fix:** the round must not `import urllib.request` inside
apply_userrepo_patches — a local import makes `urllib` function-local
and the patch-kit download above it dies with UnboundLocalError,
skipping the whole kit (wzadmin, stream pages, everything) for that
boot. The round now uses the module-level urllib and `__import__(
"hashlib")`.

## The website

- **Direct:** `https://<worker>/webdl` — always works, served by the
  bot itself.
- **GitHub Pages:** https://vot1122.github.io/ytwebdownload/ — first
  visit: click ⚙ and paste your worker URL (saved in localStorage).
  CORS is emitted by the bot (allowed origins include the Pages URL).
- **Default password: `joshi`** (change via WEBDL_PASS in /bs or
  config.env; wrong passwords are rate-limited and reported to
  LOG_CHAT).

## Config knobs (wzfix_config doc "webdl" / config.env overrides)

| Key | Default | Meaning |
|---|---|---|
| `WEBDL_PASS` | joshi | login password |
| `WEBDL_TTL` | 6 (hours) | finished files auto-delete |
| `WEBDL_MAX_GB` | 8 | disk budget for webdl files |
| `WEBDL_CONC` | 2 | concurrent downloads |
| `WEBDL_ORIGINS` | Pages URLs + localhost | CORS allow-list |

## Coverage and limits

- The web downloader uses yt-dlp's supported extractors for a broad range
  of video and social sites, then tries a bounded direct HTTP download when
  extraction fails. A direct fallback succeeds only when the URL resolves
  to a file response, not an HTML share page or login screen.
- This is broad coverage, not literally every website. CAPTCHA, login,
  premium, DRM, and unsupported site-specific flows still need valid access
  or a dedicated extractor. Successful web downloads use the same limits,
  accounting, and completion path as yt-dlp downloads.
- Torrents/aria2/qbit/mirroring are R19.
- The site only works while the bot session is awake (06:00–23:00 IST,
  plus restart windows). The page shows an offline banner and retries.
- Playlists download only the single item (`--no-playlist`).
- Files are on Kaggle disk — when a session dies, unfinished files are
gone; finished ones live only until the TTL deletes them (6 h).
- Bandwidth is the trycloudflare tunnel — fine for personal use, no SLA.

## Security notes

- Separate password (never the stream or admin one); bearer tokens are
HMAC-signed with a per-bot secret, 72 h expiry.
- Login is rate-limited per IP (5 fails → 10 min lock) and failures go
to LOG_CHAT.
- URLs are restricted to http(s); yt-dlp flags come only from the
server side; the frontend can only pick a format id.
- UA filtering on the page and login, like wzadmin.
