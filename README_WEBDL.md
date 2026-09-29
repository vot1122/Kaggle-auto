# WZFIX R17 — Web Downloader (v15.84)

The bot is now a downloader site. Paste a link on a web page, the bot's
yt-dlp engine grabs it server-side, and the finished file downloads
straight to your device — no Telegram chat, no upload, no Drive mirror.
The Telegram bot is untouched.

## Architecture (remote payload — the 1 MB kernel cap)

The Kaggle kernel source must stay under 1 MB, so R17 follows the
_real_deploy_patch pattern: the notebook carries only a tiny
sha256-pinned round (~40 lines) which fetches two public Drive files
at boot:

- `wzfix_r17_round.py` — installs the module, registers the /webdl
  routes on the stream server and adds the wserver /webdl proxy
  (streamed for file downloads)
- `wzfix_r17_webdl.py` — the module itself

Both files are also mirrored in `wzfix_deploy/` in this repo.

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

## Honest limits (by design, this round)

- **yt-dlp links only** (YouTube, Instagram, direct links).
  Torrents/aria2/qbit/mirroring are R19.
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
