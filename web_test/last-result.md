# web test results (v3 — authenticated dashboard)

- ✅ **login with admin pass** — 200 cookie=yes
- ✅ **state (all sections)** — 200 keys=['ok', 'access', 'day', 'bot', 'users', 'tasks', 'global_cap_gb', 'global_music', 'music_settings', 'totals']
- ✅ **state has users** — 3 user(s)
- ✅ **state totals** — {'used': 343932054, 'reserved': 0, 'users': 3, 'tasks': 0}
- ✅ **user history** — 200 items=25
- ✅ **action botcap (no-op)** — default cap → 15 GB
- ✅ **action gmusic (no-op)** — default music limit → 10 songs
- ✅ **action mset music_on (no-op)** — music_on → on
- ✅ **action mset ld_on (no-op)** — ld_on → on
- ✅ **action mset aliases_on (no-op)** — aliases_on → on
- ✅ **action setmusic (test user, no-op)** — music limit for 6726918562 → default
- ✅ **action report** — report sent
- ✅ **unknown action rejected** — unknown action: notarealaction
- ✅ **streampass: dashboard spset** — webtesttoken123 now needs its own password
- ✅ **streampass: correct password mints link token** — 200 link=webtesttoken123 tok=yes
- ✅ **streampass: wrong password rejected** — 401
- ❌ **streampass: gated link meta blocked without auth** — 404 hdr-probe
- ❌ **streampass: gated link data blocked without auth** — 404 (data path)
- ✅ **streampass: dashboard splist shows the link** — webtesttoken123 → testpw42; vaRKGIQ → testpw1
- ✅ **streampass: dashboard spdel** — removed — back to the global password
- ✅ **streampass: after del, link falls back to global** — 401 'error': 'wrong password'} (info)
- ✅ **loop-fix: real gated link probe (info)** — 404 — link or its password no longer present
- ❌ **loop-fix: stream page serves the v15.65 fixes** — 200 jsfix=no urlfix=no
- ✅ **logout** — 200
- ✅ **old token after logout (stateless — info)** — 200 — token stays valid until expiry (by design)

**22/25 passed**
