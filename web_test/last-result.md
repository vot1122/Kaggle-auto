# web test results (v3 — authenticated dashboard)

- ✅ **login with admin pass** — 200 cookie=yes
- ✅ **state (all sections)** — 200 keys=['ok', 'day', 'bot', 'users', 'tasks', 'global_cap_gb', 'global_music', 'music_settings', 'totals']
- ✅ **state has users** — 3 user(s)
- ✅ **state totals** — {'used': 328906940, 'reserved': 0, 'users': 3, 'tasks': 0}
- ✅ **user history** — 200 items=24
- ✅ **action botcap (no-op)** — default cap → 15 GB
- ✅ **action gmusic (no-op)** — default music limit → 10 songs
- ❌ **action mset music_on (no-op)** — setting failed: No module named 'bot.helper.helper'
- ❌ **action mset ld_on (no-op)** — setting failed: No module named 'bot.helper.helper'
- ❌ **action mset aliases_on (no-op)** — setting failed: No module named 'bot.helper.helper'
- ✅ **action setmusic (test user, no-op)** — music limit for 6726918562 → default
- ✅ **action report** — report sent
- ✅ **unknown action rejected** — unknown action: notarealaction
- ✅ **logout** — 200
- ✅ **old token after logout (stateless — info)** — 200 — token stays valid until expiry (by design)

**12/15 passed**
