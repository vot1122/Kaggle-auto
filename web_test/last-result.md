# web test results (v3 — authenticated dashboard)

- ✅ **login with admin pass** — 200 cookie=yes
- ✅ **state (all sections)** — 200 keys=['ok', 'day', 'bot', 'users', 'tasks', 'global_cap_gb', 'global_music', 'music_settings', 'totals']
- ✅ **state has users** — 3 user(s)
- ✅ **state totals** — {'used': 328906940, 'reserved': 0, 'users': 3, 'tasks': 0}
- ✅ **user history** — 200 items=24
- ❌ **action botcap (no-op)** — unknown action: 
- ❌ **action gmusic (no-op)** — unknown action: 
- ❌ **action mset music_on (no-op)** — unknown action: 
- ❌ **action mset ld_on (no-op)** — unknown action: 
- ❌ **action mset aliases_on (no-op)** — unknown action: 
- ❌ **action setmusic (test user, no-op)** — unknown action: 
- ❌ **action report** — unknown action: 
- ✅ **unknown action rejected** — unknown action: 
- ✅ **logout** — 200
- ❌ **session dead after logout** — 200

**7/15 passed**
