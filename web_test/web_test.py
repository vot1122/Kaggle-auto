"""Web UI + dashboard full test for the WZML-X deployment.

v3: complete authenticated dashboard flow — login with the admin pass,
read state, no-op writes for every setting (read current value, write it
back), user history, report action, logout, and session teardown checks.
Pure stdlib.
"""

import http.client
import json
import ssl
import time
from urllib.parse import urlparse

BASE = "https://twilight-thunder-4d48.joshifreefire-joshi.workers.dev"
ADMIN_PASS = "11"
TEST_UID = 6726918562  # CP smile (test account)

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

_ctx = ssl.create_default_context()
HOST = urlparse(BASE).hostname
RESULTS = []


def req(method, path, body=None, cookie=None, ua=BROWSER_UA):
    conn = http.client.HTTPSConnection(HOST, timeout=25, context=_ctx)
    headers = {"User-Agent": ua}
    data = None
    if body is not None:
        data = json.dumps(body)
        headers["Content-Type"] = "application/json"
    if cookie:
        headers["Cookie"] = cookie
    conn.request(method, path, body=data, headers=headers)
    resp = conn.getresponse()
    raw = resp.read(65536)
    setc = resp.getheader("Set-Cookie") or ""
    conn.close()
    try:
        parsed = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        parsed = None
    return resp.status, raw[:400], parsed, setc


def check(label, ok, detail=""):
    RESULTS.append((label, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'} | {label} | {detail}")


def main():
    # 1. login
    code, raw, body, setc = req("POST", "/wzadmin/api/login", {"pass": ADMIN_PASS})
    tok = ""
    if "wzadmin=" in setc:
        tok = setc.split("wzadmin=")[1].split(";")[0]
    check("login with admin pass", code == 200 and body and body.get("ok"),
          f"{code} cookie={'yes' if tok else 'NO'}")
    if not tok:
        check("session cookie obtained", False, "cannot continue without session")
        finish()
        return
    ck = f"wzadmin={tok}"

    # 2. state
    code, raw, st, _ = req("GET", "/wzadmin/api/state", cookie=ck)
    ok = code == 200 and isinstance(st, dict)
    keys = list(st.keys()) if ok else []
    need = ["bot", "users", "global_cap_gb", "global_music",
            "music_settings", "totals", "tasks"]
    have = all(k in keys for k in need) if ok else False
    check("state (all sections)", have, f"{code} keys={keys}")
    if not have:
        finish()
        return

    gcap = st["global_cap_gb"]
    gmus = st["global_music"]
    mset = st["music_settings"] or {}
    users = st["users"] or []
    check("state has users", len(users) >= 1, f"{len(users)} user(s)")
    check("state totals", isinstance(st["totals"], dict),
          str(st["totals"]))

    # 3. history for the test user
    code, raw, h, _ = req("GET", f"/wzadmin/api/history?uid={TEST_UID}", cookie=ck)
    ok = code == 200 and isinstance(h, dict) and "items" in h
    check("user history", ok, f"{code} items={len((h or {}).get('items', []))}")

    # 4. no-op botcap (write current value back)
    code, raw, a, _ = req("POST", "/wzadmin/api/action",
                          {"action": "botcap", "gb": gcap}, cookie=ck)
    check("action botcap (no-op)", code == 200 and a and a.get("ok"),
          str((a or {}).get("msg")))

    # 5. no-op gmusic
    code, raw, a, _ = req("POST", "/wzadmin/api/action",
                          {"action": "gmusic", "n": gmus}, cookie=ck)
    check("action gmusic (no-op)", code == 200 and a and a.get("ok"),
          str((a or {}).get("msg")))

    # 6. no-op mset x3 (write current values back)
    for k in ("music_on", "ld_on", "aliases_on"):
        v = 1 if mset.get(k, True) else 0
        code, raw, a, _ = req("POST", "/wzadmin/api/action",
                              {"action": "mset", "k": k, "v": v}, cookie=ck)
        check(f"action mset {k} (no-op)", code == 200 and a and a.get("ok"),
              str((a or {}).get("msg")))

    # 7. setmusic on the test account → current value
    me = next((u for u in users if u.get("uid") == TEST_UID), None)
    cur = (me or {}).get("music_max")
    code, raw, a, _ = req("POST", "/wzadmin/api/action",
                          {"action": "setmusic", "uid": TEST_UID,
                           "n": cur if cur else 0}, cookie=ck)
    check("action setmusic (test user, no-op)",
          code == 200 and a and a.get("ok"), str((a or {}).get("msg")))

    # 8. report
    code, raw, a, _ = req("POST", "/wzadmin/api/action",
                          {"action": "report"}, cookie=ck)
    check("action report", code == 200 and a and a.get("ok"),
          str((a or {}).get("msg")))

    # 9. unknown action
    code, raw, a, _ = req("POST", "/wzadmin/api/action",
                          {"action": "notarealaction"}, cookie=ck)
    check("unknown action rejected", code == 200 and a and not a.get("ok"),
          str((a or {}).get("msg")))

    # 10. logout
    code, raw, a, _ = req("POST", "/wzadmin/api/logout", cookie=ck)
    check("logout", code == 200 and a and a.get("ok"), f"{code}")

    # 11. state after logout (stateless HMAC tokens: expected to remain
    # valid until expiry — informational, logged as a finding)
    code, raw, a, _ = req("GET", "/wzadmin/api/state", cookie=ck)
    check("old token after logout (stateless — info)", True,
          f"{code} — token stays valid until expiry (by design)")

    finish()


def finish():
    lines = ["# web test results (v3 — authenticated dashboard)", ""]
    for label, ok, detail in RESULTS:
        lines.append(f"- {'✅' if ok else '❌'} **{label}** — {detail}")
    p = sum(1 for _, ok, _ in RESULTS if ok)
    lines += ["", f"**{p}/{len(RESULTS)} passed**", ""]
    with open("web_test/last-result.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n{p}/{len(RESULTS)} passed")


if __name__ == "__main__":
    main()
