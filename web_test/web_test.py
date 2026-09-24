"""Web UI + dashboard smoke test for the WZML-X deployment.

v2: adds /wzadmin dashboard probes with a browser UA (the dashboard
blocks script UAs) and POST support for the login endpoint tests.
Pure stdlib.
"""

import http.client
import json
import ssl
import time
from urllib.parse import urlparse

BASE = "https://twilight-thunder-4d48.joshifreefire-joshi.workers.dev"
QPASS = "52731cc8ab38b88410994a20"
NPASS = "c23d8198078c635d61cd5ad4"

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
SCRIPT_UA = "wz-web-test/1.0"

_ctx = ssl.create_default_context()
HOST = urlparse(BASE).hostname

TESTS = [
    # (label, method, path, body, expect, ua)
    ("landing page", "GET", "/", None, 200, SCRIPT_UA),
    ("owner dashboard page", "GET", "/wzadmin", None, 200, BROWSER_UA),
    ("dashboard page script-UA blocked", "GET", "/wzadmin", None, 403, SCRIPT_UA),
    ("heartbeat /_ping", "GET", "/_ping", None, 200, BROWSER_UA),
    ("state without session", "GET", "/wzadmin/api/state", None, 401, BROWSER_UA),
    ("action without session", "POST", "/wzadmin/api/action",
     {"do": "report"}, 401, BROWSER_UA),
    ("login script-UA blocked", "POST", "/wzadmin/api/login",
     {"pass": "11"}, 403, SCRIPT_UA),
    ("login wrong pass", "POST", "/wzadmin/api/login",
     {"pass": "not-the-pass"}, None, BROWSER_UA),
    ("login right pass (datacenter shield)", "POST", "/wzadmin/api/login",
     {"pass": "11"}, None, BROWSER_UA),
    ("file manager page", "GET", "/app/files", None, 200, SCRIPT_UA),
    ("pin api no gid", "GET", "/app/files/torrent", None, 200, SCRIPT_UA),
    ("qbit ui valid pass", "GET", f"/qbit/?pass={QPASS}", None, 200, SCRIPT_UA),
    ("qbit api valid pass", "GET", f"/qbit/api/v2/app/version?pass={QPASS}",
     None, 200, SCRIPT_UA),
    ("qbit wrong pass", "GET", "/qbit/?pass=wrongpass", None, 403, SCRIPT_UA),
    ("nzb ui valid pass (known broken)", "GET", f"/nzb/?pass={NPASS}",
     None, 500, SCRIPT_UA),
    ("nzb wrong pass", "GET", "/nzb/api?mode=version&output=json&pass=wrongpass",
     None, 403, SCRIPT_UA),
    ("stream bogus token", "GET", "/stream/bogus1234", None, 404, SCRIPT_UA),
    ("download bogus token", "GET", "/dl/bogus1234", None, 404, SCRIPT_UA),
    ("unknown page 404", "GET", "/definitely-not-a-page", None, 404, SCRIPT_UA),
]


def probe(method, path, body, ua):
    t0 = time.time()
    try:
        conn = http.client.HTTPSConnection(HOST, timeout=20, context=_ctx)
        headers = {"User-Agent": ua}
        data = None
        if body is not None:
            data = json.dumps(body)
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=data, headers=headers)
        resp = conn.getresponse()
        raw = resp.read(2048)
        ms = int((time.time() - t0) * 1000)
        info = {
            "status": resp.status,
            "ms": ms,
            "ctype": (resp.getheader("Content-Type") or "?").split(";")[0],
            "snippet": " ".join(raw[:250].decode("utf-8", "replace").split())[:150],
        }
        conn.close()
        return info, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def main():
    lines = ["# web test results (v2 — dashboard)",
             "", "| check | status | ms | type | result |",
             "|---|---|---|---|---|"]
    pass_n = fail_n = err_n = 0
    for label, method, path, body, expect, ua in TESTS:
        info, err = probe(method, path, body, ua)
        if err:
            err_n += 1
            lines.append(f"| {label} | ERR | - | - | {err} |")
            continue
        s = info["status"]
        if expect is not None:
            ok = (s == expect)
        else:
            ok = s < 500
        verdict = "PASS" if ok else "FAIL"
        pass_n, fail_n = (pass_n + 1, fail_n) if ok else (pass_n, fail_n + 1)
        lines.append(
            f"| {label} | {s} {verdict} | {info['ms']} | {info['ctype']} | {info['snippet']} |"
        )
    lines += ["", f"**{pass_n} pass / {fail_n} fail / {err_n} error**", ""]
    with open("web_test/last-result.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
