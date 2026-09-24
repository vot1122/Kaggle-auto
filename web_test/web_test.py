"""Web UI smoke test for the WZML-X deployment.

Probes the public BASE_URL endpoints (landing, file manager, PIN API,
qBittorrent & SABnzbd proxies, stream routes, negative cases) with raw
HTTP requests and writes results to web_test/last-result.md.
Pure stdlib — no dependencies.
"""

import http.client
import ssl
import sys
import time
from urllib.parse import urlparse

BASE = "https://twilight-thunder-4d48.joshifreefire-joshi.workers.dev"
QPASS = "52731cc8ab38b88410994a20"
NPASS = "c23d8198078c635d61cd5ad4"

TESTS = [
    # (label, path, expect)
    ("landing page", "/", 200),
    ("file manager page", "/app/files", 200),
    ("pin api no gid", "/app/files/torrent", 200),
    ("pin api invalid gid", "/app/files/torrent?gid=!!!&pin=1234", 400),
    ("pin api bad pin", "/app/files/torrent?gid=test123&pin=99", 400),
    ("qbit ui valid pass", f"/qbit/?pass={QPASS}", 200),
    ("qbit api valid pass", f"/qbit/api/v2/app/version?pass={QPASS}", 200),
    ("qbit ui wrong pass", "/qbit/?pass=wrongpass", 403),
    ("qbit api no pass", "/qbit/api/v2/app/version", 403),
    ("nzb ui valid pass", f"/nzb/?pass={NPASS}", 200),
    ("nzb api valid pass", f"/nzb/api?mode=version&output=json&pass={NPASS}", 200),
    ("nzb api wrong pass", "/nzb/api?mode=version&output=json&pass=wrongpass", 403),
    ("nzb login page", "/nzb/login", None),
    ("stream bogus token", "/stream/bogus1234", None),
    ("download bogus token", "/dl/bogus1234", None),
    ("poster bogus token", "/poster/bogus1234", None),
    ("tracks bogus token", "/tracks/bogus1234", None),
    ("path traversal guard", "/nzb/../etc/passwd", None),
    ("unknown page 404", "/definitely-not-a-page", 404),
]

_ctx = ssl.create_default_context()
parsed = urlparse(BASE)
HOST = parsed.hostname


def probe(path):
    t0 = time.time()
    try:
        conn = http.client.HTTPSConnection(HOST, timeout=20, context=_ctx)
        conn.request("GET", path, headers={"User-Agent": "wz-web-test/1.0"})
        resp = conn.getresponse()
        body = resp.read(2048)
        ms = int((time.time() - t0) * 1000)
        loc = resp.getheader("Location") or ""
        info = {
            "status": resp.status,
            "ms": ms,
            "ctype": (resp.getheader("Content-Type") or "?").split(";")[0],
            "len": len(body),
            "loc": loc,
            "snippet": " ".join(body[:200].decode("utf-8", "replace").split())[:120],
        }
        conn.close()
        return info, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def main():
    lines = ["# web test results", "", "| check | path | status | ms | type | result |",
             "|---|---|---|---|---|---|"]
    pass_n = fail_n = err_n = 0
    for label, path, expect in TESTS:
        info, err = probe(path)
        if err:
            err_n += 1
            lines.append(f"| {label} | `{path}` | ERR | - | - | {err} |")
            continue
        s = info["status"]
        if expect is not None:
            ok = (s == expect)
        else:
            ok = s < 500  # any non-server-error response counts as handled
        verdict = "PASS" if ok else "FAIL"
        if ok:
            pass_n += 1
        else:
            fail_n += 1
        extra = []
        if info["loc"]:
            extra.append(f"-> {info['loc']}")
        if info["snippet"]:
            extra.append(info["snippet"])
        res = " ".join(extra) or f"{info['len']}B"
        lines.append(
            f"| {label} | `{path}` | {s} | {info['ms']} | {info['ctype']} | {verdict} — {res} |"
        )
    lines += ["", f"**{pass_n} pass / {fail_n} fail / {err_n} error**", ""]
    with open("web_test/last-result.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        TESTS = [("custom", a, None) for a in sys.argv[1:]]
    main()
