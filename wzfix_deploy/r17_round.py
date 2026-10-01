#!/usr/bin/env python3
"""WZFIX R17 payload (v15.84) — web downloader.

Fetches _remote_r17_webdl.py from GitHub first, then Drive (sha256-pinned), writes it into
bot/helper/wzfix/, registers the /webdl routes on the bot stream
server (same /_dl anchor the wzadmin round uses) and adds the wserver
/webdl proxy with true streaming for file downloads (the buffered
/wzadmin proxy pattern would OOM on multi-GB files and its default
300 s total timeout would cut off long phone downloads).

Remote payload pattern (same as _real_deploy_patch.py) because the
Kaggle notebook kernel source must stay under 1 MB. Idempotent: every
edit is marker-checked; on any failure exits 1 and the notebook logs
it and keeps booting (webdl is skipped, the bot is unaffected).

GitHub is primary; the existing Drive file is the backup. Keep each
backup byte-identical to its GitHub source so the shared SHA pin matches.
"""
import hashlib
import os
import subprocess
import sys
import urllib.request

MODULE_GITHUB_URL = (
    "https://raw.githubusercontent.com/vot1122/Kaggle-auto/main/"
    "_remote_r17_webdl.py"
)
MODULE_DRIVE_URL = (
    "https://drive.usercontent.google.com/download?"
    "id=1bV2f-VG1R4FCZoJSCyQxzSAd44aJr3Bi&export=download&confirm=t"
)
MODULE_SHA = "2c4c27aef90aa709d364483964449fbe82f8c77c229b3b368fe0d3d1efa24e84"

WZMLX = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else ".")


def die(msg):
    print(f"r17: {msg}")
    sys.exit(1)


def main():
    # 1. module
    raw = None
    errors = []
    for source, url in (("GitHub", MODULE_GITHUB_URL),
                        ("Google Drive", MODULE_DRIVE_URL)):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as response:
                candidate = response.read()
            digest = hashlib.sha256(candidate).hexdigest()
            if digest != MODULE_SHA:
                raise ValueError(f"SHA-256 mismatch: {digest}")
            raw = candidate
            print(f"r17: module fetched from {source} and verified")
            break
        except Exception as e:
            errors.append(f"{source}: {e}")
    if raw is None:
        die("module fetch failed: " + " | ".join(errors))
    wzdir = os.path.join(WZMLX, "bot", "helper", "wzfix")
    os.makedirs(wzdir, exist_ok=True)
    mod = os.path.join(wzdir, "r17_webdl.py")
    with open(mod, "w", encoding="utf-8") as f:
        f.write(raw.decode("utf-8"))
    r = subprocess.run([sys.executable, "-m", "py_compile", mod],
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        die(f"r17_webdl.py compile FAILED: {(r.stderr or '')[-300:]}")
    print("r17: r17_webdl.py written (compiles)")

    # 2. stream server routes
    ss = os.path.join(WZMLX, "bot", "core", "stream_server.py")
    with open(ss, "r", encoding="utf-8") as f:
        s = f.read()
    if "webdl_routes" in s:
        print("r17: stream server already has webdl")
    else:
        anchor = '    app.router.add_route("*", "/_dl/{token}", _dl)'
        routes = (
            '    # WZFIX R17: web downloader (v15.84)\n'
            '    try:\n'
            '        from ..helper.wzfix.r17_webdl import webdl_routes\n'
            '        webdl_routes(app)\n'
            '    except Exception as e:\n'
            '        LOGGER.error(f"r17 webdl routes: {e}")\n'
        )
        if anchor in s:
            s = s.replace(anchor, anchor + "\n" + routes, 1)
            with open(ss, "w", encoding="utf-8") as f:
                f.write(s)
            print("r17: stream server routes registered (/webdl)")
        else:
            print("r17: WARN stream server /_dl anchor missing")

    # 3. wserver proxy (streamed for /webdl/dl, buffered otherwise)
    ws = os.path.join(WZMLX, "web", "wserver.py")
    with open(ws, "r", encoding="utf-8") as f:
        w = f.read()
    if "WZFIX_R17_WSERVER" in w:
        print("r17: wserver already patched")
    else:
        proxy = (
            '@app.api_route("/webdl", methods=["GET", "POST", "OPTIONS"])  # WZFIX_R17_WSERVER\n'
            '@app.api_route("/webdl/{path:path}", methods=["GET", "POST", "OPTIONS"])  # WZFIX_R17_WSERVER\n'
            "async def webdl_proxy(request: Request):\n"
            '    _target = f"{STREAM_BASE}{request.url.path}"\n'
            "    if request.url.query:\n"
            '        _target += f"?{request.url.query}"\n'
            "    _fwd = {k: v for k, v in request.headers.items()\n"
            '            if k.lower() not in ("host", "content-length", "accept-encoding")}\n'
            "    _body = await request.body()\n"
            "    try:\n"
            '        if request.url.path.startswith("/webdl/dl/"):\n'
            "            # large file: stream, no total timeout (5 min would\n"
            "            # kill long phone downloads)\n"
            '            from aiohttp import ClientTimeout\n'
            "            _sess = getattr(request.app.state, \"webdl_stream_session\", None)\n"
            "            if _sess is None or _sess.closed:\n"
            "                _sess = ClientSession(\n"
            "                    auto_decompress=True,\n"
            "                    timeout=ClientTimeout(total=None, connect=30,\n"
            "                                          sock_read=600))\n"
            "                request.app.state.webdl_stream_session = _sess\n"
            "            _up = await _sess.request(request.method, _target,\n"
            "                                      headers=_fwd, data=_body)\n\n"
            "            async def _wzdl_gen():\n"
            "                try:\n"
            "                    async for _chunk in _up.content.iter_any():\n"
            "                        yield _chunk\n"
            "                finally:\n"
            "                    _up.release()\n\n"
            "            return StreamingResponse(\n"
            "                _wzdl_gen(),\n"
            "                status_code=_up.status,\n"
            "                headers={k: v for k, v in _up.headers.items()\n"
            '                         if k.lower() not in ("transfer-encoding", "content-length",\n'
            '                                              "content-encoding", "connection")},\n'
            "            )\n"
            "        async with http_session.request(\n"
            "                request.method, _target, headers=_fwd, data=_body) as _up:\n"
            "            _raw = await _up.read()\n"
            "            return Response(\n"
            "                content=_raw, status_code=_up.status,\n"
            "                headers={k: v for k, v in _up.headers.items()\n"
            '                         if k.lower() not in ("transfer-encoding", "content-length",\n'
            '                                              "content-encoding", "connection")},\n'
            '                media_type=_up.headers.get("content-type"),\n'
            "            )\n"
            "    except Exception as _e:\n"
            '        return JSONResponse({"error": f"webdl upstream unreachable: {_e.__class__.__name__}"},\n'
            "                            status_code=502)\n"
        )
        anchor_home = '@app.get("/", response_class=HTMLResponse)'
        anchor_wza = '@app.api_route("/wzadmin", methods=["GET"])'
        if anchor_home in w:
            w = w.replace(anchor_home, proxy + "\n\n" + anchor_home, 1)
            with open(ws, "w", encoding="utf-8") as f:
                f.write(w)
            print("r17: wserver patched (/webdl proxy, streaming dl)")
        elif anchor_wza in w:
            w = w.replace(anchor_wza, proxy + "\n" + anchor_wza, 1)
            with open(ws, "w", encoding="utf-8") as f:
                f.write(w)
            print("r17: wserver patched (wzadmin anchor)")
        else:
            print("r17: WARN wserver anchors missing")
    print("r17: done")


main()
