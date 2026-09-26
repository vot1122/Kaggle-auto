#!/usr/bin/env python3
"""One-shot patch: v15.74 -> v15.75 (Round 16).

Direct-download auth page: opening a protected stream link directly in
a browser (the Telegram download option, /dl/<token>?user=1) showed raw
"authenticate first" text because the password modal only lives inside
the player page. Browser navigations now get a standalone password
page; it mints a token via the existing /_auth route, stores it in the
same localStorage slot the player uses, and reloads the original URL
authenticated. Player fetches (Accept without text/html) keep the raw
401 + X-Stream-Auth-Required flow that drives the modal.
"""
import ast
import hashlib
import sys

INPUT_SHA256 = "fe1d9612f15d4af6e003a15ae02c3057a4b62796ea8e83cb3295cfbcf47ff67c"
EXPECT_SHA256 = "0fc74812e816e0994e11c560229bdad5694477c03b080757a93bd96b098cb7f0"

HTML_LINES = [
    "<!DOCTYPE html>",
    '<html lang="en">',
    "<head>",
    '<meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width, initial-scale=1">',
    "<title>Stream password</title>",
    "<style>",
    "body{background:#0a0a12;color:#e6e6f0;font-family:system-ui,-apple-system,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}",
    ".card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:2.2rem 2rem;width:min(92vw,360px);text-align:center;backdrop-filter:blur(12px)}",
    "h1{font-size:1.05rem;font-weight:600;margin:0 0 .4rem}",
    "p{font-size:.85rem;color:#8a8aa0;margin:0 0 1.4rem}",
    "input{width:100%;box-sizing:border-box;background:rgba(0,0,0,.35);border:1px solid rgba(255,255,255,.12);border-radius:10px;color:#fff;padding:.75rem .9rem;font-size:1rem;outline:none}",
    "input:focus{border-color:#7c6cf0}",
    "button{width:100%;margin-top:1rem;background:linear-gradient(135deg,#7c6cf0,#5a8bf0);color:#fff;border:none;border-radius:10px;padding:.8rem;font-size:1rem;font-weight:600;cursor:pointer}",
    ".msg{color:#f0806c;font-size:.8rem;min-height:1.1rem;margin-top:.8rem}",
    "</style>",
    "</head>",
    "<body>",
    '<div class="card">',
    "<h1>This stream is password protected</h1>",
    "<p>Enter the stream password to download or play the file.</p>",
    '<input id="pw" type="password" placeholder="Stream password" autofocus>',
    '<button id="go">Unlock</button>',
    '<div class="msg" id="msg"></div>',
    "</div>",
    "<script>",
    "(function(){",
    'var M=document.getElementById("msg"),P=document.getElementById("pw");',
    "function applyToken(t){",
    '  try{localStorage.setItem("wzml_stream_auth",JSON.stringify({token:t,ts:Date.now()}))}catch(e){}',
    "  var u=new URL(location.href);",
    '  u.searchParams.set("auth",t);',
    "  location.replace(u.toString());",
    "}",
    "function tryStored(){",
    "  try{",
    '    var raw=localStorage.getItem("wzml_stream_auth");',
    "    if(!raw)return null;",
    "    var d=JSON.parse(raw);",
    '    if(!d.token||Date.now()-d.ts>24*3600*1000){localStorage.removeItem("wzml_stream_auth");return null}',
    "    return d.token;",
    "  }catch(e){return null}",
    "}",
    "var st=tryStored();",
    'if(st&&!new URL(location.href).searchParams.get("auth")){applyToken(st);return}',
    'document.getElementById("go").onclick=function(){go()};',
    'P.onkeydown=function(e){if(e.key==="Enter")go()};',
    "async function go(){",
    '  M.textContent="";',
    "  try{",
    '    var r=await fetch("/_auth",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({password:P.value})});',
    "    var d=await r.json();",
    "    if(d.token){applyToken(d.token)}",
    '    else{M.textContent=d.error||"wrong password"}',
    '  }catch(e){M.textContent="auth unavailable, try the player link"}',
    "}",
    "})();",
    "</script>",
    "</body>",
    "</html>",
]


GATE_OLD_LINES = [
    '    if request.query.get("user") == "1" and not _us_check_auth(request) and not await _r5_link_token_ok(request):',
    "        raise web.HTTPUnauthorized(",
    '            text="authenticate first",',
    '            headers={"X-Stream-Auth-Required": "1"},',
    "        )",
]


GATE_NEW_LINES = [
    "    # WZFIX_R16_DL_AUTH: a direct browser open of a",
    "    # protected stream (the Telegram download link) must",
    "    # see the password page - the password modal only",
    "    # exists inside the player page",
    "    if (",
    '        request.query.get("user") == "1"',
    "        and not _us_check_auth(request)",
    "        and not await _r5_link_token_ok(request)",
    "    ):",
    '        if "text/html" in (request.headers.get("Accept") or ""):',
    "            return _dl_auth_page()",
    "        raise web.HTTPUnauthorized(",
    '            text="authenticate first",',
    '            headers={"X-Stream-Auth-Required": "1"},',
    "        )",
]



def _chunk(lines):
    out = ""
    for ln in lines:
        out = out + "                " + chr(39) + ln + chr(92) + "n" + chr(39) + chr(10)
    return out


R16 = (
    "    # R16 (v15.75): direct-download auth page. When a protected\n"
    "    # stream link is opened directly in a browser (the Telegram\n"
    "    # download option, /dl/<token>?user=1), the password modal that\n"
    "    # lives inside the player page never loads, so the user saw raw\n"
    "    # authenticate-first text. Browser navigations now get a standalone\n"
    "    # password page: it mints a token via the existing /_auth route,\n"
    "    # stores it in the same localStorage slot the player uses, and\n"
    "    # reloads the original URL authenticated. Player fetches keep the\n"
    "    # raw 401 + X-Stream-Auth-Required flow that drives the modal.\n"
    "    try:\n"
    '        _ss16 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")\n'
    '        with open(_ss16, "r", encoding="utf-8") as _f:\n'
    "            _s16 = _f.read()\n"
    '        if "WZFIX_R16_DL_AUTH" in _s16:\n'
    '            log("  r16: download auth page already present")\n'
    "        else:\n"
    "            _html16 = (\n"
    + _chunk(HTML_LINES)
    + "            )\n"
    "            _t16 = chr(39) * 3\n"
    "            _q16 = chr(34)\n"
    "            _tph16 = chr(37) + chr(84) + chr(37)\n"
    "            _qph16 = chr(37) + chr(81) + chr(37)\n"
    "            _nph16 = chr(37) + chr(78) + chr(37)\n"
    "            _page16 = (\n"
    '                "_DL_AUTH_HTML = %T%" + _html16 + "%T%%N%%N%%N%"\n'
    '                "def _dl_auth_page():  # WZFIX_R16_DL_AUTH%N%"\n'
    '                "    return web.Response(%N%"\n'
    '                "        text=_DL_AUTH_HTML,%N%"\n'
    '                "        content_type=%Q%text/html%Q%,%N%"\n'
    '                "        status=401,%N%"\n'
    '                "        headers={%N%"\n'
    '                "            %Q%X-Stream-Auth-Required%Q%: %Q%1%Q%,%N%"\n'
    '                "            %Q%Cache-Control%Q%: %Q%no-store%Q%,%N%"\n'
    '                "        },%N%"\n'
    '                "    )%N%"\n'
    '                "%N%%N%"\n'
    '                "async def _serve(request, kind):"\n'
    "            ).replace(_tph16, _t16).replace(_qph16, _q16).replace(_nph16, chr(10))\n"
    "            _gate16_old = (\n"
    + _chunk(GATE_OLD_LINES)
    + "            )\n"
    "            _gate16_new = (\n"
    + _chunk(GATE_NEW_LINES)
    + "            )\n"
    "            _ok16 = 0\n"
    "            if _gate16_old in _s16:\n"
    "                _s16 = _s16.replace(_gate16_old, _gate16_new, 1)\n"
    "                _ok16 += 1\n"
    "            else:\n"
    '                log("  r16: gate anchor missing", "WARN")\n'
    '            if "async def _serve(request, kind):" in _s16:\n'
    "                _s16 = _s16.replace(\n"
    '                    "async def _serve(request, kind):", _page16, 1\n'
    "                )\n"
    "                _ok16 += 1\n"
    "            else:\n"
    '                log("  r16: _serve anchor missing", "WARN")\n'
    "            if _ok16 == 2:\n"
    '                with open(_ss16, "w", encoding="utf-8") as _f:\n'
    "                    _f.write(_s16)\n"
    "                _r16 = subprocess.run(\n"
    '                    [sys.executable, "-m", "py_compile", _ss16],\n'
    "                    capture_output=True,\n"
    "                    text=True,\n"
    "                    timeout=60,\n"
    "                )\n"
    "                if _r16.returncode == 0:\n"
    '                    log("  r16: download auth page applied")\n'
    "                else:\n"
    '                    log("  r16: compile FAILED: see the boot log", "ERROR")\n'
    "            else:\n"
    '                log("  r16: anchors incomplete - not written", "WARN")\n'
    "    except Exception as e:\n"
    '        log(f"  r16: FAILED - {e}", "ERROR")\n'
    "\n"
)

frag = R16
ast.parse("def _wrap():\n" + frag + "\n")

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.75" in s:
    print("already v15.75 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.74 build")

anchor = (
    "    # Kaggle addition I — v15.7 Round 1: per-user bandwidth quota + download\n"
)
assert s.count(anchor) == 1, f"anchor count {s.count(anchor)}"
s = s.replace(anchor, frag + anchor, 1)

n = s.count("v15.74")
s = s.replace("v15.74", "v15.75")
assert "v15.74" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r16 download auth page, {n} markers bumped to v15.75")
