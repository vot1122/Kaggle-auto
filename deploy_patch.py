"""One-shot patch: v15.62 -> v15.63.

J-39 dashboard stream-password management: spset / spdel / splist
actions + UI buttons on /wzadmin, wired to r5_streampass.
"""

import ast
import base64
import py_compile
import re
import sys
import tempfile

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

m = re.search(r"WZFIX_WEB_B64 = \((.*?)\n\)", s, re.S)
if not m:
    sys.exit("WZFIX_WEB_B64 block not found")
web = base64.b64decode(ast.literal_eval("(" + m.group(1) + ")")).decode("utf-8")

REPL = [
('        if act == "report":\n            ok, msg = await send_report()\n            return {"ok": ok, "msg": msg}\n        return {"ok": False, "msg": f"unknown action: {act}"}', '        if act == "report":\n            ok, msg = await send_report()\n            return {"ok": ok, "msg": msg}\n        if act == "spset":\n            from .r5_streampass import extract_token, set_link_pass\n\n            tok = extract_token(str(a.get("link", "") or ""))\n            pw = str(a.get("pass", "") or "").strip()\n            if not tok:\n                return {"ok": False, "msg": "not a stream link or token"}\n            if not pw or len(pw) > 64 or " " in pw:\n                return {"ok": False, "msg": "password 1-64 chars, no spaces"}\n            await set_link_pass(tok, pw, 0)\n            await _action_log("LOCK STREAM LINK", {"link": tok})\n            return {"ok": True, "msg": f"{tok} now needs its own password"}\n        if act == "spdel":\n            from .r5_streampass import extract_token, del_link_pass\n\n            tok = extract_token(str(a.get("link", "") or ""))\n            if not tok:\n                return {"ok": False, "msg": "not a stream link or token"}\n            removed = await del_link_pass(tok)\n            await _action_log("UNLOCK STREAM LINK", {"link": tok})\n            return {"ok": True, "msg": "removed — back to the global password" if removed else "had no custom password"}\n        if act == "splist":\n            from .r5_streampass import all_link_passes\n\n            rows = await all_link_passes()\n            msg = "; ".join(d["_id"] + " → " + d["pass"] for d in rows)\n            return {"ok": True, "msg": msg or "no custom stream passwords set"}\n        return {"ok": False, "msg": f"unknown action: {act}"}'),
    ('        <button class="btn dng" onclick="killAll()">✕ Kill all tasks</button>', '        <button class="btn" onclick="askSpSet()">🔒 Lock stream link</button>\n        <button class="btn" onclick="askSpDel()">🔓 Unlock stream link</button>\n        <button class="btn" onclick="act({action:\'splist\'},this)">📋 Stream passwords</button>\n        <button class="btn dng" onclick="killAll()">✕ Kill all tasks</button>'),
    ('function askBotCap(){var v=prompt("Default cap for ALL users (GB)",""+(window._gcap||15));if(v===null)return;act({action:"botcap",gb:parseFloat(v)||0})}', 'function askBotCap(){var v=prompt("Default cap for ALL users (GB)",""+(window._gcap||15));if(v===null)return;act({action:"botcap",gb:parseFloat(v)||0})}\nfunction askSpSet(){var l=prompt("Stream link to lock (full URL or just the token)");if(l===null)return;var p=prompt("Password for this link (no spaces)");if(p===null||!p)return;act({action:\'spset\',link:l,pass:p})}\nfunction askSpDel(){var l=prompt("Stream link to unlock");if(l===null)return;act({action:\'spdel\',link:l})}'),
    ('"bans": "List dashboard bans", "lockdash": "Lock/unlock dashboard",', '"bans": "List dashboard bans", "lockdash": "Lock/unlock dashboard",\n    "streampass": "Per-link stream passwords",')
]

for i, (old, new) in enumerate(REPL, 1):
    n = web.count(old)
    if n != 1:
        sys.exit(f"anchor {i}: expected 1 match, found {n}")
    web = web.replace(old, new)

with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False,
                                 encoding="utf-8") as tf:
    tf.write(web)
    tmp = tf.name
try:
    py_compile.compile(tmp, doraise=True)
except Exception as e:
    sys.exit(f"patched r2_web compile FAILED: {e}")

new_b64 = base64.b64encode(web.encode("utf-8")).decode("ascii")
lines = [new_b64[k:k + 100] for k in range(0, len(new_b64), 100)]
block = "WZFIX_WEB_B64 = (\n" + "".join(
    f'    "{ln}"\n' for ln in lines
) + ")"
s = s[:m.start()] + block + s[m.end():]

n = s.count("v15.62")
s = s.replace("v15.62", "v15.63")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-39 dashboard streampass actions, {n} markers bumped to v15.63")
