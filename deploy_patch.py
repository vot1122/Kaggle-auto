"""One-shot patch: v15.58 -> v15.59.

J-35 Apple Music web API first: the HTML og:title variant served to the
bot carries "- Apple Music" boilerplate, which tripped the placeholder
guard and failed the task. The numeric song id in the URL now queries
the public itunes.apple.com lookup API first (artistName/trackName);
the og:title fallback strips the "- Apple Music" suffix.
"""

import ast
import base64
import os
import py_compile
import re
import sys
import tempfile

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

NEW_FN = '''async def _resolve_apple(url, mmax=10):
    final, html = url, ""
    try:
        final, html = await _fetch(url)
    except Exception:
        pass
    _fp = final.split("?")[0]
    _up = url.split("?")[0]
    if "/song/" in _fp or ("/album/" in _fp and "?i=" in final):
        # iTunes lookup API — stable structured data for the numeric id
        # (the HTML og:title variant served to the fetcher is inconsistent)
        m = (
            re.search(r"/song/[^/]+/(\d+)", _fp or _up)
            or re.search(r"[?&]i=(\d+)", final)
        )
        if m:
            try:
                _api = (
                    "https://itunes.apple.com/lookup?id="
                    f"{m.group(1)}&entity=song"
                )
                _, _j = await _fetch(_api)
                mm = re.search(r'"artistName"\s*:\s*"([^"]+)"', _j)
                _ar = _clean(mm.group(1)) if mm else None
                mm = re.search(r'"trackName"\s*:\s*"([^"]+)"', _j)
                _tn = _clean(mm.group(1)) if mm else None
                if _tn and _ar:
                    return f"ytsearch5:{_ar} - {_tn} audio"
                if _tn:
                    return f"ytsearch5:{_tn} audio"
            except Exception:
                pass
        m = re.search(r'property="og:title"\s+content="([^"]+)"', html)
        if m and _clean(m.group(1)):
            t = _clean(m.group(1))
            t = re.sub(r"\s*[-\u2013\u2014]\s*Apple Music\s*$", "", t).strip()
            if t:
                return f"ytsearch5:{t} audio"
        m = re.search(r"<title>([^<]+)</title>", html)
        if m:
            t = _clean(m.group(1))
            t = t.replace("on Apple Music", "").strip()
            t = re.split(r"\s*[-–—]\s*(?:Single|Song|EP)\b", t)[0].strip()
            if t:
                return f"ytsearch5:{t} audio"
        name = _slug_query(final)
        if name:
            return f"ytsearch5:{name} audio"
        return None
    # artist / album pages
    name = None
    m = re.search(r'property="og:title"\s+content="([^"]+)"', html)
    if m:
        name = _clean(m.group(1)).replace("on Apple Music", "").strip()
    if not name:
        m = re.search(r"<title>([^<]+)</title>", html)
        if m:
            t = _clean(m.group(1)).replace("on Apple Music", "")
            t = re.split(r"\s*[-–]\s*(?:Songs|Album|Artist)", t)[0].strip()
            name = t or None
    if not name:
        name = _slug_query(final)
    if name:
        if "/artist/" in (_fp + _up):
            return f"ytsearch{mmax}:{name} songs audio"
        return f"ytsearch{mmax}:{name} audio"
    raise MusicUnsupported("Couldn't read this Apple Music link")
'''

# ---- locate and decode the WZFIX_R3_MUSIC_B64 block ----
m = re.search(r"WZFIX_R3_MUSIC_B64 = \((.*?)\n\)", s, re.S)
if not m:
    sys.exit("WZFIX_R3_MUSIC_B64 block not found")
b64_text = ast.literal_eval("(" + m.group(1) + ")")
code = base64.b64decode(b64_text).decode("utf-8")

# ---- swap the _resolve_apple function ----
fn_start = "async def _resolve_apple(url, mmax=10):"
fn_end = "raise MusicUnsupported(\"Couldn't read this Apple Music link\")"
if code.count(fn_start) != 1:
    sys.exit(f"apple fn start count {code.count(fn_start)} != 1")
if code.count(fn_end) != 1:
    sys.exit(f"apple fn end count {code.count(fn_end)} != 1")
i = code.index(fn_start)
j = code.index(fn_end, i) + len(fn_end)
new_code = code[:i] + NEW_FN + code[j:]

# compile-check the patched r3 module
with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False,
                                 encoding="utf-8") as tf:
    tf.write(new_code)
    tmp = tf.name
try:
    py_compile.compile(tmp, doraise=True)
except Exception as e:
    sys.exit(f"patched r3_music compile FAILED: {e}")
os.unlink(tmp)

# ---- re-encode and rebuild the block ----
new_b64 = base64.b64encode(new_code.encode("utf-8")).decode("ascii")
lines = [new_b64[k:k + 100] for k in range(0, len(new_b64), 100)]
block = "WZFIX_R3_MUSIC_B64 = (\n" + "".join(
    f'    "{ln}"\n' for ln in lines
) + ")"
s = s[:m.start()] + block + s[m.end():]

# ---- bump the version ----
n = s.count("v15.58")
s = s.replace("v15.58", "v15.59")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-35 apple itunes-api-first, {n} markers bumped to v15.59")
