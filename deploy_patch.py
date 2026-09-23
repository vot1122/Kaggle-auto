"""One-shot patch: v15.55 -> v15.56.

J-32 JioSaavn site-name guard: the bot's fetch of some song pages gets
the SHORT og:title variant ("Ishq - JioSaavn"), so the site name became
the artist and the search went out as "JioSaavn - Ishq" (wrong song).
Site/nav tokens are now blacklisted as title/artist candidates; the
JSON primary_artists fallback (proven correct: "Amrinder Gill") fills
the artist.
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

NEW_FN = '''# never accept these as song title or artist (site chrome / nav items)
_SAAVN_STOP = {
    "jiosaavn", "saavn", "song", "songs", "home", "music", "album",
    "artist", "playlist", "download",
}


async def _resolve_jiosaavn(url, mmax=10):
    final, html = url, ""
    try:
        final, html = await _fetch(url)
    except Exception:
        pass
    _fp = final.split("?")[0]
    _up = url.split("?")[0]
    if "/song/" in _fp or "/song/" in _up:
        title = artist = None
        # og:title / <title> FIRST — JioSaavn's stable pattern
        # "Song - Artist - Download or Listen Free - JioSaavn" (or the older
        # "Song - Song Download from Album @ JioSaavn")
        for pat in (
            r'property="og:title"\s+content="([^"]+)"',
            r"<title>([^<]+)</title>",
        ):
            m = re.search(pat, html)
            if not m:
                continue
            t = _clean(m.group(1))
            t = re.split(r"\s*[-\u2013\u2014]\s*Song Download\b", t)[0]
            seg = re.split(
                r"\s*[-\u2013\u2014]\s*Download or Listen Free\b", t
            )[0]
            seg = re.split(r"\s*@\s*JioSaavn\b", seg)[0].strip()
            seg = re.sub(r"\s*\|\s*JioSaavn\s*$", "", seg).strip()
            if not seg:
                continue
            parts = [
                p.strip()
                for p in re.split(r"\s*[-\u2013\u2014]\s*", seg)
                if p.strip()
            ]
            if len(parts) >= 2:
                # "Ishq - JioSaavn" (short og:title variant) carries the
                # site name as the last segment — never an artist
                _cand_t, _cand_a = parts[0], parts[-1]
                if _cand_t.lower() not in _SAAVN_STOP:
                    title = _cand_t
                if _cand_a.lower() not in _SAAVN_STOP:
                    artist = _cand_a
                if title or artist:
                    break
            elif not title:
                title = seg
        # structured JSON fallback — specific keys only (a generic "title"
        # matches nav/menu items like "Home" in the page JSON)
        if not (title and artist):
            m = re.search(r'"song_title"\s*:\s*"([^"]+)"', html)
            if m:
                title = title or _clean(m.group(1))
            m = (
                re.search(r'"primary_artists"\s*:\s*"([^"]*)"', html)
                or re.search(r'"singers"\s*:\s*"([^"]*)"', html)
                or re.search(r'"artist"\s*:\s*"([^"]*)"', html)
            )
            if m and _clean(m.group(1)).lower() not in _SAAVN_STOP:
                artist = artist or _clean(m.group(1))
        # strip parenthetical junk from the song name ("Ishq (Full Song)" -> "Ishq")
        if title:
            title = re.sub(
                r"\s*\([^)]*(?:full\s*song|official|lyrical|audio|video|hd)[^)]*\)\s*$",
                "", title, flags=re.I,
            ).strip()
        # slug fallback: /song/ishq/ID
        if not title:
            m = re.search(r"/song/([a-z0-9-]+)/", _up or _fp)
            if m and m.group(1) not in ("song",):
                title = m.group(1).replace("-", " ").strip().title()
        if title and artist:
            return f"ytsearch5:{artist} - {title} audio"
        if title:
            return f"ytsearch5:{title} audio"
        return None
    # artist / album / featured pages
    name = None
    m = re.search(r'property="og:title"\s+content="([^"]+)"', html)
    if m:
        name = _clean(m.group(1))
    if not name:
        m = re.search(r"<title>([^<]+)</title>", html)
        if m:
            t = _clean(m.group(1)).replace("@ JioSaavn", "")
            t = re.split(r"\s*[-\u2013\u2014]\s*(?:Songs|Albums|Album)", t)[0].strip()
            name = t or None
    if not name:
        name = _slug_query(final)
    if name:
        if "/artist/" in (_fp + _up):
            return f"ytsearch{mmax}:{name} songs audio"
        return f"ytsearch{mmax}:{name} audio"
    raise MusicUnsupported("Couldn't read this JioSaavn link")
'''

# ---- locate and decode the WZFIX_R3_MUSIC_B64 block ----
m = re.search(r"WZFIX_R3_MUSIC_B64 = \((.*?)\n\)", s, re.S)
if not m:
    sys.exit("WZFIX_R3_MUSIC_B64 block not found")
b64_text = ast.literal_eval("(" + m.group(1) + ")")
code = base64.b64decode(b64_text).decode("utf-8")

# ---- swap the _resolve_jiosaavn function ----
fn_start = "async def _resolve_jiosaavn(url, mmax=10):"
fn_end = "raise MusicUnsupported(\"Couldn't read this JioSaavn link\")"
if code.count(fn_start) != 1:
    sys.exit(f"jiosaavn fn start count {code.count(fn_start)} != 1")
if code.count(fn_end) != 1:
    sys.exit(f"jiosaavn fn end count {code.count(fn_end)} != 1")
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
n = s.count("v15.55")
s = s.replace("v15.55", "v15.56")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-32 jiosaavn site-name guard, {n} markers bumped to v15.56")
