#!/usr/bin/env python3
"""One-shot patch: v15.76 -> v15.77 (Round 18).

Two fixes:
  1. the download password page still never showed: the r5
     per-link gate at the top of _serve fires BEFORE the
     gate R16/R17 patched and raises the raw 401 - it now
     serves the page for download links. The page also now
     posts the link token, so a per-link password verifies
     and returns a link-signed token.
  2. artist catalogue duplicates: 'Song - 8D Audio', 'Song
     (Official Video)' etc. are the same song - the dedup
     key now folds variant words, not just brackets.
"""
import ast
import hashlib
import sys

INPUT_SHA256 = "ec00356f52068bd91f2f00c8b5b9122293de2355037b0263ef08507126196bae"
EXPECT_SHA256 = "81b910f4efc8da6b031b962d6c8d8d322350664fc3148282e7b4985f4ad4da46"

R5GATE_OLD = (
    '    if not await _r5_serve_ok(request):',
    '        raise web.HTTPUnauthorized(',
    '            text="authenticate first",',
    '            headers={"X-Stream-Auth-Required": "1"},',
    '        )',
)

R5GATE_NEW = (
    '    if not await _r5_serve_ok(request):',
    '        # WZFIX_R18: this per-link gate fires BEFORE the user gate',
    '        # below - a download-kind open (the Telegram download',
    '        # button) must see the password page here, not raw text',
    '        if kind == "download":',
    '            return _dl_auth_page()',
    '        raise web.HTTPUnauthorized(',
    '            text="authenticate first",',
    '            headers={"X-Stream-Auth-Required": "1"},',
    '        )',
)

NORM_ANCHOR_DEF = (
    'def _norm_key(name):',
)

NORM_PREPEND = (
    '_VARIANT_WORDS = frozenset(',
    '    "official video audio lyrics lyric lyrical 8d 16d hd hq 4k bass "',
    '    "boosted slowed reverb sped speedup visualizer visual karaoke "',
    '    "instrumental remix live cover version tribute full"',
    '    .split()',
    ')',
    '',
    '',
    'def _norm_key(name):',
)

NORM_CODE_OLD = (
    '    s = _FEAT_PAT.sub(" ", name or "")',
    '    s = _META_PAT.sub(" ", s)',
    '    s = re.sub(r"[^0-9a-z]+", " ", s.lower())',
    '    return " ".join(s.split())',
)

NORM_CODE_NEW = (
    '    s = _FEAT_PAT.sub(" ", name or "")',
    '    s = _META_PAT.sub(" ", s)',
    '    s = re.sub(r"(?:sped|speed)[ -]?up", " ", s, flags=re.I)',
    '    toks = re.sub(r"[^0-9a-z]+", " ", s.lower()).split()',
    '    # WZFIX_R18: variant markers also appear unbracketed, after a',
    '    # dash ("Song - 8D Audio") or bare ("Song Official Video") —',
    '    # drop every variant word so they all fold into the base song',
    '    toks = [t for t in toks if t not in _VARIANT_WORDS]',
    '    return " ".join(toks)',
)

def _c18(lines, ind):
    out = ""
    for ln in lines:
        if chr(39) not in ln:
            q = chr(39)
        else:
            q = chr(34)
        out = out + " " * ind + q + ln + chr(92) + "n" + q + chr(10)
    return out

R18 = (
    '    # R18 (v15.77): the raw-401 came from the r5 per-link gate\n'
    + '    # that fires BEFORE the user gate R16/R17 patched - it now\n'
    + '    # serves the password page for download-kind requests too,\n'
    + '    # and the page posts the link token so per-link passwords\n'
    + '    # verify (returning a link-signed token). Also: the artist\n'
    + '    # catalogue dedup now folds variant titles (8D, Official\n'
    + '    # Video, Slowed/Reverb...) so a full-catalogue batch no\n'
    + '    # longer downloads the same song twice.\n'
    + '    try:\n'
    + '        _ss18 = os.path.join(WZMLX_DIR, "bot/core/stream_server.py")\n'
    + '        with open(_ss18, "r", encoding="utf-8") as _f:\n'
    + '            _s18 = _f.read()\n'
    + '        if "WZFIX_R18" in _s18:\n'
    + '            log("  r18: download page already on the r5 gate")\n'
    + '        else:\n'
    + '            _g18o = (\n'
    + _c18(R5GATE_OLD, 16)
    + '            )\n'
    + '            _g18n = (\n'
    + _c18(R5GATE_NEW, 16)
    + '            )\n'
    + '            _ok18 = 0\n'
    + '            if _g18o in _s18:\n'
    + '                _s18 = _s18.replace(_g18o, _g18n, 1)\n'
    + '                _ok18 += 1\n'
    + '            else:\n'
    + '                log("  r18: r5 gate anchor missing", "WARN")\n'
    + '            _fa18 = "body:JSON.stringify({password:P.value})"\n'
    + '            _fb18 = (\n'
    + '                "body:JSON.stringify({password:P.value,token:"\n'
    + '                + "location.pathname.split(" + chr(34) + "/" + chr(34)\n'
    + '                + ").filter(Boolean).pop()||" + chr(34) + chr(34)\n'
    + '            )\n'
    + '            if _fa18 in _s18:\n'
    + '                _s18 = _s18.replace(_fa18, _fb18, 1)\n'
    + '                _ok18 += 1\n'
    + '            else:\n'
    + '                log("  r18: page body already posts the token", "WARN")\n'
    + '            if _ok18 == 2:\n'
    + '                with open(_ss18, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_s18)\n'
    + '                _r18 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _ss18],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r18.returncode == 0:\n'
    + '                    log("  r18: download page on the r5 gate + token in the page POST")\n'
    + '                else:\n'
    + '                    log("  r18: stream_server compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log("  r18: stream_server edits incomplete - not written", "WARN")\n'
    + '        _r3p18 = os.path.join(WZMLX_DIR, "bot/helper/wzfix/r3_music.py")\n'
    + '        with open(_r3p18, "r", encoding="utf-8") as _f:\n'
    + '            _r318 = _f.read()\n'
    + '        if "WZFIX_R18" in _r318:\n'
    + '            log("  r18: catalogue dedup already folded")\n'
    + '        else:\n'
    + '            _n18a = (\n'
    + _c18(NORM_ANCHOR_DEF, 16)
    + '            )\n'
    + '            _n18p = (\n'
    + _c18(NORM_PREPEND, 16)
    + '            )\n'
    + '            _n18o = (\n'
    + _c18(NORM_CODE_OLD, 16)
    + '            )\n'
    + '            _n18n = (\n'
    + _c18(NORM_CODE_NEW, 16)
    + '            )\n'
    + '            _ok18b = 0\n'
    + '            if _n18a in _r318 and _r318.count(_n18a) == 1:\n'
    + '                _r318 = _r318.replace(_n18a, _n18p, 1)\n'
    + '                _ok18b += 1\n'
    + '            else:\n'
    + '                log("  r18: _norm_key def anchor missing", "WARN")\n'
    + '            if _n18o in _r318 and _r318.count(_n18o) == 1:\n'
    + '                _r318 = _r318.replace(_n18o, _n18n, 1)\n'
    + '                _ok18b += 1\n'
    + '            else:\n'
    + '                log("  r18: _norm_key body anchor missing", "WARN")\n'
    + '            if _ok18b == 2:\n'
    + '                with open(_r3p18, "w", encoding="utf-8") as _f:\n'
    + '                    _f.write(_r318)\n'
    + '                _r18 = subprocess.run(\n'
    + '                    [sys.executable, "-m", "py_compile", _r3p18],\n'
    + '                    capture_output=True,\n'
    + '                    text=True,\n'
    + '                    timeout=60,\n'
    + '                )\n'
    + '                if _r18.returncode == 0:\n'
    + '                    log("  r18: catalogue dedup folds variant titles (8D, Official Video, ...)")\n'
    + '                else:\n'
    + '                    log("  r18: r3_music compile FAILED", "ERROR")\n'
    + '            else:\n'
    + '                log("  r18: dedup anchors incomplete - not written", "WARN")\n'
    + '    except Exception as e:\n'
    + '        log(f"  r18: FAILED — {e}", "ERROR")\n'
    + '\n'
)

frag = R18
ast.parse("def _wrap():" + chr(10) + frag + chr(10))

s = open("kaggle_notebook.py", encoding="utf-8").read()
if "v15.77" in s:
    print("already v15.77 - no changes")
    sys.exit(0)
if hashlib.sha256(s.encode()).hexdigest() != INPUT_SHA256:
    sys.exit("input notebook is not the expected v15.76 build")

anchor = (
    '    log("WZML-X-Bot patch kit applied")' + chr(10)
)
assert s.count(anchor) == 1, f"anchor count {s.count(anchor)}"
s = s.replace(anchor, frag + anchor, 1)

n = s.count("v15.76")
s = s.replace("v15.76", "v15.77")
assert "v15.76" not in s
ast.parse(s)

h = hashlib.sha256(s.encode()).hexdigest()
if h != EXPECT_SHA256:
    sys.exit(f"SHA256 MISMATCH - got {h}")
open("kaggle_notebook.py", "w", encoding="utf-8").write(s)
print(f"patch OK: r18 r5-gate download page + link token + variant dedup, {n} markers bumped to v15.77")
