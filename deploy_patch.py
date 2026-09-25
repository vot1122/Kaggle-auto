"""One-shot patch: v15.68 -> v15.69 (Round 10: artist discography fix).

Artist downloads only ever fetched Spotify's top-10 tracks because the
catalog token was never passed the embed page JSON that contains the
anonymous accessToken. This patch fixes the music module inside the
notebook's WZFIX_R3_MUSIC_B64 blob: the embed JSON is now handed to the
token helper, so the full discography (albums + singles, newest first)
is used up to the user's limit. Also iterates all fetched albums, not
just the first 40.
"""
import ast
import base64
import py_compile
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

if "v15.69" in s:
    py_compile.compile(P, doraise=True)
    print("already v15.69 — no changes")
    sys.exit(0)
if "v15.68" not in s:
    sys.exit("no v15.68 markers found — unexpected notebook")

i = s.index("WZFIX_R3_MUSIC_B64 = (")
j = s.index("\n)", i) + 2
src = base64.b64decode(
    ast.literal_eval(s[i + len("WZFIX_R3_MUSIC_B64 = "):j])
).decode("utf-8")

fixes = [
    (
        "async def _catalog_tracks(artist_id, name, out, limit):",
        "async def _catalog_tracks(artist_id, name, out, limit, anon_from=None):",
    ),
    (
        "    tok = await _spotify_token()\n    if not tok:",
        "    tok = await _spotify_token(anon_from)\n    if not tok:",
    ),
    (
        "                out = await _catalog_tracks(\n"
        "                    m.group(1), name, out, limit\n"
        "                )",
        "                out = await _catalog_tracks(\n"
        "                    m.group(1), name, out, limit, d\n"
        "                )",
    ),
    (
        "    for alb in albums[:40]:",
        "    for alb in albums:",
    ),
]
for old, new in fixes:
    if src.count(old) != 1:
        sys.exit("anchor not unique: " + old[:60])
    src = src.replace(old, new)

b64 = base64.b64encode(src.encode("utf-8")).decode("ascii")
chunks = [b64[k:k + 76] for k in range(0, len(b64), 76)]
block = (
    "WZFIX_R3_MUSIC_B64 = (\n"
    + "".join('    "%s"\n' % c for c in chunks)
    + ")"
)
s = s[:i] + block + s[j:]
n = s.count("v15.68")
s = s.replace("v15.68", "v15.69")
if "v15.69" not in s:
    sys.exit("version marker missing after patch")
py_compile.compile(P, doraise=True)
with open(P, "w", encoding="utf-8") as f:
    f.write(s)
print(f"patch OK: r10 artist discography fix, {n} markers bumped to v15.69")
