"""One-shot patch: v15.51 -> v15.52.

r3_music pre_resolve: the clean-title patch only fired for ytsearch5:
specs (track links); album/playlist specs (ytsearchN: with the user's
music limit) fell through and the output kept the raw video title.
Now every resolved music spec gets a clean title: strip the ytsearchN:
prefix and the "full album"/"songs"/"playlist"/"" audio suffix.
"""
import base64
import py_compile
import re
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

m = re.search(r'WZFIX_R3_MUSIC_B64 = \(\n((?:    "[^"]*"\n)+)\)', s)
if not m:
    sys.exit("r3 block not found")
enc = "".join(re.findall(r'"([^"]*)"', m.group(1)))
core = base64.b64decode(enc).decode()

old_block = (
    '        try:\n'
    '            _t = ytq.split(":", 1)[1] if ":" in ytq else ytq\n'
    '            _t = _t.rsplit(" audio", 1)[0].strip()\n'
    '            if ytq.startswith("ytsearch5:"):\n'
    '                message._wzfix_music_title = _t\n'
    '        except Exception:\n'
    '            pass\n'
)
new_block = (
    '        try:\n'
    '            _t = re.sub(r"^ytsearch\\d+:", "", str(ytq))\n'
    '            for _suf in (" full album audio", " songs audio",\n'
    '                         " playlist audio", " audio"):\n'
    '                if _t.endswith(_suf):\n'
    '                    _t = _t[: -len(_suf)]\n'
    '                    break\n'
    '            _t = _t.strip(" -|")\n'
    '            if _t:\n'
    '                message._wzfix_music_title = _t\n'
    '        except Exception:\n'
    '            pass\n'
)
n = core.count(old_block)
if n == 1:
    core = core.replace(old_block, new_block, 1)
elif "_t.strip(\" -|\")" in core:
    print("r3: robust title patch already applied")
else:
    sys.exit(f"r3 title anchor count {n} != 1")

compile(core, "r3_music", "exec")

enc2 = base64.b64encode(core.encode()).decode()
enc_lines = [enc2[i:i + 60] for i in range(0, len(enc2), 60)]
enc_body = "".join(f'    "{ln}"\n' for ln in enc_lines)
new_b = "WZFIX_R3_MUSIC_B64 = (\n" + enc_body + ")"
s = s[:m.start()] + new_b + s[m.end():]

n = s.count("v15.51")
s = s.replace("v15.51", "v15.52")
with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: robust title patch applied, {n} markers bumped to v15.52")
