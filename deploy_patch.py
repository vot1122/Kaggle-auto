"""One-shot patch: bump WZFIX notebook to the next version with the
/yl is_leech propagation fix. Run from the repo root."""
import py_compile
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

old1 = '''        "        if await pre_resolve(self.message, self.client, is_ytdl=True):\\n"'''
new1 = '''        "        if await pre_resolve(self.message, self.client,\\n"
            "                             is_ytdl=True, is_leech=self.is_leech):\\n"'''
old2 = '''                "                    self.message, self.client, is_ytdl=True\\n"'''
new2 = '''                "                    self.message, self.client,\\n"
                "                    is_ytdl=True, is_leech=self.is_leech\\n"'''

for old, new in ((old1, new1), (old2, new2)):
    n = s.count(old)
    if n != 1:
        sys.exit(f"patch anchor count {n} != 1 -- aborting")
    s = s.replace(old, new, 1)

n = s.count("v15.50")
s = s.replace("v15.50", "v15.51")
with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: is_leech hooks applied, {n} version markers bumped to v15.51")
