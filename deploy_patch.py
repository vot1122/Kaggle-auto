"""One-shot patch: v15.52 -> v15.53.

Music zip delivery: files over 10MB go through the hyper upload pool,
which routes them to LEECH_LOG_CHAT ("Leech Dumps") when configured —
so the artist -z zip never appeared in the chat where it was requested.
Music tasks (the J-25 best-pick already marks them with
listener._wzfix_music) now bypass the hyper pool entirely and upload
through the normal bot path into the requesting chat.
"""
import py_compile
import sys

P = "kaggle_notebook.py"
s = open(P, encoding="utf-8").read()

anchor = 'log(f"  r2: J-28 patch FAILED — {e}", "ERROR")'
if s.count(anchor) != 1:
    sys.exit(f"J-28 anchor count {s.count(anchor)} != 1")

if "WZFIX music keep-chat" in s:
    print("J-29 already present")
else:
    j29 = '''

    # J-29: music keep-chat (v15.53) — hyper uploads of music zips go to
    # LEECH_LOG_CHAT, hiding the delivered zip from the user's chat;
    # music files must stay in the chat where they were requested
    try:
        _p = os.path.join(
            WZMLX_DIR, "bot/helper/ext_utils/hyperul_utils.py"
        )
        with open(_p, "r", encoding="utf-8") as f:
            _t = f.read()
        if "WZFIX music keep-chat" not in _t:
            _old = (
                "            use_hyper = Config.USE_HYPER and self.clients"
                " and up_size > 10 * 1024 * 1024"
            )
            _new = (
                "            # WZFIX music keep-chat (v15.53): the hyper"
                " pool routes\\n"
                "            # >10MB files to LEECH_LOG_CHAT, which hides"
                " the delivered\\n"
                "            # zip from the user's chat — music files stay"
                " in the\\n"
                "            # requesting chat\\n"
                "            use_hyper = (\\n"
                "                Config.USE_HYPER\\n"
                "                and self.clients\\n"
                "                and up_size > 10 * 1024 * 1024\\n"
                "                and not getattr(\\n"
                "                    self._listener, \\"_wzfix_music\\", False\\n"
                "                )\\n"
                "            )"
            )
            if _old in _t:
                _t = _t.replace(_old, _new, 1)
                with open(_p, "w", encoding="utf-8") as f:
                    f.write(_t)
                _r = subprocess.run(
                    [sys.executable, "-m", "py_compile", _p],
                    capture_output=True, text=True, timeout=60,
                )
                if _r.returncode == 0:
                    log("  r2: hyperul_utils.py music keep-chat applied")
                else:
                    log(
                        f"  r2: hyperul_utils.py keep-chat FAILED — "
                        f"{(_r.stderr or '').strip()[:200]}",
                        "ERROR",
                    )
            else:
                log("  r2: hyperul_utils.py use_hyper anchor missing", "WARN")
        else:
            log("  r2: hyperul_utils.py music keep-chat already applied")
    except Exception as e:
        log(f"  r2: J-29 patch FAILED — {e}", "ERROR")
'''
    s = s.replace(anchor, anchor + j29, 1)

n = s.count("v15.52")
s = s.replace("v15.52", "v15.53")
with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-29 music keep-chat inserted, {n} markers bumped to v15.53")
