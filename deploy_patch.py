"""One-shot patch: v15.60 -> v15.61.

J-37 /ld No-button behavior: pressing "No" on the "Did you mean" card
used to kill the search immediately ("No matches found"). Users expect
No to mean "not this song" — show the OTHER matches (the same paged
list as Revise: 5 at a time, More up to 20). The "Cancel" button in
the list view still ends the search (distinguished by the message
text being the list itself).
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

OLD = '''async def music_ld_no(client, cb):
    await cb.answer()
    orig, st = _ld_orig(cb)
    if orig is not None:
        _LD_STATE.pop(getattr(orig, "id", None), None)
    await _ld_edit(
        cb.message, "❌ No matches found — try with different keywords"
    )'''

NEW = r'''async def music_ld_no(client, cb):
    await cb.answer()
    orig, st = _ld_orig(cb)
    txt = (getattr(cb.message, "text", "") or "")
    if "Matches for your lyrics" in txt:
        # Cancel from the list view — end the search
        if orig is not None:
            _LD_STATE.pop(getattr(orig, "id", None), None)
        await _ld_edit(
            cb.message, "❌ Search cancelled — send /ld again anytime"
        )
        return
    if not st:
        await _ld_edit(
            cb.message, "❌ Search expired — send /ld again"
        )
        return
    if len(st["hits"]) < 2:
        _LD_STATE.pop(getattr(orig, "id", None), None)
        await _ld_edit(
            cb.message, "❌ No other matches found — try different keywords"
        )
        return
    # "No" on the confirm card → show the OTHER matches
    st["hits"] = st["hits"][1:]
    st["cursor"] = 0
    _log("WZFIX /ld: no → showing other matches")
    await _ld_edit(
        cb.message,
        "🎧 <b>Matches for your lyrics</b>\n\nTap one to download:",
        _ld_page_kb(st),
    )'''

m = re.search(r"WZFIX_R4_CMDS_B64 = \((.*?)\n\)", s, re.S)
if not m:
    sys.exit("WZFIX_R4_CMDS_B64 block not found")
code = base64.b64decode(ast.literal_eval("(" + m.group(1) + ")")).decode("utf-8")

n = code.count(OLD)
if n != 1:
    sys.exit(f"expected exactly 1 music_ld_no block, found {n}")
code = code.replace(OLD, NEW)

with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False,
                                 encoding="utf-8") as tf:
    tf.write(code)
    tmp = tf.name
try:
    py_compile.compile(tmp, doraise=True)
except Exception as e:
    sys.exit(f"patched r4 compile FAILED: {e}")
os.unlink(tmp)

new_b64 = base64.b64encode(code.encode("utf-8")).decode("ascii")
lines = [new_b64[k:k + 100] for k in range(0, len(new_b64), 100)]
block = "WZFIX_R4_CMDS_B64 = (\n" + "".join(
    f'    "{ln}"\n' for ln in lines
) + ")"
s = s[:m.start()] + block + s[m.end():]


# ---- bot_test/run_test.py: /ld scenario now presses the No button ----
RT = "bot_test/run_test.py"
t = open(RT, encoding="utf-8").read()
TELE_OLD = '    async def download(self, raw, fname=None):\n        os.makedirs(DL_DIR, exist_ok=True)\n        target = os.path.join(DL_DIR, (fname or "file.bin")\n                              .replace("/", "_")[:180])\n        return await raw.download_media(file=target)\n'
TELE_NEW = '    async def download(self, raw, fname=None):\n        os.makedirs(DL_DIR, exist_ok=True)\n        target = os.path.join(DL_DIR, (fname or "file.bin")\n                              .replace("/", "_")[:180])\n        return await raw.download_media(file=target)\n\n    async def press_button(self, chat, msg, label):\n        for row in msg.raw.buttons:\n            for b in row:\n                if label in (b.text or ""):\n                    await b.click()\n                    return True\n        return False\n\n    async def get_msg(self, chat, mid):\n        m = await self.c.get_messages(chat, ids=mid)\n        if not m:\n            return None\n        buttons = []\n        if m.text and m.buttons:\n            buttons = [b.text for row in m.buttons for b in row]\n        fname, size = "", 0\n        if m.document:\n            for at in m.document.attributes:\n                if getattr(at, "file_name", None):\n                    fname = at.file_name\n            size = m.document.size or 0\n        return Msg(m.id, m.text or "", buttons, fname, size, m)\n\n'
if t.count(TELE_OLD) != 1:
    sys.exit(f"run_test anchor missing")
t = t.replace(TELE_OLD, TELE_NEW)
PYRO_OLD = '    async def download(self, raw, fname=None):\n        os.makedirs(DL_DIR, exist_ok=True)\n        target = os.path.join(DL_DIR, (fname or "file.bin")\n                              .replace("/", "_")[:180])\n        return await self.c.download_media(raw, file_name=target)\n'
PYRO_NEW = '    async def download(self, raw, fname=None):\n        os.makedirs(DL_DIR, exist_ok=True)\n        target = os.path.join(DL_DIR, (fname or "file.bin")\n                              .replace("/", "_")[:180])\n        return await self.c.download_media(raw, file_name=target)\n\n    async def press_button(self, chat, msg, label):\n        rm = msg.raw.reply_markup\n        if not rm:\n            return False\n        for row in rm.inline_keyboard:\n            for b in row:\n                if label in (b.text or ""):\n                    await self.c.request_callback_answer(\n                        chat_id=chat, message_id=msg.raw.id,\n                        callback_data=b.callback_data,\n                    )\n                    return True\n        return False\n\n    async def get_msg(self, chat, mid):\n        m = await self.c.get_messages(chat, mid)\n        if not m:\n            return None\n        buttons = []\n        try:\n            rm = m.reply_markup\n            if rm is not None and hasattr(rm, "inline_keyboard"):\n                buttons = [b.text for row in rm.inline_keyboard for b in row]\n        except Exception:\n            pass\n        return Msg(m.id, m.text or m.caption or "", buttons,\n                   (m.document.file_name if m.document else "") or "",\n                   (m.document.file_size if m.document else 0) or 0, m)\n\n'
if t.count(PYRO_OLD) != 1:
    sys.exit(f"run_test anchor missing")
t = t.replace(PYRO_OLD, PYRO_NEW)
LD_OLD = '    elif scenario == "ld":\n        q = arg or "Locked In Bhalwaan"\n        log(f"[send] /ld {q}")\n        sid = await adapter.send(chat, f"/ld {q}")\n        await collect(adapter, chat, sid, cap_s=300, quiet_s=60, first_s=60)\n'
LD_NEW = '    elif scenario == "ld":\n        q = arg or "Locked In Bhalwaan"\n        log(f"[send] /ld {q}")\n        sid = await adapter.send(chat, f"/ld {q}")\n        await collect(adapter, chat, sid, cap_s=300, quiet_s=60, first_s=60)\n        # press the No button on the confirm card and capture what follows\n        try:\n            msgs = await adapter.fetch_new(chat, sid)\n            card = next(\n                (m for m in msgs\n                 if "Did you mean" in (m.text or "") and m.buttons),\n                None,\n            )\n            if card is None:\n                log("[ld] confirm card not found")\n            else:\n                log(f"[ld] card buttons: {card.buttons}")\n                await adapter.press_button(chat, card, "No")\n                log("[ld] pressed No, waiting for the list...")\n                await asyncio.sleep(10)\n                fresh = await adapter.get_msg(chat, card.id)\n                if fresh is None:\n                    log("[ld] card could not be re-fetched")\n                else:\n                    txt = fresh.text or ""\n                    log(f"[ld] after No text: {txt[:500]!r}")\n                    log(f"[ld] after No buttons: {fresh.buttons}")\n                    if "Matches for your lyrics" in txt:\n                        log("[ld] RESULT: list shown after No — OK")\n                    else:\n                        log("[ld] RESULT: list NOT shown after No")\n        except Exception as e:\n            log(f"[ld-button-error] {e}")\n'
if t.count(LD_OLD) != 1:
    sys.exit(f"run_test anchor missing")
t = t.replace(LD_OLD, LD_NEW)
open(RT, "w", encoding="utf-8").write(t)
py_compile.compile(RT, doraise=True)
print("run_test.py patched: /ld scenario presses No + captures the list")

n = s.count("v15.60")
s = s.replace("v15.60", "v15.61")

with open(P, "w", encoding="utf-8") as f:
    f.write(s)
py_compile.compile(P, doraise=True)
print(f"patch OK: J-37 /ld No shows other matches, {n} markers bumped to v15.61")
