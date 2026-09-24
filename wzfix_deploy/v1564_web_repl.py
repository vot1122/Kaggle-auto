"""v15.64 dashboard user-access management - r2_web replacements.

Each tuple: (old, new). Applied to the decoded WZFIX_WEB_B64 module
source. All anchors must match exactly once. Generated against the
v15.63 block - do not hand-edit.
"""

REPL = [
    # W1
    ('    users.sort(key=lambda u: -(u["used"] + u["reserved"]))\n    return {\n        "ok": True,',
     '    users.sort(key=lambda u: -(u["used"] + u["reserved"]))\n    access = []\n    try:\n        from ... import sudo_users as _sudo_cfg\n        from ... import user_data as _ud\n\n        from .r1_core import _db, _part\n\n        cur = _db().wzfix_startusers[_part()].find({}).sort("last", -1).limit(50)\n        async for d in cur:\n            try:\n                uid = int(d["_id"])\n            except Exception:\n                continue\n            ud = _ud.get(uid) or {}\n            access.append(\n                {\n                    "uid": uid,\n                    "name": d.get("name") or "",\n                    "uname": d.get("uname") or "",\n                    "starts": int(d.get("starts") or 0),\n                    "last": d.get("last") or 0,\n                    "auth": bool(ud.get("AUTH")),\n                    "sudo": bool(ud.get("SUDO")) or uid in _sudo_cfg,\n                    "bl": bool(ud.get("BLACKLIST")),\n                }\n            )\n    except Exception:\n        pass\n    return {\n        "ok": True,\n        "access": access,'),
    # W2
    ('        if act == "spset":',
     '        if act in ("userauth", "usersudo", "userbl"):\n            if not uid:\n                return {"ok": False, "msg": "no user id"}\n            from ... import user_data as _ud\n            from ...helper.ext_utils.bot_utils import update_user_ldata\n            from ...helper.ext_utils.db_handler import database\n\n            flag = {\n                "userauth": "AUTH",\n                "usersudo": "SUDO",\n                "userbl": "BLACKLIST",\n            }[act]\n            label = flag.title()\n            v = bool(int(a.get("v", 0) or 0))\n            update_user_ldata(uid, flag, v)\n            await database.update_user_data(uid)\n            await _action_log(\n                ("GRANT " if v else "REVOKE ") + label, {"uid": uid}\n            )\n            return {\n                "ok": True,\n                "msg": f"user {uid}: {label.lower()} {\'on\' if v else \'off\'}",\n            }\n        if act == "spset":'),
    # W3
    ('    <h2>Users</h2>\n    <div id="users"></div>',
     '    <h2>Users</h2>\n    <div id="users"></div>\n    <h2>Access — /start users</h2>\n    <div id="access"></div>'),
    # W4
    ('  window._users=j.users||[];window._gcap=j.global_cap_gb||15;window._gmusic=j.global_music||10;',
     '  window._users=j.users||[];window._gcap=j.global_cap_gb||15;window._gmusic=j.global_music||10;window._access=j.access||[];renderAccess();'),
    # W5
    ('function capOf(uid){',
     'function renderAccess(){\n  var A=(window._access||[]).map(function(u){\n    var st=\'\';\n    if(u.auth)st+=\'<span class="pill">AUTH</span> \';\n    if(u.sudo)st+=\'<span class="pill">SUDO</span> \';\n    if(u.bl)st+=\'<span class="pill" style="background:#c0392b">BLOCKED</span>\';\n    var nm=esc(u.name||\'\')||String(u.uid);\n    return \'<div class="card"><div class="usr-name">\'+esc(nm)+\' \'+(u.uname?\'<span class="muted">\'+esc(u.uname)+\'</span> \':\'\')+\'<span class="pill">\'+u.uid+\'</span> \'+st+\'</div>\'\n      +\'<div class="muted">\'+u.starts+\' start(s)</div>\'\n      +\'<div class="btns">\'\n      +\'<button class="btn" onclick="act({action:\'userauth\',uid:\'+u.uid+\',v:\'+(u.auth?0:1)+\'})">\'+(u.auth?\'✓ Authorized\':\'Authorize\')+\'</button>\'\n      +\'<button class="btn" onclick="act({action:\'usersudo\',uid:\'+u.uid+\',v:\'+(u.sudo?0:1)+\'})">\'+(u.sudo?\'✗ Remove sudo\':\'Make sudo\')+\'</button>\'\n      +\'<button class="btn dng" onclick="act({action:\'userbl\',uid:\'+u.uid+\',v:\'+(u.bl?0:1)+\'})">\'+(u.bl?\'Unblock\':\'Block\')+\'</button>\'\n      +\'</div></div>\'\n  }).join(\'\');\n  $("access").innerHTML=A||\'<div class="card muted">No /start users recorded yet.</div>\';\n}\n\nfunction capOf(uid){'),
]
