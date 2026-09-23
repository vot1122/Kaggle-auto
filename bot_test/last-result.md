# bot test: cmd /log

```
[session] REPAIRED: 2 missing chars at pos 289 (restored '--')
[client] wzgram (native WZ_)
[me] 6726918562 CP smile
[send] /log
[file] log.txt 0.0MB
[logfile log.txt 298 lines, filtered 32]
  [23-Sep-26 08:53:41 PM] [INFO] - WZFIX BUILD v15.53 running | base: WZML-X wzv3 @ ab6464d2 | yt-dlp 2026.08.19 | 23 Sep 2026 (IST)
  [23-Sep-26 08:53:47 PM] [INFO] - WZFIX DB: 97.00KB across 12 collection groups (free tier 512 MB)
  [23-Sep-26 08:54:27 PM] [WARNING] - WZFIX set_bot_commands failed: Bad Request: BOT_COMMANDS_TOO_MUCH
  [23-Sep-26 09:07:03 PM] [INFO] - WZFIX music: resolving https://open.spotify.com/artist/1x02ug1CLkx7mrQP9FRswh (from text)
  [23-Sep-26 09:07:04 PM] [INFO] - WZFIX music: artist Amrinder Gill → 10 track(s) (top 10 + discography)
  [23-Sep-26 09:07:04 PM] [INFO] - WZFIX music: artist Amrinder Gill → 10 track(s) (smart order)
  [23-Sep-26 09:07:15 PM] [INFO] - WZFIX music: artist fan-out: 10 track(s) queued for Amrinder Gill (folder Amrinder_Gill, flags '-z')
  [23-Sep-26 09:10:04 PM] [INFO] - Zip: orig_path: /usr/src/app/downloads/101969/Amrinder_Gill, zip_path: /usr/src/app/downloads/101969/Amrinder_Gill.zip
  [23-Sep-26 09:10:05 PM] [INFO] - Leech Name: Amrinder_Gill.zip
  [23-Sep-26 09:11:14 PM] [INFO] - HypertgUL uploaded Amrinder_Gill.zip
  [23-Sep-26 09:11:16 PM] [INFO] - Leech Completed: Amrinder_Gill.zip
  [23-Sep-26 09:11:19 PM] [INFO] - WZFIX charge: user=6726918562 task_size=148226898 -> today_total=797505222 (day=2026-09-23)
  [23-Sep-26 09:11:20 PM] [INFO] - WZFIX library: recorded 'Amrinder_Gill.zip' size=74113449 user=6726918562 tg_parts=1
  [23-Sep-26 09:11:20 PM] [INFO] - Task Done: Amrinder_Gill.zip
  [23-Sep-26 09:22:13 PM] [INFO] - WZFIX music: resolving https://open.spotify.com/track/52HEmNCNvewyByx7xofF0T (from text)
  [23-Sep-26 09:38:54 PM] [INFO] - WZFIX music: resolving https://open.spotify.com/track/52HEmNCNvewyByx7xofF0T (from text)
  [23-Sep-26 09:50:02 PM] [ERROR] - Upload part failed after 5 attempts
  Traceback (most recent call last):
  [23-Sep-26 09:50:02 PM] [ERROR] - HypertgUL fail Amrinder Gill - Mera Deewanapan.mp3: TimeoutError: Request timed out
  [23-Sep-26 09:50:02 PM] [ERROR] - Request timed out. Path: /usr/src/app/downloads/1988/Amrinder Gill - Mera Deewanapan.mp3
  Traceback (most recent call last):
    File "/kaggle/working/WZML-X/bot/helper/mirror_leech_utils/upload_utils/telegram_uploader.py", line 605, in _upload_file
      sent_msg = await self._hu.upload(
    File "/kaggle/working/WZML-X/bot/helper/ext_utils/hyperul_utils.py", line 178, in upload
  [23-Sep-26 09:50:02 PM] [ERROR] - Request timed out. Path: /usr/src/app/downloads/1988/Amrinder Gill - Mera Deewanapan.mp3
  Traceback (most recent call last):
    File "/kaggle/working/WZML-X/bot/helper/mirror_leech_utils/upload_utils/telegram_uploader.py", line 448, in _upload_file_task
      sent = await self._upload_file(
    File "/kaggle/working/WZML-X/bot/helper/mirror_leech_utils/upload_utils/telegram_uploader.py", line 654, in _upload_file
    File "/kaggle/working/WZML-X/bot/helper/mirror_leech_utils/upload_utils/telegram_uploader.py", line 605, in _upload_file
      sent_msg = await self._hu.upload(
    File "/kaggle/working/WZML-X/bot/helper/ext_utils/hyperul_utils.py", line 178, in upload
```
