# FraudCom — Multichannel Voice Transcriber

Captures discrete channels from **multiple audio devices at once** (Dante
Virtual Soundcard, ASIO interfaces, USB mics, aggregate devices), transcribes
each channel independently, and shows the results as a group chat — with
channel groups, solo view, hot-word alerts, and off-tab notifications.

**Everything is local.** Speech recognition runs offline via
[Vosk](https://alphacephei.com/vosk/); no audio or text leaves your machine.

## Features

- **Named channels** — each channel gets a speaker name, color, and avatar.
- **Multiple devices simultaneously** — check channels across any combination
  of input devices; each runs its own capture stream.
- **Groups & tabs** — create groups ("Band", "Crew"), assign channels, and
  switch tabs to see one group at a time. Press **S** on any channel strip
  (or click a speaker's name/avatar in the feed) to solo a single channel.
- **Off-tab alerts** — if someone talks on a tab you're not viewing, the tab
  gets a red unread badge and a toast pops up (click it to jump there).
  Optional chime, toggleable.
- **Hot words** — an editable list (pre-seeded with live-audio terms like
  *feedback, soundcheck, standby, cue, downbeat*). Matches are highlighted in
  gold and always trigger an alert, even on the active tab.
- **Jargon corrections** — a server-side lexicon fixes how the recognizer
  spells trade terms (`f o h => FOH`, `d i => DI`, `i e m => IEM`). Editable
  in the UI; stored in `lexicon.json`.
- **Whisper refinement (hybrid engine)** — Vosk (`model/`, the 0.22-lgraph
  build) provides the instant word-by-word feed; a local faster-whisper
  `small.en` (int8, cached in `%LOCALAPPDATA%\FraudCom\whisper`) then
  re-transcribes each utterance in the background and upgrades the text in
  the feed and the saved transcript — punctuation, jargon, proper nouns.
  Fully offline; toggle in ⚙ Preferences. Hot words are fed to Whisper as
  a vocabulary hint. `model-small/` is the old fast model, kept as backup
  (swap folders to revert).
- **Per-input translation** — mark a channel "Translate to English" and the
  multilingual Whisper model renders whatever language is spoken as English
  (globe marker in the feed, `[EN]` in the saved transcript). Fully offline.
- **Automations** — Preferences rules: when a phrase is heard (optionally
  on one channel) or an OSC message matching a pattern arrives, send OSC
  (UDP or TCP, typed arguments, `{template}` variables) or flash the
  screen. Rules live in `automations.json`; test-send button included.
- **OSC input** (off by default) — commands addressed `/fraudcom/…` (ping,
  version, channel select/send/clear/mark/list, scroll, text size, window,
  flash, automation trigger, markers, notes, mute, start/stop), UDP and/or
  TCP. Port 9000 by default; 9765 if 9000 is taken.
- **Remote API** (off by default) — `remote_api.py` serves a REST + WebSocket
  + SSE API under `/api/v1` on port 24480 for show-control software: status,
  channels, groups, keywords, automations and webhooks, configurations, the
  live transcript, window and OSC control. Optional API key.
- **Mute, markers, health** — M mutes an input; the M key or OSC drops a
  marker; a health line shows CPU, lag and the Whisper queue while live.
- **Typed notes** — the Note button drops a timestamped note into the
  feed and the on-disk transcript.
- **Pinned tabs & jump-to-latest** — star a group tab to keep it at the
  front; a floating "Jump to latest" pill appears (with unseen count)
  when you scroll up in the feed.
- **Transcript download** and per-channel live level meters.

## Quick start (Windows)

Double-click **FraudCom.exe** (or the "FraudCom" desktop shortcut). It
starts the server and opens the app in its own window; **closing that
window shuts the server down** — no orphaned processes. Only `model/`
lives next to the exe.

App data lives in `%LOCALAPPDATA%\FraudCom\` (Windows) or
`~/Library/Application Support/FraudCom/` (macOS):
- `transcripts\transcript-YYYY-MM-DD.txt` — every final line is appended
  automatically (survives crashes and window closes)
- `lexicon.json`, `presets.json`, `state.json`, `automations.json`,
  `roster.json` + `roster-backups\` — settings, presets and inputs

By default the app auto-loads the last used preset and starts listening on
launch (toggle in ⚙ Preferences). Other devices on the LAN can watch the
feed read-only at the URL shown in ⚙ Preferences (allow the Windows
Firewall prompt on first run).

To build:

- **Windows:** `pyinstaller --noconfirm --clean FraudCom.spec` (onedir build in
  `dist\FraudCom`). Ship it as `app\` beside `model\` and `whisper\`.
- **macOS:** `./build_mac.command` (see the header of that script).

Build from a copy of the source in a neutral folder whose path contains no
user name: PyInstaller embeds some build paths. Never sign the build with a
personal certificate, never UPX-compress it, and never strip PyAV's codec
libraries (they're load-time dependencies of `av._core`; Whisper breaks
without them). After a build, delete any `*.dist-info\__pycache__` folders:
they're never imported and can carry the build path.

Errors from a windowed run are appended to `fraudcom.log` next to the exe
(or in the data folder if that isn't writable; always the data folder on
macOS).

## Setup (from source)

Python 3.9+:

```bash
pip install -r requirements.txt
```

Download a Vosk model from https://alphacephei.com/vosk/models and unzip into
a `model/` folder next to `app.py`:

- `vosk-model-small-en-us-0.15` (~40 MB) — fast; handles many channels.
- `vosk-model-en-us-0.22` (~1.8 GB) — more accurate; heavier per channel.

```bash
python app.py        # then open http://localhost:8765
```

Linux may need `sudo apt install libportaudio2`.

## Dante Virtual Soundcard notes

- **ASIO mode (recommended, Windows):** DVS appears as one device with up to
  64 discrete inputs; the app subscribes to exactly the channels you check.
- **WDM mode (Windows):** DVS shows up as multiple stereo devices
  ("DVS Receive 1-2", "3-4", ...). That's fine in v2 — check channels on as
  many of those stereo devices as you need; they all run at once.
- **macOS:** DVS is a single Core Audio device with all channels.
- Sample rates are taken per device (DVS is typically 48 kHz); Vosk
  downsamples internally.

## About custom vocabulary (musician / live-audio words)

Vosk models ship with a fixed vocabulary, so words genuinely outside it can't
be *recognized* without retraining the language model (Vosk documents this
"model adaptation" process, but it's a heavy offline job). In practice most
live-audio vocabulary — monitor, feedback, tempo, chorus, phantom — is
ordinary English and already recognized; the failure mode is jargon being
spelled out ("f o h"). The corrections lexicon fixes those deterministically,
and hot words make the terms you care about impossible to miss. If you later
want stronger accuracy on domain speech, the cleanest upgrade is swapping in
`faster-whisper`, which handles jargon and proper nouns noticeably better
(at the cost of ~2–5 s latency instead of word-by-word streaming).

## Performance

Each checked channel runs its own recognizer thread. The small English model
comfortably handles 8+ concurrent channels on a modern laptop. Watch CPU as
you add channels with the large model.
