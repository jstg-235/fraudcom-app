"""
Multichannel Voice Transcriber — v2
-----------------------------------
Captures discrete channels from MULTIPLE audio devices at once (e.g. Dante
Virtual Soundcard + a USB interface), transcribes each channel independently
with Vosk (100% local/offline), applies a corrections lexicon for live-audio
jargon, and streams results to a group-chat web UI with channel groups,
solo view, hot-word highlighting, and off-tab activity notifications.

Run:  python app.py   ->  http://localhost:8765
"""

import asyncio
import json
import os
from collections import OrderedDict, deque
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import numpy as np

import osclink

# Load the ASIO-capable PortAudio DLL (must be set before importing
# sounddevice). Required for Dante Virtual Soundcard in ASIO mode.
os.environ.setdefault("SD_ENABLE_ASIO", "1")
# The build leaves out hf_xet (an optional download accelerator). Tell
# huggingface_hub not to use it, so a missing-model download always takes the
# plain-HTTP path instead of trying an absent package.
os.environ.setdefault("HF_HUB_DISABLE_XET", "1")
# No download progress bars: there is no console to show them, and creating
# one makes tqdm create a multiprocessing lock (which, on a Mac, starts a
# helper process). See multiprocessing.freeze_support() in launcher.py.
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
import sounddevice as sd
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from vosk import KaldiRecognizer, Model, SetLogLevel

SetLogLevel(-1)

# Single source of truth for the app version. Bump on every shipped build:
# fixes -> patch (1.0.x), features -> minor (1.x.0). The UI, /state, and
# the portable zip filename all read from here.
APP_VERSION = "1.12.3"


def _default_data_dir() -> str:
    if sys.platform == "darwin":
        base = os.path.expanduser("~/Library/Application Support")
    else:
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
    d = os.path.join(base, "FraudCom")
    os.makedirs(d, exist_ok=True)
    return d


DATA_DIR = os.environ.get("FRAUDCOM_DATA") or _default_data_dir()
TRANSCRIPTS_DIR = os.path.join(DATA_DIR, "transcripts")


def _resolve_model_path() -> str:
    """model/ next to the exe (portable zip) or one level up (folder-mode
    app dir inside the project)."""
    p = os.environ.get("VOSK_MODEL_PATH", "model")
    if os.path.isdir(p):
        return p
    parent = os.path.join("..", p)
    return parent if os.path.isdir(parent) else p


MODEL_PATH = _resolve_model_path()
LEXICON_PATH = os.environ.get("LEXICON_PATH",
                              os.path.join(DATA_DIR, "lexicon.json"))
PRESETS_PATH = os.environ.get("PRESETS_PATH",
                              os.path.join(DATA_DIR, "presets.json"))
STATE_PATH = os.path.join(DATA_DIR, "state.json")
AUTOMATIONS_PATH = os.path.join(DATA_DIR, "automations.json")
# The input roster + groups used to live ONLY in browser localStorage, which
# is fragile (a single leveldb store, silently clobbered by any other launch
# sharing the profile, dropped by cache/profile resets). We now mirror it to
# a plain JSON file on every change and keep rolling timestamped backups, so a
# roster can never be silently lost again.
ROSTER_PATH = os.path.join(DATA_DIR, "roster.json")
ROSTER_BACKUP_DIR = os.path.join(DATA_DIR, "roster-backups")
ROSTER_BACKUPS_KEEP = 200            # ~KB each; a whole tour's worth
# When frozen by PyInstaller, static/ is unpacked into sys._MEIPASS;
# the model stays external, next to the exe (cwd is set there).
STATIC_DIR = (os.path.join(sys._MEIPASS, "static")
              if getattr(sys, "frozen", False) else "static")
# 0.0.0.0 so other devices on the venue network can view (read-only —
# non-GET requests from non-localhost are rejected by middleware below).
# FRAUDCOM_-prefixed on purpose: dev tools often set a global HOST/PORT, which
# would silently move or break the shipped app.
HOST = os.environ.get("FRAUDCOM_HOST", "0.0.0.0")
PORT = int(os.environ.get("FRAUDCOM_PORT", "8765"))

# Files left behind by a removed feature (the old company login): a session
# token and a machine fingerprint. Nothing reads them any more — remove them so
# an upgraded install carries no trace of it.
for _stale in ("session.json", "machine.id"):
    try:
        os.remove(os.path.join(DATA_DIR, _stale))
    except OSError:
        pass

BLOCK_SECONDS = 0.20
# macOS audio-thread block size (frames). Small blocks keep PortAudio's
# per-callback buffers tiny on Core Audio's 544 KB IO-thread stack.
HW_BLOCK = 512
LEVEL_INTERVAL = 0.10

DEFAULT_LEXICON = {
    # server-side text corrections: fixes how Vosk tends to spell out jargon
    "corrections": {
        "f o h": "FOH", "foe h": "FOH",
        "d i": "DI", "d eyes": "DIs",
        "p a": "PA", "e q": "EQ", "e cues": "EQs",
        "i e m": "IEM", "i e ms": "IEMs", "in ears": "in-ears",
        "sound check": "soundcheck", "down beat": "downbeat",
        "up beat": "upbeat", "back line": "backline",
        "set list": "setlist", "click track": "click track",
        "d b": "dB", "phantom power": "phantom power",
        "aux send": "aux send", "talk back": "talkback",
        "front of house": "front of house",
    },
    # client-side highlight/alert words (editable in the UI)
    "hotwords": [
        "feedback", "soundcheck", "monitor", "FOH", "IEM", "talkback",
        "downbeat", "tempo", "click", "cue", "encore", "setlist",
        "gain", "mute", "solo", "phantom", "aux", "DI", "EQ",
        "verse", "chorus", "bridge", "key change", "stop", "hold",
        "standby", "go", "problem", "help",
    ],
}


def _load_json(path: str, default, strict: bool = False):
    """Read a JSON file.
    - Corrupt (bad JSON/encoding): QUARANTINED as .corrupt-<ts>, default
      returned — nothing the user typed is silently destroyed.
    - Unreadable (locked by antivirus/backup/sync — OSError): retried briefly.
      Still locked: strict=True RAISES so a read-modify-write save fails
      instead of overwriting everything with only the new entry; strict=False
      (display / startup reads) returns the default WITHOUT touching the file."""
    if not os.path.exists(path):
        return default
    for attempt in range(4):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except ValueError:                 # JSONDecodeError / UnicodeDecodeError
            try:
                os.replace(path, f"{path}.corrupt-{time.strftime('%Y%m%d-%H%M%S')}")
            except OSError:
                pass
            return default
        except OSError:
            if attempt < 3:
                time.sleep(0.15)
                continue
            if strict:
                raise
            return default


def _write_json(path: str, obj, **kw):
    """Atomic write: unique temp file beside the target, then os.replace. A
    crash or power loss mid-write can never leave a truncated file; two
    concurrent saves can't share a temp file; a target briefly held open by
    another program (WinError 5) is retried."""
    import tempfile
    fd, tmp = tempfile.mkstemp(prefix=os.path.basename(path) + ".",
                               suffix=".tmp", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, **kw)
        for attempt in range(5):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.1)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def load_lexicon() -> dict:
    try:
        data = _load_json(LEXICON_PATH, None, strict=True)
    except OSError:
        # locked: use the defaults for this session but DON'T overwrite the file
        return json.loads(json.dumps(DEFAULT_LEXICON))
    if isinstance(data, dict):
        if not isinstance(data.get("corrections"), dict):
            data["corrections"] = {}
        if not isinstance(data.get("hotwords"), list):
            data["hotwords"] = []
        return data
    save_lexicon(DEFAULT_LEXICON)
    return json.loads(json.dumps(DEFAULT_LEXICON))


def save_lexicon(data: dict):
    _write_json(LEXICON_PATH, data, indent=2)


def load_presets(strict: bool = False) -> dict:
    d = _load_json(PRESETS_PATH, {}, strict=strict)
    return d if isinstance(d, dict) else {}


def save_presets(data: dict):
    _write_json(PRESETS_PATH, data, indent=2)


# Flash colors (names match the UI's FLASH_COLORS). Plain words are mapped
# so OSC / API users can say "red" or "amber"; "#rrggbb" passes through.
FLASH_COLOR_NAMES = {"danger", "warn", "ok", "c1", "blue", "purple", "magenta",
                     "pink", "orange", "yellow", "lime", "teal", "white",
                     "gray", "black"}
_FLASH_ALIASES = {"red": "danger", "amber": "warn", "green": "ok", "cyan": "c1",
                  "grey": "gray"}


def flash_color(value) -> str:
    v = str(value or "").strip().lower()
    v = _FLASH_ALIASES.get(v, v)
    if v in FLASH_COLOR_NAMES or re.fullmatch(r"#[0-9a-f]{6}", v):
        return v
    return "danger"


def person_groups(p: dict) -> list:
    """An input's groups. 1.11+ stores a list in "groups"; older rosters
    (and older FraudCom versions) have one group in "group"."""
    g = p.get("groups")
    if isinstance(g, list):
        return [x for x in g if isinstance(x, str) and x]
    return [p["group"]] if isinstance(p.get("group"), str) and p["group"] else []


def set_person_groups(p: dict, groups) -> None:
    """Store an input's groups; "group" stays the first one for older readers."""
    out = []
    for x in groups:
        if isinstance(x, str) and x and x not in out:
            out.append(x)
    p["groups"] = out
    p["group"] = out[0] if out else ""


def load_roster() -> dict:
    """The last mirrored roster ({people, groups, pins, saved}), or empty."""
    d = _load_json(ROSTER_PATH, None)
    return d if isinstance(d, dict) else {"people": [], "groups": [], "pins": []}


def _prune_roster_backups():
    try:
        files = sorted(f for f in os.listdir(ROSTER_BACKUP_DIR)
                       if f.startswith("roster-") and f.endswith(".json"))
        for stale in files[:-ROSTER_BACKUPS_KEEP]:
            try:
                os.remove(os.path.join(ROSTER_BACKUP_DIR, stale))
            except Exception:
                pass
    except Exception:
        pass


def _recent_backup(within: float = 60.0) -> bool:
    """A burst of edits (typing a name, dragging inputs around) shares ONE
    snapshot — the state before the burst — instead of burning the history
    one entry per keystroke."""
    try:
        files = [f for f in os.listdir(ROSTER_BACKUP_DIR)
                 if f.startswith("roster-") and f.endswith(".json")]
        if not files:
            return False
        newest = max(os.path.getmtime(os.path.join(ROSTER_BACKUP_DIR, f))
                     for f in files)
        return time.time() - newest < within
    except Exception:
        return False


_roster_lock = threading.Lock()


def save_roster(data: dict) -> dict:
    """Persist the roster and, when it meaningfully changes, snapshot the
    PREVIOUS state into a timestamped backup first. Returns the saved dict.
    Serialized: two overlapping saves (two windows, a slow disk) must not
    interleave their read-previous / backup / write steps."""
    with _roster_lock:
        return _save_roster(data)


def _save_roster(data: dict) -> dict:
    people = data.get("people") or []
    groups = data.get("groups") or []
    pins = data.get("pins") or []
    prev = load_roster()
    changed = (json.dumps(prev.get("people", []), sort_keys=True)
               != json.dumps(people, sort_keys=True)
               or json.dumps(prev.get("groups", []), sort_keys=True)
               != json.dumps(groups, sort_keys=True))
    # Snapshot the outgoing roster before we overwrite it, but only if it held
    # something worth keeping — so a real prior state is always recoverable.
    if changed and (prev.get("people") or prev.get("groups")) \
            and not _recent_backup():
        try:
            os.makedirs(ROSTER_BACKUP_DIR, exist_ok=True)
            stamp = time.strftime("%Y%m%d-%H%M%S")
            bpath = os.path.join(ROSTER_BACKUP_DIR, f"roster-{stamp}.json")
            # Avoid clobbering a same-second backup.
            n = 0
            while os.path.exists(bpath):
                n += 1
                bpath = os.path.join(ROSTER_BACKUP_DIR,
                                     f"roster-{stamp}-{n}.json")
            _write_json(bpath, prev)
            _prune_roster_backups()
        except Exception:
            pass
    out = {"people": people, "groups": groups, "pins": pins,
           "saved": time.time()}
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        _write_json(ROSTER_PATH, out)
    except Exception:
        pass
    return out


def list_roster_backups() -> list:
    """Newest-first list of backups: {id, saved, inputs, groups, names}."""
    items = []
    # the live roster.json shows first as the current state
    cur = load_roster()
    if cur.get("people") or cur.get("groups"):
        items.append({
            "id": "current",
            "saved": cur.get("saved"),
            "inputs": len(cur.get("people", [])),
            "groups": len(cur.get("groups", [])),
            "names": [p.get("name") for p in cur.get("people", [])
                      if isinstance(p, dict)],
        })
    try:
        files = sorted((f for f in os.listdir(ROSTER_BACKUP_DIR)
                        if f.startswith("roster-") and f.endswith(".json")),
                       reverse=True)
    except Exception:
        files = []
    for fn in files:
        try:
            with open(os.path.join(ROSTER_BACKUP_DIR, fn),
                      "r", encoding="utf-8") as f:
                d = json.load(f)
            ppl = d.get("people", [])
            items.append({
                "id": fn[:-5],                      # strip .json
                "saved": d.get("saved"),
                "inputs": len(ppl),
                "groups": len(d.get("groups", [])),
                "names": [p.get("name") for p in ppl if isinstance(p, dict)],
            })
        except Exception:
            continue
    return items


def read_roster_backup(bid: str) -> Optional[dict]:
    if bid == "current":
        return load_roster()
    # guard against path traversal — only our own basenames are valid
    if ("/" in bid or "\\" in bid or ".." in bid
            or not bid.startswith("roster-")):
        return None
    path = os.path.join(ROSTER_BACKUP_DIR, bid + ".json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


class Corrector:
    """Case-insensitive whole-phrase replacement, longest phrases first."""

    def __init__(self, corrections: Dict[str, str]):
        self.update(corrections)

    def update(self, corrections: Dict[str, str]):
        # Build the new map + regex first, then swap them in as ONE object:
        # an audio thread must never see the new map with the old regex
        # (that raised KeyError mid-show and killed the channel).
        m = {str(k).lower(): str(v) for k, v in corrections.items()}
        rx = (re.compile(r"\b(" + "|".join(
                  re.escape(p) for p in sorted(m, key=len, reverse=True))
              + r")\b", re.IGNORECASE) if m else None)
        self._state = (m, rx)

    def apply(self, text: str) -> str:
        m, rx = self._state
        if not rx or not text:
            return text
        # .get: Unicode case-folding ("ſ", "ı") can match under IGNORECASE
        # yet not lower() back to a key — keep the original text then.
        return rx.sub(lambda g: m.get(g.group(0).lower(), g.group(0)), text)


# --------------------------------------------------------------------------
class ChannelWorker:
    """One Vosk recognizer fed by a queue of raw int16 mono audio.

    Keeps the raw audio of the current utterance so the Refiner can
    re-transcribe it with Whisper after Vosk finalizes."""

    MAX_UTTERANCE_BYTES = 48000 * 2 * 30     # ~30 s @ 48 kHz int16 — Whisper
                                             # chunks at 30 s anyway; caps refiner RAM
    GATE_RMS = 0.010          # ~-40 dBFS: quieter 1-2 word "utterances"
    GATE_MAX_WORDS = 2        # are treated as breath/noise and dropped

    GATE_HOLD = 1.0           # keep the gate open this long after speech
    AUTO_RATIO = 6.0          # auto gate opens ~16 dB above the noise floor
    AUTO_MIN = 0.0018         # …but never below ~-55 dBFS absolute

    def __init__(self, key: str, name: str, model: Model,
                 samplerate: int, emit, refiner=None,
                 gate_rms: Optional[float] = None, gate_auto: bool = False,
                 translate: bool = False):
        self.key = key            # "deviceIndex:channelIndex"
        self.name = name
        self.emit = emit
        self.refiner = refiner
        self.translate = translate   # send utterance to Whisper translate
        self.sr = samplerate
        # Background-rejection gate: blocks quieter than the threshold are
        # muted so off-mic conversations never reach the recognizers.
        # auto mode tracks this input's own noise floor instead of using a
        # fixed level, so quiet mics/low gain don't lose real speech.
        self.gate = gate_rms
        self.gate_auto = gate_auto
        self._floor = 0.003                  # adaptive noise-floor estimate
        self._gate_open_until = 0.0
        self._gate_open = False
        self._preroll: "deque[bytes]" = deque(maxlen=3)
        self._zeros_left = 0
        self.q: "queue.Queue[Optional[bytes]]" = queue.Queue(maxsize=200)
        self.dropped = 0
        self.rec = KaldiRecognizer(model, samplerate)
        self._last_partial = ""
        self._utt = bytearray()
        self._peak = 0.0
        self._n = 0
        # Message ids must be unique across Stop/Start and stream restarts:
        # "key#n" alone repeated, so a cue fired in soundcheck was silently
        # suppressed on the show's first utterance and refinements patched
        # the wrong (old) bubble.
        self._born = int(time.time() * 1000)
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def feed(self, data: bytes):
        try:
            self.q.put_nowait(data)
        except queue.Full:
            self.dropped += 1           # recognizer can't keep up: shown in health

    def stop(self):
        # Drop any backlog first: a stalled worker could otherwise keep
        # emitting up to ~40 s of stale lines after Stop (ghost lines if the
        # same channel is restarted). The sentinel still makes Vosk flush its
        # in-progress sentence, so the last words aren't lost.
        try:
            while True:
                self.q.get_nowait()
        except queue.Empty:
            pass
        self.q.put(None)

    def _final(self, text: str):
        # Energy gate: Vosk invents short words from breath/room noise.
        # A 1-2 word utterance whose audio never rose above ~-40 dBFS is
        # noise, not speech.
        if (len(text.split()) <= self.GATE_MAX_WORDS
                and self._peak < self.GATE_RMS):
            self._utt = bytearray()
            self._peak = 0.0
            return
        self._n += 1
        mid = f"{self.key}#{self._n}@{self._born}"
        self.emit("final", self.key, self.name, text, msg_id=mid)
        if self.refiner is not None and self._utt:
            self.refiner.submit(mid, self.key, self.name,
                                bytes(self._utt), self.sr, self.translate)
        self._utt = bytearray()
        self._peak = 0.0

    def _process(self, data: bytes, rms: float):
        """Run one audio block through Vosk and the utterance buffer."""
        self._utt.extend(data)
        if len(self._utt) > self.MAX_UTTERANCE_BYTES:
            del self._utt[:len(self._utt) - self.MAX_UTTERANCE_BYTES]
        if rms > self._peak:
            self._peak = rms
        if self.rec.AcceptWaveform(data):
            text = json.loads(self.rec.Result()).get("text", "")
            self._last_partial = ""
            if text:
                self._final(text)
            else:
                self._utt = bytearray()          # silence — drop the buffer
                self._peak = 0.0
        else:
            partial = json.loads(self.rec.PartialResult()).get("partial", "")
            if partial and partial != self._last_partial:
                self._last_partial = partial
                self.emit("partial", self.key, self.name, partial)

    def _run(self):
        warned = False
        while True:
            data = self.q.get()
            if data is None:
                try:
                    final = json.loads(self.rec.FinalResult()).get("text", "")
                    if final:
                        self._final(final)
                except Exception:
                    pass
                return
            try:
                self._block(data)
            except Exception as e:
                # One bad block must never silently kill this channel for the
                # rest of the show: report once, keep transcribing.
                if not warned:
                    warned = True
                    try:
                        self.emit("warn", "", "",
                                  text=f"{self.name}: transcription error ({e}) — continuing")
                    except Exception:
                        pass

    def _block(self, data: bytes):
        arr = np.frombuffer(data, dtype=np.int16)
        rms = 0.0
        if arr.size:
            rms = float(np.sqrt(np.mean(
                (arr.astype(np.float32) / 32768.0) ** 2)))
        if self.gate is None and not self.gate_auto:
            self._process(data, rms)
            return
        if self.gate_auto:
            thr = max(self._floor * self.AUTO_RATIO, self.AUTO_MIN)
            # Learn the floor ONLY from non-speech blocks (below the
            # threshold), gradually in both directions: gaps between
            # words can't crater it, and a whole sentence can't drag
            # it up and choke its own tail.
            if rms < thr:
                rate = 0.1 if rms < self._floor else 0.05
                self._floor += (rms - self._floor) * rate
                self._floor = max(self._floor, 2e-5)
        else:
            thr = self.gate
        now = time.time()
        if rms >= thr:
            self._gate_open_until = now + self.GATE_HOLD
        if now <= self._gate_open_until:
            if not self._gate_open:
                # Gate just opened: replay the muted lead-in so the
                # first word's onset isn't clipped — an abrupt
                # silence-to-speech edge makes Vosk hallucinate a
                # leading "the".
                self._gate_open = True
                while self._preroll:
                    self._process(self._preroll.popleft(), 0.0)
            self._process(data, rms)
        else:
            if self._gate_open:
                self._gate_open = False
                self._zeros_left = 6         # ~1.2 s of silence lets
            self._preroll.append(data)       # Vosk finalize the line
            if self._zeros_left > 0:
                self._zeros_left -= 1
                self._process(b"\x00" * len(data), 0.0)
                if self._zeros_left == 0:
                    # Endpointer didn't commit the sentence within the
                    # flush — force it out now rather than leaving the
                    # line hanging as a partial until the next speech.
                    text = json.loads(
                        self.rec.FinalResult()).get("text", "")
                    self._last_partial = ""
                    if text:
                        self._final(text)
                    else:
                        self._utt = bytearray()
                        self._peak = 0.0
            # fully closed: feed nothing — saves CPU too


def _one_line(s) -> str:
    """Transcript files are one entry per line: text arriving over OSC or the
    API can't forge extra lines."""
    return " ".join(str(s).splitlines())


class TranscriptLog:
    """Appends every final line to a dated file in DATA_DIR/transcripts —
    the transcript survives crashes, window closes, and Clear clicks.

    Lines are held in a short pending buffer (FLUSH_AFTER seconds) so the
    Whisper refinement can upgrade the text before it hits disk; a crash
    loses at most that window."""

    FLUSH_AFTER = 30.0        # > realistic Whisper latency under load, so the
                              # file gets the refined text; a crash loses ≤30 s

    def __init__(self, dirpath: str):
        self.dir = dirpath
        self.lock = threading.Lock()
        # Serializes whole flushes: the 2 s loop and mark() could otherwise
        # both pick up the same pending lines and write them twice.
        self._flush_lock = threading.Lock()
        self.pending: Dict[str, list] = {}    # mid -> [ts, name, text]
        threading.Thread(target=self._flush_loop, daemon=True).start()

    def _write_line(self, line: str, ts: float):
        path = os.path.join(
            self.dir, f"transcript-{time.strftime('%Y-%m-%d', time.localtime(ts))}.txt")
        with self.lock:
            os.makedirs(self.dir, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)

    def _flush_loop(self):
        while True:
            time.sleep(2)
            try:
                self.flush(older_than=self.FLUSH_AFTER)
            except Exception:
                pass

    def flush(self, older_than: float = 0.0):
        with self._flush_lock:
            now = time.time()
            with self.lock:
                due = sorted(((mid, v) for mid, v in self.pending.items()
                              if now - v[0] >= older_than), key=lambda kv: kv[1][0])
            # Write FIRST, forget second: a failed write (disk full, sync
            # lock) leaves the line pending so the 2 s loop retries it,
            # instead of silently dropping it from the "crash-proof" record.
            for mid, (ts, name, text) in due:
                stamp = time.strftime("%H:%M:%S", time.localtime(ts))
                self._write_line(f"[{stamp}] {_one_line(name)}: {_one_line(text)}\n", ts)
                with self.lock:
                    self.pending.pop(mid, None)

    def write_final(self, mid: str, name: str, text: str, ts: float):
        if not AUTOSAVE["on"]:
            return
        with self.lock:
            self.pending[mid] = [ts, name, text]

    def refine(self, mid: str, text: str):
        with self.lock:
            if mid in self.pending:
                self.pending[mid][2] = text

    def retract(self, mid: str):
        with self.lock:
            self.pending.pop(mid, None)

    def mark(self, text: str):
        # A transcript-disk problem must never abort Start/Stop/recovery
        # (it used to: a full or locked transcripts folder left streams
        # running with the UI showing IDLE).
        try:
            self.flush()
            if not AUTOSAVE["on"]:
                return
            ts = time.time()
            stamp = time.strftime("%H:%M:%S", time.localtime(ts))
            self._write_line(f"--- [{stamp}] {_one_line(text)} ---\n", ts)
        except Exception as e:
            try:
                engine._emit("warn", "", "", text=f"Transcript not saved: {e}")
            except Exception:
                pass


tlog = TranscriptLog(TRANSCRIPTS_DIR)


def load_state(strict: bool = False) -> dict:
    d = _load_json(STATE_PATH, {}, strict=strict)
    return d if isinstance(d, dict) else {}


def save_state(st: dict):
    _write_json(STATE_PATH, st, indent=2)


def _lan_url():
    """This machine's address for the share link / QR code. Prefer the
    interface with the default route (no packet is actually sent); on an
    isolated show network with no gateway (common for Dante/control LANs),
    fall back to the first non-loopback IPv4 address."""
    ip = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
    except OSError:
        try:
            ip = next((a for a in socket.gethostbyname_ex(socket.gethostname())[2]
                       if not a.startswith("127.")), None)
        except OSError:
            ip = None
    return f"http://{ip}:{PORT}" if ip else None


def _is_local(request) -> bool:
    return request.client is not None and request.client.host in ("127.0.0.1", "::1")


def _ensure_com():
    """ASIO drivers are COM objects: any thread that opens or closes a
    stream must have COM initialized. /start and /stop run on worker
    threads (asyncio.to_thread), which don't have it by default."""
    if os.name == "nt":
        try:
            import ctypes
            ctypes.windll.ole32.CoInitialize(None)
        except Exception:
            pass


def slugify(name: str) -> str:
    """A channel's slug: lowercase, spaces become hyphens."""
    return (name or "").strip().lower().replace(" ", "-")


# Extension points used by the remote API (remote_api.py):
EMIT_HOOKS: list = []      # called with every engine event (a dict)
ACTION_RUNNER: list = []   # [runner(rule, key, ctx)] for rules with an "actions" list


MIDI_VIRTUAL_NAME = "FraudCom"   # virtual output other apps can receive from


def _midi_bytes(r: dict) -> List[int]:
    """Build the MIDI message for a rule/test. Types: note, cc, program,
    raw (hex bytes such as an MMC or SysEx string)."""
    kind = (r.get("midi_type") or "note").lower()
    if kind == "raw":
        toks = (r.get("midi_raw") or "").replace(",", " ").split()
        if not toks:
            raise ValueError("raw MIDI message is empty")
        try:
            data = [int(t, 16) for t in toks]
        except ValueError:
            raise ValueError("raw MIDI must be hex bytes, e.g. F0 7F 7F 06 02 F7")
        if any(b < 0 or b > 255 for b in data):
            raise ValueError("raw MIDI bytes must be 00-FF")
        return data
    ch = min(16, max(1, int(r.get("midi_channel") or 1))) - 1
    num = min(127, max(0, int(r.get("midi_num") or 0)))
    val = min(127, max(0, int(r.get("midi_val")
                                if r.get("midi_val") not in (None, "") else 127)))
    if kind == "cc":
        return [0xB0 | ch, num, val]
    if kind == "program":
        return [0xC0 | ch, num]
    if kind == "noteoff":
        return [0x80 | ch, num, val if r.get("midi_val") not in (None, "") else 0]
    return [0x90 | ch, num, val]           # note / noteon


def _midi_describe(r: dict) -> str:
    kind = (r.get("midi_type") or "note").lower()
    port = r.get("midi_port") or (MIDI_VIRTUAL_NAME + " (virtual)")
    if kind == "raw":
        what = "MIDI " + (r.get("midi_raw") or "").upper()
    else:
        label = {"note": "note", "noteon": "note on", "noteoff": "note off",
                 "cc": "CC", "program": "program"}.get(kind, kind)
        what = f"MIDI ch{r.get('midi_channel') or 1} {label} {r.get('midi_num') or 0}"
        if kind != "program":
            v = r.get("midi_val")
            what += f" = {v if v not in (None, '') else 127}"
    return f"{what} → {port}"


class _CoreMIDI:
    """Minimal CoreMIDI sender for macOS via ctypes (no extra packages, so
    the build needs nothing new). Uses MIDIPacketListInit/Add so we never
    depend on the packet struct's packing."""

    ENC_UTF8 = 0x08000100

    def __init__(self):
        import ctypes as C
        from ctypes import c_void_p, c_uint32, c_ulong, c_char_p, c_long, \
            c_uint64, c_uint16, POINTER, byref
        self.C = C
        cm = C.CDLL("/System/Library/Frameworks/CoreMIDI.framework/CoreMIDI")
        cf = C.CDLL("/System/Library/Frameworks/CoreFoundation.framework/"
                    "CoreFoundation")
        cf.CFStringCreateWithCString.restype = c_void_p
        cf.CFStringCreateWithCString.argtypes = [c_void_p, c_char_p, c_uint32]
        cf.CFStringGetCString.restype = C.c_bool
        cf.CFStringGetCString.argtypes = [c_void_p, c_char_p, c_long, c_uint32]
        cf.CFRelease.argtypes = [c_void_p]
        cm.MIDIClientCreate.argtypes = [c_void_p, c_void_p, c_void_p,
                                        POINTER(c_uint32)]
        cm.MIDIOutputPortCreate.argtypes = [c_uint32, c_void_p,
                                            POINTER(c_uint32)]
        cm.MIDISourceCreate.argtypes = [c_uint32, c_void_p, POINTER(c_uint32)]
        cm.MIDIGetNumberOfDestinations.restype = c_ulong
        cm.MIDIGetDestination.restype = c_uint32
        cm.MIDIGetDestination.argtypes = [c_ulong]
        cm.MIDIObjectGetStringProperty.argtypes = [c_uint32, c_void_p,
                                                   POINTER(c_void_p)]
        cm.MIDIPacketListInit.restype = c_void_p
        cm.MIDIPacketListInit.argtypes = [c_void_p]
        cm.MIDIPacketListAdd.restype = c_void_p
        cm.MIDIPacketListAdd.argtypes = [c_void_p, c_ulong, c_void_p,
                                         c_uint64, c_ulong, c_char_p]
        cm.MIDISend.argtypes = [c_uint32, c_uint32, c_void_p]
        cm.MIDIReceived.argtypes = [c_uint32, c_void_p]
        self.cm, self.cf = cm, cf
        self._name_prop = c_void_p.in_dll(cm, "kMIDIPropertyDisplayName")
        self.client, self.port, self.source = c_uint32(), c_uint32(), c_uint32()
        self._check(cm.MIDIClientCreate(self._cfstr("FraudCom"), None, None,
                                        byref(self.client)), "MIDIClientCreate")
        self._check(cm.MIDIOutputPortCreate(self.client, self._cfstr("FraudCom Out"),
                                            byref(self.port)), "MIDIOutputPortCreate")
        self._byref = byref

    @staticmethod
    def _check(status, what):
        if status != 0:
            raise RuntimeError(f"CoreMIDI {what} failed (error {status})")

    def _cfstr(self, text):
        return self.cf.CFStringCreateWithCString(
            None, text.encode("utf-8"), self.ENC_UTF8)

    def _name(self, obj):
        ref = self.C.c_void_p()
        if self.cm.MIDIObjectGetStringProperty(
                obj, self._name_prop, self._byref(ref)) != 0 or not ref.value:
            return None
        buf = self.C.create_string_buffer(512)
        ok = self.cf.CFStringGetCString(ref, buf, 512, self.ENC_UTF8)
        self.cf.CFRelease(ref)
        return buf.value.decode("utf-8", "replace") if ok else None

    def destinations(self):
        out = []
        for i in range(self.cm.MIDIGetNumberOfDestinations()):
            ep = self.cm.MIDIGetDestination(i)
            n = self._name(ep)
            if ep and n and n != MIDI_VIRTUAL_NAME:
                out.append((n, ep))
        return out

    def _packets(self, data: List[int]):
        buf = self.C.create_string_buffer(1024)
        cur = self.cm.MIDIPacketListInit(buf)
        cur = self.cm.MIDIPacketListAdd(buf, len(buf), cur, 0, len(data),
                                        bytes(data))
        if not cur:
            raise RuntimeError("MIDI message too long")
        return buf

    def send(self, port_name: str, data: List[int]):
        pl = self._packets(data)
        if not port_name:                      # FraudCom's virtual source
            if not self.source.value:
                self._check(self.cm.MIDISourceCreate(
                    self.client, self._cfstr(MIDI_VIRTUAL_NAME),
                    self._byref(self.source)), "MIDISourceCreate")
            self._check(self.cm.MIDIReceived(self.source, pl), "MIDIReceived")
            return
        for n, ep in self.destinations():
            if n == port_name:
                self._check(self.cm.MIDISend(self.port, ep, pl), "MIDISend")
                return
        raise RuntimeError(f"MIDI port '{port_name}' not found — is the "
                           "device connected?")


class MIDIBus:
    """Sends voice-triggered MIDI. macOS: CoreMIDI directly, with FraudCom's
    own virtual output (empty port name) that QLab, Pro Tools, console
    editors etc. can select as a MIDI input. Elsewhere: python-rtmidi if it
    is installed."""

    NOTE_LEN = 0.1                # seconds between note on and note off

    def __init__(self):
        self._mac = None
        self._outs = {}
        self._lock = threading.Lock()

    def _backend(self):
        if sys.platform == "darwin":
            with self._lock:
                if self._mac is None:
                    self._mac = _CoreMIDI()
                return self._mac
        try:
            import rtmidi
        except Exception:
            raise RuntimeError("MIDI output isn't available in this build")
        return rtmidi

    def ports(self) -> dict:
        try:
            b = self._backend()
            if sys.platform == "darwin":
                return {"ports": [n for n, _ in b.destinations()],
                        "virtual": True, "error": None}
            probe = b.MidiOut()
            return {"ports": probe.get_ports(), "virtual": False, "error": None}
        except Exception as e:
            return {"ports": [], "virtual": False, "error": str(e)}

    def _raw_send(self, port: str, data: List[int]):
        b = self._backend()
        if sys.platform == "darwin":
            with self._lock:
                b.send(port, data)
            return
        with self._lock:
            out = self._outs.get(port)
            if out is None:
                if not port:
                    raise RuntimeError("pick a MIDI output port")
                out = b.MidiOut()
                names = out.get_ports()
                if port not in names:
                    raise RuntimeError(f"MIDI port '{port}' not found")
                out.open_port(names.index(port))
                self._outs[port] = out
        out.send_message(data)

    def send(self, r: dict):
        msg = _midi_bytes(r)
        port = r.get("midi_port") or ""
        self._raw_send(port, msg)
        if (r.get("midi_type") or "note").lower() == "note":
            off = [0x80 | (msg[0] & 0x0F), msg[1], 0]

            def _off():
                try:
                    self._raw_send(port, off)
                except Exception:
                    pass
            t = threading.Timer(self.NOTE_LEN, _off)
            t.daemon = True
            t.start()


def midi_from_api(m: dict) -> dict:
    """A remote-API SendMIDIRequest (type noteOn/noteOff/controlChange/
    programChange, channel 0-15, note/velocity/controller/value/program,
    optional deviceId = output port name) -> FraudCom's MIDI rule fields."""
    t = str(m.get("type") or "noteOn")
    kind = {"noteOn": "noteon", "noteOff": "noteoff", "controlChange": "cc",
            "programChange": "program"}.get(t)
    if kind is None:
        raise ValueError(f"unknown MIDI type '{t}'")
    num = {"noteon": m.get("note"), "noteoff": m.get("note"),
           "cc": m.get("controller"), "program": m.get("program")}[kind]
    val = m.get("value") if kind == "cc" else m.get("velocity")
    dev = str(m.get("deviceId") or "")
    return {"midi_type": kind, "midi_channel": int(m.get("channel") or 0) + 1,
            "midi_num": int(num or 0), "midi_val": val,
            "midi_port": "" if dev in ("", MIDI_VIRTUAL_NAME) else dev}


class Automations:
    """Each rule has a trigger — a phrase heard on a channel, or an incoming
    OSC message matching a pattern — and an action: send OSC (UDP or TCP),
    send MIDI, or flash the screen. Actions can use {template} variables from the
    trigger. Phrase rules fire at most once per utterance (dedup across the
    instant + refined text)."""

    def __init__(self, path: str):
        self.path = path
        self.rules = self._load()
        self.bus = osclink.Sender()
        self.midi = MIDIBus()
        self._fired = OrderedDict()  # (msg_id, rule_id) already sent, oldest first
        self._lock = threading.Lock()
        self._last = {}              # rule id -> when it last fired

    @staticmethod
    def display_name(r: dict) -> str:
        return (r.get("name") or r.get("phrase") or r.get("osc_pattern") or "").strip()

    def find(self, ref: str):
        """A rule by display name (any case) or slug."""
        ref = (ref or "").strip().lower()
        return next((r for r in self.rules
                     if ref in (self.display_name(r).lower(),
                                slugify(self.display_name(r)))), None)

    MIN_GAP = 0.05                   # s between firings of one rule (20/s)

    def fire(self, r: dict, key: str, ctx: dict, throttle: bool = False):
        """Run a rule's action now. ctx feeds its {template} variables.
        throttle (network triggers: OSC, webhook, API): one rule fires at
        most 20 times a second, so a rule that triggers itself (or a flood
        of triggers) can't run away. Heard phrases are never throttled: two
        channels saying the cue word together must both fire."""
        if throttle:
            now = time.monotonic()
            with self._lock:
                rid = r.get("id") or id(r)
                if now - self._last.get(rid, -1.0) < self.MIN_GAP:
                    return
                self._last[rid] = now
        name = self.display_name(r)
        if r.get("actions") and ACTION_RUNNER:   # made through the remote API:
            ACTION_RUNNER[0](r, key, ctx)        # a list of API actions
            return
        try:
            if r.get("action") == "flash":
                # Blink a full-screen message so a critical call can't be
                # missed. Purely a UI event — no external I/O.
                text = osclink.expand(r.get("flash_text") or name, ctx)
                engine._emit("flash", key, name, text=text,
                             levels={"color": flash_color(r.get("flash_color")),
                                     "secs": float(r.get("flash_secs") or 4)})
                engine._emit("automation", key, name, text="flash: " + text)
            elif r.get("action") == "midi":
                self.midi.send(r)
                engine._emit("automation", key, name, text=_midi_describe(r))
            else:
                addr = osclink.expand(r.get("address") or "/", ctx)
                self.bus.send(r.get("host") or "127.0.0.1", r.get("port") or 8000,
                              addr, osclink.parse_args(r.get("args", ""), ctx),
                              proto=r.get("proto") or "udp")
                engine._emit("automation", key, name,
                             text=f"{addr} → {r.get('host')}:{r.get('port')}"
                                  f"{' (TCP)' if r.get('proto') == 'tcp' else ''}")
        except Exception as e:
            engine._emit("warn", "", "", text=f"automation failed: {e}")

    def on_webhook(self, path: str) -> int:
        """Fire every enabled webhook rule on this path. -> how many fired."""
        path = path.strip("/")
        hits = [r for r in self.rules if r.get("enabled", True)
                and r.get("trigger") == "webhook"
                and (r.get("webhook_path") or "").strip("/") == path]
        for r in hits:
            self.fire(r, "", {"value": path, "raw": path}, throttle=True)
        return len(hits)

    def on_osc(self, address: str, args):
        """Fire every enabled OSC-triggered rule whose pattern matches."""
        for r in self.rules:
            if r.get("enabled", True) and r.get("trigger") == "osc":
                caps = osclink.match((r.get("osc_pattern") or "").strip(), address)
                if caps is not None:
                    self.fire(r, "", osclink.message_context(address, args, caps),
                              throttle=True)

    def _load(self):
        d = _load_json(self.path, [])
        return d if isinstance(d, list) else []

    def save(self, rules):
        self.rules = rules
        _write_json(self.path, rules, indent=2)

    def on_text(self, msg_id: str, key: str, text: str, name: str = ""):
        if not self.rules or not text:
            return
        for r in self.rules:
            if not r.get("enabled", True) or (r.get("trigger") or "phrase") != "phrase":
                continue
            # Whole-word/phrase match: a "go" rule must not fire on "gone",
            # "google", or "cargo" — on an OSC cue that's a real show hazard.
            # A rule may list several keywords (the remote API's message triggers do).
            m = None
            for phrase in (r.get("keywords") or [r.get("phrase")]):
                phrase = str(phrase or "").strip()
                m = phrase and re.search(r"(?<!\w)" + re.escape(phrase) + r"(?!\w)",
                                         text, re.IGNORECASE)
                if m:
                    break
            if not m:
                continue
            ch = r.get("channel") or ""
            if ch and ch != key:
                continue
            fid = (msg_id, r.get("id"))
            with self._lock:
                if fid in self._fired:
                    continue
                self._fired[fid] = True
                while len(self._fired) > 4000:   # evict the OLDEST, not
                    self._fired.popitem(last=False)   # a random set slice
            self.fire(r, key, {"value": m.group(0), "source.channel": name,
                               "raw": text})


class Refiner:
    """Background Whisper pass: re-transcribes each finalized utterance
    locally (faster-whisper, int8 CPU) and replaces the fast Vosk text in
    the UI and the transcript log a few seconds later. 100% offline once
    the model is cached in DATA_DIR/whisper."""

    MODEL_NAME = os.environ.get("WHISPER_MODEL", "small.en")
    TMODEL_NAME = os.environ.get("WHISPER_TMODEL", "small")  # multilingual

    def __init__(self, engine):
        self.engine = engine
        self.q: "queue.Queue[tuple]" = queue.Queue(maxsize=64)
        self.skipped = 0             # lines left unrefined because Whisper was busy
        self.enabled = load_state().get("refine", True)
        self.model = None            # English transcribe model (small.en)
        self.tmodel = None           # multilingual model for translation
        self.failed = False
        self.tfailed = False
        self.load_error = None
        self.lock = threading.Lock()
        self._last_job = time.time()
        for _ in range(2):
            threading.Thread(target=self._run, daemon=True).start()
        threading.Thread(target=self._janitor, daemon=True).start()

    IDLE_UNLOAD = 600         # release model RAM after 10 min of silence

    def _janitor(self):
        while True:
            time.sleep(60)
            if time.time() - self._last_job < self.IDLE_UNLOAD:
                continue
            with self.lock:
                if self.model is not None or self.tmodel is not None:
                    self.model = None
                    self.tmodel = None
                    import gc
                    gc.collect()      # ~1-2 GB back; reloads in ~2 s

    def submit(self, mid: str, key: str, name: str, audio: bytes, sr: int,
               translate: bool = False):
        # translation still runs even if plain refinement is off
        if (not self.enabled and not translate) or (self.failed and not translate):
            return
        try:
            self.q.put_nowait((mid, key, name, audio, sr, translate))
        except queue.Full:
            self.skipped += 1                 # busy — keep the Vosk text

    @staticmethod
    def _whisper_root():
        # Split-install layout: models live one level up (../whisper), a
        # sibling of the app/ folder, so updates that replace app/ leave
        # them intact. Fall back to a whisper/ next to the exe (old flat
        # layout), then the user's data dir.
        for p in ("whisper", os.path.join("..", "whisper")):
            if os.path.isdir(p):
                return p
        return os.path.join(DATA_DIR, "whisper")

    def _open_model(self, name: str, num_workers: int):
        """Load a Whisper model with NO network access when it's already on
        disk. Without local_files_only, faster-whisper asks huggingface.co for
        the file list on every load (every reload after an idle unload too).
        Only a model that is genuinely missing is downloaded, once."""
        from faster_whisper import WhisperModel
        kw = dict(device="cpu", compute_type="int8",
                  download_root=self._whisper_root(),
                  cpu_threads=4, num_workers=num_workers)
        try:
            return WhisperModel(name, local_files_only=True, **kw)
        except Exception:
            return WhisperModel(name, **kw)       # not cached yet: fetch once

    def _load(self):
        with self.lock:
            if self.model is None and not self.failed:
                try:
                    self.model = self._open_model(self.MODEL_NAME, 2)
                except Exception as e:
                    self.failed = True
                    self.load_error = str(e)
                    self.engine._emit(
                        "warn", "", "",
                        text=f"Whisper refinement unavailable: {e}")
            return self.model

    def selftest(self):
        """Force-load the model and report the outcome — used by the launch
        self-test (FRAUDCOM_WHISPER_SELFTEST) to verify a frozen build without
        needing live audio."""
        ok = self._load() is not None
        return {"ok": ok, "error": self.load_error,
                "root": self._whisper_root()}

    def _load_t(self):
        # Multilingual model, loaded lazily only when a channel actually
        # translates — so translation costs no memory unless used.
        with self.lock:
            if self.tmodel is None and not self.tfailed:
                try:
                    self.tmodel = self._open_model(self.TMODEL_NAME, 1)
                except Exception as e:
                    self.tfailed = True
                    self.engine._emit(
                        "warn", "", "",
                        text=f"Translation model unavailable: {e}")
            return self.tmodel

    def _run(self):
        while True:
            item = self.q.get()
            mid, key, name, audio, sr, translate = item
            self._last_job = time.time()
            if not self.enabled and not translate:
                continue
            try:
                pcm = (np.frombuffer(audio, dtype=np.int16)
                       .astype(np.float32) / 32768.0)
                if sr != 16000:
                    n = int(len(pcm) * 16000 / sr)
                    pcm = np.interp(
                        np.linspace(0.0, len(pcm) - 1.0, n),
                        np.arange(len(pcm), dtype=np.float64),
                        pcm).astype(np.float32)
                if translate:
                    model = self._load_t()
                    if model is None:
                        continue          # can't translate — keep Vosk text
                    segments, _ = model.transcribe(
                        pcm, task="translate", beam_size=1,
                        condition_on_previous_text=False,
                        without_timestamps=True, vad_filter=True,
                        vad_parameters={"min_silence_duration_ms": 300})
                else:
                    model = self._load()
                    if model is None:
                        continue
                    hot = self.engine.lexicon.get("hotwords", [])[:40]
                    segments, _ = model.transcribe(
                        pcm, language="en", beam_size=1,
                        condition_on_previous_text=False,
                        without_timestamps=True, vad_filter=True,
                        vad_parameters={"min_silence_duration_ms": 300},
                        initial_prompt=", ".join(hot) or None)
                text = " ".join(s.text.strip() for s in segments).strip()
            except Exception:
                continue
            if not text:
                # Whisper's VAD found no speech: the Vosk line was a
                # phantom — pull it from the feed and the pending log.
                try:
                    tlog.retract(mid)
                except Exception:
                    pass
                self.engine._emit("retract", key, name, msg_id=mid)
                continue
            try:
                text = self.engine.corrector.apply(text)
                tlog.refine(mid, text)
                self.engine._emit("refine", key, name, text, msg_id=mid,
                                  translated=translate)
            except Exception:
                pass          # never let one job kill this refiner thread


class Engine:
    def __init__(self):
        self.refiner: Optional[Refiner] = None
        self.model: Optional[Model] = None
        self.streams: List[dict] = []    # stream records with heartbeat
        self.workers: Dict[str, ChannelWorker] = {}
        self.running = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.subscribers: List[asyncio.Queue] = []
        # Serializes start/stop/restart: without it, a Stop landing while the
        # watchdog is mid-restart left a live stream + workers transcribing
        # after the UI said "stopped".
        self._lock = threading.RLock()
        # Channel keys muted by the operator (or OSC). Lives on the engine, not
        # the worker, so a mute survives the watchdog rebuilding a stream.
        self.muted: set = set()
        self.last_devices: Optional[List[dict]] = None   # for OSC /start
        self.lexicon = load_lexicon()
        self.corrector = Corrector(self.lexicon["corrections"])
        threading.Thread(target=self._watchdog, daemon=True).start()

    # ---- event fan-out ----------------------------------------------------
    def _emit(self, etype: str, key: str, name: str, text: str = "",
              levels: Optional[dict] = None, msg_id: Optional[str] = None,
              translated: bool = False, gates: Optional[dict] = None,
              extra: Optional[dict] = None):
        if etype in ("partial", "final"):
            text = self.corrector.apply(text)
        if etype == "final" and text and msg_id:
            try:
                tlog.write_final(msg_id, name, text, time.time())
            except Exception:
                pass                    # disk trouble must never kill capture
        if etype in ("final", "refine") and text and msg_id:
            try:
                automations.on_text(msg_id, key, text, name)
            except Exception:
                pass
        msg = {"type": etype, "channel": key, "name": name,
               "text": text, "ts": time.time()}
        if msg_id:
            msg["id"] = msg_id
        if translated:
            msg["translated"] = True
        if levels is not None:
            msg["levels"] = levels
        if gates is not None:
            msg["gates"] = gates
        if extra:
            msg.update(extra)
        for hook in EMIT_HOOKS:              # e.g. the remote API
            try:
                hook(msg)
            except Exception:
                pass
        if self.loop is None:
            return
        for sub in list(self.subscribers):
            self.loop.call_soon_threadsafe(self._safe_put, sub, msg)

    @staticmethod
    def _safe_put(sub: asyncio.Queue, msg: dict):
        try:
            sub.put_nowait(msg)
        except asyncio.QueueFull:
            pass

    # ---- devices ------------------------------------------------------------
    @staticmethod
    def list_devices():
        hostapis = sd.query_hostapis()
        out = []
        for idx, dev in enumerate(sd.query_devices()):
            if dev["max_input_channels"] > 0:
                out.append({
                    "index": idx,
                    "name": dev["name"],
                    "hostapi": hostapis[dev["hostapi"]]["name"],
                    "max_input_channels": dev["max_input_channels"],
                    "default_samplerate": dev["default_samplerate"],
                })
        return out

    @staticmethod
    def _rescan_portaudio():
        """macOS: PortAudio's device list is frozen when it initializes, so an
        interface — or Dante Virtual Soundcard — started AFTER FraudCom was
        invisible to capture. Re-initialize it; callers do this only while no
        stream is open. Never on Windows: unloading Dante's ASIO driver tears
        down the process's Winsock (see _fresh_devices)."""
        if sys.platform == "darwin":
            sd._terminate()
            sd._initialize()

    @staticmethod
    def _resolve_index(d: dict) -> int:
        """Map a device chosen from a fresh scan onto THIS session's
        PortAudio numbering (they drift when hardware is plugged in or
        removed after launch)."""
        name, hostapi = d.get("name"), d.get("hostapi")
        if not name:
            return d["device_index"]
        for idx, dev in enumerate(sd.query_devices()):
            if (dev["max_input_channels"] > 0 and dev["name"] == name
                    and sd.query_hostapis(dev["hostapi"])["name"] == hostapi):
                return idx
        if sys.platform == "darwin":       # Start re-reads the device list
            raise RuntimeError(
                f"'{name}' isn't available — check it's connected (or that "
                "Dante Virtual Soundcard is running), then press Start again")
        raise RuntimeError(
            f"'{name}' is not available in this session — if it was just "
            "connected, close and reopen FraudCom")

    def _open_device(self, device_index: int, channels: List[dict],
                     samplerate: Optional[int], key_index: Optional[int] = None):
        """key_index: the device number this session's channel keys use. A
        restart passes the ORIGINAL one, so mute state and the UI's tiles
        stay attached even if the device came back under a new index."""
        if key_index is None:
            key_index = device_index
        dev = sd.query_devices(device_index)
        hostapi = sd.query_hostapis(dev["hostapi"])["name"]
        selected = sorted(c["index"] for c in channels)
        names = {c["index"]: (c.get("name") or f"Ch {c['index'] + 1}")
                 for c in channels}
        gates = {c["index"]: (bool(c.get("gate_auto")),
                              10 ** (c["gate_db"] / 20)
                              if c.get("gate_db") is not None else None)
                 for c in channels}
        translates = {c["index"]: (str(c.get("translate") or "").lower() == "en")
                      for c in channels}

        extra = None
        if "ASIO" in hostapi:
            extra = sd.AsioSettings(channel_selectors=selected)
            open_channels = len(selected)
            col_map = {ch: i for i, ch in enumerate(selected)}
        else:
            open_channels = min(max(selected) + 1, dev["max_input_channels"])
            col_map = {ch: ch for ch in selected}

        # Rate choice matters beyond compatibility: opening an ASIO device
        # SETS its clock — starting DVS at 44.1 kHz flips the whole Dante
        # device to 44.1 and silently breaks its 48 k subscriptions. Try
        # 48 kHz (the live-audio standard) FIRST so we pin, never drag,
        # the device's rate.
        #
        # macOS is different: Core Audio opens never change the device's
        # clock, and a rate that does NOT match the device makes PortAudio
        # resample — its resampler puts a channels x frames buffer on Core
        # Audio's small (544 KB) audio-thread stack, which overflowed and
        # closed the app (DVS at 96 k / 44.1 k with many channels). So on a
        # Mac use the device's own rate first: no resampling at all.
        if sys.platform == "darwin":
            candidates = ([int(samplerate)] if samplerate else []) + [
                int(dev["default_samplerate"]), 48000, 44100, 96000]
        else:
            candidates = ([int(samplerate)] if samplerate else []) + [
                48000, int(dev["default_samplerate"]), 44100, 96000]
        sr = None
        tried = []
        last_err = None
        # macOS: some Core Audio devices (Dante Virtual Soundcard) only open
        # with their full channel count, so try that as a second layout.
        # col_map is by channel index, so opening more channels is safe.
        layouts = [open_channels]
        if sys.platform == "darwin" and "ASIO" not in hostapi:
            full = int(dev["max_input_channels"])
            if full > open_channels:
                layouts.append(full)
        for layout in layouts:
            for cand in candidates:
                if (layout, cand) in tried:
                    continue
                tried.append((layout, cand))
                try:
                    sd.check_input_settings(
                        device=device_index, channels=layout,
                        dtype="int16", samplerate=cand, extra_settings=extra)
                    sr = cand
                    open_channels = layout
                    break
                except Exception as e:
                    last_err = e
                    continue
            if sr is not None:
                break
        if sr is None:
            rates = sorted({c for _, c in tried})
            if "dante" in dev["name"].lower():
                raise RuntimeError(
                    f"{dev['name']} could not be opened. Open the Dante "
                    "Virtual Soundcard app, click Start (wait until it shows "
                    "a sample rate), then press Start listening again. "
                    f"System said: {last_err}")
            raise RuntimeError(
                f"no workable sample rate (tried {rates}) — check the "
                f"device's clock/network sample rate. System said: {last_err}")

        dev_workers = {}
        for ch in selected:
            key = f"{key_index}:{ch}"
            dev_workers[ch] = ChannelWorker(
                key, names[ch], self.model, sr, self._emit,
                refiner=self.refiner, gate_auto=gates[ch][0],
                gate_rms=gates[ch][1], translate=translates[ch])

        rec = {"device_index": device_index, "channels": channels,
               "samplerate": samplerate, "stream": None,
               "name": dev["name"], "hostapi": hostapi,
               "key_index": key_index,
               "keys": [f"{key_index}:{ch}" for ch in selected],
               "last_cb": time.time(), "last_level": 0.0}

        # macOS opens with small hardware blocks (see HW_BLOCK) so the audio
        # thread's stack stays small; they're gathered back into the usual
        # BLOCK_SECONDS chunks here, so recognizers and queues see no change.
        block_frames = int(sr * BLOCK_SECONDS)
        hw_block = HW_BLOCK if sys.platform == "darwin" else 0
        acc = {"bufs": [], "n": 0}

        def callback(indata, frames, tinfo, status):
            now = time.time()
            rec["last_cb"] = now              # watchdog heartbeat
            if hw_block:
                acc["bufs"].append(indata.copy())
                acc["n"] += frames
                if acc["n"] < block_frames:
                    return
                indata = np.concatenate(acc["bufs"])
                acc["bufs"], acc["n"] = [], 0
            # Meter throttle is PER STREAM: a shared one let the first device
            # claim every slot and the second device's meters looked dead.
            send_levels = now - rec["last_level"] >= LEVEL_INTERVAL
            levels = {}
            gates = {}
            for ch, worker in dev_workers.items():
                col = col_map[ch]
                if col >= indata.shape[1]:
                    continue
                mono = np.ascontiguousarray(indata[:, col])
                # Muted: the recognizer hears silence, so a sentence in
                # progress still finishes cleanly; the meter keeps showing
                # the real input.
                worker.feed(bytes(mono.nbytes) if worker.key in self.muted
                            else mono.tobytes())
                if send_levels:
                    rms = float(np.sqrt(np.mean(
                        (mono.astype(np.float32) / 32768.0) ** 2)))
                    levels[worker.key] = round(min(1.0, rms * 6), 3)
                    gates[worker.key] = (worker._gate_open
                                         if (worker.gate_auto
                                             or worker.gate is not None)
                                         else None)
            if send_levels:
                rec["last_level"] = now
                self._emit("levels", "", "", levels=levels, gates=gates)

        try:
            stream = sd.InputStream(
                device=device_index, channels=open_channels, samplerate=sr,
                dtype="int16", blocksize=hw_block or block_frames,
                callback=callback, extra_settings=extra)
            stream.start()
        except Exception:
            for w in dev_workers.values():     # don't leave idle worker
                w.stop()                       # threads behind a failed open
            raise
        # Register workers only once the stream is really running, so the
        # channel count and Stop never include channels that never opened.
        for ch, w in dev_workers.items():
            self.workers[w.key] = w
        rec["stream"] = stream
        self.streams.append(rec)

    def start(self, devices: List[dict]):
        with self._lock:
            return self._start(devices)

    def _start(self, devices: List[dict]):
        _ensure_com()
        if self.running:
            self.stop()
        self._rescan_portaudio()          # nothing is open now (macOS only)
        if self.model is None:
            if not os.path.isdir(MODEL_PATH):
                raise RuntimeError(
                    f"Vosk model not found at '{MODEL_PATH}'. Download one "
                    "(see README.md) and unzip it there, or set VOSK_MODEL_PATH.")
            self.model = Model(MODEL_PATH)

        opened = 0
        errors = []
        for d in devices:
            if not d["channels"]:
                continue
            try:
                idx = self._resolve_index(d)
                self._open_device(idx, d["channels"], d.get("samplerate"))
                opened += 1
            except Exception as e:
                errors.append(f"device {d.get('name') or d['device_index']}: {e}")
        if opened == 0:
            self.stop()
            raise RuntimeError("; ".join(errors) or "no channels selected")
        self.last_devices = devices
        if self.refiner:
            self.refiner.skipped = 0
        self.running = True
        tlog.mark(f"listening started ({len(self.workers)} channel"
                  f"{'s' if len(self.workers) != 1 else ''})")
        self._emit("status", "", "", text="listening")
        if errors:
            self._emit("warn", "", "", text="Some devices failed: " +
                       "; ".join(errors))

    def _watchdog(self):
        """A device that dies mid-show (DVS drop, USB glitch) just goes
        silent — its callback stops. Notice within seconds, flag the
        channels, and restart the stream automatically.

        ONE watchdog for the process lifetime (started in __init__): a
        per-start thread could outlive a quick Stop→Start and then two of
        them would restart the same dead stream, doubling every line.

        It also reports health every tick, so the operator sees during
        soundcheck whether this machine keeps up with the channel count."""
        cpu_mark = (time.process_time(), time.time())
        while True:
            time.sleep(2)
            pt, wt = time.process_time(), time.time()
            cpu = (pt - cpu_mark[0]) / max(wt - cpu_mark[1], 1e-6) \
                / (os.cpu_count() or 1) * 100
            cpu_mark = (pt, wt)
            if not self.running:
                continue
            for rec in list(self.streams):
                if not self.running:
                    break
                if time.time() - rec["last_cb"] > 6:
                    self._restart_stream(rec)
            try:
                self._emit_health(cpu)
            except Exception:
                pass

    def _emit_health(self, cpu: float):
        workers = list(self.workers.values())
        r = self.refiner
        self._emit("health", "", "", extra={
            "cpu": round(cpu),
            # seconds of audio queued in the slowest channel's recognizer
            "lag": round(max((w.q.qsize() for w in workers), default=0)
                         * BLOCK_SECONDS, 1),
            "dropped": sum(w.dropped for w in workers),
            "backlog": r.q.qsize() if r else 0,
            "skipped": r.skipped if r else 0})

    def set_mute(self, key: str, on: bool):
        if on == (key in self.muted):
            return
        (self.muted.add if on else self.muted.discard)(key)
        w = self.workers.get(key)
        name = w.name if w else key
        if self.running:
            tlog.mark(f"{name} {'muted' if on else 'unmuted'}")
        self._emit("mute", key, name, text="on" if on else "off")

    def _restart_stream(self, rec):
        with self._lock:
            # stop() may have run between the watchdog's check and now —
            # never reopen a stream that was deliberately closed.
            if not self.running or rec not in self.streams:
                return
            return self._restart_stream_locked(rec)

    def _restart_stream_locked(self, rec):
        _ensure_com()
        keys = rec["keys"]
        self._emit("chanstate", ",".join(keys), "", text="down")
        self._emit("warn", "", "",
                   text=f"Audio stream lost ({len(keys)} channel"
                        f"{'s' if len(keys) != 1 else ''}) — restarting…")
        tlog.mark(f"stream lost on {', '.join(keys)} — restarting")
        try:
            rec["stream"].stop(); rec["stream"].close()
        except Exception:
            pass
        if rec in self.streams:
            self.streams.remove(rec)
        for k in keys:
            w = self.workers.pop(k, None)
            if w:
                w.stop()
        try:
            idx = rec["device_index"]
            if sys.platform == "darwin" and not self.streams:
                # Dante Virtual Soundcard relaunched mid-show comes back as a
                # NEW Core Audio device: re-read the list and find it by name.
                # (Only when no other device is capturing — a re-init would
                # cut those off.)
                self._rescan_portaudio()
                idx = self._resolve_index(rec)
            self._open_device(idx, rec["channels"], rec["samplerate"],
                              key_index=rec.get("key_index"))
            self._emit("chanstate", ",".join(keys), "", text="up")
            self._emit("warn", "", "", text="Audio stream recovered.")
            tlog.mark("stream recovered")
        except Exception as e:
            # keep the record so the watchdog retries in ~8 s
            rec["last_cb"] = time.time() + 6
            self.streams.append(rec)
            hint = ""
            if sys.platform == "darwin" and self.streams:
                # ponytail: re-reading the device list would cut the inputs
                # that are still capturing, so a device that came back as a
                # NEW Core Audio device is only found by Stop → Start. A
                # coordinated stop-all/rescan/reopen could automate this.
                hint = (" If you restarted Dante Virtual Soundcard or "
                        "reconnected this device, press Stop then Start.")
            self._emit("warn", "", "",
                       text=f"Stream restart failed ({e}) — retrying…{hint}")

    def stop(self):
        with self._lock:
            return self._stop()

    def _stop(self):
        _ensure_com()
        was_running = self.running
        self.running = False           # stops the watchdog first
        for rec in self.streams:
            try:
                rec["stream"].stop(); rec["stream"].close()
            except Exception:
                pass
        self.streams = []
        workers = list(self.workers.values())
        for w in workers:
            w.stop()
        # Let each worker finish its last sentence BEFORE writing the stop
        # marker (which flushes the transcript): otherwise every channel's
        # final line landed after the flush and was lost if the window closed.
        for w in workers:
            w.thread.join(timeout=2)
        self.workers = {}
        if was_running:
            tlog.mark("listening stopped")
        self._emit("status", "", "", text="stopped")


def _truthy(v) -> bool:
    return str(v).strip().lower() not in ("0", "0.0", "off", "false", "no", "")


class OSCReceiver:
    """Optional OSC INPUT — UDP, TCP (SLIP framing), or both on one port — so
    show control can drive FraudCom. Commands are addressed /fraudcom/<command>.
    Replies go back to the sender, on the same transport.

      ping   version
      channel/count  channel/list  channel/next  channel/previous
      channel/{ch}/select | send <text> | clear | mark | name
          {ch} = 1-based index, slug ("stage-left") or name; also all, selected
      scroll/up | down | current     window/show | hide
      font/increase | decrease       flash <text> [seconds] [repeats]
      automation/trigger <name>      automation/{slug}/trigger
      automation/count  automation/list  automation/{slug}/name
      marker [text]   note <text>   mute <ch|all> [1|0]   unmute <ch|all>
      start   stop

    Replies: ping -> <prefix>/pong; version -> <prefix>/version/reply; count,
    list and name -> "<request>/reply" (a list is one reply per item: i index,
    s slug, s name; a name is s slug, s name).

    Every incoming message (any address) is also offered to OSC-triggered
    automations. Off by default: OSC has no authentication, so anything on
    the network that can reach the port can send these."""

    # 9000 is the usual show-control OSC port. If another app already holds
    # it, FraudCom uses FALLBACK_PORT and says so — only when the user hasn't
    # picked a port.
    DEFAULT_PORT = 9000
    FALLBACK_PORT = 9765
    PREFIX = "/fraudcom/"
    QUIET = "quiet"            # queries (ping, lists…) don't pop a toast

    def __init__(self):
        self.server = None
        self.port = None
        self.proto = None
        self.error = None
        self.note = None            # e.g. "9000 is in use, so FraudCom took 9765"
        self.selected = None        # channel key picked by select / the UI
        self._want = None
        self._lock = threading.Lock()

    def apply(self, on: bool, port: Optional[int], proto: str = "both"):
        """port None = automatic: 9000, else the fallback."""
        proto = proto if proto in ("udp", "tcp", "both") else "both"
        with self._lock:
            if self.server and (not on or (port, proto) != self._want):
                self.server.close()
                self.server = None
            self.error = self.note = None
            if not on or self.server:
                return
            tries = [port] if port else [self.DEFAULT_PORT, self.FALLBACK_PORT]
            for i, p in enumerate(tries):
                try:
                    self.server = osclink.Server(HOST, p, self._handle, proto)
                except OSError as e:
                    if i == len(tries) - 1:
                        self.error = f"Can't listen on port {p} ({e.strerror or e})."
                        return
                    continue
                self.port, self.proto, self._want = p, proto, (port, proto)
                if p != tries[0]:
                    self.note = (f"Port {tries[0]} is in use by another app, so "
                                 f"FraudCom is listening on {p}.")
                return

    def _handle(self, address: str, args, reply):
        try:
            automations.on_osc(address, args)  # pattern triggers see everything
        except Exception:
            pass
        if not address.lower().startswith(self.PREFIX):
            return                  # show networks are chatty: ignore the rest
        prefix = self.PREFIX
        parts = [x for x in address[len(prefix):].split("/") if x]
        try:
            quiet = self._dispatch(prefix.rstrip("/"), parts, list(args), reply)
            if quiet != self.QUIET:
                engine._emit("oscin", "", "", text=" ".join(
                    [address] + [osclink.show(a) for a in args]))
        except Exception as e:
            engine._emit("warn", "", "", text=f"OSC {address}: {e}")

    # ---- channels: the roster, in sidebar order -------------------------------
    @staticmethod
    def channels() -> List[dict]:
        out = []
        for p in load_roster().get("people", []):
            if isinstance(p, dict):
                name = (p.get("name") or "").strip() or "Speaker"
                key = (f"{p['di']}:{p['ch']}" if p.get("di") is not None
                       and p.get("ch") is not None else None)
                out.append({"index": len(out) + 1, "slug": slugify(name),
                            "name": name, "key": key})
        return out

    def resolve(self, ref: str, chans=None) -> List[dict]:
        """{ch} -> channels: 1-based index, slug or name (any case), all, selected."""
        chans = self.channels() if chans is None else chans
        r = (ref or "").strip()
        if r.lower() == "all":
            return chans
        if r.lower() == "selected":
            hit = [c for c in chans if c["key"] and c["key"] == self.selected]
            if not hit:
                raise ValueError("no channel is selected")
            return hit
        if r.isdigit():
            if 1 <= int(r) <= len(chans):
                return [chans[int(r) - 1]]
        else:
            hit = [c for c in chans
                   if c["slug"] == slugify(r) or c["name"].lower() == r.lower()]
            if hit:
                return hit[:1]
        raise ValueError(f"no channel '{ref}'")

    def select(self, c: dict):
        self.selected = c["key"]
        engine._emit("ui", c["key"] or "", c["name"], extra={"action": "select"})

    # ---- commands ---------------------------------------------------------------
    def _dispatch(self, prefix: str, parts: List[str], args: list, reply):
        w = [x.lower() for x in parts]
        text = " ".join(osclink.show(a) for a in args).strip()

        def R(sub, *vals):
            reply(f"{prefix}/{sub}", list(vals))

        def ui(action, key="", **kw):
            engine._emit("ui", key, "", extra={"action": action, **kw})

        if w == ["ping"]:
            R("pong")
            return self.QUIET
        if w == ["version"]:
            R("version/reply", APP_VERSION)
            return self.QUIET
        if w[:1] == ["channel"] and len(w) >= 2:
            chans = self.channels()
            if w[1:] == ["count"]:
                R("channel/count/reply", len(chans))
                return self.QUIET
            if w[1:] == ["list"]:
                for c in chans:
                    R("channel/list/reply", c["index"], c["slug"], c["name"])
                return self.QUIET
            if w[1:] in (["next"], ["previous"]):
                if not chans:
                    raise ValueError("no channels")
                cur = next((c["index"] for c in chans
                            if c["key"] and c["key"] == self.selected), 0)
                step = 1 if w[1] == "next" else -1
                i = (cur - 1 + step) % len(chans) if cur else (0 if step > 0 else len(chans) - 1)
                self.select(chans[i])
                return None
            if len(w) == 3:
                ref, verb = parts[1], w[2]
                targets = self.resolve(ref, chans)
                if verb == "name":
                    R(f"channel/{ref}/name/reply", targets[0]["slug"], targets[0]["name"])
                    return self.QUIET
                if verb == "select":
                    self.select(targets[0])
                    return None
                if verb == "send":
                    if not text:
                        raise ValueError("no text to send")
                    for c in targets:
                        if c["key"]:
                            engine._emit("final", c["key"], c["name"], text,
                                         msg_id=f"osc#{c['key']}#{time.time_ns()}")
                    return None
                if verb in ("clear", "mark"):
                    ui(verb, "all" if ref.lower() == "all" else (targets[0]["key"] or ""))
                    return None
        if w[:1] == ["scroll"] and w[1:] in (["up"], ["down"], ["current"]):
            ui("scroll", dir=w[1])
            return None
        if w[:1] == ["font"] and w[1:] in (["increase"], ["decrease"]):
            ui("font", dir=w[1])
            return None
        if w[:1] == ["window"] and w[1:] in (["show"], ["hide"]):
            win = WINDOW.get("win")
            if win is None:
                raise ValueError("no app window to show or hide")
            win.show() if w[1] == "show" else win.hide()
            return None
        if w == ["flash"]:
            if not args:
                raise ValueError("no flash text")
            secs = float(args[1]) if len(args) > 1 and isinstance(args[1], (int, float)) else 4.0
            reps = int(args[2]) if len(args) > 2 and isinstance(args[2], (int, float)) else 1
            # optional 4th argument: a color name (red, blue, …) or #rrggbb
            color = flash_color(args[3]) if len(args) > 3 else "danger"
            engine._emit("flash", "", "", text=osclink.show(args[0]),
                         levels={"color": color, "secs": secs, "repeats": reps})
            return None
        if w[:1] == ["automation"] and len(w) >= 2:
            rules = automations.rules
            if w[1:] == ["count"]:
                R("automation/count/reply", len(rules))
                return self.QUIET
            if w[1:] == ["list"]:
                for i, r in enumerate(rules, 1):
                    n = automations.display_name(r)
                    R("automation/list/reply", i, slugify(n), n)
                return self.QUIET
            if w[1:] == ["trigger"] or (len(w) == 3 and w[2] in ("trigger", "name")):
                ref = text if w[1:] == ["trigger"] else parts[1]
                r = automations.find(ref)
                if r is None:
                    raise ValueError(f"no automation '{ref}'")
                if w[-1] == "name":
                    R(f"automation/{parts[1]}/name/reply", automations.display_name(r))
                    return self.QUIET
                if not r.get("enabled", True):
                    raise ValueError(f"automation '{ref}' is turned off")
                automations.fire(r, "", osclink.message_context(
                    "/".join([prefix] + parts), args), throttle=True)
                return None
        # ---- FraudCom extras
        if w == ["marker"]:
            add_note(text, "Marker")
            return None
        if w == ["note"]:
            if not text:
                raise ValueError("no note text")
            add_note(text, "Note")
            return None
        if w in (["mute"], ["unmute"]):
            if not args:
                raise ValueError("say which channel (index, name, or all)")
            on = w == ["mute"] and (len(args) < 2 or _truthy(args[1]))
            for c in self.resolve(osclink.show(args[0])):
                if c["key"]:
                    engine.set_mute(c["key"], on)
            return None
        if w == ["start"]:
            if not engine.running:
                if not engine.last_devices:
                    raise ValueError("press Start once in FraudCom first")
                engine.start(engine.last_devices)
            return None
        if w == ["stop"]:
            engine.stop()
            return None
        raise ValueError("unknown command")


# The app window, set by the launcher (window/show and window/hide over OSC).
WINDOW = {"win": None}


_marks = {"n": 0}


def add_note(text: str, kind: str = "Note"):
    """A typed note or a marker (M key, OSC) into the feed and the saved
    transcript. A marker without text is numbered."""
    if kind == "Marker":
        _marks["n"] += 1
        text = text or f"Marker {_marks['n']}"
    ts = time.time()
    try:
        tlog.mark(f"{kind.upper()}: {text}")
    except Exception:
        pass
    engine._emit("note", "note", kind, text=text,
                 msg_id=f"note#{int(ts * 1000)}")


engine = Engine()
engine.refiner = Refiner(engine)
automations = Automations(AUTOMATIONS_PATH)
osc_in = OSCReceiver()

if os.environ.get("FRAUDCOM_WHISPER_SELFTEST"):
    # Load the Whisper model once at launch and write the result to disk, so a
    # frozen build can be verified without live audio. Runs off-thread so it
    # never delays startup.
    def _whisper_selftest():
        res = engine.refiner.selftest()
        try:
            with open(os.path.join(DATA_DIR, "whisper_selftest.json"),
                      "w", encoding="utf-8") as f:
                json.dump(res, f)
        except Exception:
            pass
    threading.Thread(target=_whisper_selftest, daemon=True).start()


# --------------------------------------------------------------------------
class ChannelSel(BaseModel):
    index: int
    name: Optional[str] = None
    gate_db: Optional[float] = None   # mute audio below this level (dBFS)
    gate_auto: Optional[bool] = False  # adaptive: track the noise floor
    translate: Optional[str] = None   # "en" = translate this channel → English


class DeviceSel(BaseModel):
    device_index: int
    channels: List[ChannelSel]
    samplerate: Optional[int] = None
    name: Optional[str] = None       # for resolution against this
    hostapi: Optional[str] = None    # session's device numbering


class StartRequest(BaseModel):
    devices: List[DeviceSel]


class Lexicon(BaseModel):
    corrections: Dict[str, str]
    hotwords: List[str]


class PresetIn(BaseModel):
    name: str
    data: dict


class RosterIn(BaseModel):
    people: List[dict] = []
    groups: List = []
    pins: List = []


class RosterRestoreIn(BaseModel):
    id: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine.loop = asyncio.get_running_loop()
    st = load_state()
    await asyncio.to_thread(_apply_osc_rx, st)
    await asyncio.to_thread(_apply_api, st)
    yield
    osc_in.apply(False, 0)
    remote.server.apply(False, None)
    engine.stop()
    try:
        tlog.flush()          # write every pending line before the app exits
    except Exception:
        pass


app = FastAPI(lifespan=lifespan)


# Web-sharing switch, cached so the middleware doesn't hit disk per request.
SHARE = {"on": load_state().get("share", True)}
# Transcript auto-save to disk (the crash-proof record) — user-toggleable.
AUTOSAVE = {"on": load_state().get("autosave", True)}


_LOCAL_HOSTS = ("127.0.0.1", "::1", "localhost")
# Everything a LAN viewer needs — every other route is the operator's.
_VIEWER_PATHS = ("/", "/state", "/lexicon", "/qr.svg", "/view")


def _host_ok(host: str) -> bool:
    """Host must be localhost or a literal IP. A DNS name means a browser was
    pointed at a rebinding domain (evil.com resolving to 127.0.0.1), which
    would make an attacker's page same-origin with us."""
    h = (host or "").strip()
    if h.startswith("["):                       # [::1]:8765
        h = h[1:h.find("]")] if "]" in h else h[1:]
    else:
        h = h.rsplit(":", 1)[0]
    if h.lower() in _LOCAL_HOSTS:
        return True
    for fam in (socket.AF_INET, socket.AF_INET6):
        try:
            socket.inet_pton(fam, h)
            return True
        except OSError:
            continue
    return False


def _origin_ok(origin: str, host: str) -> bool:
    """A browser page may control us only if it came FROM us: its origin
    (host AND port) must equal the Host it is talking to. Any other site — or
    any other local web server on a different port of this PC — is refused.
    Non-browser clients send no Origin and pass."""
    if not origin:
        return True
    oh = origin.split("://", 1)[-1].split("/", 1)[0]
    return oh.lower() == (host or "").lower()


@app.middleware("http")
async def readonly_guard(request: Request, call_next):
    host = request.headers.get("host", "")
    if not _host_ok(host):
        return JSONResponse(status_code=403, content={
            "ok": False, "error": "bad host"})
    if request.method != "GET" and not _origin_ok(
            request.headers.get("origin"), host):
        return JSONResponse(status_code=403, content={
            "ok": False, "error": "cross-site request refused"})
    if not _is_local(request):
        # Sharing off: LAN devices get nothing at all.
        if not SHARE["on"]:
            return JSONResponse(status_code=403, content={
                "ok": False, "error": "web sharing is disabled"})
        # Sharing on: LAN devices may watch, not control — and not read
        # the operator's config (roster, presets, OSC targets, devices).
        if request.method != "GET":
            return JSONResponse(status_code=403, content={
                "ok": False, "error": "view-only from other devices"})
        p = request.url.path
        if not (p in _VIEWER_PATHS or p.startswith("/static/")):
            return JSONResponse(status_code=403, content={
                "ok": False, "error": "not available to viewers"})
    return await call_next(request)


class StateIn(BaseModel):
    autostart: Optional[bool] = None
    last_preset: Optional[str] = None
    refine: Optional[bool] = None
    share: Optional[bool] = None
    autosave: Optional[bool] = None
    osc_rx: Optional[bool] = None
    osc_rx_port: Optional[int] = None        # 0 = automatic
    osc_rx_proto: Optional[str] = None
    api_on: Optional[bool] = None            # the remote API
    api_port: Optional[int] = None           # 0 = automatic
    api_key: Optional[str] = None            # "" = no key


def _apply_osc_rx(st: dict):
    osc_in.apply(bool(st.get("osc_rx", False)),
                 st.get("osc_rx_port") or None,
                 st.get("osc_rx_proto", "both"))


def _apply_api(st: dict):
    remote.server.apply(bool(st.get("api_on", False)), st.get("api_port") or None)


@app.get("/state")
async def get_state(request: Request):
    st = load_state()
    local = _is_local(request)
    if not local:       # viewers: no settings, ports or errors (port map = attack map)
        return {"version": APP_VERSION, "share": st.get("share", True),
                "running": engine.running, "readonly": True}
    return {"version": APP_VERSION,
            "autostart": st.get("autostart", True),
            # operator-only details: event name, error text (can hold paths)
            "last_preset": st.get("last_preset") if local else None,
            "refine": st.get("refine", True),
            "refine_failed": engine.refiner.failed if engine.refiner else False,
            "refine_error": (engine.refiner.load_error
                             if (engine.refiner and local) else None),
            "share": st.get("share", True),
            "autosave": st.get("autosave", True),
            "osc_rx": st.get("osc_rx", False),
            "osc_rx_port": st.get("osc_rx_port") or 0,           # 0 = automatic
            "osc_rx_actual": osc_in.port if osc_in.server else None,
            "osc_rx_proto": st.get("osc_rx_proto", "both"),
            "osc_rx_error": osc_in.error if local else None,
            "osc_rx_note": osc_in.note if local else None,
            "api_on": st.get("api_on", False),
            "api_port": st.get("api_port") or 0,                 # 0 = automatic
            "api_actual": remote.server.port if remote.server.running else None,
            "api_key": st.get("api_key", "") if local else None,
            "api_error": remote.server.error if local else None,
            "api_note": remote.server.note if local else None,
            "muted": sorted(engine.muted) if local else [],
            "running": engine.running,
            "readonly": not local,
            "lan_url": _lan_url()}


def _busy(e):
    return JSONResponse(status_code=503, content={
        "ok": False, "error": f"Settings file is locked by another program "
                              f"({e.__class__.__name__}) — try again."})


@app.post("/state")
async def set_state(s: StateIn):
    try:
        st = load_state(strict=True)     # never overwrite settings we couldn't read
    except OSError as e:
        return _busy(e)
    if s.autostart is not None:
        st["autostart"] = s.autostart
    if s.last_preset is not None:
        st["last_preset"] = s.last_preset
    if s.refine is not None:
        st["refine"] = s.refine
        if engine.refiner:
            engine.refiner.enabled = s.refine
    if s.share is not None:
        st["share"] = s.share
        SHARE["on"] = s.share
    if s.autosave is not None:
        st["autosave"] = s.autosave
        AUTOSAVE["on"] = s.autosave
    for port, label in ((s.osc_rx_port, "OSC"), (s.api_port, "API")):
        if port and not 1024 <= port <= 65535:
            return JSONResponse(status_code=400, content={
                "ok": False, "error": f"{label} port must be 1024–65535 (0 = automatic)."})
    if s.osc_rx_port is not None:
        st["osc_rx_port"] = s.osc_rx_port or None
    if s.osc_rx is not None:
        st["osc_rx"] = s.osc_rx
    if s.osc_rx_proto is not None:
        if s.osc_rx_proto not in ("udp", "tcp", "both"):
            return JSONResponse(status_code=400, content={
                "ok": False, "error": "OSC transport must be udp, tcp or both."})
        st["osc_rx_proto"] = s.osc_rx_proto
    if s.api_on is not None:
        st["api_on"] = s.api_on
    if s.api_port is not None:
        st["api_port"] = s.api_port or None
    if s.api_key is not None:
        st["api_key"] = s.api_key.strip()
    save_state(st)
    out = {"ok": True}
    if (s.osc_rx, s.osc_rx_port, s.osc_rx_proto) != (None, None, None):
        await asyncio.to_thread(_apply_osc_rx, st)
        out["osc_rx_error"] = osc_in.error
    if (s.api_on, s.api_port) != (None, None):
        await asyncio.to_thread(_apply_api, st)
        out["api_error"] = remote.server.error
    return out


@app.get("/qr.svg")
async def qr_svg(tab: str = ""):
    url = _lan_url()
    if not url:
        return Response(status_code=404)
    if tab:                                  # a link that opens on one group
        if tab not in (load_roster().get("groups") or []):
            return Response(status_code=404)     # only real groups: no giant QRs
        url += "/?tab=" + urllib.parse.quote(tab)
    return Response(content=await asyncio.to_thread(_qr_svg, url),
                    media_type="image/svg+xml")


def _qr_svg(url: str) -> bytes:
    import io
    import qrcode
    import qrcode.image.svg
    img = qrcode.make(url, image_factory=qrcode.image.svg.SvgPathImage)
    buf = io.BytesIO()
    img.save(buf)
    return buf.getvalue()


@app.get("/")
async def index():
    # no-cache: the UI is baked into each build — a cached page after an
    # update means the window shows an old UI against a new server.
    return FileResponse(os.path.join(STATIC_DIR, "index.html"),
                        headers={"Cache-Control": "no-cache"})


_LOOPBACK_RX = re.compile(
    r"loop-?back|stereo mix|what u hear|wave out|virtual", re.IGNORECASE)

# macOS virtual inputs that belong to other apps (meeting software), not to
# anything a vocal can be patched into. BlackHole/Loopback stay: on a Mac
# those are deliberate routing tools.
_MAC_PSEUDO_RX = re.compile(
    r"zoomaudiodevice|microsoft teams audio|webex|ms teams", re.IGNORECASE)


def _curate_devices(devs):
    """Only endpoints that can actually receive a vocal input.

    - ASIO + WASAPI covers every real device without the MME/DirectSound/
      WDM-KS duplicates and Sound Mapper / PC Speaker pseudo-devices.
    - Loopback endpoints (an interface recording its own output) are not
      inputs.
    - Interfaces expose channel-subset endpoints of the same physical jacks
      ("Mic|Line 2" mono vs "Mic|Line 1/2" stereo): keep the widest one per
      physical device — equal-width siblings (e.g. "DVS Receive 1-2"/"3-4")
      all stay.
    """
    # macOS: Core Audio is the only host API and has no subset/duplicate
    # endpoints; Dante Virtual Soundcard is one ordinary multichannel device.
    if any(d["hostapi"] == "Core Audio" for d in devs):
        return [d for d in devs if d["hostapi"] == "Core Audio"
                and not _MAC_PSEUDO_RX.search(d["name"])]
    # The pseudo-device name filter applies ONLY to Windows endpoints:
    # ASIO devices are always real (and "Dante Virtual Soundcard" would
    # otherwise be eaten by the word "virtual").
    devs = [d for d in devs if d["hostapi"] == "ASIO"
            or (d["hostapi"] == "Windows WASAPI"
                and not _LOOPBACK_RX.search(d["name"]))]

    def hw_group(d):
        m = re.search(r"\(([^)]+)\)\s*$", d["name"])
        return (d["hostapi"], m.group(1) if m else d["name"])

    widest: Dict = {}
    for d in devs:
        if d["hostapi"] != "ASIO":
            g = hw_group(d)
            widest[g] = max(widest.get(g, 0), d["max_input_channels"])
    return [d for d in devs
            if d["hostapi"] == "ASIO"
            or d["max_input_channels"] >= widest[hw_group(d)]]


_dev_cache = {"t": 0.0, "v": None}
_dev_lock = threading.Lock()


def _fresh_devices():
    """Enumerate devices in a throwaway subprocess. NEVER re-init PortAudio
    in this process: unloading Dante ASIO drivers tears down process-wide
    Winsock (WSACleanup) and kills the web server's sockets."""
    with _dev_lock:
        if time.time() - _dev_cache["t"] < 3 and _dev_cache["v"] is not None:
            return _dev_cache["v"]
        if getattr(sys, "frozen", False):
            cmd = [sys.executable, "--list-devices"]
        else:
            cmd = [sys.executable,
                   os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "launcher.py"), "--list-devices"]
        try:
            flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            r = subprocess.run(cmd, capture_output=True, text=True,
                               timeout=30, creationflags=flags)
            devs = json.loads(r.stdout.strip().splitlines()[-1])
            _dev_cache["t"] = time.time()
            _dev_cache["v"] = devs
            return devs
        except Exception:
            return None


@app.get("/devices")
async def devices(all: int = 0):
    devs = None
    if not engine.running:
        devs = await asyncio.to_thread(_fresh_devices)
    if devs is None:
        devs = engine.list_devices()   # this session's stable view
    return devs if all else _curate_devices(devs)


@app.get("/lexicon")
async def get_lexicon(request: Request):
    if not _is_local(request):           # corrections can hold names / cues
        return {"hotwords": engine.lexicon.get("hotwords", []), "corrections": {}}
    return engine.lexicon


@app.post("/lexicon")
async def set_lexicon(lex: Lexicon):
    engine.lexicon = lex.model_dump()
    engine.corrector.update(engine.lexicon["corrections"])
    save_lexicon(engine.lexicon)
    return {"ok": True}


@app.get("/presets")
async def get_presets():
    return load_presets()


@app.post("/presets")
async def save_preset(p: PresetIn):
    try:
        presets = load_presets(strict=True)   # a locked file must not become {}
    except OSError as e:
        return _busy(e)
    presets[p.name] = p.data
    save_presets(presets)
    return {"ok": True}


@app.delete("/presets/{name:path}")    # names with "/" (dates) must be deletable
async def delete_preset(name: str):
    try:
        presets = load_presets(strict=True)
    except OSError as e:
        return _busy(e)
    presets.pop(name, None)
    save_presets(presets)
    return {"ok": True}


@app.get("/roster")
async def get_roster():
    """The server-side safety copy of the roster (people/groups/pins)."""
    return load_roster()


@app.post("/roster")
async def post_roster(r: RosterIn):
    """Mirror the live roster to disk. The UI calls this on every change."""
    saved = await asyncio.to_thread(
        save_roster, {"people": r.people, "groups": r.groups, "pins": r.pins})
    return {"ok": True, "saved": saved.get("saved")}


@app.get("/view")
async def view():
    """What a LAN viewer needs to show the operator's tabs: each channel's
    speaker name, group and color. Never devices, presets or OSC targets."""
    r = load_roster()
    chans = {}
    for p in r.get("people", []):
        if isinstance(p, dict) and p.get("di") is not None \
                and p.get("ch") is not None:
            chans[f"{p['di']}:{p['ch']}"] = {
                k: p.get(k) for k in ("name", "group", "color", "hex", "icon")}
            chans[f"{p['di']}:{p['ch']}"]["groups"] = person_groups(p)
    return {"groups": [g for g in r.get("groups", []) if isinstance(g, str)],
            "pins": [g for g in r.get("pins", []) if isinstance(g, str)],
            "channels": chans}


class SelectedIn(BaseModel):
    key: Optional[str] = None


@app.post("/selected")
async def selected(s: SelectedIn):
    """The UI soloed a channel: OSC's channel/selected/... now means it."""
    osc_in.selected = s.key
    return {"ok": True}


class MuteIn(BaseModel):
    key: str
    on: bool


@app.post("/mute")
async def mute(m: MuteIn):
    await asyncio.to_thread(engine.set_mute, m.key, m.on)
    return {"ok": True}


@app.get("/roster/backups")
async def get_roster_backups():
    return {"backups": await asyncio.to_thread(list_roster_backups)}


@app.post("/roster/restore")
async def restore_roster(r: RosterRestoreIn):
    d = await asyncio.to_thread(read_roster_backup, r.id)
    if d is None:
        return JSONResponse(status_code=404,
                            content={"ok": False, "error": "No such backup."})
    return {"ok": True, "people": d.get("people", []),
            "groups": d.get("groups", []), "pins": d.get("pins", [])}


@app.post("/start")
async def start(req: StartRequest):
    # Run in a thread with a timeout: opening an ASIO/driver stream (or the
    # first Vosk model load) must never block or wedge the event loop.
    try:
        await asyncio.wait_for(
            asyncio.to_thread(engine.start,
                              [d.model_dump() for d in req.devices]),
            timeout=30)
        return {"ok": True}
    except asyncio.TimeoutError:
        return JSONResponse(status_code=500, content={
            "ok": False,
            "error": "Audio device didn't respond within 30 s (driver hang?). "
                     "Check the DVS control panel, then try again."})
    except Exception as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})


@app.post("/stop")
async def stop():
    # Bounded like /start: a hung audio driver mid-restart must not leave the
    # Stop button spinning forever.
    try:
        await asyncio.wait_for(asyncio.to_thread(engine.stop), timeout=10)
        return {"ok": True}
    except asyncio.TimeoutError:
        return JSONResponse(status_code=500, content={
            "ok": False,
            "error": "Stop didn't finish within 10 s (audio driver busy). "
                     "Try again, or close FraudCom."})


class AutomationsIn(BaseModel):
    rules: List[dict]


class OSCTest(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000
    address: str = "/test"
    args: str = ""
    proto: str = "udp"


class MIDITest(BaseModel):
    midi_port: str = ""
    midi_type: str = "note"
    midi_channel: int = 1
    midi_num: int = 60
    midi_val: Optional[int] = 127
    midi_raw: str = ""


class NoteIn(BaseModel):
    text: str = ""
    marker: bool = False


@app.get("/automations")
async def get_automations():
    return automations.rules


@app.post("/automations")
async def set_automations(a: AutomationsIn):
    for r in a.rules:
        if r.get("trigger") == "osc":
            try:
                osclink.check_pattern((r.get("osc_pattern") or "").strip())
            except ValueError as e:
                return JSONResponse(status_code=400, content={
                    "ok": False, "error": f"{automations.display_name(r)}: {e}"})
    automations.save(a.rules)
    return {"ok": True}


@app.post("/automations/test")
async def test_osc(t: OSCTest):
    try:
        # off the event loop: a slow DNS lookup must not freeze the feed
        # templates have no trigger to fill them in a test: sent as written
        await asyncio.to_thread(automations.bus.send, t.host, t.port,
                                t.address, osclink.parse_args(t.args),
                                "tcp" if t.proto == "tcp" else "udp")
        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})


@app.get("/midi/ports")
async def midi_ports():
    return await asyncio.to_thread(automations.midi.ports)


@app.post("/automations/midi-test")
async def test_midi(t: MIDITest):
    try:
        await asyncio.to_thread(automations.midi.send, t.model_dump())
        return {"ok": True, "sent": _midi_describe(t.model_dump())}
    except Exception as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})


# ------------------------------------------------------------ setup templates
# Import: a .pcconfig show file (JSON: channels that are either an input on a
# local audio device or a group listing member ids, with colors and keywords)
# or a FraudCom setup template. Export: a FraudCom setup template (.fctemplate,
# JSON) with the inputs, groups, hot words, corrections and, optionally, the
# automations. Parsing happens here; the window shows a summary, matches the
# devices on THIS computer and applies it like a preset.
TEMPLATES_DIR = os.path.join(DATA_DIR, "templates")
TEMPLATE_FORMAT = "fraudcom-template"
TEMPLATE_MAX_BYTES = 4 * 1024 * 1024
_TPL_PERSON_KEYS = ("id", "name", "devName", "devApi", "ch", "groups", "color",
                    "hex", "icon", "gate", "translate", "muted")


def _tpl_str(v, n=120):
    return v.strip()[:n] if isinstance(v, str) else ""


def _tpl_hex(c):
    """.pcconfig color {red, green, blue, alpha} (0-1, missing = 0) -> #rrggbb."""
    if not isinstance(c, dict):
        return None
    out = []
    for k in ("red", "green", "blue"):
        try:
            v = float(c.get(k) or 0)
        except (TypeError, ValueError):
            v = 0.0
        out.append(max(0, min(255, round(v * 255))))
    return "#%02x%02x%02x" % tuple(out)


def _tpl_id(x):
    if isinstance(x, dict):
        x = x.get("string")
    return x if isinstance(x, str) else None


def _tpl_words(kws, into):
    for k in kws if isinstance(kws, list) else []:
        if isinstance(k, dict) and k.get("shouldHighlight", True):
            w = _tpl_str(k.get("text"), 60)
            if w and w.lower() not in (x.lower() for x in into):
                into.append(w)


def parse_pcconfig(d: dict) -> dict:
    chans = [c for c in (d.get("channels") or []) if isinstance(c, dict)]
    people, groups, hot, by_id, skipped = [], [], [], {}, 0
    for c in chans:
        if isinstance(c.get("group"), dict):
            continue
        ch = c.get("channel")
        loc = ch.get("local") if isinstance(ch, dict) else None
        if not isinstance(loc, dict):
            skipped += 1                      # not a local audio input
            continue
        dev = loc.get("audioDevice") if isinstance(loc.get("audioDevice"), dict) else {}
        ic = loc.get("inputChannel") if isinstance(loc.get("inputChannel"), dict) else {}
        try:
            idx = int(ic.get("index") or 0)   # 0-based; {} means channel 0
        except (TypeError, ValueError):
            idx = 0
        p = {"id": "p" + os.urandom(5).hex(), "name": _tpl_str(c.get("name"), 80) or "Input",
             "devName": _tpl_str(dev.get("name"), 120) or None, "devApi": None,
             "di": None, "ch": max(0, min(255, idx)), "groups": [], "group": "",
             "color": len(people) % 12}
        hx = _tpl_hex(c.get("color"))
        if hx:
            p["hex"] = hx
        people.append(p)
        if _tpl_id(c.get("id")):
            by_id[_tpl_id(c["id"])] = p
        _tpl_words(c.get("keywords"), hot)
    for c in chans:
        g = c.get("group")
        if not isinstance(g, dict):
            continue
        name = _tpl_str(c.get("name"), 60)
        if not name or name in groups:
            continue
        groups.append(name)
        for mid in g.get("channels") or []:
            p = by_id.get(_tpl_id(mid))
            if p is not None and name not in p["groups"]:
                set_person_groups(p, p["groups"] + [name])
        _tpl_words(c.get("keywords"), hot)
    notes = []
    if skipped:
        notes.append(f"{skipped} channel(s) that aren't local audio inputs were skipped.")
    if hot:
        notes.append("Keyword highlight colors aren't kept: FraudCom highlights every "
                     "hot word the same way, on every channel.")
    return {"kind": "pcconfig", "name": _tpl_str(d.get("name"), 80) or "Imported",
            "people": people, "groups": groups, "pins": [], "hotwords": hot,
            "corrections": None, "automations": None, "notes": notes}


def parse_fraudcom_template(d: dict) -> dict:
    people = []
    for x in d.get("people") or []:
        if not isinstance(x, dict):
            continue
        p = {k: x[k] for k in _TPL_PERSON_KEYS if k in x}
        p["name"] = _tpl_str(p.get("name"), 80) or "Input"
        p["id"] = _tpl_str(p.get("id"), 40) or "p" + os.urandom(5).hex()
        try:
            p["ch"] = max(0, min(255, int(p.get("ch") or 0)))
        except (TypeError, ValueError):
            p["ch"] = 0
        p["di"] = None                         # resolved on this computer
        set_person_groups(p, person_groups(x))
        people.append(p)
    groups = [g for g in d.get("groups") or [] if isinstance(g, str) and g][:200]
    pins = [g for g in d.get("pins") or [] if isinstance(g, str) and g in groups]
    hot = [w for w in d.get("hotwords") or [] if isinstance(w, str) and w.strip()][:500]
    corr = d.get("corrections")
    corr = ({str(k): str(v) for k, v in corr.items()} if isinstance(corr, dict) else None)
    autos = d.get("automations")
    autos = [r for r in autos if isinstance(r, dict)] if isinstance(autos, list) else None
    if autos is not None:
        for r in autos:
            if r.get("trigger") == "osc":
                osclink.check_pattern((r.get("osc_pattern") or "").strip())
    return {"kind": "fraudcom", "name": _tpl_str(d.get("name"), 80) or "Template",
            "people": people[:512], "groups": groups, "pins": pins, "hotwords": hot,
            "corrections": corr, "automations": autos, "notes": []}


def parse_template(text: str) -> dict:
    try:
        d = json.loads(text)
    except ValueError:
        raise ValueError("That file isn't a setup template (not JSON).")
    if not isinstance(d, dict):
        raise ValueError("That file isn't a setup template.")
    if d.get("format") == TEMPLATE_FORMAT:
        return parse_fraudcom_template(d)
    if isinstance(d.get("channels"), list):
        return parse_pcconfig(d)
    raise ValueError("Unrecognized file: expected a FraudCom template or a .pcconfig file.")


def build_template(name, people, groups, pins, include_automations) -> dict:
    lex = load_lexicon()
    out = []
    for x in people or []:
        if isinstance(x, dict):
            p = {k: x[k] for k in _TPL_PERSON_KEYS if k in x}
            set_person_groups(p, person_groups(x))
            out.append(p)
    t = {"format": TEMPLATE_FORMAT, "version": 1, "app": APP_VERSION,
         "name": _tpl_str(name, 80) or "FraudCom setup",
         "created": time.strftime("%Y-%m-%dT%H:%M:%S"),
         "people": out, "groups": [g for g in groups or [] if isinstance(g, str)],
         "pins": [g for g in pins or [] if isinstance(g, str)],
         "hotwords": lex.get("hotwords") or [], "corrections": lex.get("corrections") or {}}
    if include_automations:
        t["automations"] = automations.rules
    return t


def _safe_filename(name: str) -> str:
    s = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', " ", name or "")
    s = re.sub(r"\s+", " ", s).strip(" .")
    return (s or "FraudCom setup")[:80]


class TemplateParseIn(BaseModel):
    text: str


class TemplateExportIn(BaseModel):
    name: str = ""
    people: List[dict] = []
    groups: List = []
    pins: List = []
    automations: bool = False


@app.post("/template/parse")
async def template_parse(t: TemplateParseIn):
    if len(t.text.encode("utf-8", "ignore")) > TEMPLATE_MAX_BYTES:
        return JSONResponse(status_code=400, content={"ok": False, "error": "That file is too large."})
    try:
        return {"ok": True, "template": parse_template(t.text)}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})


def _save_dialog(filename: str):
    """Native Save dialog in the app window; None if there's no window
    (browser fallback) or the dialog isn't available. Returns "" if the
    user cancelled."""
    win = WINDOW.get("win")
    if win is None:
        return None
    try:
        import webview
        kind = getattr(getattr(webview, "FileDialog", None), "SAVE", None) or webview.SAVE_DIALOG
        # A type filter only on Windows: the Mac dialog turns filters into
        # Apple file types, and an unknown extension like .fctemplate can end
        # up refused. The ".fctemplate" ending is added below either way.
        kw = ({} if sys.platform == "darwin" else
              {"file_types": ("FraudCom template (*.fctemplate)", "All files (*.*)")})
        res = win.create_file_dialog(kind, save_filename=filename, **kw)
    except Exception:
        return None
    if not res:
        return ""
    return res if isinstance(res, str) else res[0]


@app.post("/template/export")
async def template_export(t: TemplateExportIn):
    data = build_template(t.name, t.people, t.groups, t.pins, t.automations)
    fname = _safe_filename(data["name"]) + ".fctemplate"
    path = await asyncio.to_thread(_save_dialog, fname)
    if path == "":
        return {"ok": False, "cancelled": True}
    dialog = path is not None
    if path is None:                            # no window: the templates folder
        os.makedirs(TEMPLATES_DIR, exist_ok=True)
        path = os.path.join(TEMPLATES_DIR, fname)
    if not path.lower().endswith(".fctemplate"):
        path += ".fctemplate"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except OSError as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
    return {"ok": True, "path": path, "dialog": dialog}


@app.post("/template/reveal")
async def template_reveal():
    """Show the templates folder (used when there was no Save dialog)."""
    os.makedirs(TEMPLATES_DIR, exist_ok=True)
    try:
        if sys.platform == "darwin":
            subprocess.run(["open", TEMPLATES_DIR], check=True)
        else:
            os.startfile(TEMPLATES_DIR)
        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})


@app.post("/open-transcripts")
async def open_transcripts():
    os.makedirs(TRANSCRIPTS_DIR, exist_ok=True)
    try:
        if sys.platform == "darwin":
            subprocess.run(["open", TRANSCRIPTS_DIR], check=True)
        else:
            os.startfile(TRANSCRIPTS_DIR)
        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})


@app.post("/note")
async def post_note(n: NoteIn):
    text = (n.text or "").strip()
    if not text and not n.marker:
        return {"ok": False}
    await asyncio.to_thread(add_note, text, "Marker" if n.marker else "Note")
    return {"ok": True}


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    viewer = (websocket.client is None
              or websocket.client.host not in ("127.0.0.1", "::1"))
    # WebSockets are exempt from the browser's same-origin policy: without
    # this, any web page open on the show PC could read the live feed.
    host = websocket.headers.get("host", "")
    if not _host_ok(host) or not _origin_ok(websocket.headers.get("origin"), host):
        await websocket.close(code=4403)
        return
    if viewer and (not SHARE["on"] or VIEWERS["n"] >= MAX_VIEWERS):
        await websocket.close(code=4403)
        return
    await websocket.accept()
    sub: asyncio.Queue = asyncio.Queue(maxsize=500)
    engine.subscribers.append(sub)
    VIEWERS["n"] += viewer                  # reported by the API's /network
    try:
        while True:
            msg = await sub.get()
            if viewer and not SHARE["on"]:       # sharing switched off
                await websocket.close(code=4403)  # mid-session: drop the
                return                            # viewer, don't keep streaming
            # Viewers get the conversation, not the operator's plumbing:
            # automation notices carry OSC target host:port, warnings carry
            # device names and error text (which can include paths), and
            # health / mute / OSC-in are operator controls.
            if viewer and msg.get("type") in (
                    "automation", "automations", "warn", "health", "mute",
                    "oscin", "ui"):
                continue
            if viewer and msg.get("type") == "flash":
                msg = {**msg, "name": ""}        # the automation's own name
            await websocket.send_json(msg)
    except WebSocketDisconnect:
        pass
    finally:
        VIEWERS["n"] -= viewer
        if sub in engine.subscribers:
            engine.subscribers.remove(sub)


VIEWERS = {"n": 0}          # LAN devices watching the feed right now
MAX_VIEWERS = 50            # each one gets every level/transcript event

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# The remote API: its own server on port 24480 (off by default;
# Preferences → Remote API). Built last: it reads this module.
import remote_api  # noqa: E402
remote = remote_api.build(sys.modules[__name__])

if __name__ == "__main__":
    print(f"\n  FraudCom v{APP_VERSION} — multichannel voice transcriber"
          f"\n  Open http://{HOST}:{PORT}\n")
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
