"""FraudCom's remote API (HTTP + WebSocket + Server-Sent Events).

A REST API under /api/v1 for show-control software: status, channels, groups,
keywords, automations (including webhook triggers), configurations, the live
transcript (paginated, streamed over SSE with replay, and over WebSocket),
window / font / scroll / flash control, and OSC. Responses use a {data, meta}
envelope and errors an {error: {code, message}, meta} one.

Served on its own port (24480) and OFF by default. When on, other computers
may control FraudCom through it — that is its purpose — so an optional API
key (Bearer token) is offered, and browser cross-site and DNS-rebinding
requests are refused as everywhere else in FraudCom.

How FraudCom's model maps onto the API:
  channel        = an input (the roster)       group     = a group
  keyword        = a hot word (shouldHighlight) and/or a jargon correction
                   (replacementText); FraudCom's keywords apply to every channel
  configuration  = a preset                    automation = an automation
  transcript     = the live feed (kept in memory: 3000 entries)
IDs are UUIDs derived from FraudCom's own ids; every {id} also accepts a name
(case-insensitive). MIDI: /midi/devices lists the MIDI outputs (on a Mac
also FraudCom's own virtual port, id "FraudCom"); /midi/send and the "midi"
action take a SendMIDIRequest. MIDI triggers (MIDI in) are not supported.
"""

import asyncio
import base64
import hmac
import json
import socket
import sys
import threading
import time
import urllib.request
import uuid
from collections import OrderedDict, deque
from datetime import datetime, timezone

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

import osclink

DEFAULT_PORT = 24480
FALLBACK_PORT = 24481
_NS = uuid.UUID("3c6b0a52-8f4e-4d1b-9a7c-52e1f0d4b6a9")
_STARTED = time.time()
# FraudCom's channel palette (dark theme), for a channel without a custom color
_PALETTE = ["#ffb020", "#39c6ff", "#ff6ec7", "#39d98a", "#b28bff", "#ff8a3d",
            "#5cd6d6", "#f2e14c", "#8fd14f", "#ff9db0", "#7aa7ff", "#d6b25c"]
_ENTRIES_MAX = 3000            # transcript entries kept (and snapshot limit)
_REPLAY_MAX, _REPLAY_BYTES = 512, 2 * 1024 * 1024   # SSE replay window


def uid(kind: str, key) -> str:
    return str(uuid.uuid5(_NS, f"{kind}:{key}"))


def iso(ts: float = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), timezone.utc) \
        .isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _meta(**extra):
    return {"timestamp": iso(), "requestId": uuid.uuid4().hex, **extra}


def ok(data, status=200, headers=None):
    return JSONResponse({"data": data, "meta": _meta()}, status_code=status,
                        headers=headers)


def page(items, limit, offset):
    lim = max(1, min(200, int(limit or 50)))
    off = max(0, int(offset or 0))
    return JSONResponse({"data": items[off:off + lim], "meta": _meta(
        totalCount=len(items), hasMore=off + lim < len(items))})


def fail(status, code, message, headers=None):
    return JSONResponse({"error": {"code": code, "message": message},
                         "meta": _meta()}, status_code=status, headers=headers)


class ApiError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


MAX_ACTIONS = 32
MAX_DELAY = 600.0                       # s


def num(x, default, lo, hi):
    """A finite number clamped to [lo, hi]; anything else -> default."""
    try:
        f = float(x if x is not None else default)
    except (TypeError, ValueError):
        return default
    return min(max(f, lo), hi) if f == f and abs(f) != float("inf") else default


def not_found(what):
    return ApiError(404, "NOT_FOUND", f"No {what} with that id or name.")


MAX_BODY = 1 << 20                      # 1 MB: nothing in this API needs more
MAX_TEXT = 4000                         # characters in one sent message


def _no_constants(c):
    raise ValueError(f"{c} is not allowed")


async def jbody(request: Request, required=True) -> dict:
    raw = await request.body()
    if len(raw) > MAX_BODY:
        raise ApiError(413, "BAD_REQUEST", "The body is too large.")
    if not raw.strip():
        if required:
            raise ApiError(400, "BAD_REQUEST", "A JSON body is required.")
        return {}
    try:
        d = json.loads(raw, parse_constant=_no_constants)
    except ValueError:
        raise ApiError(400, "BAD_REQUEST", "The body is not valid JSON.")
    if not isinstance(d, dict):
        raise ApiError(400, "VALIDATION_ERROR", "The body must be a JSON object.")
    return d


def find(items, ref, idf, namef, what):
    """-> (item, ambiguous). The UUID first, then the name (any case)."""
    r = (ref or "").strip()
    for it in items:
        if idf(it) == r:
            return it, False
    hits = [it for it in items if namef(it).strip().lower() == r.lower()]
    if hits:
        return hits[0], len(hits) > 1
    raise not_found(what)


def amb(flag):
    return {"X-Ambiguous-Name": "true"} if flag else None


# ---------------------------------------------------------------- the server
class ApiServer:
    """Runs the API app on its own uvicorn server in a thread."""

    def __init__(self, asgi, host):
        self.asgi, self.host = asgi, host
        self.server = self.thread = None
        self.port = self.error = self.note = None
        self._want = None
        self._lock = threading.Lock()

    @property
    def running(self):
        return bool(self.server and self.server.started
                    and self.thread and self.thread.is_alive())

    def _stop(self):
        if self.server:
            self.server.should_exit = True
            if self.thread:
                self.thread.join(timeout=5)
        self.server = self.thread = None

    def apply(self, on: bool, port):
        """port None = automatic: 24480, else the fallback."""
        import uvicorn
        with self._lock:
            if self.server and (not on or port != self._want or not self.running):
                self._stop()
            self.error = self.note = None
            if not on or self.server:
                return
            tries = [port] if port else [DEFAULT_PORT, FALLBACK_PORT]
            for p in tries:
                probe = socket.socket()
                try:
                    probe.bind((self.host, p))
                except OSError as e:
                    self.error = f"Can't listen on port {p} ({e.strerror or e})."
                    continue
                finally:
                    probe.close()
                srv = uvicorn.Server(uvicorn.Config(
                    self.asgi, host=self.host, port=p, log_level="warning"))
                t = threading.Thread(target=srv.run, daemon=True)
                t.start()
                end = time.time() + 8
                while time.time() < end and t.is_alive() and not srv.started:
                    time.sleep(0.05)
                if srv.started:
                    self.server, self.thread, self.port = srv, t, p
                    self._want, self.error = port, None
                    if p != tries[0]:
                        self.note = (f"Port {tries[0]} is in use by another app, "
                                     f"so the API is on {p}.")
                    return
                srv.should_exit = True
                self.error = f"The API server didn't start on port {p}."


# ---------------------------------------------------------------- live state
class Hub:
    """Transcript entries, unread counts, activity, and the live streams."""

    def __init__(self, v):
        self.v = v
        self.lock = threading.RLock()
        self.entries = OrderedDict()          # entry id -> entry
        self.mid2id = {}                      # FraudCom message id -> entry id
        self.partial = {}                     # channel key -> in-progress entry id
        self.unread = {}                      # channel key -> count
        self.activity = deque(maxlen=500)
        self.events = deque()                 # (seq, name, payload_json, channelId)
        self.event_bytes = 0
        self.seq = 0
        self.epoch = uuid.uuid4().hex[:8]
        self.loop = None
        self.sse = []                         # (queue, channelId or None)
        self.ws = []                          # (queue, set of categories or None)
        self._chan_cache = (None, {})

    # -- channel key <-> channel uuid, from the roster (cached by file time)
    def chan_map(self):
        import os
        try:
            mt = os.path.getmtime(self.v.ROSTER_PATH)
        except OSError:
            mt = None
        if self._chan_cache[0] != mt or mt is None:
            m = {}
            for p in self.v.load_roster().get("people", []):
                if isinstance(p, dict):
                    cid = uid("channel", p.get("id") or p.get("name"))
                    name = (p.get("name") or "").strip() or "Speaker"
                    if p.get("di") is not None and p.get("ch") is not None:
                        m[f"{p['di']}:{p['ch']}"] = (cid, name)
                    m[f"typed:{p.get('id')}"] = (cid, name)
            self._chan_cache = (mt, m)
        return self._chan_cache[1]

    def cid(self, key, name=""):
        hit = self.chan_map().get(key or "")
        return hit if hit else (uid("channel", key or name), name or key)

    # -- events from the engine
    def on_emit(self, m: dict):
        t = m.get("type")
        if t in ("partial", "final", "refine", "retract"):
            self._transcript(m)
        elif t == "automation":
            self.log("automation", "outbound", f"{m.get('name')}: {m.get('text')}")
            self.broadcast("automation", "automation.triggered",
                           {"name": m.get("name"), "detail": m.get("text")})
        elif t == "oscin":
            self.log("osc", "inbound", m.get("text") or "")
        elif t == "status":
            self.log("status", "internal", m.get("text") or "")
            self.broadcast("status", "status.changed", {"listening": m.get("text") == "listening"})
        elif t == "ui" and m.get("action") == "select":
            cid, name = self.cid(m.get("channel"), m.get("name"))
            with self.lock:
                self.unread[m.get("channel")] = 0
            self.broadcast("channel", "channel.selected", {"id": cid, "name": name})
        elif t == "roster":
            self.broadcast("channel", "channel.changed", {})

    def _transcript(self, m):
        t, key, now = m["type"], m.get("channel") or "", m.get("ts") or time.time()
        with self.lock:
            if t == "partial":
                eid = self.partial.get(key)
                e = self.entries.get(eid) if eid else None
                if e is None:
                    eid = str(uuid.uuid4())
                    self.partial[key] = eid
                    e = self._new(eid, key, m.get("name"), now, "audio")
                e.update(text=m.get("text") or "", inProgress=True)
                name = "transcript.updated"
            elif t == "final":
                mid = m.get("id") or str(uuid.uuid4())
                eid = self.partial.pop(key, None) or str(uuid.uuid4())
                self.mid2id[mid] = eid
                while len(self.mid2id) > 10000:       # oldest first
                    self.mid2id.pop(next(iter(self.mid2id)))
                e = self.entries.get(eid) or self._new(
                    eid, key, m.get("name"), now,
                    "typed" if mid.startswith(("osc#", "api#")) else "audio")
                e.update(text=m.get("text") or "", inProgress=False, completeDate=iso(now))
                self.unread[key] = self.unread.get(key, 0) + 1
                name = "transcript.completed"
            elif t == "refine":
                eid = self.mid2id.get(m.get("id"))
                e = self.entries.get(eid) if eid else None
                if e is None:
                    return
                e["text"] = m.get("text") or ""
                if m.get("translated"):
                    e["translatedText"] = e["text"]
                name = "transcript.updated"
            else:                                       # retract
                eid = self.mid2id.pop(m.get("id"), None)
                e = self.entries.pop(eid, None) if eid else None
                if e is None:
                    return
                self.publish("transcript.deleted", {"id": eid}, e["channelId"])
                return
            e["revision"] = e.get("revision", 0) + 1
            self.entries.move_to_end(eid)
            while len(self.entries) > _ENTRIES_MAX:
                self.entries.popitem(last=False)
            self.publish(name, dict(e), e["channelId"])

    def _new(self, eid, key, name, ts, source):
        cid, cname = self.cid(key, name)
        e = {"id": eid, "channelId": cid, "channelName": name or cname, "text": "",
             "source": source, "inProgress": True, "hasBeenSeen": False,
             "date": iso(ts), "completeDate": None, "seenDate": None,
             "translatedText": None, "triggeredAutomations": [], "revision": 0}
        self.entries[eid] = e
        return e

    def mark_read(self, keys=None):
        """keys None = every channel."""
        cmap = self.chan_map()
        cids = None if keys is None else {cmap.get(k, (None,))[0] for k in keys}
        with self.lock:
            for e in self.entries.values():
                if (cids is None or e["channelId"] in cids) and not e["hasBeenSeen"]:
                    e["hasBeenSeen"], e["seenDate"] = True, iso()
            for k in list(self.unread):
                if keys is None or k in keys:
                    self.unread[k] = 0

    def clear(self, keys=None):
        cmap = self.chan_map()
        cids = None if keys is None else {cmap.get(k, (None,))[0] for k in keys}
        with self.lock:
            for eid in [i for i, e in self.entries.items()
                        if cids is None or e["channelId"] in cids]:
                self.entries.pop(eid, None)
            # clearing transcripts also discards the replay buffer
            self.events.clear()
            self.event_bytes = 0

    def log(self, category, direction, summary):
        self.activity.appendleft({"id": str(uuid.uuid4()), "timestamp": iso(),
                                  "category": category, "direction": direction,
                                  "summary": summary})

    # -- delivery to SSE + WebSocket listeners
    def publish(self, name, data, channel_id):
        payload = json.dumps(data)
        with self.lock:
            self.seq += 1
            ev = (self.seq, name, payload, channel_id)
            self.events.append(ev)
            self.event_bytes += len(payload)
            while self.events and (len(self.events) > _REPLAY_MAX
                                   or self.event_bytes > _REPLAY_BYTES):
                self.event_bytes -= len(self.events.popleft()[2])
            sse = list(self.sse)
        for q, chan in sse:
            if chan is None or chan == channel_id:
                self._put(q, ev)
        self.broadcast("transcript", name, data)

    def broadcast(self, category, name, data):
        frame = {"type": category, "event": name, "data": data}
        for q, cats in list(self.ws):
            if cats is None or category in cats:
                self._put(q, frame)

    def _put(self, q, item):
        if self.loop is None:
            return

        def put():
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                pass
        self.loop.call_soon_threadsafe(put)

    def event_id(self, seq):
        return f"{self.epoch}-{seq}"


# ---------------------------------------------------------------- the API
def build(v):
    """v = the app module. Returns a handle with .server and .hub."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(_app):
        hub.loop = asyncio.get_running_loop()
        yield
        hub.loop = None

    hub = Hub(v)
    api = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    v.EMIT_HOOKS.append(hub.on_emit)
    srv = ApiServer(api, v.HOST)

    # -- errors in the API's {error, meta} shape
    @api.exception_handler(ApiError)
    async def _api_error(request, e: ApiError):
        return fail(e.status, e.code, e.message)

    @api.exception_handler(RequestValidationError)
    async def _validation(request, e):
        return fail(400, "VALIDATION_ERROR", "The request is not valid.")

    @api.exception_handler(StarletteHTTPException)
    async def _http(request, e):
        if e.status_code == 405:
            return fail(405, "METHOD_NOT_ALLOWED", "That method isn't allowed here.")
        if e.status_code == 404:
            return fail(404, "NOT_FOUND", "No such endpoint.")
        return fail(e.status_code, "BAD_REQUEST", str(e.detail))

    @api.exception_handler(Exception)
    async def _internal(request, e):
        return fail(500, "INTERNAL_ERROR", "Something went wrong inside FraudCom.")

    def key_ok(headers, query_key=None, client=None):
        try:
            want = (v.load_state(strict=True).get("api_key") or "").strip()
        except Exception:
            return False                  # settings unreadable: deny, never open up
        if not want:
            # No key set: this computer only. Without a key, anyone on the
            # show LAN could fire cues, rewrite the inputs or clear the feed.
            return client is not None and client.host in ("127.0.0.1", "::1")
        got = headers.get("authorization", "")
        got = got[7:].strip() if got.lower().startswith("bearer ") else (query_key or "")
        return hmac.compare_digest(got.encode(), want.encode())

    @api.middleware("http")
    async def guard(request: Request, call_next):
        host = request.headers.get("host", "")
        if not v._host_ok(host):
            return fail(400, "BAD_REQUEST", "Bad Host header.")
        if request.method != "GET" and not v._origin_ok(request.headers.get("origin"), host):
            return fail(403, "UNAUTHORIZED", "Cross-site requests are refused.")
        qkey = (request.query_params.get("key")
                if request.url.path.endswith("/transcript/stream") else None)
        if int(request.headers.get("content-length") or 0) > MAX_BODY:
            return fail(413, "BAD_REQUEST", "The body is too large.")
        if not key_ok(request.headers, qkey, request.client):
            return fail(401, "UNAUTHORIZED", "This API needs its key: Authorization: Bearer <key>.")
        try:
            return await call_next(request)
        except ApiError as e:
            return fail(e.status, e.code, e.message)

    # -- shared helpers over FraudCom state
    def roster():
        r = v.load_roster()
        return {"people": [p for p in r.get("people", []) if isinstance(p, dict)],
                "groups": [g for g in r.get("groups", []) if isinstance(g, str)],
                "pins": [g for g in r.get("pins", []) if isinstance(g, str)]}

    def save_roster(r):
        v.save_roster(r)
        v.engine._emit("roster", "", "")          # the open window reloads it

    def pkey(p):
        if p.get("di") is not None and p.get("ch") is not None:
            return f"{p['di']}:{p['ch']}"
        return f"typed:{p.get('id')}"

    def chan_id(p):
        return uid("channel", p.get("id") or p.get("name"))

    def chan_name(p):
        return (p.get("name") or "").strip() or "Speaker"

    def color(p):
        h = p.get("hex")
        if isinstance(h, str) and len(h) == 7 and h.startswith("#"):
            return h
        try:
            return _PALETTE[int(p.get("color") or 0) % len(_PALETTE)]
        except (TypeError, ValueError):
            return _PALETTE[0]

    def channel(p):
        k = pkey(p)
        return {"id": chan_id(p), "name": chan_name(p), "color": color(p),
                "sourceType": "localAudio",
                "speechLocale": None if p.get("translate") else "en-US",
                "typingEnabled": True,
                "isSelected": v.osc_in.selected == k,
                "unreadCount": hub.unread.get(k, 0)}

    def group_id(g):
        return uid("group", g)

    def group(g, people):
        return {"id": group_id(g), "name": g,
                "channelIds": [chan_id(p) for p in people if g in v.person_groups(p)],
                "shareOnNetwork": bool(v.SHARE["on"]), "groupPassword": None}

    def find_person(ref, people=None):
        people = roster()["people"] if people is None else people
        return find(people, ref, chan_id, chan_name, "channel")

    def osc_chan(p):
        """FraudCom's OSC-side channel dict for a roster person."""
        k = pkey(p)
        return next((c for c in v.osc_in.channels() if c["key"] == k),
                    {"key": k, "name": chan_name(p)})

    def item_channel(p):
        return {"type": "channel", "id": chan_id(p), "name": chan_name(p), "channel": channel(p)}

    def item_group(g, people):
        return {"type": "group", "id": group_id(g), "name": g, "group": group(g, people)}

    def ui(action, key="", **kw):
        v.engine._emit("ui", key, "", extra={"action": action, **kw})

    def flash(text, duration=None, repeats=None, color=None):
        v.engine._emit("flash", "", "", text=str(text or ""),
                       levels={"color": v.flash_color(color), "secs": float(duration or 4),
                               "repeats": int(repeats or 1)})

    def window(show):
        win = v.WINDOW.get("win")
        if win is None:
            raise ApiError(400, "BAD_REQUEST", "No app window to show or hide.")
        win.show() if show else win.hide()

    def send_text(p, text):
        v.engine._emit("final", pkey(p), chan_name(p), text,
                       msg_id=f"api#{pkey(p)}#{time.time_ns()}")

    # ================================================================ System
    @api.get("/api/v1/ping")
    async def ping():
        return ok({"pong": True})

    def status():
        st = v.load_state()
        active = st.get("last_preset")
        return {"version": v.APP_VERSION,
                # Clients expect "macOS" or "iOS" here and strict ones reject
                # anything else, so "os" carries the real system.
                "platform": "macOS", "os": {"darwin": "macOS", "win32": "Windows"}
                .get(sys.platform, sys.platform),
                "uptime": round(time.time() - _STARTED, 1),
                "activeConfigurationName": active if active in v.load_presets() else "Default",
                "channelCount": len(roster()["people"]),
                "apiPort": srv.port or DEFAULT_PORT,
                "oscPort": v.osc_in.port if v.osc_in.server else None,
                "networkBroadcastPort": None}

    @api.get("/api/v1/status")
    async def get_status():
        return ok(await asyncio.to_thread(status))

    # ================================================================ Items
    @api.get("/api/v1/items")
    async def items():
        r = roster()
        return ok([item_channel(p) for p in r["people"]]
                  + [item_group(g, r["people"]) for g in r["groups"]])

    @api.get("/api/v1/items/selected")
    async def item_selected():
        p = next((p for p in roster()["people"] if pkey(p) == v.osc_in.selected), None)
        if p is None:
            raise ApiError(404, "NOT_FOUND", "No channel is selected.")
        return ok(item_channel(p))

    @api.put("/api/v1/items/reorder")
    async def items_reorder(request: Request):
        ids = (await jbody(request)).get("ids")
        if not isinstance(ids, list):
            raise ApiError(400, "VALIDATION_ERROR", "ids must be a list.")
        r = roster()
        rank = {i: n for n, i in enumerate(ids)}
        r["people"].sort(key=lambda p: rank.get(chan_id(p), len(ids)))
        r["groups"].sort(key=lambda g: rank.get(group_id(g), len(ids)))
        save_roster(r)
        return ok({"reordered": True})

    @api.get("/api/v1/items/{id}")
    async def item_get(id: str):
        r = roster()
        try:
            p, a = find_person(id, r["people"])
            return ok(item_channel(p), headers=amb(a))
        except ApiError:
            g, a = find(r["groups"], id, group_id, lambda g: g, "item")
            return ok(item_group(g, r["people"]), headers=amb(a))

    # ================================================================ Channels
    @api.get("/api/v1/channels")
    async def channels():
        return ok([channel(p) for p in roster()["people"]])

    @api.post("/api/v1/channels")
    async def channel_create(request: Request):
        b = await jbody(request)
        name = str(b.get("name") or "").strip()
        if not name:
            raise ApiError(400, "VALIDATION_ERROR", "name is required.")
        r = roster()
        p = {"id": "p" + uuid.uuid4().hex[:10], "name": name, "di": None, "ch": None,
             "group": "", "color": len(r["people"]) % len(_PALETTE)}
        if isinstance(b.get("color"), str) and b["color"].startswith("#"):
            p["hex"] = b["color"][:7]
        r["people"].append(p)
        save_roster(r)
        return ok(channel(p), status=201)

    @api.get("/api/v1/channels/selected")
    async def channel_selected():
        p = next((p for p in roster()["people"] if pkey(p) == v.osc_in.selected), None)
        if p is None:
            raise ApiError(404, "NOT_FOUND", "No channel is selected.")
        return ok(channel(p))

    async def select_step(step):
        r = roster()
        if not r["people"]:
            raise ApiError(404, "NOT_FOUND", "There are no channels.")
        await asyncio.to_thread(v.osc_in._dispatch, "/fraudcom",
                                ["channel", step], [], lambda *a: None)
        p = next((p for p in r["people"] if pkey(p) == v.osc_in.selected), r["people"][0])
        return ok(item_channel(p))

    @api.post("/api/v1/channels/select/next")
    async def select_next():
        return await select_step("next")

    @api.post("/api/v1/channels/select/previous")
    async def select_previous():
        return await select_step("previous")

    @api.get("/api/v1/channels/{id}")
    async def channel_get(id: str):
        p, a = find_person(id)
        return ok(channel(p), headers=amb(a))

    @api.put("/api/v1/channels/{id}")
    async def channel_update(id: str, request: Request):
        b = await jbody(request)
        r = roster()
        p, a = find_person(id, r["people"])
        if "name" in b and str(b["name"] or "").strip():
            p["name"] = str(b["name"]).strip()
        if "color" in b:
            if isinstance(b["color"], str) and b["color"].startswith("#"):
                p["hex"] = b["color"][:7]
            elif b["color"] is None:
                p.pop("hex", None)
        save_roster(r)
        return ok(channel(p), headers=amb(a))

    @api.delete("/api/v1/channels/{id}")
    async def channel_delete(id: str):
        r = roster()
        p, a = find_person(id, r["people"])
        r["people"] = [x for x in r["people"] if x is not p]
        save_roster(r)
        return ok({"deleted": True}, headers=amb(a))

    @api.post("/api/v1/channels/{id}/select")
    async def channel_select(id: str):
        p, a = find_person(id)
        v.osc_in.select(osc_chan(p))
        return ok({"selected": chan_id(p)}, headers=amb(a))

    @api.post("/api/v1/channels/{id}/send")
    async def channel_send(id: str, request: Request):
        text = str((await jbody(request)).get("text") or "").strip()
        if not text:
            raise ApiError(400, "VALIDATION_ERROR", "text is required.")
        if len(text) > MAX_TEXT:
            raise ApiError(400, "VALIDATION_ERROR", f"text is limited to {MAX_TEXT} characters.")
        p, a = find_person(id)
        await asyncio.to_thread(send_text, p, text)
        return ok({"sent": True}, headers=amb(a))

    @api.post("/api/v1/channels/{id}/clear")
    async def channel_clear(id: str):
        p, a = find_person(id)
        hub.clear([pkey(p)])
        ui("clear", pkey(p))
        return ok({"cleared": True}, headers=amb(a))

    @api.post("/api/v1/channels/{id}/mark-read")
    async def channel_mark(id: str):
        p, a = find_person(id)
        hub.mark_read([pkey(p)])
        ui("mark", pkey(p))
        return ok({"marked": True}, headers=amb(a))

    # ================================================================ Keywords
    # FraudCom keywords are global: a hot word (highlight + alert) and/or a
    # jargon correction (replacementText). Channel keyword routes show and
    # change that same global list.
    def keywords():
        lex = v.engine.lexicon
        hot = [w for w in lex.get("hotwords", []) if isinstance(w, str)]
        corr = {str(k): str(val) for k, val in lex.get("corrections", {}).items()}
        out, seen = [], set()
        for w in hot:
            seen.add(w.lower())
            out.append({"id": uid("keyword", w.lower()), "text": w, "shouldHighlight": True,
                        "highlightColor": None, "replacementText": corr.get(w.lower()),
                        "isSensitive": False})
        for k, val in corr.items():
            if k.lower() not in seen:
                out.append({"id": uid("keyword", k.lower()), "text": k, "shouldHighlight": False,
                            "highlightColor": None, "replacementText": val,
                            "isSensitive": False})
        return out

    def set_lexicon(hot, corr):
        lex = {"hotwords": hot, "corrections": corr}
        v.engine.lexicon = lex
        v.engine.corrector.update(corr)
        v.save_lexicon(lex)
        v.engine._emit("lexicon", "", "")        # the open window reloads it

    def keyword_write(b, old=None):
        lex = v.engine.lexicon
        hot = [w for w in lex.get("hotwords", []) if isinstance(w, str)]
        corr = dict(lex.get("corrections", {}))
        if old is not None:
            hot = [w for w in hot if w.lower() != old["text"].lower()]
            corr.pop(old["text"].lower(), None)
            corr.pop(old["text"], None)
        text = str(b.get("text", old["text"] if old else "") or "").strip()
        if not text:
            raise ApiError(400, "VALIDATION_ERROR", "text is required.")
        if b.get("shouldHighlight", old["shouldHighlight"] if old else True):
            hot.append(text)
        rep = b.get("replacementText", old["replacementText"] if old else None)
        if rep:
            corr[text.lower()] = str(rep)
        set_lexicon(hot, corr)
        return next(k for k in keywords() if k["text"].lower() == text.lower()) \
            if any(k["text"].lower() == text.lower() for k in keywords()) else \
            {"id": uid("keyword", text.lower()), "text": text, "shouldHighlight": False,
             "highlightColor": None, "replacementText": None, "isSensitive": False}

    def keyword_delete(ref):
        k, a = find(keywords(), ref, lambda k: k["id"], lambda k: k["text"], "keyword")
        lex = v.engine.lexicon
        set_lexicon([w for w in lex.get("hotwords", []) if w.lower() != k["text"].lower()],
                    {kk: vv for kk, vv in lex.get("corrections", {}).items()
                     if kk.lower() != k["text"].lower()})
        return a

    @api.get("/api/v1/keywords")
    async def kw_list():
        return ok(keywords())

    @api.post("/api/v1/keywords")
    async def kw_create(request: Request):
        return ok(keyword_write(await jbody(request)), status=201)

    @api.get("/api/v1/keywords/{id}")
    async def kw_get(id: str):
        k, a = find(keywords(), id, lambda k: k["id"], lambda k: k["text"], "keyword")
        return ok(k, headers=amb(a))

    @api.put("/api/v1/keywords/{id}")
    async def kw_update(id: str, request: Request):
        b = await jbody(request)
        k, a = find(keywords(), id, lambda k: k["id"], lambda k: k["text"], "keyword")
        return ok(keyword_write(b, k), headers=amb(a))

    @api.delete("/api/v1/keywords/{id}")
    async def kw_delete(id: str):
        return ok({"deleted": True}, headers=amb(keyword_delete(id)))

    @api.get("/api/v1/channels/{id}/keywords")
    async def ch_kw_list(id: str):
        find_person(id)
        return ok(keywords())

    @api.post("/api/v1/channels/{id}/keywords")
    async def ch_kw_create(id: str, request: Request):
        find_person(id)
        return ok(keyword_write(await jbody(request)), status=201)

    @api.delete("/api/v1/channels/{id}/keywords/{keywordId}")
    async def ch_kw_delete(id: str, keywordId: str):
        find_person(id)
        return ok({"deleted": True}, headers=amb(keyword_delete(keywordId)))

    # ================================================================ Groups
    @api.get("/api/v1/groups")
    async def groups():
        r = roster()
        return ok([group(g, r["people"]) for g in r["groups"]])

    def set_members(r, g, ids):
        want = set(ids or [])
        for p in r["people"]:
            cur = v.person_groups(p)
            if chan_id(p) in want or (p.get("id") in want):
                if g not in cur:
                    v.set_person_groups(p, cur + [g])    # adds; keeps other groups
            elif g in cur:
                v.set_person_groups(p, [x for x in cur if x != g])

    @api.post("/api/v1/groups")
    async def group_create(request: Request):
        b = await jbody(request)
        name = str(b.get("name") or "").strip()
        if not name:
            raise ApiError(400, "VALIDATION_ERROR", "name is required.")
        r = roster()
        if name in r["groups"]:
            raise ApiError(409, "CONFLICT", "A group with that name exists.")
        r["groups"].append(name)
        if isinstance(b.get("channelIds"), list):
            set_members(r, name, b["channelIds"])
        save_roster(r)
        return ok(group(name, r["people"]), status=201)

    @api.get("/api/v1/groups/{id}")
    async def group_get(id: str):
        r = roster()
        g, a = find(r["groups"], id, group_id, lambda g: g, "group")
        return ok(group(g, r["people"]), headers=amb(a))

    @api.put("/api/v1/groups/{id}")
    async def group_update(id: str, request: Request):
        b = await jbody(request)
        r = roster()
        g, a = find(r["groups"], id, group_id, lambda g: g, "group")
        new = str(b.get("name") or g).strip() or g
        if new != g:
            r["groups"] = [new if x == g else x for x in r["groups"]]
            r["pins"] = [new if x == g else x for x in r["pins"]]
            for p in r["people"]:
                if g in v.person_groups(p):
                    v.set_person_groups(p, [new if x == g else x for x in v.person_groups(p)])
        if isinstance(b.get("channelIds"), list):
            set_members(r, new, b["channelIds"])
        save_roster(r)
        return ok(group(new, r["people"]), headers=amb(a))

    @api.delete("/api/v1/groups/{id}")
    async def group_delete(id: str):
        r = roster()
        g, a = find(r["groups"], id, group_id, lambda g: g, "group")
        r["groups"] = [x for x in r["groups"] if x != g]
        r["pins"] = [x for x in r["pins"] if x != g]
        for p in r["people"]:
            if g in v.person_groups(p):
                v.set_person_groups(p, [x for x in v.person_groups(p) if x != g])
        save_roster(r)
        return ok({"deleted": True}, headers=amb(a))

    # ================================================================ Transcript
    def entry_list(channel_ref=None, since=None):
        with hub.lock:
            es = list(hub.entries.values())
        if channel_ref:
            p, _ = find_person(channel_ref)
            es = [e for e in es if e["channelId"] == chan_id(p)]
        if since:
            es = [e for e in es if e["date"] > since]
        return es

    @api.get("/api/v1/transcript")
    async def transcript(channelId: str = None, since: str = None,
                         limit: int = 50, offset: int = 0):
        return page(entry_list(channelId, since), limit, offset)

    @api.post("/api/v1/transcript/clear")
    async def transcript_clear(request: Request):
        ref = (await jbody(request, required=False)).get("channelId")
        if ref:
            p, _ = find_person(ref)
            hub.clear([pkey(p)])
            ui("clear", pkey(p))
        else:
            hub.clear()
            ui("clear", "all")
        return ok({"cleared": True})

    @api.post("/api/v1/transcript/mark-read")
    async def transcript_mark(request: Request):
        ref = (await jbody(request, required=False)).get("channelId")
        if ref:
            p, _ = find_person(ref)
            hub.mark_read([pkey(p)])
            ui("mark", pkey(p))
        else:
            hub.mark_read()
            ui("mark", "all")
        return ok({"marked": True})

    @api.get("/api/v1/transcript/stream")
    async def transcript_stream(request: Request, snapshot: bool = False,
                                channelId: str = None):
        chan = None
        if channelId:
            p, _ = find_person(channelId)
            chan = chan_id(p)
        last = request.headers.get("last-event-id")
        q = asyncio.Queue(maxsize=2000)
        with hub.lock:
            hub.sse.append((q, chan))
            backlog = list(hub.events)
            latest = hub.seq

        def fmt(seq, name, payload):
            return f"id: {hub.event_id(seq)}\nevent: {name}\ndata: {payload}\n\n"

        async def gen():
            sent = latest
            try:
                if last:
                    epoch, _, n = last.rpartition("-")
                    seqs = [e[0] for e in backlog]
                    if epoch == hub.epoch and n.isdigit() and int(n) in seqs:
                        for seq, name, payload, c in backlog:
                            if seq > int(n) and (chan is None or c == chan):
                                yield fmt(seq, name, payload)
                    else:
                        yield fmt(latest, "transcript.reset",
                                  json.dumps({"reason": "cursor_unavailable"}))
                elif snapshot:
                    with hub.lock:
                        es = [e for e in hub.entries.values()
                              if chan is None or e["channelId"] == chan][-_ENTRIES_MAX:]
                    payload = json.dumps(es)
                    if len(payload) > 2 * 1024 * 1024:
                        yield fmt(latest, "transcript.reset",
                                  json.dumps({"reason": "snapshot_unavailable"}))
                    else:
                        yield fmt(latest, "transcript.snapshot", payload)
                yield fmt(latest, "transcript.cursor", "{}")
                while True:
                    if await request.is_disconnected():
                        return
                    try:
                        seq, name, payload, c = await asyncio.wait_for(q.get(), 15)
                    except asyncio.TimeoutError:
                        yield ": keep-alive\n\n"
                        continue
                    if seq > sent:
                        sent = seq
                        yield fmt(seq, name, payload)
            finally:
                with hub.lock:
                    hub.sse = [s for s in hub.sse if s[0] is not q]

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache"})

    @api.get("/api/v1/transcript/{id}")
    async def transcript_get(id: str):
        with hub.lock:
            e = hub.entries.get(id)
        if e is None:
            raise not_found("transcript entry")
        return ok(dict(e))

    # ================================================================ Automations
    def automation(r):
        trig = r.get("trigger") or "phrase"
        if trig == "osc":
            t = {"type": "osc", "oscTrigger": {"addressPattern": r.get("osc_pattern") or ""}}
        elif trig == "webhook":
            t = {"type": "httpWebhook", "path": r.get("webhook_path") or ""}
        else:
            p = next((p for p in roster()["people"] if pkey(p) == r.get("channel")), None)
            t = {"type": "message", "channelId": chan_id(p) if p else None,
                 "keywords": r.get("keywords") or ([r["phrase"]] if r.get("phrase") else []),
                 "triggerEarly": False}
        if r.get("actions"):
            acts = r["actions"]
        elif r.get("action") == "flash":
            acts = [{"type": "flashWindow", "text": r.get("flash_text") or None,
                     "duration": float(r.get("flash_secs") or 4), "repeats": 1,
                     "color": r.get("flash_color") or "danger"}]
        elif r.get("action") == "midi":
            acts = [{"type": "midi", "text": v._midi_describe(r),
                     "midi": {"deviceId": r.get("midi_port") or v.MIDI_VIRTUAL_NAME,
                              "messageType": r.get("midi_type") or "note",
                              "channel": int(r.get("midi_channel") or 1) - 1,
                              "number": r.get("midi_num"), "value": r.get("midi_val"),
                              "raw": r.get("midi_raw") or None}}]
        else:
            acts = [{"type": "osc", "host": r.get("host"), "port": r.get("port"),
                     "addressPattern": r.get("address"), "arguments": r.get("args") or "",
                     "transport": r.get("proto") or "udp"}]
        return {"id": uid("automation", r.get("id")),
                "name": v.automations.display_name(r) or "Automation",
                "enabled": r.get("enabled", True),
                "executionPolicy": r.get("executionPolicy") or "ignoreIfRunning",
                "trigger": t, "actions": acts}

    def rule_from(b, r=None):
        r = dict(r or {"id": "a" + uuid.uuid4().hex[:10], "enabled": True})
        if "name" in b:
            r["name"] = str(b["name"] or "").strip()
        if "enabled" in b:
            r["enabled"] = bool(b["enabled"])
        if b.get("executionPolicy") in ("cancelAndRestart", "ignoreIfRunning"):
            r["executionPolicy"] = b["executionPolicy"]
        t = b.get("trigger")
        if isinstance(t, dict):
            for k in ("phrase", "keywords", "channel", "osc_pattern", "webhook_path"):
                r.pop(k, None)
            if t.get("type") == "message":
                kws = [str(k) for k in (t.get("keywords") or []) if str(k).strip()]
                if not kws:
                    raise ApiError(400, "VALIDATION_ERROR", "A message trigger needs keywords.")
                r.update(trigger="phrase", keywords=kws, phrase=kws[0])
                if t.get("channelId"):
                    p, _ = find_person(t["channelId"])
                    r["channel"] = pkey(p)
            elif t.get("type") == "osc":
                ot = t.get("oscTrigger") or {}
                pat = ot.get("addressPattern") or ot.get("address") or ot.get("pattern")
                if not pat:
                    raise ApiError(400, "VALIDATION_ERROR", "An osc trigger needs an address pattern.")
                try:
                    osclink.check_pattern(str(pat))
                except ValueError as e:
                    raise ApiError(400, "VALIDATION_ERROR", str(e))
                r.update(trigger="osc", osc_pattern=str(pat))
            elif t.get("type") == "httpWebhook":
                if not t.get("path"):
                    raise ApiError(400, "VALIDATION_ERROR", "A webhook trigger needs a path.")
                r.update(trigger="webhook", webhook_path=str(t["path"]).strip("/"))
            elif t.get("type") == "midi":
                raise ApiError(501, "NOT_IMPLEMENTED", "FraudCom can't be triggered by MIDI.")
            else:
                raise ApiError(400, "VALIDATION_ERROR", "Unknown trigger type.")
        acts = b.get("actions")
        if acts is not None:
            if not isinstance(acts, list) or not all(isinstance(a, dict) and a.get("type") for a in acts):
                raise ApiError(400, "VALIDATION_ERROR", "actions must be a list of actions.")
            if len(acts) > MAX_ACTIONS:
                raise ApiError(400, "VALIDATION_ERROR", f"At most {MAX_ACTIONS} actions.")
            r["actions"] = acts
            first = acts[0] if acts else {}
            r["action"] = "flash" if first.get("type") == "flashWindow" else "osc"
        return r

    def rules_save(rules):
        v.automations.save(rules)
        v.engine._emit("automations", "", "")    # the open window reloads them

    def rule_find(ref):
        rules = v.automations.rules
        return find(rules, ref, lambda r: uid("automation", r.get("id")),
                    lambda r: v.automations.display_name(r), "automation")

    @api.get("/api/v1/automations")
    async def autos():
        return ok([automation(r) for r in v.automations.rules])

    @api.post("/api/v1/automations")
    async def auto_create(request: Request):
        b = await jbody(request)
        if not str(b.get("name") or "").strip() or "trigger" not in b or "actions" not in b:
            raise ApiError(400, "VALIDATION_ERROR", "name, trigger and actions are required.")
        r = rule_from(b)
        rules_save(v.automations.rules + [r])
        return ok(automation(r), status=201)

    @api.get("/api/v1/automations/{id}")
    async def auto_get(id: str):
        r, a = rule_find(id)
        return ok(automation(r), headers=amb(a))

    @api.put("/api/v1/automations/{id}")
    async def auto_update(id: str, request: Request):
        b = await jbody(request)
        r, a = rule_find(id)
        new = rule_from(b, r)
        rules_save([new if x is r else x for x in v.automations.rules])
        return ok(automation(new), headers=amb(a))

    @api.delete("/api/v1/automations/{id}")
    async def auto_delete(id: str):
        r, a = rule_find(id)
        rules_save([x for x in v.automations.rules if x is not r])
        return ok({"deleted": True}, headers=amb(a))

    @api.post("/api/v1/automations/{id}/trigger")
    async def auto_trigger(id: str):
        r, a = rule_find(id)
        await asyncio.to_thread(v.automations.fire, r, "", {"value": "", "raw": ""}, True)
        return ok({"triggered": True}, headers=amb(a))

    async def auto_enable(id, on):
        r, a = rule_find(id)
        new = dict(r, enabled=on)
        rules_save([new if x is r else x for x in v.automations.rules])
        return ok(automation(new), headers=amb(a))

    @api.put("/api/v1/automations/{id}/enable")
    async def auto_on(id: str):
        return await auto_enable(id, True)

    @api.put("/api/v1/automations/{id}/disable")
    async def auto_off(id: str):
        return await auto_enable(id, False)

    # -- running an action list (automations made through this API)
    def ref_for(a, ctx):
        target = a.get("target") or ("channel" if a.get("targetChannelId") else "selected")
        if target in ("all", "selected"):
            return target
        if target == "dynamic":
            # a dynamic target resolves by display name, then 1-based index
            return osclink.expand(str(a.get("text") or "{value}"), ctx)
        try:
            p, _ = find_person(a.get("targetChannelId") or "")
        except ApiError:
            return None
        return str(next((c["index"] for c in v.osc_in.channels() if c["key"] == pkey(p)), 0) or "")

    def dispatch(parts, args=()):
        v.osc_in._dispatch("/fraudcom", parts, list(args), lambda *x: None)

    def run_action(a, ctx):
        t = a.get("type")
        text = osclink.expand(str(a.get("text") or ""), ctx)
        if t == "flashWindow":
            flash(text or "", a.get("duration"), a.get("repeats"), a.get("color"))
        elif t in ("showWindow", "hideWindow"):
            window(t == "showWindow")
        elif t == "selectChannel":
            ref = ref_for(a, ctx)
            if ref and ref not in ("all", "selected"):
                dispatch(["channel", ref, "select"])
        elif t in ("selectNextChannel", "selectPreviousChannel"):
            dispatch(["channel", "next" if t == "selectNextChannel" else "previous"])
        elif t in ("scrollTranscriptUp", "scrollTranscriptDown", "scrollTranscriptToCurrent"):
            dispatch(["scroll", {"scrollTranscriptUp": "up", "scrollTranscriptDown": "down"}
                      .get(t, "current")])
        elif t in ("increaseTranscriptFontSize", "decreaseTranscriptFontSize"):
            dispatch(["font", "increase" if t.startswith("increase") else "decrease"])
        elif t in ("clearTranscript", "markTranscriptAsRead"):
            ref = ref_for(a, ctx) or "selected"
            dispatch(["channel", ref, "clear" if t == "clearTranscript" else "mark"])
        elif t == "sendTextToChannel":
            ref = ref_for(a, ctx) or "selected"
            if text:
                dispatch(["channel", ref, "send"], [text])
        elif t == "osc":
            osc_send_action(a, ctx)
        elif t == "httpRequest":
            url = osclink.expand(str(a.get("url") or ""), ctx)
            if not url.lower().startswith(("http://", "https://")):
                raise ValueError("httpRequest needs an http(s) url")
            body = a.get("body")
            req = urllib.request.Request(
                url, method=str(a.get("method") or "get").upper(),
                data=osclink.expand(str(body), ctx).encode() if body else None,
                headers={str(k): str(val) for k, val in (a.get("headers") or {}).items()})
            urllib.request.urlopen(req, timeout=num(a.get("timeout"), 5, 0.1, 30)).close()
        elif t == "midi":
            m = a.get("midi") if isinstance(a.get("midi"), dict) else a
            if "messageType" in m:            # as listed by GET /automations
                r = {"midi_port": "" if m.get("deviceId") in (None, "", v.MIDI_VIRTUAL_NAME)
                     else str(m["deviceId"]), "midi_type": m.get("messageType"),
                     "midi_channel": int(m.get("channel") or 0) + 1,
                     "midi_num": m.get("number"), "midi_val": m.get("value"),
                     "midi_raw": m.get("raw") or ""}
            else:                             # a SendMIDIRequest
                r = v.midi_from_api(m)
            v.automations.midi.send(r)
        else:
            raise ValueError(f"unknown action '{t}'")

    running, run_lock = {}, threading.Lock()     # rule id -> cancel Event

    def run_actions(r, key, ctx):
        """Delayed actions run on ONE thread per run, in delay order.
        executionPolicy: ignoreIfRunning skips a trigger while the rule's
        last run is still going; cancelAndRestart stops it and starts over."""
        name = v.automations.display_name(r)
        rid = r.get("id") or name
        with run_lock:
            if rid in running:
                if (r.get("executionPolicy") or "ignoreIfRunning") == "ignoreIfRunning":
                    return
                running[rid].set()
            cancel = running[rid] = threading.Event()
        acts = sorted(r["actions"][:MAX_ACTIONS],
                      key=lambda a: num(a.get("delay"), 0, 0, MAX_DELAY))
        v.engine._emit("automation", key, name, text=f"{len(acts)} action(s)")

        def go():
            t0 = time.monotonic()
            try:
                for a in acts:
                    wait = t0 + num(a.get("delay"), 0, 0, MAX_DELAY) - time.monotonic()
                    if (wait > 0 and cancel.wait(wait)) or cancel.is_set():
                        return
                    try:
                        run_action(a, ctx)
                    except Exception as e:
                        v.engine._emit("warn", "", "", text=f"automation '{name}': {e}")
            finally:
                with run_lock:
                    if running.get(rid) is cancel:
                        del running[rid]
        if any(num(a.get("delay"), 0, 0, MAX_DELAY) > 0 for a in acts):
            threading.Thread(target=go, daemon=True).start()
        else:
            go()
    v.ACTION_RUNNER[:] = [run_actions]

    # ================================================================ Configurations
    def configs():
        presets = v.load_presets()
        nauto = len(v.automations.rules)
        out = [{"id": uid("configuration", n), "name": n,
                "channelCount": len((d or {}).get("people") or []),
                "automationCount": nauto,
                "keywordCount": len((d or {}).get("hotwords") or [])}
               for n, d in sorted(presets.items())]
        return out, presets

    def current_config():
        st = v.load_state()
        cs, _ = configs()
        hit = next((c for c in cs if c["name"] == st.get("last_preset")), None)
        if hit:
            return hit
        return {"id": uid("configuration", "\0current"), "name": "Default",
                "channelCount": len(roster()["people"]),
                "automationCount": len(v.automations.rules),
                "keywordCount": len(keywords())}

    def cfg_find(ref):
        cs, presets = configs()
        c, a = find(cs, ref, lambda c: c["id"], lambda c: c["name"], "configuration")
        return c, a, presets

    def current_preset_data():
        r = roster()
        return {"people": r["people"], "groups": r["groups"],
                "hotwords": list(v.engine.lexicon.get("hotwords", []))}

    @api.get("/api/v1/configurations")
    async def cfg_list():
        return ok(configs()[0])

    @api.post("/api/v1/configurations")
    async def cfg_create(request: Request):
        name = str((await jbody(request)).get("name") or "").strip()
        if not name:
            raise ApiError(400, "VALIDATION_ERROR", "name is required.")
        presets = v.load_presets(strict=True)
        presets[name] = current_preset_data()
        v.save_presets(presets)
        return ok(next(c for c in configs()[0] if c["name"] == name), status=201)

    @api.get("/api/v1/configurations/active")
    async def cfg_active():
        return ok(current_config())

    @api.put("/api/v1/configurations/{id}")
    async def cfg_rename(id: str, request: Request):
        new = str((await jbody(request)).get("name") or "").strip()
        if not new:
            raise ApiError(400, "VALIDATION_ERROR", "name is required.")
        c, a, presets = cfg_find(id)
        if new != c["name"]:
            if new in presets:
                raise ApiError(409, "CONFLICT", "A configuration with that name exists.")
            presets[new] = presets.pop(c["name"])
            v.save_presets(presets)
            st = v.load_state()
            if st.get("last_preset") == c["name"]:
                st["last_preset"] = new
                v.save_state(st)
        return ok(next(x for x in configs()[0] if x["name"] == new), headers=amb(a))

    @api.delete("/api/v1/configurations/{id}")
    async def cfg_delete(id: str):
        c, a, presets = cfg_find(id)
        if v.load_state().get("last_preset") == c["name"]:
            raise ApiError(409, "CONFLICT", "The active configuration can't be deleted.")
        presets.pop(c["name"], None)
        v.save_presets(presets)
        return ok({"deleted": True}, headers=amb(a))

    def activate(name, d):
        r = roster()
        save_roster({"people": d.get("people") or [], "groups": d.get("groups") or [],
                     "pins": r["pins"]})
        if isinstance(d.get("hotwords"), list):
            set_lexicon(list(d["hotwords"]), dict(v.engine.lexicon.get("corrections", {})))
        st = v.load_state()
        st["last_preset"] = name
        v.save_state(st)

    @api.post("/api/v1/configurations/{id}/activate")
    async def cfg_activate(id: str):
        c, a, presets = cfg_find(id)
        await asyncio.to_thread(activate, c["name"], presets[c["name"]] or {})
        return ok(c, headers=amb(a))

    @api.get("/api/v1/configurations/{id}/export")
    async def cfg_export(id: str):
        c, a, presets = cfg_find(id)
        data = base64.b64encode(json.dumps(
            {"name": c["name"], "preset": presets[c["name"]]}).encode()).decode()
        return ok({"name": c["name"], "data": data}, headers=amb(a))

    @api.post("/api/v1/configurations/import")
    async def cfg_import(request: Request):
        b = await jbody(request)
        try:
            d = json.loads(base64.b64decode(str(b.get("data") or ""), validate=True))
            preset = d.get("preset") if isinstance(d, dict) else None
            if not isinstance(preset, dict) or not isinstance(preset.get("people", []), list):
                raise ValueError
        except (ValueError, TypeError):
            raise ApiError(400, "VALIDATION_ERROR",
                           "data isn't a configuration exported by this API.")
        name = str(b.get("name") or d.get("name") or "Imported").strip()
        presets = v.load_presets(strict=True)
        presets[name] = preset
        v.save_presets(presets)
        return ok(next(c for c in configs()[0] if c["name"] == name), status=201)

    # ================================================================ Audio
    def devices():
        devs = None if v.engine.running else v._fresh_devices()
        return v._curate_devices(devs if devs is not None else v.engine.list_devices())

    def dev_id(d):
        return f"{d['hostapi']}:{d['name']}"

    @api.get("/api/v1/audio/devices")
    async def audio_devices():
        ds = await asyncio.to_thread(devices)
        return ok([{"id": dev_id(d), "name": d["name"], "manufacturer": None,
                    "channelCount": int(d["max_input_channels"])} for d in ds])

    @api.get("/api/v1/audio/devices/{id}/channels")
    async def audio_channels(id: str):
        ds = await asyncio.to_thread(devices)
        d, _ = find(ds, id, dev_id, lambda d: d["name"], "audio device")
        labels = {p.get("ch"): chan_name(p) for p in roster()["people"]
                  if p.get("devName") == d["name"]}
        return ok([{"index": i, "label": labels.get(i)}
                   for i in range(int(d["max_input_channels"]))])

    # ================================================================ Window / font / scroll
    @api.post("/api/v1/window/show")
    async def win_show():
        window(True)
        return ok({"ok": True})

    @api.post("/api/v1/window/hide")
    async def win_hide():
        window(False)
        return ok({"ok": True})

    @api.post("/api/v1/window/flash")
    async def win_flash(request: Request):
        b = await jbody(request, required=False)
        flash(b.get("text") or "", b.get("duration"), b.get("repeats"), b.get("color"))
        return ok({"ok": True})

    for _path, _act, _dir in (("font/increase", "font", "increase"),
                              ("font/decrease", "font", "decrease"),
                              ("scroll/up", "scroll", "up"),
                              ("scroll/down", "scroll", "down"),
                              ("scroll/current", "scroll", "current")):
        def _make(act, d):
            async def h():
                ui(act, dir=d)
                return ok({"ok": True})
            return h
        api.post(f"/api/v1/{_path}")(_make(_act, _dir))

    # ================================================================ OSC
    def osc_server():
        st = v.load_state()
        return {"isEnabled": bool(st.get("osc_rx", False)),
                "receivePort": v.osc_in.port if v.osc_in.server
                else (st.get("osc_rx_port") or v.OSCReceiver.DEFAULT_PORT)}

    @api.get("/api/v1/osc/server")
    async def osc_get():
        return ok(osc_server())

    @api.put("/api/v1/osc/server")
    async def osc_put(request: Request):
        b = await jbody(request)
        st = v.load_state(strict=True)
        if "isEnabled" in b:
            st["osc_rx"] = bool(b["isEnabled"])
        if "receivePort" in b:
            port = b["receivePort"]
            if not isinstance(port, int) or not 1024 <= port <= 65535:
                raise ApiError(400, "VALIDATION_ERROR", "receivePort must be 1024–65535.")
            st["osc_rx_port"] = port
        v.save_state(st)
        await asyncio.to_thread(v._apply_osc_rx, st)
        if v.osc_in.error:
            raise ApiError(409, "CONFLICT", v.osc_in.error)
        return ok(osc_server())

    dests_path = v.os.path.join(v.DATA_DIR, "osc_destinations.json")

    def dests():
        d = v._load_json(dests_path, [])
        return [x for x in d if isinstance(x, dict)] if isinstance(d, list) else []

    def dest_find(ref):
        return find(dests(), ref, lambda d: d.get("id"), lambda d: d.get("name") or "",
                    "OSC destination")

    def dest_check(d):
        if not str(d.get("name") or "").strip() or not str(d.get("host") or "").strip():
            raise ApiError(400, "VALIDATION_ERROR", "name and host are required.")
        if not isinstance(d.get("port"), int) or not 1 <= d["port"] <= 65535:
            raise ApiError(400, "VALIDATION_ERROR", "port must be 1–65535.")

    @api.get("/api/v1/osc/destinations")
    async def dest_list():
        return ok(dests())

    @api.post("/api/v1/osc/destinations")
    async def dest_create(request: Request):
        b = await jbody(request)
        d = {"id": str(uuid.uuid4()), "name": str(b.get("name") or "").strip(),
             "host": str(b.get("host") or "").strip(), "port": b.get("port"),
             "isEnabled": bool(b.get("isEnabled", True))}
        dest_check(d)
        v._write_json(dests_path, dests() + [d], indent=2)
        return ok(d, status=201)

    @api.get("/api/v1/osc/destinations/{id}")
    async def dest_get(id: str):
        d, a = dest_find(id)
        return ok(d, headers=amb(a))

    @api.put("/api/v1/osc/destinations/{id}")
    async def dest_update(id: str, request: Request):
        b = await jbody(request)
        d, a = dest_find(id)
        new = dict(d, **{k: b[k] for k in ("name", "host", "port", "isEnabled") if k in b})
        dest_check(new)
        v._write_json(dests_path, [new if x["id"] == d["id"] else x for x in dests()], indent=2)
        return ok(new, headers=amb(a))

    @api.delete("/api/v1/osc/destinations/{id}")
    async def dest_delete(id: str):
        d, a = dest_find(id)
        v._write_json(dests_path, [x for x in dests() if x["id"] != d["id"]], indent=2)
        return ok({"deleted": True}, headers=amb(a))

    def typed(args):
        out = []
        for x in args or []:
            if not isinstance(x, dict):
                raise ApiError(400, "VALIDATION_ERROR", "arguments must be {type, value} objects.")
            t, val = x.get("type"), str(x.get("value", ""))
            try:
                if t == "int":
                    out.append(osclink.Typed("i", int(val)))
                elif t == "float":
                    out.append(osclink.Typed("f", float(val)))
                elif t == "bool":
                    out.append(val.strip().lower() in ("true", "1", "yes", "on"))
                elif t == "string":
                    out.append(osclink.Typed("s", val))
                else:
                    raise ValueError
            except ValueError:
                raise ApiError(400, "VALIDATION_ERROR", f"Bad {t or 'unknown'} argument '{val}'.")
        return out

    def osc_targets(dest_ref):
        if dest_ref:
            return [dest_find(dest_ref)[0]]
        hits = [d for d in dests() if d.get("isEnabled", True)]
        if not hits:
            raise ApiError(400, "BAD_REQUEST", "No OSC destination: create one or give destinationId.")
        return hits

    def osc_send_action(a, ctx):
        addr = osclink.expand(str(a.get("addressPattern") or a.get("address") or ""), ctx)
        if not addr.startswith("/"):
            raise ValueError("osc action needs an address")
        args = a.get("arguments")
        args = typed(args) if isinstance(args, list) else osclink.parse_args(str(args or ""), ctx)
        if a.get("host") and a.get("port"):
            targets = [{"host": a["host"], "port": int(a["port"])}]
        else:
            targets = osc_targets(a.get("destinationId"))
        for t in targets:
            v.automations.bus.send(t["host"], int(t["port"]), addr, args,
                                   proto="tcp" if a.get("transport") == "tcp" else "udp")

    @api.post("/api/v1/osc/send")
    async def osc_send(request: Request):
        b = await jbody(request)
        addr = str(b.get("addressPattern") or "")
        if not addr.startswith("/"):
            raise ApiError(400, "VALIDATION_ERROR", "addressPattern must start with /.")
        args = typed(b.get("arguments"))
        targets = osc_targets(b.get("destinationId"))
        try:
            for t in targets:
                await asyncio.to_thread(v.automations.bus.send, t["host"], int(t["port"]), addr, args)
        except OSError as e:
            raise ApiError(400, "BAD_REQUEST", f"Couldn't send: {e}")
        hub.log("osc", "outbound", f"{addr} → {', '.join(t['host'] for t in targets)}")
        return ok({"sent": True})

    # ================================================================ MIDI / network / activity
    def midi_list():
        info = v.automations.midi.ports()
        out = [{"id": n, "name": n, "manufacturer": None,
                "isInput": False, "isOutput": True} for n in info["ports"]]
        if info.get("virtual"):
            out.insert(0, {"id": v.MIDI_VIRTUAL_NAME, "name": "FraudCom (virtual port)",
                           "manufacturer": None, "isInput": False, "isOutput": True})
        return out

    @api.get("/api/v1/midi")
    async def midi():
        return ok(await asyncio.to_thread(midi_list))

    @api.get("/api/v1/midi/devices")
    async def midi_devices():
        return ok(await asyncio.to_thread(midi_list))

    @api.post("/api/v1/midi/send")
    async def midi_send(request: Request):
        b = await jbody(request)
        try:
            r = v.midi_from_api(b)
            await asyncio.to_thread(v.automations.midi.send, r)
        except (ValueError, TypeError) as e:
            raise ApiError(400, "VALIDATION_ERROR", str(e))
        except Exception as e:
            raise ApiError(502, "MIDI_ERROR", str(e))
        hub.log("midi", "outbound", v._midi_describe(r))
        return ok({"sent": True})

    @api.get("/api/v1/network")
    async def network():
        return ok({"isEnabled": bool(v.SHARE["on"]), "port": v.PORT,
                   "connectedPeers": max(0, v.VIEWERS["n"])})

    @api.get("/api/v1/activity")
    async def activity(limit: int = 50, offset: int = 0):
        return page(list(hub.activity), limit, offset)

    # ================================================================ Webhooks
    @api.api_route("/api/v1/webhooks/{path:path}",
                   methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def webhook(path: str, request: Request):
        if request.method != "POST":
            raise ApiError(405, "METHOD_NOT_ALLOWED", "Only POST is allowed.")
        n = await asyncio.to_thread(v.automations.on_webhook, path)
        if not n:
            raise ApiError(404, "NOT_FOUND", "No automation listens on that webhook path.")
        hub.log("webhook", "inbound", path)
        return ok({"triggered": True})

    # ================================================================ WebSocket
    @api.websocket("/api/v1/ws")
    async def ws(websocket: WebSocket, key: str = None):
        host = websocket.headers.get("host", "")
        if not v._host_ok(host) or not v._origin_ok(websocket.headers.get("origin"), host) \
                or not key_ok(websocket.headers, key, websocket.client):
            await websocket.close(code=1008)
            return
        await websocket.accept()
        q = asyncio.Queue(maxsize=2000)
        entry = [q, None]
        with hub.lock:
            hub.ws.append(entry)
        await websocket.send_json({"type": "welcome", "version": v.APP_VERSION})

        async def reader():
            while True:
                m = await websocket.receive_json()
                if isinstance(m, dict) and m.get("type") == "subscribe":
                    ev = m.get("events")
                    entry[1] = {str(x) for x in ev} if isinstance(ev, list) else None

        rd = asyncio.ensure_future(reader())
        try:
            while not rd.done():
                try:
                    frame = await asyncio.wait_for(q.get(), 20)
                except asyncio.TimeoutError:
                    frame = {"type": "heartbeat"}
                if entry[1] is not None and frame.get("type") not in entry[1] \
                        and frame.get("type") != "heartbeat":
                    continue
                await websocket.send_json(frame)
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            rd.cancel()
            with hub.lock:
                hub.ws = [w for w in hub.ws if w is not entry]

    class Handle:
        pass
    h = Handle()
    h.server, h.hub, h.app = srv, hub, api
    return h
