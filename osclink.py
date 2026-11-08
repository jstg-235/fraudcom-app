"""OSC plumbing for FraudCom's automations and its OSC input.

- OSC 1.0 messages with explicit types (i f s b T F N I h d), bundles decoded
- OSC 1.1 address patterns:  /cue/*  /cue/{go,stop}  /cue/[1-5]  /msg/{channel}/{text}
  ({name} without a comma captures one path segment, by name and by position)
- {template} expansion for automation actions ({value}, {address}, {arg1}, {1}, ...)
- transports: UDP, and TCP with SLIP framing (OSC 1.1)

Standard library only.
"""

import re
import shlex
import socket
import struct
import threading
import time
from functools import lru_cache

# ---------------------------------------------------------------- messages
IMPULSE = object()            # the OSC "I" (bang) argument


class Typed:
    """An argument with a forced OSC type tag, e.g. Typed("s", "42")."""
    __slots__ = ("tag", "value")

    def __init__(self, tag, value):
        self.tag, self.value = tag, value


def _pad(b: bytes) -> bytes:
    return b + b"\0" * (4 - len(b) % 4)


def encode(address: str, args=()) -> bytes:
    tags, data = ",", b""
    for a in args:
        tag, v = (a.tag, a.value) if isinstance(a, Typed) else (None, a)
        if tag is None:
            tag = ("I" if v is IMPULSE else "N" if v is None
                   else ("T" if v else "F") if isinstance(v, bool)
                   else "i" if isinstance(v, int) and -2**31 <= v < 2**31
                   else "h" if isinstance(v, int)
                   else "f" if isinstance(v, float)
                   else "b" if isinstance(v, (bytes, bytearray)) else "s")
        tags += tag
        if tag == "i":
            data += struct.pack(">i", int(v))
        elif tag == "h":
            data += struct.pack(">q", int(v))
        elif tag == "f":
            data += struct.pack(">f", float(v))
        elif tag == "d":
            data += struct.pack(">d", float(v))
        elif tag == "s":
            data += _pad(str(v).encode("utf-8"))
        elif tag == "b":
            v = bytes(v)
            data += struct.pack(">i", len(v)) + v + b"\0" * (-len(v) % 4)
        # T F N I carry no data
    return _pad(address.encode("utf-8")) + _pad(tags.encode()) + data


def _rd_str(b: bytes, i: int):
    j = b.index(b"\0", i)
    return b[i:j].decode("utf-8", "replace"), (j // 4 + 1) * 4


MAX_BUNDLE_DEPTH = 8


def decode(b: bytes, _depth: int = 0):
    """-> [(address, [args])]; bundles are flattened. Raises ValueError on junk."""
    try:
        if b.startswith(b"#bundle\0"):
            if _depth >= MAX_BUNDLE_DEPTH:     # a nesting bomb would blow the stack
                raise ValueError("OSC bundles nested too deep")
            out, i = [], 16
            while i + 4 <= len(b):
                n = struct.unpack(">i", b[i:i + 4])[0]
                i += 4
                out += decode(b[i:i + n], _depth + 1)
                i += n
            return out
        addr, i = _rd_str(b, 0)
        if not addr.startswith("/"):
            raise ValueError("not an OSC address")
        tags, i = _rd_str(b, i) if i < len(b) else (",", i)
        args = []
        for t in tags[1:]:
            if t == "i":
                args.append(struct.unpack(">i", b[i:i + 4])[0]); i += 4
            elif t == "f":
                args.append(struct.unpack(">f", b[i:i + 4])[0]); i += 4
            elif t == "h":
                args.append(struct.unpack(">q", b[i:i + 8])[0]); i += 8
            elif t == "d":
                args.append(struct.unpack(">d", b[i:i + 8])[0]); i += 8
            elif t in "sS":
                s, i = _rd_str(b, i); args.append(s)
            elif t == "b":
                n = struct.unpack(">i", b[i:i + 4])[0]; i += 4
                args.append(b[i:i + n]); i += (n + 3) // 4 * 4
            elif t in "TFNI":
                args.append({"T": True, "F": False, "N": None, "I": IMPULSE}[t])
            else:
                raise ValueError(f"unsupported OSC type '{t}'")
        return [(addr, args)]
    except (struct.error, IndexError) as e:
        raise ValueError(f"malformed OSC packet ({e})")


# ---------------------------------------------------------------- SLIP (TCP)
_END, _ESC, _ESC_END, _ESC_ESC = 0xC0, 0xDB, 0xDC, 0xDD


def slip_encode(packet: bytes) -> bytes:
    """Double-END framing (OSC 1.1): END before and after, so a receiver
    resyncs on the next packet even after line noise."""
    body = packet.replace(bytes([_ESC]), bytes([_ESC, _ESC_ESC])) \
                 .replace(bytes([_END]), bytes([_ESC, _ESC_END]))
    return bytes([_END]) + body + bytes([_END])


class SlipDecoder:
    MAX_FRAME = 65536                       # an OSC packet is never bigger

    def __init__(self):
        self._cur, self._esc, self._skip = bytearray(), False, False

    def feed(self, data: bytes):
        """-> complete packets found so far. An oversized frame is dropped
        (up to its END), so a peer that never ends a frame can't grow memory."""
        out = []
        for c in data:
            if c == _END and not self._esc:
                if self._cur and not self._skip:
                    out.append(bytes(self._cur))
                self._cur, self._skip = bytearray(), False
                continue
            if self._skip:
                continue
            if self._esc:
                self._cur.append(_END if c == _ESC_END else _ESC)
                self._esc = False
            elif c == _ESC:
                self._esc = True
            else:
                self._cur.append(c)
            if len(self._cur) > self.MAX_FRAME:
                self._cur, self._skip = bytearray(), True
        return out


# ---------------------------------------------------------------- patterns
MAX_WILDCARDS_PER_SEGMENT = 2   # more makes matching exponential (/{a}{b}{c}…)


def check_pattern(pattern: str):
    """Raise ValueError if an automation's OSC pattern is unusable."""
    _compile(pattern)


@lru_cache(maxsize=512)
def _compile(pattern: str):
    if not pattern.startswith("/"):
        raise ValueError("an OSC pattern starts with /")
    for seg in pattern.split("/"):
        if seg.count("*") + len(re.findall(r"\{[^},]*\}", seg)) > MAX_WILDCARDS_PER_SEGMENT:
            raise ValueError(f"too many wildcards in '{seg}' (at most "
                             f"{MAX_WILDCARDS_PER_SEGMENT} per part)")
    rx, names, i = "", [], 0
    while i < len(pattern):
        c = pattern[i]
        if c == "*":
            rx += "[^/]*"
        elif c == "?":
            rx += "[^/]"
        elif c == "[" and "]" in pattern[i:]:
            j = pattern.index("]", i)
            body = pattern[i + 1:j]
            neg = body.startswith("!")
            body = re.sub(r"[\\\]^]", lambda m: "\\" + m.group(0), body[1:] if neg else body)
            rx += "[" + ("^" if neg else "") + body + "]"
            i = j
        elif c == "{" and "}" in pattern[i:]:
            j = pattern.index("}", i)
            body = pattern[i + 1:j]
            if "," in body:
                rx += "(?:" + "|".join(re.escape(x) for x in body.split(",")) + ")"
            else:
                names.append(body)
                rx += "([^/]+)"
            i = j
        else:
            rx += re.escape(c)
        i += 1
    try:
        return re.compile(rx + r"\Z"), tuple(names)
    except re.error as e:
        raise ValueError(f"bad OSC pattern ({e})")


def match(pattern: str, address: str):
    """OSC 1.1 pattern match. -> None, or the captures as a dict keyed by
    name and by 1-based position ({"channel": "FOH", "1": "FOH", ...}).
    An unusable pattern matches nothing."""
    try:
        rx, names = _compile(pattern)
    except ValueError:
        return None
    m = rx.match(address)
    if not m:
        return None
    caps = {}
    for n, (name, val) in enumerate(zip(names, m.groups()), 1):
        caps[str(n)] = val
        if name:
            caps[name] = val
    return caps


# ---------------------------------------------------------------- templates
_TPL = re.compile(r"\{([\w.]+)\}")


def expand(text: str, ctx: dict) -> str:
    """Replace {name} with ctx[name]; unknown names are left as written."""
    return _TPL.sub(lambda m: str(ctx[m.group(1)]) if m.group(1) in ctx
                    else m.group(0), text or "")


def show(v) -> str:
    return "bang" if v is IMPULSE else "nil" if v is None else \
        v.hex() if isinstance(v, (bytes, bytearray)) else str(v)


def message_context(address: str, args, caps=None) -> dict:
    ctx = {"address": address,
           "value": show(args[0]) if args else "",
           "raw": " ".join([address] + [show(a) for a in args])}
    for n, a in enumerate(args, 1):
        ctx[f"arg{n}"] = show(a)
    ctx.update(caps or {})
    return ctx


def parse_args(text: str, ctx: dict = None):
    """Typed argument list from an automation's "arguments" field.
    Space-separated; "double quotes" keep spaces in one string. Plain
    numbers become int/float; true/false, nil, bang are T/F/N/I; force a
    type with a prefix: s:42  i:7  f:1  d:0.5  h:9  b:00ff (hex blob).
    {templates} are expanded inside each argument."""
    try:
        toks = shlex.split(text or "", posix=True)
    except ValueError:                         # unbalanced quote: plain split
        toks = (text or "").split()
    out = []
    for tok in toks:
        # The type prefix comes from the automation's own text, never from
        # an incoming value ("b:00ff" arriving over OSC stays a string).
        m = re.match(r"^([sifdhb]):(.*)\Z", tok, re.S)
        tok = expand(tok, ctx or {})
        if m:
            tag, v = m.group(1), expand(m.group(2), ctx or {})
            conv = {"s": str, "i": int, "f": float, "d": float, "h": int,
                    "b": bytes.fromhex}[tag]
            out.append(Typed(tag, conv(v)))
            continue
        low = tok.lower()
        if low in ("true", "false"):
            out.append(low == "true")
        elif low in ("nil", "null"):
            out.append(None)
        elif low in ("bang", "impulse"):
            out.append(IMPULSE)
        else:
            try:
                out.append(int(tok))
            except ValueError:
                try:
                    out.append(float(tok))
                except ValueError:
                    out.append(tok)
    return out


# ---------------------------------------------------------------- sending
class Sender:
    """UDP and TCP (SLIP) OSC sender. TCP connections are kept open per
    target and re-dialled once if they've dropped (QLab keeps them)."""

    NEG_TTL = 60.0            # don't re-resolve an unresolvable host for 60 s
    TIMEOUT = 2.0

    def __init__(self):
        self._ips = {}
        self._bad = {}
        self._tcp = {}
        self._lock = threading.Lock()
        self._udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _resolve(self, host: str) -> str:
        with self._lock:
            ip, failed_at = self._ips.get(host), self._bad.get(host)
        if ip:
            return ip
        if failed_at and time.time() - failed_at < self.NEG_TTL:
            raise OSError(f"can't resolve OSC host '{host}' (retrying in a minute)")
        # resolve OUTSIDE the lock: a slow lookup must not stall other sends
        try:
            ip = socket.gethostbyname(host)
        except OSError:
            with self._lock:
                self._bad[host] = time.time()
            raise
        with self._lock:
            self._ips[host] = ip
            self._bad.pop(host, None)
        return ip

    def send(self, host: str, port: int, address: str, args=(), proto="udp"):
        packet = encode(address, args)
        target = (self._resolve(host), int(port))
        if proto != "tcp":
            self._udp.sendto(packet, target)
            return
        data = slip_encode(packet)
        for attempt in (1, 2):
            with self._lock:
                s = self._tcp.get(target)
            try:
                if s is None:
                    s = socket.create_connection(target, timeout=self.TIMEOUT)
                    with self._lock:
                        self._tcp[target] = s
                s.sendall(data)
                return
            except OSError:
                with self._lock:
                    self._tcp.pop(target, None)
                try:
                    s and s.close()
                except OSError:
                    pass
                if attempt == 2:
                    raise


# ---------------------------------------------------------------- receiving
class Server:
    """Listens for OSC on one port over UDP, TCP (SLIP), or both. Calls
    handler(address, args, reply) per message (bundles unpacked);
    reply(address, args) answers the sender on the same transport."""

    def __init__(self, host: str, port: int, handler, proto: str = "both"):
        self.handler, self._socks, self._stop = handler, [], threading.Event()
        self._conns = set()                     # open TCP clients
        try:
            if proto in ("udp", "both"):
                u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                u.bind((host, port))
                self._socks.append(u)
                threading.Thread(target=self._udp_loop, args=(u,), daemon=True).start()
            if proto in ("tcp", "both"):
                t = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                t.bind((host, port))
                t.listen(8)
                self._socks.append(t)
                threading.Thread(target=self._tcp_accept, args=(t,), daemon=True).start()
        except OSError:
            self.close()
            raise

    def close(self):
        self._stop.set()
        for s in self._socks + list(self._conns):
            try:
                s.close()
            except OSError:
                pass
        self._socks = []

    MAX_REPLIES = 256                           # per incoming packet
    MAX_CONNS = 32                              # open TCP clients

    def _deliver(self, packet: bytes, reply):
        try:
            msgs = decode(packet)
        except Exception:
            return                              # not OSC: ignore
        # A spoofed UDP source must not get an amplified answer: one packet
        # (even a bundle of thousands of queries) gets a bounded number back.
        left = [self.MAX_REPLIES]

        def capped(a, args=()):
            if left[0] > 0:
                left[0] -= 1
                reply(a, args)
        for addr, args in msgs:
            try:
                self.handler(addr, args, capped)
            except Exception:
                pass                            # one bad command never stops the listener

    def _udp_loop(self, u):
        while not self._stop.is_set():
            try:
                data, src = u.recvfrom(65536)
            except ConnectionResetError:
                # Windows reports an earlier reply that hit a closed port
                # here (ICMP port unreachable) — not our listener's problem.
                continue
            except OSError:
                return
            self._deliver(data, lambda a, args=(), _s=src: u.sendto(encode(a, args), _s))

    def _tcp_accept(self, t):
        while not self._stop.is_set():
            try:
                conn, _ = t.accept()
            except OSError:
                return
            if len(self._conns) >= self.MAX_CONNS:
                conn.close()
                continue
            try:
                threading.Thread(target=self._tcp_conn, args=(conn,), daemon=True).start()
            except RuntimeError:                # can't start a thread: drop this one
                conn.close()

    def _tcp_conn(self, conn):
        dec, lock = SlipDecoder(), threading.Lock()

        def reply(a, args=()):
            with lock:
                conn.sendall(slip_encode(encode(a, args)))
        self._conns.add(conn)
        try:
            with conn:
                while not self._stop.is_set():
                    try:
                        data = conn.recv(65536)
                    except OSError:
                        return
                    if not data:
                        return
                    for packet in dec.feed(data):
                        self._deliver(packet, reply)
        finally:
            self._conns.discard(conn)
