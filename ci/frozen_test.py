"""Test the FROZEN Windows build (dist folder copied to a scratch dir, so
nothing a test run writes can end up in the shipped tree).

Usage: python frozen_test.py <copy of dist\\FraudCom> <expected version>

Checks:
  1. FraudCom.exe starts and /state reports the expected version.
  2. A DiGiCo console message sent through /automations/test arrives on a
     UDP listener with the right address and an OSC float tag.
  3. /midi/ports answers (python-rtmidi is bundled, so no error).
  4. Whisper self-test: the speech model loads inside the frozen app (proves
     ctranslate2 / PyAV DLLs are intact). Model downloaded for the test only.
  5. Network trap: with the model cached, a fresh launch makes 0 outbound
     connections (all proxies point at a logging listener).
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request

APP, WANT = sys.argv[1], sys.argv[2]
EXE = os.path.join(APP, "FraudCom.exe")
ROOT = os.path.dirname(os.path.abspath(APP))
FAILS = []


def check(cond, msg):
    print(("PASS  " if cond else "FAIL  ") + msg, flush=True)
    if not cond:
        FAILS.append(msg)


def launch(port, data, extra_env=None):
    env = dict(os.environ, FRAUDCOM_DATA=data, FRAUDCOM_PORT=str(port))
    env.update(extra_env or {})
    os.makedirs(data, exist_ok=True)
    return subprocess.Popen([EXE], env=env, cwd=APP)


def kill(p):
    subprocess.run(["taskkill", "/T", "/F", "/PID", str(p.pid)],
                   capture_output=True)
    try:
        p.wait(10)
    except Exception:
        pass


def get(port, path, timeout=3):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}",
                                timeout=timeout) as r:
        return json.loads(r.read())


def post(port, path, body):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                 "Origin": f"http://127.0.0.1:{port}"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def wait_state(port, secs=120):
    end = time.time() + secs
    while time.time() < end:
        try:
            return get(port, "/state")
        except Exception:
            time.sleep(1)
    return None


# ---------------------------------------------------------------- run 1
PORT = 18800
DATA = os.path.join(ROOT, "data1")
t0 = time.time()
p = launch(PORT, DATA, {"FRAUDCOM_WHISPER_SELFTEST": "1"})
st = wait_state(PORT)
check(st is not None, f"server came up ({time.time() - t0:.0f} s)")
if st:
    check(st.get("version") == WANT, f"/state version {st.get('version')} (want {WANT})")

    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.bind(("127.0.0.1", 0))
    rx.settimeout(5)
    addr = "/Input_Channels/12/mute"
    r = post(PORT, "/automations/test", {"host": "127.0.0.1",
             "port": rx.getsockname()[1], "address": addr, "args": "f:0"})
    try:
        pkt = rx.recv(65536)
    except socket.timeout:
        pkt = b""
    check(r.get("ok") and pkt.startswith(addr.encode()) and b",f\0" in pkt,
          f"DiGiCo OSC on the wire: {pkt[:40]!r}")

    m = get(PORT, "/midi/ports", timeout=10)
    check(isinstance(m.get("ports"), list) and not m.get("error"),
          f"/midi/ports with bundled rtmidi: {m}")

    sf = os.path.join(DATA, "whisper_selftest.json")
    end = time.time() + 600
    while time.time() < end and not os.path.exists(sf):
        time.sleep(3)
    res = json.load(open(sf)) if os.path.exists(sf) else {"ok": False, "error": "no result"}
    check(res.get("ok") is True, f"Whisper self-test in frozen app: {res}")
    # No second FraudCom may appear (a multiprocessing helper re-running the
    # app opened a second window before launcher.py called freeze_support).
    time.sleep(8)
    ps = subprocess.run(["powershell", "-NoProfile", "-Command",
        "Get-CimInstance Win32_Process -Filter \"Name='FraudCom.exe'\" | "
        "ForEach-Object { $_.ProcessId.ToString() + ' ' + $_.CommandLine }"],
        capture_output=True, text=True).stdout.strip().splitlines()
    ps = [l for l in ps if "--list-devices" not in l]
    check(len(ps) == 1, f"exactly one FraudCom running after Whisper loaded: {ps}")
kill(p)

# ---------------------------------------------------------------- run 2: network trap
hits = []
trap = socket.socket()
trap.bind(("127.0.0.1", 0))
trap.listen(16)
tport = trap.getsockname()[1]


def serve():
    while True:
        try:
            c, _ = trap.accept()
        except OSError:
            return
        c.settimeout(2)
        try:
            hits.append(c.recv(200).split(b"\r\n")[0].decode("latin-1"))
        except Exception:
            hits.append("(connection)")
        c.close()


threading.Thread(target=serve, daemon=True).start()
proxy = f"http://127.0.0.1:{tport}"
env = {k: proxy for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                          "http_proxy", "https_proxy", "all_proxy")}
env.update(NO_PROXY="", no_proxy="", FRAUDCOM_WHISPER_SELFTEST="1")
# reuse the whisper cache from run 1, in a fresh data dir
DATA2 = os.path.join(ROOT, "data2")
os.makedirs(DATA2, exist_ok=True)
if os.path.isdir(os.path.join(DATA, "whisper")):
    shutil.copytree(os.path.join(DATA, "whisper"), os.path.join(DATA2, "whisper"))
p = launch(PORT + 1, DATA2, env)
st = wait_state(PORT + 1)
check(st is not None, "server came up behind the proxy trap")
sf = os.path.join(DATA2, "whisper_selftest.json")
end = time.time() + 120
while time.time() < end and not os.path.exists(sf):
    time.sleep(2)
time.sleep(5)
kill(p)
res = json.load(open(sf)) if os.path.exists(sf) else {}
check(res.get("ok") is True, f"self-test offline (cached model): {res}")
check(not hits, f"outbound connections attempted: {hits}")

print(f"\n{'ALL PASSED' if not FAILS else str(len(FAILS)) + ' FAILED'}")
sys.exit(1 if FAILS else 0)
