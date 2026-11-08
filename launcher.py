"""
FraudCom.exe entry point.

Starts the transcriber server, opens the UI in an embedded window, and shuts
the server down when that window is closed. If a FraudCom server is already
running, it just opens a window onto it instead of starting a second one.
Anything ELSE holding the port is never adopted — FraudCom takes the next
free port.
"""

import multiprocessing
import os
import socket
import subprocess
import sys
import threading
import time

# MUST stay first. Python's multiprocessing starts helper processes (on a Mac,
# the resource tracker behind any multiprocessing lock, e.g. the one tqdm
# makes when Whisper loads) by re-running sys.executable, which in the built
# app IS FraudCom. Without this, that helper runs the whole launcher, finds
# the running FraudCom and opens a second window onto it. freeze_support()
# turns such a re-run into the helper it was meant to be, and exits.
multiprocessing.freeze_support()

# Device-enumeration subprocess mode: a fresh process gets a fresh
# PortAudio, so the server never has to re-init its own (Dante ASIO
# drivers tear down process-wide Winsock when unloaded — fatal for the
# web server).
if "--list-devices" in sys.argv:
    os.environ.setdefault("SD_ENABLE_ASIO", "1")
    import json as _json
    import sounddevice as _sd
    _out = []
    for _idx, _dev in enumerate(_sd.query_devices()):
        if _dev["max_input_channels"] > 0:
            _out.append({
                "index": _idx,
                "name": _dev["name"],
                "hostapi": _sd.query_hostapis(_dev["hostapi"])["name"],
                "max_input_channels": _dev["max_input_channels"],
                "default_samplerate": _dev["default_samplerate"],
            })
    print(_json.dumps(_out))
    sys.exit(0)


def data_dir() -> str:
    if os.environ.get("FRAUDCOM_DATA"):
        return os.environ["FRAUDCOM_DATA"]
    if sys.platform == "darwin":
        return os.path.expanduser("~/Library/Application Support/FraudCom")
    return os.path.join(
        os.environ.get("LOCALAPPDATA") or os.path.expanduser("~"), "FraudCom")


def _untranslocated(app_path: str) -> str:
    """A quarantined, ad-hoc-signed app opened in place runs from a random
    read-only copy (.../AppTranslocation/...) holding ONLY the .app. Ask
    Security.framework where the real bundle is, so the FULL package's
    model/ and whisper/ beside it can still be found."""
    if "/AppTranslocation/" not in app_path:
        return app_path
    try:
        import ctypes
        from ctypes import c_bool, c_char_p, c_long, c_void_p
        cf = ctypes.cdll.LoadLibrary(
            "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation")
        sec = ctypes.cdll.LoadLibrary(
            "/System/Library/Frameworks/Security.framework/Security")
        cf.CFURLCreateFromFileSystemRepresentation.restype = c_void_p
        cf.CFURLCreateFromFileSystemRepresentation.argtypes = [
            c_void_p, c_char_p, c_long, c_bool]
        cf.CFURLGetFileSystemRepresentation.restype = c_bool
        cf.CFURLGetFileSystemRepresentation.argtypes = [
            c_void_p, c_bool, c_char_p, c_long]
        cf.CFRelease.argtypes = [c_void_p]
        sec.SecTranslocateCreateOriginalPathForURL.restype = c_void_p
        sec.SecTranslocateCreateOriginalPathForURL.argtypes = [c_void_p, c_void_p]
        raw = app_path.encode("utf-8")
        url = cf.CFURLCreateFromFileSystemRepresentation(None, raw, len(raw), True)
        if not url:
            return app_path
        orig = sec.SecTranslocateCreateOriginalPathForURL(url, None)
        cf.CFRelease(url)
        if not orig:
            return app_path
        buf = ctypes.create_string_buffer(4096)
        ok = cf.CFURLGetFileSystemRepresentation(orig, True, buf, len(buf))
        cf.CFRelease(orig)
        return buf.value.decode("utf-8") if ok and buf.value else app_path
    except Exception:
        return app_path


def mac_models_dir() -> str:
    """macOS: model/ and whisper/ live OUTSIDE the .app, in the data dir, so
    an UPDATE (a new FraudCom.app alone) keeps them. The FULL package ships
    them beside FraudCom.app; the first launch copies them in."""
    import shutil
    dst = os.path.join(data_dir(), "models")
    os.makedirs(dst, exist_ok=True)
    app_path = os.path.normpath(os.path.join(
        os.path.dirname(sys.executable), "..", ".."))       # .../FraudCom.app
    src = os.path.dirname(_untranslocated(app_path))         # beside the .app
    for name in ("model", "whisper"):
        d = os.path.join(dst, name)
        s = os.path.join(src, name)
        if not os.path.isdir(d) and os.path.isdir(s):
            # Per-process temp name: two launches at once can't delete each
            # other's half-copy. symlinks=True: copy links as links, never
            # the files they point at (the FULL folder is user-supplied).
            tmp = f"{d}.partial-{os.getpid()}"
            shutil.rmtree(tmp, ignore_errors=True)
            shutil.copytree(s, tmp, symlinks=True)
            try:
                os.replace(tmp, d)    # never leave a half-copied model
            except OSError:           # another launch finished first
                shutil.rmtree(tmp, ignore_errors=True)
    if not os.path.isdir(os.path.join(dst, "model")):
        # Never run silently without models: say exactly what to do.
        subprocess.run(["osascript", "-e",
            'display alert "FraudCom can\'t find its speech models" message '
            '"Keep FraudCom.app, model and whisper together in the FraudCom '
            'folder from the FULL download, then open FraudCom.app again." '
            'as critical'], check=False)
        sys.exit(1)
    return dst


def base_dir() -> str:
    """Where model/ and whisper/ are found (app.py resolves them relative to
    the working directory)."""
    if getattr(sys, "frozen", False):
        if sys.platform == "darwin":
            return mac_models_dir()
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


os.chdir(base_dir())

# --noconsole builds have no stdout/stderr; send them to a log file so server
# errors are still diagnosable. Windows: next to the exe if writable, else the
# data dir (an install in a read-only folder must still start). macOS: ONLY the
# data dir — writing inside the signed .app would break its signature.
if getattr(sys, "frozen", False) and (sys.stderr is None or sys.stdout is None):
    _log = None
    _cands = (data_dir(),) if sys.platform == "darwin" else (
        os.path.dirname(sys.executable), data_dir())
    for _d in _cands:
        try:
            os.makedirs(_d, exist_ok=True)
            _p = os.path.join(_d, "fraudcom.log")
            if os.path.exists(_p) and os.path.getsize(_p) > 2_000_000:
                os.replace(_p, _p + ".old")          # simple 2 MB rotation
            _log = open(_p, "a", buffering=1, encoding="utf-8", errors="replace")
            break
        except OSError:
            continue
    sys.stdout = sys.stderr = _log or open(os.devnull, "w")

# Always-on crash trace: a native crash (e.g. inside the audio driver) kills
# the process before any Python error handling runs. faulthandler writes every
# thread's Python stack to crash.log in the data folder at that moment.
try:
    import faulthandler
    os.makedirs(data_dir(), exist_ok=True)
    _crash = os.path.join(data_dir(), "crash.log")
    if os.path.exists(_crash) and os.path.getsize(_crash) > 1_000_000:
        os.replace(_crash, _crash + ".old")
    _crash_fh = open(_crash, "a", buffering=1, encoding="utf-8")
    _crash_fh.write(f"--- start {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    faulthandler.enable(file=_crash_fh, all_threads=True)
except Exception:
    pass


def startup_failed(err_text: str):
    """Leave a readable trace of a startup failure and tell the user."""
    path = os.path.join(data_dir(), "startup_error.txt")
    try:
        os.makedirs(data_dir(), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"--- {time.strftime('%Y-%m-%d %H:%M:%S')}\n{err_text}\n")
    except OSError:
        pass
    msg = ("FraudCom could not start. Details were saved to:\n" + path)
    try:
        if sys.platform == "darwin":
            subprocess.run(["osascript", "-e",
                            f'display alert "FraudCom" message {msg!r}'.replace("'", '"')],
                           timeout=120)
        elif sys.platform == "win32":
            import ctypes
            ctypes.windll.user32.MessageBoxW(None, msg, "FraudCom", 0x10)
    except Exception:
        pass


# Same variables app.py reads (FRAUDCOM_-prefixed, never a global HOST/PORT).
HOST = os.environ.get("FRAUDCOM_HOST", "0.0.0.0")
PREFERRED_PORT = int(os.environ.get("FRAUDCOM_PORT", "8765"))
URL = f"http://127.0.0.1:{PREFERRED_PORT}"


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def port_usable(port: int) -> bool:
    """Free for us: nothing answers there AND we can actually bind it. (A port
    in a Windows reserved range answers nothing yet refuses to bind.)"""
    if port_open(port):
        return False
    with socket.socket() as s:
        try:
            s.bind((HOST, port))
            return True
        except OSError:
            return False


def is_fraudcom(port) -> bool:
    """True only if a FraudCom server answers on this port. Merely "something
    is listening" is not enough: another local web server on 8765 once made
    FraudCom open a window onto that server instead of itself."""
    try:
        import json
        import urllib.request
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/state",
                                    timeout=3) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
        return isinstance(d, dict) and "version" in d and "running" in d
    except Exception:
        return False


def choose_port(preferred: int):
    """-> (port, attach). attach=True: a FraudCom already serves there, so be a
    window onto it. Otherwise the first usable port from `preferred` upward,
    or an OS-assigned free port if that whole range is taken — never a port
    that something else answers on."""
    for port in range(preferred, preferred + 20):
        if port_usable(port):
            return port, False
        if is_fraudcom(port):
            return port, True
    with socket.socket() as s:
        s.bind((HOST, 0))
        return s.getsockname()[1], False


def find_browser():
    for c in (
        os.path.expandvars(r"%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ):
        if os.path.exists(c):
            return c
    return None


def open_webview() -> bool:
    """Show the UI in an embedded WebView2 window owned by THIS process —
    so the taskbar shows FraudCom's own icon, not Edge's. Blocks until
    the window is closed. Returns False if webview isn't available."""
    try:
        import webview
    except Exception:
        return False
    storage = os.path.join(data_dir(), "webview-profile")
    menu = []
    if sys.platform == "darwin":
        # macOS: Settings… in the app menu, where Mac users look for it
        # (⌘, itself is handled by the page). Windows keeps no menu bar.
        from webview.menu import Menu, MenuAction
        menu = [Menu("__app__", [MenuAction(
            "Settings…", lambda: win.evaluate_js("showPrefs()"))])]
    win = webview.create_window("FraudCom", URL, width=1280, height=860,
                                menu=menu)
    vox = sys.modules.get("app")     # absent when attached to a running FraudCom
    if vox is not None:
        vox.WINDOW["win"] = win      # OSC window/show and window/hide
    try:
        webview.start(private_mode=False, storage_path=storage)
    except TypeError:                     # older pywebview signature
        webview.start()
    return True


def open_app_window():
    """Fallback: open the UI in an Edge/Chrome app-mode window. A dedicated
    profile dir forces a new browser process, so the Popen's lifetime ==
    the window's lifetime."""
    exe = find_browser()
    if not exe:
        import webbrowser
        webbrowser.open(URL)
        return None
    profile = os.path.join(data_dir(), "browser-profile")
    os.makedirs(profile, exist_ok=True)
    return subprocess.Popen([
        exe, f"--app={URL}", f"--user-data-dir={profile}",
        "--no-first-run", "--no-default-browser-check",
        # This profile is only a shell for the app UI: never sign it into
        # the Windows account, never sync, never show account promos.
        "--disable-sync",
        "--disable-features=msEdgeSidebarV2,msImplicitSignin,"
        "msSeamlessWebToBrowserSignIn,msSyncConsentExperiment",
    ])


def show_window() -> bool:
    """Open the UI and block until it closes. Returns False only for the last
    fallback (a plain browser tab), which can't be waited on."""
    if open_webview():                 # blocks until the window closes
        return True
    win = open_app_window()
    if win is None:
        return False
    win.wait()
    return True


def start_server(vox, port):
    """Start uvicorn on `port`; return (server, thread) once it is really
    serving, or (None, None) if it never came up (e.g. bind refused)."""
    import uvicorn
    server = uvicorn.Server(uvicorn.Config(
        vox.app, host=HOST, port=port, log_level="warning"))
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    deadline = time.time() + 15
    while time.time() < deadline and t.is_alive() and not server.started:
        time.sleep(0.1)
    if server.started:
        return server, t
    server.should_exit = True
    return None, None


def main():
    global URL
    port, attach = choose_port(PREFERRED_PORT)
    URL = f"http://127.0.0.1:{port}"
    if attach:
        # A FraudCom server is already up — just be a window onto it. The
        # engine is NOT imported here: loading the audio stack in a second
        # process mid-show would re-enumerate the ASIO drivers the running
        # instance is using.
        show_window()
        return

    os.environ["FRAUDCOM_PORT"] = str(port)   # app.py's LAN URL / QR use it
    import app as vox                         # sets SD_ENABLE_ASIO before sounddevice

    server = t = None
    for _ in range(3):                        # a port can be lost in a race
        vox.PORT = port
        URL = f"http://127.0.0.1:{port}"
        server, t = start_server(vox, port)
        if server:
            break
        port, _ = choose_port(port + 1)
    if server is None:
        print("FraudCom: could not start the local server", file=sys.stderr)
        return

    if not show_window():
        t.join()               # untracked fallback browser: run until killed
        return

    try:
        vox.engine.stop()
        vox.tlog.flush()       # every pending transcript line, before exit
    except Exception:
        pass
    server.should_exit = True
    t.join(timeout=8)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException:
        import traceback
        startup_failed(traceback.format_exc())
        raise
