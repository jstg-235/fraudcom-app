"""Anonymity + residue gate for the built Windows app folder.

Usage: python anon_scan.py <dist\\FraudCom>   (run with the build venv's
python, which has PyInstaller for reading the exe's embedded archive)

Scans every file's bytes, and every module inside FraudCom.exe's archive
(decompressed), case-insensitively for identifying strings. Positive
controls must be found (proves the scan sees compressed code and the UI);
a negative control (a planted string) must be flagged.

Upstream exception: some prebuilt wheels (numpy, PyAV, pydantic_core,
tokenizers) were themselves built on GitHub runners and carry that
builder's paths. A hit is accepted ONLY when the shipped file is
byte-identical to the file pip installed from the wheel (or, for a module in
the exe's archive, when the installed source already contains the string) —
i.e. it was not produced by this build. Everything else fails the gate.
"""
import os
import sys
import tempfile
import zlib

from PyInstaller.archive.readers import CArchiveReader, ZlibArchiveReader

ROOT = sys.argv[1]
SITE = os.path.join(sys.prefix, "Lib", "site-packages")
# The identifying words are NOT in this file (the repository may be public):
# they come from the ANON_NEEDLES repository secret, comma-separated.
NEEDLES = [w.strip().lower().encode() for w in
           os.environ.get("ANON_NEEDLES", "").split(",") if w.strip()]
if len(NEEDLES) < 2:
    sys.exit("ANON GATE: FAIL - the ANON_NEEDLES secret is missing or empty")
for _n in NEEDLES:                      # hide each word in the public CI log
    print("::add-mask::" + _n.decode(), flush=True)
# positive controls: launcher code, app code, the UI, this build's version
# (read from the shipped UI's changelog source, app.py's APP_VERSION) and
# the newest feature's code.
import re as _re
_ver = _re.search(rb'APP_VERSION = "([^"]+)"',
                  open(os.path.join(os.path.dirname(__file__), "..", "app.py"), "rb").read()).group(1)
CONTROLS = [b"choose_port", b"midi_from_api", b"recall_macro", _ver.lower(),
            b"parse_pcconfig", b"freeze_support"]
RESIDUE = (".log", "whisper_selftest.json")
RESIDUE_DIRS = ("webview-profile", "browser-profile")


def blobs_of_exe(path):
    """Yield (name, bytes) for every entry in the exe's CArchive, expanding
    the PYZ into its modules."""
    car = CArchiveReader(path)
    for name in car.toc:
        try:
            data = car.extract(name)
        except Exception:
            continue
        if data is None:
            continue
        yield f"exe:{name}", data
        if name.endswith("PYZ.pyz") or data[:4] == b"PYZ\0":
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pyz") as t:
                t.write(data)
            try:
                z = ZlibArchiveReader(t.name)
                for mod in z.toc:
                    try:
                        raw = z.extract(mod, raw=True)
                    except TypeError:
                        raw = z.extract(mod)
                    if not isinstance(raw, (bytes, bytearray)):
                        import marshal
                        raw = marshal.dumps(raw)
                    try:
                        raw = zlib.decompress(raw)
                    except Exception:
                        pass
                    yield f"pyz:{mod}", bytes(raw)
            finally:
                os.unlink(t.name)


def scan(root, extra=None):
    hits, found, residue, n = {}, set(), [], 0
    blobs = []
    for d, dirs, files in os.walk(root):
        for x in dirs:
            if x in RESIDUE_DIRS or (x == "__pycache__" and d.endswith(".dist-info")):
                residue.append(os.path.join(d, x))
        for f in files:
            p = os.path.join(d, f)
            if f.endswith(RESIDUE) or f.startswith(".partial"):
                residue.append(p)
            with open(p, "rb") as fh:
                blobs.append((p, fh.read()))
            if f.lower() == "fraudcom.exe":
                blobs.extend(blobs_of_exe(p))
    if extra:
        blobs.append(extra)
    for name, data in blobs:
        n += 1
        low = data.lower()
        for c in CONTROLS:
            if c in low:
                found.add(c)
        for k in NEEDLES:
            if k in low:
                hits.setdefault(k.decode(), []).append(name)
    return hits, found, residue, n


def upstream(name, needle):
    """True if this hit came verbatim from an installed wheel."""
    if name.startswith("pyz:"):
        mod = name[4:].replace(".", os.sep)
        for cand in (mod + ".py", os.path.join(mod, "__init__.py")):
            p = os.path.join(SITE, cand)
            if os.path.isfile(p):
                with open(p, "rb") as f:
                    return needle.encode() in f.read().lower()
        return False
    internal = os.path.join(ROOT, "_internal")
    if not os.path.abspath(name).startswith(os.path.abspath(internal)):
        return False
    p = os.path.join(SITE, os.path.relpath(name, internal))
    if not os.path.isfile(p):
        return False
    with open(name, "rb") as a, open(p, "rb") as b:
        return a.read() == b.read()


hits, found, residue, n = scan(ROOT)
print(f"scanned {n} files/modules")
for k in list(hits):
    up = [w for w in hits[k] if upstream(w, k)]
    for w in up:
        print(f"  upstream (byte-identical to the installed wheel) word #{[n.decode() for n in NEEDLES].index(k) + 1}: {w}")
    hits[k] = [w for w in hits[k] if w not in up]
    if not hits[k]:
        del hits[k]
ok = True
for c in CONTROLS:
    print(f"  positive control {c.decode():16s}: {'found' if c in found else 'MISSING'}")
    ok &= c in found
for k, where in hits.items():
    print(f"  HIT word #{[n.decode() for n in NEEDLES].index(k) + 1}: {where[:5]}")
ok &= not hits
for r in residue:
    print(f"  RESIDUE {r}")
ok &= not residue
# negative control: a planted identifying string must be caught
_plant = b" ... ".join(n.upper() for n in NEEDLES)
neg, _, _, _ = scan(tempfile.mkdtemp(), extra=("planted", b"... " + _plant + b" ..."))
caught = all(n.decode() in neg for n in NEEDLES)
print(f"  negative control (planted strings) flagged: {caught}")
ok &= caught
print("ANON GATE: " + ("PASS" if ok else "FAIL"))
sys.exit(0 if ok else 1)
