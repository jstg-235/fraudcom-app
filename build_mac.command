#!/bin/bash
# Build FraudCom.app for macOS (Apple Silicon) -> dist/FraudCom-mac_v<ver>.zip
#
# ANONYMOUS BUILD. Every rule below exists so nothing in the shipped app
# points at whoever built it:
#   - ad-hoc signature only (codesign -s -): never a Developer ID, never
#     notarized (both embed the signer's name and team ID)
#   - neutral bundle ID, empty copyright
#   - built from /Users/Shared/fcbuild (no username in any embedded path)
#   - dist-info __pycache__ removed, extended attributes stripped, zipped
#     without resource forks / xattrs / ACLs
#
# Expects in $BUILD: .venv314 (python.org 3.14 framework build + deps) and
# models/model (Vosk en-us-0.22-lgraph) + models/whisper (faster-whisper
# small.en HF cache).
#
# Output (dist/):
#   FraudCom-Mac_v<ver>-FULL.zip    FraudCom/{FraudCom.app, model, whisper, README.txt}
#   FraudCom-Mac_v<ver>-UPDATE.zip  FraudCom.app only
# Models stay OUTSIDE the .app: on first launch launcher.py copies them from
# beside the .app into ~/Library/Application Support/FraudCom/models, so an
# UPDATE (new .app alone) keeps them.
set -euo pipefail
SRC="$(cd "$(dirname "$0")" && pwd)"
BUILD="${BUILD:-/Users/Shared/fcbuild}"
PY="$BUILD/.venv314/bin/python"
BUNDLE_ID="org.fraudcom.FraudCom"

case "$BUILD" in /Users/Shared/*|/tmp/*|/private/tmp/*) ;; *)
  echo "BUILD FAILED -> build dir must not contain a username: $BUILD" >&2; exit 1;; esac
[ -x "$PY" ] || { echo "BUILD FAILED -> no venv at $BUILD/.venv314" >&2; exit 1; }
for m in model whisper; do
  [ -d "$BUILD/models/$m" ] || { echo "BUILD FAILED -> missing $BUILD/models/$m" >&2; exit 1; }
done

# Clean-room copy of the source: only what the app needs.
W="$BUILD/src"
rm -rf "$W" "$BUILD/build" "$BUILD/dist"
mkdir -p "$W"
cp "$SRC/app.py" "$SRC/launcher.py" "$SRC/osclink.py" "$SRC/remote_api.py" "$W/"
cp -R "$SRC/static" "$W/static"
VER="$("$PY" -c "import re;print(re.search(r'APP_VERSION = \"([^\"]+)\"', open('$W/app.py').read()).group(1))")"
echo "FraudCom $VER"

# Icon: .ico (PC) -> .icns
ICONSET="$BUILD/FraudCom.iconset"
rm -rf "$ICONSET"; mkdir -p "$ICONSET"
sips -s format png "$SRC/fraudcom.ico" --out "$BUILD/icon.png" >/dev/null
for s in 16 32 128 256 512; do
  sips -z $s $s "$BUILD/icon.png" --out "$ICONSET/icon_${s}x${s}.png" >/dev/null
  sips -z $((s*2)) $((s*2)) "$BUILD/icon.png" --out "$ICONSET/icon_${s}x${s}@2x.png" >/dev/null
done
iconutil -c icns "$ICONSET" -o "$BUILD/FraudCom.icns"

cd "$BUILD"
# Same --collect-all set as the Windows build, minus pythonnet/clr_loader
# (WinForms backend); pywebview uses Cocoa/WKWebView via pyobjc here.
"$BUILD/.venv314/bin/pyinstaller" --noconfirm --windowed --onedir \
  --name FraudCom --target-arch arm64 --osx-bundle-identifier "$BUNDLE_ID" \
  --icon "$BUILD/FraudCom.icns" \
  --distpath "$BUILD/dist" --workpath "$BUILD/build" --specpath "$BUILD/build" \
  --add-data "$W/static:static" \
  --collect-all vosk --collect-all sounddevice \
  --collect-all faster_whisper --collect-all ctranslate2 --collect-all av \
  --collect-all tokenizers --collect-all webview \
  --collect-all qrcode \
  --exclude-module tkinter --exclude-module _tkinter \
  --exclude-module hf_xet --exclude-module watchfiles \
  "$W/launcher.py"

APP="$BUILD/dist/FraudCom.app"
[ -d "$APP" ] || { echo "BUILD FAILED -> $APP not produced" >&2; exit 1; }

PL="$APP/Contents/Info.plist"
pb() { /usr/libexec/PlistBuddy -c "Delete :$1" "$PL" 2>/dev/null || true
       /usr/libexec/PlistBuddy -c "Add :$1 $2 $3" "$PL"; }
pb CFBundleShortVersionString string "$VER"
pb CFBundleVersion string "$VER"
pb NSHumanReadableCopyright string ""
pb NSMicrophoneUsageDescription string "FraudCom listens to your audio inputs to transcribe them."
pb NSLocalNetworkUsageDescription string "FraudCom shares its live feed with viewers on your network and sends OSC to show systems."
pb LSMinimumSystemVersion string "11.0"

# Never imported; can carry the build path (PyAV's AUTHORS).
find "$APP" -path '*.dist-info/*' -name __pycache__ -type d -prune -exec rm -rf {} +
xattr -cr "$APP"
codesign --force --deep -s - "$APP"
codesign --verify --deep --strict "$APP"

Z="--norsrc --noextattr --noacl"
readme() { sed "s/{VER}/$VER/" "$SRC/README-mac.txt" > "$1/README.txt"; }
FULL="$BUILD/dist/FraudCom-Mac_v$VER-FULL.zip"
UPD="$BUILD/dist/FraudCom-Mac_v$VER-UPDATE.zip"

# FULL: FraudCom/{FraudCom.app, model, whisper, README.txt}
PKG="$BUILD/dist/pkg/FraudCom"
mkdir -p "$PKG"
ditto $Z "$APP" "$PKG/FraudCom.app"
ditto $Z "$BUILD/models/model" "$PKG/model"
ditto $Z "$BUILD/models/whisper" "$PKG/whisper"
readme "$PKG"
xattr -cr "$BUILD/dist/pkg"
(cd "$BUILD/dist/pkg" && ditto -c -k $Z --keepParent FraudCom "$FULL")
rm -rf "$BUILD/dist/pkg"

# UPDATE: FraudCom-Update/{FraudCom.app, README.txt}
PKG="$BUILD/dist/pkg/FraudCom-Update"
mkdir -p "$PKG"
ditto $Z "$APP" "$PKG/FraudCom.app"
readme "$PKG"
xattr -cr "$BUILD/dist/pkg"
(cd "$BUILD/dist/pkg" && ditto -c -k $Z --keepParent FraudCom-Update "$UPD")
rm -rf "$BUILD/dist/pkg"
echo "Built -> $FULL ($(du -h "$FULL" | cut -f1))"
echo "Built -> $UPD ($(du -h "$UPD" | cut -f1))"
