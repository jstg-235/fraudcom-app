@echo off
REM FraudCom (multichannel voice transcriber) - dev launcher
REM Double-click this file, or run it from a terminal.

cd /d "%~dp0"

REM If a server is already listening on 8765, don't start a second one.
netstat -ano | findstr /r /c:":8765 .*LISTENING" >nul
if %errorlevel%==0 (
    echo Already running - opening http://localhost:8765
    start http://localhost:8765
    exit /b 0
)

if not exist ".venv\Scripts\python.exe" (
    echo [!] Virtual environment not found at .venv
    echo     Create it with:  python -m venv .venv ^&^& .venv\Scripts\pip install -r requirements.txt
    pause
    exit /b 1
)

if not exist "model\conf" (
    echo [!] Vosk model not found in .\model
    echo     Download one from https://alphacephei.com/vosk/models and unzip it
    echo     so that .\model\conf, .\model\am, etc. exist.
    pause
    exit /b 1
)

echo Starting FraudCom...
echo Open http://localhost:8765  (Ctrl+C here to stop)
echo.
".venv\Scripts\python.exe" app.py

REM Keep the window open if the server exits/crashes so errors are readable.
echo.
echo Server stopped.
pause
