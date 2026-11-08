# FraudCom (multichannel voice transcriber) - dev launcher (PowerShell)
# Run from a PowerShell prompt:  .\run.ps1
# (If blocked by execution policy, either double-click run.bat instead,
#  or run:  powershell -ExecutionPolicy Bypass -File .\run.ps1 )

Set-Location -Path $PSScriptRoot

$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $py)) {
    Write-Host "[!] Virtual environment not found at .venv" -ForegroundColor Yellow
    Write-Host "    Create it with:  python -m venv .venv; .venv\Scripts\pip install -r requirements.txt"
    Read-Host "Press Enter to exit"
    exit 1
}

if (-not (Test-Path (Join-Path $PSScriptRoot "model\conf"))) {
    Write-Host "[!] Vosk model not found in .\model" -ForegroundColor Yellow
    Write-Host "    Download one from https://alphacephei.com/vosk/models and unzip it"
    Write-Host "    so that .\model\conf, .\model\am, etc. exist."
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Starting FraudCom..." -ForegroundColor Cyan
Write-Host "Open http://localhost:8765  (Ctrl+C here to stop)`n"

& $py app.py
