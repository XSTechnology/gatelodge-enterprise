@echo off
rem Gatelodge service wrapper — restart loop with 5s backoff.
rem
rem Installed by scripts\install_autostart.ps1 as a LOGON-trigger
rem scheduled task running as the current user. Logon (not AtStartup/
rem SYSTEM) on purpose: the gateway resolves provider keys from the
rem USER's OS keyring (Credential Manager is per-user; a SYSTEM session
rem cannot decrypt it), and its consumers are user-session programs.
rem The gateway's own request log lives in data\gateway-log-*.jsonl —
rem this file only captures console/boot output.
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo [gateway-service] .venv missing - create it with: uv sync
    exit /b 1
)
if not exist "data" mkdir data
:loop
".venv\Scripts\python.exe" -m gateway >> data\gateway-service.log 2>&1
echo [%date% %time%] gateway exited (%ERRORLEVEL%) - restarting in 5s >> data\gateway-service.log
timeout /t 5 /nobreak >nul
goto loop
