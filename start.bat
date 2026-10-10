@echo off
rem Gatelodge — serves http://127.0.0.1:8010/v1 (OpenAI-compatible)
rem Config: data\gateway_config.json (hot-reloadable). Models: data\models.
rem Auth: gateway_keys in the config (empty = open). Stop with Ctrl+C.
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo [gateway] .venv missing. Create it with:
    echo     py -3.13 -m venv .venv
    echo     uv sync
    exit /b 1
)
".venv\Scripts\python.exe" -m gateway
