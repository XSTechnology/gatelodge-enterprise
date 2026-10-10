@echo off
rem llm_router_enterprise — serves http://127.0.0.1:8011/v1 (core defaults
rem until the enterprise seams are wired; see enterprise_main.py).
rem Config: data\gateway_config.json. Stop with Ctrl+C.
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo [enterprise] .venv missing. Create it with:
    echo     uv sync
    exit /b 1
)
".venv\Scripts\python.exe" enterprise_main.py
