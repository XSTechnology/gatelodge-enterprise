@echo off
REM Gatelodge full-stack startup — runs at logon (scheduled task
REM "Gatelodge Stack"). Idempotent: only starts what isn't already up.
REM
REM Order: Docker Desktop (2 min) → VM (parallel) → :3040 → :8434 → :8011
REM 8010 has its own task ("Gatelodge" → gateway_service.bat).

setlocal enabledelayedexpansion
set LOG=%USERPROFILE%\gatelodge-stack.log
set FORK=C:\Users\dlang\Workspace\gatelodge_enterprise
set VENV=%FORK%\.venv\Scripts\python.exe
set VMX=C:\Users\dlang\VMs\coolify-rehearsal\coolify-rehearsal.vmx
set VMRUN="C:\Program Files (x86)\VMware\VMware Workstation\vmrun.exe"

echo [%date% %time%] stack startup beginning >> "%LOG%"

REM ── 1. Docker Desktop (needed for the cloudflared tunnel container) ─────
tasklist /FI "IMAGENAME eq Docker Desktop.exe" 2>nul | find /i "Docker Desktop" >nul
if errorlevel 1 (
    echo [%time%] starting Docker Desktop >> "%LOG%"
    start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
) else (
    echo [%time%] Docker Desktop already running >> "%LOG%"
)

REM ── 2. VM (parallel — boots while Docker warms up) ──────────────────────
%VMRUN% -T ws list 2>nul | find /i "coolify-rehearsal.vmx" >nul
if errorlevel 1 (
    echo [%time%] powering on the rehearsal VM >> "%LOG%"
    start "" /min %VMRUN% -T ws start "%VMX%" gui
) else (
    echo [%time%] VM already running >> "%LOG%"
)

REM ── 3. :3040 site origin (production via tunnel) ────────────────────────
netstat -ano | findstr ":3040" | findstr "LISTENING" >nul
if errorlevel 1 (
    echo [%time%] starting :3040 site origin >> "%LOG%"
    start "gatelodge-site" /min cmd /c "cd /d %FORK%\site && %VENV% -m http.server 3040"
) else (
    echo [%time%] :3040 already up >> "%LOG%"
)

REM ── 4. :8434 anthropic wire stub (8011 claude backend) ──────────────────
netstat -ano | findstr ":8434" | findstr "LISTENING" >nul
if errorlevel 1 (
    echo [%time%] starting :8434 anthropic stub >> "%LOG%"
    start "gatelodge-stub" /min cmd /c "cd /d C:\Users\dlang\Workspace\gl_binary_test && %VENV% anthropic_stub.py 8434"
) else (
    echo [%time%] :8434 already up >> "%LOG%"
)

REM ── 5. :8011 enterprise standing instance ───────────────────────────────
netstat -ano | findstr ":8011" | findstr "LISTENING" >nul
if errorlevel 1 (
    echo [%time%] starting :8011 enterprise instance >> "%LOG%"
    start "gatelodge-8011" /min cmd /c "cd /d %FORK% && %VENV% enterprise_main.py"
) else (
    echo [%time%] :8011 already up >> "%LOG%"
)

REM ── 6. wait for Docker daemon, then verify the tunnel ───────────────────
echo [%time%] waiting for Docker daemon... >> "%LOG%"
set /a DOCKER_WAIT=0
:docker_loop
docker info >nul 2>&1
if not errorlevel 1 goto docker_ready
set /a DOCKER_WAIT+=1
if %DOCKER_WAIT% geq 24 (
    echo [%time%] WARNING: Docker daemon not ready after 4 min >> "%LOG%"
    goto :done
)
timeout /t 10 /nobreak >nul
goto docker_loop
:docker_ready
echo [%time%] Docker daemon ready >> "%LOG%"

REM verify cloudflared is running (restart policy should auto-start it)
docker ps --filter name=cloudflared --format "{{.Names}}" 2>nul | find /i "cloudflared" >nul
if errorlevel 1 (
    echo [%time%] starting cloudflared container >> "%LOG%"
    docker start cloudflared >nul 2>&1
) else (
    echo [%time%] cloudflared already up >> "%LOG%"
)

echo [%date% %time%] stack startup complete >> "%LOG%"

:done
endlocal
