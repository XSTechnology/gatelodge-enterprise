# LLM Gateway auto-start installer (tier A service packaging).
# Creates a LOGON-trigger scheduled task "LLM Gateway" running as the
# CURRENT user (no elevation needed) that launches gateway_service.bat
# (restart-loop wrapper). ExecutionTimeLimit is disabled — the default
# 72-hour task kill would murder a long-running gateway.
#
# WHY LOGON, not AtStartup/SYSTEM (the assistant's pattern): the gateway
# resolves provider keys from the user's OS keyring at boot; a SYSTEM
# session cannot read the user's Credential Manager. The gateway serves
# user-session programs, so it starts when the session does.
#
# Usage:  powershell -NoProfile -ExecutionPolicy Bypass -File scripts\install_autostart.ps1
$ErrorActionPreference = "Stop"
$taskName = "LLM Gateway"
$repo = Split-Path -Parent $PSScriptRoot
$bat = Join-Path $repo "gateway_service.bat"
if (-not (Test-Path $bat)) { throw "gateway_service.bat not found: $bat" }

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument ("/c `"$bat`"")
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Force | Out-Null
Write-Host "Installed scheduled task '$taskName' (logon trigger, user: $env:USERNAME)."
Write-Host "Start it now with:  schtasks /Run /TN `"$taskName`""
Write-Host "Remove it with:    powershell -File scripts\uninstall_autostart.ps1"
