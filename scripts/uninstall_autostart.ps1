# Gatelodge auto-start remover — deletes the "Gatelodge" scheduled
# task created by install_autostart.ps1. Does NOT stop a running
# gateway instance (the restart-loop wrapper exits when its console
# closes or the process is killed after task removal).
# Usage:  powershell -NoProfile -ExecutionPolicy Bypass -File scripts\uninstall_autostart.ps1
$ErrorActionPreference = "Stop"
$taskName = "Gatelodge"
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "Removed scheduled task '$taskName'."
} else {
    Write-Host "No scheduled task '$taskName' found."
}
