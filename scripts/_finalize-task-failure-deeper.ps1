# Deeper task-failure evidence
$ErrorActionPreference = 'Continue'
Write-Host '=== All Dealality logs Oct1 morning ==='
Get-ChildItem 'C:\Dev\dealality-backups\logs' -File | Where-Object { $_.Name -match '2026-10-01' } | Sort-Object LastWriteTime | Format-Table Name, Length, LastWriteTime -AutoSize

Write-Host '=== OneDrive leftover from early Oct1 ==='
$od = Join-Path $env:USERPROFILE 'OneDrive\Dealality Backups'
if (Test-Path $od) {
  Get-ChildItem $od -Force | Format-Table Name, LastWriteTime -AutoSize
}

Write-Host '=== STATUS_CONTROL_C_EXIT note ==='
Write-Host '3221225786 = 0xC000013A = STATUS_CONTROL_C_EXIT'
Write-Host 'Task LogonType=Interactive, WakeToRun=False'
Write-Host 'System log shows repeated Wake from sleep ~03:12-03:18 on 2026-10-01'
Write-Host 'Staging 2026-10-01_0200 created 02:00:03 — indicates nightly job started then'
Write-Host 'No dealality-backups log for _0200 — either old script path wrote Staging, or process died before/during local log lifecycle'

# Check git history / script for when Staging was removed - can't easily. Check 2036 log for OneDrive mentions.
Write-Host '=== Sleep/hibernate events broader window ==='
try {
  Get-WinEvent -FilterHashtable @{ LogName='System'; StartTime=[datetime]'2026-10-01 01:55'; EndTime=[datetime]'2026-10-01 04:00' } -MaxEvents 80 -ErrorAction SilentlyContinue |
    Where-Object { $_.Id -in 42,107,1,506,507,109 -or $_.ProviderName -match 'Kernel-Power|Power-Troubleshooter' } |
    Select-Object -First 25 TimeCreated, Id, ProviderName |
    Format-Table -AutoSize
} catch { Write-Host $_.Exception.Message }

Write-Host '=== ZIP progress ==='
Get-Item 'G:\My Drive\Dealality Backups\2026-10-01_2327\Dealality-backup.zip' -EA SilentlyContinue | Format-List Length, LastWriteTime
Get-ChildItem 'C:\Dev\dealality-backups' -Force | Select-Object Name
Get-Content 'C:\Users\joand\.cursor\projects\c-Dev-deal-capture-proxy\terminals\677084.txt' -Tail 15 -EA SilentlyContinue
