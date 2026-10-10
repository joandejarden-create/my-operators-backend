# Cutover runner - invokes Backup-Dealality.ps1 then verifies LATEST + Google Drive ZIP
$ErrorActionPreference = "Continue"
$env:DEALALITY_BACKUP_RETENTION_DELETE = $null
$env:DEALALITY_BACKUP_DRY_RUN = $null
$env:DEALALITY_BACKUP_SKIP_CLOUD = $null
$env:DEALALITY_BACKUP_CLOUD_FROM_LATEST = $null
Remove-Item Env:DEALALITY_BACKUP_RETENTION_DELETE -ErrorAction SilentlyContinue
Remove-Item Env:DEALALITY_BACKUP_DRY_RUN -ErrorAction SilentlyContinue
Remove-Item Env:DEALALITY_BACKUP_SKIP_CLOUD -ErrorAction SilentlyContinue
Remove-Item Env:DEALALITY_BACKUP_CLOUD_FROM_LATEST -ErrorAction SilentlyContinue

$out = "C:\Dev\deal-capture-proxy\reports\_gdrive-cutover-run.log"
"=== CUTOVER START $(Get-Date -Format o) ===" | Set-Content $out -Encoding UTF8
"FreeGB=$(([math]::Round((Get-PSDrive C).Free/1GB,2)))" | Add-Content $out
"LiveArchive=$(Test-Path 'C:\Dev\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json')" | Add-Content $out

Write-Host "Starting Backup-Dealality.ps1 (full local + Google Drive)..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Dev\Backup-Scripts\Backup-Dealality.ps1" *>> $out
$code = $LASTEXITCODE
"=== BACKUP EXIT=$code $(Get-Date -Format o) ===" | Add-Content $out
Write-Host "BACKUP_EXIT=$code"
exit $code
