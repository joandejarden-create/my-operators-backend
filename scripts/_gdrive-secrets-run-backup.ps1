# Run full local LATEST refresh + Google Drive ZIP (retention deletes OFF)
$ErrorActionPreference = 'Stop'
Remove-Item Env:DEALALITY_BACKUP_RETENTION_DELETE -ErrorAction SilentlyContinue
Remove-Item Env:DEALALITY_BACKUP_DRY_RUN -ErrorAction SilentlyContinue
Remove-Item Env:DEALALITY_BACKUP_SKIP_CLOUD -ErrorAction SilentlyContinue
Remove-Item Env:DEALALITY_BACKUP_CLOUD_FROM_LATEST -ErrorAction SilentlyContinue
Remove-Item Env:DEALALITY_BACKUP_TEST_FAIL_VERIFY -ErrorAction SilentlyContinue

Write-Host 'Starting Backup-Dealality.ps1 (local refresh + Google Drive ZIP, secrets cloud-excluded)...'
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Dev\Backup-Scripts\Backup-Dealality.ps1'
$code = $LASTEXITCODE
Write-Host ("BACKUP_EXIT={0}" -f $code)
exit $code
