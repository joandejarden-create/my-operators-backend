# Wait for ZIP 2327 / backup process to finish
$ErrorActionPreference = 'Continue'
$term = 'C:\Users\joand\.cursor\projects\c-Dev-deal-capture-proxy\terminals\677077.txt'
$log = 'C:\Dev\dealality-backups\logs\Dealality-2026-10-01_2327.log'
$zip = 'G:\My Drive\Dealality Backups\2026-10-01_2327\Dealality-backup.zip'

for ($i = 1; $i -le 60; $i++) {
  Start-Sleep -Seconds 60
  $done = $false
  if (Test-Path $term) {
    $done = ((Get-Content $term -Tail 12 -EA SilentlyContinue) -join "`n") -match 'exit_code:'
  }
  # Also detect completion via log
  $logTail = if (Test-Path $log) { (Get-Content $log -Tail 30 -Encoding UTF8 -EA SilentlyContinue) -join "`n" } else { '' }
  $logDone = $logTail -match 'DEALALITY BACKUP REPORT|Google Drive ZIP size|BACKUP_EXIT'
  $packGone = -not (Test-Path 'C:\Dev\dealality-backups\_CLOUD_PACK_TEMP')
  $len = if (Test-Path $zip) { (Get-Item $zip).Length } else { 0 }
  $root = (Get-ChildItem 'C:\Dev\dealality-backups' -Force -EA SilentlyContinue | Select-Object -ExpandProperty Name) -join ','
  Write-Host ("[{0}/60] zipGB={1:N2} root=[{2}] packGone={3} doneTerm={4} doneLog={5}" -f $i, ($len/1GB), $root, $packGone, $done, $logDone)
  if ($done -or ($logDone -and $packGone)) {
    Write-Host '=== COMPLETE ==='
    if (Test-Path $term) { Get-Content $term -Tail 45 }
    Get-Content $log -Tail 70 -Encoding UTF8
    break
  }
}
