# Extended wait for backup 2327
$term = 'C:\Users\joand\.cursor\projects\c-Dev-deal-capture-proxy\terminals\677077.txt'
$log = 'C:\Dev\dealality-backups\logs\Dealality-2026-10-01_2327.log'
$zip = 'G:\My Drive\Dealality Backups\2026-10-01_2327\Dealality-backup.zip'
for ($i = 1; $i -le 40; $i++) {
  Start-Sleep -Seconds 60
  $done = ((Get-Content $term -Tail 8 -EA SilentlyContinue) -join "`n") -match 'exit_code:'
  $len = if (Test-Path $zip) { (Get-Item $zip).Length } else { 0 }
  $root = (Get-ChildItem 'C:\Dev\dealality-backups' -Force | Select-Object -ExpandProperty Name) -join ','
  Write-Host ("[{0}/40] zipGB={1:N2} root=[{2}] done={3}" -f $i, ($len/1GB), $root, $done)
  if ($done) {
    Write-Host '=== DONE ==='
    Get-Content $term -Tail 40
    Get-Content $log -Tail 60 -Encoding UTF8
    break
  }
}
