# Poll backup until done
$logDir = 'C:\Dev\dealality-backups\logs'
$term = 'C:\Users\joand\.cursor\projects\c-Dev-deal-capture-proxy\terminals\677077.txt'
$max = 90
for ($i = 1; $i -le $max; $i++) {
  Start-Sleep -Seconds 60
  $logs = @(Get-ChildItem $logDir -Filter 'Dealality-*.log' -EA SilentlyContinue | Sort-Object LastWriteTime -Descending)
  $latestLog = if ($logs) { $logs[0].FullName } else { $null }
  $termTail = Get-Content $term -Tail 8 -EA SilentlyContinue
  $done = ($termTail -join "`n") -match 'exit_code:'
  $zipDirs = @(Get-ChildItem 'G:\My Drive\Dealality Backups' -Directory -EA SilentlyContinue | Where-Object { $_.Name -match '^\d{4}-\d{2}-\d{2}_\d{4}$' } | Sort-Object Name -Descending)
  $newest = if ($zipDirs) { $zipDirs[0].Name } else { 'none' }
  $zipPath = if ($zipDirs) { Join-Path $zipDirs[0].FullName 'Dealality-backup.zip' } else { $null }
  $zipLen = if ($zipPath -and (Test-Path $zipPath)) { (Get-Item $zipPath).Length } else { 0 }
  $rootKids = (Get-ChildItem 'C:\Dev\dealality-backups' -Force -EA SilentlyContinue | Select-Object -ExpandProperty Name) -join ','
  Write-Host ("[{0}/{1}] log={2} root=[{3}] newestDrive={4} zipGB={5:N2} done={6}" -f $i, $max, $(if($latestLog){Split-Path $latestLog -Leaf}else{'n/a'}), $rootKids, $newest, ($zipLen/1GB), $done)
  if ($done) {
    Write-Host '=== TERM TAIL ==='
    Get-Content $term -Tail 50
    if ($latestLog) {
      Write-Host '=== LOG TAIL ==='
      Get-Content $latestLog -Tail 50 -Encoding UTF8
    }
    break
  }
}
