# Quick LATEST freshness vs live for ADP archive + mtimes
$ErrorActionPreference = 'Continue'
$liveIdx = 'C:\Dev\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json'
$latIdx = 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json'
Write-Host ("live index exists={0}" -f (Test-Path $liveIdx))
Write-Host ("latest index exists={0}" -f (Test-Path $latIdx))
if ((Test-Path $liveIdx) -and (Test-Path $latIdx)) {
  $hl = (Get-FileHash $liveIdx -Algorithm SHA256).Hash
  $ha = (Get-FileHash $latIdx -Algorithm SHA256).Hash
  Write-Host ("index hash match live==latest: {0}" -f ($hl -eq $ha))
  Write-Host ("live reviews={0}" -f @(Get-ChildItem (Split-Path $liveIdx) -Recurse -Filter review.json -EA SilentlyContinue).Count)
  Write-Host ("latest reviews={0}" -f @(Get-ChildItem (Split-Path $latIdx) -Recurse -Filter review.json -EA SilentlyContinue).Count)
}
$livePkg = Get-Item 'C:\Dev\deal-capture-proxy\package.json'
$latPkg = Get-Item 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\package.json' -EA SilentlyContinue
Write-Host ("live package.json mtime={0}" -f $livePkg.LastWriteTime)
if ($latPkg) { Write-Host ("latest package.json mtime={0}" -f $latPkg.LastWriteTime) }
$report = Get-Content 'C:\Dev\dealality-backups\LATEST\BACKUP_REPORT.txt' -EA SilentlyContinue
if ($report) { $report | Select-Object -First 25 }
Write-Host '---DRIVE 2036---'
Get-ChildItem 'G:\My Drive\Dealality Backups\2026-10-01_2036' -EA SilentlyContinue | Format-Table Name, Length -AutoSize
