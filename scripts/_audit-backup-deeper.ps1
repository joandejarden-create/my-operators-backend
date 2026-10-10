# READ-ONLY: deeper script/history search for leftover backup mechanisms
$ErrorActionPreference = 'Continue'

Write-Host '=== schtasks query Dealality/backup ==='
schtasks /Query /FO LIST /V 2>$null | Select-String -Pattern 'Dealality|Backup-Scripts|Nightly-Backups|Backup-Staging|dealality-backups' -Context 2,8

Write-Host ''
Write-Host '=== Logs in dealality-backups ==='
Get-ChildItem 'C:\Dev\dealality-backups\logs' -EA SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 15 Name, Length, LastWriteTime

Write-Host ''
Write-Host '=== Nightly-Backups Logs ==='
Get-ChildItem 'C:\Dev\Nightly-Backups\Logs' -EA SilentlyContinue | Select-Object -First 20 Name, Length, LastWriteTime
if (Test-Path 'C:\Dev\Nightly-Backups\Logs') {
  Get-ChildItem 'C:\Dev\Nightly-Backups\Logs' -File -EA SilentlyContinue | Select-Object -First 3 | ForEach-Object {
    Write-Host ("--- {0} head ---" -f $_.Name)
    Get-Content $_.FullName -TotalCount 40 -EA SilentlyContinue
  }
}

Write-Host ''
Write-Host '=== Search for old backup scripts by name under C:\Dev ==='
$names = @('*Backup*.ps1','*backup*.ps1','*Nightly*.ps1','*Staging*.ps1','*robocopy*.ps1','*robocopy*.bat')
foreach ($n in $names) {
  Get-ChildItem 'C:\Dev' -Recurse -Filter $n -File -EA SilentlyContinue |
    Where-Object { $_.FullName -notmatch '\\node_modules\\|\\\.git\\|\\rail-explore\\|\\matcha\\node_modules|\\tmp\\' } |
    Select-Object FullName, Length, LastWriteTime |
    ForEach-Object { Write-Host ("{0}`t{1}`t{2}" -f $_.LastWriteTime, $_.Length, $_.FullName) }
}

Write-Host ''
Write-Host '=== OneDrive Dealality Backups presence ==='
$od = Join-Path $env:USERPROFILE 'OneDrive\Dealality Backups'
Write-Host ("Path={0} Exists={1}" -f $od, (Test-Path $od))
if (Test-Path $od) {
  Get-ChildItem $od -Force -EA SilentlyContinue | Select-Object -First 20 Name, Mode, LastWriteTime | Format-Table -AutoSize
}

Write-Host ''
Write-Host '=== Google Drive Dealality Backups ==='
$gd = 'G:\My Drive\Dealality Backups'
Write-Host ("Exists={0}" -f (Test-Path $gd))
if (Test-Path $gd) {
  Get-ChildItem $gd -Force -EA SilentlyContinue | ForEach-Object {
    Write-Host ("{0} {1}" -f $_.Mode, $_.Name)
  }
}

Write-Host ''
Write-Host '=== Backup-Staging content shape (top of each stamp) ==='
foreach ($d in @('2026-08-22_0200','2026-10-01_0200')) {
  $p = "C:\Dev\Backup-Staging\$d"
  Write-Host "--- $d ---"
  if (Test-Path $p) {
    Get-ChildItem $p -Force | Select-Object Name, Mode | Format-Table -AutoSize
  }
}

Write-Host ''
Write-Host '=== Nightly snapshot shape ==='
$n = 'C:\Dev\Nightly-Backups\2026-07-30_1242'
if (Test-Path $n) {
  Get-ChildItem $n -Force | Select-Object Name, Mode | Format-Table -AutoSize
}

Write-Host ''
Write-Host '=== dealality-snapshots listing ==='
$s = 'C:\Dev\Backup-Scripts\dealality-snapshots'
if (Test-Path $s) {
  Get-ChildItem $s -Force | Format-Table Name, Mode, LastWriteTime -AutoSize
}
