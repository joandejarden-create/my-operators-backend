# READ-ONLY quick: Google Drive folders + OneDrive + script OneDrive refs + snapshot sizes
$ErrorActionPreference = 'Continue'

Write-Host '=== GDrive Dealality Backups ==='
Get-ChildItem 'G:\My Drive\Dealality Backups' -Force -EA SilentlyContinue | ForEach-Object {
  Write-Host ("{0}`t{1}" -f $_.Mode, $_.Name)
  if ($_.PSIsContainer -and $_.Name -ne '2026-10-01_2036') {
    $kids = @(Get-ChildItem $_.FullName -Force -EA SilentlyContinue | Select-Object -First 8)
    foreach ($k in $kids) { Write-Host ("  - {0} {1} len={2}" -f $k.Mode, $k.Name, $k.Length) }
    if ($kids.Count -eq 0) { Write-Host '  (empty or still syncing)' }
  }
}

Write-Host '=== OneDrive Dealality Backups ==='
$od = Join-Path $env:USERPROFILE 'OneDrive\Dealality Backups'
Write-Host ("exists={0}" -f (Test-Path $od))
if (Test-Path $od) {
  Get-ChildItem $od -Force -EA SilentlyContinue | Select-Object -First 15 Name, Mode, LastWriteTime | Format-Table -AutoSize
}

Write-Host '=== Backup-Dealality OneDrive / Staging / Nightly string refs ==='
Select-String -Path 'C:\Dev\Backup-Scripts\Backup-Dealality.ps1' -Pattern 'OneDrive|Backup-Staging|Nightly-Backups|dealality-snapshots' |
  ForEach-Object { Write-Host ("L{0}: {1}" -f $_.LineNumber, $_.Line.Trim()) }

Write-Host '=== Snapshot folder sizes ==='
Get-ChildItem 'C:\Dev\Backup-Scripts\dealality-snapshots' -Force -EA SilentlyContinue | ForEach-Object {
  $files = @(Get-ChildItem $_.FullName -Recurse -File -Force -EA SilentlyContinue)
  $bytes = ($files | Measure-Object Length -Sum).Sum
  if (-not $bytes) { $bytes = 0 }
  Write-Host ("{0}`tfiles={1}`tGB={2:N2}" -f $_.Name, $files.Count, ($bytes/1GB))
}

Write-Host '=== Active backup script count under Backup-Scripts (ps1 only, exclude snaps) ==='
Get-ChildItem 'C:\Dev\Backup-Scripts' -File -Filter *.ps1 -EA SilentlyContinue | Format-Table Name, Length, LastWriteTime -AutoSize

Write-Host '=== Confirm no second Dealality task ==='
@(Get-ScheduledTask | Where-Object { $_.TaskName -match 'Dealality' }) | ForEach-Object {
  Write-Host ("TASK {0} Enabled={1} State={2}" -f $_.TaskName, $_.Settings.Enabled, $_.State)
}
