# READ-ONLY: size inventory of backup roots
$ErrorActionPreference = 'Continue'

function Get-RootStats {
  param([string]$Path, [string]$Label)
  if (-not (Test-Path -LiteralPath $Path)) {
    Write-Host ("{0}: MISSING ({1})" -f $Label, $Path)
    return [pscustomobject]@{ Label=$Label; Path=$Path; Exists=$false; Files=0; SizeGB=0; SizeBytes=0 }
  }
  Write-Host ("Scanning {0} ..." -f $Label)
  $files = @(Get-ChildItem -LiteralPath $Path -Recurse -Force -File -EA SilentlyContinue)
  $bytes = ($files | Measure-Object Length -Sum).Sum
  if (-not $bytes) { $bytes = 0 }
  $obj = [pscustomobject]@{
    Label = $Label
    Path = $Path
    Exists = $true
    Files = $files.Count
    SizeBytes = [int64]$bytes
    SizeGB = [math]::Round($bytes / 1GB, 2)
    TopChildren = @(Get-ChildItem -LiteralPath $Path -Force -EA SilentlyContinue | Select-Object -First 20 Name, Mode, LastWriteTime)
  }
  Write-Host ("{0}: files={1} size={2} GB" -f $Label, $obj.Files, $obj.SizeGB)
  return $obj
}

$results = @()
$results += Get-RootStats 'C:\Dev\dealality-backups' 'dealality-backups'
$results += Get-RootStats 'C:\Dev\Backup-Staging' 'Backup-Staging'
$results += Get-RootStats 'C:\Dev\Nightly-Backups' 'Nightly-Backups'
$results += Get-RootStats 'C:\Dev\Cursor-Recovery-Archive-2026-09-08' 'Cursor-Recovery'
$results += Get-RootStats 'C:\Dev\Backup-Scripts\dealality-snapshots' 'Backup-Scripts-snapshots'
$results += Get-RootStats 'C:\Dev\Backup-Scripts' 'Backup-Scripts-ALL'

# Staging children separately
foreach ($child in @('2026-08-22_0200','2026-10-01_0200')) {
  $results += Get-RootStats ("C:\Dev\Backup-Staging\{0}" -f $child) ("Staging-$child")
}
$results += Get-RootStats 'C:\Dev\Nightly-Backups\2026-07-30_1242' 'Nightly-2026-07-30'
$results += Get-RootStats 'C:\Dev\dealality-backups\LATEST' 'LATEST'

$out = 'C:\Dev\deal-capture-proxy\reports\backup-simplification-sizes-20261001.json'
$results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $out -Encoding UTF8
Write-Host "WROTE $out"
$results | Select-Object Label, Exists, Files, SizeGB, Path | Format-Table -AutoSize
