Write-Host 'ELIGIBILITY'
@(
  'C:\Dev\Nightly-Backups',
  'C:\Dev\Backup-Staging\2026-08-22_0200',
  'C:\Dev\Backup-Staging\2026-10-01_0200',
  'C:\Dev\Cursor-Recovery-Archive-2026-09-08',
  'C:\Dev\Backup-Scripts\dealality-snapshots'
) | ForEach-Object { Write-Host ("{0} = {1}" -f $_, (Test-Path -LiteralPath $_)) }

Write-Host 'LOCAL SECRETS'
$l = 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy'
@('.env', '.env.local', '.env.strix', 'matcha\.env', 'rail-explore\.env') | ForEach-Object {
  Write-Host ("{0} = {1}" -f ($_.Replace('\', '/')), (Test-Path -LiteralPath (Join-Path $l $_)))
}

Write-Host 'GDRIVE ARCHIVES'
Get-ChildItem 'G:\My Drive\Dealality Backups' -Directory | ForEach-Object { Write-Host $_.Name }
