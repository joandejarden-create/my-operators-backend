# Quick retention parse fix self-test + side manifest + old-backup eligibility (read-only)
$ErrorActionPreference = 'Stop'

[datetime]$parsed = [datetime]::MinValue
$ok = [datetime]::TryParseExact(
    '2026-10-01_2036',
    'yyyy-MM-dd_HHmm',
    [System.Globalization.CultureInfo]::InvariantCulture,
    [System.Globalization.DateTimeStyles]::None,
    [ref]$parsed
)
Write-Host "TryParseExact ok=$ok parsed=$parsed"

$side = 'G:\My Drive\Dealality Backups\2026-10-01_2036'
Write-Host '---SIDE FILES---'
Get-ChildItem -LiteralPath $side | Format-Table Name, Length -AutoSize
Write-Host '---CLOUD_SECRET_EXCLUSIONS.json---'
Get-Content (Join-Path $side 'CLOUD_SECRET_EXCLUSIONS.json') -Raw
Write-Host '---MANIFEST---'
Get-Content (Join-Path $side 'MANIFEST.txt') -Raw

# Dry-run retention candidates via fixed logic inline (DeleteEnabled=false)
$CloudRoot = 'G:\My Drive\Dealality Backups'
$DailyKeepDays = 14
$WeeklyKeepWeeks = 8
$now = Get-Date
$dailyCutoff = $now.AddDays(-$DailyKeepDays)
$weeklyCutoff = $now.AddDays(-7 * $WeeklyKeepWeeks)
$archives = @(Get-ChildItem -LiteralPath $CloudRoot -Directory -EA SilentlyContinue | Where-Object { $_.Name -match '^\d{4}-\d{2}-\d{2}_\d{4}$' } | Sort-Object Name -Descending)
$keep = New-Object 'System.Collections.Generic.HashSet[string]'
$weeklyKept = @{}
$wouldDelete = New-Object System.Collections.Generic.List[string]
foreach ($dir in $archives) {
    [datetime]$p = [datetime]::MinValue
    if (-not [datetime]::TryParseExact($dir.Name, 'yyyy-MM-dd_HHmm', [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::None, [ref]$p)) { continue }
    if ($p -ge $dailyCutoff) { [void]$keep.Add($dir.FullName); continue }
    if ($p -ge $weeklyCutoff) {
        $wk = '{0:0000}-W{1:00}' -f $p.Year, [int][System.Globalization.ISOWeek]::GetWeekOfYear($p)
        if (-not $weeklyKept.ContainsKey($wk)) { $weeklyKept[$wk] = $dir.FullName; [void]$keep.Add($dir.FullName) }
    }
}
foreach ($dir in $archives) {
    if (-not $keep.Contains($dir.FullName)) { $wouldDelete.Add($dir.FullName) }
}
Write-Host ("RETENTION archives={0} keep={1} dryRunDeleteCandidates={2} actualDeletes=0" -f $archives.Count, $keep.Count, $wouldDelete.Count)
$wouldDelete | ForEach-Object { Write-Host "  WOULD DELETE: $_" }

# Old backup eligibility (existence only — no move/delete)
$elig = [ordered]@{
  NightlyBackups = Test-Path 'C:\Dev\Nightly-Backups'
  Aug22Staging = Test-Path 'C:\Dev\Backup-Staging\2026-08-22_0200'
  Oct1Staging = Test-Path 'C:\Dev\Backup-Staging\2026-10-01_0200'
  CursorRecovery = Test-Path 'C:\Dev\Cursor-Recovery-Archive-2026-09-08'
  CrashSnapshots = Test-Path 'C:\Dev\Backup-Scripts\dealality-snapshots'
}
Write-Host '---ELIGIBILITY---'
$elig | ConvertTo-Json

# Confirm local secrets still present after backup (paths only)
$latest = 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy'
$secrets = @('.env','.env.local','.env.strix','matcha\.env','rail-explore\.env')
Write-Host '---LOCAL SECRETS---'
foreach ($s in $secrets) {
  $p = Join-Path $latest $s
  Write-Host ("{0} = {1}" -f ($s.Replace('\','/')), (Test-Path -LiteralPath $p))
}

# Confirm no backup roots deleted
Write-Host '---BACKUP ROOTS STILL PRESENT---'
@(
  'C:\Dev\dealality-backups',
  'C:\Dev\Backup-Staging',
  'C:\Dev\Nightly-Backups',
  'C:\Dev\Cursor-Recovery-Archive-2026-09-08',
  'C:\Dev\Backup-Scripts\dealality-snapshots'
) | ForEach-Object { Write-Host ("{0} = {1}" -f $_, (Test-Path $_)) }
