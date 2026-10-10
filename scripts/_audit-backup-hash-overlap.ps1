# READ-ONLY: sample hash overlap between backup roots
$ErrorActionPreference = 'Continue'

function Sample-Hashes {
  param(
    [string]$Root,
    [string]$Label,
    [string[]]$RelPaths
  )
  $out = [ordered]@{ label = $Label; root = $Root; samples = [ordered]@{} }
  foreach ($rel in $RelPaths) {
    $full = Join-Path $Root $rel
    if (Test-Path -LiteralPath $full) {
      $h = Get-FileHash -LiteralPath $full -Algorithm SHA256
      $item = Get-Item -LiteralPath $full
      $out.samples[$rel] = [ordered]@{ present=$true; sha256=$h.Hash; bytes=$item.Length }
    } else {
      $out.samples[$rel] = [ordered]@{ present=$false }
    }
  }
  return $out
}

$rels = @(
  'deal-capture-proxy\package.json',
  'deal-capture-proxy\server.js',
  'deal-capture-proxy\AGENTS.md',
  'deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json',
  'deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\adp_cambridge_beaches_bermuda\2026-09\adp_mr_cambridge_beaches_bermud_2026-09_v1\report.pdf',
  'deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\adp_renaissance_times_square\2026-09\adp_mr_renaissance_times_square_2026-09_v8\review.json',
  'deal-capture-proxy\fixtures\ai-demand-positioning\monthly-review\cambridge-beaches-monthly-executive-review-v1.json',
  'deal-capture-proxy\public\capital-provider-explorer.html',
  'deal-capture-proxy\.env'
)

# Some roots nest differently
$roots = @(
  @{ Label='LATEST'; Root='C:\Dev\dealality-backups\LATEST'; Rels=$rels },
  @{ Label='Staging-Oct1'; Root='C:\Dev\Backup-Staging\2026-10-01_0200'; Rels=$rels },
  @{ Label='Staging-Aug22'; Root='C:\Dev\Backup-Staging\2026-08-22_0200'; Rels=$rels },
  @{ Label='Nightly-Jul30'; Root='C:\Dev\Nightly-Backups\2026-07-30_1242'; Rels=$rels },
  @{ Label='LiveSource'; Root='C:\Dev'; Rels=$rels }  # C:\Dev\deal-capture-proxy via rels
)

# Cursor recovery / snapshots may use different layout
$extraChecks = [ordered]@{}

# Does Nightly contain deal-capture-proxy?
$extraChecks.nightlyHasProject = Test-Path 'C:\Dev\Nightly-Backups\2026-07-30_1242\deal-capture-proxy'
$extraChecks.nightlyTop = @(Get-ChildItem 'C:\Dev\Nightly-Backups\2026-07-30_1242' -Force -EA SilentlyContinue | Select-Object -ExpandProperty Name)
$extraChecks.stagingOct1Top = @(Get-ChildItem 'C:\Dev\Backup-Staging\2026-10-01_0200' -Force -EA SilentlyContinue | Select-Object -ExpandProperty Name)
$extraChecks.stagingAug22Top = @(Get-ChildItem 'C:\Dev\Backup-Staging\2026-08-22_0200' -Force -EA SilentlyContinue | Select-Object -ExpandProperty Name)
$extraChecks.latestTop = @(Get-ChildItem 'C:\Dev\dealality-backups\LATEST' -Force -EA SilentlyContinue | Select-Object -ExpandProperty Name)

# unique probes
$extraChecks.oct1HasArchiveIndex = Test-Path 'C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json'
$extraChecks.aug22HasArchiveIndex = Test-Path 'C:\Dev\Backup-Staging\2026-08-22_0200\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json'
$extraChecks.nightlyHasArchiveIndex = Test-Path 'C:\Dev\Nightly-Backups\2026-07-30_1242\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json'
$extraChecks.latestHasArchiveIndex = Test-Path 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json'
$extraChecks.latestHasCursorWS = Test-Path 'C:\Dev\dealality-backups\LATEST\Cursor-workspaceStorage'
$extraChecks.oct1HasCursorWS = Test-Path 'C:\Dev\Backup-Staging\2026-10-01_0200\Cursor-workspaceStorage'
$extraChecks.aug22HasCursorWS = Test-Path 'C:\Dev\Backup-Staging\2026-08-22_0200\Cursor-workspaceStorage'
$extraChecks.nightlyHasCursorWS = Test-Path 'C:\Dev\Nightly-Backups\2026-07-30_1242\Cursor-workspaceStorage'

# Cursor recovery unique markers
$extraChecks.cursorRecoveryHasChats = Test-Path 'C:\Dev\Cursor-Recovery-Archive-2026-09-08\chats'
$extraChecks.cursorRecoveryHasWorkingState = Test-Path 'C:\Dev\Cursor-Recovery-Archive-2026-09-08\working-state-20260908-170517'
$extraChecks.cursorRecoveryInLatest = $false  # not expected

# Snapshot names
if (Test-Path 'C:\Dev\Backup-Scripts\dealality-snapshots') {
  $extraChecks.snapshotNames = @(Get-ChildItem 'C:\Dev\Backup-Scripts\dealality-snapshots' -Force -EA SilentlyContinue | Select-Object -ExpandProperty Name)
}

$samples = @()
foreach ($r in $roots) {
  $samples += Sample-Hashes -Root $r.Root -Label $r.Label -RelPaths $r.Rels
}

# Compare key hashes LATEST vs Staging Oct1 vs Live
function Get-HashOrNull($root, $rel) {
  $p = Join-Path $root $rel
  if (Test-Path -LiteralPath $p) { return (Get-FileHash -LiteralPath $p -Algorithm SHA256).Hash }
  return $null
}

$cmpRels = @(
  'deal-capture-proxy\server.js',
  'deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json',
  'deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\adp_cambridge_beaches_bermuda\2026-09\adp_mr_cambridge_beaches_bermud_2026-09_v1\report.pdf'
)
$comparisons = @()
foreach ($rel in $cmpRels) {
  $row = [ordered]@{
    rel = $rel
    live = Get-HashOrNull 'C:\Dev' $rel
    latest = Get-HashOrNull 'C:\Dev\dealality-backups\LATEST' $rel
    stagingOct1 = Get-HashOrNull 'C:\Dev\Backup-Staging\2026-10-01_0200' $rel
    stagingAug22 = Get-HashOrNull 'C:\Dev\Backup-Staging\2026-08-22_0200' $rel
    nightly = Get-HashOrNull 'C:\Dev\Nightly-Backups\2026-07-30_1242' $rel
  }
  $row.latestEqLive = ($row.latest -and $row.live -and ($row.latest -eq $row.live))
  $row.oct1EqLatest = ($row.stagingOct1 -and $row.latest -and ($row.stagingOct1 -eq $row.latest))
  $row.oct1EqLive = ($row.stagingOct1 -and $row.live -and ($row.stagingOct1 -eq $row.live))
  $comparisons += $row
}

# Count archive reviews per root where present
function Count-Reviews($archiveRoot) {
  if (-not (Test-Path $archiveRoot)) { return $null }
  return @(Get-ChildItem $archiveRoot -Recurse -Filter review.json -File -EA SilentlyContinue).Count
}
$archiveCounts = [ordered]@{
  live = Count-Reviews 'C:\Dev\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive'
  latest = Count-Reviews 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive'
  stagingOct1 = Count-Reviews 'C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive'
  stagingAug22 = Count-Reviews 'C:\Dev\Backup-Staging\2026-08-22_0200\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive'
  nightly = Count-Reviews 'C:\Dev\Nightly-Backups\2026-07-30_1242\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive'
}

$result = [ordered]@{
  generatedAt = (Get-Date).ToString('o')
  extraChecks = $extraChecks
  archiveReviewCounts = $archiveCounts
  comparisons = $comparisons
  samples = $samples
}

$out = 'C:\Dev\deal-capture-proxy\reports\backup-simplification-hash-overlap-20261001.json'
($result | ConvertTo-Json -Depth 8) | Set-Content -LiteralPath $out -Encoding UTF8
Write-Host "WROTE $out"
$result.extraChecks | ConvertTo-Json -Depth 5
Write-Host 'ARCHIVE COUNTS:'
$archiveCounts | ConvertTo-Json
Write-Host 'COMPARISONS:'
$comparisons | ConvertTo-Json -Depth 4
