# Verify refreshed LATEST (no secret values printed)
$ErrorActionPreference = 'Stop'
$latest = 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy'
$report = [ordered]@{}

if (-not (Test-Path $latest)) { throw "LATEST missing: $latest" }

$files = Get-ChildItem -LiteralPath $latest -Recurse -Force -File -ErrorAction SilentlyContinue
$report.fileCount = $files.Count
$report.sizeBytes = ($files | Measure-Object -Property Length -Sum).Sum
$report.sizeGB = [math]::Round($report.sizeBytes / 1GB, 2)

$archiveRoot = Join-Path $latest 'reports\ai-demand-positioning\monthly-review\archive'
$indexPath = Join-Path $archiveRoot 'index.json'
$report.archiveIndexPresent = Test-Path -LiteralPath $indexPath
$report.reviewJsonCount = @(Get-ChildItem -LiteralPath $archiveRoot -Recurse -Filter 'review.json' -File -EA SilentlyContinue).Count
$report.pdfCount = @(Get-ChildItem -LiteralPath $archiveRoot -Recurse -Filter '*.pdf' -File -EA SilentlyContinue).Count
$report.coverCount = @(Get-ChildItem -LiteralPath $archiveRoot -Recurse -Filter '*cover*' -File -EA SilentlyContinue).Count
$report.actionRegisterCount = @(Get-ChildItem -LiteralPath $archiveRoot -Recurse -Filter '*action*register*' -File -EA SilentlyContinue).Count

# Known artifacts (presence only)
$patterns = @(
  @{ name = 'CambridgeBeaches'; pattern = '*Cambridge*Beaches*' },
  @{ name = 'NOHO'; pattern = '*NOHO*' },
  @{ name = 'HotelIntelligence'; pattern = '*Hotel*Intelligence*' }
)
$artifactHits = [ordered]@{}
foreach ($p in $patterns) {
  $hits = @(Get-ChildItem -LiteralPath $latest -Recurse -Force -File -Filter $p.pattern -EA SilentlyContinue | Select-Object -First 5)
  $artifactHits[$p.name] = @{ present = ($hits.Count -gt 0); sample = @($hits | ForEach-Object { $_.FullName.Substring($latest.Length + 1) }) }
}
$report.artifacts = $artifactHits

# Secrets present yes/no (no values)
$secretRel = @(
  '.env',
  '.env.local',
  '.env.strix',
  'matcha\.env',
  'rail-explore\.env'
)
$secrets = [ordered]@{}
foreach ($rel in $secretRel) {
  $p = Join-Path $latest $rel
  $secrets[$rel.Replace('\', '/')] = Test-Path -LiteralPath $p
}
$report.localSecretsPresent = $secrets

# Sample modified/untracked markers if present in live source listing via common paths
$report.sampleTracked = Test-Path (Join-Path $latest 'server.js')
$report.samplePackageJson = Test-Path (Join-Path $latest 'package.json')

$outPath = 'C:\Dev\deal-capture-proxy\reports\google-drive-cutover-latest-verify-20261001.json'
$report | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $outPath -Encoding UTF8
Write-Host "WROTE $outPath"
$report | ConvertTo-Json -Depth 6
