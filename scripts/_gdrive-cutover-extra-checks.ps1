# Extra LATEST checks: covers outside archive/, recovered review, fixtures
$ErrorActionPreference = 'Continue'
$root = 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy'
$out = [ordered]@{}

$covers = @(Get-ChildItem -LiteralPath (Join-Path $root 'reports\ai-demand-positioning\monthly-review\pdf-pages') -Recurse -Filter 'cover.png' -File -EA SilentlyContinue)
$out.coverPngCount = $covers.Count
$out.coverSamples = @($covers | Select-Object -First 5 | ForEach-Object { $_.FullName.Substring($root.Length+1) })

$actionReg = @(Get-ChildItem -LiteralPath (Join-Path $root 'fixtures\ai-demand-positioning\monthly-review') -Filter '*action*register*' -File -EA SilentlyContinue)
$out.actionRegisterFixtures = @($actionReg | ForEach-Object { $_.Name })

# Recovered previously-missing (not Cambridge/NOHO golden-only) e.g. Renaissance
$rena = Join-Path $root 'reports\ai-demand-positioning\monthly-review\archive\adp_renaissance_times_square'
$out.renaissanceArchivePresent = Test-Path $rena
$out.renaissanceReviewJson = @(Get-ChildItem $rena -Recurse -Filter review.json -EA SilentlyContinue).Count
$out.renaissancePdfs = @(Get-ChildItem $rena -Recurse -Filter *.pdf -EA SilentlyContinue).Count

$water = Join-Path $root 'reports\ai-demand-positioning\monthly-review\archive\adp_waterstone_boca_raton'
$out.waterstonePresent = Test-Path $water
$out.waterstoneReviews = @(Get-ChildItem $water -Recurse -Filter review.json -EA SilentlyContinue).Count

# Golden fixtures
$out.cambridgeFixture = Test-Path (Join-Path $root 'fixtures\ai-demand-positioning\monthly-review\cambridge-beaches-monthly-executive-review-v1.json')
$out.nohoFixture = Test-Path (Join-Path $root 'fixtures\ai-demand-positioning\monthly-review\now-now-noho-monthly-executive-review-v1.json')
$out.cambridgePdfFixture = Test-Path (Join-Path $root 'reports\ai-demand-positioning\monthly-review\pdf\cambridge-beaches-monthly-executive-review-v1.pdf')

# Modified tracked sample (server.js) + untracked
$out.serverJs = Test-Path (Join-Path $root 'server.js')
$out.untrackedCapital = Test-Path (Join-Path $root 'public\capital-provider-explorer.html')

$path = 'C:\Dev\deal-capture-proxy\reports\google-drive-cutover-extra-checks-20261001.json'
($out | ConvertTo-Json -Depth 5) | Set-Content $path -Encoding UTF8
Write-Host "WROTE $path"
$out | ConvertTo-Json -Depth 5

Write-Host '---WAIT POLL---'
Get-Content 'C:\Users\joand\.cursor\projects\c-Dev-deal-capture-proxy\terminals\677068.txt' -Tail 25
