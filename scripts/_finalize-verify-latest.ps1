# Verify refreshed LATEST ADP + secrets (no secret values)
$ErrorActionPreference = 'Continue'
$latest = 'C:\Dev\dealality-backups\LATEST'
$proj = Join-Path $latest 'deal-capture-proxy'
$archive = Join-Path $proj 'reports\ai-demand-positioning\monthly-review\archive'
$out = [ordered]@{}

$files = @(Get-ChildItem -LiteralPath $latest -Recurse -Force -File -EA SilentlyContinue)
$out.fileCount = $files.Count
$out.sizeGB = [math]::Round((($files | Measure-Object Length -Sum).Sum / 1GB), 2)
$out.indexPresent = Test-Path (Join-Path $archive 'index.json')
$out.reviewJson = @(Get-ChildItem $archive -Recurse -Filter review.json -File -EA SilentlyContinue).Count
$out.pdfs = @(Get-ChildItem $archive -Recurse -Filter *.pdf -File -EA SilentlyContinue).Count
$out.covers = @(Get-ChildItem (Join-Path $proj 'reports\ai-demand-positioning\monthly-review\pdf-pages') -Recurse -Filter cover.png -File -EA SilentlyContinue).Count
$out.cambridge = (Test-Path (Join-Path $archive 'adp_cambridge_beaches_bermuda'))
$out.noho = (Test-Path (Join-Path $archive 'adp_now_now_noho'))
$out.renaissance = (Test-Path (Join-Path $archive 'adp_renaissance_times_square'))
$out.renaissanceReviews = @(Get-ChildItem (Join-Path $archive 'adp_renaissance_times_square') -Recurse -Filter review.json -EA SilentlyContinue).Count
$out.renaissancePdfs = @(Get-ChildItem (Join-Path $archive 'adp_renaissance_times_square') -Recurse -Filter *.pdf -EA SilentlyContinue).Count
$out.waterstone = (Test-Path (Join-Path $archive 'adp_waterstone_boca_raton'))
$out.serverJs = Test-Path (Join-Path $proj 'server.js')
$out.untrackedCapital = Test-Path (Join-Path $proj 'public\capital-provider-explorer.html')
$out.hotelIntel = Test-Path (Join-Path $proj 'hotel-intelligence-batches')
$out.cursorWS = Test-Path (Join-Path $latest 'Cursor-workspaceStorage')

$secrets = [ordered]@{}
foreach ($s in @('.env','.env.local','.env.strix','matcha\.env','rail-explore\.env')) {
  $secrets[$s.Replace('\','/')] = Test-Path -LiteralPath (Join-Path $proj $s)
}
$out.localSecrets = $secrets
$out.allSecretsPresent = (@($secrets.Values) -notcontains $false)

$path = 'C:\Dev\deal-capture-proxy\reports\nightly-gdrive-latest-verify-20261002.json'
($out | ConvertTo-Json -Depth 5) | Set-Content $path -Encoding UTF8
Write-Host "WROTE $path"
$out | ConvertTo-Json -Depth 5
