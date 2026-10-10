# Verify Google Drive ZIP integrity + hash checks vs LATEST (no secret values)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression.FileSystem

$zipPath = 'G:\My Drive\Dealality Backups\2026-10-01_2036\Dealality-backup.zip'
$latestRoot = 'C:\Dev\dealality-backups\LATEST'
$extractProbe = 'C:\Dev\dealality-backups\_ZIP_VERIFY_PROBE'
$report = [ordered]@{}

$zi = Get-Item -LiteralPath $zipPath
$report.zipPath = $zi.FullName
$report.zipBytes = $zi.Length
$report.zipGB = [math]::Round($zi.Length / 1GB, 2)

# Side files
$sideDir = Split-Path $zipPath -Parent
$report.sideManifest = Test-Path (Join-Path $sideDir 'MANIFEST.txt')
$report.sideSecretExclusions = Test-Path (Join-Path $sideDir 'CLOUD_SECRET_EXCLUSIONS.json')
if ($report.sideSecretExclusions) {
  $report.sideSecretExclusionsBody = Get-Content (Join-Path $sideDir 'CLOUD_SECRET_EXCLUSIONS.json') -Raw | ConvertFrom-Json
}

Write-Host 'Opening ZIP...'
$zip = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
try {
  $entries = @($zip.Entries)
  $report.entryCount = $entries.Count
  $names = @($entries | ForEach-Object { $_.FullName.Replace('\','/') })

  # .env presence (must be ZERO live secrets; .env.example also should be excluded by current policy)
  $envEntries = @($names | Where-Object { $_ -match '(^|/)\.env($|\.)' -or $_ -match '(^|/)\.env$' })
  $report.envEntriesInZip = $envEntries
  $report.envPresent = ($envEntries.Count -gt 0)

  $report.hasCloudSecretExclusionsInZip = ($names -contains 'CLOUD_SECRET_EXCLUSIONS.json') -or ($names | Where-Object { $_ -like '*/CLOUD_SECRET_EXCLUSIONS.json' } | Measure-Object).Count -gt 0
  $report.hasIndex = ($names | Where-Object { $_ -replace '\\','/' -like '*/monthly-review/archive/index.json' -or $_ -replace '\\','/' -eq 'deal-capture-proxy/reports/ai-demand-positioning/monthly-review/archive/index.json' }).Count -gt 0
  $report.pdfInArchiveCount = @($names | Where-Object { $_ -match 'monthly-review/archive/.*\.pdf$' }).Count
  $report.reviewJsonInArchiveCount = @($names | Where-Object { $_ -match 'monthly-review/archive/.*/review\.json$' }).Count
  $report.hasCambridgePdf = ($names | Where-Object { $_ -match 'cambridge_beaches.*\.pdf$' -or $_ -match 'cambridge-beaches.*\.pdf$' }).Count -gt 0
  $report.hasNoho = ($names | Where-Object { $_ -match 'now_now_noho|now-now-noho' }).Count -gt 0
  $report.hasRenaissance = ($names | Where-Object { $_ -match 'renaissance_times_square' }).Count -gt 0
  $report.hasServerJs = ($names | Where-Object { $_ -match '(^|/)deal-capture-proxy/server\.js$' }).Count -gt 0
  $report.hasUntrackedCapital = ($names | Where-Object { $_ -match 'capital-provider-explorer\.html$' }).Count -gt 0
  $report.hasHotelIntel = ($names | Where-Object { $_ -match 'hotel-intelligence' }).Count -gt 0
  $report.hasPackageJson = ($names | Where-Object { $_ -match '(^|/)deal-capture-proxy/package\.json$' }).Count -gt 0

  # Critical hash checks: extract specific entries to probe and hash vs LATEST
  if (Test-Path $extractProbe) { Remove-Item -LiteralPath $extractProbe -Recurse -Force }
  New-Item -ItemType Directory -Force -Path $extractProbe | Out-Null

  $critical = @(
    @{ key='index'; zip='deal-capture-proxy/reports/ai-demand-positioning/monthly-review/archive/index.json'; local='deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json' },
    @{ key='cambridgePdf'; zip='deal-capture-proxy/reports/ai-demand-positioning/monthly-review/archive/adp_cambridge_beaches_bermuda/2026-09/adp_mr_cambridge_beaches_bermud_2026-09_v1/report.pdf'; local='deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\adp_cambridge_beaches_bermuda\2026-09\adp_mr_cambridge_beaches_bermud_2026-09_v1\report.pdf' },
    @{ key='restoredReview'; zip='deal-capture-proxy/reports/ai-demand-positioning/monthly-review/archive/adp_renaissance_times_square/2026-09/adp_mr_renaissance_times_square_2026-09_v8/review.json'; local='deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\adp_renaissance_times_square\2026-09\adp_mr_renaissance_times_square_2026-09_v8\review.json' },
    @{ key='trackedServer'; zip='deal-capture-proxy/server.js'; local='deal-capture-proxy\server.js' },
    @{ key='untrackedCapital'; zip='deal-capture-proxy/public/capital-provider-explorer.html'; local='deal-capture-proxy\public\capital-provider-explorer.html' }
  )

  $hashResults = [ordered]@{}
  foreach ($c in $critical) {
    $entry = $entries | Where-Object { $_.FullName.Replace('\','/') -eq $c.zip } | Select-Object -First 1
    if (-not $entry) {
      # try case / slash variants
      $entry = $entries | Where-Object { $_.FullName.Replace('\','/') -like ("*" + ($c.zip -replace '^deal-capture-proxy/','')) } | Select-Object -First 1
    }
    $row = [ordered]@{ zipEntryFound = [bool]$entry; localFound = $false; match = $false }
    if ($entry) {
      $dest = Join-Path $extractProbe ($c.key + [IO.Path]::GetExtension($c.zip))
      $outStream = [IO.File]::Open($dest, [IO.FileMode]::Create)
      try {
        $inStream = $entry.Open()
        try { $inStream.CopyTo($outStream) } finally { $inStream.Dispose() }
      } finally { $outStream.Dispose() }
      $zipHash = (Get-FileHash -LiteralPath $dest -Algorithm SHA256).Hash
      $row.zipSha256 = $zipHash
      $localPath = Join-Path $latestRoot $c.local
      if (Test-Path -LiteralPath $localPath) {
        $row.localFound = $true
        $localHash = (Get-FileHash -LiteralPath $localPath -Algorithm SHA256).Hash
        $row.localSha256 = $localHash
        $row.match = ($zipHash -eq $localHash)
      }
    }
    $hashResults[$c.key] = $row
  }
  $report.hashChecks = $hashResults

  $report.integrityOpenOk = $true
} finally {
  $zip.Dispose()
}

# Cleanup probe
if (Test-Path $extractProbe) { Remove-Item -LiteralPath $extractProbe -Recurse -Force -EA SilentlyContinue }

# Overall
$hashesOk = @($report.hashChecks.Values | Where-Object { -not $_.match }).Count -eq 0
$report.zipVerified =
  $report.integrityOpenOk -and
  (-not $report.envPresent) -and
  $report.hasIndex -and
  $report.hasCambridgePdf -and
  $report.hasRenaissance -and
  $report.hasServerJs -and
  $report.hasUntrackedCapital -and
  $report.hasCloudSecretExclusionsInZip -and
  $hashesOk

$outPath = 'C:\Dev\deal-capture-proxy\reports\google-drive-cutover-zip-verify-20261001.json'
($report | ConvertTo-Json -Depth 8) | Set-Content -LiteralPath $outPath -Encoding UTF8
Write-Host "WROTE $outPath"
Write-Host ("ZIP_VERIFIED={0}" -f $report.zipVerified)
$report | ConvertTo-Json -Depth 8
