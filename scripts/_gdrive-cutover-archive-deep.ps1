# Archive deep-check + wait for ZIP (no secret values)
$ErrorActionPreference = 'Continue'
$archive = 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive'
$out = [ordered]@{}

Write-Host "ARCHIVE TOP:"
Get-ChildItem -LiteralPath $archive -Force | Select-Object Name, Mode | Format-Table -AutoSize | Out-String | Write-Host

$allFiles = @(Get-ChildItem -LiteralPath $archive -Recurse -Force -File -ErrorAction SilentlyContinue)
$out.totalArchiveFiles = $allFiles.Count
$out.reviewJson = @($allFiles | Where-Object { $_.Name -eq 'review.json' }).Count
$out.pdfs = @($allFiles | Where-Object { $_.Extension -eq '.pdf' }).Count
$out.pngs = @($allFiles | Where-Object { $_.Extension -in '.png','.jpg','.jpeg','.webp' }).Count
$out.covers = @($allFiles | Where-Object { $_.FullName -match '(?i)cover' }).Count
$out.actionRegisters = @($allFiles | Where-Object { $_.FullName -match '(?i)action.?register' }).Count

$cambridge = @($allFiles | Where-Object { $_.FullName -match '(?i)cambridge' } | Select-Object -First 20)
$noho = @($allFiles | Where-Object { $_.FullName -match '(?i)noho|now.now' } | Select-Object -First 20)
$out.cambridgeCount = $cambridge.Count
$out.cambridgeSamples = @($cambridge | ForEach-Object { $_.FullName.Replace($archive + '\', '') })
$out.nohoCount = $noho.Count
$out.nohoSamples = @($noho | ForEach-Object { $_.FullName.Replace($archive + '\', '') })

# index.json summary without dumping secrets
$idx = Get-Content -LiteralPath (Join-Path $archive 'index.json') -Raw | ConvertFrom-Json
if ($idx.reviews) { $out.indexReviewCount = @($idx.reviews).Count }
elseif ($idx.PSObject.Properties.Name -contains 'entries') { $out.indexReviewCount = @($idx.entries).Count }
else {
  $out.indexTopKeys = @($idx.PSObject.Properties.Name | Select-Object -First 20)
  # try common shapes
  foreach ($k in @('items','archive','properties','months','byProperty')) {
    if ($idx.PSObject.Properties.Name -contains $k) {
      $out["index_$k`_type"] = $idx.$k.GetType().Name
      try { $out["index_$k`_count"] = @($idx.$k).Count } catch {}
    }
  }
}

# Compare live source archive counts
$liveArchive = 'C:\Dev\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive'
$liveFiles = @(Get-ChildItem -LiteralPath $liveArchive -Recurse -Force -File -EA SilentlyContinue)
$out.liveReviewJson = @($liveFiles | Where-Object { $_.Name -eq 'review.json' }).Count
$out.livePdfs = @($liveFiles | Where-Object { $_.Extension -eq '.pdf' }).Count

# Hotel intelligence / customer artifacts
$hi = 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\hotel-intelligence-batches'
$out.hotelIntelligenceBatchesPresent = Test-Path -LiteralPath $hi
$out.fixturesHotelIntelPresent = Test-Path -LiteralPath 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\fixtures\hotel-intelligence'

# Untracked sample present in LATEST
$out.untrackedCapitalProviderPresent = Test-Path -LiteralPath 'C:\Dev\dealality-backups\LATEST\deal-capture-proxy\public\capital-provider-explorer.html'

# Full LATEST stats (root)
$latestRoot = 'C:\Dev\dealality-backups\LATEST'
$latestAll = @(Get-ChildItem -LiteralPath $latestRoot -Recurse -Force -File -EA SilentlyContinue)
$out.latestRootFileCount = $latestAll.Count
$out.latestRootSizeGB = [math]::Round((($latestAll | Measure-Object Length -Sum).Sum / 1GB), 2)

$jsonPath = 'C:\Dev\deal-capture-proxy\reports\google-drive-cutover-archive-deep-20261001.json'
($out | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath $jsonPath -Encoding UTF8
Write-Host "WROTE $jsonPath"
$out | ConvertTo-Json -Depth 6

# ZIP status
Write-Host '---ZIP---'
$zip = 'G:\My Drive\Dealality Backups\2026-10-01_2036\Dealality-backup.zip'
if (Test-Path -LiteralPath $zip) {
  $zi = Get-Item -LiteralPath $zip
  Write-Host ("ZIP bytes={0} GB={1:N2} mtime={2}" -f $zi.Length, ($zi.Length/1GB), $zi.LastWriteTime)
}
Write-Host '---ROOT---'
Get-ChildItem 'C:\Dev\dealality-backups' -Force | Select-Object -ExpandProperty Name
Write-Host '---LOG TAIL---'
Get-Content 'C:\Dev\dealality-backups\logs\Dealality-2026-10-01_2036.log' -Tail 12 -Encoding UTF8
