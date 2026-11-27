<#
.SYNOPSIS
  Read-only validation for GDI competitive Market Scope counts + filter layout assets.
#>
[CmdletBinding()]
param(
  [string]$BaseUrl = "http://127.0.0.1:8080",
  [string]$WyndhamHotelId = "recnPB3Q02NO8Ma1o",
  [string]$BethesdaHotelId = "recLuxvwwxID7U2B8"
)

$ErrorActionPreference = "Stop"
Write-Host "=== GDI Competitive + Filter Layout Validate ==="

function Get-Staged($hotelId) {
  $url = "$BaseUrl/api/group-demand-intelligence/hotels/$hotelId/opportunities?preview=staged"
  return Invoke-RestMethod -Uri $url -Method Get
}

$w = Get-Staged $WyndhamHotelId
$b = Get-Staged $BethesdaHotelId

$wCounts = $w.publication.inventoryCounts
$bCounts = $b.publication.inventoryCounts

Write-Host ("Wyndham All={0} Hotel={1} Competitive={2}" -f $wCounts.all, $wCounts.hotel, $wCounts.competitive)
Write-Host ("Bethesda All={0} Hotel={1} Competitive={2}" -f $bCounts.all, $bCounts.hotel, $bCounts.competitive)

if ($wCounts.competitive -ne 0) {
  throw "Expected Wyndham STAGED competitive=0 (measured). Got $($wCounts.competitive)"
}
if ($bCounts.competitive -lt 1) {
  throw "Expected Bethesda STAGED competitive>=1. Got $($bCounts.competitive)"
}

$html = Invoke-WebRequest -Uri "$BaseUrl/group-demand-intelligence.html" -UseBasicParsing
if ($html.Content -notmatch 'gdi-competitive-filter-layout-v1') {
  throw "Cache bust gdi-competitive-filter-layout-v1 missing from HTML"
}

Push-Location (Resolve-Path (Join-Path $PSScriptRoot "..\.."))
try {
  npm run test:gdi-filter-layout-stability-v1
  if ($LASTEXITCODE -ne 0) { throw "filter layout test failed" }
  npm run test:gdi-list-optional-columns-hydration-v1
  if ($LASTEXITCODE -ne 0) { throw "optional columns hydration test failed" }
}
finally {
  Pop-Location
}

Write-Host "PASS Invoke-GdiCompetitiveFilterLayoutValidate.ps1"
Write-Host "NOTE: Wyndham Competitive=0 is measured STAGED zero; Checkpoint B paid CAD not executed (PR #620 OPEN)."
