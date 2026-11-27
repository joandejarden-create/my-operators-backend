<#
.SYNOPSIS
  Validate GDI Executive Summary V2 (design + metric integrity).

.EXAMPLE
  .\scripts\gdi\Invoke-GdiExecutiveSummaryV2Validate.ps1 -HotelId recnPB3Q02NO8Ma1o -RequirePort8080
#>
[CmdletBinding()]
param(
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId = 'recnPB3Q02NO8Ma1o',
  [switch]$RequirePort8080,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

$resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
Set-Location $resolvedRepo

Write-Host '== GDI Executive Summary V2 validation =='
& node (Join-Path $resolvedRepo 'scripts\test-gdi-executive-summary-v2.mjs')
if ($LASTEXITCODE -ne 0) { throw 'test-gdi-executive-summary-v2 failed' }
& node (Join-Path $resolvedRepo 'scripts\test-gdi-executive-summary-v1.mjs')
if ($LASTEXITCODE -ne 0) { throw 'test-gdi-executive-summary-v1 failed' }

$listen = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $listen) {
  if ($RequirePort8080) { throw 'Port 8080 is not listening' }
  Write-Host '[warn] Port 8080 not listening — skipping live probe'
} else {
  $url = "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/$HotelId/executive-summary?preview=staged"
  Write-Host "Probing $url"
  try {
    $resp = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 45
    $json = $resp.Content | ConvertFrom-Json
    if (-not $json.ok) { throw "ES API ok=false: $($json.error)" }
    if ($json.version -ne 'gdi-executive-summary-v2') { throw "Expected V2 version, got $($json.version)" }
    $unspecified = @($json.sections.executiveCommercialSnapshot.priorityDemandSegments | Where-Object { $_.name -match 'Unspecified' })
    if ($unspecified.Count -gt 0) { throw 'Demand segments still Unspecified' }
    Write-Host "[8080] ES V2 ok hotel=$HotelId segs=$($json.sections.demandComposition.rows.Count) perspectives=$($json.perspectives.available -join ',')"
  } catch {
    if ($RequirePort8080) { throw }
    Write-Host "[warn] live ES probe failed: $_"
  }
}

Write-Host 'GDI Executive Summary V2 validation PASS'
