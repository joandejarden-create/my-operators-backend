<#
.SYNOPSIS
  Validate GDI Content Hydration & Presentation gate (permanent STAGED certification).

.DESCRIPTION
  Runs:
  - Content hydration presentation gate regression
  - Optional hotel DryRun audit via report script
  - Optional live :8080 title/demand smoke for HotelId

  Not a sixth research stage — part of Full E2E → STAGED validation.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiContentHydrationValidate.ps1

.EXAMPLE
  .\scripts\gdi\Invoke-GdiContentHydrationValidate.ps1 -HotelId recnPB3Q02NO8Ma1o -RequirePort8080
#>
[CmdletBinding()]
param(
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId = '',

  [switch]$RequirePort8080,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

$resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
Set-Location $resolvedRepo

Write-Host '== GDI Content Hydration & Presentation Validation =='

& node (Join-Path $resolvedRepo 'scripts\test-gdi-content-hydration-presentation-gate-v1.mjs')
if ($LASTEXITCODE -ne 0) { throw 'test-gdi-content-hydration-presentation-gate-v1 failed' }

$auditArgs = @()
if ($HotelId) { $auditArgs += @('--hotelId', $HotelId) }
& node (Join-Path $resolvedRepo 'scripts\report-gdi-content-hydration-audit-v1.mjs') @auditArgs
if ($LASTEXITCODE -ne 0) { throw 'report-gdi-content-hydration-audit-v1 failed' }

if ($RequirePort8080 -or $HotelId) {
  $listen = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $listen) {
    if ($RequirePort8080) { throw 'Port 8080 is not listening (required)' }
    Write-Host '[warn] Port 8080 not listening — skipping live content probe'
  } elseif ($HotelId) {
    $url = "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/$HotelId/opportunities?publicationPackage=1&preview=staged&surface=customer"
    Write-Host "Probing $url"
    $resp = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 45
    $json = $resp.Content | ConvertFrom-Json
    if (-not $json.ok -or [int]$json.count -lt 1) {
      throw "Port 8080 STAGED inventory unavailable for $HotelId"
    }
    $researchTitles = @()
    foreach ($opp in @($json.opportunities)) {
      $t = [string]$(if ($opp.displayTitle) { $opp.displayTitle } else { $opp.title })
      if ($t.Length -gt 90 -or $t -match '^(Opened|The airport|City |Company )') {
        $researchTitles += $t
      }
    }
    if ($researchTitles.Count -gt 0) {
      throw "Live STAGED still exposes research-paragraph titles ($($researchTitles.Count))"
    }
    Write-Host "[8080] STAGED count=$($json.count) version=$($json.publication.publicationVersion) researchParagraphTitles=0"
  }
}

Write-Host 'GDI Content Hydration validation PASS'
