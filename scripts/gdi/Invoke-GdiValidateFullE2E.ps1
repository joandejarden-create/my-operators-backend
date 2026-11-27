<#
.SYNOPSIS
  Validate GDI Full E2E V3 + permanent STAGED completion standard.

.DESCRIPTION
  Runs:
  - V3 fixture acceptance (scenarios A–O)
  - Optional hotel DryRun orchestrator VALIDATE
  - Canonical E2E→STAGED lifecycle regression
  - Cumulative integrated staging non-regression
  - Optional live :8080 STAGED visibility check for HotelId

  Permanent doctrine: E2E is not operationally complete until qualified
  results are STAGED and visible on port 8080.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiValidateFullE2E.ps1

.EXAMPLE
  .\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -RequirePort8080
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

function Invoke-NodeTest([string]$RelPath) {
  $test = Join-Path $resolvedRepo $RelPath
  if (-not (Test-Path -LiteralPath $test)) { throw "Missing test: $test" }
  Write-Host "Running $RelPath…"
  & node $test
  if ($LASTEXITCODE -ne 0) { throw "Test failed: $RelPath exit=$LASTEXITCODE" }
}

Write-Host '== GDI Full E2E validation (lifecycle: FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT) =='

Invoke-NodeTest 'scripts\test-gdi-canonical-full-e2e-v3-v1.mjs'
Invoke-NodeTest 'scripts\test-gdi-canonical-e2e-to-staged-lifecycle-v1.mjs'
Invoke-NodeTest 'scripts\test-gdi-cumulative-integrated-staging-non-regression-v1.mjs'
Invoke-NodeTest 'scripts\test-gdi-content-hydration-presentation-gate-v1.mjs'

if ($HotelId) {
  Write-Host "Validating hotel DryRun artifacts for $HotelId…"
  Invoke-GdiV3NodeStage -Stage 'VALIDATE' -HotelId $HotelId -Mode DryRun -RepoPath $resolvedRepo

  Write-Host "Validating local STAGED pointer for $HotelId…"
  $probeScript = Join-Path $resolvedRepo 'scripts\gdi\probe-gdi-staged-pointer-v1.mjs'
  & node $probeScript $HotelId
  if ($LASTEXITCODE -ne 0) {
    throw "Hotel $HotelId missing valid STAGED inventory (exit=$LASTEXITCODE)"
  }
}

if ($RequirePort8080 -or $HotelId) {
  $listen = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $listen) {
    if ($RequirePort8080) { throw 'Port 8080 is not listening (required)' }
    Write-Host '[warn] Port 8080 not listening — skipping live inventory probe'
  } elseif ($HotelId) {
    $url = "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/$HotelId/opportunities?publicationPackage=1&preview=staged&surface=customer"
    Write-Host "Probing $url"
    $resp = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 30
    $json = $resp.Content | ConvertFrom-Json
    if (-not $json.ok -or [int]$json.count -lt 1) {
      throw "Port 8080 STAGED inventory unavailable for $HotelId (ok=$($json.ok) count=$($json.count))"
    }
    Write-Host "[8080] STAGED count=$($json.count) version=$($json.publication.publicationVersion)"

    $curUrl = "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/$HotelId/opportunities?publicationPackage=1&preview=current&surface=customer"
    try {
      $curResp = Invoke-WebRequest $curUrl -UseBasicParsing -TimeoutSec 30
      $curJson = $curResp.Content | ConvertFrom-Json
    } catch {
      $curJson = @{ ok = $false; count = 0 }
    }
    Write-Host "[8080] CURRENT ok=$($curJson.ok) count=$($curJson.count) (must not auto-change from STAGED)"
  }
}

Write-Host 'GDI Full E2E validation PASS — STAGED completion standard enforced'
