<#
.SYNOPSIS
  Validate GDI Full E2E V3 stage artifacts + run fixture acceptance tests.

.DESCRIPTION
  Runs the unified V3 test suite (scenarios A–O) and optionally re-validates
  a hotel's DryRun orchestrator output directory.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiValidateFullE2E.ps1

.EXAMPLE
  .\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o
#>
[CmdletBinding()]
param(
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId = '',

  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

$resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
Set-Location $resolvedRepo

$test = Join-Path $resolvedRepo 'scripts\test-gdi-canonical-full-e2e-v3-v1.mjs'
if (-not (Test-Path -LiteralPath $test)) {
  throw "Missing V3 test: $test"
}

Write-Host 'Running GDI Full E2E V3 fixture acceptance tests…'
& node $test
if ($LASTEXITCODE -ne 0) {
  throw "V3 acceptance tests failed with exit code $LASTEXITCODE"
}

if ($HotelId) {
  Write-Host "Validating hotel DryRun artifacts for $HotelId…"
  Invoke-GdiV3NodeStage -Stage 'VALIDATE' -HotelId $HotelId -Mode DryRun -RepoPath $resolvedRepo
}

Write-Host 'GDI Full E2E V3 validation PASS'
