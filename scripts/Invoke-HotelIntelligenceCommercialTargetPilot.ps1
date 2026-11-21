<#
.SYNOPSIS
  Thin PowerShell orchestrator for Hotel Intelligence commercial target discovery pilot.

.DESCRIPTION
  Calls the shared Node factory pilot. Does not create a parallel research pipeline.
  CUSTOMER_BLOCKED. No HPC. Budget ceiling enforced in Node ($25 default).

.EXAMPLE
  pwsh -File scripts/Invoke-HotelIntelligenceCommercialTargetPilot.ps1 -Limit 160
#>
param(
  [int]$Limit = 160,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "scripts\sec-edgar-commercial-target-scale-pilot.mjs"))) {
  $Root = $PSScriptRoot + "\.."
}
Set-Location $Root

Write-Host "[HIF] Commercial target pilot limit=$Limit dryRun=$DryRun"
if ($DryRun) {
  Write-Host "[HIF] Dry-run only — exiting before paid/network pilot"
  exit 0
}

node scripts/test-commercial-target-discovery-v1.mjs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

node scripts/sec-edgar-commercial-target-scale-pilot.mjs --run --limit=$Limit
exit $LASTEXITCODE
