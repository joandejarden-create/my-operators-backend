<#
.SYNOPSIS
  Hotel Explorer Intelligence Factory entry (cross-tab consistency report included).

.DESCRIPTION
  Modes: Audit, Research, Apply, Validate, Full, Refresh, CrossTabValidate

  Every Audit / Validate / Full / Refresh / CrossTabValidate run generates:
    reports/hotel-explorer/HE-CROSS-TAB-CONSISTENCY-<hotelId>.json
    reports/hotel-explorer/HE-CROSS-TAB-CONSISTENCY-<hotelId>.md

  Permanent gates:
    HE_SUBMARKET_COHORT_VALIDATED
    HE_SUBMARKET_COVERAGE_DISCLOSED
    HE_ORG_PORTFOLIO_HYDRATION_VALIDATED
    HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT
    HE_CROSS_TAB_PARITY_VALIDATED
    HE_CROSS_TAB_CONSISTENCY_REPORT_GENERATED
    HE_E2E_CROSS_TAB_CERTIFICATION

  Browser policy: -SkipBrowser forces browser checks to NOT_TESTED (never PASS).

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -Mode Validate -HotelId recLuxvwwxID7U2B8 -SkipBrowser
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -Mode Audit -HotelId recLuxvwwxID7U2B8
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -Mode CrossTabValidate -Fixture
#>
[CmdletBinding()]
param(
  [ValidateSet("Audit", "Research", "Apply", "Validate", "Full", "Refresh", "CrossTabValidate")]
  [string]$Mode = "Validate",
  [string]$HotelId = "recLuxvwwxID7U2B8",
  [switch]$Fixture,
  [switch]$SkipBrowser
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

Write-Host "[HE Factory] mode=$Mode hotelId=$HotelId fixture=$Fixture skipBrowser=$SkipBrowser"

$ConsistencyModes = @("Audit", "Validate", "Full", "Refresh", "CrossTabValidate")
if ($ConsistencyModes -contains $Mode) {
  $nodeArgs = @(".\scripts\hotel-explorer\run-he-cross-tab-validate.mjs", "--mode=$Mode")
  if ($Fixture) {
    $nodeArgs = @(".\scripts\hotel-explorer\run-he-cross-tab-validate.mjs", "--fixture")
  } else {
    $nodeArgs += "--hotelId=$HotelId"
    if ($SkipBrowser) { $nodeArgs += "--skip-browser" } else { $nodeArgs += "--browser" }
  }
  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "Cross-tab consistency / E2E certification FAILED for $HotelId (mode=$Mode)"
  }

  $jsonReport = Join-Path $RepoRoot "reports\hotel-explorer\HE-CROSS-TAB-CONSISTENCY-$HotelId.json"
  $mdReport = Join-Path $RepoRoot "reports\hotel-explorer\HE-CROSS-TAB-CONSISTENCY-$HotelId.md"
  if (-not $Fixture) {
    if (-not (Test-Path $jsonReport) -or -not (Test-Path $mdReport)) {
      throw "Consistency report artifacts missing: $jsonReport / $mdReport"
    }
    Write-Host "[HE Factory] consistency report: $jsonReport"
    Write-Host "[HE Factory] consistency summary: $mdReport"
  }
}

# When the fuller factory runner exists (merged HE Factory V1.1+), delegate remaining modes.
$FullRunner = Join-Path $RepoRoot "scripts\hotel-explorer\run-hotel-explorer-factory.mjs"
if (($Mode -ne "CrossTabValidate") -and (Test-Path $FullRunner)) {
  $factoryArgs = @("--mode", $Mode, "--hotelId", $HotelId)
  if ($SkipBrowser) { $factoryArgs += "--skip-browser" }
  node $FullRunner @factoryArgs
  if ($LASTEXITCODE -ne 0) { throw "Hotel Explorer factory mode $Mode failed" }
} elseif ($Mode -eq "Research" -or $Mode -eq "Apply") {
  Write-Warning "Full factory runner not present on this branch — cross-tab gates completed; Research/Apply require HE Factory V1.1 merge."
}

Write-Host "[HE Factory] done mode=$Mode"
