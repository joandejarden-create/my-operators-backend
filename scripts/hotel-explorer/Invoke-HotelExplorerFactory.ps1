<#
.SYNOPSIS
  Hotel Explorer Intelligence Factory entry (cross-tab / submarket / org gates included).

.DESCRIPTION
  Modes: Audit, Research, Apply, Validate, Full, Refresh, CrossTabValidate
  Cross-tab permanent gates:
    HE_SUBMARKET_COHORT_VALIDATED
    HE_SUBMARKET_COVERAGE_DISCLOSED
    HE_ORG_PORTFOLIO_HYDRATION_VALIDATED
    HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT
    HE_CROSS_TAB_PARITY_VALIDATED

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -Mode CrossTabValidate -HotelId recLuxvwwxID7U2B8
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -Mode CrossTabValidate -Fixture
#>
[CmdletBinding()]
param(
  [ValidateSet("Audit", "Research", "Apply", "Validate", "Full", "Refresh", "CrossTabValidate")]
  [string]$Mode = "CrossTabValidate",
  [string]$HotelId = "recLuxvwwxID7U2B8",
  [switch]$Fixture,
  [switch]$SkipBrowser
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

Write-Host "[HE Factory] mode=$Mode hotelId=$HotelId fixture=$Fixture"

if ($Mode -eq "CrossTabValidate" -or $Mode -eq "Validate" -or $Mode -eq "Full" -or $Mode -eq "Refresh" -or $Mode -eq "Audit") {
  if ($Fixture) {
    node ".\scripts\hotel-explorer\run-he-cross-tab-validate.mjs" --fixture
  } else {
    node ".\scripts\hotel-explorer\run-he-cross-tab-validate.mjs" "--hotelId=$HotelId"
  }
  if ($LASTEXITCODE -ne 0) { throw "Cross-tab / submarket / org gates failed" }
}

# When the fuller factory runner exists (merged HE Factory V1.1+), delegate remaining modes.
$FullRunner = Join-Path $RepoRoot "scripts\hotel-explorer\run-hotel-explorer-factory.mjs"
if (($Mode -ne "CrossTabValidate") -and (Test-Path $FullRunner)) {
  $args = @("--mode", $Mode, "--hotelId", $HotelId)
  if ($SkipBrowser) { $args += "--skip-browser" }
  node $FullRunner @args
  if ($LASTEXITCODE -ne 0) { throw "Hotel Explorer factory mode $Mode failed" }
} elseif ($Mode -ne "CrossTabValidate" -and $Mode -ne "Validate" -and $Mode -ne "Audit") {
  Write-Warning "Full factory runner not present on this branch — cross-tab gates completed; Research/Apply require HE Factory V1.1 merge."
}

Write-Host "[HE Factory] done mode=$Mode"
