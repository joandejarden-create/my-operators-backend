<#
.SYNOPSIS
  Hotel Explorer Intelligence Factory V1.1 + permanent cross-tab consistency gates.

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
    HE_ACTIVE_TAB_COUNT (11 when HE_GUEST_SENTIMENT_ENABLED=false)
    HE_CONTACT_COMPONENT_PARITY
    HE_PHYSICAL_PRODUCT_ATTRIBUTE_COVERAGE
    HE_OWNERSHIP_VERIFICATION_STATE_PARITY
    HE_UNIFIED_THEME_CONSISTENCY

  Browser policy: -SkipBrowser forces browser checks to NOT_TESTED (never PASS).

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -Mode Validate -HotelId recLuxvwwxID7U2B8 -SkipBrowser

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -HotelId recLuxvwwxID7U2B8 -Mode Full

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -BatchFile configs/hotel-explorer/he50-eight-hotel-batch-v1.json -Mode Audit
#>
[CmdletBinding()]
param(
  [string]$HotelId = "recLuxvwwxID7U2B8",
  [string]$HotelName,
  [ValidateSet("Audit", "Research", "Apply", "Validate", "Full", "Refresh", "CrossTabValidate")]
  [string]$Mode = "Validate",
  [switch]$Apply,
  [double]$MaxSpendUSD = 0,
  [string]$OutputDir,
  [switch]$Resume,
  [string]$BatchFile,
  [switch]$Fixture,
  [switch]$SkipBrowser
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

Write-Host "[HE Factory] mode=$Mode hotelId=$HotelId hotelName=$HotelName fixture=$Fixture skipBrowser=$SkipBrowser"

$ConsistencyModes = @("Audit", "Validate", "Full", "Refresh", "CrossTabValidate")
if ($ConsistencyModes -contains $Mode) {
  $nodeArgs = @(".\scripts\hotel-explorer\run-he-cross-tab-validate.mjs", "--mode=$Mode")
  if ($Fixture) {
    $nodeArgs = @(".\scripts\hotel-explorer\run-he-cross-tab-validate.mjs", "--fixture")
  } else {
    if ($HotelId) { $nodeArgs += "--hotelId=$HotelId" }
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

$NodeRunner = Join-Path $RepoRoot "scripts\hotel-explorer\run-hotel-explorer-factory.mjs"
if (($Mode -ne "CrossTabValidate") -and (Test-Path $NodeRunner)) {
  $argsList = @($NodeRunner, "--mode", $Mode)
  if ($HotelId) { $argsList += @("--hotel-id", $HotelId) }
  if ($HotelName) { $argsList += @("--hotel-name", $HotelName) }
  if ($Apply) { $argsList += "--apply" }
  $argsList += @("--max-spend-usd", "$MaxSpendUSD")
  if ($OutputDir) { $argsList += @("--output-dir", $OutputDir) }
  if ($Resume) { $argsList += "--resume" }
  if ($BatchFile) { $argsList += @("--batch-file", $BatchFile) }
  if ($SkipBrowser) {
    # Node defaults skipBrowser=true unless --browser
  } else {
    $argsList += "--browser"
  }
  Write-Host "[HE Factory] V1.1 runner → Mode=$Mode HotelId=$HotelId Apply=$Apply"
  & node @argsList
  if ($LASTEXITCODE -ne 0) { throw "Hotel Explorer factory mode $Mode failed" }
} elseif ($Mode -eq "Research" -or $Mode -eq "Apply") {
  if (-not (Test-Path $NodeRunner)) {
    Write-Warning "Full factory runner not present — cross-tab gates completed; Research/Apply require factory runner."
  }
}

Write-Host "[HE Factory] done mode=$Mode"
