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
    HE_CONTACT_DIRECTORY_VALIDATED
    HE_CONTACT_ORG_MAPPING_VALIDATED
    HE_CONTACT_CHANNEL_PROVENANCE_VALIDATED
    HE_CONTACT_PRESENTATION_VALIDATED
    HE_SUBMARKET_METHODOLOGY_VALIDATED
    HE_NO_INTERNAL_TERMINOLOGY_LEAKAGE
    HE_CUSTOMER_COPY_VALIDATED
    HE_PROPERTY_FACTS_FORMAT_VALIDATED
    HE_CONTACT_TABLE_LAYOUT_VALIDATED
    HE_CONTACT_DETAILS_DRAWER_VALIDATED
    HE_CONTACT_CX_REGRESSION
    HE_OWNERSHIP_GRAPH_REGRESSION
    HE_PORTFOLIO_LAYOUT_REGRESSION
    HE_SUBMARKET_PRESENTATION_REGRESSION
    HE_FOUNDER_UI_CONTRACT
    HE_CANONICAL_BROWSER_PARITY (NOT_TESTED when -SkipBrowser)
    HE_LOADING_COMPONENT_CONSISTENT
    HE_HEADER_ACTIONS_HYDRATED
    HE_PHYSICAL_PRODUCT_ATTRIBUTE_COVERAGE
    HE_OWNERSHIP_VERIFICATION_STATE_PARITY
    HE_UNIFIED_THEME_CONSISTENCY
    HE_CANONICAL_AIRTABLE_READBACK
    HE_AUTHENTICATED_LIVE_RENDERING (NOT_TESTED when -SkipBrowser)
    FULL_HE_E2E_VALIDATED (remains NO until eight-hotel browser program passes)

  Browser policy: -SkipBrowser forces browser checks to NOT_TESTED (never PASS).

  Release acceptance (Contact V3 + Live API probes):
    node scripts/hotel-explorer/run-he50-release-acceptance-gates-v1.mjs [--skip-browser|--browser]

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

# Permanent Contact V3 + physical/ownership/theme Live probes (Bethesda default).
if ($ConsistencyModes -contains $Mode) {
  $acceptArgs = @(".\scripts\hotel-explorer\run-he50-release-acceptance-gates-v1.mjs")
  if ($SkipBrowser) { $acceptArgs += "--skip-browser" } else { $acceptArgs += "--browser" }
  Write-Host "[HE Factory] release acceptance gates → $($acceptArgs -join ' ')"
  & node @acceptArgs
  if ($LASTEXITCODE -ne 0) {
    throw "HE 5.0 release acceptance gates FAILED for $HotelId (mode=$Mode)"
  }

  Write-Host "[HE Factory] Organization & Portfolio UI cleanup gates"
  & npm run test:he-org-portfolio-ui-cleanup-v1
  if ($LASTEXITCODE -ne 0) {
    throw "HE Organization & Portfolio UI cleanup gates FAILED for $HotelId (mode=$Mode)"
  }

  Write-Host "[HE Factory] Organization & Portfolio padding alignment gates"
  & npm run test:he-org-portfolio-padding-v1
  if ($LASTEXITCODE -ne 0) {
    throw "HE Organization & Portfolio padding gates FAILED for $HotelId (mode=$Mode)"
  }

  Write-Host "[HE Factory] Executive Ownership Summary narrative gates"
  & npm run test:he-executive-ownership-narrative-v1
  if ($LASTEXITCODE -ne 0) {
    throw "HE Executive Ownership Summary narrative gates FAILED for $HotelId (mode=$Mode)"
  }

  $presArgs = @(".\scripts\hotel-explorer\audit-he-customer-presentation-v1.mjs")
  if (-not $SkipBrowser) { $presArgs += "--browser" }
  Write-Host "[HE Factory] customer presentation audit → $($presArgs -join ' ')"
  & node @presArgs
  if ($LASTEXITCODE -ne 0) {
    throw "HE customer presentation audit FAILED for $HotelId (mode=$Mode)"
  }

  $founderArgs = @(".\scripts\hotel-explorer\audit-he-founder-ui-contract-v1.mjs")
  if ($SkipBrowser) { $founderArgs += "--skip-browser" } else { $founderArgs += "--browser" }
  Write-Host "[HE Factory] founder UI contract → $($founderArgs -join ' ')"
  & node @founderArgs
  if ($LASTEXITCODE -ne 0) {
    throw "HE founder UI contract FAILED for $HotelId (mode=$Mode)"
  }

  if (-not $SkipBrowser) {
    Write-Host "[HE Factory] Organization & Portfolio layout browser certification"
    & npm run he:org-portfolio-layout-audit
    if ($LASTEXITCODE -ne 0) {
      throw "HE Organization & Portfolio layout browser certification FAILED for $HotelId (mode=$Mode)"
    }
  } else {
    Write-Host "[HE Factory] Organization & Portfolio layout browser certification → NOT_TESTED (SkipBrowser)"
  }
}

# Header Website/Phone hydration audit (current hotel, or full HE50 batch when -BatchFile set).
if ($ConsistencyModes -contains $Mode) {
  $headerArgs = @(".\scripts\hotel-explorer\audit-he-header-actions-hydration-v1.mjs")
  if ($BatchFile) {
    Write-Host "[HE Factory] header actions hydration → HE50 eight-hotel batch"
  } elseif ($HotelId) {
    $headerArgs += "--hotelId=$HotelId"
    Write-Host "[HE Factory] header actions hydration → hotelId=$HotelId"
  }
  & node @headerArgs
  if ($LASTEXITCODE -ne 0) {
    throw "HE header actions hydration FAILED (mode=$Mode)"
  }
}

Write-Host "[HE Factory] done mode=$Mode"
