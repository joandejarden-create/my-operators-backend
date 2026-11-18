<#
.SYNOPSIS
  Universal Dealality GDI operational workflow (Windows PowerShell entry point).

.DESCRIPTION
  Orchestrates existing Node GDI modules — research, Airtable sync, review,
  one-click publish, and full-content parity verification.

  PowerShell = workflow orchestration only.
  Node.js = business logic, Airtable, publication, parity.
  Does NOT implement a competing research engine or publication DTO path.

  Research / FullRun default to the authoritative canonical Full E2E entry
  (scripts/gdi-canonical-full-e2e-entry-v1.mjs). Pass -Legacy to use the
  historical gdi-research-to-page coordinator via the universal workflow.
  Validate / SyncAirtable / Review / Publish / Verify / Inventory remain on
  the universal operational workflow.

.PARAMETER HotelId
  Canonical hotel record id (e.g. recKRJjcPnb4tVDDS).

.PARAMETER Action
  Research | Validate | SyncAirtable | Review | Publish | Verify | FullRun | Inventory

.PARAMETER DryRun
  Safe default. Prevents production Airtable writes and publication apply.
  Omit and pass -Apply for live writes (still requires Production + ApprovalReference for Publish).

.PARAMETER Environment
  Local | Production

.PARAMETER Resume
  Resume an existing full-e2e run id.

.PARAMETER ReportPath
  Directory for workflow reports (default reports/gdi-universal-operational-workflow-v1).

.PARAMETER ApprovalReference
  Founder authorization phrase required for live Publish.

.PARAMETER Canonical
  Explicit alias: Research / FullRun use the authoritative canonical Full E2E
  entry. This is now the default for Research / FullRun; the switch remains for
  backward-compatible scripts.

.PARAMETER Legacy
  Opt into the historical Research / FullRun path (gdi-research-to-page via
  universal workflow). Deprecated for Full E2E research certification.

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action Validate

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action SyncAirtable -DryRun

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recLuxvwwxID7U2B8" -Action FullRun -DryRun

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recLuxvwwxID7U2B8" -Action Research -Canonical -DryRun

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recLuxvwwxID7U2B8" -Action FullRun -Legacy -DryRun

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action Publish -Environment Production -ApprovalReference "FOUNDER_GO_..." -Apply
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $false)]
  [string]$HotelId,

  [Parameter(Mandatory = $true)]
  [ValidateSet(
    "Research",
    "Validate",
    "SyncAirtable",
    "Review",
    "Publish",
    "Verify",
    "FullRun",
    "Inventory"
  )]
  [string]$Action,

  [switch]$DryRun,

  [switch]$Apply,

  [ValidateSet("Local", "Production")]
  [string]$Environment = "Local",

  [string]$Resume,

  [string]$ReportPath,

  [string]$ApprovalReference,

  [string]$ShareToken,

  [string]$ProdBase,

  [switch]$ExecuteResearch,

  [switch]$Canonical,

  [switch]$Legacy
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

# Safe default: DryRun unless -Apply is explicit.
$isDry = $true
if ($Apply.IsPresent) { $isDry = $false }
if ($DryRun.IsPresent) { $isDry = $true }

if ($Action -ne "Inventory" -and [string]::IsNullOrWhiteSpace($HotelId)) {
  throw "HotelId is required for Action=$Action"
}

if ($Action -eq "Publish" -and -not $isDry) {
  if ($Environment -ne "Production") {
    throw "Live Publish requires -Environment Production."
  }
  if ([string]::IsNullOrWhiteSpace($ApprovalReference)) {
    throw "Live Publish requires -ApprovalReference (founder authorization)."
  }
}

if ($Canonical.IsPresent -and $Legacy.IsPresent) {
  throw "Pass only one of -Canonical or -Legacy."
}

# Research / FullRun → canonical Full E2E by default.
# -Legacy restores the historical research-to-page coordinator.
# Paid flags are never auto-passed from this wrapper.
$useCanonicalResearch =
  ($Action -eq "Research" -or $Action -eq "FullRun") -and (-not $Legacy.IsPresent)

if ($useCanonicalResearch) {
  if ([string]::IsNullOrWhiteSpace($HotelId)) {
    throw "HotelId is required for Canonical Research/FullRun"
  }
  $mode = "dry-run"
  if (-not $isDry) {
    Write-Host "Canonical path refuses live paid/apply from gdi-workflow.ps1 — use npm run gdi:canonical-full-e2e with explicit founder flags." -ForegroundColor Yellow
  }
  $canonicalArgs = @(
    "scripts/gdi-canonical-full-e2e-entry-v1.mjs",
    "--hotel", $HotelId,
    "--mode", $mode
  )
  if (-not [string]::IsNullOrWhiteSpace($Resume)) {
    $canonicalArgs += @("--execution-run-id", $Resume)
  }
  if (-not [string]::IsNullOrWhiteSpace($ReportPath)) {
    $canonicalArgs += @("--out", $ReportPath)
  }
  Write-Host "GDI workflow (CANONICAL): Action=$Action HotelId=$HotelId mode=$mode runner=gdi-canonical-full-e2e-entry-v1" -ForegroundColor Cyan
  Write-Host "Repo: $repoRoot"
  if ($Legacy.IsPresent -eq $false -and $Canonical.IsPresent -eq $false) {
    Write-Host "Note: Research/FullRun default to canonical Full E2E. Pass -Legacy for historical gdi-research-to-page." -ForegroundColor DarkGray
  }
  & node @canonicalArgs
  $exit = $LASTEXITCODE
  if ($exit -ne 0) {
    Write-Host "GDI canonical workflow exited with code $exit" -ForegroundColor Red
    exit $exit
  }
  Write-Host "GDI canonical workflow finished." -ForegroundColor Green
  exit 0
}

$nodeArgs = @(
  "scripts/gdi-universal-operational-workflow-v1.mjs",
  "--action", $Action,
  "--environment", $Environment
)

if (-not [string]::IsNullOrWhiteSpace($HotelId)) {
  $nodeArgs += @("--hotel", $HotelId)
}
if (-not $isDry) {
  $nodeArgs += "--apply"
}
if (-not [string]::IsNullOrWhiteSpace($Resume)) {
  $nodeArgs += @("--resume-run", $Resume)
}
if (-not [string]::IsNullOrWhiteSpace($ReportPath)) {
  $nodeArgs += @("--report-path", $ReportPath)
}
if (-not [string]::IsNullOrWhiteSpace($ApprovalReference)) {
  $nodeArgs += @("--approval-reference", $ApprovalReference)
}
if (-not [string]::IsNullOrWhiteSpace($ShareToken)) {
  $nodeArgs += @("--share-token", $ShareToken)
}
if (-not [string]::IsNullOrWhiteSpace($ProdBase)) {
  $nodeArgs += @("--prod-base", $ProdBase)
}
if ($ExecuteResearch.IsPresent) {
  $nodeArgs += "--execute-research"
}

Write-Host "GDI workflow: Action=$Action HotelId=$HotelId Environment=$Environment DryRun=$isDry runner=gdi-universal-operational-workflow-v1" -ForegroundColor Cyan
Write-Host "Repo: $repoRoot"
if (($Action -eq "Research" -or $Action -eq "FullRun") -and $Legacy.IsPresent) {
  Write-Host "Historical research path (gdi-research-to-page) via -Legacy. Prefer default/canonical Full E2E for research certification." -ForegroundColor DarkYellow
}

& node @nodeArgs
$exit = $LASTEXITCODE
if ($exit -ne 0) {
  Write-Host "GDI workflow exited with code $exit" -ForegroundColor Red
  exit $exit
}
Write-Host "GDI workflow finished." -ForegroundColor Green
