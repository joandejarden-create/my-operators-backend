<#
.SYNOPSIS
  Universal Dealality GDI operational workflow (Windows PowerShell entry point).

.DESCRIPTION
  Orchestrates existing Node GDI modules — research, Airtable sync, review,
  one-click publish, and full-content parity verification.

  PowerShell = workflow orchestration only.
  Node.js = business logic, Airtable, publication, parity.
  Does NOT implement a competing research engine or publication DTO path.

.PARAMETER HotelId
  Canonical hotel record id (e.g. recKRJjcPnb4tVDDS).

.PARAMETER Action
  Research | Validate | SyncAirtable | Review | Publish | Verify | FullRun | FullE2E | Inventory

  FullE2E evaluates all 14 canonical PowerShell demand-segment workflows per hotel
  and by default invokes registered discovery executors (--execute-discovery).
  Use -SkipDiscoveryExecution for accounting-only bookkeeping.
  Does not invent leads or claim COMPLETE while NOT_RUN remains. Never publishes.

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

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action Validate

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action SyncAirtable -DryRun

.EXAMPLE
  .\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action FullRun -DryRun

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
    "FullE2E",
    "Inventory"
  )]
  [string]$Action,

  # Optional comma-separated hotel ids for FullE2E multi-hotel traversal
  [string]$HotelIds,

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

  # FullE2E only: skip discovery executor invocation (accounting / traversal bookkeeping).
  [switch]$SkipDiscoveryExecution
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

# Safe default: DryRun unless -Apply is explicit.
$isDry = $true
if ($Apply.IsPresent) { $isDry = $false }
if ($DryRun.IsPresent) { $isDry = $true }

if ($Action -eq "FullE2E") {
  # Fourteen-segment FullE2E: accounting + optional discovery executor invocation.
  # Default invokes registered discovery executors in zero-cost / historical-replay mode.
  # Pass -SkipDiscoveryExecution for accounting-only (legacy bookkeeping).
  $e2eHotels = if (-not [string]::IsNullOrWhiteSpace($HotelIds)) {
    $HotelIds
  } elseif (-not [string]::IsNullOrWhiteSpace($HotelId)) {
    $HotelId
  } else {
    "recCAGqkERSXgiku4,recTk5I8VOzsPhxDz,recXh4NgF8sDbi9cQ"
  }
  $e2eArgs = @(
    "scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs",
    "--hotels", $e2eHotels
  )
  if ($isDry) { $e2eArgs += "--dry-run" }
  $skipDiscovery = $false
  if ($PSBoundParameters.ContainsKey("SkipDiscoveryExecution") -and $SkipDiscoveryExecution) {
    $skipDiscovery = $true
  }
  # Also honor env for CI
  if ($env:GDI_FULL_E2E_ACCOUNTING_ONLY -eq "1") { $skipDiscovery = $true }
  if (-not $skipDiscovery) {
    $e2eArgs += "--execute-discovery"
    $e2eArgs += "--require-discovery-execution"
  }
  Write-Host "GDI FullE2E: fourteen-segment hotels=$e2eHotels DryRun=$isDry ExecuteDiscovery=$(-not $skipDiscovery)" -ForegroundColor Cyan
  if ($skipDiscovery) {
    Write-Host "GDI FullE2E ACCOUNTING ONLY — discovery executors will NOT run." -ForegroundColor Yellow
  }
  & node @e2eArgs
  $e2eExit = $LASTEXITCODE
  if ($e2eExit -ne 0) {
    Write-Host "GDI FullE2E exited with code $e2eExit" -ForegroundColor Red
    exit $e2eExit
  }
  if ($skipDiscovery) {
    Write-Host "GDI FullE2E accounting finished (0 executors invoked)." -ForegroundColor Yellow
  } else {
    Write-Host "GDI FullE2E discovery execution finished — inspect reports/gdi-full-e2e-fourteen-segment-orchestrator-v1/discovery-execution-traces.json" -ForegroundColor Green
  }
  exit 0
}

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

Write-Host "GDI workflow: Action=$Action HotelId=$HotelId Environment=$Environment DryRun=$isDry" -ForegroundColor Cyan
Write-Host "Repo: $repoRoot"

& node @nodeArgs
$exit = $LASTEXITCODE
if ($exit -ne 0) {
  Write-Host "GDI workflow exited with code $exit" -ForegroundColor Red
  exit $exit
}
Write-Host "GDI workflow finished." -ForegroundColor Green
