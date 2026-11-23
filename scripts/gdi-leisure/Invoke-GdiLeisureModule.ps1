<#
.SYNOPSIS
  Shared PowerShell entry for one GDI Leisure Demand Intelligence module.

.DESCRIPTION
  Thin wrapper over the canonical Node LDI runner. Uses the same Full E2E
  contracts and fourteen-segment discovery wiring — not a separate engine.

  Defaults: fixture / $0 / no publication / no Findymail / no Surfe.

.PARAMETER Module
  leisure_events | leisure_sports | leisure_attractions | leisure_universities |
  leisure_destination | leisure_distribution  (or short ids: events, sports, …)

.PARAMETER HotelId
  Canonical HPC hotel record ID (rec…).

.PARAMETER ResearchMode
  fixture | replay | preview  (paid modes require AllowLivePilot + AuthorizePaidResearch)

.EXAMPLE
  .\scripts\gdi-leisure\Invoke-GdiLeisureEvents.ps1 -HotelId recaokE8psIbuVeuH -ResearchMode fixture
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$Module,

  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [string]$HotelProfileJson = '',

  [ValidateSet('fixture', 'replay', 'preview')]
  [string]$ResearchMode = 'fixture',

  [ValidateRange(0, 5000)]
  [decimal]$BudgetUSD = 0,

  [string]$EvidenceSources = 'fixture,historical_ledger',

  [string]$ExistingInventoryPath = '',

  [string]$NeedPeriodContextJson = '',

  [string]$OutDir = '',

  [switch]$Resume,
  [switch]$Replay,

  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizePublication,
  [switch]$AllowLivePilot,

  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-FailClosed([string]$Message) {
  throw $Message
}

if ($BudgetUSD -gt 0 -and -not $AuthorizePaidResearch) {
  Write-FailClosed 'Nonzero -BudgetUSD requires -AuthorizePaidResearch.'
}
if ($AuthorizePaidResearch -and -not $AllowLivePilot) {
  Write-FailClosed 'Paid leisure research requires -AllowLivePilot after founder GO.'
}
if ($AuthorizePublication) {
  Write-FailClosed 'LDI V1 refuses customer publication.'
}
if ($AuthorizePaidResearch) {
  Write-FailClosed 'LDI V1 certification path refuses paid research. Design pilot separately; do not spend here.'
}

if (-not $RepoPath) {
  $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}
$resolvedRepo = (Resolve-Path -LiteralPath $RepoPath -ErrorAction Stop).Path
$runner = Join-Path $resolvedRepo 'scripts\gdi-leisure-demand-intelligence-v1.mjs'
if (-not (Test-Path -LiteralPath $runner)) {
  Write-FailClosed "Missing LDI runner: $runner"
}

$mode = $ResearchMode
if ($Replay) { $mode = 'replay' }

$nodeArgs = @(
  $runner,
  '--hotel-id', $HotelId,
  '--modules', $Module,
  '--mode', $mode,
  '--budget-usd', ([string][double]$BudgetUSD)
)
if ($HotelProfileJson) { $nodeArgs += @('--hotel-profile-json', $HotelProfileJson) }
if ($EvidenceSources) { $nodeArgs += @('--evidence-sources', $EvidenceSources) }
if ($ExistingInventoryPath) { $nodeArgs += @('--existing-inventory', $ExistingInventoryPath) }
if ($NeedPeriodContextJson) { $nodeArgs += @('--need-period-json', $NeedPeriodContextJson) }
if ($OutDir) { $nodeArgs += @('--out', $OutDir) }
if ($Resume) { $nodeArgs += '--resume' }

Write-Host "GDI Leisure module=$Module hotel=$HotelId mode=$mode budget=$BudgetUSD" -ForegroundColor Cyan

Push-Location $resolvedRepo
try {
  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "LDI runner exited with code $LASTEXITCODE"
  }
}
finally {
  Pop-Location
}
