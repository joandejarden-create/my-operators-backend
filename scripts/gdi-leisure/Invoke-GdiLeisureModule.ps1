<#
.SYNOPSIS
  Shared PowerShell entry for one GDI Leisure Demand Intelligence module.

.DESCRIPTION
  Thin wrapper over the canonical Node LDI runner. Uses the same Full E2E
  contracts and fourteen-segment discovery wiring — not a separate engine.

  Defaults: fixture / $0 / no publication / no Findymail / no Surfe.

  Authorized Live (product-wide) requires:
    -ResearchMode live
    -AuthorizePaidResearch -AllowLivePilot
    -AuthorizationId FOUNDER_GO_…
    -HardCeilingUsd > 0
    -BudgetUSD > 0 (≤ remaining ceiling)
    $env:GDI_LDI_LIVE = '1'

.PARAMETER Module
  leisure_events | leisure_sports | leisure_attractions | leisure_universities |
  leisure_destination | leisure_distribution  (or short ids: events, sports, …)

.EXAMPLE
  .\scripts\gdi-leisure\Invoke-GdiLeisureEvents.ps1 -HotelId recaokE8psIbuVeuH -ResearchMode fixture

.EXAMPLE
  # $0 authorized-live preflight (never spends)
  .\scripts\gdi-leisure\Invoke-GdiLeisureModule.ps1 -Module leisure_events `
    -HotelId recnPB3Q02NO8Ma1o -ResearchMode live -PreflightOnly `
    -AuthorizePaidResearch -AllowLivePilot `
    -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
    -HardCeilingUsd 18 -BudgetUSD 2

.EXAMPLE
  # Authorized Live — ONLY after founder GO + IC merge of LDI path
  $env:GDI_LDI_LIVE = '1'
  .\scripts\gdi-leisure\Invoke-GdiLeisureModule.ps1 -Module leisure_events `
    -HotelId recnPB3Q02NO8Ma1o -ResearchMode live `
    -AuthorizePaidResearch -AllowLivePilot `
    -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
    -HardCeilingUsd 18 -BudgetUSD 2 -Resume
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$Module,

  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [string]$HotelProfileJson = '',

  [ValidateSet('fixture', 'replay', 'preview', 'live')]
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

  [string]$AuthorizationId = '',
  [double]$HardCeilingUsd = 0,
  [switch]$PreflightOnly,

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

$wantsLive = ($ResearchMode -eq 'live') -or $PreflightOnly
if ($wantsLive) {
  if (-not $AuthorizePaidResearch) {
    Write-FailClosed 'ResearchMode live requires -AuthorizePaidResearch.'
  }
  if (-not $AllowLivePilot) {
    Write-FailClosed 'ResearchMode live requires -AllowLivePilot.'
  }
  if (-not $AuthorizationId) {
    Write-FailClosed 'ResearchMode live requires -AuthorizationId (FOUNDER_GO_…).'
  }
  if ($HardCeilingUsd -le 0) {
    Write-FailClosed 'ResearchMode live requires -HardCeilingUsd > 0.'
  }
  if (-not $PreflightOnly -and $BudgetUSD -le 0) {
    Write-FailClosed 'Live spend requires -BudgetUSD > 0 (use -PreflightOnly for $0 readiness).'
  }
  if (-not $PreflightOnly) {
    $envLive = [string]$env:GDI_LDI_LIVE
    if ($envLive -ne '1' -and $envLive.ToLower() -ne 'true') {
      Write-FailClosed 'Live spend requires $env:GDI_LDI_LIVE = ''1'' (product-wide gate).'
    }
  }
} elseif ($AuthorizePaidResearch -and $ResearchMode -ne 'live') {
  Write-FailClosed 'AuthorizePaidResearch is only valid with -ResearchMode live (or -PreflightOnly). Fixture/replay remain $0.'
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
if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-paid-research' }
if ($AllowLivePilot) { $nodeArgs += '--allow-live-pilot' }
if ($AuthorizationId) { $nodeArgs += @('--authorization-id', $AuthorizationId) }
if ($HardCeilingUsd -gt 0) { $nodeArgs += @('--hard-ceiling-usd', ([string]$HardCeilingUsd)) }
if ($PreflightOnly) { $nodeArgs += '--preflight-only' }

Write-Host "GDI Leisure module=$Module hotel=$HotelId mode=$mode budget=$BudgetUSD preflight=$PreflightOnly" -ForegroundColor Cyan

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
