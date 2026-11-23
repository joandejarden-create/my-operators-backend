<#
.SYNOPSIS
  Standardized founder/operator PowerShell interface for multi-hotel GDI processing.

.DESCRIPTION
  Thin portfolio orchestrator over the canonical Node Full E2E engine.
  Does NOT implement a competing research pipeline.

  Defaults (fail-closed):
    - Mode Preview / Rehearsal
    - Budget $0 / no paid research
    - No publication / no production Airtable writes
    - No weekly automation activation
    - Sequential per-hotel execution with independent run IDs

  Paid research and publication are INDEPENDENT authorizations.
  Certification / Rehearsal modes refuse paid and publication flags.

.PARAMETER HotelId
  Single canonical HPC hotel record ID (rec…).

.PARAMETER Hotels
  Comma-separated hotel IDs.

.PARAMETER HotelsFile
  Path to a text (one ID per line) or JSON hotel list.

.PARAMETER Mode
  Preview   - eligibility preflight only ($0)
  FullE2E   - canonical dry-run per hotel unless live pilot flags + AllowLivePilot
  Rehearsal  - zero-paid certification rehearsal (default for cert)
  Resume    - continue from portfolio state file after partial failures
  AuditOnly - capability matrix + scores only

.PARAMETER BudgetUSD
  Aggregate research ceiling. Requires -AuthorizePaidResearch and -AllowLivePilot.

.PARAMETER AuthorizePaidResearch
  Explicit paid research authorization (independent of publication).

.PARAMETER AuthorizePublication
  Explicit customer publication authorization (independent of paid research).

.PARAMETER ReportPath
  Output report directory.

.PARAMETER ResumeState
  Path to portfolio state JSON for -Mode Resume.

.PARAMETER AllowLivePilot
  Required in addition to paid/publication flags. Without it, this launcher stays $0.

.PARAMETER LeisureModules
  Optional CSV of LDI modules (events,sports,attractions,universities,destination,distribution or leisure_* ids).
  When set (or -LeisureOnly), runs Leisure Demand Intelligence via the shared Node LDI runner
  using the same hotel cohort inputs — does not replace the 14-segment engine.

.PARAMETER LeisureOnly
  Run only LDI modules (auto-select per hotel when -LeisureModules omitted).

.PARAMETER DisableLeisure
  Explicitly confirm leisure modules are off (default). Fourteen-segment path unchanged.

.EXAMPLE
  # Safe preview for one hotel
  .\scripts\Invoke-GdiPortfolioProcessing.ps1 -HotelId recLuxvwwxID7U2B8 -Mode Preview

.EXAMPLE
  # Certification cohort rehearsal ($0)
  .\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\gdi-portfolio-processing-certification-v1\cert-cohort-hotels.txt

.EXAMPLE
  # Leisure modules only (fixture / $0)
  .\scripts\Invoke-GdiPortfolioProcessing.ps1 -HotelId recaokE8psIbuVeuH -LeisureOnly -Mode Preview

.EXAMPLE
  # Resume after partial failure
  .\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Resume -ResumeState reports\gdi-portfolio-processing-certification-v1\<run>.state.json -HotelsFile reports\gdi-portfolio-processing-certification-v1\cert-cohort-hotels.txt
#>
[CmdletBinding()]
param(
  [string]$HotelId = '',

  [string]$Hotels = '',

  [string]$HotelsFile = '',

  [ValidateSet('Preview', 'FullE2E', 'Rehearsal', 'Resume', 'AuditOnly', 'PaidPilotPreflight', 'PaidPilotExecute', 'PaidPilotReplay', 'DesignHotelsLivePreflight', 'DesignHotelsLiveExecute', 'DesignHotelsLiveReplay', 'CohortPreflight', 'CohortExecute', 'CohortReplay')]
  [string]$Mode = 'Rehearsal',

  [ValidateRange(0, 5000)]
  [decimal]$BudgetUSD = 0,

  [ValidateRange(0, 100)]
  [decimal]$PerHotelBudgetUSD = 0,

  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizePublication,
  [switch]$AuthorizeProductionWrites,
  [switch]$AllowLivePilot,

  [string]$FounderSpendConfirmation = '',

  [string]$ReportPath = '',

  [string]$ResumeState = '',

  [string]$PortfolioRunId = '',

  [ValidateRange(1, 50)]
  [int]$MaxCandidates = 8,

  [string]$LeisureModules = '',

  [switch]$LeisureOnly,

  [switch]$DisableLeisure,

  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-FailClosed([string]$Message) {
  throw $Message
}

if ($BudgetUSD -gt 0 -and -not $AuthorizePaidResearch) {
  Write-FailClosed 'A nonzero budget requires -AuthorizePaidResearch. Nothing was executed.'
}
if ($AuthorizePaidResearch -and $BudgetUSD -le 0) {
  Write-FailClosed '-AuthorizePaidResearch requires -BudgetUSD greater than zero.'
}
if (
  ($AuthorizePaidResearch -or $AuthorizePublication -or $AuthorizeProductionWrites -or $BudgetUSD -gt 0) -and
  -not $AllowLivePilot
) {
  Write-FailClosed 'Paid research, publication, production writes, and nonzero budget require -AllowLivePilot after explicit founder approval. Certification/rehearsal stays at $0.'
}
if ($Mode -eq 'Rehearsal' -and ($AuthorizePaidResearch -or $AuthorizePublication -or $BudgetUSD -gt 0)) {
  Write-FailClosed 'Rehearsal mode refuses paid research and publication. Use Mode FullE2E with -AllowLivePilot only after founder GO.'
}
if ($Mode -eq 'PaidPilotExecute') {
  if (-not $AllowLivePilot) {
    Write-FailClosed 'PaidPilotExecute requires -AllowLivePilot.'
  }
  if (-not $AuthorizePaidResearch) {
    Write-FailClosed 'PaidPilotExecute requires -AuthorizePaidResearch.'
  }
  if ($BudgetUSD -le 0 -or $BudgetUSD -gt 25) {
    Write-FailClosed 'PaidPilotExecute requires -BudgetUSD in (0, 25].'
  }
  if ($PerHotelBudgetUSD -le 0 -or $PerHotelBudgetUSD -gt 12.5) {
    Write-FailClosed 'PaidPilotExecute requires -PerHotelBudgetUSD in (0, 12.5].'
  }
  if ($FounderSpendConfirmation -ne 'FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25') {
    Write-FailClosed 'PaidPilotExecute requires -FounderSpendConfirmation FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25.'
  }
  if ($AuthorizePublication -or $AuthorizeProductionWrites) {
    Write-FailClosed 'Paid pilot refuses publication and production writes.'
  }
}
if ($Mode -eq 'DesignHotelsLiveExecute') {
  if (-not $AllowLivePilot) {
    Write-FailClosed 'DesignHotelsLiveExecute requires -AllowLivePilot.'
  }
  if (-not $AuthorizePaidResearch) {
    Write-FailClosed 'DesignHotelsLiveExecute requires -AuthorizePaidResearch.'
  }
  if ($BudgetUSD -le 0 -or $BudgetUSD -gt 12) {
    Write-FailClosed 'DesignHotelsLiveExecute requires -BudgetUSD in (0, 12].'
  }
  if ($PerHotelBudgetUSD -le 0 -or $PerHotelBudgetUSD -gt 4) {
    Write-FailClosed 'DesignHotelsLiveExecute requires -PerHotelBudgetUSD in (0, 4].'
  }
  if ($FounderSpendConfirmation -ne 'FOUNDER_GDI_DESIGN_HOTELS_LIVE_RESEARCH_12') {
    Write-FailClosed 'DesignHotelsLiveExecute requires -FounderSpendConfirmation FOUNDER_GDI_DESIGN_HOTELS_LIVE_RESEARCH_12.'
  }
  if ($AuthorizePublication -or $AuthorizeProductionWrites) {
    Write-FailClosed 'Design Hotels live research refuses publication and production writes.'
  }
}
if ($Mode -eq 'CohortPreflight' -or $Mode -eq 'CohortExecute' -or $Mode -eq 'CohortReplay') {
  if (-not $HotelsFile -and -not $HotelId -and -not $Hotels) {
    Write-FailClosed 'Cohort modes require -HotelsFile, -Hotels, or -HotelId (dynamic cohort — no hard-coded hotel list).'
  }
}
if ($Mode -eq 'CohortExecute') {
  if (-not $AllowLivePilot) {
    Write-FailClosed 'CohortExecute requires -AllowLivePilot.'
  }
  if (-not $AuthorizePaidResearch) {
    Write-FailClosed 'CohortExecute requires -AuthorizePaidResearch.'
  }
  if ($BudgetUSD -le 0 -or $BudgetUSD -gt 5000) {
    Write-FailClosed 'CohortExecute requires -BudgetUSD greater than zero.'
  }
  if ($PerHotelBudgetUSD -le 0) {
    Write-FailClosed 'CohortExecute requires -PerHotelBudgetUSD greater than zero.'
  }
  if ($FounderSpendConfirmation -ne 'FOUNDER_GDI_UNIVERSAL_COHORT_PAID') {
    Write-FailClosed 'CohortExecute requires -FounderSpendConfirmation FOUNDER_GDI_UNIVERSAL_COHORT_PAID.'
  }
  if ($AuthorizePublication -or $AuthorizeProductionWrites) {
    Write-FailClosed 'Universal cohort processing refuses publication and production writes.'
  }
}

if (-not $RepoPath) {
  $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$resolvedRepo = (Resolve-Path -LiteralPath $RepoPath -ErrorAction Stop).Path
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  Write-FailClosed 'Node.js is not on PATH.'
}

$runLeisure = $LeisureOnly.IsPresent -or ($LeisureModules -and $LeisureModules.Trim().Length -gt 0)
if ($DisableLeisure.IsPresent -and $runLeisure) {
  Write-FailClosed 'Cannot combine -DisableLeisure with -LeisureOnly / -LeisureModules.'
}

# Leisure-only path: same hotel cohort args, canonical LDI Node runner (fixture/$0).
if ($runLeisure) {
  $ldiRunner = Join-Path $resolvedRepo 'scripts\gdi-leisure-demand-intelligence-v1.mjs'
  if (-not (Test-Path -LiteralPath $ldiRunner)) {
    Write-FailClosed "Missing LDI runner: $ldiRunner"
  }
  if ($Mode -eq 'AuditOnly' -and -not $HotelId) {
    $ldiArgs = @($ldiRunner, '--coverage-only')
    Write-Host 'GDI Leisure coverage matrix (AuditOnly)' -ForegroundColor Cyan
    Push-Location $resolvedRepo
    try {
      & node @ldiArgs
      if ($LASTEXITCODE -ne 0) { throw "GDI leisure coverage exited with code $LASTEXITCODE" }
    } finally { Pop-Location }
    if ($LeisureOnly) { return }
  }
  if (-not $HotelId) {
    Write-FailClosed 'Leisure portfolio path currently requires -HotelId (HotelsFile multi-hotel leisure batch: invoke per hotel or pass modules via Node CLI).'
  }
  $modArg = if ($LeisureModules -and $LeisureModules.Trim().Length -gt 0) { $LeisureModules } else { 'all' }
  # Preview/Rehearsal/FullE2E use mock_live ($0 mocked providers → STAGED plan).
  # AuditOnly coverage stays fixture/coverage-only above.
  $ldiMode = if ($Mode -eq 'Preview' -or $Mode -eq 'Rehearsal' -or $Mode -eq 'FullE2E') { 'mock_live' } else { 'fixture' }
  $ldiArgs = @(
    $ldiRunner,
    '--hotel-id', $HotelId,
    '--modules', $modArg,
    '--mode', $ldiMode,
    '--budget-usd', ([string][double]$BudgetUSD)
  )
  if ($ReportPath) { $ldiArgs += @('--out', $ReportPath) }
  Write-Host "GDI Portfolio Processing + Leisure: HotelId=$HotelId Modules=$modArg Mode=$ldiMode" -ForegroundColor Cyan
  Push-Location $resolvedRepo
  try {
    & node @ldiArgs
    if ($LASTEXITCODE -ne 0) {
      throw "GDI leisure runner exited with code $LASTEXITCODE"
    }
    Write-Host "Leisure module run finished (`$0 $ldiMode path). Feeds same GDI inventory gates; no separate LDI DB." -ForegroundColor Green
  }
  finally {
    Pop-Location
  }
  if ($LeisureOnly) {
    return
  }
  Write-Host 'Continuing with standard fourteen-segment portfolio runner…' -ForegroundColor DarkCyan
}

$isPaidPilotMode = ($Mode -eq 'PaidPilotPreflight' -or $Mode -eq 'PaidPilotExecute' -or $Mode -eq 'PaidPilotReplay')
$isDhLiveMode = ($Mode -eq 'DesignHotelsLivePreflight' -or $Mode -eq 'DesignHotelsLiveExecute' -or $Mode -eq 'DesignHotelsLiveReplay')
$isUniversalCohortMode = ($Mode -eq 'CohortPreflight' -or $Mode -eq 'CohortExecute' -or $Mode -eq 'CohortReplay')
$runner = if ($isUniversalCohortMode) {
  Join-Path $resolvedRepo 'scripts\gdi-universal-cohort-processing-v2.mjs'
} elseif ($isDhLiveMode) {
  Join-Path $resolvedRepo 'scripts\gdi-design-hotels-live-research-pilot-v1.mjs'
} elseif ($isPaidPilotMode) {
  Join-Path $resolvedRepo 'scripts\gdi-portfolio-cert-v1-1-paid-pilot.mjs'
} else {
  Join-Path $resolvedRepo 'scripts\gdi-portfolio-processing-certification-v1.mjs'
}
if (-not (Test-Path -LiteralPath $runner)) {
  Write-FailClosed "Missing portfolio runner: $runner"
}

$modeArg = switch ($Mode) {
  'AuditOnly' { 'audit-only' }
  'FullE2E' { 'full-e2e' }
  'Preview' { 'preview' }
  'Resume' { 'resume' }
  'Rehearsal' { 'rehearsal' }
  'PaidPilotPreflight' { 'preflight' }
  'PaidPilotExecute' { 'execute-paid' }
  'PaidPilotReplay' { 'replay' }
  'DesignHotelsLivePreflight' { 'preflight' }
  'DesignHotelsLiveExecute' { 'execute-paid' }
  'DesignHotelsLiveReplay' { 'replay' }
  'CohortPreflight' { 'preflight' }
  'CohortExecute' { 'execute-paid' }
  'CohortReplay' { 'replay' }
  default { $Mode.ToLowerInvariant() }
}

$nodeArgs = @(
  $runner,
  '--mode', $modeArg
)
if ($Mode -eq 'PaidPilotExecute') {
  $nodeArgs += @('--confirm-founder-spend-go', $FounderSpendConfirmation)
  $env:GDI_EXA_AGENT_LIVE = '1'
}
if ($Mode -eq 'DesignHotelsLiveExecute') {
  $nodeArgs += @('--confirm-founder-spend-go', $FounderSpendConfirmation)
  $env:GDI_EXA_AGENT_LIVE = '1'
}
if ($Mode -eq 'CohortExecute') {
  $nodeArgs += @('--confirm-founder-spend-go', $FounderSpendConfirmation)
  $env:GDI_EXA_AGENT_LIVE = '1'
  if ($BudgetUSD -gt 0) { $nodeArgs += @('--budget-usd', ([string][double]$BudgetUSD)) }
  if ($PerHotelBudgetUSD -gt 0) { $nodeArgs += @('--per-hotel-budget-usd', ([string][double]$PerHotelBudgetUSD)) }
}

if (-not $isPaidPilotMode -and -not $isDhLiveMode -and -not $isUniversalCohortMode) {
  $hotelList = New-Object System.Collections.Generic.List[string]
  if ($HotelId) { [void]$hotelList.Add($HotelId.Trim()) }
  if ($Hotels) {
    foreach ($part in ($Hotels -split '[,;\s]+')) {
      if ($part.Trim()) { [void]$hotelList.Add($part.Trim()) }
    }
  }
  if ($HotelsFile) {
    $nodeArgs += @('--hotels-file', $HotelsFile)
  }
  elseif ($hotelList.Count -gt 0) {
    $nodeArgs += @('--hotels', ($hotelList -join ','))
  }
}

if ($isUniversalCohortMode) {
  $hotelList = New-Object System.Collections.Generic.List[string]
  if ($HotelId) { [void]$hotelList.Add($HotelId.Trim()) }
  if ($Hotels) {
    foreach ($part in ($Hotels -split '[,;\s]+')) {
      if ($part.Trim()) { [void]$hotelList.Add($part.Trim()) }
    }
  }
  if ($HotelsFile) {
    $nodeArgs += @('--hotels-file', $HotelsFile)
  }
  elseif ($hotelList.Count -gt 0) {
    $nodeArgs += @('--hotels', ($hotelList -join ','))
  }
  if ($Mode -eq 'CohortPreflight' -or $Mode -eq 'CohortReplay') {
    if ($BudgetUSD -gt 0) { $nodeArgs += @('--budget-usd', ([string][double]$BudgetUSD)) }
    if ($PerHotelBudgetUSD -gt 0) { $nodeArgs += @('--per-hotel-budget-usd', ([string][double]$PerHotelBudgetUSD)) }
  }
}

if ($ReportPath) { $nodeArgs += @('--out', $ReportPath) }
if ($PortfolioRunId) { $nodeArgs += @('--portfolio-run-id', $PortfolioRunId) }
if (-not $isPaidPilotMode -and -not $isDhLiveMode -and -not $isUniversalCohortMode -and ($Mode -eq 'Resume' -or $ResumeState)) {
  $nodeArgs += '--resume'
  if ($ResumeState) { $nodeArgs += @('--state', $ResumeState) }
}
if ($MaxCandidates) { $nodeArgs += @('--max-candidates', [string]$MaxCandidates) }

if (-not $isPaidPilotMode -and -not $isDhLiveMode -and -not $isUniversalCohortMode -and $AllowLivePilot) {
  $nodeArgs += '--allow-live-pilot'
  if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-paid-research' }
  if ($AuthorizePublication) { $nodeArgs += '--authorize-publication' }
  if ($AuthorizeProductionWrites) { $nodeArgs += '--authorize-production-writes' }
  if ($BudgetUSD -gt 0) { $nodeArgs += @('--budget-usd', ([string][double]$BudgetUSD)) }
  if ($PerHotelBudgetUSD -gt 0) { $nodeArgs += @('--per-hotel-budget-usd', ([string][double]$PerHotelBudgetUSD)) }
}

Write-Host "GDI Portfolio Processing: Mode=$Mode BudgetUSD=$BudgetUSD AllowLivePilot=$([bool]$AllowLivePilot.IsPresent)" -ForegroundColor Cyan
Write-Host "Repo: $resolvedRepo"
Write-Host "Runner: $runner"
Write-Warning 'Canonical Node engine enforces spend ledger, publication gates, and persistence fail-closed. PowerShell alone does not spend or publish.'

Push-Location $resolvedRepo
try {
  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "GDI portfolio certification runner exited with code $LASTEXITCODE"
  }
  Write-Host 'Portfolio run finished. Review reports under reports/gdi-portfolio-processing-certification-v1/ (or -ReportPath).' -ForegroundColor Green
  Write-Host 'Do not declare CERTIFIED_FOR_SCALING from exit code alone — check readiness-verdict.json.'
}
finally {
  Pop-Location
}
