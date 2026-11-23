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

.EXAMPLE
  # Safe preview for one hotel
  .\scripts\Invoke-GdiPortfolioProcessing.ps1 -HotelId recLuxvwwxID7U2B8 -Mode Preview

.EXAMPLE
  # Certification cohort rehearsal ($0)
  .\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\gdi-portfolio-processing-certification-v1\cert-cohort-hotels.txt

.EXAMPLE
  # Resume after partial failure
  .\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Resume -ResumeState reports\gdi-portfolio-processing-certification-v1\<run>.state.json -HotelsFile reports\gdi-portfolio-processing-certification-v1\cert-cohort-hotels.txt
#>
[CmdletBinding()]
param(
  [string]$HotelId = '',

  [string]$Hotels = '',

  [string]$HotelsFile = '',

  [ValidateSet('Preview', 'FullE2E', 'Rehearsal', 'Resume', 'AuditOnly')]
  [string]$Mode = 'Rehearsal',

  [ValidateRange(0, 5000)]
  [decimal]$BudgetUSD = 0,

  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizePublication,
  [switch]$AuthorizeProductionWrites,
  [switch]$AllowLivePilot,

  [string]$ReportPath = '',

  [string]$ResumeState = '',

  [string]$PortfolioRunId = '',

  [ValidateRange(1, 50)]
  [int]$MaxCandidates = 8,

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

if (-not $RepoPath) {
  $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$resolvedRepo = (Resolve-Path -LiteralPath $RepoPath -ErrorAction Stop).Path
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  Write-FailClosed 'Node.js is not on PATH.'
}

$runner = Join-Path $resolvedRepo 'scripts\gdi-portfolio-processing-certification-v1.mjs'
if (-not (Test-Path -LiteralPath $runner)) {
  Write-FailClosed "Missing portfolio certification runner: $runner"
}

$modeArg = switch ($Mode) {
  'AuditOnly' { 'audit-only' }
  'FullE2E' { 'full-e2e' }
  'Preview' { 'preview' }
  'Resume' { 'resume' }
  'Rehearsal' { 'rehearsal' }
  default { $Mode.ToLowerInvariant() }
}

$nodeArgs = @(
  $runner,
  '--mode', $modeArg
)

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

if ($ReportPath) { $nodeArgs += @('--out', $ReportPath) }
if ($PortfolioRunId) { $nodeArgs += @('--portfolio-run-id', $PortfolioRunId) }
if ($Mode -eq 'Resume' -or $ResumeState) {
  $nodeArgs += '--resume'
  if ($ResumeState) { $nodeArgs += @('--state', $ResumeState) }
}
if ($MaxCandidates) { $nodeArgs += @('--max-candidates', [string]$MaxCandidates) }

if ($AllowLivePilot) {
  $nodeArgs += '--allow-live-pilot'
  if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-paid-research' }
  if ($AuthorizePublication) { $nodeArgs += '--authorize-publication' }
  if ($AuthorizeProductionWrites) { $nodeArgs += '--authorize-production-writes' }
  if ($BudgetUSD -gt 0) { $nodeArgs += @('--budget-usd', ([string][double]$BudgetUSD)) }
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
