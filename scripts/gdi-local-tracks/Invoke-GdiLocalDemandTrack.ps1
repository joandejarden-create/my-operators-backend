<#
.SYNOPSIS
  Thin PowerShell wrapper for GDI local demand research tracks.

.DESCRIPTION
  Invokes the canonical Full E2E Node entry with --research-track.
  Does not duplicate research logic. Defaults to fixture/dry-run safety.
  Paid live flags require the same founder authorization as canonical Full E2E.

.PARAMETER Track
  social_celebrations | planner_partnerships | venue_lodging_partnerships

.PARAMETER HotelId
  Canonical hotel record id (rec…).
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('social_celebrations', 'planner_partnerships', 'venue_lodging_partnerships')]
  [string]$Track,

  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [ValidateSet('fixture', 'dry-run', 'preflight', 'live')]
  [string]$Mode = 'fixture',

  [decimal]$BudgetCeilingUsd = 0,
  [int]$MaxCandidates = 8,
  [string]$ExaEffort = 'medium',
  [string]$OutDir = '',
  [string]$RepoPath = '',

  [switch]$AuthorizePaidDiscovery,
  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizeExaAgentLive
)

$ErrorActionPreference = 'Stop'

if (-not $RepoPath) {
  $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}

$runner = Join-Path $RepoPath 'scripts\gdi-canonical-full-e2e-entry-v1.mjs'
if (-not (Test-Path $runner)) {
  throw "Canonical entry not found: $runner"
}

$nodeArgs = @(
  $runner,
  '--hotel', $HotelId,
  '--mode', $Mode,
  '--research-track', $Track,
  '--max-candidates', ([string]$MaxCandidates),
  '--exa-effort', $ExaEffort
)

if ($BudgetCeilingUsd -gt 0) {
  $nodeArgs += @('--budget-ceiling-usd', ([string]$BudgetCeilingUsd))
}
if ($OutDir) { $nodeArgs += @('--out', $OutDir) }
if ($AuthorizePaidDiscovery) { $nodeArgs += '--authorize-paid-discovery' }
if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-paid-research' }
if ($AuthorizeExaAgentLive) { $nodeArgs += '--authorize-exa-agent-live' }

Push-Location $RepoPath
try {
  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
  Pop-Location
}
