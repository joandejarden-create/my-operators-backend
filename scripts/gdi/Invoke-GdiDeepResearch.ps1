<#
.SYNOPSIS
  GDI Full E2E V3 Stage 2 — Opportunity deep research (COMMERCIAL_FULL_E2E).

.DESCRIPTION
  Retains the commercial research queue from PR #612. Missing buyer info does
  not terminate credible opportunities — they route to Stage 3 WHO/HOW.
  Live (when authorized) reuses settled queue dossiers; it does not seed new paid
  deep research. Missing artifacts yield HOLD.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiDeepResearch.ps1 -HotelId <rec...> -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiDeepResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [ValidateSet('DryRun', 'Live')]
  [string]$Mode = 'DryRun',

  [string]$OpportunityId = '',
  [string]$OutDir = '',
  [ValidateRange(1, 500)]
  [int]$BatchSize = 20,
  [switch]$Resume,
  [switch]$AuthorizePaidResearch,

  # Accepted for runner parity (Live Full E2E params); not used by reuse-only deep research.
  [string]$AuthorizationId = '',
  [double]$HardCeilingUsd = 0,
  [string]$Manifest = '',
  [switch]$PlanResearchPersistence,
  [switch]$AuthorizeResearchPersistence,
  [string]$ExactWriteSetAuthorization = '',
  [switch]$ApplyResearchPersistence,

  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

# BatchSize reserved for a future Live Exa queue adapter (reuse-only today).
$null = $BatchSize

Invoke-GdiV3NodeStage -Stage 'OPPORTUNITY_DEEP_RESEARCH' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch `
  -AuthorizationId $AuthorizationId -HardCeilingUsd $HardCeilingUsd -Manifest $Manifest `
  -PlanResearchPersistence:$PlanResearchPersistence `
  -AuthorizeResearchPersistence:$AuthorizeResearchPersistence `
  -ExactWriteSetAuthorization $ExactWriteSetAuthorization `
  -ApplyResearchPersistence:$ApplyResearchPersistence `
  -RepoPath $RepoPath
