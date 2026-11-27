<#
.SYNOPSIS
  GDI Full E2E V3 Stage 2 — Opportunity deep research (COMMERCIAL_FULL_E2E).

.DESCRIPTION
  Retains the commercial research queue from PR #612. Missing buyer info does
  not terminate credible opportunities — they route to Stage 3 WHO/HOW.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiDeepResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
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
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

# BatchSize reserved for Live Exa queue adapter (DryRun ignores).
$null = $BatchSize

Invoke-GdiV3NodeStage -Stage 'OPPORTUNITY_DEEP_RESEARCH' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch -RepoPath $RepoPath
