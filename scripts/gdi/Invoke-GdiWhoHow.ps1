<#
.SYNOPSIS
  GDI Full E2E V3 Stage 3 — Dedicated WHO/HOW Agent resolution.

.DESCRIPTION
  Uses completed Stage 2 dossiers as input. Finds verified direct buyers or up
  to two alternative entry points. Does not rediscover demand.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -ContinuationOnly -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -OpportunityId fx_v3_b_two_ep -Mode DryRun
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
  [string]$Fixtures = '',
  [switch]$ContinuationOnly,
  [switch]$Resume,
  [switch]$AuthorizePaidResearch,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

Invoke-GdiV3NodeStage -Stage 'WHO_HOW_RESOLUTION' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Fixtures $Fixtures `
  -ContinuationOnly:$ContinuationOnly -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch -RepoPath $RepoPath
