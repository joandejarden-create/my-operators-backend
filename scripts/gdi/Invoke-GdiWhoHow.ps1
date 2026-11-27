<#
.SYNOPSIS
  GDI Full E2E V3 Stage 3 — Dedicated WHO/HOW Agent resolution.

.DESCRIPTION
  Uses completed Stage 2 dossiers as input. Finds verified direct buyers or up
  to two alternative entry points. Does not rediscover demand.

  Checkpoint A (authorized Live) invokes the canonical product-wide Exa WHO/HOW
  live adapter through this script — no ad-hoc Node bypass required.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -ContinuationOnly -Mode DryRun

.EXAMPLE
  # $0 Checkpoint A path preflight (never spends) — AuthorizationId + Manifest REQUIRED (hotel-specific)
  .\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun `
    -AuthorizationId '<FOUNDER_GO_…>' `
    -HardCeilingUsd 5 `
    -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json' `
    -ContinuationOnly -Resume -PreflightOnly

.EXAMPLE
  # Checkpoint A Live — ONLY after founder GO with hotel-specific auth + ceiling + manifest
  $env:GDI_EXA_AGENT_LIVE = '1'
  .\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode Live `
    -AuthorizePaidResearch `
    -AuthorizationId '<FOUNDER_GO_…>' `
    -HardCeilingUsd 5 `
    -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json' `
    -ContinuationOnly -Resume
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
  [string]$AuthorizationId = '',
  [double]$HardCeilingUsd = 0,
  [string]$Manifest = '',
  [switch]$PreflightOnly,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

$useCheckpointA = $PreflightOnly -or $AuthorizationId -or $Manifest -or ($HardCeilingUsd -gt 0) -or ($Mode -eq 'Live' -and $AuthorizePaidResearch)

if ($useCheckpointA) {
  Invoke-GdiWhoHowCheckpointA `
    -HotelId $HotelId `
    -Mode $Mode `
    -OpportunityId $OpportunityId `
    -OutDir $OutDir `
    -AuthorizationId $AuthorizationId `
    -HardCeilingUsd $HardCeilingUsd `
    -Manifest $Manifest `
    -ContinuationOnly:$ContinuationOnly `
    -Resume:$Resume `
    -AuthorizePaidResearch:$AuthorizePaidResearch `
    -PreflightOnly:$PreflightOnly `
    -RepoPath $RepoPath
  return
}

Invoke-GdiV3NodeStage -Stage 'WHO_HOW_RESOLUTION' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Fixtures $Fixtures `
  -ContinuationOnly:$ContinuationOnly -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch -RepoPath $RepoPath
