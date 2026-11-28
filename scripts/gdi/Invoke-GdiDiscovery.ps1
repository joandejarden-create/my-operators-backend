<#
.SYNOPSIS
  GDI Full E2E V3 Stage 1 — Demand Discovery.

.DESCRIPTION
  Reuses the existing fourteen-segment discovery system, leisure applicability,
  CAD, and PLP. DryRun inventories existing artifacts. Live (when authorized)
  uses the same reuse semantics: it never invents paid discovery, and missing
  artifacts yield HOLD (not a silent PASS).

.PARAMETER HotelId
  Canonical HPC hotel record id (rec…).

.PARAMETER Mode
  DryRun (default) | Live

.EXAMPLE
  .\scripts\gdi\Invoke-GdiDiscovery.ps1 -HotelId <rec...> -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiDiscovery.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [ValidateSet('DryRun', 'Live')]
  [string]$Mode = 'DryRun',

  [string]$OutDir = '',
  [switch]$Resume,
  [switch]$AuthorizePaidResearch,

  # Accepted for runner parity (Live Full E2E params); not used by reuse-only discovery.
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

Invoke-GdiV3NodeStage -Stage 'DEMAND_DISCOVERY' -HotelId $HotelId -Mode $Mode `
  -OutDir $OutDir -Resume:$Resume -AuthorizePaidResearch:$AuthorizePaidResearch `
  -AuthorizationId $AuthorizationId -HardCeilingUsd $HardCeilingUsd -Manifest $Manifest `
  -PlanResearchPersistence:$PlanResearchPersistence `
  -AuthorizeResearchPersistence:$AuthorizeResearchPersistence `
  -ExactWriteSetAuthorization $ExactWriteSetAuthorization `
  -ApplyResearchPersistence:$ApplyResearchPersistence `
  -RepoPath $RepoPath
