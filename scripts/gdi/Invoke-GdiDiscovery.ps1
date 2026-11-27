<#
.SYNOPSIS
  GDI Full E2E V3 Stage 1 — Demand Discovery.

.DESCRIPTION
  Reuses the existing fourteen-segment discovery system, leisure applicability,
  CAD, and PLP. DryRun inventories existing artifacts; Live requires authorization.

.PARAMETER HotelId
  Canonical HPC hotel record id (rec…).

.PARAMETER Mode
  DryRun (default) | Live

.EXAMPLE
  .\scripts\gdi\Invoke-GdiDiscovery.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
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
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

Invoke-GdiV3NodeStage -Stage 'DEMAND_DISCOVERY' -HotelId $HotelId -Mode $Mode `
  -OutDir $OutDir -Resume:$Resume -AuthorizePaidResearch:$AuthorizePaidResearch `
  -RepoPath $RepoPath
