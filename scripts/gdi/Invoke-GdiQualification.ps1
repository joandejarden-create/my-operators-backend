<#
.SYNOPSIS
  GDI Full E2E V3 Stage 5 — Strict FQO + entry-point reachability.

.DESCRIPTION
  Independent six-rule FQO. Verified non-buyer entry points may satisfy named
  reachability only when the audited entry-point rule passes; other gates stay independent.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiQualification.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
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
  [switch]$Resume,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

Invoke-GdiV3NodeStage -Stage 'COMMERCIAL_QUALIFICATION' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Fixtures $Fixtures -Resume:$Resume `
  -RepoPath $RepoPath
