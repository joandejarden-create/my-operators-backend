<#
.SYNOPSIS
  Canonical GDI Full E2E V3 orchestrator — five permanent stages in sequence.

.DESCRIPTION
  DISCOVERY → DEEP RESEARCH → WHO/HOW → CONTACT RESEARCH → STRICT FQO
  Default DryRun. Does not create a parallel research engine.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [ValidateSet('DryRun', 'Live')]
  [string]$Mode = 'DryRun',

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

Invoke-GdiV3NodeStage -Stage 'FULL' -HotelId $HotelId -Mode $Mode `
  -OutDir $OutDir -Fixtures $Fixtures -ContinuationOnly:$ContinuationOnly `
  -Resume:$Resume -AuthorizePaidResearch:$AuthorizePaidResearch -RepoPath $RepoPath
