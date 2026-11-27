<#
.SYNOPSIS
  Canonical GDI Full E2E V3 orchestrator — five permanent research stages plus
  mandatory STAGED progression (FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT).

.DESCRIPTION
  DISCOVERY → DEEP RESEARCH → WHO/HOW → CONTACT RESEARCH → STRICT FQO
  → STAGED package generation + local inventory validation

  Canonical publication lifecycle (permanent):
    FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT

  An authorized completed E2E run is not operationally complete until successfully
  qualified results are visible in STAGED (Live activates STAGED; DryRun validates
  package construction without pointer write). CURRENT never changes from this
  orchestrator — founder approval is a separate gate.

  Default DryRun. Does not create a parallel research engine.
  DryRun and research budget controls remain enforced.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode Live -AuthorizePaidResearch

.NOTES
  Generic V3 Live paid execution remains refused by the stage CLI until the
  hotel-agnostic Live orchestrator gap is closed. Use stage-specific authorized
  runners (WHO/HOW Checkpoint A, Contact + Findymail) for paid Live stages.
  Surfe is never invoked from Full E2E.
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
  [switch]$AuthorizeFindymail,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

if ($AuthorizeFindymail -and $Mode -ne 'Live') {
  throw 'AuthorizeFindymail requires -Mode Live.'
}
if ($AuthorizeFindymail -and -not $AuthorizePaidResearch) {
  throw 'AuthorizeFindymail requires -AuthorizePaidResearch.'
}

Write-Host "GDI Full E2E lifecycle: FULL_E2E → STAGED → FOUNDER_APPROVAL → CURRENT"
Write-Host "Mode=$Mode HotelId=$HotelId (STAGED progression mandatory; CURRENT never auto-published)"

Invoke-GdiV3NodeStage -Stage 'FULL' -HotelId $HotelId -Mode $Mode `
  -OutDir $OutDir -Fixtures $Fixtures -ContinuationOnly:$ContinuationOnly `
  -Resume:$Resume -AuthorizePaidResearch:$AuthorizePaidResearch `
  -AuthorizeFindymail:$AuthorizeFindymail -RepoPath $RepoPath
