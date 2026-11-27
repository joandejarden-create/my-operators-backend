<#
.SYNOPSIS
  GDI Full E2E V3 Stage 4 — Professional contact research & verification.

.DESCRIPTION
  Runs after identity/role research. Public-first for email / phone / LinkedIn.
  Findymail is a permanent Stage 4 component for unresolved professional email
  gaps when -AuthorizeFindymail is supplied with Live + paid authorization.
  Surfe is never auto-invoked from Full E2E (explicit-click only).

.EXAMPLE
  .\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode Live -AuthorizePaidResearch -AuthorizeFindymail
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
  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizeFindymail,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

if ($AuthorizeFindymail -and $Mode -ne 'Live') {
  throw 'AuthorizeFindymail requires -Mode Live (use certification dry-run script for planning).'
}
if ($AuthorizeFindymail -and -not $AuthorizePaidResearch) {
  throw 'AuthorizeFindymail requires -AuthorizePaidResearch.'
}

Invoke-GdiV3NodeStage -Stage 'PROFESSIONAL_CONTACT_RESEARCH' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Fixtures $Fixtures -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch -AuthorizeFindymail:$AuthorizeFindymail `
  -RepoPath $RepoPath
