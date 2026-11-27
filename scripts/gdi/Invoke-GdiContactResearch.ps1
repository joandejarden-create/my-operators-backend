<#
.SYNOPSIS
  GDI Full E2E V3 Stage 4 — Professional contact research & verification.

.DESCRIPTION
  Runs after identity/role research. Public-first. Findymail/Surfe OFF by default.
  Never invents emails. Surfe is never auto-invoked from Full E2E.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
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

if ($AuthorizeFindymail) {
  throw 'Findymail requires a separate founder credit authorization — not enabled in this V3 DryRun runner.'
}

Invoke-GdiV3NodeStage -Stage 'PROFESSIONAL_CONTACT_RESEARCH' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Fixtures $Fixtures -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch -RepoPath $RepoPath
