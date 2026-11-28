<#
.SYNOPSIS
  GDI Full E2E V3 Stage 4 — Professional contact research & verification.

.DESCRIPTION
  Runs after identity/role research. Public-first for email / phone / LinkedIn.
  Enforces GDI_CONTACT_RESEARCH_BEST_EFFORT_V1: every applicable channel for
  primary and alternative contacts must be researched (finding one channel does
  not terminate the others). Findymail is a permanent Stage 4 component for
  unresolved professional email gaps when -AuthorizeFindymail is supplied with
  Live + paid authorization (FINDYMAIL_API_KEY required; fails closed otherwise).
  Surfe is never auto-invoked from Full E2E (explicit-click only).

  Live reads opportunities from -OpportunitiesFile, else from a prior Live
  04-contact-research / 03-who-how report in -OutDir. Use -FindymailDryRun to plan
  Findymail eligibility with no spend.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
    -OpportunitiesFile reports/<hotel>/opportunities.json -AuthorizeFindymail

.NOTES
  Live is enabled when authorized.
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
  [string]$OpportunitiesFile = '',
  [switch]$Resume,
  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizeFindymail,
  [switch]$FindymailDryRun,

  # Accepted for runner parity; contact-only Live does not call WHO/HOW.
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

if ($AuthorizeFindymail -and $Mode -ne 'Live') {
  throw 'AuthorizeFindymail requires -Mode Live (use -FindymailDryRun with Live to plan eligibility without spend).'
}
if ($AuthorizeFindymail -and -not $AuthorizePaidResearch) {
  throw 'AuthorizeFindymail requires -AuthorizePaidResearch.'
}

Invoke-GdiV3NodeStage -Stage 'PROFESSIONAL_CONTACT_RESEARCH' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Fixtures $Fixtures `
  -OpportunitiesFile $OpportunitiesFile -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch -AuthorizeFindymail:$AuthorizeFindymail `
  -FindymailDryRun:$FindymailDryRun `
  -AuthorizationId $AuthorizationId -HardCeilingUsd $HardCeilingUsd -Manifest $Manifest `
  -PlanResearchPersistence:$PlanResearchPersistence `
  -AuthorizeResearchPersistence:$AuthorizeResearchPersistence `
  -ExactWriteSetAuthorization $ExactWriteSetAuthorization `
  -ApplyResearchPersistence:$ApplyResearchPersistence `
  -RepoPath $RepoPath
