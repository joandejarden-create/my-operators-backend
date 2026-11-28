<#
.SYNOPSIS
  GDI Full E2E V3 Stage 5 — Strict FQO + entry-point reachability.

.DESCRIPTION
  Independent six-rule FQO. Verified non-buyer entry points may satisfy named
  reachability only when the audited entry-point rule passes; other gates stay independent.
  Live (when authorized) evaluates opportunities from -OpportunitiesFile or a prior
  Live contact / WHO-HOW report in -OutDir; it never spends and never activates STAGED.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiQualification.ps1 -HotelId <rec...> -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiQualification.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
    -OpportunitiesFile reports/<hotel>/opportunities.json
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

  # Accepted for runner parity (Live Full E2E params); qualification never spends.
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

Invoke-GdiV3NodeStage -Stage 'COMMERCIAL_QUALIFICATION' -HotelId $HotelId -Mode $Mode `
  -OpportunityId $OpportunityId -OutDir $OutDir -Fixtures $Fixtures `
  -OpportunitiesFile $OpportunitiesFile -Resume:$Resume `
  -AuthorizePaidResearch:$AuthorizePaidResearch `
  -AuthorizationId $AuthorizationId -HardCeilingUsd $HardCeilingUsd -Manifest $Manifest `
  -PlanResearchPersistence:$PlanResearchPersistence `
  -AuthorizeResearchPersistence:$AuthorizeResearchPersistence `
  -ExactWriteSetAuthorization $ExactWriteSetAuthorization `
  -ApplyResearchPersistence:$ApplyResearchPersistence `
  -RepoPath $RepoPath
