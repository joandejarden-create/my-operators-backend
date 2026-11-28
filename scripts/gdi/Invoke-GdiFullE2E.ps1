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

  Live (-Mode Live -AuthorizePaidResearch) is hotel-agnostic:
    * Discovery / Deep research reuse existing artifacts (never invent paid discovery;
      missing artifacts → HOLD).
    * WHO/HOW uses the existing Exa adapter and requires hotel-specific
      -AuthorizationId, -HardCeilingUsd (> 0) and -Manifest unless -SkipWhoHow.
      No Wyndham (or any hotel) default is permitted. Also needs GDI_EXA_AGENT_LIVE=1.
    * Contact is public-first; Findymail only with -AuthorizeFindymail (+ FINDYMAIL_API_KEY).
    * Airtable research persistence is separate: -PlanResearchPersistence (dry-run) or
      -AuthorizeResearchPersistence -ExactWriteSetAuthorization <text> -ApplyResearchPersistence.
      -AuthorizePaidResearch alone never unlocks Airtable writes.
    * Unified spine: with -AllowHistoricalReplay and/or -AuthorizePaidDiscovery (or explicit
      -EnableLeisure / -EnableCad; opt out with -NoLeisure / -NoCad) Live FULL also invokes the
      canonical Leisure + CAD standard stages ($0 assess/classify; reports 01b/01c), marks PLP
      assessed from the preferred_lodging_partnerships cells of the reused discovery matrix, and
      writes 07-research-receipt.json. Orchestrator never invents paid discovery.
    * Findymail persist: -PersistFindymailResults (needs -AuthorizeFindymail) plus SEPARATE auth:
      (-AuthorizeResearchPersistence + -ExactWriteSetAuthorization) OR
      (-AuthorizeFindymailPersist + -AuthorizeAirtableProductionWrites). Dry-run plan by default;
      -ApplyFindymailPersist also needs env GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES=1.
    * Surfe is never invoked. CURRENT is never published.

.PARAMETER SkipWhoHow
  Skip Stage 3 (e.g. contact-only continuation). Combine with -ContinuationOnly / -Resume
  to reuse a prior Live WHO/HOW report, or -OpportunitiesFile to supply opportunities.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun

.EXAMPLE
  .\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
    -AuthorizationId '<FOUNDER_GO_...>' -HardCeilingUsd 5 -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json'

.EXAMPLE
  .\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
    -SkipWhoHow -ContinuationOnly -Resume -AuthorizeFindymail

.NOTES
  Live is enabled when authorized. Missing authorization parameters fail closed (exit 2);
  provider authorization refusals (env gate / missing key) exit 4.
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
  [string]$OpportunitiesFile = '',
  [string]$OpportunityId = '',
  [switch]$ContinuationOnly,
  [switch]$Resume,
  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizeFindymail,
  [switch]$FindymailDryRun,

  [string]$AuthorizationId = '',
  [double]$HardCeilingUsd = 0,
  [string]$Manifest = '',

  [switch]$PlanResearchPersistence,
  [switch]$AuthorizeResearchPersistence,
  [string]$ExactWriteSetAuthorization = '',
  [switch]$ApplyResearchPersistence,

  [switch]$AuthorizePaidDiscovery,
  [switch]$AllowHistoricalReplay,
  [switch]$EnableLeisure,
  [switch]$NoLeisure,
  [switch]$EnableCad,
  [switch]$NoCad,

  [switch]$PersistFindymailResults,
  [switch]$AuthorizeFindymailPersist,
  [switch]$AuthorizeAirtableProductionWrites,
  [switch]$ApplyFindymailPersist,
  [double]$FindymailCreditHardCap = 0,

  [switch]$SkipDiscovery,
  [switch]$SkipDeep,
  [switch]$SkipWhoHow,
  [switch]$SkipContact,
  [switch]$SkipQualification,
  [switch]$SkipStagedProgression,
  [switch]$ActivateStagedPointer,
  [switch]$NoActivateStagedPointer,

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
  -OutDir $OutDir -Fixtures $Fixtures -OpportunitiesFile $OpportunitiesFile `
  -OpportunityId $OpportunityId -ContinuationOnly:$ContinuationOnly `
  -Resume:$Resume -AuthorizePaidResearch:$AuthorizePaidResearch `
  -AuthorizeFindymail:$AuthorizeFindymail -FindymailDryRun:$FindymailDryRun `
  -AuthorizationId $AuthorizationId -HardCeilingUsd $HardCeilingUsd -Manifest $Manifest `
  -PlanResearchPersistence:$PlanResearchPersistence `
  -AuthorizeResearchPersistence:$AuthorizeResearchPersistence `
  -ExactWriteSetAuthorization $ExactWriteSetAuthorization `
  -ApplyResearchPersistence:$ApplyResearchPersistence `
  -AuthorizePaidDiscovery:$AuthorizePaidDiscovery -AllowHistoricalReplay:$AllowHistoricalReplay `
  -EnableLeisure:$EnableLeisure -NoLeisure:$NoLeisure -EnableCad:$EnableCad -NoCad:$NoCad `
  -PersistFindymailResults:$PersistFindymailResults `
  -AuthorizeFindymailPersist:$AuthorizeFindymailPersist `
  -AuthorizeAirtableProductionWrites:$AuthorizeAirtableProductionWrites `
  -ApplyFindymailPersist:$ApplyFindymailPersist `
  -FindymailCreditHardCap $FindymailCreditHardCap `
  -SkipDiscovery:$SkipDiscovery -SkipDeep:$SkipDeep -SkipWhoHow:$SkipWhoHow `
  -SkipContact:$SkipContact -SkipQualification:$SkipQualification `
  -SkipStagedProgression:$SkipStagedProgression `
  -ActivateStagedPointer:$ActivateStagedPointer -NoActivateStagedPointer:$NoActivateStagedPointer `
  -RepoPath $RepoPath
