# Shared helpers for GDI Full E2E V3 PowerShell runners.
Set-StrictMode -Version Latest

function Get-GdiRepoRoot {
  param([string]$RepoPath = '')
  if ($RepoPath) {
    return (Resolve-Path -LiteralPath $RepoPath -ErrorAction Stop).Path
  }
  return (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}

function Invoke-GdiV3NodeStage {
  param(
    [Parameter(Mandatory = $true)][string]$Stage,
    [Parameter(Mandatory = $true)][string]$HotelId,
    [ValidateSet('DryRun', 'Live')][string]$Mode = 'DryRun',
    [string]$OpportunityId = '',
    [string]$OutDir = '',
    [string]$Fixtures = '',
    [string]$OpportunitiesFile = '',
    [switch]$ContinuationOnly,
    [switch]$Resume,
    [switch]$AuthorizePaidResearch,
    [switch]$AuthorizeFindymail,
    [switch]$FindymailDryRun,
    # Live WHO/HOW (hotel-specific; never defaulted)
    [string]$AuthorizationId = '',
    [double]$HardCeilingUsd = 0,
    [string]$Manifest = '',
    # Airtable research persistence (separate from paid research authorization)
    [switch]$PlanResearchPersistence,
    [switch]$AuthorizeResearchPersistence,
    [string]$ExactWriteSetAuthorization = '',
    [switch]$ApplyResearchPersistence,
    # Unified Live FULL research spine (Leisure / CAD; $0 assess only) + PLP via discovery matrix
    [switch]$AuthorizePaidDiscovery,
    [switch]$AllowHistoricalReplay,
    [switch]$EnableLeisure,
    [switch]$NoLeisure,
    [switch]$EnableCad,
    [switch]$NoCad,
    # Findymail -> Airtable persistence (separate from paid research authorization)
    [switch]$PersistFindymailResults,
    [switch]$AuthorizeFindymailPersist,
    [switch]$AuthorizeAirtableProductionWrites,
    [switch]$ApplyFindymailPersist,
    [double]$FindymailCreditHardCap = 0,
    # Stage control
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

  $ErrorActionPreference = 'Stop'
  $resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
  Set-Location $resolvedRepo

  # VALIDATE is read-only and does not need paid authorization.
  $isValidate = ($Stage -eq 'VALIDATE')
  if ($Mode -eq 'Live' -and -not $AuthorizePaidResearch -and -not $isValidate) {
    throw 'Live mode requires -AuthorizePaidResearch.'
  }
  if ($AuthorizePaidResearch -and $Mode -ne 'Live') {
    throw '-AuthorizePaidResearch requires -Mode Live.'
  }
  if ($ActivateStagedPointer -and $NoActivateStagedPointer) {
    throw '-ActivateStagedPointer and -NoActivateStagedPointer are mutually exclusive.'
  }
  if ($Mode -ne 'Live' -and ($PlanResearchPersistence -or $AuthorizeResearchPersistence -or $ApplyResearchPersistence)) {
    throw 'Research persistence flags require -Mode Live (Airtable writes are never unlocked by DryRun or by -AuthorizePaidResearch alone).'
  }
  if ($ApplyResearchPersistence -and (-not $AuthorizeResearchPersistence -or -not $ExactWriteSetAuthorization)) {
    throw '-ApplyResearchPersistence requires -AuthorizeResearchPersistence and -ExactWriteSetAuthorization.'
  }

  if ($EnableLeisure -and $NoLeisure) { throw '-EnableLeisure and -NoLeisure are mutually exclusive.' }
  if ($EnableCad -and $NoCad) { throw '-EnableCad and -NoCad are mutually exclusive.' }
  if ($Mode -ne 'Live' -and ($AuthorizePaidDiscovery -or $PersistFindymailResults -or $ApplyFindymailPersist)) {
    throw '-AuthorizePaidDiscovery / -PersistFindymailResults / -ApplyFindymailPersist require -Mode Live.'
  }
  if ($PersistFindymailResults -and -not $AuthorizeFindymail) {
    throw '-PersistFindymailResults requires -AuthorizeFindymail.'
  }
  if ($ApplyFindymailPersist -and -not $PersistFindymailResults) {
    throw '-ApplyFindymailPersist requires -PersistFindymailResults.'
  }
  if ($PersistFindymailResults) {
    $viaResearchPersistence = ($AuthorizeResearchPersistence -and $ExactWriteSetAuthorization)
    $viaDedicated = ($AuthorizeFindymailPersist -and $AuthorizeAirtableProductionWrites)
    if (-not ($viaResearchPersistence -or $viaDedicated)) {
      throw '-PersistFindymailResults requires (-AuthorizeResearchPersistence + -ExactWriteSetAuthorization) OR (-AuthorizeFindymailPersist + -AuthorizeAirtableProductionWrites). -AuthorizePaidResearch / -AuthorizeFindymail never unlock Airtable writes.'
    }
  }

  $cli = Join-Path $resolvedRepo 'scripts\gdi\run-gdi-full-e2e-v3-stage-cli-v1.mjs'
  if (-not (Test-Path -LiteralPath $cli)) {
    throw "Missing V3 CLI: $cli"
  }

  $nodeArgs = @(
    $cli,
    '--stage', $Stage,
    '--hotel-id', $HotelId,
    '--mode', $Mode
  )
  if ($OutDir) { $nodeArgs += @('--out-dir', $OutDir) }
  if ($OpportunityId) { $nodeArgs += @('--opportunity-id', $OpportunityId) }
  if ($Fixtures) { $nodeArgs += @('--fixtures', $Fixtures) }
  if ($OpportunitiesFile) { $nodeArgs += @('--opportunities-file', $OpportunitiesFile) }
  if ($ContinuationOnly) { $nodeArgs += '--continuation-only' }
  if ($Resume) { $nodeArgs += '--resume' }
  if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-paid-research' }
  if ($AuthorizeFindymail) { $nodeArgs += '--authorize-findymail' }
  if ($FindymailDryRun) { $nodeArgs += '--findymail-dry-run' }
  # Hotel-agnostic: values are forwarded only when supplied; no Wyndham defaults.
  if ($AuthorizationId) { $nodeArgs += @('--authorization-id', $AuthorizationId) }
  if ($HardCeilingUsd -gt 0) { $nodeArgs += @('--hard-ceiling-usd', ([string]$HardCeilingUsd)) }
  if ($Manifest) { $nodeArgs += @('--manifest', $Manifest) }
  if ($PlanResearchPersistence) { $nodeArgs += '--plan-research-persistence' }
  if ($AuthorizeResearchPersistence) { $nodeArgs += '--authorize-research-persistence' }
  if ($ExactWriteSetAuthorization) { $nodeArgs += @('--exact-write-set-authorization', $ExactWriteSetAuthorization) }
  if ($ApplyResearchPersistence) { $nodeArgs += '--apply-research-persistence' }
  if ($AuthorizePaidDiscovery) { $nodeArgs += '--authorize-paid-discovery' }
  if ($AllowHistoricalReplay) { $nodeArgs += '--allow-historical-replay' }
  if ($EnableLeisure) { $nodeArgs += '--enable-leisure' }
  if ($NoLeisure) { $nodeArgs += '--no-leisure' }
  if ($EnableCad) { $nodeArgs += '--enable-cad' }
  if ($NoCad) { $nodeArgs += '--no-cad' }
  if ($PersistFindymailResults) { $nodeArgs += '--persist-findymail-results' }
  if ($AuthorizeFindymailPersist) { $nodeArgs += '--authorize-findymail-persist' }
  if ($AuthorizeAirtableProductionWrites) { $nodeArgs += '--authorize-airtable-production-writes' }
  if ($ApplyFindymailPersist) { $nodeArgs += '--apply-findymail-persist' }
  if ($FindymailCreditHardCap -gt 0) { $nodeArgs += @('--findymail-credit-hard-cap', ([string][int]$FindymailCreditHardCap)) }
  if ($SkipDiscovery) { $nodeArgs += '--skip-discovery' }
  if ($SkipDeep) { $nodeArgs += '--skip-deep' }
  if ($SkipWhoHow) { $nodeArgs += '--skip-who-how' }
  if ($SkipContact) { $nodeArgs += '--skip-contact' }
  if ($SkipQualification) { $nodeArgs += '--skip-qualification' }
  if ($SkipStagedProgression) { $nodeArgs += '--skip-staged-progression' }
  if ($ActivateStagedPointer) { $nodeArgs += '--activate-staged-pointer' }
  if ($NoActivateStagedPointer) { $nodeArgs += '--no-activate-staged-pointer' }

  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "GDI V3 stage $Stage failed with exit code $LASTEXITCODE"
  }
}

function Invoke-GdiWhoHowCheckpointA {
  param(
    [Parameter(Mandatory = $true)][string]$HotelId,
    [ValidateSet('DryRun', 'Live')][string]$Mode = 'DryRun',
    [string]$OpportunityId = '',
    [string]$OutDir = '',
    [string]$AuthorizationId = '',
    [double]$HardCeilingUsd = 0,
    [string]$Manifest = '',
    [switch]$ContinuationOnly,
    [switch]$Resume,
    [switch]$AuthorizePaidResearch,
    [switch]$PreflightOnly,
    [string]$RepoPath = ''
  )

  $ErrorActionPreference = 'Stop'
  $resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
  Set-Location $resolvedRepo

  if ($Mode -eq 'Live' -and -not $AuthorizePaidResearch -and -not $PreflightOnly) {
    throw 'Live Checkpoint A requires -AuthorizePaidResearch.'
  }
  if ($AuthorizePaidResearch -and $Mode -ne 'Live' -and -not $PreflightOnly) {
    throw '-AuthorizePaidResearch requires -Mode Live.'
  }

  $cli = Join-Path $resolvedRepo 'scripts\gdi\run-gdi-who-how-checkpoint-a-v1.mjs'
  if (-not (Test-Path -LiteralPath $cli)) {
    throw "Missing Checkpoint A CLI: $cli"
  }

  # Hotel-agnostic: never default AuthorizationId / Manifest to a Wyndham-specific path.
  if (-not $AuthorizationId) {
    throw 'Checkpoint A requires -AuthorizationId (founder-authorized per hotel/run). No hotel-specific default is permitted.'
  }
  if ($HardCeilingUsd -le 0) {
    throw 'Checkpoint A requires -HardCeilingUsd > 0.'
  }
  if (-not $Manifest) {
    throw 'Checkpoint A requires -Manifest (hotel-specific tranche). No Wyndham-hardcoded default is permitted.'
  }

  $nodeArgs = @(
    $cli,
    '--hotel-id', $HotelId,
    '--mode', $Mode,
    '--authorization-id', $AuthorizationId,
    '--hard-ceiling-usd', ([string]$HardCeilingUsd),
    '--manifest', $Manifest
  )
  if ($OutDir) { $nodeArgs += @('--out-dir', $OutDir) }
  if ($OpportunityId) { $nodeArgs += @('--opportunity-id', $OpportunityId) }
  if ($Resume) { $nodeArgs += '--resume' }
  if ($ContinuationOnly) { $nodeArgs += '--continuation-only' }
  if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-paid-research' }
  if ($PreflightOnly) { $nodeArgs += '--preflight-only' }

  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "GDI WHO/HOW Checkpoint A failed with exit code $LASTEXITCODE"
  }
}
