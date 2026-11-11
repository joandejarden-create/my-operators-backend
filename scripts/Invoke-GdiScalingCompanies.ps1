<#
.SYNOPSIS
  Single entry point for hotel-generic GDI Scaling Companies Full E2E.

.DESCRIPTION
  Orchestrates the complete Bethesda/Wyndham-standard pipeline via the Node runner:

    hotel profile - intelligent lifecycle classify (CONTINUE/REFRESH/REQUALIFY/
    MONITOR/EXCLUDE/NO_ACTION + NET_NEW plan) - discovery - identity -
    net-new reconcile - mechanism screen - ORS - buyer/contact intel -
    qualification - canonical payload - Airtable publication (authorized only) -
    customer-share verification.

  HOLD continuation, published refresh, and net-new discovery are internal
  lifecycle decisions — do not add separate Mode values for them.

  Defaults (fail-closed):
    - Mode Preview
    - Budget $0 / no paid research
    - No publication / no production writes
    - Surfe / PDL / Parallel / outreach / weekly never authorized

  Paid research and publication are INDEPENDENT authorizations.
  Do not infer publication from research (or the reverse).

  Reference:
    reports/gdi-bethesda-scaling-companies-production-verification-v1/

.PARAMETER HotelId
  Canonical Airtable hotel record ID (required).

.PARAMETER Mode
  Preview  - validate profile/strategies/inventory; may process saved discovery offline; $0; no writes.
  FullE2E  - run every permitted stage through publication/share verification (gates still apply).

.PARAMETER BudgetUSD
  Hard aggregate research ceiling (Node ledger). Required when -AuthorizePaidResearch.

.PARAMETER AuthorizePaidResearch
  Required before any paid provider spend. Independent of publication.

.PARAMETER AuthorizePublication
  Required before customer-facing promotion / canonical publish / Airtable upsert.
  Independent of paid research (may publish from saved offline research).

.PARAMETER SavedDiscoveryPath
  Optional path to prior reconciliation/discovery JSON (e.g. Waterstone 05-reconciliation.json)
  to continue without repeating paid discovery.

.PARAMETER ProbeLiveShare
  After publication, attempt live Railway share list/detail probe when a share token is known.

.EXAMPLE
  # Preview (default) - no paid, no publish
  .\scripts\Invoke-GdiScalingCompanies.ps1 -HotelId recgMYovrrZDJMqzX -Mode Preview

.EXAMPLE
  # Full E2E with saved discovery, unpaid research continuation (publication still gated)
  .\scripts\Invoke-GdiScalingCompanies.ps1 `
    -HotelId recgMYovrrZDJMqzX `
    -Mode FullE2E `
    -SavedDiscoveryPath 'reports/gdi-scaling-company-runs/scaling-20261026-180726-d5cc0d92/05-reconciliation.json'

.EXAMPLE
  # Documented live shape - NOT an authorization to run paid/publish without founder GO
  .\scripts\Invoke-GdiScalingCompanies.ps1 `
    -HotelId 'recgMYovrrZDJMqzX' `
    -Mode FullE2E `
    -BudgetUSD 15 `
    -AuthorizePaidResearch `
    -AuthorizePublication
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [ValidateNotNullOrEmpty()]
  [string]$HotelName = '',

  [ValidateSet('Preview', 'FullE2E', 'DryRun', 'Execute')]
  [string]$Mode = 'Preview',

  [string]$RepoPath = '',

  [string]$RunnerScript = '',

  [ValidateRange(0, 1000)]
  [decimal]$BudgetUSD = 0,

  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizePublication,
  [switch]$AuthorizeProductionWrites,
  [switch]$ApplyProductionWrites,
  [switch]$ProbeLiveShare,

  [string]$SavedDiscoveryPath = '',

  [ValidateRange(0, 50)]
  [int]$MaxCandidates = 8,

  # Legacy alias - maps to Preview when Mode not FullE2E
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-FailClosed([string]$Message) {
  throw $Message
}

# --- Fail-closed authorization invariants (independent gates) ---
if ($BudgetUSD -gt 0 -and -not $AuthorizePaidResearch) {
  Write-FailClosed 'A nonzero budget requires -AuthorizePaidResearch. Nothing was executed.'
}
if ($AuthorizePaidResearch -and $BudgetUSD -le 0) {
  Write-FailClosed '-AuthorizePaidResearch requires -BudgetUSD greater than zero.'
}
# Publication does NOT require paid research (saved-evidence publish is allowed when separately authorized).
# Paid research does NOT imply publication.

# Never accept dangerous providers via this launcher.
$AuthorizeSurfe = $false
$AuthorizePdl = $false
$AuthorizeParallel = $false
$AuthorizeOutreach = $false
$AuthorizeWeeklyScheduling = $false

if (-not $RepoPath) {
  $RepoPath = if (Test-Path (Join-Path $PSScriptRoot '..')) {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
  } else {
    'C:\Dev\dealality\my-operators-backend'
  }
}
$resolvedRepo = (Resolve-Path -LiteralPath $RepoPath -ErrorAction Stop).Path
$repoRoot = [IO.Path]::GetFullPath($resolvedRepo).TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar

if (-not $RunnerScript) {
  $RunnerScript = Join-Path $resolvedRepo 'scripts\gdi-scaling-companies-hotel-runner-v1.mjs'
}
$resolvedRunner = (Resolve-Path -LiteralPath $RunnerScript -ErrorAction Stop).Path
if (-not $resolvedRunner.StartsWith($repoRoot, [StringComparison]::OrdinalIgnoreCase)) {
  Write-FailClosed 'RunnerScript must be inside RepoPath. Nothing was executed.'
}
$ext = [IO.Path]::GetExtension($resolvedRunner).ToLowerInvariant()
if ($ext -ne '.js' -and $ext -ne '.mjs') {
  Write-FailClosed 'RunnerScript must be a JavaScript Node runner (.js or .mjs).'
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  Write-FailClosed 'Node.js is not on PATH.'
}

# Normalize Mode
$modeNorm = $Mode
if ($DryRun.IsPresent -and $Mode -ne 'FullE2E' -and $Mode -ne 'Execute') {
  $modeNorm = 'Preview'
}
switch ($modeNorm) {
  'Preview' { $nodeMode = 'preview' }
  'DryRun'  { $nodeMode = 'preview' }
  'FullE2E' { $nodeMode = 'full-e2e' }
  'Execute' { $nodeMode = 'full-e2e' }
  default   { $nodeMode = 'preview' }
}

$runId = 'scaling-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [Guid]::NewGuid().ToString('N').Substring(0, 8)
$runDir = Join-Path $resolvedRepo (Join-Path 'reports\gdi-scaling-company-runs' $runId)
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
$manifestPath = Join-Path $runDir 'run-config.json'

$prodWrites = [bool]($AuthorizeProductionWrites.IsPresent -or $AuthorizePublication.IsPresent)
# FullE2E + AuthorizePublication implies applyProductionWrites unless explicitly suppressed
$applyWrites = $false
if ($ApplyProductionWrites.IsPresent) {
  $applyWrites = $true
} elseif ($nodeMode -eq 'full-e2e' -and $AuthorizePublication.IsPresent -and $prodWrites) {
  $applyWrites = $true
}

$savedPathAbs = ''
if ($SavedDiscoveryPath) {
  if ([IO.Path]::IsPathRooted($SavedDiscoveryPath)) {
    $savedPathAbs = $SavedDiscoveryPath
  } else {
    $savedPathAbs = Join-Path $resolvedRepo $SavedDiscoveryPath
  }
  if (-not (Test-Path -LiteralPath $savedPathAbs)) {
    Write-FailClosed "SavedDiscoveryPath not found: $savedPathAbs"
  }
}

$config = [ordered]@{
  schemaVersion = 'gdi-scaling-run-request-v1'
  runId = $runId
  hotelId = $HotelId
  hotelName = $HotelName
  researchSegment = 'scaling_companies'
  mode = $nodeMode
  stages = @(
    'hotel_profile',
    'discovery',
    'organization_identity_resolution',
    'net_new_reconciliation',
    'commercial_mechanism_screening',
    'ors_longform_research',
    'buyer_and_contact_intelligence',
    'intelligent_investigation_loop',
    'commercial_qualification',
    'gold_standard_brief_generation',
    'canonical_opportunity_payload',
    'airtable_publication',
    'customer_share_verification'
  )
  sourceHotelsForContractReference = @('recLuxvwwxID7U2B8', 'recnPB3Q02NO8Ma1o')
  netNewReconciliationRequired = $true
  requireNamedContactResearch = $true
  permitHonestFunctionalRouting = $true
  prohibitInventedDemandOrContacts = $true
  budgetUSD = [decimal]$BudgetUSD
  budgetCeilingUsd = [double]$BudgetUSD
  authorizePaidResearch = [bool]$AuthorizePaidResearch.IsPresent
  authorizePublication = [bool]$AuthorizePublication.IsPresent
  authorizeProductionWrites = $prodWrites
  applyProductionWrites = $applyWrites
  probeLiveShare = [bool]$ProbeLiveShare.IsPresent
  savedDiscoveryPath = if ($savedPathAbs) { $savedPathAbs } else { $null }
  authorization = [ordered]@{
    paidResearch = [bool]$AuthorizePaidResearch.IsPresent
    customerPublication = [bool]$AuthorizePublication.IsPresent
    productionWrites = $prodWrites
    surfe = $AuthorizeSurfe
    pdl = $AuthorizePdl
    parallel = $AuthorizeParallel
    prospectOutreach = $AuthorizeOutreach
    weeklyActivation = $AuthorizeWeeklyScheduling
  }
  dryRun = [bool]($nodeMode -eq 'preview')
  maxCandidates = $MaxCandidates
  outputDirectory = $runDir
  reportDir = $runDir
  launcher = 'Invoke-GdiScalingCompanies.ps1'
  launchedAt = (Get-Date).ToUniversalTime().ToString('o')
}

# UTF-8 without BOM - Node JSON.parse rejects Windows PowerShell BOM.
$json = $config | ConvertTo-Json -Depth 12
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($manifestPath, $json, $utf8NoBom)

Write-Host "Config: $manifestPath"
$displayName = if ($HotelName) { $HotelName } else { '(resolved from hotel profile)' }
Write-Host "Hotel: $displayName ($HotelId) | Mode: $nodeMode | Paid ceiling: `$$BudgetUSD | Publication: $([bool]$AuthorizePublication.IsPresent) | ApplyWrites: $applyWrites"
Write-Warning 'Node runner enforces isolated spend ledger, hotel scope, independent publication gates, and share verification. PowerShell alone does not spend or publish.'

Push-Location $resolvedRepo
try {
  & node $resolvedRunner --config $manifestPath
  if ($LASTEXITCODE -ne 0) { throw "GDI runner exited with code $LASTEXITCODE" }
  Write-Host "Runner finished. Review outputs under $runDir."
  Write-Host "Do not declare FULL E2E COMPLETE from exit code alone - check SUMMARY.json fullE2eComplete."

  # Surface universal ADP→GDI readiness blockers before any paid Full E2E.
  $summaryPath = Join-Path $runDir 'SUMMARY.json'
  $gatePath = Join-Path $runDir '07b-full-e2e-publication-gate.json'
  $preflightPath = Join-Path $runDir '07-preflight.json'
  $readinessSource = $null
  if (Test-Path -LiteralPath $gatePath) {
    $readinessSource = Get-Content -LiteralPath $gatePath -Raw | ConvertFrom-Json
  } elseif (Test-Path -LiteralPath $summaryPath) {
    $readinessSource = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
  }
  if ($null -ne $readinessSource) {
    Write-Host ''
    Write-Host '=== Universal ADP/GDI readiness ==='
    $auth = $readinessSource.authorizationStates
    if ($null -ne $auth) {
      Write-Host ("  PROFILE_READY:            {0}" -f $auth.PROFILE_READY)
      Write-Host ("  RESEARCH_AUTHORIZED:      {0}" -f $auth.RESEARCH_AUTHORIZED)
      Write-Host ("  PUBLICATION_AUTHORIZED:   {0}" -f $auth.PUBLICATION_AUTHORIZED)
      Write-Host ("  WEEKLY_ACTIVE:            {0}" -f $auth.WEEKLY_ACTIVE)
    }
    Write-Host ("  In universal ADP/GDI roster: {0}" -f $readinessSource.inUniversalAdpGdiRoster)
    Write-Host ("  In ADP commissioned:         {0}" -f $readinessSource.inAdpCommissioned)
    $missing = @()
    if ($readinessSource.universalPreviewReadiness -and $readinessSource.universalPreviewReadiness.missingPrerequisites) {
      $missing = @($readinessSource.universalPreviewReadiness.missingPrerequisites)
    } elseif ($readinessSource.missingConfiguration) {
      $missing = @($readinessSource.missingConfiguration)
    }
    if ($missing.Count -gt 0) {
      Write-Host '  Missing prerequisites (fix before paid Full E2E):'
      foreach ($m in $missing) {
        if ($m -is [string]) {
          Write-Host ("    - {0}" -f $m)
        } else {
          $code = $m.code
          $action = $m.action
          if ($code) { Write-Host ("    - [{0}] {1}" -f $code, $action) }
          else { Write-Host ("    - {0}" -f ($m | ConvertTo-Json -Compress)) }
        }
      }
    } else {
      Write-Host '  No Preview readiness blockers reported for requested auth flags.'
    }
    if (Test-Path -LiteralPath $preflightPath) {
      $pf = Get-Content -LiteralPath $preflightPath -Raw | ConvertFrom-Json
      $warnProp = $pf.PSObject.Properties['fullE2eNotReadyWarning']
      if ($null -ne $warnProp -and $warnProp.Value) {
        Write-Warning ([string]$warnProp.Value)
      }
    }
  }
}
finally {
  Pop-Location
}
