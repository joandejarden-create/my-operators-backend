<#
.SYNOPSIS
  Shared PowerShell entry for GDI specialist hunters (next-gen discovery).

.DESCRIPTION
  Wraps node scripts/gdi-nextgen-hunter-run.mjs.
  Defaults: dry-run, cache-first, $0 budget. Paid flags are accepted for
  parity with other GDI scripts but are rejected by the Node safety gate
  until a separate founder authorization wave.

.PARAMETER Hunter
  Specialist hunter id (e.g. government_procurement).

.PARAMETER HotelId
  Canonical Airtable hotel record id (rec…).
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$Hunter,

  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [string]$HotelName = '',

  [ValidateRange(0, 1000)]
  [decimal]$BudgetUSD = 0,

  [switch]$DryRun,
  [switch]$Execute,
  [switch]$AuthorizePaidResearch,
  [switch]$AllowPaid,

  [string]$RepoPath = '',
  [string]$ReportDir = ''
)

$ErrorActionPreference = 'Stop'

if (-not $RepoPath) {
  $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}

$runner = Join-Path $RepoPath 'scripts\gdi-nextgen-hunter-run.mjs'
if (-not (Test-Path $runner)) {
  throw "Runner not found: $runner"
}

$nodeArgs = @(
  $runner,
  '--hunter', $Hunter,
  '--hotel-id', $HotelId,
  '--budget-usd', ([string][int]$BudgetUSD)
)

if ($HotelName) { $nodeArgs += @('--hotel-name', $HotelName) }
if ($ReportDir) { $nodeArgs += @('--report-dir', $ReportDir) }

# Default dry-run unless -Execute explicitly set
if ($Execute -and -not $DryRun) {
  $nodeArgs += '--execute'
} else {
  $nodeArgs += '--dry-run'
}

if ($AuthorizePaidResearch -or $AllowPaid) {
  Write-Warning 'Paid research flags are not authorized by the nextgen discovery wave; Node gate will fail closed.'
  if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-measurement' }
  if ($AllowPaid) { $nodeArgs += '--allow-paid' }
}

Write-Host "GDI specialist hunter: $Hunter / $HotelId (zero-paid default)"
& node @nodeArgs
exit $LASTEXITCODE
