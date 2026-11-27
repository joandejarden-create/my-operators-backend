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
    [switch]$ContinuationOnly,
    [switch]$Resume,
    [switch]$AuthorizePaidResearch,
    [string]$RepoPath = ''
  )

  $ErrorActionPreference = 'Stop'
  $resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
  Set-Location $resolvedRepo

  if ($Mode -eq 'Live' -and -not $AuthorizePaidResearch) {
    throw 'Live mode requires -AuthorizePaidResearch.'
  }
  if ($AuthorizePaidResearch -and $Mode -ne 'Live') {
    throw '-AuthorizePaidResearch requires -Mode Live.'
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
  if ($ContinuationOnly) { $nodeArgs += '--continuation-only' }
  if ($Resume) { $nodeArgs += '--resume' }
  if ($AuthorizePaidResearch) { $nodeArgs += '--authorize-paid-research' }

  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "GDI V3 stage $Stage failed with exit code $LASTEXITCODE"
  }
}
