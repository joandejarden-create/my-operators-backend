<#
.SYNOPSIS
  Explicit founder-approved promotion of a reviewed STAGED package to CURRENT.

.DESCRIPTION
  Single founder approval gate:
    FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT

  Default Mode=DryRun (non-mutating). Live requires the exact auth phrase and
  the exact reviewed publicationVersion. Never auto-called from Full E2E.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiPromoteCurrent.ps1 -HotelId recnPB3Q02NO8Ma1o `
    -PublicationVersion gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6f4aef5e -Mode DryRun
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [Parameter(Mandatory = $true)]
  [string]$PublicationVersion,

  [ValidateSet('DryRun', 'Live')]
  [string]$Mode = 'DryRun',

  [string]$Auth = '',
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

$resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
Set-Location $resolvedRepo

$runner = Join-Path $resolvedRepo 'scripts\gdi\run-gdi-promote-current-v1.mjs'
if (-not (Test-Path -LiteralPath $runner)) {
  throw "Missing runner: $runner"
}

Write-Host "GDI CURRENT promotion Mode=$Mode HotelId=$HotelId Version=$PublicationVersion"
Write-Host "Lifecycle: FOUNDER APPROVAL only (STAGED generation never publishes CURRENT)"

$argsList = @(
  $runner,
  '--hotel-id', $HotelId,
  '--version', $PublicationVersion
)
if ($Mode -eq 'Live') {
  $argsList += '--apply'
  $argsList += '--auth'
  $argsList += $Auth
}

& node @argsList
if ($LASTEXITCODE -ne 0) {
  throw "CURRENT promotion failed exit=$LASTEXITCODE"
}

Write-Host "GDI CURRENT promotion step PASS ($Mode)"
