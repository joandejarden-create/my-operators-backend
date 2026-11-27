<#
.SYNOPSIS
  Canonical PowerShell runner for GDI Executive Summary (read-only).

.DESCRIPTION
  Generates or validates the hotel Executive Summary + Research Receipt.
  Never triggers Exa, Surfe, Findymail, or other paid research.

.PARAMETER HotelId
  Canonical HPC hotel record id (rec…).

.PARAMETER Mode
  validate (default) | generate

.PARAMETER Preview
  Optional founder preview: staged

.EXAMPLE
  .\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recLuxvwwxID7U2B8 -Mode validate

.EXAMPLE
  .\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode generate -Preview staged
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,

  [ValidateSet('validate', 'generate')]
  [string]$Mode = 'validate',

  [ValidateSet('', 'staged', 'founder')]
  [string]$Preview = '',

  [string]$OutDir = '',

  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $RepoPath) {
  $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}
$resolvedRepo = (Resolve-Path -LiteralPath $RepoPath -ErrorAction Stop).Path
$cli = Join-Path $resolvedRepo 'scripts\gdi\run-gdi-executive-summary-v1.mjs'
if (-not (Test-Path -LiteralPath $cli)) {
  throw "Missing Executive Summary CLI: $cli"
}

$nodeArgs = @(
  $cli,
  '--hotel-id', $HotelId,
  '--mode', $Mode
)
if ($Preview) { $nodeArgs += @('--preview', $Preview) }
if ($OutDir) { $nodeArgs += @('--out', $OutDir) }

Write-Host "GDI Executive Summary hotel=$HotelId mode=$Mode preview=$Preview (read-only; no paid research)" -ForegroundColor Cyan

Push-Location $resolvedRepo
try {
  & node @nodeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "Executive Summary runner exited with code $LASTEXITCODE"
  }
}
finally {
  Pop-Location
}
