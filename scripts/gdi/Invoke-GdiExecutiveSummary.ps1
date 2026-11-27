<#
.SYNOPSIS
  Canonical PowerShell runner for GDI Executive Summary (read-only).

.DESCRIPTION
  Generates or validates the hotel Executive Summary + Research Receipt.
  Never triggers Exa, Surfe, Findymail, or other paid research.

.PARAMETER HotelId
  Canonical HPC hotel record id (rec…).

.PARAMETER Mode
  validate (default) | generate | readiness-audit

.PARAMETER Preview
  Optional founder preview: staged (requires founder/admin; not for ordinary hotel users)

.EXAMPLE
  .\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recLuxvwwxID7U2B8 -Mode validate

.EXAMPLE
  .\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode generate -Preview staged

.EXAMPLE
  .\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -Mode readiness-audit
#>
[CmdletBinding()]
param(
  [ValidatePattern('^rec[a-zA-Z0-9]+$|^$')]
  [string]$HotelId = '',

  [ValidateSet('validate', 'generate', 'readiness-audit')]
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

if ($Mode -eq 'readiness-audit') {
  $audit = Join-Path $resolvedRepo 'scripts\gdi\audit-gdi-executive-summary-hotel-readiness-v1.mjs'
  if (-not (Test-Path -LiteralPath $audit)) {
    throw "Missing readiness audit CLI: $audit"
  }
  $nodeArgs = @($audit)
  if ($HotelId) { $nodeArgs += @('--hotel-id', $HotelId) }
  if ($OutDir) { $nodeArgs += @('--out', $OutDir) }
  Write-Host "GDI Executive Summary readiness-audit (read-only)" -ForegroundColor Cyan
  Push-Location $resolvedRepo
  try {
    & node @nodeArgs
    if ($LASTEXITCODE -ne 0) { throw "Readiness audit exited with code $LASTEXITCODE" }
  }
  finally { Pop-Location }
  return
}

if (-not $HotelId) {
  throw '-HotelId is required for validate/generate modes.'
}

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
