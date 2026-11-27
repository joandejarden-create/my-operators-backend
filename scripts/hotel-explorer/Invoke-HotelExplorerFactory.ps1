<#
.SYNOPSIS
  Hotel Explorer Intelligence Factory V1.1 — PowerShell orchestrator.

.DESCRIPTION
  Thin wrapper around the Node factory runner. Does not implement research logic.
  Supports Audit | Research | Apply | Validate | Full | Refresh.

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -HotelId recLuxvwwxID7U2B8 -Mode Audit

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -HotelName "Bethesda Marriott" -Mode Full

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -BatchFile configs/hotel-explorer/he50-eight-hotel-batch-v1.json -Mode Audit

.EXAMPLE
  .\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -HotelId recLuxvwwxID7U2B8 -Mode Apply -Apply
#>
[CmdletBinding()]
param(
  [string]$HotelId,
  [string]$HotelName,
  [ValidateSet("Audit", "Research", "Apply", "Validate", "Full", "Refresh")]
  [string]$Mode = "Audit",
  [switch]$Apply,
  [double]$MaxSpendUSD = 0,
  [string]$OutputDir,
  [switch]$Resume,
  [string]$BatchFile,
  [switch]$SkipBrowser
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

$NodeRunner = Join-Path $RepoRoot "scripts\hotel-explorer\run-hotel-explorer-factory.mjs"
if (-not (Test-Path $NodeRunner)) {
  Write-Error "Node factory runner missing: $NodeRunner"
  exit 2
}

$argsList = @($NodeRunner, "--mode", $Mode)

if ($HotelId) { $argsList += @("--hotel-id", $HotelId) }
if ($HotelName) { $argsList += @("--hotel-name", $HotelName) }
if ($Apply) { $argsList += "--apply" }
$argsList += @("--max-spend-usd", "$MaxSpendUSD")
if ($OutputDir) { $argsList += @("--output-dir", $OutputDir) }
if ($Resume) { $argsList += "--resume" }
if ($BatchFile) { $argsList += @("--batch-file", $BatchFile) }
# Browser off by default; pass -SkipBrowser:$false and omit skip to enable later
if ($SkipBrowser -or -not $PSBoundParameters.ContainsKey("SkipBrowser")) {
  # Node defaults skipBrowser=true unless --browser
} else {
  $argsList += "--browser"
}

Write-Host "HE Factory V1.1 → Mode=$Mode HotelId=$HotelId HotelName=$HotelName Apply=$Apply"
& node @argsList
exit $LASTEXITCODE
