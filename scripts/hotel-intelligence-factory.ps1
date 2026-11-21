<#
.SYNOPSIS
  Thin PowerShell orchestration for the Hotel Intelligence Factory.

.DESCRIPTION
  Does NOT implement research or persistence logic.
  Invokes: node scripts/hotel-intelligence-factory-run.mjs

.PARAMETER Mode
  Research | CanonicalPromotion

.PARAMETER DryRun
  Default: $true. Promotion apply requires -ApplyPromotion and env gates.
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("Research", "CanonicalPromotion")]
  [string]$Mode,

  [string]$Config = "",
  [string]$RunId = "",
  [string]$Stages = "",
  [string]$Manifest = "",
  [switch]$DryRun,
  [switch]$ApplyResearch,
  [switch]$ApplyPromotion,
  [switch]$Resume,
  [double]$BudgetUsd = 0
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "scripts\hotel-intelligence-factory-run.mjs"))) {
  $Root = Get-Location
}

$nodeArgs = @(
  (Join-Path $Root "scripts\hotel-intelligence-factory-run.mjs"),
  "--mode=$Mode"
)

if ($Config) { $nodeArgs += "--config=$Config" }
if ($RunId) { $nodeArgs += "--run-id=$RunId" }
if ($Stages) { $nodeArgs += "--stages=$Stages" }
if ($Manifest) { $nodeArgs += "--manifest=$Manifest" }
if ($Resume) { $nodeArgs += "--resume" }
if ($ApplyResearch) { $nodeArgs += "--apply-research" }
if ($ApplyPromotion) { $nodeArgs += "--apply-promotion" }
# DryRun is default in Node unless apply flags set; -DryRun is documentation/intent

Write-Host "HIF orchestrator → node $($nodeArgs -join ' ')"
& node @nodeArgs
exit $LASTEXITCODE
