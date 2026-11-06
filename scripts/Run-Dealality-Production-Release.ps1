<#
.SYNOPSIS
  Shared Dealality production release orchestrator (ADP / AVA / GDI).

.DESCRIPTION
  PowerShell entrypoint that calls the Node coordinator. Railway runtime does
  not depend on PowerShell — this script is operator-side only.

  Does NOT publish customer reports, rotate share tokens, or run paid research.

.EXAMPLE
  .\scripts\Run-Dealality-Production-Release.ps1 -Product AVA -CommitSha "<40hex>" -Authorize -DryRun

.EXAMPLE
  .\scripts\Run-Dealality-Production-Release.ps1 -Product AVA -CommitSha "<40hex>" -Authorize
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("ADP", "AVA", "GDI")]
  [string]$Product,

  [Parameter(Mandatory = $true)]
  [ValidatePattern("^[0-9a-fA-F]{40}$")]
  [string]$CommitSha,

  [switch]$Authorize,
  [switch]$DryRun,
  [switch]$Status,
  [switch]$ReleaseLock,
  [string]$Owner = $env:DEALALITY_RELEASE_OWNER,
  [string]$LockRoot = $env:DEALALITY_RELEASE_LOCK_ROOT,
  [string]$Note
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $Root

if ($LockRoot) {
  $env:DEALALITY_RELEASE_LOCK_ROOT = $LockRoot
  # Explicit -LockRoot is the operator assertion that the path is durable+shared
  # (e.g. Railway volume /app/data/adp-durable-published/dealality-release-locks).
  if (-not $env:DEALALITY_RELEASE_LOCK_SHARED) {
    $env:DEALALITY_RELEASE_LOCK_SHARED = "1"
  }
}
if ($Owner) {
  $env:DEALALITY_RELEASE_OWNER = $Owner
}
if (-not $env:DEALALITY_RELEASE_LOCK_ROOT -and -not $DryRun -and -not $Status -and -not $ReleaseLock) {
  Write-Error "DEALALITY_RELEASE_LOCK_ROOT / -LockRoot is required for live production releases (fail closed; local default is not cross-machine safe)."
}

$nodeArgs = @(
  "scripts/dealality-production-release-coordinator-v1.mjs"
)

if ($Status) {
  $nodeArgs += "--status"
  & node @nodeArgs
  exit $LASTEXITCODE
}

if ($ReleaseLock) {
  $nodeArgs += @("--release-lock")
  if ($Owner) { $nodeArgs += @("--owner", $Owner) }
  & node @nodeArgs
  exit $LASTEXITCODE
}

$nodeArgs += @("--product", $Product, "--sha", $CommitSha.ToLowerInvariant())
if ($Authorize) { $nodeArgs += "--authorize-production-release" }
if ($DryRun) { $nodeArgs += "--dry-run"; $nodeArgs += "--release-after-dry-run" }
if ($Owner) { $nodeArgs += @("--owner", $Owner) }
if ($Note) { $nodeArgs += @("--note", $Note) }

Write-Host "Dealality production release coordinator"
Write-Host "  Product : $Product"
Write-Host "  SHA     : $($CommitSha.ToLowerInvariant())"
Write-Host "  DryRun  : $DryRun"
Write-Host "  LockRoot: $($env:DEALALITY_RELEASE_LOCK_ROOT)"

& node @nodeArgs
exit $LASTEXITCODE
