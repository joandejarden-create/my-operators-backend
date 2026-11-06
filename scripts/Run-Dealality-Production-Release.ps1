<#
.SYNOPSIS
  Operator helper — points to the sole GitHub Actions production release entry.

.DESCRIPTION
  Live ADP/AVA/GDI production deploys must run via GitHub Actions workflow
  dealality-production-release (cross-machine concurrency + exact-SHA checkout).

  This script does NOT call railway up. It either:
    - prints the exact gh workflow dispatch command, or
    - runs a local coordinator dry-run when -DryRun is set.

.EXAMPLE
  .\scripts\Run-Dealality-Production-Release.ps1 -Product AVA -CommitSha "<40hex>" -Authorize -DryRun

.EXAMPLE
  .\scripts\Run-Dealality-Production-Release.ps1 -Product AVA -CommitSha "<40hex>" -Authorize -PrintDispatch
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
  [switch]$PrintDispatch,
  [switch]$Status,
  [switch]$ReleaseLock,
  [string]$Owner = $env:DEALALITY_RELEASE_OWNER,
  [string]$LockRoot = $env:DEALALITY_RELEASE_LOCK_ROOT,
  [string]$Note
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $Root

$Sha = $CommitSha.ToLowerInvariant()

if ($PrintDispatch -or (-not $DryRun -and -not $Status -and -not $ReleaseLock)) {
  if (-not $Authorize) {
    Write-Error "Live production release requires -Authorize after founder authorization."
  }
  $dry = if ($DryRun) { "true" } else { "false" }
  Write-Host "Sole live production entry is GitHub Actions (not local railway up)."
  Write-Host ""
  Write-Host "gh workflow run dealality-production-release.yml ``"
  Write-Host "  -f product=$Product ``"
  Write-Host "  -f approved_sha=$Sha ``"
  Write-Host "  -f authorization_phrase=I_AUTHORIZE_PRODUCTION_RELEASE ``"
  Write-Host "  -f dry_run=$dry"
  Write-Host ""
  if (-not $DryRun) {
    Write-Host "Refusing local live deploy. Dispatch the workflow above after founder authorization."
    exit 2
  }
}

if ($LockRoot) {
  $env:DEALALITY_RELEASE_LOCK_ROOT = $LockRoot
  if (-not $env:DEALALITY_RELEASE_LOCK_SHARED) {
    $env:DEALALITY_RELEASE_LOCK_SHARED = "1"
  }
}
if ($Owner) {
  $env:DEALALITY_RELEASE_OWNER = $Owner
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

if (-not $Authorize) {
  Write-Error "Pass -Authorize after founder authorization"
}

# Local path is dry-run only.
$nodeArgs += @("--product", $Product, "--sha", $Sha, "--authorize-production-release", "--dry-run", "--release-after-dry-run", "--channel", "github-actions")
if ($Owner) { $nodeArgs += @("--owner", $Owner) }
if ($Note) { $nodeArgs += @("--note", $Note) }

Write-Host "Dealality production release coordinator (LOCAL DRY-RUN ONLY)"
Write-Host "  Product : $Product"
Write-Host "  SHA     : $Sha"
Write-Host "  Channel : github-actions (simulated for dry-run gates)"
Write-Host "  LockRoot: $($env:DEALALITY_RELEASE_LOCK_ROOT)"

& node @nodeArgs
exit $LASTEXITCODE
