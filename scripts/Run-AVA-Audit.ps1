<#
.SYNOPSIS
  Dealality permanent AVA Audit runner (Windows PowerShell).

.DESCRIPTION
  Orchestrates the canonical Node.js AVA Full E2E + ADP↔AVA certification pipeline.
  Does not implement a second methodology, PDF generator, or research process.

.PARAMETER HotelId
  Single hotel: Airtable rec…, adp_* propertyId, or payload slug.

.PARAMETER HotelIds
  Multiple hotels.

.PARAMETER AllHotels
  Run the entire authorized (certified ADP) portfolio.

.PARAMETER DryRun
  Preview without writes / archive / Airtable / Admin registration.

.PARAMETER Reproduce
  Rebuild from a frozen historical audit run.

.PARAMETER AuditRunId
  Required with -Reproduce.

.PARAMETER CertifyOnly
  Certification + archive path without paid research re-collection.

.EXAMPLE
  .\scripts\Run-AVA-Audit.ps1 -HotelId "adp_spice_island_beach_resort" -DryRun

.EXAMPLE
  .\scripts\Run-AVA-Audit.ps1 -HotelIds "adp_a","adp_b" -CertifyOnly

.EXAMPLE
  .\scripts\Run-AVA-Audit.ps1 -AllHotels -DryRun

.EXAMPLE
  .\scripts\Run-AVA-Audit.ps1 -AuditRunId "AVA-RUN-..." -Reproduce
#>
[CmdletBinding(DefaultParameterSetName = "Single")]
param(
  [Parameter(ParameterSetName = "Single", Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$HotelId,

  [Parameter(ParameterSetName = "Multi", Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string[]]$HotelIds,

  [Parameter(ParameterSetName = "All", Mandatory = $true)]
  [switch]$AllHotels,

  [Parameter(ParameterSetName = "Reproduce", Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$AuditRunId,

  [Parameter(ParameterSetName = "Reproduce")]
  [switch]$Reproduce,

  [Parameter(ParameterSetName = "Single")]
  [Parameter(ParameterSetName = "Multi")]
  [Parameter(ParameterSetName = "All")]
  [switch]$DryRun,

  [Parameter(ParameterSetName = "Single")]
  [Parameter(ParameterSetName = "Multi")]
  [Parameter(ParameterSetName = "All")]
  [switch]$CertifyOnly,

  [string]$NodeExe = "node",
  [string]$RepoRoot = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Write-AvaLog {
  param([string]$Message, [string]$Level = "INFO")
  $ts = (Get-Date).ToUniversalTime().ToString("o")
  $line = @{ ts = $ts; level = $Level; msg = $Message } | ConvertTo-Json -Compress
  Write-Host $line
}

try {
  if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
  }
  if (-not (Test-Path (Join-Path $RepoRoot "package.json"))) {
    throw "REPO_ROOT_INVALID: expected package.json under $RepoRoot"
  }

  Push-Location $RepoRoot

  $nodeCmd = Get-Command $NodeExe -ErrorAction SilentlyContinue
  if (-not $nodeCmd) {
    throw "NODE_NOT_FOUND: install the repository-supported Node.js and ensure it is on PATH"
  }

  $nodeVersion = & $NodeExe -v
  Write-AvaLog "node=$nodeVersion repo=$RepoRoot"

  if ($Reproduce -and -not $AuditRunId) {
    throw "AUDIT_RUN_ID_REQUIRED for -Reproduce"
  }
  if ($PSCmdlet.ParameterSetName -eq "Reproduce") {
    $Reproduce = $true
  }

  if ($DryRun -and $CertifyOnly) {
    throw "CONFLICTING_FLAGS: -DryRun and -CertifyOnly are mutually exclusive"
  }

  $cli = Join-Path $RepoRoot "scripts\run-ava-permanent-audit-v1.mjs"
  if (-not (Test-Path $cli)) {
    throw "CLI_MISSING: $cli"
  }

  $argList = @($cli)

  if ($Reproduce) {
    $argList += @("--reproduce", "--audit-run-id", $AuditRunId)
  }
  elseif ($AllHotels) {
    $argList += "--all"
  }
  elseif ($PSCmdlet.ParameterSetName -eq "Multi") {
    $joined = ($HotelIds | ForEach-Object { $_.Trim() }) -join ","
    $argList += @("--hotels", $joined)
  }
  else {
    $argList += @("--hotel", $HotelId)
  }

  if ($DryRun) { $argList += "--dry-run" }
  if ($CertifyOnly) { $argList += "--certify-only" }

  Write-AvaLog ("exec " + ($argList -join " "))

  & $NodeExe @argList
  $code = $LASTEXITCODE
  if ($null -eq $code) { $code = 0 }

  if ($code -eq 0) {
    Write-AvaLog "AVA_AUDIT_RUNNER_OK exit=$code"
  }
  else {
    Write-AvaLog "AVA_AUDIT_RUNNER_FAILED exit=$code" "ERROR"
  }
  exit $code
}
catch {
  Write-AvaLog $_.Exception.Message "ERROR"
  exit 1
}
finally {
  Pop-Location -ErrorAction SilentlyContinue
}
