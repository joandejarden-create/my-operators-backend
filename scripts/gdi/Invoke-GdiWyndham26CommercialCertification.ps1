<#
.SYNOPSIS
  Validate Wyndham 26/26 commercial certification + optional :8080 probe.

.EXAMPLE
  .\scripts\gdi\Invoke-GdiWyndham26CommercialCertification.ps1 -RequirePort8080
#>
[CmdletBinding()]
param(
  [switch]$RequirePort8080,
  [string]$RepoPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_GdiV3Common.ps1')

$resolvedRepo = Get-GdiRepoRoot -RepoPath $RepoPath
Set-Location $resolvedRepo

Write-Host '== Wyndham 26 commercial certification =='
& node (Join-Path $resolvedRepo 'scripts\test-gdi-wyndham-26-commercial-certification-v1.mjs')
if ($LASTEXITCODE -ne 0) { throw 'test-gdi-wyndham-26-commercial-certification-v1 failed' }
& node (Join-Path $resolvedRepo 'scripts\test-gdi-content-hydration-presentation-gate-v1.mjs')
if ($LASTEXITCODE -ne 0) { throw 'content hydration gate failed' }

$listen = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $listen) {
  if ($RequirePort8080) { throw 'Port 8080 is not listening' }
  Write-Host '[warn] Port 8080 not listening — skipping live probe'
} else {
  $url = 'http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities?preview=staged'
  Write-Host "Probing $url"
  try {
    $resp = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 60
    $json = $resp.Content | ConvertFrom-Json
    $rows = @($json.opportunities)
    if ($rows.Count -lt 26) {
      $rows = @($json.items)
    }
    if ($rows.Count -lt 26 -and $json.data) { $rows = @($json.data) }
    Write-Host "[8080] opportunities payload count=$($rows.Count) ok=$($json.ok)"
  } catch {
    if ($RequirePort8080) { throw }
    Write-Host "[warn] live opportunities probe failed: $_"
  }
}

Write-Host 'Wyndham 26 commercial certification PASS'
