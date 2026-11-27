#Requires -Version 5.1
<#
.SYNOPSIS
  Repeatable local validation for GDI sortable commercial List View.

.EXAMPLE
  powershell -NoProfile -File scripts/validate-gdi-sortable-commercial-list-view-v1.ps1
#>
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "== GDI sortable commercial list view validation ==" -ForegroundColor Cyan

Write-Host "`n[1/4] Permanent regression test"
npm run test:gdi-sortable-commercial-list-view-v1
if ($LASTEXITCODE -ne 0) { throw "test:gdi-sortable-commercial-list-view-v1 failed" }

Write-Host "`n[2/3] No-login alert shell wiring"
npm run test:gdi-no-login-alert-v1
if ($LASTEXITCODE -ne 0) { throw "no-login-alert failed" }

Write-Host "`n[3/3] Static contract smoke (table module + shells present)"
$required = @(
  "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js",
  "public/group-demand-intelligence.html",
  "public/group-demand-intelligence-share.html",
  "public/css/group-demand-intelligence.css"
)
foreach ($rel in $required) {
  if (-not (Test-Path (Join-Path $Root $rel))) { throw "Missing $rel" }
}
$auth = Get-Content (Join-Path $Root "public/group-demand-intelligence.html") -Raw
if ($auth -notmatch "gdi-commercial-list-table-v1\.js\?v=gdi-sortable-list-v1") {
  throw "Auth shell missing table module cache bust"
}

Write-Host "`nPASS validate-gdi-sortable-commercial-list-view-v1" -ForegroundColor Green
Write-Host "Browser evidence (manual on :8080):"
Write-Host "  1. Open http://localhost:8080/group-demand-intelligence.html"
Write-Host "  2. Switch List View — confirm commercial table + sticky headers"
Write-Host "  3. Sort columns; confirm Sort By dropdown sync"
Write-Host "  4. Toggle optional Columns; confirm no fabricated ADR/revenue"
Write-Host "  5. Switch Tiles — confirm unchanged cards + same opportunity IDs"
Write-Host "  6. Open drawer via name / eye icon"
Write-Host "  7. Change hotel + filters; confirm List/Tiles ID parity"
