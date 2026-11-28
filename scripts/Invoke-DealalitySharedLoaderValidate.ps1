<#
.SYNOPSIS
  Shared Dealality loading-component validation (GDI / ADP / Brand Explorer / Hotel Explorer).

.DESCRIPTION
  One shared Node validator with product-specific entry points.
  Does not duplicate the test implementation per product.

.PARAMETER Product
  gdi | adp | be | he | all (default)

.PARAMETER Browser
  When set, forwards --browser (browser gates otherwise NOT_TESTED).

.EXAMPLE
  pwsh -File scripts/Invoke-DealalitySharedLoaderValidate.ps1
  pwsh -File scripts/Invoke-DealalitySharedLoaderValidate.ps1 -Product he
  pwsh -File scripts/Invoke-DealalitySharedLoaderValidate.ps1 -Product gdi -Browser
#>
[CmdletBinding()]
param(
  [ValidateSet("gdi", "adp", "be", "he", "all")]
  [string]$Product = "all",
  [switch]$Browser
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "package.json"))) {
  $Root = $PSScriptRoot
  while ($Root -and -not (Test-Path (Join-Path $Root "package.json"))) {
    $Root = Split-Path -Parent $Root
  }
}
Set-Location $Root

$argsList = @("--product", $Product)
if ($Browser) { $argsList += "--browser" }

Write-Host "[Dealality Shared Loader] product=$Product browser=$Browser"
& npm run test:dealality-shared-loader-contract-v1 -- @argsList
if ($LASTEXITCODE -ne 0) {
  throw "Dealality shared loader contract FAILED for product=$Product"
}

# Product-specific supporting gates (reuse existing, do not reimplement wave math).
switch ($Product) {
  "he" {
    & npm run test:he-loading-ux-toast-v1
    if ($LASTEXITCODE -ne 0) { throw "HE loading toast gate failed" }
    & npm run test:he-contact-wave-loader-parity-v1
    if ($LASTEXITCODE -ne 0) { throw "HE contact wave parity gate failed" }
  }
  "gdi" {
    if (Get-Command npm -ErrorAction SilentlyContinue) {
      & npm run test:gdi-es-wave-loading-v1
      if ($LASTEXITCODE -ne 0) { throw "GDI ES wave loading gate failed" }
    }
  }
  "all" {
    & npm run test:he-loading-ux-toast-v1
    if ($LASTEXITCODE -ne 0) { throw "HE loading toast gate failed" }
    & npm run test:he-contact-wave-loader-parity-v1
    if ($LASTEXITCODE -ne 0) { throw "HE contact wave parity gate failed" }
    & npm run test:gdi-es-wave-loading-v1
    if ($LASTEXITCODE -ne 0) { throw "GDI ES wave loading gate failed" }
  }
}

Write-Host "[Dealality Shared Loader] PASS product=$Product"
exit 0
