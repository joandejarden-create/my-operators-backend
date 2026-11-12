<#
.SYNOPSIS
  GDI Next-Gen Clinical Trials Medical specialist hunter.
.DESCRIPTION
  Independently executable wrapper. Feeds existing GDI qualification — not a new demand segment.
  Default: dry-run, cache-first, $0.
.EXAMPLE
  .\scripts\gdi-nextgen-hunters\Invoke-GdiClinicalTrialsMedicalHunter.ps1 -HotelId recLuxvwwxID7U2B8 -DryRun
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,
  [string]$HotelName = '',
  [ValidateRange(0, 1000)]
  [decimal]$BudgetUSD = 0,
  [switch]$DryRun,
  [switch]$Execute,
  [switch]$AuthorizePaidResearch,
  [switch]$AllowPaid,
  [string]$RepoPath = '',
  [string]$ReportDir = ''
)
$shared = Join-Path $PSScriptRoot 'Invoke-GdiSpecialistHunter.ps1'
$params = @{
  Hunter = 'clinical_trials_medical'
  HotelId = $HotelId
  HotelName = $HotelName
  BudgetUSD = $BudgetUSD
  RepoPath = $RepoPath
  ReportDir = $ReportDir
}
if ($DryRun) { $params.DryRun = $true }
if ($Execute) { $params.Execute = $true }
if ($AuthorizePaidResearch) { $params.AuthorizePaidResearch = $true }
if ($AllowPaid) { $params.AllowPaid = $true }
& $shared @params
exit $LASTEXITCODE
