<#
.SYNOPSIS
  GDI Leisure Demand Intelligence — distribution module.
.DESCRIPTION
  Independently executable wrapper. Feeds canonical LDI/Node Full E2E contracts.
  Default: fixture, $0, no publication, no Findymail/Surfe.
.EXAMPLE
  .\scripts\gdi-leisure\Invoke-GdiLeisureDistribution.ps1 -HotelId recaokE8psIbuVeuH
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,
  [string]$HotelProfileJson = '',
  [ValidateSet('fixture', 'replay', 'preview')]
  [string]$ResearchMode = 'fixture',
  [ValidateRange(0, 5000)]
  [decimal]$BudgetUSD = 0,
  [string]$EvidenceSources = 'fixture,historical_ledger',
  [string]$ExistingInventoryPath = '',
  [string]$NeedPeriodContextJson = '',
  [string]$OutDir = '',
  [switch]$Resume,
  [switch]$Replay,
  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizePublication,
  [switch]$AllowLivePilot,
  [string]$RepoPath = ''
)
$shared = Join-Path $PSScriptRoot 'Invoke-GdiLeisureModule.ps1'
$params = @{
  Module = 'leisure_distribution'
  HotelId = $HotelId
  HotelProfileJson = $HotelProfileJson
  ResearchMode = $ResearchMode
  BudgetUSD = $BudgetUSD
  EvidenceSources = $EvidenceSources
  ExistingInventoryPath = $ExistingInventoryPath
  NeedPeriodContextJson = $NeedPeriodContextJson
  OutDir = $OutDir
  RepoPath = $RepoPath
}
if ($Resume) { $params.Resume = $true }
if ($Replay) { $params.Replay = $true }
if ($AuthorizePaidResearch) { $params.AuthorizePaidResearch = $true }
if ($AuthorizePublication) { $params.AuthorizePublication = $true }
if ($AllowLivePilot) { $params.AllowLivePilot = $true }
& $shared @params
exit $LASTEXITCODE
