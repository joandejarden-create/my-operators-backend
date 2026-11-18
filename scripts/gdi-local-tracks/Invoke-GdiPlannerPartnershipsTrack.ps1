<#
.SYNOPSIS
  Canonical wrapper: meeting / wedding planner partnerships track.
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^rec[a-zA-Z0-9]+$')]
  [string]$HotelId,
  [ValidateSet('fixture', 'dry-run', 'preflight', 'live')]
  [string]$Mode = 'fixture',
  [decimal]$BudgetCeilingUsd = 0,
  [int]$MaxCandidates = 8,
  [string]$RepoPath = '',
  [switch]$AuthorizePaidDiscovery,
  [switch]$AuthorizePaidResearch,
  [switch]$AuthorizeExaAgentLive
)

& (Join-Path $PSScriptRoot 'Invoke-GdiLocalDemandTrack.ps1') `
  -Track planner_partnerships `
  -HotelId $HotelId `
  -Mode $Mode `
  -BudgetCeilingUsd $BudgetCeilingUsd `
  -MaxCandidates $MaxCandidates `
  -RepoPath $RepoPath `
  -AuthorizePaidDiscovery:$AuthorizePaidDiscovery `
  -AuthorizePaidResearch:$AuthorizePaidResearch `
  -AuthorizeExaAgentLive:$AuthorizeExaAgentLive
