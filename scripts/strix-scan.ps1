#Requires -Version 5.1
<#
.SYNOPSIS
  Run Strix security scans for Dealality (local, GitHub, production).

.EXAMPLE
  .\scripts\strix-scan.ps1 -Profile local -ScanMode quick
  .\scripts\strix-scan.ps1 -Profile production -ScanMode standard -MaxBudget 25
  .\scripts\strix-scan.ps1 -Profile all -Headless -ScanMode deep
#>
param(
  [ValidateSet('local', 'github', 'production', 'all')]
  [string]$Profile = 'local',

  [ValidateSet('quick', 'standard', 'deep')]
  [string]$ScanMode = 'standard',

  [switch]$Headless,

  [double]$MaxBudget = 0,

  [int]$MaxTurns = 0,

  [string]$ConfigPath = '',

  [switch]$CheckOnly
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$StrixDir = Join-Path $RepoRoot 'security\strix'
$TargetList = Join-Path $StrixDir "targets.$Profile.txt"
$Instructions = Join-Path $StrixDir 'instructions.md'
$EnvFile = Join-Path $RepoRoot '.env.strix'

function Write-Step([string]$Message) {
  Write-Host "[strix-scan] $Message" -ForegroundColor Cyan
}

function Test-DockerReady {
  $docker = Get-Command docker -ErrorAction SilentlyContinue
  if (-not $docker) {
    return $false, 'Docker CLI not found. Install Docker Desktop: https://docs.docker.com/desktop/setup/install/windows-install/'
  }
  try {
    & docker info *> $null
    if ($LASTEXITCODE -ne 0) {
      return $false, 'Docker is installed but not running. Start Docker Desktop and wait until it is healthy.'
    }
  } catch {
    return $false, "Docker check failed: $($_.Exception.Message)"
  }
  return $true, 'ok'
}

function Resolve-StrixExe {
  $cmd = Get-Command strix -ErrorAction SilentlyContinue
  $candidates = @(
    $(if ($cmd) { $cmd.Source } else { $null }),
    (Join-Path $env:USERPROFILE '.local\bin\strix.exe'),
    (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python313\Scripts\strix.exe'),
    (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python312\Scripts\strix.exe')
  ) | Where-Object { $_ -and (Test-Path $_) }

  if ($candidates.Count -gt 0) { return $candidates[0] }

  $pyStrix = & python -m pip show strix-agent 2>$null
  if ($LASTEXITCODE -eq 0) {
    $scripts = & python -c "import sys, pathlib; print(pathlib.Path(sys.executable).parent / 'Scripts' / 'strix.exe')" 2>$null
    if ($scripts -and (Test-Path $scripts)) { return $scripts }
  }
  return $null
}

function Import-StrixEnv {
  if (Test-Path $EnvFile) {
    Write-Step "Loading env from .env.strix"
    Get-Content $EnvFile | ForEach-Object {
      $line = $_.Trim()
      if (-not $line -or $line.StartsWith('#')) { return }
      $eq = $line.IndexOf('=')
      if ($eq -lt 1) { return }
      $name = $line.Substring(0, $eq).Trim()
      $value = $line.Substring($eq + 1).Trim().Trim('"').Trim("'")
      if ($name) { Set-Item -Path "Env:$name" -Value $value }
    }
  }
}

Write-Step "Profile=$Profile ScanMode=$ScanMode Repo=$RepoRoot"

if (-not (Test-Path $TargetList)) {
  throw "Target list not found: $TargetList"
}

$dockerOk, $dockerMsg = Test-DockerReady
if (-not $dockerOk) {
  Write-Host "[strix-scan] BLOCKED: $dockerMsg" -ForegroundColor Red
  exit 1
}

$strixExe = Resolve-StrixExe
if (-not $strixExe) {
  Write-Host '[strix-scan] BLOCKED: strix CLI not found. Run: pip install strix-agent' -ForegroundColor Red
  exit 1
}

Import-StrixEnv

if (-not $env:LLM_API_KEY) {
  Write-Host '[strix-scan] BLOCKED: LLM_API_KEY is not set. Copy security/strix/.env.strix.example to .env.strix and add your key.' -ForegroundColor Red
  exit 1
}

if (-not $env:STRIX_LLM) {
  $env:STRIX_LLM = 'openai/gpt-5.4'
}

if ($CheckOnly) {
  $keyOk = [bool]$env:LLM_API_KEY
  Write-Step "strix=$strixExe"
  Write-Step "docker=$dockerMsg"
  Write-Step "LLM=$($env:STRIX_LLM) keyConfigured=$keyOk"
  if (-not $keyOk) {
    Write-Host '[strix-scan] Next: copy security\strix\.env.strix.example to .env.strix and set LLM_API_KEY' -ForegroundColor Yellow
    exit 2
  }
  exit 0
}

$argsList = @(
  '--target-list', $TargetList,
  '--instruction-file', $Instructions,
  '--scan-mode', $ScanMode
)

if ($Headless) { $argsList += '-n' }
if ($MaxBudget -gt 0) { $argsList += @('--max-budget', [string]$MaxBudget) }
if ($MaxTurns -gt 0) { $argsList += @('--max-turns', [string]$MaxTurns) }

if ($ConfigPath) {
  $argsList += @('--config', $ConfigPath)
} elseif (Test-Path (Join-Path $env:USERPROFILE '.strix\cli-config.json')) {
  $argsList += @('--config', (Join-Path $env:USERPROFILE '.strix\cli-config.json'))
}

Write-Step "Running: $strixExe $($argsList -join ' ')"
Push-Location $RepoRoot
try {
  & $strixExe @argsList
  exit $LASTEXITCODE
} finally {
  Pop-Location
}
