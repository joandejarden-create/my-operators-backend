# READ-ONLY: find backup scripts
$ErrorActionPreference = 'Continue'

Write-Host '=== Backup-Scripts tree ==='
Get-ChildItem 'C:\Dev\Backup-Scripts' -Recurse -Force -EA SilentlyContinue |
  Select-Object FullName, Length, LastWriteTime, @{N='Kind';E={if($_.PSIsContainer){'DIR'}else{'FILE'}}} |
  Format-Table -AutoSize

Write-Host '=== dealality-snapshots top ==='
if (Test-Path 'C:\Dev\Backup-Scripts\dealality-snapshots') {
  Get-ChildItem 'C:\Dev\Backup-Scripts\dealality-snapshots' -Force -EA SilentlyContinue |
    Format-Table Name, Mode, LastWriteTime -AutoSize
}

Write-Host '=== C:\Dev top-level scripts ==='
Get-ChildItem 'C:\Dev' -File -EA SilentlyContinue | Where-Object { $_.Extension -match '\.(ps1|bat|cmd)$' } |
  Format-Table FullName, Length, LastWriteTime -AutoSize

Write-Host '=== backup-named scripts in deal-capture-proxy (excl node_modules/.git) ==='
Get-ChildItem 'C:\Dev\deal-capture-proxy' -Recurse -File -EA SilentlyContinue |
  Where-Object {
    $_.Name -match '(?i)backup' -and
    $_.Extension -match '\.(ps1|bat|cmd|js|mjs)$' -and
    $_.FullName -notmatch '\\node_modules\\|\\\.git\\|\\tmp\\|\\rail-explore\\node_modules|\\matcha\\node_modules'
  } | Select-Object FullName, Length, LastWriteTime | Format-Table -AutoSize

Write-Host '=== Grep Backup-Scripts for destinations ==='
Get-ChildItem 'C:\Dev\Backup-Scripts' -Recurse -Include *.ps1,*.bat,*.cmd -File -EA SilentlyContinue | ForEach-Object {
  Write-Host ("FILE: {0}" -f $_.FullName)
  Select-String -Path $_.FullName -Pattern 'deal-capture-proxy|Backup-Staging|Nightly-Backups|dealality-backups|OneDrive|My Drive|robocopy|Compress-Archive|workspaceStorage|_NEW_BACKUP|LATEST' -EA SilentlyContinue |
    Select-Object -First 40 | ForEach-Object { Write-Host ("  L{0}: {1}" -f $_.LineNumber, $_.Line.Trim().Substring(0, [Math]::Min(160, $_.Line.Trim().Length))) }
}

Write-Host '=== Search C:\Dev for other ps1 mentioning Nightly-Backups or Backup-Staging write ==='
Get-ChildItem 'C:\Dev' -Directory -EA SilentlyContinue | ForEach-Object {
  $dir = $_.FullName
  if ($dir -match 'node_modules|\.git|deal-capture-proxy\\node_modules') { return }
  Get-ChildItem $dir -File -Include *.ps1,*.bat,*.cmd -EA SilentlyContinue | ForEach-Object {
    $m = Select-String -Path $_.FullName -Pattern 'Nightly-Backups|Backup-Staging|Backup-Dealality' -EA SilentlyContinue
    if ($m) {
      Write-Host ("HIT: {0}" -f $_.FullName)
      $m | Select-Object -First 8 | ForEach-Object { Write-Host ("  L{0}: {1}" -f $_.LineNumber, $_.Line.Trim().Substring(0,[Math]::Min(140,$_.Line.Trim().Length))) }
    }
  }
}
