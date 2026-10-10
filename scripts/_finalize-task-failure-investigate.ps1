# Investigate scheduled task last failure -1073741510 / 3221225786
$ErrorActionPreference = 'Continue'
Write-Host '=== TASK CONFIG ==='
$t = Get-ScheduledTask -TaskName 'Dealality Nightly Backup' -EA SilentlyContinue
$i = Get-ScheduledTaskInfo -TaskName 'Dealality Nightly Backup' -EA SilentlyContinue
Write-Host ("Name={0} State={1} Enabled={2}" -f $t.TaskName, $t.State, $t.Settings.Enabled)
Write-Host ("LastRun={0} LastResult={1} NextRun={2}" -f $i.LastRunTime, $i.LastTaskResult, $i.NextRunTime)
Write-Host ("Execute={0}" -f $t.Actions.Execute)
Write-Host ("Arguments={0}" -f $t.Actions.Arguments)
Write-Host ("WorkingDirectory={0}" -f $t.Actions.WorkingDirectory)
Write-Host ("StopIfRunsLonger={0}" -f $t.Settings.ExecutionTimeLimit)
Write-Host ("AllowStartIfOnBatteries={0}" -f $t.Settings.AllowStartIfOnBatteries)
Write-Host ("WakeToRun={0}" -f $t.Settings.WakeToRun)
Write-Host ("RunOnlyIfNetworkAvailable={0}" -f $t.Settings.RunOnlyIfNetworkAvailable)
Write-Host ("MultipleInstances={0}" -f $t.Settings.MultipleInstances)
Write-Host ("RestartCount={0}" -f $t.Settings.RestartCount)
Write-Host ("UserId={0}" -f $t.Principal.UserId)
Write-Host ("LogonType={0}" -f $t.Principal.LogonType)
Write-Host ("RunLevel={0}" -f $t.Principal.RunLevel)

Write-Host ''
Write-Host '=== Decode LastResult ==='
$code = [uint32]$i.LastTaskResult
Write-Host ("Unsigned=0x{0:X8} Signed={1}" -f $code, $i.LastTaskResult)
if ($i.LastTaskResult -eq -1073741510 -or $code -eq 0xC000013A) {
  Write-Host 'CODE MATCH: STATUS_CONTROL_C_EXIT (0xC000013A) — process terminated by Ctrl+C / console close / task kill / interactive session end'
}

Write-Host ''
Write-Host '=== Dealality-related tasks count ==='
@(Get-ScheduledTask | Where-Object { $_.TaskName -match 'Dealality' }) | ForEach-Object {
  Write-Host ("TASK: {0} Enabled={1} State={2}" -f $_.TaskName, $_.Settings.Enabled, $_.State)
}

Write-Host ''
Write-Host '=== Logs around 2026-10-01 02:00 ==='
Get-ChildItem 'C:\Dev\dealality-backups\logs' -File -EA SilentlyContinue |
  Where-Object { $_.LastWriteTime -ge [datetime]'2026-10-01' -and $_.LastWriteTime -lt [datetime]'2026-10-01 06:00' } |
  Format-Table Name, Length, LastWriteTime -AutoSize

# Any log from early morning Oct 1?
Get-ChildItem 'C:\Dev\dealality-backups\logs' -Filter 'Dealality-2026-10-01_02*.log' -EA SilentlyContinue |
  ForEach-Object {
    Write-Host ("--- {0} ---" -f $_.Name)
    Get-Content $_.FullName -TotalCount 40 -Encoding UTF8 -EA SilentlyContinue
    Write-Host '...'
    Get-Content $_.FullName -Tail 30 -Encoding UTF8 -EA SilentlyContinue
  }

# Staging 0200 was written at 2AM - prior architecture?
Write-Host ''
Write-Host '=== Staging Oct1 0200 mtime (historical write?) ==='
if (Test-Path 'C:\Dev\Backup-Staging\2026-10-01_0200') {
  Get-Item 'C:\Dev\Backup-Staging\2026-10-01_0200' | Format-List FullName, CreationTime, LastWriteTime
}

Write-Host ''
Write-Host '=== Event log TaskScheduler around 2AM Oct1 (if available) ==='
try {
  Get-WinEvent -FilterHashtable @{ LogName='Microsoft-Windows-TaskScheduler/Operational'; StartTime=[datetime]'2026-10-01 01:50'; EndTime=[datetime]'2026-10-01 03:00' } -ErrorAction Stop |
    Where-Object { $_.Message -match 'Dealality|Backup-Dealality' } |
    Select-Object -First 20 TimeCreated, Id, LevelDisplayName, Message |
    ForEach-Object {
      Write-Host ("{0} Id={1} {2}" -f $_.TimeCreated, $_.Id, ($_.Message -replace '\s+',' ').Substring(0, [Math]::Min(220, ($_.Message -replace '\s+',' ').Length)))
    }
} catch {
  Write-Host ("Event log query failed/unavailable: {0}" -f $_.Exception.Message)
}

# System events for shutdown/logoff around that time
try {
  Get-WinEvent -FilterHashtable @{ LogName='System'; StartTime=[datetime]'2026-10-01 01:50'; EndTime=[datetime]'2026-10-01 03:30'; Id=1074,6006,6008,41,42,1 } -ErrorAction SilentlyContinue |
    Select-Object -First 15 TimeCreated, Id, ProviderName, Message |
    ForEach-Object {
      $msg = ($_.Message -replace '\s+',' ')
      Write-Host ("SYS {0} Id={1} {2}" -f $_.TimeCreated, $_.Id, $msg.Substring(0, [Math]::Min(180, $msg.Length)))
    }
} catch {
  Write-Host ("System event query: {0}" -f $_.Exception.Message)
}
