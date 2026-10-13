# READ-ONLY scheduled task audit
$ErrorActionPreference = 'Continue'
Write-Host '=== ALL TASKS matching Dealality/backup/Cursor/Dev ==='
$tasks = Get-ScheduledTask -ErrorAction SilentlyContinue | Where-Object {
  $_.TaskName -match 'Dealality|backup|Backup|Cursor|Nightly|deal-capture|Robocopy' -or
  ($_.Actions.Arguments -match 'Backup-Scripts|dealality-backups|Backup-Staging|Nightly-Backups|deal-capture-proxy') -or
  ($_.Actions.Execute -match 'Backup')
}
foreach ($t in $tasks) {
  $info = Get-ScheduledTaskInfo -TaskName $t.TaskName -TaskPath $t.TaskPath -EA SilentlyContinue
  $triggers = ($t.Triggers | ForEach-Object { $_.ToString() }) -join ' | '
  Write-Host '---'
  Write-Host ("Name: {0}" -f $t.TaskName)
  Write-Host ("Path: {0}" -f $t.TaskPath)
  Write-Host ("State: {0}" -f $t.State)
  Write-Host ("Enabled: {0}" -f $t.Settings.Enabled)
  Write-Host ("LastResult: {0}" -f $info.LastTaskResult)
  Write-Host ("LastRun: {0}" -f $info.LastRunTime)
  Write-Host ("NextRun: {0}" -f $info.NextRunTime)
  foreach ($a in $t.Actions) {
    Write-Host ("Execute: {0}" -f $a.Execute)
    Write-Host ("Arguments: {0}" -f $a.Arguments)
    Write-Host ("WorkingDirectory: {0}" -f $a.WorkingDirectory)
  }
  Write-Host ("Triggers: {0}" -f $triggers)
}

Write-Host ''
Write-Host '=== BROADER: any task whose Arguments mention C:\Dev ==='
$devTasks = Get-ScheduledTask -EA SilentlyContinue | Where-Object {
  ($_.Actions.Arguments -match 'C:\\Dev') -or ($_.Actions.WorkingDirectory -match 'C:\\Dev')
}
foreach ($t in $devTasks) {
  Write-Host ("{0} | State={1} | Enabled={2} | Args={3}" -f $t.TaskName, $t.State, $t.Settings.Enabled, ($t.Actions.Arguments -join ';'))
}
Write-Host ("Dev-related task count: {0}" -f @($devTasks).Count)
