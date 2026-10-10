Write-Host '=== G:\My Drive\Dealality Backups contents ==='
Get-ChildItem 'G:\My Drive\Dealality Backups' -Force | ForEach-Object {
  $kind = if ($_.PSIsContainer) { 'DIR' } else { 'FILE' }
  $size = if (-not $_.PSIsContainer) { $_.Length } else {
    try {
      $files = Get-ChildItem $_.FullName -Recurse -File -EA SilentlyContinue
      ($files | Measure-Object Length -Sum).Sum
    } catch { 0 }
  }
  Write-Host ("{0}`t{1}`tbytes={2}`t{3}" -f $kind, $_.Name, $size, $_.FullName)
}
Write-Host '=== 2026-10-01_2036 ==='
Get-ChildItem 'G:\My Drive\Dealality Backups\2026-10-01_2036' | Format-Table Name, Length -AutoSize
