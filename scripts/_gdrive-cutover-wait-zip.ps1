# Poll until backup job completes or ZIP mtime stalls + log shows done
$ErrorActionPreference = 'Continue'
$zip = 'G:\My Drive\Dealality Backups\2026-10-01_2036\Dealality-backup.zip'
$log = 'C:\Dev\dealality-backups\logs\Dealality-2026-10-01_2036.log'
$term = 'C:\Users\joand\.cursor\projects\c-Dev-deal-capture-proxy\terminals\677063.txt'

$maxRounds = 40
$prevLen = -1
$stable = 0
for ($i = 1; $i -le $maxRounds; $i++) {
  Start-Sleep -Seconds 60
  $zi = Get-Item -LiteralPath $zip -EA SilentlyContinue
  $len = if ($zi) { $zi.Length } else { 0 }
  $tail = @(Get-Content -LiteralPath $log -Tail 5 -Encoding UTF8 -EA SilentlyContinue)
  $termTail = @(Get-Content -LiteralPath $term -Tail 8 -EA SilentlyContinue)
  $done = ($termTail -join "`n") -match 'exit_code:'
  $logDone = ($tail -join "`n") -match 'Dealality backup (complete|finished|end)|BACKUP COMPLETE|Google Drive ZIP size|STATUS: VERIFIED'
  Write-Host ("[{0}/{1}] zipGB={2:N2} mtime={3} doneTerm={4}" -f $i, $maxRounds, ($len/1GB), $(if($zi){$zi.LastWriteTime}else{'n/a'}), $done)
  if ($len -eq $prevLen -and $len -gt 0) { $stable++ } else { $stable = 0 }
  $prevLen = $len
  if ($done) {
    Write-Host 'TERMINAL DONE'
    Get-Content $term -Tail 40
    Get-Content $log -Tail 40 -Encoding UTF8
    break
  }
  if ($logDone -and $stable -ge 2) {
    Write-Host 'LOG suggests complete + zip stable'
    Get-Content $log -Tail 40 -Encoding UTF8
    break
  }
  # If zip hasn't grown for 3 minutes and _CLOUD_PACK_TEMP gone, likely done
  $pack = Test-Path 'C:\Dev\dealality-backups\_CLOUD_PACK_TEMP'
  if (-not $pack -and $stable -ge 3) {
    Write-Host 'Pack gone + zip stable'
    Get-Content $log -Tail 40 -Encoding UTF8
    break
  }
}
Write-Host '---FINAL ZIP---'
Get-Item $zip -EA SilentlyContinue | Format-List FullName, Length, LastWriteTime
Write-Host '---ROOT---'
Get-ChildItem 'C:\Dev\dealality-backups' -Force | Select-Object Name
