#Requires -Version 5.1
<#
.SYNOPSIS
  Fail-closed validator for the cumulative GDI founder review environment on :8080.

.EXAMPLE
  powershell -NoProfile -File scripts/validate-gdi-cumulative-integrated-staging-v1.ps1
#>
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Fail($msg) { throw $msg }

Write-Host "== GDI cumulative integrated staging validator ==" -ForegroundColor Cyan

# 1) Port 8080 listener + cwd
$listen = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $listen) { Fail "Port 8080 is not listening" }
$pid8080 = [int]$listen.OwningProcess
Write-Host "[1] :8080 pid=$pid8080"

$code = @'
using System; using System.Runtime.InteropServices; using System.Text;
public static class GdiCwd {
  [DllImport("ntdll.dll")] public static extern int NtQueryInformationProcess(IntPtr p, int i, ref PBI b, int s, out int r);
  [DllImport("kernel32.dll", SetLastError=true)] public static extern IntPtr OpenProcess(uint a, bool i, int pid);
  [DllImport("kernel32.dll", SetLastError=true)] public static extern bool ReadProcessMemory(IntPtr h, IntPtr a, byte[] b, int s, out int r);
  [DllImport("kernel32.dll", SetLastError=true)] public static extern bool CloseHandle(IntPtr h);
  [StructLayout(LayoutKind.Sequential)] public struct PBI { public IntPtr a,b,c,d,e,f; }
  public static string GetCwd(int pid) {
    IntPtr h = OpenProcess(0x0010|0x0400, false, pid); if (h==IntPtr.Zero) return null;
    try { PBI pbi=new PBI(); int ret; if (NtQueryInformationProcess(h,0,ref pbi,Marshal.SizeOf(pbi),out ret)!=0) return null;
      byte[] ptr=new byte[8]; int rr; if(!ReadProcessMemory(h,pbi.b+0x20,ptr,8,out rr)) return null;
      IntPtr pp=(IntPtr)BitConverter.ToInt64(ptr,0); byte[] us=new byte[16]; if(!ReadProcessMemory(h,pp+0x38,us,16,out rr)) return null;
      short len=BitConverter.ToInt16(us,0); IntPtr buf=(IntPtr)BitConverter.ToInt64(us,8); byte[] path=new byte[len];
      if(!ReadProcessMemory(h,buf,path,len,out rr)) return null; return Encoding.Unicode.GetString(path);
    } finally { CloseHandle(h);} }
}
'@
Add-Type -TypeDefinition $code -ErrorAction SilentlyContinue | Out-Null
$cwd = [GdiCwd]::GetCwd($pid8080)
Write-Host "[1b] serving cwd=$cwd"
if (-not $cwd) { Fail "Unable to resolve :8080 process cwd" }
$norm = $cwd.TrimEnd('\','/').ToLowerInvariant()
$expected = (Resolve-Path $Root).Path.TrimEnd('\','/').ToLowerInvariant()
if ($norm -ne $expected) {
  Fail "Port 8080 is serving '$cwd' — expected cumulative GDI workspace '$Root'"
}

# 2) Git identity of serving workspace
Push-Location $Root
$branch = (git branch --show-current).Trim()
$head = (git rev-parse HEAD).Trim()
$main = (git rev-parse origin/main).Trim()
Pop-Location
Write-Host "[2] branch=$branch head=$head origin/main=$main"

# 3) Served assets
$html = (Invoke-WebRequest "http://127.0.0.1:8080/group-demand-intelligence.html?ts=$(Get-Random)" -UseBasicParsing).Content
if ($html -notmatch 'executive-summary-tab\.js') { Fail "Served HTML missing Executive Summary script" }
if ($html -notmatch 'gdi-commercial-list-table-v1\.js') { Fail "Served HTML missing sortable List View script" }
if ($html -notmatch 'dealality-gdi-ui\.js\?v=') { Fail "Served HTML missing dealality-gdi-ui cache marker" }
Write-Host "[3] served assets include ES + List table modules"

# 4) Permanent non-regression unit gate
npm run test:gdi-cumulative-integrated-staging-non-regression-v1
if ($LASTEXITCODE -ne 0) { Fail "cumulative non-regression test failed" }
npm run test:gdi-sortable-commercial-list-view-v1
if ($LASTEXITCODE -ne 0) { Fail "sortable list regression failed" }

# 5) STAGED / CURRENT package counts (Wyndham + Bethesda)
function Get-Json($url) {
  try {
    $r = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 30
    return ($r.Content | ConvertFrom-Json)
  } catch {
    $resp = $_.Exception.Response
    if (-not $resp) { throw }
    $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
    $body = $reader.ReadToEnd()
    return ($body | ConvertFrom-Json)
  }
}

$wyStaged = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities?publicationPackage=1&preview=staged&surface=customer"
if (-not $wyStaged.ok -or [int]$wyStaged.count -ne 11) {
  Fail "Wyndham STAGED expected 11 (ok=$($wyStaged.ok) count=$($wyStaged.count) err=$($wyStaged.error))"
}
Write-Host "[5] Wyndham STAGED count=$($wyStaged.count)"

$wyProposed = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities?publicationPackage=1&preview=proposed&surface=customer"
if (-not $wyProposed.ok -or [int]$wyProposed.count -ne 26) {
  Fail "Wyndham Proposed expected 26 (ok=$($wyProposed.ok) count=$($wyProposed.count) err=$($wyProposed.error) scope=$($wyProposed.listScope))"
}
if ($wyProposed.listScope -ne "FOUNDER_PROPOSED_WRITE_SET") {
  Fail "Wyndham Proposed listScope expected FOUNDER_PROPOSED_WRITE_SET got $($wyProposed.listScope)"
}
Write-Host "[5a] Wyndham Proposed locked-26 count=$($wyProposed.count) scope=$($wyProposed.listScope)"

$wyHold = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities?publicationPackage=1&preview=hold&surface=customer"
if (-not $wyHold.ok -or [int]$wyHold.count -ne 2) {
  Fail "Wyndham HOLD expected 2"
}
Write-Host "[5a2] Wyndham HOLD count=$($wyHold.count)"

$beth = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities?publicationPackage=1&preview=current&surface=customer"
if (-not $beth.ok -or [int]$beth.count -lt 1) {
  Fail "Bethesda CURRENT unavailable or empty"
}
Write-Host "[5b] Bethesda CURRENT count=$($beth.count)"

$yotel = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recrPQcZg7SFARRb2/opportunities?publicationPackage=1&preview=current&surface=customer"
if (-not $yotel.ok -or [int]$yotel.count -lt 1) {
  Fail "YOTEL CURRENT unavailable or empty"
}
Write-Host "[5c] YOTEL CURRENT count=$($yotel.count)"

# 6) Served toolbar contract (single-row Columns SVG)
$html2 = (Invoke-WebRequest "http://127.0.0.1:8080/group-demand-intelligence.html?ts=$(Get-Random)" -UseBasicParsing).Content
if ($html2 -notmatch 'gdi-founder-inv-toolbar-v1') { Fail "Served HTML missing founder-inv-toolbar cache marker" }
$uiJs = (Invoke-WebRequest "http://127.0.0.1:8080/js/group-demand-intelligence/dealality-gdi-ui.js?v=gdi-founder-inv-toolbar-v1" -UseBasicParsing).Content
if ($uiJs -notmatch 'data-gdi-toolbar-row') { Fail "Served UI missing single-row toolbar marker" }
$tableJs = (Invoke-WebRequest "http://127.0.0.1:8080/js/group-demand-intelligence/gdi-commercial-list-table-v1.js?v=gdi-founder-inv-toolbar-v1" -UseBasicParsing).Content
if ($tableJs -notmatch 'Customize Columns') { Fail "Served list table missing Customize Columns control" }
if ($tableJs -match 'gdi-list-table-toolbar') { Fail "Served list table still renders separate Columns toolbar row" }
Write-Host "[6] Single-row List toolbar + Columns SVG contract OK"

Write-Host "`nPASS validate-gdi-cumulative-integrated-staging-v1" -ForegroundColor Green
Write-Host "Browser: http://localhost:8080/group-demand-intelligence.html (authorized founder session)"
