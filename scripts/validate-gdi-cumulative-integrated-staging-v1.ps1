#Requires -Version 5.1
<#
.SYNOPSIS
  Fail-closed validator for the cumulative GDI founder review environment on :8080.

  Verifies authorized workspace identity, served assets, feature manifest,
  durable STAGED inventory, opportunity-ID preservation, CURRENT isolation,
  and API health. A healthy HTTP response alone is NOT sufficient.

.EXAMPLE
  powershell -NoProfile -File scripts/validate-gdi-cumulative-integrated-staging-v1.ps1
  npm run dealality:validate-gdi-cumulative-8080
#>
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Fail($msg) {
  Write-Host "FAIL: $msg" -ForegroundColor Red
  throw $msg
}

Write-Host "== GDI cumulative integrated staging validator ==" -ForegroundColor Cyan
Write-Host "Contract: DEALALITY_ADDITIVE_NON_REGRESSION_V1 + CANONICAL_PORT_8080_OWNERSHIP_V1"

# 1) Port 8080 listener + cwd
$listen = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $listen) { Fail "Port 8080 is not listening — integrated readiness FAILED" }
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
$cwd = $null
try { $cwd = [GdiCwd]::GetCwd($pid8080) } catch { $cwd = $null }
$metaPath = Join-Path $Root "tmp\gdi-local-8080.meta.json"
if (-not $cwd -and (Test-Path $metaPath)) {
  $meta = Get-Content $metaPath -Raw | ConvertFrom-Json
  if ([int]$meta.pid -eq $pid8080) { $cwd = [string]$meta.cwd }
}
Write-Host "[1b] serving cwd=$cwd"
if (-not $cwd) { Fail "Unable to resolve :8080 process cwd — integrated readiness FAILED" }
$norm = $cwd.TrimEnd('\','/').ToLowerInvariant()
$expected = (Resolve-Path $Root).Path.TrimEnd('\','/').ToLowerInvariant()
if ($norm -ne $expected) {
  Fail "Port 8080 is serving '$cwd' — expected cumulative GDI workspace '$Root' (unauthorized worktree regression)"
}

# 2) Git identity of serving workspace
Push-Location $Root
$branch = (git branch --show-current).Trim()
$head = (git rev-parse HEAD).Trim()
$headShort = (git rev-parse --short HEAD).Trim()
$main = (git rev-parse origin/main).Trim()
Pop-Location
Write-Host "[2] branch=$branch head=$headShort origin/main=$($main.Substring(0,8))"

# 3) Runtime identity API (ownership + durable package pin)
function Get-Json($url) {
  try {
    $r = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 45
    return ($r.Content | ConvertFrom-Json)
  } catch {
    $resp = $_.Exception.Response
    if (-not $resp) { throw }
    $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
    $body = $reader.ReadToEnd()
    return ($body | ConvertFrom-Json)
  }
}

$identity = Get-Json "http://127.0.0.1:8080/api/dev/runtime-identity"
if (-not $identity.ok) { Fail "runtime-identity not ok" }
if (-not $identity.portOwnership -or $identity.portOwnership.authorizedFor8080 -ne $true) {
  Fail "runtime-identity missing authorizedFor8080 ownership"
}
if ($identity.portOwnership.ownerStream -ne "gdi" -and $identity.portOwnership.ownerStream -ne "integration") {
  Fail "runtime-identity ownerStream=$($identity.portOwnership.ownerStream) not gdi|integration"
}
if (-not $identity.publication -or -not $identity.publication.wyndhamStagedVersion) {
  Fail "runtime-identity missing Wyndham STAGED version"
}
Write-Host "[3] ownership=$($identity.portOwnership.code) stream=$($identity.portOwnership.ownerStream) staged=$($identity.publication.wyndhamStagedVersion)"

# 4) Served assets — Executive Summary + List + four-tab registry markers
$html = (Invoke-WebRequest "http://127.0.0.1:8080/group-demand-intelligence.html?ts=$(Get-Random)" -UseBasicParsing -Headers @{"Cache-Control"="no-cache"}).Content
if ($html -notmatch 'executive-summary-tab\.js') { Fail "Served HTML missing Executive Summary script — founder review NOT READY" }
if ($html -notmatch 'gdi-commercial-list-table-v1\.js') { Fail "Served HTML missing sortable List View script" }
if ($html -notmatch 'dealality-gdi-ui\.js\?v=') { Fail "Served HTML missing dealality-gdi-ui cache marker" }
if ($html -match 'hcp-ui-howto-restore-v1' -and $html -notmatch 'executive-summary-tab') {
  Fail "Served HTML looks like foreign pre-ES checkout"
}
$uiJs = (Invoke-WebRequest "http://127.0.0.1:8080/js/group-demand-intelligence/dealality-gdi-ui.js?v=probe" -UseBasicParsing).Content
if ($uiJs -notmatch 'GDI_EXEC_SUMMARY_TAB|id:\s*"executive-summary"') { Fail "Served UI missing Executive Summary tab registry" }
if ($uiJs -notmatch 'GDI_CUSTOMER_TABS') { Fail "Served UI missing GDI_CUSTOMER_TABS navigation registry" }
$esJs = (Invoke-WebRequest "http://127.0.0.1:8080/js/group-demand-intelligence/executive-summary-tab.js?v=probe" -UseBasicParsing).Content
if ($esJs -notmatch 'opportunityTileHtml|Priority Commercial|Research Productivity|Research Receipt') {
  Fail "Served Executive Summary missing consolidated dashboard markers"
}
Write-Host "[4] served assets include ES + List + four-tab registry"

# 5) Permanent unit gates (feature manifest)
npm run test:canonical-port-8080-ownership-v1
if ($LASTEXITCODE -ne 0) { Fail "canonical port ownership test failed" }
npm run test:gdi-cumulative-integrated-staging-non-regression-v1
if ($LASTEXITCODE -ne 0) { Fail "cumulative non-regression test failed" }
npm run test:gdi-canonical-e2e-to-staged-lifecycle-v1
if ($LASTEXITCODE -ne 0) { Fail "canonical E2E→STAGED lifecycle test failed" }
Write-Host "[5] ownership + cumulative + lifecycle unit gates PASS"

# 6) STAGED / CURRENT package counts + opportunity-ID preservation
$wyStaged = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities?publicationPackage=1&preview=staged&surface=customer"
if (-not $wyStaged.ok -or [int]$wyStaged.count -ne 26) {
  Fail "Wyndham STAGED expected 26 (ok=$($wyStaged.ok) count=$($wyStaged.count) err=$($wyStaged.error)) — inventory regression"
}
$ver = [string]$wyStaged.publication.publicationVersion
if ($ver -notmatch '20261127') {
  Fail "Wyndham STAGED version stale or unexpected: $ver (do not revert to CLEAN11)"
}
if ($ver -match '3283ee92') { Fail "Wyndham STAGED reverted to CLEAN11 package $ver" }
Write-Host "[6] Wyndham STAGED count=$($wyStaged.count) version=$ver"

$wyCurrent = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities?publicationPackage=1&preview=current&surface=customer"
if ($wyCurrent.ok -eq $true -and [int]$wyCurrent.count -gt 0) {
  Fail "Wyndham CURRENT must remain unpublished (ok=$($wyCurrent.ok) count=$($wyCurrent.count))"
}
Write-Host "[6a] Wyndham CURRENT unpublished (customer isolation OK)"

$beth = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities?publicationPackage=1&preview=current&surface=customer"
if (-not $beth.ok -or [int]$beth.count -lt 1) { Fail "Bethesda CURRENT unavailable or empty" }
Write-Host "[6b] Bethesda CURRENT count=$($beth.count)"

$yotel = Get-Json "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/recrPQcZg7SFARRb2/opportunities?publicationPackage=1&preview=current&surface=customer"
if (-not $yotel.ok -or [int]$yotel.count -lt 1) { Fail "YOTEL CURRENT unavailable or empty" }
Write-Host "[6c] YOTEL CURRENT count=$($yotel.count)"

# 7) Admin Resources routes present in app shell (gated; presence check)
$appJs = (Invoke-WebRequest "http://127.0.0.1:8080/app.js?v=probe" -UseBasicParsing).Content
if ($appJs -notmatch 'gdi-sales-performance|GDI Sales Performance') {
  Write-Host "[7] WARN: app.js missing GDI Sales Performance route marker (check admin nav)" -ForegroundColor Yellow
} else {
  Write-Host "[7] Admin Resources GDI Sales Performance marker present"
}
if ($appJs -notmatch 'gdi-research-intelligence|GDI Research Intelligence') {
  Write-Host "[7b] WARN: app.js missing GDI Research Intelligence route marker" -ForegroundColor Yellow
} else {
  Write-Host "[7b] Admin Resources GDI Research Intelligence marker present"
}

# 8) List toolbar contract on served UI
if ($uiJs -notmatch 'data-gdi-toolbar-row|gdiListColumnSettingsBtn') {
  Fail "Served UI missing List toolbar / Columns control markers"
}
Write-Host "[8] List toolbar / Columns markers OK"

Write-Host ""
Write-Host "PASS validate-gdi-cumulative-integrated-staging-v1" -ForegroundColor Green
Write-Host "Integrated readiness: READY_FOR_FOUNDER_REVIEW"
Write-Host "Lifecycle: FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT"
Write-Host "Browser: http://localhost:8080/group-demand-intelligence.html (authorized founder session)"
Write-Host "HEAD: $headShort ($branch)"
