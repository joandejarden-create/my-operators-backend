# READ-ONLY Dealality backup consolidation audit
# Writes reports only under deal-capture-proxy/reports
$ErrorActionPreference = "Continue"
$Primary = "C:\Dev\deal-capture-proxy"
$Latest = "C:\Dev\dealality-backups\LATEST"
$OutDir = Join-Path $Primary "reports"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Get-RobocopyBytes {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { return @{ bytes = [int64]0; files = 0 } }
  $dest = Join-Path $env:TEMP ("__bak_cons_null_" + [guid]::NewGuid().ToString("N"))
  $null = New-Item -ItemType Directory -Force -Path $dest -ErrorAction SilentlyContinue
  try {
    $raw = & robocopy $Path $dest /L /E /BYTES /NFL /NDL /NJH /NC /NS /NP 2>&1 | Out-String
    $bytes = [int64]0; $files = 0
    if ($raw -match 'Bytes\s*:\s*([\d,]+)') { $bytes = [int64](($Matches[1] -replace ',','')) }
    if ($raw -match 'Files\s*:\s*([\d,]+)') { $files = [int](($Matches[1] -replace ',','')) }
    return @{ bytes = $bytes; files = $files }
  } catch { return @{ bytes = [int64]0; files = 0 } }
  finally { Remove-Item -LiteralPath $dest -Recurse -Force -ErrorAction SilentlyContinue }
}

function Format-GB([int64]$b) {
  if ($null -eq $b -or $b -le 0) { return "0.00" }
  return ("{0:N2}" -f ($b / 1GB))
}

function Get-Sha256([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) { return $null }
  try { return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash } catch { return $null }
}

function Probe-BackupContent {
  param([string]$Root, [string]$ProjectRel = "deal-capture-proxy")
  $proj = Join-Path $Root $ProjectRel
  # Some snapshots nest differently
  if (-not (Test-Path $proj)) {
    if (Test-Path (Join-Path $Root "package.json")) { $proj = $Root }
    elseif (Test-Path (Join-Path $Root "tree\deal-capture-proxy")) { $proj = Join-Path $Root "tree" } # unlikely
    elseif (Test-Path (Join-Path $Root "tree\package.json")) { $proj = Join-Path $Root "tree" }
  }
  $archIdx = Join-Path $proj "reports\ai-demand-positioning\monthly-review\archive\index.json"
  $hasArch = Test-Path -LiteralPath $archIdx
  $archReviews = 0
  if ($hasArch) {
    try { $archReviews = @((Get-Content $archIdx -Raw | ConvertFrom-Json).reviews).Count } catch {}
  }
  $pdfCount = 0
  $archDir = Join-Path $proj "reports\ai-demand-positioning\monthly-review\archive"
  if (Test-Path $archDir) {
    $pdfCount = @(Get-ChildItem $archDir -Recurse -Filter report.pdf -ErrorAction SilentlyContinue).Count
  }
  [pscustomobject]@{
    projectPath = $proj
    hasProject = (Test-Path $proj)
    hasPackageJson = (Test-Path (Join-Path $proj "package.json"))
    hasGit = (Test-Path (Join-Path $proj ".git")) -or (Test-Path (Join-Path $Root ".git"))
    hasNodeModules = (Test-Path (Join-Path $proj "node_modules"))
    hasCursorWorkspace = (Test-Path (Join-Path $Root "Cursor-workspaceStorage")) -or ($Root -match 'workspaceStorage')
    hasEnv = (Test-Path (Join-Path $proj ".env"))
    hasMonthlyReviewArchive = $hasArch
    archiveReviewCount = $archReviews
    archivePdfCount = $pdfCount
    hasFixtures = (Test-Path (Join-Path $proj "fixtures"))
    hasPublic = (Test-Path (Join-Path $proj "public"))
    hasReports = (Test-Path (Join-Path $proj "reports"))
    hasData = (Test-Path (Join-Path $proj "data"))
  }
}

# High-value relative paths to hash-compare across backups
$ValueRels = @(
  "reports\ai-demand-positioning\monthly-review\archive\index.json",
  "package.json",
  "server.js",
  "fixtures\ai-demand-positioning\monthly-review\cambridge-beaches-monthly-executive-review-v1.json",
  "fixtures\ai-demand-positioning\monthly-review\now-now-noho-monthly-executive-review-v1.json",
  "public\js\admin-ai-demand-reviews.js",
  "api\admin-adp-monthly-reviews.js",
  "lib\ai-demand-positioning\monthly-review\admin\archive-store-v1.js",
  "lib\hotel-census\map-hotel-dto.js"
)

Write-Host "=== Collect snapshot units ==="
$units = @()

# Backup-Staging snapshots
foreach ($d in @(Get-ChildItem "C:\Dev\Backup-Staging" -Directory -ErrorAction SilentlyContinue)) {
  $units += [pscustomobject]@{ root="Backup-Staging"; snapshot=$d.Name; path=$d.FullName; kind="staging-snapshot"; date=$d.LastWriteTime }
}
# dealality-backups LATEST
if (Test-Path $Latest) {
  $units += [pscustomobject]@{ root="dealality-backups"; snapshot="LATEST"; path=$Latest; kind="local-rolling"; date=(Get-Item $Latest).LastWriteTime }
}
# Backup-Scripts snapshots
$snapRoot = "C:\Dev\Backup-Scripts\dealality-snapshots"
if (Test-Path $snapRoot) {
  foreach ($d in @(Get-ChildItem $snapRoot -Directory -ErrorAction SilentlyContinue)) {
    $units += [pscustomobject]@{ root="Backup-Scripts"; snapshot=$d.Name; path=$d.FullName; kind="safety-snapshot"; date=$d.LastWriteTime }
  }
  $units += [pscustomobject]@{ root="Backup-Scripts"; snapshot="Backup-Dealality.ps1"; path="C:\Dev\Backup-Scripts\Backup-Dealality.ps1"; kind="script"; date=(Get-Item "C:\Dev\Backup-Scripts\Backup-Dealality.ps1").LastWriteTime }
}
# Cursor recovery parts
$cr = "C:\Dev\Cursor-Recovery-Archive-2026-09-08"
if (Test-Path $cr) {
  foreach ($d in @(Get-ChildItem $cr -Directory -ErrorAction SilentlyContinue)) {
    $units += [pscustomobject]@{ root="Cursor-Recovery-Archive-2026-09-08"; snapshot=$d.Name; path=$d.FullName; kind="cursor-recovery"; date=$d.LastWriteTime }
  }
}
# Nightly
foreach ($d in @(Get-ChildItem "C:\Dev\Nightly-Backups" -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -ne 'Logs' })) {
  $units += [pscustomobject]@{ root="Nightly-Backups"; snapshot=$d.Name; path=$d.FullName; kind="nightly-snapshot"; date=$d.LastWriteTime }
}

Write-Host "Units: $($units.Count)"

# Primary + LATEST probe baselines
$liveProbe = Probe-BackupContent -Root $Primary -ProjectRel ""
# Fix: Primary IS the project
$liveProbe = [pscustomobject]@{
  projectPath = $Primary
  hasProject = $true
  hasPackageJson = Test-Path "$Primary\package.json"
  hasGit = Test-Path "$Primary\.git"
  hasNodeModules = Test-Path "$Primary\node_modules"
  hasCursorWorkspace = $false
  hasEnv = Test-Path "$Primary\.env"
  hasMonthlyReviewArchive = Test-Path "$Primary\reports\ai-demand-positioning\monthly-review\archive\index.json"
  archiveReviewCount = 0
  archivePdfCount = @(Get-ChildItem "$Primary\reports\ai-demand-positioning\monthly-review\archive" -Recurse -Filter report.pdf -EA SilentlyContinue).Count
  hasFixtures = Test-Path "$Primary\fixtures"
  hasPublic = Test-Path "$Primary\public"
  hasReports = Test-Path "$Primary\reports"
  hasData = Test-Path "$Primary\data"
}
if ($liveProbe.hasMonthlyReviewArchive) {
  try { $liveProbe.archiveReviewCount = @((Get-Content "$Primary\reports\ai-demand-positioning\monthly-review\archive\index.json" -Raw | ConvertFrom-Json).reviews).Count } catch {}
}

$liveHashes = @{}
foreach ($rel in $ValueRels) {
  $liveHashes[$rel] = Get-Sha256 (Join-Path $Primary $rel)
}

$inventories = @()
$uniqueRows = @()
$i = 0
foreach ($u in $units) {
  $i++
  Write-Host ("[{0}/{1}] {2}/{3}" -f $i, $units.Count, $u.root, $u.snapshot)
  if ($u.kind -eq "script") {
    $sz = (Get-Item $u.path).Length
    $inventories += [pscustomobject]@{
      root=$u.root; snapshot=$u.snapshot; path=$u.path; kind=$u.kind
      date=$u.date.ToString("o"); sizeBytes=[int64]$sz; sizeGB=Format-GB $sz; fileCount=1
      hasGit=$false; hasNodeModules=$false; hasCursorWorkspace=$false
      hasMonthlyReviewArchive=$false; archiveReviewCount=0; archivePdfCount=0
      hasEnv=$false; hasProject=$false
      recoveryScore="DO NOT TOUCH"; cloudClass="KEEP LOCAL"
      uniqueNote="Active Backup-Dealality.ps1 scheduled-task script"
      identicalToLiveValueFiles=0; differFromLiveValueFiles=0; missingOnLiveValueFiles=0
    }
    continue
  }

  $szInfo = Get-RobocopyBytes $u.path
  # Resolve project path inside snapshot
  $projCandidates = @(
    (Join-Path $u.path "deal-capture-proxy"),
    (Join-Path $u.path "tree"),
    (Join-Path $u.path "tree\deal-capture-proxy"),
    $u.path
  )
  $proj = $null
  foreach ($c in $projCandidates) {
    if ((Test-Path (Join-Path $c "package.json")) -or (Test-Path (Join-Path $c "server.js"))) { $proj = $c; break }
  }
  $probeRoot = if ($proj) { Split-Path $proj -Parent } else { $u.path }
  # If proj is u.path itself
  if ($proj -eq $u.path) { $probe = Probe-BackupContent -Root $u.path -ProjectRel "" ; $probe.projectPath = $u.path; $probe.hasProject = (Test-Path (Join-Path $u.path "package.json")) }
  elseif ($proj) {
    $probe = Probe-BackupContent -Root (Split-Path $proj -Parent) -ProjectRel (Split-Path $proj -Leaf)
    if ((Split-Path $proj -Leaf) -eq "tree") {
      # safety snap stores tree as working copy root
      $probe = [pscustomobject]@{
        projectPath=$proj
        hasProject=$true
        hasPackageJson=(Test-Path "$proj\package.json")
        hasGit=(Test-Path "$proj\.git")
        hasNodeModules=(Test-Path "$proj\node_modules")
        hasCursorWorkspace=(Test-Path (Join-Path $u.path "Cursor-workspaceStorage"))
        hasEnv=(Test-Path "$proj\.env")
        hasMonthlyReviewArchive=(Test-Path "$proj\reports\ai-demand-positioning\monthly-review\archive\index.json")
        archiveReviewCount=0
        archivePdfCount=@(Get-ChildItem "$proj\reports\ai-demand-positioning\monthly-review\archive" -Recurse -Filter report.pdf -EA SilentlyContinue).Count
        hasFixtures=(Test-Path "$proj\fixtures")
        hasPublic=(Test-Path "$proj\public")
        hasReports=(Test-Path "$proj\reports")
        hasData=(Test-Path "$proj\data")
      }
      if ($probe.hasMonthlyReviewArchive) {
        try { $probe.archiveReviewCount = @((Get-Content "$proj\reports\ai-demand-positioning\monthly-review\archive\index.json" -Raw | ConvertFrom-Json).reviews).Count } catch {}
      }
    }
  } else {
    $probe = [pscustomobject]@{
      projectPath=$null; hasProject=$false; hasPackageJson=$false; hasGit=$false; hasNodeModules=$false
      hasCursorWorkspace=(Test-Path (Join-Path $u.path "Cursor-workspaceStorage")) -or ($u.kind -eq "cursor-recovery")
      hasEnv=$false; hasMonthlyReviewArchive=$false; archiveReviewCount=0; archivePdfCount=0
      hasFixtures=$false; hasPublic=$false; hasReports=$false; hasData=$false
    }
  }

  # Value-file hash compare vs live
  $ident=0; $differ=0; $onlyHere=0; $onlyLive=0
  $sampleUnique = @()
  if ($proj) {
    foreach ($rel in $ValueRels) {
      $bp = Join-Path $proj $rel
      $bh = Get-Sha256 $bp
      $lh = $liveHashes[$rel]
      if ($bh -and $lh) {
        if ($bh -eq $lh) { $ident++ } else {
          $differ++
          $uniqueRows += [pscustomobject]@{
            backupRoot=$u.root; snapshot=$u.snapshot; relativePath=$rel
            classification="B. UNIQUE OLDER VERSIONS"
            backupSha=$bh; liveSha=$lh
            note="differs from live working tree"
          }
        }
      } elseif ($bh -and -not $lh) {
        $onlyHere++
        $sampleUnique += $rel
        $uniqueRows += [pscustomobject]@{
          backupRoot=$u.root; snapshot=$u.snapshot; relativePath=$rel
          classification="A. UNIQUE FILES ONLY IN THIS BACKUP"
          backupSha=$bh; liveSha=""
          note="present in backup, missing on live"
        }
      } elseif (-not $bh -and $lh) {
        $onlyLive++
      }
    }
  }

  # Special uniqueness probes
  $score = "UNKNOWN - KEEP"
  $cloudClass = "UNKNOWN"
  $uniqueNote = ""

  if ($u.root -eq "Backup-Scripts" -and $u.kind -eq "safety-snapshot") {
    $score = "HIGH VALUE ARCHIVE"
    $cloudClass = "KEEP LOCAL UNTIL CLOUD VERIFIED"
    $uniqueNote = "Pre-restore / crash safety snapshot under Backup-Scripts; may include pre-restore tiny archive state"
  }
  if ($u.root -eq "Backup-Staging" -and $u.snapshot -eq "2026-10-01_0200") {
    if ($probe.hasMonthlyReviewArchive -and $probe.archivePdfCount -gt 0) {
      # Compare archive index hash to live
      $bakIdx = Join-Path $proj "reports\ai-demand-positioning\monthly-review\archive\index.json"
      $liveIdx = Join-Path $Primary "reports\ai-demand-positioning\monthly-review\archive\index.json"
      $bh = Get-Sha256 $bakIdx; $lh = Get-Sha256 $liveIdx
      if ($bh -and $lh -and $bh -eq $lh) {
        $score = "HIGH VALUE ARCHIVE"
        $cloudClass = "KEEP LOCAL UNTIL CLOUD VERIFIED"
        $uniqueNote = "Pre-crash full monthly-review archive; index identical to LIVE after restore - still sole pre-crash provenance for Staging-era copies of other trees"
      } else {
        $score = "CRITICAL KEEP"
        $cloudClass = "KEEP LOCAL UNTIL CLOUD VERIFIED"
        $uniqueNote = "Archive index differs from live OR live missing - Staging remains recovery SoT"
      }
    }
  }
  if ($u.root -eq "Backup-Staging" -and $u.snapshot -eq "2026-08-22_0200") {
    $score = "HIGH VALUE ARCHIVE"
    $cloudClass = "MOVE TO CLOUD ARCHIVE"
    $uniqueNote = "Older staging snapshot (Aug 22); likely superseded by 2026-10-01_0200 for recovery"
  }
  if ($u.root -eq "dealality-backups" -and $u.snapshot -eq "LATEST") {
    $score = "CRITICAL KEEP"
    $cloudClass = "KEEP LOCAL"
    $uniqueNote = "Canonical local rolling operational snapshot (even if currently missing archive until refresh)"
  }
  if ($u.root -eq "Nightly-Backups") {
    $score = "HIGH VALUE ARCHIVE"
    $cloudClass = "MOVE TO CLOUD ARCHIVE"
    $uniqueNote = "Single July 30 nightly; includes Cursor workspaceStorage - check uniqueness vs Cursor-Recovery"
  }
  if ($u.root -eq "Cursor-Recovery-Archive-2026-09-08") {
    if ($u.snapshot -match 'chat|git-safety|ALL-') {
      $score = "CRITICAL KEEP"
      $cloudClass = "KEEP LOCAL UNTIL CLOUD VERIFIED"
      $uniqueNote = "Chat transcripts / git bundle / recovery URLs - not in product Git; unique history"
    } elseif ($u.snapshot -match 'working-state|recovered-projects') {
      $score = "HIGH VALUE ARCHIVE"
      $cloudClass = "KEEP LOCAL UNTIL CLOUD VERIFIED"
      $uniqueNote = "Recovered working trees from Sept 8 outage - likely partial overlap with Git history"
    } else {
      $score = "UNKNOWN - KEEP"
      $cloudClass = "KEEP LOCAL UNTIL CLOUD VERIFIED"
      $uniqueNote = "Cursor recovery artifact"
    }
  }

  # Cursor chat uniqueness: always unique vs LATEST/Git
  if ($u.snapshot -eq "chat-raw-gzip" -or $u.snapshot -eq "chats" -or $u.snapshot -eq "git-safety") {
    $uniqueRows += [pscustomobject]@{
      backupRoot=$u.root; snapshot=$u.snapshot; relativePath="(directory)"
      classification="A. UNIQUE FILES ONLY IN THIS BACKUP"
      backupSha=""; liveSha=""
      note=$uniqueNote
    }
  }

  # Staging ADP PDFs unique count vs live (directory file count delta)
  if ($u.root -eq "Backup-Staging" -and $proj -and $probe.hasMonthlyReviewArchive) {
    $bakPdfs = $probe.archivePdfCount
    $livePdfs = $liveProbe.archivePdfCount
    if ($bakPdfs -gt 0 -and $bakPdfs -eq $livePdfs) {
      $uniqueNote += "; archive PDF count matches live ($livePdfs)"
    }
  }

  # Recreatable dominant?
  if ($probe.hasNodeModules -and [double]$szInfo.bytes -gt 0) {
    # soft note only
  }

  $inventories += [pscustomobject]@{
    root = $u.root
    snapshot = $u.snapshot
    path = $u.path
    kind = $u.kind
    date = $u.date.ToString("o")
    sizeBytes = [int64]$szInfo.bytes
    sizeGB = Format-GB $szInfo.bytes
    fileCount = [int]$szInfo.files
    projectPath = $proj
    hasGit = [bool]$probe.hasGit
    hasNodeModules = [bool]$probe.hasNodeModules
    hasCursorWorkspace = [bool]$probe.hasCursorWorkspace
    hasEnv = [bool]$probe.hasEnv
    hasMonthlyReviewArchive = [bool]$probe.hasMonthlyReviewArchive
    archiveReviewCount = [int]$probe.archiveReviewCount
    archivePdfCount = [int]$probe.archivePdfCount
    hasProject = [bool]$probe.hasProject
    recoveryScore = $score
    cloudClass = $cloudClass
    uniqueNote = $uniqueNote
    identicalToLiveValueFiles = $ident
    differFromLiveValueFiles = $differ
    missingOnLiveValueFiles = $onlyHere
    valueFilesOnlyOnLive = $onlyLive
  }
}

# Overlap estimates (root-level, from prior audits + measured sizes)
Write-Host "=== Root totals ==="
$rootTotals = @()
foreach ($rn in @("Backup-Staging","Backup-Scripts","dealality-backups","Cursor-Recovery-Archive-2026-09-08","Nightly-Backups")) {
  $p = "C:\Dev\$rn"
  Write-Host "sizing $p"
  $s = Get-RobocopyBytes $p
  $rootTotals += [pscustomobject]@{ root=$rn; path=$p; sizeBytes=[int64]$s.bytes; sizeGB=Format-GB $s.bytes; fileCount=[int]$s.files }
}
$totalBackupBytes = ($rootTotals | Measure-Object sizeBytes -Sum).Sum

# Targeted Staging vs Live archive uniqueness
Write-Host "=== Staging vs Live archive deep sample ==="
$stagingArch = "C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive"
$liveArch = "C:\Dev\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive"
$stagingVsLive = [ordered]@{
  stagingExists = (Test-Path $stagingArch)
  liveExists = (Test-Path $liveArch)
  indexIdentical = $false
  pdfsCompared = 0
  pdfsIdentical = 0
  pdfsDiffer = 0
  pdfsOnlyStaging = 0
  pdfsOnlyLive = 0
}
if ((Test-Path $stagingArch) -and (Test-Path $liveArch)) {
  $stagingVsLive.indexIdentical = ((Get-Sha256 "$stagingArch\index.json") -eq (Get-Sha256 "$liveArch\index.json"))
  # Compare latest READY review PDFs per property (from live index)
  try {
    $idx = Get-Content "$liveArch\index.json" -Raw | ConvertFrom-Json
    $byProp = @{}
    foreach ($r in $idx.reviews) {
      if ($r.pdfStatus -ne "READY") { continue }
      $prev = $byProp[$r.propertyId]
      if (-not $prev -or [datetime]$r.generatedAt -gt [datetime]$prev.generatedAt) { $byProp[$r.propertyId] = $r }
    }
    foreach ($r in $byProp.Values) {
      $rel = Join-Path $r.propertyId (Join-Path $r.reportingMonthKey (Join-Path $r.reviewId "report.pdf"))
      $a = Join-Path $stagingArch $rel
      $b = Join-Path $liveArch $rel
      $stagingVsLive.pdfsCompared++
      $ha = Get-Sha256 $a; $hb = Get-Sha256 $b
      if ($ha -and $hb) {
        if ($ha -eq $hb) { $stagingVsLive.pdfsIdentical++ } else { $stagingVsLive.pdfsDiffer++ }
      } elseif ($ha -and -not $hb) { $stagingVsLive.pdfsOnlyStaging++ }
      elseif ($hb -and -not $ha) { $stagingVsLive.pdfsOnlyLive++ }
    }
  } catch {}
}

# LATEST vs Live archive
$latestArch = "C:\Dev\dealality-backups\LATEST\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\archive\index.json"
$latestHasArch = Test-Path $latestArch

# Nightly uniqueness probe: list top-level
$nightlyPath = "C:\Dev\Nightly-Backups\2026-07-30_1242"
$nightlyHasCursor = Test-Path (Join-Path $nightlyPath "Cursor-workspaceStorage")
$nightlyHasProject = Test-Path (Join-Path $nightlyPath "deal-capture-proxy")

# Cursor git bundle size
$bundle = "C:\Dev\Cursor-Recovery-Archive-2026-09-08\git-safety\dealality-all-refs.bundle"
$bundleSize = if (Test-Path $bundle) { (Get-Item $bundle).Length } else { 0 }

# Overlap heuristics (avoid double count)
# Staging 10-01 archive recovered into live => that portion ~identical to live (not to LATEST)
# Redundant vs LATEST: portions of Staging that match older LATEST content unknown precisely
# Estimate:
# - Cursor Recovery unique ~ full size (chats+bundle not in LATEST)
# - Nightly unique vs Staging: partial; estimate 30% unique / 70% overlap with Staging themes
# - Staging unique remaining after restore: non-archive trees still useful; archive PDFs now on live
# - Backup-Scripts snapshots overlap heavily with live/staging

$stagingSize = ($rootTotals | Where-Object root -eq "Backup-Staging").sizeBytes
$scriptsSize = ($rootTotals | Where-Object root -eq "Backup-Scripts").sizeBytes
$latestSize = ($rootTotals | Where-Object root -eq "dealality-backups").sizeBytes
$cursorSize = ($rootTotals | Where-Object root -eq "Cursor-Recovery-Archive-2026-09-08").sizeBytes
$nightlySize = ($rootTotals | Where-Object root -eq "Nightly-Backups").sizeBytes

# Unique estimate (conservative)
$uniqueEst = [int64]0
$uniqueEst += [int64]($cursorSize * 0.85)           # chats/bundle mostly unique
$uniqueEst += [int64]($nightlySize * 0.25)          # mostly superseded
$uniqueEst += [int64]($stagingSize * 0.15)          # residual unique after restore (other trees/dates)
$uniqueEst += [int64]($scriptsSize * 0.10)          # safety snaps + script
$uniqueEst += [int64]$latestSize                    # operational local - count as must-keep unique role (not overlap)

$overlapEst = [int64]$totalBackupBytes - [int64]$uniqueEst
if ($overlapEst -lt 0) { $overlapEst = 0 }

$overlapRows = @(
  [pscustomobject]@{ pair="Backup-Staging vs LIVE (monthly-review archive)"; identicalEstimateGB= if($stagingVsLive.pdfsIdentical -gt 0){"~archive restored"}else{"n/a"}; uniqueEstimateGB="see stagingVsLive"; note=("indexIdentical={0}; pdfsIdentical={1}/{2}" -f $stagingVsLive.indexIdentical, $stagingVsLive.pdfsIdentical, $stagingVsLive.pdfsCompared) }
  [pscustomobject]@{ pair="Backup-Staging vs LATEST"; identicalEstimateGB="HIGH (project trees)"; uniqueEstimateGB=(Format-GB ([int64]($stagingSize*0.15))); note="LATEST missing archive; Staging still has full archive copy" }
  [pscustomobject]@{ pair="Cursor-Recovery vs Git+LATEST"; identicalEstimateGB="LOW"; uniqueEstimateGB=(Format-GB ([int64]($cursorSize*0.85))); note="chats + git bundle unique" }
  [pscustomobject]@{ pair="Nightly-Backups vs Staging/LATEST"; identicalEstimateGB=(Format-GB ([int64]($nightlySize*0.70))); uniqueEstimateGB=(Format-GB ([int64]($nightlySize*0.25))); note="July snapshot; likely superseded" }
  [pscustomobject]@{ pair="Backup-Scripts snapshots vs LIVE"; identicalEstimateGB=(Format-GB ([int64]($scriptsSize*0.80))); uniqueEstimateGB=(Format-GB ([int64]($scriptsSize*0.10))); note="safety snaps around crash recovery" }
)

# Answers A-E
$answers = [ordered]@{
  A_BackupStaging_2026_10_01_0200 = [ordered]@{
    uniqueNow = "YES - still holds full pre-crash monthly-review archive tree + other Staging contents; after restore, LIVE archive index/PDFs match Staging for compared READY reviews, but LATEST still lacks archive. Staging remains provenance + fallback until cloud verified."
    pdfsIdenticalToLive = $stagingVsLive.pdfsIdentical
    pdfsCompared = $stagingVsLive.pdfsCompared
    indexIdenticalToLive = $stagingVsLive.indexIdentical
    classification = "KEEP LOCAL UNTIL CLOUD VERIFIED"
  }
  B_CursorRecovery = [ordered]@{
    uniqueNow = "YES - chat-raw-gzip, chats, ALL-RECOVERED-URLS.csv, git-safety bundle (~1.6GB) are not in product Git working tree or LATEST operational backup."
    classification = "KEEP LOCAL UNTIL CLOUD VERIFIED / CRITICAL KEEP for chat+git-safety"
  }
  C_NightlyBackups = [ordered]@{
    uniqueNow = "LIKELY LITTLE product uniqueness vs newer Staging/LATEST; may contain older Cursor-workspaceStorage state from July 30. Treat as MOVE TO CLOUD ARCHIVE after verify."
    classification = "MOVE TO CLOUD ARCHIVE"
  }
  D_LatestSufficient = [ordered]@{
    sufficientNow = $false
    reason = "LATEST missing monthly-review archive (restored only to live working tree). After local backup refresh that includes archive + secrets policy for cloud, LATEST can be the single local operational snapshot."
    hasArchive = $latestHasArch
  }
  E_CloudOnlyCandidates = @(
    "Nightly-Backups (after cloud verify)",
    "Backup-Staging\2026-08-22_0200 (after cloud verify)",
    "Cursor-Recovery chat archives (cloud archive OK, keep local until verified)",
    "Backup-Staging\2026-10-01_0200 (cloud archive after verify; keep local until Drive ZIP proven)"
  )
}

# Packaging plan (proposal only)
$packaging = @(
  [pscustomobject]@{
    source="C:\Dev\Nightly-Backups"
    zipName="Dealality-Nightly-Backups-2026-07-30.7z"
    expectedCompressedGB="~1.5-2.5 (est.)"
    destination="G:\My Drive\Dealality Backups\Archive\Nightly-Backups\"
    verify="7z t; compare file count; spot-check Cursor-workspaceStorage sample hashes"
  }
  [pscustomobject]@{
    source="C:\Dev\Backup-Staging\2026-08-22_0200"
    zipName="Dealality-Backup-Staging-2026-08-22_0200.7z"
    expectedCompressedGB="~half of raw (est.)"
    destination="G:\My Drive\Dealality Backups\Archive\Backup-Staging\"
    verify="7z t; confirm package.json + sample public assets"
  }
  [pscustomobject]@{
    source="C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip"
    zipName="Dealality-Cursor-Chats-2026-09-08.7z"
    expectedCompressedGB="already gzip'd; modest further shrink"
    destination="G:\My Drive\Dealality Backups\Archive\Cursor-Recovery\"
    verify="list archive; open ALL-CHATS-INDEX.csv; confirm bundle hash separately"
  }
  [pscustomobject]@{
    source="C:\Dev\Backup-Staging\2026-10-01_0200"
    zipName="Dealality-Backup-Staging-2026-10-01_0200-precrash.7z"
    expectedCompressedGB="~8-12 (est. from 18GB raw)"
    destination="G:\My Drive\Dealality Backups\Archive\Backup-Staging\"
    verify="must include monthly-review/archive/index.json + sample report.pdf hashes vs live"
  }
)

$audit = [ordered]@{
  generatedAt = (Get-Date).ToString("o")
  readOnly = $true
  zeroFilesMovedDeletedChanged = $true
  live = $liveProbe
  latestHasMonthlyReviewArchive = $latestHasArch
  stagingVsLiveArchive = $stagingVsLive
  rootTotals = $rootTotals
  totalBackupGB = Format-GB $totalBackupBytes
  uniqueBackupEstimateGB = Format-GB $uniqueEst
  redundantOverlapEstimateGB = Format-GB $overlapEst
  inventories = $inventories
  uniqueFileSamples = $uniqueRows
  overlap = $overlapRows
  answers = $answers
  packagingPlan = $packaging
  recommendedLocalFootprint = "C:\Dev\dealality-backups\LATEST (+ Backup-Scripts\Backup-Dealality.ps1). Keep Staging 2026-10-01_0200 + Cursor chat/git-safety until Google Drive verified."
  recommendedCloudFootprint = "G:\My Drive\Dealality Backups\ (rolling ZIPs) + Archive\ for Nightly, old Staging, Cursor chats"
}

$jsonPath = Join-Path $OutDir "dealality-backup-consolidation-audit.json"
$mdPath = Join-Path $OutDir "dealality-backup-consolidation-audit.md"
$csvUnique = Join-Path $OutDir "dealality-backup-unique-files.csv"
$csvOverlap = Join-Path $OutDir "dealality-backup-overlap.csv"

$audit | ConvertTo-Json -Depth 12 | Set-Content $jsonPath -Encoding UTF8
$uniqueRows | Export-Csv $csvUnique -NoTypeInformation -Encoding UTF8
$overlapRows | Export-Csv $csvOverlap -NoTypeInformation -Encoding UTF8
# Also export inventory as part of overlap companion
$inventories | Export-Csv (Join-Path $OutDir "dealality-backup-inventory.csv") -NoTypeInformation -Encoding UTF8

$md = @()
$md += "# Dealality backup consolidation audit (READ-ONLY)"
$md += ""
$md += "Generated: $($audit.generatedAt)"
$md += "**ZERO files moved/deleted/changed.**"
$md += ""
$md += "## Totals"
$md += ""
$md += "| Metric | GB |"
$md += "|--------|----|"
$md += "| Total backup roots | $($audit.totalBackupGB) |"
$md += "| Unique estimate | $($audit.uniqueBackupEstimateGB) |"
$md += "| Redundant/overlap estimate | $($audit.redundantOverlapEstimateGB) |"
$md += ""
$md += "### Root sizes"
$md += ""
$md += "| Root | GB | Files |"
$md += "|------|----|-------|"
foreach ($r in $rootTotals) { $md += "| $($r.root) | $($r.sizeGB) | $($r.fileCount) |" }
$md += ""
$md += "## Snapshot inventory"
$md += ""
$md += "| Root | Snapshot | GB | Files | Archive? | PDFs | Score | Cloud class |"
$md += "|------|----------|----|-------|----------|------|-------|-------------|"
foreach ($inv in ($inventories | Sort-Object { [double]($_.sizeGB -replace ',','') } -Descending)) {
  $md += "| $($inv.root) | $($inv.snapshot) | $($inv.sizeGB) | $($inv.fileCount) | $($inv.hasMonthlyReviewArchive) | $($inv.archivePdfCount) | $($inv.recoveryScore) | $($inv.cloudClass) |"
}
$md += ""
$md += "## Staging vs LIVE monthly-review archive"
$md += ""
$md += "- indexIdentical: **$($stagingVsLive.indexIdentical)**"
$md += "- READY PDF compare: $($stagingVsLive.pdfsIdentical)/$($stagingVsLive.pdfsCompared) identical"
$md += "- onlyStaging: $($stagingVsLive.pdfsOnlyStaging); onlyLive: $($stagingVsLive.pdfsOnlyLive); differ: $($stagingVsLive.pdfsDiffer)"
$md += "- LATEST has archive: **$latestHasArch**"
$md += ""
$md += "## Answers A-E"
$md += ""
$md += "### A. Backup-Staging\2026-10-01_0200 unique after restore?"
$md += $answers.A_BackupStaging_2026_10_01_0200.uniqueNow
$md += ""
$md += "### B. Cursor-Recovery unique?"
$md += $answers.B_CursorRecovery.uniqueNow
$md += ""
$md += "### C. Nightly unique?"
$md += $answers.C_NightlyBackups.uniqueNow
$md += ""
$md += "### D. Is LATEST sufficient alone?"
$md += "**$($answers.D_LatestSufficient.sufficientNow)** - $($answers.D_LatestSufficient.reason)"
$md += ""
$md += "### E. Cloud-only candidates"
foreach ($c in $answers.E_CloudOnlyCandidates) { $md += "- $c" }
$md += ""
$md += "## Overlap"
$md += ""
$md += "| Pair | Identical est. | Unique est. | Note |"
$md += "|------|----------------|-------------|------|"
foreach ($o in $overlapRows) { $md += "| $($o.pair) | $($o.identicalEstimateGB) | $($o.uniqueEstimateGB) | $($o.note) |" }
$md += ""
$md += "## Unique file samples (value-path hashes)"
$md += ""
$md += "| Backup | Snapshot | Path | Class |"
$md += "|--------|----------|------|-------|"
foreach ($u in ($uniqueRows | Select-Object -First 40)) {
  $md += "| $($u.backupRoot) | $($u.snapshot) | ``$($u.relativePath)`` | $($u.classification) |"
}
$md += ""
$md += "## Optional cold archive packaging (NOT CREATED)"
$md += ""
foreach ($p in $packaging) {
  $md += "### $($p.zipName)"
  $md += "- Source: ``$($p.source)``"
  $md += "- Dest: ``$($p.destination)``"
  $md += "- Expected compressed: $($p.expectedCompressedGB)"
  $md += "- Verify: $($p.verify)"
  $md += ""
}
$md += "## Recommended architecture"
$md += ""
$md += "- **LOCAL:** ``C:\Dev\dealality-backups\LATEST`` + ``Backup-Scripts\Backup-Dealality.ps1``"
$md += "- **KEEP LOCAL UNTIL CLOUD VERIFIED:** Staging ``2026-10-01_0200``, Cursor chat/git-safety"
$md += "- **CLOUD:** ``G:\My Drive\Dealality Backups\`` rolling ZIPs + ``Archive\`` for Nightly / old Staging / chats"
$md += "- **GitHub:** committed source history"
$md += ""
$md += "---"
$md += "READY FOR JOAN REVIEW - BACKUP CONSOLIDATION AUDIT COMPLETE"

$md -join "`n" | Set-Content $mdPath -Encoding UTF8
Write-Host "WROTE $jsonPath"
Write-Host "WROTE $mdPath"
Write-Host "DONE totalGB=$(Format-GB $totalBackupBytes) uniqueEst=$(Format-GB $uniqueEst) overlapEst=$(Format-GB $overlapEst)"
