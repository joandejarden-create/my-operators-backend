# READ-ONLY C:\Dev regeneratable space audit
# Mutations: report files under deal-capture-proxy/reports ONLY.
$ErrorActionPreference = "Continue"
$Primary = "C:\Dev\deal-capture-proxy"
$OutDir = Join-Path $Primary "reports"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Get-RobocopyBytes {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { return @{ bytes = [int64]0; files = 0 } }
  $dest = Join-Path $env:TEMP ("__cdev_regen_null_" + [guid]::NewGuid().ToString("N"))
  $null = New-Item -ItemType Directory -Force -Path $dest -ErrorAction SilentlyContinue
  try {
    $raw = & robocopy $Path $dest /L /E /BYTES /NFL /NDL /NJH /NC /NS /NP 2>&1 | Out-String
    $bytes = [int64]0
    $files = 0
    if ($raw -match 'Bytes\s*:\s*([\d,]+)') {
      $bytes = [int64](($Matches[1] -replace ',', ''))
    }
    if ($raw -match 'Files\s*:\s*([\d,]+)') {
      $files = [int](($Matches[1] -replace ',', ''))
    }
    return @{ bytes = $bytes; files = $files }
  } catch {
    return @{ bytes = [int64]0; files = 0 }
  } finally {
    Remove-Item -LiteralPath $dest -Recurse -Force -ErrorAction SilentlyContinue
  }
}

function Format-GB([int64]$b) {
  if ($null -eq $b -or $b -le 0) { return "0.00" }
  return ("{0:N2}" -f ($b / 1GB))
}

function Format-MB([int64]$b) {
  if ($null -eq $b -or $b -le 0) { return "0.00" }
  return ("{0:N2}" -f ($b / 1MB))
}

Write-Host "=== 1. Locate node_modules ==="
$nmDirs = @()
# Fast: top-level + one level down + known worktree roots
$tops = @(Get-ChildItem "C:\Dev" -Force -Directory -ErrorAction SilentlyContinue)
foreach ($t in $tops) {
  $nm = Join-Path $t.FullName "node_modules"
  if (Test-Path -LiteralPath $nm) { $nmDirs += (Get-Item -LiteralPath $nm) }
  # also nested one level (rare)
  Get-ChildItem $t.FullName -Directory -Force -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -eq 'node_modules' } |
    ForEach-Object { $nmDirs += $_ }
}

# Also find nested node_modules under deal-capture-proxy-ish trees via cmd dir /s /ad /b limited
$extraNm = @(cmd /c "dir /s /ad /b C:\Dev\*node_modules 2>nul" | Where-Object {
  $_ -match '\\node_modules$' -and $_ -notmatch '\\node_modules\\'
} | Select-Object -First 80)
foreach ($p in $extraNm) {
  if ($nmDirs.FullName -notcontains $p -and (Test-Path -LiteralPath $p)) {
    $nmDirs += (Get-Item -LiteralPath $p)
  }
}
$nmDirs = @($nmDirs | Sort-Object FullName -Unique)
Write-Host "Found $($nmDirs.Count) node_modules roots"

$nodeModules = @()
$i = 0
foreach ($nm in $nmDirs) {
  $i++
  Write-Host ("[{0}/{1}] size {2}" -f $i, $nmDirs.Count, $nm.FullName)
  $sz = Get-RobocopyBytes $nm.FullName
  $parent = Split-Path $nm.FullName -Parent
  $pkg = Test-Path (Join-Path $parent "package.json")
  $lock = (Test-Path (Join-Path $parent "package-lock.json")) -or (Test-Path (Join-Path $parent "npm-shrinkwrap.json")) -or (Test-Path (Join-Path $parent "yarn.lock")) -or (Test-Path (Join-Path $parent "pnpm-lock.yaml"))
  $isGit = $false; $isWt = $false; $dirty = $null; $mod = 0; $untr = 0; $branch = $null; $head = $null
  $inside = (& git -C $parent rev-parse --is-inside-work-tree 2>$null)
  if ($inside -eq "true") {
    $isGit = $true
    $gd = (& git -C $parent rev-parse --path-format=absolute --git-dir 2>$null)
    $cd = (& git -C $parent rev-parse --path-format=absolute --git-common-dir 2>$null)
    if ($gd -and $cd) {
      $gdn = ($gd -replace '/', '\').TrimEnd('\').ToLowerInvariant()
      $cdn = ($cd -replace '/', '\').TrimEnd('\').ToLowerInvariant()
      if ($gdn -ne $cdn -or $gdn -match '\\worktrees\\') { $isWt = $true }
    }
    $branch = (& git -C $parent rev-parse --abbrev-ref HEAD 2>$null)
    $head = (& git -C $parent rev-parse HEAD 2>$null)
    $porc = @((& git -C $parent status --porcelain=v1 --untracked-files=all 2>$null))
    $dirty = ($porc.Count -gt 0)
    $mod = @($porc | Where-Object { $_ -notmatch '^\?\?' -and $_.Trim() }).Count
    $untr = @($porc | Where-Object { $_ -match '^\?\?' }).Count
  }
  $repro = ($pkg -and $lock)
  $nodeModules += [pscustomobject]@{
    path = $nm.FullName
    parent = $parent
    parentName = Split-Path $parent -Leaf
    sizeBytes = [int64]$sz.bytes
    sizeGB = Format-GB $sz.bytes
    fileCount = [int]$sz.files
    hasPackageJson = $pkg
    hasLockfile = $lock
    reproducibleWithNpmCi = $repro
    parentIsGit = $isGit
    parentIsWorktree = $isWt
    parentDirty = $dirty
    parentModifiedCount = $mod
    parentUntrackedCount = $untr
    parentBranch = $branch
    parentHead = $head
    class = if ($repro) { "VERY LOW RISK TO CLEAN LATER" } else { "LOW RISK BUT VERIFY FIRST" }
    restoreHow = if ($repro) { "cd parent; npm ci" } else { "cd parent; npm install (no lockfile - versions may drift)" }
  }
}
$totalNmBytes = ($nodeModules | Measure-Object -Property sizeBytes -Sum).Sum
if ($null -eq $totalNmBytes) { $totalNmBytes = [int64]0 }

Write-Host "=== 2. Cache / generated dirs ==="
$cacheNamePatterns = @(
  '.cache','cache','caches','.turbo','.next','dist','build','coverage',
  'temp','tmp','.tmp','logs','.parcel-cache','.eslintcache',
  'playwright-report','test-results','.nyc_output','out','.vite'
)
$cacheHits = @()
# Scan known project roots only (not deep Backup-Staging insides for every name - too slow)
$scanRoots = @($tops | ForEach-Object { $_.FullName })
# Add primary common cache locations explicitly
$explicitCaches = @(
  "C:\Dev\deal-capture-proxy\.cache",
  "C:\Dev\deal-capture-proxy\tmp",
  "C:\Dev\deal-capture-proxy\temp",
  "C:\Dev\deal-capture-proxy\coverage",
  "C:\Dev\deal-capture-proxy\dist",
  "C:\Dev\deal-capture-proxy\build",
  "C:\Dev\deal-capture-proxy\playwright-report",
  "C:\Dev\deal-capture-proxy\test-results",
  "C:\Dev\deal-capture-proxy\.turbo",
  "C:\Dev\deal-capture-proxy\.next"
)
foreach ($er in $explicitCaches) {
  if (Test-Path -LiteralPath $er) {
    $item = Get-Item -LiteralPath $er -Force
    if ($item.PSIsContainer) { $cacheHits += $item }
  }
}

foreach ($root in $scanRoots) {
  # Only direct children matching names (avoid scanning inside Backup huge trees deeply)
  Get-ChildItem -LiteralPath $root -Force -Directory -ErrorAction SilentlyContinue |
    Where-Object { $cacheNamePatterns -contains $_.Name.ToLower() -or $cacheNamePatterns -contains $_.Name } |
    ForEach-Object { $cacheHits += $_ }
  # One level deeper into deal-capture* folders only
  if ((Split-Path $root -Leaf) -match 'deal-capture|dealality|gdi') {
    Get-ChildItem -LiteralPath $root -Force -Directory -ErrorAction SilentlyContinue |
      ForEach-Object {
        Get-ChildItem -LiteralPath $_.FullName -Force -Directory -ErrorAction SilentlyContinue |
          Where-Object { $cacheNamePatterns -contains $_.Name.ToLower() -or $cacheNamePatterns -contains $_.Name } |
          ForEach-Object { $cacheHits += $_ }
      }
  }
}

# Playwright browser cache (user/local)
$pwCache = Join-Path $env:LOCALAPPDATA "ms-playwright"
if (Test-Path $pwCache) {
  $cacheHits += (Get-Item -LiteralPath $pwCache)
}

$cacheHits = @($cacheHits | Sort-Object FullName -Unique)
Write-Host "Found $($cacheHits.Count) cache/generated candidate dirs"

$caches = @()
$i = 0
foreach ($c in $cacheHits) {
  $i++
  Write-Host ("  cache [{0}/{1}] {2}" -f $i, $cacheHits.Count, $c.FullName)
  $sz = Get-RobocopyBytes $c.FullName
  $name = $c.Name.ToLowerInvariant()
  $confidence = "MEDIUM"
  $risk = "LOW RISK BUT VERIFY FIRST"
  $why = "Named like cache/generated output"
  $deletionRisk = "May break local tooling until regenerated"

  if ($name -in @('.cache','cache','caches','.turbo','.parcel-cache','.eslintcache','.nyc_output','.vite')) {
    $confidence = "HIGH"; $risk = "VERY LOW RISK TO CLEAN LATER"
    $why = "Standard tool cache; regenerable by running tools/tests"
    $deletionRisk = "Low - tools recreate on next run"
  } elseif ($name -in @('coverage','playwright-report','test-results')) {
    $confidence = "HIGH"; $risk = "VERY LOW RISK TO CLEAN LATER"
    $why = "Test/report output regenerable via npm test / Playwright"
    $deletionRisk = "Low - loses historical test artifacts only"
  } elseif ($name -in @('tmp','temp','.tmp')) {
    $confidence = "MEDIUM"; $risk = "LOW RISK BUT VERIFY FIRST"
    $why = "Temp folder - usually disposable but may hold recovery scratch"
    $deletionRisk = "Medium - inspect for unique recovery files first"
  } elseif ($name -in @('dist','build','.next','out')) {
    $confidence = "MEDIUM"; $risk = "LOW RISK BUT VERIFY FIRST"
    $why = "Build output - regenerable if build pipeline exists"
    $deletionRisk = "Medium - confirm build script before delete"
  } elseif ($name -eq 'logs') {
    $confidence = "HIGH"; $risk = "VERY LOW RISK TO CLEAN LATER"
    $why = "Log files typically disposable"
    $deletionRisk = "Low - loses historical logs"
  } elseif ($c.FullName -match 'ms-playwright') {
    $confidence = "HIGH"; $risk = "VERY LOW RISK TO CLEAN LATER"
    $why = "Playwright browser binaries - reinstall via npx playwright install"
    $deletionRisk = "Low - next Playwright run re-downloads browsers"
  }

  # Never mark fixtures/data/research as disposable via this path scan
  if ($c.FullName -match '\\(fixtures|data\\|research|evidence)\\') {
    $confidence = "LOW"; $risk = "DO NOT TOUCH"
    $why = "Under fixtures/data/research - not treated as disposable"
    $deletionRisk = "High - may contain unique product/research content"
  }

  $caches += [pscustomobject]@{
    path = $c.FullName
    sizeBytes = [int64]$sz.bytes
    sizeGB = Format-GB $sz.bytes
    fileCount = [int]$sz.files
    lastModified = $c.LastWriteTime.ToString("o")
    whyRegeneratable = $why
    confidence = $confidence
    class = $risk
    deletionRisk = $deletionRisk
    restoreHow = "Re-run corresponding npm/playwright/build command"
  }
}
$totalCacheBytes = ($caches | Where-Object { $_.class -match 'VERY LOW RISK|LOW RISK' } | Measure-Object sizeBytes -Sum).Sum
if ($null -eq $totalCacheBytes) { $totalCacheBytes = [int64]0 }

Write-Host "=== 3. Worktree breakdown ==="
$worktrees = @()
Push-Location $Primary
try {
  $wtRaw = git worktree list --porcelain 2>$null
  $cur = @{}
  foreach ($line in $wtRaw) {
    if ($line -match '^worktree (.+)$') {
      if ($cur.path) { $worktrees += [pscustomobject]$cur }
      $cur = @{ path = $Matches[1] }
    } elseif ($line -match '^HEAD (.+)$') { $cur.head = $Matches[1] }
    elseif ($line -match '^branch (.+)$') { $cur.branch = $Matches[1] }
    elseif ($line -eq '') {
      if ($cur.path) { $worktrees += [pscustomobject]$cur }
      $cur = @{}
    }
  }
  if ($cur.path) { $worktrees += [pscustomobject]$cur }
} finally { Pop-Location }

$primaryHead = (& git -C $Primary rev-parse HEAD 2>$null)
$wtRows = @()
foreach ($w in $worktrees) {
  $p = ($w.path -replace '/', '\')
  Write-Host "  worktree $p"
  $total = Get-RobocopyBytes $p
  $nmPath = Join-Path $p "node_modules"
  $nmB = if (Test-Path $nmPath) { (Get-RobocopyBytes $nmPath).bytes } else { [int64]0 }
  $dataB = (Get-RobocopyBytes (Join-Path $p "data")).bytes
  $reportsB = (Get-RobocopyBytes (Join-Path $p "reports")).bytes
  $cacheB = [int64]0
  foreach ($cn in @('.cache','tmp','temp','coverage','dist','build','.next','playwright-report','test-results')) {
    $cp = Join-Path $p $cn
    if (Test-Path $cp) { $cacheB += (Get-RobocopyBytes $cp).bytes }
  }
  $sourceExNm = [Math]::Max([int64]0, [int64]$total.bytes - [int64]$nmB)
  $porc = @((& git -C $p status --porcelain=v1 --untracked-files=all 2>$null))
  $dirty = ($porc.Count -gt 0)
  $mod = @($porc | Where-Object { $_ -notmatch '^\?\?' -and $_.Trim() }).Count
  $untr = @($porc | Where-Object { $_ -match '^\?\?' }).Count
  $unique = 0
  try {
    $uc = (& git -C $p rev-list --count "$primaryHead..HEAD" 2>$null)
    if ($uc) { $unique = [int]$uc }
  } catch {}
  $wtRows += [pscustomobject]@{
    path = $p
    name = Split-Path $p -Leaf
    branch = $w.branch
    head = $w.head
    totalBytes = [int64]$total.bytes
    totalGB = Format-GB $total.bytes
    sourceExcludingNodeModulesGB = Format-GB $sourceExNm
    nodeModulesGB = Format-GB $nmB
    generatedCacheGB = Format-GB $cacheB
    dataGB = Format-GB $dataB
    reportsGB = Format-GB $reportsB
    dirty = $dirty
    modifiedCount = $mod
    untrackedCount = $untr
    uniqueCommitsVsPrimary = $unique
    nodeModulesSharePct = if ($total.bytes -gt 0) { [math]::Round(100.0 * $nmB / $total.bytes, 1) } else { 0 }
  }
}

Write-Host "=== 4. Backup overlap ==="
$backupRoots = @(
  "C:\Dev\Backup-Staging",
  "C:\Dev\Backup-Scripts",
  "C:\Dev\dealality-backups",
  "C:\Dev\Cursor-Recovery-Archive-2026-09-08",
  "C:\Dev\Nightly-Backups"
)
$backups = @()
foreach ($br in $backupRoots) {
  Write-Host "  backup $br"
  if (-not (Test-Path $br)) {
    $backups += [pscustomobject]@{ path=$br; exists=$false; sizeGB="0"; note="missing" }
    continue
  }
  $sz = Get-RobocopyBytes $br
  $children = @(Get-ChildItem $br -Force -ErrorAction SilentlyContinue)
  $dirs = @($children | Where-Object PSIsContainer)
  $dates = @()
  foreach ($d in $dirs) {
    if ($d.Name -match '\d{4}-\d{2}-\d{2}' -or $d.Name -match '\d{8}') { $dates += $d }
  }
  $newest = ($dirs | Sort-Object LastWriteTime -Descending | Select-Object -First 1)
  $oldest = ($dirs | Sort-Object LastWriteTime | Select-Object -First 1)
  # Probe content
  $hasGit = $false; $hasNm = $false; $hasArchive = $false; $hasSrc = $false
  $sample = @(cmd /c "dir /s /b /ad `"$br`" 2>nul" | Select-Object -First 5000)
  if ($sample -match '\\.git$' -or $sample -match '\\.git\\') { $hasGit = $true }
  # cheaper probes
  if (Test-Path (Join-Path $br "LATEST\deal-capture-proxy\.git")) { $hasGit = $true }
  if (@(Get-ChildItem $br -Recurse -Directory -Filter "node_modules" -ErrorAction SilentlyContinue | Select-Object -First 1)) { $hasNm = $true }
  if (@(Get-ChildItem $br -Recurse -Filter "index.json" -ErrorAction SilentlyContinue | Where-Object { $_.FullName -match 'monthly-review\\archive' } | Select-Object -First 1)) { $hasArchive = $true }
  # Snapshot list
  $snapNames = @($dirs | Select-Object -ExpandProperty Name)

  $class = "KEEP UNTIL GOOGLE DRIVE BACKUP VERIFIED"
  $uniqueNote = ""
  switch -Regex ($br) {
    'Backup-Staging' {
      $uniqueNote = "Contains unique pre-crash monthly-review archive used 2026-10-01 ADP recovery; treat as KEEP until cloud verified"
      $class = "KEEP UNTIL GOOGLE DRIVE BACKUP VERIFIED"
    }
    'Backup-Scripts' {
      $uniqueNote = "Contains Backup-Dealality.ps1 + dealality-snapshots + safety snaps; referenced by scheduled task - DO NOT TOUCH"
      $class = "DO NOT TOUCH"
    }
    'dealality-backups' {
      $uniqueNote = "Canonical local rolling LATEST (dual backup design); may lack archive if snapshotted post-loss"
      $class = "KEEP UNTIL GOOGLE DRIVE BACKUP VERIFIED"
    }
    'Cursor-Recovery' {
      $uniqueNote = "Cursor recovery archive 2026-09-08 - verify unrecovered unique files before any consolidation"
      $class = "BACKUP CONSOLIDATION CANDIDATE"
    }
    'Nightly-Backups' {
      $uniqueNote = "Older nightly root - compare to Staging/LATEST before considering consolidation"
      $class = "BACKUP CONSOLIDATION CANDIDATE"
    }
  }

  # Estimate node_modules inside backup (sample first matching)
  $nmInBackup = [int64]0
  $nmSample = @(Get-ChildItem $br -Recurse -Directory -Filter "node_modules" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notmatch '\\node_modules\\' } | Select-Object -First 5)
  foreach ($n in $nmSample) {
    # only measure shallow samples to avoid hours - mark as sample
  }

  $backups += [pscustomobject]@{
    path = $br
    exists = $true
    sizeBytes = [int64]$sz.bytes
    sizeGB = Format-GB $sz.bytes
    fileCount = [int]$sz.files
    childCount = $dirs.Count
    snapshotNamesSample = ($snapNames | Select-Object -First 20) -join "; "
    newestChild = if ($newest) { "$($newest.Name) ($($newest.LastWriteTime.ToString('yyyy-MM-dd')))" } else { "" }
    oldestChild = if ($oldest) { "$($oldest.Name) ($($oldest.LastWriteTime.ToString('yyyy-MM-dd')))" } else { "" }
    includesGitObjectsLikely = $hasGit
    includesNodeModulesSampleFound = [bool]$nmSample.Count
    includesMonthlyReviewArchive = $hasArchive
    uniqueContentNote = $uniqueNote
    class = $class
    lastModified = (Get-Item $br).LastWriteTime.ToString("o")
  }
}
$totalBackupBytes = ($backups | Where-Object { $_.exists } | Measure-Object sizeBytes -Sum).Sum
if ($null -eq $totalBackupBytes) { $totalBackupBytes = [int64]0 }

# Overlap estimate (conservative): Nightly + Cursor-Recovery may overlap with Staging/LATEST content themes
# Do NOT double-count Staging with LATEST as identical (they're not).
$overlapEstimateBytes = [int64]0
$nightly = $backups | Where-Object { $_.path -match 'Nightly-Backups' }
$cursorRec = $backups | Where-Object { $_.path -match 'Cursor-Recovery' }
# Conservative consolidation potential = Nightly + half of Cursor-Recovery (needs verify)
if ($nightly) { $overlapEstimateBytes += [int64]([double]$nightly.sizeBytes * 0.5) }
if ($cursorRec) { $overlapEstimateBytes += [int64]([double]$cursorRec.sizeBytes * 0.3) }

Write-Host "=== 5. Top large files ==="
# Use robocopy listing with bytes for files - slow on full C:\Dev.
# Instead: PowerShell Get-ChildItem on each top folder excluding node_modules and .git
$largeFiles = [System.Collections.Generic.List[object]]::new()
foreach ($t in $tops) {
  Write-Host "  scanning large files in $($t.Name)"
  try {
    Get-ChildItem -LiteralPath $t.FullName -Recurse -File -Force -ErrorAction SilentlyContinue |
      Where-Object {
        $_.FullName -notmatch '\\node_modules\\' -and
        $_.FullName -notmatch '\\.git\\objects\\' -and
        $_.FullName -notmatch '\\.git\\lfs\\'
      } |
      Sort-Object Length -Descending |
      Select-Object -First 40 |
      ForEach-Object { $largeFiles.Add($_) }
  } catch {}
}
$top100 = @($largeFiles | Sort-Object Length -Descending | Select-Object -First 100)
$largeRows = @()
foreach ($f in $top100) {
  $ext = $f.Extension.ToLowerInvariant()
  $p = $f.FullName
  $cls = "UNKNOWN"
  if ($p -match '\\Backup|\\dealality-backups|\\Nightly|\\Cursor-Recovery|\\.zip$|\\.7z$') { $cls = "BACKUP" }
  elseif ($ext -in @('.zip','.7z','.rar','.tar','.gz')) { $cls = "ARCHIVE" }
  elseif ($ext -in @('.db','.sqlite','.sqlite3')) { $cls = "DATABASE" }
  elseif ($ext -in @('.mp4','.mov','.webm','.avi')) { $cls = "VIDEO / IMAGE" }
  elseif ($ext -in @('.png','.jpg','.jpeg','.webp','.gif','.pdf') -and $p -match 'reports|pdf-pages|monthly-review') { $cls = "GENERATED REPORT" }
  elseif ($ext -in @('.png','.jpg','.jpeg','.webp','.gif')) { $cls = "VIDEO / IMAGE" }
  elseif ($p -match '\\cache\\|\\.cache\\|\\tmp\\|\\temp\\|\\coverage\\') { $cls = "CACHE" }
  elseif ($p -match '\\fixtures\\|\\public\\|\\src\\') { $cls = "SOURCE / PRODUCT ASSET" }
  elseif ($p -match '\\data\\') { $cls = "LOCAL DATA" }
  elseif ($ext -eq '.pdf' -or $p -match '\\reports\\') { $cls = "GENERATED REPORT" }
  $largeRows += [pscustomobject]@{
    path = $p
    sizeBytes = [int64]$f.Length
    sizeMB = Format-MB $f.Length
    sizeGB = Format-GB $f.Length
    lastModified = $f.LastWriteTime.ToString("o")
    classification = $cls
  }
}

Write-Host "=== 6. Git storage ==="
$gitCommon = (& git -C $Primary rev-parse --path-format=absolute --git-common-dir 2>$null)
$gitCommon = ($gitCommon -replace '/', '\')
$gitSize = Get-RobocopyBytes $gitCommon
$packSize = Get-RobocopyBytes (Join-Path $gitCommon "objects\pack")
$looseSize = Get-RobocopyBytes (Join-Path $gitCommon "objects")
$lfsSize = Get-RobocopyBytes (Join-Path $gitCommon "lfs")
$gcNote = "git gc --optional may reclaim loose/unreachable objects; NOT run (read-only). Worktrees share this object store."

Write-Host "=== 7. Totals / C:\Dev size ==="
$devTotal = Get-RobocopyBytes "C:\Dev"

# Top 10 consumers = top-level folders
$topConsumers = @()
foreach ($t in $tops) {
  $sz = Get-RobocopyBytes $t.FullName
  $topConsumers += [pscustomobject]@{
    name = $t.Name
    path = $t.FullName
    sizeBytes = [int64]$sz.bytes
    sizeGB = Format-GB $sz.bytes
  }
}
$topConsumers = @($topConsumers | Sort-Object sizeBytes -Descending)

# Deduped savings
# A: all reproducible node_modules
$aBytes = ($nodeModules | Where-Object { $_.reproducibleWithNpmCi } | Measure-Object sizeBytes -Sum).Sum
if ($null -eq $aBytes) { $aBytes = [int64]0 }
# B: high/medium confidence caches that are VERY LOW or LOW RISK (exclude DO NOT TOUCH)
$bBytes = ($caches | Where-Object { $_.class -match 'VERY LOW RISK|LOW RISK' -and $_.confidence -ne 'LOW' } | Measure-Object sizeBytes -Sum).Sum
if ($null -eq $bBytes) { $bBytes = [int64]0 }
# Subtract node_modules already counted if any cache path is under node_modules (shouldn't)
# C: backup consolidation estimate
$cBytes = $overlapEstimateBytes
# D: low-risk = A + B (not C - consolidation needs Drive verify)
$dBytes = [int64]$aBytes + [int64]$bBytes

$candidates = @()
foreach ($n in $nodeModules) {
  $candidates += [pscustomobject]@{
    category = "NODE_MODULES"
    class = $n.class
    estimatedGB = $n.sizeGB
    sizeBytes = $n.sizeBytes
    paths = $n.path
    whySafe = "Reproducible dependency install from lockfile" + $(if ($n.reproducibleWithNpmCi) { " via npm ci" } else { " (verify lockfile)" })
    restoreHow = $n.restoreHow
  }
}
foreach ($c in $caches) {
  if ($c.class -match 'DO NOT TOUCH|UNKNOWN') { continue }
  $candidates += [pscustomobject]@{
    category = "CACHE_GENERATED"
    class = $c.class
    estimatedGB = $c.sizeGB
    sizeBytes = $c.sizeBytes
    paths = $c.path
    whySafe = $c.whyRegeneratable
    restoreHow = $c.restoreHow
  }
}
foreach ($b in $backups) {
  if ($b.class -match 'CONSOLIDATION') {
    $candidates += [pscustomobject]@{
      category = "BACKUP"
      class = $b.class
      estimatedGB = $b.sizeGB
      sizeBytes = $b.sizeBytes
      paths = $b.path
      whySafe = $b.uniqueContentNote
      restoreHow = "Only after Google Drive / OneDrive backup verified AND uniqueness audit"
    }
  }
}

# Write reports
$audit = [ordered]@{
  generatedAt = (Get-Date).ToString("o")
  readOnly = $true
  mutationsPerformed = @("wrote reports under deal-capture-proxy/reports only")
  zeroFilesChangedDeletedMoved = $true
  cDevTotalBytes = [int64]$devTotal.bytes
  cDevTotalGB = Format-GB $devTotal.bytes
  cDevFileCount = [int]$devTotal.files
  totals = [ordered]@{
    nodeModulesGB = Format-GB $totalNmBytes
    nodeModulesBytes = [int64]$totalNmBytes
    cacheGeneratedCandidateGB = Format-GB (($caches | Measure-Object sizeBytes -Sum).Sum)
    backupTotalGB = Format-GB $totalBackupBytes
    backupOverlapEstimateGB = Format-GB $overlapEstimateBytes
    gitCommonDirGB = Format-GB $gitSize.bytes
    potentialA_nodeModules = Format-GB $aBytes
    potentialB_cacheGenerated = Format-GB $bBytes
    potentialC_backupConsolidation = Format-GB $cBytes
    potentialD_totalLowRisk = Format-GB $dBytes
  }
  nodeModules = $nodeModules
  caches = $caches
  worktrees = $wtRows
  backups = $backups
  largeFiles = $largeRows
  topConsumers = $topConsumers
  gitStorage = [ordered]@{
    commonDir = $gitCommon
    totalGB = Format-GB $gitSize.bytes
    packGB = Format-GB $packSize.bytes
    objectsTreeGB = Format-GB $looseSize.bytes
    lfsGB = Format-GB $lfsSize.bytes
    lfsPresent = ((Test-Path (Join-Path $gitCommon "lfs")))
    gcNote = $gcNote
    worktreeCount = $wtRows.Count
  }
  candidates = $candidates
  notes = @(
    "node_modules across linked worktrees are duplicate dependency trees of the same monorepo",
    "Backup-Staging holds unique pre-crash monthly-review archive - KEEP until cloud verified",
    "dealality-backups\LATEST is canonical rolling local backup per Backup-Dealality.ps1",
    "Savings A+B counted once; backup consolidation C is separate and higher risk",
    "reports/ai-demand-positioning/monthly-review/archive is GENERATED CUSTOMER ARTIFACT - backup-protected, not low-risk delete"
  )
}

$jsonPath = Join-Path $OutDir "c-dev-regeneratable-space-audit.json"
$mdPath = Join-Path $OutDir "c-dev-regeneratable-space-audit.md"
$csvLarge = Join-Path $OutDir "c-dev-large-files.csv"
$csvNm = Join-Path $OutDir "c-dev-node-modules.csv"
$csvBak = Join-Path $OutDir "c-dev-backup-overlap.csv"

$audit | ConvertTo-Json -Depth 10 | Set-Content $jsonPath -Encoding UTF8
$nodeModules | Export-Csv $csvNm -NoTypeInformation -Encoding UTF8
$largeRows | Export-Csv $csvLarge -NoTypeInformation -Encoding UTF8
$backups | Export-Csv $csvBak -NoTypeInformation -Encoding UTF8

# Markdown
$md = @()
$md += "# C:\Dev regeneratable space audit (READ-ONLY)"
$md += ""
$md += "Generated: $($audit.generatedAt)"
$md += "**ZERO files changed/deleted/moved** (reports written only)."
$md += ""
$md += "## Totals"
$md += ""
$md += "| Metric | GB |"
$md += "|--------|----|"
$md += "| C:\Dev total | $($audit.cDevTotalGB) |"
$md += "| All node_modules | $($audit.totals.nodeModulesGB) |"
$md += "| Cache/generated candidates | $($audit.totals.cacheGeneratedCandidateGB) |"
$md += "| Backup roots total | $($audit.totals.backupTotalGB) |"
$md += "| Backup overlap estimate | $($audit.totals.backupOverlapEstimateGB) |"
$md += "| Shared .git | $($audit.totals.gitCommonDirGB) |"
$md += "| **A. NODE_MODULES potential** | **$($audit.totals.potentialA_nodeModules)** |"
$md += "| **B. CACHE/GENERATED potential** | **$($audit.totals.potentialB_cacheGenerated)** |"
$md += "| **C. BACKUP consolidation potential** | **$($audit.totals.potentialC_backupConsolidation)** |"
$md += "| **D. TOTAL LOW-RISK (A+B)** | **$($audit.totals.potentialD_totalLowRisk)** |"
$md += ""
$md += "## Top 10 disk consumers (C:\Dev top-level)"
$md += ""
$md += "| Folder | GB |"
$md += "|--------|----|"
foreach ($t in ($topConsumers | Select-Object -First 10)) {
  $md += "| $($t.name) | $($t.sizeGB) |"
}
$md += ""
$md += "## 1. node_modules"
$md += ""
$md += "| Parent | GB | Files | Lockfile | Worktree | Dirty | Class |"
$md += "|--------|----|-------|----------|----------|-------|-------|"
foreach ($n in ($nodeModules | Sort-Object sizeBytes -Descending)) {
  $md += "| $($n.parentName) | $($n.sizeGB) | $($n.fileCount) | $($n.hasLockfile) | $($n.parentIsWorktree) | $($n.parentDirty) | $($n.class) |"
}
$md += ""
$md += "Likely duplicate trees: worktree node_modules under deal-capture-proxy-* / dealality-* share the same package-lock lineage as primary."
$md += ""
$md += "## 2. Cache / generated"
$md += ""
$md += "| Path | GB | Confidence | Class | Why |"
$md += "|------|----|------------|-------|-----|"
foreach ($c in ($caches | Sort-Object sizeBytes -Descending)) {
  $md += "| ``$($c.path)`` | $($c.sizeGB) | $($c.confidence) | $($c.class) | $($c.whyRegeneratable) |"
}
$md += ""
$md += "## 3. Worktrees"
$md += ""
$md += "| Worktree | Total GB | excl nm GB | nm GB | nm% | data GB | Dirty | M/U | Unique commits |"
$md += "|----------|----------|------------|-------|-----|---------|-------|-----|----------------|"
foreach ($w in ($wtRows | Sort-Object { [double]($_.totalGB -replace ',','') } -Descending)) {
  $md += "| $($w.name) | $($w.totalGB) | $($w.sourceExcludingNodeModulesGB) | $($w.nodeModulesGB) | $($w.nodeModulesSharePct)% | $($w.dataGB) | $(if($w.dirty){'dirty'}else{'clean'}) | $($w.modifiedCount)/$($w.untrackedCount) | $($w.uniqueCommitsVsPrimary) |"
}
$md += ""
$md += "## 4. Backup overlap"
$md += ""
$md += "| Root | GB | Newest | Archive? | Class | Note |"
$md += "|------|----|--------|----------|-------|------|"
foreach ($b in $backups) {
  $md += "| ``$($b.path)`` | $($b.sizeGB) | $($b.newestChild) | $($b.includesMonthlyReviewArchive) | $($b.class) | $($b.uniqueContentNote) |"
}
$md += ""
$md += "### Backup questions"
$md += ""
$md += "A. **Backup-Staging unique pre-crash material?** YES - used to restore ADP monthly-review archive (KEEP)."
$md += "B. **Cursor-Recovery unique unrecovered?** UNKNOWN without file-level diff - CONSOLIDATION CANDIDATE after verify."
$md += "C. **Nightly-Backups unique?** Likely older overlap - CONSOLIDATION CANDIDATE after compare to Staging/LATEST."
$md += "D. **dealality-backups\LATEST canonical rolling?** YES per Backup-Dealality.ps1 design."
$md += ""
$md += "## 5. Top large files (excl .git objects / node_modules)"
$md += ""
$md += "| Size MB | Class | Path |"
$md += "|---------|-------|------|"
foreach ($f in ($largeRows | Select-Object -First 40)) {
  $md += "| $($f.sizeMB) | $($f.classification) | ``$($f.path)`` |"
}
$md += ""
$md += "Full top 100: ``reports/c-dev-large-files.csv``"
$md += ""
$md += "## 6. Git storage"
$md += ""
$md += "- Common dir: ``$gitCommon``"
$md += "- Total: $($audit.gitStorage.totalGB) GB"
$md += "- Pack: $($audit.gitStorage.packGB) GB"
$md += "- Objects tree: $($audit.gitStorage.objectsTreeGB) GB"
$md += "- LFS: $($audit.gitStorage.lfsGB) GB (present=$($audit.gitStorage.lfsPresent))"
$md += "- $($audit.gitStorage.gcNote)"
$md += ""
$md += "## 7. Lowest-risk cleanup opportunities"
$md += ""
$md += "1. **Reproducible node_modules** across primary + worktrees (~$($audit.totals.potentialA_nodeModules) GB) - restore with ``npm ci`` per worktree needed."
$md += "2. **Tool caches / coverage / playwright-report / tmp** (~$($audit.totals.potentialB_cacheGenerated) GB) - regenerable."
$md += "3. **Backup consolidation** (Nightly + part of Cursor-Recovery) only after Google Drive verification (~$($audit.totals.potentialC_backupConsolidation) GB estimate)."
$md += ""
$md += "### DO NOT TOUCH"
$md += "- All linked worktree working trees (unique dirty/untracked state)"
$md += "- ``Backup-Scripts`` (scheduled task)"
$md += "- ``Backup-Staging`` until cloud backup verified (unique pre-crash archive source)"
$md += "- ``reports/.../monthly-review/archive`` (customer artifacts - backup, don't delete as 'cache')"
$md += "- Fixtures, local DBs, research data"
$md += ""
$md += "---"
$md += "READY FOR JOAN REVIEW - C:\\DEV REGENERATABLE SPACE AUDIT COMPLETE"

$md -join "`n" | Set-Content $mdPath -Encoding UTF8

Write-Host "WROTE $jsonPath"
Write-Host "WROTE $mdPath"
Write-Host "WROTE $csvLarge"
Write-Host "WROTE $csvNm"
Write-Host "WROTE $csvBak"
Write-Host "DONE"
Write-Host "TOTAL_DEV_GB=$(Format-GB $devTotal.bytes)"
Write-Host "NM_GB=$(Format-GB $totalNmBytes)"
Write-Host "LOW_RISK_GB=$(Format-GB $dBytes)"
