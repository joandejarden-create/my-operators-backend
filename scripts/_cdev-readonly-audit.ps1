# READ-ONLY C:\Dev duplicate/overlap audit
# Mutations: writes report files under deal-capture-proxy/reports only.
$ErrorActionPreference = "Continue"
$Primary = "C:\Dev\deal-capture-proxy"
$OutDir = Join-Path $Primary "reports"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Get-RobocopyBytes {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { return @{ bytes = [int64]0; files = 0 } }
  $dest = Join-Path $env:TEMP ("__cdev_audit_null_" + [guid]::NewGuid().ToString("N"))
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
  if ($b -le 0) { return "0" }
  return ("{0:N2}" -f ($b / 1GB))
}

function Get-GitInfo {
  param([string]$Path)
  $info = [ordered]@{
    isGit = $false
    isWorktree = $false
    mainWorktree = $null
    remote = $null
    branch = $null
    head = $null
    dirty = $null
    modifiedCount = 0
    untrackedCount = 0
    ignoredCount = $null
    trackedCount = 0
  }
  # Prefer git CLI (PowerShell Get-Item often fails on .git under Cursor sandbox)
  $inside = (& git -C $Path rev-parse --is-inside-work-tree 2>$null)
  if ($inside -ne "true") { return $info }
  $info.isGit = $true
  try {
    $gitDir = (& git -C $Path rev-parse --path-format=absolute --git-dir 2>$null)
    $common = (& git -C $Path rev-parse --path-format=absolute --git-common-dir 2>$null)
    if ($gitDir -and $common) {
      $gdNorm = ($gitDir -replace '/', '\').TrimEnd('\').ToLowerInvariant()
      $cdNorm = ($common -replace '/', '\').TrimEnd('\').ToLowerInvariant()
      if ($gdNorm -ne $cdNorm -or $gdNorm -match '\\worktrees\\') {
        $info.isWorktree = $true
        $info.mainWorktree = (Split-Path (($common -replace '/', '\').TrimEnd('\')) -Parent)
      }
    }
  } catch {}
  try {
    $info.remote = (& git -C $Path remote get-url origin 2>$null)
    $info.branch = (& git -C $Path rev-parse --abbrev-ref HEAD 2>$null)
    $info.head = (& git -C $Path rev-parse HEAD 2>$null)
    $info.trackedCount = @((& git -C $Path ls-files 2>$null)).Count
    $porc = @((& git -C $Path status --porcelain=v1 --untracked-files=all 2>$null))
    $info.dirty = ($porc.Count -gt 0)
    $info.modifiedCount = @($porc | Where-Object { $_ -notmatch '^\?\?' -and $_.Trim() }).Count
    $info.untrackedCount = @($porc | Where-Object { $_ -match '^\?\?' }).Count
  } catch {}
  return $info
}

function Compare-SourceTrees {
  param([string]$A, [string]$B)
  $dirs = @("api", "lib", "public", "scripts", "fixtures", "config")
  $roots = @("package.json", "server.js")
  $result = [ordered]@{
    identical = 0
    differ = 0
    onlyInA = 0
    onlyInB = 0
    newerInA = 0
    newerInB = 0
    sampleDiffer = @()
    sampleOnlyB = @()
  }
  $mapA = @{}
  $mapB = @{}
  foreach ($rel in $roots) {
    $pa = Join-Path $A $rel
    $pb = Join-Path $B $rel
    if (Test-Path -LiteralPath $pa) { $mapA[$rel] = Get-Item -LiteralPath $pa }
    if (Test-Path -LiteralPath $pb) { $mapB[$rel] = Get-Item -LiteralPath $pb }
  }
  foreach ($d in $dirs) {
    $da = Join-Path $A $d
    $db = Join-Path $B $d
    if (Test-Path -LiteralPath $da) {
      Get-ChildItem -LiteralPath $da -Recurse -File -Force -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -notmatch '\\node_modules\\|\\\.git\\' } |
        ForEach-Object {
          $rel = $_.FullName.Substring($A.Length).TrimStart('\')
          $mapA[$rel] = $_
        }
    }
    if (Test-Path -LiteralPath $db) {
      Get-ChildItem -LiteralPath $db -Recurse -File -Force -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -notmatch '\\node_modules\\|\\\.git\\' } |
        ForEach-Object {
          $rel = $_.FullName.Substring($B.Length).TrimStart('\')
          $mapB[$rel] = $_
        }
    }
  }
  $all = @($mapA.Keys + $mapB.Keys) | Select-Object -Unique
  foreach ($rel in $all) {
    $ha = $mapA.ContainsKey($rel)
    $hb = $mapB.ContainsKey($rel)
    if ($ha -and -not $hb) { $result.onlyInA++; continue }
    if ($hb -and -not $ha) {
      $result.onlyInB++
      if ($result.sampleOnlyB.Count -lt 25) { $result.sampleOnlyB += $rel }
      continue
    }
    try {
      $fa = $mapA[$rel]; $fb = $mapB[$rel]
      if ($fa.Length -eq $fb.Length) {
        $shaA = (Get-FileHash -LiteralPath $fa.FullName -Algorithm SHA256).Hash
        $shaB = (Get-FileHash -LiteralPath $fb.FullName -Algorithm SHA256).Hash
        if ($shaA -eq $shaB) { $result.identical++ }
        else {
          $result.differ++
          if ($result.sampleDiffer.Count -lt 25) { $result.sampleDiffer += $rel }
        }
      } else {
        $result.differ++
        if ($result.sampleDiffer.Count -lt 25) { $result.sampleDiffer += $rel }
      }
      if ($fa.LastWriteTimeUtc -gt $fb.LastWriteTimeUtc) { $result.newerInA++ }
      elseif ($fb.LastWriteTimeUtc -gt $fa.LastWriteTimeUtc) { $result.newerInB++ }
    } catch {
      $result.differ++
    }
  }
  return [pscustomobject]$result
}

Write-Host "Collecting top-level folders..."
$tops = @(Get-ChildItem "C:\Dev" -Force -ErrorAction SilentlyContinue | Where-Object { $_.PSIsContainer })

$primaryGit = Get-GitInfo $Primary
$primaryHead = $primaryGit.head

$folders = @()
$i = 0
foreach ($t in $tops) {
  $i++
  Write-Host ("[{0}/{1}] sizing+git {2}" -f $i, $tops.Count, $t.Name)
  $git = Get-GitInfo $t.FullName
  $sizeInfo = Get-RobocopyBytes $t.FullName
  $totalBytes = [int64]$sizeInfo.bytes
  $fileCount = [int]$sizeInfo.files

  Write-Host ("  breakdown for {0}" -f $t.Name)
  $breakdown = [ordered]@{
    node_modules_gb = Format-GB ((Get-RobocopyBytes (Join-Path $t.FullName "node_modules")).bytes)
    git_gb = Format-GB ((Get-RobocopyBytes (Join-Path $t.FullName ".git")).bytes)
    data_gb = Format-GB ((Get-RobocopyBytes (Join-Path $t.FullName "data")).bytes)
    public_gb = Format-GB ((Get-RobocopyBytes (Join-Path $t.FullName "public")).bytes)
    reports_gb = Format-GB ((Get-RobocopyBytes (Join-Path $t.FullName "reports")).bytes)
    fixtures_gb = Format-GB ((Get-RobocopyBytes (Join-Path $t.FullName "fixtures")).bytes)
    api_lib_gb = Format-GB (
      [int64]((Get-RobocopyBytes (Join-Path $t.FullName "api")).bytes) +
      [int64]((Get-RobocopyBytes (Join-Path $t.FullName "lib")).bytes) +
      [int64]((Get-RobocopyBytes (Join-Path $t.FullName "scripts")).bytes)
    )
  }

  $name = $t.Name
  $dealalityRelated = ($name -match 'deal|Dealality|gdi|Backup|backup|fairfield|Nightly|Staging|snapshot|census|Cursor-Recovery|_gdi') -or ($git.remote -match 'my-operators-backend|deal-capture')

  $uniqueCommits = 0
  $aheadOfPrimary = $null
  $mergeBase = $null
  if ($git.isGit -and $primaryHead -and $git.head) {
    Push-Location $Primary
    try {
      git cat-file -e "$($git.head)^{commit}" 2>$null | Out-Null
      $aheadOfPrimary = ($LASTEXITCODE -ne 0)
    } catch { $aheadOfPrimary = $true }
    finally { Pop-Location }

    if (-not $aheadOfPrimary) {
      Push-Location $t.FullName
      try {
        $mb = (git merge-base HEAD $primaryHead 2>$null)
        $mergeBase = $mb
        $uc = (git rev-list --count "$primaryHead..HEAD" 2>$null)
        if ($uc) { $uniqueCommits = [int]$uc } else { $uniqueCommits = 0 }
      } catch { $uniqueCommits = 0 }
      finally { Pop-Location }
    } else {
      $uniqueCommits = -1  # unknown / not in primary object DB
    }
  }

  $ignoredCount = $null
  if ($dealalityRelated -and $git.isGit -and -not $git.isWorktree) {
    Push-Location $t.FullName
    try {
      $ign = @(git status --porcelain=v1 --ignored --untracked-files=all 2>$null | Where-Object { $_ -match '^!!' })
      $ignoredCount = $ign.Count
    } catch {}
    finally { Pop-Location }
  }

  $folders += [pscustomobject]@{
    path = $t.FullName
    name = $t.Name
    totalBytes = $totalBytes
    totalGB = Format-GB $totalBytes
    fileCount = $fileCount
    lastModified = $t.LastWriteTime.ToString("o")
    dealalityRelated = [bool]$dealalityRelated
    isGit = $git.isGit
    isWorktree = $git.isWorktree
    mainWorktree = $git.mainWorktree
    remote = $git.remote
    branch = $git.branch
    head = $git.head
    dirty = $git.dirty
    modifiedCount = $git.modifiedCount
    untrackedCount = $git.untrackedCount
    ignoredCount = $ignoredCount
    trackedCount = $git.trackedCount
    uniqueCommitsVsPrimary = $uniqueCommits
    headNotInPrimary = $aheadOfPrimary
    mergeBase = $mergeBase
    breakdown = $breakdown
  }
}

# Worktree list from primary
$worktrees = @()
Push-Location $Primary
try {
  $wt = git worktree list --porcelain 2>$null
  $cur = @{}
  foreach ($line in $wt) {
    if ($line -match '^worktree (.+)$') {
      if ($cur.path) { $worktrees += [pscustomobject]$cur }
      $cur = @{ path = $Matches[1] }
    }
    elseif ($line -match '^HEAD (.+)$') { $cur.head = $Matches[1] }
    elseif ($line -match '^branch (.+)$') { $cur.branch = $Matches[1] }
    elseif ($line -eq '') {
      if ($cur.path) { $worktrees += [pscustomobject]$cur }
      $cur = @{}
    }
  }
  if ($cur.path) { $worktrees += [pscustomobject]$cur }
} catch {}
finally { Pop-Location }

# Hard-coded C:\Dev references
Write-Host "Scanning scripts for C:\Dev references..."
$refHits = @()
$searchRoots = @(
  "C:\Dev\Backup-Scripts",
  "C:\Dev\deal-capture-proxy\scripts",
  "C:\Dev\deal-capture-proxy\package.json",
  "C:\Dev\dealality-local-task-runner"
)
foreach ($sr in $searchRoots) {
  if (-not (Test-Path $sr)) { continue }
  $files = @()
  if ((Get-Item $sr) -is [System.IO.FileInfo]) { $files = @(Get-Item $sr) }
  else {
    $files = @(Get-ChildItem $sr -Recurse -Include *.ps1,*.mjs,*.js,*.cmd,*.bat,*.json,*.md -File -ErrorAction SilentlyContinue |
      Where-Object { $_.FullName -notmatch 'node_modules|\.git' } | Select-Object -First 600)
  }
  foreach ($f in $files) {
    try {
      $lines = Select-String -Path $f.FullName -Pattern 'C:\\Dev\\[^\s''"`\)]+' -ErrorAction SilentlyContinue
      foreach ($l in $lines) {
        $refHits += [pscustomobject]@{
          file = $f.FullName
          line = $l.LineNumber
          match = ($l.Matches | ForEach-Object { $_.Value }) -join "; "
        }
      }
    } catch {}
  }
}

# Folder names referenced by hits
$referencedFolders = @()
foreach ($h in $refHits) {
  if ($h.match -match 'C:\\Dev\\([^\\/\s]+)') {
    $referencedFolders += $Matches[1]
  }
}
$referencedFolders = @($referencedFolders | Select-Object -Unique | Sort-Object)

# Scheduled tasks
$tasks = @()
try {
  Get-ScheduledTask -ErrorAction SilentlyContinue | Where-Object { $_.TaskName -match 'Dealality|Backup|deal|Deal' } | ForEach-Object {
    $act = ($_.Actions | ForEach-Object { "$($_.Execute) $($_.Arguments)" }) -join " | "
    $tasks += [pscustomobject]@{ task = $_.TaskName; path = $_.TaskPath; actions = $act; state = [string]$_.State }
  }
} catch {}

function Classify-Folder($f) {
  $n = $f.name
  if ($f.path -eq $Primary) { return "A. ACTIVE PRIMARY REPO" }
  if ($f.isWorktree) { return "B. ACTIVE GIT WORKTREE" }
  if ($n -match 'Backup-Staging|dealality-backups|Nightly-Backups|Backup-Scripts|Cursor-Recovery|snapshot') {
    return "G. BACKUP / SNAPSHOT"
  }
  if ($n -eq 'data' -or $n -eq 'fixtures' -or $n -match '^_gdi') {
    return "H. GENERATED / CACHE"
  }
  if ($f.isGit -and $f.remote -match 'my-operators-backend') {
    if ($f.dirty -or $f.untrackedCount -gt 0) { return "F. UNIQUE UNCOMMITTED WORK" }
    if ($f.headNotInPrimary) { return "E. NEWER CLONE" }
    if ($f.uniqueCommitsVsPrimary -gt 0) { return "E. NEWER CLONE" }
    if ($f.head -and $primaryHead -and $f.head -ne $primaryHead) { return "D. OLDER CLONE" }
    if ($f.head -eq $primaryHead) { return "C. TRUE DUPLICATE" }
    return "D. OLDER CLONE"
  }
  if ($n -match 'deal-capture-proxy|dealality|Dealality|gdi-') {
    if (-not $f.isGit) { return "G. BACKUP / SNAPSHOT" }
    return "I. UNKNOWN - DO NOT TOUCH"
  }
  return "I. UNKNOWN - DO NOT TOUCH"
}

function Recommend-Folder($f, $class) {
  $n = $f.name
  if ($f.path -eq $Primary) {
    return @{ rec = "KEEP - ACTIVE"; doNotTouch = $true; reasons = @("primary working repo") }
  }
  if ($f.isWorktree) {
    return @{ rec = "KEEP - WORKTREE"; doNotTouch = $true; reasons = @("active git worktree") }
  }
  if ($referencedFolders -contains $n) {
    return @{ rec = "DO NOT TOUCH"; doNotTouch = $true; reasons = @("referenced by Backup/scripts/configs") }
  }
  if ($class -match 'BACKUP') {
    return @{ rec = "KEEP - BACKUP"; doNotTouch = $true; reasons = @("backup/staging/snapshot") }
  }
  if ($f.dirty -or $f.untrackedCount -gt 50) {
    return @{ rec = "KEEP - UNIQUE UNCOMMITTED WORK"; doNotTouch = $true; reasons = @("dirty or many untracked files") }
  }
  if ($f.headNotInPrimary -or ($f.uniqueCommitsVsPrimary -gt 0)) {
    return @{ rec = "DO NOT TOUCH"; doNotTouch = $true; reasons = @("HEAD not in primary or unique commits vs primary") }
  }
  if ($class -match 'TRUE DUPLICATE' -and -not $f.dirty -and $f.untrackedCount -eq 0 -and $f.head -eq $primaryHead) {
    return @{ rec = "SAFE TO CONSIDER DELETING"; doNotTouch = $false; reasons = @("same remote HEAD as primary, clean tree - verify before delete") }
  }
  if ($class -match 'OLDER CLONE' -and -not $f.dirty -and $f.untrackedCount -eq 0) {
    return @{ rec = "SAFE TO MOVE LATER"; doNotTouch = $false; reasons = @("older commit of same remote; confirm no unique files") }
  }
  if ($class -match 'GENERATED') {
    return @{ rec = "REQUIRES MANUAL REVIEW"; doNotTouch = $true; reasons = @("loose data/fixtures/cache at C:\Dev root") }
  }
  return @{ rec = "REQUIRES MANUAL REVIEW"; doNotTouch = $true; reasons = @("insufficient evidence for safe deletion") }
}

# Deep compare for Dealality git folders vs primary
Write-Host "File-level compare for Dealality git folders vs primary..."
$compareRows = @()
$dealGit = @($folders | Where-Object { $_.dealalityRelated -and $_.isGit -and $_.path -ne $Primary })
foreach ($f in $dealGit) {
  Write-Host ("  compare {0}" -f $f.name)
  $identical = 0; $differ = 0; $onlyA = 0; $onlyB = 0
  $newerA = 0; $newerB = 0
  $sampleDiffer = @(); $sampleOnlyB = @()

  if (-not $f.headNotInPrimary -and $f.head -and $primaryHead) {
    # Same object DB (or reachable): use git diff between trees - fast + accurate
    $diffNames = @(& git -C $Primary diff --name-only "$($f.head)" "$primaryHead" -- api lib public scripts fixtures config package.json server.js 2>$null)
    $differ = @($diffNames | Where-Object { $_ }).Count
    $sampleDiffer = @($diffNames | Select-Object -First 25)
    # Files only in one tree tip:
    $onlyPrimary = @(& git -C $Primary diff --diff-filter=A --name-only "$($f.head)" "$primaryHead" -- api lib public scripts fixtures config package.json server.js 2>$null)
    $onlyFolder = @(& git -C $Primary diff --diff-filter=D --name-only "$($f.head)" "$primaryHead" -- api lib public scripts fixtures config package.json server.js 2>$null)
    # Note: A relative to f->primary means added in primary (= only in primary from folder's view of history)
    $onlyA = @($onlyPrimary | Where-Object { $_ }).Count
    $onlyB = @($onlyFolder | Where-Object { $_ }).Count
    $sampleOnlyB = @($onlyFolder | Select-Object -First 25)
    # Approximate identical as tracked overlap minus differ - rough
    $trackedOverlap = [Math]::Min([int]$f.trackedCount, [int]$primaryGit.trackedCount)
    $identical = [Math]::Max(0, $trackedOverlap - $differ)
  } else {
    # Fallback filesystem compare (slower) - skip node_modules
    $cmp = Compare-SourceTrees -A $Primary -B $f.path
    $identical = $cmp.identical
    $differ = $cmp.differ
    $onlyA = $cmp.onlyInA
    $onlyB = $cmp.onlyInB
    $newerA = $cmp.newerInA
    $newerB = $cmp.newerInB
    $sampleDiffer = $cmp.sampleDiffer
    $sampleOnlyB = $cmp.sampleOnlyB
  }

  # Working-tree unique untracked source files (api/lib/public/scripts)
  $uniqueUntrackedSrc = @()
  if ($f.dirty) {
    $porc = @(& git -C $f.path status --porcelain=v1 --untracked-files=all 2>$null)
    $uniqueUntrackedSrc = @($porc | Where-Object {
      $_ -match '^\?\?\s+(api/|lib/|public/|scripts/|fixtures/|config/|server\.js|package\.json)'
    } | ForEach-Object { $_.Substring(3).Trim() } | Select-Object -First 40)
  }

  $compareRows += [pscustomobject]@{
    folder = $f.path
    name = $f.name
    branch = $f.branch
    head = $f.head
    headEqualsPrimary = ($f.head -eq $primaryHead)
    mergeBase = $f.mergeBase
    isWorktree = $f.isWorktree
    identical = $identical
    differ = $differ
    onlyInPrimary = $onlyA
    onlyInFolder = $onlyB
    newerInPrimary = $newerA
    newerInFolder = $newerB
    dirty = $f.dirty
    modifiedCount = $f.modifiedCount
    untrackedCount = $f.untrackedCount
    uniqueCommitsVsPrimary = $f.uniqueCommitsVsPrimary
    sampleDiffer = ($sampleDiffer -join "; ")
    sampleOnlyInFolder = ($sampleOnlyB -join "; ")
    uniqueUntrackedSourceSample = ($uniqueUntrackedSrc -join "; ")
    classification = (Classify-Folder $f)
  }
}

# Enrich
$enriched = @()
foreach ($f in $folders) {
  $class = Classify-Folder $f
  $rec = Recommend-Folder $f $class
  $obj = [ordered]@{}
  foreach ($p in $f.PSObject.Properties) { $obj[$p.Name] = $p.Value }
  $obj.classification = $class
  $obj.recommendation = $rec.rec
  $obj.doNotTouch = $rec.doNotTouch
  $obj.evidence = ($rec.reasons -join "; ")
  $enriched += [pscustomobject]$obj
}

$audit = [ordered]@{
  generatedAt = (Get-Date).ToString("o")
  primaryRepo = $Primary
  primaryHead = $primaryHead
  primaryBranch = $primaryGit.branch
  topLevelFolderCount = $enriched.Count
  folders = $enriched
  worktreesFromPrimary = $worktrees
  hardCodedPathRefs = $refHits
  referencedTopFolders = $referencedFolders
  scheduledTasks = $tasks
  duplicateCompare = $compareRows
  readOnly = $true
  mutationsPerformed = @("wrote reports under deal-capture-proxy/reports only")
}

$jsonPath = Join-Path $OutDir "c-dev-safety-audit.json"
$csvPath = Join-Path $OutDir "c-dev-duplicate-analysis.csv"
$mdPath = Join-Path $OutDir "c-dev-safety-audit.md"

$audit | ConvertTo-Json -Depth 10 | Set-Content -Path $jsonPath -Encoding UTF8

$enriched | Select-Object name, path, totalGB, fileCount, lastModified, isGit, isWorktree, remote, branch, head, dirty, modifiedCount, untrackedCount, ignoredCount, trackedCount, uniqueCommitsVsPrimary, headNotInPrimary, mergeBase, classification, recommendation, doNotTouch, evidence |
  Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

# Append compare rows to CSV as second section via separate file note in MD
$cmpCsv = Join-Path $OutDir "c-dev-duplicate-file-compare.csv"
$compareRows | Export-Csv -Path $cmpCsv -NoTypeInformation -Encoding UTF8

$md = @()
$md += "# C:\Dev read-only duplicate / overlap audit"
$md += ""
$md += "Generated: $($audit.generatedAt)"
$md += "Primary repo: ``$Primary`` @ ``$primaryHead`` (``$($primaryGit.branch)``)"
$md += ""
$md += "**READ-ONLY** - no deletes, moves, resets, cleans, or repo mutations (except writing these report files)."
$md += ""
$md += "## Top-level inventory"
$md += ""
$md += "| Folder | GB | Files | Git | Branch | HEAD | Dirty | M/U | Class | Recommendation |"
$md += "|--------|----|-------|-----|--------|------|-------|-----|-------|----------------|"
foreach ($f in ($enriched | Sort-Object { [double]($_.totalGB -replace ',', '') } -Descending)) {
  $headShort = if ($f.head) { $f.head.Substring(0, [Math]::Min(8, $f.head.Length)) } else { "-" }
  $md += "| $($f.name) | $($f.totalGB) | $($f.fileCount) | $(if($f.isGit){'Y'}else{'N'})$(if($f.isWorktree){' WT'}else{''}) | $($f.branch) | $headShort | $(if($f.dirty){'dirty'}else{'clean'}) | $($f.modifiedCount)/$($f.untrackedCount) | $($f.classification) | $($f.recommendation) |"
}
$md += ""
$md += "## Worktrees (from primary ``git worktree list``)"
$md += ""
if ($worktrees.Count -eq 0) { $md += "_None reported._" }
foreach ($w in $worktrees) {
  $md += "- ``$($w.path)`` head=$($w.head) branch=$($w.branch)"
}
$md += ""
$md += "## Dealality file-level compare vs primary (api/lib/public/scripts/fixtures/config + package.json/server.js)"
$md += ""
$md += "| Folder | Same HEAD | Ident | Differ | Only folder | Newer in folder | Dirty | Untracked | Unique commits | Class |"
$md += "|--------|-----------|-------|--------|-------------|-----------------|-------|-----------|----------------|-------|"
foreach ($c in $compareRows) {
  $md += "| $($c.name) | $($c.headEqualsPrimary) | $($c.identical) | $($c.differ) | $($c.onlyInFolder) | $($c.newerInFolder) | $($c.dirty) | $($c.untrackedCount) | $($c.uniqueCommitsVsPrimary) | $($c.classification) |"
}
$md += ""
$md += "### Sample files only in clone / differing"
$md += ""
foreach ($c in $compareRows) {
  if ($c.onlyInFolder -gt 0 -or $c.differ -gt 0) {
    $md += "**$($c.name)**"
    $md += "- Differ sample: $($c.sampleDiffer)"
    $md += "- Only-in-folder sample: $($c.sampleOnlyInFolder)"
    $md += ""
  }
}
$md += "## Hard-coded C:\\Dev references"
$md += ""
$md += "Hit count: $($refHits.Count)"
$md += ""
$md += "Top-level folders referenced: $($referencedFolders -join ', ')"
$md += ""
foreach ($h in ($refHits | Select-Object -First 50)) {
  $md += "- ``$($h.file):$($h.line)`` -> $($h.match)"
}
$md += ""
$md += "## Scheduled tasks (Dealality/Backup/deal)"
$md += ""
if ($tasks.Count -eq 0) { $md += "_None matched or Task Scheduler inaccessible._" }
foreach ($t in $tasks) {
  $md += "- **$($t.task)** ($($t.state)): ``$($t.actions)``"
}
$md += ""
$md += "## Disk breakdown (largest folders)"
$md += ""
foreach ($f in ($enriched | Sort-Object { [double]($_.totalGB -replace ',', '') } -Descending | Select-Object -First 15)) {
  $md += "### $($f.name) ($($f.totalGB) GB)"
  $md += "- node_modules: $($f.breakdown.node_modules_gb) GB"
  $md += "- .git: $($f.breakdown.git_gb) GB"
  $md += "- data: $($f.breakdown.data_gb) GB"
  $md += "- public: $($f.breakdown.public_gb) GB"
  $md += "- reports: $($f.breakdown.reports_gb) GB"
  $md += "- fixtures: $($f.breakdown.fixtures_gb) GB"
  $md += "- api+lib+scripts: $($f.breakdown.api_lib_gb) GB"
  $md += "- Class: $($f.classification)"
  $md += "- Recommendation: $($f.recommendation)"
  $md += "- Evidence: $($f.evidence)"
  $md += ""
}
$md += "## DO NOT TOUCH list"
$md += ""
foreach ($f in $enriched | Where-Object { $_.doNotTouch }) {
  $md += "- **$($f.name)**: $($f.recommendation) - $($f.evidence)"
}
$md += ""
$md += "## Deletion / move candidates (manual confirmation required - NO ACTION TAKEN)"
$md += ""
$cands = @($enriched | Where-Object { $_.recommendation -match 'SAFE TO CONSIDER DELETING|SAFE TO MOVE' })
if ($cands.Count -eq 0) {
  $md += "_No folders met the strict clean-duplicate criteria. Prefer KEEP / DO NOT TOUCH until Joan reviews._"
} else {
  foreach ($f in $cands) {
    $md += "### $($f.name)"
    $md += "- Path: ``$($f.path)``"
    $md += "- Size: $($f.totalGB) GB"
    $md += "- Why: $($f.evidence)"
    $md += "- HEAD: $($f.head)"
    $md += "- Same as primary HEAD: $($f.head -eq $primaryHead)"
    $md += "- Dirty: $($f.dirty); modified=$($f.modifiedCount); untracked=$($f.untrackedCount)"
    $md += "- Recoverable space (estimate): $($f.totalGB) GB **only after confirming no unique local files**"
    $md += ""
  }
}
$md += "---"
$md += ""
$md += "READY FOR JOAN REVIEW - C:\\DEV READ-ONLY DUPLICATE AUDIT COMPLETE"

$md -join "`n" | Set-Content -Path $mdPath -Encoding UTF8

Write-Host "WROTE $jsonPath"
Write-Host "WROTE $csvPath"
Write-Host "WROTE $cmpCsv"
Write-Host "WROTE $mdPath"
Write-Host "DONE"
