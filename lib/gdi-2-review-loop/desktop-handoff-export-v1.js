/**
 * GDI 2.0 permanent ChatGPT Desktop handoff export.
 *
 * Creates:
 *   C:\Users\joand\OneDrive\Desktop\gdi-2-run-NNN-phaseXX-handoff.zip
 *   C:\Users\joand\OneDrive\Desktop\gdi-2-run-NNN-phaseXX-integrity.txt
 *
 * Never modifies a sealed run. Blocks export when security validation fails.
 */

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { ROOT, LOOP_ROOT } from './constants.js';
import {
  hashFileSha256,
  isRunSealed,
  readHandoff,
  runDirFor,
  verifyManifest,
} from './immutability.js';
import { verifyIntegrityAnchor } from './integrity-anchor.js';
import { validateExportPackage } from './export-scan.js';

export const DESKTOP_HANDOFF_EXPORT_V1 = 'gdi-2-desktop-handoff-export-v1';

export const DEFAULT_DESKTOP_DIR = 'C:\\Users\\joand\\OneDrive\\Desktop';

const UNSAFE_NAME_RE =
  /(^|\/|\\)(\.env|\.env\..*|.*credentials\.json|.*\.pem|.*\.p12|id_rsa|active-tokens\.json|revoked-token-ids\.json|production-share-contract-tokens\.json)($|\/|\\)/i;

function toPosix(p) {
  return String(p).replace(/\\/g, '/');
}

function ensureDir(abs) {
  fs.mkdirSync(abs, { recursive: true });
}

function copyFileSafe(src, dest, excluded) {
  const rel = toPosix(path.relative(ROOT, src));
  if (UNSAFE_NAME_RE.test(rel) || UNSAFE_NAME_RE.test(path.basename(src))) {
    excluded.push({ path: rel, reason: 'unsafe_or_secret_path' });
    return false;
  }
  ensureDir(path.dirname(dest));
  fs.copyFileSync(src, dest);
  return true;
}

function copyTree(srcDir, destDir, excluded, { filter } = {}) {
  if (!fs.existsSync(srcDir)) {
    excluded.push({ path: toPosix(path.relative(ROOT, srcDir)), reason: 'missing_source' });
    return;
  }
  const entries = fs.readdirSync(srcDir, { withFileTypes: true });
  for (const ent of entries) {
    const abs = path.join(srcDir, ent.name);
    const rel = toPosix(path.relative(ROOT, abs));
    if (filter && !filter(rel, ent)) {
      excluded.push({ path: rel, reason: 'filtered' });
      continue;
    }
    if (UNSAFE_NAME_RE.test(rel) || UNSAFE_NAME_RE.test(ent.name)) {
      excluded.push({ path: rel, reason: 'unsafe_or_secret_path' });
      continue;
    }
    const dest = path.join(destDir, ent.name);
    if (ent.isDirectory()) {
      if (ent.name === 'node_modules' || ent.name === '.git' || ent.name === 'validation-reports') {
        excluded.push({ path: rel, reason: 'bulk_or_regenerable' });
        continue;
      }
      ensureDir(dest);
      copyTree(abs, dest, excluded, { filter });
    } else if (ent.isFile()) {
      copyFileSafe(abs, dest, excluded);
    }
  }
}

function walkFiles(absDir, out = []) {
  if (!fs.existsSync(absDir)) return out;
  for (const ent of fs.readdirSync(absDir, { withFileTypes: true })) {
    const abs = path.join(absDir, ent.name);
    if (ent.isDirectory()) walkFiles(abs, out);
    else if (ent.isFile()) out.push(abs);
  }
  return out;
}

function resolveDesktopDir(preferred) {
  const candidates = [
    preferred,
    DEFAULT_DESKTOP_DIR,
    path.join(process.env.USERPROFILE || '', 'OneDrive', 'Desktop'),
    path.join(process.env.USERPROFILE || '', 'Desktop'),
  ].filter(Boolean);
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  const fallback = DEFAULT_DESKTOP_DIR;
  ensureDir(fallback);
  return fallback;
}

function zipStage(stageDir, zipPath) {
  if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath);
  // Prefer PowerShell Compress-Archive for Windows path fidelity
  const ps = `
$ErrorActionPreference = 'Stop'
$stage = '${stageDir.replace(/'/g, "''")}'
$zip = '${zipPath.replace(/'/g, "''")}'
if (Test-Path $zip) { Remove-Item $zip -Force }
Compress-Archive -Path (Join-Path $stage '*') -DestinationPath $zip -CompressionLevel Optimal
`;
  const r = spawnSync(
    'powershell.exe',
    ['-NoProfile', '-NonInteractive', '-Command', ps],
    { encoding: 'utf8' },
  );
  if (r.status !== 0 || !fs.existsSync(zipPath)) {
    throw new Error(`ZIP creation failed: ${r.stderr || r.stdout || 'unknown'}`);
  }
}

function verifyZipReadable(zipPath, requiredRelPaths = []) {
  const ps = `
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead('${zipPath.replace(/'/g, "''")}')
try {
  $names = @($zip.Entries | ForEach-Object { $_.FullName.Replace('\\','/') })
  $count = $names.Count
  $missing = @()
  foreach ($req in @(${requiredRelPaths.map((p) => `'${p.replace(/'/g, "''")}'`).join(',')})) {
    $hit = $false
    foreach ($n in $names) { if ($n -eq $req -or $n.EndsWith('/' + $req) -or $n -eq ($req.TrimEnd('/') + '/')) { $hit = $true; break } }
    if (-not $hit) {
      $prefix = $req.TrimEnd('/') + '/'
      foreach ($n in $names) { if ($n.StartsWith($prefix) -or $n -eq $req) { $hit = $true; break } }
    }
    if (-not $hit) { $missing += $req }
  }
  @{ ok = ($missing.Count -eq 0); count = $count; missing = $missing } | ConvertTo-Json -Compress
} finally { $zip.Dispose() }
`;
  const r = spawnSync(
    'powershell.exe',
    ['-NoProfile', '-NonInteractive', '-Command', ps],
    { encoding: 'utf8' },
  );
  if (r.status !== 0) {
    return { ok: false, count: 0, missing: [], error: r.stderr || r.stdout };
  }
  try {
    return JSON.parse(String(r.stdout || '').trim());
  } catch (e) {
    return { ok: false, count: 0, missing: [], error: String(e) };
  }
}

function captureDiff(relPaths, destFile) {
  const lines = [];
  for (const rel of relPaths) {
    if (rel.endsWith('/')) continue;
    const abs = path.join(ROOT, ...rel.split('/'));
    if (!fs.existsSync(abs) || !fs.statSync(abs).isFile()) continue;
    const diff = spawnSync('git', ['diff', '--', rel], {
      cwd: ROOT,
      encoding: 'utf8',
      maxBuffer: 8 * 1024 * 1024,
    });
    if (diff.status === 0 && textHasContent(diff.stdout)) {
      lines.push(`\n##### DIFF ${rel}\n`);
      lines.push(diff.stdout);
    } else {
      const show = spawnSync('git', ['show', `HEAD:${rel}`], {
        cwd: ROOT,
        encoding: 'utf8',
        maxBuffer: 2 * 1024 * 1024,
      });
      if (show.status !== 0) {
        lines.push(`\n##### NEW_FILE ${rel} (not in HEAD)\n`);
      }
    }
  }
  ensureDir(path.dirname(destFile));
  fs.writeFileSync(destFile, lines.join('') || '# No git diffs captured (new or unchanged vs HEAD).\n', 'utf8');
}

function textHasContent(s) {
  return Boolean(String(s || '').trim());
}

/**
 * @param {{ runId: string, phaseId: string, docsDir?: string, extraPaths?: string[], desktopDir?: string }} opts
 */
export function exportDesktopHandoffPackage(opts = {}) {
  const runId = opts.runId;
  const phaseId = opts.phaseId;
  const docsDirName = opts.docsDir || phaseId;
  if (!runId || !phaseId) {
    throw new Error('runId and phaseId are required');
  }
  if (!isRunSealed(runId)) {
    return { ok: false, error: `${runId} is not sealed — refuse export` };
  }

  const handoff = readHandoff(runId);
  const runDir = runDirFor(runId);
  const anchorPath = path.join(LOOP_ROOT, 'integrity-anchors', `${runId}.integrity.json`);
  const excluded = [];
  const included = [];

  const desktopDir = resolveDesktopDir(opts.desktopDir);
  const zipName = `gdi-2-${runId}-${phaseId}-handoff.zip`;
  const integrityName = `gdi-2-${runId}-${phaseId}-integrity.txt`;
  const zipPath = path.join(desktopDir, zipName);
  const integrityPath = path.join(desktopDir, integrityName);
  const stage = path.join(
    process.env.TEMP || process.env.TMP || ROOT,
    `gdi-2-${runId}-${phaseId}-export-stage`,
  );

  if (fs.existsSync(stage)) fs.rmSync(stage, { recursive: true, force: true });
  ensureDir(stage);

  // 1) Sealed run (read-only copy)
  copyTree(runDir, path.join(stage, 'reports', 'gdi-2-review-loop', 'runs', runId), excluded);

  // 2) Living phase / assessment docs (docsDir may differ from zip phaseId label)
  const phaseDir = path.join(LOOP_ROOT, docsDirName);
  if (fs.existsSync(phaseDir)) {
    copyTree(
      phaseDir,
      path.join(stage, 'reports', 'gdi-2-review-loop', docsDirName),
      excluded,
    );
  } else {
    excluded.push({
      path: `reports/gdi-2-review-loop/${docsDirName}`,
      reason: 'phase_dir_missing',
    });
  }

  // 3) Integrity anchor + living docs
  for (const rel of [
    `reports/gdi-2-review-loop/integrity-anchors/${runId}.integrity.json`,
    'reports/gdi-2-review-loop/CURRENT_HANDOFF.md',
    'reports/gdi-2-review-loop/CURRENT_STATE.md',
    'reports/gdi-2-review-loop/MASTER_ROADMAP.md',
    'reports/gdi-2-review-loop/DECISION_LOG.md',
    'reports/gdi-2-review-loop/NEXT_CURSOR_PROMPT.md',
    'reports/gdi-2-review-loop/CHATGPT_REVIEW_EXPORT_POLICY.md',
    'reports/gdi-2-review-loop/ARCHITECTURE.md',
    'reports/gdi-2-review-loop/GITHUB_SYNC.md',
    `.cursor/rules/gdi-2-chatgpt-review-loop.mdc`,
  ]) {
    const abs = path.join(ROOT, ...rel.split('/'));
    if (!fs.existsSync(abs)) {
      excluded.push({ path: rel, reason: 'missing_optional_or_required_doc' });
      continue;
    }
    copyFileSafe(abs, path.join(stage, ...rel.split('/')), excluded);
  }

  // 4) Changed paths from sealed handoff + extras
  const changedPaths = [
    ...(Array.isArray(handoff?.changedPaths) ? handoff.changedPaths : []),
    ...(opts.extraPaths || []),
  ];
  const filePathsForDiff = [];
  for (const relRaw of changedPaths) {
    const rel = toPosix(relRaw).replace(/^\.\//, '');
    const abs = path.join(ROOT, ...rel.split('/').filter(Boolean));
    if (!fs.existsSync(abs)) {
      excluded.push({ path: rel, reason: 'changed_path_missing' });
      continue;
    }
    const st = fs.statSync(abs);
    if (st.isDirectory()) {
      copyTree(abs, path.join(stage, ...rel.split('/')), excluded);
    } else if (st.isFile()) {
      if (copyFileSafe(abs, path.join(stage, ...rel.split('/')), excluded)) {
        filePathsForDiff.push(rel);
      }
    }
  }

  // Always include review-loop lib used for export/validate
  copyTree(
    path.join(ROOT, 'lib', 'gdi-2-review-loop'),
    path.join(stage, 'lib', 'gdi-2-review-loop'),
    excluded,
  );

  // Diffs
  captureDiff(filePathsForDiff, path.join(stage, 'reports', 'gdi-2-review-loop', 'export', `${runId}-source-diffs.patch`));

  // Inventory
  const stagedFiles = walkFiles(stage).map((abs) => toPosix(path.relative(stage, abs)));
  for (const rel of stagedFiles) included.push(rel);
  const inventory = {
    contractId: DESKTOP_HANDOFF_EXPORT_V1,
    runId,
    phaseId,
    generatedAt: new Date().toISOString(),
    sealedStatus: handoff?.status || null,
    includedFileCount: included.length,
    included,
    excluded,
    notes: [
      'Repository-relative paths preserved inside the ZIP.',
      'Sealed run contents were copied read-only; the sealed package was not modified.',
      'Secrets, env files, share tokens, and node_modules are excluded.',
    ],
  };
  ensureDir(path.join(stage, 'reports', 'gdi-2-review-loop', 'export'));
  fs.writeFileSync(
    path.join(stage, 'reports', 'gdi-2-review-loop', 'export', `${runId}-FILE_INVENTORY.json`),
    `${JSON.stringify(inventory, null, 2)}\n`,
    'utf8',
  );

  // Security validation on living export roots (blocking)
  const exportValidation = validateExportPackage({ writeReport: false });
  if (!exportValidation.ok) {
    return {
      ok: false,
      error: 'export_security_validation_failed',
      exportValidation,
      blocker: exportValidation.errors || exportValidation,
    };
  }

  const manifestOk = verifyManifest(runId);
  const anchorOk = verifyIntegrityAnchor(runId);
  if (!manifestOk.ok || !anchorOk.ok) {
    return {
      ok: false,
      error: 'seal_or_anchor_verification_failed',
      manifestOk,
      anchorOk,
    };
  }

  // Create ZIP
  zipStage(stage, zipPath);
  const zipSha256 = hashFileSha256(zipPath);
  const zipStat = fs.statSync(zipPath);

  const required = [
    `reports/gdi-2-review-loop/runs/${runId}/HANDOFF.json`,
    `reports/gdi-2-review-loop/runs/${runId}/SEAL.json`,
    `reports/gdi-2-review-loop/runs/${runId}/MANIFEST.sha256`,
    `reports/gdi-2-review-loop/integrity-anchors/${runId}.integrity.json`,
    'reports/gdi-2-review-loop/CURRENT_HANDOFF.md',
    'reports/gdi-2-review-loop/MASTER_ROADMAP.md',
    'reports/gdi-2-review-loop/DECISION_LOG.md',
    `reports/gdi-2-review-loop/export/${runId}-FILE_INVENTORY.json`,
  ];
  const zipCheck = verifyZipReadable(zipPath, required);
  if (!zipCheck.ok) {
    return {
      ok: false,
      error: 'zip_verification_failed',
      zipCheck,
      zipPath,
    };
  }

  // Re-hash zip after verify (unchanged)
  const zipSha256Verify = hashFileSha256(zipPath);
  if (zipSha256 !== zipSha256Verify) {
    return { ok: false, error: 'zip_digest_changed_during_verify', zipSha256, zipSha256Verify };
  }

  const anchor = JSON.parse(fs.readFileSync(anchorPath, 'utf8'));
  const integrityBody = `# GDI 2.0 ChatGPT Review Package Integrity

runId: ${runId}
phaseId: ${phaseId}
sealedStatus: ${handoff?.status || 'UNKNOWN'}
generatedAt: ${new Date().toISOString()}

zipFilename: ${zipName}
zipFullPath: ${zipPath}
zipBytes: ${zipStat.size}
zipFileCount: ${zipCheck.count}
zipSha256: ${zipSha256}

packageRootSha256: ${anchor.digests.packageRootSha256}
manifestSha256: ${anchor.digests.manifestFileSha256}
sealSha256: ${anchor.digests.sealJsonSha256}
handoffSha256: ${anchor.digests.handoffSha256}

exportValidationOk: ${exportValidation.ok}
manifestVerifyOk: ${manifestOk.ok}
anchorVerifyOk: ${anchorOk.ok}
zipOpenVerifyOk: ${zipCheck.ok}

excludedCount: ${excluded.length}
limitations:
- Sealed run was not modified
- Application SEAL is not cryptographic immutability; trust packageRootSha256 + this integrity file
- Creating this ZIP does not authorize paid research, GitHub sync, production writes, or deployment
- Phase gate remains locked until ChatGPT PASS/CONDITIONAL_PASS + APPROVED NEXT_CURSOR_PROMPT

excludedSample:
${excluded
  .slice(0, 40)
  .map((e) => `- ${e.path} (${e.reason})`)
  .join('\n') || '- (none)'}
`;

  fs.writeFileSync(integrityPath, integrityBody, 'utf8');

  // Cleanup stage
  try {
    fs.rmSync(stage, { recursive: true, force: true });
  } catch {
    /* ignore */
  }

  return {
    ok: true,
    runId,
    phaseId,
    sealedStatus: handoff?.status,
    zipPath,
    integrityPath,
    zipSha256,
    zipBytes: zipStat.size,
    zipFileCount: zipCheck.count,
    packageRootSha256: anchor.digests.packageRootSha256,
    manifestSha256: anchor.digests.manifestFileSha256,
    sealSha256: anchor.digests.sealJsonSha256,
    handoffSha256: anchor.digests.handoffSha256,
    exportValidationOk: exportValidation.ok,
    excludedCount: excluded.length,
    nextPhaseUnlocked: false,
  };
}
