import fs from 'node:fs';
import path from 'node:path';
import { ROOT, LOOP_ROOT, MAX_ARTIFACT_BYTES, VALIDATION_REPORTS_DIR } from './constants.js';
import {
  scanRunPackageForSecrets,
  BLOCKING_SECRET_PATTERNS,
  isSecretPatternAllowlistedPath,
} from './secrets-scan.js';
import { atomicWriteFile } from './immutability.js';

/**
 * Roots included in a controlled review-loop ZIP / future GitHub sync package.
 * Export validation MUST cover all of these, not only runs/run-NNN/.
 */
export const EXPORT_ROOTS = [
  'reports/gdi-2-review-loop',
  'lib/gdi-2-review-loop',
  'scripts',
  '.cursor/rules',
];

const EXPORT_SCRIPT_PREFIXES = [
  'gdi-2-review-loop-',
  'test-gdi-2-review-loop-',
];

const EXPORT_RULE_NAME = 'gdi-2-chatgpt-review-loop.mdc';

const TEXT_EXT = new Set([
  '.md',
  '.json',
  '.txt',
  '.log',
  '.csv',
  '.yml',
  '.yaml',
  '.js',
  '.mjs',
  '.cjs',
  '.html',
  '.sha256',
  '.mdc',
]);
const BINARY_EXT = new Set([
  '.png',
  '.jpg',
  '.jpeg',
  '.gif',
  '.webp',
  '.pdf',
  '.zip',
  '.exe',
  '.dll',
  '.node',
  '.wasm',
  '.bin',
]);

function toPosix(p) {
  return String(p).replace(/\\/g, '/');
}

function shouldIncludeExportPath(relPosix) {
  if (relPosix.startsWith('reports/gdi-2-review-loop/')) {
    // Regenerable validation outputs echo prior findings; exclude from export package set.
    if (relPosix.startsWith('reports/gdi-2-review-loop/validation-reports/')) return false;
    if (relPosix.includes('/_test-work/')) return false;
    if (relPosix.includes('/_phase1-drafts/')) return false;
    return true;
  }
  if (relPosix.startsWith('lib/gdi-2-review-loop/')) return true;
  if (relPosix.startsWith('scripts/')) {
    const base = path.posix.basename(relPosix);
    return EXPORT_SCRIPT_PREFIXES.some((p) => base.startsWith(p));
  }
  if (relPosix === `.cursor/rules/${EXPORT_RULE_NAME}`) return true;
  return false;
}

function walkDir(absDir, out = []) {
  if (!fs.existsSync(absDir)) return out;
  let entries;
  try {
    entries = fs.readdirSync(absDir, { withFileTypes: true });
  } catch (e) {
    out.push({ abs: absDir, error: `unreadable directory: ${e.message}` });
    return out;
  }
  for (const ent of entries) {
    const abs = path.join(absDir, ent.name);
    let st;
    try {
      st = fs.lstatSync(abs);
    } catch (e) {
      out.push({ abs, error: `unreadable path: ${e.message}` });
      continue;
    }
    if (st.isSymbolicLink()) {
      out.push({ abs, symlink: true, size: st.size });
      continue;
    }
    if (st.isDirectory()) walkDir(abs, out);
    else if (st.isFile()) out.push({ abs, size: st.size });
    else out.push({ abs, error: `unsupported file type at ${abs}` });
  }
  return out;
}

/**
 * Enumerate every file that would be included in the review-loop export package.
 */
export function listExportPackageFiles() {
  const files = [];
  const errors = [];

  for (const root of EXPORT_ROOTS) {
    const absRoot = path.join(ROOT, ...root.split('/'));
    if (!fs.existsSync(absRoot)) {
      errors.push(`missing export root: ${root}`);
      continue;
    }
    const walked = walkDir(absRoot);
    for (const item of walked) {
      if (item.error) {
        errors.push(item.error);
        continue;
      }
      const rel = toPosix(path.relative(ROOT, item.abs));
      if (!shouldIncludeExportPath(rel)) continue;
      files.push({
        abs: item.abs,
        relPosix: rel,
        size: item.size || 0,
        symlink: Boolean(item.symlink),
      });
    }
  }

  files.sort((a, b) => a.relPosix.localeCompare(b.relPosix));
  return { files, errors };
}

function scanTextBuffer(text, relPosix, errors) {
  for (const pat of BLOCKING_SECRET_PATTERNS) {
    if (pat.re.test(text)) {
      errors.push(`secret/privacy finding [${pat.id}/${pat.label}] in ${relPosix}`);
    }
  }
}

/**
 * Full export/GitHub-prep validation. Fail closed on unreadable/unsafe files.
 * Does NOT claim scanning guarantees absence of all confidential information.
 */
export function validateExportPackage({
  writeReport = true,
  maxBytes = MAX_ARTIFACT_BYTES,
} = {}) {
  const errors = [];
  const warnings = [
    'Automated scanning reduces risk but does not guarantee that no confidential information is present.',
  ];

  const listed = listExportPackageFiles();
  errors.push(...listed.errors);

  for (const f of listed.files) {
    if (f.symlink) {
      errors.push(`symlink not allowed in export package: ${f.relPosix}`);
      continue;
    }
    if (f.size > maxBytes) {
      errors.push(`oversized export artifact (${f.size} > ${maxBytes}): ${f.relPosix}`);
    }

    const base = path.basename(f.abs);
    if (/^\.env($|\.)/i.test(base)) {
      errors.push(`sensitive configuration file in export set: ${f.relPosix}`);
      continue;
    }

    const ext = path.extname(f.abs).toLowerCase();
    if (BINARY_EXT.has(ext)) {
      errors.push(`unsupported binary in export set: ${f.relPosix}`);
      continue;
    }

    let st;
    try {
      st = fs.statSync(f.abs);
    } catch (e) {
      errors.push(`unreadable export file (fail closed): ${f.relPosix} (${e.message})`);
      continue;
    }

    if (!TEXT_EXT.has(ext)) {
      try {
        const fd = fs.openSync(f.abs, 'r');
        const buf = Buffer.alloc(Math.min(512, st.size));
        fs.readSync(fd, buf, 0, buf.length, 0);
        fs.closeSync(fd);
        if (buf.includes(0)) {
          errors.push(`unexpected binary content in export set: ${f.relPosix}`);
          continue;
        }
      } catch (e) {
        errors.push(`unreadable export file (fail closed): ${f.relPosix} (${e.message})`);
        continue;
      }
    }

    if (!TEXT_EXT.has(ext)) continue;

    let text;
    try {
      text = fs.readFileSync(f.abs, 'utf8');
    } catch (e) {
      errors.push(`unreadable text export file (fail closed): ${f.relPosix} (${e.message})`);
      continue;
    }
    if (isSecretPatternAllowlistedPath(f.relPosix)) {
      // Fixture/scanner source files may contain example secret strings for negative tests.
      const local = [];
      scanTextBuffer(text, f.relPosix, local);
      if (local.length) {
        warnings.push(
          `allowlisted pattern hits in ${f.relPosix} (${local.length}) — not blocking; still not a confidentiality guarantee`,
        );
      }
    } else {
      scanTextBuffer(text, f.relPosix, errors);
    }
  }

  // Also re-scan each sealed/unsealed run dir with existing helper (defense in depth)
  const runsDir = path.join(LOOP_ROOT, 'runs');
  if (fs.existsSync(runsDir)) {
    for (const name of fs.readdirSync(runsDir)) {
      const p = path.join(runsDir, name);
      if (!fs.statSync(p).isDirectory()) continue;
      const scan = scanRunPackageForSecrets(p, { maxBytes });
      if (!scan.ok) {
        for (const e of scan.errors) {
          const msg = `run-package:${name}: ${e}`;
          if (!errors.includes(msg)) errors.push(msg);
        }
      }
    }
  }

  const report = {
    ok: errors.length === 0,
    contractId: 'GDI_2_EXPORT_VALIDATION_V1',
    scannedAt: new Date().toISOString(),
    fileCount: listed.files.length,
    errorCount: errors.length,
    warningCount: warnings.length,
    errors,
    warnings,
    roots: EXPORT_ROOTS,
    blockingForExternalSync: errors.length > 0,
    disclaimer:
      'Pass means no matched blocking patterns / unsafe files in the scanned export set. It is not a guarantee of zero confidential information.',
  };

  if (writeReport) {
    fs.mkdirSync(VALIDATION_REPORTS_DIR, { recursive: true });
    const stamp = report.scannedAt.replace(/[:.]/g, '-');
    const outPath = path.join(VALIDATION_REPORTS_DIR, `export-package-${stamp}.json`);
    atomicWriteFile(outPath, `${JSON.stringify(report, null, 2)}\n`);
    report.reportPath = toPosix(path.relative(LOOP_ROOT, outPath));
  }

  return report;
}
