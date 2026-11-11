import fs from 'node:fs';
import path from 'node:path';
import { APPROVED_EVIDENCE_ROOTS, ROOT, RUNS_DIR } from './constants.js';

function toPosix(p) {
  return String(p || '').replace(/\\/g, '/');
}

/**
 * Resolve an evidence path safely. Rejects absolute paths outside the repo,
 * path traversal, and paths outside approved roots.
 */
export function resolveEvidencePath(evidencePath, runId) {
  const errors = [];
  if (typeof evidencePath !== 'string' || !evidencePath.trim()) {
    return { ok: false, absPath: null, relPosix: null, errors: ['evidencePath empty'] };
  }

  const raw = evidencePath.trim();
  if (raw.includes('\0')) {
    return { ok: false, absPath: null, relPosix: null, errors: ['evidencePath contains NUL'] };
  }

  // Disallow Windows absolute / UNC and POSIX absolute for handoff claims.
  if (path.isAbsolute(raw) || /^[a-zA-Z]:[\\/]/.test(raw) || raw.startsWith('\\\\')) {
    return {
      ok: false,
      absPath: null,
      relPosix: null,
      errors: [`unsafe absolute evidence path: ${raw}`],
    };
  }

  const normalized = toPosix(path.normalize(raw));
  if (normalized.startsWith('..') || normalized.includes('/../') || normalized === '..') {
    return {
      ok: false,
      absPath: null,
      relPosix: null,
      errors: [`path traversal rejected: ${raw}`],
    };
  }

  // Allow short paths relative to the run directory (e.g. evidence/foo.json).
  let relPosix = normalized.replace(/^\.\//, '');
  if (!relPosix.startsWith('reports/')) {
    relPosix = toPosix(path.posix.join(`reports/gdi-2-review-loop/runs/${runId}`, relPosix));
  }

  const approved = APPROVED_EVIDENCE_ROOTS.some((root) => relPosix.startsWith(root));
  if (!approved) {
    errors.push(`evidence path outside approved roots: ${relPosix}`);
  }

  // Must stay under this run unless explicitly under runs/<runId>/
  const runPrefix = `reports/gdi-2-review-loop/runs/${runId}/`;
  if (!relPosix.startsWith(runPrefix)) {
    errors.push(`evidence path must remain under ${runPrefix}`);
  }

  const absPath = path.resolve(ROOT, ...relPosix.split('/'));
  const rootResolved = path.resolve(ROOT);
  if (!absPath.startsWith(rootResolved + path.sep) && absPath !== rootResolved) {
    errors.push(`resolved path escapes repository root: ${relPosix}`);
  }

  // Extra: must be under RUNS_DIR/runId
  const runAbs = path.resolve(RUNS_DIR, runId);
  if (!absPath.startsWith(runAbs + path.sep) && absPath !== runAbs) {
    errors.push(`resolved path escapes run directory: ${relPosix}`);
  }

  return {
    ok: errors.length === 0,
    absPath,
    relPosix,
    errors,
  };
}

export function inspectEvidenceFile(absPath) {
  if (!fs.existsSync(absPath)) {
    return { exists: false, empty: true, size: 0, errors: ['evidence file missing'] };
  }
  const st = fs.statSync(absPath);
  if (!st.isFile()) {
    return { exists: true, empty: true, size: 0, errors: ['evidence path is not a file'] };
  }
  if (st.size === 0) {
    return { exists: true, empty: true, size: 0, errors: ['evidence file is empty'] };
  }
  return { exists: true, empty: false, size: st.size, errors: [] };
}

/**
 * Validate one test entry for evidence integrity.
 */
export function validateTestEvidence(test, index, runId) {
  const errors = [];
  const warnings = [];
  const label = `tests[${index}]`;

  if (!test || typeof test !== 'object') {
    return { errors: [`${label} invalid`], warnings };
  }

  if (typeof test.command !== 'string' || !test.command.trim()) {
    errors.push(`${label}.command required`);
  }
  if (typeof test.exitCode !== 'number' || !Number.isInteger(test.exitCode)) {
    errors.push(`${label}.exitCode must be integer`);
  }
  if (typeof test.claimedPass !== 'boolean') {
    errors.push(`${label}.claimedPass must be boolean`);
  }
  if (typeof test.executedAt !== 'string' || !test.executedAt.trim()) {
    errors.push(`${label}.executedAt required (ISO timestamp)`);
  }
  if (!['pass', 'fail', 'skipped'].includes(test.result)) {
    errors.push(`${label}.result must be pass|fail|skipped`);
  }

  const resolved = resolveEvidencePath(test.evidencePath, runId);
  if (!resolved.ok) {
    errors.push(...resolved.errors.map((e) => `${label}: ${e}`));
  } else {
    const file = inspectEvidenceFile(resolved.absPath);
    if (file.errors.length) {
      errors.push(...file.errors.map((e) => `${label}: ${e}`));
    }
  }

  // Consistency rules
  if (test.result === 'pass') {
    if (test.exitCode !== 0) errors.push(`${label}: result=pass requires exitCode 0`);
    if (test.claimedPass !== true) errors.push(`${label}: result=pass requires claimedPass=true`);
  }
  if (test.result === 'fail') {
    if (test.claimedPass === true) {
      errors.push(`${label}: false success — result=fail cannot claim claimedPass=true`);
    }
  }
  if (test.result === 'skipped') {
    if (test.claimedPass === true) {
      errors.push(`${label}: skipped test cannot claim claimedPass=true`);
    }
  }
  if (test.claimedPass === true && test.exitCode !== 0) {
    errors.push(`${label}: false success — claimedPass=true with exitCode=${test.exitCode}`);
  }
  if (test.claimedPass === true && test.result !== 'pass') {
    errors.push(`${label}: false success — claimedPass=true requires result=pass`);
  }

  // V1.2 execution provenance is validated separately via validateExecutionConsistency.
  if (test.evidenceOrigin === 'independent_execution' && !test.executionRecordPath) {
    errors.push(`${label}.executionRecordPath required when evidenceOrigin=independent_execution`);
  }

  return { errors, warnings, resolved };
}

/**
 * Basic consistency between HANDOFF.tests and TEST_RESULTS.md (machine vs human).
 */
export function validateTestResultsMarkdownConsistency(handoff, testResultsMd) {
  const errors = [];
  const tests = Array.isArray(handoff?.tests) ? handoff.tests : [];
  for (const [i, t] of tests.entries()) {
    if (!t?.command) continue;
    // Require the command string (or a distinctive substring) to appear in the human report.
    const needle = t.command.length > 80 ? t.command.slice(0, 80) : t.command;
    if (!testResultsMd.includes(needle) && !testResultsMd.includes(t.evidencePath || '')) {
      errors.push(
        `TEST_RESULTS.md missing reference for tests[${i}] command/evidence (machine↔human inconsistency)`,
      );
    }
    if (t.claimedPass === true) {
      const passHints = ['pass', 'yes', 'exit 0', `| ${t.exitCode} |`];
      // Soft check: if claimed pass, exit code should be mentioned somewhere near evidence
      if (!String(testResultsMd).includes(String(t.exitCode))) {
        errors.push(`TEST_RESULTS.md does not mention exitCode ${t.exitCode} for tests[${i}]`);
      }
      void passHints;
    }
  }
  return { errors };
}
