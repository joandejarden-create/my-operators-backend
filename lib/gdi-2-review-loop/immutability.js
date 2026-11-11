import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {
  MANIFEST_NAME,
  SEAL_MARKER_NAME,
  SEALED_STATUSES,
  RUNS_DIR,
} from './constants.js';

export function runDirFor(runId) {
  return path.join(RUNS_DIR, runId);
}

export function readHandoff(runId) {
  const p = path.join(runDirFor(runId), 'HANDOFF.json');
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

export function isRunSealed(runId) {
  const runDir = runDirFor(runId);
  const sealPath = path.join(runDir, SEAL_MARKER_NAME);
  // Application lock is the SEAL marker. Status alone is not sufficient so DRAFT→seal can finish atomically.
  if (fs.existsSync(sealPath)) return true;
  const handoff = (() => {
    try {
      return readHandoff(runId);
    } catch {
      return null;
    }
  })();
  // Legacy run-001: READY without SEAL.json is still treated immutable.
  if (handoff?.schemaVersion === 'GDI_2_HANDOFF_V1' && SEALED_STATUSES.has(handoff.status)) {
    return true;
  }
  if (handoff?.status === 'SEALED') return true;
  return false;
}

export function assertRunMutable(runId, { action = 'modify' } = {}) {
  if (!isRunSealed(runId)) return { ok: true };
  return {
    ok: false,
    error: `Refusing to ${action} sealed/immutable run ${runId}. Create a new run or a separate review record under reviews/.`,
  };
}

function walkRelativeFiles(dir, base = dir, out = []) {
  for (const name of fs.readdirSync(dir)) {
    if (name === MANIFEST_NAME || name === SEAL_MARKER_NAME) continue;
    // Validation reports must never live inside sealed evidence; skip if present historically
    if (name === 'validation-reports') continue;
    const abs = path.join(dir, name);
    const st = fs.statSync(abs);
    if (st.isDirectory()) walkRelativeFiles(abs, base, out);
    else out.push(path.relative(base, abs).replace(/\\/g, '/'));
  }
  return out.sort();
}

export function hashFileSha256(absPath) {
  const hash = crypto.createHash('sha256');
  hash.update(fs.readFileSync(absPath));
  return hash.digest('hex');
}

/**
 * Build SHA-256 manifest lines: "<hash>  <relativePath>"
 */
export function buildManifest(runId) {
  const runDir = runDirFor(runId);
  const files = walkRelativeFiles(runDir);
  const lines = files.map((rel) => {
    const digest = hashFileSha256(path.join(runDir, rel));
    return `${digest}  ${rel}`;
  });
  return {
    algorithm: 'sha256',
    files,
    text: `${lines.join('\n')}\n`,
    lines,
  };
}

export function writeManifest(runId) {
  const mutable = assertRunMutable(runId, { action: 'write manifest before seal' });
  // During seal we allow writing manifest once before marker exists — caller uses sealRun.
  const runDir = runDirFor(runId);
  const manifest = buildManifest(runId);
  const target = path.join(runDir, MANIFEST_NAME);
  atomicWriteFile(target, manifest.text);
  return manifest;
}

export function verifyManifest(runId) {
  const runDir = runDirFor(runId);
  const manifestPath = path.join(runDir, MANIFEST_NAME);
  if (!fs.existsSync(manifestPath)) {
    return { ok: false, errors: [`missing ${MANIFEST_NAME}`] };
  }
  const text = fs.readFileSync(manifestPath, 'utf8');
  const errors = [];
  const expected = new Map();
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim()) continue;
    const m = /^([a-f0-9]{64})  (.+)$/.exec(line);
    if (!m) {
      errors.push(`malformed manifest line: ${line}`);
      continue;
    }
    expected.set(m[2], m[1]);
  }
  const actualFiles = walkRelativeFiles(runDir);
  for (const rel of actualFiles) {
    if (!expected.has(rel)) errors.push(`file present but not in manifest: ${rel}`);
    else {
      const digest = hashFileSha256(path.join(runDir, rel));
      if (digest !== expected.get(rel)) errors.push(`hash mismatch: ${rel}`);
    }
  }
  for (const rel of expected.keys()) {
    if (!actualFiles.includes(rel)) errors.push(`manifest lists missing file: ${rel}`);
  }
  return { ok: errors.length === 0, errors, fileCount: actualFiles.length };
}

export function atomicWriteFile(targetPath, contents) {
  const dir = path.dirname(targetPath);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, `.tmp-${path.basename(targetPath)}-${process.pid}-${Date.now()}`);
  fs.writeFileSync(tmp, contents, 'utf8');
  fs.renameSync(tmp, targetPath);
}

/**
 * Atomically write multiple files into a run via staging directory, then rename.
 * Refuses if destination run is already sealed.
 */
export function atomicWriteRunPackage(runId, filesMap) {
  const gate = assertRunMutable(runId, { action: 'write package into' });
  if (!gate.ok) {
    const err = new Error(gate.error);
    err.code = 'RUN_SEALED';
    throw err;
  }
  const runDir = runDirFor(runId);
  const parent = path.dirname(runDir);
  fs.mkdirSync(parent, { recursive: true });
  const staging = path.join(parent, `.staging-${runId}-${process.pid}-${Date.now()}`);
  fs.mkdirSync(staging, { recursive: true });
  try {
    for (const [rel, body] of Object.entries(filesMap)) {
      const abs = path.join(staging, rel);
      fs.mkdirSync(path.dirname(abs), { recursive: true });
      fs.writeFileSync(abs, body, 'utf8');
    }
    if (fs.existsSync(runDir)) {
      // Merge: copy existing non-conflicting? For seal/finalize we replace listed files only into existing dir.
      for (const [rel, body] of Object.entries(filesMap)) {
        const abs = path.join(runDir, rel);
        atomicWriteFile(abs, body);
      }
      fs.rmSync(staging, { recursive: true, force: true });
    } else {
      fs.renameSync(staging, runDir);
    }
  } catch (e) {
    try {
      fs.rmSync(staging, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
    throw e;
  }
}

/**
 * Seal a completed run: write MANIFEST + SEAL application lock.
 * SEAL.json is a process lock only — not cryptographic immutability.
 * Callers MUST also write an external integrity anchor via integrity-anchor.js
 * (retained outside the sealed run directory).
 */
export function sealRun(runId, { handoff, sealedAt = new Date().toISOString() } = {}) {
  if (isRunSealed(runId) && fs.existsSync(path.join(runDirFor(runId), SEAL_MARKER_NAME))) {
    const err = new Error(`Run ${runId} is already sealed`);
    err.code = 'RUN_SEALED';
    throw err;
  }

  const runDir = runDirFor(runId);
  if (!fs.existsSync(runDir)) {
    throw new Error(`Run directory missing: ${runId}`);
  }

  const externalAnchorPath = `reports/gdi-2-review-loop/integrity-anchors/${runId}.integrity.json`;
  const applicationLockDisclaimer =
    'SEAL.json is an application-level process lock only. It is NOT cryptographic immutability. Tampering detection relies on independently retained digests in integrity-anchors/.';

  // Write final handoff first (still mutable until SEAL marker)
  if (handoff) {
    const next = {
      ...handoff,
      status: handoff.status === 'DRAFT' || handoff.status === 'EVIDENCE_INCOMPLETE'
        ? 'READY_FOR_CHATGPT_REVIEW'
        : handoff.status,
      integrity: {
        ...(handoff.integrity || {}),
        manifestPath: MANIFEST_NAME,
        sealedAt,
        sealAlgorithm: 'sha256',
        applicationLockDisclaimer,
        externalAnchorPath,
        packageRootSha256: null,
      },
    };
    if (!SEALED_STATUSES.has(next.status)) {
      next.status = 'READY_FOR_CHATGPT_REVIEW';
    }
    atomicWriteFile(path.join(runDir, 'HANDOFF.json'), `${JSON.stringify(next, null, 2)}\n`);
  }

  const manifest = buildManifest(runId);
  atomicWriteFile(path.join(runDir, MANIFEST_NAME), manifest.text);

  const seal = {
    runId,
    sealedAt,
    algorithm: 'sha256',
    manifestPath: MANIFEST_NAME,
    fileCount: manifest.files.length,
    policy: 'APPLICATION_LOCK_V1_NOT_CRYPTOGRAPHIC',
    note: 'Application-level lock only. Do not modify this run. Corrections require a new run or reviews/<runId>-*.json. External verification uses integrity-anchors/.',
  };
  atomicWriteFile(path.join(runDir, SEAL_MARKER_NAME), `${JSON.stringify(seal, null, 2)}\n`);

  return {
    ok: true,
    seal,
    manifestFileCount: manifest.files.length,
    externalAnchorPath,
    applicationLockDisclaimer,
    nextStep: 'writeIntegrityAnchor(runId) — external digest retained outside sealed run',
  };
}

/**
 * Attempt to write a file into a run — used by tests to prove overwrite is blocked.
 */
export function tryWriteIntoRun(runId, relativePath, contents) {
  const gate = assertRunMutable(runId, { action: 'write' });
  if (!gate.ok) {
    return { ok: false, blocked: true, error: gate.error };
  }
  const abs = path.join(runDirFor(runId), relativePath);
  atomicWriteFile(abs, contents);
  return { ok: true, blocked: false };
}
