import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import {
  ROOT,
  LOOP_ROOT,
  MANIFEST_NAME,
  SEAL_MARKER_NAME,
} from './constants.js';
import { hashFileSha256, runDirFor, atomicWriteFile, verifyManifest } from './immutability.js';

export const INTEGRITY_ANCHORS_DIR = path.join(LOOP_ROOT, 'integrity-anchors');

const APPLICATION_LOCK_DISCLAIMER =
  'SEAL.json is an application-level process lock only. It is NOT cryptographic immutability. Tampering detection relies on independently retained digests in integrity-anchors/ (and optional future Git commit retention), not filesystem permissions.';

function git(args) {
  const r = spawnSync('git', args, { cwd: ROOT, encoding: 'utf8' });
  return {
    ok: r.status === 0,
    stdout: String(r.stdout || '').trim(),
  };
}

export function sha256Text(text) {
  return crypto.createHash('sha256').update(String(text), 'utf8').digest('hex');
}

export function anchorPathFor(runId) {
  return path.join(INTEGRITY_ANCHORS_DIR, `${runId}.integrity.json`);
}

/**
 * Build an externally retained integrity record for a sealed run.
 * Lives OUTSIDE runs/<runId>/ so altering the sealed folder without updating
 * this record (or regenerating both) is detectable when verified.
 */
export function buildIntegrityAnchor(runId, { anchoredAt = new Date().toISOString() } = {}) {
  const runDir = runDirFor(runId);
  const manifestPath = path.join(runDir, MANIFEST_NAME);
  const sealPath = path.join(runDir, SEAL_MARKER_NAME);
  const handoffPath = path.join(runDir, 'HANDOFF.json');

  if (!fs.existsSync(manifestPath) || !fs.existsSync(sealPath) || !fs.existsSync(handoffPath)) {
    throw new Error(`Cannot anchor ${runId}: missing MANIFEST/SEAL/HANDOFF`);
  }

  const head = git(['rev-parse', 'HEAD']);
  const dirty = git(['status', '--porcelain']);

  const digests = {
    manifestFileSha256: hashFileSha256(manifestPath),
    sealJsonSha256: hashFileSha256(sealPath),
    handoffSha256: hashFileSha256(handoffPath),
  };
  digests.packageRootSha256 = sha256Text(
    [
      `manifest:${digests.manifestFileSha256}`,
      `seal:${digests.sealJsonSha256}`,
      `handoff:${digests.handoffSha256}`,
    ].join('\n'),
  );

  return {
    contractId: 'GDI_2_INTEGRITY_ANCHOR_V1',
    runId,
    anchoredAt,
    retention: 'EXTERNAL_TO_SEALED_RUN',
    applicationLockDisclaimer: APPLICATION_LOCK_DISCLAIMER,
    gitContext: {
      headShaAtSeal: head.ok ? head.stdout : null,
      dirtyTreeAtSeal: head.ok ? Boolean(dirty.stdout) : null,
      note:
        'Git HEAD is contextual metadata for independent retention. A dirty working tree means sealed files may not exist in that commit until an authorized commit/PR retains them.',
    },
    digests,
    sealedPaths: {
      runDir: `reports/gdi-2-review-loop/runs/${runId}/`,
      manifest: `reports/gdi-2-review-loop/runs/${runId}/${MANIFEST_NAME}`,
      seal: `reports/gdi-2-review-loop/runs/${runId}/${SEAL_MARKER_NAME}`,
      handoff: `reports/gdi-2-review-loop/runs/${runId}/HANDOFF.json`,
      anchor: `reports/gdi-2-review-loop/integrity-anchors/${runId}.integrity.json`,
    },
    limitations: [
      'Not asymmetrically signed',
      'Not automatically pushed to GitHub',
      'Application SEAL lock ≠ cryptographic immutability',
      'Automated scanning does not guarantee absence of all confidential information',
    ],
  };
}

export function writeIntegrityAnchor(runId, opts = {}) {
  const anchor = buildIntegrityAnchor(runId, opts);
  fs.mkdirSync(INTEGRITY_ANCHORS_DIR, { recursive: true });
  const abs = anchorPathFor(runId);
  atomicWriteFile(abs, `${JSON.stringify(anchor, null, 2)}\n`);
  return { abs, anchor };
}

/**
 * Verify sealed artifacts still match the external integrity anchor.
 * Also detects "alter content + regenerate MANIFEST" when the external anchor is unchanged.
 */
export function verifyIntegrityAnchor(runId) {
  const abs = anchorPathFor(runId);
  const errors = [];
  if (!fs.existsSync(abs)) {
    return {
      ok: false,
      errors: [`missing external integrity anchor: integrity-anchors/${runId}.integrity.json`],
    };
  }

  let stored;
  try {
    stored = JSON.parse(fs.readFileSync(abs, 'utf8'));
  } catch (e) {
    return { ok: false, errors: [`anchor parse error: ${e.message}`] };
  }

  let live;
  try {
    live = buildIntegrityAnchor(runId, { anchoredAt: stored.anchoredAt });
  } catch (e) {
    return { ok: false, errors: [`cannot rebuild live digests: ${e.message}`] };
  }

  for (const key of ['manifestFileSha256', 'sealJsonSha256', 'handoffSha256', 'packageRootSha256']) {
    if (stored.digests?.[key] !== live.digests?.[key]) {
      errors.push(`integrity mismatch on ${key}: stored=${stored.digests?.[key]} live=${live.digests?.[key]}`);
    }
  }

  // Detect sealed *content* changes even when MANIFEST.sha256 bytes were not regenerated.
  const manifestCheck = verifyManifest(runId);
  if (!manifestCheck.ok) {
    errors.push(...manifestCheck.errors.map((e) => `sealed-content: ${e}`));
  }

  return {
    ok: errors.length === 0,
    errors,
    storedPackageRootSha256: stored.digests?.packageRootSha256 || null,
    livePackageRootSha256: live.digests?.packageRootSha256 || null,
    applicationLockDisclaimer: APPLICATION_LOCK_DISCLAIMER,
  };
}

export { APPLICATION_LOCK_DISCLAIMER };
