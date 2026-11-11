import fs from 'node:fs';
import path from 'node:path';
import {
  LOOP_ROOT,
  REQUIRED_RUN_FILES,
  SEALED_STATUSES,
  SCHEMA_VERSION,
  SCHEMA_VERSION_V1_1,
  VALIDATION_REPORTS_DIR,
  MANIFEST_NAME,
  SEAL_MARKER_NAME,
} from './constants.js';
import { validateHandoffSchema } from './schema-validate.js';
import { validateTestEvidence, validateTestResultsMarkdownConsistency } from './evidence.js';
import { validateExecutionConsistency } from './execution-evidence.js';
import { scanRunPackageForSecrets } from './secrets-scan.js';
import { isRunSealed, verifyManifest, runDirFor, atomicWriteFile } from './immutability.js';
import { verifyIntegrityAnchor } from './integrity-anchor.js';

function readUtf8(p) {
  return fs.readFileSync(p, 'utf8');
}

/**
 * Full run validation. Never writes into a sealed run directory.
 * Optional report goes to reports/gdi-2-review-loop/validation-reports/.
 */
export function validateRun(runId, {
  requireReady = false,
  requireComplete = false,
  writeReport = true,
  requireSealArtifacts = false,
} = {}) {
  const errors = [];
  const warnings = [];
  const runDir = runDirFor(runId);

  if (!fs.existsSync(runDir)) {
    return {
      ok: false,
      runId,
      errors: [`Missing run directory: ${path.relative(LOOP_ROOT, runDir).replace(/\\/g, '/')}`],
      warnings,
      validatedAt: new Date().toISOString(),
    };
  }

  for (const name of REQUIRED_RUN_FILES) {
    if (!fs.existsSync(path.join(runDir, name))) {
      errors.push(`Missing required artifact: ${name}`);
    }
  }

  let handoff = null;
  const handoffPath = path.join(runDir, 'HANDOFF.json');
  if (fs.existsSync(handoffPath)) {
    try {
      handoff = JSON.parse(readUtf8(handoffPath));
    } catch (e) {
      errors.push(`HANDOFF.json parse error: ${e.message}`);
    }
  }

  let schemaResult = null;
  if (handoff) {
    schemaResult = validateHandoffSchema(handoff);
    if (!schemaResult.ok) {
      for (const e of schemaResult.errors) {
        errors.push(`schema: ${e.instancePath || '/'} ${e.message}`);
      }
    }
    if (handoff.runId !== runId) {
      errors.push(`runId mismatch: handoff=${handoff.runId} arg=${runId}`);
    }

    // Git / gate completeness for current + recent schemas
    if (handoff.schemaVersion === SCHEMA_VERSION || handoff.schemaVersion === SCHEMA_VERSION_V1_1) {
      if (!handoff.git?.repository) errors.push('git.repository required');
      if (typeof handoff.git?.dirtyTree !== 'boolean') errors.push('git.dirtyTree required');
      if (!handoff.git?.headSha || String(handoff.git.headSha).length < 7) {
        errors.push('git.headSha missing or too short');
      }
      if (!handoff.environment?.nodeVersion) errors.push('environment.nodeVersion required');
      if (!handoff.reproducibility?.validateCommands?.length) {
        errors.push('reproducibility.validateCommands required');
      }
      if (!handoff.phaseGate || handoff.phaseGate.blockNextMajorPhase !== true) {
        if (SEALED_STATUSES.has(handoff.status) || requireReady) {
          errors.push('phaseGate.blockNextMajorPhase must be true until explicit approved unlock');
        }
      }
      if (handoff.phaseGate?.nextPhaseUnlocked === true) {
        errors.push(
          'unauthorized phase advancement: handoff.phaseGate.nextPhaseUnlocked must remain false; unlock only via approved reviews/ decision + APPROVED NEXT_CURSOR_PROMPT.md',
        );
      }
    }

    if (handoff.schemaVersion === SCHEMA_VERSION) {
      if (!handoff.integrity?.applicationLockDisclaimer) {
        errors.push('integrity.applicationLockDisclaimer required');
      } else if (/cryptographically immutable/i.test(handoff.integrity.applicationLockDisclaimer)) {
        errors.push('integrity disclaimer must not claim cryptographic immutability for SEAL locks');
      }
      if (!handoff.integrity?.externalAnchorPath) {
        errors.push('integrity.externalAnchorPath required for V1.2');
      }
    }

    if (Array.isArray(handoff.tests)) {
      for (const [i, t] of handoff.tests.entries()) {
        const te = validateTestEvidence(t, i, runId);
        errors.push(...te.errors);
        warnings.push(...te.warnings);
        if (handoff.schemaVersion === SCHEMA_VERSION) {
          const ex = validateExecutionConsistency(t, i, runId);
          errors.push(...ex.errors);
          warnings.push(...ex.warnings);
        }
      }
    }

    if (fs.existsSync(path.join(runDir, 'TEST_RESULTS.md')) && handoff.tests) {
      const md = readUtf8(path.join(runDir, 'TEST_RESULTS.md'));
      const cons = validateTestResultsMarkdownConsistency(handoff, md);
      errors.push(...cons.errors);
    }

    if (handoff.approvals?.paidResearch === true) {
      warnings.push('paidResearch=true — confirm founder authorization');
    }
    if (handoff.approvals?.productionWrites === true) {
      warnings.push('productionWrites=true — confirm founder authorization');
    }

    if (requireReady) {
      if (handoff.status !== 'READY_FOR_CHATGPT_REVIEW' && handoff.status !== 'SEALED') {
        errors.push(`--require-ready but status=${handoff.status}`);
      }
    }

    const mustBeComplete =
      requireComplete ||
      handoff.status === 'READY_FOR_CHATGPT_REVIEW' ||
      handoff.status === 'SEALED';
    if (mustBeComplete) {
      if (!Array.isArray(handoff.changedPaths) || handoff.changedPaths.length === 0) {
        errors.push('complete package requires non-empty changedPaths');
      }
      if (!Array.isArray(handoff.tests) || handoff.tests.length === 0) {
        errors.push('complete package requires at least one test entry');
      }
      if (!Array.isArray(handoff.nextRecommendations) || handoff.nextRecommendations.length === 0) {
        errors.push('complete package requires nextRecommendations');
      }
    }
  }

  const sealed = isRunSealed(runId);
  if (sealed || requireSealArtifacts) {
    if (!fs.existsSync(path.join(runDir, MANIFEST_NAME))) {
      errors.push(`sealed run missing ${MANIFEST_NAME}`);
    } else {
      const mv = verifyManifest(runId);
      if (!mv.ok) errors.push(...mv.errors.map((e) => `manifest: ${e}`));
    }
    if (!fs.existsSync(path.join(runDir, SEAL_MARKER_NAME))) {
      // READY without SEAL.json is allowed for legacy run-001 only
      if (handoff?.schemaVersion === SCHEMA_VERSION || handoff?.schemaVersion === SCHEMA_VERSION_V1_1) {
        errors.push(`missing ${SEAL_MARKER_NAME} for sealed package`);
      } else {
        warnings.push(`legacy run without ${SEAL_MARKER_NAME}`);
      }
    }
    if (handoff?.schemaVersion === SCHEMA_VERSION) {
      const anchor = verifyIntegrityAnchor(runId);
      if (!anchor.ok) {
        errors.push(...anchor.errors.map((e) => `integrity-anchor: ${e}`));
      }
    }
  }

  const secretScan = scanRunPackageForSecrets(runDir);
  if (!secretScan.ok) {
    errors.push(...secretScan.errors.map((e) => `security: ${e}`));
  }
  warnings.push(...(secretScan.warnings || []));

  const report = {
    ok: errors.length === 0,
    runId,
    status: handoff?.status || null,
    schemaVersion: handoff?.schemaVersion || null,
    schemaDriven: schemaResult?.schemaDriven === true,
    sealed,
    errorCount: errors.length,
    warningCount: warnings.length,
    errors,
    warnings,
    secretScan: {
      ok: secretScan.ok,
      filesScanned: secretScan.filesScanned,
      blockingForExternalSync: !secretScan.ok,
    },
    validatedAt: new Date().toISOString(),
  };

  if (writeReport) {
    fs.mkdirSync(VALIDATION_REPORTS_DIR, { recursive: true });
    const stamp = report.validatedAt.replace(/[:.]/g, '-');
    const outPath = path.join(VALIDATION_REPORTS_DIR, `${runId}-${stamp}.json`);
    // Never write into sealed run evidence.
    atomicWriteFile(outPath, `${JSON.stringify(report, null, 2)}\n`);
    report.reportPath = path.relative(LOOP_ROOT, outPath).replace(/\\/g, '/');
    // For unsealed draft runs, optionally mirror into evidence for author convenience
    if (!sealed) {
      const evidenceMirror = path.join(runDir, 'evidence', 'validate-handoff-report.json');
      fs.mkdirSync(path.dirname(evidenceMirror), { recursive: true });
      atomicWriteFile(evidenceMirror, `${JSON.stringify(report, null, 2)}\n`);
      report.evidenceMirrorPath = path.relative(runDir, evidenceMirror).replace(/\\/g, '/');
    }
  }

  return report;
}
