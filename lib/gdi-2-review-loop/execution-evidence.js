import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { ROOT } from './constants.js';
import { atomicWriteFile, runDirFor } from './immutability.js';
import { resolveEvidencePath } from './evidence.js';

export const EVIDENCE_ORIGINS = new Set([
  'independent_execution',
  'imported',
  'manual_assertion',
]);

/**
 * Run a process and persist raw execution evidence under the run evidence folder.
 * Returns a HANDOFF.tests-compatible object plus paths to stdout/stderr artifacts.
 */
export function captureIndependentExecution({
  runId,
  label,
  command,
  args = [],
  cwd = ROOT,
  shell = false,
  env = process.env,
} = {}) {
  if (!runId || !label || !command) {
    throw new Error('captureIndependentExecution requires runId, label, command');
  }

  const evidenceDir = path.join(runDirFor(runId), 'evidence', 'executions', label);
  fs.mkdirSync(evidenceDir, { recursive: true });

  const started = Date.now();
  const executedAt = new Date(started).toISOString();
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    env,
    shell,
    maxBuffer: 8 * 1024 * 1024,
  });
  const ended = Date.now();
  const exitCode = result.status === null ? 1 : result.status;
  const stdout = String(result.stdout || '');
  const stderr = String(result.stderr || '');

  const stdoutRel = `reports/gdi-2-review-loop/runs/${runId}/evidence/executions/${label}/stdout.txt`;
  const stderrRel = `reports/gdi-2-review-loop/runs/${runId}/evidence/executions/${label}/stderr.txt`;
  const recordRel = `reports/gdi-2-review-loop/runs/${runId}/evidence/executions/${label}/execution.json`;

  atomicWriteFile(path.join(ROOT, stdoutRel), stdout);
  atomicWriteFile(path.join(ROOT, stderrRel), stderr);

  const record = {
    contractId: 'GDI_2_EXECUTION_RECORD_V1',
    evidenceOrigin: 'independent_execution',
    label,
    command,
    args,
    shell: Boolean(shell),
    cwd: path.resolve(cwd),
    exitCode,
    executedAt,
    durationMs: ended - started,
    runtime: {
      nodeVersion: process.version,
      platform: process.platform,
      arch: process.arch,
      pid: process.pid,
    },
    stdoutArtifact: stdoutRel,
    stderrArtifact: stderrRel,
    stdoutBytes: Buffer.byteLength(stdout, 'utf8'),
    stderrBytes: Buffer.byteLength(stderr, 'utf8'),
    signal: result.signal || null,
    error: result.error ? String(result.error.message || result.error) : null,
  };

  atomicWriteFile(path.join(ROOT, recordRel), `${JSON.stringify(record, null, 2)}\n`);

  return {
    command: [command, ...args].join(' ').trim(),
    exitCode,
    evidencePath: recordRel,
    executionRecordPath: recordRel,
    evidenceOrigin: 'independent_execution',
    claimedPass: exitCode === 0,
    executedAt,
    result: exitCode === 0 ? 'pass' : 'fail',
    notes: `independent execution label=${label}`,
    record,
  };
}

/**
 * Validate HANDOFF test entry against its execution record when origin requires it.
 */
export function validateExecutionConsistency(test, index, runId) {
  const errors = [];
  const warnings = [];
  const label = `tests[${index}]`;

  const origin = test?.evidenceOrigin || null;
  if (!origin) {
    errors.push(`${label}.evidenceOrigin required (independent_execution|imported|manual_assertion)`);
    return { errors, warnings };
  }
  if (!EVIDENCE_ORIGINS.has(origin)) {
    errors.push(`${label}.evidenceOrigin invalid: ${origin}`);
    return { errors, warnings };
  }

  if (origin === 'manual_assertion' && test.claimedPass === true) {
    errors.push(
      `${label}: fabricated/unsupported success — manual_assertion cannot claim claimedPass=true without independent execution`,
    );
  }

  if (origin === 'imported' && test.claimedPass === true) {
    warnings.push(`${label}: imported evidence claimed pass — ensure provenance is documented`);
  }

  if (origin !== 'independent_execution') {
    return { errors, warnings };
  }

  if (!test.executionRecordPath) {
    errors.push(`${label}.executionRecordPath required for independent_execution`);
    return { errors, warnings };
  }

  const resolved = resolveEvidencePath(test.executionRecordPath, runId);
  if (!resolved.ok) {
    errors.push(...resolved.errors.map((e) => `${label}: ${e}`));
    return { errors, warnings };
  }

  let record;
  try {
    record = JSON.parse(fs.readFileSync(resolved.absPath, 'utf8'));
  } catch (e) {
    errors.push(`${label}: cannot parse execution record (${e.message})`);
    return { errors, warnings };
  }

  if (record.contractId !== 'GDI_2_EXECUTION_RECORD_V1') {
    errors.push(`${label}: execution record contract mismatch`);
  }
  if (record.evidenceOrigin !== 'independent_execution') {
    errors.push(`${label}: execution record origin mismatch`);
  }
  if (typeof record.exitCode !== 'number') {
    errors.push(`${label}: execution record missing exitCode`);
  }
  if (test.exitCode !== record.exitCode) {
    errors.push(
      `${label}: contradictory report — handoff exitCode=${test.exitCode} vs execution record exitCode=${record.exitCode}`,
    );
  }
  if (test.claimedPass === true && record.exitCode !== 0) {
    errors.push(
      `${label}: fabricated success — claimedPass=true but execution record exitCode=${record.exitCode}`,
    );
  }
  if (test.result === 'pass' && record.exitCode !== 0) {
    errors.push(
      `${label}: contradictory result=pass vs execution record exitCode=${record.exitCode}`,
    );
  }
  if (!record.stdoutArtifact || !record.stderrArtifact) {
    errors.push(`${label}: execution record missing stdout/stderr artifact refs`);
  } else {
    for (const art of [record.stdoutArtifact, record.stderrArtifact]) {
      const r = resolveEvidencePath(art, runId);
      if (!r.ok || !fs.existsSync(r.absPath)) {
        errors.push(`${label}: missing stdout/stderr artifact ${art}`);
      }
    }
  }
  if (!record.cwd || !record.executedAt || !record.runtime?.nodeVersion) {
    errors.push(`${label}: execution record missing cwd/executedAt/runtime`);
  }

  // Command agreement (handoff command should reference recorded command)
  if (record.command && test.command && !String(test.command).includes(String(record.command))) {
    // allow handoff to include npm wrappers; require at least arg overlap or exact match alternative
    const recFull = [record.command, ...(record.args || [])].join(' ');
    if (!String(test.command).includes(recFull) && recFull !== test.command) {
      warnings.push(`${label}: handoff command text differs from execution record command`);
    }
  }

  return { errors, warnings };
}
