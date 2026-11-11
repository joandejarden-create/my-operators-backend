import { spawnSync } from 'node:child_process';
import { ROOT } from './constants.js';

function git(args) {
  const r = spawnSync('git', args, { cwd: ROOT, encoding: 'utf8' });
  return {
    ok: r.status === 0,
    stdout: String(r.stdout || '').trim(),
    stderr: String(r.stderr || '').trim(),
    status: r.status ?? 1,
  };
}

function safeCmd(command, args) {
  const r = spawnSync(command, args, { cwd: ROOT, encoding: 'utf8', shell: false });
  return {
    ok: r.status === 0,
    stdout: String(r.stdout || '').trim(),
    status: r.status ?? 1,
  };
}

/**
 * Capture git + runtime metadata for handoff reproducibility.
 */
export function captureReproducibilityMeta() {
  const branch = git(['rev-parse', '--abbrev-ref', 'HEAD']);
  const sha = git(['rev-parse', 'HEAD']);
  const dirty = git(['status', '--porcelain']);
  const remote = git(['config', '--get', 'remote.origin.url']);
  const top = git(['rev-parse', '--show-toplevel']);
  const npmV = safeCmd('npm', ['-v']);

  const dirtyTree = dirty.ok && dirty.stdout.length > 0;
  const changedFilesInventory = [];
  if (dirty.ok && dirty.stdout) {
    for (const line of dirty.stdout.split(/\r?\n/)) {
      if (!line.trim()) continue;
      const status = line.slice(0, 2).trim();
      const filePath = line.slice(3).trim().replace(/\\/g, '/');
      changedFilesInventory.push({ path: filePath, status: status || '?' });
    }
  }

  return {
    git: {
      repository: top.ok ? top.stdout.replace(/\\/g, '/') : 'unknown',
      remoteUrl: remote.ok ? remote.stdout : null,
      branch: branch.ok ? branch.stdout : 'unknown',
      headSha: sha.ok ? sha.stdout : 'unknown',
      dirtyTree,
      changedFilesInventory,
      commitRefWhenAvailable: null,
      syncMechanism: 'not_authorized',
    },
    environment: {
      nodeVersion: process.version,
      npmVersion: npmV.ok ? npmV.stdout : null,
      platform: process.platform,
      arch: process.arch,
    },
  };
}

export function defaultReproducibilityBlock(runId) {
  return {
    validateCommands: [
      `npm run gdi2:review-loop:validate -- --run ${runId} --require-ready --no-write-report`,
      'npm run test:gdi-2-review-loop-harden-v1',
    ],
    npmScripts: [
      'gdi2:review-loop:scaffold',
      'gdi2:review-loop:validate',
      'gdi2:review-loop:seal',
      'test:gdi-2-review-loop-harden-v1',
    ],
    assumptions: [
      'Node.js 18+ with local node_modules including ajv@8 and ajv-formats',
      'Git available on PATH for metadata capture',
      'No paid providers, production writes, or deploys required to validate',
      'Dirty working tree may exist; validation does not mutate git',
    ],
    independentReviewerSteps: [
      'Open reports/gdi-2-review-loop/CURRENT_HANDOFF.md and the referenced run folder',
      'Confirm SEAL.json and MANIFEST.sha256 exist for sealed runs',
      `Run: npm run gdi2:review-loop:validate -- --run ${runId} --require-ready --no-write-report`,
      'Confirm validate exit code 0 and that validation reports are written outside the sealed run (validation-reports/)',
      'Do not modify sealed run files; file ChatGPT outcomes under reviews/',
      'Do not unlock next major phase unless NEXT_CURSOR_PROMPT.md status is APPROVED and a review decision records unlock explicitly',
    ],
  };
}
