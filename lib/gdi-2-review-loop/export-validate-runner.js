import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { ROOT } from './constants.js';
import { atomicWriteFile } from './immutability.js';

/**
 * Build a durable export-validate helper whose import is rooted at the repository ROOT
 * via an absolute file: URL — never via ../../ traversal from a nested evidence folder.
 */
export function buildExportValidateRunnerSource({ root = ROOT, writeReport = false } = {}) {
  const moduleAbs = path.resolve(root, 'lib', 'gdi-2-review-loop', 'export-scan.js');
  if (!fs.existsSync(moduleAbs)) {
    throw new Error(`export-scan module missing at ${moduleAbs}`);
  }
  const moduleHref = pathToFileURL(moduleAbs).href;
  return [
    '/**',
    ' * Auto-generated export validation runner.',
    ' * Import is repository-root-derived (absolute file URL), not relative traversal.',
    ` * root: ${root.replace(/\\/g, '/')}`,
    ` * module: ${moduleAbs.replace(/\\/g, '/')}`,
    ' */',
    `import { validateExportPackage } from ${JSON.stringify(moduleHref)};`,
    `const r = validateExportPackage({ writeReport: ${writeReport ? 'true' : 'false'} });`,
    'if (!r.ok) {',
    '  console.error(JSON.stringify({ ok: false, errors: r.errors.slice(0, 20) }, null, 2));',
    '  process.exit(1);',
    '}',
    'console.log(JSON.stringify({ ok: true, fileCount: r.fileCount, reportPath: r.reportPath || null }));',
    'process.exit(0);',
    '',
  ].join('\n');
}

/**
 * Write the helper under a run evidence folder (or any destination path).
 */
export function writeExportValidateRunner(destAbsPath, opts = {}) {
  const source = buildExportValidateRunnerSource(opts);
  atomicWriteFile(destAbsPath, source);
  return { destAbsPath, source, moduleHref: pathToFileURL(path.resolve(opts.root || ROOT, 'lib', 'gdi-2-review-loop', 'export-scan.js')).href };
}

/**
 * Fragile relative import used historically (incorrect from runs/.../evidence/).
 * Kept only for regression tests proving it must not be used.
 */
export function buildFragileRelativeExportValidateRunnerSource() {
  return [
    "import { validateExportPackage } from '../../../../../../lib/gdi-2-review-loop/export-scan.js';",
    'const r = validateExportPackage({ writeReport: false });',
    'if (!r.ok) { console.error(JSON.stringify(r.errors.slice(0, 20), null, 2)); process.exit(1); }',
    'console.log(JSON.stringify({ ok: true, fileCount: r.fileCount }));',
    'process.exit(0);',
    '',
  ].join('\n');
}
