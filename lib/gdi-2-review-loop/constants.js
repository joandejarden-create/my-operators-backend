import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const ROOT = path.resolve(__dirname, '..', '..');
export const LOOP_ROOT = path.join(ROOT, 'reports', 'gdi-2-review-loop');
export const RUNS_DIR = path.join(LOOP_ROOT, 'runs');
export const REVIEWS_DIR = path.join(LOOP_ROOT, 'reviews');
export const VALIDATION_REPORTS_DIR = path.join(LOOP_ROOT, 'validation-reports');
export const SCHEMA_V1_2_PATH = path.join(LOOP_ROOT, 'schema', 'HANDOFF.schema.json');
export const SCHEMA_V1_1_LEGACY_PATH = path.join(LOOP_ROOT, 'schema', 'HANDOFF.schema.v1-1-legacy.json');
export const SCHEMA_V1_LEGACY_PATH = path.join(LOOP_ROOT, 'schema', 'HANDOFF.schema.v1-legacy.json');
/** @deprecated alias — current schema path */
export const SCHEMA_V1_1_PATH = SCHEMA_V1_2_PATH;

export const CONTRACT_ID = 'GDI_2_CHATGPT_REVIEW_LOOP_V1';
export const SCHEMA_VERSION = 'GDI_2_HANDOFF_V1_2';
export const SCHEMA_VERSION_V1_1 = 'GDI_2_HANDOFF_V1_1';
export const SCHEMA_VERSION_LEGACY = 'GDI_2_HANDOFF_V1';

export const INTEGRITY_ANCHORS_DIRNAME = 'integrity-anchors';

/** Statuses that lock the run package against mutation. */
export const SEALED_STATUSES = new Set([
  'READY_FOR_CHATGPT_REVIEW',
  'SEALED',
  'CHATGPT_REVIEW_IN_PROGRESS',
  'CHATGPT_REVIEW_COMPLETE',
  'NEXT_PROMPT_APPROVED',
  'SUPERSEDED',
]);

export const REQUIRED_RUN_FILES = [
  'IMPLEMENTATION_SUMMARY.md',
  'CHANGED_FILES.md',
  'TEST_RESULTS.md',
  'OPEN_ISSUES.md',
  'NEXT_RECOMMENDATIONS.md',
  'HANDOFF.json',
];

export const MANIFEST_NAME = 'MANIFEST.sha256';
export const SEAL_MARKER_NAME = 'SEAL.json';

/** Max evidence/artifact file size for export/sync (bytes). */
export const MAX_ARTIFACT_BYTES = 2 * 1024 * 1024;

/** Approved relative roots for evidence paths (posix). */
export const APPROVED_EVIDENCE_ROOTS = [
  'reports/gdi-2-review-loop/runs/',
];
