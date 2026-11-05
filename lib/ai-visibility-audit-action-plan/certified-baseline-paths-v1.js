/**
 * Paths for lean, pinned ADP baselines used by AI Visibility Audit.
 * Production durability follows the ADP monthly-review archive pattern:
 * small JSON artifacts shipped via explicit Railway allowlists — not full staged/.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export const AVA_CERTIFIED_BASELINES_ROOT = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/certified-baselines"
);

export const AVA_CERTIFIED_BASELINES_INDEX = path.join(
  AVA_CERTIFIED_BASELINES_ROOT,
  "index.json"
);

export const AVA_PRODUCTION_ARCHIVE_ROOT = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive"
);

export const AVA_PRODUCTION_ARCHIVE_INDEX = path.join(
  AVA_PRODUCTION_ARCHIVE_ROOT,
  "index.json"
);

/** Runtime + deployable store (allowlisted; mirrors ADP archive durability model). */
export const AVA_PRODUCTION_STORE_ROOT = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/production/store"
);

export function certifiedBaselineFile(propertyId, periodId) {
  return path.join(
    AVA_CERTIFIED_BASELINES_ROOT,
    propertyId,
    `${periodId}.json`
  );
}

export function archivePropertyDir(propertyId) {
  return path.join(AVA_PRODUCTION_ARCHIVE_ROOT, propertyId);
}
