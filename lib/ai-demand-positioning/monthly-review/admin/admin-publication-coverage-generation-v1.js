/**
 * Admin publication coverage generation token.
 * Bumped after successful authorized publication so Admin clients can detect freshness
 * without manual metadata reconciliation. Coverage API already sends Cache-Control: no-store.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ADP_ADMIN_PUBLICATION_COVERAGE_GENERATION_V1 =
  "ADP_ADMIN_PUBLICATION_COVERAGE_GENERATION_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");
const GEN_PATH = path.join(
  ROOT,
  "data/ai-demand-positioning/admin-publication-coverage-generation.json"
);

export function getAdminPublicationCoverageGeneration() {
  try {
    if (!fs.existsSync(GEN_PATH)) {
      return {
        contract: ADP_ADMIN_PUBLICATION_COVERAGE_GENERATION_V1,
        generation: 0,
        updatedAt: null,
        lastPropertyId: null,
        lastPeriodId: null,
      };
    }
    const doc = JSON.parse(fs.readFileSync(GEN_PATH, "utf8"));
    return {
      contract: ADP_ADMIN_PUBLICATION_COVERAGE_GENERATION_V1,
      generation: Number(doc.generation || 0),
      updatedAt: doc.updatedAt || null,
      lastPropertyId: doc.lastPropertyId || null,
      lastPeriodId: doc.lastPeriodId || null,
    };
  } catch {
    return {
      contract: ADP_ADMIN_PUBLICATION_COVERAGE_GENERATION_V1,
      generation: 0,
      updatedAt: null,
      lastPropertyId: null,
      lastPeriodId: null,
    };
  }
}

/** Call after a successful authorized publication (local or production). */
export function bumpAdminPublicationCoverageGeneration({
  propertyId = null,
  periodId = null,
  reason = "publication",
} = {}) {
  const prev = getAdminPublicationCoverageGeneration();
  const next = {
    contract: ADP_ADMIN_PUBLICATION_COVERAGE_GENERATION_V1,
    generation: Number(prev.generation || 0) + 1,
    updatedAt: new Date().toISOString(),
    lastPropertyId: propertyId || prev.lastPropertyId || null,
    lastPeriodId: periodId || prev.lastPeriodId || null,
    reason: String(reason || "publication"),
  };
  fs.mkdirSync(path.dirname(GEN_PATH), { recursive: true });
  fs.writeFileSync(GEN_PATH, `${JSON.stringify(next, null, 2)}\n`);
  return next;
}
