/**
 * Pilot Delivery — Brand & Operator Explorer coverage (FPP steps 5–17).
 * Inventory source: npm run choice-brand-explorer:manifest + Kimpton/Curio fixtures.
 */
import { listChoiceBrandManifest } from "../../scripts/lib/choice-brand-explorer-manifest.mjs";

const COMPLETED_DATE = "2026-07-03";

/** @typedef {{ status: string, progress: string, completedDate?: string, nextAction: string }} ExplorerTaskTarget */

function ip(progress, nextAction) {
  return { status: "In Progress", progress, nextAction };
}

function needsReview(progress, nextAction) {
  return { status: "Needs Review", progress, nextAction };
}

function done(nextAction) {
  return {
    status: "Completed",
    progress: "100%",
    completedDate: COMPLETED_DATE,
    nextAction: nextAction || "No action required — segment baseline complete for pilot.",
  };
}

/**
 * Live CHI manifest summary (22 brands).
 * @returns {{ l2Complete: string[], l1Generated: string[], needsEnrichment: string[] }}
 */
export function summarizeChoiceBrandCoverage() {
  const rows = listChoiceBrandManifest();
  return {
    l2Complete: rows.filter((b) => b.parity === "complete").map((b) => b.profileName),
    l1Generated: rows.filter((b) => b.parity === "generated").map((b) => b.profileName),
    needsEnrichment: rows.filter((b) => b.parity === "needs-enrichment").map((b) => b.profileName),
    total: rows.length,
  };
}

/** Non-CHI pilot brands with enriched Explorer presentation fixtures. */
export const NON_CHI_ENRICHED_BRANDS = ["Kimpton", "Curio Collection by Hilton"];

/**
 * FPP updates keyed by Airtable record id (Pilot Delivery explorer tasks).
 * @type {Record<string, ExplorerTaskTarget>}
 */
export const EXPLORER_COVERAGE_BY_RECORD_ID = {
  /** Master completion tracker */
  recHls6zriLafoqJT: needsReview(
    "11%",
    "Master tracker: 30/271 taxonomy brands in repo. 8 chain-scale brand tasks (steps 6–13) + 4 operator tasks (14–17). Report: reports/brand-operator-explorer-coverage-tracker.md."
  ),

  /** Operator — Third-party / institutional */
  recsyq5eyLYQq3nwW: ip(
    "50%",
    "Operator Explorer live (list + gold-mock profile). Active third-party operators in new-base with submission_status Active — continue Operator Setup → Explorer profile hydration for pilot operators."
  ),

  /** Operator — Regional / CALA / owner-operators */
  recGA1BQsbLFBVuEA: ip(
    "45%",
    "HE CALA + regional operators partially in Operator Setup new-base. Complete DNA/Explorer tabs for CALA pilot operators (Mexico, Caribbean, Central America)."
  ),

  /** Operator — Resort / all-inclusive */
  recdkY3nfe3VDDqY5: ip(
    "15%",
    "Resort/all-inclusive operator profiles not yet prioritized. Identify 2–3 CALA resort operators for pilot if demo requires."
  ),

  /** Operator — Lifestyle / boutique */
  rec85eaHTdGcgQrEe: ip(
    "20%",
    "Lifestyle/boutique operator subset overlaps CALA independents — map after regional operator batch (step 15)."
  ),
};

/**
 * @param {object} fields — Airtable Founder Project Plan fields
 * @returns {{ patch: object, target: ExplorerTaskTarget, recordId: string } | null}
 */
export function buildExplorerCoveragePatch(recordId, fields) {
  const target = EXPLORER_COVERAGE_BY_RECORD_ID[recordId];
  if (!target) return null;

  const F = {
    status: "Status",
    progress: "Progress",
    nextAction: "Next Action",
    completedDate: "Completed Date",
  };

  const patch = {};
  const existing = fields || {};

  if (existing[F.status] !== target.status) patch[F.status] = target.status;
  if (existing[F.progress] !== target.progress) patch[F.progress] = target.progress;
  if (existing[F.nextAction] !== target.nextAction) patch[F.nextAction] = target.nextAction;

  if (target.status === "Completed" && target.completedDate) {
    if (!existing[F.completedDate]) patch[F.completedDate] = target.completedDate;
  } else if (target.status !== "Completed" && existing[F.completedDate]) {
    patch[F.completedDate] = null;
  }

  if (Object.keys(patch).length === 0) return null;
  return { patch, target, recordId };
}

export function listExplorerCoverageRecordIds() {
  return Object.keys(EXPLORER_COVERAGE_BY_RECORD_ID);
}
