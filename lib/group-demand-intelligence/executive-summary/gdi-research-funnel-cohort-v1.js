/**
 * Hotel-scoped research funnel cohort for the customer Research Receipt.
 *
 * Uses durable stage artifacts — never invents intermediate funnel stages,
 * and never substitutes qualified inventory for discovery/research counts.
 *
 * Contract: gdi-research-funnel-cohort-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const GDI_RESEARCH_FUNNEL_COHORT_V1 = "gdi-research-funnel-cohort-v1";

const WYNDHAM_HOTEL_ID = "recnPB3Q02NO8Ma1o";
const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_REPO_ROOT = path.resolve(MODULE_DIR, "../../..");

/** Known durable Wyndham research cohort (locked Full E2E discovery → deep). */
const WYNDHAM_COHORT = {
  hotelId: WYNDHAM_HOTEL_ID,
  discoveryMatrixRel:
    "reports/gdi-wyndham-sd-bayside-paid-live-cert-25usd-v1/fourteen-segment-discovery-matrix.json",
  deepResearchRel:
    "reports/gdi-canonical-full-e2e-v3/recnPB3Q02NO8Ma1o/02-deep-research.json",
  commercialViabilityRel:
    "reports/gdi-canonical-full-e2e-v3/recnPB3Q02NO8Ma1o/02b-commercial-viability-precheck.json",
};

function text(v) {
  return String(v == null ? "" : v).trim();
}

function readJson(abs) {
  try {
    if (!abs || !fs.existsSync(abs)) return null;
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function resolveRepoRoot(explicit) {
  if (explicit) return explicit;
  return DEFAULT_REPO_ROOT;
}

/**
 * Load discovery → deep-research stage counts for a hotel when durable artifacts exist.
 * Omits "validated" when no independent post-research validation milestone is measurable.
 *
 * @param {{ hotelId?: string, repoRoot?: string, screenedOutCount?: number|null }} [opts]
 */
export function resolveResearchFunnelCohort(opts = {}) {
  const hotelId = text(opts.hotelId);
  const root = resolveRepoRoot(opts.repoRoot);

  const empty = {
    contractId: GDI_RESEARCH_FUNNEL_COHORT_V1,
    hotelId: hotelId || null,
    available: false,
    cohortLabel: null,
    reportingScope: "RESEARCH_STAGE_ACTIVITY",
    countingUnit: "UNIQUE_OPPORTUNITY_OR_CANDIDATE",
    stages: {
      potentialOpportunitiesIdentified: null,
      opportunitiesResearched: null,
      opportunitiesValidated: null,
      qualifiedOpportunitiesDelivered: null,
    },
    supporting: {
      opportunitiesScreenedOut: null,
    },
    omitValidated: true,
    omitScreenedOut: true,
    sources: [],
    notes: [],
  };

  if (!hotelId) return empty;

  // Currently only Wyndham has a reconciled Full E2E discovery matrix + deep queue.
  if (hotelId !== WYNDHAM_HOTEL_ID) {
    return {
      ...empty,
      notes: [
        "Stage-level discovery/research funnel artifacts are not registered for this hotel yet.",
      ],
    };
  }

  const matrixPath = path.join(root, WYNDHAM_COHORT.discoveryMatrixRel);
  const deepPath = path.join(root, WYNDHAM_COHORT.deepResearchRel);
  const viabilityPath = path.join(root, WYNDHAM_COHORT.commercialViabilityRel);

  const matrix = readJson(matrixPath);
  const deep = readJson(deepPath);
  const viability = readJson(viabilityPath);

  const discovered = Number(matrix?.summary?.totalExtractedCandidates);
  const researched = Number(deep?.metrics?.dossierCount);
  const viabilityProceed = Number(viability?.count);

  const sources = [];
  if (matrix) {
    sources.push({
      role: "discovery",
      path: WYNDHAM_COHORT.discoveryMatrixRel,
      value: Number.isFinite(discovered) ? discovered : null,
    });
  }
  if (deep) {
    sources.push({
      role: "deep_research",
      path: WYNDHAM_COHORT.deepResearchRel,
      value: Number.isFinite(researched) ? researched : null,
    });
  }
  if (viability) {
    sources.push({
      role: "commercial_viability_precheck",
      path: WYNDHAM_COHORT.commercialViabilityRel,
      value: Number.isFinite(viabilityProceed) ? viabilityProceed : null,
      note: "All rows PROCEED_TO_WHO_HOW — not used as a distinct customer funnel stage.",
    });
  }

  // Validated: only emit when an independent filter exists between researched and qualified.
  // Wyndham commercial-viability count equals researched (121) with no screen-outs → omit.
  const omitValidated =
    !Number.isFinite(viabilityProceed) ||
    !Number.isFinite(researched) ||
    viabilityProceed === researched;

  // Screened-out: only from explicit dispositions — never discovered − qualified.
  const screenedExplicit =
    opts.screenedOutCount == null ? null : Number(opts.screenedOutCount);
  const discoveryRejections = Array.isArray(matrix?.cells)
    ? matrix.cells.reduce((n, c) => {
        const rej = c?.rejections;
        if (Array.isArray(rej)) return n + rej.length;
        if (rej && typeof rej === "object") {
          return (
            n +
            Object.values(rej).reduce((a, v) => a + (Number(v) || 0), 0)
          );
        }
        return n + (Number(rej) || 0);
      }, 0)
    : null;
  const screenedOut =
    screenedExplicit != null && Number.isFinite(screenedExplicit)
      ? screenedExplicit
      : discoveryRejections != null && discoveryRejections > 0
        ? discoveryRejections
        : null;

  const available =
    Number.isFinite(discovered) && discovered > 0 && Number.isFinite(researched);

  return {
    contractId: GDI_RESEARCH_FUNNEL_COHORT_V1,
    hotelId,
    available,
    cohortLabel: "Wyndham San Diego Bayside Full E2E research cohort",
    reportingScope: "RESEARCH_STAGE_ACTIVITY",
    countingUnit: "UNIQUE_CANDIDATE_OR_DOSSIER",
    researchRunId: matrix?.summary?.researchRunId || null,
    stages: {
      potentialOpportunitiesIdentified: Number.isFinite(discovered)
        ? discovered
        : null,
      opportunitiesResearched: Number.isFinite(researched) ? researched : null,
      opportunitiesValidated: omitValidated
        ? null
        : Number.isFinite(viabilityProceed)
          ? viabilityProceed
          : null,
      qualifiedOpportunitiesDelivered: null, // filled by caller from publication inventory
    },
    supporting: {
      opportunitiesScreenedOut: screenedOut,
    },
    omitValidated,
    omitScreenedOut: screenedOut == null,
    sources,
    notes: [
      "Potential Opportunities Identified = unique discovery candidates (matrix totalExtractedCandidates).",
      "Opportunities Researched = unique deep-research dossiers.",
      omitValidated
        ? "Opportunities Validated omitted — no independent post-research validation milestone distinct from researched."
        : "Opportunities Validated = commercial viability proceed count.",
      screenedOut == null
        ? "Opportunities Screened Out omitted — no durable screening disposition ledger (not computed as discovered − qualified)."
        : "Opportunities Screened Out from explicit disposition ledger.",
      "Qualified Opportunities Delivered uses authorized publication inventory (cumulative), not discovery grain.",
    ],
  };
}

export default {
  GDI_RESEARCH_FUNNEL_COHORT_V1,
  resolveResearchFunnelCohort,
};
