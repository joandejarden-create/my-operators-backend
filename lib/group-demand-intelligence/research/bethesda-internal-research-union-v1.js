/**
 * Bethesda Internal Research Union V1
 *
 * Read-time union of canonical Bethesda opportunities (59) + reconstructed
 * historical research-stage records (29) for AUTHORIZED INTERNAL GDI list/detail.
 *
 * Never writes opportunities.json.
 * Never merges into share / published inventory / customer-facing APIs.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import { loadStagedReconstructionBag } from "./bethesda-29-historical-reconstruction-v1.js";
import { RECOVERY_CATEGORY } from "./bethesda-historical-opportunity-recovery-v1.js";
import { RECOVERY_DISPOSITION } from "./gdi-research-stage-persistence-v1.js";
import { GDI_LIST_SCOPE } from "../canonical-customer-facing-inventory-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./bethesda-historical-commercial-category-map-v1.js";

export const BETHESDA_INTERNAL_RESEARCH_UNION_V1 =
  "gdi-bethesda-internal-research-union-v1";

export const BETHESDA_INTERNAL_LIST_SCOPE = "BETHESDA_INTERNAL_UNION_88";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function oppId(o) {
  return text(o?.id || o?.opportunityId);
}

function categoryLabel(cat) {
  if (cat === RECOVERY_CATEGORY.LOCAL_CATERING) return "Local Catering / Events";
  if (cat === RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS) {
    return "Potential Partnerships";
  }
  if (cat === RECOVERY_CATEGORY.SPORTS_HOUSING) return "Sports Housing";
  if (cat === RECOVERY_CATEGORY.RELOCATION_EXTENDED_STAY) {
    return "Relocation / Extended Stay";
  }
  if (cat === RECOVERY_CATEGORY.ASSOCIATION_EVENTS) return "Association Events";
  if (cat === RECOVERY_CATEGORY.CORPORATE_GROUP) return "Corporate / Group";
  return "Group Demand";
}

function priorityForHistorical(row) {
  const disp = text(row.historicalRecoveryDisposition || row.recoveryDisposition);
  if (disp === "OVERFLOW_WATCH" || disp === RECOVERY_DISPOSITION?.OVERFLOW_WATCH) {
    return {
      priority: "WATCHLIST",
      priorityPillLabel: "Overflow Watch",
      priorityFilterKey: "WATCHLIST",
    };
  }
  if (
    disp === "PARTNERSHIP_WATCH" ||
    disp === RECOVERY_DISPOSITION?.PARTNERSHIP_WATCH
  ) {
    return {
      priority: "WATCHLIST",
      priorityPillLabel: "Partnership Watch",
      priorityFilterKey: "WATCHLIST",
    };
  }
  return {
    priority: "WATCHLIST",
    priorityPillLabel: "Research Continuation",
    priorityFilterKey: "WATCHLIST",
  };
}

/**
 * Project a research-stage reconstructed row into list/detail-compatible shape.
 * Does not invent commercial facts. Never Sales Ready / customer-facing.
 * Stamps existing GDI taxonomy fields so type filters are not Other/Unclassified.
 */
export function projectHistoricalRowForInternalList(row = {}) {
  const id = oppId(row);
  const pri = priorityForHistorical(row);
  const cat = row.researchCategory || null;
  const archivalLabel = categoryLabel(cat);
  const categorized = applyHistoricalCommercialCategoryProjection(row);
  const what =
    text(categorized.whatIsHappening || row.whatIsHappening) ||
    "Historical research attestation — further research required.";
  const missing = Array.isArray(row.missingResearchRequirements)
    ? row.missingResearchRequirements
    : [];
  const maturityPill =
    text(categorized.commercialStagePill) ||
    text(categorized.priorityPillLabel) ||
    pri.priorityPillLabel;
  const maturity =
    text(categorized.commercialMaturity) ||
    (maturityPill.includes("Overflow")
      ? "OVERFLOW_WATCH"
      : maturityPill.includes("Partnership")
        ? "PARTNERSHIP_WATCH"
        : "RESEARCH_CONTINUATION");

  return {
    ...categorized,
    id,
    opportunityId: id,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    organizationName: text(categorized.organizationName || row.organizationName) || id,
    title:
      text(categorized.title || row.title) ||
      text(categorized.organizationName || row.organizationName) ||
      id,
    // Keep archival recovery category for provenance / admin grouping.
    researchCategory: cat,
    researchCategoryArchival: cat,
    researchCategoryLabelArchival: archivalLabel,
    demandSegment: categorized.demandSegment || row.demandSegment || row.researchSegment || null,
    researchSegment:
      categorized.researchSegment || row.researchSegment || row.demandSegment || null,
    ...pri,
    priorityPillLabel: maturityPill,
    summaryWhat: what,
    tileCustomerSummary: text(categorized.tileCustomerSummary) || what,
    summaryWhyHotel:
      text(categorized.summaryWhyHotel || categorized.whyThisHotel || categorized.hotelFitRationale) ||
      null,
    summaryWhyMatters: what,
    recommendedAction: text(categorized.recommendedAction || row.recommendedAction) || null,
    outreachReadiness: text(categorized.outreachReadiness) || maturity,
    commercialStagePill: maturityPill,
    commercialMaturity: maturity,
    salesReady: false,
    customerFacing: false,
    customerVisible: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
    // Allow detail through isGdiTestOrFixtureOpportunity local-preview gate
    // without implying customer publication.
    bethesdaInternalResearchInventory: true,
    localShadowOnly: true,
    doesNotAffectProduction: true,
    gdiInternalUnionOnly: true,
    gdiListScope: GDI_LIST_SCOPE.INTERNAL_ALL,
    missingEvidenceNote:
      missing.length > 0
        ? `Needs research: ${missing.slice(0, 4).join(", ")}`
        : "Historical reconstruction — publication not authorized.",
    sources: Array.isArray(row.sources) ? row.sources : [],
  };
}

export function isBethesdaInternalUnionHotel(hotelId) {
  return text(hotelId) === BETHESDA_CANONICAL_HOTEL_ID;
}

export function loadBethesdaHistoricalResearchRows({ root = ROOT } = {}) {
  const staged = loadStagedReconstructionBag({ root });
  const rows = staged?.opportunities || [];
  const byId = new Map();
  for (const row of rows) {
    const id = oppId(row);
    if (!id || !BETHESDA_29_ATTESTED_IDS.includes(id)) continue;
    byId.set(id, projectHistoricalRowForInternalList(row));
  }
  // Ensure all 29 IDs are present even if fixture/stage incomplete.
  for (const id of BETHESDA_29_ATTESTED_IDS) {
    if (!byId.has(id)) {
      byId.set(
        id,
        projectHistoricalRowForInternalList({
          id,
          organizationName: id,
          whatIsHappening:
            "Historical attestation ID present but reconstructed body unavailable in research-stage bag.",
          reconstructionStatus: "STILL_MISSING_RESEARCH",
          historicalRecoveryDisposition: "ATTESTED_BODY_MISSING",
          missingResearchRequirements: ["ORIGINAL_RESEARCH_BODY"],
          customerFacing: false,
          salesReady: false,
        })
      );
    }
  }
  return {
    rows: BETHESDA_29_ATTESTED_IDS.map((id) => byId.get(id)),
    count: 29,
    source: staged ? "research-stage-or-fixture" : "id-fallback",
  };
}

/**
 * Build dry-run merge manifest for founder review (no writes).
 */
export function buildBethesda88UnionDryRunManifest({
  root = ROOT,
  canonicalOpportunities = [],
} = {}) {
  const canonical = Array.isArray(canonicalOpportunities)
    ? canonicalOpportunities
    : [];
  const canonicalIds = canonical.map(oppId).filter(Boolean);
  const hist = loadBethesdaHistoricalResearchRows({ root });
  const histIds = hist.rows.map(oppId);
  const overlap = histIds.filter((id) => canonicalIds.includes(id));
  const unionIds = [...new Set([...canonicalIds, ...histIds])];
  const oppPath = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "opportunities.json"
  );
  const hash = fs.existsSync(oppPath)
    ? crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex")
    : null;

  return {
    version: BETHESDA_INTERNAL_RESEARCH_UNION_V1,
    mode: "READ_TIME_UNION_NO_STORE_WRITE",
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    generatedAt: new Date().toISOString(),
    counts: {
      canonical: canonicalIds.length,
      historical: histIds.length,
      overlap: overlap.length,
      union: unionIds.length,
      expectedUnion: 88,
    },
    overlapIds: overlap,
    unionIds,
    opportunityStore: {
      pathRel: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`,
      sha256: hash,
      writesAuthorized: false,
      writeApplied: false,
    },
    rollback: {
      note: "Read-time union only — disable by reverting auth list wiring; no opportunities.json mutation to roll back.",
    },
    safety: {
      customerShare: false,
      publicationRegistry: false,
      salesReadyPromotion: false,
      inventedBodies: false,
    },
  };
}

/**
 * Merge canonical Bethesda inventory with 29 historical research-stage rows.
 * Canonical rows win on ID collision. Does not mutate inputs.
 */
export function mergeBethesdaInternalResearchOpportunities(
  hotelId,
  canonicalOpportunities = [],
  { root = ROOT } = {}
) {
  if (!isBethesdaInternalUnionHotel(hotelId)) {
    return {
      applied: false,
      opportunities: Array.isArray(canonicalOpportunities)
        ? [...canonicalOpportunities]
        : [],
      scope: GDI_LIST_SCOPE.CUSTOMER_FACING,
    };
  }

  const canonical = Array.isArray(canonicalOpportunities)
    ? canonicalOpportunities
    : [];
  const byId = new Map();
  for (const o of canonical) {
    const id = oppId(o);
    if (!id) continue;
    byId.set(id, {
      ...o,
      gdiInternalUnionMember: "CANONICAL",
    });
  }
  const hist = loadBethesdaHistoricalResearchRows({ root });
  let added = 0;
  for (const row of hist.rows) {
    const id = oppId(row);
    if (!id) continue;
    if (byId.has(id)) continue; // canonical wins
    byId.set(id, row);
    added += 1;
  }

  const opportunities = [...byId.values()];
  return {
    applied: true,
    opportunities,
    scope: BETHESDA_INTERNAL_LIST_SCOPE,
    counts: {
      canonical: canonical.length,
      historicalAdded: added,
      historicalAvailable: hist.count,
      union: opportunities.length,
    },
    listScope: GDI_LIST_SCOPE.INTERNAL_ALL,
  };
}

export function findBethesdaInternalResearchOpportunity(
  hotelId,
  opportunityId,
  { root = ROOT } = {}
) {
  if (!isBethesdaInternalUnionHotel(hotelId)) return null;
  const id = text(opportunityId);
  if (!id || !BETHESDA_29_ATTESTED_IDS.includes(id)) return null;
  const hist = loadBethesdaHistoricalResearchRows({ root });
  return hist.rows.find((r) => oppId(r) === id) || null;
}
