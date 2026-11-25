/**
 * Canonical Full E2E — Competitive Account Discovery (CAD) opt-in stage V1.
 *
 * Reuses PR #602 / #603 classifier + contract. Enriches Research SoT fields only.
 * Does NOT auto-promote FQO, authorize persistence, or invent a predictive engine.
 */

import {
  attachCompetitiveAccountDiscovery,
  classifyCompetitiveAccountDiscovery,
  CAD_CLASS,
  CAD_ATTAINABILITY,
  CAD_SOURCING_WINDOW_STATE,
} from "./gdi-competitive-account-discovery-classifier-v1.js";
import { GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1 } from "./gdi-competitive-account-discovery-contract-v1.js";

export const GDI_CANONICAL_CAD_OPT_IN_STAGE_V1 =
  "gdi-canonical-cad-opt-in-stage-v1";

/**
 * Resolve CAD opt-in. Default OFF.
 */
export function resolveCanonicalCadOptIn(opts = {}) {
  const enabled =
    opts.enableCadStage === true ||
    opts.cadOptIn === true ||
    opts.enableCompetitiveAccountDiscovery === true;
  return {
    enabled,
    reason: enabled ? "EXPLICIT_CAD_OPT_IN" : "CAD_OPT_IN_OFF_BY_DEFAULT",
    note: enabled
      ? "CAD classifies competitive evidence onto Research SoT; does not auto-promote FQO or authorize persistence."
      : "CAD classifier is not a default canonical stage.",
  };
}

function collectOpportunities(opts = {}) {
  const rows = [];
  const pushAll = (arr) => {
    if (!Array.isArray(arr)) return;
    for (const row of arr) {
      if (row && typeof row === "object") rows.push(row);
    }
  };
  pushAll(opts.opportunities);
  pushAll(opts.seedOpportunities);
  pushAll(opts.chain?.processed);
  pushAll(opts.chain?.opportunities);
  pushAll(opts.researchSotRows);

  // De-dupe by opportunity id / title key
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key =
      String(row.id || row.opportunityId || "").trim() ||
      [
        String(row.organizationName || row.organization || "").toLowerCase(),
        String(row.title || row.displayTitle || "").toLowerCase(),
      ].join("|");
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

/**
 * Run CAD enrichment over research opportunities (pure classify + attach).
 */
export function runCanonicalCadOptInStage(opts = {}) {
  const resolved = resolveCanonicalCadOptIn(opts);
  if (!resolved.enabled) {
    return {
      contract: GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
      status: "SKIPPED_OPT_IN_OFF",
      enabled: false,
      classifiedCount: 0,
      enriched: [],
      fqoAutoPromoted: false,
      persistenceAuthorized: false,
      predictiveEngineClaim: false,
      note: resolved.note,
    };
  }

  const opportunities = collectOpportunities(opts);
  const enriched = [];
  const classCounts = {
    [CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL]: 0,
    [CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY]: 0,
    [CAD_CLASS.FQO_CANDIDATE]: 0,
    UNCLASSIFIED: 0,
  };
  const windowCounts = {
    [CAD_SOURCING_WINDOW_STATE.CONFIRMED]: 0,
    [CAD_SOURCING_WINDOW_STATE.INFERRED]: 0,
    [CAD_SOURCING_WINDOW_STATE.UNKNOWN]: 0,
  };
  const attainabilityCounts = {
    [CAD_ATTAINABILITY.WINNABLE]: 0,
    [CAD_ATTAINABILITY.OVERFLOW_ONLY]: 0,
    [CAD_ATTAINABILITY.MONITOR]: 0,
    [CAD_ATTAINABILITY.UNATTAINABLE]: 0,
  };

  for (const opp of opportunities) {
    const attached = attachCompetitiveAccountDiscovery(opp, {
      research: opts.research || undefined,
      cycleKey: opts.cycleKey || null,
    });
    const cad = attached.competitiveAccountDiscovery || null;
    const cadClass = cad?.cadClass || null;
    if (cadClass && classCounts[cadClass] != null) classCounts[cadClass] += 1;
    else classCounts.UNCLASSIFIED += 1;
    const win = cad?.sourcingWindow?.state || CAD_SOURCING_WINDOW_STATE.UNKNOWN;
    if (windowCounts[win] != null) windowCounts[win] += 1;
    const att = cad?.attainability || null;
    if (att && attainabilityCounts[att] != null) attainabilityCounts[att] += 1;

    enriched.push({
      opportunityId: opp.id || opp.opportunityId || null,
      title: opp.title || opp.displayTitle || null,
      organizationName: opp.organizationName || opp.organization || null,
      cadClass,
      attainability: cad?.attainability || null,
      sourcingWindowState: win,
      programKey: cad?.programKey || null,
      fqoPass: false,
      persistenceAuthorized: false,
      competitiveAccountDiscovery: cad,
      __cadClassification: attached.__cadClassification,
    });
  }

  return {
    contract: GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
    cadContract: GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1,
    status: "CLASSIFIED",
    enabled: true,
    ok: true,
    classifiedCount: enriched.length,
    classCounts,
    windowCounts,
    attainabilityCounts,
    enriched,
    // Explicit non-claims
    fqoAutoPromoted: false,
    persistenceAuthorized: false,
    publicationAuthorized: false,
    predictiveEngineClaim: false,
    preserves: {
      HISTORICAL_COMPETITIVE_INTEL: true,
      EMERGING_COMPETITIVE_OPPORTUNITY: true,
      FQO_CANDIDATE: true,
      attainability: true,
      sourcingWindows: ["CONFIRMED", "INFERRED", "UNKNOWN"],
      competitorCommitmentEvidence: true,
      crossHotelProgramIdentityReuse: true,
    },
    note: resolved.note,
  };
}

/**
 * Merge CAD enrichment back onto opportunity rows for Research SoT carriers.
 * Never sets finalizedQualified / salesReady from CAD alone.
 */
export function applyCadEnrichmentToResearchRows(rows = [], cadStage = null) {
  if (!cadStage || cadStage.enabled !== true) {
    return Array.isArray(rows) ? rows.map((r) => ({ ...r })) : [];
  }
  const byId = new Map();
  for (const e of cadStage.enriched || []) {
    const key = String(e.opportunityId || "").trim();
    if (key) byId.set(key, e);
  }
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const key = String(row.id || row.opportunityId || "").trim();
    const hit = key ? byId.get(key) : null;
    if (!hit) return { ...row };
    return {
      ...row,
      competitiveAccountDiscovery: hit.competitiveAccountDiscovery,
      __cadClassification: hit.__cadClassification,
      // Never let CAD alone flip qualification
      finalizedQualifiedOpportunity:
        row.finalizedQualifiedOpportunity === true ? true : false,
      salesReady: row.salesReady === true ? true : false,
    };
  });
}

export {
  classifyCompetitiveAccountDiscovery,
  attachCompetitiveAccountDiscovery,
  CAD_CLASS,
  CAD_ATTAINABILITY,
  CAD_SOURCING_WINDOW_STATE,
};
