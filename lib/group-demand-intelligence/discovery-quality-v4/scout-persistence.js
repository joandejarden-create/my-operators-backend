/**
 * Scout detail persistence — every accepted scout hit gets a stable row or rejection reason.
 * Fixes V3 silent drop of AssociationScout (SCOUT_YIELD without ASSOCIATION_RESULTS.csv).
 */

import { SCOUT_FAMILY } from "../discovery-expansion-v3/scouts.js";

export const ALL_SCOUT_RESULT_FILES = Object.freeze({
  [SCOUT_FAMILY.ASSOCIATION]: "ASSOCIATION_RESULTS.csv",
  [SCOUT_FAMILY.PROCUREMENT]: "PROCUREMENT_RESULTS.csv",
  [SCOUT_FAMILY.CORPORATE_TRIGGER]: "CORPORATE_TRIGGER_RESULTS.csv",
  [SCOUT_FAMILY.PROJECT_WORKFORCE]: "PROJECT_WORKFORCE_RESULTS.csv",
  [SCOUT_FAMILY.MEDICAL_RESEARCH]: "MEDICAL_RESEARCH_RESULTS.csv",
  [SCOUT_FAMILY.SPORTS_HOUSING]: "SPORTS_HOUSING_RESULTS.csv",
  [SCOUT_FAMILY.UNIVERSITY]: "UNIVERSITY_RESULTS.csv",
  [SCOUT_FAMILY.TOUR_DMC]: "TOUR_DMC_RESULTS.csv",
  [SCOUT_FAMILY.HIDDEN_DEMAND]: "HIDDEN_DEMAND_RESULTS.csv",
});

/**
 * Build persistence ledger for one hotel run.
 * @returns {{ persistedByScout, rejected, missingScoutFiles }}
 */
export function buildScoutPersistenceLedger(newCandidates = [], opts = {}) {
  const byScout = {};
  for (const scout of Object.values(SCOUT_FAMILY)) {
    byScout[scout] = [];
  }
  const rejected = [];
  for (const c of newCandidates || []) {
    const scout = c.discoveryMeta?.scoutFamily || c.scoutFamily;
    if (!scout || !byScout[scout]) {
      rejected.push({
        id: c.id,
        title: c.title,
        reason: "UNKNOWN_OR_MISSING_SCOUT_FAMILY",
        scoutFamily: scout || null,
      });
      continue;
    }
    if (!c.officialSource && !c.discoverySource) {
      rejected.push({
        id: c.id,
        title: c.title,
        reason: "NO_SOURCE_URL",
        scoutFamily: scout,
      });
      continue;
    }
    byScout[scout].push(c);
  }

  const writtenFiles = new Set(opts.writtenScoutFiles || []);
  const missingScoutFiles = [];
  for (const [scout, file] of Object.entries(ALL_SCOUT_RESULT_FILES)) {
    if ((byScout[scout] || []).length > 0 && !writtenFiles.has(file) && !writtenFiles.has(scout)) {
      missingScoutFiles.push({ scoutFamily: scout, file, count: byScout[scout].length });
    }
  }

  return {
    persistedByScout: Object.fromEntries(
      Object.entries(byScout).map(([k, v]) => [k, v.length])
    ),
    rejected,
    missingScoutFiles,
    associationProduced: (byScout[SCOUT_FAMILY.ASSOCIATION] || []).length,
  };
}

export function associationPersistenceRootCause() {
  return {
    dropLocation:
      "scripts/gdi-discovery-expansion-v3-2026-10-03.mjs report writer — rowsForScout() never called for SCOUT_FAMILY.ASSOCIATION",
    producedWhere: "orchestrator scoutStats / SCOUT_YIELD.csv (counts only)",
    persistedWhere: "ASSOCIATION_SERIES_RESULTS.csv only (series subset, not detail hits)",
    missingFile: "ASSOCIATION_RESULTS.csv",
    silentLoss: true,
    fix: "Write ASSOCIATION_RESULTS.csv via rowsForScout(ASSOCIATION) + ledger asserting no scout family with candidates lacks a CSV",
  };
}
