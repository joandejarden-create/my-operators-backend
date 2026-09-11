/**
 * Hotel alias consolidation + duplicate physical-property detection (read-only audit).
 */

import { CROSS_REPORT_VERDICT } from "./adp-client-readiness-contract-v1.js";

export const ADP_HOTEL_ALIAS_CONSOLIDATION_INTEGRITY = "ADP_HOTEL_ALIAS_CONSOLIDATION_INTEGRITY";

/** Light normalize for clustering — does NOT claim identity alone. */
export function normalizeHotelSurface(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(hotel|resort|spa|casino|the|a|an|by|and)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Heuristic: A and B likely same physical property if one normalizes to a
 * prefix/superset of the other with only amenity/legal suffixes differing.
 */
export function likelySamePhysicalHotel(a, b) {
  const na = normalizeHotelSurface(a);
  const nb = normalizeHotelSurface(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  const shorter = na.length <= nb.length ? na : nb;
  const longer = na.length <= nb.length ? nb : na;
  if (shorter.length < 12) return false;
  if (!longer.startsWith(shorter) && !longer.includes(shorter)) return false;
  const remainder = longer.replace(shorter, "").trim();
  // remainder should only be amenity/legal tokens already stripped — empty or tiny
  return remainder.length === 0 || /^(and\s+)?(casino|spa|suites?|collection)?$/.test(remainder);
}

/** Negative collision fixtures — must NOT merge. */
export const ENTITY_COLLISION_FIXTURES_V1 = Object.freeze([
  {
    id: "st_regis_cross_city",
    a: "The St. Regis Mexico City",
    b: "The St. Regis Cap Cana Resort",
    mustNotMerge: true,
  },
  {
    id: "jw_vs_marriott",
    a: "JW Marriott Hotel Santo Domingo",
    b: "Marriott Hotel Santo Domingo",
    mustNotMerge: true,
  },
  {
    id: "radisson_vs_blu",
    a: "Radisson Hotel Santo Domingo",
    b: "Radisson Blu Santo Domingo",
    mustNotMerge: true,
  },
  {
    id: "airport_vs_downtown_mex",
    a: "JW Marriott Hotel Mexico City",
    b: "Mexico City Marriott Hotel Airport",
    mustNotMerge: true,
  },
]);

export function runCollisionProtectionTests() {
  const results = ENTITY_COLLISION_FIXTURES_V1.map((f) => {
    const merged = likelySamePhysicalHotel(f.a, f.b);
    return {
      ...f,
      heuristicMerged: merged,
      pass: f.mustNotMerge ? !merged : true,
    };
  });
  return {
    gate: "ADP_HOTEL_ENTITY_COLLISION_PROTECTION",
    pass: results.every((r) => r.pass),
    results,
  };
}

/**
 * Scan competitor/displacement rows across reports for unresolved same-physical duplicates.
 */
export function auditEntityDuplicatesAcrossReports(reportsByPropertyId) {
  const groups = [];
  const defects = [];

  for (const [propertyId, report] of Object.entries(reportsByPropertyId)) {
    const payload = report?.payload || report;
    const names = [];
    for (const row of payload?.competitiveSet?.observed || []) {
      names.push({
        surface: "competitiveSet.observed",
        name: row.name,
        entityId: row.entityId ?? null,
        mentions: row.mentions ?? null,
        scenarioCount: row.scenarioCount ?? null,
      });
    }
    for (const row of payload?.lostDemand?.displacement || []) {
      names.push({
        surface: "lostDemand.displacement",
        name: row.name,
        entityId: row.entityId ?? null,
        displacementCount: row.displacementCount ?? null,
      });
    }

    // Pairwise within report
    for (let i = 0; i < names.length; i++) {
      for (let j = i + 1; j < names.length; j++) {
        const a = names[i];
        const b = names[j];
        if (a.name === b.name) continue;
        if (!likelySamePhysicalHotel(a.name, b.name)) continue;
        const sameEntityId =
          a.entityId && b.entityId && a.entityId === b.entityId;
        const group = {
          propertyId,
          canonicalGuess: a.name.length >= b.name.length ? a.name : b.name,
          aliases: [a.name, b.name],
          rows: [a, b],
          aggregationStatus: sameEntityId
            ? "CONSOLIDATED"
            : a.entityId || b.entityId
              ? "PARTIAL"
              : "DEFECT",
        };
        groups.push(group);
        if (group.aggregationStatus === "DEFECT" || group.aggregationStatus === "PARTIAL") {
          defects.push({
            ...group,
            analyticalImpact: estimateDuplicateImpact(a, b),
          });
        }
      }
    }
  }

  return {
    gate: ADP_HOTEL_ALIAS_CONSOLIDATION_INTEGRITY,
    pass: defects.length === 0,
    duplicateGroupCount: groups.length,
    unresolvedDefectCount: defects.length,
    groups,
    defects,
  };
}

function estimateDuplicateImpact(a, b) {
  const mentions = (a.mentions || 0) + (b.mentions || 0);
  const scenarios = Math.max(a.scenarioCount || 0, b.scenarioCount || 0);
  const displacement =
    (a.displacementCount || 0) + (b.displacementCount || 0);
  return {
    splitMentions: { a: a.mentions ?? null, b: b.mentions ?? null, combinedIfMerged: mentions || null },
    splitScenarios: {
      a: a.scenarioCount ?? null,
      b: b.scenarioCount ?? null,
      note: "scenarioCounts may overlap — combined is upper bound only",
      maxObserved: scenarios || null,
    },
    splitDisplacement: {
      a: a.displacementCount ?? null,
      b: b.displacementCount ?? null,
      combinedIfMerged: displacement || null,
    },
    rankDistortionRisk: "HIGH",
    note: "Split aliases understate per-row counts and can inflate competitor list rank positions.",
  };
}

/**
 * Shared-hotel cross-report metric matrix (symbolic "43% test").
 * Compares subject consideration/demandCapture vs competitor appearance rates
 * only when scope is marked comparable.
 */
export function buildSharedHotelCrossReportMatrix(cohort) {
  /**
   * cohort item.competitorAppearances = rows from OTHER hotels' reports
   * where this subject appears as a competitor.
   */
  const rows = [];
  for (const subject of cohort) {
    for (const hit of subject.competitorAppearances || []) {
      const own = subject.ownMetrics?.considerationRate ?? null;
      const competitorMetric = hit.presenceEstimate ?? null;
      let verdict = CROSS_REPORT_VERDICT.NOT_COMPARABLE;
      let reason =
        "Competitor appearance/scenario counts are not the same metric as subject AI Consideration unless explicitly same-scope labeled.";
      let scopeEquivalent = false;

      if (
        hit.metricScope === "subject_consideration_equivalent" &&
        own != null &&
        competitorMetric != null
      ) {
        scopeEquivalent = true;
        const delta = Math.abs(Number(own) - Number(competitorMetric));
        if (delta <= 0.15) {
          verdict = CROSS_REPORT_VERDICT.EXACT_MATCH;
          reason = "Same-scope consideration rates match within rounding tolerance.";
        } else {
          verdict = CROSS_REPORT_VERDICT.INVALID_MISMATCH;
          reason = `Same-scope mismatch delta=${delta}`;
        }
      } else if (own != null && competitorMetric != null) {
        verdict = CROSS_REPORT_VERDICT.VALID_SCOPE_DIFFERENCE;
        reason =
          "Competitor row uses appearance/scenario grain; own report uses observation-grain consideration — difference expected if scopes differ.";
      }

      rows.push({
        canonicalHotelId: subject.canonicalHotelId || null,
        displayName: subject.displayName,
        reportWhereCompetitorAppears: hit.inReportPropertyId,
        competitorMetric,
        competitorMentions: hit.mentions ?? null,
        competitorScenarioCount: hit.scenarioCount ?? null,
        ownReportMetric: own,
        ownDemandCapture: subject.ownMetrics?.demandCapture ?? null,
        scopeEquivalent,
        expectedMatch: scopeEquivalent,
        actualDelta:
          own != null && competitorMetric != null
            ? Math.abs(Number(own) - Number(competitorMetric))
            : null,
        verdict,
        reasonIfDifferent: reason,
      });
    }
  }

  const invalid = rows.filter((r) => r.verdict === CROSS_REPORT_VERDICT.INVALID_MISMATCH);
  return {
    gate: "ADP_SHARED_HOTEL_43_PERCENT_RECONCILIATION_TEST",
    pass: invalid.length === 0,
    rowCount: rows.length,
    invalidMismatchCount: invalid.length,
    rows,
  };
}
