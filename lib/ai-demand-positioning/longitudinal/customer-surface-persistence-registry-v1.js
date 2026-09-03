/**
 * Customer-surface → historical persistence registry.
 * If a customer-visible value has no persisted historical source → FAIL
 * REPORT_SNAPSHOT_COMPLETENESS_INTEGRITY
 */

import { REPORT_SNAPSHOT_COMPLETENESS_INTEGRITY } from "./report-snapshot-v1.js";

export const CUSTOMER_SURFACE_PERSISTENCE_REGISTRY_VERSION =
  "adp_customer_surface_persistence_registry_v1";

/**
 * Each row: customer surface → payload path(s) → structured history concept → snapshot source.
 * Airtable table names are FUTURE design (writes not enabled).
 */
export const CUSTOMER_SURFACE_PERSISTENCE_REGISTRY = Object.freeze([
  {
    surface: "Executive Read",
    payloadPaths: ["executive.narrative", "executive.headline"],
    structuredStore: "ADP Period Metrics / ADP Report Snapshots",
    snapshotSource: "customerPayload.executive",
  },
  {
    surface: "AI Consideration",
    payloadPaths: ["kpis.considerationRate", "kpis.considerationNumerator", "kpis.considerationDenominator"],
    structuredStore: "ADP Period Metrics",
    snapshotSource: "customerPayload.kpis.considerationRate",
  },
  {
    surface: "Scenario Presence",
    payloadPaths: ["kpis.scenarioPresence", "kpis.scenarioPresenceNumerator", "kpis.scenarioPresenceDenominator"],
    structuredStore: "ADP Period Metrics",
    snapshotSource: "customerPayload.kpis.scenarioPresence",
  },
  {
    surface: "Demand Capture",
    payloadPaths: ["kpis.demandCapture"],
    structuredStore: "ADP Period Metrics",
    snapshotSource: "customerPayload.kpis.demandCapture",
  },
  {
    surface: "Reality Coverage",
    payloadPaths: ["kpis.realityCoverage"],
    structuredStore: "ADP Period Metrics",
    snapshotSource: "customerPayload.kpis.realityCoverage",
  },
  {
    surface: "#1 Appearance / Top-3 Appearance",
    payloadPaths: ["kpis.numberOneAppearanceRate", "kpis.topThreeAppearanceRate"],
    structuredStore: "ADP Period Metrics",
    snapshotSource: "customerPayload.kpis",
  },
  {
    surface: "Presence Index / CORE",
    payloadPaths: ["kpis.presenceIndex", "kpis.coreBenchmark", "kpis.coreComposition"],
    structuredStore: "ADP Period Metrics",
    snapshotSource: "customerPayload.kpis",
  },
  {
    surface: "Demand Territories",
    payloadPaths: ["territories[]"],
    structuredStore: "ADP Territory Metrics",
    snapshotSource: "customerPayload.territories",
  },
  {
    surface: "Provider Presence",
    payloadPaths: ["providers[]"],
    structuredStore: "ADP Provider Metrics",
    snapshotSource: "customerPayload.providers",
  },
  {
    surface: "Competitive Overview",
    payloadPaths: ["competitive.overview.entities[]", "competitive.overview.visibleSubset"],
    structuredStore: "ADP Competitive Rankings",
    snapshotSource: "customerPayload.competitive.overview",
  },
  {
    surface: "Competitive Context",
    payloadPaths: [
      "competitive.context.topAlternative",
      "competitive.context.displacement",
      "competitive.context.narratives",
      "competitive.context.actions",
    ],
    structuredStore: "ADP Period Metrics + linked entity/scenario IDs",
    snapshotSource: "customerPayload.competitive.context",
  },
  {
    surface: "Reality Gaps",
    payloadPaths: ["realityGaps[]"],
    structuredStore: "ADP Period Metrics (findings JSON)",
    snapshotSource: "customerPayload.realityGaps",
  },
  {
    surface: "Sources / Citations",
    payloadPaths: ["sources[]"],
    structuredStore: "ADP Evidence Observations / Sources",
    snapshotSource: "customerPayload.sources",
  },
  {
    surface: "Positive Evidence",
    payloadPaths: ["evidence.positive[]"],
    structuredStore: "ADP Evidence Observations",
    snapshotSource: "customerPayload.evidence.positive",
  },
  {
    surface: "Missing Evidence",
    payloadPaths: ["evidence.missing[]"],
    structuredStore: "ADP Evidence Observations",
    snapshotSource: "customerPayload.evidence.missing",
  },
  {
    surface: "Displacement Evidence",
    payloadPaths: ["evidence.displacement[]"],
    structuredStore: "ADP Evidence Observations",
    snapshotSource: "customerPayload.evidence.displacement",
  },
  {
    surface: "Trends",
    payloadPaths: ["trends.points[]", "trends.comparisonMode", "trends.baselinePeriodId"],
    structuredStore: "ADP Trend History",
    snapshotSource: "customerPayload.trends",
  },
  {
    surface: "Ranking Movement",
    payloadPaths: ["rankMovement.byScope", "rankMovement.comparisonPeriodId", "rankMovement.deltaColumnLabel"],
    structuredStore: "ADP Competitive Rankings + ADP Monitoring Periods",
    snapshotSource: "customerPayload.rankMovement",
  },
  {
    surface: "Review / Actions",
    payloadPaths: ["actions[]"],
    structuredStore: "ADP Period Metrics",
    snapshotSource: "customerPayload.actions",
  },
]);

function pathExists(obj, path) {
  if (!path || path.includes("[]")) {
    // array path: check parent exists and is array/object
    const base = path.replace(/\[\]$/, "").replace(/\.\w+$/, "");
    const parts = path.replace(/\[\]/g, "").split(".").filter(Boolean);
    let cur = obj;
    for (const p of parts) {
      if (cur == null) return false;
      cur = cur[p];
    }
    return cur !== undefined && cur !== null;
  }
  const parts = path.split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur == null || typeof cur !== "object") return false;
    cur = cur[p];
  }
  return cur !== undefined;
}

/**
 * Audit a customerPayload against the persistence registry.
 */
export function auditReportSnapshotCompleteness(customerPayload) {
  const defects = [];
  const missing = [];
  for (const row of CUSTOMER_SURFACE_PERSISTENCE_REGISTRY) {
    const ok = (row.payloadPaths || []).some((p) => {
      if (p.endsWith("[]")) {
        const key = p.slice(0, -2);
        const parts = key.split(".");
        let cur = customerPayload;
        for (const part of parts) {
          if (cur == null) return false;
          cur = cur[part];
        }
        return Array.isArray(cur) || (cur && typeof cur === "object");
      }
      if (p.includes(".overview.entities")) {
        return Array.isArray(customerPayload?.competitive?.overview?.entities);
      }
      return pathExists(customerPayload, p);
    });
    if (!ok) {
      missing.push(row.surface);
      defects.push({
        code: REPORT_SNAPSHOT_COMPLETENESS_INTEGRITY,
        detail: `customer surface lacks historical source: ${row.surface}`,
        surface: row.surface,
      });
    }
  }
  return {
    pass: defects.length === 0,
    defects,
    missing,
    registryVersion: CUSTOMER_SURFACE_PERSISTENCE_REGISTRY_VERSION,
    coveredSurfaces: CUSTOMER_SURFACE_PERSISTENCE_REGISTRY.length - missing.length,
    totalSurfaces: CUSTOMER_SURFACE_PERSISTENCE_REGISTRY.length,
  };
}
