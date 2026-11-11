/**
 * ADP_FULL_E2E_AIRTABLE_HISTORY_PERSISTENCE_GATE_V1
 *
 * Full ADP E2E must not silently skip the 9-table longitudinal history sync.
 * Writes remain founder-gated; this gate fail-closes CUSTOMER_READY when:
 *   - history sync is required for certification AND records are missing, OR
 *   - writes were authorized but readback failed / partial.
 *
 * When writes are not authorized, status is AWAITING_HISTORY_WRITE_APPROVAL
 * (not a false PASS that Airtable is complete).
 */

import "dotenv/config";
import {
  HISTORY_TABLES,
  EXISTING_LIVE_PUBLISH,
  isAdpHistoryWritesEnabled,
  HISTORY_ENV,
} from "./airtable-history-schema-final-v1.js";
import {
  ADP_GOVERNED_BASE_ID,
  resolveProductionHistoryDestination,
} from "./resolve-adp-history-destination-v1.js";
import { getAdpPersistencePolicy } from "../adp-persistence-policy.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../certification/adp-unprompted-independent-discovery-standard-v1.js";

export const ADP_FULL_E2E_AIRTABLE_HISTORY_PERSISTENCE_GATE_V1 =
  "ADP_FULL_E2E_AIRTABLE_HISTORY_PERSISTENCE_GATE_V1";

export const HISTORY_SYNC_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  PARTIAL: "PARTIAL",
  MISSING: "MISSING",
  AWAITING_HISTORY_WRITE_APPROVAL: "AWAITING_HISTORY_WRITE_APPROVAL",
  WRITE_AUTHORIZED_READBACK_FAILED: "WRITE_AUTHORIZED_READBACK_FAILED",
  LOOKUP_UNAVAILABLE: "LOOKUP_UNAVAILABLE",
});

/** All nine Measurement-History tables — Prompt Ledger is required (not optional). */
const NINE_HISTORY_TABLES = Object.freeze([
  HISTORY_TABLES.MONITORING_PERIODS,
  HISTORY_TABLES.REPORT_SNAPSHOTS,
  HISTORY_TABLES.PERIOD_METRICS,
  HISTORY_TABLES.TERRITORY_METRICS,
  HISTORY_TABLES.PROVIDER_METRICS,
  HISTORY_TABLES.COMPETITIVE_RANKINGS,
  HISTORY_TABLES.EVIDENCE_INDEX,
  HISTORY_TABLES.REPORT_CORRECTIONS,
  HISTORY_TABLES.PROMPT_LEDGER,
]);

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function historyBaseId() {
  const dest = resolveProductionHistoryDestination({ confirmGovernedAdpBase: true });
  return (
    dest.baseId ||
    process.env.ADP_HISTORY_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    ADP_GOVERNED_BASE_ID
  );
}

/**
 * Map staged owner payload → shape expected by buildHistoricalWriteManifest.
 */
export function stagedOwnerPayloadToHistoryShape(stagedPayload, { propertyId, periodId } = {}) {
  const p = stagedPayload && typeof stagedPayload === "object" ? stagedPayload : {};
  const em = p.executiveMetrics || {};
  const cons = em.considerationRate || {};
  const presence = em.scenarioPresence || {};
  const rg = p.realityGap || {};
  const overall =
    p.competitiveRankingByTerritory?.byTerritory?.overall || null;
  const byTerritory = p.competitiveRankingByTerritory?.byTerritory || {};

  const territories = Object.entries(byTerritory)
    .filter(([key]) => key !== "overall")
    .map(([territoryId, block]) => {
      const subject = (block.displayRows || []).find((r) => r.isSubject);
      return {
        territoryId,
        name: block.territory || territoryId,
        numerator: subject?.appearances ?? null,
        denominator: block.comparableN ?? null,
        aiPresence: subject?.aiPresencePct ?? null,
        rank: subject?.displayRank ?? subject?.observedRank ?? null,
      };
    });

  const providerRows = p.evidence?.providerPresence || p.providerPresence || [];
  const providers = (Array.isArray(providerRows) ? providerRows : []).map((row) => ({
    providerId: row.providerId || row.provider || row.id,
    name: row.providerName || row.name || row.providerId,
    numerator: row.presentCount ?? row.numerator ?? null,
    denominator: row.scheduledCount ?? row.denominator ?? null,
    presenceRate: row.presenceRate ?? row.rate ?? null,
    missingCount: row.missingCount ?? null,
    completeness: row.completeness || null,
  }));

  // Fallback: 4 canonical providers from period meta when evidence rows absent
  if (!providers.length) {
    for (const id of ["openai", "gemini", "claude", "perplexity"]) {
      providers.push({
        providerId: id,
        name: id,
        numerator: null,
        denominator: null,
        presenceRate: null,
      });
    }
  }

  const entities = (overall?.displayRows || []).map((r) => ({
    entityId: r.entityId || (r.isSubject ? "__subject__" : null),
    displayName: r.name,
    rank: r.displayRank ?? r.observedRank,
    numerator: r.appearances,
    denominator: overall?.comparableN,
    presencePct: r.aiPresencePct,
    isSubject: Boolean(r.isSubject),
  }));

  return {
    propertyId: propertyId || p.property?.propertyId,
    periodId: periodId || p.period?.periodId,
    kpis: {
      realityCoverage: rg.coverageScore ?? rg.realityCoverage ?? null,
      scenarioPresence: presence.rate ?? null,
      scenarioPresenceNumerator: presence.capturedScenarios ?? null,
      scenarioPresenceDenominator: presence.eligibleScenarios ?? null,
      demandCapture: p.demandCapture?.rate ?? p.demandCapture?.demandCaptureRate ?? null,
      considerationRate: cons.rate ?? null,
      considerationNumerator: cons.presentObservations ?? null,
      considerationDenominator: cons.comparableObservations ?? null,
      numberOneAppearanceRate: em.rankMetrics?.numberOneAppearanceRate ?? null,
      topThreeAppearanceRate: em.rankMetrics?.topThreeAppearanceRate ?? null,
    },
    territories,
    providers,
    competitive: {
      overview: { entities },
    },
    evidence: {
      positive: (p.evidence?.positiveExamples || p.evidence?.positive || []).slice(0, 40),
      missing: (p.evidence?.missingExamples || p.evidence?.missing || []).slice(0, 40),
      displacement: (p.evidence?.displacementExamples || []).slice(0, 40),
    },
    methodologyId:
      cons.methodologyId ||
      cons.correctedMethodologyId ||
      CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
    measurementMethodology: p.measurementMethodology || null,
  };
}

/**
 * Expected minimum Core history rows for one hotel/period (before evidence expansion).
 */
export function expectedCoreHistoryMinimumCounts({
  territoryCount = 0,
  providerCount = 4,
  competitiveEntityCount = 0,
  evidenceCount = 0,
  includeCorrection = false,
} = {}) {
  return {
    [HISTORY_TABLES.MONITORING_PERIODS]: 1,
    [HISTORY_TABLES.REPORT_SNAPSHOTS]: 1,
    [HISTORY_TABLES.PERIOD_METRICS]: 1,
    [HISTORY_TABLES.TERRITORY_METRICS]: Math.max(0, territoryCount),
    [HISTORY_TABLES.PROVIDER_METRICS]: Math.max(0, providerCount),
    [HISTORY_TABLES.COMPETITIVE_RANKINGS]: Math.max(0, competitiveEntityCount),
    [HISTORY_TABLES.EVIDENCE_INDEX]: Math.max(0, evidenceCount),
    [HISTORY_TABLES.REPORT_CORRECTIONS]: includeCorrection ? 1 : 0,
  };
}

export async function countPropertyRowsInHistoryTable({
  table,
  propertyId,
  periodId = null,
  baseId = null,
} = {}) {
  const token = airtableToken();
  const base = baseId || historyBaseId();
  if (!token || !base) {
    return { ok: false, count: 0, error: "AIRTABLE_CONFIG_MISSING" };
  }
  const clauses = [`{Property ID}='${String(propertyId).replace(/'/g, "\\'")}'`];
  if (periodId) {
    clauses.push(`{Period ID}='${String(periodId).replace(/'/g, "\\'")}'`);
  }
  const filter = clauses.length === 1 ? clauses[0] : `AND(${clauses.join(",")})`;
  const q = new URLSearchParams({
    pageSize: "100",
    filterByFormula: filter,
  });
  let offset = null;
  let count = 0;
  do {
    if (offset) q.set("offset", offset);
    else q.delete("offset");
    const url = `https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}?${q}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      return {
        ok: false,
        count,
        error: `Airtable ${res.status}: ${json?.error?.message || ""}`,
      };
    }
    count += (json.records || []).length;
    offset = json.offset || null;
  } while (offset);
  return { ok: true, count };
}

/**
 * Assess history sync for one property (read-only).
 */
export async function assessAdpAirtableHistorySyncGate({
  propertyId,
  periodId = null,
  requireCompleteForCustomerReady = true,
} = {}) {
  const policy = getAdpPersistencePolicy();
  const writesEnabled = isAdpHistoryWritesEnabled();
  const writeApply =
    process.env[HISTORY_ENV.ADP_HISTORY_AIRTABLE_WRITE_APPLY] === "true" ||
    process.env[HISTORY_ENV.ADP_HISTORY_AIRTABLE_WRITE_APPLY] === "1";
  const dest = resolveProductionHistoryDestination({ confirmGovernedAdpBase: true });
  const failures = [];
  const warnings = [];

  if (!airtableToken()) {
    return {
      gate: ADP_FULL_E2E_AIRTABLE_HISTORY_PERSISTENCE_GATE_V1,
      ok: false,
      blocksCustomerReady: requireCompleteForCustomerReady,
      status: HISTORY_SYNC_STATUS.LOOKUP_UNAVAILABLE,
      failures: ["AIRTABLE_TOKEN_MISSING"],
      warnings,
      policy,
      writesEnabled,
      writeApply,
      destination: dest,
      tables: NINE_HISTORY_TABLES,
      liveOverlayTable: EXISTING_LIVE_PUBLISH.table,
      note: "Cannot certify Airtable sync without read credentials",
    };
  }

  const coverage = {};
  /** Minimum tables that must be non-empty for AIRTABLE_SYNC_COMPLETE (Corrections may be empty). */
  const requiredForComplete = [
    HISTORY_TABLES.MONITORING_PERIODS,
    HISTORY_TABLES.REPORT_SNAPSHOTS,
    HISTORY_TABLES.PERIOD_METRICS,
    HISTORY_TABLES.EVIDENCE_INDEX,
    HISTORY_TABLES.PROMPT_LEDGER,
  ];
  let presentRequired = 0;
  for (const table of requiredForComplete) {
    const hit = await countPropertyRowsInHistoryTable({
      table,
      propertyId,
      periodId,
      baseId: dest.baseId,
    });
    coverage[table] = hit;
    if (hit.ok && hit.count > 0) presentRequired += 1;
    if (!hit.ok) failures.push(`LOOKUP_FAILED_${table}`);
    if (hit.ok && hit.count === 0) {
      warnings.push(`MISSING_ROWS_${table}`);
    }
  }

  let status = HISTORY_SYNC_STATUS.MISSING;
  if (presentRequired === requiredForComplete.length) {
    status = HISTORY_SYNC_STATUS.COMPLETE;
  } else if (presentRequired > 0) {
    status = HISTORY_SYNC_STATUS.PARTIAL;
  }

  if (!writesEnabled || !writeApply) {
    if (status !== HISTORY_SYNC_STATUS.COMPLETE) {
      status = HISTORY_SYNC_STATUS.AWAITING_HISTORY_WRITE_APPROVAL;
      warnings.push(
        "History tables incomplete — backfill requires founder ADP_HISTORY_WRITES_ENABLED + ADP_HISTORY_AIRTABLE_WRITE_APPLY"
      );
    }
  } else if (status !== HISTORY_SYNC_STATUS.COMPLETE) {
    failures.push("WRITE_AUTHORIZED_BUT_HISTORY_INCOMPLETE");
    status = HISTORY_SYNC_STATUS.WRITE_AUTHORIZED_READBACK_FAILED;
  }

  const ok =
    status === HISTORY_SYNC_STATUS.COMPLETE ||
    (!requireCompleteForCustomerReady &&
      status === HISTORY_SYNC_STATUS.AWAITING_HISTORY_WRITE_APPROVAL);

  // CUSTOMER_READY must not claim Airtable complete when awaiting approval
  const blocksCustomerReady =
    requireCompleteForCustomerReady && status !== HISTORY_SYNC_STATUS.COMPLETE;

  return {
    gate: ADP_FULL_E2E_AIRTABLE_HISTORY_PERSISTENCE_GATE_V1,
    ok: ok && failures.length === 0,
    blocksCustomerReady,
    status,
    failures,
    warnings,
    policy,
    writesEnabled,
    writeApply,
    destination: dest,
    propertyId,
    periodId,
    coverage,
    tables: NINE_HISTORY_TABLES,
    liveOverlayTable: EXISTING_LIVE_PUBLISH.table,
    liveOverlayRole: EXISTING_LIVE_PUBLISH.role,
    note: "Published Reports ≠ longitudinal history. COMPLETE requires Monitoring+Snapshot+Period Metrics+Evidence Index+Prompt Ledger. FS remains PRIMARY_SOT.",
    requiredForComplete,
  };
}

/** Negative fixtures — intentional defect detection. */
export function runAirtableHistoryPersistenceNegativeFixtures() {
  const results = [];
  // Staged→published confusion
  results.push({
    id: "STAGED_MUST_NOT_BE_MARKED_PUBLISHED",
    pass: EXISTING_LIVE_PUBLISH.longitudinalSafe === false,
  });
  results.push({
    id: "NINE_TABLES_INCLUDE_PROMPT_LEDGER",
    pass: NINE_HISTORY_TABLES.includes(HISTORY_TABLES.PROMPT_LEDGER),
  });
  // Writes default off
  const prev = process.env.ADP_HISTORY_WRITES_ENABLED;
  delete process.env.ADP_HISTORY_WRITES_ENABLED;
  results.push({
    id: "HISTORY_WRITES_DEFAULT_OFF",
    pass: isAdpHistoryWritesEnabled() === false,
  });
  if (prev != null) process.env.ADP_HISTORY_WRITES_ENABLED = prev;
  // Governed base isolation
  const dest = resolveProductionHistoryDestination({ confirmGovernedAdpBase: true });
  results.push({
    id: "HISTORY_BASE_IS_GOVERNED_ADP",
    pass: dest.baseId === ADP_GOVERNED_BASE_ID,
  });
  results.push({
    id: "LIVE_OVERLAY_NOT_USED_AS_HISTORY",
    pass: dest.isolation?.liveOverlayTableUsedForHistory === false,
  });
  return {
    ok: results.every((r) => r.pass),
    results,
  };
}
