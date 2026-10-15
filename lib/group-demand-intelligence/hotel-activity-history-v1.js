/**
 * Hotel-level research / import / publication activity history.
 *
 * Distinct from opportunity.updatedAt. Aggregates:
 * - legacy filesystem research runs (hotels/{id}/runs/)
 * - weekly-refresh state + run manifests
 * - durable activity ledger (hotels/{id}/activity/ledger-v1.json)
 * - opportunity evidence / verification stamps
 * - reviewed-package import metadata when present
 *
 * Missing history → status "HISTORY_UNAVAILABLE" (UI: "History unavailable"),
 * not "Never" / NEVER_RUN unless we can prove no run was recorded.
 */

import fs from "node:fs";
import path from "node:path";
import { hotelDir, listResearchRuns, resolveLastResearchAt } from "./repository.js";
import { loadWeeklyRefreshState } from "./workflow/weekly-refresh-state-v1.js";

export const HOTEL_ACTIVITY_HISTORY_VERSION = "gdi-hotel-activity-history-v1";

export const ACTIVITY_KIND = Object.freeze({
  RESEARCH_RUN: "research_run",
  WEEKLY_REFRESH: "weekly_refresh",
  CONTACT_ENRICHMENT: "contact_enrichment",
  REVIEWED_IMPORT: "reviewed_import",
  CANONICAL_PUBLICATION: "canonical_publication",
  REPROCESSING: "reprocessing",
});

const RUN_STATUS_LABEL = Object.freeze({
  completed: "Completed",
  COMPLETED: "Completed",
  partial: "Partial",
  PARTIAL: "Partial",
  failed: "Failed",
  FAILED: "Failed",
  running: "Running",
  RUNNING: "Running",
  pending: "Pending",
  PENDING: "Pending",
  blocked: "Blocked",
  BLOCKED: "Blocked",
  HISTORY_UNAVAILABLE: "History unavailable",
  NOT_YET_RUN: "Not yet run",
  NEVER_RUN: "Not yet run",
});

function activityLedgerPath(hotelId, env = process.env) {
  return path.join(hotelDir(hotelId, env), "activity", "ledger-v1.json");
}

function emptyLedger(hotelId) {
  return {
    version: HOTEL_ACTIVITY_HISTORY_VERSION,
    hotelId,
    events: [],
    updatedAt: null,
  };
}

export function loadHotelActivityLedger(hotelId, env = process.env) {
  const p = activityLedgerPath(hotelId, env);
  if (!fs.existsSync(p)) return emptyLedger(hotelId);
  try {
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    return {
      ...emptyLedger(hotelId),
      ...raw,
      events: Array.isArray(raw.events) ? raw.events : [],
    };
  } catch {
    return emptyLedger(hotelId);
  }
}

export function appendHotelActivityEvent(hotelId, event, env = process.env) {
  const ledger = loadHotelActivityLedger(hotelId, env);
  const nextEvent = {
    id:
      event.id ||
      `gdi_act_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
    kind: event.kind,
    status: event.status || null,
    at: event.at || new Date().toISOString(),
    runId: event.runId || null,
    source: event.source || null,
    provenance: event.provenance || "live_write",
    note: event.note || null,
    recovered: event.recovered === true,
  };
  const dup = ledger.events.find(
    (e) =>
      e.runId &&
      nextEvent.runId &&
      e.runId === nextEvent.runId &&
      e.kind === nextEvent.kind
  );
  if (!dup) ledger.events.push(nextEvent);
  ledger.updatedAt = new Date().toISOString();
  const dir = path.dirname(activityLedgerPath(hotelId, env));
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    activityLedgerPath(hotelId, env),
    JSON.stringify(ledger, null, 2) + "\n"
  );
  return nextEvent;
}

function listWeeklyRefreshRuns(hotelId, env = process.env) {
  const dir = path.join(hotelDir(hotelId, env), "weekly-refresh", "runs");
  if (!fs.existsSync(dir)) return [];
  const out = [];
  for (const name of fs.readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    const p = path.join(dir, name);
    try {
      const st = fs.statSync(p);
      if (!st.isFile()) continue;
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      out.push({
        id: raw.id || raw.runId || name.replace(/\.json$/i, ""),
        status: raw.status || null,
        startedAt: raw.startedAt || null,
        completedAt:
          raw.completedAt ||
          raw.finishedAt ||
          raw.lastSuccessfulRunAt ||
          null,
        kind: ACTIVITY_KIND.WEEKLY_REFRESH,
        source: "weekly-refresh/runs",
        provenance: "recovered_weekly_refresh_manifest",
        recovered: true,
      });
    } catch {
      // skip corrupt
    }
  }
  return out.sort((a, b) =>
    String(b.completedAt || b.startedAt || "").localeCompare(
      String(a.completedAt || a.startedAt || "")
    )
  );
}

function parseTs(v) {
  if (!v) return null;
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}

/**
 * Instance-like research run IDs (not static capability version strings).
 * Verifiable: weekly / paid-ms / dated canonical run tokens.
 */
function isInstanceResearchRunId(id) {
  const s = String(id || "").trim();
  if (!s || s.length < 12) return false;
  if (/^gdi_weekly_/i.test(s)) return true;
  if (/^gdi_paid_/i.test(s)) return true;
  if (/^gdi_act_/i.test(s)) return false;
  // Dated token e.g. pe_sd_may_canonical_2026_10_15
  if (/\d{4}[-_]\d{2}[-_]\d{2}/.test(s)) return true;
  if (/_\d{4}_\d{2}_\d{2}/.test(s)) return true;
  return false;
}

/**
 * Recover publication stamps from opportunities that carry an explicit
 * publication basis (reviewed package / PE / corporate canonical writers).
 * Does not treat bare updatedAt as publication proof.
 */
function publicationStampsFromOpportunities(opportunities = []) {
  const stamps = [];
  for (const o of opportunities || []) {
    if (!o || typeof o !== "object") continue;
    const basis = String(
      o.prospectPublicationBasis || o.publicationBasis || ""
    ).trim();
    if (!basis) continue;
    const candidates = [
      o.reviewedImportAt,
      o.canonicalPublishedAt,
      o.publishedAt,
      o.firstSeenAt,
      o.createdAt,
      o.lastVerifiedAt,
      o.lastResearchedAt,
    ];
    for (const c of candidates) {
      const t = parseTs(c);
      if (t) {
        stamps.push(t);
        break;
      }
    }
  }
  return stamps;
}

/**
 * Recover instance run rows from opportunity runId / researchVersion when
 * filesystem run ledgers are empty (common on ephemeral production FS).
 */
function recoveredRunsFromOpportunities(opportunities = []) {
  const byId = new Map();
  for (const o of opportunities || []) {
    if (!o || typeof o !== "object") continue;
    const id = o.runId || null;
    const version = o.researchVersion || null;
    const candidate = isInstanceResearchRunId(id)
      ? id
      : isInstanceResearchRunId(version)
        ? version
        : null;
    if (!candidate) continue;
    const at =
      parseTs(o.lastVerifiedAt) ||
      parseTs(o.lastResearchedAt) ||
      parseTs(o.hotelRelevanceResearchedAt) ||
      parseTs(o.createdAt) ||
      parseTs(o.firstSeenAt);
    if (!at) continue;
    const prev = byId.get(candidate);
    if (!prev || at > prev._ts) {
      byId.set(candidate, {
        id: candidate,
        status: "completed",
        startedAt: null,
        completedAt: new Date(at).toISOString(),
        kind: ACTIVITY_KIND.RESEARCH_RUN,
        source: "opportunity.runId",
        provenance: "recovered_opportunity_run_id",
        recovered: true,
        _ts: at,
      });
    }
  }
  return [...byId.values()].map(({ _ts, ...rest }) => rest);
}

/**
 * Build customer-facing hotel history summary.
 * @param {string} hotelId
 * @param {{ opportunities?: object[], opportunityDoc?: object }} [opts]
 */
export function buildHotelActivityHistory(hotelId, opts = {}) {
  const opportunities = opts.opportunities || [];
  const opportunityDoc = opts.opportunityDoc || {};
  const durableSummary = opportunityDoc.hotelActivitySummary || null;
  const legacyRuns = listResearchRuns(hotelId).map((r) => ({
    id: r.id,
    status: r.status,
    startedAt: r.startedAt,
    completedAt: r.completedAt,
    kind: ACTIVITY_KIND.RESEARCH_RUN,
    source: "runs/",
    provenance: "legacy_research_run",
    recovered: false,
  }));
  const weeklyRuns = listWeeklyRefreshRuns(hotelId);
  const weeklyState = loadWeeklyRefreshState(hotelId);
  const ledger = loadHotelActivityLedger(hotelId);
  const recoveredOppRuns = recoveredRunsFromOpportunities(opportunities);

  const researchRuns = [
    ...legacyRuns,
    ...weeklyRuns,
    ...recoveredOppRuns,
  ].sort((a, b) =>
    String(b.completedAt || b.startedAt || "").localeCompare(
      String(a.completedAt || a.startedAt || "")
    )
  );

  const lastResearchFromOpps = resolveLastResearchAt(opportunities, legacyRuns);
  const researchStamps = [];
  const lr = parseTs(lastResearchFromOpps);
  if (lr) researchStamps.push(lr);
  for (const r of researchRuns) {
    const t = parseTs(r.completedAt) || parseTs(r.startedAt);
    if (t) researchStamps.push(t);
  }
  if (weeklyState.lastSuccessfulRunAt) {
    const t = parseTs(weeklyState.lastSuccessfulRunAt);
    if (t) researchStamps.push(t);
  }
  // Research / weekly only — contact enrichment and imports are distinct.
  for (const e of ledger.events) {
    if (
      e.kind === ACTIVITY_KIND.RESEARCH_RUN ||
      e.kind === ACTIVITY_KIND.WEEKLY_REFRESH
    ) {
      const t = parseTs(e.at);
      if (t) researchStamps.push(t);
    }
  }
  if (durableSummary?.lastResearchAt) {
    const t = parseTs(durableSummary.lastResearchAt);
    if (t) researchStamps.push(t);
  }

  const lastResearchAt = researchStamps.length
    ? new Date(Math.max(...researchStamps)).toISOString()
    : null;

  const latestRun = researchRuns[0] || null;
  let runStatus = null;
  let runStatusLabel = null;
  let latestRunId = null;
  let runHistoryProvenance = null;

  if (latestRun) {
    runStatus = latestRun.status || "completed";
    runStatusLabel = RUN_STATUS_LABEL[runStatus] || String(runStatus);
    latestRunId = latestRun.id || null;
    runHistoryProvenance = latestRun.provenance || latestRun.source;
  } else if (weeklyState.lastSuccessfulRunId) {
    runStatus = "completed";
    runStatusLabel = "Completed";
    latestRunId = weeklyState.lastSuccessfulRunId;
    runHistoryProvenance = "weekly_refresh_state";
  } else if (durableSummary?.latestRunId || durableSummary?.runStatus) {
    runStatus = durableSummary.runStatus || "completed";
    runStatusLabel =
      RUN_STATUS_LABEL[runStatus] ||
      durableSummary.runStatusLabel ||
      String(runStatus);
    latestRunId = durableSummary.latestRunId || null;
    runHistoryProvenance = "durable_hotel_activity_summary";
  } else if (lastResearchAt) {
    // Evidence exists but no tracked runner file — do not claim NEVER_RUN.
    runStatus = "HISTORY_UNAVAILABLE";
    runStatusLabel = "History unavailable";
    runHistoryProvenance = "opportunity_evidence_without_run_ledger";
  } else {
    runStatus = "NOT_YET_RUN";
    runStatusLabel = "Not yet run";
    runHistoryProvenance = "empty_history";
  }

  const reviewedImportAt =
    opportunityDoc?.reviewedProspectImport?.importedAt ||
    opportunityDoc?.lastReviewedPackageImportAt ||
    durableSummary?.lastReviewedPackageImportAt ||
    null;

  const publicationStamps = [];
  const pubAt = parseTs(weeklyState.lastSuccessfulPublicationAt);
  if (pubAt) publicationStamps.push(pubAt);
  const revAt = parseTs(reviewedImportAt);
  if (revAt) publicationStamps.push(revAt);
  for (const e of ledger.events) {
    if (
      e.kind === ACTIVITY_KIND.CANONICAL_PUBLICATION ||
      e.kind === ACTIVITY_KIND.REVIEWED_IMPORT
    ) {
      const t = parseTs(e.at);
      if (t) publicationStamps.push(t);
    }
  }
  for (const t of publicationStampsFromOpportunities(opportunities)) {
    publicationStamps.push(t);
  }
  if (durableSummary?.lastPublishedUpdateAt) {
    const t = parseTs(durableSummary.lastPublishedUpdateAt);
    if (t) publicationStamps.push(t);
  }

  const lastPublishedUpdateAt = publicationStamps.length
    ? new Date(Math.max(...publicationStamps)).toISOString()
    : null;

  return {
    version: HOTEL_ACTIVITY_HISTORY_VERSION,
    hotelId,
    lastResearchAt,
    lastResearchProvenance: lastResearchAt
      ? "max_of_runs_and_opportunity_evidence"
      : "none",
    runStatus,
    runStatusLabel,
    latestRunId,
    runHistoryProvenance,
    lastReviewedPackageImportAt: reviewedImportAt,
    lastPublishedUpdateAt,
    lastPublishedUpdateLabel: lastPublishedUpdateAt
      ? "Last Published Update"
      : null,
    researchRunCount: researchRuns.length,
    recoveredWeeklyRunCount: weeklyRuns.length,
    recoveredOpportunityRunCount: recoveredOppRuns.length,
    ledgerEventCount: ledger.events.length,
  };
}

/**
 * Persist a compact durable summary onto an opportunity document payload
 * (FS mirror / next canonical save). Safe to merge into Airtable-backed docs.
 */
export function attachHotelActivitySummaryToDoc(hotelId, doc = {}, env = process.env) {
  const history = buildHotelActivityHistory(hotelId, {
    opportunities: doc.opportunities || [],
    opportunityDoc: doc,
  });
  return {
    ...doc,
    hotelActivitySummary: {
      version: HOTEL_ACTIVITY_HISTORY_VERSION,
      updatedAt: new Date().toISOString(),
      lastResearchAt: history.lastResearchAt,
      runStatus: history.runStatus,
      runStatusLabel: history.runStatusLabel,
      latestRunId: history.latestRunId,
      lastReviewedPackageImportAt: history.lastReviewedPackageImportAt,
      lastPublishedUpdateAt: history.lastPublishedUpdateAt,
      runHistoryProvenance: history.runHistoryProvenance,
    },
  };
}

/**
 * Recover weekly-refresh / legacy run manifests into the durable ledger once.
 * Preserves original timestamps; marks recovered provenance.
 */
export function recoverHotelActivityLedgerFromManifests(hotelId, env = process.env) {
  const weekly = listWeeklyRefreshRuns(hotelId, env);
  const legacy = listResearchRuns(hotelId).map((r) => ({
    id: r.id,
    status: r.status,
    completedAt: r.completedAt,
    startedAt: r.startedAt,
    kind: ACTIVITY_KIND.RESEARCH_RUN,
  }));
  const recovered = [];
  for (const r of [...weekly, ...legacy]) {
    const at = r.completedAt || r.startedAt;
    if (!r.id || !at) continue;
    const ev = appendHotelActivityEvent(
      hotelId,
      {
        kind: r.kind || ACTIVITY_KIND.WEEKLY_REFRESH,
        status: r.status || "completed",
        at,
        runId: r.id,
        source: r.source || "manifest_recovery",
        provenance: r.provenance || "recovered_manifest",
        recovered: true,
        note: "Recovered from verifiable run manifest; original timestamp preserved",
      },
      env
    );
    recovered.push(ev);
  }
  return recovered;
}

export function formatRunStatusForCustomer(status, label) {
  if (label) return label;
  if (!status) return "History unavailable";
  return RUN_STATUS_LABEL[status] || String(status);
}

export { RUN_STATUS_LABEL };
