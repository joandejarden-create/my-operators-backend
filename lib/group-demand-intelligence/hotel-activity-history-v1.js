/**
 * Hotel-level research / import / publication activity history.
 *
 * Distinct from opportunity.updatedAt. Aggregates:
 * - legacy filesystem research runs (hotels/{id}/runs/)
 * - weekly-refresh state + run manifests
 * - durable activity ledger (hotels/{id}/activity/ledger-v1.json)
 * - opportunity research-execution stamps (not bare verification)
 * - reviewed-package import / canonical publication metadata
 *
 * Customer aggregation excludes test clocks, dry-runs, fixtures, and
 * materially future timestamps (small clock-skew tolerance only).
 * Ineligible events remain in the ledger for audit (customerHistoryEligible=false).
 *
 * Missing history → status "HISTORY_UNAVAILABLE" (UI: "History unavailable"),
 * not "Never" / NEVER_RUN unless we can prove no run was recorded.
 * A runId alone never implies Completed.
 */

import fs from "node:fs";
import path from "node:path";
import { hotelDir, listResearchRuns } from "./repository.js";
import { loadWeeklyRefreshState } from "./workflow/weekly-refresh-state-v1.js";

export const HOTEL_ACTIVITY_HISTORY_VERSION = "gdi-hotel-activity-history-v2";

/** Allow small clock skew only — do not clamp future stamps to "now". */
export const CUSTOMER_HISTORY_CLOCK_SKEW_MS = 2 * 60 * 60 * 1000;

export const ACTIVITY_KIND = Object.freeze({
  RESEARCH_RUN: "research_run",
  WEEKLY_REFRESH: "weekly_refresh",
  CONTACT_ENRICHMENT: "contact_enrichment",
  REVIEWED_IMPORT: "reviewed_import",
  CANONICAL_PUBLICATION: "canonical_publication",
  REPROCESSING: "reprocessing",
  SOURCE_VERIFICATION: "source_verification",
});

export const CUSTOMER_HISTORY_EXCLUSION = Object.freeze({
  DRY_RUN: "dry_run",
  TEST_CLOCK: "test_or_simulated_clock",
  FUTURE_TIMESTAMP: "materially_future_timestamp",
  PLAN_OR_QUEUE_ONLY: "plan_or_queue_only",
  FIXTURE_OR_REPLAY: "fixture_or_replay",
  NO_COMPLETION_EVIDENCE: "no_explicit_completion_status",
  LIVE_WRITE_BLOCKED: "test_or_simulated_live_write_blocked",
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
  STATUS_UNAVAILABLE: "Status unavailable",
  NOT_YET_RUN: "Not yet run",
  NEVER_RUN: "Not yet run",
});

const EXPLICIT_COMPLETION_STATUSES = new Set([
  "completed",
  "COMPLETED",
  "partial",
  "PARTIAL",
  "failed",
  "FAILED",
  "blocked",
  "BLOCKED",
  "running",
  "RUNNING",
  "pending",
  "PENDING",
]);

const TEST_CLOCK_RE =
  /isolated_future|future_clock|test_clock|simulated|sim_clock|fixture_clock|replay_clock/i;

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

function saveLedger(hotelId, ledger, env = process.env) {
  const dir = path.dirname(activityLedgerPath(hotelId, env));
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    activityLedgerPath(hotelId, env),
    JSON.stringify(ledger, null, 2) + "\n"
  );
}

export function parseTs(v) {
  if (!v) return null;
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}

function isTestOrSimulatedClock(clockLabel) {
  const s = String(clockLabel || "").trim();
  if (!s) return false;
  return TEST_CLOCK_RE.test(s);
}

function isFixtureOrReplayProvenance(provenance, source) {
  const blob = `${provenance || ""} ${source || ""}`.toLowerCase();
  return (
    /fixture|replay|synthetic|seed_only|pilot_sim/.test(blob) &&
    !/recovered_weekly_refresh_manifest|recovered_manifest|live_write/.test(blob)
  );
}

/**
 * Classify whether an activity candidate may enter customer-facing history.
 * Preserves original timestamps — never clamps future stamps to now.
 */
export function classifyCustomerHistoryEligibility(candidate = {}, opts = {}) {
  const nowMs = parseTs(opts.now) ?? Date.now();
  const skewMs =
    Number.isFinite(opts.clockSkewMs) && opts.clockSkewMs >= 0
      ? opts.clockSkewMs
      : CUSTOMER_HISTORY_CLOCK_SKEW_MS;

  const dryRun = candidate.dryRun === true;
  const clockLabel = candidate.clockLabel || null;
  const atMs =
    parseTs(candidate.at) ||
    parseTs(candidate.completedAt) ||
    parseTs(candidate.startedAt);
  const status = candidate.status || null;
  const planOrQueueOnly =
    candidate.planOnly === true ||
    candidate.queueOnly === true ||
    String(candidate.runMode || "").toLowerCase() === "plan_only" ||
    String(candidate.runMode || "").toLowerCase() === "queue_only";

  const reasons = [];
  if (dryRun) reasons.push(CUSTOMER_HISTORY_EXCLUSION.DRY_RUN);
  if (isTestOrSimulatedClock(clockLabel)) {
    reasons.push(CUSTOMER_HISTORY_EXCLUSION.TEST_CLOCK);
  }
  if (atMs != null && atMs > nowMs + skewMs) {
    reasons.push(CUSTOMER_HISTORY_EXCLUSION.FUTURE_TIMESTAMP);
  }
  if (planOrQueueOnly) {
    reasons.push(CUSTOMER_HISTORY_EXCLUSION.PLAN_OR_QUEUE_ONLY);
  }
  if (isFixtureOrReplayProvenance(candidate.provenance, candidate.source)) {
    reasons.push(CUSTOMER_HISTORY_EXCLUSION.FIXTURE_OR_REPLAY);
  }
  if (candidate.requireCompletionStatus !== false) {
    const hasStatus =
      status != null &&
      String(status).trim() !== "" &&
      EXPLICIT_COMPLETION_STATUSES.has(String(status));
    // Recovered runId-only rows intentionally omit status → ineligible for Completed.
    if (candidate.completionEvidenceRequired && !hasStatus) {
      reasons.push(CUSTOMER_HISTORY_EXCLUSION.NO_COMPLETION_EVIDENCE);
    }
  }

  return {
    eligible: reasons.length === 0,
    reasons,
    atMs,
    dryRun,
    clockLabel,
    status,
  };
}

/**
 * Append an activity event. Test/simulated/future live writes are retained
 * for audit with customerHistoryEligible=false and are not customer-maxed.
 * Recovery paths may forceWrite=true to keep the audit trail.
 */
export function appendHotelActivityEvent(hotelId, event, env = process.env) {
  const ledger = loadHotelActivityLedger(hotelId, env);
  const classification = classifyCustomerHistoryEligibility(event, {
    now: env.GDI_HISTORY_NOW || undefined,
  });

  const isLiveWrite = (event.provenance || "live_write") === "live_write";
  const blockedLiveTestWrite =
    isLiveWrite &&
    event.forceWrite !== true &&
    (classification.reasons.includes(CUSTOMER_HISTORY_EXCLUSION.DRY_RUN) ||
      classification.reasons.includes(CUSTOMER_HISTORY_EXCLUSION.TEST_CLOCK) ||
      classification.reasons.includes(CUSTOMER_HISTORY_EXCLUSION.FUTURE_TIMESTAMP));

  if (blockedLiveTestWrite) {
    // Still record for audit when forceAuditWrite, else refuse production pollution.
    if (event.forceAuditWrite !== true) {
      return {
        skipped: true,
        reason: CUSTOMER_HISTORY_EXCLUSION.LIVE_WRITE_BLOCKED,
        eligibilityReasons: classification.reasons,
      };
    }
  }

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
    dryRun: event.dryRun === true,
    clockLabel: event.clockLabel || null,
    planOnly: event.planOnly === true,
    queueOnly: event.queueOnly === true,
    runMode: event.runMode || null,
    customerHistoryEligible: classification.eligible && !blockedLiveTestWrite,
    customerHistoryExclusionReasons: classification.eligible
      ? []
      : classification.reasons.concat(
          blockedLiveTestWrite ? [CUSTOMER_HISTORY_EXCLUSION.LIVE_WRITE_BLOCKED] : []
        ),
  };

  const dup = ledger.events.find(
    (e) =>
      e.runId &&
      nextEvent.runId &&
      e.runId === nextEvent.runId &&
      e.kind === nextEvent.kind
  );
  if (!dup) {
    ledger.events.push(nextEvent);
  } else {
    // Refresh eligibility annotation without mutating original `at`.
    Object.assign(dup, {
      dryRun: nextEvent.dryRun || dup.dryRun,
      clockLabel: nextEvent.clockLabel || dup.clockLabel,
      planOnly: nextEvent.planOnly || dup.planOnly,
      queueOnly: nextEvent.queueOnly || dup.queueOnly,
      runMode: nextEvent.runMode || dup.runMode,
      customerHistoryEligible: nextEvent.customerHistoryEligible,
      customerHistoryExclusionReasons: nextEvent.customerHistoryExclusionReasons,
      status: dup.status || nextEvent.status,
    });
  }
  ledger.updatedAt = new Date().toISOString();
  ledger.version = HOTEL_ACTIVITY_HISTORY_VERSION;
  saveLedger(hotelId, ledger, env);
  return dup || nextEvent;
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
      const dryRun = raw.dryRun === true;
      const clockLabel = raw.clockLabel || null;
      const runMode = raw.runMode || raw.queues?.runMode || null;
      // Queue-building / plan-only: no executed research changes & no provider results.
      const hasExecutionEvidence =
        (Array.isArray(raw.changes) && raw.changes.length > 0) ||
        (Array.isArray(raw.hotelFitReassessments) &&
          raw.hotelFitReassessments.length > 0) ||
        (raw.summary &&
          (raw.summary.researchExecuted === true ||
            raw.summary.accountsResearched > 0 ||
            raw.summary.providerJobsCompleted > 0)) ||
        (Array.isArray(raw.providerJobIds) && raw.providerJobIds.length > 0);
      const queueOnly =
        dryRun === false &&
        !hasExecutionEvidence &&
        (runMode === "queue_only" ||
          runMode === "plan_only" ||
          (raw.queues && !raw.scorecard && !hasExecutionEvidence));

      out.push({
        id: raw.id || raw.runId || name.replace(/\.json$/i, ""),
        status: raw.status || null,
        startedAt: raw.startedAt || null,
        completedAt:
          raw.completedAt ||
          raw.finishedAt ||
          raw.endedAt ||
          null,
        kind: ACTIVITY_KIND.WEEKLY_REFRESH,
        source: "weekly-refresh/runs",
        provenance: "recovered_weekly_refresh_manifest",
        recovered: true,
        dryRun,
        clockLabel,
        runMode,
        queueOnly: queueOnly === true,
        planOnly: runMode === "plan_only",
        hasExecutionEvidence: hasExecutionEvidence === true,
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
  if (/\d{4}[-_]\d{2}[-_]\d{2}/.test(s)) return true;
  if (/_\d{4}_\d{2}_\d{2}/.test(s)) return true;
  return false;
}

/**
 * Recover publication stamps from opportunities that carry an explicit
 * publication basis (reviewed package / PE / corporate canonical writers).
 * Does not treat bare updatedAt, lastVerifiedAt, or lastResearchedAt as publication.
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
      o.canonicalPublishedAt,
      o.publishedAt,
      o.reviewedImportAt,
      o.firstPublishedAt,
    ];
    // createdAt / firstSeenAt only when basis proves a successful apply path
    // and an explicit published/import stamp is absent — still not updatedAt.
    let hit = false;
    for (const c of candidates) {
      const t = parseTs(c);
      if (t) {
        stamps.push(t);
        hit = true;
        break;
      }
    }
    if (!hit && /REVIEWED_PACKAGE|CANONICAL_PUBLISH|AIRTABLE_APPLY/i.test(basis)) {
      const t = parseTs(o.firstSeenAt) || parseTs(o.createdAt);
      if (t) stamps.push(t);
    }
  }
  return stamps;
}

/**
 * Source verification stamps (separate from Last Research).
 */
function sourceVerificationStampsFromOpportunities(opportunities = []) {
  const stamps = [];
  for (const o of opportunities || []) {
    if (!o || typeof o !== "object") continue;
    const candidates = [o.lastVerifiedAt];
    if (Array.isArray(o.evidence)) {
      for (const e of o.evidence) {
        if (!e || typeof e !== "object") continue;
        candidates.push(e.verifiedAt, e.checkedAt);
      }
    }
    for (const c of candidates) {
      const t = parseTs(c);
      if (t) stamps.push(t);
    }
  }
  return stamps;
}

/**
 * Executed research stamps from opportunities — not verification, not runId alone.
 */
function executedResearchStampsFromOpportunities(opportunities = []) {
  const stamps = [];
  for (const o of opportunities || []) {
    if (!o || typeof o !== "object") continue;
    const audit = o.contactResearchAudit;
    if (audit && typeof audit === "object") {
      const auditStatus = String(audit.status || audit.completionStatus || "").toLowerCase();
      if (
        auditStatus === "completed" ||
        auditStatus === "partial" ||
        audit.completedAt ||
        audit.researchedAt
      ) {
        const t =
          parseTs(audit.completedAt) ||
          parseTs(audit.researchedAt) ||
          parseTs(o.lastResearchedAt);
        if (t) stamps.push(t);
      }
    }
    // Explicit research completion field only (not lastVerifiedAt).
    const t = parseTs(o.lastResearchedAt) || parseTs(o.hotelRelevanceResearchedAt);
    if (t && (o.researchCompleted === true || o.researchExecutionStatus)) {
      const st = String(o.researchExecutionStatus || "").toLowerCase();
      if (
        o.researchCompleted === true ||
        st === "completed" ||
        st === "partial"
      ) {
        stamps.push(t);
      }
    }
  }
  return stamps;
}

/**
 * Opportunity runId rows are identity anchors only — never invent Completed.
 */
function recoveredRunAnchorsFromOpportunities(opportunities = []) {
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
      parseTs(o.lastResearchedAt) ||
      parseTs(o.hotelRelevanceResearchedAt) ||
      parseTs(o.createdAt) ||
      parseTs(o.firstSeenAt);
    if (!at) continue;
    const prev = byId.get(candidate);
    if (!prev || at > prev._ts) {
      byId.set(candidate, {
        id: candidate,
        // Intentionally no status — runId alone ≠ Completed
        status: null,
        startedAt: null,
        completedAt: null,
        kind: ACTIVITY_KIND.RESEARCH_RUN,
        source: "opportunity.runId",
        provenance: "recovered_opportunity_run_id",
        recovered: true,
        completionEvidenceRequired: true,
        customerHistoryEligible: false,
        customerHistoryExclusionReasons: [
          CUSTOMER_HISTORY_EXCLUSION.NO_COMPLETION_EVIDENCE,
        ],
        _ts: at,
      });
    }
  }
  return [...byId.values()].map(({ _ts, ...rest }) => rest);
}

function eventEligibleForCustomer(event, opts = {}) {
  if (event && event.customerHistoryEligible === false) return false;
  const c = classifyCustomerHistoryEligibility(event, opts);
  return c.eligible;
}

function normalizeRunStatus(status) {
  if (status == null || String(status).trim() === "") return null;
  const s = String(status);
  if (!EXPLICIT_COMPLETION_STATUSES.has(s)) return null;
  return s;
}

/**
 * Build customer-facing hotel history summary.
 * @param {string} hotelId
 * @param {{ opportunities?: object[], opportunityDoc?: object, now?: string, env?: object }} [opts]
 */
export function buildHotelActivityHistory(hotelId, opts = {}) {
  const env = opts.env || process.env;
  const nowIso = opts.now || env.GDI_HISTORY_NOW || new Date().toISOString();
  const eligibilityOpts = { now: nowIso };
  const opportunities = opts.opportunities || [];
  const opportunityDoc = opts.opportunityDoc || {};
  const durableSummary = opportunityDoc.hotelActivitySummary || null;

  const legacyRuns = listResearchRuns(hotelId).map((r) => ({
    id: r.id,
    status: r.status || null,
    startedAt: r.startedAt,
    completedAt: r.completedAt,
    kind: ACTIVITY_KIND.RESEARCH_RUN,
    source: "runs/",
    provenance: "legacy_research_run",
    recovered: false,
    dryRun: r.dryRun === true,
    clockLabel: r.clockLabel || null,
  }));
  const weeklyRuns = listWeeklyRefreshRuns(hotelId, env);
  const weeklyState = loadWeeklyRefreshState(hotelId, env);
  const ledger = loadHotelActivityLedger(hotelId, env);
  const recoveredOppAnchors = recoveredRunAnchorsFromOpportunities(opportunities);

  // Enrich ledger rows from weekly manifests when recovery omitted dryRun/clockLabel.
  const weeklyById = new Map(weeklyRuns.map((r) => [r.id, r]));
  const ledgerRunCandidates = ledger.events
    .filter(
      (e) =>
        e.kind === ACTIVITY_KIND.RESEARCH_RUN ||
        e.kind === ACTIVITY_KIND.WEEKLY_REFRESH
    )
    .map((e) => {
      const manifest = e.runId ? weeklyById.get(e.runId) : null;
      const dryRun = e.dryRun === true || manifest?.dryRun === true;
      const clockLabel = e.clockLabel || manifest?.clockLabel || null;
      const planOnly = e.planOnly === true || manifest?.planOnly === true;
      const queueOnly = e.queueOnly === true || manifest?.queueOnly === true;
      const runMode = e.runMode || manifest?.runMode || null;
      const enriched = {
        id: e.runId || e.id,
        status: e.status || null,
        startedAt: null,
        completedAt: e.at || null,
        kind: e.kind,
        source: e.source || "activity/ledger",
        provenance: e.provenance || "activity_ledger",
        recovered: e.recovered === true,
        dryRun,
        clockLabel,
        planOnly,
        queueOnly,
        runMode,
      };
      // Recompute eligibility from enriched fields — do not trust stale true flags.
      const classification = classifyCustomerHistoryEligibility(enriched, eligibilityOpts);
      return {
        ...enriched,
        customerHistoryEligible: classification.eligible,
        customerHistoryExclusionReasons: classification.reasons,
      };
    });

  const allRunCandidates = [...legacyRuns, ...weeklyRuns, ...ledgerRunCandidates];
  const eligibleRuns = allRunCandidates
    .filter((r) => eventEligibleForCustomer(r, eligibilityOpts))
    .filter((r) => normalizeRunStatus(r.status))
    .sort((a, b) =>
      String(b.completedAt || b.startedAt || "").localeCompare(
        String(a.completedAt || a.startedAt || "")
      )
    );

  const researchStamps = [];
  for (const r of eligibleRuns) {
    const t = parseTs(r.completedAt) || parseTs(r.startedAt);
    if (t) researchStamps.push(t);
  }

  // Weekly state lastSuccessfulRunAt only if matching journal is customer-eligible.
  if (weeklyState.lastSuccessfulRunAt && weeklyState.lastSuccessfulRunId) {
    const match = weeklyRuns.find((r) => r.id === weeklyState.lastSuccessfulRunId);
    if (match && eventEligibleForCustomer(match, eligibilityOpts)) {
      const t = parseTs(weeklyState.lastSuccessfulRunAt);
      if (t) researchStamps.push(t);
    }
  }

  for (const t of executedResearchStampsFromOpportunities(opportunities)) {
    if (t <= Date.parse(nowIso) + CUSTOMER_HISTORY_CLOCK_SKEW_MS) {
      researchStamps.push(t);
    }
  }

  // Durable summary only when its stamp is not future and not known-ineligible.
  if (durableSummary?.lastResearchAt) {
    const t = parseTs(durableSummary.lastResearchAt);
    const durableEligible =
      t != null &&
      t <= Date.parse(nowIso) + CUSTOMER_HISTORY_CLOCK_SKEW_MS &&
      durableSummary.customerHistoryEligible !== false;
    if (durableEligible) researchStamps.push(t);
  }

  const lastResearchAt = researchStamps.length
    ? new Date(Math.max(...researchStamps)).toISOString()
    : null;

  const verificationStamps = sourceVerificationStampsFromOpportunities(
    opportunities
  ).filter((t) => t <= Date.parse(nowIso) + CUSTOMER_HISTORY_CLOCK_SKEW_MS);
  const lastSourceVerificationAt = verificationStamps.length
    ? new Date(Math.max(...verificationStamps)).toISOString()
    : null;

  const latestRun = eligibleRuns[0] || null;
  let runStatus = null;
  let runStatusLabel = null;
  let latestRunId = null;
  let runHistoryProvenance = null;

  if (latestRun && normalizeRunStatus(latestRun.status)) {
    runStatus = latestRun.status;
    runStatusLabel = RUN_STATUS_LABEL[runStatus] || String(runStatus);
    latestRunId = latestRun.id || null;
    runHistoryProvenance = latestRun.provenance || latestRun.source;
  } else if (
    weeklyState.lastSuccessfulRunId &&
    weeklyRuns.some(
      (r) =>
        r.id === weeklyState.lastSuccessfulRunId &&
        eventEligibleForCustomer(r, eligibilityOpts) &&
        normalizeRunStatus(r.status)
    )
  ) {
    const match = weeklyRuns.find((r) => r.id === weeklyState.lastSuccessfulRunId);
    runStatus = match.status;
    runStatusLabel = RUN_STATUS_LABEL[runStatus] || String(runStatus);
    latestRunId = weeklyState.lastSuccessfulRunId;
    runHistoryProvenance = "weekly_refresh_state";
  } else if (
    durableSummary?.latestRunId &&
    durableSummary?.runStatus &&
    normalizeRunStatus(durableSummary.runStatus) &&
    durableSummary.customerHistoryEligible !== false
  ) {
    runStatus = durableSummary.runStatus;
    runStatusLabel =
      RUN_STATUS_LABEL[runStatus] ||
      durableSummary.runStatusLabel ||
      String(runStatus);
    latestRunId = durableSummary.latestRunId || null;
    runHistoryProvenance = "durable_hotel_activity_summary";
  } else if (recoveredOppAnchors.length && !lastResearchAt) {
    // runId present without completion evidence
    runStatus = "STATUS_UNAVAILABLE";
    runStatusLabel = "Status unavailable";
    latestRunId = recoveredOppAnchors[0].id;
    runHistoryProvenance = "opportunity_run_id_without_completion";
  } else if (lastResearchAt || lastSourceVerificationAt) {
    runStatus = "HISTORY_UNAVAILABLE";
    runStatusLabel = "History unavailable";
    runHistoryProvenance = lastResearchAt
      ? "opportunity_research_without_run_ledger"
      : "source_verification_without_run_ledger";
  } else {
    runStatus = "NOT_YET_RUN";
    runStatusLabel = "Not yet run";
    runHistoryProvenance = "empty_history";
  }

  const reviewedImportAt =
    opportunityDoc?.reviewedProspectImport?.importedAt ||
    opportunityDoc?.lastReviewedPackageImportAt ||
    (durableSummary?.lastReviewedPackageImportAt &&
    durableSummary.customerHistoryEligible !== false
      ? durableSummary.lastReviewedPackageImportAt
      : null) ||
    null;

  const publicationStamps = [];
  const pubAt = parseTs(weeklyState.lastSuccessfulPublicationAt);
  if (pubAt && pubAt <= Date.parse(nowIso) + CUSTOMER_HISTORY_CLOCK_SKEW_MS) {
    publicationStamps.push(pubAt);
  }
  const revAt = parseTs(reviewedImportAt);
  if (revAt && revAt <= Date.parse(nowIso) + CUSTOMER_HISTORY_CLOCK_SKEW_MS) {
    publicationStamps.push(revAt);
  }
  for (const e of ledger.events) {
    if (
      e.kind !== ACTIVITY_KIND.CANONICAL_PUBLICATION &&
      e.kind !== ACTIVITY_KIND.REVIEWED_IMPORT
    ) {
      continue;
    }
    if (!eventEligibleForCustomer(e, eligibilityOpts)) continue;
    const t = parseTs(e.at);
    if (t) publicationStamps.push(t);
  }
  for (const t of publicationStampsFromOpportunities(opportunities)) {
    if (t <= Date.parse(nowIso) + CUSTOMER_HISTORY_CLOCK_SKEW_MS) {
      publicationStamps.push(t);
    }
  }
  if (
    durableSummary?.lastPublishedUpdateAt &&
    durableSummary.customerHistoryEligible !== false
  ) {
    const t = parseTs(durableSummary.lastPublishedUpdateAt);
    if (t && t <= Date.parse(nowIso) + CUSTOMER_HISTORY_CLOCK_SKEW_MS) {
      publicationStamps.push(t);
    }
  }

  const lastPublishedUpdateAt = publicationStamps.length
    ? new Date(Math.max(...publicationStamps)).toISOString()
    : null;

  const excludedWeekly = weeklyRuns.filter(
    (r) => !eventEligibleForCustomer(r, eligibilityOpts)
  );
  const excludedLedger = ledger.events.filter(
    (e) =>
      (e.kind === ACTIVITY_KIND.RESEARCH_RUN ||
        e.kind === ACTIVITY_KIND.WEEKLY_REFRESH) &&
      !eventEligibleForCustomer(e, eligibilityOpts)
  );

  return {
    version: HOTEL_ACTIVITY_HISTORY_VERSION,
    hotelId,
    lastResearchAt,
    lastResearchProvenance: lastResearchAt
      ? "max_of_eligible_executed_research"
      : "none",
    lastSourceVerificationAt,
    lastSourceVerificationLabel: lastSourceVerificationAt
      ? "Last Source Verification"
      : null,
    runStatus,
    runStatusLabel,
    latestRunId,
    runHistoryProvenance,
    lastReviewedPackageImportAt: reviewedImportAt,
    lastPublishedUpdateAt,
    lastPublishedUpdateLabel: lastPublishedUpdateAt
      ? "Last Published Update"
      : null,
    researchRunCount: eligibleRuns.length,
    recoveredWeeklyRunCount: weeklyRuns.length,
    recoveredOpportunityRunCount: recoveredOppAnchors.length,
    ledgerEventCount: ledger.events.length,
    excludedFromCustomerHistory: {
      weeklyManifestCount: excludedWeekly.length,
      ledgerEventCount: excludedLedger.length,
      reasonsSample: [
        ...excludedWeekly.slice(0, 3).map((r) => ({
          runId: r.id,
          reasons: classifyCustomerHistoryEligibility(r, eligibilityOpts).reasons,
        })),
        ...excludedLedger.slice(0, 3).map((e) => ({
          runId: e.runId,
          reasons:
            e.customerHistoryExclusionReasons ||
            classifyCustomerHistoryEligibility(e, eligibilityOpts).reasons,
        })),
      ],
    },
    clockSkewToleranceMs: CUSTOMER_HISTORY_CLOCK_SKEW_MS,
    evaluatedAt: nowIso,
  };
}

/**
 * Persist a compact durable summary onto an opportunity document payload
 * (FS mirror / next canonical save). Survives redeploys when the opportunity
 * doc is stored in Airtable/FS — recomputation remains authoritative.
 */
export function attachHotelActivitySummaryToDoc(hotelId, doc = {}, env = process.env) {
  const history = buildHotelActivityHistory(hotelId, {
    opportunities: doc.opportunities || [],
    opportunityDoc: doc,
    env,
  });
  return {
    ...doc,
    hotelActivitySummary: {
      version: HOTEL_ACTIVITY_HISTORY_VERSION,
      updatedAt: new Date().toISOString(),
      lastResearchAt: history.lastResearchAt,
      lastSourceVerificationAt: history.lastSourceVerificationAt || null,
      runStatus: history.runStatus,
      runStatusLabel: history.runStatusLabel,
      latestRunId: history.latestRunId,
      lastReviewedPackageImportAt: history.lastReviewedPackageImportAt,
      lastPublishedUpdateAt: history.lastPublishedUpdateAt,
      runHistoryProvenance: history.runHistoryProvenance,
      customerHistoryEligible: true,
      excludedFromCustomerHistory: history.excludedFromCustomerHistory || null,
    },
  };
}

/**
 * Recover weekly-refresh / legacy run manifests into the durable ledger.
 * Preserves original timestamps; marks recovered provenance + eligibility.
 * Does not invent Completed for dry-run / test-clock / future manifests
 * in the customer aggregation (customerHistoryEligible=false).
 */
export function recoverHotelActivityLedgerFromManifests(hotelId, env = process.env) {
  const weekly = listWeeklyRefreshRuns(hotelId, env);
  const legacy = listResearchRuns(hotelId).map((r) => ({
    id: r.id,
    status: r.status || null,
    completedAt: r.completedAt,
    startedAt: r.startedAt,
    kind: ACTIVITY_KIND.RESEARCH_RUN,
    dryRun: r.dryRun === true,
    clockLabel: r.clockLabel || null,
    source: "runs/",
    provenance: "recovered_legacy_run_manifest",
  }));
  const recovered = [];
  for (const r of [...weekly, ...legacy]) {
    const at = r.completedAt || r.startedAt;
    if (!r.id || !at) continue;
    const ev = appendHotelActivityEvent(
      hotelId,
      {
        kind: r.kind || ACTIVITY_KIND.WEEKLY_REFRESH,
        status: r.status || null,
        at,
        runId: r.id,
        source: r.source || "manifest_recovery",
        provenance: r.provenance || "recovered_manifest",
        recovered: true,
        dryRun: r.dryRun === true,
        clockLabel: r.clockLabel || null,
        planOnly: r.planOnly === true,
        queueOnly: r.queueOnly === true,
        runMode: r.runMode || null,
        forceAuditWrite: true,
        note: "Recovered from run manifest; original timestamp preserved; eligibility annotated",
      },
      env
    );
    recovered.push(ev);
  }
  return recovered;
}

/**
 * Re-annotate existing ledger events for eligibility without changing `at`.
 */
export function reannotateHotelActivityLedgerEligibility(hotelId, env = process.env) {
  const ledger = loadHotelActivityLedger(hotelId, env);
  const weeklyById = new Map(
    listWeeklyRefreshRuns(hotelId, env).map((r) => [r.id, r])
  );
  let changed = 0;
  for (const e of ledger.events) {
    const manifest = e.runId ? weeklyById.get(e.runId) : null;
    if (manifest) {
      if (manifest.dryRun === true) e.dryRun = true;
      if (manifest.clockLabel) e.clockLabel = manifest.clockLabel;
      if (manifest.planOnly === true) e.planOnly = true;
      if (manifest.queueOnly === true) e.queueOnly = true;
      if (manifest.runMode) e.runMode = e.runMode || manifest.runMode;
    }
    const c = classifyCustomerHistoryEligibility(e, {
      now: env.GDI_HISTORY_NOW || undefined,
    });
    const nextEligible = c.eligible;
    const nextReasons = c.reasons;
    if (
      e.customerHistoryEligible !== nextEligible ||
      JSON.stringify(e.customerHistoryExclusionReasons || []) !==
        JSON.stringify(nextReasons) ||
      (manifest && e.dryRun !== true && manifest.dryRun === true)
    ) {
      e.customerHistoryEligible = nextEligible;
      e.customerHistoryExclusionReasons = nextReasons;
      changed += 1;
    }
  }
  if (changed) {
    ledger.updatedAt = new Date().toISOString();
    ledger.version = HOTEL_ACTIVITY_HISTORY_VERSION;
    saveLedger(hotelId, ledger, env);
  }
  return { changed, total: ledger.events.length };
}

export function formatRunStatusForCustomer(status, label) {
  if (label) return label;
  if (!status) return "History unavailable";
  return RUN_STATUS_LABEL[status] || String(status);
}

export { RUN_STATUS_LABEL };
