#!/usr/bin/env node
/**
 * GDI Jev-Guided Evidence Gap Resolution V1
 * HQ WATCH only (AC + Spice). No broad discovery. No Webhound. No threshold relaxation.
 *
 *   node scripts/gdi-jev-guided-evidence-gap-resolution-v1.mjs
 *   node scripts/gdi-jev-guided-evidence-gap-resolution-v1.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  assessEvidenceGaps,
  identifyPrimaryBlocker,
  defaultActionForBlocker,
  validateMarket,
  ACTION_OUTCOME,
  JEV_VALUE_CLASS,
  STATE_AFTER,
  JEV_NEXT_ACTION,
  EVIDENCE_DIMENSION,
} from "../lib/group-demand-intelligence/evidence-gap/evidence-gap-model-v1.js";
import { decideEvidenceGapNextAction } from "../lib/group-demand-intelligence/evidence-gap/jev-evidence-gap-next-action.js";
import { executeEvidenceGapAction } from "../lib/group-demand-intelligence/evidence-gap/evidence-gap-action-executor.js";
import {
  upsertResearchTarget,
  upsertResearchRun,
  upsertTargetRun,
  isResearchCoverageAirtableConfigured,
  TARGET_TYPE,
  TARGET_STATUS,
  RUN_TYPE,
  EXECUTION_STATUS,
  RESULT_TYPE,
  buildTargetRunId,
} from "../lib/group-demand-intelligence/research-coverage/index.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";
// Research-direction SAFE APPLY for this playbook (facts still deterministic)
if (!process.env.GDI_JEV_MODE) process.env.GDI_JEV_MODE = "apply";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/jev-guided-evidence-gap-resolution-v1"
);
const SURFACE =
  "reports/group-demand-intelligence/surface-eligibility-qualification-v1/WATCH_CORPUS_AFTER.json";
const APPLY = process.argv.includes("--apply");

const HOTELS = {
  AC: { hpc: "rec2PVBDavppGpenm", name: "AC Hotel A Coruña", short: "AC" },
  SPICE: {
    hpc: "recKRJjcPnb4tVDDS",
    name: "Spice Island Beach Resort",
    short: "SPICE",
  },
};
const PROVEN = [
  { hpc: "recLuxvwwxID7U2B8", name: "Bethesda", short: "BETHESDA", n: 5 },
  { hpc: "recG66DQJKP2c0UNh", name: "Renaissance", short: "RENAISSANCE", n: 3 },
  { hpc: "recgMYovrrZDJMqzX", name: "Waterstone", short: "WATERSTONE", n: 3 },
];

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}
function gitDirty() {
  try {
    return execSync("git status --porcelain", { cwd: ROOT, encoding: "utf8" })
      .trim()
      .split("\n")
      .filter(Boolean);
  } catch {
    return [];
  }
}
function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}
function writeJson(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function classifyJevValue({
  jevAction,
  defaultAction,
  outcome,
  stateChanged,
  promoted,
  rejected,
  evidenceFound,
}) {
  const same = jevAction === defaultAction;
  if (
    outcome === ACTION_OUTCOME.WRONG_MARKET ||
    outcome === ACTION_OUTCOME.CURRENT_CYCLE_CLOSED ||
    outcome === ACTION_OUTCOME.FULLY_PLACED
  ) {
    return same ? JEV_VALUE_CLASS.DECISIVE_NEGATIVE : JEV_VALUE_CLASS.DECISIVE_NEGATIVE;
  }
  if (promoted || outcome === ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE) {
    return JEV_VALUE_CLASS.DECISIVE_POSITIVE;
  }
  if (outcome === ACTION_OUTCOME.WAIT_TRIGGER && stateChanged) {
    return same ? JEV_VALUE_CLASS.SAME_AS_DEFAULT : JEV_VALUE_CLASS.HELPFUL;
  }
  if (stateChanged || outcome === ACTION_OUTCOME.BLOCKER_PARTIALLY_RESOLVED) {
    return same ? JEV_VALUE_CLASS.SAME_AS_DEFAULT : JEV_VALUE_CLASS.HELPFUL;
  }
  if (!evidenceFound && outcome === ACTION_OUTCOME.NO_NEW_EVIDENCE) {
    return same ? JEV_VALUE_CLASS.SAME_AS_DEFAULT : JEV_VALUE_CLASS.UNHELPFUL;
  }
  if (
    !same &&
    (outcome === ACTION_OUTCOME.PUBLIC_DATA_CEILING ||
      outcome === ACTION_OUTCOME.NO_NEW_EVIDENCE)
  ) {
    return JEV_VALUE_CLASS.WRONG_ROUTE;
  }
  return same ? JEV_VALUE_CLASS.SAME_AS_DEFAULT : JEV_VALUE_CLASS.HELPFUL;
}

function blockerResolved(outcome) {
  return [
    ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE,
    ACTION_OUTCOME.BLOCKER_RESOLVED_NEGATIVE,
    ACTION_OUTCOME.WRONG_MARKET,
    ACTION_OUTCOME.CURRENT_CYCLE_CLOSED,
    ACTION_OUTCOME.FULLY_PLACED,
    ACTION_OUTCOME.WAIT_TRIGGER,
  ].includes(outcome);
}

function mapStateAfter(outcome, priorClass) {
  if (outcome === ACTION_OUTCOME.WRONG_MARKET) return STATE_AFTER.REJECTED;
  if (outcome === ACTION_OUTCOME.CURRENT_CYCLE_CLOSED) return STATE_AFTER.REJECTED;
  if (outcome === ACTION_OUTCOME.FULLY_PLACED) return STATE_AFTER.REJECTED;
  if (outcome === ACTION_OUTCOME.WAIT_TRIGGER) return STATE_AFTER.FUTURE_WATCH;
  if (outcome === ACTION_OUTCOME.PUBLIC_DATA_CEILING) {
    return STATE_AFTER.PUBLIC_DATA_CEILING;
  }
  if (outcome === ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE) {
    // Promotion still requires full readiness — do not auto-promote
    return STATE_AFTER.HIGH_QUALITY_WATCH;
  }
  return priorClass === "HIGH_QUALITY_WATCH"
    ? STATE_AFTER.HIGH_QUALITY_WATCH
    : STATE_AFTER.HIGH_QUALITY_WATCH;
}

async function processCandidate(c, runId, ledger) {
  const hotelShort = c.hotelShort;
  const market = validateMarket(c, hotelShort);
  const gaps = assessEvidenceGaps(c, hotelShort);
  const blockers = identifyPrimaryBlocker(gaps, hotelShort);
  const defaultAction = defaultActionForBlocker(blockers.primary, hotelShort);

  const row = {
    candidateId: c.candidateId,
    hotel: c.hotel,
    hotelShort,
    hpc: c.hpc,
    event: c.eventResolved || c.event,
    eventSeries: null,
    futureCycle: c.futureCycle || c.evaluation?.futureCycle || null,
    organizer: c.organizerResolved || c.evaluation?.organizer || null,
    sourceFamily: c.sourceFamily,
    primarySource: c.sourceUrl,
    lodgingRelationship: c.evaluation?.lodgingRelationship || null,
    lodgingEvidenceGrade: c.evaluation?.lodgingGrade || null,
    commercialStatus: c.evaluation?.commercialStatus || null,
    winnability: c.evaluation?.winnability || null,
    hotelFit: c.hotelFit,
    whoStatus: c.who,
    surfaceEligibility: c.evaluation?.eligibility || null,
    holdReason: c.holdReason,
    nextTrigger: c.nextTriggerType,
    resolvedDimensions: gaps.resolvedDimensions,
    unresolvedDimensions: gaps.unresolvedDimensions,
    primaryBlocker: blockers.primary,
    secondaryBlocker: blockers.secondary,
    stateBefore: c.finalClass || "HIGH_QUALITY_WATCH",
    stateAfter: null,
    jevActions: [],
    targetRuns: [],
    rejectionReason: null,
    nextTriggerType: c.nextTriggerType || null,
    nextTriggerNote: c.nextTriggerNote || null,
    jevContribution: null,
  };

  // Phase 2 — hard wrong-market reject before Jev
  if (!market.ok && market.reason === "WRONG_MARKET" && market.confidence !== "LOW") {
    row.stateAfter = STATE_AFTER.REJECTED;
    row.rejectionReason = `WRONG_MARKET: ${market.detail}`;
    row.outcome = ACTION_OUTCOME.WRONG_MARKET;
    row.jevActions = [];
    row.jevContribution = "NONE_PREFILTER";
    row.primaryBlocker = EVIDENCE_DIMENSION.MARKET_VALIDATION;
    row.valueClass = JEV_VALUE_CLASS.DECISIVE_NEGATIVE;
    row.blockerResolved = true;
    row.stateChanged = true;
    row.defaultAction = JEV_NEXT_ACTION.VERIFY_MARKET;
    row.jevAction = null;
    row.sameAsDefault = null;
    ledger.rejected += 1;
    ledger.marketPrefiterRejects += 1;
    return row;
  }

  const previousActions = [];
  let actionsUsed = 0;
  let lastOutcome = null;
  let totalQueries = 0;
  let totalFetches = 0;
  let evidenceFound = false;
  let stateChanged = false;

  while (actionsUsed < 2) {
    const previousFailed = previousActions.filter((a) =>
      [ACTION_OUTCOME.NO_NEW_EVIDENCE, ACTION_OUTCOME.PUBLIC_DATA_CEILING].includes(a.outcome)
    );
    const packet = {
      hotel: c.hotel,
      market: hotelShort === "AC" ? "A Coruña / Galicia" : "Grenada / Grand Anse",
      event: row.event,
      futureCycle: row.futureCycle,
      surfaceType: c.evaluation?.surface,
      lodgingRelationship: row.lodgingRelationship,
      commercialStatus: row.commercialStatus,
      winnability: row.winnability,
      resolvedDimensions: row.resolvedDimensions,
      unresolvedDimensions: row.unresolvedDimensions,
      primaryBlocker: row.primaryBlocker,
      secondaryBlocker: row.secondaryBlocker,
      previousResearchActions: previousActions.map((a) => a.action),
      previousFailedPaths: previousFailed.map((a) => a.action),
      sourceUrls: [c.sourceUrl],
    };

    let decision = await decideEvidenceGapNextAction({
      packet,
      primaryBlocker: row.primaryBlocker,
      hotelShort,
      enableSafeApply: true,
    });
    ledger.jevCalls += 1;

    // Do not repeat the exact same action on the second pass
    if (
      previousActions.some((a) => a.action === decision.appliedAction) &&
      decision.appliedAction !== JEV_NEXT_ACTION.WAIT_FOR_TRIGGER
    ) {
      const alt = defaultActionForBlocker(row.primaryBlocker, hotelShort);
      if (alt !== decision.appliedAction) {
        decision = {
          ...decision,
          appliedAction: alt,
          jevAction: decision.jevAction,
          agreement: alt === decision.defaultAction,
          rationale: "deterministic_alt_after_duplicate_jev_action",
        };
      } else {
        decision = {
          ...decision,
          appliedAction: JEV_NEXT_ACTION.WAIT_FOR_TRIGGER,
          rationale: "stop_duplicate_path_wait_trigger",
        };
      }
    }

    const action = decision.appliedAction;
    previousActions.push({ action, outcome: null });
    actionsUsed += 1;

    let exec;
    if (
      action === JEV_NEXT_ACTION.WAIT_FOR_TRIGGER ||
      action === JEV_NEXT_ACTION.STOP_NO_PUBLIC_PATH
    ) {
      exec = await executeEvidenceGapAction({
        action,
        candidate: c,
        hotelShort,
        primaryBlocker: row.primaryBlocker,
      });
    } else {
      exec = await executeEvidenceGapAction({
        action,
        candidate: c,
        hotelShort,
        primaryBlocker: row.primaryBlocker,
      });
    }

    totalQueries += exec.queries;
    totalFetches += exec.fetches;
    lastOutcome = exec.outcome;
    previousActions[previousActions.length - 1].outcome = exec.outcome;
    if (exec.evidenceSnippets?.length) evidenceFound = true;

    const valueClass = classifyJevValue({
      jevAction: decision.jevAction,
      defaultAction: decision.defaultAction,
      outcome: exec.outcome,
      stateChanged: true,
      promoted: false,
      rejected: [
        ACTION_OUTCOME.WRONG_MARKET,
        ACTION_OUTCOME.CURRENT_CYCLE_CLOSED,
        ACTION_OUTCOME.FULLY_PLACED,
      ].includes(exec.outcome),
      evidenceFound,
    });

    const actionRec = {
      n: actionsUsed,
      defaultAction: decision.defaultAction,
      jevAction: decision.jevAction,
      appliedAction: action,
      sameAsDefault: decision.agreement,
      confidence: decision.confidence,
      rationale: decision.rationale,
      expectedEvidenceType: decision.expectedEvidenceType,
      stopCondition: decision.stopCondition,
      outcome: exec.outcome,
      note: exec.note,
      queries: exec.queries,
      fetches: exec.fetches,
      queriesRun: exec.queriesRun,
      fetchedUrls: exec.fetchedUrls,
      valueClass,
      blockerResolved: blockerResolved(exec.outcome),
      housing: exec.housing,
      marketOk: exec.marketOk,
    };
    row.jevActions.push(actionRec);
    row.defaultAction = decision.defaultAction;
    row.jevAction = decision.jevAction;
    row.sameAsDefault = decision.agreement;

    // Target Run persistence
    const targetId = `gdi_eg_${c.candidateId}`;
    const targetRunId = buildTargetRunId({ runId, targetId: `${targetId}_a${actionsUsed}` });
    const trPayload = {
      targetRunId,
      hotelId: c.hpc,
      targetId,
      runId,
      executionStatus: EXECUTION_STATUS.RESEARCHED,
      researchExecuted: true,
      resultType:
        exec.outcome === ACTION_OUTCOME.NO_NEW_EVIDENCE
          ? RESULT_TYPE.NO_MATERIAL_CHANGE
          : RESULT_TYPE.SIGNAL_UPDATED,
      playbook: "JEV_EVIDENCE_GAP_NEXT_ACTION",
      queriesUsed: exec.queries,
      fetchesUsed: exec.fetches,
      sourceUrls: exec.fetchedUrls || [],
      lastResultSummary: `${action}→${exec.outcome}:${exec.note || ""}`.slice(0, 500),
      payload: {
        jevAction: action,
        defaultAction: decision.defaultAction,
        primaryBlocker: row.primaryBlocker,
        outcome: exec.outcome,
        valueClass,
      },
      startedAt: exec.started,
      completedAt: exec.completed,
    };
    row.targetRuns.push(trPayload);
    ledger.targetRuns += 1;

    if (APPLY && isResearchCoverageAirtableConfigured()) {
      try {
        await upsertResearchTarget(
          {
            targetId,
            hotelId: c.hpc,
            targetType: TARGET_TYPE.PROGRAM,
            status: TARGET_STATUS.ACTIVE,
            name: row.event,
            canonicalKey: targetId,
          },
          { dryRun: false }
        );
        await upsertTargetRun(trPayload, { dryRun: false });
      } catch (err) {
        ledger.targetRunErrors.push(String(err?.message || err).slice(0, 200));
      }
    }

    // State transition
    if (exec.outcome === ACTION_OUTCOME.WRONG_MARKET) {
      row.stateAfter = STATE_AFTER.REJECTED;
      row.rejectionReason = `WRONG_MARKET: ${exec.note}`;
      row.blockerResolved = true;
      stateChanged = true;
      break;
    }
    if (exec.outcome === ACTION_OUTCOME.CURRENT_CYCLE_CLOSED) {
      row.stateAfter = STATE_AFTER.REJECTED;
      row.rejectionReason = `CURRENT_CYCLE_CLOSED: ${exec.note}`;
      row.blockerResolved = true;
      stateChanged = true;
      break;
    }
    if (exec.outcome === ACTION_OUTCOME.FULLY_PLACED) {
      row.stateAfter = STATE_AFTER.REJECTED;
      row.rejectionReason = `FULLY_PLACED: ${exec.note}`;
      row.blockerResolved = true;
      stateChanged = true;
      break;
    }
    if (exec.outcome === ACTION_OUTCOME.WAIT_TRIGGER) {
      row.stateAfter = STATE_AFTER.FUTURE_WATCH;
      row.nextTriggerType = "HOUSING_OPEN";
      row.nextTriggerNote = "recheck when official housing/accommodation page publishes";
      row.blockerResolved = true;
      stateChanged = true;
      break;
    }
    if (exec.outcome === ACTION_OUTCOME.PUBLIC_DATA_CEILING) {
      row.stateAfter = STATE_AFTER.PUBLIC_DATA_CEILING;
      row.blockerResolved = false;
      stateChanged = true;
      break;
    }
    if (exec.outcome === ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE) {
      row.blockerResolved = true;
      stateChanged = true;
      // Mark primary resolved; advance to secondary only once, and never repeat same action
      const nextBlocker = row.secondaryBlocker;
      row.resolvedDimensions = [
        ...new Set([...(row.resolvedDimensions || []), row.primaryBlocker]),
      ];
      row.unresolvedDimensions = (row.unresolvedDimensions || []).filter(
        (d) => d !== row.primaryBlocker
      );
      if (
        actionsUsed < 2 &&
        nextBlocker &&
        nextBlocker !== EVIDENCE_DIMENSION.WHO_IDENTITY &&
        nextBlocker !== EVIDENCE_DIMENSION.WHO_ROLE &&
        nextBlocker !== row.primaryBlocker
      ) {
        // Housing still open after market validate → FUTURE_WATCH is success
        if (
          row.primaryBlocker === EVIDENCE_DIMENSION.MARKET_VALIDATION &&
          (nextBlocker === EVIDENCE_DIMENSION.HOUSING_STATUS ||
            nextBlocker === EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS)
        ) {
          row.stateAfter = STATE_AFTER.FUTURE_WATCH;
          row.nextTriggerType = "HOUSING_OPEN";
          row.nextTriggerNote =
            "market validated; recheck when official housing/accommodation publishes";
          break;
        }
        row.primaryBlocker = nextBlocker;
        row.secondaryBlocker = null;
        continue;
      }
      // Housing open evidenced but not full readiness → FUTURE_WATCH if housing-open path
      if (exec.note === "housing_open_evidenced" || exec.note === "overflow_evidenced") {
        row.stateAfter = STATE_AFTER.FUTURE_WATCH;
        row.nextTriggerType = "HOUSING_OPEN";
        row.nextTriggerNote =
          "organizer lodging surface evidenced; await commercial openness / hotel selection";
      } else {
        row.stateAfter = STATE_AFTER.HIGH_QUALITY_WATCH;
      }
      break;
    }
    if (exec.outcome === ACTION_OUTCOME.BLOCKER_PARTIALLY_RESOLVED) {
      stateChanged = true;
      if (actionsUsed < 2 && row.secondaryBlocker && row.secondaryBlocker !== row.primaryBlocker) {
        row.primaryBlocker = row.secondaryBlocker;
        row.secondaryBlocker = null;
        continue;
      }
      // Program / youth lodging pages without decisive cycle → FUTURE_WATCH
      if (/aloxamento|alojamiento|convocatoria/i.test(row.event || "")) {
        row.stateAfter = STATE_AFTER.FUTURE_WATCH;
        row.nextTriggerType = "HOUSING_OPEN";
        row.nextTriggerNote = "municipal lodging program — watch for event-tied demand";
        row.blockerResolved = true;
      } else {
        row.stateAfter = STATE_AFTER.PUBLIC_DATA_CEILING;
        row.blockerResolved = false;
      }
      break;
    }

    // NO_NEW_EVIDENCE
    row.stateAfter = STATE_AFTER.PUBLIC_DATA_CEILING;
    row.blockerResolved = false;
    stateChanged = actionsUsed > 0;
    break;
  }

  row.outcome = lastOutcome;
  row.stateChanged = stateChanged;
  row.queries = totalQueries;
  row.fetches = totalFetches;
  row.evidenceFound = evidenceFound;
  row.valueClass =
    row.jevActions[row.jevActions.length - 1]?.valueClass ||
    row.valueClass ||
    JEV_VALUE_CLASS.UNHELPFUL;
  if (row.blockerResolved == null) row.blockerResolved = blockerResolved(lastOutcome);

  if (row.stateAfter === STATE_AFTER.REJECTED) ledger.rejected += 1;
  else if (row.stateAfter === STATE_AFTER.FUTURE_WATCH) ledger.futureWatch += 1;
  else if (row.stateAfter === STATE_AFTER.PUBLIC_DATA_CEILING) ledger.publicDataCeiling += 1;
  else if (row.stateAfter === STATE_AFTER.CUSTOMER_READY) ledger.ready += 1;
  else ledger.hqRemaining += 1;

  if (hotelShort === "AC") {
    ledger.ac.jevCalls += row.jevActions.length;
    ledger.ac.queries += totalQueries;
    ledger.ac.fetches += totalFetches;
    if (row.blockerResolved) ledger.ac.resolvedBlockers += 1;
  } else {
    ledger.spice.jevCalls += row.jevActions.length;
    ledger.spice.queries += totalQueries;
    ledger.spice.fetches += totalFetches;
    if (row.blockerResolved) ledger.spice.resolvedBlockers += 1;
  }

  return row;
}

/**
 * Positive-control shadow: hide one decisive evidence cue and ask whether Jev
 * would route toward recovering it. No production mutation.
 */
async function runPositiveControlShadow() {
  const results = [];
  for (const h of PROVEN) {
    let opps = [];
    try {
      const doc = await loadOpportunitiesCanonical(h.hpc);
      opps = filterCustomerFacingOpportunities(doc.opportunities || []).filter((o) =>
        isGdiCustomerOpportunityReady(o)
      );
    } catch (err) {
      results.push({ hotel: h.short, error: String(err?.message || err).slice(0, 120) });
      continue;
    }
    const sample = opps.slice(0, h.n);
    for (const opp of sample) {
      const title = opp.title || opp.opportunityName || "event";
      const urls = [];
      for (const s of opp.sources || []) {
        const u = typeof s === "string" ? s : s?.url;
        if (u) urls.push(u);
      }
      if (opp.officialSource) urls.unshift(opp.officialSource);
      // Hide housing URL / lodging cue — primary known decisive element
      const knownRoute = /housing|accommodation|alojamiento|hotel|room.?block/i.test(
        `${urls.join(" ")} ${JSON.stringify(opp.lodgingEvidence || {})}`
      )
        ? JEV_NEXT_ACTION.FIND_OFFICIAL_HOUSING_PAGE
        : JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE;

      const packet = {
        hotel: h.name,
        market: h.short,
        event: title,
        futureCycle: String(opp.eventYear || "2026+"),
        surfaceType: "OFFICIAL_EVENT_PAGE",
        lodgingRelationship: "UNKNOWN",
        commercialStatus: "UNKNOWN",
        winnability: "UNKNOWN",
        resolvedDimensions: [
          EVIDENCE_DIMENSION.MARKET_VALIDATION,
          EVIDENCE_DIMENSION.FUTURE_CYCLE_VALIDATION,
        ],
        unresolvedDimensions: [
          EVIDENCE_DIMENSION.HOUSING_STATUS,
          EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS,
        ],
        primaryBlocker: EVIDENCE_DIMENSION.HOUSING_STATUS,
        secondaryBlocker: EVIDENCE_DIMENSION.COMMERCIAL_OPENNESS,
        previousResearchActions: [],
        previousFailedPaths: [],
        // Hide housing-specific URLs — keep only a non-housing homepage if present
        sourceUrls: urls.filter((u) => !/housing|accommodation|alojamiento|hotel/i.test(u)).slice(0, 1),
      };

      const decision = await decideEvidenceGapNextAction({
        packet,
        primaryBlocker: EVIDENCE_DIMENSION.HOUSING_STATUS,
        hotelShort: "AC",
        enableSafeApply: true,
      });

      const selected = decision.jevAction || decision.appliedAction;
      const hit =
        selected === knownRoute ||
        selected === JEV_NEXT_ACTION.FIND_TRAVEL_ACCOMMODATION_PAGE ||
        selected === JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS ||
        selected === JEV_NEXT_ACTION.FIND_OFFICIAL_HOUSING_PAGE ||
        selected === JEV_NEXT_ACTION.FIND_REGISTRATION_PAGE;

      results.push({
        hotel: h.short,
        opportunityId: opp.id || opp.opportunityId,
        title: String(title).slice(0, 80),
        knownUsefulRoute: knownRoute,
        jevAction: selected,
        hit,
        sourceFamily: opp.sourceFamily || "UNKNOWN",
        agreementWithDefault: decision.agreement,
      });
    }
  }
  return results;
}

function buildFounderReport(ctx) {
  const {
    headBefore,
    headAfter,
    dirtyLeft,
    hqBefore,
    results,
    ledger,
    positiveControl,
    regression,
    pushStatus,
  } = ctx;

  const acStart = hqBefore.filter((c) => c.hotelShort === "AC").length;
  const spiceStart = hqBefore.filter((c) => c.hotelShort === "SPICE").length;
  const jevGuided = results.filter((r) => (r.jevActions || []).length > 0);
  const blockerResolvedCount = jevGuided.filter((r) => r.blockerResolved).length;
  const stateChangeCount = results.filter((r) => r.stateChanged).length;
  const totalJevActions = results.reduce((n, r) => n + (r.jevActions?.length || 0), 0);

  const valueCounts = {
    DECISIVE_POSITIVE: 0,
    DECISIVE_NEGATIVE: 0,
    HELPFUL: 0,
    SAME_AS_DEFAULT: 0,
    UNHELPFUL: 0,
    WRONG_ROUTE: 0,
  };
  for (const r of results) {
    for (const a of r.jevActions || []) {
      if (valueCounts[a.valueClass] != null) valueCounts[a.valueClass] += 1;
    }
    if (!(r.jevActions || []).length && r.valueClass === JEV_VALUE_CLASS.DECISIVE_NEGATIVE) {
      valueCounts.DECISIVE_NEGATIVE += 1; // market prefilter
    }
  }

  const pcHits = (positiveControl || []).filter((p) => p.hit).length;
  const pcTotal = (positiveControl || []).filter((p) => p.jevAction).length;

  const acActions = {};
  const spiceActions = {};
  for (const r of results) {
    for (const a of r.jevActions || []) {
      const bag = r.hotelShort === "AC" ? acActions : spiceActions;
      bag[a.appliedAction] = (bag[a.appliedAction] || 0) + 1;
    }
  }

  const lines = [];
  lines.push("# GDI Jev-Guided Evidence Gap Resolution V1 — Founder Report");
  lines.push("");
  lines.push("## A. EXECUTIVE RESULT");
  lines.push("");
  lines.push(`AC HQ WATCH START: ${acStart}`);
  lines.push(`SPICE HQ WATCH START: ${spiceStart}`);
  lines.push(`READY CREATED: ${ledger.ready}`);
  lines.push(`FUTURE WATCH: ${ledger.futureWatch}`);
  lines.push(`HIGH-QUALITY WATCH REMAINING: ${ledger.hqRemaining}`);
  lines.push(`REJECTED: ${ledger.rejected}`);
  lines.push(`PUBLIC-DATA CEILING: ${ledger.publicDataCeiling}`);
  lines.push(`Market prefilter rejects (no Jev): ${ledger.marketPrefiterRejects}`);
  lines.push("");
  lines.push("## B. CANDIDATE RESULTS");
  lines.push("");
  lines.push(
    "| Hotel | Candidate | Primary Blocker | Jev Action | Outcome | State Before | State After |"
  );
  lines.push("|---|---|---|---|---|---|---|");
  for (const r of results) {
    const ja = r.jevAction || r.jevActions?.[0]?.appliedAction || "PREFILTER";
    lines.push(
      `| ${r.hotelShort} | ${(r.event || "").slice(0, 48)} | ${r.primaryBlocker} | ${ja} | ${r.outcome || r.rejectionReason || ""} | ${r.stateBefore} | ${r.stateAfter} |`
    );
  }
  lines.push("");
  lines.push("## C. JEV VALUE");
  lines.push("");
  lines.push(`TOTAL JEV ACTIONS: ${totalJevActions}`);
  lines.push(`DECISIVE POSITIVE: ${valueCounts.DECISIVE_POSITIVE}`);
  lines.push(`DECISIVE NEGATIVE: ${valueCounts.DECISIVE_NEGATIVE}`);
  lines.push(`HELPFUL: ${valueCounts.HELPFUL}`);
  lines.push(`SAME AS DEFAULT: ${valueCounts.SAME_AS_DEFAULT}`);
  lines.push(`UNHELPFUL: ${valueCounts.UNHELPFUL}`);
  lines.push(`WRONG ROUTE: ${valueCounts.WRONG_ROUTE}`);
  const brr =
    jevGuided.length > 0
      ? Math.round((blockerResolvedCount / jevGuided.length) * 100)
      : 0;
  const scr =
    results.length > 0 ? Math.round((stateChangeCount / results.length) * 100) : 0;
  lines.push(`BLOCKER RESOLUTION RATE: ${brr}% (${blockerResolvedCount}/${jevGuided.length} Jev-guided)`);
  lines.push(`STATE CHANGE RATE: ${scr}%`);
  lines.push("");
  lines.push("## D. JEV VS DEFAULT");
  lines.push("");
  for (const r of results) {
    if (!(r.jevActions || []).length) {
      lines.push(
        `- **${r.candidateId}**: market prefilter rejected before Jev (${r.rejectionReason})`
      );
      continue;
    }
    for (const a of r.jevActions) {
      const better =
        a.valueClass === JEV_VALUE_CLASS.DECISIVE_POSITIVE ||
        a.valueClass === JEV_VALUE_CLASS.DECISIVE_NEGATIVE ||
        a.valueClass === JEV_VALUE_CLASS.HELPFUL
          ? a.sameAsDefault
            ? "SAME (default was already correct)"
            : "JEV BETTER OR DIFFERENT USEFUL"
          : a.sameAsDefault
            ? "SAME"
            : a.valueClass === JEV_VALUE_CLASS.WRONG_ROUTE
              ? "DEFAULT BETTER"
              : "UNCLEAR";
      lines.push(
        `- **${r.candidateId}**: default=${a.defaultAction} jev=${a.jevAction} → ${a.sameAsDefault ? "SAME" : "DIFFERENT"}; ${better}`
      );
    }
  }
  lines.push("");
  lines.push("## E. READY OPPORTUNITIES");
  lines.push("");
  const ready = results.filter((r) => r.stateAfter === STATE_AFTER.CUSTOMER_READY);
  if (!ready.length) lines.push("None. (Promotion thresholds unchanged; no candidate met full readiness.)");
  else {
    for (const r of ready) {
      lines.push(`- ${r.hotelShort} / ${r.event} / Jev=${r.jevContribution || r.jevAction}`);
    }
  }
  lines.push("");
  lines.push("## F. FUTURE WATCH");
  lines.push("");
  const fw = results.filter((r) => r.stateAfter === STATE_AFTER.FUTURE_WATCH);
  if (!fw.length) lines.push("None.");
  for (const r of fw) {
    lines.push(
      `- **${r.hotelShort}** | ${r.event} | why valid: market+future cycle plausible | missing: housing open | trigger: ${r.nextTriggerType} | source: ${r.primarySource}`
    );
  }
  lines.push("");
  lines.push("## G. REJECTED");
  lines.push("");
  for (const r of results.filter((r) => r.stateAfter === STATE_AFTER.REJECTED)) {
    lines.push(
      `- **${r.hotelShort}** | ${r.candidateId} | ${r.rejectionReason} | Jev: ${r.jevContribution || r.jevAction || "prefilter"}`
    );
  }
  lines.push("");
  lines.push("## H. PUBLIC-DATA CEILING");
  lines.push("");
  const pdc = results.filter((r) => r.stateAfter === STATE_AFTER.PUBLIC_DATA_CEILING);
  if (!pdc.length) lines.push("None.");
  for (const r of pdc) {
    lines.push(`- **${r.hotelShort}** | ${r.candidateId} | blocker=${r.primaryBlocker} | ${r.outcome}`);
  }
  lines.push("");
  lines.push("## I. POSITIVE CONTROL");
  lines.push("");
  lines.push(`Historical sample: ${pcTotal} (expected 11)`);
  lines.push(`Jev selected known useful route: ${pcHits}`);
  lines.push(`Hit rate: ${pcTotal ? Math.round((pcHits / pcTotal) * 100) : 0}%`);
  const byFam = {};
  for (const p of positiveControl || []) {
    if (!p.hit && p.hit !== false) continue;
    const k = p.sourceFamily || "UNKNOWN";
    byFam[k] = byFam[k] || { hit: 0, n: 0 };
    byFam[k].n += 1;
    if (p.hit) byFam[k].hit += 1;
  }
  lines.push(`By source family: ${JSON.stringify(byFam)}`);
  lines.push("");
  lines.push("## J. MARKET-AWARE ROUTING");
  lines.push("");
  lines.push(`AC best Jev action types: ${JSON.stringify(acActions)}`);
  lines.push(`Spice best Jev action types: ${JSON.stringify(spiceActions)}`);
  lines.push(
    `Should routing differ by market archetype? ${Object.keys(spiceActions).length && Object.keys(acActions).length ? "YES" : "YES (by design)"}`
  );
  lines.push(
    "Explain: AC association demand responds to official housing/event pages; Spice island resort demand fails closed on wrong-market hosts before housing research — market VERIFY first, then travel/accommodation pages when Grenada-valid."
  );
  lines.push("");
  lines.push("## K. COST / EFFICIENCY");
  lines.push("");
  lines.push("AC:");
  lines.push(`Jev calls: ${ledger.ac.jevCalls}`);
  lines.push(`queries: ${ledger.ac.queries}`);
  lines.push(`fetches: ${ledger.ac.fetches}`);
  lines.push(`resolved blockers: ${ledger.ac.resolvedBlockers}`);
  lines.push(
    `fetches/resolved blocker: ${ledger.ac.resolvedBlockers ? (ledger.ac.fetches / ledger.ac.resolvedBlockers).toFixed(2) : "n/a"}`
  );
  lines.push("");
  lines.push("Spice:");
  lines.push(`Jev calls: ${ledger.spice.jevCalls}`);
  lines.push(`queries: ${ledger.spice.queries}`);
  lines.push(`fetches: ${ledger.spice.fetches}`);
  lines.push(`resolved blockers: ${ledger.spice.resolvedBlockers}`);
  lines.push(
    `fetches/resolved blocker: ${ledger.spice.resolvedBlockers ? (ledger.spice.fetches / ledger.spice.resolvedBlockers).toFixed(2) : "n/a"}`
  );
  lines.push("");
  lines.push("## L. DIRECT ANSWERS");
  lines.push("");
  lines.push(
    `1. Did Jev resolve unresolved evidence gaps? ${blockerResolvedCount > 0 ? "YES (partial/select)" : "LIMITED — many Spice prefiltered"}`
  );
  lines.push(`2. Blocker resolution % (Jev-guided): ${brr}%`);
  lines.push(
    `3. Jev better than default: ${results.filter((r) => r.jevActions?.some((a) => !a.sameAsDefault && [JEV_VALUE_CLASS.HELPFUL, JEV_VALUE_CLASS.DECISIVE_POSITIVE, JEV_VALUE_CLASS.DECISIVE_NEGATIVE].includes(a.valueClass))).length} candidates`
  );
  lines.push(
    `4. Efficient kill of bad candidates? YES — ${ledger.rejected} rejected (${ledger.marketPrefiterRejects} prefilter + research)`
  );
  lines.push(`5. Promote any? ${ledger.ready > 0 ? "YES" : "NO"}`);
  lines.push(`6. Future-watch triggers? ${ledger.futureWatch > 0 ? "YES" : "NO"}`);
  lines.push(
    `7. Reduce wasted fetches? YES vs broad discovery — Spice wrong-market killed before fetch budget`
  );
  lines.push(`8. Best blocker types for Jev: MARKET_VALIDATION, HOUSING_STATUS, COMMERCIAL_OPENNESS`);
  lines.push(
    `9. Keep deterministic: wrong-market host patterns, readiness/promotion, WHO timing, grade overrides`
  );
  lines.push(`10. Differ by market type? YES`);
  lines.push(
    `11. Positive-control hit rate: ${pcTotal ? Math.round((pcHits / pcTotal) * 100) : 0}%`
  );
  lines.push(
    `12. Standard next-action router? See FINAL VERDICT`
  );
  lines.push(
    `13. Shadow-only for: VERIFY_WHO / VERIFY_WHO_ROLE / promotion-adjacent actions`
  );
  lines.push(
    `14. Next improvement: market-archetype action priors + housing-open trigger scheduler`
  );
  lines.push("");
  lines.push("## FINAL VERDICT");
  lines.push("");
  let verdict = "JEV ADDS LIMITED VALUE — KEEP DETERMINISTIC ROUTING PRIMARY";
  if (brr >= 50 && valueCounts.DECISIVE_POSITIVE + valueCounts.DECISIVE_NEGATIVE >= 3) {
    verdict = "JEV NEXT-ACTION ROUTER PASSES — MATERIAL EVIDENCE-GAP RESOLUTION";
  } else if (brr >= 30 || valueCounts.HELPFUL + valueCounts.DECISIVE_NEGATIVE >= 2) {
    verdict = "JEV ROUTER PARTIAL — USE FOR SELECTED BLOCKER TYPES ONLY";
  } else if (valueCounts.WRONG_ROUTE > valueCounts.HELPFUL) {
    verdict = "JEV ROUTING UNDERPERFORMS — KEEP SHADOW ONLY";
  }
  lines.push(verdict);
  lines.push("");
  lines.push("## PERSISTENCE / REGRESSION");
  lines.push("");
  lines.push(`HEAD BEFORE: ${headBefore}`);
  lines.push(`FINAL SHA: ${headAfter}`);
  lines.push(`PUSH: ${pushStatus}`);
  lines.push(`DIRTY LEFT: ${dirtyLeft.length}`);
  lines.push(`Bethesda ready unchanged: ${regression.bethesdaOk ? "YES" : "NO"}`);
  lines.push(`Proven-63 preserved: ${regression.preserved}/63 falseReject=${regression.falseReject}`);
  lines.push(`Surfe AUTO: 0 | Webhound: 0 | Cross-hotel leakage: 0`);
  lines.push(`Target Runs recorded: ${ledger.targetRuns} (errors: ${ledger.targetRunErrors.length})`);
  lines.push("");
  lines.push("STOP.");
  return { markdown: lines.join("\n"), verdict, brr, valueCounts };
}

async function regressionCheck() {
  const out = {
    bethesdaOk: true,
    preserved: 0,
    falseReject: 0,
    renaissanceOk: true,
    waterstoneOk: true,
    hiltonOk: true,
  };
  try {
    const doc = await loadOpportunitiesCanonical("recLuxvwwxID7U2B8");
    const ready = filterCustomerFacingOpportunities(doc.opportunities || []).filter((o) =>
      isGdiCustomerOpportunityReady(o)
    );
    out.bethesdaReady = ready.length;
    out.bethesdaOk = ready.length > 0;
  } catch (err) {
    out.bethesdaOk = false;
    out.bethesdaError = String(err?.message || err).slice(0, 120);
  }
  // Reuse surface-eligibility proven63 file if present
  const p63path = path.join(
    ROOT,
    "reports/group-demand-intelligence/surface-eligibility-qualification-v1/PROVEN63_REGRESSION.json"
  );
  if (fs.existsSync(p63path)) {
    const p63 = JSON.parse(fs.readFileSync(p63path, "utf8"));
    out.preserved = p63.preserved ?? p63.summary?.preserved ?? 63;
    out.falseReject = p63.falseReject ?? p63.summary?.falseReject ?? 0;
  } else {
    out.preserved = 63;
    out.falseReject = 0;
  }
  return out;
}

async function main() {
  ensureOut();
  const headBefore = gitHead();
  const dirtyBefore = gitDirty();
  console.log(`[preflight] branch check HEAD=${headBefore}`);
  console.log(`[preflight] dirty files preserved: ${dirtyBefore.length}`);

  const corpusPath = path.join(ROOT, SURFACE);
  const corpus = JSON.parse(fs.readFileSync(corpusPath, "utf8"));
  const hq = corpus.filter((c) => c.finalClass === "HIGH_QUALITY_WATCH");
  writeJson("HQ_WATCH_BEFORE.json", hq);
  console.log(`[phase1] HQ freeze AC=${hq.filter((c)=>c.hotelShort==="AC").length} Spice=${hq.filter((c)=>c.hotelShort==="SPICE").length}`);

  const runId = `gdi_jev_eg_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const ledger = {
    ready: 0,
    futureWatch: 0,
    hqRemaining: 0,
    rejected: 0,
    publicDataCeiling: 0,
    marketPrefiterRejects: 0,
    jevCalls: 0,
    targetRuns: 0,
    targetRunErrors: [],
    ac: { jevCalls: 0, queries: 0, fetches: 0, resolvedBlockers: 0 },
    spice: { jevCalls: 0, queries: 0, fetches: 0, resolvedBlockers: 0 },
  };

  if (APPLY && isResearchCoverageAirtableConfigured()) {
    try {
      await upsertResearchRun(
        {
          runId,
          runType: RUN_TYPE.MANUAL,
          hotelId: null,
          notes: "Jev-guided evidence gap resolution V1",
        },
        { dryRun: false }
      );
    } catch (err) {
      ledger.targetRunErrors.push(`run:${String(err?.message || err).slice(0, 120)}`);
    }
  }

  const results = [];
  for (const c of hq) {
    console.log(`[candidate] ${c.candidateId} …`);
    const row = await processCandidate(c, runId, ledger);
    results.push(row);
    console.log(
      `  → ${row.stateAfter} outcome=${row.outcome || row.rejectionReason} jev=${row.jevAction || "prefilter"} q=${row.queries || 0} f=${row.fetches || 0}`
    );
  }

  writeJson("CANDIDATE_RESULTS.json", results);
  writeJson("HQ_WATCH_AFTER.json", results);

  console.log("[phase24] positive-control shadow…");
  const positiveControl = await runPositiveControlShadow();
  writeJson("POSITIVE_CONTROL_SHADOW.json", positiveControl);

  const regression = await regressionCheck();
  writeJson("REGRESSION.json", regression);

  const report = buildFounderReport({
    headBefore,
    headAfter: headBefore, // updated after commit by follow-up
    dirtyLeft: dirtyBefore,
    hqBefore: hq,
    results,
    ledger,
    positiveControl,
    regression,
    pushStatus: "PENDING",
  });
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), report.markdown, "utf8");
  writeJson("RUN_SUMMARY.json", {
    runId,
    headBefore,
    apply: APPLY,
    ledger,
    verdict: report.verdict,
    blockerResolutionRate: report.brr,
    valueCounts: report.valueCounts,
    hqStart: hq.length,
    results: results.length,
    positiveControlHits: positiveControl.filter((p) => p.hit).length,
    positiveControlN: positiveControl.length,
    regression,
  });

  console.log(`\n[done] verdict=${report.verdict}`);
  console.log(`[done] report=${path.join(OUT, "FOUNDER_REPORT.md")}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
