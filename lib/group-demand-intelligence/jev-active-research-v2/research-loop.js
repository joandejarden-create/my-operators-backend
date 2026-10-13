/**
 * Page-level adaptive research loop for active Jev controller.
 */

import {
  fetchCandidatePage,
  extractEvidenceFromPage,
  runTargetedCompletionSearch,
  stampWhoCeilingIfNeeded,
} from "../candidate-completion-v2/page-research.js";
import {
  buildEvidenceGapMap,
  classifyLodgingCompletion,
  GAP_STATE,
} from "../candidate-completion-v2/evidence-gap.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import { classifyEntityTruth } from "../entity-truth-gate-v1.js";
import { reviewJevActiveStrategy, reviewJevActiveLoop, JEV_LOOP } from "./jev-active-advisor.js";
import { gateJevResearchAdvice, gateJevLoopAction, POLICY_VERDICT } from "./policy-gate.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";

export const TERMINAL = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  VALID_FUTURE_WATCH: "VALID_FUTURE_WATCH",
  REJECTED_CONFIRMED: "REJECTED_CONFIRMED",
  UNRESOLVED_PUBLIC_DATA_CEILING: "UNRESOLVED_PUBLIC_DATA_CEILING",
});

const TIMING = Object.freeze({
  CONFIRMED_EXACT: "CONFIRMED_EXACT",
  CONFIRMED_RANGE: "CONFIRMED_RANGE",
  CONFIRMED_MONTH: "CONFIRMED_MONTH",
  CONFIRMED_YEAR: "CONFIRMED_YEAR",
  RECURRING_EXPECTED: "RECURRING_EXPECTED",
  ROTATION_PREDICTED: "ROTATION_PREDICTED",
  FUTURE_UNCONFIRMED: "FUTURE_UNCONFIRMED",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
});

function classifyTiming(opp = {}) {
  const start = String(opp.eventStartDate || "");
  if (/^\d{4}-\d{2}-\d{2}$/.test(start)) return TIMING.CONFIRMED_EXACT;
  if (/^\d{4}-\d{2}$/.test(start)) return TIMING.CONFIRMED_MONTH;
  if (opp.eventYear) return TIMING.CONFIRMED_YEAR;
  if (/RECURRING|ANNUAL/i.test(String(opp.futureCycleEvidenceState || ""))) {
    return TIMING.RECURRING_EXPECTED;
  }
  if (opp.rotates || /ROTATION/i.test(String(opp.timingState || ""))) {
    return TIMING.ROTATION_PREDICTED;
  }
  return TIMING.FUTURE_UNCONFIRMED;
}

function itemToOpp(item) {
  return {
    id: item.opportunityId || item.researchId,
    title: item.title,
    organizationName: item.organization,
    opportunityName: item.eventProgram || item.title,
    officialSource: item.source,
    discoverySource: item.source,
    sources: item.source ? [{ url: item.source, kind: "jev_active_research_v2" }] : [],
    summaryWhat: String(item.title || "").slice(0, 280),
    hotelOpportunityThesis: `${item.hotelLabel}: research ${item.signalType} — confirm lodging/timing/contact before promotion.`,
    whyNow: item.nextResearchTrigger || "Active research tranche — confirm public evidence.",
    recommendedAction: "Validate official evidence; do not sell until gates pass.",
    summaryWhyMatters: "Signal/series pending page-level validation.",
    summaryWhyHotel: `${item.hotelLabel}: fit pending validation.`,
    hotelFitScore: item.defaultFitScore ?? 48,
    venueStatus: "Unknown",
    eventLocationSummary: item.placeNames?.[0] || null,
    destinationStatus: item.placeNames?.[0] || null,
    eventStartDate: item.existingEvidence?.nextKnownCycle || null,
    eventYear: null,
    eventSeriesId: item.existingEvidence?.seriesId || null,
    futureCycleEvidenceState: item.timingState,
    lodgingEvidence:
      item.lodgingState === "HINT"
        ? { housingPageFound: false, roomBlockMentioned: false, status: "WEAK" }
        : null,
    queryLanguage: item.language,
    discoveryMeta: {
      signalType: item.signalType,
      demandEngine: item.demandEngine,
      feederMarket: item.feederMarket,
      jarV2: true,
    },
    hotelId: item.hotelId,
  };
}

function terminalClassify(opp, opts = {}) {
  const ready = isGdiCustomerOpportunityReady(opp, opts);
  if (ready.ok) return { class: TERMINAL.CUSTOMER_READY, ready, watch: null };
  const watch = isValidFutureWatch(opp, { ...opts, ignoreTerminalPriority: true });
  const watchOk = watch?.ok === true || watch?.valid === true || watch?.class === "VALID_FUTURE_WATCH";
  if (watchOk) return { class: TERMINAL.VALID_FUTURE_WATCH, ready, watch };
  if (
    opp.contactResearchState === "PUBLIC_DATA_CEILING" ||
    opp.publicDataCeiling === true ||
    opp.whoPathClass === "NO_CONTACT_AFTER_RESEARCH"
  ) {
    return { class: TERMINAL.UNRESOLVED_PUBLIC_DATA_CEILING, ready, watch };
  }
  const entity = classifyEntityTruth(opp);
  if (!entity.validEntity) {
    return { class: TERMINAL.REJECTED_CONFIRMED, ready, watch, reason: "ENTITY_INVALID" };
  }
  return {
    class: TERMINAL.REJECTED_CONFIRMED,
    ready,
    watch,
    reason: opts.depthExhausted ? "DEPTH_EXHAUSTED_NOT_QUALIFIED" : "FAILED_GATES",
  };
}

function countResolved(before, after) {
  let n = 0;
  for (const k of Object.keys(before.gaps || {})) {
    const b = before.gaps[k];
    const a = after.gaps[k];
    if ((b === GAP_STATE.FAIL || b === GAP_STATE.UNKNOWN) && (a === GAP_STATE.PASS || a === GAP_STATE.PARTIAL)) {
      n += 1;
    }
  }
  return n;
}

function followUrls(opp, advice) {
  const base = opp.officialSource || "";
  try {
    const origin = new URL(base).origin;
    if (/lodging|housing|accommodation/i.test(advice.jevSourceFamily || "")) {
      return [`${origin}/accommodation`, `${origin}/housing`, `${origin}/hotels`];
    }
    if (/contact|who|secretariat/i.test(advice.jevSourceFamily || "")) {
      return [`${origin}/contact`, `${origin}/en/contact`];
    }
  } catch {
    /* ignore */
  }
  return base ? [base] : [];
}

async function enrichFromCanonical(item) {
  if (!item.opportunityId || !item.hotelId) return null;
  try {
    const doc = await loadOpportunitiesCanonical(item.hotelId);
    return (doc.opportunities || []).find((o) => o.id === item.opportunityId) || null;
  } catch {
    return null;
  }
}

/**
 * Research one selected item end-to-end.
 */
export async function researchOneItem(item, opts = {}) {
  const nowDate = opts.nowDate || "2026-10-03";
  const budget = opts.budget || { leftUsd: 5, costUsd: 0 };

  const adviceRaw = await reviewJevActiveStrategy(item);
  budget.costUsd += adviceRaw.costUsd || 0;
  budget.leftUsd -= adviceRaw.costUsd || 0;

  const gated = gateJevResearchAdvice(item, adviceRaw, {
    hardCapSteps: 3,
    budgetLeftUsd: budget.leftUsd,
  });

  const policyRow = {
    researchId: item.researchId,
    hotel: item.hotelKey,
    jevDecision: adviceRaw.jevDecision,
    jevDepth: adviceRaw.jevDepth,
    verdict: gated.verdict,
    reason: gated.reason,
    source: adviceRaw.source,
    jevCalled: adviceRaw.jevCalled,
    jevLive: adviceRaw.jevLive,
  };

  const advice = gated.acceptedAdvice || adviceRaw;
  const maxSteps = advice.maxSteps ?? 0;

  let opp = itemToOpp(item);
  const canonical = await enrichFromCanonical(item);
  if (canonical) {
    opp = {
      ...opp,
      ...canonical,
      id: canonical.id,
      // keep research thesis soft
      gdiJarV2: true,
    };
    if (!item.source && canonical.officialSource) item.source = canonical.officialSource;
  }

  const economics = {
    researchSteps: 0,
    pagesOpened: 0,
    queriesRun: 0,
    providerCalls: 0,
    estimatedCost: adviceRaw.costUsd || 0,
    blockersResolved: 0,
    jevCost: adviceRaw.costUsd || 0,
  };

  const gap0 = buildEvidenceGapMap(opp, { nowDate, geoTokens: item.geoTokens });
  const pageRows = [];
  const loopLog = [];
  let whoAttempted = false;
  let researched = false;

  if (
    gated.verdict !== POLICY_VERDICT.JEV_ACCEPTED ||
    advice.jevDecision !== "RESEARCH_NOW" ||
    maxSteps <= 0
  ) {
    const term = terminalClassify(opp, { nowDate, depthExhausted: true });
    return finalize({
      item,
      opp,
      adviceRaw,
      advice,
      policyRow,
      gap0,
      gapFinal: gap0,
      economics,
      pageRows,
      loopLog,
      term,
      researched: false,
      stopReason: gated.reason,
    });
  }

  researched = true;
  for (let step = 0; step < maxSteps && step < 3; step++) {
    economics.researchSteps += 1;
    const urls = [];
    if (step === 0 && (opp.officialSource || item.source)) {
      urls.push(opp.officialSource || item.source);
    } else {
      urls.push(...followUrls(opp, advice));
    }

    if (!urls.length) {
      const q =
        advice.jevResearchQuestion ||
        `${item.organization || item.title} ${advice.jevTargetBlocker} ${item.placeNames?.[0] || ""}`;
      const serp = await runTargetedCompletionSearch(q, {
        hl: advice.jevLanguage || "en",
        gl: item.serpGl || "us",
      });
      economics.queriesRun += 1;
      economics.providerCalls += 1;
      economics.estimatedCost += serp.costUsd || 0.05;
      budget.costUsd += serp.costUsd || 0.05;
      budget.leftUsd -= serp.costUsd || 0.05;
      for (const hit of (serp.hits || []).slice(0, 2)) {
        if (hit.link) urls.push(hit.link);
      }
    }

    let stepResolved = 0;
    let pageEmpty = true;
    for (const url of [...new Set(urls)].slice(0, 2)) {
      const page = await fetchCandidatePage(url);
      economics.pagesOpened += 1;
      economics.estimatedCost += 0.01;
      pageRows.push({
        researchId: item.researchId,
        step: step + 1,
        url: page.url || url,
        ok: page.ok,
        empty: !page.text,
        chars: (page.text || "").length,
        error: page.error || "",
      });
      if (!page.ok || !page.text) continue;
      pageEmpty = false;
      const before = buildEvidenceGapMap(opp, { nowDate, geoTokens: item.geoTokens });
      const { updates, facts } = extractEvidenceFromPage(opp, page);
      if (Object.keys(updates).length) {
        opp = { ...opp, ...updates };
        if (updates.functionalContactEmail || updates.organizationContactUrl || /contact/i.test(url)) {
          whoAttempted = true;
        }
        const after = buildEvidenceGapMap(opp, {
          nowDate,
          geoTokens: item.geoTokens,
          pageText: page.text,
        });
        stepResolved += countResolved(before, after);
        pageRows[pageRows.length - 1].factsExtracted = facts.length;
      }
    }

    if (whoAttempted) opp = stampWhoCeilingIfNeeded(opp, true);

    const gapNow = buildEvidenceGapMap(opp, { nowDate, geoTokens: item.geoTokens });
    economics.blockersResolved += stepResolved;

    // Update blockers list for next Jev loop
    item.currentBlockers = Object.entries(gapNow.gaps)
      .filter(([, v]) => v === GAP_STATE.FAIL || v === GAP_STATE.UNKNOWN)
      .map(([k]) => k);
    item.researchSteps = economics.researchSteps;

    const loop = await reviewJevActiveLoop(item, {
      stepsDone: economics.researchSteps,
      maxSteps,
      blockersResolvedThisStep: stepResolved,
      contactAttempted: whoAttempted,
      hasContact: Boolean(
        opp.primaryContact?.email || opp.functionalContactEmail || opp.organizationContactUrl
      ),
      pageEmpty,
      estimatedCost: economics.estimatedCost,
    });
    economics.estimatedCost += loop.costUsd || 0;
    budget.costUsd += loop.costUsd || 0;

    const loopGate = gateJevLoopAction(loop.action, item, { hardCapSteps: 3 });
    loopLog.push({
      researchId: item.researchId,
      step: economics.researchSteps,
      jevAction: loop.action,
      executedAction: loopGate.action,
      verdict: loopGate.verdict,
      rationale: loop.rationale,
      source: loop.source,
      blockersResolved: stepResolved,
    });

    if (
      loopGate.action === JEV_LOOP.STOP_LOW_INFORMATION_GAIN ||
      loopGate.action === JEV_LOOP.STOP_PUBLIC_DATA_CEILING
    ) {
      break;
    }
  }

  const gapFinal = buildEvidenceGapMap(opp, { nowDate, geoTokens: item.geoTokens });
  const term = terminalClassify(opp, { nowDate, depthExhausted: true });

  return finalize({
    item,
    opp,
    adviceRaw,
    advice,
    policyRow,
    gap0,
    gapFinal,
    economics,
    pageRows,
    loopLog,
    term,
    researched,
    stopReason: loopLog[loopLog.length - 1]?.rationale || "depth_complete",
  });
}

function finalize(ctx) {
  const {
    item,
    opp,
    adviceRaw,
    advice,
    policyRow,
    gap0,
    gapFinal,
    economics,
    pageRows,
    loopLog,
    term,
    researched,
    stopReason,
  } = ctx;

  const whoResolved = Boolean(
    opp.primaryContact?.email || opp.functionalContactEmail || opp.organizationContactUrl
  );
  const useful =
    term.class === TERMINAL.CUSTOMER_READY || term.class === TERMINAL.VALID_FUTURE_WATCH;
  const classChanged = useful === true;

  return {
    researchId: item.researchId,
    hotelKey: item.hotelKey,
    signalType: item.signalType,
    demandEngine: item.demandEngine,
    researchPriority: item.researchPriority,
    completionPotentialScore: item.completionPotentialScore,
    title: item.title,
    organization: item.organization,
    opportunityId: item.opportunityId || opp.id,
    advice: adviceRaw,
    acceptedAdvice: advice,
    policyRow,
    depth: advice.jevDepth || adviceRaw.jevDepth,
    maxSteps: advice.maxSteps ?? 0,
    gapBefore: gap0,
    gapAfter: gapFinal,
    lodgingClass: classifyLodgingCompletion(opp),
    timingState: classifyTiming(opp),
    whoResolved,
    whoEntity: opp.organizationName,
    whoPerson: opp.primaryContact?.name || opp.primaryContactName || null,
    howContact:
      opp.primaryContact?.email || opp.functionalContactEmail || opp.organizationContactUrl || null,
    terminalClass: term.class,
    terminalReason: term.reason || term.class,
    readyOk: term.ready?.ok === true,
    watchOk:
      term.watch?.ok === true ||
      term.watch?.valid === true ||
      term.watch?.class === "VALID_FUTURE_WATCH",
    useful,
    classChanged,
    economics,
    pageRows,
    loopLog,
    researched,
    stopReason,
    opportunity: opp,
    competitorFact: item.existingEvidence?.historicHotels || null,
    competitorInference: item.existingEvidence?.couldCompeteNextCycle || null,
  };
}

/**
 * Select P0 + selected P1 with type diversity (rotation/pre-RFP/signals/competitive).
 */
export function selectResearchTargets(scored = [], opts = {}) {
  const maxTotal = opts.maxTotal ?? 65;
  const quotas = {
    ROTATION_SERIES: opts.maxRotation ?? 22,
    PRE_RFP: opts.maxPreRfp ?? 18,
    DEMAND_SIGNAL: opts.maxSignals ?? 16,
    COMPETITIVE_PATTERN: opts.maxCompetitive ?? 2,
    V3_UNRESOLVED: opts.maxV3 ?? 8,
  };

  const pool = scored.filter(
    (x) => x.researchPriority === "P0_HIGH" || x.researchPriority === "P1_MEDIUM"
  );
  const picked = [];
  const used = new Set();
  const counts = Object.fromEntries(Object.keys(quotas).map((k) => [k, 0]));

  const take = (pred, n) => {
    for (const x of pool) {
      if (picked.length >= maxTotal) break;
      if (n <= 0) break;
      if (used.has(x.researchId)) continue;
      if (!pred(x)) continue;
      picked.push(x);
      used.add(x.researchId);
      counts[x.signalType] = (counts[x.signalType] || 0) + 1;
      n -= 1;
    }
  };

  // Guaranteed competitive first (tiny set)
  take((x) => x.signalType === "COMPETITIVE_PATTERN", quotas.COMPETITIVE_PATTERN);
  take((x) => x.signalType === "PRE_RFP" && x.researchPriority === "P0_HIGH", Math.min(10, quotas.PRE_RFP));
  take((x) => x.signalType === "ROTATION_SERIES" && x.researchPriority === "P0_HIGH", Math.min(12, quotas.ROTATION_SERIES));
  take(
    (x) =>
      x.signalType === "DEMAND_SIGNAL" &&
      (x.completionSignals || []).includes("lodging_hint"),
    Math.min(10, quotas.DEMAND_SIGNAL)
  );
  take((x) => x.signalType === "PRE_RFP", quotas.PRE_RFP - (counts.PRE_RFP || 0));
  take((x) => x.signalType === "ROTATION_SERIES", quotas.ROTATION_SERIES - (counts.ROTATION_SERIES || 0));
  take((x) => x.signalType === "DEMAND_SIGNAL", quotas.DEMAND_SIGNAL - (counts.DEMAND_SIGNAL || 0));
  take((x) => x.signalType === "V3_UNRESOLVED", quotas.V3_UNRESOLVED);

  // Fill remainder with highest-score leftovers
  take(() => true, maxTotal - picked.length);

  return picked;
}
