/**
 * Candidate Completion Engine V2 orchestrator.
 * Page-level evidence closure for P0/P1. Jev advisory only. No threshold changes.
 */

import { buildGdiCompletionPriority, COMPLETION_PRIORITY } from "./priority.js";
import { resolveGdiResearchDepth, RESEARCH_DEPTH, HARD_CAP_STEPS } from "./depth-policy.js";
import { buildEvidenceGapMap, classifyLodgingCompletion, GAP_STATE } from "./evidence-gap.js";
import {
  reviewJevResearchStrategy,
  reviewJevNextAction,
  JEV_LOOP_ACTION,
} from "./jev-strategy.js";
import {
  fetchCandidatePage,
  extractEvidenceFromPage,
  runTargetedCompletionSearch,
  stampWhoCeilingIfNeeded,
} from "./page-research.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import { classifyEntityTruth } from "../entity-truth-gate-v1.js";
import { classifyDemandEngine } from "../demand-engine-v1/classify-engine.js";

export const TERMINAL_CLASS = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  VALID_FUTURE_WATCH: "VALID_FUTURE_WATCH",
  REJECTED_CONFIRMED: "REJECTED_CONFIRMED",
  UNRESOLVED_PUBLIC_DATA_CEILING: "UNRESOLVED_PUBLIC_DATA_CEILING",
});

export const TIMING_STATE = Object.freeze({
  CONFIRMED_EXACT: "CONFIRMED_EXACT",
  CONFIRMED_RANGE: "CONFIRMED_RANGE",
  CONFIRMED_MONTH: "CONFIRMED_MONTH",
  CONFIRMED_YEAR: "CONFIRMED_YEAR",
  RECURRING_EXPECTED: "RECURRING_EXPECTED",
  ROTATION_PREDICTED: "ROTATION_PREDICTED",
  FUTURE_UNCONFIRMED: "FUTURE_UNCONFIRMED",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
});

function classifyTimingState(opp = {}) {
  const start = String(opp.eventStartDate || "");
  if (/^\d{4}-\d{2}-\d{2}$/.test(start)) return TIMING_STATE.CONFIRMED_EXACT;
  if (/^\d{4}-\d{2}$/.test(start)) return TIMING_STATE.CONFIRMED_MONTH;
  if (opp.eventEndDate && start) return TIMING_STATE.CONFIRMED_RANGE;
  if (opp.eventYear || /^\d{4}$/.test(start)) return TIMING_STATE.CONFIRMED_YEAR;
  if (/RECURRING|ANNUAL|ÉDITION/i.test(String(opp.futureCycleEvidenceState || opp.title || ""))) {
    return TIMING_STATE.RECURRING_EXPECTED;
  }
  if (/HISTORIC|PAST/i.test(String(opp.futureCycleEvidenceState || ""))) {
    return TIMING_STATE.HISTORICAL_ONLY;
  }
  if (opp.futureCycleEvidenceState) return TIMING_STATE.FUTURE_UNCONFIRMED;
  return TIMING_STATE.FUTURE_UNCONFIRMED;
}

function terminalClassify(opp, opts = {}) {
  const ready = isGdiCustomerOpportunityReady(opp, opts);
  if (ready.ok) return { class: TERMINAL_CLASS.CUSTOMER_READY, ready, watch: null };

  const watch = isValidFutureWatch(opp, { ...opts, ignoreTerminalPriority: true });
  const watchOk = watch?.ok === true || watch?.valid === true || watch?.class === "VALID_FUTURE_WATCH";
  if (watchOk) return { class: TERMINAL_CLASS.VALID_FUTURE_WATCH, ready, watch };

  if (
    opp.contactResearchState === "PUBLIC_DATA_CEILING" ||
    opp.whoPathClass === "NO_CONTACT_AFTER_RESEARCH" ||
    opp.publicDataCeiling === true
  ) {
    return { class: TERMINAL_CLASS.UNRESOLVED_PUBLIC_DATA_CEILING, ready, watch };
  }

  const entity = classifyEntityTruth(opp);
  if (!entity.validEntity) {
    return { class: TERMINAL_CLASS.REJECTED_CONFIRMED, ready, watch, reason: "ENTITY_INVALID" };
  }

  // Exhausted depth with no ready/watch → rejected confirmed (not "needs more research")
  if (opts.depthExhausted) {
    return {
      class: TERMINAL_CLASS.REJECTED_CONFIRMED,
      ready,
      watch,
      reason: "DEPTH_EXHAUSTED_NOT_QUALIFIED",
    };
  }

  return { class: TERMINAL_CLASS.REJECTED_CONFIRMED, ready, watch, reason: "FAILED_GATES" };
}

function countResolvedBlockers(before, after) {
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

function buildFollowUpUrl(opp, advice, hotelCtx) {
  const base = opp.officialSource || "";
  try {
    const u = new URL(base);
    const origin = u.origin;
    if (/lodging|housing|accommodation/i.test(advice.jevRecommendedSourceFamily || "")) {
      return [
        `${origin}/accommodation`,
        `${origin}/housing`,
        `${origin}/hotels`,
        `${origin}/en/accommodation`,
      ];
    }
    if (/contact|who|secretariat/i.test(advice.jevRecommendedSourceFamily || "")) {
      return [`${origin}/contact`, `${origin}/en/contact`, `${origin}/about`];
    }
    if (/procurement/i.test(advice.jevRecommendedSourceFamily || "")) {
      return [base];
    }
  } catch {
    /* ignore */
  }
  return [];
}

/**
 * Complete one candidate through page-level research loop.
 */
export async function completeOneCandidate(candidate, hotelCtx, opts = {}) {
  const nowDate = opts.nowDate || "2026-10-03";
  const geoTokens = hotelCtx.geoTokens || [];
  let opp = { ...candidate };
  const economics = {
    researchSteps: 0,
    pagesOpened: 0,
    queriesRun: 0,
    providerCalls: 0,
    estimatedCost: 0,
    blockersResolved: 0,
    classificationChange: false,
  };

  const gap0 = buildEvidenceGapMap(opp, { nowDate, geoTokens });
  const primaryBlocker = (gap0.smallestMissingFact || "TIMING").toUpperCase().includes("TIMING")
    ? "TIMING"
    : (gap0.smallestMissingFact || "LODGING").split("_")[0].toUpperCase();

  const strategyPacket = {
    title: opp.title,
    hotelKey: hotelCtx.hotelKey,
    hotelLabel: hotelCtx.label,
    marketId: hotelCtx.marketId,
    completionPriority: opp.completionPriority,
    completionScore: opp.completionScore,
    primaryBlocker,
    smallestMissingFact: gap0.smallestMissingFact,
    unresolvedDimensions: Object.entries(gap0.gaps)
      .filter(([, v]) => v === GAP_STATE.FAIL || v === GAP_STATE.UNKNOWN)
      .map(([k]) => k),
    sourcesAttempted: [],
    languagesAttempted: [opp.queryLanguage || "en"].filter(Boolean),
    feederMarketsAttempted: [],
    queryLanguage: opp.queryLanguage || "en",
    lodgingHint: gap0.lodgingHint,
    lodgingState: classifyLodgingCompletion(opp),
    timingState: classifyTimingState(opp),
    hotelFit: opp.hotelFitScore,
    entityPass: gap0.entityPass,
    estimatedCost: 0,
    estimatedRemainingResearchCost: 0.3,
    defaultFeederMarket: hotelCtx.feederHint || null,
  };

  const jevAdvice = await reviewJevResearchStrategy(strategyPacket);
  const depthDecision = resolveGdiResearchDepth(
    opp,
    {
      researchSteps: 0,
      entityPass: gap0.entityPass,
      geographyPass: gap0.geographyPass,
      LODGING: gap0.gaps.LODGING,
      lodgingHint: gap0.lodgingHint,
      commercialPotential: opp.completionScore,
      estimatedCost: 0,
      hotelFitScore: opp.hotelFitScore,
      ENTITY: gap0.gaps.ENTITY,
      GEOGRAPHY: gap0.gaps.GEOGRAPHY,
    },
    jevAdvice
  );

  const jevLog = [];
  jevLog.push({
    phase: "STRATEGY",
    opportunityId: opp.id,
    recommendation: jevAdvice.jevRecommendedSourceFamily,
    depth: jevAdvice.jevRecommendedDepth,
    accepted: true,
    rationale: jevAdvice.jevRationale,
    source: jevAdvice.source,
  });

  const pageRows = [];
  const maxSteps = depthDecision.maxSteps || 0;
  let gapPrev = gap0;
  let stopReason = depthDecision.rationale;
  let whoAttempted = false;

  if (maxSteps === 0) {
    const term = terminalClassify(opp, { nowDate, depthExhausted: true });
    return finalizeResult({
      opp,
      hotelCtx,
      jevAdvice,
      depthDecision,
      gap0,
      gapFinal: gap0,
      economics,
      jevLog,
      pageRows,
      term,
      stopReason,
      researched: false,
    });
  }

  // Step 1+: page fetch primary URL
  for (let step = 0; step < maxSteps && step < HARD_CAP_STEPS; step++) {
    economics.researchSteps += 1;
    const urls = [];
    if (step === 0 && opp.officialSource) urls.push(opp.officialSource);
    else urls.push(...buildFollowUpUrl(opp, jevAdvice, hotelCtx));

    // Targeted SERP if no URL / empty prior
    if (!urls.length || (step > 0 && pageRows.length && pageRows[pageRows.length - 1].empty)) {
      const q =
        jevAdvice.jevNextQuestion ||
        `${opp.organizationName || opp.title} ${jevAdvice.jevRecommendedSourceFamily} ${hotelCtx.placeNames?.[0] || ""}`;
      const serp = await runTargetedCompletionSearch(q, {
        hl: jevAdvice.jevRecommendedLanguage || "en",
        gl: hotelCtx.serpGl || "us",
      });
      economics.queriesRun += 1;
      economics.providerCalls += 1;
      economics.estimatedCost += serp.costUsd || 0.05;
      for (const hit of (serp.hits || []).slice(0, 2)) {
        if (hit.link) urls.push(hit.link);
      }
    }

    let stepResolved = 0;
    let pageEmpty = true;
    for (const url of [...new Set(urls)].slice(0, 2)) {
      const page = await fetchCandidatePage(url);
      economics.pagesOpened += 1;
      economics.estimatedCost += 0.01; // fetch cost proxy
      pageRows.push({
        opportunityId: opp.id,
        step: step + 1,
        url: page.url || url,
        ok: page.ok,
        error: page.error || "",
        empty: !page.text,
        chars: (page.text || "").length,
      });
      if (!page.ok || !page.text) continue;
      pageEmpty = false;
      const { updates, facts } = extractEvidenceFromPage(opp, page);
      if (Object.keys(updates).length) {
        const beforeGap = buildEvidenceGapMap(opp, { nowDate, geoTokens, pageText: "" });
        opp = { ...opp, ...updates };
        if (/contact/i.test(url) || updates.functionalContactEmail || updates.organizationContactUrl) {
          whoAttempted = true;
        }
        const afterGap = buildEvidenceGapMap(opp, {
          nowDate,
          geoTokens,
          pageText: page.text,
        });
        stepResolved += countResolvedBlockers(beforeGap, afterGap);
        pageRows[pageRows.length - 1].factsExtracted = facts.length;
      }
    }

    if (whoAttempted) {
      opp = stampWhoCeilingIfNeeded(opp, true);
    }

    const gapNow = buildEvidenceGapMap(opp, { nowDate, geoTokens });
    economics.blockersResolved += stepResolved;
    gapPrev = gapNow;

    const next = await reviewJevNextAction({
      gapMap: gapNow,
      stepsDone: economics.researchSteps,
      maxSteps,
      blockersResolvedThisStep: stepResolved,
      contactAttempted: whoAttempted,
      hasContact: Boolean(
        opp.primaryContact?.email || opp.functionalContactEmail || opp.organizationContactUrl
      ),
      pagesEmpty: pageEmpty,
      estimatedCost: economics.estimatedCost,
    });

    jevLog.push({
      phase: "LOOP",
      opportunityId: opp.id,
      step: economics.researchSteps,
      recommendation: next.action,
      accepted: next.acceptedByPolicy !== false,
      rejectedByPolicy: next.rejectedByPolicy === true,
      blockerResolved: stepResolved > 0,
      rationale: next.rationale,
      source: next.source,
    });

    stopReason = next.rationale || stopReason;
    if (
      next.action === JEV_LOOP_ACTION.STOP_LOW_INFORMATION_GAIN ||
      next.action === JEV_LOOP_ACTION.STOP_PUBLIC_DATA_CEILING
    ) {
      break;
    }
    if (next.action === JEV_LOOP_ACTION.SWITCH_SOURCE) {
      // continue loop with follow-up URLs
      continue;
    }
    if (next.action !== JEV_LOOP_ACTION.CONTINUE && next.action !== JEV_LOOP_ACTION.SWITCH_BLOCKER) {
      break;
    }
  }

  const gapFinal = buildEvidenceGapMap(opp, { nowDate, geoTokens });
  const term = terminalClassify(opp, {
    nowDate,
    depthExhausted: true,
  });

  return finalizeResult({
    opp,
    hotelCtx,
    jevAdvice,
    depthDecision,
    gap0,
    gapFinal,
    economics,
    jevLog,
    pageRows,
    term,
    stopReason,
    researched: true,
  });
}

function finalizeResult({
  opp,
  hotelCtx,
  jevAdvice,
  depthDecision,
  gap0,
  gapFinal,
  economics,
  jevLog,
  pageRows,
  term,
  stopReason,
  researched,
}) {
  const lodgingClass = classifyLodgingCompletion(opp);
  const timingState = classifyTimingState(opp);
  const whoResolved = Boolean(
    opp.primaryContact?.email || opp.functionalContactEmail || opp.organizationContactUrl
  );
  const classBefore = "V3_UNQUALIFIED";
  economics.classificationChange = term.class !== "REJECTED_CONFIRMED" || economics.blockersResolved > 0;

  return {
    opportunity: opp,
    hotelKey: hotelCtx.hotelKey,
    completionPriority: opp.completionPriority,
    completionScore: opp.completionScore,
    completionSignals: opp.completionSignals,
    demandEngine: (() => {
      const e = opp.demandEngine || classifyDemandEngine(opp);
      return typeof e === "string" ? e : e?.demandEngine || null;
    })(),
    jevAdvice,
    depthDecision,
    depthAssigned: depthDecision.depth,
    gapBefore: gap0,
    gapAfter: gapFinal,
    lodgingClass,
    timingState,
    procurementStatus: opp.procurementStatus || "UNKNOWN",
    whoResolved,
    whoEntity: opp.organizationName || null,
    whoRole: opp.primaryContact?.role || opp.primaryContactRole || null,
    whoPerson: opp.primaryContact?.name || opp.primaryContactName || null,
    howContact:
      opp.primaryContact?.email ||
      opp.functionalContactEmail ||
      opp.organizationContactUrl ||
      null,
    terminalClass: term.class,
    terminalReason: term.reason || term.class,
    readyOk: term.ready?.ok === true,
    watchOk:
      term.watch?.ok === true ||
      term.watch?.valid === true ||
      term.watch?.class === "VALID_FUTURE_WATCH",
    readyFailed: term.ready?.failed || [],
    economics,
    jevLog,
    pageRows,
    stopReason,
    researched,
    classBefore,
    useful:
      term.class === TERMINAL_CLASS.CUSTOMER_READY ||
      term.class === TERMINAL_CLASS.VALID_FUTURE_WATCH,
    competitorFact: null,
    competitorInference: null,
    rotationNote: opp.eventSeriesId
      ? "Series id present; historical cycles not expanded beyond public page"
      : null,
  };
}

/**
 * Run completion for a hotel's candidate list (already prioritized).
 */
export async function runCompletionForHotel(hotelCtx, candidates, opts = {}) {
  const { queue, tallies } = buildGdiCompletionPriority(candidates);
  const selected = queue.filter(
    (c) =>
      c.completionPriority === COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL ||
      c.completionPriority === COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL
  );

  const results = [];
  const maxP0 = opts.maxP0 ?? 999;
  const maxP1 = opts.maxP1 ?? 999;
  let p0n = 0;
  let p1n = 0;

  for (const c of selected) {
    if (c.completionPriority === COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL) {
      if (p0n >= maxP0) continue;
      p0n += 1;
    } else {
      if (p1n >= maxP1) continue;
      p1n += 1;
    }
    const r = await completeOneCandidate(c, hotelCtx, opts);
    results.push(r);
  }

  // P2 deferred — terminal without research
  for (const c of queue.filter(
    (x) => x.completionPriority === COMPLETION_PRIORITY.P2_LOW_DEFER
  )) {
    const gap = buildEvidenceGapMap(c, { nowDate: opts.nowDate, geoTokens: hotelCtx.geoTokens });
    results.push({
      opportunity: c,
      hotelKey: hotelCtx.hotelKey,
      completionPriority: c.completionPriority,
      completionScore: c.completionScore,
      completionSignals: c.completionSignals,
      demandEngine: (() => {
        const e = c.demandEngine || classifyDemandEngine(c);
        return typeof e === "string" ? e : e?.demandEngine || null;
      })(),
      jevAdvice: null,
      depthDecision: { depth: RESEARCH_DEPTH.DEPTH_0_STOP_NOW, maxSteps: 0, rationale: "P2_defer" },
      depthAssigned: RESEARCH_DEPTH.DEPTH_0_STOP_NOW,
      gapBefore: gap,
      gapAfter: gap,
      lodgingClass: classifyLodgingCompletion(c),
      timingState: classifyTimingState(c),
      procurementStatus: "UNKNOWN",
      whoResolved: false,
      terminalClass: TERMINAL_CLASS.REJECTED_CONFIRMED,
      terminalReason: "P2_DEFERRED_NO_COMPLETION",
      readyOk: false,
      watchOk: false,
      readyFailed: ["deferred"],
      economics: {
        researchSteps: 0,
        pagesOpened: 0,
        queriesRun: 0,
        providerCalls: 0,
        estimatedCost: 0,
        blockersResolved: 0,
        classificationChange: false,
      },
      jevLog: [],
      pageRows: [],
      stopReason: "P2_defer",
      researched: false,
      useful: false,
    });
  }

  return { queue, tallies, results, selectedCount: selected.length };
}
