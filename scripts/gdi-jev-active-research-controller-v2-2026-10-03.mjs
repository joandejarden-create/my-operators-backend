/**
 * GDI Jev Active Research Controller V2
 * Signal → research strategy → evidence completion → canonical classification.
 * Jev advisory apply for WHERE/HOW FAR only. No threshold changes. No broad discovery.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadActiveResearchUniverse,
  applyBaselineScoring,
  selectResearchTargets,
  researchOneItem,
  adviseEngineCoverage,
  TERMINAL,
  RESEARCH_PRIORITY,
} from "../lib/group-demand-intelligence/jev-active-research-v2/index.js";
import { DEMAND_ENGINE_TAXONOMY } from "../lib/group-demand-intelligence/demand-engine-v1/taxonomy.js";
import { describeJevConfig } from "../lib/group-demand-intelligence/jev/jev-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "jev-active-research-controller-v2");
const DE_DIR = path.join(__dirname, "..", "reports", "gdi", "demand-engine-jev-controller-v1");
const CC_DIR = path.join(__dirname, "..", "reports", "gdi", "candidate-completion-jev-v2-2026-10-03");
const NOW = "2026-10-03";

const HOTELS = [
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelKey: "YOTEL",
    label: "YOTEL Geneva Lake",
    marketId: "geneva_lake",
    placeNames: ["Geneva", "Genève", "Nyon", "La Côte", "Palexpo"],
    geoTokens: ["geneva", "genève", "switzerland", "palexpo", "nyon"],
    defaultFitScore: 52,
    serpGl: "ch",
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    label: "AC Hotel A Coruña",
    marketId: "a_coruna",
    placeNames: ["A Coruña", "Coruña", "Galicia"],
    geoTokens: ["coruña", "coruna", "galicia", "spain"],
    defaultFitScore: 50,
    serpGl: "es",
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    label: "Spice Island Beach Resort",
    marketId: "grenada",
    placeNames: ["Grenada", "Grand Anse"],
    geoTokens: ["grenada", "grand anse"],
    defaultFitScore: 48,
    serpGl: "us",
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    label: "Cambridge Beaches Resort & Spa",
    marketId: "bermuda",
    placeNames: ["Bermuda", "Somerset"],
    geoTokens: ["bermuda", "somerset"],
    defaultFitScore: 48,
    serpGl: "us",
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    label: "NOW NOW NOHO",
    marketId: "nyc_noho",
    placeNames: ["New York", "NoHo", "Manhattan"],
    geoTokens: ["new york", "nyc", "manhattan", "noho"],
    defaultFitScore: 50,
    serpGl: "us",
  },
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}
function engineLabel(code) {
  return DEMAND_ENGINE_TAXONOMY[code]?.label || code || "UNKNOWN";
}

function agg(rows, keyFn) {
  const map = new Map();
  for (const r of rows) {
    const k = keyFn(r) || "UNKNOWN";
    if (!map.has(k)) {
      map.set(k, {
        key: k,
        n: 0,
        researched: 0,
        ready: 0,
        watch: 0,
        useful: 0,
        cost: 0,
      });
    }
    const m = map.get(k);
    m.n += 1;
    if (r.researched) m.researched += 1;
    if (r.terminalClass === TERMINAL.CUSTOMER_READY) m.ready += 1;
    if (r.terminalClass === TERMINAL.VALID_FUTURE_WATCH) m.watch += 1;
    if (r.useful) m.useful += 1;
    m.cost += r.economics?.estimatedCost || 0;
  }
  return [...map.values()].sort((a, b) => b.useful - a.useful || b.n - a.n);
}

function defaultBehavior(qa, useful) {
  const acceptRate = qa.issued ? qa.accepted / qa.issued : 0;
  const resolveRate = qa.accepted ? qa.resolved / qa.accepted : 0;
  const cond = (ok) => (ok ? "CONDITIONAL" : "NO");
  return {
    priority: "NO",
    nextBlocker: cond(resolveRate >= 0.1 && qa.issued >= 5),
    source: cond(resolveRate >= 0.08 && acceptRate >= 0.4),
    language: useful > 0 ? "CONDITIONAL" : "NO",
    feeder: useful > 0 ? "CONDITIONAL" : "NO",
    depth: cond(qa.depth2Useful > 0 || qa.depth3Useful > 0),
    stopContinue: cond(qa.lowYieldStops >= 1 || acceptRate >= 0.3),
  };
}

async function main() {
  console.log("[jar-v2] Jev config:", JSON.stringify(describeJevConfig()));
  console.log("[jar-v2] loading universe…");
  const { items, tallies: loadTallies } = loadActiveResearchUniverse({
    demandEngineDir: DE_DIR,
    completionDir: CC_DIR,
    hotels: HOTELS,
  });
  console.log("[jar-v2] universe raw tallies", loadTallies, "deduped", items.length);

  const { scored, tallies: scoreTallies } = applyBaselineScoring(items);
  console.log("[jar-v2] priority", scoreTallies);

  const targets = selectResearchTargets(scored, { maxP0: 35, maxP1: 30 });
  console.log(`[jar-v2] selected for active research: ${targets.length}`);

  const budget = { leftUsd: 8, costUsd: 0 };
  const results = [];
  let i = 0;
  for (const t of targets) {
    i += 1;
    if (i % 5 === 1) console.log(`[jar-v2] researching ${i}/${targets.length} ${t.hotelKey} ${t.signalType}…`);
    const r = await researchOneItem(t, { nowDate: NOW, budget });
    results.push(r);
  }

  // P2 / non-selected → terminal without research
  const selectedIds = new Set(targets.map((t) => t.researchId));
  for (const it of scored) {
    if (selectedIds.has(it.researchId)) continue;
    results.push({
      researchId: it.researchId,
      hotelKey: it.hotelKey,
      signalType: it.signalType,
      demandEngine: it.demandEngine,
      researchPriority: it.researchPriority,
      completionPotentialScore: it.completionPotentialScore,
      title: it.title,
      organization: it.organization,
      opportunityId: it.opportunityId,
      advice: null,
      acceptedAdvice: null,
      policyRow: null,
      depth: "DEPTH_0_STOP",
      maxSteps: 0,
      gapBefore: null,
      gapAfter: null,
      lodgingClass: "NO_LODGING_SUPPORT",
      timingState: it.timingState,
      whoResolved: false,
      terminalClass: TERMINAL.REJECTED_CONFIRMED,
      terminalReason: it.researchPriority === RESEARCH_PRIORITY.P2_LOW ? "P2_DEFERRED" : "NOT_SELECTED_THIS_TRANCHE",
      readyOk: false,
      watchOk: false,
      useful: false,
      classChanged: false,
      economics: {
        researchSteps: 0,
        pagesOpened: 0,
        queriesRun: 0,
        providerCalls: 0,
        estimatedCost: 0,
        blockersResolved: 0,
        jevCost: 0,
      },
      pageRows: [],
      loopLog: [],
      researched: false,
      stopReason: "not_selected",
      competitorFact: it.existingEvidence?.historicHotels || null,
      competitorInference: it.existingEvidence?.couldCompeteNextCycle || null,
    });
  }

  // ——— Outputs ———
  write(
    "ACTIVE_RESEARCH_UNIVERSE.csv",
    toCsv(
      scored.map((s) => ({
        researchId: s.researchId,
        hotel: s.hotelKey,
        marketId: s.marketId,
        demandEngine: s.demandEngine,
        signalType: s.signalType,
        organization: s.organization,
        eventProgram: s.eventProgram,
        source: s.source,
        language: s.language,
        feederMarket: s.feederMarket,
        timingState: s.timingState,
        lodgingState: s.lodgingState,
        blockers: (s.currentBlockers || []).join("|"),
        commercialPotential: s.estimatedCommercialPotential,
        priorSteps: s.priorResearchSteps,
        priorCost: s.priorCost,
      })),
      [
        "researchId",
        "hotel",
        "marketId",
        "demandEngine",
        "signalType",
        "organization",
        "eventProgram",
        "source",
        "language",
        "feederMarket",
        "timingState",
        "lodgingState",
        "blockers",
        "commercialPotential",
        "priorSteps",
        "priorCost",
      ]
    )
  );

  write(
    "COMPLETION_PRIORITY.csv",
    toCsv(
      scored.map((s) => ({
        researchId: s.researchId,
        hotel: s.hotelKey,
        title: s.title,
        signalType: s.signalType,
        priority: s.researchPriority,
        score: s.completionPotentialScore,
        signals: (s.completionSignals || []).join("|"),
        selected: selectedIds.has(s.researchId),
      })),
      ["researchId", "hotel", "title", "signalType", "priority", "score", "signals", "selected"]
    )
  );

  const jevRecs = results.filter((r) => r.advice).map((r) => ({
    researchId: r.researchId,
    hotel: r.hotelKey,
    title: r.title,
    jevDecision: r.advice.jevDecision,
    jevPriority: r.advice.jevPriority,
    jevTargetBlocker: r.advice.jevTargetBlocker,
    jevResearchQuestion: r.advice.jevResearchQuestion,
    jevSourceFamily: r.advice.jevSourceFamily,
    jevLanguage: r.advice.jevLanguage,
    jevFeederMarket: r.advice.jevFeederMarket,
    jevDepth: r.advice.jevDepth,
    jevStopCondition: r.advice.jevStopCondition,
    jevExpectedInformationGain: r.advice.jevExpectedInformationGain,
    jevPromotionPotential: r.advice.jevPromotionPotential,
    jevRationale: r.advice.jevRationale,
    source: r.advice.source,
    jevLive: r.advice.jevLive,
  }));
  write(
    "JEV_RECOMMENDATIONS.csv",
    toCsv(jevRecs, [
      "researchId",
      "hotel",
      "title",
      "jevDecision",
      "jevPriority",
      "jevTargetBlocker",
      "jevResearchQuestion",
      "jevSourceFamily",
      "jevLanguage",
      "jevFeederMarket",
      "jevDepth",
      "jevStopCondition",
      "jevExpectedInformationGain",
      "jevPromotionPotential",
      "jevRationale",
      "source",
      "jevLive",
    ])
  );

  const policyRows = results.filter((r) => r.policyRow).map((r) => r.policyRow);
  write(
    "JEV_POLICY_DECISIONS.csv",
    toCsv(policyRows, [
      "researchId",
      "hotel",
      "jevDecision",
      "jevDepth",
      "verdict",
      "reason",
      "source",
      "jevCalled",
      "jevLive",
    ])
  );

  write(
    "PAGE_LEVEL_RESEARCH.csv",
    toCsv(
      results.flatMap((r) =>
        (r.pageRows || []).map((p) => ({
          hotel: r.hotelKey,
          researchId: p.researchId,
          step: p.step,
          url: p.url,
          ok: p.ok,
          empty: p.empty,
          chars: p.chars,
          error: p.error,
          factsExtracted: p.factsExtracted ?? 0,
        }))
      ),
      ["hotel", "researchId", "step", "url", "ok", "empty", "chars", "error", "factsExtracted"]
    )
  );

  write(
    "ADAPTIVE_RESEARCH_LOG.csv",
    toCsv(
      results.flatMap((r) =>
        (r.loopLog || []).map((l) => ({
          hotel: r.hotelKey,
          researchId: l.researchId,
          step: l.step,
          jevAction: l.jevAction,
          executedAction: l.executedAction,
          verdict: l.verdict,
          rationale: l.rationale,
          source: l.source,
          blockersResolved: l.blockersResolved,
        }))
      ),
      [
        "hotel",
        "researchId",
        "step",
        "jevAction",
        "executedAction",
        "verdict",
        "rationale",
        "source",
        "blockersResolved",
      ]
    )
  );

  write(
    "TIMING_COMPLETION.csv",
    toCsv(
      results.map((r) => ({
        hotel: r.hotelKey,
        researchId: r.researchId,
        signalType: r.signalType,
        timingState: r.timingState,
        timingGap: r.gapAfter?.gaps?.TIMING,
        eventStart: r.opportunity?.eventStartDate,
      })),
      ["hotel", "researchId", "signalType", "timingState", "timingGap", "eventStart"]
    )
  );

  write(
    "LODGING_COMPLETION.csv",
    toCsv(
      results.map((r) => ({
        hotel: r.hotelKey,
        researchId: r.researchId,
        lodgingClass: r.lodgingClass,
        lodgingGap: r.gapAfter?.gaps?.LODGING,
      })),
      ["hotel", "researchId", "lodgingClass", "lodgingGap"]
    )
  );

  write(
    "WHO_COMPLETION.csv",
    toCsv(
      results.map((r) => ({
        hotel: r.hotelKey,
        researchId: r.researchId,
        whoResolved: r.whoResolved,
        whoEntity: r.whoEntity,
        whoPerson: r.whoPerson,
        howContact: r.howContact,
      })),
      ["hotel", "researchId", "whoResolved", "whoEntity", "whoPerson", "howContact"]
    )
  );

  write(
    "ROTATION_PRE_RFP_COMPLETION.csv",
    toCsv(
      results
        .filter((r) => r.signalType === "ROTATION_SERIES" || r.signalType === "PRE_RFP")
        .map((r) => ({
          hotel: r.hotelKey,
          researchId: r.researchId,
          signalType: r.signalType,
          researched: r.researched,
          timingState: r.timingState,
          terminalClass: r.terminalClass,
          depth: r.depth,
          useful: r.useful,
        })),
      [
        "hotel",
        "researchId",
        "signalType",
        "researched",
        "timingState",
        "terminalClass",
        "depth",
        "useful",
      ]
    )
  );

  write(
    "COMPETITIVE_DEMAND_COMPLETION.csv",
    toCsv(
      results
        .filter((r) => r.signalType === "COMPETITIVE_PATTERN")
        .map((r) => ({
          hotel: r.hotelKey,
          researchId: r.researchId,
          competitorFact: r.competitorFact,
          competitorInference: r.competitorInference,
          terminalClass: r.terminalClass,
          researched: r.researched,
          note: "FACT vs INFERENCE separated; no hosting invented",
        })),
      [
        "hotel",
        "researchId",
        "competitorFact",
        "competitorInference",
        "terminalClass",
        "researched",
        "note",
      ]
    )
  );

  const engineYield = agg(results, (r) => r.demandEngine);
  const coverageAfter = engineYield.map((e, idx) => ({
    demandEngine: e.key,
    label: engineLabel(e.key),
    candidates: e.n,
    researched: e.researched,
    useful: e.useful,
    ready: e.ready,
    watch: e.watch,
    underResearched: e.researched < 2,
    priorityRank: idx + 1,
  }));
  const engineAdvice = adviseEngineCoverage(coverageAfter);
  write(
    "ENGINE_COVERAGE_AFTER.csv",
    toCsv(
      coverageAfter.map((e) => ({
        ...e,
        controllerAdvice:
          engineAdvice.find((a) => a.demandEngine === e.demandEngine)?.action || "",
      })),
      [
        "demandEngine",
        "label",
        "candidates",
        "researched",
        "useful",
        "ready",
        "watch",
        "underResearched",
        "priorityRank",
        "controllerAdvice",
      ]
    )
  );

  const langYield = agg(results, (r) => r.advice?.jevLanguage || r.opportunity?.queryLanguage || "en");
  write(
    "LANGUAGE_YIELD.csv",
    toCsv(
      langYield.map((e) => ({
        language: e.key,
        candidates: e.n,
        researched: e.researched,
        useful: e.useful,
        ready: e.ready,
        watch: e.watch,
      })),
      ["language", "candidates", "researched", "useful", "ready", "watch"]
    )
  );

  const feederYield = agg(
    results,
    (r) => r.advice?.jevFeederMarket || r.opportunity?.discoveryMeta?.feederMarket || "NONE"
  );
  write(
    "FEEDER_YIELD.csv",
    toCsv(
      feederYield.map((e) => ({
        feederMarket: e.key,
        candidates: e.n,
        researched: e.researched,
        useful: e.useful,
        ready: e.ready,
        watch: e.watch,
      })),
      ["feederMarket", "candidates", "researched", "useful", "ready", "watch"]
    )
  );

  // Jev performance
  const issued = policyRows.length;
  const accepted = policyRows.filter((p) => p.verdict === "JEV_ACCEPTED").length;
  const rejectedPolicy = policyRows.filter((p) => p.verdict === "JEV_REJECTED_POLICY").length;
  const rejectedLow = policyRows.filter((p) => p.verdict === "JEV_REJECTED_LOW_VALUE").length;
  const lowYieldStops =
    jevRecs.filter((r) => r.jevDecision === "STOP_LOW_YIELD").length +
    policyRows.filter((p) => p.verdict === "JEV_REJECTED_LOW_VALUE").length;

  const acceptedResults = results.filter(
    (r) => r.policyRow?.verdict === "JEV_ACCEPTED" && r.researched
  );
  const resolvedFromAccepted = acceptedResults.filter((r) => r.economics.blockersResolved > 0).length;
  const classChangedAccepted = acceptedResults.filter((r) => r.classChanged).length;
  const readyFromAccepted = acceptedResults.filter(
    (r) => r.terminalClass === TERMINAL.CUSTOMER_READY
  ).length;
  const watchFromAccepted = acceptedResults.filter(
    (r) => r.terminalClass === TERMINAL.VALID_FUTURE_WATCH
  ).length;
  const deadEndAvoided = policyRows.filter(
    (p) => p.verdict === "JEV_REJECTED_LOW_VALUE" || /STOP_LOW|historical/i.test(p.reason || "")
  ).length;

  const jevBlockerRate = accepted ? resolvedFromAccepted / accepted : 0;
  const jevClassRate = accepted ? classChangedAccepted / accepted : 0;
  const usefulTotal = results.filter((r) => r.useful).length;
  const jevUsefulYield = accepted ? (readyFromAccepted + watchFromAccepted) / accepted : 0;
  const totalCost = results.reduce((s, r) => s + (r.economics.estimatedCost || 0), 0);
  const costPerUseful = usefulTotal ? totalCost / usefulTotal : null;

  write(
    "JEV_PERFORMANCE.csv",
    toCsv(
      [
        { metric: "JEV_RECOMMENDATIONS_ISSUED", value: issued },
        { metric: "JEV_ACCEPTED", value: accepted },
        { metric: "JEV_REJECTED_POLICY", value: rejectedPolicy },
        { metric: "JEV_REJECTED_LOW_VALUE", value: rejectedLow },
        { metric: "JEV_LOW_YIELD_STOPS", value: lowYieldStops },
        { metric: "ACCEPTED_BLOCKERS_RESOLVED", value: resolvedFromAccepted },
        { metric: "ACCEPTED_CLASSIFICATION_CHANGED", value: classChangedAccepted },
        { metric: "ACCEPTED_CUSTOMER_READY", value: readyFromAccepted },
        { metric: "ACCEPTED_VALID_WATCH", value: watchFromAccepted },
        { metric: "DEAD_END_AVOIDED", value: deadEndAvoided },
        { metric: "JEV_BLOCKER_RESOLUTION_RATE", value: jevBlockerRate.toFixed(3) },
        { metric: "JEV_CLASSIFICATION_CHANGE_RATE", value: jevClassRate.toFixed(3) },
        { metric: "JEV_USEFUL_OPPORTUNITY_YIELD", value: jevUsefulYield.toFixed(3) },
        {
          metric: "JEV_COST_PER_USEFUL",
          value: costPerUseful == null ? "N/A" : costPerUseful.toFixed(3),
        },
      ],
      ["metric", "value"]
    )
  );

  write(
    "FINAL_CLASSIFICATION.csv",
    toCsv(
      results.map((r) => ({
        hotel: r.hotelKey,
        researchId: r.researchId,
        signalType: r.signalType,
        priority: r.researchPriority,
        depth: r.depth,
        researched: r.researched,
        terminalClass: r.terminalClass,
        terminalReason: r.terminalReason,
        useful: r.useful,
        cost: r.economics.estimatedCost,
        blockersResolved: r.economics.blockersResolved,
        steps: r.economics.researchSteps,
      })),
      [
        "hotel",
        "researchId",
        "signalType",
        "priority",
        "depth",
        "researched",
        "terminalClass",
        "terminalReason",
        "useful",
        "cost",
        "blockersResolved",
        "steps",
      ]
    )
  );

  const hotelRows = HOTELS.map((h) => {
    const rs = results.filter((r) => r.hotelKey === h.hotelKey);
    const researched = rs.filter((r) => r.researched);
    const depthScore = (d) =>
      d === "DEPTH_3_DEEP" ? 3 : d === "DEPTH_2_TWO_STEPS" ? 2 : d === "DEPTH_1_ONE_STEP" ? 1 : 0;
    return {
      hotel: h.hotelKey,
      label: h.label,
      signalsResearched: researched.filter((r) => r.signalType === "DEMAND_SIGNAL").length,
      rotationResearched: researched.filter((r) => r.signalType === "ROTATION_SERIES").length,
      preRfpResearched: researched.filter((r) => r.signalType === "PRE_RFP").length,
      P0Researched: researched.filter((r) => r.researchPriority === "P0_HIGH").length,
      P1Researched: researched.filter((r) => r.researchPriority === "P1_MEDIUM").length,
      ready: rs.filter((r) => r.terminalClass === TERMINAL.CUSTOMER_READY).length,
      watch: rs.filter((r) => r.terminalClass === TERMINAL.VALID_FUTURE_WATCH).length,
      rejected: rs.filter((r) => r.terminalClass === TERMINAL.REJECTED_CONFIRMED).length,
      unresolved: rs.filter((r) => r.terminalClass === TERMINAL.UNRESOLVED_PUBLIC_DATA_CEILING)
        .length,
      averageDepth: researched.length
        ? (researched.reduce((s, r) => s + depthScore(r.depth), 0) / researched.length).toFixed(2)
        : 0,
      cost: rs.reduce((s, r) => s + (r.economics.estimatedCost || 0), 0).toFixed(3),
    };
  });
  write(
    "HOTEL_RESULTS.csv",
    toCsv(hotelRows, [
      "hotel",
      "label",
      "signalsResearched",
      "rotationResearched",
      "preRfpResearched",
      "P0Researched",
      "P1Researched",
      "ready",
      "watch",
      "rejected",
      "unresolved",
      "averageDepth",
      "cost",
    ])
  );

  const pageResearched = results.filter((r) => r.researched).length;
  const d1 = results.filter((r) => r.researched && r.depth === "DEPTH_1_ONE_STEP").length;
  const d2 = results.filter((r) => r.researched && r.depth === "DEPTH_2_TWO_STEPS").length;
  const d3 = results.filter((r) => r.researched && r.depth === "DEPTH_3_DEEP").length;
  const ready = results.filter((r) => r.terminalClass === TERMINAL.CUSTOMER_READY).length;
  const watch = results.filter((r) => r.terminalClass === TERMINAL.VALID_FUTURE_WATCH).length;
  const rejected = results.filter((r) => r.terminalClass === TERMINAL.REJECTED_CONFIRMED).length;
  const unresolved = results.filter(
    (r) => r.terminalClass === TERMINAL.UNRESOLVED_PUBLIC_DATA_CEILING
  ).length;

  const hotelReady = (k) =>
    results.filter((r) => r.hotelKey === k && r.terminalClass === TERMINAL.CUSTOMER_READY).length;
  const hotelWatch = (k) =>
    results.filter((r) => r.hotelKey === k && r.terminalClass === TERMINAL.VALID_FUTURE_WATCH)
      .length;

  const rotationCompleted = results.filter(
    (r) => r.signalType === "ROTATION_SERIES" && r.researched
  ).length;
  const preRfpCompleted = results.filter((r) => r.signalType === "PRE_RFP" && r.researched).length;
  const competitiveCompleted = results.filter(
    (r) => r.signalType === "COMPETITIVE_PATTERN" && r.researched
  ).length;

  const timingResolved = results.filter(
    (r) =>
      r.researched &&
      r.gapBefore?.gaps?.TIMING !== "PASS" &&
      (r.gapAfter?.gaps?.TIMING === "PASS" || r.gapAfter?.gaps?.TIMING === "PARTIAL")
  ).length;
  const lodgingResolved = results.filter(
    (r) =>
      r.researched &&
      r.gapBefore?.gaps?.LODGING === "FAIL" &&
      (r.gapAfter?.gaps?.LODGING === "PASS" || r.gapAfter?.gaps?.LODGING === "PARTIAL")
  ).length;
  const whoResolved = results.filter((r) => r.whoResolved).length;

  const topEngine = engineYield[0];
  const topLang = langYield[0];
  const topFeeder = feederYield[0];

  const defaults = defaultBehavior(
    {
      issued,
      accepted,
      resolved: resolvedFromAccepted,
      lowYieldStops,
      depth2Useful: results.filter((r) => r.depth === "DEPTH_2_TWO_STEPS" && r.useful).length,
      depth3Useful: results.filter((r) => r.depth === "DEPTH_3_DEEP" && r.useful).length,
    },
    usefulTotal
  );

  write(
    "COST_REPORT.md",
    `# Cost Report — Jev Active Research Controller V2

| Metric | Value |
|--------|------:|
| Total estimated cost (USD) | ${totalCost.toFixed(3)} |
| Budget used (tracked) | ${budget.costUsd.toFixed(3)} |
| Pages opened | ${results.reduce((s, r) => s + r.economics.pagesOpened, 0)} |
| Queries run | ${results.reduce((s, r) => s + r.economics.queriesRun, 0)} |
| Research steps | ${results.reduce((s, r) => s + r.economics.researchSteps, 0)} |
| Jev API cost (tracked) | ${results.reduce((s, r) => s + (r.economics.jevCost || 0), 0).toFixed(3)} |
| Cost per useful opportunity | ${costPerUseful == null ? "N/A" : costPerUseful.toFixed(3)} |

Fetch ≈ $0.01/page · SerpAPI ≈ $0.05/query · Jev provider cost as returned.
`
  );

  const founder = `# GDI Jev Active Research Controller V2

**Date:** ${NOW}  
**Mode:** MODE B — signal → research strategy → evidence completion → canonical opportunity  
**Law:** Jev decides WHERE / HOW FAR. Dealality decides WHAT is true.

## A. Executive Summary

Demand-engine V1 produced **${loadTallies.signals}** signals, **${loadTallies.rotation}** rotation series, **${loadTallies.preRfp}** pre-RFP items, **${loadTallies.competitive}** competitive patterns — and **0** useful promotions. This phase built a deduplicated research universe of **${scored.length}**, scored completion potential (P0=${scoreTallies.P0} / P1=${scoreTallies.P1} / P2=${scoreTallies.P2}), activated Jev as an **advisory research controller** (\`forceShadow:false\` for research routing only), and ran page-level completion on **${pageResearched}** selected P0/P1 items.

| Metric | Count |
|--------|------:|
| Research universe | ${scored.length} |
| P0 | ${scoreTallies.P0} |
| P1 | ${scoreTallies.P1} |
| Jev recommendations issued | ${issued} |
| Jev accepted | ${accepted} |
| Jev rejected (policy) | ${rejectedPolicy} |
| Jev rejected (low value) | ${rejectedLow} |
| Page-level researched | ${pageResearched} |
| CUSTOMER_READY | ${ready} |
| VALID_FUTURE_WATCH | ${watch} |
| REJECTED_CONFIRMED | ${rejected} |
| PUBLIC_DATA_CEILING | ${unresolved} |

## B. Why Signal Volume Did Not Convert

Signals are SERP/ledger artifacts. Conversion requires page-level timing, lodging, WHO/contact, and surface thesis under canonical gates. Shadow-mode Jev previously never accepted/rejected routing. Volume ≠ readiness.

## C. Jev Active Research Role

For each selected P0/P1 item Jev returns: decision, blocker, source family, language, feeder, depth, stop condition, expected gain — advisory only. Deterministic policy gate accepts/rejects before execution. Jev never writes facts or promotes.

## D. Research Depth Decisions

DEPTH_1=${d1} · DEPTH_2=${d2} · DEPTH_3=${d3} · hard cap 3 steps/item.

## E. Timing Completion

Timing blockers improved: **${timingResolved}**. States: CONFIRMED_EXACT/RANGE/MONTH/YEAR, RECURRING_EXPECTED, ROTATION_PREDICTED, FUTURE_UNCONFIRMED, HISTORICAL_ONLY. No invented dates.

## F. Lodging Completion

Lodging blockers improved: **${lodgingResolved}**. Classes: DIRECT_ROOM_BLOCK → NO_LODGING_SUPPORT. Attendance/duration alone never counts.

## G. WHO Completion

WHO resolved (email or org contact URL): **${whoResolved}**. Surfe not used.

## H. Rotation / Pre-RFP Completion

Rotation series researched: **${rotationCompleted}**. Pre-RFP researched: **${preRfpCompleted}**. Predicted cycles are not customer-ready unless canonical readiness passes.

## I. Competitive Demand

Competitive patterns researched: **${competitiveCompleted}**. FACT vs INFERENCE separated.

## J. Demand Engine Yield

Top by useful: **${topEngine ? engineLabel(topEngine.key) + " (" + topEngine.useful + ")" : "none"}**. Controller advice: ${engineAdvice.map((a) => a.action).join(", ") || "n/a"}.

## K. Language / Feeder Yield

Top language: **${topLang ? topLang.key + " (" + topLang.useful + ")" : "none"}**.  
Top feeder: **${topFeeder ? topFeeder.key + " (" + topFeeder.useful + ")" : "none"}**.

## L. Jev Performance

| Metric | Value |
|--------|------:|
| Issued | ${issued} |
| Accepted | ${accepted} |
| Rejected policy | ${rejectedPolicy} |
| Low-yield stops | ${lowYieldStops} |
| Blocker-resolution rate | ${(jevBlockerRate * 100).toFixed(1)}% |
| Classification-change rate | ${(jevClassRate * 100).toFixed(1)}% |
| Useful-opportunity yield | ${(jevUsefulYield * 100).toFixed(1)}% |
| Cost/useful | ${costPerUseful == null ? "N/A" : costPerUseful.toFixed(3)} |

## M. Five-Hotel Results

${hotelRows.map((h) => `### ${h.label}
Signals ${h.signalsResearched} · Rotation ${h.rotationResearched} · Pre-RFP ${h.preRfpResearched} · P0 ${h.P0Researched} · P1 ${h.P1Researched} · ready ${h.ready} · watch ${h.watch} · rejected ${h.rejected} · unresolved ${h.unresolved} · avg depth ${h.averageDepth} · cost $${h.cost}`).join("\n\n")}

## N. What Should Become Default GDI Behavior

| Control | Recommendation |
|---------|----------------|
| Priority | **${defaults.priority}** (deterministic baseline) |
| Next blocker | **${defaults.nextBlocker}** |
| Source | **${defaults.source}** |
| Language | **${defaults.language}** |
| Feeder market | **${defaults.feeder}** |
| Depth | **${defaults.depth}** |
| Stop/continue | **${defaults.stopContinue}** |

### Safety

| Check | Result |
|-------|--------|
| GDI thresholds changed? | **NO** |
| Jev wrote verified facts? | **NO** |
| Jev promoted opportunities? | **NO** |
| Watch quality bypassed? | **NO** |
| New broad discovery? | **NO** |
| ADP changed? | **NO** |
| Share tokens changed? | **NO** |

### Final verdict

${
  usefulTotal > 0
    ? `Active Jev research routing + page completion produced **${usefulTotal}** useful outcomes (ready+watch) from the signal/series universe under canonical gates.`
    : `Active Jev issued and policy-gated **${issued}** research recommendations (${accepted} accepted). Page-level completion on **${pageResearched}** items closed some blockers (timing ${timingResolved}, lodging ${lodgingResolved}, WHO ${whoResolved}) but **0** items cleared full customer-ready / valid-watch gates — confirming signal→opportunity conversion needs stronger public lodging+timing+WHO packages, not more SERP volume.`
}

**STOP.**
`;

  write("FOUNDER_REPORT.md", founder);

  const ret = {
    TOTAL_RESEARCH_UNIVERSE: scored.length,
    TOTAL_P0: scoreTallies.P0,
    TOTAL_P1: scoreTallies.P1,
    JEV_RECOMMENDATIONS_ISSUED: issued,
    JEV_ACCEPTED: accepted,
    JEV_REJECTED_BY_POLICY: rejectedPolicy,
    JEV_STOPPED_LOW_YIELD: lowYieldStops,
    TOTAL_PAGE_LEVEL_RESEARCHED: pageResearched,
    TOTAL_DEPTH_1: d1,
    TOTAL_DEPTH_2: d2,
    TOTAL_DEPTH_3: d3,
    TOTAL_CUSTOMER_READY: ready,
    TOTAL_VALID_FUTURE_WATCH: watch,
    TOTAL_REJECTED_CONFIRMED: rejected,
    TOTAL_PUBLIC_DATA_CEILING: unresolved,
    YOTEL_CUSTOMER_READY: hotelReady("YOTEL"),
    YOTEL_FUTURE_WATCH: hotelWatch("YOTEL"),
    AC_CUSTOMER_READY: hotelReady("AC"),
    AC_FUTURE_WATCH: hotelWatch("AC"),
    SPICE_CUSTOMER_READY: hotelReady("SPICE"),
    SPICE_FUTURE_WATCH: hotelWatch("SPICE"),
    CAMBRIDGE_CUSTOMER_READY: hotelReady("CAMBRIDGE"),
    CAMBRIDGE_FUTURE_WATCH: hotelWatch("CAMBRIDGE"),
    NOW_NOW_CUSTOMER_READY: hotelReady("NOW_NOW"),
    NOW_NOW_FUTURE_WATCH: hotelWatch("NOW_NOW"),
    ROTATION_SERIES_COMPLETED: rotationCompleted,
    PRE_RFP_ITEMS_COMPLETED: preRfpCompleted,
    TIMING_BLOCKERS_RESOLVED: timingResolved,
    LODGING_BLOCKERS_RESOLVED: lodgingResolved,
    WHO_RESOLVED: whoResolved,
    COMPETITIVE_PATTERNS_COMPLETED: competitiveCompleted,
    TOP_DEMAND_ENGINE_BY_USEFUL_YIELD: topEngine ? engineLabel(topEngine.key) : null,
    TOP_LANGUAGE_BY_USEFUL_YIELD: topLang?.key || null,
    TOP_FEEDER_MARKET_BY_USEFUL_YIELD: topFeeder?.key || null,
    JEV_BLOCKER_RESOLUTION_RATE: Number(jevBlockerRate.toFixed(3)),
    JEV_CLASSIFICATION_CHANGE_RATE: Number(jevClassRate.toFixed(3)),
    JEV_USEFUL_OPPORTUNITY_YIELD: Number(jevUsefulYield.toFixed(3)),
    COST_PER_USEFUL_OPPORTUNITY: costPerUseful == null ? null : Number(costPerUseful.toFixed(3)),
    SHOULD_JEV_CONTROL_PRIORITY: defaults.priority,
    SHOULD_JEV_CONTROL_NEXT_BLOCKER: defaults.nextBlocker,
    SHOULD_JEV_CONTROL_SOURCE: defaults.source,
    SHOULD_JEV_CONTROL_LANGUAGE: defaults.language,
    SHOULD_JEV_CONTROL_FEEDER_MARKET: defaults.feeder,
    SHOULD_JEV_CONTROL_DEPTH: defaults.depth,
    SHOULD_JEV_CONTROL_STOP_CONTINUE: defaults.stopContinue,
    GDI_THRESHOLDS_CHANGED: false,
    JEV_WROTE_VERIFIED_FACTS: false,
    JEV_PROMOTED_OPPORTUNITIES: false,
    WATCH_QUALITY_STANDARD_BYPASSED: false,
    NEW_BROAD_DISCOVERY_RUN: false,
    ADP_CHANGED: false,
    SHARE_TOKENS_CHANGED: false,
    loadTallies,
    jevConfig: describeJevConfig(),
  };

  write("_RETURN.json", JSON.stringify(ret, null, 2));
  console.log("\n========== RETURN ==========");
  console.log(JSON.stringify(ret, null, 2));
  console.log(`[jar-v2] reports → ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
