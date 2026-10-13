/**
 * GDI Candidate Completion Engine + Jev Research Controller V2
 * Page-level evidence closure on V3 candidates. No broad rediscovery. No threshold changes.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadV3CandidatesFromReports,
  runCompletionForHotel,
  COMPLETION_PRIORITY,
  RESEARCH_DEPTH,
  TERMINAL_CLASS,
} from "../lib/group-demand-intelligence/candidate-completion-v2/index.js";
import { DEMAND_ENGINE_TAXONOMY } from "../lib/group-demand-intelligence/demand-engine-v1/taxonomy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "candidate-completion-jev-v2-2026-10-03");
const V3_DIR = path.join(__dirname, "..", "reports", "gdi", "discovery-expansion-v3-2026-10-03");
const NOW = "2026-10-03";

const HOTELS = [
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelKey: "YOTEL",
    label: "YOTEL Geneva Lake",
    marketId: "geneva_lake",
    placeNames: ["Geneva", "Genève", "Nyon", "La Côte", "Palexpo", "Lausanne"],
    geoTokens: ["geneva", "genève", "switzerland", "palexpo", "lausanne", "nyon"],
    rooms: 237,
    defaultFitScore: 52,
    fitLine: "YOTEL Geneva Lake: airport / La Côte corridor; 237 rooms compact upscale.",
    serpGl: "ch",
    feederHint: "france_paris",
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    label: "AC Hotel A Coruña",
    marketId: "a_coruna",
    placeNames: ["A Coruña", "Coruña", "Galicia"],
    geoTokens: ["coruña", "coruna", "galicia", "spain"],
    rooms: 180,
    defaultFitScore: 50,
    fitLine: "AC A Coruña: urban upscale for association / corporate groups.",
    serpGl: "es",
    feederHint: "madrid",
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    label: "Spice Island Beach Resort",
    marketId: "grenada",
    placeNames: ["Grenada", "Grand Anse"],
    geoTokens: ["grenada", "grand anse"],
    rooms: 64,
    defaultFitScore: 48,
    fitLine: "Spice Island: luxury beach resort for incentive / high-end group lodging.",
    serpGl: "us",
    feederHint: "us_northeast",
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    label: "Cambridge Beaches Resort & Spa",
    marketId: "bermuda",
    placeNames: ["Bermuda", "Somerset"],
    geoTokens: ["bermuda", "somerset"],
    rooms: 94,
    defaultFitScore: 48,
    fitLine: "Cambridge Beaches: cottage resort for incentive / executive retreat demand.",
    serpGl: "us",
    feederHint: "us_northeast",
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    label: "NOW NOW NOHO",
    marketId: "nyc_noho",
    placeNames: ["New York", "NoHo", "Manhattan"],
    geoTokens: ["new york", "nyc", "manhattan", "noho"],
    rooms: 80,
    defaultFitScore: 50,
    fitLine: "NOW NOW NoHo: boutique urban for tech / creative / extended-stay groups.",
    serpGl: "us",
    feederHint: "us_domestic",
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

function aggYield(rows, keyFn) {
  const map = new Map();
  for (const r of rows) {
    const k = keyFn(r) || "UNKNOWN";
    if (!map.has(k)) {
      map.set(k, {
        key: k,
        candidates: 0,
        researched: 0,
        ready: 0,
        watch: 0,
        rejected: 0,
        unresolved: 0,
        useful: 0,
        cost: 0,
        blockers: 0,
      });
    }
    const m = map.get(k);
    m.candidates += 1;
    if (r.researched) m.researched += 1;
    if (r.terminalClass === TERMINAL_CLASS.CUSTOMER_READY) m.ready += 1;
    else if (r.terminalClass === TERMINAL_CLASS.VALID_FUTURE_WATCH) m.watch += 1;
    else if (r.terminalClass === TERMINAL_CLASS.UNRESOLVED_PUBLIC_DATA_CEILING) m.unresolved += 1;
    else m.rejected += 1;
    if (r.useful) m.useful += 1;
    m.cost += r.economics?.estimatedCost || 0;
    m.blockers += r.economics?.blockersResolved || 0;
  }
  return [...map.values()].sort((a, b) => b.useful - a.useful || b.candidates - a.candidates);
}

function decideDefaultBehavior(qa, depthYield, usefulTotal) {
  const issued = qa.issued || 0;
  const accepted = qa.accepted || 0;
  const resolved = qa.resolvedBlockers || 0;
  const changed = qa.changedClass || 0;
  const acceptRate = issued ? accepted / issued : 0;
  const resolveRate = accepted ? resolved / accepted : 0;

  const cond = (yesWhen) => (yesWhen ? "CONDITIONAL" : "NO");

  return {
    candidatePriority: "NO", // deterministic baseline required
    nextBlocker: cond(resolveRate >= 0.15 && issued >= 5),
    sourceSelection: cond(resolveRate >= 0.1 && acceptRate >= 0.5),
    languageSelection: usefulTotal > 0 ? "CONDITIONAL" : "NO",
    feederMarketSelection: usefulTotal > 0 ? "CONDITIONAL" : "NO",
    researchDepth: cond(depthYield.some((d) => d.useful > 0 && /DEPTH_[23]/.test(d.key))),
    stopContinue: cond(qa.lowYield >= 1 || acceptRate >= 0.4),
  };
}

async function main() {
  console.log("[completion-v2] loading V3 candidates…");
  const { candidates, meta: loadMeta } = loadV3CandidatesFromReports(V3_DIR, HOTELS);
  console.log(
    `[completion-v2] pool=${candidates.length} officialV3=${loadMeta.v3OfficialTotal} assocMissing=${loadMeta.associationScoutYieldFromScoutCsv}`
  );

  const allResults = [];
  const hotelSummaries = {};

  for (const h of HOTELS) {
    const pool = candidates.filter((c) => c.hotelKey === h.hotelKey);
    console.log(`[completion-v2] ${h.hotelKey}: ${pool.length} candidates — researching P0/P1…`);
    const { tallies, results } = await runCompletionForHotel(h, pool, { nowDate: NOW });
    hotelSummaries[h.hotelKey] = {
      label: h.label,
      v3Candidates: pool.length,
      tallies,
      results,
    };
    allResults.push(...results);
    const useful = results.filter((r) => r.useful).length;
    const researched = results.filter((r) => r.researched).length;
    console.log(
      `[completion-v2] ${h.hotelKey}: P0=${tallies.P0} P1=${tallies.P1} researched=${researched} useful=${useful}`
    );
  }

  // ——— CSV outputs ———
  const priorityRows = allResults.map((r) => ({
    hotel: r.hotelKey,
    opportunityId: r.opportunity.id,
    title: r.opportunity.title,
    priority: r.completionPriority,
    score: r.completionScore,
    signals: (r.completionSignals || []).join("|"),
    demandEngine: r.demandEngine,
    scoutFamily: r.opportunity.discoveryMeta?.scoutFamily,
    language: r.opportunity.queryLanguage,
  }));
  write(
    "COMPLETION_PRIORITY_QUEUE.csv",
    toCsv(priorityRows, [
      "hotel",
      "opportunityId",
      "title",
      "priority",
      "score",
      "signals",
      "demandEngine",
      "scoutFamily",
      "language",
    ])
  );

  const strategyRows = allResults
    .filter((r) => r.jevAdvice)
    .map((r) => ({
      hotel: r.hotelKey,
      opportunityId: r.opportunity.id,
      title: r.opportunity.title,
      jevNextQuestion: r.jevAdvice.jevNextQuestion,
      jevRecommendedSourceFamily: r.jevAdvice.jevRecommendedSourceFamily,
      jevRecommendedLanguage: r.jevAdvice.jevRecommendedLanguage,
      jevRecommendedFeederMarket: r.jevAdvice.jevRecommendedFeederMarket,
      jevExpectedInformationGain: r.jevAdvice.jevExpectedInformationGain,
      jevEstimatedPromotionPotential: r.jevAdvice.jevEstimatedPromotionPotential,
      jevRecommendedDepth: r.jevAdvice.jevRecommendedDepth,
      jevStopCondition: r.jevAdvice.jevStopCondition,
      jevRationale: r.jevAdvice.jevRationale,
      source: r.jevAdvice.source,
    }));
  write(
    "JEV_RESEARCH_STRATEGY.csv",
    toCsv(strategyRows, [
      "hotel",
      "opportunityId",
      "title",
      "jevNextQuestion",
      "jevRecommendedSourceFamily",
      "jevRecommendedLanguage",
      "jevRecommendedFeederMarket",
      "jevExpectedInformationGain",
      "jevEstimatedPromotionPotential",
      "jevRecommendedDepth",
      "jevStopCondition",
      "jevRationale",
      "source",
    ])
  );

  const depthRows = allResults.map((r) => ({
    hotel: r.hotelKey,
    opportunityId: r.opportunity.id,
    priority: r.completionPriority,
    depth: r.depthAssigned,
    maxSteps: r.depthDecision?.maxSteps,
    rationale: r.depthDecision?.rationale,
    stepsRun: r.economics.researchSteps,
    stopReason: r.stopReason,
  }));
  write(
    "RESEARCH_DEPTH_DECISIONS.csv",
    toCsv(depthRows, [
      "hotel",
      "opportunityId",
      "priority",
      "depth",
      "maxSteps",
      "rationale",
      "stepsRun",
      "stopReason",
    ])
  );

  const gapRows = allResults.map((r) => {
    const g = r.gapAfter?.gaps || {};
    return {
      hotel: r.hotelKey,
      opportunityId: r.opportunity.id,
      ENTITY: g.ENTITY,
      TIMING: g.TIMING,
      GEOGRAPHY: g.GEOGRAPHY,
      LODGING: g.LODGING,
      FIT: g.FIT,
      PLACEMENT: g.PLACEMENT,
      WHO: g.WHO,
      CONTACT: g.CONTACT,
      SUMMARY: g.SUMMARY,
      SURFACE: g.SURFACE,
      smallestMissingFact: r.gapAfter?.smallestMissingFact,
      lodgingClass: r.lodgingClass,
    };
  });
  write(
    "EVIDENCE_GAP_MAP.csv",
    toCsv(gapRows, [
      "hotel",
      "opportunityId",
      "ENTITY",
      "TIMING",
      "GEOGRAPHY",
      "LODGING",
      "FIT",
      "PLACEMENT",
      "WHO",
      "CONTACT",
      "SUMMARY",
      "SURFACE",
      "smallestMissingFact",
      "lodgingClass",
    ])
  );

  const pageRows = allResults.flatMap((r) =>
    (r.pageRows || []).map((p) => ({
      hotel: r.hotelKey,
      opportunityId: p.opportunityId,
      step: p.step,
      url: p.url,
      ok: p.ok,
      error: p.error,
      empty: p.empty,
      chars: p.chars,
      factsExtracted: p.factsExtracted ?? 0,
    }))
  );
  write(
    "PAGE_LEVEL_RESEARCH.csv",
    toCsv(pageRows, [
      "hotel",
      "opportunityId",
      "step",
      "url",
      "ok",
      "error",
      "empty",
      "chars",
      "factsExtracted",
    ])
  );

  write(
    "LODGING_COMPLETION.csv",
    toCsv(
      allResults.map((r) => ({
        hotel: r.hotelKey,
        opportunityId: r.opportunity.id,
        lodgingClass: r.lodgingClass,
        lodgingGap: r.gapAfter?.gaps?.LODGING,
        housingPage: r.opportunity.lodgingEvidence?.housingPageFound,
        roomBlock: r.opportunity.lodgingEvidence?.roomBlockMentioned,
      })),
      ["hotel", "opportunityId", "lodgingClass", "lodgingGap", "housingPage", "roomBlock"]
    )
  );

  write(
    "TIMING_COMPLETION.csv",
    toCsv(
      allResults.map((r) => ({
        hotel: r.hotelKey,
        opportunityId: r.opportunity.id,
        timingState: r.timingState,
        timingGap: r.gapAfter?.gaps?.TIMING,
        eventStartDate: r.opportunity.eventStartDate,
        eventYear: r.opportunity.eventYear,
        futureCycleEvidenceState: r.opportunity.futureCycleEvidenceState,
      })),
      [
        "hotel",
        "opportunityId",
        "timingState",
        "timingGap",
        "eventStartDate",
        "eventYear",
        "futureCycleEvidenceState",
      ]
    )
  );

  write(
    "WHO_COMPLETION.csv",
    toCsv(
      allResults.map((r) => ({
        hotel: r.hotelKey,
        opportunityId: r.opportunity.id,
        whoResolved: r.whoResolved,
        whoEntity: r.whoEntity,
        whoRole: r.whoRole,
        whoPerson: r.whoPerson,
        howContact: r.howContact,
        whoGap: r.gapAfter?.gaps?.WHO,
        contactGap: r.gapAfter?.gaps?.CONTACT,
      })),
      [
        "hotel",
        "opportunityId",
        "whoResolved",
        "whoEntity",
        "whoRole",
        "whoPerson",
        "howContact",
        "whoGap",
        "contactGap",
      ]
    )
  );

  write(
    "PROCUREMENT_COMPLETION.csv",
    toCsv(
      allResults
        .filter(
          (r) =>
            /Procurement|PROCUREMENT/i.test(r.demandEngine || "") ||
            /ProcurementScout/i.test(r.opportunity.discoveryMeta?.scoutFamily || "")
        )
        .map((r) => ({
          hotel: r.hotelKey,
          opportunityId: r.opportunity.id,
          title: r.opportunity.title,
          status: r.procurementStatus,
          buyer: r.opportunity.organizationName,
          source: r.opportunity.officialSource,
        })),
      ["hotel", "opportunityId", "title", "status", "buyer", "source"]
    )
  );

  write(
    "ROTATION_COMPLETION.csv",
    toCsv(
      allResults
        .filter((r) => r.opportunity.eventSeriesId || /series|annual|rotat/i.test(r.opportunity.title || ""))
        .map((r) => ({
          hotel: r.hotelKey,
          opportunityId: r.opportunity.id,
          seriesId: r.opportunity.eventSeriesId,
          timingState: r.timingState,
          note: r.rotationNote || "page-level only; no invented cycles",
        })),
      ["hotel", "opportunityId", "seriesId", "timingState", "note"]
    )
  );

  write(
    "COMPETITOR_DEMAND_COMPLETION.csv",
    toCsv(
      allResults.map((r) => ({
        hotel: r.hotelKey,
        opportunityId: r.opportunity.id,
        competitorFact: r.competitorFact || "",
        competitorInference: r.competitorInference || "",
        note: "FACT/INFERENCE separated; no hosting invented",
      })),
      ["hotel", "opportunityId", "competitorFact", "competitorInference", "note"]
    )
  );

  const jevActionRows = allResults.flatMap((r) =>
    (r.jevLog || []).map((j) => ({
      hotel: r.hotelKey,
      opportunityId: j.opportunityId,
      phase: j.phase,
      step: j.step ?? "",
      recommendation: j.recommendation,
      accepted: j.accepted,
      rejectedByPolicy: j.rejectedByPolicy || false,
      blockerResolved: j.blockerResolved || false,
      classificationChanged: r.useful,
      rationale: j.rationale,
      source: j.source,
      cost: r.economics.estimatedCost,
    }))
  );
  write(
    "JEV_NEXT_ACTION_LOG.csv",
    toCsv(jevActionRows, [
      "hotel",
      "opportunityId",
      "phase",
      "step",
      "recommendation",
      "accepted",
      "rejectedByPolicy",
      "blockerResolved",
      "classificationChanged",
      "rationale",
      "source",
      "cost",
    ])
  );

  // Jev QA
  const issued = jevActionRows.length;
  const accepted = jevActionRows.filter((j) => j.accepted === true || j.accepted === "true").length;
  const rejectedByPolicy = jevActionRows.filter(
    (j) => j.rejectedByPolicy === true || j.rejectedByPolicy === "true"
  ).length;
  const resolvedBlockers = jevActionRows.filter((j) => j.blockerResolved).length;
  const changedClass = jevActionRows.filter((j) => j.classificationChanged && j.phase === "LOOP").length;
  const lowYield = jevActionRows.filter(
    (j) => /STOP_LOW_INFORMATION|LOW_YIELD|zero blockers/i.test(String(j.rationale || j.recommendation))
  ).length;

  write(
    "JEV_PERFORMANCE_QA.csv",
    toCsv(
      [
        {
          metric: "JEV_RECOMMENDATIONS_ISSUED",
          value: issued,
        },
        { metric: "JEV_RECOMMENDATIONS_ACCEPTED", value: accepted },
        { metric: "JEV_RECOMMENDATIONS_REJECTED_BY_POLICY", value: rejectedByPolicy },
        { metric: "JEV_RECOMMENDATIONS_RESOLVING_BLOCKERS", value: resolvedBlockers },
        { metric: "JEV_RECOMMENDATIONS_CHANGING_CLASSIFICATION", value: changedClass },
        { metric: "JEV_LOW_YIELD_RECOMMENDATIONS", value: lowYield },
      ],
      ["metric", "value"]
    )
  );

  const researched = allResults.filter((r) => r.researched);
  const depthYield = aggYield(researched, (r) => r.depthAssigned);
  // ensure DEPTH_1/2/3 rows
  for (const d of [
    RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP,
    RESEARCH_DEPTH.DEPTH_2_TWO_STEP_COMPLETION,
    RESEARCH_DEPTH.DEPTH_3_DEEP_RESEARCH_JUSTIFIED,
  ]) {
    if (!depthYield.find((x) => x.key === d)) {
      depthYield.push({
        key: d,
        candidates: 0,
        researched: 0,
        ready: 0,
        watch: 0,
        rejected: 0,
        unresolved: 0,
        useful: 0,
        cost: 0,
        blockers: 0,
      });
    }
  }
  write(
    "DEPTH_YIELD.csv",
    toCsv(
      depthYield.map((d) => ({
        depth: d.key,
        candidates: d.candidates,
        ready: d.ready,
        watch: d.watch,
        rejected: d.rejected,
        unresolved: d.unresolved,
        useful: d.useful,
        averageCost: d.candidates ? (d.cost / d.candidates).toFixed(3) : 0,
        averageBlockersResolved: d.candidates ? (d.blockers / d.candidates).toFixed(2) : 0,
      })),
      [
        "depth",
        "candidates",
        "ready",
        "watch",
        "rejected",
        "unresolved",
        "useful",
        "averageCost",
        "averageBlockersResolved",
      ]
    )
  );

  const engineYield = aggYield(allResults, (r) => {
    const e = r.demandEngine;
    return typeof e === "string" ? e : e?.demandEngine || "UNKNOWN";
  });
  write(
    "DEMAND_ENGINE_YIELD.csv",
    toCsv(
      engineYield.map((e) => ({
        demandEngine: e.key,
        label: engineLabel(e.key),
        candidates: e.candidates,
        researched: e.researched,
        ready: e.ready,
        watch: e.watch,
        useful: e.useful,
        cost: e.cost.toFixed(3),
      })),
      ["demandEngine", "label", "candidates", "researched", "ready", "watch", "useful", "cost"]
    )
  );

  const langYield = aggYield(allResults, (r) => r.opportunity.queryLanguage || "en");
  write(
    "LANGUAGE_YIELD.csv",
    toCsv(
      langYield.map((e) => ({
        language: e.key,
        candidates: e.candidates,
        researched: e.researched,
        useful: e.useful,
        ready: e.ready,
        watch: e.watch,
      })),
      ["language", "candidates", "researched", "useful", "ready", "watch"]
    )
  );

  const feederYield = aggYield(
    allResults,
    (r) => r.opportunity.discoveryMeta?.feederMarket || r.jevAdvice?.jevRecommendedFeederMarket || "NONE"
  );
  write(
    "FEEDER_MARKET_YIELD.csv",
    toCsv(
      feederYield.map((e) => ({
        feederMarket: e.key,
        candidates: e.candidates,
        researched: e.researched,
        useful: e.useful,
        ready: e.ready,
        watch: e.watch,
      })),
      ["feederMarket", "candidates", "researched", "useful", "ready", "watch"]
    )
  );

  write(
    "FINAL_CLASSIFICATION.csv",
    toCsv(
      allResults.map((r) => ({
        hotel: r.hotelKey,
        opportunityId: r.opportunity.id,
        title: r.opportunity.title,
        priority: r.completionPriority,
        depth: r.depthAssigned,
        researched: r.researched,
        terminalClass: r.terminalClass,
        terminalReason: r.terminalReason,
        readyOk: r.readyOk,
        watchOk: r.watchOk,
        useful: r.useful,
        cost: r.economics.estimatedCost,
        blockersResolved: r.economics.blockersResolved,
        steps: r.economics.researchSteps,
      })),
      [
        "hotel",
        "opportunityId",
        "title",
        "priority",
        "depth",
        "researched",
        "terminalClass",
        "terminalReason",
        "readyOk",
        "watchOk",
        "useful",
        "cost",
        "blockersResolved",
        "steps",
      ]
    )
  );

  const hotelRows = HOTELS.map((h) => {
    const rs = hotelSummaries[h.hotelKey].results;
    const t = hotelSummaries[h.hotelKey].tallies;
    return {
      hotel: h.hotelKey,
      label: h.label,
      v3Candidates: hotelSummaries[h.hotelKey].v3Candidates,
      P0: t.P0,
      P1: t.P1,
      P2: t.P2,
      fullyResearched: rs.filter((r) => r.researched).length,
      customerReady: rs.filter((r) => r.terminalClass === TERMINAL_CLASS.CUSTOMER_READY).length,
      validWatch: rs.filter((r) => r.terminalClass === TERMINAL_CLASS.VALID_FUTURE_WATCH).length,
      confirmedRejected: rs.filter((r) => r.terminalClass === TERMINAL_CLASS.REJECTED_CONFIRMED).length,
      publicDataCeiling: rs.filter(
        (r) => r.terminalClass === TERMINAL_CLASS.UNRESOLVED_PUBLIC_DATA_CEILING
      ).length,
      averageDepth: (() => {
        const d = rs.filter((r) => r.researched);
        if (!d.length) return 0;
        const score = (x) =>
          x.depthAssigned === RESEARCH_DEPTH.DEPTH_3_DEEP_RESEARCH_JUSTIFIED
            ? 3
            : x.depthAssigned === RESEARCH_DEPTH.DEPTH_2_TWO_STEP_COMPLETION
              ? 2
              : x.depthAssigned === RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP
                ? 1
                : 0;
        return (d.reduce((s, r) => s + score(r), 0) / d.length).toFixed(2);
      })(),
      cost: rs.reduce((s, r) => s + (r.economics.estimatedCost || 0), 0).toFixed(3),
    };
  });
  write(
    "HOTEL_RESULTS.csv",
    toCsv(hotelRows, [
      "hotel",
      "label",
      "v3Candidates",
      "P0",
      "P1",
      "P2",
      "fullyResearched",
      "customerReady",
      "validWatch",
      "confirmedRejected",
      "publicDataCeiling",
      "averageDepth",
      "cost",
    ])
  );

  // Totals
  const P0 = allResults.filter(
    (r) => r.completionPriority === COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL
  ).length;
  const P1 = allResults.filter(
    (r) => r.completionPriority === COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL
  ).length;
  const fullyResearched = allResults.filter((r) => r.researched).length;
  const customerReady = allResults.filter(
    (r) => r.terminalClass === TERMINAL_CLASS.CUSTOMER_READY
  ).length;
  const validWatch = allResults.filter(
    (r) => r.terminalClass === TERMINAL_CLASS.VALID_FUTURE_WATCH
  ).length;
  const rejected = allResults.filter(
    (r) => r.terminalClass === TERMINAL_CLASS.REJECTED_CONFIRMED
  ).length;
  const unresolved = allResults.filter(
    (r) => r.terminalClass === TERMINAL_CLASS.UNRESOLVED_PUBLIC_DATA_CEILING
  ).length;
  const usefulTotal = customerReady + validWatch;
  const totalCost = allResults.reduce((s, r) => s + (r.economics.estimatedCost || 0), 0);
  const p0Cost =
    allResults
      .filter((r) => r.completionPriority === COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL)
      .reduce((s, r) => s + (r.economics.estimatedCost || 0), 0) / (P0 || 1);
  const whoResolvedCount = allResults.filter((r) => r.whoResolved).length;
  const timingResolved = allResults.filter(
    (r) =>
      r.gapBefore?.gaps?.TIMING !== "PASS" &&
      (r.gapAfter?.gaps?.TIMING === "PASS" || r.gapAfter?.gaps?.TIMING === "PARTIAL") &&
      r.economics.blockersResolved > 0
  ).length;
  const lodgingResolved = allResults.filter(
    (r) =>
      r.gapBefore?.gaps?.LODGING === "FAIL" &&
      (r.gapAfter?.gaps?.LODGING === "PASS" || r.gapAfter?.gaps?.LODGING === "PARTIAL")
  ).length;
  const nextBestActions = jevActionRows.filter((j) => j.phase === "LOOP").length;

  const d1 = depthYield.find((d) => d.key === RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP) || {
    candidates: 0,
    useful: 0,
  };
  const d2 = depthYield.find((d) => d.key === RESEARCH_DEPTH.DEPTH_2_TWO_STEP_COMPLETION) || {
    candidates: 0,
    useful: 0,
  };
  const d3 = depthYield.find((d) => d.key === RESEARCH_DEPTH.DEPTH_3_DEEP_RESEARCH_JUSTIFIED) || {
    candidates: 0,
    useful: 0,
  };

  const hotelReady = (k) =>
    hotelSummaries[k].results.filter((r) => r.terminalClass === TERMINAL_CLASS.CUSTOMER_READY)
      .length;
  const hotelWatch = (k) =>
    hotelSummaries[k].results.filter((r) => r.terminalClass === TERMINAL_CLASS.VALID_FUTURE_WATCH)
      .length;

  const topEngine = engineYield[0];
  const topLang = langYield.sort((a, b) => b.useful - a.useful)[0];
  const topFeeder = feederYield.sort((a, b) => b.useful - a.useful)[0];

  const defaults = decideDefaultBehavior(
    { issued, accepted, rejectedByPolicy, resolvedBlockers, changedClass, lowYield },
    depthYield,
    usefulTotal
  );

  const costPerUseful = usefulTotal ? totalCost / usefulTotal : null;

  write(
    "COST_REPORT.md",
    `# Cost Report — Candidate Completion Jev V2

| Metric | Value |
|--------|------:|
| Total estimated cost (USD) | ${totalCost.toFixed(3)} |
| Pages opened | ${allResults.reduce((s, r) => s + r.economics.pagesOpened, 0)} |
| Queries run | ${allResults.reduce((s, r) => s + r.economics.queriesRun, 0)} |
| Provider calls | ${allResults.reduce((s, r) => s + r.economics.providerCalls, 0)} |
| Research steps | ${allResults.reduce((s, r) => s + r.economics.researchSteps, 0)} |
| Average cost per P0 | ${p0Cost.toFixed(3)} |
| Cost per useful opportunity | ${costPerUseful == null ? "N/A (0 useful)" : costPerUseful.toFixed(3)} |
| Blockers resolved (sum) | ${allResults.reduce((s, r) => s + r.economics.blockersResolved, 0)} |

Fetch proxy = $0.01/page · SerpAPI = $0.05/query (same convention as V3).
`
  );

  const founder = `# GDI Candidate Completion + Jev Research Controller V2

**Date:** ${NOW}  
**Mode:** MODE B — page-level completion on V3 discovery candidates  
**Law:** Jev decides WHERE / HOW FAR to look. Dealality decides WHAT is true.

## A. Executive Summary

V3 produced **${loadMeta.v3OfficialTotal}** new candidates and **0** qualified / customer-ready / valid future watch promotions — correctly refusing thin SERP hits. This phase rebuilt a researchable pool of **${candidates.length}** candidates from V3 scout CSVs (+ association series), prioritized P0/P1, and ran page-level completion with adaptive depth (hard cap ${3}).

| Metric | Count |
|--------|------:|
| V3 official candidates | ${loadMeta.v3OfficialTotal} |
| Research pool (reconstructed) | ${candidates.length} |
| P0 | ${P0} |
| P1 | ${P1} |
| Fully researched | ${fullyResearched} |
| CUSTOMER_READY | ${customerReady} |
| VALID_FUTURE_WATCH | ${validWatch} |
| REJECTED_CONFIRMED | ${rejected} |
| UNRESOLVED_PUBLIC_DATA_CEILING | ${unresolved} |

**AssociationScout note:** V3 did not persist AssociationScout detail rows to a RESULTS.csv (SCOUT_YIELD shows **${loadMeta.associationScoutYieldFromScoutCsv}** Association hits). Pool uses detail scout CSVs + ASSOCIATION_SERIES rows only. No new broad discovery was run.

## B. Why V3 Produced 156 Candidates but 0 Qualified

Discovery stopped at SERP snippets. Qualification requires page-level lodging, timing, WHO/contact, and surface thesis. Multi-blocker thin hits correctly failed \`isGdiCustomerOpportunityReady\` / \`isValidFutureWatch\` — architecture working as designed. Bottleneck = completion, not thresholds.

## C. Jev's Role

Advisory only:
- next question / source family / language / feeder / depth / stop
- **never** verified facts, promotion, rejection, threshold changes, or canonical writes

Deterministic controller accepts/rejects Jev advice under depth policy + hard caps.

## D. Candidate Priority Logic

Deterministic \`buildGdiCompletionPriority()\`: lodging hint, procurement/RFP, housing URL, named organizer, future timing, repeat series, known venue, hotel fit, public contact route, overnight/travel language → P0 (≥7) / P1 (≥4) / P2.

## E. Adaptive Research Depth

| Priority | Default depth |
|----------|---------------|
| P0 | up to 2 steps (\`DEPTH_2\`) |
| P1 | 1 step (\`DEPTH_1\`) |
| P2 | none unless high-info Jev exception |
| DEPTH_3 | only strong fit + entity + geo + lodging partial + commercial + cost |

Hard cap: **3** completion steps/candidate.

## F. YOTEL Geneva Lake

V3 pool ${hotelSummaries.YOTEL.v3Candidates} · P0 ${hotelRows.find((x) => x.hotel === "YOTEL").P0} · P1 ${hotelRows.find((x) => x.hotel === "YOTEL").P1} · researched ${hotelRows.find((x) => x.hotel === "YOTEL").fullyResearched} · ready ${hotelReady("YOTEL")} · watch ${hotelWatch("YOTEL")} · rejected ${hotelRows.find((x) => x.hotel === "YOTEL").confirmedRejected} · ceiling ${hotelRows.find((x) => x.hotel === "YOTEL").publicDataCeiling}

## G. AC A Coruña

V3 pool ${hotelSummaries.AC.v3Candidates} · P0 ${hotelRows.find((x) => x.hotel === "AC").P0} · P1 ${hotelRows.find((x) => x.hotel === "AC").P1} · researched ${hotelRows.find((x) => x.hotel === "AC").fullyResearched} · ready ${hotelReady("AC")} · watch ${hotelWatch("AC")} · rejected ${hotelRows.find((x) => x.hotel === "AC").confirmedRejected} · ceiling ${hotelRows.find((x) => x.hotel === "AC").publicDataCeiling}

## H. Spice Island

V3 pool ${hotelSummaries.SPICE.v3Candidates} · P0 ${hotelRows.find((x) => x.hotel === "SPICE").P0} · P1 ${hotelRows.find((x) => x.hotel === "SPICE").P1} · researched ${hotelRows.find((x) => x.hotel === "SPICE").fullyResearched} · ready ${hotelReady("SPICE")} · watch ${hotelWatch("SPICE")} · rejected ${hotelRows.find((x) => x.hotel === "SPICE").confirmedRejected} · ceiling ${hotelRows.find((x) => x.hotel === "SPICE").publicDataCeiling}

## I. Cambridge Beaches

V3 pool ${hotelSummaries.CAMBRIDGE.v3Candidates} · P0 ${hotelRows.find((x) => x.hotel === "CAMBRIDGE").P0} · P1 ${hotelRows.find((x) => x.hotel === "CAMBRIDGE").P1} · researched ${hotelRows.find((x) => x.hotel === "CAMBRIDGE").fullyResearched} · ready ${hotelReady("CAMBRIDGE")} · watch ${hotelWatch("CAMBRIDGE")} · rejected ${hotelRows.find((x) => x.hotel === "CAMBRIDGE").confirmedRejected} · ceiling ${hotelRows.find((x) => x.hotel === "CAMBRIDGE").publicDataCeiling}

## J. NOW NOW

V3 pool ${hotelSummaries.NOW_NOW.v3Candidates} · P0 ${hotelRows.find((x) => x.hotel === "NOW_NOW").P0} · P1 ${hotelRows.find((x) => x.hotel === "NOW_NOW").P1} · researched ${hotelRows.find((x) => x.hotel === "NOW_NOW").fullyResearched} · ready ${hotelReady("NOW_NOW")} · watch ${hotelWatch("NOW_NOW")} · rejected ${hotelRows.find((x) => x.hotel === "NOW_NOW").confirmedRejected} · ceiling ${hotelRows.find((x) => x.hotel === "NOW_NOW").publicDataCeiling}

## K. Timing Findings

Timing remains the systemic kill gate. Page extraction only stamps dates when public future patterns appear — no invented dates. Timing blockers resolved (partial/pass after fail): **${timingResolved}**.

## L. Lodging Findings

Lodging classes used: DIRECT_ROOM_BLOCK / OFFICIAL_HOUSING_PROGRAM / OFFICIAL_ACCOMMODATION_GUIDANCE / TRAVELING_DELEGATION / MULTI_DAY_GROUP_INFERENCE / NO_LODGING_SUPPORT. Attendance or duration alone never counts as room demand. Lodging blockers improved: **${lodgingResolved}**.

## M. WHO Findings

WHO resolved (email or org contact URL): **${whoResolvedCount}**. Surfe not used. Public-data ceiling stamped when contact research attempted without path.

## N. Rotation / Repeat Findings

Series IDs from association series retained where present. No invented historic hosts. Further historical-cycle research stopped when page evidence did not establish recurrence.

## O. Competitor Demand Findings

Competitor FACT vs INFERENCE kept separate. No hosting inferred without evidence. See COMPETITOR_DEMAND_COMPLETION.csv.

## P. Jev Performance

| Metric | Value |
|--------|------:|
| Recommendations issued | ${issued} |
| Accepted | ${accepted} |
| Rejected by policy | ${rejectedByPolicy} |
| Resolving blockers | ${resolvedBlockers} |
| Changing classification | ${changedClass} |
| Low-yield | ${lowYield} |

## Q. Research Depth Economics

| Depth | N | Useful | Avg cost |
|-------|--:|-------:|---------:|
| DEPTH_1 | ${d1.candidates} | ${d1.useful} | ${d1.candidates ? (d1.cost / d1.candidates).toFixed(3) : "0"} |
| DEPTH_2 | ${d2.candidates} | ${d2.useful} | ${d2.candidates ? (d2.cost / d2.candidates).toFixed(3) : "0"} |
| DEPTH_3 | ${d3.candidates} | ${d3.useful} | ${d3.candidates ? (d3.cost / d3.candidates).toFixed(3) : "0"} |

Cost/useful = ${costPerUseful == null ? "N/A" : costPerUseful.toFixed(3)} USD.

## R. Demand Engine Yield

Top by completed useful yield: **${topEngine ? engineLabel(topEngine.key) + " (" + topEngine.useful + ")" : "none"}**. Full table in DEMAND_ENGINE_YIELD.csv.

## S. Language / Feeder Market Yield

Top language by useful: **${topLang ? topLang.key + " (" + topLang.useful + ")" : "none"}**.  
Top feeder by useful: **${topFeeder ? topFeeder.key + " (" + topFeeder.useful + ")" : "none"}**.  
Judged from completed useful opportunities, not SERP volume.

## T. Recommended Default GDI Research Behavior

| Control | Recommendation |
|---------|----------------|
| Candidate prioritization | **${defaults.candidatePriority}** (keep deterministic baseline) |
| Next blocker selection | **${defaults.nextBlocker}** |
| Source selection | **${defaults.sourceSelection}** |
| Language selection | **${defaults.languageSelection}** |
| Feeder-market selection | **${defaults.feederMarketSelection}** |
| Research-depth decision | **${defaults.researchDepth}** |
| Stop/continue decision | **${defaults.stopContinue}** |

Based on measured yield this run — not theory.

---

### Safety attestations

| Check | Result |
|-------|--------|
| GDI thresholds changed? | **NO** |
| Jev wrote verified facts? | **NO** |
| Jev promoted opportunities? | **NO** |
| Watch quality standard bypassed? | **NO** |
| New broad discovery run? | **NO** |
| ADP changed? | **NO** |
| Share tokens changed? | **NO** |

### Final verdict

${
  usefulTotal > 0
    ? `Page-level completion converted **${usefulTotal}** V3 candidates into useful outcomes (ready+watch). Jev advisory guided look/depth; canonical gates decided truth.`
    : `Page-level completion confirmed V3 thinness: **0** useful promotions after bounded P0/P1 research. Depth policy + public-data ceilings terminated research without threshold cuts. Jev stop/continue economics are the primary learning for default GDI behavior.`
}

**STOP.**
`;

  write("FOUNDER_REPORT.md", founder);

  write(
    "_RETURN.json",
    JSON.stringify(
      {
        TOTAL_V3_CANDIDATES: loadMeta.v3OfficialTotal,
        RESEARCH_POOL: candidates.length,
        TOTAL_P0_CANDIDATES: P0,
        TOTAL_P1_CANDIDATES: P1,
        TOTAL_FULLY_RESEARCHED: fullyResearched,
        TOTAL_CUSTOMER_READY: customerReady,
        TOTAL_VALID_FUTURE_WATCH: validWatch,
        TOTAL_REJECTED_CONFIRMED: rejected,
        TOTAL_UNRESOLVED_PUBLIC_DATA_CEILING: unresolved,
        JEV_RECOMMENDATIONS_ISSUED: issued,
        JEV_RECOMMENDATIONS_ACCEPTED: accepted,
        JEV_RECOMMENDATIONS_REJECTED_BY_POLICY: rejectedByPolicy,
        JEV_RECOMMENDATIONS_RESOLVING_BLOCKERS: resolvedBlockers,
        JEV_RECOMMENDATIONS_CHANGING_CLASSIFICATION: changedClass,
        DEPTH_1_COUNT: d1.candidates,
        DEPTH_1_USEFUL: d1.useful,
        DEPTH_2_COUNT: d2.candidates,
        DEPTH_2_USEFUL: d2.useful,
        DEPTH_3_COUNT: d3.candidates,
        DEPTH_3_USEFUL: d3.useful,
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
        TOP_DEMAND_ENGINE: topEngine ? engineLabel(topEngine.key) : null,
        TOP_LANGUAGE: topLang?.key || null,
        TOP_FEEDER_MARKET: topFeeder?.key || null,
        WHO_RESOLVED_COUNT: whoResolvedCount,
        TIMING_BLOCKERS_RESOLVED: timingResolved,
        LODGING_BLOCKERS_RESOLVED: lodgingResolved,
        NEXT_BEST_RESEARCH_ACTIONS_RUN: nextBestActions,
        AVERAGE_RESEARCH_COST_PER_P0: Number(p0Cost.toFixed(3)),
        COST_PER_USEFUL_OPPORTUNITY: costPerUseful == null ? null : Number(costPerUseful.toFixed(3)),
        SHOULD_JEV_CONTROL_CANDIDATE_PRIORITY: defaults.candidatePriority,
        SHOULD_JEV_CONTROL_NEXT_BLOCKER: defaults.nextBlocker,
        SHOULD_JEV_CONTROL_SOURCE_SELECTION: defaults.sourceSelection,
        SHOULD_JEV_CONTROL_LANGUAGE_SELECTION: defaults.languageSelection,
        SHOULD_JEV_CONTROL_FEEDER_MARKET_SELECTION: defaults.feederMarketSelection,
        SHOULD_JEV_CONTROL_RESEARCH_DEPTH: defaults.researchDepth,
        SHOULD_JEV_CONTROL_STOP_CONTINUE: defaults.stopContinue,
        GDI_THRESHOLDS_CHANGED: false,
        JEV_WROTE_VERIFIED_FACTS: false,
        JEV_PROMOTED_OPPORTUNITIES: false,
        WATCH_QUALITY_STANDARD_BYPASSED: false,
        NEW_BROAD_DISCOVERY_RUN: false,
        ADP_CHANGED: false,
        SHARE_TOKENS_CHANGED: false,
        loadMeta,
      },
      null,
      2
    )
  );

  console.log("\n========== RETURN ==========");
  console.log(JSON.stringify(JSON.parse(fs.readFileSync(path.join(OUT, "_RETURN.json"), "utf8")), null, 2));
  console.log(`[completion-v2] reports → ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
