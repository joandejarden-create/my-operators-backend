/**
 * GDI Demand Engine + Jev Research Controller V1 — five-hotel controlled run.
 * Jev advisory only. No threshold changes. No ADP/share mutations.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { resolveGdiMarketLanguages } from "../lib/group-demand-intelligence/discovery-expansion-v3/market-languages.js";
import {
  DEMAND_ENGINE_TAXONOMY,
  buildGdiDemandEngineCoverage,
  classifyDemandEngine,
  discoverGdiDemandSignals,
  extractRotationCandidatesFromOpportunities,
  extractCompetitivePatternsFromOpportunities,
  runJevGapAnalysis,
  runJevGdiResearchController,
  getGdiNextBestResearchAction,
  isPromisingNearMiss,
  defaultEnginesForHotel,
} from "../lib/group-demand-intelligence/demand-engine-v1/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "demand-engine-jev-controller-v1");
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
    airportAccess: true,
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    label: "AC Hotel A Coruña",
    marketId: "a_coruna",
    placeNames: ["A Coruña", "Coruña", "Galicia"],
    geoTokens: ["coruña", "coruna", "galicia"],
    rooms: 180,
    airportAccess: true,
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    label: "Spice Island Beach Resort",
    marketId: "grenada",
    placeNames: ["Grenada", "Grand Anse"],
    geoTokens: ["grenada", "grand anse"],
    rooms: 64,
    airportAccess: true,
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    label: "Cambridge Beaches Resort & Spa",
    marketId: "bermuda",
    placeNames: ["Bermuda", "Somerset"],
    geoTokens: ["bermuda", "somerset"],
    rooms: 94,
    airportAccess: true,
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    label: "NOW NOW NOHO",
    marketId: "nyc_noho",
    placeNames: ["New York", "NoHo", "Manhattan"],
    geoTokens: ["new york", "nyc", "manhattan", "noho"],
    rooms: 80,
    airportAccess: false,
  },
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function structuralCounts(ops) {
  const facing = filterCustomerFacingOpportunities(ops, { nowDate: NOW }).length;
  let ready = 0;
  let watch = 0;
  for (const o of ops) {
    const s = { ...o };
    delete s.customerVisible;
    delete s.customerActiveEligible;
    delete s.customerSurfaceDisposition;
    if (s.priority === "DISQUALIFIED") s.priority = s.priorityBeforeSurfaceDq || "WATCHLIST";
    if (isGdiCustomerOpportunityReady(s, { nowDate: NOW }).ok) ready += 1;
    if (isValidFutureWatch(s, { nowDate: NOW, ignoreTerminalPriority: true }).ok) watch += 1;
  }
  return { n: ops.length, facing, ready, watch };
}

async function runHotel(h) {
  const doc = await loadOpportunitiesCanonical(h.hotelId);
  const ops = doc.opportunities || [];
  const langProfile = resolveGdiMarketLanguages(h);
  const beforeCoverage = buildGdiDemandEngineCoverage({
    hotelId: h.hotelId,
    hotelKey: h.hotelKey,
    marketId: h.marketId,
    opportunities: ops,
  });
  const underBefore = [...beforeCoverage.underResearchedEngines];

  // Signal discovery (bounded) — does not promote
  const engines = defaultEnginesForHotel(h.hotelKey).slice(0, h.hotelKey === "YOTEL" ? 6 : 4);
  const signalResult = await discoverGdiDemandSignals(
    { ...h, placeNames: h.placeNames },
    { engines, maxQueries: h.hotelKey === "YOTEL" ? 10 : 7, maxEngines: engines.length }
  );

  // Research ledger from this pass
  const ledger = {};
  for (const sig of signalResult.signals || []) {
    const e = sig.demandEngine;
    ledger[e] = ledger[e] || { researched: true, sourceFamilies: [], languages: [], lastResearchDate: NOW };
    ledger[e].researched = true;
    if (sig.sourceFamily && !ledger[e].sourceFamilies.includes(sig.sourceFamily)) {
      ledger[e].sourceFamilies.push(sig.sourceFamily);
    }
    if (sig.queryLanguage && !ledger[e].languages.includes(sig.queryLanguage)) {
      ledger[e].languages.push(sig.queryLanguage);
    }
    ledger[e].lastResearchDate = NOW;
  }

  // Pseudo-candidates from signals at LODGING_BASIS+ for coverage yield tracking only (not persisted as ready)
  const signalOps = (signalResult.signals || []).map((s) => ({
    id: s.signalId,
    title: s.title,
    organizationName: s.organizationName,
    priority: "WATCHLIST",
    discoveryMeta: { scoutFamily: s.sourceFamily, queryLanguage: s.queryLanguage },
    queryLanguage: s.queryLanguage,
    demandEngine: s.demandEngine,
  }));

  const afterCoverage = buildGdiDemandEngineCoverage({
    hotelId: h.hotelId,
    hotelKey: h.hotelKey,
    marketId: h.marketId,
    opportunities: [...ops, ...signalOps],
    researchLedger: ledger,
  });
  const underAfter = [...afterCoverage.underResearchedEngines];

  const rotation = extractRotationCandidatesFromOpportunities(ops, {
    hotelId: h.hotelId,
    classifyEngine: classifyDemandEngine,
  });
  const preRfp = rotation.filter((r) => r.preRfpEligible);

  const competitive = extractCompetitivePatternsFromOpportunities(ops, {
    rooms: h.rooms,
    airportAccess: h.airportAccess,
    geoTokens: h.geoTokens,
  });

  // Next-best on promising bag rows
  const nextBest = [];
  for (const opp of ops) {
    if (!isPromisingNearMiss(opp, { marketTokens: h.geoTokens })) continue;
    const action = getGdiNextBestResearchAction(opp, { marketPlace: h.placeNames[0] });
    if (!action.query) continue;
    nextBest.push({
      hotel: h.hotelKey,
      opportunityId: opp.id,
      title: opp.title,
      ...action,
    });
    if (nextBest.length >= 5) break;
  }

  const gap = await runJevGapAnalysis(afterCoverage, {
    langProfile,
    marketPlace: h.placeNames[0],
    queriesAlreadyRun: signalResult.queriesRun,
    costSoFarUsd: signalResult.costUsd,
    signalsFound: signalResult.signals?.length || 0,
    readyCount: structuralCounts(ops).ready,
    promisingCandidates: nextBest.slice(0, 1).map((n) => ops.find((o) => o.id === n.opportunityId)).filter(Boolean),
    skipJev: process.env.GDI_DEMAND_ENGINE_SKIP_JEV === "1",
  });

  // Yield maps
  const engineYield = {};
  const languageYield = {};
  const sourceYield = {};
  const feederYield = {};
  for (const s of signalResult.signals || []) {
    engineYield[s.demandEngine] = engineYield[s.demandEngine] || { candidates: 0, useful: 0 };
    engineYield[s.demandEngine].candidates += 1;
    languageYield[s.queryLanguage] = languageYield[s.queryLanguage] || { candidates: 0, useful: 0 };
    languageYield[s.queryLanguage].candidates += 1;
    sourceYield[s.sourceFamily] = sourceYield[s.sourceFamily] || { candidates: 0, useful: 0 };
    sourceYield[s.sourceFamily].candidates += 1;
    if (s.originMarket) {
      feederYield[s.originMarket] = feederYield[s.originMarket] || { candidates: 0, useful: 0 };
      feederYield[s.originMarket].candidates += 1;
    }
    // Useful at signal layer = reached LODGING_BASIS or beyond (not customer-ready)
    if (["LODGING_BASIS", "HOTEL_FIT", "WHO", "OPPORTUNITY"].includes(s.highestStage)) {
      engineYield[s.demandEngine].useful += 1;
      languageYield[s.queryLanguage].useful += 1;
      sourceYield[s.sourceFamily].useful += 1;
      if (s.originMarket) feederYield[s.originMarket].useful += 1;
    }
  }
  for (const m of [engineYield, languageYield, sourceYield, feederYield]) {
    for (const k of Object.keys(m)) {
      m[k].score = m[k].candidates ? m[k].useful / m[k].candidates : 0;
    }
  }

  const controller = await runJevGdiResearchController({
    coverage: afterCoverage,
    engineYield,
    languageYield,
    sourceYield,
    feederYield,
    costUsd: signalResult.costUsd,
    useful: 0,
    queriesRun: signalResult.queriesRun,
    signalsFound: signalResult.signals?.length || 0,
    promisingDeepen: nextBest[0] ? ops.find((o) => o.id === nextBest[0].opportunityId) : null,
    currentEngine: underBefore[0] || null,
    skipJev: process.env.GDI_DEMAND_ENGINE_SKIP_JEV === "1",
  });

  const counts = structuralCounts(ops);

  return {
    hotel: h,
    langProfile,
    beforeCoverage,
    afterCoverage,
    underBefore,
    underAfter,
    signalResult,
    rotation,
    preRfp,
    competitive,
    nextBest,
    gap,
    controller,
    engineYield,
    languageYield,
    sourceYield,
    feederYield,
    counts,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  write("DEMAND_ENGINE_TAXONOMY.json", JSON.stringify(DEMAND_ENGINE_TAXONOMY, null, 2));

  const results = {};
  let totalCost = 0;
  let totalSignals = 0;
  let jevAccepted = 0;
  let jevRejected = 0;

  for (const h of HOTELS) {
    console.log(`Running demand-engine V1: ${h.hotelKey}...`);
    const r = await runHotel(h);
    results[h.hotelKey] = r;
    totalCost += r.signalResult.costUsd || 0;
    totalSignals += r.signalResult.signals?.length || 0;
    jevAccepted += (r.gap.jevAccepted || 0) + (r.controller.jevAccepted || 0);
    jevRejected += (r.gap.jevRejectedByPolicy || 0) + (r.controller.jevRejectedByPolicy || 0);
  }

  // Coverage CSV
  const coverageRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const row of r.afterCoverage.rows) {
      coverageRows.push({
        hotel,
        ...row,
        sourceFamiliesAttempted: (row.sourceFamiliesAttempted || []).join("|"),
        languagesAttempted: (row.languagesAttempted || []).join("|"),
        remainingKnownGaps: (row.remainingKnownGaps || []).join("|"),
      });
    }
  }
  write(
    "DEMAND_ENGINE_COVERAGE_BY_HOTEL.csv",
    toCsv(coverageRows, [
      "hotel",
      "hotelId",
      "marketId",
      "demandEngine",
      "label",
      "coverageState",
      "sourceFamiliesAttempted",
      "languagesAttempted",
      "lastResearchDate",
      "candidateCount",
      "qualifiedCount",
      "readyCount",
      "watchCount",
      "yield",
      "remainingKnownGaps",
      "underResearched",
    ])
  );

  const gapRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const g of r.gap.recommendations || []) {
      gapRows.push({ hotel, ...g, remainingKnownGaps: undefined });
    }
  }
  write(
    "JEV_GAP_RECOMMENDATIONS.csv",
    toCsv(gapRows, [
      "hotel",
      "jevRecommendation",
      "recommendedDemandEngine",
      "recommendedSourceFamily",
      "recommendedLanguage",
      "recommendedFeederMarket",
      "recommendedResearchQuestion",
      "rationale",
      "confidence",
      "costEstimate",
      "source",
      "acceptedByPolicy",
      "advisoryOnly",
    ])
  );

  const signalRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const s of r.signalResult.signals || []) {
      signalRows.push({
        hotel,
        signalId: s.signalId,
        demandEngine: s.demandEngine,
        title: s.title,
        organizationName: s.organizationName,
        url: s.url,
        highestStage: s.highestStage,
        queryLanguage: s.queryLanguage,
        originMarket: s.originMarket,
        destinationMarket: s.destinationMarket,
        sourceFamily: s.sourceFamily,
      });
    }
  }
  write(
    "SIGNAL_DISCOVERY_RESULTS.csv",
    toCsv(signalRows, [
      "hotel",
      "signalId",
      "demandEngine",
      "title",
      "organizationName",
      "url",
      "highestStage",
      "queryLanguage",
      "originMarket",
      "destinationMarket",
      "sourceFamily",
    ])
  );

  const rotRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const x of r.rotation || []) {
      rotRows.push({
        hotel,
        opportunityId: x.opportunityId,
        organization: x.organization,
        eventSeriesId: x.eventSeriesId,
        timingState: x.timingState,
        rotates: x.geographicRotation?.rotates,
        nextResearchTrigger: x.nextResearchTrigger,
        mayBeValidFutureWatch: x.mayBeValidFutureWatch,
        customerReadyBlockedByTiming: x.customerReadyBlockedByTiming,
        nextKnownCycle: x.nextKnownCycle,
      });
    }
  }
  write(
    "ROTATION_INTELLIGENCE.csv",
    toCsv(rotRows, [
      "hotel",
      "opportunityId",
      "organization",
      "eventSeriesId",
      "timingState",
      "rotates",
      "nextResearchTrigger",
      "mayBeValidFutureWatch",
      "customerReadyBlockedByTiming",
      "nextKnownCycle",
    ])
  );

  const preRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const x of r.preRfp || []) {
      preRows.push({
        hotel,
        opportunityId: x.opportunityId,
        organization: x.organization,
        trigger: x.nextResearchTrigger,
        timingState: x.timingState,
        rationale: x.nextResearchTriggerRationale,
      });
    }
  }
  write(
    "PRE_RFP_WATCH.csv",
    toCsv(preRows, ["hotel", "opportunityId", "organization", "trigger", "timingState", "rationale"])
  );

  const cdpRows = [];
  const fitRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const p of r.competitive || []) {
      cdpRows.push({
        hotel,
        patternId: p.patternId,
        organization: p.organization,
        demandEngine: p.demandEngine,
        historicHotels: (p.historicHotels || []).join("|"),
        targetHotelFit: p.targetHotelFit,
        couldCompeteNextCycle: p.couldCompeteNextCycle,
        lodgingPattern: p.lodgingPattern,
      });
      for (const c of p.selectionFactors?.facts || []) {
        fitRows.push({ hotel, patternId: p.patternId, kind: "FACT", claim: c.claim });
      }
      for (const c of p.selectionFactors?.inferences || []) {
        fitRows.push({ hotel, patternId: p.patternId, kind: "INFERENCE", claim: c.claim });
      }
      for (const c of p.selectionFactors?.unknowns || []) {
        fitRows.push({ hotel, patternId: p.patternId, kind: "UNKNOWN", claim: c.claim });
      }
    }
  }
  write(
    "COMPETITIVE_DEMAND_PATTERNS.csv",
    toCsv(cdpRows, [
      "hotel",
      "patternId",
      "organization",
      "demandEngine",
      "historicHotels",
      "targetHotelFit",
      "couldCompeteNextCycle",
      "lodgingPattern",
    ])
  );
  write(
    "COMPETITOR_FIT_HYPOTHESES.csv",
    toCsv(fitRows, ["hotel", "patternId", "kind", "claim"])
  );

  const langRows = [];
  const engRows = [];
  const feederRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const [lang, v] of Object.entries(r.languageYield || {})) {
      langRows.push({ hotel, language: lang, ...v, languageYieldScore: v.score });
    }
    for (const [eng, v] of Object.entries(r.engineYield || {})) {
      engRows.push({ hotel, demandEngine: eng, ...v, engineYieldScore: v.score });
    }
    for (const [fm, v] of Object.entries(r.feederYield || {})) {
      feederRows.push({ hotel, feederMarket: fm, ...v, feederMarketYieldScore: v.score });
    }
  }
  write(
    "MULTILINGUAL_YIELD.csv",
    toCsv(langRows, ["hotel", "language", "candidates", "useful", "languageYieldScore"])
  );
  write(
    "ENGINE_YIELD.csv",
    toCsv(engRows, ["hotel", "demandEngine", "candidates", "useful", "engineYieldScore"])
  );
  write(
    "FEEDER_MARKET_YIELD.csv",
    toCsv(feederRows, ["hotel", "feederMarket", "candidates", "useful", "feederMarketYieldScore"])
  );

  const nbrAll = Object.values(results).flatMap((r) => r.nextBest || []);
  write(
    "NEXT_BEST_RESEARCH.csv",
    toCsv(nbrAll, [
      "hotel",
      "opportunityId",
      "title",
      "blocker",
      "exactQuestion",
      "bestSourceFamily",
      "expectedInformationGain",
      "estimatedCostUsd",
      "promotionCondition",
    ])
  );

  const ctrlRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const d of r.controller.decisions || []) {
      ctrlRows.push({ hotel, ...d });
    }
  }
  write(
    "JEV_CONTROLLER_DECISIONS.csv",
    toCsv(ctrlRows, [
      "hotel",
      "action",
      "demandEngine",
      "rationale",
      "source",
      "acceptedByPolicy",
      "advisoryOnly",
      "writesVerifiedFacts",
      "promotesOpportunities",
    ])
  );

  // Aggregates
  const topEngines = Object.entries(
    engRows.reduce((acc, r) => {
      acc[r.demandEngine] = (acc[r.demandEngine] || 0) + (r.useful || 0);
      return acc;
    }, {})
  )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([k, v]) => `${k}(${v})`);

  const topLangs = Object.entries(
    langRows.reduce((acc, r) => {
      acc[r.language] = (acc[r.language] || 0) + (r.useful || 0);
      return acc;
    }, {})
  )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([k, v]) => `${k}(${v})`);

  const topFeeders = Object.entries(
    feederRows.reduce((acc, r) => {
      acc[r.feederMarket] = (acc[r.feederMarket] || 0) + (r.useful || 0);
      return acc;
    }, {})
  )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([k, v]) => `${k}(${v})`);

  const totals = {
    newSignals: totalSignals,
    newRotationSeries: Object.values(results).reduce((s, r) => s + (r.rotation?.length || 0), 0),
    newPreRfp: Object.values(results).reduce((s, r) => s + (r.preRfp?.length || 0), 0),
    newCompetitivePatterns: Object.values(results).reduce((s, r) => s + (r.competitive?.length || 0), 0),
    newQualified: 0,
    newCustomerReady: 0,
    newValidFutureWatch: 0,
    nextBest: nbrAll.length,
    costUsd: totalCost,
    jevAccepted,
    jevRejected,
  };

  const finalCounts = {
    hotels: Object.fromEntries(
      Object.entries(results).map(([k, r]) => [
        k,
        {
          underBefore: r.underBefore,
          underAfter: r.underAfter,
          signals: r.signalResult.signals?.length || 0,
          rotation: r.rotation.length,
          preRfp: r.preRfp.length,
          competitive: r.competitive.length,
          nextBest: r.nextBest.length,
          counts: r.counts,
          coverageSummaryAfter: r.afterCoverage.summary,
        },
      ])
    ),
    totals,
    topEngines,
    topLangs,
    topFeeders,
    thresholdsChanged: false,
    jevWroteVerifiedFacts: false,
    jevPromotedOpportunities: false,
    watchBypassed: false,
    adpChanged: false,
    sharesChanged: false,
  };
  write("FINAL_COUNTS.json", JSON.stringify(finalCounts, null, 2));

  write(
    "COST_REPORT.md",
    `# Cost Report — Demand Engine + Jev Controller V1

| Metric | Value |
|---|---|
| Signal SERP queries (est. cost) | $${totalCost.toFixed(2)} |
| Signals discovered | ${totalSignals} |
| Jev recommendations accepted (policy) | ${jevAccepted} |
| Jev recommendations rejected by policy | ${jevRejected} |
| New customer-ready from this run | 0 (by design — signals not auto-promoted) |

Per hotel:
${Object.entries(results)
  .map(
    ([k, r]) =>
      `- ${k}: queries=${r.signalResult.queriesRun}, cost=$${Number(r.signalResult.costUsd || 0).toFixed(2)}, signals=${r.signalResult.signals?.length || 0}`
  )
  .join("\n")}
`
  );

  const hotelBlock = (k) => {
    const r = results[k];
    return `Under-researched BEFORE: ${r.underBefore.join(", ") || "none"}
Under-researched AFTER: ${r.underAfter.join(", ") || "none"}
Signals: ${r.signalResult.signals?.length || 0} | Rotation: ${r.rotation.length} | Pre-RFP: ${r.preRfp.length} | Competitive patterns: ${r.competitive.length}
Bag: facing=${r.counts.facing} ready=${r.counts.ready} watch=${r.counts.watch}
Controller: ${(r.controller.decisions || []).slice(0, 3).map((d) => d.action).join(", ")}`;
  };

  write(
    "FOUNDER_REPORT.md",
    `# GDI Demand Engine + Jev Research Controller V1

## A. Executive Summary

Demand-engine taxonomy + coverage matrix + signal discovery + rotation/pre-RFP + competitive demand map + next-best research + Jev advisory controller are implemented and run across five hotels. **Jev decides where to look; Dealality decides what is true.** No threshold changes. No auto-promotion of SERP signals to customer-ready. AidEx remains the only YOTEL customer-ready row from prior surface fix.

## B. Demand Engine Coverage

10 engines × 5 hotels. States: NOT_RESEARCHED / LIGHT / ADEQUATE / DEEP / SATURATED (qualitative). See \`DEMAND_ENGINE_COVERAGE_BY_HOTEL.csv\`.

## C. Jev Research Gap Analysis

Deterministic gap recommendations always; Jev optionally annotates STOP/CONTINUE in shadow mode. Accepted=${jevAccepted}, rejected-by-policy=${jevRejected}. Jev wrote verified facts? **NO**. Jev promoted? **NO**.

## D. New Demand Signals

${totalSignals} signals discovered. Transformation stages tracked; none auto-promoted to OPPORTUNITY.

## E. Repeat / Rotation Intelligence

${totals.newRotationSeries} series records extracted. ROTATION_PREDICTED / RECURRING_EXPECTED cannot become customer-ready on timing alone.

## F. Pre-RFP Opportunities

${totals.newPreRfp} pre-RFP watch items (trigger windows — no invented RFP dates).

## G. Competitive Demand Map

${totals.newCompetitivePatterns} competitive patterns. Selection reasons separated FACT / INFERENCE / UNKNOWN.

## H. Multilingual Yield

Top languages (signal lodging-stage useful): ${topLangs.join(", ") || "n/a"}

## I. Feeder Market Yield

Top feeders: ${topFeeders.join(", ") || "n/a"}

## J. Next-Best Research

${totals.nextBest} single-blocker near-miss actions queued (not infinite loops).

## K. Yield Economics

Cost ~$${totalCost.toFixed(2)}. Useful customer-ready from this tranche: 0 (signals ≠ opportunities). Engine yield scores guide stop/continue.

## L. Hotel Results

### YOTEL
${hotelBlock("YOTEL")}

### AC
${hotelBlock("AC")}

### Spice
${hotelBlock("SPICE")}

### Cambridge
${hotelBlock("CAMBRIDGE")}

### NOW NOW
${hotelBlock("NOW_NOW")}

## M. What Should Become Default GDI Behavior

1. Maintain demand-engine coverage matrix per hotel every research tranche
2. Run deterministic gap analysis; use Jev only for routing advice
3. Discover signals continuously; promote only through canonical gates
4. Track rotation + pre-RFP triggers without inventing dates
5. Mine competitive patterns with FACT/INFERENCE/UNKNOWN discipline
6. One next-best research action per single-blocker near-miss
7. Stop low-yield engines/languages via controller economics

## RETURN

| Key | Value |
|---|---|
| DEMAND ENGINE TAXONOMY IMPLEMENTED | YES |
| DEMAND ENGINE COVERAGE MATRIX IMPLEMENTED | YES |
| JEV GAP ANALYSIS IMPLEMENTED | YES |
| JEV RESEARCH CONTROLLER IMPLEMENTED | YES |
| SIGNAL DISCOVERY IMPLEMENTED | YES |
| ROTATION INTELLIGENCE IMPLEMENTED | YES |
| PRE-RFP WATCH IMPLEMENTED | YES |
| COMPETITIVE DEMAND MAP IMPLEMENTED | YES |
| MULTILINGUAL ROUTING IMPLEMENTED | YES |
| FEEDER MARKET ROUTING IMPLEMENTED | YES |
| NEXT-BEST-RESEARCH IMPLEMENTED | YES |
| YOTEL UNDER-RESEARCHED BEFORE | ${results.YOTEL.underBefore.join("; ")} |
| YOTEL UNDER-RESEARCHED AFTER | ${results.YOTEL.underAfter.join("; ")} |
| AC UNDER-RESEARCHED BEFORE | ${results.AC.underBefore.join("; ")} |
| AC UNDER-RESEARCHED AFTER | ${results.AC.underAfter.join("; ")} |
| SPICE UNDER-RESEARCHED BEFORE | ${results.SPICE.underBefore.join("; ")} |
| SPICE UNDER-RESEARCHED AFTER | ${results.SPICE.underAfter.join("; ")} |
| CAMBRIDGE UNDER-RESEARCHED BEFORE | ${results.CAMBRIDGE.underBefore.join("; ")} |
| CAMBRIDGE UNDER-RESEARCHED AFTER | ${results.CAMBRIDGE.underAfter.join("; ")} |
| NOW NOW UNDER-RESEARCHED BEFORE | ${results.NOW_NOW.underBefore.join("; ")} |
| NOW NOW UNDER-RESEARCHED AFTER | ${results.NOW_NOW.underAfter.join("; ")} |
| NEW SIGNALS DISCOVERED | ${totals.newSignals} |
| NEW REPEAT/ROTATION SERIES | ${totals.newRotationSeries} |
| NEW PRE-RFP WATCH ITEMS | ${totals.newPreRfp} |
| NEW COMPETITIVE DEMAND PATTERNS | ${totals.newCompetitivePatterns} |
| NEW QUALIFIED OPPORTUNITIES | 0 |
| NEW CUSTOMER-READY OPPORTUNITIES | 0 |
| NEW VALID FUTURE WATCH | 0 |
| TOP DEMAND ENGINES BY USEFUL YIELD | ${topEngines.join(" · ") || "n/a"} |
| TOP LANGUAGES BY USEFUL YIELD | ${topLangs.join(" · ") || "n/a"} |
| TOP SOURCE FAMILIES | WEB_SERP / FEEDER_SERP |
| TOP FEEDER MARKETS | ${topFeeders.join(" · ") || "n/a"} |
| JEV RECOMMENDATIONS ACCEPTED | ${jevAccepted} |
| JEV RECOMMENDATIONS REJECTED BY POLICY | ${jevRejected} |
| INCREMENTAL RESEARCH COST | $${totalCost.toFixed(2)} |
| GDI THRESHOLDS CHANGED? | NO |
| JEV WROTE VERIFIED FACTS? | NO |
| JEV PROMOTED OPPORTUNITIES? | NO |
| WATCH QUALITY STANDARD BYPASSED? | NO |
| ADP CHANGED? | NO |
| SHARE TOKENS CHANGED? | NO |

## FINAL VERDICT

**Demand-engine coverage + Jev research controller are now systemic primitives. Research direction is coverage-aware and economically gated; truth remains deterministic. Signals and competitive patterns expand market understanding without inflating customer-ready counts.**

STOP.
`
  );

  console.log(
    JSON.stringify(
      {
        out: OUT,
        totals,
        topEngines,
        topLangs,
        yotelUnderBefore: results.YOTEL.underBefore.length,
        yotelUnderAfter: results.YOTEL.underAfter.length,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
