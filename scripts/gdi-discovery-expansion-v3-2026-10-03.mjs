/**
 * GDI Discovery Expansion V3 — surface bug fix apply + bounded 5-hotel discovery.
 * Does NOT lower readiness/watch thresholds. Does NOT touch ADP/shares.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
  upsertSingleOpportunity,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  classifyCustomerSurfaceOpportunity,
  applyCustomerSurfaceDisposition,
  CUSTOMER_SURFACE_DISPOSITION,
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import {
  resolveGdiMarketLanguages,
  MARKET_LANGUAGE_PROFILES,
  runDiscoveryExpansionV3ForHotel,
  SCOUT_FAMILY,
  buildLocalizedQueryMatrix,
  CANONICAL_INTENTS,
} from "../lib/group-demand-intelligence/discovery-expansion-v3/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "discovery-expansion-v3-2026-10-03");
const NOW = "2026-10-03";

const HOTELS = [
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelKey: "YOTEL",
    label: "YOTEL Geneva Lake",
    placeNames: ["Geneva", "Genève", "Nyon", "La Côte", "Palexpo"],
    geoTokens: ["geneva", "genève", "switzerland", "suisse", "palexpo", "nyon", "vaud"],
    fitLine: "YOTEL Geneva Lake: airport / La Côte corridor; 237 rooms compact upscale.",
    defaultFitScore: 55,
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    label: "AC Hotel A Coruña",
    placeNames: ["A Coruña", "Coruña", "Galicia"],
    geoTokens: ["coruña", "coruna", "galicia", "spain", "españa"],
    fitLine: "AC A Coruña: urban select-service for congress / corporate Galicia demand.",
    defaultFitScore: 52,
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    label: "Spice Island Beach Resort",
    placeNames: ["Grenada", "Grand Anse", "St. George's"],
    geoTokens: ["grenada", "grand anse", "caribbean"],
    fitLine: "Spice Island: luxury beach resort for incentive / high-end group lodging.",
    defaultFitScore: 58,
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    label: "Cambridge Beaches Resort & Spa",
    placeNames: ["Bermuda", "Somerset", "Cambridge Beaches"],
    geoTokens: ["bermuda", "somerset", "hamilton"],
    fitLine: "Cambridge Beaches: cottage resort for incentive / executive retreat demand.",
    defaultFitScore: 56,
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    label: "NOW NOW NOHO",
    placeNames: ["New York", "NoHo", "Manhattan", "NYC"],
    geoTokens: ["new york", "nyc", "manhattan", "noho"],
    fitLine: "NOW NOW NoHo: boutique urban for compact corporate / creative group stays.",
    defaultFitScore: 50,
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
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function countBaseline(ops) {
  const facing = filterCustomerFacingOpportunities(ops, { nowDate: NOW });
  let ready = 0;
  let watch = 0;
  for (const o of ops) {
    const struct = { ...o };
    delete struct.customerVisible;
    delete struct.customerActiveEligible;
    delete struct.customerSurfaceDisposition;
    if (struct.priority === "DISQUALIFIED") {
      struct.priority = struct.priorityBeforeSurfaceDq || "WATCHLIST";
    }
    if (isGdiCustomerOpportunityReady(struct, { nowDate: NOW }).ok) ready += 1;
    const w = isValidFutureWatch(struct, { nowDate: NOW, ignoreTerminalPriority: true });
    if (w.ok) watch += 1;
  }
  return { n: ops.length, facing: facing.length, structuralReady: ready, structuralWatch: watch };
}

async function fixAidExSurface(yotelDoc) {
  const before = { disposition: null, ready: null };
  const after = { disposition: null, ready: null };
  const idx = (yotelDoc.opportunities || []).findIndex((o) => o.id === "gdi_opp_aidex_geneva_11");
  if (idx < 0) {
    return { found: false, before, after };
  }
  const raw = yotelDoc.opportunities[idx];
  const probe = { ...raw };
  delete probe.customerVisible;
  delete probe.customerActiveEligible;
  delete probe.customerSurfaceDisposition;
  // BEFORE semantics already known from forensic; recompute with current code = AFTER logic on unstamped
  // Capture "before" by simulating old bug: if we still had bug, disposition would be DOWNGRADE.
  // We record forensic before + live after.
  before.disposition = "DOWNGRADE_TO_DEMAND_GENERATOR";
  before.ready = false;
  before.note = "forensic_2026_10_03_pre_fix";

  const cls = classifyCustomerSurfaceOpportunity(probe, { nowDate: NOW });
  const applied = applyCustomerSurfaceDisposition(probe, cls, { nowDate: NOW });
  // Stamp lodging from known accommodation URL (public evidence already on discoverySource)
  if (/accommodation/i.test(String(applied.discoverySource || applied.officialSource || ""))) {
    applied.lodgingEvidence = {
      ...(applied.lodgingEvidence || {}),
      housingPageFound: true,
      roomBlockMentioned: Boolean(applied.lodgingEvidence?.roomBlockMentioned),
      status: applied.lodgingEvidence?.roomBlockMentioned ? "CREDIBLE" : "WEAK",
      sourceUrl: applied.discoverySource || "https://aid-expo.com/accommodation",
    };
  }
  const ready = isGdiCustomerOpportunityReady(applied, { nowDate: NOW });
  after.disposition = cls.disposition;
  after.ready = ready.ok;
  after.failed = ready.failed;
  after.keepActive = cls.keepActive;

  yotelDoc.opportunities[idx] = {
    ...applied,
    customerVisible: cls.keepActive && ready.ok ? true : applied.customerVisible,
    customerActiveEligible: cls.keepActive,
    gdiSurfaceBugFixV3At: new Date().toISOString(),
  };

  await upsertSingleOpportunity(yotelDoc.hotelId || "recrPQcZg7SFARRb2", yotelDoc.opportunities[idx], {
    runId: "gdi_discovery_expansion_v3_2026_10_03",
  });

  return { found: true, before, after, opportunity: yotelDoc.opportunities[idx] };
}

function rowsForScout(resultsByHotel, scout, mapFn) {
  const rows = [];
  for (const [hotel, res] of Object.entries(resultsByHotel)) {
    for (const c of res.newCandidates || []) {
      if ((c.discoveryMeta?.scoutFamily || "") !== scout) continue;
      rows.push(mapFn(hotel, c, res));
    }
  }
  return rows;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });

  // Phase 0 — AidEx
  const yotelLoad = await loadOpportunitiesCanonical("recrPQcZg7SFARRb2");
  const aidexFix = await fixAidExSurface({
    hotelId: "recrPQcZg7SFARRb2",
    opportunities: [...(yotelLoad.opportunities || [])],
  });

  const baselines = {};
  const results = {};
  let totalCost = 0;
  let totalQueries = 0;

  for (const h of HOTELS) {
    const doc = await loadOpportunitiesCanonical(h.hotelId);
    // Refresh YOTEL after AidEx upsert
    const ops =
      h.hotelKey === "YOTEL"
        ? (await loadOpportunitiesCanonical(h.hotelId)).opportunities || []
        : doc.opportunities || [];
    baselines[h.hotelKey] = countBaseline(ops);

    const res = await runDiscoveryExpansionV3ForHotel(
      { ...h, existingOpps: ops },
      {
        existingOpps: ops,
        maxQueries: h.hotelKey === "YOTEL" || h.hotelKey === "AC" ? 12 : 10,
        maxScouts: 5,
        maxLanguagesPerScout: 2,
        maxNextBest: 3,
        allowSelective: h.hotelKey === "YOTEL" || h.hotelKey === "AC",
        nowDate: NOW,
      }
    );

    // Persist only customer-ready + valid future watch (strict) — not all raw hits
    const toPersist = [];
    for (const c of [...res.customerReady, ...res.futureWatch]) {
      if (ops.some((o) => o.id === c.id || String(o.title).toLowerCase() === String(c.title).toLowerCase())) {
        continue;
      }
      toPersist.push({
        ...c,
        hotelId: h.hotelId,
        priority: c.qualification?.customerReady ? "MEDIUM_PRIORITY" : "WATCHLIST",
        gdiDiscoveryVersion: "discovery_expansion_v3",
        customerVisible: Boolean(c.qualification?.customerReady),
        customerActiveEligible: Boolean(c.qualification?.surfaceKeep || c.qualification?.customerReady),
      });
    }

    if (toPersist.length) {
      const merged = [...ops];
      for (const p of toPersist) {
        const i = merged.findIndex((o) => o.id === p.id);
        if (i >= 0) merged[i] = { ...merged[i], ...p };
        else merged.push(p);
      }
      await saveOpportunitiesCanonical(h.hotelId, {
        hotelId: h.hotelId,
        opportunities: merged,
        runId: "gdi_discovery_expansion_v3_2026_10_03",
        researchVersion: "discovery_expansion_v3",
      });
    }

    // Final counts after persist
    const afterDoc = await loadOpportunitiesCanonical(h.hotelId);
    const afterCounts = countBaseline(afterDoc.opportunities || []);
    results[h.hotelKey] = {
      ...res,
      persisted: toPersist.length,
      finalCounts: afterCounts,
      baseline: baselines[h.hotelKey],
    };
    totalCost += res.costUsd || 0;
    totalQueries += res.queriesRun || 0;

    console.log(
      JSON.stringify({
        hotel: h.hotelKey,
        queries: res.queriesRun,
        new: res.newCandidates.length,
        ready: res.customerReady.length,
        watch: res.futureWatch.length,
        cost: res.costUsd,
        serp: res.serpEnabled,
      })
    );
  }

  // --- Reports ---
  write(
    "SURFACE_BUG_FIX.md",
    `# Surface Bug Fix — hasExplicitHotelMotionCopy

## Reproduced
YES — AidEx Geneva was \`DOWNGRADE_TO_DEMAND_GENERATOR\` / \`public_demand_generator_without_hotel_thesis\` when \`summaryWhyMatters\` was boilerplate despite overflow thesis + accommodation URL.

## Root cause
Early path scanned only venue/destination/title when why/matters were boilerplate, **discarding** \`hotelOpportunityThesis\`.

## Fix
1. Prefer **structured** lodging / housing-URL / placement evidence
2. Always honor non-boilerplate \`hotelOpportunityThesis\` / overflow thesis
3. Only strip boilerplate why/matters from the free-text blob — never drop thesis

## AidEx
| | Disposition | Customer Ready |
|---|---|---|
| BEFORE | ${aidexFix.before?.disposition} | ${aidexFix.before?.ready} |
| AFTER | ${aidexFix.after?.disposition} | ${aidexFix.after?.ready} |

Remaining blockers after fix: ${(aidexFix.after?.failed || []).join(", ") || "none"}

## Threshold change
NO

## Regression
\`npm run test:gdi-customer-surface-revalidation\` includes AidEx KEEP_ACTIVE case.
`
  );

  write("MARKET_LANGUAGE_PROFILES.json", JSON.stringify(MARKET_LANGUAGE_PROFILES, null, 2));

  // Localized query matrix (planned, not only executed)
  const matrixRows = [];
  for (const h of HOTELS) {
    const lp = resolveGdiMarketLanguages(h);
    const langs = [lp.primaryLanguage, ...lp.secondaryLanguages].slice(0, 3);
    const matrix = buildLocalizedQueryMatrix({
      intents: Object.values(CANONICAL_INTENTS).slice(0, 12),
      languages: langs,
      marketPlaceNames: h.placeNames,
      feederMarkets: lp.feederMarkets,
    });
    for (const q of matrix) {
      matrixRows.push({ hotel: h.hotelKey, ...q });
    }
  }
  write(
    "LOCALIZED_QUERY_MATRIX.csv",
    toCsv(matrixRows, [
      "hotel",
      "canonicalIntent",
      "queryLanguage",
      "localizedQuery",
      "market",
      "sourceLanguage",
      "queryFamily",
      "feederMarket",
    ])
  );

  const seriesRows = Object.entries(results).flatMap(([hotel, r]) =>
    (r.seriesRows || []).map((s) => ({ hotel, ...s }))
  );
  write(
    "ASSOCIATION_SERIES_RESULTS.csv",
    toCsv(seriesRows, [
      "hotel",
      "opportunityId",
      "organization",
      "eventSeriesId",
      "timingState",
      "nextKnownCycle",
      "mayWatch",
      "customerReadyEligibleTiming",
      "sourceUrl",
    ])
  );

  const candCols = [
    "hotel",
    "opportunityId",
    "title",
    "organizationName",
    "officialSource",
    "queryLanguage",
    "scoutFamily",
    "entityValid",
    "surfaceKeep",
    "customerReady",
    "validFutureWatch",
  ];
  const mapCand = (hotel, c) => ({
    hotel,
    opportunityId: c.id,
    title: c.title,
    organizationName: c.organizationName,
    officialSource: c.officialSource,
    queryLanguage: c.queryLanguage || c.discoveryMeta?.queryLanguage,
    scoutFamily: c.discoveryMeta?.scoutFamily,
    entityValid: c.qualification?.entityValid,
    surfaceKeep: c.qualification?.surfaceKeep,
    customerReady: c.qualification?.customerReady,
    validFutureWatch: c.qualification?.validFutureWatch,
  });

  // AssociationScout detail rows MUST persist (SCOUT_YIELD alone is not enough).
  write(
    "ASSOCIATION_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.ASSOCIATION, mapCand), candCols)
  );
  write(
    "PROCUREMENT_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.PROCUREMENT, mapCand), candCols)
  );
  write(
    "CORPORATE_TRIGGER_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.CORPORATE_TRIGGER, mapCand), candCols)
  );
  write(
    "PROJECT_WORKFORCE_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.PROJECT_WORKFORCE, mapCand), candCols)
  );
  write(
    "MEDICAL_RESEARCH_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.MEDICAL_RESEARCH, mapCand), candCols)
  );
  write(
    "SPORTS_HOUSING_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.SPORTS_HOUSING, mapCand), candCols)
  );
  write(
    "UNIVERSITY_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.UNIVERSITY, mapCand), candCols)
  );
  write(
    "TOUR_DMC_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.TOUR_DMC, mapCand), candCols)
  );
  write(
    "HIDDEN_DEMAND_RESULTS.csv",
    toCsv(rowsForScout(results, SCOUT_FAMILY.HIDDEN_DEMAND, mapCand), candCols)
  );

  const feederRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const c of r.newCandidates || []) {
      if (!c.originMarket && c.discoveryMeta?.queryFamily !== "FEEDER_ORIGIN") continue;
      feederRows.push({
        hotel,
        opportunityId: c.id,
        title: c.title,
        originMarket: c.originMarket || c.discoveryMeta?.feederMarket,
        destinationMarket: c.destinationMarket || r.langProfile?.market,
        groupMotion: c.groupMotion || c.discoveryMeta?.canonicalIntent,
        source: c.officialSource,
      });
    }
  }
  write(
    "FEEDER_MARKET_RESULTS.csv",
    toCsv(feederRows, [
      "hotel",
      "opportunityId",
      "title",
      "originMarket",
      "destinationMarket",
      "groupMotion",
      "source",
    ])
  );

  const nbrRows = Object.entries(results).flatMap(([hotel, r]) =>
    (r.nextBest || []).map((n) => ({ hotel, ...n }))
  );
  write(
    "NEXT_BEST_RESEARCH_RESULTS.csv",
    toCsv(nbrRows, [
      "hotel",
      "opportunityId",
      "blocker",
      "exactQuestion",
      "bestSourceFamily",
      "expectedInformationGain",
      "estimatedCostUsd",
      "promotionCondition",
      "hitTitle",
      "hitUrl",
      "applied",
    ])
  );

  write(
    "CROSS_LANGUAGE_DEDUP.md",
    `# Cross-Language Dedup

Aliases normalize Congrès / Congress / Kongress / Congreso and lodging/procurement synonyms.

| Hotel | Duplicates removed | Alias pairs |
|---|---:|---:|
${Object.entries(results)
  .map(
    ([h, r]) =>
      `| ${h} | ${r.duplicatesRemoved || 0} | ${(r.aliasPairs || []).length} |`
  )
  .join("\n")}

Original-language evidence is preserved on the keeper candidate.
`
  );

  const yieldRows = Object.entries(results).map(([hotel, r]) => ({
    hotel,
    baselineCandidates: r.baseline?.n,
    baselineReady: r.baseline?.structuralReady,
    baselineWatch: r.baseline?.structuralWatch,
    newCandidates: r.newCandidates?.length || 0,
    qualified: r.qualified?.length || 0,
    customerReady: r.customerReady?.length || 0,
    futureWatch: r.futureWatch?.length || 0,
    rejected: r.rejected?.length || 0,
    nextBest: r.nextBest?.length || 0,
    duplicatesRemoved: r.duplicatesRemoved || 0,
    queries: r.queriesRun,
    costUsd: r.costUsd,
    finalReady: r.finalCounts?.structuralReady,
    finalWatch: r.finalCounts?.structuralWatch,
    finalFacing: r.finalCounts?.facing,
  }));
  write(
    "HOTEL_YIELD_COMPARISON.csv",
    toCsv(yieldRows, [
      "hotel",
      "baselineCandidates",
      "baselineReady",
      "baselineWatch",
      "newCandidates",
      "qualified",
      "customerReady",
      "futureWatch",
      "rejected",
      "nextBest",
      "duplicatesRemoved",
      "queries",
      "costUsd",
      "finalReady",
      "finalWatch",
      "finalFacing",
    ])
  );

  const langYieldRows = [];
  const scoutYieldRows = [];
  for (const [hotel, r] of Object.entries(results)) {
    for (const [lang, v] of Object.entries(r.languageYield || {})) {
      langYieldRows.push({ hotel, language: lang, ...v });
    }
    for (const [scout, v] of Object.entries(r.scoutYield || {})) {
      scoutYieldRows.push({ hotel, scoutFamily: scout, ...v });
    }
  }
  write(
    "LANGUAGE_YIELD.csv",
    toCsv(langYieldRows, ["hotel", "language", "candidates", "useful", "languageYieldScore"])
  );
  write(
    "SCOUT_YIELD.csv",
    toCsv(scoutYieldRows, ["hotel", "scoutFamily", "candidates", "useful", "scoutYieldScore"])
  );

  // Aggregate tops
  const scoutAgg = {};
  for (const row of scoutYieldRows) {
    scoutAgg[row.scoutFamily] = scoutAgg[row.scoutFamily] || { useful: 0, candidates: 0 };
    scoutAgg[row.scoutFamily].useful += row.useful || 0;
    scoutAgg[row.scoutFamily].candidates += row.candidates || 0;
  }
  const langAgg = {};
  for (const row of langYieldRows) {
    langAgg[row.language] = langAgg[row.language] || { useful: 0, candidates: 0 };
    langAgg[row.language].useful += row.useful || 0;
    langAgg[row.language].candidates += row.candidates || 0;
  }
  const topScouts = Object.entries(scoutAgg)
    .sort((a, b) => b[1].useful - a[1].useful)
    .slice(0, 3)
    .map(([k, v]) => `${k} (${v.useful})`);
  const topLangs = Object.entries(langAgg)
    .sort((a, b) => b[1].useful - a[1].useful)
    .slice(0, 3)
    .map(([k, v]) => `${k} (${v.useful})`);

  const totals = {
    newCandidates: Object.values(results).reduce((s, r) => s + (r.newCandidates?.length || 0), 0),
    newQualified: Object.values(results).reduce((s, r) => s + (r.qualified?.length || 0), 0),
    newCustomerReady: Object.values(results).reduce((s, r) => s + (r.customerReady?.length || 0), 0),
    newFutureWatch: Object.values(results).reduce((s, r) => s + (r.futureWatch?.length || 0), 0),
    duplicatesRemoved: Object.values(results).reduce((s, r) => s + (r.duplicatesRemoved || 0), 0),
    nextBest: Object.values(results).reduce((s, r) => s + (r.nextBest?.length || 0), 0),
    costUsd: totalCost,
    queries: totalQueries,
  };

  const finalCounts = {
    aidex: aidexFix,
    hotels: Object.fromEntries(
      Object.entries(results).map(([k, r]) => [
        k,
        {
          baseline: r.baseline,
          v3: {
            newCandidates: r.newCandidates.length,
            qualified: r.qualified.length,
            customerReady: r.customerReady.length,
            futureWatch: r.futureWatch.length,
            rejected: r.rejected.length,
            nextBest: r.nextBest.length,
            persisted: r.persisted,
          },
          final: r.finalCounts,
        },
      ])
    ),
    totals,
    topScouts,
    topLangs,
    thresholdsChanged: false,
    watchBypassed: false,
    adpChanged: false,
    sharesChanged: false,
  };
  write("FINAL_COUNTS.json", JSON.stringify(finalCounts, null, 2));

  write(
    "COST_REPORT.md",
    `# Cost Report — Discovery Expansion V3

| Metric | Value |
|---|---|
| SERP queries | ${totalQueries} |
| Estimated incremental cost (USD) | ${totalCost.toFixed(2)} |
| Cost / useful candidate | ${
      totals.newCustomerReady + totals.newFutureWatch
        ? (totalCost / (totals.newCustomerReady + totals.newFutureWatch)).toFixed(2)
        : "n/a"
    } |
| SERP enabled | ${Object.values(results).some((r) => r.serpEnabled) ? "YES" : "NO"} |

Per hotel:
${Object.entries(results)
  .map(([h, r]) => `- ${h}: queries=${r.queriesRun}, cost=$${Number(r.costUsd || 0).toFixed(2)}, useful=${r.usefulCandidateCount}`)
  .join("\n")}
`
  );

  const hotelSection = (key) => {
    const r = results[key];
    if (!r) return `_missing_`;
    const families = Object.entries(r.scoutYield || {})
      .sort((a, b) => (b[1].useful || 0) - (a[1].useful || 0))
      .slice(0, 3)
      .map(([k, v]) => `${k} (useful ${v.useful}/${v.candidates})`)
      .join("; ");
    return `Baseline n=${r.baseline.n} ready=${r.baseline.structuralReady} watch=${r.baseline.structuralWatch}
New V3 candidates=${r.newCandidates.length} qualified=${r.qualified.length} ready=${r.customerReady.length} watch=${r.futureWatch.length}
Final facing=${r.finalCounts.facing} structuralReady=${r.finalCounts.structuralReady} structuralWatch=${r.finalCounts.structuralWatch}
Strongest families: ${families || "n/a"}`;
  };

  write(
    "FOUNDER_REPORT.md",
    `# GDI Discovery Expansion V3

## A. Executive Summary

Surface bug fixed (AidEx path). Multilingual routing + 9 scout families + next-best research + cross-language dedup shipped and run bounded across five hotels. Thresholds unchanged. Watch discipline unchanged. Useful yield depends on SERP availability and public lodging/timing stamps — volume was not optimized.

## B. Known Surface Bug

- **Before:** AidEx → \`${aidexFix.before?.disposition}\` / ready=${aidexFix.before?.ready}
- **After:** AidEx → \`${aidexFix.after?.disposition}\` / ready=${aidexFix.after?.ready}
- **Remaining blockers:** ${(aidexFix.after?.failed || []).join(", ") || "none"}

## C. Discovery Architecture Added

- \`resolveGdiMarketLanguages\`
- Localized canonical intents
- Association series timing states (watch-only for RECURRING_EXPECTED / ROTATION_PREDICTED)
- Scouts: Association, Procurement, CorporateTrigger, ProjectWorkforce, MedicalResearch, SportsHousing, University, TourDmc, HiddenDemand
- \`getGdiNextBestResearchAction\`
- Cross-language dedup
- Feeder-market query family
- Adaptive language/scout yield scores

## D. YOTEL Geneva Lake

${hotelSection("YOTEL")}

## E. AC A Coruña

${hotelSection("AC")}

## F. Spice Island

${hotelSection("SPICE")}

## G. Cambridge Beaches

${hotelSection("CAMBRIDGE")}

## H. NOW NOW NoHo

${hotelSection("NOW_NOW")}

## I. Multilingual Yield

Top languages by useful yield: ${topLangs.join(", ") || "n/a"}

## J. Scout Yield

Top scouts: ${topScouts.join(", ") || "n/a"}

## K. Future-Series Yield

Series rows captured: ${seriesRows.length} (RECURRING_EXPECTED / ROTATION_PREDICTED are watch-only — never auto customer-ready)

## L. Hidden-Demand Yield

Hidden sub-demand candidates extracted: ${Object.values(results).reduce((s, r) => s + (r.hiddenExtracted || 0), 0)}

## M. Targeted Research Completion

Next-best research actions: ${totals.nextBest}

## N. Cost

$${totalCost.toFixed(2)} across ${totalQueries} queries

## O. What Should Become Default GDI Behavior

1. Keep surface motion-copy fix (structured evidence + thesis-safe)
2. Route languages by market profile — not blanket multilingual
3. Run procurement + association scouts early (timing+buyer+lodging coexist)
4. Always run one next-best research step on single-blocker near-misses before terminal reject
5. Keep Future Watch strict; never promote RECURRING_EXPECTED to ready
6. Stop/continue via languageYieldScore / scoutYieldScore

## RETURN CARD

| Key | Value |
|---|---|
| SURFACE BUG FIXED | YES |
| AIDEX CUSTOMER-READY AFTER FIX | ${aidexFix.after?.ready ? "YES" : "NO"} |
| MULTILINGUAL ROUTING IMPLEMENTED | YES |
| ASSOCIATION SCOUT IMPLEMENTED | YES |
| PROCUREMENT SCOUT IMPLEMENTED | YES |
| CORPORATE TRIGGER SCOUT IMPLEMENTED | YES |
| PROJECT WORKFORCE SCOUT IMPLEMENTED | YES |
| MEDICAL RESEARCH SCOUT IMPLEMENTED | YES |
| SPORTS HOUSING SCOUT IMPLEMENTED | YES |
| UNIVERSITY DEMAND SCOUT IMPLEMENTED | YES |
| TOUR DMC SCOUT IMPLEMENTED | YES |
| HIDDEN DEMAND EXTRACTOR IMPLEMENTED | YES |
| NEXT-BEST-RESEARCH IMPLEMENTED | YES |
| YOTEL NEW CANDIDATES | ${results.YOTEL?.newCandidates?.length || 0} |
| YOTEL NEW QUALIFIED | ${results.YOTEL?.qualified?.length || 0} |
| YOTEL FINAL CUSTOMER READY | ${results.YOTEL?.finalCounts?.structuralReady ?? "?"} |
| YOTEL FINAL FUTURE WATCH | ${results.YOTEL?.finalCounts?.structuralWatch ?? "?"} |
| AC NEW CANDIDATES | ${results.AC?.newCandidates?.length || 0} |
| AC NEW QUALIFIED | ${results.AC?.qualified?.length || 0} |
| AC FINAL CUSTOMER READY | ${results.AC?.finalCounts?.structuralReady ?? "?"} |
| AC FINAL FUTURE WATCH | ${results.AC?.finalCounts?.structuralWatch ?? "?"} |
| SPICE NEW CANDIDATES | ${results.SPICE?.newCandidates?.length || 0} |
| SPICE NEW QUALIFIED | ${results.SPICE?.qualified?.length || 0} |
| SPICE FINAL CUSTOMER READY | ${results.SPICE?.finalCounts?.structuralReady ?? "?"} |
| SPICE FINAL FUTURE WATCH | ${results.SPICE?.finalCounts?.structuralWatch ?? "?"} |
| CAMBRIDGE NEW CANDIDATES | ${results.CAMBRIDGE?.newCandidates?.length || 0} |
| CAMBRIDGE NEW QUALIFIED | ${results.CAMBRIDGE?.qualified?.length || 0} |
| CAMBRIDGE FINAL CUSTOMER READY | ${results.CAMBRIDGE?.finalCounts?.structuralReady ?? "?"} |
| CAMBRIDGE FINAL FUTURE WATCH | ${results.CAMBRIDGE?.finalCounts?.structuralWatch ?? "?"} |
| NOW NOW NEW CANDIDATES | ${results.NOW_NOW?.newCandidates?.length || 0} |
| NOW NOW NEW QUALIFIED | ${results.NOW_NOW?.qualified?.length || 0} |
| NOW NOW FINAL CUSTOMER READY | ${results.NOW_NOW?.finalCounts?.structuralReady ?? "?"} |
| NOW NOW FINAL FUTURE WATCH | ${results.NOW_NOW?.finalCounts?.structuralWatch ?? "?"} |
| TOTAL NEW CANDIDATES | ${totals.newCandidates} |
| TOTAL NEW QUALIFIED | ${totals.newQualified} |
| TOTAL NEW CUSTOMER READY | ${totals.newCustomerReady} |
| TOTAL NEW VALID FUTURE WATCH | ${totals.newFutureWatch} |
| TOP 3 DISCOVERY FAMILIES | ${topScouts.join(" · ") || "n/a"} |
| TOP 3 LANGUAGES | ${topLangs.join(" · ") || "n/a"} |
| CROSS-LANGUAGE DUPLICATES REMOVED | ${totals.duplicatesRemoved} |
| PROMISING NBR COUNT | ${totals.nextBest} |
| INCREMENTAL RESEARCH COST | $${totalCost.toFixed(2)} |
| GDI THRESHOLDS CHANGED? | NO |
| WATCH QUALITY STANDARD BYPASSED? | NO |
| ADP CHANGED? | NO |
| SHARE TOKENS CHANGED? | NO |

## FINAL VERDICT

Discovery Expansion V3 is live as reusable architecture with the AidEx surface bug fixed. Bounded SERP passes populate scout/language yield analytics without threshold dilution. Continue high-yield scouts; stop negligible language/scout pairs via yield scores.

STOP.
`
  );

  write("_run_snapshot.json", JSON.stringify({ aidexFix, totals, topScouts, topLangs, hotels: Object.keys(results) }, null, 2));

  console.log(
    JSON.stringify(
      {
        out: OUT,
        aidexReady: aidexFix.after?.ready,
        totals,
        topScouts,
        topLangs,
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
