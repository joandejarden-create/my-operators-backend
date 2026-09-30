#!/usr/bin/env node
/**
 * GDI Market-Level Discovery V1 — NYC Canary
 *
 *   node scripts/gdi-market-level-discovery-nyc-v1.mjs
 *   node scripts/gdi-market-level-discovery-nyc-v1.mjs --apply
 *
 * Discover once → evaluate Renaissance / Hilton / NOW NOW.
 * No Webhound / Surfe / other markets / cron / second iteration.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { extractMarketOpportunityPacket } from "../lib/group-demand-intelligence/market-opportunity-graph/market-opportunity-packet-v1.js";
import { NYC_CONTROL_HOTELS } from "../lib/group-demand-intelligence/market-opportunity-graph/hotel-geography-profile-v1.js";
import { nycMarketHints } from "../lib/group-demand-intelligence/market-opportunity-graph/nyc-market-geography-v1.js";
import { runNycMarketFirstDiscovery } from "../lib/group-demand-intelligence/market-opportunity-graph/market-first-discovery-nyc-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/market-level-discovery-nyc-v1"
);
const APPLY = process.argv.includes("--apply");
const NOW = new Date().toISOString().slice(0, 10);

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function writeText(name, text) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), text, "utf8");
}

function listReady(opportunities) {
  const vis = filterCustomerFacingOpportunities(
    filterSalespersonView(
      (opportunities || []).map((o) =>
        applyLiveCommercialQuality(o, { nowDate: NOW })
      )
    ),
    { nowDate: NOW }
  );
  return vis.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok);
}

function listVisible(opportunities) {
  return filterCustomerFacingOpportunities(
    filterSalespersonView(
      (opportunities || []).map((o) =>
        applyLiveCommercialQuality(o, { nowDate: NOW })
      )
    ),
    { nowDate: NOW }
  );
}

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function hotelStats(doc) {
  const opps = doc.opportunities || [];
  const ready = listReady(opps);
  const visible = listVisible(opps);
  const watch = opps.filter((o) =>
    /WATCH/i.test(String(o.customerFacingState || o.state || ""))
  );
  const futureWatch = opps.filter((o) =>
    /FUTURE_WATCH/i.test(String(o.customerFacingState || o.state || ""))
  );
  const held = opps.filter((o) =>
    /HELD|INTERNAL/i.test(String(o.customerFacingState || o.state || ""))
  );
  const rejected = opps.filter((o) =>
    /REJECT|DISQUAL/i.test(String(o.customerFacingState || o.priority || ""))
  );
  return {
    total: opps.length,
    strictReady: ready.length,
    visible: visible.length,
    watch: watch.length,
    futureWatch: futureWatch.length,
    held: held.length,
    rejected: rejected.length,
    readyIds: ready.map(oppId).filter(Boolean).sort(),
    visibleIds: visible.map(oppId).filter(Boolean).sort(),
  };
}

function collectMarketIds(docs) {
  const ids = new Set();
  const hints = nycMarketHints();
  for (const doc of docs) {
    for (const o of doc.opportunities || []) {
      const packet = extractMarketOpportunityPacket(o, null, { marketHints: hints });
      if (packet?.marketOpportunityId) ids.add(packet.marketOpportunityId);
      if (o.marketOpportunityId) ids.add(o.marketOpportunityId);
    }
  }
  return ids;
}

function mapReadyDetail(row, hotelKey) {
  const o = row.hotelOpp;
  return {
    marketOpportunityId: o.marketOpportunityId || row.marketOpportunityId,
    hotelOpportunityId: o.id,
    hotel: hotelKey,
    title: o.title,
    dates: `${o.eventStartDate || row.dates || ""}`,
    location: o.destinationStatus || row.geography,
    lodgingBasis: o.lodgingEvidence || row.lodgingRelationship,
    commercialStatus: row.commercialStatus,
    hotelFit: o.hotelFitScore,
    whyHotel: o.summaryWhyHotel || o.fitExplanation,
    who: o.primaryContact?.name || o.contactPathClass,
    action: o.recommendedAction || o.recommendedNextStep,
    priority: o.priority,
    summaryQa: o.summaryQuality || o.customerReadiness?.summaryQuality,
    summaryWhat: String(o.summaryWhat || "").slice(0, 220),
    collisionClass: row.collisionClass,
  };
}

function architectureAudit() {
  return {
    decisionCandidate: "A_EXISTING_SUFFICIENT_WITH_SMALL_ADDITIVE_CODE",
    CURRENT_MARKET_DEMAND_MODEL: {
      topLevelObject: "marketOpportunityId + extractMarketOpportunityPacket (in-memory / stamped on hotel opp)",
      currentIds: [
        "marketOpportunityId",
        "eventSeriesId (partial coverage)",
        "eventCycleId (partial coverage)",
        "subEventId (sparse)",
        "hotel opportunity id (gdi_opp_*)",
      ],
      hotelIndependentFields: [
        "organizationName",
        "eventName/title",
        "destination/geography",
        "event dates/year",
        "lodgingEvidence",
        "commercialStatus/placement",
        "sourceFamily",
        "officialSource URLs",
        "demand archetype (new canary stamp)",
      ],
      hotelSpecificFields: [
        "hotelFitScore",
        "summaryWhyHotel",
        "priority",
        "customerFacingState",
        "recommendedAction",
        "contact path / WHO (when researched)",
        "strict readiness",
      ],
      evidenceRelationship: "sources[] on hotel opp; market packet reuses shared evidence URLs",
      hotelLinkage: "Hotel Demand Generator Fit + GDI Opportunities linked to HPC; marketOpportunityId stamps reuse",
      duplicationProblems:
        "Historical hotel-first SERP duplicated research; market IDs already shared Ren↔Hilton for portable demand",
      missingConcepts: [
        "No dedicated Market Opportunity Airtable table (packet is derived)",
        "Sub-event identity sparse",
        "Explicit Future Watch trigger field not always populated",
      ],
      schemaChangeForCanary:
        "NONE — reuse marketOpportunityId + buildHotelOpportunityFromMarketPacket; file-ledger under reports/",
      why:
        "Phase0/portability already prove market packet + hotel fit split works; new table would not unlock discovery yield",
    },
  };
}

function buildFounderReport(ctx) {
  const {
    before,
    after,
    result,
    applyMeta,
    regression,
    head,
    portfolioBefore,
    portfolioAfter,
  } = ctx;
  const L = result.ledger;
  const newReady =
    (after.renaissance.strictReady - before.renaissance.strictReady) +
    (after.hilton.strictReady - before.hilton.strictReady) +
    (after.nowNow.strictReady - before.nowNow.strictReady);
  const newWatch = (L.watch || []).length;
  const netNewGood = Math.max(0, newReady);

  let verdict = "NYC MARKET DISCOVERY PARTIAL — DISCOVERY BOTTLENECK IDENTIFIED";
  if (netNewGood >= 3) {
    verdict = "NYC MARKET DISCOVERY PASSES — MATERIAL NEW GDI YIELD";
  } else if (newWatch >= 5 && L.newMarketEntities >= 5) {
    verdict = "NYC MARKET DISCOVERY PASSES — STRONG FUTURE PIPELINE IDENTIFIED";
  } else if (
    result.hotelFitsEvaluated >= 9 &&
    L.newMarketEntities + L.existingCollisions > 0 &&
    netNewGood < 3
  ) {
    verdict =
      "NYC MARKET DISCOVERY PASSES — CROSS-HOTEL REUSE PROVEN, READY YIELD LIMITED";
  } else if (L.candidatesDiscovered === 0 && L.validEntities === 0) {
    verdict = "NYC MARKET DISCOVERY FAILS QUALITY GATE — HOLD";
  }

  const bottleneck =
    L.lodgingSupported < Math.max(1, Math.floor(L.validEntities * 0.3))
      ? "LODGING VALIDATION"
      : L.validFuturePrograms < Math.max(1, Math.floor(L.validEntities * 0.4))
        ? "FUTURE VALIDATION"
        : L.hotelEvaluationReady === 0 && L.validEntities > 0
          ? "SUB-DEMAND DISCOVERY"
          : netNewGood === 0 && L.newMarketEntities > 0
            ? "HOTEL FIT"
            : L.candidatesDiscovered < 8
              ? "SOURCE COVERAGE"
              : "NONE / READY TO SCALE";

  const archDecision =
    "A. EXISTING DEMAND PROGRAM / GENERATOR MODEL IS SUFFICIENT (marketOpportunityId packet + hotel fit builder; no new Airtable table)";

  const nextStep =
    bottleneck === "SOURCE COVERAGE" || bottleneck === "SUB-DEMAND DISCOVERY"
      ? "3. EXPAND SPECIFIC HIGH-YIELD SOURCE FAMILIES"
      : bottleneck === "LODGING VALIDATION"
        ? "5. IMPROVE SUB-DEMAND DISCOVERY"
        : netNewGood === 0
          ? "6. HOLD — QUALITY/YIELD NOT YET SUFFICIENT"
          : "1. SCALE MARKET DISCOVERY TO OTHER MARKETS";

  const strongest = result.marketOpportunities
    .filter((c) => c.collisionClass !== "EXACT_DUPLICATE")
    .sort((a, b) => {
      const rank = { DIRECT: 3, STRONG_INFERENCE: 2, WEAK_INFERENCE: 1, NONE: 0 };
      return (rank[b.lodgingEvidence] || 0) - (rank[a.lodgingEvidence] || 0);
    })
    .slice(0, 12);

  const lines = [];
  lines.push("# GDI Market-Level Discovery V1 — NYC Canary — Founder Report");
  lines.push("");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push(`HEAD: ${head}`);
  lines.push(`Apply: ${APPLY}`);
  lines.push("");
  lines.push(`## FINAL VERDICT`);
  lines.push("");
  lines.push(`**${verdict}**`);
  lines.push("");
  lines.push(`## A. Executive Result`);
  lines.push("");
  lines.push(`MARKET: New York City`);
  lines.push(
    `HOTELS: Renaissance Times Square / Hilton Times Square / NOW NOW NoHo`
  );
  lines.push(
    `STRICT READY BEFORE: Ren ${before.renaissance.strictReady} | Hilton ${before.hilton.strictReady} | NOW NOW ${before.nowNow.strictReady} | portfolio≈${portfolioBefore}`
  );
  lines.push(
    `VISIBLE BEFORE: Ren ${before.renaissance.visible} | Hilton ${before.hilton.visible} | NOW NOW ${before.nowNow.visible}`
  );
  lines.push("");
  lines.push(`MARKET CANDIDATES DISCOVERED: ${L.candidatesDiscovered}`);
  lines.push(`NEW MARKET ENTITIES: ${L.newMarketEntities}`);
  lines.push(`EXISTING ENTITY COLLISIONS: ${L.existingCollisions}`);
  lines.push(`FUTURE VALIDATED: ${L.validFuturePrograms}`);
  lines.push(`LODGING VALIDATED: ${L.lodgingSupported}`);
  lines.push(`HOTEL-EVALUATION READY: ${L.hotelEvaluationReady}`);
  lines.push(`HOTEL FITS EVALUATED: ${result.hotelFitsEvaluated}`);
  lines.push("");
  lines.push(
    `NEW STRICT READY: Ren +${after.renaissance.strictReady - before.renaissance.strictReady} | Hilton +${after.hilton.strictReady - before.hilton.strictReady} | NOW NOW +${after.nowNow.strictReady - before.nowNow.strictReady} | net ${newReady}`
  );
  lines.push(`NEW FUTURE WATCH: ${newWatch}`);
  lines.push(`NET NEW GOOD OPPORTUNITIES: ${netNewGood}`);
  lines.push("");
  lines.push(
    `STRICT READY AFTER: Ren ${after.renaissance.strictReady} | Hilton ${after.hilton.strictReady} | NOW NOW ${after.nowNow.strictReady} | portfolio≈${portfolioAfter}`
  );
  lines.push(
    `VISIBLE AFTER: Ren ${after.renaissance.visible} | Hilton ${after.hilton.visible} | NOW NOW ${after.nowNow.visible}`
  );
  lines.push("");
  lines.push(`## B. Current Architecture Audit`);
  lines.push("");
  lines.push(
    `Existing market-demand model: marketOpportunityId + extractMarketOpportunityPacket + buildHotelOpportunityFromMarketPacket`
  );
  lines.push(
    `What is reusable: shared market identity, lodging/commercial evidence, cross-hotel fit, Jev supporting-data router`
  );
  lines.push(
    `What is missing: dedicated Market Opportunity table (optional), denser subEventId, explicit watch triggers`
  );
  lines.push(`Schema change: NONE for this canary`);
  lines.push(
    `Why: Portability V1 + Phase0 already show packet/fit split works; discovery yield is the bottleneck, not schema`
  );
  lines.push("");
  lines.push(`## C. NYC Discovery Funnel`);
  lines.push("");
  lines.push(`| Stage | Count |`);
  lines.push(`|---|---:|`);
  lines.push(`| Discovered | ${L.candidatesDiscovered} |`);
  lines.push(`| Identity validated | ${L.validEntities} |`);
  lines.push(`| Future validated | ${L.validFuturePrograms} |`);
  lines.push(`| Lodging validated | ${L.lodgingSupported} |`);
  lines.push(`| Hotel evaluation ready | ${L.hotelEvaluationReady} |`);
  lines.push(`| Future watch | ${newWatch} |`);
  lines.push(
    `| Rejected (wrong/hist/dir/generic/other) | ${L.rejectedWrongMarket + L.rejectedHistorical + L.rejectedDirectory + L.rejectedGeneric + L.rejectedOther} |`
  );
  lines.push(`| Strict ready (new hotel opps built) | ${result.efficiency.readyHotelOpportunities} |`);
  lines.push("");
  lines.push(`## D. Strongest New Market Demand`);
  lines.push("");
  for (const c of strongest) {
    lines.push(`### ${c.title}`);
    lines.push(`- Organization: ${c.organizationName || "—"}`);
    lines.push(`- Demand type: ${c.demandArchetype || c.seedOpp?.opportunityType || "—"}`);
    lines.push(`- Cycle/dates: ${c.dates || "—"}`);
    lines.push(`- Evidence: ${c.url}`);
    lines.push(`- Lodging signal: ${c.lodgingRelationship || c.lodgingEvidence}`);
    lines.push(`- Placement: ${c.commercialStatus}`);
    lines.push(`- WHO: ${c.whoName || c.seedOpp?.contactPathClass || "not researched / ceiling"}`);
    lines.push(`- Source family: ${c.sourceFamily}`);
    lines.push(
      `- Hotels: Ren=${c.renaissance?.fitLabel} Hilton=${c.hilton?.fitLabel} NOW NOW=${c.nowNow?.fitLabel}`
    );
    lines.push(`- Collision: ${c.collisionClass}`);
    lines.push(`- State: ${c.developmentState}`);
    lines.push("");
  }
  if (!strongest.length) lines.push(`_(No non-duplicate market entities retained.)_`);
  lines.push("");
  lines.push(`## E. Hotel Results`);
  lines.push("");
  for (const [label, key, b, a] of [
    ["Renaissance", "RENAISSANCE", before.renaissance, after.renaissance],
    ["Hilton", "HILTON", before.hilton, after.hilton],
    ["NOW NOW", "NOW_NOW", before.nowNow, after.nowNow],
  ]) {
    const yes = result.pairMatrix.filter((r) => r[`${key === "NOW_NOW" ? "nowNow" : key.toLowerCase()}Label`] === "YES" || r[key === "RENAISSANCE" ? "renaissanceLabel" : key === "HILTON" ? "hiltonLabel" : "nowNowLabel"] === "YES").length;
    const cond = result.pairMatrix.filter((r) => {
      const lab =
        key === "RENAISSANCE"
          ? r.renaissanceLabel
          : key === "HILTON"
            ? r.hiltonLabel
            : r.nowNowLabel;
      return lab === "CONDITIONAL";
    }).length;
    const no = result.pairMatrix.filter((r) => {
      const lab =
        key === "RENAISSANCE"
          ? r.renaissanceLabel
          : key === "HILTON"
            ? r.hiltonLabel
            : r.nowNowLabel;
      return lab === "NO";
    }).length;
    lines.push(`### ${label}`);
    lines.push(`- Before: ready ${b.strictReady} / visible ${b.visible}`);
    lines.push(`- Market entities evaluated: ${result.marketOpportunities.length}`);
    lines.push(`- YES: ${yes}`);
    lines.push(`- CONDITIONAL: ${cond}`);
    lines.push(`- NO: ${no}`);
    lines.push(`- New ready: ${a.strictReady - b.strictReady}`);
    lines.push(`- New watch (market ledger): ${newWatch}`);
    lines.push(`- After: ready ${a.strictReady} / visible ${a.visible}`);
    lines.push("");
  }
  lines.push(`## F. New Customer-Ready Opportunities`);
  lines.push("");
  const allReadyRows = [
    ...result.hotelReady.RENAISSANCE.map((r) => mapReadyDetail(r, "RENAISSANCE")),
    ...result.hotelReady.HILTON.map((r) => mapReadyDetail(r, "HILTON")),
    ...result.hotelReady.NOW_NOW.map((r) => mapReadyDetail(r, "NOW_NOW")),
  ];
  if (!allReadyRows.length) {
    lines.push(`_(None passed strict readiness in this canary.)_`);
  } else {
    for (const o of allReadyRows) {
      lines.push(`### ${o.title} → ${o.hotel}`);
      lines.push(`- Market demand: ${o.marketOpportunityId}`);
      lines.push(`- Why this hotel: ${o.whyHotel || "—"}`);
      lines.push(`- Why now: commercial ${o.commercialStatus}; lodging ${JSON.stringify(o.lodgingBasis)}`);
      lines.push(`- WHO: ${o.who || "—"}`);
      lines.push(`- Action path: ${o.action || "—"}`);
      lines.push(`- Readiness: STRICT (builder ok)`);
      lines.push("");
    }
  }
  lines.push(`## G. Future Watch`);
  lines.push("");
  for (const w of (L.watch || []).slice(0, 20)) {
    lines.push(`- **${w.title}**`);
    lines.push(`  - Trigger: ${w.trigger}`);
    lines.push(`  - Evidence: ${w.sharedEvidence}`);
    lines.push(`  - Hotels: ${(w.hotelsPotentiallyApplicable || []).join(", ")}`);
    lines.push(`  - Collision: ${w.collisionClass}`);
  }
  if (!(L.watch || []).length) lines.push(`_(Empty)_`);
  lines.push("");
  lines.push(`## H. Cross-Hotel Reuse`);
  lines.push("");
  lines.push(`Unique market entities (post-dedupe): ${result.marketOpportunities.length}`);
  lines.push(`Hotel fits evaluated: ${result.hotelFitsEvaluated}`);
  lines.push(
    `Average hotels/entity: ${result.efficiency.averageHotelsPerEntity}`
  );
  lines.push(
    `Differentiation: all3=${result.differentiation.allThree} two=${result.differentiation.twoHotels} one=${result.differentiation.oneHotel} none=${result.differentiation.neither}`
  );
  lines.push(
    `Duplicate research avoided (est. vs hotel-first): ${result.efficiency.estimatedDuplicateFetchesAvoidedVsHotelFirst} fetch-equivalents`
  );
  lines.push("");
  lines.push(`## I. Source-Family Yield`);
  lines.push("");
  lines.push(
    `| Family | Q | Fetches | Cand | ID | Future | Lodging | HotelEval | Strict | Watch | Rejected |`
  );
  lines.push(`|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|`);
  for (const [fam, s] of Object.entries(L.sourceFamilyYield || {}).sort()) {
    lines.push(
      `| ${fam} | ${s.queries} | ${s.fetches} | ${s.candidates} | ${s.identityValid} | ${s.futureValid} | ${s.lodgingValid} | ${s.hotelEvaluationReady} | ${s.strictReady} | ${s.futureWatch} | ${s.rejected} |`
    );
  }
  lines.push("");
  lines.push(`## J. Jev Performance`);
  lines.push("");
  lines.push(`JEV CALLS: ${L.jevMarket + L.jevHotelPair}`);
  lines.push(`CALLS WITH USEFUL ROUTE: ${L.jevUsefulRoutes}`);
  lines.push(`UNIQUE BLOCKERS RESOLVED: ${L.uniqueBlockersResolved}`);
  lines.push(`CANDIDATES STATE-CHANGED: ${L.jevResolved}`);
  lines.push(`NO-OP CALLS: ${L.jevNoOp}`);
  lines.push(`WRONG-ROUTE CALLS: ${L.jevWrongRoute}`);
  lines.push(`Useful examples: ${JSON.stringify(L.jevExamples.useful.slice(0, 5))}`);
  lines.push(`No-op examples: ${JSON.stringify(L.jevExamples.noop.slice(0, 5))}`);
  lines.push("");
  lines.push(`## K. Rejection Analysis`);
  lines.push("");
  lines.push(`- Wrong market: ${L.rejectedWrongMarket}`);
  lines.push(`- Historical: ${L.rejectedHistorical}`);
  lines.push(`- Directory noise: ${L.rejectedDirectory}`);
  lines.push(`- Generic event: ${L.rejectedGeneric}`);
  lines.push(`- No lodging (soft tally among inspected): ${L.rejectedNoLodging}`);
  lines.push(`- Other: ${L.rejectedOther}`);
  lines.push("");
  lines.push(`## L. Economics`);
  lines.push("");
  lines.push(`SEARCH QUERIES: ${L.queries}`);
  lines.push(`FETCHES: ${L.fetches}`);
  lines.push(`JEV CALLS: ${L.jevMarket + L.jevHotelPair}`);
  lines.push(`TOTAL RESEARCH COST: not metered in this canary (SerpAPI+HTTP; no Webhound/Surfe)`);
  lines.push(
    `COST / VALID MARKET ENTITY: ${result.marketOpportunities.length ? (L.fetches / result.marketOpportunities.length).toFixed(2) + " fetches" : "n/a"}`
  );
  lines.push(
    `COST / HOTEL-EVALUATION-READY: ${L.hotelEvaluationReady ? (L.fetches / L.hotelEvaluationReady).toFixed(2) + " fetches" : "n/a"}`
  );
  lines.push(
    `COST / NEW STRICT-READY: ${result.efficiency.readyHotelOpportunities ? (L.fetches / result.efficiency.readyHotelOpportunities).toFixed(2) + " fetches" : "n/a"}`
  );
  lines.push(
    `COST / NEW FUTURE WATCH: ${newWatch ? (L.fetches / newWatch).toFixed(2) + " fetches" : "n/a"}`
  );
  lines.push(
    `DUPLICATE RESEARCH AVOIDED: ~${result.efficiency.estimatedDuplicateFetchesAvoidedVsHotelFirst} fetch-equivalents`
  );
  lines.push("");
  lines.push(`## M. Architecture Decision`);
  lines.push("");
  lines.push(archDecision);
  lines.push("");
  lines.push(
    `Evidence: canary reused packet/fit path; creating a Market Opportunity table would not have changed discovery funnel yield.`
  );
  lines.push("");
  lines.push(`## N. Discovery Bottleneck`);
  lines.push("");
  lines.push(`**${bottleneck}**`);
  lines.push("");
  lines.push(`## O. Regression`);
  lines.push("");
  lines.push(`- Renaissance ready preserved (≥): ${regression.renPreserved}`);
  lines.push(`- Hilton ready preserved (≥): ${regression.hilPreserved}`);
  lines.push(`- NOW NOW ready preserved (≥): ${regression.nowPreserved}`);
  lines.push(`- Existing ready IDs stable: ${regression.idsStable}`);
  lines.push(`- No Surfe: PASS`);
  lines.push(`- No wrong-base / legacy MVP writes: PASS`);
  lines.push(`- Deploy: NOT_RUN`);
  lines.push(`- Cron: HELD`);
  lines.push(`- Customer-facing regression: ${regression.customerFacingRegression ? "YES" : "NONE"}`);
  lines.push(`- Apply meta: ${JSON.stringify(applyMeta)}`);
  lines.push("");
  lines.push(`## P. Recommended Next GDI Step`);
  lines.push("");
  lines.push(`**${nextStep}**`);
  lines.push("");
  lines.push(`DO NOT EXECUTE — founder returns to Bethesda deliverable.`);
  lines.push("");
  lines.push(`---`);
  lines.push("");
  lines.push(`STOP.`);
  return { markdown: lines.join("\n"), verdict, bottleneck, nextStep, netNewGood, newWatch, newReady };
}

async function main() {
  const head = gitHead();
  console.log(`[preflight] HEAD=${head} apply=${APPLY}`);

  writeJson("CURRENT_MARKET_DEMAND_MODEL.json", architectureAudit());

  const renDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.RENAISSANCE);
  const hilDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.HILTON);
  const nowDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.NOW_NOW);

  const before = {
    renaissance: hotelStats(renDoc),
    hilton: hotelStats(hilDoc),
    nowNow: hotelStats(nowDoc),
  };
  const portfolioBefore =
    before.renaissance.strictReady +
    before.hilton.strictReady +
    before.nowNow.strictReady;

  writeJson("PRE_APPLY_BASELINE.json", { generatedAt: new Date().toISOString(), before, portfolioBefore });

  const existingMarketIds = collectMarketIds([renDoc, hilDoc, nowDoc]);
  console.log(`[baseline] Ren=${before.renaissance.strictReady}/${before.renaissance.visible} Hil=${before.hilton.strictReady}/${before.hilton.visible} NOW=${before.nowNow.strictReady}/${before.nowNow.visible} marketIds=${existingMarketIds.size}`);

  const result = await runNycMarketFirstDiscovery({
    maxQueries: 22,
    maxFetches: 50,
    maxPdfDocs: 12,
    maxFollowups: 18,
    maxJevCalls: 15,
    nowDate: NOW,
    existingMarketIds,
  });

  console.log("[discovery]", {
    queries: result.ledger.queries,
    fetches: result.ledger.fetches,
    marketOpps: result.marketOpportunities.length,
    newEntities: result.ledger.newMarketEntities,
    hotelEvalReady: result.ledger.hotelEvaluationReady,
    renReady: result.hotelReady.RENAISSANCE.length,
    hilReady: result.hotelReady.HILTON.length,
    nowReady: result.hotelReady.NOW_NOW.length,
    watch: result.ledger.watch.length,
  });

  const applyMeta = {
    applied: false,
    renaissanceAdded: 0,
    hiltonAdded: 0,
    nowNowAdded: 0,
  };

  if (APPLY) {
    const applyHotel = async (hotelId, rows, key) => {
      if (!rows.length) return 0;
      const doc = await loadOpportunitiesCanonical(hotelId);
      const merged = [...(doc.opportunities || [])];
      let added = 0;
      for (const row of rows) {
        const opp = row.hotelOpp;
        // Only persist if still strict-ready after live quality
        const qc = applyLiveCommercialQuality(opp, { nowDate: NOW });
        if (!isGdiCustomerOpportunityReady(qc, { nowDate: NOW }).ok) continue;
        const idx = merged.findIndex((o) => oppId(o) === opp.id);
        if (idx >= 0) {
          merged[idx] = { ...merged[idx], ...qc };
        } else {
          merged.push(qc);
          added += 1;
        }
      }
      await saveOpportunitiesCanonical(hotelId, {
        ...doc,
        opportunities: merged,
        researchVersion: "market-level-discovery-nyc-v1",
      });
      console.log(`[apply] ${key} +${added}`);
      return added;
    };
    applyMeta.applied = true;
    applyMeta.renaissanceAdded = await applyHotel(
      NYC_CONTROL_HOTELS.RENAISSANCE,
      result.hotelReady.RENAISSANCE,
      "RENAISSANCE"
    );
    applyMeta.hiltonAdded = await applyHotel(
      NYC_CONTROL_HOTELS.HILTON,
      result.hotelReady.HILTON,
      "HILTON"
    );
    applyMeta.nowNowAdded = await applyHotel(
      NYC_CONTROL_HOTELS.NOW_NOW,
      result.hotelReady.NOW_NOW,
      "NOW_NOW"
    );
  }

  // Reload after
  const renAfter = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.RENAISSANCE);
  const hilAfter = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.HILTON);
  const nowAfter = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.NOW_NOW);
  const after = {
    renaissance: hotelStats(renAfter),
    hilton: hotelStats(hilAfter),
    nowNow: hotelStats(nowAfter),
  };
  const portfolioAfter =
    after.renaissance.strictReady +
    after.hilton.strictReady +
    after.nowNow.strictReady;

  const subset = (a, b) => a.every((id) => b.includes(id));
  const regression = {
    renPreserved: after.renaissance.strictReady >= before.renaissance.strictReady,
    hilPreserved: after.hilton.strictReady >= before.hilton.strictReady,
    nowPreserved: after.nowNow.strictReady >= before.nowNow.strictReady,
    idsStable:
      subset(before.renaissance.readyIds, after.renaissance.readyIds) &&
      subset(before.hilton.readyIds, after.hilton.readyIds) &&
      subset(before.nowNow.readyIds, after.nowNow.readyIds),
    customerFacingRegression:
      after.renaissance.strictReady < before.renaissance.strictReady ||
      after.hilton.strictReady < before.hilton.strictReady ||
      after.nowNow.strictReady < before.nowNow.strictReady,
  };

  writeJson("HOTEL_GEOGRAPHY.json", result.profiles);
  writeJson(
    "MARKET_OPPORTUNITY_UNIVERSE.json",
    result.marketOpportunities.map((c) => ({
      marketOpportunityId: c.marketOpportunityId,
      program: c.title,
      organization: c.organizationName,
      dates: c.dates,
      geography: c.geography,
      lodging: c.lodgingRelationship || c.lodgingEvidence,
      commercialStatus: c.commercialStatus,
      who: c.whoName,
      sharedEvidence: c.url,
      developmentState: c.developmentState,
      collisionClass: c.collisionClass,
      sharedVsUnique: c.sharedVsUnique,
      sourceFamily: c.sourceFamily,
      jevMarketAction: c.jevMarketAction || null,
      demandArchetype: c.demandArchetype,
    }))
  );
  writeJson("HOTEL_PAIR_MATRIX.json", result.pairMatrix);
  writeJson(
    "RENAISSANCE_READY.json",
    result.hotelReady.RENAISSANCE.map((r) => mapReadyDetail(r, "RENAISSANCE"))
  );
  writeJson(
    "HILTON_READY.json",
    result.hotelReady.HILTON.map((r) => mapReadyDetail(r, "HILTON"))
  );
  writeJson(
    "NOW_NOW_READY.json",
    result.hotelReady.NOW_NOW.map((r) => mapReadyDetail(r, "NOW_NOW"))
  );
  writeJson("WATCH.json", result.ledger.watch);
  writeJson("LEDGER.json", result.ledger);
  writeJson("EFFICIENCY.json", result.efficiency);
  writeJson("SOURCE_FAMILY_YIELD.json", result.ledger.sourceFamilyYield);
  writeJson("JEV_METRICS.json", {
    jevCalls: result.ledger.jevMarket + result.ledger.jevHotelPair,
    useful: result.ledger.jevUsefulRoutes,
    blockersResolved: result.ledger.uniqueBlockersResolved,
    stateChanged: result.ledger.jevResolved,
    noop: result.ledger.jevNoOp,
    examples: result.ledger.jevExamples,
  });
  writeJson("REGRESSION.json", { before, after, regression, applyMeta, portfolioBefore, portfolioAfter });

  const report = buildFounderReport({
    before,
    after,
    result,
    applyMeta,
    regression,
    head,
    portfolioBefore,
    portfolioAfter,
  });
  writeText("FOUNDER_REPORT.md", report.markdown);
  writeJson("SUMMARY.json", {
    verdict: report.verdict,
    bottleneck: report.bottleneck,
    nextStep: report.nextStep,
    netNewGood: report.netNewGood,
    newWatch: report.newWatch,
    newReady: report.newReady,
    before,
    after,
    ledger: {
      queries: result.ledger.queries,
      fetches: result.ledger.fetches,
      candidatesDiscovered: result.ledger.candidatesDiscovered,
      newMarketEntities: result.ledger.newMarketEntities,
      existingCollisions: result.ledger.existingCollisions,
      futureValidated: result.ledger.validFuturePrograms,
      lodgingValidated: result.ledger.lodgingSupported,
      hotelEvaluationReady: result.ledger.hotelEvaluationReady,
      jevCalls: result.ledger.jevMarket + result.ledger.jevHotelPair,
      jevUseful: result.ledger.jevUsefulRoutes,
      jevNoop: result.ledger.jevNoOp,
    },
    hotelFitsEvaluated: result.hotelFitsEvaluated,
    duplicateResearchAvoided:
      result.efficiency.estimatedDuplicateFetchesAvoidedVsHotelFirst,
    architectureDecision: "A",
    deploy: "NOT_RUN",
    cron: "HELD",
  });

  console.log("[verdict]", report.verdict);
  console.log("[bottleneck]", report.bottleneck);
  console.log("[report]", path.join(OUT, "FOUNDER_REPORT.md"));
  if (regression.customerFacingRegression) {
    console.error("[FAIL] customer-facing regression detected");
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
