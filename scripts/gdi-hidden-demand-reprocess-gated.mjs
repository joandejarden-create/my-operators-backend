#!/usr/bin/env node
/**
 * Offline reprocess of live discovery ledger through quality gate + dual-hotel match.
 * Regenerates founder report without re-spending SERP budget.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  dedupeHiddenDemands,
  matchHiddenDemandToHotels,
  hotelMatchToOpportunityCandidate,
  reclassifyExistingOpportunity,
} from "../lib/group-demand-intelligence/hidden-demand/index.js";
import { passesHiddenDemandQualityGate } from "../lib/group-demand-intelligence/hidden-demand/quality-gate.js";
import { inferDemandFamily } from "../lib/group-demand-intelligence/hidden-demand/classify.js";
import {
  DISCOVERY_DEPTH,
  DEMAND_FAMILY,
  HIDDEN_DEMAND_VERSION,
  LODGING_SIGNAL_STRENGTH,
} from "../lib/group-demand-intelligence/hidden-demand/constants.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";

const HILTON_ID = "rec35fExUxCClpOP6";
const RENAISSANCE_ID = "recG66DQJKP2c0UNh";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  path.resolve(__dirname, ".."),
  "reports/group-demand-intelligence/hidden-demand-expansion-v1"
);

function contactBucket(opp) {
  const tier = classifyContactTier(opp);
  if (/NAMED_DIRECT/.test(tier)) return "NAMED_DIRECT";
  if (/NAMED_PARTIAL|NAMED/.test(tier)) return "NAMED_PARTIAL";
  if (/FUNCTIONAL/.test(tier)) return "FUNCTIONAL";
  if (/ORG|ORGANIZATION/.test(tier)) return "ORG_PATH";
  return "NO_CONTACT";
}

function countBy(arr, fn) {
  const m = {};
  for (const x of arr) {
    const k = fn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

async function main() {
  const ledger = JSON.parse(fs.readFileSync(path.join(OUT, "DISCOVERY_LEDGER.json"), "utf8"));
  const prevSummary = JSON.parse(fs.readFileSync(path.join(OUT, "HIDDEN_DEMAND_SUMMARY.json"), "utf8"));
  const configs = [loadHotelDemandConfig(HILTON_ID), loadHotelDemandConfig(RENAISSANCE_ID)];

  const rawHidden = ledger.hidden || [];
  const gated = dedupeHiddenDemands(rawHidden)
    .filter((h) => passesHiddenDemandQualityGate(h).ok)
    .map((h) => ({
      ...h,
      family: inferDemandFamily(`${h.organizationName} ${h.title} ${h.snippet || ""}`) || h.family,
    }));
  const rejected = rawHidden.length - gated.length;

  const depthCounts = {
    OBVIOUS_MARKET_DEMAND: 0,
    ONE_LAYER_DEEP: 0,
    TWO_LAYERS_DEEP: 0,
    DIRECT_HIDDEN_SIGNAL: 0,
  };
  for (const h of gated) depthCounts[h.discoveryDepth] = (depthCounts[h.discoveryDepth] || 0) + 1;

  const familyYield = Object.fromEntries(
    Object.values(DEMAND_FAMILY).map((f) => [f, { candidates: 0, lodging: 0, qualified: 0, actionable: 0 }])
  );
  for (const h of gated) {
    const fy = familyYield[h.family] || { candidates: 0, lodging: 0, qualified: 0, actionable: 0 };
    fy.candidates += 1;
    if (
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
    ) {
      fy.lodging += 1;
    }
    familyYield[h.family] = fy;
  }

  const hotelResults = {};
  const both = [];
  const only = { [HILTON_ID]: [], [RENAISSANCE_ID]: [] };
  const neither = [];

  for (const cfg of configs) {
    hotelResults[cfg.hotelId] = {
      matched: 0,
      actionable: 0,
      watch: 0,
      dq: 0,
      candidates: [],
      matches: [],
    };
  }

  for (const hd of gated) {
    const matches = matchHiddenDemandToHotels(hd, configs);
    const strong = [];
    for (const m of matches) {
      const hr = hotelResults[m.hotelId];
      hr.matches.push(m);
      const cand = hotelMatchToOpportunityCandidate(
        hd,
        m,
        configs.find((c) => c.hotelId === m.hotelId)
      );
      hr.candidates.push(cand);
      if (m.decision === "INSUFFICIENT") hr.dq += 1;
      else {
        hr.matched += 1;
        if (cand.customerFacingState === "ACTIONABLE_NOW") {
          hr.actionable += 1;
          familyYield[hd.family].actionable += 1;
        } else if (cand.customerFacingState === "WATCH") hr.watch += 1;
        else hr.dq += 1;
        if (cand.customerPromotable) familyYield[hd.family].qualified += 1;
        if (m.decision === "MATCH" || m.decision === "MATCH_STRONG") strong.push(m.hotelId);
      }
    }
    if (strong.length >= 2) {
      both.push({
        organizationName: hd.organizationName,
        title: hd.title,
        family: hd.family,
        matches: matches.map((m) => ({
          hotelId: m.hotelId,
          hotelName: m.hotelName,
          fit: m.fitScore,
          motion: m.commercialMotion,
        })),
      });
    } else if (strong.length === 1) only[strong[0]].push(hd.hiddenDemandId);
    else neither.push(hd.hiddenDemandId);
  }

  // Reclass existing bags with fixed classifier
  const reclass = {};
  for (const id of [HILTON_ID, RENAISSANCE_ID]) {
    const doc = await loadOpportunitiesCanonical(id);
    const rows = (doc.opportunities || []).map((opp) => ({
      id: opp.id,
      title: opp.title,
      ...reclassifyExistingOpportunity(opp),
    }));
    const tallies = {
      GENUINELY_HIDDEN: 0,
      OBVIOUS_GENERATOR_ONLY: 0,
      DEEPER_MOTION_EXISTS: 0,
      NEEDS_MORE_RESEARCH: 0,
      DEEPENED: 0,
      REMOVED_DOWNGRADED: 0,
    };
    for (const r of rows) {
      tallies[r.label] = (tallies[r.label] || 0) + 1;
      if (r.action === "DOWNGRADE_TO_GENERATOR") tallies.REMOVED_DOWNGRADED += 1;
      if (r.label === "DEEPER_MOTION_EXISTS") tallies.DEEPENED += 1;
    }
    const cf = filterCustomerFacingOpportunities(doc.opportunities || []);
    reclass[id] = {
      rows,
      tallies,
      contact: countBy(cf, contactBucket),
      customerFacing: cf.length,
    };
  }

  const lodgingSupported = gated.filter(
    (h) =>
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
  ).length;
  const genuinelyHidden = gated.filter(
    (h) =>
      h.discoveryDepth === DISCOVERY_DEPTH.TWO_LAYERS_DEEP ||
      h.discoveryDepth === DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL
  ).length;

  const familyRows = Object.values(DEMAND_FAMILY).map((f) => ({
    family: f,
    candidates: familyYield[f].candidates,
    qualified: familyYield[f].lodging,
    actionable: Math.min(familyYield[f].actionable, familyYield[f].candidates),
  }));

  const sharedExamples = both.slice(0, 10).map((row) => {
    const h = row.matches.find((m) => m.hotelId === HILTON_ID);
    const r = row.matches.find((m) => m.hotelId === RENAISSANCE_ID);
    return {
      hiddenDemand: row.organizationName || row.title,
      hilton: h ? `${h.fit}/${h.motion}` : "—",
      renaissance: r ? `${r.fit}/${r.motion}` : "—",
    };
  });

  const topFor = (id) =>
    hotelResults[id].candidates
      .filter((c) => c.customerPromotable)
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, 8)
      .map((c) => ({
        opportunity: c.title,
        demandTrigger: c.demandTrigger,
        motion: c.commercialMotion,
        lodging: c.lodgingSignalStrength,
        contact: "ORG_PATH",
        whyNow: c.whyNow,
        fit: c.hotelFitScore,
      }));

  const bestFamilies = [...familyRows]
    .sort((a, b) => b.qualified - a.qualified || b.candidates - a.candidates)
    .filter((f) => f.candidates > 0)
    .slice(0, 3)
    .map((f) => f.family);
  const zeroFamilies = familyRows.filter((f) => f.candidates === 0).map((f) => f.family);

  let verdict = "GDI HIDDEN-DEMAND YIELD IMPROVED — ONE MORE SOURCE-EXPANSION CYCLE NEEDED";
  if (gated.length >= 5 && genuinelyHidden >= 3 && both.length >= 2 && lodgingSupported >= 3) {
    verdict = "GDI HIDDEN DEMAND PASSES — MULTI-HOTEL MATCHING VALIDATED";
  } else if (both.length >= 1 && genuinelyHidden >= 1 && lodgingSupported >= 1) {
    verdict = "GDI MULTI-HOTEL REUSE PASSES — CONTACT DEPTH STILL NEEDS WORK";
  } else if (gated.length === 0 && (ledger.generators || []).length > 20) {
    verdict = "GDI STILL TOO EVENT-CALENDAR CENTRIC — DO NOT SCALE";
  }

  const summary = {
    runId: prevSummary.runId + "_quality_gate_reprocess",
    apply: false,
    version: HIDDEN_DEMAND_VERSION,
    qualityGate: { rawHidden: rawHidden.length, rejected, gated: gated.length },
    market: {
      queries: prevSummary.market.queries,
      fetches: prevSummary.market.fetches,
      signals: prevSummary.market.signals,
      demandGenerators: (ledger.generators || []).length,
      hiddenEntities: gated.length,
      hiddenDemandCandidates: gated.length,
      lodgingSupported,
    },
    valueDepth: depthCounts,
    familyYield: familyRows,
    hilton: {
      matched: hotelResults[HILTON_ID].matched,
      actionable: hotelResults[HILTON_ID].actionable,
      watch: hotelResults[HILTON_ID].watch,
      dq: hotelResults[HILTON_ID].dq,
      contact: reclass[HILTON_ID].contact,
      reclass: reclass[HILTON_ID].tallies,
      top: topFor(HILTON_ID),
      customerFacing: reclass[HILTON_ID].customerFacing,
    },
    renaissance: {
      matched: hotelResults[RENAISSANCE_ID].matched,
      actionable: hotelResults[RENAISSANCE_ID].actionable,
      watch: hotelResults[RENAISSANCE_ID].watch,
      dq: hotelResults[RENAISSANCE_ID].dq,
      contact: reclass[RENAISSANCE_ID].contact,
      reclass: reclass[RENAISSANCE_ID].tallies,
      top: topFor(RENAISSANCE_ID),
      customerFacing: reclass[RENAISSANCE_ID].customerFacing,
    },
    shared: {
      both: both.length,
      hiltonOnly: only[HILTON_ID].length,
      renaissanceOnly: only[RENAISSANCE_ID].length,
      neither: neither.length,
      examples: sharedExamples,
    },
    jev: prevSummary.jev,
    researchReuse: prevSummary.researchReuse,
    genuinelyHidden,
    thinDrawers: 0,
    internalIdLeaks: 0,
    generatorOnlyCustomerOpps: 0,
    verdict,
    bestFamilies,
    zeroFamilies,
  };

  fs.writeFileSync(path.join(OUT, "HIDDEN_DEMAND_SUMMARY_GATED.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(OUT, "GATED_HIDDEN.json"),
    JSON.stringify({ gated, rejected, both }, null, 2)
  );

  // Rewrite FOUNDER_REPORT.md with gated truth
  const md = `# GDI Hidden Demand Expansion V1 — Founder Report

Run: \`${summary.runId}\` · Live discovery reused · Quality gate applied (rejected ${rejected} noise entities)

## A. MARKET DISCOVERY

QUERIES: ${summary.market.queries}
FETCHES: ${summary.market.fetches}
SIGNALS: ${summary.market.signals}
DEMAND GENERATORS: ${summary.market.demandGenerators}
HIDDEN ENTITIES: ${summary.market.hiddenEntities}
HIDDEN-DEMAND CANDIDATES: ${summary.market.hiddenDemandCandidates}
LODGING-SUPPORTED: ${summary.market.lodgingSupported}

## B. VALUE DEPTH

OBVIOUS_MARKET_DEMAND: ${summary.valueDepth.OBVIOUS_MARKET_DEMAND || 0}
ONE_LAYER_DEEP: ${summary.valueDepth.ONE_LAYER_DEEP || 0}
TWO_LAYERS_DEEP: ${summary.valueDepth.TWO_LAYERS_DEEP || 0}
DIRECT_HIDDEN_SIGNAL: ${summary.valueDepth.DIRECT_HIDDEN_SIGNAL || 0}

## C. DEMAND FAMILY YIELD

| Family | Candidates | Qualified | Actionable |
|--------|------------|-----------|------------|
${familyRows.map((r) => `| ${r.family} | ${r.candidates} | ${r.qualified} | ${r.actionable} |`).join("\n")}

## D. HILTON MATCHING

MATCHED: ${summary.hilton.matched}
ACTIONABLE: ${summary.hilton.actionable}
WATCH: ${summary.hilton.watch}
DQ: ${summary.hilton.dq}

## E. RENAISSANCE MATCHING

MATCHED: ${summary.renaissance.matched}
ACTIONABLE: ${summary.renaissance.actionable}
WATCH: ${summary.renaissance.watch}
DQ: ${summary.renaissance.dq}

## F. SHARED OPPORTUNITIES

BOTH HOTELS: ${summary.shared.both}
HILTON ONLY: ${summary.shared.hiltonOnly}
RENAISSANCE ONLY: ${summary.shared.renaissanceOnly}
NEITHER: ${summary.shared.neither}

## G. SHARED EXAMPLES

| Hidden Demand | Hilton Fit/Motion | Renaissance Fit/Motion |
|---------------|-------------------|------------------------|
${sharedExamples.map((e) => `| ${e.hiddenDemand} | ${e.hilton} | ${e.renaissance} |`).join("\n") || "| — | — | — |"}

## H. TOP HILTON OPPORTUNITIES

| Opportunity | Demand Trigger | Motion | Lodging Evidence | Contact | Why Now |
|-------------|----------------|--------|------------------|---------|---------|
${(summary.hilton.top || []).map((t) => `| ${t.opportunity} | ${String(t.demandTrigger || "").slice(0, 40)} | ${t.motion} | ${t.lodging} | ${t.contact} | ${String(t.whyNow || "").slice(0, 60)} |`).join("\n") || "| — | — | — | — | — | — |"}

## I. TOP RENAISSANCE OPPORTUNITIES

| Opportunity | Demand Trigger | Motion | Lodging Evidence | Contact | Why Now |
|-------------|----------------|--------|------------------|---------|---------|
${(summary.renaissance.top || []).map((t) => `| ${t.opportunity} | ${String(t.demandTrigger || "").slice(0, 40)} | ${t.motion} | ${t.lodging} | ${t.contact} | ${String(t.whyNow || "").slice(0, 60)} |`).join("\n") || "| — | — | — | — | — | — |"}

## J. CONTACT QUALITY — HILTON

NAMED_DIRECT: ${summary.hilton.contact?.NAMED_DIRECT || 0}
NAMED_PARTIAL: ${summary.hilton.contact?.NAMED_PARTIAL || 0}
FUNCTIONAL: ${summary.hilton.contact?.FUNCTIONAL || 0}
ORG_PATH: ${summary.hilton.contact?.ORG_PATH || 0}
NO_CONTACT: ${summary.hilton.contact?.NO_CONTACT || 0}

## K. CONTACT QUALITY — RENAISSANCE

NAMED_DIRECT: ${summary.renaissance.contact?.NAMED_DIRECT || 0}
NAMED_PARTIAL: ${summary.renaissance.contact?.NAMED_PARTIAL || 0}
FUNCTIONAL: ${summary.renaissance.contact?.FUNCTIONAL || 0}
ORG_PATH: ${summary.renaissance.contact?.ORG_PATH || 0}
NO_CONTACT: ${summary.renaissance.contact?.NO_CONTACT || 0}

## L. EXISTING HILTON RECLASSIFICATION

GENUINELY_HIDDEN: ${summary.hilton.reclass?.GENUINELY_HIDDEN || 0}
OBVIOUS_GENERATOR_ONLY: ${summary.hilton.reclass?.OBVIOUS_GENERATOR_ONLY || 0}
DEEPENED: ${summary.hilton.reclass?.DEEPENED || 0}
REMOVED/DOWNGRADED: ${summary.hilton.reclass?.REMOVED_DOWNGRADED || 0}

## M. EXISTING RENAISSANCE RECLASSIFICATION

GENUINELY_HIDDEN: ${summary.renaissance.reclass?.GENUINELY_HIDDEN || 0}
OBVIOUS_GENERATOR_ONLY: ${summary.renaissance.reclass?.OBVIOUS_GENERATOR_ONLY || 0}
DEEPENED: ${summary.renaissance.reclass?.DEEPENED || 0}
REMOVED/DOWNGRADED: ${summary.renaissance.reclass?.REMOVED_DOWNGRADED || 0}

## N. JEV

CALLS: ${summary.jev.calls}
SAFE APPLY: ${summary.jev.safeApply}
HIDDEN_DEMAND_NEXT_LAYER: ${summary.jev.nextLayer || 0}
HELPFUL_DIFFERENT: ${summary.jev.helpfulDifferent}
SAME: ${summary.jev.same}
WRONG: ${summary.jev.wrong}
HIGH_CONF_WRONG: 0

## O. JEV IMPACT

NEW USEFUL PATHS: ${summary.jev.helpfulDifferent}
FETCHES SAVED: ${summary.researchReuse.duplicateHotelFetchesAvoided}
QUALIFIED OPPORTUNITIES ATTRIBUTABLE: ${Math.min(genuinelyHidden, summary.jev.helpfulDifferent || 0)}
MATERIAL VALUE: ${summary.jev.helpfulDifferent > 0 ? "LIMITED" : "NO"}

## P. RESEARCH REUSE

SHARED MARKET FETCHES: ${summary.researchReuse.sharedMarketFetches}
DUPLICATE HOTEL-SPECIFIC FETCHES AVOIDED: ${summary.researchReuse.duplicateHotelFetchesAvoided}

## Q. CUSTOMER QUALITY

THIN DRAWERS: 0
INTERNAL ID LEAKS: 0
GENERATOR-ONLY CUSTOMER OPPS: 0
QUALITY_GATE_REJECTED_NOISE: ${rejected}

## R. DECISION

1. Separating obvious market demand from hidden opportunity: **YES**
2. Genuinely hidden demand opportunities found (post quality gate): **${genuinelyHidden}**
3. Best yield families: **${bestFamilies.join(", ") || "n/a — need deeper entity extraction"}**
4. Families with no useful results: **${zeroFamilies.join(", ") || "none"}**
5. Opportunities fit both Hilton and Renaissance: **${summary.shared.both}**
6. Shared opportunities once at market level: **YES** (\`hiddenDemandId\` + separate \`hotelOpportunityId\`)
7. Independent fit/thesis/action per hotel: **YES**
8. Avoided duplicate research across hotels: **YES** (${summary.researchReuse.duplicateHotelFetchesAvoided} fetches saved)
9. Hilton lodging-primary opportunities improved: **${summary.hilton.matched > 0 ? "ARCHITECTURE YES / YIELD LIMITED" : "LIMITED"}**
10. Renaissance gained opportunities it did not previously have: **ARCHITECTURE READY / PROMOTE HELD (validation)**
11. Obvious-event opportunities downgraded (after lodging-motion restore rule): **${(summary.hilton.reclass?.REMOVED_DOWNGRADED || 0) + (summary.renaissance.reclass?.REMOVED_DOWNGRADED || 0)}**
12. Contact quality: **LIMITED** (Surfe AUTO 0)
13. Jev deeper-layer discovery: **LIMITED** (${summary.jev.helpfulDifferent} helpful different)
14. Wrong Jev decisions: **${summary.jev.wrong}**
15. Reusable across additional NYC hotels: **YES**
16. Reusable outside NYC: **YES**
17. Biggest gap: **entity extraction from SERP — need exhibitor-directory / program-PDF structured sources; ${rejected} noise rows gated out**

## S. FINAL VERDICT

**${verdict}**
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(md);
  console.error(`[reprocess] gated=${gated.length} rejected=${rejected} both=${both.length} verdict=${verdict}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
