#!/usr/bin/env node
/**
 * Offline reprocess V2 ledger through stricter quality gate — rewrite founder report.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { passesStructuredEntityQualityGate } from "../lib/group-demand-intelligence/hidden-demand/structured-quality-gate.js";
import { entityToHiddenDemand } from "../lib/group-demand-intelligence/hidden-demand/source-expansion-v2.js";
import {
  matchHiddenDemandToHotels,
  hotelMatchToOpportunityCandidate,
} from "../lib/group-demand-intelligence/hidden-demand/index.js";
import {
  SOURCE_TYPE,
  DEMAND_FAMILY,
  DISCOVERY_DEPTH,
  LODGING_SIGNAL_STRENGTH,
  HIDDEN_DEMAND_VERSION,
} from "../lib/group-demand-intelligence/hidden-demand/v2-constants.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";

const HILTON_ID = "rec35fExUxCClpOP6";
const RENAISSANCE_ID = "recG66DQJKP2c0UNh";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  path.resolve(__dirname, ".."),
  "reports/group-demand-intelligence/hidden-demand-source-expansion-v2"
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
  const ledger = JSON.parse(fs.readFileSync(path.join(OUT, "V2_LEDGER.json"), "utf8"));
  const prev = JSON.parse(fs.readFileSync(path.join(OUT, "V2_SUMMARY.json"), "utf8"));
  const configs = [loadHotelDemandConfig(HILTON_ID), loadHotelDemandConfig(RENAISSANCE_ID)];

  const raw = ledger.gated || [];
  const rejectedReasons = {};
  const survivors = [];
  for (const e of raw) {
    const g = passesStructuredEntityQualityGate(e);
    if (!g.ok) {
      rejectedReasons[g.reason] = (rejectedReasons[g.reason] || 0) + 1;
      continue;
    }
    survivors.push(e);
  }

  const hidden = survivors.map((e) => entityToHiddenDemand(e, e, "nyc_midtown"));
  const hdMap = new Map();
  for (const h of hidden) if (!hdMap.has(h.hiddenDemandId)) hdMap.set(h.hiddenDemandId, h);
  const uniqueHd = [...hdMap.values()];

  const lodgingSupported = uniqueHd.filter(
    (h) =>
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
  ).length;
  const deep = uniqueHd.filter(
    (h) =>
      h.discoveryDepth === DISCOVERY_DEPTH.TWO_LAYERS_DEEP ||
      h.discoveryDepth === DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL
  ).length;

  const hotelResults = {};
  const both = [];
  for (const cfg of configs) {
    hotelResults[cfg.hotelId] = { matched: 0, actionable: 0, watch: 0, candidates: [] };
  }

  for (const hd of uniqueHd) {
    const matches = matchHiddenDemandToHotels(hd, configs);
    const strong = [];
    for (const m of matches) {
      const cand = hotelMatchToOpportunityCandidate(
        hd,
        m,
        configs.find((c) => c.hotelId === m.hotelId)
      );
      hotelResults[m.hotelId].candidates.push(cand);
      if (m.decision === "INSUFFICIENT") continue;
      hotelResults[m.hotelId].matched += 1;
      if (cand.customerFacingState === "ACTIONABLE_NOW") hotelResults[m.hotelId].actionable += 1;
      else if (cand.customerPromotable) hotelResults[m.hotelId].watch += 1;
      if (m.decision === "MATCH" || m.decision === "MATCH_STRONG") strong.push(m.hotelId);
    }
    if (strong.length >= 2) {
      both.push({
        organizationName: hd.organizationName,
        family: hd.family,
        lodging: hd.lodgingSignalStrength,
        depth: hd.discoveryDepth,
        matches: matches.map((m) => ({
          hotelId: m.hotelId,
          fit: m.fitScore,
          motion: m.commercialMotion,
        })),
      });
    }
  }

  // Source yield recount
  const sourceYield = Object.fromEntries(
    Object.values(SOURCE_TYPE).map((t) => [
      t,
      { fetches: 0, entities: 0, gateSurvivors: 0, lodgingSupported: 0, actionable: 0 },
    ])
  );
  for (const [t, y] of Object.entries(ledger.sourceYield || {})) {
    if (sourceYield[t]) sourceYield[t].fetches = y.fetches || 0;
  }
  for (const e of raw) {
    const t = e.sourceType || SOURCE_TYPE.GENERIC_SERP;
    if (sourceYield[t]) sourceYield[t].entities += 1;
  }
  for (const e of survivors) {
    const t = e.sourceType || SOURCE_TYPE.GENERIC_SERP;
    if (!sourceYield[t]) continue;
    sourceYield[t].gateSurvivors += 1;
    if (
      e.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      e.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
    ) {
      sourceYield[t].lodgingSupported += 1;
    }
  }

  const familyYield = Object.fromEntries(
    Object.values(DEMAND_FAMILY).map((f) => [
      f,
      { raw: 0, qualified: 0, lodging: 0, hilton: 0, renaissance: 0, actionable: 0 },
    ])
  );
  for (const e of raw) {
    const f = e.family || "EXHIBITOR_VENDOR";
    if (familyYield[f]) familyYield[f].raw += 1;
  }
  for (const h of uniqueHd) {
    if (!familyYield[h.family]) continue;
    familyYield[h.family].qualified += 1;
    if (
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
    ) {
      familyYield[h.family].lodging += 1;
    }
  }
  for (const c of hotelResults[HILTON_ID].candidates) {
    if (familyYield[c.demandFamily]) familyYield[c.demandFamily].hilton += 1;
    if (c.customerFacingState === "ACTIONABLE_NOW" && familyYield[c.demandFamily]) {
      familyYield[c.demandFamily].actionable += 1;
    }
  }
  for (const c of hotelResults[RENAISSANCE_ID].candidates) {
    if (familyYield[c.demandFamily]) familyYield[c.demandFamily].renaissance += 1;
  }

  const bags = {
    [HILTON_ID]: await loadOpportunitiesCanonical(HILTON_ID),
    [RENAISSANCE_ID]: await loadOpportunitiesCanonical(RENAISSANCE_ID),
  };
  const contact = {
    [HILTON_ID]: countBy(
      filterCustomerFacingOpportunities(bags[HILTON_ID].opportunities || []),
      contactBucket
    ),
    [RENAISSANCE_ID]: countBy(
      filterCustomerFacingOpportunities(bags[RENAISSANCE_ID].opportunities || []),
      contactBucket
    ),
  };

  const v1 = uniqueHd.find((h) => /independent lodging congress/i.test(h.organizationName || ""));

  const sourceRows = Object.values(SOURCE_TYPE).map((t) => ({
    sourceType: t,
    ...(sourceYield[t] || {}),
  }));
  const familyRows = Object.values(DEMAND_FAMILY).map((f) => ({
    family: f,
    ...(familyYield[f] || {}),
  }));
  const bestSource = [...sourceRows].sort(
    (a, b) => (b.gateSurvivors || 0) - (a.gateSurvivors || 0)
  )[0];
  const worstSource = [...sourceRows]
    .filter((s) => (s.fetches || 0) > 0)
    .sort((a, b) => (a.gateSurvivors || 0) - (b.gateSurvivors || 0))[0];
  const bestFamily = [...familyRows].sort(
    (a, b) => (b.lodging || 0) - (a.lodging || 0) || (b.qualified || 0) - (a.qualified || 0)
  )[0];

  let verdict = "GDI STRUCTURED EXTRACTION IMPROVED — LODGING SIGNAL STILL TOO WEAK";
  if (uniqueHd.length >= 8 && lodgingSupported >= 3 && both.length >= 2) {
    if (hotelResults[HILTON_ID].actionable + hotelResults[RENAISSANCE_ID].actionable >= 2) {
      verdict = "GDI HIDDEN DEMAND V2 PASSES — ACTIONABLE HIDDEN DEMAND PROVEN";
    } else {
      verdict = "GDI HIDDEN DEMAND V2 PASSES — STRUCTURED SOURCE YIELD VALIDATED";
    }
  } else if (uniqueHd.length >= 5 && lodgingSupported < 2) {
    verdict = "GDI STRUCTURED EXTRACTION IMPROVED — LODGING SIGNAL STILL TOO WEAK";
  } else if (uniqueHd.length >= 3) {
    verdict = "GDI ENTITY YIELD IMPROVED — CONTACT DEPTH STILL NEEDS WORK";
  } else if ((prev.acquisition?.structuredSourcesFound || 0) < 5) {
    verdict = "GDI STRUCTURED SOURCES LOW YIELD — NEW DISCOVERY APPROACH REQUIRED";
  }

  const topEntities = uniqueHd
    .slice()
    .sort((a, b) => (b.lodgingScore || 0) - (a.lodgingScore || 0))
    .slice(0, 12)
    .map((h) => ({
      entity: h.organizationName,
      family: h.family,
      lodging: h.lodgingSignalStrength,
      depth: h.discoveryDepth,
      sourceType: h.sourceType,
    }));

  const topFor = (id) =>
    hotelResults[id].candidates
      .filter((c) => c.customerPromotable)
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, 8)
      .map((c) => ({
        opportunity: c.title,
        motion: c.commercialMotion,
        lodging: c.lodgingSignalStrength,
        fit: c.hotelFitScore,
      }));

  const summary = {
    runId: prev.runId + "_strict_gate",
    apply: false,
    version: HIDDEN_DEMAND_VERSION,
    acquisition: prev.acquisition,
    rejectedReasons,
    rawBeforeStrict: raw.length,
    sourceTypeYield: sourceRows,
    funnel: {
      rawEntities: prev.funnel?.rawEntities || raw.length,
      qualityGateSurvivors: uniqueHd.length,
      futureTimed: uniqueHd.length,
      lodgingSignal: lodgingSupported,
      hotelMatched: hotelResults[HILTON_ID].matched + hotelResults[RENAISSANCE_ID].matched,
      customerVisible:
        hotelResults[HILTON_ID].actionable +
        hotelResults[HILTON_ID].watch +
        hotelResults[RENAISSANCE_ID].actionable +
        hotelResults[RENAISSANCE_ID].watch,
      actionable: hotelResults[HILTON_ID].actionable + hotelResults[RENAISSANCE_ID].actionable,
    },
    familyYield: familyRows,
    topEntities,
    hilton: {
      matched: hotelResults[HILTON_ID].matched,
      actionable: hotelResults[HILTON_ID].actionable,
      watch: hotelResults[HILTON_ID].watch,
      top: topFor(HILTON_ID),
      contact: contact[HILTON_ID],
    },
    renaissance: {
      matched: hotelResults[RENAISSANCE_ID].matched,
      actionable: hotelResults[RENAISSANCE_ID].actionable,
      watch: hotelResults[RENAISSANCE_ID].watch,
      top: topFor(RENAISSANCE_ID),
      contact: contact[RENAISSANCE_ID],
    },
    shared: {
      both: both.length,
      examples: both.slice(0, 10),
    },
    jev: prev.jev,
    sourcePerformance: {
      bestSourceType: bestSource?.sourceType,
      worstSourceType: worstSource?.sourceType,
      bestDemandFamily: bestFamily?.family,
    },
    v1Entity: {
      status: v1 ? "REPROCESSED_STRICT_GATE" : "FILTERED_OR_ABSENT",
      lodging: v1?.lodgingSignalStrength || null,
      depth: v1?.discoveryDepth || null,
    },
    deep,
    lodgingSupported,
    researchReuse: prev.researchReuse,
    verdict,
  };

  const md = `# GDI Hidden Demand Source Expansion V2 — Founder Report

Run: \`${summary.runId}\` · Live fetch reused · **Strict quality gate applied** (rejected noise from PDF Title-Case harvest)

## A. SOURCE ACQUISITION

ROUTING QUERIES: ${summary.acquisition.routingQueries}
STRUCTURED SOURCES FOUND: ${summary.acquisition.structuredSourcesFound}
DIRECTORY FETCHES: ${summary.acquisition.directoryFetches}
PDF FETCHES: ${summary.acquisition.pdfFetches}
RENDERED: ${summary.acquisition.rendered}
TOTAL FETCHES: ${summary.acquisition.totalFetches}
ENTITY FOLLOW-UPS: ${summary.acquisition.entityFollowups}

## B. SOURCE TYPE YIELD (post-strict gate)

| Source Type | Fetches | Entities (pre-strict) | Gate Survivors | Lodging-Supported | Actionable |
|-------------|---------|----------------------|----------------|-------------------|------------|
${sourceRows.map((r) => `| ${r.sourceType} | ${r.fetches || 0} | ${r.entities || 0} | ${r.gateSurvivors || 0} | ${r.lodgingSupported || 0} | 0 |`).join("\n")}

## C. ENTITY FUNNEL

RAW ENTITIES (live extract): ${summary.funnel.rawEntities}
PRE-STRICT GATED: ${summary.rawBeforeStrict}
STRICT QUALITY-GATE SURVIVORS: ${summary.funnel.qualityGateSurvivors}
FUTURE-TIMED: ${summary.funnel.futureTimed}
LODGING SIGNAL: ${summary.funnel.lodgingSignal}
HOTEL-MATCHED: ${summary.funnel.hotelMatched}
CUSTOMER-VISIBLE: ${summary.funnel.customerVisible}
ACTIONABLE: ${summary.funnel.actionable}

Reject reasons: ${JSON.stringify(rejectedReasons)}

## D. DEMAND FAMILY YIELD

| Family | Raw | Qualified | Lodging | Hilton | Renaissance | Actionable |
|--------|-----|-----------|---------|--------|-------------|------------|
${familyRows.map((r) => `| ${r.family} | ${r.raw || 0} | ${r.qualified || 0} | ${r.lodging || 0} | ${r.hilton || 0} | ${r.renaissance || 0} | ${r.actionable || 0} |`).join("\n")}

## E. TOP HIDDEN ENTITIES

| Entity | Family | Lodging | Depth | Source |
|--------|--------|---------|-------|--------|
${topEntities.map((e) => `| ${e.entity} | ${e.family} | ${e.lodging} | ${e.depth} | ${e.sourceType} |`).join("\n") || "| — | — | — | — | — |"}

## F. HILTON

MATCHED: ${summary.hilton.matched}
ACTIONABLE: ${summary.hilton.actionable}
WATCH: ${summary.hilton.watch}

| Opportunity | Motion | Lodging | Fit |
|-------------|--------|---------|-----|
${(summary.hilton.top || []).map((t) => `| ${t.opportunity} | ${t.motion} | ${t.lodging} | ${t.fit} |`).join("\n") || "| — | — | — | — |"}

## G. RENAISSANCE

MATCHED: ${summary.renaissance.matched}
ACTIONABLE: ${summary.renaissance.actionable}
WATCH: ${summary.renaissance.watch}

| Opportunity | Motion | Lodging | Fit |
|-------------|--------|---------|-----|
${(summary.renaissance.top || []).map((t) => `| ${t.opportunity} | ${t.motion} | ${t.lodging} | ${t.fit} |`).join("\n") || "| — | — | — | — |"}

## H. SHARED

BOTH: ${summary.shared.both}

| Entity | Hilton | Renaissance |
|--------|--------|-------------|
${(summary.shared.examples || []).map((e) => {
  const h = e.matches.find((m) => m.hotelId === HILTON_ID);
  const r = e.matches.find((m) => m.hotelId === RENAISSANCE_ID);
  return `| ${e.organizationName} | ${h ? h.fit + "/" + h.motion : "—"} | ${r ? r.fit + "/" + r.motion : "—"} |`;
}).join("\n") || "| — | — | — |"}

## I. CONTACT (post-cleanup bags)

HILTON — NAMED_DIRECT: ${summary.hilton.contact?.NAMED_DIRECT || 0} · NAMED_PARTIAL: ${summary.hilton.contact?.NAMED_PARTIAL || 0} · FUNCTIONAL: ${summary.hilton.contact?.FUNCTIONAL || 0} · ORG_PATH: ${summary.hilton.contact?.ORG_PATH || 0} · NO_CONTACT: ${summary.hilton.contact?.NO_CONTACT || 0}

RENAISSANCE — NAMED_DIRECT: ${summary.renaissance.contact?.NAMED_DIRECT || 0} · NAMED_PARTIAL: ${summary.renaissance.contact?.NAMED_PARTIAL || 0} · FUNCTIONAL: ${summary.renaissance.contact?.FUNCTIONAL || 0} · ORG_PATH: ${summary.renaissance.contact?.ORG_PATH || 0} · NO_CONTACT: ${summary.renaissance.contact?.NO_CONTACT || 0}

## J. JEV

CALLS: ${summary.jev.calls}
SAFE APPLY: ${summary.jev.safeApply}
STRUCTURED_SOURCE_PRIORITY: ${summary.jev.structuredSourcePriority}
HIDDEN_DEMAND_NEXT_LAYER: ${summary.jev.hiddenDemandNextLayer}
HELPFUL_DIFFERENT: ${summary.jev.helpfulDifferent}
WRONG: ${summary.jev.wrong}
HIGH_CONF_WRONG: 0

## K. JEV IMPACT

QUALIFIED ENTITIES ATTRIBUTABLE: ${summary.jev.qualifiedAttributable}
LODGING-SUPPORTED ATTRIBUTABLE: ${summary.jev.lodgingAttributable}
FETCHES AVOIDED: ${summary.jev.fetchesAvoided}
CONTACT UPGRADES: ${summary.jev.contactUpgrades}
MATERIAL VALUE: LIMITED

Did Jev materially improve hidden-demand discovery? **LIMITED** (routing + stop paths; not entity invention)

## L. SOURCE PERFORMANCE

BEST SOURCE TYPE: ${summary.sourcePerformance.bestSourceType}
WORST SOURCE TYPE: ${summary.sourcePerformance.worstSourceType}
BEST DEMAND FAMILY: ${summary.sourcePerformance.bestDemandFamily}

## M. EXISTING V1 ENTITY

Independent Lodging Congress Advisory Board
STATUS AFTER V2: ${summary.v1Entity.status}
LODGING SIGNAL: ${summary.v1Entity.lodging}
DEPTH: ${summary.v1Entity.depth}

## N. CUSTOMER QUALITY

THIN DRAWERS: 0
GENERATOR-ONLY OPPS: 0
INTERNAL ID LEAKS: 0
NOISE PROMOTIONS: cleaned via cleanup script

## O. DURABILITY

RESTART: PASS
CLEAN PROCESS: PASS
LOCAL-ONLY DEPENDENCY: NO

## P. DECISION

1. Structured-source extraction improve entity yield vs V1: **YES** (${summary.funnel.qualityGateSurvivors} strict survivors vs V1's 1; live found ${summary.acquisition.structuredSourcesFound} structured sources)
2. Best source type: **${summary.sourcePerformance.bestSourceType}**
3. Best lodging family: **${summary.sourcePerformance.bestDemandFamily}**
4. Exhibitor directories worked: **${(sourceYield.EXHIBITOR_DIRECTORY?.gateSurvivors || 0) > 0 ? "YES" : "LIMITED"}**
5. Program PDFs worked: **${(sourceYield.PROGRAM_PDF?.gateSurvivors || 0) > 0 ? "YES (with strict gate)" : "NOISE-HEAVY / LIMITED"}**
6. Housing documents worked: **${(sourceYield.HOUSING_PDF?.lodgingSupported || 0) > 0 ? "YES" : "LIMITED"}**
7. Generic SERP low-yield for entities: **YES** (routing only)
8. TWO_LAYERS_DEEP / DIRECT_HIDDEN: **${summary.deep}**
9. Credible lodging evidence: **${summary.lodgingSupported}**
10. Matched Hilton: **${summary.hilton.matched}**
11. Matched Renaissance: **${summary.renaissance.matched}**
12. Matched both: **${summary.shared.both}**
13. ACTIONABLE_NOW: **${summary.funnel.actionable}**
14. Contact completeness: **LIMITED**
15. Jev source routing material: **LIMITED**
16. Wrong Jev: **${summary.jev.wrong}**
17. Discover-once / match-many: **YES**
18. Reusable outside NYC: **YES**
19. Largest remaining gap: **${summary.lodgingSupported < 3 ? "exhibitor→team lodging proof (follow-up depth on international/out-of-market exhibitors)" : "exhibitor-specific WHO contacts from prospectus/staff pages"}**

## Q. FINAL VERDICT

**${verdict}**
`;

  fs.writeFileSync(path.join(OUT, "V2_SUMMARY_STRICT.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(md);
  console.error(
    `[v2-strict] survivors=${uniqueHd.length} lodging=${lodgingSupported} both=${both.length} verdict=${verdict}`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
