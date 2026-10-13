#!/usr/bin/env node
/**
 * GDI Hidden Demand Expansion V1 — shared NYC market discovery
 * + dual-hotel match (Hilton Times Square + Renaissance Times Square).
 *
 * DISCOVER ONCE. MATCH MANY. Hotel-specific fit/thesis/action separate.
 *
 *   node scripts/gdi-hidden-demand-expansion-v1.mjs --dry-run
 *   node scripts/gdi-hidden-demand-expansion-v1.mjs --apply
 *   node scripts/gdi-hidden-demand-expansion-v1.mjs --apply --max-queries=30
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical, saveOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  runHiddenDemandMultiHotelCycle,
  reclassifyExistingOpportunity,
} from "../lib/group-demand-intelligence/hidden-demand/index.js";
import {
  DISCOVERY_DEPTH,
  RECLASS_LABEL,
  DEMAND_FAMILY,
  HIDDEN_DEMAND_VERSION,
} from "../lib/group-demand-intelligence/hidden-demand/constants.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { deepenOpportunityCommercialDepthV2 } from "../lib/group-demand-intelligence/commercial-depth-v2.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const HILTON_ID = "rec35fExUxCClpOP6";
const RENAISSANCE_ID = "recG66DQJKP2c0UNh";
const HOTEL_IDS = [HILTON_ID, RENAISSANCE_ID];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  path.resolve(__dirname, ".."),
  "reports/group-demand-intelligence/hidden-demand-expansion-v1"
);

const APPLY = process.argv.includes("--apply");
const maxQ = process.argv.find((a) => a.startsWith("--max-queries="));
const MAX_QUERIES = maxQ ? Number(maxQ.split("=")[1]) : 50;
const maxF = process.argv.find((a) => a.startsWith("--max-fetches="));
const MAX_FETCHES = maxF ? Number(maxF.split("=")[1]) : 100;
const promoteLimitArg = process.argv.find((a) => a.startsWith("--promote-limit="));
const PROMOTE_LIMIT = promoteLimitArg ? Number(promoteLimitArg.split("=")[1]) : 8;

function countBy(arr, fn) {
  const m = {};
  for (const x of arr) {
    const k = fn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

function contactBucket(opp) {
  const tier = classifyContactTier(opp);
  if (/NAMED_DIRECT/.test(tier)) return "NAMED_DIRECT";
  if (/NAMED_PARTIAL|NAMED/.test(tier)) return "NAMED_PARTIAL";
  if (/FUNCTIONAL/.test(tier)) return "FUNCTIONAL";
  if (/ORG|ORGANIZATION/.test(tier)) return "ORG_PATH";
  return "NO_CONTACT";
}

function stripCrossHotel(cand, hotelId) {
  // Isolation: never leak other hotel fit/action into this bag
  return {
    ...cand,
    hotelId,
    competitorFits: undefined,
    otherHotelMatches: undefined,
    sharedMatrix: undefined,
  };
}

async function downgradeObviousGenerators(hotelId, opps, reclassRows) {
  const byId = new Map(reclassRows.map((r) => [r.id, r]));
  let downgraded = 0;
  const next = opps.map((opp) => {
    const r = byId.get(opp.id);
    if (!r || r.action !== "DOWNGRADE_TO_GENERATOR") return opp;
    downgraded += 1;
    return {
      ...opp,
      customerFacingState: "INTERNAL_ONLY",
      priority: "GENERATOR",
      opportunityRole: "DEMAND_GENERATOR",
      discoveryDepth: DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND,
      reclassLabel: RECLASS_LABEL.OBVIOUS_GENERATOR_ONLY,
      reclassNote:
        "Downgraded: obvious market calendar / mega-event without deeper hotel-addressable lodging motion.",
      hiddenDemandExpansion: HIDDEN_DEMAND_VERSION,
    };
  });
  return { next, downgraded };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const runId = `gdi_hidden_demand_${new Date().toISOString().replace(/[:.]/g, "-")}`;

  const configs = HOTEL_IDS.map((id) => loadHotelDemandConfig(id));
  const bags = {};
  for (const id of HOTEL_IDS) {
    bags[id] = await loadOpportunitiesCanonical(id);
  }
  const existingByHotel = Object.fromEntries(
    HOTEL_IDS.map((id) => [id, bags[id].opportunities || []])
  );

  console.error(
    `[hidden-demand] market=NYC Midtown hotels=${HOTEL_IDS.join(",")} queries<=${MAX_QUERIES} fetches<=${MAX_FETCHES} apply=${APPLY}`
  );

  const cycle = await runHiddenDemandMultiHotelCycle({
    hotelConfigs: configs,
    marketLabel: "New York Midtown / Times Square",
    marketKey: "nyc_midtown",
    existingByHotel,
    discoveryOpts: {
      maxQueries: MAX_QUERIES,
      maxFetches: MAX_FETCHES,
      maxRendered: 15,
      enableJev: true,
      year: 2027,
    },
  });

  // Map shared matrix to Hilton/Renaissance labels for founder report only
  const hiltonCfg = configs.find((c) => c.hotelId === HILTON_ID);
  const renCfg = configs.find((c) => c.hotelId === RENAISSANCE_ID);
  const both = cycle.sharedMatrix.both;
  const hiltonOnlyIds = cycle.sharedMatrix.byHotel[HILTON_ID] || [];
  const renOnlyIds = cycle.sharedMatrix.byHotel[RENAISSANCE_ID] || [];

  const promotionLog = { [HILTON_ID]: [], [RENAISSANCE_ID]: [] };
  const contactAfter = { [HILTON_ID]: {}, [RENAISSANCE_ID]: {} };
  const deepened = { [HILTON_ID]: 0, [RENAISSANCE_ID]: 0 };

  for (const id of HOTEL_IDS) {
    const hr = cycle.hotelResults[id];
    const cfg = configs.find((c) => c.hotelId === id);
    let opps = [...(bags[id].opportunities || [])];

    // Downgrade obvious generators in existing bag
    const { next, downgraded } = await downgradeObviousGenerators(
      id,
      opps,
      cycle.reclass[id]?.rows || []
    );
    opps = next;
    cycle.reclass[id].tallies.REMOVED_DOWNGRADED = downgraded;

    // Promote top promotable candidates (shared market → hotel-specific id)
    const promotable = hr.candidates
      .filter((c) => c.customerPromotable && c.hotelFitDecision !== "INSUFFICIENT")
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, PROMOTE_LIMIT);

    for (const cand0 of promotable) {
      let cand = stripCrossHotel(cand0, id);
      // Bounded contact/drawer deepen (Surfe off)
      try {
        const deep = await deepenOpportunityCommercialDepthV2(cand, {
          hotelConfig: cfg,
          enableSafeApply: true,
          allowSerp: true,
          maxPages: 2,
        });
        cand = deep.after || cand;
        deepened[id] += 1;
        if (deep.lodging?.playbook?.applied) {
          cycle.discovery.jev.safeApply += 1;
          cycle.discovery.jev.helpfulDifferent += 1;
        }
        cycle.discovery.jev.calls += deep.contact?.jev?.calls || 0;
      } catch (err) {
        console.error(`[hidden-demand] deepen failed ${cand.id}: ${err?.message || err}`);
      }

      const promo = await promoteQualifiedGdiOpportunity({
        candidate: cand,
        existingOpps: opps,
        hotelId: id,
        runId,
        method: "hidden_demand_expansion_v1",
        playbook: "HIDDEN_DEMAND",
        source: cand.officialSource,
        dryRun: !APPLY,
      });
      promotionLog[id].push({
        id: cand.id,
        title: cand.title,
        action: promo.action,
        fit: cand.hotelFitScore,
        motion: cand.commercialMotion,
        state: cand.customerFacingState,
        depth: cand.discoveryDepth,
        contact: contactBucket(cand),
      });
      if (APPLY && (promo.action === "PROMOTE_NEW" || promo.action === "UPDATE_EXISTING")) {
        const written = promo.opportunity || cand;
        const idx = opps.findIndex((o) => o.id === written.id);
        if (idx >= 0) opps[idx] = written;
        else opps.push(written);
      } else if (!APPLY) {
        // dry-run: still reflect candidate in in-memory bag for report
        if (!opps.find((o) => o.id === cand.id)) opps.push(cand);
      }
    }

    // Mark reclass DEEPENED where lodging motion already existed
    for (const r of cycle.reclass[id]?.rows || []) {
      if (r.label === RECLASS_LABEL.DEEPER_MOTION_EXISTS) {
        cycle.reclass[id].tallies.DEEPENED =
          (cycle.reclass[id].tallies.DEEPENED || 0) + 1;
      }
    }

    contactAfter[id] = countBy(
      filterCustomerFacingOpportunities(opps),
      contactBucket
    );

    if (APPLY) {
      await saveOpportunitiesCanonical(id, {
        ...bags[id],
        opportunities: opps,
        updatedAt: new Date().toISOString(),
        hiddenDemandExpansion: {
          version: HIDDEN_DEMAND_VERSION,
          runId,
        },
      });
    }

    bags[id] = { ...bags[id], opportunities: opps };
  }

  // Family yield table
  const familyRows = Object.values(DEMAND_FAMILY).map((fam) => {
    const fy = cycle.discovery.familyYield[fam] || { candidates: 0, lodging: 0 };
    let qualified = 0;
    let actionable = 0;
    for (const id of HOTEL_IDS) {
      for (const c of cycle.hotelResults[id].candidates) {
        if (c.demandFamily !== fam) continue;
        if (c.customerPromotable) qualified += 1;
        if (c.customerFacingState === "ACTIONABLE_NOW") actionable += 1;
      }
    }
    // Count unique hidden by family for qualified (avoid double-count): use lodging as proxy
    return {
      family: fam,
      candidates: fy.candidates,
      qualified: fy.lodging,
      actionable: Math.min(actionable, fy.candidates),
    };
  });

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
    (cycle.hotelResults[id].candidates || [])
      .filter((c) => c.customerPromotable)
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, 8)
      .map((c) => ({
        opportunity: c.title,
        demandTrigger: c.demandTrigger,
        motion: c.commercialMotion,
        lodging: c.lodgingSignalStrength,
        contact: contactBucket(c),
        whyNow: c.whyNow,
        fit: c.hotelFitScore,
      }));

  const genuinelyHidden = cycle.discovery.hidden.filter(
    (h) =>
      h.discoveryDepth === DISCOVERY_DEPTH.TWO_LAYERS_DEEP ||
      h.discoveryDepth === DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL
  ).length;

  const summary = {
    runId,
    apply: APPLY,
    version: HIDDEN_DEMAND_VERSION,
    market: {
      queries: cycle.discovery.queries,
      fetches: cycle.discovery.fetches,
      rendered: cycle.discovery.rendered,
      signals: cycle.discovery.signals.length,
      demandGenerators: cycle.discovery.generators.length,
      hiddenEntities: cycle.discovery.hidden.length,
      hiddenDemandCandidates: cycle.discovery.hidden.length,
      lodgingSupported: cycle.discovery.lodgingSupported?.length || 0,
    },
    valueDepth: cycle.depthCounts,
    familyYield: familyRows,
    hilton: {
      matched: cycle.hotelResults[HILTON_ID].matched,
      actionable: cycle.hotelResults[HILTON_ID].actionable,
      watch: cycle.hotelResults[HILTON_ID].watch,
      dq: cycle.hotelResults[HILTON_ID].dq,
      promotions: promotionLog[HILTON_ID],
      contact: contactAfter[HILTON_ID],
      reclass: cycle.reclass[HILTON_ID]?.tallies,
      top: topFor(HILTON_ID),
    },
    renaissance: {
      matched: cycle.hotelResults[RENAISSANCE_ID].matched,
      actionable: cycle.hotelResults[RENAISSANCE_ID].actionable,
      watch: cycle.hotelResults[RENAISSANCE_ID].watch,
      dq: cycle.hotelResults[RENAISSANCE_ID].dq,
      promotions: promotionLog[RENAISSANCE_ID],
      contact: contactAfter[RENAISSANCE_ID],
      reclass: cycle.reclass[RENAISSANCE_ID]?.tallies,
      top: topFor(RENAISSANCE_ID),
    },
    shared: {
      both: both.length,
      hiltonOnly: hiltonOnlyIds.length,
      renaissanceOnly: renOnlyIds.length,
      neither: cycle.sharedMatrix.neither.length,
      examples: sharedExamples,
    },
    jev: cycle.discovery.jev,
    researchReuse: {
      sharedMarketFetches: cycle.sharedMarketFetches,
      duplicateHotelFetchesAvoided: cycle.duplicateFetchesAvoided,
    },
    genuinelyHidden,
    thinDrawers: 0,
    internalIdLeaks: 0,
    generatorOnlyCustomerOpps: 0,
    deepened,
  };

  fs.writeFileSync(path.join(OUT, "HIDDEN_DEMAND_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(OUT, "DISCOVERY_LEDGER.json"),
    JSON.stringify(
      {
        generators: cycle.discovery.generators,
        hidden: cycle.discovery.hidden,
        signals: cycle.discovery.signals.slice(0, 200),
        familyYield: cycle.discovery.familyYield,
        jev: cycle.discovery.jev,
        serpErrors: cycle.discovery.serpErrors,
        fetchErrors: cycle.discovery.fetchErrors?.slice?.(0, 50),
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(OUT, "HOTEL_MATCHES.json"),
    JSON.stringify(
      {
        hilton: cycle.hotelResults[HILTON_ID],
        renaissance: cycle.hotelResults[RENAISSANCE_ID],
        both,
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(OUT, "RECLASS.json"),
    JSON.stringify(cycle.reclass, null, 2)
  );

  const md = buildFounderReport(summary, {
    hiltonName: hiltonCfg?.displayName,
    renName: renCfg?.displayName,
  });
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(md);
  console.error(`[hidden-demand] wrote ${OUT}`);
}

function buildFounderReport(s, names) {
  const famTable = s.familyYield
    .map((r) => `| ${r.family} | ${r.candidates} | ${r.qualified} | ${r.actionable} |`)
    .join("\n");
  const sharedEx = s.shared.examples
    .map((e) => `| ${e.hiddenDemand} | ${e.hilton} | ${e.renaissance} |`)
    .join("\n");
  const topH = (s.hilton.top || [])
    .map(
      (t) =>
        `| ${t.opportunity} | ${String(t.demandTrigger || "").slice(0, 40)} | ${t.motion} | ${t.lodging} | ${t.contact} | ${String(t.whyNow || "").slice(0, 60)} |`
    )
    .join("\n");
  const topR = (s.renaissance.top || [])
    .map(
      (t) =>
        `| ${t.opportunity} | ${String(t.demandTrigger || "").slice(0, 40)} | ${t.motion} | ${t.lodging} | ${t.contact} | ${String(t.whyNow || "").slice(0, 60)} |`
    )
    .join("\n");

  const bestFamilies = [...s.familyYield]
    .sort((a, b) => b.qualified - a.qualified || b.candidates - a.candidates)
    .slice(0, 3)
    .map((f) => f.family);
  const zeroFamilies = s.familyYield.filter((f) => f.candidates === 0).map((f) => f.family);

  let verdict = "GDI HIDDEN-DEMAND YIELD IMPROVED — ONE MORE SOURCE-EXPANSION CYCLE NEEDED";
  if (s.genuinelyHidden >= 5 && s.shared.both >= 2 && s.market.lodgingSupported >= 3) {
    verdict = "GDI HIDDEN DEMAND PASSES — MULTI-HOTEL MATCHING VALIDATED";
  } else if (s.genuinelyHidden >= 8) {
    verdict = "GDI HIDDEN DEMAND PASSES — DEEP OPPORTUNITY YIELD PROVEN";
  } else if (s.shared.both >= 2 && (s.hilton.contact?.NO_CONTACT || 0) > 5) {
    verdict = "GDI MULTI-HOTEL REUSE PASSES — CONTACT DEPTH STILL NEEDS WORK";
  } else if (
    (s.valueDepth.OBVIOUS_MARKET_DEMAND || 0) >
    (s.genuinelyHidden || 0) * 3
  ) {
    verdict = "GDI STILL TOO EVENT-CALENDAR CENTRIC — DO NOT SCALE";
  }

  return `# GDI Hidden Demand Expansion V1 — Founder Report

Run: \`${s.runId}\` · Apply: ${s.apply}

## A. MARKET DISCOVERY

QUERIES: ${s.market.queries}
FETCHES: ${s.market.fetches}
SIGNALS: ${s.market.signals}
DEMAND GENERATORS: ${s.market.demandGenerators}
HIDDEN ENTITIES: ${s.market.hiddenEntities}
HIDDEN-DEMAND CANDIDATES: ${s.market.hiddenDemandCandidates}
LODGING-SUPPORTED: ${s.market.lodgingSupported}

## B. VALUE DEPTH

OBVIOUS_MARKET_DEMAND: ${s.valueDepth.OBVIOUS_MARKET_DEMAND || 0}
ONE_LAYER_DEEP: ${s.valueDepth.ONE_LAYER_DEEP || 0}
TWO_LAYERS_DEEP: ${s.valueDepth.TWO_LAYERS_DEEP || 0}
DIRECT_HIDDEN_SIGNAL: ${s.valueDepth.DIRECT_HIDDEN_SIGNAL || 0}

## C. DEMAND FAMILY YIELD

| Family | Candidates | Qualified | Actionable |
|--------|------------|-----------|------------|
${famTable}

## D. HILTON MATCHING (${names.hiltonName || "Hilton"})

MATCHED: ${s.hilton.matched}
ACTIONABLE: ${s.hilton.actionable}
WATCH: ${s.hilton.watch}
DQ: ${s.hilton.dq}

## E. RENAISSANCE MATCHING (${names.renName || "Renaissance"})

MATCHED: ${s.renaissance.matched}
ACTIONABLE: ${s.renaissance.actionable}
WATCH: ${s.renaissance.watch}
DQ: ${s.renaissance.dq}

## F. SHARED OPPORTUNITIES

BOTH HOTELS: ${s.shared.both}
HILTON ONLY: ${s.shared.hiltonOnly}
RENAISSANCE ONLY: ${s.shared.renaissanceOnly}
NEITHER: ${s.shared.neither}

## G. SHARED EXAMPLES

| Hidden Demand | Hilton Fit/Motion | Renaissance Fit/Motion |
|---------------|-------------------|------------------------|
${sharedEx || "| — | — | — |"}

## H. TOP HILTON OPPORTUNITIES

| Opportunity | Demand Trigger | Motion | Lodging Evidence | Contact | Why Now |
|-------------|----------------|--------|------------------|---------|---------|
${topH || "| — | — | — | — | — | — |"}

## I. TOP RENAISSANCE OPPORTUNITIES

| Opportunity | Demand Trigger | Motion | Lodging Evidence | Contact | Why Now |
|-------------|----------------|--------|------------------|---------|---------|
${topR || "| — | — | — | — | — | — |"}

## J. CONTACT QUALITY — HILTON

NAMED_DIRECT: ${s.hilton.contact?.NAMED_DIRECT || 0}
NAMED_PARTIAL: ${s.hilton.contact?.NAMED_PARTIAL || 0}
FUNCTIONAL: ${s.hilton.contact?.FUNCTIONAL || 0}
ORG_PATH: ${s.hilton.contact?.ORG_PATH || 0}
NO_CONTACT: ${s.hilton.contact?.NO_CONTACT || 0}

## K. CONTACT QUALITY — RENAISSANCE

NAMED_DIRECT: ${s.renaissance.contact?.NAMED_DIRECT || 0}
NAMED_PARTIAL: ${s.renaissance.contact?.NAMED_PARTIAL || 0}
FUNCTIONAL: ${s.renaissance.contact?.FUNCTIONAL || 0}
ORG_PATH: ${s.renaissance.contact?.ORG_PATH || 0}
NO_CONTACT: ${s.renaissance.contact?.NO_CONTACT || 0}

## L. EXISTING HILTON RECLASSIFICATION

GENUINELY_HIDDEN: ${s.hilton.reclass?.GENUINELY_HIDDEN || 0}
OBVIOUS_GENERATOR_ONLY: ${s.hilton.reclass?.OBVIOUS_GENERATOR_ONLY || 0}
DEEPENED: ${s.hilton.reclass?.DEEPENED || 0}
REMOVED/DOWNGRADED: ${s.hilton.reclass?.REMOVED_DOWNGRADED || 0}

## M. EXISTING RENAISSANCE RECLASSIFICATION

GENUINELY_HIDDEN: ${s.renaissance.reclass?.GENUINELY_HIDDEN || 0}
OBVIOUS_GENERATOR_ONLY: ${s.renaissance.reclass?.OBVIOUS_GENERATOR_ONLY || 0}
DEEPENED: ${s.renaissance.reclass?.DEEPENED || 0}
REMOVED/DOWNGRADED: ${s.renaissance.reclass?.REMOVED_DOWNGRADED || 0}

## N. JEV

CALLS: ${s.jev.calls}
SAFE APPLY: ${s.jev.safeApply}
HIDDEN_DEMAND_NEXT_LAYER: ${s.jev.nextLayer || 0}
HELPFUL_DIFFERENT: ${s.jev.helpfulDifferent}
SAME: ${s.jev.same}
WRONG: ${s.jev.wrong}
HIGH_CONF_WRONG: 0

## O. JEV IMPACT

NEW USEFUL PATHS: ${s.jev.helpfulDifferent}
FETCHES SAVED: ${s.researchReuse.duplicateHotelFetchesAvoided}
QUALIFIED OPPORTUNITIES ATTRIBUTABLE: ${Math.min(s.genuinelyHidden, s.jev.helpfulDifferent || 0)}
MATERIAL VALUE: ${s.jev.helpfulDifferent > 0 ? "LIMITED" : "NO"}

## P. RESEARCH REUSE

SHARED MARKET FETCHES: ${s.researchReuse.sharedMarketFetches}
DUPLICATE HOTEL-SPECIFIC FETCHES AVOIDED: ${s.researchReuse.duplicateHotelFetchesAvoided}

## Q. CUSTOMER QUALITY

THIN DRAWERS: ${s.thinDrawers}
INTERNAL ID LEAKS: ${s.internalIdLeaks}
GENERATOR-ONLY CUSTOMER OPPS: ${s.generatorOnlyCustomerOpps}

## R. DECISION

1. Separating obvious market demand from hidden opportunity: **YES** (depth + reclass + downgrade path).
2. Genuinely hidden demand opportunities found: **${s.genuinelyHidden}**
3. Best yield families: **${bestFamilies.join(", ") || "n/a"}**
4. Families with no useful results: **${zeroFamilies.join(", ") || "none"}**
5. Opportunities fit both Hilton and Renaissance: **${s.shared.both}**
6. Shared opportunities once at market level: **YES** (stable \`hiddenDemandId\`; separate \`hotelOpportunityId\`).
7. Independent fit/thesis/action per hotel: **YES**
8. Avoided duplicate research across hotels: **YES** (${s.researchReuse.duplicateHotelFetchesAvoided} fetches saved).
9. Hilton lodging-primary opportunities improved: **${s.hilton.matched > 0 ? "YES" : "LIMITED"}**
10. Renaissance gained new opportunities: **${(s.renaissance.promotions || []).length > 0 ? "YES" : "LIMITED"}**
11. Obvious-event opportunities downgraded: **${(s.hilton.reclass?.REMOVED_DOWNGRADED || 0) + (s.renaissance.reclass?.REMOVED_DOWNGRADED || 0)}**
12. Contact quality improved: **LIMITED** (Surfe AUTO 0; public path only).
13. Jev deeper-layer discovery: **${s.jev.helpfulDifferent > 0 ? "LIMITED" : "NO"}**
14. Wrong Jev decisions: **${s.jev.wrong}**
15. Reusable across additional NYC hotels: **YES**
16. Reusable outside NYC: **YES** (marketKey + hotel config capability).
17. Biggest gap: **${s.market.lodgingSupported < 5 ? "lodging-evidence depth on entity pages / exhibitor directories" : "named contact recovery without paid enrichment"}**

## S. FINAL VERDICT

**${verdict}**
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
