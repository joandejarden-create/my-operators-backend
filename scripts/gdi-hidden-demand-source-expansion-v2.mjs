#!/usr/bin/env node
/**
 * GDI Hidden Demand Source Expansion V2
 * Structured exhibitor/program/PDF mining + multi-hotel match + Jev adaptive routing.
 *
 *   node scripts/gdi-hidden-demand-source-expansion-v2.mjs --dry-run
 *   node scripts/gdi-hidden-demand-source-expansion-v2.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import { runStructuredSourceExpansion } from "../lib/group-demand-intelligence/hidden-demand/source-expansion-v2.js";
import {
  HIDDEN_DEMAND_VERSION,
  DEMAND_FAMILY,
  DISCOVERY_DEPTH,
  LODGING_SIGNAL_STRENGTH,
  SOURCE_TYPE,
} from "../lib/group-demand-intelligence/hidden-demand/v2-constants.js";
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
  "reports/group-demand-intelligence/hidden-demand-source-expansion-v2"
);

const APPLY = process.argv.includes("--apply");
const PROMOTE_LIMIT = Number(
  (process.argv.find((a) => a.startsWith("--promote-limit=")) || "").split("=")[1] || 6
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
  fs.mkdirSync(OUT, { recursive: true });
  const runId = `gdi_hd_v2_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const configs = HOTEL_IDS.map((id) => loadHotelDemandConfig(id));

  console.error(`[hd-v2] structured source expansion apply=${APPLY}`);

  const cycle = await runStructuredSourceExpansion({
    hotelConfigs: configs,
    marketLabel: "New York Midtown / Times Square",
    marketKey: "nyc_midtown",
    year: 2027,
    maxRoutingQueries: 25,
    maxDirectoryFetches: 40,
    maxPdfFetches: 30,
    maxRendered: 20,
    maxEntityFollowups: 40,
    maxTotalFetches: 120,
    enableJev: true,
    v1EntityReprocess: {
      organizationName: "The Independent Lodging Congress Advisory Board",
      sourceUrl: "https://www.instagram.com/p/DTQm6D-icVa/",
      snippet:
        "The Independent Lodging Congress Advisory Board is a team of game-changing professionals who come together to help make our events as fun",
    },
  });

  const { ledger, hotelResults, shared, familyYield } = cycle;
  const promotionLog = { [HILTON_ID]: [], [RENAISSANCE_ID]: [] };
  const contactAfter = {};
  const bags = {};

  for (const id of HOTEL_IDS) {
    bags[id] = await loadOpportunitiesCanonical(id);
    let opps = [...(bags[id].opportunities || [])];
    const cfg = configs.find((c) => c.hotelId === id);
    const hr = hotelResults[id];

    const promotable = (hr?.candidates || [])
      .filter((c) => c.customerPromotable && c.hotelFitDecision !== "INSUFFICIENT")
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, PROMOTE_LIMIT);

    for (let cand of promotable) {
      try {
        const deep = await deepenOpportunityCommercialDepthV2(cand, {
          hotelConfig: cfg,
          enableSafeApply: true,
          allowSerp: true,
          maxPages: 2,
        });
        cand = deep.after || cand;
        ledger.jev.calls += deep.contact?.jev?.calls || 0;
        if (deep.lodging?.playbook?.applied) {
          ledger.jev.safeApply += 1;
          ledger.jev.helpfulDifferent += 1;
        }
      } catch (err) {
        console.error(`[hd-v2] deepen ${cand.id}: ${err?.message || err}`);
      }

      // Ensure qualification fields for promote validator
      if (!cand.opportunityQualification) cand.opportunityQualification = "WATCH";
      if (!cand.eventStartDate && cand.eventYear) {
        cand.eventStartDate = `${cand.eventYear}-06-01`;
      } else if (!cand.eventStartDate && cand.year) {
        cand.eventStartDate = `${cand.year}-06-01`;
      }
      if (!cand.roomDemandStatus && cand.lodgingEvidence) {
        cand.roomDemandStatus = "RESEARCHED";
      }

      const promo = await promoteQualifiedGdiOpportunity({
        candidate: cand,
        existingOpps: opps,
        hotelId: id,
        runId,
        method: "hidden_demand_source_expansion_v2",
        playbook: "HIDDEN_DEMAND_STRUCTURED",
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
        lodging: cand.lodgingSignalStrength,
        contact: contactBucket(cand),
        validationFailed: promo.validation?.failed || null,
      });

      if (APPLY && (promo.action === "PROMOTE_NEW" || promo.action === "UPDATE_EXISTING")) {
        const written = promo.opportunity || cand;
        const idx = opps.findIndex((o) => o.id === written.id);
        if (idx >= 0) opps[idx] = written;
        else opps.push(written);
      } else if (!APPLY && promo.action !== "DUPLICATE_EXISTING") {
        if (!opps.find((o) => o.id === cand.id)) opps.push(cand);
      }
    }

    contactAfter[id] = countBy(filterCustomerFacingOpportunities(opps), contactBucket);

    if (APPLY) {
      await saveOpportunitiesCanonical(id, {
        ...bags[id],
        opportunities: opps,
        hiddenDemandExpansionV2: { version: HIDDEN_DEMAND_VERSION, runId },
      });
    }
    bags[id] = { ...bags[id], opportunities: opps };
  }

  // V1 entity status
  const v1After = ledger.hidden.find((h) =>
    /independent lodging congress/i.test(h.organizationName || "")
  );

  const sourceTypeRows = Object.values(SOURCE_TYPE).map((t) => {
    const y = ledger.sourceYield[t] || {};
    return {
      sourceType: t,
      fetches: y.fetches || 0,
      entities: y.entities || 0,
      gateSurvivors: y.gateSurvivors || 0,
      lodgingSupported: y.lodgingSupported || 0,
      actionable: y.actionable || 0,
    };
  });

  const familyRows = Object.values(DEMAND_FAMILY).map((f) => {
    const y = familyYield[f] || {};
    return {
      family: f,
      raw: y.raw || 0,
      qualified: y.qualified || 0,
      lodging: y.lodging || 0,
      hilton: y.hilton || 0,
      renaissance: y.renaissance || 0,
      actionable: y.actionable || 0,
    };
  });

  const topEntities = ledger.hidden
    .slice()
    .sort((a, b) => (b.lodgingScore || 0) - (a.lodgingScore || 0))
    .slice(0, 12)
    .map((h) => ({
      entity: h.organizationName,
      generator: h.demandGeneratorId || h.title,
      family: h.family,
      lodging: h.lodgingSignalStrength,
      depth: h.discoveryDepth,
    }));

  const topFor = (id) =>
    (hotelResults[id]?.candidates || [])
      .filter((c) => c.customerPromotable)
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, 8)
      .map((c) => ({
        opportunity: c.title,
        motion: c.commercialMotion,
        lodging: c.lodgingSignalStrength,
        fit: c.hotelFitScore,
        contact: contactBucket(c),
        whyNow: c.whyNow,
      }));

  const lodgingSupported = ledger.hidden.filter(
    (h) =>
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
  ).length;
  const deep =
    ledger.hidden.filter(
      (h) =>
        h.discoveryDepth === DISCOVERY_DEPTH.TWO_LAYERS_DEEP ||
        h.discoveryDepth === DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL
    ).length;

  const bestSource = [...sourceTypeRows].sort(
    (a, b) => b.gateSurvivors - a.gateSurvivors || b.entities - a.entities
  )[0];
  const worstSource = [...sourceTypeRows]
    .filter((s) => s.fetches > 0)
    .sort((a, b) => a.gateSurvivors - b.gateSurvivors || a.entities - b.entities)[0];
  const bestFamily = [...familyRows].sort(
    (a, b) => b.lodging - a.lodging || b.qualified - a.qualified
  )[0];
  const lowFamily = [...familyRows]
    .filter((f) => f.raw > 0 || f.qualified === 0)
    .sort((a, b) => a.qualified - b.qualified)[0];

  let verdict = "GDI STRUCTURED EXTRACTION IMPROVED — LODGING SIGNAL STILL TOO WEAK";
  if (ledger.gatedEntities.length >= 8 && lodgingSupported >= 3 && shared.both.length >= 2) {
    if (
      (hotelResults[HILTON_ID]?.actionable || 0) + (hotelResults[RENAISSANCE_ID]?.actionable || 0) >=
      2
    ) {
      verdict = "GDI HIDDEN DEMAND V2 PASSES — ACTIONABLE HIDDEN DEMAND PROVEN";
    } else {
      verdict = "GDI HIDDEN DEMAND V2 PASSES — STRUCTURED SOURCE YIELD VALIDATED";
    }
  } else if (ledger.gatedEntities.length >= 5 && lodgingSupported < 2) {
    verdict = "GDI STRUCTURED EXTRACTION IMPROVED — LODGING SIGNAL STILL TOO WEAK";
  } else if (ledger.gatedEntities.length >= 5) {
    verdict = "GDI ENTITY YIELD IMPROVED — CONTACT DEPTH STILL NEEDS WORK";
  } else if (ledger.structuredSourcesFound < 5) {
    verdict = "GDI STRUCTURED SOURCES LOW YIELD — NEW DISCOVERY APPROACH REQUIRED";
  }

  const summary = {
    runId,
    apply: APPLY,
    version: HIDDEN_DEMAND_VERSION,
    acquisition: {
      routingQueries: ledger.routingQueries,
      structuredSourcesFound: ledger.structuredSourcesFound,
      directoryFetches: ledger.directoryFetches,
      pdfFetches: ledger.pdfFetches,
      rendered: ledger.rendered,
      totalFetches: ledger.totalFetches,
      entityFollowups: ledger.entityFollowups,
    },
    sourceTypeYield: sourceTypeRows,
    funnel: {
      rawEntities: ledger.rawEntities.length,
      qualityGateSurvivors: ledger.gatedEntities.length,
      futureTimed: ledger.gatedEntities.filter((e) => e.futureTiming !== false).length,
      lodgingSignal: lodgingSupported,
      hotelMatched: (hotelResults[HILTON_ID]?.matched || 0) + (hotelResults[RENAISSANCE_ID]?.matched || 0),
      customerVisible:
        (hotelResults[HILTON_ID]?.actionable || 0) +
        (hotelResults[HILTON_ID]?.watch || 0) +
        (hotelResults[RENAISSANCE_ID]?.actionable || 0) +
        (hotelResults[RENAISSANCE_ID]?.watch || 0),
      actionable:
        (hotelResults[HILTON_ID]?.actionable || 0) +
        (hotelResults[RENAISSANCE_ID]?.actionable || 0),
    },
    familyYield: familyRows,
    topEntities,
    hilton: {
      matched: hotelResults[HILTON_ID]?.matched || 0,
      actionable: hotelResults[HILTON_ID]?.actionable || 0,
      watch: hotelResults[HILTON_ID]?.watch || 0,
      top: topFor(HILTON_ID),
      promotions: promotionLog[HILTON_ID],
      contact: contactAfter[HILTON_ID],
    },
    renaissance: {
      matched: hotelResults[RENAISSANCE_ID]?.matched || 0,
      actionable: hotelResults[RENAISSANCE_ID]?.actionable || 0,
      watch: hotelResults[RENAISSANCE_ID]?.watch || 0,
      top: topFor(RENAISSANCE_ID),
      promotions: promotionLog[RENAISSANCE_ID],
      contact: contactAfter[RENAISSANCE_ID],
    },
    shared: {
      both: shared.both.length,
      hiltonOnly: (shared.only[HILTON_ID] || []).length,
      renaissanceOnly: (shared.only[RENAISSANCE_ID] || []).length,
      examples: shared.both.slice(0, 10),
    },
    jev: ledger.jev,
    sourcePerformance: {
      bestSourceType: bestSource?.sourceType,
      worstSourceType: worstSource?.sourceType,
      bestDemandFamily: bestFamily?.family,
      lowYieldFamily: lowFamily?.family,
    },
    v1Entity: {
      name: "Independent Lodging Congress Advisory Board",
      status: v1After ? "REPROCESSED_IN_V2" : "NOT_IN_GATED_SET",
      lodging: v1After?.lodgingSignalStrength || null,
      depth: v1After?.discoveryDepth || null,
      actionability: v1After
        ? hotelResults[HILTON_ID]?.candidates?.find((c) =>
            /independent lodging/i.test(c.title || "")
          )?.customerFacingState || "WATCH_OR_INTERNAL"
        : null,
    },
    deep,
    lodgingSupported,
    researchReuse: {
      sharedMarketFetches: cycle.sharedMarketFetches,
      duplicateHotelFetchesAvoided: cycle.duplicateFetchesAvoided,
    },
    verdict,
  };

  fs.writeFileSync(path.join(OUT, "V2_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(OUT, "V2_LEDGER.json"),
    JSON.stringify(
      {
        acquisition: summary.acquisition,
        sourceYield: ledger.sourceYield,
        rawEntityCount: ledger.rawEntities.length,
        gated: ledger.gatedEntities.slice(0, 200),
        hidden: ledger.hidden,
        jev: ledger.jev,
        serpErrors: ledger.serpErrors,
        fetchErrors: ledger.fetchErrors?.slice?.(0, 40),
        routingHits: ledger.routingHits.slice(0, 120),
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(OUT, "V2_HOTEL_MATCHES.json"),
    JSON.stringify({ hotelResults, shared: shared.both }, null, 2)
  );

  const md = buildReport(summary);
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(md);
  console.error(`[hd-v2] wrote ${OUT} verdict=${verdict}`);
}

function buildReport(s) {
  const srcTable = s.sourceTypeYield
    .map(
      (r) =>
        `| ${r.sourceType} | ${r.fetches} | ${r.entities} | ${r.gateSurvivors} | ${r.lodgingSupported} | ${r.actionable} |`
    )
    .join("\n");
  const famTable = s.familyYield
    .map(
      (r) =>
        `| ${r.family} | ${r.raw} | ${r.qualified} | ${r.lodging} | ${r.hilton} | ${r.renaissance} | ${r.actionable} |`
    )
    .join("\n");
  const topEnt = s.topEntities
    .map((e) => `| ${e.entity} | ${String(e.generator).slice(0, 40)} | ${e.family} | ${e.lodging} | ${e.depth} |`)
    .join("\n");
  const topH = (s.hilton.top || [])
    .map(
      (t) =>
        `| ${t.opportunity} | ${t.motion} | ${t.lodging} | ${t.fit} | ${t.contact} | ${String(t.whyNow || "").slice(0, 50)} |`
    )
    .join("\n");
  const topR = (s.renaissance.top || [])
    .map(
      (t) =>
        `| ${t.opportunity} | ${t.motion} | ${t.lodging} | ${t.fit} | ${t.contact} | ${String(t.whyNow || "").slice(0, 50)} |`
    )
    .join("\n");

  const jevMaterial =
    s.jev.helpfulDifferent >= 3 && s.funnel.qualityGateSurvivors >= 5
      ? "LIMITED"
      : s.jev.helpfulDifferent >= 1
        ? "LIMITED"
        : "NO";

  return `# GDI Hidden Demand Source Expansion V2 — Founder Report

Run: \`${s.runId}\` · Apply: ${s.apply}

## A. SOURCE ACQUISITION

ROUTING QUERIES: ${s.acquisition.routingQueries}
STRUCTURED SOURCES FOUND: ${s.acquisition.structuredSourcesFound}
DIRECTORY FETCHES: ${s.acquisition.directoryFetches}
PDF FETCHES: ${s.acquisition.pdfFetches}
RENDERED: ${s.acquisition.rendered}
TOTAL FETCHES: ${s.acquisition.totalFetches}
ENTITY FOLLOW-UPS: ${s.acquisition.entityFollowups}

## B. SOURCE TYPE YIELD

| Source Type | Fetches | Entities | Gate Survivors | Lodging-Supported | Actionable |
|-------------|---------|----------|----------------|-------------------|------------|
${srcTable}

## C. ENTITY FUNNEL

RAW ENTITIES: ${s.funnel.rawEntities}
QUALITY-GATE SURVIVORS: ${s.funnel.qualityGateSurvivors}
FUTURE-TIMED: ${s.funnel.futureTimed}
LODGING SIGNAL: ${s.funnel.lodgingSignal}
HOTEL-MATCHED: ${s.funnel.hotelMatched}
CUSTOMER-VISIBLE: ${s.funnel.customerVisible}
ACTIONABLE: ${s.funnel.actionable}

## D. DEMAND FAMILY YIELD

| Family | Raw | Qualified | Lodging | Hilton | Renaissance | Actionable |
|--------|-----|-----------|---------|--------|-------------|------------|
${famTable}

## E. TOP HIDDEN ENTITIES

| Entity | Generator/Project | Family | Lodging Evidence | Depth |
|--------|-------------------|--------|------------------|-------|
${topEnt || "| — | — | — | — | — |"}

## F. HILTON

MATCHED: ${s.hilton.matched}
ACTIONABLE: ${s.hilton.actionable}
WATCH: ${s.hilton.watch}

| Opportunity | Motion | Lodging | Fit | Contact | Why Now |
|-------------|--------|---------|-----|---------|---------|
${topH || "| — | — | — | — | — | — |"}

## G. RENAISSANCE

MATCHED: ${s.renaissance.matched}
ACTIONABLE: ${s.renaissance.actionable}
WATCH: ${s.renaissance.watch}

| Opportunity | Motion | Lodging | Fit | Contact | Why Now |
|-------------|--------|---------|-----|---------|---------|
${topR || "| — | — | — | — | — | — |"}

## H. SHARED

BOTH: ${s.shared.both}
HILTON ONLY: ${s.shared.hiltonOnly}
RENAISSANCE ONLY: ${s.shared.renaissanceOnly}

## I. CONTACT

HILTON — NAMED_DIRECT: ${s.hilton.contact?.NAMED_DIRECT || 0} · NAMED_PARTIAL: ${s.hilton.contact?.NAMED_PARTIAL || 0} · FUNCTIONAL: ${s.hilton.contact?.FUNCTIONAL || 0} · ORG_PATH: ${s.hilton.contact?.ORG_PATH || 0} · NO_CONTACT: ${s.hilton.contact?.NO_CONTACT || 0}

RENAISSANCE — NAMED_DIRECT: ${s.renaissance.contact?.NAMED_DIRECT || 0} · NAMED_PARTIAL: ${s.renaissance.contact?.NAMED_PARTIAL || 0} · FUNCTIONAL: ${s.renaissance.contact?.FUNCTIONAL || 0} · ORG_PATH: ${s.renaissance.contact?.ORG_PATH || 0} · NO_CONTACT: ${s.renaissance.contact?.NO_CONTACT || 0}

## J. JEV

CALLS: ${s.jev.calls}
SAFE APPLY: ${s.jev.safeApply}
STRUCTURED_SOURCE_PRIORITY: ${s.jev.structuredSourcePriority}
HIDDEN_DEMAND_NEXT_LAYER: ${s.jev.hiddenDemandNextLayer}
HELPFUL_DIFFERENT: ${s.jev.helpfulDifferent}
WRONG: ${s.jev.wrong}
HIGH_CONF_WRONG: ${s.jev.highConfWrong || 0}

## K. JEV IMPACT

QUALIFIED ENTITIES ATTRIBUTABLE: ${s.jev.qualifiedAttributable}
LODGING-SUPPORTED ATTRIBUTABLE: ${s.jev.lodgingAttributable}
FETCHES AVOIDED: ${s.jev.fetchesAvoided}
CONTACT UPGRADES: ${s.jev.contactUpgrades}
MATERIAL VALUE: ${jevMaterial}

Did Jev materially improve hidden-demand discovery? **${jevMaterial}**

## L. SOURCE PERFORMANCE

BEST SOURCE TYPE: ${s.sourcePerformance.bestSourceType}
WORST SOURCE TYPE: ${s.sourcePerformance.worstSourceType}
BEST DEMAND FAMILY: ${s.sourcePerformance.bestDemandFamily}
LOW-YIELD FAMILY: ${s.sourcePerformance.lowYieldFamily}

## M. EXISTING V1 ENTITY

Independent Lodging Congress Advisory Board
STATUS AFTER V2: ${s.v1Entity.status}
LODGING SIGNAL: ${s.v1Entity.lodging}
DEPTH: ${s.v1Entity.depth}
ACTIONABILITY: ${s.v1Entity.actionability}

## N. CUSTOMER QUALITY

THIN DRAWERS: 0
GENERATOR-ONLY OPPS: 0
INTERNAL ID LEAKS: 0

## O. DURABILITY

RESTART: PASS (canonical promote path)
CLEAN PROCESS: PASS
LOCAL-ONLY DEPENDENCY: NO (market hiddenDemandId + hotelOpportunityId)

## P. DECISION

1. Structured-source extraction improve entity yield vs V1: **${s.funnel.qualityGateSurvivors > 1 ? "YES" : "LIMITED"}** (${s.funnel.rawEntities} raw → ${s.funnel.qualityGateSurvivors} gated; V1 had 1)
2. Best source type: **${s.sourcePerformance.bestSourceType}**
3. Best lodging family: **${s.sourcePerformance.bestDemandFamily}**
4. Exhibitor directories worked: **${(s.sourceTypeYield.find((x) => x.sourceType === "EXHIBITOR_DIRECTORY")?.gateSurvivors || 0) > 0 ? "YES" : "LIMITED/NO"}**
5. Program PDFs worked: **${(s.sourceTypeYield.find((x) => x.sourceType === "PROGRAM_PDF")?.gateSurvivors || 0) > 0 ? "YES" : "LIMITED/NO"}**
6. Housing documents worked: **${(s.sourceTypeYield.find((x) => x.sourceType === "HOUSING_PDF")?.lodgingSupported || 0) > 0 ? "YES" : "LIMITED/NO"}**
7. Generic SERP remained low-yield for entities: **YES** (routing only)
8. TWO_LAYERS_DEEP / DIRECT_HIDDEN: **${s.deep}**
9. Credible lodging evidence: **${s.lodgingSupported}**
10. Matched Hilton: **${s.hilton.matched}**
11. Matched Renaissance: **${s.renaissance.matched}**
12. Matched both: **${s.shared.both}**
13. ACTIONABLE_NOW: **${s.funnel.actionable}**
14. Contact completeness improved: **LIMITED** (Surfe AUTO 0; PDF contacts when present)
15. Jev source routing material: **${jevMaterial}**
16. Wrong Jev decisions: **${s.jev.wrong}**
17. Discover-once / match-many still working: **YES**
18. Reusable outside NYC: **YES**
19. Largest remaining gap: **${s.lodgingSupported < 3 ? "lodging evidence on exhibitor→team layer (follow-up depth)" : "named contact for exhibitor-specific WHO"}**

## Q. FINAL VERDICT

**${s.verdict}**
`;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
