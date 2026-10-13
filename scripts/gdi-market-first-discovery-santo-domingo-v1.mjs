#!/usr/bin/env node
/**
 * GDI Market-First Discovery V1 — Santo Domingo
 *
 *   node scripts/gdi-market-first-discovery-santo-domingo-v1.mjs
 *   node scripts/gdi-market-first-discovery-santo-domingo-v1.mjs --apply
 *
 * Discover once → evaluate JW + Radisson. No Webhound / Surfe / NYC mutation / cron.
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
import { NYC_CONTROL_HOTELS } from "../lib/group-demand-intelligence/market-opportunity-graph/hotel-geography-profile-v1.js";
import { SANTO_DOMINGO_HOTELS } from "../lib/group-demand-intelligence/market-opportunity-graph/santo-domingo-market-geography-v1.js";
import { runSantoDomingoMarketFirstDiscovery } from "../lib/group-demand-intelligence/market-opportunity-graph/market-first-discovery-santo-domingo-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/market-first-discovery-santo-domingo-v1"
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

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function mapReadyDetail(row, hotelKey) {
  const o = row.hotelOpp;
  return {
    marketOpportunityId: o.marketOpportunityId || row.marketOpportunityId,
    hotelOpportunityId: o.id,
    hotel: hotelKey,
    title: o.title,
    dates: `${o.eventStartDate || row.dates || ""}–${o.eventEndDate || ""}`,
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
  };
}

async function main() {
  const head = gitHead();
  console.log(`[preflight] HEAD=${head} apply=${APPLY}`);

  // NYC regression snapshot (read-only)
  const renDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.RENAISSANCE);
  const hilDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.HILTON);
  const nowDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.NOW_NOW);
  const jwBefore = await loadOpportunitiesCanonical(SANTO_DOMINGO_HOTELS.JW);
  const radBefore = await loadOpportunitiesCanonical(SANTO_DOMINGO_HOTELS.RADISSON);

  const nycRegression = {
    renaissanceReady: listReady(renDoc.opportunities || []).length,
    hiltonReady: listReady(hilDoc.opportunities || []).length,
    nowReady: listReady(nowDoc.opportunities || []).length,
    jwReadyBefore: listReady(jwBefore.opportunities || []).length,
    radissonReadyBefore: listReady(radBefore.opportunities || []).length,
    jwTotalBefore: (jwBefore.opportunities || []).length,
    radissonTotalBefore: (radBefore.opportunities || []).length,
  };
  console.log("[nyc/sd baseline]", nycRegression);

  const result = await runSantoDomingoMarketFirstDiscovery({
    maxQueries: 70,
    maxFetches: 140,
    maxPdfDocs: 30,
    maxFollowups: 50,
    maxJevCalls: 25,
    nowDate: NOW,
  });

  console.log("[discovery]", {
    queries: result.ledger.queries,
    fetches: result.ledger.fetches,
    marketOpps: result.marketOpportunities.length,
    hotelMatchable: result.ledger.hotelMatchable,
    jwReady: result.jwReady.length,
    radReady: result.radReady.length,
    both: result.differentiation.both,
    jwOnly: result.differentiation.jwOnly,
    radOnly: result.differentiation.radOnly,
    neither: result.differentiation.neither,
  });

  // Persist customer-ready only behind --apply (append; no wipe)
  if (APPLY) {
    if (result.jwReady.length) {
      const doc = await loadOpportunitiesCanonical(SANTO_DOMINGO_HOTELS.JW);
      const merged = [...(doc.opportunities || [])];
      for (const row of result.jwReady) {
        const opp = row.hotelOpp;
        const idx = merged.findIndex((o) => oppId(o) === opp.id);
        if (idx >= 0) merged[idx] = { ...merged[idx], ...opp };
        else merged.push(opp);
      }
      await saveOpportunitiesCanonical(SANTO_DOMINGO_HOTELS.JW, {
        ...doc,
        opportunities: merged,
        researchVersion: "market-first-discovery-santo-domingo-v1",
      });
      console.log(`[apply] JW +${result.jwReady.length}`);
    }
    if (result.radReady.length) {
      const doc = await loadOpportunitiesCanonical(SANTO_DOMINGO_HOTELS.RADISSON);
      const merged = [...(doc.opportunities || [])];
      for (const row of result.radReady) {
        const opp = row.hotelOpp;
        const idx = merged.findIndex((o) => oppId(o) === opp.id);
        if (idx >= 0) merged[idx] = { ...merged[idx], ...opp };
        else merged.push(opp);
      }
      await saveOpportunitiesCanonical(SANTO_DOMINGO_HOTELS.RADISSON, {
        ...doc,
        opportunities: merged,
        researchVersion: "market-first-discovery-santo-domingo-v1",
      });
      console.log(`[apply] Radisson +${result.radReady.length}`);
    }
  }

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
      sharedVsUnique: c.sharedVsUnique,
      sourceFamily: c.sourceFamily,
      jevMarketAction: c.jevMarketAction || null,
    }))
  );
  writeJson("HOTEL_PAIR_MATRIX.json", result.pairMatrix);
  writeJson(
    "JW_READY.json",
    result.jwReady.map((r) => mapReadyDetail(r, "JW"))
  );
  writeJson(
    "RADISSON_READY.json",
    result.radReady.map((r) => mapReadyDetail(r, "RADISSON"))
  );
  writeJson("WATCH.json", result.ledger.watch);
  writeJson("LEDGER.json", result.ledger);
  writeJson("EFFICIENCY.json", result.efficiency);
  writeJson("DIFFERENTIATION.json", result.differentiation);
  writeJson("GEO_EFFECT.json", result.geoEffect);

  const summary = {
    head,
    apply: APPLY,
    generatedAt: new Date().toISOString(),
    nycRegression,
    executive: {
      marketOpportunitiesDiscovered: result.marketOpportunities.length,
      lodgingSupported: result.ledger.lodgingSupported,
      openTbd: result.ledger.openTbd,
      jwReady: result.jwReady.length,
      jwNeedsData: result.jwNeeds.length,
      radissonReady: result.radReady.length,
      radissonNeedsData: result.radNeeds.length,
      bothHotels: result.differentiation.both,
      jwOnly: result.differentiation.jwOnly,
      radissonOnly: result.differentiation.radOnly,
      neither: result.differentiation.neither,
    },
    funnel: {
      queries: result.ledger.queries,
      fetches: result.ledger.fetches,
      validEntities: result.ledger.validEntities,
      validFuturePrograms: result.ledger.validFuturePrograms,
      lodgingSupported: result.ledger.lodgingSupported,
      openTbd: result.ledger.openTbd,
      hotelMatchable: result.ledger.hotelMatchable,
    },
    jev: {
      marketLevelActions: result.ledger.jevMarket,
      hotelPairActions: result.ledger.jevHotelPair,
      blockersResolved: result.ledger.jevResolved,
      fetches: result.ledger.fetches,
    },
    efficiency: result.efficiency,
    geoEffect: result.geoEffect,
    watchCount: result.ledger.watch.length,
    customerMutationSd: APPLY && (result.jwReady.length > 0 || result.radReady.length > 0),
    customerMutationNyc: false,
    cron: "HELD",
    deploy: "NOT_RUN",
  };
  writeJson("RUN_SUMMARY.json", summary);

  console.log("[done]", summary.executive);
  console.log(`[out] ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
