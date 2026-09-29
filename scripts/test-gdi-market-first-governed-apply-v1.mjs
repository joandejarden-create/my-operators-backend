/**
 * Tests: GDI Market-First Governed Apply V1
 *   node scripts/test-gdi-market-first-governed-apply-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  NYC_CONTROL_HOTELS,
  buildNycControlGeographyProfiles,
  computeMarketLinkedHotelOpportunityId,
  extractMarketOpportunityPacket,
  buildHotelOpportunityFromMarketPacket,
  evaluateHotelGeographicApplicability,
  evaluateCrossHotelFit,
  GEO_APPLICABILITY,
  classifyOpportunityGeography,
  fanoutDecisionForGeography,
} from "../lib/group-demand-intelligence/market-opportunity-graph/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/group-demand-intelligence/market-first-governed-apply-v1"
);
const PRIOR = path.join(
  __dirname,
  "../reports/group-demand-intelligence/market-opportunity-graph-nyc-parity-v1"
);

function loadJson(dir, name) {
  return JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"));
}

const summary = loadJson(OUT, "RUN_SUMMARY.json");
const applyRows = loadJson(OUT, "HILTON_APPLY_ROWS.json");
const newReady = loadJson(OUT, "HILTON_NEW_READY.json");
const nowCtrl = loadJson(OUT, "NOW_NOW_CONTROL.json");
const sdGeo = loadJson(OUT, "SANTO_DOMINGO_GEOGRAPHY.json");
const sdCross = loadJson(OUT, "SANTO_DOMINGO_CROSS_EVAL.json");
const priorHil = loadJson(PRIOR, "RENAISSANCE_TO_HILTON.json");

// Renaissance 11 preserved as apply source
{
  assert.equal(summary.nyc.renaissanceReady, 11);
  assert.equal(applyRows.length, 11);
  assert.equal(priorHil.length, 11);
}

// Hilton governed apply: 10 ready, 1 not fit (Forum commercially closed)
{
  assert.equal(summary.nyc.hiltonApplied, 10);
  assert.equal(summary.nyc.hiltonReadyAfter, 10);
  assert.equal(summary.nyc.hiltonNotFit, 1);
  assert.equal(summary.nyc.hiltonNeedsData, 0);
  assert.equal(newReady.length, 10);
  const forum = applyRows.find((r) => /Forum on Education Abroad/i.test(r.renaissanceTitle || ""));
  assert.ok(forum);
  assert.equal(forum.finalState, "NOT_FIT");
  assert.equal(forum.applied, false);
  assert.match(String(forum.blocker || ""), /commercial_lodging_closed/);
}

// No duplicated market-opportunity identity across Hilton links
{
  const mktIds = newReady.map((r) => r.marketOpportunityId).filter(Boolean);
  assert.equal(mktIds.length, 10);
  assert.equal(new Set(mktIds).size, 10);
  assert.equal(summary.duplication.duplicateMarketIdentities, 0);
  assert.equal(summary.duplication.duplicateSourcePackets, 0);
}

// Boutique incomplete pair resolved via Jev lodging action
{
  const boutique = applyRows.find((r) =>
    /Boutique Hotel Investment/i.test(r.renaissanceTitle || "")
  );
  assert.ok(boutique);
  assert.equal(boutique.priorShadow, "HOTEL_MATCHED_NEEDS_MORE_DATA");
  assert.equal(boutique.applied, true);
  assert.equal(boutique.jev, "VERIFY_LODGING_STATUS");
  assert.equal(boutique.jevResolved, true);
  assert.equal(summary.jev.nycRouted, 1);
  assert.equal(summary.jev.nycResolved, 1);
}

// NOW NOW selective reuse (not blind NYC fanout)
{
  assert.equal(summary.nyc.nowShadowReady, 2);
  assert.equal(summary.nyc.nowNeedsData, 8);
  assert.equal(summary.nyc.nowNotFit, 1);
  assert.ok(nowCtrl.every((r) => r.geo === GEO_APPLICABILITY.PLAUSIBLE || r.final === "NOT_FIT"));
  assert.ok(nowCtrl.every((r) => r.geo !== GEO_APPLICABILITY.DIRECT));
}

// Times Square same-micro-area: Hilton DIRECT/STRONG/PLAUSIBLE candidates only
{
  for (const r of applyRows.filter((x) => x.applied)) {
    assert.ok(
      [GEO_APPLICABILITY.DIRECT, GEO_APPLICABILITY.STRONG, GEO_APPLICABILITY.PLAUSIBLE].includes(
        r.hiltonGeo
      )
    );
  }
}

// NoHo selective applicability (metro ≠ auto-apply)
{
  const metro = classifyOpportunityGeography({
    title: "Association meeting",
    destinationStatus: "New York City",
  });
  const fan = fanoutDecisionForGeography(metro);
  assert.equal(fan.autoEvaluateMetroWide, false);
}

// Santo Domingo geography differentiation
{
  assert.match(sdGeo.JW.submarket, /Piantini/i);
  assert.match(sdGeo.RADISSON.submarket, /Naco/i);
  assert.notEqual(sdGeo.JW.microArea, sdGeo.RADISSON.microArea);
  assert.notEqual(sdGeo.JW.archetype, sdGeo.RADISSON.archetype);
}

// Santo Domingo cross-eval: empty corpus → no false fanout
{
  assert.equal(summary.sd.jwTotal, 0);
  assert.equal(summary.sd.radissonTotal, 0);
  assert.equal(sdCross.length, 0);
  assert.equal(summary.sd.shadowReady, 0);
  assert.equal(summary.customerMutationSd, false);
}

// Hotel-linked IDs are deterministic and distinct per hotel
{
  const mkt = "gdi_mkt_test_identity";
  const a = computeMarketLinkedHotelOpportunityId(NYC_CONTROL_HOTELS.HILTON, mkt);
  const b = computeMarketLinkedHotelOpportunityId(NYC_CONTROL_HOTELS.HILTON, mkt);
  const c = computeMarketLinkedHotelOpportunityId(NYC_CONTROL_HOTELS.RENAISSANCE, mkt);
  assert.equal(a, b);
  assert.notEqual(a, c);
}

// Commercial closed lodging → NOT_FIT (no blind promote)
{
  const geo = buildNycControlGeographyProfiles();
  const seed = {
    id: "gdi_opp_forum_test",
    title: "Forum on Education Abroad Annual Conference",
    organizationName: "Forum on Education Abroad",
    eventStartDate: "2027-03-10",
    destinationStatus: "New York City / Times Square",
    venueStatus: "FULLY_PLACED",
    hotelOpportunityThesis:
      "None — organizer keeps booking within single-hotel arrangement and does not use third-party housing.",
    summaryWhyMatters: "Eliminate superficially attractive Marriott-adjacent event.",
    opportunityType: "FUTURE_CYCLE",
  };
  const packet = extractMarketOpportunityPacket(seed, NYC_CONTROL_HOTELS.RENAISSANCE);
  const built = buildHotelOpportunityFromMarketPacket({
    marketPacket: packet,
    seedOpp: seed,
    targetHotelProfile: geo.HILTON,
    requireStrictReady: true,
  });
  assert.equal(built.ok, false);
  assert.equal(built.finalState, "NOT_FIT");
  assert.equal(built.reason, "commercial_lodging_closed");
}

// Deployment hold
{
  assert.equal(summary.cron, "HELD");
  assert.equal(summary.deploy, "NOT_RUN");
  assert.equal(summary.customerMutationHilton, true);
}

console.log("test-gdi-market-first-governed-apply-v1: PASS");
