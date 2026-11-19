#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  DISCOVERY_YIELD_LABEL,
  COMMERCIAL_DISPOSITION,
  PHASE2B_EXCLUDED_OPPORTUNITY_IDS,
  labelDiscoveryYield,
  classifyCommercialDisposition,
  assessOpportunityDrawerReadiness,
  applyIntakeSalesReadyGate,
  resolveSharedOpportunityIdentity,
} from "../lib/group-demand-intelligence/research/gdi-discovery-intake-disposition-v1.js";
import {
  createFixtureDiscoveryProvider,
  createHistoricalHitProvider,
  runSegmentDiscoveryExecutor,
} from "../lib/group-demand-intelligence/research/segment-discovery-executor-v1.js";
import { runOfflineValidation } from "../lib/group-demand-intelligence/experiments/gdi-discovery-intake-disposition-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// --- Unit: yield contract ---
const fixtureYield = labelDiscoveryYield({
  candidate: { organizationName: "Fixture Org", title: "Fixture Event", fixture: true },
  retrievalMode: "FIXTURE",
  isLive: false,
  freshRetrievals: 0,
});
assert.equal(fixtureYield.discoveryYieldLabel, DISCOVERY_YIELD_LABEL.FIXTURE);
assert.equal(fixtureYield.claimsGenuineNewDiscovery, false);

const historicalYield = labelDiscoveryYield({
  candidate: { organizationName: "Hist Org", title: "Hist Event", historical: true },
  retrievalMode: "HISTORICAL_REPLAY",
  isLive: false,
  freshRetrievals: 0,
});
assert.equal(historicalYield.discoveryYieldLabel, DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY);
assert.equal(historicalYield.claimsGenuineNewDiscovery, false);

const lyingFixture = labelDiscoveryYield({
  candidate: { organizationName: "X", title: "Y", fixture: true },
  retrievalMode: "FIXTURE",
  isLive: true,
  freshRetrievals: 9,
});
assert.equal(lyingFixture.discoveryYieldLabel, DISCOVERY_YIELD_LABEL.FIXTURE);

const liveYield = labelDiscoveryYield({
  candidate: { organizationName: "Live Org Probe", title: "Live Program 2028" },
  retrievalMode: "FRESH_RETRIEVAL",
  isLive: true,
  freshRetrievals: 2,
});
assert.equal(liveYield.discoveryYieldLabel, DISCOVERY_YIELD_LABEL.LIVE_NEW);

// --- Unit: commercial disposition ---
const competitor = classifyCommercialDisposition(
  {
    organizationName: "Test Assoc",
    title: "Annual Banquet 2026",
    qualificationFailureReason: "VENUE_ALREADY_SELECTED",
    eventActivityEvidence: "Banquet held at Gaylord National, National Harbor.",
    competitorHotel: "Gaylord National",
  },
  { targetHotel: "Bethesda Marriott" },
);
assert.equal(competitor.commercialDisposition, COMMERCIAL_DISPOSITION.COMPETITOR_PLACED);
assert.equal(competitor.autoRejectOpportunity, false);
assert.equal(competitor.preserveAsCompetitiveIntelligence, true);

const tooEarly = classifyCommercialDisposition(
  {
    organizationName: "Future Org",
    title: "2029 Symposium",
    qualificationFailureReason: "TOO_EARLY",
    whatIsHappening: "Symposium announced; lodging requirement not published.",
  },
  { targetHotel: "Bethesda Marriott" },
);
assert.equal(tooEarly.commercialDisposition, COMMERCIAL_DISPOSITION.TOO_EARLY_WATCH);
assert.ok(tooEarly.watch?.revisitTrigger);
assert.ok(tooEarly.watch?.evidenceGap);

const speculative = classifyCommercialDisposition(
  {
    organizationName: "Speculative Co",
    title: "Possible meetings",
    demandEvidenceClass: "SPECULATIVE_DEMAND",
    whatIsHappening: "Interesting organization without verified lodging demand.",
  },
  { targetHotel: "Bethesda Marriott" },
);
assert.equal(speculative.commercialDisposition, COMMERCIAL_DISPOSITION.SPECULATIVE);

const gated = applyIntakeSalesReadyGate(
  { status: "SALES_READY", salesReady: true, gaps: [] },
  speculative,
);
assert.equal(gated.status, "RESEARCH_CONTINUATION");
assert.equal(gated.intakeBlockedSalesReady, true);

// --- Unit: shared identity + drawer gaps (no fabrication) ---
const shared = resolveSharedOpportunityIdentity({
  organizationName: "AHIMA",
  title: "AHIMA Advocacy Summit 2027",
});
assert.ok(shared.organizationKey);
assert.ok(shared.programKey);
assert.equal(shared.sharedDemandKey, shared.programKey);

const emptyDrawer = assessOpportunityDrawerReadiness({
  organizationName: "Gap Org",
  title: "Gap Event",
});
assert.equal(emptyDrawer.ready, false);
assert.ok(emptyDrawer.gaps.includes("whyActNow"));
assert.ok(emptyDrawer.gaps.includes("recommendedNextStep"));

// --- Executor wiring: fixture candidates never LIVE_NEW ---
const fixtureProvider = createFixtureDiscoveryProvider();
const exec = await runSegmentDiscoveryExecutor({
  hotelId: "recLuxvwwxID7U2B8",
  segmentId: "government_public_sector",
  skipHpc: true,
  authorizePaidDiscovery: false,
  provider: fixtureProvider,
  useFixtureProvider: true,
});
assert.ok(exec.ok);
for (const c of exec.candidates || []) {
  assert.notEqual(c.discoveryYieldLabel, DISCOVERY_YIELD_LABEL.LIVE_NEW);
  assert.equal(c.claimsGenuineNewDiscovery, false);
}

const histProvider = createHistoricalHitProvider([
  {
    organizationName: "Historical Replay Org",
    title: "Historical Program",
    url: "https://example.com/historical",
    excerpt: "Prior ledger row",
  },
]);
const histExec = await runSegmentDiscoveryExecutor({
  hotelId: "recLuxvwwxID7U2B8",
  segmentId: "government_public_sector",
  skipHpc: true,
  provider: histProvider,
});
for (const c of histExec.candidates || []) {
  assert.equal(c.discoveryYieldLabel, DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY);
}

assert.ok(PHASE2B_EXCLUDED_OPPORTUNITY_IDS.includes("gdi_lmc_00562bf81b1e1957"));
assert.ok(
  PHASE2B_EXCLUDED_OPPORTUNITY_IDS.includes("gdi_opp_2027_ddaa_annual_conference_8"),
);

// --- Offline frozen cohort ---
const report = runOfflineValidation({ writeReports: true });
assert.equal(report.paidUsd, 0);
assert.equal(report.claimsImprovedLiveDiscovery, false);
assert.equal(report.acceptance.zeroHistoricalFixtureLabeledLiveNew, true);
assert.equal(report.acceptance.zeroInventoryReopenLabeledLiveNew, true);
assert.equal(report.acceptance.noSpeculativePromotedToSalesReady, true);
assert.equal(report.duplicates.newCanonicalOpportunitiesIntroduced, 0);
assert.ok(
  report.disposition.accuracy != null && report.disposition.accuracy >= 0.8,
  `disposition accuracy ${report.disposition.accuracy} below 0.8`,
);
assert.ok(report.acceptancePass, "offline acceptance must pass");

const resultsPath = path.join(
  ROOT,
  "reports/gdi-discovery-intake-disposition-v1/RESULTS.json",
);
assert.ok(fs.existsSync(resultsPath));

console.log(
  JSON.stringify(
    {
      ok: true,
      accuracy: report.disposition.accuracy,
      competitorPrecision: report.disposition.competitorPlacement.precision,
      competitorRecall: report.disposition.competitorPlacement.recall,
      drawerReady: report.drawer.readyCount,
      drawerAudited: report.drawer.audited,
      recommendedNextSlice: report.recommendedNextSlice.id,
    },
    null,
    2,
  ),
);
