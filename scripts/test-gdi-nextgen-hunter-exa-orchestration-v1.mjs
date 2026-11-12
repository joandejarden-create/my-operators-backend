#!/usr/bin/env node
/**
 * Gates for Hunter→Exa orchestration V1.
 * Zero paid. Proves adapters, L1/L2/L3, dedupe, independence, ORS handoff.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FIRST_WAVE_HUNTERS,
  SPECIALIST_HUNTER,
  SPECIALIST_HUNTER_LIST,
} from "../lib/group-demand-intelligence/nextgen-discovery/specialist-hunter-contract-v1.js";
import { NEED_DATE_RESEARCH_STRATEGY } from "../lib/group-demand-intelligence/need-date-demand/need-date-demand-contract-v1.js";
import {
  normalizeHunterDiscovery,
  NORMALIZE_DISCOVERY_V1,
} from "../lib/group-demand-intelligence/nextgen-discovery/orchestration/normalize-discovery-v1.js";
import {
  buildDedupeKeySets,
  dedupeNormalizedDiscoveries,
} from "../lib/group-demand-intelligence/nextgen-discovery/orchestration/discovery-dedupe-v1.js";
import {
  selectResearchDepthForDiscovery,
  HUNTER_RESEARCH_LEVEL,
} from "../lib/group-demand-intelligence/nextgen-discovery/orchestration/research-depth-selector-v1.js";
import {
  runHunterToExaOrchestration,
  prepareBoundedPaidResearchPilots,
} from "../lib/group-demand-intelligence/nextgen-discovery/orchestration/hunter-to-exa-orchestration-v1.js";
import { runPublicSourceAdapter } from "../lib/group-demand-intelligence/nextgen-discovery/adapters/public-source-adapters-v1.js";
import { planExaEvidenceGapResearch } from "../lib/group-demand-intelligence/nextgen-discovery/adapters/exa-hunter-bridge-v1.js";
import { planApifyHunterRetrieval } from "../lib/group-demand-intelligence/nextgen-discovery/adapters/apify-hunter-bridge-v1.js";
import { DISCOVERY_EVIDENCE_CLASS } from "../lib/group-demand-intelligence/nextgen-discovery/shared/discovery-evidence-class-v1.js";
import { resolveHunterConfig } from "../lib/group-demand-intelligence/nextgen-discovery/shared/hunter-config-registry-v1.js";
import { getHunterRunner } from "../lib/group-demand-intelligence/nextgen-discovery/hunters/index.js";
import { NIH_SBPO_VOS_HISTORICAL_NOTICE } from "../lib/group-demand-intelligence/nextgen-discovery/hunters/government-procurement-hunter-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BETHESDA = "recLuxvwwxID7U2B8";

const checks = [];
function check(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => {
      checks.push({ name, ok: true });
      console.log("PASS", name);
    })
    .catch((err) => {
      checks.push({ name, ok: false, err });
      console.error("FAIL", name, err.message);
    });
}

await check("fourteen_segments_preserved_cmi_not_segment", () => {
  assert.equal(Object.values(NEED_DATE_RESEARCH_STRATEGY).length, 14);
  assert.equal(SPECIALIST_HUNTER_LIST.length, 10);
  assert.equal(FIRST_WAVE_HUNTERS.length, 5);
});

await check("normalize_discovery_shape", () => {
  const n = normalizeHunterDiscovery(
    {
      organizationName: "NIH SBPO",
      activityTitle: "Vendor Outreach",
      evidenceUrls: [NIH_SBPO_VOS_HISTORICAL_NOTICE.sourceUrl],
      pursuitThesis: "Bethesda lodging adjacency",
      discoveryEvidenceClass: DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY,
    },
    { hunterId: "government_procurement", hotelId: BETHESDA }
  );
  assert.equal(n.schema, NORMALIZE_DISCOVERY_V1);
  assert.ok(n.organizationName);
  assert.ok(n.sourceProvenance.researchFreshness);
  assert.ok(n.unresolvedEvidenceGaps.length >= 1);
});

await check("dedupe_against_existing_inventory", () => {
  const a = normalizeHunterDiscovery({
    organizationName: "Org A",
    activityTitle: "Event 1",
    activityDate: "2027-01-01",
    evidenceUrls: ["https://example.com/a"],
    pursuitThesis: "thesis",
  });
  const b = normalizeHunterDiscovery({
    organizationName: "Org A",
    activityTitle: "Event 1",
    activityDate: "2027-01-01",
    evidenceUrls: ["https://example.com/a2"],
    pursuitThesis: "thesis",
  });
  const keys = buildDedupeKeySets({
    existingHotelOpportunities: [
      {
        id: "existing_1",
        organizationName: "Org A",
        activityTitle: "Event 1",
        activityDate: "2027-01-01",
      },
    ],
  });
  const out = dedupeNormalizedDiscoveries([a, b], keys);
  assert.equal(out.summary.incremental, 0);
  assert.ok(out.summary.duplicates >= 1);
});

await check("depth_skips_paid_when_evidence_sufficient", () => {
  const row = normalizeHunterDiscovery({
    organizationName: "NIH SBPO",
    activityTitle: NIH_SBPO_VOS_HISTORICAL_NOTICE.activityTitle,
    evidenceUrls: NIH_SBPO_VOS_HISTORICAL_NOTICE.evidenceUrls,
    pursuitThesis: NIH_SBPO_VOS_HISTORICAL_NOTICE.pursuitThesis,
    hotelDemandRelevance: 0.78,
    discoveryEvidenceClass: DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY,
    unresolvedEvidenceGaps: ["named_buyer_or_activity_connected_intermediary_unverified"],
    uncertainties: ["Next session unknown"],
  });
  const d = selectResearchDepthForDiscovery(row, {
    paidAuthorized: false,
    budgetUsd: 0,
    commercialPriorities: { targetSegments: ["government"] },
  });
  assert.equal(d.level, HUNTER_RESEARCH_LEVEL.L1_DISCOVERY_SCREEN);
  assert.equal(d.skipPaidRetrieval, true);
});

await check("first_wave_public_adapters_zero_paid", async () => {
  for (const hunterId of FIRST_WAVE_HUNTERS) {
    const out = await runPublicSourceAdapter(hunterId, {
      hotelId: BETHESDA,
      hotelName: "Bethesda Marriott",
      city: "Bethesda",
      skipDirectPublicFetches: true,
      preferHistoricalOnly: true,
      attemptLivePublic: false,
      rootDir: ROOT,
    });
    assert.equal(out.meta.paidAuthorized, false);
    assert.equal(out.meta.budgetUsdSpent, 0);
    assert.ok(
      (out.discoveries || []).length >= 1,
      `${hunterId} should return at least one historical/public discovery`
    );
  }
});

await check("exa_and_apify_bridges_plan_only", () => {
  const exa = planExaEvidenceGapResearch({
    hunterId: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
    discoveries: [
      {
        organizationName: "NIH",
        unresolvedEvidenceGaps: ["named_buyer_or_activity_connected_intermediary_unverified"],
      },
    ],
    authorizePaid: true,
    budgetUsd: 3,
  });
  assert.equal(exa.execute, false);
  assert.equal(exa.paidAuthorized, false);
  const apify = planApifyHunterRetrieval({
    hunterId: SPECIALIST_HUNTER.SPORTS_HOUSING,
    authorizePaid: true,
    budgetUsd: 3,
  });
  assert.equal(apify.execute, false);
  assert.equal(apify.forceMockOnly, true);
});

await check("zero_paid_gov_orchestration_to_ors", async () => {
  const report = await runHunterToExaOrchestration({
    hunterId: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
    hotelId: BETHESDA,
    hotelName: "Bethesda Marriott",
    territory: "Bethesda MD",
    city: "Bethesda",
    dryRun: true,
    attemptLivePublic: false,
    reportDir: path.join(
      ROOT,
      "reports/gdi-nextgen-hunter-exa-orchestration-v1/test-gov"
    ),
    rootDir: ROOT,
  });
  assert.equal(report.paidAuthorized, false);
  assert.equal(report.paidResearchPlans.executed, false);
  assert.equal(
    report.adapterMeta.discoveryEvidenceClass,
    DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
  );
  assert.ok(report.measurement.counts.incrementalUniqueCandidates >= 1);
  assert.ok(report.qualificationHandoff.summary.routedToOrs >= 1);
  assert.equal(report.orsQualification.salesReady, 0);
  assert.equal(report.publicationSafeguard.autoPublish, false);
  assert.equal(report.publicationSafeguard.cmiIsNotDemandSegment, true);
  assert.ok(fs.existsSync(report.reportPath));
});

await check("independence_disable_one_hunter", async () => {
  const sportsOff = resolveHunterConfig(SPECIALIST_HUNTER.SPORTS_HOUSING, {
    hotelId: BETHESDA,
    hotelOverrides: { sports_housing: { enabled: false } },
  });
  const govOn = resolveHunterConfig(SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT, {
    hotelId: BETHESDA,
    hotelOverrides: { sports_housing: { enabled: false } },
  });
  assert.equal(sportsOff.enabled, false);
  assert.equal(govOn.enabled, true);

  const gov = await getHunterRunner(
    SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT
  ).run({
    hotelId: BETHESDA,
    dryRun: true,
    budgetUsd: 0,
    rootDir: ROOT,
    attemptLivePublic: false,
  });
  assert.ok(gov.metrics.organizations >= 1);

  // Other nine runners still resolve
  for (const id of SPECIALIST_HUNTER_LIST) {
    if (id === SPECIALIST_HUNTER.SPORTS_HOUSING) continue;
    assert.ok(getHunterRunner(id), `runner missing after sports disable: ${id}`);
  }
});

await check("bounded_paid_pilots_prepared_not_executed", () => {
  const pilots = prepareBoundedPaidResearchPilots({
    hunterIds: FIRST_WAVE_HUNTERS.slice(),
    hotelId: BETHESDA,
    budgetUsdPerHunter: 2.5,
  });
  assert.equal(pilots.execute, false);
  assert.equal(pilots.paidAuthorized, false);
  assert.equal(pilots.pilots.length, 5);
  assert.ok(pilots.totalPlannedCeilingUsd > 0);
  assert.ok(pilots.pilots.every((p) => p.awaitingFounderAuthorization === true));
});

const failed = checks.filter((c) => !c.ok);
if (failed.length) {
  console.error(`\n${failed.length}/${checks.length} failed`);
  process.exit(1);
}
console.log(`\n${checks.length}/${checks.length} passed`);
