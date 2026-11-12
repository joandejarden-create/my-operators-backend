#!/usr/bin/env node
/**
 * Regression + architecture gates for GDI next-gen discovery.
 */

import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SPECIALIST_HUNTER_LIST,
  FIRST_WAVE_HUNTERS,
  HUNTER_HYPOTHESIS_STATUS,
} from "../lib/group-demand-intelligence/nextgen-discovery/specialist-hunter-contract-v1.js";
import {
  createEcosystemGraph,
  buildHotelDemandHypothesis,
  mergeEcosystemDelta,
  ECOSYSTEM_NODE_KIND,
} from "../lib/group-demand-intelligence/nextgen-discovery/organization-demand-ecosystem-v1.js";
import { buildCapabilityGapMatrixV1 } from "../lib/group-demand-intelligence/nextgen-discovery/capability-gap-matrix-v1.js";
import { buildHunterProviderRoutingMatrix } from "../lib/group-demand-intelligence/nextgen-discovery/hunter-provider-routing-v1.js";
import { selectSpecialistHuntersForHotel } from "../lib/group-demand-intelligence/nextgen-discovery/hotel-adaptive-hunter-selection-v1.js";
import { screenHunterCandidate } from "../lib/group-demand-intelligence/nextgen-discovery/hunter-base-v1.js";
import {
  runFirstWavePilot,
  NEXTGEN_PILOT_HOTELS,
  buildWeeklySpecialistHunterPlan,
} from "../lib/group-demand-intelligence/nextgen-discovery/hunter-orchestrator-v1.js";
import {
  listImplementedHunterIds,
  listFirstWaveHunterIds,
  getHunterRunner,
} from "../lib/group-demand-intelligence/nextgen-discovery/hunters/index.js";
import { NEED_DATE_RESEARCH_STRATEGY } from "../lib/group-demand-intelligence/need-date-demand/need-date-demand-contract-v1.js";
import { APIFY_ACTOR_REGISTRY } from "../lib/group-demand-intelligence/research/apify/apify-actor-registry-v1.js";
import { buildWeeklyWorkQueues } from "../lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js";
import {
  DISCOVERY_EVIDENCE_CLASS,
  isSyntheticOrFixtureEvidenceClass,
} from "../lib/group-demand-intelligence/nextgen-discovery/shared/discovery-evidence-class-v1.js";
import {
  resolveHunterConfig,
  HUNTER_CONFIG_DEFAULTS,
} from "../lib/group-demand-intelligence/nextgen-discovery/shared/hunter-config-registry-v1.js";
import { NIH_SBPO_VOS_HISTORICAL_NOTICE } from "../lib/group-demand-intelligence/nextgen-discovery/hunters/government-procurement-hunter-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

async function main() {
  // 1) Fourteen segments preserved — hunters are not a 15th
  const fourteen = Object.values(NEED_DATE_RESEARCH_STRATEGY);
  assert.equal(fourteen.length, 14, "must preserve fourteen demand strategies");
  assert.equal(SPECIALIST_HUNTER_LIST.length, 10, "ten specialist hunters");
  assert.equal(FIRST_WAVE_HUNTERS.length, 5, "five first-wave hunters");

  const gap = buildCapabilityGapMatrixV1();
  assert.equal(gap.fourteenCount, 14);
  assert.equal(gap.summary.new + gap.summary.partial + gap.summary.exists, 10);
  assert.ok(gap.nonGoals.includes("No fifteenth demand segment"));

  // 2) Apify registry recovered
  assert.ok(APIFY_ACTOR_REGISTRY.length >= 5, "Apify actor registry recovered");
  const routing = buildHunterProviderRoutingMatrix();
  assert.equal(routing.rows.length, 10);
  assert.ok(routing.apifyRegistrySize >= 5);
  assert.ok(routing.lessonsRetained.some((l) => /Party Bus/i.test(l)));

  // 3) Ecosystem: trigger ≠ Sales Ready
  const hyp = buildHotelDemandHypothesis({
    hotelId: "recLuxvwwxID7U2B8",
    organizationName: "Example Org",
    demandTrigger: "announcement only",
    evidenceUrls: [],
    sourceHunter: "government_procurement",
  });
  assert.equal(hyp.hypothesisStatus, HUNTER_HYPOTHESIS_STATUS.DEMAND_TRIGGER_INFERRED);
  assert.match(hyp.notSalesReadyReason, /never auto Sales Ready/i);

  const graph = createEcosystemGraph({ hotelId: "recLuxvwwxID7U2B8" });
  mergeEcosystemDelta(graph, {
    nodes: [
      {
        id: "org_test",
        kind: ECOSYSTEM_NODE_KIND.ORGANIZATION,
        name: "Example Org",
        evidenceUrls: ["https://example.com"],
      },
    ],
    edges: [],
  });
  assert.equal(graph.stats.organizations, 1);

  // 4) Party Bus gate
  const rejected = screenHunterCandidate({
    organizationName: "Party Bus NYC",
    activityTitle: "club crawl",
  });
  assert.equal(rejected.accepted, false);

  // 5) Hotel-adaptive: does not copy other hotel; not all ten
  const selA = selectSpecialistHuntersForHotel({
    hotelId: "recLuxvwwxID7U2B8",
    commercialPriorities: {
      source: "HOTEL_A",
      targetSegments: ["sports", "university"],
      weekendFocus: true,
    },
    maxHunters: 3,
  });
  const selB = selectSpecialistHuntersForHotel({
    hotelId: "recnPB3Q02NO8Ma1o",
    commercialPriorities: {
      source: "HOTEL_B",
      targetSegments: ["government", "project"],
      weekdayFocus: true,
    },
    maxHunters: 3,
  });
  assert.equal(selA.copiedFromOtherHotel, false);
  assert.ok(selA.selectedHunterIds.length <= 3);
  assert.ok(selA.selectedHunterIds.includes("sports_housing"));
  assert.ok(selB.selectedHunterIds.includes("government_procurement"));
  assert.notDeepEqual(selA.selectedHunterIds, selB.selectedHunterIds);

  // 6) Weekly soft plan
  const weeklyPlan = buildWeeklySpecialistHunterPlan({
    hotelId: "recLuxvwwxID7U2B8",
    commercialPriorities: { targetSegments: ["sports"] },
  });
  assert.equal(weeklyPlan.executePaid, false);

  const queues = buildWeeklyWorkQueues({
    hotelId: "recLuxvwwxID7U2B8",
    hotelConfig: {
      hotelId: "recLuxvwwxID7U2B8",
      commercialPriorities: { targetSegments: ["corporate", "sports"] },
    },
    opportunities: [],
  });
  assert.ok(queues.specialistHunterPlan);
  assert.equal(queues.specialistHunterPlan.executePaid, false);

  // 7) Ten independent runners; first-wave subset for pilot selection
  assert.equal(listImplementedHunterIds().length, 10);
  assert.deepEqual(
    listFirstWaveHunterIds().sort(),
    FIRST_WAVE_HUNTERS.slice().sort()
  );
  for (const id of SPECIALIST_HUNTER_LIST) {
    assert.ok(getHunterRunner(id), `runner missing for ${id}`);
    assert.ok(HUNTER_CONFIG_DEFAULTS[id], `config missing for ${id}`);
  }
  // Config isolation: disabling sports does not disable government
  const sportsOff = resolveHunterConfig("sports_housing", {
    hotelId: "recLuxvwwxID7U2B8",
    hotelOverrides: { sports_housing: { enabled: false } },
  });
  const govOn = resolveHunterConfig("government_procurement", {
    hotelId: "recLuxvwwxID7U2B8",
    hotelOverrides: { sports_housing: { enabled: false } },
  });
  assert.equal(sportsOff.enabled, false);
  assert.equal(govOn.enabled, true);

  const pilot = await runFirstWavePilot({
    hotels: NEXTGEN_PILOT_HOTELS,
    rootDir: ROOT,
    reportRoot: path.join(
      ROOT,
      "reports/gdi-nextgen-discovery-architecture-v1/pilot-test"
    ),
  });
  assert.equal(pilot.budgetUsdSpent, 0);
  assert.equal(pilot.paidAuthorized, false);
  assert.equal(pilot.portfolioTotals.salesReady, 0);
  assert.equal(pilot.portfolioTotals.autoPublished, 0);
  assert.ok(pilot.portfolioTotals.incrementalHypotheses >= 16);
  assert.ok(pilot.portfolioTotals.rejected >= 4, "Party Bus rejection across sports runs");

  // Sports housing must reject Party Bus
  const sportsRuns = pilot.hotels.flatMap((h) =>
    h.moduleRuns.filter((m) => m.hunterId === "sports_housing")
  );
  assert.ok(sportsRuns.every((m) => (m.metrics?.rejected || 0) >= 1));
  assert.ok(
    sportsRuns.every((m) => {
      const cls =
        m.discoveryEvidenceClass || m.discoverMeta?.discoveryEvidenceClass;
      return (
        cls === DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY ||
        isSyntheticOrFixtureEvidenceClass(cls)
      );
    }),
    "sports output must be historical public replay or synthetic fixture — never unlabeled live-paid"
  );

  // 8) Zero-paid Government Procurement historical path
  const gov = await getHunterRunner("government_procurement").run({
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    dryRun: true,
    budgetUsd: 0,
    rootDir: ROOT,
    reportDir: path.join(
      ROOT,
      "reports/gdi-nextgen-discovery-architecture-v1/gov-zero-paid-demo"
    ),
  });
  assert.equal(gov.paidAuthorized, false);
  assert.equal(gov.budgetUsdSpent, 0);
  assert.equal(
    gov.discoveryEvidenceClass,
    DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
  );
  assert.equal(gov.metrics.salesReadyEmitted, 0);
  assert.equal(gov.publicationSafeguard.autoPublish, false);
  assert.ok(gov.metrics.organizations >= 1);
  assert.ok(gov.qualificationHandoff.summary.salesReady === 0);
  assert.ok(
    String(NIH_SBPO_VOS_HISTORICAL_NOTICE.sourceUrl).includes("sam.gov"),
    "historical notice must cite SAM.gov"
  );
  assert.ok(
    gov.incremental.every((h) => h.publicationEligible === false),
    "no automatic publication"
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        fourteenSegments: fourteen.length,
        hunters: SPECIALIST_HUNTER_LIST.length,
        implementedRunners: listImplementedHunterIds().length,
        firstWave: listFirstWaveHunterIds().length,
        apifyActors: APIFY_ACTOR_REGISTRY.length,
        pilotIncremental: pilot.portfolioTotals.incrementalHypotheses,
        pilotRejected: pilot.portfolioTotals.rejected,
        budgetUsdSpent: pilot.budgetUsdSpent,
        govEvidenceClass: gov.discoveryEvidenceClass,
        govOrsRouted: gov.qualificationHandoff.summary.routedToOrs,
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
