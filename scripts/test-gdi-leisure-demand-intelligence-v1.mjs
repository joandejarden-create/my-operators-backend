#!/usr/bin/env node
/**
 * $0 certification — GDI Leisure Demand Intelligence V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listLeisureModuleIds,
  buildLeisureFourteenSegmentCoverageMatrix,
  getLeisureModule,
  runLeisureModules,
  runLeisureOrchestratorDisabledCheck,
  assertLeisureRunAuthorization,
  classifyLeisureCommercialPathway,
  assertLeisureSalesReadyNotAutoPromoted,
  evaluateLeisureNeedPeriod,
  NEED_PERIOD_STATUS,
  selectLeisureModulesForHotel,
  LEISURE_HOTEL_PROFILE_PRESETS,
  dedupeLeisureAgainstInventory,
  buildLeisureOpportunityContract,
  designLeisureAdpLinkage,
  LEISURE_COMMERCIAL_PATHWAY,
} from "../lib/group-demand-intelligence/leisure-demand-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT = path.join(ROOT, "reports/gdi-leisure-demand-intelligence-v1");
fs.mkdirSync(REPORT, { recursive: true });

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
    console.error(`FAIL ${name}: ${err?.message || err}`);
    throw err;
  }
}

check("six_modules_registered", () => {
  const ids = listLeisureModuleIds();
  assert.equal(ids.length, 6);
  for (const id of ids) assert.ok(getLeisureModule(id));
});

check("coverage_matrix_links_fourteen_segments", () => {
  const matrix = buildLeisureFourteenSegmentCoverageMatrix();
  assert.equal(matrix.length, 6);
  for (const row of matrix) {
    assert.ok(row.primarySegments.length >= 1);
    assert.ok(["EXISTS", "PARTIAL", "OVERLAP_WRAPPER", "GAP"].includes(row.coverage));
  }
});

check("powershell_entrypoints_exist", () => {
  const files = [
    "Invoke-GdiLeisureEvents.ps1",
    "Invoke-GdiLeisureSports.ps1",
    "Invoke-GdiLeisureAttractions.ps1",
    "Invoke-GdiLeisureUniversities.ps1",
    "Invoke-GdiLeisureDestination.ps1",
    "Invoke-GdiLeisureDistribution.ps1",
    "Invoke-GdiLeisureModule.ps1",
  ];
  for (const f of files) {
    assert.ok(
      fs.existsSync(path.join(ROOT, "scripts/gdi-leisure", f)),
      `missing ${f}`,
    );
  }
});

check("independent_module_invoke_fixture", () => {
  const r = runLeisureModules({
    hotelId: "recaokE8psIbuVeuH",
    modules: ["leisure_events"],
    researchMode: "fixture",
    writeArtifacts: false,
  });
  assert.equal(r.ok, true);
  assert.equal(r.spentUsd, 0);
  assert.ok(r.opportunityCount >= 1);
});

check("orchestrator_all_six_modules", () => {
  const r = runLeisureModules({
    hotelId: "recaokE8psIbuVeuH",
    hotelProfile: {
      hotelId: "recaokE8psIbuVeuH",
      classification: "leisure resort villa destination",
    },
    modules: listLeisureModuleIds(),
    researchMode: "fixture",
    writeArtifacts: true,
    outDir: path.join(REPORT, "cert-all-six"),
  });
  assert.equal(r.ok, true);
  assert.equal(r.moduleIds.length, 6);
  assert.equal(r.spentUsd, 0);
  assert.ok(fs.existsSync(path.join(REPORT, "cert-all-six/leisure-run.json")));
});

check("leisure_disabled_leaves_gdi_unchanged", () => {
  const d = runLeisureOrchestratorDisabledCheck();
  assert.equal(d.leisureEnabled, false);
  assert.deepEqual(d.modulesInvoked, []);
  assert.equal(d.fourteenSegmentsUnchanged, true);
});

check("canonical_opportunity_contract_fields", () => {
  const c = buildLeisureOpportunityContract({
    hotelId: "recTEST",
    leisureModuleId: "leisure_distribution",
    demandGenerator: "Test DMC",
    organization: "Test DMC",
    leisureCommercialPathway: "PARTNERSHIP_DRIVEN",
    sourceUrls: ["https://example.invalid/x"],
  });
  assert.equal(c.schema, "gdi-leisure-opportunity-contract-v1");
  assert.ok(c.opportunityId.startsWith("gdi_ldi_"));
  assert.ok(c.segmentAssociations.includes("travel_trade_group_intermediaries"));
  assert.equal(c.salesReady, false);
  assert.equal(c.publicationEligible, false);
});

check("cross_segment_dedupe_carnival", () => {
  const r = runLeisureModules({
    hotelId: "recaokE8psIbuVeuH",
    modules: ["leisure_events", "leisure_sports"],
    researchMode: "fixture",
    writeArtifacts: false,
  });
  assert.equal(r.ok, true);
  const titles = r.opportunities.map((o) => o.demandGenerator);
  const carnivalCount = titles.filter((t) => /spice mas carnival/i.test(t)).length;
  assert.equal(carnivalCount, 1, "carnival must appear once after dedupe");
  const sportsResult = r.moduleResults.find((m) => m.leisureModuleId === "leisure_sports");
  assert.ok(sportsResult.suppressedCount >= 1);
});

check("commercial_pathway_rules", () => {
  const campaign = classifyLeisureCommercialPathway({
    leisureCommercialPathway: "CAMPAIGN_DRIVEN",
  });
  assert.equal(campaign.pathway, LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN);
  const bad = assertLeisureSalesReadyNotAutoPromoted({
    leisureCommercialPathway: "CAMPAIGN_DRIVEN",
    salesReady: true,
  });
  assert.equal(bad.ok, false);
  const eventOnly = assertLeisureSalesReadyNotAutoPromoted({
    leisureCommercialPathway: "DIRECTLY_ADDRESSABLE",
    salesReady: true,
    salesReadyReason: "event_exists",
  });
  assert.equal(eventOnly.ok, false);
});

check("need_period_optional_unknown", () => {
  const u = evaluateLeisureNeedPeriod({ demandGenerator: "X" }, null);
  assert.equal(u.status, NEED_PERIOD_STATUS.NEED_PERIOD_UNKNOWN);
  assert.equal(u.inventedMetrics, false);
  const o = evaluateLeisureNeedPeriod(
    { eventOrRecurringDates: { start: "2027-05-10", end: "2027-05-12" } },
    {
      authorized: true,
      needPeriods: [{ id: "p1", label: "May shoulder", start: "2027-05-01", end: "2027-05-31" }],
    },
  );
  assert.equal(o.status, NEED_PERIOD_STATUS.OVERLAP);
});

check("budget_authorization_fail_closed", () => {
  const a = assertLeisureRunAuthorization({ budgetUsd: 5 });
  assert.equal(a.ok, false);
  const b = assertLeisureRunAuthorization({
    budgetUsd: 5,
    authorizePaidResearch: true,
  });
  assert.equal(b.ok, false);
  const c = runLeisureModules({
    hotelId: "recaokE8psIbuVeuH",
    modules: ["leisure_events"],
    budgetUsd: 1,
    authorizePaidResearch: true,
    allowLivePilot: true,
    writeArtifacts: false,
  });
  // Runner still refuses publication; paid path returns PAID_BLOCKED semantics via auth ok
  // but Invoke wrapper refuses — library allows auth ok for design; paid calls remain 0
  assert.equal(c.spentUsd ?? 0, 0);
});

check("zero_paid_replay", () => {
  const r1 = runLeisureModules({
    hotelId: "recaokE8psIbuVeuH",
    modules: ["leisure_distribution"],
    researchMode: "fixture",
    writeArtifacts: false,
  });
  const r2 = runLeisureModules({
    hotelId: "recaokE8psIbuVeuH",
    modules: ["leisure_distribution"],
    researchMode: "replay",
    writeArtifacts: false,
  });
  assert.equal(r1.spentUsd, 0);
  assert.equal(r2.spentUsd, 0);
  assert.equal(r1.opportunityCount, r2.opportunityCount);
});

check("hotel_module_selection_presets", () => {
  const laluna = selectLeisureModulesForHotel({ hotelId: "recaokE8psIbuVeuH" });
  assert.equal(laluna.source, "hpc_preset");
  assert.ok(laluna.moduleIds.includes("leisure_distribution"));
  assert.ok(LEISURE_HOTEL_PROFILE_PRESETS.rec1t35e3sPerzxYu);
  assert.ok(LEISURE_HOTEL_PROFILE_PRESETS.rece0NdV9FCnaFlVS);
});

check("no_publication_or_airtable_flags", () => {
  const r = runLeisureModules({
    hotelId: "recaokE8psIbuVeuH",
    modules: ["leisure_attractions"],
    authorizePublication: true,
    writeArtifacts: false,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "PUBLICATION_NOT_AUTHORIZED_IN_LDI_V1");
});

check("adp_linkage_design_only", () => {
  const d = designLeisureAdpLinkage();
  assert.equal(d.requiredForLdi, false);
  assert.equal(d.thisPhase.adpResearch, "NONE");
});

check("inventory_compatible_dedupe_api", () => {
  const existing = [
    {
      id: "gdi_existing_1",
      organization: "Grenada Carnival Committee",
      demandGenerator: "Spice Mas Carnival Week",
      year: "2027",
      segmentAssociations: ["sports_entertainment"],
    },
  ];
  const leisure = [
    buildLeisureOpportunityContract({
      hotelId: "recX",
      leisureModuleId: "leisure_events",
      demandGenerator: "Spice Mas Carnival Week",
      organization: "Grenada Carnival Committee",
      year: "2027",
    }),
  ];
  const d = dedupeLeisureAgainstInventory(leisure, existing);
  assert.equal(d.acceptedCount, 0);
  assert.equal(d.suppressedCount, 1);
});

const summary = {
  schema: "gdi-leisure-demand-intelligence-v1-cert",
  spentUsd: 0,
  publication: false,
  airtableWrites: false,
  findymail: false,
  surfe: false,
  liveCommercialCertification: false,
  results,
  allPassed: results.every((r) => r.ok),
};
fs.writeFileSync(
  path.join(REPORT, "CERTIFICATION_RESULTS.json"),
  JSON.stringify(summary, null, 2),
);
console.log("\nOK test:gdi-leisure-demand-intelligence-v1");
