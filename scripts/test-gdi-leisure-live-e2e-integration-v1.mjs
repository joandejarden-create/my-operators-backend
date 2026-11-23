#!/usr/bin/env node
/**
 * $0 cert — LDI live E2E integration (mocked providers → STAGED plan).
 * No paid Exa / Findymail / Surfe / Airtable / publication.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_LEISURE_LIVE_E2E_INTEGRATION_V1,
  DESIGN_HOTELS_LDI_COHORT,
  evaluateLeisureFinalizationGate,
  planLeisureStagedPackage,
  runLeisureLiveE2eIntegration,
  certifyLeisureLiveE2eDesignHotels,
  selectLeisureModulesForHotel,
  runLeisureModules,
} from "../lib/group-demand-intelligence/leisure-demand-intelligence/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(ROOT, "reports/gdi-leisure-live-e2e-integration-v1");
fs.mkdirSync(REPORT, { recursive: true });

const results = [];
async function check(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
    console.error(`FAIL ${name}: ${e.message || e}`);
    throw e;
  }
}

await check("design_hotels_cohort_three", () => {
  assert.equal(DESIGN_HOTELS_LDI_COHORT.length, 3);
  assert.deepEqual(
    DESIGN_HOTELS_LDI_COHORT.map((h) => h.hotelId).sort(),
    ["rec1t35e3sPerzxYu", "recaokE8psIbuVeuH", "rece0NdV9FCnaFlVS"].sort(),
  );
});

await check("laluna_memmo_prioritize_distribution_attractions_events", () => {
  for (const id of ["recaokE8psIbuVeuH", "rec1t35e3sPerzxYu"]) {
    const sel = selectLeisureModulesForHotel({ hotelId: id });
    assert.equal(sel.moduleIds[0], "leisure_distribution");
    assert.equal(sel.moduleIds[1], "leisure_attractions");
    assert.equal(sel.moduleIds[2], "leisure_events");
  }
});

await check("condesa_relevant_modules", () => {
  const sel = selectLeisureModulesForHotel({ hotelId: "rece0NdV9FCnaFlVS" });
  assert.ok(sel.moduleIds.includes("leisure_events"));
  assert.ok(sel.moduleIds.includes("leisure_attractions"));
  assert.ok(sel.moduleIds.includes("leisure_distribution"));
  assert.ok(sel.moduleIds.includes("leisure_universities"));
});

await check("mock_live_invokes_fourteen_segment_cells", async () => {
  const r = await runLeisureLiveE2eIntegration({
    hotelId: "recaokE8psIbuVeuH",
    modules: ["leisure_distribution", "leisure_attractions", "leisure_events"],
    researchMode: "mock_live",
    writeArtifacts: true,
    outDir: path.join(REPORT, "laluna-mock-live"),
  });
  assert.equal(r.ok, true);
  assert.equal(r.spentUsd, 0);
  assert.ok(r.fourteenSegmentCalls.length >= 3);
  assert.ok(
    r.fourteenSegmentCalls.every((c) => c.retrievalMode === "MOCK_PROVIDER"),
  );
  assert.equal(r.stagedPlan.writeApplied, false);
  assert.ok(fs.existsSync(path.join(REPORT, "laluna-mock-live/STAGED_PLAN.json")));
});

await check("runner_delegates_mock_live", async () => {
  const r = await runLeisureModules({
    hotelId: "rec1t35e3sPerzxYu",
    modules: ["leisure_events"],
    researchMode: "mock_live",
    writeArtifacts: false,
  });
  assert.equal(r.ok, true);
  assert.equal(r.schema, GDI_LEISURE_LIVE_E2E_INTEGRATION_V1);
  assert.equal(r.spentUsd, 0);
});

await check("finalization_gate_blocks_incomplete", () => {
  const bad = evaluateLeisureFinalizationGate({
    demandGenerator: "X",
    leisureCommercialPathway: "DIRECTLY_ADDRESSABLE",
  });
  assert.equal(bad.finalizedReady, false);
  assert.ok(bad.reasons.includes("MISSING_EVIDENCE_URLS"));
});

await check("staged_plan_never_writes", () => {
  const plan = planLeisureStagedPackage({
    hotelId: "rece0NdV9FCnaFlVS",
    opportunities: [
      {
        opportunityId: "t1",
        demandGenerator: "Org",
        organization: "Org",
        sourceUrls: ["https://example.invalid/a"],
        roomNightDemandMechanism: "visitor nights via attraction partnership",
        leisureCommercialPathway: "PARTNERSHIP_DRIVEN",
        commercialDecisionMakerOrEntryPoint: {
          organizationEntry: "Partnerships desk",
        },
        recommendedSalesOrMarketingAction: "Open partnership conversation",
      },
    ],
  });
  assert.equal(plan.writeApplied, false);
  assert.equal(plan.kind, "STAGED_PLAN");
  assert.equal(plan.opportunityCount, 1);
});

await check("design_hotels_cohort_cert_zero_spend", async () => {
  const cert = await certifyLeisureLiveE2eDesignHotels({
    writeArtifacts: true,
    outDir: path.join(REPORT, "design-hotels-cert"),
  });
  assert.equal(cert.passed, true);
  assert.equal(cert.spentUsdTotal, 0);
  assert.equal(cert.gates.findymail, false);
  assert.equal(cert.gates.surfe, false);
  assert.equal(cert.gates.airtableOpportunityWrites, false);
  assert.equal(cert.gates.customerPublication, false);
  assert.equal(cert.gates.paidExa, false);
  assert.equal(cert.gates.fourteenSegmentsPreserved, true);
  fs.writeFileSync(
    path.join(REPORT, "CERTIFICATION_RESULTS.json"),
    JSON.stringify(cert, null, 2),
  );
});

const summary = {
  schema: "gdi-leisure-live-e2e-integration-v1-cert",
  spentUsd: 0,
  results,
  allPassed: results.every((r) => r.ok),
};
fs.writeFileSync(
  path.join(REPORT, "TEST_RESULTS.json"),
  JSON.stringify(summary, null, 2),
);
console.log(`\nOK ${results.length} live E2E integration gates`);
