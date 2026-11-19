#!/usr/bin/env node
/**
 * Regression tests for BE_HPC_FOOTPRINT_SUMMARY_V1.
 * Pure / local — no Airtable writes. Live fetch is not required for PASS.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BE_HPC_FOOTPRINT_CERT_STATES,
  BE_HPC_FOOTPRINT_SUMMARY_V1,
  BE_PHASE2_PILOT_BRANDS,
  buildHpcFootprintSummaryFromRows,
  getPhase2PilotBySlug,
  summarizePhase2Pilots,
} from "../lib/brand-explorer/be-hpc-footprint-summary-v1.js";
import { BE_CANONICAL_TAB_IDS, BE_FOOTPRINT_METRIC_CONTRACT } from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";
import { CANONICAL_TAB_IDS } from "../lib/brand-explorer/canonical/product-shell.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

{
  assert.equal(BE_HPC_FOOTPRINT_SUMMARY_V1, "BE_HPC_FOOTPRINT_SUMMARY_V1");
  assert.equal(BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority, "hpc");
  assert.ok(BE_PHASE2_PILOT_BRANDS.length >= 5);
  const ids = new Set(BE_PHASE2_PILOT_BRANDS.map((p) => p.recordId));
  assert.equal(ids.size, BE_PHASE2_PILOT_BRANDS.length, "pilot Brand Basics ids must be unique");
  for (const p of BE_PHASE2_PILOT_BRANDS) {
    assert.match(p.recordId, /^rec[a-zA-Z0-9]{10,}$/);
    assert.ok(p.hpcMatchers.length >= 1);
    assert.ok(p.affiliationModel);
    assert.ok(p.operatingModel);
  }
  const models = new Set(BE_PHASE2_PILOT_BRANDS.map((p) => p.affiliationModel));
  assert.ok(models.size >= 3, "pilots must span multiple affiliation models");
  console.log("PASS pilot identity + diversity");
}

{
  const pilot = getPhase2PilotBySlug("autograph-collection");
  assert.equal(pilot.recordId, "recEJCTDj1zrsjPM6");
  const summary = summarizePhase2Pilots();
  assert.equal(summary.writesAuthorized, false);
  assert.equal(summary.paidResearchAuthorized, false);
  assert.equal(summary.autoPublishAuthorized, false);
  console.log("PASS pilot lookup + safety flags");
}

{
  const rows = [
    { id: "recH1", currentBrand: "Autograph Collection", radarStatus: "Open", roomsKeys: 200 },
    { id: "recH1", currentBrand: "Autograph Collection", radarStatus: "Open", roomsKeys: 200 },
    { id: "recH2", currentBrand: "Autograph Collection", radarStatus: "Pipeline", roomsKeys: 150 },
    { id: "recH3", currentBrand: "Autograph Collection", radarStatus: "Closed", roomsKeys: 90 },
    { id: "recH4", currentBrand: "Other Brand", radarStatus: "Open", roomsKeys: 300 },
  ];
  const out = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: rows,
  });
  assert.equal(out.contract, BE_HPC_FOOTPRINT_SUMMARY_V1);
  assert.equal(out.certified, true);
  assert.equal(out.certState, BE_HPC_FOOTPRINT_CERT_STATES.CERTIFIED);
  assert.equal(out.fallbackRecommended, false);
  assert.deepEqual(out.metrics, {
    totalOpenHotels: 1,
    totalOpenKeys: 200,
    totalPipelineHotels: 1,
    totalPipelineKeys: 150,
  });
  assert.equal(out.census.recordsMatched, 2);
  assert.equal(out.writes, 0);
  assert.equal(out.source.table, "Hotel Property Census");
  assert.equal(out.adpGdi.speculativeMetrics, false);
  console.log("PASS HPC Open/Pipeline aggregate + certification");
}

{
  const blocked = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: [{ id: "recH1", currentBrand: "Autograph Collection", radarStatus: "Open", roomsKeys: 10 }],
    companyReported: { totalOpenHotels: 999 },
    preferCompanyReported: true,
  });
  assert.equal(blocked.certState, BE_HPC_FOOTPRINT_CERT_STATES.BLOCKED_COMPANY_OVERRIDE);
  assert.equal(blocked.available, false);
  console.log("PASS company-reported must not replace HPC");
}

{
  const empty = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: [],
  });
  assert.equal(empty.certified, false);
  assert.equal(empty.fallbackRecommended, true);
  assert.equal(empty.certState, BE_HPC_FOOTPRINT_CERT_STATES.INSUFFICIENT_MATCH);
  console.log("PASS empty match stays uncertified (no fabricated counts)");
}

{
  const offline = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    liveFetchOk: false,
  });
  assert.equal(offline.certState, BE_HPC_FOOTPRINT_CERT_STATES.LIVE_UNAVAILABLE);
  console.log("PASS live unavailable does not invent metrics");
}

{
  assert.deepEqual([...CANONICAL_TAB_IDS], [...BE_CANONICAL_TAB_IDS]);
  assert.equal(CANONICAL_TAB_IDS.length, 10);
  const tabContracts = fs.readFileSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-tab-contracts.js"),
    "utf8",
  );
  assert.ok(tabContracts.includes("CANONICAL_TAB_IDS"));
  assert.ok(tabContracts.includes("atelier-footprint"));
  assert.ok(tabContracts.includes("atelier-insight"));
  console.log("PASS 10-tab shell parity with SoT + tab contracts");
}

console.log("\nAll BE HPC footprint summary V1 tests passed.");
