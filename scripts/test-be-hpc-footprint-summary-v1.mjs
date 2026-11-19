#!/usr/bin/env node
/**
 * Regression tests for BE_HPC_FOOTPRINT_SUMMARY_V1 (founder certification corrections).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BE_HPC_DATA_ORIGINS,
  BE_HPC_FOOTPRINT_CERT_STATES,
  BE_HPC_FOOTPRINT_SUMMARY_V1,
  BE_PHASE2_PILOT_BRANDS,
  buildHpcFootprintSummaryFromRows,
  evaluateHpcCertificationGates,
  getPhase2PilotBySlug,
  summarizePhase2Pilots,
} from "../lib/brand-explorer/be-hpc-footprint-summary-v1.js";
import { BE_CANONICAL_TAB_IDS, BE_FOOTPRINT_METRIC_CONTRACT } from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";
import { CANONICAL_TAB_IDS } from "../lib/brand-explorer/canonical/product-shell.js";
import { auditLegacyNpmWriteGuards } from "../lib/brand-explorer/canonical/legacy-npm-write-guard-audit.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

{
  assert.equal(BE_HPC_FOOTPRINT_SUMMARY_V1, "BE_HPC_FOOTPRINT_SUMMARY_V1");
  assert.equal(BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority, "hpc");
  assert.ok(BE_PHASE2_PILOT_BRANDS.length >= 5);
  const ids = new Set(BE_PHASE2_PILOT_BRANDS.map((p) => p.recordId));
  assert.equal(ids.size, BE_PHASE2_PILOT_BRANDS.length);
  console.log("PASS pilot identity + diversity");
}

{
  const pilot = getPhase2PilotBySlug("autograph-collection");
  assert.equal(pilot.recordId, "recEJCTDj1zrsjPM6");
  const summary = summarizePhase2Pilots();
  assert.equal(summary.writesAuthorized, false);
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

  const synthetic = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: rows,
    dataOrigin: BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE,
    paginationComplete: true,
    independentCertification: true,
  });
  assert.equal(synthetic.certified, false);
  assert.equal(synthetic.certState, BE_HPC_FOOTPRINT_CERT_STATES.SYNTHETIC_FIXTURE_VALIDATION);
  assert.equal(synthetic.dataOrigin, BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE);
  assert.deepEqual(synthetic.metrics, {
    totalOpenHotels: 1,
    totalOpenKeys: 200,
    totalPipelineHotels: 1,
    totalPipelineKeys: 150,
  });
  console.log("PASS synthetic fixtures never CERTIFIED");
}

{
  const liveUnverified = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: [
      { id: "recH1", currentBrand: "Autograph Collection", radarStatus: "Open", roomsKeys: 200 },
      { id: "recH2", currentBrand: "Autograph Collection", radarStatus: "Pipeline", roomsKeys: 150 },
    ],
    dataOrigin: BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE,
    paginationComplete: true,
    independentCertification: false,
  });
  assert.equal(liveUnverified.certified, false);
  assert.equal(liveUnverified.certState, BE_HPC_FOOTPRINT_CERT_STATES.LIVE_UNVERIFIED_INVENTORY);
  console.log("PASS live matching rows alone stay LIVE_UNVERIFIED");
}

{
  const partial = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: [{ id: "recH1", currentBrand: "Autograph Collection", radarStatus: "Open", roomsKeys: 200 }],
    dataOrigin: BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE,
    paginationComplete: false,
    independentCertification: false,
  });
  assert.equal(partial.certState, BE_HPC_FOOTPRINT_CERT_STATES.LIVE_PARTIAL_COVERAGE);
  assert.equal(partial.certified, false);
  console.log("PASS incomplete pagination → LIVE_PARTIAL_COVERAGE");
}

{
  const certified = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: [
      { id: "recH1", currentBrand: "Autograph Collection", radarStatus: "Open", roomsKeys: 200 },
      { id: "recH2", currentBrand: "Autograph Collection", radarStatus: "Pipeline", roomsKeys: 80 },
    ],
    dataOrigin: BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE,
    paginationComplete: true,
    independentCertification: true,
    coverageNotes: ["Independent certification recorded for Autograph pilot"],
  });
  assert.equal(certified.certified, true);
  assert.equal(certified.certState, BE_HPC_FOOTPRINT_CERT_STATES.CERTIFIED);
  assert.equal(certified.certificationGates.identityMappingProven, true);
  assert.equal(certified.certificationGates.paginationComplete, true);
  console.log("PASS independent live certification with full gates");
}

{
  const blocked = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    brandMatchers: ["Autograph Collection"],
    hpcRows: [{ id: "recH1", currentBrand: "Autograph Collection", radarStatus: "Open", roomsKeys: 10 }],
    companyReported: { totalOpenHotels: 999 },
    preferCompanyReported: true,
    dataOrigin: BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE,
    paginationComplete: true,
  });
  assert.equal(blocked.certState, BE_HPC_FOOTPRINT_CERT_STATES.BLOCKED_COMPANY_OVERRIDE);
  console.log("PASS company-reported must not replace HPC");
}

{
  const gates = evaluateHpcCertificationGates({
    dataOrigin: BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE,
    brandBasicsRecordId: "recEJCTDj1zrsjPM6",
    brandMatchers: ["Autograph Collection"],
    recordsMatched: 2,
    paginationComplete: true,
    statusFilteringApplied: true,
    deduplicationApplied: true,
    coverageDocumented: true,
    independentCertification: false,
  });
  assert.equal(gates.certified, false);
  console.log("PASS certification requires independentCertification flag");
}

{
  assert.deepEqual([...CANONICAL_TAB_IDS], [...BE_CANONICAL_TAB_IDS]);
  const tabContracts = fs.readFileSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-tab-contracts.js"),
    "utf8",
  );
  assert.ok(tabContracts.includes("CANONICAL_TAB_IDS"));
  console.log("PASS 10-tab shell parity");
}

{
  const audit = auditLegacyNpmWriteGuards(ROOT);
  assert.equal(audit.factoryCriticalUnguarded.length, 0, audit.factoryCriticalUnguarded.join(","));
  assert.ok(audit.guardModuleOk);
  assert.ok(audit.guardedCount >= audit.factoryCritical.length);
  console.log(`PASS factory-critical legacy npm guards (${audit.guardedCount}/${audit.applyCapableCount} guarded)`);
}

console.log("\nAll BE HPC footprint summary V1 tests passed.");
