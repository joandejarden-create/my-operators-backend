#!/usr/bin/env node
/**
 * Regression tests for BE_AIRTABLE_INTEGRATION_SOT_V1.
 * Pure / local — no Airtable writes. Optional live reads are not required for PASS.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BE_AIRTABLE_INTEGRATION_SOT_V1,
  BE_BRAND_SETUP_CHILD_TABLES,
  BE_CANONICAL_TAB_IDS,
  BE_EXTERNAL_READ_CONTRACTS,
  BE_FOOTPRINT_METRIC_CONTRACT,
  BE_HPC_TABLE,
  BE_HOTEL_CENSUS_TABLE,
  BE_IDENTITY_CONTRACT,
  BE_PRESENTATION_TABLE,
  BE_TAB_SOURCE_OF_TRUTH,
  aggregateOpenPipelineCounts,
  assertCanonicalTabOrder,
  buildCoverageLimitations,
  companyReportedMustNotReplaceHpc,
  dedupeByRecordId,
  filterHpcRowsForBrandFootprint,
  getTabSoT,
  normalizeExactKey,
  summarizeContract,
} from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

{
  const s = summarizeContract();
  assert.equal(s.contract, BE_AIRTABLE_INTEGRATION_SOT_V1);
  assert.equal(s.tabCount, 10);
  assert.equal(s.footprintDesignatedAuthority, "hpc");
  assert.equal(s.footprintTransitionalAuthority, "hotel_census");
  assert.equal(BE_HPC_TABLE, "Hotel Property Census");
  assert.equal(BE_HOTEL_CENSUS_TABLE, "Hotel Census");
  assert.equal(BE_PRESENTATION_TABLE, "Brand Setup - Brand Explorer Presentation");
  assert.ok(BE_BRAND_SETUP_CHILD_TABLES.includes("Brand Setup - Brand Footprint"));
  console.log("PASS contract identity + tables");
}

{
  const order = assertCanonicalTabOrder(BE_CANONICAL_TAB_IDS);
  assert.equal(order.ok, true);
  const drift = assertCanonicalTabOrder([
    ...BE_CANONICAL_TAB_IDS.slice(0, 9),
    "atelier-commercial",
  ]);
  assert.equal(drift.ok, false);
  console.log("PASS canonical 10-tab order");
}

{
  assert.equal(BE_TAB_SOURCE_OF_TRUTH.length, 10);
  for (const id of BE_CANONICAL_TAB_IDS) {
    const row = getTabSoT(id);
    assert.ok(row, `missing SoT for ${id}`);
    assert.equal(row.tabId, id);
    assert.ok(row.primaryOwner);
    assert.ok(row.writeClass);
  }
  const fp = getTabSoT("atelier-footprint");
  assert.equal(fp.primaryOwner, "hpc");
  assert.equal(fp.writeClass, "system_owned_no_factory_write");
  console.log("PASS ten-tab SoT ownership");
}

{
  assert.equal(BE_IDENTITY_CONTRACT.primaryKey.table, "Brand Setup - Brand Basics");
  assert.equal(BE_IDENTITY_CONTRACT.primaryKey.field, "id");
  assert.ok(BE_IDENTITY_CONTRACT.universeFilter.includes("Active"));
  assert.ok(BE_IDENTITY_CONTRACT.universeFilter.includes("Live"));
  assert.equal(normalizeExactKey("  Courtyard by Marriott "), "courtyard by marriott");
  console.log("PASS identity mapping contract");
}

{
  const rows = [
    { id: "rec1", currentBrand: "Courtyard", radarStatus: "Open", roomsKeys: 120 },
    { id: "rec1", currentBrand: "Courtyard", radarStatus: "Open", roomsKeys: 120 },
    { id: "rec2", currentBrand: "Courtyard", radarStatus: "Pipeline", roomsKeys: 80 },
    { id: "rec3", currentBrand: "Courtyard", radarStatus: "Closed", roomsKeys: 50 },
    { id: "rec4", currentBrand: "Other Brand", radarStatus: "Open", roomsKeys: 200 },
    { currentBrand: "Courtyard", radarStatus: "Open", roomsKeys: 10 },
  ];
  const { matched, excluded } = filterHpcRowsForBrandFootprint(rows, {
    brandMatchers: ["Courtyard"],
  });
  assert.equal(matched.length, 2);
  assert.equal(excluded.noBrandMatch, 1);
  assert.equal(excluded.nonQualifyingStatus, 1);
  assert.equal(excluded.missingId, 1);
  const deduped = dedupeByRecordId(rows);
  assert.equal(deduped.length, 4);
  const metrics = aggregateOpenPipelineCounts(matched);
  assert.equal(metrics.totalOpenHotels, 1);
  assert.equal(metrics.totalOpenKeys, 120);
  assert.equal(metrics.totalPipelineHotels, 1);
  assert.equal(metrics.totalPipelineKeys, 80);
  assert.ok(metrics.byRegion && typeof metrics.byRegion === "object");
  console.log("PASS HPC join / dedupe / Open|Pipeline filters");
}

{
  const stale = companyReportedMustNotReplaceHpc({
    hpcMetrics: { totalOpenHotels: 10 },
    companyReported: { totalOpenHotels: 999 },
    preferCompanyReported: true,
  });
  assert.equal(stale.ok, false);
  const ok = companyReportedMustNotReplaceHpc({
    hpcMetrics: { totalOpenHotels: 10 },
    companyReported: { totalOpenHotels: 999 },
    preferCompanyReported: false,
  });
  assert.equal(ok.ok, true);
  const limitations = buildCoverageLimitations({
    usingTransitional: true,
    unmatchedAliases: ["legacy-name"],
    excludedStatuses: ["Closed"],
    recordsMatched: 0,
  });
  assert.ok(limitations.some((l) => /transitional/i.test(l)));
  assert.ok(limitations.some((l) => /Unmatched alias/i.test(l)));
  assert.ok(limitations.some((l) => /No qualifying inventory/i.test(l)));
  assert.ok(BE_FOOTPRINT_METRIC_CONTRACT.requiredMetricKeys.includes("totalOpenHotels"));
  console.log("PASS company-reported vs HPC + coverage limitations");
}

{
  assert.equal(BE_EXTERNAL_READ_CONTRACTS.adpBrandLevel.allowed, false);
  assert.equal(BE_EXTERNAL_READ_CONTRACTS.gdiBrandLevel.allowed, false);
  assert.equal(BE_EXTERNAL_READ_CONTRACTS.adpBrandLevel.status, "RESERVED_READ_ONLY");
  assert.equal(BE_EXTERNAL_READ_CONTRACTS.gdiBrandLevel.status, "RESERVED_READ_ONLY");
  console.log("PASS ADP/GDI reserved read contracts (no speculative metrics)");
}

{
  const atelierPath = path.join(ROOT, "public/js/brand-explorer-atelier-from-api.js");
  const src = fs.readFileSync(atelierPath, "utf8");
  for (const id of BE_CANONICAL_TAB_IDS) {
    assert.ok(src.includes(`'${id}'`) || src.includes(`"${id}"`), `atelier missing ${id}`);
  }
  console.log("PASS atelier renderer includes all 10 canonical tab ids");
}

{
  const brandLibrary = fs.readFileSync(path.join(ROOT, "api/brand-library.js"), "utf8");
  assert.ok(brandLibrary.includes(BE_PRESENTATION_TABLE));
  assert.ok(brandLibrary.includes("Brand Setup - Brand Basics"));
  assert.ok(brandLibrary.includes("censusSummary"));
  const censusSummary = fs.readFileSync(
    path.join(ROOT, "lib/hotel-census/build-brand-census-summary.js"),
    "utf8",
  );
  assert.ok(censusSummary.includes("Brand Alias Mapping") || censusSummary.includes("BRAND_ALIAS"));
  const footprintTrust = fs.readFileSync(path.join(ROOT, "lib/brand-explorer-footprint-trust.js"), "utf8");
  assert.ok(footprintTrust.includes("census"));
  assert.ok(footprintTrust.includes("mvp-footprint"));
  console.log("PASS existing API/census/trust wiring still present (backward compatible)");
}

console.log("PASS: BE Airtable integration SoT v1");
