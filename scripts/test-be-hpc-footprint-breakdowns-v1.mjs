#!/usr/bin/env node
/**
 * Portfolio Distribution breakdowns + customer-facing copy gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildHpcFootprintSummaryFromRows,
  buildCustomerFacingHpcCoverageCopy,
  buildHpcPortfolioBreakdowns,
} from "../lib/brand-explorer/be-hpc-footprint-summary-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

{
  const rows = [
    { id: "a", currentBrand: "Curio Collection", radarStatus: "Open", roomsKeys: 100, country: "United States", propertyType: "Hotel" },
    { id: "b", currentBrand: "Curio Collection", radarStatus: "Open", roomsKeys: null, country: "Mexico", propertyType: "" },
    { id: "c", currentBrand: "Curio Collection", radarStatus: "Open", roomsKeys: 50, country: "Atlantis-Myth", propertyType: "Resort" }, // unmapped country
  ];
  const summary = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId: "receQkxgjlezsc1xg",
    brandName: "Curio Collection by Hilton",
    brandMatchers: ["Curio Collection"],
    hpcRows: rows,
    dataOrigin: "live_airtable",
    paginationComplete: true,
  });
  assert.equal(summary.metrics.totalOpenHotels, 3);
  const regionHotels = summary.breakdowns.region.reduce((n, r) => n + r.hotels, 0);
  const countryHotels = summary.breakdowns.country.reduce((n, r) => n + r.hotels, 0);
  const typeHotels = summary.breakdowns.propertyType.reduce((n, r) => n + r.hotels, 0);
  assert.equal(regionHotels, 3, "region must reconcile to open hotels");
  assert.equal(countryHotels, 3, "country must reconcile");
  assert.equal(typeHotels, 3, "property type must reconcile");
  assert.ok(summary.breakdowns.region.some((r) => r.label === "Unclassified"));
  assert.ok(summary.breakdowns.propertyType.some((r) => r.label === "Unknown"));
  assert.equal(summary.roomsCoverage.openMissingKeys, 1);
  assert.equal(summary.displayMetrics.totalOpenKeys, 150);
  const copy = buildCustomerFacingHpcCoverageCopy(summary);
  assert.match(copy, /Dealality has identified 3 affiliated properties/);
  assert.doesNotMatch(copy, /LIVE_UNVERIFIED|certState|Radar Status|fixture|candidate/i);
  console.log("PASS reconcile + Unclassified/Unknown + customer copy");
}

{
  const atelier = fs.readFileSync(path.join(ROOT, "public/js/brand-explorer-atelier-from-api.js"), "utf8");
  assert.match(atelier, /data-fp-view/);
  assert.match(atelier, /beAtFpView-/);
  assert.match(atelier, /Known Open Rooms/);
  assert.doesNotMatch(atelier, /Coverage status:\s*'\s*\+\s*String\(disp\.hpcCertState\)/);
  const css = fs.readFileSync(path.join(ROOT, "public/css/brand-explorer-atelier-oe-panels.css"), "utf8");
  assert.match(css, /data-fp-view='region'/);
  assert.doesNotMatch(css, /#beAtFpReg:checked/);
  const census = fs.readFileSync(path.join(ROOT, "public/js/brand-explorer-census-metrics.js"), "utf8");
  assert.match(census, /buildHpcCustomerCoverageNote/);
  assert.doesNotMatch(census, /Radar Status Open\/Pipeline/);
  console.log("PASS unique toggles + Known Rooms + no internal certState in customer path");
}

{
  // Missing rooms must not become verified zero in display metrics when all missing.
  const allMissing = buildHpcPortfolioBreakdowns([
    { id: "x", radarStatus: "Open", roomsKeys: null, country: "Mexico", propertyType: "Hotel" },
    { id: "y", radarStatus: "Open", roomsKeys: null, country: "Mexico", propertyType: "Hotel" },
  ]);
  assert.equal(allMissing.roomsCoverage.allOpenKeysMissing, true);
  assert.equal(allMissing.region[0].displayOpenKeys, null);
  console.log("PASS missing rooms → null display keys");
}

{
  // Saint Lucia / accented Caribbean → CALA; brand-wide figures must not clone across regions.
  const mixed = buildHpcPortfolioBreakdowns([
    { id: "a", radarStatus: "Open", roomsKeys: 100, country: "United States", propertyType: "Hotel" },
    { id: "b", radarStatus: "Open", roomsKeys: 50, country: "Saint Lucia", propertyType: "Resort" },
    { id: "c", radarStatus: "Open", roomsKeys: null, country: "Curaçao", propertyType: "" },
  ]);
  assert.equal(mixed.region.reduce((n, r) => n + r.hotels, 0), 3);
  assert.ok(!mixed.region.some((r) => r.label === "Unclassified"), "Caribbean islands must classify");
  const am = mixed.region.find((r) => r.label === "AM");
  const cala = mixed.region.find((r) => r.label === "CALA");
  assert.equal(am?.hotels, 1);
  assert.equal(am?.keys, 100);
  assert.equal(cala?.hotels, 2);
  assert.equal(cala?.keys, 50, "known rooms only; missing must not inflate");
  assert.ok(mixed.propertyType.some((r) => r.label === "Unknown"));
  console.log("PASS Saint Lucia/Curaçao CALA + no brand-wide clone across regions");
}

{
  const atelier = fs.readFileSync(path.join(ROOT, "public/js/brand-explorer-atelier-from-api.js"), "utf8");
  assert.match(atelier, /knownDenom = Number\(row\.openWithKeys\)/);
  assert.match(atelier, /knownDenom = Number\(o\.openWithKeys\)/);
  assert.match(atelier, /Never fall back to total hotels when room coverage is incomplete/);
  const census = fs.readFileSync(path.join(ROOT, "public/js/brand-explorer-census-metrics.js"), "utf8");
  assert.match(census, /displayOpenKeys:/);
  assert.match(census, /openWithKeys: openWithKeys/);
  console.log("PASS average keys uses openWithKeys denominator; normalize preserves coverage");
}

console.log(
  JSON.stringify({
    ok: true,
    contract: "BE_HPC_FOOTPRINT_BREAKDOWNS_V1",
    airtableWrites: 0,
  }),
);
