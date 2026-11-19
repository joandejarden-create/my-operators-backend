#!/usr/bin/env node
/**
 * Read-only Curio / Ascend HPC footprint reconciliation.
 * No Airtable writes. No paid research. No publication.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BE_PHASE2_PILOT_BRANDS,
  buildBrandHpcFootprintSummary,
  fetchHpcRowsForBrandMatchers,
  hpcFootprintLiveConfigured,
} from "../lib/brand-explorer/be-hpc-footprint-summary-v1.js";
import {
  filterHpcRowsForBrandFootprint,
  aggregateOpenPipelineCounts,
} from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(ROOT, "reports/brand-explorer/hpc-footprint-integration-v1");
fs.mkdirSync(OUT_DIR, { recursive: true });

const cohort = BE_PHASE2_PILOT_BRANDS.filter((b) =>
  ["curio-collection", "ascend"].includes(b.slug),
);

const report = {
  contract: "BE_HPC_FOOTPRINT_RECONCILE_V1",
  generatedAt: new Date().toISOString(),
  liveConfigured: hpcFootprintLiveConfigured(),
  airtableWrites: 0,
  paidResearch: false,
  publication: false,
  brands: {},
};

for (const b of cohort) {
  const fetchResult = await fetchHpcRowsForBrandMatchers([...b.hpcMatchers], {
    maxPages: 20,
    pageSize: 100,
  });
  const { matched, excluded } = filterHpcRowsForBrandFootprint(fetchResult.rows || [], {
    brandMatchers: [...b.hpcMatchers],
  });
  const metrics = aggregateOpenPipelineCounts(matched);
  const summary = await buildBrandHpcFootprintSummary({
    brandBasicsRecordId: b.recordId,
    brandName: b.name,
    brandMatchers: [...b.hpcMatchers],
    allowLive: true,
  });
  const missingRooms = matched.filter((r) => r.roomsKeys == null || r.roomsKeys === "");
  report.brands[b.slug] = {
    brandBasicsRecordId: b.recordId,
    brandName: b.name,
    matchers: [...b.hpcMatchers],
    fetchOk: fetchResult.ok,
    fetchError: fetchResult.error || null,
    rawRows: (fetchResult.rows || []).length,
    pages: fetchResult.pages,
    truncated: fetchResult.truncated,
    qualifyingMatched: matched.length,
    excluded,
    metrics: {
      totalOpenHotels: metrics.totalOpenHotels,
      totalOpenKeys: metrics.totalOpenKeys,
      totalPipelineHotels: metrics.totalPipelineHotels,
      totalPipelineKeys: metrics.totalPipelineKeys,
    },
    displayMetrics: summary.displayMetrics,
    roomsMissingCount: missingRooms.length,
    breakdowns: summary.breakdowns,
    roomsCoverage: summary.roomsCoverage,
    certState: summary.certState,
    dataOrigin: summary.dataOrigin,
    coverageLimitations: summary.coverageLimitations,
    warnings: summary.warnings,
    sample: matched.slice(0, 8).map((r) => ({
      id: r.id,
      name: r.propertyName,
      brand: r.currentBrand,
      radarStatus: r.radarStatus,
      roomsKeys: r.roomsKeys,
      country: r.country,
      propertyType: r.propertyType,
    })),
    lastSuccessfulSourceRefresh: summary.source?.aggregatedAt || null,
  };
}

const jsonPath = path.join(OUT_DIR, "curio-ascend-hpc-reconciliation.json");
fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok: true, out: jsonPath, brands: Object.keys(report.brands) }, null, 2));
