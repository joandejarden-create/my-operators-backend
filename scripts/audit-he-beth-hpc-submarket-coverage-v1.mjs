#!/usr/bin/env node
/**
 * Read-only Bethesda / HPC submarket coverage audit for Hotel Explorer.
 * Dry-run only — no Airtable writes.
 *
 * Usage: node scripts/audit-he-beth-hpc-submarket-coverage-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildMarketCoverageReport } from "../lib/scout/market-coverage.js";
import {
  defaultRadiusKmForLocationType,
  defaultRadiusMilesForLocationType,
  MI_TO_KM,
} from "../lib/hotel-intelligence/he-measurement-prefs-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_JSON = path.join(
  ROOT,
  "reports/hotel-explorer/HE-BETH-HPC-SUBMARKET-COVERAGE-V1.json"
);
const OUT_MD = path.join(
  ROOT,
  "reports/hotel-explorer/HE-BETH-HPC-SUBMARKET-COVERAGE-V1.md"
);

const BETHESDA = {
  id: "recLuxvwwxID7U2B8",
  name: "Bethesda Marriott",
  city: "Bethesda",
  country: "United States",
  market: null,
  submarket: "Pooks Hill / Rockville Pike",
  locationType: "Suburban",
};

function haversineKm(a, b) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)));
}

async function runQuery(label, query) {
  const report = await buildMarketCoverageReport(query);
  return {
    label,
    query,
    ok: Boolean(report.ok),
    error: report.error || null,
    metrics: report.metrics || null,
    sampleCount: (report.recordsSample || []).length,
    warnings: report.warnings || [],
    source: report.source || null,
    sample: (report.recordsSample || []).slice(0, 25).map((r) => ({
      id: r.id,
      name: r.name,
      status: r.status,
      rooms: r.rooms,
      city: r.city,
      market: r.market,
      submarket: r.submarket,
      chainScale: r.chainScale,
      latitude: r.latitude,
      longitude: r.longitude,
    })),
  };
}

async function main() {
  const radiusMi = defaultRadiusMilesForLocationType(BETHESDA.locationType);
  const radiusKm = defaultRadiusKmForLocationType(BETHESDA.locationType);

  const queries = [
    await runQuery("submarket_exact", {
      country: BETHESDA.country,
      submarket: BETHESDA.submarket,
      includePipeline: true,
    }),
    await runQuery("city_bethesda", {
      country: BETHESDA.country,
      city: BETHESDA.city,
      includePipeline: true,
    }),
    await runQuery("city_rockville", {
      country: BETHESDA.country,
      city: "Rockville",
      includePipeline: true,
    }),
  ];

  const city = queries.find((q) => q.label === "city_bethesda");
  const missingSubmarket = {
    assignmentMethod: "provisional_corridor_label",
    verificationStatus: "UNVERIFIED_PROVISIONAL",
    note:
      "Do not treat city or nearest-hotel distance alone as an official market definition. Propose stewardship dry-run before Airtable apply.",
    canonicalProposal: {
      marketId: null,
      marketLabel: "Washington DC / Montgomery County (provisional)",
      submarketId: null,
      submarketLabel: BETHESDA.submarket,
      source: "HE founder pilot / property context",
      geographicBoundary: "not verified — no official Dealality polygon yet",
    },
  };

  const payload = {
    version: "he-beth-hpc-submarket-coverage-v1",
    generatedAt: new Date().toISOString(),
    hotel: BETHESDA,
    areaHotelsRadius: {
      locationType: BETHESDA.locationType,
      initialMiles: radiusMi,
      initialKm: Number(radiusKm.toFixed(2)),
      miToKm: MI_TO_KM,
      inclusionIndependentOfDisplayUnits: true,
    },
    queries,
    missingSubmarketAssessment: missingSubmarket,
    airtableDryRun: {
      mode: "NOT_EXECUTED",
      authorization: "REQUIRED_BEFORE_APPLY",
      proposedActions: [
        "Review hotels in city_bethesda / city_rockville samples lacking Submarket",
        "Assign provisional vs verified corridor labels with source + method",
        "Never bulk-write without founder-reviewed dry-run JSON",
      ],
    },
    conclusion: {
      exactSubmarketMatchCount: queries[0]?.metrics?.totalHotels ?? 0,
      cityBethesdaMatchCount: city?.metrics?.totalHotels ?? 0,
      recommendation:
        (queries[0]?.metrics?.totalHotels || 0) > 0
          ? "Use exact submarket census rows for Submarket Snapshot."
          : "Exact submarket label is not populated in Hotel Census. UI should show city/market Scout aggregates with explicit provisional coverage language — do not invent market totals.",
    },
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(payload, null, 2));

  const md = [
    "# Bethesda HPC Submarket Coverage (read-only)",
    "",
    `- Generated: ${payload.generatedAt}`,
    `- Hotel: ${BETHESDA.name} (${BETHESDA.id})`,
    `- Pilot submarket label: \`${BETHESDA.submarket}\``,
    `- Area Hotels suburban default: ${radiusMi} mi / ${radiusKm.toFixed(2)} km`,
    "",
    "## Query results",
    "",
    ...queries.map(
      (q) =>
        `- **${q.label}**: ok=${q.ok}, totalHotels=${q.metrics?.totalHotels ?? "n/a"}, open=${q.metrics?.openHotels ?? "n/a"}, pipeline=${q.metrics?.pipelineHotels ?? "n/a"}, sample=${q.sampleCount}`
    ),
    "",
    "## Missing assignment assessment",
    "",
    `- Status: \`${missingSubmarket.verificationStatus}\``,
    `- Method: ${missingSubmarket.assignmentMethod}`,
    `- ${missingSubmarket.note}`,
    "",
    "## Airtable",
    "",
    "No writes executed. Any bulk submarket classification requires a reviewed dry-run and explicit authorization.",
    "",
    `JSON: \`${path.relative(ROOT, OUT_JSON)}\``,
  ].join("\n");
  fs.writeFileSync(OUT_MD, md);
  console.log("Wrote", path.relative(ROOT, OUT_JSON));
  console.log("Wrote", path.relative(ROOT, OUT_MD));
  console.log(JSON.stringify(payload.conclusion, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
