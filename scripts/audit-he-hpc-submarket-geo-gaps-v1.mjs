#!/usr/bin/env node
/**
 * Bounded read-only audit: HPC hotels missing Market / Submarket / coords / status / rooms.
 * Dry-run only — never applies Airtable updates.
 *
 * Usage:
 *   node scripts/audit-he-hpc-submarket-geo-gaps-v1.mjs
 *   node scripts/audit-he-hpc-submarket-geo-gaps-v1.mjs --hotelId=recLuxvwwxID7U2B8
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildHpcSubmarketSnapshot,
  HE_HPC_SUBMARKET_SNAPSHOT_V1,
} from "../lib/hotel-intelligence/he-hpc-submarket-snapshot-v1.js";
import {
  fetchHpcRadarStatusEligibleRecords,
  formatHotelPropertyCensusAsBrandPresenceHotel,
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import Airtable from "airtable";
import { HPC_TABLE_ID, GRAPH_BASE_ID_CANONICAL } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports/hotel-explorer/phase4-quality");
const OUT_JSON = path.join(OUT_DIR, "HE-HPC-SUBMARKET-GEO-GAPS-DRY-RUN-V1.json");
const OUT_MD = path.join(OUT_DIR, "HE-HPC-SUBMARKET-GEO-GAPS-DRY-RUN-V1.md");

function argValue(name) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : null;
}

function createBase() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId =
    process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID || GRAPH_BASE_ID_CANONICAL;
  if (!key || !baseId) return null;
  return { base: new Airtable({ apiKey: key }).base(baseId), baseId };
}

function classifyGaps(hotel) {
  const gaps = [];
  if (!hotel.market) gaps.push("market");
  if (!hotel.submarket) gaps.push("submarket");
  const lat = Number(hotel.lat);
  const lng = Number(hotel.lng);
  if (!(Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0))) {
    gaps.push("coordinates");
  }
  if (!hotel.status && !hotel.radarStatus) gaps.push("radar_status");
  if (!(Number.isFinite(Number(hotel.rooms)) && Number(hotel.rooms) > 0)) gaps.push("rooms");
  return gaps;
}

async function main() {
  const hotelId = argValue("hotelId") || "recLuxvwwxID7U2B8";
  const ctx = createBase();
  if (!ctx) {
    console.error("Airtable not configured");
    process.exit(1);
  }

  const snapshot = await buildHpcSubmarketSnapshot({
    hotelId,
    airtableBase: ctx.base,
  });

  const fetched = await fetchHpcRadarStatusEligibleRecords({
    airtableBase: ctx.base,
    pageSize: 100,
  });
  const hotels = (fetched.records || []).map(formatHotelPropertyCensusAsBrandPresenceHotel);
  const gapRows = hotels.map((h) => {
    const gaps = classifyGaps(h);
    return {
      id: h.id,
      name: h.name,
      city: h.city,
      market: h.market,
      submarket: h.submarket,
      status: h.status,
      rooms: h.rooms,
      gaps,
    };
  });
  const withGaps = gapRows.filter((r) => r.gaps.length);
  const counts = {
    totalOpenPipeline: gapRows.length,
    missingMarket: gapRows.filter((r) => r.gaps.includes("market")).length,
    missingSubmarket: gapRows.filter((r) => r.gaps.includes("submarket")).length,
    missingCoordinates: gapRows.filter((r) => r.gaps.includes("coordinates")).length,
    missingStatus: gapRows.filter((r) => r.gaps.includes("radar_status")).length,
    missingRooms: gapRows.filter((r) => r.gaps.includes("rooms")).length,
    anyGap: withGaps.length,
  };

  const proposedUpserts = withGaps.slice(0, 50).map((r) => ({
    recordId: r.id,
    action: "RESEARCH_THEN_UPDATE",
    fieldsProposed: r.gaps,
    applyAuthorized: false,
    note: "Prepare verified Market/Submarket/coords/rooms before any Airtable apply",
  }));

  const report = {
    version: "he-hpc-submarket-geo-gaps-dry-run-v1",
    snapshotVersion: HE_HPC_SUBMARKET_SNAPSHOT_V1,
    generatedAt: new Date().toISOString(),
    applyAuthorized: false,
    writeOnRead: false,
    productionWrites: false,
    baseId: ctx.baseId,
    tableId: HPC_TABLE_ID,
    tableName: HOTEL_PROPERTY_CENSUS_TABLE,
    focusHotelId: hotelId,
    focusSnapshot: {
      ok: snapshot.ok,
      selectedHotelIncluded: snapshot.selectedHotelIncluded,
      grouping: snapshot.grouping,
      hotelCount: snapshot.hotels?.length || 0,
      coverage: snapshot.coverage,
      metrics: snapshot.metrics,
    },
    gapCounts: counts,
    sampleGaps: withGaps.slice(0, 25),
    proposedUpsertsDryRun: proposedUpserts,
    reconciliationRules: [
      "Canonical identity = HPC record id (tbl9aY5ijiuIzzWam)",
      "Hotel Census / Scout may enrich geography but never replace HPC identity",
      "Selected HPC hotel always included in Submarket Snapshot even when Scout is empty",
      "Do not invent Market/Submarket — research then dry-run apply only",
    ],
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  const md = [
    "# HE HPC Submarket geo gaps (dry-run)",
    "",
    `- Generated: ${report.generatedAt}`,
    `- Apply authorized: **false**`,
    `- Focus hotel: \`${hotelId}\``,
    `- Selected included: **${report.focusSnapshot.selectedHotelIncluded}**`,
    `- Grouping: \`${report.focusSnapshot.grouping?.mode}\` / ${report.focusSnapshot.grouping?.label || "—"}`,
    `- Matched HPC hotels: **${report.focusSnapshot.hotelCount}**`,
    "",
    "## Open|Pipeline gap counts",
    "",
    `- Total Open|Pipeline retrieved: ${counts.totalOpenPipeline}`,
    `- Missing Market: ${counts.missingMarket}`,
    `- Missing Submarket: ${counts.missingSubmarket}`,
    `- Missing coordinates: ${counts.missingCoordinates}`,
    `- Missing rooms: ${counts.missingRooms}`,
    `- Any gap: ${counts.anyGap}`,
    "",
    "## Next research process",
    "",
    "1. Resolve Market / Submarket from verified Dealality geography (not invented).",
    "2. Confirm coordinates under storage-terms gates.",
    "3. Stage dry-run HPC field updates.",
    "4. Founder-authorize Airtable apply separately.",
    "",
    `Artifact: \`${path.relative(ROOT, OUT_JSON).replace(/\\\\/g, "/")}\``,
  ].join("\n");
  fs.writeFileSync(OUT_MD, md);
  console.log(JSON.stringify({ ok: true, outJson: OUT_JSON, focus: report.focusSnapshot, counts }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
