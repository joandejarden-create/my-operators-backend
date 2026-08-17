#!/usr/bin/env node
/**
 * Staged Tripadvisor/Apify room-count enrichment validation — READ ONLY.
 *
 * Uses existing Actor dataset pool (from benchmark / MCP downloads).
 * Runs verification waterfall (official site + room-count research/SerpApi).
 * NEVER writes Airtable / census.
 *
 * Usage:
 *   node scripts/tripadvisor-apify-rooms-enrichment-staged.mjs
 *   node scripts/tripadvisor-apify-rooms-enrichment-staged.mjs --limit=12
 *   node scripts/tripadvisor-apify-rooms-enrichment-staged.mjs --skip-verify
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  enrichHotelTripadvisorRooms,
  summarizeEnrichmentBatch,
  assertNotBannedDestinationQuery,
  buildTripadvisorResolutionPlan,
  PPE_USD,
} from "../lib/hotel-intelligence/tripadvisor-rooms/index.js";
import { createHotelbedsProvider } from "../lib/hotel-intelligence/providers/hotelbeds.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BENCH = path.join(
  ROOT,
  "data/hotel-intelligence/tripadvisor-apify-benchmark-v1"
);
const OUT_DATA = path.join(
  ROOT,
  "data/hotel-intelligence/tripadvisor-apify-rooms-enrichment-v1"
);
const OUT_REPORT = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-apify-rooms-enrichment-v1"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

function parseArgs(argv) {
  const out = { limit: 12, skipVerify: false, known: 6, missing: 6 };
  for (const a of argv.slice(2)) {
    if (a === "--skip-verify") out.skipVerify = true;
    if (a.startsWith("--limit=")) out.limit = Number(a.slice(8)) || 12;
    if (a.startsWith("--known=")) out.known = Number(a.slice(8)) || 6;
    if (a.startsWith("--missing=")) out.missing = Number(a.slice(10)) || 6;
  }
  return out;
}

function loadPool() {
  const poolPath = path.join(BENCH, "ta-pool.json");
  if (!fs.existsSync(poolPath)) {
    throw new Error(`Missing TA pool at ${poolPath} — run Apify benchmark fetch first`);
  }
  const raw = JSON.parse(fs.readFileSync(poolPath, "utf8"));
  return Array.isArray(raw.items) ? raw.items : [];
}

async function main() {
  const args = parseArgs(process.argv);
  fs.mkdirSync(OUT_DATA, { recursive: true });
  fs.mkdirSync(OUT_REPORT, { recursive: true });

  // Safety: ban destination free-text
  const ban = assertNotBannedDestinationQuery("hotels in Bogotá");
  if (ban.ok) throw new Error("banned query guard failed");

  const pool = loadPool();
  const samples = JSON.parse(
    fs.readFileSync(path.join(BENCH, "samples.json"), "utf8")
  );

  const { matchTripadvisorHotel } = await import(
    "../lib/hotel-intelligence/tripadvisor-rooms/match.js"
  );

  const known = [];
  for (const h of samples.phase2_known || []) {
    if (known.length >= args.known) break;
    if (matchTripadvisorHotel(h, pool).match) known.push(h);
  }
  for (const h of samples.phase2_known || []) {
    if (known.length >= args.known) break;
    if (!known.find((x) => x.record_id === h.record_id)) known.push(h);
  }
  const missing = [];
  for (const h of samples.phase3_missing || []) {
    if (missing.length >= args.missing) break;
    if (matchTripadvisorHotel(h, pool).match) missing.push(h);
  }
  for (const h of samples.phase3_missing || []) {
    if (missing.length >= args.missing) break;
    if (!missing.find((x) => x.record_id === h.record_id)) missing.push(h);
  }
  const hotels = [...known, ...missing].slice(0, args.limit);

  const hotelbeds = createHotelbedsProvider({ env: process.env });
  const rows = [];
  let i = 0;
  for (const hotel of hotels) {
    i += 1;
    const plan = buildTripadvisorResolutionPlan(hotel);
    console.error(
      `[${i}/${hotels.length}] ${hotel.name} (${hotel.country}) plan=${plan.steps?.[0]?.kind || "none"}`
    );
    // eslint-disable-next-line no-await-in-loop
    const row = await enrichHotelTripadvisorRooms(hotel, pool, {
      allowRoomCountResearch: !args.skipVerify && hotel.rooms == null,
      allowSerpapi: !args.skipVerify,
      maxSearches: 2,
      maxPageFetches: 2,
      hotelbedsProvider: hotelbeds,
      env: process.env,
    });
    rows.push(row);
  }

  // Cost: reuse pool build cost estimate if present; else estimate SILVER × pool touch
  let cost = null;
  const costPath = path.join(BENCH, "cost-estimate.json");
  if (fs.existsSync(costPath)) {
    const c = JSON.parse(fs.readFileSync(costPath, "utf8"));
    // Attribute a share of PPE for staged sample (~1.5 results/hotel × SILVER)
    const attributed = Math.round(hotels.length * 1.5 * PPE_USD.SILVER * 10000) / 10000;
    cost = {
      model: "attributed_ppe_silver_for_staged_sample",
      total_usd: attributed,
      note: "Apify MCP pool reused from benchmark; attributed PPE for staged hotels only",
      benchmark_pool_total_usd: c.total_usd ?? null,
      price_per_result_usd: PPE_USD.SILVER,
    };
  } else {
    cost = {
      model: "attributed_ppe_silver",
      total_usd: Math.round(hotels.length * 1.5 * PPE_USD.SILVER * 10000) / 10000,
      price_per_result_usd: PPE_USD.SILVER,
    };
  }

  const summary = summarizeEnrichmentBatch(rows, cost);

  fs.writeFileSync(
    path.join(OUT_DATA, "staged-results.json"),
    JSON.stringify(summary, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT_REPORT, "staged-results.json"),
    JSON.stringify(summary, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT_REPORT, "summary.json"),
    JSON.stringify(
      {
        production_writes: false,
        counts: summary.counts,
        cost: summary.cost,
        cost_per_verified_room_count: summary.cost_per_verified_room_count,
        hotels_sampled: hotels.map((h) => ({
          record_id: h.record_id,
          name: h.name,
          country: h.country,
          rooms: h.rooms ?? null,
        })),
      },
      null,
      2
    )
  );

  const md = renderReport(summary, hotels);
  fs.writeFileSync(
    path.join(OUT_REPORT, "DEALALITY_TRIPADVISOR_APIFY_ROOMS_ENRICHMENT_STAGED.md"),
    md
  );

  console.log(
    JSON.stringify(
      {
        production_writes: false,
        counts: summary.counts,
        cost: summary.cost,
        cost_per_verified_room_count: summary.cost_per_verified_room_count,
        report: path.relative(ROOT, path.join(OUT_REPORT, "DEALALITY_TRIPADVISOR_APIFY_ROOMS_ENRICHMENT_STAGED.md")),
      },
      null,
      2
    )
  );
}

function renderReport(summary, hotels) {
  const c = summary.counts;
  const lines = [
    "# Tripadvisor / Apify Room Enrichment — Staged Validation",
    "",
    "`TRIPADVISOR_APIFY_ROOMS_ENRICHMENT_STAGED_COMPLETE`",
    "",
    "**Mode:** READ ONLY — no Airtable / census writes",
    `**Module:** ${summary.module_version}`,
    `**Sample size:** ${hotels.length}`,
    "",
    "## Safety",
    "",
    "```text",
    "Airtable writes: 0",
    "Census Rooms / Keys overwrites: 0",
    "Banned destination free-text queries: enforced",
    "Resolution preference: Hotel_Review → Hotels-g → per-hotel Search?q=",
    "```",
    "",
    "## Results",
    "",
    "| Metric | Count |",
    "| --- | ---: |",
    `| New room candidates (missing authoritative) | ${c.new_room_candidates} |`,
    `| Primary-source verified | ${c.primary_source_verified} |`,
    `| Multi-source verified | ${c.multi_source_verified} |`,
    `| Single-source candidates | ${c.single_source_candidates} |`,
    `| Conflicts | ${c.conflicts} |`,
    `| Unresolved | ${c.unresolved} |`,
    `| False-match rejections | ${c.false_match_rejections} |`,
    `| Authoritative EXACT (compare only) | ${c.authoritative_exact} |`,
    `| Authoritative NEAR (compare only) | ${c.authoritative_near} |`,
    `| Authoritative CONFLICT (compare only) | ${c.authoritative_conflict} |`,
    "",
    "## Cost",
    "",
    `- Total Apify cost (attributed staged): **$${summary.cost?.total_usd ?? "n/a"}**`,
    `- Cost per verified room count: **$${summary.cost_per_verified_room_count ?? "n/a"}**`,
    "",
    "## Row highlights",
    "",
  ];

  for (const r of summary.rows || []) {
    lines.push(
      `- **${r.dealality_name}** (${r.dealality_country}): auth=${r.rooms_authoritative ?? "—"} cand=${r.rooms_candidate ?? "—"} status=\`${r.rooms_verification_status}\` compare=\`${r.room_compare_vs_authoritative}\``
    );
  }

  lines.push(
    "",
    "## Data model (local staged)",
    "",
    "`rooms_authoritative`, `rooms_candidate`, `rooms_source`, `rooms_source_url`, `rooms_verified_at`, `rooms_confidence`, `rooms_verification_status` + Tripadvisor side fields.",
    "",
    "## STOP",
    "",
    "Staged validation complete. No production writes.",
    ""
  );
  return lines.join("\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
