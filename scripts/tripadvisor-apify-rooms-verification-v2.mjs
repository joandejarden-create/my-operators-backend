#!/usr/bin/env node
/**
 * Tripadvisor room verification v2 — 50-hotel staged validation (READ ONLY).
 *
 * Candidate generator: existing Apify Tripadvisor pool (MCP benchmark datasets).
 * Verification: official-site crawl + Hotelbeds (if code) + room-count research.
 *
 * NEVER writes Airtable / census Rooms / Keys.
 *
 * Usage:
 *   node scripts/tripadvisor-apify-rooms-verification-v2.mjs
 *   node scripts/tripadvisor-apify-rooms-verification-v2.mjs --limit=50
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  enrichHotelTripadvisorRooms,
  summarizeEnrichmentBatch,
  assertNotBannedDestinationQuery,
  matchTripadvisorHotel,
  PPE_USD,
  SERPAPI_USD_PER_SEARCH,
  ROOMS_VERIFICATION_STATUS,
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
  "data/hotel-intelligence/tripadvisor-apify-rooms-enrichment-v2"
);
const OUT_REPORT = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-apify-rooms-enrichment-v2"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

function parseArgs(argv) {
  const out = { limit: 50, skipVerify: false };
  for (const a of argv.slice(2)) {
    if (a === "--skip-verify") out.skipVerify = true;
    if (a.startsWith("--limit=")) out.limit = Number(a.slice(8)) || 50;
  }
  return out;
}

function loadPool() {
  const poolPath = path.join(BENCH, "ta-pool.json");
  if (!fs.existsSync(poolPath)) {
    throw new Error(`Missing TA pool: ${poolPath}`);
  }
  const raw = JSON.parse(fs.readFileSync(poolPath, "utf8"));
  return Array.isArray(raw.items) ? raw.items : [];
}

/**
 * Diverse 50 missing-room hotels, preferring pool matches.
 */
function selectFiftyMissing(samples, pool, limit) {
  const missing = samples.phase3_missing || [];
  const byCountry = new Map();
  for (const h of missing) {
    const c = h.country || "Unknown";
    if (!byCountry.has(c)) byCountry.set(c, []);
    byCountry.get(c).push(h);
  }

  const matched = [];
  const unmatched = [];
  for (const h of missing) {
    const { match } = matchTripadvisorHotel(h, pool);
    if (match && match.item?.numberOfRooms != null) matched.push(h);
    else unmatched.push(h);
  }

  const picked = [];
  const used = new Set();
  const countries = [...byCountry.keys()].sort();

  // Round-robin countries from matched first
  let guard = 0;
  while (picked.length < limit && guard < limit * 20) {
    guard += 1;
    let added = false;
    for (const c of countries) {
      if (picked.length >= limit) break;
      const poolC = matched.filter((h) => h.country === c && !used.has(h.record_id));
      if (!poolC.length) continue;
      const h = poolC[0];
      used.add(h.record_id);
      picked.push(h);
      added = true;
    }
    if (!added) break;
  }

  for (const h of matched) {
    if (picked.length >= limit) break;
    if (used.has(h.record_id)) continue;
    used.add(h.record_id);
    picked.push(h);
  }
  for (const h of unmatched) {
    if (picked.length >= limit) break;
    if (used.has(h.record_id)) continue;
    used.add(h.record_id);
    picked.push(h);
  }

  return picked.slice(0, limit);
}

async function main() {
  const args = parseArgs(process.argv);
  fs.mkdirSync(OUT_DATA, { recursive: true });
  fs.mkdirSync(OUT_REPORT, { recursive: true });

  const ban = assertNotBannedDestinationQuery("hotels in Bogotá");
  if (ban.ok) throw new Error("banned destination query guard failed");

  const pool = loadPool();
  const samples = JSON.parse(
    fs.readFileSync(path.join(BENCH, "samples.json"), "utf8")
  );
  const hotels = selectFiftyMissing(samples, pool, args.limit);

  const hotelbeds = createHotelbedsProvider({ env: process.env });
  const rows = [];
  let serpSearches = 0;
  let pagesFetched = 0;

  for (let i = 0; i < hotels.length; i += 1) {
    const hotel = hotels[i];
    console.error(
      `[${i + 1}/${hotels.length}] ${hotel.name} (${hotel.country}) website=${hotel.website ? "yes" : "no"}`
    );
    // eslint-disable-next-line no-await-in-loop
    const row = await enrichHotelTripadvisorRooms(hotel, pool, {
      allowRoomCountResearch: !args.skipVerify,
      allowSerpapi: !args.skipVerify,
      maxSearches: 2,
      maxPageFetches: 3,
      officialMaxPages: 6,
      hotelbedsProvider: hotelbeds,
      env: process.env,
    });
    serpSearches += Number(row.verification?.cost_signals?.serpapi_searches || 0);
    pagesFetched += Number(row.verification?.cost_signals?.pages_fetched || 0);
    rows.push(row);

    // checkpoint every 10
    if ((i + 1) % 10 === 0) {
      fs.writeFileSync(
        path.join(OUT_DATA, "checkpoint-rows.json"),
        JSON.stringify({ at: i + 1, rows }, null, 2)
      );
    }
  }

  const taPpe =
    Math.round(hotels.length * 1.5 * PPE_USD.SILVER * 10000) / 10000;
  const serpCost = Math.round(serpSearches * SERPAPI_USD_PER_SEARCH * 10000) / 10000;
  const cost = {
    model: "attributed_ta_ppe_silver_plus_serpapi_estimate",
    tripadvisor_usd: taPpe,
    serpapi_usd_estimate: serpCost,
    other_incremental_usd: serpCost,
    total_usd: Math.round((taPpe + serpCost) * 10000) / 10000,
    serpapi_searches: serpSearches,
    pages_fetched: pagesFetched,
    notes: [
      "Tripadvisor PPE attributed from reused MCP pool (SILVER $0.0025 × ~1.5 results/hotel)",
      "SerpApi cost is order-of-magnitude estimate only",
      "Official-site fetches are bandwidth-only (no incremental license fee)",
    ],
  };

  const summary = summarizeEnrichmentBatch(rows, cost);

  fs.writeFileSync(
    path.join(OUT_DATA, "verification-v2-results.json"),
    JSON.stringify(summary, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT_REPORT, "verification-v2-results.json"),
    JSON.stringify(summary, null, 2)
  );

  const metrics = buildMetrics(summary, hotels, cost);
  fs.writeFileSync(
    path.join(OUT_REPORT, "metrics.json"),
    JSON.stringify(metrics, null, 2)
  );

  const report = renderReport(summary, hotels, metrics);
  fs.writeFileSync(
    path.join(OUT_REPORT, "DEALALITY_TRIPADVISOR_ROOM_VERIFICATION_V2.md"),
    report
  );

  console.log(
    JSON.stringify(
      {
        status: "TRIPADVISOR_ROOM_VERIFICATION_V2_COMPLETE",
        ...metrics.status_block,
        report: path.relative(
          ROOT,
          path.join(OUT_REPORT, "DEALALITY_TRIPADVISOR_ROOM_VERIFICATION_V2.md")
        ),
      },
      null,
      2
    )
  );
}

function buildMetrics(summary, hotels, cost) {
  const c = summary.counts;
  const r = summary.rates;
  const verified = c.primary_source_verified + c.multi_source_verified;
  return {
    TOTAL_SAMPLE: hotels.length,
    TRIPADVISOR_MATCHES: c.tripadvisor_matches,
    TRIPADVISOR_ROOM_CANDIDATES: c.tripadvisor_room_candidates,
    VERIFIED_PRIMARY_SOURCE: c.primary_source_verified,
    VERIFIED_MULTI_SOURCE: c.multi_source_verified,
    CANDIDATE_SINGLE_SOURCE: c.single_source_candidates,
    CONFLICT_REVIEW_REQUIRED: c.conflicts,
    SOURCE_INDEPENDENCE_UNCERTAIN: c.source_independence_uncertain,
    UNRESOLVED: c.unresolved,
    FALSE_MATCH_REJECTED: c.false_match_rejections,
    OFFICIAL_WEBSITE_ROOM_COUNT_FOUND: c.official_website_room_count_found,
    OFFICIAL_PDF_FACTSHEET_FOUND: c.official_pdf_factsheet_found,
    SECONDARY_SOURCE_VERIFICATIONS: c.secondary_source_verifications,
    TRIPADVISOR_COST: cost.tripadvisor_usd,
    OTHER_INCREMENTAL_COST: cost.other_incremental_usd,
    TOTAL_COST: cost.total_usd,
    COST_PER_ROOM_CANDIDATE: summary.cost_per_room_candidate,
    COST_PER_VERIFIED_ROOM: summary.cost_per_verified_room_count,
    CANDIDATE_TO_VERIFIED_CONVERSION_RATE: r.candidate_to_verified_conversion,
    ROOM_RESOLUTION_RATE: r.room_resolution_rate,
    TRIPADVISOR_MATCH_RATE: r.tripadvisor_match_rate,
    ROOM_CANDIDATE_RATE: r.room_candidate_rate,
    status_block: {
      SAMPLE: hotels.length,
      TRIPADVISOR_MATCH_RATE: `${r.tripadvisor_match_rate}%`,
      ROOM_CANDIDATE_RATE: `${r.room_candidate_rate}%`,
      PRIMARY_VERIFIED: c.primary_source_verified,
      MULTI_SOURCE_VERIFIED: c.multi_source_verified,
      SINGLE_SOURCE_ONLY: c.single_source_candidates,
      CONFLICTS: c.conflicts,
      SOURCE_INDEPENDENCE_UNCERTAIN: c.source_independence_uncertain,
      UNRESOLVED: c.unresolved,
      CANDIDATE_TO_VERIFIED_CONVERSION: `${r.candidate_to_verified_conversion ?? "n/a"}%`,
      TOTAL_ROOM_RESOLUTION_RATE: `${r.room_resolution_rate}%`,
      TOTAL_COST: `$${cost.total_usd}`,
      COST_PER_VERIFIED_ROOM:
        summary.cost_per_verified_room_count != null
          ? `$${summary.cost_per_verified_room_count}`
          : "n/a",
      PRODUCTION_WRITES: 0,
      VERIFIED_TOTAL: verified,
    },
  };
}

function renderReport(summary, hotels, metrics) {
  const c = summary.counts;
  const verifiedRows = (summary.rows || []).filter(
    (r) =>
      r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.VERIFIED_PRIMARY_SOURCE ||
      r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.VERIFIED_MULTI_SOURCE
  );
  const conflictRows = (summary.rows || []).filter(
    (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.CONFLICT_REVIEW_REQUIRED
  );

  const giata =
    metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE != null &&
    metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE >= 40
      ? "DEFER_GIATA_MHG — verification conversion strong enough to continue low-cost waterfall; revisit if unresolved tail stays large after scale-up"
      : metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE != null &&
          metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE >= 20
        ? "CONDITIONAL_DEFER — promising but conversion still modest; expand official-site + PDF coverage before GIATA commit"
        : "KEEP_GIATA_OPTION_OPEN — conversion too low on this sample to defer MHG solely on Tripadvisor+waterfall";

  const lines = [
    "# Tripadvisor Room Verification V2",
    "",
    "`TRIPADVISOR_ROOM_VERIFICATION_V2_COMPLETE`",
    "",
    "**Mode:** READ ONLY / staged — no Airtable or census writes",
    `**Module:** ${summary.module_version}`,
    `**Verify:** ${summary.verify_version}`,
    `**Sample:** ${hotels.length} missing-room CALA hotels`,
    "",
    "## 1. Executive summary",
    "",
    `Tripadvisor remains the **candidate generator**. Verification v2 adds official-site path crawl, composition totals, source-independence gating, and conflict classification.`,
    "",
    `- Candidate→verified conversion: **${metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE ?? "n/a"}%**`,
    `- Room resolution rate (sample): **${metrics.ROOM_RESOLUTION_RATE}%**`,
    `- Primary verified: **${c.primary_source_verified}** · Multi: **${c.multi_source_verified}** · Single-source: **${c.single_source_candidates}**`,
    `- Conflicts: **${c.conflicts}** · Independence uncertain: **${c.source_independence_uncertain}**`,
    `- Total cost (attributed): **$${metrics.TOTAL_COST}**`,
    "",
    "## 2. Architecture changes",
    "",
    "- `official-site-verify.js` — capped path crawl + PDF links + composition arithmetic",
    "- `independence.js` — cluster-based independence (blocks OTA↔OTA / TA↔HBX auto-multi)",
    "- `conflicts.js` — conflict cause taxonomy (no averaging)",
    "- `verify.js` → **v2** waterfall",
    "",
    "## 3. Verification waterfall",
    "",
    "1. Official hotel/brand website paths (`/rooms`, `/about`, `/fact-sheet`, ES paths, …)",
    "2. Hotelbeds Content API when `HBX Hotel Code` present",
    "3. Existing room-count research (SerpApi + ≤4 page fetches)",
    "4. Extra approved observations hook",
    "",
    "Skipped as room-count sources: StayingAPI, SerpApi Google Hotels field, GIATA Drive (firewalled).",
    "",
    "## 4. Source hierarchy",
    "",
    "| Tier | Sources |",
    "| --- | --- |",
    "| 1 | Official hotel / brand / owner pages & fact sheets |",
    "| 2 | Tourism/gov docs, Hotelbeds structured roomsNumber |",
    "| 3 | Credible press / industry pubs via research |",
    "| 4 | OTAs / aggregators (never sufficient alone for MULTI) |",
    "",
    "## 5. Source-independence logic",
    "",
    "- Tripadvisor is never an independent confirmer of itself",
    "- OTA-only agreement → `SOURCE_INDEPENDENCE_UNCERTAIN`",
    "- Tripadvisor + Hotelbeds alone → uncertain (possible shared upstream)",
    "- Official explicit match → `VERIFIED_PRIMARY_SOURCE`",
    "- Two distinct non-OTA clusters → `VERIFIED_MULTI_SOURCE`",
    "",
    "## 6. 50-hotel results",
    "",
    "| Metric | Value |",
    "| --- | ---: |",
    `| TOTAL_SAMPLE | ${metrics.TOTAL_SAMPLE} |`,
    `| TRIPADVISOR_MATCHES | ${metrics.TRIPADVISOR_MATCHES} |`,
    `| TRIPADVISOR_ROOM_CANDIDATES | ${metrics.TRIPADVISOR_ROOM_CANDIDATES} |`,
    `| VERIFIED_PRIMARY_SOURCE | ${metrics.VERIFIED_PRIMARY_SOURCE} |`,
    `| VERIFIED_MULTI_SOURCE | ${metrics.VERIFIED_MULTI_SOURCE} |`,
    `| CANDIDATE_SINGLE_SOURCE | ${metrics.CANDIDATE_SINGLE_SOURCE} |`,
    `| CONFLICT_REVIEW_REQUIRED | ${metrics.CONFLICT_REVIEW_REQUIRED} |`,
    `| SOURCE_INDEPENDENCE_UNCERTAIN | ${metrics.SOURCE_INDEPENDENCE_UNCERTAIN} |`,
    `| UNRESOLVED | ${metrics.UNRESOLVED} |`,
    `| FALSE_MATCH_REJECTED | ${metrics.FALSE_MATCH_REJECTED} |`,
    `| OFFICIAL_WEBSITE_ROOM_COUNT_FOUND | ${metrics.OFFICIAL_WEBSITE_ROOM_COUNT_FOUND} |`,
    `| OFFICIAL_PDF_FACTSHEET_FOUND | ${metrics.OFFICIAL_PDF_FACTSHEET_FOUND} |`,
    `| SECONDARY_SOURCE_VERIFICATIONS | ${metrics.SECONDARY_SOURCE_VERIFICATIONS} |`,
    `| CANDIDATE_TO_VERIFIED_CONVERSION | ${metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE ?? "n/a"}% |`,
    `| ROOM_RESOLUTION_RATE | ${metrics.ROOM_RESOLUTION_RATE}% |`,
    "",
    "## 7. Hotel-level evidence (verified)",
    "",
  ];

  if (!verifiedRows.length) {
    lines.push("_No hotels reached VERIFIED_PRIMARY_SOURCE or VERIFIED_MULTI_SOURCE in this run._", "");
  }
  for (const r of verifiedRows) {
    const ev = (r.verification?.evidence_audit || [])[0] || {};
    lines.push(
      `### ${r.dealality_name}`,
      "",
      `| Field | Value |`,
      `| --- | --- |`,
      `| Room count | ${r.preferred_verified_rooms ?? r.rooms_candidate} |`,
      `| Status | ${r.rooms_verification_status} |`,
      `| Source | ${ev.provider || r.rooms_source} / ${ev.source_category || ""} |`,
      `| Source URL | ${ev.url || r.rooms_source_url || ""} |`,
      `| Evidence | ${(ev.quote || "").replace(/\|/g, "/").slice(0, 180)} |`,
      `| Retrieved | ${r.rooms_verified_at || ev.retrieved || ""} |`,
      ""
    );
  }

  lines.push("## 8. Conflict analysis", "");
  if (!conflictRows.length) lines.push("_No CONFLICT_REVIEW_REQUIRED rows._", "");
  for (const r of conflictRows.slice(0, 15)) {
    const ca = r.verification?.conflict_analysis;
    lines.push(
      `- **${r.dealality_name}**: TA=${r.rooms_candidate} preferred=${r.preferred_verified_rooms ?? "—"} cause=\`${ca?.cause || "n/a"}\` — ${ca?.note || ""}`
    );
  }

  lines.push(
    "",
    "## 9. Cost analysis",
    "",
    `| Item | USD |`,
    `| --- | ---: |`,
    `| Tripadvisor (attributed PPE) | ${metrics.TRIPADVISOR_COST} |`,
    `| Other incremental (SerpApi est.) | ${metrics.OTHER_INCREMENTAL_COST} |`,
    `| Total | ${metrics.TOTAL_COST} |`,
    `| Per room candidate | ${metrics.COST_PER_ROOM_CANDIDATE ?? "n/a"} |`,
    `| Per verified room | ${metrics.COST_PER_VERIFIED_ROOM ?? "n/a"} |`,
    "",
    "## 10. Candidate-to-verified conversion",
    "",
    `\`(PRIMARY + MULTI) / TRIPADVISOR_ROOM_CANDIDATES = ${metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE ?? "n/a"}%\``,
    "",
    "## 11. GIATA defer/buy assessment",
    "",
    `**${giata}**`,
    "",
    "Compare vs GIATA MHG proposal (€200/mo + €150 setup, 24-mo → €4,950): decision is driven by conversion, provenance, and unresolved tail — not price alone.",
    "",
    "| Dimension | Assessment |",
    "| --- | --- |",
    "| Coverage | Limited to Tripadvisor match + verifiable official/web evidence |",
    "| Accuracy | Primary-source path is strong when official pages state inventory explicitly |",
    "| Provenance | Auditable URL + quote retained for verified rows |",
    "| Freshness | Live crawl; SPA/blocked sites remain a failure mode |",
    "| Scalability | Cheap per hotel; SerpApi + fetch latency dominate runtime |",
    "| Complexity | Higher than a licensed feed; needs match + independence gates |",
    "| Stability | Official sites change; Actor PPE is stable |",
    "",
    "## 12. Recommended next step",
    "",
    metrics.CANDIDATE_TO_VERIFIED_CONVERSION_RATE >= 40
      ? "Scale staged verification to 200 missing-room hotels; still no production writes until human QA of verified set."
      : "Improve official-site hit rate (brand deep links, more fact-sheet discovery) before any GIATA commit or production promotion.",
    "",
    "## Phase 1 inspection notes (why v1 stayed single-source)",
    "",
    "- v1 fetched only homepage URLs (no `/rooms`, `/fact-sheet`, ES paths)",
    "- No composition arithmetic for rooms+suites",
    "- No independence gating (would have over-called MULTI on weak pairs)",
    "- Sample hotels often lacked HBX codes; StayingAPI/GIATA Drive do not expose room counts",
    "- SerpApi snippets alone rarely yield explicit inventory phrases",
    "",
    "## Safety",
    "",
    "```text",
    "PRODUCTION_WRITES: 0",
    "Census Rooms / Keys overwrites: 0",
    "Tripadvisor treated as candidate only",
    "```",
    "",
    "## STOP",
    "",
    "50-hotel staged validation complete. No production enrichment enabled.",
    ""
  );

  return lines.join("\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
