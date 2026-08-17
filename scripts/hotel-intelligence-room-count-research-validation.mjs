#!/usr/bin/env node
/**
 * Controlled Room Count Research validation on frozen CALA sample.
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0 — research only.
 *
 * Sample: hotel-intelligence-cala-validation-v1
 * Target: 72 HBX-linked missing rooms + expand to 100 missing-room hotels.
 *
 * Usage:
 *   node scripts/hotel-intelligence-room-count-research-validation.mjs
 *   node scripts/hotel-intelligence-room-count-research-validation.mjs --limit 25
 *   node scripts/hotel-intelligence-room-count-research-validation.mjs --no-serpapi
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { createLocalStore } from "../lib/hotel-intelligence/local-store.js";
import { createExternalIdRegistry } from "../lib/hotel-intelligence/external-ids.js";
import { createEvidenceStore } from "../lib/hotel-intelligence/evidence-store.js";
import { researchHotelRoomCount } from "../lib/hotel-intelligence/room-count-research/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const FROZEN = path.join(
  ROOT,
  "reports/hotel-intelligence/cala-validation-v1/01-sample-definition.json"
);
const HBX_ELIG = path.join(
  ROOT,
  "reports/hotel-intelligence/hotelbeds-live-rooms-validation-v1/01-eligibility.json"
);
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/room-count-research-validation-v1"
);
const DATA_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/room-count-research-validation-v1"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

const args = process.argv.slice(2);
function argNum(name, fallback) {
  const i = args.indexOf(name);
  if (i < 0) return fallback;
  const n = Number(args[i + 1]);
  return Number.isFinite(n) ? n : fallback;
}
const LIMIT = argNum("--limit", 100);
const NO_SERPAPI = args.includes("--no-serpapi");
const MAX_SEARCHES = argNum("--max-searches", 2);
const MAX_FETCHES = argNum("--max-fetches", 2);
const SLEEP_MS = argNum("--sleep-ms", 400);

function blank(v) {
  return v == null || !String(v).trim();
}
function hasRooms(f) {
  const n = Number(f[MAP_CENSUS_FIELDS.roomCount]);
  return Number.isFinite(n) && n > 0;
}
function writeJson(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function pct(n, d) {
  if (!d) return 0;
  return Math.round((n / d) * 1000) / 10;
}

function resolvePat() {
  return (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
}

async function main() {
  const t0 = Date.now();
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });

  if (String(process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES || "0") !== "0") {
    throw new Error("Refusing run with Airtable writes enabled");
  }

  const def = JSON.parse(fs.readFileSync(FROZEN, "utf8"));
  const ids = def.record_ids;
  const hbxElig = fs.existsSync(HBX_ELIG)
    ? JSON.parse(fs.readFileSync(HBX_ELIG, "utf8"))
    : { hbx_missing_room_ids: [] };
  const hbxMissingSet = new Set(hbxElig.hbx_missing_room_ids || []);

  const pat = resolvePat();
  const baseId = (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
  if (!pat || !baseId) throw new Error("Airtable read credentials required");

  console.log(`Frozen sample: ${ids.length} (seed=${def.seed})`);
  console.log("Loading census fields (read-only)...");

  const table = new Airtable({ apiKey: pat }).base(baseId)(
    MAP_HOTEL_PROPERTY_CENSUS.tableId
  );
  const fields = [
    MAP_CENSUS_FIELDS.propertyName,
    MAP_CENSUS_FIELDS.officialName,
    MAP_CENSUS_FIELDS.propertyIdentityKey,
    MAP_CENSUS_FIELDS.city,
    MAP_CENSUS_FIELDS.country,
    MAP_CENSUS_FIELDS.brandName,
    MAP_CENSUS_FIELDS.website,
    MAP_CENSUS_FIELDS.latitude,
    MAP_CENSUS_FIELDS.longitude,
    MAP_CENSUS_FIELDS.roomCount,
    MAP_CENSUS_FIELDS.hbxHotelCode,
  ];
  const byId = new Map();
  for (let i = 0; i < ids.length; i += 40) {
    const chunk = ids.slice(i, i + 40);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
    await table
      .select({ filterByFormula: formula, fields, pageSize: 100 })
      .eachPage((recs, next) => {
        for (const r of recs) byId.set(r.id, r);
        next();
      });
  }

  const store = createLocalStore({ dataDir: DATA_DIR });
  const idRegistry = createExternalIdRegistry(store);
  const evidence = createEvidenceStore(store);

  const allMissing = [];
  for (const id of ids) {
    const r = byId.get(id);
    if (!r) continue;
    const f = r.fields || {};
    if (hasRooms(f)) continue;
    allMissing.push({
      census_record_id: id,
      name:
        f[MAP_CENSUS_FIELDS.officialName] ||
        f[MAP_CENSUS_FIELDS.propertyName] ||
        null,
      city: f[MAP_CENSUS_FIELDS.city] || null,
      country: f[MAP_CENSUS_FIELDS.country] || null,
      brand: f[MAP_CENSUS_FIELDS.brandName] || null,
      website: f[MAP_CENSUS_FIELDS.website] || null,
      latitude: f[MAP_CENSUS_FIELDS.latitude] != null
        ? Number(f[MAP_CENSUS_FIELDS.latitude])
        : null,
      longitude: f[MAP_CENSUS_FIELDS.longitude] != null
        ? Number(f[MAP_CENSUS_FIELDS.longitude])
        : null,
      hbx: String(f[MAP_CENSUS_FIELDS.hbxHotelCode] || "").trim() || null,
      property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
      hbx_missing_priority: hbxMissingSet.has(id),
    });
  }

  // Prefer 72 HBX-linked missing, then fill to LIMIT
  const prioritized = [
    ...allMissing.filter((h) => h.hbx_missing_priority),
    ...allMissing.filter((h) => !h.hbx_missing_priority),
  ];
  const sample = prioritized.slice(0, LIMIT);

  writeJson(path.join(OUT_DIR, "01-sample.json"), {
    seed: def.seed,
    missing_in_frozen: allMissing.length,
    hbx_linked_missing: allMissing.filter((h) => h.hbx_missing_priority).length,
    researched: sample.length,
    max_searches: MAX_SEARCHES,
    max_fetches: MAX_FETCHES,
    allow_serpapi: !NO_SERPAPI,
  });

  console.log(
    `Researching ${sample.length} hotels (HBX-priority first; serpapi=${!NO_SERPAPI})`
  );

  const rows = [];
  const totals = {
    resolved: 0,
    high_confidence: 0,
    conflicts: 0,
    no_evidence: 0,
    manual_review: 0,
    searches: 0,
    pages_fetched: 0,
    sources_inspected: 0,
  };

  for (let i = 0; i < sample.length; i++) {
    const h = sample[i];
    const hotelId = idRegistry.ensureHotelIdForAirtable(h.census_record_id, {
      property_identity_key: h.property_identity_key,
    });
    const result = await researchHotelRoomCount(
      {
        hotel_id: hotelId,
        hotel_name: h.name,
        city: h.city,
        country: h.country,
        brand: h.brand,
        website: h.website,
        latitude: h.latitude,
        longitude: h.longitude,
        identity_confidence: 0.9,
      },
      {
        evidence,
        allowSerpapi: !NO_SERPAPI,
        maxSearches: MAX_SEARCHES,
        maxPageFetches: MAX_FETCHES,
      }
    );

    totals.searches += result.metrics?.searches || 0;
    totals.pages_fetched += result.metrics?.pages_fetched || 0;
    totals.sources_inspected +=
      (result.metrics?.snippets_inspected || 0) + (result.metrics?.pages_ok || 0);

    if (result.candidate_room_count != null) totals.resolved += 1;
    if (result.confidence >= 0.85 && result.candidate_room_count != null) {
      totals.high_confidence += 1;
    }
    if (result.research_status === "CONFLICT") totals.conflicts += 1;
    if (result.research_status === "NO_EVIDENCE") totals.no_evidence += 1;
    if (result.research_status === "MANUAL_REVIEW" || result.review_required) {
      totals.manual_review += 1;
    }

    rows.push({
      census_record_id: h.census_record_id,
      dhl_id: hotelId,
      hbx_hotel_code: h.hbx,
      name: h.name,
      country: h.country,
      website: h.website,
      candidate_room_count: result.candidate_room_count,
      confidence: result.confidence,
      research_status: result.research_status,
      review_required: result.review_required,
      supporting_quotes: result.supporting_quotes,
      supporting_sources: result.supporting_sources,
      conflicts: result.conflicts,
      metrics: result.metrics,
    });

    if ((i + 1) % 10 === 0 || i === sample.length - 1) {
      console.log(
        `  ${i + 1}/${sample.length} resolved=${totals.resolved} high=${totals.high_confidence} none=${totals.no_evidence}`
      );
    }
    await sleep(SLEEP_MS);
  }

  writeJson(path.join(OUT_DIR, "02-rows.json"), { count: rows.length, rows });

  const n = sample.length || 1;
  const summary = {
    marker: "ROOM_COUNT_RESEARCH_ENGINE_COMPLETE",
    safety: {
      airtable_writes: 0,
      census_writes: 0,
      brand_explorer_writes: 0,
      automatic_merges: 0,
      schema_changes: 0,
      secrets_exposed: false,
    },
    sample: {
      frozen_seed: def.seed,
      researched: sample.length,
      hbx_linked_in_sample: sample.filter((h) => h.hbx_missing_priority).length,
      missing_rooms_in_frozen: allMissing.length,
    },
    recovery: {
      resolved: totals.resolved,
      resolved_pct: pct(totals.resolved, sample.length),
      high_confidence: totals.high_confidence,
      high_confidence_pct: pct(totals.high_confidence, sample.length),
      conflicts: totals.conflicts,
      no_evidence: totals.no_evidence,
      manual_review: totals.manual_review,
      manual_review_pct: pct(totals.manual_review, sample.length),
    },
    performance: {
      total_searches: totals.searches,
      total_pages_fetched: totals.pages_fetched,
      avg_searches_per_hotel: Math.round((totals.searches / n) * 100) / 100,
      avg_pages_per_hotel: Math.round((totals.pages_fetched / n) * 100) / 100,
      avg_sources_inspected: Math.round((totals.sources_inspected / n) * 100) / 100,
      runtime_ms: Date.now() - t0,
      avg_runtime_ms: Math.round((Date.now() - t0) / n),
    },
    scale_projection: {
      note: "Extrapolation from this controlled run only",
      "1000": {
        high_confidence_est: Math.round(
          1000 * (totals.high_confidence / n)
        ),
        manual_review_est: Math.round(1000 * (totals.manual_review / n)),
        searches_est: Math.round(1000 * (totals.searches / n)),
      },
      "5956": {
        high_confidence_est: Math.round(
          5765 * (totals.high_confidence / n)
        ),
        manual_review_est: Math.round(5765 * (totals.manual_review / n)),
        searches_est: Math.round(5765 * (totals.searches / n)),
      },
      "10000": {
        high_confidence_est: Math.round(
          10000 * (totals.high_confidence / n)
        ),
        manual_review_est: Math.round(10000 * (totals.manual_review / n)),
        searches_est: Math.round(10000 * (totals.searches / n)),
      },
      cost: "UNKNOWN USD; SerpApi search units observed via account delta if measured separately",
    },
    recommended_production_workflow: [
      "1. Only research hotels missing Rooms/Keys with resolved identity",
      "2. Prefer official website fetch first (0 SerpApi if page yields High)",
      "3. Cap SerpApi google searches (2–3) per hotel; never crawl",
      "4. Stage evidence + quotes; require human review for CONFLICT / <0.85",
      "5. When Hotelbeds LIVE returns roomsNumber, treat as validator (agreement boost)",
      "6. Auto-accept only after separate founder policy (≥0.85 + official + no conflict)",
    ],
  };

  writeJson(path.join(OUT_DIR, "05-summary.json"), summary);

  const md = `# Room Count Research Engine Validation

\`ROOM_COUNT_RESEARCH_ENGINE_COMPLETE\`

## Recovery (${sample.length} hotels)
| Metric | Count | % |
| --- | ---: | ---: |
| Resolved | ${totals.resolved} | ${pct(totals.resolved, sample.length)}% |
| High confidence (≥0.85) | ${totals.high_confidence} | ${pct(totals.high_confidence, sample.length)}% |
| Conflicts | ${totals.conflicts} | ${pct(totals.conflicts, sample.length)}% |
| No evidence | ${totals.no_evidence} | ${pct(totals.no_evidence, sample.length)}% |
| Manual review | ${totals.manual_review} | ${pct(totals.manual_review, sample.length)}% |

## Performance
- Avg searches/hotel: ${summary.performance.avg_searches_per_hotel}
- Avg page fetches/hotel: ${summary.performance.avg_pages_per_hotel}
- Avg runtime: ${summary.performance.avg_runtime_ms} ms

## Safety
Airtable/Census writes: 0
`;
  fs.writeFileSync(path.join(OUT_DIR, "ROOM_COUNT_RESEARCH_REPORT.md"), md, "utf8");

  console.log("\nROOM_COUNT_RESEARCH_ENGINE_COMPLETE");
  console.log(
    JSON.stringify(
      {
        researched: sample.length,
        resolved: totals.resolved,
        high_confidence: totals.high_confidence,
        no_evidence: totals.no_evidence,
        manual_review: totals.manual_review,
        avg_searches: summary.performance.avg_searches_per_hotel,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
