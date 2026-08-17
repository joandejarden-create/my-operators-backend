#!/usr/bin/env node
/**
 * Tripadvisor Census + Profile Pack pilot — controlled, auditable.
 *
 * Default: READ-ONLY manifest (no Airtable writes) unless BOTH
 *   ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=1
 *   ENABLE_CENSUS_FIELD_ENRICHMENT=1
 * Rooms NEVER auto-written as authoritative.
 *
 * Usage:
 *   node scripts/tripadvisor-census-profile-pack-pilot.mjs
 *   node scripts/tripadvisor-census-profile-pack-pilot.mjs --pilot=100
 *   node scripts/tripadvisor-census-profile-pack-pilot.mjs --skip-apify
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
  MAP_PROVIDER_IDS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import {
  CALA_COUNTRIES,
  COMPLETENESS_PRIORITY_FIELDS,
  TA_FIELD_MAP,
  WRITE_TIER,
  buildTripadvisorProfilePack,
  profilePackCoverageFlags,
  proposeCensusWrites,
  evaluateProductionWriteGate,
  competitiveRankPercentile,
} from "../lib/hotel-intelligence/tripadvisor-profile/index.js";
import { matchTripadvisorHotel } from "../lib/hotel-intelligence/tripadvisor-rooms/match.js";
import { createExternalIdRegistry } from "../lib/hotel-intelligence/external-ids.js";
import { createEvidenceStore } from "../lib/hotel-intelligence/evidence-store.js";
import {
  createApifyUsageStore,
  APIFY_USE_CASES,
  APIFY_AUTH_METHODS,
} from "../lib/hotel-intelligence/apify-usage/index.js";

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES =
  process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES || "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-census-profile-pack-v1"
);
const DATA_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/tripadvisor-census-profile-pack-v1"
);

function parseArgs(argv) {
  const out = {
    pilot: 100,
    skipApify: false,
    completenessLimit: 0,
    reusePilot: true,
    reselectPilot: false,
  };
  for (const a of argv.slice(2)) {
    if (a.startsWith("--pilot=")) out.pilot = Number(a.slice(8)) || 100;
    if (a === "--skip-apify") out.skipApify = true;
    if (a.startsWith("--completeness-limit="))
      out.completenessLimit = Number(a.slice(21)) || 0;
    if (a === "--reselect-pilot") {
      out.reselectPilot = true;
      out.reusePilot = false;
    }
    if (a === "--no-reuse-pilot") out.reusePilot = false;
  }
  return out;
}

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(file, data) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function blank(v) {
  return v == null || String(v).trim() === "";
}
function pct(n, d) {
  if (!d) return null;
  return Number(((100 * n) / d).toFixed(1));
}

function loadTaPools() {
  const paths = [
    path.join(
      ROOT,
      "data/hotel-intelligence/giata-tripadvisor-room-decision-v1/ta-decision-pool.json"
    ),
    path.join(
      ROOT,
      "data/hotel-intelligence/tripadvisor-apify-benchmark-v1/ta-pool.json"
    ),
    path.join(DATA_DIR, "ta-pilot-pool.json"),
  ];
  const byId = new Map();
  const sources = [];
  for (const p of paths) {
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const items = Array.isArray(j) ? j : j.items || [];
    sources.push({ path: p, count: items.length });
    for (const it of items) {
      if (it?.id != null) byId.set(String(it.id), it);
    }
  }
  return { items: [...byId.values()], sources };
}

function gapScore(fields) {
  let s = 0;
  if (blank(fields[MAP_CENSUS_FIELDS.website])) s += 5;
  if (blank(fields[MAP_CENSUS_FIELDS.phone])) s += 5;
  if (blank(fields[MAP_CENSUS_FIELDS.address])) s += 4;
  if (blank(fields[MAP_CENSUS_FIELDS.latitude])) s += 5;
  if (blank(fields[MAP_CENSUS_FIELDS.longitude])) s += 5;
  if (blank(fields[MAP_CENSUS_FIELDS.roomCount])) s += 3;
  if (blank(fields["Hotel Class / Segment"])) s += 2;
  if (blank(fields[MAP_CENSUS_FIELDS.propertyType])) s += 2;
  if (blank(fields[MAP_CENSUS_FIELDS.stateRegion])) s += 1;
  return s;
}

async function loadCensusHotels() {
  const token = (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
  const baseId = (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
  if (!token || !baseId) throw new Error("Airtable credentials missing");

  const base = new Airtable({ apiKey: token }).base(baseId);
  const fieldNames = [
    ...new Set([
      ...COMPLETENESS_PRIORITY_FIELDS,
      MAP_CENSUS_FIELDS.propertyName,
      MAP_CENSUS_FIELDS.officialName,
      MAP_CENSUS_FIELDS.propertyIdentityKey,
      "Hotel Class / Segment",
      "Amenities - Structured Tags",
      "Phone Confidence",
      "Address Confidence",
      "Coordinate Confidence",
      "Rooms Confidence",
    ]),
  ];

  const hotels = [];
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({ pageSize: 100, fields: fieldNames })
    .eachPage((page, next) => {
      for (const rec of page) {
        const f = rec.fields || {};
        const name = String(
          f[MAP_CENSUS_FIELDS.officialName] ||
            f[MAP_CENSUS_FIELDS.propertyName] ||
            ""
        ).trim();
        const country = String(f[MAP_CENSUS_FIELDS.country] || "").trim();
        if (!name || !country) continue;
        hotels.push({
          record_id: rec.id,
          name,
          city: String(f[MAP_CENSUS_FIELDS.city] || "").trim() || null,
          country,
          fields: f,
          gap_score: gapScore(f),
        });
      }
      next();
    });
  return hotels;
}

function buildCompleteness(hotels) {
  const total = hotels.length;
  const matrix = {};
  for (const field of COMPLETENESS_PRIORITY_FIELDS) {
    let present = 0;
    for (const h of hotels) {
      if (!blank(h.fields[field])) present += 1;
    }
    matrix[field] = {
      TOTAL_HOTELS: total,
      FIELD_PRESENT: present,
      FIELD_MISSING: total - present,
      COMPLETENESS_PCT: pct(present, total),
    };
  }
  return matrix;
}

function selectPilot(hotels, n) {
  const calaSet = new Set(CALA_COUNTRIES.map((c) => c.toLowerCase()));
  const cala = hotels.filter((h) => calaSet.has(h.country.toLowerCase()));
  const pool = cala.length >= n ? cala : hotels;
  // prioritize gaps, diversify countries
  const byCountry = new Map();
  for (const h of pool) {
    if (!byCountry.has(h.country)) byCountry.set(h.country, []);
    byCountry.get(h.country).push(h);
  }
  for (const arr of byCountry.values()) {
    arr.sort((a, b) => b.gap_score - a.gap_score);
  }
  const countries = [...byCountry.keys()].sort(
    (a, b) => byCountry.get(b).length - byCountry.get(a).length
  );
  const picked = [];
  const used = new Set();
  let guard = 0;
  while (picked.length < n && guard < n * 40) {
    guard += 1;
    let progressed = false;
    for (const c of countries) {
      if (picked.length >= n) break;
      const arr = byCountry.get(c);
      while (arr.length) {
        const h = arr.shift();
        if (used.has(h.record_id)) continue;
        if (h.gap_score <= 0 && picked.length > n * 0.7) continue;
        used.add(h.record_id);
        picked.push(h);
        progressed = true;
        break;
      }
    }
    if (!progressed) break;
  }
  return picked;
}

function isRichProfile(it) {
  return (
    it &&
    (it.type === "HOTEL" || it.category === "hotel") &&
    it.rating != null &&
    it.rankingPosition != null
  );
}

async function main() {
  const args = parseArgs(process.argv);
  ensureDir(OUT_DIR);
  ensureDir(DATA_DIR);

  const writeGate = evaluateProductionWriteGate(process.env);
  console.error(JSON.stringify({ writeGate }, null, 2));

  console.error("Loading census…");
  let hotels = await loadCensusHotels();
  if (args.completenessLimit > 0) {
    hotels = hotels.slice(0, args.completenessLimit);
  }
  const before = buildCompleteness(hotels);
  writeJson(path.join(OUT_DIR, "before-completeness-matrix.json"), before);
  writeJson(path.join(DATA_DIR, "census-total.json"), {
    total: hotels.length,
    cala: hotels.filter((h) =>
      CALA_COUNTRIES.map((c) => c.toLowerCase()).includes(h.country.toLowerCase())
    ).length,
  });

  const pilotPath = path.join(DATA_DIR, "pilot-hotels.json");
  let pilot;
  if (args.reusePilot && !args.reselectPilot && fs.existsSync(pilotPath)) {
    const prior = JSON.parse(fs.readFileSync(pilotPath, "utf8"));
    const byId = new Map(hotels.map((h) => [h.record_id, h]));
    const reused = [];
    for (const row of prior.hotels || []) {
      const h = byId.get(row.record_id);
      if (h) reused.push(h);
    }
    if (reused.length >= Math.min(args.pilot, (prior.hotels || []).length)) {
      pilot = reused.slice(0, args.pilot);
      console.error(
        `Reusing pinned pilot list (${pilot.length} hotels) from ${pilotPath}`
      );
    }
  }
  if (!pilot) {
    pilot = selectPilot(hotels, args.pilot);
  }
  writeJson(pilotPath, {
    n: pilot.length,
    hotels: pilot.map((h) => ({
      record_id: h.record_id,
      name: h.name,
      city: h.city,
      country: h.country,
      gap_score: h.gap_score,
    })),
  });

  const taPool = loadTaPools();
  const extIds = createExternalIdRegistry();
  const evidence = createEvidenceStore();

  const results = [];
  const manifest = [];
  const blockedAll = [];
  const candidatesAll = [];
  const packs = [];
  const needApify = [];

  for (const h of pilot) {
    const hotelQuery = {
      name: h.name,
      city: h.city,
      country: h.country,
      lat: h.fields[MAP_CENSUS_FIELDS.latitude] ?? null,
      lng: h.fields[MAP_CENSUS_FIELDS.longitude] ?? null,
      website: h.fields[MAP_CENSUS_FIELDS.website] || null,
      rooms: h.fields[MAP_CENSUS_FIELDS.roomCount] ?? null,
    };
    const { match, rejection } = matchTripadvisorHotel(hotelQuery, taPool.items);
    if (!match) {
      needApify.push({
        record_id: h.record_id,
        name: h.name,
        city: h.city,
        country: h.country,
        search_url: `https://www.tripadvisor.com/Search?q=${encodeURIComponent(
          [h.name, h.city, h.country].filter(Boolean).join(" ")
        )}`,
        rejection: rejection?.reason || "no_match",
      });
      results.push({
        record_id: h.record_id,
        name: h.name,
        country: h.country,
        status: "UNMATCHED_NEED_APIFY",
        rejection: rejection?.reason || null,
      });
      continue;
    }

    const taItem = match.item;
    const matchMeta = {
      confidence: match.confidence,
      score: match.score,
      retrieved_at: new Date().toISOString(),
    };
    const pack = buildTripadvisorProfilePack(taItem, matchMeta);
    packs.push({
      record_id: h.record_id,
      hotel_name: h.name,
      coverage: profilePackCoverageFlags(pack),
      pack,
    });

    // Local external id + evidence (not Airtable)
    const hotelId = extIds.ensureHotelIdForAirtable(h.record_id, {
      property_identity_key:
        h.fields[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    });
    extIds.linkExternalId(
      hotelId,
      MAP_PROVIDER_IDS.tripadvisor_apify,
      String(taItem.id),
      { source_url: taItem.webUrl || null }
    );

    // Store profile pack evidence locally for HI fields
    for (const [field, value] of [
      ["tripadvisor_rating", pack.guest_reputation.rating],
      ["tripadvisor_review_count", pack.guest_reputation.number_of_reviews],
      [
        "guest_ranking_percentile",
        pack.competitive_standing.guest_ranking_percentile,
      ],
      ["tripadvisor_hotel_class", pack.product.hotel_class],
    ]) {
      if (value == null) continue;
      evidence.addEvidence({
        hotel_id: hotelId,
        field,
        value,
        source: MAP_PROVIDER_IDS.tripadvisor_apify,
        source_record_id: String(taItem.id),
        completeness: 1,
      });
    }

    const proposed = proposeCensusWrites({
      censusFields: h.fields,
      taItem,
      matchMeta,
      profilePack: pack,
    });

    for (const p of proposed.proposals) {
      manifest.push({
        hotel: h.name,
        hotel_id: hotelId,
        airtable_record_id: h.record_id,
        field: p.field,
        old_value: p.old_value,
        new_value: p.new_value,
        source: p.source,
        field_confidence: p.field_confidence,
        match_confidence: p.match_confidence,
        write_policy_tier: p.write_policy_tier,
        reason: p.reason,
        companion_fields: p.companion_fields || null,
        requires_env: p.requires_env || null,
        requires_manual_or_flag: p.requires_manual_or_flag || false,
      });
    }
    blockedAll.push(
      ...proposed.blocked.map((b) => ({
        hotel: h.name,
        airtable_record_id: h.record_id,
        ...b,
      }))
    );
    candidatesAll.push(
      ...proposed.candidates.map((c) => ({
        hotel: h.name,
        airtable_record_id: h.record_id,
        hotel_id: hotelId,
        ...c,
      }))
    );

    results.push({
      record_id: h.record_id,
      hotel_id: hotelId,
      name: h.name,
      country: h.country,
      status: "MATCHED",
      tripadvisor_id: String(taItem.id),
      match_confidence: match.confidence,
      rich_profile: isRichProfile(taItem),
      proposals: proposed.proposals.length,
      blocked: proposed.blocked.length,
      candidates: proposed.candidates.length,
      guest_ranking_percentile:
        pack.competitive_standing.guest_ranking_percentile,
    });
  }

  // Tier A only for potential execute — still gated
  const tierA = manifest.filter(
    (m) =>
      m.write_policy_tier === WRITE_TIER.A_SAFE_GAP_FILL &&
      !m.requires_manual_or_flag
  );
  const tierB = manifest.filter(
    (m) => m.write_policy_tier === WRITE_TIER.B_CONDITIONAL
  );

  let executed = [];
  if (writeGate.allow_any_airtable_writes) {
    // Explicitly not implementing auto-apply in this pilot without further founder confirm.
    // Safety: still stop — require CONFIRM_TRIPADVISOR_TIER_A_WRITES=1
    if (String(process.env.CONFIRM_TRIPADVISOR_TIER_A_WRITES || "0") === "1") {
      console.error(
        "CONFIRM_TRIPADVISOR_TIER_A_WRITES set — but apply executor not enabled in this script version; manifest only."
      );
    }
  }

  // Profile pack coverage
  const covKeys = [
    "RATING_COVERAGE",
    "REVIEW_COUNT_COVERAGE",
    "RANKING_COVERAGE",
    "RANK_DENOMINATOR_COVERAGE",
    "GUEST_RANK_PERCENTILE_COVERAGE",
    "AMENITY_COVERAGE",
    "CATEGORY_SCORE_COVERAGE",
    "PRICE_POSITION_COVERAGE",
    "HOTEL_CLASS_COVERAGE",
    "CONTACT_COVERAGE",
  ];
  const coverage = {};
  for (const k of covKeys) {
    const n = packs.filter((p) => p.coverage[k]).length;
    coverage[k] = {
      n,
      of_matched: packs.length,
      pct: pct(n, packs.length),
    };
  }

  // After matrix: simulate Tier A null-fills on pilot only (not written)
  const afterSim = structuredClone(before);
  const byRecord = new Map(pilot.map((h) => [h.record_id, { ...h.fields }]));
  for (const m of tierA) {
    const f = byRecord.get(m.airtable_record_id);
    if (!f) continue;
    if (blank(f[m.field])) f[m.field] = m.new_value;
  }
  const simulatedHotels = hotels.map((h) => {
    if (!byRecord.has(h.record_id)) return h;
    return { ...h, fields: { ...h.fields, ...byRecord.get(h.record_id) } };
  });
  const after = buildCompleteness(simulatedHotels);

  const improvement = {};
  for (const field of COMPLETENESS_PRIORITY_FIELDS) {
    const b = before[field];
    const a = after[field];
    const written = tierA.filter((m) => m.field === field).length;
    improvement[field] = {
      MISSING_BEFORE: b.FIELD_MISSING,
      VALUES_PROPOSED_TIER_A: written,
      VALUES_WRITTEN: executed.filter((e) => e.field === field).length,
      COMPLETENESS_BEFORE: b.COMPLETENESS_PCT,
      COMPLETENESS_AFTER_IF_TIER_A_APPLIED: a.COMPLETENESS_PCT,
      RECOVERY_RATE_OF_PILOT_PROPOSALS: written
        ? pct(written, written)
        : null,
    };
  }

  writeJson(path.join(DATA_DIR, "need-apify.json"), { hotels: needApify });
  writeJson(path.join(DATA_DIR, "profile-packs.json"), { packs });
  writeJson(path.join(OUT_DIR, "proposed-write-manifest.json"), {
    production_safety: writeGate,
    proposed_writes: manifest,
    tier_a: tierA,
    tier_b: tierB,
    executed_writes: executed,
  });
  writeJson(path.join(OUT_DIR, "rejected-blocked.json"), { blocked: blockedAll });
  writeJson(path.join(OUT_DIR, "room-candidates.json"), {
    candidates: candidatesAll,
  });
  writeJson(path.join(OUT_DIR, "pilot-results.json"), { results });
  writeJson(path.join(OUT_DIR, "profile-pack-coverage.json"), coverage);
  writeJson(path.join(OUT_DIR, "after-completeness-matrix-simulated.json"), after);
  writeJson(path.join(OUT_DIR, "completeness-improvement.json"), improvement);
  writeJson(path.join(OUT_DIR, "field-map.json"), { map: TA_FIELD_MAP });

  const summary = {
    CENSUS_TOTAL: hotels.length,
    PILOT_HOTELS: pilot.length,
    MATCHED: results.filter((r) => r.status === "MATCHED").length,
    UNMATCHED_NEED_APIFY: needApify.length,
    RICH_PROFILES: results.filter((r) => r.rich_profile).length,
    PROPOSED_WRITES: manifest.length,
    TIER_A_PROPOSED: tierA.length,
    TIER_B_PROPOSED: tierB.length,
    EXECUTED_WRITES: executed.length,
    BLOCKED_WRITES: blockedAll.length,
    ROOM_CANDIDATES: candidatesAll.filter((c) => c.field === MAP_CENSUS_FIELDS.roomCount)
      .length,
    PROFILE_PACKS: packs.length,
    PRODUCTION_SAFETY_STATUS: writeGate.status,
    NEED_APIFY_FOR_FULL_CALA_DETAIL: needApify.length,
  };
  writeJson(path.join(OUT_DIR, "pilot-summary.json"), summary);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
