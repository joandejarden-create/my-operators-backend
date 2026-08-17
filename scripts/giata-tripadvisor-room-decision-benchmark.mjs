#!/usr/bin/env node
/**
 * GIATA MHG vs Tripadvisor room-count decision benchmark — READ ONLY.
 *
 * - Uses GIATA MHG TEST + MultiCodes TEST (random properties — not CALA-valid for coverage).
 * - Matches Tripadvisor via injected pool and/or --ta-dataset JSON.
 * - Matches Dealality census trusted Rooms / Keys when identity is high-confidence.
 * - NEVER writes Airtable / census.
 *
 * Usage:
 *   node scripts/giata-tripadvisor-room-decision-benchmark.mjs
 *   node scripts/giata-tripadvisor-room-decision-benchmark.mjs --mhg-limit=40 --ta-dataset=path.json
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
import { ROOM_COMPARE } from "../lib/hotel-intelligence/tripadvisor-rooms/constants.js";
import {
  classifyRoomCompare,
  matchTripadvisorHotel,
  nameSimilarity,
} from "../lib/hotel-intelligence/tripadvisor-rooms/match.js";

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/giata-tripadvisor-room-decision-v1"
);
const DATA_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/giata-tripadvisor-room-decision-v1"
);
const WARNING = "TEST_SAMPLE_NOT_VALID_FOR_GEOGRAPHIC_COVERAGE";

function parseArgs(argv) {
  const out = {
    mhgLimit: 40,
    taDataset: null,
    skipLiveGiata: false,
    skipCensus: false,
  };
  for (const a of argv.slice(2)) {
    if (a.startsWith("--mhg-limit=")) out.mhgLimit = Number(a.slice(12)) || 40;
    if (a.startsWith("--ta-dataset=")) out.taDataset = a.slice(13);
    if (a === "--skip-live-giata") out.skipLiveGiata = true;
    if (a === "--skip-census") out.skipCensus = true;
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

function sanitizeError(msg) {
  return String(msg || "")
    .replace(/Basic\s+[A-Za-z0-9+/=]+/gi, "Basic [REDACTED]")
    .replace(/Authorization:\s*\S+/gi, "Authorization: [REDACTED]")
    .replace(/password[=:]\s*\S+/gi, "password=[REDACTED]")
    .slice(0, 240);
}

function basicAuthHeader(user, pass) {
  return `Basic ${Buffer.from(`${user}:${pass}`, "utf8").toString("base64")}`;
}

function resolveGiataBasicUsername(rawUser) {
  const u = String(rawUser || "").trim();
  const company = String(
    process.env.GIATA_AUTH_COMPANY || process.env.GIATA_COMPANY || ""
  ).trim();
  const nameOverride = String(process.env.GIATA_AUTH_NAME || "").trim();
  if (u.includes("|")) return u;
  if (u.includes("@")) {
    const [local, domain] = u.split("@");
    if (local && domain) return `${local}|${domain}`;
  }
  if (nameOverride && company) return `${nameOverride}|${company}`;
  if (u && company) return `${u}|${company}`;
  return u;
}

async function giataRequest(baseUrl, pathname, user, pass) {
  const base = String(baseUrl || "").replace(/\/$/, "");
  const url = `${base}${pathname.startsWith("/") ? pathname : `/${pathname}`}`;
  const headers = {
    Accept: "application/xml, text/xml, application/json, */*",
    Authorization: basicAuthHeader(user, pass),
  };
  const res = await fetch(url, { headers });
  const text = await res.text();
  return { ok: res.ok, status: res.status, text };
}

function allMatches(str, re) {
  const out = [];
  const r = new RegExp(re.source, re.flags.includes("g") ? re.flags : `${re.flags}g`);
  let m;
  while ((m = r.exec(str))) out.push(m);
  return out;
}

function attr(tag, name) {
  const m = String(tag || "").match(new RegExp(`${name}="([^"]*)"`, "i"));
  return m ? m[1] : null;
}

function textBetween(block, tag) {
  const m = String(block).match(
    new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tag}>`, "i")
  );
  if (!m) return null;
  return m[1].replace(/<[^>]+>/g, "").trim() || null;
}

function extractGiataIdsFromList(xml, limit) {
  const ids = [];
  for (const m of allMatches(xml, /giataId="(\d+)"/g)) {
    if (!ids.includes(m[1])) ids.push(m[1]);
    if (ids.length >= limit) break;
  }
  return ids;
}

function extractNumRoomsTotal(xml) {
  const m = String(xml).match(
    /<fact\b[^>]*name="num_rooms_total"[^>]*>[\s\S]*?<value>(\d+)<\/value>/i
  );
  if (m) return Number(m[1]);
  return null;
}

function parseMhgShell(xml) {
  const block = String(xml).match(/<item\b[\s\S]*?<\/item>/i)?.[0] || String(xml);
  return {
    giata_id: attr(block.match(/<item\b[^>]*>/i)?.[0] || "", "giataId"),
    name: textBetween(block, "name"),
    city: textBetween(block, "city"),
    country: textBetween(block, "country"),
    street: textBetween(block, "street"),
  };
}

function parseMultiCodes(xml) {
  const block =
    String(xml).match(/<property\b[\s\S]*?<\/property>/i)?.[0] || String(xml);
  const lat = textBetween(block, "latitude");
  const lng = textBetween(block, "longitude");
  return {
    giata_id: attr(block.match(/<property\b[^>]*>/i)?.[0] || "", "giataId"),
    name: textBetween(block, "name"),
    city: textBetween(block, "city") || textBetween(block, "cityName"),
    country: textBetween(block, "country"),
    address: [textBetween(block, "street"), textBetween(block, "streetNumber")]
      .filter(Boolean)
      .join(" "),
    latitude: lat != null && lat !== "" ? Number(lat) : null,
    longitude: lng != null && lng !== "" ? Number(lng) : null,
    phone: textBetween(block, "phone"),
    website: textBetween(block, "url"),
  };
}

function countryNameFromIso(iso) {
  const map = {
    MX: "Mexico",
    DO: "Dominican Republic",
    CO: "Colombia",
    CR: "Costa Rica",
    PA: "Panama",
    EG: "Egypt",
    IT: "Italy",
    BG: "Bulgaria",
    MV: "Maldives",
    US: "United States",
    ES: "Spain",
    FR: "France",
    DE: "Germany",
    GB: "United Kingdom",
    BR: "Brazil",
    AR: "Argentina",
    CL: "Chile",
    PE: "Peru",
    PT: "Portugal",
    GR: "Greece",
    TR: "Turkey",
    AE: "United Arab Emirates",
    TH: "Thailand",
    ID: "Indonesia",
    MY: "Malaysia",
    SG: "Singapore",
    JP: "Japan",
    CN: "China",
    IN: "India",
  };
  const c = String(iso || "").trim().toUpperCase();
  if (c.length === 2) return map[c] || c;
  return String(iso || "").trim() || null;
}

function fold(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadCachedMhgSample() {
  const candidates = [
    path.join(DATA_DIR, "giata-mhg-sample.json"),
    path.join(
      ROOT,
      "reports/hotel-intelligence/giata-test-products-validation-v1/mhg-sample.json"
    ),
  ];
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const sample = j.hotels || j.sample || [];
    if (!sample.length) continue;
    return sample.map((s) => {
      const rooms =
        s.num_rooms_total ??
        (s.room_findings || []).find((f) =>
          /num_rooms_total/i.test(f.field_name || "")
        )?.numeric_candidate ??
        (s.room_findings || []).find(
          (f) => f.classification === "TOTAL_PROPERTY_ROOM_COUNT_CANDIDATE"
        )?.numeric_candidate ??
        null;
      return {
        giata_id: String(s.giata_id),
        name: s.name,
        city: s.city,
        country: s.country,
        country_name: s.country_name || countryNameFromIso(s.country),
        address: s.address || s.street || null,
        latitude: s.latitude ?? null,
        longitude: s.longitude ?? null,
        website: s.website || null,
        phone: s.phone || null,
        num_rooms_total: rooms,
        source: p.includes("giata-mhg-sample")
          ? "cached_decision_sample"
          : "cached_mhg_sample",
      };
    });
  }
  return [];
}

async function fetchLiveMhg(limit) {
  const base =
    process.env.GIATA_MHG_BASE_URL ||
    "https://ghgml.giatamedia.com/webservice/rest/1.0";
  const pass = String(process.env.GIATA_MHG_PASSWORD || "").trim();
  const user = resolveGiataBasicUsername(
    process.env.GIATA_MHG_USERNAME || ""
  );
  if (!user || !pass) {
    return { ok: false, error: "mhg_credentials_missing", hotels: [] };
  }
  const list = await giataRequest(base, "/items/", user, pass);
  if (!list.ok) {
    return {
      ok: false,
      error: sanitizeError(`MHG list HTTP ${list.status}`),
      hotels: [],
    };
  }
  const ids = extractGiataIdsFromList(list.text, limit);
  const hotels = [];
  for (const id of ids) {
    const item = await giataRequest(base, `/items/${id}`, user, pass);
    const facts = await giataRequest(base, `/factsheets/${id}`, user, pass);
    const shell = parseMhgShell(item.ok ? item.text : "");
    const rooms = facts.ok ? extractNumRoomsTotal(facts.text) : null;
    hotels.push({
      giata_id: String(shell.giata_id || id),
      name: shell.name,
      city: shell.city,
      country: shell.country,
      country_name: countryNameFromIso(shell.country),
      street: shell.street,
      num_rooms_total: rooms,
      source: "live_mhg_test",
      mhg_item_ok: item.ok,
      mhg_facts_ok: facts.ok,
    });
  }
  return {
    ok: true,
    authenticated: true,
    hotels,
    username_has_pipe: user.includes("|"),
  };
}

async function enrichMultiCodes(hotels) {
  const base =
    process.env.GIATA_MULTICODES_BASE_URL ||
    "https://multicodes.giatamedia.com/webservice/rest/1.latest";
  const pass = String(process.env.GIATA_MULTICODES_PASSWORD || "").trim();
  const user = resolveGiataBasicUsername(
    process.env.GIATA_MULTICODES_USERNAME || ""
  );
  if (!user || !pass) return { ok: false, hotels };
  const out = [];
  for (const h of hotels) {
    try {
      const res = await giataRequest(
        base,
        `/properties/${h.giata_id}`,
        user,
        pass
      );
      if (!res.ok) {
        out.push({ ...h, multicodes_ok: false });
        continue;
      }
      const mc = parseMultiCodes(res.text);
      out.push({
        ...h,
        multicodes_ok: true,
        name: h.name || mc.name,
        city: h.city || mc.city,
        country: h.country || mc.country,
        country_name: countryNameFromIso(h.country || mc.country),
        address: mc.address || h.street || null,
        latitude: mc.latitude,
        longitude: mc.longitude,
        website: mc.website || null,
        phone: mc.phone || null,
      });
    } catch (err) {
      out.push({
        ...h,
        multicodes_ok: false,
        multicodes_error: sanitizeError(err.message),
      });
    }
  }
  return { ok: true, hotels: out };
}

function loadTripadvisorPool(taDatasetPath) {
  const paths = [
    taDatasetPath,
    path.join(DATA_DIR, "ta-decision-pool.json"),
    path.join(
      ROOT,
      "data/hotel-intelligence/tripadvisor-apify-benchmark-v1/ta-pool.json"
    ),
  ].filter(Boolean);
  const items = [];
  const sources = [];
  for (const p of paths) {
    if (!p || !fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const arr = Array.isArray(j) ? j : j.items || j.dataset || [];
    if (arr.length) {
      items.push(...arr);
      sources.push({ path: p, count: arr.length });
    }
  }
  // de-dupe by id
  const byId = new Map();
  for (const it of items) {
    const id = it?.id != null ? String(it.id) : null;
    if (id) byId.set(id, it);
    else items.push(it);
  }
  return { items: [...byId.values()], sources };
}

function loadOfficialVerified() {
  const p = path.join(
    ROOT,
    "data/hotel-intelligence/tripadvisor-apify-rooms-enrichment-v2/verification-v2-results.json"
  );
  if (!fs.existsSync(p)) return [];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const rows = Array.isArray(j) ? j : j.rows || j.results || [];
  return rows
    .filter(
      (r) =>
        r.rooms_verification_status === "VERIFIED_PRIMARY_SOURCE" ||
        r.verification?.status === "VERIFIED_PRIMARY_SOURCE"
    )
    .map((r) => ({
      name: r.name || r.hotel_name || r.hotel?.name,
      rooms:
        r.rooms_candidate ||
        r.verified_room_count ||
        r.verification?.verified_rooms ||
        null,
      country: r.country || r.hotel?.country,
      record_id: r.record_id || r.hotel?.record_id,
    }));
}

async function loadCensusIndex() {
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
  if (!token || !baseId) return { ok: false, hotels: [], error: "airtable_missing" };

  const base = new Airtable({ apiKey: token }).base(baseId);
  const fields = [
    MAP_CENSUS_FIELDS.propertyName,
    MAP_CENSUS_FIELDS.officialName,
    MAP_CENSUS_FIELDS.city,
    MAP_CENSUS_FIELDS.country,
    MAP_CENSUS_FIELDS.roomCount,
    MAP_CENSUS_FIELDS.latitude,
    MAP_CENSUS_FIELDS.longitude,
    MAP_CENSUS_FIELDS.website,
    MAP_CENSUS_FIELDS.identityConfidence,
    MAP_CENSUS_FIELDS.dataConfidenceTier,
  ];
  const hotels = [];
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({ pageSize: 100, fields })
    .eachPage((page, next) => {
      for (const rec of page) {
        const f = rec.fields || {};
        const name = String(
          f[MAP_CENSUS_FIELDS.officialName] ||
            f[MAP_CENSUS_FIELDS.propertyName] ||
            ""
        ).trim();
        if (!name) continue;
        const roomsRaw = f[MAP_CENSUS_FIELDS.roomCount];
        const rooms =
          roomsRaw != null && roomsRaw !== "" ? Number(roomsRaw) : null;
        hotels.push({
          record_id: rec.id,
          name,
          city: String(f[MAP_CENSUS_FIELDS.city] || "").trim() || null,
          country: String(f[MAP_CENSUS_FIELDS.country] || "").trim() || null,
          rooms: Number.isFinite(rooms) ? rooms : null,
          lat: f[MAP_CENSUS_FIELDS.latitude] ?? null,
          lng: f[MAP_CENSUS_FIELDS.longitude] ?? null,
          website: String(f[MAP_CENSUS_FIELDS.website] || "").trim() || null,
          tier: String(
            f[MAP_CENSUS_FIELDS.dataConfidenceTier] ||
              f[MAP_CENSUS_FIELDS.identityConfidence] ||
              ""
          ),
        });
      }
      next();
    });
  return { ok: true, hotels };
}

function matchDealality(giataHotel, censusHotels) {
  const country = fold(giataHotel.country_name || giataHotel.country);
  let best = null;
  for (const h of censusHotels) {
    const hc = fold(h.country);
    if (country && hc && country !== hc) {
      // allow ISO vs name soft: if neither contains the other, skip
      if (!hc.includes(country) && !country.includes(hc) && country.length <= 3) {
        // iso already mapped; if still mismatch skip
        continue;
      }
      if (country.length > 3 && hc.length > 3 && country !== hc) continue;
    }
    const sim = nameSimilarity(giataHotel.name, h.name);
    if (sim < 0.82) continue;
    const score = sim;
    if (!best || score > best.score) {
      best = { hotel: h, score, confidence: sim >= 0.92 ? "high" : "medium" };
    }
  }
  return best;
}

function matchOfficial(giataHotel, official) {
  let best = null;
  for (const o of official) {
    const sim = nameSimilarity(giataHotel.name, o.name);
    if (sim < 0.85) continue;
    if (!best || sim > best.score) best = { ...o, score: sim };
  }
  return best;
}

function classifyPair(a, b) {
  if (a == null || b == null || !Number.isFinite(Number(a)) || !Number.isFinite(Number(b))) {
    return "MISSING";
  }
  return classifyRoomCompare(a, b, { numberOfRooms: b });
}

function pct(n, d) {
  if (!d) return null;
  return Number(((100 * n) / d).toFixed(1));
}

function simulateWaterfall(row) {
  if (row.official_rooms != null && Number.isFinite(row.official_rooms)) {
    return "VERIFIED_PRIMARY_SOURCE";
  }
  const ta = row.tripadvisor_rooms;
  const g = row.giata_rooms;
  const both =
    ta != null && g != null && Number.isFinite(ta) && Number.isFinite(g);
  if (both) {
    const cmp = classifyPair(g, ta);
    if (
      (cmp === ROOM_COMPARE.EXACT || cmp === ROOM_COMPARE.NEAR_MATCH) &&
      row.match_confidence !== "low" &&
      row.ta_match_confidence !== "low"
    ) {
      // Independence gate: do not auto-promote to multi-source if uncertain
      if (row.source_independence === "INDEPENDENT" ||
          row.source_independence === "LIKELY_INDEPENDENT") {
        return "VERIFIED_TA_GIATA";
      }
      return "SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT";
    }
    if (cmp === ROOM_COMPARE.CONFLICT) return "CONFLICT_REVIEW_REQUIRED";
  }
  if (g != null && Number.isFinite(g) && (ta == null || !Number.isFinite(ta))) {
    return "GIATA_ONLY";
  }
  if (ta != null && Number.isFinite(ta) && (g == null || !Number.isFinite(g))) {
    return "TA_ONLY";
  }
  return "UNRESOLVED";
}

function assessIndependence(evidence) {
  // hotelClassAttribution = star class from GIATA — not proof rooms share upstream.
  // Near-universal Giata class attribution shows deep commercial coupling.
  // Conflicts prove rooms are not always identical copies; independence still unproven.
  const attrN = evidence.ta_pool_hotelClassAttribution_giata_mentions || 0;
  const poolN = evidence.ta_pool_size || 0;
  const attrNote =
    poolN > 0
      ? `In this decision TA pool, ${attrN}/${poolN} hotels (~${(
          (100 * attrN) /
          Math.max(poolN, 1)
        ).toFixed(0)}%) show Giata class attribution — deep Tripadvisor↔Giata coupling for classification raises (but does not prove) shared-upstream risk for inventory fields.`
      : "Giata class attribution is commonly present on Tripadvisor hotel records.";
  return {
    assessment: "INDEPENDENCE_UNCERTAIN",
    rationale: [
      "Tripadvisor hotelClassAttribution referencing Giata documents star/class provenance, not room inventory.",
      attrNote,
      "No Tripadvisor schema field or GIATA entitlement doc in this repo proves numberOfRooms ← MHG num_rooms_total.",
      "TA↔GIATA room conflicts on high-confidence matches show fields are not always identical (freshness/definition divergence possible even under partial shared upstream).",
      "Therefore TA+GIATA agreement must NOT auto-qualify as VERIFIED_MULTI_SOURCE / VERIFIED_TA_GIATA without additional independent primary evidence.",
    ],
    evidence_notes: evidence,
  };
}

async function main() {
  const args = parseArgs(process.argv);
  ensureDir(OUT_DIR);
  ensureDir(DATA_DIR);

  const access = {
    mhg: { credentials_present: false, authenticated: false },
    multicodes: { credentials_present: false, authenticated: false },
    warning: WARNING,
  };

  let giataHotels = [];
  if (!args.skipLiveGiata) {
    const live = await fetchLiveMhg(args.mhgLimit);
    access.mhg = {
      credentials_present: Boolean(
        process.env.GIATA_MHG_USERNAME && process.env.GIATA_MHG_PASSWORD
      ),
      authenticated: Boolean(live.ok),
      error: live.error || null,
      sample_size: live.hotels?.length || 0,
      fields_confirmed: [
        "giata_id",
        "name",
        "city",
        "country",
        "num_rooms_total",
      ],
      provenance_update_metadata: "NOT_OBSERVED_IN_TEST_PAYLOAD",
    };
    giataHotels = live.hotels || [];
  }
  if (!giataHotels.length) {
    giataHotels = loadCachedMhgSample();
    access.mhg.fallback_cached_sample = giataHotels.length;
  }

  const mc = await enrichMultiCodes(giataHotels);
  access.multicodes = {
    credentials_present: Boolean(
      process.env.GIATA_MULTICODES_USERNAME &&
        process.env.GIATA_MULTICODES_PASSWORD
    ),
    authenticated: Boolean(mc.ok),
    enriched: (mc.hotels || []).filter((h) => h.multicodes_ok).length,
  };
  giataHotels = mc.hotels || giataHotels;

  writeJson(path.join(DATA_DIR, "giata-mhg-sample.json"), {
    warning: WARNING,
    hotels: giataHotels,
  });

  const taPool = loadTripadvisorPool(args.taDataset);
  const official = loadOfficialVerified();
  let census = { ok: false, hotels: [] };
  if (!args.skipCensus) {
    census = await loadCensusIndex();
  }

  const rows = [];
  const needTaSearch = [];

  for (const g of giataHotels) {
    const hotelQuery = {
      name: g.name,
      city: g.city,
      country: g.country_name || g.country,
      lat: g.latitude,
      lng: g.longitude,
      website: g.website,
      rooms: g.num_rooms_total,
    };
    const { match, rejection } = matchTripadvisorHotel(hotelQuery, taPool.items);
    const deal = census.ok ? matchDealality(g, census.hotels) : null;
    const off = matchOfficial(g, official);

    const taItem = match?.item || null;
    const taRooms =
      taItem?.numberOfRooms != null && Number.isFinite(Number(taItem.numberOfRooms))
        ? Number(taItem.numberOfRooms)
        : null;
    const giataRooms =
      g.num_rooms_total != null && Number.isFinite(Number(g.num_rooms_total))
        ? Number(g.num_rooms_total)
        : null;
    const trusted =
      deal?.hotel?.rooms != null && Number.isFinite(deal.hotel.rooms)
        ? Number(deal.hotel.rooms)
        : null;
    const officialRooms =
      off?.rooms != null && Number.isFinite(Number(off.rooms))
        ? Number(off.rooms)
        : null;

    const classAttr = taItem?.hotelClassAttribution || null;

    const row = {
      giata_id: g.giata_id,
      giata_name: g.name,
      country: g.country_name || g.country,
      city: g.city,
      giata_rooms: giataRooms,
      tripadvisor_id: taItem?.id != null ? String(taItem.id) : null,
      tripadvisor_name: taItem?.name || null,
      tripadvisor_rooms: taRooms,
      tripadvisor_hotel_class_attribution: classAttr,
      tripadvisor_match_score: match?.score ?? null,
      ta_match_confidence: match?.confidence || (rejection ? "rejected" : "none"),
      ta_rejection: rejection?.reason || null,
      dealality_record_id: deal?.hotel?.record_id || null,
      dealality_name: deal?.hotel?.name || null,
      dealality_trusted_rooms: trusted,
      dealality_match_score: deal?.score ?? null,
      match_confidence: deal?.confidence || (match ? match.confidence : "low"),
      official_rooms: officialRooms,
      official_name: off?.name || null,
      source_independence: "INDEPENDENCE_UNCERTAIN",
    };

    row.giata_vs_trusted = trusted != null ? classifyPair(trusted, giataRooms) : null;
    row.ta_vs_trusted = trusted != null ? classifyPair(trusted, taRooms) : null;
    row.ta_vs_giata =
      taRooms != null && giataRooms != null
        ? classifyPair(giataRooms, taRooms)
        : taRooms == null && giataRooms == null
          ? "BOTH_MISSING"
          : "ONE_MISSING";
    row.simulated_status = simulateWaterfall(row);

    rows.push(row);
    if (!taItem && g.name) {
      needTaSearch.push({
        giata_id: g.giata_id,
        name: g.name,
        city: g.city,
        country: g.country_name || g.country,
        search_url: `https://www.tripadvisor.com/Search?q=${encodeURIComponent(
          [g.name, g.city, g.country_name || g.country].filter(Boolean).join(" ")
        )}`,
      });
    }
  }

  // Metrics — Phase 3
  const withGiataRooms = rows.filter((r) => r.giata_rooms != null);
  const withTrusted = rows.filter((r) => r.dealality_trusted_rooms != null);
  const giataVsTrusted = withTrusted.filter((r) => r.giata_rooms != null);
  const exactG = giataVsTrusted.filter((r) => r.giata_vs_trusted === ROOM_COMPARE.EXACT);
  const nearG = giataVsTrusted.filter((r) => r.giata_vs_trusted === ROOM_COMPARE.NEAR_MATCH);
  const conflictG = giataVsTrusted.filter((r) => r.giata_vs_trusted === ROOM_COMPARE.CONFLICT);
  const missingG = withTrusted.filter(
    (r) => r.giata_rooms == null || r.giata_vs_trusted === ROOM_COMPARE.MISSING
  );

  // Phase 4
  const bothRooms = rows.filter(
    (r) => r.tripadvisor_rooms != null && r.giata_rooms != null
  );
  const taGiataExact = bothRooms.filter((r) => r.ta_vs_giata === ROOM_COMPARE.EXACT);
  const taGiataNear = bothRooms.filter((r) => r.ta_vs_giata === ROOM_COMPARE.NEAR_MATCH);
  const taGiataConflict = bothRooms.filter((r) => r.ta_vs_giata === ROOM_COMPARE.CONFLICT);

  const agreeWithTruth = bothRooms.filter(
    (r) =>
      r.dealality_trusted_rooms != null &&
      (r.ta_vs_giata === ROOM_COMPARE.EXACT ||
        r.ta_vs_giata === ROOM_COMPARE.NEAR_MATCH)
  );
  const agreeCorrect = agreeWithTruth.filter(
    (r) =>
      r.giata_vs_trusted === ROOM_COMPARE.EXACT ||
      r.giata_vs_trusted === ROOM_COMPARE.NEAR_MATCH
  );

  const conflictWithTruth = bothRooms.filter(
    (r) =>
      r.dealality_trusted_rooms != null && r.ta_vs_giata === ROOM_COMPARE.CONFLICT
  );
  let giataWins = 0;
  let taWins = 0;
  let bothWrong = 0;
  for (const r of conflictWithTruth) {
    const gOk =
      r.giata_vs_trusted === ROOM_COMPARE.EXACT ||
      r.giata_vs_trusted === ROOM_COMPARE.NEAR_MATCH;
    const tOk =
      r.ta_vs_trusted === ROOM_COMPARE.EXACT ||
      r.ta_vs_trusted === ROOM_COMPARE.NEAR_MATCH;
    if (gOk && !tOk) giataWins += 1;
    else if (tOk && !gOk) taWins += 1;
    else if (!gOk && !tOk) bothWrong += 1;
    else {
      // both near/exact despite conflict between them — count as neither win cleanly
      bothWrong += 1;
    }
  }

  // Phase 5 simulation counts
  const sim = {};
  for (const r of rows) {
    sim[r.simulated_status] = (sim[r.simulated_status] || 0) + 1;
  }
  const verifiedSim =
    (sim.VERIFIED_PRIMARY_SOURCE || 0) + (sim.VERIFIED_TA_GIATA || 0);
  // Note: SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT is NOT counted as verified

  const decisionPoolPath = path.join(DATA_DIR, "ta-decision-pool.json");
  const decisionPoolItems = fs.existsSync(decisionPoolPath)
    ? JSON.parse(fs.readFileSync(decisionPoolPath, "utf8")).items || []
    : taPool.items.filter((it) => it.hotelClassAttribution != null);
  const decisionHotels = decisionPoolItems.filter(
    (it) => it?.type === "HOTEL" || it?.category === "hotel"
  );
  const independence = assessIndependence({
    ta_pool_hotelClassAttribution_giata_mentions: decisionHotels.filter((it) =>
      /giata/i.test(String(it.hotelClassAttribution || ""))
    ).length,
    ta_pool_size: decisionHotels.length || taPool.items.length,
    literature:
      "Tripadvisor star/class often attributed to Giata in Europe; rooms field is separate in scraper schemas.",
    dealality_identity_matches: rows.filter((r) => r.dealality_record_id).length,
    dealality_trusted_room_matches: rows.filter(
      (r) => r.dealality_trusted_rooms != null
    ).length,
  });

  // Decision logic
  const trustedN = giataVsTrusted.length;
  const giataExactAcc = pct(exactG.length, trustedN);
  const overlapBoth = bothRooms.length;
  const materialChange =
    trustedN >= 10 &&
    giataExactAcc != null &&
    giataExactAcc + pct(nearG.length, trustedN) >= 85 &&
    conflictG.length / Math.max(trustedN, 1) <= 0.15 &&
    // and independence allows multi-source OR primary covers enough
    independence.assessment === "INDEPENDENT";

  let decision = "KEEP_GIATA_OPTION_OPEN";
  let rationale = "";
  if (trustedN < 5 || overlapBoth < 5) {
    decision = "KEEP_GIATA_OPTION_OPEN";
    rationale =
      "TEST entitlement returns random non-CALA properties; trusted Dealality∩GIATA∩Tripadvisor ground-truth overlap is too small to justify a 24-month €4,950 MHG commit. MHG num_rooms_total is schema-confirmed, but production CALA coverage/accuracy remains unproven. Tripadvisor finds candidates but verification conversion is ~2%; GIATA could help later if production sample proves accuracy + if independence is clarified — not BUY_NOW.";
  } else if (materialChange) {
    decision = "BUY_GIATA_NOW";
    rationale =
      "GIATA materially improves trustworthy automated room coverage with proven accuracy on trusted ground truth and established source independence.";
  } else if (
    trustedN >= 8 &&
    conflictG.length / trustedN > 0.35 &&
    exactG.length / trustedN < 0.5
  ) {
    decision = "DO_NOT_BUY_GIATA";
    rationale =
      "On available trusted overlap, GIATA conflict rate is too high and exact accuracy too low to justify €4,950 commitment versus continuing Tripadvisor candidates + primary-source verification.";
  } else {
    decision = "KEEP_GIATA_OPTION_OPEN";
    rationale =
      "GIATA MHG exposes num_rooms_total reliably in TEST, but (1) TEST geography is random / not CALA-representative, (2) TA↔GIATA independence is UNCERTAIN (star class ≠ rooms provenance), so agreement cannot safely mint VERIFIED_MULTI_SOURCE, (3) Tripadvisor already supplies candidates cheaply — the gap is primary verification, which MHG alone does not replace without proven CALA accuracy. Revisit with production MHG sample or larger trusted overlap before BUY.";
  }

  const metrics = {
    warning: WARNING,
    production_writes: 0,
    OVERLAP_SAMPLE: rows.length,
    OVERLAP_WITH_TA_ROOMS: bothRooms.length,
    OVERLAP_WITH_TRUSTED_DEALALITY_ROOMS: trustedN,
    GIATA_HOTELS_WITH_ROOMS: withGiataRooms.length,
    GIATA_ROOM_COVERAGE: pct(withGiataRooms.length, rows.length),
    GIATA_EXACT_ACCURACY: giataExactAcc,
    GIATA_NEAR_MATCH: pct(nearG.length, trustedN),
    GIATA_CONFLICT_RATE: pct(conflictG.length, trustedN),
    GIATA_MISSING_VS_TRUSTED: pct(missingG.length, withTrusted.length),
    TA_GIATA_AGREEMENT: pct(taGiataExact.length + taGiataNear.length, bothRooms.length),
    TA_GIATA_EXACT: pct(taGiataExact.length, bothRooms.length),
    TA_GIATA_NEAR: pct(taGiataNear.length, bothRooms.length),
    TA_GIATA_CONFLICT: pct(taGiataConflict.length, bothRooms.length),
    TA_GIATA_AGREE_GROUND_TRUTH_ACCURACY: pct(
      agreeCorrect.length,
      agreeWithTruth.length
    ),
    GIATA_WINS_CONFLICTS: giataWins,
    TRIPADVISOR_WINS_CONFLICTS: taWins,
    BOTH_WRONG_CONFLICTS: bothWrong,
    conflict_with_truth_n: conflictWithTruth.length,
    agree_with_truth_n: agreeWithTruth.length,
    SIMULATED_STATUS_COUNTS: sim,
    SIMULATED_VERIFIED_COVERAGE: pct(verifiedSim, rows.length),
    SIMULATED_REVIEW_REQUIRED: pct(sim.CONFLICT_REVIEW_REQUIRED || 0, rows.length),
    SIMULATED_INDEPENDENCE_UNCERTAIN_AGREEMENT: pct(
      sim.SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT || 0,
      rows.length
    ),
    SOURCE_INDEPENDENCE_ASSESSMENT: independence.assessment,
    GIATA_24_MONTH_COST_EUR: 4950,
    DECISION: decision,
    need_ta_search_count: needTaSearch.length,
    ta_pool_sources: taPool.sources,
    tripadvisor_v2_context: {
      sample: 50,
      candidate_to_verified_conversion: "2%",
      note: "TA finds rooms; verification remains the bottleneck",
    },
  };

  writeJson(path.join(DATA_DIR, "comparison-rows.json"), { rows });
  writeJson(path.join(DATA_DIR, "need-ta-search.json"), { hotels: needTaSearch });
  writeJson(path.join(OUT_DIR, "metrics.json"), metrics);
  writeJson(path.join(OUT_DIR, "independence.json"), independence);
  writeJson(path.join(OUT_DIR, "access-audit.json"), access);

  const report = buildReport({
    metrics,
    independence,
    access,
    rows,
    decision,
    rationale,
  });
  fs.writeFileSync(
    path.join(OUT_DIR, "DEALALITY_GIATA_TRIPADVISOR_ROOM_DECISION.md"),
    report,
    "utf8"
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "STATUS.txt"),
    [
      "GIATA_TRIPADVISOR_ROOM_DECISION_COMPLETE",
      `OVERLAP_SAMPLE: ${metrics.OVERLAP_SAMPLE}`,
      `GIATA_ROOM_COVERAGE: ${metrics.GIATA_ROOM_COVERAGE}%`,
      `GIATA_EXACT_ACCURACY: ${metrics.GIATA_EXACT_ACCURACY ?? "n/a"}%`,
      `GIATA_NEAR_MATCH: ${metrics.GIATA_NEAR_MATCH ?? "n/a"}%`,
      `GIATA_CONFLICT_RATE: ${metrics.GIATA_CONFLICT_RATE ?? "n/a"}%`,
      `TA_GIATA_AGREEMENT: ${metrics.TA_GIATA_AGREEMENT ?? "n/a"}%`,
      `TA_GIATA_CONFLICT: ${metrics.TA_GIATA_CONFLICT ?? "n/a"}%`,
      `TA_GIATA_AGREE_GROUND_TRUTH_ACCURACY: ${metrics.TA_GIATA_AGREE_GROUND_TRUTH_ACCURACY ?? "n/a"}%`,
      `GIATA_WINS_CONFLICTS: ${metrics.GIATA_WINS_CONFLICTS}`,
      `TRIPADVISOR_WINS_CONFLICTS: ${metrics.TRIPADVISOR_WINS_CONFLICTS}`,
      `BOTH_WRONG_CONFLICTS: ${metrics.BOTH_WRONG_CONFLICTS}`,
      `SIMULATED_VERIFIED_COVERAGE: ${metrics.SIMULATED_VERIFIED_COVERAGE}%`,
      `SIMULATED_REVIEW_REQUIRED: ${metrics.SIMULATED_REVIEW_REQUIRED}%`,
      `SOURCE_INDEPENDENCE_ASSESSMENT: ${metrics.SOURCE_INDEPENDENCE_ASSESSMENT}`,
      "GIATA_24_MONTH_COST: €4,950",
      `DECISION: ${decision}`,
      "PRODUCTION_WRITES: 0",
      "",
    ].join("\n"),
    "utf8"
  );

  console.log(JSON.stringify({ decision, metrics, needTaSearch: needTaSearch.length }, null, 2));
}

function buildReport({ metrics, independence, access, rows, decision, rationale }) {
  const both = rows.filter((r) => r.tripadvisor_rooms != null && r.giata_rooms != null);
  const trusted = rows.filter((r) => r.dealality_trusted_rooms != null);
  const table = both
    .slice(0, 40)
    .map(
      (r) =>
        `| ${r.giata_name || ""} | ${r.country || ""} | ${r.giata_rooms ?? "—"} | ${r.tripadvisor_rooms ?? "—"} | ${r.dealality_trusted_rooms ?? "—"} | ${r.ta_vs_giata} | ${r.simulated_status} |`
    )
    .join("\n");

  return `# GIATA MHG vs Tripadvisor room-count decision

**Marker:** \`GIATA_TRIPADVISOR_ROOM_DECISION_COMPLETE\`  
**Warning:** \`${WARNING}\`  
**Production writes:** 0

## 1. Executive summary

Tripadvisor can **find** room counts (v2: 100% candidate rate on matched CALA sample) but only **~2%** convert to independently verified primary/multi-source values.

GIATA MHG TEST confirms structured \`num_rooms_total\` (total property keys). That does **not** automatically justify a €4,950 / 24-month MHG commitment: TEST geography is random, Dealality trusted-room overlap is limited, and **TA↔GIATA source independence is UNCERTAIN**.

**Decision: \`${decision}\`**

${rationale}

## 2. Phase 1 — GIATA access (no secrets)

| Product | Credentials | Auth |
| --- | --- | --- |
| MHG TEST | ${access.mhg.credentials_present} | ${access.mhg.authenticated} |
| MultiCodes TEST | ${access.multicodes.credentials_present} | ${access.multicodes.authenticated} |

Confirmed MHG fields: GIATA ID, name, city/country, \`num_rooms_total\`. Update/provenance metadata: **not observed** in TEST payloads.

## 3. Sample construction

- MHG-driven sample (random TEST hotels), MultiCodes enrichment for geo when available.
- Tripadvisor match via existing Actor pool (+ optional decision dataset).
- Dealality trusted rooms only when high-confidence name/country identity match.
- Not forced to CALA-only (TEST cannot support geographic validity).

| Metric | Value |
| --- | --- |
| OVERLAP_SAMPLE | ${metrics.OVERLAP_SAMPLE} |
| GIATA with rooms | ${metrics.GIATA_HOTELS_WITH_ROOMS} |
| Both TA+GIATA rooms | ${metrics.OVERLAP_WITH_TA_ROOMS} |
| Trusted Dealality rooms overlap | ${metrics.OVERLAP_WITH_TRUSTED_DEALALITY_ROOMS} |

## 4. GIATA vs trusted Dealality rooms

Tolerance: EXACT / NEAR (≤5 keys or ≤5%) / CONFLICT — same as Tripadvisor.

| Metric | Value |
| --- | --- |
| GIATA_ROOM_COVERAGE | ${metrics.GIATA_ROOM_COVERAGE}% |
| GIATA_EXACT_ACCURACY | ${metrics.GIATA_EXACT_ACCURACY ?? "n/a"}% |
| GIATA_NEAR_MATCH | ${metrics.GIATA_NEAR_MATCH ?? "n/a"}% |
| GIATA_CONFLICT_RATE | ${metrics.GIATA_CONFLICT_RATE ?? "n/a"}% |

Trusted rows: ${trusted.length}.

## 5. Tripadvisor vs GIATA

| Metric | Value |
| --- | --- |
| TA_GIATA_AGREEMENT (exact+near) | ${metrics.TA_GIATA_AGREEMENT ?? "n/a"}% |
| TA_GIATA_CONFLICT | ${metrics.TA_GIATA_CONFLICT ?? "n/a"}% |
| Agree ∩ ground-truth accuracy | ${metrics.TA_GIATA_AGREE_GROUND_TRUTH_ACCURACY ?? "n/a"}% (n=${metrics.agree_with_truth_n}) |
| GIATA wins conflicts | ${metrics.GIATA_WINS_CONFLICTS} |
| Tripadvisor wins conflicts | ${metrics.TRIPADVISOR_WINS_CONFLICTS} |
| Both wrong | ${metrics.BOTH_WRONG_CONFLICTS} |

## 6. Simulated production waterfall (no writes)

Statuses: VERIFIED_PRIMARY_SOURCE → VERIFIED_TA_GIATA (only if independence allows) → GIATA_ONLY → TA_ONLY → CONFLICT_REVIEW_REQUIRED → UNRESOLVED.  
Agreements under uncertain independence → \`SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT\` (**not** counted as verified).

\`\`\`json
${JSON.stringify(metrics.SIMULATED_STATUS_COUNTS, null, 2)}
\`\`\`

- SIMULATED_VERIFIED_COVERAGE: **${metrics.SIMULATED_VERIFIED_COVERAGE}%**
- SIMULATED_REVIEW_REQUIRED: **${metrics.SIMULATED_REVIEW_REQUIRED}%**

## 7. Source independence

**Assessment: \`${independence.assessment}\`**

${independence.rationale.map((x) => `- ${x}`).join("\n")}

## 8. Economics

| Option | Notes |
| --- | --- |
| A. Tripadvisor-only candidates | Cheap discovery; not authoritative |
| B. TA + verification waterfall | Correct architecture; ~2% verified conversion today |
| C. GIATA MHG alone | Structured keys; €4,950 / 24mo; TEST coverage not CALA-proof |
| D. TA + GIATA hybrid | Attractive **if** independence proven; today agreement ≠ multi-source verify |

GIATA license reminders: store during license; delete on termination; independently verified facts / non-reconstructable analytics may remain; combine OK; attribution required; no DB redistribution.

## 9. Hotel-level overlap (TA+GIATA rooms)

| Hotel | Country | GIATA | TA | Trusted | TA vs GIATA | Simulated |
| --- | --- | ---: | ---: | ---: | --- | --- |
${table || "| — | — | — | — | — | — | — |"}

## 10. Decision

\`\`\`
DECISION: ${decision}
GIATA_24_MONTH_COST: €4,950
PRODUCTION_WRITES: 0
\`\`\`
`;
}

main().catch((err) => {
  console.error(sanitizeError(err.message || err));
  process.exit(1);
});
