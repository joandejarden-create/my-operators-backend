/**
 * Independent geographic gap discovery wave v1
 *
 * Uses aggregate benchmark priorities ONLY to decide WHERE to search.
 * Never imports/persists benchmark hotel identities.
 *
 * Sources: HBX → SerpAPI Google Hotels → existing Cvent/HBX candidate universe.
 * Writes: Hotel Property Census only (Core Identity shells).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";

import {
  listDealalityCalaGeographies,
  resolveDealalityCalaGeography,
  normalizeGeographyLabel,
} from "./dealality-cala-geography-registry-v1.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { resolveHbxConfig, hbxFetchJson, contentUrl } from "./hbx-content-api-client.js";
import { createHbxRequestRateLimiter } from "./hbx-request-rate-limiter-v1.js";
import {
  extractHbxHotel,
  pullCountryHotels,
} from "./hbx-content-api-cala-wave1-dry-run-v1.js";
import { HBX_ALTERNATE_QUERY_CODES } from "./full-cala-hbx-geography-discovery-wave-v1.js";
import { searchGoogleHotels } from "./providers/serpapi-google-hotels/search.js";
import { SerpApiCreditTracker } from "./providers/serpapi-google-hotels/credit-tracker.js";
import {
  CENSUS_TABLE_ID,
  MATCH,
  SHELL_PREFLIGHT_CLASS,
  classifyAgainstCensus,
  classifyShellPreflightQuality,
  listCensusIndex,
  buildShellFields,
  insertBatch,
  loadMasterUniverseCandidates,
  loadHbxCandidates,
  mergeCandidateUniverses,
} from "./full-cala-15k-census-shell-insert-v1.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import { assertNoProtectedShellFields } from "./full-cala-15k-shell-orchestrator-v1.js";
import { runFullCalaGeographyCoverageRegistryAuditV1 } from "./full-cala-geography-coverage-registry-audit-v1.js";
import { isDirtyStateRegionValue } from "./census-city-to-state-map.js";
import {
  STATE_REGION_NOT_APPLICABLE,
} from "./full-cala-core-identity-foundation-closure-v1.js";
import { resolveStateRegionFromCity } from "./census-city-to-state-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const GAP_DISCOVERY_OBJECTIVE =
  "independent-geographic-gap-discovery-wave-v1";
export const GAP_DISCOVERY_VERSION =
  "independent-geographic-gap-discovery-wave-v1";

const PRIORITY_FP = path.join(
  ROOT,
  "reports/research-engine-v2/cala-geography-discovery-priority-from-benchmark.json"
);
const MATRIX_FP = path.join(
  ROOT,
  "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.json"
);
const APPLIED_FP = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/applied-index.json"
);
const HOLDS_FP = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
);
const STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/independent-geographic-gap-discovery"
);
const MERGED_PACK = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-full-geography-candidate-pack.json"
);

/** Force-include even if benchmark aggregate is empty. */
const ALWAYS_INCLUDE = [
  "Cuba",
  "Saint Martin",
  "Sint Maarten",
  "Bonaire",
  "Montserrat",
  "Sint Eustatius",
  "Saba",
];

/** Destination seeds used only when artifact has few destinations for a geo. */
const SEED_DESTINATIONS = Object.freeze({
  Cuba: ["Havana", "Varadero", "Santiago de Cuba", "Cayo Coco", "Cayo Santa Maria", "Holguin"],
  "Saint Martin": ["Marigot", "Orient Bay", "Grand Case"],
  "Sint Maarten": ["Philipsburg", "Simpson Bay", "Maho"],
  Bonaire: ["Kralendijk"],
  Montserrat: ["Brades", "Plymouth"],
  "Sint Eustatius": ["Oranjestad"],
  Saba: ["The Bottom", "Windwardside"],
  Belize: ["Belize City", "San Pedro", "Caye Caulker", "Placencia", "San Ignacio"],
  "Puerto Rico": ["San Juan", "Condado", "Isla Verde", "Dorado", "Ponce", "Rincon", "Vieques", "Culebra"],
  Guatemala: ["Guatemala City", "Antigua Guatemala", "Panajachel", "Flores"],
  Nicaragua: ["Managua", "Granada", "San Juan del Sur", "Leon"],
  "El Salvador": ["San Salvador", "El Tunco", "Suchitoto"],
  "Cayman Islands": ["George Town", "Seven Mile Beach", "West Bay"],
  Honduras: ["Tegucigalpa", "San Pedro Sula", "Roatan", "Utila"],
  Curaçao: ["Willemstad", "Jan Thiel", "Westpunt"],
  Grenada: ["St. George's", "Grand Anse"],
  "Saint Kitts and Nevis": ["Basseterre", "Frigate Bay", "Charlestown"],
});

const NON_HOTEL_RE =
  /\b(vacation rental|airbnb|vrbo|condo|condominium|apartment only|hostel|campground|campsite|restaurant|museum|attraction)\b/i;
const HOTEL_LIKE_RE =
  /\b(hotel|resort|inn|lodge|suites?|marriott|hilton|hyatt|ihg|radisson|accor|meli[aá]|barcel[oó]|ri[uo]|iberostar)\b/i;

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || String(v).trim() === "";
}
function hashId(parts) {
  return crypto.createHash("sha1").update(parts.filter(Boolean).join("|")).digest("hex").slice(0, 16);
}

function isStateApplicable(country) {
  const c = String(country || "").trim();
  if (!c) return false;
  if (STATE_REGION_NOT_APPLICABLE.has(c)) return false;
  const g = resolveDealalityCalaGeography(c);
  if (g && STATE_REGION_NOT_APPLICABLE.has(g.name)) return false;
  return true;
}

function attributeBesGeography(hotel) {
  const blob = [hotel.city, hotel.destination, hotel.zone, hotel.name, hotel.address]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");
  if (/eustati|statia/.test(blob)) return "Sint Eustatius";
  if (/\bsaba\b/.test(blob)) return "Saba";
  if (/bonaire|kralendijk/.test(blob)) return "Bonaire";
  return "Bonaire";
}

function hbxCodesForGeography(g) {
  const alts = HBX_ALTERNATE_QUERY_CODES[g.geography_id] || [];
  const primary = g.iso_code ? [g.iso_code] : [];
  const out = [];
  for (const c of [...primary, ...alts]) {
    const up = String(c || "").toUpperCase();
    if (up && !out.includes(up)) out.push(up);
  }
  return out;
}

export function buildGapDiscoveryQueue({ priorityDoc, matrixDoc }) {
  const matrixByGeo = new Map((matrixDoc?.matrix || []).map((r) => [r.geography, r]));
  const byName = new Map();

  for (const row of priorityDoc?.GEOGRAPHY_DISCOVERY_PRIORITY || []) {
    byName.set(row.geography, {
      geography: row.geography,
      priority_score: row.priority_score || 0,
      gap_class: row.gap_class || null,
      BENCHMARK_COUNT: row.BENCHMARK_COUNT ?? null,
      dealality_census_count: row.dealality_census_count ?? null,
      dealality_coverage_status: row.dealality_coverage_status || null,
      active_holds: row.active_holds || 0,
      tourism_priority: row.tourism_priority || null,
      from_benchmark_priority: true,
    });
  }

  for (const name of ALWAYS_INCLUDE) {
    if (!byName.has(name)) {
      const m = matrixByGeo.get(name) || {};
      byName.set(name, {
        geography: name,
        priority_score: 1200,
        gap_class: "ZERO_OR_NEAR_ZERO_FORCE",
        BENCHMARK_COUNT: null,
        dealality_census_count: m.census_after ?? 0,
        dealality_coverage_status: m.coverage_status || null,
        active_holds: m.active_holds || 0,
        tourism_priority: null,
        from_benchmark_priority: false,
      });
    } else {
      byName.get(name).priority_score = Math.max(byName.get(name).priority_score, 1100);
    }
  }

  // Destinations from artifact (city/destination labels only — not hotel names)
  const destByGeo = new Map();
  for (const d of priorityDoc?.CITY_DESTINATION_DISCOVERY_PRIORITY || []) {
    if (!destByGeo.has(d.geography)) destByGeo.set(d.geography, []);
    destByGeo.get(d.geography).push({
      city_or_destination: d.city_or_destination,
      priority_score: d.priority_score || 0,
    });
  }

  const queue = [...byName.values()].sort(
    (a, b) => (b.priority_score || 0) - (a.priority_score || 0)
  );

  for (const row of queue) {
    const fromArt = (destByGeo.get(row.geography) || [])
      .sort((a, b) => b.priority_score - a.priority_score)
      .map((x) => x.city_or_destination);
    const seeds = SEED_DESTINATIONS[row.geography] || [];
    row.destinations = [...new Set([...fromArt, ...seeds])].slice(0, 8);
  }

  return queue;
}

function isInScopeHotelName(name) {
  const n = String(name || "");
  if (!n.trim()) return false;
  if (NON_HOTEL_RE.test(n) && !HOTEL_LIKE_RE.test(n)) return false;
  return true;
}

function toHbxCandidateRow(hotel, country) {
  return {
    candidate_id: `hbx_${hotel.hbx_hotel_code}`,
    property_name: hotel.name,
    normalized_property_name: normName(hotel.name),
    country,
    city: hotel.city || null,
    address: hotel.address || null,
    website: hotel.website || null,
    phone: hotel.phonehotel || null,
    source_name: "hbx_content_api",
    source_type: "hbx_content_api",
    external_ids: { hbx_code: hotel.hbx_hotel_code },
    chain_text: hotel.chain_code || null,
    hbx_category_code: hotel.category || null,
    confidence: "high",
    merged_sources: ["hbx_content_api"],
    discovery_notes: GAP_DISCOVERY_VERSION,
  };
}

function toSerpCandidateRow(hit, country, destination) {
  const name = hit.name || hit.property_name;
  const city =
    hit.city ||
    destination ||
    (typeof hit.address === "string"
      ? hit.address.split(",").slice(-2, -1)[0]?.trim()
      : null);
  const id = `serp_${hashId([normName(name), country, city, hit.property_token || hit.google_property_url])}`;
  return {
    candidate_id: id,
    property_name: name,
    normalized_property_name: normName(name),
    country,
    city: city || null,
    address: hit.address || null,
    website: hit.google_property_url || hit.website || null,
    phone: hit.phone || null,
    source_name: "serpapi_google_hotels",
    source_type: "serpapi_google_hotels",
    external_ids: {
      serpapi_property_token: hit.property_token || null,
    },
    confidence: "medium",
    merged_sources: ["serpapi_google_hotels"],
    discovery_notes: `${GAP_DISCOVERY_VERSION}|dest=${destination || ""}`,
  };
}

function appendHbxPack(hotels) {
  const existing = readJson(MERGED_PACK, { candidates: [] });
  const byCode = new Map(
    (existing.candidates || [])
      .filter((c) => c.hbx_hotel_code != null)
      .map((c) => [Number(c.hbx_hotel_code), c])
  );
  let added = 0;
  for (const h of hotels) {
    if (h.hbx_hotel_code == null) continue;
    const n = Number(h.hbx_hotel_code);
    if (byCode.has(n)) continue;
    byCode.set(n, {
      hbx_hotel_code: n,
      name: h.name,
      country: h.country,
      city: h.city,
      address: h.address,
      website: h.website,
      phonehotel: h.phonehotel,
      chain_code: h.chain_code,
      category: h.category,
      latitude: null,
      longitude: null,
      discovery_wave: GAP_DISCOVERY_VERSION,
    });
    added += 1;
  }
  writeJson(MERGED_PACK, {
    objective: GAP_DISCOVERY_OBJECTIVE,
    generated_at: new Date().toISOString(),
    count: byCode.size,
    candidates: [...byCode.values()],
  });
  return added;
}

async function completenessSnapshot(baseId, token) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of ["Property Name", "Country", "City", "State / Region"]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census_snap_failed:${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset;
    await sleep(100);
  } while (offset);

  let city = 0;
  let stateApp = 0;
  let stateFilled = 0;
  const byCountry = {};
  for (const r of records) {
    const f = r.fields || {};
    const c = String(f.Country || "").trim() || "UNK";
    byCountry[c] = (byCountry[c] || 0) + 1;
    if (!isBlank(f.City)) city += 1;
    if (isStateApplicable(c)) {
      stateApp += 1;
      if (!isBlank(f["State / Region"]) && !isDirtyStateRegionValue(f["State / Region"])) {
        stateFilled += 1;
      }
    }
  }
  const n = records.length || 1;
  return {
    count: records.length,
    city_pct: Math.round((100 * city) / n),
    state_applicable: stateApp,
    state_pct_applicable: stateApp ? Math.round((100 * stateFilled) / stateApp) : 100,
    byCountry,
    zeroGeographies: listDealalityCalaGeographies({ inScopeOnly: true })
      .filter((g) => !(byCountry[g.name] > 0))
      .map((g) => g.name),
  };
}

async function prepareAndInsert(candidates, { index, baseId, token, log, applied, batchLabel }) {
  const tallies = {
    discovered: candidates.length,
    existing: 0,
    duplicate: 0,
    invalid: 0,
    hold: 0,
    inserted: 0,
  };
  const prepared = [];
  const appliedHbx = new Set((applied.hbx_codes || []).map(Number));
  const appliedCand = new Set(applied.candidate_ids || []);

  for (const c of candidates) {
    if (!isInScopeHotelName(c.property_name)) {
      tallies.invalid += 1;
      continue;
    }
    if (appliedCand.has(c.candidate_id)) {
      tallies.existing += 1;
      continue;
    }
    const hbxCode =
      c.external_ids?.hbx_code != null ? Number(c.external_ids.hbx_code) : null;
    if (hbxCode != null && (appliedHbx.has(hbxCode) || index.byHbx?.has?.(hbxCode))) {
      tallies.existing += 1;
      continue;
    }

    const cls = classifyAgainstCensus(c, index);
    c.match_class = cls.match_class;
    if (cls.match_class === MATCH.EXISTING_HIGH || cls.match_class === MATCH.EXISTING_MEDIUM) {
      tallies.existing += 1;
      continue;
    }
    if (cls.match_class === MATCH.PROBABLE_DUP) {
      tallies.duplicate += 1;
      continue;
    }
    if (cls.match_class === MATCH.REJECT_NON_HOTEL || cls.match_class === MATCH.REJECT_IDENTITY) {
      tallies.invalid += 1;
      continue;
    }

    const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
    const hasHbx = hbxCode != null;
    const serpSafe =
      c.source_type === "serpapi_google_hotels" &&
      !isBlank(c.property_name) &&
      !isBlank(c.country) &&
      !isBlank(c.city) &&
      (HOTEL_LIKE_RE.test(c.property_name) || (c.address && c.city));

    const allow =
      pf.class === SHELL_PREFLIGHT_CLASS.SAFE ||
      (serpSafe && pf.class !== SHELL_PREFLIGHT_CLASS.NON_HOTEL) ||
      (hasHbx && pf.class === SHELL_PREFLIGHT_CLASS.SAFE);

    if (!allow) {
      if (
        pf.class === SHELL_PREFLIGHT_CLASS.WEAK ||
        pf.class === SHELL_PREFLIGHT_CLASS.REVIEW ||
        pf.class === SHELL_PREFLIGHT_CLASS.INSUFFICIENT
      ) {
        tallies.hold += 1;
      } else {
        tallies.invalid += 1;
      }
      continue;
    }

    try {
      const fieldsBuilt = buildShellFields(c, [], { countryBatchLabel: batchLabel });
      if (!fieldsBuilt.validation.pass) {
        tallies.hold += 1;
        continue;
      }
      assertNoProtectedShellFields(fieldsBuilt.fields);
      if (
        fieldsBuilt.fields["Current Brand"] != null ||
        fieldsBuilt.fields["Brand Family"] != null
      ) {
        throw new Error("protected_brand_field_proposed");
      }
      // SerpAPI / independent: allow phone when present
      if (!fieldsBuilt.fields.Phone && c.phone) {
        fieldsBuilt.fields.Phone = String(c.phone).trim();
      }
      // State/Region when deterministically mapped
      if (!fieldsBuilt.fields["State / Region"] && c.city && isStateApplicable(c.country)) {
        const st = resolveStateRegionFromCity({
          city: c.city,
          country: c.country,
          state: null,
        });
        if (st.ok && st.state) fieldsBuilt.fields["State / Region"] = st.state;
      }
      fieldsBuilt.fields["Shell Insert Batch ID"] = GAP_DISCOVERY_VERSION;
      fieldsBuilt.fields["Discovery Source"] =
        c.source_type === "serpapi_google_hotels"
          ? "SerpAPI Google Hotels"
          : fieldsBuilt.fields["Discovery Source"];

      prepared.push({
        candidate_id: c.candidate_id,
        property_name: c.property_name,
        hbx_hotel_code: hbxCode,
        source_type: c.source_type,
        city: c.city,
        fields: fieldsBuilt.fields,
        is_hbx: hasHbx,
        is_cvent: false,
      });
    } catch (err) {
      log(`[gap] prepare skip ${c.candidate_id}: ${String(err?.message || err).slice(0, 120)}`);
      tallies.hold += 1;
    }
  }

  if (!prepared.length) return tallies;

  const result = await insertBatch(prepared, {
    baseId,
    token,
    tableId: CENSUS_TABLE_ID,
    log,
  });
  tallies.inserted = result.inserted || 0;

  for (const p of prepared) {
    appliedCand.add(p.candidate_id);
    if (p.hbx_hotel_code != null) appliedHbx.add(Number(p.hbx_hotel_code));
  }
  applied.hbx_codes = [...appliedHbx];
  applied.candidate_ids = [...appliedCand];
  applied.updated_at = new Date().toISOString();
  writeJson(APPLIED_FP, applied);

  // Refresh index lightly for subsequent geos
  for (const p of prepared) {
    if (p.hbx_hotel_code != null) index.byHbx?.set?.(Number(p.hbx_hotel_code), true);
    const key = `${normName(p.property_name)}|${normName(p.fields.Country)}`;
    if (!index.byNameCountry) index.byNameCountry = new Map();
    if (!index.byNameCountry.has(key)) index.byNameCountry.set(key, []);
    index.byNameCountry.get(key).push({ id: "new", fields: p.fields });
  }
  index.count = (index.count || 0) + tallies.inserted;

  return tallies;
}

export async function runIndependentGeographicGapDiscoveryWaveV1(opts = {}) {
  const log = opts.log || console.log;
  const mode = String(opts.mode || "dry-run").toLowerCase();
  const enableWrites = Boolean(
    opts.enableProductionWrites && (mode === "run" || mode === "resume")
  );
  const generated_at = new Date().toISOString();
  const hbxBudget = Math.max(0, Number(opts.hbxBudget ?? 10));
  const serpMax = Math.max(0, Number(opts.serpMax ?? 60));
  const maxGeographies = Math.max(1, Number(opts.maxGeographies ?? 17));

  try {
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
    if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
      throw new Error("wrong_production_table");
    }
  } catch (err) {
    return {
      ok: false,
      DISCOVERY_STATUS: "independent_geographic_gap_discovery_stop_for_founder_review",
      FOUNDER_DECISION_REQUIRED: "YES",
      FOUNDER_DECISION: String(err?.message || err),
      generated_at,
    };
  }

  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;

  const priorityDoc = readJson(PRIORITY_FP, { GEOGRAPHY_DISCOVERY_PRIORITY: [] });
  const matrixDoc = readJson(MATRIX_FP, { matrix: [] });
  const queue = buildGapDiscoveryQueue({ priorityDoc, matrixDoc }).slice(0, maxGeographies);

  writeJson(path.join(STATE_DIR, "discovery-queue.json"), {
    generated_at,
    note: "Geography/destination labels only. No benchmark hotel identities.",
    queue: queue.map((q) => ({
      geography: q.geography,
      priority_score: q.priority_score,
      gap_class: q.gap_class,
      destinations: q.destinations,
      BENCHMARK_COUNT: q.BENCHMARK_COUNT,
    })),
  });

  log(`[gap] completeness snapshot before…`);
  const before = await completenessSnapshot(baseId, token);
  const CENSUS_BEFORE = before.count;
  const ZERO_BEFORE = before.zeroGeographies;

  log(`[gap] indexing Census…`);
  let index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const applied = readJson(APPLIED_FP, { hbx_codes: [], candidate_ids: [] });

  const geoResults = [];
  let shells = 0;
  let existing = 0;
  let duplicates = 0;
  let invalids = 0;
  let discovered = 0;
  let hbxUsed = 0;
  let hbxPaused = false;
  let hbxNew = 0;
  let serpSearches = 0;
  let serpConfirmed = 0;
  const majorInvestigated = [];
  const majorImproved = [];

  // —— HBX ——
  const cfg = resolveHbxConfig(process.env);
  let limiter = null;
  if (cfg.ok && hbxBudget > 0) {
    const gate = await hbxFetchJson(
      contentUrl(cfg, "hotels?fields=code,name&language=ENG&from=1&to=1&useSecondaryLanguage=false"),
      cfg
    );
    hbxUsed += 1;
    log(`[gap] HBX gate ${gate.status} remaining=${gate.response_headers?.["x-ratelimit-remaining"]}`);
    if (gate.ok) {
      const remainingHdr = Number(gate.response_headers?.["x-ratelimit-remaining"] || hbxBudget);
      const budget = Math.min(hbxBudget, Math.max(0, remainingHdr - 2));
      limiter = createHbxRequestRateLimiter({
        minIntervalMs: 1200,
        maxRequestsPerRun: budget,
        maxRetriesOn429: 2,
      });
    } else if (/quota/i.test(String(gate.error_message || ""))) {
      hbxPaused = true;
      log(`[gap] HBX_PAUSED_QUOTA at gate`);
    }
  }

  const besPulled = { done: false, hotels: [] };

  for (const item of queue) {
    const g = resolveDealalityCalaGeography(item.geography);
    const geoStart = index.count;
    const result = {
      geography: item.geography,
      gap_class: item.gap_class,
      hbx_status: "NOT_RUN",
      serpapi_status: "NOT_RUN",
      destinations_searched: [],
      candidates_discovered: 0,
      existing_matches: 0,
      duplicates_skipped: 0,
      invalids: 0,
      new_shells: 0,
      census_before_geo: geoStart,
    };
    majorInvestigated.push(item.geography);

    // HBX pull
    if (limiter && !hbxPaused && g) {
      const isBes = ["bonaire", "sint_eustatius", "saba"].includes(g.geography_id);
      try {
        if (isBes) {
          if (!besPulled.done) {
            log(`[gap] HBX BES group BQ…`);
            const pulled = await pullCountryHotels(cfg, "Caribbean Netherlands", "BQ", {
              batchSize: 1000,
              maxHotelsPerCountry: 500,
              delayMs: 1200,
              rateLimiter: limiter,
              fields: "all",
              onBatch: (b) => log(`[gap] BQ ${b.from}-${b.to} ${b.pulled}`),
            });
            hbxUsed = 1 + (limiter.requestCount || 0);
            if (pulled.error?.quota_exceeded) {
              hbxPaused = true;
              result.hbx_status = "HBX_PAUSED_QUOTA";
            } else {
              besPulled.hotels = (pulled.hotels || []).map((raw) =>
                extractHbxHotel(raw, "Bonaire")
              );
              besPulled.done = true;
              result.hbx_status = "COMPLETE";
            }
          } else {
            result.hbx_status = "COMPLETE_SHARED_BQ";
          }
          if (besPulled.done) {
            const attributed = [];
            for (const h of besPulled.hotels) {
              const country = attributeBesGeography(h);
              if (country !== item.geography) continue;
              h.country = country;
              attributed.push(h);
            }
            hbxNew += attributed.length;
            appendHbxPack(attributed.map((h) => ({ ...h, country: item.geography })));
            const cands = attributed
              .filter((h) => h.hbx_hotel_code != null && h.name)
              .map((h) => toHbxCandidateRow(h, item.geography));
            discovered += cands.length;
            result.candidates_discovered += cands.length;
            if (enableWrites && cands.length) {
              const t = await prepareAndInsert(cands, {
                index,
                baseId,
                token,
                log,
                applied,
                batchLabel: `${item.geography} Gap Discovery HBX`,
              });
              result.new_shells += t.inserted;
              result.existing_matches += t.existing;
              result.duplicates_skipped += t.duplicate;
              result.invalids += t.invalid;
              shells += t.inserted;
              existing += t.existing;
              duplicates += t.duplicate;
              invalids += t.invalid;
            }
          }
        } else {
          const codes = hbxCodesForGeography(g);
          const code = codes[0];
          if (!code) {
            result.hbx_status = "NO_ISO_CODE";
          } else if (limiter.requestCount >= limiter.maxRequestsPerRun) {
            result.hbx_status = "BUDGET_EXHAUSTED";
          } else {
            log(`[gap] HBX ${item.geography} code=${code}…`);
            const pulled = await pullCountryHotels(cfg, item.geography, code, {
              batchSize: 1000,
              maxHotelsPerCountry: 1500,
              delayMs: 1200,
              rateLimiter: limiter,
              fields: "all",
              onBatch: (b) =>
                log(`[gap] ${item.geography} ${b.from}-${b.to} ${b.pulled}/${b.total ?? "?"}`),
            });
            hbxUsed = 1 + (limiter.requestCount || 0);
            if (pulled.error?.quota_exceeded) {
              hbxPaused = true;
              result.hbx_status = "HBX_PAUSED_QUOTA";
              log(`[gap] HBX_PAUSED_QUOTA`);
            } else {
              result.hbx_status =
                (pulled.hotels || []).length === 0 ? "COMPLETE_ZERO_OR_EMPTY" : "COMPLETE";
              const hotels = [];
              for (const raw of pulled.hotels || []) {
                const h = extractHbxHotel(raw, item.geography);
                if (h.hbx_hotel_code == null || !h.name) continue;
                h.country = item.geography;
                hotels.push(h);
              }
              hbxNew += hotels.length;
              appendHbxPack(hotels);
              const cands = hotels.map((h) => toHbxCandidateRow(h, item.geography));
              discovered += cands.length;
              result.candidates_discovered += cands.length;
              if (enableWrites && cands.length) {
                const t = await prepareAndInsert(cands, {
                  index,
                  baseId,
                  token,
                  log,
                  applied,
                  batchLabel: `${item.geography} Gap Discovery HBX`,
                });
                result.new_shells += t.inserted;
                result.existing_matches += t.existing;
                result.duplicates_skipped += t.duplicate;
                result.invalids += t.invalid;
                shells += t.inserted;
                existing += t.existing;
                duplicates += t.duplicate;
                invalids += t.invalid;
              }
            }
          }
        }
      } catch (err) {
        result.hbx_status = `ERROR:${String(err?.message || err).slice(0, 80)}`;
        log(`[gap] HBX error ${item.geography}: ${result.hbx_status}`);
      }
    } else if (hbxPaused) {
      result.hbx_status = "HBX_PAUSED_QUOTA";
    }

    // SerpAPI destination searches
    const serpKey = String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim();
    if (serpKey && serpMax > serpSearches) {
      const tracker = new SerpApiCreditTracker({
        ceiling: serpMax,
      });
      // Account for searches already spent this wave
      tracker.charged = serpSearches;
      const dests = item.destinations?.length
        ? item.destinations
        : [item.geography];
      const serpCands = [];
      result.serpapi_status = "RUN";
      for (const dest of dests.slice(0, 4)) {
        if (!tracker.canSpend(1)) break;
        const q = `hotels in ${dest}, ${item.geography}`;
        log(`[gap] SerpAPI ${q}`);
        const res = await searchGoogleHotels({ q, gl: "us", hl: "en" }, { tracker });
        serpSearches += 1;
        result.destinations_searched.push(dest);
        await sleep(400);
        if (!res.ok || !res.candidates?.length) continue;
        for (const hit of res.candidates.slice(0, 25)) {
          if (!hit?.name) continue;
          if (!isInScopeHotelName(hit.name)) {
            invalids += 1;
            result.invalids += 1;
            continue;
          }
          serpCands.push(toSerpCandidateRow(hit, item.geography, dest));
        }
      }
      // dedupe serp by name+country
      const seen = new Set();
      const uniq = [];
      for (const c of serpCands) {
        const k = `${c.normalized_property_name}|${normName(c.country)}`;
        if (seen.has(k)) continue;
        seen.add(k);
        uniq.push(c);
      }
      serpConfirmed += uniq.length;
      discovered += uniq.length;
      result.candidates_discovered += uniq.length;
      writeJson(path.join(STATE_DIR, "serpapi-candidates", `${g?.geography_id || item.geography}.json`), {
        geography: item.geography,
        count: uniq.length,
        // Persist Dealality-discovered SerpAPI candidates only (independent source)
        candidates: uniq.map((c) => ({
          candidate_id: c.candidate_id,
          property_name: c.property_name,
          country: c.country,
          city: c.city,
          address: c.address || null,
          website: c.website || null,
          phone: c.phone || null,
          source_type: c.source_type,
        })),
      });
      if (enableWrites && uniq.length) {
        const t = await prepareAndInsert(uniq, {
          index,
          baseId,
          token,
          log,
          applied,
          batchLabel: `${item.geography} Gap Discovery SerpAPI`,
        });
        result.new_shells += t.inserted;
        result.existing_matches += t.existing;
        result.duplicates_skipped += t.duplicate;
        result.invalids += t.invalid;
        shells += t.inserted;
        existing += t.existing;
        duplicates += t.duplicate;
        invalids += t.invalid;
      }
      if (!uniq.length && result.serpapi_status === "RUN") result.serpapi_status = "EMPTY";
      else if (uniq.length) result.serpapi_status = "IDENTITIES_FOUND";
    } else if (!serpKey) {
      result.serpapi_status = "NO_KEY";
    } else {
      result.serpapi_status = "BUDGET_EXHAUSTED";
    }

    // Targeted Cvent holds for this geography via existing universe merge
    if (enableWrites) {
      const universe = loadMasterUniverseCandidates();
      const hbx = loadHbxCandidates();
      const { merged } = mergeCandidateUniverses(universe, hbx);
      const focused = merged.filter(
        (c) => normalizeGeographyLabel(c.country) === normalizeGeographyLabel(item.geography)
      );
      if (focused.length) {
        const t = await prepareAndInsert(focused, {
          index,
          baseId,
          token,
          log,
          applied,
          batchLabel: `${item.geography} Gap Discovery Holds/Cvent`,
        });
        result.new_shells += t.inserted;
        result.existing_matches += t.existing;
        result.duplicates_skipped += t.duplicate;
        result.invalids += t.invalid;
        shells += t.inserted;
        existing += t.existing;
        duplicates += t.duplicate;
        invalids += t.invalid;
        discovered += focused.length;
        result.candidates_discovered += focused.length;
      }
    }

    result.census_after_geo = index.count;
    if (result.new_shells > 0) majorImproved.push(item.geography);
    geoResults.push(result);
    writeJson(path.join(STATE_DIR, "geo-results.json"), { generated_at, geoResults });
  }

  log(`[gap] completeness snapshot after…`);
  const after = await completenessSnapshot(baseId, token);
  const ZERO_AFTER = after.zeroGeographies;

  // Matrix refresh
  log(`[gap] regenerating 52-geography matrix…`);
  const geoAudit = await runFullCalaGeographyCoverageRegistryAuditV1({ log });
  const matrixSrc = geoAudit.FULL_GEOGRAPHY_COVERAGE_MATRIX || [];
  const holds = readJson(HOLDS_FP, { by_candidate_id: {} });
  const holdByCountry = {};
  for (const h of Object.values(holds.by_candidate_id || {})) {
    holdByCountry[h.country] = (holdByCountry[h.country] || 0) + 1;
  }
  const priorityByGeo = new Map(
    (priorityDoc.GEOGRAPHY_DISCOVERY_PRIORITY || []).map((r) => [r.geography, r])
  );
  const resultByGeo = new Map(geoResults.map((r) => [r.geography, r]));

  const matrix = matrixSrc.map((r) => {
    const name = r.name;
    const census = after.byCountry[name] || 0;
    const pr = priorityByGeo.get(name);
    const gr = resultByGeo.get(name);
    let coverage = "NEEDS_TARGETED_DISCOVERY";
    if (census === 0) coverage = "ZERO_CONFIRMED_PROPERTIES";
    else if (census < 40 && (r.tourism_priority === "S" || r.tourism_priority === "A"))
      coverage = "SOURCE_GAP";
    else if (census >= 200) coverage = "CORE_COVERAGE_STRONG";
    else if (census >= 80) coverage = "CORE_COVERAGE_MODERATE";
    else if (census >= 20) coverage = "CORE_COVERAGE_WEAK";

    return {
      geography: name,
      tourism_priority: r.tourism_priority,
      census_before: matrixDoc.matrix?.find((x) => x.geography === name)?.census_after ?? r.census_count,
      independent_candidates_discovered: gr?.candidates_discovered || 0,
      existing_matches: gr?.existing_matches || 0,
      new_shells: gr?.new_shells || 0,
      census_after: census,
      active_holds: holdByCountry[name] || 0,
      name_pct: census ? 100 : 0,
      city_pct: null,
      state_region_pct: null,
      HBX_STATUS: gr?.hbx_status || r.HBX_SEARCHED || null,
      SERPAPI_STATUS: gr?.serpapi_status || null,
      other_sources_searched: gr
        ? ["hbx_content_api", "serpapi_google_hotels", "cvent_candidate_universe"].filter(Boolean)
        : [],
      coverage_status: coverage,
      benchmark_aggregate_comparison_category: pr?.gap_class || null,
      primary_remaining_gap: census === 0 ? "independent_multi_source_discovery" : r.recommended_next_action,
    };
  });
  matrix.sort((a, b) => {
    const rank = {
      ZERO_CONFIRMED_PROPERTIES: 0,
      SOURCE_GAP: 1,
      NEEDS_TARGETED_DISCOVERY: 2,
      CORE_COVERAGE_WEAK: 3,
      CORE_COVERAGE_MODERATE: 4,
      CORE_COVERAGE_STRONG: 5,
    };
    return (rank[a.coverage_status] ?? 5) - (rank[b.coverage_status] ?? 5) || a.census_after - b.census_after;
  });

  const matrixPath =
    "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.json";
  writeJson(path.join(ROOT, matrixPath), {
    generated_at,
    wave: GAP_DISCOVERY_VERSION,
    census_before: CENSUS_BEFORE,
    census_after: after.count,
    PROPERTY_LEVEL_BENCHMARK_DATA_PERSISTED: "NO",
    matrix,
  });
  writeMd(
    path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.md"),
    [
      `# Core Identity 52-Geography Matrix (Gap Discovery Wave)`,
      ``,
      `Census ${CENSUS_BEFORE} → ${after.count}`,
      `Benchmark property data persisted: **NO**`,
      ``,
      `| Geography | Census | New shells | Coverage | HBX | SerpAPI | Benchmark gap class |`,
      `| --- | ---: | ---: | --- | --- | --- | --- |`,
      ...matrix.map(
        (r) =>
          `| ${r.geography} | ${r.census_after} | ${r.new_shells} | ${r.coverage_status} | ${r.HBX_STATUS || "—"} | ${r.SERPAPI_STATUS || "—"} | ${r.benchmark_aggregate_comparison_category || "—"} |`
      ),
    ].join("\n")
  );

  const counts = matrix.reduce((a, r) => {
    a[r.coverage_status] = (a[r.coverage_status] || 0) + 1;
    return a;
  }, {});

  const final = {
    ok: true,
    DISCOVERY_STATUS: enableWrites
      ? "independent_geographic_gap_discovery_wave_complete"
      : "independent_geographic_gap_discovery_wave_dry_run",
    mode,
    production_writes: enableWrites,
    CENSUS_BEFORE,
    CENSUS_AFTER: after.count,
    NEW_PROPERTIES_DISCOVERED: discovered,
    NEW_SHELLS_INSERTED: shells,
    EXISTING_MATCHES: existing,
    DUPLICATES_SKIPPED: duplicates,
    INVALIDS_EXCLUDED: invalids,
    ZERO_CENSUS_GEOGRAPHIES_BEFORE: ZERO_BEFORE,
    ZERO_CENSUS_GEOGRAPHIES_AFTER: ZERO_AFTER,
    MAJOR_GAP_GEOGRAPHIES_INVESTIGATED: majorInvestigated,
    MAJOR_GAP_GEOGRAPHIES_IMPROVED: majorImproved,
    HBX_REQUESTS_USED: hbxUsed,
    HBX_PAUSED_QUOTA: hbxPaused ? "YES" : "NO",
    HBX_NEW_IDENTITIES: hbxNew,
    SERPAPI_SEARCHES: serpSearches,
    SERPAPI_IDENTITIES_CONFIRMED: serpConfirmed,
    CITY_COMPLETENESS_BEFORE: before.city_pct,
    CITY_COMPLETENESS_AFTER: after.city_pct,
    STATE_REGION_COMPLETENESS_BEFORE: before.state_pct_applicable,
    STATE_REGION_COMPLETENESS_AFTER: after.state_pct_applicable,
    STRONG: counts.CORE_COVERAGE_STRONG || 0,
    MODERATE: counts.CORE_COVERAGE_MODERATE || 0,
    WEAK: counts.CORE_COVERAGE_WEAK || 0,
    SOURCE_GAP: counts.SOURCE_GAP || 0,
    ZERO: counts.ZERO_CONFIRMED_PROPERTIES || 0,
    TOP_10_REMAINING_GEOGRAPHIC_GAPS: matrix.slice(0, 10),
    ACTIVE_HOLDS_REMAINING: Object.keys(holds.by_candidate_id || {}).length,
    BENCHMARK_PROPERTY_DATA_PERSISTED: "NO",
    BENCHMARK_USED_AS_PROVENANCE: "NO",
    FULL_52_GEOGRAPHY_MATRIX_PATH: matrixPath,
    FOUNDER_DECISION_REQUIRED: "NO",
    NEXT_RECOMMENDED_ACTION:
      ZERO_AFTER.length > 0
        ? `Continue independent destination discovery for remaining zero geos: ${ZERO_AFTER.join(", ")}. Prefer SerpAPI / official directories; do not use benchmark records.`
        : "Re-run State/Region + City foundation backfill on new shells; then resume Core Identity completeness before enrichment.",
    geo_results: geoResults,
    generated_at,
  };

  writeJson(
    path.join(ROOT, "reports/research-engine-v2/independent-geographic-gap-discovery-final.json"),
    final
  );
  writeMd(
    path.join(ROOT, "reports/research-engine-v2/independent-geographic-gap-discovery-final.md"),
    `# Independent Geographic Gap Discovery\n\n**Status:** \`${final.DISCOVERY_STATUS}\`\n\nCensus ${CENSUS_BEFORE} → ${after.count} (+${shells} shells)\n\nZeros ${ZERO_BEFORE.length} → ${ZERO_AFTER.length}\n\nBenchmark property data persisted: **NO**\n`
  );

  log(
    `[gap] DONE census ${CENSUS_BEFORE}→${after.count} shells=${shells} zeros ${ZERO_BEFORE.length}→${ZERO_AFTER.length}`
  );
  return final;
}
