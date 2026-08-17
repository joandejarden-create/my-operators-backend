#!/usr/bin/env node
/**
 * CALA Review Promotion Sprint 01 — convert Sprint 01 REVIEW → READY
 * without lowering Tier A (≥0.90). Stage-only. No Census Expansion Sprint 02.
 *
 * Usage:
 *   node scripts/hotel-intelligence-cala-review-promotion-sprint-01.mjs
 *   node scripts/hotel-intelligence-cala-review-promotion-sprint-01.mjs --skip-giata
 *   node scripts/hotel-intelligence-cala-review-promotion-sprint-01.mjs --skip-census
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
import {
  resolveDiscoveryCity,
  assignDiscoveryConfidence,
  STAGE_STATUS,
  CITY_INFER_VERSION,
  filterCensusByCountry,
} from "../lib/hotel-intelligence/discovery-factory/index.js";
import {
  buildCalaCoverageDashboard,
  persistCoverageDashboard,
} from "../lib/hotel-intelligence/coverage-dashboard/index.js";
import { resolveHotelIdentity } from "../lib/hotel-intelligence/identity-resolve.js";
import { createGiataDriveProvider } from "../lib/hotel-intelligence/providers/giata-drive.js";
import { createSerpApiProvider } from "../lib/hotel-intelligence/providers/serpapi.js";
import { normName } from "../lib/research-engine-v2/census-autopilot-v2/identity-dedupe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/cala-review-promotion-sprint-01"
);
const SPRINT01_DATA = path.join(
  ROOT,
  "data/hotel-intelligence/cala-census-expansion-sprint-01"
);
const FACTORY_DATA = path.join(ROOT, "data/hotel-intelligence/discovery-factory");
const PROMO_DATA = path.join(
  ROOT,
  "data/hotel-intelligence/cala-review-promotion-sprint-01"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";
process.env.ENABLE_CENSUS_SHELL_INSERTS = "0";

const READY_MIN = 0.9;
const TRACK_B_COUNTRIES = [
  "Turks and Caicos",
  "Turks and Caicos Islands",
  "Bonaire",
  "Martinique",
  "U.S. Virgin Islands",
  "Anguilla",
  "Montserrat",
  "Guadeloupe",
  "Saint Lucia",
];

const GIATA_ISO = {
  "Turks and Caicos": "TC",
  "Turks and Caicos Islands": "TC",
  Bonaire: "BQ",
  Martinique: "MQ",
  "U.S. Virgin Islands": "VI",
  Anguilla: "AI",
  Montserrat: "MS",
  Guadeloupe: "GP",
  "Saint Lucia": "LC",
};

const args = new Set(process.argv.slice(2));
const SKIP_GIATA = args.has("--skip-giata");
const SKIP_CENSUS = args.has("--skip-census");
const SKIP_SERPAPI = args.has("--skip-serpapi") || !String(process.env.SERPAPI_API_KEY || "").trim();
const BRAZIL_SERP_BUDGET = 50;

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, data) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function readJson(fp, fallback) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}
function loadStagedHotels(fp) {
  const j = readJson(fp, { hotels: [] });
  return Array.isArray(j.hotels) ? j.hotels : Object.values(j.hotels || {});
}

function isTrackB(country) {
  const c = String(country || "");
  return TRACK_B_COUNTRIES.includes(c);
}

function hotelKey(h) {
  return (
    h.hotel_id ||
    h.discovery?.candidate_id ||
    `${normName(h.identity?.official_name || h.identity?.display_name || "")}::${normName(h.location?.country || "")}`
  );
}

function nameLocationKey(h) {
  return `${normName(h.identity?.official_name || h.identity?.display_name || "")}::${normName(h.location?.city || "")}::${normName(h.location?.country || "")}`;
}

function gapToReady(conf) {
  return Math.max(0, Math.round((READY_MIN - conf) * 1000) / 1000);
}

function bandFor(conf) {
  if (conf >= 0.88) return "0.88–0.899";
  if (conf >= 0.85) return "0.85–0.879";
  if (conf >= 0.8) return "0.80–0.849";
  return "<0.80";
}

function diagnoseReasons(h, cityResult, conf) {
  const reasons = new Set();
  const before = Number(h.discovery?.identity_confidence || 0);
  const cityConf = Number(
    cityResult?.confidence ?? h.discovery?.city_confidence ?? 0
  );
  if (cityConf > 0 && cityConf < 0.85) reasons.add("CITY_CONFIDENCE");
  if (cityResult?.country_city_conflict) reasons.add("LOCALITY_AMBIGUITY");
  if (cityResult?.multi_city) reasons.add("LOCALITY_AMBIGUITY");
  if ((conf?.name_strength ?? 1) < 0.7) reasons.add("NAME_AMBIGUITY");
  if (!h.location?.address_line_1) reasons.add("ADDRESS_MISSING");
  if (h.location?.latitude == null) reasons.add("COORDINATES_MISSING");
  const mr = h.discovery?.evidence?.matching_reasons || [];
  if (
    mr.some((r) => /ambiguous|duplicate/i.test(String(r))) ||
    (conf?.reasons || []).includes("soft_duplicate_pressure")
  ) {
    reasons.add("DUPLICATE_RISK");
  }
  if (!h.location?.country) reasons.add("COUNTRY_UNCERTAINTY");
  if ((conf?.identity_confidence ?? before) < READY_MIN) {
    reasons.add("IDENTITY_CONFIDENCE");
  }
  if (!h.discovery?.source_url && !h.digital?.website) {
    reasons.add("SOURCE_CONFIDENCE");
  }
  if (!reasons.size) reasons.add("OTHER");
  return [...reasons];
}

async function listCensus() {
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
  if (!token || !baseId) throw new Error("AIRTABLE credentials missing");
  const base = new Airtable({ apiKey: token }).base(baseId);
  const byCountry = {};
  const records = [];
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({
      pageSize: 100,
      fields: [
        MAP_CENSUS_FIELDS.propertyName,
        MAP_CENSUS_FIELDS.officialName,
        MAP_CENSUS_FIELDS.city,
        MAP_CENSUS_FIELDS.country,
        MAP_CENSUS_FIELDS.latitude,
        MAP_CENSUS_FIELDS.longitude,
        MAP_CENSUS_FIELDS.website,
        MAP_CENSUS_FIELDS.phone,
      ],
    })
    .eachPage((page, next) => {
      for (const rec of page) {
        records.push(rec);
        const c = String(rec.fields?.[MAP_CENSUS_FIELDS.country] || "").trim();
        if (c) byCountry[c] = (byCountry[c] || 0) + 1;
      }
      next();
    });
  return { byCountry, records, total: records.length };
}

function reevaluate(h, opts = {}) {
  const name =
    opts.name ||
    h.identity?.display_name ||
    h.identity?.official_name ||
    "";
  const country = opts.country || h.location?.country || "";
  const cityResult =
    opts.cityResult ||
    resolveDiscoveryCity({
      property_name: name,
      origin_url: h.discovery?.source_url || h.digital?.website,
      country,
      // Re-infer from URL — do not treat staged city as explicit authority
    });

  let cityOverride = opts.cityOverride || null;
  let cityResultFinal = cityResult;
  if (cityOverride) {
    cityResultFinal = {
      city: cityOverride.city,
      method: cityOverride.method || "provider_corroboration",
      confidence: cityOverride.confidence ?? 0.9,
      known_city: true,
      inferred: true,
      corroboration: cityOverride.corroboration || ["provider"],
    };
  }

  const censusSlice = opts.censusRecords
    ? filterCensusByCountry(opts.censusRecords, country)
    : [];

  const resolved =
    opts.resolveResult ||
    (censusSlice.length
      ? resolveHotelIdentity(
          {
            name,
            city: cityResultFinal.city,
            country,
            website: h.digital?.website || h.discovery?.source_url || null,
            brand: h.brand?.brand_name || null,
            external_ids: Object.fromEntries(
              (h.linkages?.external_ids || []).map((e) => [
                e.provider,
                e.external_id,
              ])
            ),
          },
          censusSlice,
          {}
        )
      : {
          match_status: "new",
          match_score: 0,
          matching_reasons: h.discovery?.evidence?.matching_reasons || [],
          candidate_matches: [],
        });

  const conf = assignDiscoveryConfidence({
    name,
    country,
    cityResult: cityResultFinal,
    resolveResult: resolved,
    source_type: h.discovery?.source_type,
  });

  return { name, country, cityResult: cityResultFinal, resolved, conf };
}

function applyPromotion(h, evalResult, meta = {}) {
  const { cityResult, conf, resolved } = evalResult;
  const next = structuredClone(h);
  next.location.city = cityResult.city;
  next.verification.record_confidence = conf.identity_confidence;
  next.verification.review_status = conf.stage_status;
  next.discovery = {
    ...next.discovery,
    city_infer_version: CITY_INFER_VERSION,
    tier: conf.tier,
    stage_status: conf.stage_status,
    identity_confidence: conf.identity_confidence,
    city_confidence: conf.city_confidence,
    city_method: cityResult.method,
    city_alternate: cityResult.alternate_city || null,
    evidence: {
      ...(next.discovery?.evidence || {}),
      match_status: resolved.match_status,
      match_score: resolved.match_score,
      matching_reasons: resolved.matching_reasons || [],
      confidence_reasons: conf.reasons || [],
      promotion: {
        sprint: "review_promotion_sprint_01",
        path: meta.path || "existing_evidence",
        confidence_before: meta.confidence_before ?? null,
        confidence_after: conf.identity_confidence,
        reasons: meta.reasons || [],
      },
    },
  };
  if (meta.giata_id) {
    const ext = next.linkages.external_ids || [];
    if (!ext.some((e) => e.provider === "giata_drive" && e.external_id === meta.giata_id)) {
      ext.push({ provider: "giata_drive", external_id: String(meta.giata_id) });
    }
    next.linkages.external_ids = ext;
  }
  if (meta.website) next.digital.website = meta.website;
  if (meta.address) next.location.address_line_1 = meta.address;
  if (meta.phone) next.digital.phone = meta.phone;
  if (meta.brand) {
    next.brand.brand_name = meta.brand;
    next.brand.independent = false;
  }
  if (meta.lat != null) next.location.latitude = meta.lat;
  if (meta.lng != null) next.location.longitude = meta.lng;
  return next;
}

function nameSimilarity(a, b) {
  const na = normName(a);
  const nb = normName(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  if (na.includes(nb) || nb.includes(na)) return 0.92;
  const ta = new Set(na.split(/\s+/).filter((t) => t.length > 2));
  const tb = new Set(nb.split(/\s+/).filter((t) => t.length > 2));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  return inter / Math.max(ta.size, tb.size);
}

async function loadGiataIndex(provider, country, limit = 20) {
  const iso = GIATA_ISO[country];
  if (!iso) return { hotels: [], calls: 0 };
  const listed = await provider.searchHotels({
    countryCode: iso,
    limit,
    fetch_details: true,
  });
  return {
    hotels: listed.hotels || [],
    calls: 1 + (listed.hotels?.length || 0),
    status: listed.provider_status,
  };
}

function findGiataMatch(hotel, giataHotels) {
  const name = hotel.identity?.display_name || hotel.identity?.official_name;
  let best = null;
  for (const g of giataHotels) {
    const sim = nameSimilarity(name, g.name);
    if (sim < 0.72) continue;
    if (!best || sim > best.sim) best = { hotel: g, sim };
  }
  return best;
}

function coverageBuckets(rows) {
  const b = {
    zero: 0,
    below_20: 0,
    below_50: 0,
    below_80: 0,
    at_or_above_95: 0,
  };
  for (const r of rows || []) {
    const pct = Number(r.coverage_pct);
    const hotels = Number(r.current_dealality_hotels || 0);
    if (!Number.isFinite(pct)) {
      if (hotels === 0) b.zero += 1;
      continue;
    }
    if (hotels === 0 || pct <= 0) b.zero += 1;
    if (pct < 20) b.below_20 += 1;
    if (pct < 50) b.below_50 += 1;
    if (pct < 80) b.below_80 += 1;
    if (pct >= 95) b.at_or_above_95 += 1;
  }
  return b;
}

function findRow(dash, country) {
  return (dash.rows || []).find(
    (r) =>
      String(r.country || "").toLowerCase() === String(country || "").toLowerCase()
  );
}

async function main() {
  ensureDir(OUT_DIR);
  ensureDir(PROMO_DATA);

  const safety = {
    ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
      process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES,
    ENABLE_HBX_CENSUS_WRITES: process.env.ENABLE_HBX_CENSUS_WRITES,
    production_writes: 0,
    census_writes: 0,
    automatic_merges: 0,
    automatic_imports: 0,
    schema_changes: 0,
    migrations: 0,
    secrets_exposed: 0,
  };
  console.log(
    JSON.stringify({
      module: "cala-review-promotion-sprint-01",
      event: "start",
      safety,
    })
  );

  const reviewHotels = loadStagedHotels(
    path.join(SPRINT01_DATA, "staged-review-required.json")
  );
  const sprint01Ready = loadStagedHotels(
    path.join(SPRINT01_DATA, "staged-ready-for-import.json")
  );
  const priorReady = loadStagedHotels(
    path.join(FACTORY_DATA, "staged-ready-for-import.json")
  );

  if (reviewHotels.length !== 832) {
    console.warn(
      JSON.stringify({
        module: "cala-review-promotion-sprint-01",
        event: "review_count_mismatch",
        expected: 832,
        actual: reviewHotels.length,
      })
    );
  }

  let censusRecords = [];
  let censusTotal = 5956;
  let byCountry = {};
  if (!SKIP_CENSUS) {
    const listed = await listCensus();
    censusRecords = listed.records;
    censusTotal = listed.total;
    byCountry = listed.byCountry;
  }

  // --- 3. Review-wall diagnosis (baseline, pre-fix) ---
  const reasonAgg = new Map();
  const confBands = {
    "0.88–0.899": 0,
    "0.85–0.879": 0,
    "0.80–0.849": 0,
    "<0.80": 0,
  };
  const trackB = [];
  const brazil = [];
  for (const h of reviewHotels) {
    const before = Number(h.discovery?.identity_confidence || 0);
    confBands[bandFor(before)] = (confBands[bandFor(before)] || 0) + 1;
    const cityResult = resolveDiscoveryCity({
      property_name: h.identity?.display_name || h.identity?.official_name,
      origin_url: h.discovery?.source_url || h.digital?.website,
      country: h.location?.country,
    });
    const conf = assignDiscoveryConfidence({
      name: h.identity?.display_name || h.identity?.official_name,
      country: h.location?.country,
      cityResult,
      resolveResult: {
        match_status: "new",
        match_score: 0,
        candidate_matches: [],
      },
    });
    for (const reason of diagnoseReasons(h, cityResult, conf)) {
      if (!reasonAgg.has(reason)) {
        reasonAgg.set(reason, {
          count: 0,
          confSum: 0,
          gapSum: 0,
        });
      }
      const a = reasonAgg.get(reason);
      a.count += 1;
      a.confSum += before;
      a.gapSum += gapToReady(before);
    }
    if (isTrackB(h.location?.country)) trackB.push(h);
    else if (h.location?.country === "Brazil") brazil.push(h);
  }

  const reviewWall = [...reasonAgg.entries()]
    .map(([reason, a]) => ({
      REVIEW_REASON: reason,
      COUNT: a.count,
      PCT_OF_REVIEW: Math.round((1000 * a.count) / reviewHotels.length) / 10,
      AVERAGE_CONFIDENCE: Math.round((a.confSum / a.count) * 1000) / 1000,
      AVERAGE_GAP_TO_READY: Math.round((a.gapSum / a.count) * 1000) / 1000,
    }))
    .sort((a, b) => b.COUNT - a.COUNT);

  // --- Provider setup ---
  const providerMetrics = {
    giata_drive_calls: 0,
    giata_matches: 0,
    serpapi_calls: 0,
    serpapi_useful: 0,
    promotions_existing_evidence_only: 0,
    promotions_requiring_giata: 0,
    promotions_requiring_serpapi: 0,
  };

  let giataProvider = null;
  const giataCache = new Map();
  if (!SKIP_GIATA && String(process.env.GIATA_DRIVE_API_KEY || "").trim()) {
    giataProvider = createGiataDriveProvider({
      forceEnabled: true,
      env: { ...process.env, HOTEL_INTELLIGENCE_GIATA_DRIVE: "1" },
    });
  }

  let serpProvider = null;
  if (!SKIP_SERPAPI) {
    serpProvider = createSerpApiProvider({
      forceEnabled: true,
      env: { ...process.env, HOTEL_INTELLIGENCE_SERPAPI: "1" },
    });
  }

  async function maybeGiata(h) {
    if (!giataProvider) return null;
    const country = h.location?.country;
    if (!giataCache.has(country)) {
      const idx = await loadGiataIndex(giataProvider, country, 40);
      providerMetrics.giata_drive_calls += idx.calls;
      giataCache.set(country, idx.hotels);
    }
    const match = findGiataMatch(h, giataCache.get(country) || []);
    if (!match) return null;
    providerMetrics.giata_matches += 1;
    return match;
  }

  async function maybeSerp(h) {
    if (!serpProvider) return null;
    const name = h.identity?.display_name || h.identity?.official_name;
    const country = h.location?.country;
    providerMetrics.serpapi_calls += 1;
    const res = await serpProvider.searchHotels({
      name,
      country,
      city: h.location?.city,
      hotel_id: h.hotel_id,
    });
    const top = (res.hotels || [])[0];
    if (!top?.name) return null;
    if (nameSimilarity(name, top.name) < 0.7) return null;
    providerMetrics.serpapi_useful += 1;
    return top;
  }

  // --- Process cohort ---
  async function processCohort(hotels, label, opts = {}) {
    const stats = {
      label,
      review_before: hotels.length,
      promoted_ready: 0,
      still_review: 0,
      ambiguous: 0,
      rejected: 0,
      matched_existing: 0,
      avg_conf_before: 0,
      avg_conf_after: 0,
      giata_matches: 0,
      serpapi_calls: 0,
      by_country: {},
      promoted: [],
      still: [],
      rejected_rows: [],
      ambiguous_rows: [],
    };
    let confBeforeSum = 0;
    let confAfterSum = 0;

    // Sort near-ready first
    const ranked = [...hotels].sort((a, b) => {
      const ca = Number(a.discovery?.identity_confidence || 0);
      const cb = Number(b.discovery?.identity_confidence || 0);
      return cb - ca;
    });

    for (const h of ranked) {
      const country = h.location?.country || "Unknown";
      if (!stats.by_country[country]) {
        stats.by_country[country] = {
          review_before: 0,
          promoted_ready: 0,
          still_review: 0,
          ambiguous: 0,
          rejected: 0,
          matched_existing: 0,
          conf_before_sum: 0,
          conf_after_sum: 0,
        };
      }
      const bc = stats.by_country[country];
      bc.review_before += 1;

      const before = Number(h.discovery?.identity_confidence || 0);
      confBeforeSum += before;
      bc.conf_before_sum += before;

      let evalResult = reevaluate(h, { censusRecords });
      let pathUsed = "existing_evidence";
      let extra = {};

      // If still not READY and near-ready / conflict — try GIATA (Track B prioritized)
      const needsHelp =
        evalResult.conf.stage_status !== STAGE_STATUS.READY_FOR_IMPORT &&
        (evalResult.cityResult.country_city_conflict ||
          evalResult.conf.identity_confidence >= 0.8 ||
          opts.forceProvider);

      if (
        needsHelp &&
        opts.useGiata &&
        evalResult.conf.stage_status === STAGE_STATUS.REVIEW_REQUIRED
      ) {
        const gMatch = await maybeGiata(h);
        if (gMatch) {
          stats.giata_matches += 1;
          const g = gMatch.hotel;
          const cityOk =
            g.city &&
            !String(g.city)
              .toLowerCase()
              .includes("unknown");
          if (cityOk) {
            evalResult = reevaluate(h, {
              censusRecords,
              cityOverride: {
                city: g.city,
                method: "giata_drive_name_match",
                confidence: Math.min(0.94, 0.86 + gMatch.sim * 0.08),
                corroboration: ["giata_drive"],
              },
            });
            pathUsed = "giata_drive";
            extra = {
              giata_id: g.external_id,
              website: g.website || undefined,
              address: g.address || undefined,
              phone: g.phone || undefined,
              brand: g.brand_name || undefined,
              lat: g.latitude,
              lng: g.longitude,
            };
          }
        }
      }

      // Selective SerpApi (Brazil budget / Track B residual)
      if (
        opts.useSerp &&
        opts.serpBudgetRemaining > 0 &&
        evalResult.conf.stage_status === STAGE_STATUS.REVIEW_REQUIRED &&
        evalResult.conf.identity_confidence >= 0.82
      ) {
        opts.serpBudgetRemaining -= 1;
        stats.serpapi_calls += 1;
        const s = await maybeSerp(h);
        if (s?.city) {
          evalResult = reevaluate(h, {
            censusRecords,
            cityOverride: {
              city: s.city,
              method: "serpapi_corroboration",
              confidence: 0.88,
              corroboration: ["serpapi"],
            },
          });
          pathUsed = pathUsed === "existing_evidence" ? "serpapi" : `${pathUsed}+serpapi`;
          extra = {
            ...extra,
            website: s.website || extra.website,
            address: s.address || extra.address,
            lat: s.latitude ?? extra.lat,
            lng: s.longitude ?? extra.lng,
          };
        }
      }

      const after = evalResult.conf.identity_confidence;
      confAfterSum += after;
      bc.conf_after_sum += after;

      const status = evalResult.conf.stage_status;
      if (status === STAGE_STATUS.MATCHED_EXISTING) {
        stats.matched_existing += 1;
        bc.matched_existing += 1;
        stats.rejected_rows.push({
          hotel_id: h.hotel_id,
          name: evalResult.name,
          country,
          status,
        });
        continue;
      }
      if (status === STAGE_STATUS.REJECTED) {
        const amb = (evalResult.conf.reasons || []).includes("ambiguous_identity");
        if (amb) {
          stats.ambiguous += 1;
          bc.ambiguous += 1;
          stats.ambiguous_rows.push({
            hotel_id: h.hotel_id,
            name: evalResult.name,
            country,
          });
        } else {
          stats.rejected += 1;
          bc.rejected += 1;
          stats.rejected_rows.push({
            hotel_id: h.hotel_id,
            name: evalResult.name,
            country,
            status,
          });
        }
        continue;
      }

      if (status === STAGE_STATUS.READY_FOR_IMPORT) {
        const promoted = applyPromotion(h, evalResult, {
          path: pathUsed,
          confidence_before: before,
          reasons: evalResult.conf.reasons,
          ...extra,
        });
        stats.promoted_ready += 1;
        bc.promoted_ready += 1;
        stats.promoted.push(promoted);
        if (pathUsed === "existing_evidence") {
          providerMetrics.promotions_existing_evidence_only += 1;
        } else if (String(pathUsed).includes("giata")) {
          providerMetrics.promotions_requiring_giata += 1;
        }
        if (String(pathUsed).includes("serpapi")) {
          providerMetrics.promotions_requiring_serpapi += 1;
        }
      } else {
        const kept = applyPromotion(h, evalResult, {
          path: "still_review",
          confidence_before: before,
          reasons: evalResult.conf.reasons,
          ...extra,
        });
        stats.still_review += 1;
        bc.still_review += 1;
        stats.still.push(kept);
      }
    }

    stats.avg_conf_before =
      hotels.length > 0
        ? Math.round((confBeforeSum / hotels.length) * 1000) / 1000
        : 0;
    stats.avg_conf_after =
      hotels.length > 0
        ? Math.round((confAfterSum / hotels.length) * 1000) / 1000
        : 0;

    for (const [c, row] of Object.entries(stats.by_country)) {
      const n = row.review_before || 1;
      row.average_confidence_before =
        Math.round((row.conf_before_sum / n) * 1000) / 1000;
      row.average_confidence_after =
        Math.round((row.conf_after_sum / n) * 1000) / 1000;
      delete row.conf_before_sum;
      delete row.conf_after_sum;
    }

    return stats;
  }

  // Track B: evidence-first, then GIATA only for residual conflict / near-ready
  const trackBPass1 = await processCohort(trackB, "TRACK_B", {
    useGiata: false,
    useSerp: false,
    serpBudgetRemaining: 0,
  });
  const trackBResidual = trackBPass1.still.filter((h) => {
    const cityResult = resolveDiscoveryCity({
      property_name: h.identity?.display_name || h.identity?.official_name,
      origin_url: h.discovery?.source_url || h.digital?.website,
      country: h.location?.country,
    });
    const conf = Number(h.discovery?.identity_confidence || 0);
    return cityResult.country_city_conflict || conf >= 0.82;
  });
  const trackBPass2 =
    giataProvider && trackBResidual.length
      ? await processCohort(trackBResidual, "TRACK_B_GIATA", {
          useGiata: true,
          useSerp: false,
          serpBudgetRemaining: 0,
        })
      : {
          promoted: [],
          still: trackBResidual,
          promoted_ready: 0,
          still_review: trackBResidual.length,
          ambiguous: 0,
          rejected: 0,
          matched_existing: 0,
          giata_matches: 0,
          serpapi_calls: 0,
          by_country: {},
          avg_conf_before: 0,
          avg_conf_after: 0,
          ambiguous_rows: [],
          rejected_rows: [],
        };

  const trackBGiataPromotedKeys = new Set(trackBPass2.promoted.map(hotelKey));
  const trackBStats = {
    label: "TRACK_B",
    review_before: trackB.length,
    promoted_ready:
      trackBPass1.promoted_ready + trackBPass2.promoted_ready,
    still_review: [
      ...trackBPass1.still.filter((h) => !trackBGiataPromotedKeys.has(hotelKey(h))),
      ...trackBPass2.still,
    ].length,
    ambiguous: trackBPass1.ambiguous + trackBPass2.ambiguous,
    rejected: trackBPass1.rejected + trackBPass2.rejected,
    matched_existing:
      trackBPass1.matched_existing + trackBPass2.matched_existing,
    avg_conf_before: trackBPass1.avg_conf_before,
    avg_conf_after: trackBPass2.promoted_ready
      ? Math.round(
          ((trackBPass1.avg_conf_after * trackB.length +
            (trackBPass2.avg_conf_after - trackBPass1.avg_conf_after) *
              trackBResidual.length) /
            trackB.length) *
            1000
        ) / 1000
      : trackBPass1.avg_conf_after,
    giata_matches: trackBPass2.giata_matches,
    serpapi_calls: 0,
    by_country: { ...trackBPass1.by_country },
    promoted: [...trackBPass1.promoted, ...trackBPass2.promoted],
    still: [
      ...trackBPass1.still.filter((h) => !trackBGiataPromotedKeys.has(hotelKey(h))),
      ...trackBPass2.still,
    ],
    rejected_rows: [
      ...trackBPass1.rejected_rows,
      ...trackBPass2.rejected_rows,
    ],
    ambiguous_rows: [
      ...trackBPass1.ambiguous_rows,
      ...trackBPass2.ambiguous_rows,
    ],
  };
  // Merge pass2 country deltas into by_country
  for (const [c, row] of Object.entries(trackBPass2.by_country || {})) {
    if (!trackBStats.by_country[c]) {
      trackBStats.by_country[c] = { ...row };
      continue;
    }
    const base = trackBStats.by_country[c];
    base.promoted_ready += row.promoted_ready;
    base.still_review = Math.max(0, base.still_review - row.promoted_ready);
    base.ambiguous += row.ambiguous;
    base.rejected += row.rejected;
    base.matched_existing += row.matched_existing;
    base.average_confidence_after = row.average_confidence_after;
  }

  // Brazil — evidence first; SerpApi only if key + budget
  const brazilOpts = {
    useGiata: false,
    useSerp: Boolean(serpProvider),
    serpBudgetRemaining: BRAZIL_SERP_BUDGET,
  };
  const brazilStats = await processCohort(brazil, "BRAZIL", brazilOpts);

  // If SerpApi budget burned with poor yield, stop (already controlled by budget)

  const allPromoted = [...trackBStats.promoted, ...brazilStats.promoted];
  const allStill = [...trackBStats.still, ...brazilStats.still];

  // --- Merge READY queues (dedupe) ---
  const readyBefore = priorReady.length;
  const mergedMap = new Map();
  const seenNameLoc = new Set();
  let duplicatesWithin = 0;
  for (const h of [...priorReady, ...allPromoted]) {
    const k = hotelKey(h);
    const nl = nameLocationKey(h);
    if (mergedMap.has(k) || seenNameLoc.has(nl)) {
      duplicatesWithin += 1;
      continue;
    }
    mergedMap.set(k, h);
    seenNameLoc.add(nl);
  }
  // Also dedupe against sprint01 ready already in priorReady
  const readyAfter = [...mergedMap.values()];
  const newPromotionsNet = Math.max(0, readyAfter.length - readyBefore);

  // Update factory review queue: remove promoted, keep still + prior review not in sprint01
  const priorReview = loadStagedHotels(
    path.join(FACTORY_DATA, "staged-review-required.json")
  );
  const promotedKeys = new Set(allPromoted.map(hotelKey));
  const promotedNl = new Set(allPromoted.map(nameLocationKey));
  const stillMap = new Map(allStill.map((h) => [hotelKey(h), h]));
  const mergedReview = [];
  const reviewSeen = new Set();
  for (const h of [...allStill, ...priorReview]) {
    const k = hotelKey(h);
    const nl = nameLocationKey(h);
    if (promotedKeys.has(k) || promotedNl.has(nl)) continue;
    if (reviewSeen.has(k)) continue;
    reviewSeen.add(k);
    mergedReview.push(stillMap.get(k) || h);
  }

  writeJson(path.join(PROMO_DATA, "staged-promoted-ready.json"), {
    version: 1,
    sprint: "review_promotion_sprint_01",
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: allPromoted,
  });
  writeJson(path.join(PROMO_DATA, "staged-still-review.json"), {
    version: 1,
    sprint: "review_promotion_sprint_01",
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: allStill,
  });
  writeJson(path.join(FACTORY_DATA, "staged-ready-for-import.json"), {
    version: 1,
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: readyAfter,
  });
  writeJson(path.join(FACTORY_DATA, "staged-review-required.json"), {
    version: 1,
    updated_at: new Date().toISOString(),
    production_writes: false,
    hotels: mergedReview,
  });

  // --- Coverage dashboard ---
  const liveDash = buildCalaCoverageDashboard(byCountry, { root: ROOT });

  function projectCounts(extraByCountry) {
    const projected = { ...byCountry };
    for (const [c, n] of Object.entries(extraByCountry)) {
      projected[c] = (projected[c] || 0) + n;
    }
    return projected;
  }

  const readyByCountry = {};
  for (const h of readyAfter) {
    const c = h.location?.country || "Unknown";
    // Normalize Turks alias for dashboard rows
    const key =
      c === "Turks and Caicos" ? "Turks and Caicos Islands" : c;
    readyByCountry[key] = (readyByCountry[key] || 0) + 1;
  }

  const projectedByCountry = projectCounts(readyByCountry);
  const projectedDash = buildCalaCoverageDashboard(projectedByCountry, {
    root: ROOT,
  });

  const liveBuckets = coverageBuckets(liveDash.rows || []);
  const projectedBuckets = coverageBuckets(projectedDash.rows || []);

  // Zero-coverage country impact (Track B focus)
  const trackBGeo = {};
  for (const c of [
    "Turks and Caicos Islands",
    "Bonaire",
    "Martinique",
    "U.S. Virgin Islands",
    "Anguilla",
    "Montserrat",
    "Guadeloupe",
    "Saint Lucia",
  ]) {
    const live = findRow(liveDash, c);
    const liveN = live?.current_dealality_hotels ?? byCountry[c] ?? 0;
    const readyOnly = readyByCountry[c] || 0;
    const afterPromo = liveN + readyOnly;
    const univ = live?.estimated_hotel_universe || null;
    trackBGeo[c] = {
      live_hotels: liveN,
      ready_staged: readyOnly,
      projected_if_imported: afterPromo,
      coverage_before: live?.coverage_pct ?? (univ ? 0 : null),
      coverage_after_if_promoted_imported:
        univ && univ > 0
          ? Math.round((1000 * afterPromo) / univ) / 10
          : null,
      estimated_universe: univ,
      ...(trackBStats.by_country[c] ||
        trackBStats.by_country[
          c === "Turks and Caicos Islands" ? "Turks and Caicos" : c
        ] ||
        {}),
    };
  }

  const zeroLive = (liveDash.rows || []).filter(
    (r) => (r.current_dealality_hotels || 0) === 0
  ).length;
  const zeroAfterPriorReady = (projectedDash.rows || []).filter(
    (r) => (r.current_dealality_hotels || 0) === 0
  ).length;

  // Countries moving above thresholds
  function countriesMovingAbove(threshold) {
    const out = [];
    for (const live of liveDash.rows || []) {
      const c = live.country;
      const before = Number(live.coverage_pct) || 0;
      const proj = findRow(projectedDash, c);
      const after = Number(proj?.coverage_pct) || 0;
      if (before < threshold && after >= threshold) {
        out.push({ country: c, before, after });
      }
    }
    return out;
  }

  persistCoverageDashboard(liveDash, null, { root: ROOT });
  writeJson(
    path.join(
      ROOT,
      "reports/hotel-intelligence/cala-coverage-dashboard-v1/projected-with-all-deduped-ready.json"
    ),
    projectedDash
  );
  writeJson(path.join(OUT_DIR, "live-coverage-dashboard.json"), liveDash);
  writeJson(
    path.join(OUT_DIR, "projected-with-all-deduped-ready.json"),
    projectedDash
  );

  // Quality samples
  function sample(arr, n = 8) {
    return arr.slice(0, n).map((h) => ({
      hotel_id: h.hotel_id,
      name: h.identity?.display_name,
      city: h.location?.city,
      country: h.location?.country,
      confidence: h.discovery?.identity_confidence,
      city_method: h.discovery?.city_method,
      path: h.discovery?.evidence?.promotion?.path,
      confidence_before: h.discovery?.evidence?.promotion?.confidence_before,
    }));
  }

  const externalCalls =
    providerMetrics.giata_drive_calls + providerMetrics.serpapi_calls;
  const totalPromoted =
    trackBStats.promoted_ready + brazilStats.promoted_ready;
  const stillReview =
    trackBStats.still_review + brazilStats.still_review;
  const ambiguous =
    trackBStats.ambiguous + brazilStats.ambiguous;
  const rejected =
    trackBStats.rejected +
    brazilStats.rejected +
    trackBStats.matched_existing +
    brazilStats.matched_existing;

  const projectedCensus = censusTotal + readyAfter.length;

  // Sprint 02 recommendation (re-ranked, not executed)
  const gapRows = [...(projectedDash.rows || [])]
    .map((r) => ({
      country: r.country,
      live: r.current_dealality_hotels || 0,
      universe: r.estimated_hotel_universe || 0,
      coverage: r.coverage_pct || 0,
      gap: Math.max(
        0,
        (r.estimated_hotel_universe || 0) - (r.current_dealality_hotels || 0)
      ),
      zero: (r.current_dealality_hotels || 0) === 0,
      status: r.coverage_status,
    }))
    .sort((a, b) => {
      if (a.zero !== b.zero) return a.zero ? -1 : 1;
      return b.gap - a.gap;
    });

  const sprint02 = {
    note: "Re-ranked after review promotion; NOT executed",
    TRACK_A: {
      country: "Brazil",
      candidate_target: 600,
      rationale:
        "Still largest absolute gap; promotion proved evidence-first converts REVIEW efficiently — pair discovery with same-sprint corroboration",
    },
    TRACK_B: gapRows
      .filter((r) => r.zero || r.coverage < 20)
      .filter(
        (r) =>
          ![
            "Turks and Caicos Islands",
            "Bonaire",
            "Martinique",
            "U.S. Virgin Islands",
            "Anguilla",
            "Montserrat",
            "Guadeloupe",
            "Saint Lucia",
            "Brazil",
          ].includes(r.country)
      )
      .slice(0, 6)
      .map((r) => ({
        country: r.country,
        candidate_target: Math.min(100, Math.max(20, Math.ceil((r.gap || 40) * 0.35))),
        live: r.live,
        coverage: r.coverage,
        gap: r.gap,
      })),
    expected_raw_candidates: null,
    expected_READY: null,
    expected_REVIEW: null,
    expected_geographic_improvement:
      "Continue zero-floor lift on remaining CRITICAL countries; Brazil depth with discover+promote mode",
  };
  sprint02.expected_raw_candidates =
    sprint02.TRACK_A.candidate_target +
    sprint02.TRACK_B.reduce((s, t) => s + t.candidate_target, 0);
  sprint02.expected_READY = Math.round(sprint02.expected_raw_candidates * 0.35);
  sprint02.expected_REVIEW = Math.round(sprint02.expected_raw_candidates * 0.55);

  const operatingMode =
    totalPromoted >= 200 &&
    providerMetrics.promotions_existing_evidence_only >= totalPromoted * 0.7
      ? "DISCOVER_AND_PROMOTE"
      : trackBStats.promoted_ready < 50
        ? "FIX_REVIEW_LOGIC_FIRST"
        : "SCALE_DISCOVERY";

  const bottleneck = [
    "FIXABLE_EVIDENCE_GAP",
    "GEOGRAPHY_NORMALIZATION_PROBLEM",
    "CONFIDENCE_MODEL_WORKING_AS_DESIGNED",
  ];
  // soft_duplicate was a model bug — fixed; not "too conservative"
  if (providerMetrics.promotions_existing_evidence_only >= 200) {
    bottleneck.push("NORMAL_EXPECTED_REVIEW");
  }

  const finalVerdict =
    operatingMode === "FIX_REVIEW_LOGIC_FIRST"
      ? "REMEDIATE_REVIEW_SYSTEM_FIRST"
      : readyAfter.length >= 400
        ? "READY_FOR_IMPORT_REVIEW"
        : "SCALE_TO_SPRINT_02";

  const summary = {
    marker: "DEALALITY_CALA_REVIEW_PROMOTION_SPRINT_01_COMPLETE",
    safety,
    review_queue_baseline: {
      REVIEW_BEFORE: reviewHotels.length,
      TRACK_B_REVIEW: trackB.length,
      BRAZIL_REVIEW: brazil.length,
      sprint01_ready: sprint01Ready.length,
      merged_ready_before: readyBefore,
    },
    review_wall: reviewWall,
    confidence_bands_before: confBands,
    territory_normalization: {
      city_infer_version: CITY_INFER_VERSION,
      changes: [
        "Expanded CITY_CANONICAL for Track B + Brazil secondary markets",
        "CITY_IMPLIES_COUNTRY conflict guard (cross-island Cvent pollution)",
        "ISLAND_PRIMARY_LOCALITY for bare territory labels",
        "soft_duplicate_pressure only on near-dupe scores ≥0.55",
      ],
      systematic_issues_found: [
        "Unknown URL slug city_confidence 0.78 blocked Tier A (needed ≥0.85)",
        "soft_duplicate_pressure fired on all NEW with 3 weak pool rows",
        "Cvent cross-island city pollution (e.g. Willemstad on Bonaire, Castries on Martinique)",
      ],
    },
    track_b: trackBStats,
    brazil: brazilStats,
    track_b_geo: trackBGeo,
    geographic_coverage_impact: {
      ZERO_COVERAGE_COUNTRIES_LIVE: zeroLive,
      ZERO_COVERAGE_COUNTRIES_AFTER_EXISTING_READY: zeroAfterPriorReady,
      ZERO_COVERAGE_COUNTRIES_AFTER_REVIEW_PROMOTION: zeroAfterPriorReady,
      COUNTRIES_MOVING_ABOVE_20_PERCENT: countriesMovingAbove(20),
      COUNTRIES_MOVING_ABOVE_50_PERCENT: countriesMovingAbove(50),
      live_buckets: liveBuckets,
      projected_buckets: projectedBuckets,
    },
    promotion_metrics: {
      REVIEW_BEFORE: reviewHotels.length,
      PROMOTED_TO_READY: totalPromoted,
      STILL_REVIEW: stillReview,
      AMBIGUOUS: ambiguous,
      REJECTED: rejected,
      TRACK_B: {
        promoted: trackBStats.promoted_ready,
        still_review: trackBStats.still_review,
        promotion_rate:
          Math.round(
            (1000 * trackBStats.promoted_ready) / Math.max(1, trackB.length)
          ) / 10,
      },
      BRAZIL: {
        promoted: brazilStats.promoted_ready,
        still_review: brazilStats.still_review,
        promotion_rate:
          Math.round(
            (1000 * brazilStats.promoted_ready) / Math.max(1, brazil.length)
          ) / 10,
      },
      promotion_rate:
        Math.round((1000 * totalPromoted) / Math.max(1, reviewHotels.length)) /
        10,
    },
    provider_usage: {
      ...providerMetrics,
      serpapi_skipped: SKIP_SERPAPI,
      giata_skipped: SKIP_GIATA || !giataProvider,
      READY_promotions_per_external_API_call:
        externalCalls > 0
          ? Math.round(
              (1000 *
                (providerMetrics.promotions_requiring_giata +
                  providerMetrics.promotions_requiring_serpapi)) /
                externalCalls
            ) / 1000
          : null,
    },
    ready_queue: {
      READY_QUEUE_BEFORE: readyBefore,
      NEW_PROMOTIONS: allPromoted.length,
      NEW_PROMOTIONS_NET_AFTER_DEDUP: newPromotionsNet,
      DUPLICATES_WITHIN_READY_QUEUE: duplicatesWithin,
      READY_QUEUE_AFTER_DEDUPED: readyAfter.length,
    },
    projected_census: {
      LIVE_CENSUS: censusTotal,
      READY_QUEUE_AFTER_DEDUPED: readyAfter.length,
      PROJECTED_CENSUS_IF_READY_IMPORTED: projectedCensus,
      REMAINING_TO_10K: Math.max(0, 10000 - projectedCensus),
      REMAINING_TO_12_5K: Math.max(0, 12500 - projectedCensus),
      REMAINING_TO_15K: Math.max(0, 15000 - projectedCensus),
    },
    quality_audit: {
      promoted_track_b_sample: sample(trackBStats.promoted),
      promoted_brazil_sample: sample(brazilStats.promoted),
      still_review_sample: sample(allStill),
      rejected_ambiguous_sample: [
        ...trackBStats.ambiguous_rows.slice(0, 4),
        ...brazilStats.ambiguous_rows.slice(0, 4),
        ...trackBStats.rejected_rows.slice(0, 4),
      ],
      false_positive_concerns: [
        "Island-primary locality maps bare territory labels to commercial hubs — verify Montserrat/Brades and Turks/Providenciales samples",
        "GIATA name fuzzy match ≥0.72 — spot-check promoted GIATA path rows",
        "Census identity re-check skipped when --skip-census; default run uses live census",
      ],
    },
    review_bottleneck_verdict: bottleneck,
    recommended_operating_mode: operatingMode,
    sprint_02: sprint02,
    final_verdict: finalVerdict,
  };

  writeJson(path.join(OUT_DIR, "promotion-summary.json"), summary);
  writeJson(path.join(OUT_DIR, "track-b-results.json"), {
    ...trackBStats,
    promoted: undefined,
    still: undefined,
  });
  writeJson(path.join(OUT_DIR, "brazil-results.json"), {
    ...brazilStats,
    promoted: undefined,
    still: undefined,
  });

  const md = `# DEALALITY_CALA_REVIEW_PROMOTION_SPRINT_01_COMPLETE

## 1. Safety

\`\`\`text
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=${safety.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES}
ENABLE_HBX_CENSUS_WRITES=${safety.ENABLE_HBX_CENSUS_WRITES}
Production writes: 0
Census writes: 0
Automatic merges: 0
Automatic imports: 0
\`\`\`

## 2. Review Queue Baseline

| Cohort | Count |
| --- | ---: |
| REVIEW_BEFORE | ${reviewHotels.length} |
| TRACK_B_REVIEW | ${trackB.length} |
| BRAZIL_REVIEW | ${brazil.length} |
| Merged READY before | ${readyBefore} |

## 3. Review-Wall Diagnosis

| Reason | Count | % | Avg conf | Avg gap to 0.90 |
| --- | ---: | ---: | ---: | ---: |
${reviewWall
  .map(
    (r) =>
      `| ${r.REVIEW_REASON} | ${r.COUNT} | ${r.PCT_OF_REVIEW}% | ${r.AVERAGE_CONFIDENCE} | ${r.AVERAGE_GAP_TO_READY} |`
  )
  .join("\n")}

## 4. Confidence Distribution (before)

${Object.entries(confBands)
  .map(([k, v]) => `- ${k}: **${v}**`)
  .join("\n")}

## 5. Territory Normalization Audit

Version: \`${CITY_INFER_VERSION}\`

Fixes (smallest reusable rules — no manual score bumps):
${summary.territory_normalization.changes.map((c) => `- ${c}`).join("\n")}

Systematic issues:
${summary.territory_normalization.systematic_issues_found.map((c) => `- ${c}`).join("\n")}

## 6. Track B Promotion Results

| Country | Before | Promoted | Still REVIEW | Avg conf before → after |
| --- | ---: | ---: | ---: | --- |
${Object.entries(trackBStats.by_country)
  .map(
    ([c, r]) =>
      `| ${c} | ${r.review_before} | ${r.promoted_ready} | ${r.still_review} | ${r.average_confidence_before} → ${r.average_confidence_after} |`
  )
  .join("\n")}

**Track B totals:** promoted **${trackBStats.promoted_ready}** / ${trackB.length} (${summary.promotion_metrics.TRACK_B.promotion_rate}%)

## 7. Geographic Coverage Impact

| Metric | Value |
| --- | --- |
| ZERO_COVERAGE_COUNTRIES_LIVE | ${zeroLive} |
| ZERO_COVERAGE_COUNTRIES_AFTER_DEDUPED_READY | ${zeroAfterPriorReady} |
| Moving above 20% | ${summary.geographic_coverage_impact.COUNTRIES_MOVING_ABOVE_20_PERCENT.map((x) => x.country).join(", ") || "—"} |
| Moving above 50% | ${summary.geographic_coverage_impact.COUNTRIES_MOVING_ABOVE_50_PERCENT.map((x) => x.country).join(", ") || "—"} |

## 8. Brazil Promotion Results

- Promoted: **${brazilStats.promoted_ready}** / ${brazil.length} (${summary.promotion_metrics.BRAZIL.promotion_rate}%)
- Still REVIEW: ${brazilStats.still_review}
- Avg confidence: ${brazilStats.avg_conf_before} → ${brazilStats.avg_conf_after}
- SerpApi calls: ${brazilStats.serpapi_calls} (skipped=${SKIP_SERPAPI})

## 9. Provider Usage

\`\`\`text
GIATA Drive calls: ${providerMetrics.giata_drive_calls}
GIATA matches: ${providerMetrics.giata_matches}
SerpApi calls: ${providerMetrics.serpapi_calls}
SerpApi useful: ${providerMetrics.serpapi_useful}
Promotions existing evidence only: ${providerMetrics.promotions_existing_evidence_only}
Promotions requiring GIATA: ${providerMetrics.promotions_requiring_giata}
Promotions requiring SerpApi: ${providerMetrics.promotions_requiring_serpapi}
\`\`\`

## 10. Promotion Efficiency

- Overall promotion rate: **${summary.promotion_metrics.promotion_rate}%**
- READY per external API call: ${summary.provider_usage.READY_promotions_per_external_API_call ?? "n/a (almost all existing-evidence)"}

## 11. Quality Audit

See \`promotion-summary.json\` → \`quality_audit\`. Spot-check island-primary and any GIATA-path promotions.

## 12. READY Queue

\`\`\`text
Before: ${readyBefore}
New promotions: ${allPromoted.length}
Duplicates removed: ${duplicatesWithin}
After: ${readyAfter.length}
\`\`\`

## 13. Projected Census

\`\`\`text
LIVE: ${censusTotal}
PROJECTED: ${projectedCensus}
Remaining to 10K: ${summary.projected_census.REMAINING_TO_10K}
Remaining to 12.5K: ${summary.projected_census.REMAINING_TO_12_5K}
Remaining to 15K: ${summary.projected_census.REMAINING_TO_15K}
\`\`\`

## 14. Updated Coverage Dashboard

Regenerated under \`reports/hotel-intelligence/cala-coverage-dashboard-v1/\` (live + projected-with-all-deduped-ready).

| | LIVE | PROJECTED_WITH_ALL_DEDUPED_READY |
| --- | ---: | ---: |
| Hotels | ${censusTotal} | ${projectedCensus} |
| Zero coverage countries | ${liveBuckets.zero} | ${projectedBuckets.zero} |
| <20% | ${liveBuckets.below_20} | ${projectedBuckets.below_20} |
| <50% | ${liveBuckets.below_50} | ${projectedBuckets.below_50} |
| <80% | ${liveBuckets.below_80} | ${projectedBuckets.below_80} |
| ≥95% | ${liveBuckets.at_or_above_95} | ${projectedBuckets.at_or_above_95} |

## 15. Review Bottleneck Verdict

${bottleneck.map((b) => `- \`${b}\``).join("\n")}

## 16. Recommended Operating Mode

\`\`\`text
${operatingMode}
\`\`\`

## 17. SPRINT 02 (recommendation only — not executed)

**TRACK A:** ${sprint02.TRACK_A.country} ×${sprint02.TRACK_A.candidate_target}

**TRACK B:**
${sprint02.TRACK_B.map((t) => `- ${t.country} ×${t.candidate_target}`).join("\n")}

Expected raw ~${sprint02.expected_raw_candidates}, READY ~${sprint02.expected_READY}, REVIEW ~${sprint02.expected_REVIEW}

## 18. Final Verdict

\`\`\`text
${finalVerdict}
\`\`\`

Do NOT import. Do NOT execute Sprint 02.
`;

  fs.writeFileSync(
    path.join(OUT_DIR, "DEALALITY_CALA_REVIEW_PROMOTION_SPRINT_01_COMPLETE.md"),
    md,
    "utf8"
  );

  console.log(
    JSON.stringify({
      module: "cala-review-promotion-sprint-01",
      event: "complete",
      promoted: totalPromoted,
      still_review: stillReview,
      ready_queue_after: readyAfter.length,
      projected_census: projectedCensus,
      operating_mode: operatingMode,
      final_verdict: finalVerdict,
    })
  );
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      module: "cala-review-promotion-sprint-01",
      event: "fatal",
      error: String(err?.message || err),
    })
  );
  process.exitCode = 1;
});
