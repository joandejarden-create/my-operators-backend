#!/usr/bin/env node
/**
 * High-only enrichment for DR coverage-insert HPC rows
 * (pending enrichment / ind_*_do_* identity keys — not OSM dual-lane).
 *
 * Writes (blank-fill only, good evidence only):
 * - Market / Submarket (Dealality High corridor)
 * - Address / Phone from Official Property URL (JSON-LD / tel:) then Google Places High
 * - Rooms / Keys from Official Property URL (JSON-LD + Level-2 High extractor)
 * - Lat/Long via Mapbox Permanent Geocoding of High street Address
 *   (MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1). Never Google coords.
 *
 * Never invents Rooms. Never writes Rooms from Google. Never overwrites non-blank.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import * as cheerio from "cheerio";
import { proposeCensusSubmarketCorridor } from "../lib/hotel-census/census-dealality-submarket.js";
import { resolveMarketFromCity } from "../lib/research-engine-v2/census-region-market-map.js";
import { extractOfficialRoomsFromHtml } from "../lib/research-engine-v2/census-level-2-parent-extractors.js";
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";
import { isBrandHomepageOfficialUrl } from "../lib/independent-census/official-property-url-quality.js";
import { lookupHotelOfficialUrlWithGoogle } from "../lib/independent-census/google-places-hotel-url-lookup.js";
import { resolveGoogleApiKey } from "../lib/location-verification/google-api-config.js";
import {
  resolveMapboxCoordinates,
  MAPBOX_COORDINATE_STATUSES,
} from "../lib/research-engine-v2/census-mapbox-coordinate-provider.js";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const USER_AGENT = "DealalityCensusCoverageEnrichment/1.0 (research; dry-run)";
const FETCH_TIMEOUT_MS = 25000;

const STATE_BY_CITY = Object.freeze({
  "Punta Cana": "La Altagracia",
  "Cap Cana": "La Altagracia",
  "La Romana": "La Romana",
  "Santo Domingo": "Distrito Nacional",
  "Las Terrenas": "Samaná",
  Miches: "El Seibo",
  Bayahibe: "La Romana",
  Santiago: "Santiago",
  "Puerto Plata": "Puerto Plata",
  "Uvero Alto": "La Altagracia",
  Bávaro: "La Altagracia",
  Bavaro: "La Altagracia",
});

/** Approximate city centers for Mapbox proximity bias only (never written as coords). */
const DR_CITY_PROXIMITY = Object.freeze({
  "punta cana": { latitude: 18.5601, longitude: -68.3725 },
  "cap cana": { latitude: 18.468, longitude: -68.405 },
  bavaro: { latitude: 18.685, longitude: -68.45 },
  bávaro: { latitude: 18.685, longitude: -68.45 },
  "uvero alto": { latitude: 18.78, longitude: -68.58 },
  miches: { latitude: 18.983, longitude: -69.047 },
  "santo domingo": { latitude: 18.4861, longitude: -69.9312 },
  santiago: { latitude: 19.4517, longitude: -70.697 },
  "la romana": { latitude: 18.4273, longitude: -68.9728 },
  "las terrenas": { latitude: 19.311, longitude: -69.543 },
  bayahibe: { latitude: 18.366, longitude: -68.843 },
});

function proximityForCity(city) {
  const key = String(city || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
  return DR_CITY_PROXIMITY[key] || DR_CITY_PROXIMITY[String(city || "").toLowerCase().trim()] || null;
}

const READ_FIELDS = [
  "Property Name",
  "Current Brand",
  "City",
  "Country",
  "State / Region",
  "Market",
  "Submarket",
  "Address",
  "Phone",
  "Rooms / Keys",
  "Latitude",
  "Longitude",
  "Official Property URL",
  "Property Identity Key",
  "Enrichment Status",
];

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    limit: Number(get("--limit", "60")) || 60,
    delayMs: Number(get("--delay-ms", "600")) || 600,
    skipPlaces: argv.includes("--skip-places"),
    skipOfficialFetch: argv.includes("--skip-official-fetch"),
    /** Only Mapbox coords + geo normalize (no official/Places contact fetch). */
    coordsOnly: argv.includes("--coords-only"),
    /** Broader: also Enrichment Status contains Discovered (default: identity-key cohort only). */
    includeDiscovered: argv.includes("--include-discovered"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

/** Strong DR corridor cues from name/address that beat a wrong City label. */
const DR_CORRIDOR_OVERRIDES = Object.freeze([
  {
    re: /\bmiches\b/i,
    city: "Miches",
    market: "Punta Cana",
    submarket: "Miches / Costa Esmeralda",
  },
  {
    re: /\bcap\s*cana\b/i,
    city: "Cap Cana",
    market: "Punta Cana",
    submarket: "Punta Cana / Bávaro / Cap Cana",
  },
  {
    re: /\bbayah[ií]be\b/i,
    city: "Bayahibe",
    market: "La Romana",
    submarket: "La Romana / Bayahibe",
  },
  {
    re: /\blas\s+terrenas\b/i,
    city: "Las Terrenas",
    market: "Samaná",
    submarket: "Samaná / Las Terrenas",
  },
  {
    re: /\b(b[aá]varo|uvero\s+alto|macao)\b/i,
    city: null,
    market: "Punta Cana",
    submarket: "Punta Cana / Bávaro / Cap Cana",
  },
]);

/** Reject city-only / vague locality strings. */
function isUsableStreetAddress(address) {
  const a = String(address || "").trim();
  if (!isStreetLevelAddress(a)) return false;
  // Google Plus Codes alone are not street addresses for census.
  if (/^[A-Z0-9]{4,}\+[A-Z0-9]{2,}\b/i.test(a) && !/\b(calle|av\.?|carr|km|blvd)\b/i.test(a)) {
    return false;
  }
  if (/^(playa de|dominicus|bayahibe)\b/i.test(a) && !/\b(calle|av|carr|km)\b/i.test(a)) {
    return false;
  }
  if (
    !/\b(calle|c\.|av\.?|ave|avenue|carr\.?|carretera|blvd|boulevard|street|st\.|road|rd\.|km|plaza|paseo|highway|hotel|chalet)\b/i.test(
      a
    ) &&
    !/\d+\s*[ªºnN°]?\s*[A-Za-záéíóúñ]/i.test(a)
  ) {
    return false;
  }
  return true;
}

function parseJsonLd(html) {
  const blocks = [];
  const re =
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html))) {
    try {
      blocks.push(JSON.parse(m[1].trim()));
    } catch {
      /* ignore malformed JSON-LD */
    }
  }
  return blocks;
}

function walkLd(node, out = []) {
  if (!node) return out;
  if (Array.isArray(node)) {
    for (const x of node) walkLd(x, out);
    return out;
  }
  if (typeof node === "object") {
    out.push(node);
    if (node["@graph"]) walkLd(node["@graph"], out);
  }
  return out;
}

function extractFromJsonLd(blocks) {
  const nodes = [];
  for (const b of blocks) walkLd(b, nodes);
  let address = "";
  let phone = "";
  let rooms = null;
  let lat = null;
  let lng = null;
  for (const n of nodes) {
    const types = []
      .concat(n["@type"] || [])
      .map((t) => String(t).toLowerCase());
    const isHotel = types.some((t) =>
      /hotel|lodging|resort|motel|guesthouse/.test(t)
    );
    if (
      !isHotel &&
      !n.address &&
      n.telephone == null &&
      n.numberOfRooms == null &&
      !n.geo
    ) {
      continue;
    }
    if (!address && n.address) {
      if (typeof n.address === "string") address = n.address;
      else if (typeof n.address === "object") {
        address = [
          n.address.streetAddress,
          n.address.addressLocality,
          n.address.addressRegion,
          n.address.postalCode,
          n.address.addressCountry,
        ]
          .filter(Boolean)
          .join(", ");
      }
    }
    if (!phone && (n.telephone || n.phone)) {
      phone = String(n.telephone || n.phone);
    }
    if (rooms == null && n.numberOfRooms != null) {
      const num = Number.parseInt(String(n.numberOfRooms), 10);
      if (Number.isFinite(num) && num >= 20 && num <= 2000) rooms = num;
    }
    const geo = n.geo || {};
    const gLat = Number(geo.latitude ?? geo["@latitude"]);
    const gLng = Number(geo.longitude ?? geo["@longitude"]);
    if (lat == null && Number.isFinite(gLat) && Number.isFinite(gLng)) {
      lat = gLat;
      lng = gLng;
    }
  }
  return {
    address: address.trim(),
    phone: phone.trim(),
    rooms,
    latitude: lat,
    longitude: lng,
  };
}

async function fetchOfficialMeta(url, attempt = 1) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (compatible; DealalityCensusBot/1.0; +https://dealality.com; research)",
        Accept: "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
    });
    if ((res.status === 429 || res.status === 403) && attempt < 3) {
      await sleep(1200 * attempt);
      return fetchOfficialMeta(url, attempt + 1);
    }
    if (!res.ok) return { ok: false, reason: `http_${res.status}` };
    const html = await res.text();
    const finalUrl = String(res.url || url);
    const fromLd = extractFromJsonLd(parseJsonLd(html));
    const $ = cheerio.load(html);
    if (!fromLd.phone) {
      const tel = $('a[href^="tel:"]').first().attr("href") || "";
      if (tel) fromLd.phone = tel.replace(/^tel:/i, "").trim();
    }
    let rooms = fromLd.rooms;
    if (rooms == null) {
      const extracted = extractOfficialRoomsFromHtml(html, finalUrl);
      if (extracted.ok) rooms = extracted.rooms;
    }
    return { ok: true, ...fromLd, rooms, final_url: finalUrl, html_len: html.length };
  } catch (err) {
    if (attempt < 3) {
      await sleep(800 * attempt);
      return fetchOfficialMeta(url, attempt + 1);
    }
    return { ok: false, reason: err.message || "fetch_failed" };
  } finally {
    clearTimeout(t);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function listCoveragePending(baseId, token, { includeDiscovered = false } = {}) {
  const identityOr = [
    "FIND('ind_bahia_do_',{Property Identity Key}&'')",
    "FIND('ind_hyatt_do_',{Property Identity Key}&'')",
    "FIND('ind_barcelo_do_',{Property Identity Key}&'')",
    "FIND('ind_hodelpa_do_',{Property Identity Key}&'')",
    "FIND('ind_marriott_do_',{Property Identity Key}&'')",
  ];
  if (includeDiscovered) {
    identityOr.unshift("FIND('Discovered',{Enrichment Status}&'')");
  }
  const formula = `AND({Country}='Dominican Republic',OR(${identityOr.join(",")}))`;
  const out = [];
  let offset;
  do {
    const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
    for (const f of READ_FIELDS) p.append("fields[]", f);
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function patchRecords(baseId, token, records) {
  const updated = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    updated.push(...(json.records || []));
  }
  return updated;
}

function findCorridorOverride(haystack) {
  const text = String(haystack || "");
  for (const rule of DR_CORRIDOR_OVERRIDES) {
    if (rule.re.test(text)) return rule;
  }
  return null;
}

function buildGeoPatch(fields, evidenceText = "") {
  /** @type {Record<string, unknown>} */
  const patch = {};
  const reasons = [];
  let city = String(fields.City || "").trim();
  const country = String(fields.Country || "Dominican Republic").trim();
  const name = String(fields["Property Name"] || "").trim();
  const hay = [name, fields.Address || "", evidenceText].filter(Boolean).join(" | ");

  // Correct City/Market/Submarket when name/address proves a stronger corridor (e.g. Miches mislabeled as Punta Cana).
  const override = findCorridorOverride(hay);
  if (override) {
    if (override.city && city && normCity(city) !== normCity(override.city)) {
      // Only correct when current city is a broad metro label that often swallows Miches/Cap Cana.
      if (/^(punta\s*cana|santo\s*domingo|unknown)$/i.test(city) || override.city === "Miches") {
        patch.City = override.city;
        city = override.city;
        reasons.push(`city_corrected_from_evidence_${normCity(override.city)}`);
      }
    }
    if (!String(fields.Market || "").trim() || patch.City) {
      patch.Market = override.market;
      reasons.push("market_from_corridor_override");
    }
    if (!String(fields.Submarket || "").trim() || patch.City) {
      patch.Submarket = override.submarket;
      reasons.push("submarket_from_corridor_override");
    }
  }

  if (!String(fields["State / Region"] || "").trim() && STATE_BY_CITY[city]) {
    patch["State / Region"] = STATE_BY_CITY[city];
    reasons.push("state_from_city_map");
  } else if (patch.City && STATE_BY_CITY[patch.City]) {
    patch["State / Region"] = STATE_BY_CITY[patch.City];
    reasons.push("state_from_corrected_city");
  }

  let market = String(patch.Market || fields.Market || "").trim();
  if (!market && city && !/^unknown$/i.test(city)) {
    const m = resolveMarketFromCity({ city, country });
    if (m.ok && m.market) {
      market = m.market;
      patch.Market = m.market;
      reasons.push(`market_${m.method}`);
    }
  }

  if (!String(patch.Submarket || fields.Submarket || "").trim() && city) {
    const sub = proposeCensusSubmarketCorridor(
      {
        country,
        city,
        market,
        Market: market || fields.Market || "",
        Submarket: "",
        name,
      },
      { minConfidence: "High", normalizeLabels: true }
    );
    const conf = String(sub?.confidence || "").toLowerCase();
    if (sub?.submarket && !sub.skipped && conf === "high" && sub.submarket !== "Other") {
      patch.Submarket = sub.submarket;
      reasons.push(`submarket_${sub.reason || sub.source}_high`);
    }
  }

  // Normalize legacy Cap Cana label → Dealality corridor.
  const currentSub = String(patch.Submarket || fields.Submarket || "").trim();
  if (/^cap\s*cana$/i.test(currentSub)) {
    patch.Submarket = "Punta Cana / Bávaro / Cap Cana";
    reasons.push("submarket_normalized_cap_cana_corridor");
  }

  return { patch, reasons };
}

function normCity(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

async function main() {
  const args = parseArgs();
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error(JSON.stringify({ ok: false, blocked: "wrong_write_target" }));
    process.exit(1);
  }

  const allowPlacesCoords = false; // Census stored coords: Mapbox permanent only (not Google Places).
  const apiKey = resolveGoogleApiKey();
  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const rows = await listCoveragePending(baseId, token, {
    includeDiscovered: args.includeDiscovered,
  });

  if (args.coordsOnly) {
    args.skipOfficialFetch = true;
    args.skipPlaces = true;
  }

  const proposals = [];
  const steward = [];
  const fieldHits = {
    City: 0,
    Market: 0,
    Submarket: 0,
    "State / Region": 0,
    Address: 0,
    Phone: 0,
    "Rooms / Keys": 0,
    Latitude: 0,
  };

  let officialFetches = 0;
  let placesLookups = 0;
  let mapboxLookups = 0;
  // Prefer recently inserted resort-gap / Hyatt / Bahia keys first.
  const ranked = [...rows].sort((a, b) => {
    const ka = String(a.fields?.["Property Identity Key"] || "");
    const kb = String(b.fields?.["Property Identity Key"] || "");
    const score = (k) =>
      /ind_(bahia|hyatt|barcelo|hodelpa)_do_/.test(k) ? 0 : /ind_marriott_do_/.test(k) ? 1 : 2;
    return score(ka) - score(kb);
  });
  const limited = ranked.slice(0, args.limit);

  for (const rec of limited) {
    const f = rec.fields || {};
    /** @type {Record<string, unknown>} */
    const patch = {};
    const reasons = [];

    const existingAddrUsable = isUsableStreetAddress(f.Address || "");
    const needAddr = !existingAddrUsable;
    const needPhone = !String(f.Phone || "").trim();
    const needRooms = f["Rooms / Keys"] == null || f["Rooms / Keys"] === "";
    const needCoords =
      f.Latitude == null ||
      f.Latitude === "" ||
      f.Longitude == null ||
      f.Longitude === "";
    if (String(f.Address || "").trim() && !existingAddrUsable) {
      reasons.push("address_existing_rejected_not_street_level");
    }

    // --coords-only: skip contact enrichment; only Mapbox when street address already High.
    if (args.coordsOnly && (!needCoords || !existingAddrUsable)) {
      if (needCoords && !existingAddrUsable) {
        steward.push({
          id: rec.id,
          name: f["Property Name"],
          identity_key: f["Property Identity Key"],
          city: f.City || null,
          reasons: ["coords_waiting_for_street_address"],
        });
      }
      continue;
    }

    const url = String(f["Official Property URL"] || "").trim();
    let officialMeta = null;
    if (
      !args.skipOfficialFetch &&
      url &&
      !isBrandHomepageOfficialUrl(url) &&
      (needAddr || needPhone || needRooms || needCoords)
    ) {
      officialMeta = await fetchOfficialMeta(url);
      officialFetches += 1;
      await sleep(args.delayMs);
      if (officialMeta.ok) {
        if (needAddr && isUsableStreetAddress(officialMeta.address)) {
          patch.Address = officialMeta.address;
          patch["Address Confidence"] = "High";
          patch["Address Source URL"] = officialMeta.final_url || url;
          reasons.push("address_from_official_jsonld");
        }
        if (needPhone && officialMeta.phone) {
          patch.Phone = officialMeta.phone;
          reasons.push("phone_from_official_page");
        }
        if (needRooms && officialMeta.rooms != null) {
          const rooms = Number(officialMeta.rooms);
          // Large branded resorts almost never have <80 keys; treat as extractor FP.
          const looksLarge =
            /\b(resort|palace|secrets|dreams|bah[ií]a|embajador|ziva|zilara|vivid|breathless|sunscape|zo[eë]try)\b/i.test(
              String(f["Property Name"] || "")
            );
          if (looksLarge && rooms < 80) {
            reasons.push(`rooms_steward_blocked_suspicious_low_${rooms}`);
          } else {
            patch["Rooms / Keys"] = rooms;
            patch["Rooms Confidence"] = "High";
            patch["Rooms Source URL"] = officialMeta.final_url || url;
            patch["Rooms Source Type"] = "official_property_page";
            patch["Rooms Reviewed Date"] = todayIsoDate();
            reasons.push("rooms_from_official_high");
          }
        }
        if (
          needCoords &&
          Number.isFinite(officialMeta.latitude) &&
          Number.isFinite(officialMeta.longitude)
        ) {
          patch.Latitude = officialMeta.latitude;
          patch.Longitude = officialMeta.longitude;
          patch["Coordinate Source Type"] = "structured_data_extraction";
          patch["Coordinate Confidence"] = "High";
          patch["Geocode Provider"] = "Official Page";
          patch["Geocode Method"] = "structured_data_extraction";
          reasons.push("coords_from_official_jsonld_geo");
        }
      } else {
        reasons.push(`official_fetch_${officialMeta.reason || "failed"}`);
      }
    }

    const stillNeedAddr = needAddr && !patch.Address;
    const stillNeedPhone = needPhone && !patch.Phone;
    let placesAddressEvidence = "";

    if (!args.skipPlaces && apiKey && (stillNeedAddr || stillNeedPhone)) {
      const places = await lookupHotelOfficialUrlWithGoogle(
        {
          property_name: f["Property Name"],
          current_brand: f["Current Brand"],
          city: f.City,
          country: f.Country || "Dominican Republic",
          source_record_id: f["Property Identity Key"],
        },
        { apiKey, maxResults: 5 }
      );
      placesLookups += 1;
      await sleep(args.delayMs);

      const gConf = String(places?.match_confidence || "").toLowerCase();
      // High preferred; Medium OK only for usable street addresses that include city cue.
      const gOk = gConf === "high" || gConf === "medium";
      if (places?.status === "matched" && gOk && places.place) {
        placesAddressEvidence = places.place.google_formatted_address || "";
        const cityCue = String(f.City || "")
          .toLowerCase()
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "");
        const addrCue = placesAddressEvidence
          .toLowerCase()
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "");
        const cityInAddr =
          !cityCue ||
          addrCue.includes(cityCue) ||
          (cityCue === "cap cana" && addrCue.includes("punta cana")) ||
          (cityCue === "uvero alto" &&
            (addrCue.includes("uvero") || addrCue.includes("punta cana")));
        if (
          stillNeedAddr &&
          isUsableStreetAddress(placesAddressEvidence) &&
          cityInAddr &&
          (gConf === "high" || gConf === "medium")
        ) {
          patch.Address = placesAddressEvidence;
          patch["Address Confidence"] = gConf === "high" ? "High" : "Medium";
          patch["Address Source URL"] =
            places.place.google_maps_uri || places.place.google_website_uri || "";
          reasons.push(`address_from_google_places_${gConf}`);
        }
        if (stillNeedPhone && places.place.google_phone && gConf === "high") {
          patch.Phone = places.place.google_phone;
          reasons.push("phone_from_google_places_high");
        } else if (stillNeedPhone && places.place.google_phone && gConf === "medium" && patch.Address) {
          patch.Phone = places.place.google_phone;
          reasons.push("phone_from_google_places_medium_with_address");
        }
        // Never store Google Places lat/lng — Mapbox permanent is the census coordinate path.
      } else if (places?.status === "matched") {
        reasons.push(`places_match_${gConf || "low"}_skipped`);
      } else {
        reasons.push(`places_${places?.status || "no_result"}`);
      }
    } else if (!apiKey && (stillNeedAddr || stillNeedPhone)) {
      reasons.push("places_skipped_no_api_key");
    }

    // Geography after address evidence so Miches/Cap Cana overrides can use Places address.
    const geo = buildGeoPatch(
      { ...f, Address: patch.Address || f.Address },
      placesAddressEvidence || patch.Address || ""
    );
    Object.assign(patch, geo.patch);
    reasons.push(...geo.reasons);

    // Clear Plus-code / non-street addresses when no better replacement exists.
    if (
      String(f.Address || "").trim() &&
      !existingAddrUsable &&
      !patch.Address
    ) {
      patch.Address = null;
      patch["Address Confidence"] = null;
      patch["Address Source URL"] = null;
      reasons.push("address_cleared_non_street_level");
    }

    // Mapbox Permanent Geocoding for blank coords when we have a street-level address.
    const addressForGeocode = String(patch.Address || f.Address || "").trim();
    const cityForGeocode = String(patch.City || f.City || "").trim();
    const stillNeedCoords = needCoords && patch.Latitude == null;
    if (stillNeedCoords && isUsableStreetAddress(addressForGeocode) && cityForGeocode) {
      const mbInput = {
        propertyName: f["Property Name"],
        brand: f["Current Brand"],
        address: addressForGeocode,
        city: cityForGeocode,
        stateRegion: patch["State / Region"] || f["State / Region"],
        country: f.Country || "Dominican Republic",
        sourceUrl: patch["Address Source URL"] || f["Official Property URL"],
      };
      const proximity = proximityForCity(cityForGeocode);
      const attempts = [
        { proximity },
        { proximity, omitPropertyName: true },
        // State/region tokens often lower Mapbox relevance on correct DR streets.
        { proximity, omitPropertyName: true, dropState: true },
        { proximity, types: "poi,address", allowPoi: true, minRelevance: 0.8 },
        {
          proximity,
          omitPropertyName: true,
          types: "poi,address",
          allowPoi: true,
          minRelevance: 0.8,
        },
      ];
      let mb = null;
      for (const attempt of attempts) {
        const inputAttempt = attempt.dropState
          ? { ...mbInput, stateRegion: undefined }
          : mbInput;
        const { dropState: _drop, ...mbOpts } = attempt;
        mb = await resolveMapboxCoordinates(inputAttempt, mbOpts);
        mapboxLookups += 1;
        if (mb.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) break;
      }
      await sleep(Math.min(args.delayMs, 250));
      if (mb?.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) {
        patch.Latitude = mb.latitude;
        patch.Longitude = mb.longitude;
        patch["Coordinate Source Type"] = "official_address_geocode";
        patch["Coordinate Confidence"] = "High";
        patch["Geocode Provider"] = "Mapbox";
        patch["Geocode Method"] =
          mb.geocode_method || "permanent_geocoding_official_address";
        patch["Geocode Reviewed Date"] = todayIsoDate();
        reasons.push(`coords_from_mapbox_permanent_high:${mb.reason || "ok"}`);
      } else {
        reasons.push(`mapbox_${mb?.status || "unresolved"}:${mb?.reason || ""}`);
      }
    } else if (stillNeedCoords) {
      reasons.push("coords_waiting_for_street_address");
    }

    if (Object.keys(patch).length) {
      patch["Last Reviewed Date"] = todayIsoDate();
      for (const k of Object.keys(fieldHits)) {
        if (patch[k] != null) fieldHits[k]++;
      }
      proposals.push({
        id: rec.id,
        property_name: f["Property Name"],
        identity_key: f["Property Identity Key"],
        city: f.City || null,
        patch,
        reasons,
        remaining_blank: {
          address: needAddr && !patch.Address,
          phone: needPhone && !patch.Phone,
          rooms: needRooms && patch["Rooms / Keys"] == null,
          coords: needCoords && patch.Latitude == null,
          submarket: !String(f.Submarket || "").trim() && !patch.Submarket,
        },
      });
    } else {
      steward.push({
        id: rec.id,
        name: f["Property Name"],
        identity_key: f["Property Identity Key"],
        city: f.City || null,
        reasons,
      });
    }
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);
  let patched = [];
  if (doWrite && proposals.length) {
    patched = await patchRecords(
      baseId,
      token,
      proposals.map((p) => ({ id: p.id, fields: p.patch }))
    );
  }

  const report = {
    status: doWrite ? "applied" : "dry_run",
    hard_rule:
      "High-only Submarket + official/Places contact evidence; lat/lng via Mapbox permanent only (never Google coords); never invent Rooms",
    generated_at: new Date().toISOString(),
    scanned: rows.length,
    processed: limited.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    steward_count: steward.length,
    field_hits: fieldHits,
    official_fetches: officialFetches,
    places_lookups: placesLookups,
    mapbox_lookups: mapboxLookups,
    coords_only: Boolean(args.coordsOnly),
    airtable_writes: doWrite,
    field_mapping: {
      Market: "resolveMarketFromCity High",
      Submarket: "proposeCensusSubmarketCorridor minConfidence=High",
      Address: "Official JSON-LD → Google Places High street-level (contact only)",
      Phone: "Official tel/JSON-LD → Google Places High",
      "Rooms / Keys": "Official JSON-LD / Level-2 High only",
      Latitude: "Mapbox permanent geocode of High street Address",
    },
    proposals,
    steward: steward.slice(0, 40),
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-dr-coverage-pending-enrichment-applied.json"
    : "reports/census-dr-coverage-pending-enrichment-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));

  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        scanned: report.scanned,
        proposal_count: report.proposal_count,
        field_hits: report.field_hits,
        official_fetches: report.official_fetches,
        places_lookups: report.places_lookups,
        mapbox_lookups: report.mapbox_lookups,
        coords_only: report.coords_only,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 8).map((p) => ({
          n: p.property_name,
          reasons: p.reasons,
          patch_keys: Object.keys(p.patch),
          remaining: p.remaining_blank,
        })),
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
