/**
 * DR OSM HPC enrichment remediation:
 * - Continent / Sub-Continent / Market / Submarket (Dealality)
 * - Address / Phone from OSM + Google Places (High only)
 * - Rooms / Keys from OSM rooms tag (High, >0)
 * - Brand-homepage Official Property URL → property page
 */

import {
  resolveContinentSubContinentFromCountry,
  resolveMarketFromCity,
} from "../research-engine-v2/census-region-market-map.js";
import { proposeCensusSubmarketCorridor } from "../hotel-census/census-dealality-submarket.js";
import {
  isBrandHomepageOfficialUrl,
  classifyOfficialPropertyUrl,
} from "./official-property-url-quality.js";
import { sanitizeOfficialUrlCandidate } from "./known-chain-official-url-enrichment.js";

/** Identity key → property-specific Official URL (never brand homepage). */
export const DR_OSM_PROPERTY_URL_FIXES = Object.freeze({
  osm_do_node_4901959222:
    "https://www.riu.com/en/hotel/dominican-republic/punta-cana/hotel-riu-republica",
  osm_do_way_312593390:
    "https://www.hodelpa.com/hodelpa-garden-court.html",
  osm_do_way_310075723:
    "https://www.sirenishotels.com/en/hotels/grand-sirenis-punta-cana-resort-casino/",
  osm_do_node_431025349:
    "https://www.barcelo.com/en-us/barcelo-bavaro-palace/",
  osm_do_way_255806141:
    "https://www.barcelo.com/en-us/barcelo-dominican-beach/",
  osm_do_node_302800763:
    "https://www.wyndhamhotels.com/alltra/punta-cana-dominican-republic/wyndham-alltra-punta-cana/overview",
  osm_do_node_5181801658:
    "https://www.amhsamarina.com/grand-paradise-playa-dorada",
  osm_do_node_302771731:
    "https://www.blaunaturapark.com/en/",
});

/**
 * @param {string} identityKey osm_do_node_123
 * @returns {string} node/123 | way/123
 */
export function identityKeyToOsmSourceId(identityKey) {
  const m = String(identityKey || "").match(/^osm_do_(node|way|relation)_(\d+)$/i);
  if (!m) return "";
  return `${m[1].toLowerCase()}/${m[2]}`;
}

/**
 * @param {object} fields HPC fields
 * @param {{ osmBySourceId?: Map, googleBySourceId?: Map }} sources
 */
export function buildDrOsmFieldEnrichmentProposal(fields, sources = {}) {
  const osmById = sources.osmBySourceId || new Map();
  const googleById = sources.googleBySourceId || new Map();
  const key = String(fields["Property Identity Key"] || "").trim();
  const sid = identityKeyToOsmSourceId(key);
  const osm = osmById.get(sid) || {};
  const google = googleById.get(sid) || {};

  /** @type {Record<string, unknown>} */
  const patch = {};
  const reasons = [];

  const country = String(fields.Country || "Dominican Republic").trim();
  const city = String(fields.City || "").trim();

  // Continent / Sub-Continent
  const cs = resolveContinentSubContinentFromCountry(country);
  if (cs) {
    if (!String(fields.Continent || "").trim()) {
      patch.Continent = cs.continent;
      reasons.push("continent_from_country_map");
    }
    if (!String(fields["Sub-Continent"] || "").trim()) {
      patch["Sub-Continent"] = cs.subContinent;
      reasons.push("subcontinent_from_country_map");
    }
  }

  // Market
  if (!String(fields.Market || "").trim() && city && !/^unknown$/i.test(city)) {
    const m = resolveMarketFromCity({ city, country });
    if (m.ok && m.market) {
      patch.Market = m.market;
      reasons.push(`market_${m.method}`);
    }
  }

  // Submarket — Dealality corridor (High only)
  if (!String(fields.Submarket || "").trim() && city) {
    const sub = proposeCensusSubmarketCorridor(
      {
        country,
        city,
        Market: patch.Market || fields.Market || "",
        market: patch.Market || fields.Market || "",
        Submarket: "",
        name: fields["Property Name"] || "",
      },
      { minConfidence: "High", normalizeLabels: true }
    );
    const conf = String(sub?.confidence || "").toLowerCase();
    if (sub?.submarket && !sub.skipped && (conf === "high" || conf === "medium")) {
      // Prefer High; accept Medium corridor inference for DR OSM blank fill
      if (conf === "high" || conf === "medium") {
        patch.Submarket = sub.submarket;
        reasons.push(`submarket_${sub.source || sub.reason || "dealality_corridor"}`);
      }
    }
  }

  // Address — OSM first, else Google formatted (High match only)
  if (!String(fields.Address || "").trim()) {
    const osmAddr = String(osm.rawAddress || "").trim();
    const gAddr = String(
      google.place?.google_formatted_address ||
        google.google_formatted_address ||
        ""
    ).trim();
    const gConf = String(google.match_confidence || "").toLowerCase();
    if (osmAddr) {
      patch.Address = osmAddr;
      reasons.push("address_from_osm");
    } else if (gAddr && (gConf === "high" || gConf === "medium")) {
      patch.Address = gAddr;
      reasons.push("address_from_google_places");
    }
  }

  // Phone — OSM first, else Google
  if (!String(fields.Phone || "").trim()) {
    const osmPhone = String(osm.rawPhone || "").trim();
    const gPhone = String(
      google.place?.google_phone || google.google_phone || ""
    ).trim();
    if (osmPhone) {
      patch.Phone = osmPhone;
      reasons.push("phone_from_osm");
    } else if (gPhone) {
      patch.Phone = gPhone;
      reasons.push("phone_from_google_places");
    }
  }

  // Rooms / Keys — OSM rooms tag only, positive integer
  if (fields["Rooms / Keys"] == null || fields["Rooms / Keys"] === "") {
    let roomsRaw = "";
    try {
      const payload =
        typeof osm.rawPayloadJson === "string"
          ? JSON.parse(osm.rawPayloadJson)
          : osm.rawPayloadJson || {};
      roomsRaw = String(payload.rooms || payload["room:count"] || "").trim();
    } catch {
      roomsRaw = "";
    }
    const n = Number.parseInt(roomsRaw, 10);
    if (Number.isFinite(n) && n > 0 && n < 5000) {
      patch["Rooms / Keys"] = n;
      reasons.push("rooms_from_osm_tag");
    }
  }

  // Official Property URL — fix brand homepage
  const url = String(fields["Official Property URL"] || "").trim();
  const fix = DR_OSM_PROPERTY_URL_FIXES[key];
  if (fix) {
    const cleaned = sanitizeOfficialUrlCandidate(fix);
    if (cleaned && !isBrandHomepageOfficialUrl(cleaned)) {
      if (!url || isBrandHomepageOfficialUrl(url)) {
        patch["Official Property URL"] = cleaned;
        reasons.push("official_url_property_page_fix");
      }
    }
  } else if (url && isBrandHomepageOfficialUrl(url)) {
    reasons.push("official_url_brand_homepage_needs_steward");
  }

  return {
    ok: Object.keys(patch).length > 0,
    patch,
    reasons,
    classification: url ? classifyOfficialPropertyUrl(url) : null,
    source_record_id: sid,
  };
}
