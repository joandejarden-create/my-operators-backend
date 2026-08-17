/**
 * Repair malformed City values on Hotel Property Census rows.
 *
 * Primary bug: Marriott Autograph names like
 *   "Royalton Punta Cana, An Autograph Collection All-Inclusive Resort & Casino"
 * were parsed so City became "An & Casino".
 *
 * Also repairs Choice Ascend names with City=Unknown when city is in the name/URL.
 */

import {
  canonicalCalaCity,
  isDescriptorCity,
  normalizePlaceKey,
} from "../research-engine-v2/census-city-state-normalizer.js";
import { resolveDominicanRepublicStateRegion } from "./dominican-republic-state-region.js";
import {
  getCensusOfficialEntry,
  resolveCensusOfficialBrand,
} from "../research-engine-v2/census-official-brand-registry.js";

export const CITY_CASINO_CLEANUP_VERSION = "census-city-an-casino-cleanup-v1";

const AUTOgraph_TAIL =
  /,\s*an\s+autograph\s+collection\b.*$/i;
const ASCEND_TAIL = /,\s*an\s+ascend\s+collection\s+hotel\b.*$/i;
const ALL_INCLUSIVE_TAIL =
  /,\s*an?\s+all[-\s]?inclusive\b.*$/i;

/** Marriott URL path segment → city when High confidence. */
const MARRIOTT_SLUG_CITY = Object.freeze({
  "punta-cana": "Punta Cana",
  bavaro: "Bávaro",
  "cap-cana": "Cap Cana",
  "santo-domingo": "Santo Domingo",
  "puerto-plata": "Puerto Plata",
  "la-romana": "La Romana",
  miches: "Miches",
});

const CHOICE_PATH_CITY = Object.freeze({
  "playa-dorada": "Puerto Plata",
  "juan-dolio-beach": "Juan Dolio",
  "juan-dolio": "Juan Dolio",
  "punta-cana": "Punta Cana",
  "santo-domingo": "Santo Domingo",
});

/** Prefer accented / census-standard city spellings for DR tourism cities. */
const DR_CITY_DISPLAY = Object.freeze({
  bavaro: "Bávaro",
  "bávaro": "Bávaro",
  "punta cana": "Punta Cana",
  "cap cana": "Cap Cana",
  "puerto plata": "Puerto Plata",
  "juan dolio": "Juan Dolio",
  "santo domingo": "Santo Domingo",
  "la romana": "La Romana",
  samana: "Samaná",
  "samaná": "Samaná",
  sosua: "Sosúa",
  "sosúa": "Sosúa",
  miches: "Miches",
  "playa dorada": "Puerto Plata",
});

function toDrDisplayCity(city) {
  const raw = String(city || "").trim();
  if (!raw) return null;
  const canon = canonicalCalaCity(raw);
  if (canon) return canon;
  const key = normalizePlaceKey(raw);
  return DR_CITY_DISPLAY[key] || raw;
}

function isBadCity(city) {
  const c = String(city || "").trim();
  if (!c) return true;
  if (isDescriptorCity(c)) return true;
  if (/^an\s*&/i.test(c)) return true;
  if (/casino/i.test(c) && !/punta cana|bavaro|romana/i.test(c)) return true;
  return false;
}

/**
 * Extract city from Marriott / Autograph-style property name.
 * @param {string} name
 */
export function extractCityFromAutographStyleName(name) {
  const raw = String(name || "").trim();
  if (!raw) return null;

  let head = raw;
  if (AUTOgraph_TAIL.test(head)) head = head.replace(AUTOgraph_TAIL, "").trim();
  else if (ASCEND_TAIL.test(head)) head = head.replace(ASCEND_TAIL, "").trim();
  else if (ALL_INCLUSIVE_TAIL.test(head)) {
    head = head.replace(ALL_INCLUSIVE_TAIL, "").trim();
  }

  // "Emotions All Inclusive Puerto Plata" / "Royalton Splash Punta Cana"
  const placeHints = [
    "Punta Cana",
    "Bávaro",
    "Bavaro",
    "Cap Cana",
    "Puerto Plata",
    "Playa Dorada",
    "Juan Dolio",
    "Santo Domingo",
    "La Romana",
    "Samaná",
    "Samana",
    "Miches",
    "Sosúa",
    "Sosua",
  ];
  for (const hint of placeHints) {
    const re = new RegExp(`\\b${hint.replace(/\s+/g, "\\s+")}\\b`, "i");
    if (re.test(head) || re.test(raw)) {
      return toDrDisplayCity(hint);
    }
  }

  // "Royalton CHIC Punta Cana" already covered; last comma segment if clean
  if (head.includes(",")) {
    const left = head.split(",")[0].trim();
    // Prefer trailing place tokens in left
    for (const hint of placeHints) {
      const re = new RegExp(`\\b${hint.replace(/\s+/g, "\\s+")}\\b`, "i");
      if (re.test(left)) return toDrDisplayCity(hint);
    }
  }

  return null;
}

/**
 * City from official Marriott / Choice URL path.
 * @param {string} url
 */
export function extractCityFromOfficialUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return null;
  let path = "";
  try {
    path = new URL(raw).pathname.toLowerCase();
  } catch {
    path = raw.toLowerCase();
  }

  for (const [slug, city] of Object.entries(MARRIOTT_SLUG_CITY)) {
    if (path.includes(slug)) return toDrDisplayCity(city);
  }
  for (const [slug, city] of Object.entries(CHOICE_PATH_CITY)) {
    if (path.includes(`/${slug}/`) || path.includes(`/${slug}`)) {
      return toDrDisplayCity(city);
    }
  }
  return null;
}

/**
 * Build City (+ optional State / Brand) remediation for one record.
 * @param {object} fields
 */
export function buildAnCasinoCityCleanupProposal(fields) {
  const city = String(fields.City || "").trim();
  const name = String(fields["Property Name"] || "").trim();
  const url = String(fields["Official Property URL"] || "").trim();
  const country = String(fields.Country || "Dominican Republic").trim();

  if (!isBadCity(city) && city) {
    return {
      ok: false,
      reason: "city_already_usable",
      patch: {},
    };
  }

  const fromName = extractCityFromAutographStyleName(name);
  const fromUrl = extractCityFromOfficialUrl(url);
  const cityResolved = toDrDisplayCity(fromName || fromUrl);

  if (!cityResolved) {
    return {
      ok: false,
      reason: "could_not_resolve_city",
      patch: {},
    };
  }

  /** @type {Record<string, string>} */
  const patch = { City: cityResolved };

  if (/^dominican republic$/i.test(country)) {
    const st = resolveDominicanRepublicStateRegion(cityResolved);
    if (st.ok && st.province) patch["State / Region"] = st.province;
  }

  // Emotions Ascend mislabeled as Radisson Individuals
  if (/ascend collection/i.test(name)) {
    const brand = resolveCensusOfficialBrand("Ascend Hotel Collection", {
      propertyName: name,
      sourceUrl: url,
    });
    if (brand.ok) {
      patch["Current Brand"] = brand.canonical;
      if (brand.parent) {
        patch["Brand Family"] = brand.parent;
        patch["Family / Source Family"] = brand.parent;
      }
    } else {
      const entry = getCensusOfficialEntry("Ascend Hotel Collection");
      if (entry) {
        patch["Current Brand"] = entry.canonical;
        patch["Brand Family"] = entry.parent;
        patch["Family / Source Family"] = entry.parent;
      }
    }
  }

  return {
    ok: true,
    reason: fromName ? "city_from_property_name" : "city_from_official_url",
    city_before: city || null,
    city_after: cityResolved,
    patch,
    confidence: "High",
  };
}
