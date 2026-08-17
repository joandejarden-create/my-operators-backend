/**
 * Official-first property resolution — routes by brand family.
 * Output carries field-level source / retrieved_at / confidence / rights class.
 */

import { fetchText, sleep } from "../adapters/adapter-utils.js";
import { IHG_FETCH_HEADERS } from "../../ihg-brand-directory-extract.js";
import { HILTON_FETCH_HEADERS } from "../../hilton-brand-directory-extract.js";
import { CHOICE_FETCH_HEADERS } from "../../choice-regional-directory-extract.js";
import { extractHiltonCtyhocn } from "../census-autopilot-family-directory-adapters.js";
import { FAMILY_ADAPTER_META, ADAPTER_CLASS } from "./constants.js";
import { inferBrandFamily } from "../census-autopilot-v2/identity-dedupe.js";

export const OFFICIAL_RESOLVER_VERSION = "census-autopilot-v2.2-official-first";

function field(value, source, confidence, rights_class = "official_research") {
  if (value == null || value === "") {
    return {
      value: null,
      source: null,
      retrieved_at: null,
      confidence: null,
      match_confidence: null,
      rights_class: null,
      unknown: true,
    };
  }
  return {
    value,
    source,
    retrieved_at: new Date().toISOString(),
    confidence,
    match_confidence: confidence,
    rights_class,
    unknown: false,
  };
}

function extractMeta(html, prop) {
  const re = new RegExp(
    `<meta[^>]*(?:property|name)=["']${prop}["'][^>]*content=["']([^"']+)["']`,
    "i"
  );
  const m = html.match(re);
  return m ? m[1].trim() : null;
}

function extractTitle(html) {
  const m = html.match(/<title[^>]*>([^<]+)<\/title>/i);
  return m ? m[1].replace(/\s+/g, " ").trim() : null;
}

function extractIhgCode(url, html) {
  const fromUrl = String(url || "").match(/\/([a-z0-9]{5})\/hoteldetail/i);
  if (fromUrl) return fromUrl[1].toUpperCase();
  const fromHtml = html.match(/"hotelCode"\s*:\s*"([A-Z0-9]{3,8})"/i);
  return fromHtml ? fromHtml[1].toUpperCase() : null;
}

function extractPhone(html) {
  const m =
    html.match(/"telephone"\s*:\s*"([^"]+)"/i) ||
    html.match(/tel:([+\d][\d\s().-]{7,})/i) ||
    html.match(/itemprop=["']telephone["'][^>]*content=["']([^"']+)["']/i);
  return m ? m[1].replace(/\s+/g, " ").trim() : null;
}

function extractCoords(html) {
  const lat =
    html.match(/"latitude"\s*:\s*"?(-?\d+\.?\d*)"?/i) ||
    html.match(/itemprop=["']latitude["'][^>]*content=["']([^"']+)["']/i);
  const lng =
    html.match(/"longitude"\s*:\s*"?(-?\d+\.?\d*)"?/i) ||
    html.match(/itemprop=["']longitude["'][^>]*content=["']([^"']+)["']/i);
  if (!lat || !lng) return { lat: null, lng: null };
  const la = Number(lat[1]);
  const ln = Number(lng[1]);
  if (!Number.isFinite(la) || !Number.isFinite(ln)) return { lat: null, lng: null };
  return { lat: la, lng: ln };
}

function extractAddressBits(html) {
  const street =
    html.match(/"streetAddress"\s*:\s*"([^"]+)"/i) ||
    html.match(/itemprop=["']streetAddress["'][^>]*>([^<]+)/i);
  const city =
    html.match(/"addressLocality"\s*:\s*"([^"]+)"/i) ||
    html.match(/itemprop=["']addressLocality["'][^>]*>([^<]+)/i);
  const region =
    html.match(/"addressRegion"\s*:\s*"([^"]+)"/i) ||
    html.match(/itemprop=["']addressRegion["'][^>]*>([^<]+)/i);
  const country =
    html.match(/"addressCountry"\s*:\s*"([^"]+)"/i) ||
    html.match(/itemprop=["']addressCountry["'][^>]*content=["']([^"']+)["']/i);
  return {
    address: street ? street[1].trim() : null,
    city: city ? city[1].trim() : null,
    state_region: region ? region[1].trim() : null,
    country: country ? country[1].trim() : null,
  };
}

/**
 * @param {object} propertyCandidate
 * @param {{ delayMs?: number }} [opts]
 */
export async function resolveFromOfficialSources(propertyCandidate, opts = {}) {
  const name = propertyCandidate.name || propertyCandidate.origin_name;
  const family =
    propertyCandidate.family ||
    propertyCandidate.brand_family_inferred ||
    inferBrandFamily(name);
  const meta = FAMILY_ADAPTER_META[family] || FAMILY_ADAPTER_META.Independent;
  const url = propertyCandidate.website || propertyCandidate.official_url || null;
  const retrieved_at = new Date().toISOString();

  const empty = {
    resolver_version: OFFICIAL_RESOLVER_VERSION,
    family,
    adapter_class: meta.class,
    official_path_available: meta.class === ADAPTER_CLASS.STRONG_NATIVE || meta.class === ADAPTER_CLASS.PARTIAL,
    property_identity: field(propertyCandidate.property_identity_id || null, null, null),
    official_name: field(null, null, null),
    brand: field(propertyCandidate.brand || family, "candidate", "LOW"),
    parent: field(null, null, null),
    official_property_id: field(null, null, null),
    official_url: field(url, url ? "candidate" : null, url ? "MEDIUM" : null),
    address: field(null, null, null),
    city: field(propertyCandidate.city || null, "candidate", propertyCandidate.city ? "LOW" : null),
    state_region: field(null, null, null),
    country: field(propertyCandidate.country || null, "candidate", propertyCandidate.country ? "MEDIUM" : null),
    lat: field(null, null, null),
    lng: field(null, null, null),
    phone: field(null, null, null),
    website: field(url, url ? "candidate" : null, url ? "MEDIUM" : null),
    rooms: field(null, null, null),
    amenities: field(null, null, null),
    status: field(null, null, null),
    attempts: [],
    fields_resolved: [],
  };

  if (!url || meta.class === ADAPTER_CLASS.INDEPENDENT || meta.class === ADAPTER_CLASS.UNKNOWN) {
    empty.reason = !url ? "no_official_url" : "no_native_adapter";
    return empty;
  }

  if (opts.delayMs) await sleep(opts.delayMs);

  let headers = {};
  if (family === "IHG") headers = IHG_FETCH_HEADERS;
  else if (family === "Hilton") headers = HILTON_FETCH_HEADERS;
  else if (family === "Choice") headers = CHOICE_FETCH_HEADERS;

  const page = await fetchText(url, { headers, timeoutMs: opts.timeoutMs || 25000 });
  empty.attempts.push({ kind: "official_detail", ok: page.ok, status: page.status, url });

  if (!page.ok || !page.text) {
    empty.reason = page.status === 403 ? "access_blocked" : "fetch_failed";
    return empty;
  }

  const html = page.text;
  const title = extractTitle(html);
  const ogTitle = extractMeta(html, "og:title");
  const officialName = (ogTitle || title || name || "").split("|")[0].split(" - ")[0].trim();
  empty.official_name = field(officialName || name, url, "HIGH");
  empty.fields_resolved.push("official_name");

  const addr = extractAddressBits(html);
  if (addr.address) {
    empty.address = field(addr.address, url, "HIGH");
    empty.fields_resolved.push("address");
  }
  if (addr.city) {
    empty.city = field(addr.city, url, "HIGH");
    empty.fields_resolved.push("city");
  }
  if (addr.state_region) {
    empty.state_region = field(addr.state_region, url, "HIGH");
    empty.fields_resolved.push("state_region");
  }
  if (addr.country) {
    empty.country = field(addr.country, url, "HIGH");
    empty.fields_resolved.push("country");
  }

  const coords = extractCoords(html);
  if (coords.lat != null) {
    empty.lat = field(coords.lat, url, "HIGH");
    empty.lng = field(coords.lng, url, "HIGH");
    empty.fields_resolved.push("lat", "lng");
  }

  const phone = extractPhone(html);
  if (phone) {
    empty.phone = field(phone, url, "HIGH");
    empty.fields_resolved.push("phone");
  }

  empty.website = field(url, url, "HIGH");
  empty.official_url = field(url, url, "HIGH");
  empty.fields_resolved.push("website", "official_url");

  let propId = null;
  if (family === "IHG") propId = extractIhgCode(url, html);
  if (family === "Hilton") {
    propId =
      extractHiltonCtyhocn(
        { "Official Property URL": url, "Brand Property Code": propertyCandidate.property_ids?.[0] },
        propertyCandidate.property_identity_id
      ) ||
      html.match(/"ctyhocn"\s*:\s*"([A-Z0-9]{5,8})"/i)?.[1];
  }
  if (family === "Choice") {
    propId =
      propertyCandidate.property_ids?.[0] ||
      html.match(/"hotelId"\s*:\s*"([A-Z0-9]+)"/i)?.[1] ||
      html.match(/\/hotel\/([A-Z0-9-]+)/i)?.[1];
  }
  if (family === "Marriott") {
    propId =
      propertyCandidate.property_ids?.[0] ||
      html.match(/"marshaCode"\s*:\s*"([A-Z0-9]{4,8})"/i)?.[1] ||
      url.match(/\/([a-z]{4,8})-hotel\/?$/i)?.[1]?.toUpperCase();
  }
  if (propId) {
    empty.official_property_id = field(propId, url, "HIGH");
    empty.property_identity = field(
      propertyCandidate.property_identity_id || `official:${family}:${propId}`,
      url,
      "HIGH"
    );
    empty.fields_resolved.push("official_property_id", "property_identity");
  }

  empty.brand = field(propertyCandidate.brand || family, url, "MEDIUM");
  empty.parent = field(family, "brand_family_map", "MEDIUM");
  empty.status = field("Operating", url, "LOW"); // weak — do not treat as production without corroboration
  empty.retrieved_at = retrieved_at;
  empty.reason = empty.fields_resolved.length >= 3 ? "official_partial_or_complete" : "official_sparse";

  return empty;
}

/**
 * Whether SerpApi is still economically justified after official resolve.
 */
export function officialGapsForSerpApi(official) {
  const gaps = [];
  if (!official?.address?.value) gaps.push("address");
  if (official?.lat?.value == null) gaps.push("coordinates");
  if (!official?.phone?.value) gaps.push("phone");
  if (!official?.website?.value) gaps.push("website");
  if (!official?.amenities?.value) gaps.push("amenities");
  if (!official?.official_name?.value) gaps.push("identity");
  return gaps;
}
