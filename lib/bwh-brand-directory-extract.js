/**
 * Best Western official property URL parse + curated CALA seed catalog extract.
 * Live bestwestern.com fetches are often captcha-blocked; seed URLs are official
 * propertyCode / hotel-details pages verified from public listings.
 */

import { readFileSync, existsSync } from "node:fs";
import { isCalaCountry } from "./design-hotels-census-enrichment.js";

export const BWH_ORIGIN = "https://www.bestwestern.com";
export const BWH_CONTENT_SOURCE = "bwh_official_property_url";

export const BWH_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "application/json,text/html,*/*",
  "Accept-Language": "en-US,en;q=0.9",
};

/** Exact Brand Setup Affiliation names. */
export const BWH_AFFILIATION_BY_FAMILY = {
  "bw-premier-collection": "BW Premier Collection",
  "bw-signature-collection": "BW Signature Collection",
  /** Legacy Best Western Premier flag pages still mapped to Active Brand Setup name. */
  "best-western-premier": "BW Premier Collection",
};

export const BWH_PARENT_COMPANY = "BWH Hotels";

export const DEFAULT_BWH_SEED_JSON = "fixtures/bwh-cala-directory-seed.json";

/**
 * @param {string} url
 */
export function parseBwhPropertyUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    if (!/bestwestern\.com$/i.test(u.hostname.replace(/^www\./, "")) && !u.hostname.includes("bestwestern.com")) {
      return null;
    }
    const path = u.pathname;
    let propertyCode = "";
    const codeMatch = path.match(/propertyCode\.(\d{4,6})(?:\.html)?/i);
    if (codeMatch) propertyCode = codeMatch[1];
    const detailsMatch = path.match(/hotel-details\.(\d{4,6})(?:\.html)?/i);
    if (!propertyCode && detailsMatch) propertyCode = detailsMatch[1];
    if (!propertyCode) return null;

    let brandFamily = "";
    if (/bw-premier-collection/i.test(path) || /bw-premier-collection/i.test(url)) {
      brandFamily = "bw-premier-collection";
    } else if (/bw-signature-collection/i.test(path) || /bw-signature-collection/i.test(url)) {
      brandFamily = "bw-signature-collection";
    } else if (/best-western-premier/i.test(path) || /best western premier/i.test(url)) {
      brandFamily = "best-western-premier";
    }

    return {
      propertyUrl: `${u.origin}${u.pathname}`.replace(/\/$/, ""),
      propertyCode: normalizeBwhPropertyCode(propertyCode) || String(propertyCode),
      brandFamily,
      source: BWH_CONTENT_SOURCE,
    };
  } catch {
    return null;
  }
}

/**
 * Normalize property code to 5-digit string when numeric.
 * @param {string|number} code
 */
export function normalizeBwhPropertyCode(code) {
  const s = String(code || "").trim();
  if (!/^\d{4,6}$/.test(s)) return "";
  return s.padStart(5, "0");
}

/**
 * @param {string} [seedPath]
 */
export function loadBwhDirectorySeed(seedPath = DEFAULT_BWH_SEED_JSON) {
  if (!existsSync(seedPath)) return [];
  const data = JSON.parse(readFileSync(seedPath, "utf8"));
  const rows = Array.isArray(data.properties) ? data.properties : Array.isArray(data) ? data : [];
  /** @type {object[]} */
  const out = [];
  for (const row of rows) {
    const parsed = parseBwhPropertyUrl(row.propertyUrl || row.sourcePageUrl || "");
    const propertyCode = normalizeBwhPropertyCode(row.propertyCode || parsed?.propertyCode);
    const brandFamily =
      row.brandFamily ||
      parsed?.brandFamily ||
      (row.affiliation === "BW Signature Collection"
        ? "bw-signature-collection"
        : row.affiliation === "BW Premier Collection"
          ? "bw-premier-collection"
          : "");
    const affiliation =
      row.affiliation || BWH_AFFILIATION_BY_FAMILY[brandFamily] || "";
    const propertyUrl = String(row.propertyUrl || parsed?.propertyUrl || "").replace(/\/$/, "");
    const country = String(row.country || "").trim();
    if (!propertyCode || !propertyUrl || !affiliation) continue;
    if (row.calaOnly !== false && country && !isCalaCountry(country)) continue;

    out.push({
      propertyId: propertyCode,
      propertyCode,
      name: String(row.name || "").trim(),
      inferredHotelName: String(row.name || "").trim(),
      city: String(row.city || "").trim(),
      country,
      brandFamily,
      affiliation,
      propertyUrl,
      website: propertyUrl,
      statusOpen: row.statusOpen !== false,
      listingStatus: row.listingStatus || (row.statusOpen === false ? "pipeline" : "open"),
      source: row.source || BWH_CONTENT_SOURCE,
      notes: row.notes || "",
    });
  }
  return out;
}

/**
 * hotelDetails proxy — may be captcha-blocked; returns null on failure.
 * @param {string} propertyCode
 * @param {object} [opts]
 */
export async function fetchBwhHotelDetails(propertyCode, opts = {}) {
  const code = normalizeBwhPropertyCode(propertyCode);
  if (!code) return { ok: false, error: "invalid_property_code" };
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const url = `${BWH_ORIGIN}/bin/bestwestern/proxy/hotelDetails?propertyCode=${code}&locale=en_US`;
  try {
    const res = await fetchFn(url, { headers: BWH_FETCH_HEADERS, redirect: "follow" });
    const text = await res.text();
    if (!res.ok || /captcha-delivery|interstitial/i.test(text)) {
      return { ok: false, status: res.status, error: "blocked_or_http", url };
    }
    const json = JSON.parse(text);
    return { ok: true, status: res.status, url, json };
  } catch (err) {
    return { ok: false, error: String(err?.message || err), url };
  }
}

/**
 * Pull amenity labels from hotelDetails JSON when present (no invention).
 * @param {object} json
 */
export function extractBwhAmenitiesFromHotelDetails(json) {
  /** @type {string[]} */
  const labels = [];
  const seen = new Set();
  const push = (v) => {
    const s = String(v || "")
      .replace(/\s+/g, " ")
      .trim();
    if (!s || s.length < 2 || s.length > 100) return;
    const key = s.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    labels.push(s);
  };

  const walk = (node, depth = 0) => {
    if (!node || depth > 6) return;
    if (Array.isArray(node)) {
      for (const item of node) walk(item, depth + 1);
      return;
    }
    if (typeof node !== "object") return;
    for (const [k, v] of Object.entries(node)) {
      if (/amenit/i.test(k)) {
        if (typeof v === "string") push(v);
        else if (Array.isArray(v)) {
          for (const item of v) {
            if (typeof item === "string") push(item);
            else if (item && typeof item === "object") {
              push(item.name || item.label || item.description || item.title);
            }
          }
        } else if (v && typeof v === "object") {
          push(v.name || v.label);
          walk(v, depth + 1);
        }
      } else if (v && typeof v === "object") {
        walk(v, depth + 1);
      }
    }
  };
  walk(json);
  return labels.sort((a, b) => a.localeCompare(b));
}
