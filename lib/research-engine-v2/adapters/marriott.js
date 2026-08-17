/**
 * Marriott adapter V1.1 — Tribute / Autograph / Design Hotels soft-brand aware.
 */

import { readFileSync, existsSync } from "node:fs";
import { marshaFromMarriottWebsite } from "../../marriott-brand-directory-extract.js";
import { canonicalizeObservedBrand, defaultParentForFamily } from "../brand-family.js";
import { assessEntityMatch, brandLabelsAlign } from "../match-confidence.js";
import {
  fetchText,
  normalizeAdapterObservation,
  sleep,
} from "./adapter-utils.js";

export const MARRIOTT_TRIBUTE_CALA_DEFAULT =
  "reports/cala-tribute-property-visual-discovery.json";

/**
 * @param {string} [path]
 */
export function loadMarriottTributeDirectoryRows(path = MARRIOTT_TRIBUTE_CALA_DEFAULT) {
  if (!existsSync(path)) return [];
  const doc = JSON.parse(readFileSync(path, "utf8"));
  return (doc.properties || []).map((p) => ({
    ...p,
    name: p.propertyName,
    country: p.countryRegion,
    propertyUrl: p.propertyPageUrl || p.overviewUrl,
    website: p.propertyPageUrl || p.overviewUrl,
    brand: marriottBrandFromUrl(p.propertyPageUrl || p.overviewUrl || "") || "Tribute Portfolio",
    marsha: p.marsha,
  }));
}

/**
 * Merge Tribute catalog with any Autograph/Design rows passed in opts or extra report paths.
 * @param {object[]} [extraRows]
 */
export function loadMarriottSoftBrandDirectoryRows(extraRows = []) {
  const base = loadMarriottTributeDirectoryRows();
  const byKey = new Map();
  for (const row of [...base, ...extraRows]) {
    const key = String(row.marsha || row.propertyUrl || row.name || "").toUpperCase();
    if (!key) continue;
    if (!byKey.has(key)) byKey.set(key, row);
  }
  return [...byKey.values()];
}

/**
 * @param {string} url
 */
export function marriottBrandFromUrl(url) {
  const u = String(url || "").toLowerCase();
  if (/tribute-portfolio/.test(u)) return "Tribute Portfolio";
  if (/autograph-collection/.test(u)) return "Autograph Collection";
  if (/design-hotels/.test(u)) return "Design Hotels";
  if (/marriott-hotels/.test(u)) return "Marriott Hotels";
  if (/westin/.test(u)) return "Westin";
  if (/sheraton/.test(u)) return "Sheraton";
  if (/w-hotels/.test(u)) return "W Hotels";
  if (/st-regis/.test(u)) return "St. Regis";
  if (/ritz-carlton/.test(u)) return "The Ritz-Carlton";
  return "";
}

/**
 * @param {string} url
 */
export function marriottProbeUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (/\/photos\/?$/i.test(raw)) return raw;
  const base = raw
    .replace(/\/+$/, "")
    .replace(/\/(overview|experiences|rooms|dining|events|reviews)\/?$/i, "");
  return `${base}/photos/`;
}

/**
 * Select best Marriott soft-brand candidate.
 * Same-brand preferred for status; cross-soft-brand allowed only at High+ name identity (reflag path).
 * @param {object} hotel
 * @param {object[]} directoryRows
 */
export function selectMarriottDirectoryCandidate(hotel, directoryRows) {
  const currentBrand = canonicalizeObservedBrand(hotel.currentBrand || hotel.affiliation || "");
  let bestSame = null;
  let bestCross = null;

  for (const row of directoryRows || []) {
    const rowBrand = canonicalizeObservedBrand(
      row.brand || marriottBrandFromUrl(row.propertyUrl || row.website || row.overviewUrl || "")
    );
    const match = assessEntityMatch(hotel, {
      ...row,
      brand: rowBrand,
      officialUrl: row.propertyUrl || row.website || row.overviewUrl,
      marsha: row.marsha || row.marshaCode,
      propertyId: row.marsha || row.marshaCode,
    });
    if (match.level === "Reject") continue;

    const brandRel = brandLabelsAlign(currentBrand, rowBrand);
    const scored = { row: { ...row, brand: rowBrand }, match };
    if (brandRel === "align" || brandRel === "compatible") {
      if (!bestSame || matchRank(match) > matchRank(bestSame.match)) bestSame = scored;
    } else if (brandRel === "conflict") {
      // Soft-brand reflag candidate — require High/Exact only
      if (match.allowMaterialCorrection) {
        if (!bestCross || matchRank(match) > matchRank(bestCross.match)) bestCross = scored;
      }
    }
  }

  if (bestSame) return { ...bestSame, matchMode: "same_brand" };
  if (bestCross) return { ...bestCross, matchMode: "soft_brand_reflag_candidate" };
  return null;
}

function matchRank(match) {
  const rank = { Exact: 5, High: 4, Medium: 3, Low: 2, Reject: 1 };
  return (rank[match.level] || 0) * 10 + (match.score || 0);
}

/**
 * @param {object} hotel
 * @param {{ directoryRows?: object[], fetchDelayMs?: number, website?: string }} [opts]
 */
export async function fetchMarriottHotelObservation(hotel, opts = {}) {
  const directoryRows = opts.directoryRows || loadMarriottSoftBrandDirectoryRows();
  const website = opts.website || hotel.website || hotel.officialUrl || "";

  let matched = null;
  let entityMatch = {
    level: "Reject",
    score: 0,
    allowMaterialCorrection: false,
    allowReviewOnly: false,
    reasons: [],
    signals: {},
  };
  let matchMode = "none";

  if (website && /marriott\.com/i.test(website)) {
    matched = {
      propertyUrl: website,
      website,
      name: hotel.name,
      brand: marriottBrandFromUrl(website),
      marsha: marshaFromMarriottWebsite(website),
    };
    entityMatch = assessEntityMatch(hotel, matched);
    matchMode = "website";
  } else {
    const selected = selectMarriottDirectoryCandidate(hotel, directoryRows);
    if (selected) {
      matched = selected.row;
      entityMatch = selected.match;
      matchMode = selected.matchMode;
    }
  }

  if (!matched || entityMatch.level === "Reject" || entityMatch.level === "Low") {
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "marriott",
      parent: defaultParentForFamily("marriott"),
      confidence: 0.15,
      notes: "No Marriott soft-brand Medium+ entity match",
      rawSignals: { entityMatch, matchMode },
    });
  }

  const probeUrl = marriottProbeUrl(matched.propertyUrl || matched.website || matched.overviewUrl || "");
  if (!probeUrl) {
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "marriott",
      notes: "Missing Marriott URL",
      rawSignals: { entityMatch, matchMode },
    });
  }

  if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
  const page = await fetchText(probeUrl);
  const accessDenied = /access denied/i.test(page.text) || page.status === 403;
  const hotelFound = page.ok && !accessDenied && /marriott\.com\/.*\/hotels\//i.test(page.url);
  const brand =
    canonicalizeObservedBrand(marriottBrandFromUrl(page.url || probeUrl)) ||
    canonicalizeObservedBrand(matched.brand || "");

  const liveMatch = assessEntityMatch(hotel, {
    name: matched.propertyName || matched.name,
    brand,
    country: matched.countryRegion || matched.country || hotel.country,
    city: hotel.city,
    officialUrl: page.url || probeUrl,
    marsha: matched.marsha || marshaFromMarriottWebsite(probeUrl),
    propertyId: matched.marsha || marshaFromMarriottWebsite(probeUrl),
  });

  let operatingStatus = null;
  let bookable = null;
  if (hotelFound && liveMatch.level !== "Reject") {
    const comingSoon = /coming soon|opening soon|opens\s+\w+\s+\d{4}/i.test(page.text);
    operatingStatus = comingSoon ? "Pipeline" : "Open";
    bookable = !comingSoon;
  }

  const conf =
    liveMatch.level === "Exact" ? 0.88 : liveMatch.level === "High" ? 0.78 : liveMatch.level === "Medium" ? 0.5 : 0.2;

  return normalizeAdapterObservation({
    hotelFound: hotelFound && liveMatch.level !== "Reject",
    officialHotelName: matched.propertyName || matched.name || null,
    brand,
    parent: defaultParentForFamily("marriott"),
    city: hotel.city || null,
    country: matched.countryRegion || matched.country || hotel.country || null,
    operatingStatus,
    bookable,
    officialUrl: page.url || probeUrl,
    evidenceTimestamp: page.retrievedAt,
    sourceType: "official_brand_directory",
    adapter: "marriott",
    confidence: hotelFound ? conf : 0.2,
    notes: accessDenied ? "Marriott page access denied" : matchMode === "soft_brand_reflag_candidate" ? "Soft-brand cross match" : "",
    rawSignals: {
      entityMatch: liveMatch,
      preFetchMatch: entityMatch,
      matchMode,
      httpStatus: page.status,
      accessDenied,
      marsha: matched.marsha || marshaFromMarriottWebsite(probeUrl),
    },
  });
}
