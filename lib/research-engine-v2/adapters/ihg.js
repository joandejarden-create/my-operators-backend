/**
 * IHG official directory adapter (Hotel Indigo, Kimpton, …) — V1.1 match-gated.
 */

import { readFileSync, existsSync } from "node:fs";
import {
  ihgBrandFromUrl,
  extractIhgHotelNameFromHoteldetailHtml,
  IHG_FETCH_HEADERS,
} from "../../ihg-brand-directory-extract.js";
import { canonicalizeObservedBrand, defaultParentForFamily } from "../brand-family.js";
import { assessEntityMatch, brandLabelsAlign } from "../match-confidence.js";
import {
  fetchText,
  normalizeAdapterObservation,
  sleep,
} from "./adapter-utils.js";

export const IHG_DIRECTORY_DEFAULT = "reports/ihg-cala-directory-extract.json";

/**
 * @param {string} [path]
 */
export function loadIhgDirectoryRows(path = IHG_DIRECTORY_DEFAULT) {
  if (!existsSync(path)) return [];
  const doc = JSON.parse(readFileSync(path, "utf8"));
  return Array.isArray(doc.propertyRows) ? doc.propertyRows : [];
}

/**
 * Restrict directory candidates to the Dealality property-level brand (not parent IHG).
 * @param {string} dealalityBrand
 * @param {object} row
 */
export function ihgRowMatchesDealalityBrand(dealalityBrand, row) {
  const want = canonicalizeObservedBrand(dealalityBrand).toLowerCase();
  const rowBrand = String(row.brand || ihgBrandFromUrl(row.propertyUrl || row.website || "") || "").toLowerCase();
  if (/indigo/.test(want)) return /hotelindigo|indigo/.test(rowBrand);
  if (/kimpton/.test(want)) return /kimpton/.test(rowBrand);
  if (/holiday inn express/.test(want)) return /holidayinnexpress/.test(rowBrand);
  if (/holiday inn/.test(want)) return /holidayinn/.test(rowBrand) && !/express/.test(rowBrand);
  if (/intercontinental/.test(want)) return /intercontinental/.test(rowBrand);
  // unknown brand within IHG — do not match across brands
  return false;
}

/**
 * Parse open/pipeline signals from IHG hoteldetail HTML.
 * @param {string} html
 * @param {string} finalUrl
 */
export function parseIhgHoteldetailStatus(html, finalUrl) {
  const onHoteldetail = /\/hoteldetail\/?$/i.test(String(finalUrl || "").replace(/\/$/, "") + "/");
  const redirectedAway =
    !onHoteldetail ||
    /\/explore\/?$/i.test(finalUrl) ||
    /\/hotels\/us\/en\/?$/i.test(finalUrl);

  const statusText =
    [...String(html || "").matchAll(/<div class="cmp-hotelstatus"[^>]*>([\s\S]*?)<\/div>\s*<\/div>/gi)]
      .map((m) => m[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim())
      .find(Boolean) || "";

  const hasComingSoon =
    /coming soon|not yet open|under construction|opens\s+(in|on)\s+\w+/i.test(html) &&
    !/footer-banner__link/i.test(statusText);
  const hasBookNow =
    /cmp-button__text">Book Now</i.test(html) ||
    /class="booknow"/i.test(html) ||
    /check rates|check availability|find rooms/i.test(html);
  const newHotelBanner = /new hotel/i.test(statusText);

  /** @type {string|null} */
  let operatingStatus = null;
  /** @type {boolean|null} */
  let bookable = null;

  if (redirectedAway) {
    return {
      hotelFound: false,
      operatingStatus: null,
      bookable: null,
      statusText,
      redirectedAway: true,
      rawSignals: { hasComingSoon, hasBookNow, newHotelBanner },
    };
  }

  if (hasComingSoon && !hasBookNow) {
    operatingStatus = "Pipeline";
    bookable = false;
  } else if (hasBookNow || newHotelBanner) {
    operatingStatus = "Open";
    bookable = true;
  } else if (onHoteldetail) {
    operatingStatus = "Open";
    bookable = hasBookNow;
  }

  return {
    hotelFound: true,
    operatingStatus,
    bookable,
    statusText,
    redirectedAway: false,
    rawSignals: { hasComingSoon, hasBookNow, newHotelBanner, onHoteldetail },
  };
}

/**
 * Best entity-gated directory candidate for an IHG hotel.
 * @param {object} hotel
 * @param {object[]} directoryRows
 */
export function selectIhgDirectoryCandidate(hotel, directoryRows) {
  const brand = hotel.currentBrand || hotel.affiliation || "";
  const sameBrandRows = (directoryRows || []).filter((row) => ihgRowMatchesDealalityBrand(brand, row));

  let best = null;
  for (const row of sameBrandRows) {
    const city =
      row.citySlug
        ? String(row.citySlug).replace(/-/g, " ")
        : row.city || "";
    // Prefer citySlug over state-level city labels (e.g. Quintana Roo)
    const cityIsState = /quintana roo|baja california|ciudad de m[eé]xico|mexico city state/i.test(
      String(row.city || "")
    );
    const match = assessEntityMatch(hotel, {
      ...row,
      city: cityIsState && row.citySlug ? String(row.citySlug).replace(/-/g, " ") : city || row.city,
      brand: canonicalizeObservedBrand(row.brand || ihgBrandFromUrl(row.propertyUrl || "")),
      officialUrl: row.propertyUrl || row.website,
      propertyId: row.propertyId || row.mnemonic,
      mnemonic: row.mnemonic,
    });
    if (!best || matchScore(match) > matchScore(best.match)) {
      best = { row, match };
    }
  }
  return best;
}

function matchScore(match) {
  const rank = { Exact: 5, High: 4, Medium: 3, Low: 2, Reject: 1 };
  return (rank[match.level] || 0) * 10 + (match.score || 0);
}

/**
 * @param {object} hotel
 * @param {{ directoryRows?: object[], fetchDelayMs?: number, website?: string }} [opts]
 */
export async function fetchIhgHotelObservation(hotel, opts = {}) {
  const directoryRows = opts.directoryRows || loadIhgDirectoryRows();
  const website = opts.website || hotel.website || hotel.officialUrl || "";

  /** @type {object|null} */
  let matched = null;
  /** @type {object} */
  let entityMatch = {
    level: "Reject",
    score: 0,
    signals: {},
    reasons: ["no candidate"],
    allowMaterialCorrection: false,
    allowReviewOnly: false,
  };

  if (website && /ihg\.com/i.test(website)) {
    const urlBrand = canonicalizeObservedBrand(ihgBrandFromUrl(website));
    const brandOk = brandLabelsAlign(hotel.currentBrand || hotel.affiliation, urlBrand);
    matched = {
      propertyUrl: website,
      website,
      name: hotel.name,
      brand: urlBrand,
      city: hotel.city,
      country: hotel.country,
      propertyId: "",
    };
    entityMatch = assessEntityMatch(hotel, matched);
    if (brandOk === "conflict") {
      // Website points at different IHG brand — keep as observation but mark brand conflict; identity may still be weak
      entityMatch = {
        ...entityMatch,
        reasons: [...(entityMatch.reasons || []), "Website brand differs from Dealality brand"],
      };
    }
  } else {
    const selected = selectIhgDirectoryCandidate(hotel, directoryRows);
    if (selected && selected.match.level !== "Reject") {
      matched = selected.row;
      entityMatch = selected.match;
    }
  }

  if (!matched || entityMatch.level === "Reject") {
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "ihg",
      parent: defaultParentForFamily("ihg"),
      confidence: 0.15,
      notes: "No Exact/High/Medium IHG same-brand directory match",
      sourceState: "Empty",
      sourceStateReason: "no_directory_match",
      rawSignals: { entityMatch, sourceState: { state: "Empty", reason: "no_directory_match" } },
    });
  }

  // Do not fetch Low matches
  if (entityMatch.level === "Low") {
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "ihg",
      parent: defaultParentForFamily("ihg"),
      confidence: 0.2,
      notes: "IHG candidate below Medium — fetch skipped",
      sourceState: "Empty",
      sourceStateReason: "below_medium_skip_fetch",
      rawSignals: {
        entityMatch,
        skippedFetch: true,
        sourceState: { state: "Empty", reason: "below_medium_skip_fetch" },
      },
    });
  }

  const url = String(matched.propertyUrl || matched.website || "").trim();
  if (!url) {
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "ihg",
      notes: "Directory row missing URL",
      sourceState: "Empty",
      sourceStateReason: "missing_url",
      rawSignals: { entityMatch },
    });
  }

  if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);

  const page = await fetchText(url, { headers: IHG_FETCH_HEADERS });
  if (page.status === 403 || page.status === 429) {
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "ihg",
      parent: defaultParentForFamily("ihg"),
      confidence: 0.1,
      notes: `IHG page blocked (HTTP ${page.status})`,
      sourceState: "Blocked",
      sourceStateReason: `http_${page.status}`,
      officialUrl: page.url || url,
      rawSignals: {
        entityMatch,
        httpStatus: page.status,
        sourceState: { state: "Blocked", reason: `http_${page.status}` },
      },
    });
  }
  const parsed = parseIhgHoteldetailStatus(page.text, page.url);
  const brandRaw = ihgBrandFromUrl(page.url) || matched.brand || "";
  const brand = canonicalizeObservedBrand(brandRaw) || canonicalizeObservedBrand(matched.brand || "");
  const officialName =
    extractIhgHotelNameFromHoteldetailHtml(page.text) || matched.name || matched.inferredHotelName || null;

  const cityIsState = /quintana roo|baja california|ciudad de m[eé]xico/i.test(String(matched.city || ""));
  const resolvedCity =
    cityIsState && matched.citySlug
      ? String(matched.citySlug).replace(/-/g, " ")
      : matched.citySlug
        ? String(matched.citySlug).replace(/-/g, " ")
        : matched.city || hotel.city || "";

  // Re-assess with live official name + brand
  let liveMatch = assessEntityMatch(hotel, {
    name: officialName || matched.name,
    brand,
    country: matched.country || hotel.country,
    city: resolvedCity,
    officialUrl: page.url || url,
    propertyId: matched.propertyId || matched.mnemonic,
    mnemonic: matched.mnemonic,
  });

  // Prefer pre-fetch High/Exact over a flaky live re-score when URL is the selected candidate
  if (
    (liveMatch.level === "Reject" || liveMatch.level === "Low") &&
    entityMatch.allowMaterialCorrection
  ) {
    liveMatch = {
      ...entityMatch,
      reasons: [...(entityMatch.reasons || []), "retained_prefetch_match_after_live_fetch"],
    };
  }

  const conf =
    liveMatch.level === "Exact" ? 0.92 : liveMatch.level === "High" ? 0.82 : liveMatch.level === "Medium" ? 0.55 : 0.25;

  return normalizeAdapterObservation({
    hotelFound: parsed.hotelFound && liveMatch.level !== "Reject",
    officialHotelName: officialName,
    brand,
    parent: defaultParentForFamily("ihg"),
    city: resolvedCity || matched.city || hotel.city || null,
    country: matched.country || hotel.country || null,
    operatingStatus: parsed.operatingStatus,
    bookable: parsed.bookable,
    officialUrl: page.url || url,
    evidenceTimestamp: page.retrievedAt,
    sourceType: "official_brand_directory",
    sourceDate: null,
    adapter: "ihg",
    confidence: parsed.hotelFound ? conf : 0.25,
    notes: parsed.statusText ? `IHG hotelstatus: ${parsed.statusText}` : "",
    sourceState: page.ok ? "Available" : "Failed",
    sourceStateReason: page.ok ? "ihg_hoteldetail_ok" : `http_${page.status}`,
    rawSignals: {
      ...parsed.rawSignals,
      entityMatch: liveMatch,
      preFetchMatch: entityMatch,
      httpStatus: page.status,
      redirectedAway: parsed.redirectedAway,
      propertyId: matched.propertyId || matched.mnemonic || null,
      sourceState: {
        state: page.ok ? "Available" : "Failed",
        reason: page.ok ? "ihg_hoteldetail_ok" : `http_${page.status}`,
      },
    },
  });
}
