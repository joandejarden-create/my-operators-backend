/**
 * Choice Hotels adapter V1.1 — Radisson Individuals Americas + mapping reviews.
 */

import { existsSync } from "node:fs";
import { loadChoiceSitemapDirectoryRows } from "../../hotel-census/plan-choice-census-sitemap-match.js";
import { canonicalizeObservedBrand, defaultParentForFamily } from "../brand-family.js";
import { assessEntityMatch, brandLabelsAlign } from "../match-confidence.js";
import {
  fetchText,
  normalizeAdapterObservation,
  sleep,
} from "./adapter-utils.js";

/**
 * @param {string} url
 * @param {string} [name]
 */
export function choiceBrandFromUrlOrName(url, name = "") {
  const blob = `${url} ${name}`.toLowerCase();
  if (/radisson.?individual|individuals/.test(blob)) return "Radisson Individuals Americas";
  if (/ascend/.test(blob)) return "Ascend Collection";
  if (/cambria/.test(blob)) return "Cambria Hotels";
  if (/radisson/.test(blob)) return "Radisson";
  return "Choice Hotels";
}

function individualsPreferred(row) {
  const blob = `${row.brand || ""} ${row.matchedBrandSetupBrand || ""} ${row.inferredHotelName || ""} ${row.name || ""} ${row.propertyUrl || ""}`;
  return /radisson.?individual|individuals|faranda/i.test(blob);
}

/**
 * @param {object} hotel
 * @param {object[]} directoryRows
 */
export function selectChoiceDirectoryCandidate(hotel, directoryRows) {
  const currentBrand = canonicalizeObservedBrand(hotel.currentBrand || hotel.affiliation || "");
  const preferIndividuals = /radisson individual/i.test(currentBrand) || /radisson individual|faranda/i.test(hotel.name || "");

  let best = null;
  for (const row of directoryRows || []) {
    if (preferIndividuals && !individualsPreferred(row)) {
      // Still allow assessment but soft-penalize later via brand conflict → Reject for material
    }
    const rowBrand = canonicalizeObservedBrand(
      choiceBrandFromUrlOrName(row.propertyUrl || "", row.inferredHotelName || row.name || "")
    );
    const match = assessEntityMatch(hotel, {
      ...row,
      brand: rowBrand,
      officialUrl: row.propertyUrl || row.website,
      propertyId: row.propertyId,
    });
    if (match.level === "Reject" || match.level === "Low") continue;

    // Prefer Individuals rows when Dealality says Individuals
    const brandRel = brandLabelsAlign(currentBrand, rowBrand);
    const scoreBoost = preferIndividuals && individualsPreferred(row) ? 1 : 0;
    const rank = matchRank(match) + scoreBoost;
    if (brandRel === "conflict" && !match.allowMaterialCorrection) continue;
    if (brandRel === "conflict" && preferIndividuals && /ascend/i.test(rowBrand)) {
      // Ascend sibling in same city — reject unless Exact name
      if (match.level !== "Exact") continue;
    }
    if (!best || rank > best.rank) best = { row: { ...row, brand: rowBrand }, match, rank };
  }
  return best;
}

function matchRank(match) {
  const rank = { Exact: 5, High: 4, Medium: 3, Low: 2, Reject: 1 };
  return (rank[match.level] || 0) * 10 + (match.score || 0);
}

/**
 * @param {object} hotel
 * @param {{ directoryRows?: object[], fetchDelayMs?: number, website?: string }} [opts]
 */
export async function fetchChoiceHotelObservation(hotel, opts = {}) {
  const directoryRows =
    opts.directoryRows ||
    (existsSync("reports/independent-census-choice-property-url-extract-cala-2026-05-20.json")
      ? loadChoiceSitemapDirectoryRows()
      : []);

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

  if (website && /choicehotels\.com/i.test(website)) {
    matched = {
      propertyUrl: website,
      website,
      name: hotel.name,
      brand: choiceBrandFromUrlOrName(website, hotel.name),
    };
    entityMatch = assessEntityMatch(hotel, matched);
  } else {
    const selected = selectChoiceDirectoryCandidate(hotel, directoryRows);
    if (selected) {
      matched = selected.row;
      entityMatch = selected.match;
    }
  }

  if (!matched || entityMatch.level === "Reject" || entityMatch.level === "Low") {
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "choice",
      parent: defaultParentForFamily("choice"),
      brand: canonicalizeObservedBrand(hotel.currentBrand || hotel.affiliation || ""),
      confidence: 0.15,
      notes: "No Choice Medium+ entity match",
      rawSignals: { entityMatch, directoryRowsLoaded: directoryRows.length },
    });
  }

  const url = String(matched.propertyUrl || matched.website || "").trim();
  if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
  const page = await fetchText(url);
  const hotelFound = page.ok && /choicehotels\.com/i.test(page.url);
  const brand = canonicalizeObservedBrand(
    choiceBrandFromUrlOrName(page.url || url, matched.inferredHotelName || matched.name || hotel.name)
  );
  const comingSoon = /coming soon|opening soon|not yet open/i.test(page.text);

  const liveMatch = assessEntityMatch(hotel, {
    name: matched.inferredHotelName || matched.name,
    brand,
    country: matched.country || hotel.country,
    city: matched.city || hotel.city,
    officialUrl: page.url || url,
    propertyId: matched.propertyId,
  });

  // 403 / not found → do not propose reflags
  if (!hotelFound || page.status === 403 || page.status === 429) {
    const blocked = page.status === 403 || page.status === 429;
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "choice",
      parent: defaultParentForFamily("choice"),
      brand: null,
      confidence: 0.1,
      notes: `Choice page not usable (HTTP ${page.status})`,
      sourceState: blocked ? "Blocked" : "Empty",
      sourceStateReason: `http_${page.status}`,
      rawSignals: {
        entityMatch: liveMatch,
        httpStatus: page.status,
        sourceState: {
          state: blocked ? "Blocked" : "Empty",
          reason: `http_${page.status}`,
        },
      },
    });
  }

  const conf =
    liveMatch.level === "Exact" ? 0.85 : liveMatch.level === "High" ? 0.75 : liveMatch.level === "Medium" ? 0.5 : 0.2;

  return normalizeAdapterObservation({
    hotelFound: liveMatch.level !== "Reject",
    officialHotelName: matched.inferredHotelName || matched.name || null,
    brand,
    parent: defaultParentForFamily("choice"),
    city: matched.city || hotel.city || null,
    country: matched.country || hotel.country || null,
    operatingStatus: comingSoon ? "Pipeline" : "Open",
    bookable: !comingSoon,
    officialUrl: page.url || url,
    evidenceTimestamp: page.retrievedAt,
    sourceType: "official_brand_directory",
    adapter: "choice",
    confidence: conf,
    notes: "",
    rawSignals: { entityMatch: liveMatch, preFetchMatch: entityMatch, httpStatus: page.status },
  });
}
