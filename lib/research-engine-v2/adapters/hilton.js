/**
 * Thin Hilton adapter for Research Engine V2 (reuses existing GraphQL status fetch).
 * Source failures never imply closed/reflagged/discontinued.
 */

import { fetchHiltonHotelStatus } from "../../hilton-hotel-status-fetch.js";
import { defaultParentForFamily } from "../brand-family.js";
import { assessEntityMatch } from "../match-confidence.js";
import { classifySourceState } from "../source-state.js";
import { normalizeAdapterObservation, sleep } from "./adapter-utils.js";

/**
 * @param {object} hotel
 * @param {{ fetchDelayMs?: number, ctyhocn?: string }} [opts]
 */
export async function fetchHiltonHotelObservation(hotel, opts = {}) {
  const code = String(opts.ctyhocn || hotel.ctyhocn || hotel.propertyId || hotel.brandPropertyCode || "")
    .trim()
    .toUpperCase();

  if (!code) {
    const src = classifySourceState({ hotelFound: false, notes: "missing_ctyhocn" });
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "hilton",
      parent: defaultParentForFamily("unknown"),
      brand: hotel.currentBrand || "Hilton",
      confidence: 0.1,
      notes: "No ctyhocn / property code for Hilton GraphQL",
      sourceState: "Empty",
      sourceStateReason: "missing_ctyhocn",
      rawSignals: {
        sourceState: src,
        entityMatch: {
          level: "Reject",
          score: 0,
          allowMaterialCorrection: false,
          allowReviewOnly: false,
          reasons: ["missing_ctyhocn"],
          signals: {},
        },
      },
    });
  }

  if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);

  try {
    const hilton = await fetchHiltonHotelStatus(code, { refererUrl: hotel.website || undefined });
    const entityMatch = assessEntityMatch(hotel, {
      name: hilton.name,
      brand: hotel.currentBrand || "Hilton",
      country: hotel.country,
      city: hotel.city,
      propertyId: hilton.ctyhocn,
      ctyhocn: hilton.ctyhocn,
      officialUrl: hotel.website || `https://www.hilton.com/en/hotels/${code.toLowerCase()}-hotel/`,
    });

    return normalizeAdapterObservation({
      hotelFound: true,
      officialHotelName: hilton.name,
      brand: hotel.currentBrand || null,
      parent: "Hilton",
      city: hotel.city || null,
      country: hotel.country || null,
      operatingStatus: hilton.hiltonStatus,
      bookable: hilton.hiltonOpen === true,
      officialUrl: hotel.website || `https://www.hilton.com/en/hotels/${code.toLowerCase()}-hotel/`,
      evidenceTimestamp: new Date().toISOString(),
      sourceType: "official_brand_directory",
      sourceDate: hilton.openDate || null,
      adapter: "hilton",
      confidence: entityMatch.level === "Exact" || entityMatch.level === "High" ? 0.9 : 0.6,
      notes: "Hilton GraphQL display.open",
      sourceState: "Available",
      sourceStateReason: "hilton_graphql_ok",
      rawSignals: {
        entityMatch,
        hiltonOpen: hilton.hiltonOpen,
        openDate: hilton.openDate,
        ctyhocn: hilton.ctyhocn,
        hasBookNow: hilton.hiltonOpen === true,
        sourceState: { state: "Available", reason: "hilton_graphql_ok" },
      },
    });
  } catch (err) {
    const msg = err?.message || String(err);
    const src = classifySourceState({ error: msg, hotelFound: false });
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "hilton",
      parent: "Hilton",
      confidence: 0.2,
      notes: msg,
      sourceState: src.state,
      sourceStateReason: src.reason,
      rawSignals: {
        sourceState: src,
        entityMatch: {
          level: "Reject",
          score: 0,
          allowMaterialCorrection: false,
          allowReviewOnly: false,
          reasons: ["hilton_graphql_error"],
          signals: {},
        },
      },
    });
  }
}
