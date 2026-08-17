/**
 * Lightweight generic official-source adapter (e.g. Avani / Minor).
 * No elaborate Minor directory — uses provided website or simple official-domain heuristics.
 */

import { canonicalizeObservedBrand, defaultParentForFamily } from "../brand-family.js";
import { fetchText, normalizeAdapterObservation, sleep } from "./adapter-utils.js";

/**
 * @param {object} hotel
 * @param {{ fetchDelayMs?: number, website?: string }} [opts]
 */
export async function fetchGenericOfficialObservation(hotel, opts = {}) {
  const website = String(opts.website || hotel.website || hotel.officialUrl || "").trim();
  const brandHint = canonicalizeObservedBrand(hotel.currentBrand || hotel.affiliation || hotel.name || "");
  const isAvani = /avani/i.test(`${hotel.name} ${brandHint}`);

  if (!website) {
    // Attempt well-known Minor/Avani search page is not reliable enough — leave unverified.
    return normalizeAdapterObservation({
      hotelFound: false,
      adapter: "generic",
      brand: brandHint || null,
      parent: isAvani ? defaultParentForFamily("minor") : null,
      confidence: 0.1,
      notes: "No official website available for generic adapter probe",
    });
  }

  if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
  const page = await fetchText(website);
  const hotelFound = page.ok && page.status < 400;
  const comingSoon = /coming soon|opening soon|under development/i.test(page.text);

  return normalizeAdapterObservation({
    hotelFound,
    officialHotelName: hotel.name || null,
    brand: brandHint || null,
    parent: isAvani ? defaultParentForFamily("minor") : hotel.parentCompany || null,
    city: hotel.city || null,
    country: hotel.country || null,
    operatingStatus: hotelFound ? (comingSoon ? "Pipeline" : "Open") : null,
    bookable: hotelFound ? !comingSoon : null,
    officialUrl: page.url || website,
    evidenceTimestamp: page.retrievedAt,
    sourceType: /avanihotels|minorhotels/i.test(website)
      ? "official_brand_directory"
      : "official_hotel_website",
    adapter: "generic",
    confidence: hotelFound ? 0.45 : 0.15,
    notes: "Generic official URL probe (no dedicated Minor adapter)",
    rawSignals: { httpStatus: page.status },
  });
}
