/**
 * Parse Accor booking URLs for property codes (discovery aid only).
 * Amenities/geo still come from canonical all.accor.com/hotel/{code}/index.en.shtml pages.
 */

import { accorCanonicalPropertyUrl } from "./hotel-census/accor-directory-name-normalize.js";

/**
 * @param {string} url
 */
export function accorPropertyIdFromBookingUrl(url) {
  const s = String(url || "").trim();
  if (!s) return "";

  const bookingHotel = s.match(/\/booking\/[^/]+\/(?:[a-z-]+\/)?hotel\/([0-9A-Za-z]{3,6})(?:[/?]|$)/i);
  if (bookingHotel) return bookingHotel[1].toUpperCase();

  const canonical = s.match(/\/hotel\/([0-9A-Za-z]{3,6})\//i);
  if (canonical) return canonical[1].toUpperCase();

  return "";
}

/**
 * @param {string} citySlug e.g. rio-de-janeiro-state-of-rio-de-janeiro-brazil
 * @param {string} [brand] ibis|accor|novotel|mercure
 */
export function accorBookingCitySearchUrl(citySlug, brand = "accor") {
  const slug = String(citySlug || "").trim().toLowerCase();
  if (!slug) return "";
  const b = String(brand || "accor").trim().toLowerCase();
  return `https://all.accor.com/booking/en/${b}/hotels/${slug}?compositions=1`;
}

/**
 * @param {string} propertyId
 * @param {string} [brand]
 */
export function accorBookingHotelUrl(propertyId, brand = "accor") {
  const code = String(propertyId || "").trim().toUpperCase();
  if (!code) return "";
  const b = String(brand || "accor").trim().toLowerCase();
  return `https://all.accor.com/booking/en/${b}/hotel/${code}`;
}

export { accorCanonicalPropertyUrl };
