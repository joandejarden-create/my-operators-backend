/**
 * Brand-family → adapter routing for Research Engine V2.
 */

export const BRAND_FAMILY_ADAPTERS = Object.freeze({
  ihg: "ihg",
  marriott: "marriott",
  choice: "choice",
  minor: "generic",
  unknown: "generic",
});

/**
 * @param {object} hotel
 * @returns {"ihg"|"marriott"|"choice"|"generic"}
 */
export function resolveBrandFamily(hotel) {
  const brand = String(hotel.currentBrand || hotel.affiliation || hotel.brand || "").toLowerCase();
  const parent = String(hotel.currentParent || hotel.parentCompany || "").toLowerCase();
  const name = String(hotel.name || hotel.hotelName || "").toLowerCase();
  const blob = `${brand} ${parent} ${name}`;

  if (
    /ihg|intercontinental hotels group|hotel indigo|kimpton|holiday inn|crowne plaza|voco|staybridge|candlewood/.test(
      blob
    )
  ) {
    return "ihg";
  }
  if (/hilton|hampton|doubletree|embassy suites|homewood|home2|spark by hilton|tempo by hilton|lxr|waldorf|conrad|canopy/.test(blob)) {
    return "hilton";
  }
  if (
    /marriott|tribute portfolio|autograph|design hotels|westin|sheraton|w hotels|st\.?\s*regis|ritz-carlton|courtyard|residence inn/.test(
      blob
    )
  ) {
    return "marriott";
  }
  // Soft-brand labels without parent still route to Marriott adapter
  if (/^tribute portfolio$|^autograph collection$|^design hotels$/i.test(brand.trim())) {
    return "marriott";
  }
  if (/choice|radisson individual|ascend collection|cambria|quality inn|comfort inn|sleep inn/.test(blob)) {
    return "choice";
  }
  if (/minor|avani|nh hotel|anantara|tia/.test(blob)) {
    return "minor";
  }
  return "unknown";
}

/**
 * Canonical brand label for adapter outputs.
 * @param {string} raw
 */
export function canonicalizeObservedBrand(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (/hotelindigo|hotel indigo/i.test(s)) return "Hotel Indigo";
  if (/kimpton/i.test(s)) return "Kimpton";
  if (/tribute/i.test(s)) return "Tribute Portfolio";
  if (/autograph/i.test(s)) return "Autograph Collection";
  if (/design hotels/i.test(s)) return "Design Hotels";
  if (/radisson individual/i.test(s)) return "Radisson Individuals Americas";
  if (/avani/i.test(s)) return "Avani";
  return s;
}

/**
 * Canonical parent from brand family.
 * @param {"ihg"|"marriott"|"choice"|"minor"|"unknown"} family
 */
export function defaultParentForFamily(family) {
  if (family === "ihg") return "IHG Hotels & Resorts";
  if (family === "marriott") return "Marriott International";
  if (family === "choice") return "Choice Hotels International, Inc.";
  if (family === "minor") return "Minor Hotel Group Limited";
  if (family === "hilton") return "Hilton";
  return "";
}
