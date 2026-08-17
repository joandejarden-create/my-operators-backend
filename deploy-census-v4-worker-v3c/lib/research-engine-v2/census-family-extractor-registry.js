/**
 * Family-specific extractor registry — prefer brand family before generic fallback.
 * Thin adapter over existing production-census extractors.
 */

export const FAMILY_EXTRACTOR_REGISTRY = Object.freeze({
  IHG: {
    parent_aliases: ["IHG", "InterContinental Hotels Group", "IHG Hotels & Resorts"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "IHG.com official pages; reject VIC rooms=22 false positive",
  },
  Marriott: {
    parent_aliases: ["Marriott", "Marriott International"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "Prefer official marriott.com property pages",
  },
  Hilton: {
    parent_aliases: ["Hilton", "Hilton Worldwide"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "Edge/challenge pages often block extraction",
  },
  Accor: {
    parent_aliases: ["Accor", "AccorHotels"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "Family-first before generic",
  },
  Wyndham: {
    parent_aliases: ["Wyndham", "Wyndham Hotels", "Wyndham Hotels & Resorts"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "CALA filter via JSON-LD addressCountry — never path keywords alone",
  },
  Preferred: {
    parent_aliases: ["Preferred", "Preferred Hotels & Resorts", "Preferred Hotels"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "Directory collections are not Brand Setup brands",
  },
  Hyatt: {
    parent_aliases: ["Hyatt", "Hyatt Hotels"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "Family-first before generic",
  },
  Choice: {
    parent_aliases: ["Choice", "Choice Hotels"],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "Family-first before generic",
  },
  generic: {
    parent_aliases: [],
    description: "production-census-description-extractor",
    rooms: "production-census-rooms-keys-extractor",
    notes: "Fallback after family-specific attempts",
  },
});

/**
 * Resolve extractor family for a parent company string.
 * @param {string|null|undefined} parentCompany
 */
export function resolveExtractorFamily(parentCompany) {
  const p = String(parentCompany || "").trim().toLowerCase();
  if (!p) return { family: "generic", entry: FAMILY_EXTRACTOR_REGISTRY.generic };
  for (const [family, entry] of Object.entries(FAMILY_EXTRACTOR_REGISTRY)) {
    if (family === "generic") continue;
    if (entry.parent_aliases.some((a) => p.includes(String(a).toLowerCase()) || String(a).toLowerCase().includes(p))) {
      return { family, entry };
    }
  }
  return { family: "generic", entry: FAMILY_EXTRACTOR_REGISTRY.generic };
}

/**
 * Ordered extractor attempt list: family then generic.
 * @param {string|null|undefined} parentCompany
 * @param {'description'|'rooms'} kind
 */
export function extractorAttemptOrder(parentCompany, kind = "rooms") {
  const { family, entry } = resolveExtractorFamily(parentCompany);
  const order = [{ family, module: entry[kind] || entry.rooms }];
  if (family !== "generic") {
    order.push({ family: "generic", module: FAMILY_EXTRACTOR_REGISTRY.generic[kind] || FAMILY_EXTRACTOR_REGISTRY.generic.rooms });
  }
  return order;
}
