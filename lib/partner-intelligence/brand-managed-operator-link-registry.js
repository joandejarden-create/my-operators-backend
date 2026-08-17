/**
 * Brand Explorer parentCompany → brand-managed Operator Explorer Master link.
 * Core 5 only (Wave C). Link chip only — do not merge Brand/Operator IA.
 *
 * @typedef {{
 *   slug: string,
 *   recordId: string,
 *   companyName: string,
 *   explorerPath: string,
 *   canonicalParent: string,
 *   aliases: string[],
 * }} BrandManagedOperatorLink
 */

/** @type {BrandManagedOperatorLink[]} */
export const BRAND_MANAGED_OPERATOR_LINKS = Object.freeze([
  Object.freeze({
    slug: "marriott-international-managed",
    recordId: "recGmiPhRt6hiayd9",
    companyName: "Marriott International (Managed)",
    explorerPath: "/operator-explorer-gold-mock.html?id=recGmiPhRt6hiayd9",
    canonicalParent: "Marriott International",
    aliases: Object.freeze(["Marriott International", "Marriott", "Marriott International, Inc."]),
  }),
  Object.freeze({
    slug: "ihg-managed",
    recordId: "rec7IXYQYpKMYsrDl",
    companyName: "IHG Hotels & Resorts (Managed)",
    explorerPath: "/operator-explorer-gold-mock.html?id=rec7IXYQYpKMYsrDl",
    canonicalParent: "IHG Hotels & Resorts",
    aliases: Object.freeze([
      "IHG Hotels & Resorts",
      "IHG",
      "InterContinental Hotels Group",
      "InterContinental Hotels Group (IHG)",
    ]),
  }),
  Object.freeze({
    slug: "hilton-managed",
    recordId: "rec3Uwxe6ovpiokuN",
    companyName: "Hilton (Managed)",
    explorerPath: "/operator-explorer-gold-mock.html?id=rec3Uwxe6ovpiokuN",
    canonicalParent: "Hilton",
    aliases: Object.freeze(["Hilton", "Hilton Worldwide", "Hilton Worldwide Holdings"]),
  }),
  Object.freeze({
    slug: "accor-managed",
    recordId: "recF2WqLqNVyKGz9E",
    companyName: "Accor (Managed)",
    explorerPath: "/operator-explorer-gold-mock.html?id=recF2WqLqNVyKGz9E",
    canonicalParent: "Accor",
    aliases: Object.freeze(["Accor", "AccorHotels", "Accor Hotels", "Accor Group"]),
  }),
  Object.freeze({
    slug: "minor-hotels-managed",
    recordId: "rec8SrT3VjRkkYTxm",
    companyName: "Minor Hotels (Managed)",
    explorerPath: "/operator-explorer-gold-mock.html?id=rec8SrT3VjRkkYTxm",
    canonicalParent: "Minor Hotels",
    aliases: Object.freeze(["Minor Hotels", "Minor International", "Minor Hotel Group"]),
  }),
]);

function normalizeParentKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

const ALIAS_INDEX = (() => {
  /** @type {Map<string, BrandManagedOperatorLink>} */
  const map = new Map();
  for (const link of BRAND_MANAGED_OPERATOR_LINKS) {
    for (const alias of link.aliases) {
      map.set(normalizeParentKey(alias), link);
    }
    map.set(normalizeParentKey(link.canonicalParent), link);
  }
  return map;
})();

/**
 * @param {string|null|undefined} parentCompany
 * @returns {BrandManagedOperatorLink | null}
 */
export function resolveBrandManagedOperatorLink(parentCompany) {
  const key = normalizeParentKey(parentCompany);
  if (!key) return null;
  if (ALIAS_INDEX.has(key)) return ALIAS_INDEX.get(key) || null;
  // Soft prefix match for "Marriott International …" variants
  for (const [aliasKey, link] of ALIAS_INDEX.entries()) {
    if (key === aliasKey || key.startsWith(aliasKey + " ") || key.startsWith(aliasKey + ",")) {
      return link;
    }
  }
  return null;
}

export function listBrandManagedOperatorLinks() {
  return [...BRAND_MANAGED_OPERATOR_LINKS];
}
