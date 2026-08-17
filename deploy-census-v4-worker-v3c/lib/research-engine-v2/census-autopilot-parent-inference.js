/**
 * Read-only Brand Setup parent inference for Autopilot routing only.
 * Never writes Brand Setup / Brand Explorer.
 *
 * Brand Setup often lacks Parent Company; Active Setup discovery would otherwise
 * skip Accor / Wyndham / Preferred / etc. unless slug→parent is inferred.
 */

export const PARENT_INFERENCE_VERSION = "census-autopilot-parent-inference-v1";

/**
 * Known Active/Live brand slug → parent company (Autopilot routing only).
 * Keep aligned with Brand Explorer Active/Live universe + gap-list anchors.
 */
export const PARENT_BY_SLUG = Object.freeze({
  // Marriott
  "aloft-hotels": "Marriott",
  "autograph-collection": "Marriott",
  "ac-hotels-by-marriott": "Marriott",
  "city-express-by-marriott": "Marriott",
  "courtyard-by-marriott": "Marriott",
  "marriott-hotels": "Marriott",
  "moxy-hotels": "Marriott",
  "residence-inn-by-marriott": "Marriott",
  sheraton: "Marriott",
  "springhill-suites-by-marriott": "Marriott",
  studiores: "Marriott",
  "towneplace-suites-by-marriott": "Marriott",
  "tribute-portfolio": "Marriott",
  westin: "Marriott",
  // Hilton
  "canopy-by-hilton": "Hilton",
  "curio-collection": "Hilton",
  "doubletree-by-hilton": "Hilton",
  "hampton-by-hilton": "Hilton",
  "hilton-garden-inn": "Hilton",
  "hilton-hotels-and-resorts": "Hilton",
  "home2-suites-by-hilton": "Hilton",
  "homewood-suites-by-hilton": "Hilton",
  "motto-by-hilton": "Hilton",
  "spark-by-hilton": "Hilton",
  "tempo-by-hilton": "Hilton",
  "tru-by-hilton": "Hilton",
  "tapestry-collection-by-hilton": "Hilton",
  // Choice
  ascend: "Choice",
  "comfort-inn-suites": "Choice",
  "country-inn-suites": "Choice",
  "quality-inn": "Choice",
  radisson: "Choice",
  "radisson-blu": "Choice",
  "radisson-red": "Choice",
  "radisson-individuals-by-choice": "Choice",
  "suburban-studios": "Choice",
  // IHG
  "avid-hotels": "IHG",
  "even-hotels": "IHG",
  "holiday-inn-express": "IHG",
  "hotel-indigo": "IHG",
  kimpton: "IHG",
  "voco-hotels": "IHG",
  "handwritten-collection": "IHG",
  "vignette-collection": "IHG",
  // Accor
  "design-hotels": "Accor",
  "fairmont-hotels-and-resorts": "Accor",
  ibis: "Accor",
  mercure: "Accor",
  novotel: "Accor",
  pullman: "Accor",
  "mgallery-collection": "Accor",
  "mama-shelter": "Accor",
  "so-hotels-and-resorts": "Accor",
  // Wyndham
  "dazzler-by-wyndham": "Wyndham",
  "trademark-collection-by-wyndham": "Wyndham",
  "woodspring-suites": "Wyndham",
  "everhome-suites": "Wyndham",
  // Soft / representation
  "bw-premier-collection": "BWH Hotels",
  "bw-signature-collection": "BWH Hotels",
  "preferred-hotels-and-resorts": "Preferred Hotels & Resorts",
  "small-luxury-hotels-of-the-world": "SLH",
  "bunkhouse-hotels": "Bunkhouse",
});

/**
 * Slug-token heuristics when PARENT_BY_SLUG has no exact hit (lower confidence).
 * @param {string} slug
 */
export function inferParentFromSlugTokens(slug) {
  const s = String(slug || "").toLowerCase();
  if (!s) return null;
  if (s.includes("marriott") || s.includes("sheraton") || s.includes("westin") || s.includes("aloft")) {
    return "Marriott";
  }
  if (s.includes("hilton") || s.includes("hampton") || s.includes("doubletree") || s.includes("curio")) {
    return "Hilton";
  }
  if (
    s.includes("choice") ||
    s.includes("radisson") ||
    s.includes("ascend") ||
    s.includes("comfort") ||
    s.includes("quality-inn")
  ) {
    return "Choice";
  }
  if (
    s.includes("ihg") ||
    s.includes("holiday-inn") ||
    s.includes("indigo") ||
    s.includes("kimpton") ||
    s.includes("voco") ||
    s.includes("avid")
  ) {
    return "IHG";
  }
  if (
    s.includes("ibis") ||
    s.includes("novotel") ||
    s.includes("mercure") ||
    s.includes("pullman") ||
    s.includes("mgallery") ||
    s.includes("fairmont") ||
    s.includes("mama-shelter") ||
    s.includes("design-hotels") ||
    s.includes("sofitel")
  ) {
    return "Accor";
  }
  if (
    s.includes("wyndham") ||
    s.includes("dazzler") ||
    s.includes("trademark") ||
    s.includes("woodspring") ||
    s.includes("everhome") ||
    s.includes("la-quinta") ||
    s.includes("laquinta") ||
    s.includes("ramada") ||
    s.includes("days-inn") ||
    s.includes("microtel")
  ) {
    return "Wyndham";
  }
  if (s.includes("preferred")) return "Preferred Hotels & Resorts";
  if (s.includes("best-western") || s.startsWith("bw-")) return "BWH Hotels";
  if (s.includes("small-luxury") || s.includes("slh")) return "SLH";
  if (s.includes("bunkhouse")) return "Bunkhouse";
  return null;
}

/**
 * Resolve parent for Autopilot routing (read-only).
 * @param {{
 *   brand_slug?: string|null,
 *   slug?: string|null,
 *   parent_company?: string|null,
 *   parentCompany?: string|null,
 *   parentPlatform?: string|null,
 *   parent?: string|null,
 *   family?: string|null,
 * }} brand
 * @returns {{
 *   parent_company: string|null,
 *   parent_company_raw: string|null,
 *   inferred: boolean,
 *   inference_confidence: 'High'|'Medium'|'Low'|null,
 *   inference_source: string|null,
 * }}
 */
export function inferParentCompanyForAutopilot(brand = {}) {
  const slug = String(brand.brand_slug || brand.slug || "").trim();
  const raw =
    brand.parent_company ||
    brand.parentCompany ||
    brand.parentPlatform ||
    brand.parent ||
    brand.family ||
    null;
  const rawTrim = raw != null && String(raw).trim() ? String(raw).trim() : null;

  if (rawTrim) {
    return {
      parent_company: rawTrim,
      parent_company_raw: rawTrim,
      inferred: false,
      inference_confidence: "High",
      inference_source: "brand_setup_parent_field",
    };
  }

  if (slug && PARENT_BY_SLUG[slug]) {
    return {
      parent_company: PARENT_BY_SLUG[slug],
      parent_company_raw: null,
      inferred: true,
      inference_confidence: "High",
      inference_source: "parent_by_slug_map",
    };
  }

  const tokenParent = inferParentFromSlugTokens(slug);
  if (tokenParent) {
    return {
      parent_company: tokenParent,
      parent_company_raw: null,
      inferred: true,
      inference_confidence: "Medium",
      inference_source: "slug_token_heuristic",
    };
  }

  return {
    parent_company: null,
    parent_company_raw: null,
    inferred: false,
    inference_confidence: null,
    inference_source: null,
  };
}
