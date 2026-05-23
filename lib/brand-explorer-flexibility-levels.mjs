/**
 * Canonical vocabulary for operations.flexibility.* presentation slots.
 * UI: brand-explorer-atelier-from-api.js flexibilityFillClass()
 * Docs: docs/brand-explorer-presentation-slots.md
 */

export const FLEXIBILITY_SLOT_KEYS = [
  "operations.flexibility.design",
  "operations.flexibility.conversion",
  "operations.flexibility.localization",
  "operations.flexibility.operational_rigidity",
  "operations.flexibility.pip",
  "operations.flexibility.prototype",
];

export const CANONICAL_FLEX_LEVELS = [
  "Minimal",
  "Low",
  "Moderate",
  "Medium",
  "High",
  "Very high",
];

const SLOT_SUFFIX = {
  "operations.flexibility.design": "design",
  "operations.flexibility.conversion": "conversion",
  "operations.flexibility.localization": "localization",
  "operations.flexibility.operational_rigidity": "operational_rigidity",
  "operations.flexibility.pip": "pip",
  "operations.flexibility.prototype": "prototype",
};

/** @param {string} segment economy | extendedStay | upscale | softCollection | midscale */
export function flexLevelsForSegment(segment) {
  const tables = {
    economy: {
      design: "Moderate",
      conversion: "High",
      localization: "Low",
      operational_rigidity: "Moderate",
      pip: "Moderate",
      prototype: "Low",
    },
    extendedStay: {
      design: "Moderate",
      conversion: "High",
      localization: "Low",
      operational_rigidity: "Moderate",
      pip: "Moderate",
      prototype: "Low",
    },
    midscale: {
      design: "High",
      conversion: "Very high",
      localization: "Low",
      operational_rigidity: "High",
      pip: "Moderate",
      prototype: "Low",
    },
    upscale: {
      design: "High",
      conversion: "High",
      localization: "Moderate",
      operational_rigidity: "High",
      pip: "Moderate",
      prototype: "High",
    },
    softCollection: {
      design: "High",
      conversion: "High",
      localization: "High",
      operational_rigidity: "High",
      pip: "Moderate",
      prototype: "Moderate",
    },
  };
  return tables[segment] || tables.midscale;
}

export function flexLevelForSlot(segment, slotKey) {
  const suffix = SLOT_SUFFIX[slotKey];
  if (!suffix) return "Moderate";
  return flexLevelsForSegment(segment)[suffix] || "Moderate";
}

/**
 * Map compound / synonym text to a single canonical label for storage.
 * @param {string} raw
 * @returns {string|null} canonical level or null if narrative (not a level)
 */
export function coerceToCanonicalFlexLevel(raw) {
  const s = String(raw || "")
    .trim()
    .replace(/[–—]/g, "-");
  if (!s) return null;
  const lower = s.toLowerCase();
  if (/^(n\/?a|na|none|unknown|tbd|-)$/.test(lower)) return null;
  if (/^([1-6])(?:\s*\/\s*6)?$/.test(lower)) {
    const n = parseInt(lower[1], 10);
    return CANONICAL_FLEX_LEVELS[n - 1] || null;
  }
  if (/^very\s*high$/i.test(s)) return "Very high";
  if (/^high$/i.test(s)) return "High";
  if (/^medium$/i.test(s)) return "Medium";
  if (/^moderate$/i.test(s)) return "Moderate";
  if (/^minimal$/i.test(s)) return "Minimal";
  if (/^low$/i.test(s)) return "Low";
  if (/moderate\s+to\s+high|high\s+to\s+moderate|high-moderate/i.test(lower)) return "High";
  if (/low\s+to\s+moderate|moderate\s+to\s+low|low-moderate/i.test(lower)) return "Low";
  if (/^limited\b/i.test(lower)) return "Low";
  if (/^high[—-]/i.test(s)) return "High";
  if (s.length <= 24 && CANONICAL_FLEX_LEVELS.some((l) => l.toLowerCase() === lower)) {
    return CANONICAL_FLEX_LEVELS.find((l) => l.toLowerCase() === lower);
  }
  if (s.length > 48 || (s.match(/[.!?]/g) || []).length >= 1) return null;
  if (/\b(very\s*high|exceptional|maximum)\b/i.test(s)) return "Very high";
  if (/\b(high|strong|significant)\b/i.test(s) && !/\blow\b/i.test(s)) return "High";
  if (/\b(low|limited|light)\b/i.test(s) && !/\bhigh\b/i.test(s)) return "Low";
  if (/\bmoderate\b/i.test(s)) return "Moderate";
  return null;
}

/**
 * Editorial paragraphs moved out of flex slots (appended to standards philosophy).
 * @param {{ segment?: string, developmentModel?: string, name?: string }} profile
 */
export function flexEditorialSupplement(profile) {
  const segment = profile?.segment || "midscale";
  const name = profile?.name || "This brand";
  const developmentModel =
    profile?.developmentModel ||
    "Conversion and new construction—confirm path in FDD and development agreement.";

  const designNarr =
    segment === "softCollection"
      ? "Collection flexibility within standards—local character preserved where contract allows."
      : "Prototype-driven with conversion paths—confirm design manual for your asset.";
  const localizationNarr =
    segment === "softCollection"
      ? "Local F&B and design narrative encouraged within collection rules."
      : "Brand standards govern guest-facing consistency; limited localization of core guest touchpoints.";
  const prototypeNarr =
    segment === "upscale"
      ? "Design-forward prototype with local décor accents."
      : "Brand prototype with efficient footprint where applicable.";
  const pipNarr =
    "PIP at opening, conversion, and renewal per agreement—sequence with financing.";

  return [
    `Design flexibility (detail): ${designNarr}`,
    `Conversion & development (detail): ${developmentModel}`,
    `Localization (detail): ${localizationNarr}`,
    `PIP / lifecycle capital (detail): ${pipNarr}`,
    `Prototype dependence (detail): ${prototypeNarr}`,
  ].join("\n\n");
}

/**
 * @param {string} slotKey
 * @param {string} body
 * @param {string} segment
 * @returns {{ level: string, wasNarrative: boolean, prior: string }}
 */
export function normalizeFlexSlotBody(slotKey, body, segment) {
  const prior = String(body || "").trim();
  const coerced = coerceToCanonicalFlexLevel(prior);
  const level = coerced || flexLevelForSlot(segment, slotKey);
  return {
    level,
    wasNarrative: coerced === null && prior.length > 0,
    prior,
  };
}
