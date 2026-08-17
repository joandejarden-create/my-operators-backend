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

/** @param {string} slotKey */
export function isFlexibilitySlotKey(slotKey) {
  return FLEXIBILITY_SLOT_KEYS.includes(String(slotKey || "").trim());
}

/**
 * Infer flex segment for segment-default fallbacks when Body has no leading level.
 * Soft collections / lifestyle affiliation brands map to softCollection.
 * @param {string} brandName
 * @returns {"economy"|"extendedStay"|"upscale"|"softCollection"|"midscale"}
 */
export function inferFlexSegmentForBrand(brandName) {
  const key = String(brandName || "")
    .trim()
    .toLowerCase();
  if (!key) return "midscale";

  if (
    /ascend|autograph|curio|tapestry|tribute|vignette|handwritten|mgallery|design hotels|small luxury|\bslh\b|preferred hotels|kimpton|indigo|individuals|signature collection|premier collection|soft.?collection|collection by hilton|collection by marriott/.test(
      key
    )
  ) {
    return "softCollection";
  }
  if (/econo|rodeway|woodspring/.test(key)) return "economy";
  if (/mainstay|everhome|suburban/.test(key)) return "extendedStay";
  if (
    /cambria|radisson blu|radisson red|\bred\b|blu|park inn|country inn|radisson(?!\s+collection|\s+individuals)/.test(
      key
    )
  ) {
    return "upscale";
  }
  return "midscale";
}

/**
 * Write-path sanitizer: Flexibility Indicator Body must be one canonical level.
 * - Keeps / extracts leading levels (`Very high\nprose…` → `Very high`).
 * - Falls back to segment-appropriate default when Body is empty or pure narrative.
 *
 * @param {{ slotKey: string, body?: string, brandName?: string, segment?: string }} opts
 * @returns {{ level: string, prior: string, coerced: string|null, usedFallback: boolean, segment: string, changed: boolean }}
 */
export function sanitizeFlexibilityPresentationBody(opts) {
  const slotKey = String(opts?.slotKey || "").trim();
  const prior = String(opts?.body ?? "").trim();
  const segment =
    opts?.segment ||
    inferFlexSegmentForBrand(opts?.brandName || "") ||
    "midscale";

  if (!isFlexibilitySlotKey(slotKey)) {
    return {
      level: prior,
      prior,
      coerced: null,
      usedFallback: false,
      segment,
      changed: false,
    };
  }

  const coerced = coerceToCanonicalFlexLevel(prior);
  const level = coerced || flexLevelForSlot(segment, slotKey);
  return {
    level,
    prior,
    coerced,
    usedFallback: coerced === null,
    segment,
    changed: level !== prior,
  };
}

/**
 * Map a short level token / compound (optionally followed by prose) to a canonical label.
 * @param {string} s
 * @returns {string|null}
 */
function extractLeadingCanonicalFlexLevel(s) {
  const lower = String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[–—]/g, "-");
  if (!lower) return null;
  if (/^(n\/?a|na|none|unknown|tbd|-)$/.test(lower)) return null;
  if (/^([1-6])(?:\s*\/\s*6)?$/.test(lower)) {
    const n = parseInt(lower[1], 10);
    return CANONICAL_FLEX_LEVELS[n - 1] || null;
  }
  // Compounds before single tokens (Medium-high before Medium/High).
  if (
    /^(moderate\s+to\s+high|high\s+to\s+moderate|high-moderate|medium-high|medium\s+to\s+high)\b/.test(
      lower
    )
  ) {
    return "High";
  }
  if (
    /^(low\s+to\s+moderate|moderate\s+to\s+low|low-moderate|low\s+to\s+medium|low-medium)\b/.test(
      lower
    )
  ) {
    return "Low";
  }
  if (/^very\s*high\b/.test(lower)) return "Very high";
  if (/^very\s*low\b/.test(lower)) return "Minimal";
  if (/^minimal\b/.test(lower)) return "Minimal";
  if (/^moderate\b/.test(lower)) return "Moderate";
  if (/^medium\b/.test(lower)) return "Medium";
  if (/^high\b/.test(lower)) return "High";
  if (/^low\b/.test(lower)) return "Low";
  if (/^limited\b/.test(lower)) return "Low";
  if (/^exceptional\b|^maximum\b|^extensive\b/.test(lower)) return "Very high";
  return null;
}

/**
 * Map compound / synonym text to a single canonical label for storage.
 * Accepts Ascend-style `High` and compound `Very high\n…prose…` / `High Independent…`.
 * @param {string} raw
 * @returns {string|null} canonical level or null if narrative (not a level)
 */
export function coerceToCanonicalFlexLevel(raw) {
  const full = String(raw || "")
    .trim()
    .replace(/[–—]/g, "-");
  if (!full) return null;

  // Prefer first line when Body is "Level\nprose" (common soft-brand mistake).
  const firstLine = full.split(/\r?\n/)[0].trim();
  const fromFirst = extractLeadingCanonicalFlexLevel(firstLine);
  if (fromFirst) return fromFirst;

  if (firstLine !== full) {
    const fromFullLead = extractLeadingCanonicalFlexLevel(full);
    if (fromFullLead) return fromFullLead;
  }

  const lower = full.toLowerCase();
  if (full.length <= 24 && CANONICAL_FLEX_LEVELS.some((l) => l.toLowerCase() === lower)) {
    return CANONICAL_FLEX_LEVELS.find((l) => l.toLowerCase() === lower);
  }
  // Pure narrative (no leading level token) — do not guess from buried keywords.
  if (full.length > 48 || (full.match(/[.!?]/g) || []).length >= 1) return null;
  if (/\b(very\s*high|exceptional|maximum)\b/i.test(full)) return "Very high";
  if (/\b(high|strong|significant)\b/i.test(full) && !/\blow\b/i.test(full)) return "High";
  if (/\b(low|limited|light)\b/i.test(full) && !/\bhigh\b/i.test(full)) return "Low";
  if (/\bmoderate\b/i.test(full)) return "Moderate";
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
