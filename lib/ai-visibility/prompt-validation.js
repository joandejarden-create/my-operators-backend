/**
 * Prompt validation + neutrality checks (deterministic).
 */

import {
  validateCountryRegionPair,
  COMMERCIAL_REGIONS,
} from "./commercial-geography.js";

export const PROMPT_VALIDATION_VERSION = "ai_visibility_prompt_validation_v1";

const INTENT_TERRITORIES = new Set([
  "Brand Selection",
  "Operator Selection",
  "Conversion",
  "New Build",
  "HMA vs Franchise",
  "Owner Economics",
  "Owner Flexibility",
  "Branded Residences",
  "Mixed Use",
  "Market / Geography",
  "Chain Scale / Positioning",
  "Development Strategy",
  "Other",
  // Phase 3A.9 showcase territories
  "Collection / Soft Brand",
  "Lifestyle Positioning",
  "Upper-Upscale Positioning",
  "Soft-Brand Affiliation Flexibility",
]);

const ENTITY_SCOPES = new Set(["Brand", "Operator", "Both"]);
const GEOGRAPHY_SCOPES = new Set(["Global", "Region", "Subregion", "Country", "Market"]);
const LANGUAGES = new Set(["en", "es", "English", "Spanish"]);

const BIAS_PATTERNS = [
  /\bwhy\s+is\s+[A-Z][\w\s&'-]{1,40}\s+the\s+best\b/i,
  /\bbest\s+(?:brand|operator)\s+for\b/i,
  /\bguarantee(?:d|s)?\b/i,
  /\bdealality\s+recommends\b/i,
];

/**
 * @param {object} row
 * @returns {{ ok: boolean, errors: string[], warnings: string[] }}
 */
export function validatePromptRow(row = {}) {
  const errors = [];
  const warnings = [];

  if (!String(row.promptId || "").trim()) errors.push("missing_prompt_id");
  if (!String(row.promptName || "").trim()) errors.push("missing_prompt_name");
  if (!String(row.promptText || "").trim()) errors.push("missing_prompt_text");
  if (!String(row.version || "").trim()) errors.push("missing_version");
  if (!INTENT_TERRITORIES.has(row.intentTerritory)) {
    errors.push(`unsupported_intent_territory:${row.intentTerritory}`);
  }
  if (!ENTITY_SCOPES.has(row.entityScope)) {
    errors.push(`unsupported_entity_scope:${row.entityScope}`);
  }
  if (!GEOGRAPHY_SCOPES.has(row.geographyScope)) {
    errors.push(`unsupported_geography_scope:${row.geographyScope}`);
  }

  if (row.language != null && String(row.language).trim() !== "") {
    if (!LANGUAGES.has(String(row.language).trim()) && !LANGUAGES.has(String(row.language).toLowerCase())) {
      // accept en/es via normalize later; reject locales
      const lang = String(row.language).trim().toLowerCase();
      if (lang.includes("-") || lang.includes("_")) {
        errors.push("language_must_not_be_locale");
      } else if (!["en", "es", "english", "spanish"].includes(lang)) {
        errors.push(`unsupported_language:${row.language}`);
      }
    }
  }

  // Wave-1: Spanish only for CALA region or Mexico country
  const langNorm = String(row.language || "").trim().toLowerCase();
  const isEs = langNorm === "es" || langNorm === "spanish";
  if (isEs) {
    const isCala = row.commercialRegion === "CALA" && row.geographyScope === "Region" && !row.country;
    const isMexico = row.country === "Mexico" && row.geographyScope === "Country";
    if (!isCala && !isMexico) {
      errors.push("spanish_wave1_only_cala_or_mexico");
    }
  }

  if (row.geographyScope === "Global") {
    if (row.commercialRegion) {
      errors.push("global_must_not_set_commercial_region");
    }
    if (row.country) errors.push("global_must_not_set_country");
  }

  if (row.geographyScope === "Region") {
    if (!row.commercialRegion) errors.push("region_scope_requires_commercial_region");
    if (row.country) {
      warnings.push("region_scope_has_country_ignored_for_headline_metrics");
    }
    if (row.commercialRegion && !COMMERCIAL_REGIONS.includes(row.commercialRegion)) {
      errors.push(`unsupported_commercial_region:${row.commercialRegion}`);
    }
  }

  if (row.geographyScope === "Country") {
    if (!row.country) errors.push("country_scope_requires_country");
    const pair = validateCountryRegionPair(row.country, row.commercialRegion);
    if (!pair.ok) errors.push(pair.error);
  }

  if (row.geographyScope === "Subregion" && !row.subregion) {
    errors.push("subregion_scope_requires_subregion");
  }

  const text = String(row.promptText || "");
  for (const re of BIAS_PATTERNS) {
    if (re.test(text)) {
      errors.push(`non_neutral_wording:${re.source}`);
    }
  }

  return { ok: errors.length === 0, errors, warnings };
}

/**
 * Validate a full seed set for duplicates and row rules.
 * @param {object[]} rows
 */
export function validatePromptSeedSet(rows = []) {
  const errors = [];
  const warnings = [];
  const seenIdVersion = new Map();
  const seenIds = new Map();

  for (const row of rows) {
    const key = `${row.promptId}::${row.version}`;
    if (seenIdVersion.has(key)) {
      errors.push(`duplicate_prompt_id_version:${key}`);
    }
    seenIdVersion.set(key, true);

    if (seenIds.has(row.promptId) && seenIds.get(row.promptId) !== row.version) {
      // Same ID different version is OK (versioning). Track only.
    }
    seenIds.set(row.promptId, row.version);

    const result = validatePromptRow(row);
    for (const e of result.errors) errors.push(`${row.promptId}:${e}`);
    for (const w of result.warnings) warnings.push(`${row.promptId}:${w}`);
  }

  return {
    ok: errors.length === 0,
    PROMPTS_PROPOSED: rows.length,
    PROMPTS_VALIDATED: rows.filter((r) => validatePromptRow(r).ok).length,
    errors,
    warnings,
  };
}
