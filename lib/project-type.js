/**
 * Deals → Project Type — canonical options, legacy aliases, and classification helpers.
 * Single source of truth for intake, readiness, brand alignment, and operator capability.
 */

/** Canonical Airtable / intake values (final list). */
export const PROJECT_TYPE_CANONICAL_OPTIONS = [
  "New Build",
  "Conversion / Reflag",
  "Renovation / Repositioning",
  "Existing Operating Hotel",
  "Adaptive Reuse",
  "Mixed-Use Hospitality Project",
  "Other / To Be Confirmed",
];

/**
 * Legacy values that may exist on older records — recognized for reads/rules, not offered on new intake.
 * @type {Record<string, string>}
 */
export const PROJECT_TYPE_LEGACY_TO_CANONICAL = {
  "Renovation / repositioning (open hotel)": "Renovation / Repositioning",
  Conversion: "Conversion / Reflag",
  "Acquisition of operating hotel": "Existing Operating Hotel",
  "Land / greenfield only": "New Build",
};

/** @typedef {'new_build'|'conversion_reflag'|'renovation_repositioning'|'existing_operating'|'adaptive_reuse'|'mixed_use'|'other_tbc'|'unknown'} ProjectTypeKind */

/**
 * @param {unknown} raw
 * @returns {string}
 */
export function strProjectType(raw) {
  if (raw == null || raw === "") return "";
  if (typeof raw === "string") return raw.trim();
  if (typeof raw === "object" && raw.name) return String(raw.name).trim();
  return String(raw).trim();
}

/**
 * Map legacy / typo values to canonical label (empty if unknown).
 * @param {unknown} raw
 * @returns {string}
 */
export function normalizeProjectTypeLabel(raw) {
  const s = strProjectType(raw);
  if (!s) return "";
  if (PROJECT_TYPE_CANONICAL_OPTIONS.includes(s)) return s;
  const mapped = PROJECT_TYPE_LEGACY_TO_CANONICAL[s];
  if (mapped) return mapped;
  if (/^conversion\s*\/?\s*reflag?$/i.test(s)) return "Conversion / Reflag";
  if (/^new\s*build$/i.test(s)) return "New Build";
  if (/renovation.*reposition|reposition.*renovation/i.test(s) && !/open hotel/i.test(s)) {
    return "Renovation / Repositioning";
  }
  if (/acquisition.*operating|operating.*acquisition/i.test(s)) {
    return "Existing Operating Hotel";
  }
  if (/land.*greenfield|greenfield.*only/i.test(s)) return "New Build";
  return s;
}

/**
 * @param {unknown} raw
 * @returns {ProjectTypeKind}
 */
export function resolveProjectTypeKind(raw) {
  const canonical = normalizeProjectTypeLabel(raw);
  const s = (canonical || strProjectType(raw)).toLowerCase();
  if (!s) return "unknown";

  if (/^new build$|ground.?up|development hotel/i.test(s)) return "new_build";
  if (/conversion|reflag|re-flag|brand change|affiliation change/i.test(s)) return "conversion_reflag";
  if (/renovation|reposition/i.test(s) && !/mixed-use/i.test(s)) return "renovation_repositioning";
  if (/existing operating|operating hotel|stabilized operating/i.test(s)) return "existing_operating";
  if (/adaptive reuse/i.test(s)) return "adaptive_reuse";
  if (/mixed-use|mixed use/i.test(s)) return "mixed_use";
  if (/other.*to be confirmed|to be confirmed|tbd|not sure/i.test(s)) return "other_tbc";

  if (/operating|existing hotel|open hotel/i.test(s)) return "existing_operating";
  if (/adaptive/i.test(s)) return "adaptive_reuse";

  return "unknown";
}

/** @param {ProjectTypeKind} kind */
export function isNewBuildProjectType(kind) {
  return kind === "new_build";
}

/** Conversion / reflag (not renovation-only). */
export function isConversionReflagProjectType(kind) {
  return kind === "conversion_reflag";
}

export function isRenovationRepositioningProjectType(kind) {
  return kind === "renovation_repositioning";
}

export function isExistingOperatingProjectType(kind) {
  return kind === "existing_operating";
}

/** Includes conversion, renovation, reflag patterns (brand alignment / opening phase). */
export function isTransitionProjectTypeKind(kind) {
  return (
    kind === "conversion_reflag" ||
    kind === "renovation_repositioning" ||
    kind === "adaptive_reuse"
  );
}

export function isOtherToBeConfirmedProjectType(kind) {
  return kind === "other_tbc";
}

/**
 * @param {unknown} raw
 */
export function isConversionDealProjectType(raw) {
  const kind = resolveProjectTypeKind(raw);
  return kind === "conversion_reflag" || kind === "renovation_repositioning";
}

/**
 * Backfill: map deprecated Land / greenfield only using site/development signals.
 * @param {Record<string, unknown>} merged
 * @returns {{ value: string, note: string }}
 */
export function mapLegacyLandGreenfieldProjectType(merged) {
  const stage = strProjectType(merged["Stage of Development"]).toLowerCase();
  const site = strProjectType(merged["Current Form of Site Control"]).toLowerCase();
  const zoning = strProjectType(merged["Zoned for Hotel Development"]).toLowerCase();
  if (
    /new build/i.test(strProjectType(merged["Project Type"])) ||
    /under construction|entitlement|land under control|fully entitled|pre-construction/i.test(stage) ||
    /owned|leased|option|site control/i.test(site) ||
    /^yes$/i.test(zoning)
  ) {
    return { value: "New Build", note: "Legacy Land / greenfield only → New Build (development signals)" };
  }
  return {
    value: "Other / To Be Confirmed",
    note: "Legacy Land / greenfield only → Other / To Be Confirmed (insufficient development signals)",
  };
}

/**
 * True when value is a deprecated intake option that should not be written as Project Type.
 * @param {string} label
 */
export function isDeprecatedProjectTypeWriteValue(label) {
  const s = strProjectType(label);
  return (
    s === "Land / greenfield only" ||
    s === "Acquisition of operating hotel" ||
    s === "Renovation / repositioning (open hotel)"
  );
}

/**
 * Options to merge into Airtable that are not in the default base set.
 */
export function projectTypeOptionsToEnsure(existingNames) {
  const have = new Set(existingNames || []);
  return PROJECT_TYPE_CANONICAL_OPTIONS.filter((n) => !have.has(n));
}
