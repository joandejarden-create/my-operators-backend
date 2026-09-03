/**
 * Governed property-specific subject aliases (Option A).
 * Aliases are NEVER global substring shortcuts — location/market guards required.
 */

export const GOVERNED_SUBJECT_ALIAS_POLICY_VERSION = "adp_governed_subject_aliases_v1";

/** Approved property-scoped aliases only. */
export const GOVERNED_APPROVED_ALIASES = Object.freeze({
  adp_renaissance_times_square: Object.freeze([
    "Renaissance NYC Times Square",
    "Renaissance Times Square",
    "Renaissance New York Times Square",
  ]),
});

/** Explicitly rejected as standalone aliases — never add to general alias list. */
export const GOVERNED_REJECTED_ALIASES = Object.freeze({
  adp_renaissance_times_square: Object.freeze([
    "Renaissance New York",
    "Renaissance",
    "Renaissance NYC", // standalone rejected; may resolve only via contextual entity resolution
  ]),
});

/** Required location tokens (normalized haystack) for any alias on the property. */
export const GOVERNED_ALIAS_LOCATION_GUARDS = Object.freeze({
  adp_renaissance_times_square: Object.freeze(["times square"]),
});

export function getGovernedApprovedAliases(propertyId) {
  return [...(GOVERNED_APPROVED_ALIASES[propertyId] || [])];
}

export function getGovernedRejectedAliases(propertyId) {
  return [...(GOVERNED_REJECTED_ALIASES[propertyId] || [])];
}

/**
 * Whether an alias string may be used for subject matching on this property.
 */
export function isGovernedAliasAllowed(alias, propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (!propertyId || !alias) return false;
  const needle = String(alias)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (needle.length < 4) return false;

  for (const banned of getGovernedRejectedAliases(propertyId)) {
    const b = String(banned)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (needle === b) return false;
  }

  const guards = GOVERNED_ALIAS_LOCATION_GUARDS[propertyId] || [];
  for (const tok of guards) {
    if (!needle.includes(tok)) return false;
  }
  return true;
}

/**
 * Merge profile aliases + governed approved list; drop rejected / unguarded.
 */
export function resolveSubjectAliasesForProfile(propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  const fromProfile = [
    ...(propertyProfile?.identityAliases || []),
    ...(propertyProfile?.aliases || []),
  ];
  const fromGoverned = getGovernedApprovedAliases(propertyId);
  const merged = [...fromProfile, ...fromGoverned];
  const out = [];
  const seen = new Set();
  for (const a of merged) {
    const t = String(a || "").trim();
    if (!t || seen.has(t.toLowerCase())) continue;
    if (!isGovernedAliasAllowed(t, propertyProfile)) continue;
    seen.add(t.toLowerCase());
    out.push(t);
  }
  return out;
}
