/**
 * Strip legacy STR-era "a Member of {Brand}" suffixes from Hotel Census `name`.
 * Dealality uses Affiliation + Parent Company — not member-of phrasing in display names.
 */

const LEGACY_MEMBER_OF_SUFFIXES = [
  /,\s*a\s+member\s+of\s+design\s+hotels\s*$/i,
  /\s+a\s+member\s+of\s+design\s+hotels\s*$/i,
  /,\s*member\s+of\s+design\s+hotels\s*$/i,
  /\s+member\s+of\s+design\s+hotels\s*$/i,
];

/**
 * @param {string} name
 * @returns {{ canonical: string, changed: boolean, previous: string }}
 */
export function normalizeDesignHotelsCensusName(name) {
  const previous = String(name || "").trim();
  if (!previous) {
    return { canonical: previous, changed: false, previous };
  }

  let canonical = previous;
  for (const pattern of LEGACY_MEMBER_OF_SUFFIXES) {
    const next = canonical.replace(pattern, "").trim();
    if (next !== canonical) {
      canonical = next;
      break;
    }
  }

  canonical = canonical.replace(/,\s*$/, "").trim();

  return {
    canonical,
    changed: canonical !== previous,
    previous,
  };
}

/**
 * @param {string} name
 * @returns {boolean}
 */
export function hasLegacyMemberOfDesignHotelsSuffix(name) {
  return normalizeDesignHotelsCensusName(name).changed;
}
