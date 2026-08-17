/**
 * Wave 13 Accor Active/Live identity anchors (recordId ↔ canonical slug).
 *
 * Durable after Wave 13 left factory preview — factory-preview candidates are
 * Wave 14-only and must not be the Active Accor identity source.
 *
 * Used by: active universe SoT, PVQL, 24-tab quality, footnote audit, 46 baseline.
 * Read-only identity map — never writes Airtable.
 */
export const WAVE13_ACTIVE_IDENTITY_VERSION = "wave13-active-identity-anchors-v1";

/** Canonical Active Accor Wave 13 public set (partial six + SO/). */
export const WAVE13_ACTIVE_IDENTITY_ANCHORS = Object.freeze([
  Object.freeze({
    slug: "mama-shelter",
    recordId: "recXCZCK05XXYX7Q8",
    name: "Mama Shelter",
    parentPlatform: "Accor",
  }),
  Object.freeze({
    slug: "mercure",
    recordId: "recevrLJ3m6rIug3S",
    name: "Mercure",
    parentPlatform: "Accor",
  }),
  Object.freeze({
    slug: "ibis",
    recordId: "reclFXbpZ5XzLWbGP",
    name: "ibis",
    parentPlatform: "Accor",
  }),
  Object.freeze({
    slug: "novotel",
    recordId: "recQE2lSSSSyuUrMQ",
    name: "Novotel",
    parentPlatform: "Accor",
  }),
  Object.freeze({
    slug: "pullman",
    recordId: "recFW9kfqKfOjv7Z1",
    name: "Pullman",
    parentPlatform: "Accor",
  }),
  Object.freeze({
    slug: "fairmont-hotels-and-resorts",
    recordId: "recJhPaDVU3YUDQUt",
    name: "Fairmont",
    parentPlatform: "Accor",
    nameAliases: Object.freeze(["Fairmont", "Fairmont Hotels & Resorts"]),
    slugAliases: Object.freeze(["fairmont"]),
  }),
  Object.freeze({
    slug: "so-hotels-and-resorts",
    recordId: "recTJdPlr4mDs9app",
    name: "SO/",
    parentPlatform: "Accor",
    nameAliases: Object.freeze(["SO/", "SO/ Hotels & Resorts", "SO Hotels & Resorts"]),
    slugAliases: Object.freeze(["so"]),
  }),
]);

/** Short slug → canonical slug for Fairmont / SO/ report identity. */
export const WAVE13_ACTIVE_SLUG_ALIASES = Object.freeze({
  fairmont: "fairmont-hotels-and-resorts",
  "fairmont-hotels-and-resorts": "fairmont",
  so: "so-hotels-and-resorts",
  "so-hotels-and-resorts": "so",
});

export function canonicalWave13ActiveSlug(slug) {
  const s = String(slug || "").trim().toLowerCase();
  if (s === "fairmont") return "fairmont-hotels-and-resorts";
  if (s === "so") return "so-hotels-and-resorts";
  return s;
}

export function getWave13ActiveIdentityBySlug(slug) {
  const wanted = canonicalWave13ActiveSlug(slug);
  return (
    WAVE13_ACTIVE_IDENTITY_ANCHORS.find((a) => a.slug === wanted) ||
    WAVE13_ACTIVE_IDENTITY_ANCHORS.find((a) => (a.slugAliases || []).includes(String(slug || "").trim().toLowerCase())) ||
    null
  );
}

export function getWave13ActiveIdentityByRecordId(recordId) {
  const id = String(recordId || "").trim();
  if (!id) return null;
  return WAVE13_ACTIVE_IDENTITY_ANCHORS.find((a) => a.recordId === id) || null;
}
