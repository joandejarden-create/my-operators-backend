/**
 * Match GTM Contacts to Owner Target CALA footprints via company / owner name.
 */
import { normalizeOwnerKey } from "./normalize.js";
import { isPartialNormalizedMatch, isExactOnlyOwnerKey, normalizeForMatch } from "./name-match.js";
import {
  buildOwnerCalaFootprintIndex,
  lookupOwnerFootprint,
  mergeOwnerFootprints,
} from "./cala-footprint.js";

/**
 * @param {string} label
 * @param {Map<string, string[]>} nameAliasIndex ownerKey → display names
 * @param {Map<string, object>} ownerFootprintIndex
 */
function resolveNameToFootprint(label, nameAliasIndex, ownerFootprintIndex) {
  const raw = String(label || "").trim();
  if (!raw) return { matchType: "none", matchedOn: null, matchedOwnerName: null, footprint: null };

  const directHits = lookupOwnerFootprint(raw, ownerFootprintIndex);
  if (directHits.length) {
    return {
      matchType: "owner_exact",
      matchedOn: raw,
      matchedOwnerName: directHits.map((h) => h.ownerName).join(" | "),
      footprint: mergeOwnerFootprints(directHits),
    };
  }

  const norm = normalizeOwnerKey(raw);
  const loose = normalizeForMatch(raw);

  for (const [ownerKey, names] of nameAliasIndex) {
    for (const alias of names) {
      const aliasNorm = normalizeOwnerKey(alias);
      const aliasLoose = normalizeForMatch(alias);
      if (aliasNorm === norm || (loose.length >= 4 && aliasLoose === loose)) {
        const hits = lookupOwnerFootprint(alias, ownerFootprintIndex);
        if (hits.length) {
          return {
            matchType: "alias_exact",
            matchedOn: raw,
            matchedOwnerName: hits.map((h) => h.ownerName).join(" | "),
            footprint: mergeOwnerFootprints(hits),
          };
        }
        const hit = ownerFootprintIndex.get(ownerKey);
        if (hit) {
          return {
            matchType: "alias_exact",
            matchedOn: raw,
            matchedOwnerName: hit.ownerName,
            footprint: hit.footprint,
          };
        }
      }
    }
  }

  if (norm.length >= 4 || loose.length >= 4) {
    for (const [ownerKey, hit] of ownerFootprintIndex) {
      if (isExactOnlyOwnerKey(ownerKey)) continue;
      const ownerLoose = normalizeForMatch(hit.ownerName);
      if (
        isPartialNormalizedMatch(norm, ownerKey) ||
        isPartialNormalizedMatch(loose, ownerLoose)
      ) {
        return {
          matchType: "partial",
          matchedOn: raw,
          matchedOwnerName: hit.ownerName,
          footprint: hit.footprint,
        };
      }
    }
  }

  return { matchType: "none", matchedOn: raw, matchedOwnerName: null, footprint: null };
}

/**
 * @param {object} options
 * @param {{ ownerName: string, properties: object[] }[]} options.ownerGroups
 * @param {string[]} [options.companyNames]
 * @param {{ company: { name: string }, ownerTarget?: { preferredNames?: string[] } | null }[]} [options.profileEnrichments]
 */
export function buildContactCalaMatchContext({
  ownerGroups,
  companyNames = [],
  profileEnrichments = [],
}) {
  const ownerFootprintIndex = buildOwnerCalaFootprintIndex(ownerGroups);

  /** @type {Map<string, string[]>} */
  const nameAliasIndex = new Map();
  const addAlias = (name) => {
    const label = String(name || "").trim();
    if (!label) return;
    const key = normalizeOwnerKey(label);
    if (!key) return;
    if (!nameAliasIndex.has(key)) nameAliasIndex.set(key, []);
    const list = nameAliasIndex.get(key);
    if (!list.includes(label)) list.push(label);
  };

  for (const group of ownerGroups) addAlias(group.ownerName);
  for (const companyName of companyNames) addAlias(companyName);
  for (const profile of profileEnrichments) {
    addAlias(profile.company?.name);
    for (const name of profile.ownerTarget?.preferredNames || []) addAlias(name);
  }

  return { ownerFootprintIndex, nameAliasIndex };
}

/**
 * @param {{ company?: string, name?: string }} contact
 * @param {ReturnType<buildContactCalaMatchContext>} context
 */
export function classifyContactCalaFootprint(contact, context) {
  const company = String(contact.company || "").trim();
  const resolution = resolveNameToFootprint(company, context.nameAliasIndex, context.ownerFootprintIndex);

  let calaHotelContact = "unknown";
  if (resolution.footprint) {
    calaHotelContact = resolution.footprint.hasCalaHotels ? "yes" : "no";
  }

  return {
    calaHotelContact,
    matchType: resolution.matchType,
    matchedOwnerName: resolution.matchedOwnerName,
    matchedOn: resolution.matchedOn,
    calaPropertyCount: resolution.footprint?.calaPropertyCount ?? null,
    totalPropertyCount: resolution.footprint?.totalPropertyCount ?? null,
    calaCountriesSummary: resolution.footprint?.calaCountriesSummary || "",
    calaOnly: resolution.footprint?.calaOnly ?? null,
  };
}
