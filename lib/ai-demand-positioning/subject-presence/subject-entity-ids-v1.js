/**
 * Subject canonical entity IDs for a property — shared by presence, displacement,
 * and competitor extraction. Universal (not hotel-specific string filters).
 */

import { getEntityRegistryForProperty } from "../metrics/adp-property-entity-registries.js";

/**
 * Active subject entity id for a property (never every hotel marked subject:true
 * in a shared market registry — that incorrectly treats peers as the subject).
 *
 * @param {object|null} propertyProfile
 * @returns {string|null}
 */
export function primarySubjectEntityIdForProperty(propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (!propertyId) return null;
  if (propertyProfile.entityId) return String(propertyProfile.entityId);
  if (propertyProfile.canonicalEntityId) return String(propertyProfile.canonicalEntityId);
  if (propertyProfile.canonicalHotelId) return String(propertyProfile.canonicalHotelId);
  if (propertyId.startsWith("adp_")) return propertyId.replace(/^adp_/, "");
  return null;
}

function hotelBelongsToActiveSubject(hotel, primaryEntityId, propertyId) {
  if (!hotel) return false;
  const hid = String(hotel.entityId || "");
  if (primaryEntityId && hid === primaryEntityId) return true;
  if (hotel.subjectPropertyId && propertyId && hotel.subjectPropertyId === propertyId) {
    return true;
  }
  // Legacy shared-market registries marked multiple hotels subject:true.
  // Only accept subject:true when the entity id matches this property's slug.
  if (hotel.subject === true && primaryEntityId && hid === primaryEntityId) return true;
  return false;
}

/**
 * @param {object|null} propertyProfile
 * @returns {Set<string>}
 */
export function subjectEntityIdsForProperty(propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  const ids = new Set();
  if (!propertyId) return ids;

  const primary = primarySubjectEntityIdForProperty(propertyProfile);
  if (primary) ids.add(primary);

  const reg = getEntityRegistryForProperty(propertyId);
  for (const h of reg?.hotels || []) {
    if (hotelBelongsToActiveSubject(h, primary, propertyId)) {
      ids.add(String(h.entityId));
    }
  }

  return ids;
}

/**
 * Subject aliases from the market entity registry for THIS property only.
 * Shared market registries must not inject peer hotels (e.g. JW into Westin).
 *
 * @param {object|null} propertyProfile
 * @returns {string[]}
 */
export function subjectRegistryAliasesForProperty(propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (!propertyId) return [];
  const primary = primarySubjectEntityIdForProperty(propertyProfile);
  const reg = getEntityRegistryForProperty(propertyId);
  const out = [];
  for (const h of reg?.hotels || []) {
    if (!hotelBelongsToActiveSubject(h, primary, propertyId)) continue;
    if (h.canonical) out.push(String(h.canonical));
    for (const a of h.aliases || []) {
      if (a) out.push(String(a));
    }
  }
  return out;
}
