/**
 * Organization-level aggregates derived from the ownership graph + Census joins.
 * Not a separate portfolio database (P1.7).
 *
 * Never mass-ensures hotel_ids for the full Census — only joins hotels already
 * present on relationships (or already mapped).
 */

import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";

export const ORGANIZATION_AGGREGATES_VERSION = "ownership-organization-aggregates-v1";

function censusIndexByAirtableAndHotelId(censusRecords, idRegistry) {
  const byAirtable = new Map();
  const byHotelId = new Map();
  for (const rec of censusRecords || []) {
    if (!rec?.id) continue;
    byAirtable.set(rec.id, rec);
    const existing = idRegistry?.getByAirtableId?.(rec.id)?.hotel_id || null;
    if (existing) byHotelId.set(existing, rec);
  }
  return { byAirtable, byHotelId };
}

/**
 * @param {object} repo
 * @param {string} entityId
 * @param {{ censusRecords?: object[], idRegistry?: object }} [opts]
 */
export async function deriveOrganizationAggregates(repo, entityId, opts = {}) {
  const entity = await repo.getEntity(entityId);
  if (!entity) {
    return { ok: false, error: "entity_not_found" };
  }
  const rels = await repo.listRelationshipsForEntity(entityId, { currentOnly: false });
  const { byAirtable, byHotelId } = censusIndexByAirtableAndHotelId(
    opts.censusRecords || [],
    opts.idRegistry
  );

  const currentHotels = [];
  const historicalHotels = [];
  const countries = new Set();
  const brands = new Set();
  const operators = new Set();
  const relMix = {};
  let rooms = 0;

  for (const rel of rels || []) {
    const type = rel.relationship_type || "unknown";
    relMix[type] = (relMix[type] || 0) + 1;
    const hotelId = rel.subject_hotel_id;
    if (!hotelId) continue;

    let rec = byHotelId.get(hotelId) || null;
    if (!rec && opts.idRegistry?.getByHotelId) {
      const mapped = opts.idRegistry.getByHotelId(hotelId);
      if (mapped?.airtable_record_id) {
        rec = byAirtable.get(mapped.airtable_record_id) || null;
      }
    }

    const f = rec?.fields || {};
    const name =
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      hotelId;
    const country = f[MAP_CENSUS_FIELDS.country] || null;
    const brand = f[MAP_CENSUS_FIELDS.brandName] || null;
    const operator = f[MAP_CENSUS_FIELDS.operatorName] || null;
    const roomCount = Number(f[MAP_CENSUS_FIELDS.roomCount]);
    if (country) countries.add(country);
    if (brand) brands.add(brand);
    if (operator) operators.add(operator);
    if (Number.isFinite(roomCount) && roomCount > 0) rooms += roomCount;

    const row = {
      hotel_id: hotelId,
      name,
      country,
      brand,
      relationship_type: type,
      verification_status: rel.verification_status,
      is_current: rel.is_current !== false,
      rooms: Number.isFinite(roomCount) ? roomCount : null,
    };
    if (rel.is_current === false) historicalHotels.push(row);
    else currentHotels.push(row);
  }

  return {
    ok: true,
    version: ORGANIZATION_AGGREGATES_VERSION,
    entity_id: entityId,
    legal_name: entity.legal_name,
    display_name: entity.display_name,
    entity_type: entity.entity_type,
    known_current_hotels: currentHotels,
    known_historical_hotels: historicalHotels,
    current_hotel_count: currentHotels.length,
    historical_hotel_count: historicalHotels.length,
    countries: [...countries].sort(),
    brands: [...brands].sort(),
    operators: [...operators].sort(),
    known_rooms: rooms || null,
    relationship_type_mix: relMix,
    ownership_structure_mix: relMix,
  };
}
