/**
 * Match GTM Companies to CALA hotel footprints via Properties (True Owner and related roles).
 */
import { normalizeOwnerKey } from "./normalize.js";
import { isPartialNormalizedMatch, isExactOnlyOwnerKey, normalizeForMatch } from "./name-match.js";
import { summarizePropertyFootprint } from "./cala-footprint.js";
import {
  buildContactCalaMatchContext,
  classifyContactCalaFootprint,
} from "./contact-cala-match.js";

/**
 * @param {string} companyName
 * @param {{ row: object }[]} propertyRecords
 */
function findPropertiesByOperatorRole(companyName, propertyRecords) {
  const norm = normalizeOwnerKey(companyName);
  const loose = normalizeForMatch(companyName);
  if (!norm && !loose) return [];

  /** @type {object[]} */
  const hits = [];
  const seen = new Set();

  for (const rec of propertyRecords || []) {
    const row = rec.row;
    const key = row.costarPropertyId || row.sourceRowKey || JSON.stringify(row);
    if (seen.has(key)) continue;

    for (const roleValue of [row.trueOwner, row.parentCompany, row.hotelOperator]) {
      const roleNorm = normalizeOwnerKey(roleValue);
      const roleLoose = normalizeForMatch(roleValue);
      if (!roleNorm && !roleLoose) continue;

      const exact = roleNorm === norm || (loose.length >= 4 && roleLoose === loose);
      const partial =
        !exact &&
        !isExactOnlyOwnerKey(roleNorm) &&
        (isPartialNormalizedMatch(norm, roleNorm) ||
          isPartialNormalizedMatch(loose, roleLoose));

      if (exact || partial) {
        seen.add(key);
        hits.push(row);
        break;
      }
    }
  }

  return hits;
}

/**
 * @param {object} options
 * @param {{ ownerName: string, properties: object[] }[]} options.ownerGroups
 * @param {string[]} [options.companyNames]
 * @param {object[]} [options.profileEnrichments]
 * @param {{ row: object }[]} [options.propertyRecords]
 */
export function buildCompanyCalaMatchContext({
  ownerGroups,
  companyNames = [],
  profileEnrichments = [],
  propertyRecords = [],
}) {
  return {
    ...buildContactCalaMatchContext({ ownerGroups, companyNames, profileEnrichments }),
    propertyRecords,
  };
}

/**
 * @param {string} companyName
 * @param {ReturnType<buildCompanyCalaMatchContext>} context
 */
export function classifyCompanyCalaFootprint(companyName, context) {
  const label = String(companyName || "").trim();
  const ownerMatch = classifyContactCalaFootprint({ company: label }, context);

  if (ownerMatch.calaHotelContact !== "unknown") {
    return {
      calaHotels: ownerMatch.calaHotelContact,
      matchType: ownerMatch.matchType,
      matchedOwnerName: ownerMatch.matchedOwnerName,
      calaPropertyCount: ownerMatch.calaPropertyCount,
      totalPropertyCount: ownerMatch.totalPropertyCount,
      calaCountriesSummary: ownerMatch.calaCountriesSummary,
      calaOnly: ownerMatch.calaOnly,
    };
  }

  const roleProperties = findPropertiesByOperatorRole(label, context.propertyRecords);
  if (!roleProperties.length) {
    return {
      calaHotels: "unknown",
      matchType: "none",
      matchedOwnerName: null,
      calaPropertyCount: null,
      totalPropertyCount: null,
      calaCountriesSummary: "",
      calaOnly: null,
    };
  }

  const footprint = summarizePropertyFootprint(roleProperties);
  return {
    calaHotels: footprint.hasCalaHotels ? "yes" : "no",
    matchType: "property_role",
    matchedOwnerName: label,
    calaPropertyCount: footprint.calaPropertyCount,
    totalPropertyCount: footprint.totalPropertyCount,
    calaCountriesSummary: footprint.calaCountriesSummary,
    calaOnly: footprint.calaOnly,
  };
}
