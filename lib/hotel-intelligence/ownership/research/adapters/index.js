/**
 * Country ownership adapter dispatch.
 */

import { lookupMexicoOwnership } from "./mexico.js";
import { lookupBrazilCadasturOwnership } from "./brazil-cadastur.js";
import { lookupColombiaRntOwnership } from "./colombia-rnt.js";
import { lookupPeruMinceturOwnership } from "./peru-mincetur.js";
import { lookupGuatemalaInguatOwnership } from "./guatemala-inguat.js";
import { lookupEcuadorMinturOwnership } from "./ecuador-mintur.js";

export const COUNTRY_OWNERSHIP_ADAPTERS_VERSION =
  "ownership-country-adapters-v1";

/**
 * @param {string} country
 */
export function resolveOwnershipAdapterCountry(country) {
  const c = String(country || "").trim().toLowerCase();
  if (/^mexico$|^méxico$/.test(c)) return "Mexico";
  if (/^brazil$|^brasil$/.test(c)) return "Brazil";
  if (/^colombia$/.test(c)) return "Colombia";
  if (/^peru$|^perú$/.test(c)) return "Peru";
  if (/^guatemala$/.test(c)) return "Guatemala";
  if (/^ecuador$/.test(c)) return "Ecuador";
  return null;
}

/**
 * @param {object} hotel
 * @param {{ env?: object }} [ctx]
 */
export async function lookupCountryOwnershipEvidence(hotel, ctx = {}) {
  const key = resolveOwnershipAdapterCountry(hotel?.country);
  if (!key) {
    return {
      ok: true,
      skipped: true,
      country: hotel?.country || null,
      candidates: [],
      metrics: { provider: "none", skipped: true },
      notes: ["no_country_adapter"],
      failure_type: null,
    };
  }

  switch (key) {
    case "Mexico":
      return { country: key, ...(await lookupMexicoOwnership(hotel, ctx)) };
    case "Brazil":
      return {
        country: key,
        ...(await lookupBrazilCadasturOwnership(hotel, ctx)),
      };
    case "Colombia":
      return {
        country: key,
        ...(await lookupColombiaRntOwnership(hotel, ctx)),
      };
    case "Peru":
      return {
        country: key,
        ...(await lookupPeruMinceturOwnership(hotel, ctx)),
      };
    case "Guatemala":
      return {
        country: key,
        ...(await lookupGuatemalaInguatOwnership(hotel, ctx)),
      };
    case "Ecuador":
      return {
        country: key,
        ...(await lookupEcuadorMinturOwnership(hotel, ctx)),
      };
    default:
      return {
        ok: true,
        skipped: true,
        country: key,
        candidates: [],
        metrics: {},
        notes: ["adapter_not_implemented"],
      };
  }
}

export {
  lookupMexicoOwnership,
  lookupBrazilCadasturOwnership,
  lookupColombiaRntOwnership,
  lookupPeruMinceturOwnership,
  lookupGuatemalaInguatOwnership,
  lookupEcuadorMinturOwnership,
};
