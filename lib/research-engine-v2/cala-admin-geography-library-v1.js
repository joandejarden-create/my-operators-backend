/**
 * Durable CALA administrative geography library v1.
 * Provider-independent: State/Region applicability + subdivisions + city/locality maps.
 * Used by Core Geography Closeout (not tied to HBX / SerpAPI / DataForSEO).
 */
import {
  listDealalityCalaGeographies,
  normalizeGeographyLabel,
} from "./dealality-cala-geography-registry-v1.js";
import { STATE_REGION_NOT_APPLICABLE } from "./full-cala-core-identity-foundation-closure-v1.js";
import { normalizePlaceKey } from "./census-city-state-normalizer.js";

export const CALA_ADMIN_GEOGRAPHY_LIBRARY_VERSION =
  "cala-admin-geography-library-v1";

/** Canonical Cuban provinces (State / Region level for Dealality). */
export const CUBA_PROVINCES = Object.freeze([
  "Pinar del Río",
  "Artemisa",
  "La Habana",
  "Mayabeque",
  "Matanzas",
  "Cienfuegos",
  "Villa Clara",
  "Sancti Spíritus",
  "Ciego de Ávila",
  "Camagüey",
  "Las Tunas",
  "Holguín",
  "Granma",
  "Santiago de Cuba",
  "Guantánamo",
  "Isla de la Juventud",
]);

/** City / locality / destination → Cuban province. */
export const CUBA_CITY_TO_PROVINCE = Object.freeze({
  havana: "La Habana",
  habana: "La Habana",
  "la habana": "La Habana",
  "habana vieja": "La Habana",
  "old havana": "La Habana",
  vedado: "La Habana",
  miramar: "La Habana",
  "east havana": "La Habana",
  "habana del este": "La Habana",
  varadero: "Matanzas",
  matanzas: "Matanzas",
  "cayo coco": "Ciego de Ávila",
  "jardines del rey": "Ciego de Ávila",
  "cayo guillermo": "Ciego de Ávila",
  "ciego de avila": "Ciego de Ávila",
  "cayo santa maria": "Villa Clara",
  "cayo las brujas": "Villa Clara",
  "santa clara": "Villa Clara",
  remedios: "Villa Clara",
  holguin: "Holguín",
  guardalavaca: "Holguín",
  gibara: "Holguín",
  "santiago de cuba": "Santiago de Cuba",
  trinidad: "Sancti Spíritus",
  "sancti spiritus": "Sancti Spíritus",
  "topes de collantes": "Sancti Spíritus",
  cienfuegos: "Cienfuegos",
  vinales: "Pinar del Río",
  "pinar del rio": "Pinar del Río",
  "maria la gorda": "Pinar del Río",
  "cabo de san antonio": "Pinar del Río",
  camaguey: "Camagüey",
  "santa lucia": "Camagüey",
  "cayo largo": "Isla de la Juventud",
  "isla de la juventud": "Isla de la Juventud",
  baracoa: "Guantánamo",
  guantanamo: "Guantánamo",
  "peninsula de zapata": "Matanzas",
  "playa giron": "Matanzas",
  "playa larga": "Matanzas",
  bayamo: "Granma",
  granma: "Granma",
  "las tunas": "Las Tunas",
  artemisa: "Artemisa",
  mayabeque: "Mayabeque",
  "cayo saetia": "Holguín",
  "playa pesquero": "Holguín",
  "costa verde": "Holguín",
});

/**
 * First-order admin level name by Dealality geography.
 * @type {Record<string, { applicable: boolean, admin_level_name: string|null, subdivisions?: string[] }>}
 */
const ADMIN_BY_GEOGRAPHY = Object.freeze({
  Mexico: { applicable: true, admin_level_name: "State" },
  "Costa Rica": { applicable: true, admin_level_name: "Province" },
  Panama: { applicable: true, admin_level_name: "Province" },
  Guatemala: { applicable: true, admin_level_name: "Department" },
  Honduras: { applicable: true, admin_level_name: "Department" },
  Nicaragua: { applicable: true, admin_level_name: "Department" },
  "El Salvador": { applicable: true, admin_level_name: "Department" },
  Belize: { applicable: true, admin_level_name: "District" },
  Colombia: { applicable: true, admin_level_name: "Department" },
  Brazil: { applicable: true, admin_level_name: "State" },
  Argentina: { applicable: true, admin_level_name: "Province" },
  Chile: { applicable: true, admin_level_name: "Region" },
  Peru: { applicable: true, admin_level_name: "Department" },
  Ecuador: { applicable: true, admin_level_name: "Province" },
  Bolivia: { applicable: true, admin_level_name: "Department" },
  Paraguay: { applicable: true, admin_level_name: "Department" },
  Uruguay: { applicable: true, admin_level_name: "Department" },
  Venezuela: { applicable: true, admin_level_name: "State" },
  "Dominican Republic": { applicable: true, admin_level_name: "Province" },
  Cuba: {
    applicable: true,
    admin_level_name: "Province",
    subdivisions: [...CUBA_PROVINCES],
  },
  Jamaica: { applicable: true, admin_level_name: "Parish" },
  Barbados: { applicable: true, admin_level_name: "Parish" },
  "Puerto Rico": { applicable: true, admin_level_name: "Municipality" },
  Dominica: { applicable: true, admin_level_name: "Parish" },
  "Trinidad and Tobago": { applicable: true, admin_level_name: "Region / Municipality" },
  "Antigua and Barbuda": { applicable: true, admin_level_name: "Parish" },
  "Saint Lucia": { applicable: true, admin_level_name: "District" },
  "Saint Vincent and the Grenadines": {
    applicable: true,
    admin_level_name: "Parish",
  },
  Grenada: { applicable: true, admin_level_name: "Parish" },
  "Saint Kitts and Nevis": { applicable: true, admin_level_name: "Parish" },
  Bahamas: { applicable: true, admin_level_name: "Island / District" },
  Haiti: { applicable: true, admin_level_name: "Department" },
  Suriname: { applicable: true, admin_level_name: "District" },
  Guyana: { applicable: true, admin_level_name: "Region" },
});

/**
 * @param {string} country
 */
export function isStateRegionApplicable(country) {
  const raw = String(country || "").trim();
  if (!raw) return false;
  if (STATE_REGION_NOT_APPLICABLE.has(raw)) return false;
  const resolved =
    listDealalityCalaGeographies({ includeScopeReview: true }).find(
      (g) =>
        g.name === raw ||
        normalizeGeographyLabel(g.name) === normalizeGeographyLabel(raw)
    )?.name || raw;
  if (STATE_REGION_NOT_APPLICABLE.has(resolved)) return false;
  const meta = ADMIN_BY_GEOGRAPHY[resolved];
  if (meta) return meta.applicable !== false;
  // Default for other in-scope CALA geos not in small-territory set: applicable
  return true;
}

/**
 * @param {string} country
 */
export function getAdminGeographyMeta(country) {
  const raw = String(country || "").trim();
  const resolved =
    listDealalityCalaGeographies({ includeScopeReview: true }).find(
      (g) =>
        g.name === raw ||
        normalizeGeographyLabel(g.name) === normalizeGeographyLabel(raw)
    )?.name || raw;
  if (STATE_REGION_NOT_APPLICABLE.has(resolved) || STATE_REGION_NOT_APPLICABLE.has(raw)) {
    return {
      geography: resolved || raw,
      STATE_REGION_APPLICABLE: false,
      ADMIN_LEVEL_NAME: null,
      subdivisions: [],
      reason: "small_territory_or_unitary_dealality_level",
    };
  }
  const meta = ADMIN_BY_GEOGRAPHY[resolved] || {
    applicable: true,
    admin_level_name: "First-order administrative division",
  };
  return {
    geography: resolved || raw,
    STATE_REGION_APPLICABLE: meta.applicable !== false,
    ADMIN_LEVEL_NAME: meta.admin_level_name || null,
    subdivisions: meta.subdivisions || [],
  };
}

/**
 * Build registry snapshot for all 52 Dealality geographies.
 */
export function buildCalaAdminGeographyLibrarySnapshot() {
  const geos = listDealalityCalaGeographies({ includeScopeReview: true });
  return {
    version: CALA_ADMIN_GEOGRAPHY_LIBRARY_VERSION,
    geography_count: geos.length,
    geographies: geos.map((g) => ({
      geography_id: g.geography_id,
      name: g.name,
      ...getAdminGeographyMeta(g.name),
    })),
  };
}

/**
 * Resolve Cuba province from city/locality (deterministic).
 * @param {string} city
 */
export function resolveCubaProvinceFromCity(city) {
  const key = normalizePlaceKey(city);
  if (!key) return null;
  if (CUBA_CITY_TO_PROVINCE[key]) return CUBA_CITY_TO_PROVINCE[key];
  for (const [k, v] of Object.entries(CUBA_CITY_TO_PROVINCE)) {
    if (key.includes(k) || k.includes(key)) return v;
  }
  return null;
}
