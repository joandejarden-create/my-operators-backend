/**
 * Dealality canonical CALA geography registry v1.
 *
 * Explicit Caribbean + Latin America hospitality-commercial universe.
 * NOT derived from HBX/Cvent inventory presence.
 *
 * Status / discovery audit: full-cala-geography-coverage-registry-audit-v1
 */
export const DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION =
  "dealality-cala-geography-registry-v1";

/**
 * @typedef {{
 *   geography_id: string,
 *   name: string,
 *   iso_code: string|null,
 *   region: 'Central America'|'South America'|'Caribbean'|'Scope Review',
 *   subregion?: string|null,
 *   aliases: string[],
 *   parent_country_encodings?: string[],
 *   tourism_priority: 'S'|'A'|'B'|'C',
 *   scope: 'in_scope'|'scope_review',
 *   notes?: string|null,
 * }} DealalityCalaGeography
 */

/** @type {DealalityCalaGeography[]} */
const REGISTRY = [
  // —— Central America ——
  {
    geography_id: "belize",
    name: "Belize",
    iso_code: "BZ",
    region: "Central America",
    aliases: ["Belize"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "costa_rica",
    name: "Costa Rica",
    iso_code: "CR",
    region: "Central America",
    aliases: ["Costa Rica"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "el_salvador",
    name: "El Salvador",
    iso_code: "SV",
    region: "Central America",
    aliases: ["El Salvador"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "guatemala",
    name: "Guatemala",
    iso_code: "GT",
    region: "Central America",
    aliases: ["Guatemala"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "honduras",
    name: "Honduras",
    iso_code: "HN",
    region: "Central America",
    aliases: ["Honduras"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "mexico",
    name: "Mexico",
    iso_code: "MX",
    region: "Central America",
    subregion: "North America / CALA ops",
    aliases: ["Mexico", "México", "México"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "nicaragua",
    name: "Nicaragua",
    iso_code: "NI",
    region: "Central America",
    aliases: ["Nicaragua"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "panama",
    name: "Panama",
    iso_code: "PA",
    region: "Central America",
    aliases: ["Panama", "Panamá"],
    tourism_priority: "S",
    scope: "in_scope",
  },

  // —— South America ——
  {
    geography_id: "argentina",
    name: "Argentina",
    iso_code: "AR",
    region: "South America",
    aliases: ["Argentina"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "bolivia",
    name: "Bolivia",
    iso_code: "BO",
    region: "South America",
    aliases: ["Bolivia"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "brazil",
    name: "Brazil",
    iso_code: "BR",
    region: "South America",
    aliases: ["Brazil", "Brasil"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "chile",
    name: "Chile",
    iso_code: "CL",
    region: "South America",
    aliases: ["Chile"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "colombia",
    name: "Colombia",
    iso_code: "CO",
    region: "South America",
    aliases: ["Colombia"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "ecuador",
    name: "Ecuador",
    iso_code: "EC",
    region: "South America",
    aliases: ["Ecuador"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "french_guiana",
    name: "French Guiana",
    iso_code: "GF",
    region: "South America",
    aliases: ["French Guiana", "Guyane", "Guyane française"],
    parent_country_encodings: ["France"],
    tourism_priority: "C",
    scope: "in_scope",
  },
  {
    geography_id: "guyana",
    name: "Guyana",
    iso_code: "GY",
    region: "South America",
    aliases: ["Guyana"],
    tourism_priority: "C",
    scope: "in_scope",
  },
  {
    geography_id: "paraguay",
    name: "Paraguay",
    iso_code: "PY",
    region: "South America",
    aliases: ["Paraguay"],
    tourism_priority: "C",
    scope: "in_scope",
  },
  {
    geography_id: "peru",
    name: "Peru",
    iso_code: "PE",
    region: "South America",
    aliases: ["Peru", "Perú"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "suriname",
    name: "Suriname",
    iso_code: "SR",
    region: "South America",
    aliases: ["Suriname"],
    tourism_priority: "C",
    scope: "in_scope",
  },
  {
    geography_id: "uruguay",
    name: "Uruguay",
    iso_code: "UY",
    region: "South America",
    aliases: ["Uruguay"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "venezuela",
    name: "Venezuela",
    iso_code: "VE",
    region: "South America",
    aliases: ["Venezuela"],
    tourism_priority: "B",
    scope: "in_scope",
  },

  // —— Caribbean ——
  {
    geography_id: "anguilla",
    name: "Anguilla",
    iso_code: "AI",
    region: "Caribbean",
    aliases: ["Anguilla"],
    parent_country_encodings: ["United Kingdom", "UK", "Great Britain"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "antigua_and_barbuda",
    name: "Antigua and Barbuda",
    iso_code: "AG",
    region: "Caribbean",
    aliases: ["Antigua and Barbuda", "Antigua"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "aruba",
    name: "Aruba",
    iso_code: "AW",
    region: "Caribbean",
    aliases: ["Aruba"],
    parent_country_encodings: ["Netherlands", "Kingdom of the Netherlands"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "bahamas",
    name: "Bahamas",
    iso_code: "BS",
    region: "Caribbean",
    aliases: ["Bahamas", "The Bahamas"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "barbados",
    name: "Barbados",
    iso_code: "BB",
    region: "Caribbean",
    aliases: ["Barbados"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "bonaire",
    name: "Bonaire",
    iso_code: "BQ",
    region: "Caribbean",
    aliases: ["Bonaire"],
    parent_country_encodings: [
      "Netherlands",
      "Caribbean Netherlands",
      "Bonaire, Sint Eustatius and Saba",
    ],
    tourism_priority: "A",
    scope: "in_scope",
    notes: "Preserve as distinct hotel market; do not collapse into BES aggregate only.",
  },
  {
    geography_id: "sint_eustatius",
    name: "Sint Eustatius",
    iso_code: "BQ",
    region: "Caribbean",
    aliases: ["Sint Eustatius", "Statia", "St. Eustatius", "St Eustatius"],
    parent_country_encodings: [
      "Netherlands",
      "Caribbean Netherlands",
      "Bonaire, Sint Eustatius and Saba",
    ],
    tourism_priority: "C",
    scope: "in_scope",
  },
  {
    geography_id: "saba",
    name: "Saba",
    iso_code: "BQ",
    region: "Caribbean",
    aliases: ["Saba"],
    parent_country_encodings: [
      "Netherlands",
      "Caribbean Netherlands",
      "Bonaire, Sint Eustatius and Saba",
    ],
    tourism_priority: "C",
    scope: "in_scope",
  },
  {
    geography_id: "british_virgin_islands",
    name: "British Virgin Islands",
    iso_code: "VG",
    region: "Caribbean",
    aliases: ["British Virgin Islands", "BVI", "Virgin Islands (British)"],
    parent_country_encodings: ["United Kingdom", "UK"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "cayman_islands",
    name: "Cayman Islands",
    iso_code: "KY",
    region: "Caribbean",
    aliases: ["Cayman Islands", "Cayman"],
    parent_country_encodings: ["United Kingdom", "UK"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "cuba",
    name: "Cuba",
    iso_code: "CU",
    region: "Caribbean",
    aliases: ["Cuba"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "curacao",
    name: "Curaçao",
    iso_code: "CW",
    region: "Caribbean",
    aliases: ["Curaçao", "Curacao", "Curaçao"],
    parent_country_encodings: ["Netherlands", "Kingdom of the Netherlands"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "dominica",
    name: "Dominica",
    iso_code: "DM",
    region: "Caribbean",
    aliases: ["Dominica"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "dominican_republic",
    name: "Dominican Republic",
    iso_code: "DO",
    region: "Caribbean",
    aliases: ["Dominican Republic", "República Dominicana", "DR"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "grenada",
    name: "Grenada",
    iso_code: "GD",
    region: "Caribbean",
    aliases: ["Grenada"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "guadeloupe",
    name: "Guadeloupe",
    iso_code: "GP",
    region: "Caribbean",
    aliases: ["Guadeloupe"],
    parent_country_encodings: ["France"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "haiti",
    name: "Haiti",
    iso_code: "HT",
    region: "Caribbean",
    aliases: ["Haiti", "Haïti"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "jamaica",
    name: "Jamaica",
    iso_code: "JM",
    region: "Caribbean",
    aliases: ["Jamaica"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "martinique",
    name: "Martinique",
    iso_code: "MQ",
    region: "Caribbean",
    aliases: ["Martinique"],
    parent_country_encodings: ["France"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "montserrat",
    name: "Montserrat",
    iso_code: "MS",
    region: "Caribbean",
    aliases: ["Montserrat"],
    parent_country_encodings: ["United Kingdom", "UK"],
    tourism_priority: "C",
    scope: "in_scope",
  },
  {
    geography_id: "puerto_rico",
    name: "Puerto Rico",
    iso_code: "PR",
    region: "Caribbean",
    aliases: ["Puerto Rico"],
    parent_country_encodings: ["United States", "USA", "US", "United States of America"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "saint_barthelemy",
    name: "Saint Barthélemy",
    iso_code: "BL",
    region: "Caribbean",
    aliases: [
      "Saint Barthélemy",
      "Saint Barthelemy",
      "St. Barthélemy",
      "St Barthelemy",
      "St. Barts",
      "St Barts",
      "St. Barth",
      "St Barth",
      "St. Bart's",
    ],
    parent_country_encodings: ["France"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "saint_kitts_and_nevis",
    name: "Saint Kitts and Nevis",
    iso_code: "KN",
    region: "Caribbean",
    aliases: ["Saint Kitts and Nevis", "St. Kitts and Nevis", "St Kitts and Nevis"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "saint_lucia",
    name: "Saint Lucia",
    iso_code: "LC",
    region: "Caribbean",
    aliases: ["Saint Lucia", "St. Lucia", "St Lucia"],
    tourism_priority: "A",
    scope: "in_scope",
  },
  {
    geography_id: "saint_martin_fr",
    name: "Saint Martin",
    iso_code: "MF",
    region: "Caribbean",
    aliases: ["Saint Martin", "St. Martin", "St Martin", "Saint-Martin"],
    parent_country_encodings: ["France"],
    tourism_priority: "A",
    scope: "in_scope",
    notes: "French side — keep distinct from Sint Maarten (Dutch).",
  },
  {
    geography_id: "saint_vincent_and_the_grenadines",
    name: "Saint Vincent and the Grenadines",
    iso_code: "VC",
    region: "Caribbean",
    aliases: [
      "Saint Vincent and the Grenadines",
      "St. Vincent and the Grenadines",
      "St Vincent and the Grenadines",
    ],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "sint_maarten",
    name: "Sint Maarten",
    iso_code: "SX",
    region: "Caribbean",
    aliases: ["Sint Maarten", "St. Maarten", "St Maarten", "Saint Maarten"],
    parent_country_encodings: ["Netherlands", "Kingdom of the Netherlands"],
    tourism_priority: "A",
    scope: "in_scope",
    notes: "Dutch side — keep distinct from Saint Martin (French).",
  },
  {
    geography_id: "trinidad_and_tobago",
    name: "Trinidad and Tobago",
    iso_code: "TT",
    region: "Caribbean",
    aliases: ["Trinidad and Tobago", "Trinidad & Tobago"],
    tourism_priority: "B",
    scope: "in_scope",
  },
  {
    geography_id: "turks_and_caicos",
    name: "Turks and Caicos Islands",
    iso_code: "TC",
    region: "Caribbean",
    aliases: [
      "Turks and Caicos Islands",
      "Turks and Caicos",
      "Turks & Caicos",
      "Turks & Caicos Islands",
    ],
    parent_country_encodings: ["United Kingdom", "UK"],
    tourism_priority: "S",
    scope: "in_scope",
  },
  {
    geography_id: "us_virgin_islands",
    name: "U.S. Virgin Islands",
    iso_code: "VI",
    region: "Caribbean",
    aliases: [
      "U.S. Virgin Islands",
      "US Virgin Islands",
      "United States Virgin Islands",
      "USVI",
      "Virgin Islands (U.S.)",
      "Virgin Islands, U.S.",
    ],
    parent_country_encodings: ["United States", "USA", "US", "United States of America"],
    tourism_priority: "S",
    scope: "in_scope",
  },

  {
    geography_id: "bermuda",
    name: "Bermuda",
    iso_code: "BM",
    region: "Caribbean",
    aliases: ["Bermuda"],
    parent_country_encodings: ["United Kingdom", "UK"],
    tourism_priority: "A",
    scope: "in_scope",
    notes:
      "Founder decision 2026-08-09: INCLUDE in Dealality commercial CALA hospitality universe.",
  },
];

export const DEALALITY_CALA_GEOGRAPHIES = Object.freeze(REGISTRY);

/** Documented HBX Content API Wave 1 country pulls (searched intentionally). */
export const HBX_WAVE1_SEARCHED_GEOGRAPHIES = Object.freeze([
  "Mexico",
  "Dominican Republic",
  "Colombia",
  "Costa Rica",
  "Panama",
]);

/**
 * Normalize a free-text country/territory label for matching.
 * @param {string} s
 */
export function normalizeGeographyLabel(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\bthe\b/g, " ")
    .replace(/\bst\b/g, "saint")
    .replace(/\bsaint\b/g, "saint")
    .replace(/\s+/g, " ")
    .trim();
}

function buildAliasIndex() {
  /** @type {Map<string, DealalityCalaGeography>} */
  const map = new Map();
  for (const g of DEALALITY_CALA_GEOGRAPHIES) {
    const keys = new Set([
      g.name,
      g.geography_id.replace(/_/g, " "),
      ...(g.aliases || []),
    ]);
    for (const k of keys) {
      const n = normalizeGeographyLabel(k);
      if (n) map.set(n, g);
    }
  }
  // Extra common normalizations
  map.set(normalizeGeographyLabel("Turks and Caicos"), map.get(normalizeGeographyLabel("Turks and Caicos Islands")));
  map.set(normalizeGeographyLabel("Curacao"), map.get(normalizeGeographyLabel("Curaçao")));
  return map;
}

const ALIAS_INDEX = buildAliasIndex();

/**
 * Resolve a source/Census country string to a canonical registry geography.
 * @param {string} raw
 * @returns {DealalityCalaGeography|null}
 */
export function resolveDealalityCalaGeography(raw) {
  const n = normalizeGeographyLabel(raw);
  if (!n) return null;
  if (ALIAS_INDEX.has(n)) return ALIAS_INDEX.get(n);

  // Soft contains for longer territory labels
  for (const g of DEALALITY_CALA_GEOGRAPHIES) {
    for (const a of [g.name, ...(g.aliases || [])]) {
      const an = normalizeGeographyLabel(a);
      if (an.length >= 5 && (n === an || n.includes(an) || an.includes(n))) {
        return g;
      }
    }
  }
  return null;
}

export function listDealalityCalaGeographies(opts = {}) {
  const includeScopeReview = opts.includeScopeReview !== false;
  return DEALALITY_CALA_GEOGRAPHIES.filter(
    (g) => includeScopeReview || g.scope === "in_scope"
  );
}

export function getParentEncodingLeakageHints() {
  /** @type {Record<string, string[]>} */
  const out = {};
  for (const g of DEALALITY_CALA_GEOGRAPHIES) {
    for (const p of g.parent_country_encodings || []) {
      if (!out[p]) out[p] = [];
      out[p].push(g.name);
    }
  }
  return out;
}
