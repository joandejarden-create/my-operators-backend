/**
 * Shared CALA discovery priority countries (live-probed / Webhound-confirmed).
 */

export const CALA_DISCOVERY_PRIORITY_COUNTRIES = Object.freeze([
  "Mexico",
  "Dominican Republic",
  "Costa Rica",
  "Colombia",
  "Panama",
]);

/** ISO-ish country codes for directory filtering. */
export const CALA_DISCOVERY_COUNTRY_ISO = Object.freeze({
  Mexico: "MX",
  "Dominican Republic": "DO",
  "Costa Rica": "CR",
  Colombia: "CO",
  Panama: "PA",
  Jamaica: "JM",
  Peru: "PE",
  Brazil: "BR",
  Chile: "CL",
  Ecuador: "EC",
  Guatemala: "GT",
  Honduras: "HN",
  "El Salvador": "SV",
  Bahamas: "BS",
  Barbados: "BB",
  "Trinidad and Tobago": "TT",
  "Puerto Rico": "PR",
  Argentina: "AR",
  Aruba: "AW",
  Belize: "BZ",
  Bolivia: "BO",
  Uruguay: "UY",
  Paraguay: "PY",
  Haiti: "HT",
  Nicaragua: "NI",
  Venezuela: "VE",
});

/**
 * @param {string|null} countryFilter
 * @param {string[]} [defaultCountries]
 */
export function resolveDiscoveryCountries(countryFilter, defaultCountries = CALA_DISCOVERY_PRIORITY_COUNTRIES) {
  if (countryFilter) {
    const hit = defaultCountries.find(
      (c) => c.toLowerCase() === String(countryFilter).trim().toLowerCase()
    );
    return hit ? [hit] : [String(countryFilter).trim()].filter(Boolean);
  }
  return [...defaultCountries];
}
