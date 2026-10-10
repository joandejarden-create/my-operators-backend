/**
 * Canonical Dealality property display labels for selectors / headers.
 *
 * Format:
 *   Hotel Name — City, State        (when state/region present)
 *   Hotel Name — City, Country      (when state absent, country present)
 *   Hotel Name — City               (city only)
 *
 * Never emits dangling commas, "null"/"undefined", or duplicate country/state.
 */

const ISO2_COUNTRY_DISPLAY = Object.freeze({
  IT: "Italy",
  CH: "Switzerland",
  ES: "Spain",
  FR: "France",
  DE: "Germany",
  GB: "United Kingdom",
  UK: "United Kingdom",
  IE: "Ireland",
  PT: "Portugal",
  NL: "Netherlands",
  BE: "Belgium",
  AT: "Austria",
  US: "United States",
  CA: "Canada",
  MX: "Mexico",
  DO: "Dominican Republic",
  PR: "Puerto Rico",
  BM: "Bermuda",
  GD: "Grenada",
  BB: "Barbados",
  JM: "Jamaica",
  BR: "Brazil",
  AR: "Argentina",
  CL: "Chile",
  CO: "Colombia",
  PE: "Peru",
  PA: "Panama",
  CR: "Costa Rica",
  AE: "United Arab Emirates",
  SG: "Singapore",
  JP: "Japan",
  AU: "Australia",
  NZ: "New Zealand",
});

function cleanPart(value) {
  if (value == null) return "";
  const s = String(value).trim();
  if (!s || /^null$/i.test(s) || /^undefined$/i.test(s)) return "";
  return s.replace(/\s+/g, " ").replace(/,+\s*$/g, "").trim();
}

function normKey(value) {
  return cleanPart(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "");
}

/**
 * Human-readable country for display. ISO-2 codes expand when known.
 * Returns "" for empty / unusable values.
 */
export function formatCountryDisplay(country) {
  const raw = cleanPart(country);
  if (!raw) return "";
  if (/^[A-Za-z]{2}$/.test(raw)) {
    const mapped = ISO2_COUNTRY_DISPLAY[raw.toUpperCase()];
    return mapped || raw.toUpperCase();
  }
  return raw;
}

function isUsCountry(countryDisplay) {
  return /^(us|usa|u\.s\.a\.?|united states)$/i.test(cleanPart(countryDisplay));
}

/**
 * Location segment only (no hotel name).
 * @param {{ city?: string, state?: string, region?: string, country?: string }} parts
 */
export function formatPropertyLocationLine(parts = {}) {
  const city = cleanPart(parts.city);
  const state = cleanPart(parts.state) || cleanPart(parts.region);
  const country = formatCountryDisplay(parts.country);

  const out = [];
  if (city) out.push(city);

  if (state) {
    // Keep City, State even when equal (e.g. New York, New York).
    out.push(state);
    // When state/region is present, keep established City, State behavior.
    // Do not append country (avoids "Maryland, United States" / "Bermuda, Bermuda").
    return out.join(", ");
  }

  if (country) {
    // Avoid "Rome, Rome" if country somehow matches city; otherwise append.
    if (city && normKey(country) === normKey(city)) return city;
    out.push(isUsCountry(country) ? "United States" : country);
  }

  return out.join(", ");
}

/**
 * Full selector / header label: "Hotel Name — City, State|Country"
 * @param {{
 *   name?: string,
 *   hotelName?: string,
 *   displayName?: string,
 *   propertyId?: string,
 *   hotelId?: string,
 *   city?: string,
 *   state?: string,
 *   region?: string,
 *   country?: string,
 * }} property
 */
export function formatPropertySelectorLabel(property = {}) {
  const name =
    cleanPart(property.name) ||
    cleanPart(property.hotelName) ||
    cleanPart(property.displayName) ||
    cleanPart(property.propertyId) ||
    cleanPart(property.hotelId) ||
    "";
  const locationLine = formatPropertyLocationLine(property);
  if (!name) return locationLine;
  if (!locationLine) return name;
  return `${name} — ${locationLine}`;
}

/**
 * True when a label has dangling punctuation / empty second segment.
 */
export function hasDanglingPropertyLabelPunctuation(label) {
  const s = String(label || "");
  return /—\s*$/.test(s) || /,\s*$/.test(s) || /—\s*,/.test(s) || /,\s*—/.test(s);
}
