/**
 * Cvent LATAM / Caribbean country slug registry for Supplier Network results pages.
 * Scope: every Latin America + Caribbean country Cvent commonly lists (founder 1B).
 * Pattern: https://www.cvent.com/venues/results/{slug}
 */

export const CVENT_LATAM_COUNTRY_REGISTRY_VERSION =
  "census-cvent-latam-country-registry-v1";

/**
 * @typedef {{
 *   country: string,
 *   slug: string,
 *   region: 'Caribbean'|'Central America'|'South America'|'North America',
 *   aliases?: string[],
 * }} CventLatamCountry
 */

/** Seed registry — probed at harvest time; empty totalCount / 404 dropped. */
export const CVENT_LATAM_CARIBBEAN_COUNTRIES = Object.freeze(
  /** @type {CventLatamCountry[]} */ ([
    // North America (LATAM ops)
    { country: "Mexico", slug: "Mexico", region: "North America", aliases: ["México"] },
    // Central America
    { country: "Guatemala", slug: "Guatemala", region: "Central America" },
    { country: "Belize", slug: "Belize", region: "Central America" },
    { country: "El Salvador", slug: "El-Salvador", region: "Central America" },
    { country: "Honduras", slug: "Honduras", region: "Central America" },
    { country: "Nicaragua", slug: "Nicaragua", region: "Central America" },
    { country: "Costa Rica", slug: "Costa-Rica", region: "Central America" },
    { country: "Panama", slug: "Panama", region: "Central America", aliases: ["Panamá"] },
    // Caribbean
    { country: "Puerto Rico", slug: "Puerto-Rico", region: "Caribbean" },
    {
      country: "Dominican Republic",
      slug: "Dominican-Republic",
      region: "Caribbean",
      aliases: ["República Dominicana"],
    },
    { country: "Cuba", slug: "Cuba", region: "Caribbean" },
    { country: "Haiti", slug: "Haiti", region: "Caribbean", aliases: ["Haïti"] },
    { country: "Jamaica", slug: "Jamaica", region: "Caribbean" },
    { country: "Bahamas", slug: "Bahamas", region: "Caribbean", aliases: ["The Bahamas"] },
    { country: "Trinidad and Tobago", slug: "Trinidad-and-Tobago", region: "Caribbean" },
    { country: "Barbados", slug: "Barbados", region: "Caribbean" },
    { country: "Aruba", slug: "Aruba", region: "Caribbean" },
    { country: "Curaçao", slug: "Curacao", region: "Caribbean", aliases: ["Curacao"] },
    { country: "Bonaire", slug: "Bonaire", region: "Caribbean" },
    { country: "Cayman Islands", slug: "Cayman-Islands", region: "Caribbean" },
    { country: "Turks and Caicos", slug: "Turks-and-Caicos", region: "Caribbean", aliases: ["Turks & Caicos"] },
    { country: "Saint Lucia", slug: "Saint-Lucia", region: "Caribbean", aliases: ["St. Lucia", "St Lucia"] },
    {
      country: "Antigua and Barbuda",
      slug: "Antigua-and-Barbuda",
      region: "Caribbean",
      aliases: ["Antigua"],
    },
    { country: "Grenada", slug: "Grenada", region: "Caribbean" },
    {
      country: "Saint Vincent and the Grenadines",
      slug: "Saint-Vincent-and-the-Grenadines",
      region: "Caribbean",
    },
    { country: "Dominica", slug: "Dominica", region: "Caribbean" },
    {
      country: "Saint Kitts and Nevis",
      slug: "Saint-Kitts-and-Nevis",
      region: "Caribbean",
      aliases: ["St. Kitts and Nevis"],
    },
    {
      country: "British Virgin Islands",
      slug: "British-Virgin-Islands",
      region: "Caribbean",
    },
    {
      country: "U.S. Virgin Islands",
      slug: "US-Virgin-Islands",
      region: "Caribbean",
      aliases: ["United States Virgin Islands", "US Virgin Islands"],
    },
    { country: "Martinique", slug: "Martinique", region: "Caribbean" },
    { country: "Guadeloupe", slug: "Guadeloupe", region: "Caribbean" },
    { country: "Sint Maarten", slug: "Sint-Maarten", region: "Caribbean", aliases: ["St. Maarten"] },
    { country: "Saint Martin", slug: "Saint-Martin", region: "Caribbean" },
    { country: "Anguilla", slug: "Anguilla", region: "Caribbean" },
    { country: "Montserrat", slug: "Montserrat", region: "Caribbean" },
    { country: "Saint Barthélemy", slug: "Saint-Barthelemy", region: "Caribbean", aliases: ["St. Barts"] },
    // South America
    { country: "Colombia", slug: "Colombia", region: "South America" },
    { country: "Venezuela", slug: "Venezuela", region: "South America" },
    { country: "Guyana", slug: "Guyana", region: "South America" },
    { country: "Suriname", slug: "Suriname", region: "South America" },
    { country: "French Guiana", slug: "French-Guiana", region: "South America" },
    { country: "Ecuador", slug: "Ecuador", region: "South America" },
    { country: "Peru", slug: "Peru", region: "South America", aliases: ["Perú"] },
    { country: "Bolivia", slug: "Bolivia", region: "South America" },
    { country: "Brazil", slug: "Brazil", region: "South America", aliases: ["Brasil"] },
    { country: "Paraguay", slug: "Paraguay", region: "South America" },
    { country: "Chile", slug: "Chile", region: "South America" },
    { country: "Argentina", slug: "Argentina", region: "South America" },
    { country: "Uruguay", slug: "Uruguay", region: "South America" },
  ])
);

/**
 * Build Cvent results page URL for a country slug (page 1 by default).
 * @param {string} slug
 * @param {{ page?: number, term?: string }} [opts]
 */
export function buildCventCountryResultsUrl(slug, opts = {}) {
  const s = String(slug || "").trim();
  if (!s) return null;
  const page = Number(opts.page || 1);
  const term = opts.term || s.replace(/-/g, " ");
  const base = `https://www.cvent.com/venues/results/${encodeURIComponent(s)}`;
  const params = new URLSearchParams();
  params.set("term", term);
  if (page > 1) params.set("p", String(page));
  return `${base}?${params.toString()}`;
}

/**
 * @param {string} countryOrSlug
 * @returns {CventLatamCountry|null}
 */
export function findCventLatamCountry(countryOrSlug) {
  const q = String(countryOrSlug || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
  if (!q) return null;
  for (const c of CVENT_LATAM_CARIBBEAN_COUNTRIES) {
    const names = [c.country, c.slug, ...(c.aliases || [])].map((n) =>
      String(n)
        .toLowerCase()
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/&/g, "and")
    );
    if (names.some((n) => n === q || n.replace(/\s+/g, "-") === q.replace(/\s+/g, "-"))) {
      return c;
    }
  }
  return null;
}

/**
 * Filter registry by optional country name/slug list.
 * @param {string[]|null} filter
 */
export function resolveCventLatamCountries(filter = null) {
  if (!filter || !filter.length) return [...CVENT_LATAM_CARIBBEAN_COUNTRIES];
  const out = [];
  const seen = new Set();
  for (const f of filter) {
    const hit = findCventLatamCountry(f);
    if (hit && !seen.has(hit.slug)) {
      seen.add(hit.slug);
      out.push(hit);
    }
  }
  return out;
}
