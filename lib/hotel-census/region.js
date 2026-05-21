/**
 * Dealality UI regions from country (aligned with Radar / operators-by-brand-region).
 */

export const REGION_UI_TO_COUNTRIES = {
  "Caribbean & Latin America": [
    "Mexico", "Jamaica", "Dominican Republic", "Puerto Rico", "Cuba", "Bahamas", "Aruba",
    "Curaçao", "Cayman Islands", "Trinidad and Tobago", "Barbados", "Haiti",
    "Saint Lucia", "Antigua and Barbuda", "Grenada", "Saint Vincent and the Grenadines",
    "Dominica", "Saint Kitts and Nevis", "Turks and Caicos", "British Virgin Islands",
    "U.S. Virgin Islands", "Martinique", "Guadeloupe", "Bonaire",
    "Colombia", "Brazil", "Argentina", "Chile", "Peru", "Ecuador", "Costa Rica",
    "Panama", "Guatemala", "Honduras", "El Salvador", "Nicaragua", "Venezuela", "Uruguay", "Paraguay", "Bolivia",
  ],
  "North America": ["United States", "USA", "Canada", "United States of America"],
  "Europe": [
    "United Kingdom", "France", "Germany", "Spain", "Italy", "Portugal", "Netherlands",
    "Ireland", "Switzerland", "Austria", "Belgium", "Greece", "Poland", "Turkey",
    "Russia", "Czech Republic", "Hungary", "Romania", "Sweden", "Norway", "Denmark", "Finland",
    "Iceland", "Luxembourg", "Malta", "Cyprus", "Croatia", "Bulgaria", "Serbia", "Ukraine",
  ],
  "Middle East & Africa": [
    "United Arab Emirates", "Saudi Arabia", "Qatar", "Israel", "Egypt", "Jordan",
    "Lebanon", "Bahrain", "Kuwait", "Oman", "South Africa", "Morocco", "Kenya",
    "Nigeria", "Ethiopia", "Tanzania", "Ghana", "Tunisia", "Mauritius", "Rwanda",
  ],
  "Asia Pacific": [
    "China", "Japan", "India", "Singapore", "Thailand", "Indonesia", "Malaysia",
    "South Korea", "Vietnam", "Philippines", "Australia", "New Zealand", "Hong Kong",
    "Taiwan", "Sri Lanka", "Maldives", "Cambodia", "Myanmar", "Macau", "Pakistan", "Bangladesh",
  ],
};

function normalizeCountry(s) {
  if (s == null || typeof s !== "string") return "";
  return s.toLowerCase().trim();
}

export function countryToDealalityRegion(country) {
  if (!country || !normalizeCountry(country)) return "Other";
  const c = normalizeCountry(country);
  for (const [regionKey, list] of Object.entries(REGION_UI_TO_COUNTRIES)) {
    if (list.some((r) => normalizeCountry(r) === c)) return regionKey;
  }
  return "Other";
}
