/**
 * South America countrywide DA builds (excluding Brazil).
 */
export const SOUTH_AMERICA_COUNTRY_BUILDS = [
  {
    slug: "argentina",
    country: "Argentina",
    market: "Argentina Countrywide",
    sequence: 36,
    submarkets: [
      "Buenos Aires",
      "Mendoza",
      "Bariloche",
      "Córdoba",
      "Puerto Iguazú",
      "Mar del Plata",
      "Ushuaia",
      "Salta",
      "Other",
    ],
  },
  {
    slug: "ecuador",
    country: "Ecuador",
    market: "Ecuador Countrywide",
    sequence: 37,
    submarkets: ["Quito", "Guayaquil", "Galápagos", "Cuenca", "Other"],
  },
  {
    slug: "uruguay",
    country: "Uruguay",
    market: "Uruguay Countrywide",
    sequence: 38,
    submarkets: ["Montevideo", "Punta del Este", "Colonia", "Other"],
  },
];

/** Peru Lima/Cusco uses legacy slug + fixture naming. */
export const PERU_LIMA_CUSCO_BUILD = {
  slug: "peru-lima-cusco",
  country: "Peru",
  market: "Lima / Cusco",
  candidatesFixture: "fixtures/demand-anchors-peru-lima-cusco-candidates.json",
  realFixture: "fixtures/demand-anchors-peru-lima-cusco-real.json",
  microFixture: "fixtures/demand-anchors-peru-lima-cusco-micro-pass.json",
};
