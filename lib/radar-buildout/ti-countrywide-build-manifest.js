/**
 * Countrywide Travel Infrastructure build manifest (post-DA import).
 */
export const TI_COUNTRYWIDE_BUILDS = [
  { slug: "colombia", country: "Colombia", market: "Colombia Countrywide" },
  { slug: "belize", country: "Belize", market: "Belize Countrywide" },
  { slug: "guatemala", country: "Guatemala", market: "Guatemala Countrywide" },
  { slug: "honduras", country: "Honduras", market: "Honduras Countrywide" },
  { slug: "nicaragua", country: "Nicaragua", market: "Nicaragua Countrywide" },
  { slug: "el-salvador", country: "El Salvador", market: "El Salvador Countrywide" },
  { slug: "argentina", country: "Argentina", market: "Argentina Countrywide" },
  { slug: "ecuador", country: "Ecuador", market: "Ecuador Countrywide" },
  { slug: "uruguay", country: "Uruguay", market: "Uruguay Countrywide" },
];

export const PERU_TI_BUILD = {
  slug: "peru-lima-cusco",
  country: "Peru",
  market: "Lima / Cusco",
  fixture: "fixtures/travel-infrastructure-peru-lima-cusco-real.json",
};

/** Legacy fixtures that required verification envelope repair before import. */
export const TI_LEGACY_REPAIR_FIXTURES = [
  "fixtures/travel-infrastructure-puerto-rico-additional-real.json",
  "fixtures/travel-infrastructure-dominican-republic-second-pass-real.json",
];
