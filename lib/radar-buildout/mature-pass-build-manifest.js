/**
 * Mature / deepening pass build manifest — post-first-pass gap fills.
 */
export const MATURE_PASS_BUILDS = [
  {
    slug: "dominican-republic-secondary-coasts",
    country: "Dominican Republic",
    region: "Caribbean",
    market: "DR Secondary Coasts Mature Pass",
    kind: "da-ti",
    submarkets: ["Miches / Costa Esmeralda", "Barahona / Pedernales", "Jarabacoa / Constanza"],
  },
  {
    slug: "mexico-secondary-markets",
    country: "Mexico",
    region: "North America",
    market: "Mexico Secondary Markets",
    kind: "da-ti",
    submarkets: ["Oaxaca", "Querétaro", "Guanajuato", "Mazatlán"],
  },
  {
    slug: "peru-arequipa-paracas",
    country: "Peru",
    region: "South America",
    market: "Arequipa / Paracas",
    kind: "da-ti",
    submarkets: ["Arequipa", "Paracas"],
  },
  {
    slug: "chile-valparaiso-patagonia",
    country: "Chile",
    region: "South America",
    market: "Valparaíso / Patagonia",
    kind: "da-ti",
    submarkets: ["Valparaíso / Viña del Mar", "Patagonia Lakes", "Puerto Natales"],
  },
  {
    slug: "argentina-regional-depth",
    country: "Argentina",
    region: "South America",
    market: "Argentina Regional Depth",
    kind: "da-ti",
    submarkets: ["Mendoza", "Bariloche", "Puerto Iguazú"],
  },
];

/** TI-only deepening passes (no Google verify). */
export const MATURE_TI_ONLY_BUILDS = [
  {
    slug: "colombia-ti-mature",
    country: "Colombia",
    region: "South America",
    market: "Colombia TI Mature Pass",
    kind: "ti-only",
  },
];
