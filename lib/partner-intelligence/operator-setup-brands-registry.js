/**
 * Canonical Operator Setup brand relationships.
 *
 * Updates:
 * - Profile `Brand Families Operated` (multi-select — includes Independent)
 * - Profile `brands` (linked Brand Setup - Brand Basics names)
 * - Profile `numberOfBrands` (derived from resolved links)
 *
 * Allowed Brand Families Operated options (schema):
 * Marriott | Hilton | Hyatt | IHG | Choice | Wyndham | Accor | Sonesta |
 * Radisson / Choice | Independent | Soft brands / collections | Other
 *
 * `brandsExpandParents` expands to all Brand Basics rows whose Parent Company
 * matches (case-insensitive substring). Use for brand-managed Explorer lenses.
 *
 * Do not invent brand families outside the allowed select list.
 */

export const OPERATOR_SETUP_BRAND_FAMILIES_ALLOWED = Object.freeze([
  "Marriott",
  "Hilton",
  "Hyatt",
  "IHG",
  "Choice",
  "Wyndham",
  "Accor",
  "Sonesta",
  "Radisson / Choice",
  "Independent",
  "Soft brands / collections",
  "Other",
]);

/**
 * Placeholder Brand Basics row for proprietary / unflagged assets.
 * Created Draft + Internal Only so it does not enter Brand Explorer public universe.
 */
export const OPERATOR_SETUP_INDEPENDENT_BRAND_NAME = "Independent";

/**
 * @typedef {{
 *   brandFamiliesOperated: string[],
 *   brands: string[],
 *   brandsExpandParents?: string[],
 *   sourceNote: string
 * }} OperatorBrandsSpec
 */

/** @param {string[]} families @param {string[]} brands @param {string} sourceNote @param {string[]} [expandParents] */
function spec(families, brands, sourceNote, expandParents) {
  const brandFamiliesOperated = [...new Set(families)].filter((f) =>
    OPERATOR_SETUP_BRAND_FAMILIES_ALLOWED.includes(f)
  );
  return Object.freeze({
    brandFamiliesOperated,
    brands: [...new Set(brands.filter(Boolean))],
    brandsExpandParents: expandParents ? [...new Set(expandParents)] : [],
    sourceNote,
  });
}

/** @type {Record<string, OperatorBrandsSpec>} keyed by factory/baseline slug */
export const OPERATOR_SETUP_BRANDS_BY_SLUG = Object.freeze({
  "arbor-lodging-cala": spec(
    ["Marriott", "Hilton", "Hyatt", "IHG", "Independent", "Soft brands / collections"],
    [
      "Marriott Hotels",
      "AC Hotels by Marriott",
      "Aloft Hotels",
      "Courtyard by Marriott",
      "Fairfield by Marriott",
      "Residence Inn by Marriott",
      "SpringHill Suites by Marriott",
      "TownePlace Suites by Marriott",
      "Curio Collection by Hilton",
      "Hampton by Hilton",
      "Hilton Garden Inn",
      "Homewood Suites by Hilton",
      "Home2 Suites by Hilton",
      "Tru by Hilton",
      "Hyatt Place",
      "Candlewood Suites",
      "Holiday Inn",
      "Holiday Inn Express",
      "Hotel Indigo",
      "Staybridge Suites",
      "Independent",
    ],
    "arborlodging.com/portfolio — Brands We Operate (Marriott/Hilton/Hyatt/IHG) + independent/lifestyle assets cited on site"
  ),

  "hotel-equities-cala": spec(
    [
      "Marriott",
      "Hilton",
      "Hyatt",
      "IHG",
      "Choice",
      "Wyndham",
      "Independent",
      "Soft brands / collections",
      "Other",
    ],
    [
      "AC Hotels by Marriott",
      "Autograph Collection",
      "Courtyard by Marriott",
      "Element by Westin",
      "Fairfield by Marriott",
      "Luxury Collection",
      "Marriott Hotels",
      "Residence Inn by Marriott",
      "Curio Collection by Hilton",
      "DoubleTree by Hilton",
      "Hampton by Hilton",
      "Hilton Garden Inn",
      "Hilton Hotels & Resorts",
      "Home2 Suites by Hilton",
      "Homewood Suites by Hilton",
      "LXR Hotels & Resorts",
      "Tapestry Collection by Hilton",
      "Hyatt Place",
      "Holiday Inn Express",
      "Kimpton Hotels",
      "Best Western Premier",
      "Small Luxury Hotels of the World",
      "Vignette Collection",
      "Independent",
    ],
    "hotelequities.com FAQ/portfolio — preferred partner Marriott/Hilton/Hyatt/IHG/Choice/Wyndham/Best Western + independent/lifestyle (Other=BWH)"
  ),

  "ghl-hoteles": spec(
    [
      "Marriott",
      "Hyatt",
      "IHG",
      "Sonesta",
      "Radisson / Choice",
      "Independent",
      "Soft brands / collections",
    ],
    [
      "Sheraton",
      "Four Points by Sheraton",
      "AC Hotels by Marriott",
      "Hyatt Place",
      "Hyatt Centric",
      "Holiday Inn",
      "Sonesta Hotels & Resorts",
      "Radisson",
      "Radisson by Choice",
      "Independent",
    ],
    "ghlhoteles.com / ghloperador.com portfolio + Sonesta MFA through 2034; proprietary GHL/Style/Relax/Collection → Independent"
  ),

  "aimbridge-latam": spec(
    [
      "Marriott",
      "Hilton",
      "IHG",
      "Wyndham",
      "Accor",
      "Hyatt",
      "Independent",
      "Soft brands / collections",
    ],
    [
      "AC Hotels by Marriott",
      "Aloft Hotels",
      "Autograph Collection",
      "Courtyard by Marriott",
      "JW Marriott",
      "Marriott Hotels",
      "Sheraton",
      "Westin",
      "Hampton by Hilton",
      "Hilton Garden Inn",
      "Tapestry Collection by Hilton",
      "Crowne Plaza",
      "Holiday Inn",
      "Holiday Inn Express",
      "Staybridge Suites",
      "Voco Hotels",
      "ibis",
      "Wyndham",
      "Wyndham Garden",
      "Microtel by Wyndham",
      "Hyatt Place",
      "Independent",
    ],
    "aimbridgelatam.com About + hotel directory / 2026 portfolio PDF — Marriott, Hilton, IHG, Wyndham, Accor, Hyatt + HNF/independent assets"
  ),

  "tafer-hotels-resorts": spec(
    ["Independent", "Soft brands / collections"],
    ["Independent"],
    "TAFER / Villa Group proprietary leisure brands (Mousai, Garza Blanca, Villa del Palmar, Sierra Lago) — not major franchise families"
  ),

  "grupo-presidente": spec(
    ["Marriott", "Hyatt", "IHG", "Independent", "Soft brands / collections"],
    [
      "InterContinental",
      "Holiday Inn",
      "Holiday Inn Express",
      "Staybridge Suites",
      "Candlewood Suites",
      "Kimpton Hotels",
      "Hyatt House",
      "Hyatt Place",
      "Aloft Hotels",
      "Courtyard by Marriott",
      "Independent",
    ],
    "grupopresidente.com.mx — represents Marriott, Hyatt, IHG in Mexico; Casa Pepe boutique → Independent; Kimpton → Soft brands"
  ),

  highgate: spec(
    ["Marriott", "Hilton", "Hyatt", "IHG", "Independent", "Soft brands / collections"],
    [
      "Luxury Collection",
      "AC Hotels by Marriott",
      "Aloft Hotels",
      "Autograph Collection",
      "Westin",
      "Marriott Hotels",
      "Embassy Suites by Hilton",
      "Tapestry Collection by Hilton",
      "Hilton Hotels & Resorts",
      "Crowne Plaza",
      "Hyatt Regency",
      "Independent",
    ],
    "Highgate ESG / CALA Peru press (Luxury Collection, Aloft, AC, Westin, Autograph, Tapestry, Embassy Suites) + independent hotels cited"
  ),

  "grupo-hotelero-santa-fe": spec(
    ["Hyatt", "Hilton", "Accor", "Independent", "Soft brands / collections"],
    [
      "Hyatt Regency",
      "Hyatt Place",
      "Hyatt Centric",
      "Hilton Garden Inn",
      "Hampton by Hilton",
      "ibis",
      "Independent",
    ],
    "GSF corporate presentation / AR — Krystal proprietary + Hyatt / Hilton / Accor (Ibis); Secrets/Inclusive → Soft brands (Hyatt system)"
  ),

  "arriva-hospitality-group": spec(
    ["Marriott", "Accor", "Independent", "Soft brands / collections"],
    ["Westin", "ibis", "Independent"],
    "Arriva About timeline — proprietary Crown Paradise / Vista / Sensira + selective Westin / Ibis flags"
  ),

  "brittain-resorts-hotels": spec(
    ["Marriott", "Independent", "Soft brands / collections"],
    ["Courtyard by Marriott", "SpringHill Suites by Marriott", "Independent"],
    "brittainresorts.com portfolio — primarily proprietary Myrtle Beach resorts + Courtyard / SpringHill Suites by Marriott (Hilton cited as pipeline, not confirmed operating)"
  ),

  "atlantica-hotels-international": spec(
    [
      "Choice",
      "Hilton",
      "Wyndham",
      "Radisson / Choice",
      "Independent",
      "Soft brands / collections",
    ],
    [
      "Quality Inn",
      "Comfort Inn & Suites",
      "Clarion",
      "Sleep Inn",
      "Ascend Hotel Collection",
      "Radisson by Choice",
      "Radisson Blu by Choice",
      "Radisson Collection by Choice",
      "Radisson RED by Choice",
      "Park Inn by Choice",
      "Park Plaza by Choice",
      "Country Inn & Suites by Choice",
      "Hilton Hotels & Resorts",
      "Hilton Garden Inn",
      "Hampton by Hilton",
      "Wyndham",
      "Wyndham Garden",
      "Independent",
    ],
    "ahi.com.br Our brands — exclusive Choice (incl. Radisson-by-Choice) + Hilton + Wyndham partnerships; proprietary Atlantica brands → Independent"
  ),

  "marriott-international-managed": spec(
    ["Marriott", "Soft brands / collections"],
    [],
    "Brand-managed Explorer lens — Marriott International brand system (Autograph/Luxury Collection etc. via parent expand)",
    ["Marriott International"]
  ),

  "ihg-managed": spec(
    ["IHG", "Soft brands / collections"],
    [],
    "Brand-managed Explorer lens — IHG One Rewards brand system",
    ["InterContinental Hotels Group"]
  ),

  "hilton-managed": spec(
    ["Hilton", "Soft brands / collections"],
    [],
    "Brand-managed Explorer lens — Hilton Honors brand system",
    ["Hilton Worldwide"]
  ),

  "accor-managed": spec(
    ["Accor", "Soft brands / collections"],
    [],
    "Brand-managed Explorer lens — Accor brand system (45+ brands)",
    ["Accor"]
  ),

  "minor-hotels-managed": spec(
    ["Soft brands / collections", "Other"],
    ["Anantara", "NH Hotels", "Oaks", "Tivoli Hotels & Resorts"],
    "Brand-managed Explorer lens — Minor Hotels portfolio (Anantara/NH/Oaks/Tivoli in Brand Basics; Other=Minor family — no dedicated select option)"
  ),

  "playa-hotels-resorts": spec(
    [
      "Hyatt",
      "Hilton",
      "Wyndham",
      "IHG",
      "Marriott",
      "Independent",
      "Soft brands / collections",
    ],
    [
      "Hyatt Ziva",
      "Hyatt Zilara",
      "Tapestry Collection by Hilton",
      "Wyndham",
      "Kimpton Hotels",
      "Luxury Collection",
      "Independent",
    ],
    "Pre-Hyatt-close Playa portfolio cites Hyatt Ziva/Zilara, Hilton AI/Tapestry, Wyndham Alltra, Seadust, Kimpton, Jewel, Luxury Collection — Independent for proprietary/Seadust/Jewel not in Brand Basics"
  ),

  "royalton-hotels-resorts": spec(
    ["Marriott", "Independent", "Soft brands / collections"],
    ["Independent"],
    "royaltonresorts.com — proprietary Royalton family brands; select resorts participate in Marriott Bonvoy (guest FAQ)"
  ),

  "driftwood-hospitality-management": spec(
    [
      "Marriott",
      "Hilton",
      "IHG",
      "Hyatt",
      "Wyndham",
      "Choice",
      "Independent",
      "Soft brands / collections",
      "Other",
    ],
    [
      "Marriott Hotels",
      "Courtyard by Marriott",
      "Residence Inn by Marriott",
      "Hilton Garden Inn",
      "Hampton by Hilton",
      "Homewood Suites by Hilton",
      "DoubleTree by Hilton",
      "Holiday Inn",
      "Holiday Inn Express",
      "Crowne Plaza",
      "Hyatt Place",
      "Hyatt House",
      "Wyndham",
      "Wyndham Garden",
      "Comfort Inn & Suites",
      "Quality Inn",
      "Best Western",
      "Best Western Plus",
      "Independent",
    ],
    "driftwoodhospitality.com — ~24 brands incl. Hilton, Marriott, Hyatt, IHG, Wyndham, Choice, Margaritaville, Best Western + independent (Other=BWH)"
  ),

  "remington-hospitality": spec(
    ["Marriott", "Hilton", "IHG", "Hyatt", "Independent", "Soft brands / collections"],
    [
      "Marriott Hotels",
      "Autograph Collection",
      "Courtyard by Marriott",
      "Hilton Garden Inn",
      "Hampton by Hilton",
      "Kimpton Hotels",
      "Holiday Inn",
      "Hyatt Place",
      "LXR Hotels & Resorts",
      "Independent",
    ],
    "remingtonhospitality.com — 26 brands + independent/boutique; CALA cites Kimpton, Hilton Garden Inn, Autograph/LXR"
  ),

  // Staging / In Review — keep conservative from existing Profile signals + Independent when proprietary-leaning
  "cordillera-one-gestion": spec(
    ["Hilton", "IHG", "Independent"],
    ["Hilton Garden Inn", "Holiday Inn", "Independent"],
    "Existing Profile brand links (Hilton Garden Inn, Holiday Inn) + Independent for non-flagged assets"
  ),
  "cenote-azul-operadores": spec(
    ["Wyndham", "Independent"],
    ["Wyndham", "Independent"],
    "Existing Profile Brand Families (Independent, Wyndham)"
  ),
  "antillano-norte-hospitality-group": spec(
    ["Marriott", "Wyndham", "Independent"],
    ["Marriott Hotels", "Wyndham", "Independent"],
    "Existing Profile Brand Families (Marriott, Wyndham) + Independent"
  ),
  "viento-sur-gestion-hotelera": spec(
    ["Marriott", "Hilton", "Hyatt", "Independent"],
    ["Hilton Garden Inn", "Hyatt Place", "Independent"],
    "Existing Profile brand links + Independent"
  ),
  "mangle-azul-hospitalidad": spec(
    ["IHG", "Independent", "Soft brands / collections"],
    ["Holiday Inn Express", "Independent"],
    "Existing Profile (Independent, Soft brands) + Holiday Inn Express link"
  ),
  "panamerican-lodging-partners": spec(
    ["Marriott", "Hilton", "Accor", "Hyatt", "Independent"],
    ["Hyatt", "Independent"],
    "Existing Profile families (Marriott, Hilton, Accor) + Hyatt link + Independent"
  ),
  "rio-plata-hotel-partners": spec(
    ["Marriott", "Independent"],
    ["Sheraton", "Independent"],
    "Existing Profile Independent + Sheraton link → Marriott family"
  ),
  "barrio-hotelero-cdmx": spec(
    ["Accor", "Independent"],
    ["ibis", "Independent"],
    "Existing Profile Independent + ibis link → Accor"
  ),
  "metro-lodging-sao-paulo": spec(
    ["Choice", "IHG", "Accor", "Independent"],
    ["Novotel", "Mercure", "Independent"],
    "Existing Profile (Choice, IHG) + Novotel/Mercure links → Accor; Independent for non-flagged"
  ),
  "oro-verde-lodge-hotel-operators": spec(
    ["Marriott", "Wyndham", "Independent", "Soft brands / collections"],
    ["Autograph Collection", "Independent"],
    "Existing Profile (Independent, Wyndham) + Autograph link → Marriott soft brand"
  ),
});

export const OPERATOR_SETUP_BRANDS_BY_COMPANY_NAME = Object.freeze({
  "Arbor Lodging (CALA)": OPERATOR_SETUP_BRANDS_BY_SLUG["arbor-lodging-cala"],
  "Hotel Equities (CALA)": OPERATOR_SETUP_BRANDS_BY_SLUG["hotel-equities-cala"],
  "GHL Hoteles (GHL Holding)": OPERATOR_SETUP_BRANDS_BY_SLUG["ghl-hoteles"],
  "Aimbridge Hospitality (LATAM)": OPERATOR_SETUP_BRANDS_BY_SLUG["aimbridge-latam"],
  "Tafer Hotels & Resorts": OPERATOR_SETUP_BRANDS_BY_SLUG["tafer-hotels-resorts"],
  "Grupo Presidente": OPERATOR_SETUP_BRANDS_BY_SLUG["grupo-presidente"],
  Highgate: OPERATOR_SETUP_BRANDS_BY_SLUG.highgate,
  "Grupo Hotelero Santa Fe": OPERATOR_SETUP_BRANDS_BY_SLUG["grupo-hotelero-santa-fe"],
  "Arriva Hospitality Group (AHG)": OPERATOR_SETUP_BRANDS_BY_SLUG["arriva-hospitality-group"],
  "Brittain Resorts & Hotels (BRH)": OPERATOR_SETUP_BRANDS_BY_SLUG["brittain-resorts-hotels"],
  "Atlantica Hotels International (AHI)":
    OPERATOR_SETUP_BRANDS_BY_SLUG["atlantica-hotels-international"],
  "Marriott International (Managed)":
    OPERATOR_SETUP_BRANDS_BY_SLUG["marriott-international-managed"],
  "IHG Hotels & Resorts (Managed)": OPERATOR_SETUP_BRANDS_BY_SLUG["ihg-managed"],
  "Hilton (Managed)": OPERATOR_SETUP_BRANDS_BY_SLUG["hilton-managed"],
  "Accor (Managed)": OPERATOR_SETUP_BRANDS_BY_SLUG["accor-managed"],
  "Minor Hotels (Managed)": OPERATOR_SETUP_BRANDS_BY_SLUG["minor-hotels-managed"],
  "Playa Hotels & Resorts": OPERATOR_SETUP_BRANDS_BY_SLUG["playa-hotels-resorts"],
  "Royalton Hotels & Resorts": OPERATOR_SETUP_BRANDS_BY_SLUG["royalton-hotels-resorts"],
  "Driftwood Hospitality Management":
    OPERATOR_SETUP_BRANDS_BY_SLUG["driftwood-hospitality-management"],
  "Remington Hospitality": OPERATOR_SETUP_BRANDS_BY_SLUG["remington-hospitality"],
  "Cordillera One Gestión": OPERATOR_SETUP_BRANDS_BY_SLUG["cordillera-one-gestion"],
  "Cenote Azul Operadores": OPERATOR_SETUP_BRANDS_BY_SLUG["cenote-azul-operadores"],
  "Antillano Norte Hospitality Group":
    OPERATOR_SETUP_BRANDS_BY_SLUG["antillano-norte-hospitality-group"],
  "Viento Sur Gestión Hotelera": OPERATOR_SETUP_BRANDS_BY_SLUG["viento-sur-gestion-hotelera"],
  "Mangle Azul Hospitalidad": OPERATOR_SETUP_BRANDS_BY_SLUG["mangle-azul-hospitalidad"],
  "Panamerican Lodging Partners S.A.":
    OPERATOR_SETUP_BRANDS_BY_SLUG["panamerican-lodging-partners"],
  "Río Plata Hotel Partners": OPERATOR_SETUP_BRANDS_BY_SLUG["rio-plata-hotel-partners"],
  "Barrio Hotelero CDMX": OPERATOR_SETUP_BRANDS_BY_SLUG["barrio-hotelero-cdmx"],
  "Metro Lodging São Paulo": OPERATOR_SETUP_BRANDS_BY_SLUG["metro-lodging-sao-paulo"],
  "Oro Verde Lodge & Hotel Operators":
    OPERATOR_SETUP_BRANDS_BY_SLUG["oro-verde-lodge-hotel-operators"],
});

/**
 * @param {{ slug?: string|null, companyName?: string|null }} identity
 * @returns {OperatorBrandsSpec | null}
 */
export function resolveOperatorBrands(identity = {}) {
  const slug = String(identity.slug || "").trim();
  if (slug && OPERATOR_SETUP_BRANDS_BY_SLUG[slug]) return OPERATOR_SETUP_BRANDS_BY_SLUG[slug];
  const name = String(identity.companyName || "").trim();
  if (name && OPERATOR_SETUP_BRANDS_BY_COMPANY_NAME[name]) {
    return OPERATOR_SETUP_BRANDS_BY_COMPANY_NAME[name];
  }
  return null;
}
