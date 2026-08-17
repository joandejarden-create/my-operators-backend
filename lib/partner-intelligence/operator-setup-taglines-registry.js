/**
 * Canonical Operator Setup companyTagline — real published slogans / purpose lines only.
 * Never invent marketing copy. Prefer official homepage hero, trademarked slogan, or
 * published corporate purpose. If no verified short tagline exists, leave empty (null).
 *
 * Staging / sample operators: clear invented taglines (action: clear).
 */

/**
 * @typedef {{
 *   companyTagline: string | null,
 *   sourceNote: string,
 *   clearIfPresent?: boolean
 * }} OperatorTaglineSpec
 */

/** @param {string|null} tagline @param {string} sourceNote @param {{ clearIfPresent?: boolean }} [opts] */
function t(tagline, sourceNote, opts = {}) {
  const companyTagline =
    tagline == null || String(tagline).trim() === "" ? null : String(tagline).trim();
  return Object.freeze({
    companyTagline,
    sourceNote,
    clearIfPresent: Boolean(opts.clearIfPresent),
  });
}

/** @type {Record<string, OperatorTaglineSpec>} keyed by factory/baseline slug */
export const OPERATOR_SETUP_TAGLINES_BY_SLUG = Object.freeze({
  "hotel-equities-cala": t(
    "Deliver unrivaled value for owners, partners, guests, and team members",
    "hotelequities.com/culture + FAQ — published Purpose statement"
  ),
  "arbor-lodging-cala": t(
    "Smart Deals. Good People. Great Results.",
    "arborlodging.com homepage hero slogan"
  ),
  highgate: t(
    null,
    "No short official tagline verified on highgate.com (homepage uses descriptive positioning only) — leave empty"
  ),
  "aimbridge-latam": t(
    "Our People. Your Success.",
    "aimbridgehospitality.com homepage hero (global Aimbridge; LATAM is Aimbridge division)"
  ),
  "ghl-hoteles": t(
    "Una Experiencia GHL",
    "ghlhoteles.com guest marketing campaign / #UnaExperienciaGHL"
  ),
  "tafer-hotels-resorts": t(
    "Extraordinary vacation experiences for a day, a week, or a lifetime",
    "taferresorts.com About / mission phrasing (merge excellence, quality and creativity…)"
  ),
  "grupo-presidente": t(
    "La mejor forma de viajar por México",
    "grupopresidente.com.mx/quienes-somos — published slogan"
  ),
  "grupo-hotelero-santa-fe": t(
    null,
    "No short official tagline verified on gsf-hotels.com corporate site — leave empty"
  ),
  "arriva-hospitality-group": t(
    "Dedicated to make every moment unforgettable",
    "arrivahotels.mx homepage meta/description (also mission: Generar experiencias memorables…)"
  ),
  "brittain-resorts-hotels": t(
    "Elevating Hospitality Since 1943.",
    "brittainresorts.com homepage H1"
  ),
  "atlantica-hotels-international": t(
    "Taking care of those who trust us",
    "ahi.com.br/en homepage headline"
  ),
  "marriott-international-managed": t(
    "Maximize your financial performance",
    "hotel-development.marriott.com Managed by Marriott — 'one simple goal…to maximize your financial performance'"
  ),
  "hilton-managed": t(
    "Fill the earth with the light and warmth of hospitality",
    "hilton.com/en/corporate — founding vision / purpose"
  ),
  "ihg-managed": t(
    "True Hospitality for Good",
    "ihgplc.com — published corporate purpose"
  ),
  "accor-managed": t(
    "Develop with Accor",
    "group.accor.com/en/hotel-development — official development partnership brand line"
  ),
  "minor-hotels-managed": t(
    "Explore the World with Minor Hotels",
    "minorhotels.com homepage H1"
  ),
  "playa-hotels-resorts": t(
    "Service from the Heart®",
    "Playa public materials / press — signature Service from the Heart®"
  ),
  "royalton-hotels-resorts": t(
    "All-In Luxury®",
    "royaltonresorts.com — trademarked All-In Luxury® brand concept"
  ),
  "driftwood-hospitality-management": t(
    null,
    "No short official tagline verified on driftwoodhospitality.com — leave empty"
  ),
  "remington-hospitality": t(
    "Driven by People. Powered by Performance.",
    "remingtonhospitality.com homepage H1"
  ),

  // Staging / sample operators — invented taglines must be cleared
  "cordillera-one-gestion": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "cenote-azul-operadores": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "antillano-norte-hospitality-group": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "viento-sur-gestion-hotelera": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "mangle-azul-hospitalidad": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "panamerican-lodging-partners": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "rio-plata-hotel-partners": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "barrio-hotelero-cdmx": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "metro-lodging-sao-paulo": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
  "oro-verde-lodge-hotel-operators": t(null, "Staging sample — clear invented tagline", {
    clearIfPresent: true,
  }),
});

export const OPERATOR_SETUP_TAGLINES_BY_COMPANY_NAME = Object.freeze({
  "Hotel Equities (CALA)": OPERATOR_SETUP_TAGLINES_BY_SLUG["hotel-equities-cala"],
  "Arbor Lodging (CALA)": OPERATOR_SETUP_TAGLINES_BY_SLUG["arbor-lodging-cala"],
  Highgate: OPERATOR_SETUP_TAGLINES_BY_SLUG.highgate,
  "Aimbridge Hospitality (LATAM)": OPERATOR_SETUP_TAGLINES_BY_SLUG["aimbridge-latam"],
  "GHL Hoteles (GHL Holding)": OPERATOR_SETUP_TAGLINES_BY_SLUG["ghl-hoteles"],
  "Tafer Hotels & Resorts": OPERATOR_SETUP_TAGLINES_BY_SLUG["tafer-hotels-resorts"],
  "Grupo Presidente": OPERATOR_SETUP_TAGLINES_BY_SLUG["grupo-presidente"],
  "Grupo Hotelero Santa Fe": OPERATOR_SETUP_TAGLINES_BY_SLUG["grupo-hotelero-santa-fe"],
  "Arriva Hospitality Group (AHG)": OPERATOR_SETUP_TAGLINES_BY_SLUG["arriva-hospitality-group"],
  "Brittain Resorts & Hotels (BRH)": OPERATOR_SETUP_TAGLINES_BY_SLUG["brittain-resorts-hotels"],
  "Atlantica Hotels International (AHI)":
    OPERATOR_SETUP_TAGLINES_BY_SLUG["atlantica-hotels-international"],
  "Marriott International (Managed)":
    OPERATOR_SETUP_TAGLINES_BY_SLUG["marriott-international-managed"],
  "Hilton (Managed)": OPERATOR_SETUP_TAGLINES_BY_SLUG["hilton-managed"],
  "IHG Hotels & Resorts (Managed)": OPERATOR_SETUP_TAGLINES_BY_SLUG["ihg-managed"],
  "Accor (Managed)": OPERATOR_SETUP_TAGLINES_BY_SLUG["accor-managed"],
  "Minor Hotels (Managed)": OPERATOR_SETUP_TAGLINES_BY_SLUG["minor-hotels-managed"],
  "Playa Hotels & Resorts": OPERATOR_SETUP_TAGLINES_BY_SLUG["playa-hotels-resorts"],
  "Royalton Hotels & Resorts": OPERATOR_SETUP_TAGLINES_BY_SLUG["royalton-hotels-resorts"],
  "Driftwood Hospitality Management":
    OPERATOR_SETUP_TAGLINES_BY_SLUG["driftwood-hospitality-management"],
  "Remington Hospitality": OPERATOR_SETUP_TAGLINES_BY_SLUG["remington-hospitality"],
  "Cordillera One Gestión": OPERATOR_SETUP_TAGLINES_BY_SLUG["cordillera-one-gestion"],
  "Cenote Azul Operadores": OPERATOR_SETUP_TAGLINES_BY_SLUG["cenote-azul-operadores"],
  "Antillano Norte Hospitality Group":
    OPERATOR_SETUP_TAGLINES_BY_SLUG["antillano-norte-hospitality-group"],
  "Viento Sur Gestión Hotelera": OPERATOR_SETUP_TAGLINES_BY_SLUG["viento-sur-gestion-hotelera"],
  "Mangle Azul Hospitalidad": OPERATOR_SETUP_TAGLINES_BY_SLUG["mangle-azul-hospitalidad"],
  "Panamerican Lodging Partners S.A.":
    OPERATOR_SETUP_TAGLINES_BY_SLUG["panamerican-lodging-partners"],
  "Río Plata Hotel Partners": OPERATOR_SETUP_TAGLINES_BY_SLUG["rio-plata-hotel-partners"],
  "Barrio Hotelero CDMX": OPERATOR_SETUP_TAGLINES_BY_SLUG["barrio-hotelero-cdmx"],
  "Metro Lodging São Paulo": OPERATOR_SETUP_TAGLINES_BY_SLUG["metro-lodging-sao-paulo"],
  "Oro Verde Lodge & Hotel Operators":
    OPERATOR_SETUP_TAGLINES_BY_SLUG["oro-verde-lodge-hotel-operators"],
});

/**
 * @param {{ slug?: string|null, companyName?: string|null }} identity
 * @returns {OperatorTaglineSpec | null}
 */
export function resolveOperatorTagline(identity = {}) {
  const slug = String(identity.slug || "").trim();
  if (slug && OPERATOR_SETUP_TAGLINES_BY_SLUG[slug]) return OPERATOR_SETUP_TAGLINES_BY_SLUG[slug];
  const name = String(identity.companyName || "").trim();
  if (name && OPERATOR_SETUP_TAGLINES_BY_COMPANY_NAME[name]) {
    return OPERATOR_SETUP_TAGLINES_BY_COMPANY_NAME[name];
  }
  return null;
}
