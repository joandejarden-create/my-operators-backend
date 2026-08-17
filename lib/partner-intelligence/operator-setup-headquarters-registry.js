/**
 * Canonical Operator Setup headquarters — City, Country only.
 * Used by packs + live Airtable normalize. Do not invent cities without a source.
 */
export const OPERATOR_SETUP_HQ_FORMAT =
  "City, Country (e.g. Bethesda, United States). No country-only or narrative HQ strings.";

/**
 * @typedef {{ city: string, country: string, headquarters: string, sourceNote: string }} OperatorHqSpec
 */

/** @type {Record<string, OperatorHqSpec>} keyed by factory/baseline slug */
export const OPERATOR_SETUP_HEADQUARTERS_BY_SLUG = Object.freeze({
  "arbor-lodging-cala": hq("Chicago", "United States", "Arbor Lodging public / company materials"),
  "hotel-equities-cala": hq(
    "Alpharetta",
    "United States",
    "Hotel Equities Alpharetta HQ (Atlanta metro) — hotelequities.com / Alpharetta headquarters press release"
  ),
  "ghl-hoteles": hq("Bogotá", "Colombia", "GHL Hotels company profile — Bogotá"),
  "aimbridge-latam": hq(
    "Monterrey",
    "Mexico",
    "Aimbridge LATAM office (Pabellón M, Monterrey) on aimbridgehospitality.com/contact — LATAM Explorer lens"
  ),
  "tafer-hotels-resorts": hq("Puerto Vallarta", "Mexico", "TAFER / LinkedIn / CB Insights — Puerto Vallarta"),
  "grupo-presidente": hq("Mexico City", "Mexico", "Grupo Presidente corporate address — Mexico City"),
  highgate: hq("New York", "United States", "Highgate corporate office — New York"),
  "grupo-hotelero-santa-fe": hq(
    "Mexico City",
    "Mexico",
    "GSF annual report — Santa Fe, Ciudad de México"
  ),
  "arriva-hospitality-group": hq(
    "Guadalajara",
    "Mexico",
    "Arriva Hospitality Group corporate presence — Guadalajara"
  ),
  "brittain-resorts-hotels": hq("Myrtle Beach", "United States", "Brittain Resorts — Myrtle Beach"),
  "atlantica-hotels-international": hq(
    "Barueri",
    "Brazil",
    "Atlantica Hotels International — Barueri (São Paulo metro)"
  ),
  "marriott-international-managed": hq(
    "Bethesda",
    "United States",
    "Marriott International corporate HQ — Bethesda"
  ),
  "ihg-managed": hq("Denham", "United Kingdom", "IHG Hotels & Resorts — Denham"),
  "hilton-managed": hq("McLean", "United States", "Hilton — McLean"),
  "accor-managed": hq("Issy-les-Moulineaux", "France", "Accor Group — Issy-les-Moulineaux"),
  "minor-hotels-managed": hq("Bangkok", "Thailand", "Minor Hotels — Bangkok"),
  "playa-hotels-resorts": hq("Fairfax", "United States", "Playa Hotels & Resorts — Fairfax"),
  "royalton-hotels-resorts": hq(
    "St. Michael",
    "Barbados",
    "Royalton Hotels & Resorts marketing address — Cidel Place, St. Michael, Barbados"
  ),
  "driftwood-hospitality-management": hq(
    "North Palm Beach",
    "United States",
    "Driftwood Hospitality Management — North Palm Beach, FL"
  ),
  "remington-hospitality": hq(
    "Dallas",
    "United States",
    "Remington Hospitality corporate office — Dallas (CALA regional office Miami labeled separately)"
  ),
  // Factory / staging operators — City, Country already on Profile; lock format
  "cordillera-one-gestion": hq("Bogotá", "Colombia", "Operator Setup Profile — Cordillera One"),
  "cenote-azul-operadores": hq("Mérida", "Mexico", "Operator Setup Profile — Cenote Azul"),
  "antillano-norte-hospitality-group": hq(
    "San Juan",
    "Puerto Rico",
    "Operator Setup Profile — Antillano Norte"
  ),
  "viento-sur-gestion-hotelera": hq("Santiago", "Chile", "Operator Setup Profile — Viento Sur"),
  "mangle-azul-hospitalidad": hq("Barranquilla", "Colombia", "Operator Setup Profile — Mangle Azul"),
  "panamerican-lodging-partners": hq(
    "São Paulo",
    "Brazil",
    "Operator Setup Profile — Panamerican Lodging Partners"
  ),
  "rio-plata-hotel-partners": hq("Montevideo", "Uruguay", "Operator Setup Profile — Río Plata"),
  "barrio-hotelero-cdmx": hq("Mexico City", "Mexico", "Operator Setup Profile — Barrio Hotelero CDMX"),
  "metro-lodging-sao-paulo": hq("São Paulo", "Brazil", "Operator Setup Profile — Metro Lodging"),
  "oro-verde-lodge-hotel-operators": hq(
    "San José",
    "Costa Rica",
    "Operator Setup Profile — Oro Verde"
  ),
});

/** Optional aliases by Master company_name (exact trim match). */
export const OPERATOR_SETUP_HEADQUARTERS_BY_COMPANY_NAME = Object.freeze({
  "Arbor Lodging (CALA)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["arbor-lodging-cala"],
  "Hotel Equities (CALA)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["hotel-equities-cala"],
  "GHL Hoteles (GHL Holding)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["ghl-hoteles"],
  "Aimbridge Hospitality (LATAM)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["aimbridge-latam"],
  "Tafer Hotels & Resorts": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["tafer-hotels-resorts"],
  "Grupo Presidente": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["grupo-presidente"],
  Highgate: OPERATOR_SETUP_HEADQUARTERS_BY_SLUG.highgate,
  "Grupo Hotelero Santa Fe": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["grupo-hotelero-santa-fe"],
  "Arriva Hospitality Group (AHG)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["arriva-hospitality-group"],
  "Brittain Resorts & Hotels (BRH)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["brittain-resorts-hotels"],
  "Atlantica Hotels International (AHI)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["atlantica-hotels-international"],
  "Marriott International (Managed)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["marriott-international-managed"],
  "IHG Hotels & Resorts (Managed)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["ihg-managed"],
  "Hilton (Managed)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["hilton-managed"],
  "Accor (Managed)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["accor-managed"],
  "Minor Hotels (Managed)": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["minor-hotels-managed"],
  "Playa Hotels & Resorts": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["playa-hotels-resorts"],
  "Royalton Hotels & Resorts": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["royalton-hotels-resorts"],
  "Driftwood Hospitality Management":
    OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["driftwood-hospitality-management"],
  "Remington Hospitality": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["remington-hospitality"],
  "Cordillera One Gestión": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["cordillera-one-gestion"],
  "Cenote Azul Operadores": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["cenote-azul-operadores"],
  "Antillano Norte Hospitality Group":
    OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["antillano-norte-hospitality-group"],
  "Viento Sur Gestión Hotelera": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["viento-sur-gestion-hotelera"],
  "Mangle Azul Hospitalidad": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["mangle-azul-hospitalidad"],
  "Panamerican Lodging Partners S.A.":
    OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["panamerican-lodging-partners"],
  "Río Plata Hotel Partners": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["rio-plata-hotel-partners"],
  "Barrio Hotelero CDMX": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["barrio-hotelero-cdmx"],
  "Metro Lodging São Paulo": OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["metro-lodging-sao-paulo"],
  "Oro Verde Lodge & Hotel Operators":
    OPERATOR_SETUP_HEADQUARTERS_BY_SLUG["oro-verde-lodge-hotel-operators"],
});

function hq(city, country, sourceNote) {
  return Object.freeze({
    city,
    country,
    headquarters: `${city}, ${country}`,
    sourceNote,
  });
}

/**
 * @param {string|null|undefined} value
 * @returns {boolean}
 */
export function isValidOperatorHeadquartersFormat(value) {
  const s = String(value || "").trim();
  if (!s) return false;
  // City, Country — single comma, both sides non-empty, no parentheses/narrative clauses
  if (/[();]|confirm |enterprise|platform|portfolio|global |diligence/i.test(s)) return false;
  const parts = s.split(",").map((p) => p.trim()).filter(Boolean);
  if (parts.length !== 2) return false;
  if (parts[0].length < 2 || parts[1].length < 2) return false;
  // Country-only rejection: left side must look like a city (not a country name alone when right empty — already 2 parts)
  return true;
}

/**
 * @param {{ slug?: string|null, recordId?: string|null, companyName?: string|null }} identity
 * @returns {OperatorHqSpec | null}
 */
export function resolveOperatorHeadquarters(identity = {}) {
  const slug = String(identity.slug || "").trim();
  if (slug && OPERATOR_SETUP_HEADQUARTERS_BY_SLUG[slug]) {
    return OPERATOR_SETUP_HEADQUARTERS_BY_SLUG[slug];
  }
  const name = String(identity.companyName || "").trim();
  if (name && OPERATOR_SETUP_HEADQUARTERS_BY_COMPANY_NAME[name]) {
    return OPERATOR_SETUP_HEADQUARTERS_BY_COMPANY_NAME[name];
  }
  return null;
}

export function listOperatorHeadquartersSlugs() {
  return Object.keys(OPERATOR_SETUP_HEADQUARTERS_BY_SLUG);
}
