/**
 * Canonical Operator Setup yearEstablished + yearsInBusiness.
 * yearsInBusiness = asOfYear - yearEstablished (calendar years as of mid-year asOf).
 * Do not invent founding years without a source note.
 */
export const OPERATOR_SETUP_YEARS_AS_OF = 2026;

/**
 * @typedef {{ yearEstablished: number, yearsInBusiness: number, sourceNote: string }} OperatorYearsSpec
 */

function y(yearEstablished, sourceNote) {
  const ye = Number(yearEstablished);
  return Object.freeze({
    yearEstablished: ye,
    yearsInBusiness: OPERATOR_SETUP_YEARS_AS_OF - ye,
    sourceNote,
  });
}

/** @type {Record<string, OperatorYearsSpec>} keyed by factory/baseline slug */
export const OPERATOR_SETUP_YEARS_BY_SLUG = Object.freeze({
  "arbor-lodging-cala": y(2006, "Arbor Lodging public materials / baseline Profile"),
  "hotel-equities-cala": y(
    1989,
    "Hotel Equities company founding (1989) — not CALA division start year"
  ),
  "ghl-hoteles": y(1964, "GHL Hotels company profile / existing Profile"),
  "aimbridge-latam": y(2003, "Aimbridge Hospitality founded 2003 (parent; LATAM lens)"),
  "tafer-hotels-resorts": y(
    1984,
    "Villa Group / TAFER founder Fernando González Corona formed Villa Group 1984 (Nitu / company bios)"
  ),
  "grupo-presidente": y(1961, "Grupo Presidente multi-decade Mexican hospitality (~50+ years; 1961 industry cite)"),
  highgate: y(1988, "Highgate founded 1988 (company profiles / CB Insights)"),
  "grupo-hotelero-santa-fe": y(
    2006,
    "GHSF incorporated 24 Nov 2006 (annual report); operations began 2010"
  ),
  "arriva-hospitality-group": y(1967, "Arriva About timeline — operations from 1967"),
  "brittain-resorts-hotels": y(1943, "Brittain Resorts founded 1943 (brittainresorts.com)"),
  "atlantica-hotels-international": y(
    1998,
    "Atlantica Hotels International founding year (industry company profiles)"
  ),
  "marriott-international-managed": y(1927, "Marriott International founding 1927"),
  "ihg-managed": y(2003, "IHG Hotels & Resorts group formation 2003"),
  "hilton-managed": y(1919, "Hilton Hotels founding 1919"),
  "accor-managed": y(1967, "Accor founding 1967 (Novotel)"),
  "minor-hotels-managed": y(1978, "Minor Hotels founding 1978"),
  "playa-hotels-resorts": y(2006, "Playa Hotels & Resorts founded 2006"),
  "royalton-hotels-resorts": y(
    2010,
    "Blue Diamond / Royalton corporate origin ~2010 (15th anniversary 2025 materials)"
  ),
  "driftwood-hospitality-management": y(
    1999,
    "Driftwood Hospitality Management founded 1999"
  ),
  "remington-hospitality": y(1968, "Remington founded 1968 (remingtonhospitality.com)"),
  aadesa: y(2003, "AADESA Hotel Management founded 2003 (RocketReach / company profiles; Clarín founding partners)"),
  "alvarez-arguelles-hoteles": y(
    1954,
    "Official Historia: Hotel Europa rental Mar del Plata begins chain operations (1954); Iruña acquired 1958"
  ),
  "auberge-resorts-collection": y(
    1998,
    "Mark Harmon formed Auberge Resorts management company 1998 (flagship Auberge du Soleil traces to 1981 restaurant)"
  ),
  "barcelo-hotel-group": y(1931, "Barceló Group travel/hotel origins Mallorca 1931 (corporate history)"),
  "four-seasons-hotels-and-resorts": y(1960, "Four Seasons founded by Isadore Sharp, Toronto 1960"),
  "hyatt-managed": y(1957, "Hyatt House near LAX 1957 — modern Hyatt lodging origin"),
  "mandarin-oriental-hotel-group": y(
    1963,
    "The Mandarin Hong Kong opened 1963 — Mandarin Oriental Hotel Group operating origin"
  ),
  "melia-hotels-international": y(1956, "Gabriel Escarrer founded Meliá in Palma 1956"),
  "rosewood-hotel-group": y(1979, "Rosewood founded by Caroline Rose Hunt 1979"),
  "shangri-la-group": y(1971, "First Shangri-La hotel Singapore 1971 (Kuok / Shangri-La Asia)"),
  "sonesta-international": y(
    1937,
    "A.M. Sonnabend Preston Beach Hotel 1937; company renamed Sonesta 1970 (Sonesta Legacy Story)"
  ),
  "tremun-hoteles": y(2005, "Tremun Hoteles began operations 2005 (tremunhoteles.com.ar/sobre-tremun)"),
  "grupo-marta-hospitality": y(1960, "Grupo Marta founded 1960 (official About)"),
  "oxo-hotel": y(2009, "OxoHotel founding ~2009 (prior Wave E Setup pack)"),
  "grupo-iberostar": y(1956, "Iberostar / Grupo Iberostar founding 1956 (Wave E pack)"),
  // Staging / factory profiles already holding years — lock to same values
  "cordillera-one-gestion": y(2009, "Operator Setup Profile"),
  "cenote-azul-operadores": y(2012, "Operator Setup Profile"),
  "antillano-norte-hospitality-group": y(2005, "Operator Setup Profile"),
  "viento-sur-gestion-hotelera": y(2001, "Operator Setup Profile"),
  "mangle-azul-hospitalidad": y(2007, "Operator Setup Profile"),
  "panamerican-lodging-partners": y(1995, "Operator Setup Profile"),
  "rio-plata-hotel-partners": y(1998, "Operator Setup Profile"),
  "barrio-hotelero-cdmx": y(2016, "Operator Setup Profile"),
  "metro-lodging-sao-paulo": y(2014, "Operator Setup Profile"),
  "oro-verde-lodge-hotel-operators": y(2004, "Operator Setup Profile"),
});

export const OPERATOR_SETUP_YEARS_BY_COMPANY_NAME = Object.freeze({
  "Arbor Lodging (CALA)": OPERATOR_SETUP_YEARS_BY_SLUG["arbor-lodging-cala"],
  "Hotel Equities (CALA)": OPERATOR_SETUP_YEARS_BY_SLUG["hotel-equities-cala"],
  "GHL Hoteles (GHL Holding)": OPERATOR_SETUP_YEARS_BY_SLUG["ghl-hoteles"],
  "Aimbridge Hospitality (LATAM)": OPERATOR_SETUP_YEARS_BY_SLUG["aimbridge-latam"],
  "Tafer Hotels & Resorts": OPERATOR_SETUP_YEARS_BY_SLUG["tafer-hotels-resorts"],
  "Grupo Presidente": OPERATOR_SETUP_YEARS_BY_SLUG["grupo-presidente"],
  Highgate: OPERATOR_SETUP_YEARS_BY_SLUG.highgate,
  "Grupo Hotelero Santa Fe": OPERATOR_SETUP_YEARS_BY_SLUG["grupo-hotelero-santa-fe"],
  "Arriva Hospitality Group (AHG)": OPERATOR_SETUP_YEARS_BY_SLUG["arriva-hospitality-group"],
  "Brittain Resorts & Hotels (BRH)": OPERATOR_SETUP_YEARS_BY_SLUG["brittain-resorts-hotels"],
  "Atlantica Hotels International (AHI)":
    OPERATOR_SETUP_YEARS_BY_SLUG["atlantica-hotels-international"],
  "Marriott International (Managed)":
    OPERATOR_SETUP_YEARS_BY_SLUG["marriott-international-managed"],
  "IHG Hotels & Resorts (Managed)": OPERATOR_SETUP_YEARS_BY_SLUG["ihg-managed"],
  "Hilton (Managed)": OPERATOR_SETUP_YEARS_BY_SLUG["hilton-managed"],
  "Accor (Managed)": OPERATOR_SETUP_YEARS_BY_SLUG["accor-managed"],
  "Minor Hotels (Managed)": OPERATOR_SETUP_YEARS_BY_SLUG["minor-hotels-managed"],
  "Playa Hotels & Resorts": OPERATOR_SETUP_YEARS_BY_SLUG["playa-hotels-resorts"],
  "Royalton Hotels & Resorts": OPERATOR_SETUP_YEARS_BY_SLUG["royalton-hotels-resorts"],
  "Driftwood Hospitality Management":
    OPERATOR_SETUP_YEARS_BY_SLUG["driftwood-hospitality-management"],
  "Remington Hospitality": OPERATOR_SETUP_YEARS_BY_SLUG["remington-hospitality"],
  AADESA: OPERATOR_SETUP_YEARS_BY_SLUG.aadesa,
  "Álvarez Argüelles Hoteles": OPERATOR_SETUP_YEARS_BY_SLUG["alvarez-arguelles-hoteles"],
  "Auberge Resorts Collection": OPERATOR_SETUP_YEARS_BY_SLUG["auberge-resorts-collection"],
  "Barceló Hotel Group": OPERATOR_SETUP_YEARS_BY_SLUG["barcelo-hotel-group"],
  "Four Seasons Hotels and Resorts": OPERATOR_SETUP_YEARS_BY_SLUG["four-seasons-hotels-and-resorts"],
  "Hyatt (Managed)": OPERATOR_SETUP_YEARS_BY_SLUG["hyatt-managed"],
  "Mandarin Oriental Hotel Group": OPERATOR_SETUP_YEARS_BY_SLUG["mandarin-oriental-hotel-group"],
  "Meliá Hotels International": OPERATOR_SETUP_YEARS_BY_SLUG["melia-hotels-international"],
  "Rosewood Hotel Group": OPERATOR_SETUP_YEARS_BY_SLUG["rosewood-hotel-group"],
  "Shangri-La Group": OPERATOR_SETUP_YEARS_BY_SLUG["shangri-la-group"],
  "Sonesta International": OPERATOR_SETUP_YEARS_BY_SLUG["sonesta-international"],
  "Tremun Hoteles": OPERATOR_SETUP_YEARS_BY_SLUG["tremun-hoteles"],
  "Grupo Marta Hospitality": OPERATOR_SETUP_YEARS_BY_SLUG["grupo-marta-hospitality"],
  OxoHotel: OPERATOR_SETUP_YEARS_BY_SLUG["oxo-hotel"],
  "Grupo Iberostar": OPERATOR_SETUP_YEARS_BY_SLUG["grupo-iberostar"],
  "Cordillera One Gestión": OPERATOR_SETUP_YEARS_BY_SLUG["cordillera-one-gestion"],
  "Cenote Azul Operadores": OPERATOR_SETUP_YEARS_BY_SLUG["cenote-azul-operadores"],
  "Antillano Norte Hospitality Group":
    OPERATOR_SETUP_YEARS_BY_SLUG["antillano-norte-hospitality-group"],
  "Viento Sur Gestión Hotelera": OPERATOR_SETUP_YEARS_BY_SLUG["viento-sur-gestion-hotelera"],
  "Mangle Azul Hospitalidad": OPERATOR_SETUP_YEARS_BY_SLUG["mangle-azul-hospitalidad"],
  "Panamerican Lodging Partners S.A.":
    OPERATOR_SETUP_YEARS_BY_SLUG["panamerican-lodging-partners"],
  "Río Plata Hotel Partners": OPERATOR_SETUP_YEARS_BY_SLUG["rio-plata-hotel-partners"],
  "Barrio Hotelero CDMX": OPERATOR_SETUP_YEARS_BY_SLUG["barrio-hotelero-cdmx"],
  "Metro Lodging São Paulo": OPERATOR_SETUP_YEARS_BY_SLUG["metro-lodging-sao-paulo"],
  "Oro Verde Lodge & Hotel Operators":
    OPERATOR_SETUP_YEARS_BY_SLUG["oro-verde-lodge-hotel-operators"],
});

/**
 * @param {{ slug?: string|null, companyName?: string|null }} identity
 * @returns {OperatorYearsSpec | null}
 */
export function resolveOperatorYears(identity = {}) {
  const slug = String(identity.slug || "").trim();
  if (slug && OPERATOR_SETUP_YEARS_BY_SLUG[slug]) return OPERATOR_SETUP_YEARS_BY_SLUG[slug];
  const name = String(identity.companyName || "").trim();
  if (name && OPERATOR_SETUP_YEARS_BY_COMPANY_NAME[name]) {
    return OPERATOR_SETUP_YEARS_BY_COMPANY_NAME[name];
  }
  return null;
}
