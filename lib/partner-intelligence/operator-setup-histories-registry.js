/**
 * Canonical Operator Setup companyHistory — real, source-backed narratives only.
 * Never invent founding stories. Staging/sample operators: clear invented fiction.
 */

/**
 * @typedef {{
 *   companyHistory: string | null,
 *   sourceNote: string
 * }} OperatorHistorySpec
 */

/** @param {string|null} history @param {string} sourceNote */
function h(history, sourceNote) {
  const companyHistory =
    history == null || String(history).trim() === "" ? null : String(history).trim();
  return Object.freeze({ companyHistory, sourceNote });
}

/** @type {Record<string, OperatorHistorySpec>} keyed by factory/baseline slug */
export const OPERATOR_SETUP_HISTORIES_BY_SLUG = Object.freeze({
  "arbor-lodging-cala": h(
    "Arbor Lodging was founded in 2006 in Chicago by Vamsi Bonthala and Sheenal Patel as a vertically integrated hotel investment and management platform. The company operates Arbor Lodging Partners (investment) and Arbor Lodging Management (operations), with Arbor Management Solutions providing outsourced accounting and commercial support to other management companies. CALA expansion has been deliberate: a Mexico City office integrated with U.S. operations for several years, with Christian Hutchinson appointed Director of Business Development, CALA, in November 2025. This CALA Explorer profile should not be read as a current managed-hotel footprint claim for the region.",
    "arborlodging.com About / company materials; CALA BD appointment press"
  ),

  "hotel-equities-cala": h(
    "Founded in 1989 by Fred \"Coach\" Cerrone, Hotel Equities grew as a U.S. hotel management and development company centered on operational excellence and a people-first culture. In 2016 the company expanded into Canada, building an in-market corporate team and a multi-property Canadian portfolio. In 2024 Hotel Equities formed a strategic partnership with Trust Hospitality to launch and scale a dedicated Caribbean and Latin America (CALA) division — the lens for this Explorer profile.",
    "hotelequities.com company history / CALA partnership materials"
  ),

  highgate: h(
    "Highgate was founded in 1988 by Mahmood Khimji (with Mehdi Khimji) as a hospitality investment and management firm. Over more than three decades it has grown into a multi-segment platform spanning lifestyle and luxury, full-service, and select-service hotels, with investment, operations, technology, and development capabilities. Corporate materials describe a diverse portfolio across North America, Europe, the Caribbean, and Latin America; treat enterprise scale separately from any CALA-specific underwriting.",
    "highgate.com company positioning; founder/founding year from public company profiles"
  ),

  "aimbridge-latam": h(
    "Aimbridge Hospitality was founded in 2003 in Plano, Texas, with eight hotels. In 2019 Aimbridge merged with Interstate Hotels & Resorts (founded 1960), forming one of the world’s largest third-party hotel management companies. Official legacy materials cite later international expansion (including Canada and the Caribbean) and a dedicated All-Inclusive division launched in 2025. This Explorer profile is the LATAM / All-Inclusive operating lens — not the full global Aimbridge enterprise count.",
    "aimbridgehospitality.com/legacy.htm"
  ),

  "ghl-hoteles": h(
    "GHL Hoteles began operations in 1964 in Colombia (origins often traced to Popayán) and grew into one of Latin America’s leading multi-brand third-party hotel operators. Over decades the group expanded across Andean, Central American, and Southern Cone markets through proprietary brands (including GHL Collection, GHL Relax, and GHL Style) and partnerships with international chains such as Marriott, Hyatt, Sonesta, and Radisson. In 2022 Advent International announced a partnership to accelerate GHL’s regional growth. Public materials cite on the order of 60+ hotels across multiple Latin American countries — confirm current counts in diligence.",
    "GHL / Advent partnership releases; ghlhoteles.com / hospitalitynet company profiles"
  ),

  "tafer-hotels-resorts": h(
    "TAFER Hotels & Resorts is a Mexico-focused leisure hospitality company whose leadership traces more than three decades in tourism and vacation products. TAFER is the principal owner of The Villagroup Resorts collection and develops and operates distinctive leisure brands including Hotel Mousai, Garza Blanca, Villa del Palmar, Sierra Lago, and Sian Ka’an Village. Official About materials emphasize integrated capabilities across hotel design and construction, resort management, vacation ownership, marketing, and travel services.",
    "taferresorts.com/about-us"
  ),

  "grupo-presidente": h(
    "Grupo Presidente is a 100% Mexican hospitality company with more than 50 years in hotels and restaurants. Public materials state that the company represents Marriott, Hyatt, and IHG brands across major Mexican cities and beach destinations, alongside a portfolio of specialty restaurants and a Ballesol alliance for senior living residences. Corporate communications frame excellence and unique guest experiences as core values of a multi-decade owner-operator platform.",
    "grupopresidente.com.mx About / sustainability report positioning"
  ),

  "grupo-hotelero-santa-fe": h(
    "Grupo Hotelero Santa Fe (BMV: HOTEL) is a publicly listed Mexican hotel company focused on acquiring, developing, converting, and operating owned and third-party hotels. Corporate materials highlight the proprietary Krystal brand family (including Krystal Grand, Krystal Resorts/Urban lines) alongside international affiliations such as Hyatt, Hilton, and Accor (e.g. Ibis). The portfolio mixes beach and urban destinations across Mexico; treat published room and hotel counts as of the cited investor materials and confirm in diligence.",
    "gsf-hotels.com corporate / investor presentations"
  ),

  "arriva-hospitality-group": h(
    "Arriva Hospitality Group traces hotel operations in Mexico to 1967 (Presidente and Alameda Morelia on the official About timeline). Subsequent milestones published by the company include Vista Hotels (from 1978), Crown Paradise Club Cancún (1990), Crown Paradise Golden Puerto Vallarta (1997), the Aranzazú–Vista fusion (2005), creation of Arriva Hospitality Group (2012), Westin Cozumel (2017), Ibis Tijuana (2019), and Sensira Resort & Spa Riviera Maya (2020). Public home materials also cite roughly 1,700+ rooms across beach and city hotels under proprietary leisure brands and select international flags.",
    "arrivahotels.mx About timeline"
  ),

  "brittain-resorts-hotels": h(
    "Brittain Resorts & Hotels was founded in 1943 and is based in Myrtle Beach, South Carolina. Official materials describe a multi-decade full-service hospitality management company focused on the U.S. Southeast, with oceanfront resorts, condominium hotels, and branded select-service assets (including Courtyard and SpringHill Suites by Marriott) under management, plus extensive F&B operations. Public site copy emphasizes proprietary operating systems across HR, revenue management, and sales & marketing.",
    "brittainresorts.com About / portfolio"
  ),

  "atlantica-hotels-international": h(
    "Atlantica Hospitality International (historically Atlantica Hotels) was founded in 1998 and is headquartered in the São Paulo metro (Barueri). It grew into one of South America’s largest multi-brand hotel operators, with exclusive or long-standing franchise/master-franchise relationships in Brazil for Choice Hotels brands (including Comfort, Quality, and Radisson-by-Choice lines) plus partnerships with groups such as Hilton and Wyndham, alongside proprietary brands (e.g. Transamerica / by Atlantica family). Official materials position Atlantica as taking care of investor and guest relationships across a nationwide Brazilian portfolio — confirm current hotel/room counts in diligence.",
    "ahi.com.br; Choice/Radisson MFA press; company founding year from industry profiles"
  ),

  "marriott-international-managed": h(
    "Marriott International’s roots date to 1927; Managed by Marriott (MxM) is Marriott’s hotel management organization for owners of Marriott-branded hotels. Official development materials state a simple goal — maximize owner financial performance — and cite nearly a century of hospitality experience and decades of turnkey management expertise across luxury, premium, select, and all-inclusive brands. This Explorer profile is the brand-managed / MxM operating lens; enterprise hotel counts are labeled separately from any CALA managed subset.",
    "hotel-development.marriott.com Managed by Marriott; Marriott founding 1927"
  ),

  "hilton-managed": h(
    "Conrad Hilton entered the hotel business in 1919 with the Mobley Hotel in Cisco, Texas; the first hotel to bear the Hilton name opened in Dallas in 1925. Hilton Hotels Corporation later expanded domestically and internationally through ownership, management, and franchise models and today operates as a global multi-brand hospitality company under Hilton Honors. This Explorer profile is the brand-managed / management-agreement lens for owners evaluating Hilton operating pathways — not a pure third-party independent manager. Enterprise footprint is labeled separately from any CALA managed subset.",
    "stories.hilton.com/history; hilton.com/en/corporate"
  ),

  "ihg-managed": h(
    "InterContinental Hotels Group (IHG Hotels & Resorts) became an independent listed hotel company in 2003 following the separation of Six Continents’ hotels business. Brand lineage includes Holiday Inn (via Bass) and InterContinental (founded 1946 by Juan Trippe). IHG today describes itself as a global multi-brand hospitality company whose purpose is True Hospitality for Good, with franchise, managed, and owner pathways under IHG One Rewards. This Explorer profile is the brand-managed operating lens; enterprise scale is labeled separately from any CALA managed subset.",
    "ihgplc.com About / history; True Hospitality for Good purpose"
  ),

  "accor-managed": h(
    "Accor was founded in 1967 by Paul Dubrule and Gérard Pélisson with the first Novotel in Lille-Lesquin, France, later expanding through brands such as ibis and Mercure into one of the world’s leading hotel operators and franchisors. Group materials describe an asset-light model focused on management and franchise partnerships, a diversified brand portfolio, and the ALL Accor loyalty/booking platform. Official hotel-development pages emphasize an owner-centric, 360-degree partnership approach. This Explorer profile is the Accor brand-managed / management-contract lens — not a pure third-party independent manager.",
    "group.accor.com hotel development / Accor founding history"
  ),

  "minor-hotels-managed": h(
    "Minor Hotels was founded in Thailand (first hotel 1978/1979 per company About materials) by William E. Heinecke and grew into a global multi-brand hospitality company spanning ownership, management, and brand partnerships. Flagship and sister brands in Brand Basics include Anantara, NH Hotels, Oaks, and Tivoli, among others. Official purpose language emphasizes innovative hospitality experiences for guests, team members, and partners. This Explorer profile is the brand-managed Minor Hotels lens; enterprise/global footprint is labeled separately from any Americas/CALA managed subset.",
    "minorhotels.com/en/about-us"
  ),

  "playa-hotels-resorts": h(
    "Playa Hotels & Resorts was founded in 2006 as an owner, operator, and developer of all-inclusive resorts in prime beachfront locations in Mexico, Jamaica, and the Dominican Republic. Investor and corporate materials describe a multi-brand all-inclusive platform historically including Hyatt Ziva/Zilara and other affiliations, with Service from the Heart® as a signature guest-service philosophy. In June 2025 Hyatt completed its acquisition of Playa; treat post-close brand and management transitions as evolving and confirm current operating structure in diligence.",
    "Playa investor overview / press; Hyatt acquisition close June 2025"
  ),

  "royalton-hotels-resorts": h(
    "The platform’s public origin narratives begin around 2010/2011 under Blue Diamond Resorts. In 2025 the company publicly evolved its corporate identity to Royalton Hotels & Resorts to align with guest recognition of its Royalton-branded all-inclusive portfolio across Caribbean and Mexico beach destinations (and Costa Rica). Official guest materials emphasize the All-In Luxury® concept across Royalton family brands. Marketing/contact materials publish an address at Cidel Place, Lower Collymore Rock, St. Michael, Barbados.",
    "royaltonresorts.com; 2025 corporate identity materials"
  ),

  "driftwood-hospitality-management": h(
    "Driftwood Hospitality Management was founded in 1999 and is headquartered in North Palm Beach, Florida. Official site materials describe a top-20 U.S. third-party hotel management company operating full-service and select-service hotels across major brand families (including Hilton, Marriott, Hyatt, IHG, Wyndham, and Choice) plus independent/lifestyle assets, with capabilities spanning management, development, acquisitions, and turnaround situations. Published scale figures (hotels, rooms, brands, ownership groups) should be confirmed in diligence as of the cited date.",
    "driftwoodhospitality.com; founding year from company/industry profiles"
  ),

  "remington-hospitality": h(
    "Remington was founded in 1968 and later rebranded as Remington Hospitality. Official materials describe a multi-decade U.S. hotel management company focused on being the best rather than the biggest, with a people-led operating culture (motto: Where Passionate People Thrive) and the homepage line Driven by People. Powered by Performance. Corporate materials cite 120+ hotels across many brands plus independent/boutique assets. Remington publicly expanded into Caribbean & Latin America from late 2022, establishing a Miami regional office in 2023; corporate office is published in Dallas, Texas. Label enterprise U.S. scale separately from CALA footprint.",
    "remingtonhospitality.com; CALA expansion press"
  ),

  // Staging / sample — clear invented fiction
  "cordillera-one-gestion": h(null, "Staging sample — clear invented companyHistory"),
  "cenote-azul-operadores": h(null, "Staging sample — clear invented companyHistory"),
  "antillano-norte-hospitality-group": h(null, "Staging sample — clear invented companyHistory"),
  "viento-sur-gestion-hotelera": h(null, "Staging sample — clear invented companyHistory"),
  "mangle-azul-hospitalidad": h(null, "Staging sample — clear invented companyHistory"),
  "panamerican-lodging-partners": h(null, "Staging sample — clear invented companyHistory"),
  "rio-plata-hotel-partners": h(null, "Staging sample — clear invented companyHistory"),
  "barrio-hotelero-cdmx": h(null, "Staging sample — clear invented companyHistory"),
  "metro-lodging-sao-paulo": h(null, "Staging sample — clear invented companyHistory"),
  "oro-verde-lodge-hotel-operators": h(null, "Staging sample — clear invented companyHistory"),
});

export const OPERATOR_SETUP_HISTORIES_BY_COMPANY_NAME = Object.freeze({
  "Arbor Lodging (CALA)": OPERATOR_SETUP_HISTORIES_BY_SLUG["arbor-lodging-cala"],
  "Hotel Equities (CALA)": OPERATOR_SETUP_HISTORIES_BY_SLUG["hotel-equities-cala"],
  Highgate: OPERATOR_SETUP_HISTORIES_BY_SLUG.highgate,
  "Aimbridge Hospitality (LATAM)": OPERATOR_SETUP_HISTORIES_BY_SLUG["aimbridge-latam"],
  "GHL Hoteles (GHL Holding)": OPERATOR_SETUP_HISTORIES_BY_SLUG["ghl-hoteles"],
  "Tafer Hotels & Resorts": OPERATOR_SETUP_HISTORIES_BY_SLUG["tafer-hotels-resorts"],
  "Grupo Presidente": OPERATOR_SETUP_HISTORIES_BY_SLUG["grupo-presidente"],
  "Grupo Hotelero Santa Fe": OPERATOR_SETUP_HISTORIES_BY_SLUG["grupo-hotelero-santa-fe"],
  "Arriva Hospitality Group (AHG)": OPERATOR_SETUP_HISTORIES_BY_SLUG["arriva-hospitality-group"],
  "Brittain Resorts & Hotels (BRH)": OPERATOR_SETUP_HISTORIES_BY_SLUG["brittain-resorts-hotels"],
  "Atlantica Hotels International (AHI)":
    OPERATOR_SETUP_HISTORIES_BY_SLUG["atlantica-hotels-international"],
  "Marriott International (Managed)":
    OPERATOR_SETUP_HISTORIES_BY_SLUG["marriott-international-managed"],
  "Hilton (Managed)": OPERATOR_SETUP_HISTORIES_BY_SLUG["hilton-managed"],
  "IHG Hotels & Resorts (Managed)": OPERATOR_SETUP_HISTORIES_BY_SLUG["ihg-managed"],
  "Accor (Managed)": OPERATOR_SETUP_HISTORIES_BY_SLUG["accor-managed"],
  "Minor Hotels (Managed)": OPERATOR_SETUP_HISTORIES_BY_SLUG["minor-hotels-managed"],
  "Playa Hotels & Resorts": OPERATOR_SETUP_HISTORIES_BY_SLUG["playa-hotels-resorts"],
  "Royalton Hotels & Resorts": OPERATOR_SETUP_HISTORIES_BY_SLUG["royalton-hotels-resorts"],
  "Driftwood Hospitality Management":
    OPERATOR_SETUP_HISTORIES_BY_SLUG["driftwood-hospitality-management"],
  "Remington Hospitality": OPERATOR_SETUP_HISTORIES_BY_SLUG["remington-hospitality"],
  "Cordillera One Gestión": OPERATOR_SETUP_HISTORIES_BY_SLUG["cordillera-one-gestion"],
  "Cenote Azul Operadores": OPERATOR_SETUP_HISTORIES_BY_SLUG["cenote-azul-operadores"],
  "Antillano Norte Hospitality Group":
    OPERATOR_SETUP_HISTORIES_BY_SLUG["antillano-norte-hospitality-group"],
  "Viento Sur Gestión Hotelera": OPERATOR_SETUP_HISTORIES_BY_SLUG["viento-sur-gestion-hotelera"],
  "Mangle Azul Hospitalidad": OPERATOR_SETUP_HISTORIES_BY_SLUG["mangle-azul-hospitalidad"],
  "Panamerican Lodging Partners S.A.":
    OPERATOR_SETUP_HISTORIES_BY_SLUG["panamerican-lodging-partners"],
  "Río Plata Hotel Partners": OPERATOR_SETUP_HISTORIES_BY_SLUG["rio-plata-hotel-partners"],
  "Barrio Hotelero CDMX": OPERATOR_SETUP_HISTORIES_BY_SLUG["barrio-hotelero-cdmx"],
  "Metro Lodging São Paulo": OPERATOR_SETUP_HISTORIES_BY_SLUG["metro-lodging-sao-paulo"],
  "Oro Verde Lodge & Hotel Operators":
    OPERATOR_SETUP_HISTORIES_BY_SLUG["oro-verde-lodge-hotel-operators"],
});

/**
 * @param {{ slug?: string|null, companyName?: string|null }} identity
 * @returns {OperatorHistorySpec | null}
 */
export function resolveOperatorHistory(identity = {}) {
  const slug = String(identity.slug || "").trim();
  if (slug && OPERATOR_SETUP_HISTORIES_BY_SLUG[slug]) return OPERATOR_SETUP_HISTORIES_BY_SLUG[slug];
  const name = String(identity.companyName || "").trim();
  if (name && OPERATOR_SETUP_HISTORIES_BY_COMPANY_NAME[name]) {
    return OPERATOR_SETUP_HISTORIES_BY_COMPANY_NAME[name];
  }
  return null;
}
