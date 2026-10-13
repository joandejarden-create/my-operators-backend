/**
 * Packet 2.5 — Canonical entity registry for evaluation corpus resolution.
 * Not product Airtable SoT — local resolution anchors for existing evidence only.
 */

export const PACKET25_ENTITY_REGISTRY_VERSION = "packet-2.5b-entity-registry-v1";

export const PACKET25_ENTITY_REGISTRY = Object.freeze({
  hotels: [
    {
      id: "dhl_kgpv",
      name: "Krystal Grand Puerto Vallarta",
      scope: "PROPERTY_SPECIFIC",
      city: "Puerto Vallarta",
      country: "Mexico",
      rooms: 451,
      lat: 20.64842,
      lng: -105.241708,
      airtable_record_id: "recUNycnMwOVFX0hc",
      aliases: [
        { alias: "Krystal Grand Vallarta", alias_type: "TRADE_NAME", state: "VERIFIED_ALIAS" },
        { alias: "Hilton Puerto Vallarta Resort", alias_type: "FORMER_BRAND_IDENTITY", state: "VERIFIED_ALIAS" },
        { alias: "Krystal Altitude Puerto Vallarta", alias_type: "FORMER_NAME", state: "VERIFIED_ALIAS" },
        { alias: "The Hacienda at Krystal Grand Puerto Vallarta", alias_type: "LOCAL_NAME", state: "VERIFIED_ALIAS" },
      ],
    },
    {
      id: "dhl_krystal_resort_pv",
      name: "Krystal Resort Puerto Vallarta",
      scope: "PROPERTY_SPECIFIC",
      city: "Puerto Vallarta",
      country: "Mexico",
      rooms: 530,
      aliases: [
        // Ambiguous short form — candidate only (must not auto-merge with Grand)
        {
          alias: "Krystal Puerto Vallarta",
          alias_type: "TRADE_NAME",
          state: "CANDIDATE_ALIAS",
          confidence: 0.55,
        },
      ],
    },
    {
      id: "dhl_mahekal",
      name: "Mahekal Beach Resort",
      scope: "PROPERTY_SPECIFIC",
      city: "Playa Del Carmen",
      country: "Mexico",
      airtable_record_id: "recz3ng6hNt1sBD8t",
      aliases: [{ alias: "Mahekal", alias_type: "TRADE_NAME" }],
    },
    {
      id: "dhl_pedregal",
      name: "Waldorf Astoria Los Cabos Pedregal",
      scope: "PROPERTY_SPECIFIC",
      city: "Cabo San Lucas",
      country: "Mexico",
      aliases: [
        { alias: "Capella Pedregal", alias_type: "FORMER_BRAND_IDENTITY" },
        { alias: "Resort at Pedregal", alias_type: "FORMER_NAME" },
        { alias: "Pedregal", alias_type: "LOCAL_NAME" },
      ],
    },
    {
      id: "dhl_palmilla",
      name: "One&Only Palmilla",
      scope: "PROPERTY_SPECIFIC",
      city: "San Jose Del Cabo",
      country: "Mexico",
      aliases: [
        { alias: "One & Only Palmilla", alias_type: "TRADE_NAME" },
        { alias: "Palmilla", alias_type: "LOCAL_NAME" },
      ],
    },
    {
      id: "dhl_chable_maroma",
      name: "Chablé Maroma",
      scope: "PROPERTY_SPECIFIC",
      city: "Riviera Maya",
      country: "Mexico",
      aliases: [{ alias: "Chable Maroma", alias_type: "TRADE_NAME" }],
    },
    {
      id: "dhl_chable_yucatan",
      name: "Chablé Yucatán",
      scope: "PROPERTY_SPECIFIC",
      city: "Yucatán",
      country: "Mexico",
      aliases: [{ alias: "Chable Yucatan", alias_type: "TRADE_NAME" }],
    },
    {
      id: "dhl_sparse_gsf_op",
      name: "Krystal Urban Guadalajara",
      scope: "PROPERTY_SPECIFIC",
      city: "Guadalajara",
      country: "Mexico",
      aliases: [],
      note: "Sparse operator-known / owner-unresolved evaluation case",
    },
  ],
  organizations: [
    {
      id: "dle_gsf",
      name: "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
      scope: "ORGANIZATION_LEVEL",
      entity_subtype: "parent_company",
      ticker: "HOTEL",
      country: "Mexico",
      jurisdiction: "Mexico",
      domain: "gsf-hotels.com",
      aliases: [
        { alias: "Grupo Hotelero Santa Fe", alias_type: "TRADE_NAME", state: "VERIFIED_ALIAS" },
        { alias: "GSF", alias_type: "ABBREVIATION", state: "VERIFIED_ALIAS" },
        { alias: "BMV: HOTEL", alias_type: "TICKER_ALIAS", state: "VERIFIED_ALIAS" },
        { alias: "HOTEL", alias_type: "TICKER_ALIAS", state: "VERIFIED_ALIAS" },
      ],
    },
    {
      id: "dle_ihvsf",
      name: "Inmobiliaria en Hotelería Vallarta Santa Fe, S. de R.L. de C.V.",
      scope: "PROPERTY_SPECIFIC",
      entity_subtype: "propco",
      parent_id: "dle_gsf",
      country: "Mexico",
      jurisdiction: "Mexico",
      allowed_hotel_ids: ["dhl_kgpv"],
      aliases: [
        { alias: "IHVSF", alias_type: "ABBREVIATION", state: "VERIFIED_ALIAS" },
        { alias: "Inmobiliaria en Hoteleria Vallarta Santa Fe", alias_type: "LEGAL_NAME", state: "VERIFIED_ALIAS" },
      ],
    },
    {
      id: "dle_grupo_chable",
      name: "Grupo Chablé",
      scope: "ORGANIZATION_LEVEL",
      entity_subtype: "parent_company",
      country: "Mexico",
      aliases: [
        { alias: "Grupo Chable", alias_type: "TRADE_NAME", state: "VERIFIED_ALIAS" },
        // Bare "Chablé" is ambiguous with hotel names — do not verify as org alias
        {
          alias: "Chablé Group",
          alias_type: "TRADE_NAME",
          state: "CANDIDATE_ALIAS",
        },
      ],
    },
    {
      id: "dle_chartwell",
      name: "Grupo Chartwell",
      scope: "ORGANIZATION_LEVEL",
      aliases: [
        { alias: "Chartwell", alias_type: "ABBREVIATION" },
        { alias: "Grupo Chartwell de México", alias_type: "LEGAL_NAME" },
      ],
    },
    {
      id: "dle_walton_mx",
      name: "Walton Street Capital Mexico",
      scope: "ORGANIZATION_LEVEL",
      aliases: [
        { alias: "Walton Street Capital México", alias_type: "TRADE_NAME" },
        { alias: "filiales de Walton Street Capital México", alias_type: "OTHER" },
        { alias: "affiliates of Walton Street Capital Mexico", alias_type: "OTHER" },
      ],
    },
    {
      id: "dle_nakheel",
      name: "Nakheel Hotels",
      scope: "ORGANIZATION_LEVEL",
      aliases: [{ alias: "Nakheel", alias_type: "ABBREVIATION" }],
    },
    {
      id: "dle_mhkl",
      name: "MHKL",
      scope: "PROPERTY_SPECIFIC",
      entity_subtype: "propco",
      country: "Mexico",
      allowed_hotel_ids: ["dhl_mahekal"],
      aliases: [
        { alias: "Mahekal Holding", alias_type: "TRADE_NAME", state: "VERIFIED_ALIAS" },
      ],
    },
    {
      id: "dle_fideicomiso_mahekal",
      name: "Fideicomiso Mahekal",
      scope: "PROPERTY_SPECIFIC",
      entity_subtype: "trust",
      country: "Mexico",
      allowed_hotel_ids: ["dhl_mahekal"],
      aliases: [
        { alias: "Fideicomiso Playa Del Carmen", alias_type: "OTHER", state: "CANDIDATE_ALIAS" },
      ],
    },
    {
      id: "dle_hyatt",
      name: "Hyatt",
      scope: "ORGANIZATION_LEVEL",
      aliases: [
        { alias: "Hyatt Hotels", alias_type: "TRADE_NAME" },
        { alias: "Inclusive Collection", alias_type: "TRADE_NAME" },
      ],
    },
    {
      id: "dle_hilton",
      name: "Hilton",
      scope: "ORGANIZATION_LEVEL",
      aliases: [{ alias: "Hilton Worldwide", alias_type: "TRADE_NAME" }],
    },
    {
      id: "dle_breathless_brand",
      name: "Breathless",
      scope: "ORGANIZATION_LEVEL",
      aliases: [
        { alias: "Breathless Puerto Vallarta", alias_type: "PROJECT_NAME" },
        { alias: "Breathless Puerto Vallarta Resort & Spa", alias_type: "PROJECT_NAME" },
      ],
    },
    {
      id: "dle_krystal_grand_brand",
      name: "Krystal Grand",
      scope: "ORGANIZATION_LEVEL",
      aliases: [],
    },
  ],
  people: [
    {
      id: "dlp_luis_avelar",
      name: "Luis Avelar",
      scope: "UNKNOWN_SCOPE",
      aliases: [],
    },
    {
      id: "dlp_norma_garcia",
      name: "Norma Garcia",
      scope: "UNKNOWN_SCOPE",
      aliases: [{ alias: "Norma García", alias_type: "OTHER" }],
    },
  ],
});
