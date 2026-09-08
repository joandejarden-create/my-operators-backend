/**
 * CALA six-hotel ADP — CORE role overrides + stabilized CORE IDs (RESEARCH ONLY).
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";

const CC = Object.freeze({
  CORE_COMPETITOR: "CORE_COMPETITOR",
  SECONDARY_ALTERNATIVE: "SECONDARY_ALTERNATIVE",
  CONDITIONAL: "CONDITIONAL",
  NON_COMPARABLE: "NON_COMPARABLE",
});

function all(className) {
  return Object.fromEntries(Object.values(TRAVELER_INTENTS).map((i) => [i, className]));
}

function roles(map) {
  const out = {};
  for (const [entityId, byIntent] of Object.entries(map)) {
    out[entityId] = Object.freeze({ ...byIntent });
  }
  return Object.freeze(out);
}

const CORE = CC.CORE_COMPETITOR;
const SEC = CC.SECONDARY_ALTERNATIVE;
const COND = CC.CONDITIONAL;
const NON = CC.NON_COMPARABLE;

function bizLeisureCore(spaOk = true) {
  return {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: CORE,
    [TRAVELER_INTENTS.WELLNESS]: spaOk ? CORE : SEC,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  };
}

function secondaryMostly() {
  return {
    [TRAVELER_INTENTS.BUSINESS]: SEC,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: COND,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: COND,
  };
}

export const CALA_SIX_PROPERTY_IDS = Object.freeze([
  "adp_st_regis_mexico_city",
  "adp_st_regis_cap_cana",
  "adp_jw_marriott_santo_domingo",
  "adp_radisson_santo_domingo",
  "adp_hotel_caribe_faranda_grand",
  "adp_faranda_collection_bogota",
]);

export const ST_REGIS_MEXICO_CITY_ROLE_OVERRIDES = roles({
  four_seasons_mexico_city: bizLeisureCore(true),
  ritz_carlton_mexico_city: bizLeisureCore(true),
  sofitel_mexico_city_reforma: bizLeisureCore(true),
  jw_marriott_mexico_city: bizLeisureCore(true),
  las_alcobas_mexico_city: {
    [TRAVELER_INTENTS.BUSINESS]: SEC,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: CORE,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  hotel_habita_mexico_city: secondaryMostly(),
  intercontinental_presidente_mexico_city: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: SEC,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: CORE,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  marriott_mexico_city_airport: all(NON),
});

export const ST_REGIS_CAP_CANA_ROLE_OVERRIDES = roles({
  eden_roc_cap_cana: bizLeisureCore(true),
  secrets_cap_cana: {
    [TRAVELER_INTENTS.BUSINESS]: COND,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: CORE,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  excellence_punta_cana: {
    [TRAVELER_INTENTS.BUSINESS]: COND,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: SEC,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  tortuga_bay_punta_cana: bizLeisureCore(true),
  hyatt_zilara_cap_cana: {
    [TRAVELER_INTENTS.BUSINESS]: COND,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: NON,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: CORE,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  sanctuary_cap_cana: {
    [TRAVELER_INTENTS.BUSINESS]: COND,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: NON,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: SEC,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  club_med_miches: secondaryMostly(),
});

export const JW_MARRIOTT_SANTO_DOMINGO_ROLE_OVERRIDES = roles({
  radisson_santo_domingo: bizLeisureCore(false),
  intercontinental_real_santo_domingo: bizLeisureCore(false),
  hilton_santo_domingo: bizLeisureCore(false),
  sheraton_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: SEC,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: CORE,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  holiday_inn_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: NON,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: CORE,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  embassy_suites_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: SEC,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  el_embajador_santo_domingo: secondaryMostly(),
});

export const RADISSON_SANTO_DOMINGO_ROLE_OVERRIDES = roles({
  jw_marriott_santo_domingo: bizLeisureCore(true),
  intercontinental_real_santo_domingo: bizLeisureCore(false),
  hilton_santo_domingo: bizLeisureCore(false),
  sheraton_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: SEC,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: CORE,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  holiday_inn_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: NON,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: CORE,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  embassy_suites_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: SEC,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  el_embajador_santo_domingo: secondaryMostly(),
});

export const HOTEL_CARIBE_FARANDA_GRAND_ROLE_OVERRIDES = roles({
  hilton_cartagena: bizLeisureCore(true),
  intercontinental_cartagena: bizLeisureCore(true),
  sofitel_santa_clara_cartagena: {
    [TRAVELER_INTENTS.BUSINESS]: SEC,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: CORE,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  charleston_santa_teresa: {
    [TRAVELER_INTENTS.BUSINESS]: SEC,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: SEC,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  movich_cartagena: bizLeisureCore(false),
  ghl_capilla_del_mar: {
    [TRAVELER_INTENTS.BUSINESS]: SEC,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: SEC,
    [TRAVELER_INTENTS.FAMILY]: CORE,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  hyatt_regency_cartagena: secondaryMostly(),
});

export const FARANDA_COLLECTION_BOGOTA_ROLE_OVERRIDES = roles({
  jw_marriott_bogota: bizLeisureCore(true),
  hilton_bogota: bizLeisureCore(false),
  grand_hyatt_bogota: bizLeisureCore(true),
  bogota_marriott: bizLeisureCore(false),
  nh_collection_teleport_bogota: {
    [TRAVELER_INTENTS.BUSINESS]: CORE,
    [TRAVELER_INTENTS.LEISURE]: SEC,
    [TRAVELER_INTENTS.COUPLES]: SEC,
    [TRAVELER_INTENTS.FAMILY]: SEC,
    [TRAVELER_INTENTS.GROUP_MEETING]: CORE,
    [TRAVELER_INTENTS.WELLNESS]: NON,
    [TRAVELER_INTENTS.ADVENTURE]: COND,
    [TRAVELER_INTENTS.CELEBRATION]: SEC,
  },
  w_bogota: {
    [TRAVELER_INTENTS.BUSINESS]: SEC,
    [TRAVELER_INTENTS.LEISURE]: CORE,
    [TRAVELER_INTENTS.COUPLES]: CORE,
    [TRAVELER_INTENTS.FAMILY]: NON,
    [TRAVELER_INTENTS.GROUP_MEETING]: SEC,
    [TRAVELER_INTENTS.WELLNESS]: CORE,
    [TRAVELER_INTENTS.ADVENTURE]: SEC,
    [TRAVELER_INTENTS.CELEBRATION]: CORE,
  },
  sofia_hotel_bogota: secondaryMostly(),
});

export const CALA_SIX_ROLE_OVERRIDES = Object.freeze({
  adp_st_regis_mexico_city: ST_REGIS_MEXICO_CITY_ROLE_OVERRIDES,
  adp_st_regis_cap_cana: ST_REGIS_CAP_CANA_ROLE_OVERRIDES,
  adp_jw_marriott_santo_domingo: JW_MARRIOTT_SANTO_DOMINGO_ROLE_OVERRIDES,
  adp_radisson_santo_domingo: RADISSON_SANTO_DOMINGO_ROLE_OVERRIDES,
  adp_hotel_caribe_faranda_grand: HOTEL_CARIBE_FARANDA_GRAND_ROLE_OVERRIDES,
  adp_faranda_collection_bogota: FARANDA_COLLECTION_BOGOTA_ROLE_OVERRIDES,
});

export const CALA_SIX_STABILIZED_CORE_IDS = Object.freeze({
  adp_st_regis_mexico_city: {
    [TRAVELER_INTENTS.BUSINESS]: [
      "four_seasons_mexico_city",
      "ritz_carlton_mexico_city",
      "sofitel_mexico_city_reforma",
      "jw_marriott_mexico_city",
    ],
    [TRAVELER_INTENTS.LEISURE]: [
      "four_seasons_mexico_city",
      "ritz_carlton_mexico_city",
      "sofitel_mexico_city_reforma",
      "las_alcobas_mexico_city",
    ],
    [TRAVELER_INTENTS.COUPLES]: [
      "four_seasons_mexico_city",
      "ritz_carlton_mexico_city",
      "las_alcobas_mexico_city",
      "sofitel_mexico_city_reforma",
    ],
    [TRAVELER_INTENTS.FAMILY]: [
      "four_seasons_mexico_city",
      "jw_marriott_mexico_city",
      "ritz_carlton_mexico_city",
      "sofitel_mexico_city_reforma",
    ],
    [TRAVELER_INTENTS.GROUP_MEETING]: [
      "four_seasons_mexico_city",
      "jw_marriott_mexico_city",
      "ritz_carlton_mexico_city",
      "intercontinental_presidente_mexico_city",
    ],
    [TRAVELER_INTENTS.WELLNESS]: [
      "four_seasons_mexico_city",
      "ritz_carlton_mexico_city",
      "las_alcobas_mexico_city",
      "sofitel_mexico_city_reforma",
    ],
    [TRAVELER_INTENTS.ADVENTURE]: [
      "four_seasons_mexico_city",
      "sofitel_mexico_city_reforma",
      "las_alcobas_mexico_city",
      "jw_marriott_mexico_city",
    ],
    [TRAVELER_INTENTS.CELEBRATION]: [
      "four_seasons_mexico_city",
      "ritz_carlton_mexico_city",
      "las_alcobas_mexico_city",
      "sofitel_mexico_city_reforma",
    ],
  },
  adp_st_regis_cap_cana: {
    [TRAVELER_INTENTS.BUSINESS]: [
      "eden_roc_cap_cana",
      "tortuga_bay_punta_cana",
      "hyatt_zilara_cap_cana",
      "sanctuary_cap_cana",
    ],
    [TRAVELER_INTENTS.LEISURE]: [
      "eden_roc_cap_cana",
      "secrets_cap_cana",
      "tortuga_bay_punta_cana",
      "excellence_punta_cana",
    ],
    [TRAVELER_INTENTS.COUPLES]: [
      "eden_roc_cap_cana",
      "secrets_cap_cana",
      "tortuga_bay_punta_cana",
      "hyatt_zilara_cap_cana",
    ],
    [TRAVELER_INTENTS.FAMILY]: [
      "secrets_cap_cana",
      "excellence_punta_cana",
      "eden_roc_cap_cana",
      "tortuga_bay_punta_cana",
    ],
    [TRAVELER_INTENTS.GROUP_MEETING]: [
      "eden_roc_cap_cana",
      "tortuga_bay_punta_cana",
      "secrets_cap_cana",
      "hyatt_zilara_cap_cana",
    ],
    [TRAVELER_INTENTS.WELLNESS]: [
      "eden_roc_cap_cana",
      "secrets_cap_cana",
      "tortuga_bay_punta_cana",
      "hyatt_zilara_cap_cana",
    ],
    [TRAVELER_INTENTS.ADVENTURE]: [
      "eden_roc_cap_cana",
      "secrets_cap_cana",
      "excellence_punta_cana",
      "tortuga_bay_punta_cana",
    ],
    [TRAVELER_INTENTS.CELEBRATION]: [
      "eden_roc_cap_cana",
      "secrets_cap_cana",
      "tortuga_bay_punta_cana",
      "hyatt_zilara_cap_cana",
    ],
  },
  adp_jw_marriott_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: [
      "radisson_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "holiday_inn_santo_domingo",
    ],
    [TRAVELER_INTENTS.LEISURE]: [
      "radisson_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.COUPLES]: [
      "radisson_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.FAMILY]: [
      "radisson_santo_domingo",
      "hilton_santo_domingo",
      "holiday_inn_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.GROUP_MEETING]: [
      "radisson_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "sheraton_santo_domingo",
    ],
    [TRAVELER_INTENTS.WELLNESS]: [
      "hilton_santo_domingo",
      "intercontinental_real_santo_domingo",
      "radisson_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.ADVENTURE]: [
      "radisson_santo_domingo",
      "hilton_santo_domingo",
      "intercontinental_real_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.CELEBRATION]: [
      "radisson_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
  },
  adp_radisson_santo_domingo: {
    [TRAVELER_INTENTS.BUSINESS]: [
      "jw_marriott_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "holiday_inn_santo_domingo",
    ],
    [TRAVELER_INTENTS.LEISURE]: [
      "jw_marriott_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.COUPLES]: [
      "jw_marriott_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.FAMILY]: [
      "jw_marriott_santo_domingo",
      "hilton_santo_domingo",
      "holiday_inn_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.GROUP_MEETING]: [
      "jw_marriott_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "sheraton_santo_domingo",
    ],
    [TRAVELER_INTENTS.WELLNESS]: [
      "jw_marriott_santo_domingo",
      "hilton_santo_domingo",
      "intercontinental_real_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.ADVENTURE]: [
      "jw_marriott_santo_domingo",
      "hilton_santo_domingo",
      "intercontinental_real_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
    [TRAVELER_INTENTS.CELEBRATION]: [
      "jw_marriott_santo_domingo",
      "intercontinental_real_santo_domingo",
      "hilton_santo_domingo",
      "embassy_suites_santo_domingo",
    ],
  },
  adp_hotel_caribe_faranda_grand: {
    [TRAVELER_INTENTS.BUSINESS]: [
      "hilton_cartagena",
      "intercontinental_cartagena",
      "movich_cartagena",
      "ghl_capilla_del_mar",
    ],
    [TRAVELER_INTENTS.LEISURE]: [
      "hilton_cartagena",
      "intercontinental_cartagena",
      "sofitel_santa_clara_cartagena",
      "charleston_santa_teresa",
    ],
    [TRAVELER_INTENTS.COUPLES]: [
      "sofitel_santa_clara_cartagena",
      "charleston_santa_teresa",
      "hilton_cartagena",
      "intercontinental_cartagena",
    ],
    [TRAVELER_INTENTS.FAMILY]: [
      "hilton_cartagena",
      "intercontinental_cartagena",
      "ghl_capilla_del_mar",
      "movich_cartagena",
    ],
    [TRAVELER_INTENTS.GROUP_MEETING]: [
      "hilton_cartagena",
      "intercontinental_cartagena",
      "movich_cartagena",
      "ghl_capilla_del_mar",
    ],
    [TRAVELER_INTENTS.WELLNESS]: [
      "hilton_cartagena",
      "intercontinental_cartagena",
      "sofitel_santa_clara_cartagena",
      "charleston_santa_teresa",
    ],
    [TRAVELER_INTENTS.ADVENTURE]: [
      "hilton_cartagena",
      "intercontinental_cartagena",
      "sofitel_santa_clara_cartagena",
      "movich_cartagena",
    ],
    [TRAVELER_INTENTS.CELEBRATION]: [
      "sofitel_santa_clara_cartagena",
      "charleston_santa_teresa",
      "hilton_cartagena",
      "intercontinental_cartagena",
    ],
  },
  adp_faranda_collection_bogota: {
    [TRAVELER_INTENTS.BUSINESS]: [
      "jw_marriott_bogota",
      "hilton_bogota",
      "bogota_marriott",
      "nh_collection_teleport_bogota",
    ],
    [TRAVELER_INTENTS.LEISURE]: [
      "jw_marriott_bogota",
      "grand_hyatt_bogota",
      "w_bogota",
      "hilton_bogota",
    ],
    [TRAVELER_INTENTS.COUPLES]: [
      "w_bogota",
      "grand_hyatt_bogota",
      "jw_marriott_bogota",
      "hilton_bogota",
    ],
    [TRAVELER_INTENTS.FAMILY]: [
      "jw_marriott_bogota",
      "hilton_bogota",
      "bogota_marriott",
      "grand_hyatt_bogota",
    ],
    [TRAVELER_INTENTS.GROUP_MEETING]: [
      "jw_marriott_bogota",
      "hilton_bogota",
      "grand_hyatt_bogota",
      "nh_collection_teleport_bogota",
    ],
    [TRAVELER_INTENTS.WELLNESS]: [
      "jw_marriott_bogota",
      "grand_hyatt_bogota",
      "w_bogota",
      "hilton_bogota",
    ],
    [TRAVELER_INTENTS.ADVENTURE]: [
      "w_bogota",
      "jw_marriott_bogota",
      "grand_hyatt_bogota",
      "hilton_bogota",
    ],
    [TRAVELER_INTENTS.CELEBRATION]: [
      "w_bogota",
      "grand_hyatt_bogota",
      "jw_marriott_bogota",
      "hilton_bogota",
    ],
  },
});
