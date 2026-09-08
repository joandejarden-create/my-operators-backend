/**
 * CALA six-hotel ADP — standard market packs + property-specific scenarios.
 * RESEARCH ONLY. Counts: 48 standard + 15 property = 63 per hotel.
 */

function S(scenarioId, intent, frame, query) {
  return Object.freeze({ scenarioId, intent, frame, query });
}

/** @param {string} p prefix e.g. std_mexr */
function marketPack(p, L) {
  return Object.freeze([
    S(`${p}_biz_01`, "business", "best_for", `Best luxury hotel in ${L.bizBest} for a business trip`),
    S(`${p}_biz_02`, "business", "recommend", `Recommend an upscale hotel in ${L.market} for corporate meetings`),
    S(`${p}_biz_03`, "business", "where_should", `Where should I stay in ${L.sub} for a week of client meetings?`),
    S(`${p}_biz_04`, "business", "best_for", `Best full-service hotel near ${L.landmark} for business travelers`),
    S(`${p}_biz_05`, "business", "recommend", `Upscale hotel in ${L.sub} with loyalty points and executive amenities`),
    S(`${p}_biz_06`, "business", "compare", `Compare luxury and upper-upscale business hotels in ${L.market}`),
    S(`${p}_biz_07`, "business", "best_for", `Best hotel in ${L.sub} for executives visiting offices nearby`),
    S(`${p}_biz_08`, "business", "where_should", `Where to stay in ${L.market} for meetings in ${L.sub}?`),
    S(`${p}_biz_09`, "business", "recommend", `Hotel in ${L.sub} with strong WiFi, workspace, and executive amenities`),
    S(`${p}_biz_10`, "business", "best_for", `Best upscale hotel in ${L.region} for a multi-day corporate engagement`),

    S(`${p}_lei_01`, "leisure", "best_for", `Best upscale hotel in ${L.sub} for a weekend city break`),
    S(`${p}_lei_02`, "leisure", "where_should", `Where should I stay in ${L.market} for shopping and dining?`),
    S(`${p}_lei_03`, "leisure", "recommend", `Recommend a luxury hotel in ${L.market} with a pool and great views`),
    S(`${p}_lei_04`, "leisure", "best_for", `Best hotel near ${L.leisureNear} for leisure travelers`),
    S(`${p}_lei_05`, "leisure", "compare", `Compare upscale hotels in ${L.sub} for a 3-night leisure trip`),
    S(`${p}_lei_06`, "leisure", "recommend", `Upscale ${L.market} hotel walkable to restaurants and nightlife`),
    S(`${p}_lei_07`, "leisure", "where_should", `Where to stay in ${L.market} for a relaxing stay in ${L.sub}?`),
    S(`${p}_lei_08`, "leisure", "best_for", `Best luxury hotel in ${L.sub} for first-time visitors`),

    S(`${p}_cpl_01`, "couples", "best_for", `Best romantic hotel in ${L.sub} for a couples weekend`),
    S(`${p}_cpl_02`, "couples", "recommend", `Recommend a luxury ${L.market} hotel with spa for an anniversary`),
    S(`${p}_cpl_03`, "couples", "where_should", `Where should we stay in ${L.market} for a romantic getaway with fine dining?`),
    S(`${p}_cpl_04`, "couples", "best_for", `Best hotel in ${L.sub} with spa or rooftop dining for couples`),
    S(`${p}_cpl_05`, "couples", "recommend", `Upscale hotel in ${L.sub} for a proposal or anniversary overnight`),
    S(`${p}_cpl_06`, "couples", "compare", `Compare romantic luxury hotels in ${L.market}`),
    S(`${p}_cpl_07`, "couples", "best_for", `Best hotel in ${L.sub} with views and spa for couples`),

    S(`${p}_grp_01`, "group_meeting", "best_for", `Best hotel in ${L.sub} for a corporate retreat`),
    S(`${p}_grp_02`, "group_meeting", "recommend", `Recommend a ${L.market} hotel for a team offsite of 40 people`),
    S(`${p}_grp_03`, "group_meeting", "where_should", `Where should I host an executive leadership meeting in ${L.sub}?`),
    S(`${p}_grp_04`, "group_meeting", "best_for", `Best hotel with meeting space near ${L.landmark} in ${L.market}`),
    S(`${p}_grp_05`, "group_meeting", "recommend", `Luxury hotel in ${L.sub} for a board meeting and overnight stays`),
    S(`${p}_grp_06`, "group_meeting", "best_for", `Best venue in ${L.market} for a company event of 100-200 guests`),
    S(`${p}_grp_07`, "group_meeting", "where_should", `Where to hold a sales kickoff in ${L.sub} with upscale lodging?`),
    S(`${p}_grp_08`, "group_meeting", "compare", `Compare meeting hotels in ${L.market} for a mid-size corporate event`),

    S(`${p}_fam_01`, "family", "best_for", `Best upscale family-friendly hotel in ${L.sub} with a pool`),
    S(`${p}_fam_02`, "family", "recommend", `Recommend a ${L.market} hotel for a family visiting for a weekend`),
    S(`${p}_fam_03`, "family", "where_should", `Where should a family stay near ${L.leisureNear}?`),
    S(`${p}_fam_04`, "family", "best_for", `Best hotel in ${L.sub} for a multigenerational family trip`),
    S(`${p}_fam_05`, "family", "compare", `Compare family-friendly upscale hotels in ${L.market}`),

    S(`${p}_cel_01`, "celebration", "best_for", `Best hotel in ${L.sub} for a birthday celebration dinner and overnight`),
    S(`${p}_cel_02`, "celebration", "recommend", `Recommend a luxury ${L.market} venue for a rehearsal dinner or small wedding`),
    S(`${p}_cel_03`, "celebration", "where_should", `Where to host an engagement party in ${L.sub} with hotel rooms for guests?`),
    S(`${p}_cel_04`, "celebration", "best_for", `Best upscale hotel in ${L.market} for a milestone celebration`),
    S(`${p}_cel_05`, "celebration", "recommend", `Hotel in ${L.sub} for a wedding weekend lodging and events`),

    S(`${p}_wel_01`, "wellness", "best_for", `Best spa hotel in ${L.sub} for a wellness weekend`),
    S(`${p}_wel_02`, "wellness", "recommend", `Recommend a ${L.market} hotel with spa, pool, and fitness center`),
    S(`${p}_wel_03`, "wellness", "where_should", `Where to stay in ${L.sub} for a quiet spa-focused overnight?`),

    S(`${p}_adv_01`, "adventure", "best_for", `Best hotel in ${L.sub} as a base for exploring ${L.market}`),
    S(`${p}_adv_02`, "adventure", "recommend", `Upscale hotel in ${L.sub} for dining, nightlife, and local discovery`),
  ]);
}

export const MEXICO_CITY_REFORMA_SCENARIOS = marketPack("std_mexr", {
  market: "Mexico City",
  sub: "Paseo de la Reforma",
  bizBest: "Mexico City Reforma",
  landmark: "Ángel de la Independencia",
  leisureNear: "Chapultepec and Polanco",
  region: "CDMX",
});

export const CAP_CANA_SCENARIOS = marketPack("std_capc", {
  market: "Cap Cana",
  sub: "Cap Cana / Punta Cana",
  bizBest: "Cap Cana",
  landmark: "Cap Cana Marina",
  leisureNear: "Juanillo Beach and Cap Cana Marina",
  region: "Punta Cana",
});

export const SANTO_DOMINGO_SCENARIOS = marketPack("std_sdq", {
  market: "Santo Domingo",
  sub: "Piantini / Naco",
  bizBest: "Santo Domingo Piantini",
  landmark: "Blue Mall",
  leisureNear: "Blue Mall and Zona Colonial",
  region: "Distrito Nacional",
});

export const CARTAGENA_BOCAGRANDE_SCENARIOS = marketPack("std_ctg", {
  market: "Cartagena",
  sub: "Bocagrande",
  bizBest: "Cartagena Bocagrande",
  landmark: "Bocagrande Beach",
  leisureNear: "Bocagrande and the Walled City",
  region: "Bolívar",
});

export const BOGOTA_NORTE_SCENARIOS = marketPack("std_bog", {
  market: "Bogotá",
  sub: "Bogotá Norte",
  bizBest: "Bogotá Norte",
  landmark: "Parque de la 93",
  leisureNear: "Usaquén and Zona T",
  region: "Cundinamarca",
});

export const ST_REGIS_MEXICO_CITY_SCENARIOS = Object.freeze([
  S("prop_srmx_01", "business", "best_for", "St. Regis Mexico City on Paseo de la Reforma for a business trip"),
  S("prop_srmx_02", "business", "recommend", "The St. Regis Mexico City with butler service and Bonvoy points"),
  S("prop_srmx_03", "leisure", "where_should", "Where to stay at The St. Regis Mexico City for Reforma shopping and dining?"),
  S("prop_srmx_04", "couples", "best_for", "Romantic St. Regis Mexico City with King Cole Bar and spa"),
  S("prop_srmx_05", "celebration", "best_for", "St. Regis Mexico City for a milestone celebration overnight"),
  S("prop_srmx_06", "group_meeting", "recommend", "The St. Regis Mexico City for executive meetings and overnight stays"),
  S("prop_srmx_07", "leisure", "best_for", "Best St. Regis in Mexico City for first-time luxury visitors"),
  S("prop_srmx_08", "wellness", "recommend", "St. Regis Mexico City spa hotel with full-service amenities"),
  S("prop_srmx_09", "business", "where_should", "Where to stay near Ángel de la Independencia at a St. Regis hotel?"),
  S("prop_srmx_10", "group_meeting", "best_for", "Luxury Reforma hotel for corporate board meetings"),
  S("prop_srmx_11", "leisure", "recommend", "Hotel with fine dining on Paseo de la Reforma Mexico City"),
  S("prop_srmx_12", "celebration", "recommend", "St. Regis CDMX for anniversary dinner and overnight lodging"),
  S("prop_srmx_13", "couples", "recommend", "Couples anniversary stay at The St. Regis Mexico City"),
  S("prop_srmx_14", "group_meeting", "where_should", "Where to host VIP guests at St. Regis Mexico City?"),
  S("prop_srmx_15", "adventure", "recommend", "St. Regis Mexico City as a base for Polanco and Chapultepec"),
]);

export const ST_REGIS_CAP_CANA_SCENARIOS = Object.freeze([
  S("prop_srcc_01", "leisure", "best_for", "St. Regis Cap Cana Resort for a luxury beach vacation"),
  S("prop_srcc_02", "couples", "recommend", "The St. Regis Cap Cana with butler service and private beach"),
  S("prop_srcc_03", "leisure", "where_should", "Where to stay at The St. Regis Cap Cana Resort near Juanillo Beach?"),
  S("prop_srcc_04", "family", "best_for", "Family luxury stay at St. Regis Cap Cana with pool and beach"),
  S("prop_srcc_05", "celebration", "best_for", "St. Regis Cap Cana for a honeymoon or anniversary"),
  S("prop_srcc_06", "group_meeting", "recommend", "The St. Regis Cap Cana for a small executive retreat"),
  S("prop_srcc_07", "wellness", "best_for", "Best spa resort at Cap Cana St. Regis"),
  S("prop_srcc_08", "leisure", "recommend", "Cap Cana luxury resort with swim-out suites and infinity pool"),
  S("prop_srcc_09", "couples", "where_should", "Where should couples stay at St. Regis Cap Cana?"),
  S("prop_srcc_10", "adventure", "best_for", "St. Regis Cap Cana as a base for marina and golf"),
  S("prop_srcc_11", "business", "recommend", "Luxury Cap Cana hotel for a short executive beach trip"),
  S("prop_srcc_12", "celebration", "recommend", "St. Regis Cap Cana wedding weekend lodging"),
  S("prop_srcc_13", "family", "recommend", "Multigenerational Cap Cana stay at St. Regis Resort"),
  S("prop_srcc_14", "leisure", "compare", "Compare St. Regis Cap Cana with other Cap Cana luxury resorts"),
  S("prop_srcc_15", "wellness", "recommend", "Oceanfront spa weekend at The St. Regis Cap Cana Resort"),
]);

export const JW_MARRIOTT_SANTO_DOMINGO_SCENARIOS = Object.freeze([
  S("prop_jwsd_01", "business", "best_for", "JW Marriott Hotel Santo Domingo for a business trip"),
  S("prop_jwsd_02", "business", "recommend", "JW Marriott Santo Domingo at Blue Mall with Bonvoy points"),
  S("prop_jwsd_03", "leisure", "where_should", "Where to stay at JW Marriott Santo Domingo for Blue Mall shopping?"),
  S("prop_jwsd_04", "couples", "best_for", "Romantic JW Marriott Santo Domingo with rooftop terrace"),
  S("prop_jwsd_05", "celebration", "best_for", "JW Marriott Santo Domingo for a celebration overnight"),
  S("prop_jwsd_06", "group_meeting", "recommend", "JW Marriott Santo Domingo for corporate meetings"),
  S("prop_jwsd_07", "leisure", "best_for", "Best JW Marriott in Santo Domingo for leisure travelers"),
  S("prop_jwsd_08", "wellness", "recommend", "JW Marriott Santo Domingo with infinity pool and fitness"),
  S("prop_jwsd_09", "business", "where_should", "Where to stay in Piantini at a JW Marriott hotel?"),
  S("prop_jwsd_10", "group_meeting", "best_for", "Luxury Piantini hotel for board meetings and overnight"),
  S("prop_jwsd_11", "leisure", "recommend", "Hotel with Blue Mall access in Santo Domingo"),
  S("prop_jwsd_12", "celebration", "recommend", "JW Marriott Blue Mall for birthday dinner and lodging"),
  S("prop_jwsd_13", "couples", "recommend", "Couples anniversary at JW Marriott Hotel Santo Domingo"),
  S("prop_jwsd_14", "group_meeting", "where_should", "Where to host VIP guests at JW Marriott Santo Domingo?"),
  S("prop_jwsd_15", "adventure", "recommend", "JW Marriott Santo Domingo as a base for Zona Colonial"),
]);

export const RADISSON_SANTO_DOMINGO_SCENARIOS = Object.freeze([
  S("prop_rsdq_01", "business", "best_for", "Radisson Hotel Santo Domingo for a business trip"),
  S("prop_rsdq_02", "business", "recommend", "Radisson Santo Domingo in Naco with meeting space"),
  S("prop_rsdq_03", "leisure", "where_should", "Where to stay at Radisson Hotel Santo Domingo near Tiradentes?"),
  S("prop_rsdq_04", "couples", "best_for", "Radisson Santo Domingo overnight for a couples city break"),
  S("prop_rsdq_05", "celebration", "best_for", "Radisson Hotel Santo Domingo for a small celebration"),
  S("prop_rsdq_06", "group_meeting", "recommend", "Radisson Santo Domingo for a team offsite"),
  S("prop_rsdq_07", "leisure", "best_for", "Best Radisson in Santo Domingo for leisure travelers"),
  S("prop_rsdq_08", "wellness", "recommend", "Radisson Santo Domingo with outdoor pool and fitness center"),
  S("prop_rsdq_09", "business", "where_should", "Where to stay in Naco at a Radisson hotel?"),
  S("prop_rsdq_10", "group_meeting", "best_for", "Upper-upscale Santo Domingo hotel for corporate meetings"),
  S("prop_rsdq_11", "leisure", "recommend", "Hotel on Tiradentes corridor Santo Domingo"),
  S("prop_rsdq_12", "celebration", "recommend", "Radisson Naco for birthday dinner and overnight lodging"),
  S("prop_rsdq_13", "family", "recommend", "Family weekend at Radisson Hotel Santo Domingo"),
  S("prop_rsdq_14", "group_meeting", "where_should", "Where to host meetings at Radisson Santo Domingo?"),
  S("prop_rsdq_15", "adventure", "recommend", "Radisson Santo Domingo as a base for city exploration"),
]);

export const HOTEL_CARIBE_FARANDA_GRAND_SCENARIOS = Object.freeze([
  S("prop_hcfg_01", "leisure", "best_for", "Hotel Caribe by Faranda Grand for a Cartagena beach vacation"),
  S("prop_hcfg_02", "leisure", "recommend", "Hotel Caribe Bocagrande member of Radisson Individuals"),
  S("prop_hcfg_03", "family", "where_should", "Where to stay at Hotel Caribe Cartagena with kids and a pool?"),
  S("prop_hcfg_04", "couples", "best_for", "Romantic Hotel Caribe Faranda Grand near Bocagrande Beach"),
  S("prop_hcfg_05", "celebration", "best_for", "Hotel Caribe Cartagena for a celebration weekend"),
  S("prop_hcfg_06", "group_meeting", "recommend", "Hotel Caribe by Faranda Grand for meetings and overnight"),
  S("prop_hcfg_07", "leisure", "best_for", "Best Bocagrande hotel near the historic walled city"),
  S("prop_hcfg_08", "wellness", "recommend", "Hotel Caribe Cartagena with oceanfront pool and fitness"),
  S("prop_hcfg_09", "business", "where_should", "Where to stay in Bocagrande at Hotel Caribe Faranda Grand?"),
  S("prop_hcfg_10", "group_meeting", "best_for", "Cartagena Bocagrande hotel for a team retreat"),
  S("prop_hcfg_11", "leisure", "recommend", "Oceanfront Faranda Grand hotel in Cartagena"),
  S("prop_hcfg_12", "celebration", "recommend", "Hotel Caribe for wedding guest lodging in Cartagena"),
  S("prop_hcfg_13", "family", "recommend", "Multigenerational Cartagena stay at Hotel Caribe"),
  S("prop_hcfg_14", "adventure", "recommend", "Hotel Caribe as a base for Ciudad Amurallada"),
  S("prop_hcfg_15", "couples", "recommend", "Couples anniversary at Hotel Caribe by Faranda Grand"),
]);

export const FARANDA_COLLECTION_BOGOTA_SCENARIOS = Object.freeze([
  S("prop_fcbg_01", "business", "best_for", "Faranda Collection Bogotá for a business trip"),
  S("prop_fcbg_02", "business", "recommend", "Faranda Collection Bogotá Radisson Individuals with meeting space"),
  S("prop_fcbg_03", "leisure", "where_should", "Where to stay at Faranda Collection Bogotá near Usaquén?"),
  S("prop_fcbg_04", "couples", "best_for", "Faranda Collection Bogotá for a couples city break"),
  S("prop_fcbg_05", "celebration", "best_for", "Faranda Collection Bogotá for a small celebration overnight"),
  S("prop_fcbg_06", "group_meeting", "recommend", "Faranda Collection Bogotá for a team offsite"),
  S("prop_fcbg_07", "leisure", "best_for", "Best Faranda Collection hotel in Bogotá Norte"),
  S("prop_fcbg_08", "wellness", "recommend", "Faranda Collection Bogotá with fitness center"),
  S("prop_fcbg_09", "business", "where_should", "Where to stay in Bogotá Norte at Faranda Collection?"),
  S("prop_fcbg_10", "group_meeting", "best_for", "Upscale Bogotá Norte hotel for corporate meetings"),
  S("prop_fcbg_11", "leisure", "recommend", "Hotel near Parque de la 93 and Zona T Bogotá"),
  S("prop_fcbg_12", "celebration", "recommend", "Faranda Collection Bogotá for birthday dinner and lodging"),
  S("prop_fcbg_13", "family", "recommend", "Family weekend at Faranda Collection Bogotá"),
  S("prop_fcbg_14", "group_meeting", "where_should", "Where to host meetings at Faranda Collection Bogotá?"),
  S("prop_fcbg_15", "adventure", "recommend", "Faranda Collection Bogotá as a base for Usaquén discovery"),
]);

export const CALA_SIX_PROPERTY_SCENARIO_MAP = Object.freeze({
  adp_st_regis_mexico_city: ST_REGIS_MEXICO_CITY_SCENARIOS,
  adp_st_regis_cap_cana: ST_REGIS_CAP_CANA_SCENARIOS,
  adp_jw_marriott_santo_domingo: JW_MARRIOTT_SANTO_DOMINGO_SCENARIOS,
  adp_radisson_santo_domingo: RADISSON_SANTO_DOMINGO_SCENARIOS,
  adp_hotel_caribe_faranda_grand: HOTEL_CARIBE_FARANDA_GRAND_SCENARIOS,
  adp_faranda_collection_bogota: FARANDA_COLLECTION_BOGOTA_SCENARIOS,
});

export const CALA_SIX_MARKET_KEYS = Object.freeze([
  "mexico_city_reforma",
  "cap_cana",
  "santo_domingo",
  "cartagena_bocagrande",
  "bogota_norte",
]);

export function getCalaSixStandardScenarios(market) {
  switch (market) {
    case "mexico_city_reforma":
      return MEXICO_CITY_REFORMA_SCENARIOS;
    case "cap_cana":
      return CAP_CANA_SCENARIOS;
    case "santo_domingo":
      return SANTO_DOMINGO_SCENARIOS;
    case "cartagena_bocagrande":
      return CARTAGENA_BOCAGRANDE_SCENARIOS;
    case "bogota_norte":
      return BOGOTA_NORTE_SCENARIOS;
    default:
      return null;
  }
}

export function getCalaSixPropertyScenarios(propertyId) {
  return CALA_SIX_PROPERTY_SCENARIO_MAP[propertyId] || null;
}
