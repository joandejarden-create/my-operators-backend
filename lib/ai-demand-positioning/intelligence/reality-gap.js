/**
 * AI Demand Positioning — AI Reality Gap.
 * Compares what the property actually has vs what AI recognizes.
 */

import { formatAdpPercent, roundAdpPercent } from "../format-percent.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";
import {
  ATTRIBUTE_MEASUREMENT_STATE_V1,
  collapseSynonymAttributes,
} from "../property-attributes/adp-hotel-attribute-intelligence-standard-v1.js";
import {
  ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1,
  resolveCanonicalAttributeSet,
  countAliasUnionRecognition,
  perAliasRecognitionRates,
  getEquivalenceGroupForAttribute,
} from "../property-attributes/adp-reality-gap-canonical-attribute-dedup-v1.js";
import { isAttributeEligibleForCoreAiTesting } from "../execution/response-parser.js";
import {
  HISTORICAL_IMMUTABLE_METHODOLOGY_ID,
  CONDITIONAL_APPROVED_METHODOLOGY_ID,
} from "./reality-gap-founder-approval-v2.js";

/** Recognition thresholds (subject-mentioned corpus). Immutable classification contract. */
export const REALITY_GAP_RECOGNITION_THRESHOLDS_V1 = Object.freeze({
  recognizedMinRate: 0.4,
  partialMinRate: 0.15,
  partialMaxRateExclusive: 0.4,
  missedMaxRateExclusive: 0.15,
  grain: "SUBJECT_MENTIONED_CORPUS",
});

const GENERIC_ATTR_KEYS = new Set([
  "full_service",
  "luxury",
  "upscale",
  "upper_upscale",
  "urban_hotel",
  "resort",
  "urban_luxury",
]);

export const ATTRIBUTE_DEFINITIONS = {
  // Waterstone Resort & Marina
  waterfront: { label: "Waterfront Location", priority: "high" },
  marina: { label: "Marina & Boat Access", priority: "high" },
  watersports: { label: "Water Sports", priority: "medium" },
  intracoastal_waterway: { label: "Intracoastal Waterway", priority: "high" },
  walking_distance_beach: { label: "Walking Distance to Beach", priority: "medium" },
  near_mizner_park: { label: "Near Mizner Park", priority: "medium" },
  heated_outdoor_pool: { label: "Heated Outdoor Pool", priority: "medium" },
  pet_friendly: { label: "Pet-Friendly", priority: "low" },
  soft_brand: { label: "Soft Brand / Independence", priority: "medium" },
  hilton_honors: { label: "Hilton Honors Integration", priority: "medium" },
  meeting_space: { label: "Meeting & Event Space", priority: "high" },
  event_space_outdoor: { label: "Outdoor Event Space", priority: "medium" },
  ballroom: { label: "Ballroom", priority: "medium" },
  boutique_feel: { label: "Boutique Character", priority: "medium" },
  panoramic_views: { label: "Panoramic Views", priority: "medium" },
  private_balconies: { label: "Private Balconies", priority: "medium" },
  ev_charging: { label: "EV Charging", priority: "low" },

  // Renaissance New York Times Square
  times_square_location: { label: "Times Square Location", priority: "high" },
  broadway_theater_district: { label: "Broadway / Theater District", priority: "high" },
  midtown_manhattan: { label: "Midtown Manhattan", priority: "medium" },
  rooftop_bar: { label: "Rooftop Bar", priority: "high" },
  times_square_views: { label: "Times Square Views", priority: "medium" },
  business_center: { label: "Business Center", priority: "low" },
  marriott_bonvoy: { label: "Marriott Bonvoy Loyalty", priority: "high" },
  walking_distance_broadway: { label: "Walking Distance to Broadway", priority: "medium" },
  near_central_park: { label: "Near Central Park", priority: "medium" },
  near_rockefeller_center: { label: "Near Rockefeller Center", priority: "medium" },
  urban_lifestyle: { label: "Urban Lifestyle Hotel", priority: "medium" },
  design_forward: { label: "Design-Forward Interiors", priority: "medium" },
  full_service: { label: "Full-Service Hotel", priority: "medium" },
  concierge: { label: "Concierge Service", priority: "low" },

  // Cambridge Beaches Resort & Spa
  private_beaches: { label: "Private Beaches", priority: "high" },
  five_private_coves: { label: "Five Private Coves", priority: "high" },
  cottage_style: { label: "Cottage-Colony Style", priority: "high" },
  oceanfront: { label: "Oceanfront", priority: "high" },
  full_service_spa: { label: "Full-Service Spa", priority: "high" },
  heated_infinity_pool: { label: "Heated Infinity Pool", priority: "medium" },
  tennis_courts: { label: "Tennis Courts", priority: "low" },
  kayaking: { label: "Kayaking", priority: "medium" },
  snorkeling: { label: "Snorkeling", priority: "medium" },
  paddleboard: { label: "Paddleboarding", priority: "medium" },
  scuba_diving: { label: "Scuba Diving", priority: "medium" },
  wedding_venue: { label: "Wedding Venue", priority: "high" },
  honeymoon_destination: { label: "Honeymoon Destination", priority: "high" },
  adults_only: { label: "Adults-Only", priority: "high" },
  historic_property: { label: "Historic Property", priority: "medium" },
  bermuda_heritage: { label: "Bermuda Heritage", priority: "medium" },
  all_inclusive_option: { label: "All-Inclusive Option", priority: "medium" },
  island_resort: { label: "Island Resort", priority: "medium" },

  // JW Marriott / Westin Monterrey Valle (shared market pack)
  monterrey_valle: { label: "Monterrey Valle Location", priority: "high" },
  san_pedro_garza_garcia: { label: "San Pedro Garza García", priority: "high" },
  valle_del_campestre: { label: "Valle del Campestre", priority: "medium" },
  punto_valle: { label: "Punto Valle Adjacency", priority: "high" },
  luxury: { label: "Luxury Positioning", priority: "high" },
  upper_upscale: { label: "Upper-Upscale Positioning", priority: "medium" },
  jw_marriott: { label: "JW Marriott Brand", priority: "high" },
  westin: { label: "Westin Brand", priority: "high" },
  spa: { label: "On-Site Spa", priority: "high" },
  heavenly_spa: { label: "Heavenly Spa", priority: "high" },
  outdoor_pool: { label: "Outdoor Pool", priority: "medium" },
  infinity_pool: { label: "Infinity Pool", priority: "high" },
  fitness_center: { label: "Fitness Center", priority: "medium" },
  fitness_center_24hr: { label: "24-Hour Fitness Center", priority: "medium" },
  executive_lounge: { label: "Executive Lounge", priority: "high" },
  westin_club: { label: "Westin Club", priority: "medium" },
  rooftop_dining: { label: "Rooftop Dining", priority: "high" },
  business_district: { label: "Business District Location", priority: "high" },
  financial_center: { label: "Financial Center Location", priority: "medium" },
  aimbridge_managed: { label: "Aimbridge Managed", priority: "medium" },

  // CALA six-hotel cohort
  paseo_de_la_reforma: { label: "Paseo de la Reforma Location", priority: "high" },
  mexico_city: { label: "Mexico City Location", priority: "high" },
  st_regis: { label: "St. Regis Brand", priority: "high" },
  butler_service: { label: "Butler Service", priority: "high" },
  fine_dining: { label: "Fine Dining", priority: "high" },
  king_cole_bar: { label: "King Cole Bar", priority: "medium" },
  urban_luxury: { label: "Urban Luxury", priority: "medium" },
  five_diamond: { label: "Five Diamond Positioning", priority: "medium" },
  cap_cana: { label: "Cap Cana Location", priority: "high" },
  punta_cana: { label: "Punta Cana Location", priority: "high" },
  beachfront: { label: "Beachfront", priority: "high" },
  private_beach: { label: "Private Beach", priority: "high" },
  swim_out: { label: "Swim-Out Suites", priority: "medium" },
  resort: { label: "Resort Experience", priority: "medium" },
  santo_domingo: { label: "Santo Domingo Location", priority: "high" },
  piantini: { label: "Piantini Location", priority: "high" },
  blue_mall: { label: "Blue Mall Adjacency", priority: "high" },
  naco: { label: "Naco Location", priority: "high" },
  tiradentes: { label: "Tiradentes Corridor", priority: "medium" },
  radisson: { label: "Radisson Brand", priority: "high" },
  choice_hotels: { label: "Choice Hotels Distribution", priority: "medium" },
  suite_inventory: { label: "Suite Inventory", priority: "medium" },
  cartagena: { label: "Cartagena Location", priority: "high" },
  bocagrande: { label: "Bocagrande Location", priority: "high" },
  faranda_grand: { label: "Faranda Grand Brand", priority: "high" },
  radisson_individuals: { label: "Radisson Individuals Membership", priority: "high" },
  beach_access: { label: "Beach Access", priority: "high" },
  family_friendly: { label: "Family-Friendly", priority: "medium" },
  historic_city_access: { label: "Historic City Access", priority: "medium" },
  zona_colonial: { label: "Zona Colonial / Ciudad Colonial", priority: "high" },
  vignette_collection: { label: "Vignette Collection Affiliation", priority: "high" },
  ihg_one_rewards: { label: "IHG One Rewards Loyalty", priority: "high" },
  bogota: { label: "Bogotá Location", priority: "high" },
  bogota_norte: { label: "Bogotá Norte Location", priority: "high" },
  faranda_collection: { label: "Faranda Collection Brand", priority: "high" },
  upscale: { label: "Upscale Positioning", priority: "medium" },
  urban_hotel: { label: "Urban Hotel", priority: "medium" },

  // Bethesda / Montgomery County (WASBT)
  bethesda_md_location: { label: "Bethesda, Maryland Location", priority: "high" },
  pooks_hill: { label: "Pooks Hill Location", priority: "high" },
  montgomery_county: { label: "Montgomery County Location", priority: "medium" },
  marriott_hotels: { label: "Marriott Hotels Brand", priority: "high" },
  m_club: { label: "M Club / Concierge Level", priority: "high" },
  coopers_mill: { label: "Cooper's Mill Restaurant", priority: "high" },
  peloton: { label: "Peloton Fitness", priority: "medium" },
  nih_access: { label: "NIH Access", priority: "high" },
  dc_metro_access: { label: "Washington DC Metro Access", priority: "high" },

  // Shared full-service / wellness (Munich + portable)
  wellness: { label: "Wellness Positioning", priority: "medium" },
  large_ballroom: { label: "Large Ballroom", priority: "high" },

  // Portable brand / service keys used across Marriott portfolio profiles
  marriott: { label: "Marriott Brand Family", priority: "medium" },
  airport: { label: "Airport Location / Access", priority: "high" },

  // Charlotte Uptown / City Center
  charlotte_uptown: { label: "Uptown Charlotte / City Center Location", priority: "high" },

  // Portable / previously unregistered profile keys (ADP_HOTEL_ATTRIBUTE_INTELLIGENCE_STANDARD_V1)
  lifestyle: { label: "Lifestyle Hotel Positioning", priority: "medium", category: "A_IDENTITY_POSITIONING" },
  meetings: { label: "Meetings Capability", priority: "high", category: "D_MEETINGS_GROUPS" },
  restaurant: { label: "On-Site Restaurant", priority: "high", category: "E_FOOD_BEVERAGE" },
  on_site_restaurant: { label: "On-Site Restaurant", priority: "high", category: "E_FOOD_BEVERAGE" },
  wellness_fitness: { label: "Wellness & Fitness", priority: "medium", category: "F_WELLNESS_LEISURE" },
  meeting_rooms: { label: "Meeting Rooms", priority: "high", category: "D_MEETINGS_GROUPS" },
  meeting_space_small: { label: "Intimate Meeting Space", priority: "medium", category: "D_MEETINGS_GROUPS" },
  private_dining: { label: "Private Dining", priority: "high", category: "E_FOOD_BEVERAGE" },
  connecting_rooms: { label: "Connecting Rooms", priority: "medium", category: "C_GUEST_ACCOMMODATIONS" },
  room_service: { label: "In-Room Dining", priority: "medium", category: "E_FOOD_BEVERAGE" },
  valet_parking: { label: "Valet Parking", priority: "low", category: "G_BUSINESS_GUEST_SERVICES" },
  airport_shuttle: { label: "Airport Shuttle", priority: "medium", category: "G_BUSINESS_GUEST_SERVICES" },
  park_and_fly: { label: "Park-and-Fly", priority: "medium", category: "G_BUSINESS_GUEST_SERVICES" },
  digital_key: { label: "Digital Key", priority: "low", category: "G_BUSINESS_GUEST_SERVICES" },
  leisure_resort: { label: "Leisure Resort Positioning", priority: "medium", category: "A_IDENTITY_POSITIONING" },
  golf_resort: { label: "Golf Resort", priority: "high", category: "F_WELLNESS_LEISURE" },
  golf_access: { label: "Golf Access", priority: "high", category: "F_WELLNESS_LEISURE" },
  sports_golf_travel: { label: "Sports / Golf Travel", priority: "medium", category: "H_PROPERTY_DIFFERENTIATION" },
  wedding_celebrations: { label: "Wedding & Celebrations", priority: "high", category: "D_MEETINGS_GROUPS" },
  luxury_collection: { label: "Luxury Collection Affiliation", priority: "high", category: "A_IDENTITY_POSITIONING" },
  curio_collection: { label: "Curio Collection by Hilton", priority: "high", category: "A_IDENTITY_POSITIONING" },
  sheraton: { label: "Sheraton Brand", priority: "high", category: "A_IDENTITY_POSITIONING" },
  hyatt_world_of_hyatt: { label: "World of Hyatt Loyalty", priority: "high", category: "A_IDENTITY_POSITIONING" },
  wyndham_rewards: { label: "Wyndham Rewards Loyalty", priority: "high", category: "A_IDENTITY_POSITIONING" },
  historic_building: { label: "Historic Building", priority: "high", category: "H_PROPERTY_DIFFERENTIATION" },
  historic_art_deco: { label: "Historic Art Deco Architecture", priority: "high", category: "H_PROPERTY_DIFFERENTIATION" },
  nrhp_listed: { label: "National Register Listing", priority: "medium", category: "H_PROPERTY_DIFFERENTIATION" },
  heritage_castle: { label: "Heritage Castle Property", priority: "high", category: "H_PROPERTY_DIFFERENTIATION" },
  sky_lobby: { label: "Sky Lobby", priority: "medium", category: "H_PROPERTY_DIFFERENTIATION" },
  city_views: { label: "City Views", priority: "medium", category: "H_PROPERTY_DIFFERENTIATION" },
  large_spacious_rooms: { label: "Large / Spacious Rooms", priority: "medium", category: "C_GUEST_ACCOMMODATIONS" },
  terrace_rooms: { label: "Terrace Rooms", priority: "medium", category: "C_GUEST_ACCOMMODATIONS" },
  recently_renovated: { label: "Recently Renovated", priority: "medium", category: "H_PROPERTY_DIFFERENTIATION" },
  art_forward: { label: "Art-Forward Design", priority: "medium", category: "H_PROPERTY_DIFFERENTIATION" },
  speakeasy: { label: "Speakeasy / Signature Bar", priority: "high", category: "E_FOOD_BEVERAGE" },
  social_event_lounge: { label: "Social Event Lounge", priority: "medium", category: "D_MEETINGS_GROUPS" },
  walkable_downtown: { label: "Walkable Downtown", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  neighborhood_navigator: { label: "Neighborhood Concierge / Navigator", priority: "low", category: "G_BUSINESS_GUEST_SERVICES" },
  near_penn_station: { label: "Near Penn Station", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  near_empire_state: { label: "Near Empire State Building", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  near_madison_square_garden: { label: "Near Madison Square Garden", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  near_bryant_park: { label: "Near Bryant Park", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  near_grand_central: { label: "Near Grand Central", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  near_javits_center: { label: "Near Javits Center", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  near_convention_center: { label: "Near Convention Center", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  near_t_mobile_center: { label: "Near T-Mobile Center", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  near_kauffman_center: { label: "Near Kauffman Center", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  downtown_kansas_city: { label: "Downtown Kansas City Location", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  power_and_light_district: { label: "Power & Light District", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  streetcar_access: { label: "Streetcar Access", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  noho_location: { label: "NoHo Location", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  lower_manhattan: { label: "Lower Manhattan", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  soho_adjacent: { label: "SoHo Adjacent", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  greenwich_village_access: { label: "Greenwich Village Access", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  lafayette_street: { label: "Lafayette Street Location", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  near_nyu: { label: "Near NYU", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  near_washington_square: { label: "Near Washington Square", priority: "medium", category: "B_LOCATION_ACCESSIBILITY" },
  lake_boca_raton: { label: "Lake Boca Raton Setting", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  croquet: { label: "Croquet", priority: "low", category: "F_WELLNESS_LEISURE" },
  mangrove_bay: { label: "Mangrove Bay Setting", priority: "medium", category: "H_PROPERTY_DIFFERENTIATION" },
  "3_diamond": { label: "AAA Three Diamond", priority: "low", category: "A_IDENTITY_POSITIONING" },

  // Nobu / Marylebone differentiation (research-backed keys)
  marylebone_location: { label: "Marylebone / Portman Square Location", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  nobu_brand: { label: "Nobu Hotels Brand", priority: "high", category: "A_IDENTITY_POSITIONING" },
  nobu_restaurant: { label: "Nobu Restaurant", priority: "high", category: "E_FOOD_BEVERAGE" },
  west_end_access: { label: "West End Access", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  japanese_hospitality: { label: "Japanese Hospitality Positioning", priority: "high", category: "H_PROPERTY_DIFFERENTIATION" },
  ballroom_capacity: { label: "Large Ballroom / Event Capacity", priority: "high", category: "D_MEETINGS_GROUPS" },

  // YOTEL differentiation
  yotel_brand: { label: "YOTEL Brand", priority: "high", category: "A_IDENTITY_POSITIONING" },
  geneva_lake_location: { label: "Geneva Lake / La Côte Location", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  airport_adjacent: { label: "Airport-Adjacent Location", priority: "high", category: "B_LOCATION_ACCESSIBILITY" },
  cabin_rooms: { label: "Cabin-Style Guestrooms", priority: "medium", category: "C_GUEST_ACCOMMODATIONS" },
  select_service: { label: "Select-Service Positioning", priority: "medium", category: "A_IDENTITY_POSITIONING" },

  // UMusic Hotel Madrid differentiation (official site 2026-10-30)
  umusic_hotels_brand: { label: "UMusic Hotels Brand", priority: "high", category: "A_IDENTITY_POSITIONING" },
  madrid_puerta_del_sol_location: {
    label: "Puerta del Sol / Madrid Centro Location",
    priority: "high",
    category: "B_LOCATION_ACCESSIBILITY",
  },
  teatro_albeniz_on_site: {
    label: "On-Site Teatro Albéniz",
    priority: "high",
    category: "H_PROPERTY_DIFFERENTIATION",
  },
  music_culture_entertainment: {
    label: "Music & Culture Entertainment Hotel",
    priority: "high",
    category: "H_PROPERTY_DIFFERENTIATION",
  },
  piano_bar: { label: "Piano Bar", priority: "high", category: "E_FOOD_BEVERAGE" },
  seasonal_rooftop_pool: {
    label: "Seasonal Rooftop Pool",
    priority: "medium",
    category: "F_WELLNESS_LEISURE",
  },
};

/**
 * Stamp measured/measurementStatus onto legacy realityGap blocks that already
 * contain totals + recognized/gaps lists but predate the MEASURED contract.
 * Does not invent attributes or recognition rates.
 */
export function normalizeLegacyRealityGap(realityGap, propertyProfile = null) {
  if (!realityGap || typeof realityGap !== "object") return realityGap;
  if (realityGap.measured === true && realityGap.measurementStatus === "MEASURED") {
    return realityGap;
  }
  if (
    realityGap.measurementStatus === "NOT_MEASURED_NO_GOVERNED_ATTRIBUTES" ||
    realityGap.measured === false
  ) {
    return realityGap;
  }

  const profileAttrCount = Array.isArray(propertyProfile?.attributes)
    ? propertyProfile.attributes.filter((a) => ATTRIBUTE_DEFINITIONS[a]).length
    : 0;
  const listedAttrCount =
    (realityGap.recognized?.length || 0) + (realityGap.gaps?.length || 0);
  const totalAttributes = Number(
    realityGap.totalAttributes != null
      ? realityGap.totalAttributes
      : listedAttrCount > 0
        ? listedAttrCount
        : profileAttrCount
  );
  const hasLists =
    (Array.isArray(realityGap.recognized) && realityGap.recognized.length > 0) ||
    (Array.isArray(realityGap.gaps) && realityGap.gaps.length > 0) ||
    Number.isFinite(Number(realityGap.gapScore)) ||
    Number.isFinite(Number(realityGap.recognizedCount)) ||
    Number.isFinite(Number(realityGap.gapCount));

  if (totalAttributes > 0 && hasLists) {
    return {
      ...realityGap,
      measured: true,
      measurementStatus: "MEASURED",
      totalAttributes,
      recognizedCount: realityGap.recognizedCount ?? realityGap.recognized?.length ?? 0,
      gapCount: realityGap.gapCount ?? realityGap.gaps?.length ?? 0,
      display:
        realityGap.display ||
        (realityGap.gapScore != null ? formatAdpPercent(realityGap.gapScore) : realityGap.display),
      allAttributesWellRepresented:
        realityGap.allAttributesWellRepresented ??
        ((realityGap.gaps?.length || 0) === 0 && (realityGap.recognized?.length || 0) > 0),
    };
  }

  if (profileAttrCount > 0 && !hasLists) {
    // Profile has governed attrs but payload never computed — leave for recompute path
    return realityGap;
  }

  return realityGap;
}

/**
 * @param {object[]} observations
 * @param {object} propertyProfile
 * @param {{ methodologyId?: string }} [opts]
 *   methodologyId — RG_BINARY_GAP_V1 (historical default) or RG_WEIGHTED_PARTIAL_V2.
 *   Classifications always use the same 15%/40% thresholds; only composite gapScore differs.
 */
export function computeRealityGap(observations, propertyProfile, opts = {}) {
  const methodologyId =
    opts.methodologyId === CONDITIONAL_APPROVED_METHODOLOGY_ID
      ? CONDITIONAL_APPROVED_METHODOLOGY_ID
      : HISTORICAL_IMMUTABLE_METHODOLOGY_ID;

  // Prefer governed subject mention (includes alias recovery) over raw mentioned flag.
  const relevantObs = observations.filter(
    (o) => getGovernedSubjectMentioned(o) === true || o.mentioned === true
  );
  const totalMentioned = relevantObs.length;

  // Per-key recognition (diagnostics only). Customer scores use alias observation-union below.
  const attributeRecognition = {};
  for (const obs of relevantObs) {
    for (const attr of obs.attributesRecognized || []) {
      attributeRecognition[attr] = (attributeRecognition[attr] || 0) + 1;
    }
  }

  const propertyAttributes = propertyProfile.attributes || [];
  const governed = propertyAttributes.filter((a) => ATTRIBUTE_DEFINITIONS[a]);
  const resolved = resolveCanonicalAttributeSet(governed, {
    labelFor: (id) => ATTRIBUTE_DEFINITIONS[id]?.label || id,
  });
  // Prefer registry collapse; fall back to legacy synonym collapse if registry empty.
  const uniqueGoverned =
    resolved.canonicalIds.length > 0
      ? resolved.canonicalIds
      : collapseSynonymAttributes(governed);
  const resolutionByDisplay = Object.fromEntries(
    (resolved.resolutions || []).map((r) => [r.displayAttributeId, r])
  );
  const gaps = [];
  const recognized = [];
  const attributeClassifications = [];
  const aliasReconciliations = [];

  const observationCorpus = Array.isArray(observations) ? observations : [];
  const hasObservationCorpus = observationCorpus.length > 0;
  const eligibleDenom = totalMentioned; // SUBJECT_MENTIONED_CORPUS grain (documented)
  const thr = REALITY_GAP_RECOGNITION_THRESHOLDS_V1;

  for (const attr of uniqueGoverned) {
    const def = ATTRIBUTE_DEFINITIONS[attr];
    if (!def) continue;
    const resolution = resolutionByDisplay[attr];
    const group = getEquivalenceGroupForAttribute(attr);
    const aliases =
      resolution?.aliasesPresent?.length > 0
        ? resolution.aliasesPresent
        : group?.aliases?.includes(attr)
          ? group.aliases.filter((a) => governed.includes(a))
          : [attr];
    const union = countAliasUnionRecognition(relevantObs, aliases);
    const recognitionCount = union.count;
    const recognitionRate = eligibleDenom > 0 ? recognitionCount / eligibleDenom : 0;
    const perAlias = perAliasRecognitionRates(relevantObs, aliases, eligibleDenom);
    if (aliases.length > 1) {
      aliasReconciliations.push({
        displayAttributeId: attr,
        aliases,
        unionCount: recognitionCount,
        perAlias,
        policy: "OBSERVATION_UNION_ANY_ALIAS",
      });
    }
    // Core eligibility: any alias with a Core detector qualifies the canonical capability.
    const coreTestEligible = aliases.some((a) => isAttributeEligibleForCoreAiTesting(a));

    let measurementState = ATTRIBUTE_MEASUREMENT_STATE_V1.INSUFFICIENT_OBSERVATION_EVIDENCE;
    if (!coreTestEligible) {
      // Verified hotel fact without Core keyword detector — never treat as an AI miss.
      measurementState = ATTRIBUTE_MEASUREMENT_STATE_V1.VERIFIED_ATTRIBUTE_NOT_TESTED;
    } else if (!hasObservationCorpus || eligibleDenom === 0) {
      measurementState = ATTRIBUTE_MEASUREMENT_STATE_V1.INSUFFICIENT_OBSERVATION_EVIDENCE;
    } else if (recognitionRate >= thr.recognizedMinRate) {
      measurementState = ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_RECOGNIZED;
    } else if (recognitionRate >= thr.partialMinRate) {
      measurementState = ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_PARTIALLY_RECOGNIZED;
    } else {
      measurementState = ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_NOT_RECOGNIZED;
    }

    const row = {
      attribute: attr,
      label: def.label,
      priority: def.priority,
      category: def.category || null,
      recognitionRate: roundAdpPercent(recognitionRate * 100),
      count: recognitionCount,
      total: eligibleDenom,
      measurementState,
      coreTestEligible,
      eligibilityGrain: "SUBJECT_MENTIONED_CORPUS",
      isGeneric: GENERIC_ATTR_KEYS.has(attr),
      aliasesCollapsed: resolution?.aliasesCollapsed || [],
      recognitionAggregation: "OBSERVATION_UNION_ANY_ALIAS",
    };
    attributeClassifications.push(row);

    // Untested / insufficient evidence must not become an AI-miss card
    if (
      measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.INSUFFICIENT_OBSERVATION_EVIDENCE ||
      measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.VERIFIED_ATTRIBUTE_NOT_TESTED
    ) {
      continue;
    }

    if (measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_RECOGNIZED) {
      recognized.push({
        attribute: attr,
        label: def.label,
        recognitionRate: row.recognitionRate,
        count: recognitionCount,
        total: eligibleDenom,
        measurementState,
        aliasesCollapsed: resolution?.aliasesCollapsed || [],
      });
    } else {
      gaps.push({
        attribute: attr,
        label: def.label,
        priority: def.priority,
        recognitionRate: row.recognitionRate,
        count: recognitionCount,
        total: eligibleDenom,
        severity: recognitionRate < 0.1 ? "HIGH" : recognitionRate < 0.25 ? "MEDIUM" : "LOW",
        measurementState,
        aliasesCollapsed: resolution?.aliasesCollapsed || [],
      });
    }
  }

  // Prefer differentiated (non-generic) findings in customer display order
  const byDifferentiation = (a, b) => {
    const ag = GENERIC_ATTR_KEYS.has(a.attribute) ? 1 : 0;
    const bg = GENERIC_ATTR_KEYS.has(b.attribute) ? 1 : 0;
    if (ag !== bg) return ag - bg;
    return (b.recognitionRate || 0) - (a.recognitionRate || 0);
  };
  recognized.sort(byDifferentiation);
  gaps.sort((a, b) => {
    const sevOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
    const sev = (sevOrder[a.severity] || 2) - (sevOrder[b.severity] || 2);
    if (sev !== 0) return sev;
    return byDifferentiation(a, b);
  });

  const testedEligible = attributeClassifications.filter(
    (c) =>
      c.measurementState !== ATTRIBUTE_MEASUREMENT_STATE_V1.VERIFIED_ATTRIBUTE_NOT_TESTED &&
      c.measurementState !== ATTRIBUTE_MEASUREMENT_STATE_V1.INSUFFICIENT_OBSERVATION_EVIDENCE &&
      c.measurementState !== ATTRIBUTE_MEASUREMENT_STATE_V1.NOT_APPLICABLE
  );
  const missedCount = testedEligible.filter(
    (c) => c.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_NOT_RECOGNIZED
  ).length;
  const partialCount = testedEligible.filter(
    (c) => c.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_PARTIALLY_RECOGNIZED
  ).length;
  const recognizedCountScored = testedEligible.filter(
    (c) => c.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_RECOGNIZED
  ).length;

  // Gap % denominator = Core-test-eligible attributes that were actually scored (not untested facts).
  const totalAttributes = testedEligible.length > 0 ? testedEligible.length : uniqueGoverned.length;
  const measured = uniqueGoverned.length > 0 && hasObservationCorpus;
  const denom = testedEligible.length;
  const binaryGapV1 =
    measured && denom > 0
      ? roundAdpPercent(((missedCount + partialCount) / denom) * 100)
      : measured
        ? roundAdpPercent(0)
        : null;
  const weightedGapV2 =
    measured && denom > 0
      ? roundAdpPercent(((missedCount + 0.5 * partialCount) / denom) * 100)
      : measured
        ? roundAdpPercent(0)
        : null;
  const gapRate =
    methodologyId === CONDITIONAL_APPROVED_METHODOLOGY_ID ? weightedGapV2 : binaryGapV1;

  let measurementStatus = "MEASURED";
  let display;
  if (totalAttributes === 0) {
    measurementStatus = "NOT_MEASURED_NO_GOVERNED_ATTRIBUTES";
    display = "Not measured — governed property attributes unavailable";
  } else if (!hasObservationCorpus) {
    measurementStatus = "NOT_MEASURED_NO_OBSERVATIONS";
    display =
      "Not measured — governed attributes present but observation archive unavailable";
  } else {
    display = formatAdpPercent(gapRate);
  }

  // Property Reality Coverage = fully recognized / tested (NOT 100 − weightedGap).
  // When partials > 0, 100−gap equals weighted recognition and must not be labeled Coverage.
  const coveragePercent =
    measured && denom > 0 ? roundAdpPercent((recognizedCountScored / denom) * 100) : null;

  return {
    gapScore: measured ? gapRate : null,
    /** Always preserved — immutable historical customer formula. */
    gapScoreBinaryV1: measured ? binaryGapV1 : null,
    /** Always computed — activated for staged when methodologyId is V2. */
    gapScoreWeightedV2: measured ? weightedGapV2 : null,
    /** Fully recognized ÷ tested eligible — authoritative Property Reality Coverage. */
    coveragePercent,
    display,
    measured,
    measurementStatus,
    methodologyId,
    methodologyLabel:
      methodologyId === CONDITIONAL_APPROVED_METHODOLOGY_ID
        ? "Weighted gap (miss + 0.5×partial) — RG_WEIGHTED_PARTIAL_V2"
        : "Binary gap (miss + partial) — RG_BINARY_GAP_V1",
    historicalImmutableMethodologyId: HISTORICAL_IMMUTABLE_METHODOLOGY_ID,
    methodologyNote:
      methodologyId === CONDITIONAL_APPROVED_METHODOLOGY_ID
        ? "Methodology-driven score change vs prior binary gap — not a change in hotel AI visibility."
        : null,
    totalAttributes,
    recognizedCount: recognized.length,
    gapCount: gaps.length,
    missedCount,
    partialCount,
    recognizedCountScored,
    testedEligibleCount: denom,
    recognized,
    gaps,
    attributeClassifications,
    eligibilityGrain: "SUBJECT_MENTIONED_CORPUS",
    recognitionThresholds: REALITY_GAP_RECOGNITION_THRESHOLDS_V1,
    synonymCollapsedFrom: governed.length,
    canonicalDedup: {
      contractVersion: ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1,
      inputGovernedCount: governed.length,
      canonicalCount: uniqueGoverned.length,
      collapsedCount: governed.length - uniqueGoverned.length,
      resolutions: resolved.resolutions || [],
      aliasReconciliations,
    },
    totalPropertyMentions: totalMentioned,
    observationCount: observationCorpus.length,
    // Never imply "all attributes well-represented" when the monitored set is empty.
    allAttributesWellRepresented: measured ? gaps.length === 0 && recognized.length > 0 : false,
    contractVersion:
      methodologyId === CONDITIONAL_APPROVED_METHODOLOGY_ID
        ? "ADP_REALITY_GAP_WEIGHTED_PARTIAL_V2"
        : "ADP_REALITY_GAP_INDEPENDENT_CERTIFICATION_V1",
  };
}
