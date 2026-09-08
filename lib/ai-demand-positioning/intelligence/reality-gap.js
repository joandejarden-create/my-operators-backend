/**
 * AI Demand Positioning — AI Reality Gap.
 * Compares what the property actually has vs what AI recognizes.
 */

import { formatAdpPercent, roundAdpPercent } from "../format-percent.js";

const ATTRIBUTE_DEFINITIONS = {
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
  bogota: { label: "Bogotá Location", priority: "high" },
  bogota_norte: { label: "Bogotá Norte Location", priority: "high" },
  faranda_collection: { label: "Faranda Collection Brand", priority: "high" },
  upscale: { label: "Upscale Positioning", priority: "medium" },
  urban_hotel: { label: "Urban Hotel", priority: "medium" },
};

export function computeRealityGap(observations, propertyProfile) {
  const relevantObs = observations.filter((o) => o.mentioned);
  const totalMentioned = relevantObs.length;

  const attributeRecognition = {};
  for (const obs of relevantObs) {
    for (const attr of obs.attributesRecognized || []) {
      attributeRecognition[attr] = (attributeRecognition[attr] || 0) + 1;
    }
  }

  const propertyAttributes = propertyProfile.attributes || [];
  const gaps = [];
  const recognized = [];

  for (const attr of propertyAttributes) {
    const def = ATTRIBUTE_DEFINITIONS[attr];
    if (!def) continue;
    const recognitionCount = attributeRecognition[attr] || 0;
    const recognitionRate = totalMentioned > 0 ? recognitionCount / totalMentioned : 0;

    if (recognitionRate >= 0.4) {
      recognized.push({
        attribute: attr,
        label: def.label,
        recognitionRate: roundAdpPercent(recognitionRate * 100),
        count: recognitionCount,
        total: totalMentioned,
      });
    } else {
      gaps.push({
        attribute: attr,
        label: def.label,
        priority: def.priority,
        recognitionRate: roundAdpPercent(recognitionRate * 100),
        count: recognitionCount,
        total: totalMentioned,
        severity: recognitionRate < 0.1 ? "HIGH" : recognitionRate < 0.25 ? "MEDIUM" : "LOW",
      });
    }
  }

  gaps.sort((a, b) => {
    const sevOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
    return (sevOrder[a.severity] || 2) - (sevOrder[b.severity] || 2);
  });

  const totalAttributes = propertyAttributes.filter((a) => ATTRIBUTE_DEFINITIONS[a]).length;
  const gapRate = totalAttributes > 0 ? roundAdpPercent((gaps.length / totalAttributes) * 100) : 0;

  return {
    gapScore: gapRate,
    display: formatAdpPercent(gapRate),
    totalAttributes,
    recognizedCount: recognized.length,
    gapCount: gaps.length,
    recognized,
    gaps,
    totalPropertyMentions: totalMentioned,
  };
}
