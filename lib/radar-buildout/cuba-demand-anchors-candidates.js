/**
 * Cuba Countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyCubaGovernanceDefaults,
  CUBA_SUBMARKETS,
} from "./cuba-demand-anchor-governance.js";

const COUNTRY = "Cuba";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyCubaGovernanceDefaults);

export const CUBA_CANDIDATES = [
  pt({ name: "José Martí International Airport Corridor", pointType: "Future Growth Node", city: "Havana", submarket: "Havana", latitude: 22.9892, longitude: -82.4091, sourceReference: "https://www.havana-airport.com/", manuallyVerified: true }),
  pt({ name: "Port of Havana Cruise Terminal", pointType: "Mixed-Use Development", city: "Havana", submarket: "Havana", latitude: 23.1389, longitude: -82.3472, sourceReference: "https://www.cubatravel.cu/en", manuallyVerified: true }),
  pt({ name: "Old Havana UNESCO World Heritage Core", pointType: "Tourist Attraction", city: "Havana", submarket: "Havana", latitude: 23.1355, longitude: -82.3503, sourceReference: "https://whc.unesco.org/en/list/204" }),
  pt({ name: "Malecón de La Habana Waterfront", pointType: "Beach / Waterfront", city: "Havana", submarket: "Havana", latitude: 23.1408, longitude: -82.4089, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Plaza de la Revolución Civic Precinct", pointType: "Government / Civic Center", city: "Havana", submarket: "Havana", latitude: 23.1247, longitude: -82.3861, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "El Capitolio Nacional", pointType: "Tourist Attraction", city: "Havana", submarket: "Havana", latitude: 23.1353, longitude: -82.3597, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Gran Teatro de La Habana Alicia Alonso", pointType: "Entertainment District", city: "Havana", submarket: "Havana", latitude: 23.1378, longitude: -82.3589, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Fábrica de Arte Cubano", pointType: "Entertainment District", city: "Havana", submarket: "Havana", latitude: 23.1289, longitude: -82.4012, sourceReference: "https://www.fac.cu/" }),
  pt({ name: "Miramar Business and Embassy Corridor", pointType: "Business District", city: "Havana", submarket: "Havana", latitude: 23.1123, longitude: -82.4456, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Playa del Este Resort Strip", pointType: "Beach / Waterfront", city: "Havana", submarket: "Havana", latitude: 23.1567, longitude: -82.3012, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Morro-Cabaña Historic Fortress Complex", pointType: "Tourist Attraction", city: "Havana", submarket: "Havana", latitude: 23.1561, longitude: -82.3512, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "University of Havana Campus", pointType: "University / College", city: "Havana", submarket: "Havana", latitude: 23.1378, longitude: -82.3845, sourceReference: "https://www.uh.cu/" }),
  pt({ name: "Instituto Superior de Arte", pointType: "University / College", city: "Havana", submarket: "Havana", latitude: 23.1012, longitude: -82.4234, sourceReference: "https://www.isa.cult.cu/" }),
  pt({ name: "Hermanos Ameijeiras Hospital", pointType: "Medical Campus", city: "Havana", submarket: "Havana", latitude: 23.1289, longitude: -82.3789, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Juan Gualberto Gómez International Airport Corridor", pointType: "Future Growth Node", city: "Varadero", submarket: "Varadero", latitude: 23.0344, longitude: -81.4353, sourceReference: "https://www.varaderointernationalairport.com/", manuallyVerified: true }),
  pt({ name: "Varadero Beach Resort Coastline", pointType: "Beach / Waterfront", city: "Varadero", submarket: "Varadero", latitude: 23.1397, longitude: -81.2861, sourceReference: "https://www.cubatravel.cu/en", manuallyVerified: true }),
  pt({ name: "Parque Josone Varadero", pointType: "Tourist Attraction", city: "Varadero", submarket: "Varadero", latitude: 23.1312, longitude: -81.2789, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Plaza América Convention Zone", pointType: "Convention Center", city: "Varadero", submarket: "Varadero", latitude: 23.1278, longitude: -81.2912, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Melia Varadero Resort Corridor", pointType: "Mixed-Use Development", city: "Varadero", submarket: "Varadero", latitude: 23.1456, longitude: -81.2712, sourceReference: "https://www.melia.com/" }),
  pt({ name: "Marina Dársena de Varadero", pointType: "Beach / Waterfront", city: "Varadero", submarket: "Varadero", latitude: 23.1334, longitude: -81.2834, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Trinidad Plaza Mayor UNESCO Core", pointType: "Tourist Attraction", city: "Trinidad", submarket: "Trinidad", latitude: 21.8022, longitude: -79.9831, sourceReference: "https://whc.unesco.org/en/list/460" }),
  pt({ name: "Valle de los Ingenios Heritage Corridor", pointType: "Tourist Attraction", city: "Trinidad", submarket: "Trinidad", latitude: 21.8234, longitude: -79.9456, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Topes de Collantes Nature Park", pointType: "Tourist Attraction", city: "Trinidad", submarket: "Trinidad", latitude: 21.9123, longitude: -80.0123, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Playa Ancón Beach", pointType: "Beach / Waterfront", city: "Trinidad", submarket: "Trinidad", latitude: 21.8567, longitude: -79.9789, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Antonio Maceo International Airport Corridor", pointType: "Future Growth Node", city: "Santiago de Cuba", submarket: "Santiago de Cuba", latitude: 19.9698, longitude: -75.8354, sourceReference: "https://www.cubatravel.cu/en", manuallyVerified: true }),
  pt({ name: "Parque Céspedes Santiago Civic Center", pointType: "Government / Civic Center", city: "Santiago de Cuba", submarket: "Santiago de Cuba", latitude: 20.0211, longitude: -75.8263, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Castillo del Morro Santiago", pointType: "Tourist Attraction", city: "Santiago de Cuba", submarket: "Santiago de Cuba", latitude: 19.9712, longitude: -75.8712, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Santiago Carnival District", pointType: "Entertainment District", city: "Santiago de Cuba", submarket: "Santiago de Cuba", latitude: 20.0234, longitude: -75.8312, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Santa Ifigenia Cemetery — José Martí Mausoleum", pointType: "Tourist Attraction", city: "Santiago de Cuba", submarket: "Santiago de Cuba", latitude: 20.0289, longitude: -75.8234, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Viñales Valley UNESCO Landscape", pointType: "Tourist Attraction", city: "Viñales", submarket: "Other", latitude: 22.6167, longitude: -83.7167, sourceReference: "https://whc.unesco.org/en/list/840" }),
  pt({ name: "Cayo Coco Resort Archipelago", pointType: "Mixed-Use Development", city: "Cayo Coco", submarket: "Other", latitude: 22.5123, longitude: -78.5012, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Jardines del Rey Airport Corridor", pointType: "Future Growth Node", city: "Cayo Coco", submarket: "Other", latitude: 22.4612, longitude: -78.3289, sourceReference: "https://www.cubatravel.cu/en", manuallyVerified: true }),
  pt({ name: "Cienfuegos Punta Gorda Waterfront", pointType: "Beach / Waterfront", city: "Cienfuegos", submarket: "Other", latitude: 22.1289, longitude: -80.4512, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Holguín City Business District", pointType: "Business District", city: "Holguín", submarket: "Other", latitude: 20.8872, longitude: -76.2631, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Guardalavaca Beach Resort Coast", pointType: "Beach / Waterfront", city: "Guardalavaca", submarket: "Other", latitude: 21.1234, longitude: -75.8234, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Frank País Airport Holguín Corridor", pointType: "Future Growth Node", city: "Holguín", submarket: "Other", latitude: 20.7856, longitude: -76.3151, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Santa Clara Che Guevara Mausoleum", pointType: "Tourist Attraction", city: "Santa Clara", submarket: "Other", latitude: 22.4078, longitude: -79.9645, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Mariel Special Development Zone", pointType: "Industrial / Logistics Zone", city: "Mariel", submarket: "Other", latitude: 22.9878, longitude: -82.7512, sourceReference: "https://www.zedmariel.cu/" }),
  pt({ name: "Zapata Peninsula Eco-Tourism Corridor", pointType: "Tourist Attraction", city: "Playa Larga", submarket: "Other", latitude: 22.3012, longitude: -81.1234, sourceReference: "https://www.cubatravel.cu/en" }),
  pt({ name: "Baracoa First City Heritage District", pointType: "Tourist Attraction", city: "Baracoa", submarket: "Other", latitude: 20.3489, longitude: -74.4967, sourceReference: "https://www.cubatravel.cu/en" }),
];

export function getCubaCandidates() {
  return CUBA_CANDIDATES;
}

export { CUBA_SUBMARKETS };
