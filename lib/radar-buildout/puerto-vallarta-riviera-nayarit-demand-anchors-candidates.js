/**
 * Puerto Vallarta / Riviera Nayarit demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyPuertoVallartaRivieraNayaritGovernanceDefaults,
  PUERTO_VALLARTA_RIVIERA_NAYARIT_SUBMARKETS,
} from "./puerto-vallarta-riviera-nayarit-demand-anchor-governance.js";

import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyPuertoVallartaRivieraNayaritGovernanceDefaults);

export const PUERTO_VALLARTA_RIVIERA_NAYARIT_CANDIDATES = [
  pt({ name: "Puerto Vallarta International Airport Corridor", pointType: "Future Growth Node", city: "Puerto Vallarta", submarket: "Airport Corridor", latitude: 20.6801, longitude: -105.2544, sourceReference: "https://www.aeropuertosgap.com.mx/", manuallyVerified: true }),
  pt({ name: "Malecón Puerto Vallarta Waterfront", pointType: "Beach / Waterfront", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6113, longitude: -105.2303, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta", manuallyVerified: true }),
  pt({ name: "Los Muertos Beach Hotel Zone", pointType: "Beach / Waterfront", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.5978, longitude: -105.2389, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Zona Romántica Dining and Nightlife", pointType: "Entertainment District", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.5989, longitude: -105.2412, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Playa de los Muertos Pier Los Muertos", pointType: "Tourist Attraction", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.5967, longitude: -105.2378, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Parroquia de Nuestra Señora de Guadalupe", pointType: "Tourist Attraction", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6089, longitude: -105.2345, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Isla Río Cuale Artisan Market", pointType: "Entertainment District", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6034, longitude: -105.2367, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Conchas Chinas Beach Resort Cove", pointType: "Beach / Waterfront", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.5845, longitude: -105.2512, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Hotel Zone North Puerto Vallarta", pointType: "Beach / Waterfront", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6234, longitude: -105.2289, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Puerto Vallarta Cruise Ship Terminal", pointType: "Mixed-Use Development", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6145, longitude: -105.2456, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta", manuallyVerified: true }),
  pt({ name: "Marina Vallarta Resort and Yacht Harbor", pointType: "Beach / Waterfront", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6653, longitude: -105.2419, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta", manuallyVerified: true }),
  pt({ name: "Marina Vallarta Golf Course", pointType: "Sports Venue", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6712, longitude: -105.2489, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Marina Vallarta Boardwalk Restaurants", pointType: "Entertainment District", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6678, longitude: -105.2434, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Marina Vallarta Luxury Hotel Strip", pointType: "Mixed-Use Development", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6634, longitude: -105.2467, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Nuevo Vallarta Master-Planned Resort Zone", pointType: "Mixed-Use Development", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.6989, longitude: -105.2717, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Vidanta Nuevo Vallarta Entertainment Complex", pointType: "Entertainment District", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.7012, longitude: -105.2689, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Paradise Village Marina Nuevo Vallarta", pointType: "Beach / Waterfront", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.6934, longitude: -105.2745, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Flamingos Golf Club Nuevo Vallarta", pointType: "Sports Venue", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.7056, longitude: -105.2634, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Mayan Palace Nuevo Vallarta Resort", pointType: "Mixed-Use Development", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.6967, longitude: -105.2698, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Bucerías Beach Town Riviera Nayarit", pointType: "Beach / Waterfront", city: "Bucerías", submarket: "Riviera Nayarit North Coast", latitude: 20.7512, longitude: -105.3345, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "La Cruz de Huanacaxtle Marina", pointType: "Beach / Waterfront", city: "La Cruz de Huanacaxtle", submarket: "Riviera Nayarit North Coast", latitude: 20.7534, longitude: -105.3789, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Nuevo Nayarit Convention Center", pointType: "Convention Center", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.7089, longitude: -105.2612, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Riviera Nayarit North Coast Resort Corridor", pointType: "Beach / Waterfront", city: "Compostela", submarket: "Riviera Nayarit North Coast", latitude: 21.1234, longitude: -105.3012, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Chacala Beach Village", pointType: "Tourist Attraction", city: "Chacala", submarket: "Riviera Nayarit North Coast", latitude: 21.1456, longitude: -105.2234, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "San Blas Historic Port Town", pointType: "Tourist Attraction", city: "San Blas", submarket: "Riviera Nayarit North Coast", latitude: 21.5434, longitude: -105.2845, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Punta de Mita Luxury Resort Peninsula", pointType: "Beach / Waterfront", city: "Punta de Mita", submarket: "Sayulita / Punta de Mita", latitude: 20.7736, longitude: -105.5286, sourceReference: "https://www.rivieranayarit.com/", manuallyVerified: true }),
  pt({ name: "Four Seasons Punta Mita Resort Zone", pointType: "Mixed-Use Development", city: "Punta de Mita", submarket: "Sayulita / Punta de Mita", latitude: 20.7812, longitude: -105.5345, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "St. Regis Punta Mita Resort", pointType: "Mixed-Use Development", city: "Punta de Mita", submarket: "Sayulita / Punta de Mita", latitude: 20.7789, longitude: -105.5312, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Sayulita Surf and Bohemian Town", pointType: "Entertainment District", city: "Sayulita", submarket: "Sayulita / Punta de Mita", latitude: 20.8694, longitude: -105.4389, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Sayulita Beach Surf Break", pointType: "Beach / Waterfront", city: "Sayulita", submarket: "Sayulita / Punta de Mita", latitude: 20.8712, longitude: -105.4412, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "San Pancho Francisco Beach Town", pointType: "Tourist Attraction", city: "San Francisco", submarket: "Sayulita / Punta de Mita", latitude: 20.9012, longitude: -105.4123, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Litibú Beach Resort Development", pointType: "Mixed-Use Development", city: "Punta de Mita", submarket: "Sayulita / Punta de Mita", latitude: 20.7623, longitude: -105.5189, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Vallarta Botanical Gardens", pointType: "Tourist Attraction", city: "Cabo Corrientes", submarket: "Other", latitude: 20.4234, longitude: -105.3234, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Mismaloya Beach Film Location", pointType: "Beach / Waterfront", city: "Puerto Vallarta", submarket: "Other", latitude: 20.5312, longitude: -105.2912, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Las Caletas Beach Adventure Park", pointType: "Tourist Attraction", city: "Cabo Corrientes", submarket: "Other", latitude: 20.4789, longitude: -105.3123, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Yelapa Waterfall Village", pointType: "Tourist Attraction", city: "Cabo Corrientes", submarket: "Other", latitude: 20.4512, longitude: -105.3789, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Marina La Cruz Yacht Harbor", pointType: "Beach / Waterfront", city: "La Cruz de Huanacaxtle", submarket: "Riviera Nayarit North Coast", latitude: 20.7545, longitude: -105.3812, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "PVR Airport Hotel and Business Corridor", pointType: "Business District", city: "Puerto Vallarta", submarket: "Airport Corridor", latitude: 20.6756, longitude: -105.2489, sourceReference: "https://www.aeropuertosgap.com.mx/" }),
  pt({ name: "Marina Vallarta Convention Hotel Zone", pointType: "Convention Center", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6689, longitude: -105.2456, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Nuevo Vallarta Beach Resort Strip", pointType: "Beach / Waterfront", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.6912, longitude: -105.2789, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Punta Mita Golf Pacifico Course", pointType: "Sports Venue", city: "Punta de Mita", submarket: "Sayulita / Punta de Mita", latitude: 20.7767, longitude: -105.5267, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Bucerías Art Walk and Dining Strip", pointType: "Entertainment District", city: "Bucerías", submarket: "Riviera Nayarit North Coast", latitude: 20.7534, longitude: -105.3312, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Rincón de Guayabitos Beach Resort Town", pointType: "Beach / Waterfront", city: "Compostela", submarket: "Riviera Nayarit North Coast", latitude: 21.0234, longitude: -105.2678, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Hospital CMQ Puerto Vallarta", pointType: "Medical Campus", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6612, longitude: -105.2389, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Marina Vallarta Whale Watching Harbor", pointType: "Tourist Attraction", city: "Puerto Vallarta", submarket: "Marina Vallarta", latitude: 20.6645, longitude: -105.2423, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Puerto Vallarta Romantic Zone Art Walk", pointType: "Entertainment District", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.5998, longitude: -105.2398, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Nuevo Vallarta Vidanta World Entertainment", pointType: "Entertainment District", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.7034, longitude: -105.2667, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Punta de Mita Village Center", pointType: "Entertainment District", city: "Punta de Mita", submarket: "Sayulita / Punta de Mita", latitude: 20.7712, longitude: -105.5234, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Puerto Vallarta Municipal Market", pointType: "Entertainment District", city: "Puerto Vallarta", submarket: "Zona Hotelera / Malecón", latitude: 20.6067, longitude: -105.2323, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Riviera Nayarit Luxury Growth Corridor", pointType: "Future Growth Node", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.7123, longitude: -105.2589, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Playa Destiladeras Public Beach", pointType: "Beach / Waterfront", city: "Nuevo Nayarit", submarket: "Nuevo Vallarta", latitude: 20.7234, longitude: -105.2512, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "Puerto Vallarta South Shore Hidden Beaches", pointType: "Beach / Waterfront", city: "Puerto Vallarta", submarket: "Other", latitude: 20.5123, longitude: -105.3012, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/puerto-vallarta" }),
  pt({ name: "Sayulita Plaza Principal", pointType: "Entertainment District", city: "Sayulita", submarket: "Sayulita / Punta de Mita", latitude: 20.8689, longitude: -105.4378, sourceReference: "https://www.rivieranayarit.com/" }),
  pt({ name: "PVR Airport Cargo Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Puerto Vallarta", submarket: "Airport Corridor", latitude: 20.6834, longitude: -105.2612, sourceReference: "https://www.aeropuertosgap.com.mx/" }),
];

export function getPuertoVallartaRivieraNayaritCandidates() {
  return PUERTO_VALLARTA_RIVIERA_NAYARIT_CANDIDATES;
}

export { PUERTO_VALLARTA_RIVIERA_NAYARIT_SUBMARKETS };
