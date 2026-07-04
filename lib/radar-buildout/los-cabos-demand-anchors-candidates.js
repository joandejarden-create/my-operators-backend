/**
 * Los Cabos demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyLosCabosGovernanceDefaults,
  LOS_CABOS_SUBMARKETS,
} from "./los-cabos-demand-anchor-governance.js";

import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyLosCabosGovernanceDefaults);

export const LOS_CABOS_CANDIDATES = [
  pt({ name: "Los Cabos International Airport Corridor", pointType: "Future Growth Node", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.1518, longitude: -109.7211, sourceReference: "https://www.asur.com.mx/", manuallyVerified: true }),
  pt({ name: "San José del Cabo Historic Art District", pointType: "Entertainment District", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0594, longitude: -109.6972, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Puerto Los Cabos Marina Village", pointType: "Mixed-Use Development", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0634, longitude: -109.6845, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Estero San José Ecological Reserve", pointType: "Tourist Attraction", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0512, longitude: -109.7012, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo Real Golf Club San José", pointType: "Sports Venue", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0345, longitude: -109.7123, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Palmilla Beach Club Zone", pointType: "Beach / Waterfront", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0289, longitude: -109.7234, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Marina", pointType: "Beach / Waterfront", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8797, longitude: -109.9083, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", manuallyVerified: true }),
  pt({ name: "Medano Beach Resort Strip", pointType: "Beach / Waterfront", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8903, longitude: -109.9056, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", manuallyVerified: true }),
  pt({ name: "El Arco de Cabo San Lucas Land's End", pointType: "Tourist Attraction", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8767, longitude: -109.9142, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", manuallyVerified: true }),
  pt({ name: "Cabo San Lucas Downtown Nightlife Corridor", pointType: "Entertainment District", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8889, longitude: -109.9167, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Puerto Paraíso Shopping and Marina", pointType: "Entertainment District", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8812, longitude: -109.9078, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Cruise Ship Terminal", pointType: "Mixed-Use Development", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8756, longitude: -109.9112, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos", manuallyVerified: true }),
  pt({ name: "Lover's Beach Cabo San Lucas", pointType: "Beach / Waterfront", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8745, longitude: -109.9123, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Sportfishing Fleet Harbor", pointType: "Beach / Waterfront", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8834, longitude: -109.9045, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Chileno Bay Beach Club Corridor", pointType: "Beach / Waterfront", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9789, longitude: -109.8234, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Santa María Bay Resort Zone", pointType: "Beach / Waterfront", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9912, longitude: -109.8156, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo del Sol Ocean Golf Course", pointType: "Sports Venue", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9678, longitude: -109.8345, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Querencia Golf and Beach Club", pointType: "Mixed-Use Development", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9456, longitude: -109.8512, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Palmilla Bay Tourist Corridor", pointType: "Beach / Waterfront", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 23.0123, longitude: -109.8089, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo Pulmo National Park Gateway", pointType: "Tourist Attraction", city: "Cabo Pulmo", submarket: "East Cape", latitude: 23.4456, longitude: -109.4234, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Los Barriles East Cape Sportfishing", pointType: "Beach / Waterfront", city: "Los Barriles", submarket: "East Cape", latitude: 23.6789, longitude: -109.7012, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Buena Vista Beach East Cape", pointType: "Beach / Waterfront", city: "Los Barriles", submarket: "East Cape", latitude: 23.6234, longitude: -109.6845, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "La Ribera East Cape Gateway", pointType: "Future Growth Node", city: "La Ribera", submarket: "East Cape", latitude: 23.5345, longitude: -109.5456, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "East Cape Wind Sports Corridor", pointType: "Sports Venue", city: "Los Barriles", submarket: "East Cape", latitude: 23.6912, longitude: -109.7123, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Todos Santos Pueblo Mágico", pointType: "Tourist Attraction", city: "Todos Santos", submarket: "Other", latitude: 23.4467, longitude: -110.2234, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/todos-santos" }),
  pt({ name: "Hotel California Todos Santos Landmark", pointType: "Entertainment District", city: "Todos Santos", submarket: "Other", latitude: 23.4478, longitude: -110.2245, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/todos-santos" }),
  pt({ name: "Cerritos Beach Surf Zone", pointType: "Beach / Waterfront", city: "Pescadero", submarket: "Other", latitude: 23.3789, longitude: -110.1789, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Flora Farms Culinary Destination", pointType: "Tourist Attraction", city: "San José del Cabo", submarket: "Tourist Corridor", latitude: 23.0234, longitude: -109.7456, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Camino Real Los Cabos Resort Zone", pointType: "Mixed-Use Development", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9567, longitude: -109.8423, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Diamante Cabo San Lucas Golf", pointType: "Sports Venue", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.9012, longitude: -109.9234, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Hospital Zone", pointType: "Medical Campus", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8934, longitude: -109.9189, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "San José del Cabo Estuary Bird Sanctuary", pointType: "Tourist Attraction", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0489, longitude: -109.6989, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Corridor 1 Resort Access Node", pointType: "Future Growth Node", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9823, longitude: -109.8198, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Twin Dolphin Marina Corridor", pointType: "Beach / Waterfront", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9712, longitude: -109.8289, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Pedregal Cabo San Lucas Luxury Hills", pointType: "Mixed-Use Development", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8678, longitude: -109.9289, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Whale Watching Harbor", pointType: "Tourist Attraction", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8867, longitude: -109.9023, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "San José del Cabo Thursday Art Walk", pointType: "Entertainment District", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0601, longitude: -109.6965, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "El Dorado Golf Club Corridor", pointType: "Sports Venue", city: "San José del Cabo", submarket: "Tourist Corridor", latitude: 23.0178, longitude: -109.7512, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Marina Boardwalk", pointType: "Entertainment District", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8801, longitude: -109.9091, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Playa El Chileno Public Beach", pointType: "Beach / Waterfront", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9812, longitude: -109.8212, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Playa Santa María Snorkel Cove", pointType: "Beach / Waterfront", city: "Los Cabos", submarket: "Tourist Corridor", latitude: 22.9923, longitude: -109.8145, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Los Cabos Convention Center Precinct", pointType: "Convention Center", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0567, longitude: -109.6912, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Luxury Hills Growth Node", pointType: "Future Growth Node", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8623, longitude: -109.9345, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "East Cape Four Seasons Resort Zone", pointType: "Mixed-Use Development", city: "Los Barriles", submarket: "East Cape", latitude: 23.6456, longitude: -109.6934, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "SJD Airport Industrial Logistics Zone", pointType: "Industrial / Logistics Zone", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.1489, longitude: -109.7156, sourceReference: "https://www.asur.com.mx/" }),
  pt({ name: "Cabo San Lucas Medano Entertainment Pier", pointType: "Entertainment District", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8912, longitude: -109.9034, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Playa Acapulquito Surf Break", pointType: "Beach / Waterfront", city: "San José del Cabo", submarket: "San José del Cabo", latitude: 23.0312, longitude: -109.7189, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo Real Convention and Resort District", pointType: "Convention Center", city: "San José del Cabo", submarket: "Tourist Corridor", latitude: 23.0267, longitude: -109.7389, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Playa El Tule East Cape Beach", pointType: "Beach / Waterfront", city: "La Ribera", submarket: "East Cape", latitude: 23.5289, longitude: -109.5512, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Misiones del Cabo Resort Corridor", pointType: "Mixed-Use Development", city: "San José del Cabo", submarket: "Tourist Corridor", latitude: 23.0089, longitude: -109.7623, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
  pt({ name: "Cabo San Lucas Marina Golden Zone", pointType: "Entertainment District", city: "Cabo San Lucas", submarket: "Cabo San Lucas", latitude: 22.8789, longitude: -109.9101, sourceReference: "https://www.visitmexico.com/en/main-destinations/baja-california-sur/los-cabos" }),
];

export function getLosCabosCandidates() {
  return LOS_CABOS_CANDIDATES;
}

export { LOS_CABOS_SUBMARKETS };
