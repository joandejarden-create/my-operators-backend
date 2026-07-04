/**
 * Monterrey demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyMonterreyGovernanceDefaults,
  MONTERREY_SUBMARKETS,
} from "./monterrey-demand-anchor-governance.js";

import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyMonterreyGovernanceDefaults);

export const MONTERREY_CANDIDATES = [
  pt({ name: "Monterrey International Airport Corridor", pointType: "Future Growth Node", city: "Apodaca", submarket: "Airport Corridor", latitude: 25.7785, longitude: -100.1069, sourceReference: "https://www.oma.aero/", manuallyVerified: true }),
  pt({ name: "Macroplaza Civic and Government Core", pointType: "Government / Civic Center", city: "Monterrey", submarket: "Centro", latitude: 25.6714, longitude: -100.3097, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Faro de Comercio Landmark", pointType: "Tourist Attraction", city: "Monterrey", submarket: "Centro", latitude: 25.6708, longitude: -100.3089, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Palacio de Gobierno Nuevo León", pointType: "Government / Civic Center", city: "Monterrey", submarket: "Centro", latitude: 25.6712, longitude: -100.3101, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Museo de Historia Mexicana", pointType: "Tourist Attraction", city: "Monterrey", submarket: "Centro", latitude: 25.6723, longitude: -100.3112, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "MARCO Contemporary Art Museum", pointType: "Tourist Attraction", city: "Monterrey", submarket: "Centro", latitude: 25.6698, longitude: -100.3078, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Barrio Antiguo Entertainment District", pointType: "Entertainment District", city: "Monterrey", submarket: "Centro", latitude: 25.6689, longitude: -100.3045, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Catedral Metropolitana de Monterrey", pointType: "Tourist Attraction", city: "Monterrey", submarket: "Centro", latitude: 25.6718, longitude: -100.3091, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Parque Fundidora Urban Park", pointType: "Mixed-Use Development", city: "Monterrey", submarket: "Centro", latitude: 25.6789, longitude: -100.2845, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Arena Monterrey Entertainment Complex", pointType: "Sports Venue", city: "Monterrey", submarket: "Centro", latitude: 25.6812, longitude: -100.2823, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Paseo Santa Lucía Riverwalk", pointType: "Beach / Waterfront", city: "Monterrey", submarket: "Centro", latitude: 25.6745, longitude: -100.2912, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Cerro de la Silla Landmark", pointType: "Tourist Attraction", city: "Monterrey", submarket: "Centro", latitude: 25.6234, longitude: -100.2456, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "San Pedro Garza García Corporate Corridor", pointType: "Business District", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6514, longitude: -100.3567, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Calzada del Valle Luxury Retail Strip", pointType: "Entertainment District", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6534, longitude: -100.3612, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Fashion Drive San Pedro", pointType: "Entertainment District", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6489, longitude: -100.3589, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Hospital Zambrano Hellion Medical Campus", pointType: "Medical Campus", city: "Monterrey", submarket: "San Pedro", latitude: 25.6456, longitude: -100.3512, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Club Campestre San Pedro", pointType: "Sports Venue", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6423, longitude: -100.3678, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Valle Oriente Financial District", pointType: "Business District", city: "San Pedro Garza García", submarket: "Valle Oriente", latitude: 25.6389, longitude: -100.3234, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Galerías Monterrey Shopping Center", pointType: "Entertainment District", city: "San Pedro Garza García", submarket: "Valle Oriente", latitude: 25.6412, longitude: -100.3289, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Paseo San Pedro Mall", pointType: "Entertainment District", city: "San Pedro Garza García", submarket: "Valle Oriente", latitude: 25.6367, longitude: -100.3312, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Torre KOI Landmark Tower", pointType: "Business District", city: "San Pedro Garza García", submarket: "Valle Oriente", latitude: 25.6345, longitude: -100.3267, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Valle Oriente Convention Hotel Zone", pointType: "Convention Center", city: "San Pedro Garza García", submarket: "Valle Oriente", latitude: 25.6378, longitude: -100.3245, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "BBVA Stadium Rayados", pointType: "Sports Venue", city: "Guadalupe", submarket: "Other", latitude: 25.6869, longitude: -100.2458, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Estadio Universitario Tigres", pointType: "Sports Venue", city: "San Nicolás de los Garza", submarket: "Other", latitude: 25.7234, longitude: -100.2912, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Universidad Autónoma de Nuevo León Campus", pointType: "University / College", city: "San Nicolás de los Garza", submarket: "Other", latitude: 25.7189, longitude: -100.3012, sourceReference: "https://www.uanl.mx/" }),
  pt({ name: "ITESM Monterrey Main Campus", pointType: "University / College", city: "Monterrey", submarket: "Other", latitude: 25.6512, longitude: -100.2912, sourceReference: "https://www.tec.mx/" }),
  pt({ name: "Santa Catarina Industrial Corridor", pointType: "Industrial / Logistics Zone", city: "Santa Catarina", submarket: "Santa Catarina", latitude: 25.6734, longitude: -100.4567, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Parque Industrial Santa Catarina", pointType: "Industrial / Logistics Zone", city: "Santa Catarina", submarket: "Santa Catarina", latitude: 25.6812, longitude: -100.4623, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Grupo Industrial Alfa Campus Zone", pointType: "Industrial / Logistics Zone", city: "Santa Catarina", submarket: "Santa Catarina", latitude: 25.6678, longitude: -100.4489, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Puente de la Unidad Landmark", pointType: "Tourist Attraction", city: "Monterrey", submarket: "Centro", latitude: 25.6912, longitude: -100.2789, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Cintermex Convention Center", pointType: "Convention Center", city: "Monterrey", submarket: "Centro", latitude: 25.6834, longitude: -100.2934, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Hospital Universitario UANL", pointType: "Medical Campus", city: "Monterrey", submarket: "Other", latitude: 25.6912, longitude: -100.3123, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "García Industrial and Logistics Park", pointType: "Industrial / Logistics Zone", city: "García", submarket: "Airport Corridor", latitude: 25.8012, longitude: -100.5234, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Apodaca Airport Hotel Corridor", pointType: "Business District", city: "Apodaca", submarket: "Airport Corridor", latitude: 25.7712, longitude: -100.1123, sourceReference: "https://www.oma.aero/" }),
  pt({ name: "MTY Airport Cargo and Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Apodaca", submarket: "Airport Corridor", latitude: 25.7823, longitude: -100.1012, sourceReference: "https://www.oma.aero/" }),
  pt({ name: "Chipinque Ecological Park", pointType: "Tourist Attraction", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6012, longitude: -100.3567, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Grutas de García Cave Attraction", pointType: "Tourist Attraction", city: "García", submarket: "Other", latitude: 25.8234, longitude: -100.5456, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Cola de Caballo Waterfall", pointType: "Tourist Attraction", city: "Santiago", submarket: "Other", latitude: 25.5789, longitude: -100.1789, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Bioparque Estrella Safari Park", pointType: "Tourist Attraction", city: "Juárez", submarket: "Other", latitude: 25.6234, longitude: -100.1234, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Plaza Fiesta San Agustín", pointType: "Entertainment District", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6467, longitude: -100.3712, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Punto Valle Shopping Center", pointType: "Entertainment District", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6523, longitude: -100.3645, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Monterrey Metropolitan Convention Bureau Zone", pointType: "Convention Center", city: "Monterrey", submarket: "Centro", latitude: 25.6734, longitude: -100.3067, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Santa Catarina La Puerta Industrial Gateway", pointType: "Future Growth Node", city: "Santa Catarina", submarket: "Santa Catarina", latitude: 25.6589, longitude: -100.4512, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Valle Poniente Mixed-Use Growth Node", pointType: "Future Growth Node", city: "Santa Catarina", submarket: "Santa Catarina", latitude: 25.6912, longitude: -100.4678, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "San Pedro WTC Monterrey", pointType: "Business District", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6545, longitude: -100.3534, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Monterrey Steel Museum Horno3", pointType: "Tourist Attraction", city: "Monterrey", submarket: "Centro", latitude: 25.6778, longitude: -100.2812, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Parque La Huasteca Climbing Zone", pointType: "Tourist Attraction", city: "Santa Catarina", submarket: "Other", latitude: 25.6123, longitude: -100.4123, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Monterrey Airport Business Park", pointType: "Mixed-Use Development", city: "Apodaca", submarket: "Airport Corridor", latitude: 25.7689, longitude: -100.1189, sourceReference: "https://www.oma.aero/" }),
  pt({ name: "Valle Oriente Office Tower Cluster", pointType: "Business District", city: "San Pedro Garza García", submarket: "Valle Oriente", latitude: 25.6356, longitude: -100.3223, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "Centro Histórico Monterrey Hotel Zone", pointType: "Future Growth Node", city: "Monterrey", submarket: "Centro", latitude: 25.6701, longitude: -100.3082, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
  pt({ name: "San Pedro Calzada San Pedro Dining Corridor", pointType: "Entertainment District", city: "San Pedro Garza García", submarket: "San Pedro", latitude: 25.6498, longitude: -100.3623, sourceReference: "https://www.visitmexico.com/en/main-destinations/nuevo-leon/monterrey" }),
];

export function getMonterreyCandidates() {
  return MONTERREY_CANDIDATES;
}

export { MONTERREY_SUBMARKETS };
