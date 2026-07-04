/**
 * Mexico City demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyMexicoCityGovernanceDefaults,
  MEXICO_CITY_SUBMARKETS,
} from "./mexico-city-demand-anchor-governance.js";

import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyMexicoCityGovernanceDefaults);

export const MEXICO_CITY_CANDIDATES = [
  pt({ name: "Benito Juárez International Airport Corridor", pointType: "Future Growth Node", city: "Mexico City", submarket: "Airport Corridor", latitude: 19.4363, longitude: -99.0721, sourceReference: "https://www.aicm.com.mx/", manuallyVerified: true }),
  pt({ name: "Museo Soumaya Polanco", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Polanco", latitude: 19.4407, longitude: -99.2056, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Antara Fashion Hall Polanco", pointType: "Entertainment District", city: "Mexico City", submarket: "Polanco", latitude: 19.4383, longitude: -99.2017, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Museo Jumex Polanco", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Polanco", latitude: 19.4401, longitude: -99.2041, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Polanco Business and Embassy Corridor", pointType: "Business District", city: "Mexico City", submarket: "Polanco", latitude: 19.4336, longitude: -99.1991, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Centro Citibanamex Convention Precinct", pointType: "Convention Center", city: "Mexico City", submarket: "Polanco", latitude: 19.4378, longitude: -99.2045, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Lago Mayor Chapultepec Polanco Edge", pointType: "Beach / Waterfront", city: "Mexico City", submarket: "Polanco", latitude: 19.4245, longitude: -99.1945, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Ángel de la Independencia Reforma", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.427, longitude: -99.1677, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Chapultepec Castle and Park", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.4204, longitude: -99.1817, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Museo Nacional de Antropología", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.426, longitude: -99.1863, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Paseo de la Reforma Financial Corridor", pointType: "Business District", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.4286, longitude: -99.1611, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Torre Mayor Reforma CBD", pointType: "Business District", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.4245, longitude: -99.1758, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Diana Cazadora Roundabout Reforma", pointType: "Entertainment District", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.4252, longitude: -99.1708, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Zona Rosa Entertainment Corridor", pointType: "Entertainment District", city: "Mexico City", submarket: "Reforma / Juárez", latitude: 19.4268, longitude: -99.1635, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Centro Santa Fe", pointType: "Mixed-Use Development", city: "Mexico City", submarket: "Santa Fe", latitude: 19.3594, longitude: -99.2767, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Samara Shops Santa Fe", pointType: "Entertainment District", city: "Mexico City", submarket: "Santa Fe", latitude: 19.3631, longitude: -99.2712, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Hospital ABC Santa Fe Campus", pointType: "Medical Campus", city: "Mexico City", submarket: "Santa Fe", latitude: 19.3678, longitude: -99.2634, sourceReference: "https://www.abchospital.com/" }),
  pt({ name: "Parque La Mexicana Santa Fe", pointType: "Mixed-Use Development", city: "Mexico City", submarket: "Santa Fe", latitude: 19.3712, longitude: -99.2689, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Torre Corporativa Santa Fe", pointType: "Business District", city: "Mexico City", submarket: "Santa Fe", latitude: 19.3567, longitude: -99.2745, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Tecnológico de Monterrey Santa Fe Campus", pointType: "University / College", city: "Mexico City", submarket: "Santa Fe", latitude: 19.3523, longitude: -99.2812, sourceReference: "https://www.tec.mx/" }),
  pt({ name: "Parque México Condesa", pointType: "Entertainment District", city: "Mexico City", submarket: "Condesa / Roma", latitude: 19.4117, longitude: -99.1717, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Condesa Dining and Nightlife Corridor", pointType: "Entertainment District", city: "Mexico City", submarket: "Condesa / Roma", latitude: 19.4123, longitude: -99.1756, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Roma Norte Art and Design District", pointType: "Entertainment District", city: "Mexico City", submarket: "Condesa / Roma", latitude: 19.4189, longitude: -99.1623, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Plaza Luis Cabrera Condesa", pointType: "Mixed-Use Development", city: "Mexico City", submarket: "Condesa / Roma", latitude: 19.4089, longitude: -99.1689, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Centro Cultural de España en México", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Condesa / Roma", latitude: 19.4212, longitude: -99.1589, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Mercado Roma Food Hall", pointType: "Entertainment District", city: "Mexico City", submarket: "Condesa / Roma", latitude: 19.4167, longitude: -99.1612, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Zócalo Plaza de la Constitución UNESCO", pointType: "Government / Civic Center", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4326, longitude: -99.1332, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city", manuallyVerified: true }),
  pt({ name: "Palacio de Bellas Artes", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4352, longitude: -99.1413, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Metropolitan Cathedral Mexico City", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4345, longitude: -99.1312, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Templo Mayor Archaeological Zone", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.435, longitude: -99.1313, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Torre Latinoamericana Observation Deck", pointType: "Tourist Attraction", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4338, longitude: -99.1406, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Alameda Central Park", pointType: "Entertainment District", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4358, longitude: -99.1442, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Palacio Nacional Government Precinct", pointType: "Government / Civic Center", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4324, longitude: -99.1315, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Mercado de San Juan Gourmet Market", pointType: "Entertainment District", city: "Mexico City", submarket: "Centro Histórico", latitude: 19.4278, longitude: -99.1389, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "World Trade Center Mexico City", pointType: "Convention Center", city: "Mexico City", submarket: "Insurgentes / WTC", latitude: 19.3942, longitude: -99.1735, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Insurgentes Sur Corporate Corridor", pointType: "Business District", city: "Mexico City", submarket: "Insurgentes / WTC", latitude: 19.3812, longitude: -99.1789, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Torre Insignia WTC District", pointType: "Business District", city: "Mexico City", submarket: "Insurgentes / WTC", latitude: 19.3934, longitude: -99.1712, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Pepsi Center WTC Arena", pointType: "Sports Venue", city: "Mexico City", submarket: "Insurgentes / WTC", latitude: 19.3956, longitude: -99.1756, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Centro Banorte WTC Convention Annex", pointType: "Convention Center", city: "Mexico City", submarket: "Insurgentes / WTC", latitude: 19.3923, longitude: -99.1767, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Aeropuerto Cargo and Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Mexico City", submarket: "Airport Corridor", latitude: 19.4289, longitude: -99.0656, sourceReference: "https://www.aicm.com.mx/" }),
  pt({ name: "Peñón de los Baños Transit Hub", pointType: "Future Growth Node", city: "Mexico City", submarket: "Airport Corridor", latitude: 19.4512, longitude: -99.0789, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Pantitlán Multimodal Station", pointType: "Future Growth Node", city: "Mexico City", submarket: "Airport Corridor", latitude: 19.4156, longitude: -99.0723, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Museo Frida Kahlo Casa Azul", pointType: "Tourist Attraction", city: "Coyoacán", submarket: "Coyoacán / San Ángel", latitude: 19.355, longitude: -99.1623, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "UNAM Ciudad Universitaria Campus", pointType: "University / College", city: "Coyoacán", submarket: "Coyoacán / San Ángel", latitude: 19.3244, longitude: -99.2, sourceReference: "https://www.unam.mx/" }),
  pt({ name: "Estadio Olímpico Universitario", pointType: "Sports Venue", city: "Coyoacán", submarket: "Coyoacán / San Ángel", latitude: 19.3312, longitude: -99.1912, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "San Ángel Bazaar and Cultural District", pointType: "Tourist Attraction", city: "Álvaro Obregón", submarket: "Coyoacán / San Ángel", latitude: 19.3456, longitude: -99.1945, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Estadio Azteca", pointType: "Sports Venue", city: "Tlalpan", submarket: "Coyoacán / San Ángel", latitude: 19.3029, longitude: -99.1505, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Basilica of Our Lady of Guadalupe", pointType: "Tourist Attraction", city: "Gustavo A. Madero", submarket: "Other", latitude: 19.4847, longitude: -99.1176, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Teotihuacán Archaeological Zone", pointType: "Tourist Attraction", city: "San Juan Teotihuacán", submarket: "Other", latitude: 19.6925, longitude: -98.8437, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-state/teotihuacan", manuallyVerified: true }),
  pt({ name: "Arena Ciudad de México", pointType: "Sports Venue", city: "Azcapotzalco", submarket: "Other", latitude: 19.4931, longitude: -99.2386, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Autódromo Hermanos Rodríguez", pointType: "Sports Venue", city: "Iztacalco", submarket: "Other", latitude: 19.4042, longitude: -99.0908, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
  pt({ name: "Foro Sol Entertainment Complex", pointType: "Entertainment District", city: "Iztacalco", submarket: "Other", latitude: 19.4056, longitude: -99.0923, sourceReference: "https://www.visitmexico.com/en/main-destinations/mexico-city" }),
];

export function getMexicoCityCandidates() {
  return MEXICO_CITY_CANDIDATES;
}

export { MEXICO_CITY_SUBMARKETS };
