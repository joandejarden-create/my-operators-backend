/**
 * Mérida / Yucatán demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyMeridaYucatanGovernanceDefaults,
  MERIDA_YUCATAN_SUBMARKETS,
} from "./merida-yucatan-demand-anchor-governance.js";

import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyMeridaYucatanGovernanceDefaults);

export const MERIDA_YUCATAN_CANDIDATES = [
  pt({ name: "Mérida International Airport Corridor", pointType: "Future Growth Node", city: "Mérida", submarket: "Airport Corridor", latitude: 20.937, longitude: -89.6577, sourceReference: "https://www.aeropuertosasa.mx/", manuallyVerified: true }),
  pt({ name: "Paseo de Montejo Historic Boulevard", pointType: "Tourist Attraction", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9756, longitude: -89.6178, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Plaza Grande Centro Histórico", pointType: "Government / Civic Center", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9674, longitude: -89.5926, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Catedral de San Ildefonso Mérida", pointType: "Tourist Attraction", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9671, longitude: -89.5923, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Palacio de Gobierno Yucatán", pointType: "Government / Civic Center", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9678, longitude: -89.5912, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Teatro Peón Contreras", pointType: "Tourist Attraction", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9689, longitude: -89.6234, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Museo Regional de Antropología Palacio Cantón", pointType: "Tourist Attraction", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9789, longitude: -89.6234, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Gran Museo del Mundo Maya", pointType: "Tourist Attraction", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9512, longitude: -89.6234, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Santa Lucía Park Dining Corridor", pointType: "Entertainment District", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9712, longitude: -89.6189, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Centro Histórico Boutique Hotel Zone", pointType: "Mixed-Use Development", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9667, longitude: -89.5945, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Centro de Convenciones Yucatán Siglo XXI", pointType: "Convention Center", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9939, longitude: -89.6142, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Hotel Zone Siglo XXI Convention Hotels", pointType: "Mixed-Use Development", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9912, longitude: -89.6167, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Plaza Galerías Mérida", pointType: "Entertainment District", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9889, longitude: -89.6189, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "City Center Mérida Shopping Complex", pointType: "Entertainment District", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9867, longitude: -89.6212, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Siglo XXI Government and Civic Precinct", pointType: "Government / Civic Center", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9956, longitude: -89.6123, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "MID Airport Hotel and Business Corridor", pointType: "Business District", city: "Mérida", submarket: "Airport Corridor", latitude: 20.9312, longitude: -89.6512, sourceReference: "https://www.aeropuertosasa.mx/" }),
  pt({ name: "Mérida Airport Cargo and Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Mérida", submarket: "Airport Corridor", latitude: 20.9412, longitude: -89.6623, sourceReference: "https://www.aeropuertosasa.mx/" }),
  pt({ name: "Progreso Malecón Beach and Cruise Port", pointType: "Beach / Waterfront", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2853, longitude: -89.6644, sourceReference: "https://www.yucatan.travel/", manuallyVerified: true }),
  pt({ name: "Puerto Progreso Cruise Ship Terminal", pointType: "Mixed-Use Development", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2812, longitude: -89.6689, sourceReference: "https://www.yucatan.travel/", manuallyVerified: true }),
  pt({ name: "Progreso Beach Resort Strip", pointType: "Beach / Waterfront", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2789, longitude: -89.6712, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Chicxulub Puerto Beach Town", pointType: "Beach / Waterfront", city: "Chicxulub Puerto", submarket: "Progreso / Costa Yucateca", latitude: 21.2689, longitude: -89.6012, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Telchac Puerto Beach Corridor", pointType: "Beach / Waterfront", city: "Telchac Puerto", submarket: "Progreso / Costa Yucateca", latitude: 21.3234, longitude: -89.2789, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Chichén Itzá Archaeological Zone UNESCO", pointType: "Tourist Attraction", city: "Tinúm", submarket: "Other", latitude: 20.6843, longitude: -88.5678, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/chichen-itza", manuallyVerified: true }),
  pt({ name: "Uxmal Archaeological Zone UNESCO", pointType: "Tourist Attraction", city: "Santa Elena", submarket: "Other", latitude: 20.3594, longitude: -89.7715, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/uxmal", manuallyVerified: true }),
  pt({ name: "Cenote Ik Kil Chichén Itzá", pointType: "Tourist Attraction", city: "Tinúm", submarket: "Other", latitude: 20.6612, longitude: -88.5534, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/chichen-itza" }),
  pt({ name: "Valladolid Colonial Pueblo Mágico", pointType: "Tourist Attraction", city: "Valladolid", submarket: "Other", latitude: 20.6889, longitude: -88.2012, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/valladolid" }),
  pt({ name: "Izamal Yellow City Pueblo Mágico", pointType: "Tourist Attraction", city: "Izamal", submarket: "Other", latitude: 20.9334, longitude: -89.0178, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/izamal" }),
  pt({ name: "Celestún Biosphere Reserve Flamingo Coast", pointType: "Tourist Attraction", city: "Celestún", submarket: "Other", latitude: 20.8612, longitude: -90.4012, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Ruta de los Cenotes Corridor", pointType: "Tourist Attraction", city: "Cuzamá", submarket: "Other", latitude: 20.7412, longitude: -89.3234, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Hospital Star Médica Mérida", pointType: "Medical Campus", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9845, longitude: -89.6234, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Clínica de Mérida Medical District", pointType: "Medical Campus", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9734, longitude: -89.6012, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Universidad Autónoma de Yucatán Campus", pointType: "University / College", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9889, longitude: -89.6345, sourceReference: "https://www.uady.mx/" }),
  pt({ name: "Universidad Marista de Mérida", pointType: "University / College", city: "Mérida", submarket: "Industrial / Periférico", latitude: 20.9612, longitude: -89.6512, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Parque Industrial Yucatán", pointType: "Industrial / Logistics Zone", city: "Umán", submarket: "Industrial / Periférico", latitude: 20.8912, longitude: -89.7512, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Periférico Mérida Industrial Corridor", pointType: "Industrial / Logistics Zone", city: "Mérida", submarket: "Industrial / Periférico", latitude: 20.9512, longitude: -89.6789, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Ciudad Industrial Mérida Norte", pointType: "Industrial / Logistics Zone", city: "Mérida", submarket: "Industrial / Periférico", latitude: 21.0012, longitude: -89.6345, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Mérida Norte Residential Growth Node", pointType: "Future Growth Node", city: "Mérida", submarket: "Industrial / Periférico", latitude: 21.0234, longitude: -89.6123, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "La Isla Mérida Shopping Center", pointType: "Entertainment District", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9823, longitude: -89.6267, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Altabrisa Mall Mérida", pointType: "Entertainment District", city: "Mérida", submarket: "Industrial / Periférico", latitude: 20.9689, longitude: -89.6412, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Hacienda Sotuta de Peón Living Museum", pointType: "Tourist Attraction", city: "Tekit", submarket: "Other", latitude: 20.5312, longitude: -89.2789, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Mayapán Archaeological Site", pointType: "Tourist Attraction", city: "Telchaquillo", submarket: "Other", latitude: 20.6289, longitude: -89.4612, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Dzibilchaltún Archaeological Site", pointType: "Tourist Attraction", city: "Mérida", submarket: "Other", latitude: 21.0912, longitude: -89.5912, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Mérida Gastronomic Corridor Santiago", pointType: "Entertainment District", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9645, longitude: -89.6234, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Paseo de Montejo Mansion Row", pointType: "Tourist Attraction", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9812, longitude: -89.6189, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Mérida International Convention Hotel Cluster", pointType: "Convention Center", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9901, longitude: -89.6156, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Progreso International Terminal Logistics", pointType: "Industrial / Logistics Zone", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2834, longitude: -89.6712, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Mérida Periférico Convention Access Node", pointType: "Future Growth Node", city: "Mérida", submarket: "Siglo XXI / Convention Zone", latitude: 20.9878, longitude: -89.6089, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Chuburná Puerto Beach Residential", pointType: "Beach / Waterfront", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2734, longitude: -89.6634, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Mérida Centro Histórico Sunday Market", pointType: "Entertainment District", city: "Mérida", submarket: "Centro Histórico / Paseo de Montejo", latitude: 20.9672, longitude: -89.5934, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
  pt({ name: "Yucatán Industrial Port of Progreso Access", pointType: "Future Growth Node", city: "Progreso", submarket: "Progreso / Costa Yucateca", latitude: 21.2798, longitude: -89.6656, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Mérida Airport Business Park Growth Node", pointType: "Future Growth Node", city: "Mérida", submarket: "Airport Corridor", latitude: 20.9289, longitude: -89.6489, sourceReference: "https://www.aeropuertosasa.mx/" }),
  pt({ name: "Hacienda Temozón Luxury Hotel Zone", pointType: "Mixed-Use Development", city: "Temozón", submarket: "Other", latitude: 20.9012, longitude: -88.9234, sourceReference: "https://www.yucatan.travel/" }),
  pt({ name: "Mérida North Periférico Office Corridor", pointType: "Business District", city: "Mérida", submarket: "Industrial / Periférico", latitude: 21.0123, longitude: -89.6234, sourceReference: "https://www.visitmexico.com/en/main-destinations/yucatan/merida" }),
];

export function getMeridaYucatanCandidates() {
  return MERIDA_YUCATAN_CANDIDATES;
}

export { MERIDA_YUCATAN_SUBMARKETS };
