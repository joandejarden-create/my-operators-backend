/**
 * Colombia demand anchor candidates (market-by-market first pass).
 * These are source-backed candidates for pre-import verification.
 */

import { getPointTypeDefaults } from "../demand-anchors/point-type-defaults.js";
import {
  applyColombiaGovernanceDefaults,
  COLOMBIA_PHASE_1_SUBMARKETS,
  COLOMBIA_PHASE_2_SUBMARKETS,
} from "./colombia-demand-anchor-governance.js";

const COUNTRY = "Colombia";
const REGION = "South America";

function pt(v) {
  const defaults = getPointTypeDefaults(v.pointType);
  const rationale =
    v.hotelDemandNote ||
    defaults.hotelDemandRationale ||
    "Supports identifiable hotel demand in this submarket.";
  const base = {
    name: v.name,
    pointType: v.pointType,
    city: v.city,
    country: COUNTRY,
    region: REGION,
    submarket: v.submarket,
    latitude: v.latitude,
    longitude: v.longitude,
    source: "Public Source",
    sourceReference: v.sourceReference,
    dataConfidence: v.dataConfidence || "Medium",
    notes:
      v.notes ||
      `Submarket: ${v.submarket}. ${rationale} Candidate pending Google pre-import verification.`,
  };
  if (v.googleSearchQuery) base.googleSearchQuery = v.googleSearchQuery;
  return applyColombiaGovernanceDefaults(base, v.governance || {});
}

export const COLOMBIA_SUBMARKETS = [...COLOMBIA_PHASE_1_SUBMARKETS, ...COLOMBIA_PHASE_2_SUBMARKETS];

const COLOMBIA_ALL_CANDIDATES = [
  // Cartagena (16)
  pt({ name: "Cartagena de Indias Convention Center", pointType: "Convention Center", city: "Cartagena", submarket: "Cartagena", latitude: 10.4212, longitude: -75.548, sourceReference: "https://www.cccartagena.com/" }),
  pt({ name: "Centro Histórico de Cartagena", pointType: "Tourist Attraction", city: "Cartagena", submarket: "Cartagena", latitude: 10.4236, longitude: -75.5511, sourceReference: "https://whc.unesco.org/en/list/285/" }),
  pt({ name: "Bocagrande Waterfront Strip", pointType: "Beach / Waterfront", city: "Cartagena", submarket: "Cartagena", latitude: 10.4033, longitude: -75.5564, sourceReference: "https://www.colombia.travel/en/cartagena" }),
  pt({ name: "Playa Blanca Barú", pointType: "Beach / Waterfront", city: "Cartagena", submarket: "Cartagena", latitude: 10.1812, longitude: -75.7564, sourceReference: "https://www.colombia.travel/en/cartagena" }),
  pt({ name: "Hospital Serena del Mar", pointType: "Medical Campus", city: "Cartagena", submarket: "Cartagena", latitude: 10.4874, longitude: -75.4863, sourceReference: "https://www.serenadelmar.com.co/" }),
  pt({ name: "Universidad de Cartagena (San Agustín Campus)", pointType: "University / College", city: "Cartagena", submarket: "Cartagena", latitude: 10.4254, longitude: -75.5515, sourceReference: "https://www.unicartagena.edu.co/" }),
  pt({ name: "Estadio Jaime Morón León", pointType: "Sports Venue", city: "Cartagena", submarket: "Cartagena", latitude: 10.3912, longitude: -75.4877, sourceReference: "https://en.wikipedia.org/wiki/Jaime_Mor%C3%B3n_Le%C3%B3n_Stadium" }),
  pt({ name: "Getsemaní Entertainment District", pointType: "Entertainment District", city: "Cartagena", submarket: "Cartagena", latitude: 10.4194, longitude: -75.5463, sourceReference: "https://www.colombia.travel/en/cartagena" }),
  pt({ name: "Zona Franca la Candelaria", pointType: "Industrial / Logistics Zone", city: "Cartagena", submarket: "Cartagena", latitude: 10.3538, longitude: -75.4914, sourceReference: "https://www.zonafrancalacandelaria.com/" }),
  pt({ name: "Mamonal Industrial Corridor", pointType: "Industrial / Logistics Zone", city: "Cartagena", submarket: "Cartagena", latitude: 10.3336, longitude: -75.5159, sourceReference: "https://www.cartagena.gov.co/" }),
  pt({ name: "Centro Administrativo Distrital de Cartagena", pointType: "Government / Civic Center", city: "Cartagena", submarket: "Cartagena", latitude: 10.3974, longitude: -75.4832, sourceReference: "https://www.cartagena.gov.co/" }),
  pt({ name: "Avenida San Martín Business Corridor", pointType: "Business District", city: "Cartagena", submarket: "Cartagena", latitude: 10.4041, longitude: -75.5545, sourceReference: "https://www.colombia.travel/en/cartagena" }),
  pt({ name: "La Serrezuela Mixed-Use Complex", pointType: "Mixed-Use Development", city: "Cartagena", submarket: "Cartagena", latitude: 10.4248, longitude: -75.5479, sourceReference: "https://laserrezuela.com/" }),
  pt({ name: "Puerto de Cartagena Cruise Terminal", pointType: "Tourist Attraction", city: "Cartagena", submarket: "Cartagena", latitude: 10.4008, longitude: -75.5346, sourceReference: "https://www.puertocartagena.com/" }),
  pt({ name: "Rafael Núñez International Airport Gateway", pointType: "Future Growth Node", city: "Cartagena", submarket: "Cartagena", latitude: 10.4424, longitude: -75.513, sourceReference: "https://www.aerocivil.gov.co/" }),
  pt({ name: "Serena del Mar Urban Expansion Node", pointType: "Future Growth Node", city: "Cartagena", submarket: "Cartagena", latitude: 10.4932, longitude: -75.4887, sourceReference: "https://www.serenadelmar.com.co/" }),

  // Bogota (16)
  pt({ name: "Corferias Bogotá Convention Center", pointType: "Convention Center", city: "Bogotá", submarket: "Bogotá", latitude: 4.6297, longitude: -74.0935, sourceReference: "https://corferias.com/" }),
  pt({ name: "Ágora Bogotá Convention Center", pointType: "Convention Center", city: "Bogotá", submarket: "Bogotá", latitude: 4.6281, longitude: -74.093, sourceReference: "https://agorabogota.com/" }),
  pt({ name: "La Candelaria Historic District", pointType: "Tourist Attraction", city: "Bogotá", submarket: "Bogotá", latitude: 4.5981, longitude: -74.0721, sourceReference: "https://www.colombia.travel/en/bogota" }),
  pt({ name: "Monserrate Sanctuary", pointType: "Tourist Attraction", city: "Bogotá", submarket: "Bogotá", latitude: 4.6056, longitude: -74.0551, sourceReference: "https://www.colombia.travel/en/bogota" }),
  pt({ name: "Zona T / Zona Rosa Entertainment District", pointType: "Entertainment District", city: "Bogotá", submarket: "Bogotá", latitude: 4.6676, longitude: -74.0545, sourceReference: "https://www.colombia.travel/en/bogota" }),
  pt({ name: "Parque de la 93 Entertainment District", pointType: "Entertainment District", city: "Bogotá", submarket: "Bogotá", latitude: 4.6768, longitude: -74.0487, sourceReference: "https://www.colombia.travel/en/bogota" }),
  pt({ name: "Fundación Santa Fe de Bogotá", pointType: "Medical Campus", city: "Bogotá", submarket: "Bogotá", latitude: 4.7027, longitude: -74.0418, sourceReference: "https://fundacionsantafedebogota.com/" }),
  pt({ name: "Hospital Universitario San Ignacio", pointType: "Medical Campus", city: "Bogotá", submarket: "Bogotá", latitude: 4.6408, longitude: -74.0647, sourceReference: "https://www.husi.org.co/" }),
  pt({ name: "Universidad de los Andes", pointType: "University / College", city: "Bogotá", submarket: "Bogotá", latitude: 4.6013, longitude: -74.0661, sourceReference: "https://uniandes.edu.co/" }),
  pt({ name: "Universidad Nacional de Colombia (Bogotá)", pointType: "University / College", city: "Bogotá", submarket: "Bogotá", latitude: 4.6387, longitude: -74.0841, sourceReference: "https://unal.edu.co/" }),
  pt({ name: "Estadio Nemesio Camacho El Campín", pointType: "Sports Venue", city: "Bogotá", submarket: "Bogotá", latitude: 4.6459, longitude: -74.0785, sourceReference: "https://en.wikipedia.org/wiki/Estadio_El_Camp%C3%ADn" }),
  pt({ name: "Centro Internacional Business District", pointType: "Business District", city: "Bogotá", submarket: "Bogotá", latitude: 4.6113, longitude: -74.0711, sourceReference: "https://www.bogota.gov.co/" }),
  pt({ name: "Calle 100 / Chicó Business Corridor", pointType: "Business District", city: "Bogotá", submarket: "Bogotá", latitude: 4.6774, longitude: -74.0484, sourceReference: "https://www.colombia.travel/en/bogota" }),
  pt({ name: "Salitre Plaza Mixed-Use Hub", pointType: "Mixed-Use Development", city: "Bogotá", submarket: "Bogotá", latitude: 4.6549, longitude: -74.1084, sourceReference: "https://www.salitreplaza.com/" }),
  pt({ name: "Paloquemao Logistics Market Zone", pointType: "Industrial / Logistics Zone", city: "Bogotá", submarket: "Bogotá", latitude: 4.6212, longitude: -74.1029, sourceReference: "https://www.bogota.gov.co/" }),
  pt({ name: "CAN Government Complex (Centro Administrativo Nacional)", pointType: "Government / Civic Center", city: "Bogotá", submarket: "Bogotá", latitude: 4.6488, longitude: -74.0958, sourceReference: "https://www.funcionpublica.gov.co/" }),

  // Medellin (13)
  pt({ name: "Plaza Mayor Medellín Convention Center", pointType: "Convention Center", city: "Medellín", submarket: "Medellín", latitude: 6.2433, longitude: -75.5763, sourceReference: "https://www.plazamayor.com.co/" }),
  pt({ name: "Poblado Entertainment District (Parque Lleras)", pointType: "Entertainment District", city: "Medellín", submarket: "Medellín", latitude: 6.209, longitude: -75.5675, sourceReference: "https://www.colombia.travel/en/medellin" }),
  pt({ name: "Comuna 13 Tourism Corridor", pointType: "Tourist Attraction", city: "Medellín", submarket: "Medellín", latitude: 6.2568, longitude: -75.6232, sourceReference: "https://www.colombia.travel/en/medellin" }),
  pt({ name: "Botero Plaza", pointType: "Tourist Attraction", city: "Medellín", submarket: "Medellín", latitude: 6.2518, longitude: -75.5685, sourceReference: "https://www.medellin.gov.co/" }),
  pt({ name: "Hospital Pablo Tobón Uribe", pointType: "Medical Campus", city: "Medellín", submarket: "Medellín", latitude: 6.2759, longitude: -75.5762, sourceReference: "https://www.hptu.org.co/" }),
  pt({ name: "Hospital Universitario San Vicente Fundación", pointType: "Medical Campus", city: "Medellín", submarket: "Medellín", latitude: 6.2654, longitude: -75.5626, sourceReference: "https://www.sanvicentefundacion.com/" }),
  pt({ name: "Universidad de Antioquia", pointType: "University / College", city: "Medellín", submarket: "Medellín", latitude: 6.2672, longitude: -75.5687, sourceReference: "https://www.udea.edu.co/" }),
  pt({ name: "Universidad EAFIT", pointType: "University / College", city: "Medellín", submarket: "Medellín", latitude: 6.1997, longitude: -75.5781, sourceReference: "https://www.eafit.edu.co/" }),
  pt({ name: "Estadio Atanasio Girardot", pointType: "Sports Venue", city: "Medellín", submarket: "Medellín", latitude: 6.2567, longitude: -75.5906, sourceReference: "https://en.wikipedia.org/wiki/Estadio_Atanasio_Girardot" }),
  pt({ name: "Milla de Oro Business District", pointType: "Business District", city: "Medellín", submarket: "Medellín", latitude: 6.2017, longitude: -75.5719, sourceReference: "https://www.colombia.travel/en/medellin" }),
  pt({ name: "Ruta N Innovation District", pointType: "Mixed-Use Development", city: "Medellín", submarket: "Medellín", latitude: 6.2647, longitude: -75.5666, sourceReference: "https://www.rutanmedellin.org/" }),
  pt({ name: "Zona Franca Rionegro Logistics Hub", pointType: "Industrial / Logistics Zone", city: "Medellín", submarket: "Medellín", latitude: 6.1622, longitude: -75.4248, sourceReference: "https://www.zonafranca.com.co/" }),
  pt({ name: "Alpujarra Administrative Center", pointType: "Government / Civic Center", city: "Medellín", submarket: "Medellín", latitude: 6.2446, longitude: -75.5713, sourceReference: "https://www.medellin.gov.co/" }),

  // Barranquilla (9)
  pt({ name: "Puerta de Oro Convention Center", pointType: "Convention Center", city: "Barranquilla", submarket: "Barranquilla", latitude: 11.0063, longitude: -74.8186, sourceReference: "https://www.puertadeoro.org/" }),
  pt({ name: "Gran Malecón del Río", pointType: "Beach / Waterfront", city: "Barranquilla", submarket: "Barranquilla", latitude: 11.0029, longitude: -74.8075, sourceReference: "https://www.barranquilla.gov.co/" }),
  pt({ name: "Barranquilla Carnival Zone", pointType: "Entertainment District", city: "Barranquilla", submarket: "Barranquilla", latitude: 10.9878, longitude: -74.8019, sourceReference: "https://www.colombia.travel/en/barranquilla" }),
  pt({ name: "Clínica Portoazul Auna", pointType: "Medical Campus", city: "Barranquilla", submarket: "Barranquilla", latitude: 11.0146, longitude: -74.8426, sourceReference: "https://www.auna.org.co/" }),
  pt({ name: "Universidad del Norte", pointType: "University / College", city: "Barranquilla", submarket: "Barranquilla", latitude: 11.0189, longitude: -74.8508, sourceReference: "https://www.uninorte.edu.co/" }),
  pt({ name: "Estadio Metropolitano Roberto Meléndez", pointType: "Sports Venue", city: "Barranquilla", submarket: "Barranquilla", latitude: 10.9263, longitude: -74.8004, sourceReference: "https://en.wikipedia.org/wiki/Estadio_Metropolitano_Roberto_Mel%C3%A9ndez" }),
  pt({ name: "Prado / Alto Prado Business District", pointType: "Business District", city: "Barranquilla", submarket: "Barranquilla", latitude: 10.9994, longitude: -74.8044, sourceReference: "https://www.colombia.travel/en/barranquilla" }),
  pt({ name: "Zona Franca Barranquilla", pointType: "Industrial / Logistics Zone", city: "Barranquilla", submarket: "Barranquilla", latitude: 10.8987, longitude: -74.7835, sourceReference: "https://www.zonafrancabarranquilla.com/" }),
  pt({ name: "Malambo Airport Corridor Growth Node", pointType: "Future Growth Node", city: "Barranquilla", submarket: "Barranquilla", latitude: 10.8913, longitude: -74.7761, sourceReference: "https://www.aerocivil.gov.co/" }),

  // Cali (9)
  pt({ name: "Centro de Eventos Valle del Pacífico", pointType: "Convention Center", city: "Cali", submarket: "Cali", latitude: 3.4475, longitude: -76.4794, sourceReference: "https://www.eventosvalledelpacifico.com/" }),
  pt({ name: "San Antonio Historic District", pointType: "Tourist Attraction", city: "Cali", submarket: "Cali", latitude: 3.4479, longitude: -76.539, sourceReference: "https://www.colombia.travel/en/cali" }),
  pt({ name: "Bulevar del Río Cali", pointType: "Entertainment District", city: "Cali", submarket: "Cali", latitude: 3.4524, longitude: -76.5334, sourceReference: "https://www.cali.gov.co/" }),
  pt({ name: "Fundación Valle del Lili", pointType: "Medical Campus", city: "Cali", submarket: "Cali", latitude: 3.3704, longitude: -76.5314, sourceReference: "https://valledellili.org/" }),
  pt({ name: "Universidad del Valle (Meléndez)", pointType: "University / College", city: "Cali", submarket: "Cali", latitude: 3.3754, longitude: -76.5326, sourceReference: "https://www.univalle.edu.co/" }),
  pt({ name: "Estadio Olímpico Pascual Guerrero", pointType: "Sports Venue", city: "Cali", submarket: "Cali", latitude: 3.4372, longitude: -76.545, sourceReference: "https://en.wikipedia.org/wiki/Estadio_Ol%C3%ADmpico_Pascual_Guerrero" }),
  pt({ name: "Ciudad Jardín Business Corridor", pointType: "Business District", city: "Cali", submarket: "Cali", latitude: 3.3695, longitude: -76.5388, sourceReference: "https://www.cali.gov.co/" }),
  pt({ name: "Yumbo Industrial Zone", pointType: "Industrial / Logistics Zone", city: "Cali", submarket: "Cali", latitude: 3.5821, longitude: -76.4915, sourceReference: "https://www.yumbo.gov.co/" }),
  pt({ name: "Cali Administrative Center (CAM)", pointType: "Government / Civic Center", city: "Cali", submarket: "Cali", latitude: 3.4513, longitude: -76.5342, sourceReference: "https://www.cali.gov.co/" }),

  // Santa Marta (8)
  pt({ name: "Bahía de Santa Marta Waterfront", pointType: "Beach / Waterfront", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.2419, longitude: -74.2053, sourceReference: "https://www.colombia.travel/en/santa-marta" }),
  pt({ name: "Rodadero Beach District", pointType: "Beach / Waterfront", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.2013, longitude: -74.2285, sourceReference: "https://www.colombia.travel/en/santa-marta" }),
  pt({ name: "Parque Tayrona Gateway", pointType: "Tourist Attraction", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.3057, longitude: -74.1112, sourceReference: "https://www.parquesnacionales.gov.co/" }),
  pt({ name: "Quinta de San Pedro Alejandrino", pointType: "Tourist Attraction", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.2407, longitude: -74.1974, sourceReference: "https://www.colombia.travel/en/santa-marta" }),
  pt({ name: "Clínica La Milagrosa Santa Marta", pointType: "Medical Campus", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.2401, longitude: -74.1996, sourceReference: "https://www.clinicalamilagrosa.com/" }),
  pt({ name: "Universidad del Magdalena", pointType: "University / College", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.2257, longitude: -74.1854, sourceReference: "https://www.unimagdalena.edu.co/" }),
  pt({ name: "Puerto de Santa Marta Logistics Zone", pointType: "Industrial / Logistics Zone", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.2468, longitude: -74.2206, sourceReference: "https://www.spsm.com.co/" }),
  pt({ name: "Centro Histórico de Santa Marta", pointType: "Entertainment District", city: "Santa Marta", submarket: "Santa Marta", latitude: 11.2403, longitude: -74.2102, sourceReference: "https://www.colombia.travel/en/santa-marta" }),

  // Coffee Region / Pereira (8)
  pt({ name: "Expofuturo Convention Center", pointType: "Convention Center", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.8219, longitude: -75.7289, sourceReference: "https://www.expofuturo.com/" }),
  pt({ name: "Viaducto César Gaviria Trujillo", pointType: "Tourist Attraction", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.8131, longitude: -75.7078, sourceReference: "https://www.colombia.travel/en/coffee-cultural-landscape" }),
  pt({ name: "Ukumarí Biopark", pointType: "Tourist Attraction", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.8138, longitude: -75.8062, sourceReference: "https://ukumari.org/" }),
  pt({ name: "Clínica Los Rosales Pereira", pointType: "Medical Campus", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.8062, longitude: -75.6944, sourceReference: "https://www.clinicalosrosales.com/" }),
  pt({ name: "Universidad Tecnológica de Pereira", pointType: "University / College", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.7946, longitude: -75.6901, sourceReference: "https://www.utp.edu.co/" }),
  pt({ name: "Estadio Hernán Ramírez Villegas", pointType: "Sports Venue", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.8056, longitude: -75.7368, sourceReference: "https://en.wikipedia.org/wiki/Estadio_Hern%C3%A1n_Ram%C3%ADrez_Villegas" }),
  pt({ name: "Pereira CBD (Centro Financiero)", pointType: "Business District", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.8133, longitude: -75.6961, sourceReference: "https://www.pereira.gov.co/" }),
  pt({ name: "Coffee Landscape Growth Node (Filandia Corridor)", pointType: "Future Growth Node", city: "Pereira", submarket: "Coffee Region / Pereira", latitude: 4.6773, longitude: -75.6588, sourceReference: "https://whc.unesco.org/en/list/1121/" }),

  // San Andres (8)
  pt({ name: "Spratt Bight Beach", pointType: "Beach / Waterfront", city: "San Andrés", submarket: "San Andrés", latitude: 12.5839, longitude: -81.6909, sourceReference: "https://www.colombia.travel/en/san-andres" }),
  pt({ name: "Johnny Cay Regional Park", pointType: "Tourist Attraction", city: "San Andrés", submarket: "San Andrés", latitude: 12.6008, longitude: -81.6881, sourceReference: "https://www.colombia.travel/en/san-andres" }),
  pt({ name: "West View Waterfront", pointType: "Beach / Waterfront", city: "San Andrés", submarket: "San Andrés", latitude: 12.5401, longitude: -81.7351, sourceReference: "https://www.colombia.travel/en/san-andres" }),
  pt({ name: "Hoyo Soplador Coastal Attraction", pointType: "Tourist Attraction", city: "San Andrés", submarket: "San Andrés", latitude: 12.5144, longitude: -81.722, sourceReference: "https://www.colombia.travel/en/san-andres" }),
  pt({ name: "Hospital Departamental Clarence Lynd Newball", pointType: "Medical Campus", city: "San Andrés", submarket: "San Andrés", latitude: 12.5812, longitude: -81.6992, sourceReference: "https://www.sanandres.gov.co/" }),
  pt({ name: "Punta Norte Entertainment Strip", pointType: "Entertainment District", city: "San Andrés", submarket: "San Andrés", latitude: 12.5848, longitude: -81.6918, sourceReference: "https://www.colombia.travel/en/san-andres" }),
  pt({ name: "Muelle Portofino Commercial Pier", pointType: "Mixed-Use Development", city: "San Andrés", submarket: "San Andrés", latitude: 12.5845, longitude: -81.6926, sourceReference: "https://www.colombia.travel/en/san-andres" }),
  pt({ name: "Gustavo Rojas Pinilla Airport Corridor", pointType: "Future Growth Node", city: "San Andrés", submarket: "San Andrés", latitude: 12.5836, longitude: -81.7112, sourceReference: "https://www.aerocivil.gov.co/" }),
];

export const COLOMBIA_DEMAND_ANCHOR_CANDIDATES = COLOMBIA_ALL_CANDIDATES;

export function filterColombiaCandidatesByPhase(phase = 1) {
  const allowed =
    phase === 2
      ? new Set(COLOMBIA_PHASE_2_SUBMARKETS)
      : new Set(COLOMBIA_PHASE_1_SUBMARKETS);
  return COLOMBIA_ALL_CANDIDATES.filter((p) => allowed.has(p.submarket));
}

