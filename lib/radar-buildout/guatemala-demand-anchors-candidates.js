/**
 * Guatemala countrywide demand anchor candidates (source-backed).
 */
import {
  createCentralAmericaCandidateBuilder,
} from "./central-america-country-shared.js";
import {
  applyGuatemalaGovernanceDefaults,
  GUATEMALA_SUBMARKETS,
} from "./guatemala-demand-anchor-governance.js";

const COUNTRY = "Guatemala";

const pt = createCentralAmericaCandidateBuilder(COUNTRY, applyGuatemalaGovernanceDefaults);

export const GUATEMALA_CANDIDATES = [
  pt({ name: "La Aurora International Airport Corridor", pointType: "Future Growth Node", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5833, longitude: -90.5275, sourceReference: "https://www.dgac.gob.gt/", manuallyVerified: true }),
  pt({ name: "Centro Internacional de Convenciones Guatemala", pointType: "Convention Center", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5978, longitude: -90.5123, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "World Trade Center Guatemala", pointType: "Convention Center", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5989, longitude: -90.5089, sourceReference: "https://www.wtcguatemala.com/" }),
  pt({ name: "Zona Viva Business and Hospitality District", pointType: "Business District", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.597, longitude: -90.507, sourceReference: "https://www.visitguatemala.com/", manuallyVerified: true }),
  pt({ name: "Avenida Reforma Corporate Corridor", pointType: "Business District", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.6012, longitude: -90.5189, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Hospital Roosevelt Medical Campus", pointType: "Medical Campus", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5967, longitude: -90.5189, sourceReference: "https://www.hospitaloosevelt.gob.gt/" }),
  pt({ name: "Hospital Herrera Llerandi Medical Campus", pointType: "Medical Campus", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5945, longitude: -90.5145, sourceReference: "https://www.hospitalherrerallerandi.com/" }),
  pt({ name: "Hospital Militar Medical Campus", pointType: "Medical Campus", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.6012, longitude: -90.5123, sourceReference: "https://www.hospitalmilitar.gob.gt/" }),
  pt({ name: "Universidad de San Carlos de Guatemala Central Campus", pointType: "University / College", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5778, longitude: -90.5554, sourceReference: "https://www.usac.edu.gt/" }),
  pt({ name: "Universidad Francisco Marroquín Campus", pointType: "University / College", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.6087, longitude: -90.4965, sourceReference: "https://www.ufm.edu/" }),
  pt({ name: "Universidad del Valle de Guatemala Campus", pointType: "University / College", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5869, longitude: -90.5003, sourceReference: "https://www.uvg.edu.gt/" }),
  pt({ name: "Universidad Galileo Campus", pointType: "University / College", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5978, longitude: -90.5189, sourceReference: "https://www.galileo.edu/" }),
  pt({ name: "Estadio Doroteo Guamuch Flores", pointType: "Sports Venue", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.6223, longitude: -90.5156, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Plaza de la Constitución Historic Civic Core", pointType: "Government / Civic Center", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.6407, longitude: -90.5133, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Palacio Nacional de la Cultura", pointType: "Government / Civic Center", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.6412, longitude: -90.5128, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Oakland Place Mixed-Use Corridor", pointType: "Mixed-Use Development", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5962, longitude: -90.5056, sourceReference: "https://www.oaklandplace.com.gt/" }),
  pt({ name: "Multiplaza Complex Business Zone", pointType: "Mixed-Use Development", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5984, longitude: -90.5067, sourceReference: "https://www.multiplaza.com.gt/" }),
  pt({ name: "Antigua Guatemala UNESCO World Heritage Core", pointType: "Tourist Attraction", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5586, longitude: -90.7344, sourceReference: "https://whc.unesco.org/en/list/65" }),
  pt({ name: "Parque Central Antigua Colonial Plaza", pointType: "Tourist Attraction", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5565, longitude: -90.7337, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Cerro de la Cruz Viewpoint Corridor", pointType: "Tourist Attraction", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5612, longitude: -90.7289, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Casa Santo Domingo Heritage Hotel District", pointType: "Mixed-Use Development", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5578, longitude: -90.7312, sourceReference: "https://www.casasantodomingo.com.gt/" }),
  pt({ name: "Universidad de San Carlos Antigua Campus", pointType: "University / College", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5534, longitude: -90.7356, sourceReference: "https://www.usac.edu.gt/" }),
  pt({ name: "Mercado de Artesanías Tourism Corridor", pointType: "Entertainment District", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5556, longitude: -90.7323, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Museo del Jade Antigua Cultural District", pointType: "Tourist Attraction", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5589, longitude: -90.7312, sourceReference: "https://www.museodeljade.com/" }),
  pt({ name: "Convento de las Capuchinas Heritage Precinct", pointType: "Tourist Attraction", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5567, longitude: -90.7367, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Panajachel Lakeside Tourism Hub", pointType: "Entertainment District", city: "Panajachel", submarket: "Lake Atitlán", latitude: 14.7419, longitude: -91.1532, sourceReference: "https://www.visitguatemala.com/", manuallyVerified: true }),
  pt({ name: "Santiago Atitlán Heritage Town", pointType: "Tourist Attraction", city: "Santiago Atitlán", submarket: "Lake Atitlán", latitude: 14.6361, longitude: -91.2293, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "San Pedro La Laguna Adventure Tourism District", pointType: "Entertainment District", city: "San Pedro La Laguna", submarket: "Lake Atitlán", latitude: 14.6922, longitude: -91.2722, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Santa Catarina Palopó Waterfront", pointType: "Beach / Waterfront", city: "Santa Catarina Palopó", submarket: "Lake Atitlán", latitude: 14.7234, longitude: -91.1189, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "San Marcos La Laguna Wellness Corridor", pointType: "Tourist Attraction", city: "San Marcos La Laguna", submarket: "Lake Atitlán", latitude: 14.7234, longitude: -91.2589, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Hotel Atitlán Marina District", pointType: "Mixed-Use Development", city: "Panajachel", submarket: "Lake Atitlán", latitude: 14.7389, longitude: -91.1567, sourceReference: "https://www.hotelatitlan.com/" }),
  pt({ name: "Santiago Bay Cultural Tourism Anchor", pointType: "Tourist Attraction", city: "Santiago Atitlán", submarket: "Lake Atitlán", latitude: 14.6345, longitude: -91.2312, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Lake Atitlán Volcanic View Corridor", pointType: "Tourist Attraction", city: "Panajachel", submarket: "Lake Atitlán", latitude: 14.7456, longitude: -91.1489, sourceReference: "https://www.inguat.gob.gt/", manuallyVerified: true }),
  pt({ name: "Tikal UNESCO Archaeological Core", pointType: "Tourist Attraction", city: "Tikal", submarket: "Petén / Tikal", latitude: 17.222, longitude: -89.6237, sourceReference: "https://whc.unesco.org/en/list/64", manuallyVerified: true }),
  pt({ name: "Tikal National Park Gateway", pointType: "Future Growth Node", city: "Tikal", submarket: "Petén / Tikal", latitude: 17.2189, longitude: -89.6312, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Flores Petén Island Tourism Hub", pointType: "Entertainment District", city: "Flores", submarket: "Petén / Tikal", latitude: 16.9267, longitude: -89.8922, sourceReference: "https://www.visitguatemala.com/", manuallyVerified: true }),
  pt({ name: "Mundo Maya International Airport Corridor", pointType: "Future Growth Node", city: "Flores", submarket: "Petén / Tikal", latitude: 16.9138, longitude: -89.8664, sourceReference: "https://www.dgac.gob.gt/", manuallyVerified: true }),
  pt({ name: "Yaxhá Archaeological Site", pointType: "Tourist Attraction", city: "Petén", submarket: "Petén / Tikal", latitude: 17.0734, longitude: -89.4012, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "El Petén Rainforest Eco-Tourism Corridor", pointType: "Tourist Attraction", city: "Petén", submarket: "Petén / Tikal", latitude: 16.9123, longitude: -89.8934, sourceReference: "https://www.inguat.gob.gt/" }),
  pt({ name: "Maya Biosphere Reserve Gateway", pointType: "Tourist Attraction", city: "Petén", submarket: "Petén / Tikal", latitude: 17.4567, longitude: -89.6234, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Pacaya Volcano Active Tourism Anchor", pointType: "Tourist Attraction", city: "San Vicente Pacaya", submarket: "Other", latitude: 14.3814, longitude: -90.6017, sourceReference: "https://www.visitguatemala.com/", manuallyVerified: true }),
  pt({ name: "Chichicastenango Indigenous Market Heritage Site", pointType: "Tourist Attraction", city: "Chichicastenango", submarket: "Other", latitude: 14.9431, longitude: -91.1112, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Quetzaltenango Xela Business and University Hub", pointType: "Business District", city: "Quetzaltenango", submarket: "Other", latitude: 14.8347, longitude: -91.5181, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Livingston Garifuna Caribbean Coast", pointType: "Beach / Waterfront", city: "Livingston", submarket: "Other", latitude: 15.8289, longitude: -88.7512, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Río Dulce Cruise and Eco-Tourism Gateway", pointType: "Mixed-Use Development", city: "Río Dulce", submarket: "Other", latitude: 15.6567, longitude: -88.9912, sourceReference: "https://www.visitguatemala.com/" }),
  pt({ name: "Semuc Champey Natural Attraction", pointType: "Tourist Attraction", city: "Lanquín", submarket: "Other", latitude: 15.5345, longitude: -89.9612, sourceReference: "https://www.visitguatemala.com/" }),
];

export function getGuatemalaCandidates() {
  return GUATEMALA_CANDIDATES;
}

export { GUATEMALA_SUBMARKETS };
