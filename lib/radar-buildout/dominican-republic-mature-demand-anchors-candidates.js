/**
 * Dominican Republic Mature Pass demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  applyDominicanRepublicMatureGovernanceDefaults,
  DOMINICAN_REPUBLIC_MATURE_SUBMARKETS,
} from "./dominican-republic-mature-demand-anchor-governance.js";

const COUNTRY = "Dominican Republic";
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, applyDominicanRepublicMatureGovernanceDefaults);

export const DOMINICAN_REPUBLIC_MATURE_CANDIDATES = [
  pt({ name: "Hard Rock Hotel & Casino Punta Cana", pointType: "Entertainment District", city: "Bávaro", submarket: "Punta Cana / Bávaro / Cap Cana", latitude: 18.6789, longitude: -68.4123, sourceReference: "https://www.hardrockhotelpuntacana.com/" }),
  pt({ name: "Arena del Caribe Convention Center", pointType: "Convention Center", city: "Punta Cana", submarket: "Punta Cana / Bávaro / Cap Cana", latitude: 18.5512, longitude: -68.3812, sourceReference: "https://www.arenadelcaribe.com/" }),
  pt({ name: "Macao Beach Surf and Eco Corridor", pointType: "Beach / Waterfront", city: "Macao", submarket: "Punta Cana / Bávaro / Cap Cana", latitude: 18.7612, longitude: -68.5312, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Uvero Alto Beach Resort Growth Zone", pointType: "Future Growth Node", city: "Uvero Alto", submarket: "Punta Cana / Bávaro / Cap Cana", latitude: 18.8312, longitude: -68.6012, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Verón Town Commercial Hub", pointType: "Business District", city: "Verón", submarket: "Punta Cana / Bávaro / Cap Cana", latitude: 18.5712, longitude: -68.3812, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Galería 360 Shopping and Entertainment", pointType: "Entertainment District", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4812, longitude: -69.9412, sourceReference: "https://www.galeria360.com/" }),
  pt({ name: "Parque Mirador del Este", pointType: "Tourist Attraction", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4712, longitude: -69.8512, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Faro a Colón — Columbus Lighthouse", pointType: "Tourist Attraction", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4789, longitude: -69.8689, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Zona Colonial — Conde Gate and Ramparts", pointType: "Tourist Attraction", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4767, longitude: -69.8834, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Gazcue Historic Residential District", pointType: "Tourist Attraction", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4689, longitude: -69.9012, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Bella Vista Corporate Corridor", pointType: "Business District", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4612, longitude: -69.9312, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Instituto Tecnológico de Santo Domingo (INTEC)", pointType: "University / College", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4889, longitude: -69.9612, sourceReference: "https://www.intec.edu.do/" }),
  pt({ name: "Hospital General de la Plaza de la Cultura", pointType: "Medical Campus", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4712, longitude: -69.9089, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Palacio de los Deportes Virgilio Travieso Soto", pointType: "Sports Venue", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4812, longitude: -69.9012, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Playa Sosúa Beach and Snorkel Coast", pointType: "Beach / Waterfront", city: "Sosúa", submarket: "Puerto Plata / Sosúa / Cabarete", latitude: 19.7512, longitude: -70.5189, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Malecón de Puerto Plata", pointType: "Beach / Waterfront", city: "Puerto Plata", submarket: "Puerto Plata / Sosúa / Cabarete", latitude: 19.8012, longitude: -70.6912, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Teleférico Puerto Plata — Isabel de Torres", pointType: "Tourist Attraction", city: "Puerto Plata", submarket: "Puerto Plata / Sosúa / Cabarete", latitude: 19.7612, longitude: -70.7012, sourceReference: "https://www.telefericopuertoplata.com/" }),
  pt({ name: "27 Charcos de Damajagua Adventure Park", pointType: "Tourist Attraction", city: "Puerto Plata", submarket: "Puerto Plata / Sosúa / Cabarete", latitude: 19.6312, longitude: -70.8312, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "La Isabela Historic Settlement", pointType: "Tourist Attraction", city: "Puerto Plata", submarket: "Puerto Plata / Sosúa / Cabarete", latitude: 19.8912, longitude: -71.0812, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Dominicus Beach — La Romana South", pointType: "Beach / Waterfront", city: "Dominicus", submarket: "La Romana / Bayahibe", latitude: 18.3612, longitude: -68.8012, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Isla Catalina Day-Trip Marina", pointType: "Beach / Waterfront", city: "La Romana", submarket: "La Romana / Bayahibe", latitude: 18.3512, longitude: -68.8312, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Teeth of the Dog Golf Course", pointType: "Sports Venue", city: "La Romana", submarket: "La Romana / Bayahibe", latitude: 18.4212, longitude: -68.9112, sourceReference: "https://www.casadecampo.com.do/" }),
  pt({ name: "Bahía de Samaná Waterfront Promenade", pointType: "Beach / Waterfront", city: "Samaná", submarket: "Samaná / Las Terrenas", latitude: 19.2067, longitude: -69.3356, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Cayo Levantado Island Resort", pointType: "Mixed-Use Development", city: "Samaná", submarket: "Samaná / Las Terrenas", latitude: 19.1789, longitude: -69.3012, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "El Limón Waterfall Eco-Attraction", pointType: "Tourist Attraction", city: "Samaná", submarket: "Samaná / Las Terrenas", latitude: 19.3012, longitude: -69.4512, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Pueblo de los Pescadores — Las Terrenas", pointType: "Entertainment District", city: "Las Terrenas", submarket: "Samaná / Las Terrenas", latitude: 19.3212, longitude: -69.5312, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Centro León Cultural Center", pointType: "Convention Center", city: "Santiago", submarket: "Santiago / Cibao", latitude: 19.4512, longitude: -70.6912, sourceReference: "https://www.centroleon.org.do/" }),
  pt({ name: "Aeropuerto Internacional del Cibao Corridor", pointType: "Future Growth Node", city: "Santiago", submarket: "Santiago / Cibao", latitude: 19.4061, longitude: -70.6047, sourceReference: "https://www.aeropuertocibao.com.do/", manuallyVerified: true }),
  pt({ name: "Plaza Internacional Santiago Commercial Hub", pointType: "Business District", city: "Santiago", submarket: "Santiago / Cibao", latitude: 19.4489, longitude: -70.7089, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Jarabacoa River Rafting and Adventure Hub", pointType: "Tourist Attraction", city: "Jarabacoa", submarket: "Jarabacoa / Constanza", latitude: 19.1212, longitude: -70.6412, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Salto de Jimenoa Waterfall", pointType: "Tourist Attraction", city: "Jarabacoa", submarket: "Jarabacoa / Constanza", latitude: 19.1012, longitude: -70.6212, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Pico Duarte Trailhead — La Ciénaga", pointType: "Tourist Attraction", city: "Jarabacoa", submarket: "Jarabacoa / Constanza", latitude: 18.9512, longitude: -70.8812, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Four Points by Sheraton Puntacana Village", pointType: "Mixed-Use Development", city: "Punta Cana", submarket: "Punta Cana / Bávaro / Cap Cana", latitude: 18.5612, longitude: -68.3712, sourceReference: "https://www.marriott.com/" }),
  pt({ name: "Laguna Redonda and Laguna Limón Eco-Zone", pointType: "Tourist Attraction", city: "Miches", submarket: "Miches / Costa Esmeralda", latitude: 18.9612, longitude: -69.0712, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Barahona City Waterfront and Marina", pointType: "Beach / Waterfront", city: "Barahona", submarket: "Barahona / Pedernales", latitude: 18.2089, longitude: -71.1012, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Los Patos Beach — Barahona", pointType: "Beach / Waterfront", city: "Barahona", submarket: "Barahona / Pedernales", latitude: 18.1512, longitude: -71.1212, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Pedernales Border Tourism Gateway", pointType: "Future Growth Node", city: "Pedernales", submarket: "Barahona / Pedernales", latitude: 18.0389, longitude: -71.7412, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Andrés Boca Chica Beach Club Strip", pointType: "Entertainment District", city: "Boca Chica", submarket: "Boca Chica / Juan Dolio", latitude: 18.4512, longitude: -69.6112, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Autopista Las Américas — SDQ Airport Corridor", pointType: "Future Growth Node", city: "Santo Domingo", submarket: "Boca Chica / Juan Dolio", latitude: 18.4312, longitude: -69.6712, sourceReference: "https://www.godominicanrepublic.com/", manuallyVerified: true }),
  pt({ name: "Higüey Basílica de la Altagracia", pointType: "Tourist Attraction", city: "Higüey", submarket: "Other", latitude: 18.615, longitude: -68.7089, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "San Pedro de Macorís Sugar Heritage City", pointType: "Tourist Attraction", city: "San Pedro de Macorís", submarket: "Other", latitude: 18.4512, longitude: -69.3012, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "La Romana International Airport Corridor", pointType: "Future Growth Node", city: "La Romana", submarket: "La Romana / Bayahibe", latitude: 18.4512, longitude: -68.9118, sourceReference: "https://www.godominicanrepublic.com/", manuallyVerified: true }),
  pt({ name: "Puerto Plata Gregorio Luperón Airport Corridor", pointType: "Future Growth Node", city: "Puerto Plata", submarket: "Puerto Plata / Sosúa / Cabarete", latitude: 19.7579, longitude: -70.57, sourceReference: "https://www.godominicanrepublic.com/", manuallyVerified: true }),
  pt({ name: "Constanza Highland Vegetable Basket Tourism", pointType: "Tourist Attraction", city: "Constanza", submarket: "Jarabacoa / Constanza", latitude: 18.9089, longitude: -70.7412, sourceReference: "https://www.godominicanrepublic.com/" }),
  pt({ name: "Nagua North Coast Beach Gateway", pointType: "Beach / Waterfront", city: "Nagua", submarket: "Other", latitude: 19.3812, longitude: -69.8512, sourceReference: "https://www.godominicanrepublic.com/" }),
];

export function getDominicanRepublicMatureCandidates() {
  return DOMINICAN_REPUBLIC_MATURE_CANDIDATES;
}

export { DOMINICAN_REPUBLIC_MATURE_SUBMARKETS };
