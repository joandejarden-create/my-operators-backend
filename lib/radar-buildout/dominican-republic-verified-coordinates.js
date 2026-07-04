/**

 * Verified coordinates for ALL Dominican Republic radar points (98 records).

 * Beach/waterfront points use on-shore coordinates (not offshore centroids).

 * Sources: OpenStreetMap Nominatim, Wikipedia, ICAO, godominicanrepublic.com, official venue sites.

 * Last OSM audit: 2026-06-17 (demand anchors full pass).

 */



/** @type {Record<string, { latitude: number, longitude: number, city?: string, sourceReference?: string }>} */

export const DR_VERIFIED_COORDINATES = {

  // ── Punta Cana / Bávaro / Cap Cana ──

  "Bávaro Beach Resort Coastline": { latitude: 18.693, longitude: -68.445, city: "Bávaro" },

  "Cap Cana Marina": { latitude: 18.5025, longitude: -68.3839, city: "Cap Cana", sourceReference: "https://www.capcana.com/marina/" },

  "Juanillo Beach — Cap Cana": { latitude: 18.4997, longitude: -68.3781, city: "Cap Cana" },

  "ICC Punta Cana — International Convention Center": { latitude: 18.5425, longitude: -68.3769, city: "Bávaro", sourceReference: "https://iccpuntacana.com/" },

  "Barceló Bávaro Convention Center": { latitude: 18.6861, longitude: -68.4497, city: "Bávaro" },

  "PUNTACANA Resort & Club": { latitude: 18.5365, longitude: -68.3669, city: "Punta Cana", sourceReference: "https://www.puntacana.com/" },

  "BlueMall Punta Cana": { latitude: 18.557, longitude: -68.3831, city: "Punta Cana" },

  "Downtown Punta Cana": { latitude: 18.637, longitude: -68.3976, city: "Punta Cana" },

  "Scape Park at Cap Cana": { latitude: 18.4841, longitude: -68.4409, city: "Cap Cana" },

  "Hospiten Bávaro": { latitude: 18.5989, longitude: -68.4143, city: "Punta Cana", sourceReference: "https://hospiten.com/en/hospitals/hospiten-bavaro" },

  "Coco Bongo Punta Cana": { latitude: 18.6352, longitude: -68.395, city: "Punta Cana" },

  "Punta Cana International Airport": { latitude: 18.5674, longitude: -68.3634, city: "Punta Cana" },

  "Punta Cana Cruise Port": { latitude: 18.5542, longitude: -68.3735, city: "Punta Cana" },

  "Autopista del Coral — Punta Cana Corridor Access": { latitude: 18.615, longitude: -68.452, city: "Higüey" },

  "Expreso Bavaro Bus Terminal": { latitude: 18.5595, longitude: -68.3728, city: "Verón" },



  // ── Santo Domingo Metro ──

  "Ciudad Colonial — UNESCO World Heritage Site": { latitude: 18.4732, longitude: -69.8848, city: "Santo Domingo" },

  "Los Tres Ojos National Park": { latitude: 18.48, longitude: -69.843, city: "Santo Domingo Este" },

  "Malecón de Santo Domingo": { latitude: 18.4582, longitude: -69.9096, city: "Santo Domingo" },

  "Piantini Business District": { latitude: 18.4758, longitude: -69.9365, city: "Santo Domingo" },

  "Parque Colón — Colonial Zone": { latitude: 18.473, longitude: -69.8847, city: "Santo Domingo" },

  "Centro de los Héroes Government Complex": { latitude: 18.4536, longitude: -69.9092, city: "Santo Domingo" },

  "Universidad Autónoma de Santo Domingo (UASD)": { latitude: 18.4613, longitude: -69.9168, city: "Santo Domingo" },

  "Pontificia Universidad Católica Madre y Maestra — Santo Domingo": { latitude: 18.4633, longitude: -69.9297, city: "Santo Domingo" },

  "Teatro Nacional Eduardo Brito — Plaza de la Cultura": { latitude: 18.4709, longitude: -69.9109, city: "Santo Domingo" },

  "Hospital General de la Plaza de la Salud": { latitude: 18.4888, longitude: -69.9219, city: "Santo Domingo" },

  "Clínica Abreu — Santo Domingo": { latitude: 18.4669, longitude: -69.8947, city: "Santo Domingo" },

  "Sambil Santo Domingo": { latitude: 18.483, longitude: -69.9119, city: "Santo Domingo" },

  "Centro Internacional de Ferias y Congresos (CIDAC)": { latitude: 18.4872, longitude: -69.9594, city: "Santo Domingo" },

  "Calle El Conde — Colonial Zone Entertainment Corridor": { latitude: 18.4735, longitude: -69.8855, city: "Santo Domingo" },

  "Agora Mall — Santo Domingo": { latitude: 18.4828, longitude: -69.94, city: "Santo Domingo" },

  "Avenida Winston Churchill Business Corridor": { latitude: 18.4755, longitude: -69.9385, city: "Santo Domingo" },

  "Acropolis Convention Center": { latitude: 18.4612, longitude: -69.9408, city: "Santo Domingo" },

  "Hotel El Embajador Convention Center": { latitude: 18.4689, longitude: -69.9425, city: "Santo Domingo" },

  "Estadio Quisqueya Juan Marichal": { latitude: 18.4886, longitude: -69.9262, city: "Santo Domingo" },

  "Universidad Iberoamericana (UNIBE)": { latitude: 18.4589, longitude: -69.9422, city: "Santo Domingo" },

  "Caribe Tours Terminal — Santo Domingo": { latitude: 18.4756, longitude: -69.9125, city: "Santo Domingo" },

  "Port of Sans Souci — Santo Domingo": { latitude: 18.4485, longitude: -69.882, city: "Santo Domingo" },

  "Santo Domingo Cruise Port": { latitude: 18.4485, longitude: -69.882, city: "Santo Domingo" },

  "Barceló Convention Center": { latitude: 18.4569, longitude: -69.9412, city: "Santo Domingo" },

  "Las Américas International Airport": { latitude: 18.4297, longitude: -69.6689, city: "Santo Domingo" },

  "Santo Domingo Las Américas Airport": { latitude: 18.4297, longitude: -69.6689, city: "Santo Domingo" },

  "La Isabela International Airport": { latitude: 18.5725, longitude: -69.9856, city: "Santo Domingo" },

  "Los Llanos de Sabanatosa Airport": { latitude: 18.5964, longitude: -69.5258, city: "Sabaná de la Mar" },



  // ── Boca Chica / Juan Dolio (south coast — on-shore beach coords) ──

  "Boca Chica Beach": { latitude: 18.4509, longitude: -69.6059, city: "Boca Chica", sourceReference: "https://www.beachatlas.com/boca-chica-2" },

  "Playa Juan Dolio": { latitude: 18.4251, longitude: -69.4215, city: "Juan Dolio", sourceReference: "https://www.godominicanrepublic.com/beaches/juan-dolio" },

  "Guayacanes Beach": { latitude: 18.4205, longitude: -69.4511, city: "Guayacanes", sourceReference: "https://www.beachesontheair.com/beaches/playa-guayacanes" },

  "Fiesta Resort Convention & Casino": { latitude: 18.436, longitude: -69.43, city: "Juan Dolio" },

  "Metro Country Club — Juan Dolio": { latitude: 18.435, longitude: -69.425, city: "Juan Dolio" },

  "Caucedo Multimodal Port Logistics Zone": { latitude: 18.4242, longitude: -69.6167, city: "Boca Chica" },

  "DP World Caucedo Multimodal Port": { latitude: 18.4242, longitude: -69.6167, city: "Boca Chica" },



  // ── Puerto Plata / Sosúa / Cabarete ──

  "Playa Dorada Resort Coast": { latitude: 19.7652, longitude: -70.6134, city: "Puerto Plata" },

  "Amber Cove Cruise Port": { latitude: 19.8339, longitude: -70.7758, city: "Maimón" },

  "Fortaleza San Felipe": { latitude: 19.8042, longitude: -70.695, city: "Puerto Plata" },

  "Cabarete Kite Beach": { latitude: 19.7497, longitude: -70.4086, city: "Cabarete" },

  "Ocean World Adventure Park": { latitude: 19.731, longitude: -70.655, city: "Cofresí" },

  "Puerto Plata City Center": { latitude: 19.7934, longitude: -70.6884, city: "Puerto Plata" },

  "Gregorio Luperon International Airport": { latitude: 19.7579, longitude: -70.57, city: "Puerto Plata" },

  "Puerto Plata Cruise Port": { latitude: 19.8339, longitude: -70.7758, city: "Maimón" },

  "Puerto Plata Maritime Support Port — Maimón": { latitude: 19.831, longitude: -70.731, city: "Maimón" },



  // ── La Romana / Bayahibe ──

  "Altos de Chavón": { latitude: 18.4213, longitude: -68.8917, city: "La Romana" },

  "Bayahibe Beach": { latitude: 18.3727, longitude: -68.842, city: "Bayahibe" },

  "Bayahibe Marina — Saona Island Departures": { latitude: 18.3705, longitude: -68.8345, city: "Bayahibe" },

  "Casa de Campo Resort & Villas": { latitude: 18.4142, longitude: -68.9355, city: "La Romana" },

  "La Romana Cruise Port": { latitude: 18.4273, longitude: -68.9728, city: "La Romana" },

  "Central Romana Industrial Zone": { latitude: 18.418, longitude: -68.965, city: "La Romana" },

  "Casa De Campo International Airport": { latitude: 18.4507, longitude: -68.9108, city: "La Romana" },



  // ── Samaná / Las Terrenas (on peninsula — not in bay water) ──

  "Las Terrenas Beach": { latitude: 19.323, longitude: -69.5415, city: "Las Terrenas" },

  "Samaná Bay — Whale Watching Corridor": { latitude: 19.2065, longitude: -69.3358, city: "Samaná" },

  "Playa Rincón": { latitude: 19.2833, longitude: -69.2565, city: "Rincón" },

  "Las Galeras Village & Beach": { latitude: 19.2924, longitude: -69.1971, city: "Las Galeras" },

  "Bahía Príncipe Grand Samaná": { latitude: 19.1845, longitude: -69.2702, city: "Santa Bárbara de Samaná" },

  "Samaná Peninsula Eco-Tourism Corridor": { latitude: 19.298, longitude: -69.452, city: "Las Terrenas" },

  "Sabana de la Mar Ferry Terminal": { latitude: 18.976, longitude: -69.409, city: "Sabana de la Mar" },

  "Santa Bárbara de Samaná Maritime Terminal": { latitude: 19.206, longitude: -69.3355, city: "Samaná" },

  "Samaná El Catey International Airport": { latitude: 19.2693, longitude: -69.7374, city: "El Catey" },



  // ── Santiago / Cibao ──

  "Santiago Monument — Monumento a los Héroes de la Restauración": { latitude: 19.4517, longitude: -70.697, city: "Santiago" },

  "PUCMM Campus Santiago (Cibao)": { latitude: 19.4456, longitude: -70.6867, city: "Santiago" },

  "Hospital Metropolitano de Santiago (HOMS)": { latitude: 19.4358, longitude: -70.6607, city: "Santiago" },

  "Estadio Cibao": { latitude: 19.4673, longitude: -70.7088, city: "Santiago" },

  "Zona Franca Industrial Santiago": { latitude: 19.4739, longitude: -70.7334, city: "Santiago" },

  "Santiago Business District — Centro Histórico": { latitude: 19.4505, longitude: -70.692, city: "Santiago" },

  "Plaza Lama Santiago Commercial Hub": { latitude: 19.456, longitude: -70.701, city: "Santiago" },

  "Cibao International Airport": { latitude: 19.4061, longitude: -70.6047, city: "Santiago" },

  "Autopista Duarte (DR-1) — Santiago Access Node": { latitude: 19.42, longitude: -70.68, city: "Santiago" },



  // ── Miches / Costa Esmeralda ──

  "Playa Esmeralda — Miches": { latitude: 19.0189, longitude: -69.0034, city: "Miches" },

  "Zemi Miches All-Inclusive Resort": { latitude: 18.988, longitude: -69.041, city: "Miches" },

  "Montaña Redonda": { latitude: 18.9823, longitude: -68.9168, city: "Miches" },

  "Costa Esmeralda Resort Growth Corridor": { latitude: 19.0261, longitude: -68.9537, city: "Miches" },



  // ── Barahona / Pedernales ──

  "Bahía de las Águilas": { latitude: 17.8305, longitude: -71.6278, city: "Pedernales" },

  "Parque Nacional Jaragua": { latitude: 17.7825, longitude: -71.6686, city: "Pedernales" },

  "Cabo Rojo / Pedernales Tourism Growth Zone": { latitude: 17.9, longitude: -71.667, city: "Pedernales", sourceReference: "https://en.wikipedia.org/wiki/Cabo_Rojo,_Dominican_Republic" },

  "Carretera Sánchez — Barahona Coastal Highway Access": { latitude: 18.208, longitude: -71.108, city: "Barahona" },

  "Maria Montez International Airport": { latitude: 18.2515, longitude: -71.1204, city: "Barahona" },

  "Lago Enriquillo — Ecotourism Anchor": { latitude: 18.5626, longitude: -71.6978, city: "La Descubierta" },



  // ── Jarabacoa / Constanza ──

  "Jarabacoa Mountain Valley Tourism Hub": { latitude: 19.1167, longitude: -70.6333, city: "Jarabacoa" },

  "Constanza Highland — Valle Nuevo Gateway": { latitude: 18.9167, longitude: -70.7333, city: "Constanza" },

  "Rancho Baiguate — Jarabacoa": { latitude: 19.11, longitude: -70.64, city: "Jarabacoa" },

};



export function coordDistanceKm(name, lat, lng, verified) {

  const v = verified || DR_VERIFIED_COORDINATES[name];

  if (!v) return null;

  const R = 6371;

  const dLat = ((v.latitude - lat) * Math.PI) / 180;

  const dLng = ((v.longitude - lng) * Math.PI) / 180;

  const a =

    Math.sin(dLat / 2) ** 2 +

    Math.cos((lat * Math.PI) / 180) *

      Math.cos((v.latitude * Math.PI) / 180) *

      Math.sin(dLng / 2) ** 2;

  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

}



export function listUnverifiedDrPointNames(names) {

  return (names || []).filter((n) => !DR_VERIFIED_COORDINATES[n]);

}


