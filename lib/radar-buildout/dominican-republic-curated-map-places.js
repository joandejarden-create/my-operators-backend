/**
 * Curated demand anchor names + coordinates (Google Maps / official sources).
 * Used when geocoder rate-limits or returns no match.
 * source: google | official | osm
 */

/** @type {Record<string, { name?: string, latitude: number, longitude: number, city?: string, address?: string, source?: string }>} */
export const DR_CURATED_MAP_PLACES = {
  "Hospiten Bávaro": {
    name: "Hospiten Bávaro",
    latitude: 18.5989,
    longitude: -68.4143,
    city: "Punta Cana",
    address: "Carretera Higüey - Punta Cana km 106, Verón",
    source: "google",
  },
  "Bahía Príncipe Grand Samaná": {
    name: "Bahía Príncipe Grand Samaná",
    latitude: 19.1845,
    longitude: -69.2702,
    city: "Santa Bárbara de Samaná",
    address: "Carretera Samaná-Las Galeras, Los Cacaos",
    source: "google",
  },
  "Barceló Bávaro Convention Center": {
    latitude: 18.6861,
    longitude: -68.4497,
    city: "Bávaro",
    address: "Carretera Bávaro Km 1",
    source: "google",
  },
  "BlueMall Punta Cana": {
    latitude: 18.557,
    longitude: -68.3831,
    city: "Punta Cana",
    source: "google",
  },
  "ICC Punta Cana — International Convention Center": {
    latitude: 18.5425,
    longitude: -68.3769,
    city: "Bávaro",
    address: "Blvd. Turístico del Este, Bávaro",
    source: "google",
  },
  "Acropolis Convention Center": {
    latitude: 18.4612,
    longitude: -69.9408,
    city: "Santo Domingo",
    address: "Av. Winston Churchill",
    source: "google",
  },
  "Centro Internacional de Ferias y Congresos (CIDAC)": {
    latitude: 18.4872,
    longitude: -69.9594,
    city: "Santo Domingo",
    source: "google",
  },
  "Hotel El Embajador Convention Center": {
    latitude: 18.4689,
    longitude: -69.9425,
    city: "Santo Domingo",
    source: "google",
  },
  "Fiesta Resort Convention & Casino": {
    latitude: 18.436,
    longitude: -69.43,
    city: "Juan Dolio",
    source: "google",
  },
  "Scape Park at Cap Cana": {
    latitude: 18.4841,
    longitude: -68.4409,
    city: "Cap Cana",
    source: "google",
  },
  "Ocean World Adventure Park": {
    latitude: 19.731,
    longitude: -70.655,
    city: "Cofresí",
    source: "google",
  },
  "Montaña Redonda": {
    latitude: 18.9823,
    longitude: -68.9168,
    city: "Miches",
    source: "google",
  },
  "Zemi Miches All-Inclusive Resort": {
    latitude: 18.988,
    longitude: -69.041,
    city: "Miches",
    source: "google",
  },
  "Cabarete Kite Beach": {
    latitude: 19.7497,
    longitude: -70.4086,
    city: "Cabarete",
    source: "google",
  },
  "Universidad Autónoma de Santo Domingo (UASD)": {
    latitude: 18.4613,
    longitude: -69.9168,
    city: "Santo Domingo",
    address: "Av. Alma Mater, Ciudad Universitaria",
    source: "google",
  },
  "Parque Nacional Jaragua": {
    latitude: 17.7825,
    longitude: -71.6686,
    city: "Pedernales",
    source: "google",
  },
  "Guayacanes Beach": {
    latitude: 18.4194,
    longitude: -69.4558,
    city: "Guayacanes",
    source: "google",
  },
  "Estadio Quisqueya Juan Marichal": {
    latitude: 18.4886,
    longitude: -69.9262,
    city: "Santo Domingo",
    source: "google",
  },
  "Teatro Nacional Eduardo Brito — Plaza de la Cultura": {
    latitude: 18.4709,
    longitude: -69.9109,
    city: "Santo Domingo",
    source: "google",
  },
  "Universidad Iberoamericana (UNIBE)": {
    latitude: 18.4589,
    longitude: -69.9422,
    city: "Santo Domingo",
    source: "google",
  },
  "PUCMM Campus Santiago (Cibao)": {
    latitude: 19.4456,
    longitude: -70.6867,
    city: "Santiago",
    source: "google",
  },
  "Santiago Monument — Monumento a los Héroes de la Restauración": {
    latitude: 19.4517,
    longitude: -70.697,
    city: "Santiago",
    source: "google",
  },
  "Rancho Baiguate — Jarabacoa": {
    latitude: 19.11,
    longitude: -70.64,
    city: "Jarabacoa",
    source: "google",
  },
  "Metro Country Club — Juan Dolio": {
    latitude: 18.435,
    longitude: -69.425,
    city: "Juan Dolio",
    source: "google",
  },
  "Las Galeras Village & Beach": {
    latitude: 19.2924,
    longitude: -69.1971,
    city: "Las Galeras",
    source: "google",
  },
  "Lago Enriquillo — Ecotourism Anchor": {
    latitude: 18.5626,
    longitude: -71.6978,
    city: "La Descubierta",
    source: "google",
  },
  "Parque Colón — Colonial Zone": {
    latitude: 18.473,
    longitude: -69.8847,
    city: "Santo Domingo",
    source: "google",
  },
  "Ciudad Colonial — UNESCO World Heritage Site": {
    latitude: 18.4732,
    longitude: -69.8848,
    city: "Santo Domingo",
    source: "google",
  },
};
