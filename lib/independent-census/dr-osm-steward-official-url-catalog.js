/**
 * High-confidence Official Property URL + City catalog for DR OSM steward holds.
 * Keys = OSM source_record_id (node/… or way/…).
 * Only brand-domain property pages — never OTA / homepage-only invents.
 */

export const DR_OSM_STEWARD_OFFICIAL_URL_CATALOG = Object.freeze({
  // Hyatt Inclusive / Dreams / Breathless
  "node/287032841": {
    // Dreams Palm Beach Resort (Punta Cana)
    url: "https://www.hyattinclusivecollection.com/en/resorts-hotels/dreams/dominican-republic/palm-beach-punta-cana/",
    city: "Punta Cana",
    brand_hosts: ["hyattinclusivecollection.com", "dreamsresorts.com", "hyatt.com"],
  },
  "way/335856784": {
    url: "https://www.hyattinclusivecollection.com/en/resorts-hotels/dreams/dominican-republic/punta-cana-resort-spa/",
    city: "Punta Cana",
    brand_hosts: ["hyattinclusivecollection.com", "dreamsresorts.com", "hyatt.com"],
  },
  "node/3287789561": {
    url: "https://www.hyattinclusivecollection.com/en/resorts-hotels/breathless/dominican-republic/punta-cana-resort-spa/",
    city: "Punta Cana",
    brand_hosts: ["hyattinclusivecollection.com", "breathlessresorts.com", "hyatt.com"],
  },

  // Be Live (named properties only — skip generic "Be Live" OSM nodes)
  "node/302771929": {
    url: "https://www.belivehotels.com/en/hotels-punta-cana/be-live-collection-puntacana/",
    city: "Bávaro",
    brand_hosts: ["belivehotels.com"],
  },
  "node/1207422127": {
    url: "https://www.belivehotels.com/en/hotels-puerto-plata/be-live-collection-marien/",
    city: "Puerto Plata",
    brand_hosts: ["belivehotels.com"],
  },

  // Club Med OSM name is generic ("Club Med") — leave steward (Cap Cana vs Miches ambiguity).

  // Breezes Puerto Plata (SuperClubs)
  "node/270997923": {
    url: "https://www.breezes.com/resorts/breezes-puerto-plata/",
    city: "Puerto Plata",
    brand_hosts: ["breezes.com", "superclubs.com"],
  },

  // Meliá
  "node/4268870091": {
    url: "https://www.melia.com/en/hotels/dominican-republic/punta-cana/melia-caribe-beach-resort",
    city: "Punta Cana",
    brand_hosts: ["melia.com"],
  },
  "way/272974124": {
    url: "https://www.melia.com/en/hotels/dominican-republic/santo-domingo/melia-santo-domingo",
    city: "Santo Domingo",
    brand_hosts: ["melia.com"],
  },

  // Bahía Príncipe
  "node/4660748798": {
    url: "https://www.bahia-principe.com/en/resorts-in-dominican-republic/resort-bavaro/",
    city: "Bávaro",
    brand_hosts: ["bahia-principe.com"],
  },
  "way/175357851": {
    url: "https://www.bahia-principe.com/en/resorts-in-dominican-republic/resort-cayacoa/",
    city: "Samaná",
    brand_hosts: ["bahia-principe.com"],
  },
  "way/342786288": {
    url: "https://www.bahia-principe.com/en/resorts-in-dominican-republic/resort-playa-nueva-romana/",
    city: "La Romana",
    brand_hosts: ["bahia-principe.com"],
  },

  // Catalonia
  "node/6314999313": {
    url: "https://www.cataloniahotels.com/en/hotel/catalonia-bavaro-beach",
    city: "Bávaro",
    brand_hosts: ["cataloniahotels.com"],
  },

  // Barceló / Occidental
  "way/34879211": {
    url: "https://www.barcelo.com/en-us/barcelo-bavaro-palace/",
    city: "Bávaro",
    brand_hosts: ["barcelo.com"],
  },
  "node/2998191668": {
    url: "https://www.barcelo.com/en-us/barcelo-puerto-plata/",
    city: "Puerto Plata",
    brand_hosts: ["barcelo.com"],
  },
  "node/2998189889": {
    url: "https://www.barcelo.com/en-us/occidental-allegro-playa-dorada/",
    city: "Puerto Plata",
    brand_hosts: ["barcelo.com", "occidentalhotels.com"],
  },
  "way/33120922": {
    url: "https://www.barcelo.com/en-us/occidental-allegro-playa-dorada/",
    city: "Puerto Plata",
    brand_hosts: ["barcelo.com", "occidentalhotels.com"],
  },

  // IHG InterContinental Santo Domingo
  "node/3161538019": {
    url: "https://www.ihg.com/intercontinental/hotels/us/en/santo-domingo/sdqic/hoteldetail",
    city: "Santo Domingo",
    brand_hosts: ["ihg.com"],
  },
  "way/310694392": {
    url: "https://www.ihg.com/intercontinental/hotels/us/en/santo-domingo/sdqic/hoteldetail",
    city: "Santo Domingo",
    brand_hosts: ["ihg.com"],
  },

  // Marriott Westin Punta Cana
  "way/404174344": {
    url: "https://www.marriott.com/en-us/hotels/pujwi-the-westin-puntacana-resort/overview/",
    city: "Punta Cana",
    brand_hosts: ["marriott.com"],
  },

  // Hilton / Hard Rock / Choice / Amhsa
  "way/253125050": {
    url: "https://www.hilton.com/en/hotels/lrqhihh-hilton-la-romana-an-all-inclusive-adult-resort/",
    city: "La Romana",
    brand_hosts: ["hilton.com"],
  },
  "way/255788067": {
    url: "https://hotel.hardrock.com/punta-cana/",
    city: "Punta Cana",
    brand_hosts: ["hardrock.com", "hardrockhotels.com"],
  },
  "way/251860535": {
    url: "https://www.choicehotels.com/dominican-republic/santo-domingo/quality-inn-hotels/do002",
    city: "Santo Domingo",
    brand_hosts: ["choicehotels.com"],
  },
  "way/204089278": {
    url: "https://www.amhsamarina.com/casa-marina-beach",
    city: "Sosúa",
    brand_hosts: ["amhsamarina.com"],
  },

  // Sheraton Santo Domingo (generic OSM name but single SD property)
  "node/2778735252": {
    url: "https://www.marriott.com/en-us/hotels/sdqds-sheraton-santo-domingo-hotel/overview/",
    city: "Santo Domingo",
    brand_hosts: ["marriott.com"],
  },

  // Soft-brand / city-only clears (URL already present on payload)
  "node/2172701438": {
    city: "Barahona",
  },
  "way/1536664489": {
    url: "https://www.marriott.com/en-us/hotels/popak-donoma-las-terrenas-resort-and-villas-autograph-collection/overview/",
    city: "Las Terrenas",
    brand_hosts: ["marriott.com"],
  },
});

/**
 * @param {string} sourceRecordId
 */
export function getDrOsmStewardCatalogEntry(sourceRecordId) {
  const entry = DR_OSM_STEWARD_OFFICIAL_URL_CATALOG[String(sourceRecordId || "")];
  if (!entry) return null;
  if (!entry.url && !entry.city) return null;
  return entry;
}
