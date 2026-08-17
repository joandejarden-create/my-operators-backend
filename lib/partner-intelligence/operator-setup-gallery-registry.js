/**
 * Operator Explorer Image Gallery — named hotels + distinct images.
 * HE CALA pattern: materials.gallery.1..6 with Title = hotel name, Body = country.
 *
 * Only include verified public portfolio hotels + fetchable image URLs.
 * Staging / sample operators: skip inventing (action: clearGenericGallery).
 */

/** @typedef {{ title: string, country: string, imageUrl: string, sourceNote: string }} GalleryHotel */
/** @typedef {{
 *   masterId: string,
 *   companyName: string,
 *   hotels: GalleryHotel[],
 *   clearGenericGallery?: boolean,
 *   deactivateGallery?: boolean,
 *   skip?: boolean,
 *   skipReason?: string
 * }} OperatorGallerySpec */

/** @param {string} title @param {string} country @param {string} imageUrl @param {string} sourceNote */
function h(title, country, imageUrl, sourceNote) {
  return Object.freeze({ title, country, imageUrl, sourceNote });
}

/** @type {Record<string, OperatorGallerySpec>} */
export const OPERATOR_GALLERY_BY_SLUG = Object.freeze({
  "hotel-equities-cala": {
    masterId: "recWPKu5laVZxsvpn",
    companyName: "Hotel Equities (CALA)",
    hotels: [
      h(
        "AMARIS Grace Bay, LXR Hotels & Resorts",
        "Turks and Caicos",
        "https://www.hotelequities.com/files/6564/26571742_ImageLargeWidth.jpg",
        "hotelequities.com/cala.htm"
      ),
      h(
        "Casas del XVI, Vignette Collection",
        "Dominican Republic",
        "https://www.hotelequities.com/files/6564/27472752_ImageLargeWidth.jpg",
        "hotelequities.com/cala.htm"
      ),
      h(
        "Ceòra, a Luxury Collection Resort, Curaçao",
        "Curaçao",
        "https://www.hotelequities.com/files/6564/26057290_ImageLargeWidth.jpg",
        "hotelequities.com/cala.htm"
      ),
      h(
        "Donoma Las Terrenas Beach Resort & Spa, Autograph Collection",
        "Dominican Republic",
        "https://www.hotelequities.com/files/6564/27758953_ImageLargeWidth.jpg",
        "hotelequities.com/cala.htm"
      ),
      h(
        "Hilton Garden Inn Cuernavaca",
        "Mexico",
        "https://www.hotelequities.com/files/6564/28690482_ImageLargeWidth.jpg",
        "hotelequities.com/cala.htm"
      ),
      h(
        "Elephant Tree Resort and Villas Tobago, Tapestry Collection by Hilton",
        "Trinidad and Tobago",
        "https://www.hotelequities.com/files/6564/26057291_ImageLargeWidth.jpg",
        "hotelequities.com/cala.htm"
      ),
    ],
  },

  "ghl-hoteles": {
    masterId: "reciI2tYQBfMoMK9G",
    companyName: "GHL Hoteles (GHL Holding)",
    hotels: [
      h(
        "GHL Hotel Lago Titicaca",
        "Peru",
        "https://images.squarespace-cdn.com/content/v1/6876676c4eb2db6f7e98950f/11740477-eacd-401c-b5b3-6ba674fee9ce/Lago+Titicaca+-+Fachada+-+008.jpg",
        "ghloperador.com portfolio"
      ),
      h(
        "Sheraton San José Hotel",
        "Costa Rica",
        "https://images.squarespace-cdn.com/content/v1/6876676c4eb2db6f7e98950f/8b38bc5f-ff71-4821-aca7-7e73b35740e9/Sheraton+San+Jos%C3%A9.jpeg",
        "ghloperador.com portfolio"
      ),
      h(
        "Bastión Luxury Hotel",
        "Colombia",
        "https://images.squarespace-cdn.com/content/v1/6876676c4eb2db6f7e98950f/6b5c9c67-14c4-44fc-8bbe-454741dc4563/Bastion+-+Piscina+-+002.jpg",
        "ghloperador.com portfolio"
      ),
      h(
        "Four Points by Sheraton Tequendama, Bogotá",
        "Colombia",
        "https://images.squarespace-cdn.com/content/v1/6876676c4eb2db6f7e98950f/c73b1856-9ef9-42bb-9ddb-f0fb50c88387/fps+tequendama.jpg",
        "ghloperador.com portfolio"
      ),
      h(
        "GHL Hotel Capital",
        "Colombia",
        "https://images.squarespace-cdn.com/content/v1/6876676c4eb2db6f7e98950f/8b48f97d-934f-439e-a35c-cb6db6d088bb/GHL+Capital+-+Fachada+-+004.jpg",
        "ghloperador.com portfolio"
      ),
      h(
        "Radisson Hotel Panamá Canal",
        "Panama",
        "https://images.squarespace-cdn.com/content/v1/6876676c4eb2db6f7e98950f/47fa3670-8a16-4d8f-9634-745983697396/RadissonPanam%C3%A1.jpg",
        "ghloperador.com portfolio"
      ),
    ],
  },

  "royalton-hotels-resorts": {
    masterId: "recOc5kpsg4Muip9Y",
    companyName: "Royalton Hotels & Resorts",
    hotels: [
      h(
        "Royalton Riviera Cancun",
        "Mexico",
        "https://marriott.cdn.tambourine.com/royalton-resorts/media/dji_0482-resized-69373913a1bf5.jpg",
        "royaltonresorts.com resort page og:image"
      ),
      h(
        "Royalton Negril",
        "Jamaica",
        "https://marriott.cdn.tambourine.com/royalton-resorts/media/rng-resized-6a51569ee93ea.png",
        "royaltonresorts.com resort page og:image"
      ),
      h(
        "Royalton Punta Cana",
        "Dominican Republic",
        "https://marriott.cdn.tambourine.com/royalton-resorts/media/638049739753972014-resized-68acb620a2bef.jpg",
        "royaltonresorts.com resort page og:image"
      ),
      h(
        "Royalton Saint Lucia",
        "Saint Lucia",
        "https://marriott.cdn.tambourine.com/royalton-resorts/media/royalton_saint_lucia_display_image1600x1200-625772ab29180.jpg",
        "royaltonresorts.com resort page og:image"
      ),
      h(
        "Royalton Grenada",
        "Grenada",
        "https://marriott.cdn.tambourine.com/royalton-resorts/media/dji_0241-625da79b4acce.jpg",
        "royaltonresorts.com resort page og:image"
      ),
      h(
        "Royalton Antigua",
        "Antigua and Barbuda",
        "https://marriott.cdn.tambourine.com/royalton-resorts/media/header-689643cab652f.jpg",
        "royaltonresorts.com resort page og:image"
      ),
    ],
  },

  "arbor-lodging-cala": {
    masterId: "recF5Z87OAqFgndoq",
    companyName: "Arbor Lodging (CALA)",
    hotels: [
      h(
        "Hotel Phillips Kansas City",
        "United States",
        "https://symphony.cdn.tambourine.com/arbor-lodging/media/arborlodging-portfolio-slider-01-hotelphillipskansascity-68598fbf2dae2.webp",
        "arborlodging.com/portfolio (enterprise portfolio; no named CALA hotels published)"
      ),
      h(
        "Eaglewood Resort & Spa",
        "United States",
        "https://symphony.cdn.tambourine.com/arbor-lodging/media/eaglewood-resort-and-spa-1-69123d9edc3e5.webp",
        "arborlodging.com/portfolio"
      ),
      h(
        "AC Hotel Phoenix Downtown",
        "United States",
        "https://symphony.cdn.tambourine.com/arbor-lodging/media/arborlodging-portfolio-slider-03-achotelphoenixdowntown-68598fc465f17.webp",
        "arborlodging.com/portfolio"
      ),
      h(
        "Yarrow Hotel Park City",
        "United States",
        "https://symphony.cdn.tambourine.com/arbor-lodging/media/arborlodging-portfolio-slider-04-yarrowhotelparkcity-68598fc69675a.webp",
        "arborlodging.com/portfolio"
      ),
      h(
        "Residence Inn Salt Lake City Downtown",
        "United States",
        "https://symphony.cdn.tambourine.com/arbor-lodging/media/arborlodging-portfolio-slider-06-residenceinnsaltlakecitydowntown-68598fcb7bd65.webp",
        "arborlodging.com/portfolio"
      ),
      h(
        "Hampton Inn Santa Barbara/Goleta",
        "United States",
        "https://symphony.cdn.tambourine.com/arbor-lodging/media/arborlodging-portfolio-slider-08-hamptoninnsantabarbaragoleta-68598fcf9842e.webp",
        "arborlodging.com/portfolio"
      ),
    ],
  },

  "aimbridge-latam": {
    masterId: "recGWxIJqnYHkJZFD",
    companyName: "Aimbridge Hospitality (LATAM)",
    hotels: [
      h(
        "Ibis México Alameda",
        "Mexico",
        "https://aimbridgelatam.com/wp-content/uploads/2025/09/Ibis-Mexico-Alameda-2.jpg",
        "aimbridgelatam.com/directorio-hoteles"
      ),
      h(
        "Hotel Bloom Tulum",
        "Mexico",
        "https://aimbridgelatam.com/wp-content/uploads/2024/12/bloom-15.jpg",
        "aimbridgelatam.com/directorio-hoteles"
      ),
      h(
        "GRAN HOTEL DE PUEBLA by HNF",
        "Mexico",
        "https://aimbridgelatam.com/wp-content/uploads/2023/07/GRAN-HOTEL-DE-PUEBLA.jpg",
        "aimbridgelatam.com/directorio-hoteles"
      ),
      h(
        "Sheraton Guadalajara Expo",
        "Mexico",
        "https://aimbridgelatam.com/wp-content/uploads/2023/07/GRAN-HOTEL-EXPO-GUADALAJARA.jpg",
        "aimbridgelatam.com/directorio-hoteles"
      ),
      h(
        "Hotel Guadalajara Country Club by HNF",
        "Mexico",
        "https://aimbridgelatam.com/wp-content/uploads/2023/07/HOTEL-GUADALAJARA-COUNTRY-CLUB.jpg",
        "aimbridgelatam.com/directorio-hoteles"
      ),
      h(
        "Holiday Inn Guadalajara Patria-Universidad",
        "Mexico",
        "https://aimbridgelatam.com/wp-content/uploads/2021/09/GDLAN_-_Breakfast_Area_-_1087818.jpg",
        "aimbridgelatam.com/directorio-hoteles"
      ),
    ],
  },

  "remington-hospitality": {
    masterId: "rec6UB6RpMKSs2tAo",
    companyName: "Remington Hospitality",
    hotels: [
      h(
        "Kimpton Mas Olas Resort & Spa",
        "Mexico",
        "https://www.remingtonhospitality.com/resourcefiles/portfoliopagelistcala/kimpton-mas-olas-resort.png",
        "remingtonhospitality.com/cala"
      ),
      h(
        "Hilton Garden Inn La Romana",
        "Dominican Republic",
        "https://www.remingtonhospitality.com/resourcefiles/portfoliopagelistcala/hilton-garden-inn-la-romana.png",
        "remingtonhospitality.com/cala"
      ),
      h(
        "ONE | GT Grand Cayman",
        "Cayman Islands",
        "https://www.remingtonhospitality.com/resourcefiles/portfoliopagelistcala/onegt.png",
        "remingtonhospitality.com/cala"
      ),
      h(
        "Aruna Resort & Villas",
        "Belize",
        "https://www.remingtonhospitality.com/resourcefiles/portfoliopagelistcala/aruna-resort-villas.png",
        "remingtonhospitality.com/cala"
      ),
      h(
        "Croc's Resort & Casino",
        "Costa Rica",
        "https://www.crocsresortcasino.com/wp-content/uploads/2024/10/DSC0705-Ad-Hoc-Preset-1-4871x3248-1-scaled.webp",
        "crocsresort.com / Remington CALA press"
      ),
      h(
        "Autograph Collection Sarchí (Opening Q3 2026)",
        "Costa Rica",
        "https://www.remingtonhospitality.com/resourcefiles/future-properties/autograph.png",
        "remingtonhospitality.com/cala future properties"
      ),
    ],
  },

  highgate: {
    masterId: "recLjxtxIIVJaGbXK",
    companyName: "Highgate",
    hotels: [
      h(
        "The Huntington Hotel, San Francisco",
        "United States",
        "https://www.highgate.com/app/uploads/2026/05/The-Huntington-Hotel-Lobby-1-e1778595377839.jpg",
        "highgate.com homepage / portfolio"
      ),
      h(
        "The Crescent Hotel, Fort Worth",
        "United States",
        "https://www.highgate.com/app/uploads/2025/08/Crescent_ralphs2_4055-1920x1280.jpg",
        "highgate.com homepage"
      ),
      h(
        "Hotel Paracas, a Luxury Collection Resort, Peru",
        "Peru",
        "https://www.highgate.com/app/uploads/2025/08/PARACAS-PERU-MAIN-POOL-1920x1280.jpg",
        "highgate.com homepage"
      ),
      h(
        "The Goodtime Hotel, Miami",
        "United States",
        "https://www.highgate.com/app/uploads/2025/08/GOODTIME_0050_v1-1920x1280.jpg",
        "highgate.com/portfolio"
      ),
      h(
        "The Joule Hotel, Dallas",
        "United States",
        "https://www.highgate.com/app/uploads/2025/09/Jimmy-5-1920x1440.jpg",
        "highgate.com/portfolio"
      ),
      h(
        "Tambo del Inka, a Luxury Collection Resort, Peru",
        "Peru",
        "https://www.highgate.com/app/uploads/2025/02/tambo-del-inka-1920x1080.jpg",
        "highgate.com/portfolio"
      ),
    ],
  },

  "arriva-hospitality-group": {
    masterId: "reck6gjQd3wdeugmZ",
    companyName: "Arriva Hospitality Group (AHG)",
    hotels: [],
    deactivateGallery: true,
    skipReason:
      "Spot-check failed: Crown Paradise staging CDN reuses shared assets; Vista Express assets not verified as hotel exteriors. Deactivated until property-domain images are confirmed.",
  },

  "accor-managed": {
    masterId: "recF2WqLqNVyKGz9E",
    companyName: "Accor (Managed)",
    hotels: [
      h(
        "Sofitel Legend Santa Clara Cartagena",
        "Colombia",
        "https://www.ahstatic.com/photos/1621_ho_00_p_1024x768.jpg",
        "all.accor.com hotel page og:image"
      ),
      h(
        "Sofitel Mexico City Reforma",
        "Mexico",
        "https://www.ahstatic.com/photos/9167_ho_00_p_1024x768.jpg",
        "all.accor.com hotel page og:image"
      ),
      h(
        "Fairmont Mayakoba",
        "Mexico",
        "https://www.ahstatic.com/photos/b4i2_ho_00_p_1024x768.jpg",
        "all.accor.com hotel page og:image"
      ),
      h(
        "Novotel Bogotá Parque 93",
        "Colombia",
        "https://www.ahstatic.com/photos/b1i4_ho_00_p_1024x768.jpg",
        "all.accor.com hotel page og:image"
      ),
      h(
        "ibis Styles Lima Larco Miraflores",
        "Peru",
        "https://www.ahstatic.com/photos/b1b7_ho_00_p_1024x768.jpg",
        "all.accor.com hotel page og:image"
      ),
      h(
        "MGallery Hotel Collection Casa Santo Domingo, Antigua",
        "Guatemala",
        "https://www.ahstatic.com/photos/b9i1_ho_00_p_1024x768.jpg",
        "all.accor.com hotel page og:image"
      ),
    ],
  },

  "grupo-presidente": {
    masterId: "recJtFkhjaO57rSDC",
    companyName: "Grupo Presidente",
    hotels: [
      h(
        "Presidente InterContinental Cancún Resort",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-cancun-8894764486-2x1",
        "IHG Cancún gallery CDN"
      ),
      h(
        "Presidente InterContinental Cozumel Resort & Spa",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-cozumel-8880269871-2x1",
        "IHG property page og:image"
      ),
      h(
        "InterContinental Presidente Mexico City",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-ciudad-de-mexico-9161779243-2x1",
        "IHG property page og:image"
      ),
      h(
        "Presidente InterContinental Guadalajara",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-guadalajara-9728454528-2x1",
        "IHG property page og:image"
      ),
      h(
        "Kimpton Aluna Tulum",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/kimpton-tulum-7182479079-4x3",
        "IHG Kimpton Aluna Tulum CDN (Grupo Presidente operated)"
      ),
      h(
        "Presidente InterContinental Puebla",
        "Mexico",
        "https://presidenteicpuebla.com/wp-content/uploads/2024/10/OG-TC-general5-1.jpg",
        "presidenteicpuebla.com og:image"
      ),
    ],
  },

  "brittain-resorts-hotels": {
    masterId: "receHCdI6CEsJqdG4",
    companyName: "Brittain Resorts & Hotels (BRH)",
    hotels: [
      h(
        "The Breakers Resort",
        "United States",
        "https://www.breakers.com/wp-content/uploads/sites/13/2022/06/the-breakers-resort-paradise-guest-room-2.jpg",
        "breakers.com og:image"
      ),
      h(
        "Caribbean Resort & Villas",
        "United States",
        "https://www.caribbeanresort.com/wp-content/uploads/sites/2/2018/09/caribbean-drone-back-angled-with-beach-1200x800-1.jpg",
        "caribbeanresort.com og:image"
      ),
      h(
        "Compass Cove Oceanfront Resort",
        "United States",
        "https://www.compasscove.com/wp-content/uploads/2022/09/compass-cove-video-background.jpg",
        "compasscove.com og:image"
      ),
      h(
        "Grande Cayman Resort",
        "United States",
        "https://www.grandecaymanresort.com/wp-content/uploads/sites/14/2020/12/grande-cayman-straight-on-beach-2048x1152_GC_1.jpg",
        "grandecaymanresort.com og:image"
      ),
      h(
        "Paradise Resort",
        "United States",
        "https://www.paradiseresortmb.com/wp-content/uploads/sites/7/2019/06/Pr.jpg",
        "paradiseresortmb.com og:image"
      ),
      h(
        "Ocean Reef Resort",
        "United States",
        "https://www.oceanreefmyrtlebeach.com/wp-content/uploads/sites/8/2022/08/ocean-reef-video-background.jpg",
        "oceanreefmyrtlebeach.com"
      ),
    ],
  },

  "ihg-managed": {
    masterId: "rec7IXYQYpKMYsrDl",
    companyName: "IHG Hotels & Resorts (Managed)",
    hotels: [
      h(
        "Presidente InterContinental Cancún Resort",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-cancun-8894764486-2x1",
        "IHG Cancún gallery CDN"
      ),
      h(
        "InterContinental Presidente Mexico City",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-ciudad-de-mexico-9161779243-2x1",
        "IHG property page"
      ),
      h(
        "Presidente InterContinental Cozumel Resort & Spa",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-cozumel-8880269871-2x1",
        "IHG property page"
      ),
      h(
        "Presidente InterContinental Guadalajara",
        "Mexico",
        "https://digital.ihg.com/is/image/ihg/intercontinental-guadalajara-9728454528-2x1",
        "IHG property page"
      ),
      h(
        "InterContinental Cartagena de Indias",
        "Colombia",
        "https://digital.ihg.com/is/image/ihg/intercontinental-cartagena-de-indias-9469001091-2x1",
        "IHG property page"
      ),
      h(
        "Holiday Inn Express Bogotá Parque 93",
        "Colombia",
        "https://digital.ihg.com/is/image/ihg/holiday-inn-express-bogota-4123635518-2x1",
        "IHG property page"
      ),
    ],
  },

  "tafer-hotels-resorts": {
    masterId: "recJ6NPSYveCTo3At",
    companyName: "Tafer Hotels & Resorts",
    hotels: [
      h(
        "Garza Blanca Resort & Spa Puerto Vallarta",
        "Mexico",
        "https://a.storyblok.com/f/285826016720786/2000x1302/8004012767/garza-blanca-los-puerto-vallarta-property.jpg",
        "garzablancaresort.com Storyblok property aerial"
      ),
      h(
        "Garza Blanca Resort & Spa Cancún",
        "Mexico",
        "https://a.storyblok.com/f/285826016720786/1720x967/e00d8d199f/21-12-14_gbcn_facilities_property-01.webp",
        "garzablancaresort.com/cancun Storyblok facilities/property"
      ),
      h(
        "Garza Blanca Resort & Spa Los Cabos",
        "Mexico",
        "https://a.storyblok.com/f/285826016720786/1920x1282/6f8f98280e/garza-blanca-los-cabos.webp",
        "garzablancaresort.com/los-cabos Storyblok CDN"
      ),
      h(
        "The Sanctuary at Garza Blanca Puerto Vallarta",
        "Mexico",
        "https://a.storyblok.com/f/285826016720786/5259x3938/806eea7fb9/tafer_sanctuary_facilities_river-side-pool-6.webp",
        "garzablancaresort.com/sanctuary Storyblok facilities"
      ),
      h(
        "Sierra Lago Resort & Spa",
        "Mexico",
        "https://www.sierralagoresort.com/cms/resources/heated-pool-time-sierra-lago-resort.jpg",
        "sierralagoresort.com cms"
      ),
      h(
        "Hotel Mousai Puerto Vallarta",
        "Mexico",
        "https://a.storyblok.com/f/285826016720786/1920x1279/c5ab579538/mspv-facilities-property_04.jpg",
        "hotelmousai.com Puerto Vallarta Storyblok property"
      ),
    ],
  },
  "grupo-hotelero-santa-fe": {
    masterId: "reckyv9O0Y3auYpJJ",
    companyName: "Grupo Hotelero Santa Fe",
    hotels: [
      h(
        "Krystal Grand Cancún All Inclusive",
        "Mexico",
        "https://www.krystalgrand-cancun.com/uploads/galeriahoteles/exteriores-gran-cancun-a.jpg",
        "krystalgrand-cancun.com gallery exteriores"
      ),
      h(
        "Krystal Grand Los Cabos All Inclusive",
        "Mexico",
        "https://www.krystalgrand-loscabos.com/uploads/galeriahoteles/aerial-grand-los-cabos-a.jpg",
        "krystalgrand-loscabos.com gallery aerial"
      ),
      h(
        "Krystal Grand Nuevo Vallarta All Inclusive",
        "Mexico",
        "https://www.krystalgrand-nuevovallarta.com/uploads/galeriahoteles/ext-infinity-pool-grand-nuevo-vallarta-a.jpeg",
        "krystalgrand-nuevovallarta.com gallery (Krystal star in pool)"
      ),
      h(
        "Krystal Grand Puerto Vallarta All Inclusive",
        "Mexico",
        "https://www.krystalgrand-puertovallarta.com/uploads/galeriahoteles/panoramica-grand-puerto-vallarta-a.jpg",
        "krystalgrand-puertovallarta.com gallery panoramica"
      ),
      h(
        "Mahekal Beach Resort Playa del Carmen",
        "Mexico",
        "https://api.mahekal.axovia.mx/uploads/Beach_House_King_Plunge_Pool_bdfa8e7893.webp",
        "mahekalbeachresort.com CDN Beach House suite"
      ),
      h(
        "Krystal Cancún",
        "Mexico",
        "https://www.krystal-cancun.com/uploads/galeriahoteles/krystal-cancun-panoramic-1.jpg",
        "krystal-cancun.com gallery panoramic"
      ),
    ],
  },
  "atlantica-hotels-international": {
    masterId: "recfwDdU5t9h4uFnZ",
    companyName: "Atlantica Hotels International (AHI)",
    hotels: [],
    deactivateGallery: true,
    skipReason:
      "Spot-check failed: Transamerica 'banner-desktop' assets are lifestyle marketing, not identifiable hotel exteriors; one Berrini gallery file was wrong property. Deactivated until facade/lobby photos are verified.",
  },
  "marriott-international-managed": {
    masterId: "recGmiPhRt6hiayd9",
    companyName: "Marriott International (Managed)",
    hotels: [],
    deactivateGallery: true,
    skipReason:
      "Spot-check failed: several Commons images were not hotel exteriors (beach-near, watermarked) and brand CDNs block automated fetch. Deactivated until six logo/path-verified managed hotels are sourced.",
  },
  "hilton-managed": {
    masterId: "rec3Uwxe6ovpiokuN",
    companyName: "Hilton (Managed)",
    hotels: [],
    deactivateGallery: true,
    skipReason:
      "Spot-check failed: Commons picks were fire-show / ocean-view / guest-room shots rather than identifiable exteriors; Hilton CDN rate-limits. Deactivated until six verified managed-hotel photos are sourced.",
  },
  "minor-hotels-managed": {
    masterId: "rec8SrT3VjRkkYTxm",
    companyName: "Minor Hotels (Managed)",
    hotels: [
      h(
        "NH Collection Mexico City Reforma",
        "Mexico",
        "https://img.nh-hotels.net/pr1Xq/oJNdzw/original/NH_Collection_Mexico_City_Reforma_Facade_day_street_external_signage.jpg",
        "nh-hotels.com property CDN facade + NH COLLECTION REFORMA signage"
      ),
      h(
        "NH Collection Mexico City Airport T2",
        "Mexico",
        "https://img.nh-hotels.net/NLPal/wwon1/original/F_NH_collection_aeropuerto_t2_mexic_077.jpg",
        "nh-hotels.com property CDN"
      ),
      h(
        "NH Collection Mexico City Centro Histórico",
        "Mexico",
        "https://img.nh-hotels.net/ZEBRA/e27Re0/original/NH_Collection_Mexico_City_Centro_Historico_Facade_sunrise_corner.jpg",
        "nh-hotels.com property CDN"
      ),
      h(
        "NH Collection Mexico City Santa Fe",
        "Mexico",
        "https://img.nh-hotels.net/YOqk9/PBDNp/original/F_NH_collection_santa_fe_170.jpg",
        "nh-hotels.com property CDN"
      ),
      h(
        "NH Collection Buenos Aires Lancaster",
        "Argentina",
        "https://img.nh-hotels.net/J92X7/6R9bV/original/F_NH_collection_lancaster_088.jpg",
        "nh-hotels.com property CDN"
      ),
      h(
        "NH Collection Buenos Aires Centro Histórico",
        "Argentina",
        "https://img.nh-hotels.net/7rJ7l/JOPbov/original/NH_Collection_Centro_Histo%CC%81rico_Facade_Horizontal_Nhc_isotype_Golden_External_Signage.jpg",
        "nh-hotels.com property CDN facade signage"
      ),
    ],
  },
  "playa-hotels-resorts": {
    masterId: "rec3TUHT9Z4AnFp5P",
    companyName: "Playa Hotels & Resorts",
    hotels: [
      h(
        "Hyatt Ziva Cancún",
        "Mexico",
        "https://playa-cms-assets.s3.amazonaws.com/media/Hyatt_ziva_cancun/hyatt-ziva-cancun-aerial-10.jpg",
        "playaresorts.com CMS aerial (Punta Cancún / Z logo)"
      ),
      h(
        "Jewel Palm Beach",
        "Dominican Republic",
        "https://playa-cms-assets.s3.amazonaws.com/media/jewel_palm_beach/23/jewel-palm-beach-aerial-resort-02.jpg",
        "playaresorts.com CMS Jewel Palm Beach aerial"
      ),
      h(
        "Hyatt Ziva Los Cabos",
        "Mexico",
        "https://playa-cms-assets.s3.amazonaws.com/styled/www.resortsbyhyatt.com/Hyatt_Ziva_Los_Cabos/Hyatt_Ziva_Los_Cabos_General_Resort/Hyatt-Ziva-Los-Cabos-Aerial-3-500-500-e03b931ef3a9117a1bf073eda0791b6c.jpg",
        "playaresorts.com CMS General_Resort aerial"
      ),
      h(
        "Hyatt Ziva Puerto Vallarta",
        "Mexico",
        "https://playa-media.imgix.net/mediavalet/medialibrary-4454e06b53b64519ba3d815bb8b551e5/f9a06468388e4098872a2c253b6a7c70/f9a06468388e4098872a2c253b6a7c70/Original/Hyatt-Ziva-Puerto-Vallarta-Aerial-5.jpg?w=1600",
        "playaresorts.com MediaValet aerial filename"
      ),
      h(
        "Hyatt Zilara Riviera Maya",
        "Mexico",
        "https://playa-cms-assets.s3.amazonaws.com/media/Hyatt_Zilara_Riviera_Maya/2023/hyatt-zilara-riviera-maya-main-pool-hero-shot-3.jpg",
        "playaresorts.com CMS main pool hero"
      ),
      h(
        "Hyatt Ziva Rose Hall",
        "Jamaica",
        "https://playa-cms-assets.s3.amazonaws.com/media/Hyatt_Ziva_Rose_Hall/GENERAL-RESORT/Hyatt-Ziva-Rose-Hall-Aerial.Jpg",
        "playaresorts.com CMS GENERAL-RESORT aerial"
      ),
    ],
  },
  "driftwood-hospitality-management": {
    masterId: "recKVILWcRLqrQlWs",
    companyName: "Driftwood Hospitality Management",
    hotels: [
      h(
        "Hotel Rumbao, a Tribute Portfolio Hotel — San Juan",
        "Puerto Rico",
        "https://symphony.cdn.tambourine.com/driftwood-hospitality-manageme/media/hotel-rumbao-resized-670d45d97a0f7.webp",
        "driftwoodhospitality.com portfolio CDN"
      ),
      h(
        "DoubleTree by Hilton Hilton Head Island",
        "United States",
        "https://symphony.cdn.tambourine.com/driftwood-hospitality-manageme/media/doubletree-hilton-head-island-69fa33429b6bf.webp",
        "driftwoodhospitality.com portfolio CDN"
      ),
      h(
        "Courtyard by Marriott Dallas Plano Parkway",
        "United States",
        "https://symphony.cdn.tambourine.com/driftwood-hospitality-manageme/media/courtyard-plano-6a314d23d4ae8.webp",
        "driftwoodhospitality.com portfolio CDN"
      ),
      h(
        "Element San Diego Mission Valley",
        "United States",
        "https://symphony.cdn.tambourine.com/driftwood-hospitality-manageme/media/element-san-diego-69f8dd4c8fac7.webp",
        "driftwoodhospitality.com portfolio CDN (element canopy branding)"
      ),
      h(
        "The Scottsdale Resort & Spa, Curio Collection by Hilton",
        "United States",
        "https://symphony.cdn.tambourine.com/driftwood-hospitality-manageme/media/scottsdale_scottsdaleresortandspa_curio_exterior_pooldrone5-resized-670d46fbb0b42.webp",
        "driftwoodhospitality.com portfolio CDN exterior drone"
      ),
      h(
        "InterContinental Kansas City at The Plaza",
        "United States",
        "https://symphony.cdn.tambourine.com/driftwood-hospitality-manageme/media/intercontinental-kansas-city-at-the-plaza-69b435e4aca37.webp",
        "driftwoodhospitality.com portfolio CDN"
      ),
    ],
  },

  "antillano-norte-hospitality-group": {
    masterId: "recTUjuDxL96yWcQA",
    companyName: "Antillano Norte Hospitality Group",
    hotels: [],
    clearGenericGallery: true,
  },
  "barrio-hotelero-cdmx": {
    masterId: "recq3NiRxOerg4kZU",
    companyName: "Barrio Hotelero CDMX",
    hotels: [],
    clearGenericGallery: true,
  },
  "cenote-azul-operadores": {
    masterId: "recQ6Cf8O2z0tiqBz",
    companyName: "Cenote Azul Operadores",
    hotels: [],
    clearGenericGallery: true,
  },
  "cordillera-one-gestion": {
    masterId: "recBReJUmxdOUvQzp",
    companyName: "Cordillera One Gestión",
    hotels: [],
    clearGenericGallery: true,
  },
  "mangle-azul-hospitalidad": {
    masterId: "recZgNR85WZKDItLF",
    companyName: "Mangle Azul Hospitalidad",
    hotels: [],
    clearGenericGallery: true,
  },
  "metro-lodging-sao-paulo": {
    masterId: "recwbyY4qfNP1bV3r",
    companyName: "Metro Lodging São Paulo",
    hotels: [],
    clearGenericGallery: true,
  },
  "oro-verde-lodge-hotel-operators": {
    masterId: "recxAa86Qoc0nFRSt",
    companyName: "Oro Verde Lodge & Hotel Operators",
    hotels: [],
    clearGenericGallery: true,
  },
  "panamerican-lodging-partners": {
    masterId: "recbT3q8ApRIBu4j5",
    companyName: "Panamerican Lodging Partners S.A.",
    hotels: [],
    clearGenericGallery: true,
  },
  "rio-plata-hotel-partners": {
    masterId: "reckO98E46sKTn3F3",
    companyName: "Río Plata Hotel Partners",
    hotels: [],
    clearGenericGallery: true,
  },
  "viento-sur-gestion-hotelera": {
    masterId: "recZPHT2zqc8K6itx",
    companyName: "Viento Sur Gestión Hotelera",
    hotels: [],
    clearGenericGallery: true,
  },
  oxohotel: {
    masterId: "rectsHzacZDFTH1Ze",
    companyName: "OxoHotel",
    hotels: [],
    clearGenericGallery: true,
  },
  "grupo-marta-hospitality": {
    masterId: "recuEDrp6oeJIEuRX",
    companyName: "Grupo Marta Hospitality",
    hotels: [],
    clearGenericGallery: true,
  },
  "grupo-iberostar": {
    masterId: "recwEHUotSGpfkZEJ",
    companyName: "Grupo Iberostar",
    hotels: [],
    clearGenericGallery: true,
  },
});

/**
 * Body text for Explorer Materials gallery rows — country only (shown as caption meta).
 * @param {GalleryHotel} hotel
 */
export function galleryBodyFromHotel(hotel) {
  const country = String(hotel?.country || "").trim();
  return country;
}

export function listReadyGallerySpecs() {
  return Object.entries(OPERATOR_GALLERY_BY_SLUG)
    .filter(
      ([, s]) =>
        !s.skip &&
        !s.clearGenericGallery &&
        !s.deactivateGallery &&
        (s.hotels?.length || 0) >= 6
    )
    .map(([slug, s]) => ({ slug, ...s }));
}
