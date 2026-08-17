/**
 * Wave 14 Stage 5 — supplemental International Reference openings/property pages
 * for brands whose source packs lack steward-matched property overview URLs.
 * Official marriott.com hotel overview pages discovered from brand microsites.
 */
export const WAVE14_IMAGE_SUPPLEMENTAL_VERSION = "wave14-image-supplemental-v1";

/**
 * Brands that must hold named property gallery/openings until a Flex-specific
 * property overview URL is steward-matched. StudioRes Fort Myers is no longer held.
 */
export const WAVE14_PROPERTY_HOLD_SLUGS = Object.freeze(["four-points-flex-by-sheraton"]);

export const WAVE14_SUPPLEMENTAL_OPENINGS_BY_SLUG = Object.freeze({
  "marriott-hotels": Object.freeze([
    {
      propertyName: "Marriott Culiacan Hotel",
      url: "https://www.marriott.com/en-us/hotels/culmc-marriott-culiacan-hotel/overview/",
      market: "Culiacán, Mexico",
      marketCity: "Culiacán",
      geographyLabel: "CALA",
    },
    {
      propertyName: "Barranquilla Marriott Hotel",
      url: "https://www.marriott.com/en-us/hotels/baqmc-barranquilla-marriott-hotel/overview/",
      market: "Barranquilla, Colombia",
      marketCity: "Barranquilla",
      geographyLabel: "CALA",
    },
  ]),
  "springhill-suites-by-marriott": Object.freeze([
    {
      propertyName: "SpringHill Suites by Marriott San Diego Carlsbad",
      url: "https://www.marriott.com/en-us/hotels/sansc-springhill-suites-san-diego-carlsbad/overview/",
      market: "San Diego / Carlsbad, USA",
      marketCity: "Carlsbad",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "SpringHill Suites by Marriott Colorado Springs Downtown",
      url: "https://www.marriott.com/en-us/hotels/cossd-springhill-suites-colorado-springs-downtown/overview/",
      market: "Colorado Springs, USA",
      marketCity: "Colorado Springs",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "SpringHill Suites by Marriott Myrtle Beach Oceanfront",
      url: "https://www.marriott.com/en-us/hotels/myrso-springhill-suites-myrtle-beach-oceanfront/overview/",
      market: "Myrtle Beach, USA",
      marketCity: "Myrtle Beach",
      geographyLabel: "International Reference",
    },
  ]),
  "towneplace-suites-by-marriott": Object.freeze([
    {
      propertyName: "TownePlace Suites by Marriott Tempe",
      url: "https://www.marriott.com/en-us/hotels/phxpe-towneplace-suites-tempe/overview/",
      market: "Tempe, USA",
      marketCity: "Tempe",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "TownePlace Suites by Marriott Dallas Plano Legacy",
      url: "https://www.marriott.com/en-us/hotels/daltp-towneplace-suites-dallas-plano-legacy/overview/",
      market: "Dallas / Plano, USA",
      marketCity: "Plano",
      geographyLabel: "International Reference",
    },
  ]),
  studiores: Object.freeze([
    {
      propertyName: "StudioRes Fort Myers Airport",
      url: "https://www.marriott.com/en-us/hotels/rswsr-studiores-fort-myers-airport/overview/",
      market: "Fort Myers, USA",
      marketCity: "Fort Myers",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "StudioRes Newnan",
      url: "https://www.marriott.com/en-us/hotels/atlni-studiores-newnan/overview/",
      market: "Newnan, USA",
      marketCity: "Newnan",
      geographyLabel: "International Reference",
    },
    {
      propertyName: "StudioRes Oak Ridge",
      url: "https://www.marriott.com/en-us/hotels/tysoe-studiores-oak-ridge/overview/",
      market: "Oak Ridge, USA",
      marketCity: "Oak Ridge",
      geographyLabel: "International Reference",
    },
  ]),
});

export function getWave14SupplementalOpenings(slug) {
  const s = String(slug || "").trim().toLowerCase();
  return WAVE14_SUPPLEMENTAL_OPENINGS_BY_SLUG[s] || [];
}

export function isWave14PropertyHoldSlug(slug) {
  return WAVE14_PROPERTY_HOLD_SLUGS.includes(String(slug || "").trim().toLowerCase());
}

/**
 * Curated official Flex imagery when live brand pages are Getty/stock-only.
 * Development gallery + verified Scene7 Flex property asset (London The Hub).
 * Do not use Four Points by Sheraton imagery.
 */
export const WAVE14_CURATED_POOL_SEED_BY_SLUG = Object.freeze({
  "four-points-flex-by-sheraton": Object.freeze([
    {
      propertyKey: "four-points-flex-development-gallery",
      propertyName: "Four Points Flex by Sheraton (official development gallery)",
      marketCity: "",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
      imageUrl:
        "https://www.hotel-development.marriott.com/resourcefiles/apartments/four-points-express-top.jpg",
      label: "development_gallery",
      role: "exterior_arrival",
      caption: "Four Points Flex exterior / arrival prototype (official Marriott development gallery).",
    },
    {
      propertyKey: "four-points-flex-development-gallery",
      propertyName: "Four Points Flex by Sheraton (official development gallery)",
      marketCity: "",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
      imageUrl:
        "https://www.hotel-development.marriott.com/resourcefiles/inner-gallery/fpx-reception-1.jpg",
      label: "development_gallery",
      role: "public_space_lobby",
      caption: "Four Points Flex reception / public space (official Marriott development gallery).",
    },
    {
      propertyKey: "four-points-flex-development-gallery",
      propertyName: "Four Points Flex by Sheraton (official development gallery)",
      marketCity: "",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
      imageUrl:
        "https://www.hotel-development.marriott.com/resourcefiles/inner-gallery/fpx-guestroom.jpg",
      label: "development_gallery",
      role: "guest_room_suite",
      caption: "Four Points Flex guest room (official Marriott development gallery).",
    },
    {
      propertyKey: "four-points-flex-development-gallery",
      propertyName: "Four Points Flex by Sheraton (official development gallery)",
      marketCity: "",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
      imageUrl:
        "https://www.hotel-development.marriott.com/resourcefiles/inner-gallery/fpx-buffet-1.jpg",
      label: "development_gallery",
      role: "food_beverage_experience",
      caption: "Four Points Flex breakfast / F&B area (official Marriott development gallery).",
    },
    {
      propertyKey: "four-points-flex-development-gallery",
      propertyName: "Four Points Flex by Sheraton (official development gallery)",
      marketCity: "",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
      imageUrl:
        "https://www.hotel-development.marriott.com/resourcefiles/inner-gallery/fpx-buffet-2.jpg",
      label: "development_gallery",
      role: "food_beverage_experience",
      caption: "Four Points Flex buffet / dining context (official Marriott development gallery).",
    },
    {
      propertyKey: "four-points-flex-london-the-hub",
      propertyName: "Four Points Flex by Sheraton London — The Hub",
      marketCity: "London",
      geographyLabel: "International Reference",
      sourcePageUrl: "https://www.marriott.com/brands/four-points-flex.mi",
      imageUrl:
        "https://cache.marriott.com/is/image/marriotts7prod/xf-lonfb-the-hub-18877:Wide-Hor?wid=1600&fit=constrain",
      label: "brand_site",
      role: "public_space_lobby",
      caption: "Four Points Flex London The Hub public space (official Marriott Bonvoy Scene7 asset).",
    },
  ]),
});

export function getWave14CuratedPoolSeed(slug) {
  const s = String(slug || "").trim().toLowerCase();
  return WAVE14_CURATED_POOL_SEED_BY_SLUG[s] || [];
}
