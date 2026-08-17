/**
 * Design Hotels Recent Momentum parity content v35F-R5.
 * Recent CALA openings with press / trade announcement URLs — Tribute v25C-3F pattern.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

export const V35F_R4_MOMENTUM_VERSION = "v35F-R5";

/** Source Library record IDs from v35C capture (approved for Explorer). */
const MOMENTUM_SOURCE_IDS = Object.freeze(["recXNyd8GO1q9E5uP", "recFCCRtptfSBrh9e"]);

/** Existing presentation row IDs from v35F momentum slot creates. */
export const DESIGN_HOTELS_MOMENTUM_RECORD_IDS = Object.freeze({
  wakeBiohotel: "recEExDl1cq6bWusn",
  nestBaja: "recYRLzyijFkSzt2c",
  wakeMedellin: "recdkxeVhs4fGb5GK",
});

export const DESIGN_HOTELS_MOMENTUM_LABEL = "Recent openings · linked announcements";

const PR_OR_TRADE_URL_RE =
  /newsroom|press-release|press_release|\/news\/|prnewswire\.com|globenewswire\.com|businesswire\.com|marriott\.pressarea\.com|hotel-online\.com\/(news|press)|journaldespalaces\.com\/en\/pressrelease|einpresswire\.com|businesstraveller\.com\/news/i;

function openingMomentumPackage({
  recordId,
  sort,
  propertyKey,
  propertyName,
  dateLine,
  title,
  summary,
  announcementUrl,
  sourceBasis,
}) {
  if (!PR_OR_TRADE_URL_RE.test(announcementUrl)) {
    throw new Error(`Non-announcement URL for ${propertyKey}: ${announcementUrl}`);
  }
  return {
    recordId,
    sort,
    propertyKey,
    propertyName,
    slotKey: "footprint.momentum",
    tab: "Footprint & Growth",
    title,
    dateLine,
    summary,
    announcementUrl,
    sourceUrl: announcementUrl,
    body: buildMomentumBody({
      dateLine,
      summary,
      sourceUrl: announcementUrl,
    }),
    sourceIds: MOMENTUM_SOURCE_IDS,
    sourceBasis,
  };
}

/** Founder-reviewed opening announcements — CALA recent openings with linked press/trade sources. */
export const DESIGN_HOTELS_MOMENTUM_PARITY_PACKAGES = Object.freeze([
  openingMomentumPackage({
    recordId: DESIGN_HOTELS_MOMENTUM_RECORD_IDS.wakeBiohotel,
    sort: 0,
    propertyKey: "wake-biohotel",
    propertyName: "Wake BioHotel",
    dateLine: "Aug 2025",
    title: "Wake BioHotel Opens As Design Hotels Member In Colombia",
    summary:
      "Wake BioHotel opened in Medellín as Colombia's first Design Hotels member—a wellness and longevity-focused debut that extends the collection's CALA design-led footprint for owners comparing affiliation paths.",
    announcementUrl:
      "https://www.businesstraveller.com/news/hotels/wake-biohotel-redefines-luxury-in-medellin/",
    sourceBasis: "Business Traveller opening coverage; corroborated by Forbes Colombia and Design Hotels Colombia portfolio announcements.",
  }),
  openingMomentumPackage({
    recordId: DESIGN_HOTELS_MOMENTUM_RECORD_IDS.nestBaja,
    sort: 1,
    propertyKey: "nest-baja",
    propertyName: "NEST Baja",
    dateLine: "Jun 2025",
    title: "NEST Baja Opens As Design Hotels Member In Los Cabos",
    summary:
      "Namron Hospitality debuted NEST Baja on Los Cabos' East Cape as a Design Hotels member—boutique resort opening that adds Mexico CALA momentum within Marriott's design-led collection.",
    announcementUrl:
      "https://www.hotel-online.com/news/namron-hospitality-debuts-nest-baja-second-property-in-the-nest-collection-and-member-of-marriotts-design-hotels",
    sourceBasis: "Hotel Online opening announcement; property listed on Design Hotels consumer directory.",
  }),
  openingMomentumPackage({
    recordId: DESIGN_HOTELS_MOMENTUM_RECORD_IDS.wakeMedellin,
    sort: 2,
    propertyKey: "wake-medellin",
    propertyName: "Wake Medellín",
    dateLine: "Jun 2026",
    title: "Wake Medellín Opens As Wellness Hotel In Medellín",
    summary:
      "Wake Medellín opened in Provenza as a wellness-focused hospitality debut in Medellín—urban lifestyle opening reinforcing Colombia's CALA momentum for owners evaluating design-led affiliation.",
    announcementUrl:
      "https://www.einpresswire.com/article/918483387/medell-n-joins-the-rise-of-luxury-wellness-travel-with-the-opening-of-wake-medell-n",
    sourceBasis: "EIN Presswire opening announcement; Wake Medellín listed on Design Hotels member directory.",
  }),
]);

export function buildDesignHotelsMomentumParityPackagesV35FR4() {
  return [
    ...DESIGN_HOTELS_MOMENTUM_PARITY_PACKAGES,
    {
      slotKey: "footprint.momentum_label",
      tab: "Footprint & Growth",
      title: "",
      body: DESIGN_HOTELS_MOMENTUM_LABEL,
      sort: 0,
      sourceIds: MOMENTUM_SOURCE_IDS,
      patchOnly: true,
    },
  ];
}

export function isDesignHotelsMomentumAnnouncementUrl(url) {
  return PR_OR_TRADE_URL_RE.test(String(url || ""));
}
