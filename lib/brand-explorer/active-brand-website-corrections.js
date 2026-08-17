/**
 * Source-backed brand page URLs for Active Brand Basics rows.
 * Must be brand-specific pages (not parent company homepages), https://, no www.
 */
export const ACTIVE_BRAND_WEBSITE_CORRECTIONS = Object.freeze({
  "AC Hotels by Marriott": "https://marriott.com/en-us/brands/ac-hotels",
  "Aloft Hotels": "https://marriott.com/en-us/brands/aloft-hotels",
  "Ascend Hotel Collection": "https://choicehotels.com/ascend",
  "Autograph Collection": "https://marriott.com/en-us/brands/autograph-collection",
  "avid hotels": "https://ihg.com/avidhotels",
  "Bunkhouse Hotels": "https://bunkhousegroup.com",
  "Canopy by Hilton": "https://hilton.com/en/brands/canopy-by-hilton",
  "City Express by Marriott": "https://marriott.com/en-us/brands/city-express",
  "Comfort Inn & Suites": "https://choicehotels.com/comfort-hotels",
  "Country Inn & Suites by Choice": "https://choicehotels.com/country-inn-suites",
  "Courtyard by Marriott": "https://marriott.com/en-us/brands/courtyard",
  "Curio Collection by Hilton": "https://hilton.com/en/brands/curio-collection",
  "Dazzler by Wyndham": "https://wyndhamhotels.com/dazzler",
  "Design Hotels": "https://design-hotels.marriott.com",
  "Esplendor by Wyndham": "https://wyndhamhotels.com/esplendor",
  "Even Hotels": "https://ihg.com/evenhotels",
  "Everhome Suites": "https://choicehotels.com/everhome-suites",
  "Four Points Flex by Sheraton": "https://marriott.com/en-us/brands/four-points-flex",
  "Hampton by Hilton": "https://hilton.com/en/brands/hampton-by-hilton",
  "Handwritten Collection": "https://handwrittencollection.accor.com",
  "Hilton Garden Inn": "https://hilton.com/en/brands/hilton-garden-inn",
  "Holiday Inn Express": "https://ihg.com/holidayinnexpress",
  "Home2 Suites by Hilton": "https://hilton.com/en/brands/home2-suites",
  "Hyatt Place": "https://hyatt.com/hyatt-place",
  "Hyatt Zilara": "https://hyatt.com/en-US/brand/hyatt-zilara",
  "Hyatt Ziva": "https://hyatt.com/en-US/brand/hyatt-ziva",
  "Iberostar Selection": "https://iberostar.com/en/hotels/iberostar-selection",
  ibis: "https://ibis.accor.com",
  "Kimpton Hotels": "https://kimptonhotels.com",
  "Marriott Hotels": "https://marriott.com/en-us/marriott-hotels/travel",
  Mercure: "https://mercure.accor.com",
  "MGallery Collection": "https://mgallery.accor.com",
  "Motto by Hilton": "https://hilton.com/en/brands/motto-by-hilton",
  "Moxy Hotels": "https://marriott.com/en-us/brands/moxy-hotels",
  "Mr & Mrs Smith": "https://mrandmrssmith.com",
  "NH Hotels": "https://nh-hotels.com",
  Novotel: "https://novotel.accor.com",
  Pullman: "https://pullman.accor.com",
  "Quality Inn": "https://choicehotels.com/quality-inn",
  "Radisson Blu by Choice": "https://choicehotels.com/radisson-blu",
  "Radisson by Choice": "https://choicehotels.com/radisson",
  "Radisson Individuals by Choice": "https://choicehotels.com/radisson-individuals",
  "Radisson RED by Choice": "https://choicehotels.com/radisson-red",
  "Residence Inn by Marriott": "https://marriott.com/en-us/brands/residence-inn",
  "Secrets Resorts & Spas": "https://hyatt.com/en-US/brand/secrets-resorts-and-spas",
  Sheraton: "https://marriott.com/en-us/brands/sheraton",
  "Small Luxury Hotels of the World": "https://slh.com",
  "Spark by Hilton": "https://hilton.com/en/brands/spark-by-hilton",
  "SpringHill Suites by Marriott": "https://marriott.com/en-us/brands/springhill-suites",
  StudioRes: "https://marriott.com/en-us/brands/studiores",
  "Suburban Studios": "https://choicehotels.com/suburban",
  "Sunscape Resorts & Spas": "https://hyattinclusivecollection.com/en/resorts-hotels/sunscape",
  "Tapestry Collection by Hilton": "https://hilton.com/en/brands/tapestry-collection",
  "Tempo by Hilton": "https://hilton.com/en/brands/tempo-by-hilton",
  "The Leading Hotels of the World": "https://lhw.com",
  "TownePlace Suites by Marriott": "https://marriott.com/en-us/brands/towneplace-suites",
  "Trademark Collection by Wyndham": "https://wyndhamhotels.com/trademark",
  "Travelodge by Wyndham": "https://wyndhamhotels.com/travelodge",
  "Tribute Portfolio": "https://tribute-portfolio.marriott.com",
  "Tru by Hilton": "https://hilton.com/en/brands/tru-by-hilton",
  "Unbound Collection by Hyatt": "https://hyatt.com/en-US/brand/the-unbound-collection-by-hyatt",
  "Vignette Collection": "https://ihg.com/vignettecollection",
  "Voco Hotels": "https://ihg.com/voco",
  Westin: "https://marriott.com/en-us/brands/westin",
  "WoodSpring Suites": "https://woodspring.com",
  Wyndham: "https://wyndhamhotels.com/wyndham",
});

export function normalizeBrandWebsiteUrl(raw) {
  const v = String(raw ?? "").trim();
  if (!v) return "";
  let url;
  try {
    url = new URL(v.includes("://") ? v : `https://${v}`);
  } catch {
    return v;
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") return v;
  url.protocol = "https:";
  if (url.hostname.toLowerCase().startsWith("www.")) {
    url.hostname = url.hostname.slice(4);
  }
  let out = url.toString();
  if (out.endsWith("/") && url.pathname !== "/") out = out.replace(/\/+$/, "");
  return out;
}

export function isParentCompanyHomepage(url, parentCompany = "") {
  const normalized = normalizeBrandWebsiteUrl(url).toLowerCase();
  const parent = String(parentCompany || "").toLowerCase();
  const generic = [
    "https://marriott.com",
    "https://marriott.com/",
    "https://ihg.com",
    "https://ihg.com/",
    "https://hyatt.com",
    "https://hyatt.com/",
    "https://accor.com",
    "https://accor.com/",
    "https://wyndham.com",
    "https://wyndham.com/",
    "https://minorhotels.com",
    "https://minorhotels.com/",
    "https://hilton.com",
    "https://hilton.com/",
  ];
  if (generic.includes(normalized)) return true;
  if (/minorhotels\.com\/?$/i.test(normalized) && parent.includes("nh")) return true;
  return false;
}
