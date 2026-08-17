/**
 * Real footprint.openings from Choice CALA property URLs (census Phase 4K CSV)
 * and curated press/fixture cards for Radisson-family brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { AIRTABLE_TO_PROFILE_NAME } from "./choice-chi-brand-resolve.mjs";
import { selectDiversifiedUrlsForBrand } from "./choice-cala-url-index.mjs";
import {
  BRANDS_NO_CALA_OPENINGS,
  urlMatchesBrandSlug,
} from "./choice-cala-brand-url-slugs.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

/** @typedef {import('./choice-tier1-explorer-profiles.mjs').FootprintOpeningCard} FootprintOpeningCard */

/** @type {Record<string, string[]>} */
const PROFILE_ALIASES = {
  "Park Inn by Choice": "Park Inn by Radisson (Choice)",
  "Radisson by Choice": "Radisson (Choice)",
  "Radisson Blu by Choice": "Radisson Blu (Choice)",
  "Radisson Individuals by Choice": "Radisson Individual (Choice)",
  "Radisson RED by Choice": "Radisson RED  (Choice)",
  "Radisson Collection by Choice": "Radisson Collection  (Choice)",
  "Park Plaza by Choice": "Park Plaza (Choice)",
  "Country Inn & Suites": "Country Inn & Suites by Radisson (Choice)",
};

function profileKey(name) {
  return PROFILE_ALIASES[name] || AIRTABLE_TO_PROFILE_NAME[name] || name;
}

function loadFixtureRows(relPath) {
  const p = path.join(ROOT, relPath);
  if (!fs.existsSync(p)) return [];
  const data = JSON.parse(fs.readFileSync(p, "utf8"));
  return (data.rows || [])
    .filter((r) => r.slotKey === "footprint.openings")
    .map((r) => ({
      title: r.title,
      body: r.body,
      sort: r.sort ?? 0,
      caseSummaryOverview: r.caseSummaryOverview || "",
      caseSummaryOwnerObjective: r.caseSummaryOwnerObjective || "",
      caseSummaryBrandRelevance: r.caseSummaryBrandRelevance || "",
      caseSummaryInterpretation: r.caseSummaryInterpretation || "",
      caseSummaryTags: r.caseSummaryTags || "",
    }));
}

function extractUrlFromBody(body) {
  const m = String(body || "").match(/(https:\/\/www\.choicehotels\.com\/\S+)/i);
  return m ? m[1].trim() : "";
}

const COUNTRY_LABEL = {
  argentina: "Argentina",
  aruba: "Aruba",
  bahamas: "Bahamas",
  barbados: "Barbados",
  bolivia: "Bolivia",
  brazil: "Brazil",
  chile: "Chile",
  colombia: "Colombia",
  "costa-rica": "Costa Rica",
  "dominican-republic": "Dominican Republic",
  ecuador: "Ecuador",
  "el-salvador": "El Salvador",
  grenada: "Grenada",
  guatemala: "Guatemala",
  guyana: "Guyana",
  haiti: "Haiti",
  honduras: "Honduras",
  mexico: "Mexico",
  panama: "Panama",
  paraguay: "Paraguay",
  peru: "Peru",
  "puerto-rico": "Puerto Rico",
  suriname: "Suriname",
  "trinidad-and-tobago": "Trinidad and Tobago",
  uruguay: "Uruguay",
};

function parseUrl(url) {
  const u = new URL(url);
  const parts = u.pathname.split("/").filter(Boolean);
  const countrySlug = parts[0] || "";
  const citySlug = parts[1] || "";
  const city = citySlug
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ")
    .replace(/Antofagasta/i, "Antofagasta")
    .replace(/Santo Domingo/i, "Santo Domingo");
  return {
    country: COUNTRY_LABEL[countrySlug] || countrySlug,
    city,
    countrySlug,
    citySlug,
  };
}

function titleFromUrl(url, brandName) {
  const { city, country } = parseUrl(url);
  const short = brandName.replace(/\s+\(Choice\)/i, "").replace(/\s+by Choice/i, "");
  if (city && country) return `${short} — ${city}, ${country}`;
  return `${short} — ${city || country || "CALA"}`;
}

/**
 * @param {string} url
 * @param {string} brandName
 * @param {number} sort
 */
function cardFromUrl(url, brandName, sort) {
  const { city, country, citySlug, countrySlug } = parseUrl(url);
  const place = city ? `${city}, ${country}` : country;
  const body = `CALA, ${country}, Urban / corridor\n\n${place}\n\nChoice-affiliated · listed on choicehotels.com\n\nCALA footprint · census property URL\n\nActive Choice Hotels property page in the CALA corridor—use for market and distribution context; confirm flag, fees, and opening status in your LOI and FDD.\n\n${url}`;

  return {
    title: titleFromUrl(url, brandName).replace(/^—\s*/, "").trim() || titleFromUrl(url, brandName),
    body,
    sort,
    caseSummaryOverview: `${place}: property listed under ${brandName} on choicehotels.com (Dealality census URL extract, CALA).`,
    caseSummaryOwnerObjective: `Benchmark CALA corridor positioning, access, and brand retail for ${brandName} in ${country}.`,
    caseSummaryBrandRelevance: `Confirms ${brandName} has published CALA inventory on Choice consumer paths—not a comp-set guarantee for your submarket.`,
    caseSummaryInterpretation: `Validate ${citySlug || countrySlug} economics locally; census URL is a directory anchor, not Item 19 performance.`,
    caseSummaryTags: `${country}, CALA, ${brandName.split(" ")[0]}`,
  };
}

/** Curated overrides for high-value census URLs (better copy than auto). */
/** @type {Record<string, FootprintOpeningCard[]>} */
const CURATED_BY_PROFILE = {
  "Comfort Inn & Suites": [
    {
      title: "Comfort Inn & Suites — Scarborough, Trinidad and Tobago",
      body: "Island, Trinidad and Tobago, CALA, Leisure\n\nScarborough, Tobago\n\nUpper-midscale · beach/leisure corridor\n\nCaribbean CALA · Choice distribution\n\nListed Choice-affiliated Comfort Inn property in the Trinidad & Tobago corridor—use for CALA upper-midscale leisure and transient mix benchmarking.\n\nhttps://www.choicehotels.com/trinidad-and-tobago/scarborough/comfort-inn-hotels/tt005",
      sort: 0,
      caseSummaryOverview: "Scarborough, Tobago: Comfort Inn on choicehotels.com in a CALA island market.",
      caseSummaryOwnerObjective: "Reference for Caribbean upper-midscale breakfast-led economics under Choice Privileges.",
      caseSummaryBrandRelevance: "Comfort Inn & Suites CALA footprint—not a US-only corridor story.",
      caseSummaryInterpretation: "Island seasonality and access patterns differ from mainland CALA—model locally.",
      caseSummaryTags: "Trinidad and Tobago, CALA, Caribbean, Comfort Inn",
    },
    {
      title: "Comfort Inn — Tegucigalpa, Honduras",
      body: "Urban, Honduras, CALA, Business travel\n\nTegucigalpa, Honduras\n\nUpper-midscale · corporate transient\n\nCentral America gateway\n\nComfort Inn listed in Honduras capital—corporate and government transient demand patterns.\n\nhttps://www.choicehotels.com/honduras/tegucigalpa/comfort-inn-hotels/hn014",
      sort: 1,
      caseSummaryOverview: "Tegucigalpa: Comfort Inn CALA listing in Central American capital city.",
      caseSummaryOwnerObjective: "Shows upper-midscale Choice flag in Andean/Central American urban core.",
      caseSummaryBrandRelevance: "Breakfast-led upper-midscale versus economy highway flags in same country.",
      caseSummaryInterpretation: "Confirm authorization geography and prototype fees for Honduras in disclosure.",
      caseSummaryTags: "Honduras, CALA, Urban, Comfort Inn",
    },
    {
      title: "Comfort Inn — San Miguel, El Salvador",
      body: "Regional, El Salvador, CALA, Mixed demand\n\nSan Miguel, El Salvador\n\nUpper-midscale · conversion-friendly\n\nSecondary city CALA\n\nComfort Inn property page for eastern El Salvador—secondary-city CALA growth pattern.\n\nhttps://www.choicehotels.com/el-salvador/san-miguel/comfort-inn-hotels/sv004",
      sort: 2,
      caseSummaryOverview: "San Miguel: Comfort Inn in secondary Salvadoran market on choicehotels.com.",
      caseSummaryOwnerObjective: "Illustrates CALA expansion beyond capital-only gateways.",
      caseSummaryBrandRelevance: "Upper-midscale Choice retail in smaller CALA metros.",
      caseSummaryInterpretation: "Secondary-city demand mix requires local comp study—not capital proxy.",
      caseSummaryTags: "El Salvador, CALA, Secondary city",
    },
  ],
  "Quality Inn": [
    {
      title: "Quality Inn — Santo Domingo, Dominican Republic",
      body: "Urban, Dominican Republic, CALA, Business & leisure\n\nSanto Domingo, Dominican Republic\n\nMidscale · meetings-capable corridor\n\nCapital metro CALA\n\nQuality Inn in Santo Domingo—midscale Choice flag in Caribbean capital.\n\nhttps://www.choicehotels.com/dominican-republic/santo-domingo/quality-inn-hotels/do002",
      sort: 0,
      caseSummaryOverview: "Santo Domingo: Quality Inn listed on choicehotels.com in Dominican capital.",
      caseSummaryOwnerObjective: "Midscale CALA urban reference for conversion and NC screening.",
      caseSummaryBrandRelevance: "Core Choice midscale with CALA distribution—not US-only.",
      caseSummaryInterpretation: "Capital compression and fees differ by zone—underwrite your submarket.",
      caseSummaryTags: "Dominican Republic, CALA, Santo Domingo, Midscale",
    },
    {
      title: "Quality Inn — San José, Costa Rica",
      body: "Urban, Costa Rica, CALA, Corporate transient\n\nSan José, Costa Rica\n\nMidscale · airport/city access\n\nCentral America hub\n\nQuality Inn San José corridor property on Choice consumer site.\n\nhttps://www.choicehotels.com/costa-rica/san-jose/quality-inn-hotels/cr010",
      sort: 1,
      caseSummaryOverview: "San José: Quality Inn in Costa Rica primary business corridor.",
      caseSummaryOwnerObjective: "Benchmark midscale CALA urban transient and tour demand.",
      caseSummaryBrandRelevance: "Shows Quality Inn participation in Central American hub markets.",
      caseSummaryInterpretation: "Use local comps for RevPAR—directory listing is not performance data.",
      caseSummaryTags: "Costa Rica, CALA, San José, Midscale",
    },
  ],
  "Sleep Inn": [
    {
      title: "Sleep Inn — San José, Costa Rica",
      body: "Urban, Costa Rica, CALA, Value midscale\n\nSan José, Costa Rica\n\nMidscale · efficient prototype\n\nCentral America hub\n\nSleep Inn listed in San José—midscale efficient box in CALA hub.\n\nhttps://www.choicehotels.com/costa-rica/san-jose/sleep-inn-hotels/cr013",
      sort: 0,
      caseSummaryOverview: "San José: Sleep Inn on choicehotels.com in Costa Rica.",
      caseSummaryOwnerObjective: "Midscale CALA urban reference for NC/conversion economics.",
      caseSummaryBrandRelevance: "Sleep Inn CALA footprint complements US-centric system counts.",
      caseSummaryInterpretation: "Validate Costa Rica fees and PIP separately from US prototype.",
      caseSummaryTags: "Costa Rica, CALA, Sleep Inn",
    },
    {
      title: "Sleep Inn — Cuajimalpa, Mexico City area",
      body: "Urban, Mexico, CALA, Metro adjacency\n\nCuajimalpa de Morelos, Mexico (Mexico City area)\n\nMidscale · urban infill\n\nMexico metro CALA\n\nSleep Inn property serving greater Mexico City west corridor.\n\nhttps://www.choicehotels.com/mexico/cuajimalpa-de-morelos/sleep-inn-hotels/mx108",
      sort: 1,
      caseSummaryOverview: "Mexico City area: Sleep Inn CALA listing in Cuajimalpa corridor.",
      caseSummaryOwnerObjective: "Shows midscale Choice flag in major Mexican metro.",
      caseSummaryBrandRelevance: "Urban Mexico infill versus resort corridor plays.",
      caseSummaryInterpretation: "Metro access and parking drive economics—model locally.",
      caseSummaryTags: "Mexico, CALA, Mexico City, Sleep Inn",
    },
  ],
  "Econo Lodge": [
    {
      title: "Econo Lodge — Mexico corridor (Resville area)",
      body: "Highway, Mexico, CALA, Value transient\n\nResville area, Mexico (per Choice directory slug)\n\nEconomy · price-sensitive transient\n\nCorridor capture\n\nEcono Lodge property listed on Choice Mexico consumer paths—economy CALA corridor reference.\n\nhttps://www.choicehotels.com/mexico/resville/econo-lodge-hotels/tr503",
      sort: 0,
      caseSummaryOverview: "Mexico: Econo Lodge listing on choicehotels.com CALA directory.",
      caseSummaryOwnerObjective: "Economy highway/corridor CALA positioning under Choice stack.",
      caseSummaryBrandRelevance: "Value tier CALA participation—OTA discipline critical.",
      caseSummaryInterpretation: "Confirm exact city/market from LOI—directory slug may differ from marketing name.",
      caseSummaryTags: "Mexico, CALA, Economy, Econo Lodge",
    },
  ],
};

CURATED_BY_PROFILE["Radisson RED  (Choice)"] = [
  {
    title: "Radisson RED Minneapolis",
    body: "Urban, United States, North America, OUIBar + KTCHN\n\nMinneapolis, Minnesota\n\nUpscale select-service · urban social hub\n\nReference property · flex F&B & design\n\nFlagship North American RED with OUIBar + KTCHN communal bar-food space and bold lobby experience—illustrative of playful urban positioning under Choice in the Americas.\n\nUrban RED economics hinge on flex F&B labor and social-lobby activation—not full-service kitchen capex. Model Minneapolis-style urban ADR and event/leisure mix for your market.\n\nhttps://www.choicehotels.com/minnesota/minneapolis/radisson-red-hotels/mn290",
    sort: 0,
    caseSummaryOverview:
      "Minneapolis: North American RED reference with OUIBar + KTCHN and urban social lobby—press-kit photography market.",
    caseSummaryOwnerObjective:
      "Benchmark flex deli-bar F&B and informal service model for U.S. urban conversion or NC.",
    caseSummaryBrandRelevance:
      "Shows RED playful upscale select-service in a major U.S. city—not Radisson core full-service.",
    caseSummaryInterpretation:
      "One urban reference does not replace your comp set—confirm fees, PIP, and flex F&B operating plan locally.",
    caseSummaryTags: "United States, Urban, OUIBar, Reference",
  },
  {
    title: "Radisson RED Miami Airport",
    body: "Urban gateway, United States, North America, Airport\n\nMiami, Florida (airport corridor)\n\nUpscale select-service · transient + leisure\n\nGateway urban · social lobby\n\nAirport-adjacent RED with lobby and urban design personality—illustrative of gateway transient and bleisure demand under RED informal service standards.\n\nAirport urban sites need strong lobby throughput, 24/7 fitness, and Wi-Fi infrastructure—stress-test fee stack and OTA versus member mix.\n\nhttps://www.choicehotels.com/florida/miami/radisson-red-hotels/flj13",
    sort: 1,
    caseSummaryOverview:
      "Miami Airport corridor: RED upscale select-service at a gateway—lobby-led social experience for transient guests.",
    caseSummaryOwnerObjective:
      "Compare airport urban RED against conventional upscale select-service flags on flex F&B and design PIP.",
    caseSummaryBrandRelevance: "Gateway urban fit for RED—not resort Blu or Individuals soft collection.",
    caseSummaryInterpretation:
      "Airport compression varies by season and airline mix—underwrite from local airport comps, not press photos alone.",
    caseSummaryTags: "United States, Airport, Urban gateway",
  },
  {
    title: "Radisson RED Campinas",
    body: "Urban, Brazil, CALA, Social F&B\n\nCampinas, São Paulo state, Brazil\n\nUpscale select-service · CALA growth\n\nLounge RØD Grainne's · urban culture\n\nCampinas RED with Lounge RØD Grainne's and local F&B personality—illustrative CALA urban social hub under Choice-affiliated Radisson RED in Brazil.\n\nCALA urban RED requires flex F&B execution and design PIP aligned to brand playfulness—confirm Choice agreement scope and RHG ownership outside Americas.\n\nhttps://www.choicehotels.com/brazil/campinas/radisson-red-hotels/br157",
    sort: 2,
    caseSummaryOverview:
      "Campinas, Brazil: CALA RED with lounge-led social F&B—press-kit imagery for Brazil urban lifestyle positioning.",
    caseSummaryOwnerObjective:
      "Reference for Brazilian urban conversion economics and RED flex F&B versus core Radisson full-service.",
    caseSummaryBrandRelevance:
      "CALA pipeline context alongside 4 open / 5 development Americas scale cited in press materials.",
    caseSummaryInterpretation: "Brazil fees, labor, and FX differ from U.S. references—local counsel and comp set required.",
    caseSummaryTags: "Brazil, CALA, Urban, Social F&B",
  },
  {
    title: "Radisson RED — Rosario, Argentina",
    body: "Urban, Argentina, CALA, Social F&B\n\nRosario, Santa Fe, Argentina\n\nUpscale select-service · urban social hub\n\nCALA urban · Lounge RØD personality\n\nRadisson RED listed on choicehotels.com in Rosario—CALA urban social-hotel pattern under Choice-affiliated distribution.\n\nhttps://www.choicehotels.com/argentina/rosario/radisson-red-hotels/aa024",
    sort: 3,
    caseSummaryOverview: "Rosario: Radisson RED property page on Choice CALA consumer paths.",
    caseSummaryOwnerObjective: "Benchmark CALA urban RED flex F&B and lobby activation economics.",
    caseSummaryBrandRelevance: "Real RED CALA listing—not a US-only reference card.",
    caseSummaryInterpretation: "Confirm Argentina fees and PIP in LOI; census URL is directory anchor.",
    caseSummaryTags: "Argentina, CALA, Rosario, Radisson RED",
  },
  {
    title: "Radisson RED — Miraflores, Peru",
    body: "Urban, Peru, CALA, Coastal business & leisure\n\nMiraflores, Lima, Peru\n\nUpscale select-service · Pacific urban\n\nLima metro CALA\n\nRED property in Miraflores corridor on choicehotels.com—Andean Pacific urban upscale select-service named in Jul 2023 Choice integration press.\n\nhttps://www.choicehotels.com/peru/miraflores/radisson-red-hotels/pe012",
    sort: 4,
    caseSummaryOverview: "Miraflores, Lima: Radisson RED on choicehotels.com in Peru.",
    caseSummaryOwnerObjective: "Andean metro upscale select-service CALA reference for RED conversions.",
    caseSummaryBrandRelevance: "Published RED CALA inventory for owner diligence.",
    caseSummaryInterpretation: "Lima compression and seasonality require local comp study.",
    caseSummaryTags: "Peru, CALA, Lima, Radisson RED",
  },
];
CURATED_BY_PROFILE["Radisson Individual (Choice)"] = [
  {
    title: "Radisson Individuals — Medellín, Colombia",
    body: "Urban, Colombia, CALA, Soft collection\n\nMedellín, Colombia (El Poblado context)\n\nUpper-upscale collection · lifestyle\n\nCALA urban lifestyle\n\nV Grand / Individuals Medellín cited in Choice press—collection-tier CALA growth in trendy El Poblado.\n\nhttps://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030",
    sort: 0,
    caseSummaryOverview: "Medellín: Radisson Individuals member on choicehotels.com.",
    caseSummaryOwnerObjective: "Collection-tier CALA urban lifestyle economics and PIP scope.",
    caseSummaryBrandRelevance: "Real Individuals CALA listing—not illustrative gateway copy.",
    caseSummaryInterpretation: "Member agreements differ from core Radisson—confirm collection terms.",
    caseSummaryTags: "Colombia, CALA, Medellín, Individuals",
  },
  {
    title: "Radisson Individuals — Cartagena, Colombia",
    body: "Urban heritage, Colombia, CALA, Boutique leisure\n\nCartagena, Colombia\n\nSoft collection · historic core adjacency\n\nCaribbean heritage CALA\n\nIndividuals member property in Cartagena—heritage leisure CALA boutique pattern.\n\nhttps://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb018",
    sort: 1,
    caseSummaryOverview: "Cartagena: Individuals CALA listing on Choice consumer site.",
    caseSummaryOwnerObjective: "Heritage conversion and collection compliance in Caribbean Colombia.",
    caseSummaryBrandRelevance: "Individuals CALA footprint for soft-brand sponsors.",
    caseSummaryInterpretation: "Heritage PIP and preservation costs dominate returns—model locally.",
    caseSummaryTags: "Colombia, CALA, Cartagena, Individuals",
  },
  {
    title: "Radisson Individuals — Panama City, Panama",
    body: "Urban, Panama, CALA, Corporate & leisure\n\nPanama City, Panama\n\nSoft collection · capital metro\n\nGateway CALA\n\nIndividuals listing in Panama City capital corridor on choicehotels.com.\n\nhttps://www.choicehotels.com/panama/panama-city/radisson-individuals-hotels/pa006",
    sort: 2,
    caseSummaryOverview: "Panama City: Individuals CALA property on Choice directory.",
    caseSummaryOwnerObjective: "Capital gateway collection economics versus full-service Radisson.",
    caseSummaryBrandRelevance: "Confirms Individuals CALA inventory in Central America hub.",
    caseSummaryInterpretation: "Gateway fees and contract mix differ from resort Individuals plays.",
    caseSummaryTags: "Panama, CALA, Panama City, Individuals",
  },
];
CURATED_BY_PROFILE["Country Inn & Suites by Radisson (Choice)"] = [
  {
    title: "Country Inn & Suites by Radisson, San Jose Aeropuerto, Costa Rica",
    body: "Airport, Costa Rica, CALA, Corporate transient\n\nSan José–Heredia corridor, Costa Rica (near Juan Santamaría International Airport)\n\nUpper-midscale · breakfast-led · Radisson family\n\nCentral America gateway · airport capture\n\nListed on choicehotels.com as Country Inn & Suites by Radisson, San Jose Aeropuerto—upper-midscale Radisson-family CALA footprint serving business and leisure travelers in Costa Rica's capital corridor.\n\nhttps://www.choicehotels.com/costa-rica/san-jose-heredia/country-hotels/cr022",
    sort: 0,
    caseSummaryOverview: "Heredia corridor, Costa Rica: Country Inn & Suites by Radisson, San Jose Aeropuerto on Choice CALA consumer paths.",
    caseSummaryOwnerObjective: "Shows how Country Inn anchors a CALA airport/corporate-transient node under Choice distribution.",
    caseSummaryBrandRelevance: "Real Country Inn CALA listing—not Comfort Inn comp placeholder.",
    caseSummaryInterpretation: "Validate Costa Rica authorization and Item 19 tables in disclosure.",
    caseSummaryTags: "Costa Rica, CALA, Airport, Country Inn",
  },
];
CURATED_BY_PROFILE["Ascend Hotel Collection"] = [
  {
    title: "Amberes 64 Ascend Hotel Collection — Mexico City",
    body: "Urban boutique, Mexico, CALA, Conversion, Zona Rosa\n\nMexico City, Mexico (Zona Rosa)\n\nConversion · 64 suites · kitchenettes, restaurant, bar\n\nChoice Hotels CALA inaugurated Amberes Seis Cuatro in July 2024—boutique soft-collection conversion minutes from Ángel de la Independencia with gym, steam, and social spaces.\n\nhttps://www.choicehotels.com/mexico/mexico-city/ascend-hotels/mx228",
    sort: 0,
    caseSummaryOverview:
      "Zona Rosa, Mexico City: Amberes 64 Ascend—inaugurated Jul 2024 per Choice Hotels CALA press; 64-suite boutique conversion on choicehotels.com.",
    caseSummaryOwnerObjective:
      "Independent boutique conversion with local F&B and design character under Ascend collection standards—not a rigid prototype.",
    caseSummaryBrandRelevance: "Flagship CALA Ascend inauguration story—soft brand with kitchenette suites and urban social spaces.",
    caseSummaryInterpretation: "Validate conversion PIP, Zona Rosa comp set, and fee stack locally—press is positioning, not audited results.",
    caseSummaryTags: "Mexico, CALA, Mexico City, Boutique, Conversion, Zona Rosa",
  },
  {
    title: "Grand Hotel Guayaquil Ascend Hotel Collection — Guayaquil",
    body: "Historic, Ecuador, CALA, Urban full-service, Waterfront\n\nGuayaquil, Ecuador\n\nLandmark conversion · meetings and F&B\n\nWaterfront historic hotel operating as Ascend member—Ecuador gateway full-service character with Choice distribution.\n\nhttps://www.choicehotels.com/ecuador/guayaquil/ascend-hotels/ec002",
    sort: 1,
    caseSummaryOverview: "Guayaquil: Grand Hotel Guayaquil Ascend—historic waterfront member on choicehotels.com.",
    caseSummaryOwnerObjective: "Landmark full-service identity with collection membership and loyalty retail.",
    caseSummaryBrandRelevance: "CALA Ascend historic urban conversion—not Cambria or hard-brand prototype.",
    caseSummaryInterpretation: "Heritage preservation and F&B complexity drive economics—model locally.",
    caseSummaryTags: "Ecuador, CALA, Guayaquil, Historic, Full-service",
  },
  {
    title: "Ascend Hotel Collection — Quito, Ecuador",
    body: "Urban heritage, Ecuador, CALA, Capital gateway\n\nQuito, Ecuador\n\nSoft collection · Andean capital\n\nAscend member in Ecuador's capital—local character with Choice consumer distribution.\n\nhttps://www.choicehotels.com/ecuador/quito/ascend-hotels/ec001",
    sort: 2,
    caseSummaryOverview: "Quito: Ascend Hotel Collection member on Choice CALA consumer paths.",
    caseSummaryOwnerObjective: "Capital gateway boutique or independent upscale with collection backing.",
    caseSummaryBrandRelevance: "Ecuador Ascend inventory alongside Guayaquil landmark—CALA soft-collection depth.",
    caseSummaryInterpretation: "Altitude-market seasonality and access patterns require local comps.",
    caseSummaryTags: "Ecuador, CALA, Quito, Capital gateway",
  },
  {
    title: "Ascend Hotel Collection — Juan Dolio Beach, Dominican Republic",
    body: "Resort, Dominican Republic, CALA, Beach leisure\n\nJuan Dolio Beach, Dominican Republic\n\nSoft collection · beach resort\n\nCaribbean leisure Ascend listing—lifestyle resort character with Choice distribution.\n\nhttps://www.choicehotels.com/dominican-republic/juan-dolio-beach/ascend-hotels/do012",
    sort: 3,
    caseSummaryOverview: "Juan Dolio Beach: Ascend CALA resort property on choicehotels.com.",
    caseSummaryOwnerObjective: "Beach leisure independent or conversion with collection flexibility versus hard-brand resort capex.",
    caseSummaryBrandRelevance: "Caribbean Ascend leisure proof point—soft collection in resort markets.",
    caseSummaryInterpretation: "Resort seasonality and amenity stack differ from urban boutique conversions.",
    caseSummaryTags: "Dominican Republic, CALA, Beach, Resort, Leisure",
  },
];
CURATED_BY_PROFILE["Radisson (Choice)"] = loadFixtureRows(
  "fixtures/brand-explorer-presentation-radisson-footprint-openings.json"
);
CURATED_BY_PROFILE["Radisson Blu (Choice)"] = loadFixtureRows(
  "fixtures/brand-explorer-presentation-radisson-blu-footprint-openings.json"
);

/** @deprecated Legacy comps — no longer applied; brands without slug-matched CALA URLs get zero rows. */
/** @type {Record<string, FootprintOpeningCard[]>} */
const NO_DIRECT_CALA_FALLBACK_LEGACY = {
  "Cambria Hotels": [
    {
      title: "Radisson Puebla Angelópolis (CALA upscale corridor comp)",
      body: "Meetings, Mexico, CALA, Urban upscale\n\nPuebla, Puebla, Mexico\n\nUpscale full-service · meetings\n\nCALA corridor reference\n\nCambria has no open CALA hotels on choicehotels.com today (US/Canada-focused pipeline per Choice press). This Radisson opening illustrates upscale CALA economics Cambria owners study when evaluating international growth.\n\nhttps://www.choicehotels.com/en-mx/mexico/puebla/radisson-hotels/mxpue",
      sort: 0,
      caseSummaryOverview:
        "Puebla Angelópolis Radisson (Oct 2024) is a real CALA upscale opening—used here because Cambria has no CALA property listings yet.",
      caseSummaryOwnerObjective:
        "Understand CALA upscale full-service meetings/F&B capex while Cambria remains US/Canada-weighted in public materials.",
      caseSummaryBrandRelevance:
        "Portfolio context only—do not present as a Cambria-flagged hotel.",
      caseSummaryInterpretation:
        "Confirm whether Cambria is authorized for your CALA market in LOI; first Canadian Cambria is planned, not CALA.",
      caseSummaryTags: "Mexico, CALA, Upscale comp, Cambria context",
    },
    {
      title: "Ascend — Juan Dolio Beach, Dominican Republic (CALA lifestyle comp)",
      body: "Resort, Dominican Republic, CALA, Leisure\n\nJuan Dolio Beach, Dominican Republic\n\nSoft collection · beach leisure\n\nCALA lifestyle reference\n\nAscend Hotel Collection CALA listing illustrates lifestyle/upscale leisure compression relevant to Cambria owners watching Caribbean growth.\n\nhttps://www.choicehotels.com/dominican-republic/juan-dolio-beach/ascend-hotels/do012",
      sort: 1,
      caseSummaryOverview:
        "Real Ascend CALA property—portfolio lifestyle reference while Cambria lacks CALA inventory on consumer site.",
      caseSummaryOwnerObjective: "Compare leisure CALA public-space investment needs versus Cambria prototype.",
      caseSummaryBrandRelevance: "Not Cambria—Choice CALA soft/upscale leisure pattern only.",
      caseSummaryInterpretation: "Use for corridor storytelling; Cambria fees and PIP come from Cambria FDD only.",
      caseSummaryTags: "Dominican Republic, CALA, Lifestyle comp",
    },
  ],
  "MainStay Suites": [
    {
      title: "Quality Inn — San José, Costa Rica (extended-stay corridor comp)",
      body: "Urban, Costa Rica, CALA, Weekly / transient mix\n\nSan José, Costa Rica\n\nMidscale hub · multi-night demand\n\nCALA reference\n\nMainStay has no CALA listings on choicehotels.com; Quality Inn San José shows midscale CALA urban demand patterns extended-stay owners evaluate for weekly mix.\n\nhttps://www.choicehotels.com/costa-rica/san-jose/quality-inn-hotels/cr010",
      sort: 0,
      caseSummaryOverview: "Real Quality Inn CALA listing used as urban demand reference for extended-stay feasibility.",
      caseSummaryOwnerObjective: "MainStay owners modeling CALA should confirm authorized geography and weekly-stay economics locally.",
      caseSummaryBrandRelevance: "Not MainStay—midscale CALA corridor comp only.",
      caseSummaryInterpretation: "Extended-stay CALA authorization may be limited—validate in disclosure before underwriting.",
      caseSummaryTags: "Costa Rica, CALA, Extended-stay context",
    },
  ],
  "WoodSpring Suites": [
    {
      title: "Suburban Studios — no CALA listing; Quality Inn San José reference",
      body: "Urban, Costa Rica, CALA, Multi-night urban\n\nSan José, Costa Rica\n\nMidscale urban · weekly potential\n\nCALA reference\n\nWoodSpring has no published CALA properties on choicehotels.com; this Quality Inn illustrates urban CALA transient demand extended-stay sponsors study.\n\nhttps://www.choicehotels.com/costa-rica/san-jose/quality-inn-hotels/cr010",
      sort: 0,
      caseSummaryOverview: "WoodSpring CALA footprint not listed on consumer site—urban CALA comp for context.",
      caseSummaryOwnerObjective: "Screen weekly-stay and kitchen economics against CALA urban demand, not US-only averages.",
      caseSummaryBrandRelevance: "Portfolio reference—not WoodSpring-flagged.",
      caseSummaryInterpretation: "Confirm Item 19 format for WoodSpring; CALA may be US-weighted in disclosure.",
      caseSummaryTags: "Costa Rica, CALA, Extended-stay context",
    },
  ],
  "Suburban Studios": [
    {
      title: "Quality Inn — Santo Domingo (economy-extended corridor comp)",
      body: "Urban, Dominican Republic, CALA, Value / midscale\n\nSanto Domingo, Dominican Republic\n\nMidscale capital metro\n\nCALA reference\n\nSuburban Studios has no CALA consumer-site listings; Santo Domingo Quality Inn shows capital-city midscale patterns relevant to economy-extended sponsors.\n\nhttps://www.choicehotels.com/dominican-republic/santo-domingo/quality-inn-hotels/do002",
      sort: 0,
      caseSummaryOverview: "Real CALA midscale capital property used because Suburban has no CALA directory listings.",
      caseSummaryOwnerObjective: "Economy-extended CALA feasibility requires local weekly-mix study—not this hotel's flag.",
      caseSummaryBrandRelevance: "Comp only—not Suburban Studios.",
      caseSummaryInterpretation: "Validate whether Suburban is offered in your CALA market in LOI.",
      caseSummaryTags: "Dominican Republic, CALA, Economy-extended context",
    },
  ],
  "Everhome Suites": [
    {
      title: "Sleep Inn — Cuajimalpa, Mexico (newer extended system comp)",
      body: "Urban, Mexico, CALA, Metro corridor\n\nCuajimalpa de Morelos, Mexico\n\nMidscale urban\n\nCALA reference\n\nEverhome has no CALA listings; Sleep Inn Mexico City area shows urban CALA midscale demand for newer extended-stay system comparisons.\n\nhttps://www.choicehotels.com/mexico/cuajimalpa-de-morelos/sleep-inn-hotels/mx108",
      sort: 0,
      caseSummaryOverview: "Everhome CALA properties not on choicehotels.com—urban Mexico midscale comp.",
      caseSummaryOwnerObjective: "Newer extended-stay system: diligence CALA separately from US pipeline.",
      caseSummaryBrandRelevance: "Not Everhome—urban CALA midscale reference.",
      caseSummaryInterpretation: "Item 19 may lack CALA performance tables—use market study.",
      caseSummaryTags: "Mexico, CALA, Extended-stay context",
    },
  ],
  "Park Plaza (Choice)": [
    {
      title: "Radisson Blu — Palm Beach, Aruba (upper-upscale CALA resort comp)",
      body: "Resort, Aruba, CALA, Beachfront leisure\n\nPalm Beach, Aruba\n\nUpper-upscale resort\n\nCALA leisure reference\n\nPark Plaza has minimal CALA consumer listings; Blu Aruba illustrates upper-upscale resort CALA execution under Choice-affiliated distribution.\n\nhttps://www.choicehotels.com/aruba/palm-beach/radisson-blu-hotels/aw007",
      sort: 0,
      caseSummaryOverview: "Real Radisson Blu CALA resort—portfolio comp for Park Plaza upscale positioning.",
      caseSummaryOwnerObjective: "Resort CALA capex and seasonality benchmark—not Park Plaza-specific.",
      caseSummaryBrandRelevance: "Not Park Plaza—Choice CALA upper-upscale resort pattern.",
      caseSummaryInterpretation: "Confirm Park Plaza authorization in CALA if marketed in LOI.",
      caseSummaryTags: "Aruba, CALA, Resort comp, Upscale",
    },
    {
      title: "Radisson Individuals — Medellín (collection comp)",
      body: "Urban, Colombia, CALA, Soft collection\n\nMedellín, Colombia (El Poblado context)\n\nUpper-upscale collection\n\nCALA urban lifestyle\n\nV Grand / Individuals Medellín openings cited in Choice press—collection-tier CALA growth reference for luxury-collection positioning.\n\nhttps://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030",
      sort: 1,
      caseSummaryOverview: "Medellín: Radisson Individuals CALA listing—soft collection in Andean metro.",
      caseSummaryOwnerObjective: "Park Plaza owners compare full-service upscale versus collection tiers in CALA.",
      caseSummaryBrandRelevance: "Individuals comp—not Park Plaza flag.",
      caseSummaryInterpretation: "Luxury-collection CALA supply is thin—underwrite from disclosure.",
      caseSummaryTags: "Colombia, CALA, Medellín, Collection comp",
    },
  ],
  "Radisson Collection  (Choice)": [
    {
      title: "Radisson Individuals — Cartagena (heritage collection comp)",
      body: "Urban heritage, Colombia, CALA, Boutique\n\nCartagena, Colombia\n\nUpper-upscale soft brand\n\nHistoric leisure CALA\n\nRadisson Individuals member property in Cartagena—collection-tier CALA heritage boutique pattern.\n\nhttps://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb018",
      sort: 0,
      caseSummaryOverview: "Cartagena Individuals member—real CALA soft-collection listing.",
      caseSummaryOwnerObjective: "Collection CALA comp for Radisson Collection luxury positioning.",
      caseSummaryBrandRelevance: "Not Radisson Collection—Individuals tier reference.",
      caseSummaryInterpretation: "Luxury collection CALA is selective—confirm brand tier in agreement.",
      caseSummaryTags: "Colombia, CALA, Cartagena, Collection",
    },
    {
      title: "Radisson Blu — Santiago, Chile (urban upscale comp)",
      body: "Urban, Chile, CALA, Business district\n\nSantiago, Chile\n\nUpper-upscale · meetings\n\nCapital metro\n\nBlu Santiago illustrates upper-upscale urban CALA meetings product relevant to collection-tier sponsors.\n\nhttps://www.choicehotels.com/chile/santiago/radisson-blu-hotels/cl012",
      sort: 1,
      caseSummaryOverview: "Santiago Radisson Blu—real CALA upper-upscale urban listing.",
      caseSummaryOwnerObjective: "Compare urban upscale CALA versus resort Individuals plays.",
      caseSummaryBrandRelevance: "Blu comp for Collection tier economics—not same flag.",
      caseSummaryInterpretation: "Collection vs Blu fee stacks differ—use correct FDD.",
      caseSummaryTags: "Chile, CALA, Santiago, Upscale comp",
    },
  ],
  "Radisson Inn & Suites": [
    {
      title: "Park Inn by Radisson — Antofagasta, Chile",
      body: "Urban, Chile, CALA, Mining / corporate corridor\n\nAntofagasta, Chile\n\nUpper-midscale · corporate transient\n\nNorthern Chile gateway\n\nPark Inn CALA listing in Antofagasta—upper-midscale Radisson-family box in mining corridor.\n\nhttps://www.choicehotels.com/chile/antofagasta/park-inn-hotels/cl023",
      sort: 0,
      caseSummaryOverview: "Antofagasta Park Inn—real CALA upper-midscale property (Radisson family).",
      caseSummaryOwnerObjective: "Radisson Inn & Suites owners compare new-build upper-midscale versus Park Inn tier in CALA.",
      caseSummaryBrandRelevance: "Park Inn comp within Radisson family—not Inn & Suites flag.",
      caseSummaryInterpretation: "Inn & Suites CALA listings may be limited—confirm in Item 20.",
      caseSummaryTags: "Chile, CALA, Antofagasta, Park Inn",
    },
    {
      title: "Park Inn by Radisson — Quito, Ecuador",
      body: "Urban, Ecuador, CALA, Capital metro\n\nQuito, Pichincha, Ecuador\n\nUpper-midscale · altitude capital\n\nAndean gateway\n\nPark Inn Quito corridor—upper-midscale CALA capital reference.\n\nhttps://www.choicehotels.com/ecuador/quito-pichincha/park-inn-hotels/ec005",
      sort: 1,
      caseSummaryOverview: "Quito Park Inn listing on choicehotels.com—Andean capital upper-midscale.",
      caseSummaryOwnerObjective: "Benchmark Radisson-family upper-midscale in Ecuador for Inn & Suites comparisons.",
      caseSummaryBrandRelevance: "Same family, different sub-brand tier.",
      caseSummaryInterpretation: "Altitude market seasonality unique—local comps required.",
      caseSummaryTags: "Ecuador, CALA, Quito, Park Inn",
    },
  ],
  "Clarion Pointe": [
    {
      title: "Clarion — French Harbour Roatan (CALA midscale comp)",
      body: "Island, Honduras, CALA, Leisure\n\nFrench Harbour Roatan, Honduras\n\nMidscale upper · island leisure\n\nCALA reference\n\nClarion Pointe has no separate CALA listings on choicehotels.com; this Clarion property illustrates CALA midscale island economics in the same tier family.\n\nhttps://www.choicehotels.com/honduras/french-harbour-roatan/clarion-hotels/hn011",
      sort: 0,
      caseSummaryOverview: "Honduras: Clarion CALA listing used as tier-family reference for Clarion Pointe sponsors.",
      caseSummaryOwnerObjective: "Screen CALA midscale island feasibility—confirm Clarion Pointe authorization in LOI.",
      caseSummaryBrandRelevance: "Clarion peer—not Clarion Pointe flag.",
      caseSummaryInterpretation: "Pointe may be US-weighted in Item 20—validate geography before underwriting.",
      caseSummaryTags: "Honduras, CALA, Clarion family",
    },
  ],
  "Rodeway Inn": [
    {
      title: "Econo Lodge — Mexico corridor (economy CALA peer)",
      body: "Highway, Mexico, CALA, Value transient\n\nMexico (Resville directory area)\n\nEconomy tier\n\nCorridor economics\n\nRodeway and Econo Lodge share economy CALA corridor dynamics—Econo listing on Choice Mexico paths.\n\nhttps://www.choicehotels.com/mexico/resville/econo-lodge-hotels/tr503",
      sort: 0,
      caseSummaryOverview: "Economy CALA directory listing—peer tier reference for Rodeway corridor strategy.",
      caseSummaryOwnerObjective: "Economy CALA: OTA and net RevPAR discipline versus fee stack.",
      caseSummaryBrandRelevance: "Econo peer—not Rodeway property.",
      caseSummaryInterpretation: "Confirm Rodeway CALA authorization for your site in LOI.",
      caseSummaryTags: "Mexico, CALA, Economy",
    },
  ],
};

const MAX_OPENINGS = 8;

/**
 * @param {string} profileName — canonical profile key (Brand Basics name in fixtures)
 * @returns {FootprintOpeningCard[]}
 */
export function buildCalaOpeningsForProfile(profileName) {
  const key = profileKey(profileName);
  if (BRANDS_NO_CALA_OPENINGS.has(key)) return [];

  const curated = (CURATED_BY_PROFILE[key] || []).filter((card) => {
    const url = extractUrlFromBody(card.body);
    return !url || urlMatchesBrandSlug(key, url);
  });
  const censusUrls = selectDiversifiedUrlsForBrand(key, MAX_OPENINGS);

  /** @type {FootprintOpeningCard[]} */
  const out = [];
  const seenUrls = new Set();

  for (const card of curated) {
    if (out.length >= MAX_OPENINGS) break;
    const url = extractUrlFromBody(card.body);
    if (url && !urlMatchesBrandSlug(key, url)) continue;
    if (url && seenUrls.has(url)) continue;
    if (url) seenUrls.add(url);
    out.push({ ...card, sort: out.length });
  }

  for (const url of censusUrls) {
    if (out.length >= MAX_OPENINGS) break;
    if (!urlMatchesBrandSlug(key, url)) continue;
    if (seenUrls.has(url)) continue;
    const { city, country } = parseUrl(url);
    const placeKey = `${city}|${country}`.toLowerCase();
    if (out.some((card) => {
      const u = extractUrlFromBody(card.body);
      if (!u) return false;
      const p = parseUrl(u);
      return `${p.city}|${p.country}`.toLowerCase() === placeKey;
    })) continue;
    seenUrls.add(url);
    out.push(cardFromUrl(url, key, out.length));
  }

  return out;
}
