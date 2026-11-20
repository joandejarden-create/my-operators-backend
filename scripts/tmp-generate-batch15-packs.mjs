#!/usr/bin/env node
/** One-shot generator for Batch 15 depth packs (Minor + Meliá + Barceló). */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const outPath = join(ROOT, "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch15-v1.js");
const S = "S";
const M = "M";
const L = "L";
const N = "N";

/** @type {Array<object>} */
const brands = [
  // Minor
  {
    id: "recJPlQbmat5KZDPe",
    name: "Anantara Hotels & Resorts",
    live: "Anantara",
    slug: "anantara-hotels-resorts",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "Anantara",
    feeGeo: "Confirm current Minor Hotels management / development disclosure for Anantara (luxury). Confirm GHA DISCOVERY participation terms.",
    brandPage: "https://www.anantara.com/",
    positioning: "Luxury Destination Resorts & Urban",
    commercial:
      "Versus Avani select-service, Anantara sells harder luxury destination and urban hospitality; versus Tivoli heritage luxury, Anantara is Minor's contemporary luxury flag with GHA DISCOVERY.",
    scenarios: [
      ["Luxury Destination Resort", "Resorts funding Anantara spa, F&B, and service intensity."],
      ["Gateway Luxury Urban", "Gateways where Anantara luxury and DISCOVERY clear ADR."],
      ["Conversion Into Anantara Luxury", "Conversions only when luxury PIP and service clear Anantara QA."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Anantara is built for luxury destination resorts."],
      ["Luxury Urban Flagship", S, "Select urban luxury flags fit Anantara with DISCOVERY."],
      ["Conversion & Repositioning", M, "Conversions succeed only with true luxury PIP."],
      ["Meetings & Group Compression", M, "Meetings fit when luxury full-service plant exists."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Anantara luxury."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Anantara."],
    ],
    ops: "Minor Hotels luxury Anantara with GHA DISCOVERY. Confirm brand-level program participation—do not invent fees. Preserve live name Anantara.",
    footprint: "Confirm live Anantara counts from current Minor disclosures—APAC / Middle East / selective global luxury footprint.",
    insight: "Anantara fits luxury destination sponsors with DISCOVERY; fails when Avani select-service capital is assumed.",
  },
  {
    id: "recXVvsweIigv064m",
    name: "Tivoli Hotels & Resorts",
    live: "Tivoli Hotels & Resorts",
    slug: "tivoli-hotels-resorts",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "Tivoli",
    feeGeo: "Confirm current Minor Hotels development / management disclosure for Tivoli (luxury / upper-upscale). Confirm GHA DISCOVERY participation.",
    brandPage: "https://www.tivolihotels.com/",
    positioning: "Luxury & Upper-Upscale Portugal / Brazil Heritage",
    commercial:
      "Versus Anantara contemporary luxury, Tivoli sells heritage Portugal / Brazil luxury and upper-upscale; versus NH Collection upscale, Tivoli is harder luxury intensity with DISCOVERY.",
    scenarios: [
      ["Portugal / Brazil Luxury Heritage", "Markets funding Tivoli heritage luxury product."],
      ["Resort & Urban Upper-Upscale", "Resorts and cities where Tivoli ADR clears IC."],
      ["Conversion Into Tivoli", "Conversions clearing Tivoli heritage / luxury PIP."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Tivoli fits Portugal / Brazil leisure luxury destinations."],
      ["Luxury Urban Flagship", S, "Heritage urban flags are a core Tivoli lane."],
      ["Meetings & Group Compression", M, "Meetings fit when full-service plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed at luxury / upper-upscale capital."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Tivoli."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Tivoli."],
    ],
    ops: "Minor Hotels Tivoli — luxury / upper-upscale with GHA DISCOVERY. Confirm participation and fee exhibits. Strong Portugal / Brazil concentration.",
    footprint: "Confirm live Tivoli counts from current Minor disclosures—Portugal / Brazil weighted with selective growth.",
    insight: "Tivoli fits heritage luxury sponsors in Portugal / Brazil with DISCOVERY; fails when NH midscale capital is assumed.",
  },
  {
    id: "NEW_nh-collection",
    name: "NH Collection Hotels & Resorts",
    live: "NH Collection Hotels & Resorts",
    slug: "nh-collection-hotels-resorts",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "NH Collection",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Minor / NH development disclosure for NH Collection (upscale). Confirm GHA DISCOVERY vs legacy NH Rewards integration.",
    brandPage: "https://www.nh-collection.com/",
    positioning: "Upscale Full-Service Collection",
    commercial:
      "Versus NH Hotels midscale-upscale, NH Collection sells higher upscale design intensity; versus nhow lifestyle, Collection is classic upscale full-service with DISCOVERY.",
    scenarios: [
      ["European Upscale Urban", "Cities where NH Collection ADR and DISCOVERY clear IC."],
      ["Meetings Upscale Full-Service", "Markets funding Collection meetings plant."],
      ["Conversion Into NH Collection", "Conversions clearing upscale Collection PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "NH Collection fits productive European upscale mix."],
      ["Meetings & Group Compression", S, "Meetings are a core Collection lane."],
      ["Conversion & Repositioning", M, "Conversions succeed at upscale capital."],
      ["Luxury Urban Flagship", L, "True luxury sits closer to Anantara / Tivoli."],
      ["Resort & Destination Leisure", M, "Select resort Collections can fit."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Collection."],
    ],
    ops: "Minor / NH Collection upscale with GHA DISCOVERY. NEW Basics create requires founder auth. Confirm loyalty integration—do not assume identical NH Rewards economics across all Minor brands.",
    footprint: "Confirm live NH Collection counts from current Minor / NH disclosures—Europe weighted.",
    insight: "NH Collection fits European upscale sponsors with DISCOVERY; fails when NH midscale or Anantara luxury fees are assumed.",
  },
  {
    id: "recZZlLvzB7NDs8yB",
    name: "NH Hotels & Resorts",
    live: "NH Hotels",
    slug: "nh-hotels-resorts",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "NH Hotels",
    feeGeo: "Confirm current Minor / NH development / franchise disclosure for NH Hotels. Confirm GHA DISCOVERY participation and any legacy NH Rewards transition terms.",
    brandPage: "https://www.nh-hotels.com/",
    positioning: "Upper-Midscale / Upscale Urban Europe",
    commercial:
      "Versus NH Collection upscale, NH Hotels sells broader upper-midscale / upscale urban; versus Avani select-service lifestyle, NH is classic European urban hospitality with DISCOVERY.",
    scenarios: [
      ["European Urban Corporate", "Cities where NH Hotels ADR and DISCOVERY clear IC."],
      ["Meetings / Transient Mix", "Markets funding NH meetings and transient mix."],
      ["Conversion Into NH", "Conversions clearing NH brand standards."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "NH Hotels fits productive European urban / suburban mix."],
      ["Meetings & Group Compression", S, "Meetings are a core NH lane."],
      ["Airport & Transportation", M, "Airport / gateway NH assets can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at upper-midscale capital."],
      ["Luxury Urban Flagship", L, "Luxury flags sit above NH Hotels."],
      ["Highway & Interstate Transient", L, "Pure highway limited-service is usually not NH's core."],
    ],
    ops: "Minor NH Hotels — European urban upper-midscale / upscale with GHA DISCOVERY. Preserve live name NH Hotels. Confirm franchise / management exhibits and loyalty transition.",
    footprint: "Confirm live NH Hotels counts from current Minor / NH disclosures—Europe-concentrated with selective global growth.",
    insight: "NH Hotels fits European urban sponsors with DISCOVERY; fails when NH Collection upscale or Anantara luxury fees are assumed.",
  },
  {
    id: "NEW_nhow",
    name: "nhow Hotels & Resorts",
    live: "nhow Hotels & Resorts",
    slug: "nhow-hotels-resorts",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "nhow",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Minor / NH development disclosure for nhow (lifestyle). Confirm GHA DISCOVERY participation.",
    brandPage: "https://www.nhow-hotels.com/",
    positioning: "Design-Led Urban Lifestyle",
    commercial:
      "Versus NH Collection classic upscale, nhow sells design-led lifestyle; versus ME by Meliá lifestyle, nhow is Minor's lifestyle node with DISCOVERY.",
    scenarios: [
      ["Urban Lifestyle Flag", "Creative cities funding nhow design product."],
      ["Lifestyle Conversion", "Assets clearing nhow design PIP."],
      ["Leisure Urban Stay", "Cities where lifestyle ADR clears IC with DISCOVERY."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Design-led conversions are a core nhow path."],
      ["Suburban Business & Leisure", M, "Select urban-edge lifestyle sites can fit."],
      ["Luxury Urban Flagship", M, "Lifestyle flags fit; ultra-luxury does not."],
      ["Meetings & Group Compression", L, "Large meetings are secondary to nhow lifestyle."],
      ["Resort & Destination Leisure", L, "Pure resorts sit closer to Anantara / Tivoli."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside nhow."],
    ],
    ops: "Minor nhow — design-led urban lifestyle with GHA DISCOVERY. NEW Basics create requires founder auth. Confirm participation; do not invent fees.",
    footprint: "Confirm live nhow counts from current Minor / NH disclosures—selective European / global lifestyle set.",
    insight: "nhow fits urban lifestyle sponsors with DISCOVERY; fails when NH midscale or Anantara luxury fees are assumed.",
  },
  {
    id: "NEW_avani",
    name: "Avani Hotels & Resorts",
    live: "Avani Hotels & Resorts",
    slug: "avani-hotels-resorts",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "Avani",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Minor Hotels development disclosure for Avani (upscale select / lifestyle). Confirm GHA DISCOVERY participation.",
    brandPage: "https://www.avanihotels.com/",
    positioning: "Upscale Select / Lifestyle",
    commercial:
      "Versus Anantara luxury, Avani sells upscale select / lifestyle efficiency; versus Oaks apartments, Avani is hotel-first select with DISCOVERY.",
    scenarios: [
      ["Upscale Select Urban / Resort", "Markets where Avani ADR and DISCOVERY clear IC."],
      ["Lifestyle Conversion", "Conversions clearing Avani select standards."],
      ["Leisure Select Destination", "Destinations funding Avani leisure product."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Avani fits productive upscale select mix."],
      ["Resort & Destination Leisure", S, "Select leisure destinations fit Avani."],
      ["Conversion & Repositioning", M, "Conversions succeed at select-service capital."],
      ["Airport & Transportation", M, "Airport select Avani can fit."],
      ["Luxury Urban Flagship", L, "True luxury sits at Anantara."],
      ["Highway & Interstate Transient", L, "Pure highway limited-service is usually not Avani's core."],
    ],
    ops: "Minor Avani — upscale select / lifestyle with GHA DISCOVERY. NEW Basics create requires founder auth. Confirm franchise / management exhibits.",
    footprint: "Confirm live Avani counts from current Minor disclosures—APAC / Middle East / selective global.",
    insight: "Avani fits upscale select sponsors with DISCOVERY; fails when Anantara luxury or Oaks apartment fees are assumed.",
  },
  {
    id: "recSAUYLDhZPmEita",
    name: "Oaks Hotels, Resorts & Suites",
    live: "Oaks",
    slug: "oaks-hotels-resorts-suites",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "Oaks",
    feeGeo: "Confirm current Minor / Oaks development disclosure for Oaks (apartments / suites). Confirm actual GHA DISCOVERY participation for Oaks properties—do not assume identical participation across all Minor brands.",
    brandPage: "https://www.oakshotels.com/",
    positioning: "Serviced Apartments & Suites APAC",
    commercial:
      "Versus Avani hotel select, Oaks sells apartment / suite extended-stay economics; versus Elewana safari lodges, Oaks is urban / leisure apartment stock with DISCOVERY where participating.",
    scenarios: [
      ["APAC Serviced Apartment", "Markets funding Oaks apartment stays."],
      ["Extended Corporate / Leisure Stay", "Corridors needing suite / apartment inventory."],
      ["Conversion Into Oaks", "Assets clearing apartment PIP and Minor standards."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Oaks is built for apartment / suite longer stays."],
      ["Resort & Destination Leisure", S, "Leisure apartment resorts fit Oaks well."],
      ["Suburban Business & Leisure", M, "Suburban apartment corridors can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at apartment capital."],
      ["Luxury Urban Flagship", L, "True luxury hotels sit above Oaks apartments."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Oaks."],
    ],
    ops: "Minor Oaks — serviced apartments / suites. Preserve live name Oaks. Confirm GHA DISCOVERY participation per property class—do not invent fees. Apartment housekeeping ≠ transient hotel economics.",
    footprint: "Confirm live Oaks counts from current Minor disclosures—Australia / APAC weighted.",
    insight: "Oaks fits apartment / suite sponsors in APAC; fails when Avani hotel select or Anantara luxury fees are assumed. Confirm loyalty participation.",
  },
  {
    id: "NEW_elewana",
    name: "Elewana Collection",
    live: "Elewana Collection",
    slug: "elewana-collection",
    parent: "Minor Hotels",
    loyalty: "GHA DISCOVERY",
    proof: "GHA_PROOF",
    short: "Elewana",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm Minor / Elewana management terms and GHA DISCOVERY applicability for safari / experiential lodges—do not invent conventional hotel franchise tables.",
    brandPage: "https://www.elewana.com/",
    positioning: "Safari & Experiential Luxury Lodges",
    commercial:
      "Versus Anantara resort luxury, Elewana sells East African safari / experiential lodges; versus Oaks apartments, Elewana is destination lodge hospitality with DISCOVERY where participating.",
    scenarios: [
      ["Safari / Experiential Lodge", "East African destinations funding Elewana lodge product."],
      ["Luxury Experiential Leisure", "Itineraries where Elewana exclusivity clears ADR."],
      ["Conservation / Destination Partnership", "Sites aligned with Elewana conservation positioning."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Elewana is built for safari / experiential leisure destinations."],
      ["Luxury Urban Flagship", N, "Urban hotels sit outside Elewana lodge product."],
      ["Conversion & Repositioning", L, "Conversions are rare—purpose-built / curated lodges dominate."],
      ["Meetings & Group Compression", L, "Large groups often conflict with lodge exclusivity."],
      ["Extended-Stay Corporate", N, "Corporate ES apartments sit outside Elewana."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Elewana."],
    ],
    ops: "Minor Elewana Collection — safari / experiential luxury lodges. NEW Basics create requires founder auth. Confirm DISCOVERY participation; underwrite lodge / conservation ops—not urban hotel FDD tables.",
    footprint: "Confirm live Elewana lodge counts from current Minor / Elewana disclosures—East Africa concentrated.",
    insight: "Elewana fits safari / experiential lodge sponsors; fails when NH urban or Oaks apartment economics are assumed. Demand scenarios must stay safari-led.",
  },
  // Meliá
  {
    id: "recm6hadfIlOVdszS",
    name: "Gran Meliá Hotels & Resorts",
    live: "Gran Meliá",
    slug: "gran-melia-hotels-resorts",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "Gran Meliá",
    feeGeo: "Confirm current Meliá development / management disclosure for Gran Meliá (luxury)",
    brandPage: "https://www.melia.com/en/hotels/gran-melia",
    positioning: "Luxury Full-Service",
    commercial:
      "Versus Meliá Hotels upscale, Gran Meliá sells harder luxury; versus ME lifestyle, Gran Meliá is classic luxury full-service inside MeliáRewards.",
    scenarios: [
      ["Luxury Urban / Resort Flag", "Gateways and resorts funding Gran Meliá luxury product."],
      ["Mediterranean Luxury Leisure", "Mediterranean destinations where Gran Meliá ADR clears IC."],
      ["Conversion Into Gran Meliá", "Conversions clearing luxury PIP."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Gran Meliá fits luxury urban flags with MeliáRewards."],
      ["Resort & Destination Leisure", S, "Luxury resorts are a core Gran Meliá lane."],
      ["Meetings & Group Compression", M, "Meetings fit when luxury plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed only with luxury PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Gran Meliá."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Gran Meliá."],
    ],
    ops: "Meliá Gran Meliá luxury with MeliáRewards. Preserve live name Gran Meliá. Confirm fee exhibits—do not invent royalties.",
    footprint: "Confirm live Gran Meliá counts from current Meliá disclosures—Europe / Americas / selective global luxury.",
    insight: "Gran Meliá fits luxury sponsors with MeliáRewards; fails when Sol midscale or Paradisus AI fees are assumed.",
  },
  {
    id: "recoMzqIJuI2du3VS",
    name: "ME by Meliá",
    live: "ME by Meliá",
    slug: "me-by-melia",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "ME by Meliá",
    feeGeo: "Confirm current Meliá development disclosure for ME by Meliá (lifestyle)",
    brandPage: "https://www.melia.com/en/hotels/me",
    positioning: "Design Lifestyle Hotels",
    commercial:
      "Versus Gran Meliá classic luxury, ME sells design lifestyle; versus INNSiDE urban select, ME is harder lifestyle intensity inside MeliáRewards.",
    scenarios: [
      ["Urban Lifestyle Flag", "Creative cities funding ME design product."],
      ["Lifestyle Leisure Destination", "Destinations where ME lifestyle ADR clears IC."],
      ["Conversion Into ME", "Assets clearing ME design PIP."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Design-led conversions are a core ME path."],
      ["Luxury Urban Flagship", M, "Lifestyle flags fit; ultra-luxury sits at Gran Meliá."],
      ["Resort & Destination Leisure", M, "Select lifestyle leisure destinations fit ME."],
      ["Suburban Business & Leisure", L, "Suburban productive mix sits closer to INNSiDE / Meliá."],
      ["Meetings & Group Compression", L, "Large meetings are secondary."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside ME."],
    ],
    ops: "Meliá ME by Meliá — design lifestyle with MeliáRewards. Preserve live name ME by Meliá.",
    footprint: "Confirm live ME counts from current Meliá disclosures—selective lifestyle global set.",
    insight: "ME fits lifestyle sponsors with MeliáRewards; fails when Sol leisure midscale or Paradisus AI fees are assumed.",
  },
  {
    id: "NEW_melia-collection",
    name: "The Meliá Collection",
    live: "The Meliá Collection",
    slug: "the-melia-collection",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "The Meliá Collection",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Meliá disclosure for The Meliá Collection (curated independent / soft collection). Confirm MeliáRewards participation.",
    brandPage: "https://www.melia.com/",
    positioning: "Curated Independent Collection",
    commercial:
      "Versus Gran Meliá hard luxury brand, The Meliá Collection sells curated independent character; versus ZEL newer lifestyle, Collection is soft-collection affiliation inside MeliáRewards.",
    scenarios: [
      ["Independent Character Affiliation", "Assets seeking Meliá distribution without hard-brand prototype."],
      ["Curated Urban / Resort Flag", "Properties clearing Collection quality with MeliáRewards."],
      ["Soft-Brand Conversion", "Conversions preferring collection affiliation vs hard brand."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Soft-collection affiliation is a core Collection path."],
      ["Luxury Urban Flagship", M, "Curated luxury independents can fit."],
      ["Resort & Destination Leisure", M, "Select resort independents can fit."],
      ["Suburban Business & Leisure", L, "Prototype select-service sits outside Collection."],
      ["Meetings & Group Compression", L, "Meetings vary by asset—not Collection-standardized."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Collection."],
    ],
    ops: "Meliá Collection — curated / soft collection with MeliáRewards. NEW Basics create requires founder auth. Confirm membership obligations vs hard-brand franchise tables.",
    footprint: "Confirm live Collection counts from current Meliá disclosures—intentionally curated set.",
    insight: "The Meliá Collection fits independent character sponsors seeking MeliáRewards; fails when hard-brand Gran Meliá or Sol fees are assumed.",
  },
  {
    id: "NEW_paradisus",
    name: "Paradisus by Meliá",
    live: "Paradisus by Meliá",
    slug: "paradisus-by-melia",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "Paradisus",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Meliá disclosure for Paradisus all-inclusive resorts—AI exhibits, not transient hotel royalty tables",
    brandPage: "https://www.melia.com/en/hotels/paradisus",
    positioning: "Luxury All-Inclusive Resorts",
    commercial:
      "Versus Sol leisure midscale AI/resort, Paradisus sells luxury all-inclusive; versus Gran Meliá transient luxury, Paradisus is AI resort economics inside MeliáRewards.",
    scenarios: [
      ["Luxury All-Inclusive Destination", "CALA / leisure destinations funding Paradisus AI product."],
      ["Couples / Family Luxury AI", "Markets where Paradisus AI package ADR clears IC."],
      ["Conversion Into Paradisus AI", "Resorts that can fund AI F&B and amenity intensity."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Paradisus is built for luxury all-inclusive leisure."],
      ["Conversion & Repositioning", M, "Conversions succeed only with true AI capital."],
      ["Meetings & Group Compression", M, "Groups / weddings can fit when AI plant supports."],
      ["Luxury Urban Flagship", N, "Urban transient luxury sits at Gran Meliá."],
      ["Extended-Stay Corporate", N, "Corporate ES sits outside Paradisus AI."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Paradisus."],
    ],
    ops: "Meliá Paradisus — luxury all-inclusive resorts with MeliáRewards. NEW Basics create requires founder auth. AI F&B / labor intensity ≠ transient hotel FDD tables.",
    footprint: "Confirm live Paradisus counts from current Meliá disclosures—CALA / leisure weighted.",
    insight: "Paradisus fits luxury AI resort sponsors with MeliáRewards; fails when Gran Meliá transient or Sol midscale fees are assumed.",
  },
  {
    id: "recR7im3gU3xoddBM",
    name: "Meliá Hotels & Resorts",
    live: "Meliá",
    slug: "melia-hotels-resorts",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "Meliá Hotels",
    feeGeo: "Confirm current Meliá development / franchise disclosure for Meliá Hotels & Resorts (upscale)",
    brandPage: "https://www.melia.com/",
    positioning: "Upscale Full-Service Hotels & Resorts",
    commercial:
      "Versus Gran Meliá luxury, Meliá Hotels sells productive upscale; versus INNSiDE select lifestyle, Meliá is classic full-service inside MeliáRewards.",
    scenarios: [
      ["Urban Upscale Full-Service", "Cities where Meliá ADR and MeliáRewards clear IC."],
      ["Mediterranean Resort Upscale", "Resort markets funding Meliá full-service product."],
      ["Conversion Into Meliá", "Conversions clearing upscale PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Meliá fits productive upscale urban / suburban mix."],
      ["Resort & Destination Leisure", S, "Upscale resorts are a core Meliá lane."],
      ["Meetings & Group Compression", S, "Meetings fit when full-service plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed at upscale capital."],
      ["Luxury Urban Flagship", L, "True luxury sits at Gran Meliá."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Meliá."],
    ],
    ops: "Meliá Hotels & Resorts — upscale full-service with MeliáRewards. Preserve live name Meliá. Confirm franchise / management exhibits.",
    footprint: "Confirm live Meliá Hotels counts from current Meliá disclosures—Europe / Americas / CALA weighted.",
    insight: "Meliá Hotels fits upscale sponsors with MeliáRewards; fails when Sol midscale or Paradisus AI fees are assumed.",
  },
  {
    id: "NEW_innside",
    name: "INNSiDE by Meliá",
    live: "INNSiDE by Meliá",
    slug: "innside-by-melia",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "INNSiDE",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Meliá development disclosure for INNSiDE by Meliá (urban select / lifestyle)",
    brandPage: "https://www.melia.com/en/hotels/innside",
    positioning: "Urban Select Lifestyle",
    commercial:
      "Versus Meliá full-service upscale, INNSiDE sells urban select lifestyle efficiency; versus ZEL newer lifestyle, INNSiDE is established select inside MeliáRewards.",
    scenarios: [
      ["Urban Select Lifestyle", "Cities where INNSiDE ADR and MeliáRewards clear IC."],
      ["Airport / Gateway Select", "Gateways needing efficient lifestyle rooms."],
      ["Conversion Into INNSiDE", "Conversions clearing select lifestyle PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "INNSiDE fits productive urban select mix."],
      ["Airport & Transportation", S, "Gateway select is a core INNSiDE lane."],
      ["Conversion & Repositioning", M, "Conversions succeed at select capital."],
      ["Meetings & Group Compression", L, "Large meetings sit closer to Meliá full-service."],
      ["Resort & Destination Leisure", L, "Pure resorts sit closer to Meliá / Paradisus."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside INNSiDE."],
    ],
    ops: "Meliá INNSiDE — urban select lifestyle with MeliáRewards. NEW Basics create requires founder auth.",
    footprint: "Confirm live INNSiDE counts from current Meliá disclosures—Europe / selective global urban.",
    insight: "INNSiDE fits urban select lifestyle sponsors with MeliáRewards; fails when Meliá full-service or Paradisus AI fees are assumed.",
  },
  {
    id: "NEW_zel",
    name: "ZEL",
    live: "ZEL",
    slug: "zel",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "ZEL",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Meliá disclosure for ZEL (newer lifestyle). Do not invent mature operating metrics for a newer brand.",
    brandPage: "https://www.melia.com/",
    positioning: "Newer Lifestyle Brand",
    commercial:
      "Versus ME established lifestyle, ZEL is a newer Meliá lifestyle expression; versus INNSiDE urban select, ZEL is earlier-stage lifestyle inside MeliáRewards.",
    scenarios: [
      ["Lifestyle Urban Launch", "Cities funding ZEL lifestyle product as the brand matures."],
      ["Conversion / Soft Launch", "Assets clearing ZEL standards—confirm current brand definition."],
      ["Leisure Lifestyle Stay", "Select leisure markets where ZEL ADR can clear IC."],
    ],
    demand: [
      ["Conversion & Repositioning", M, "Conversions possible as brand standards firm—confirm current definition."],
      ["Suburban Business & Leisure", M, "Select urban lifestyle sites may fit as footprint grows."],
      ["Luxury Urban Flagship", L, "Ultra-luxury sits at Gran Meliá."],
      ["Resort & Destination Leisure", L, "Pure resorts sit closer to Paradisus / Meliá."],
      ["Meetings & Group Compression", L, "Meetings are secondary for early lifestyle."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside ZEL."],
    ],
    ops: "Meliá ZEL — newer lifestyle brand with MeliáRewards. NEW Basics create requires founder auth. Do not invent mature hotel counts or fee percentages.",
    footprint: "Confirm live ZEL counts from current Meliá disclosures—early-stage; do not invent mature footprint.",
    insight: "ZEL fits early lifestyle sponsors with MeliáRewards; fails when ME mature lifestyle or Sol midscale fees are assumed. Newer-brand maturity caution.",
  },
  {
    id: "NEW_sol-by-melia",
    name: "Sol by Meliá",
    live: "Sol by Meliá",
    slug: "sol-by-melia",
    parent: "Meliá Hotels International",
    loyalty: "MeliáRewards",
    proof: "MELIA_PROOF",
    short: "Sol by Meliá",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Meliá disclosure for Sol by Meliá (midscale leisure / resort). Distinguish AI vs transient Sol assets.",
    brandPage: "https://www.melia.com/en/hotels/sol",
    positioning: "Midscale Leisure Resorts & Hotels",
    commercial:
      "Versus Paradisus luxury AI, Sol sells midscale leisure / resort value; versus Meliá upscale, Sol is lower ladder intensity inside MeliáRewards.",
    scenarios: [
      ["Midscale Leisure Resort", "Leisure destinations funding Sol product."],
      ["Family Leisure Value", "Markets where Sol package ADR clears IC."],
      ["Conversion Into Sol", "Conversions clearing midscale leisure PIP."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Sol is built for midscale leisure destinations."],
      ["Conversion & Repositioning", M, "Conversions succeed at midscale leisure capital."],
      ["Suburban Business & Leisure", L, "Urban corporate mix sits closer to INNSiDE / Meliá."],
      ["Meetings & Group Compression", L, "Large meetings are secondary."],
      ["Luxury Urban Flagship", N, "Luxury flags sit at Gran Meliá."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Sol leisure."],
    ],
    ops: "Meliá Sol — midscale leisure / resort with MeliáRewards. NEW Basics create requires founder auth. Distinguish AI Sol assets from transient—do not blend underwriting.",
    footprint: "Confirm live Sol counts from current Meliá disclosures—Mediterranean / leisure weighted.",
    insight: "Sol fits midscale leisure sponsors with MeliáRewards; fails when Paradisus luxury AI or Gran Meliá luxury fees are assumed.",
  },
  // Barceló
  {
    id: "NEW_royal-hideaway",
    name: "Royal Hideaway Hotels & Resorts",
    live: "Royal Hideaway Hotels & Resorts",
    slug: "royal-hideaway-hotels-resorts",
    parent: "Barceló Hotel Group",
    loyalty: "my Barceló",
    proof: "BARCELO_PROOF",
    short: "Royal Hideaway",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Barceló disclosure for Royal Hideaway (luxury). Confirm my Barceló recognition applicability—not Bonvoy-scale points assumptions.",
    brandPage: "https://www.barcelo.com/en-gb/hotels/royal-hideaway/",
    positioning: "Luxury Hotels & Resorts",
    commercial:
      "Versus Barceló Hotels upscale, Royal Hideaway sells harder luxury; versus Occidental leisure, Hideaway is luxury intensity with my Barceló recognition.",
    scenarios: [
      ["Luxury Resort / Urban Flag", "Destinations funding Royal Hideaway luxury product."],
      ["Mediterranean / CALA Luxury Leisure", "Leisure markets where Hideaway ADR clears IC."],
      ["Conversion Into Royal Hideaway", "Conversions clearing luxury PIP."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Royal Hideaway fits luxury flags with my Barceló recognition."],
      ["Resort & Destination Leisure", S, "Luxury resorts are a core Hideaway lane."],
      ["Conversion & Repositioning", M, "Conversions succeed only with luxury PIP."],
      ["Meetings & Group Compression", M, "Meetings fit when luxury plant exists."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Hideaway."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Hideaway."],
    ],
    ops: "Barceló Royal Hideaway luxury with my Barceló recognition. NEW Basics create requires founder auth. Do not equate my Barceló with conventional global points mega-programs.",
    footprint: "Confirm live Royal Hideaway counts from current Barceló disclosures—selective luxury set.",
    insight: "Royal Hideaway fits luxury sponsors with my Barceló recognition; fails when Allegro midscale or Occidental leisure fees are assumed.",
  },
  {
    id: "rec5dHX0hHSdA8nbd",
    name: "Barceló Hotels & Resorts",
    live: "Barceló",
    slug: "barcelo-hotels-resorts",
    parent: "Barceló Hotel Group",
    loyalty: "my Barceló",
    proof: "BARCELO_PROOF",
    short: "Barceló Hotels",
    feeGeo: "Confirm current Barceló development / management disclosure for Barceló Hotels & Resorts. Confirm my Barceló recognition terms.",
    brandPage: "https://www.barcelo.com/",
    positioning: "Upscale Hotels & Resorts",
    commercial:
      "Versus Royal Hideaway luxury, Barceló Hotels sells productive upscale; versus Occidental leisure mid-upscale, Barceló is the core upscale flag with my Barceló recognition.",
    scenarios: [
      ["Urban Upscale Full-Service", "Cities where Barceló ADR clears IC."],
      ["Mediterranean / LATAM Resort Upscale", "Resort markets funding Barceló product."],
      ["Conversion Into Barceló", "Conversions clearing upscale PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Barceló fits productive upscale urban / suburban mix."],
      ["Resort & Destination Leisure", S, "Upscale resorts are a core Barceló lane."],
      ["Meetings & Group Compression", S, "Meetings fit when full-service plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed at upscale capital."],
      ["Luxury Urban Flagship", L, "True luxury sits at Royal Hideaway."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Barceló."],
    ],
    ops: "Barceló Hotels & Resorts — upscale with my Barceló recognition. Preserve live name Barceló. Confirm franchise / management exhibits; do not invent points-program economics.",
    footprint: "Confirm live Barceló Hotels counts from current Barceló disclosures—EMEA / Americas / LATAM weighted.",
    insight: "Barceló Hotels fits upscale sponsors with my Barceló recognition; fails when Royal Hideaway luxury or Allegro midscale fees are assumed.",
  },
  {
    id: "NEW_occidental",
    name: "Occidental Hotels & Resorts",
    live: "Occidental Hotels & Resorts",
    slug: "occidental-hotels-resorts",
    parent: "Barceló Hotel Group",
    loyalty: "my Barceló",
    proof: "BARCELO_PROOF",
    short: "Occidental",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Barceló disclosure for Occidental (upscale leisure / AI mix). Distinguish AI vs transient Occidental assets.",
    brandPage: "https://www.barcelo.com/en-gb/hotels/occidental/",
    positioning: "Upscale Leisure Resorts",
    commercial:
      "Versus Barceló Hotels broader upscale, Occidental sells leisure / resort intensity; versus Allegro midscale leisure, Occidental is higher leisure ladder with my Barceló recognition.",
    scenarios: [
      ["Upscale Leisure Resort", "Leisure destinations funding Occidental product."],
      ["All-Inclusive / Resort Mix", "Markets where Occidental AI or resort package ADR clears IC—confirm AI vs transient."],
      ["Conversion Into Occidental", "Conversions clearing leisure resort PIP."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Occidental is built for upscale leisure destinations."],
      ["Conversion & Repositioning", M, "Conversions succeed at leisure resort capital."],
      ["Meetings & Group Compression", M, "Groups / leisure groups can fit when plant supports."],
      ["Suburban Business & Leisure", L, "Urban corporate mix sits closer to Barceló Hotels."],
      ["Luxury Urban Flagship", L, "True luxury sits at Royal Hideaway."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Occidental."],
    ],
    ops: "Barceló Occidental — upscale leisure / resort (often AI-capable). NEW Basics create requires founder auth. Distinguish AI vs transient underwriting. my Barceló ≠ Bonvoy-scale points.",
    footprint: "Confirm live Occidental counts from current Barceló disclosures—Mediterranean / Americas / CALA leisure weighted.",
    insight: "Occidental fits upscale leisure sponsors with my Barceló recognition; fails when Allegro midscale or Royal Hideaway luxury fees are assumed.",
  },
  {
    id: "NEW_allegro",
    name: "Allegro Hotels",
    live: "Allegro Hotels",
    slug: "allegro-hotels",
    parent: "Barceló Hotel Group",
    loyalty: "my Barceló",
    proof: "BARCELO_PROOF",
    short: "Allegro",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Barceló disclosure for Allegro (midscale leisure). Confirm my Barceló applicability.",
    brandPage: "https://www.barcelo.com/en-gb/hotels/allegro/",
    positioning: "Midscale Leisure Hotels & Resorts",
    commercial:
      "Versus Occidental upscale leisure, Allegro sells midscale leisure value; versus Barceló Hotels upscale, Allegro is lower ladder intensity with my Barceló recognition.",
    scenarios: [
      ["Midscale Leisure Destination", "Leisure markets funding Allegro product."],
      ["Family Leisure Value", "Markets where Allegro package ADR clears IC."],
      ["Conversion Into Allegro", "Conversions clearing midscale leisure PIP."],
    ],
    demand: [
      ["Resort & Destination Leisure", S, "Allegro is built for midscale leisure destinations."],
      ["Conversion & Repositioning", M, "Conversions succeed at midscale leisure capital."],
      ["Suburban Business & Leisure", L, "Urban corporate mix sits closer to Barceló Hotels."],
      ["Meetings & Group Compression", L, "Large meetings are secondary."],
      ["Luxury Urban Flagship", N, "Luxury flags sit at Royal Hideaway."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Allegro leisure."],
    ],
    ops: "Barceló Allegro — midscale leisure with my Barceló recognition. NEW Basics create requires founder auth. Do not invent fee percentages or equate recognition with global points mega-programs.",
    footprint: "Confirm live Allegro counts from current Barceló disclosures—leisure / Mediterranean / Americas weighted.",
    insight: "Allegro fits midscale leisure sponsors with my Barceló recognition; fails when Occidental upscale or Royal Hideaway luxury fees are assumed.",
  },
];

function esc(s) {
  return String(s).replace(/\\/g, "\\\\").replace(/`/g, "\\`").replace(/\$/g, "\\$");
}

function demandFit(code) {
  if (code === "S") return "S";
  if (code === "M") return "M";
  if (code === "L") return "L";
  return "N";
}

const lines = [];
lines.push(`/**`);
lines.push(` * Factory 2.0 Batch 15 — Minor Hotels + Meliá + Barceló.`);
lines.push(` * NEW_BRAND_CANDIDATE packs use provisional NEW_<slug> keys — never invent Airtable rec IDs.`);
lines.push(` * GHA DISCOVERY / MeliáRewards / my Barceló — confirm brand-level participation; my Barceló ≠ Bonvoy-scale points.`);
lines.push(` * AI resorts (Paradisus) and safari lodges (Elewana) require distinct underwriting from urban transient hotels.`);
lines.push(` */`);
lines.push(`import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";`);
lines.push(`import { GHA_PROOF, MELIA_PROOF, BARCELO_PROOF } from "./be-factory-2-depth-loyalty-proofs-v1.js";`);
lines.push(``);
lines.push(`function pack(base) {`);
lines.push(`  return Object.freeze({ researchSpendUsd: 0, paidCalls: 0, loyaltyProofRequired: 6, batchTag: "batch-15", ...base });`);
lines.push(`}`);
lines.push(`function demand(rows) { return rows.map((r) => ({ title: r.title, fit: r.fit, explanation: r.explanation })); }`);
lines.push(`function gallery(brand, url) {`);
lines.push(`  return [`);
lines.push(`    { title: \`\${brand} — Exterior / Arrival\`, body: "Exterior presence · confirm property photography before publication", source: url },`);
lines.push(`    { title: \`\${brand} — Guest Room / Suite / Lodge\`, body: "Guest-room, suite, apartment, or lodge product typical of this ladder rung", source: url },`);
lines.push(`    { title: \`\${brand} — Lobby / Public Space\`, body: "Public space supporting the brand promise", source: url },`);
lines.push(`    { title: \`\${brand} — F&B / Amenity Cue\`, body: "F&B, spa, kitchen, or amenity cues where required by standards", source: url },`);
lines.push(`    { title: \`\${brand} — Meetings / Pool / Experience Cue\`, body: "Meetings, pool, safari, or AI amenity cues typical of this product", source: url },`);
lines.push(`    { title: \`\${brand} — Context / Setting\`, body: "Urban, resort, destination, safari, or corridor context where typical", source: url },`);
lines.push(`  ];`);
lines.push(`}`);
lines.push(`export const FACTORY_2_BATCH_15_V1 = "FACTORY_2_BATCH_15_V1";`);
lines.push(`const FDD = "Confirm current franchise / management / disclosure exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical figures from the live agreement. Amounts are not staged without a verified disclosure. Confirm brand-level loyalty participation (GHA DISCOVERY / MeliáRewards / my Barceló). For all-inclusive and safari lodges, do not import urban transient royalty tables blindly. Newer brands: do not invent mature operating metrics.";`);
lines.push(`const S = DEMAND_FIT_LABELS.STRONG_FIT;`);
lines.push(`const M = DEMAND_FIT_LABELS.MODERATE_FIT;`);
lines.push(`const L = DEMAND_FIT_LABELS.LIMITED_FIT;`);
lines.push(`const N = DEMAND_FIT_LABELS.NOT_A_FIT;`);
lines.push(`export const BATCH_15_IDS = Object.freeze([`);
for (const b of brands) lines.push(`  "${b.id}", // ${b.name}`);
lines.push(`]);`);
lines.push(`export const BATCH_15_PACKS_BY_ID = Object.freeze({`);

for (const b of brands) {
  const researchClass = b.researchClass || "EXISTING_CANONICAL";
  lines.push(`  "${b.id}": pack({`);
  lines.push(`    brandId: "${b.id}",`);
  lines.push(`    brandName: ${JSON.stringify(b.name)},`);
  lines.push(`    liveBrandName: ${JSON.stringify(b.live)},`);
  lines.push(`    slug: "${b.slug}",`);
  lines.push(`    parentCompany: ${JSON.stringify(b.parent)},`);
  lines.push(`    loyaltyProgram: ${JSON.stringify(b.loyalty)},`);
  lines.push(`    researchClass: ${JSON.stringify(researchClass)},`);
  lines.push(`    feeGeography: ${JSON.stringify(b.feeGeo)},`);
  lines.push(`    sources: { brandPage: ${JSON.stringify(b.brandPage)} },`);
  lines.push(`    positioning: ${JSON.stringify(b.positioning)},`);
  lines.push(`    commercialDifferentiator: ${JSON.stringify(b.commercial)},`);
  lines.push(`    scenarios: [`);
  for (const [t, body] of b.scenarios) lines.push(`      { title: ${JSON.stringify(t)}, body: ${JSON.stringify(body)} },`);
  lines.push(`    ],`);
  lines.push(`    demandScenarios: demand([`);
  for (const [t, fit, expl] of b.demand) lines.push(`      { title: ${JSON.stringify(t)}, fit: ${demandFit(fit)}, explanation: ${JSON.stringify(expl)} },`);
  lines.push(`    ]),`);
  lines.push(`    loyaltyProof: ${b.proof}(${JSON.stringify(b.short)}),`);
  lines.push(`    feeNote: \`${esc(b.short)} application, royalty, marketing, loyalty, and PIP — \${FDD}\`,`);
  lines.push(`    operatingModel: ${JSON.stringify(b.ops)},`);
  lines.push(`    footprint: ${JSON.stringify(b.footprint)},`);
  lines.push(`    insight: ${JSON.stringify(b.insight)},`);
  lines.push(`    standards: [`);
  lines.push(`      { title: "Brand Product Standards", body: "Rooms / suites / apartments / lodges / resort product must clear ${esc(b.short)} standards for the operating region." },`);
  lines.push(`      { title: "Conversion / PIP Discipline", body: "PIP or affiliation quality scope must match the brand promise—not under-scoped capital." },`);
  lines.push(`      { title: "Loyalty / Network Integration", body: "Confirm ${esc(b.loyalty)} delivery obligations for this brand before IC—participation is not automatic across every sibling." },`);
  lines.push(`    ],`);
  lines.push(`    valueOwnerScenarios: [`);
  lines.push(`      { title: "Primary Owner Corridor", body: "Sites where ${esc(b.short)} ADR / contribution clears IC versus local comps." },`);
  lines.push(`      { title: "Conversion / Affiliation Path", body: "Assets where ${esc(b.short)} affiliation clears IC versus sibling alternatives." },`);
  lines.push(`      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / AI / lodge exhibits for ${esc(b.short)}." },`);
  lines.push(`      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },`);
  lines.push(`    ],`);
  lines.push(`    gallery: gallery(${JSON.stringify(b.short)}, ${JSON.stringify(b.brandPage)}),`);
  lines.push(`  }),`);
  lines.push(``);
}
lines.push(`});`);
lines.push(`export function listBatch15() { return BATCH_15_IDS.map((id) => BATCH_15_PACKS_BY_ID[id]).filter(Boolean); }`);
lines.push(`export function getBatch15Pack(brandId) { return BATCH_15_PACKS_BY_ID[brandId] || null; }`);
lines.push(``);

writeFileSync(outPath, lines.join("\n"));
console.log(`Wrote ${outPath} (${brands.length} packs)`);
