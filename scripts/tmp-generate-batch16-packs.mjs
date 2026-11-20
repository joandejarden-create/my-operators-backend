#!/usr/bin/env node
/** One-shot generator for Batch 16 depth packs (Pan Pacific + Shangri-La + Dusit + Rotana + Ascott). */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const outPath = join(ROOT, "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch16-v1.js");
const S = "S";
const M = "M";
const L = "L";
const N = "N";

/** @type {Array<object>} */
const brands = [
  {
    id: "NEW_pan-pacific",
    name: "Pan Pacific Hotels & Resorts",
    live: "Pan Pacific Hotels & Resorts",
    slug: "pan-pacific-hotels-resorts",
    parent: "Pan Pacific Hotels Group",
    loyalty: "Pan Pacific Hotels Group Loyalty",
    proof: "PAN_PACIFIC_PROOF",
    short: "Pan Pacific",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Pan Pacific Hotels Group management / development disclosure for Pan Pacific (luxury / upper-upscale)",
    brandPage: "https://www.panpacific.com/",
    positioning: "Luxury & Upper-Upscale Full-Service APAC",
    commercial:
      "Versus PARKROYAL upscale, Pan Pacific sells harder luxury / upper-upscale full-service; versus Shangri-La peer luxury, Pan Pacific is PPHG-owned with group loyalty.",
    scenarios: [
      ["APAC Luxury / Upper-Upscale Urban", "Gateways where Pan Pacific ADR and group loyalty clear IC."],
      ["Meetings & Corporate Full-Service", "Markets funding Pan Pacific meetings plant."],
      ["Conversion Into Pan Pacific", "Conversions clearing luxury / upper-upscale PIP."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Pan Pacific fits APAC luxury / upper-upscale urban flags."],
      ["Meetings & Group Compression", S, "Meetings are a core Pan Pacific lane when plant exists."],
      ["Suburban Business & Leisure", M, "Select suburban full-service sites can fit."],
      ["Resort & Destination Leisure", M, "Select resort Pan Pacific assets can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at luxury / upper-upscale capital."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Pan Pacific."],
    ],
    ops: "PPHG Pan Pacific — luxury / upper-upscale full-service. NEW Basics create requires founder auth. Confirm loyalty program name and fee exhibits.",
    footprint: "Confirm live Pan Pacific counts from current PPHG disclosures—APAC weighted with selective global growth.",
    insight: "Pan Pacific fits APAC luxury / upper-upscale sponsors; fails when PARKROYAL mid-upscale capital is assumed.",
  },
  {
    id: "NEW_parkroyal-collection",
    name: "PARKROYAL COLLECTION",
    live: "PARKROYAL COLLECTION",
    slug: "parkroyal-collection",
    parent: "Pan Pacific Hotels Group",
    loyalty: "Pan Pacific Hotels Group Loyalty",
    proof: "PAN_PACIFIC_PROOF",
    short: "PARKROYAL COLLECTION",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current PPHG disclosure for PARKROYAL COLLECTION (curated upscale). Distinguish from PARKROYAL Hotels & Resorts.",
    brandPage: "https://www.panpacific.com/",
    positioning: "Curated Upscale Collection",
    commercial:
      "Versus PARKROYAL Hotels prototype, PARKROYAL COLLECTION sells curated character; versus Pan Pacific luxury, Collection is softer upscale intensity inside PPHG loyalty.",
    scenarios: [
      ["Curated Upscale Affiliation", "Assets seeking PPHG distribution with collection character."],
      ["Urban / Lifestyle Collection Flag", "Cities where Collection ADR clears IC."],
      ["Soft-Brand Conversion", "Conversions preferring collection vs hard PARKROYAL prototype."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Collection affiliation is a core PARKROYAL COLLECTION path."],
      ["Suburban Business & Leisure", M, "Select urban upscale sites can fit."],
      ["Luxury Urban Flagship", M, "Curated upscale flags fit; ultra-luxury sits at Pan Pacific."],
      ["Meetings & Group Compression", L, "Meetings vary by asset—not Collection-standardized."],
      ["Resort & Destination Leisure", M, "Select resort Collections can fit."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Collection."],
    ],
    ops: "PPHG PARKROYAL COLLECTION — curated upscale. NEW Basics create requires founder auth. Do not confuse with PARKROYAL Hotels & Resorts.",
    footprint: "Confirm live PARKROYAL COLLECTION counts from current PPHG disclosures—intentionally curated APAC set.",
    insight: "PARKROYAL COLLECTION fits curated upscale sponsors; fails when PARKROYAL prototype or Pan Pacific luxury fees are assumed.",
  },
  {
    id: "NEW_parkroyal",
    name: "PARKROYAL Hotels & Resorts",
    live: "PARKROYAL Hotels & Resorts",
    slug: "parkroyal-hotels-resorts",
    parent: "Pan Pacific Hotels Group",
    loyalty: "Pan Pacific Hotels Group Loyalty",
    proof: "PAN_PACIFIC_PROOF",
    short: "PARKROYAL",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current PPHG development disclosure for PARKROYAL Hotels & Resorts (upscale). Distinguish from PARKROYAL COLLECTION.",
    brandPage: "https://www.panpacific.com/",
    positioning: "Upscale Full-Service APAC",
    commercial:
      "Versus Pan Pacific luxury, PARKROYAL sells productive upscale; versus PARKROYAL COLLECTION curated, PARKROYAL is prototype upscale full-service inside PPHG loyalty.",
    scenarios: [
      ["APAC Upscale Urban", "Cities where PARKROYAL ADR and group loyalty clear IC."],
      ["Meetings Upscale Full-Service", "Markets funding PARKROYAL meetings plant."],
      ["Conversion Into PARKROYAL", "Conversions clearing upscale PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "PARKROYAL fits productive APAC upscale mix."],
      ["Meetings & Group Compression", S, "Meetings are a core PARKROYAL lane."],
      ["Airport & Transportation", M, "Airport / gateway PARKROYAL can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at upscale capital."],
      ["Luxury Urban Flagship", L, "True luxury sits at Pan Pacific."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside PARKROYAL."],
    ],
    ops: "PPHG PARKROYAL — upscale full-service. NEW Basics create requires founder auth. Distinguish from PARKROYAL COLLECTION.",
    footprint: "Confirm live PARKROYAL counts from current PPHG disclosures—APAC weighted.",
    insight: "PARKROYAL fits APAC upscale sponsors; fails when Pan Pacific luxury or Collection soft-brand fees are assumed.",
  },
  {
    id: "recRqHJW2SbKY0N2v",
    name: "Shangri-La Hotels & Resorts",
    live: "Shangri-La Hotels and Resorts",
    slug: "shangri-la-hotels-resorts",
    parent: "Shangri-La Group",
    loyalty: "Golden Circle",
    proof: "GOLDEN_CIRCLE_PROOF",
    short: "Shangri-La",
    feeGeo: "Confirm current Shangri-La Group management / development disclosure for Shangri-La (luxury)",
    brandPage: "https://www.shangri-la.com/",
    positioning: "Luxury Full-Service",
    commercial:
      "Versus Kerry Hotels upper-upscale, Shangri-La sells harder luxury; versus JEN select lifestyle, Shangri-La is classic luxury full-service inside Golden Circle.",
    scenarios: [
      ["Luxury Urban / Resort Flag", "Gateways and resorts funding Shangri-La luxury product."],
      ["International Corporate & Meetings", "Markets where Shangri-La meetings and Golden Circle clear IC."],
      ["Conversion Into Shangri-La Luxury", "Conversions only when luxury PIP clears Shangri-La QA."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Shangri-La fits luxury urban flags with Golden Circle."],
      ["Resort & Destination Leisure", S, "Luxury resorts are a core Shangri-La lane."],
      ["Meetings & Group Compression", S, "High-end meetings and events fit when plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed only with true luxury PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Shangri-La."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Shangri-La."],
    ],
    ops: "Shangri-La luxury full-service with Golden Circle. Preserve live name Shangri-La Hotels and Resorts. Confirm fee exhibits—do not invent royalties.",
    footprint: "Confirm live Shangri-La counts from current Shangri-La Group disclosures—APAC weighted with selective global luxury.",
    insight: "Shangri-La fits luxury sponsors with Golden Circle; fails when JEN select or Traders mid-upscale fees are assumed.",
  },
  {
    id: "NEW_kerry-hotels",
    name: "Kerry Hotels",
    live: "Kerry Hotels",
    slug: "kerry-hotels",
    parent: "Shangri-La Group",
    loyalty: "Golden Circle",
    proof: "GOLDEN_CIRCLE_PROOF",
    short: "Kerry Hotels",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Shangri-La Group disclosure for Kerry Hotels (upper-upscale / lifestyle). Confirm Golden Circle participation.",
    brandPage: "https://www.shangri-la.com/",
    positioning: "Upper-Upscale Lifestyle",
    commercial:
      "Versus Shangri-La classic luxury, Kerry sells upper-upscale lifestyle; versus JEN select, Kerry is higher intensity inside Golden Circle.",
    scenarios: [
      ["Upper-Upscale Urban Lifestyle", "Cities where Kerry ADR and Golden Circle clear IC."],
      ["Lifestyle Meetings Mix", "Markets funding Kerry lifestyle + meetings product."],
      ["Conversion Into Kerry", "Conversions clearing Kerry design and service PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Kerry fits productive upper-upscale lifestyle mix."],
      ["Meetings & Group Compression", M, "Meetings fit when plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed at upper-upscale capital."],
      ["Luxury Urban Flagship", M, "Lifestyle luxury flags fit; ultra-luxury sits at Shangri-La."],
      ["Resort & Destination Leisure", L, "Pure resorts sit closer to Shangri-La."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Kerry."],
    ],
    ops: "Shangri-La Kerry Hotels — upper-upscale lifestyle with Golden Circle. NEW Basics create requires founder auth.",
    footprint: "Confirm live Kerry counts from current Shangri-La disclosures—selective urban set.",
    insight: "Kerry fits upper-upscale lifestyle sponsors with Golden Circle; fails when Shangri-La luxury or JEN select fees are assumed.",
  },
  {
    id: "NEW_jen",
    name: "JEN by Shangri-La",
    live: "JEN by Shangri-La",
    slug: "jen-by-shangri-la",
    parent: "Shangri-La Group",
    loyalty: "Golden Circle",
    proof: "GOLDEN_CIRCLE_PROOF",
    short: "JEN",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Shangri-La Group disclosure for JEN by Shangri-La (select / lifestyle)",
    brandPage: "https://www.shangri-la.com/jen/",
    positioning: "Select Lifestyle Urban",
    commercial:
      "Versus Shangri-La luxury, JEN sells select lifestyle efficiency; versus Traders mid-upscale, JEN is lifestyle-forward select inside Golden Circle.",
    scenarios: [
      ["Urban Select Lifestyle", "Cities where JEN ADR and Golden Circle clear IC."],
      ["Gateway / Transit Select", "Gateways needing efficient lifestyle rooms."],
      ["Conversion Into JEN", "Conversions clearing select lifestyle PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "JEN fits productive urban select lifestyle mix."],
      ["Airport & Transportation", S, "Gateway select is a core JEN lane."],
      ["Conversion & Repositioning", M, "Conversions succeed at select capital."],
      ["Meetings & Group Compression", L, "Large meetings sit closer to Shangri-La / Kerry."],
      ["Luxury Urban Flagship", L, "True luxury sits at Shangri-La."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside JEN."],
    ],
    ops: "Shangri-La JEN — select lifestyle with Golden Circle. NEW Basics create requires founder auth.",
    footprint: "Confirm live JEN counts from current Shangri-La disclosures—APAC urban weighted.",
    insight: "JEN fits urban select lifestyle sponsors with Golden Circle; fails when Shangri-La luxury fees are assumed.",
  },
  {
    id: "NEW_traders",
    name: "Traders Hotels",
    live: "Traders Hotels",
    slug: "traders-hotels",
    parent: "Shangri-La Group",
    loyalty: "Golden Circle",
    proof: "GOLDEN_CIRCLE_PROOF",
    short: "Traders",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Shangri-La Group disclosure for Traders Hotels (mid-upscale / upscale). Confirm Golden Circle participation.",
    brandPage: "https://www.shangri-la.com/",
    positioning: "Mid-Upscale / Upscale Business",
    commercial:
      "Versus Shangri-La luxury, Traders sells productive mid-upscale / upscale business hotels; versus JEN lifestyle select, Traders is classic business hospitality inside Golden Circle.",
    scenarios: [
      ["APAC Business Urban", "Cities where Traders ADR and Golden Circle clear IC."],
      ["Corporate & Meetings Mix", "Markets funding Traders corporate product."],
      ["Conversion Into Traders", "Conversions clearing Traders standards."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Traders fits productive APAC business mix."],
      ["Meetings & Group Compression", S, "Meetings are a core Traders lane."],
      ["Airport & Transportation", M, "Airport / gateway Traders can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at mid-upscale capital."],
      ["Luxury Urban Flagship", L, "Luxury flags sit at Shangri-La."],
      ["Highway & Interstate Transient", L, "Pure highway limited-service is usually not Traders' core."],
    ],
    ops: "Shangri-La Traders — mid-upscale / upscale business with Golden Circle. NEW Basics create requires founder auth. Confirm participation.",
    footprint: "Confirm live Traders counts from current Shangri-La disclosures—APAC / Middle East weighted where present.",
    insight: "Traders fits APAC business hotel sponsors with Golden Circle; fails when Shangri-La luxury or JEN lifestyle fees are assumed.",
  },
  // Dusit
  {
    id: "NEW_dusit-thani",
    name: "Dusit Thani",
    live: "Dusit Thani",
    slug: "dusit-thani",
    parent: "Dusit International",
    loyalty: "Dusit Gold",
    proof: "DUSIT_PROOF",
    short: "Dusit Thani",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Dusit International management / development disclosure for Dusit Thani (luxury). Confirm Dusit Gold participation.",
    brandPage: "https://www.dusit.com/",
    positioning: "Luxury Thai Hospitality",
    commercial:
      "Versus dusitD2 lifestyle, Dusit Thani sells harder luxury Thai hospitality; versus Dusit Princess midscale, Thani is luxury intensity inside Dusit Gold.",
    scenarios: [
      ["Luxury Urban / Resort Flag", "Destinations funding Dusit Thani luxury product."],
      ["Thai Hospitality & Wellness", "Markets where Dusit Thani service and wellness clear ADR."],
      ["Conversion Into Dusit Thani", "Conversions clearing luxury PIP."],
    ],
    demand: [
      ["Luxury Urban Flagship", S, "Dusit Thani fits luxury flags with Dusit Gold."],
      ["Resort & Destination Leisure", S, "Luxury resorts / wellness are a core Thani lane."],
      ["Meetings & Group Compression", M, "Meetings fit when luxury plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed only with luxury PIP."],
      ["Suburban Business & Leisure", L, "Suburban select-service sits outside Thani."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Dusit Thani."],
    ],
    ops: "Dusit Thani luxury with Dusit Gold. NEW Basics create requires founder auth. Confirm fee exhibits.",
    footprint: "Confirm live Dusit Thani counts from current Dusit disclosures—Thailand / APAC / selective global luxury.",
    insight: "Dusit Thani fits luxury Thai hospitality sponsors; fails when Dusit Princess midscale or ASAI select fees are assumed.",
  },
  {
    id: "NEW_dusitd2",
    name: "dusitD2",
    live: "dusitD2",
    slug: "dusitd2",
    parent: "Dusit International",
    loyalty: "Dusit Gold",
    proof: "DUSIT_PROOF",
    short: "dusitD2",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Dusit disclosure for dusitD2 (lifestyle). Confirm Dusit Gold participation.",
    brandPage: "https://www.dusit.com/",
    positioning: "Design Lifestyle Hotels",
    commercial:
      "Versus Dusit Thani luxury, dusitD2 sells design lifestyle; versus ASAI select, dusitD2 is higher lifestyle intensity inside Dusit Gold.",
    scenarios: [
      ["Urban Lifestyle Flag", "Creative cities funding dusitD2 design product."],
      ["Lifestyle Leisure Stay", "Destinations where dusitD2 lifestyle ADR clears IC."],
      ["Conversion Into dusitD2", "Assets clearing dusitD2 design PIP."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Design-led conversions are a core dusitD2 path."],
      ["Suburban Business & Leisure", M, "Select urban lifestyle sites can fit."],
      ["Luxury Urban Flagship", M, "Lifestyle flags fit; ultra-luxury sits at Dusit Thani."],
      ["Resort & Destination Leisure", M, "Select lifestyle leisure destinations fit."],
      ["Meetings & Group Compression", L, "Large meetings are secondary."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside dusitD2."],
    ],
    ops: "Dusit dusitD2 — design lifestyle with Dusit Gold. NEW Basics create requires founder auth. Preserve casing dusitD2.",
    footprint: "Confirm live dusitD2 counts from current Dusit disclosures—selective lifestyle set.",
    insight: "dusitD2 fits lifestyle sponsors with Dusit Gold; fails when Dusit Thani luxury or ASAI select fees are assumed.",
  },
  {
    id: "NEW_dusit-princess",
    name: "Dusit Princess",
    live: "Dusit Princess",
    slug: "dusit-princess",
    parent: "Dusit International",
    loyalty: "Dusit Gold",
    proof: "DUSIT_PROOF",
    short: "Dusit Princess",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Dusit disclosure for Dusit Princess (midscale / upper-midscale)",
    brandPage: "https://www.dusit.com/",
    positioning: "Midscale / Upper-Midscale Hotels",
    commercial:
      "Versus Dusit Thani luxury, Dusit Princess sells midscale / upper-midscale value; versus ASAI select lifestyle, Princess is classic midscale hospitality inside Dusit Gold.",
    scenarios: [
      ["Midscale Urban / Leisure", "Markets where Dusit Princess ADR clears IC."],
      ["Corporate Midscale Mix", "Cities funding Princess corporate product."],
      ["Conversion Into Dusit Princess", "Conversions clearing midscale PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Dusit Princess fits productive midscale mix."],
      ["Conversion & Repositioning", M, "Conversions succeed at midscale capital."],
      ["Airport & Transportation", M, "Airport midscale can fit."],
      ["Resort & Destination Leisure", M, "Select leisure Princess assets can fit."],
      ["Luxury Urban Flagship", N, "Luxury flags sit at Dusit Thani."],
      ["Highway & Interstate Transient", L, "Pure highway limited-service may sit outside Princess."],
    ],
    ops: "Dusit Princess — midscale / upper-midscale with Dusit Gold. NEW Basics create requires founder auth.",
    footprint: "Confirm live Dusit Princess counts from current Dusit disclosures—Thailand / regional weighted.",
    insight: "Dusit Princess fits midscale sponsors with Dusit Gold; fails when Dusit Thani luxury fees are assumed.",
  },
  {
    id: "NEW_asai",
    name: "ASAI Hotels",
    live: "ASAI Hotels",
    slug: "asai-hotels",
    parent: "Dusit International",
    loyalty: "Dusit Gold",
    proof: "DUSIT_PROOF",
    short: "ASAI",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Dusit disclosure for ASAI Hotels (select / lifestyle). Newer brand—do not invent mature metrics.",
    brandPage: "https://www.dusit.com/",
    positioning: "Select Lifestyle / Community Hotels",
    commercial:
      "Versus dusitD2 higher lifestyle, ASAI sells select / community lifestyle efficiency; versus Dusit Princess midscale, ASAI is lifestyle-forward select inside Dusit Gold.",
    scenarios: [
      ["Urban Select Lifestyle", "Cities where ASAI ADR clears IC as brand matures."],
      ["Community / Lifestyle Stay", "Neighborhood markets funding ASAI product."],
      ["Conversion Into ASAI", "Conversions clearing ASAI select standards."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "ASAI fits productive urban select lifestyle mix."],
      ["Conversion & Repositioning", M, "Conversions succeed at select capital—confirm standards."],
      ["Airport & Transportation", M, "Gateway select can fit."],
      ["Luxury Urban Flagship", L, "True luxury sits at Dusit Thani."],
      ["Meetings & Group Compression", L, "Large meetings are secondary."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside ASAI."],
    ],
    ops: "Dusit ASAI — select lifestyle / community. NEW Basics create requires founder auth. Newer-brand maturity caution—do not invent mature counts.",
    footprint: "Confirm live ASAI counts from current Dusit disclosures—early-stage / growing; do not invent mature footprint.",
    insight: "ASAI fits select lifestyle sponsors with Dusit Gold; fails when Dusit Thani luxury fees are assumed. Newer-brand maturity caution.",
  },
  {
    id: "NEW_dusit-collection",
    name: "Dusit Collection",
    live: "Dusit Collection",
    slug: "dusit-collection",
    parent: "Dusit International",
    loyalty: "Dusit Gold",
    proof: "DUSIT_PROOF",
    short: "Dusit Collection",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Dusit disclosure for Dusit Collection (curated / soft collection). Confirm Dusit Gold participation.",
    brandPage: "https://www.dusit.com/",
    positioning: "Curated Independent Collection",
    commercial:
      "Versus Dusit Thani hard luxury brand, Dusit Collection sells curated independent character; versus ASAI select prototype, Collection is soft-collection affiliation inside Dusit Gold.",
    scenarios: [
      ["Independent Character Affiliation", "Assets seeking Dusit distribution without hard-brand prototype."],
      ["Curated Urban / Resort Flag", "Properties clearing Collection quality with Dusit Gold."],
      ["Soft-Brand Conversion", "Conversions preferring collection affiliation."],
    ],
    demand: [
      ["Conversion & Repositioning", S, "Soft-collection affiliation is a core Collection path."],
      ["Luxury Urban Flagship", M, "Curated luxury independents can fit."],
      ["Resort & Destination Leisure", M, "Select resort independents can fit."],
      ["Suburban Business & Leisure", L, "Prototype select-service sits outside Collection."],
      ["Meetings & Group Compression", L, "Meetings vary by asset."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Collection."],
    ],
    ops: "Dusit Collection — curated / soft collection with Dusit Gold. NEW Basics create requires founder auth. Confirm membership obligations vs hard-brand franchise tables.",
    footprint: "Confirm live Dusit Collection counts from current Dusit disclosures—intentionally curated set.",
    insight: "Dusit Collection fits independent character sponsors seeking Dusit Gold; fails when hard-brand Dusit Thani fees are assumed.",
  },
  // Rotana
  {
    id: "NEW_rotana",
    name: "Rotana Hotels & Resorts",
    live: "Rotana Hotels & Resorts",
    slug: "rotana-hotels-resorts",
    parent: "Rotana",
    loyalty: "Rotana Rewards",
    proof: "ROTANA_PROOF",
    short: "Rotana",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Rotana management / development disclosure for Rotana Hotels & Resorts (full-service). Confirm Rotana Rewards participation.",
    brandPage: "https://www.rotana.com/",
    positioning: "Full-Service Hotels & Resorts Middle East / Regional",
    commercial:
      "Versus Centro limited-service, Rotana sells full-service intensity; versus Rayhaan upper-upscale, Rotana is the core full-service flag inside Rotana Rewards.",
    scenarios: [
      ["Middle East Full-Service Urban", "Cities where Rotana ADR and Rewards clear IC."],
      ["Corporate / Government & Meetings", "Markets funding Rotana corporate and meetings product."],
      ["Conversion Into Rotana", "Conversions clearing full-service PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Rotana fits productive Middle East full-service mix."],
      ["Meetings & Group Compression", S, "Meetings are a core Rotana lane."],
      ["Resort & Destination Leisure", M, "Select resort Rotana assets can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at full-service capital."],
      ["Extended-Stay Corporate", L, "Apartments sit closer to Arjaan."],
      ["Highway & Interstate Transient", N, "Highway limited-service sits outside Rotana full-service."],
    ],
    ops: "Rotana full-service with Rotana Rewards. NEW Basics create requires founder auth. Strong Middle East / regional concentration.",
    footprint: "Confirm live Rotana counts from current Rotana disclosures—Middle East / Africa / selective regional growth.",
    insight: "Rotana fits Middle East full-service sponsors with Rotana Rewards; fails when Centro limited-service or Arjaan apartment fees are assumed.",
  },
  {
    id: "NEW_rayhaan",
    name: "Rayhaan Hotels & Resorts",
    live: "Rayhaan Hotels & Resorts",
    slug: "rayhaan-hotels-resorts",
    parent: "Rotana",
    loyalty: "Rotana Rewards",
    proof: "ROTANA_PROOF",
    short: "Rayhaan",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Rotana disclosure for Rayhaan (upper-upscale / luxury-leaning). Confirm Rotana Rewards participation.",
    brandPage: "https://www.rotana.com/",
    positioning: "Upper-Upscale / Luxury-Leaning",
    commercial:
      "Versus Rotana core full-service, Rayhaan sells higher upper-upscale intensity; versus Centro limited-service, Rayhaan is harder product inside Rotana Rewards.",
    scenarios: [
      ["Upper-Upscale Urban / Resort", "Markets where Rayhaan ADR and Rewards clear IC."],
      ["Premium Corporate & Leisure", "Corridors funding Rayhaan premium product."],
      ["Conversion Into Rayhaan", "Conversions clearing upper-upscale PIP."],
    ],
    demand: [
      ["Luxury Urban Flagship", M, "Upper-upscale flags fit; ultra-luxury diligence still required."],
      ["Suburban Business & Leisure", S, "Rayhaan fits productive premium Middle East mix."],
      ["Resort & Destination Leisure", S, "Premium leisure resorts can fit Rayhaan."],
      ["Meetings & Group Compression", M, "Meetings fit when plant exists."],
      ["Conversion & Repositioning", M, "Conversions succeed at upper-upscale capital."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Rayhaan."],
    ],
    ops: "Rotana Rayhaan — upper-upscale / luxury-leaning with Rotana Rewards. NEW Basics create requires founder auth.",
    footprint: "Confirm live Rayhaan counts from current Rotana disclosures—selective Middle East / regional set.",
    insight: "Rayhaan fits upper-upscale sponsors with Rotana Rewards; fails when Centro limited-service fees are assumed.",
  },
  {
    id: "NEW_arjaan",
    name: "Arjaan Hotel Apartments",
    live: "Arjaan Hotel Apartments",
    slug: "arjaan-hotel-apartments",
    parent: "Rotana",
    loyalty: "Rotana Rewards",
    proof: "ROTANA_PROOF",
    short: "Arjaan",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Rotana disclosure for Arjaan Hotel Apartments (serviced apartments). Do not import full-service Rotana royalty tables blindly.",
    brandPage: "https://www.rotana.com/",
    positioning: "Serviced Apartments / Long-Stay",
    commercial:
      "Versus Rotana full-service hotels, Arjaan sells apartment / long-stay economics; versus Centro limited-service transient, Arjaan is residence-led inside Rotana Rewards.",
    scenarios: [
      ["Middle East Serviced Apartment", "Markets funding Arjaan apartment stays."],
      ["Corporate / Project Long-Stay", "Corridors needing apartment inventory with Rotana Rewards."],
      ["Conversion Into Arjaan", "Assets clearing apartment PIP and Rotana standards."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Arjaan is built for serviced apartment / long-stay demand."],
      ["Suburban Business & Leisure", M, "Suburban apartment corridors can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at apartment capital."],
      ["Meetings & Group Compression", L, "Large meetings sit closer to Rotana hotels."],
      ["Luxury Urban Flagship", L, "True luxury hotels sit above Arjaan apartments."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Arjaan."],
    ],
    ops: "Rotana Arjaan — hotel apartments / long-stay with Rotana Rewards. NEW Basics create requires founder auth. Apartment housekeeping ≠ full-service hotel economics.",
    footprint: "Confirm live Arjaan counts from current Rotana disclosures—Middle East apartment weighted.",
    insight: "Arjaan fits serviced-apartment sponsors with Rotana Rewards; fails when Rotana full-service hotel fees are assumed.",
  },
  {
    id: "NEW_centro",
    name: "Centro Hotels",
    live: "Centro Hotels",
    slug: "centro-hotels",
    parent: "Rotana",
    loyalty: "Rotana Rewards",
    proof: "ROTANA_PROOF",
    short: "Centro",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Rotana disclosure for Centro Hotels (limited / select service)",
    brandPage: "https://www.rotana.com/",
    positioning: "Limited / Select Service",
    commercial:
      "Versus Rotana full-service, Centro sells limited / select-service efficiency; versus Arjaan apartments, Centro is transient select inside Rotana Rewards.",
    scenarios: [
      ["Middle East Select / Limited Service", "Markets where Centro ADR and Rewards clear IC."],
      ["Corporate Value Transient", "Cities funding Centro corporate value product."],
      ["Conversion Into Centro", "Conversions clearing select / limited-service PIP."],
    ],
    demand: [
      ["Suburban Business & Leisure", S, "Centro fits productive select / limited-service mix."],
      ["Airport & Transportation", S, "Airport / gateway Centro can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at select capital."],
      ["Highway & Interstate Transient", M, "Select highway corridors can fit Centro."],
      ["Extended-Stay Corporate", L, "Apartments sit closer to Arjaan."],
      ["Luxury Urban Flagship", N, "Luxury flags sit outside Centro."],
    ],
    ops: "Rotana Centro — limited / select service with Rotana Rewards. NEW Basics create requires founder auth.",
    footprint: "Confirm live Centro counts from current Rotana disclosures—Middle East / regional weighted.",
    insight: "Centro fits select / limited-service sponsors with Rotana Rewards; fails when Rotana full-service or Arjaan apartment fees are assumed.",
  },
  // Ascott
  {
    id: "NEW_citadines",
    name: "Citadines",
    live: "Citadines",
    slug: "citadines",
    parent: "The Ascott Limited",
    loyalty: "Ascott Star Rewards",
    proof: "ASCOTT_PROOF",
    short: "Citadines",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Ascott disclosure for Citadines (aparthotel / midscale residences). Do not apply full-service hotel economics without adjustment.",
    brandPage: "https://www.discoverasr.com/en/citadines",
    positioning: "Aparthotel / Midscale Serviced Residence",
    commercial:
      "Versus Somerset upscale residences, Citadines sells midscale aparthotel efficiency; versus lyf coliving lifestyle, Citadines is classic aparthotel inside Ascott Star Rewards.",
    scenarios: [
      ["Urban Aparthotel / Midscale Residence", "Cities funding Citadines apartment stays with Star Rewards."],
      ["Corporate Relocation & Project Stay", "Markets needing midscale residence inventory."],
      ["Conversion Into Citadines", "Assets clearing aparthotel PIP."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Citadines is built for midscale long-stay / aparthotel demand."],
      ["Suburban Business & Leisure", S, "Urban / suburban residence corridors fit Citadines."],
      ["Conversion & Repositioning", M, "Conversions succeed at aparthotel capital."],
      ["Airport & Transportation", M, "Gateway residences can fit."],
      ["Meetings & Group Compression", L, "Large meetings sit outside aparthotel core."],
      ["Luxury Urban Flagship", N, "Luxury hotels sit outside Citadines midscale residences."],
    ],
    ops: "Ascott Citadines — aparthotel / midscale serviced residence with Ascott Star Rewards. NEW Basics create requires founder auth. Length-of-stay and housekeeping ≠ transient hotel KPIs.",
    footprint: "Confirm live Citadines counts from current Ascott disclosures—global aparthotel footprint, Europe / APAC weighted.",
    insight: "Citadines fits midscale residence / relocation sponsors with Star Rewards; fails when Somerset upscale or Shangri-La hotel fees are assumed.",
  },
  {
    id: "NEW_somerset",
    name: "Somerset",
    live: "Somerset",
    slug: "somerset",
    parent: "The Ascott Limited",
    loyalty: "Ascott Star Rewards",
    proof: "ASCOTT_PROOF",
    short: "Somerset",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Ascott disclosure for Somerset (upscale serviced residences). Do not apply transient full-service hotel economics without adjustment.",
    brandPage: "https://www.discoverasr.com/en/somerset",
    positioning: "Upscale Serviced Residences",
    commercial:
      "Versus Citadines midscale aparthotel, Somerset sells upscale residence living; versus Oakwood premium apartments, Somerset is Ascott's core upscale residence inside Star Rewards.",
    scenarios: [
      ["Upscale Urban Serviced Residence", "Cities funding Somerset apartment stays with Star Rewards."],
      ["Executive Relocation & Long-Stay", "Markets needing upscale residence inventory."],
      ["Conversion Into Somerset", "Assets clearing upscale residence PIP."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Somerset is built for upscale long-stay / residence demand."],
      ["Suburban Business & Leisure", M, "Suburban upscale residence corridors can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at upscale residence capital."],
      ["Luxury Urban Flagship", L, "True luxury hotels sit above Somerset residences."],
      ["Meetings & Group Compression", L, "Large meetings are not Somerset's core."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside Somerset."],
    ],
    ops: "Ascott Somerset — upscale serviced residences with Ascott Star Rewards. NEW Basics create requires founder auth. Residence economics ≠ transient hotel FDD tables.",
    footprint: "Confirm live Somerset counts from current Ascott disclosures—APAC / global upscale residence weighted.",
    insight: "Somerset fits upscale residence / relocation sponsors with Star Rewards; fails when Citadines midscale or hotel full-service fees are assumed.",
  },
  {
    id: "NEW_lyf",
    name: "lyf",
    live: "lyf",
    slug: "lyf",
    parent: "The Ascott Limited",
    loyalty: "Ascott Star Rewards",
    proof: "ASCOTT_PROOF",
    short: "lyf",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Ascott disclosure for lyf (coliving / lifestyle residences). Newer brand—do not invent mature metrics. Residence economics ≠ transient hotel tables.",
    brandPage: "https://www.discoverasr.com/en/lyf",
    positioning: "Coliving / Lifestyle Residences",
    commercial:
      "Versus Citadines classic aparthotel, lyf sells coliving / lifestyle community residences; versus Somerset upscale, lyf is lifestyle-forward residence inside Ascott Star Rewards.",
    scenarios: [
      ["Urban Coliving / Lifestyle Residence", "Cities funding lyf community living with Star Rewards."],
      ["Flexible Long-Stay Lifestyle", "Markets wanting lifestyle residences vs classic aparthotel."],
      ["Conversion / New-Build lyf", "Sites clearing lyf coliving standards—confirm current brand definition."],
    ],
    demand: [
      ["Extended-Stay Corporate", M, "lyf can fit flexible long-stay; classic ES sits closer to Citadines / Somerset."],
      ["Suburban Business & Leisure", M, "Select urban lifestyle residence sites can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed when coliving PIP clears—confirm standards."],
      ["Luxury Urban Flagship", L, "Luxury hotels sit outside lyf."],
      ["Meetings & Group Compression", L, "Large meetings are secondary."],
      ["Highway & Interstate Transient", N, "Highway transient sits outside lyf."],
    ],
    ops: "Ascott lyf — coliving / lifestyle residences with Ascott Star Rewards. NEW Basics create requires founder auth. Newer-brand maturity caution. Preserve lowercase lyf branding.",
    footprint: "Confirm live lyf counts from current Ascott disclosures—early-stage / growing; do not invent mature footprint.",
    insight: "lyf fits coliving / lifestyle residence sponsors with Star Rewards; fails when Somerset upscale or hotel transient fees are assumed. Newer-brand maturity caution.",
  },
  {
    id: "NEW_oakwood",
    name: "Oakwood",
    live: "Oakwood",
    slug: "oakwood",
    parent: "The Ascott Limited",
    loyalty: "Ascott Star Rewards",
    proof: "ASCOTT_PROOF",
    short: "Oakwood",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeo: "Confirm current Ascott disclosure for Oakwood (serviced apartments). Do not confuse with Minor Oaks. Residence economics ≠ transient hotel tables.",
    brandPage: "https://www.discoverasr.com/en/oakwood",
    positioning: "Serviced Apartments / Corporate Housing",
    commercial:
      "Versus Citadines aparthotel, Oakwood sells corporate housing / apartment intensity; versus Minor Oaks (different parent), Oakwood is Ascott Star Rewards residence product.",
    scenarios: [
      ["Corporate Housing / Serviced Apartment", "Markets funding Oakwood apartment stays with Star Rewards."],
      ["Project & Relocation Demand", "Corridors needing Oakwood inventory for longer stays."],
      ["Conversion Into Oakwood", "Assets clearing Oakwood apartment PIP."],
    ],
    demand: [
      ["Extended-Stay Corporate", S, "Oakwood is built for corporate housing / apartment long-stay."],
      ["Suburban Business & Leisure", M, "Suburban apartment corridors can fit."],
      ["Conversion & Repositioning", M, "Conversions succeed at apartment capital."],
      ["Airport & Transportation", M, "Gateway apartments can fit."],
      ["Meetings & Group Compression", L, "Large meetings sit outside apartment core."],
      ["Luxury Urban Flagship", N, "Luxury hotels sit outside Oakwood apartments."],
    ],
    ops: "Ascott Oakwood — serviced apartments / corporate housing with Ascott Star Rewards. NEW Basics create requires founder auth. Never confuse with Minor Oaks (Batch 15). Residence economics ≠ transient hotel FDD.",
    footprint: "Confirm live Oakwood counts from current Ascott disclosures—Americas / global corporate housing weighted.",
    insight: "Oakwood fits corporate housing / apartment sponsors with Star Rewards; fails when Minor Oaks or hotel full-service fees are assumed. Distinct from Minor Oaks.",
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
lines.push(` * Factory 2.0 Batch 16 — Pan Pacific + Shangri-La + Dusit + Rotana + Ascott.`);
lines.push(` * NEW_BRAND_CANDIDATE packs use provisional NEW_<slug> keys — never invent Airtable rec IDs.`);
lines.push(` * Ascott residences / Arjaan apartments ≠ transient full-service hotel FDD tables.`);
lines.push(` * Oakwood (Ascott) ≠ Minor Oaks. PARKROYAL COLLECTION ≠ PARKROYAL Hotels.`);
lines.push(` */`);
lines.push(`import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";`);
lines.push(
  `import { PAN_PACIFIC_PROOF, GOLDEN_CIRCLE_PROOF, DUSIT_PROOF, ROTANA_PROOF, ASCOTT_PROOF } from "./be-factory-2-depth-loyalty-proofs-v1.js";`
);
lines.push(``);
lines.push(`function pack(base) {`);
lines.push(
  `  return Object.freeze({ researchSpendUsd: 0, paidCalls: 0, loyaltyProofRequired: 6, batchTag: "batch-16", ...base });`
);
lines.push(`}`);
lines.push(`function demand(rows) { return rows.map((r) => ({ title: r.title, fit: r.fit, explanation: r.explanation })); }`);
lines.push(`function gallery(brand, url) {`);
lines.push(`  return [`);
lines.push(
  `    { title: \`\${brand} — Exterior / Arrival\`, body: "Exterior presence · confirm property photography before publication", source: url },`
);
lines.push(
  `    { title: \`\${brand} — Guest Room / Suite / Apartment\`, body: "Guest-room, suite, or apartment product typical of this ladder rung", source: url },`
);
lines.push(
  `    { title: \`\${brand} — Lobby / Public Space\`, body: "Public space supporting the brand promise", source: url },`
);
lines.push(
  `    { title: \`\${brand} — F&B / Amenity Cue\`, body: "F&B, kitchen, spa, or amenity cues where required by standards", source: url },`
);
lines.push(
  `    { title: \`\${brand} — Meetings / Residence / Flexibility Cue\`, body: "Meetings, residence, coliving, or amenity cues typical of this product", source: url },`
);
lines.push(
  `    { title: \`\${brand} — Context / Setting\`, body: "Urban, resort, destination, or corridor context where typical", source: url },`
);
lines.push(`  ];`);
lines.push(`}`);
lines.push(`export const FACTORY_2_BATCH_16_V1 = "FACTORY_2_BATCH_16_V1";`);
lines.push(
  `const FDD = "Confirm current franchise / management / disclosure exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical figures from the live agreement. Amounts are not staged without a verified disclosure. Confirm brand-level loyalty participation. For serviced residences and apartments, do not import transient full-service hotel royalty tables blindly. Newer brands: do not invent mature operating metrics.";`
);
lines.push(`const S = DEMAND_FIT_LABELS.STRONG_FIT;`);
lines.push(`const M = DEMAND_FIT_LABELS.MODERATE_FIT;`);
lines.push(`const L = DEMAND_FIT_LABELS.LIMITED_FIT;`);
lines.push(`const N = DEMAND_FIT_LABELS.NOT_A_FIT;`);
lines.push(`export const BATCH_16_IDS = Object.freeze([`);
for (const b of brands) lines.push(`  "${b.id}", // ${b.name}`);
lines.push(`]);`);
lines.push(`export const BATCH_16_PACKS_BY_ID = Object.freeze({`);

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
  for (const [t, body] of b.scenarios)
    lines.push(`      { title: ${JSON.stringify(t)}, body: ${JSON.stringify(body)} },`);
  lines.push(`    ],`);
  lines.push(`    demandScenarios: demand([`);
  for (const [t, fit, expl] of b.demand)
    lines.push(
      `      { title: ${JSON.stringify(t)}, fit: ${demandFit(fit)}, explanation: ${JSON.stringify(expl)} },`
    );
  lines.push(`    ]),`);
  lines.push(`    loyaltyProof: ${b.proof}(${JSON.stringify(b.short)}),`);
  lines.push(`    feeNote: \`${esc(b.short)} application, royalty, marketing, loyalty, and PIP — \${FDD}\`,`);
  lines.push(`    operatingModel: ${JSON.stringify(b.ops)},`);
  lines.push(`    footprint: ${JSON.stringify(b.footprint)},`);
  lines.push(`    insight: ${JSON.stringify(b.insight)},`);
  lines.push(`    standards: [`);
  lines.push(
    `      { title: "Brand Product Standards", body: "Rooms / suites / apartments / residence product must clear ${esc(b.short)} standards for the operating region." },`
  );
  lines.push(
    `      { title: "Conversion / PIP Discipline", body: "PIP or affiliation quality scope must match the brand promise—not under-scoped capital." },`
  );
  lines.push(
    `      { title: "Loyalty / Network Integration", body: "Confirm ${esc(b.loyalty)} delivery obligations for this brand before IC—participation is not automatic across every sibling." },`
  );
  lines.push(`    ],`);
  lines.push(`    valueOwnerScenarios: [`);
  lines.push(
    `      { title: "Primary Owner Corridor", body: "Sites where ${esc(b.short)} ADR / contribution clears IC versus local comps." },`
  );
  lines.push(
    `      { title: "Conversion / Affiliation Path", body: "Assets where ${esc(b.short)} affiliation clears IC versus sibling alternatives." },`
  );
  lines.push(
    `      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / residence exhibits for ${esc(b.short)}." },`
  );
  lines.push(`      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },`);
  lines.push(`    ],`);
  lines.push(`    gallery: gallery(${JSON.stringify(b.short)}, ${JSON.stringify(b.brandPage)}),`);
  lines.push(`  }),`);
  lines.push(``);
}
lines.push(`});`);
lines.push(
  `export function listBatch16() { return BATCH_16_IDS.map((id) => BATCH_16_PACKS_BY_ID[id]).filter(Boolean); }`
);
lines.push(`export function getBatch16Pack(brandId) { return BATCH_16_PACKS_BY_ID[brandId] || null; }`);
lines.push(``);

writeFileSync(outPath, lines.join("\n"));
console.log(`Wrote ${outPath} (${brands.length} packs)`);
