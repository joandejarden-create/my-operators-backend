/**
 * Factory 2.0 Batch 12 — Hilton / Accor-Ennismore / Choice.
 * NEW_BRAND_CANDIDATE packs use provisional staging keys (NEW_<slug>) — never invent Airtable rec IDs.
 * Franchise fee amounts never invented — FDD / disclosure confirmation only.
 * Demand Scenario Fit: brand-specific six (not mechanical repeats).
 */
import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";
import {
  HONORS_PROOF,
  ACCOR_PROOF,
  CHOICE_PROOF,
} from "./be-factory-2-depth-loyalty-proofs-v1.js";

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    batchTag: "batch-12",
    ...base,
  });
}

function demand(rows) {
  return rows.map((r) => ({
    title: r.title,
    fit: r.fit,
    explanation: r.explanation,
  }));
}

function gallery(brand, url) {
  return [
    { title: `${brand} — Exterior / Arrival`, body: "Exterior presence · confirm property photography before publication", source: url },
    { title: `${brand} — Guest Room`, body: "Guest-room product typical of this ladder rung", source: url },
    { title: `${brand} — Lobby / Public Space`, body: "Public space supporting the brand promise", source: url },
    { title: `${brand} — F&B / Amenity Cue`, body: "F&B or amenity cues where required by standards", source: url },
    { title: `${brand} — Meetings / Flexibility Cue`, body: "Meetings, kitchen, or efficiency cues typical of this product", source: url },
    { title: `${brand} — Context / Setting`, body: "Urban, suburban, airport, or destination context where typical", source: url },
  ];
}

export const FACTORY_2_BATCH_12_V1 = "FACTORY_2_BATCH_12_V1";

const FDD =
  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";

const S = DEMAND_FIT_LABELS.STRONG_FIT;
const M = DEMAND_FIT_LABELS.MODERATE_FIT;
const L = DEMAND_FIT_LABELS.LIMITED_FIT;
const N = DEMAND_FIT_LABELS.NOT_A_FIT;

export const BATCH_12_IDS = Object.freeze([
  "recQxUKKgH0Pq1VgR", // Graduate by Hilton
  "NEW_outset-collection", // Outset Collection by Hilton
  "recVAnsCsz4ifaAw2", // LivSmart Studios by Hilton
  "recjvESTY8tYZjlAJ", // Embassy Suites by Hilton
  "recqsodgJLUzsdCED", // NoMad Hotels
  "recF9TQzTd3wwEiKa", // ibis Styles
  "rec0lQ4Imf614F8jo", // ibis budget
  "recGJctYTmJiwwXKG", // greet
  "rec9Q89yzK4QZKnal", // hotelF1
  "rechyrQmHmeR1ovme", // TRIBE
  "reck9Gq60El13HG5Z", // Adagio
  "recSAVQWVAnn0NKYg", // Adagio Access
  "recKPpUCq9EZlSSYx", // 25hours Hotels
  "recXmadOxgzU6Ug5l", // The Hoxton
  "recPF5NM9kDZwkNmS", // Mondrian
  "recharEz6EaOg9AiP", // Hyde Hotels
  "recrJtboQyjYGsZZm", // SLS Hotels
  "rect7iv1y3mGFadqL", // Swissôtel
  "reclu1Fgv5Leq25qn", // Park Plaza
  "recyI1O61Or7LXwXl", // Park Inn by Radisson
]);

export const BATCH_12_PACKS_BY_ID = Object.freeze({
  "recQxUKKgH0Pq1VgR": pack({
    brandId: "recQxUKKgH0Pq1VgR",
    brandName: "Graduate by Hilton",
    liveBrandName: "Graduate Hotels",
    slug: "graduate-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Hilton franchise / affiliation disclosure for Graduate by Hilton (college-town lifestyle)",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/brands/graduate-by-hilton/",
    },
    positioning: "College Town Lifestyle",
    commercialDifferentiator: "Versus Canopy neighborhood lifestyle, Graduate sells campus-town lifestyle identity; versus Embassy Suites, it is lifestyle transient rather than suite product inside Hilton Honors.",
    scenarios: [
      { title: "College-Town Lifestyle Urban", body: "Campus-adjacent markets where Graduate identity and Honors clear lifestyle ADR." },
      { title: "Adaptive Reuse Campus Corridors", body: "Conversions that can fund Graduate design narrative near universities." },
      { title: "Sibling Clarity Versus Canopy", body: "Owners comparing Graduate campus lifestyle versus Canopy neighborhood lifestyle." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Graduate core is college-town lifestyle with Hilton Honors." },
      { title: "University & Medical", fit: S, explanation: "Campus and academic demand is a primary Graduate pocket." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears Graduate standards." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss Graduate lifestyle intensity." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings plant is Embassy / Hilton full-service territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Graduate lifestyle." },
    ]),
    loyaltyProof: HONORS_PROOF("Graduate"),
    feeNote: `Graduate application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hilton lifestyle brand focused on college towns with Hilton Honors. Confirm franchise/affiliation path—do not invent royalty percentages. Preserve live name Graduate Hotels.",
    footprint: "Confirm live hotel counts from current Hilton / Graduate disclosures—campus-town footprint; do not underwrite from launch media alone.",
    insight: "Graduate fits campus-town lifestyle assets with Honors; fails when Embassy suite economics or Canopy neighborhood fees are blindly assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Graduate standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Hilton Honors delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Graduate ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Graduate affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Graduate." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Graduate", "https://www.hilton.com/en/hotels/brands/graduate-by-hilton/"),
  }),

  "NEW_outset-collection": pack({
    brandId: "NEW_outset-collection",
    brandName: "Outset Collection by Hilton",
    liveBrandName: "Outset Collection by Hilton",
    slug: "outset-collection-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeography: "Confirm current Hilton soft-collection / affiliation exhibits for Outset AFTER Brand Basics create",
    sources: {
      brandPage: "https://www.hilton.com/",
    },
    positioning: "Independent Collection",
    commercialDifferentiator: "Versus Tapestry / Curio soft collections, Outset (if a distinct Hilton soft lane) needs verified ladder position before IC; stage research only—do not invent fee tables.",
    scenarios: [
      { title: "Soft-Collection Diligence Gate", body: "Sites where sponsors must confirm Outset versus Curio / Tapestry before assuming a distinct owner use case." },
      { title: "Independent Character Conversion", body: "Distinctive independents needing Honors—validate brand standards after Basics create." },
      { title: "Conversion Into Outset", body: "Conversions only after authorized Brand Basics create and current disclosure exhibits." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: M, explanation: "Provisional: soft-collection conversions may fit—confirm after Basics create." },
      { title: "Urban Lifestyle & Design", fit: M, explanation: "Provisional: design-led independents may fit Outset standards." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Provisional: destination character hotels may fit when curation holds." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually sit below soft-collection upscale." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not the soft-collection lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside independent collection." },
    ]),
    loyaltyProof: HONORS_PROOF("Outset Collection"),
    feeNote: `Outset Collection application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics match for Outset Collection by Hilton. Do not invent rec ID. Soft-collection diligence after authorized create.",
    footprint: "No Airtable inventory yet. Confirm existence and counts from Hilton disclosures before underwriting.",
    insight: "Outset Collection is NEW_BRAND_CANDIDATE—founder must authorize Basics create only after brand existence and ladder position are verified versus Curio / Tapestry.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Outset Collection standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Hilton Honors delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Outset Collection ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Outset Collection affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Outset Collection." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Outset Collection", "https://www.hilton.com/"),
  }),

  "recVAnsCsz4ifaAw2": pack({
    brandId: "recVAnsCsz4ifaAw2",
    brandName: "LivSmart Studios by Hilton",
    liveBrandName: "LivSmart Studios by Hilton",
    slug: "livsmart-studios-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Hilton franchise / development disclosure for LivSmart Studios (extended-stay)",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/brands/livsmart-studios-by-hilton/",
    },
    positioning: "Midscale Extended-Stay",
    commercialDifferentiator: "Versus Homewood / Home2 extended-stay, LivSmart is Hilton's newer extended-stay ladder rung; versus Embassy Suites, it is length-of-stay kitchenette economics inside Hilton Honors.",
    scenarios: [
      { title: "Extended-Stay Corporate", body: "Markets with multi-week demand needing kitchenettes with Hilton Honors." },
      { title: "Project / Medical Longer Stay", body: "Project and medical corridors needing suite product." },
      { title: "New-Build Extended-Stay Prototype", body: "New-builds optimized for LivSmart prototypes—confirm disclosure exhibits." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "LivSmart thesis is extended-stay with kitchens and Hilton Honors." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce weekly stays are a primary extended-stay pocket." },
      { title: "University & Medical", fit: M, explanation: "Medical/university longer stays can fit suite kitchenette product." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban midweek extended-stay works; pure weekend leisure is secondary." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is Hampton / Hilton Garden territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside LivSmart extended-stay." },
    ]),
    loyaltyProof: HONORS_PROOF("LivSmart Studios"),
    feeNote: `LivSmart Studios application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hilton midscale extended-stay with kitchenettes and Hilton Honors. Housekeeping cadence differs from transient—confirm FDD exhibits.",
    footprint: "Confirm live hotel counts from current Hilton / LivSmart disclosures—growth brand; do not underwrite from launch media alone.",
    insight: "LivSmart fits sponsors with genuine extended-stay demand and suite capital; fails when underwritten like Embassy Suites transient without kitchen PIP.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear LivSmart Studios standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Hilton Honors delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where LivSmart Studios ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where LivSmart Studios affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for LivSmart Studios." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("LivSmart Studios", "https://www.hilton.com/en/hotels/brands/livsmart-studios-by-hilton/"),
  }),

  "recjvESTY8tYZjlAJ": pack({
    brandId: "recjvESTY8tYZjlAJ",
    brandName: "Embassy Suites by Hilton",
    liveBrandName: "Embassy Suites by Hilton",
    slug: "embassy-suites-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Hilton franchise disclosure for Embassy Suites (all-suite upscale)",
    sources: {
      brandPage: "https://www.hilton.com/en/embassy-suites/",
    },
    positioning: "Upscale All-Suite",
    commercialDifferentiator: "Versus Hilton Hotels full-service, Embassy sells all-suite product with atrium / cooked-to-order breakfast cues; versus LivSmart extended-stay, it is suite transient rather than kitchenette weekly stay inside Hilton Honors.",
    scenarios: [
      { title: "Upscale All-Suite Urban / Suburban", body: "Markets where suite product and Honors clear upscale ADR." },
      { title: "Meetings-Capable Suite Hotels", body: "Assets with atrium / meetings plant that can fund Embassy standards." },
      { title: "Conversion Into Embassy Suites", body: "Conversions that can deliver suite and atrium PIP." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Embassy core includes suburban upscale suite business/leisure with Honors." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Meetings-capable atrium plant is an Embassy strength when present." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport suite hubs fit when product and Honors defend share." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds suites and public space." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "True weekly kitchenette extended-stay is LivSmart / Homewood territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Embassy upscale suites." },
    ]),
    loyaltyProof: HONORS_PROOF("Embassy Suites"),
    feeNote: `Embassy Suites application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hilton upscale all-suite with cooked-to-order breakfast / evening reception cues and Hilton Honors. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Hilton / Embassy Suites disclosures—large North America footprint with international presence.",
    insight: "Embassy Suites fits owners funding upscale suite product with Honors; fails when LivSmart extended-stay or select-service capital is assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Embassy Suites standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Hilton Honors delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Embassy Suites ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Embassy Suites affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Embassy Suites." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Embassy Suites", "https://www.hilton.com/en/embassy-suites/"),
  }),

  "recqsodgJLUzsdCED": pack({
    brandId: "recqsodgJLUzsdCED",
    brandName: "NoMad Hotels",
    liveBrandName: "NoMad",
    slug: "nomad-hotels",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Hilton / NoMad affiliation disclosure (luxury lifestyle soft brand)",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/brands/nomad/",
    },
    positioning: "Luxury Lifestyle",
    commercialDifferentiator: "Versus Waldorf / Conrad hard luxury, NoMad sells lifestyle luxury individuality; versus Curio soft collection, NoMad is a defined lifestyle luxury node with Hilton Honors—confirm affiliation path carefully.",
    scenarios: [
      { title: "Luxury Lifestyle Urban", body: "Gateway lifestyle districts where NoMad design and Honors clear luxury ADR." },
      { title: "Destination Design Luxury", body: "Destination assets that can fund design authorship and F&B character." },
      { title: "Affiliation Into NoMad", body: "Affiliations that can meet NoMad luxury lifestyle standards." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "NoMad core is luxury lifestyle urban design with Hilton Honors." },
      { title: "Luxury Urban Flagship", fit: S, explanation: "Luxury flagship lifestyle fits when product clears NoMad standards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed only when luxury lifestyle PIP is funded." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select destination resorts can work when design authorship is credible." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics sit outside NoMad luxury lifestyle." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside luxury lifestyle." },
    ]),
    loyaltyProof: HONORS_PROOF("NoMad"),
    feeNote: `NoMad application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hilton luxury lifestyle brand (live name: NoMad). Luxury-band capital and operating intensity—do not import select-service economics. Confirm Honors delivery and affiliation exhibits.",
    footprint: "Confirm live hotel counts from current Hilton / NoMad disclosures—select curated footprint.",
    insight: "NoMad fits sponsors who can fund luxury lifestyle delivery with Honors; wrong when capital is upper-upscale only. Preserve live name NoMad.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear NoMad standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Hilton Honors delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where NoMad ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where NoMad affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for NoMad." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("NoMad", "https://www.hilton.com/en/hotels/brands/nomad/"),
  }),

  "recF9TQzTd3wwEiKa": pack({
    brandId: "recF9TQzTd3wwEiKa",
    brandName: "ibis Styles",
    liveBrandName: "ibis Styles",
    slug: "ibis-styles",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for ibis Styles (design midscale)",
    sources: {
      brandPage: "https://all.accor.com/brands/ibis-styles.en.shtml",
    },
    positioning: "Design Midscale",
    commercialDifferentiator: "Versus classic ibis economy, ibis Styles sells design-led midscale; versus Mercure fuller midscale, Styles keeps leaner capital with ALL - Accor Live Limitless.",
    scenarios: [
      { title: "Design Midscale Urban / Corridor", body: "Markets where design midscale and ALL clear ADR without Mercure capital." },
      { title: "Conversion Into ibis Styles", body: "Conversions that can fund Styles design standards." },
      { title: "Sibling Clarity Versus ibis / ibis budget", body: "Owners comparing Styles versus classic ibis and ibis budget." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "ibis Styles core is design-led midscale with ALL." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban midscale mix fits when product clears Styles standards." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge design midscale can fit Styles." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears Styles—not economy-only scopes." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Hard economy highway transient is ibis budget / hotelF1 territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside ibis Styles midscale." },
    ]),
    loyaltyProof: ACCOR_PROOF("ibis Styles"),
    feeNote: `ibis Styles application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor design midscale limited-service with ALL. Confirm ibis family exhibits—do not invent royalty percentages or confuse with classic ibis / ibis budget.",
    footprint: "Confirm live hotel counts from current Accor / ibis Styles disclosures across EMEA, APAC, and Americas.",
    insight: "ibis Styles fits design midscale with ALL; fails when classic ibis economy or Mercure midscale fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear ibis Styles standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where ibis Styles ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where ibis Styles affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for ibis Styles." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("ibis Styles", "https://all.accor.com/brands/ibis-styles.en.shtml"),
  }),

  "rec0lQ4Imf614F8jo": pack({
    brandId: "rec0lQ4Imf614F8jo",
    brandName: "ibis budget",
    liveBrandName: "ibis budget",
    slug: "ibis-budget",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for ibis budget (economy / budget)",
    sources: {
      brandPage: "https://all.accor.com/brands/ibis-budget.en.shtml",
    },
    positioning: "Budget Economy",
    commercialDifferentiator: "Versus classic ibis, ibis budget is leaner budget economy; versus hotelF1 ultra-budget, it sits above the hardest Accor budget rung with ALL participation.",
    scenarios: [
      { title: "Budget Economy Corridor", body: "Highway and value corridors needing Accor budget product and ALL." },
      { title: "Airport & Gateway Value", body: "Airport-edge value stays without midscale capital." },
      { title: "Conversion Into ibis budget", body: "Conversions that can fund budget standards—not Styles design scopes." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "ibis budget is built for value transient corridors with ALL." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets are a core budget economy use case." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport-edge budget stays fit when product clears ibis budget standards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds budget—not midscale Styles capital." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban value mix can work at budget ADR." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside ibis budget." },
    ]),
    loyaltyProof: ACCOR_PROOF("ibis budget"),
    feeNote: `ibis budget application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor budget economy limited-service with lean operations and ALL. Confirm exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / ibis budget disclosures.",
    insight: "ibis budget fits Accor budget economy with ALL; fails when Styles design or Mercure midscale fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear ibis budget standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where ibis budget ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where ibis budget affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for ibis budget." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("ibis budget", "https://all.accor.com/brands/ibis-budget.en.shtml"),
  }),

  "recGJctYTmJiwwXKG": pack({
    brandId: "recGJctYTmJiwwXKG",
    brandName: "greet",
    liveBrandName: "Greet",
    slug: "greet",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for greet (eco midscale)",
    sources: {
      brandPage: "https://all.accor.com/brands/greet.en.shtml",
    },
    positioning: "Eco Midscale",
    commercialDifferentiator: "Versus ibis Styles design midscale, greet sells eco / responsible midscale cues; versus Mercure, it keeps leaner midscale capital with ALL.",
    scenarios: [
      { title: "Eco Midscale Urban / Corridor", body: "Markets where responsible midscale positioning and ALL clear ADR." },
      { title: "Conversion Into greet", body: "Conversions that can fund greet eco product standards." },
      { title: "Sibling Clarity Versus ibis Styles", body: "Owners comparing greet versus Styles design midscale." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "greet fits midscale business/leisure with eco cues and ALL." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Conversions are a core greet use case when eco standards clear." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets fit when ADR supports greet fees." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge midscale can fit greet." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Pure lifestyle design intensity is more Styles / TRIBE." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside greet midscale." },
    ]),
    loyaltyProof: ACCOR_PROOF("greet"),
    feeNote: `greet application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor eco / responsible midscale with ALL. Confirm franchise exhibits—do not invent royalty percentages. Preserve live name Greet.",
    footprint: "Confirm live hotel counts from current Accor / greet disclosures.",
    insight: "greet fits owners funding eco midscale with ALL; fails when Styles or Mercure fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear greet standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where greet ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where greet affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for greet." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("greet", "https://all.accor.com/brands/greet.en.shtml"),
  }),

  "rec9Q89yzK4QZKnal": pack({
    brandId: "rec9Q89yzK4QZKnal",
    brandName: "hotelF1",
    liveBrandName: "hotelF1",
    slug: "hotelf1",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for hotelF1 (ultra-budget)",
    sources: {
      brandPage: "https://all.accor.com/brands/hotelf1.en.shtml",
    },
    positioning: "Ultra Budget",
    commercialDifferentiator: "Versus ibis budget, hotelF1 is Accor's hardest ultra-budget rung; versus classic ibis, it is deliberately leaner capital and product with ALL participation where applicable.",
    scenarios: [
      { title: "Ultra-Budget Corridor", body: "Value corridors needing Accor ultra-budget product." },
      { title: "Highway Transient Value", body: "Highway sites optimized for ultra-budget ADR." },
      { title: "Conversion Into hotelF1", body: "Conversions that can fund ultra-budget standards only." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "hotelF1 is built for ultra-budget transient corridors." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets needing Accor ultra-budget." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge ultra-budget can fit when product clears hotelF1." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed only with ultra-budget capital scopes." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban midscale mix usually sits above hotelF1." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside hotelF1." },
    ]),
    loyaltyProof: ACCOR_PROOF("hotelF1"),
    feeNote: `hotelF1 application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor ultra-budget limited-service with leanest Accor operating stack and ALL where delivered. Confirm exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / hotelF1 disclosures—primarily EMEA.",
    insight: "hotelF1 fits ultra-budget Accor economics; fails when ibis budget or midscale fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear hotelF1 standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where hotelF1 ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where hotelF1 affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for hotelF1." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("hotelF1", "https://all.accor.com/brands/hotelf1.en.shtml"),
  }),

  "rechyrQmHmeR1ovme": pack({
    brandId: "rechyrQmHmeR1ovme",
    brandName: "TRIBE",
    liveBrandName: "TRIBE",
    slug: "tribe",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for TRIBE (upscale lifestyle)",
    sources: {
      brandPage: "https://all.accor.com/brands/tribe.en.shtml",
    },
    positioning: "Upscale Lifestyle",
    commercialDifferentiator: "Versus ibis Styles midscale design, TRIBE sells upscale lifestyle; versus SO/ Accor lifestyle, confirm sibling clarity on upscale lifestyle intensity inside ALL.",
    scenarios: [
      { title: "Upscale Lifestyle Urban", body: "City sites where TRIBE lifestyle and ALL lift rates." },
      { title: "Conversion Into TRIBE", body: "Conversions that can fund lifestyle public-space standards." },
      { title: "Sibling Clarity Versus SO/ / Mama Shelter", body: "Owners comparing TRIBE versus other Accor lifestyle lanes." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "TRIBE core is upscale urban lifestyle with ALL." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears lifestyle standards." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss TRIBE lifestyle intensity." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not TRIBE's core lane." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is Pullman / Novotel territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside upscale lifestyle." },
    ]),
    loyaltyProof: ACCOR_PROOF("TRIBE"),
    feeNote: `TRIBE application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor upscale lifestyle brand with design-forward public space and ALL. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / TRIBE disclosures.",
    insight: "TRIBE fits upscale lifestyle with ALL; fails when midscale Styles or ultra-budget fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear TRIBE standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where TRIBE ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where TRIBE affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for TRIBE." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("TRIBE", "https://all.accor.com/brands/tribe.en.shtml"),
  }),

  "reck9Gq60El13HG5Z": pack({
    brandId: "reck9Gq60El13HG5Z",
    brandName: "Adagio",
    liveBrandName: "Adagio Aparthotel",
    slug: "adagio",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for Adagio (aparthotel midscale+)",
    sources: {
      brandPage: "https://all.accor.com/brands/adagio.en.shtml",
    },
    positioning: "Aparthotel Midscale",
    commercialDifferentiator: "Versus Adagio Access leaner aparthotel, Adagio sells fuller aparthotel product; versus Novotel transient midscale, it is length-of-stay apartment economics with ALL.",
    scenarios: [
      { title: "Aparthotel Extended Stay", body: "Urban markets with multi-night and weekly demand needing kitchens." },
      { title: "Urban Aparthotel Conversion", body: "Conversions that can fund Adagio apartment standards." },
      { title: "Sibling Clarity Versus Adagio Access", body: "Owners comparing Adagio versus Access ladder." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Adagio core is aparthotel extended-stay with ALL." },
      { title: "Urban Lifestyle & Design", fit: M, explanation: "Urban aparthotel can carry lifestyle cues when product clears Adagio." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban midweek extended-stay works when kitchens are real." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when apartment PIP is funded." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is ibis territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside Adagio aparthotel." },
    ]),
    loyaltyProof: ACCOR_PROOF("Adagio"),
    feeNote: `Adagio application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor aparthotel (live: Adagio Aparthotel) with kitchenette product and ALL. Distinct from transient midscale—confirm exhibits.",
    footprint: "Confirm live hotel counts from current Accor / Adagio disclosures across EMEA and beyond.",
    insight: "Adagio fits aparthotel demand with ALL; fails when Adagio Access lean capital or Novotel transient fees are assumed. Preserve live name Adagio Aparthotel.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Adagio standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Adagio ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Adagio affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Adagio." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Adagio", "https://all.accor.com/brands/adagio.en.shtml"),
  }),

  "recSAVQWVAnn0NKYg": pack({
    brandId: "recSAVQWVAnn0NKYg",
    brandName: "Adagio Access",
    liveBrandName: "Aparthotel Adagio Access",
    slug: "adagio-access",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for Adagio Access (economy aparthotel)",
    sources: {
      brandPage: "https://all.accor.com/brands/adagio-access.en.shtml",
    },
    positioning: "Economy Aparthotel",
    commercialDifferentiator: "Versus Adagio fuller aparthotel, Access is leaner economy aparthotel; versus ibis budget transient, it is length-of-stay kitchenette economics with ALL.",
    scenarios: [
      { title: "Economy Aparthotel Extended Stay", body: "Markets needing kitchenettes at economy capital with ALL." },
      { title: "Conversion Into Adagio Access", body: "Conversions that can fund Access apartment standards." },
      { title: "Sibling Clarity Versus Adagio", body: "Owners comparing Access versus fuller Adagio." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Adagio Access thesis is economy aparthotel extended-stay with ALL." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce weekly stays fit Access kitchenette product." },
      { title: "University & Medical", fit: M, explanation: "Medical/university longer stays can fit when rates clear economy aparthotel IC." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds Access—not fuller Adagio scopes." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is ibis budget territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside Adagio Access." },
    ]),
    loyaltyProof: ACCOR_PROOF("Adagio Access"),
    feeNote: `Adagio Access application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor economy aparthotel (live: Aparthotel Adagio Access) with leaner product than Adagio and ALL. Confirm exhibits.",
    footprint: "Confirm live hotel counts from current Accor / Adagio Access disclosures.",
    insight: "Adagio Access fits lean aparthotel demand with ALL; fails when fuller Adagio capital is assumed. Preserve live name Aparthotel Adagio Access.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Adagio Access standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Adagio Access ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Adagio Access affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Adagio Access." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Adagio Access", "https://all.accor.com/brands/adagio-access.en.shtml"),
  }),

  "recKPpUCq9EZlSSYx": pack({
    brandId: "recKPpUCq9EZlSSYx",
    brandName: "25hours Hotels",
    liveBrandName: "25Hours Hotel",
    slug: "25hours-hotels",
    parentCompany: "Accor / Ennismore",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor / Ennismore affiliation disclosure for 25hours (lifestyle)",
    sources: {
      brandPage: "https://www.25hours-hotels.com/",
    },
    positioning: "Lifestyle Design Hotels",
    commercialDifferentiator: "Versus The Hoxton lifestyle, 25hours sells playful design authorship; versus Mondrian luxury lifestyle, it is more accessible lifestyle intensity inside ALL / Ennismore.",
    scenarios: [
      { title: "Lifestyle Urban Design", body: "Cities where 25hours design narrative and ALL clear lifestyle ADR." },
      { title: "Conversion Into 25hours", body: "Conversions that can fund design authorship and F&B character." },
      { title: "Sibling Clarity Versus Hoxton / Mondrian", body: "Owners comparing 25hours versus other Ennismore lifestyle lanes." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "25hours core is lifestyle design urban hotels with ALL." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears 25hours standards." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select destination lifestyle hotels can fit." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics miss 25hours lifestyle intensity." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings plant is not the 25hours lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside lifestyle design." },
    ]),
    loyaltyProof: ACCOR_PROOF("25hours"),
    feeNote: `25hours application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor / Ennismore lifestyle brand (live: 25Hours Hotel). Soft/lifestyle diligence—not hard-brand prototype economics. Confirm ALL delivery.",
    footprint: "Confirm live hotel counts from current 25hours / Accor disclosures—curated lifestyle footprint.",
    insight: "25hours fits design lifestyle assets with ALL; fails when midscale Accor fees are assumed. Preserve live name 25Hours Hotel.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear 25hours standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where 25hours ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where 25hours affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for 25hours." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("25hours", "https://www.25hours-hotels.com/"),
  }),

  "recXmadOxgzU6Ug5l": pack({
    brandId: "recXmadOxgzU6Ug5l",
    brandName: "The Hoxton",
    liveBrandName: "The Hoxton",
    slug: "the-hoxton",
    parentCompany: "Accor / Ennismore",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor / Ennismore affiliation disclosure for The Hoxton (lifestyle)",
    sources: {
      brandPage: "https://thehoxton.com/",
    },
    positioning: "Urban Lifestyle",
    commercialDifferentiator: "Versus 25hours playful design, Hoxton sells neighborhood lifestyle hospitality; versus Mondrian luxury lifestyle, Hoxton is more accessible urban lifestyle inside ALL / Ennismore.",
    scenarios: [
      { title: "Neighborhood Lifestyle Urban", body: "Walkable urban neighborhoods where Hoxton and ALL lift lifestyle rates." },
      { title: "Adaptive Reuse Lifestyle", body: "Conversions that can fund Hoxton public-space and F&B character." },
      { title: "Sibling Clarity Versus 25hours / Mondrian", body: "Owners comparing Hoxton versus other Ennismore lanes." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "The Hoxton core is neighborhood urban lifestyle with ALL." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Conversions fit when individuality and Accor systems both clear IC." },
      { title: "Resort & Destination Leisure", fit: L, explanation: "Destination resort is secondary to urban neighborhood product." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics miss Hoxton lifestyle." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not the Hoxton lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside Hoxton lifestyle." },
    ]),
    loyaltyProof: ACCOR_PROOF("The Hoxton"),
    feeNote: `The Hoxton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor / Ennismore urban lifestyle brand with ALL. Soft/lifestyle diligence—confirm affiliation exhibits.",
    footprint: "Confirm live hotel counts from current Hoxton / Accor disclosures—urban lifestyle footprint.",
    insight: "The Hoxton fits neighborhood lifestyle assets with ALL; fails when midscale Accor or Mondrian luxury fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear The Hoxton standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where The Hoxton ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where The Hoxton affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for The Hoxton." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("The Hoxton", "https://thehoxton.com/"),
  }),

  "recPF5NM9kDZwkNmS": pack({
    brandId: "recPF5NM9kDZwkNmS",
    brandName: "Mondrian",
    liveBrandName: "Mondrian",
    slug: "mondrian",
    parentCompany: "Accor / Ennismore",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor / Ennismore affiliation disclosure for Mondrian (luxury lifestyle)",
    sources: {
      brandPage: "https://www.sbe.com/hotels/mondrian",
    },
    positioning: "Luxury Lifestyle",
    commercialDifferentiator: "Versus Hyde / SLS Ennismore luxury lifestyle siblings, Mondrian sells design luxury lifestyle; versus Swissôtel classic upscale, Mondrian is lifestyle luxury intensity inside ALL.",
    scenarios: [
      { title: "Luxury Lifestyle Urban", body: "Gateway lifestyle districts where Mondrian and ALL clear luxury ADR." },
      { title: "Destination Design Luxury", body: "Destination assets that can fund luxury lifestyle F&B and design." },
      { title: "Sibling Clarity Versus Hyde / SLS", body: "Owners comparing Mondrian versus Hyde and SLS." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Mondrian core is luxury lifestyle urban design with ALL." },
      { title: "Luxury Urban Flagship", fit: S, explanation: "Luxury flagship lifestyle fits when product clears Mondrian." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select destination resorts can work when design authorship holds." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed only with luxury lifestyle PIP." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service sits outside Mondrian luxury lifestyle." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside luxury lifestyle." },
    ]),
    loyaltyProof: ACCOR_PROOF("Mondrian"),
    feeNote: `Mondrian application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor / Ennismore luxury lifestyle brand with ALL. Luxury-band capital—do not import midscale Accor economics.",
    footprint: "Confirm live hotel counts from current Mondrian / Accor disclosures—curated luxury lifestyle footprint.",
    insight: "Mondrian fits luxury lifestyle with ALL; fails when Hoxton accessible lifestyle or Swissôtel classic fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Mondrian standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Mondrian ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Mondrian affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Mondrian." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Mondrian", "https://www.sbe.com/hotels/mondrian"),
  }),

  "recharEz6EaOg9AiP": pack({
    brandId: "recharEz6EaOg9AiP",
    brandName: "Hyde Hotels",
    liveBrandName: "Hyde",
    slug: "hyde-hotels",
    parentCompany: "Accor / Ennismore",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor / Ennismore affiliation disclosure for Hyde (luxury lifestyle)",
    sources: {
      brandPage: "https://www.sbe.com/hotels/hyde",
    },
    positioning: "Luxury Lifestyle",
    commercialDifferentiator: "Versus Mondrian design luxury lifestyle, Hyde sells nightlife-forward luxury lifestyle; versus SLS, confirm sibling clarity inside Ennismore / ALL.",
    scenarios: [
      { title: "Luxury Lifestyle Nightlife Urban", body: "Lifestyle districts where Hyde energy and ALL clear luxury ADR." },
      { title: "Destination Party / Resort Energy", body: "Destination assets that can sustain nightlife and F&B intensity." },
      { title: "Sibling Clarity Versus Mondrian / SLS", body: "Owners comparing Hyde versus Mondrian and SLS." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Hyde core is luxury lifestyle urban energy with ALL." },
      { title: "Luxury Urban Flagship", fit: S, explanation: "Luxury lifestyle flagship fits when product clears Hyde." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Destination resorts can work when entertainment intensity is funded." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed only with luxury lifestyle PIP." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service sits outside Hyde." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside luxury lifestyle." },
    ]),
    loyaltyProof: ACCOR_PROOF("Hyde"),
    feeNote: `Hyde application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor / Ennismore luxury lifestyle brand (live: Hyde) with ALL. Luxury-band operating intensity—confirm affiliation exhibits.",
    footprint: "Confirm live hotel counts from current Hyde / Accor disclosures.",
    insight: "Hyde fits luxury lifestyle energy with ALL; fails when midscale Accor capital is assumed. Preserve live name Hyde.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Hyde standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Hyde ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Hyde affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Hyde." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Hyde", "https://www.sbe.com/hotels/hyde"),
  }),

  "recrJtboQyjYGsZZm": pack({
    brandId: "recrJtboQyjYGsZZm",
    brandName: "SLS Hotels",
    liveBrandName: "SLS",
    slug: "sls-hotels",
    parentCompany: "Accor / Ennismore",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor / Ennismore affiliation disclosure for SLS (luxury lifestyle)",
    sources: {
      brandPage: "https://www.sbe.com/hotels/sls",
    },
    positioning: "Luxury Lifestyle",
    commercialDifferentiator: "Versus Mondrian / Hyde Ennismore siblings, SLS sells luxury lifestyle with strong F&B / nightlife cues; versus Swissôtel classic upscale, SLS is lifestyle luxury intensity inside ALL.",
    scenarios: [
      { title: "Luxury Lifestyle Urban Flagship", body: "Gateway districts where SLS and ALL clear luxury ADR." },
      { title: "Destination Luxury Lifestyle", body: "Destination assets that can fund SLS F&B and design." },
      { title: "Sibling Clarity Versus Mondrian / Hyde", body: "Owners comparing SLS versus Mondrian and Hyde." },
    ],
    demandScenarios: demand([
      { title: "Luxury Urban Flagship", fit: S, explanation: "SLS fits luxury lifestyle flagship with ALL when product clears standards." },
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Design and nightlife-forward urban lifestyle is an SLS strength." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select destination resorts can work when luxury lifestyle is funded." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed only with luxury lifestyle PIP." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service sits outside SLS." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside luxury lifestyle." },
    ]),
    loyaltyProof: ACCOR_PROOF("SLS"),
    feeNote: `SLS application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor / Ennismore luxury lifestyle brand (live: SLS) with ALL. Luxury-band capital—confirm affiliation exhibits.",
    footprint: "Confirm live hotel counts from current SLS / Accor disclosures—curated footprint.",
    insight: "SLS fits luxury lifestyle with ALL; fails when Swissôtel classic or midscale Accor fees are assumed. Preserve live name SLS.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear SLS standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where SLS ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where SLS affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for SLS." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("SLS", "https://www.sbe.com/hotels/sls"),
  }),

  "rect7iv1y3mGFadqL": pack({
    brandId: "rect7iv1y3mGFadqL",
    brandName: "Swissôtel",
    liveBrandName: "Swissotel",
    slug: "swissotel",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Accor franchise / development disclosure for Swissôtel (upscale full-service)",
    sources: {
      brandPage: "https://all.accor.com/brands/swissotel.en.shtml",
    },
    positioning: "Upscale Full-Service",
    commercialDifferentiator: "Versus Pullman premium meetings, Swissôtel sells classic upscale full-service; versus Mondrian lifestyle luxury, Swissôtel is classic Accor upscale with ALL.",
    scenarios: [
      { title: "Upscale Full-Service Urban", body: "Gateway and secondary cities where Swissôtel and ALL clear upscale ADR." },
      { title: "Meetings-Capable Upscale", body: "Assets with meetings plant that can fund Swissôtel standards." },
      { title: "Conversion Into Swissôtel", body: "Conversions that can deliver upscale full-service PIP." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Swissôtel fits upscale business/leisure with ALL when product clears standards." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Meetings-capable plant is a Swissôtel strength." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport upscale hubs can fit Swissôtel." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds upscale full-service." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Pure lifestyle design is Ennismore / TRIBE territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service sits outside Swissôtel upscale full-service." },
    ]),
    loyaltyProof: ACCOR_PROOF("Swissôtel"),
    feeNote: `Swissôtel application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor upscale full-service (live: Swissotel) with meetings capability and ALL. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / Swissôtel disclosures across EMEA, APAC, and Americas.",
    insight: "Swissôtel fits classic upscale full-service with ALL; fails when Ennismore lifestyle or ibis midscale fees are assumed. Preserve live name Swissotel.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Swissôtel standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Swissôtel ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Swissôtel affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Swissôtel." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Swissôtel", "https://all.accor.com/brands/swissotel.en.shtml"),
  }),

  "reclu1Fgv5Leq25qn": pack({
    brandId: "reclu1Fgv5Leq25qn",
    brandName: "Park Plaza",
    liveBrandName: "Park Plaza",
    slug: "park-plaza",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD / Radisson portfolio exhibits for Park Plaza (upscale)",
    sources: {
      brandPage: "https://www.choicehotels.com/park-plaza",
    },
    positioning: "Upscale Full-Service",
    commercialDifferentiator: "Versus Park Inn by Radisson midscale, Park Plaza sells higher upscale full-service; versus Radisson Blu, confirm sibling clarity inside Choice Privileges / Radisson portfolio.",
    scenarios: [
      { title: "Upscale Full-Service Urban", body: "Markets where Park Plaza product and Privileges clear upscale ADR." },
      { title: "Meetings-Capable Upscale", body: "Assets with meetings plant that can fund Park Plaza standards." },
      { title: "Conversion Into Park Plaza", body: "Conversions that can deliver upscale full-service PIP." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Park Plaza fits upscale business/leisure with Privileges when product clears standards." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Meetings-capable plant is a Park Plaza strength." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport upscale hubs can fit Park Plaza." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds upscale full-service." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Pure lifestyle design is more Radisson RED territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service sits outside Park Plaza upscale." },
    ]),
    loyaltyProof: CHOICE_PROOF("Park Plaza"),
    feeNote: `Park Plaza application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice / Radisson portfolio upscale full-service with Choice Privileges. Live Airtable parent may still read Radisson Hotel Group—confirm Choice commercial path. Do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice / Park Plaza disclosures—primarily EMEA.",
    insight: "Park Plaza fits upscale full-service with Privileges; fails when Park Inn midscale fees are assumed. Airtable parent currently Radisson Hotel Group (Draft).",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Park Plaza standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Park Plaza ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Park Plaza affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Park Plaza." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Park Plaza", "https://www.choicehotels.com/park-plaza"),
  }),

  "recyI1O61Or7LXwXl": pack({
    brandId: "recyI1O61Or7LXwXl",
    brandName: "Park Inn by Radisson",
    liveBrandName: "Park Inn by Radisson",
    slug: "park-inn-by-radisson",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD / Radisson portfolio exhibits for Park Inn by Radisson (midscale)",
    sources: {
      brandPage: "https://www.choicehotels.com/park-inn",
    },
    positioning: "Midscale Full-Service",
    commercialDifferentiator: "Versus Park Plaza upscale, Park Inn is midscale full-service; versus Comfort limited-service, Park Inn keeps fuller midscale product inside Choice Privileges.",
    scenarios: [
      { title: "Midscale Full-Service Urban / Suburban", body: "Markets where Park Inn product and Privileges clear midscale ADR." },
      { title: "Meetings-Capable Midscale", body: "Secondary cities with midscale meetings plant." },
      { title: "Conversion Into Park Inn", body: "Conversions that can fund midscale full-service PIP." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Park Inn fits midscale business/leisure with Privileges." },
      { title: "Meetings & Group Compression", fit: M, explanation: "Meetings plant can fit when midscale full-service is present." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport midscale hubs can fit Park Inn." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale full-service." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Hard limited-service highway transient is Comfort / Quality territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside Park Inn midscale." },
    ]),
    loyaltyProof: CHOICE_PROOF("Park Inn"),
    feeNote: `Park Inn application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice / Radisson portfolio midscale full-service with Choice Privileges. Live Airtable parent may still read Radisson Hotel Group—confirm Choice commercial path.",
    footprint: "Confirm live hotel counts from current Choice / Park Inn disclosures.",
    insight: "Park Inn fits midscale full-service with Privileges; fails when Park Plaza upscale fees are assumed. Airtable status Draft.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Park Inn standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Park Inn ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Park Inn affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Park Inn." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Park Inn", "https://www.choicehotels.com/park-inn"),
  }),

});

export function listBatch12() {
  return BATCH_12_IDS.map((id) => BATCH_12_PACKS_BY_ID[id]).filter(Boolean);
}

export function getBatch12Pack(brandId) {
  return BATCH_12_PACKS_BY_ID[brandId] || null;
}
