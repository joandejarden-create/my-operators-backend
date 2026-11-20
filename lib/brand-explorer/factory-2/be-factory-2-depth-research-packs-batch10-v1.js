/**
 * Factory 2.0 Batch 10 — EXISTING Active Airtable brands
 * (Accor, Choice, Wyndham, BWH & independent collections).
 * All IDs are live EXISTING_CANONICAL Brand Basics.
 * Franchise fee amounts never invented — FDD / membership disclosure confirmation only.
 * Independent networks (Preferred / SLH): membership diligence — not franchise royalty tables.
 * Demand Scenario Fit: brand-specific six (not mechanical repeats).
 */
import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";
import {
  ACCOR_PROOF,
  CHOICE_PROOF,
  WYNDHAM_PROOF,
  BWH_PROOF,
  MEMBERSHIP_NETWORK_PROOF,
} from "./be-factory-2-depth-loyalty-proofs-v1.js";

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    batchTag: "batch-10",
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

export const FACTORY_2_BATCH_10_V1 = "FACTORY_2_BATCH_10_V1";

const FDD =
  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";

const S = DEMAND_FIT_LABELS.STRONG_FIT;
const M = DEMAND_FIT_LABELS.MODERATE_FIT;
const L = DEMAND_FIT_LABELS.LIMITED_FIT;
const N = DEMAND_FIT_LABELS.NOT_A_FIT;

export const BATCH_10_IDS = Object.freeze([
  "rec7hTXwMRC81EPqz", // Handwritten Collection
  "recrWCD1LMqu864oU", // MGallery Collection
  "recFW9kfqKfOjv7Z1", // Pullman
  "recJhPaDVU3YUDQUt", // Fairmont
  "recevrLJ3m6rIug3S", // Mercure
  "reclFXbpZ5XzLWbGP", // ibis
  "recXCZCK05XXYX7Q8", // Mama Shelter
  "recQE2lSSSSyuUrMQ", // Novotel
  "recTJdPlr4mDs9app", // SO/
  "recwXZ5gVZ8ZH8ekA", // BW Premier Collection
  "recdeh1NsP4gjrv80", // BW Signature Collection
  "recob7tgHRryRSbeO", // Trademark Collection by Wyndham
  "rec5CNMM4ZUD7ZHlM", // Dazzler by Wyndham
  "reclcjg5Foa9Vs5TC", // Suburban Studios
  "recqkkrsevi4r9ibj", // Everhome Suites
  "recd8o4k1JddhkRWW", // Quality Inn
  "recmKqo7M7mLZgRqQ", // Radisson RED by Choice
  "recywbx1YQSTCPqW1", // Radisson by Choice
  "recwl5JOYxlChuCAr", // Preferred Hotels & Resorts
  "recjjSnY2opb8P4DG", // Small Luxury Hotels of the World
]);

export const BATCH_10_PACKS_BY_ID = Object.freeze({
  rec7hTXwMRC81EPqz: pack({
    brandId: "rec7hTXwMRC81EPqz",
    brandName: "Handwritten Collection",
    liveBrandName: "Handwritten Collection",
    slug: "handwritten-collection",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor soft-collection / Handwritten membership and commercial exhibits",
    sources: {
      brandPage: "https://all.accor.com/brands/handwritten.en.shtml",
    },
    positioning: "Midscale Independent Collection",
    commercialDifferentiator: "Versus Mercure midscale hard brand, Handwritten sells independent character inside Accor; versus MGallery luxury boutique collection, it is a more accessible midscale collection lane.",
    scenarios: [
      { title: "Character Midscale Conversion", body: "Distinctive midscale assets that need Accor distribution without a hard prototype." },
      { title: "Urban Independent Collection", body: "Urban conversions where local story and ALL clear rates above generic midscale." },
      { title: "Soft-Collection Diligence", body: "Owners comparing Handwritten versus Mercure hard brand and other Accor soft lanes." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Handwritten is built for character conversions that keep independent story with Accor systems." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix can work when product clears collection standards—not economy-only capital." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets fit when ALL and local character defend ADR." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype midscale is usually a Mercure / ibis lane, not Handwritten." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is limited for midscale soft collection." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Handwritten midscale collection." },
    ]),
    loyaltyProof: ACCOR_PROOF("Handwritten"),
    feeNote: `Handwritten application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor soft / independent collection — distinctive midscale conversions with ALL participation. Do not underwrite as a hard-brand prototype franchise.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "Handwritten fits owners who need Accor soft-collection distribution for distinctive midscale assets; fails when sponsors underwrite Mercure hard-brand fees or invent royalty percentages.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Handwritten standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Handwritten ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Handwritten affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Handwritten." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Handwritten", "https://all.accor.com/brands/handwritten.en.shtml"),
  }),

  recrWCD1LMqu864oU: pack({
    brandId: "recrWCD1LMqu864oU",
    brandName: "MGallery Collection",
    liveBrandName: "MGallery Collection",
    slug: "mgallery-collection",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor MGallery soft-collection / boutique collection exhibits",
    sources: {
      brandPage: "https://all.accor.com/brands/mgallery.en.shtml",
    },
    positioning: "Luxury Boutique Collection",
    commercialDifferentiator: "Versus Fairmont landmark luxury, MGallery sells boutique collection identity; versus Handwritten midscale collection, it is a higher luxury boutique lane.",
    scenarios: [
      { title: "Boutique Luxury Conversion", body: "Design-led assets that can fund MGallery boutique standards and ALL." },
      { title: "Heritage & Adaptive Reuse", body: "Heritage conversions where boutique story clears luxury-adjacent rates." },
      { title: "Soft-Collection Versus Hard Luxury", body: "Owners comparing MGallery soft collection versus Fairmont / Sofitel hard luxury." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "MGallery core is boutique conversion and adaptive reuse with Accor luxury-adjacent distribution." },
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Urban design-led demand fits when product clears boutique collection standards." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select destination resorts can work when boutique identity is credible." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually sit below MGallery boutique luxury." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not the MGallery lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside luxury boutique collection." },
    ]),
    loyaltyProof: ACCOR_PROOF("MGallery"),
    feeNote: `MGallery application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor luxury boutique soft collection — design-led independent character with ALL. Membership/collection diligence, not hard-brand prototype economics.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "MGallery fits owners with design-credible boutique assets needing Accor ALL; fails when capital only supports midscale soft collection or when fees are invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear MGallery standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where MGallery ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where MGallery affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for MGallery." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("MGallery", "https://all.accor.com/brands/mgallery.en.shtml"),
  }),

  recFW9kfqKfOjv7Z1: pack({
    brandId: "recFW9kfqKfOjv7Z1",
    brandName: "Pullman",
    liveBrandName: "Pullman",
    slug: "pullman",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor franchise / management disclosure for Pullman (premium business & meetings)",
    sources: {
      brandPage: "https://all.accor.com/brands/pullman.en.shtml",
    },
    positioning: "Premium Business and Meetings",
    commercialDifferentiator: "Versus Novotel midscale full-service, Pullman sells higher meetings and premium business intensity; versus Fairmont luxury, it is productive premium rather than landmark luxury.",
    scenarios: [
      { title: "Urban Business & Meetings", body: "Gateway and secondary cities where meetings ADR clears premium full-service IC." },
      { title: "Airport Premium Hub", body: "Airport-edge hubs needing Pullman meetings product and ALL." },
      { title: "Conversion Into Premium Meetings", body: "Conversions that can fund meetings plant and Pullman standards." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Pullman is built for productive meetings and premium business compression." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport hubs fit when meetings and ALL defend share." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban productive mix is a core Pullman use case." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds meetings and premium rooms." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay kitchens are not Pullman's core product." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Pullman premium meetings." },
    ]),
    loyaltyProof: ACCOR_PROOF("Pullman"),
    feeNote: `Pullman application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Premium business and meetings full-service Accor brand. Confirm franchise vs management path from current Accor exhibits—do not invent fee tables.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "Pullman fits owners underwriting premium business/meetings with Accor ALL; fails when capital only supports Novotel midscale or when royalty % is invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Pullman standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Pullman ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Pullman affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Pullman." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Pullman", "https://all.accor.com/brands/pullman.en.shtml"),
  }),

  recJhPaDVU3YUDQUt: pack({
    brandId: "recJhPaDVU3YUDQUt",
    brandName: "Fairmont",
    liveBrandName: "Fairmont",
    slug: "fairmont",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor luxury franchise / management exhibits for Fairmont",
    sources: {
      brandPage: "https://all.accor.com/brands/fairmont.en.shtml",
    },
    positioning: "Luxury",
    commercialDifferentiator: "Versus Pullman premium meetings, Fairmont sells landmark luxury; versus MGallery boutique collection, it is a harder luxury brand experience.",
    scenarios: [
      { title: "Landmark Urban Luxury", body: "Iconic urban assets that can fund Fairmont luxury service and ALL." },
      { title: "Luxury Resort Destinations", body: "Resort sites where Fairmont F&B and spa intensity clear luxury IC." },
      { title: "Conversion Into Landmark Luxury", body: "Conversions only when capital and service clear luxury QA." },
    ],
    demandScenarios: demand([
      { title: "Luxury Urban Flagship", fit: S, explanation: "Fairmont core is landmark urban luxury with Accor ALL." },
      { title: "Resort & Destination Leisure", fit: S, explanation: "Luxury resorts fit when service and F&B clear Fairmont standards." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Luxury meetings and social events are a Fairmont strength when plant exists." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions only when capital funds true luxury—not soft reflags." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics sit outside Fairmont luxury." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient is not a Fairmont use case." },
    ]),
    loyaltyProof: ACCOR_PROOF("Fairmont"),
    feeNote: `Fairmont application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Landmark luxury full-service Accor brand with high service intensity, elevated F&B, and ALL. Often management-led—confirm agreement path before IC.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "Fairmont fits owners with landmark luxury capital and Accor ALL; fails when sponsors underwrite Pullman premium fees or invent luxury royalty tables.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Fairmont standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Fairmont ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Fairmont affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Fairmont." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Fairmont", "https://all.accor.com/brands/fairmont.en.shtml"),
  }),

  recevrLJ3m6rIug3S: pack({
    brandId: "recevrLJ3m6rIug3S",
    brandName: "Mercure",
    liveBrandName: "Mercure",
    slug: "mercure",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor franchise / development disclosure for Mercure (midscale)",
    sources: {
      brandPage: "https://all.accor.com/brands/mercure.en.shtml",
    },
    positioning: "Midscale",
    commercialDifferentiator: "Versus ibis economy, Mercure is fuller midscale product; versus Handwritten soft collection, it is a harder midscale brand standard.",
    scenarios: [
      { title: "Urban & Suburban Midscale", body: "Markets where Mercure midscale and ALL clear local ADR." },
      { title: "Conversion Into Mercure Midscale", body: "Conversions that can fund Mercure PIP and prototype standards." },
      { title: "Regional Midscale Network", body: "Secondary cities needing Accor midscale consistency." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Mercure core demand is midscale business/leisure with ALL." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets fit when Mercure product and ALL defend share." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport midscale can work when prototype and ALL clear comps." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale—not economy-only capital." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is limited versus Pullman." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside Mercure midscale." },
    ]),
    loyaltyProof: ACCOR_PROOF("Mercure"),
    feeNote: `Mercure application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor midscale hard brand with local character cues inside a defined prototype. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "Mercure fits owners needing Accor midscale hard-brand distribution; fails when capital only supports ibis or when fees are invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Mercure standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Mercure ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Mercure affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Mercure." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Mercure", "https://all.accor.com/brands/mercure.en.shtml"),
  }),

  reclFXbpZ5XzLWbGP: pack({
    brandId: "reclFXbpZ5XzLWbGP",
    brandName: "ibis",
    liveBrandName: "ibis",
    slug: "ibis",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor franchise / development disclosure for ibis (economy)",
    sources: {
      brandPage: "https://all.accor.com/brands/ibis.en.shtml",
    },
    positioning: "Economy",
    commercialDifferentiator: "Versus Mercure midscale, ibis is leaner economy; versus ibis budget, classic ibis sits above pure budget when product clears the brand's economy bar.",
    scenarios: [
      { title: "Economy Urban & Highway", body: "Sites where economy product and ALL clear value ADR." },
      { title: "Conversion Into Economy Accor", body: "Conversions that can fund ibis standards without midscale capital." },
      { title: "Secondary-Market Value", body: "Secondary markets needing Accor economy distribution." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "ibis fits value transient and highway corridors with Accor ALL." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets are a core ibis economy use case." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport economy can work when product and ALL defend share." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP matches economy—not midscale spend." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban value mix fits when guests accept economy product." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside ibis economy." },
    ]),
    loyaltyProof: ACCOR_PROOF("ibis"),
    feeNote: `ibis application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor economy limited-service brand with lean operations and ALL participation. Confirm current ibis family exhibits (ibis / ibis Styles / ibis budget distinctions).",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "ibis fits owners underwriting Accor economy with ALL; fails when sponsors import Mercure midscale fees or invent royalty percentages.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear ibis standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where ibis ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where ibis affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for ibis." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("ibis", "https://all.accor.com/brands/ibis.en.shtml"),
  }),

  recXCZCK05XXYX7Q8: pack({
    brandId: "recXCZCK05XXYX7Q8",
    brandName: "Mama Shelter",
    liveBrandName: "Mama Shelter",
    slug: "mama-shelter",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor / Mama Shelter lifestyle affiliation and commercial exhibits",
    sources: {
      brandPage: "https://www.mamashelter.com/",
    },
    positioning: "Urban Social Lifestyle",
    commercialDifferentiator: "Versus SO/ luxury lifestyle, Mama Shelter is more accessible social lifestyle; versus Novotel midscale full-service, it sells nightlife and social F&B as the commercial edge.",
    scenarios: [
      { title: "Urban Social Lifestyle", body: "Cities where F&B and social programming clear lifestyle ADR with ALL." },
      { title: "Adaptive Reuse Lifestyle", body: "Adaptive-reuse assets that can host Mama Shelter public-space intensity." },
      { title: "Lifestyle Versus Soft Collection", body: "Owners comparing Mama Shelter lifestyle versus Accor soft collections." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Mama Shelter core is urban social lifestyle with Accor ALL." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Adaptive reuse and lifestyle conversions are a primary path." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss Mama Shelter social F&B intensity." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not the Mama Shelter lane." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large corporate meetings are secondary to social lifestyle demand." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside urban social lifestyle." },
    ]),
    loyaltyProof: ACCOR_PROOF("Mama Shelter"),
    feeNote: `Mama Shelter application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Urban social lifestyle brand with F&B-forward public space and Accor ALL. Diligence lifestyle operating intensity—not select-service economics.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "Mama Shelter fits owners who can fund social F&B lifestyle operations with Accor ALL; fails when capital only supports Novotel midscale or when fees are invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Mama Shelter standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Mama Shelter ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Mama Shelter affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Mama Shelter." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Mama Shelter", "https://www.mamashelter.com/"),
  }),

  recQE2lSSSSyuUrMQ: pack({
    brandId: "recQE2lSSSSyuUrMQ",
    brandName: "Novotel",
    liveBrandName: "Novotel",
    slug: "novotel",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor franchise / development disclosure for Novotel (midscale full-service)",
    sources: {
      brandPage: "https://all.accor.com/brands/novotel.en.shtml",
    },
    positioning: "Midscale Full-Service",
    commercialDifferentiator: "Versus Pullman premium meetings, Novotel is midscale full-service; versus Mercure midscale, it typically carries fuller F&B and meetings plant.",
    scenarios: [
      { title: "Midscale Full-Service Urban", body: "Urban and suburban sites needing Novotel full-service and ALL." },
      { title: "Family & Meetings Blend", body: "Markets where family leisure and light meetings share a midscale plant." },
      { title: "Conversion Into Novotel", body: "Conversions that can fund midscale full-service PIP." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Novotel core is midscale full-service business/leisure with ALL." },
      { title: "Meetings & Group Compression", fit: M, explanation: "Light-to-moderate meetings fit when plant exists—not Pullman premium intensity." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport midscale full-service can work with ALL." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds full-service midscale." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets are a strong Novotel use case." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside Novotel midscale full-service." },
    ]),
    loyaltyProof: ACCOR_PROOF("Novotel"),
    feeNote: `Novotel application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor midscale full-service with family-friendly and meetings-capable product. Confirm franchise exhibits—do not invent royalty tables.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "Novotel fits owners needing Accor midscale full-service with ALL; fails when capital only supports ibis or when Pullman premium fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Novotel standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Novotel ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Novotel affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Novotel." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Novotel", "https://all.accor.com/brands/novotel.en.shtml"),
  }),

  recTJdPlr4mDs9app: pack({
    brandId: "recTJdPlr4mDs9app",
    brandName: "SO/",
    liveBrandName: "SO/",
    slug: "so",
    parentCompany: "Accor",
    loyaltyProgram: "ALL - Accor Live Limitless",
    feeGeography: "Confirm current Accor SO/ luxury lifestyle franchise / management exhibits",
    sources: {
      brandPage: "https://all.accor.com/brands/so.en.shtml",
    },
    positioning: "Luxury Lifestyle",
    commercialDifferentiator: "Versus Fairmont landmark luxury, SO/ sells contemporary luxury lifestyle; versus Mama Shelter social lifestyle, it is a higher luxury ladder.",
    scenarios: [
      { title: "Luxury Lifestyle Urban", body: "Gateway cities where design luxury and ALL clear lifestyle ADR." },
      { title: "Design-Led Conversion", body: "Conversions that can fund SO/ luxury lifestyle standards." },
      { title: "Lifestyle Versus Landmark Luxury", body: "Owners comparing SO/ lifestyle versus Fairmont landmark luxury." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "SO/ core is luxury lifestyle urban design with Accor ALL." },
      { title: "Luxury Urban Flagship", fit: S, explanation: "Luxury lifestyle flagship demand fits when product clears SO/ standards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions only when capital funds luxury lifestyle—not midscale." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select resorts can work when lifestyle identity is credible." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics sit outside SO/ luxury lifestyle." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient is not a SO/ use case." },
    ]),
    loyaltyProof: ACCOR_PROOF("SO/"),
    feeNote: `SO/ application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Accor luxury lifestyle brand with design-forward public space and ALL. Confirm agreement path—often selective development.",
    footprint: "Confirm live hotel counts from current Accor / brand disclosures—do not underwrite from launch media alone.",
    insight: "SO/ fits owners with luxury lifestyle capital and Accor ALL; fails when sponsors underwrite Mama Shelter or midscale fees.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear SO/ standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm ALL - Accor Live Limitless delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where SO/ ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where SO/ affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for SO/." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("SO/", "https://all.accor.com/brands/so.en.shtml"),
  }),

  recwXZ5gVZ8ZH8ekA: pack({
    brandId: "recwXZ5gVZ8ZH8ekA",
    brandName: "BW Premier Collection",
    liveBrandName: "BW Premier Collection",
    slug: "bw-premier-collection",
    parentCompany: "Best Western Hotels & Resorts",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current Best Western Premier Collection membership / soft-collection exhibits",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/brand/bw-premier-collection.html",
    },
    positioning: "Premium Independent Collection",
    commercialDifferentiator: "Versus BW Signature Collection upper-midscale soft collection, Premier is a higher premium independent lane; versus hard Best Western Plus, it preserves more independent character.",
    scenarios: [
      { title: "Premium Independent Conversion", body: "Distinctive assets needing BWH Rewards without a hard prototype." },
      { title: "Upper Independent Collection", body: "Markets where premium collection identity clears rates above Signature." },
      { title: "Soft-Collection Diligence", body: "Owners comparing Premier versus Signature and hard Best Western flags." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Premier Collection is built for distinctive conversions with BWH Rewards." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix can work when product clears premium collection standards." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets fit when Rewards and identity defend ADR." },
      { title: "Urban Lifestyle & Design", fit: M, explanation: "Design-led urban assets can fit the premium independent lane." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype midscale is usually a hard Best Western lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway economy transient sits outside premium independent collection." },
    ]),
    loyaltyProof: BWH_PROOF("BW Premier Collection"),
    feeNote: `BW Premier Collection application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Best Western soft / independent premium collection — distinctive hotels with Best Western Rewards. Membership diligence, not hard-brand prototype economics.",
    footprint: "Confirm live hotel counts from current Best Western Hotels & Resorts / brand disclosures—do not underwrite from launch media alone.",
    insight: "BW Premier Collection fits owners needing BWH soft-collection premium distribution; fails when hard Best Western fees are assumed or invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear BW Premier Collection standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Best Western Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where BW Premier Collection ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where BW Premier Collection affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for BW Premier Collection." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("BW Premier Collection", "https://www.bestwestern.com/en_US/hotels/brand/bw-premier-collection.html"),
  }),

  recdeh1NsP4gjrv80: pack({
    brandId: "recdeh1NsP4gjrv80",
    brandName: "BW Signature Collection",
    liveBrandName: "BW Signature Collection",
    slug: "bw-signature-collection",
    parentCompany: "Best Western Hotels & Resorts",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current Best Western Signature Collection membership / soft-collection exhibits",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/brand/bw-signature-collection.html",
    },
    positioning: "Upper-Midscale Independent Collection",
    commercialDifferentiator: "Versus BW Premier Collection premium, Signature is a more accessible upper-midscale soft collection; versus hard Best Western Plus, it preserves independent character.",
    scenarios: [
      { title: "Upper-Midscale Independent Conversion", body: "Distinctive upper-midscale assets needing BWH Rewards." },
      { title: "Soft-Collection Versus Hard BWH", body: "Owners comparing Signature soft collection versus Best Western Plus hard brand." },
      { title: "Regional Independent Character", body: "Secondary markets where independent story and Rewards clear ADR." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Signature Collection targets distinctive conversions with BWH Rewards." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets are a strong Signature use case." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix fits when product clears upper-midscale collection standards." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is usually a hard BWH lane." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is limited." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside Signature upper-midscale collection." },
    ]),
    loyaltyProof: BWH_PROOF("BW Signature Collection"),
    feeNote: `BW Signature Collection application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Best Western soft / independent upper-midscale collection with Best Western Rewards. Membership diligence—not hard-brand prototype.",
    footprint: "Confirm live hotel counts from current Best Western Hotels & Resorts / brand disclosures—do not underwrite from launch media alone.",
    insight: "BW Signature Collection fits owners needing upper-midscale soft-collection BWH distribution; fails when Premier or hard-brand fee tables are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear BW Signature Collection standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Best Western Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where BW Signature Collection ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where BW Signature Collection affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for BW Signature Collection." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("BW Signature Collection", "https://www.bestwestern.com/en_US/hotels/brand/bw-signature-collection.html"),
  }),

  recob7tgHRryRSbeO: pack({
    brandId: "recob7tgHRryRSbeO",
    brandName: "Trademark Collection by Wyndham",
    liveBrandName: "Trademark Collection by Wyndham",
    slug: "trademark-collection-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham Trademark Collection soft-brand / franchise exhibits",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/trademark",
    },
    positioning: "Upscale Independent Collection",
    commercialDifferentiator: "Versus Dazzler lifestyle hard brand, Trademark preserves independent identity; versus La Quinta limited-service, it is an upscale soft-collection lane.",
    scenarios: [
      { title: "Upscale Independent Soft Brand", body: "Distinctive upscale assets needing Wyndham Rewards without a hard prototype." },
      { title: "Conversion Soft Collection", body: "Conversions that can clear Trademark soft-brand standards." },
      { title: "Soft Brand Versus Lifestyle Prototype", body: "Owners comparing Trademark soft brand versus Dazzler lifestyle prototype." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Trademark is built for independent conversions with Wyndham Rewards." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix can work when product clears soft-brand upscale standards." },
      { title: "Urban Lifestyle & Design", fit: M, explanation: "Design-led urban independents can fit Trademark." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets fit when Rewards and identity defend ADR." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype upscale is usually not Trademark's core." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway economy transient sits outside Trademark upscale soft collection." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Trademark Collection"),
    feeNote: `Trademark Collection application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Wyndham soft-brand collection for independent and distinctive hotels with Wyndham Rewards. Soft-brand diligence—not hard-brand prototype economics.",
    footprint: "Confirm live hotel counts from current Wyndham Hotels & Resorts / brand disclosures—do not underwrite from launch media alone.",
    insight: "Trademark fits owners needing Wyndham soft-brand distribution for distinctive assets; fails when Dazzler prototype fees or invented royalties are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Trademark Collection standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Wyndham Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Trademark Collection ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Trademark Collection affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Trademark Collection." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Trademark Collection", "https://www.wyndhamhotels.com/trademark"),
  }),

  rec5CNMM4ZUD7ZHlM: pack({
    brandId: "rec5CNMM4ZUD7ZHlM",
    brandName: "Dazzler by Wyndham",
    liveBrandName: "Dazzler by Wyndham",
    slug: "dazzler-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham franchise / development disclosure for Dazzler (upscale urban lifestyle)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/dazzler",
    },
    positioning: "Upscale Urban Lifestyle",
    commercialDifferentiator: "Versus Trademark soft collection, Dazzler sells a defined lifestyle prototype; versus midscale Wyndham flags, it is an upscale urban lifestyle lane.",
    scenarios: [
      { title: "Upscale Urban Lifestyle", body: "Urban corridors where design lifestyle and Rewards clear ADR." },
      { title: "LATAM & Lifestyle Urban", body: "Lifestyle urban markets (incl. historic LATAM concentration) needing Dazzler product." },
      { title: "Conversion Into Lifestyle Prototype", body: "Conversions that can fund Dazzler design standards." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Dazzler core is upscale urban lifestyle with Wyndham Rewards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears lifestyle standards." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss Dazzler urban lifestyle." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not Dazzler's core lane." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is limited." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside upscale urban lifestyle." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Dazzler"),
    feeNote: `Dazzler application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Wyndham upscale urban lifestyle brand with defined design personality and Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham Hotels & Resorts / brand disclosures—do not underwrite from launch media alone.",
    insight: "Dazzler fits owners funding upscale urban lifestyle with Wyndham Rewards; fails when Trademark soft-brand economics are assumed or fees invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Dazzler standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Wyndham Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Dazzler ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Dazzler affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Dazzler." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Dazzler", "https://www.wyndhamhotels.com/dazzler"),
  }),

  reclcjg5Foa9Vs5TC: pack({
    brandId: "reclcjg5Foa9Vs5TC",
    brandName: "Suburban Studios",
    liveBrandName: "Suburban Studios",
    slug: "suburban-studios",
    parentCompany: "Choice Hotels",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice Hotels FDD for Suburban Studios (economy extended stay)",
    sources: {
      brandPage: "https://www.choicehotels.com/suburban",
    },
    positioning: "Economy Extended Stay",
    commercialDifferentiator: "Versus Everhome midscale extended stay, Suburban is economy extended stay; versus Quality Inn transient midscale, it is weekly-stay kitchen-led product.",
    scenarios: [
      { title: "Economy Extended-Stay Corridors", body: "Suburban and secondary markets needing economy weekly stays with Privileges." },
      { title: "Conversion Into Economy Extended Stay", body: "Conversions that can fund kitchenette product for Suburban." },
      { title: "Value Long-Stay Demand", body: "Crews and value long-stay demand needing Choice distribution." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Suburban Studios core is economy extended-stay / weekly demand with Privileges." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets are a strong economy extended-stay use case." },
      { title: "Highway & Interstate Transient", fit: M, explanation: "Some transient occurs, but kitchens and weekly economics are the core." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when kitchenette PIP clears Suburban standards." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban value mix fits economy extended stay." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside economy extended stay." },
    ]),
    loyaltyProof: CHOICE_PROOF("Suburban Studios"),
    feeNote: `Suburban Studios application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice economy extended-stay with kitchenettes and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / brand disclosures—do not underwrite from launch media alone.",
    insight: "Suburban Studios fits owners underwriting Choice economy extended stay; fails when Everhome midscale fees are assumed or royalties invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Suburban Studios standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Suburban Studios ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Suburban Studios affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Suburban Studios." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Suburban Studios", "https://www.choicehotels.com/suburban"),
  }),

  recqkkrsevi4r9ibj: pack({
    brandId: "recqkkrsevi4r9ibj",
    brandName: "Everhome Suites",
    liveBrandName: "Everhome Suites",
    slug: "everhome-suites",
    parentCompany: "Choice Hotels",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice Hotels FDD for Everhome Suites (midscale extended stay)",
    sources: {
      brandPage: "https://www.choicehotels.com/everhome-suites",
    },
    positioning: "Midscale Extended Stay",
    commercialDifferentiator: "Versus Suburban economy extended stay, Everhome is midscale extended stay; versus Quality Inn transient, it is kitchen-led weekly product.",
    scenarios: [
      { title: "Midscale Extended-Stay Corridors", body: "Markets where midscale weekly stays and Privileges clear ADR above economy extended stay." },
      { title: "Conversion Into Midscale Extended Stay", body: "Conversions that can fund Everhome suite and kitchen standards." },
      { title: "Corporate Weekly Demand", body: "Crews and corporate weekly demand needing Choice midscale extended stay." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Everhome core is midscale extended-stay corporate/weekly demand with Privileges." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban midscale extended stay is a primary use case." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets fit when midscale weekly ADR clears IC." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale extended-stay product." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge weekly demand can fit Everhome." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside midscale extended stay." },
    ]),
    loyaltyProof: CHOICE_PROOF("Everhome Suites"),
    feeNote: `Everhome Suites application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice midscale extended-stay with fuller suite product and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / brand disclosures—do not underwrite from launch media alone.",
    insight: "Everhome fits owners underwriting Choice midscale extended stay; fails when Suburban economy fees are assumed or royalties invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Everhome Suites standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Everhome Suites ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Everhome Suites affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Everhome Suites." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Everhome Suites", "https://www.choicehotels.com/everhome-suites"),
  }),

  recd8o4k1JddhkRWW: pack({
    brandId: "recd8o4k1JddhkRWW",
    brandName: "Quality Inn",
    liveBrandName: "Quality Inn",
    slug: "quality-inn",
    parentCompany: "Choice Hotels",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice Hotels FDD for Quality Inn (midscale)",
    sources: {
      brandPage: "https://www.choicehotels.com/quality-inn",
    },
    positioning: "Midscale",
    commercialDifferentiator: "Versus Comfort midscale limited-service siblings, Quality is a core Choice midscale node; versus Suburban extended stay, it is transient midscale rather than weekly kitchens.",
    scenarios: [
      { title: "Midscale Limited-Service Transient", body: "Highway and suburban sites needing Quality midscale and Privileges." },
      { title: "Conversion Into Quality Midscale", body: "Conversions that can fund Quality PIP and breakfast standards." },
      { title: "Secondary-Market Midscale", body: "Secondary markets needing Choice midscale distribution." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Quality Inn core includes highway and value midscale transient with Privileges." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets are a strong Quality use case." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban midscale limited-service mix fits Quality." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport midscale can work when product and Privileges defend share." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale—not economy-only capital." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury flagship sits outside Quality midscale." },
    ]),
    loyaltyProof: CHOICE_PROOF("Quality Inn"),
    feeNote: `Quality Inn application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice midscale limited-service brand with breakfast and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / brand disclosures—do not underwrite from launch media alone.",
    insight: "Quality Inn fits owners needing Choice midscale limited-service with Privileges; fails when extended-stay or invented royalty tables are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Quality Inn standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Quality Inn ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Quality Inn affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Quality Inn." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Quality Inn", "https://www.choicehotels.com/quality-inn"),
  }),

  recmKqo7M7mLZgRqQ: pack({
    brandId: "recmKqo7M7mLZgRqQ",
    brandName: "Radisson RED by Choice",
    liveBrandName: "Radisson RED by Choice",
    slug: "radisson-red-by-choice",
    parentCompany: "Choice Hotels",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice Hotels FDD / development exhibits for Radisson RED (upscale lifestyle)",
    sources: {
      brandPage: "https://www.choicehotels.com/radisson-red",
    },
    positioning: "Upscale Lifestyle",
    commercialDifferentiator: "Versus Radisson by Choice upscale full-service, RED sells lifestyle design intensity; versus Quality midscale, it is an upscale lifestyle ladder.",
    scenarios: [
      { title: "Upscale Lifestyle Urban", body: "Urban markets where lifestyle design and Privileges clear upscale ADR." },
      { title: "Conversion Into Lifestyle Upscale", body: "Conversions that can fund Radisson RED design standards." },
      { title: "Lifestyle Versus Classic Radisson", body: "Owners comparing RED lifestyle versus Radisson full-service." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Radisson RED core is upscale lifestyle urban design with Privileges." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears lifestyle standards." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss RED lifestyle intensity." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not RED's core lane." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is limited versus classic Radisson." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside upscale lifestyle." },
    ]),
    loyaltyProof: CHOICE_PROOF("Radisson RED"),
    feeNote: `Radisson RED application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice upscale lifestyle brand (Radisson RED) with bold design and Choice Privileges. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / brand disclosures—do not underwrite from launch media alone.",
    insight: "Radisson RED fits owners funding upscale lifestyle with Choice Privileges; fails when classic Radisson or midscale fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Radisson RED standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Radisson RED ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Radisson RED affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Radisson RED." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Radisson RED", "https://www.choicehotels.com/radisson-red"),
  }),

  recywbx1YQSTCPqW1: pack({
    brandId: "recywbx1YQSTCPqW1",
    brandName: "Radisson by Choice",
    liveBrandName: "Radisson by Choice",
    slug: "radisson-by-choice",
    parentCompany: "Choice Hotels",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice Hotels FDD / development exhibits for Radisson (upscale full-service)",
    sources: {
      brandPage: "https://www.choicehotels.com/radisson",
    },
    positioning: "Upscale Full-Service",
    commercialDifferentiator: "Versus Radisson RED lifestyle, classic Radisson sells upscale full-service; versus Quality midscale, it is a higher full-service ladder.",
    scenarios: [
      { title: "Upscale Full-Service Urban", body: "Urban and suburban sites needing Radisson full-service and Privileges." },
      { title: "Meetings-Capable Upscale", body: "Markets where meetings ADR clears upscale full-service IC." },
      { title: "Conversion Into Radisson Upscale", body: "Conversions that can fund upscale full-service PIP." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Radisson core includes upscale full-service business/leisure with Privileges." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Meetings-capable plant is a Radisson strength when present." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport upscale hubs can fit Radisson." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds upscale full-service." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Pure lifestyle design is more RED than classic Radisson." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Radisson upscale full-service." },
    ]),
    loyaltyProof: CHOICE_PROOF("Radisson"),
    feeNote: `Radisson application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice upscale full-service Radisson brand with meetings capability and Choice Privileges. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / brand disclosures—do not underwrite from launch media alone.",
    insight: "Radisson by Choice fits owners underwriting upscale full-service with Privileges; fails when RED lifestyle or midscale fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Radisson standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Radisson ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Radisson affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Radisson." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Radisson", "https://www.choicehotels.com/radisson"),
  }),

  recwl5JOYxlChuCAr: pack({
    brandId: "recwl5JOYxlChuCAr",
    brandName: "Preferred Hotels & Resorts",
    liveBrandName: "Preferred Hotels & Resorts",
    slug: "preferred-hotels-and-resorts",
    parentCompany: "Preferred Hotels & Resorts",
    loyaltyProgram: "Preferred Hotels & Resorts membership network (not a franchise loyalty program)",
    feeGeography: "Confirm current Preferred Hotels & Resorts membership, representation, and commercial exhibits — not a franchise FDD table",
    sources: {
      brandPage: "https://preferredhotels.com/",
    },
    positioning: "Independent Luxury Hotel Network",
    commercialDifferentiator: "Versus SLH luxury independent collection, Preferred is a broader independent luxury representation network; versus parent soft brands (Curio / Autograph), Preferred preserves owner control without a parent hard-brand prototype.",
    scenarios: [
      { title: "Independent Luxury Representation", body: "Luxury independents needing global representation without a franchise flag." },
      { title: "Owner-Control Diligence", body: "Owners comparing Preferred membership versus soft-brand parent collections." },
      { title: "Network Distribution Without Franchise", body: "Assets that need GDS/sales reach while keeping independent identity." },
    ],
    demandScenarios: demand([
      { title: "Luxury Urban Flagship", fit: S, explanation: "Preferred fits independent luxury urban assets that clear network quality bars." },
      { title: "Resort & Destination Leisure", fit: S, explanation: "Luxury resorts can fit when membership standards and demand align." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Adaptive reuse and independent repositioning can fit when quality clears Preferred." },
      { title: "Urban Lifestyle & Design", fit: M, explanation: "Design-led independents can fit the network when QA holds." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually sit outside Preferred luxury independent." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient is not a Preferred use case." },
    ]),
    loyaltyProof: MEMBERSHIP_NETWORK_PROOF("Preferred"),
    feeNote: `Preferred Hotels & Resorts membership, representation, marketing, and distribution assessments — confirm current membership exhibits. This is not a traditional franchise FDD royalty table; do not invent franchise percentages.`,
    operatingModel: "Independent luxury hotel representation / membership network — not a traditional franchise hard brand. Diligence membership fees, standards, and distribution without inventing franchise royalty percentages.",
    footprint: "Confirm live hotel counts from current Preferred Hotels & Resorts / brand disclosures—do not underwrite from launch media alone.",
    insight: "Preferred fits owners seeking independent luxury representation without franchise prototype economics; fails when soft-brand royalty tables are invented or membership is treated as a hard franchise.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Preferred standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Preferred Hotels & Resorts membership network (not a franchise loyalty program) delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Preferred ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Preferred affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Preferred." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Preferred", "https://preferredhotels.com/"),
  }),

  recjjSnY2opb8P4DG: pack({
    brandId: "recjjSnY2opb8P4DG",
    brandName: "Small Luxury Hotels of the World",
    liveBrandName: "Small Luxury Hotels of the World",
    slug: "small-luxury-hotels-of-the-world",
    parentCompany: "Small Luxury Hotels of the World",
    loyaltyProgram: "SLH membership network (not a franchise loyalty program)",
    feeGeography: "Confirm current SLH membership and commercial exhibits — not a franchise FDD table",
    sources: {
      brandPage: "https://www.slh.com/",
    },
    positioning: "Luxury Independent Collection",
    commercialDifferentiator: "Versus Preferred broader representation network, SLH is a curated small-luxury membership collection; versus parent soft brands, SLH is membership—not a franchise soft brand.",
    scenarios: [
      { title: "Small Luxury Membership", body: "Boutique luxury assets that clear SLH curation standards." },
      { title: "Independent Luxury Diligence", body: "Owners comparing SLH membership versus Preferred and soft-brand collections." },
      { title: "Owner Control With Luxury Halo", body: "Assets needing luxury independent halo without franchise prototype." },
    ],
    demandScenarios: demand([
      { title: "Luxury Urban Flagship", fit: S, explanation: "SLH fits curated small luxury urban hotels that clear membership standards." },
      { title: "Resort & Destination Leisure", fit: S, explanation: "Boutique luxury resorts are a core SLH lane when curation holds." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Repositionings can fit only when true small-luxury product clears SLH." },
      { title: "Urban Lifestyle & Design", fit: M, explanation: "Design-led boutique luxury can fit when QA and scale match SLH." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics sit outside SLH small luxury." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient is not an SLH use case." },
    ]),
    loyaltyProof: MEMBERSHIP_NETWORK_PROOF("SLH"),
    feeNote: `Small Luxury Hotels of the World membership, marketing, and distribution assessments — confirm current membership exhibits. This is not a traditional franchise FDD royalty table; do not invent franchise percentages or parent-brand prototype fees.`,
    operatingModel: "Independent luxury membership collection — curated small luxury hotels. Diligence membership standards and fees; never invent franchise royalty percentages or parent-brand prototype economics.",
    footprint: "Confirm live hotel counts from current Small Luxury Hotels of the World / brand disclosures—do not underwrite from launch media alone.",
    insight: "SLH fits owners of curated small luxury independents seeking membership distribution; fails when franchise soft-brand fees are invented or parent-brand language is used.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear SLH standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm SLH membership network (not a franchise loyalty program) delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where SLH ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where SLH affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for SLH." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("SLH", "https://www.slh.com/"),
  }),

});

export function listBatch10() {
  return BATCH_10_IDS.map((id) => BATCH_10_PACKS_BY_ID[id]).filter(Boolean);
}

export function getBatch10Pack(brandId) {
  return BATCH_10_PACKS_BY_ID[brandId] || null;
}
