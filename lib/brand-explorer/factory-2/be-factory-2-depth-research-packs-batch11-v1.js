/**
 * Factory 2.0 Batch 11 — Part A five EXISTING Active (original inventory remainder)
 * + Part B fifteen Choice/Wyndham/Hyatt/Sonesta candidates.
 * NEW_BRAND_CANDIDATE packs use provisional staging keys (NEW_<slug>) — never invent Airtable rec IDs.
 * Franchise fee amounts never invented — FDD / disclosure confirmation only.
 * Demand Scenario Fit: brand-specific six (not mechanical repeats).
 */
import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";
import {
  CHOICE_PROOF,
  WYNDHAM_PROOF,
  HYATT_PROOF,
  SONESTA_PROOF,
} from "./be-factory-2-depth-loyalty-proofs-v1.js";

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    batchTag: "batch-11",
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

export const FACTORY_2_BATCH_11_V1 = "FACTORY_2_BATCH_11_V1";

const FDD =
  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";

const S = DEMAND_FIT_LABELS.STRONG_FIT;
const M = DEMAND_FIT_LABELS.MODERATE_FIT;
const L = DEMAND_FIT_LABELS.LIMITED_FIT;
const N = DEMAND_FIT_LABELS.NOT_A_FIT;

export const BATCH_11_IDS = Object.freeze([
  "recWPEvxBQxVVzSq3", // Radisson Blu by Choice
  "recOzH5iAE1xEjyD0", // Comfort Inn & Suites
  "recRyvM8OmLlDj9G7", // Radisson Individuals by Choice
  "recaayt9u7YYg8h7Y", // Country Inn & Suites by Choice
  "recsOd51NzRPYsMko", // WoodSpring Suites
  "recAT3p3d7LAZZNkb", // Cambria Hotels
  "recq3vCeOn8lQdY4P", // Clarion Pointe
  "recsbxr7pMGMN0N6R", // Clarion
  "recWpSnsKxoUTcWs2", // Wyndham Grand
  "rec4w2XsL3yDUsv2C", // Dolce Hotels & Resorts by Wyndham
  "recXIwHYrvJ89a1yK", // TRYP by Wyndham
  "recE5LoJH1EaszkqG", // Esplendor by Wyndham
  "rec72kWHpqp5awuZY", // Registry Collection Hotels
  "recSgIQ0bhRpPXZbU", // Caption by Hyatt
  "NEW_hyatt-studios", // Hyatt Studios
  "rec4oSoJsqHtGfFYL", // JdV by Hyatt
  "recDQwcMFontK2CSP", // The Unbound Collection by Hyatt
  "NEW_hyatt-select", // Hyatt Select
  "recmK2PWDTHW6Y6tx", // Sonesta Simply Suites
  "NEW_sonesta-essential", // Sonesta Essential Hotels
]);

export const BATCH_11_PACKS_BY_ID = Object.freeze({
  "recWPEvxBQxVVzSq3": pack({
    brandId: "recWPEvxBQxVVzSq3",
    brandName: "Radisson Blu by Choice",
    liveBrandName: "Radisson Blu by Choice",
    slug: "radisson-blu-by-choice",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD / development exhibits for Radisson Blu (upscale full-service)",
    sources: {
      brandPage: "https://www.choicehotels.com/radisson-blu",
    },
    positioning: "Upscale Full-Service",
    commercialDifferentiator: "Versus Radisson by Choice classic upscale, Blu sells a more design-forward upscale full-service lane; versus Radisson RED lifestyle, Blu keeps fuller service and meetings capability inside Choice Privileges.",
    scenarios: [
      { title: "Upscale Full-Service Urban", body: "Gateway and secondary cities where Blu product and Privileges clear upscale ADR." },
      { title: "Meetings-Capable Upscale", body: "Assets with credible meetings plant that can fund Blu standards." },
      { title: "Conversion Into Radisson Blu", body: "Conversions that can deliver upscale public space without RED-only lifestyle underwriting." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Radisson Blu fits upscale business/leisure mix with Choice Privileges when product clears Blu standards." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Meetings-capable plant is a Blu strength versus RED lifestyle." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport upscale hubs can fit when service intensity matches Blu." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds upscale full-service—not midscale paint." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Pure lifestyle design intensity is more RED than Blu." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Radisson Blu upscale full-service." },
    ]),
    loyaltyProof: CHOICE_PROOF("Radisson Blu"),
    feeNote: `Radisson Blu application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice upscale full-service Radisson Blu with meetings capability and Choice Privileges. Confirm franchise vs management path from current exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / Radisson Blu disclosures—do not underwrite from launch media alone.",
    insight: "Radisson Blu fits owners funding upscale full-service with Privileges; fails when RED lifestyle or midscale Comfort economics are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Radisson Blu standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Radisson Blu ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Radisson Blu affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Radisson Blu." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Radisson Blu", "https://www.choicehotels.com/radisson-blu"),
  }),

  "recOzH5iAE1xEjyD0": pack({
    brandId: "recOzH5iAE1xEjyD0",
    brandName: "Comfort Inn & Suites",
    liveBrandName: "Comfort Inn & Suites",
    slug: "comfort-inn-suites",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD for Comfort Inn & Suites (midscale limited-service / suites mix)",
    sources: {
      brandPage: "https://www.choicehotels.com/comfort-inn",
    },
    positioning: "Midscale Limited-Service",
    commercialDifferentiator: "Versus Quality Inn midscale, Comfort Inn & Suites is the Choice midscale workhorse with suite-capable product; versus Cambria upscale, it keeps leaner capital and breakfast-led limited-service economics.",
    scenarios: [
      { title: "Corridor Midscale Limited-Service", body: "Highway and suburban corridors where Comfort product and Privileges compete on value." },
      { title: "Airport & Gateway Value", body: "Airport-adjacent value stays needing Choice distribution without upscale capital." },
      { title: "Conversion Into Comfort Midscale", body: "Conversions that can fund Comfort standards and breakfast delivery." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Comfort Inn & Suites is built for corridor midscale transient with Privileges." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban midweek/weekend mix is a core Comfort use case." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport-edge value midscale fits when product clears Comfort standards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale rooms and breakfast—not economy-only scopes." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "True extended-stay kitchens are WoodSpring / Suburban territory, not Comfort suites marketing alone." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Comfort midscale limited-service." },
    ]),
    loyaltyProof: CHOICE_PROOF("Comfort Inn & Suites"),
    feeNote: `Comfort Inn & Suites application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice midscale limited-service with breakfast and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels disclosures—Comfort is a scaled midscale flag.",
    insight: "Comfort Inn & Suites fits efficient midscale limited-service with Privileges; wrong when sponsors underwrite Cambria upscale or WoodSpring extended-stay economics.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Comfort Inn & Suites standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Comfort Inn & Suites ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Comfort Inn & Suites affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Comfort Inn & Suites." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Comfort Inn & Suites", "https://www.choicehotels.com/comfort-inn"),
  }),

  "recRyvM8OmLlDj9G7": pack({
    brandId: "recRyvM8OmLlDj9G7",
    brandName: "Radisson Individuals by Choice",
    liveBrandName: "Radisson Individuals by Choice",
    slug: "radisson-individuals-by-choice",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels soft-collection / affiliation exhibits for Radisson Individuals",
    sources: {
      brandPage: "https://www.choicehotels.com/radisson-individuals",
    },
    positioning: "Upscale Independent Collection",
    commercialDifferentiator: "Versus Radisson Blu hard upscale brand, Individuals sells independent character inside Choice; versus Preferred / SLH membership networks, it rides Choice Privileges rather than pure membership distribution.",
    scenarios: [
      { title: "Independent Character Conversion", body: "Distinctive upscale independents needing Privileges without hard Radisson prototyping." },
      { title: "Soft-Collection Diligence", body: "Owners comparing Individuals versus Blu / RED hard brands and other soft lanes." },
      { title: "Heritage & Adaptive Reuse", body: "Heritage assets where individuality is the product and Choice systems expand distribution." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Radisson Individuals is built for character conversions that keep independent story with Privileges." },
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Design-led independents fit when collection standards clear." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix can work when product clears soft-collection standards—not economy capital." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is usually a hard Radisson Blu / Radisson lane." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype midscale/upscale is not the soft-collection lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside upscale independent collection." },
    ]),
    loyaltyProof: CHOICE_PROOF("Radisson Individuals"),
    feeNote: `Radisson Individuals application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice soft / independent collection — distinctive upscale conversions with Choice Privileges. Do not underwrite as a hard Radisson Blu prototype franchise.",
    footprint: "Confirm live hotel counts from current Choice / Radisson Individuals disclosures—curated versus mass Choice midscale flags.",
    insight: "Radisson Individuals fits owners who need Choice soft-collection distribution for distinctive upscale assets; fails when Blu hard-brand fees or invented royalty percentages are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Radisson Individuals standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Radisson Individuals ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Radisson Individuals affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Radisson Individuals." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Radisson Individuals", "https://www.choicehotels.com/radisson-individuals"),
  }),

  "recaayt9u7YYg8h7Y": pack({
    brandId: "recaayt9u7YYg8h7Y",
    brandName: "Country Inn & Suites by Choice",
    liveBrandName: "Country Inn & Suites by Choice",
    slug: "country-inn-suites-by-choice",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD for Country Inn & Suites (midscale limited-service)",
    sources: {
      brandPage: "https://www.choicehotels.com/country-inn",
    },
    positioning: "Midscale Limited-Service",
    commercialDifferentiator: "Versus Comfort Inn & Suites, Country Inn leans warmer residential midscale cues; versus Clarion full-service midscale, it keeps limited-service capital inside Choice Privileges.",
    scenarios: [
      { title: "Suburban Midscale Limited-Service", body: "Suburban corridors where Country Inn product and Privileges clear midscale ADR." },
      { title: "Family & Leisure Midscale", body: "Markets where residential midscale cues support leisure mix." },
      { title: "Conversion Into Country Inn", body: "Conversions that can fund Country Inn standards without Clarion full-service capital." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Country Inn core is suburban midscale business/leisure with Privileges." },
      { title: "Highway & Interstate Transient", fit: S, explanation: "Corridor midscale transient fits when product clears Country Inn standards." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets fit when Privileges and midscale product defend ADR." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale—not economy-only scopes." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings plant is more Clarion / Radisson territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Country Inn midscale." },
    ]),
    loyaltyProof: CHOICE_PROOF("Country Inn & Suites"),
    feeNote: `Country Inn & Suites application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice midscale limited-service with breakfast and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels disclosures.",
    insight: "Country Inn & Suites fits residential midscale limited-service with Privileges; wrong when Clarion full-service or Cambria upscale capital is assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Country Inn & Suites standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Country Inn & Suites ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Country Inn & Suites affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Country Inn & Suites." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Country Inn & Suites", "https://www.choicehotels.com/country-inn"),
  }),

  "recsOd51NzRPYsMko": pack({
    brandId: "recsOd51NzRPYsMko",
    brandName: "WoodSpring Suites",
    liveBrandName: "WoodSpring Suites",
    slug: "woodspring-suites",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD for WoodSpring Suites (economy / midscale extended-stay)",
    sources: {
      brandPage: "https://www.choicehotels.com/woodspring-suites",
    },
    positioning: "Economy Extended-Stay",
    commercialDifferentiator: "Versus Suburban Studios / Everhome midscale extended-stay, WoodSpring sells leaner economy extended-stay; versus Comfort transient midscale, it is length-of-stay kitchenette economics inside Choice Privileges.",
    scenarios: [
      { title: "Economy Extended-Stay Corporate", body: "Markets with multi-week demand needing kitchenettes at economy capital." },
      { title: "Project / Workforce Longer Stay", body: "Project and workforce corridors needing value extended-stay." },
      { title: "Conversion Into WoodSpring Suites", body: "Conversions that can fund suite/kitchen PIP at WoodSpring standards." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "WoodSpring core is economy extended-stay with kitchens and Privileges." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce weekly stays are a primary WoodSpring pocket." },
      { title: "University & Medical", fit: M, explanation: "Medical/university longer stays can fit when rates clear economy extended-stay IC." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban midweek extended-stay works; pure weekend leisure is secondary." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is a weak fit versus Comfort / Quality." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside WoodSpring economy extended-stay." },
    ]),
    loyaltyProof: CHOICE_PROOF("WoodSpring"),
    feeNote: `WoodSpring application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice economy extended-stay with kitchenettes and Choice Privileges. Housekeeping cadence differs from transient midscale—confirm FDD exhibits.",
    footprint: "Confirm live hotel counts from current Choice Hotels / WoodSpring disclosures.",
    insight: "WoodSpring fits sponsors with genuine extended-stay demand and lean kitchenette capital; fails when underwritten like Comfort transient without suite PIP.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear WoodSpring standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where WoodSpring ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where WoodSpring affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for WoodSpring." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("WoodSpring", "https://www.choicehotels.com/woodspring-suites"),
  }),

  "recAT3p3d7LAZZNkb": pack({
    brandId: "recAT3p3d7LAZZNkb",
    brandName: "Cambria Hotels",
    liveBrandName: "Cambria Hotels",
    slug: "cambria-hotels",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD for Cambria Hotels (upscale select-service)",
    sources: {
      brandPage: "https://www.choicehotels.com/cambria",
    },
    positioning: "Upscale Select-Service",
    commercialDifferentiator: "Versus Comfort midscale limited-service, Cambria sells upscale select-service design and F&B lite; versus Radisson Blu full-service, it keeps tighter capital inside Choice Privileges.",
    scenarios: [
      { title: "Urban Upscale Select-Service", body: "Urban and mixed-use sites where Cambria design and Privileges lift select-service rates." },
      { title: "Airport & Gateway Upscale Select", body: "Airport-adjacent assets needing upscale select-service without full-service capital." },
      { title: "Conversion Into Cambria", body: "Conversions that can fund Cambria public-space and room standards." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Cambria fits upscale select-service business/leisure with Privileges." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport-edge upscale select-service is a common Cambria use case." },
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Design-led urban select-service is a Cambria strength." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds upscale select-service—not midscale Comfort scopes." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is limited versus full-service Radisson Blu." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Hard highway midscale transient sits below Cambria upscale select-service." },
    ]),
    loyaltyProof: CHOICE_PROOF("Cambria"),
    feeNote: `Cambria application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice upscale select-service with design-forward public space and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / Cambria disclosures.",
    insight: "Cambria fits owners funding upscale select-service with Privileges; wrong when Comfort midscale capital or Radisson full-service fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Cambria standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Cambria ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Cambria affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Cambria." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Cambria", "https://www.choicehotels.com/cambria"),
  }),

  "recq3vCeOn8lQdY4P": pack({
    brandId: "recq3vCeOn8lQdY4P",
    brandName: "Clarion Pointe",
    liveBrandName: "Clarion Pointe",
    slug: "clarion-pointe",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD for Clarion Pointe (midscale limited-service)",
    sources: {
      brandPage: "https://www.choicehotels.com/clarion-pointe",
    },
    positioning: "Midscale Limited-Service",
    commercialDifferentiator: "Versus Clarion full-service midscale, Clarion Pointe is the leaner limited-service Clarion ladder; versus Comfort, it is Clarion-branded midscale limited-service inside Privileges.",
    scenarios: [
      { title: "Corridor Midscale Limited-Service", body: "Highway and suburban corridors needing Clarion Pointe product and Privileges." },
      { title: "Conversion Into Clarion Pointe", body: "Conversions that can fund Pointe standards without Clarion full-service capital." },
      { title: "Sibling Clarity Versus Clarion", body: "Owners comparing Pointe limited-service versus Clarion full-service." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Clarion Pointe is built for corridor midscale limited-service transient." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban midweek/weekend mix fits Pointe when product clears standards." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets fit when Privileges defend midscale ADR." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale limited-service." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Meetings plant is Clarion full-service territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Clarion Pointe." },
    ]),
    loyaltyProof: CHOICE_PROOF("Clarion Pointe"),
    feeNote: `Clarion Pointe application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice midscale limited-service Clarion Pointe with breakfast and Choice Privileges. Do not import Clarion full-service fee or staffing assumptions.",
    footprint: "Confirm live hotel counts from current Choice Hotels disclosures.",
    insight: "Clarion Pointe fits lean midscale limited-service with Privileges; fails when Clarion full-service meetings economics are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Clarion Pointe standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Clarion Pointe ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Clarion Pointe affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Clarion Pointe." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Clarion Pointe", "https://www.choicehotels.com/clarion-pointe"),
  }),

  "recsbxr7pMGMN0N6R": pack({
    brandId: "recsbxr7pMGMN0N6R",
    brandName: "Clarion",
    liveBrandName: "Clarion",
    slug: "clarion",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Choice Hotels FDD for Clarion (midscale full-service / meetings-capable)",
    sources: {
      brandPage: "https://www.choicehotels.com/clarion",
    },
    positioning: "Midscale Full-Service",
    commercialDifferentiator: "Versus Clarion Pointe limited-service, Clarion sells midscale full-service and meetings; versus Radisson Blu upscale, it is a lower full-service ladder inside Choice Privileges.",
    scenarios: [
      { title: "Midscale Full-Service Meetings", body: "Secondary cities where meetings ADR clears Clarion full-service IC." },
      { title: "Suburban Full-Service Utility", body: "Suburban assets needing F&B and meetings without upscale Radisson capital." },
      { title: "Conversion Into Clarion Full-Service", body: "Conversions that can fund Clarion public-space and meetings PIP." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Clarion is built for midscale meetings and full-service utility with Privileges." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban full-service mix is a core Clarion use case." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport midscale full-service hubs can fit Clarion." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds full-service—not Pointe limited-service scopes." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Hard limited-service highway transient is Pointe / Comfort territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Clarion midscale full-service." },
    ]),
    loyaltyProof: CHOICE_PROOF("Clarion"),
    feeNote: `Clarion application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Choice midscale full-service with meetings capability and Choice Privileges. Confirm FDD exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Choice Hotels / Clarion disclosures.",
    insight: "Clarion fits midscale full-service meetings assets with Privileges; wrong when Clarion Pointe limited-service capital is assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Clarion standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Choice Privileges delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Clarion ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Clarion affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Clarion." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Clarion", "https://www.choicehotels.com/clarion"),
  }),

  "recWpSnsKxoUTcWs2": pack({
    brandId: "recWpSnsKxoUTcWs2",
    brandName: "Wyndham Grand",
    liveBrandName: "Wyndham Grand",
    slug: "wyndham-grand",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Wyndham franchise / development disclosure for Wyndham Grand (upscale full-service)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/wyndham-grand",
    },
    positioning: "Upscale Full-Service",
    commercialDifferentiator: "Versus Trademark soft-brand collection, Wyndham Grand is a hard upscale full-service brand; versus Dolce meetings-intensive, Grand sells broader upscale full-service with Wyndham Rewards.",
    scenarios: [
      { title: "Upscale Full-Service Urban", body: "Gateway and secondary cities where Grand product and Rewards clear upscale ADR." },
      { title: "Meetings-Capable Upscale", body: "Assets with meetings plant that can fund Wyndham Grand standards." },
      { title: "Conversion Into Wyndham Grand", body: "Conversions that can deliver upscale full-service PIP." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Wyndham Grand fits upscale business/leisure with Rewards when product clears Grand standards." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Meetings-capable plant is a Grand strength." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport upscale hubs can fit Wyndham Grand." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds upscale full-service." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Pure lifestyle design is more TRYP / Dazzler territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Wyndham Grand." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Wyndham Grand"),
    feeNote: `Wyndham Grand application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Wyndham upscale full-service with meetings capability and Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham Hotels disclosures.",
    insight: "Wyndham Grand fits owners funding upscale full-service with Rewards; fails when Trademark soft-brand or midscale fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Wyndham Grand standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Wyndham Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Wyndham Grand ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Wyndham Grand affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Wyndham Grand." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Wyndham Grand", "https://www.wyndhamhotels.com/wyndham-grand"),
  }),

  "rec4w2XsL3yDUsv2C": pack({
    brandId: "rec4w2XsL3yDUsv2C",
    brandName: "Dolce Hotels & Resorts by Wyndham",
    liveBrandName: "Dolce Hotels and Resorts by Wyndham",
    slug: "dolce-hotels-resorts-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Wyndham franchise / development disclosure for Dolce (meetings-intensive upscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/dolce",
    },
    positioning: "Meetings Intensive Upscale",
    commercialDifferentiator: "Versus Wyndham Grand broader upscale full-service, Dolce sells meetings- and conference-intensive product; versus TRYP lifestyle, it is group-compression economics inside Wyndham Rewards.",
    scenarios: [
      { title: "Conference & Meetings Destination", body: "Sites where conference plant and Rewards underwrite Dolce contribution." },
      { title: "Corporate Campus Meetings", body: "Campus-adjacent assets needing meetings intensity without luxury capital." },
      { title: "Conversion Into Dolce Meetings", body: "Conversions that can fund meetings plant and Dolce standards." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Dolce is built for meetings and conference compression with Wyndham Rewards." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix works when meetings plant is real—not hotel-as-rooms-only." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge conference hotels can fit Dolce." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds meetings capacity." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Lifestyle design intensity is TRYP / Dazzler territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside Dolce meetings-intensive upscale." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Dolce"),
    feeNote: `Dolce application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Wyndham meetings-intensive upscale / conference hotel brand with Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham / Dolce disclosures—portfolio is meetings-curated versus mass midscale flags.",
    insight: "Dolce fits sponsors with genuine meetings plant and Rewards demand; fails when underwritten like TRYP lifestyle or limited-service transient.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Dolce standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Wyndham Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Dolce ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Dolce affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Dolce." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Dolce", "https://www.wyndhamhotels.com/dolce"),
  }),

  "recXIwHYrvJ89a1yK": pack({
    brandId: "recXIwHYrvJ89a1yK",
    brandName: "TRYP by Wyndham",
    liveBrandName: "TRYP by Wyndham",
    slug: "tryp-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Wyndham franchise / development disclosure for TRYP (upscale lifestyle)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/tryp",
    },
    positioning: "Upscale Lifestyle",
    commercialDifferentiator: "Versus Wyndham Grand full-service, TRYP sells urban lifestyle design; versus Dazzler / Esplendor regional lifestyle, TRYP is the global Wyndham lifestyle node with Rewards.",
    scenarios: [
      { title: "Urban Lifestyle Upscale", body: "City sites where lifestyle design and Rewards lift TRYP rates." },
      { title: "Gateway Lifestyle Conversion", body: "Conversions that can fund TRYP public-space and design standards." },
      { title: "Sibling Clarity Versus Grand / Dolce", body: "Owners comparing lifestyle TRYP versus Grand full-service or Dolce meetings." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "TRYP core is upscale urban lifestyle with Wyndham Rewards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears lifestyle standards." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss TRYP lifestyle intensity." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not TRYP's core lane." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is Dolce / Grand territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside upscale lifestyle." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("TRYP"),
    feeNote: `TRYP application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Wyndham upscale lifestyle brand with design-forward public space and Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham / TRYP disclosures across Americas, EMEA, and APAC.",
    insight: "TRYP fits owners funding urban lifestyle with Rewards; fails when Grand full-service or midscale fees are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear TRYP standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Wyndham Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where TRYP ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where TRYP affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for TRYP." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("TRYP", "https://www.wyndhamhotels.com/tryp"),
  }),

  "recE5LoJH1EaszkqG": pack({
    brandId: "recE5LoJH1EaszkqG",
    brandName: "Esplendor by Wyndham",
    liveBrandName: "Esplendor by Wyndham",
    slug: "esplendor-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Wyndham franchise / development disclosure for Esplendor (LATAM / CALA upscale lifestyle)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/esplendor",
    },
    positioning: "Upscale Lifestyle LATAM",
    commercialDifferentiator: "Versus TRYP global lifestyle, Esplendor is the LATAM / CALA upscale lifestyle node; versus Dazzler, confirm sibling clarity on regional lifestyle positioning inside Wyndham Rewards.",
    scenarios: [
      { title: "LATAM Urban Lifestyle", body: "CALA / LATAM urban markets where Esplendor design and Rewards clear upscale ADR." },
      { title: "Gateway Lifestyle Conversion", body: "Conversions that can fund Esplendor standards in regional gateways." },
      { title: "Sibling Clarity Versus TRYP / Dazzler", body: "Owners comparing Esplendor versus TRYP and Dazzler lifestyle lanes." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Esplendor core is LATAM / CALA upscale lifestyle with Wyndham Rewards." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Regional gateways in CALA / LATAM fit when product clears Esplendor standards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design PIP clears lifestyle standards." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss Esplendor lifestyle intensity." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is Dolce / Grand territory." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside upscale lifestyle." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Esplendor"),
    feeNote: `Esplendor application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Wyndham upscale lifestyle brand focused on LATAM / CALA with Wyndham Rewards. Confirm franchise exhibits—do not invent royalty percentages.",
    footprint: "Confirm live hotel counts from current Wyndham / Esplendor disclosures—primarily CALA / LATAM.",
    insight: "Esplendor fits LATAM / CALA lifestyle assets with Rewards; fails when North American midscale or Dolce meetings economics are assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Esplendor standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Wyndham Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Esplendor ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Esplendor affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Esplendor." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Esplendor", "https://www.wyndhamhotels.com/esplendor"),
  }),

  "rec72kWHpqp5awuZY": pack({
    brandId: "rec72kWHpqp5awuZY",
    brandName: "Registry Collection Hotels",
    liveBrandName: "Registry Collection Hotels",
    slug: "registry-collection-hotels",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Wyndham soft-collection / affiliation exhibits for Registry Collection",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/registry-collection",
    },
    positioning: "Upscale Independent Collection",
    commercialDifferentiator: "Versus Trademark soft-brand collection, Registry Collection sits higher on upscale/luxury-adjacent independent character; versus Wyndham Grand hard brand, it preserves individuality with Wyndham Rewards.",
    scenarios: [
      { title: "Independent Upscale Conversion", body: "Distinctive upscale independents needing Rewards without hard Grand prototyping." },
      { title: "Soft-Collection Diligence", body: "Owners comparing Registry versus Trademark and hard Wyndham brands." },
      { title: "Destination Character Hotels", body: "Destination assets where individuality drives rate with Rewards distribution." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Registry Collection is built for distinctive upscale conversions with Rewards." },
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Design-led independents fit when collection standards clear." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select destination resorts can work when individuality is credible." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually sit below Registry collection." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not the soft-collection lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside upscale independent collection." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Registry Collection"),
    feeNote: `Registry Collection application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Wyndham soft / independent collection — distinctive upscale conversions with Wyndham Rewards. Do not underwrite as a hard Wyndham Grand prototype franchise.",
    footprint: "Confirm live hotel counts from current Wyndham / Registry Collection disclosures—curated versus mass midscale flags.",
    insight: "Registry Collection fits owners needing Wyndham soft-collection distribution for distinctive upscale assets; fails when Grand hard-brand fees are invented.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Registry Collection standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Wyndham Rewards delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Registry Collection ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Registry Collection affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Registry Collection." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Registry Collection", "https://www.wyndhamhotels.com/registry-collection"),
  }),

  "recSgIQ0bhRpPXZbU": pack({
    brandId: "recSgIQ0bhRpPXZbU",
    brandName: "Caption by Hyatt",
    liveBrandName: "Caption by Hyatt",
    slug: "caption-by-hyatt",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Hyatt franchise / development disclosure for Caption by Hyatt (select-service lifestyle)",
    sources: {
      brandPage: "https://www.hyatt.com/caption",
    },
    positioning: "Select-Service Lifestyle",
    commercialDifferentiator: "Versus Hyatt Place conventional select-service, Caption sells lifestyle-forward select-service; versus Hyatt Centric explorer lifestyle, Caption keeps leaner select-service capital inside World of Hyatt.",
    scenarios: [
      { title: "Urban Select-Service Lifestyle", body: "City and mixed-use sites where Caption lifestyle cues lift select-service rates with World of Hyatt." },
      { title: "Conversion Into Caption", body: "Conversions that can fund Caption public-space standards without Centric full lifestyle capital." },
      { title: "Sibling Clarity Versus Hyatt Place", body: "Owners comparing Caption lifestyle select-service versus Hyatt Place utility." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "Caption core is lifestyle-forward select-service with World of Hyatt." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban mix can work when lifestyle product clears Caption standards." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge lifestyle select-service can fit Caption." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds lifestyle select-service—not Place-only scopes." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is Hyatt Regency / Grand Hyatt territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Caption select-service lifestyle." },
    ]),
    loyaltyProof: HYATT_PROOF("Caption"),
    feeNote: `Caption application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hyatt select-service lifestyle with leaner staffing than full-service Hyatt brands and World of Hyatt systems. Do not import Park Hyatt / Andaz luxury economics.",
    footprint: "Confirm live hotel counts from current Hyatt / Caption disclosures—growth brand; do not underwrite from launch media alone.",
    insight: "Caption fits owners wanting lifestyle select-service with World of Hyatt; wrong when Hyatt Place utility capital or Centric explorer fees are blindly assumed.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Caption standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm World of Hyatt delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Caption ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Caption affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Caption." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Caption", "https://www.hyatt.com/caption"),
  }),

  "NEW_hyatt-studios": pack({
    brandId: "NEW_hyatt-studios",
    brandName: "Hyatt Studios",
    liveBrandName: "Hyatt Studios",
    slug: "hyatt-studios",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeography: "Confirm current Hyatt franchise / development disclosure for Hyatt Studios (extended-stay) AFTER Brand Basics create",
    sources: {
      brandPage: "https://www.hyatt.com/",
    },
    positioning: "Midscale Extended-Stay",
    commercialDifferentiator: "Versus Hyatt House upper-midscale extended-stay, Hyatt Studios is the leaner extended-stay ladder; versus Hyatt Place transient select-service, it is length-of-stay kitchenette economics inside World of Hyatt.",
    scenarios: [
      { title: "Extended-Stay Corporate", body: "Markets with multi-week demand needing kitchenettes at midscale capital with World of Hyatt." },
      { title: "Project / Medical Longer Stay", body: "Project and medical corridors needing suite product." },
      { title: "New-Build Extended-Stay Prototype", body: "New-builds optimized for Hyatt Studios prototypes—confirm disclosure after Basics create." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Hyatt Studios thesis is extended-stay with kitchens and World of Hyatt (confirm live brand standards)." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce weekly stays are a primary extended-stay pocket." },
      { title: "University & Medical", fit: M, explanation: "Medical/university longer stays can fit suite kitchenette product." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban midweek extended-stay works; pure weekend leisure is secondary." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is Hyatt Place territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Hyatt Studios extended-stay." },
    ]),
    loyaltyProof: HYATT_PROOF("Hyatt Studios"),
    feeNote: `Hyatt Studios application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hyatt midscale extended-stay (staged NEW_BRAND_CANDIDATE — no Brand Basics ID yet). Confirm franchise path and Travel Pass / World of Hyatt delivery after authorized Basics create.",
    footprint: "Confirm live hotel counts from current Hyatt disclosures after Brand Basics create—do not invent inventory from media alone.",
    insight: "Hyatt Studios is staged as NEW_BRAND_CANDIDATE: research pack ready; Brand Basics create requires separate founder authorization—no invented rec ID.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Hyatt Studios standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm World of Hyatt delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Hyatt Studios ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Hyatt Studios affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Hyatt Studios." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Hyatt Studios", "https://www.hyatt.com/"),
  }),

  "rec4oSoJsqHtGfFYL": pack({
    brandId: "rec4oSoJsqHtGfFYL",
    brandName: "JdV by Hyatt",
    liveBrandName: "Joie de Vivre Hotels",
    slug: "jdv-by-hyatt",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Hyatt / JdV affiliation disclosure for Joie de Vivre (independent lifestyle collection)",
    sources: {
      brandPage: "https://www.jdvhotels.com/",
    },
    positioning: "Independent Lifestyle Collection",
    commercialDifferentiator: "Versus The Unbound Collection luxury soft collection, JdV sells independent lifestyle character at upper-upscale; versus Caption select-service lifestyle, JdV is fuller independent lifestyle with World of Hyatt.",
    scenarios: [
      { title: "Independent Lifestyle Conversion", body: "Distinctive lifestyle independents needing World of Hyatt without hard Hyatt prototyping." },
      { title: "Urban Character Hotels", body: "Urban assets where individuality and place narrative drive rate." },
      { title: "Sibling Clarity Versus Unbound / Centric", body: "Owners comparing JdV versus Unbound Collection and Hyatt Centric." },
    ],
    demandScenarios: demand([
      { title: "Urban Lifestyle & Design", fit: S, explanation: "JdV core is independent lifestyle character with World of Hyatt." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Conversions fit when individuality and Hyatt systems both clear IC." },
      { title: "Resort & Destination Leisure", fit: M, explanation: "Select destination lifestyle hotels can fit JdV." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics usually miss JdV lifestyle intensity." },
      { title: "Airport & Transportation", fit: L, explanation: "Airport hard-prototype is not the independent lifestyle lane." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient sits outside JdV independent lifestyle." },
    ]),
    loyaltyProof: HYATT_PROOF("JdV"),
    feeNote: `JdV application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hyatt independent lifestyle collection (live Brand Basics name: Joie de Vivre Hotels). Soft-collection diligence—not hard-brand prototype economics. World of Hyatt participation.",
    footprint: "Confirm live hotel counts from current Hyatt / JdV disclosures.",
    insight: "JdV fits distinctive lifestyle independents needing World of Hyatt; fails when Caption select-service or Unbound luxury fees are blindly assumed. Preserve live name Joie de Vivre Hotels.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear JdV standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm World of Hyatt delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where JdV ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where JdV affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for JdV." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("JdV", "https://www.jdvhotels.com/"),
  }),

  "recDQwcMFontK2CSP": pack({
    brandId: "recDQwcMFontK2CSP",
    brandName: "The Unbound Collection by Hyatt",
    liveBrandName: "Unbound Collection by Hyatt",
    slug: "unbound-collection-by-hyatt",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Hyatt soft-collection / affiliation exhibits for The Unbound Collection (luxury)",
    sources: {
      brandPage: "https://www.hyatt.com/unbound-collection",
    },
    positioning: "Luxury Independent Collection",
    commercialDifferentiator: "Versus Park Hyatt intimate hard luxury, Unbound sells soft-collection individuality; versus JdV lifestyle collection, Unbound sits higher on luxury intensity inside World of Hyatt.",
    scenarios: [
      { title: "Independent Luxury Character", body: "Assets with strong design authorship needing World of Hyatt without Park Hyatt prototyping." },
      { title: "Destination Luxury Soft Collection", body: "Destination luxury hotels where individuality is the product." },
      { title: "Affiliation Into Unbound", body: "Affiliations that can meet Unbound luxury standards while preserving character." },
    ],
    demandScenarios: demand([
      { title: "Luxury Urban Flagship", fit: S, explanation: "Unbound fits curated luxury independents that clear soft-collection standards with World of Hyatt." },
      { title: "Resort & Destination Leisure", fit: S, explanation: "Destination luxury resorts are a core Unbound lane when curation holds." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Repositionings fit only when true luxury product clears Unbound." },
      { title: "Urban Lifestyle & Design", fit: M, explanation: "Design-led luxury lifestyle can fit when QA matches Unbound." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban select-service economics sit outside Unbound luxury." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway transient is not an Unbound use case." },
    ]),
    loyaltyProof: HYATT_PROOF("Unbound Collection"),
    feeNote: `Unbound Collection application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Hyatt luxury soft collection — property individuality with World of Hyatt. Operating and design capital remain luxury-band—do not confuse with Caption select-service.",
    footprint: "Confirm live hotel counts from current Hyatt / Unbound disclosures—intentionally curated.",
    insight: "Unbound works when individuality and luxury delivery are both real; fails when sponsors want World of Hyatt without luxury capital. Preserve live name Unbound Collection by Hyatt.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Unbound Collection standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm World of Hyatt delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Unbound Collection ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Unbound Collection affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Unbound Collection." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Unbound Collection", "https://www.hyatt.com/unbound-collection"),
  }),

  "NEW_hyatt-select": pack({
    brandId: "NEW_hyatt-select",
    brandName: "Hyatt Select",
    liveBrandName: "Hyatt Select",
    slug: "hyatt-select",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeography: "Confirm current Hyatt franchise / development disclosure for Hyatt Select AFTER Brand Basics create",
    sources: {
      brandPage: "https://www.hyatt.com/",
    },
    positioning: "Select-Service",
    commercialDifferentiator: "Versus Hyatt Place upper-midscale select-service, Hyatt Select (if launched as a distinct ladder rung) needs verified positioning versus Place / Caption before IC; stage research only—do not invent fee tables.",
    scenarios: [
      { title: "Select-Service Diligence Gate", body: "Sites where sponsors must confirm Hyatt Select versus Hyatt Place / Caption before assuming a distinct owner use case." },
      { title: "Corridor Select-Service", body: "Corridor assets needing World of Hyatt—validate brand standards after Basics create." },
      { title: "Conversion Into Hyatt Select", body: "Conversions only after authorized Brand Basics create and current disclosure exhibits." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: M, explanation: "Provisional: suburban select-service may fit if Hyatt Select standards clear—confirm after Basics create." },
      { title: "Airport & Transportation", fit: M, explanation: "Provisional: airport-edge select-service may fit—confirm live brand standards." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions gated on authorized Basics create and disclosure exhibits." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Highway midscale may overlap Place / other Hyatt select-service—confirm differentiation." },
      { title: "Urban Lifestyle & Design", fit: L, explanation: "Lifestyle intensity may be Caption territory—confirm sibling clarity." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside select-service." },
    ]),
    loyaltyProof: HYATT_PROOF("Hyatt Select"),
    feeNote: `Hyatt Select application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics match for Hyatt Select. Do not invent rec ID. Research pack only; franchise path and World of Hyatt delivery confirmed after authorized create.",
    footprint: "No Airtable inventory yet. Confirm existence and counts from Hyatt disclosures before underwriting.",
    insight: "Hyatt Select is NEW_BRAND_CANDIDATE with weak public inventory signal—founder must authorize Basics create only after brand existence and ladder position are verified versus Hyatt Place / Caption.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Hyatt Select standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm World of Hyatt delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Hyatt Select ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Hyatt Select affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Hyatt Select." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Hyatt Select", "https://www.hyatt.com/"),
  }),

  "recmK2PWDTHW6Y6tx": pack({
    brandId: "recmK2PWDTHW6Y6tx",
    brandName: "Sonesta Simply Suites",
    liveBrandName: "Sonesta Simply Suites",
    slug: "sonesta-simply-suites",
    parentCompany: "Sonesta International Hotels Corporation",
    loyaltyProgram: "Sonesta Travel Pass",
    researchClass: "EXISTING_CANONICAL",
    feeGeography: "Confirm current Sonesta franchise / development disclosure for Sonesta Simply Suites (economy / midscale extended-stay)",
    sources: {
      brandPage: "https://www.sonesta.com/sonesta-simply-suites",
    },
    positioning: "Economy Extended-Stay",
    commercialDifferentiator: "Versus Sonesta ES Suites midscale extended-stay, Simply Suites sells leaner economy extended-stay; versus Sonesta Select transient, it is length-of-stay kitchenette economics inside Sonesta Travel Pass.",
    scenarios: [
      { title: "Economy Extended-Stay Corporate", body: "Markets with multi-week demand needing kitchenettes at economy capital." },
      { title: "Project / Workforce Longer Stay", body: "Project and workforce corridors needing value extended-stay." },
      { title: "Conversion Into Simply Suites", body: "Conversions that can fund suite/kitchen PIP at Simply Suites standards." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Simply Suites core is economy extended-stay with kitchens and Travel Pass." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce weekly stays are a primary Simply Suites pocket." },
      { title: "University & Medical", fit: M, explanation: "Medical/university longer stays can fit when rates clear economy extended-stay IC." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban midweek extended-stay works; pure weekend leisure is secondary." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is Select / Red Lion territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Simply Suites economy extended-stay." },
    ]),
    loyaltyProof: SONESTA_PROOF("Sonesta Simply Suites"),
    feeNote: `Sonesta Simply Suites application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Sonesta economy extended-stay with kitchenettes and Sonesta Travel Pass. Housekeeping cadence differs from transient select-service—confirm disclosure exhibits.",
    footprint: "Confirm live hotel counts from current Sonesta disclosures (Brand Basics status: Draft).",
    insight: "Sonesta Simply Suites fits lean extended-stay demand with Travel Pass; fails when underwritten like Sonesta Select transient or ES Suites midscale without kitchen PIP.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Sonesta Simply Suites standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Sonesta Travel Pass delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Sonesta Simply Suites ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Sonesta Simply Suites affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Sonesta Simply Suites." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Sonesta Simply Suites", "https://www.sonesta.com/sonesta-simply-suites"),
  }),

  "NEW_sonesta-essential": pack({
    brandId: "NEW_sonesta-essential",
    brandName: "Sonesta Essential Hotels",
    liveBrandName: "Sonesta Essential Hotels",
    slug: "sonesta-essential-hotels",
    parentCompany: "Sonesta International Hotels Corporation",
    loyaltyProgram: "Sonesta Travel Pass",
    researchClass: "NEW_BRAND_CANDIDATE",
    feeGeography: "Confirm current Sonesta franchise / development disclosure for Sonesta Essential AFTER Brand Basics create",
    sources: {
      brandPage: "https://www.sonesta.com/",
    },
    positioning: "Essential Midscale",
    commercialDifferentiator: "Versus Sonesta Select upper-midscale select-service, Essential (if a distinct ladder rung) needs verified positioning versus Select / Red Lion before IC; stage research only—do not invent fee tables.",
    scenarios: [
      { title: "Essential Midscale Diligence Gate", body: "Sites where sponsors must confirm Essential versus Select / Red Lion before assuming a distinct owner use case." },
      { title: "Corridor Essential Midscale", body: "Corridor assets needing Travel Pass—validate brand standards after Basics create." },
      { title: "Conversion Into Sonesta Essential", body: "Conversions only after authorized Brand Basics create and current disclosure exhibits." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: M, explanation: "Provisional: corridor essential midscale may fit—confirm after Basics create." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Provisional: suburban mix may fit—confirm live brand standards." },
      { title: "Airport & Transportation", fit: M, explanation: "Provisional: airport-edge value stays may fit." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions gated on authorized Basics create and disclosure exhibits." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay is Simply Suites / ES Suites territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside essential midscale." },
    ]),
    loyaltyProof: SONESTA_PROOF("Sonesta Essential"),
    feeNote: `Sonesta Essential application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Staged NEW_BRAND_CANDIDATE — no live Brand Basics match for Sonesta Essential Hotels. Do not invent rec ID. Research pack only; franchise path and Travel Pass delivery confirmed after authorized create.",
    footprint: "No Airtable inventory yet. Confirm existence and counts from Sonesta disclosures before underwriting.",
    insight: "Sonesta Essential Hotels is NEW_BRAND_CANDIDATE—founder must authorize Basics create only after brand existence and ladder position are verified versus Sonesta Select / Red Lion.",
    standards: [
      { title: "Brand Product Standards", body: "Rooms and public space must clear Sonesta Essential standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP or membership quality scope must match the brand promise—not under-scoped capital." },
      { title: "Loyalty / Network Integration", body: "Confirm Sonesta Travel Pass delivery obligations for this brand before IC." },
    ],
    valueOwnerScenarios: [
      { title: "Primary Owner Corridor", body: "Sites where Sonesta Essential ADR clears IC versus local comps." },
      { title: "Conversion / Membership Path", body: "Assets where Sonesta Essential affiliation clears IC versus sibling alternatives." },
      { title: "Disclosure Fee Gate", body: "Model only from current disclosure / membership exhibits for Sonesta Essential." },
      { title: "Sibling Clarity", body: "Diligence versus sibling flags / networks before IC." },
    ],
    gallery: gallery("Sonesta Essential", "https://www.sonesta.com/"),
  }),

});

export function listBatch11() {
  return BATCH_11_IDS.map((id) => BATCH_11_PACKS_BY_ID[id]).filter(Boolean);
}

export function getBatch11Pack(brandId) {
  return BATCH_11_PACKS_BY_ID[brandId] || null;
}
