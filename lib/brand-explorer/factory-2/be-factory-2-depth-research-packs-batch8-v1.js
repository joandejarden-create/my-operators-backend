/**
 * Factory 2.0 Batch 8 — economy / midscale / extended-stay depth packs
 * (Wyndham + BWH + Sonesta). All IDs are live EXISTING_CANONICAL Brand Basics.
 * Franchise fee amounts never invented — FDD / disclosure confirmation only.
 * Demand Scenario Fit: brand-specific six (not mechanical repeats).
 */
import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";
import { WYNDHAM_PROOF, BWH_PROOF, SONESTA_PROOF } from "./be-factory-2-depth-loyalty-proofs-v1.js";

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    batchTag: "batch-8",
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
    { title: `${brand} — Breakfast / Amenity Cue`, body: "Breakfast or amenity cues where required by standards", source: url },
    { title: `${brand} — Corridor / Efficiency`, body: "Operating efficiency cues typical of this product", source: url },
    { title: `${brand} — Parking / Context`, body: "Highway, suburban, or urban context where typical", source: url },
  ];
}

export const FACTORY_2_BATCH_8_V1 = "FACTORY_2_BATCH_8_V1";

const FDD =
  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";

const S = DEMAND_FIT_LABELS.STRONG_FIT;
const M = DEMAND_FIT_LABELS.MODERATE_FIT;
const L = DEMAND_FIT_LABELS.LIMITED_FIT;
const N = DEMAND_FIT_LABELS.NOT_A_FIT;

export const BATCH_8_IDS = Object.freeze([
  "recRHZlHTXBkVySUC", // La Quinta
  "recvCgmyT7vBpJmcE", // AmericInn
  "recrMPYs4pOsA6Nys", // Howard Johnson
  "reczvWzz6ugk5IzFc", // Wingate
  "rec0wIg8ie5ZfPY1f", // Ramada
  "recyggm1Bif6Mk0a2", // Hawthorn
  "recTDqjhN6rXWoC07", // Wyndham Garden
  "rec5RhdBK4iNCQF4u", // Best Western
  "rec5KPgalPPAFl7UZ", // Best Western Plus
  "rec9ViCcmQBA55vGA", // SureStay
  "recpmv6vrM9ztGedL", // SureStay Plus
  "recTsfe9nlEMqLE3j", // SureStay Studio
  "recLOEXjnKYZdxuEb", // GLō
  "rectk6HIVra7oyoDP", // Executive Residency
  "recPpLc3gOsPOwtCJ", // Aiden
  "rec4ztolqpD1uYrsG", // Sonesta Select
  "recEaNmmaNdbWvipS", // Sonesta ES Suites
  "rechMdsQxPPhet6rV", // Red Lion
  "recgYprNavH0rgb7z", // Signature Inn
  "recgMqPkLsfaMU9vA", // Americas Best Value Inn
]);

export const BATCH_8_PACKS_BY_ID = Object.freeze({
  recRHZlHTXBkVySUC: pack({
    brandId: "recRHZlHTXBkVySUC",
    brandName: "La Quinta by Wyndham",
    liveBrandName: "La Quinta by Wyndham",
    slug: "la-quinta-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham FDD for La Quinta (upper-midscale limited-service)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/laquinta",
      development: "https://www.wyndhamdevelopment.com/",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Upper-midscale limited-service: brighter rooms, breakfast, and Wyndham Rewards for travelers seeking consistency above hard midscale without full-service capital.",
    commercialDifferentiator:
      "Versus Days Inn / Super 8 economy, La Quinta sells a fuller upper-midscale limited-service stack; versus Wingate, it is more breakfast-led transient than business-suite positioning—model from current FDD.",
    scenarios: [
      { title: "Suburban Upper-Midscale Limited-Service", body: "Suburban corridors where breakfast and Rewards support rates above economy peers." },
      { title: "Airport & Highway Value", body: "Airport and interstate sites needing brand distribution without full-service F&B capital." },
      { title: "Conversion Into Upper-Midscale", body: "Conversions that can fund La Quinta PIP without jumping to full-service." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "La Quinta is built for interstate and corridor transient with breakfast and Rewards—strong when local ADR clears upper-midscale limited-service fees." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport edge sites fit when product and Rewards defend share versus other upper-midscale limited-service flags." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban business/leisure mix is a core La Quinta use case—underwrite breakfast and Rewards contribution locally." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work when PIP funds upper-midscale guest rooms and breakfast—not economy paint-and-carpet alone." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets work when ADR supports La Quinta fees; diligence versus Baymont / Hampton-class comps." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship economics sit outside La Quinta's limited-service product and fee structure." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("La Quinta"),
    feeNote: `La Quinta application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Upper-midscale limited-service with breakfast and leaner staffing than full-service. Confirm housekeeping and breakfast requirements in current Wyndham standards.",
    footprint:
      "Broad North American La Quinta footprint with selective international presence. Confirm live counts from Wyndham disclosures.",
    insight:
      "La Quinta fits owners who want upper-midscale consistency with Wyndham Rewards; wrong when capital only supports hard economy or when guests expect full-service without funding it.",
    standards: [
      { title: "Upper-Midscale Limited-Service Product", body: "Rooms, breakfast, and public space must clear La Quinta standards." },
      { title: "Conversion / PIP Discipline", body: "PIP scope must match upper-midscale—not economy-only capital." },
      { title: "Wyndham Rewards Integration", body: "Rewards enrollment and Elite delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Corridor Upper-Midscale", body: "Sites where La Quinta rates clear IC above economy comps." },
      { title: "Airport Limited-Service", body: "Airport edge assets needing Rewards without full-service capital." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD / agreement exhibits." },
      { title: "Sibling Clarity Versus Wingate / Baymont", body: "Diligence versus Wingate and Baymont before IC." },
    ],
    gallery: gallery("La Quinta", "https://www.wyndhamhotels.com/laquinta"),
  }),

  recvCgmyT7vBpJmcE: pack({
    brandId: "recvCgmyT7vBpJmcE",
    brandName: "AmericInn by Wyndham",
    liveBrandName: "AmericInn by Wyndham",
    slug: "americinn-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham FDD for AmericInn (midscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/americinn",
      development: "https://www.wyndhamdevelopment.com/",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Midscale: indoor pool / lodge cues and Wyndham Rewards for travelers seeking a warmer midscale experience than hard economy.",
    commercialDifferentiator:
      "Versus Super 8 hard economy, AmericInn sells midscale amenities (often indoor pool); versus La Quinta upper-midscale, confirm local ADR and PIP intensity from current FDD.",
    scenarios: [
      { title: "Secondary-Market Midscale Lodge", body: "Secondary markets where pool/lodge cues and Rewards support midscale rates." },
      { title: "Highway Family & Leisure Transient", body: "Highway leisure demand that values indoor pool and brand consistency." },
      { title: "Conversion Into Midscale", body: "Conversions that can fund AmericInn amenity standards." },
    ],
    demandScenarios: demand([
      { title: "Regional & Secondary Markets", fit: S, explanation: "AmericInn is strongest in secondary and regional markets where lodge/pool cues clear midscale ADR." },
      { title: "Highway & Interstate Transient", fit: S, explanation: "Highway leisure/family transient fits when amenities and Rewards defend rates versus economy peers." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix works when midscale product matches local comps—confirm breakfast and pool operating costs." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale amenities—not economy-only scopes." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not AmericInn's core product—consider Hawthorn or SureStay Studio instead." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside AmericInn's midscale lodge positioning." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("AmericInn"),
    feeNote: `AmericInn application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Midscale limited-service with amenity intensity (often indoor pool) above hard economy. Budget pool labor and utilities into ops.",
    footprint:
      "Primarily North American midscale footprint. Confirm live counts from Wyndham disclosures.",
    insight:
      "AmericInn fits owners who can fund midscale lodge amenities with Wyndham Rewards; fails when capital only supports Super 8 economics or when guests expect La Quinta upper-midscale without funding it.",
    standards: [
      { title: "Midscale Amenity Delivery", body: "Pool/lodge and room standards must clear AmericInn expectations." },
      { title: "Operating Cost Honesty", body: "Underwrite amenity labor—do not import hard-economy staffing." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Secondary Midscale Lodge", body: "Markets where pool/lodge cues clear midscale IC." },
      { title: "Highway Family Demand", body: "Corridors with leisure family mix." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Super 8 / La Quinta", body: "Diligence versus Super 8 and La Quinta before IC." },
    ],
    gallery: gallery("AmericInn", "https://www.wyndhamhotels.com/americinn"),
  }),

  recrMPYs4pOsA6Nys: pack({
    brandId: "recrMPYs4pOsA6Nys",
    brandName: "Howard Johnson by Wyndham",
    liveBrandName: "Howard Johnson by Wyndham",
    slug: "howard-johnson-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham FDD for Howard Johnson (economy / midscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/hojo",
      development: "https://www.wyndhamdevelopment.com/",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Economy / midscale value: familiar brand awareness and Wyndham Rewards for price-sensitive transient demand.",
    commercialDifferentiator:
      "Versus Super 8, HoJo often reads as slightly fuller economy/midscale value; versus Ramada, confirm meetings and midscale expectations from current FDD—not heritage marketing.",
    scenarios: [
      { title: "Highway Value Transient", body: "Highway sites where awareness and Rewards matter more than upper-midscale amenities." },
      { title: "Secondary-Market Economy", body: "Secondary markets needing brand distribution at economy capital." },
      { title: "Conversion Cost Discipline", body: "Conversions that stay capital-efficient versus midscale peers." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "HoJo's core demand is highway and interstate value transient with Rewards distribution." },
      { title: "Economy Value-Oriented Transient", fit: S, explanation: "Price-sensitive economy transient fits when product and fees clear local economy comps." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets work when ADR supports HoJo fees without midscale overbuilding." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work with economy/midscale PIP discipline—avoid upper-midscale capital assumptions." },
      { title: "Meetings & Group Intensive", fit: L, explanation: "Meetings-intensive assets usually need Ramada or fuller midscale product—not classic HoJo." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is not a Howard Johnson use case." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Howard Johnson"),
    feeNote: `Howard Johnson application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Economy / midscale limited-service with lean staffing. Confirm breakfast and amenity requirements in current standards.",
    footprint:
      "Legacy North American and selective international HoJo footprint. Confirm live counts from Wyndham disclosures.",
    insight:
      "Howard Johnson fits owners wanting familiar economy/midscale awareness with Wyndham Rewards; wrong when local comps require Ramada meetings product or La Quinta upper-midscale without funding it.",
    standards: [
      { title: "Value Product Basics", body: "Clean rooms and safety basics must clear HoJo standards." },
      { title: "Capital Efficiency", body: "PIP should stay economy/midscale-scope." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Highway Value Flag", body: "Corridors competing on rate with Rewards." },
      { title: "Secondary Economy", body: "Secondary markets with economy ADR." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Super 8 / Ramada", body: "Diligence versus Super 8 and Ramada before IC." },
    ],
    gallery: gallery("Howard Johnson", "https://www.wyndhamhotels.com/hojo"),
  }),

  reczvWzz6ugk5IzFc: pack({
    brandId: "reczvWzz6ugk5IzFc",
    brandName: "Wingate by Wyndham",
    liveBrandName: "Wingate by Wyndham",
    slug: "wingate-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham FDD for Wingate (upper-midscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/wingate",
      development: "https://www.wyndhamdevelopment.com/",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Upper-midscale: business-oriented rooms, breakfast, and Wyndham Rewards for corporate and road-warrior transient.",
    commercialDifferentiator:
      "Versus La Quinta breakfast-led upper-midscale, Wingate leans business-traveler product cues; versus full-service Ramada Plaza, it stays limited-service capital—confirm FDD fees.",
    scenarios: [
      { title: "Suburban Business Transient", body: "Suburban office-park and medical corridors needing upper-midscale business product." },
      { title: "Airport Business Edge", body: "Airport edge sites with corporate midweek mix." },
      { title: "Conversion Into Business Upper-Midscale", body: "Conversions that can fund Wingate business-room standards." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Wingate is strongest for suburban business transient with Rewards and breakfast." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport business edge fits when product clears upper-midscale business expectations." },
      { title: "Corporate-Led Urban", fit: M, explanation: "Corporate-led urban can work when account ladders accept Wingate—confirm negotiated rates." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions need business-room PIP—not economy shells with a logo." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure highway leisure/economy transient is a weaker fit than La Quinta / Days Inn corridors." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "True extended-stay is Hawthorn / SureStay Studio territory—not Wingate's primary product." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Wingate"),
    feeNote: `Wingate application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Upper-midscale limited-service with business-oriented rooms and breakfast. Staffing above hard economy; below full-service.",
    footprint:
      "North American upper-midscale footprint. Confirm live counts from Wyndham disclosures.",
    insight:
      "Wingate fits owners targeting business upper-midscale with Wyndham Rewards; wrong when demand is pure highway economy or when full-service meetings capital is required.",
    standards: [
      { title: "Business Room Product", body: "Rooms and work-friendly cues must clear Wingate standards." },
      { title: "Breakfast & Amenity Delivery", body: "Confirm breakfast and amenity requirements in current standards." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Suburban Business Upper-Midscale", body: "Office-park corridors where Wingate rates clear IC." },
      { title: "Airport Business Edge", body: "Airport midweek corporate mix." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus La Quinta / Hawthorn", body: "Diligence versus La Quinta and Hawthorn before IC." },
    ],
    gallery: gallery("Wingate", "https://www.wyndhamhotels.com/wingate"),
  }),

  rec0wIg8ie5ZfPY1f: pack({
    brandId: "rec0wIg8ie5ZfPY1f",
    brandName: "Ramada by Wyndham",
    liveBrandName: "Ramada by Wyndham",
    slug: "ramada-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham FDD for Ramada (midscale / upper-midscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/ramada",
      development: "https://www.wyndhamdevelopment.com/",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Midscale to upper-midscale: broader public space and selective meetings cues with Wyndham Rewards for travelers seeking more than limited-service economy.",
    commercialDifferentiator:
      "Versus Days Inn / HoJo economy, Ramada often carries fuller public space; versus Wyndham Garden, confirm service level and F&B intensity from current brand standards and FDD.",
    scenarios: [
      { title: "Secondary Midscale With Meetings Cues", body: "Secondary markets needing limited meetings capacity without full-service luxury capital." },
      { title: "Conversion Into Midscale Public Space", body: "Conversions that can fund lobby/meetings upgrades." },
      { title: "Regional Transient & SMERF", body: "Regional transient and small group demand with Rewards." },
    ],
    demandScenarios: demand([
      { title: "Regional & Secondary Markets", fit: S, explanation: "Ramada fits secondary midscale markets with modest meetings and Rewards distribution." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban business/leisure mix works when public space clears midscale expectations." },
      { title: "Meetings & Group Intensive", fit: M, explanation: "Small meetings/SMERF can fit; convention-intensive assets usually need fuller upper-upscale product." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions need funded public-space PIP—cosmetic reflag fails." },
      { title: "Highway & Interstate Transient", fit: M, explanation: "Highway transient can work but pure economy corridors may prefer Days Inn / Super 8 economics." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside Ramada's midscale positioning." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Ramada"),
    feeNote: `Ramada application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Midscale / upper-midscale with selective meetings and fuller public space than hard limited-service. Confirm F&B and banquet labor where offered.",
    footprint:
      "Global Ramada footprint with North American concentration. Confirm live counts from Wyndham disclosures.",
    insight:
      "Ramada fits owners who can fund midscale public space with Wyndham Rewards; wrong when capital only supports economy limited-service or when guests expect Wyndham Garden upper-midscale without funding it.",
    standards: [
      { title: "Public Space Standards", body: "Lobby and meetings cues must clear Ramada expectations where promised." },
      { title: "Service Level Honesty", body: "Do not underwrite as limited-service economy if the brand path requires fuller midscale ops." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Secondary Midscale", body: "Markets needing midscale public space and Rewards." },
      { title: "Small Meetings Contribution", body: "Assets with modest SMERF capacity." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Days Inn / Wyndham Garden", body: "Diligence versus Days Inn and Wyndham Garden before IC." },
    ],
    gallery: gallery("Ramada", "https://www.wyndhamhotels.com/ramada"),
  }),

  recyggm1Bif6Mk0a2: pack({
    brandId: "recyggm1Bif6Mk0a2",
    brandName: "Hawthorn Extended Stay by Wyndham",
    liveBrandName: "Hawthorn Suites by Wyndham",
    slug: "hawthorn-extended-stay-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham FDD for Hawthorn (extended-stay)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/hawthorn-extended-stay",
      development: "https://www.wyndhamdevelopment.com/",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Midscale extended-stay: suites with kitchens and Wyndham Rewards for project, corporate, and longer-stay demand.",
    commercialDifferentiator:
      "Versus transient La Quinta / Wingate, Hawthorn is length-of-stay economics; versus SureStay Studio, it rides Wyndham Rewards rather than Best Western Rewards—model from current FDD.",
    scenarios: [
      { title: "Corporate & Project Extended-Stay", body: "Markets with multi-week corporate and project demand." },
      { title: "Medical / University Longer Stay", body: "Medical and campus corridors needing kitchenettes." },
      { title: "Conversion Into Extended-Stay", body: "Conversions that can fund suite/kitchen PIP." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Hawthorn's core thesis is corporate and project extended-stay with kitchens and Rewards." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce multi-week stays fit when suite product and weekly rates clear IC." },
      { title: "University & Medical", fit: S, explanation: "Medical/university longer stays are a natural Hawthorn demand pocket." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban midweek extended-stay works; pure weekend leisure transient is secondary." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure one-night highway transient is a weak fit versus Days Inn / Super 8." },
      { title: "Resort & Coastal Leisure", fit: N, explanation: "Resort leisure destination economics are outside Hawthorn extended-stay product." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Hawthorn"),
    feeNote: `Hawthorn application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Extended-stay with housekeeping cadence and kitchenette maintenance distinct from transient limited-service. Underwrite labor for longer stays.",
    footprint:
      "North American extended-stay footprint. Confirm live counts from Wyndham disclosures.",
    insight:
      "Hawthorn fits sponsors with genuine extended-stay demand and suite capital; fails when underwritten like transient midscale without kitchen PIP or weekly-rate realism.",
    standards: [
      { title: "Suite / Kitchen Product", body: "Kitchenettes and suite layouts must clear Hawthorn standards." },
      { title: "Extended-Stay Ops Cadence", body: "Housekeeping and maintenance rhythms differ from transient flags." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Corporate Extended-Stay", body: "Markets with multi-week corporate demand." },
      { title: "Project / Workforce Suites", body: "Project corridors needing weekly rates." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Transient Wyndham Flags", body: "Diligence versus La Quinta / Wingate before assuming interchangeable use cases." },
    ],
    gallery: gallery("Hawthorn", "https://www.wyndhamhotels.com/hawthorn-extended-stay"),
  }),

  recTDqjhN6rXWoC07: pack({
    brandId: "recTDqjhN6rXWoC07",
    brandName: "Wyndham Garden",
    liveBrandName: "Wyndham Garden",
    slug: "wyndham-garden",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham FDD for Wyndham Garden (upper-midscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/wyndham-garden",
      development: "https://www.wyndhamdevelopment.com/",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Upper-midscale: fuller service cues and Wyndham Rewards for travelers seeking more polished midscale product than limited-service economy.",
    commercialDifferentiator:
      "Versus Ramada midscale, Wyndham Garden typically reads higher on service/polish; versus full-service Wyndham Hotels, it still sits below flagship full-service capital—confirm FDD and standards.",
    scenarios: [
      { title: "Suburban Upper-Midscale", body: "Suburban markets needing polished upper-midscale with Rewards." },
      { title: "Airport / Edge Upper-Midscale", body: "Airport edge sites with higher guest expectations than hard limited-service." },
      { title: "Conversion With Service Upgrade", body: "Conversions that fund service and public-space upgrades." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Wyndham Garden fits suburban upper-midscale business/leisure when product and fees clear comps." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport edge upper-midscale is a common Garden use case." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets work when ADR supports upper-midscale—not economy fee assumptions." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions need funded service/public-space upgrades." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure highway economy transient is a weaker fit than Days Inn / Super 8." },
      { title: "Economy Value-Oriented Transient", fit: N, explanation: "Hard economy value transient is outside Wyndham Garden's upper-midscale thesis." },
    ]),
    loyaltyProof: WYNDHAM_PROOF("Wyndham Garden"),
    feeNote: `Wyndham Garden application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Upper-midscale with higher staffing and amenity intensity than hard limited-service. Confirm F&B/service requirements in current standards.",
    footprint:
      "Global Wyndham Garden footprint. Confirm live counts from Wyndham disclosures.",
    insight:
      "Wyndham Garden fits owners who can fund upper-midscale service with Rewards; fails when underwritten like economy limited-service or when full-service Wyndham capital is actually required.",
    standards: [
      { title: "Upper-Midscale Service Delivery", body: "Service and public space must clear Garden standards." },
      { title: "PIP Honesty", body: "Conversion capital must match upper-midscale—not economy scopes." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Suburban Upper-Midscale", body: "Markets where Garden rates clear IC." },
      { title: "Airport Edge Polish", body: "Airport sites needing above limited-service product." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Ramada / Wyndham Hotels", body: "Diligence versus Ramada and Wyndham Hotels before IC." },
    ],
    gallery: gallery("Wyndham Garden", "https://www.wyndhamhotels.com/wyndham-garden"),
  }),

  // ——— BWH ———
  rec5RhdBK4iNCQF4u: pack({
    brandId: "rec5RhdBK4iNCQF4u",
    brandName: "Best Western",
    liveBrandName: "Best Western",
    slug: "best-western",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for Best Western (midscale)",
    sources: {
      brandPage: "https://www.bestwestern.com/",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Midscale: owner-operator network, breakfast cues, and Best Western Rewards for regional and highway travelers seeking familiar midscale consistency.",
    commercialDifferentiator:
      "Versus SureStay economy collection, Best Western is fuller midscale; versus Best Western Plus, it sits one rung lower—confirm fees and standards from current BWH disclosure.",
    scenarios: [
      { title: "Regional Midscale Highway", body: "Regional highway markets needing midscale brand awareness." },
      { title: "Secondary-Market Conversion", body: "Conversions into Best Western midscale standards." },
      { title: "Owner-Operator Midscale", body: "Owner-operators seeking BWH distribution and Rewards." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Best Western is a classic highway/regional midscale fit with Rewards distribution." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary midscale markets are core Best Western territory." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix works when product clears midscale—confirm breakfast and Rewards contribution." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Conversion is a primary Best Western growth path when PIP funds midscale standards." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay is SureStay Studio / Executive Residency territory—not core Best Western." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside Best Western midscale." },
    ]),
    loyaltyProof: BWH_PROOF("Best Western"),
    feeNote: `Best Western application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Midscale limited-service with owner-operator flexibility within brand standards. Confirm breakfast and housekeeping requirements.",
    footprint:
      "Global Best Western midscale footprint. Confirm live counts from BWH disclosures.",
    insight:
      "Best Western fits owner-operators who want midscale distribution with Rewards; wrong when capital only supports SureStay economy or when guests expect Plus polish without funding it.",
    standards: [
      { title: "Midscale Product Basics", body: "Rooms and breakfast must clear Best Western standards." },
      { title: "Conversion / PIP Discipline", body: "PIP must match midscale—not economy-only capital." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Regional Midscale Flag", body: "Highway/regional markets where Best Western rates clear IC." },
      { title: "Owner-Operator Conversion", body: "Conversions into BWH midscale standards." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus Plus / SureStay", body: "Diligence versus Best Western Plus and SureStay before IC." },
    ],
    gallery: gallery("Best Western", "https://www.bestwestern.com/"),
  }),

  rec5KPgalPPAFl7UZ: pack({
    brandId: "rec5KPgalPPAFl7UZ",
    brandName: "Best Western Plus",
    liveBrandName: "Best Western Plus",
    slug: "best-western-plus",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for Best Western Plus (upper-midscale)",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/best-western-plus.html",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Upper-midscale: elevated finishes and Best Western Rewards for travelers seeking more polish than core Best Western without lifestyle soft-brand capital.",
    commercialDifferentiator:
      "Versus Best Western core, Plus requires higher product capital; versus Aiden lifestyle, Plus stays conventional upper-midscale—confirm disclosure fees and PIP.",
    scenarios: [
      { title: "Suburban Upper-Midscale", body: "Suburban markets needing Plus polish with Rewards." },
      { title: "Airport Edge Upper-Midscale", body: "Airport edge sites with higher guest expectations." },
      { title: "Upgrade From Core Best Western", body: "Assets that can fund Plus standards versus remaining core Best Western." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Plus fits suburban upper-midscale business/leisure when ADR clears higher product capital." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport edge upper-midscale is a common Plus use case." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets work only when ADR supports Plus—not core Best Western fees alone." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions must fund Plus finishes—core Best Western PIP is insufficient." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure highway economy transient is weaker than core Best Western / SureStay." },
      { title: "Economy Value-Oriented Transient", fit: N, explanation: "Hard economy value transient is outside Plus upper-midscale." },
    ]),
    loyaltyProof: BWH_PROOF("Best Western Plus"),
    feeNote: `Best Western Plus application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Upper-midscale limited-service with higher finish and amenity intensity than core Best Western. Confirm standards before IC.",
    footprint:
      "Global Best Western Plus footprint. Confirm live counts from BWH disclosures.",
    insight:
      "Plus fits owners who can fund upper-midscale polish with Rewards; fails when underwritten like core Best Western or SureStay economy.",
    standards: [
      { title: "Plus Finish Standards", body: "Finishes and amenities must clear Plus—not core Best Western." },
      { title: "PIP Honesty", body: "Upgrade capital must be real before promising Plus." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Suburban Plus", body: "Markets where Plus rates clear IC." },
      { title: "Airport Edge Plus", body: "Airport sites needing upper-midscale polish." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus Best Western / Aiden", body: "Diligence versus core Best Western and Aiden before IC." },
    ],
    gallery: gallery("Best Western Plus", "https://www.bestwestern.com/en_US/hotels/best-western-plus.html"),
  }),

  rec9ViCcmQBA55vGA: pack({
    brandId: "rec9ViCcmQBA55vGA",
    brandName: "SureStay by Best Western",
    liveBrandName: "SureStay Collection by Best Western",
    slug: "surestay-by-best-western",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for SureStay (economy / midscale collection)",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/surestay.html",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Economy / midscale collection: conversion-friendly product with Best Western Rewards for owners seeking lower capital intensity than core Best Western.",
    commercialDifferentiator:
      "Versus Best Western midscale, SureStay is conversion-oriented and capital-lighter; versus SureStay Plus / Studio, confirm product rung from current BWH standards.",
    scenarios: [
      { title: "Economy Conversion Into BWH", body: "Conversions that need Rewards distribution without full Best Western midscale capital." },
      { title: "Highway Value Transient", body: "Highway sites competing on rate with BWH distribution." },
      { title: "Secondary-Market Economy", body: "Secondary markets with economy ADR." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "SureStay is designed for conversion-friendly economy/midscale entry into BWH Rewards." },
      { title: "Highway & Interstate Transient", fit: S, explanation: "Highway value transient is a core SureStay demand pocket." },
      { title: "Economy Value-Oriented Transient", fit: S, explanation: "Price-sensitive economy transient fits when product and fees clear local economy comps." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets work with economy/midscale ADR realism." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix can work; do not assume Best Western Plus polish." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside SureStay." },
    ]),
    loyaltyProof: BWH_PROOF("SureStay"),
    feeNote: `SureStay application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Economy / midscale collection with leaner capital and ops than core Best Western. Confirm collection standards for the specific SureStay path.",
    footprint:
      "Growing SureStay collection footprint. Confirm live counts from BWH disclosures.",
    insight:
      "SureStay fits conversion owners needing BWH Rewards at lower capital; fails when guests or lenders expect Best Western / Plus product without funding it.",
    standards: [
      { title: "Collection Product Basics", body: "Rooms and safety basics must clear SureStay collection standards." },
      { title: "Conversion Capital Discipline", body: "Keep PIP economy/midscale-scope." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Conversion Into BWH", body: "Economy shells entering Rewards via SureStay." },
      { title: "Highway Value", body: "Corridors competing on rate." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus Best Western / SureStay Plus", body: "Diligence versus Best Western and SureStay Plus before IC." },
    ],
    gallery: gallery("SureStay", "https://www.bestwestern.com/en_US/hotels/surestay.html"),
  }),

  recpmv6vrM9ztGedL: pack({
    brandId: "recpmv6vrM9ztGedL",
    brandName: "SureStay Plus by Best Western",
    liveBrandName: "SureStay Plus Hotel by Best Western",
    slug: "surestay-plus-by-best-western",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for SureStay Plus",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/surestay.html",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Upper collection rung within SureStay: more polish than core SureStay with Best Western Rewards, still conversion-oriented versus Best Western Plus.",
    commercialDifferentiator:
      "Versus SureStay core, Plus expects higher product; versus Best Western Plus, it remains a collection conversion path—confirm standards and fees in current disclosure.",
    scenarios: [
      { title: "Conversion With Extra Polish", body: "Conversions that can fund SureStay Plus finishes." },
      { title: "Suburban Value Upper Collection", body: "Suburban sites needing more than core SureStay." },
      { title: "Rewards Distribution Without Full Plus Capital", body: "Owners wanting Rewards without Best Western Plus prototypes." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "SureStay Plus targets conversions that can fund more polish than core SureStay." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban mix fits when product clears the Plus collection rung." },
      { title: "Highway & Interstate Transient", fit: M, explanation: "Highway transient works; pure hard economy may still prefer core SureStay." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets need ADR that supports Plus collection fees." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay is SureStay Studio territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside SureStay Plus." },
    ]),
    loyaltyProof: BWH_PROOF("SureStay Plus"),
    feeNote: `SureStay Plus application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Collection upper rung with higher finish than core SureStay; still leaner than Best Western Plus prototypes. Confirm current standards.",
    footprint:
      "SureStay Plus collection footprint. Confirm live counts from BWH disclosures.",
    insight:
      "SureStay Plus fits conversion owners who can fund more polish than core SureStay but not Best Western Plus capital; wrong when either under- or over-specified versus local ADR.",
    standards: [
      { title: "Plus Collection Finishes", body: "Finishes must clear SureStay Plus—not core SureStay alone." },
      { title: "Conversion Honesty", body: "Fund the Plus collection PIP before promising the rung." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Polished Conversion", body: "Shells that can fund SureStay Plus finishes." },
      { title: "Suburban Collection Upper Rung", body: "Suburban ADR that clears Plus collection fees." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus SureStay / Best Western Plus", body: "Diligence versus SureStay and Best Western Plus before IC." },
    ],
    gallery: gallery("SureStay Plus", "https://www.bestwestern.com/en_US/hotels/surestay.html"),
  }),

  recTsfe9nlEMqLE3j: pack({
    brandId: "recTsfe9nlEMqLE3j",
    brandName: "SureStay Studio by Best Western",
    liveBrandName: "SureStay Studio",
    slug: "surestay-studio-by-best-western",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for SureStay Studio (extended-stay)",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/surestay.html",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Affordable extended-stay: studio suites with kitchenettes and Best Western Rewards for longer-stay value demand.",
    commercialDifferentiator:
      "Versus transient SureStay, Studio is length-of-stay economics; versus Hawthorn, it rides Best Western Rewards rather than Wyndham Rewards—model from current disclosure.",
    scenarios: [
      { title: "Affordable Extended-Stay", body: "Markets with multi-week value demand needing kitchenettes." },
      { title: "Project / Medical Longer Stay", body: "Project and medical corridors with weekly rates." },
      { title: "Conversion Into Studio Suites", body: "Conversions that can fund studio/kitchen product." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "SureStay Studio's core thesis is affordable extended-stay with Rewards." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce weekly stays are a primary Studio demand pocket." },
      { title: "University & Medical", fit: S, explanation: "Medical/university longer stays fit studio kitchenette product." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work when kitchenette PIP is funded." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is weaker than SureStay transient collection." },
      { title: "Resort & Coastal Leisure", fit: N, explanation: "Resort leisure destination economics are outside Studio extended-stay." },
    ]),
    loyaltyProof: BWH_PROOF("SureStay Studio"),
    feeNote: `SureStay Studio application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Affordable extended-stay with kitchenette maintenance and housekeeping cadence distinct from transient flags.",
    footprint:
      "SureStay Studio footprint. Confirm live counts from BWH disclosures.",
    insight:
      "SureStay Studio fits sponsors with genuine longer-stay demand and studio capital; fails when underwritten like transient SureStay without kitchen PIP.",
    standards: [
      { title: "Studio / Kitchen Product", body: "Kitchenettes must clear Studio standards." },
      { title: "Extended-Stay Ops Cadence", body: "Housekeeping rhythms differ from transient." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Affordable Extended-Stay", body: "Weekly-rate markets needing studio suites." },
      { title: "Project / Medical Suites", body: "Project and medical corridors." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus SureStay Transient / Hawthorn", body: "Diligence versus SureStay and Hawthorn before IC." },
    ],
    gallery: gallery("SureStay Studio", "https://www.bestwestern.com/en_US/hotels/surestay.html"),
  }),

  recLOEXjnKYZdxuEb: pack({
    brandId: "recLOEXjnKYZdxuEb",
    brandName: "GLō by Best Western",
    liveBrandName: "Glō by Best Western",
    slug: "glo-by-best-western",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for GLō",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/glo.html",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Select-service lifestyle midscale: contemporary design cues and Best Western Rewards for travelers seeking a more modern midscale read.",
    commercialDifferentiator:
      "Versus core Best Western, GLō sells lifestyle design; versus Aiden, confirm lifestyle intensity and capital from current BWH standards—do not invent fee tables.",
    scenarios: [
      { title: "Suburban Lifestyle Midscale", body: "Suburban markets wanting contemporary midscale design with Rewards." },
      { title: "Conversion With Design Upgrade", body: "Conversions that can fund GLō design standards." },
      { title: "Younger Transient Mix", body: "Markets where design cues matter to capture midscale share." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "GLō fits suburban midscale lifestyle when design capital clears local ADR." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Design-forward conversion is a primary GLō path." },
      { title: "Gateway Urban", fit: M, explanation: "Gateway urban can work selectively; confirm versus Aiden lifestyle intensity." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets need ADR that funds lifestyle finishes." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure highway economy transient is a weaker fit than SureStay / Best Western core." },
      { title: "Economy Value-Oriented Transient", fit: N, explanation: "Hard economy value transient is outside GLō lifestyle midscale." },
    ]),
    loyaltyProof: BWH_PROOF("GLō"),
    feeNote: `GLō application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Lifestyle midscale select-service with design-led public space. Confirm staffing and F&B cues in current standards.",
    footprint:
      "Select GLō footprint. Confirm live counts from BWH disclosures.",
    insight:
      "GLō fits owners who can fund contemporary midscale design with Rewards; fails when capital only supports core Best Western or SureStay economy.",
    standards: [
      { title: "Lifestyle Design Standards", body: "Design and finishes must clear GLō—not core Best Western alone." },
      { title: "PIP Honesty", body: "Fund lifestyle PIP before promising GLō." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Lifestyle Midscale Conversion", body: "Shells that can fund GLō design." },
      { title: "Suburban Contemporary Midscale", body: "Suburban ADR that clears lifestyle capital." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus Best Western / Aiden", body: "Diligence versus Best Western and Aiden before IC." },
    ],
    gallery: gallery("GLō", "https://www.bestwestern.com/en_US/hotels/glo.html"),
  }),

  rectk6HIVra7oyoDP: pack({
    brandId: "rectk6HIVra7oyoDP",
    brandName: "Executive Residency by Best Western",
    liveBrandName: "Executive Residency by Best Western",
    slug: "executive-residency-by-best-western",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for Executive Residency (extended-stay)",
    sources: {
      brandPage: "https://www.bestwestern.com/",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Upscale-leaning extended-stay: residential suites with Best Western Rewards for longer-stay demand wanting more polish than SureStay Studio.",
    commercialDifferentiator:
      "Versus SureStay Studio affordable extended-stay, Executive Residency expects higher suite finishes; versus Hawthorn, it rides Best Western Rewards—confirm disclosure fees.",
    scenarios: [
      { title: "Upscale Extended-Stay", body: "Markets with longer-stay demand needing residential suite polish." },
      { title: "Corporate Relocation / Project Suites", body: "Corporate relocation and project demand with higher guest expectations." },
      { title: "Conversion Into Residential Suites", body: "Conversions that can fund residential suite PIP." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Executive Residency targets longer-stay corporate demand with residential suite product." },
      { title: "Project & Workforce Demand", fit: M, explanation: "Higher-end project stays can fit; pure workforce economy extended-stay may prefer Studio." },
      { title: "University & Medical", fit: M, explanation: "Medical/university longer stays fit when guests expect residential polish." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban extended-stay midweek mix can work." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is a weak fit." },
      { title: "Economy Value-Oriented Transient", fit: N, explanation: "Hard economy transient is outside Executive Residency." },
    ]),
    loyaltyProof: BWH_PROOF("Executive Residency"),
    feeNote: `Executive Residency application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Extended-stay with higher suite finishes and residential cues than SureStay Studio. Underwrite housekeeping and suite maintenance accordingly.",
    footprint:
      "Select Executive Residency footprint. Confirm live counts from BWH disclosures.",
    insight:
      "Executive Residency fits sponsors with longer-stay demand and residential suite capital; fails when underwritten like SureStay Studio economy extended-stay.",
    standards: [
      { title: "Residential Suite Product", body: "Suite finishes must clear Executive Residency standards." },
      { title: "Extended-Stay Ops", body: "Housekeeping cadence differs from transient flags." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Residential Extended-Stay", body: "Markets needing polished longer-stay suites." },
      { title: "Corporate Relocation Suites", body: "Relocation/project demand with higher expectations." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus SureStay Studio / Hawthorn", body: "Diligence versus SureStay Studio and Hawthorn before IC." },
    ],
    gallery: gallery("Executive Residency", "https://www.bestwestern.com/"),
  }),

  recPpLc3gOsPOwtCJ: pack({
    brandId: "recPpLc3gOsPOwtCJ",
    brandName: "Aiden by Best Western",
    liveBrandName: "Aiden by Best Western",
    slug: "aiden-by-best-western",
    parentCompany: "BWH Hotels",
    loyaltyProgram: "Best Western Rewards",
    feeGeography: "Confirm current BWH franchise disclosure for Aiden (lifestyle soft brand)",
    sources: {
      brandPage: "https://www.bestwestern.com/en_US/hotels/aiden.html",
      development: "https://www.bestwestern.com/en_US/hotels/join-best-western.html",
      bestWesternRewards: "https://www.bestwestern.com/en_US/rewards.html",
    },
    positioning:
      "Lifestyle soft brand: individual design and Best Western Rewards for travelers seeking local character with BWH distribution.",
    commercialDifferentiator:
      "Versus GLō prototype lifestyle, Aiden is softer/individual; versus Best Western Plus conventional upper-midscale, Aiden sells place-led design—confirm disclosure fees and design latitude.",
    scenarios: [
      { title: "Urban / Neighborhood Lifestyle", body: "Neighborhood assets with design character needing Rewards distribution." },
      { title: "Soft-Brand Conversion", body: "Independent or tired midscale assets converting with design latitude." },
      { title: "Secondary Lifestyle Markets", body: "Secondary cities wanting lifestyle cues without mega-chain soft-brand capital." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Aiden is built for soft-brand conversion with design latitude and Rewards." },
      { title: "Gateway Urban", fit: S, explanation: "Neighborhood/gateway urban lifestyle is a core Aiden use case when design capital clears ADR." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban lifestyle can work; confirm versus GLō prototype path." },
      { title: "Independent Soft-Brand Distribution", fit: S, explanation: "Independents seeking BWH Rewards without losing place character." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure highway economy transient is a weak fit." },
      { title: "Economy Value-Oriented Transient", fit: N, explanation: "Hard economy value transient is outside Aiden lifestyle." },
    ]),
    loyaltyProof: BWH_PROOF("Aiden"),
    feeNote: `Aiden application, royalty, marketing, Best Western Rewards, and PIP — ${FDD}`,
    operatingModel:
      "Lifestyle soft brand with design-led public space and BWH systems. Confirm design latitude and QA in current standards.",
    footprint:
      "Select Aiden footprint. Confirm live counts from BWH disclosures.",
    insight:
      "Aiden fits owners with place-led assets who want Best Western Rewards; fails when capital cannot fund design or when guests expect conventional Best Western Plus without lifestyle investment.",
    standards: [
      { title: "Lifestyle Design Latitude", body: "Design must clear Aiden expectations while remaining brand-compliant." },
      { title: "Soft-Brand Conversion Discipline", body: "Fund design PIP—cosmetic reflag fails." },
      { title: "Best Western Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Neighborhood Soft Brand", body: "Place-led assets needing Rewards distribution." },
      { title: "Design-Led Conversion", body: "Conversions with funded design latitude." },
      { title: "Disclosure Fee Gate", body: "Model only from current BWH disclosure exhibits." },
      { title: "Sibling Clarity Versus GLō / Best Western Plus", body: "Diligence versus GLō and Best Western Plus before IC." },
    ],
    gallery: gallery("Aiden", "https://www.bestwestern.com/en_US/hotels/aiden.html"),
  }),

  // ——— Sonesta ———
  rec4ztolqpD1uYrsG: pack({
    brandId: "rec4ztolqpD1uYrsG",
    brandName: "Sonesta Select",
    liveBrandName: "Sonesta Select",
    slug: "sonesta-select",
    parentCompany: "Sonesta International Hotels Corporation",
    loyaltyProgram: "Sonesta Travel Pass",
    feeGeography: "Confirm current Sonesta franchise / development disclosure for Sonesta Select",
    sources: {
      brandPage: "https://www.sonesta.com/sonesta-select",
      development: "https://www.sonesta.com/franchise",
      sonestaTravelPass: "https://www.sonesta.com/travelpass",
    },
    positioning:
      "Upper-midscale select-service: contemporary rooms and Sonesta Travel Pass for travelers seeking polished limited-service without full-service capital.",
    commercialDifferentiator:
      "Versus Red Lion midscale, Select is more contemporary select-service; versus Sonesta ES Suites, it is transient-focused—confirm disclosure fees.",
    scenarios: [
      { title: "Suburban Select-Service", body: "Suburban corridors needing upper-midscale select-service with Travel Pass." },
      { title: "Airport Edge Select-Service", body: "Airport edge sites with midweek corporate mix." },
      { title: "Conversion Into Select-Service", body: "Conversions that can fund Select product standards." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Sonesta Select fits suburban select-service business/leisure with Travel Pass." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport edge select-service is a common Select use case." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work when PIP funds select-service product." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets need ADR that supports Select fees." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay is Sonesta ES Suites territory." },
      { title: "Economy Value-Oriented Transient", fit: N, explanation: "Hard economy value transient is outside Select." },
    ]),
    loyaltyProof: SONESTA_PROOF("Sonesta Select"),
    feeNote: `Sonesta Select application, royalty, marketing, Travel Pass, and PIP — ${FDD}`,
    operatingModel:
      "Upper-midscale select-service with breakfast and leaner staffing than full-service. Confirm standards in current Sonesta materials.",
    footprint:
      "Sonesta Select footprint across North America. Confirm live counts from Sonesta disclosures.",
    insight:
      "Sonesta Select fits owners who want polished select-service with Travel Pass; wrong when demand is extended-stay (ES Suites) or hard economy (ABVI).",
    standards: [
      { title: "Select-Service Product", body: "Rooms and breakfast must clear Select standards." },
      { title: "PIP Honesty", body: "Fund select-service PIP—not economy scopes." },
      { title: "Sonesta Travel Pass Integration", body: "Travel Pass delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Suburban Select-Service", body: "Corridors where Select rates clear IC." },
      { title: "Airport Edge Select", body: "Airport midweek corporate mix." },
      { title: "Disclosure Fee Gate", body: "Model only from current Sonesta disclosure exhibits." },
      { title: "Sibling Clarity Versus ES Suites / Red Lion", body: "Diligence versus ES Suites and Red Lion before IC." },
    ],
    gallery: gallery("Sonesta Select", "https://www.sonesta.com/sonesta-select"),
  }),

  recEaNmmaNdbWvipS: pack({
    brandId: "recEaNmmaNdbWvipS",
    brandName: "Sonesta ES Suites",
    liveBrandName: "Sonesta ES Suites",
    slug: "sonesta-es-suites",
    parentCompany: "Sonesta International Hotels Corporation",
    loyaltyProgram: "Sonesta Travel Pass",
    feeGeography: "Confirm current Sonesta franchise / development disclosure for Sonesta ES Suites (extended-stay)",
    sources: {
      brandPage: "https://www.sonesta.com/sonesta-es-suites",
      development: "https://www.sonesta.com/franchise",
      sonestaTravelPass: "https://www.sonesta.com/travelpass",
    },
    positioning:
      "Midscale extended-stay: suites with kitchens and Sonesta Travel Pass for project, corporate, and longer-stay demand.",
    commercialDifferentiator:
      "Versus Sonesta Select transient, ES Suites is length-of-stay economics; versus Hawthorn / SureStay Studio, it rides Travel Pass—confirm disclosure fees.",
    scenarios: [
      { title: "Corporate Extended-Stay", body: "Markets with multi-week corporate demand." },
      { title: "Project / Medical Longer Stay", body: "Project and medical corridors needing kitchenettes." },
      { title: "Conversion Into Extended-Stay Suites", body: "Conversions that can fund suite/kitchen PIP." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "ES Suites' core thesis is corporate extended-stay with kitchens and Travel Pass." },
      { title: "Project & Workforce Demand", fit: S, explanation: "Project/workforce weekly stays are a primary ES Suites pocket." },
      { title: "University & Medical", fit: S, explanation: "Medical/university longer stays fit suite kitchenette product." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban midweek extended-stay works; pure weekend leisure is secondary." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "One-night highway transient is a weak fit versus ABVI / Red Lion." },
      { title: "Resort & Coastal Leisure", fit: N, explanation: "Resort leisure destination economics are outside ES Suites." },
    ]),
    loyaltyProof: SONESTA_PROOF("Sonesta ES Suites"),
    feeNote: `Sonesta ES Suites application, royalty, marketing, Travel Pass, and PIP — ${FDD}`,
    operatingModel:
      "Extended-stay with kitchenette maintenance and housekeeping cadence distinct from transient select-service.",
    footprint:
      "Sonesta ES Suites footprint. Confirm live counts from Sonesta disclosures.",
    insight:
      "ES Suites fits sponsors with genuine extended-stay demand and suite capital; fails when underwritten like Sonesta Select transient without kitchen PIP.",
    standards: [
      { title: "Suite / Kitchen Product", body: "Kitchenettes must clear ES Suites standards." },
      { title: "Extended-Stay Ops Cadence", body: "Housekeeping rhythms differ from transient." },
      { title: "Sonesta Travel Pass Integration", body: "Travel Pass delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Corporate Extended-Stay", body: "Multi-week corporate demand markets." },
      { title: "Project / Medical Suites", body: "Project and medical corridors." },
      { title: "Disclosure Fee Gate", body: "Model only from current Sonesta disclosure exhibits." },
      { title: "Sibling Clarity Versus Select / Hawthorn", body: "Diligence versus Sonesta Select and Hawthorn before IC." },
    ],
    gallery: gallery("Sonesta ES Suites", "https://www.sonesta.com/sonesta-es-suites"),
  }),

  rechMdsQxPPhet6rV: pack({
    brandId: "rechMdsQxPPhet6rV",
    brandName: "Red Lion Hotels, Inns & Suites",
    liveBrandName: "Red Lion Hotels",
    slug: "red-lion-hotels",
    parentCompany: "Sonesta International Hotels Corporation",
    loyaltyProgram: "Sonesta Travel Pass",
    feeGeography: "Confirm current Sonesta / Red Lion franchise disclosure",
    sources: {
      brandPage: "https://www.sonesta.com/red-lion-hotels",
      development: "https://www.sonesta.com/franchise",
      sonestaTravelPass: "https://www.sonesta.com/travelpass",
    },
    positioning:
      "Midscale: regional brand awareness and Sonesta Travel Pass for travelers seeking familiar midscale product in secondary and highway markets.",
    commercialDifferentiator:
      "Versus Sonesta Select contemporary select-service, Red Lion is broader midscale/heritage regional; versus ABVI economy, it sits above hard economy—confirm disclosure fees.",
    scenarios: [
      { title: "Regional Midscale Highway", body: "Regional highway markets with Red Lion awareness." },
      { title: "Secondary-Market Conversion", body: "Conversions into Red Lion midscale standards." },
      { title: "Travel Pass Distribution Defense", body: "Assets needing Sonesta distribution to reduce OTA dependence." },
    ],
    demandScenarios: demand([
      { title: "Regional & Secondary Markets", fit: S, explanation: "Red Lion is strongest in regional/secondary midscale markets." },
      { title: "Highway & Interstate Transient", fit: S, explanation: "Highway midscale transient fits when product and Travel Pass clear local comps." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work with midscale PIP discipline." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix can work; confirm versus Select contemporary product." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay is ES Suites territory." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside Red Lion midscale." },
    ]),
    loyaltyProof: SONESTA_PROOF("Red Lion"),
    feeNote: `Red Lion application, royalty, marketing, Travel Pass, and PIP — ${FDD}`,
    operatingModel:
      "Midscale limited-service / selective fuller public space depending on asset. Confirm current Red Lion standards under Sonesta.",
    footprint:
      "Legacy Red Lion regional footprint. Confirm live counts from Sonesta disclosures.",
    insight:
      "Red Lion fits regional midscale owners wanting Travel Pass; wrong when capital only supports ABVI economy or when guests expect Sonesta Select contemporary product without funding it.",
    standards: [
      { title: "Midscale Product Basics", body: "Rooms and public space must clear Red Lion standards." },
      { title: "Conversion / PIP Discipline", body: "PIP must match midscale—not economy-only capital." },
      { title: "Sonesta Travel Pass Integration", body: "Travel Pass delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Regional Midscale", body: "Highway/regional markets where Red Lion rates clear IC." },
      { title: "Secondary Conversion", body: "Conversions into Red Lion midscale." },
      { title: "Disclosure Fee Gate", body: "Model only from current Sonesta / Red Lion disclosure exhibits." },
      { title: "Sibling Clarity Versus Select / ABVI", body: "Diligence versus Sonesta Select and Americas Best Value Inn before IC." },
    ],
    gallery: gallery("Red Lion", "https://www.sonesta.com/red-lion-hotels"),
  }),

  recgYprNavH0rgb7z: pack({
    brandId: "recgYprNavH0rgb7z",
    brandName: "Signature Inn",
    liveBrandName: "Signature Inn",
    slug: "signature-inn",
    parentCompany: "Sonesta International Hotels Corporation",
    loyaltyProgram: "Sonesta Travel Pass",
    feeGeography: "Confirm current Sonesta franchise disclosure for Signature Inn",
    sources: {
      brandPage: "https://www.sonesta.com/",
      development: "https://www.sonesta.com/franchise",
      sonestaTravelPass: "https://www.sonesta.com/travelpass",
    },
    positioning:
      "Economy / midscale value: essential product and Sonesta Travel Pass for price-sensitive transient demand.",
    commercialDifferentiator:
      "Versus ABVI hard economy, Signature Inn may read slightly fuller value midscale depending on asset; versus Red Lion, confirm midscale expectations from current standards—model from disclosure.",
    scenarios: [
      { title: "Highway Value Transient", body: "Highway sites competing on rate with Travel Pass." },
      { title: "Secondary-Market Economy", body: "Secondary markets with economy ADR." },
      { title: "Conversion Cost Discipline", body: "Conversions that stay capital-efficient." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Signature Inn fits highway value transient with Travel Pass." },
      { title: "Economy Value-Oriented Transient", fit: S, explanation: "Price-sensitive economy transient is a core use case." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets work with economy/midscale ADR realism." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work with capital-efficient PIP." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban upper-midscale expectations usually exceed Signature Inn product." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside Signature Inn." },
    ]),
    loyaltyProof: SONESTA_PROOF("Signature Inn"),
    feeNote: `Signature Inn application, royalty, marketing, Travel Pass, and PIP — ${FDD}`,
    operatingModel:
      "Economy / midscale value with lean staffing. Confirm breakfast and amenity requirements in current Sonesta standards.",
    footprint:
      "Select Signature Inn footprint. Confirm live counts from Sonesta disclosures.",
    insight:
      "Signature Inn fits owners optimizing for value capital with Travel Pass; wrong when guests expect Red Lion midscale or Select polish without funding it.",
    standards: [
      { title: "Value Product Basics", body: "Clean rooms and safety basics must clear Signature Inn standards." },
      { title: "Capital Efficiency", body: "Keep PIP economy/midscale-scope." },
      { title: "Sonesta Travel Pass Integration", body: "Travel Pass delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Highway Value Flag", body: "Corridors competing on rate with Travel Pass." },
      { title: "Secondary Economy", body: "Secondary markets with economy ADR." },
      { title: "Disclosure Fee Gate", body: "Model only from current Sonesta disclosure exhibits." },
      { title: "Sibling Clarity Versus ABVI / Red Lion", body: "Diligence versus Americas Best Value Inn and Red Lion before IC." },
    ],
    gallery: gallery("Signature Inn", "https://www.sonesta.com/"),
  }),

  recgMqPkLsfaMU9vA: pack({
    brandId: "recgMqPkLsfaMU9vA",
    brandName: "Americas Best Value Inn",
    liveBrandName: "Americas Best Value Inn",
    slug: "americas-best-value-inn",
    parentCompany: "Sonesta International Hotels Corporation",
    loyaltyProgram: "Sonesta Travel Pass",
    feeGeography: "Confirm current Sonesta / ABVI franchise disclosure (economy)",
    sources: {
      brandPage: "https://www.sonesta.com/americas-best-value-inn",
      development: "https://www.sonesta.com/franchise",
      sonestaTravelPass: "https://www.sonesta.com/travelpass",
    },
    positioning:
      "Hard economy: essential rooms and lowest-cost operations with Sonesta Travel Pass for highly price-sensitive transient demand.",
    commercialDifferentiator:
      "Versus Signature Inn / Red Lion midscale, ABVI is capital- and amenity-light economy; versus Super 8 / Econo Lodge peers, it rides Travel Pass—model from current disclosure, not peer rumor.",
    scenarios: [
      { title: "Highway Hard Economy", body: "Highway sites where price leadership and Travel Pass matter more than midscale amenities." },
      { title: "Low-Cost Conversion", body: "Conversions that can meet ABVI standards with minimal PIP relative to midscale flags." },
      { title: "Secondary-Market Value", body: "Secondary markets where economy ADR and Sonesta distribution underwrite contribution." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "ABVI's core demand is highway hard-economy transient with Travel Pass." },
      { title: "Economy Value-Oriented Transient", fit: S, explanation: "Price-sensitive economy transient is the primary thesis." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary economy markets fit when fees and PIP stay hard-economy scope." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work with minimal PIP—midscale overbuilding destroys the thesis." },
      { title: "Suburban Business & Leisure", fit: L, explanation: "Suburban midscale expectations usually exceed ABVI product." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay is ES Suites territory—not ABVI." },
    ]),
    loyaltyProof: SONESTA_PROOF("Americas Best Value Inn"),
    feeNote: `Americas Best Value Inn application, royalty, marketing, Travel Pass, and PIP — ${FDD}`,
    operatingModel:
      "Hard economy with minimal amenity stack and lean staffing. Confirm breakfast and housekeeping requirements—do not assume midscale standards.",
    footprint:
      "Broad ABVI economy footprint. Confirm live counts from Sonesta disclosures.",
    insight:
      "ABVI fits owners optimizing for lowest capital and operating cost with Travel Pass; it fails when guests or lenders expect midscale product (Red Lion / Select) without funding it.",
    standards: [
      { title: "Essential Economy Product", body: "Clean rooms and safety basics must clear ABVI standards." },
      { title: "Capital Efficiency", body: "PIP should stay economy-scope—midscale overbuilding destroys the thesis." },
      { title: "Sonesta Travel Pass Integration", body: "Travel Pass delivery within economy capability." },
    ],
    valueOwnerScenarios: [
      { title: "Lowest-Cost Economy Flag", body: "Sites where hard economy fees and PIP clear IC." },
      { title: "Highway Price Leadership", body: "Corridors competing on rate with Travel Pass." },
      { title: "Disclosure Fee Gate", body: "Model only from current Sonesta / ABVI disclosure exhibits." },
      { title: "Sibling Clarity Versus Signature Inn / Red Lion", body: "Diligence versus Signature Inn and Red Lion before IC." },
    ],
    gallery: gallery("Americas Best Value Inn", "https://www.sonesta.com/americas-best-value-inn"),
  }),
});

export function listBatch8() {
  return BATCH_8_IDS.map((id) => BATCH_8_PACKS_BY_ID[id]).filter(Boolean);
}

export function getBatch8Pack(brandId) {
  return BATCH_8_PACKS_BY_ID[brandId] || null;
}
