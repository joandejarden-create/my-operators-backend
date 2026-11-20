/**
 * Factory 2.0 Batch 9 — EXISTING Active Airtable brands
 * (Marriott / Hilton / Hyatt select-service, extended-stay, full-service, lifestyle).
 * All IDs are live EXISTING_CANONICAL Brand Basics. Delta Hotels resolved: rec50qkCj6yt9fMPg.
 * Franchise fee amounts never invented — FDD / disclosure confirmation only.
 * Demand Scenario Fit: brand-specific six (not mechanical repeats).
 */
import { DEMAND_FIT_LABELS } from "../be-factory-permanent-demand-scenarios-six-v1.js";
import { BONVOY_PROOF, HONORS_PROOF, HYATT_PROOF } from "./be-factory-2-depth-loyalty-proofs-v1.js";

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    batchTag: "batch-9",
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

export const FACTORY_2_BATCH_9_V1 = "FACTORY_2_BATCH_9_V1";

const FDD =
  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";

const S = DEMAND_FIT_LABELS.STRONG_FIT;
const M = DEMAND_FIT_LABELS.MODERATE_FIT;
const L = DEMAND_FIT_LABELS.LIMITED_FIT;
const N = DEMAND_FIT_LABELS.NOT_A_FIT;

export const BATCH_9_IDS = Object.freeze([
  "recucEzAS6724tOYA", // City Express
  "recBzdGfkMUN9fYsv", // SpringHill Suites
  "recDM0LAD8jVRA2x3", // StudioRes
  "recUPiPDivkhNUogr", // TownePlace Suites
  "recpUTDtwt1wPMDPj", // Fairfield
  "recH5ZF9V6ivz9p5h", // Four Points
  "rec9Ufbpa0GxJGzt8", // Residence Inn
  "recn59UtkyyoYwzSz", // Marriott Hotels
  "recg8HjT5Bky7NXeV", // Sheraton
  "recWubG3rhiS1BaWi", // Hilton Hotels
  "rechVYWQ5ikRnr99B", // DoubleTree
  "rectRvOWQPaL6FkzZ", // Hampton
  "recrvdAjRlXxPvPPF", // Hilton Garden Inn
  "reccZ4zV6wMav7a2i", // Home2 Suites
  "recZjYI4nYflGHFNR", // Homewood Suites
  "recfv66er4Ch2vJDO", // Spark
  "recJLiMTv4W8VgO9L", // Tru
  "recP9SqDootMrzaU1", // Hyatt Regency
  "rec4Mga6ejz3L1M3P", // Thompson
  "rec50qkCj6yt9fMPg", // Delta Hotels
]);

export const BATCH_9_PACKS_BY_ID = Object.freeze({
  recucEzAS6724tOYA: pack({
    brandId: "recucEzAS6724tOYA",
    brandName: "City Express by Marriott",
    liveBrandName: "City Express by Marriott",
    slug: "city-express-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott franchise / development disclosure for City Express (LATAM midscale limited-service)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/city-express-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "LATAM midscale limited-service: efficient rooms and Bonvoy distribution for travelers seeking consistent midscale product across Mexico and broader CALA corridors.",
    commercialDifferentiator: "Versus Fairfield midscale limited-service in North America, City Express is the LATAM midscale node; versus full-service Marriott Hotels, it keeps leaner capital and operating intensity—model from current Marriott disclosure for this brand.",
    scenarios: [
      { title: "LATAM Midscale Limited-Service", body: "Urban and corridor sites across Mexico / CALA where midscale product and Bonvoy clear local ADR without full-service capital." },
      { title: "Conversion Into LATAM Midscale", body: "Conversions that can fund City Express standards and PIP for regional midscale demand." },
      { title: "Airport & Business Corridor Value", body: "Airport-edge and business-corridor assets needing Bonvoy without upscale full-service spend." },
    ],
    demandScenarios: demand([
      { title: "Regional & Secondary Markets", fit: S, explanation: "City Express is built for LATAM regional and secondary markets where midscale limited-service and Bonvoy clear local ADR." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport-edge sites fit when product and Bonvoy defend share versus other LATAM midscale flags." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban business/leisure mix works where City Express standards match corridor demand." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when PIP funds midscale guest rooms—not economy-only capital." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not the core City Express product—consider TownePlace or Residence Inn ladders instead." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship economics sit outside City Express midscale limited-service positioning." },
    ]),
    loyaltyProof: BONVOY_PROOF("City Express"),
    feeNote: `City Express application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "LATAM midscale limited-service with leaner staffing than full-service. Confirm breakfast, housekeeping, and regional standards in current Marriott disclosures.",
    footprint: "Primarily Mexico and broader CALA / LATAM City Express footprint. Confirm live counts from Marriott disclosures—do not underwrite from launch media alone.",
    insight: "City Express fits owners who need Bonvoy midscale distribution in LATAM with limited-service capital; wrong when sponsors underwrite North American Fairfield fees or expect full-service Marriott Hotels product.",
    standards: [
      { title: "LATAM Midscale Product", body: "Rooms and public space must clear City Express midscale standards for the operating region." },
      { title: "Conversion / PIP Discipline", body: "PIP scope must match midscale—not economy paint-and-carpet alone." },
      { title: "Bonvoy Integration", body: "Bonvoy enrollment and Elite delivery within midscale capability." },
    ],
    valueOwnerScenarios: [
      { title: "LATAM Midscale Corridor", body: "Markets where City Express ADR clears IC above local midscale comps." },
      { title: "Airport Limited-Service", body: "Airport-edge assets needing Bonvoy without full-service capital." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott disclosure / agreement exhibits for City Express." },
      { title: "Sibling Clarity Versus Fairfield", body: "Diligence versus Fairfield and other Marriott midscale siblings before IC." },
    ],
    gallery: gallery("City Express", "https://www.marriott.com/en-us/hotels/travel/city-express-hotels/"),
  }),

  recBzdGfkMUN9fYsv: pack({
    brandId: "recBzdGfkMUN9fYsv",
    brandName: "SpringHill Suites by Marriott",
    liveBrandName: "SpringHill Suites by Marriott",
    slug: "springhill-suites-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD for SpringHill Suites (all-suite select-service)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/springhill-suites-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "All-suite select-service: suite living space and Bonvoy for travelers who want more room than a standard select-service king without full extended-stay kitchens.",
    commercialDifferentiator: "Versus Courtyard select-service, SpringHill sells suite footprint; versus Residence Inn / TownePlace extended-stay, it is shorter-stay all-suite rather than kitchen-led weekly stays—model from current FDD.",
    scenarios: [
      { title: "Suburban All-Suite Select-Service", body: "Suburban corridors where suite space and Bonvoy support rates above standard select-service kings." },
      { title: "Airport Suite Demand", body: "Airport sites needing suite product for crews and leisure without full extended-stay capital." },
      { title: "Conversion Into All-Suite", body: "Conversions that can deliver suite geometry and SpringHill standards." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "SpringHill Suites core demand is suburban business/leisure needing suite space with Bonvoy." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport sites fit when suite product and Bonvoy defend share versus Courtyard-class comps." },
      { title: "Extended-Stay Corporate", fit: M, explanation: "Some longer stays occur, but kitchens and weekly economics are not the SpringHill core—compare TownePlace / Residence Inn." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work when suite geometry and PIP clear SpringHill standards." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings compression is limited—select-service suite product is not full-service ballroom economics." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside SpringHill all-suite select-service." },
    ]),
    loyaltyProof: BONVOY_PROOF("SpringHill Suites"),
    feeNote: `SpringHill Suites application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "All-suite select-service with suite housekeeping intensity above standard kings. Confirm breakfast and suite standards in current Marriott FDD.",
    footprint: "Broad North American SpringHill Suites footprint with selective international presence. Confirm live counts from Marriott disclosures.",
    insight: "SpringHill fits owners who can fund suite product with Bonvoy; fails when capital only supports standard select-service kings or when guests need full kitchens for weekly stays.",
    standards: [
      { title: "All-Suite Product", body: "Suite living space and guest-room standards must clear SpringHill expectations." },
      { title: "Select-Service Operating Model", body: "Breakfast and lean F&B stay select-service—do not import full-service labor." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Suburban Suite Premium", body: "Sites where suite ADR clears IC versus standard select-service." },
      { title: "Airport Suite Mix", body: "Airport demand needing suite space with Bonvoy." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott FDD exhibits." },
      { title: "Sibling Clarity Versus Courtyard / Residence Inn", body: "Diligence versus Courtyard and Residence Inn before IC." },
    ],
    gallery: gallery("SpringHill Suites", "https://www.marriott.com/en-us/hotels/travel/springhill-suites-hotels/"),
  }),

  recDM0LAD8jVRA2x3: pack({
    brandId: "recDM0LAD8jVRA2x3",
    brandName: "StudioRes",
    liveBrandName: "StudioRes",
    slug: "studiores",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott franchise / development disclosure for StudioRes (affordable extended-stay)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/studiores-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Affordable extended-stay studio product with Bonvoy for longer-stay guests who need kitchenette efficiency without premium extended-stay capital.",
    commercialDifferentiator: "Versus TownePlace midscale extended-stay and Residence Inn upscale extended-stay, StudioRes targets leaner studio economics; versus Fairfield transient midscale, it is stay-length and kitchenette-led—confirm live disclosure and prototype before IC.",
    scenarios: [
      { title: "Affordable Extended-Stay Studio", body: "Markets where weekly and monthly demand needs Bonvoy at capital below premium extended-stay." },
      { title: "Corporate Project & Relocation Housing", body: "Project crews and relocation guests needing kitchenette studios without Residence Inn rates." },
      { title: "Conversion Into Studio Extended-Stay", body: "Conversions that can deliver studio / kitchenette geometry for StudioRes standards." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "StudioRes is built for affordable extended-stay corporate and project demand with Bonvoy." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets fit when weekly ADR clears StudioRes fees without premium extended-stay capital." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Conversions work when kitchenette / studio PIP is funded—confirm prototype requirements." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix can work when longer-stay share is material; pure transient may prefer Fairfield." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Highway overnight transient is not the core StudioRes stay-length thesis." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship is outside StudioRes affordable extended-stay positioning." },
    ]),
    loyaltyProof: BONVOY_PROOF("StudioRes"),
    feeNote: `StudioRes application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Affordable extended-stay with kitchenette housekeeping and longer average stay. Confirm staffing and prototype in current Marriott disclosures—do not invent fee tables.",
    footprint: "Emerging Marriott affordable extended-stay footprint. Confirm live hotels and pipeline from Marriott disclosures only.",
    insight: "StudioRes fits owners chasing weekly demand with Bonvoy at leaner capital; fails when sponsors underwrite Residence Inn product or invent franchise fee percentages.",
    standards: [
      { title: "Studio / Kitchenette Product", body: "Studio geometry and kitchenette standards must clear StudioRes expectations." },
      { title: "Extended-Stay Operating Model", body: "Housekeeping and stay-length policies differ from transient midscale—budget accordingly." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Weekly Affordable Extended-Stay", body: "Markets where StudioRes ADR clears IC versus local extended-stay comps." },
      { title: "Project Housing Mix", body: "Corporate project and relocation demand with kitchenette needs." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott disclosure exhibits for StudioRes." },
      { title: "Sibling Clarity Versus TownePlace / Residence Inn", body: "Diligence versus TownePlace and Residence Inn before IC." },
    ],
    gallery: gallery("StudioRes", "https://www.marriott.com/en-us/hotels/travel/studiores-hotels/"),
  }),

  recUPiPDivkhNUogr: pack({
    brandId: "recUPiPDivkhNUogr",
    brandName: "TownePlace Suites by Marriott",
    liveBrandName: "TownePlace Suites by Marriott",
    slug: "towneplace-suites-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD for TownePlace Suites (midscale extended-stay)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/towneplace-suites-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Midscale extended-stay: suites with kitchens and Bonvoy for guests staying longer than a typical transient night without upscale Residence Inn capital.",
    commercialDifferentiator: "Versus Residence Inn upscale extended-stay, TownePlace is midscale weekly economics; versus StudioRes affordable studio, it typically sits higher on product and rate—confirm FDD and local comps.",
    scenarios: [
      { title: "Midscale Extended-Stay Suites", body: "Suburban and secondary markets where weekly demand supports midscale kitchens and Bonvoy." },
      { title: "Corporate Project Housing", body: "Project and relocation guests needing suites without Residence Inn rates." },
      { title: "Conversion Into Midscale Extended-Stay", body: "Conversions that can fund kitchen suites and TownePlace PIP." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "TownePlace core demand is midscale extended-stay corporate and project housing with Bonvoy." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban corridors with weekly share fit when kitchens and Bonvoy defend rates." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets work when midscale weekly ADR clears TownePlace fees." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when kitchen-suite PIP is funded." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings are not the TownePlace thesis—extended-stay suites dominate." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside TownePlace midscale extended-stay." },
    ]),
    loyaltyProof: BONVOY_PROOF("TownePlace Suites"),
    feeNote: `TownePlace Suites application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Midscale extended-stay with kitchen housekeeping and longer stays. Confirm breakfast and suite standards in current Marriott FDD.",
    footprint: "Broad North American TownePlace Suites footprint. Confirm live counts from Marriott disclosures.",
    insight: "TownePlace fits owners with weekly demand who can fund midscale kitchens with Bonvoy; wrong when capital only supports StudioRes lean studios or when guests expect Residence Inn upscale product without funding it.",
    standards: [
      { title: "Midscale Kitchen Suites", body: "Suite and kitchen standards must clear TownePlace expectations." },
      { title: "Extended-Stay Ops", body: "Housekeeping cadence and pantry/breakfast requirements differ from transient midscale." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Weekly Midscale Extended-Stay", body: "Markets where TownePlace ADR clears IC versus local extended-stay comps." },
      { title: "Project & Relocation Mix", body: "Corporate project housing with kitchen needs." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott FDD exhibits." },
      { title: "Sibling Clarity Versus Residence Inn / StudioRes", body: "Diligence versus Residence Inn and StudioRes before IC." },
    ],
    gallery: gallery("TownePlace Suites", "https://www.marriott.com/en-us/hotels/travel/towneplace-suites-hotels/"),
  }),

  recpUTDtwt1wPMDPj: pack({
    brandId: "recpUTDtwt1wPMDPj",
    brandName: "Fairfield by Marriott",
    liveBrandName: "Fairfield by Marriott",
    slug: "fairfield-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD for Fairfield by Marriott (midscale limited-service)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/fairfield-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Midscale limited-service: consistent rooms, breakfast, and Bonvoy for value-conscious transient demand without select-service F&B capital.",
    commercialDifferentiator: "Versus Courtyard / Hilton Garden Inn select-service, Fairfield keeps leaner limited-service economics; versus Hampton upper-midscale limited-service peers, confirm local ADR and fee tables from current FDD—not sibling assumptions.",
    scenarios: [
      { title: "Suburban Midscale Limited-Service", body: "Suburban corridors where breakfast and Bonvoy support midscale transient rates." },
      { title: "Airport & Highway Value", body: "Airport and interstate sites needing Marriott distribution without select-service capital." },
      { title: "Conversion Into Midscale Limited-Service", body: "Conversions that can fund Fairfield PIP without jumping to select-service." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Fairfield is built for highway and corridor transient with breakfast and Bonvoy." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport-edge sites fit when midscale limited-service and Bonvoy defend share." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban business/leisure mix is a core Fairfield use case." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work when PIP funds midscale guest rooms and breakfast." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Fairfield's core—consider TownePlace / Residence Inn." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Fairfield midscale limited-service." },
    ]),
    loyaltyProof: BONVOY_PROOF("Fairfield"),
    feeNote: `Fairfield by Marriott application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Midscale limited-service with breakfast and leaner staffing than select-service. Confirm housekeeping and breakfast requirements in current Marriott FDD.",
    footprint: "Broad North American Fairfield footprint with selective international presence. Confirm live counts from Marriott disclosures.",
    insight: "Fairfield fits owners who want Bonvoy midscale limited-service consistency; fails when sponsors underwrite Courtyard select-service capital or invent royalty percentages.",
    standards: [
      { title: "Midscale Limited-Service Product", body: "Rooms, breakfast, and public space must clear Fairfield standards." },
      { title: "Conversion / PIP Discipline", body: "PIP must match midscale—not economy-only scopes." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Corridor Midscale Limited-Service", body: "Sites where Fairfield ADR clears IC above economy comps." },
      { title: "Airport Limited-Service", body: "Airport assets needing Bonvoy without select-service capital." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott FDD exhibits." },
      { title: "Sibling Clarity Versus Courtyard / Hampton", body: "Diligence versus Courtyard and Hampton-class peers before IC." },
    ],
    gallery: gallery("Fairfield", "https://www.marriott.com/en-us/hotels/travel/fairfield-hotels/"),
  }),

  recH5ZF9V6ivz9p5h: pack({
    brandId: "recH5ZF9V6ivz9p5h",
    brandName: "Four Points by Sheraton",
    liveBrandName: "Four Points by Sheraton",
    slug: "four-points-by-sheraton",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD for Four Points by Sheraton (upper-midscale / conversion-friendly)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/four-points-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Upper-midscale / select-service with conversion flexibility: practical comfort and Bonvoy for travelers who want Sheraton-family recognition without full-service Sheraton capital.",
    commercialDifferentiator: "Versus Fairfield limited-service, Four Points often reads fuller upper-midscale / select; versus Sheraton full-service, it is conversion-friendlier with leaner F&B—confirm current FDD and brand standards.",
    scenarios: [
      { title: "Conversion-Friendly Upper-Midscale", body: "Existing hotels that can meet Four Points standards without full Sheraton meetings capital." },
      { title: "Airport & Suburban Select Demand", body: "Airport and suburban sites needing Bonvoy upper-midscale product." },
      { title: "Secondary-Market Brand Flag", body: "Secondary markets where Four Points clears rates above limited-service peers." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Four Points is frequently conversion-oriented—strong when PIP funds upper-midscale product with Bonvoy." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport sites fit when upper-midscale product and Bonvoy defend share." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban mix is a core Four Points use case." },
      { title: "Regional & Secondary Markets", fit: M, explanation: "Secondary markets work when ADR supports Four Points fees versus Fairfield / Hampton comps." },
      { title: "Meetings & Group Compression", fit: M, explanation: "Limited meetings utility may exist—do not underwrite like full-service Sheraton ballrooms." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Four Points upper-midscale positioning." },
    ]),
    loyaltyProof: BONVOY_PROOF("Four Points"),
    feeNote: `Four Points by Sheraton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Upper-midscale / select-service operating intensity above limited-service; below full-service Sheraton. Confirm F&B and staffing in current FDD.",
    footprint: "Global Four Points footprint with conversion emphasis in many markets. Confirm live counts from Marriott disclosures.",
    insight: "Four Points fits owners converting into Marriott upper-midscale with Bonvoy; wrong when capital only supports Fairfield limited-service or when guests expect Sheraton full-service meetings without funding them.",
    standards: [
      { title: "Upper-Midscale Product", body: "Rooms and public space must clear Four Points standards." },
      { title: "Conversion PIP Honesty", body: "PIP must match upper-midscale—not limited-service-only capital." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Conversion Upper-Midscale", body: "Assets where Four Points PIP clears IC versus limited-service comps." },
      { title: "Airport Upper-Midscale", body: "Airport demand needing Bonvoy above Fairfield." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott FDD exhibits." },
      { title: "Sibling Clarity Versus Fairfield / Sheraton", body: "Diligence versus Fairfield and Sheraton before IC." },
    ],
    gallery: gallery("Four Points", "https://www.marriott.com/en-us/hotels/travel/four-points-hotels/"),
  }),

  rec9Ufbpa0GxJGzt8: pack({
    brandId: "rec9Ufbpa0GxJGzt8",
    brandName: "Residence Inn by Marriott",
    liveBrandName: "Residence Inn by Marriott",
    slug: "residence-inn-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD for Residence Inn (upscale extended-stay)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/residence-inn-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Upscale extended-stay: full kitchens, residential suites, and Bonvoy for guests staying weeks with higher service and space expectations than midscale extended-stay.",
    commercialDifferentiator: "Versus TownePlace midscale extended-stay, Residence Inn sells upscale residential product; versus Homewood Suites (Hilton), it rides Bonvoy rather than Honors—confirm FDD and local weekly ADR.",
    scenarios: [
      { title: "Upscale Extended-Stay Suites", body: "Markets where weekly demand supports upscale kitchens and Bonvoy." },
      { title: "Corporate Relocation & Project Premium", body: "Longer-stay guests needing residential quality above midscale extended-stay." },
      { title: "Suburban Upscale Weekly Demand", body: "Suburban corridors with material weekly share and Bonvoy accounts." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Residence Inn core demand is upscale extended-stay corporate, relocation, and project housing with Bonvoy." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban weekly mix fits when residential suites and Bonvoy defend rates." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge extended-stay can work when weekly share is durable." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when upscale kitchen-suite PIP is funded." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure highway overnight transient is not the Residence Inn thesis." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship meetings hotels sit outside Residence Inn extended-stay." },
    ]),
    loyaltyProof: BONVOY_PROOF("Residence Inn"),
    feeNote: `Residence Inn by Marriott application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Upscale extended-stay with full kitchens, evening socials where required, and longer-stay housekeeping. Confirm standards in current Marriott FDD.",
    footprint: "Broad global Residence Inn footprint. Confirm live counts from Marriott disclosures.",
    insight: "Residence Inn fits owners with durable weekly demand who can fund upscale kitchens with Bonvoy; fails when capital only supports TownePlace midscale or when sponsors invent fee percentages.",
    standards: [
      { title: "Upscale Kitchen Suites", body: "Full kitchens and suite standards must clear Residence Inn expectations." },
      { title: "Extended-Stay Programming", body: "Social hours and pantry cadence differ from transient select-service." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Weekly Upscale Extended-Stay", body: "Markets where Residence Inn ADR clears IC versus TownePlace / Homewood comps." },
      { title: "Relocation & Project Premium", body: "Corporate longer-stay demand needing residential quality." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott FDD exhibits." },
      { title: "Sibling Clarity Versus TownePlace / Homewood", body: "Diligence versus TownePlace and Homewood Suites before IC." },
    ],
    gallery: gallery("Residence Inn", "https://www.marriott.com/en-us/hotels/travel/residence-inn-hotels/"),
  }),

  recn59UtkyyoYwzSz: pack({
    brandId: "recn59UtkyyoYwzSz",
    brandName: "Marriott Hotels",
    liveBrandName: "Marriott Hotels",
    slug: "marriott-hotels",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD / management agreements for Marriott Hotels (full-service upscale)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/marriott-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Full-service upscale flagship: meetings, F&B, and Bonvoy for travelers and groups expecting classic Marriott Hotels service intensity.",
    commercialDifferentiator: "Versus Sheraton / Hilton Hotels full-service peers, Marriott Hotels is the core Bonvoy full-service flag; versus Courtyard select-service, it requires full-service capital and labor—model from current agreements, not select-service assumptions.",
    scenarios: [
      { title: "Urban Full-Service Flagship", body: "CBD and urban sites where meetings, F&B, and Bonvoy support upscale full-service rates." },
      { title: "Meetings & Group Compression", body: "Assets with ballroom and group capacity that can underwrite full-service labor." },
      { title: "Airport Full-Service Hub", body: "Major airport hotels needing full-service product and Bonvoy accounts." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Marriott Hotels is built for meetings and group compression with full-service F&B and Bonvoy." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban full-service can work when meetings and transient mix clear labor costs." },
      { title: "Airport & Transportation", fit: S, explanation: "Major airport full-service hubs fit when product and Bonvoy defend share." },
      { title: "Resort & Leisure Destination", fit: M, explanation: "Resort full-service can fit selected destinations—confirm F&B and leisure programming costs." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Marriott Hotels' core—consider Residence Inn." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient economics sit outside Marriott Hotels full-service." },
    ]),
    loyaltyProof: BONVOY_PROOF("Marriott Hotels"),
    feeNote: `Marriott Hotels application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Full-service upscale with meetings, F&B, and higher staffing intensity. Do not import select-service labor models. Confirm fees in current Marriott agreements / FDD exhibits.",
    footprint: "Global Marriott Hotels footprint across APAC, EMEA, CALA, and North America. Confirm live counts from Marriott disclosures.",
    insight: "Marriott Hotels fits owners who can fund full-service meetings and F&B with Bonvoy; wrong when capital only supports Courtyard select-service or when sponsors invent royalty tables.",
    standards: [
      { title: "Full-Service Upscale Product", body: "Rooms, meetings, and F&B must clear Marriott Hotels standards." },
      { title: "Meetings & F&B Labor Honesty", body: "Underwrite full-service labor—do not import select-service staffing." },
      { title: "Bonvoy Integration", body: "Bonvoy Elite delivery at full-service capability." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Meetings Flagship", body: "Sites where group and transient ADR clear full-service IC." },
      { title: "Airport Full-Service Hub", body: "Major airport demand with Bonvoy accounts." },
      { title: "Agreement Fee Gate", body: "Model only from current Marriott agreements / FDD exhibits." },
      { title: "Sibling Clarity Versus Courtyard / Sheraton", body: "Diligence versus Courtyard and Sheraton before IC." },
    ],
    gallery: gallery("Marriott Hotels", "https://www.marriott.com/en-us/hotels/travel/marriott-hotels/"),
  }),

  recg8HjT5Bky7NXeV: pack({
    brandId: "recg8HjT5Bky7NXeV",
    brandName: "Sheraton",
    liveBrandName: "Sheraton",
    slug: "sheraton",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD / management agreements for Sheraton (full-service upscale)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/sheraton-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Full-service upscale social & meetings: lobby energy, gatherings, and Bonvoy for travelers who want Sheraton community cues with Marriott commercial systems.",
    commercialDifferentiator: "Versus Marriott Hotels classic full-service, Sheraton emphasizes social lobby and gatherings; versus Four Points, it is full-service not upper-midscale conversion product—confirm agreements and PIP.",
    scenarios: [
      { title: "Urban Social Full-Service", body: "Urban sites where lobby social space and meetings support upscale rates with Bonvoy." },
      { title: "Meetings & Gatherings", body: "Assets with group capacity and Sheraton social programming." },
      { title: "Airport Full-Service With Social Lobby", body: "Airport hotels needing full-service product and Bonvoy." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Sheraton core demand includes meetings and gatherings with full-service product and Bonvoy." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban full-service works when social/meetings mix clears labor costs." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport full-service hubs fit when Sheraton product and Bonvoy defend share." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions can work when full-service PIP is funded—not Four Points-level capital alone." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Sheraton's core product." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Sheraton full-service." },
    ]),
    loyaltyProof: BONVOY_PROOF("Sheraton"),
    feeNote: `Sheraton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Full-service upscale with social lobby and meetings intensity. Confirm F&B and staffing in current Marriott agreements / FDD.",
    footprint: "Global Sheraton footprint across APAC, EMEA, CALA, and North America. Confirm live counts from Marriott disclosures.",
    insight: "Sheraton fits owners who can fund full-service social and meetings product with Bonvoy; fails when sponsors underwrite Four Points conversion capital or invent fee percentages.",
    standards: [
      { title: "Full-Service Social Product", body: "Lobby, rooms, and meetings must clear Sheraton standards." },
      { title: "Gatherings & F&B Labor", body: "Underwrite full-service labor honestly." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Gatherings Full-Service", body: "Sites where social and meetings ADR clear full-service IC." },
      { title: "Airport Full-Service Hub", body: "Airport demand with Bonvoy accounts." },
      { title: "Agreement Fee Gate", body: "Model only from current Marriott agreements / FDD exhibits." },
      { title: "Sibling Clarity Versus Marriott Hotels / Four Points", body: "Diligence versus Marriott Hotels and Four Points before IC." },
    ],
    gallery: gallery("Sheraton", "https://www.marriott.com/en-us/hotels/travel/sheraton-hotels/"),
  }),

  recWubG3rhiS1BaWi: pack({
    brandId: "recWubG3rhiS1BaWi",
    brandName: "Hilton Hotels & Resorts",
    liveBrandName: "Hilton Hotels & Resorts",
    slug: "hilton-hotels-resorts",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD / management agreements for Hilton Hotels & Resorts (full-service upscale)",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Full-service upscale flagship: meetings, F&B, and Hilton Honors for travelers and groups expecting classic Hilton Hotels service intensity.",
    commercialDifferentiator: "Versus DoubleTree conversion-friendly full-service, Hilton Hotels is the core flagship; versus Hilton Garden Inn select-service, it requires full-service capital—model from current Hilton agreements.",
    scenarios: [
      { title: "Urban Full-Service Flagship", body: "CBD sites where meetings, F&B, and Honors support upscale full-service rates." },
      { title: "Meetings & Group Compression", body: "Assets with ballroom capacity that can underwrite full-service labor." },
      { title: "Resort & Destination Full-Service", body: "Selected leisure destinations needing Hilton Hotels product and Honors." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Hilton Hotels is built for meetings and group compression with full-service F&B and Honors." },
      { title: "Resort & Leisure Destination", fit: S, explanation: "Resort full-service fits selected destinations when leisure programming is funded." },
      { title: "Airport & Transportation", fit: S, explanation: "Major airport full-service hubs fit when product and Honors defend share." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban full-service works when meetings and transient mix clear labor costs." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Hilton Hotels' core—consider Homewood / Home2." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Hilton Hotels full-service." },
    ]),
    loyaltyProof: HONORS_PROOF("Hilton Hotels"),
    feeNote: `Hilton Hotels & Resorts application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Full-service upscale with meetings, F&B, and higher staffing intensity. Do not import Garden Inn select-service labor models.",
    footprint: "Global Hilton Hotels & Resorts footprint across APAC, EMEA, CALA, and North America. Confirm live counts from Hilton disclosures.",
    insight: "Hilton Hotels fits owners who can fund full-service meetings and F&B with Honors; wrong when capital only supports Hilton Garden Inn or when sponsors invent royalty tables.",
    standards: [
      { title: "Full-Service Upscale Product", body: "Rooms, meetings, and F&B must clear Hilton Hotels standards." },
      { title: "Meetings & F&B Labor Honesty", body: "Underwrite full-service labor—do not import select-service staffing." },
      { title: "Honors Integration", body: "Hilton Honors Elite delivery at full-service capability." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Meetings Flagship", body: "Sites where group and transient ADR clear full-service IC." },
      { title: "Resort Full-Service", body: "Leisure destinations needing Hilton Hotels product." },
      { title: "Agreement Fee Gate", body: "Model only from current Hilton agreements / FDD exhibits." },
      { title: "Sibling Clarity Versus DoubleTree / Garden Inn", body: "Diligence versus DoubleTree and Hilton Garden Inn before IC." },
    ],
    gallery: gallery("Hilton Hotels", "https://www.hilton.com/en/hotels/"),
  }),

  rechVYWQ5ikRnr99B: pack({
    brandId: "rechVYWQ5ikRnr99B",
    brandName: "DoubleTree by Hilton",
    liveBrandName: "DoubleTree by Hilton",
    slug: "doubletree-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD for DoubleTree by Hilton (full-service upscale / conversion-friendly)",
    sources: {
      brandPage: "https://www.hilton.com/en/brands/doubletree/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Full-service upscale with warm-welcome cues and conversion flexibility: cookie welcome, meetings utility, and Hilton Honors for owners converting into Hilton full-service.",
    commercialDifferentiator: "Versus Hilton Hotels flagship, DoubleTree is often more conversion-friendly; versus Hilton Garden Inn select-service, it still carries fuller F&B and meetings expectations—confirm FDD and PIP.",
    scenarios: [
      { title: "Conversion Into Full-Service Upscale", body: "Existing hotels that can meet DoubleTree standards with funded PIP." },
      { title: "Suburban Meetings Full-Service", body: "Suburban sites with meetings capacity and Honors demand." },
      { title: "Airport Full-Service Conversion", body: "Airport hotels converting into DoubleTree with Honors." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "DoubleTree is frequently conversion-oriented—strong when full-service PIP is funded with Honors." },
      { title: "Meetings & Group Compression", fit: S, explanation: "Meetings utility fits when ballroom and F&B capacity exist." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport full-service conversions fit when product and Honors defend share." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban mix is a core DoubleTree use case." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not DoubleTree's core product." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside DoubleTree full-service." },
    ]),
    loyaltyProof: HONORS_PROOF("DoubleTree"),
    feeNote: `DoubleTree by Hilton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Full-service upscale with meetings and F&B intensity. Confirm staffing in current Hilton FDD—do not import Garden Inn labor models.",
    footprint: "Global DoubleTree footprint with conversion emphasis. Confirm live counts from Hilton disclosures.",
    insight: "DoubleTree fits owners converting into Hilton full-service with Honors; fails when capital only supports Garden Inn select-service or when sponsors invent fee percentages.",
    standards: [
      { title: "Full-Service Upscale Product", body: "Rooms, meetings, and F&B must clear DoubleTree standards." },
      { title: "Conversion PIP Honesty", body: "PIP must match full-service—not select-service-only capital." },
      { title: "Honors Integration", body: "Hilton Honors delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Conversion Full-Service", body: "Assets where DoubleTree PIP clears IC versus select-service comps." },
      { title: "Suburban Meetings", body: "Suburban meetings demand with Honors." },
      { title: "FDD Fee Gate", body: "Model only from current Hilton FDD exhibits." },
      { title: "Sibling Clarity Versus Hilton Hotels / Garden Inn", body: "Diligence versus Hilton Hotels and Hilton Garden Inn before IC." },
    ],
    gallery: gallery("DoubleTree", "https://www.hilton.com/en/brands/doubletree/"),
  }),

  rectRvOWQPaL6FkzZ: pack({
    brandId: "rectRvOWQPaL6FkzZ",
    brandName: "Hampton by Hilton",
    liveBrandName: "Hampton by Hilton",
    slug: "hampton-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD for Hampton by Hilton (upper-midscale limited-service)",
    sources: {
      brandPage: "https://www.hilton.com/en/brands/hampton/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Upper-midscale limited-service: free hot breakfast, clean consistency, and Hilton Honors for travelers seeking reliability without select-service F&B capital.",
    commercialDifferentiator: "Versus Hilton Garden Inn select-service, Hampton keeps leaner limited-service economics; versus Fairfield / Tru midscale peers, confirm local ADR and FDD—not brand-family assumptions.",
    scenarios: [
      { title: "Suburban Upper-Midscale Limited-Service", body: "Suburban corridors where breakfast and Honors support upper-midscale rates." },
      { title: "Airport & Highway Consistency", body: "Airport and interstate sites needing Hilton distribution without select-service capital." },
      { title: "Conversion Into Upper-Midscale Limited-Service", body: "Conversions that can fund Hampton PIP and breakfast standards." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Hampton is built for highway and corridor transient with breakfast and Honors." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport-edge sites fit when upper-midscale limited-service and Honors defend share." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban business/leisure mix is a core Hampton use case." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work when PIP funds upper-midscale guest rooms and breakfast." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Hampton's core—consider Home2 / Homewood." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Hampton limited-service." },
    ]),
    loyaltyProof: HONORS_PROOF("Hampton"),
    feeNote: `Hampton by Hilton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Upper-midscale limited-service with hot breakfast and leaner staffing than Garden Inn. Confirm standards in current Hilton FDD.",
    footprint: "Broad North American Hampton footprint with selective international presence. Confirm live counts from Hilton disclosures.",
    insight: "Hampton fits owners who want Honors upper-midscale limited-service consistency; fails when sponsors underwrite Garden Inn select-service capital or invent royalty percentages.",
    standards: [
      { title: "Upper-Midscale Limited-Service Product", body: "Rooms, breakfast, and public space must clear Hampton standards." },
      { title: "Conversion / PIP Discipline", body: "PIP must match upper-midscale—not economy-only scopes." },
      { title: "Honors Integration", body: "Hilton Honors delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Corridor Upper-Midscale Limited-Service", body: "Sites where Hampton ADR clears IC above midscale comps." },
      { title: "Airport Limited-Service", body: "Airport assets needing Honors without select-service capital." },
      { title: "FDD Fee Gate", body: "Model only from current Hilton FDD exhibits." },
      { title: "Sibling Clarity Versus Garden Inn / Fairfield", body: "Diligence versus Hilton Garden Inn and Fairfield before IC." },
    ],
    gallery: gallery("Hampton", "https://www.hilton.com/en/brands/hampton/"),
  }),

  recrvdAjRlXxPvPPF: pack({
    brandId: "recrvdAjRlXxPvPPF",
    brandName: "Hilton Garden Inn",
    liveBrandName: "Hilton Garden Inn",
    slug: "hilton-garden-inn",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD for Hilton Garden Inn (upper-midscale select-service)",
    sources: {
      brandPage: "https://www.hilton.com/en/brands/hilton-garden-inn/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Upper-midscale select-service: evening F&B, flexible meetings pods, and Hilton Honors for travelers needing more than limited-service without full-service capital.",
    commercialDifferentiator: "Versus Hampton limited-service, Garden Inn sells select-service F&B and meetings utility; versus Courtyard (Marriott), it rides Honors rather than Bonvoy—confirm FDD and local comps.",
    scenarios: [
      { title: "Suburban Select-Service", body: "Suburban corridors where evening F&B and Honors support select-service rates." },
      { title: "Airport Select-Service Hub", body: "Airport sites needing select-service product and Honors." },
      { title: "Small Meetings Utility", body: "Assets with flexible meeting space that can underwrite select-service labor." },
    ],
    demandScenarios: demand([
      { title: "Suburban Business & Leisure", fit: S, explanation: "Hilton Garden Inn core demand is suburban business/leisure with select-service F&B and Honors." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport select-service hubs fit when product and Honors defend share." },
      { title: "Meetings & Group Compression", fit: M, explanation: "Small meetings utility fits—do not underwrite like Hilton Hotels ballrooms." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions work when select-service PIP and F&B are funded." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Garden Inn's core—consider Homewood / Home2." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Hilton Garden Inn select-service." },
    ]),
    loyaltyProof: HONORS_PROOF("Hilton Garden Inn"),
    feeNote: `Hilton Garden Inn application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Upper-midscale select-service with evening F&B and higher labor than Hampton. Confirm standards in current Hilton FDD.",
    footprint: "Broad North American Hilton Garden Inn footprint with selective international presence. Confirm live counts from Hilton disclosures.",
    insight: "Garden Inn fits owners who can fund select-service F&B with Honors; fails when capital only supports Hampton limited-service or when sponsors invent fee percentages.",
    standards: [
      { title: "Select-Service Product", body: "Rooms, evening F&B, and meetings pods must clear Garden Inn standards." },
      { title: "Select-Service Labor Honesty", body: "Underwrite F&B labor—do not import Hampton limited-service staffing." },
      { title: "Honors Integration", body: "Hilton Honors delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Suburban Select-Service", body: "Sites where Garden Inn ADR clears IC versus Hampton comps." },
      { title: "Airport Select-Service", body: "Airport demand needing Honors select-service." },
      { title: "FDD Fee Gate", body: "Model only from current Hilton FDD exhibits." },
      { title: "Sibling Clarity Versus Hampton / Courtyard", body: "Diligence versus Hampton and Courtyard before IC." },
    ],
    gallery: gallery("Hilton Garden Inn", "https://www.hilton.com/en/brands/hilton-garden-inn/"),
  }),

  reccZ4zV6wMav7a2i: pack({
    brandId: "reccZ4zV6wMav7a2i",
    brandName: "Home2 Suites by Hilton",
    liveBrandName: "Home2 Suites by Hilton",
    slug: "home2-suites-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD for Home2 Suites (midscale extended-stay)",
    sources: {
      brandPage: "https://www.hilton.com/en/brands/home2-suites/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Midscale extended-stay: studio suites with kitchens, flexible public space, and Hilton Honors for guests staying longer than a typical transient night.",
    commercialDifferentiator: "Versus Homewood Suites upscale extended-stay, Home2 is midscale weekly economics; versus TownePlace (Marriott), it rides Honors rather than Bonvoy—confirm FDD and local weekly ADR.",
    scenarios: [
      { title: "Midscale Extended-Stay Suites", body: "Suburban markets where weekly demand supports midscale kitchens and Honors." },
      { title: "Corporate Project Housing", body: "Project and relocation guests needing suites without Homewood rates." },
      { title: "Conversion Into Midscale Extended-Stay", body: "Conversions that can fund kitchen studios and Home2 PIP." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Home2 Suites core demand is midscale extended-stay corporate and project housing with Honors." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban corridors with weekly share fit when kitchens and Honors defend rates." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets work when midscale weekly ADR clears Home2 fees." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when kitchen-studio PIP is funded." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large meetings are not the Home2 thesis." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Home2 midscale extended-stay." },
    ]),
    loyaltyProof: HONORS_PROOF("Home2 Suites"),
    feeNote: `Home2 Suites by Hilton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Midscale extended-stay with kitchen housekeeping and longer stays. Confirm breakfast and suite standards in current Hilton FDD.",
    footprint: "Broad North American Home2 Suites footprint. Confirm live counts from Hilton disclosures.",
    insight: "Home2 fits owners with weekly demand who can fund midscale kitchens with Honors; wrong when capital only supports Tru transient midscale or when guests expect Homewood upscale product without funding it.",
    standards: [
      { title: "Midscale Kitchen Studios", body: "Suite and kitchen standards must clear Home2 expectations." },
      { title: "Extended-Stay Ops", body: "Housekeeping cadence differs from transient midscale." },
      { title: "Honors Integration", body: "Hilton Honors delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Weekly Midscale Extended-Stay", body: "Markets where Home2 ADR clears IC versus local extended-stay comps." },
      { title: "Project & Relocation Mix", body: "Corporate project housing with kitchen needs." },
      { title: "FDD Fee Gate", body: "Model only from current Hilton FDD exhibits." },
      { title: "Sibling Clarity Versus Homewood / TownePlace", body: "Diligence versus Homewood and TownePlace before IC." },
    ],
    gallery: gallery("Home2 Suites", "https://www.hilton.com/en/brands/home2-suites/"),
  }),

  recZjYI4nYflGHFNR: pack({
    brandId: "recZjYI4nYflGHFNR",
    brandName: "Homewood Suites by Hilton",
    liveBrandName: "Homewood Suites by Hilton",
    slug: "homewood-suites-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD for Homewood Suites (upscale extended-stay)",
    sources: {
      brandPage: "https://www.hilton.com/en/brands/homewood-suites/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Upscale extended-stay: full kitchens, evening socials, and Hilton Honors for guests staying weeks with higher service expectations than midscale extended-stay.",
    commercialDifferentiator: "Versus Home2 midscale extended-stay, Homewood sells upscale residential product; versus Residence Inn (Marriott), it rides Honors rather than Bonvoy—confirm FDD and local weekly ADR.",
    scenarios: [
      { title: "Upscale Extended-Stay Suites", body: "Markets where weekly demand supports upscale kitchens and Honors." },
      { title: "Corporate Relocation Premium", body: "Longer-stay guests needing residential quality above Home2." },
      { title: "Suburban Upscale Weekly Demand", body: "Suburban corridors with material weekly share and Honors accounts." },
    ],
    demandScenarios: demand([
      { title: "Extended-Stay Corporate", fit: S, explanation: "Homewood Suites core demand is upscale extended-stay corporate, relocation, and project housing with Honors." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban weekly mix fits when residential suites and Honors defend rates." },
      { title: "Airport & Transportation", fit: M, explanation: "Airport-edge extended-stay can work when weekly share is durable." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when upscale kitchen-suite PIP is funded." },
      { title: "Highway & Interstate Transient", fit: L, explanation: "Pure highway overnight transient is not the Homewood thesis." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship meetings hotels sit outside Homewood extended-stay." },
    ]),
    loyaltyProof: HONORS_PROOF("Homewood Suites"),
    feeNote: `Homewood Suites by Hilton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Upscale extended-stay with full kitchens, evening socials, and longer-stay housekeeping. Confirm standards in current Hilton FDD.",
    footprint: "Broad North American Homewood Suites footprint with selective international presence. Confirm live counts from Hilton disclosures.",
    insight: "Homewood fits owners with durable weekly demand who can fund upscale kitchens with Honors; fails when capital only supports Home2 midscale or when sponsors invent fee percentages.",
    standards: [
      { title: "Upscale Kitchen Suites", body: "Full kitchens and suite standards must clear Homewood expectations." },
      { title: "Extended-Stay Programming", body: "Evening socials and pantry cadence differ from transient select-service." },
      { title: "Honors Integration", body: "Hilton Honors delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Weekly Upscale Extended-Stay", body: "Markets where Homewood ADR clears IC versus Home2 / Residence Inn comps." },
      { title: "Relocation & Project Premium", body: "Corporate longer-stay demand needing residential quality." },
      { title: "FDD Fee Gate", body: "Model only from current Hilton FDD exhibits." },
      { title: "Sibling Clarity Versus Home2 / Residence Inn", body: "Diligence versus Home2 and Residence Inn before IC." },
    ],
    gallery: gallery("Homewood Suites", "https://www.hilton.com/en/brands/homewood-suites/"),
  }),

  recfv66er4Ch2vJDO: pack({
    brandId: "recfv66er4Ch2vJDO",
    brandName: "Spark by Hilton",
    liveBrandName: "Spark by Hilton",
    slug: "spark-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD for Spark by Hilton (midscale conversion / value)",
    sources: {
      brandPage: "https://www.hilton.com/en/brands/spark/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Midscale conversion-friendly value: efficient rooms and Hilton Honors for owners converting existing hotels into a Hilton midscale flag without Hampton upper-midscale capital.",
    commercialDifferentiator: "Versus Hampton upper-midscale limited-service, Spark targets leaner midscale / conversion economics; versus Tru efficient midscale, confirm prototype and FDD differences before IC.",
    scenarios: [
      { title: "Conversion Into Midscale Value", body: "Existing hotels that can meet Spark standards with capital-efficient PIP." },
      { title: "Secondary-Market Midscale Flag", body: "Secondary markets needing Honors distribution at midscale capital." },
      { title: "Highway & Corridor Value", body: "Corridor sites where Spark and Honors compete on value." },
    ],
    demandScenarios: demand([
      { title: "Conversion & Repositioning", fit: S, explanation: "Spark is conversion-oriented—strong when midscale PIP clears with Honors." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets fit when midscale ADR supports Spark fees." },
      { title: "Highway & Interstate Transient", fit: S, explanation: "Highway value transient fits Spark's midscale thesis." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban mix works when product matches local midscale comps." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Spark's core—consider Home2." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Spark midscale conversion." },
    ]),
    loyaltyProof: HONORS_PROOF("Spark"),
    feeNote: `Spark by Hilton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Midscale limited-service / conversion product with leaner intensity than Hampton. Confirm standards in current Hilton FDD—do not invent fee tables.",
    footprint: "Growing Hilton Spark conversion footprint. Confirm live hotels and pipeline from Hilton disclosures only.",
    insight: "Spark fits owners converting into Hilton midscale with Honors at leaner capital; fails when sponsors underwrite Hampton upper-midscale fees or invent royalty percentages.",
    standards: [
      { title: "Midscale Conversion Product", body: "Rooms and public space must clear Spark standards." },
      { title: "Capital-Efficient PIP", body: "PIP should stay midscale conversion—not Hampton overbuild." },
      { title: "Honors Integration", body: "Hilton Honors delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Conversion Midscale Value", body: "Assets where Spark PIP clears IC versus unflagged midscale comps." },
      { title: "Secondary-Market Honors Flag", body: "Secondary markets needing Honors distribution." },
      { title: "FDD Fee Gate", body: "Model only from current Hilton FDD exhibits." },
      { title: "Sibling Clarity Versus Hampton / Tru", body: "Diligence versus Hampton and Tru before IC." },
    ],
    gallery: gallery("Spark", "https://www.hilton.com/en/brands/spark/"),
  }),

  recJLiMTv4W8VgO9L: pack({
    brandId: "recJLiMTv4W8VgO9L",
    brandName: "Tru by Hilton",
    liveBrandName: "Tru by Hilton",
    slug: "tru-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton FDD for Tru by Hilton (midscale efficient / social lobby)",
    sources: {
      brandPage: "https://www.hilton.com/en/brands/tru/",
      development: "https://www.hilton.com/en/development/",
      honors: "https://www.hilton.com/en/hilton-honors/",
    },
    positioning: "Midscale efficient product: compact rooms, social lobby energy, and Hilton Honors for travelers seeking modern midscale without upper-midscale Hampton capital.",
    commercialDifferentiator: "Versus Hampton upper-midscale limited-service, Tru is leaner midscale with social public space; versus Spark conversion midscale, confirm prototype differences and FDD before IC.",
    scenarios: [
      { title: "Efficient Midscale With Social Lobby", body: "Sites where compact rooms and social lobby support midscale rates with Honors." },
      { title: "Highway & Suburban Value", body: "Corridor and suburban sites needing modern midscale Honors distribution." },
      { title: "New-Build Efficient Midscale", body: "New builds optimized for Tru prototype efficiency." },
    ],
    demandScenarios: demand([
      { title: "Highway & Interstate Transient", fit: S, explanation: "Tru fits highway and corridor transient with efficient midscale product and Honors." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban mix works when social lobby and compact rooms match demand." },
      { title: "Regional & Secondary Markets", fit: S, explanation: "Secondary markets fit when midscale ADR clears Tru fees." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Some conversions work—confirm prototype fit versus Spark conversion thesis." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Tru's core—consider Home2." },
      { title: "Luxury Urban Flagship", fit: N, explanation: "Luxury urban flagship sits outside Tru efficient midscale." },
    ]),
    loyaltyProof: HONORS_PROOF("Tru"),
    feeNote: `Tru by Hilton application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Efficient midscale with compact rooms and social lobby. Confirm breakfast and staffing in current Hilton FDD—do not import Hampton upper-midscale labor.",
    footprint: "Broad North American Tru footprint with selective expansion. Confirm live counts from Hilton disclosures.",
    insight: "Tru fits owners who want Honors midscale efficiency with social lobby cues; fails when sponsors underwrite Hampton fees or invent royalty percentages.",
    standards: [
      { title: "Efficient Midscale Prototype", body: "Compact rooms and social lobby must clear Tru standards." },
      { title: "Lean Operating Model", body: "Staffing stays midscale efficient—not Hampton upper-midscale overbuild." },
      { title: "Honors Integration", body: "Hilton Honors delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Efficient Midscale Corridor", body: "Sites where Tru ADR clears IC versus midscale comps." },
      { title: "Social Lobby Value Mix", body: "Demand that values lobby energy with Honors." },
      { title: "FDD Fee Gate", body: "Model only from current Hilton FDD exhibits." },
      { title: "Sibling Clarity Versus Hampton / Spark", body: "Diligence versus Hampton and Spark before IC." },
    ],
    gallery: gallery("Tru", "https://www.hilton.com/en/brands/tru/"),
  }),

  recP9SqDootMrzaU1: pack({
    brandId: "recP9SqDootMrzaU1",
    brandName: "Hyatt Regency",
    liveBrandName: "Hyatt Regency",
    slug: "hyatt-regency",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    feeGeography: "Confirm current Hyatt FDD / management agreements for Hyatt Regency (full-service upscale)",
    sources: {
      brandPage: "https://www.hyatt.com/hyatt-regency",
      development: "https://www.hyatt.com/development",
      worldOfHyatt: "https://www.hyatt.com/world-of-hyatt",
    },
    positioning: "Full-service upscale meetings & gatherings: atriums, ballrooms, and World of Hyatt for groups and travelers expecting classic Regency service intensity.",
    commercialDifferentiator: "Versus Grand Hyatt / Park Hyatt luxury peers, Regency is upscale full-service meetings; versus Hyatt Place select-service, it requires full-service capital—model from current Hyatt agreements.",
    scenarios: [
      { title: "Urban Meetings Full-Service", body: "CBD sites where ballrooms, atriums, and World of Hyatt support upscale rates." },
      { title: "Airport Full-Service Hub", body: "Major airport hotels needing full-service product and Hyatt accounts." },
      { title: "Suburban Convention Corridor", body: "Suburban convention corridors with group capacity." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Hyatt Regency is built for meetings and group compression with full-service F&B and World of Hyatt." },
      { title: "Airport & Transportation", fit: S, explanation: "Major airport full-service hubs fit when product and World of Hyatt defend share." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban full-service works when meetings and transient mix clear labor costs." },
      { title: "Resort & Leisure Destination", fit: M, explanation: "Selected leisure destinations can fit—confirm F&B and leisure programming costs." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Regency's core—consider Hyatt House." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Hyatt Regency full-service." },
    ]),
    loyaltyProof: HYATT_PROOF("Hyatt Regency"),
    feeNote: `Hyatt Regency application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Full-service upscale with meetings, atriums, and higher staffing intensity. Do not import Hyatt Place select-service labor models.",
    footprint: "Global Hyatt Regency footprint across APAC, EMEA, CALA, and North America. Confirm live counts from Hyatt disclosures.",
    insight: "Hyatt Regency fits owners who can fund full-service meetings with World of Hyatt; wrong when capital only supports Hyatt Place or when sponsors invent royalty tables.",
    standards: [
      { title: "Full-Service Upscale Product", body: "Rooms, meetings, and F&B must clear Hyatt Regency standards." },
      { title: "Meetings & F&B Labor Honesty", body: "Underwrite full-service labor—do not import select-service staffing." },
      { title: "World of Hyatt Integration", body: "World of Hyatt Elite delivery at full-service capability." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Meetings Flagship", body: "Sites where group and transient ADR clear full-service IC." },
      { title: "Airport Full-Service Hub", body: "Major airport demand with World of Hyatt accounts." },
      { title: "Agreement Fee Gate", body: "Model only from current Hyatt agreements / FDD exhibits." },
      { title: "Sibling Clarity Versus Hyatt Place / Grand Hyatt", body: "Diligence versus Hyatt Place and Grand Hyatt before IC." },
    ],
    gallery: gallery("Hyatt Regency", "https://www.hyatt.com/hyatt-regency"),
  }),

  rec4Mga6ejz3L1M3P: pack({
    brandId: "rec4Mga6ejz3L1M3P",
    brandName: "Thompson Hotels",
    liveBrandName: "Thompson Hotels",
    slug: "thompson-hotels",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    feeGeography: "Confirm current Hyatt franchise / affiliation disclosure for Thompson Hotels (lifestyle upscale)",
    sources: {
      brandPage: "https://www.hyatt.com/thompson-hotels",
      development: "https://www.hyatt.com/development",
      worldOfHyatt: "https://www.hyatt.com/world-of-hyatt",
    },
    positioning: "Lifestyle upscale: design-forward public space, destination F&B cues, and World of Hyatt for travelers seeking place and design over classic meetings atriums.",
    commercialDifferentiator: "Versus Hyatt Regency meetings full-service, Thompson is lifestyle/design-led; versus Hyatt Centric explorer lifestyle, Thompson often reads more design-hotel / destination F&B—confirm disclosure and standards.",
    scenarios: [
      { title: "Lifestyle Urban Design Hotels", body: "Walkable urban sites where design and destination F&B lift lifestyle rates with World of Hyatt." },
      { title: "Destination Leisure & Weekend Mix", body: "Markets where weekend leisure and local discovery drive mix." },
      { title: "Conversion Into Lifestyle Upscale", body: "Distinctive assets that can sustain Thompson design standards." },
    ],
    demandScenarios: demand([
      { title: "Lifestyle Neighborhood Urban", fit: S, explanation: "Thompson is strongest in lifestyle urban neighborhoods where design and place narrative lift rates with World of Hyatt." },
      { title: "Resort & Leisure Destination", fit: S, explanation: "Selected leisure destinations fit when design and F&B programming are funded." },
      { title: "Suburban Business & Leisure", fit: M, explanation: "Suburban lifestyle can work selectively—confirm local comps versus Regency meetings product." },
      { title: "Conversion & Repositioning", fit: M, explanation: "Conversions succeed when design authorship and F&B can be sustained." },
      { title: "Meetings & Group Compression", fit: L, explanation: "Large ballroom meetings are not Thompson's core thesis—do not underwrite like Hyatt Regency." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Thompson lifestyle upscale." },
    ]),
    loyaltyProof: HYATT_PROOF("Thompson"),
    feeNote: `Thompson Hotels application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Lifestyle upscale with design-led public space and destination F&B intensity. Confirm staffing in current Hyatt disclosures—do not import Regency atrium labor models blindly.",
    footprint: "Selective Thompson Hotels footprint in lifestyle urban and leisure markets. Confirm live counts from Hyatt disclosures.",
    insight: "Thompson fits owners who can sustain design and destination F&B with World of Hyatt; fails when sponsors underwrite Hyatt Regency meetings economics or invent fee percentages.",
    standards: [
      { title: "Lifestyle Design Product", body: "Design authorship and public space must clear Thompson expectations." },
      { title: "Destination F&B Honesty", body: "Underwrite F&B labor for lifestyle programming." },
      { title: "World of Hyatt Integration", body: "World of Hyatt delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Lifestyle Urban Design", body: "Sites where design ADR clears IC versus classic full-service comps." },
      { title: "Weekend Leisure Mix", body: "Leisure demand that values place and design with World of Hyatt." },
      { title: "Disclosure Fee Gate", body: "Model only from current Hyatt disclosure exhibits." },
      { title: "Sibling Clarity Versus Hyatt Regency / Centric", body: "Diligence versus Hyatt Regency and Hyatt Centric before IC." },
    ],
    gallery: gallery("Thompson", "https://www.hyatt.com/thompson-hotels"),
  }),

  rec50qkCj6yt9fMPg: pack({
    brandId: "rec50qkCj6yt9fMPg",
    brandName: "Delta Hotels by Marriott",
    liveBrandName: "Delta Hotels by Marriott",
    slug: "delta-hotels-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott FDD for Delta Hotels by Marriott (upscale full-service / productive / conversion-friendly)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/travel/delta-hotels/",
      development: "https://hotel-development.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning: "Upscale productive full-service: efficient meetings, thoughtful F&B, and Bonvoy for travelers and groups who want upscale utility with conversion-friendly pathways.",
    commercialDifferentiator: "Versus Marriott Hotels classic full-service, Delta often emphasizes productive efficiency and conversion; versus Four Points upper-midscale, Delta sits higher as upscale full-service—confirm FDD and PIP.",
    scenarios: [
      { title: "Productive Upscale Full-Service", body: "Urban and suburban sites where efficient meetings and Bonvoy support upscale rates." },
      { title: "Conversion Into Upscale Full-Service", body: "Existing hotels that can meet Delta standards with funded PIP." },
      { title: "Airport & Suburban Meetings Utility", body: "Airport and suburban assets needing productive meetings with Bonvoy." },
    ],
    demandScenarios: demand([
      { title: "Meetings & Group Compression", fit: S, explanation: "Delta Hotels supports productive meetings compression with upscale full-service product and Bonvoy." },
      { title: "Conversion & Repositioning", fit: S, explanation: "Delta is frequently conversion-friendly—strong when upscale full-service PIP is funded." },
      { title: "Airport & Transportation", fit: S, explanation: "Airport upscale hubs fit when product and Bonvoy defend share." },
      { title: "Suburban Business & Leisure", fit: S, explanation: "Suburban productive mix is a core Delta use case." },
      { title: "Extended-Stay Corporate", fit: L, explanation: "Extended-stay corporate is not Delta's core—consider Residence Inn." },
      { title: "Highway & Interstate Transient", fit: N, explanation: "Highway limited-service transient sits outside Delta upscale full-service." },
    ]),
    loyaltyProof: BONVOY_PROOF("Delta Hotels"),
    feeNote: `Delta Hotels by Marriott application, royalty, marketing, loyalty, and PIP — ${FDD}`,
    operatingModel: "Upscale full-service with productive meetings and F&B intensity. Confirm staffing in current Marriott FDD—do not import Four Points upper-midscale labor models.",
    footprint: "Growing Delta Hotels footprint with conversion emphasis across North America and selective international markets. Confirm live counts from Marriott disclosures.",
    insight: "Delta fits owners converting into Marriott upscale full-service with Bonvoy and productive meetings; fails when capital only supports Four Points or when sponsors invent royalty percentages.",
    standards: [
      { title: "Upscale Full-Service Product", body: "Rooms, meetings, and F&B must clear Delta Hotels standards." },
      { title: "Conversion PIP Honesty", body: "PIP must match upscale full-service—not upper-midscale-only capital." },
      { title: "Bonvoy Integration", body: "Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Productive Upscale Meetings", body: "Sites where meetings ADR clears upscale full-service IC." },
      { title: "Conversion Upscale Full-Service", body: "Assets where Delta PIP clears IC versus Four Points comps." },
      { title: "FDD Fee Gate", body: "Model only from current Marriott FDD exhibits." },
      { title: "Sibling Clarity Versus Marriott Hotels / Four Points", body: "Diligence versus Marriott Hotels and Four Points before IC." },
    ],
    gallery: gallery("Delta Hotels", "https://www.marriott.com/en-us/hotels/travel/delta-hotels/"),
  }),
});

export function listBatch9() {
  return BATCH_9_IDS.map((id) => BATCH_9_PACKS_BY_ID[id]).filter(Boolean);
}

export function getBatch9Pack(brandId) {
  return BATCH_9_PACKS_BY_ID[brandId] || null;
}
