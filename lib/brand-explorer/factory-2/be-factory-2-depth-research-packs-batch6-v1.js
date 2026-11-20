/**
 * Factory 2.0 Batch 6 depth research packs.
 * All IDs are live EXISTING_CANONICAL Brand Basics (Draft/Under Review).
 * Fee amounts never invented — management-heavy brands use disclosure language.
 */
import {
  BONVOY_PROOF,
  HONORS_PROOF,
  HYATT_PROOF,
  IHG_PROOF,
} from "./be-factory-2-depth-loyalty-proofs-v1.js";

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    batchTag: "batch-6",
    ...base,
  });
}

export const FACTORY_2_BATCH_6_V1 = "FACTORY_2_BATCH_6_V1";

export const BATCH_6_IDS = Object.freeze([
  "recX9RJcmafZxWdXx", // Ritz-Carlton
  "recYsUrz4Po7Tg48T", // St. Regis
  "rec66ikTdLlT49Vkh", // Edition
  "recpKmYbyFr8ScGTi", // Luxury Collection
  "recmj7cNIUbYPKVJl", // Renaissance
  "recJUualJaqReVHLO", // Signia
  "reczor3qV3qAXFydD", // Alila
  "recumPNYZZkNdbTXF", // Hyatt House
  "recC3w95Hgi9SzXzD", // InterContinental
  "rec4ckn09CLLmppCe", // Six Senses
]);

export const BATCH_6_PACKS_BY_ID = Object.freeze({
  recX9RJcmafZxWdXx: pack({
    brandId: "recX9RJcmafZxWdXx",
    brandName: "The Ritz-Carlton",
    liveBrandName: "Ritz-Carlton",
    slug: "ritz-carlton",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography:
      "Confirm current Marriott luxury management / development disclosure for The Ritz-Carlton — do not invent franchise percentages for management-led deals",
    sources: {
      brandPage: "https://www.ritzcarlton.com/",
      marriott: "https://www.marriott.com/en-us/hotels/brands/ritz-carlton/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — The Ritz-Carlton (public)",
    },
    positioning:
      "Iconic luxury: legendary service cues, elevated F&B, and Marriott Bonvoy distribution for travelers seeking classic luxury hotels and resorts.",
    commercialDifferentiator:
      "Versus St. Regis bespoke luxury and EDITION lifestyle luxury, Ritz-Carlton sells iconic classic luxury; versus JW Marriott, it sits higher on the Bonvoy luxury ladder—confirm management vs franchise path before IC.",
    scenarios: [
      {
        title: "Gateway Iconic Luxury",
        body: "Primary gateways where classic luxury product and Bonvoy accounts can underwrite Ritz-Carlton rates.",
      },
      {
        title: "Luxury Resort Destinations",
        body: "Destination resorts that can fund spa, F&B, and service intensity matching Ritz-Carlton standards.",
      },
      {
        title: "Management-Led Luxury Development",
        body: "Developments where Marriott management economics—not assumed franchise fee tables—govern the deal structure.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Ritz-Carlton"),
    feeNote:
      "The Ritz-Carlton deals are frequently management-led. Application, management fees, program contribution, and Bonvoy assessments — confirm current Marriott luxury disclosure exhibits. Do not invent franchise royalty percentages. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury full-service / resort with high service intensity. Many assets operate under Marriott management rather than pure franchise—underwrite the actual agreement path.",
    footprint:
      "Global Ritz-Carlton urban and resort footprint. Confirm live counts and pipeline from Marriott disclosures across APAC, EMEA, CALA / LATAM, and North America.",
    insight:
      "Ritz-Carlton fits sponsors who can fund iconic luxury delivery and accept management economics; wrong when underwriting like upper-upscale franchise light.",
    standards: [
      {
        title: "Iconic Luxury Product",
        body: "Public space, F&B, and finishes must read iconic luxury—not upper-upscale renovations.",
      },
      {
        title: "Service Intensity",
        body: "Service staffing and training must match Ritz-Carlton expectations continuously.",
      },
      {
        title: "Bonvoy Luxury Delivery",
        body: "Marriott Bonvoy Elite recognition and systems integration required.",
      },
    ],
    valueOwnerScenarios: [
      {
        title: "Gateway Classic Luxury",
        body: "Urban icons where Bonvoy luxury demand clears IC with management economics disclosed.",
      },
      {
        title: "Resort Luxury",
        body: "Destination resorts that can sustain spa and F&B intensity.",
      },
      {
        title: "Management Path Discipline",
        body: "Model only from current management / fee exhibits—never peer rumor franchise tables.",
      },
      {
        title: "Sibling Clarity Versus St. Regis / JW",
        body: "Diligence versus St. Regis and JW Marriott before assuming identical owner use case.",
      },
    ],
    gallery: [
      { title: "Ritz-Carlton — Luxury Public Space", body: "Iconic luxury lobby · confirm property photography before publication", source: "https://www.ritzcarlton.com/" },
      { title: "Ritz-Carlton — Guest Room", body: "Luxury guest-room finishes typical of the brand", source: "https://www.ritzcarlton.com/" },
      { title: "Ritz-Carlton — F&B", body: "Elevated F&B supporting iconic luxury contribution", source: "https://www.ritzcarlton.com/" },
      { title: "Ritz-Carlton — Spa / Wellness", body: "Spa and wellness where offered", source: "https://www.ritzcarlton.com/" },
      { title: "Ritz-Carlton — Exterior", body: "Exterior / arrival presence that reads iconic luxury", source: "https://www.ritzcarlton.com/" },
      { title: "Ritz-Carlton — Events", body: "Events and ballroom product where designed", source: "https://www.ritzcarlton.com/" },
    ],
  }),

  recYsUrz4Po7Tg48T: pack({
    brandId: "recYsUrz4Po7Tg48T",
    brandName: "St. Regis Hotels & Resorts",
    liveBrandName: "St. Regis",
    slug: "st-regis",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography:
      "Confirm current Marriott luxury management / development disclosure for St. Regis — do not invent franchise percentages",
    sources: {
      brandPage: "https://st-regis.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — St. Regis (public)",
    },
    positioning:
      "Bespoke luxury: butler-service heritage cues, refined public space, and Marriott Bonvoy distribution for travelers seeking personalized luxury.",
    commercialDifferentiator:
      "Versus Ritz-Carlton iconic classic luxury, St. Regis sells bespoke / butler-forward luxury; versus EDITION, it is quieter refinement rather than nightlife lifestyle.",
    scenarios: [
      { title: "Urban Bespoke Luxury", body: "Gateway assets where personalized luxury and Bonvoy can support St. Regis rates." },
      { title: "Resort Bespoke Luxury", body: "Resort assets that can fund butler-forward service and elevated F&B." },
      { title: "Management-Led Luxury", body: "Developments where management economics—not assumed franchise tables—govern IC." },
    ],
    loyaltyProof: BONVOY_PROOF("St. Regis"),
    feeNote:
      "St. Regis deals are frequently management-led. Management fees, program contribution, and Bonvoy assessments — confirm current Marriott luxury disclosure exhibits. Do not invent franchise royalties. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury with personalized service intensity (butler cues where offered). Often Marriott-managed—underwrite the actual agreement path.",
    footprint:
      "Select global St. Regis footprint. Confirm live hotels and pipeline from Marriott disclosures.",
    insight:
      "St. Regis fits sponsors who can fund bespoke luxury service; wrong when capital or staffing ambition is upper-upscale only.",
    standards: [
      { title: "Bespoke Luxury Delivery", body: "Service and public space must clear St. Regis personalized luxury standards." },
      { title: "F&B Elevation", body: "F&B intensity must match luxury expectations." },
      { title: "Bonvoy Integration", body: "Full Marriott Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Bespoke Luxury", body: "Gateways where personalized luxury and Bonvoy clear IC." },
      { title: "Resort Personalized Luxury", body: "Resorts that can sustain butler-forward service." },
      { title: "Management Economics Gate", body: "Model only from disclosed management / fee exhibits." },
      { title: "Sibling Clarity Versus Ritz / EDITION", body: "Diligence versus Ritz-Carlton and EDITION before IC." },
    ],
    gallery: [
      { title: "St. Regis — Luxury Public Space", body: "Bespoke luxury public space · confirm property photography before publication", source: "https://st-regis.marriott.com/" },
      { title: "St. Regis — Guest Room", body: "Luxury guest-room product typical of St. Regis", source: "https://st-regis.marriott.com/" },
      { title: "St. Regis — F&B", body: "Elevated F&B supporting bespoke luxury stays", source: "https://st-regis.marriott.com/" },
      { title: "St. Regis — Butler / Service Cue", body: "Personalized service cues where offered", source: "https://st-regis.marriott.com/" },
      { title: "St. Regis — Exterior", body: "Exterior / arrival presence in market", source: "https://st-regis.marriott.com/" },
      { title: "St. Regis — Spa / Leisure", body: "Spa or leisure where offered", source: "https://st-regis.marriott.com/" },
    ],
  }),

  rec66ikTdLlT49Vkh: pack({
    brandId: "rec66ikTdLlT49Vkh",
    brandName: "EDITION Hotels",
    liveBrandName: "Edition",
    slug: "edition-hotels",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography:
      "Confirm current Marriott luxury lifestyle management / development disclosure for EDITION — do not invent franchise percentages",
    sources: {
      brandPage: "https://www.editionhotels.com/",
      marriott: "https://www.marriott.com/en-us/hotels/brands/edition/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — EDITION (public)",
    },
    positioning:
      "Luxury lifestyle: design-forward public space, destination F&B and nightlife energy with Marriott Bonvoy distribution.",
    commercialDifferentiator:
      "Versus W Hotels lifestyle luxury, EDITION is a tighter curated luxury lifestyle set; versus Ritz-Carlton classic luxury, it sells nightlife and design intensity—confirm management economics.",
    scenarios: [
      { title: "Urban Lifestyle Luxury", body: "Lifestyle districts where EDITION design and Bonvoy can support luxury lifestyle rates." },
      { title: "Destination Entertainment Luxury", body: "Destination assets that can sustain nightlife and F&B intensity." },
      { title: "Management-Led Lifestyle Luxury", body: "Developments underwritten from management disclosures—not assumed franchise tables." },
    ],
    loyaltyProof: BONVOY_PROOF("EDITION"),
    feeNote:
      "EDITION deals are frequently management-led. Management fees, program contribution, and Bonvoy assessments — confirm current Marriott luxury lifestyle disclosure exhibits. Do not invent franchise royalties. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury lifestyle with intensive F&B and entertainment. Operating costs typically exceed classic full-service when nightlife runs hard; often Marriott-managed.",
    footprint:
      "Select global EDITION footprint. Confirm live hotels and pipeline from Marriott disclosures.",
    insight:
      "EDITION creates value when design and entertainment capital are real; fails when underwritten like quiet classic luxury or select-service.",
    standards: [
      { title: "Design & Social Public Space", body: "Public space must feel EDITION-authentic—not generic luxury renovations." },
      { title: "F&B & Nightlife Intensity", body: "Underwrite operator capability and licensing for destination F&B / nightlife." },
      { title: "Bonvoy Luxury Delivery", body: "Bonvoy Elite delivery must not break inside lifestyle product." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Lifestyle Luxury", body: "Lifestyle districts with Bonvoy luxury demand and design capital." },
      { title: "Entertainment Destination", body: "Assets that can sustain nightlife contribution." },
      { title: "Management Path Gate", body: "Model only from disclosed management / fee exhibits." },
      { title: "Sibling Clarity Versus W / Ritz", body: "Diligence versus W Hotels and Ritz-Carlton before IC." },
    ],
    gallery: [
      { title: "EDITION — Social Public Space", body: "Lifestyle luxury lobby · confirm property photography before publication", source: "https://www.editionhotels.com/" },
      { title: "EDITION — Guest Room", body: "Design-forward guest-room product", source: "https://www.editionhotels.com/" },
      { title: "EDITION — F&B / Bar", body: "Destination F&B and bar programming", source: "https://www.editionhotels.com/" },
      { title: "EDITION — Pool / Exterior Energy", body: "Pool or exterior cues where offered", source: "https://www.editionhotels.com/" },
      { title: "EDITION — Events / Social", body: "Social and entertainment spaces", source: "https://www.editionhotels.com/" },
      { title: "EDITION — Arrival", body: "Arrival experience that reads lifestyle luxury", source: "https://www.editionhotels.com/" },
    ],
  }),

  recpKmYbyFr8ScGTi: pack({
    brandId: "recpKmYbyFr8ScGTi",
    brandName: "The Luxury Collection",
    liveBrandName: "Luxury Collection",
    slug: "luxury-collection",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography:
      "Confirm current Marriott luxury soft-collection / affiliation disclosure for The Luxury Collection — do not invent undisclosed fees",
    sources: {
      brandPage: "https://luxury-collection.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — The Luxury Collection (public)",
    },
    positioning:
      "Luxury soft collection: distinctive historic and contemporary luxury hotels with Marriott Bonvoy distribution—each property keeps independent character.",
    commercialDifferentiator:
      "Versus Autograph individuality at upper-upscale, Luxury Collection sits on the luxury ladder; versus Ritz hard luxury brands, it preserves property character inside Bonvoy soft collection.",
    scenarios: [
      { title: "Historic Luxury Character", body: "Assets with strong heritage character needing Bonvoy luxury without hard Ritz prototyping." },
      { title: "Destination Luxury Soft Collection", body: "Destination luxury hotels where individuality is the product." },
      { title: "Affiliation Into Luxury Collection", body: "Affiliations that can meet luxury standards while preserving property character." },
    ],
    loyaltyProof: BONVOY_PROOF("Luxury Collection"),
    feeNote:
      "Luxury Collection affiliation / application, program fees, and Bonvoy assessments — confirm current Marriott soft-collection luxury disclosure exhibits. Do not invent franchise royalties from peer rumor. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury soft collection: property individuality with Marriott systems and Bonvoy. Capital remains luxury-band—soft collection is not a capital shortcut.",
    footprint:
      "Select global Luxury Collection portfolio. Confirm live hotels and pipeline from Marriott disclosures.",
    insight:
      "Luxury Collection works when individuality and luxury delivery are both real; fails when sponsors want Bonvoy without luxury capital.",
    standards: [
      { title: "Luxury Individuality", body: "Property character must remain distinct while clearing Luxury Collection standards." },
      { title: "Service & F&B Luxury", body: "Service and F&B must read luxury." },
      { title: "Bonvoy Integration", body: "Full Marriott Bonvoy delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Character Luxury Affiliation", body: "Independent luxury assets needing Bonvoy without hard-brand prototyping." },
      { title: "Destination Soft Collection", body: "Destination hotels where individuality drives rate." },
      { title: "Standards Gate", body: "Affiliate only when luxury QA and PIP clear IC." },
      { title: "Sibling Clarity Versus Autograph / Ritz", body: "Diligence versus Autograph and Ritz-Carlton before IC." },
    ],
    gallery: [
      { title: "Luxury Collection — Distinctive Public Space", body: "Property-specific luxury public space · confirm photography before publication", source: "https://luxury-collection.marriott.com/" },
      { title: "Luxury Collection — Guest Room", body: "Individual luxury guest-room character", source: "https://luxury-collection.marriott.com/" },
      { title: "Luxury Collection — F&B", body: "Destination F&B supporting soft-collection luxury", source: "https://luxury-collection.marriott.com/" },
      { title: "Luxury Collection — Exterior / Arrival", body: "Arrival presence unique to each property", source: "https://luxury-collection.marriott.com/" },
      { title: "Luxury Collection — Heritage / Design Detail", body: "Heritage or design authorship detail", source: "https://luxury-collection.marriott.com/" },
      { title: "Luxury Collection — Wellness / Leisure", body: "Wellness or leisure cues where offered", source: "https://luxury-collection.marriott.com/" },
    ],
  }),

  recmj7cNIUbYPKVJl: pack({
    brandId: "recmj7cNIUbYPKVJl",
    brandName: "Renaissance Hotels",
    liveBrandName: "Renaissance",
    slug: "renaissance-hotels",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott franchise / development disclosure for Renaissance Hotels",
    sources: {
      brandPage: "https://renaissance-hotels.marriott.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — Renaissance (public)",
    },
    positioning:
      "Upper-upscale discovery: navigators programming, destination discovery cues, and Marriott Bonvoy distribution for curious travelers.",
    commercialDifferentiator:
      "Versus Le Méridien discovery, Renaissance is Marriott's navigator upper-upscale; versus Westin wellness, it sells discovery and social public space—confirm franchise vs management path.",
    scenarios: [
      { title: "Urban Discovery Upper-Upscale", body: "City assets where navigator programming and Bonvoy differentiate versus generic peers." },
      { title: "Gateway & Meetings Mix", body: "Gateways needing meetings utility with discovery brand story." },
      { title: "Conversion Into Renaissance", body: "Conversions that can fund public-space and F&B cues aligned with Renaissance standards." },
    ],
    loyaltyProof: BONVOY_PROOF("Renaissance"),
    feeNote:
      "Renaissance application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Upper-upscale full-service with discovery programming and Bonvoy systems. Often franchise-capable—still confirm the deal path; do not import luxury management fee assumptions.",
    footprint:
      "Global Renaissance footprint. Confirm openings and pipeline from Marriott disclosures across APAC, EMEA, CALA / LATAM, and North America.",
    insight:
      "Renaissance fits owners who want Bonvoy upper-upscale with discovery narrative—not luxury soft-collection or select-service capital light.",
    standards: [
      { title: "Discovery Public Space", body: "Public spaces should support navigator / discovery cues." },
      { title: "F&B Character", body: "F&B should feel place-aware within brand standards." },
      { title: "Bonvoy Integration", body: "Full Bonvoy enrollment and Elite delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Discovery", body: "City sites where discovery narrative and Bonvoy lift rates." },
      { title: "Gateway Utility With Story", body: "Gateways needing meetings utility plus brand story." },
      { title: "Conversion Standards", body: "Convert only when public-space PIP can carry discovery cues." },
      { title: "Sibling Clarity Versus Le Méridien / Westin", body: "Diligence versus Le Méridien and Westin before IC." },
    ],
    gallery: [
      { title: "Renaissance — Public Space", body: "Discovery-led public space · confirm property photography before publication", source: "https://renaissance-hotels.marriott.com/" },
      { title: "Renaissance — Guest Room", body: "Upper-upscale guest-room product", source: "https://renaissance-hotels.marriott.com/" },
      { title: "Renaissance — F&B", body: "Place-aware F&B supporting discovery positioning", source: "https://renaissance-hotels.marriott.com/" },
      { title: "Renaissance — Meetings", body: "Meetings product where designed", source: "https://renaissance-hotels.marriott.com/" },
      { title: "Renaissance — Exterior", body: "Exterior / arrival presence in market", source: "https://renaissance-hotels.marriott.com/" },
      { title: "Renaissance — Social / Navigator Cue", body: "Navigator / social programming cue", source: "https://renaissance-hotels.marriott.com/" },
    ],
  }),

  recJUualJaqReVHLO: pack({
    brandId: "recJUualJaqReVHLO",
    brandName: "Signia by Hilton",
    liveBrandName: "Signia by Hilton",
    slug: "signia-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography:
      "Confirm current Hilton development / management disclosure for Signia by Hilton — meetings-led upper-upscale; do not invent franchise tables",
    sources: {
      brandPage: "https://www.hilton.com/en/signia/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      development: "Hilton development materials — Signia by Hilton (public)",
    },
    positioning:
      "Meetings-led upper-upscale: grand public space, large meetings capacity, and Hilton Honors distribution for group and transient demand.",
    commercialDifferentiator:
      "Versus Conrad luxury, Signia is meetings-forward upper-upscale; versus Hilton Hotels mainstream, it is a higher meetings-led product—confirm management vs franchise path.",
    scenarios: [
      { title: "Gateway Meetings Hub", body: "Gateways where large meetings capacity and Honors accounts underwrite Signia." },
      { title: "Urban Convention Adjacent", body: "Convention-adjacent assets with grand public space and group demand." },
      { title: "Development Into Signia Standards", body: "Developments that can fund meetings PIP at Signia standards." },
    ],
    loyaltyProof: HONORS_PROOF("Signia"),
    feeNote:
      "Signia deals may be management-led or otherwise structured. Application, management / royalty, program fees, and Hilton Honors assessments — confirm current Hilton disclosure exhibits. Do not invent franchise percentages. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Full-service meetings intensity with elevated public space and Honors systems. Operating costs exceed select-service; confirm whether the deal is managed or franchised.",
    footprint:
      "Emerging Signia footprint focused on major meetings markets. Confirm live hotels and pipeline from Hilton disclosures.",
    insight:
      "Signia fits meetings-capable assets with Honors demand; wrong when sponsors want lifestyle luxury or select-service capital light.",
    standards: [
      { title: "Meetings Capacity & Public Space", body: "Ballroom, breakout, and lobby scale must clear Signia expectations." },
      { title: "Service Delivery", body: "Service staffing must match upper-upscale full-service." },
      { title: "Honors Integration", body: "Full Hilton Honors systems required." },
    ],
    valueOwnerScenarios: [
      { title: "Meetings-Led Gateway", body: "Gateways where group and Honors accounts clear IC." },
      { title: "Convention-Adjacent Urban", body: "Assets with grand public space next to convention demand." },
      { title: "PIP Reality", body: "Fund meetings and public-space PIP before assuming Signia rates." },
      { title: "Sibling Clarity Versus Conrad / Hilton", body: "Diligence versus Conrad and Hilton Hotels before IC." },
    ],
    gallery: [
      { title: "Signia — Grand Lobby", body: "Meetings-led public space · confirm property photography before publication", source: "https://www.hilton.com/en/signia/" },
      { title: "Signia — Guest Room", body: "Upper-upscale guest-room product", source: "https://www.hilton.com/en/signia/" },
      { title: "Signia — Ballroom / Meetings", body: "Large meetings and ballroom capacity", source: "https://www.hilton.com/en/signia/" },
      { title: "Signia — F&B", body: "Full-service F&B supporting group and transient mix", source: "https://www.hilton.com/en/signia/" },
      { title: "Signia — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hilton.com/en/signia/" },
      { title: "Signia — Breakout / Pre-Function", body: "Breakout and pre-function spaces typical of Signia", source: "https://www.hilton.com/en/signia/" },
    ],
  }),

  reczor3qV3qAXFydD: pack({
    brandId: "reczor3qV3qAXFydD",
    brandName: "Alila",
    liveBrandName: "Alila",
    slug: "alila",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    feeGeography:
      "Confirm current Hyatt development / management disclosure for Alila — lifestyle luxury; do not invent franchise percentages",
    sources: {
      brandPage: "https://www.hyatt.com/alila",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt development materials — Alila (public)",
    },
    positioning:
      "Lifestyle luxury: design-led resorts and urban hideaways with World of Hyatt distribution for travelers seeking place-specific luxury.",
    commercialDifferentiator:
      "Versus Andaz lifestyle luxury, Alila leans resort / hideaway design authorship; versus Park Hyatt intimacy, Alila is more design-destination—confirm management economics.",
    scenarios: [
      { title: "Design-Led Resort Luxury", body: "Destination resorts where design authorship and World of Hyatt support Alila rates." },
      { title: "Urban Hideaway Luxury", body: "Urban hideaways that can fund design intensity without Grand Hyatt meetings scale." },
      { title: "Management-Led Lifestyle Luxury", body: "Developments underwritten from Hyatt disclosures—not assumed franchise tables." },
    ],
    loyaltyProof: HYATT_PROOF("Alila"),
    feeNote:
      "Alila deals are often management / affiliation structured. Management fees, program fees, and World of Hyatt assessments — confirm current Hyatt disclosure exhibits. Do not invent franchise royalties. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Lifestyle luxury with design-intensive product and World of Hyatt systems. Do not import Hyatt House / Hyatt Place select-service economics.",
    footprint:
      "Select Alila footprint skewed to resort and design destinations, with urban presence. Confirm live hotels from Hyatt disclosures across APAC, EMEA, CALA / LATAM, and North America.",
    insight:
      "Alila fits sponsors who can fund design-destination luxury; wrong when underwriting like extended-stay select-service.",
    standards: [
      { title: "Design Authorship", body: "Design must feel place-led within Alila standards." },
      { title: "Service & F&B Luxury", body: "Service and F&B must clear lifestyle luxury expectations." },
      { title: "World of Hyatt Integration", body: "Full World of Hyatt delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Resort Design Luxury", body: "Destination resorts with World of Hyatt demand and design capital." },
      { title: "Urban Hideaway", body: "Urban hideaways without convention-scale ambition." },
      { title: "Management Economics Gate", body: "Model only from disclosed Hyatt fee / management exhibits." },
      { title: "Sibling Clarity Versus Andaz / Park Hyatt", body: "Diligence versus Andaz and Park Hyatt before IC." },
    ],
    gallery: [
      { title: "Alila — Design Public Space", body: "Lifestyle luxury public space · confirm property photography before publication", source: "https://www.hyatt.com/alila" },
      { title: "Alila — Guest Room", body: "Design-led guest-room product", source: "https://www.hyatt.com/alila" },
      { title: "Alila — F&B", body: "Place-led F&B supporting lifestyle luxury", source: "https://www.hyatt.com/alila" },
      { title: "Alila — Pool / Resort Cue", body: "Pool or resort programming where offered", source: "https://www.hyatt.com/alila" },
      { title: "Alila — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hyatt.com/alila" },
      { title: "Alila — Design Detail", body: "Design authorship detail typical of Alila", source: "https://www.hyatt.com/alila" },
    ],
  }),

  recumPNYZZkNdbTXF: pack({
    brandId: "recumPNYZZkNdbTXF",
    brandName: "Hyatt House",
    liveBrandName: "Hyatt House",
    slug: "hyatt-house",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    feeGeography: "Confirm current Hyatt franchise / development disclosure for Hyatt House (extended-stay select-service)",
    sources: {
      brandPage: "https://www.hyatt.com/hyatt-house",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt development materials — Hyatt House (public)",
    },
    positioning:
      "Upper-midscale extended-stay: residential suites, kitchenettes, and World of Hyatt distribution for longer-stay transient demand.",
    commercialDifferentiator:
      "Versus Hyatt Place transient select-service, Hyatt House sells extended-stay residential utility; versus Alila / Park Hyatt luxury, it is deliberately capital-light select-service—do not import luxury fee or staffing assumptions.",
    scenarios: [
      { title: "Corporate Extended-Stay Corridor", body: "Suburban corridors where longer-stay corporate demand and World of Hyatt underwrite Hyatt House." },
      { title: "Medical & Project Stay Markets", body: "Markets with medical, project, and relocation demand needing suite product." },
      { title: "New-Build Prototype Efficiency", body: "New-builds optimized for Hyatt House prototypes against local extended-stay comps." },
    ],
    loyaltyProof: HYATT_PROOF("Hyatt House"),
    feeNote:
      "Hyatt House application, royalty, program fees, and World of Hyatt assessments — confirm current Hyatt select-service / extended-stay disclosure exhibits. Do not apply luxury-brand or management-only fee exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Extended-stay select-service with leaner staffing than full-service Hyatt brands, residential suite product, and World of Hyatt systems. Operating model must not copy Alila or Park Hyatt luxury intensity.",
    footprint:
      "Large Hyatt House footprint across North America with selective international presence. Confirm live counts from Hyatt disclosures.",
    insight:
      "Hyatt House is for efficient extended-stay economics with World of Hyatt—wrong when sponsors underwrite luxury lifestyle capital.",
    standards: [
      { title: "Extended-Stay Prototype Discipline", body: "Suites and residential public space must clear Hyatt House standards without luxury overbuilding." },
      { title: "Lean Operating Model", body: "Staffing stays select-service / extended-stay—do not import luxury operating costs." },
      { title: "World of Hyatt Integration", body: "World of Hyatt enrollment and Elite delivery within select-service capability." },
    ],
    valueOwnerScenarios: [
      { title: "Corporate Extended-Stay Efficiency", body: "Corridor sites where suite product and World of Hyatt clear IC." },
      { title: "Medical / Project Stay", body: "Markets with longer-stay demand needing residential utility." },
      { title: "New-Build Cost Discipline", body: "New-builds optimized for Hyatt House prototypes." },
      { title: "Sibling Clarity Versus Luxury Hyatt", body: "Do not confuse with Alila, Park Hyatt, or Andaz economics." },
    ],
    gallery: [
      { title: "Hyatt House — Suite / Living Space", body: "Extended-stay suite product · confirm property photography before publication", source: "https://www.hyatt.com/hyatt-house" },
      { title: "Hyatt House — Kitchenette", body: "In-suite kitchenette utility typical of Hyatt House", source: "https://www.hyatt.com/hyatt-house" },
      { title: "Hyatt House — Public Space / H Bar", body: "Residential public space supporting longer stays", source: "https://www.hyatt.com/hyatt-house" },
      { title: "Hyatt House — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hyatt.com/hyatt-house" },
      { title: "Hyatt House — Fitness / Amenity", body: "Fitness and amenity cues typical of extended-stay", source: "https://www.hyatt.com/hyatt-house" },
      { title: "Hyatt House — Laundry / Residential Cue", body: "Residential utility cues supporting longer stays", source: "https://www.hyatt.com/hyatt-house" },
    ],
  }),

  recC3w95Hgi9SzXzD: pack({
    brandId: "recC3w95Hgi9SzXzD",
    brandName: "InterContinental Hotels & Resorts",
    liveBrandName: "InterContinental",
    slug: "intercontinental",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    feeGeography:
      "Confirm current IHG franchise / management disclosure for InterContinental — luxury / upper-upscale; do not invent undisclosed fees",
    sources: {
      brandPage: "https://www.ihg.com/intercontinental",
      development: "https://development.ihg.com/brand/intercontinental",
      oneRewards: "https://www.ihg.com/onerewards",
    },
    positioning:
      "Luxury / upper-upscale full-service: grand public space, elevated F&B, and IHG One Rewards distribution for global travelers.",
    commercialDifferentiator:
      "Versus Six Senses wellness luxury, InterContinental sells classic global luxury / upper-upscale; versus Hotel Indigo lifestyle, it is fuller-service—confirm franchise vs management path.",
    scenarios: [
      { title: "Gateway Global Luxury", body: "Primary gateways where InterContinental product and One Rewards underwrite rates." },
      { title: "Resort & Destination Full-Service", body: "Destination assets that can fund elevated F&B and service." },
      { title: "Conversion / Development Into IC Standards", body: "Conversions and new-builds that can clear InterContinental public-space and service standards." },
    ],
    loyaltyProof: IHG_PROOF("InterContinental"),
    feeNote:
      "InterContinental may operate under franchise or management structures. Application, royalty / management fees, program fees, and One Rewards assessments — confirm current IHG disclosure exhibits. Do not invent undisclosed percentages. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Full-service luxury / upper-upscale with elevated F&B and One Rewards systems. Confirm whether the deal is franchised or managed before modeling fees.",
    footprint:
      "Global InterContinental footprint across urban and resort markets. Confirm live counts and pipeline from IHG disclosures across APAC, EMEA, CALA / LATAM, and North America.",
    insight:
      "InterContinental fits sponsors who can fund global full-service luxury / upper-upscale; wrong when underwriting like midscale limited-service.",
    standards: [
      { title: "Grand Public Space & F&B", body: "Lobby and F&B must clear InterContinental expectations." },
      { title: "Service Delivery", body: "Service staffing must match luxury / upper-upscale full-service." },
      { title: "One Rewards Integration", body: "Full IHG One Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Gateway Global Full-Service", body: "Gateways where One Rewards and IC product clear IC." },
      { title: "Destination Full-Service", body: "Resorts and destinations with elevated F&B capacity." },
      { title: "Agreement Path Discipline", body: "Confirm franchise vs management before modeling fees." },
      { title: "Sibling Clarity Versus Six Senses / Indigo", body: "Diligence versus Six Senses and Hotel Indigo before IC." },
    ],
    gallery: [
      { title: "InterContinental — Grand Public Space", body: "Luxury / upper-upscale lobby · confirm property photography before publication", source: "https://www.ihg.com/intercontinental" },
      { title: "InterContinental — Guest Room", body: "Elevated guest-room product", source: "https://www.ihg.com/intercontinental" },
      { title: "InterContinental — F&B", body: "Elevated F&B supporting full-service contribution", source: "https://www.ihg.com/intercontinental" },
      { title: "InterContinental — Meetings / Events", body: "Meetings capacity where designed", source: "https://www.ihg.com/intercontinental" },
      { title: "InterContinental — Exterior", body: "Exterior / arrival presence in market", source: "https://www.ihg.com/intercontinental" },
      { title: "InterContinental — Spa / Leisure", body: "Spa or leisure cues where offered", source: "https://www.ihg.com/intercontinental" },
    ],
  }),

  rec4ckn09CLLmppCe: pack({
    brandId: "rec4ckn09CLLmppCe",
    brandName: "Six Senses Hotels Resorts Spas",
    liveBrandName: "Six Senses Hotels Resorts Spas",
    slug: "six-senses",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    feeGeography:
      "Confirm current IHG / Six Senses management / development disclosure — wellness luxury often management-led; do not invent franchise percentages",
    sources: {
      brandPage: "https://www.sixsenses.com/",
      ihg: "https://www.ihg.com/sixsenses",
      oneRewards: "https://www.ihg.com/onerewards",
      development: "IHG development materials — Six Senses (public)",
    },
    positioning:
      "Wellness luxury: sustainability-forward resorts and spas with IHG One Rewards distribution for travelers seeking transformative wellness stays.",
    commercialDifferentiator:
      "Versus InterContinental classic luxury, Six Senses sells wellness and sustainability luxury; versus Regent, confirm sibling clarity—often management-led, not franchise-table underwriting.",
    scenarios: [
      { title: "Destination Wellness Resort", body: "Resort assets that can fund spa, wellness programming, and sustainability standards." },
      { title: "Wellness Hideaway Luxury", body: "Hideaway assets where wellness is the product and One Rewards expands distribution." },
      { title: "Management-Led Wellness Luxury", body: "Developments underwritten from management disclosures—not assumed franchise tables." },
    ],
    loyaltyProof: IHG_PROOF("Six Senses"),
    feeNote:
      "Six Senses deals are frequently management-led. Management fees, program fees, and One Rewards assessments — confirm current IHG / Six Senses disclosure exhibits. Do not invent franchise royalties. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Wellness luxury with spa intensity, sustainability standards, and One Rewards systems. Operating costs sit in the luxury band; do not import midscale franchise assumptions.",
    footprint:
      "Select global Six Senses footprint skewed to wellness resorts. Confirm live hotels and pipeline from IHG / Six Senses disclosures across APAC, EMEA, CALA / LATAM, and North America.",
    insight:
      "Six Senses fits sponsors who can fund wellness luxury delivery; wrong when underwriting like conventional upper-upscale or inventing franchise fees for management deals.",
    standards: [
      { title: "Wellness & Spa Intensity", body: "Spa and wellness programming must clear Six Senses expectations." },
      { title: "Sustainability Standards", body: "Sustainability and design standards are product—not marketing garnish." },
      { title: "One Rewards Integration", body: "IHG One Rewards delivery required within Six Senses product." },
    ],
    valueOwnerScenarios: [
      { title: "Wellness Resort Destination", body: "Resorts where spa intensity and One Rewards clear IC." },
      { title: "Hideaway Wellness Luxury", body: "Hideaways that can sustain transformative wellness product." },
      { title: "Management Economics Gate", body: "Model only from disclosed management / fee exhibits." },
      { title: "Sibling Clarity Versus InterContinental", body: "Diligence versus InterContinental classic luxury before IC." },
    ],
    gallery: [
      { title: "Six Senses — Wellness Public Space", body: "Wellness luxury public space · confirm property photography before publication", source: "https://www.sixsenses.com/" },
      { title: "Six Senses — Guest Pavilion / Room", body: "Guest pavilion or room product typical of Six Senses", source: "https://www.sixsenses.com/" },
      { title: "Six Senses — Spa", body: "Spa product central to the brand promise", source: "https://www.sixsenses.com/" },
      { title: "Six Senses — Pool / Nature Cue", body: "Pool or nature integration where offered", source: "https://www.sixsenses.com/" },
      { title: "Six Senses — F&B", body: "Wellness-forward F&B supporting resort contribution", source: "https://www.sixsenses.com/" },
      { title: "Six Senses — Exterior / Arrival", body: "Exterior / arrival presence in market", source: "https://www.sixsenses.com/" },
    ],
  }),
});

export function listBatch6() {
  return BATCH_6_IDS.map((id) => BATCH_6_PACKS_BY_ID[id]).filter(Boolean);
}
