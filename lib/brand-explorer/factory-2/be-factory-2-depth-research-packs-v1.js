/**
 * Factory 2.0 depth research packs — named batches (free sources; $0 paid).
 * Fee amounts are never invented — disclosure confirmation language only.
 */
import {
  BATCH_6_IDS,
  BATCH_6_PACKS_BY_ID,
  listBatch6 as listBatch6FromModule,
} from "./be-factory-2-depth-research-packs-batch6-v1.js";
import {
  BATCH_7_IDS,
  BATCH_7_PACKS_BY_ID,
  listBatch7 as listBatch7FromModule,
} from "./be-factory-2-depth-research-packs-batch7-v1.js";
import {
  BATCH_8_IDS,
  BATCH_8_PACKS_BY_ID,
  listBatch8 as listBatch8FromModule,
} from "./be-factory-2-depth-research-packs-batch8-v1.js";
import {
  BATCH_9_IDS,
  BATCH_9_PACKS_BY_ID,
  listBatch9 as listBatch9FromModule,
} from "./be-factory-2-depth-research-packs-batch9-v1.js";
import {
  BATCH_10_IDS,
  BATCH_10_PACKS_BY_ID,
  listBatch10 as listBatch10FromModule,
} from "./be-factory-2-depth-research-packs-batch10-v1.js";
import {
  BATCH_11_IDS,
  BATCH_11_PACKS_BY_ID,
  listBatch11 as listBatch11FromModule,
} from "./be-factory-2-depth-research-packs-batch11-v1.js";
import {
  BATCH_12_IDS,
  BATCH_12_PACKS_BY_ID,
  listBatch12 as listBatch12FromModule,
} from "./be-factory-2-depth-research-packs-batch12-v1.js";
import {
  BONVOY_PROOF,
  IHG_PROOF,
  HONORS_PROOF,
  HYATT_PROOF,
  CHOICE_PROOF,
  WYNDHAM_PROOF,
  BWH_PROOF,
  SONESTA_PROOF,
  ACCOR_PROOF,
  MEMBERSHIP_NETWORK_PROOF,
} from "./be-factory-2-depth-loyalty-proofs-v1.js";

export { BATCH_6_IDS, listBatch6FromModule as listBatch6 };
export { BATCH_7_IDS, listBatch7FromModule as listBatch7 };
export { BATCH_8_IDS, listBatch8FromModule as listBatch8 };
export { BATCH_9_IDS, listBatch9FromModule as listBatch9 };
export { BATCH_10_IDS, listBatch10FromModule as listBatch10 };
export { BATCH_11_IDS, listBatch11FromModule as listBatch11 };
export { BATCH_12_IDS, listBatch12FromModule as listBatch12 };
export {
  BONVOY_PROOF,
  IHG_PROOF,
  HONORS_PROOF,
  HYATT_PROOF,
  CHOICE_PROOF,
  WYNDHAM_PROOF,
  BWH_PROOF,
  SONESTA_PROOF,
  ACCOR_PROOF,
  MEMBERSHIP_NETWORK_PROOF,
};
export const FACTORY_2_BATCH_6_V1 = "FACTORY_2_BATCH_6_V1";
export const FACTORY_2_BATCH_7_V1 = "FACTORY_2_BATCH_7_V1";
export const FACTORY_2_BATCH_8_V1 = "FACTORY_2_BATCH_8_V1";
export const FACTORY_2_BATCH_9_V1 = "FACTORY_2_BATCH_9_V1";
export const FACTORY_2_BATCH_10_V1 = "FACTORY_2_BATCH_10_V1";
export const FACTORY_2_BATCH_11_V1 = "FACTORY_2_BATCH_11_V1";
export const FACTORY_2_BATCH_12_V1 = "FACTORY_2_BATCH_12_V1";

export const FACTORY_2_NAMED_BATCH_FIVE_V1 = "FACTORY_2_NAMED_BATCH_FIVE_V1";
export const FACTORY_2_BATCH_3_V1 = "FACTORY_2_BATCH_3_V1";
export const FACTORY_2_BATCH_4_V1 = "FACTORY_2_BATCH_4_V1";
export const FACTORY_2_BATCH_5_V1 = "FACTORY_2_BATCH_5_V1";

export const BATCH_FIVE_IDS = Object.freeze([
  "rec9aZp7GHtzUEg0c", // AC Hotels
  "recahVIW4aCx0Ao84", // Moxy
  "recDwzv86TWnz2gGB", // Vignette
  "recsggfbKlJbjeRP9", // Canopy
  "recvvmiyReHhiKdoK", // EVEN
]);

export const BATCH_3_IDS = Object.freeze([
  "recwONQTqGU1jHCsM", // voco
  "reclt44apoi8co0e6", // Motto
  "recoEarnE8T6sDjZq", // avid
  "recCvV0PuZOi8c3hC", // Tribute Portfolio
  "recegXrqaPiSLGCIe", // Hotel Indigo
]);

export const BATCH_4_IDS = Object.freeze([
  "reccXxMHEh7NNRhIE", // Tapestry
  "recqiHq3GHKMj8Meo", // Tempo
  "rec02zPClpWUTCyXM", // Design Hotels
  "recJ1GZQpttX7qHgw", // Aloft
  "recCKuXCmGvxHPfb3", // Kimpton
  "recGv268Wda31PlSZ", // Bunkhouse
  "recNy2efMm4N1JtgC", // Hyatt Centric
  "rec6hye5H8zJmAGv3", // Courtyard
  "recIPuBC50fv13zRR", // Westin
  "recmGmiIqDtAsm01f", // Holiday Inn Express
]);

/** Existing Brand Basics (Draft / Under Review) — no new Basics creates. */
export const BATCH_5_IDS = Object.freeze([
  "recj5BCSpWqMEEBDg", // JW Marriott
  "recqBiokz5ePxbFTE", // W Hotels
  "recN6sas0Km17O2Yn", // Le Meridien
  "recXopdFodrD3IyIR", // Waldorf Astoria
  "rec90n1dSVVyoEOto", // Conrad
  "recUsZOxtqIpeA5ZW", // LXR
  "recswdE1mhwDvXtBi", // Park Hyatt
  "recGCWhkeEvsQxuez", // Grand Hyatt
  "recbtJoMub5wcankK", // Andaz
  "recgwxli5IsoPKCOY", // Hyatt Place
]);

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    ...base,
  });
}

export const DEPTH_PACKS_BY_BRAND_ID = Object.freeze({
  rec9aZp7GHtzUEg0c: pack({
    brandId: "rec9aZp7GHtzUEg0c",
    brandName: "AC Hotels by Marriott",
    liveBrandName: "AC Hotels by Marriott",
    slug: "ac-hotels-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott franchise / development disclosure for AC Hotels",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/brands/ac-hotels/",
      news: "https://news.marriott.com/brands/ac-hotels",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — AC Hotels by Marriott positioning (public)",
    },
    positioning:
      "European-inspired select-service lifestyle: contemporary design, residential public space, and efficient rooms for urban and airport-adjacent travelers.",
    commercialDifferentiator:
      "Versus hard limited-service boxes, AC sells design-led select-service with Bonvoy distribution; versus full-service lifestyle, it keeps a tighter capital and operating stack.",
    scenarios: [
      {
        title: "Urban Select-Service Lifestyle",
        body: "City and mixed-use sites where contemporary design and social lobby energy can lift select-service rates without full-service capital intensity.",
      },
      {
        title: "Airport & Corridor Modernization",
        body: "Airport-adjacent and corridor assets needing a modern lifestyle read versus conventional select-service competitors—underwrite design and F&B lite carefully.",
      },
      {
        title: "Conversion Within Marriott Soft Stack",
        body: "Conversions that need Marriott Bonvoy without jumping to soft-collection individuality (Autograph/Tribute) or luxury lifestyle capital.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("AC Hotels"),
    feeNote:
      "AC Hotels application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recahVIW4aCx0Ao84: pack({
    brandId: "recahVIW4aCx0Ao84",
    brandName: "Moxy Hotels",
    liveBrandName: "Moxy Hotels",
    slug: "moxy-hotels",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    feeGeography: "Confirm current Marriott franchise / development disclosure for Moxy",
    sources: {
      brandPage: "https://moxy-hotels.marriott.com/",
      news: "https://news.marriott.com/brands/moxy-hotels",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — Moxy Hotels positioning (public)",
    },
    positioning:
      "Accessible lifestyle select-service: playful social public space, efficient rooms, and nightlife-adjacent energy for younger and value-conscious travelers.",
    commercialDifferentiator:
      "Versus AC Hotels' European contemporary select-service, Moxy leans budget lifestyle intensity; versus W, it avoids luxury lifestyle capital while still riding Bonvoy.",
    scenarios: [
      {
        title: "Urban Lifestyle on a Select-Service Stack",
        body: "Dense urban sites where social lobby programming and accessible rates can outperform conventional limited-service without full-service F&B capital.",
      },
      {
        title: "Entertainment & Nightlife Corridors",
        body: "Districts where evening energy and social public space are the demand story—underwrite noise, F&B lite, and neighbor mix before assuming Moxy fit.",
      },
      {
        title: "Airport & Gateway Value Lifestyle",
        body: "Gateway and airport-adjacent assets seeking lifestyle differentiation at accessible price points versus AC, Aloft, or hard limited-service flags.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Moxy"),
    feeNote:
      "Moxy application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recDwzv86TWnz2gGB: pack({
    brandId: "recDwzv86TWnz2gGB",
    brandName: "Vignette Collection",
    liveBrandName: "Vignette Collection",
    slug: "vignette-collection",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    feeGeography: "Confirm current IHG development / franchise disclosure for Vignette Collection",
    sources: {
      brandPage: "https://www.ihg.com/vignettecollection/hotels/us/en/reservation",
      development: "https://development.ihg.com/brand/vignette-collection",
      oneRewards: "https://www.ihg.com/onerewards",
      fullBuild: "lib/partner-intelligence/brand-explorer-full-build-content-vignette-collection.js",
    },
    positioning:
      "IHG soft collection of independent-spirited hotels: individuality and local character with IHG One Rewards and commercial systems—without collapsing into a hard-brand prototype.",
    commercialDifferentiator:
      "Versus Curio/Autograph peers, Vignette is the IHG soft-collection path; versus Indigo, it prioritizes collection individuality over a more standardized boutique format.",
    scenarios: [
      {
        title: "Independent Character Conversion",
        body: "Distinctive independents that want IHG One Rewards distribution without erasing the hotel's individual identity into a hard IHG prototype.",
      },
      {
        title: "Landmark & Adaptive Reuse",
        body: "Heritage and adaptive-reuse assets where architecture and sense of place are the product—pair design capital with IHG commercial systems.",
      },
      {
        title: "Destination Lifestyle Soft Collection",
        body: "Urban and destination hotels where local narrative drives rate power and owners need soft-collection flexibility inside IHG.",
      },
    ],
    loyaltyProof: IHG_PROOF("Vignette"),
    feeNote:
      "Vignette Collection join/operate/change fees — confirm current IHG development or franchise disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recsggfbKlJbjeRP9: pack({
    brandId: "recsggfbKlJbjeRP9",
    brandName: "Canopy by Hilton",
    liveBrandName: "Canopy by Hilton",
    slug: "canopy-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    feeGeography: "Confirm current Hilton franchise disclosure for Canopy by Hilton",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/brands/canopy-by-hilton/",
      development: "https://www.hilton.com/en/corporate/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      news: "Hilton Canopy brand communications (public)",
    },
    positioning:
      "Neighborhood lifestyle: local design cues, welcoming public space, and destination-forward stays with Hilton Honors distribution.",
    commercialDifferentiator:
      "Versus Curio's soft-collection individuality, Canopy is a more defined lifestyle brand; versus Hotel Indigo peers, it rides Hilton Honors rather than IHG One Rewards.",
    scenarios: [
      {
        title: "Neighborhood Lifestyle Urban",
        body: "Walkable urban neighborhoods where local design, F&B, and place narrative can lift lifestyle rates with Hilton Honors behind the stay.",
      },
      {
        title: "Adaptive Reuse Lifestyle Conversion",
        body: "Conversions that can express neighborhood character without requiring soft-collection one-off standards—underwrite design PIP carefully.",
      },
      {
        title: "Leisure-Led Destination Weekends",
        body: "Markets where weekend leisure and local discovery drive mix—pair Honors midweek accounts with neighborhood leisure programming.",
      },
    ],
    loyaltyProof: HONORS_PROOF("Canopy"),
    feeNote:
      "Canopy by Hilton application, royalty, program fees, and Honors assessments — confirm current Hilton franchise disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recvvmiyReHhiKdoK: pack({
    brandId: "recvvmiyReHhiKdoK",
    brandName: "EVEN Hotels",
    liveBrandName: "Even Hotels",
    slug: "even-hotels",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    feeGeography: "Confirm current IHG development / franchise disclosure for EVEN Hotels",
    sources: {
      brandPage: "https://www.ihg.com/evenhotels/hotels/us/en/reservation",
      development: "https://development.ihg.com/",
      oneRewards: "https://www.ihg.com/onerewards",
      gallery: "fixtures/wave12-even-hotels-gallery-pool.json",
    },
    positioning:
      "Wellness-forward select-service: fitness-centric public space, healthier F&B cues, and efficient rooms for travelers who prioritize wellbeing routines on the road.",
    commercialDifferentiator:
      "Versus conventional select-service, EVEN sells wellness programming as the demand hook; versus full-service spa resorts, it keeps a tighter capital stack inside IHG One Rewards.",
    scenarios: [
      {
        title: "Urban Wellness Select-Service",
        body: "City sites where fitness-forward public space and healthier stay cues can differentiate versus standard select-service competitors.",
      },
      {
        title: "Corporate Corridor Health Positioning",
        body: "Midweek corporate corridors where wellness amenities support rate and loyalty mix—validate that the fitness product is real, not cosmetic.",
      },
      {
        title: "Conversion Into Wellness Lifestyle",
        body: "Select-service conversions that can fund credible fitness and F&B upgrades without jumping to full-service spa economics.",
      },
    ],
    loyaltyProof: IHG_PROOF("EVEN"),
    feeNote:
      "EVEN Hotels application, royalty, program fees, and One Rewards assessments — confirm current IHG disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  // --- Batch 3 ---
  recwONQTqGU1jHCsM: pack({
    brandId: "recwONQTqGU1jHCsM",
    brandName: "voco Hotels",
    liveBrandName: "Voco Hotels",
    slug: "voco-hotels",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    batchTag: "batch-3",
    feeGeography: "Confirm current IHG development / franchise disclosure for voco",
    sources: {
      brandPage: "https://www.ihg.com/voco/hotels/us/en/reservation",
      development: "https://development.ihg.com/brand/voco",
      oneRewards: "https://www.ihg.com/onerewards",
    },
    positioning:
      "Upscale lifestyle with local flavor: individual hotel character, social public space, and destination-forward stays inside IHG One Rewards.",
    commercialDifferentiator:
      "Versus Hotel Indigo's neighborhood boutique format, voco leans upscale lifestyle with more hotel-specific expression; versus soft collections, it remains a defined lifestyle brand rather than a one-off collection path.",
    scenarios: [
      {
        title: "Urban Upscale Lifestyle",
        body: "City assets where design, F&B, and local story can lift upscale lifestyle rates with IHG One Rewards behind the stay.",
      },
      {
        title: "Destination & Resort Lifestyle",
        body: "Leisure destinations where hotel individuality and place narrative drive rate power without jumping to luxury soft-collection capital.",
      },
      {
        title: "Conversion Into Defined Lifestyle",
        body: "Conversions that can fund credible lifestyle product and public space while staying inside a defined IHG lifestyle brand—not a soft-collection one-off.",
      },
    ],
    loyaltyProof: IHG_PROOF("voco"),
    feeNote:
      "voco application, royalty, program fees, and One Rewards assessments — confirm current IHG disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  reclt44apoi8co0e6: pack({
    brandId: "reclt44apoi8co0e6",
    brandName: "Motto by Hilton",
    liveBrandName: "Motto by Hilton",
    slug: "motto-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    batchTag: "batch-3",
    feeGeography: "Confirm current Hilton franchise disclosure for Motto by Hilton",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/brands/motto-by-hilton/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      development: "Hilton development materials — Motto by Hilton (public)",
    },
    positioning:
      "Urban micro-hotel lifestyle: efficient rooms, social lobby energy, and walkable neighborhood access with Hilton Honors distribution.",
    commercialDifferentiator:
      "Versus Canopy's neighborhood lifestyle full-service lean, Motto compresses the stay into a micro-hotel format; versus conventional select-service, it sells urban social density as the product.",
    scenarios: [
      {
        title: "Dense Urban Micro-Hotel",
        body: "High-cost urban sites where smaller keys and social public space can underwrite lifestyle rates with Hilton Honors demand.",
      },
      {
        title: "Transit & Walkable Corridors",
        body: "Walkable entertainment and transit corridors where guests trade room size for location and lobby energy.",
      },
      {
        title: "Adaptive Reuse Micro Footprint",
        body: "Narrow or adaptive-reuse footprints that cannot carry conventional key counts—validate Motto product standards before assuming fit.",
      },
    ],
    loyaltyProof: HONORS_PROOF("Motto"),
    feeNote:
      "Motto by Hilton application, royalty, program fees, and Honors assessments — confirm current Hilton franchise disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recoEarnE8T6sDjZq: pack({
    brandId: "recoEarnE8T6sDjZq",
    brandName: "avid hotels",
    liveBrandName: "avid hotels",
    slug: "avid-hotels",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    batchTag: "batch-3",
    feeGeography: "Confirm current IHG development / franchise disclosure for avid hotels",
    sources: {
      brandPage: "https://www.ihg.com/avidhotels/hotels/us/en/reservation",
      development: "https://development.ihg.com/brand/avid-hotels",
      oneRewards: "https://www.ihg.com/onerewards",
    },
    positioning:
      "Essential midscale: bright, efficient rooms and straightforward public space for value-conscious travelers inside IHG One Rewards.",
    commercialDifferentiator:
      "Versus Holiday Inn Express peers, avid sells a simpler essential midscale stack; versus upscale lifestyle brands, it keeps capital and operating intensity deliberately lean.",
    scenarios: [
      {
        title: "Corridor Essential Midscale",
        body: "Highway and suburban corridors where efficient rooms and IHG One Rewards can compete on value without lifestyle capital.",
      },
      {
        title: "Airport & Gateway Value",
        body: "Airport-adjacent value stays where clean product and loyalty distribution matter more than F&B depth.",
      },
      {
        title: "New-Build Efficient Prototypes",
        body: "New-build sites optimized for essential midscale prototypes—underwrite development cost discipline against local midscale competitors.",
      },
    ],
    loyaltyProof: IHG_PROOF("avid"),
    feeNote:
      "avid hotels application, royalty, program fees, and One Rewards assessments — confirm current IHG disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recCvV0PuZOi8c3hC: pack({
    brandId: "recCvV0PuZOi8c3hC",
    brandName: "Tribute Portfolio",
    liveBrandName: "Tribute Portfolio",
    slug: "tribute-portfolio",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-3",
    feeGeography: "Confirm current Marriott franchise / development disclosure for Tribute Portfolio",
    sources: {
      brandPage: "https://tribute-portfolio.marriott.com/",
      news: "https://news.marriott.com/brands/tribute-portfolio",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — Tribute Portfolio (public)",
    },
    positioning:
      "Independent-spirited soft collection: distinctive hotels with local character plus Marriott Bonvoy commercial systems—without collapsing into a hard-brand prototype.",
    commercialDifferentiator:
      "Versus Autograph's often more design-led upper-upscale to luxury-leaning character hotels, Tribute Portfolio spans independent upper-upscale conversions with soft-collection flexibility inside Bonvoy.",
    scenarios: [
      {
        title: "Independent Character Conversion",
        body: "Distinctive independents that want Marriott Bonvoy without erasing hotel individuality into a hard Marriott prototype.",
      },
      {
        title: "Landmark & Adaptive Reuse",
        body: "Heritage and adaptive-reuse assets where architecture and sense of place are the product—pair design capital with Bonvoy systems.",
      },
      {
        title: "Sibling Clarity Versus Autograph",
        body: "Assets that fit soft-collection economics but need diligence against Autograph and Design Hotels before assuming the same owner use case or fee path.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Tribute Portfolio"),
    feeNote:
      "Tribute Portfolio application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recegXrqaPiSLGCIe: pack({
    brandId: "recegXrqaPiSLGCIe",
    brandName: "Hotel Indigo",
    liveBrandName: "Hotel Indigo",
    slug: "hotel-indigo",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    batchTag: "batch-3",
    feeGeography: "Confirm current IHG development / franchise disclosure for Hotel Indigo",
    sources: {
      brandPage: "https://www.ihg.com/hotelindigo/hotels/us/en/reservation",
      development: "https://development.ihg.com/brand/hotel-indigo",
      oneRewards: "https://www.ihg.com/onerewards",
    },
    positioning:
      "Neighborhood boutique: local design cues, neighborhood storytelling, and upscale-lifestyle stays with IHG One Rewards distribution.",
    commercialDifferentiator:
      "Versus voco's upscale lifestyle expression, Indigo is the neighborhood boutique format; versus Canopy (Hilton), it rides IHG One Rewards rather than Hilton Honors.",
    scenarios: [
      {
        title: "Neighborhood Boutique Urban",
        body: "Walkable neighborhoods where local design, F&B, and place narrative can lift boutique rates with IHG One Rewards behind the stay.",
      },
      {
        title: "Adaptive Reuse Neighborhood Story",
        body: "Conversions that can express neighborhood character—underwrite design PIP and local storytelling so the Indigo promise is visible in product.",
      },
      {
        title: "Leisure-Led Destination Weekends",
        body: "Markets where weekend leisure and local discovery drive mix—pair One Rewards midweek accounts with neighborhood leisure programming.",
      },
    ],
    loyaltyProof: IHG_PROOF("Hotel Indigo"),
    feeNote:
      "Hotel Indigo application, royalty, program fees, and One Rewards assessments — confirm current IHG disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  // --- Batch 4 ---
  reccXxMHEh7NNRhIE: pack({
    brandId: "reccXxMHEh7NNRhIE",
    brandName: "Tapestry Collection by Hilton",
    liveBrandName: "Tapestry Collection by Hilton",
    slug: "tapestry-collection-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    batchTag: "batch-4",
    feeGeography: "Confirm current Hilton franchise disclosure for Tapestry Collection",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/brands/tapestry-collection-by-hilton/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      development: "Hilton development materials — Tapestry Collection (public)",
    },
    positioning:
      "Hilton soft collection of independent-spirited hotels: local character with Hilton Honors commercial systems—without collapsing into a hard-brand prototype.",
    commercialDifferentiator:
      "Versus Curio's often more upscale soft-collection positioning, Tapestry spans distinctive upper-upscale independents; versus Canopy, it remains a collection path rather than a defined lifestyle prototype.",
    scenarios: [
      {
        title: "Independent Character Conversion",
        body: "Distinctive independents that want Hilton Honors without erasing hotel individuality into a hard Hilton prototype.",
      },
      {
        title: "Landmark & Adaptive Reuse",
        body: "Heritage and adaptive-reuse assets where architecture and sense of place are the product—pair design capital with Honors systems.",
      },
      {
        title: "Sibling Clarity Versus Curio",
        body: "Assets that fit soft-collection economics but need diligence against Curio before assuming the same owner use case or fee path.",
      },
    ],
    loyaltyProof: HONORS_PROOF("Tapestry"),
    feeNote:
      "Tapestry Collection application, royalty, program fees, and Honors assessments — confirm current Hilton franchise disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recqiHq3GHKMj8Meo: pack({
    brandId: "recqiHq3GHKMj8Meo",
    brandName: "Tempo by Hilton",
    liveBrandName: "Tempo by Hilton",
    slug: "tempo-by-hilton",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    batchTag: "batch-4",
    feeGeography: "Confirm current Hilton franchise disclosure for Tempo by Hilton",
    sources: {
      brandPage: "https://www.hilton.com/en/hotels/brands/tempo-by-hilton/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      development: "Hilton development materials — Tempo by Hilton (public)",
    },
    positioning:
      "Vibrant lifestyle: music- and culture-forward public space with Hilton Honors distribution for travelers seeking social energy on the road.",
    commercialDifferentiator:
      "Versus Canopy's neighborhood lifestyle, Tempo leans entertainment and cultural programming; versus Motto's micro-hotel format, Tempo keeps a fuller lifestyle public-space story.",
    scenarios: [
      {
        title: "Urban Lifestyle Entertainment",
        body: "City sites where music, culture, and social public space can lift lifestyle rates with Hilton Honors behind the stay.",
      },
      {
        title: "Adaptive Reuse Cultural Districts",
        body: "Conversions in cultural districts that can fund credible programming—validate that Tempo's promise is visible in product, not only marketing.",
      },
      {
        title: "Leisure-Led Destination Weekends",
        body: "Markets where weekend leisure and nightlife-adjacent discovery drive mix—pair Honors midweek accounts with Tempo leisure programming.",
      },
    ],
    loyaltyProof: HONORS_PROOF("Tempo"),
    feeNote:
      "Tempo by Hilton application, royalty, program fees, and Honors assessments — confirm current Hilton franchise disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  rec02zPClpWUTCyXM: pack({
    brandId: "rec02zPClpWUTCyXM",
    brandName: "Design Hotels",
    liveBrandName: "Design Hotels",
    slug: "design-hotels",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-4",
    feeGeography: "Confirm current Marriott / Design Hotels affiliation disclosure",
    sources: {
      brandPage: "https://www.designhotels.com/",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      news: "https://news.marriott.com/brands/design-hotels",
    },
    positioning:
      "Design-led independent collection: architecture and cultural originality with Marriott Bonvoy commercial systems for owners who can sustain design QA.",
    commercialDifferentiator:
      "Versus Autograph and Tribute soft collections, Design Hotels prioritizes design originality and member curation; underwrite design capital and standards discipline carefully.",
    scenarios: [
      {
        title: "Design-Original Independent",
        body: "Hotels where architecture and design authorship are the product—pair Bonvoy distribution with design QA that protects originality.",
      },
      {
        title: "Cultural Destination Landmarks",
        body: "Destination assets where cultural programming and design narrative drive rate power inside the Marriott soft-collection ladder.",
      },
      {
        title: "Sibling Clarity Versus Autograph",
        body: "Assets that need diligence against Autograph and Tribute before assuming the same owner use case, fee path, or design capital stack.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Design Hotels"),
    feeNote:
      "Design Hotels affiliation / application, program fees, and Bonvoy assessments — confirm current Marriott or Design Hotels disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recJ1GZQpttX7qHgw: pack({
    brandId: "recJ1GZQpttX7qHgw",
    brandName: "Aloft Hotels",
    liveBrandName: "Aloft Hotels",
    slug: "aloft-hotels",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-4",
    feeGeography: "Confirm current Marriott franchise / development disclosure for Aloft",
    sources: {
      brandPage: "https://aloft-hotels.marriott.com/",
      news: "https://news.marriott.com/brands/aloft-hotels",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning:
      "Select-service lifestyle: modern rooms, social lobby energy, and tech-forward cues for urban and airport-adjacent travelers inside Marriott Bonvoy.",
    commercialDifferentiator:
      "Versus Moxy's budget lifestyle intensity and AC Hotels' European contemporary select-service, Aloft sits in modern lifestyle select-service without W-level luxury capital.",
    scenarios: [
      {
        title: "Urban & Airport Lifestyle Select-Service",
        body: "Urban and airport-adjacent assets where social lobby energy and modern rooms can lift select-service demand with Bonvoy distribution.",
      },
      {
        title: "Mixed-Use Lifestyle Anchors",
        body: "Mixed-use sites seeking lifestyle select-service without full-service capital—underwrite public-space programming carefully.",
      },
      {
        title: "Sibling Clarity Versus Moxy & AC",
        body: "Assets that need diligence against Moxy and AC Hotels before assuming the same owner use case or fee path.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Aloft"),
    feeNote:
      "Aloft application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recCKuXCmGvxHPfb3: pack({
    brandId: "recCKuXCmGvxHPfb3",
    brandName: "Kimpton Hotels",
    liveBrandName: "Kimpton Hotels",
    slug: "kimpton-hotels",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    batchTag: "batch-4",
    feeGeography: "Confirm current IHG development / franchise disclosure for Kimpton",
    sources: {
      brandPage: "https://www.ihg.com/kimptonhotels/hotels/us/en/reservation",
      development: "https://development.ihg.com/",
      oneRewards: "https://www.ihg.com/onerewards",
    },
    positioning:
      "Boutique lifestyle: design-led hotels with social public space, F&B character, and IHG One Rewards distribution for owners who can run distinctive full-service hospitality.",
    commercialDifferentiator:
      "Versus Hotel Indigo's neighborhood boutique format, Kimpton leans fuller boutique lifestyle with stronger F&B and social programming; versus soft collections, it remains a defined lifestyle brand.",
    scenarios: [
      {
        title: "Urban Boutique Lifestyle",
        body: "City assets where design, F&B, and social public space can lift boutique lifestyle rates with IHG One Rewards behind the stay.",
      },
      {
        title: "Adaptive Reuse Character Conversions",
        body: "Conversions that can fund credible design and F&B—validate Kimpton product standards before assuming soft-collection flexibility.",
      },
      {
        title: "Leisure & Corporate Mix Markets",
        body: "Markets where midweek accounts and weekend lifestyle demand both fit—pair One Rewards corporate flow with Kimpton leisure programming.",
      },
    ],
    loyaltyProof: IHG_PROOF("Kimpton"),
    feeNote:
      "Kimpton application, royalty, program fees, and One Rewards assessments — confirm current IHG disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recGv268Wda31PlSZ: pack({
    brandId: "recGv268Wda31PlSZ",
    brandName: "Bunkhouse Hotels",
    liveBrandName: "Bunkhouse Hotels",
    slug: "bunkhouse-hotels",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    batchTag: "batch-4",
    feeGeography: "Confirm current Hyatt / Bunkhouse affiliation disclosure",
    sources: {
      brandPage: "https://www.bunkhousehotels.com/",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt / Bunkhouse public brand communications",
    },
    positioning:
      "Independent-spirited lifestyle collection: design originality and place narrative with World of Hyatt commercial systems for owners who can sustain distinctive hospitality.",
    commercialDifferentiator:
      "Versus Hyatt Centric's explorer lifestyle format, Bunkhouse prioritizes independent design authorship; underwrite design capital and affiliation economics carefully.",
    scenarios: [
      {
        title: "Design-Original Lifestyle",
        body: "Hotels where design authorship and place narrative are the product—pair World of Hyatt distribution with standards that protect originality.",
      },
      {
        title: "Destination Cultural Stays",
        body: "Destination assets where cultural programming and F&B character drive rate power inside the Hyatt lifestyle ladder.",
      },
      {
        title: "Sibling Clarity Versus Hyatt Lifestyle",
        body: "Assets that need diligence against Hyatt Centric and other Hyatt lifestyle brands before assuming the same owner use case or fee path.",
      },
    ],
    loyaltyProof: HYATT_PROOF("Bunkhouse"),
    feeNote:
      "Bunkhouse affiliation / application, program fees, and World of Hyatt assessments — confirm current Hyatt or Bunkhouse disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recNy2efMm4N1JtgC: pack({
    brandId: "recNy2efMm4N1JtgC",
    brandName: "Hyatt Centric",
    liveBrandName: "Hyatt Centric",
    slug: "hyatt-centric",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    batchTag: "batch-4",
    feeGeography: "Confirm current Hyatt franchise / development disclosure for Hyatt Centric",
    sources: {
      brandPage: "https://www.hyatt.com/hyatt-centric",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt development materials — Hyatt Centric (public)",
    },
    positioning:
      "Explorer lifestyle: location-forward urban hotels with neighborhood access and World of Hyatt distribution for travelers who want place over prototype.",
    commercialDifferentiator:
      "Versus Hotel Indigo / Canopy peers, Centric rides World of Hyatt; versus Hyatt Regency, it is explorer lifestyle rather than meetings-led full-service.",
    scenarios: [
      {
        title: "Urban Explorer Lifestyle",
        body: "Walkable urban sites where location, design, and neighborhood access can lift lifestyle rates with World of Hyatt behind the stay.",
      },
      {
        title: "Conversion Into Explorer Product",
        body: "Conversions that can deliver explorer-forward public space and F&B—underwrite design narrative so Centric is visible in product.",
      },
      {
        title: "Leisure-Led Destination Weekends",
        body: "Markets where weekend leisure and local discovery drive mix—pair World of Hyatt midweek accounts with Centric leisure programming.",
      },
    ],
    loyaltyProof: HYATT_PROOF("Hyatt Centric"),
    feeNote:
      "Hyatt Centric application, royalty, program fees, and World of Hyatt assessments — confirm current Hyatt disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  rec6hye5H8zJmAGv3: pack({
    brandId: "rec6hye5H8zJmAGv3",
    brandName: "Courtyard by Marriott",
    liveBrandName: "Courtyard by Marriott",
    slug: "courtyard-by-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-4",
    feeGeography: "Confirm current Marriott franchise / development disclosure for Courtyard",
    sources: {
      brandPage: "https://courtyard.marriott.com/",
      news: "https://news.marriott.com/brands/courtyard",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning:
      "Upper midscale select-service: reliable rooms, functional public space, and Marriott Bonvoy distribution for corporate and leisure transient demand.",
    commercialDifferentiator:
      "Versus hard limited-service boxes, Courtyard sells a fuller select-service stack; versus lifestyle select-service (Aloft/AC), it prioritizes reliability and corporate utility over design intensity.",
    scenarios: [
      {
        title: "Corporate Corridor Select-Service",
        body: "Suburban and corridor sites where midweek corporate demand and Bonvoy accounts underwrite Courtyard economics.",
      },
      {
        title: "Airport & Gateway Utility",
        body: "Airport-adjacent assets where reliable product and Marriott distribution matter more than lifestyle programming.",
      },
      {
        title: "New-Build Prototype Efficiency",
        body: "New-build sites optimized for Courtyard prototypes—underwrite development cost discipline against local upper-midscale competitors.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Courtyard"),
    feeNote:
      "Courtyard application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recIPuBC50fv13zRR: pack({
    brandId: "recIPuBC50fv13zRR",
    brandName: "Westin",
    liveBrandName: "Westin",
    slug: "westin",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-4",
    feeGeography: "Confirm current Marriott franchise / development disclosure for Westin",
    sources: {
      brandPage: "https://westin.marriott.com/",
      news: "https://news.marriott.com/brands/westin",
      bonvoy: "https://www.marriott.com/loyalty.mi",
    },
    positioning:
      "Upper-upscale wellness: Heavenly Bed heritage, fitness cues, and Marriott Bonvoy distribution for travelers who prioritize restorative stays.",
    commercialDifferentiator:
      "Versus Sheraton's broader upper-upscale utility, Westin sells wellness and restorative product; versus lifestyle brands, it keeps a more standardized upper-upscale operating stack.",
    scenarios: [
      {
        title: "Urban Upper-Upscale Wellness",
        body: "City full-service assets where wellness programming and Bonvoy accounts can support upper-upscale rates.",
      },
      {
        title: "Airport & Meetings Upper-Upscale",
        body: "Airport and meetings-adjacent assets where Westin product standards and Bonvoy corporate flow underwrite contribution.",
      },
      {
        title: "Conversion / PIP Into Westin Standards",
        body: "Conversions that can fund Heavenly / fitness / public-space PIP—validate capital stack before assuming lifestyle-level flexibility.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Westin"),
    feeNote:
      "Westin application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
  }),

  recmGmiIqDtAsm01f: pack({
    brandId: "recmGmiIqDtAsm01f",
    brandName: "Holiday Inn Express",
    liveBrandName: "Holiday Inn Express",
    slug: "holiday-inn-express",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    batchTag: "batch-4",
    feeGeography: "Confirm current IHG development / franchise disclosure for Holiday Inn Express",
    sources: {
      brandPage: "https://www.ihg.com/holidayinnexpress/hotels/us/en/reservation",
      development: "https://development.ihg.com/brand/holiday-inn-express",
      oneRewards: "https://www.ihg.com/onerewards",
    },
    positioning:
      "Midscale limited-service: efficient rooms, Express Start breakfast, and IHG One Rewards distribution for value-conscious transient demand.",
    commercialDifferentiator:
      "Versus avid's essential midscale positioning, Holiday Inn Express is the scaled midscale limited-service workhorse; versus upscale lifestyle brands, it keeps capital and operating intensity deliberately lean.",
    scenarios: [
      {
        title: "Corridor Midscale Limited-Service",
        body: "Highway and suburban corridors where efficient rooms, breakfast, and One Rewards can compete on value.",
      },
      {
        title: "Airport & Gateway Value",
        body: "Airport-adjacent value stays where clean product and loyalty distribution matter more than F&B depth.",
      },
      {
        title: "New-Build Prototype Efficiency",
        body: "New-build sites optimized for Express prototypes—underwrite development cost discipline against local midscale competitors.",
      },
    ],
    loyaltyProof: IHG_PROOF("Holiday Inn Express"),
    feeNote:
      "Holiday Inn Express application, royalty, program fees, and One Rewards assessments — confirm current IHG disclosure exhibits. Amounts are not staged without a verified disclosure.",
  }),

  // ——— Batch 5 (existing Draft/Under Review Brand Basics; empty Presentation) ———
  recj5BCSpWqMEEBDg: pack({
    brandId: "recj5BCSpWqMEEBDg",
    brandName: "JW Marriott",
    liveBrandName: "JW Marriott",
    slug: "jw-marriott",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-5",
    feeGeography: "Confirm current Marriott franchise / development disclosure for JW Marriott (luxury)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/brands/jw-marriott/",
      news: "https://news.marriott.com/brands/jw-marriott",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — JW Marriott (public)",
    },
    positioning:
      "Luxury full-service: polished public spaces, elevated F&B, and Marriott Bonvoy distribution for travelers seeking classic luxury with modern service.",
    commercialDifferentiator:
      "Versus W Hotels' lifestyle luxury, JW Marriott sells classic luxury full-service; versus Ritz-Carlton, it is more accessible luxury within Marriott's Bonvoy ladder—confirm fee and PIP intensity before IC.",
    scenarios: [
      {
        title: "Gateway Luxury Full-Service",
        body: "Primary and secondary gateway cities where meetings, leisure, and Bonvoy accounts can underwrite classic luxury product.",
      },
      {
        title: "Resort & Destination Luxury",
        body: "Destination resort assets that can fund elevated F&B and spa without sliding into lifestyle-only capital assumptions.",
      },
      {
        title: "Conversion Into JW Standards",
        body: "Conversions that can deliver JW public-space and F&B standards—underwrite luxury PIP carefully versus upper-upscale peers.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("JW Marriott"),
    feeNote:
      "JW Marriott application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott luxury disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Typically franchise or managed luxury full-service with elevated F&B, meetings capacity where designed, and Bonvoy-facing front-of-house. Staffing and QA intensity sit above upper-upscale select-service.",
    footprint:
      "Global Marriott luxury flag with urban and resort presence across Americas, EMEA, and APAC. Confirm live hotel counts and pipeline from Marriott disclosures—do not underwrite from media headlines alone.",
    insight:
      "JW Marriott works when sponsors can fund classic luxury delivery and Bonvoy contribution; it fails when the asset is underwritten like select-service or lifestyle-light capital.",
    standards: [
      {
        title: "Public Space & F&B Elevation",
        body: "Lobby, F&B, and public-space quality must read as luxury—not upper-midscale renovations with a luxury flag.",
      },
      {
        title: "Service & Brand QA",
        body: "Service delivery and brand audits match luxury expectations; budget training and ongoing QA in the operating plan.",
      },
      {
        title: "Bonvoy Systems Integration",
        body: "Property systems must support Marriott Bonvoy enrollment, Elite recognition, and redemption stays.",
      },
    ],
    valueOwnerScenarios: [
      {
        title: "Gateway Classic Luxury",
        body: "Urban gateways where Bonvoy luxury demand and meetings mix can support JW rates when product and service clear luxury QA.",
      },
      {
        title: "Destination Resort Luxury",
        body: "Resort assets with spa and F&B depth that can carry JW positioning without lifestyle-only underwriting shortcuts.",
      },
      {
        title: "Conversion Discipline",
        body: "Conversions only when PIP funds credible luxury public space—avoid flagging upper-upscale shells as JW without capital.",
      },
      {
        title: "Sibling Clarity Versus W / Ritz",
        body: "Diligence versus W lifestyle luxury and Ritz-Carlton before assuming identical owner use case or fee path.",
      },
    ],
    gallery: [
      {
        title: "JW Marriott — Luxury Public Space",
        body: "Classic luxury lobby and public-space product · confirm property-specific photography before publication",
        source: "https://www.marriott.com/en-us/hotels/brands/jw-marriott/",
      },
      {
        title: "JW Marriott — Guest Room",
        body: "Elevated guest-room finishes typical of the luxury full-service ladder",
        source: "https://www.marriott.com/en-us/hotels/brands/jw-marriott/",
      },
      {
        title: "JW Marriott — F&B",
        body: "Restaurant and bar product supporting luxury stay contribution",
        source: "https://www.marriott.com/en-us/hotels/brands/jw-marriott/",
      },
      {
        title: "JW Marriott — Meetings / Ballroom",
        body: "Meetings capacity where the asset is designed for group and corporate demand",
        source: "https://www.marriott.com/en-us/hotels/brands/jw-marriott/",
      },
      {
        title: "JW Marriott — Wellness / Spa Cue",
        body: "Spa or wellness programming where offered at luxury JW assets",
        source: "https://www.marriott.com/en-us/hotels/brands/jw-marriott/",
      },
      {
        title: "JW Marriott — Exterior / Arrival",
        body: "Arrival and exterior presence that reads as classic luxury in market",
        source: "https://www.marriott.com/en-us/hotels/brands/jw-marriott/",
      },
    ],
  }),

  recqBiokz5ePxbFTE: pack({
    brandId: "recqBiokz5ePxbFTE",
    brandName: "W Hotels",
    liveBrandName: "W Hotels",
    slug: "w-hotels",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-5",
    feeGeography: "Confirm current Marriott franchise / development disclosure for W Hotels (luxury lifestyle)",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/brands/w-hotels/",
      news: "https://news.marriott.com/brands/w-hotels",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — W Hotels (public)",
    },
    positioning:
      "Luxury lifestyle: music, design, and nightlife-forward public spaces with Marriott Bonvoy distribution for travelers who want energy over classical luxury.",
    commercialDifferentiator:
      "Versus JW Marriott's classic luxury, W sells lifestyle luxury and social programming; versus Edition, confirm sibling clarity on design intensity and fee path before IC.",
    scenarios: [
      {
        title: "Urban Lifestyle Luxury",
        body: "Gateway and lifestyle districts where W's social public space and Bonvoy can lift luxury lifestyle rates.",
      },
      {
        title: "Destination Party / Resort Energy",
        body: "Resort and destination assets that can sustain nightlife and F&B intensity without eroding contribution.",
      },
      {
        title: "Conversion Into W Product",
        body: "Conversions that can fund design and entertainment programming—underwrite capital beyond a soft reflag.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("W Hotels"),
    feeNote:
      "W Hotels application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott luxury lifestyle disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury lifestyle with intensive F&B, entertainment, and design QA. Operating costs typically exceed classic full-service peers when nightlife and programming run hard.",
    footprint:
      "Global W footprint across urban lifestyle and resort markets in Americas, EMEA, and APAC. Confirm openings and pipeline from Marriott brand disclosures.",
    insight:
      "W creates owner value when the asset can sustain lifestyle energy and Bonvoy luxury demand; it destroys value when sponsors underwrite like a quiet classic luxury hotel.",
    standards: [
      {
        title: "Design & Social Public Space",
        body: "Living Room / social lobby and entertainment cues must feel brand-authentic—not generic luxury renovations.",
      },
      {
        title: "F&B & Nightlife Intensity",
        body: "Where W programming includes nightlife and destination F&B, underwrite operator capability and noise/licensing reality.",
      },
      {
        title: "Bonvoy Luxury Delivery",
        body: "Elite recognition and Bonvoy systems still apply inside a lifestyle product—service must not break loyalty delivery.",
      },
    ],
    valueOwnerScenarios: [
      {
        title: "Gateway Lifestyle Luxury",
        body: "Urban lifestyle districts where design and social energy can support W rates with Bonvoy behind the stay.",
      },
      {
        title: "Resort Entertainment Product",
        body: "Resort assets that can fund entertainment and F&B intensity without collapsing into OTA-only leisure.",
      },
      {
        title: "Conversion Capital Reality",
        body: "Only convert when design and programming capital clear IC—avoid lifestyle flagging of undercapitalized shells.",
      },
      {
        title: "Sibling Clarity Versus JW / Edition",
        body: "Diligence versus classic JW luxury and Edition before assuming the same owner use case.",
      },
    ],
    gallery: [
      { title: "W Hotels — Living Room / Social Lobby", body: "Lifestyle public space · confirm property photography before publication", source: "https://www.marriott.com/en-us/hotels/brands/w-hotels/" },
      { title: "W Hotels — Guest Room", body: "Design-forward guest-room product typical of W luxury lifestyle", source: "https://www.marriott.com/en-us/hotels/brands/w-hotels/" },
      { title: "W Hotels — F&B / Bar", body: "Destination F&B and bar programming supporting lifestyle contribution", source: "https://www.marriott.com/en-us/hotels/brands/w-hotels/" },
      { title: "W Hotels — Pool / Exterior Energy", body: "Pool and exterior cues where resort or urban rooftop product exists", source: "https://www.marriott.com/en-us/hotels/brands/w-hotels/" },
      { title: "W Hotels — Events / Social", body: "Social and entertainment spaces that define W versus classic luxury", source: "https://www.marriott.com/en-us/hotels/brands/w-hotels/" },
      { title: "W Hotels — Arrival", body: "Arrival experience that reads lifestyle luxury in market", source: "https://www.marriott.com/en-us/hotels/brands/w-hotels/" },
    ],
  }),

  recN6sas0Km17O2Yn: pack({
    brandId: "recN6sas0Km17O2Yn",
    brandName: "Le Méridien",
    liveBrandName: "Le Meridien",
    slug: "le-meridien",
    parentCompany: "Marriott International, Inc.",
    loyaltyProgram: "Marriott Bonvoy",
    batchTag: "batch-5",
    feeGeography: "Confirm current Marriott franchise / development disclosure for Le Méridien",
    sources: {
      brandPage: "https://www.marriott.com/en-us/hotels/brands/le-meridien/",
      news: "https://news.marriott.com/brands/le-meridien",
      bonvoy: "https://www.marriott.com/loyalty.mi",
      development: "Marriott development materials — Le Méridien (public)",
    },
    positioning:
      "Upscale discovery brand: European heritage cues, destination discovery programming, and Marriott Bonvoy distribution for curious travelers.",
    commercialDifferentiator:
      "Versus JW classic luxury, Le Méridien sits as discovery-led upscale/upper-upscale; versus Autograph individuality, it keeps a more brand-standardized discovery narrative inside Bonvoy.",
    scenarios: [
      {
        title: "Urban Discovery Upscale",
        body: "City assets where destination discovery programming and Bonvoy can differentiate versus generic upper-upscale peers.",
      },
      {
        title: "Gateway & Meetings Mix",
        body: "Gateway hotels that need meetings utility with a discovery brand story rather than hard lifestyle nightlife.",
      },
      {
        title: "Conversion Into Discovery Product",
        body: "Conversions that can fund public-space and F&B cues aligned with Le Méridien discovery standards.",
      },
    ],
    loyaltyProof: BONVOY_PROOF("Le Méridien"),
    feeNote:
      "Le Méridien application, royalty, program contribution, and Bonvoy assessments — confirm current Marriott disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Upscale / upper-upscale full-service with discovery programming and Bonvoy systems. Operating intensity sits below lifestyle luxury (W) but above lean select-service.",
    footprint:
      "Global Le Méridien presence across urban and resort markets. Confirm live counts and pipeline from Marriott disclosures; spelling in Airtable is Le Meridien (no accent).",
    insight:
      "Le Méridien fits owners who want Bonvoy upscale distribution with a discovery narrative—not owners seeking soft-collection individuality or select-service capital light.",
    standards: [
      { title: "Discovery Public Space", body: "Public spaces and programming should support destination discovery—not anonymous upper-upscale boxes." },
      { title: "F&B Character", body: "F&B should feel place-aware within brand standards; underwrite operator capability." },
      { title: "Bonvoy Integration", body: "Full Bonvoy enrollment and Elite delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Discovery", body: "City sites where discovery narrative and Bonvoy can lift rates versus generic peers." },
      { title: "Gateway Utility With Story", body: "Gateways needing meetings utility plus brand story without W-level nightlife capital." },
      { title: "Conversion Standards", body: "Convert only when public-space PIP can carry discovery cues." },
      { title: "Sibling Clarity", body: "Diligence versus JW, Westin, and Autograph before assuming identical economics." },
    ],
    gallery: [
      { title: "Le Méridien — Public Space", body: "Discovery-led public space · confirm property photography before publication", source: "https://www.marriott.com/en-us/hotels/brands/le-meridien/" },
      { title: "Le Méridien — Guest Room", body: "Upscale guest-room product with brand design cues", source: "https://www.marriott.com/en-us/hotels/brands/le-meridien/" },
      { title: "Le Méridien — F&B", body: "Place-aware F&B supporting discovery positioning", source: "https://www.marriott.com/en-us/hotels/brands/le-meridien/" },
      { title: "Le Méridien — Meetings", body: "Meetings product where designed into the asset", source: "https://www.marriott.com/en-us/hotels/brands/le-meridien/" },
      { title: "Le Méridien — Exterior", body: "Exterior / arrival presence in market", source: "https://www.marriott.com/en-us/hotels/brands/le-meridien/" },
      { title: "Le Méridien — Destination Cue", body: "Destination discovery programming cue typical of the brand", source: "https://www.marriott.com/en-us/hotels/brands/le-meridien/" },
    ],
  }),

  recXopdFodrD3IyIR: pack({
    brandId: "recXopdFodrD3IyIR",
    brandName: "Waldorf Astoria Hotels & Resorts",
    liveBrandName: "Waldorf Astoria",
    slug: "waldorf-astoria",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    batchTag: "batch-5",
    feeGeography: "Confirm current Hilton development / franchise disclosure for Waldorf Astoria (luxury)",
    sources: {
      brandPage: "https://www.hilton.com/en/waldorf-astoria/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      development: "Hilton development materials — Waldorf Astoria (public)",
    },
    positioning:
      "Iconic luxury: heritage luxury cues, elevated service, and Hilton Honors distribution for travelers seeking classic luxury resorts and urban icons.",
    commercialDifferentiator:
      "Versus Conrad's contemporary luxury, Waldorf Astoria sells iconic / heritage luxury; versus LXR soft collection, it is a harder luxury brand standard inside Honors.",
    scenarios: [
      { title: "Iconic Urban Luxury", body: "Gateway icons where heritage luxury and Honors can support Waldorf rates." },
      { title: "Luxury Resort Destinations", body: "Resort assets that can fund spa, F&B, and service intensity matching Waldorf standards." },
      { title: "Conversion Into Iconic Luxury", body: "Conversions only when capital and service can clear luxury QA—not soft reflags." },
    ],
    loyaltyProof: HONORS_PROOF("Waldorf Astoria"),
    feeNote:
      "Waldorf Astoria application, royalty, program fees, and Hilton Honors assessments — confirm current Hilton luxury disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury full-service / resort with high service intensity, elevated F&B, and Honors systems. Do not underwrite like select-service or midscale franchise economics.",
    footprint:
      "Select global luxury footprint (urban icons and resorts). Confirm live hotel counts from Hilton disclosures—portfolio is intentionally tighter than Conrad or mainstream Hilton flags.",
    insight:
      "Waldorf Astoria is for sponsors who can fund iconic luxury delivery; it is the wrong flag when capital or service ambition is upper-upscale only.",
    standards: [
      { title: "Iconic Luxury Product", body: "Public space, F&B, and finishes must read iconic luxury—not contemporary-upscale renovations." },
      { title: "Service Intensity", body: "Service staffing and training must match luxury expectations continuously." },
      { title: "Honors Delivery", body: "Hilton Honors Elite recognition and systems integration required." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Icon Luxury", body: "Gateway icons where heritage luxury and Honors demand clear IC." },
      { title: "Resort Luxury", body: "Destination resorts that can sustain spa and F&B intensity." },
      { title: "Conversion Capital Gate", body: "Convert only with luxury PIP and service plan—avoid undercapitalized shells." },
      { title: "Sibling Clarity Versus Conrad / LXR", body: "Diligence versus Conrad contemporary luxury and LXR soft collection before assuming identical use case." },
    ],
    gallery: [
      { title: "Waldorf Astoria — Luxury Public Space", body: "Iconic luxury public space · confirm property photography before publication", source: "https://www.hilton.com/en/waldorf-astoria/" },
      { title: "Waldorf Astoria — Guest Room", body: "Luxury guest-room finishes typical of the brand", source: "https://www.hilton.com/en/waldorf-astoria/" },
      { title: "Waldorf Astoria — F&B", body: "Elevated F&B supporting iconic luxury contribution", source: "https://www.hilton.com/en/waldorf-astoria/" },
      { title: "Waldorf Astoria — Spa / Wellness", body: "Spa and wellness where offered at Waldorf resorts and urban icons", source: "https://www.hilton.com/en/waldorf-astoria/" },
      { title: "Waldorf Astoria — Exterior", body: "Exterior / arrival presence that reads iconic luxury", source: "https://www.hilton.com/en/waldorf-astoria/" },
      { title: "Waldorf Astoria — Events", body: "Events and ballroom product where designed into the asset", source: "https://www.hilton.com/en/waldorf-astoria/" },
    ],
  }),

  rec90n1dSVVyoEOto: pack({
    brandId: "rec90n1dSVVyoEOto",
    brandName: "Conrad Hotels & Resorts",
    liveBrandName: "Conrad Hotels & Resorts",
    slug: "conrad-hotels-resorts",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    batchTag: "batch-5",
    feeGeography: "Confirm current Hilton development / franchise disclosure for Conrad (luxury)",
    sources: {
      brandPage: "https://www.hilton.com/en/conrad/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      development: "Hilton development materials — Conrad (public)",
    },
    positioning:
      "Contemporary luxury: modern design, elevated service, and Hilton Honors distribution for travelers seeking polished luxury without Waldorf heritage cues.",
    commercialDifferentiator:
      "Versus Waldorf Astoria's iconic heritage luxury, Conrad sells contemporary luxury; versus Curio individuality, it keeps a harder luxury brand standard inside Honors.",
    scenarios: [
      { title: "Urban Contemporary Luxury", body: "Gateway and CBD assets where contemporary luxury and Honors can support Conrad rates." },
      { title: "Luxury Resort Contemporary", body: "Resort assets with modern luxury product rather than heritage icon positioning." },
      { title: "Conversion Into Conrad Standards", body: "Conversions that can fund contemporary luxury public space and service." },
    ],
    loyaltyProof: HONORS_PROOF("Conrad"),
    feeNote:
      "Conrad application, royalty, program fees, and Hilton Honors assessments — confirm current Hilton luxury disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury full-service with contemporary design QA, elevated F&B, and Honors systems. Operating costs sit in the luxury band—not select-service.",
    footprint:
      "Global Conrad urban and resort footprint. Confirm openings and pipeline from Hilton disclosures across APAC, EMEA, CALA / LATAM, and North America.",
    insight:
      "Conrad fits sponsors who want Hilton luxury distribution with contemporary product—wrong when the asset cannot clear luxury service and design standards.",
    standards: [
      { title: "Contemporary Luxury Design", body: "Design and public space must read contemporary luxury consistently." },
      { title: "Service & F&B Elevation", body: "Service and F&B intensity must match luxury expectations." },
      { title: "Honors Systems", body: "Full Hilton Honors integration required." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Contemporary Luxury", body: "CBD and gateway sites where Conrad product and Honors clear IC." },
      { title: "Resort Contemporary Luxury", body: "Resorts that can fund modern luxury without heritage-icon capital myths." },
      { title: "Conversion Discipline", body: "Convert only with luxury PIP—avoid upper-upscale shells." },
      { title: "Sibling Clarity Versus Waldorf / LXR", body: "Diligence versus Waldorf and LXR before assuming identical economics." },
    ],
    gallery: [
      { title: "Conrad — Contemporary Public Space", body: "Contemporary luxury lobby · confirm property photography before publication", source: "https://www.hilton.com/en/conrad/" },
      { title: "Conrad — Guest Room", body: "Luxury guest-room product typical of Conrad", source: "https://www.hilton.com/en/conrad/" },
      { title: "Conrad — F&B", body: "Elevated F&B supporting contemporary luxury stays", source: "https://www.hilton.com/en/conrad/" },
      { title: "Conrad — Pool / Resort Cue", body: "Pool or resort programming where offered", source: "https://www.hilton.com/en/conrad/" },
      { title: "Conrad — Meetings / Events", body: "Meetings and events capacity where designed", source: "https://www.hilton.com/en/conrad/" },
      { title: "Conrad — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hilton.com/en/conrad/" },
    ],
  }),

  recUsZOxtqIpeA5ZW: pack({
    brandId: "recUsZOxtqIpeA5ZW",
    brandName: "LXR Hotels & Resorts",
    liveBrandName: "LXR Hotels & Resorts",
    slug: "lxr-hotels-resorts",
    parentCompany: "Hilton Worldwide",
    loyaltyProgram: "Hilton Honors",
    batchTag: "batch-5",
    feeGeography: "Confirm current Hilton development / affiliation disclosure for LXR (luxury soft collection)",
    sources: {
      brandPage: "https://www.hilton.com/en/lxr/",
      honors: "https://www.hilton.com/en/hilton-honors/",
      development: "Hilton development materials — LXR Hotels & Resorts (public)",
    },
    positioning:
      "Luxury soft collection: independent-spirited luxury hotels with Hilton Honors distribution—each property keeps distinct character under LXR standards.",
    commercialDifferentiator:
      "Versus Conrad/Waldorf hard luxury brands, LXR sells soft-collection individuality with Honors; versus Curio, it sits higher on the luxury ladder—confirm affiliation path and PIP carefully.",
    scenarios: [
      { title: "Independent Luxury Character", body: "Assets with strong design authorship that need Honors without hard Conrad/Waldorf prototyping." },
      { title: "Destination Luxury Soft Collection", body: "Destination luxury hotels where individuality is the product and Honors expands distribution." },
      { title: "Affiliation Into LXR", body: "Affiliations that can meet LXR luxury standards while preserving property character." },
    ],
    loyaltyProof: HONORS_PROOF("LXR"),
    feeNote:
      "LXR affiliation / application, program fees, and Hilton Honors assessments — confirm current Hilton soft-collection luxury disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury soft collection: property individuality with Hilton systems and Honors. Operating and design capital remain luxury-band—do not confuse with select-service franchise light.",
    footprint:
      "Select global LXR luxury portfolio. Confirm live hotels and pipeline from Hilton disclosures—intentionally curated versus mass Hilton flags.",
    insight:
      "LXR works when individuality and luxury delivery are both real; it fails when sponsors want Honors without luxury capital or when hard-brand fees would fit better.",
    standards: [
      { title: "Luxury Individuality", body: "Property character must remain distinct while clearing LXR luxury standards." },
      { title: "Service & F&B Luxury", body: "Service and F&B must read luxury—soft collection is not a capital shortcut." },
      { title: "Honors Integration", body: "Hilton Honors enrollment and Elite delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Character Luxury Affiliation", body: "Independent luxury assets needing Honors without Conrad prototyping." },
      { title: "Destination Soft Collection", body: "Destination hotels where individuality drives rate with Honors distribution." },
      { title: "Standards Gate", body: "Affiliate only when luxury QA and PIP clear IC." },
      { title: "Sibling Clarity Versus Conrad / Curio", body: "Diligence versus Conrad hard luxury and Curio lifestyle collection before IC." },
    ],
    gallery: [
      { title: "LXR — Distinctive Public Space", body: "Property-specific luxury public space · confirm photography before publication", source: "https://www.hilton.com/en/lxr/" },
      { title: "LXR — Guest Room", body: "Individual luxury guest-room character typical of LXR", source: "https://www.hilton.com/en/lxr/" },
      { title: "LXR — F&B", body: "Destination F&B supporting soft-collection luxury", source: "https://www.hilton.com/en/lxr/" },
      { title: "LXR — Exterior / Arrival", body: "Arrival presence unique to each LXR property", source: "https://www.hilton.com/en/lxr/" },
      { title: "LXR — Wellness / Leisure", body: "Wellness or leisure cues where offered", source: "https://www.hilton.com/en/lxr/" },
      { title: "LXR — Design Detail", body: "Design authorship detail that differentiates soft-collection luxury", source: "https://www.hilton.com/en/lxr/" },
    ],
  }),

  recswdE1mhwDvXtBi: pack({
    brandId: "recswdE1mhwDvXtBi",
    brandName: "Park Hyatt",
    liveBrandName: "Park Hyatt",
    slug: "park-hyatt",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    batchTag: "batch-5",
    feeGeography: "Confirm current Hyatt development / franchise disclosure for Park Hyatt (luxury)",
    sources: {
      brandPage: "https://www.hyatt.com/park-hyatt",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt development materials — Park Hyatt (public)",
    },
    positioning:
      "Intimate luxury: residential-feeling luxury hotels with World of Hyatt distribution for travelers seeking quiet refinement over large convention product.",
    commercialDifferentiator:
      "Versus Grand Hyatt's meetings-led scale, Park Hyatt sells intimate luxury; versus Andaz lifestyle, it is quieter residential luxury inside World of Hyatt.",
    scenarios: [
      { title: "Intimate Urban Luxury", body: "Urban sites where residential luxury and World of Hyatt can support Park Hyatt rates." },
      { title: "Destination Quiet Luxury", body: "Destination assets that can fund intimate luxury without Grand Hyatt meetings scale." },
      { title: "Conversion Into Park Hyatt", body: "Conversions only when design and service clear intimate luxury standards." },
    ],
    loyaltyProof: HYATT_PROOF("Park Hyatt"),
    feeNote:
      "Park Hyatt application, royalty, program fees, and World of Hyatt assessments — confirm current Hyatt luxury disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury with intimate scale, elevated F&B, and World of Hyatt systems. Staffing intensity is luxury-band; do not import Hyatt Place select-service economics.",
    footprint:
      "Select global Park Hyatt footprint. Confirm live hotels and pipeline from Hyatt disclosures across APAC, EMEA, CALA / LATAM, and North America.",
    insight:
      "Park Hyatt fits sponsors who can deliver intimate luxury; wrong flag when the asset needs meetings scale (Grand Hyatt) or select-service capital (Hyatt Place).",
    standards: [
      { title: "Intimate Luxury Design", body: "Residential luxury cues and finishes must clear Park Hyatt standards." },
      { title: "Service Intensity", body: "Service delivery must match intimate luxury expectations." },
      { title: "World of Hyatt Integration", body: "Full World of Hyatt enrollment and Elite delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Intimate Urban Luxury", body: "Urban residential luxury sites with World of Hyatt demand." },
      { title: "Quiet Destination Luxury", body: "Destination assets without convention-scale ambition." },
      { title: "Conversion Capital Gate", body: "Convert only with luxury PIP and service plan." },
      { title: "Sibling Clarity Versus Grand / Andaz", body: "Diligence versus Grand Hyatt and Andaz before IC." },
    ],
    gallery: [
      { title: "Park Hyatt — Intimate Public Space", body: "Residential luxury public space · confirm property photography before publication", source: "https://www.hyatt.com/park-hyatt" },
      { title: "Park Hyatt — Guest Room", body: "Intimate luxury guest-room product", source: "https://www.hyatt.com/park-hyatt" },
      { title: "Park Hyatt — F&B", body: "Elevated F&B supporting quiet luxury stays", source: "https://www.hyatt.com/park-hyatt" },
      { title: "Park Hyatt — Spa / Wellness", body: "Spa or wellness where offered", source: "https://www.hyatt.com/park-hyatt" },
      { title: "Park Hyatt — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hyatt.com/park-hyatt" },
      { title: "Park Hyatt — Design Detail", body: "Residential design detail typical of Park Hyatt", source: "https://www.hyatt.com/park-hyatt" },
    ],
  }),

  recGCWhkeEvsQxuez: pack({
    brandId: "recGCWhkeEvsQxuez",
    brandName: "Grand Hyatt",
    liveBrandName: "Grand Hyatt",
    slug: "grand-hyatt",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    batchTag: "batch-5",
    feeGeography: "Confirm current Hyatt development / franchise disclosure for Grand Hyatt",
    sources: {
      brandPage: "https://www.hyatt.com/grand-hyatt",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt development materials — Grand Hyatt (public)",
    },
    positioning:
      "Upper-upscale / luxury meetings-led full-service: grand public spaces, meetings capacity, and World of Hyatt distribution for group and leisure demand.",
    commercialDifferentiator:
      "Versus Park Hyatt intimacy, Grand Hyatt sells scale and meetings; versus Hyatt Regency, it sits higher on design and service ambition—confirm fee and PIP before IC.",
    scenarios: [
      { title: "Gateway Meetings Full-Service", body: "Gateway assets where meetings capacity and World of Hyatt accounts underwrite Grand Hyatt." },
      { title: "Urban Grand Public Space", body: "Urban hotels with grand lobby and F&B that can carry Grand Hyatt positioning." },
      { title: "Conversion Into Grand Hyatt", body: "Conversions that can fund meetings and public-space PIP at Grand Hyatt standards." },
    ],
    loyaltyProof: HYATT_PROOF("Grand Hyatt"),
    feeNote:
      "Grand Hyatt application, royalty, program fees, and World of Hyatt assessments — confirm current Hyatt disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Full-service with meetings intensity, elevated public space, and World of Hyatt systems. Operating costs exceed select-service (Hyatt Place) by a wide margin.",
    footprint:
      "Global Grand Hyatt urban and resort footprint. Confirm openings and pipeline from Hyatt disclosures.",
    insight:
      "Grand Hyatt fits meetings-capable assets with World of Hyatt demand; wrong when sponsors want Park Hyatt intimacy or Hyatt Place capital light.",
    standards: [
      { title: "Grand Public Space & Meetings", body: "Lobby, meetings, and F&B must support Grand Hyatt scale expectations." },
      { title: "Service Delivery", body: "Service staffing must match upper-upscale / luxury full-service." },
      { title: "World of Hyatt Integration", body: "Full World of Hyatt systems required." },
    ],
    valueOwnerScenarios: [
      { title: "Meetings-Led Gateway", body: "Gateways where group and World of Hyatt accounts clear IC." },
      { title: "Urban Grand Product", body: "Urban assets with grand public space and F&B depth." },
      { title: "Conversion PIP Reality", body: "Convert only when meetings and public-space PIP are funded." },
      { title: "Sibling Clarity Versus Park / Regency", body: "Diligence versus Park Hyatt and Hyatt Regency before IC." },
    ],
    gallery: [
      { title: "Grand Hyatt — Grand Lobby", body: "Grand public space · confirm property photography before publication", source: "https://www.hyatt.com/grand-hyatt" },
      { title: "Grand Hyatt — Guest Room", body: "Full-service guest-room product typical of Grand Hyatt", source: "https://www.hyatt.com/grand-hyatt" },
      { title: "Grand Hyatt — Ballroom / Meetings", body: "Meetings and ballroom capacity where designed", source: "https://www.hyatt.com/grand-hyatt" },
      { title: "Grand Hyatt — F&B", body: "Elevated F&B supporting full-service contribution", source: "https://www.hyatt.com/grand-hyatt" },
      { title: "Grand Hyatt — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hyatt.com/grand-hyatt" },
      { title: "Grand Hyatt — Pool / Leisure", body: "Leisure cues where offered at Grand Hyatt assets", source: "https://www.hyatt.com/grand-hyatt" },
    ],
  }),

  recbtJoMub5wcankK: pack({
    brandId: "recbtJoMub5wcankK",
    brandName: "Andaz",
    liveBrandName: "Andaz",
    slug: "andaz",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    batchTag: "batch-5",
    feeGeography: "Confirm current Hyatt development / franchise disclosure for Andaz (luxury lifestyle)",
    sources: {
      brandPage: "https://www.hyatt.com/andaz",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt development materials — Andaz (public)",
    },
    positioning:
      "Luxury lifestyle: local design authorship, social public space, and World of Hyatt distribution for travelers who want place-led luxury.",
    commercialDifferentiator:
      "Versus Park Hyatt quiet residential luxury, Andaz sells lifestyle and local design; versus Hyatt Centric explorer lifestyle, Andaz sits higher on luxury intensity.",
    scenarios: [
      { title: "Urban Lifestyle Luxury", body: "Lifestyle districts where local design and World of Hyatt can support Andaz rates." },
      { title: "Destination Design Luxury", body: "Destination assets that can fund design authorship and F&B character." },
      { title: "Conversion Into Andaz", body: "Conversions that can deliver lifestyle luxury without erasing local character." },
    ],
    loyaltyProof: HYATT_PROOF("Andaz"),
    feeNote:
      "Andaz application, royalty, program fees, and World of Hyatt assessments — confirm current Hyatt luxury lifestyle disclosure exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Luxury lifestyle with design-intensive public space, F&B character, and World of Hyatt systems. Capital and operating intensity exceed Hyatt Place select-service by design.",
    footprint:
      "Select global Andaz footprint in lifestyle urban and destination markets. Confirm live hotels from Hyatt disclosures.",
    insight:
      "Andaz creates value when design authorship and luxury delivery are both funded; fails when underwritten like select-service or anonymous upper-upscale.",
    standards: [
      { title: "Local Design Authorship", body: "Design must feel place-led within Andaz standards—not generic luxury renovations." },
      { title: "Social Public Space & F&B", body: "Social lobby and F&B character must clear lifestyle luxury expectations." },
      { title: "World of Hyatt Integration", body: "Full World of Hyatt delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Urban Lifestyle Luxury", body: "Lifestyle districts with World of Hyatt demand and design capital." },
      { title: "Destination Design", body: "Destination assets that can sustain design and F&B authorship." },
      { title: "Conversion Capital Gate", body: "Convert only when design PIP clears IC." },
      { title: "Sibling Clarity Versus Park / Centric", body: "Diligence versus Park Hyatt and Hyatt Centric before IC." },
    ],
    gallery: [
      { title: "Andaz — Social Public Space", body: "Lifestyle luxury public space · confirm property photography before publication", source: "https://www.hyatt.com/andaz" },
      { title: "Andaz — Guest Room", body: "Design-led guest-room product typical of Andaz", source: "https://www.hyatt.com/andaz" },
      { title: "Andaz — F&B", body: "Place-led F&B supporting lifestyle luxury", source: "https://www.hyatt.com/andaz" },
      { title: "Andaz — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hyatt.com/andaz" },
      { title: "Andaz — Pool / Leisure", body: "Leisure cues where offered", source: "https://www.hyatt.com/andaz" },
      { title: "Andaz — Design Detail", body: "Local design authorship detail typical of Andaz", source: "https://www.hyatt.com/andaz" },
    ],
  }),

  recgwxli5IsoPKCOY: pack({
    brandId: "recgwxli5IsoPKCOY",
    brandName: "Hyatt Place",
    liveBrandName: "Hyatt Place",
    slug: "hyatt-place",
    parentCompany: "Hyatt Hotels Corporation",
    loyaltyProgram: "World of Hyatt",
    batchTag: "batch-5",
    feeGeography: "Confirm current Hyatt franchise / development disclosure for Hyatt Place (select-service)",
    sources: {
      brandPage: "https://www.hyatt.com/hyatt-place",
      worldOfHyatt: "https://world.hyatt.com/",
      development: "Hyatt development materials — Hyatt Place (public)",
    },
    positioning:
      "Upper-midscale select-service: efficient rooms, Gallery / public-space utility, and World of Hyatt distribution for value-conscious transient demand.",
    commercialDifferentiator:
      "Versus Park Hyatt / Andaz luxury, Hyatt Place is deliberately select-service capital light; versus Courtyard peers, it rides World of Hyatt rather than Bonvoy—do not import luxury fee or staffing assumptions.",
    scenarios: [
      { title: "Corporate Corridor Select-Service", body: "Suburban and corridor sites where midweek corporate demand and World of Hyatt underwrite Hyatt Place." },
      { title: "Airport & Gateway Utility", body: "Airport-adjacent assets where reliable select-service product and loyalty distribution matter more than lifestyle F&B." },
      { title: "New-Build Prototype Efficiency", body: "New-build sites optimized for Hyatt Place prototypes—underwrite development cost discipline against local upper-midscale competitors." },
    ],
    loyaltyProof: HYATT_PROOF("Hyatt Place"),
    feeNote:
      "Hyatt Place application, royalty, program fees, and World of Hyatt assessments — confirm current Hyatt select-service disclosure exhibits. Do not apply luxury-brand fee exhibits. Amounts are not staged without a verified disclosure.",
    operatingModel:
      "Select-service with leaner staffing than full-service Hyatt brands, Gallery breakfast / public space, and World of Hyatt systems. Operating model must not copy Park Hyatt or Grand Hyatt luxury intensity.",
    footprint:
      "Large Hyatt Place footprint across North America with growing international presence. Confirm live counts and pipeline from Hyatt disclosures—scale is select-service, not luxury-curated.",
    insight:
      "Hyatt Place is for efficient select-service economics with World of Hyatt—wrong when sponsors underwrite luxury lifestyle capital or expect Park Hyatt rates.",
    standards: [
      { title: "Select-Service Prototype Discipline", body: "Rooms and Gallery public space must clear Hyatt Place standards without luxury overbuilding." },
      { title: "Lean Operating Model", body: "Staffing and F&B intensity stay select-service—do not import luxury operating costs." },
      { title: "World of Hyatt Integration", body: "World of Hyatt enrollment and Elite delivery required within select-service capability." },
    ],
    valueOwnerScenarios: [
      { title: "Corporate Corridor Efficiency", body: "Corridor sites where select-service and World of Hyatt clear IC." },
      { title: "Airport Utility", body: "Airport assets needing reliable product without luxury capital." },
      { title: "New-Build Cost Discipline", body: "New-builds optimized for Hyatt Place prototypes and local upper-midscale comps." },
      { title: "Sibling Clarity Versus Luxury Hyatt", body: "Do not confuse with Park Hyatt, Grand Hyatt, or Andaz economics." },
    ],
    gallery: [
      { title: "Hyatt Place — Gallery / Public Space", body: "Select-service Gallery public space · confirm property photography before publication", source: "https://www.hyatt.com/hyatt-place" },
      { title: "Hyatt Place — Guest Room", body: "Efficient guest-room product typical of Hyatt Place", source: "https://www.hyatt.com/hyatt-place" },
      { title: "Hyatt Place — Breakfast / Gallery", body: "Breakfast and Gallery utility supporting select-service stays", source: "https://www.hyatt.com/hyatt-place" },
      { title: "Hyatt Place — Exterior", body: "Exterior / arrival presence in market", source: "https://www.hyatt.com/hyatt-place" },
      { title: "Hyatt Place — Meeting Pod / Small Meetings", body: "Small meetings utility where offered", source: "https://www.hyatt.com/hyatt-place" },
      { title: "Hyatt Place — Fitness / Amenity", body: "Fitness and amenity cues typical of select-service", source: "https://www.hyatt.com/hyatt-place" },
    ],
  }),
});

export function getDepthPack(brandId) {
  return (
    DEPTH_PACKS_BY_BRAND_ID[brandId] ||
    BATCH_6_PACKS_BY_ID[brandId] ||
    BATCH_7_PACKS_BY_ID[brandId] ||
    BATCH_8_PACKS_BY_ID[brandId] ||
    BATCH_9_PACKS_BY_ID[brandId] ||
    BATCH_10_PACKS_BY_ID[brandId] ||
    BATCH_11_PACKS_BY_ID[brandId] ||
    BATCH_12_PACKS_BY_ID[brandId] ||
    null
  );
}

export function listPacksByIds(ids) {
  return (ids || []).map((id) => getDepthPack(id)).filter(Boolean);
}

export function listNamedBatchFive() {
  return listPacksByIds(BATCH_FIVE_IDS);
}

export function listBatch3() {
  return listPacksByIds(BATCH_3_IDS);
}

export function listBatch4() {
  return listPacksByIds(BATCH_4_IDS);
}

export function listBatch5() {
  return listPacksByIds(BATCH_5_IDS);
}
