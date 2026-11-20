/**
 * Factory 2.0 depth research packs — named batches (free sources; $0 paid).
 * Fee amounts are never invented — disclosure confirmation language only.
 */
export const FACTORY_2_NAMED_BATCH_FIVE_V1 = "FACTORY_2_NAMED_BATCH_FIVE_V1";
export const FACTORY_2_BATCH_3_V1 = "FACTORY_2_BATCH_3_V1";

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

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    ...base,
  });
}

const BONVOY_PROOF = (brandShort) => [
  {
    title: "Repeat Guest Capture",
    body: `Marriott Bonvoy earn and recognition increase the probability of repeat ${brandShort} stays where the hotel already competes on product fit—not on a standardized prototype alone.`,
  },
  {
    title: "Direct Channel & Member Pricing",
    body: `Bonvoy member rates and Marriott direct paths help ${brandShort} hotels defend contribution against high-commission intermediaries while keeping brand positioning intact.`,
  },
  {
    title: "Elite Member Value",
    body: `Elite recognition (upgrades where available, late checkout, milestone rewards) supports contribution at ${brandShort}; amenity delivery still varies by property capability.`,
  },
  {
    title: "Cross-Brand Traveler Flow",
    body: `Bonvoy recognition across Marriott sister brands matters for guests who split stays within the network—${brandShort} remains the brand-specific node inside that earn/redeem graph.`,
  },
  {
    title: "Cobrand & Payment Scale",
    body: `Bonvoy co-branded cards, Moments, and bonus-point campaigns lift acquisition; model hotel-level contribution after loyalty chargebacks—not portfolio vanity metrics.`,
  },
  {
    title: "Corporate & Small Meetings",
    body: `Marriott account linkage and negotiated transient patterns support midweek compression where corporate and SMERF mix fits the ${brandShort} asset—confirm for this hotel's source markets.`,
  },
];

const IHG_PROOF = (brandShort) => [
  {
    title: "Repeat Guest Capture",
    body: `IHG One Rewards earn and recognition support repeat ${brandShort} stays when the hotel already delivers the brand promise—membership alone does not replace product fit.`,
  },
  {
    title: "Direct Channel & Member Pricing",
    body: `IHG One Rewards member rates and IHG direct paths help ${brandShort} defend contribution versus high-commission intermediaries; confirm current rate rules before IC.`,
  },
  {
    title: "Elite Member Value",
    body: `Elite recognition benefits support contribution at ${brandShort} where the property can deliver upgrades and service standards; capability still varies by asset.`,
  },
  {
    title: "Cross-Brand Traveler Flow",
    body: `One Rewards recognition across IHG sister brands matters for multi-brand itineraries—${brandShort} is the brand-specific earn/redeem node inside that network.`,
  },
  {
    title: "Partner & Payment Scale",
    body: `IHG One Rewards partnerships and bonus campaigns expand acquisition; underwrite hotel-level net contribution after loyalty assessments—not headline membership counts.`,
  },
  {
    title: "Corporate & Negotiated Demand",
    body: `IHG account and negotiated transient patterns can support midweek compression where corporate mix fits ${brandShort}—validate against this hotel's source markets.`,
  },
];

const HONORS_PROOF = (brandShort) => [
  {
    title: "Repeat Guest Capture",
    body: `Hilton Honors earn and recognition increase the odds of repeat ${brandShort} stays where neighborhood lifestyle product already competes—points do not substitute for place.`,
  },
  {
    title: "Direct Channel & Member Pricing",
    body: `Honors member rates and Hilton direct channels help ${brandShort} defend contribution against high-commission intermediaries while keeping the local lifestyle narrative.`,
  },
  {
    title: "Elite Member Value",
    body: `Elite recognition (upgrades where available, flexible benefits) supports contribution at ${brandShort}; delivery still depends on property inventory and service capacity.`,
  },
  {
    title: "Cross-Brand Traveler Flow",
    body: `Honors recognition across Hilton sister brands matters for guests who split stays—${brandShort} remains the lifestyle-neighborhood node inside that earn/redeem graph.`,
  },
  {
    title: "Cobrand & Payment Scale",
    body: `Honors co-branded cards and bonus campaigns lift acquisition; model hotel-level contribution after loyalty chargebacks—not portfolio vanity metrics.`,
  },
  {
    title: "Corporate & Weekend Mix",
    body: `Hilton account linkage can support midweek compression while ${brandShort}'s neighborhood leisure story carries weekends—confirm mix for this asset before IC.`,
  },
];

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
});

export function getDepthPack(brandId) {
  return DEPTH_PACKS_BY_BRAND_ID[brandId] || null;
}

export function listPacksByIds(ids) {
  return (ids || []).map((id) => DEPTH_PACKS_BY_BRAND_ID[id]).filter(Boolean);
}

export function listNamedBatchFive() {
  return listPacksByIds(BATCH_FIVE_IDS);
}

export function listBatch3() {
  return listPacksByIds(BATCH_3_IDS);
}
