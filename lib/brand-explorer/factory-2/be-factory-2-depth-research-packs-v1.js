/**
 * Factory 2.0 depth research packs — first named five-brand batch.
 * Evidence-backed free sources only; paid budget $0.
 * Fee amounts are never invented — disclosure confirmation language only.
 */
export const FACTORY_2_NAMED_BATCH_FIVE_V1 = "FACTORY_2_NAMED_BATCH_FIVE_V1";

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
});

export function getDepthPack(brandId) {
  return DEPTH_PACKS_BY_BRAND_ID[brandId] || null;
}

export function listNamedBatchFive() {
  return Object.values(DEPTH_PACKS_BY_BRAND_ID);
}
