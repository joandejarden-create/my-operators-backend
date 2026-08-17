/**
 * Wave 13 Stage 3 — curated official source pack content (read-only).
 * No Airtable / Presentation / Brand Status / release / CV / Source / Registry writes.
 * Match property URLs by property name (never array index).
 * CALA examples first; otherwise label International Reference.
 */
export const WAVE13_SOURCE_PACKS_VERSION = "wave13-source-packs-v1";

export const SAFE_TGS_OPTIONS = Object.freeze([
  "Experience-Oriented",
  "Leisure",
  "Bleisure",
  "International Inbound",
]);

export const TGS_AVOID_NOTE =
  "Do not combine Luxury / Discerning with Leisure (or Experience-Oriented adjacency that renders as generic audience prose). Prefer brand-specific Bleisure / Experience-Oriented / Leisure / International Inbound only when source-supported.";

function pack(partial) {
  return Object.freeze({
    writeAirtable: false,
    writeBrandStatus: false,
    writeReleaseFields: false,
    writeTargetGuestSegments: false,
    writePresentation: false,
    writeCompanyValidated: false,
    writeSourceLibrary: false,
    writeRegistry: false,
    ...partial,
  });
}

export const WAVE13_SOURCE_PACKS_BY_SLUG = Object.freeze({
  "mama-shelter": pack({
    slug: "mama-shelter",
    name: "Mama Shelter",
    brandBasicsName: "Mama Shelter",
    recordId: "recXCZCK05XXYX7Q8",
    brandStatus: "Under Review",
    parentPlatform: "Accor / Ennismore (lifestyle collective)",
    family: "accor-ennismore",
    lens:
      "Affordable urban lifestyle brand with playful design, social F&B, and neighborhood placemaking. Part of Ennismore’s lifestyle collective under Accor. Distinguish from Moxy, Bunkhouse, Hotel Indigo, SO/, and soft lifestyle collections.",
    calaAvailability: "pipeline",
    calaFirstPosture:
      "CALA pipeline supported (Mama Shelter Mexico City — opening end of 2026 on Accor ALL). Until open, pair pipeline CALA with International Reference operating examples.",
    internationalReferenceRequired: true,
    officialBrandPage: {
      url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mama-shelter",
      label: "Mama Shelter — Accor Group brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.mamashelter.com/investors",
      label: "Mama Shelter — investors / development (brand site)",
      role: "development_page",
      trust: "high",
      note: "Brand-owned investor page; treat as brand development context, not fee language.",
    },
    parentPlatformContext: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mama-shelter",
        label: "Accor Group — Mama Shelter (Ennismore JV labeled on page)",
        role: "parent_platform",
        trust: "highest",
        note: "Parent/platform context — Ennismore collective + Accor distribution/loyalty.",
      },
      {
        url: "https://assets.group.accor.com/yrj0orc8tx24/5LtY0I2Mr4izMLJgFhDtZd/61e3a105a3e44cb3f7967de3cf98968f/Accor_Brandbook_in_English.pdf",
        label: "Accor Brandbook (March 2026) — Mama Shelter entry",
        role: "parent_platform",
        trust: "high",
        note: "Parent brandbook context only; do not paste fee/ADR stacks into owner-facing copy.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Mama Shelter Mexico City",
        url: "https://all.accor.com/hotel/C4I1/index.en.shtml",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico (pipeline — opening end of 2026)",
        matchKey: "Mama Shelter Mexico City",
        note: "Official Accor ALL property page; opening timing labeled on page.",
      },
      {
        propertyName: "Mama Shelter Paris East",
        url: "https://all.accor.com/hotel/9921/index.en.shtml",
        geographyLabel: "International Reference",
        market: "Paris, France",
        matchKey: "Mama Shelter Paris East",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "End of 2026 (pipeline)",
        title: "Mama Shelter Mexico City — Accor ALL listing (Roma Norte)",
        summary:
          "Official Accor ALL property page lists Mama Shelter Mexico City opening end of 2026 in Roma Norte — first Hispanic America pipeline signal for CALA examples / openings cards.",
        announcementUrl: "https://all.accor.com/hotel/C4I1/index.en.shtml",
        linkLabel: "Accor ALL — Mama Shelter Mexico City",
        geographyLabel: "CALA",
        whyRelevant:
          "Property-name-matched CALA pipeline opening for Recent Momentum / Openings; do not imply open inventory until opening is confirmed.",
      },
      {
        dateLine: "Ongoing (brandbook 2026)",
        title: "Mama Shelter positioned in Accor/Ennismore lifestyle portfolio",
        summary:
          "Accor Brandbook and Group brand page frame Mama Shelter as affordable, irreverent lifestyle — useful for owner-facing positioning without inventing CALA operating proof.",
        announcementUrl:
          "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mama-shelter",
        linkLabel: "Accor Group — Mama Shelter brand page",
        geographyLabel: "International Reference",
        whyRelevant: "Official brand positioning source for Stage 4 tab copy.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mama-shelter",
        label: "Accor Group Mama Shelter brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://all.accor.com/hotel/C4I1/index.en.shtml",
        label: "Mama Shelter Mexico City property imagery (pipeline)",
        role: "image_source",
        trust: "high",
        note: "Prefer property-page assets tied to this property name only.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Leisure", "Bleisure"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Mama is social lifestyle / urban experience — not luxury collection. Experience-Oriented + Leisure + Bleisure match design-led urban stays without overstating luxury.",
    },
    ownerFacingPositioningNotes: [
      "Lead with social public spaces, F&B, and neighborhood energy — not fee stacks.",
      "Label Ennismore/Accor as parent/platform context; do not conflate with Accor PM&E midscale brands.",
      "Mexico City is pipeline — use International Reference operating examples until open.",
    ],
    distinguishFrom: [
      "moxy-hotels",
      "bunkhouse-hotels",
      "hotel-indigo",
      "so-hotels-and-resorts",
      "mgallery-collection",
    ],
    siblingBrandDistinctionNotes: [
      "vs Moxy / Bunkhouse / Indigo: Mama is Ennismore lifestyle with irreverent F&B/social DNA, not Marriott/IHG lifestyle soft brands.",
      "vs SO/: SO/ is fashion-led luxury lifestyle; Mama is more accessible/popular/irreverent.",
      "vs soft collections (MGallery, etc.): Mama is a branded lifestyle product, not a collection stamp.",
    ],
    sourceGaps: [
      "Limited open CALA inventory — Mexico City still pipeline.",
      "Dedicated Accor development brochure for Mama may be thinner than PM&E brands; use brand investor page carefully.",
    ],
    manualReviewRisks: [
      "Do not invent open CALA presence from pipeline pages.",
      "Avoid confusing Mama Shelter with Mondrian / Hyde / other Ennismore siblings in copy.",
    ],
    stage4ReadinessRecommendation:
      "Ready for Stage 4 tab-factory-build after source-pack review — use CALA pipeline + International Reference operating examples; keep Ennismore labeling explicit.",
    notes: [
      "Brand Basics exists (Under Review); 0 Presentation rows at manifest time.",
      "Consumer site mamashelter.com remains a supporting brand source; Accor Group page is primary official brand page.",
    ],
  }),

  mercure: pack({
    slug: "mercure",
    name: "Mercure",
    brandBasicsName: "Mercure",
    recordId: "recevrLJ3m6rIug3S",
    brandStatus: "Under Review",
    parentPlatform: "Accor (Premium, Midscale & Economy)",
    family: "accor",
    lens:
      "Accor midscale brand built around local inspiration, destination discovery, and conversion-friendly platform relevance. Distinguish from ibis, Novotel, Pullman, and Grand Mercure.",
    calaAvailability: "strong",
    calaFirstPosture: "CALA operating examples available (Colombia, Brazil). Prefer CALA property cards first.",
    internationalReferenceRequired: false,
    officialBrandPage: {
      url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mercure-hotels",
      label: "Mercure — Accor Group brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://assets.group.accor.com/yrj0orc8tx24/6IVJQZdEztj7JMSP0LBlgR/70705c611e3a2bbe7000ff74364c4f79/Premium__Midscale___Economy_Global_Development_Presentation.pdf",
      label: "Accor PM&E Global Development Presentation (parent/platform)",
      role: "development_page",
      trust: "high",
      note: "Parent/platform development deck — label clearly; extract Mercure-relevant themes only (local conversion), never fee/ADR stacks.",
    },
    parentPlatformContext: [
      {
        url: "https://all.accor.com/a/en/brands/mercure.html",
        label: "ALL Accor — Mercure brand consumer page",
        role: "parent_platform",
        trust: "highest",
        note: "ALL booking platform brand landing — still brand-specific.",
      },
      {
        url: "https://mercure.accor.com/",
        label: "mercure.accor.com consumer brand site",
        role: "brand_page",
        trust: "highest",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Mercure Bogotá BH Zona Financiera",
        url: "https://all.accor.com/hotel/A535/index.en.shtml",
        geographyLabel: "CALA",
        market: "Bogotá, Colombia",
        matchKey: "Mercure Bogotá BH Zona Financiera",
      },
      {
        propertyName: "Mercure Rio Boutique Copacabana",
        url: "https://all.accor.com/hotel/B215/index.en.shtml",
        geographyLabel: "CALA",
        market: "Rio de Janeiro, Brazil",
        matchKey: "Mercure Rio Boutique Copacabana",
      },
      {
        propertyName: "Mercure Bangkok Sukhumvit 11",
        url: "https://all.accor.com/hotel/A247/index.en.shtml",
        geographyLabel: "International Reference",
        market: "Bangkok, Thailand",
        matchKey: "Mercure Bangkok Sukhumvit 11",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "2024 (Accor PM&E growth narrative)",
        title: "Mercure called out among Accor conversion-friendly midscale brands",
        summary:
          "Accor press on PM&E growth highlights Mercure (with Handwritten / TRIBE) as conversion-friendly — useful for owner-facing platform relevance without inventing deal terms.",
        announcementUrl:
          "https://press.accor.com/accor-drives-unprecedented-growth-and-record-performance-in-new-signings",
        linkLabel: "Accor press — PM&E growth / Mercure conversion note",
        geographyLabel: "International Reference",
        whyRelevant: "Supports conversion/platform relevance for owners; keep as Accor parent press, not brand fee proof.",
      },
      {
        dateLine: "Brandbook March 2026",
        title: "Mercure — feel local everywhere positioning (Accor Brandbook)",
        summary:
          "Official Accor Brandbook restates Mercure’s local immersion / authentic cuisine positioning for midscale discovery stays.",
        announcementUrl:
          "https://assets.group.accor.com/yrj0orc8tx24/5LtY0I2Mr4izMLJgFhDtZd/61e3a105a3e44cb3f7967de3cf98968f/Accor_Brandbook_in_English.pdf",
        linkLabel: "Accor Brandbook — Mercure",
        geographyLabel: "International Reference",
        whyRelevant: "Canonical official positioning language for Stage 4.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mercure-hotels",
        label: "Accor Group Mercure brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://all.accor.com/hotel/A535/index.en.shtml",
        label: "Mercure Bogotá BH Zona Financiera property imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Leisure", "Bleisure", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Mercure is midscale local-discovery — Leisure + Bleisure + International Inbound fit without luxury framing.",
    },
    ownerFacingPositioningNotes: [
      "Emphasize local design/F&B and conversion-friendly midscale platform — not Grand Mercure or Pullman premium.",
      "CALA examples (Bogotá / Rio) should lead Openings / Examples.",
    ],
    distinguishFrom: ["ibis", "novotel", "pullman", "grand-mercure"],
    siblingBrandDistinctionNotes: [
      "vs ibis: Mercure is midscale local immersion, not economy essential-stay.",
      "vs Novotel: Mercure leans destination/local storytelling; Novotel leans family/business wellbeing mix.",
      "vs Pullman: Pullman is premium meetings/lifestyle; Mercure is midscale.",
      "vs Grand Mercure: sibling upper midscale / regional line — do not merge identities.",
    ],
    sourceGaps: [
      "Brand-specific owner development microsite is thinner than consumer brand pages — rely on Group brand page + labeled PM&E decks.",
    ],
    manualReviewRisks: [
      "Grand Mercure name collision in search / AI copy.",
      "Do not cite ibis Styles properties as Mercure examples.",
    ],
    stage4ReadinessRecommendation:
      "Ready for Stage 4 after review — strong CALA property set; keep Grand Mercure sibling distinction explicit.",
    notes: ["Brand Basics exists (Under Review)."],
  }),

  ibis: pack({
    slug: "ibis",
    name: "ibis",
    brandBasicsName: "ibis",
    recordId: "reclFXbpZ5XzLWbGP",
    brandStatus: "Under Review",
    parentPlatform: "Accor (Premium, Midscale & Economy)",
    family: "accor",
    lens:
      "Accor economy master brand — essential stay, efficient operations, social value. This pack is for **ibis** (master), not ibis Styles or ibis budget unless labeled as family context.",
    calaAvailability: "strong",
    calaFirstPosture:
      "CALA operating examples available. Prefer master-brand ibis property pages; avoid ibis Styles / ibis budget URLs unless explicitly family context.",
    internationalReferenceRequired: false,
    officialBrandPage: {
      url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/ibis",
      label: "ibis — Accor Group brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://assets.group.accor.com/yrj0orc8tx24/6IVJQZdEztj7JMSP0LBlgR/70705c611e3a2bbe7000ff74364c4f79/Premium__Midscale___Economy_Global_Development_Presentation.pdf",
      label: "Accor PM&E Global Development Presentation (parent/platform)",
      role: "development_page",
      trust: "high",
      note: "Parent/platform deck — extract economy/ibis themes only; label clearly.",
    },
    parentPlatformContext: [
      {
        url: "https://all.accor.com/a/en/brands/ibis.html",
        label: "ALL Accor — ibis brand page",
        role: "parent_platform",
        trust: "highest",
      },
      {
        url: "https://assets.group.accor.com/yrj0orc8tx24/5LtY0I2Mr4izMLJgFhDtZd/61e3a105a3e44cb3f7967de3cf98968f/Accor_Brandbook_in_English.pdf",
        label: "Accor Brandbook — ibis / ibis Styles / ibis budget family map",
        role: "parent_platform",
        trust: "high",
        note: "Use only to keep sibling lines distinct — do not merge Styles/budget into master ibis copy.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "ibis Mexico Alameda",
        url: "https://all.accor.com/hotel/9011/index.en.shtml",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "ibis Mexico Alameda",
        note: "Master ibis property — not ibis Styles Mexico Reforma.",
      },
      {
        propertyName: "ibis Lima Larco Miraflores",
        url: "https://all.accor.com/hotel/6971/index.en.shtml",
        geographyLabel: "CALA",
        market: "Lima, Peru",
        matchKey: "ibis Lima Larco Miraflores",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "2024–2025 (Accor PM&E)",
        title: "ibis called out as Accor economy growth brand (80th country narrative)",
        summary:
          "Accor PM&E growth press highlights ibis expansion (including 80th-country milestone narrative for 2025) — use as network momentum, not fee language.",
        announcementUrl:
          "https://press.accor.com/accor-drives-unprecedented-growth-and-record-performance-in-new-signings",
        linkLabel: "Accor press — PM&E / ibis growth",
        geographyLabel: "International Reference",
        whyRelevant: "Official Accor momentum for economy master brand network growth.",
      },
      {
        dateLine: "Brandbook March 2026",
        title: "ibis — cozy comfort / social economy positioning",
        summary:
          "Accor Brandbook positions ibis as world-leading economy brand with cozy comfort and social connection for budget-conscious travelers.",
        announcementUrl:
          "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/ibis",
        linkLabel: "Accor Group — ibis brand page",
        geographyLabel: "International Reference",
        whyRelevant: "Canonical master-brand positioning distinct from Styles/budget.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/ibis",
        label: "Accor Group ibis brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://all.accor.com/hotel/9011/index.en.shtml",
        label: "ibis Mexico Alameda property imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Leisure", "Bleisure", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency", "Experience-Oriented overstatement"],
      rationale:
        "Economy essential-stay: Leisure + Bleisure + International Inbound. Avoid luxury / overstated lifestyle framing.",
    },
    ownerFacingPositioningNotes: [
      "Keep master ibis identity — never present Styles design story or budget pricing as ibis master proof.",
      "CALA examples should be property-name matched to ibis (not Styles).",
    ],
    distinguishFrom: ["ibis-styles", "ibis-budget", "mercure", "novotel"],
    siblingBrandDistinctionNotes: [
      "vs ibis Styles: design-led economy sibling — family context only.",
      "vs ibis budget: ultra-economy sibling — family context only.",
      "vs Mercure/Novotel: midscale; do not upscale ibis copy into midscale claims.",
    ],
    sourceGaps: [
      "ALL search UIs often surface Styles adjacent — stewards must filter property match keys carefully.",
    ],
    manualReviewRisks: [
      "High wrong-brand carryover risk from ibis Styles Mexico Reforma and ibis budget siblings near CALA examples.",
      "Confirm Lima property page remains master ibis before Stage 4 apply.",
    ],
    stage4ReadinessRecommendation:
      "Ready for Stage 4 after review — enforce master-brand property matching; flag any Styles/budget URL as FAIL.",
    notes: ["Brand Basics name is lowercase 'ibis' — preserve Accor casing."],
  }),

  novotel: pack({
    slug: "novotel",
    name: "Novotel",
    brandBasicsName: "Novotel",
    recordId: "recQE2lSSSSyuUrMQ",
    brandStatus: "Under Review",
    parentPlatform: "Accor (Premium, Midscale & Economy)",
    family: "accor",
    lens:
      "Accor midscale / upper-midscale brand for family + business with wellbeing, meetings, and leisure/business mix. Distinguish from Mercure, Pullman, and ibis.",
    calaAvailability: "strong",
    calaFirstPosture: "CALA operating examples available (Mexico City). Prefer CALA first.",
    internationalReferenceRequired: false,
    officialBrandPage: {
      url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/novotel",
      label: "Novotel — Accor Group brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://assets.group.accor.com/yrj0orc8tx24/4JuuFfBNy816VDPMP1H2Ne/f14253389b17d860b9679ab9e2cbe509/Why_invest_in_Novotel_2026.pdf",
      label: "Why invest in Novotel 2026 (Accor development PDF)",
      role: "development_page",
      trust: "highest",
      note: "Official development PDF — do not paste fee/ADR/GOP claims into public owner-facing body; use for positioning themes only.",
    },
    parentPlatformContext: [
      {
        url: "https://novotel.accor.com/",
        label: "novotel.accor.com consumer brand site",
        role: "brand_page",
        trust: "highest",
      },
      {
        url: "https://assets.group.accor.com/yrj0orc8tx24/5LtY0I2Mr4izMLJgFhDtZd/61e3a105a3e44cb3f7967de3cf98968f/Accor_Brandbook_in_English.pdf",
        label: "Accor Brandbook — Novotel",
        role: "parent_platform",
        trust: "high",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Novotel Mexico City World Trade Center",
        url: "https://all.accor.com/hotel/B552/index.en.shtml",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "Novotel Mexico City World Trade Center",
      },
      {
        propertyName: "Novotel Mexico City Centro Histórico",
        url: "https://all.accor.com/hotel/B904/index.en.shtml",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "Novotel Mexico City Centro Histórico",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "2026 (development PDF)",
        title: "Why invest in Novotel 2026 — wellbeing / longevity midscale platform",
        summary:
          "Accor development PDF restates Novotel’s wellbeing, family/business, and adaptive design themes for owners — use themes only, not numeric performance claims in public body.",
        announcementUrl:
          "https://assets.group.accor.com/yrj0orc8tx24/4JuuFfBNy816VDPMP1H2Ne/f14253389b17d860b9679ab9e2cbe509/Why_invest_in_Novotel_2026.pdf",
        linkLabel: "Accor — Why invest in Novotel 2026",
        geographyLabel: "International Reference",
        whyRelevant: "Official development positioning for Stage 4 owner-facing notes.",
      },
      {
        dateLine: "Early 2025 (Accor openings line-up)",
        title: "Novotel Valencia Lavant / other 2025 openings (Accor line-up)",
        summary:
          "Accor 2025 openings communications include Novotel conversions/openings (e.g. Valencia) — International Reference momentum only unless CALA-specific announcements appear.",
        announcementUrl: "https://www.webwire.com/ViewPressRel.asp?aId=331112",
        linkLabel: "Accor 2025 openings line-up (via WebWire)",
        geographyLabel: "International Reference",
        whyRelevant: "Dated openings momentum; prefer Accor primary URLs when Stage 4 materializes cards.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/novotel",
        label: "Accor Group Novotel brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://all.accor.com/hotel/B552/index.en.shtml",
        label: "Novotel Mexico City World Trade Center property imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Family/business midscale mix maps to Bleisure + Leisure + International Inbound without luxury claims.",
    },
    ownerFacingPositioningNotes: [
      "Lead with wellbeing, family, meetings, and adaptive conversion — not Pullman premium or Mercure local boutique.",
      "Strong Mexico City CALA pair for Openings / Examples.",
    ],
    distinguishFrom: ["mercure", "pullman", "ibis"],
    siblingBrandDistinctionNotes: [
      "vs Mercure: Novotel is more standardized family/business wellbeing; Mercure is local immersion.",
      "vs Pullman: Pullman is premium meetings/lifestyle; Novotel is midscale.",
      "vs ibis: economy vs midscale — do not downscale Novotel into essential-stay claims.",
    ],
    sourceGaps: [
      "Prefer Accor primary press URLs over syndicated WebWire when available for Stage 4 cards.",
    ],
    manualReviewRisks: [
      "Do not import Pullman 'exchange' language into Novotel tabs.",
      "Strip numeric GOP/RGI claims from development PDFs before public copy.",
    ],
    stage4ReadinessRecommendation: "Ready for Stage 4 after review — CALA-strong; sanitize development PDF claims.",
    notes: ["Brand Basics exists (Under Review)."],
  }),

  pullman: pack({
    slug: "pullman",
    name: "Pullman",
    brandBasicsName: "Pullman",
    recordId: "recFW9kfqKfOjv7Z1",
    brandStatus: "Under Review",
    parentPlatform: "Accor (Premium, Midscale & Economy)",
    family: "accor",
    lens:
      "Accor premium brand for business/lifestyle exchange — meetings/events, social public space, F&B. Distinguish from Novotel, Fairmont, SO/, Sofitel, and MGallery.",
    calaAvailability: "strong",
    calaFirstPosture: "CALA operating example available (Pullman Lima Miraflores). Prefer CALA first.",
    internationalReferenceRequired: false,
    officialBrandPage: {
      url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/pullman",
      label: "Pullman — Accor Group brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://assets.group.accor.com/yrj0orc8tx24/3AvofituW71skmGx8tvEIX/e68fb604177d597814f11e53fb372e9e/Why_invest_in_Pullman_2026.pdf",
      label: "Why invest in Pullman 2026 (Accor development PDF)",
      role: "development_page",
      trust: "highest",
      note: "Official development PDF — positioning themes only; no fee/ADR/GOP in public body.",
    },
    parentPlatformContext: [
      {
        url: "https://pullman.accor.com/",
        label: "pullman.accor.com consumer brand site",
        role: "brand_page",
        trust: "highest",
      },
      {
        url: "https://group.accor.com/en/news-stories/the-new-era-of-pullman-designed-for-exchange",
        label: "Accor news — New Era of Pullman / xChange (Nov 2025)",
        role: "announcement",
        trust: "highest",
        note: "Brand momentum / positioning — not fee language.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Pullman Lima Miraflores",
        url: "https://all.accor.com/hotel/B464/index.en.shtml",
        geographyLabel: "CALA",
        market: "Lima, Peru",
        matchKey: "Pullman Lima Miraflores",
      },
      {
        propertyName: "Pullman Dubai Downtown",
        url: "https://all.accor.com/hotel/B8D7/index.en.shtml",
        geographyLabel: "International Reference",
        market: "Dubai, UAE",
        matchKey: "Pullman Dubai Downtown",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "November 18, 2025",
        title: "New Era of Pullman — Designed for Exchange / Pullman xChange Dubai",
        summary:
          "Accor Group news story dated Nov 18, 2025 covers Pullman’s renewed exchange positioning and inaugural Pullman xChange in Dubai (Nov 13–14, 2025).",
        announcementUrl:
          "https://group.accor.com/en/news-stories/the-new-era-of-pullman-designed-for-exchange",
        linkLabel: "Accor Group — New Era of Pullman",
        geographyLabel: "International Reference",
        whyRelevant: "Dated official brand repositioning / momentum for Recent Momentum cards.",
      },
      {
        dateLine: "2026 (development PDF)",
        title: "Why invest in Pullman 2026 — premium meetings & social spaces",
        summary:
          "Accor development PDF frames Pullman for MICE, business & leisure social experiences and flexible events spaces — themes only for owner-facing copy.",
        announcementUrl:
          "https://assets.group.accor.com/yrj0orc8tx24/3AvofituW71skmGx8tvEIX/e68fb604177d597814f11e53fb372e9e/Why_invest_in_Pullman_2026.pdf",
        linkLabel: "Accor — Why invest in Pullman 2026",
        geographyLabel: "International Reference",
        whyRelevant: "Official development positioning; sanitize metrics before Stage 4.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/pullman",
        label: "Accor Group Pullman brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://all.accor.com/hotel/B464/index.en.shtml",
        label: "Pullman Lima Miraflores property imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Experience-Oriented", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Premium business/lifestyle exchange maps to Bleisure + Experience-Oriented + International Inbound without Fairmont-level luxury framing.",
    },
    ownerFacingPositioningNotes: [
      "Lead with meetings/events, social lobby, F&B exchange — not Fairmont heritage luxury or SO/ fashion luxury.",
      "CALA: Pullman Lima Miraflores is the primary property-name-matched example.",
    ],
    distinguishFrom: [
      "novotel",
      "fairmont-hotels-and-resorts",
      "so-hotels-and-resorts",
      "sofitel",
      "mgallery-collection",
    ],
    siblingBrandDistinctionNotes: [
      "vs Novotel: premium vs midscale.",
      "vs Fairmont: Pullman is premium contemporary exchange; Fairmont is landmark luxury heritage.",
      "vs SO/: fashion-led Ennismore luxury lifestyle vs Accor PM&E premium.",
      "vs Sofitel / MGallery: different luxury/collection lanes — keep Pullman in premium PM&E.",
    ],
    sourceGaps: [
      "Confirm Pullman Dubai Downtown hotel code still resolves before Stage 4 image pull if used.",
    ],
    manualReviewRisks: [
      "Do not paste GOP margin language from development PDFs into public tabs.",
      "Avoid Fairmont/Sofitel adjacency in hero copy.",
    ],
    stage4ReadinessRecommendation: "Ready for Stage 4 after review — strong dated momentum + CALA property.",
    notes: ["Brand Basics exists (Under Review)."],
  }),

  "so-hotels-and-resorts": pack({
    slug: "so-hotels-and-resorts",
    name: "SO/ Hotels & Resorts",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    missingBrandBasics: true,
    parentPlatform: "Accor / Ennismore (lifestyle collective)",
    family: "accor-ennismore",
    lens:
      "Fashion-led luxury lifestyle collection (Ennismore). No Brand Basics record yet — Stage 3 documents creation recommendation only; does not create the record.",
    calaAvailability: "none_found",
    calaFirstPosture:
      "No reliable CALA operating examples located on official SO/ pages. Mark all property examples International Reference until CALA opens on official sources.",
    internationalReferenceRequired: true,
    officialNamingAssessment: {
      variantsSeen: ["SO/", "SO/ Hotels", "SO/ Hotels & Resorts", "SO Hotels & Resorts"],
      recommendedDisplayName: "SO/ Hotels & Resorts",
      recommendedBrandBasicsName: "SO/",
      rationale:
        "Accor Group / Brandbook short name is **SO/**; consumer site so-hotels.com footer/branding uses **SO/ Hotels & Resorts**. Recommend Brand Basics Name = `SO/` (portfolio alignment) with slug `so-hotels-and-resorts` and display alias `SO/ Hotels & Resorts` in explorer copy.",
      doNotCreateInThisStage: true,
    },
    brandBasicsCreationRecommendation: {
      brandName: "SO/",
      slug: "so-hotels-and-resorts",
      parentPlatform: "AccorHotels",
      initialBrandStatus: "Under Review",
      notes: [
        "Create in a separate Basics creation stage — not Stage 3.",
        "Display alias for explorer: SO/ Hotels & Resorts.",
        "Parent/platform: Accor + Ennismore JV (label Ennismore on brand page).",
        "Do not set Active/Live until factory gates + founder approval.",
      ],
    },
    officialBrandPage: {
      url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/so-hotels",
      label: "SO/ Hotels — Accor Group brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://so-hotels.com/en/",
      label: "SO/ Hotels & Resorts — official consumer site",
      role: "development_page",
      trust: "highest",
      note: "Brand site doubles as brand universe / property directory; dedicated owner PDF not required for Stage 3.",
    },
    parentPlatformContext: [
      {
        url: "https://assets.group.accor.com/yrj0orc8tx24/5LtY0I2Mr4izMLJgFhDtZd/61e3a105a3e44cb3f7967de3cf98968f/Accor_Brandbook_in_English.pdf",
        label: "Accor Brandbook — SO/ entry",
        role: "parent_platform",
        trust: "high",
        note: "Parent brandbook lists brand as SO/.",
      },
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/so-hotels",
        label: "Accor Group — Ennismore labeling on SO/ page",
        role: "parent_platform",
        trust: "highest",
      },
    ],
    propertyExamples: [
      {
        propertyName: "SO/ Paris",
        url: "https://so-hotels.com/en/paris",
        geographyLabel: "International Reference",
        market: "Paris, France",
        matchKey: "SO/ Paris",
      },
      {
        propertyName: "SO/ Maldives",
        url: "https://so-hotels.com/en/maldives",
        geographyLabel: "International Reference",
        market: "Maldives",
        matchKey: "SO/ Maldives",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Brandbook March 2026",
        title: "SO/ — fashion-rooted luxury lifestyle collection (Accor Brandbook)",
        summary:
          "Official Accor Brandbook positions SO/ as a fashion-born luxury lifestyle collection with network + pipeline counts — use positioning only, not room counts as diligence proof.",
        announcementUrl:
          "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/so-hotels",
        linkLabel: "Accor Group — SO/ Hotels",
        geographyLabel: "International Reference",
        whyRelevant: "Official brand identity source while Basics record is still missing.",
      },
      {
        dateLine: "Ongoing (brand site)",
        title: "SO/ Paris featured as fashion/art lifestyle flagship on so-hotels.com",
        summary:
          "Official SO/ site presents SO/ Paris as fashion-and-art-led lifestyle flagship — International Reference opening/example candidate.",
        announcementUrl: "https://so-hotels.com/en/paris",
        linkLabel: "SO/ Paris — official property page",
        geographyLabel: "International Reference",
        whyRelevant: "Property-name-matched example for Openings / Examples until CALA exists.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://so-hotels.com/en/",
        label: "SO/ Hotels official site imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://so-hotels.com/en/paris",
        label: "SO/ Paris property imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Fashion-led lifestyle luxury: Experience-Oriented + International Inbound. Avoid Leisure adjacency with luxury discerning labels.",
    },
    ownerFacingPositioningNotes: [
      "Do not create Brand Basics in Stage 3 — recommendation only.",
      "Label Ennismore/Accor parent context clearly.",
      "No CALA implication until official property pages exist.",
    ],
    distinguishFrom: [
      "fairmont-hotels-and-resorts",
      "mgallery-collection",
      "mama-shelter",
      "the-house-of-originals",
    ],
    siblingBrandDistinctionNotes: [
      "vs Fairmont: SO/ is fashion/art lifestyle; Fairmont is heritage landmark luxury.",
      "vs Mama Shelter: SO/ is luxury fashion-led; Mama is accessible irreverent lifestyle.",
      "vs MGallery / House of Originals / Morgans Originals: different collection/soft-brand lanes.",
    ],
    sourceGaps: [
      "No Brand Basics record.",
      "No CALA official property examples located.",
      "Naming dualism (SO/ vs SO/ Hotels & Resorts) needs founder confirmation at Basics creation.",
    ],
    manualReviewRisks: [
      "Creating Basics with wrong display name.",
      "Confusing with Sofitel / SO Softel legacy naming.",
    ],
    stage4ReadinessRecommendation:
      "NOT ready for Stage 4 tab-factory-build until Brand Basics is created in a separate stage. Source pack is ready to support that creation + later factory build.",
    notes: [
      "classification: missing_brand_basics_record",
      "Factory preview cohort includes slug with recordId null — code-only overlay.",
    ],
  }),

  "fairmont-hotels-and-resorts": pack({
    slug: "fairmont-hotels-and-resorts",
    name: "Fairmont",
    brandBasicsName: "Fairmont",
    recordId: "recJhPaDVU3YUDQUt",
    brandStatus: "Under Review",
    parentPlatform: "Accor (Luxury)",
    family: "accor-luxury",
    lens:
      "Accor luxury landmark / heritage brand — urban and resort luxury, celebrations, mixed-use relevance where supported. Distinguish from SO/, Raffles, Sofitel, MGallery, and Pullman.",
    calaAvailability: "strong",
    calaFirstPosture: "CALA operating example available (Fairmont Mayakoba). Prefer CALA first.",
    internationalReferenceRequired: false,
    namingDisplayIssue: {
      brandBasicsName: "Fairmont",
      slug: "fairmont-hotels-and-resorts",
      consumerOfficialOftenUses: "Fairmont Hotels & Resorts",
      recommendation:
        "Document only — do **not** rename Brand Basics in Stage 3. Explorer display may continue as Fairmont; consumer/legal often uses Fairmont Hotels & Resorts. Founder may later decide to align Brand Name without touching Stage 3.",
      renameInThisStage: false,
    },
    officialBrandPage: {
      url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/fairmont",
      label: "Fairmont — Accor Group brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.fairmont.com/en.html",
      label: "fairmont.com — official consumer brand site",
      role: "development_page",
      trust: "highest",
      note: "Consumer brand hub; Accor Group page remains primary corporate brand page.",
    },
    parentPlatformContext: [
      {
        url: "https://group.accor.com/en/news-stories/fairmont-pursuit-of-making-special-happen",
        label: "Accor Group — Fairmont Make Special Happen (Nov 10, 2025)",
        role: "announcement",
        trust: "highest",
      },
      {
        url: "https://assets.group.accor.com/yrj0orc8tx24/5LtY0I2Mr4izMLJgFhDtZd/61e3a105a3e44cb3f7967de3cf98968f/Accor_Brandbook_in_English.pdf",
        label: "Accor Brandbook — Fairmont luxury entry",
        role: "parent_platform",
        trust: "high",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Fairmont Mayakoba",
        url: "https://www.fairmont.com/en/hotels/riviera-maya/fairmont-mayakoba.html",
        geographyLabel: "CALA",
        market: "Riviera Maya, Mexico",
        matchKey: "Fairmont Mayakoba",
      },
      {
        propertyName: "Fairmont San Francisco",
        url: "https://www.fairmont.com/en/hotels/san-francisco/fairmont-san-francisco.html",
        geographyLabel: "International Reference",
        market: "San Francisco, USA",
        matchKey: "Fairmont San Francisco",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "May 19, 2025",
        title: "Fairmont Presents “Make Special Happen” global brand campaign",
        summary:
          "Fairmont Hotels & Resorts unveiled the Make Special Happen campaign (PR Newswire / Accor ecosystem) celebrating heritage as host of storied celebrations.",
        announcementUrl:
          "https://www.prnewswire.com/news-releases/fairmont-hotels--resorts-unveils-new-global-brand-campaign--fairmont-presents-make-special-happen--a-cinematic-tribute-to-the-brands-heritage-as-storied-celebrators-302458697.html",
        linkLabel: "PR Newswire — Fairmont Make Special Happen",
        geographyLabel: "International Reference",
        whyRelevant: "Dated brand campaign momentum; Accor Group follow-up story Nov 2025 reinforces longevity.",
      },
      {
        dateLine: "November 10, 2025",
        title: "Fairmont’s Pursuit of Making Special Happen (Accor Group)",
        summary:
          "Accor Group news story expands the campaign into on-property Special Happens experiences — structured source for Recent Momentum.",
        announcementUrl:
          "https://group.accor.com/en/news-stories/fairmont-pursuit-of-making-special-happen",
        linkLabel: "Accor Group — Fairmont Make Special Happen",
        geographyLabel: "International Reference",
        whyRelevant: "Official Accor-dated follow-through on brand campaign.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/fairmont",
        label: "Accor Group Fairmont brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://www.fairmont.com/en/hotels/riviera-maya/fairmont-mayakoba.html",
        label: "Fairmont Mayakoba property imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "Landmark luxury: Experience-Oriented + International Inbound. Avoid Leisure adjacency with Luxury/Discerning labels.",
    },
    ownerFacingPositioningNotes: [
      "Lead with heritage, landmark gatherings, resort/urban luxury — not Pullman meetings premium or SO/ fashion.",
      "Naming: keep Brand Basics as Fairmont; document Hotels & Resorts as consumer/legal phrasing only.",
      "Mixed-use/residential only where source-supported — do not invent.",
    ],
    distinguishFrom: [
      "so-hotels-and-resorts",
      "raffles",
      "sofitel",
      "mgallery-collection",
      "pullman",
    ],
    siblingBrandDistinctionNotes: [
      "vs SO/: heritage landmark vs fashion lifestyle.",
      "vs Raffles / Sofitel: sibling luxury lanes — keep Fairmont celebration/heritage identity.",
      "vs MGallery: collection vs iconic brand.",
      "vs Pullman: luxury landmark vs premium contemporary.",
    ],
    sourceGaps: [
      "Brand Name vs consumer legal name dualism remains unresolved pending founder decision (no rename now).",
    ],
    manualReviewRisks: [
      "Accidental Brand Name rename to Fairmont Hotels & Resorts.",
      "Pulling residential/mixed-use claims without property-level support.",
    ],
    stage4ReadinessRecommendation:
      "Ready for Stage 4 after review — CALA-strong (Mayakoba); keep naming issue documented, not changed.",
    notes: ["Manifest maps slug fairmont-hotels-and-resorts → Brand Basics name Fairmont."],
  }),

  "the-house-of-originals": pack({
    slug: "the-house-of-originals",
    name: "The House of Originals",
    brandBasicsName: "The House of Originals",
    recordId: "rec7ZPOVYsldGmNfx",
    brandStatus: "Under Review",
    parentPlatform: "Historical Accor/sbe (verify) — current official successor appears Morgans Originals / Ennismore",
    family: "accor-ennismore-historical",
    lens:
      "Historical Accor/sbe luxury lifestyle collection launched 2019. Current Accor/Ennismore portfolio lists **Morgans Originals**, not The House of Originals. Stage 3 verifies/flags — does not assume active House of Originals development.",
    calaAvailability: "none_found",
    calaFirstPosture:
      "No current CALA House of Originals inventory found. Any examples must be International Reference and labeled historical/successor carefully.",
    internationalReferenceRequired: true,
    officialStatusAssessment: {
      status: "likely_superseded_manual_review_required",
      currentOfficialSuccessor: "Morgans Originals",
      successorBrandPage:
        "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/morgans-originals",
      historicalLaunchSource:
        "https://assets.group.accor.com/yrj0orc8tx24/7dD0TXRFpngDoPrGBjXLru/7f1f6212b4be2e021b0bc3d84bac8d71/PR_The-House-of-Originals_050319.pdf",
      ennismorePortfolioNote:
        "Accor–Ennismore JV materials list Morgans Originals among lifestyle brands; The House of Originals is not listed in current Accor Brandbook lifestyle set.",
      recommendation:
        "Flag for founder/manual review before Stage 4. Options: (a) rebuild as historical/archive profile, (b) retarget Wave seat to Morgans Originals (separate slug/Basics), (c) keep Under Review dormant. Do not invent Accor/Ennismore support for active House of Originals development.",
    },
    officialBrandPage: {
      url: "https://assets.group.accor.com/yrj0orc8tx24/7dD0TXRFpngDoPrGBjXLru/7f1f6212b4be2e021b0bc3d84bac8d71/PR_The-House-of-Originals_050319.pdf",
      label: "sbe/Accor PR — The House of Originals launch (March 5, 2019)",
      role: "brand_page",
      trust: "high",
      note: "Historical official launch PDF — not current consumer brand hub.",
    },
    developmentPage: null,
    parentPlatformContext: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/morgans-originals",
        label: "Morgans Originals — current Accor Group brand page (successor context)",
        role: "parent_platform",
        trust: "highest",
        note: "Clearly labeled successor/current brand — not proof that House of Originals remains active.",
      },
      {
        url: "https://assets.group.accor.com/yrj0orc8tx24/4mLcVarmI4pyq7VcQDSgKu/959dcaafeca5d6f5658061d04f2b6550/PR_AccorEnnismore_04102021_Def.pdf",
        label: "Accor–Ennismore JV closing PR (lists Morgans Originals)",
        role: "parent_platform",
        trust: "high",
        note: "Parent/platform — lists Morgans Originals, not House of Originals.",
      },
      {
        url: "https://press.accor.com/global-expansion-independent-spirit-collection-brands-from-accor-and-ennismore-unlock-new-opportunity/",
        label: "Accor press — collection brands incl. Morgans Originals",
        role: "announcement",
        trust: "high",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Sanderson London, a Morgans Originals hotel",
        url: "https://all.accor.com/a/en/brands/morgans-originals.html",
        geographyLabel: "International Reference",
        market: "London, United Kingdom",
        matchKey: "Sanderson London, a Morgans Originals hotel",
        note: "Successor-brand property example — do not label as active House of Originals inventory.",
      },
      {
        propertyName: "St Martins Lane London, a Morgans Originals hotel",
        url: "https://all.accor.com/a/en/brands/morgans-originals.html",
        geographyLabel: "International Reference",
        market: "London, United Kingdom",
        matchKey: "St Martins Lane London, a Morgans Originals hotel",
        note: "Successor-brand property example on ALL Morgans Originals page.",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "March 5, 2019",
        title: "sbe announces The House of Originals with Accor (historical launch)",
        summary:
          "Historical Accor-hosted PR launches The House of Originals (Sanderson, St Martins Lane, etc.). Use only as lineage context.",
        announcementUrl:
          "https://assets.group.accor.com/yrj0orc8tx24/7dD0TXRFpngDoPrGBjXLru/7f1f6212b4be2e021b0bc3d84bac8d71/PR_The-House-of-Originals_050319.pdf",
        linkLabel: "Accor assets — House of Originals launch PR (2019)",
        geographyLabel: "International Reference",
        whyRelevant: "Documents original brand existence; not current active development proof.",
      },
      {
        dateLine: "April 14, 2021",
        title: "Accor launches Morgans Originals (successor brand narrative)",
        summary:
          "Trade/press coverage of Accor’s Morgans Originals launch (Legacy Miami) marks the lifestyle collection successor framing after House of Originals.",
        announcementUrl:
          "https://www.hotel-online.com/news/accor-to-operate-downtown-miamis-new-legacy-hotel-residences-developed-by-rpc-under-the-morgans-originals-portfolio",
        linkLabel: "Hotel Online — Morgans Originals / Legacy Miami",
        geographyLabel: "International Reference",
        whyRelevant: "Dated successor-brand launch signal for manual review.",
      },
    ],
    imageSourceHints: [
      {
        url: "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/morgans-originals",
        label: "Morgans Originals Accor brand imagery (successor)",
        role: "image_source",
        trust: "highest",
        note: "Only if founder chooses successor retarget — not for presenting House of Originals as live.",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "International Inbound"],
      avoid: ["Luxury / Discerning + Leisure adjacency"],
      rationale:
        "If profile proceeds as historical/collection lifestyle: Experience-Oriented + International Inbound. Revisit if retargeted to Morgans Originals.",
    },
    ownerFacingPositioningNotes: [
      "Do not claim active Accor development under The House of Originals without new official support.",
      "Prefer founder decision: archive vs Morgans Originals retarget.",
      "Distinguish from MGallery, Design Hotels, Tribute, Autograph, Curio, Mama Shelter.",
    ],
    distinguishFrom: [
      "mgallery-collection",
      "design-hotels",
      "tribute-portfolio",
      "autograph-collection",
      "curio-collection",
      "mama-shelter",
    ],
    siblingBrandDistinctionNotes: [
      "vs Morgans Originals: likely successor brand — do not treat as simultaneous active peers without founder call.",
      "vs MGallery / Design Hotels / soft collections: different collection mechanics and parents.",
      "vs Mama Shelter: branded lifestyle product vs collection/originals stamp.",
    ],
    sourceGaps: [
      "No current official House of Originals consumer brand hub found.",
      "No CALA House of Originals properties found.",
      "Brand Basics still named The House of Originals while portfolio maps to Morgans Originals.",
    ],
    manualReviewRisks: [
      "HIGH — publishing active brand profile under obsolete name.",
      "Confusing Accor parent claims without current official support.",
      "Property examples belong to Morgans Originals — must be labeled as such.",
    ],
    stage4ReadinessRecommendation:
      "NOT ready for Stage 4 tab-factory-build until founder/manual review decides archive vs Morgans Originals retarget. Source pack documents the issue only.",
    notes: [
      "Brand Basics exists (Under Review) — identity may be stale relative to Accor Brandbook.",
      "Do not write Brand Status / rename / create Morgans Originals Basics in Stage 3.",
    ],
  }),
});

export function getWave13SourcePack(slug) {
  return WAVE13_SOURCE_PACKS_BY_SLUG[slug] || null;
}
