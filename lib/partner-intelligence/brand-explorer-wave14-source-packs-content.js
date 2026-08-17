/**
 * Wave 14 Stage 3 — curated official source pack content (read-only).
 * No Airtable / Presentation / Brand Status / release / CV / Source / Registry writes.
 * CALA examples first; otherwise label International Reference.
 * Four Points Flex ≠ Four Points by Sheraton; StudioRes ≠ Residence Inn / TPS / Element / Apartments.
 */
export const WAVE14_SOURCE_PACKS_VERSION = "wave14-source-packs-v1";

export const SAFE_TGS_OPTIONS = Object.freeze([
  "Experience-Oriented",
  "Leisure",
  "Bleisure",
  "International Inbound",
]);

export const TGS_AVOID_NOTE =
  "Prefer brand-specific Bleisure / Experience-Oriented / Leisure / International Inbound only when source-supported. Do not invent luxury adjacency for midscale / extended-stay lines.";

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

export const WAVE14_SOURCE_PACKS_BY_SLUG = Object.freeze({
  "marriott-hotels": pack({
    slug: "marriott-hotels",
    officialBrandName: "Marriott Hotels",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "Flagship full-service Marriott Hotels brand — not Marriott International corporate. Distinguish from JW Marriott, Sheraton, Westin, Renaissance, Autograph, Tribute.",
    calaAvailability: "supported",
    calaFirstPosture:
      "CALA operating examples available (e.g. Marriott Cancún / Mexico City corridor). Prefer CALA property pages; label International Reference only for non-CALA comps.",
    internationalReferenceRequired: false,
    stage4Readiness: "ready_for_tab_factory_build_after_basics",
    officialBrandPage: {
      url: "https://marriott-hotels.marriott.com/",
      label: "Marriott Hotels — brand site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/marriott",
      label: "Marriott Hotels — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.marriott.com/",
        label: "Marriott International / Marriott Bonvoy (parent/platform)",
        role: "parent_platform",
        trust: "highest",
        note: "Parent/platform context only — do not treat as the Marriott Hotels brand page.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Marriott Cancun Resort",
        url: "https://www.marriott.com/en-us/hotels/cunmc-marriott-cancun-resort/overview/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "Marriott Cancun Resort",
      },
      {
        propertyName: "Mexico City Marriott Reforma Hotel",
        url: "https://www.marriott.com/en-us/hotels/mexmc-mexico-city-marriott-reforma-hotel/overview/",
        geographyLabel: "CALA",
        market: "Mexico City, Mexico",
        matchKey: "Mexico City Marriott Reforma Hotel",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development site)",
        title: "Marriott Hotels positioned as full-service flagship on Marriott development",
        summary:
          "Official hotel-development brand page frames Marriott Hotels as the namesake full-service brand — use for owner positioning, not corporate IR copy.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/marriott",
        linkLabel: "Marriott development — Marriott Hotels",
        geographyLabel: "International Reference",
        whyRelevant: "Official brand identity source; keep distinct from Marriott International corporate narrative.",
      },
    ],
    openingsExamplesPropertiesCandidates: [
      "Marriott Cancun Resort (CALA)",
      "Mexico City Marriott Reforma Hotel (CALA)",
    ],
    imageSourceHints: [
      {
        url: "https://marriott-hotels.marriott.com/",
        label: "Marriott Hotels brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://www.marriott.com/en-us/hotels/cunmc-marriott-cancun-resort/overview/",
        label: "Marriott Cancun Resort property imagery",
        role: "image_source",
        trust: "high",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure", "International Inbound"],
      avoid: ["Corporate-only framing that erases leisure/resort demand"],
      rationale:
        "Full-service flagship serves business + leisure + inbound travelers; avoid implying JW/luxury-only.",
    },
    ownerFacingPositioningNotes: [
      "Lead with flagship full-service Marriott Hotels — never as Marriott International corporate.",
      "Distinguish JW / Autograph / Tribute / Renaissance as separate owner choices.",
      "CALA examples preferred when property URLs match the named hotel.",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Marriott International (parent)",
      "≠ JW Marriott, Sheraton, Westin, Renaissance, Autograph, Tribute",
    ],
    sourceGaps: [
      "Confirm Brand Basics record naming (Marriott Hotels vs Marriott) before Stage 4.",
    ],
    manualReviewRisks: [
      "Corporate vs brand page confusion in steward fields.",
    ],
  }),

  sheraton: pack({
    slug: "sheraton",
    officialBrandName: "Sheraton",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "Full-service Sheraton with legacy repositioning logic. Distinguish from Marriott Hotels, Westin, Four Points by Sheraton, and Four Points Flex by Sheraton.",
    calaAvailability: "supported",
    calaFirstPosture:
      "CALA Sheraton inventory exists (e.g. Cancún / Puerto Vallarta corridors). Prefer CALA; International Reference for non-CALA comps.",
    internationalReferenceRequired: false,
    stage4Readiness: "ready_for_tab_factory_build_after_basics",
    officialBrandPage: {
      url: "https://sheraton.marriott.com/",
      label: "Sheraton — brand site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/sheraton",
      label: "Sheraton — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.marriott.com/",
        label: "Marriott International / Marriott Bonvoy (parent/platform)",
        role: "parent_platform",
        trust: "highest",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Sheraton Cancun Resort & Spa",
        url: "https://www.marriott.com/en-us/hotels/cunsi-sheraton-cancun-resort-and-spa/overview/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "Sheraton Cancun Resort & Spa",
      },
      {
        propertyName: "Sheraton Buganvilias Resort & Convention Center",
        url: "https://www.marriott.com/en-us/hotels/pvrsi-sheraton-buganvilias-resort-and-convention-center/overview/",
        geographyLabel: "CALA",
        market: "Puerto Vallarta, Mexico",
        matchKey: "Sheraton Buganvilias Resort & Convention Center",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development site)",
        title: "Sheraton development positioning on Marriott hotel-development",
        summary:
          "Official development page supports owner-facing full-service Sheraton framing and legacy-to-modern repositioning language without inventing fee stacks.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/sheraton",
        linkLabel: "Marriott development — Sheraton",
        geographyLabel: "International Reference",
        whyRelevant: "Official brand development source for Stage 4 tabs.",
      },
    ],
    openingsExamplesPropertiesCandidates: [
      "Sheraton Cancun Resort & Spa (CALA)",
      "Sheraton Buganvilias Resort & Convention Center (CALA)",
    ],
    imageSourceHints: [
      {
        url: "https://sheraton.marriott.com/",
        label: "Sheraton brand imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure", "International Inbound"],
      avoid: ["Confusing Sheraton with Four Points Flex midscale"],
      rationale: "Full-service meetings + leisure resort demand; keep distinct from Flex midscale.",
    },
    ownerFacingPositioningNotes: [
      "Frame as full-service Sheraton, not Four Points / Flex.",
      "CALA resort examples are strong for openings/gallery when URLs match property names.",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Marriott Hotels, Westin",
      "≠ Four Points by Sheraton",
      "≠ Four Points Flex by Sheraton",
    ],
    sourceGaps: ["Confirm display name Sheraton vs Sheraton Hotels & Resorts in Brand Basics."],
    manualReviewRisks: ["Alias overlap with Four Points Flex search tokens."],
  }),

  westin: pack({
    slug: "westin",
    officialBrandName: "Westin",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "Wellness / premium full-service Westin. Distinguish from Sheraton, Marriott Hotels, W, JW Marriott, Renaissance.",
    calaAvailability: "supported",
    calaFirstPosture:
      "CALA Westin inventory exists (e.g. Cancún). Prefer CALA property pages; International Reference for non-CALA comps.",
    internationalReferenceRequired: false,
    stage4Readiness: "ready_for_tab_factory_build_after_basics",
    officialBrandPage: {
      url: "https://westin.marriott.com/",
      label: "Westin — brand site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/westin",
      label: "Westin — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.marriott.com/",
        label: "Marriott International / Marriott Bonvoy (parent/platform)",
        role: "parent_platform",
        trust: "highest",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "The Westin Resort & Spa, Cancun",
        url: "https://www.marriott.com/en-us/hotels/cunwi-the-westin-resort-and-spa-cancun/overview/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "The Westin Resort & Spa, Cancun",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development site)",
        title: "Westin wellness / premium positioning on Marriott development",
        summary:
          "Official development brand page anchors wellness/premium full-service owner narrative without inventing ADR/fee language.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/westin",
        linkLabel: "Marriott development — Westin",
        geographyLabel: "International Reference",
        whyRelevant: "Official positioning for Stage 4.",
      },
    ],
    openingsExamplesPropertiesCandidates: [
      "The Westin Resort & Spa, Cancun (CALA)",
    ],
    imageSourceHints: [
      {
        url: "https://westin.marriott.com/",
        label: "Westin brand imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Leisure", "Bleisure", "Experience-Oriented"],
      avoid: ["Generic midscale framing"],
      rationale: "Premium wellness full-service — Leisure + Bleisure + Experience-Oriented when source-supported.",
    },
    ownerFacingPositioningNotes: [
      "Lead with wellness / Heavenly / premium full-service cues from official brand sources only.",
      "Keep distinct from W (lifestyle luxury) and JW (luxury).",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Sheraton, Marriott Hotels",
      "≠ W Hotels, JW Marriott, Renaissance",
    ],
    sourceGaps: ["Add second CALA or International Reference property once steward-confirmed."],
    manualReviewRisks: ["Do not pull W Hotels imagery into Westin gallery."],
  }),

  "residence-inn-by-marriott": pack({
    slug: "residence-inn-by-marriott",
    officialBrandName: "Residence Inn by Marriott",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "Upscale extended-stay leader. Distinguish from TownePlace Suites, StudioRes, Element, Apartments by Marriott Bonvoy.",
    calaAvailability: "supported",
    calaFirstPosture:
      "CALA example supported (Residence Inn Merida and other CALA RI inventory). Prefer CALA; International Reference for US/Canada comps when needed.",
    internationalReferenceRequired: false,
    stage4Readiness: "ready_for_tab_factory_build_after_basics",
    officialBrandPage: {
      url: "https://residence-inn.marriott.com/",
      label: "Residence Inn by Marriott — brand site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/residence-inn",
      label: "Residence Inn — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.hotel-development.marriott.com/brands/extended-stay-brands",
        label: "Marriott Longer Stays / Extended Stay brands (parent family context)",
        role: "parent_platform",
        trust: "highest",
        note: "Family context — label clearly; do not merge sibling brand proof.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Residence Inn Merida",
        url: "https://www.marriott.com/en-us/hotels/midri-residence-inn-merida/overview/",
        geographyLabel: "CALA",
        market: "Mérida, Mexico",
        matchKey: "Residence Inn Merida",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development site)",
        title: "Residence Inn on Marriott Longer Stays family page",
        summary:
          "Official Longer Stays family page positions Residence Inn as the extended-stay leader — useful for sibling distinction vs StudioRes / TPS.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/extended-stay-brands",
        linkLabel: "Marriott development — Longer Stays",
        geographyLabel: "International Reference",
        whyRelevant: "Sibling differentiation source for Stage 4.",
      },
    ],
    openingsExamplesPropertiesCandidates: ["Residence Inn Merida (CALA)"],
    imageSourceHints: [
      {
        url: "https://residence-inn.marriott.com/",
        label: "Residence Inn brand imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "International Inbound"],
      avoid: ["Leisure-only framing that ignores extended-stay purpose"],
      rationale: "Extended-stay productivity + longer trips; Bleisure / inbound when source-supported.",
    },
    ownerFacingPositioningNotes: [
      "Lead with extended-stay suites + residential feel — not select-service short-stay.",
      "Explicitly distinguish StudioRes (midscale new-build longer stay) and TownePlace Suites.",
    ],
    siblingBrandDistinctionNotes: [
      "≠ TownePlace Suites by Marriott",
      "≠ StudioRes",
      "≠ Element",
      "≠ Apartments by Marriott Bonvoy",
    ],
    sourceGaps: ["Add a second CALA Residence Inn property when a named hotel with matching official URL is verified."],
    manualReviewRisks: [
      "Keep StudioRes and TownePlace Suites examples out of Residence Inn openings cards.",
    ],
  }),

  "springhill-suites-by-marriott": pack({
    slug: "springhill-suites-by-marriott",
    officialBrandName: "SpringHill Suites by Marriott",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "All-suite select-service. Distinguish from Residence Inn, Fairfield, Courtyard, TownePlace Suites.",
    calaAvailability: "weak_or_limited",
    calaFirstPosture:
      "CALA inventory may be limited — if a named CALA hotel with a matching official property URL is not available, label examples International Reference and do not imply CALA presence.",
    internationalReferenceRequired: true,
    stage4Readiness: "ready_for_tab_factory_build_after_basics_with_intl_ref",
    officialBrandPage: {
      url: "https://springhillsuites.marriott.com/",
      label: "SpringHill Suites by Marriott — brand site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/springhill-suites",
      label: "SpringHill Suites — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.marriott.com/",
        label: "Marriott International / Marriott Bonvoy (parent/platform)",
        role: "parent_platform",
        trust: "highest",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "SpringHill Suites (International Reference — steward to match specific property)",
        url: "https://springhillsuites.marriott.com/",
        geographyLabel: "International Reference",
        market: "US / Canada primary (confirm specific property URL before Stage 4 cards)",
        matchKey: null,
        note: "Do not invent a property URL — bind cards only after property-name match.",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development site)",
        title: "SpringHill Suites all-suite select-service positioning",
        summary:
          "Official development page supports all-suite select-service owner narrative vs Fairfield/Courtyard/RI.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/springhill-suites",
        linkLabel: "Marriott development — SpringHill Suites",
        geographyLabel: "International Reference",
        whyRelevant: "Official brand positioning for Stage 4.",
      },
    ],
    openingsExamplesPropertiesCandidates: [
      "Pending steward-matched property URLs (International Reference until CALA confirmed)",
    ],
    imageSourceHints: [
      {
        url: "https://springhillsuites.marriott.com/",
        label: "SpringHill Suites brand imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure"],
      avoid: ["Extended-stay-only framing reserved for RI/TPS/StudioRes"],
      rationale: "All-suite select-service short-to-medium stays — not Residence Inn extended-stay.",
    },
    ownerFacingPositioningNotes: [
      "All-suite select-service — distinguish from Courtyard/Fairfield and from extended-stay siblings.",
      "Do not imply CALA presence without property-name-matched URLs.",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Residence Inn by Marriott",
      "≠ Fairfield by Marriott",
      "≠ Courtyard by Marriott",
      "≠ TownePlace Suites by Marriott",
    ],
    sourceGaps: [
      "CALA property examples are limited — prefer verified CALA hotels or clearly labeled International Reference.",
    ],
    manualReviewRisks: ["Confirm property URL binding before openings cards go live."],
  }),

  "towneplace-suites-by-marriott": pack({
    slug: "towneplace-suites-by-marriott",
    officialBrandName: "TownePlace Suites by Marriott",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "Longer-stay / extended-stay select-service. Distinguish from Residence Inn, StudioRes, SpringHill Suites, Element.",
    calaAvailability: "weak_or_limited",
    calaFirstPosture:
      "CALA inventory may be limited — prefer any verified CALA TownePlace Suites hotel; otherwise International Reference and do not imply CALA presence.",
    internationalReferenceRequired: true,
    stage4Readiness: "ready_for_tab_factory_build_after_basics_with_intl_ref",
    officialBrandPage: {
      url: "https://towneplacesuites.marriott.com/",
      label: "TownePlace Suites by Marriott — brand site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/towneplace-suites",
      label: "TownePlace Suites — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.hotel-development.marriott.com/brands/extended-stay-brands",
        label: "Marriott Longer Stays / Extended Stay brands (parent family context)",
        role: "parent_platform",
        trust: "highest",
        note: "Family context — keep TPS distinct from RI / StudioRes / Element.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "TownePlace Suites (International Reference — steward to match specific property)",
        url: "https://towneplacesuites.marriott.com/",
        geographyLabel: "International Reference",
        market: "US primary (confirm specific property URL before Stage 4 cards)",
        matchKey: null,
        note: "Do not invent a property URL — bind cards only after property-name match.",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development site)",
        title: "TownePlace Suites on Marriott Longer Stays family page",
        summary:
          "Official Longer Stays page positions TPS alongside RI / Element / StudioRes — use for sibling distinction, not interchangeable proof.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/extended-stay-brands",
        linkLabel: "Marriott development — Longer Stays",
        geographyLabel: "International Reference",
        whyRelevant: "Sibling differentiation for Stage 4.",
      },
    ],
    openingsExamplesPropertiesCandidates: [
      "Pending steward-matched property URLs (International Reference until CALA confirmed)",
    ],
    imageSourceHints: [
      {
        url: "https://towneplacesuites.marriott.com/",
        label: "TownePlace Suites brand imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "International Inbound"],
      avoid: ["Upscale RI-equivalent framing"],
      rationale: "Select-service longer stay — distinct from upscale Residence Inn and midscale StudioRes.",
    },
    ownerFacingPositioningNotes: [
      "Longer-stay select-service — not Residence Inn upscale extended-stay.",
      "Explicitly separate StudioRes (new midscale longer-stay platform).",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Residence Inn by Marriott",
      "≠ StudioRes",
      "≠ SpringHill Suites by Marriott",
      "≠ Element",
    ],
    sourceGaps: [
      "CALA property examples are limited — prefer verified CALA hotels or clearly labeled International Reference.",
    ],
    manualReviewRisks: ["Keep Residence Inn and StudioRes clearly separated in openings and proof cards."],
  }),

  "aloft-hotels": pack({
    slug: "aloft-hotels",
    officialBrandName: "Aloft Hotels",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "Select-service lifestyle. Distinguish from Moxy, AC Hotels, Four Points, Element, W.",
    calaAvailability: "supported",
    calaFirstPosture:
      "CALA Aloft inventory exists (e.g. Cancún / Mexico City corridors). Prefer CALA; International Reference for non-CALA comps.",
    internationalReferenceRequired: false,
    stage4Readiness: "ready_for_tab_factory_build_after_basics",
    officialBrandPage: {
      url: "https://aloft-hotels.marriott.com/",
      label: "Aloft Hotels — brand site",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/aloft",
      label: "Aloft — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.marriott.com/",
        label: "Marriott International / Marriott Bonvoy (parent/platform)",
        role: "parent_platform",
        trust: "highest",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Aloft Cancun",
        url: "https://www.marriott.com/en-us/hotels/cunal-aloft-cancun/overview/",
        geographyLabel: "CALA",
        market: "Cancún, Mexico",
        matchKey: "Aloft Cancun",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (development site)",
        title: "Aloft select-service lifestyle positioning",
        summary:
          "Official development page supports lifestyle select-service owner narrative vs Moxy / AC / Four Points.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/aloft",
        linkLabel: "Marriott development — Aloft",
        geographyLabel: "International Reference",
        whyRelevant: "Official brand positioning for Stage 4.",
      },
    ],
    openingsExamplesPropertiesCandidates: ["Aloft Cancun (CALA)"],
    imageSourceHints: [
      {
        url: "https://aloft-hotels.marriott.com/",
        label: "Aloft brand imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Experience-Oriented", "Bleisure", "Leisure"],
      avoid: ["Luxury / W-equivalent framing"],
      rationale: "Lifestyle select-service — Experience-Oriented + Bleisure without luxury overclaim.",
    },
    ownerFacingPositioningNotes: [
      "Social lobby / lifestyle select-service — not W luxury lifestyle.",
      "Distinguish Moxy (budget lifestyle) and AC (European contemporary select).",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Moxy Hotels",
      "≠ AC Hotels by Marriott",
      "≠ Four Points by Sheraton",
      "≠ Element",
      "≠ W Hotels",
    ],
    sourceGaps: ["Add second CALA Aloft when steward-confirmed."],
    manualReviewRisks: ["Do not pull W imagery into Aloft gallery."],
  }),

  "four-points-flex-by-sheraton": pack({
    slug: "four-points-flex-by-sheraton",
    officialBrandName: "Four Points Flex by Sheraton",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "Affordable midscale conversion-friendly franchise brand under the Sheraton family. Formerly Four Points Express by Sheraton. Distinct from Four Points by Sheraton.",
    calaAvailability: "none_supported",
    calaFirstPosture:
      "No source-supported CALA inventory identified — International Reference required (EMEA / APAC ex-China focus). Do not imply CALA presence.",
    internationalReferenceRequired: true,
    stage4Readiness: "ready_for_tab_factory_build_after_basics_intl_ref_only",
    officialBrandPage: {
      url: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
      label: "Four Points Flex by Sheraton — Marriott hotel development brand page",
      role: "brand_page",
      trust: "highest",
      note: "Development URL path retains fourpointsexpress; page titles Four Points Flex by Sheraton.",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
      label: "Four Points Flex by Sheraton — development / franchise positioning",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.hotel-development.marriott.com/brands/four-points",
        label: "Four Points by Sheraton (sibling brand — do not conflate)",
        role: "sibling_brand",
        trust: "highest",
        note: "Sibling distinction only — never use as Flex proof.",
      },
      {
        url: "https://www.marriott.com/",
        label: "Marriott International / Marriott Bonvoy (parent/platform)",
        role: "parent_platform",
        trust: "highest",
        note: "Parent/platform context only.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "Four Points Flex by Sheraton (International Reference — EMEA conversions)",
        url: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
        geographyLabel: "International Reference",
        market: "Europe / Middle East / Africa / Asia Pacific Excluding China",
        matchKey: null,
        note: "Bind Stage 4 property cards only after steward matches a specific Flex property URL — do not use Four Points by Sheraton hotels.",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "2025-04 (trade press; corroborate against Marriott statements)",
        title: "Marriott plans to expand Four Points Flex portfolio in Europe toward 50+ by end-2026",
        summary:
          "Trade coverage of Marriott’s European midscale expansion for Four Points Flex — use structured Recent Momentum with date + source; do not paste fee/ADR language.",
        announcementUrl:
          "https://skift.com/2025/04/01/marriotts-hotel-push-in-europe-targets-middle-class-and-smaller-cities/",
        linkLabel: "Skift — Four Points Flex Europe expansion",
        geographyLabel: "International Reference",
        whyRelevant: "Credible growth signal for Momentum; prefer Marriott-owned announcement when available.",
      },
      {
        dateLine: "Ongoing (official development)",
        title: "Official Flex conversion-friendly midscale franchise positioning",
        summary:
          "Marriott hotel-development page frames Flex as conversion-friendly midscale with Bonvoy distribution — primary Stage 4 positioning source.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
        linkLabel: "Marriott development — Four Points Flex",
        geographyLabel: "International Reference",
        whyRelevant: "Highest-trust official brand/development source.",
      },
    ],
    openingsExamplesPropertiesCandidates: [
      "Pending steward-matched Flex property URLs (International Reference; never Four Points by Sheraton)",
    ],
    imageSourceHints: [
      {
        url: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
        label: "Four Points Flex development imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "Leisure", "International Inbound"],
      avoid: ["Upscale Four Points by Sheraton framing"],
      rationale: "Affordable midscale / value-conscious traveler — distinct from classic Four Points.",
    },
    ownerFacingPositioningNotes: [
      "Official name: Four Points Flex by Sheraton (rebrand from Four Points Express).",
      "Conversion-friendly franchise / light operational model — do not invent fee stacks.",
      "Never use Four Points by Sheraton properties as Flex examples.",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Four Points by Sheraton (critical)",
      "≠ Sheraton full-service",
    ],
    sourceGaps: [
      "Guest-facing brand microsite may be limited — development page is primary official source.",
      "No CALA examples — International Reference required.",
      "Specific open property URLs need steward match before Stage 4 openings cards.",
    ],
    manualReviewRisks: [
      "Naming path still says fourpointsexpress — confirm steward uses Flex display name.",
      "High risk of conflating with Four Points by Sheraton in Basics / Presentation.",
    ],
  }),

  studiores: pack({
    slug: "studiores",
    officialBrandName: "StudioRes",
    brandBasicsName: null,
    recordId: null,
    brandStatus: null,
    parentPlatform: "Marriott International",
    lens:
      "StudioRes by Marriott — midscale longer-stay / extended-stay new-build (US & Canada) with EMEA conversion variant on Longer Stays family page. Distinct from Residence Inn, TownePlace Suites, Element, Apartments by Marriott Bonvoy.",
    calaAvailability: "none_supported",
    calaFirstPosture:
      "No source-supported CALA inventory identified — International Reference required (US/Canada new-build; EMEA conversion option). Do not imply CALA presence.",
    internationalReferenceRequired: true,
    stage4Readiness: "ready_for_tab_factory_build_after_basics_intl_ref_only",
    officialBrandPage: {
      url: "https://www.marriott.com/brands/studiores.mi",
      label: "StudioRes — Marriott brand page",
      role: "brand_page",
      trust: "highest",
    },
    developmentPage: {
      url: "https://www.hotel-development.marriott.com/brands/studiores",
      label: "StudioRes by Marriott — hotel development brand page",
      role: "development_page",
      trust: "highest",
    },
    parentPlatformContext: [
      {
        url: "https://www.hotel-development.marriott.com/brands/extended-stay-brands",
        label: "Marriott Longer Stays family (parent family context)",
        role: "parent_platform",
        trust: "highest",
        note: "Lists StudioRes separately from RI / TPS / Element / Apartments — use for sibling distinction.",
      },
      {
        url: "https://help.marriott.com/s/article/what-is-StudioRes",
        label: "Marriott Help — What is StudioRes? (Bonvoy participation notes)",
        role: "parent_platform",
        trust: "high",
        note: "Loyalty program nuance — do not invent fee language; label as platform context.",
      },
    ],
    propertyExamples: [
      {
        propertyName: "StudioRes Fort Myers",
        url: "https://www.marriott.com/brands/studiores.mi",
        geographyLabel: "International Reference",
        market: "Fort Myers, Florida, USA (named on brand page; bind property overview URL before Stage 4)",
        matchKey: "StudioRes Fort Myers",
        note: "Brand page names StudioRes Fort Myers — steward must confirm dedicated property overview URL before openings/gallery cards.",
      },
    ],
    recentMomentumCandidates: [
      {
        dateLine: "Ongoing (official development)",
        title: "StudioRes by Marriott — midscale longer-stay new-build platform (US & Canada)",
        summary:
          "Official development page frames StudioRes as efficient new-build midscale longer stay — primary Stage 4 positioning source; do not invent CALA openings.",
        announcementUrl: "https://www.hotel-development.marriott.com/brands/studiores",
        linkLabel: "Marriott development — StudioRes",
        geographyLabel: "International Reference",
        whyRelevant: "Highest-trust official development source.",
      },
      {
        dateLine: "Ongoing (brand page)",
        title: "StudioRes guest brand page live on Marriott.com",
        summary:
          "Guest-facing StudioRes brand page confirms Bonvoy participation and longer-stay studio product — use for positioning, not sibling-brand proof.",
        announcementUrl: "https://www.marriott.com/brands/studiores.mi",
        linkLabel: "Marriott.com — StudioRes",
        geographyLabel: "International Reference",
        whyRelevant: "Official guest brand presence for Stage 4.",
      },
    ],
    openingsExamplesPropertiesCandidates: [
      "StudioRes Fort Myers (International Reference — confirm property overview URL)",
    ],
    imageSourceHints: [
      {
        url: "https://www.marriott.com/brands/studiores.mi",
        label: "StudioRes brand imagery",
        role: "image_source",
        trust: "highest",
      },
      {
        url: "https://www.hotel-development.marriott.com/brands/studiores",
        label: "StudioRes development imagery",
        role: "image_source",
        trust: "highest",
      },
    ],
    targetGuestSegmentsRecommendation: {
      recommended: ["Bleisure", "International Inbound"],
      avoid: ["Upscale Residence Inn framing", "Soft-brand Apartments by Marriott Bonvoy framing"],
      rationale: "Midscale longer-stay studios — distinct from upscale RI and soft-brand apartments.",
    },
    ownerFacingPositioningNotes: [
      "Official naming: StudioRes / StudioRes by Marriott (display name StudioRes per Wave 14 plan).",
      "US & Canada new-build midscale longer stay; EMEA conversion option exists on Longer Stays page — label geography carefully.",
      "Never present RI / TPS / Element / Apartments inventory as StudioRes.",
      "If steward data is thin, flag gaps — do not invent openings or CALA proof.",
    ],
    siblingBrandDistinctionNotes: [
      "≠ Residence Inn by Marriott (critical)",
      "≠ TownePlace Suites by Marriott (critical)",
      "≠ Element (critical)",
      "≠ Apartments by Marriott Bonvoy (critical)",
    ],
    sourceGaps: [
      "Dedicated property overview URLs need steward confirmation (Fort Myers named on brand page).",
      "No CALA examples — International Reference required.",
      "Fee / FDD figures must never enter owner-facing Presentation copy.",
    ],
    manualReviewRisks: [
      "High risk of conflating with Residence Inn / TownePlace / Element / Apartments.",
      "Display name variants (StudioRes vs StudioRes by Marriott) — keep Wave 14 slug studiores.",
    ],
  }),
});

export function getWave14SourcePack(slug) {
  const packRow = WAVE14_SOURCE_PACKS_BY_SLUG[slug];
  if (!packRow) throw new Error(`Unknown Wave 14 source pack slug: ${slug}`);
  return packRow;
}
