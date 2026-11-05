/**
 * Standard reusable AI Visibility Audit research modules (hotel-independent).
 * Commercial objectives and recommendations remain hotel-specific at runtime.
 */

export const AVA_STANDARD_MODULES = Object.freeze([
  {
    id: "M01_ADP_VISIBILITY_CONTEXT",
    title: "AI visibility context from existing ADP",
    description:
      "Reuse certified ADP observations, consideration, scenario presence, and peer position. No remeasurement.",
    paidByDefault: false,
  },
  {
    id: "M02_WEBSITE_CONTENT_ACCURACY",
    title: "Website content accuracy and completeness",
    description:
      "Inspect official website claims (awards, rooms, offers, amenities) against authoritative sources.",
    paidByDefault: false,
  },
  {
    id: "M03_TECHNICAL_ACCESSIBILITY",
    title: "Website technical accessibility",
    description:
      "robots.txt, sitemap, HTTP probes, and whether intended crawlers/AI systems can reach official content.",
    paidByDefault: false,
  },
  {
    id: "M04_IDENTITY_DOMAIN_CONSISTENCY",
    title: "Public digital identity and domain consistency",
    description:
      "Official domains, similar/confusable domains, staging hosts, brand vs property URLs.",
    paidByDefault: false,
  },
  {
    id: "M05_THIRD_PARTY_CONSISTENCY",
    title: "Third-party information consistency",
    description:
      "OTAs, tourism boards, and partner listings vs official facts.",
    paidByDefault: false,
  },
  {
    id: "M06_BACKLINK_AUTHORITY",
    title: "Digital authority, backlink & AI citation opportunities",
    description:
      "Public partial backlink/mention/citation opportunity research by default. Distinguishes OBSERVED_AI_CITATION, VERIFIED_BACKLINK, VERIFIED_EXTERNAL_MENTION, and POTENTIAL_CITATION_OPPORTUNITY. Comprehensive Ahrefs/Semrush-style inventories require separate founder-authorized paid research.",
    paidByDefault: false,
    paidComprehensiveInventory: true,
  },
  {
    id: "M11_DIGITAL_CONTENT_READINESS",
    title: "AI discoverability & digital content readiness",
    description:
      "Discover brand vs hotel-operated sources, Hotel Facts Register, traveler-question coverage, cross-platform consistency, and platform-specific correction ownership. Does not invent AI ranking causes.",
    paidByDefault: false,
  },
  {
    id: "M07_COMPETITIVE_DIGITAL_POSITIONING",
    title: "Competitive digital positioning",
    description:
      "How peer digital presence and ADP peer set compare for the subject hotel type.",
    paidByDefault: false,
  },
  {
    id: "M08_COMMERCIAL_RELEVANCE",
    title: "Commercial relevance",
    description:
      "Map findings to hotel-type demand (luxury, urban business, convention, boutique, extended-stay, select-service).",
    paidByDefault: false,
  },
  {
    id: "M09_RECOMMENDATION_GENERATION",
    title: "Evidence-backed recommendation generation",
    description:
      "Qualify actions; never promote unsupported hypotheses to customer recommendations.",
    paidByDefault: false,
  },
  {
    id: "M10_IMPLEMENTATION_VERIFICATION",
    title: "Implementation and verification planning",
    description:
      "Owners, timing, completion criteria, hotel approvals, agency technical panels.",
    paidByDefault: false,
  },
]);

export const AVA_HOTEL_TYPE_LENSES = Object.freeze([
  "LUXURY_RESORT",
  "URBAN_BUSINESS",
  "CONVENTION",
  "BOUTIQUE",
  "EXTENDED_STAY",
  "SELECT_SERVICE",
]);
