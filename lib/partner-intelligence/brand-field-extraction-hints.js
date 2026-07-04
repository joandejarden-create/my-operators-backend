/**
 * Pattern-based extraction hints for brand fields (Kimpton / IHG sources).
 * Patterns run against merged source text; evidence must match source substring.
 */

export const BRAND_FIELD_EXTRACTION_HINTS = {
  "be.identity.brandName": {
    patterns: [/Kimpton Hotels?(?:\s*&\s*Restaurants)?/i],
    fixedValue: "Kimpton Hotels",
  },
  "be.identity.parentCompany": {
    patterns: [/IHG Hotels?\s*&\s*Resorts/i, /InterContinental Hotels Group/i],
    fixedValue: "IHG Hotels & Resorts",
  },
  "be.positioning.tagline": {
    patterns: [/Luxury with a Wink/i],
    fixedValue: "Luxury with a Wink",
  },
  "be.positioning.history": {
    patterns: [
      /Since first introducing the boutique concept to the US in 1981[^.]*\./i,
      /innovating in hospitality since the\s+beginning/i,
    ],
  },
  "be.positioning.summary": {
    patterns: [
      /What we stand for[\s\S]{0,800}?(?=Where we play|Why the brand|Distribution)/i,
      /original boutique, luxury-\s*lifestyle brand[\s\S]{0,500}/i,
      /design-led hotels and restaurants[\s\S]{0,400}/i,
    ],
  },
  "be.positioning.guestPromise": {
    patterns: [
      /unscripted and\s+refreshingly human approach to\s+service/i,
      /design-led hotels, expansive programming, and locally loved restaurants/i,
    ],
  },
  "be.overview.typicalUseCase": {
    patterns: [/Where we play[\s\S]{0,600}?(?=Why the brand|Distribution)/i],
  },
  "be.overview.developmentModel": {
    patterns: [
      /new build or adaptive reuse/i,
      /flexible construction process/i,
      /whether for a new build or adaptive reuse/i,
    ],
  },
  "be.overview.whyValue": {
    patterns: [/Why the brand[\s\S]{0,1200}?(?=Distribution|Enterprise contribution)/i],
  },
  "be.overview.scenarios": {
    patterns: [
      /sought after urban and resort markets/i,
      /gateway cities/i,
      /adaptive reuse of an historic building/i,
    ],
  },
  "be.footprint.globalHotels": {
    patterns: [/Global\s+(\d+)\s+[\d,]+\s+(\d+)/i, /Global\s+(\d+)\b/i],
    transform: (m) => m[1],
  },
  "be.footprint.globalRooms": {
    patterns: [/Global\s+\d+\s+([\d,]+)\s+\d+/i],
    transform: (m) => m[1].replace(/,/g, ""),
  },
  "be.footprint.globalPipeline": {
    patterns: [/Global\s+\d+\s+[\d,]+\s+(\d+)/i],
    transform: (m) => m[1],
  },
  "be.footprint.americasHotels": {
    patterns: [/Americas\s+(\d+)\s+[\d,]+\s+(\d+)/i],
    transform: (m) => m[1],
  },
  "be.footprint.geoIntro": {
    patterns: [/Distribution[\s\S]{0,400}?(?=Kimpton Grand|Enterprise contribution|$)/i],
  },
  "be.loyalty.programName": {
    patterns: [/IHG One\s*Rewards/i],
    fixedValue: "IHG One Rewards",
  },
  "be.loyalty.memberCount": {
    patterns: [/more than\s+(\d+)\s*m\s+members/i, /(\d+)\s*million members/i],
    transform: (m) => String(Number(m[1])),
  },
  "be.loyalty.roomContributionPct": {
    patterns: [
      /IHG One Rewards generated an average of\s+([\d.]+)%/i,
      /([\d.]+)%\s+of bookings\s+at Kimpton/i,
    ],
    transform: (m) => m[1],
  },
  "be.loyalty.enterpriseBookingPct": {
    patterns: [
      /IHG['\u2019]?s booking channels represented an average of\s+([\d.]+)%/i,
      /average of\s+([\d.]+)%\s+of Kimpton Hotels\s+reservations/i,
    ],
    transform: (m) => m[1],
  },
  "be.commercial.intro": {
    patterns: [/award-winning restaurants and bars/i, /operational excellence with a commitment/i],
  },
  "be.economics.royaltyPct": {
    patterns: [/(\d+(?:\.\d+)?)\s*%\s*(?:of\s+)?(?:gross\s+)?(?:room\s+)?reven/i],
    transform: (m) => String(Number(m[1]) / 100),
  },
  "be.economics.initialFranchiseFee": {
    patterns: [/initial franchise fee[^$$\d]{0,40}\$?\s*([\d,]+)/i],
    transform: (m) => m[1].replace(/,/g, ""),
  },
  "be.meta.overallSourceConfidence": {
    fixedValue: "High — IHG development brochure and brand web capture (human review recommended).",
  },
  "be.meta.lastReviewedDate": {
    fixedValue: () => new Date().toISOString().slice(0, 10),
  },
};
