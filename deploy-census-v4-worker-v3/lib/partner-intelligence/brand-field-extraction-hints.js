/**
 * Pattern-based extraction hints for brand fields — scoped by pilot brand profile.
 * Kimpton-specific patterns must not apply to other brand contexts.
 */
import { buildIdentityFieldHint } from "./brand-extraction-context.js";

/** Stricter footprint pattern — avoids FDD date fragments like "Global 06 January". */
export const FOOTPRINT_GLOBAL_HOTELS_HINT = {
  patterns: [/Global\s+(\d{2,})\s+([\d,]+)\s+(\d+)/i],
  transform: (m) => m[1],
};

const KIMPTON_HINT_PROFILE = {
  "be.identity.brandName": {
    patterns: [/Kimpton Hotels?(?:\s*&\s*Restaurants)?/i],
    fixedValue: "Kimpton Hotels",
    pilotKey: "kimptonHotels",
    identityFallback: true,
  },
  "be.identity.parentCompany": {
    patterns: [/IHG Hotels?\s*&\s*Resorts/i, /InterContinental Hotels Group/i],
    fixedValue: "IHG Hotels & Resorts",
    pilotKey: "kimptonHotels",
    identityFallback: true,
  },
  "be.positioning.tagline": {
    patterns: [/Luxury with a Wink/i],
    fixedValue: "Luxury with a Wink",
    pilotKey: "kimptonHotels",
  },
  "be.positioning.history": {
    patterns: [
      /Since first introducing the boutique concept to the US in 1981[^.]*\./i,
      /innovating in hospitality since the\s+beginning/i,
    ],
    pilotKey: "kimptonHotels",
  },
  "be.positioning.summary": {
    patterns: [
      /What we stand for[\s\S]{0,800}?(?=Where we play|Why the brand|Distribution)/i,
      /original boutique, luxury-\s*lifestyle brand[\s\S]{0,500}/i,
      /design-led hotels and restaurants[\s\S]{0,400}/i,
    ],
    pilotKey: "kimptonHotels",
  },
  "be.positioning.guestPromise": {
    patterns: [
      /unscripted and\s+refreshingly human approach to\s+service/i,
      /design-led hotels, expansive programming, and locally loved restaurants/i,
    ],
    pilotKey: "kimptonHotels",
  },
  "be.overview.typicalUseCase": {
    patterns: [/Where we play[\s\S]{0,600}?(?=Why the brand|Distribution)/i],
    pilotKey: "kimptonHotels",
  },
  "be.overview.developmentModel": {
    patterns: [
      /new build or adaptive reuse/i,
      /flexible construction process/i,
      /whether for a new build or adaptive reuse/i,
    ],
    pilotKey: "kimptonHotels",
  },
  "be.overview.whyValue": {
    patterns: [/Why the brand[\s\S]{0,1200}?(?=Distribution|Enterprise contribution)/i],
    pilotKey: "kimptonHotels",
  },
  "be.overview.scenarios": {
    patterns: [
      /sought after urban and resort markets/i,
      /gateway cities/i,
      /adaptive reuse of an historic building/i,
    ],
    pilotKey: "kimptonHotels",
  },
  "be.footprint.globalHotels": {
    ...FOOTPRINT_GLOBAL_HOTELS_HINT,
    pilotKey: "kimptonHotels",
  },
  "be.footprint.globalRooms": {
    patterns: [/Global\s+\d+\s+([\d,]+)\s+\d+/i],
    transform: (m) => m[1].replace(/,/g, ""),
    pilotKey: "kimptonHotels",
  },
  "be.footprint.globalPipeline": {
    patterns: [/Global\s+\d+\s+[\d,]+\s+(\d+)/i],
    transform: (m) => m[1],
    pilotKey: "kimptonHotels",
  },
  "be.footprint.americasHotels": {
    patterns: [/Americas\s+(\d+)\s+[\d,]+\s+(\d+)/i],
    transform: (m) => m[1],
    pilotKey: "kimptonHotels",
  },
  "be.footprint.geoIntro": {
    patterns: [/Distribution[\s\S]{0,400}?(?=Kimpton Grand|Enterprise contribution|$)/i],
    pilotKey: "kimptonHotels",
  },
  "be.loyalty.programName": {
    patterns: [/IHG One\s*Rewards/i],
    fixedValue: "IHG One Rewards",
    pilotKey: "kimptonHotels",
  },
  "be.loyalty.memberCount": {
    patterns: [/more than\s+(\d+)\s*m\s+members/i, /(\d+)\s*million members/i],
    transform: (m) => String(Number(m[1])),
    pilotKey: "kimptonHotels",
  },
  "be.loyalty.roomContributionPct": {
    patterns: [
      /IHG One Rewards generated an average of\s+([\d.]+)%/i,
      /([\d.]+)%\s+of bookings\s+at Kimpton/i,
    ],
    transform: (m) => m[1],
    pilotKey: "kimptonHotels",
  },
  "be.loyalty.enterpriseBookingPct": {
    patterns: [
      /IHG['\u2019]?s booking channels represented an average of\s+([\d.]+)%/i,
      /average of\s+([\d.]+)%\s+of Kimpton Hotels\s+reservations/i,
    ],
    transform: (m) => m[1],
    pilotKey: "kimptonHotels",
  },
  "be.commercial.intro": {
    patterns: [/award-winning restaurants and bars/i, /operational excellence with a commitment/i],
    pilotKey: "kimptonHotels",
  },
  "be.economics.royaltyPct": {
    patterns: [/(\d+(?:\.\d+)?)\s*%\s*(?:of\s+)?(?:gross\s+)?(?:room\s+)?reven/i],
    transform: (m) => String(Number(m[1]) / 100),
    pilotKey: "kimptonHotels",
  },
  "be.economics.initialFranchiseFee": {
    patterns: [/initial franchise fee[^$$\d]{0,40}\$?\s*([\d,]+)/i],
    transform: (m) => m[1].replace(/,/g, ""),
    pilotKey: "kimptonHotels",
  },
  "be.meta.overallSourceConfidence": {
    fixedValue: "High — IHG development brochure and brand web capture (human review recommended).",
    pilotKey: "kimptonHotels",
  },
  "be.meta.lastReviewedDate": {
    fixedValue: () => new Date().toISOString().slice(0, 10),
    pilotKey: "kimptonHotels",
  },
};

const CURIO_HINT_PROFILE = {
  "be.positioning.summary": {
    patterns: [
      /Curio Collection(?:\s+by\s+Hilton)?[\s\S]{0,600}/i,
      /collection of unique hotels[\s\S]{0,400}/i,
    ],
    pilotKey: "curioCollection",
  },
  "be.positioning.tagline": {
    patterns: [/Stay True[\s\S]{0,120}/i],
    pilotKey: "curioCollection",
  },
  "be.positioning.guestPromise": {
    patterns: [/unique, authentic experiences/i, /independently spirited hotels/i],
    pilotKey: "curioCollection",
  },
  "be.overview.typicalUseCase": {
    patterns: [/independent hotel/i, /conversion/i, /unique hotel/i],
    pilotKey: "curioCollection",
  },
  "be.footprint.globalHotels": {
    ...FOOTPRINT_GLOBAL_HOTELS_HINT,
    pilotKey: "curioCollection",
  },
};

export const BRAND_HINT_PROFILES = {
  kimptonHotels: KIMPTON_HINT_PROFILE,
  curioCollection: CURIO_HINT_PROFILE,
};

/** @deprecated use getBrandFieldHints */
export const BRAND_FIELD_EXTRACTION_HINTS = KIMPTON_HINT_PROFILE;

/**
 * @param {string} fieldKey
 * @param {{ pilotKey?: string|null, brandName?: string|null, parentCompany?: string|null, resolved?: boolean }} [brandContext]
 */
export function getBrandFieldHints(fieldKey, brandContext = {}) {
  const identityHint = buildIdentityFieldHint(fieldKey, brandContext);
  if (identityHint) return identityHint;

  const pilotKey = brandContext?.pilotKey;
  if (!pilotKey) return null;

  return BRAND_HINT_PROFILES[pilotKey]?.[fieldKey] ?? null;
}
