/**
 * Wave 14 — Post-release Recent Momentum dated-card packages (eight public brands).
 *
 * Fixes section_pattern `dated_cards_below_min` (Stage 6 used `Directory` on all cards).
 *
 * Rules:
 * - Property overview URLs → dateLine `Directory` (evidence rejects invented years on /overview/).
 * - Official development / brand pages → dateLine `2026` (steward-year; Wave 13 living-page pattern).
 * - Need ≥2 year/month-dated cards for section_pattern; CALA-available brands need a CALA card first.
 * - URLs from Wave 14 source packs only. Flex excluded.
 */
import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";
import { WAVE14_PARTIAL_PROMOTION_SLUGS } from "./brand-explorer-wave14-factory-plan.js";

export const WAVE14_DATED_MOMENTUM_PACKAGES_VERSION =
  "wave14-dated-momentum-packages-v2";

export const WAVE14_DATED_MOMENTUM_SLUGS = WAVE14_PARTIAL_PROMOTION_SLUGS;

function momentum({ title, dateLine, summary, url, sort, regionLabel }) {
  const card = buildRecentMomentumCard({ title, dateLine, summary, url, sort });
  return Object.freeze({ ...card, regionLabel });
}

function freezePkg(pkg) {
  return Object.freeze({
    ...pkg,
    momentumCards: Object.freeze(
      pkg.momentumCards.map((c, i) => Object.freeze({ ...c, sort: i + 1 }))
    ),
  });
}

const YEAR = "2026";
const DATE_RATIONALE =
  "Property overview cards use Directory (evidence rule). Development/brand pages use steward-year 2026 for section-pattern dated minimum (Wave 13 living-page convention). URLs from Wave 14 source packs.";

export const WAVE14_DATED_MOMENTUM_PACKAGES = Object.freeze({
  "marriott-hotels": freezePkg({
    brandSlug: "marriott-hotels",
    brandName: "Marriott Hotels",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "Marriott Cancun Resort Confirms Marriott Hotels CALA Presence",
        dateLine: "Directory",
        summary:
          "Marriott.com lists Marriott Cancun Resort as a live CALA full-service Marriott Hotels property—owner-relevant operating proof for meetings scope and regional leisure delivery. Confirm the hotel remains Marriott Hotels (not JW or soft-brand) and underwrite service intensity against Cancún comps before affiliation.",
        url: "https://www.marriott.com/en-us/hotels/cunmc-marriott-cancun-resort/overview/",
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Marriott Hotels Development Page Frames Flagship Full-Service Lane",
        dateLine: YEAR,
        summary:
          "Official Marriott hotel-development materials continue to position Marriott Hotels as the namesake full-service brand for owners. Use that framing for product and standards diligence—not Marriott International corporate IR—and keep JW, Sheraton, and Westin in separate underwriting lanes.",
        url: "https://www.hotel-development.marriott.com/brands/marriott",
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Marriott Hotels Brand Site Confirms Flagship Full-Service Product",
        dateLine: YEAR,
        summary:
          "The official Marriott Hotels brand site continues to frame the namesake full-service product for owners comparing affiliation lanes. Use it with the development page as International Reference brand evidence while CALA property proof stays on named Marriott.com overview pages.",
        url: "https://marriott-hotels.marriott.com/",
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  sheraton: freezePkg({
    brandSlug: "sheraton",
    brandName: "Sheraton",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "Sheraton Cancun Resort & Spa Anchors CALA Full-Service Proof",
        dateLine: "Directory",
        summary:
          "Marriott.com lists Sheraton Cancun Resort & Spa as a live CALA full-service Sheraton—useful for owners comparing resort product, meetings capacity, and public-space reactivation. Keep Four Points by Sheraton and Four Points Flex out of the same underwriting lane.",
        url: "https://www.marriott.com/en-us/hotels/cunsi-sheraton-cancun-resort-and-spa/overview/",
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Sheraton Development Positioning Remains Full-Service for Owners",
        dateLine: YEAR,
        summary:
          "Official Marriott development materials continue to position Sheraton as a full-service brand with conversion and reinvestment relevance for owners. Owners evaluating public-space and meetings reactivation should use this as brand-lane evidence—not Flex midscale conversion logic.",
        url: "https://www.hotel-development.marriott.com/brands/sheraton",
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Sheraton Brand Site Confirms Full-Service Product for Owners",
        dateLine: YEAR,
        summary:
          "The official Sheraton brand site continues to support full-service affiliation diligence for owners comparing public-space and meetings reactivation. Use it with the development page while keeping Four Points by Sheraton and Four Points Flex out of the Sheraton underwriting lane.",
        url: "https://sheraton.marriott.com/",
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  westin: freezePkg({
    brandSlug: "westin",
    brandName: "Westin",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "The Westin Resort & Spa Cancun Confirms CALA Premium Wellness Proof",
        dateLine: "Directory",
        summary:
          "Marriott.com lists The Westin Resort & Spa, Cancun as a live CALA Westin—owner-relevant evidence for premium wellness-led resort product and service intensity. Keep W Hotels lifestyle luxury and JW Marriott luxury in separate diligence lanes.",
        url: "https://www.marriott.com/en-us/hotels/cunwi-the-westin-resort-and-spa-cancun/overview/",
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Westin Development Page Frames Premium Wellness Full-Service Lane",
        dateLine: YEAR,
        summary:
          "Official Marriott development materials continue to frame Westin around premium wellness-led full-service positioning for owners. Owners should underwrite rooms, fitness, and service cues from this brand page rather than borrowing W or JW proof points.",
        url: "https://www.hotel-development.marriott.com/brands/westin",
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Westin Brand Presence Confirms Premium Wellness Product for Owners",
        dateLine: YEAR,
        summary:
          "Official Westin guest and development materials continue to support premium wellness full-service affiliation diligence for owners. Use them to separate Westin from Sheraton meetings-led full-service and from W lifestyle luxury when comparing capital and service intensity.",
        url: "https://westin.marriott.com/",
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  "residence-inn-by-marriott": freezePkg({
    brandSlug: "residence-inn-by-marriott",
    brandName: "Residence Inn by Marriott",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "Residence Inn Merida Confirms Residence Inn by Marriott CALA Presence",
        dateLine: "Directory",
        summary:
          "Marriott.com lists Residence Inn Merida as a live CALA Residence Inn by Marriott upscale extended-stay property—useful suite and longer-stay operating proof for regional owners comparing affiliation. Keep StudioRes, TownePlace Suites, and SpringHill Suites in separate sibling lanes during underwriting.",
        url: "https://www.marriott.com/en-us/hotels/midri-residence-inn-merida/overview/",
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Residence Inn by Marriott Development Page Frames Upscale Extended-Stay",
        dateLine: YEAR,
        summary:
          "Official Marriott hotel-development materials continue to position Residence Inn by Marriott as the upscale extended-stay leader for owners reviewing affiliation. Use this for sibling distinction versus StudioRes and TownePlace Suites—not interchangeable proof across longer-stay lanes.",
        url: "https://www.hotel-development.marriott.com/brands/residence-inn",
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Residence Inn by Marriott On Marriott Longer Stays Family Positioning",
        dateLine: YEAR,
        summary:
          "Official Marriott Longer Stays materials continue to list Residence Inn by Marriott alongside Element, TownePlace Suites, and StudioRes for owners. Owners should underwrite upscale extended-stay suite product from this family context without borrowing midscale StudioRes economics.",
        url: "https://www.hotel-development.marriott.com/brands/extended-stay-brands",
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  "springhill-suites-by-marriott": freezePkg({
    brandSlug: "springhill-suites-by-marriott",
    brandName: "SpringHill Suites by Marriott",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "SpringHill Suites by Marriott Development Page Frames All-Suite Select-Service",
        dateLine: YEAR,
        summary:
          "Official Marriott hotel-development materials continue to frame SpringHill Suites by Marriott as an all-suite select-service brand for owners. Separate it from Residence Inn by Marriott extended-stay and Fairfield limited-service lanes when underwriting suite product and operating intensity.",
        url: "https://www.hotel-development.marriott.com/brands/springhill-suites",
        sort: 1,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "SpringHill Suites by Marriott Guest Brand Site Confirms All-Suite Product",
        dateLine: YEAR,
        summary:
          "The official SpringHill Suites by Marriott guest brand site confirms guest-facing all-suite select-service framing owners can use when comparing short-to-medium stay suite product. Treat this as brand-directory evidence—not a named property opening—and do not imply CALA presence without a steward-matched property URL.",
        url: "https://springhillsuites.marriott.com/",
        sort: 2,
        regionLabel: "International Reference",
      }),
    ],
  }),

  "towneplace-suites-by-marriott": freezePkg({
    brandSlug: "towneplace-suites-by-marriott",
    brandName: "TownePlace Suites by Marriott",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "TownePlace Suites by Marriott On Marriott Longer Stays Family Positioning",
        dateLine: YEAR,
        summary:
          "Official Marriott Longer Stays materials continue to position TownePlace Suites by Marriott alongside Residence Inn, Element, and StudioRes for owners. Use this for sibling distinction—select-service longer stay—not upscale Residence Inn or midscale StudioRes interchangeable proof.",
        url: "https://www.hotel-development.marriott.com/brands/extended-stay-brands",
        sort: 1,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "TownePlace Suites by Marriott Development Page Frames Longer-Stay Select-Service",
        dateLine: YEAR,
        summary:
          "Official Marriott hotel-development materials continue to frame TownePlace Suites by Marriott as a longer-stay select-service brand for owners. Underwrite suite mix and stay-length demand against local comps without inventing CALA inventory until a property-name-matched URL is steward-confirmed.",
        url: "https://www.hotel-development.marriott.com/brands/towneplace-suites",
        sort: 2,
        regionLabel: "International Reference",
      }),
    ],
  }),

  "aloft-hotels": freezePkg({
    brandSlug: "aloft-hotels",
    brandName: "Aloft Hotels",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "Aloft Cancun Confirms Aloft Hotels CALA Lifestyle Select-Service Presence",
        dateLine: "Directory",
        summary:
          "Marriott.com lists Aloft Cancun as a live CALA Aloft Hotels property—owner-relevant proof for social public space and lifestyle select-service delivery in a leisure corridor for regional owners. Keep Element, AC Hotels, and Moxy in separate sibling lanes during underwriting.",
        url: "https://www.marriott.com/en-us/hotels/cunal-aloft-cancun/overview/",
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Aloft Hotels Development Page Frames Lifestyle Select-Service Lane",
        dateLine: YEAR,
        summary:
          "Official Marriott development materials continue to position Aloft Hotels as a lifestyle select-service brand for owners. Underwrite design character and social public space from this brand page rather than borrowing full-service Sheraton or Westin proof points.",
        url: "https://www.hotel-development.marriott.com/brands/aloft",
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Aloft Hotels Brand Presence Confirms Lifestyle Select-Service Product",
        dateLine: YEAR,
        summary:
          "Official Aloft Hotels guest and development materials continue to support lifestyle select-service affiliation diligence for owners. Use them to keep Aloft distinct from Element extended-stay and from Moxy when comparing public-space programming and capital intensity.",
        url: "https://aloft-hotels.marriott.com/",
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  studiores: freezePkg({
    brandSlug: "studiores",
    brandName: "StudioRes",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    dateRationale: DATE_RATIONALE,
    momentumCards: [
      momentum({
        title: "StudioRes Development Page Frames Midscale Longer-Stay Platform",
        dateLine: YEAR,
        summary:
          "Official Marriott hotel-development materials frame StudioRes as a midscale longer-stay platform for US and Canada new-build owners reviewing affiliation. Use this as primary product-posture evidence without inventing CALA openings or borrowing Residence Inn upscale extended-stay assumptions.",
        url: "https://www.hotel-development.marriott.com/brands/studiores",
        sort: 1,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "StudioRes Brand Page Confirms Midscale Longer-Stay Presence",
        dateLine: YEAR,
        summary:
          "The official StudioRes Marriott brand page confirms guest-facing midscale longer-stay framing and names International Reference inventory for owner review. Treat named US references as brand-directory evidence until dedicated property overview URLs are steward-confirmed; keep Residence Inn and TownePlace Suites separate.",
        url: "https://www.marriott.com/brands/studiores.mi",
        sort: 2,
        regionLabel: "International Reference",
      }),
    ],
  }),
});

export function getWave14DatedMomentumPackage(slug) {
  const pkg = WAVE14_DATED_MOMENTUM_PACKAGES[slug];
  if (!pkg) throw new Error(`No Wave 14 dated-momentum package for ${slug}`);
  return pkg;
}
