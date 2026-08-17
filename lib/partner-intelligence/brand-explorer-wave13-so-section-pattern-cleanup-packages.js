/**
 * Wave 13 SO/ section-pattern cleanup packages (Presentation only).
 * Target: so-hotels-and-resorts · Brand Name SO/ Hotels & Resorts · Basics recTJdPlr4mDs9app
 */
import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
  withRecentMomentumSortOrder,
} from "./brand-explorer-recent-momentum-contract.js";

export const WAVE13_SO_SECTION_PATTERN_PACKAGES_VERSION =
  "wave13-so-section-pattern-cleanup-packages-v1";

export const SO_SLUG = "so-hotels-and-resorts";
export const SO_BASICS_RECORD_ID = "recTJdPlr4mDs9app";
export const SO_PRESENTATION_BRAND_NAME = "SO/ Hotels & Resorts";

const BRAND_URL =
  "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/so-hotels";
const PARIS_URL = "https://so-hotels.com/en/paris";
const MALDIVES_URL = "https://so-hotels.com/en/maldives";

function momentum({ title, dateLine, summary, url, sort }) {
  return buildRecentMomentumCard({ title, dateLine, summary, url, sort });
}

/** Dated Recent Momentum cards (source pack: Brandbook Mar 2026 + Paris brand-site listing). */
export const SO_SECTION_MOMENTUM_CARDS = Object.freeze(
  withRecentMomentumSortOrder([
    momentum({
      title: "SO/ Hotels & Resorts — Fashion-Rooted Luxury Lifestyle Collection (Accor Brandbook)",
      dateLine: "Mar 2026",
      summary:
        "Accor Brandbook (March 2026) positions SO/ Hotels & Resorts as a fashion-born luxury lifestyle collection for design-forward hotels and resorts. Owners should use the positioning for affiliation thesis—design intensity, destination energy, and F&B programming—without treating network or pipeline counts as diligence proof for a specific asset. International Reference identity source.",
      url: BRAND_URL,
      sort: 1,
    }),
    momentum({
      title: "SO/ Paris Featured as Fashion and Art Lifestyle Flagship",
      dateLine: "2025",
      summary:
        "Official so-hotels.com presents SO/ Paris as a fashion-and-art-led lifestyle flagship for SO/ Hotels & Resorts—International Reference proof for selective urban conversions. Owners can compare design intensity, cultural programming, and social public space against local comps; do not model the listing as CALA inventory.",
      url: PARIS_URL,
      sort: 2,
    }),
  ])
);

export const SO_SECTION_MOMENTUM_LABEL = RECENT_MOMENTUM_DEFAULT_LABEL;

export const SO_SECTION_GEO_INTRO =
  "SO/ Hotels & Resorts is a fashion-led luxury lifestyle collection with International Reference operating proof in Europe and selective Indian Ocean resorts. No verified CALA or Middle East & Africa operating inventory appears on official SO/ pages today—treat those regions as diligence-only until published. Underwrite affiliation on design intensity, destination F&B, and operator readiness for high-touch experiential hospitality.";

export const SO_SECTION_REGION_BODIES = Object.freeze({
  "footprint.region.eu": {
    title: "Europe",
    body:
      "Europe anchors SO/ Hotels & Resorts operating proof—SO/ Paris and SO/ Berlin Das Stue illustrate fashion-led urban lifestyle hotels where design, art direction, and social public space define the stay. Owners comparing European awareness to a first CALA conversion should underwrite local labor, design intensity, and district authenticity rather than importing Paris ramp curves.",
    tags: "International Reference · Europe",
    caseSummary:
      "Evidence: so-hotels.com Paris + Accor ALL Berlin Das Stue (International Reference).",
  },
  "footprint.region.apac": {
    title: "Asia Pacific / Indian Ocean",
    body:
      "Selective resort proof includes SO/ Maldives on so-hotels.com—destination energy and design-led hospitality as International Reference for SO/ Hotels & Resorts. CALA owners can use Maldives for brand-fit context on F&B and public-space intensity while underwriting to local labor and competitive sets.",
    tags: "International Reference · Maldives",
    caseSummary: "Evidence: so-hotels.com Maldives property page (International Reference).",
  },
  "footprint.region.am": {
    title: "Americas Diligence",
    body:
      "No verified Americas operating inventory for SO/ Hotels & Resorts is published on official SO/ pages today. Treat the Americas as diligence-only—confirm future openings on Accor Group or so-hotels.com before modeling open-room inventory or regional density claims for hotels and resorts under this collection.",
    tags: "Americas · diligence only",
    caseSummary: "Americas operating inventory not source-confirmed — diligence posture only.",
  },
  "footprint.region.cala": {
    title: "CALA",
    body:
      "No verified CALA operating examples for SO/ Hotels & Resorts appear on official SO/ or Accor Group brand pages. Keep CALA cleanly unavailable for operating inventory and use International Reference hotels and resorts for brand-fit diligence until CALA properties are published.",
    tags: "CALA · cleanly unavailable",
    caseSummary: "No verified CALA operating examples on official SO/ sources.",
  },
});

/** MEA: no source-supported inventory — suppress empty panel. */
export const SO_SECTION_MEA_SUPPRESS = Object.freeze({
  slotKey: "footprint.region.mea",
  action: "suppress_do_not_display",
  reason: "No source-supported SO/ MEA operating inventory on official brand/property pages.",
});

export const SO_SECTION_GROWTH_THEMES =
  "Selective luxury lifestyle growth\nFashion- and design-led hotels and resorts\nDestination energy & F&B intensity\nPremium urban or resort settings\nHigh-touch experiential operator readiness";

export const SO_SECTION_GROWTH_EDITORIAL =
  "SO/ Hotels & Resorts should grow selectively as a fashion- and design-led luxury lifestyle collection—hotels and resorts with destination energy, social F&B programming, and premium urban or resort settings. Owners and operators need readiness for high-touch experiential hospitality; the fit is stronger for differentiated lifestyle assets than for standardized broad-distribution hotels. Distinguish SO/ from Mama Shelter accessible irreverence, Fairmont heritage landmark luxury, MGallery soft-collection positioning, and generic Accor lifestyle-platform boilerplate—underwrite the SO/ guest promise, not parent averages.";

export const SO_SECTION_GROWTH_FIT =
  "Best growth fit for SO/ Hotels & Resorts: assets ready for fashion-led luxury lifestyle execution with elevated public space and destination F&B. Soft fit when sponsors will not capitalize design intensity, resort or urban destination energy, or operating complexity required for high-touch experiential hospitality.";
