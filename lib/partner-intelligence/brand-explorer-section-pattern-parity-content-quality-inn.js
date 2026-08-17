/**
 * Section pattern parity — Quality Inn.
 * Recent Momentum = opening / midscale development press with linked announcements.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const CHOICE_DEV_SUCCESS_2024 =
  "https://media.choicehotels.com/2025-01-27-Choice-Hotels-International-Celebrates-Year-of-Development-Success";
const CHOICE_DEV_SUCCESS_INVESTOR =
  "https://investor.choicehotels.com/news/news-details/2025/Choice-Hotels-International-Celebrates-Year-of-Development-Success/default.aspx";
const CHOICE_DEV_PERFORMANCE_2025 =
  "https://media.choicehotels.com/2026-01-26-Choice-Hotels-International-Announces-2025-Development-Performance-Fueled-by-Record-International-Growth-and-Sustained-Momentum-Across-Key-Segments";

function card({ title, dateLine, summary, url, sort }) {
  return {
    title,
    dateLine,
    summary,
    url,
    sort,
    body: buildMomentumBody({ dateLine, summary, sourceUrl: url }),
  };
}

export const QUALITY_INN_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "quality-inn",
  brandName: "Quality Inn",
  replaceMomentum: true,
  momentumLabel: "Recent openings & pipeline · linked announcements",
  momentumCards: [
    card({
      title: "Choice 2025 Development Year Sustains Midscale Opening Momentum",
      dateLine: "2025",
      summary:
        "Choice's 2025 development performance release underscores continued franchise openings across key midscale segments. For Quality Inn owners, use the cadence as affiliation timing context—confirm local corridor demand, conversion PIP, and breakfast operating capacity before treating system headlines as a property forecast.",
      url: CHOICE_DEV_PERFORMANCE_2025,
      sort: 1,
    }),
    card({
      title: "Choice Celebrates 2024 Development Success Across Midscale Brands",
      dateLine: "Jan 2025",
      summary:
        "Choice Hotels' year-of-development-success narrative highlights continued midscale franchise activity and conversion openings. Owner relevance for Quality Inn: conversion-ready midscale boxes with breakfast-led limited-service expectations—diligence prototype residuals and Choice Privileges participation locally.",
      url: CHOICE_DEV_SUCCESS_2024,
      sort: 2,
    }),
    card({
      title: "Investor Development Update Reinforces Quality Conversion Path",
      dateLine: "2024",
      summary:
        "Choice's investor development update frames midscale conversion and new-build momentum that Quality Inn owners use as brand-health context. Confirm agreement terms, PIP scope, and operator midscale depth for the specific asset—compare versus Comfort before selecting the higher upper-midscale path.",
      url: CHOICE_DEV_SUCCESS_INVESTOR,
      sort: 3,
    }),
  ],
  geoIntro:
    "Quality Inn is Choice Hotels' foundational midscale flag with broad Americas highway and suburban conversion relevance. International nodes are authorization-dependent; do not invent sparse-region hotel counts. Owner diligence should weight conversion PIP, breakfast model, and Choice Privileges contribution versus local comps—not generic upscale assumptions.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Americas—especially U.S. highway, suburban, and secondary-city corridors—carry Quality Inn's conversion-ready midscale story. Owners should underwrite prototype compliance, breakfast execution, and fee-stack net contribution against Comfort and Sleep siblings before selecting the flag.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA Quality Inn relevance is selective and must be confirmed with Choice development for authorized geography. Do not assume U.S. highway conversion economics transfer to resort or gateway markets without local midscale demand diligence and prototype fit review.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe is limited direct relevance for Quality Inn owner diligence on Americas assets. Treat as international brand-awareness context and confirm market authorization before modeling affiliation.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA is not a primary Quality Inn growth thesis for most Americas sponsors. Confirm authorization only for cross-border portfolios; otherwise anchor underwriting to Americas midscale corridors and Choice systems participation.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC Quality Inn exposure is not a core Americas diligence base. Focus on local conversion PIP, breakfast model, and competitive set rather than importing APAC midscale density assumptions.",
      sort: 15,
    },
  ],
  growthThemes:
    "Midscale conversion & new-build openings\nHighway and suburban corridors\nBreakfast-led limited-service\nFranchisee-friendly prototype economics\nChoice Privileges participation",
  growthEditorial:
    "Quality Inn growth is conversion-led midscale scale: Choice compounds the flag when owners can fund prototype refresh and operators can deliver reliable breakfast-led stays. Prefer linked opening/development announcements—still underwrite each corridor locally without inventing pipeline counts in Explorer copy.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Quality Inn sits at the heart of Choice's midscale band—founding-brand awareness with conversion-ready economics below Comfort upper-midscale breakfast intensity and above economy roadside boxes. Owners should compare fee stack, amenity requirements, and capital residuals across Quality, Comfort, and Sleep before selecting a flag.",
  },
  notes:
    "Momentum cards use Choice opening/development press (2024–2025 development success and 2025 performance)—not development-page diligence filler.",
});
