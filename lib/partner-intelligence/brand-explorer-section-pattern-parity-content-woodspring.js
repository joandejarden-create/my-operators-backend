/**
 * Section pattern parity — WoodSpring Suites.
 * Recent Momentum = opening / development press with linked announcements
 * (not development-spec diligence filler).
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const CHOICE_DEV_SUCCESS_2024 =
  "https://investor.choicehotels.com/news/news-details/2025/Choice-Hotels-International-Celebrates-Year-of-Development-Success/default.aspx";
const CHOICE_EXTENDED_STAY_500 =
  "https://www.prnewswire.com/news-releases/choice-hotels-international-celebrates-major-milestone-with-500th-extended-stay-property-opening-everhome-suites-glendale-302284172.html";
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

export const WOODSPRING_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "woodspring-suites",
  brandName: "WoodSpring Suites",
  replaceMomentum: true,
  momentumLabel: "Recent openings & pipeline · linked announcements",
  momentumCards: [
    card({
      title: "WoodSpring Opens Record 25 Hotels In 2024",
      dateLine: "2024",
      summary:
        "Choice reported 25 WoodSpring Suites openings in 2024—a brand record within 61 total extended-stay openings. For owners, that is opening-pace context for kitchen-suite new construction demand; confirm local weekly mix, prototype residuals, and franchise path before treating national opening counts as a site forecast.",
      url: CHOICE_DEV_SUCCESS_2024,
      sort: 1,
    }),
    card({
      title: "WoodSpring Leads Choice Extended-Stay Opening Pace",
      dateLine: "2024",
      summary:
        "Choice's 500th extended-stay milestone release put WoodSpring on track for another record opening year (forecast ~25 by year-end) alongside Everhome, MainStay, and Suburban. Use as platform growth signal—underwrite corridor employment demand and new-build capital rather than assuming a conversion reflag budget.",
      url: CHOICE_EXTENDED_STAY_500,
      sort: 2,
    }),
    card({
      title: "WoodSpring Opens 28 Hotels In 2025 Extended-Stay Record Year",
      dateLine: "2025",
      summary:
        "Choice said WoodSpring opened 28 hotels in 2025 and led extended-stay franchise signings—owner-relevant proof the kitchen-suite prototype continues to clear development. Confirm market authorization, construction timeline, and Choice Privileges participation for the specific asset before locking affiliation capital.",
      url: CHOICE_DEV_PERFORMANCE_2025,
      sort: 3,
    }),
  ],
  geoIntro:
    "WoodSpring Suites is Choice Hotels' economy-to-midscale extended-stay flag built around kitchen-equipped suites and operating simplicity—primarily U.S. employment, medical, and logistics corridors with selective Americas context. Confirm international authorization before modeling CALA or other regions; do not invent sparse-region hotel counts. Underwrite weekly demand depth and kitchen wear rather than nightly limited-service assumptions.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Americas—especially U.S. corridors—carry WoodSpring's practical operating footprint for weekly and longer stays. Owners should compare local extended-stay supply, kitchen prototype residuals, and lean staffing capacity versus Suburban and other Choice extended siblings before selecting the flag.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA WoodSpring relevance is selective and must be confirmed with Choice development for authorized geography and prototype fit. Do not assume U.S. weekly employment curves transfer to leisure-heavy CALA markets without local extended-stay demand diligence.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe is limited direct relevance for WoodSpring owner diligence on Americas assets. Treat as international awareness only; confirm market authorization before modeling affiliation and keep capital underwriting anchored to kitchen-suite weekly operations.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA is not a primary WoodSpring growth thesis for most Americas sponsors. Confirm authorization only if the portfolio is cross-border; otherwise focus diligence on Americas corridor weekly demand and Choice extended-stay support.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC WoodSpring exposure is not a core Americas diligence base. Underwrite local weekly mix, kitchen FF&E, and Choice systems participation rather than importing APAC extended-stay density assumptions.",
      sort: 15,
    },
  ],
  growthThemes:
    "Record annual opening pace (25 in 2024; 28 in 2025)\nKitchen-equipped extended-stay suites\nWeekly & longer-stay employment corridors\nNew-construction prototype discipline\nChoice extended-stay family comparison",
  growthEditorial:
    "WoodSpring growth is opening- and prototype-led: Choice compounds the flag when owners fund kitchen-equipped new builds and operators can run lean extended-stay rhythms. Prefer linked opening/development announcements over diligence filler—still confirm local corridor economics without inventing property-level occupancy.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "WoodSpring Suites sits in Choice's extended-stay family as an economy-to-midscale kitchen-suite flag—distinct from Suburban economy studios, MainStay residential extended, and nightly midscale boxes. Owners should compare capital intensity, weekly mix, and operating simplicity across those siblings before selecting a path.",
  },
  notes:
    "Momentum cards use Choice opening/development press (2024 record openings, 500th extended-stay context, 2025 opening performance)—not development-page diligence filler.",
});
