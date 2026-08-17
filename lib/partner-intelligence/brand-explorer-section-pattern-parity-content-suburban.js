/**
 * Section pattern parity — Suburban Studios.
 * Recent Momentum = opening / extended-stay development press with linked announcements.
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

export const SUBURBAN_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "suburban-studios",
  brandName: "Suburban Studios",
  replaceMomentum: true,
  momentumLabel: "Recent openings & pipeline · linked announcements",
  momentumCards: [
    card({
      title: "Suburban Studios Continues Extended-Stay Opening Cadence Into 2025",
      dateLine: "2025",
      summary:
        "Choice's 2025 extended-stay performance release underscores continued openings across Suburban Studios, MainStay, WoodSpring, and Everhome. For owners, treat Suburban as a weekly-stay studio path inside that platform—confirm corridor demand, kitchen-in-a-box residuals, and franchise terms before locking conversion capital.",
      url: CHOICE_DEV_PERFORMANCE_2025,
      sort: 1,
    }),
    card({
      title: "Fourteen Suburban Studios Openings In Choice 2024 Development Year",
      dateLine: "2024",
      summary:
        "Choice reported 14 Suburban Studios openings in 2024 within a record extended-stay year. Owner relevance: named opening cadence for economy extended-stay studios—underwrite weekly mix and Fast Track conversion fit locally rather than treating national counts as a site forecast.",
      url: CHOICE_DEV_SUCCESS_2024,
      sort: 2,
    }),
    card({
      title: "Suburban Included In Choice 500th Extended-Stay Milestone",
      dateLine: "2024",
      summary:
        "Choice's 500th extended-stay milestone release cited Suburban Studios openings alongside WoodSpring, MainStay, and Everhome growth. Use as platform opening context for economy studio affiliation—diligence kitchen prototype, weekly billing, and operator extended-stay discipline for the specific asset.",
      url: CHOICE_EXTENDED_STAY_500,
      sort: 3,
    }),
  ],
  geoIntro:
    "Suburban Studios is Choice Hotels' economy extended-stay studio flag—primarily Americas weekly-stay corridors near employment, medical, and logistics nodes. International presence is selective and must be confirmed for authorization; do not invent sparse-region hotel counts. Owners should underwrite studio product, kitchen capability, and weekly billing integrity rather than nightly midscale assumptions.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Americas carry the practical Suburban Studios operating footprint—economy extended-stay studios with kitchen solutions for weekly guests. Owners should compare local weekly demand, competitor suite supply, and conversion PIP before selecting Suburban versus other Choice extended-stay siblings.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA Suburban relevance is selective and authorization-dependent—confirm with Choice development whether the market and asset type fit economy extended-stay studio standards. Do not assume U.S. suburban weekly curves transfer to resort or gateway transient markets without local diligence.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe is limited direct relevance for Suburban Studios owner diligence on Americas assets. Treat as international brand-awareness context only and confirm market authorization before modeling affiliation economics.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA is not a primary Suburban growth thesis for most Americas sponsors. Confirm authorization if evaluating cross-border portfolios; otherwise keep underwriting anchored to Americas weekly-stay corridors and Choice extended-stay support.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC Suburban exposure is not a core diligence base for Americas deals. Focus on local weekly demand, studio prototype, and Choice systems participation rather than importing APAC extended-stay density assumptions.",
      sort: 15,
    },
  ],
  growthThemes:
    "Economy extended-stay studio openings\nFast Track conversion pathway\nWeekly rate mix near employment nodes\nKitchen-in-a-box entry conversions\nChoice extended-stay family positioning",
  growthEditorial:
    "Suburban Studios compounds when owners convert or build for true weekly demand with lean studio operations inside Choice's extended-stay platform. Prefer linked opening/development announcements over diligence filler—still underwrite kitchen residuals, weekly billing, and operator fit locally.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Suburban Studios sits in Choice's extended-stay family as an economy studio path—distinct from WoodSpring's kitchen-equipped suite simplicity, MainStay residential extended, and Everhome midscale extended. Owners should compare weekly mix, capital intensity, and prototype expectations across those siblings before selecting a flag.",
  },
  notes:
    "Momentum cards use Choice opening/development press (2024 Suburban openings count, 500th extended-stay milestone, 2025 extended-stay performance)—not development-page diligence filler.",
});
