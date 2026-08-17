/**
 * Section pattern parity — Country Inn & Suites by Choice.
 * Supplies real titles for empty momentum cards + brand-specific region breakdown.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const CHOICE_RADISSON_INTEGRATION =
  "https://media.choicehotels.com/2023-07-26-Choice-Hotels-Completes-Radisson-Hotels-Americas-Milestone,-Integrating-Loyalty-Programs-And-Allowing-For-Full-Booking-Capabilities-On-ChoiceHotels-com";
const CHOICE_DEV_SUCCESS_2025 =
  "https://media.choicehotels.com/2025-01-27-Choice-Hotels-International-Celebrates-Year-of-Development-Success";
const COUNTRY_PERFORMANCE_2025 =
  "https://media.choicehotels.com/2025-03-10-Country-Inn-Suites-by-Radisson-Achieves-Strong-Performance-Gains";
const COMFORT_COUNTRY_PROTOTYPE =
  "https://media.choicehotels.com/2025-03-18-Choice-Hotels-International-Introduces-Sharpened-Brand-Identities-and-Refreshed-Brand-Prototypes-for-Comfort-and-Country-Inn-Suites-by-Radisson";

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

export const COUNTRY_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "country-inn-suites",
  brandName: "Country Inn & Suites by Choice",
  replaceMomentum: true,
  momentumLabel: "Recent openings & pipeline · linked announcements",
  momentumCards: [
    card({
      title: "Refreshed Country Inn Prototype And Brand Identity",
      dateLine: "Mar 2025",
      summary:
        "Choice introduced sharpened Country Inn & Suites brand identity and value-engineered prototypes aimed at construction-cost efficiency within the same footprint. Owners should diligence suite mix, residential lobby cues, and breakfast footprint before locking conversion or new-build capital.",
      url: COMFORT_COUNTRY_PROTOTYPE,
      sort: 1,
    }),
    card({
      title: "Performance Gains And Thirty-Eight Pipeline Additions",
      dateLine: "Mar 2025",
      summary:
        "Two years after the Radisson Americas combination, Choice reported Country Inn RevPAR Index and direct-online gains plus 38 new Country Inn properties added to the development pipeline. Use as brand-momentum context for owners comparing Country versus Comfort—still model local comps and capital residuals independently.",
      url: COUNTRY_PERFORMANCE_2025,
      sort: 2,
    }),
    card({
      title: "Six Country Inn Openings In Record Development Year",
      dateLine: "Jan 2025",
      summary:
        "Choice Hotels cited six Country Inn & Suites by Radisson openings in its 2024 development-success narrative. Owner relevance: named opening cadence for suburban and highway upper-midscale conversion or new-build interest—confirm prototype fit and breakfast execution for the specific corridor.",
      url: CHOICE_DEV_SUCCESS_2025,
      sort: 3,
    }),
    card({
      title: "Country Inn Fully Bookable On ChoiceHotels.com",
      dateLine: "Jul 2023",
      summary:
        "Choice completed a Radisson Hotels Americas integration milestone—Country Inn & Suites by Radisson became fully bookable on ChoiceHotels.com with integrated loyalty capabilities. Owners evaluating affiliation should underwrite Choice Privileges and enterprise distribution as commercial context, not as a property-level demand guarantee.",
      url: CHOICE_RADISSON_INTEGRATION,
      sort: 4,
    }),
  ],
  geoIntro:
    "Country Inn & Suites by Choice is a U.S.-anchored, breakfast-led upper-midscale flag in the Radisson family under Choice—suburban corridors, airport markets, and highway nodes rather than resort or urban lifestyle plays. Selective CALA presence exists (e.g., Costa Rica airport-corridor context on census paths); owner underwriting should weight domestic supply and refreshed prototype economics first. Confirm international authorization before using other Radisson-family CALA openings as analogues.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "North America carries nearly all open Country Inn inventory—suburban, airport, and university-adjacent upper-midscale boxes with complimentary hot breakfast and residential-style lobbies. Choice development press cites six Country Inn openings in 2024, a refreshed prototype, and sharpened brand identity in 2025; owner conversations center on franchise economics versus Comfort and Quality, not core Radisson full-service.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA Country Inn presence is selective—census-backed paths include a Costa Rica San José–Heredia airport corridor node rather than a broad multi-country resort expansion story. Confirm authorization geography and local comps before importing U.S. suburban ramp curves into CALA underwriting.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Country Inn is franchised in the Americas by Choice; European Radisson-family hotels operate under different ownership structures. Useful for traveler brand recognition—not a template for European greenfield or conversion underwriting on this flag.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA is limited direct relevance for Country Inn owner diligence on Americas assets. Treat as international brand-awareness context only—not a regional growth thesis for this flag—and confirm market authorization before modeling affiliation.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC Country Inn presence is thin on census-backed Americas diligence paths. Domestic U.S. corridor economics and prototype residuals should drive feasibility—not APAC Radisson network density assumptions.",
      sort: 15,
    },
  ],
  growthThemes:
    "Suburban & highway upper-midscale\nAirport and university-adjacent nodes\nRadisson-family conversion & refresh\nBreakfast-led limited-service execution\nU.S. development pipeline momentum",
  growthEditorial:
    "Country Inn growth under Choice is distribution- and prototype-led: the brand compounds when owners want warm, residential upper-midscale with Radisson recognition and Choice enterprise mix—not upscale full-service or soft-collection flexibility. Public 2024–2025 opening and pipeline signals are affiliation context; model each corridor locally.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Country Inn & Suites by Choice sits in the upper-midscale select-service band—suite-oriented comfort above midscale Quality and Sleep formulas and below upscale Cambria and full-service Radisson tiers. Owners should compare breakfast labor, suite mix, and capital intensity across Choice siblings before selecting a flag.",
  },
  notes:
    "Titles pulled from country-inn footprint momentum fixture (Choice media). Empty-title remediation. Regions avoid invented zero counts; CALA framed as selective presence.",
});
