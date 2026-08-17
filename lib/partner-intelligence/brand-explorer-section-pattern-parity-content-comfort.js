/**
 * Section pattern parity — Comfort Inn & Suites.
 * Strengthens thin/untitled momentum with named CALA opening + Choice development signals.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const COMFORT_PUEBLA =
  "https://media.choicehotels.com/2024-20-04-Choice-Hotels-CALA-abre-dos-hoteles-en-Puebla-Comfort-Sleep";
const CHOICE_DEV_SUCCESS_2025 =
  "https://media.choicehotels.com/2025-01-27-Choice-Hotels-International-Celebrates-Year-of-Development-Success";
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

export const COMFORT_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "comfort-inn-suites",
  brandName: "Comfort Inn & Suites",
  replaceMomentum: true,
  momentumLabel: "Recent openings & brand development · linked announcements",
  momentumCards: [
    card({
      title: "Sharpened Comfort Identity And Value-Engineered Prototype",
      dateLine: "Mar 2025",
      summary:
        "Choice introduced sharpened Comfort brand identity and refreshed prototypes targeting construction-cost efficiency while protecting revenue-driving public spaces. Owners comparing conversion or new-build Comfort should diligence prototype residuals, breakfast footprint, and smoke-free standards before locking capital.",
      url: COMFORT_COUNTRY_PROTOTYPE,
      sort: 1,
    }),
    card({
      title: "Twenty-Six Comfort Openings In Choice Development Year",
      dateLine: "Jan 2025",
      summary:
        "Choice Hotels reported 26 Comfort openings in its 2024 development-success narrative alongside other upper-midscale growth. Owners should treat the opening cadence as brand-health context for Comfort new-build and conversion interest—confirm local authorization, prototype fit, and breakfast operating capacity for the specific asset.",
      url: CHOICE_DEV_SUCCESS_2025,
      sort: 2,
    }),
    card({
      title: "Comfort Inn Puebla Centro Histórico Debuts In Mexico",
      dateLine: "Feb 2024",
      summary:
        "Choice Hotels CALA opened Comfort Inn Puebla Centro Histórico—a nine-story, 54-room upper-midscale hotel on Av. Juárez beside Paseo Bravo, with buffet breakfast and meeting space. Owner relevance: named Mexico conversion/opening proof for breakfast-led Comfort affiliation in secondary-city CALA corridors.",
      url: COMFORT_PUEBLA,
      sort: 3,
    }),
  ],
  geoIntro:
    "Comfort Inn & Suites is Choice Hotels' breakfast-led upper-midscale flag with deep North American scale and selective CALA openings (including Puebla Centro Histórico). Owner diligence should weight domestic suburban and interstate corridors first, then confirm international authorization for CALA or other markets—do not invent sparse-region hotel counts. Compare fee stack and breakfast labor versus Quality and Country siblings before selecting the flag.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "North America carries the core Comfort open inventory—suburban, interstate, and growth-corridor upper-midscale boxes with hearty breakfast and smoke-free positioning. Owners should underwrite prototype compliance, breakfast labor, and Choice Privileges participation against local comps rather than national system scale alone.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA includes named Comfort activity such as Comfort Inn Puebla Centro Histórico—secondary-city Mexico upper-midscale with breakfast and small meetings. Confirm authorized geography and operator breakfast execution before treating Puebla as a template for resort or primary-gateway deals.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "European Comfort presence is limited relative to Americas scale and is primarily brand-recognition context for travelers. Americas owners should not import European ramp assumptions; confirm whether the specific market is authorized for Comfort development with Choice.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA is not a primary Comfort growth thesis for most Americas sponsors. Treat as international awareness only and confirm market authorization before modeling affiliation—local corridor demand and breakfast economics remain the feasibility base.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC Comfort exposure is selective international context within Choice's broader midscale story. Focus Americas and authorized CALA diligence on prototype, breakfast delivery, and competitive set—not APAC density as a proxy for your asset.",
      sort: 15,
    },
  ],
  growthThemes:
    "Breakfast-led upper-midscale\nNew construction & conversion pipeline\nSmoke-free prototype execution\nMexico CALA opening (Puebla)\nChoice Privileges participation",
  growthEditorial:
    "Comfort growth is prototype- and breakfast-led: Choice compounds the flag when owners can fund refreshed guestrooms, hearty breakfast operations, and smoke-free standards. Named Puebla opening plus 2024 opening cadence and 2025 prototype refresh are affiliation signals—still underwrite each corridor locally with Choice development before locking capital.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Comfort Inn & Suites anchors Choice's upper-midscale breakfast-led band—above midscale Quality and Sleep formulas and below upscale Cambria and full-service Radisson paths. Owners should compare capital intensity, breakfast labor, and prototype expectations across those siblings before selecting a flag.",
  },
  notes:
    "Uses known Choice CALA Comfort Puebla opening + Choice development-success (26 Comfort openings) + Mar 2025 prototype press. Replaces untitled Item-19 style momentum blobs. No invented zero counts.",
});
