/**
 * Section pattern parity — Hotel Indigo.
 * Replaces single untitled IHG lifestyle blob with named MLAC opening/pipeline cards.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const IHG_MLAC_2024 =
  "https://www.ihgplc.com/en/news-and-media/news-releases/2024/ihg-hotels-and-resorts-heats-up-in-mexico-latin-america-and-the-caribbean-with-exciting-openings";
const IHG_MLAC_2025 =
  "https://www.ihgplc.com/en/news-and-media/news-releases/2025/ihg-hotels-and-resorts-strengthens-position-across-mexico-latin-america-and-the-caribbean";

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

export const HOTEL_INDIGO_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "hotel-indigo",
  brandName: "Hotel Indigo",
  replaceMomentum: true,
  momentumLabel: "Recent openings & pipeline · linked announcements",
  momentumCards: [
    card({
      title: "IHG Strengthens MLAC Position Into 2025 Lifestyle Pipeline",
      dateLine: "2025",
      summary:
        "IHG's 2025 Mexico, Latin America, and Caribbean release reinforces continued lifestyle-brand momentum—including Hotel Indigo as a neighborhood-inspired growth path. Use as affiliation timing context; confirm development interest, franchise or management path, and PIP scope for the specific asset.",
      url: IHG_MLAC_2025,
      sort: 1,
    }),
    card({
      title: "Hotel Indigo Grand Cayman Opens In Caribbean Gateway",
      dateLine: "2024",
      summary:
        "IHG highlighted Hotel Indigo Grand Cayman among Mexico, Latin America, and Caribbean lifestyle openings—owner-relevant proof that Indigo's neighborhood-led full-service model travels into Caribbean gateway markets when district storytelling and IHG systems participation are underwritten together.",
      url: IHG_MLAC_2024,
      sort: 2,
    }),
    card({
      title: "Hotel Indigo La Paz Puerta Cortés Extends Baja Lifestyle Footprint",
      dateLine: "2024",
      summary:
        "IHG's MLAC openings narrative includes Hotel Indigo La Paz Puerta Cortés—Mexico Pacific lifestyle conversion/opening context for owners comparing place-led Indigo affiliation against select-service or hard-brand paths in Baja leisure corridors.",
      url: IHG_MLAC_2024,
      sort: 3,
    }),
    card({
      title: "Hotel Indigo Tijuana Adds Border-Metro Neighborhood Story",
      dateLine: "2024",
      summary:
        "Hotel Indigo Tijuana appears in IHG's MLAC heat-up release as an urban lifestyle signal—owners evaluating border-metro or gateway assets should diligence district authenticity, full-service labor, and IHG One Rewards participation before treating Indigo as a light reflag.",
      url: IHG_MLAC_2024,
      sort: 4,
    }),
  ],
  geoIntro:
    "Hotel Indigo is IHG's neighborhood-inspired lifestyle brand—urban, gateway, and culturally distinctive hotels rather than a uniform select-service box. Owner-relevant geography spans Americas and CALA lifestyle openings (Grand Cayman, La Paz, Tijuana) with European and APAC gateway recognition that supports traveler awareness. Confirm IHG development authorization and district fit locally; do not import global Indigo density as a local occupancy forecast.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Americas Hotel Indigo hotels concentrate on neighborhood-led urban and gateway markets where local storytelling can carry full-service public spaces and F&B. Owners should underwrite district authenticity, operator lifestyle capacity, and IHG systems cutover—not assume a select-service conversion budget will pass Indigo design review.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA relevance includes named Hotel Indigo activity in Grand Cayman, La Paz Puerta Cortés, and Tijuana from IHG MLAC releases—Caribbean gateway, Baja leisure, and border-metro lifestyle patterns. Confirm market authorization and competitive set before using those openings as analogues for dissimilar corridors.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe contributes dense Hotel Indigo neighborhood recognition that travelers may already know. For CALA or U.S. deals, European hotels are brand-awareness context—underwrite to local comps, labor intensity, and IHG agreement terms rather than European ramp curves.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA Hotel Indigo exposure is selective gateway context within IHG's lifestyle network. Americas owners should treat MEA as recognition reference and confirm whether the specific market is authorized for Indigo development before modeling affiliation value.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC lifestyle gateways reinforce Indigo's international neighborhood story. For Americas and CALA affiliation diligence, focus on local district fit and IHG commercial participation; APAC density is not a substitute for site-level feasibility.",
      sort: 15,
    },
  ],
  growthThemes:
    "Neighborhood-led lifestyle hotels\nUrban / gateway / culturally distinctive assets\nMLAC openings & pipeline (Grand Cayman, La Paz, Tijuana)\nFull-service conversion with local storytelling\nIHG One Rewards & systems participation",
  growthEditorial:
    "Hotel Indigo growth compounds when owners fund place-led design, credible public spaces, and operators who can execute local storytelling inside IHG systems. Named MLAC openings show the model travels across Caribbean and Mexico corridors—still underwrite each asset on district authenticity, PIP scope, and franchise or management path.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Hotel Indigo sits in IHG's lifestyle band as the neighborhood-inspired full-service flag—distinct from select-service IHG boxes and from soft collections that preserve more independent identity. Owners should compare capital intensity, F&B complexity, and parent-system rigidity versus Kimpton, voco, and Accor soft-collection peers before selecting a path.",
  },
  notes:
    "Source-backed: IHG PLC 2024 MLAC openings + 2025 MLAC strengthening releases. Quarantine untitled Public IHG lifestyle context blob.",
});
