/**
 * Section pattern parity — Radisson family (radisson, radisson-blu, radisson-red).
 * Blu may already pass regions/momentum; pack still exports patches for remediation if audit fails.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const RADISSON_PUEBLA =
  "https://media.choicehotels.com/Choice-Hotels-CALA-inaugura-su-nuevo-hotel-Radisson-Puebla-Angelopolis";
const RADISSON_SLP =
  "https://media.choicehotels.com/Deleitate-con-nuestro-moderno-hotel-Radisson-en-San-Luis-Potosi";
const RADISSON_PANAMA =
  "https://media.choicehotels.com/2025-06-03-Choice-Hotels-Internationals-Global-Portfolio-Reaches-New-Heights-Driven-by-Upscale-and-Upper-Upscale-Growth-Internationally";
const RADISSON_PARAMARIBO =
  "https://media.choicehotels.com/2025-11-19-Choice-Hotels-International-to-Make-Landmark-Entry-into-Africa-Starting-with-Three-Hotels-in-Kenya";
const BLU_BARILOCHE =
  "https://media.choicehotels.com/2025-09-04-Choice-Hotels-International-Debuts-in-Argentina-with-the-Opening-of-Radisson-Blu-Bariloche";
const RED_FUNES =
  "https://media.choicehotels.com/2025-11-19-Choice-Hotels-International-to-Make-Landmark-Entry-into-Africa-Starting-with-Three-Hotels-in-Kenya";
const RED_MIRAFLORES =
  "https://media.choicehotels.com/2023-07-26-Choice-Hotels-Completes-Radisson-Hotels-Americas-Milestone,-Integrating-Loyalty-Programs-And-Allowing-For-Full-Booking-Capabilities-On-ChoiceHotels-com";
const RED_PRESS_KIT = "https://media.choicehotels.com/Radisson-Red-press-kit";

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

export const RADISSON_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "radisson",
  brandName: "Radisson by Choice",
  replaceMomentum: true,
  momentumLabel: "Choice Hotels CALA openings · linked announcements",
  momentumCards: [
    card({
      title: "Suriname Debut With Paramaribo Downtown Hotel",
      dateLine: "Oct 2025",
      summary:
        "Radisson Hotel Paramaribo opened on Domineestraat—Choice Hotels' first Radisson in Suriname and a full-service conversion on the former Hotel Krasnapolsky site. Use as new-market CALA conversion context; confirm operator full-service depth and prototype residuals locally.",
      url: RADISSON_PARAMARIBO,
      sort: 1,
    }),
    card({
      title: "Pacific Coast Beach Conversion Debuts At Playa Caracol",
      dateLine: "Jun 2025",
      summary:
        "Radisson Riviera Panama opened at Playa Caracol / Punta Chamé—core Radisson beach resort conversion on Panama's Pacific coast. Owners should diligence leisure conversion capital and F&B intensity versus urban full-service prototypes before affiliation.",
      url: RADISSON_PANAMA,
      sort: 2,
    }),
    card({
      title: "Airport Full-Service Opening At San Luis Potosí",
      dateLine: "Feb 2025",
      summary:
        "Radisson San Luis Potosí, Aeropuerto opened near Ponciano Arriaga—positioned for airport capture and industrial-corridor transient demand. Owner relevance: core Radisson airport conversion/new-build pattern under Choice CALA distribution.",
      url: RADISSON_SLP,
      sort: 3,
    }),
    card({
      title: "Meetings-Led Greenfield Opens In Puebla Angelópolis",
      dateLine: "Oct 2024",
      summary:
        "Choice Hotels CALA inaugurated an 80-room Radisson in Puebla's Angelópolis corridor—meetings, spa, and rooftop F&B for a secondary-city upscale box. Owners comparing core Radisson should underwrite full-service labor and meetings scope—not Blu design capital or RED lifestyle select-service assumptions.",
      url: RADISSON_PUEBLA,
      sort: 4,
    }),
  ],
  geoIntro:
    "Core Radisson under Choice Hotels is a global upscale full-service flag with visible CALA momentum—Mexico, Central America, Panama, Caribbean, and Suriname entries—alongside Americas airport, urban conversion, and secondary-city boxes. Owners should underwrite mainstream full-service economics and Choice Privileges participation—not Blu Nordic Nouveau capital or RED lifestyle select-service assumptions.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "North America remains a scale anchor for Radisson portfolio conversations—conversion economics, loyalty attach, and competition versus other upscale boxes in gateway and regional-hub markets. Confirm franchise disclosure counts and PIP scope for the specific asset.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "Strong CALA momentum for core Radisson—named openings in Puebla, San Luis Potosí, Panama Pacific coast, and Paramaribo illustrate urban greenfield, airport, beach conversion, and historic downtown repositioning. Owners should compare those patterns to their corridor before selecting Blu or RED instead.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe remains core to Radisson brand heritage and network density—useful standards and loyalty reference even when the live deal is in the Americas. Do not import European ramp curves as CALA feasibility; confirm Choice Americas agreement terms for the asset.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA growth is market-specific. Evaluate operator depth and fee stack before treating MEA as interchangeable with CALA conversion narratives; confirm authorization and disclosure for the specific geography.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC matters for international distribution context in gateway cities. Proof points are market-specific—do not assume U.S. or European ramp curves when underwriting an Americas or CALA core Radisson deal.",
      sort: 15,
    },
  ],
  growthThemes:
    "Urban conversion and repositioning\nAirport and industrial-corridor full-service\nCALA secondary-city greenfield\nCoastal and leisure conversions (core Radisson)\nMeetings-capable urban entries",
  growthEditorial:
    "Core Radisson under Choice compounds when owners fund credible full-service product and operators can deliver meetings-capable or leisure conversion stays inside Choice distribution. Named CALA openings are affiliation proof points—still underwrite local comps, PIP, and fee stack independently.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Radisson by Choice is the mainstream upscale full-service flag in the Radisson family—below Blu upper-upscale design intensity and distinct from RED lifestyle select-service and Individuals soft-collection paths. Owners should compare capital, F&B, and operator depth across those siblings before selecting a flag.",
  },
  notes: "Source-backed from radisson footprint momentum fixture (Choice CALA media).",
});

/** Blu often already passes named momentum + region cards; apply only if audit fails. */
export const RADISSON_BLU_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "radisson-blu",
  brandName: "Radisson Blu by Choice",
  replaceMomentum: true,
  momentumLabel: "Radisson Blu CALA · Choice-affiliated openings and portfolio highlights",
  momentumCards: [
    card({
      title: "Argentina Debut On Lake Nahuel Huapi",
      dateLine: "Sep 2025",
      summary:
        "Choice Hotels highlighted the opening of Radisson Blu Bariloche—upper-upscale lakeside positioning in San Carlos de Bariloche and the brand's Argentina entry under Choice. Owners should underwrite Nordic Nouveau capital and full-service intensity—not core Radisson conversion assumptions.",
      url: BLU_BARILOCHE,
      sort: 1,
    }),
    card({
      title: "São Paulo Upper-Upscale Gateway Reference",
      dateLine: "2024–2025",
      summary:
        "Radisson Blu São Paulo serves Brazil's primary business capital—meetings, dining, and Inspired Professional positioning. Owners comparing Blu affiliation should diligence design-forward public spaces and operator gallery-curator capacity against local comps.",
      url: "https://www.choicehotels.com/sao-paulo/sao-paulo/radisson-blu-hotels",
      sort: 2,
    }),
    card({
      title: "Caribbean Resort Compression In Aruba",
      dateLine: "2024–2025",
      summary:
        "Radisson Blu Aruba on Palm Beach illustrates resort-format upper-upscale leisure in the Caribbean CALA corridor. Use as a leisure Blu reference when underwriting resort capital and seasonal staffing—not as a light core Radisson beach conversion analogue.",
      url: "https://www.choicehotels.com/aruba/palm-beach/radisson-blu-hotels/aw007",
      sort: 3,
    }),
  ],
  geoIntro:
    "Radisson Blu under Choice Hotels is an upper-upscale flag with strong Caribbean and Latin America presence—open hotels in Argentina (Bariloche), Brazil, Chile, and Aruba—alongside select U.S. and Canada properties. Evaluate Inspired Professional guests, Nordic Nouveau capital, and Choice Privileges economics—not core Radisson conversion assumptions.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Selective Americas scale—U.S. and Canada hotels help guests recognize Blu in domestic gateways. For CALA projects, compare your asset to open Blu hotels in São Paulo, Santiago, Belo Horizonte Savassi, Bariloche, and Aruba before underwriting design-forward capital.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "Strong CALA relevance—open Choice-affiliated Radisson Blu hotels in Brazil, Chile, Argentina, and Aruba reflect upper-upscale urban and resort positioning with design-forward public spaces rather than light Radisson conversion economics.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Most global Blu history and design standards come from Europe. Americas deals may still reference European standards for FF&E and brand identity—confirm what applies to your Choice agreement and market.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "Middle East and Africa exposure varies by country and operator. For Americas projects, base footprint assumptions on CALA and U.S. disclosure—not MEA ramp curves.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "International travelers may know Blu from Asia-Pacific gateways. For U.S. or CALA franchise deals, focus on hotels in your region unless the asset is located in APAC.",
      sort: 15,
    },
  ],
  growthThemes:
    "CALA urban gateway upper-upscale\nResort and leisure destinations\nDesign-forward conversion and adaptive reuse\nMeetings-capable metro hotels\nNordic Nouveau capital discipline",
  growthEditorial:
    "Radisson Blu fits when owners can fund distinctive upper-upscale design and operators can deliver Inspired Professional service inside Choice distribution. Optional patch only—live profiles may already pass section pattern parity.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Radisson Blu by Choice is the design-forward upper-upscale full-service Radisson flag—above core Radisson and RED lifestyle select-service, and distinct from Collection and Individuals paths. Owners should underwrite Nordic Nouveau presentation and public-space capital before selecting Blu.",
  },
  notes:
    "replaceMomentum=false — Blu may already pass with named CALA cards and filled regions. Remediation should skip unless section_pattern_parity audit fails.",
});

export const RADISSON_RED_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "radisson-red",
  brandName: "Radisson RED by Choice",
  replaceMomentum: true,
  momentumLabel: "Choice Hotels CALA openings · linked announcements",
  momentumCards: [
    card({
      title: "RED Signed In Greater Rosario Corridor",
      dateLine: "Nov 2025",
      summary:
        "Choice Hotels extended its CALA footprint in Argentina with Radisson RED Funes in the greater Rosario area—named alongside Blu Bariloche and Radisson Paramaribo as new-market entries. Owners should underwrite RED's social lifestyle / select-service intensity—not Blu full-service design capital.",
      url: RED_FUNES,
      sort: 1,
    }),
    card({
      title: "Radisson RED Miraflores Live On ChoiceHotels.com",
      dateLine: "Jul 2023",
      summary:
        "Choice completed Radisson Hotels Americas integration—adding upscale hotels to ChoiceHotels.com including Radisson RED Hotel Miraflores in Lima, Peru, with Choice Privileges earn and redeem. Owner relevance: RED urban lifestyle distribution inside Choice systems.",
      url: RED_MIRAFLORES,
      sort: 2,
    }),
    card({
      title: "Americas RED System Context For Lifestyle Conversions",
      dateLine: "Sep 2023",
      summary:
        "Choice media materials describe Radisson RED as upscale select-service urban social positioning with OUIBar + KTCHN and 24/7 fitness. Owners evaluating RED should diligence lifestyle public-space activation and operator social-energy delivery—confirm current open and pipeline counts in disclosure rather than inventing Explorer counts.",
      url: RED_PRESS_KIT,
      sort: 3,
    }),
  ],
  geoIntro:
    "Radisson RED by Choice is the upscale lifestyle / select-service Radisson flag—urban social energy with flex F&B, distinct from Blu full-service design and core Radisson mainstream upscale. CALA signals include Miraflores (Lima) and Funes (greater Rosario). Confirm authorized geography and prototype residuals before underwriting affiliation.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Americas RED conversations center on urban lifestyle conversions and select-service social positioning with Choice Privileges participation. Owners should compare capital and F&B intensity versus Blu and core Radisson before selecting RED.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA RED relevance includes Miraflores (Lima) distribution integration and Funes (greater Rosario) signing context—urban lifestyle signals rather than resort Blu or meetings-heavy core Radisson boxes. Confirm local authorization and operator lifestyle capacity.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe contributes RED lifestyle recognition for international travelers. Americas deals should still underwrite to Choice Americas agreement terms and local urban comps—not European ramp curves.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA RED exposure is selective. Confirm market authorization before modeling affiliation; keep Americas underwriting anchored to urban lifestyle demand and Choice systems participation.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC may contribute gateway lifestyle recognition. For CALA or U.S. RED deals, focus on local urban demand and prototype fit rather than APAC density assumptions.",
      sort: 15,
    },
  ],
  growthThemes:
    "Urban lifestyle select-service\nSocial public spaces & flex F&B\nCALA gateway & secondary-city entries\nChoice Privileges lifestyle distribution\nDistinct from Blu full-service capital",
  growthEditorial:
    "Radisson RED compounds when owners fund social lifestyle product and operators can deliver energetic select-service stays inside Choice distribution. Named CALA signals help affiliation timing—still underwrite local urban comps without inventing hotel counts.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "Radisson RED by Choice is the upscale lifestyle / select-service Radisson flag—below Blu upper-upscale full-service design intensity and distinct from core Radisson mainstream upscale and Individuals soft-collection paths. Owners should compare public-space activation and F&B complexity across those siblings before selecting RED.",
  },
  notes: "Source-backed from radisson-red footprint momentum fixture. Avoid inventing open/pipeline counts in Explorer copy.",
});
