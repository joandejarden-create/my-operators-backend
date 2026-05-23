/**
 * Six Overview proof cards per Tier 1 brand.
 * Card 5 is always Choice Privileges® (system + brand FDD Item 19).
 * Cards 1–4 and 6 are brand-specific strengths only.
 */
import { FDD_ITEM19 } from "./choice-fdd-item19.mjs";

/** @param {import('./choice-fdd-item19.mjs').FDD_ITEM19[string]} item19 */
function choicePrivilegesCard(item19) {
  const lines = [
    "System program: 7,100+ hotels worldwide; earn up to 10 points per $1 on eligible direct stays; reward nights from 8,000 points; U.S. News #1 hotel rewards program.",
  ];
  if (item19.loyaltyPct != null) {
    lines.push(
      `This brand (FDD Item 19, ${item19.performanceYear || "FY 2025"} sample): ~${item19.loyaltyPct}% of rooms from Choice Privileges contribution.`
    );
  } else {
    lines.push("This brand: no Choice Privileges room-mix % in current FDD Item 19—confirm in disclosure.");
  }
  if (item19.enterprisePct != null) {
    lines.push(`Enterprise / CRS booking mix ~${item19.enterprisePct}% in the same sample.`);
  } else if (item19.proprietaryPct != null) {
    lines.push(`Proprietary (non-OTA) booking mix ~${item19.proprietaryPct}% in the same sample.`);
  }
  if (item19.notes) lines.push(item19.notes);
  return {
    title: "Choice Privileges®",
    body: lines.join(" "),
  };
}

/** @type {Record<string, { title: string, body: string }[]>} */
const BRAND_PROOF = {
  "Comfort Inn & Suites": [
    {
      title: "Largest Smoke-Free Footprint",
      body: "100% smoke-free brand—largest smoke-free hotel brand in North America (press kit)—core identity versus midscale/economy competitors.",
    },
    {
      title: "1,600+ U.S. / 2,100+ Global",
      body: "Among the largest upper-midscale systems in Choice portfolio; 300+ pipeline with ~80% new construction (internal data, press kit).",
    },
    {
      title: "Post-Renaissance Momentum",
      body: "Major Welcome-to-Goodbye transformation; guest and franchisee satisfaction near all-time highs after product and experience refresh.",
    },
    {
      title: "Hearty Breakfast Standard",
      body: "Complimentary hearty breakfast (waffle bar, hot proteins, RAIO amenities, pillow choice)—defines operating model and labor plan.",
    },
    null,
    {
      title: "Breakfast-Led Operators",
      body: "Wins with operators who execute smoke-free compliance, breakfast food cost, and guestroom QA—not full-service F&B or meetings.",
    },
  ],
  "Sleep Inn": [
    {
      title: "Lowest Midscale Build Cost",
      body: "Press kit: lowest costs to build in midscale—efficient footprint and smart room layout for NC developers.",
    },
    {
      title: "550+ Open or Pipeline",
      body: "550+ Sleep Inn hotels worldwide; rapid U.S. expansion with nature-inspired simply stylish prototype.",
    },
    {
      title: "Sleep + MainStay Dual-Brand",
      body: "10 open dual-brand hotels; 90+ dual projects in pipeline—shared-site economics with MainStay extended stay.",
    },
    {
      title: "Morning Medley & Zenses",
      body: "Dream Cup coffee, Zenses bath products, Morning Medley breakfast—midscale amenity stack at 5.5% royalty.",
    },
    null,
    {
      title: "NC & Dual-Brand Operators",
      body: "Operators skilled at midscale new construction and optional dual-brand back-of-house—not upscale F&B.",
    },
  ],
  "Quality Inn": [
    {
      title: "Founding Choice Brand",
      body: "Brand Choice was founded on—high awareness and equity support competitive pricing in midscale corridors.",
    },
    {
      title: "1,800+ Hotels Globally",
      body: "1,800+ Quality hotels open; 1,600+ in U.S.; fastest-growing midscale brand in system (press kit).",
    },
    {
      title: "Conversion-Led ROI",
      body: "Growth fueled by conversions delivering high ROI—Value Q framework (Q Bed, Q Breakfast, Q Shower, Q Service, Q Essentials).",
    },
    {
      title: "5.25% Royalty Tier",
      body: "Lowest royalty among Tier 1 midscale trio (vs Sleep 5.5%, Comfort 6%)—conversion economics advantage.",
    },
    null,
    {
      title: "Conversion Specialists",
      body: "Operators who execute Value Q checklist and midscale breakfast—without Cambria meetings or resort capex.",
    },
  ],
  "Cambria Hotels": [
    {
      title: "J.D. Power #1 Upscale",
      body: "#1 upscale in J.D. Power 2023 North America Guest Satisfaction Index (press kit citation).",
    },
    {
      title: "Design-Forward Pipeline",
      body: "Growing faster than ever; 8 hotels opened in 2022 cited; NC, conversion, and adaptive reuse near demand generators.",
    },
    {
      title: "F&B, Bar & Rooftop",
      body: "Onsite restaurants, local craft beer, Cambria Estate Wines, rooftop/outdoor spaces—full upscale amenity stack.",
    },
    {
      title: "Convention & Urban Sites",
      body: "Site criteria: corporate campuses, convention centers, attractions—not passive interstate limited-service.",
    },
    null,
    {
      title: "Upscale F&B Operators",
      body: "Requires beverage, culinary, meetings sales, and design-forward GM depth—midscale operators need upgrade or partner.",
    },
  ],
  "MainStay Suites": [
    {
      title: "In-Suite Kitchen Product",
      body: "Residential extended-stay with kitchens—distinct from nightly midscale Comfort/Quality/Sleep.",
    },
    {
      title: "Dual-Brand With Sleep Inn",
      body: "10 open Sleep+MainStay; 90+ dual projects in pipeline—capital efficiency on one pad.",
    },
    {
      title: "Weekly-Stay Demand",
      body: "Project, relocation, and medical extended-stay corridors—revenue mix is weekly/monthly, not transient ADR alone.",
    },
    {
      title: "6.0% Extended-Stay Royalty",
      body: "Same 6% royalty as Suburban/Everhome—compare guest ADR and kitchen opex across extended trio.",
    },
    null,
    {
      title: "Extended-Stay Operators",
      body: "Kitchen FF&E, weekly billing, housekeeping cadence, and utility reserves—not breakfast buffet ops.",
    },
  ],
  "Ascend Hotel Collection": [
    {
      title: "142-Hotel FDD Sample",
      body: "Item 19 performance sample: 142 hotels (FY 2025)—collection scale without single prototype homogenization.",
    },
    {
      title: "Proprietary Booking Mix",
      body: "~45.8% proprietary (non-OTA) in sample—critical for independent and boutique positioning.",
    },
    {
      title: "Local Character Preserved",
      body: "Soft collection for unique independents—boutique, historic, and design conversions, not highway boxes.",
    },
    {
      title: "5.0% Membership Fee",
      body: "Membership fee on gross room revenues plus marketing & reservation fees per FDD—read full fee article.",
    },
    null,
    {
      title: "Boutique Collection Ops",
      body: "Balance local F&B/design story with collection QA and loyalty fulfillment—not limited-service breakfast only.",
    },
  ],
  Clarion: [
    {
      title: "Meetings-Capable Midscale",
      body: "Event space and group revenue potential—core difference from Quality/Sleep breakfast-only flags.",
    },
    {
      title: "155-Hotel Combined Sample",
      body: "Item 19 includes Clarion + Clarion Pointe (155 hotels, FY 2025)—shared performance tables.",
    },
    {
      title: "SMERF & Small Groups",
      body: "Suburban markets where local groups and corporate small meetings supplement transient.",
    },
    {
      title: "5.5% Midscale Royalty",
      body: "Midscale fee level vs Cambria 6% or Quality 5.25%—model meetings staffing in pro forma.",
    },
    null,
    {
      title: "Group Sales & Events",
      body: "Operators with modest banquet/catering capability—not breakfast-only or upscale resort culinary.",
    },
  ],
  "Clarion Pointe": [
    {
      title: "Select-Service Clarion",
      body: "Clarion family at lower capex than full Clarion—efficient prototype for conversion-friendly assets.",
    },
    {
      title: "Shared Clarion FDD Tables",
      body: "Item 19 combined with Clarion (155 hotels)—same loyalty/enterprise benchmarks; Pointe-specific standards separate.",
    },
    {
      title: "Modest Meeting Room",
      body: "Small group option without full Clarion banquet infrastructure—right-size public space investment.",
    },
    {
      title: "Conversion Economics",
      body: "Designed when building cannot carry full Clarion meetings/F&B capex—avoid over-building.",
    },
    null,
    {
      title: "Right-Sized Select-Service",
      body: "Match Pointe prototype to physical asset—do not sign full Clarion PIP scope on Pointe flag.",
    },
  ],
  "Econo Lodge": [
    {
      title: "Pure Economy Positioning",
      body: "Lowest amenity burden in Tier 1—travel basics without breakfast buffet or meetings revenue.",
    },
    {
      title: "5.0% Royalty",
      body: "Economy royalty tier with Rodeway—model full marketing, tech, and reservation fees in FDD.",
    },
    {
      title: "Price-Sensitive Corridors",
      body: "Highway and budget markets where economy ADR clears lean staffing and minimal public space.",
    },
    {
      title: "Fresh Coffee & Clean Rooms",
      body: "Core product: cleanliness, Wi-Fi, morning coffee—not Q Breakfast or event space.",
    },
    null,
    {
      title: "Lean Economy Operators",
      body: "Expert at cost per key and basic QA—accept ~33.8% loyalty attach in FDD sample.",
    },
  ],
  "Rodeway Inn": [
    {
      title: "600+ Economy Properties",
      body: "600+ Rodeway Inn hotels worldwide (press kit)—national brand recognition at budget tier.",
    },
    {
      title: "Lowest Loyalty Attach",
      body: "FDD FY 2025: ~26.8% Choice Privileges room contribution—lowest in Tier 1; plan OTA and highway retail.",
    },
    {
      title: "Travel Basics Product",
      body: "Fresh rooms, premium movie channels, Wi-Fi, morning coffee—no breakfast buffet or meetings.",
    },
    {
      title: "5.0% Economy Royalty",
      body: "Same royalty as Econo Lodge—win on recognition and cost control in your submarket.",
    },
    null,
    {
      title: "Disciplined Highway Ops",
      body: "Maximize occupancy at low ADR with minimal labor—realistic about limited loyalty lift.",
    },
  ],
  "Suburban Studios": [
    {
      title: "Studio Kitchenette Product",
      body: "Weekly/monthly guests with in-suite kitchenette—economy-extended vs MainStay residential upscale.",
    },
    {
      title: "Employment-Center Studios",
      body: "Industrial, medical, and project corridors—not convention, resort, or interstate transient-only.",
    },
    {
      title: "Economy-Extended ADR",
      body: "Lower guest spend than MainStay; compare Everhome for newest extended brand and prototype.",
    },
    {
      title: "Kitchen Wear & Utilities",
      body: "Returns driven by housekeeping cadence, appliance wear, and utility economics—not headline RevPAR.",
    },
    null,
    {
      title: "Weekly-Stay Studio Ops",
      body: "Operators who manage kitchenettes and weekly billing—not midscale breakfast execution.",
    },
  ],
  "Park Inn by Radisson (Choice)": [
    {
      title: "Radisson Family (Americas)",
      body: "Fresh, functional, friendly sub-brand—Choice operates Americas; RHG outside Americas.",
    },
    {
      title: "5-Hotel FDD Sample",
      body: "Item 19 uses only 5 hotels—treat footprint and performance metrics as directional, not system-wide.",
    },
    {
      title: "Airport & Suburban Fit",
      body: "Upper-midscale conversions without core Radisson upscale or Blu design-forward capex.",
    },
    {
      title: "5.5% Royalty",
      body: "Below Country Inn 6% and below full-service Radisson fee burden—confirm RHG vs Choice program rules.",
    },
    null,
    {
      title: "Functional Upper-Midscale",
      body: "Service discipline for airport/suburban full-service light—not economy lean or Blu gallery-curator.",
    },
  ],
  "Country Inn & Suites by Radisson (Choice)": [
    {
      title: "Warm Radisson Family",
      body: "Residential warmth and welcoming positioning—versus Park Inn functional sub-brand.",
    },
    {
      title: "Breakfast-Led Upper-Midscale",
      body: "Competes with Comfort on breakfast and suburban family corridors—6.0% royalty tier.",
    },
    {
      title: "Suburban & Family Markets",
      body: "Leisure and business suburban demand—not urban upscale F&B or economy highway-only.",
    },
    {
      title: "High Enterprise Mix",
      body: "FDD sample ~81.8% enterprise/CRS—distribution-heavy; execute on CRS and member fulfillment.",
    },
    null,
    {
      title: "Warm-Service Operators",
      body: "Guestroom warmth and breakfast consistency with Radisson-family QA under Choice systems.",
    },
  ],
  "Radisson RED  (Choice)": [
    {
      title: "Urban Social Upscale",
      body: "Upscale select-service in vibrant cities—OUIBar + KTCHN and social lobby, not full-service Radisson core.",
    },
    {
      title: "4 Open / 5 Pipeline",
      body: "Choice press kit (Sep 2023): 4 hotels, 606 rooms open; 5 in development across the Americas.",
    },
    {
      title: "Playful Design & Flex F&B",
      body: "Bold design, 24/7 fitness, rain showers, RED amenities—flex deli-bar versus full restaurant capex.",
    },
    {
      title: "No Item 19 Table",
      body: "Master CHI FDD cites 0 RED hotels in sample—property-level feasibility required.",
    },
    null,
    {
      title: "Informal Service Culture",
      body: "Operators who activate social hub and digiwall lobby—guest-controlled work/leisure blend.",
    },
  ],
  "Radisson Individual (Choice)": [
    {
      title: "Hand-Selected Soft Brand",
      body: "Upper-upscale independents and boutiques only—bold vision and exceptional service per press kit.",
    },
    {
      title: "Three Pillars",
      body: "Characterful Encounters, Vivid Settings, Explorer's Compass—local culture with reliable service.",
    },
    {
      title: "15 Hotels / 1,732 Rooms",
      body: "Choice press kit (Sep 2024): Americas operating scale for Individuals relaunch.",
    },
    {
      title: "No Item 19 Table",
      body: "Item 19: no financial performance representations—feasibility and local comps required.",
    },
    null,
    {
      title: "Collection Operators",
      body: "Preserve design uniqueness while meeting collection compliance and Explorer's Compass training.",
    },
  ],
  "Everhome Suites": [
    {
      title: "No Item 19 Averages",
      body: "FDD Item 19: no financial performance representations—underwrite from local extended-stay comps only.",
    },
    {
      title: "Newest Extended Platform",
      body: "Newest Choice extended-stay brand—confirm open count in Item 20 and development materials.",
    },
    {
      title: "Residential Weekly Suites",
      body: "Kitchen-equipped residential suites for weekly stays—not nightly midscale or upscale F&B.",
    },
    {
      title: "6% Room Revenue Royalty",
      body: "Six percent of room revenue for agreement duration (FDD)—compare MainStay and Suburban prototypes.",
    },
    null,
    {
      title: "Feasibility-Led Underwriting",
      body: "Extended-stay operators comfortable without system Item 19 tables—strong market study required.",
    },
  ],
};

/**
 * @param {string} brandName
 * @returns {{ title: string, body: string }[]}
 */
export function buildProofCards(brandName) {
  const item19 = FDD_ITEM19[brandName] || {};
  const slots = BRAND_PROOF[brandName];
  if (!slots || slots.length !== 6) {
    throw new Error(`Missing proof card set for: ${brandName}`);
  }
  const cp = choicePrivilegesCard(item19);
  return slots.map((s, i) => (i === 4 ? cp : s));
}
