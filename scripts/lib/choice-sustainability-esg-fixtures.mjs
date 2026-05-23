/**
 * Sustainability & ESG field payloads for Choice Hotels International brands.
 */

export const CHOICE_CORPORATE_ESG = {
  "Sustainability Programs": "Yes - Standard",
  "ESG Reporting": "Yes - Annual",
  "Carbon Footprint Tracking": "Yes",
};

const RTG = "Choice Hotels' Room to be Green®";

/** @type {Record<string, { energy: string, waste: string, overrides?: Record<string, string> }>} */
export const BRAND_ESG = {
  "Ascend Hotel Collection": {
    energy: `• ${RTG} — property-level energy and water conservation for franchised hotels\n• Independent Ascend properties implement efficiency measures suited to each hotel's design, age, and local building codes`,
    waste: `• ${RTG} recycling and waste-reduction practices\n• Soft-brand hotels adapt waste programs to local operations while meeting membership standards`,
  },
  "Cambria Hotels": {
    energy: `• Value-engineered upscale prototype with thoughtful design efficiencies\n• ${RTG} participation for franchised Cambria properties`,
    waste: `• ${RTG} waste reduction and environmentally preferable purchasing\n• Bar-forward, quick-service food and beverage model versus full three-meal service`,
  },
  Clarion: {
    energy: `• ${RTG} for franchised full-service Clarion hotels\n• Meeting, event, and food and beverage operations follow Choice corporate environmental standards`,
    waste: `• ${RTG} recycling and waste diversion programs\n• On-site restaurant, bar, and banquet operations managed per brand standards`,
  },
  "Clarion Pointe": {
    energy: `• Midscale select-service prototype designed for efficient build and operations\n• ${RTG} for franchisees`,
    waste: `• Starting Pointe breakfast and marketplace grab-and-go model\n• ${RTG} waste-reduction practices`,
  },
  "Comfort Inn & Suites": {
    energy: `• Move to Modern refreshed guestrooms and public spaces\n• Largest 100% smoke-free midscale hotel brand in North America — improved indoor air quality for guests and associates\n• ${RTG}`,
    waste: `• Complimentary hot breakfast operations aligned with ${RTG} guidance\n• Smoke-free portfolio reduces environmental tobacco smoke exposure`,
  },
  "Country Inn & Suites by Radisson (Choice)": {
    energy: `• Upper-midscale select-service prototype with welcoming, home-like shared spaces\n• ${RTG} for franchised properties`,
    waste: `• Homestyle Country Breakfast and welcome cookie tradition with ${RTG} recycling\n• Warm communal spaces operated per brand design and operating standards`,
  },
  "Econo Lodge": {
    energy: `• Economy roadside hotels implement ${RTG} essentials within conversion-flexible standards\n• EasyStop hot coffee and practical in-room amenities with efficient operations`,
    waste: `• ${RTG} waste-reduction basics for the economy segment\n• Straightforward continental breakfast limits food waste versus full-service hotels`,
  },
  "Everhome Suites": {
    energy: `• Energy-efficient appliances, windows, and lighting\n• Electric vehicle charging stations where applicable\n• Modular movable furniture and prototypes built to brand standards and local codes`,
    waste: `• On-site self-service guest laundry reduces off-site trips for longer stays\n• Homebase 24/7 marketplace grab-and-go\n• ${RTG}`,
  },
  "MainStay Suites": {
    energy: `• Extended-stay apartment-style suites with in-room kitchens — lower daily food and beverage energy use than traditional nightly hotels\n• ${RTG}`,
    waste: `• In-suite kitchens reduce packaging waste from off-site dining\n• On-property guest laundry supports weekly and longer stays\n• ${RTG}`,
  },
  "Park Inn by Radisson (Choice)": {
    energy: `• Premium-value conversion segment with design standards that encourage efficient lighting and refreshed casegoods\n• ${RTG}`,
    waste: `• Continental breakfast and grab-and-go food and beverage model\n• Signature art and bright-welcome graphics program\n• ${RTG}`,
  },
  "Park Plaza (Choice)": {
    energy: `• Upscale full-service city and resort hotels participate in ${RTG} and Choice corporate environmental programs where applicable\n• Meetings and leisure operations per brand standards`,
    waste: `• Full-service food, beverage, and events managed with franchise environmental guidance\n• ${RTG}`,
  },
  "Quality Inn": {
    energy: `• Midscale Value Qs prototype focused on reliable essentials at efficient cost to build and operate\n• ${RTG} across the global system`,
    waste: `• Q Breakfast and core amenity model with ${RTG} waste reduction\n• Conversion-driven growth with standardized operating practices`,
  },
  "Radisson (Choice)": {
    energy: `• Upscale full-service conversions participate in ${RTG} and Choice corporate energy and water initiatives\n• Operational efficiency emphasized in brand development standards`,
    waste: `• ${RTG} for franchised Radisson hotels in the Americas\n• Food, beverage, and meeting operations per brand standards`,
  },
  "Radisson Blu (Choice)": {
    energy: `• Nordic Nouveau design prioritizing comfort, warmth, and efficient guestroom technology\n• ${RTG} and Choice upscale platform support`,
    waste: `• Wellbeing-focused bath amenities and in-room refreshments\n• ${RTG}`,
  },
  "Radisson Collection  (Choice)": {
    energy: `• Collection emphasizes wellness and sustainability across dining, fitness, and property experiences\n• Responsible Business integrated into the guest experience\n• ${RTG} where operationally applicable`,
    waste: `• Iconic properties implement property-appropriate waste and wellness programs\n• ${RTG} and parent-company ESG alignment`,
  },
  "Radisson Individual (Choice)": {
    energy: `• Hand-selected independent and boutique hotels participate in ${RTG} where applicable\n• Vivid settings and local design may incorporate regional materials and natural light`,
    waste: `• Soft-brand flexibility with ${RTG} as the corporate baseline\n• Property-specific recycling aligned to local practice and membership standards`,
  },
  "Radisson Inn & Suites": {
    energy: `• Naturally grounded design with biophilic materials, natural light, and indoor-outdoor connection\n• ${RTG}`,
    waste: `• Café lobby breakfast and residential-style guestrooms\n• ${RTG} waste-reduction standards`,
  },
  "Radisson RED  (Choice)": {
    energy: `• Upscale select-service urban prototype with an efficient operating model\n• ${RTG}`,
    waste: `• Flexible deli-bar food and beverage and social spaces\n• ${RTG}`,
  },
  "Rodeway Inn": {
    energy: `• Economy essentials model with ${RTG} within low-complexity operations\n• Fresh coffee, Wi-Fi, and an efficient roadside footprint`,
    waste: `• Good Night. Great Savings. positioning with minimal food and beverage\n• ${RTG} recycling basics`,
  },
  "Sleep Inn": {
    energy: `• Scenic Dreams nature-inspired prototype in a smoke-free environment\n• Flexible, efficient prototype designed to lower cost to build and operate`,
    waste: `• Dream Corner lobby marketplace\n• Efficient back-of-house and laundry placement\n• ${RTG}`,
  },
  "Suburban Studios": {
    energy: `• Economy extended-stay with in-room kitchens and flexible nightly, weekly, and monthly rates\n• Kitchen Pack Program for guest cooking essentials\n• ${RTG}`,
    waste: `• On-site guest laundry for longer stays\n• In-suite kitchens reduce off-site meal packaging\n• ${RTG}`,
  },
  "WoodSpring Suites": {
    energy: `• Energy-efficient windows, lighting, HVAC, water heaters, and appliances\n• Security lighting consistent with USDOE LZ2 requirements\n• EV charging stations and site design to minimize runoff and erosion`,
    waste: `• On-site 24/7 guest laundry for extended stays\n• In-suite kitchens reduce packaging from off-site meals\n• ${RTG}`,
  },
};

export function buildEsgFieldsForBrand(brandName) {
  const spec = BRAND_ESG[brandName];
  if (!spec) return null;
  return {
    ...CHOICE_CORPORATE_ESG,
    ...(spec.overrides || {}),
    "Energy Efficiency Initiatives": spec.energy,
    "Waste Reduction Programs": spec.waste,
  };
}

export const TARGET_BRANDS = Object.keys(BRAND_ESG);
