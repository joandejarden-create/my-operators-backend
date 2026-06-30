/**
 * CHI Brands Architecture _ Oct 2025.pdf — internal brand positioning (Nov 2025).
 * Single source for audit + apply scripts. Do not infer fields beyond this mapping.
 */

/** @typedef {{ tier: string, service: string, brandIdea: string, altBrandIdea?: string, strategicProposition: string, pillars?: string[], scaleNote?: string }} ArchBrand */

/** @type {Record<string, ArchBrand>} Keys are legacy profile names; use resolveArchForAirtableName for live Airtable rows. */
export const CHOICE_ARCHITECTURE_OCT2025 = {
  "Ascend Hotel Collection": {
    tier: "Upscale",
    service: "Select-Service",
    brandIdea: "Find your travel story.",
    strategicProposition: "We believe in the power of originality.",
    pillars: ["Distinct Details (Design)", "Home Away From Home (Experience)", "Heartfelt Connection (Service)"],
  },
  "Cambria Hotels": {
    tier: "Upscale",
    service: "Select-Service",
    brandIdea: "Going Places",
    strategicProposition: "A brand on the rise, for people who are too",
    pillars: ["Effortlessly Refined (Design)", "Fueling the Journey (Experience)", "Paving the Way (Service)"],
  },
  "Radisson (Choice)": {
    tier: "Upscale",
    service: "Full-Service",
    brandIdea: "Where New Feels Known",
    strategicProposition:
      "To give people the confidence to explore what's new, by offering them the safety of what's known.",
    pillars: ["Balanced Calm (Design)", "Ease of Discovery (Experience)", "Confident Authenticity (Service)"],
  },
  "Radisson Blu (Choice)": {
    tier: "Upper Upscale",
    service: "Full-Service",
    brandIdea: "Think in Blu",
    strategicProposition: "We believe in transcending the ordinary.",
    pillars: ["Nordic Nouveau (Product)", "Enticing Moments (Experience)", "Curatorial Warmth (Service)"],
  },
  "Radisson RED  (Choice)": {
    tier: "Upscale",
    service: "Select-Service",
    brandIdea: "Enjoy It!",
    strategicProposition: "We believe every moment matters.",
    pillars: ["Design With Attitude", "Share & Connect", "Fun & Flexible"],
  },
  "Radisson Collection  (Choice)": {
    tier: "Upper Upscale",
    service: "Full-Service",
    brandIdea: "Explorers Welcome.",
    strategicProposition: "We believe in fueling curiosity.",
    pillars: ["Vivid Settings", "Characterful Encounters", "Explorer's Compass"],
  },
  "Comfort Inn & Suites": {
    tier: "Upper Midscale",
    service: "Select-Service",
    brandIdea: "Where the joy happens.",
    strategicProposition: "A familiar base to unlock the joy of travel.",
    pillars: ["Rise & Shine (Product)", "Memories in the Making (Experience)", "Joy Loves Company (Service)"],
  },
  "Country Inn & Suites by Radisson (Choice)": {
    tier: "Upper Midscale",
    service: "Select-Service",
    brandIdea: "Generosity you can feel.",
    strategicProposition: "Generous hospitality with touches of home.",
    pillars: [
      "Comfortable Continuity (Product)",
      "There's One Place Like Home (Experience)",
      "Where The Heart Is (Service)",
    ],
  },
  "Sleep Inn": {
    tier: "Midscale",
    service: "Limited Service",
    brandIdea: "Dream Better Here",
    strategicProposition:
      "Deliver the lowest cost-to-build and operate midscale brand that doesn't compromise on design or guest experience.",
    pillars: ["Scenic Dreams (Product)", "Better Night's Rest (Experience)", "Happy to Help (Service)"],
  },
  Clarion: {
    tier: "Midscale",
    service: "Full-Service",
    brandIdea: "Get Together Here",
    strategicProposition:
      "Clarion delivers focused full-service hospitality designed to support meaningful gatherings.",
    pillars: ["On-site dining", "Meeting & event spaces", "Open lobby & bar", "Business support", "Fitness & wellness"],
    scaleNote: "Guardrails table lists Minimum Quality Levels as Midscale/Upper Midscale for Clarion column.",
  },
  "Clarion Pointe": {
    tier: "Midscale",
    service: "Limited Service",
    brandIdea: "stay on pointe",
    strategicProposition:
      "Clarion Pointe provides affordable elevated essentials in just the right places for a sharper, more connected stay",
    pillars: ["Focal Pointes", "Contemporary Design Touches", "Starting Pointe Breakfast", "On-Demand Connectivity"],
  },
  "Quality Inn": {
    tier: "Midscale",
    service: "Limited Service",
    brandIdea: "Get Your Money's Worth",
    strategicProposition: "Value means getting more for your money and creating memories that matter",
    pillars: ["Q Breakfast", "Q Bed", "Q Service", "Q Shower", "Q Essentials"],
  },
  "Rodeway Inn": {
    tier: "Economy",
    service: "Economy",
    brandIdea: "Good night. Great savings.",
    strategicProposition:
      "Rodeway Inn hotels give guests an affordable place to stay that they can rely on. The bare essentials. No frills, nothing fancy.",
  },
  "Econo Lodge": {
    tier: "Economy",
    service: "Economy",
    brandIdea: "Easy Stop On The Road",
    strategicProposition: "Econo Lodge hotels make it easy for guests to feel confident and capable when they travel.",
  },
  "Park Inn by Radisson (Choice)": {
    tier: "Premium Value",
    service: "Premium Value",
    brandIdea: "Have a Happy Stay",
    altBrandIdea: "Brighten up the stay",
    strategicProposition:
      "Delivering brighter basics with a contemporary design and elevated essentials at a competitive price.",
    scaleNote: "Grouped with Value & Economy in architecture; guardrails = Premium Value (not upper midscale).",
  },
  "Everhome Suites": {
    tier: "Extended Stay (Midscale positioning in portfolio)",
    service: "Extended Stay",
    brandIdea: "Closer to Home.®",
    strategicProposition:
      "For grey-collar work travelers and personal stay guests, Everhome Suites is more than a place to stay - it's an experience designed to keep life moving.",
  },
  "MainStay Suites": {
    tier: "Extended Stay",
    service: "Extended Stay",
    brandIdea: "Live Like Home.®",
    strategicProposition:
      "MainStay Suites is more than a place to sleep - it's a space designed to help guests stay in control of their lifestyle and maintain their routines.",
  },
  "WoodSpring Suites": {
    tier: "Economy Extended Stay",
    service: "Extended Stay",
    brandIdea: "It's Simple. Done Better.®",
    strategicProposition:
      "For blue-collar work travelers and personal stay guests, WoodSpring Suites is the straightforward, affordable extended stay hotel that delivers just what guests need",
  },
  "Suburban Studios": {
    tier: "Economy Extended Stay",
    service: "Extended Stay",
    brandIdea: "Longer Stays Made Easy",
    strategicProposition:
      "For hardworking travelers - from skilled trades to everyday guests - Suburban Studios makes longer stays easy and affordable.",
  },
};

/** Live Airtable Brand Name → CHOICE_ARCHITECTURE_OCT2025 key */
export const AIRTABLE_NAME_TO_ARCH_KEY = {
  "Park Inn by Choice": "Park Inn by Radisson (Choice)",
  "Radisson by Choice": "Radisson (Choice)",
  "Radisson Blu by Choice": "Radisson Blu (Choice)",
  "Radisson RED by Choice": "Radisson RED  (Choice)",
  "Radisson Collection by Choice": "Radisson Collection  (Choice)",
  "Country Inn & Suites by Choice": "Country Inn & Suites by Radisson (Choice)",
  "Country Inn & Suites by Radisson": "Country Inn & Suites by Radisson (Choice)",
  "Country Inn & Suites": "Country Inn & Suites by Radisson (Choice)",
};

/** Not covered in Oct 2025 architecture deck (use other source materials). */
export const CHOICE_ARCHITECTURE_NOT_IN_DOC = [
  "Radisson Individuals by Choice",
  "Radisson Individual (Choice)",
  "Radisson Inn & Suites",
  "Park Plaza by Choice",
  "Park Plaza (Choice)",
];

export const ARCHITECTURE_SYNC_FIELD_ALLOWLIST = [
  "Brand Tagline",
  "Hotel Chain Scale",
  "Hotel Service Model",
  "Brand Positioning",
  "Brand Pillars",
];

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[™®©.]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} airtableBrandName
 * @returns {ArchBrand | null}
 */
export function resolveArchForAirtableName(airtableBrandName) {
  const name = String(airtableBrandName || "").trim();
  const key = AIRTABLE_NAME_TO_ARCH_KEY[name] || name;
  return CHOICE_ARCHITECTURE_OCT2025[key] || null;
}

/**
 * @param {ArchBrand} arch
 * @returns {string | null}
 */
export function mapArchitectureTierToChainScale(arch) {
  const tier = norm(arch.tier);
  if (tier.includes("premium value")) return "Economy";
  if (tier.includes("economy extended stay")) return "Economy";
  if (tier.includes("extended stay") && tier.includes("midscale")) return "Midscale";
  if (tier === "extended stay") return "Midscale";
  if (tier.includes("upper upscale")) return "Upper Upscale";
  if (tier === "upscale") return "Upscale";
  if (tier.includes("upper midscale")) return "Upper Midscale";
  if (tier === "midscale") return "Midscale";
  if (tier === "economy") return "Economy";
  return null;
}

/**
 * @param {ArchBrand} arch
 * @returns {string | null}
 */
export function mapArchitectureServiceToHotelServiceModel(arch) {
  const s = norm(arch.service);
  if (s.includes("extended stay")) return "Extended Stay";
  if (s.includes("limited service") || s.includes("select-service") || s.includes("economy") || s.includes("premium value")) {
    return "Select-Service";
  }
  if (s.includes("full-service") || s === "full service") return "Full-Service";
  return arch.service || null;
}

/**
 * @param {ArchBrand} arch
 * @returns {string}
 */
export function buildArchitecturePositioning(arch) {
  const parts = [];
  if (arch.strategicProposition) parts.push(arch.strategicProposition.trim());
  if (arch.brandIdea) parts.push(`Brand idea: ${arch.brandIdea.replace(/\.$/, "")}.`);
  return parts.join(" ");
}

/** Remove internal architecture source suffix from stored Brand Positioning copy. */
export function stripArchitectureSourceFromPositioning(text) {
  return String(text || "")
    .replace(/\s*Source:\s*CHI Brands Architecture\s*\([^)]+\)\s*[—–-]\s*[^.]+(?:\.|$)/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {ArchBrand} arch
 * @returns {string | null}
 */
export function buildArchitecturePillars(arch) {
  if (!arch.pillars?.length) return null;
  return arch.pillars.join("\n\n");
}

/**
 * Build Airtable Brand Basics patch from architecture record.
 * @param {string} airtableBrandName
 * @returns {Record<string, string> | null}
 */
export function buildArchitectureBasicsFields(airtableBrandName) {
  const arch = resolveArchForAirtableName(airtableBrandName);
  if (!arch) return null;

  const fields = {
    "Brand Tagline": arch.brandIdea,
    "Brand Positioning": buildArchitecturePositioning(arch),
  };

  const chainScale = mapArchitectureTierToChainScale(arch);
  if (chainScale) fields["Hotel Chain Scale"] = chainScale;

  const serviceModel = mapArchitectureServiceToHotelServiceModel(arch);
  if (serviceModel) fields["Hotel Service Model"] = serviceModel;

  const pillars = buildArchitecturePillars(arch);
  if (pillars) fields["Brand Pillars"] = pillars;

  return fields;
}
