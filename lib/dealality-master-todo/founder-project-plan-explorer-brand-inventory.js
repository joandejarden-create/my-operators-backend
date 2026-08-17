/**
 * Dealality pilot Brand Explorer taxonomy — chain scale × parent company.
 * Working taxonomy for pilot readiness (not each company's internal portfolio labels).
 *
 * @typedef {{ parent: string, brands: string[] }} BrandParentGroup
 * @typedef {{
 *   id: string,
 *   seedId: string,
 *   recordId: string,
 *   stepNumber: number,
 *   taskName: string,
 *   deliverables: string,
 *   priorityNote: string,
 *   groups: BrandParentGroup[],
 * }} ChainScaleSegment
 */

/** @type {ChainScaleSegment[]} */
export const CHAIN_SCALE_BRAND_SEGMENTS = [
  {
    id: "luxury-ultra-luxury",
    seedId: "explorer-scale-01-luxury",
    recordId: "recyByGp3IMbKekKw",
    stepNumber: 6,
    taskName: "Complete Brand Explorer profiles — Luxury / Ultra-Luxury brands",
    deliverables: "Completed Luxury / Ultra-Luxury Brand Explorer checklist and profile updates",
    priorityNote:
      "Highest-detail Brand Explorers — owner positioning, luxury conversion, resort repositioning, high-stakes operator/brand conversations.",
    groups: [
      {
        parent: "Marriott",
        brands: [
          "Ritz-Carlton",
          "Ritz-Carlton Reserve",
          "St. Regis",
          "JW Marriott",
          "EDITION",
          "The Luxury Collection",
          "W Hotels",
          "Bulgari Hotels & Resorts",
        ],
      },
      {
        parent: "Hilton",
        brands: ["Waldorf Astoria", "Conrad", "LXR", "NoMad"],
      },
      {
        parent: "Hyatt",
        brands: ["Park Hyatt", "Alila", "Miraval", "Andaz", "Thompson"],
      },
      {
        parent: "IHG",
        brands: ["Six Senses", "Regent", "InterContinental", "Kimpton", "Vignette Collection"],
      },
      {
        parent: "Accor",
        brands: [
          "Raffles",
          "Orient Express",
          "Fairmont",
          "Sofitel Legend",
          "Sofitel",
          "MGallery",
          "Emblems",
          "Banyan Tree",
          "Faena",
        ],
      },
      {
        parent: "Minor Hotels",
        brands: ["Anantara", "Tivoli", "Minor Reserve Collection", "Elewana"],
      },
      {
        parent: "BWH / WorldHotels",
        brands: ["WorldHotels Luxury", "WorldHotels Elite"],
      },
      {
        parent: "Radisson",
        brands: ["Radisson Collection", "Radisson Blu"],
      },
      {
        parent: "Iberostar",
        brands: ["JOIA by Iberostar"],
      },
    ],
  },
  {
    id: "upper-upscale-premium",
    seedId: "explorer-scale-02-upper-upscale",
    recordId: "recogqpllM7cKkqCG",
    stepNumber: 7,
    taskName: "Complete Brand Explorer profiles — Upper-Upscale / Premium brands",
    deliverables: "Completed Upper-Upscale / Premium Brand Explorer checklist and profile updates",
    priorityNote:
      "Most important for pilot — serious hotel owner evaluating brand/operator options.",
    groups: [
      {
        parent: "Marriott",
        brands: [
          "Marriott Hotels",
          "Sheraton",
          "Westin",
          "Renaissance",
          "Le Méridien",
          "Delta Hotels",
          "Autograph Collection",
          "Tribute Portfolio",
          "Design Hotels",
          "Gaylord Hotels",
          "MGM Collection with Marriott Bonvoy",
        ],
      },
      {
        parent: "Hilton",
        brands: [
          "Hilton Hotels & Resorts",
          "Signia by Hilton",
          "DoubleTree",
          "Embassy Suites",
          "Curio Collection",
          "Tapestry Collection",
          "Graduate by Hilton",
        ],
      },
      {
        parent: "Hyatt",
        brands: [
          "Grand Hyatt",
          "Hyatt Regency",
          "Hyatt",
          "Hyatt Centric",
          "Destination by Hyatt",
          "The Unbound Collection",
          "Dream Hotels",
        ],
      },
      {
        parent: "IHG",
        brands: [
          "Crowne Plaza",
          "voco",
          "Hotel Indigo",
          "HUALUXE",
          "Iberostar Beachfront Resorts",
          "Ruby",
        ],
      },
      {
        parent: "Accor",
        brands: [
          "Pullman",
          "Swissôtel",
          "Mövenpick",
          "Grand Mercure",
          "Mantis",
          "Art Series",
          "Peppers",
          "The Sebel",
        ],
      },
      {
        parent: "Choice / Radisson Americas",
        brands: [
          "Cambria",
          "Ascend Hotel Collection",
          "Radisson",
          "Radisson RED",
          "Radisson Individuals",
        ],
      },
      {
        parent: "BWH",
        brands: ["BW Premier Collection", "Best Western Premier", "BW Signature Collection"],
      },
      {
        parent: "Minor Hotels",
        brands: ["NH Collection", "Avani", "nhow"],
      },
      {
        parent: "Iberostar",
        brands: ["Iberostar Selection"],
      },
    ],
  },
  {
    id: "upscale-lifestyle-boutique",
    seedId: "explorer-scale-03-upscale-lifestyle",
    recordId: "recUTjIKhqzLunNgS",
    stepNumber: 8,
    taskName: "Complete Brand Explorer profiles — Upscale / Lifestyle / Boutique brands",
    deliverables: "Completed Upscale / Lifestyle / Boutique Brand Explorer checklist and profile updates",
    priorityNote:
      "Conversion/repositioning use cases where a hotel does not fit a traditional full-service flag.",
    groups: [
      {
        parent: "Marriott",
        brands: ["AC Hotels", "Aloft", "Moxy", "citizenM", "Protea Hotels", "Series by Marriott"],
      },
      {
        parent: "Hilton",
        brands: [
          "Canopy",
          "Tempo",
          "Motto",
          "Curio Collection",
          "Tapestry Collection",
          "Graduate",
        ],
      },
      {
        parent: "Hyatt",
        brands: [
          "Caption by Hyatt",
          "Hyatt Centric",
          "JDV by Hyatt",
          "Bunkhouse Hotels",
          "Dream Hotels",
        ],
      },
      {
        parent: "IHG",
        brands: ["Kimpton", "Hotel Indigo", "voco", "Ruby", "Vignette Collection"],
      },
      {
        parent: "Accor / Ennismore",
        brands: [
          "25hours",
          "Mama Shelter",
          "The Hoxton",
          "Mondrian",
          "SLS",
          "SO/",
          "Delano",
          "Hyde",
          "Morgans Originals",
          "21c Museum Hotels",
          "Jo&Joe",
          "Tribe",
          "Handwritten Collection",
        ],
      },
      {
        parent: "BWH",
        brands: ["Aiden", "Sadie", "GLō", "Vīb"],
      },
      {
        parent: "Wyndham",
        brands: [
          "TRYP by Wyndham",
          "Dazzler by Wyndham",
          "Esplendor Boutique Hotels",
          "Vienna House by Wyndham",
          "Trademark Collection by Wyndham",
        ],
      },
      {
        parent: "Radisson",
        brands: ["Radisson RED", "Radisson Individuals"],
      },
    ],
  },
  {
    id: "upper-midscale-select-service",
    seedId: "explorer-scale-04-upper-midscale",
    recordId: "recohyfsjEmUASe5E",
    stepNumber: 9,
    taskName: "Complete Brand Explorer profiles — Upper-Midscale / Select-Service brands",
    deliverables: "Completed Upper-Midscale / Select-Service Brand Explorer checklist and profile updates",
    priorityNote:
      "Major pilot group — owners deciding conversion, select-service, upper-midscale, or regional brand path.",
    groups: [
      {
        parent: "Marriott",
        brands: [
          "Courtyard",
          "Four Points",
          "Fairfield",
          "SpringHill Suites",
          "City Express by Marriott",
          "Four Points Flex by Sheraton",
        ],
      },
      {
        parent: "Hilton",
        brands: ["Hilton Garden Inn", "Hampton by Hilton", "Spark by Hilton"],
      },
      {
        parent: "Hyatt",
        brands: ["Hyatt Place", "Hyatt House"],
      },
      {
        parent: "IHG",
        brands: ["Holiday Inn", "Holiday Inn Express", "EVEN Hotels", "Garner", "avid"],
      },
      {
        parent: "Choice / Radisson Americas",
        brands: [
          "Comfort",
          "Country Inn & Suites by Radisson",
          "Sleep Inn",
          "Quality Inn",
          "Clarion",
          "Clarion Pointe",
          "Park Inn by Radisson",
          "Radisson Inn & Suites",
        ],
      },
      {
        parent: "Wyndham",
        brands: [
          "La Quinta",
          "Wingate",
          "AmericInn",
          "Baymont",
          "Microtel",
          "Ramada",
          "Ramada Encore",
        ],
      },
      {
        parent: "BWH",
        brands: ["Best Western Plus", "Best Western", "Executive Residency"],
      },
    ],
  },
  {
    id: "midscale-economy",
    seedId: "explorer-scale-05-midscale",
    recordId: "recyCmdpANFWF1C1B",
    stepNumber: 10,
    taskName: "Complete Brand Explorer profiles — Midscale / Economy brands",
    deliverables: "Completed Midscale / Economy Brand Explorer checklist and profile updates",
    priorityNote:
      "Lower priority unless pilot owner has conversion, roadside, value, or secondary-market asset.",
    groups: [
      {
        parent: "Choice",
        brands: ["Econo Lodge", "Rodeway Inn"],
      },
      {
        parent: "Wyndham",
        brands: [
          "Days Inn",
          "Super 8",
          "Travelodge",
          "Howard Johnson",
          "Baymont",
          "Microtel",
        ],
      },
      {
        parent: "Accor",
        brands: ["ibis", "ibis Styles", "ibis budget", "hotelF1", "greet", "BreakFree"],
      },
      {
        parent: "IHG",
        brands: ["Garner", "avid", "Holiday Inn Express"],
      },
      {
        parent: "BWH",
        brands: ["SureStay", "SureStay Plus", "SureStay Collection"],
      },
    ],
  },
  {
    id: "extended-stay-all-suites",
    seedId: "explorer-scale-06-extended-stay",
    recordId: "recsMTzJEXYsXN2hQ",
    stepNumber: 11,
    taskName: "Complete Brand Explorer profiles — Extended-Stay / All-Suites brands",
    deliverables: "Completed Extended-Stay / All-Suites Brand Explorer checklist and profile updates",
    priorityNote:
      "Distinct operating model — length of stay, labor, room mix, kitchen requirements, owner economics.",
    groups: [
      {
        parent: "Marriott",
        brands: [
          "Residence Inn",
          "TownePlace Suites",
          "Element",
          "StudioRes",
          "Apartments by Marriott Bonvoy",
          "Marriott Executive Apartments",
        ],
      },
      {
        parent: "Hilton",
        brands: [
          "Homewood Suites",
          "Home2 Suites",
          "LivSmart Studios",
          "Embassy Suites",
          "Hilton Grand Vacations",
        ],
      },
      {
        parent: "Hyatt",
        brands: ["Hyatt House"],
      },
      {
        parent: "IHG",
        brands: [
          "Staybridge Suites",
          "Candlewood Suites",
          "Atwell Suites",
          "Holiday Inn Club Vacations",
        ],
      },
      {
        parent: "Choice",
        brands: ["MainStay Suites", "Everhome Suites", "Suburban Studios", "WoodSpring Suites"],
      },
      {
        parent: "Wyndham",
        brands: ["Hawthorn Extended Stay", "WaterWalk", "Echo Suites Extended Stay"],
      },
      {
        parent: "BWH",
        brands: ["SureStay Studio", "Executive Residency"],
      },
      {
        parent: "Accor",
        brands: ["Adagio", "Mantra"],
      },
    ],
  },
  {
    id: "resort-all-inclusive",
    seedId: "explorer-scale-07-resort",
    recordId: "recdqXb3Zo3q2KtJr",
    stepNumber: 12,
    taskName: "Complete Brand Explorer profiles — Resort / All-Inclusive brands",
    deliverables: "Completed Resort / All-Inclusive Brand Explorer checklist and profile updates",
    priorityNote:
      "Caribbean, Mexico, Spain, resort, and leisure-heavy owner conversations.",
    groups: [
      {
        parent: "Hyatt Inclusive Collection",
        brands: [
          "Hyatt Ziva",
          "Hyatt Zilara",
          "Secrets",
          "Dreams",
          "Breathless",
          "Zoëtry",
          "Alua",
          "Sunscape",
          "Impression by Secrets",
        ],
      },
      {
        parent: "IHG / Iberostar",
        brands: [
          "Iberostar Beachfront Resorts",
          "JOIA by Iberostar",
          "Iberostar Selection",
          "Iberostar Waves",
        ],
      },
      {
        parent: "Wyndham",
        brands: ["Wyndham Alltra"],
      },
      {
        parent: "TUI",
        brands: ["TUI BLUE", "TUI MAGIC LIFE", "ROBINSON"],
      },
      {
        parent: "Accor",
        brands: [
          "Rixos",
          "Banyan Tree",
          "Fairmont resort properties",
          "Sofitel resort properties",
        ],
      },
      {
        parent: "Minor",
        brands: ["Anantara Resorts", "Avani Resorts", "Tivoli Resorts"],
      },
    ],
  },
  {
    id: "soft-brand-collection",
    seedId: "explorer-scale-08-soft-brand",
    recordId: "recvgqK2MgcMz2FeE",
    stepNumber: 13,
    taskName: "Complete Brand Explorer profiles — Soft Brand / Collection brands",
    deliverables: "Completed Soft Brand / Collection Brand Explorer checklist and profile updates",
    priorityNote:
      "High pilot relevance — owners ask: hard flag, stay independent, or join a collection?",
    groups: [
      {
        parent: "Marriott",
        brands: [
          "Autograph Collection",
          "Tribute Portfolio",
          "The Luxury Collection",
          "Design Hotels",
          "MGM Collection",
          "Series by Marriott",
        ],
      },
      {
        parent: "Hilton",
        brands: ["Curio Collection", "Tapestry Collection", "LXR"],
      },
      {
        parent: "Hyatt",
        brands: ["The Unbound Collection", "Destination by Hyatt", "JDV by Hyatt"],
      },
      {
        parent: "IHG",
        brands: ["Vignette Collection", "voco", "Hotel Indigo"],
      },
      {
        parent: "Choice",
        brands: ["Ascend Hotel Collection", "Radisson Individuals"],
      },
      {
        parent: "Wyndham",
        brands: ["Trademark Collection", "Registry Collection"],
      },
      {
        parent: "BWH / WorldHotels",
        brands: [
          "WorldHotels Luxury",
          "WorldHotels Elite",
          "WorldHotels Distinctive",
          "WorldHotels Crafted",
          "BW Premier Collection",
          "BW Signature Collection",
          "SureStay Collection",
        ],
      },
      {
        parent: "Accor",
        brands: ["MGallery", "Handwritten Collection", "Emblems"],
      },
    ],
  },
];

/**
 * Normalize inventory brand label → repo profile lookup key.
 * @type {Record<string, string>}
 */
export const BRAND_INVENTORY_TO_REPO = {
  "radisson blu": "Radisson Blu (Choice)",
  "radisson collection": "Radisson Collection  (Choice)",
  radisson: "Radisson (Choice)",
  "radisson red": "Radisson RED  (Choice)",
  "radisson individuals": "Radisson Individual (Choice)",
  "radisson individual": "Radisson Individual (Choice)",
  "ascend hotel collection": "Ascend Hotel Collection",
  cambria: "Cambria Hotels",
  comfort: "Comfort Inn & Suites",
  "country inn & suites by radisson": "Country Inn & Suites by Radisson (Choice)",
  "sleep inn": "Sleep Inn",
  "quality inn": "Quality Inn",
  clarion: "Clarion",
  "clarion pointe": "Clarion Pointe",
  "park inn by radisson": "Park Inn by Radisson (Choice)",
  "radisson inn & suites": "Radisson Inn & Suites",
  "econo lodge": "Econo Lodge",
  "rodeway inn": "Rodeway Inn",
  "mainstay suites": "MainStay Suites",
  "everhome suites": "Everhome Suites",
  "suburban studios": "Suburban Studios",
  "woodspring suites": "WoodSpring Suites",
  "park plaza": "Park Plaza (Choice)",
  kimpton: "Kimpton",
  "curio collection": "Curio Collection by Hilton",
  "curio collection by hilton": "Curio Collection by Hilton",
};

export function normalizeBrandKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[™®]/g, "")
    .trim();
}

/** Unique brand names within a segment (deduped). */
export function flattenSegmentBrands(segment) {
  const seen = new Set();
  const out = [];
  for (const g of segment.groups) {
    for (const brand of g.brands) {
      const key = normalizeBrandKey(brand);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ brand, parent: g.parent, key });
    }
  }
  return out;
}

export function buildSegmentScopeDescription(segment) {
  const lines = [
    "Purpose:",
    `- Complete Brand Explorer profiles for every brand in the ${segment.taskName.replace("Complete Brand Explorer profiles — ", "")} pilot taxonomy.`,
    "",
    segment.priorityNote,
    "",
    "Scope (by parent company):",
  ];
  for (const g of segment.groups) {
    lines.push(`- ${g.parent}: ${g.brands.join(", ")}`);
  }
  lines.push(
    "",
    "For each brand profile capture: affiliation model, standards, economics, footprint, case studies, conversion fit, pilot relevance.",
    "Flag unknown fields rather than guessing.",
    "",
    "Completion standard:",
    "- Every listed brand has its own Brand Explorer coverage row with status (Complete, Needs Review, Missing Data, Not Applicable, or Deferred).",
    "- Profiles usable in pilot demos, sample outputs, and owner/advisor conversations."
  );
  return lines.join("\n");
}

export function slugifyParent(parent) {
  return normalizeBrandKey(parent)
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

export function extractScaleLabel(segment) {
  return segment.taskName
    .replace("Complete Brand Explorer profiles — ", "")
    .replace(/ brands$/, "");
}

/**
 * @typedef {{
 *   segmentId: string,
 *   segmentSeedId: string,
 *   scaleLabel: string,
 *   parent: string,
 *   brands: string[],
 *   seedId: string,
 *   stepNumber: number,
 *   taskName: string,
 *   priorityNote: string,
 *   deliverables: string,
 * }} ChainScaleParentTask
 */

/** One FPP row per chain-scale × parent-company combination (60 rows). */
export function expandChainScaleParentTasks(startStep = 6) {
  /** @type {ChainScaleParentTask[]} */
  const tasks = [];
  let step = startStep;
  for (const segment of CHAIN_SCALE_BRAND_SEGMENTS) {
    const scaleLabel = extractScaleLabel(segment);
    for (const group of segment.groups) {
      tasks.push({
        segmentId: segment.id,
        segmentSeedId: segment.seedId,
        scaleLabel,
        parent: group.parent,
        brands: group.brands,
        seedId: `${segment.seedId}--${slugifyParent(group.parent)}`,
        stepNumber: step,
        taskName: `Complete Brand Explorer profiles — ${scaleLabel} — ${group.parent}`,
        priorityNote: segment.priorityNote,
        deliverables: `Completed ${group.parent} — ${scaleLabel} Brand Explorer profiles`,
      });
      step += 1;
    }
  }
  return tasks;
}

export function buildParentTaskScopeDescription(task) {
  return [
    "Purpose:",
    `- Complete Brand Explorer profiles for ${task.parent} brands in the ${task.scaleLabel} chain-scale segment.`,
    "",
    task.priorityNote,
    "",
    "Scope:",
    `- Chain scale: ${task.scaleLabel}`,
    `- Parent company: ${task.parent}`,
    `- Brands: ${task.brands.join(", ")}`,
    "",
    "For each brand profile capture: affiliation model, standards, economics, footprint, case studies, conversion fit, pilot relevance.",
    "Flag unknown fields rather than guessing.",
    "",
    "Completion standard:",
    "- Every listed brand has Brand Explorer coverage with status (Complete, Needs Review, Missing Data, Not Applicable, or Deferred).",
    "- Profiles usable in pilot demos, sample outputs, and owner/advisor conversations.",
  ].join("\n");
}
