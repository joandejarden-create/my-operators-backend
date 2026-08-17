/**
 * Brand & Relationships Explorer seed payloads (Operator Setup — Profile table).
 * Keep in sync with public/js/operator-brand-relationships-sections.js DEFAULTS.
 */

export const BRAND_JSON_FIELD_KEYS = [
  "brand_portfolio_mix_json",
  "brand_relationship_depth_json",
  "brand_execution_capabilities_json",
  "brand_governance_compliance_json",
];

export const BRAND_TEXT_FIELD_KEYS = [
  "brand_soft_independent_narrative",
  "brand_conversion_project_count",
  "brandedVsIndependentMix",
];

export const BRAND_KPI_FIELD_KEYS = ["numberOfBrands"];

export const BRAND_EXPLORER_AIRTABLE_FIELD_SPECS = [
  ...BRAND_JSON_FIELD_KEYS.map((name) => ({ name, type: "multilineText" })),
  { name: "brand_soft_independent_narrative", type: "multilineText" },
  { name: "brand_conversion_project_count", type: "singleLineText" },
  { name: "brandedVsIndependentMix", type: "singleLineText" },
];

/** Per-operator variation for snapshot KPIs (index cycles). */
export const BRAND_KPI_PRESETS = [
  {
    numberOfBrands: 15,
    brandedVsIndependentMix: "62% branded / 38% independent",
    brand_conversion_project_count: "11",
    mixScale: 1,
  },
  {
    numberOfBrands: 12,
    brandedVsIndependentMix: "58% branded / 42% independent",
    brand_conversion_project_count: "9",
    mixScale: 0.95,
  },
  {
    numberOfBrands: 18,
    brandedVsIndependentMix: "65% branded / 35% independent",
    brand_conversion_project_count: "14",
    mixScale: 1.05,
  },
  {
    numberOfBrands: 10,
    brandedVsIndependentMix: "55% branded / 45% independent",
    brand_conversion_project_count: "8",
    mixScale: 0.9,
  },
  {
    numberOfBrands: 14,
    brandedVsIndependentMix: "60% branded / 40% independent",
    brand_conversion_project_count: "10",
    mixScale: 1,
  },
  {
    numberOfBrands: 16,
    brandedVsIndependentMix: "64% branded / 36% independent",
    brand_conversion_project_count: "12",
    mixScale: 1.02,
  },
];

const SEED_DEFAULTS = {
  brand_portfolio_mix_json: [
    {
      brandFlagType: "Marriott Family",
      portfolioMix: "18%",
      assetContext: "Upscale / Lifestyle / Select Service",
      relationshipStatus: "Active / Prior",
    },
    {
      brandFlagType: "Hilton Family",
      portfolioMix: "14%",
      assetContext: "Full-Service / Resort / Select Service",
      relationshipStatus: "Active / Approved",
    },
    {
      brandFlagType: "Hyatt family",
      portfolioMix: "9%",
      assetContext: "Lifestyle / Resort / Luxury",
      relationshipStatus: "Prior / Selective",
    },
    {
      brandFlagType: "IHG Family",
      portfolioMix: "8%",
      assetContext: "Lifestyle / Upscale / Conversion",
      relationshipStatus: "Active / Prior",
    },
    {
      brandFlagType: "Wyndham / Choice / Other",
      portfolioMix: "13%",
      assetContext: "Select-Service / Conversion / Resort-Adjacent",
      relationshipStatus: "Prior / Target",
    },
    {
      brandFlagType: "Independent / Soft Brand",
      portfolioMix: "38%",
      assetContext:
        "Independent Resorts, Collections, Condo-Hotels, Owner-Led Assets",
      relationshipStatus: "Active Strength",
    },
  ],
  brand_relationship_depth_json: [
    {
      brandSegment: "Global Luxury",
      relationshipType: "Prior experience",
      depth: "Selective",
      ownerContext:
        "Luxury resort, branded residence, high-touch service, elevated owner expectations",
    },
    {
      brandSegment: "Upper Upscale / Full Service",
      relationshipType: "Active / approved",
      depth: "Strong",
      ownerContext:
        "Conversions, resort assets, urban-leisure hybrids, owner reporting discipline",
    },
    {
      brandSegment: "Lifestyle",
      relationshipType: "Active / prior",
      depth: "Strong",
      ownerContext:
        "Independent conversions, experience-led positioning, F&B and programming relevance",
    },
    {
      brandSegment: "Resort",
      relationshipType: "Active / approved",
      depth: "Strong",
      ownerContext:
        "Beach, leisure, spa, F&B-heavy assets, complex staffing and guest experience",
    },
    {
      brandSegment: "Select Service",
      relationshipType: "Prior / target",
      depth: "Moderate",
      ownerContext:
        "Resort-adjacent, airport, business-leisure demand, development corridor opportunities",
    },
    {
      brandSegment: "Independent Collections",
      relationshipType: "Active",
      depth: "Strong",
      ownerContext:
        "Owner-controlled assets seeking distribution lift without losing identity",
    },
    {
      brandSegment: "Soft Brands",
      relationshipType: "Active / target",
      depth: "Strong",
      ownerContext:
        "Conversion flexibility, story-driven positioning, owner-friendly brand transition",
    },
  ],
  brand_execution_capabilities_json: [
    {
      title: "Brand Onboarding",
      description:
        "Supports owners through application, brand review, onboarding, documentation, and initial implementation.",
    },
    {
      title: "Standards Translation",
      description:
        "Helps owners understand the operational and capex implications of brand standards, technical services, and PIPs.",
    },
    {
      title: "Conversion / Reflag Execution",
      description:
        "Coordinates operating readiness, systems, training, staffing, and guest-facing transition issues.",
    },
    {
      title: "Brand-Owner Coordination",
      description:
        "Manages communication between ownership, brand development, operations, technical services, and property teams.",
    },
    {
      title: "Soft Brand Strategy",
      description:
        "Helps preserve asset identity while leveraging distribution, loyalty, and brand credibility.",
    },
    {
      title: "Independent-to-Branded Transition",
      description:
        "Supports owners moving from independent operations into a branded or collection environment.",
    },
  ],
  brand_governance_compliance_json: [
    {
      title: "Brand Compliance",
      description:
        "QA readiness, operating standards, audit preparation, and recurring compliance tracking.",
    },
    {
      title: "Technical Services Coordination",
      description:
        "Design, PIP, life safety, systems, opening checklists, and brand-required deliverables.",
    },
    {
      title: "Brand Reporting",
      description:
        "Brand performance reviews, guest metrics, loyalty contribution, and standards-related action plans.",
    },
    {
      title: "Owner Decision Support",
      description:
        "Clear explanation of brand trade-offs, obligations, timing, and operational requirements.",
    },
  ],
  brand_soft_independent_narrative:
    "If you are evaluating soft-brand affiliation, an independent-to-branded conversion, or repositioning an independent resort, you can expect help interpreting brand requirements, protecting local identity, preparing operationally for a flag transition, and understanding how distribution and loyalty may affect your operating model.",
};

function personalizeNarrative(text, companyName) {
  const name = String(companyName || "").trim();
  if (!name) return text;
  const s = String(text);
  if (/^if you are evaluating/i.test(s)) {
    return name + " can help if you are evaluating" + s.slice("If you are evaluating".length);
  }
  return s.replace(/^This operator/, name);
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function scaleMixPercents(rows, scale) {
  const cloned = cloneJson(rows);
  if (!scale || scale === 1) return cloned;
  let brandedTotal = 0;
  let indepIdx = -1;
  cloned.forEach((row, i) => {
    const label = String(row.brandFlagType || "").toLowerCase();
    const m = String(row.portfolioMix || "").match(/(\d+(?:\.\d+)?)/);
    if (!m) return;
    let pct = Math.round(parseFloat(m[1], 10) * scale);
    if (label.indexOf("independent") >= 0) {
      indepIdx = i;
      row.portfolioMix = `${pct}%`;
    } else {
      brandedTotal += pct;
      row.portfolioMix = `${pct}%`;
    }
  });
  if (indepIdx >= 0 && brandedTotal > 0) {
    cloned[indepIdx].portfolioMix = `${Math.max(0, 100 - brandedTotal)}%`;
  }
  return cloned;
}

/**
 * @param {{ index?: number, existingFields?: Record<string, unknown>, companyName?: string }} opts
 */
export function buildBrandExplorerSeedFields(opts) {
  opts = opts || {};
  const index = Number(opts.index) || 0;
  const existing = opts.existingFields || {};
  const companyName = opts.companyName || "";
  const preset = BRAND_KPI_PRESETS[index % BRAND_KPI_PRESETS.length];

  const mixRows = scaleMixPercents(SEED_DEFAULTS.brand_portfolio_mix_json, preset.mixScale);

  const fields = {
    brand_portfolio_mix_json: JSON.stringify(mixRows),
    brand_relationship_depth_json: JSON.stringify(cloneJson(SEED_DEFAULTS.brand_relationship_depth_json)),
    brand_execution_capabilities_json: JSON.stringify(
      cloneJson(SEED_DEFAULTS.brand_execution_capabilities_json)
    ),
    brand_governance_compliance_json: JSON.stringify(
      cloneJson(SEED_DEFAULTS.brand_governance_compliance_json)
    ),
    brand_soft_independent_narrative: personalizeNarrative(
      SEED_DEFAULTS.brand_soft_independent_narrative,
      companyName
    ),
    brand_conversion_project_count: preset.brand_conversion_project_count,
    brandedVsIndependentMix: preset.brandedVsIndependentMix,
    numberOfBrands: preset.numberOfBrands,
  };

  for (const key of BRAND_KPI_FIELD_KEYS) {
    const cur = existing[key];
    if (cur != null && cur !== "" && !(key === "numberOfBrands" && Number(cur) === 0)) {
      delete fields[key];
    }
  }

  const mixCur = existing.brandedVsIndependentMix;
  if (mixCur != null && String(mixCur).trim() !== "") {
    delete fields.brandedVsIndependentMix;
  }

  const convCur = existing.brand_conversion_project_count;
  if (convCur != null && String(convCur).trim() !== "") {
    delete fields.brand_conversion_project_count;
  }

  return fields;
}

/** Keys written by seed (full payload). */
export function brandExplorerSeedFieldKeys() {
  return [...BRAND_JSON_FIELD_KEYS, ...BRAND_TEXT_FIELD_KEYS, ...BRAND_KPI_FIELD_KEYS];
}

/** Keys that indicate explorer subsection seed is present (excludes numberOfBrands — often set elsewhere). */
export function brandExplorerSeedMarkerKeys() {
  return [...BRAND_JSON_FIELD_KEYS, ...BRAND_TEXT_FIELD_KEYS];
}
