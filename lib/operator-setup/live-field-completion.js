/**
 * Generic Operator Setup live-field completion engine.
 * Live Airtable is authority — not curated retained-field lists.
 */
export const LIVE_FIELD_COMPLETION_VERSION = "operator-setup-live-field-completion-v1";

export function isBlank(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

export function sampleValue(v, max = 120) {
  if (isBlank(v)) return null;
  if (Array.isArray(v)) {
    if (v[0] && typeof v[0] === "object" && v[0].url) return `[${v.length} attachments]`;
    return v.slice(0, 6).map(String).join("; ");
  }
  return String(v).replace(/\n/g, " ").slice(0, max);
}

/**
 * @typedef {'COMPLETE ALREADY'|'PARTIAL — POPULATE'|'EMPTY BUT ACTIVE — POPULATE'|'FIXTURE-ONLY — PRODUCTIONIZE'|'REMOVE'} FieldDisposition
 */

/**
 * Infer activity from live fill + known consumers.
 */
export function classifyFieldActivity(fieldStat, ctx = {}) {
  const { name, type, prodPop, fixPop } = fieldStat;
  const consumers = ctx.consumersByField?.[name] || [];
  const hasConsumer = consumers.length > 0;
  const isLink = type === "multipleRecordLinks";
  const isAttachment = type === "multipleAttachments";
  const isAuto = type === "formula" || type === "createdTime" || type === "lastModifiedTime" || type === "autoNumber";

  if (isAuto || name === "Operator") {
    return {
      disposition: "COMPLETE ALREADY",
      presumedActive: true,
      reason: "Identity/link/system field",
      strategy: "NONE",
    };
  }

  if (prodPop === 36) {
    return {
      disposition: "COMPLETE ALREADY",
      presumedActive: true,
      reason: "Already 36/36 Production",
      strategy: "NONE",
    };
  }

  if (prodPop > 0 && prodPop < 36) {
    // Attachment logos cannot be auto-completed safely
    if (isAttachment) {
      return {
        disposition: "REMOVE",
        presumedActive: false,
        reason: "Partial Production attachments — cannot auto-fabricate; resolve logo via website/CDN in UI",
        strategy: "REMOVE",
      };
    }
    // Thin unsupported census / deal-stage noise
    if (
      /^locationType/i.test(name) ||
      /^(capitalStatus|readyForInvestorPublication|marketExpansionRampTimeMonths|insuranceCoverage|energyEfficiency|wasteReduction|carbonTracking|crisisExperience)$/i.test(
        name
      ) ||
      (prodPop <= 3 && /Experience$|AvgStaff|Ramp|exits/i.test(name))
    ) {
      return {
        disposition: "REMOVE",
        presumedActive: false,
        reason: `Thin Production fill (${prodPop}/36) without defensible product methodology`,
        strategy: "REMOVE",
      };
    }
    return {
      disposition: "PARTIAL — POPULATE",
      presumedActive: true,
      reason: `Production values exist (${prodPop}/36)${hasConsumer ? "; product consumer present" : ""}`,
      strategy: inferStrategy(name, type),
    };
  }

  // prodPop === 0
  if (fixPop > 0) {
    return {
      disposition: hasConsumer ? "FIXTURE-ONLY — PRODUCTIONIZE" : "REMOVE",
      presumedActive: hasConsumer,
      reason: hasConsumer
        ? "Fixture examples only but consumer exists — productionize"
        : "Fixture/scaffold only — remove from active product",
      strategy: hasConsumer ? inferStrategy(name, type) : "REMOVE",
    };
  }

  if (hasConsumer || name === "additionalBrands") {
    if (name === "additionalBrands") {
      return {
        disposition: "REMOVE",
        presumedActive: false,
        reason: "Empty; redundant with brands / Brand Families",
        strategy: "REMOVE",
      };
    }
    return {
      disposition: "EMPTY BUT ACTIVE — POPULATE",
      presumedActive: true,
      reason: "Consumer/schema requires field despite empty Production",
      strategy: inferStrategy(name, type),
    };
  }

  return {
    disposition: "REMOVE",
    presumedActive: false,
    reason: "No Production values and no known consumer",
    strategy: "REMOVE",
  };
}

export function inferStrategy(name, type) {
  if (/^yearEstablished$/i.test(name)) return "DIRECT RESEARCH";
  if (/^yearsInBusiness$|^numberOfBrands$|^brandedVsIndependentMix$|^brand_conversion/i.test(name)) return "DERIVED";
  if (/_json$/i.test(name)) return "DERIVED";
  if (/brand_signal_/i.test(name)) return "CONTROLLED TAXONOMY";
  if (/figuresAsOf/i.test(name)) return "CONTROLLED TAXONOMY";
  if (/emergencyResponse|businessContinuity|support24x7|sustainabilityPrograms|esgReporting/i.test(name)) {
    return "CONTROLLED TAXONOMY";
  }
  if (type === "singleSelect" || type === "multipleSelects") return "CONTROLLED TAXONOMY";
  if (/overview_|companyTagline|brand_narrative|brand_soft|mission|philosophy|Description|History|differentiator/i.test(name)) {
    return "WRITER V2";
  }
  if (type === "multilineText" || type === "singleLineText") return "DIRECT RESEARCH";
  return "DIRECT RESEARCH";
}

/** Known product consumers (UI / API / Explorer) */
export const PROFILE_FIELD_CONSUMERS = Object.freeze({
  companyTagline: ["public/js/operator-dna-view-model.js", "operator-explorer"],
  overview_bestat_1_headline: ["public/js/operator-explorer-gold-mock-data.js"],
  overview_bestat_1_story: ["public/js/operator-explorer-gold-mock-data.js"],
  overview_bestat_2_headline: ["public/js/operator-explorer-gold-mock-data.js"],
  overview_bestat_2_story: ["public/js/operator-explorer-gold-mock-data.js"],
  overview_bestat_3_headline: ["public/js/operator-explorer-gold-mock-data.js"],
  overview_bestat_3_story: ["public/js/operator-explorer-gold-mock-data.js"],
  overview_why_1_headline: ["operator-explorer overview"],
  overview_why_1_story: ["operator-explorer overview"],
  overview_why_2_headline: ["operator-explorer overview"],
  overview_why_2_story: ["operator-explorer overview"],
  overview_why_3_headline: ["operator-explorer overview"],
  overview_why_3_story: ["operator-explorer overview"],
  overview_signal_1_value: ["operator-explorer overview"],
  overview_signal_2_value: ["operator-explorer overview"],
  overview_signal_3_value: ["operator-explorer overview"],
  brand_narrative_relationship: ["public/js/operator-brand-relationships-sections.js"],
  brand_narrative_compliance: ["public/js/operator-dna-dealality-insights.js"],
  brand_signal_reflag: ["public/js/operator-brand-relationships-sections.js"],
  brand_signal_audit: ["operator-explorer brand"],
  brand_signal_franchise_align: ["operator-explorer brand"],
  brand_signal_soft_retention: ["operator-explorer brand"],
  brand_portfolio_mix_json: ["operator-explorer brand"],
  brand_relationship_depth_json: ["operator-explorer brand"],
  brand_execution_capabilities_json: ["operator-explorer brand"],
  brand_governance_compliance_json: ["operator-explorer brand"],
  brand_soft_independent_narrative: ["operator-explorer brand"],
  companyDescription: ["operator-explorer", "operator-setup"],
  companyHistory: ["operator-explorer"],
  differentiators: ["operator-explorer"],
  primaryServiceModel: ["operator-alignment", "operator-explorer"],
  managementPhilosophy: ["operator-explorer"],
  missionStatement: ["operator-explorer"],
  yearEstablished: ["operator-explorer"],
  yearsInBusiness: ["operator-explorer"],
  brands: ["operator-setup", "operator-alignment"],
});

export function buildSemanticContract(fieldName, exampleValues = []) {
  const examples = exampleValues.filter(Boolean).slice(0, 5);
  const base = {
    fieldName,
    question: null,
    answerShape: null,
    length: null,
    examples,
  };
  if (fieldName === "companyTagline") {
    return {
      ...base,
      question: "What short official or company-used positioning line summarizes this operator?",
      answerShape: "1 short line (tagline / corporate slogan / development slogan)",
      length: "3–12 words preferred",
    };
  }
  if (/overview_bestat_(\d)_headline/.test(fieldName)) {
    return {
      ...base,
      question: `Best-at card ${RegExp.$1}: what is one concrete capability owners hire this operator for?`,
      answerShape: "Short headline (capability label)",
      length: "2–6 words",
    };
  }
  if (/overview_bestat_(\d)_story/.test(fieldName)) {
    return {
      ...base,
      question: `Best-at card ${RegExp.$1}: evidence-backed explanation of that capability`,
      answerShape: "1–2 factual sentences",
      length: "1–2 sentences",
    };
  }
  if (/overview_why_(\d)_headline/.test(fieldName)) {
    return {
      ...base,
      question: `Why-owners-choose reason ${RegExp.$1} headline`,
      answerShape: "Short headline",
      length: "2–7 words",
    };
  }
  if (/overview_why_(\d)_story/.test(fieldName)) {
    return {
      ...base,
      question: `Why-owners-choose reason ${RegExp.$1} story`,
      answerShape: "1–2 factual sentences",
      length: "1–2 sentences",
    };
  }
  if (/overview_signal_/.test(fieldName)) {
    return {
      ...base,
      question: "Short overview signal / proof point for the Explorer snapshot strip",
      answerShape: "Compact fact (scale, geography, model, tenure)",
      length: "2–8 words",
    };
  }
  if (fieldName === "brand_narrative_relationship") {
    return {
      ...base,
      question: "How does this operator relate to hotel brands (managed / franchise / proprietary)?",
      answerShape: "1–3 factual sentences",
      length: "1–3 sentences",
    };
  }
  if (fieldName === "brand_narrative_compliance") {
    return {
      ...base,
      question: "How does the operator handle brand standards / compliance / PIP?",
      answerShape: "1–2 factual sentences",
      length: "1–2 sentences",
    };
  }
  return {
    ...base,
    question: `Complete field ${fieldName} consistently with existing Production examples`,
    answerShape: examples[0] ? typeof examples[0] : "text",
    length: "match existing examples",
  };
}

export function verticalQaField(values, { requireNonBlank = true } = {}) {
  const issues = [];
  let blank = 0;
  const fingerprints = new Map();
  for (const row of values) {
    if (isBlank(row.value)) {
      blank++;
      continue;
    }
    const fp = String(row.value)
      .toLowerCase()
      .replace(/\d+/g, "N")
      .replace(/\s+/g, " ")
      .slice(0, 160);
    fingerprints.set(fp, (fingerprints.get(fp) || 0) + 1);
  }
  if (requireNonBlank && blank) issues.push({ type: "blank", count: blank });
  const templates = [...fingerprints.entries()].filter(([, n]) => n >= 3).map(([fp, n]) => ({ fp, n }));
  if (templates.length) issues.push({ type: "template_cluster", clusters: templates.length });
  return { pass: issues.length === 0, blank, templates: templates.length, issues };
}

export function deriveNumberOfBrands(brandLinks = []) {
  return Array.isArray(brandLinks) ? brandLinks.length : 0;
}

export function deriveBrandedVsIndependentMix(brandFamilies = []) {
  const fams = (brandFamilies || []).map(String);
  if (!fams.length) return "Not Measured / N/A";
  const ind = fams.filter((f) => /independent/i.test(f)).length;
  const branded = fams.length - ind;
  if (branded && !ind) return "Primarily branded";
  if (ind && !branded) return "Primarily independent";
  if (branded && ind) return "Mixed branded and independent";
  return "Not Measured / N/A";
}

export function buildBrandPortfolioMixJson({ brandNames = [], brandFamilies = [], om = "", companyName = "", differentiators = "" }) {
  const rows = [];
  const names = brandNames.length ? brandNames : brandFamilies;
  const who = companyName || "Operator";
  const ctx = String(differentiators || "").split(/[.!\n]/)[0].slice(0, 140);
  if (!names.length) {
    return JSON.stringify([
      {
        brandFlagType: /Brand|Integrated/i.test(om) ? who : "Independent / regional",
        portfolioMix: om || "Operator platform",
        assetContext: ctx || `${who} brand platform`,
        relationshipStatus: "Active / evidenced",
      },
    ]);
  }
  for (const b of names.slice(0, 12)) {
    rows.push({
      brandFlagType: String(b),
      portfolioMix: /Independent/i.test(String(b))
        ? `${who} independent / proprietary assets`
        : /Third-Party|Hybrid/i.test(om)
          ? `${who} third-party / hybrid — ${b}`
          : `${who} brand platform — ${b}`,
      assetContext: ctx || `${who} — ${b}`,
      relationshipStatus: /Brand|Integrated/i.test(om) ? "Brand-managed / proprietary" : "Active / evidenced",
    });
  }
  return JSON.stringify(rows);
}

export function buildBrandRelationshipDepthJson({ brandFamilies = [], om = "", companyName = "", differentiators = "" }) {
  const who = companyName || "Operator";
  const ctx = String(differentiators || "").split(/[.!\n]/)[0].slice(0, 180);
  const rows = (brandFamilies.length ? brandFamilies : ["Core platform"]).slice(0, 6).map((seg) => ({
    brandSegment: String(seg),
    relationshipType: /Brand|Integrated/i.test(om) ? "Brand-managed / proprietary" : /Third-Party|Hybrid/i.test(om) ? "Third-party / hybrid" : "Active / approved",
    depth: "Company-specific",
    ownerContext: ctx || `${who}: evidenced ${seg} relationship under ${om || "operator"} model`,
  }));
  return JSON.stringify(rows);
}

export function buildBrandExecutionJson({ om = "", companyName = "", differentiators = "", complianceNarrative = "" }) {
  const who = companyName || "Operator";
  const lead = String(complianceNarrative || differentiators || "").split(/[.!\n]/)[0].slice(0, 200);
  return JSON.stringify([
    {
      title: `${who.split("(")[0].trim()} brand standards execution`,
      description: lead || `${who} executes brand or proprietary standards through its ${om || "operating"} model.`,
    },
    {
      title: `${who.split("(")[0].trim()} owner onboarding / transition`,
      description: `${who} owner onboarding and operating transition follow the company’s documented ${om || "operating"} posture.`,
    },
  ]);
}

export function buildBrandGovernanceJson({ companyName = "", complianceNarrative = "", om = "" } = {}) {
  const who = companyName || "Operator";
  const lead = String(complianceNarrative || "").split(/[.!\n]/)[0].slice(0, 200);
  return JSON.stringify([
    {
      title: `${who.split("(")[0].trim()} standards & compliance`,
      description: lead || `${who} tracks brand or proprietary standards readiness under its ${om || "operating"} platform.`,
    },
  ]);
}

// —— Semantic QA (re-export) ——
export {
  PROFILE_SEMANTIC_QA_VERSION,
  classifyProfileCell,
  analyzeFieldVertical,
  looksGenericMarker,
  NARRATIVE_FIELDS,
  STANDARDIZED_TAXONOMY_FIELDS,
} from "./profile-semantic-qa.js";

