/**
 * Profile semantic QA — extends live-field-completion for cross-company quality.
 * Population ≠ validity. Narrative must be company-specific; taxonomy may repeat.
 */
import {
  isBannedGeneric,
  counterfactualCouldApplyToPeers,
  stripCompanyName,
} from "./field-specific-writer-v2.js";
import { isBlank, sampleValue, buildSemanticContract } from "./live-field-completion.js";

export const PROFILE_SEMANTIC_QA_VERSION = "operator-setup-profile-semantic-qa-v1";

/** Fields that legitimately share taxonomy / structured options across operators */
export const STANDARDIZED_TAXONOMY_FIELDS = new Set([
  "companySize",
  "primaryServiceModel",
  "chainScalesSupported",
  "propertyTypes",
  "additionalExperience",
  "Service Models Supported",
  "Brand Families Operated",
  "Soft Brand / Lifestyle Experience",
  "brand_signal_audit",
  "brand_signal_reflag",
  "brand_signal_franchise_align",
  "brand_signal_soft_retention",
  "emergencyResponse", // may be narrative OR select — treat carefully
  "businessContinuity",
  "support24x7",
  "sustainabilityPrograms",
  "esgReporting",
]);

/** Short structured facts (years, counts, URLs) */
export const DERIVED_OR_FACT_FIELDS = new Set([
  "company_name",
  "website",
  "headquarters",
  "yearEstablished",
  "yearsInBusiness",
  "numberOfBrands",
  "brands",
  "brandedVsIndependentMix",
  "brand_conversion_project_count",
  "figuresAsOf",
]);

/** Narrative / presentation fields requiring company specificity */
export const NARRATIVE_FIELDS = new Set([
  "companyDescription",
  "companyTagline",
  "companyHistory",
  "differentiators",
  "managementPhilosophy",
  "missionStatement",
  "overview_bestat_1_headline",
  "overview_bestat_1_story",
  "overview_bestat_2_headline",
  "overview_bestat_2_story",
  "overview_bestat_3_headline",
  "overview_bestat_3_story",
  "overview_why_1_headline",
  "overview_why_1_story",
  "overview_why_2_headline",
  "overview_why_2_story",
  "overview_why_3_headline",
  "overview_why_3_story",
  "overview_signal_1_value",
  "overview_signal_2_value",
  "overview_signal_3_value",
  "brand_narrative_compliance",
  "brand_narrative_relationship",
  "brand_soft_independent_narrative",
  "brand_portfolio_mix_json",
  "brand_relationship_depth_json",
  "brand_execution_capabilities_json",
  "brand_governance_compliance_json",
]);

const GENERIC_MARKERS = [
  /^August 2026 \(full live Profile completion\)$/i,
  /^Confirm in owner diligence — no standardized/i,
  /Documented brand relationship \/ current assignment/i,
  /Hotel operating delivery under .+ model with brand or proprietary standards as applicable/i,
  /Brand or proprietary standards readiness and recurring operating compliance as applicable/i,
  /^Evidenced .+ relationship under/i,
  /brand relationships are documented via Brand Relationships/i,
  /Operating posture reflects/i,
  /delivers hotel operations under a .+ model with evidenced portfolio capabilities/i,
  /^No company-wide public sustainability framework enumerated/i,
  /^No standardized public ESG reporting protocol enumerated/i,
];

/** Soft markers — only invalid when they dominate and lack company specificity */
const SOFT_GENERIC_STEMS = [
  /map property-level applicability in diligence/i,
  /confirm operator-owner reporting package/i,
  /confirm property-level (CALA )?playbooks in diligence/i,
  /confirm local BCP detail in diligence/i,
  /confirm after-hours escalation/i,
  /align ESG (reporting )?expectations during owner diligence/i,
  /initiatives are property- and brand-dependent/i,
];

export function looksGenericMarker(v) {
  if (isBlank(v)) return true;
  const s = typeof v === "string" ? v : JSON.stringify(v);
  if (isBannedGeneric(s)) return true;
  if (GENERIC_MARKERS.some((re) => re.test(s))) return true;
  return false;
}

export function looksSoftGenericBoilerplate(v, companyName) {
  const s = typeof v === "string" ? v : JSON.stringify(v);
  if (!SOFT_GENERIC_STEMS.some((re) => re.test(s))) return false;
  const shortCo = companyName ? companyName.split(/[\s(]/)[0] : "";
  const named = shortCo && shortCo.length > 2 && s.includes(shortCo);
  // Soft boilerplate alone without distinctive program names → generic
  if (!named) return true;
  if (/(Wave of Change|World of Hyatt|S\.P\.A\.R\.K|OwnView|A Sense of Place|Franchise Platform)/i.test(s)) {
    return false;
  }
  // Named but mostly diligence filler
  return SOFT_GENERIC_STEMS.filter((re) => re.test(s)).length >= 1 && s.length < 280;
}

const TAXONOMY_OK = new Set([
  "Not Measured / N/A",
  "Low",
  "Moderate",
  "High",
  "Yes",
  "No",
  "Yes - Standard",
  "Yes - Comprehensive",
  "Yes - Full 24/7",
  "Yes - Limited hours",
  "None documented",
  "Limited",
  "Active",
  "Strong",
]);

export function fingerprintValue(v) {
  if (isBlank(v)) return "";
  return String(typeof v === "object" ? JSON.stringify(v) : v)
    .toLowerCase()
    .replace(/\d+/g, "N")
    .replace(/\s+/g, " ")
    .replace(/["']/g, "")
    .slice(0, 220);
}

export function structuralTemplateFingerprint(v, companyName) {
  return fingerprintValue(stripCompanyName(String(typeof v === "object" ? JSON.stringify(v) : v), companyName));
}

export function isTaxonomyValue(v) {
  if (isBlank(v)) return false;
  if (Array.isArray(v)) return true;
  const s = String(v).trim();
  if (TAXONOMY_OK.has(s)) return true;
  if (/^(Yes|No)\b/i.test(s) && s.length < 40) return true;
  if (/^Not Measured/i.test(s)) return true;
  return false;
}

/**
 * Classify one cell.
 * @returns {{ verdict: string, issueType: string|null, proposedAction: string }}
 */
export function classifyProfileCell({
  fieldName,
  value,
  companyName,
  peerFingerprints = [],
  researchHints = null,
  fieldType = "text",
}) {
  const contract = buildSemanticContract(fieldName, []);
  const blank = isBlank(value);
  if (blank) {
    return {
      verdict: "INVALID — UNSUPPORTED",
      issueType: "blank_active",
      proposedAction: "POPULATE",
      contract,
    };
  }

  const s = typeof value === "string" ? value : JSON.stringify(value);
  const isNarrative = NARRATIVE_FIELDS.has(fieldName);
  const isTaxField = STANDARDIZED_TAXONOMY_FIELDS.has(fieldName);
  const isFact = DERIVED_OR_FACT_FIELDS.has(fieldName);

  // Derived / identity facts
  if (isFact || fieldName === "company_name") {
    return {
      verdict: fieldName === "yearsInBusiness" || fieldName === "numberOfBrands" || fieldName === "brandedVsIndependentMix"
        ? "VALID — DERIVED FACT"
        : "VALID — VERIFIED CORPORATE FACT",
      issueType: null,
      proposedAction: "KEEP",
      contract,
    };
  }

  // Taxonomy / selects
  if (isTaxField && (isTaxonomyValue(value) || Array.isArray(value) || s.length < 80)) {
    // Long diligence narratives in "taxonomy" fields are NOT standardized
    if (s.length > 120) {
      const shortCo = companyName ? companyName.split(/[\s(]/)[0] : "";
      const named = shortCo && shortCo.length > 2 && s.includes(shortCo);
      if (looksGenericMarker(value) && !named) {
        return {
          verdict: "INVALID — GENERIC",
          issueType: "generic_diligence_boilerplate",
          proposedAction: "REWRITE_COMPANY_SPECIFIC",
          contract,
        };
      }
      if (looksGenericMarker(value) && named && /^No (company-wide|standardized)/i.test(s)) {
        return {
          verdict: "INVALID — GENERIC",
          issueType: "generic_diligence_boilerplate",
          proposedAction: "REWRITE_COMPANY_SPECIFIC",
          contract,
        };
      }
      if (looksSoftGenericBoilerplate(value, companyName)) {
        return {
          verdict: "INVALID — GENERIC",
          issueType: "soft_diligence_boilerplate",
          proposedAction: "REWRITE_COMPANY_SPECIFIC",
          contract,
        };
      }
      if (named) {
        return {
          verdict: "VALID — COMPANY SPECIFIC",
          issueType: null,
          proposedAction: "KEEP",
          contract,
        };
      }
    }
    // Legitimate Not Measured for brand_signal_audit
    if (fieldName === "brand_signal_audit" && /^Not Measured/i.test(s)) {
      return {
        verdict: "VALID — STANDARDIZED TAXONOMY",
        issueType: null,
        proposedAction: "KEEP",
        contract,
      };
    }
    if (isTaxonomyValue(value) || Array.isArray(value)) {
      return {
        verdict: "VALID — STANDARDIZED TAXONOMY",
        issueType: null,
        proposedAction: "KEEP",
        contract,
      };
    }
  }

  // Brand JSON structural templates
  if (/_json$/i.test(fieldName)) {
    if (/Documented brand relationship \/ current assignment/i.test(s) || /as applicable to the platform/i.test(s)) {
      return {
        verdict: "INVALID — TEMPLATE",
        issueType: "identical_brand_json_template",
        proposedAction: "REWRITE_FROM_BRAND_FAMILIES",
        contract,
      };
    }
    const fp = fingerprintValue(value);
    const peers = peerFingerprints.filter((p) => p === fp).length;
    if (peers >= 3) {
      return {
        verdict: "INVALID — TEMPLATE",
        issueType: `json_duplicate_cluster_n${peers}`,
        proposedAction: "REWRITE_COMPANY_SPECIFIC_JSON",
        contract,
      };
    }
    // Company name in JSON → likely specific enough
    if (companyName && s.includes(companyName.split("(")[0].trim().slice(0, 8))) {
      return {
        verdict: "VALID — COMPANY SPECIFIC",
        issueType: null,
        proposedAction: "KEEP",
        contract,
      };
    }
  }

  // Generic markers
  if (looksGenericMarker(value) || looksSoftGenericBoilerplate(value, companyName)) {
    return {
      verdict: "INVALID — GENERIC",
      issueType: looksGenericMarker(value) ? "generic_marker" : "soft_diligence_boilerplate",
      proposedAction: "REWRITE_COMPANY_SPECIFIC",
      contract,
    };
  }

  // Narrative company-specificity
  if (isNarrative) {
    const cf = counterfactualCouldApplyToPeers(s, companyName);
    const fp = structuralTemplateFingerprint(value, companyName);
    const templatePeers = peerFingerprints.filter((p) => p === fp).length;

    // Year-founded signals are a valid repeated shape (years differ)
    // Founding-year recognition chips are company-specific facts with a shared signal shape
    // (not interchangeable taxonomy like Operating Model selects).
    if (/^overview_signal_/i.test(fieldName) && /^(founded|since)\s+\d{4}/i.test(s)) {
      return {
        verdict: "VALID — VERIFIED CORPORATE FACT",
        issueType: null,
        proposedAction: "KEEP",
        contract,
      };
    }

    // "What Differentiates [OPERATOR]" is a bad template headline
    if (/headline$/i.test(fieldName) && /what differentiates/i.test(fp) && templatePeers >= 3) {
      return {
        verdict: "INVALID — TEMPLATE",
        issueType: `headline_template_cluster_n${templatePeers}`,
        proposedAction: "REWRITE_COMPANY_SPECIFIC",
        contract,
      };
    }

    // Shared brand-managed boilerplate across Marriott/Hilton/IHG managed
    if (
      templatePeers >= 3 &&
      /brand-managed path|standards discipline|brand qa and management-agreement|label cala clearly|owners evaluating .+ management agreements/i.test(
        fp
      )
    ) {
      return {
        verdict: "INVALID — TEMPLATE",
        issueType: `template_cluster_n${templatePeers}`,
        proposedAction: "REWRITE_COMPANY_SPECIFIC",
        contract,
      };
    }

    // Short headlines / signals
    if (/headline$/i.test(fieldName) || /^overview_signal_/i.test(fieldName) || fieldName === "companyTagline") {
      if (s.length < 8) {
        return {
          verdict: "INVALID — INCONSISTENT GRANULARITY",
          issueType: "too_thin",
          proposedAction: "REWRITE",
          contract,
        };
      }
      if (fieldName === "companyTagline" && s.toLowerCase() === String(companyName || "").toLowerCase()) {
        return {
          verdict: "INVALID — GENERIC",
          issueType: "tagline_equals_company_name",
          proposedAction: "REWRITE_OFFICIAL_OR_POSITIONING",
          contract,
        };
      }
      return {
        verdict: "VALID — COMPANY SPECIFIC",
        issueType: null,
        proposedAction: "KEEP",
        contract,
      };
    }

    if (cf.fail && s.length > 40) {
      if (researchHints && researchHints.hasDistinctiveFacts) {
        return {
          verdict: "VALID — COMPANY SPECIFIC",
          issueType: null,
          proposedAction: "KEEP",
          contract,
        };
      }
      return {
        verdict: "INVALID — GENERIC",
        issueType: `counterfactual:${cf.reason}`,
        proposedAction: "RERESEARCH_REWRITE",
        contract,
      };
    }

    return {
      verdict: "VALID — COMPANY SPECIFIC",
      issueType: null,
      proposedAction: "KEEP",
      contract,
    };
  }

  // Long text in ops fields
  if (isTaxField && s.length > 80) {
    if (companyName && s.includes(companyName.split(/[\s(]/)[0])) {
      return {
        verdict: "VALID — COMPANY SPECIFIC",
        issueType: null,
        proposedAction: "KEEP",
        contract,
      };
    }
    if (looksGenericMarker(value) || /confirm in diligence/i.test(s)) {
      return {
        verdict: "INVALID — GENERIC",
        issueType: "ops_boilerplate",
        proposedAction: "REWRITE_COMPANY_SPECIFIC",
        contract,
      };
    }
  }

  return {
    verdict: "VALID — COMPANY SPECIFIC",
    issueType: null,
    proposedAction: "KEEP",
    contract,
  };
}

/**
 * Vertical field analysis across 36 operators.
 */
export function analyzeFieldVertical(fieldName, cells) {
  const fps = new Map();
  const structFps = new Map();
  for (const c of cells) {
    const fp = fingerprintValue(c.value);
    fps.set(fp, (fps.get(fp) || 0) + 1);
    const sfp = structuralTemplateFingerprint(c.value, c.operator);
    structFps.set(sfp, (structFps.get(sfp) || 0) + 1);
  }
  const uniqueMeaningful = [...fps.entries()].filter(([fp, n]) => fp && n >= 1).length;
  const duplicateClusters = [...fps.entries()].filter(([, n]) => n >= 2).map(([fp, n]) => ({ fp: fp.slice(0, 80), n }));
  const templateClusters = [...structFps.entries()].filter(([, n]) => n >= 3).map(([fp, n]) => ({ fp: fp.slice(0, 80), n }));

  const verdicts = cells.map((c) => c.qa?.verdict || "");
  const generic = verdicts.filter((v) => /GENERIC|TEMPLATE/.test(v)).length;
  const standardized = verdicts.filter((v) => v === "VALID — STANDARDIZED TAXONOMY").length;
  const companySpecific = verdicts.filter((v) => v === "VALID — COMPANY SPECIFIC" || v === "VALID — VERIFIED CORPORATE FACT" || v === "VALID — DERIVED FACT").length;

  return {
    fieldName,
    n: cells.length,
    uniqueMeaningful,
    duplicateClusters,
    templateClusters,
    genericRate: cells.length ? generic / cells.length : 0,
    standardizedValidRate: cells.length ? standardized / cells.length : 0,
    companySpecificRate: cells.length ? companySpecific / cells.length : 0,
  };
}

/**
 * Detect mission vs philosophy overlap for one operator.
 */
export function missionPhilosophyOverlap(mission, philosophy) {
  if (isBlank(mission) || isBlank(philosophy)) return { overlap: false, score: 0 };
  const a = new Set(
    String(mission)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 3)
  );
  const b = new Set(
    String(philosophy)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 3)
  );
  let inter = 0;
  for (const w of a) if (b.has(w)) inter++;
  const score = inter / Math.max(1, Math.min(a.size, b.size));
  return { overlap: score >= 0.55, score };
}

/**
 * Best-at cards must be distinct within one operator.
 */
export function bestAtDistinctness(h1, h2, h3) {
  const hs = [h1, h2, h3].map((x) => fingerprintValue(x)).filter(Boolean);
  const uniq = new Set(hs);
  return { distinct: uniq.size === hs.length, uniqueCount: uniq.size };
}

export function summarizeQaCells(cells) {
  const byVerdict = {};
  for (const c of cells) {
    const v = c.qa?.verdict || "UNCLASSIFIED";
    byVerdict[v] = (byVerdict[v] || 0) + 1;
  }
  return byVerdict;
}
