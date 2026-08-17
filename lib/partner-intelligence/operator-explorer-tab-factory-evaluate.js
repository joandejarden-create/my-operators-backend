/**
 * Sync Operator Explorer Tab Factory evaluation (no Airtable loaders).
 * auditPass = failFindings === 0 (patch plan is not a pass).
 */
import {
  OPERATOR_FIELD_RESOLUTION_STATES,
  OPERATOR_TAB_CONTRACT_FAIL_RULES,
  OPERATOR_TAB_CONTRACTS,
  OPERATOR_TAB_FACTORY_VERSION,
  getOperatorTabFactoryContractSummary,
} from "./operator-explorer-tab-contracts.js";
import { isProtectedOperatorQualityBaseline } from "./operator-explorer-quality-baseline.js";
import { evaluateOperatorSectionPatternParity } from "./operator-explorer-section-pattern-parity.js";
import {
  collectFixtureProvenanceSources,
  evaluateOperatorSourceProvenanceByTab,
} from "./operator-explorer-source-provenance-by-tab.js";

const MIN_NARRATIVE_WORDS = 8;
const MIN_JSON_ITEMS = 1;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s)
    .split(/\s+/)
    .filter(Boolean).length;
}

function parseMaybeJson(raw) {
  if (raw == null) return null;
  if (typeof raw === "object") return raw;
  if (typeof raw !== "string") return raw;
  const t = raw.trim();
  if (!t) return null;
  if (!(t.startsWith("{") || t.startsWith("["))) return raw;
  try {
    return JSON.parse(t);
  } catch {
    return raw;
  }
}

function isEmptyValue(raw) {
  if (raw == null) return true;
  if (typeof raw === "string") return !nz(raw);
  if (Array.isArray(raw)) return raw.length === 0;
  if (typeof raw === "object") return Object.keys(raw).length === 0;
  return false;
}

function jsonHasSubstance(parsed) {
  if (parsed == null) return false;
  if (Array.isArray(parsed)) {
    if (parsed.length < MIN_JSON_ITEMS) return false;
    return parsed.some((item) => {
      if (item == null) return false;
      if (typeof item === "string") {
        // Brand family chips / short labels count (e.g. "Marriott", "Hilton")
        return nz(item).length >= 2;
      }
      if (typeof item === "object") {
        const title = nz(
          item.title ||
            item.headline ||
            item.name ||
            item.brandSegment ||
            item.function ||
            item.leadRole ||
            item.market ||
            item.language ||
            item.label
        );
        const body = nz(
          item.description ||
            item.story ||
            item.body ||
            item.ownerContext ||
            item.summary ||
            item.relevance ||
            item.depth ||
            item.capability ||
            item.notes
        );
        if (title || body) return true;
        const stringVals = Object.values(item).filter((v) => typeof v === "string" && nz(v));
        return stringVals.length >= 2;
      }
      return true;
    });
  }
  if (typeof parsed === "object") {
    if (Array.isArray(parsed.items)) return jsonHasSubstance(parsed.items);
    const intro = nz(parsed.intro || parsed.description || parsed.title);
    if (intro && words(intro) >= 4) return true;
    return Object.keys(parsed).length > 0;
  }
  return words(String(parsed)) >= 2;
}

function resolvePrefillValue(prefill, field) {
  const keys = [];
  if (field.prefillKey) keys.push(field.prefillKey);
  // Common aliases for child-backed fields
  if (field.prefillKey === "leadership_executives_json") {
    keys.push("leadershipTeam", "executives");
  }
  if (field.prefillKey === "case_studies_json") {
    keys.push("caseStudiesDetail", "caseStudies");
  }
  if (field.prefillKey === "owner_diligence_json") {
    keys.push("ownerDiligenceQa", "owner_diligence_qa");
  }

  for (const key of keys) {
    if (!key) continue;
    if (Object.prototype.hasOwnProperty.call(prefill, key) && !isEmptyValue(prefill[key])) {
      return { key, raw: prefill[key] };
    }
  }
  return { key: field.prefillKey || null, raw: undefined };
}

function classifyField(field, prefill) {
  const { key, raw } = resolvePrefillValue(prefill, field);
  const valueType = field.valueType || (field.fieldKey?.includes(".json.") ? "json" : "text");
  // PI allowGapCopy ≠ Tab Factory optional. Empty content hard-fails unless explicitly optional.
  const tabFactoryOptional = field.tabFactoryOptional === true;
  const isMeta = String(field.fieldKey || "").startsWith("op.meta.");
  const suppressKeys = Array.isArray(prefill?.__intentionalSuppressFieldKeys)
    ? prefill.__intentionalSuppressFieldKeys
    : [];
  const suppressReasons =
    prefill?.__intentionalSuppressReasons && typeof prefill.__intentionalSuppressReasons === "object"
      ? prefill.__intentionalSuppressReasons
      : {};

  if (suppressKeys.includes(field.fieldKey)) {
    return {
      status: "pass",
      resolution: "intentionally_suppressed",
      hardFail: false,
      reason: suppressReasons[field.fieldKey] || "Intentionally suppressed for this operator profile",
      key,
      preview: nz(raw).slice(0, 120),
    };
  }

  if (isMeta) {
    if (isEmptyValue(raw)) {
      return {
        status: "pass",
        resolution: "intentionally_suppressed",
        hardFail: false,
        reason: "Governance meta field empty — excluded from content hard-fail gate in v1",
        key,
        preview: "",
      };
    }
    return {
      status: "pass",
      resolution: "complete",
      hardFail: false,
      reason: "Governance meta present",
      key,
      preview: nz(raw).slice(0, 120),
    };
  }

  if (isEmptyValue(raw)) {
    if (tabFactoryOptional) {
      return {
        status: "pass",
        resolution: "intentionally_suppressed",
        hardFail: false,
        reason: "Empty tab-factory-optional field",
        key,
        preview: "",
      };
    }
    return {
      status: "fail",
      resolution: "blocked_empty_render",
      hardFail: true,
      reason: OPERATOR_TAB_CONTRACT_FAIL_RULES.visible_empty_field,
      key,
      preview: "",
      proposedPatch: `Populate ${field.displayLabel} (${field.fieldKey}) from operator-specific sources`,
    };
  }

  const parsed = valueType === "json" || valueType === "number" ? parseMaybeJson(raw) : raw;

  if (valueType === "json" || (typeof parsed === "object" && parsed != null)) {
    if (!jsonHasSubstance(parsed)) {
      return {
        status: "fail",
        resolution: "blocked_empty_render",
        hardFail: true,
        reason: OPERATOR_TAB_CONTRACT_FAIL_RULES.visible_empty_card,
        key,
        preview: typeof raw === "string" ? raw.slice(0, 120) : JSON.stringify(parsed).slice(0, 120),
        proposedPatch: `Thicken JSON content for ${field.displayLabel} to Arbor/HE card depth`,
      };
    }
    return {
      status: "pass",
      resolution: "complete",
      hardFail: false,
      reason: "JSON content has substance",
      key,
      preview: JSON.stringify(parsed).slice(0, 120),
    };
  }

  if (valueType === "number") {
    const n = Number(parsed);
    if (!Number.isFinite(n)) {
      return {
        status: "fail",
        resolution: "needs_patch",
        hardFail: true,
        reason: "Non-numeric value for number field",
        key,
        preview: nz(raw).slice(0, 120),
        proposedPatch: `Set numeric value for ${field.displayLabel}`,
      };
    }
    return {
      status: "pass",
      resolution: "complete",
      hardFail: false,
      reason: "Numeric value present",
      key,
      preview: String(n),
    };
  }

  const text = nz(parsed);
  const narrativeKeys =
    /history|philosophy|mission|narrative|description|differentiat|overview|story/i;
  const needsNarrative =
    narrativeKeys.test(field.fieldKey || "") ||
    narrativeKeys.test(field.displayLabel || "") ||
    narrativeKeys.test(field.prefillKey || "");

  if (needsNarrative && words(text) < MIN_NARRATIVE_WORDS) {
    return {
      status: "fail",
      resolution: "needs_patch",
      hardFail: true,
      reason: OPERATOR_TAB_CONTRACT_FAIL_RULES.below_benchmark_depth,
      key,
      preview: text.slice(0, 120),
      proposedPatch: `Expand ${field.displayLabel} to ≥${MIN_NARRATIVE_WORDS} words of operator-specific copy`,
    };
  }

  if (!text) {
    return {
      status: "fail",
      resolution: "blocked_empty_render",
      hardFail: true,
      reason: OPERATOR_TAB_CONTRACT_FAIL_RULES.visible_empty_field,
      key,
      preview: "",
      proposedPatch: `Populate ${field.displayLabel}`,
    };
  }

  return {
    status: "pass",
    resolution: "complete",
    hardFail: false,
    reason: "Text value present",
    key,
    preview: text.slice(0, 120),
  };
}

/**
 * @param {object} opts
 * @param {string} opts.operatorSlug
 * @param {string} [opts.operatorName]
 * @param {string} [opts.recordId]
 * @param {Record<string, unknown>} opts.prefill
 * @param {'fixtures'|'live'|'merged'} [opts.source]
 * @param {string[]} [opts.fixtureFiles]
 */
export function evaluateOperatorTabFactoryFromPayload({
  operatorSlug,
  operatorName = operatorSlug,
  recordId = null,
  prefill = {},
  source = "fixtures",
  fixtureFiles = [],
  provenanceSources = null,
} = {}) {
  const findings = [];
  const tabSummaries = [];

  for (const tab of OPERATOR_TAB_CONTRACTS) {
    let pass = 0;
    let fail = 0;
    for (const field of tab.fields) {
      const result = classifyField(field, prefill);
      const finding = {
        tabName: tab.tabName,
        tabIndex: tab.tabIndex,
        fieldKey: field.fieldKey,
        displayLabel: field.displayLabel,
        explorerSection: field.explorerSection,
        prefillKey: field.prefillKey || null,
        resolvedKey: result.key,
        status: result.status,
        resolution: result.resolution,
        hardFail: result.hardFail === true,
        reason: result.reason,
        preview: result.preview,
        proposedPatch: result.proposedPatch || null,
      };
      findings.push(finding);
      if (finding.hardFail) fail += 1;
      else pass += 1;
    }
    tabSummaries.push({
      tabName: tab.tabName,
      tabIndex: tab.tabIndex,
      fieldCount: tab.fields.length,
      passCount: pass,
      failCount: fail,
      auditPass: fail === 0,
    });
  }

  const hardFails = findings.filter((f) => f.hardFail);
  const patchPlan = hardFails
    .filter((f) => f.proposedPatch)
    .map((f) => ({
      fieldKey: f.fieldKey,
      tabName: f.tabName,
      proposedPatch: f.proposedPatch,
    }));

  const sectionPatternParity = evaluateOperatorSectionPatternParity({
    operatorSlug,
    operatorName,
    recordId,
    prefill,
    source,
  });

  const provenanceSourcesResolved =
    Array.isArray(provenanceSources) && provenanceSources.length
      ? provenanceSources
      : collectFixtureProvenanceSources(operatorSlug);
  const sourceProvenance = evaluateOperatorSourceProvenanceByTab({
    operatorSlug,
    operatorName,
    recordId,
    sources: provenanceSourcesResolved,
  });

  const fieldAuditPass = hardFails.length === 0;
  const auditPass =
    fieldAuditPass &&
    sectionPatternParity.pass === true &&
    sourceProvenance.pass === true;
  const patchPlanComplete = hardFails.every((f) => Boolean(f.proposedPatch));

  return {
    version: OPERATOR_TAB_FACTORY_VERSION,
    operatorSlug,
    operatorName,
    recordId,
    source,
    fixtureFiles,
    protectedBaseline: isProtectedOperatorQualityBaseline(operatorSlug) ||
      isProtectedOperatorQualityBaseline(recordId),
    contractSummary: getOperatorTabFactoryContractSummary(),
    auditComplete: true,
    patchPlanComplete,
    auditPass,
    fieldAuditPass,
    failFindings: hardFails.length,
    emptyRenderFailFindings: hardFails.filter((f) => f.resolution === "blocked_empty_render")
      .length,
    releaseQualityDecision: auditPass
      ? "field_complete"
      : !sourceProvenance.pass
        ? "source_provenance_required"
        : patchPlanComplete && fieldAuditPass && !sectionPatternParity.pass
          ? "section_pattern_parity_required"
          : patchPlanComplete
            ? "field_complete_after_patch"
            : "not_field_complete",
    tabSummaries,
    findings,
    failFindingDetails: hardFails,
    patchPlan,
    sectionPatternParity,
    sourceProvenance,
    gates: {
      tab_factory_audit: fieldAuditPass,
      rendered_field_completeness: fieldAuditPass,
      no_empty_rendered_components: hardFails.every(
        (f) => f.resolution !== "blocked_empty_render"
      ),
      source_provenance_by_tab: sourceProvenance.pass === true,
      section_pattern_parity: sectionPatternParity.pass === true,
      golden_content_quality: fieldAuditPass,
      operator_specific_source_validation:
        sourceProvenance.gates?.operator_specific_source_validation === true,
    },
    resolutionStates: OPERATOR_FIELD_RESOLUTION_STATES,
  };
}

/**
 * Lightweight unit helper — synthetic prefill.
 */
export function evaluateOperatorTabFactoryForTest(prefill, slug = "test-operator") {
  return evaluateOperatorTabFactoryFromPayload({
    operatorSlug: slug,
    operatorName: slug,
    prefill,
    source: "fixtures",
  });
}
