/**
 * Executive Intelligence copy governance.
 * Enforces semantic precision and evidence-construct-specific copy assembly.
 */

const FINDING_TYPES = Object.freeze({
  LARGEST_COMPETITIVE_GAP: "LARGEST_COMPETITIVE_GAP",
  HIGHEST_PRIORITY_REVIEW: "HIGHEST_PRIORITY_REVIEW",
  STRONGEST_VALIDATED_ASSOCIATION: "STRONGEST_VALIDATED_ASSOCIATION",
  POTENTIAL_AI_PERCEPTION_GAP: "POTENTIAL_AI_PERCEPTION_GAP",
  PROVIDER_DISAGREEMENT: "PROVIDER_DISAGREEMENT",
  MATERIAL_MOVEMENT: "MATERIAL_MOVEMENT",
  SOURCE_CITATION_GAP: "SOURCE_CITATION_GAP",
  NARRATIVE_PATTERN: "NARRATIVE_PATTERN",
  SOURCE_PATTERN: "SOURCE_PATTERN",
});

export const EXECUTIVE_COPY_GOVERNANCE_VERSION = "ai_visibility_exec_copy_governance_v1";

export const EVIDENCE_CONSTRUCTS = Object.freeze({
  PRESENCE: "PRESENCE",
  GAP: "GAP",
  ASSOCIATION: "ASSOCIATION",
  NARRATIVE: "NARRATIVE",
  TRUTH: "TRUTH",
  PROVIDER_COMPARISON: "PROVIDER_COMPARISON",
  SOURCE: "SOURCE",
  STABILITY: "STABILITY",
});

function sentenceCase(text) {
  const s = String(text || "").trim();
  if (!s) return "";
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function normalizeSpace(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function recurrencePhraseFromSupport(raw) {
  const s = String(raw || "").toLowerCase();
  if (!s) return null;
  if (s.includes("early")) return "Early repeated evidence";
  if (s.includes("recurr")) return "Recurring across monitored prompts";
  if (s.includes("repeat")) return "Repeated across monitored prompts";
  return null;
}

function replaceCitationMisuse(text, allowCitationTerms = false) {
  if (allowCitationTerms) return text;
  return String(text || "")
    .replace(/\bcited\b/gi, "appeared")
    .replace(/\bcitation\b/gi, "mention")
    .replace(/\bcitations\b/gi, "mentions");
}

function recurrenceLabelFromObservationCount(n = 0) {
  if (n <= 1) return "Early signal";
  if (n === 2) return "Repeated";
  return "Recurring";
}

function formatProviderCoverage(n) {
  if (!Number.isFinite(n) || n <= 0) return null;
  return `${n} provider${n === 1 ? "" : "s"}`;
}

function formatObservationEvidence(n, qualifier = null) {
  if (!Number.isFinite(n) || n <= 0) return qualifier || null;
  if (qualifier) return `${n} ${qualifier}`;
  return `${n} observation${n === 1 ? "" : "s"}`;
}

function formatScenarioBreadth(finding) {
  const n = Number(finding?.scenarioCount || 0);
  if (!Number.isFinite(n) || n <= 0) return null;
  return `${n} owner scenario${n === 1 ? "" : "s"}`;
}

function classifyEvidenceConstruct(finding) {
  switch (finding?.findingType) {
    case FINDING_TYPES.LARGEST_COMPETITIVE_GAP:
    case FINDING_TYPES.HIGHEST_PRIORITY_REVIEW:
      return EVIDENCE_CONSTRUCTS.GAP;
    case FINDING_TYPES.STRONGEST_VALIDATED_ASSOCIATION:
      return EVIDENCE_CONSTRUCTS.ASSOCIATION;
    case FINDING_TYPES.POTENTIAL_AI_PERCEPTION_GAP:
      return EVIDENCE_CONSTRUCTS.TRUTH;
    case FINDING_TYPES.PROVIDER_DISAGREEMENT:
      return EVIDENCE_CONSTRUCTS.PROVIDER_COMPARISON;
    case FINDING_TYPES.NARRATIVE_PATTERN:
      return EVIDENCE_CONSTRUCTS.NARRATIVE;
    case FINDING_TYPES.SOURCE_CITATION_GAP:
    case FINDING_TYPES.SOURCE_PATTERN:
      return EVIDENCE_CONSTRUCTS.SOURCE;
    case FINDING_TYPES.MATERIAL_MOVEMENT:
      return EVIDENCE_CONSTRUCTS.PRESENCE;
    default:
      return EVIDENCE_CONSTRUCTS.PRESENCE;
  }
}

function formatEvidenceLine(finding, construct) {
  if (construct === EVIDENCE_CONSTRUCTS.PROVIDER_COMPARISON) {
    if (finding?.providerStrongLabel && finding?.providerWeakLabel && finding?.providerStrongPct && finding?.providerWeakPct) {
      return `${finding.providerStrongLabel} ${finding.providerStrongPct} vs ${finding.providerWeakLabel} ${finding.providerWeakPct} · Same comparable prompt cohort`;
    }
    return finding.evidenceSummary || "Comparable prompt cohort";
  }

  if (construct === EVIDENCE_CONSTRUCTS.TRUTH) {
    const cls = finding?.governedClassification || null;
    return `Early signal · 1 observation${cls ? ` · Governed classification: ${cls}` : ""}`;
  }

  if (construct === EVIDENCE_CONSTRUCTS.NARRATIVE) {
    const parts = [
      formatProviderCoverage(finding.providerCount),
      finding.comparableResponseCount
        ? `${finding.comparableResponseCount} comparable responses`
        : formatObservationEvidence(finding.observationCount),
      formatScenarioBreadth(finding),
    ].filter(Boolean);
    if (!parts.length) return finding.evidenceSummary || "Narrative evidence observed";
    return parts.join(" · ");
  }

  if (construct === EVIDENCE_CONSTRUCTS.ASSOCIATION) {
    const parts = [
      formatProviderCoverage(finding.providerCount),
      formatObservationEvidence(finding.observationCount, "qualifying association observations"),
      finding.associationAttributeId
        ? `${sentenceCase(String(finding.associationAttributeId).replace(/_/g, " ").toLowerCase())} validated`
        : null,
    ].filter(Boolean);
    if (!parts.length) return finding.evidenceSummary || "Association evidence observed";
    return parts.join(" · ");
  }

  if (construct === EVIDENCE_CONSTRUCTS.GAP) {
    const recurrence =
      recurrencePhraseFromSupport(finding.observationSupport) ||
      (Number(finding.observationCount || 0) <= 1 ? "Early signal" : null);
    const parts = [
      formatProviderCoverage(finding.providerCount),
      formatObservationEvidence(finding.observationCount),
      finding.scenarioName && recurrence
        ? recurrence.replace("monitored prompts", String(finding.scenarioName))
        : recurrence,
    ].filter(Boolean);
    if (!parts.length) return finding.evidenceSummary || "Comparable prompt observations";
    return parts.join(" · ");
  }

  if (construct === EVIDENCE_CONSTRUCTS.SOURCE) {
    return finding.evidenceSummary || "Owned + external sources cited";
  }

  return finding.evidenceSummary || "Monitored evidence observed";
}

function formatHeadline(finding, construct) {
  const brand = finding.brandName || "This brand";
  const peer = finding.leadPeerName || "peer brands";
  const scenario = String(finding.scenarioName || finding.scenarioId || "").toLowerCase();

  if (construct === EVIDENCE_CONSTRUCTS.GAP) {
    if (scenario.includes("independent") && scenario.includes("conversion")) {
      return `${brand} is absent from independent conversion discussions where ${peer} appears`;
    }
    if (scenario.includes("collection") || scenario.includes("affiliation")) {
      return `${brand} is absent from collection-affiliation discussions where ${peer} appears`;
    }
    return `${brand} appears less often than ${peer} in monitored owner-decision discussions`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.PROVIDER_COMPARISON) {
    return `Portfolio visibility is materially higher on ${finding.providerStrongLabel || "one provider"} than ${finding.providerWeakLabel || "another provider"} in ${finding.context || "the selected cohort"}`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.TRUTH) {
    return `${brand} shows a potential brand-architecture perception gap in AI responses`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.ASSOCIATION) {
    const attr = String(finding.associationAttributeId || "").toLowerCase();
    if (attr.includes("distribution")) {
      return `${brand} is consistently associated with distribution in monitored responses`;
    }
    return `${brand} is consistently associated with ${String(finding.associationAttributeId || "a validated attribute")
      .replace(/_/g, " ")
      .toLowerCase()}`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.NARRATIVE) {
    const fam = String(finding.narrativeFamily || "").toLowerCase();
    if (fam.includes("distribution")) {
      return `${brand} is repeatedly represented around distribution across owner-decision scenarios`;
    }
    return `${brand} is repeatedly represented around ${String(finding.narrativeFamily || "a recurring narrative")
      .replace(/_/g, " ")
      .toLowerCase()}`;
  }
  return normalizeSpace(finding.headline || "");
}

function formatBody(finding, construct) {
  const brand = finding.brandName || "This brand";
  const scenario = finding.scenarioName || "owner-decision prompts";
  const peer = finding.leadPeerName || "peer brands";

  if (construct === EVIDENCE_CONSTRUCTS.GAP) {
    return `Across ${scenario}, AI responses mention ${peer} while ${brand} is frequently missing. This limits visibility in owner conversations where shortlist inclusion drives consideration.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.PROVIDER_COMPARISON) {
    return `Portfolio presence is stronger on one provider than another across the same monitored cohort. The spread shows why visibility should be reviewed provider by provider before portfolio-wide conclusions.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.TRUTH) {
    return `One observed response frames ${brand} in a way that diverges from Dealality's governed brand architecture. The signal is early, but it merits review because architecture context can shape owner interpretation.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.ASSOCIATION) {
    return `${brand} is repeatedly represented with this validated association in qualifying monitored responses. The pattern reinforces how owners are likely to see this brand theme in AI discussions.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.NARRATIVE) {
    return `${brand} is repeatedly represented with this narrative across monitored owner-decision contexts. The recurrence suggests a stable positioning pattern rather than a one-off mention.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.SOURCE) {
    return `Source citation patterns around this finding are concentrated in recurring domains. This helps prioritize source review without implying causal influence on model behavior.`;
  }
  return normalizeSpace(replaceCitationMisuse(finding.headline || ""));
}

export function validateExecutiveFindingCopy(finding) {
  const headline = String(finding?.governedHeadline || "").trim();
  const body = String(finding?.governedBody || "").trim();
  const evidence = String(finding?.governedEvidence || "").trim();
  const lower = `${headline} ${body} ${evidence}`.toLowerCase();
  const construct = finding?.evidenceConstruct;
  const noCitationMisuse =
    construct === EVIDENCE_CONSTRUCTS.SOURCE ||
    (!/\bbrand\b.*\bcited\b/.test(lower) && !/\bcited\b.*\bbrand\b/.test(lower));
  const recurrenceSafe =
    !(Number(finding?.observationCount || 0) <= 1 && /\brepeated|recurring|consistent|consistently\b/i.test(lower));
  const denominatorSafe =
    !/\b\d+\s*of\s*\d+\b/i.test(evidence) || /\bcomparable|observed runs|prompt cohort|responses\b/i.test(evidence);
  const noCausalLanguage =
    construct !== EVIDENCE_CONSTRUCTS.SOURCE ||
    !/\bcaus|influenc|drives|learned from|trusts\b/i.test(lower);
  const noInternalEnums = !/\bACTION_REQUIRED|REVIEW_REQUIRED|MONITOR_ONLY|NO_ACTION_EXPECTED_POSITIONING\b/.test(
    `${headline} ${body} ${evidence}`
  );

  return {
    HEADLINE_PRESENT: Boolean(headline),
    BODY_PRESENT: Boolean(body),
    EVIDENCE_PRESENT: Boolean(evidence),
    NO_CITATION_MISUSE: noCitationMisuse,
    DENOMINATOR_SAFE: denominatorSafe,
    RECURRENCE_LANGUAGE_SAFE: recurrenceSafe,
    QUALIFIER_SUPPORTED: recurrenceSafe,
    EVIDENCE_CONSTRUCT_COHERENT: Boolean(construct),
    NO_CAUSAL_SOURCE_LANGUAGE: noCausalLanguage,
    NO_NUMERIC_CONFIDENCE: !/\bconfidence\b|\breliability\b/i.test(lower),
    NO_FILLER: body.split(" ").length <= 55,
    NO_INTERNAL_ENUM_COPY: noInternalEnums,
  };
}

export function applyExecutiveCopyGovernance(findings = []) {
  const governed = findings.map((finding) => {
    const construct = classifyEvidenceConstruct(finding);
    const governedHeadline = replaceCitationMisuse(
      normalizeSpace(formatHeadline(finding, construct)),
      construct === EVIDENCE_CONSTRUCTS.SOURCE
    );
    const governedBody = replaceCitationMisuse(formatBody(finding, construct), construct === EVIDENCE_CONSTRUCTS.SOURCE);
    const governedEvidence = replaceCitationMisuse(
      normalizeSpace(formatEvidenceLine(finding, construct)),
      construct === EVIDENCE_CONSTRUCTS.SOURCE
    );
    const checks = validateExecutiveFindingCopy({
      ...finding,
      evidenceConstruct: construct,
      governedHeadline,
      governedBody,
      governedEvidence,
    });
    return {
      ...finding,
      evidenceConstruct: construct,
      governedHeadline,
      governedBody,
      governedEvidence,
      copyValidation: checks,
      semanticValidationPass: Object.values(checks).every(Boolean),
    };
  });

  return {
    version: EXECUTIVE_COPY_GOVERNANCE_VERSION,
    findings: governed,
    checks: governed.map((f) => ({
      dedupeKey: f.dedupeKey,
      findingType: f.findingType,
      checks: f.copyValidation,
      semanticValidationPass: f.semanticValidationPass,
    })),
  };
}
