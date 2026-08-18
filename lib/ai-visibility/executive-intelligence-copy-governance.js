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

export const EXECUTIVE_COPY_GOVERNANCE_VERSION = "ai_visibility_exec_copy_governance_v3";

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

function formatPeerList(names = []) {
  const list = (Array.isArray(names) ? names : []).filter(Boolean);
  if (!list.length) return "competing brands";
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} and ${list[1]}`;
  const rest = list.length - 2;
  return `${list[0]}, ${list[1]} and ${rest} other${rest === 1 ? "" : "s"}`;
}

function possessive(brand) {
  const s = String(brand || "").trim();
  if (!s) return "the brand's";
  return /s$/i.test(s) ? `${s}'` : `${s}'s`;
}

function formatPersistenceLabel(raw) {
  if (!raw) return null;
  return String(raw)
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

const COMMERCIAL_IMPLICATION_MARKERS = [
  "shortlist",
  "consideration",
  "visibility",
  "owner",
  "platform",
  "provider",
  "brand fit",
  "positioning",
  "monitoring",
  "portfolio",
  "interpret",
  "inclusion",
  "ecosystem",
  "affiliation",
  "conversion",
  "corrective",
  "signal",
  "architecture",
];

function tokenOverlapRatio(a, b) {
  const aTokens = new Set(
    String(a || "")
      .toLowerCase()
      .split(/\s+/)
      .filter(Boolean)
  );
  const bTokens = String(b || "")
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
  if (!aTokens.size || !bTokens.length) return 0;
  const overlap = bTokens.filter((t) => aTokens.has(t)).length;
  return overlap / bTokens.length;
}

function bodyNotHeadlineRestatement(headline, body) {
  const h = normalizeSpace(headline).toLowerCase();
  const b = normalizeSpace(body).toLowerCase();
  if (!h || !b) return false;
  if (b === h || b.startsWith(h) || h.startsWith(b)) return false;
  return tokenOverlapRatio(h, b) < 0.62;
}

function bodyAddsCommercialInterpretation(body) {
  const text = normalizeSpace(body);
  if (!text) return false;
  const lower = text.toLowerCase();
  const sentenceCount = text.split(/(?<=[.!?])\s+/).filter(Boolean).length;
  const hasMarker = COMMERCIAL_IMPLICATION_MARKERS.some((m) => lower.includes(m));
  return sentenceCount >= 2 && hasMarker;
}

function includesKeyMetric(text) {
  const s = normalizeSpace(text);
  if (!s) return false;
  return (
    /\b\d+(?:\.\d+)?%/.test(s) ||
    /\b\d+\b/.test(s) ||
    /\b(one|two|three|four|five|six|seven|eight|nine|ten)\b/i.test(s)
  );
}

function estimateWhiteLineTarget(body) {
  const words = normalizeSpace(body).split(" ").filter(Boolean).length;
  return words >= 30 && words <= 65;
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
      formatPersistenceLabel(finding.persistence) ||
      recurrencePhraseFromSupport(finding.observationSupport) ||
      recurrenceLabelFromObservationCount(Number(finding.observationCount || 0));
    const scenario = String(finding.scenarioName || "").toLowerCase();
    const recurrenceLabel = scenario.includes("independent") && scenario.includes("conversion")
      ? "Repeated across monitored conversion prompts"
      : recurrence;
    const parts = [
      recurrenceLabel,
      formatProviderCoverage(finding.providerCount),
      formatObservationEvidence(finding.observationCount),
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
      return `${brand} is frequently absent from independent conversion discussions where competing brands appear`;
    }
    if (scenario.includes("collection") || scenario.includes("affiliation")) {
      return `${brand} is frequently absent from collection-affiliation discussions where ${peer} appears`;
    }
    return `${brand} appears less often than ${peer} in monitored owner-decision discussions`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.PROVIDER_COMPARISON) {
    const cohort = finding.context || finding.geographyKey || "the selected cohort";
    return `Portfolio visibility is materially higher on ${finding.providerStrongLabel || "one provider"} than ${finding.providerWeakLabel || "another provider"} in ${cohort}`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.TRUTH) {
    return `${brand} shows an early potential brand-architecture perception gap`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.ASSOCIATION) {
    const attr = String(finding.associationAttributeId || "").toLowerCase();
    if (attr.includes("distribution")) {
      return `${brand} is strongly associated with distribution in qualifying owner-decision responses`;
    }
    return `${brand} is strongly associated with ${String(finding.associationAttributeId || "a validated attribute")
      .replace(/_/g, " ")
      .toLowerCase()} in qualifying owner-decision responses`;
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
  const peer = formatPeerList(finding.peerBrandNames || [finding.leadPeerName].filter(Boolean));
  const scenarioLower = String(scenario).toLowerCase();
  const providerCount = Number(finding.providerCount || 0);
  const observationCount = Number(finding.observationCount || 0);

  if (construct === EVIDENCE_CONSTRUCTS.GAP) {
    if (scenarioLower.includes("independent") && scenarioLower.includes("conversion")) {
      return `Across ${observationCount || "monitored"} monitored independent-conversion observations spanning ${providerCount || "multiple"} AI providers, ${brand} is frequently absent while ${peer} appear more consistently. That reduces ${brand}'s visibility when owners are forming an initial conversion shortlist.`;
    }
    if (scenarioLower.includes("collection") || scenarioLower.includes("affiliation")) {
      return `Across ${observationCount || "monitored"} collection-affiliation observations spanning ${providerCount || "multiple"} AI providers, ${brand} is frequently absent while ${peer} appear more consistently. That weaker visibility may reduce ${possessive(brand)} inclusion in early owner affiliation consideration.`;
    }
    return `Across ${observationCount || "monitored"} observations in ${scenario}, ${brand} appears less consistently while ${peer} appear more often. That limits visibility in owner conversations where shortlist inclusion drives consideration.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.PROVIDER_COMPARISON) {
    const strong = Number.parseFloat(String(finding.providerStrongPct || "").replace("%", ""));
    const weak = Number.parseFloat(String(finding.providerWeakPct || "").replace("%", ""));
    const spread = Number.isFinite(strong) && Number.isFinite(weak) ? `${Math.abs(strong - weak).toFixed(1)}` : null;
    return `Across the same comparable CALA prompt cohort, portfolio Presence reaches ${finding.providerStrongPct || "higher levels"} on ${finding.providerStrongLabel || "one provider"} versus ${finding.providerWeakPct || "lower levels"} on ${finding.providerWeakLabel || "another provider"}.${spread ? ` The ${spread}-point difference` : " This difference"} shows that strong visibility on one AI platform does not necessarily translate to equivalent visibility across providers.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.TRUTH) {
    const cls = finding.governedClassification || null;
    return `One observed AI response represents ${brand} in soft-brand-style context even though Dealality's governed architecture classifies ${brand} as ${cls || "a different class"}. The signal is early, but the mismatch merits review because brand architecture can influence how owners interpret strategic fit.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.ASSOCIATION) {
    const attr = String(finding.associationAttributeId || "this attribute")
      .replace(/_/g, " ")
      .toLowerCase();
    if (attr.includes("distribution")) {
      return `Across ${observationCount || "qualifying"} qualifying association observations spanning ${providerCount || "multiple"} AI providers, ${brand} is repeatedly associated with distribution in owner-decision responses. This is a positive positioning signal that reinforces representation and is more appropriate for monitoring than corrective action.`;
    }
    return `${brand} is repeatedly represented with ${attr} in qualifying monitored responses. The pattern reinforces how owners are likely to see this brand theme in AI discussions.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.NARRATIVE) {
    return `${brand} is repeatedly represented with this narrative across monitored owner-decision contexts. The recurrence suggests a stable positioning pattern rather than a one-off mention.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.SOURCE) {
    return `Source citation patterns around this finding are concentrated in recurring domains. This helps prioritize source review without implying causal influence on model behavior.`;
  }
  return normalizeSpace(replaceCitationMisuse(finding.headline || ""));
}

function formatDetailBody(finding, construct, body) {
  const base = normalizeSpace(body);
  if (!base) return base;
  const context = finding.context ? normalizeSpace(finding.context) : null;
  const commercial = finding.commercialMeaning ? normalizeSpace(finding.commercialMeaning) : null;

  if (construct === EVIDENCE_CONSTRUCTS.GAP && context) {
    return `${base} Context: ${context}.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.ASSOCIATION && commercial && !base.includes(commercial)) {
    return `${base} ${commercial}`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.TRUTH && finding.scenarioName) {
    return `${base} Observed in ${finding.scenarioName} owner-decision prompts.`;
  }
  if (construct === EVIDENCE_CONSTRUCTS.PROVIDER_COMPARISON && context) {
    return `${base} Cohort: ${context}.`;
  }
  return base;
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
  const bodyNotRestatement = bodyNotHeadlineRestatement(headline, body);
  const bodyCommercial = bodyAddsCommercialInterpretation(body);
  const category = String(finding?.title || "").trim().toLowerCase();
  const bodyLower = body.toLowerCase();
  const categoryNotRepeated = !category || !bodyLower.startsWith(category);
  const noDuplicateWhiteHeader = bodyLower !== headline.toLowerCase() && (!headline || !bodyLower.startsWith(headline.toLowerCase()));
  const selfContained = body.split(/(?<=[.!?])\s+/).filter(Boolean).length >= 2;
  const keyMetricIncluded = includesKeyMetric(body);
  const lineTarget = estimateWhiteLineTarget(body);
  const evidenceSecondary = body.length > evidence.length;

  return {
    HEADLINE_PRESENT: Boolean(headline),
    BODY_PRESENT: Boolean(body),
    BODY_NOT_HEADLINE_RESTATEMENT: bodyNotRestatement,
    BODY_ADDS_COMMERCIAL_INTERPRETATION: bodyCommercial,
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
    CATEGORY_NOT_REPEATED_IN_WHITE_COPY: categoryNotRepeated,
    NO_DUPLICATE_WHITE_HEADER: noDuplicateWhiteHeader,
    EXECUTIVE_FINDING_SELF_CONTAINED: selfContained,
    EXECUTIVE_FINDING_INCLUDES_KEY_METRIC: keyMetricIncluded,
    EXECUTIVE_FINDING_INCLUDES_COMMERCIAL_MEANING: bodyCommercial,
    WHITE_COPY_4_TO_5_LINE_TARGET: lineTarget,
    EVIDENCE_SECONDARY_NOT_PRIMARY: evidenceSecondary,
    NO_HEADLINE_BODY_DUPLICATION: bodyNotRestatement,
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
    const governedExecutiveFindingText = governedBody;
    const governedDetailBody = replaceCitationMisuse(
      formatDetailBody(finding, construct, governedExecutiveFindingText),
      construct === EVIDENCE_CONSTRUCTS.SOURCE
    );
    const governedEvidence = replaceCitationMisuse(
      normalizeSpace(formatEvidenceLine(finding, construct)),
      construct === EVIDENCE_CONSTRUCTS.SOURCE
    );
    const checks = validateExecutiveFindingCopy({
      ...finding,
      evidenceConstruct: construct,
      governedHeadline,
      governedBody,
      governedExecutiveFindingText,
      governedDetailBody,
      governedEvidence,
    });
    return {
      ...finding,
      evidenceConstruct: construct,
      governedHeadline,
      governedBody,
      governedExecutiveFindingText,
      governedDetailBody,
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
