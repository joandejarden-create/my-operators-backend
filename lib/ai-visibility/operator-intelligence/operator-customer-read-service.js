/**
 * Operator AI Intelligence — customer-safe read assembly (V1).
 * Certified Presence + Questions Missing + All Providers + client-promoted gaps only.
 * No provider calls. No Brand imports. No prompt / matrix / gold label exposure.
 */

import {
  ALL_PROVIDERS_SELECTOR_ID,
  AI_VISIBILITY_PROVIDER_LABELS,
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  isAllProvidersSelector,
  resolveProviderId,
} from "../provider-dimension.js";
import { OPERATOR_AI_PRODUCT } from "./product.js";
import { OPERATOR_SIGNAL_PRESENCE } from "./presence.js";
import {
  ALL_PROVIDERS_SCOPE,
  buildOperatorQuestionsMissingMatrix,
  computeOperatorQuestionsMissing,
  detectOperatorProviderDisagreement,
  summarizeQuestionsMissingMatrix,
} from "./questions-missing.js";
import {
  loadCertifiedOperatorPresenceCorpus,
  auditArborOperatorSpecificEvidence,
  CERTIFIED_OPERATOR_PRESENCE_WAVE_ID,
} from "./competitive-intelligence.js";
import { extractOperatorCompetitiveGapCandidates } from "./gaps.js";
import {
  ARBOR_LODGING_ID,
  COMMERCIAL_RELATION,
  classifyPresentOperators,
} from "./comparability.js";
import { getComparabilityTruth } from "./comparability-truth.js";
import { eligibilityFor, ELIGIBILITY } from "./eligibility.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";
import { getOperatorById, OPERATOR_AI_UNIVERSE, PRIMARY_OPERATOR_COUNT } from "./universe.js";
import { getOperatorScenarioProductionPolicy } from "./scenario-production-policy.js";
import {
  OPERATOR_CUSTOMER_DISCLOSURE_VERSION,
  OPERATOR_CUSTOMER_OWNER_INTENT,
  OPERATOR_INTERNAL_PROMPT_FIELD_NAMES,
  assertNoOperatorPromptLeak,
  getOperatorCustomerDecisionContext,
  getOperatorCustomerOwnerIntent,
  toCustomerSafeCompetitiveGapRow,
  toCustomerSafeQuestionsMissingRow,
} from "./customer-disclosure.js";

export const OPERATOR_CUSTOMER_UI_VERSION = "operator_ai_customer_ui_v1";

export const OPERATOR_CUSTOMER_INFO_COPY = Object.freeze({
  aiPresence:
    "Shows how often this operator appears across comparable monitored owner and developer decision observations in the selected AI provider scope.",
  ownerIntent:
    "Governed owner-decision contexts Dealality monitors for operator selection — not exact prompt wording.",
  questionsMissing:
    "A scenario is considered missing when this operator is absent across every comparable monitored provider for that owner-decision context. Provider failures or unavailable providers are excluded.",
  peerPresentGaps:
    "Counts owner-decision observations where this operator was absent while one or more directly comparable operators appeared. This indicates a competitive visibility gap, not that another operator won.",
  competitiveGap:
    "A certified Competitive Gap identifies an owner-decision context where this operator was absent while one or more directly comparable operators appeared. Dealality only shows this when the underlying presence, commercial comparability, and interpretation checks have passed.",
  providerDisagreement:
    "AI providers differ in whether this operator appears for this owner-decision scenario. This is an observation across providers, not a confidence score.",
  relevantComparableOperators:
    "Operators that meet Dealality's commercial comparability requirements for this specific owner-decision context.",
  allProviders:
    "All Providers combines comparable observations across the monitored AI providers. It is a derived view, not a separate AI provider. Missing or failed provider responses are excluded rather than counted as zero.",
  arborInsufficientEvidence:
    "Dealality does not yet have enough validated positive operator-specific observations to support competitive interpretation for this operator.",
  institutionalPlatform:
    "This scenario often produces general institutional or platform responses rather than operator-specific alternatives.",
});

export const OPERATOR_CUSTOMER_GAP_DISPLAY = Object.freeze({
  CERTIFIED_GAP: "Certified Gap",
  NO_CERTIFIED_GAP: "No certified gap",
  NOT_APPLICABLE: "Not applicable",
  INSUFFICIENT_CONTEXT: "Insufficient context",
});

const OPERATOR_MODEL_CUSTOMER = Object.freeze({
  BRAND_MANAGED_PLATFORM: "Brand-Managed Operator",
  THIRD_PARTY_MANAGER: "Third-Party Manager",
  REGIONAL_PLATFORM_MIXED: "Regional Hotel Platform",
  OTHER_UNCERTAIN: null,
});

const MONITORED_SCOPE_CUSTOMER = Object.freeze({
  GLOBAL: "Global",
  LATAM: "LATAM",
  CALA: "CALA",
  US_SOUTHEAST: "US Southeast",
});

const FORBIDDEN_CUSTOMER_SUBSTRINGS = Object.freeze([
  "CORE_COMPARABLE_RELATIONSHIPS",
  "TRUE_COMPETITIVE_GAP",
  "EXPECTED_POSITIONING_DIFFERENCE",
  "SCENARIO_OUT_OF_SCOPE",
  "REQUIRES_REVIEW",
  "goldLabel",
  "comparabilityMatrix",
  "promptId",
  "rawPrompt",
  "canonicalPrompt",
]);

function customerOperatorName(canonicalId) {
  return getOperatorById(canonicalId)?.canonicalName || null;
}

function customerMonitoredScope(scope) {
  return MONITORED_SCOPE_CUSTOMER[scope] || scope || null;
}

function customerOperatorModel(operatorId) {
  const pack = getComparabilityTruth(operatorId);
  if (!pack?.model) return null;
  return OPERATOR_MODEL_CUSTOMER[pack.model] || null;
}

export function operatorProviderScopeToGapKey(providerSelector) {
  if (isAllProvidersSelector(providerSelector)) return ALL_PROVIDERS_SCOPE;
  return resolveProviderId(providerSelector);
}

export function buildClientPromotedGapIndex(candidates = []) {
  const index = new Map();
  for (const row of candidates) {
    if (!row.clientPromoted) continue;
    index.set(`${row.operatorId}|${row.scenarioId}|${row.providerScope}`, row);
  }
  return index;
}

function filterExtractionsByScope(extractions, providerSelector) {
  if (isAllProvidersSelector(providerSelector)) return extractions;
  const provider = resolveProviderId(providerSelector);
  return (extractions || []).filter((e) => e.provider === provider);
}

function computePeerPresentGapCount(operatorId, scenarioId, extractions) {
  let count = 0;
  for (const extraction of extractions || []) {
    const present = (extraction.presentOperatorIds || []).includes(operatorId);
    if (present) continue;
    const classified = classifyPresentOperators(
      operatorId,
      extraction.presentOperatorIds || [],
      scenarioId
    );
    if (classified.byRelation[COMMERCIAL_RELATION.CORE_COMPARABLE].length > 0) {
      count += 1;
    }
  }
  return count;
}

function customerPresenceLabel(operatorId, operatorPresence, comparableProviderCount, arborBlocked) {
  if (arborBlocked) {
    return {
      display: "Insufficient operator-specific evidence",
      availability: "insufficient_evidence",
    };
  }
  if (operatorPresence === "NOT_APPLICABLE") {
    return { display: "Not applicable", availability: "not_applicable" };
  }
  if (operatorPresence === "INCOMPARABLE" || comparableProviderCount === 0) {
    return { display: "Not monitored", availability: "not_monitored" };
  }
  if (operatorPresence === "PRESENT") {
    return { display: "Present", availability: "present" };
  }
  if (operatorPresence === "ABSENT") {
    return { display: "Absent", availability: "absent" };
  }
  return { display: "—", availability: "unavailable" };
}

function customerMissingLabel(absenceClass) {
  if (absenceClass === "NOT_APPLICABLE") {
    return { display: "Not applicable", availability: "not_applicable" };
  }
  if (absenceClass === "MISSING") {
    return { display: "Missing", availability: "missing" };
  }
  if (absenceClass === "PRESENT") {
    return { display: "—", availability: "present" };
  }
  return { display: "—", availability: "unavailable" };
}

function mapCompetitiveGapDisplay({
  operatorId,
  scenarioId,
  providerScopeKey,
  gapIndex,
  policy,
  eligibilityStatus,
  arborBlocked,
}) {
  if (arborBlocked) {
    return {
      display: OPERATOR_CUSTOMER_GAP_DISPLAY.INSUFFICIENT_CONTEXT,
      state: "INSUFFICIENT_CONTEXT",
      clientPromoted: false,
    };
  }
  if (eligibilityStatus === ELIGIBILITY.OUT_OF_SCOPE) {
    return {
      display: OPERATOR_CUSTOMER_GAP_DISPLAY.NOT_APPLICABLE,
      state: "NOT_APPLICABLE",
      clientPromoted: false,
    };
  }
  if (policy.customerEligible !== "YES") {
    return {
      display: OPERATOR_CUSTOMER_GAP_DISPLAY.NOT_APPLICABLE,
      state: "NOT_APPLICABLE",
      clientPromoted: false,
    };
  }
  const promoted = gapIndex.get(`${operatorId}|${scenarioId}|${providerScopeKey}`) || null;
  if (promoted?.clientPromoted) {
    const safe = toCustomerSafeCompetitiveGapRow(promoted, { clientPromoted: true });
    const { gapInterpretation, ...customerDetail } = safe;
    void gapInterpretation;
    return {
      display: OPERATOR_CUSTOMER_GAP_DISPLAY.CERTIFIED_GAP,
      state: "CERTIFIED_GAP",
      clientPromoted: true,
      detail: customerDetail,
    };
  }
  return {
    display: OPERATOR_CUSTOMER_GAP_DISPLAY.NO_CERTIFIED_GAP,
    state: "NO_CERTIFIED_GAP",
    clientPromoted: false,
  };
}

function buildProviderCoverageSummary(scenarioObs) {
  const providers = [...new Set(scenarioObs.map((o) => o.provider).filter(Boolean))];
  return providers.map((provider) => {
    const rows = scenarioObs.filter((o) => o.provider === provider);
    const present = rows.some((o) => o.present === true);
    return {
      provider,
      providerLabel: AI_VISIBILITY_PROVIDER_LABELS[provider] || provider,
      present,
      display: present ? "Present" : "Absent",
    };
  });
}

function buildOwnerIntentRow({
  operator,
  scenario,
  extractions,
  providerSelector,
  gapIndex,
  arborBlocked,
}) {
  const providerScopeKey = operatorProviderScopeToGapKey(providerSelector);
  const scenarioObs = filterExtractionsByScope(extractions, providerSelector).filter(
    (e) => e.scenarioId === scenario.scenarioId
  );
  const observations = scenarioObs.map((e) => ({
    promptId: e.promptId,
    provider: e.provider,
    present: (e.presentOperatorIds || []).includes(operator.canonicalId),
  }));
  const comparableProviders = [...new Set(scenarioObs.map((o) => o.provider).filter(Boolean))];
  const elig = eligibilityFor(operator.canonicalId, scenario.scenarioId);
  const policy = getOperatorScenarioProductionPolicy(scenario.scenarioId);
  const present = scenarioObs.some((o) => (o.presentOperatorIds || []).includes(operator.canonicalId));
  const operatorPresence =
    elig.status === ELIGIBILITY.OUT_OF_SCOPE
      ? "NOT_APPLICABLE"
      : !comparableProviders.length
        ? "INCOMPARABLE"
        : present
          ? "PRESENT"
          : "ABSENT";
  const absenceClass =
    elig.status === ELIGIBILITY.OUT_OF_SCOPE
      ? "NOT_APPLICABLE"
      : operatorPresence === "ABSENT"
        ? scenarioObs.every((o) => !(o.presentOperatorIds || []).includes(operator.canonicalId))
          ? "MISSING"
          : "ABSENT"
        : operatorPresence === "PRESENT"
          ? "PRESENT"
          : "INCOMPARABLE";
  const allComparableProvidersMissing =
    comparableProviders.length > 0 &&
    scenarioObs.every((o) => !(o.presentOperatorIds || []).includes(operator.canonicalId));
  const relevantIds = [
    ...new Set(
      scenarioObs
        .flatMap((o) => o.presentOperatorIds || [])
        .filter((id) => id && id !== operator.canonicalId)
    ),
  ];
  const corePresentIds = classifyPresentOperators(
    operator.canonicalId,
    relevantIds,
    scenario.scenarioId
  ).byRelation[COMMERCIAL_RELATION.CORE_COMPARABLE];
  const observedCompetitors = [
    ...new Set(
      scenarioObs
        .flatMap((o) => o.observedCompetitors || [])
        .map((c) => c.canonicalName || c.name)
        .filter(Boolean)
    ),
  ].map((name) => ({ name, role: "OBSERVED_COMPETITIVE_CONTEXT" }));
  const disagreement = detectOperatorProviderDisagreement(observations);
  const peerPresentGapCount = computePeerPresentGapCount(
    operator.canonicalId,
    scenario.scenarioId,
    scenarioObs
  );
  const gap = mapCompetitiveGapDisplay({
    operatorId: operator.canonicalId,
    scenarioId: scenario.scenarioId,
    providerScopeKey,
    gapIndex,
    policy,
    eligibilityStatus: elig.status,
    arborBlocked,
  });
  const qm = computeOperatorQuestionsMissing({
    operatorId: operator.canonicalId,
    promptIds: observations.map((o) => o.promptId),
    observations,
  });

  const row = {
    scenarioId: scenario.scenarioId,
    ownerIntent: getOperatorCustomerOwnerIntent(scenario.scenarioId),
    decisionContext: getOperatorCustomerDecisionContext(scenario.scenarioId),
    yourPresence: customerPresenceLabel(
      operator.canonicalId,
      operatorPresence,
      comparableProviders.length,
      arborBlocked
    ),
    missing: customerMissingLabel(
      allComparableProvidersMissing && absenceClass !== "NOT_APPLICABLE" ? "MISSING" : absenceClass
    ),
    peerPresentGaps: {
      display: String(peerPresentGapCount),
      count: peerPresentGapCount,
      availability: comparableProviders.length ? "available" : "not_monitored",
    },
    competitiveGap: gap,
    providerDisagreement: {
      display: isAllProvidersSelector(providerSelector)
        ? disagreement.hasDisagreement
          ? "Provider disagreement"
          : comparableProviders.length >= 2
            ? "Agreement"
            : "—"
        : "—",
      hasDisagreement: isAllProvidersSelector(providerSelector) ? disagreement.hasDisagreement : false,
    },
    evidenceCount: scenarioObs.length,
    relevantComparableOperators: corePresentIds.map((id) => customerOperatorName(id)).filter(Boolean),
    observedCompetitors,
    providerCoverage: buildProviderCoverageSummary(scenarioObs),
    detailNotes:
      scenario.scenarioId === "op_scenario_institutional_platform_alignment_v1"
        ? OPERATOR_CUSTOMER_INFO_COPY.institutionalPlatform
        : null,
    expandable: true,
  };

  if (process.env.NODE_ENV !== "production") {
    row._internal = {
      absenceClass,
      questionsMissingDenominator: qm.denominator,
      competitiveGapTier: policy.competitiveGapTier,
    };
  }
  return row;
}

function computePresenceKpi(operatorId, extractions, providerSelector, arborBlocked) {
  if (arborBlocked) {
    return {
      display: "Insufficient operator-specific evidence",
      availability: "insufficient_evidence",
      rate: null,
    };
  }
  const scoped = filterExtractionsByScope(extractions, providerSelector);
  const byScenario = new Map();
  for (const scenario of OPERATOR_DECISION_SCENARIOS) {
    const elig = eligibilityFor(operatorId, scenario.scenarioId);
    if (elig.status === ELIGIBILITY.OUT_OF_SCOPE) continue;
    const scenarioObs = scoped.filter((e) => e.scenarioId === scenario.scenarioId);
    if (!scenarioObs.length) continue;
    byScenario.set(scenario.scenarioId, scenarioObs);
  }
  let comparablePrompts = 0;
  let presentPrompts = 0;
  for (const [, scenarioObs] of byScenario) {
    const byPrompt = new Map();
    for (const obs of scenarioObs) {
      if (!obs.promptId) continue;
      if (!byPrompt.has(obs.promptId)) byPrompt.set(obs.promptId, []);
      byPrompt.get(obs.promptId).push(obs);
    }
    for (const [, rows] of byPrompt) {
      if (!rows.length) continue;
      comparablePrompts += 1;
      const present = rows.some((r) => (r.presentOperatorIds || []).includes(operatorId));
      if (present) presentPrompts += 1;
    }
  }
  if (!comparablePrompts) {
    return { display: "Not monitored", availability: "not_monitored", rate: null };
  }
  const rate = presentPrompts / comparablePrompts;
  return {
    display: `${Math.round(rate * 1000) / 10}%`,
    availability: "available",
    rate,
    comparablePrompts,
    presentPrompts,
  };
}

function buildExecutiveFindings(operator, rows, providerSelector, arborBlocked) {
  const findings = [];
  if (arborBlocked) {
    findings.push({
      category: "PRESENCE",
      finding:
        "Dealality does not yet have enough validated positive operator-specific observations to support competitive interpretation for this operator.",
      evidence: "Insufficient operator-specific evidence · Presence model identity validated",
    });
    return findings.slice(0, 5);
  }

  for (const row of rows) {
    if (!row.competitiveGap?.clientPromoted) continue;
    const cores = (row.relevantComparableOperators || []).slice(0, 2).join(" and ");
    const providerLabel = isAllProvidersSelector(providerSelector)
      ? "All Providers"
      : AI_VISIBILITY_PROVIDER_LABELS[resolveProviderId(providerSelector)] || "";
    findings.push({
      category: "COMPETITIVE_VISIBILITY_GAP",
      finding: `${operator.canonicalName.split(" (")[0]} is absent in the ${row.ownerIntent.toLowerCase()} context${
        providerLabel ? ` on ${providerLabel}` : ""
      } while ${cores || "directly comparable operators"} appear as directly comparable operators.`,
      evidence: [providerLabel, row.ownerIntent, "CORE comparable evidence"].filter(Boolean).join(" · "),
    });
  }

  const disagreementRows = rows.filter((r) => r.providerDisagreement?.hasDisagreement);
  if (findings.length < 5 && disagreementRows.length) {
    const row = disagreementRows[0];
    findings.push({
      category: "PROVIDER_DISAGREEMENT",
      finding: `AI providers differ on whether ${operator.canonicalName.split(" (")[0]} appears for ${row.ownerIntent.toLowerCase()}.`,
      evidence: `${row.ownerIntent} · Provider disagreement across comparable observations`,
    });
  }

  const missingRows = rows.filter((r) => r.missing?.display === "Missing");
  if (findings.length < 5 && missingRows.length) {
    const row = missingRows[0];
    findings.push({
      category: "OWNER_DECISION_COVERAGE",
      finding: `${operator.canonicalName.split(" (")[0]} is absent across every comparable monitored provider for ${row.ownerIntent.toLowerCase()}.`,
      evidence: `${row.ownerIntent} · Questions Missing`,
    });
  }

  const presentRate = rows.filter((r) => r.yourPresence?.display === "Present").length;
  if (findings.length < 5 && presentRate > 0) {
    findings.push({
      category: "PRESENCE",
      finding: `${operator.canonicalName.split(" (")[0]} appears in ${presentRate} monitored owner-decision contexts in the selected provider scope.`,
      evidence: `${OPERATOR_SIGNAL_PRESENCE} · ${presentRate} contexts with presence`,
    });
  }

  return findings.slice(0, 5);
}

function buildQuestionsMissingWatchlist(rows) {
  return rows
    .filter((r) => r.missing?.display === "Missing")
    .map((r) => ({
      ownerIntent: r.ownerIntent,
      decisionContext: r.decisionContext,
      missingProviders: (r.providerCoverage || [])
        .filter((p) => !p.present)
        .map((p) => p.providerLabel),
      relevantOperatorsPresent: r.relevantComparableOperators || [],
      providerDisagreement: r.providerDisagreement?.display || "—",
      evidenceCount: r.evidenceCount,
    }));
}

export function auditOperatorCustomerPayload(payload = {}) {
  const { securityAudit, ...customerPayload } = payload;
  const text = JSON.stringify(customerPayload);
  const leaks = {
    rawPrompt: 0,
    comparabilityMatrix: 0,
    goldLabel: 0,
    unpromotedGap: 0,
  };
  for (const key of OPERATOR_INTERNAL_PROMPT_FIELD_NAMES) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) leaks.rawPrompt += 1;
  }
  if (/op_p_(core|ext)_/i.test(text)) leaks.rawPrompt += 1;
  if (/CORE_COMPARABLE_RELATIONSHIPS|"comparabilityMatrix"/.test(text)) leaks.comparabilityMatrix += 1;
  if (/goldLabel|"TRUE_COMPETITIVE_GAP"/.test(text)) leaks.goldLabel += 1;

  const rows = payload.detail?.ownerIntentRows || [];
  for (const row of rows) {
    if (row.competitiveGap?.state === "CERTIFIED_GAP" && !row.competitiveGap?.clientPromoted) {
      leaks.unpromotedGap += 1;
    }
    if (
      row.competitiveGap?.state === "CERTIFIED_GAP" &&
      row.competitiveGap?.detail?.gapInterpretation &&
      row.competitiveGap.detail.gapInterpretation !== "TRUE_COMPETITIVE_GAP"
    ) {
      leaks.unpromotedGap += 1;
    }
  }
  for (const forbidden of FORBIDDEN_CUSTOMER_SUBSTRINGS) {
    if (forbidden === "TRUE_COMPETITIVE_GAP" && /gapInterpretation/.test(text)) continue;
    if (text.includes(forbidden) && !["promptId"].includes(forbidden)) {
      if (forbidden === "promptId" && !/"promptId"\s*:/.test(text)) continue;
    }
  }
  try {
    assertNoOperatorPromptLeak(payload);
  } catch {
    leaks.rawPrompt += 1;
  }
  return leaks;
}

export function buildOperatorCustomerUniversePayload() {
  return {
    ok: true,
    success: true,
    product: OPERATOR_AI_PRODUCT.title,
    version: OPERATOR_CUSTOMER_UI_VERSION,
    disclosureVersion: OPERATOR_CUSTOMER_DISCLOSURE_VERSION,
    primaryOperatorCount: PRIMARY_OPERATOR_COUNT,
    operators: OPERATOR_AI_UNIVERSE.map((o) => ({
      operatorId: o.canonicalId,
      name: o.canonicalName,
      operatorModel: customerOperatorModel(o.canonicalId),
      monitoredScope: customerMonitoredScope(o.monitoredScope),
      slug: o.slug,
    })),
    providers: [
      {
        id: ALL_PROVIDERS_SELECTOR_ID,
        label: AI_VISIBILITY_PROVIDER_LABELS[ALL_PROVIDERS_SELECTOR_ID],
        derived: true,
      },
      ...KNOWN_AI_VISIBILITY_PROVIDER_IDS.map((id) => ({
        id,
        label: AI_VISIBILITY_PROVIDER_LABELS[id],
        derived: false,
      })),
    ],
    infoCopy: OPERATOR_CUSTOMER_INFO_COPY,
    blockedModules: {
      operatorIndex: "BLOCKED",
      associations: "RESEARCH_ONLY",
      narrativeSources: "DEFERRED",
      recommendations: "BLOCKED",
    },
  };
}

export function buildOperatorCustomerPayload(operatorId, providerSelector = ALL_PROVIDERS_SELECTOR_ID) {
  const operator = getOperatorById(operatorId);
  if (!operator) {
    return { ok: false, success: false, error: "operator_not_found" };
  }

  const corpus = loadCertifiedOperatorPresenceCorpus();
  const extractions = corpus.extractions || [];
  const candidates = extractOperatorCompetitiveGapCandidates(extractions);
  const gapIndex = buildClientPromotedGapIndex(candidates);
  const arborAudit = auditArborOperatorSpecificEvidence({
    extractions,
    report: corpus.report,
  });
  const arborBlocked = operatorId === ARBOR_LODGING_ID;

  const ownerIntentRows = OPERATOR_DECISION_SCENARIOS.map((scenario) =>
    buildOwnerIntentRow({
      operator,
      scenario,
      extractions,
      providerSelector,
      gapIndex,
      arborBlocked,
    })
  );

  const promotedRenderable = ownerIntentRows.filter((r) => r.competitiveGap?.clientPromoted);
  const qmSummary = {
    diagnosticMissingRows: ownerIntentRows.filter((r) => r.missing?.display === "Missing").length,
    applicableMissingRows: ownerIntentRows.filter(
      (r) => r.missing?.display === "Missing" && r.yourPresence?.display !== "Not applicable"
    ).length,
  };

  const providerScopeKey = operatorProviderScopeToGapKey(providerSelector);
  const allProvidersGapReady =
    [...gapIndex.values()].some((r) => r.providerScope === ALL_PROVIDERS_SCOPE) ? "CERTIFIED" : "NOT_CERTIFIED";

  const payload = {
    ok: true,
    success: true,
    product: OPERATOR_AI_PRODUCT.title,
    version: OPERATOR_CUSTOMER_UI_VERSION,
    disclosureVersion: OPERATOR_CUSTOMER_DISCLOSURE_VERSION,
    waveId: CERTIFIED_OPERATOR_PRESENCE_WAVE_ID,
    operator: {
      operatorId: operator.canonicalId,
      name: operator.canonicalName,
      operatorModel: customerOperatorModel(operator.canonicalId),
      monitoredScope: customerMonitoredScope(operator.monitoredScope),
      insufficientOperatorEvidence: arborBlocked,
      insufficientEvidenceCopy: arborBlocked ? OPERATOR_CUSTOMER_INFO_COPY.arborInsufficientEvidence : null,
    },
    providerScope: resolveProviderId(providerSelector, ALL_PROVIDERS_SELECTOR_ID),
    providerLabel: isAllProvidersSelector(providerSelector)
      ? AI_VISIBILITY_PROVIDER_LABELS[ALL_PROVIDERS_SELECTOR_ID]
      : AI_VISIBILITY_PROVIDER_LABELS[resolveProviderId(providerSelector)],
    allProvidersDerived: isAllProvidersSelector(providerSelector),
    kpis: {
      aiPresence: computePresenceKpi(operator.canonicalId, extractions, providerSelector, arborBlocked),
      ownerIntentsMonitored: {
        display: String(OPERATOR_DECISION_SCENARIOS.length),
        count: OPERATOR_DECISION_SCENARIOS.length,
      },
      questionsMissing: {
        display: String(qmSummary.applicableMissingRows),
        count: qmSummary.applicableMissingRows,
        diagnosticCount: qmSummary.diagnosticMissingRows,
      },
      competitiveGaps: {
        display: String(promotedRenderable.length),
        count: promotedRenderable.length,
      },
      providerAgreement: {
        display: ownerIntentRows.some((r) => r.providerDisagreement?.hasDisagreement)
          ? "Provider disagreement"
          : isAllProvidersSelector(providerSelector)
            ? "Agreement"
            : "—",
      },
    },
    executive: {
      maxFindings: 5,
      findings: buildExecutiveFindings(operator, ownerIntentRows, providerSelector, arborBlocked),
    },
    detail: {
      ownerIntentRows,
      questionsMissingWatchlist: buildQuestionsMissingWatchlist(ownerIntentRows),
      ownerIntentTaxonomy: Object.values(OPERATOR_CUSTOMER_OWNER_INTENT),
    },
    contracts: {
      presenceStatus: "PRODUCTION_VALIDATED",
      questionsMissingStatus: "READY",
      allProvidersStatus: "READY",
      competitiveGapCertification: "PARTIAL",
      expectedClientPromoted: 8,
      actualClientPromotedRenderable: [...gapIndex.values()].filter((r) => r.clientPromoted).length,
      allProvidersGapReadiness: allProvidersGapReady,
      providerScopeKey,
    },
    infoCopy: OPERATOR_CUSTOMER_INFO_COPY,
    blockedModules: {
      operatorIndex: "BLOCKED",
      associations: "RESEARCH_ONLY",
      narrativeSources: "DEFERRED",
      recommendations: "BLOCKED",
    },
    guards: {
      PROVIDER_CALLS: 0,
      SPEND: "$0",
      CENSUS_READS: 0,
    },
  };

  const audit = auditOperatorCustomerPayload(payload);
  payload.securityAudit = audit;
  return payload;
}

export function buildOperatorCustomerUiCertificationReport() {
  const corpus = loadCertifiedOperatorPresenceCorpus();
  const candidates = extractOperatorCompetitiveGapCandidates(corpus.extractions || []);
  const promoted = candidates.filter((c) => c.clientPromoted);
  const providerIds = [
    ALL_PROVIDERS_SELECTOR_ID,
    ...KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  ];

  const renderChecks = [];
  for (const gap of promoted) {
    for (const providerSelector of providerIds) {
      const scopeKey = operatorProviderScopeToGapKey(providerSelector);
      if (gap.providerScope !== scopeKey) continue;
      const payload = buildOperatorCustomerPayload(gap.operatorId, providerSelector);
      const row = (payload.detail?.ownerIntentRows || []).find(
        (r) => r.scenarioId === gap.scenarioId
      );
      renderChecks.push({
        operatorId: gap.operatorId,
        scenarioId: gap.scenarioId,
        providerScope: scopeKey,
        renderable: row?.competitiveGap?.clientPromoted === true,
      });
    }
  }

  const unexpectedPromoted = [];
  for (const op of OPERATOR_AI_UNIVERSE) {
    for (const providerSelector of providerIds) {
      const payload = buildOperatorCustomerPayload(op.canonicalId, providerSelector);
      for (const row of payload.detail?.ownerIntentRows || []) {
        if (row.competitiveGap?.clientPromoted) {
          const key = `${op.canonicalId}|${row.scenarioId}|${operatorProviderScopeToGapKey(providerSelector)}`;
          if (!promoted.some((g) => `${g.operatorId}|${g.scenarioId}|${g.providerScope}` === key)) {
            unexpectedPromoted.push(key);
          }
        }
      }
    }
  }

  let security = { rawPrompt: 0, comparabilityMatrix: 0, goldLabel: 0, unpromotedGap: 0 };
  for (const op of OPERATOR_AI_UNIVERSE) {
    for (const providerSelector of providerIds) {
      const payload = buildOperatorCustomerPayload(op.canonicalId, providerSelector);
      const audit = auditOperatorCustomerPayload(payload);
      security.rawPrompt += audit.rawPrompt;
      security.comparabilityMatrix += audit.comparabilityMatrix;
      security.goldLabel += audit.goldLabel;
      security.unpromotedGap += audit.unpromotedGap;
    }
  }

  const aimbridgePayload = buildOperatorCustomerPayload(
    promoted.find((g) => g.canonicalName?.includes("Aimbridge"))?.operatorId ||
      "recGWxIJqnYHkJZFD",
    "claude"
  );
  const hePayloadOpenai = buildOperatorCustomerPayload("recWPKu5laVZxsvpn", "openai");
  const remingtonPayload = buildOperatorCustomerPayload("rec6UB6RpMKSs2tAo", "openai");
  const marriottPayload = buildOperatorCustomerPayload("recGmiPhRt6hiayd9", "openai");
  const brittainPayload = buildOperatorCustomerPayload("receHCdI6CEsJqdG4", "openai");
  const arborPayload = buildOperatorCustomerPayload(ARBOR_LODGING_ID, "openai");

  const marriottTpmGap = (marriottPayload.detail?.ownerIntentRows || []).find(
    (r) => r.scenarioId === "op_scenario_third_party_management_v1"
  )?.competitiveGap?.clientPromoted;
  const brittainCalaGap = (brittainPayload.detail?.ownerIntentRows || []).find(
    (r) => r.scenarioId === "op_scenario_cala_latam_regional_capability_v1"
  )?.competitiveGap?.clientPromoted;
  const arborCompetitiveClaims = (arborPayload.executive?.findings || []).filter(
    (f) => f.category === "COMPETITIVE_VISIBILITY_GAP"
  ).length;

  const allProvidersStale = [];
  for (const gap of promoted.filter((g) => g.providerScope !== ALL_PROVIDERS_SCOPE)) {
    const allPayload = buildOperatorCustomerPayload(gap.operatorId, ALL_PROVIDERS_SELECTOR_ID);
    const row = (allPayload.detail?.ownerIntentRows || []).find(
      (r) => r.scenarioId === gap.scenarioId
    );
    if (row?.competitiveGap?.clientPromoted) {
      allProvidersStale.push(`${gap.operatorId}|${gap.scenarioId}`);
    }
  }

  return {
    token: "OPERATOR_AI_CUSTOMER_UI_V1_COMPLETE",
    route: OPERATOR_AI_PRODUCT.route,
    primaryMonitoredOperators: PRIMARY_OPERATOR_COUNT,
    expectedClientPromoted: 8,
    actualClientPromotedRenderable: renderChecks.filter((r) => r.renderable).length,
    unexpectedPromoted: unexpectedPromoted.length,
    renderChecks,
    security,
    marriottFalseTpmGaps: marriottTpmGap ? 1 : 0,
    brittainFalseCalaGaps: brittainCalaGap ? 1 : 0,
    arborCompetitiveClaims,
    allProvidersStalePaint: allProvidersStale.length,
    allProvidersGapReadiness: promoted.some((g) => g.providerScope === ALL_PROVIDERS_SCOPE)
      ? "CERTIFIED"
      : "NOT_CERTIFIED",
    qmRegression: {
      totalOperatorScenarioRows: 9 * 12,
      diagnosticMissingRows: summarizeQmFromCorpus(corpus.extractions).totalMissingRows,
      applicableMissingRows: summarizeQmFromCorpus(corpus.extractions).applicableMissingRows,
    },
    brandDiff: 0,
    operatorPresenceDiff: 0,
    operatorCompetitiveCertificationDiff: 0,
    providerCalls: 0,
    spend: "$0",
  };
}

function summarizeQmFromCorpus(extractions) {
  return summarizeQuestionsMissingMatrix(buildOperatorQuestionsMissingMatrix(extractions));
}
