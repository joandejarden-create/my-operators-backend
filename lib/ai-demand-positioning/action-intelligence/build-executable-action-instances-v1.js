/**
 * Build property-specific executable action instances from governed playbooks + ADP evidence.
 * Generic evidence-driven selection — no hotel-specific builder forks.
 * Rejects generic slogan recommendations.
 */

import { SOURCE_CATALOG } from "./action-executability-contract-v1.js";
import { getActionPattern } from "./action-pattern-library-v1.js";
import { createEmptyLearningRecord } from "./action-learning-model-v1.js";
import { validateExecutableActionInstance } from "./validate-executable-action-v1.js";
import {
  ACTION_CAP_BY_ARCHETYPE,
  REVIEW_POSTURE,
} from "../monthly-review/monthly-review-contract-v1.js";
import { resolveCanonicalRealityCoveragePct } from "../monthly-review/performance-review-canonical-binder-v1.js";

function fmtPct(n) {
  if (n == null || Number.isNaN(Number(n))) return "not published";
  return `${Number(n).toFixed(1)}%`;
}

function realityCoveragePct(report) {
  return resolveCanonicalRealityCoveragePct(report);
}

function pickSources(pattern, preferred = []) {
  const defaults = pattern.defaultSources || [];
  const selected = preferred.length
    ? preferred.filter((s) => defaults.includes(s) || true)
    : defaults.slice(0, 5);
  return [...new Set(selected)].slice(0, 6);
}

function relevantChecklist(pattern, focusLabels = []) {
  const all = [...(pattern.checklist || []), ...(pattern.detailChecklist || [])];
  if (!focusLabels.length) return all.slice(0, 10);
  const focused = all.filter((item) =>
    focusLabels.some((f) => item.toLowerCase().includes(String(f).toLowerCase()))
  );
  return (focused.length ? focused : all).slice(0, 12);
}

function buildDefinitionOfDone({ pattern, targetSources, focusLabels }) {
  return [
    `Canonical facts for “${pattern.categoryLabel}” are confirmed by hotel management.`,
    `Each target source is reviewed: ${targetSources.join("; ")}.`,
    `Sources that were incorrect or incomplete are corrected, with URLs or screenshots stored.`,
    focusLabels.length
      ? `The specific missing/inconsistent items are resolved: ${focusLabels.join("; ")}.`
      : `Checklist items applicable to this property are completed or marked not-applicable with rationale.`,
    "Accountable owner signs off with completion date.",
  ];
}

function buildImplementationSteps({
  pattern,
  focusLabels,
  targetSources,
  propertyName,
  competitorName,
}) {
  const checklist = relevantChecklist(pattern, focusLabels);
  const steps = [
    `Confirm the hotel’s canonical facts for ${pattern.categoryLabel} with the accountable owner (${propertyName}).`,
    `Complete the applicable checklist items: ${checklist.join("; ")}.`,
    `Audit each target source and record current wording/state: ${targetSources.join("; ")}.`,
    `Correct only the sources where facts are missing, conflicting, or outdated. Do not change measurement methodology.`,
  ];
  if (competitorName) {
    steps.push(
      `Compare public facts credited to ${competitorName} in displacement evidence against the subject hotel’s corrected pages; close only evidence-backed gaps.`
    );
  }
  steps.push(
    "Store completion evidence (canonical policy summary, sources reviewed, sources corrected, URLs/screenshots, completion date)."
  );
  steps.push(
    "Notify Dealality of completion so the next monitoring period can check the expected signal without assuming causation."
  );
  return steps;
}

function createInstance({
  propertyId,
  propertyName,
  monitoringPeriodId,
  actionPatternId,
  observedIssue,
  evidence,
  focusLabels = [],
  preferredSources = [],
  accountableOwnerRole,
  supportingTeam,
  competitorName = null,
  trace,
  metricBefore = null,
  index = 1,
}) {
  const pattern = getActionPattern(actionPatternId);
  if (!pattern) {
    return { rejected: true, reason: `unknown_pattern:${actionPatternId}` };
  }

  const targetSources = pickSources(pattern, preferredSources);
  const implementationSteps = buildImplementationSteps({
    pattern,
    focusLabels,
    targetSources,
    propertyName,
    competitorName,
  });
  const definitionOfDone = buildDefinitionOfDone({
    pattern,
    targetSources,
    focusLabels,
  });

  const actionTitle = focusLabels.length
    ? `${pattern.titleTemplate} — focus: ${focusLabels.slice(0, 3).join(", ")}`
    : pattern.titleTemplate;

  const actionId = `${propertyId}__${monitoringPeriodId || "period"}__playbook_a${index}`;
  const learningRecord = createEmptyLearningRecord({
    actionInstanceId: actionId,
    actionPatternId,
    propertyId,
    hotelAttributes: { propertyName },
    issuePattern: trace?.sourceGap || observedIssue.slice(0, 160),
    implementationStepsUsed: implementationSteps,
    metricBefore,
  });

  const action = {
    actionId,
    propertyId,
    monitoringPeriodId,
    actionPatternId,
    actionTitle,
    observedIssue,
    evidence,
    recommendedAction: implementationSteps.join(" "),
    targetSources,
    implementationSteps,
    accountableOwnerRole,
    supportingTeam,
    targetDate: null,
    definitionOfDone,
    expectedSignal: `Dealality will monitor whether ${pattern.categoryLabel.toLowerCase()} recognition and related scenario inclusion change in the next monitoring period. Monitoring alone does not establish causation.`,
    nextMonitoringCheck: `Next published ADP run — review ${pattern.categoryLabel} attribute recognition, related demand-territory presence, and evidence excerpts for the corrected facts.`,
    status: "Open",
    trace: {
      observedMetric: trace?.observedMetric || null,
      scenarioOrTerritory: trace?.scenarioOrTerritory || null,
      provider: trace?.provider || null,
      evidenceExample: trace?.evidenceExample || null,
      sourceGap: trace?.sourceGap || null,
      actionPatternId,
    },
    learningRecord,
    rationale:
      "This action addresses an observed information/positioning gap in the current monitoring period. It does not claim a guaranteed metric uplift.",
  };

  const validation = validateExecutableActionInstance(action);
  if (!validation.ok) {
    return { rejected: true, reason: validation.failures.join(","), action, validation };
  }
  return { rejected: false, action, validation };
}

/** Attribute / label → pattern + focus defaults (data-driven, not hotel-specific). */
const REALITY_GAP_PATTERN_RULES = Object.freeze([
  {
    test: (g) =>
      g.attribute === "all_inclusive" ||
      g.attribute === "all_inclusive_option" ||
      /all-inclusive/i.test(g.label || ""),
    actionPatternId: "ACTION_ALL_INCLUSIVE_OFFER_RECONCILIATION",
    focusLabels: [
      "whether an all-inclusive option exists",
      "what is included",
      "what is excluded",
      "how the option is labeled on booking paths",
    ],
    owner: "Director of Marketing / Revenue Marketing",
    team: "Content + Distribution / eCommerce",
    sourceGap: "all_inclusive_underrepresented",
  },
  {
    test: (g) => g.attribute === "pet_friendly" || /pet/i.test(g.label || ""),
    actionPatternId: "ACTION_PET_POLICY_RECONCILIATION",
    focusLabels: [
      "pets accepted? yes/no",
      "dogs/cats/other",
      "weight limits",
      "per-stay / per-night fee",
      "deposit",
      "restricted room types",
      "service-animal policy",
    ],
    owner: "Director of Marketing / Revenue Marketing",
    team: "Content + Channel distribution",
    sourceGap: "pet_policy_missing_or_inconsistent",
  },
  {
    test: (g) => /parking/i.test(g.attribute || "") || /parking/i.test(g.label || ""),
    actionPatternId: "ACTION_PARKING_INFORMATION_RECONCILIATION",
    focusLabels: [
      "confirm whether parking is available",
      "self / valet / both",
      "daily / nightly fee",
      "reservation requirement",
    ],
    owner: "General Manager / Operations",
    team: "Front office + Marketing content",
    sourceGap: "parking_information_gap",
  },
  {
    test: (g) =>
      g.attribute === "meeting_space" || /meeting/i.test(g.label || ""),
    actionPatternId: "ACTION_MEETINGS_DECISION_PACKAGE_COMPLETION",
    focusLabels: [
      "total meeting space",
      "number of rooms",
      "largest capacity",
      "AV capabilities",
      "meeting sales contact",
      "downloadable floorplans",
    ],
    owner: "Commercial / Sales Lead",
    team: "Meetings sales + Marketing content",
    sourceGap: "meetings_decision_package_incomplete",
  },
  {
    test: (g) => /family/i.test(g.attribute || "") || /family/i.test(g.label || ""),
    actionPatternId: "ACTION_FAMILY_TRAVEL_INFORMATION_COMPLETION",
    focusLabels: ["family amenities", "connecting rooms", "kids policies"],
    owner: "Director of Marketing",
    team: "Content",
    sourceGap: "family_travel_information_gap",
  },
  {
    test: (g) =>
      /accessib/i.test(g.attribute || "") || /accessib/i.test(g.label || ""),
    actionPatternId: "ACTION_ACCESSIBILITY_INFORMATION_COMPLETION",
    focusLabels: ["accessible rooms", "public-space access", "booking path clarity"],
    owner: "General Manager / Guest Experience",
    team: "Operations + Marketing",
    sourceGap: "accessibility_information_gap",
  },
  {
    test: (g) =>
      /fee|resort_fee|mandatory/i.test(g.attribute || "") ||
      /fee|resort fee/i.test(g.label || ""),
    actionPatternId: "ACTION_MANDATORY_FEE_INFORMATION_RECONCILIATION",
    focusLabels: ["mandatory fees", "what is included", "disclosure on booking paths"],
    owner: "Revenue / Marketing Lead",
    team: "Distribution + Content",
    sourceGap: "mandatory_fee_disclosure_gap",
  },
]);

const BANNED_GENERIC_PREDECESSORS = Object.freeze([
  "Improve AI representation",
  "Strengthen overall AI authority signals",
  "Improve OTA content",
  "Pursue high-potential demand opportunities",
  "Strengthen local positioning",
  "Improve family content",
]);

function severityRank(severity) {
  const s = String(severity || "").toUpperCase();
  if (s === "HIGH" || s === "CRITICAL") return 3;
  if (s === "MEDIUM") return 2;
  if (s === "LOW") return 1;
  return 0;
}

function isCorrectiveArchetype(archetype) {
  return (
    archetype === REVIEW_POSTURE.CORRECTIVE ||
    archetype === REVIEW_POSTURE.STABLE_LOW ||
    archetype === REVIEW_POSTURE.MIXED
  );
}

function isStrongArchetype(archetype) {
  return (
    archetype === REVIEW_POSTURE.STRONG_PERFORMER ||
    archetype === REVIEW_POSTURE.STABLE_STRONG
  );
}

/**
 * Generic evidence-driven action selection for any eligible ADP property.
 */
export function buildGenericExecutableActions(report, evidence, ctx = {}) {
  const propertyId = report.property?.propertyId;
  const propertyName = report.property?.name || propertyId || "Property";
  const monitoringPeriodId = ctx.monitoringPeriodId;
  const archetype = ctx.archetype || REVIEW_POSTURE.MIXED;
  const cap = ACTION_CAP_BY_ARCHETYPE[archetype] ?? 4;

  const candidates = [];
  const rejected = BANNED_GENERIC_PREDECESSORS.map((t) => ({
    genericText: t,
    reason: "banned_generic_predecessor",
  }));
  const usedPatterns = new Set();

  const gaps = [...(report.realityGap?.gaps || [])].sort(
    (a, b) =>
      severityRank(b.severity) - severityRank(a.severity) ||
      (Number(a.recognitionRate) || 100) - (Number(b.recognitionRate) || 100)
  );

  for (const gap of gaps) {
    if (candidates.length >= cap) break;
    const rule = REALITY_GAP_PATTERN_RULES.find((r) => r.test(gap));
    if (!rule || usedPatterns.has(rule.actionPatternId)) continue;
    // Strong performers: only high-severity concentrated gaps
    if (isStrongArchetype(archetype) && severityRank(gap.severity) < 3) continue;
    usedPatterns.add(rule.actionPatternId);
    candidates.push(
      createInstance({
        propertyId,
        propertyName,
        monitoringPeriodId,
        actionPatternId: rule.actionPatternId,
        observedIssue: `“${gap.label}” was recognized in only ${fmtPct(gap.recognitionRate)} of applicable monitored answers (${gap.count} of ${gap.total}; severity ${gap.severity}).`,
        evidence: `Reality-gap monitoring for attribute ${gap.attribute}.`,
        focusLabels: rule.focusLabels,
        preferredSources: [
          SOURCE_CATALOG.HOTEL_WEBSITE,
          SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
          SOURCE_CATALOG.BOOKING_COM,
          SOURCE_CATALOG.EXPEDIA,
          SOURCE_CATALOG.TRIPADVISOR,
        ],
        accountableOwnerRole: rule.owner,
        supportingTeam: rule.team,
        trace: {
          observedMetric: `realityGap.${gap.attribute}=${gap.recognitionRate}%`,
          scenarioOrTerritory: "Property reality attributes",
          provider: "All providers (attribute recognition)",
          evidenceExample: `${gap.label} recognition ${fmtPct(gap.recognitionRate)}`,
          sourceGap: rule.sourceGap,
        },
        metricBefore: {
          realityCoverage: realityCoveragePct(report),
          attributeRecognitionRate: gap.recognitionRate,
        },
        index: candidates.length + 1,
      })
    );
  }

  // Meetings via white-space opportunity when no meeting reality gap fired
  if (!usedPatterns.has("ACTION_MEETINGS_DECISION_PACKAGE_COMPLETION") && candidates.length < cap) {
    const meetingOpp = (report.whiteSpace?.opportunities || []).find(
      (o) => o.intent === "business" || /meeting/i.test(o.ownerIntentSummary || "")
    );
    if (meetingOpp && !isStrongArchetype(archetype)) {
      usedPatterns.add("ACTION_MEETINGS_DECISION_PACKAGE_COMPLETION");
      candidates.push(
        createInstance({
          propertyId,
          propertyName,
          monitoringPeriodId,
          actionPatternId: "ACTION_MEETINGS_DECISION_PACKAGE_COMPLETION",
          observedIssue: `Business/meeting demand opportunity remains contested (“${meetingOpp.ownerIntentSummary}”) with weak competitor ownership in monitored answers.`,
          evidence: `whiteSpace.${meetingOpp.intent}`,
          focusLabels: [
            "total meeting space",
            "largest capacity",
            "meeting sales contact",
            "downloadable floorplans",
            "event fact sheet",
          ],
          preferredSources: [
            SOURCE_CATALOG.HOTEL_WEBSITE,
            SOURCE_CATALOG.MEETING_FACT_SHEET,
            SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
          ],
          accountableOwnerRole: "Commercial / Sales Lead",
          supportingTeam: "Meetings sales + Marketing content",
          trace: {
            observedMetric: "whiteSpace.business",
            scenarioOrTerritory: "Business Travel / Meetings",
            provider: "Cross-provider",
            evidenceExample: meetingOpp.rationale || meetingOpp.ownerIntentSummary,
            sourceGap: "meetings_decision_package_incomplete",
          },
          metricBefore: {
            scenarioPresence: report.executiveMetrics?.scenarioPresence?.rate,
          },
          index: candidates.length + 1,
        })
      );
    }
  }

  // Top displacement competitor (any name — no hotel-specific regex)
  const leadDisplacement = (report.lostDemand?.displacement || [])
    .filter((d) => d && d.name && Number(d.displacementCount) > 0)
    .sort((a, b) => Number(b.displacementCount) - Number(a.displacementCount))[0];
  if (
    leadDisplacement &&
    !usedPatterns.has("ACTION_COMPETITIVE_ABSENCE_SOURCE_RECONCILIATION") &&
    candidates.length < cap
  ) {
    // Strong: only if displacement is concentrated (≥2)
    if (!isStrongArchetype(archetype) || Number(leadDisplacement.displacementCount) >= 2) {
      usedPatterns.add("ACTION_COMPETITIVE_ABSENCE_SOURCE_RECONCILIATION");
      const entityKey =
        leadDisplacement.entityId ||
        String(leadDisplacement.name).toLowerCase().replace(/\s+/g, "_");
      candidates.push(
        createInstance({
          propertyId,
          propertyName,
          monitoringPeriodId,
          actionPatternId: "ACTION_COMPETITIVE_ABSENCE_SOURCE_RECONCILIATION",
          observedIssue: `${leadDisplacement.name} appeared in ${leadDisplacement.displacementCount} monitored scenarios where ${propertyName} was absent.`,
          evidence: `lostDemand.displacement leader ${leadDisplacement.name} (${leadDisplacement.displacementCount}).`,
          focusLabels: [
            "list traveler needs / scenarios where absence concentrates",
            "identify subject facts that are missing or inconsistent on priority sources",
            "update first-party pages covering those facts",
          ],
          preferredSources: [
            SOURCE_CATALOG.HOTEL_WEBSITE,
            SOURCE_CATALOG.TRIPADVISOR,
            SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
            SOURCE_CATALOG.BOOKING_COM,
          ],
          accountableOwnerRole: "General Manager + Marketing Lead",
          supportingTeam: "Marketing content + Distribution",
          competitorName: leadDisplacement.name,
          trace: {
            observedMetric: `displacement.${entityKey}=${leadDisplacement.displacementCount}`,
            scenarioOrTerritory: "Scenarios where subject is absent",
            provider: "Cross-provider",
            evidenceExample: `${leadDisplacement.name} displacement count ${leadDisplacement.displacementCount}`,
            sourceGap: `competitive_absence_vs_${entityKey}`,
          },
          metricBefore: {
            aiConsideration: report.executiveMetrics?.considerationRate?.rate,
            displacementCount: leadDisplacement.displacementCount,
          },
          index: candidates.length + 1,
        })
      );
    }
  }

  // TripAdvisor management program when citation share is material
  const taShare = (report.evidence?.topSources || []).find((s) =>
    /tripadvisor/i.test(s.domain || "")
  );
  if (
    taShare &&
    Number(taShare.frequency) >= 30 &&
    !usedPatterns.has("ACTION_TRIPADVISOR_MANAGEMENT_RESPONSE_PROGRAM") &&
    candidates.length < cap
  ) {
    usedPatterns.add("ACTION_TRIPADVISOR_MANAGEMENT_RESPONSE_PROGRAM");
    candidates.push(
      createInstance({
        propertyId,
        propertyName,
        monitoringPeriodId,
        actionPatternId: "ACTION_TRIPADVISOR_MANAGEMENT_RESPONSE_PROGRAM",
        observedIssue: `TripAdvisor appears in ${taShare.frequency}% of cited sources in the current monitoring period (${taShare.count} citations), making response quality and factual corrections operationally material when misconceptions appear.`,
        evidence: `report.evidence.topSources tripadvisor frequency ${taShare.frequency}%.`,
        focusLabels: [
          "designate accountable owner",
          "set response SLA",
          "identify recurring complaint themes monthly",
          "correct factual misconceptions where appropriate",
          "track response rate",
        ],
        preferredSources: [SOURCE_CATALOG.TRIPADVISOR],
        accountableOwnerRole: "Guest Experience / Reputation Owner",
        supportingTeam: "Front office + Marketing",
        trace: {
          observedMetric: `evidence.topSources.tripadvisor.frequency=${taShare.frequency}`,
          scenarioOrTerritory: "Citation / reputation sources",
          provider: "Cross-provider citations",
          evidenceExample: `tripadvisor.com citation frequency ${taShare.frequency}%`,
          sourceGap: "tripadvisor_management_response_program",
        },
        metricBefore: { tripadvisorCitationFrequency: taShare.frequency },
        index: candidates.length + 1,
      })
    );
  }

  // Corrective archetypes: room expectation + GBP only when under-inclusion is clear
  const consideration = report.executiveMetrics?.considerationRate?.rate;
  const scenarioPresence = report.executiveMetrics?.scenarioPresence?.rate;
  const highSeverityGaps = gaps.filter((g) => severityRank(g.severity) >= 3);

  if (
    isCorrectiveArchetype(archetype) &&
    consideration != null &&
    consideration < 45 &&
    highSeverityGaps.length >= 1 &&
    !usedPatterns.has("ACTION_ROOM_EXPECTATION_ALIGNMENT") &&
    candidates.length < cap
  ) {
    usedPatterns.add("ACTION_ROOM_EXPECTATION_ALIGNMENT");
    candidates.push(
      createInstance({
        propertyId,
        propertyName,
        monitoringPeriodId,
        actionPatternId: "ACTION_ROOM_EXPECTATION_ALIGNMENT",
        observedIssue: `AI Consideration is ${fmtPct(consideration)} with Reality Coverage ${fmtPct(
          realityCoveragePct(report)
        )}. High-severity reality gaps indicate stay-expectation descriptions may contribute to under-inclusion.`,
        evidence: `Corrective posture from Core under-inclusion plus high-severity reality gaps (${highSeverityGaps
          .slice(0, 3)
          .map((g) => g.attribute)
          .join(", ")}).`,
        focusLabels: [
          "identify exact recurring expectation mismatch",
          "update first-party room descriptions",
          "verify OTA room-type descriptions",
          "ensure imagery accurately reflects room type",
        ],
        preferredSources: [
          SOURCE_CATALOG.HOTEL_WEBSITE,
          SOURCE_CATALOG.OTA_ROOM_TYPES,
          SOURCE_CATALOG.BOOKING_COM,
          SOURCE_CATALOG.EXPEDIA,
        ],
        accountableOwnerRole: "Director of Marketing / Revenue Marketing",
        supportingTeam: "Content + Channel distribution + Revenue",
        trace: {
          observedMetric: `consideration=${consideration}`,
          scenarioOrTerritory: "Room / stay expectation",
          provider: "Cross-provider",
          evidenceExample: highSeverityGaps[0]?.label || "high-severity reality gaps",
          sourceGap: "room_expectation_mismatch",
        },
        metricBefore: {
          aiConsideration: consideration,
          realityCoverage: realityCoveragePct(report),
        },
        index: candidates.length + 1,
      })
    );
  }

  if (
    isCorrectiveArchetype(archetype) &&
    scenarioPresence != null &&
    scenarioPresence < 50 &&
    !usedPatterns.has("ACTION_GOOGLE_BUSINESS_PROFILE_COMPLETENESS_AUDIT") &&
    candidates.length < cap
  ) {
    usedPatterns.add("ACTION_GOOGLE_BUSINESS_PROFILE_COMPLETENESS_AUDIT");
    candidates.push(
      createInstance({
        propertyId,
        propertyName,
        monitoringPeriodId,
        actionPatternId: "ACTION_GOOGLE_BUSINESS_PROFILE_COMPLETENESS_AUDIT",
        observedIssue: `Broad under-inclusion (Scenario Presence ${fmtPct(
          scenarioPresence
        )}) requires verifying that Google Business Profile amenities, description, and booking link match the hotel’s canonical facts before other channels can be trusted as consistent.`,
        evidence: "Corrective Core under-inclusion + multi-attribute reality gaps.",
        focusLabels: [
          "amenities",
          "description",
          "booking link",
          "photos",
          "Q&A",
          "outdated user-added information",
        ],
        preferredSources: [
          SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
          SOURCE_CATALOG.HOTEL_WEBSITE,
        ],
        accountableOwnerRole: "Marketing / Local Presence Owner",
        supportingTeam: "Content",
        trace: {
          observedMetric: `scenarioPresence=${scenarioPresence}`,
          scenarioOrTerritory: "Local profile completeness",
          provider: "N/A (profile audit)",
          evidenceExample: "GBP completeness as foundation for local AI/source consistency",
          sourceGap: "gbp_incomplete_or_outdated",
        },
        metricBefore: { scenarioPresence },
        index: candidates.length + 1,
      })
    );
  }

  const accepted = [];
  for (const c of candidates) {
    if (c.rejected) rejected.push(c);
    else accepted.push(c.action);
  }

  // Monitor-only path: zero corrective actions is allowed for strong performers
  return {
    accepted: accepted.slice(0, cap),
    rejected,
    rejectedGenerics: BANNED_GENERIC_PREDECESSORS,
    archetype,
    actionCap: cap,
    builderMode: "generic_evidence_driven_v1",
  };
}

/**
 * Single dispatcher — no Cambridge/NOHO/Waterstone forks.
 */
export function buildExecutableActionsForProperty(propertyId, report, evidence, ctx = {}) {
  const pack = buildGenericExecutableActions(report, evidence, {
    ...ctx,
    propertyId,
  });
  return pack;
}

/**
 * @deprecated Use buildGenericExecutableActions — kept as thin alias for golden tests.
 */
export function buildCambridgeExecutableActions(report, evidence, ctx = {}) {
  return buildGenericExecutableActions(report, evidence, {
    ...ctx,
    archetype: ctx.archetype || REVIEW_POSTURE.STRONG_PERFORMER,
  });
}

/**
 * @deprecated Use buildGenericExecutableActions — kept as thin alias for golden tests.
 */
export function buildNohoExecutableActions(report, evidence, ctx = {}) {
  return buildGenericExecutableActions(report, evidence, {
    ...ctx,
    archetype: ctx.archetype || REVIEW_POSTURE.CORRECTIVE,
  });
}
