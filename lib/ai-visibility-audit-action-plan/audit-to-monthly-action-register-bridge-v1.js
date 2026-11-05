/**
 * Proposed shared action-register bridge:
 * Onboarding AI Visibility Audit actions → Monthly ADP Performance Review tracking.
 *
 * DATA CONTRACT + LIFECYCLE ONLY.
 * Do not wire into production ADP without separate founder authorization.
 */

import { ACTION_REGISTER_SCHEMA, createActionRegisterEntry } from "../ai-demand-positioning/monthly-review/action-register-schema-v1.js";
import {
  AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
  AUDIT_IMPLEMENTATION_STATUS,
} from "./audit-pdf-contract-v1.js";

export const ONBOARDING_AUDIT_ACTION_BRIDGE_CONTRACT =
  "AI_VISIBILITY_AUDIT_TO_ADP_MONTHLY_ACTION_REGISTER_V1";

export const ONBOARDING_ACTION_SOURCE = "AI_VISIBILITY_AUDIT_ONBOARDING";

export const ONBOARDING_ACTION_LIFECYCLE = Object.freeze([
  "DRAFT_IN_AUDIT",
  "HOTEL_APPROVED",
  "OPEN_IN_MONTHLY_REGISTER",
  "IN_PROGRESS",
  "COMPLETED",
  "DEFERRED",
  "VERIFIED_IN_FOLLOW_UP",
]);

/**
 * @param {object} auditAction
 * @param {{ propertyId: string, monitoringPeriodId?: string|null, createdDate?: string }} ctx
 */
export function mapAuditActionToMonthlyRegisterEntry(auditAction, ctx = {}) {
  const statusMap = {
    [AUDIT_IMPLEMENTATION_STATUS.NOT_STARTED]: "Open",
    [AUDIT_IMPLEMENTATION_STATUS.AWAITING_HOTEL_DECISION]: "Open",
    [AUDIT_IMPLEMENTATION_STATUS.IN_PROGRESS]: "In Progress",
    [AUDIT_IMPLEMENTATION_STATUS.COMPLETED]: "Completed",
    [AUDIT_IMPLEMENTATION_STATUS.VERIFIED]: "Completed",
  };

  const evidenceItems =
    auditAction.supportingEvidence ||
    auditAction.technicalDetails?.evidence ||
    [];
  const evidenceText = Array.isArray(evidenceItems)
    ? evidenceItems
        .map((e) =>
          typeof e === "string" ? e : [e.label, e.url, e.note].filter(Boolean).join(" ")
        )
        .join(" ")
    : String(evidenceItems || "");
  const evidenceRefs = Array.isArray(auditAction.evidenceRefs)
    ? auditAction.evidenceRefs
    : evidenceItems
        .map((e) => (typeof e === "string" ? e : e?.url))
        .filter((u) => u && /^https?:\/\//i.test(String(u)));

  return createActionRegisterEntry({
    actionId: auditAction.actionId,
    propertyId: ctx.propertyId || auditAction.propertyId || null,
    monitoringPeriodId: ctx.monitoringPeriodId || null,
    actionTitle:
      auditAction.headline ||
      auditAction.recommendedAction?.slice(0, 180) ||
      auditAction.whatWeFound?.slice(0, 180) ||
      "",
    observedIssue: auditAction.whatWeFound || auditAction.finding || "",
    recommendedAction: Array.isArray(auditAction.recommendedSteps)
      ? auditAction.recommendedSteps.join(" ")
      : auditAction.recommendedAction || "",
    rationale: [
      auditAction.whyItMatters || "",
      auditAction.claimType ? `Claim type: ${auditAction.claimType}.` : "",
      evidenceText,
    ]
      .filter(Boolean)
      .join(" "),
    evidenceRefs,
    accountableOwnerRole:
      auditAction.accountableOwner ||
      auditAction.responsibleParty ||
      "Hotel digital marketing / agency",
    supportingTeam:
      auditAction.supportingParties ||
      auditAction.supportingTeam ||
      "Dealality (advisory)",
    createdDate: ctx.createdDate || auditAction.createdDate || null,
    targetDate: auditAction.suggestedTimeframe || auditAction.targetDate || null,
    status: statusMap[auditAction.implementationStatus] || "Open",
    expectedSignal: (() => {
      const cc = auditAction.completionCriteria;
      if (cc && typeof cc === "object") {
        return [
          cc.investigationCompleted && `Investigation: ${cc.investigationCompleted}`,
          cc.correctionSubmitted && `Submitted: ${cc.correctionSubmitted}`,
          cc.correctionVerifiedLive && `Verified live: ${cc.correctionVerifiedLive}`,
          cc.noChangeRequired && `No-change path: ${cc.noChangeRequired}`,
        ]
          .filter(Boolean)
          .join(" ");
      }
      if (Array.isArray(auditAction.successCriteria)) {
        return auditAction.successCriteria.join("; ");
      }
      return String(
        auditAction.successCriteria || auditAction.verificationCriteria || ""
      );
    })(),
    founderCustomerNotes: auditAction.requiresHotelApproval
      ? `Hotel decision required${
          auditAction.hotelApprovalNotes
            ? `: ${auditAction.hotelApprovalNotes}`
            : "."
        }`
      : null,
    impactTracking: {
      schema: "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
      before: null,
      actionDate: null,
      nextRun: null,
      change: null,
      assessment: null,
      causationClaimed: false,
      note: "Onboarding audit actions may be tracked in monthly reviews. Status changes do not prove AI ranking or revenue causation.",
      onboardingBridge: {
        contract: ONBOARDING_AUDIT_ACTION_BRIDGE_CONTRACT,
        sourceTemplate: AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
        sourceProduct: ONBOARDING_ACTION_SOURCE,
        claimType: auditAction.claimType || null,
        priority: auditAction.priority || null,
        commercialObjective: auditAction.commercialObjective || null,
        unprovenOutcomes: auditAction.unprovenOutcomes || null,
        lifecycleHint: auditAction.requiresHotelApproval
          ? "DRAFT_IN_AUDIT→HOTEL_APPROVED→OPEN_IN_MONTHLY_REGISTER"
          : "DRAFT_IN_AUDIT→OPEN_IN_MONTHLY_REGISTER",
      },
    },
  });
}

/**
 * @param {object} auditPayload
 */
export function proposeMonthlyRegisterImportFromAudit(auditPayload) {
  const propertyId = auditPayload?.property?.propertyId || null;
  const actions = (auditPayload?.actions || [])
    .map((a) =>
      mapAuditActionToMonthlyRegisterEntry(a, {
        propertyId,
        createdDate: auditPayload?.reporting?.generatedAt || null,
      })
    );

  return {
    contract: ONBOARDING_AUDIT_ACTION_BRIDGE_CONTRACT,
    targetSchema: ACTION_REGISTER_SCHEMA,
    productionIntegrationAuthorized: false,
    propertyId,
    actionCount: actions.length,
    lifecycle: ONBOARDING_ACTION_LIFECYCLE,
    actions,
    notes: [
      "Proposal only — do not write to monthly review archive or Airtable without founder authorization.",
      "Optional commercial actions should remain AWAITING_HOTEL_DECISION until leadership confirms.",
      "Internal research-only items must not be imported as customer monthly actions.",
    ],
  };
}
