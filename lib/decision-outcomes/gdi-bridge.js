/**
 * GDI ↔ Decision & Outcome bridge.
 * Maps legacy feedback / share-validation into canonical Validation/Action/Outcome events.
 * Does not remove legacy stores — dual-writes for compatibility.
 */

import {
  PRODUCT_MODULE,
  DECISION_TYPE,
  SUBJECT_TYPE,
  GDI_VALIDATION_TYPE,
  GDI_FAMILIARITY,
  GDI_COMMERCIAL_VALUE,
  GDI_CONTACT_PERSON,
  GDI_EMAIL,
  GDI_PHONE,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
  ACTOR_ROLE,
} from "./types.js";
import {
  createDecision,
  getSubjectDecision,
  recordValidation,
  recordAction,
  recordOutcome,
} from "./service.js";

/** Map share / human labels → canonical familiarity */
export function mapFamiliarityToCanonical(value) {
  const v = String(value || "").trim();
  const map = {
    NEVER_SEEN_BEFORE: GDI_FAMILIARITY.NEVER_SEEN,
    NEVER_SEEN: GDI_FAMILIARITY.NEVER_SEEN,
    "Never Seen": GDI_FAMILIARITY.NEVER_SEEN,
    ALREADY_KNOWN: GDI_FAMILIARITY.ALREADY_KNOWN,
    Familiar: GDI_FAMILIARITY.ALREADY_KNOWN,
    "Already Received": GDI_FAMILIARITY.ALREADY_KNOWN,
    ACTIVELY_PURSUING: GDI_FAMILIARITY.ACTIVELY_PURSUING,
    "Already Pursuing": GDI_FAMILIARITY.ACTIVELY_PURSUING,
    PREVIOUSLY_PURSUED_LOST: GDI_FAMILIARITY.PREVIOUSLY_PURSUED,
    PREVIOUSLY_PURSUED: GDI_FAMILIARITY.PREVIOUSLY_PURSUED,
    BOOKED_WON: GDI_FAMILIARITY.BOOKED_WON,
    NOT_RELEVANT: GDI_FAMILIARITY.NOT_RELEVANT,
    UNSURE: GDI_FAMILIARITY.UNSURE,
  };
  return map[v] || null;
}

export function mapCommercialToCanonical(value) {
  const v = String(value || "").trim();
  const map = {
    WORTH_PURSUING_NOW: GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
    "Worth Pursuing": GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
    "Worth Pursuing Now": GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
    WORTH_WATCHING: GDI_COMMERCIAL_VALUE.WORTH_WATCHING,
    "Worth Watching": GDI_COMMERCIAL_VALUE.WORTH_WATCHING,
    NOT_WORTH_PURSUING: GDI_COMMERCIAL_VALUE.NOT_WORTH_PURSUING,
    "Not a Fit": GDI_COMMERCIAL_VALUE.NOT_WORTH_PURSUING,
    "Not Worth Pursuing": GDI_COMMERCIAL_VALUE.NOT_WORTH_PURSUING,
    UNSURE: GDI_COMMERCIAL_VALUE.UNSURE,
    Maybe: GDI_COMMERCIAL_VALUE.UNSURE,
  };
  return map[v] || null;
}

export function mapSalesOutcomeToAction(value) {
  const v = String(value || "").trim();
  const map = {
    Contacted: GDI_ACTION_TYPE.CONTACTED,
    Researching: GDI_ACTION_TYPE.PLANNED_TO_CONTACT,
    "Response Received": GDI_ACTION_TYPE.FOLLOW_UP_REQUIRED,
    "RFP Received": GDI_ACTION_TYPE.RFP_RECEIVED,
    "Site Visit": GDI_ACTION_TYPE.SITE_VISIT_COMPLETED,
    Proposal: GDI_ACTION_TYPE.PROPOSAL_SUBMITTED,
    "Not Pursued": GDI_ACTION_TYPE.NO_ACTION,
    "Not Reviewed": null,
  };
  return map[v] ?? null;
}

export function mapSalesOutcomeToOutcome(value) {
  const v = String(value || "").trim();
  const map = {
    Won: GDI_OUTCOME_TYPE.WON,
    Lost: GDI_OUTCOME_TYPE.LOST,
    "Already Won": GDI_OUTCOME_TYPE.WON,
    "Already Lost": GDI_OUTCOME_TYPE.LOST,
    "Already Booked Elsewhere": GDI_OUTCOME_TYPE.BOOKED,
    BOOKED_WON: GDI_OUTCOME_TYPE.BOOKED,
  };
  return map[v] || null;
}

/**
 * Ensure a Decision exists for a customer-facing qualified GDI opportunity.
 */
export async function ensureGdiOpportunityDecision({
  hotelId,
  opportunity,
  sourceSystem = "GDI",
} = {}) {
  if (!hotelId || !opportunity?.id) {
    return { decision: null, created: false, skipped: "missing_ids" };
  }
  const priority = opportunity.priority;
  if (priority === "DISQUALIFIED") {
    return { decision: null, created: false, skipped: "disqualified" };
  }

  const recommendation =
    opportunity.recommendedAction ||
    opportunity.hotelOpportunityThesis ||
    opportunity.summaryWhyHotel ||
    `Pursue: ${opportunity.title || opportunity.id}`;

  return createDecision({
    hotelId,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    subjectId: opportunity.id,
    recommendation,
    recommendationSummary: String(opportunity.title || recommendation).slice(0, 200),
    evidenceReferenceIds: (opportunity.evidence || [])
      .map((e) => e.sourceUrl || e.url)
      .filter(Boolean)
      .slice(0, 20),
    evidenceSnapshotSummary: {
      title: opportunity.title,
      organizationName: opportunity.organizationName,
      priority: opportunity.priority,
      opportunityType: opportunity.opportunityType,
      venueSourcingStatus: opportunity.venueSourcingStatus,
      hotelFitScore: opportunity.hotelFitScore,
      evidenceConfidence: opportunity.evidenceConfidence,
      frozenAt: new Date().toISOString(),
    },
    confidence: opportunity.evidenceConfidence ?? null,
    confidenceBand:
      opportunity.evidenceConfidence >= 70
        ? "HIGH"
        : opportunity.evidenceConfidence >= 45
          ? "MEDIUM"
          : "LOW",
    confidenceMethodologyVersion: "gdi_evidence_confidence_v1",
    sourceSystem,
    createdBy: { role: ACTOR_ROLE.SYSTEM, userId: "gdi_system" },
  });
}

async function writeValidation(hotelId, decisionId, type, value, meta = {}) {
  if (!value) return null;
  return recordValidation(hotelId, decisionId, {
    validationType: type,
    validationValue: value,
    validationNote: meta.note || null,
    userId: meta.userId,
    role: meta.role || ACTOR_ROLE.OTHER,
    sourceSurface: meta.sourceSurface || "gdi",
    validatedAt: meta.validatedAt || null,
    provenance: meta.provenance || null,
  });
}

/**
 * Dual-write from authenticated GDI feedback payload into canonical events.
 */
export async function ingestGdiAuthFeedback({
  hotelId,
  opportunityId,
  opportunity = null,
  feedback = {},
  actor = {},
} = {}) {
  const ensured = opportunity
    ? await ensureGdiOpportunityDecision({ hotelId, opportunity })
    : await createDecision({
        hotelId,
        productModule: PRODUCT_MODULE.GDI,
        decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
        subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
        subjectId: opportunityId,
        recommendation: `GDI opportunity ${opportunityId}`,
        sourceSystem: "GDI",
        createdBy: { role: ACTOR_ROLE.SYSTEM, userId: "gdi_system" },
      });

  const decision = ensured.decision;
  if (!decision) return { decision: null, events: [] };

  const meta = {
    userId: actor.userId || actor.email || null,
    role: actor.role || ACTOR_ROLE.OTHER,
    sourceSurface: "gdi_auth_feedback",
    note: feedback.comment || null,
    provenance: { legacyStore: "feedback.json", opportunityId },
  };

  const events = [];
  const fam = mapFamiliarityToCanonical(feedback.familiarity);
  if (fam) {
    events.push(
      await writeValidation(
        hotelId,
        decision.decisionId,
        GDI_VALIDATION_TYPE.FAMILIARITY,
        fam,
        meta
      )
    );
  }
  const commercial =
    mapCommercialToCanonical(feedback.commercialStatus) ||
    mapCommercialToCanonical(feedback.value) ||
    mapCommercialToCanonical(feedback.quality);
  if (commercial) {
    events.push(
      await writeValidation(
        hotelId,
        decision.decisionId,
        GDI_VALIDATION_TYPE.COMMERCIAL_VALUE,
        commercial,
        meta
      )
    );
  }

  const actionType = mapSalesOutcomeToAction(
    feedback.salesOutcome || feedback.commercialStatus
  );
  if (actionType) {
    events.push(
      await recordAction(hotelId, decision.decisionId, {
        actionType,
        actionDescription: feedback.salesOutcome,
        userId: meta.userId,
        role: meta.role,
        sourceSurface: meta.sourceSurface,
        provenance: meta.provenance,
      })
    );
  }

  const outcomeType = mapSalesOutcomeToOutcome(
    feedback.salesOutcome || feedback.commercialStatus
  );
  if (outcomeType) {
    events.push(
      await recordOutcome(hotelId, decision.decisionId, {
        outcomeType,
        outcomeNote: feedback.comment || null,
        userId: meta.userId,
        role: meta.role,
        sourceSurface: meta.sourceSurface,
        provenance: meta.provenance,
      })
    );
  }

  return {
    decision: await getSubjectDecision(hotelId, {
      productModule: PRODUCT_MODULE.GDI,
      subjectId: opportunityId,
      decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    }),
    events: events.filter(Boolean),
  };
}

/**
 * Dual-write from GDI share validation into canonical events.
 * Action/outcome intentionally not exposed on share surface by default.
 */
export async function ingestGdiShareValidation({
  hotelId,
  opportunityId,
  opportunity = null,
  validation = {},
  actor = {},
} = {}) {
  const ensured = opportunity
    ? await ensureGdiOpportunityDecision({ hotelId, opportunity })
    : await createDecision({
        hotelId,
        productModule: PRODUCT_MODULE.GDI,
        decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
        subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
        subjectId: opportunityId,
        recommendation: `GDI opportunity ${opportunityId}`,
        sourceSystem: "GDI_SHARE",
        createdBy: {
          role: ACTOR_ROLE.OTHER,
          userId: actor.userId || "share_reviewer",
        },
      });

  const decision = ensured.decision;
  if (!decision) return { decision: null, events: [] };

  const meta = {
    userId: actor.userId || validation.validator || "SHARE_REVIEWER",
    role: actor.role || ACTOR_ROLE.OTHER,
    sourceSurface: "gdi_share_validation",
    note: validation.note || null,
    validatedAt: validation.validatedAt || null,
    provenance: { legacyStore: "share-validation.json", opportunityId },
  };

  const events = [];
  const fam = mapFamiliarityToCanonical(validation.familiarityStatus);
  if (fam) {
    events.push(
      await writeValidation(
        hotelId,
        decision.decisionId,
        GDI_VALIDATION_TYPE.FAMILIARITY,
        fam,
        meta
      )
    );
  }
  const commercial = mapCommercialToCanonical(validation.commercialValue);
  if (commercial) {
    events.push(
      await writeValidation(
        hotelId,
        decision.decisionId,
        GDI_VALIDATION_TYPE.COMMERCIAL_VALUE,
        commercial,
        meta
      )
    );
  }
  if (validation.contactPersonAssessment) {
    const v =
      GDI_CONTACT_PERSON[validation.contactPersonAssessment] ||
      validation.contactPersonAssessment;
    events.push(
      await writeValidation(
        hotelId,
        decision.decisionId,
        GDI_VALIDATION_TYPE.CONTACT_PERSON_VALIDATION,
        v,
        meta
      )
    );
  }
  if (validation.emailAssessment) {
    events.push(
      await writeValidation(
        hotelId,
        decision.decisionId,
        GDI_VALIDATION_TYPE.EMAIL_VALIDATION,
        GDI_EMAIL[validation.emailAssessment] || validation.emailAssessment,
        meta
      )
    );
  }
  if (validation.phoneAssessment) {
    const phoneMap = {
      DIRECT_USABLE: GDI_PHONE.DIRECT_USABLE,
      MAIN_SHARED_LINE: GDI_PHONE.MAIN_SHARED,
      MAIN_SHARED: GDI_PHONE.MAIN_SHARED,
      WRONG: GDI_PHONE.WRONG,
      NOT_TESTED: GDI_PHONE.NOT_TESTED,
    };
    events.push(
      await writeValidation(
        hotelId,
        decision.decisionId,
        GDI_VALIDATION_TYPE.PHONE_VALIDATION,
        phoneMap[validation.phoneAssessment] || validation.phoneAssessment,
        meta
      )
    );
  }

  return {
    decision: await getSubjectDecision(hotelId, {
      productModule: PRODUCT_MODULE.GDI,
      subjectId: opportunityId,
    }),
    events: events.filter(Boolean),
  };
}
