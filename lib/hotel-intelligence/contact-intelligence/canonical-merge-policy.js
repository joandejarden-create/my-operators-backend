/**
 * Canonical Contact Merge Policy — hotel-agnostic Contact Intelligence.
 *
 * Dealality owns WHO. Providers may contribute HOW TO REACH only after
 * identity + field-ownership gates pass.
 *
 * GDI Operating Law: reusable product logic lives here.
 * Hotel-specific facts never appear as constants in this module.
 *
 * Default write mode: REVIEW_REQUIRED (no automatic production writes).
 */

import { personDedupeKey } from "./contact-reachability.js";
import {
  IDENTITY_DECISION,
  EMAIL_OUTCOME,
  PHONE_OUTCOME,
} from "./surfe-identity-acceptance.js";

export const CANONICAL_MERGE_POLICY_VERSION = "canonical-contact-merge-policy-v1";

/** Field kinds supported on canonical person records. */
export const CANONICAL_FIELD_KIND = Object.freeze({
  EMAIL: "EMAIL",
  PHONE: "PHONE",
  MOBILE: "MOBILE",
  ROLE_EMAIL: "ROLE_EMAIL",
  MAIN_ORG_PHONE: "MAIN_ORG_PHONE",
});

/**
 * Cross-product field source classes (generic — not hotel-specific).
 * Distinct from owner-resolution V2 SOURCE_CLASS (evidence origin taxonomy).
 */
export const FIELD_SOURCE_CLASS = Object.freeze({
  HOTEL_VALIDATED: "HOTEL_VALIDATED",
  USER_VALIDATED: "USER_VALIDATED",
  OFFICIAL_DIRECT: "OFFICIAL_DIRECT",
  OFFICIAL_FUNCTIONAL: "OFFICIAL_FUNCTIONAL",
  CANONICAL_INTERNAL: "CANONICAL_INTERNAL",
  PROVIDER_VERIFIED: "PROVIDER_VERIFIED",
  PROVIDER_ACCEPTED: "PROVIDER_ACCEPTED",
  PROVIDER_CORROBORATION: "PROVIDER_CORROBORATION",
  INFERRED: "INFERRED",
});

/** Higher = stronger. Never overwrite stronger with weaker. */
export const FIELD_SOURCE_PRECEDENCE = Object.freeze({
  [FIELD_SOURCE_CLASS.HOTEL_VALIDATED]: 100,
  [FIELD_SOURCE_CLASS.USER_VALIDATED]: 95,
  [FIELD_SOURCE_CLASS.OFFICIAL_DIRECT]: 85,
  [FIELD_SOURCE_CLASS.CANONICAL_INTERNAL]: 75,
  [FIELD_SOURCE_CLASS.PROVIDER_VERIFIED]: 65,
  [FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED]: 55,
  [FIELD_SOURCE_CLASS.OFFICIAL_FUNCTIONAL]: 45,
  [FIELD_SOURCE_CLASS.PROVIDER_CORROBORATION]: 35,
  [FIELD_SOURCE_CLASS.INFERRED]: 15,
});

export const FIELD_MERGE_ACTION = Object.freeze({
  ACCEPT_NEW_FIELD: "ACCEPT_NEW_FIELD",
  REPLACE_WEAKER_FIELD: "REPLACE_WEAKER_FIELD",
  CORROBORATE_EXISTING: "CORROBORATE_EXISTING",
  HOLD_FOR_REVIEW: "HOLD_FOR_REVIEW",
  REJECT_FIELD: "REJECT_FIELD",
  NO_INCREMENTAL_VALUE: "NO_INCREMENTAL_VALUE",
});

export const MERGE_WRITE_MODE = Object.freeze({
  DRY_RUN: "DRY_RUN",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  AUTO_ACCEPT_SAFE: "AUTO_ACCEPT_SAFE",
});

export const MERGE_SAFETY_TIER = Object.freeze({
  TIER_1_SAFE: "TIER_1_SAFE",
  TIER_2_REVIEW: "TIER_2_REVIEW",
  TIER_3_BLOCK: "TIER_3_BLOCK",
});

export const REUSE_OUTCOME = Object.freeze({
  CONTACT_REUSED_FROM_CANONICAL: "CONTACT_REUSED_FROM_CANONICAL",
  CONTACT_NEWLY_ENRICHED: "CONTACT_NEWLY_ENRICHED",
  CONTACT_CORROBORATED: "CONTACT_CORROBORATED",
  CONTACT_CONFLICT: "CONTACT_CONFLICT",
});

export const HOTEL_FEEDBACK_STATE = Object.freeze({
  CONFIRMED_CORRECT: "CONFIRMED_CORRECT",
  CONFIRMED_WRONG: "CONFIRMED_WRONG",
  OUTDATED: "OUTDATED",
  LEFT_ORGANIZATION: "LEFT_ORGANIZATION",
  WRONG_ROLE: "WRONG_ROLE",
  WRONG_PERSON: "WRONG_PERSON",
  VALID_BUT_NOT_DECISION_MAKER: "VALID_BUT_NOT_DECISION_MAKER",
});

const BLOCKING_IDENTITY = new Set([
  IDENTITY_DECISION.AMBIGUOUS,
  IDENTITY_DECISION.REJECTED,
  IDENTITY_DECISION.NOT_FOUND,
  IDENTITY_DECISION.CORROBORATION_ONLY,
]);

const BLOCKING_FIELD_OUTCOMES = new Set([
  EMAIL_OUTCOME.IDENTITY_REJECTED,
  EMAIL_OUTCOME.CONFLICTS_WITH_OFFICIAL,
  EMAIL_OUTCOME.NOT_FOUND,
  PHONE_OUTCOME.IDENTITY_REJECTED,
  PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION,
  PHONE_OUTCOME.AMBIGUOUS,
  PHONE_OUTCOME.NOT_FOUND,
]);

export function sourceClassPrecedence(sourceClass) {
  return FIELD_SOURCE_PRECEDENCE[sourceClass] || 0;
}

export function normalizeFieldValue(kind, value) {
  if (value == null || value === "") return null;
  const s = String(value).trim();
  if (!s) return null;
  if (kind === CANONICAL_FIELD_KIND.EMAIL || kind === CANONICAL_FIELD_KIND.ROLE_EMAIL) {
    return s.toLowerCase();
  }
  if (
    kind === CANONICAL_FIELD_KIND.PHONE ||
    kind === CANONICAL_FIELD_KIND.MOBILE ||
    kind === CANONICAL_FIELD_KIND.MAIN_ORG_PHONE
  ) {
    return s.replace(/\D/g, "").slice(-15) || null;
  }
  return s;
}

export function valuesEquivalent(kind, a, b) {
  const x = normalizeFieldValue(kind, a);
  const y = normalizeFieldValue(kind, b);
  if (!x || !y) return false;
  if (
    kind === CANONICAL_FIELD_KIND.PHONE ||
    kind === CANONICAL_FIELD_KIND.MOBILE ||
    kind === CANONICAL_FIELD_KIND.MAIN_ORG_PHONE
  ) {
    return x.slice(-10) === y.slice(-10) || x.endsWith(y) || y.endsWith(x);
  }
  return x === y;
}

export function createCanonicalField(partial = {}) {
  const kind = partial.kind || CANONICAL_FIELD_KIND.EMAIL;
  return {
    kind,
    value: partial.value ?? null,
    displayValue: partial.displayValue ?? partial.value ?? null,
    sourceClass: partial.sourceClass || FIELD_SOURCE_CLASS.INFERRED,
    source: partial.source || null,
    provider: partial.provider || null,
    discoveredAt: partial.discoveredAt || null,
    verifiedAt: partial.verifiedAt || null,
    verificationState: partial.verificationState || "UNVERIFIED",
    confidence: partial.confidence ?? null,
    ownershipConfidence: partial.ownershipConfidence ?? null,
    canonicalStatus: partial.canonicalStatus || "PROPOSED",
    supersededValue: partial.supersededValue || null,
    supersededReason: partial.supersededReason || null,
    hotelId: partial.hotelId || null,
    opportunityId: partial.opportunityId || null,
    provenance: partial.provenance || null,
    acceptedAt: partial.acceptedAt || null,
    appliedVia: partial.appliedVia || null,
    proposalId: partial.proposalId || null,
    audit: Array.isArray(partial.audit) ? partial.audit : [],
  };
}

/** Person-canonical reachability — NOT opportunity relationship. */
export function createCanonicalPerson(partial = {}) {
  return {
    personId:
      partial.personId ||
      `person_${personDedupeKey(partial.displayName, partial.organization)}`,
    displayName: partial.displayName || null,
    organization: partial.organization || null,
    title: partial.title || null,
    affiliationStatus: partial.affiliationStatus || "CURRENT_UNKNOWN",
    formerAffiliation: partial.formerAffiliation === true,
    fields: {
      EMAIL: partial.fields?.EMAIL || null,
      PHONE: partial.fields?.PHONE || null,
      MOBILE: partial.fields?.MOBILE || null,
      ROLE_EMAIL: partial.fields?.ROLE_EMAIL || null,
      MAIN_ORG_PHONE: partial.fields?.MAIN_ORG_PHONE || null,
    },
    hotelFeedback: Array.isArray(partial.hotelFeedback) ? partial.hotelFeedback : [],
    reuseBlocked: partial.reuseBlocked === true,
    provenance: partial.provenance || {},
    recordVersion: partial.recordVersion ?? 0,
  };
}

/** Opportunity-specific relationship — never promoted to universal person facts. */
export function createOpportunityRelationship(partial = {}) {
  return {
    relationshipId: partial.relationshipId || null,
    hotelId: partial.hotelId || null,
    opportunityId: partial.opportunityId || null,
    opportunityTitle: partial.opportunityTitle || null,
    personId: partial.personId || null,
    eventRole: partial.eventRole || null,
    whyThisPerson: partial.whyThisPerson || null,
    candidateScore: partial.candidateScore ?? null,
    whoConfidence: partial.whoConfidence || null,
    eventConfidence: partial.eventConfidence || null,
    gdiContactRole: partial.gdiContactRole || null,
    createdAt: partial.createdAt || new Date().toISOString(),
  };
}

function decisionEnvelope({
  action,
  reasonCode,
  explanation,
  safetyTier,
  resultingPrecedence,
  previousField = null,
  proposedField = null,
  identityDecision = null,
  ownershipDecision = null,
  writeAllowed = false,
} = {}) {
  return {
    action,
    reasonCode,
    explanation,
    safetyTier,
    resultingPrecedence: resultingPrecedence ?? null,
    previousField,
    proposedField,
    identityDecision,
    ownershipDecision,
    writeAllowed,
    policyVersion: CANONICAL_MERGE_POLICY_VERSION,
    decidedAt: new Date().toISOString(),
  };
}

/**
 * Core merge decision for one field.
 */
export function evaluateContactFieldMerge(input = {}) {
  const {
    canonicalPerson = {},
    canonicalField = null,
    incomingField = null,
    identityDecision = null,
    fieldOutcome = null,
    ownershipDecision = null,
    allowLimitedEvidence = true,
    writeMode = MERGE_WRITE_MODE.REVIEW_REQUIRED,
  } = input;

  const kind = incomingField?.kind || canonicalField?.kind || CANONICAL_FIELD_KIND.EMAIL;
  const incomingValue = incomingField?.value ?? null;
  const currentValue = canonicalField?.value ?? null;

  if (!identityDecision || identityDecision === "UNKNOWN") {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.REJECT_FIELD,
      reasonCode: "IDENTITY_UNKNOWN",
      explanation: "Canonical merge requires an identity decision.",
      safetyTier: MERGE_SAFETY_TIER.TIER_3_BLOCK,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }
  if (BLOCKING_IDENTITY.has(identityDecision)) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.REJECT_FIELD,
      reasonCode: `IDENTITY_${identityDecision}`,
      explanation: `Identity ${identityDecision} cannot merge fields into canonical person.`,
      safetyTier: MERGE_SAFETY_TIER.TIER_3_BLOCK,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }
  if (
    identityDecision === IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE &&
    !allowLimitedEvidence
  ) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.HOLD_FOR_REVIEW,
      reasonCode: "LIMITED_EVIDENCE_NOT_PERMITTED",
      explanation: "Limited-evidence identity is not permitted under current policy.",
      safetyTier: MERGE_SAFETY_TIER.TIER_2_REVIEW,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }

  if (canonicalPerson.formerAffiliation === true) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.REJECT_FIELD,
      reasonCode: "FORMER_EMPLOYEE",
      explanation: "Former employee — do not merge new reachability onto this person.",
      safetyTier: MERGE_SAFETY_TIER.TIER_3_BLOCK,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }
  if (canonicalPerson.reuseBlocked === true) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.REJECT_FIELD,
      reasonCode: "REUSE_BLOCKED_BY_FEEDBACK",
      explanation: "Hotel/user feedback blocks reuse of this person/contact.",
      safetyTier: MERGE_SAFETY_TIER.TIER_3_BLOCK,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }
  const badFeedback = (canonicalPerson.hotelFeedback || []).some((f) =>
    [
      HOTEL_FEEDBACK_STATE.CONFIRMED_WRONG,
      HOTEL_FEEDBACK_STATE.WRONG_PERSON,
      HOTEL_FEEDBACK_STATE.LEFT_ORGANIZATION,
    ].includes(f?.state)
  );
  if (badFeedback) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.REJECT_FIELD,
      reasonCode: "HOTEL_FEEDBACK_BLOCKS_FIELD",
      explanation: "Hotel/user marked person or field as wrong / left org.",
      safetyTier: MERGE_SAFETY_TIER.TIER_3_BLOCK,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }

  if (
    ownershipDecision === "COLLISION" ||
    ownershipDecision === "OTHER_PERSON" ||
    fieldOutcome === PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION
  ) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.REJECT_FIELD,
      reasonCode: "FIELD_OWNERSHIP_CONFLICT",
      explanation: "Provider field is owned by another known person — reject.",
      safetyTier: MERGE_SAFETY_TIER.TIER_3_BLOCK,
      identityDecision,
      ownershipDecision: ownershipDecision || "COLLISION",
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }
  if (fieldOutcome && BLOCKING_FIELD_OUTCOMES.has(fieldOutcome)) {
    const isConflict = fieldOutcome === EMAIL_OUTCOME.CONFLICTS_WITH_OFFICIAL;
    return decisionEnvelope({
      action: isConflict ? FIELD_MERGE_ACTION.HOLD_FOR_REVIEW : FIELD_MERGE_ACTION.REJECT_FIELD,
      reasonCode: `FIELD_OUTCOME_${fieldOutcome}`,
      explanation: `Field outcome ${fieldOutcome} blocks safe auto-merge.`,
      safetyTier: isConflict ? MERGE_SAFETY_TIER.TIER_2_REVIEW : MERGE_SAFETY_TIER.TIER_3_BLOCK,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }

  if (!incomingValue) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.NO_INCREMENTAL_VALUE,
      reasonCode: "NO_INCOMING_VALUE",
      explanation: "No incoming field value.",
      safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }

  const incomingClass = incomingField.sourceClass || FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED;
  const incomingPrec = sourceClassPrecedence(incomingClass);
  const currentClass = canonicalField?.sourceClass || null;
  const currentPrec = currentClass ? sourceClassPrecedence(currentClass) : 0;

  if (currentValue && valuesEquivalent(kind, currentValue, incomingValue)) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.CORROBORATE_EXISTING,
      reasonCode: "SAME_VALUE_CORROBORATION",
      explanation: "Incoming value matches canonical — corroborate provenance only.",
      safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
      resultingPrecedence: Math.max(currentPrec, incomingPrec),
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
      writeAllowed: writeMode !== MERGE_WRITE_MODE.DRY_RUN,
    });
  }

  if (currentValue && incomingPrec < currentPrec) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.NO_INCREMENTAL_VALUE,
      reasonCode: "WEAKER_THAN_CANONICAL",
      explanation: `Incoming ${incomingClass} (${incomingPrec}) weaker than ${currentClass} (${currentPrec}).`,
      safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
      resultingPrecedence: currentPrec,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }

  if (fieldOutcome === PHONE_OUTCOME.SAME_MAIN_LINE) {
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.NO_INCREMENTAL_VALUE,
      reasonCode: "SAME_MAIN_LINE",
      explanation: "Provider returned the same main organization line.",
      safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
    });
  }

  const limited =
    identityDecision === IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE;

  if (!currentValue) {
    const tier = limited ? MERGE_SAFETY_TIER.TIER_2_REVIEW : MERGE_SAFETY_TIER.TIER_1_SAFE;
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
      reasonCode: limited ? "ACCEPT_NEW_LIMITED_EVIDENCE" : "ACCEPT_NEW_MISSING_FIELD",
      explanation: limited
        ? "New field with limited-evidence identity — review required."
        : "Fill missing canonical field from accepted provider/official source.",
      safetyTier: tier,
      resultingPrecedence: incomingPrec,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: incomingField,
      writeAllowed:
        writeMode === MERGE_WRITE_MODE.AUTO_ACCEPT_SAFE &&
        tier === MERGE_SAFETY_TIER.TIER_1_SAFE,
    });
  }

  if (incomingPrec > currentPrec) {
    const roleToDirect =
      (kind === CANONICAL_FIELD_KIND.EMAIL || kind === CANONICAL_FIELD_KIND.ROLE_EMAIL) &&
      (currentClass === FIELD_SOURCE_CLASS.OFFICIAL_FUNCTIONAL ||
        currentClass === FIELD_SOURCE_CLASS.INFERRED ||
        kind === CANONICAL_FIELD_KIND.ROLE_EMAIL);
    const tier =
      limited || roleToDirect
        ? MERGE_SAFETY_TIER.TIER_2_REVIEW
        : MERGE_SAFETY_TIER.TIER_1_SAFE;
    return decisionEnvelope({
      action: FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD,
      reasonCode: roleToDirect ? "UPGRADE_ROLE_TO_DIRECT" : "REPLACE_WEAKER_SOURCE",
      explanation: `Replace ${currentClass} with stronger ${incomingClass}.`,
      safetyTier: tier,
      resultingPrecedence: incomingPrec,
      identityDecision,
      ownershipDecision,
      previousField: canonicalField,
      proposedField: {
        ...incomingField,
        supersededValue: currentValue,
        supersededReason: `Replaced by ${incomingClass}`,
      },
      writeAllowed:
        writeMode === MERGE_WRITE_MODE.AUTO_ACCEPT_SAFE &&
        tier === MERGE_SAFETY_TIER.TIER_1_SAFE,
    });
  }

  return decisionEnvelope({
    action: FIELD_MERGE_ACTION.HOLD_FOR_REVIEW,
    reasonCode: "SAME_PRECEDENCE_DIFFERENT_VALUE",
    explanation: "Equal-strength conflicting values require human review.",
    safetyTier: MERGE_SAFETY_TIER.TIER_2_REVIEW,
    resultingPrecedence: currentPrec,
    identityDecision,
    ownershipDecision,
    previousField: canonicalField,
    proposedField: incomingField,
  });
}

export function sourceClassFromEmailOutcome(outcome) {
  if (outcome === EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL) {
    return FIELD_SOURCE_CLASS.PROVIDER_CORROBORATION;
  }
  if (outcome === EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL) {
    return FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED;
  }
  if (outcome === EMAIL_OUTCOME.NEW_ROLE_BASED_EMAIL) {
    return FIELD_SOURCE_CLASS.OFFICIAL_FUNCTIONAL;
  }
  return FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED;
}

export function sourceClassFromPhoneOutcome(outcome) {
  if (outcome === PHONE_OUTCOME.CORROBORATION_ONLY || outcome === PHONE_OUTCOME.SAME_MAIN_LINE) {
    return FIELD_SOURCE_CLASS.PROVIDER_CORROBORATION;
  }
  return FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED;
}

/** Apply hotel/user feedback — never deletes audit history. */
export function applyHotelFeedback(person, feedback = {}) {
  const next = {
    ...person,
    hotelFeedback: [...(person.hotelFeedback || []), { ...feedback, at: new Date().toISOString() }],
  };
  if (
    [
      HOTEL_FEEDBACK_STATE.CONFIRMED_WRONG,
      HOTEL_FEEDBACK_STATE.WRONG_PERSON,
      HOTEL_FEEDBACK_STATE.LEFT_ORGANIZATION,
    ].includes(feedback.state)
  ) {
    next.reuseBlocked = true;
    if (feedback.state === HOTEL_FEEDBACK_STATE.LEFT_ORGANIZATION) {
      next.formerAffiliation = true;
      next.affiliationStatus = "FORMER";
    }
  }
  return next;
}

/**
 * In-memory canonical person registry for dry-run / multi-hotel reuse.
 * Not a production writer.
 */
export function createCanonicalPersonRegistry() {
  const people = new Map();
  const relationships = [];
  const auditLog = [];
  let providerCallsAvoided = 0;

  function lookupKey(name, organization) {
    return personDedupeKey(name, organization);
  }

  function getByNameOrg(name, organization) {
    return people.get(lookupKey(name, organization)) || null;
  }

  function upsertPerson(partial) {
    const key = lookupKey(partial.displayName, partial.organization);
    const existing = people.get(key);
    const person = createCanonicalPerson({
      ...existing,
      ...partial,
      personId: existing?.personId || partial.personId,
      fields: { ...(existing?.fields || {}), ...(partial.fields || {}) },
      hotelFeedback: partial.hotelFeedback || existing?.hotelFeedback || [],
    });
    people.set(key, person);
    return person;
  }

  function addRelationship(rel) {
    const row = createOpportunityRelationship(rel);
    relationships.push(row);
    return row;
  }

  function resolveReuse({ name, organization, needEmail = false, needPhone = false } = {}) {
    const person = getByNameOrg(name, organization);
    if (!person || person.reuseBlocked || person.formerAffiliation) {
      return {
        outcome: null,
        person: null,
        reuseEmail: false,
        reusePhone: false,
        providerCallsAvoided: 0,
        requestEmail: needEmail,
        requestMobile: needPhone,
      };
    }
    const hasEmail = Boolean(person.fields.EMAIL?.value || person.fields.ROLE_EMAIL?.value);
    const hasPhone = Boolean(person.fields.MOBILE?.value || person.fields.PHONE?.value);
    const reuseEmail = needEmail && hasEmail;
    const reusePhone = needPhone && hasPhone;
    let avoided = 0;
    if (reuseEmail) avoided += 1;
    if (reusePhone) avoided += 1;
    if (avoided) providerCallsAvoided += avoided;
    return {
      outcome:
        reuseEmail || reusePhone ? REUSE_OUTCOME.CONTACT_REUSED_FROM_CANONICAL : null,
      person,
      reuseEmail,
      reusePhone,
      providerCallsAvoided: avoided,
      requestEmail: needEmail && !hasEmail,
      requestMobile: needPhone && !hasPhone,
    };
  }

  function recordAudit(entry) {
    auditLog.push({ ...entry, recordedAt: new Date().toISOString() });
  }

  function applyMergeDecision({
    person,
    fieldKind,
    decision,
    writeMode = MERGE_WRITE_MODE.DRY_RUN,
  }) {
    const canMutate =
      writeMode === MERGE_WRITE_MODE.AUTO_ACCEPT_SAFE &&
      decision.writeAllowed &&
      decision.safetyTier === MERGE_SAFETY_TIER.TIER_1_SAFE &&
      (decision.action === FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD ||
        decision.action === FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD ||
        decision.action === FIELD_MERGE_ACTION.CORROBORATE_EXISTING);

    recordAudit({
      personId: person?.personId,
      fieldKind,
      action: decision.action,
      reasonCode: decision.reasonCode,
      explanation: decision.explanation,
      safetyTier: decision.safetyTier,
      previousValue: decision.previousField?.value ?? null,
      proposedValue: decision.proposedField?.value ?? null,
      sourceClass: decision.proposedField?.sourceClass ?? null,
      identityDecision: decision.identityDecision,
      ownershipDecision: decision.ownershipDecision,
      writeMode,
      mutated: false,
    });

    if (!canMutate) {
      return { person, mutated: false, decision };
    }

    const fields = { ...(person.fields || {}) };
    if (
      decision.action === FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD ||
      decision.action === FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD
    ) {
      fields[fieldKind] = {
        ...decision.proposedField,
        canonicalStatus: "CANONICAL",
      };
    }
    const updated = upsertPerson({ ...person, fields });
    auditLog[auditLog.length - 1].mutated = true;
    return { person: updated, mutated: true, decision };
  }

  function snapshot() {
    return {
      people: [...people.values()],
      relationships: [...relationships],
      auditLog: [...auditLog],
      providerCallsAvoided,
      metrics: {
        canonicalPersonCount: people.size,
        relationshipCount: relationships.length,
        auditCount: auditLog.length,
        providerCallsAvoided,
      },
    };
  }

  return {
    lookupKey,
    getByNameOrg,
    upsertPerson,
    addRelationship,
    resolveReuse,
    recordAudit,
    applyMergeDecision,
    snapshot,
    get providerCallsAvoided() {
      return providerCallsAvoided;
    },
  };
}

/**
 * Simulate merges from a reachability eval row into registry.
 * DRY_RUN applies proposed fields in-memory only (PROPOSED_DRY_RUN status).
 * Never writes production packages.
 */
export function simulateReachabilityRowMerge(registry, row, opts = {}) {
  const writeMode = opts.writeMode || MERGE_WRITE_MODE.DRY_RUN;
  const hotelId = opts.hotelId || null;
  const identityDecision = row.identity?.decision || null;

  let person = registry.getByNameOrg(row.name, row.organization);
  if (!person) {
    const fields = {};
    if (row.before?.email) {
      fields.EMAIL = createCanonicalField({
        kind: CANONICAL_FIELD_KIND.EMAIL,
        value: row.before.email,
        sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_DIRECT,
        source: "official_source_discovery",
        verificationState: "OFFICIAL_SOURCE_VERIFIED",
        canonicalStatus: "CANONICAL",
        opportunityId: row.opportunityId,
        hotelId,
      });
    }
    if (row.before?.phone) {
      fields.PHONE = createCanonicalField({
        kind: CANONICAL_FIELD_KIND.PHONE,
        value: row.before.phone,
        sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_DIRECT,
        source: "official_source_discovery",
        canonicalStatus: "CANONICAL",
        opportunityId: row.opportunityId,
        hotelId,
      });
    }
    person = registry.upsertPerson({
      displayName: row.name,
      organization: row.organization,
      title: row.role,
      fields,
    });
  }

  registry.addRelationship({
    hotelId,
    opportunityId: row.opportunityId,
    opportunityTitle: row.opportunityTitle,
    personId: person.personId,
    eventRole: row.role,
    whoConfidence: row.whoConfidence,
  });

  const decisions = [];

  const emailVal = row.after?.acceptedEmail || null;
  if (emailVal || row.surfe?.emailOutcome) {
    const incoming = createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: emailVal,
      sourceClass: sourceClassFromEmailOutcome(row.surfe?.emailOutcome),
      source: "surfe",
      provider: "surfe",
      discoveredAt: new Date().toISOString(),
      opportunityId: row.opportunityId,
      hotelId,
    });
    const d = evaluateContactFieldMerge({
      canonicalPerson: person,
      canonicalField: person.fields.EMAIL || person.fields.ROLE_EMAIL,
      incomingField: incoming,
      identityDecision,
      fieldOutcome: row.surfe?.emailOutcome,
      allowLimitedEvidence: true,
      writeMode,
    });
    decisions.push({ field: "EMAIL", ...d });
    registry.applyMergeDecision({ person, fieldKind: "EMAIL", decision: d, writeMode });
    if (
      writeMode === MERGE_WRITE_MODE.DRY_RUN &&
      (d.action === FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD ||
        d.action === FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD)
    ) {
      person = registry.upsertPerson({
        ...person,
        fields: {
          ...person.fields,
          EMAIL: { ...incoming, canonicalStatus: "PROPOSED_DRY_RUN" },
        },
      });
    }
  }

  person = registry.getByNameOrg(row.name, row.organization) || person;

  const phoneVal = row.after?.acceptedPhone || null;
  if (phoneVal || row.surfe?.phoneOutcome) {
    const isMobile = row.surfe?.phoneOutcome === PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL;
    const fieldKind = isMobile ? CANONICAL_FIELD_KIND.MOBILE : CANONICAL_FIELD_KIND.PHONE;
    const incoming = createCanonicalField({
      kind: fieldKind,
      value: phoneVal,
      sourceClass: sourceClassFromPhoneOutcome(row.surfe?.phoneOutcome),
      source: "surfe",
      provider: "surfe",
      discoveredAt: new Date().toISOString(),
      opportunityId: row.opportunityId,
      hotelId,
    });
    const d = evaluateContactFieldMerge({
      canonicalPerson: person,
      canonicalField: person.fields[fieldKind] || person.fields.PHONE || person.fields.MOBILE,
      incomingField: incoming,
      identityDecision,
      fieldOutcome: row.surfe?.phoneOutcome,
      ownershipDecision:
        row.surfe?.phoneOutcome === PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION
          ? "COLLISION"
          : null,
      allowLimitedEvidence: true,
      writeMode,
    });
    decisions.push({ field: fieldKind, ...d });
    registry.applyMergeDecision({ person, fieldKind, decision: d, writeMode });
    if (
      writeMode === MERGE_WRITE_MODE.DRY_RUN &&
      (d.action === FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD ||
        d.action === FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD)
    ) {
      person = registry.upsertPerson({
        ...person,
        fields: {
          ...person.fields,
          [fieldKind]: { ...incoming, canonicalStatus: "PROPOSED_DRY_RUN" },
        },
      });
    }
  }

  return { person: registry.getByNameOrg(row.name, row.organization), decisions };
}

export function summarizeMergeDecisions(decisionRows = []) {
  const counts = {
    ACCEPT_NEW_FIELD: 0,
    REPLACE_WEAKER_FIELD: 0,
    CORROBORATE_EXISTING: 0,
    HOLD_FOR_REVIEW: 0,
    REJECT_FIELD: 0,
    NO_INCREMENTAL_VALUE: 0,
  };
  for (const d of decisionRows) {
    if (counts[d.action] != null) counts[d.action] += 1;
  }
  return counts;
}

export function extendCoverageWithMergeMetrics(
  coverage = {},
  { mergeSummary, reuseAttempts = 0, reuseHits = 0, providerCallsAvoided = 0 } = {}
) {
  const attempted = Object.values(mergeSummary || {}).reduce((s, n) => s + n, 0);
  const accepted =
    (mergeSummary?.ACCEPT_NEW_FIELD || 0) + (mergeSummary?.REPLACE_WEAKER_FIELD || 0);
  const pct = (n, d) => (d ? Math.round((1000 * n) / d) / 10 : null);
  return {
    ...coverage,
    rates: {
      ...(coverage.rates || {}),
      canonicalMergeAcceptanceRate: pct(accepted, attempted),
      contactReuseRate: pct(reuseHits, reuseAttempts),
      providerCallsAvoided,
    },
    mergeSummary: mergeSummary || null,
    providerCallsAvoided,
  };
}
