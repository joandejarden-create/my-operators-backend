/**
 * Canonical Contact Merge — REVIEW_REQUIRED apply path + phone auto-apply pilot.
 *
 * Default: REVIEW_REQUIRED for email. Phone pilot may AUTO_APPLY when gates pass.
 * AUTO_ACCEPT_SAFE remains disabled globally.
 *
 * GDI Operating Law: reusable logic only — no hotel-specific constants.
 */

import crypto from "node:crypto";
import { personDedupeKey } from "./contact-reachability.js";
import { IDENTITY_DECISION, PHONE_OUTCOME } from "./surfe-identity-acceptance.js";
import {
  CANONICAL_FIELD_KIND,
  FIELD_MERGE_ACTION,
  FIELD_SOURCE_CLASS,
  MERGE_SAFETY_TIER,
  MERGE_WRITE_MODE,
  REUSE_OUTCOME,
  createCanonicalField,
  createCanonicalPersonRegistry,
  evaluateContactFieldMerge,
  normalizeFieldValue,
} from "./canonical-merge-policy.js";

export const REVIEW_APPLY_VERSION = "canonical-merge-review-apply-v1";

export const PROPOSAL_STATUS = Object.freeze({
  PENDING_REVIEW: "PENDING_REVIEW",
  APPROVED: "APPROVED",
  REJECTED_BY_REVIEWER: "REJECTED_BY_REVIEWER",
  AUTO_APPLY_PHONE_PILOT: "AUTO_APPLY_PHONE_PILOT",
  APPLIED: "APPLIED",
  APPLY_FAILED: "APPLY_FAILED",
  ROLLED_BACK: "ROLLED_BACK",
});

export const APPROVAL_PATH = Object.freeze({
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  AUTO_APPLY_PHONE_PILOT: "AUTO_APPLY_PHONE_PILOT",
});

export const APPLY_RESULT = Object.freeze({
  APPLIED: "APPLIED",
  BLOCKED_NOT_APPROVED: "BLOCKED_NOT_APPROVED",
  BLOCKED_NOT_TIER_1: "BLOCKED_NOT_TIER_1",
  STALE_PROPOSAL_REQUIRES_REEVALUATION: "STALE_PROPOSAL_REQUIRES_REEVALUATION",
  BLOCKED_STRONGER_CANONICAL: "BLOCKED_STRONGER_CANONICAL",
  BLOCKED_IDENTITY: "BLOCKED_IDENTITY",
  BLOCKED_OWNERSHIP: "BLOCKED_OWNERSHIP",
  APPLY_FAILED: "APPLY_FAILED",
  ALREADY_APPLIED: "ALREADY_APPLIED",
  ROLLED_BACK: "ROLLED_BACK",
});

const PHONE_FIELD_KINDS = new Set([
  CANONICAL_FIELD_KIND.PHONE,
  CANONICAL_FIELD_KIND.MOBILE,
  CANONICAL_FIELD_KIND.MAIN_ORG_PHONE,
]);

function newId(prefix) {
  return `${prefix}_${crypto.randomBytes(6).toString("hex")}`;
}

function hashValue(kind, value) {
  const norm = normalizeFieldValue(kind, value) || "";
  return crypto.createHash("sha256").update(`${kind}::${norm}`).digest("hex").slice(0, 16);
}

/** Optimistic concurrency fingerprint for a person field slot. */
export function fieldStateFingerprint(person, fieldKind) {
  const field = person?.fields?.[fieldKind] || null;
  return {
    personVersion: person?.recordVersion ?? 0,
    fieldKind,
    valueHash: field ? hashValue(fieldKind, field.value) : null,
    canonicalStatus: field?.canonicalStatus ?? null,
    sourceClass: field?.sourceClass ?? null,
  };
}

export function createReviewApplyStore(baseRegistry = null) {
  const registry = baseRegistry || createCanonicalPersonRegistry();
  const proposals = new Map();
  const immutableAudit = [];
  const phonePilotMetrics = createEmptyPhonePilotMetrics();

  function recordImmutableAudit(entry) {
    immutableAudit.push({
      ...entry,
      auditId: newId("audit"),
      recordedAt: new Date().toISOString(),
    });
  }

  function getProposal(proposalId) {
    return proposals.get(proposalId) || null;
  }

  function listProposals(filter = {}) {
    let list = [...proposals.values()];
    if (filter.status) list = list.filter((p) => p.status === filter.status);
    if (filter.personId) list = list.filter((p) => p.personId === filter.personId);
    return list;
  }

  function upsertProposal(proposal) {
    proposals.set(proposal.proposalId, proposal);
    return proposal;
  }

  return {
    registry,
    getProposal,
    listProposals,
    upsertProposal,
    recordImmutableAudit,
    immutableAudit,
    phonePilotMetrics,
    snapshot() {
      return {
        registry: registry.snapshot(),
        proposals: [...proposals.values()],
        immutableAudit: [...immutableAudit],
        phonePilotMetrics: { ...phonePilotMetrics },
      };
    },
  };
}

export function createEmptyPhonePilotMetrics() {
  return {
    phoneAttempts: 0,
    providerPhoneReturned: 0,
    identityAccepted: 0,
    fieldOwnershipPassed: 0,
    autoApplied: 0,
    held: 0,
    rejected: 0,
    directMobile: 0,
    directOffice: 0,
    mainSharedLine: 0,
    wrongPersonCollision: 0,
    noResult: 0,
    laterHotelConfirmedUseful: 0,
    laterHotelConfirmedWrong: 0,
    creditsSpent: 0,
    providerCallsAvoided: 0,
  };
}

/**
 * Phone auto-apply eligibility (pilot measurement gates).
 * Default: identity ACCEPTED + TIER_1_SAFE.
 * Optional pilot flag allows LIMITED evidence mobile when ownership is clean.
 */
export function evaluatePhoneAutoApplyEligibility(proposal, person, opts = {}) {
  const reasons = [];
  const allowLimited =
    opts.phonePilotAllowLimitedEvidenceMobile === true ||
    process.env.PHONE_PILOT_ALLOW_LIMITED_EVIDENCE_MOBILE === "1";

  if (!PHONE_FIELD_KINDS.has(proposal.fieldType)) {
    return { eligible: false, reasons: ["NOT_PHONE_FIELD"] };
  }

  if (proposal.mergeDecision !== FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD &&
      proposal.mergeDecision !== FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD) {
    reasons.push("MERGE_DECISION_NOT_APPLYABLE");
  }

  if (proposal.safetyTier === MERGE_SAFETY_TIER.TIER_3_BLOCK) {
    reasons.push("TIER_3_BLOCK");
  }

  const id = proposal.identityDecision;
  if (id === IDENTITY_DECISION.AMBIGUOUS || id === IDENTITY_DECISION.REJECTED ||
      id === IDENTITY_DECISION.NOT_FOUND || id === IDENTITY_DECISION.CORROBORATION_ONLY) {
    reasons.push(`IDENTITY_${id}`);
  } else if (id === IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE) {
    if (!allowLimited) reasons.push("LIMITED_EVIDENCE_REQUIRES_REVIEW");
  } else if (id !== IDENTITY_DECISION.ACCEPTED) {
    reasons.push("IDENTITY_NOT_ACCEPTED");
  }

  if (proposal.safetyTier === MERGE_SAFETY_TIER.TIER_2_REVIEW && !allowLimited) {
    reasons.push("TIER_2_REQUIRES_REVIEW");
  }

  if (proposal.fieldOwnershipDecision === "COLLISION" ||
      proposal.fieldOwnershipDecision === "OTHER_PERSON") {
    reasons.push("OWNERSHIP_CONFLICT");
  }

  if (proposal.phoneOutcome === PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION) {
    reasons.push("OTHER_PERSON_PHONE_COLLISION");
  }
  if (proposal.phoneOutcome === PHONE_OUTCOME.SAME_MAIN_LINE) {
    reasons.push("MAIN_LINE_NOT_AUTO_APPLY");
  }
  if (proposal.phoneOutcome === PHONE_OUTCOME.IDENTITY_REJECTED ||
      proposal.phoneOutcome === PHONE_OUTCOME.AMBIGUOUS) {
    reasons.push(`PHONE_OUTCOME_${proposal.phoneOutcome}`);
  }

  if (person?.formerAffiliation || person?.reuseBlocked) {
    reasons.push("PERSON_BLOCKED");
  }

  if (!proposal.proposedValue) {
    reasons.push("NO_PROPOSED_VALUE");
  }

  return { eligible: reasons.length === 0, reasons };
}

/**
 * Build field-level proposals from dry-run row + reachability source row.
 */
export function generateMergeProposalsFromReachability(store, {
  dryRunRow,
  reachabilityRow,
  hotelId = null,
  phonePilotAllowLimitedEvidenceMobile = false,
} = {}) {
  const created = [];
  const person = store.registry.getByNameOrg(dryRunRow.name, dryRunRow.organization) ||
    store.registry.upsertPerson({
      displayName: dryRunRow.name,
      organization: dryRunRow.organization,
      title: reachabilityRow?.role || null,
    });

  for (const d of dryRunRow.decisions || []) {
    if (d.action !== FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD &&
        d.action !== FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD) {
      continue;
    }
    if (d.safetyTier === MERGE_SAFETY_TIER.TIER_3_BLOCK) continue;

    const fieldType = d.field === "MOBILE" ? CANONICAL_FIELD_KIND.MOBILE : d.field;
    const isPhone = PHONE_FIELD_KINDS.has(fieldType);
    const isEmail = fieldType === CANONICAL_FIELD_KIND.EMAIL ||
      fieldType === CANONICAL_FIELD_KIND.ROLE_EMAIL;

    const blockedIdentity = new Set([
      IDENTITY_DECISION.AMBIGUOUS,
      IDENTITY_DECISION.REJECTED,
      IDENTITY_DECISION.NOT_FOUND,
      IDENTITY_DECISION.CORROBORATION_ONLY,
    ]);
    if (blockedIdentity.has(dryRunRow.identity)) {
      if (isPhone) store.phonePilotMetrics.rejected += 1;
      continue;
    }

    // Email path: TIER_1 only for first apply path
    if (isEmail && d.safetyTier !== MERGE_SAFETY_TIER.TIER_1_SAFE) continue;

    const baseline = fieldStateFingerprint(person, fieldType);
    const proposal = {
      proposalId: newId("prop"),
      personId: person.personId,
      displayName: dryRunRow.name,
      organization: dryRunRow.organization,
      fieldType,
      currentValue: d.previousValue ?? null,
      proposedValue: d.proposedValue ?? null,
      mergeDecision: d.action,
      safetyTier: d.safetyTier,
      source: "surfe",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
      provider: "surfe",
      provenance: buildProvenanceFromReachability(reachabilityRow, fieldType, d.proposedValue),
      identityDecision: dryRunRow.identity,
      fieldOwnershipDecision: reachabilityRow?.surfe?.phoneOutcome ===
        PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION
        ? "COLLISION"
        : "PASS",
      precedenceResult: d.reasonCode,
      reasonCode: d.reasonCode,
      explanation: d.explanation,
      createdAt: new Date().toISOString(),
      status: PROPOSAL_STATUS.PENDING_REVIEW,
      approvalPath: APPROVAL_PATH.REVIEW_REQUIRED,
      baselineFieldHash: baseline.valueHash,
      personVersion: baseline.personVersion,
      hotelId,
      opportunityId: dryRunRow.opportunityId,
      opportunityTitle: reachabilityRow?.opportunityTitle || null,
      phoneOutcome: isPhone ? reachabilityRow?.surfe?.phoneOutcome : null,
      emailOutcome: isEmail ? reachabilityRow?.surfe?.emailOutcome : null,
      creditsSpent: isPhone || isEmail ? (reachabilityRow?.creditsSpent || 0) : 0,
      reviewer: null,
      reviewerNote: null,
      approvedAt: null,
      appliedAt: null,
      rolledBackAt: null,
    };

    if (isPhone) {
      store.phonePilotMetrics.phoneAttempts += 1;
      if (proposal.proposedValue) store.phonePilotMetrics.providerPhoneReturned += 1;
      if (proposal.identityDecision === IDENTITY_DECISION.ACCEPTED) {
        store.phonePilotMetrics.identityAccepted += 1;
      }
      if (proposal.fieldOwnershipDecision === "PASS") {
        store.phonePilotMetrics.fieldOwnershipPassed += 1;
      }
      if (proposal.phoneOutcome === PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL) {
        store.phonePilotMetrics.directMobile += 1;
      } else if (proposal.phoneOutcome === PHONE_OUTCOME.NEW_OFFICE_PHONE ||
                 proposal.phoneOutcome === PHONE_OUTCOME.NEW_DIRECT_PHONE) {
        store.phonePilotMetrics.directOffice += 1;
      } else if (proposal.phoneOutcome === PHONE_OUTCOME.SAME_MAIN_LINE) {
        store.phonePilotMetrics.mainSharedLine += 1;
      } else if (proposal.phoneOutcome === PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION) {
        store.phonePilotMetrics.wrongPersonCollision += 1;
      } else if (proposal.phoneOutcome === PHONE_OUTCOME.NOT_FOUND) {
        store.phonePilotMetrics.noResult += 1;
      }

      const phoneElig = evaluatePhoneAutoApplyEligibility(
        proposal,
        person,
        { phonePilotAllowLimitedEvidenceMobile }
      );
      if (phoneElig.eligible) {
        proposal.approvalPath = APPROVAL_PATH.AUTO_APPLY_PHONE_PILOT;
        proposal.status = PROPOSAL_STATUS.AUTO_APPLY_PHONE_PILOT;
        proposal.reviewer = "PHONE_PILOT_AUTO";
        proposal.reviewerNote = "Phone pilot auto-apply — all gates passed";
        proposal.approvedAt = new Date().toISOString();
      } else {
        store.phonePilotMetrics.held += 1;
        proposal.phonePilotBlockReasons = phoneElig.reasons;
      }
    }

    store.upsertProposal(proposal);
    created.push(proposal);
  }

  return created;
}

function buildProvenanceFromReachability(row, fieldType, value) {
  if (!row) return { source: "unknown" };
  const isPhone = PHONE_FIELD_KINDS.has(fieldType);
  return {
    source: "surfe_reachability_eval",
    provider: "surfe",
    sourceUrl: null,
    identityDecision: row.identity?.decision || null,
    verificationState: isPhone ? "PROVIDER_ACCEPTED" : "PROVIDER_ACCEPTED",
    confidence: row.whoConfidence || null,
    discoveredAt: row.started_at || new Date().toISOString(),
    opportunityId: row.opportunityId,
    opportunityTitle: row.opportunityTitle,
    surfeOutcome: isPhone ? row.surfe?.phoneOutcome : row.surfe?.emailOutcome,
    creditsSpent: row.creditsSpent || 0,
    value,
  };
}

export function approveCanonicalContactMergeProposal(store, proposalId, {
  reviewer = "FOUNDER_REVIEW",
  note = null,
} = {}) {
  const proposal = store.getProposal(proposalId);
  if (!proposal) return { ok: false, error: "PROPOSAL_NOT_FOUND" };
  if (proposal.status === PROPOSAL_STATUS.APPLIED) {
    return { ok: false, error: "ALREADY_APPLIED" };
  }
  if (proposal.status === PROPOSAL_STATUS.ROLLED_BACK) {
    return { ok: false, error: "ROLLED_BACK_REAPPLY_BLOCKED" };
  }
  if (proposal.safetyTier !== MERGE_SAFETY_TIER.TIER_1_SAFE) {
    return { ok: false, error: "NOT_TIER_1_SAFE", proposal };
  }
  if (PHONE_FIELD_KINDS.has(proposal.fieldType)) {
    return { ok: false, error: "PHONE_USE_AUTO_APPLY_PILOT_PATH", proposal };
  }

  proposal.status = PROPOSAL_STATUS.APPROVED;
  proposal.reviewer = reviewer;
  proposal.reviewerNote = note;
  proposal.approvedAt = new Date().toISOString();
  store.upsertProposal(proposal);

  store.recordImmutableAudit({
    type: "PROPOSAL_APPROVED",
    proposalId,
    reviewer,
    note,
    fieldType: proposal.fieldType,
    proposedValue: proposal.proposedValue,
  });

  return { ok: true, proposal };
}

export function rejectCanonicalContactMergeProposal(store, proposalId, {
  reviewer = "FOUNDER_REVIEW",
  note = null,
} = {}) {
  const proposal = store.getProposal(proposalId);
  if (!proposal) return { ok: false, error: "PROPOSAL_NOT_FOUND" };
  if (proposal.status === PROPOSAL_STATUS.APPLIED) {
    return { ok: false, error: "ALREADY_APPLIED" };
  }

  proposal.status = PROPOSAL_STATUS.REJECTED_BY_REVIEWER;
  proposal.reviewer = reviewer;
  proposal.reviewerNote = note;
  store.upsertProposal(proposal);

  store.recordImmutableAudit({
    type: "PROPOSAL_REJECTED",
    proposalId,
    reviewer,
    note,
  });

  return { ok: true, proposal };
}

/**
 * Re-validate proposal against live canonical state before apply.
 */
export function validateProposalForApply(store, proposalId) {
  const proposal = store.getProposal(proposalId);
  if (!proposal) return { ok: false, result: APPLY_RESULT.APPLY_FAILED, error: "NOT_FOUND" };

  const person = findPersonById(store, proposal.personId);
  if (!person) return { ok: false, result: APPLY_RESULT.APPLY_FAILED, error: "PERSON_NOT_FOUND" };

  const allowedStatus = new Set([
    PROPOSAL_STATUS.APPROVED,
    PROPOSAL_STATUS.AUTO_APPLY_PHONE_PILOT,
  ]);
  if (!allowedStatus.has(proposal.status)) {
    return { ok: false, result: APPLY_RESULT.BLOCKED_NOT_APPROVED, proposal, person };
  }

  const isPhone = PHONE_FIELD_KINDS.has(proposal.fieldType);
  if (isPhone) {
    const elig = evaluatePhoneAutoApplyEligibility(proposal, person, {
      phonePilotAllowLimitedEvidenceMobile:
        proposal.approvalPath === APPROVAL_PATH.AUTO_APPLY_PHONE_PILOT,
    });
    if (!elig.eligible && proposal.approvalPath === APPROVAL_PATH.AUTO_APPLY_PHONE_PILOT) {
      return { ok: false, result: APPLY_RESULT.BLOCKED_OWNERSHIP, reasons: elig.reasons, proposal, person };
    }
  } else if (proposal.safetyTier !== MERGE_SAFETY_TIER.TIER_1_SAFE) {
    return { ok: false, result: APPLY_RESULT.BLOCKED_NOT_TIER_1, proposal, person };
  }

  const live = fieldStateFingerprint(person, proposal.fieldType);
  // Field-scoped stale check — sibling field applies may bump personVersion without
  // invalidating this proposal's target slot.
  if (live.valueHash !== proposal.baselineFieldHash) {
    return { ok: false, result: APPLY_RESULT.STALE_PROPOSAL_REQUIRES_REEVALUATION, proposal, person, live };
  }

  // Re-run merge decision
  const incoming = createCanonicalField({
    kind: proposal.fieldType,
    value: proposal.proposedValue,
    sourceClass: proposal.sourceClass,
    source: proposal.source,
    provider: proposal.provider,
    discoveredAt: proposal.provenance?.discoveredAt,
    verificationState: proposal.provenance?.verificationState,
    opportunityId: proposal.opportunityId,
    hotelId: proposal.hotelId,
    canonicalStatus: "PROPOSED",
  });

  const fresh = evaluateContactFieldMerge({
    canonicalPerson: person,
    canonicalField: person.fields?.[proposal.fieldType] || null,
    incomingField: incoming,
    identityDecision: proposal.identityDecision,
    fieldOutcome: proposal.phoneOutcome || proposal.emailOutcome,
    ownershipDecision: proposal.fieldOwnershipDecision === "PASS" ? null : proposal.fieldOwnershipDecision,
    writeMode: MERGE_WRITE_MODE.REVIEW_REQUIRED,
  });

  if (fresh.action === FIELD_MERGE_ACTION.REJECT_FIELD ||
      fresh.action === FIELD_MERGE_ACTION.HOLD_FOR_REVIEW) {
    return { ok: false, result: APPLY_RESULT.BLOCKED_IDENTITY, fresh, proposal, person };
  }
  if (fresh.action === FIELD_MERGE_ACTION.NO_INCREMENTAL_VALUE) {
    return { ok: false, result: APPLY_RESULT.BLOCKED_STRONGER_CANONICAL, fresh, proposal, person };
  }

  return { ok: true, proposal, person, fresh, incoming };
}

function findPersonById(store, personId) {
  const snap = store.registry.snapshot();
  return snap.people.find((p) => p.personId === personId) || null;
}

export function applyApprovedCanonicalContactMerge(store, proposalId, opts = {}) {
  const proposal = store.getProposal(proposalId);
  if (proposal?.status === PROPOSAL_STATUS.APPLIED) {
    return { ok: true, result: APPLY_RESULT.ALREADY_APPLIED, proposal };
  }
  if (proposal?.status === PROPOSAL_STATUS.ROLLED_BACK) {
    return { ok: false, result: APPLY_RESULT.ROLLED_BACK, proposal };
  }

  const validation = validateProposalForApply(store, proposalId);
  if (!validation.ok) {
    if (proposal && proposal.status !== PROPOSAL_STATUS.ROLLED_BACK) {
      proposal.status = PROPOSAL_STATUS.APPLY_FAILED;
      proposal.applyError = validation.result;
      store.upsertProposal(proposal);
    }
    store.recordImmutableAudit({
      type: "APPLY_BLOCKED",
      proposalId,
      result: validation.result,
      error: validation.error || validation.reasons,
    });
    return { ok: false, ...validation };
  }

  const { person, incoming } = validation;
  const fieldKind = proposal.fieldType;
  const beforeValue = person.fields?.[fieldKind]?.value ?? null;
  const beforeField = person.fields?.[fieldKind] ? { ...person.fields[fieldKind] } : null;

  const appliedField = {
    ...incoming,
    kind: fieldKind,
    canonicalStatus: "CANONICAL",
    acceptedAt: new Date().toISOString(),
    provenance: proposal.provenance,
    appliedVia: proposal.approvalPath,
    proposalId: proposal.proposalId,
    reviewer: proposal.reviewer,
  };

  const fields = { ...(person.fields || {}), [fieldKind]: appliedField };
  const updated = store.registry.upsertPerson({
    ...person,
    fields,
    recordVersion: (person.recordVersion || 0) + 1,
  });

  proposal.status = PROPOSAL_STATUS.APPLIED;
  proposal.appliedAt = new Date().toISOString();
  store.upsertProposal(proposal);

  if (PHONE_FIELD_KINDS.has(fieldKind) &&
      proposal.approvalPath === APPROVAL_PATH.AUTO_APPLY_PHONE_PILOT) {
    store.phonePilotMetrics.autoApplied += 1;
    store.phonePilotMetrics.creditsSpent += proposal.creditsSpent || 0;
  }

  store.recordImmutableAudit({
    type: "FIELD_APPLIED",
    proposalId,
    personId: person.personId,
    fieldType: fieldKind,
    beforeValue,
    afterValue: proposal.proposedValue,
    source: proposal.source,
    sourceClass: proposal.sourceClass,
    provider: proposal.provider,
    identityDecision: proposal.identityDecision,
    fieldOwnershipDecision: proposal.fieldOwnershipDecision,
    mergeDecision: proposal.mergeDecision,
    precedence: proposal.precedenceResult,
    reviewer: proposal.reviewer,
    approvedAt: proposal.approvedAt,
    appliedAt: proposal.appliedAt,
    hotelId: proposal.hotelId,
    opportunityId: proposal.opportunityId,
    providerCost: proposal.creditsSpent || 0,
    provenance: proposal.provenance,
    beforeField,
    afterField: appliedField,
  });

  return { ok: true, result: APPLY_RESULT.APPLIED, proposal, person: updated };
}

export function rollbackCanonicalContactMerge(store, proposalId, {
  reviewer = "FOUNDER_REVIEW",
  reason = null,
} = {}) {
  const proposal = store.getProposal(proposalId);
  if (!proposal) return { ok: false, error: "PROPOSAL_NOT_FOUND" };
  if (proposal.status !== PROPOSAL_STATUS.APPLIED) {
    return { ok: false, error: "NOT_APPLIED", status: proposal.status };
  }

  const person = findPersonById(store, proposal.personId);
  if (!person) return { ok: false, error: "PERSON_NOT_FOUND" };

  const applyAudit = [...store.immutableAudit]
    .reverse()
    .find((a) => a.type === "FIELD_APPLIED" && a.proposalId === proposalId);

  const fieldKind = proposal.fieldType;
  const fields = { ...(person.fields || {}) };

  if (applyAudit?.beforeField) {
    fields[fieldKind] = applyAudit.beforeField;
  } else if (applyAudit?.beforeValue == null) {
    fields[fieldKind] = null;
  } else {
    fields[fieldKind] = createCanonicalField({
      kind: fieldKind,
      value: applyAudit.beforeValue,
      sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_DIRECT,
      canonicalStatus: "CANONICAL",
    });
  }

  const restored = store.registry.upsertPerson({
    ...person,
    fields,
    recordVersion: (person.recordVersion || 0) + 1,
  });

  proposal.status = PROPOSAL_STATUS.ROLLED_BACK;
  proposal.rolledBackAt = new Date().toISOString();
  proposal.rollbackReason = reason;
  store.upsertProposal(proposal);

  store.recordImmutableAudit({
    type: "ROLLBACK",
    proposalId,
    personId: person.personId,
    fieldType: fieldKind,
    restoredValue: fields[fieldKind]?.value ?? null,
    reviewer,
    reason,
    priorApplyAuditId: applyAudit?.auditId || null,
  });

  return { ok: true, proposal, person: restored };
}

/** After apply — reuse lookup with provenance. */
export function resolveCanonicalReuseAfterApply(store, { name, organization, needEmail, needPhone } = {}) {
  const reuse = store.registry.resolveReuse({ name, organization, needEmail, needPhone });
  if (reuse.outcome === REUSE_OUTCOME.CONTACT_REUSED_FROM_CANONICAL) {
    store.phonePilotMetrics.providerCallsAvoided += reuse.providerCallsAvoided;
  }
  const person = reuse.person;
  if (person) {
    const emailProv = person.fields?.EMAIL?.provenance || null;
    const mobileProv = person.fields?.MOBILE?.provenance || null;
    const phoneProv = person.fields?.PHONE?.provenance || null;
    reuse.provenance = {
      provider:
        emailProv?.provider ||
        mobileProv?.provider ||
        phoneProv?.provider ||
        person.fields?.EMAIL?.provider ||
        person.fields?.MOBILE?.provider ||
        null,
      email: emailProv || person.fields?.EMAIL?.source || null,
      mobile: mobileProv || person.fields?.MOBILE?.source || null,
      phone: phoneProv || person.fields?.PHONE?.source || null,
      originatingHotelId:
        person.fields?.MOBILE?.hotelId ||
        person.fields?.EMAIL?.hotelId ||
        emailProv?.hotelId ||
        null,
    };
  }
  return reuse;
}

export function computePhonePilotRates(metrics = {}) {
  const auto = metrics.autoApplied || 0;
  const useful = metrics.laterHotelConfirmedUseful || 0;
  const wrong = metrics.laterHotelConfirmedWrong || 0;
  const pct = (n, d) => (d ? Math.round((1000 * n) / d) / 10 : null);
  return {
    phoneUsefulnessRate: pct(useful, auto),
    phoneFieldAccuracyRate: pct(auto - wrong, auto),
    phoneDirectnessRate: pct(metrics.directMobile + metrics.directOffice, metrics.providerPhoneReturned),
    phoneWrongPersonRate: pct(metrics.wrongPersonCollision, metrics.phoneAttempts),
    phoneMainLineRate: pct(metrics.mainSharedLine, metrics.providerPhoneReturned),
    creditsPerUsefulPhone: useful ? Math.round((100 * (metrics.creditsSpent || 0)) / useful) / 100 : null,
    creditsPerAutoAppliedPhone: auto ? Math.round((100 * (metrics.creditsSpent || 0)) / auto) / 100 : null,
  };
}

/**
 * Hydrate registry from dry-run baseline (official fields only, not PROPOSED_DRY_RUN).
 */
export function hydrateRegistryFromReachability(registry, reachabilityResults, hotelId) {
  for (const row of reachabilityResults || []) {
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
          hotelId,
          opportunityId: row.opportunityId,
        });
      }
      if (row.before?.phone) {
        fields.PHONE = createCanonicalField({
          kind: CANONICAL_FIELD_KIND.PHONE,
          value: row.before.phone,
          sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_DIRECT,
          source: "official_source_discovery",
          canonicalStatus: "CANONICAL",
          hotelId,
          opportunityId: row.opportunityId,
        });
      }
      person = registry.upsertPerson({
        displayName: row.name,
        organization: row.organization,
        title: row.role,
        fields,
        recordVersion: 0,
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
  }
}

export function buildProposalSummary(proposals = []) {
  const byStatus = {};
  for (const p of proposals) {
    byStatus[p.status] = (byStatus[p.status] || 0) + 1;
  }
  return {
    total: proposals.length,
    byStatus,
    pending: byStatus[PROPOSAL_STATUS.PENDING_REVIEW] || 0,
    approved: byStatus[PROPOSAL_STATUS.APPROVED] || 0,
    autoApplyPhone: byStatus[PROPOSAL_STATUS.AUTO_APPLY_PHONE_PILOT] || 0,
    applied: byStatus[PROPOSAL_STATUS.APPLIED] || 0,
    rejected: byStatus[PROPOSAL_STATUS.REJECTED_BY_REVIEWER] || 0,
    rolledBack: byStatus[PROPOSAL_STATUS.ROLLED_BACK] || 0,
  };
}
