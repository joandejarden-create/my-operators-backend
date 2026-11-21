/**
 * Additive schema proposal for Contract Terms & Renewals (V4).
 * DO NOT apply without explicit founder Meta API authorization.
 *
 * Existing HI Hotel Contracts already has:
 *   Commencement Date, Scheduled Expiration Date, Term Years,
 *   Renewal Options Summary, Termination Rights Summary
 *
 * This proposal adds structured fields for lifecycle intelligence.
 */

export const CONTRACT_TERMS_SCHEMA_PROPOSAL_V4 = "contract-terms-schema-proposal-v4";

export const DISCLOSURE_CLASS = Object.freeze({
  EXPLICITLY_DISCLOSED: "EXPLICITLY_DISCLOSED",
  CALCULATED_FROM_VERIFIED_TERMS: "CALCULATED_FROM_VERIFIED_TERMS",
  CONDITIONAL: "CONDITIONAL",
  UNDETERMINED: "UNDETERMINED",
  NOT_DISCLOSED: "NOT_DISCLOSED",
  NOT_VERIFIED: "NOT_VERIFIED",
});

export const RENEWAL_STATUS_CLASS = Object.freeze({
  RENEWAL_EXECUTED_VERIFIED: "RENEWAL_EXECUTED_VERIFIED",
  EXTENSION_EXECUTED_VERIFIED: "EXTENSION_EXECUTED_VERIFIED",
  AUTOMATIC_RENEWAL_CONDITIONAL: "AUTOMATIC_RENEWAL_CONDITIONAL",
  RENEWAL_OPTION_AVAILABLE: "RENEWAL_OPTION_AVAILABLE",
  TERMINATED_VERIFIED: "TERMINATED_VERIFIED",
  REPLACED_OR_REFLAGGED_VERIFIED: "REPLACED_OR_REFLAGGED_VERIFIED",
  CURRENT_TERMS_CORROBORATED: "CURRENT_TERMS_CORROBORATED",
  STATUS_UNCONFIRMED: "STATUS_UNCONFIRMED",
});

export const EXISTING_CONTRACT_FIELDS_REUSED = Object.freeze([
  "Commencement Date",
  "Scheduled Expiration Date",
  "Term Years",
  "Renewal Options Summary",
  "Termination Rights Summary",
  "Contract Status",
  "Agreement Role",
  "Counterparty Name Raw",
  "Primary Source URL",
  "Primary SEC Accession",
  "Evidence Confidence",
  "Publication State",
]);

/** Additive fields proposed for HI Hotel Contracts (Meta apply deferred). */
export const PROPOSED_CONTRACT_ADDITIVE_FIELDS = Object.freeze([
  {
    name: "Original Agreement Date",
    type: "date",
    purpose: "Quoted original agreement / instrument date",
  },
  {
    name: "Original Effective Date",
    type: "date",
    purpose: "Quoted commencement / effective date when distinct",
  },
  {
    name: "Current Corroborated Expiration",
    type: "date",
    purpose: "Post-amendment / post-renewal expiration when verified",
  },
  {
    name: "Renewal Options Count",
    type: "number",
    purpose: "Number of disclosed renewal options",
  },
  {
    name: "Renewal Option Years Each",
    type: "number",
    purpose: "Years per option when uniform",
  },
  {
    name: "Renewal Exercise Party",
    type: "singleSelect",
    choices: ["FRANCHISEE_OWNER", "FRANCHISOR", "MANAGER", "OWNER", "EITHER", "NOT_DISCLOSED"],
  },
  {
    name: "Renewal Mode",
    type: "singleSelect",
    choices: ["ELECTIVE", "AUTOMATIC", "AUTOMATIC_CONDITIONAL", "NOT_DISCLOSED"],
  },
  {
    name: "Renewal Notice Days",
    type: "number",
    purpose: "Advance notice days when disclosed",
  },
  {
    name: "Renewal Notice Deadline",
    type: "date",
  },
  {
    name: "Option Exercise Deadline",
    type: "date",
  },
  {
    name: "Renewal Status Class",
    type: "singleSelect",
    choices: Object.values(RENEWAL_STATUS_CLASS),
  },
  {
    name: "Termination Rights Structured",
    type: "multilineText",
    purpose: "JSON/text: cause / without-cause / performance / cure / CoC",
  },
  {
    name: "Change Of Control Rights Summary",
    type: "multilineText",
  },
  {
    name: "Financial Terms Summary",
    type: "multilineText",
    purpose: "Royalties / base / incentive / marketing when disclosed",
  },
  {
    name: "Terms Evidence JSON",
    type: "multilineText",
    purpose: "Per-field evidence excerpts + accession + confidence (research)",
  },
  {
    name: "Decision Window Next Date",
    type: "date",
  },
  {
    name: "Decision Window Class",
    type: "singleSelect",
    choices: Object.values(DISCLOSURE_CLASS),
  },
  {
    name: "Decision Window Notes",
    type: "multilineText",
  },
  {
    name: "Terms Schema Version",
    type: "singleLineText",
  },
]);

/** Additive milestone types already mostly covered; propose enum extensions only. */
export const PROPOSED_MILESTONE_TYPE_ADDITIONS = Object.freeze([
  "RENEWAL_NOTICE_DEADLINE",
  "OPTION_EXERCISE_DEADLINE",
  "PERFORMANCE_TEST_REVIEW",
  "CHANGE_OF_CONTROL_TRIGGER",
  "EARLIEST_TERMINATION_WINDOW",
]);

export function buildAdditiveSchemaDiffPacket() {
  return {
    module: CONTRACT_TERMS_SCHEMA_PROPOSAL_V4,
    status: "PROPOSED_PENDING_FOUNDER_META_APPLY",
    baseId: "appCCUsuGsE1ifoLk",
    table: "HI Hotel Contracts",
    existingFieldsReused: EXISTING_CONTRACT_FIELDS_REUSED,
    addFields: PROPOSED_CONTRACT_ADDITIVE_FIELDS.map((f) => f.name),
    fieldBlueprints: PROPOSED_CONTRACT_ADDITIVE_FIELDS,
    milestoneTypeAdditions: PROPOSED_MILESTONE_TYPE_ADDITIONS,
    publicationState: "CUSTOMER_BLOCKED",
    stop: "REQUIRES_EXPLICIT_FOUNDER_META_API_AUTHORIZATION",
    note: "Do not apply merely because design is proposed. Persist structured terms to research JSON / Notes until authorized.",
  };
}

export default {
  buildAdditiveSchemaDiffPacket,
  PROPOSED_CONTRACT_ADDITIVE_FIELDS,
  RENEWAL_STATUS_CLASS,
  DISCLOSURE_CLASS,
};
