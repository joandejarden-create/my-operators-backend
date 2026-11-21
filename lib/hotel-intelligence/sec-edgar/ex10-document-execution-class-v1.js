/**
 * Document execution classification for EX-10 research persistence.
 * Forms/drafts must never become verified executed hotel contracts.
 */

export const EX10_DOCUMENT_EXECUTION_CLASS_V1 = "ex10-document-execution-class-v1";

export const DOCUMENT_EXECUTION_CLASS = Object.freeze({
  EXECUTED_NAMED_HOTEL_AGREEMENT: "EXECUTED_NAMED_HOTEL_AGREEMENT",
  EXECUTED_PORTFOLIO_AGREEMENT: "EXECUTED_PORTFOLIO_AGREEMENT",
  AMENDMENT_OR_EXTENSION_INSTRUMENT: "AMENDMENT_OR_EXTENSION_INSTRUMENT",
  FORM_AGREEMENT: "FORM_AGREEMENT",
  DRAFT_OR_PROPOSED_AGREEMENT: "DRAFT_OR_PROPOSED_AGREEMENT",
  EXECUTION_STATUS_UNRESOLVED: "EXECUTION_STATUS_UNRESOLVED",
});

export const END_DATE_STATUS = Object.freeze({
  EXPLICIT_CONTRACT_END_DATE: "EXPLICIT_CONTRACT_END_DATE",
  CALCULATED_FROM_VERIFIED_TERMS: "CALCULATED_FROM_VERIFIED_TERMS",
  HISTORICAL_DATE_STATUS_UNCONFIRMED: "HISTORICAL_DATE_STATUS_UNCONFIRMED",
  CURRENTLY_CORROBORATED_END_DATE: "CURRENTLY_CORROBORATED_END_DATE",
  TERMINATION_EXECUTED_VERIFIED: "TERMINATION_EXECUTED_VERIFIED",
  NOT_DISCLOSED: "NOT_DISCLOSED",
});

export const PROSPECT_CLASS = Object.freeze({
  A_VERIFIED_DECISION_WINDOW: "A_VERIFIED_DECISION_WINDOW",
  B_VERIFIED_END_DATE_STATUS_UNCONFIRMED: "B_VERIFIED_END_DATE_STATUS_UNCONFIRMED",
  C_VERIFIED_COMMERCIAL_SIGNAL: "C_VERIFIED_COMMERCIAL_SIGNAL",
  D_RESEARCH_REQUIRED: "D_RESEARCH_REQUIRED",
  CLOSED_SUPERSEDED: "CLOSED_SUPERSEDED",
});

/**
 * Map harvest instrument + scope → execution class.
 */
export function classifyDocumentExecution(rec = {}) {
  const instrument = String(rec.instrumentClass || "").toUpperCase();
  const scope = String(rec.scopeClass || "").toUpperCase();

  if (instrument === "FORM_OF_AGREEMENT" || scope === "FORM_WITHOUT_PROPERTY" && instrument === "FORM_OF_AGREEMENT") {
    return DOCUMENT_EXECUTION_CLASS.FORM_AGREEMENT;
  }
  if (instrument === "DRAFT_OR_PROPOSED") {
    return DOCUMENT_EXECUTION_CLASS.DRAFT_OR_PROPOSED_AGREEMENT;
  }
  if (
    instrument === "AMENDMENT" ||
    instrument === "RENEWAL_OR_EXTENSION_INSTRUMENT" ||
    instrument === "TERMINATION_INSTRUMENT"
  ) {
    return DOCUMENT_EXECUTION_CLASS.AMENDMENT_OR_EXTENSION_INSTRUMENT;
  }
  if (instrument === "EXECUTED_AGREEMENT" || /\bthis\s+agreement\s+(?:is\s+)?(?:made|entered)/i.test(rec.textPreview || "")) {
    if (scope === "NAMED_HOTEL" && rec.hotelAttributionAllowed) {
      return DOCUMENT_EXECUTION_CLASS.EXECUTED_NAMED_HOTEL_AGREEMENT;
    }
    if (scope === "PORTFOLIO_LEVEL") {
      return DOCUMENT_EXECUTION_CLASS.EXECUTED_PORTFOLIO_AGREEMENT;
    }
  }
  if (scope === "PORTFOLIO_LEVEL") {
    return DOCUMENT_EXECUTION_CLASS.EXECUTED_PORTFOLIO_AGREEMENT;
  }
  if (scope === "FORM_WITHOUT_PROPERTY") {
    return DOCUMENT_EXECUTION_CLASS.FORM_AGREEMENT;
  }
  if (scope === "NAMED_HOTEL" && rec.hotelAttributionAllowed) {
    // Named hotel but instrument unknown — do not invent EXECUTED
    return DOCUMENT_EXECUTION_CLASS.EXECUTION_STATUS_UNRESOLVED;
  }
  return DOCUMENT_EXECUTION_CLASS.EXECUTION_STATUS_UNRESOLVED;
}

/**
 * Persistence policy: what Airtable destinations are allowed.
 */
export function persistencePolicyForExecutionClass(executionClass) {
  switch (executionClass) {
    case DOCUMENT_EXECUTION_CLASS.FORM_AGREEMENT:
    case DOCUMENT_EXECUTION_CLASS.DRAFT_OR_PROPOSED_AGREEMENT:
      return {
        createContractIdentity: false,
        createMilestone: false,
        createFinding: true,
        createInventory: false,
        inventHotelAttribution: false,
        reason: "form_or_draft_document_evidence_only",
      };
    case DOCUMENT_EXECUTION_CLASS.EXECUTED_PORTFOLIO_AGREEMENT:
      return {
        createContractIdentity: false,
        createMilestone: false,
        createFinding: true,
        createInventory: false,
        inventHotelAttribution: false,
        reason: "portfolio_agreement_no_hotel_attribution",
      };
    case DOCUMENT_EXECUTION_CLASS.AMENDMENT_OR_EXTENSION_INSTRUMENT:
      return {
        createContractIdentity: true, // only when named hotel extracted
        createMilestone: false, // only if verified end date later
        createFinding: true,
        createInventory: true, // only when named hotel extracted
        inventHotelAttribution: false,
        requiresNamedHotel: true,
        reason: "amendment_extension_requires_named_hotel_for_contract_row",
      };
    case DOCUMENT_EXECUTION_CLASS.EXECUTED_NAMED_HOTEL_AGREEMENT:
      return {
        createContractIdentity: true,
        createMilestone: false,
        createFinding: true,
        createInventory: true,
        inventHotelAttribution: false,
        requiresNamedHotel: true,
        reason: "executed_named_hotel_agreement",
      };
    default:
      return {
        createContractIdentity: false,
        createMilestone: false,
        createFinding: true,
        createInventory: false,
        inventHotelAttribution: false,
        reason: "execution_unresolved_document_evidence_only",
      };
  }
}

export function mapAgreementRole(instrumentClass, executionClass) {
  const i = String(instrumentClass || "").toUpperCase();
  if (i === "AMENDMENT") return "AMENDMENT";
  if (i === "RENEWAL_OR_EXTENSION_INSTRUMENT") return "RENEWAL_EXTENSION";
  if (i === "TERMINATION_INSTRUMENT") return "TERMINATION_INSTRUMENT";
  if (executionClass === DOCUMENT_EXECUTION_CLASS.EXECUTED_NAMED_HOTEL_AGREEMENT) return "ORIGINAL";
  return "ORIGINAL";
}

/**
 * Additive schema fields needed for clean document-vs-contract separation.
 * Do not apply without founder Meta authorization.
 */
export function buildDocumentExecutionSchemaProposal() {
  return {
    status: "PROPOSED_PENDING_FOUNDER_META_APPLY",
    baseId: "appCCUsuGsE1ifoLk",
    tables: {
      "HI Hotel Contracts": {
        addFields: [
          {
            name: "Document Execution Class",
            type: "singleSelect",
            choices: Object.values(DOCUMENT_EXECUTION_CLASS),
            purpose: "Distinguish form/draft/portfolio evidence from executed hotel agreements",
          },
          {
            name: "Scope Class",
            type: "singleSelect",
            choices: ["NAMED_HOTEL", "PORTFOLIO_LEVEL", "FORM_WITHOUT_PROPERTY", "UNRESOLVED"],
          },
          {
            name: "End Date Status Class",
            type: "singleSelect",
            choices: Object.values(END_DATE_STATUS),
          },
          {
            name: "Notice Period Days",
            type: "number",
          },
          {
            name: "Notice Deadline",
            type: "date",
          },
        ],
      },
      "HI Discovery Findings": {
        addFields: [
          {
            name: "Document Execution Class",
            type: "singleSelect",
            choices: Object.values(DOCUMENT_EXECUTION_CLASS),
          },
          {
            name: "Publication State",
            type: "singleSelect",
            choices: ["CUSTOMER_BLOCKED", "INTERNAL_ONLY", "DO_NOT_PUBLISH"],
          },
        ],
      },
    },
    interimPersistence:
      "Until Meta apply: encode Document Execution Class in Notes / Quarantine; Forms/Drafts/Portfolio → Findings only; named-hotel contracts use Agreement Role + Contract Status + Notes",
    stop: "REQUIRES_EXPLICIT_FOUNDER_META_API_AUTHORIZATION",
    publicationState: "CUSTOMER_BLOCKED",
  };
}

export default {
  classifyDocumentExecution,
  persistencePolicyForExecutionClass,
  mapAgreementRole,
  buildDocumentExecutionSchemaProposal,
  DOCUMENT_EXECUTION_CLASS,
  END_DATE_STATUS,
  PROSPECT_CLASS,
};
