/**
 * Permanent ADP publication workflow — one path for every hotel.
 *
 * MEASURE → VALIDATE EVIDENCE → CALCULATE (V1) → CERTIFY →
 * GENERATE REPORT+PDF → VERIFY CUSTOMER SURFACES → PUBLISH → VERIFY SHARES
 *
 * NEW measurements must use Independent Unprompted V1 only.
 * Historical legacy reads remain supported; legacy is disabled for NEW runs.
 *
 * Contract: ADP_PERMANENT_PUBLICATION_WORKFLOW_V1
 */

import {
  ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1,
  getIndependentUnpromptedMethodologyContract,
} from "./adp-independent-unprompted-methodology-contract-v1.js";
import { certifyIndependentUnpromptedPublicationV1 } from "./adp-independent-unprompted-publication-cert-v1.js";
import { assessMethodologyCompatibility } from "./adp-methodology-compatibility-contract-v1.js";

export const ADP_PERMANENT_PUBLICATION_WORKFLOW_V1 =
  "ADP_PERMANENT_PUBLICATION_WORKFLOW_V1";

export const PUBLICATION_WORKFLOW_STEPS = Object.freeze([
  "MEASURE_OR_REUSE_VALID_EVIDENCE",
  "VERIFY_HOTEL_ATTRIBUTION",
  "VALIDATE_EVIDENCE",
  "CALCULATE_CANONICAL_V1",
  "INDEPENDENTLY_CERTIFY",
  "GENERATE_REPORT_AND_PDF",
  "FREEZE_APPROVED_PACKAGE",
  "VERIFY_CUSTOMER_FACING_OUTPUTS",
  "PUBLISH_EXACT_APPROVED_PACKAGE",
  "VERIFY_ADMIN_AND_EXTERNAL_SHARES",
]);

/** Hard rules permanently enforced by this workflow. */
export const PUBLICATION_WORKFLOW_HARD_RULES = Object.freeze([
  "Reuse archived prompts/responses when evidence is complete and trustworthy",
  "Consideration numerator = detectPropertyMention on original response text only",
  "Runtime observations[].mentioned flags are non-authoritative",
  "Missing response text fails certification (no invented observations)",
  "One certified snapshot feeds Admin, customer report, PDF, and external shares",
  "Approve freezes exact content + presentation package; publish promotes that package only",
  "Do not regenerate or reinterpret approved content during publication",
  "Do not silently replace historical periods/PDFs",
  "Paid research only with explicit founder authorization",
  "Production publish only with separate founder authorization",
  "ADP_DURABLE_PUBLISHED_MODE=prefer — durable registry authoritative for current",
]);

/** Legacy methodology IDs — allowed for historical READ only, never NEW measurement. */
export const LEGACY_METHODOLOGY_IDS_READ_ONLY = Object.freeze([
  "ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1",
  null,
  "",
]);

export function assertNewMeasurementUsesIndependentUnpromptedV1(methodologyId) {
  const id = String(methodologyId || "").trim();
  if (id !== ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1) {
    const err = new Error(
      `NEW ADP measurement must use ${ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1}; got ${id || "(empty)"}`
    );
    err.code = "LEGACY_METHODOLOGY_DISABLED_FOR_NEW_MEASUREMENT";
    throw err;
  }
  return true;
}

/**
 * Fail-closed pre-publish gate for a candidate report.
 * Does not mutate production.
 */
export function runPermanentPublicationPreflight({
  propertyId,
  reportDoc,
  period = null,
  priorMethodologyId = null,
  pdfPeriodId = null,
  shareResolvesToPeriodId = null,
  founderAuthorized = false,
} = {}) {
  const blockers = [];
  const warnings = [];
  const contract = getIndependentUnpromptedMethodologyContract();

  if (!founderAuthorized) {
    blockers.push({
      code: "FOUNDER_AUTHORIZATION_REQUIRED",
      detail: "Publication requires explicit founder authorization phrase",
    });
  }

  let cert;
  try {
    cert = certifyIndependentUnpromptedPublicationV1({
      propertyId,
      reportDoc,
      period,
      requireFrozenIdsInReport: false,
      allowFreezeFromLiveContract: true,
    });
  } catch (err) {
    blockers.push({
      code: "CERTIFICATION_THREW",
      detail: String(err?.message || err),
    });
    cert = null;
  }

  if (cert && !cert.ok) {
    for (const b of cert.blockers || []) blockers.push(b);
  }

  const payload = reportDoc?.payload || reportDoc || {};
  const currentPeriod =
    payload?.period?.periodId || period?.periodId || null;
  if (pdfPeriodId && currentPeriod && pdfPeriodId !== currentPeriod) {
    blockers.push({
      code: "PDF_PERIOD_MISMATCH",
      detail: `PDF period ${pdfPeriodId} ≠ report period ${currentPeriod}`,
    });
  }
  if (
    shareResolvesToPeriodId &&
    currentPeriod &&
    shareResolvesToPeriodId !== currentPeriod
  ) {
    blockers.push({
      code: "SHARE_PERIOD_MISMATCH",
      detail: `Share resolves to ${shareResolvesToPeriodId} ≠ ${currentPeriod}`,
    });
  }

  const compat = assessMethodologyCompatibility({
    priorMethodologyId,
    currentMethodologyId: ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  });
  if (compat.status === "INCOMPATIBLE") {
    warnings.push({
      code: "METHODOLOGY_TRANSITION",
      detail: "MoM performance delta must be suppressed at cutover",
      customerFraming: compat.customerFraming,
    });
  }

  return {
    contract: ADP_PERMANENT_PUBLICATION_WORKFLOW_V1,
    methodologyContract: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1,
    approvedMethodologyId: ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
    steps: PUBLICATION_WORKFLOW_STEPS,
    ok: blockers.length === 0,
    blockers,
    warnings,
    certification: cert,
    compatibility: compat,
    requiredStampFields: contract.requiredPublicationFields,
  };
}

export function getPermanentPublicationWorkflowDescription() {
  return {
    contract: ADP_PERMANENT_PUBLICATION_WORKFLOW_V1,
    steps: PUBLICATION_WORKFLOW_STEPS,
    approvedMethodologyId: ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
    durableMode: "prefer",
    failClosedCodes: [
      "METHODOLOGY_INCORRECT",
      "ELIGIBLE_ID_REGISTER_MISSING",
      "PROMPTS_MISSING_FROM_ARCHIVE",
      "NUMERATOR_NOT_REPRODUCED",
      "DENOMINATOR_NOT_REPRODUCED",
      "CONSIDERATION_INCONSISTENT",
      "SOURCE_EVIDENCE_INSUFFICIENT",
      "PDF_PERIOD_MISMATCH",
      "SHARE_PERIOD_MISMATCH",
      "FOUNDER_AUTHORIZATION_REQUIRED",
    ],
    historicalLegacyReadSupport: true,
    legacyDisabledForNewMeasurement: true,
  };
}
