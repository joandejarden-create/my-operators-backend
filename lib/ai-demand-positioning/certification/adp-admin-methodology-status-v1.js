/**
 * Admin methodology status classifications.
 * Distinct from Customer Ready / founder publication approval.
 *
 * Contract: ADP_ADMIN_METHODOLOGY_STATUS_V1
 */

export const ADP_ADMIN_METHODOLOGY_STATUS_V1 = "ADP_ADMIN_METHODOLOGY_STATUS_V1";

export const ADMIN_METHODOLOGY_STATUS = Object.freeze({
  VERIFIED_V1: "VERIFIED_V1",
  V1_CERTIFICATION_INCOMPLETE: "V1_CERTIFICATION_INCOMPLETE",
  LEGACY_METHODOLOGY: "LEGACY_METHODOLOGY",
  READY_FOR_V1_BASELINE_PUBLICATION: "READY_FOR_V1_BASELINE_PUBLICATION",
  METHODOLOGY_BLOCKED: "METHODOLOGY_BLOCKED",
});

/**
 * Map certification outcome → Admin methodology status.
 * Does NOT set Customer Ready.
 */
export function resolveAdminMethodologyStatus({
  publishedClassification = null,
  stagedCertOk = false,
  historicalEligibilityRecoverable = null,
  calculationMismatch = false,
  evidenceIncomplete = false,
  readyForPublication = false,
  blocked = false,
} = {}) {
  if (blocked || calculationMismatch) {
    return {
      status: ADMIN_METHODOLOGY_STATUS.METHODOLOGY_BLOCKED,
      customerReadyIndependent: true,
      note: "Methodology certification blocked; Customer Ready is a separate gate",
    };
  }
  if (readyForPublication && stagedCertOk) {
    return {
      status: ADMIN_METHODOLOGY_STATUS.READY_FOR_V1_BASELINE_PUBLICATION,
      customerReadyIndependent: true,
      note: "V1 baseline certified locally; still requires founder publication auth + Customer Ready checks",
    };
  }
  if (publishedClassification === "VERIFIED_INDEPENDENT_UNPROMPTED_V1") {
    return {
      status: ADMIN_METHODOLOGY_STATUS.VERIFIED_V1,
      customerReadyIndependent: true,
    };
  }
  if (publishedClassification === "LEGACY_MENTION_ALL_CORE") {
    return {
      status: ADMIN_METHODOLOGY_STATUS.LEGACY_METHODOLOGY,
      customerReadyIndependent: true,
      note: stagedCertOk
        ? "Legacy current; staged V1 candidate may be READY_FOR_V1_BASELINE_PUBLICATION"
        : "Legacy current; staged V1 not certified",
    };
  }
  if (
    publishedClassification === "DECLARED_V1_NOT_REPRODUCIBLE" ||
    historicalEligibilityRecoverable === false ||
    evidenceIncomplete
  ) {
    return {
      status: ADMIN_METHODOLOGY_STATUS.V1_CERTIFICATION_INCOMPLETE,
      customerReadyIndependent: true,
      note: "Declares V1 but independent certification incomplete",
    };
  }
  return {
    status: ADMIN_METHODOLOGY_STATUS.METHODOLOGY_BLOCKED,
    customerReadyIndependent: true,
    note: "Unrecognized methodology certification state",
  };
}
