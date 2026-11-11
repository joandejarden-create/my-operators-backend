/**
 * ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1
 *
 * Automated gates may establish technical / content / visual readiness.
 * Only an explicit founder action may set FOUNDER_DESIGN_ACCEPTED.
 * Scripts must never auto-promote to FOUNDER_DESIGN_ACCEPTED.
 */

export const ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1 =
  "ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1";

export const PDF_TECHNICALLY_VALID = "PDF_TECHNICALLY_VALID";
export const PDF_CONTENT_PARITY_PASS = "PDF_CONTENT_PARITY_PASS";
export const PDF_VISUAL_QA_PASS = "PDF_VISUAL_QA_PASS";
export const PDF_BETHESDA_DESIGN_REVIEW_READY =
  "PDF_BETHESDA_DESIGN_REVIEW_READY";
export const PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING =
  "PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING";
export const FOUNDER_DESIGN_ACCEPTED = "FOUNDER_DESIGN_ACCEPTED";

/** States automation is allowed to assert. */
export const AUTOMATION_ALLOWED_ACCEPTANCE_STATES = Object.freeze([
  PDF_TECHNICALLY_VALID,
  PDF_CONTENT_PARITY_PASS,
  PDF_VISUAL_QA_PASS,
  PDF_BETHESDA_DESIGN_REVIEW_READY,
  PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
]);

/**
 * @param {{
 *   technicallyValid?: boolean,
 *   contentParityPass?: boolean,
 *   visualQaPass?: boolean,
 *   bethesdaDesignReviewReady?: boolean,
 *   founderAccepted?: boolean,
 *   founderAcceptedBy?: string|null,
 *   founderAcceptedAt?: string|null,
 *   founderAcceptedNote?: string|null,
 * }} [opts]
 */
export function assessExecutivePdfFounderDesignAcceptanceV1(opts = {}) {
  const states = [];
  if (opts.technicallyValid) states.push(PDF_TECHNICALLY_VALID);
  if (opts.contentParityPass) states.push(PDF_CONTENT_PARITY_PASS);
  if (opts.visualQaPass) states.push(PDF_VISUAL_QA_PASS);
  if (opts.bethesdaDesignReviewReady) {
    states.push(PDF_BETHESDA_DESIGN_REVIEW_READY);
  }

  const founderAccepted =
    opts.founderAccepted === true &&
    String(opts.founderAcceptedBy || "").trim().length > 0;

  if (founderAccepted) {
    return {
      contract: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
      status: FOUNDER_DESIGN_ACCEPTED,
      states: [...states, FOUNDER_DESIGN_ACCEPTED],
      publicationBlocked: false,
      automationMaySetFounderAccepted: false,
      founderAcceptedBy: String(opts.founderAcceptedBy).trim(),
      founderAcceptedAt: opts.founderAcceptedAt || null,
      founderAcceptedNote: opts.founderAcceptedNote || null,
    };
  }

  return {
    contract: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
    status: PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
    states: [...states, PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING],
    publicationBlocked: true,
    automationMaySetFounderAccepted: false,
    founderAcceptedBy: null,
    founderAcceptedAt: null,
    founderAcceptedNote: null,
  };
}

/**
 * Guard: reject any attempt by automation to claim founder acceptance.
 * @param {string} claimedStatus
 */
export function assertAutomationDidNotClaimFounderAcceptance(claimedStatus) {
  if (String(claimedStatus || "") === FOUNDER_DESIGN_ACCEPTED) {
    throw Object.assign(
      new Error("automation_cannot_set_FOUNDER_DESIGN_ACCEPTED"),
      {
        code: "ADP_EXECUTIVE_PDF_FOUNDER_ACCEPTANCE_FORBIDDEN",
        contract: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
      }
    );
  }
  return true;
}
