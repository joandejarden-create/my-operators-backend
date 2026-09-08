/**
 * Promote to Paid ADP Pilot — Phase 1 confirmation stub.
 * Does not create or overwrite production ADP monitoring records.
 */

import { REQUEST_STATUS } from "./schema-v1.js";

export const PROMOTE_CONFIRMATION_TEXT =
  "This will create or connect this hotel to a paid ADP monitoring workflow. Free audit records will remain separate. No production records will be overwritten.";

/**
 * @param {object} store
 * @param {object} options
 * @param {string} options.requestId
 * @param {boolean} options.confirmed — must be true
 * @param {string} [options.confirmedBy]
 */
export function promoteLeakAuditToPilotStub(store, options = {}) {
  if (options.confirmed !== true) {
    const err = new Error("promotion_requires_explicit_confirmation");
    err.code = "PROMOTION_CONFIRMATION_REQUIRED";
    err.confirmationText = PROMOTE_CONFIRMATION_TEXT;
    throw err;
  }

  const request = store.getRequest(options.requestId);
  if (!request) throw new Error("request_not_found");

  const runs = store.listRuns({ auditRequestId: request.id });
  const latestRun = runs[0] || null;
  const reports = latestRun ? store.listReports({ auditRunId: latestRun.id }) : [];
  const latestReport = reports[0] || null;

  const promotion = store.createPromotionStub({
    auditRequestId: request.id,
    auditRunId: latestRun?.id || null,
    reportId: latestReport?.id || null,
    confirmed: true,
    confirmedBy: options.confirmedBy || "admin",
  });

  const updated = store.updateRequest(request.id, {
    status: REQUEST_STATUS.CONVERTED,
    promotionStubId: promotion.id,
  });

  return {
    ok: true,
    confirmationText: PROMOTE_CONFIRMATION_TEXT,
    productionAdpRecordCreated: false,
    request: updated,
    promotion,
  };
}
