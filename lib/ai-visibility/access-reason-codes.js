/**
 * AI Visibility access reason codes (deterministic; safe for logs).
 * Client-facing errors should map to generic forbidden messages.
 */

export const ACCESS_REASON = Object.freeze({
  ENTITLED_BRAND: "ENTITLED_BRAND",
  ENTITLED_BRAND_PORTFOLIO: "ENTITLED_BRAND_PORTFOLIO",
  PEER_BRAND_COMPARATIVE: "PEER_BRAND_COMPARATIVE",
  ENTITLED_OPERATOR: "ENTITLED_OPERATOR",
  PEER_OPERATOR_COMPARATIVE: "PEER_OPERATOR_COMPARATIVE",
  ENTITLED_DEAL: "ENTITLED_DEAL",
  ENTITLED_HOTEL_ASSET: "ENTITLED_HOTEL_ASSET",
  ADMIN_OVERRIDE: "ADMIN_OVERRIDE",
  WORKSPACE_MISMATCH: "WORKSPACE_MISMATCH",
  SUBJECT_NOT_ENTITLED: "SUBJECT_NOT_ENTITLED",
  SUBJECT_NOT_FOUND: "SUBJECT_NOT_FOUND",
  SUBJECT_TYPE_UNSUPPORTED: "SUBJECT_TYPE_UNSUPPORTED",
  VIEWER_REQUIRED: "VIEWER_REQUIRED",
  INVALID_SUBJECT: "INVALID_SUBJECT",
});

export const ACCESS_REASON_VERSION = "ai_visibility_access_reason_v1";

/** Map internal reason → client-safe error code (no entitlement details). */
export function toClientAccessError(reasonCode) {
  if (
    reasonCode === ACCESS_REASON.SUBJECT_NOT_FOUND ||
    reasonCode === ACCESS_REASON.INVALID_SUBJECT ||
    reasonCode === ACCESS_REASON.SUBJECT_TYPE_UNSUPPORTED
  ) {
    return { error: "not_found", message: "Requested intelligence subject was not found." };
  }
  if (reasonCode === ACCESS_REASON.VIEWER_REQUIRED) {
    return { error: "authentication_required", message: "Authentication required." };
  }
  return { error: "forbidden", message: "You do not have access to this intelligence subject." };
}
