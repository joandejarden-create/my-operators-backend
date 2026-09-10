/**
 * ADP_FINAL_TRUST_RECOVERY_POINT
 * Freeze before final trust closure / recertification / deploy.
 * Does not mutate raw responses or methodology.
 */

export const ADP_FINAL_TRUST_RECOVERY_POINT_ID = "ADP_FINAL_TRUST_RECOVERY_POINT";

export const ADP_FINAL_TRUST_CLOSURE_MODE =
  "ADP_FINAL_TRUST_CLOSURE_FULL_UNIVERSE_RECERT_PRODUCTION_PARITY_EXTERNAL_LINK_RELEASE";

export const ADP_EXISTING_CLIENT_URL_IMMUTABLE = "ADP_EXISTING_CLIENT_URL_IMMUTABLE";

export const SINGLE_CANONICAL_DISPLACEMENT_AGGREGATOR =
  "SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET";

/** @deprecated Alias kept for older recovery docs — same value as aggregator. */
export const SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET =
  SINGLE_CANONICAL_DISPLACEMENT_AGGREGATOR;

export const ADP_PRINT_CONTROL_HIDDEN_UNTIL_CLIENT_RELEASE =
  "ADP_PRINT_CONTROL_HIDDEN_UNTIL_CLIENT_RELEASE";

/** Lifted only when ADP_END_TO_END_ANALYTICAL_INTEGRITY_V1 === TRUSTED */
export function buildExternalDistributionHoldState({ trusted = false } = {}) {
  if (trusted) {
    return Object.freeze({
      status: "LIFTED",
      reason:
        "ADP end-to-end analytical integrity TRUSTED; existing current_published URLs preserved; new shares only where none exist.",
      allowExistingUrls: true,
      allowNewExternalCommunications: false,
      allowNewClientReadyEditions: true,
      clientReadyStatus: "CLIENT_READY",
    });
  }
  return Object.freeze({
    status: "HOLD",
    reason: "CLIENT_READY_PENDING_FORENSIC_REVALIDATION",
    allowExistingUrls: true,
    allowNewExternalCommunications: false,
    allowNewClientReadyEditions: false,
    clientReadyStatus: "CLIENT_READY_PENDING_FORENSIC_REVALIDATION",
  });
}
