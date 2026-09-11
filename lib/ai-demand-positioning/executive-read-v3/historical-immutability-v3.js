/**
 * ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY
 * Previously issued reports keep prior composition; no silent in-place mutation.
 */

export const ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY =
  "ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY";

export const HISTORICAL_IMMUTABILITY_POLICY_V3 = Object.freeze({
  rule: ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY,
  previouslyIssuedReports: "IMMUTABLE_PRIOR_COMPOSITION",
  nonDistributedCurrentReport: "MAY_RECEIVE_V3_ONLY_VIA_NEW_IMMUTABLE_EDITION",
  futureReports: "V3_DEFAULT_ONLY_AFTER_ACTIVATION",
  silentInPlaceMutation: "PROHIBITED",
  activationRequiredForCustomerDefault: true,
});

/**
 * Decide whether a V3 composition may be written onto a report record.
 * Never mutates issued history in place.
 *
 * @param {{
 *   distributionStatus?: 'ISSUED'|'SIGNED_SHARE'|'PUBLISHED_CURRENT'|'INTERNAL_ONLY'|'DRAFT',
 *   existingCompositionVersion?: string|null,
 *   v3Activated?: boolean,
 *   allowNewEdition?: boolean,
 * }} opts
 */
export function resolveExecutiveReadCompositionWritePolicyV3(opts = {}) {
  const {
    distributionStatus = "INTERNAL_ONLY",
    existingCompositionVersion = null,
    v3Activated = false,
    allowNewEdition = false,
  } = opts;

  const issued =
    distributionStatus === "ISSUED" || distributionStatus === "SIGNED_SHARE";

  if (issued) {
    return {
      allowed: false,
      mode: "REJECT_IN_PLACE_MUTATION",
      reason: "PREVIOUSLY_ISSUED_REPORT_IMMUTABLE",
      policy: HISTORICAL_IMMUTABILITY_POLICY_V3,
      enforced: true,
    };
  }

  if (existingCompositionVersion && existingCompositionVersion !== "ADP_EXECUTIVE_READ_COMPOSITION_V3") {
    if (!allowNewEdition) {
      return {
        allowed: false,
        mode: "REQUIRE_NEW_EDITION",
        reason: "COMPOSITION_VERSION_CHANGE_NOT_SILENT_HISTORY_MUTATION",
        policy: HISTORICAL_IMMUTABILITY_POLICY_V3,
        enforced: true,
      };
    }
    return {
      allowed: true,
      mode: "NEW_IMMUTABLE_EDITION",
      reason: "NON_DISTRIBUTED_OR_EXPLICIT_EDITION",
      policy: HISTORICAL_IMMUTABILITY_POLICY_V3,
      enforced: true,
      customerDefault: false,
    };
  }

  // Attach as inactive structured field is always allowed for foundation wiring
  return {
    allowed: true,
    mode: v3Activated ? "CUSTOMER_DEFAULT_V3" : "INACTIVE_STRUCTURED_ATTACH",
    reason: v3Activated ? "ACTIVATED" : "FOUNDATION_WIRING_ONLY",
    policy: HISTORICAL_IMMUTABILITY_POLICY_V3,
    enforced: true,
    customerDefault: v3Activated === true,
  };
}

/**
 * Merge V3 onto executiveRead without overwriting production writeup/ux.
 */
export function attachInactiveExecutiveReadV3(executiveRead, v3Payload, writePolicy) {
  const er = executiveRead && typeof executiveRead === "object" ? { ...executiveRead } : {};
  if (!writePolicy?.allowed) {
    return {
      executiveRead: er,
      attached: false,
      writePolicy,
    };
  }

  er.compositionV3 = {
    ...v3Payload,
    activated: false,
    customerFacingDefault: false,
    attachMode: writePolicy.mode,
  };

  // Explicit compositionVersion stamp for payload versioning (inactive)
  er.compositionVersionPreview = v3Payload.compositionVersion;
  er.compositionVersions = Array.from(
    new Set([...(er.compositionVersions || []), v3Payload.compositionVersion].filter(Boolean))
  );

  return { executiveRead: er, attached: true, writePolicy };
}
