/**
 * Surfe transient policy V1 — click-only, never durable PII.
 */

export const SURFE_TRANSIENT_POLICY_V1 = "surfe-transient-policy-v1";

export const SURFE_POLICY = Object.freeze({
  userInitiatedOnly: true,
  persistEmail: false,
  persistTelephone: false,
  autoDuringGdiE2E: false,
  autoDuringHeE2E: false,
  allowNonPiiUsageMetadata: true,
  countsTowardDurableContactability: false,
});

/**
 * Validate that a channel / result does not claim Surfe durability.
 */
export function assertSurfeTransientOnly(channelOrResult = {}) {
  const provider = String(
    channelOrResult.provider || channelOrResult.sourceProvider || "",
  ).toLowerCase();
  const isSurfe = /surfe/.test(provider) || channelOrResult.surfeDerived === true;
  if (!isSurfe) {
    return {
      ok: true,
      status: "PASS",
      gate: "CONTACT_SURFE_TRANSIENT_ONLY",
      reason: "NOT_SURFE",
    };
  }
  const persisted =
    channelOrResult.persisted === true ||
    channelOrResult.airtableId ||
    channelOrResult.canonicalAirtableRecordId ||
    channelOrResult.sessionOnly === false;
  if (persisted && channelOrResult.sessionOnly !== true) {
    return {
      ok: false,
      status: "FAIL",
      gate: "CONTACT_SURFE_TRANSIENT_ONLY",
      reason: "SURFE_PII_MUST_NOT_PERSIST",
    };
  }
  return {
    ok: true,
    status: "PASS",
    gate: "CONTACT_SURFE_TRANSIENT_ONLY",
    reason: "SESSION_ONLY",
    policy: SURFE_POLICY,
  };
}

export function surfeCountsTowardContactability() {
  return false;
}

export default {
  SURFE_TRANSIENT_POLICY_V1,
  SURFE_POLICY,
  assertSurfeTransientOnly,
  surfeCountsTowardContactability,
};
