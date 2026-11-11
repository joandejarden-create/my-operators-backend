/**
 * Runtime + certification contract for Property Snapshot
 * `competitiveSet.topObservedAlternative`.
 *
 * Distinguishes:
 * - Missing mandatory parent (`competitiveSet`) on Core owner reports → FAIL
 * - Present parent with null/absent leaf → valid absence (safe "—" render)
 * - Loyalty measurement artifacts mis-selected as Core → FAIL
 * - Cross-hotel contamination of top alternative identity → FAIL when subject ids mismatch
 */

import {
  isCoreOwnerReportPayload,
  isLoyaltyMeasurementArtifact,
} from "../review/adp-staged-report-store-v1.js";

export const ADP_TOP_OBSERVED_ALTERNATIVE_RUNTIME_CONTRACT_V1 =
  "adp_top_observed_alternative_runtime_contract_v1";

/**
 * Safe extract for SPA-equivalent binding (never throws).
 */
export function extractTopObservedAlternativeSafe(payload) {
  const cs =
    payload && payload.competitiveSet && typeof payload.competitiveSet === "object"
      ? payload.competitiveSet
      : null;
  const top =
    (cs && cs.topObservedAlternative && cs.topObservedAlternative.name) ||
    (cs && Array.isArray(cs.observed) && cs.observed[0] && cs.observed[0].name) ||
    null;
  return {
    competitiveSetPresent: !!cs,
    topObservedAlternative: cs?.topObservedAlternative || null,
    displayName: top || "—",
    validAbsence: !!cs && !top,
    missingParent: !cs,
  };
}

/**
 * Certify a payload intended for Core ADP customer/review render.
 */
export function certifyTopObservedAlternativeRuntimeContract(payload, opts = {}) {
  const propertyId = opts.propertyId || payload?.property?.propertyId || payload?.propertyId || null;
  const expectedPropertyId = opts.expectedPropertyId || propertyId;
  const issues = [];

  if (isLoyaltyMeasurementArtifact(payload, opts.certification)) {
    issues.push({
      severity: "P0",
      code: "LOYALTY_ARTIFACT_SELECTED_AS_CORE_REPORT",
      message:
        "Loyalty measurement artifact lacks competitiveSet; must not drive Core Property Snapshot render",
    });
    return {
      ok: false,
      version: ADP_TOP_OBSERVED_ALTERNATIVE_RUNTIME_CONTRACT_V1,
      issues,
      extract: extractTopObservedAlternativeSafe(payload),
    };
  }

  if (!isCoreOwnerReportPayload(payload, opts.certification)) {
    issues.push({
      severity: "P0",
      code: "NOT_CORE_OWNER_REPORT_SHAPE",
      message: "Payload is not a Core owner report (missing demandCapture/executiveMetrics/competitiveSet)",
    });
  }

  const extract = extractTopObservedAlternativeSafe(payload);
  if (extract.missingParent) {
    issues.push({
      severity: "P0",
      code: "MISSING_COMPETITIVE_SET_PARENT",
      message:
        "competitiveSet parent object missing — renderExecKpis cannot bind topObservedAlternative; certification fail-closed",
    });
  }

  // Valid absence of leaf is OK when parent exists.
  if (extract.competitiveSetPresent && extract.validAbsence) {
    // no issue — optional competitor absence
  }

  // Cross-hotel contamination: payload property id must match expected.
  const payloadPid =
    payload?.property?.propertyId || payload?.propertyId || null;
  if (
    expectedPropertyId &&
    payloadPid &&
    String(expectedPropertyId) !== String(payloadPid)
  ) {
    issues.push({
      severity: "P0",
      code: "CROSS_HOTEL_PAYLOAD_CONTAMINATION",
      message: `Expected ${expectedPropertyId} but payload propertyId=${payloadPid}`,
    });
  }

  const topEntityPropertyHint =
    extract.topObservedAlternative?.entityId ||
    extract.topObservedAlternative?.propertyId ||
    null;
  if (
    expectedPropertyId &&
    topEntityPropertyHint &&
    String(topEntityPropertyHint) === String(expectedPropertyId)
  ) {
    issues.push({
      severity: "P0",
      code: "TOP_ALTERNATIVE_IS_SUBJECT",
      message: "topObservedAlternative must not be the subject hotel",
    });
  }

  return {
    ok: issues.filter((i) => i.severity === "P0").length === 0,
    version: ADP_TOP_OBSERVED_ALTERNATIVE_RUNTIME_CONTRACT_V1,
    issues,
    extract,
    propertyId: payloadPid,
  };
}

/**
 * Simulate the SPA binding that previously threw when competitiveSet was undefined.
 * Returns { threw, displayName } for regression assertions.
 */
export function simulateRenderExecKpisTopAltBinding(payload) {
  try {
    const d = payload || {};
    const cs =
      d.competitiveSet && typeof d.competitiveSet === "object"
        ? d.competitiveSet
        : null;
    const topAlt =
      (cs && cs.topObservedAlternative && cs.topObservedAlternative.name) ||
      (cs && cs.observed && cs.observed.length ? cs.observed[0].name : null) ||
      null;
    return { threw: false, displayName: topAlt || "—", competitiveSetPresent: !!cs };
  } catch (err) {
    return { threw: true, error: err?.message || String(err), displayName: null };
  }
}
