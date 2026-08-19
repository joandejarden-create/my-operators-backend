/**
 * Brand benchmark read service — customer-safe + internal diagnostic payloads.
 * No provider calls.
 */

import { runBrandPresenceIndexPilot, getPilotSubjectResult, PILOT_VERSION } from "./brand-presence-index-pilot.js";
import { buildBenchmarkResponseForAccess } from "./access-redaction.js";
import { redactToCustomerAllowlist } from "./customer-payload.js";
import { auditPayloadForMethodologyLeaks } from "./access-redaction.js";
import { PEER_SET_ID_V5 } from "../peer-sets.js";

let cachedPilotReport = null;

function getPilotReport(opts = {}) {
  if (!cachedPilotReport || opts.refresh) {
    cachedPilotReport = runBrandPresenceIndexPilot({ writeReport: true, ...opts });
  }
  return cachedPilotReport;
}

export function getBrandBenchmarkPayload(opts = {}) {
  const brandId = String(opts.brandId || "").trim();
  if (!brandId) return { ok: false, error: "missing_brand_id" };

  const report = getPilotReport(opts);
  const subject = getPilotSubjectResult(brandId, { report });
  if (!subject) return { ok: false, error: "subject_not_in_pilot", brandId };

  const accessClass = opts.internalAdmin ? "INTERNAL_ADMIN" : "CUSTOMER_ENTITY";
  const customerPayload = redactToCustomerAllowlist(subject.customerPayload || {});
  // V1 pilot payload remains INTERNAL_REVIEW_ONLY. Scenario-cohort V2 is not customer-certified.

  if (accessClass === "INTERNAL_ADMIN") {
    const response = buildBenchmarkResponseForAccess({
      accessClass,
      viewerContext: { internalAdmin: true },
      subjectEntityId: brandId,
      customerPayload,
      indexResult: { indexValue: subject.aiPresenceIndex },
      benchmarkMembers: subject.internalPayload?.benchmarkMembers,
      allCompetitorScores: subject.internalPayload?.allCompetitorPresenceRates,
      cohortSelectionExplanation: subject.internalPayload?.cohortSelectionExplanation,
      datasetNamespace: "DEMO_VALIDATION",
    });
    return {
      ok: true,
      brandId,
      peerSetId: PEER_SET_ID_V5,
      pilotVersion: PILOT_VERSION,
      providerCalls: 0,
      ...response,
    };
  }

  const leakAudit = auditPayloadForMethodologyLeaks(customerPayload, { accessClass });
  if (!leakAudit.ok) {
    return { ok: false, error: "payload_leak_audit_failed", violations: leakAudit.violations };
  }

  return {
    ok: true,
    brandId,
    peerSetId: PEER_SET_ID_V5,
    pilotVersion: PILOT_VERSION,
    providerCalls: 0,
    accessClass,
    payload: customerPayload,
    redacted: true,
    FULL_PEER_MATRIX_EXPOSED: false,
    FULL_COMPETITOR_SCORES_EXPOSED: false,
  };
}

export function resetBenchmarkPilotCache() {
  cachedPilotReport = null;
}
