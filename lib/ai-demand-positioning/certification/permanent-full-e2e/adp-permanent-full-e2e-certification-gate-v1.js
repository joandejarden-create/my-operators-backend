/**
 * Permanent Full E2E enforcement — fail closed if certification missing/invalid.
 *
 * Enforcement points:
 *  - After measurement calculation (S8)
 *  - Before report generation (S9)
 *  - Before publication package approval
 *  - Before publication
 *  - After production publication
 *  - Before Customer Ready
 */

import { loadPeriod, loadPropertyProfile } from "../../data-model.js";
import {
  CERTIFICATION_FAILURE_STATES,
  CERTIFICATION_LIFECYCLE_STATES,
  ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
  canAdvanceTo,
} from "./adp-permanent-full-e2e-lifecycle-states-v1.js";
import { certifyAdpMeasurementReconciliation } from "./adp-measurement-reconciliation-certification-v1.js";
import { certifyAdpImmutablePackage } from "./adp-immutable-package-certification-v1.js";
import { certifyAdpCustomerVisibleParity } from "./adp-customer-visible-parity-certification-v1.js";

export const ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1 =
  "ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1";

export const ENFORCEMENT_POINTS = Object.freeze({
  AFTER_MEASUREMENT_CALCULATION: "AFTER_MEASUREMENT_CALCULATION",
  BEFORE_REPORT_GENERATION: "BEFORE_REPORT_GENERATION",
  BEFORE_PUBLICATION_PACKAGE_APPROVAL: "BEFORE_PUBLICATION_PACKAGE_APPROVAL",
  BEFORE_PUBLICATION: "BEFORE_PUBLICATION",
  AFTER_PRODUCTION_PUBLICATION: "AFTER_PRODUCTION_PUBLICATION",
  BEFORE_CUSTOMER_READY: "BEFORE_CUSTOMER_READY",
});

/**
 * Run the mandatory measurement reconciliation for a property/period.
 * Fail closed — returns ok:false with lifecycle failure state.
 */
export function enforceMeasurementReconciliationGate({
  propertyId,
  periodId,
  packagePayload = null,
  period = null,
  profile = null,
  enforcementPoint = ENFORCEMENT_POINTS.AFTER_MEASUREMENT_CALCULATION,
} = {}) {
  const resolvedProfile = profile || loadPropertyProfile(propertyId);
  let resolvedPeriod = period;
  if (!resolvedPeriod && periodId) {
    try {
      resolvedPeriod = loadPeriod(periodId);
    } catch {
      resolvedPeriod = null;
    }
  }
  if (!resolvedPeriod?.observations?.length) {
    return {
      ok: false,
      gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
      enforcementPoint,
      lifecycleState: CERTIFICATION_FAILURE_STATES.EVIDENCE_INCOMPLETE,
      failures: [
        {
          code: CERTIFICATION_FAILURE_STATES.EVIDENCE_INCOMPLETE,
          detail: "Period observations required for permanent certification",
        },
      ],
      blocksDownstream: true,
    };
  }

  const measurement = certifyAdpMeasurementReconciliation({
    propertyId,
    periodId: periodId || resolvedPeriod.periodId,
    observations: resolvedPeriod.observations,
    propertyProfile: resolvedProfile,
    packagePayload,
    methodologyFingerprintSha256:
      packagePayload?.methodologyFingerprintSha256 || null,
  });

  if (!measurement.ok) {
    return {
      ok: false,
      gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
      enforcementPoint,
      measurement,
      lifecycleState: measurement.lifecycleState,
      failures: measurement.failures,
      blocksDownstream: true,
    };
  }

  let packageCert = null;
  if (
    packagePayload &&
    (enforcementPoint === ENFORCEMENT_POINTS.BEFORE_PUBLICATION_PACKAGE_APPROVAL ||
      enforcementPoint === ENFORCEMENT_POINTS.BEFORE_PUBLICATION ||
      enforcementPoint === ENFORCEMENT_POINTS.BEFORE_CUSTOMER_READY)
  ) {
    packageCert = certifyAdpImmutablePackage({
      packagePayload,
      measurementCertification: measurement,
      propertyId,
      measurementPeriodId: periodId || resolvedPeriod.periodId,
    });
    if (!packageCert.ok) {
      return {
        ok: false,
        gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
        enforcementPoint,
        measurement,
        packageCert,
        lifecycleState: packageCert.lifecycleState,
        failures: packageCert.failures,
        blocksDownstream: true,
      };
    }
  }

  let parity = null;
  if (
    enforcementPoint === ENFORCEMENT_POINTS.AFTER_PRODUCTION_PUBLICATION ||
    enforcementPoint === ENFORCEMENT_POINTS.BEFORE_CUSTOMER_READY
  ) {
    parity = certifyAdpCustomerVisibleParity({
      propertyId,
      approvedPackage: packagePayload,
      expectedPeriodId: periodId || resolvedPeriod.periodId,
      surfaces: {}, // caller must supply live surfaces for full CUSTOMER_READY
    });
    // Without surfaces, package-level render safety still required
    if (!parity.checks?.approved_package_client_render?.ok) {
      return {
        ok: false,
        gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
        enforcementPoint,
        measurement,
        packageCert,
        parity,
        lifecycleState: CERTIFICATION_FAILURE_STATES.RENDER_VALIDATION_FAILED,
        failures: parity.failures,
        blocksDownstream: true,
      };
    }
  }

  return {
    ok: true,
    gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
    contract: ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
    enforcementPoint,
    measurement,
    packageCert,
    parity,
    lifecycleState: CERTIFICATION_LIFECYCLE_STATES.METRICS_RECONCILED,
    blocksDownstream: false,
    artifact: {
      certificationHash: measurement.certificationHash,
      certificationTimestamp: measurement.certificationTimestamp,
      required: true,
    },
  };
}

/**
 * Assert a Full E2E manifest cannot claim CUSTOMER_READY / APPROVED_FOR_PUBLICATION
 * without a permanent certification artifact.
 */
export function assertFullE2eManifestCertification(manifest = {}) {
  const failures = [];
  const cert =
    manifest.permanentCertification ||
    manifest.stageResults?.S8_MEASUREMENT_RECONCILIATION?.detail ||
    null;
  const claimsReady =
    manifest.customerReady === true ||
    manifest.state === "CUSTOMER_READY" ||
    manifest.publicationStatus === "APPROVED_FOR_PUBLICATION" ||
    manifest.publicationStatus === "PUBLISHED";

  if (claimsReady && (!cert || cert.ok !== true)) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
      detail:
        "CUSTOMER_READY / publication claim without passing permanent certification artifact",
    });
  }

  if (cert?.artifact?.required && !cert?.artifact?.certificationHash) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
      detail: "Certification artifact missing certificationHash",
    });
  }

  return {
    ok: failures.length === 0,
    failures,
    gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
  };
}

export function attachPermanentCertificationToManifest(manifest, gateResult) {
  const next = { ...(manifest || {}) };
  next.permanentCertification = {
    contract: ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
    gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
    ok: gateResult?.ok === true,
    enforcementPoint: gateResult?.enforcementPoint || null,
    certificationHash: gateResult?.artifact?.certificationHash || null,
    certificationTimestamp: gateResult?.measurement?.certificationTimestamp || null,
    lifecycleState: gateResult?.lifecycleState || null,
    failures: gateResult?.failures || [],
  };
  next.stageResults = { ...(next.stageResults || {}) };
  next.stageResults.S8_MEASUREMENT_RECONCILIATION = {
    stageId: "S8_MEASUREMENT_RECONCILIATION",
    status: gateResult?.ok ? "PASS" : "FAIL",
    detail: next.permanentCertification,
  };
  if (!gateResult?.ok) {
    next.customerReady = false;
    if (
      next.publicationStatus === "APPROVED_FOR_PUBLICATION" ||
      next.publicationStatus === "PUBLISHED"
    ) {
      next.publicationStatus = "BLOCKED_CERTIFICATION_FAILED";
    }
  }
  return next;
}

export {
  canAdvanceTo,
  CERTIFICATION_LIFECYCLE_STATES,
  CERTIFICATION_FAILURE_STATES,
  certifyAdpMeasurementReconciliation,
  certifyAdpImmutablePackage,
  certifyAdpCustomerVisibleParity,
};
