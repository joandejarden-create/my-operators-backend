/**
 * Immutable report package certification — built only from certified canonical metrics.
 */

import { createHash } from "crypto";
import { auditProviderPresenceGrain } from "../../metrics/adp-provider-presence-grain-lock-v1.js";
import {
  ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
  CERTIFICATION_FAILURE_STATES,
  CERTIFICATION_LIFECYCLE_STATES,
} from "./adp-permanent-full-e2e-lifecycle-states-v1.js";
import { ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1 } from "./adp-measurement-reconciliation-certification-v1.js";

export const ADP_IMMUTABLE_PACKAGE_CERTIFICATION_V1 =
  "ADP_IMMUTABLE_PACKAGE_CERTIFICATION_V1";

const REQUIRED_FIELDS = Object.freeze([
  "propertyId",
  "measurementPeriodId",
  "certificationId",
  "certificationHash",
  "methodologyFingerprintSha256",
  "frozenEligibilityManifest",
  "canonicalMetricPopulationManifest",
  "certifiedConsideration",
  "providerMetrics",
  "scenarioMetrics",
  "reportContentHash",
]);

export function certifyAdpImmutablePackage(input = {}) {
  const failures = [];
  const pkg = input.package || input.packagePayload || input;
  const measurementCert = input.measurementCertification || null;
  const propertyId = pkg.propertyId || input.propertyId;
  const periodId =
    pkg.periodId ||
    pkg.period?.periodId ||
    pkg.measurementPeriodId ||
    input.measurementPeriodId;

  const stamp = pkg._adpPermanentCertification || pkg._certification || {};
  const frozenIds =
    pkg.executiveMetrics?.considerationRate?.eligibleScenarioIds ||
    measurementCert?.canonicalMetrics?.population?.eligibleScenarioIds ||
    [];

  const manifest = {
    propertyId,
    measurementPeriodId: periodId,
    certificationId:
      stamp.certificationId ||
      stamp.certificationHash ||
      measurementCert?.certificationHash ||
      null,
    certificationHash:
      stamp.certificationHash || measurementCert?.certificationHash || null,
    methodologyFingerprintSha256:
      pkg.methodologyFingerprintSha256 ||
      stamp.methodologyFingerprintSha256 ||
      null,
    contentVersion: pkg.reportContentVersion || pkg.contentVersion || null,
    presentationVersion: pkg.presentationBundleVersion || null,
    reportTemplateVersion: pkg.reportTemplateVersion || null,
    frozenEligibilityManifest: {
      eligibleScenarioIds: frozenIds,
      count: Array.isArray(frozenIds) ? frozenIds.length : 0,
    },
    canonicalMetricPopulationManifest:
      measurementCert?.populationDenominatorManifest ||
      pkg._adpPermanentCertification?.populationManifest ||
      null,
    certifiedConsideration: pkg.executiveMetrics?.considerationRate || null,
    providerMetrics: pkg.evidence?.providers || null,
    scenarioMetrics: pkg.executiveMetrics?.scenarioPresence || null,
    territoryMetrics: pkg.demandCapture || pkg.territoryMetrics || null,
    competitiveMetrics:
      pkg.competitiveRankingByTerritory || pkg.lostDemand || null,
    evidenceReferences: {
      periodId,
      providerGrainLock: pkg.evidence?.providerGrainLock || null,
    },
    reportContentHash: null,
    publicationManifestSha256: pkg.publicationManifestSha256 || null,
    pdfArtifactSha256: pkg.pdfArtifactSha256 || null,
  };

  const contentBasis = {
    propertyId,
    periodId,
    consideration: manifest.certifiedConsideration,
    providers: manifest.providerMetrics,
    scenarioPresence: manifest.scenarioMetrics,
    frozenIds,
  };
  manifest.reportContentHash = createHash("sha256")
    .update(JSON.stringify(contentBasis))
    .digest("hex");

  for (const field of REQUIRED_FIELDS) {
    const v = manifest[field];
    const empty =
      v == null ||
      (typeof v === "object" &&
        !Array.isArray(v) &&
        Object.keys(v).length === 0 &&
        field !== "canonicalMetricPopulationManifest") ||
      (Array.isArray(v) && v.length === 0 && field === "frozenEligibilityManifest");
    if (
      field === "frozenEligibilityManifest" &&
      (!manifest.frozenEligibilityManifest?.count ||
        !manifest.frozenEligibilityManifest.eligibleScenarioIds?.length)
    ) {
      failures.push({
        code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
        field,
        detail: "frozen eligibility missing",
      });
    } else if (
      field !== "canonicalMetricPopulationManifest" &&
      field !== "frozenEligibilityManifest" &&
      (v == null || v === "")
    ) {
      failures.push({
        code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
        field,
        detail: `Required package field missing: ${field}`,
      });
    }
  }

  if (measurementCert && measurementCert.ok !== true) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.METRIC_RECONCILIATION_FAILED,
      detail: "Package cannot be certified without passing measurement reconciliation",
    });
  }

  if (
    measurementCert?.certificationHash &&
    stamp.certificationHash &&
    measurementCert.certificationHash !== stamp.certificationHash
  ) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
      detail: "Package certificationHash drifted from measurement certification",
    });
  }

  const grain = auditProviderPresenceGrain(pkg);
  if (!grain.pass) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
      detail: "Package Provider Presence grain audit failed",
    });
  }

  // Independent recalculation forbidden marker
  const independentRecalc =
    pkg._adpIndependentMetricRecalculation === true ||
    pkg.evidence?.recalculatedOutsideCertification === true;
  if (independentRecalc) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
      detail: "Package claims independent metric recalculation outside certification",
    });
  }

  const ok = failures.length === 0;
  return {
    ok,
    verdict: ok ? "PACKAGE_CERTIFIED" : "FAILED",
    gate: ADP_IMMUTABLE_PACKAGE_CERTIFICATION_V1,
    measurementGate: ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1,
    contract: ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
    lifecycleState: ok
      ? CERTIFICATION_LIFECYCLE_STATES.PACKAGE_CERTIFIED
      : CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
    packageManifest: manifest,
    grainPass: grain.pass,
    failures,
    certificationTimestamp: new Date().toISOString(),
    rule: "Never silently mutate an approved package; create a new immutable version instead.",
  };
}
