#!/usr/bin/env node
/**
 * Reconcile all 38 certified hotels under the permanent certification contract.
 * Prepare corrected immutable packages for Group B locally — DO NOT publish.
 *
 * No paid research / Airtable writes / production deploy / share rotation.
 */
import { mkdirSync, writeFileSync, readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  certifyAdpMeasurementReconciliation,
  buildCorrectedPackageFromCertification,
} from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-measurement-reconciliation-certification-v1.js";
import { certifyAdpImmutablePackage } from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-immutable-package-certification-v1.js";
import { certifyAdpCustomerVisibleParity } from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-customer-visible-parity-certification-v1.js";
import { auditProviderPresenceGrain } from "../lib/ai-demand-positioning/metrics/adp-provider-presence-grain-lock-v1.js";

const OUT = "reports/dealality-adp-permanent-full-e2e-certification-v1";
const PKG_OUT = join(OUT, "prepared-group-b-packages");
const CURRENT = "data/adp-durable-published/current";
const SNAP = "data/adp-durable-published/snapshots";

const GROUP_A = new Set(
  JSON.parse(
    readFileSync(
      "reports/dealality-adp-minimum-necessary-remeasurement-assessment-v1/02-summary.json",
      "utf8"
    )
  ).summary.groupA
);
const GROUP_B = new Set(
  JSON.parse(
    readFileSync(
      "reports/dealality-adp-minimum-necessary-remeasurement-assessment-v1/02-summary.json",
      "utf8"
    )
  ).summary.groupB
);

mkdirSync(PKG_OUT, { recursive: true });

const hotels = readdirSync(CURRENT).filter((d) => d.startsWith("adp_"));
const matrix = [];
const groupBCerts = [];

for (const propertyId of hotels) {
  const man = JSON.parse(
    readFileSync(join(CURRENT, propertyId, "manifest.json"), "utf8")
  );
  const wrap = JSON.parse(
    readFileSync(join(SNAP, propertyId, man.reportFile), "utf8")
  );
  const payload = wrap.payload || wrap;
  const period = loadPeriod(man.latestPeriodId);
  const profile = loadPropertyProfile(propertyId);
  const group = GROUP_A.has(propertyId)
    ? "A"
    : GROUP_B.has(propertyId)
      ? "B"
      : "UNKNOWN";

  const bakedGrain = auditProviderPresenceGrain(payload);
  const measurement = certifyAdpMeasurementReconciliation({
    propertyId,
    periodId: man.latestPeriodId,
    observations: period?.observations || [],
    propertyProfile: profile,
    // For Group A: certify against baked package.
    // For Group B: first certify evidence-only (no package constraints that fail grain),
    // then build corrected package and re-certify.
    packagePayload: group === "A" ? payload : null,
    frozenEligibleScenarioIds:
      payload.executiveMetrics?.considerationRate?.eligibleScenarioIds || [],
    methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
    methodologyId: wrap.methodologyId,
  });

  let corrected = null;
  let measurementOnCorrected = null;
  let packageCert = null;
  let parity = null;

  if (group === "A") {
    packageCert = certifyAdpImmutablePackage({
      packagePayload: {
        ...payload,
        propertyId,
        periodId: man.latestPeriodId,
        _adpPermanentCertification: {
          certificationHash: measurement.certificationHash,
          certificationTimestamp: measurement.certificationTimestamp,
          methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
        },
      },
      measurementCertification: measurement,
      propertyId,
      measurementPeriodId: man.latestPeriodId,
    });
    parity = certifyAdpCustomerVisibleParity({
      propertyId,
      approvedPackage: payload,
      expectedPeriodId: man.latestPeriodId,
      surfaces: {},
    });
  }

  if (group === "B") {
    const frozenIds =
      payload.executiveMetrics?.considerationRate?.eligibleScenarioIds || [];
    // Evidence cert: verify Consideration reproduction; do not require baked PP/SP.
    const evidenceProbePayload = {
      executiveMetrics: {
        considerationRate: {
          ...payload.executiveMetrics?.considerationRate,
          eligibleScenarioIds: frozenIds,
        },
        scenarioPresence: {
          ...payload.executiveMetrics?.scenarioPresence,
          eligibleScenarios: frozenIds.length,
        },
      },
      methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
    };
    const evidenceCert = certifyAdpMeasurementReconciliation({
      propertyId,
      periodId: man.latestPeriodId,
      observations: period?.observations || [],
      propertyProfile: profile,
      packagePayload: evidenceProbePayload,
      frozenEligibleScenarioIds: frozenIds,
      methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
    });

    corrected = buildCorrectedPackageFromCertification(payload, evidenceCert, {
      supersedesNote:
        "Group B offline reprocess — Provider/Scenario Presence rebuilt from frozen eligibleScenarioIds + detectPropertyMention; Consideration unchanged",
    });

    if (corrected.ok) {
      // Stamp identity fields required for package cert
      corrected.payload.propertyId = propertyId;
      corrected.payload.periodId = man.latestPeriodId;
      corrected.payload.methodologyFingerprintSha256 =
        wrap.methodologyFingerprintSha256;
      corrected.payload.publicationManifestSha256 =
        wrap.publicationManifestSha256;
      corrected.payload.pdfArtifactSha256 = wrap.pdfArtifactSha256;

      measurementOnCorrected = certifyAdpMeasurementReconciliation({
        propertyId,
        periodId: man.latestPeriodId,
        observations: period?.observations || [],
        propertyProfile: profile,
        packagePayload: corrected.payload,
        frozenEligibleScenarioIds:
          payload.executiveMetrics?.considerationRate?.eligibleScenarioIds || [],
        methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
      });

      // Align package stamp hash to measurement-on-corrected
      if (measurementOnCorrected.ok) {
        corrected.payload._adpPermanentCertification.certificationHash =
          measurementOnCorrected.certificationHash;
        corrected.payload._adpPermanentCertification.certificationTimestamp =
          measurementOnCorrected.certificationTimestamp;
      }

      packageCert = certifyAdpImmutablePackage({
        packagePayload: corrected.payload,
        measurementCertification: measurementOnCorrected,
        propertyId,
        measurementPeriodId: man.latestPeriodId,
      });
      parity = certifyAdpCustomerVisibleParity({
        propertyId,
        approvedPackage: corrected.payload,
        expectedPeriodId: man.latestPeriodId,
        surfaces: {},
      });

      const outPath = join(PKG_OUT, `${propertyId}.corrected-package.json`);
      writeFileSync(
        outPath,
        JSON.stringify(
          {
            preparedAt: new Date().toISOString(),
            publicationAuthorized: false,
            published: false,
            propertyId,
            periodId: man.latestPeriodId,
            priorPublicationManifestSha256: wrap.publicationManifestSha256,
            priorPdfArtifactSha256: wrap.pdfArtifactSha256,
            methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
            measurementCertification: {
              ok: measurementOnCorrected.ok,
              certificationHash: measurementOnCorrected.certificationHash,
              consideration: measurementOnCorrected.canonicalMetrics?.consideration,
            },
            packageCertification: {
              ok: packageCert.ok,
              failures: packageCert.failures,
            },
            parity: { ok: parity.ok, failures: parity.failures },
            payload: corrected.payload,
          },
          null,
          2
        )
      );
      groupBCerts.push({
        propertyId,
        path: outPath,
        measurementOk: measurementOnCorrected.ok,
        packageOk: packageCert.ok,
        parityOk: parity.ok,
        considerationUnchanged:
          Number(payload.executiveMetrics?.considerationRate?.rate) ===
          Number(
            corrected.payload.executiveMetrics?.considerationRate?.rate
          ),
      });
    }
  }

  matrix.push({
    propertyId,
    group,
    periodId: man.latestPeriodId,
    bakedGrainPass: bakedGrain.pass,
    evidenceMeasurementOk:
      group === "B"
        ? measurement.ok || measurementOnCorrected?.ok
        : measurement.ok,
    measurementOk:
      group === "A" ? measurement.ok : measurementOnCorrected?.ok === true,
    packageOk: packageCert?.ok === true,
    parityOk: parity?.ok === true,
    consideration:
      payload.executiveMetrics?.considerationRate?.rate ?? null,
    frozenIds: (
      payload.executiveMetrics?.considerationRate?.eligibleScenarioIds || []
    ).length,
    certificationHash:
      group === "A"
        ? measurement.certificationHash
        : measurementOnCorrected?.certificationHash || null,
    failures:
      group === "A"
        ? measurement.failures
        : measurementOnCorrected?.failures || corrected?.reason || null,
  });
}

const summary = {
  assessedAt: new Date().toISOString(),
  contract: "ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1",
  hotelCount: matrix.length,
  groupA: {
    count: matrix.filter((r) => r.group === "A").length,
    allMeasurementOk: matrix
      .filter((r) => r.group === "A")
      .every((r) => r.measurementOk),
    allPackageOk: matrix
      .filter((r) => r.group === "A")
      .every((r) => r.packageOk),
    allParityOk: matrix.filter((r) => r.group === "A").every((r) => r.parityOk),
  },
  groupB: {
    count: matrix.filter((r) => r.group === "B").length,
    preparedPackages: groupBCerts.length,
    allMeasurementOk: groupBCerts.every((r) => r.measurementOk),
    allPackageOk: groupBCerts.every((r) => r.packageOk),
    allParityOk: groupBCerts.every((r) => r.parityOk),
    allConsiderationUnchanged: groupBCerts.every((r) => r.considerationUnchanged),
    published: false,
    publicationAuthorized: false,
  },
  groupC: { count: 0 },
  paidResearchUsd: 0,
  airtableWrites: false,
  productionDeploy: false,
  stopForFounderApproval: true,
};

writeFileSync(join(OUT, "38-hotel-reconciliation-matrix.json"), JSON.stringify({ summary, matrix, groupBCerts }, null, 2));
writeFileSync(join(OUT, "21-corrected-package-certification.json"), JSON.stringify({ summary: summary.groupB, groupBCerts }, null, 2));

console.log(JSON.stringify(summary, null, 2));
