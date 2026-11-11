#!/usr/bin/env node
/**
 * Permanent Full E2E certification regression fixtures.
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  certifyAdpMeasurementReconciliation,
  buildCorrectedPackageFromCertification,
} from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-measurement-reconciliation-certification-v1.js";
import { certifyAdpCustomerVisibleParity } from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-customer-visible-parity-certification-v1.js";
import { certifyAdpImmutablePackage } from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-immutable-package-certification-v1.js";
import {
  assertFullE2eManifestCertification,
  attachPermanentCertificationToManifest,
  ENFORCEMENT_POINTS,
} from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-permanent-full-e2e-certification-gate-v1.js";
import { canAdvanceTo, CERTIFICATION_LIFECYCLE_STATES } from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-permanent-full-e2e-lifecycle-states-v1.js";

const CURRENT = "data/adp-durable-published/current";
const SNAP = "data/adp-durable-published/snapshots";

function loadPkg(propertyId) {
  const man = JSON.parse(
    readFileSync(join(CURRENT, propertyId, "manifest.json"), "utf8")
  );
  const wrap = JSON.parse(
    readFileSync(join(SNAP, propertyId, man.reportFile), "utf8")
  );
  return { man, wrap, payload: wrap.payload || wrap };
}

// --- Lifecycle order ---
{
  const adv = canAdvanceTo(
    [CERTIFICATION_LIFECYCLE_STATES.RESEARCH_COMPLETE],
    CERTIFICATION_LIFECYCLE_STATES.METRICS_RECONCILED
  );
  assert.equal(adv.ok, false, "cannot skip upstream gates");
  console.log("PASS lifecycle no-skip");
}

// --- Hilton Provider Presence mismatch fixture ---
{
  const { man, wrap, payload } = loadPkg("adp_hilton_times_square");
  const period = loadPeriod(man.latestPeriodId);
  const profile = loadPropertyProfile("adp_hilton_times_square");
  assert.equal(
    payload.executiveMetrics.scenarioPresence.eligibleScenarios,
    53
  );
  assert.equal(
    payload.executiveMetrics.considerationRate.eligibleScenarioIds.length,
    47
  );

  // Baked package fails customer parity
  const bakedParity = certifyAdpCustomerVisibleParity({
    propertyId: "adp_hilton_times_square",
    approvedPackage: payload,
  });
  assert.equal(bakedParity.ok, false, "Hilton baked package must fail parity");

  const frozen = payload.executiveMetrics.considerationRate.eligibleScenarioIds;
  const evidenceCert = certifyAdpMeasurementReconciliation({
    propertyId: "adp_hilton_times_square",
    periodId: man.latestPeriodId,
    observations: period.observations,
    propertyProfile: profile,
    packagePayload: {
      executiveMetrics: {
        considerationRate: payload.executiveMetrics.considerationRate,
        scenarioPresence: {
          ...payload.executiveMetrics.scenarioPresence,
          eligibleScenarios: frozen.length,
        },
      },
    },
    frozenEligibleScenarioIds: frozen,
    methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
  });
  assert.equal(evidenceCert.ok, true, "Hilton evidence must certify");
  assert.equal(
    evidenceCert.canonicalMetrics.consideration.presentObservations,
    18
  );

  const corrected = buildCorrectedPackageFromCertification(payload, evidenceCert);
  assert.equal(corrected.ok, true);
  const correctedParity = certifyAdpCustomerVisibleParity({
    propertyId: "adp_hilton_times_square",
    approvedPackage: corrected.payload,
  });
  assert.equal(correctedParity.ok, true, "corrected Hilton must pass parity");
  console.log("PASS Hilton Provider Presence mismatch fixture");
}

// --- Casas inflated runtime mentioned flags ---
{
  const { man, wrap, payload } = loadPkg("adp_casas_del_xvi");
  const period = loadPeriod(man.latestPeriodId);
  const profile = loadPropertyProfile("adp_casas_del_xvi");
  const frozen = payload.executiveMetrics.considerationRate.eligibleScenarioIds;
  const cert = certifyAdpMeasurementReconciliation({
    propertyId: "adp_casas_del_xvi",
    periodId: man.latestPeriodId,
    observations: period.observations,
    propertyProfile: profile,
    packagePayload: {
      executiveMetrics: {
        considerationRate: payload.executiveMetrics.considerationRate,
        scenarioPresence: {
          ...payload.executiveMetrics.scenarioPresence,
          eligibleScenarios: frozen.length,
        },
      },
    },
    frozenEligibleScenarioIds: frozen,
    methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
  });
  assert.equal(cert.ok, true);
  assert.equal(cert.canonicalMetrics.consideration.rate, 10.1);
  assert.equal(cert.canonicalMetrics.consideration.presentObservations, 17);
  // Stored flags would inflate — certify path must ignore them
  const storedPresent = period.observations.filter(
    (o) =>
      frozen.includes(o.scenarioId) &&
      o.mentioned === true
  ).length;
  assert.ok(
    storedPresent > 17,
    "fixture expects stale stored mentioned flags > certified"
  );
  console.log("PASS Casas del XVI inflated runtime flags fixture");
}

// --- Legacy 78 contamination ---
{
  const poisoned = {
    executiveMetrics: {
      considerationRate: {
        presentObservations: 10,
        comparableObservations: 40,
        rate: 25,
        methodologyId: "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1",
        eligibleScenarioIds: Array.from({ length: 10 }, (_, i) => `s${i}`),
      },
      scenarioPresence: { eligibleScenarios: 10, capturedScenarios: 2, rate: 20 },
    },
    evidence: {
      providers: [
        { provider: "openai", mentioned: 3, comparable: 78, scenariosScheduled: 78 },
        { provider: "gemini", mentioned: 3, comparable: 78, scenariosScheduled: 78 },
        { provider: "perplexity", mentioned: 2, comparable: 78, scenariosScheduled: 78 },
        { provider: "claude", mentioned: 2, comparable: 78, scenariosScheduled: 78 },
      ],
    },
  };
  const parity = certifyAdpCustomerVisibleParity({
    propertyId: "fixture_legacy_78",
    approvedPackage: poisoned,
  });
  assert.equal(parity.ok, false);
  console.log("PASS legacy 78 contamination fixture");
}

// --- Missing response text fail-closed ---
{
  const cert = certifyAdpMeasurementReconciliation({
    propertyId: "fixture_missing_text",
    periodId: "p1",
    observations: [
      { observationId: "o1", scenarioId: "s1", provider: "openai", mentioned: true },
      { observationId: "o2", scenarioId: "s1", provider: "gemini", mentioned: false },
    ],
    frozenEligibleScenarioIds: ["s1"],
    propertyProfile: { name: "Test Hotel" },
  });
  assert.equal(cert.ok, false);
  assert.ok(
    cert.failures.some((f) => f.check === "A" || f.code === "EVIDENCE_INCOMPLETE")
  );
  console.log("PASS missing response text fail-closed");
}

// --- Invalid frozen eligibility ---
{
  const cert = certifyAdpMeasurementReconciliation({
    propertyId: "fixture_no_frozen",
    observations: [
      {
        observationId: "o1",
        scenarioId: "s1",
        provider: "openai",
        rawResponse: "Hello Test Hotel is great",
        mentioned: true,
        parsed: true,
      },
    ],
    frozenEligibleScenarioIds: [],
    propertyProfile: { name: "Test Hotel" },
  });
  assert.equal(cert.ok, false);
  console.log("PASS invalid frozen eligibility");
}

// --- Manifest CUSTOMER_READY without cert artifact ---
{
  const bad = assertFullE2eManifestCertification({
    customerReady: true,
    permanentCertification: null,
  });
  assert.equal(bad.ok, false);
  const good = attachPermanentCertificationToManifest(
    {},
    {
      ok: true,
      artifact: { certificationHash: "abc", required: true },
      measurement: { certificationTimestamp: new Date().toISOString() },
      lifecycleState: "METRICS_RECONCILED",
      enforcementPoint: ENFORCEMENT_POINTS.AFTER_MEASUREMENT_CALCULATION,
    }
  );
  const okManifest = assertFullE2eManifestCertification({
    ...good,
    customerReady: true,
  });
  assert.equal(okManifest.ok, true);
  console.log("PASS manifest certification artifact gate");
}

// --- Prepared Group B packages exist and certify ---
{
  const hiltonPath =
    "reports/dealality-adp-permanent-full-e2e-certification-v1/prepared-group-b-packages/adp_hilton_times_square.corrected-package.json";
  assert.ok(existsSync(hiltonPath), "Hilton corrected package must be prepared");
  const prep = JSON.parse(readFileSync(hiltonPath, "utf8"));
  assert.equal(prep.published, false);
  assert.equal(prep.publicationAuthorized, false);
  assert.equal(prep.measurementCertification.ok, true);
  assert.equal(prep.payload.executiveMetrics.scenarioPresence.eligibleScenarios, 47);
  const pkgCert = certifyAdpImmutablePackage({
    packagePayload: prep.payload,
    measurementCertification: {
      ok: true,
      certificationHash: prep.measurementCertification.certificationHash,
      populationDenominatorManifest: {
        eligibleScenarioIds:
          prep.payload.executiveMetrics.considerationRate.eligibleScenarioIds,
      },
    },
  });
  assert.equal(pkgCert.ok, true);
  console.log("PASS prepared Group B package certification");
}

// --- HTTP 200 insufficient (browser blocked) ---
{
  const { payload } = loadPkg("adp_hilton_times_square");
  const parity = certifyAdpCustomerVisibleParity({
    propertyId: "adp_hilton_times_square",
    approvedPackage: payload,
    surfaces: {
      shareApi: { httpStatus: 200, payload },
      browser: { httpStatus: 200, payload, blockedReport: true },
    },
  });
  assert.equal(parity.ok, false);
  console.log("PASS HTTP 200 but browser blocked");
}

console.log("OK test-adp-permanent-full-e2e-certification-v1");
