#!/usr/bin/env node
/**
 * Permanent Full E2E certification — Layer A offline CI fixtures.
 *
 * Uses sanitized synthetic fixtures under
 * fixtures/ai-demand-positioning/permanent-full-e2e-offline/.
 * Does NOT require data/adp-durable-published (Layer B).
 * Synthetic fixtures are NOT production proof.
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "fs";
import { createHash } from "crypto";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
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
import {
  canAdvanceTo,
  CERTIFICATION_LIFECYCLE_STATES,
} from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-permanent-full-e2e-lifecycle-states-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const FIXTURE_ROOT = join(
  ROOT,
  "fixtures/ai-demand-positioning/permanent-full-e2e-offline"
);

function loadJson(name) {
  const path = join(FIXTURE_ROOT, name);
  assert.ok(existsSync(path), `missing fixture ${name}`);
  return JSON.parse(readFileSync(path, "utf8"));
}

function assertChecksum(name, expected) {
  const text = readFileSync(join(FIXTURE_ROOT, name), "utf8");
  const actual = createHash("sha256").update(text).digest("hex");
  assert.equal(actual, expected, `checksum drift for ${name}`);
}

const manifest = loadJson("MANIFEST.json");
assert.equal(manifest.kind, "SYNTHETIC_OFFLINE_CI");
assert.equal(manifest.notProductionProof, true);
for (const [name, sha] of Object.entries(manifest.checksums || {})) {
  assertChecksum(name, sha);
}
console.log("PASS offline fixture provenance + checksums");

// --- Lifecycle order ---
{
  const adv = canAdvanceTo(
    [CERTIFICATION_LIFECYCLE_STATES.RESEARCH_COMPLETE],
    CERTIFICATION_LIFECYCLE_STATES.METRICS_RECONCILED
  );
  assert.equal(adv.ok, false, "cannot skip upstream gates");
  console.log("PASS lifecycle no-skip");
}

// --- Provider Presence / scenario mismatch → correct via evidence ---
{
  const profile = loadJson("property-profile.json");
  const { observations, periodId, propertyId } = loadJson("observations.json");
  const baked = loadJson("baked-package.json");
  const frozen = baked.executiveMetrics.considerationRate.eligibleScenarioIds;

  assert.equal(
    baked.executiveMetrics.scenarioPresence.eligibleScenarios,
    53
  );
  assert.equal(frozen.length, 8);

  const bakedParity = certifyAdpCustomerVisibleParity({
    propertyId,
    approvedPackage: baked,
  });
  assert.equal(bakedParity.ok, false, "baked package must fail parity");

  const evidenceCert = certifyAdpMeasurementReconciliation({
    propertyId,
    periodId,
    observations,
    propertyProfile: profile,
    packagePayload: {
      executiveMetrics: {
        considerationRate: baked.executiveMetrics.considerationRate,
        scenarioPresence: {
          ...baked.executiveMetrics.scenarioPresence,
          eligibleScenarios: frozen.length,
        },
      },
    },
    frozenEligibleScenarioIds: frozen,
    methodologyFingerprintSha256: baked.methodologyFingerprintSha256,
  });
  assert.equal(evidenceCert.ok, true, "evidence must certify");
  assert.equal(
    evidenceCert.canonicalMetrics.consideration.presentObservations,
    manifest.expected.certifiedPresentObservations
  );

  const corrected = buildCorrectedPackageFromCertification(baked, evidenceCert);
  assert.equal(corrected.ok, true);
  const correctedParity = certifyAdpCustomerVisibleParity({
    propertyId,
    approvedPackage: corrected.payload,
  });
  assert.equal(correctedParity.ok, true, "corrected package must pass parity");

  const staleMentioned = observations.filter(
    (o) => frozen.includes(o.scenarioId) && o.mentioned === true
  ).length;
  assert.ok(
    staleMentioned > evidenceCert.canonicalMetrics.consideration.presentObservations,
    "fixture expects stale stored mentioned flags > certified"
  );
  console.log("PASS offline provider mismatch + runtime-flag ignore");
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

// --- Prepared offline package exists and certifies ---
{
  const prep = loadJson("prepared-package.json");
  assert.equal(prep.published, false);
  assert.equal(prep.publicationAuthorized, false);
  assert.equal(prep.measurementCertification.ok, true);
  assert.equal(
    prep.payload.executiveMetrics.scenarioPresence.eligibleScenarios,
    prep.payload.executiveMetrics.considerationRate.eligibleScenarioIds.length
  );
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
  console.log("PASS prepared offline package certification");
}

// --- HTTP 200 insufficient (browser blocked) ---
{
  const prep = loadJson("prepared-package.json");
  const parity = certifyAdpCustomerVisibleParity({
    propertyId: prep.propertyId,
    approvedPackage: prep.payload,
    surfaces: {
      shareApi: { httpStatus: 200, payload: prep.payload },
      browser: { httpStatus: 200, payload: prep.payload, blockedReport: true },
    },
  });
  assert.equal(parity.ok, false);
  console.log("PASS HTTP 200 but browser blocked");
}

console.log(
  JSON.stringify(
    {
      ok: true,
      layer: "A",
      gate: "test:adp-permanent-full-e2e-certification-v1",
      fixtureRoot: "fixtures/ai-demand-positioning/permanent-full-e2e-offline",
      notProductionProof: true,
    },
    null,
    2
  )
);
console.log("OK test-adp-permanent-full-e2e-certification-v1 (Layer A offline)");
