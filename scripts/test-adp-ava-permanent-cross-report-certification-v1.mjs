#!/usr/bin/env node
/**
 * Regression fixtures for ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION_V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  ADP_AVA_CERTIFICATION_STATUS,
  ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
  certifyAvaAgainstCustomerFacingAdp,
  assertAvaAdpCrossReportCertificationForPublication,
  resolveCustomerFacingAdpForAvaDelivery,
  detectAdpAvaPermanentCertificationDefects,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";
import {
  isCustomerFacingPublicationAllowed,
  assertAdpAvaCertificationForCustomerReady,
} from "../lib/ai-visibility-audit-action-plan/lifecycle-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

function load(file) {
  return JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8"));
}

function clone(v) {
  return structuredClone(v);
}

// 1) Matching reports — Spice is the current-period CERTIFIED_MATCH hotel
{
  const payload = load("spice-island-beach-resort-v1.json");
  const res = resolveCustomerFacingAdpForAvaDelivery(payload.property.propertyId);
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
  assert.equal(res.alignment.avaEqualsPublished, true);
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
  assert.equal(cert.jointPublicationAllowed, true);
  assert.ok(cert.crossReportIdentity.adpPeriodId);
  assert.ok(cert.certificationId);
}

// 1b) Hilton remains CERTIFICATION_BLOCKED (known lean/customer metric drift)
{
  const payload = load("hilton-times-square-v1.json");
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED);
  assert.equal(cert.jointPublicationAllowed, false);
}

// 2) Incorrect consideration percentage → blocked / not match
{
  const bad = clone(load("spice-island-beach-resort-v1.json"));
  bad.kpis = bad.kpis.map((k) =>
    k.label === "AI Consideration" ? { ...k, value: "99.9%" } : k
  );
  const cert = await certifyAvaAgainstCustomerFacingAdp(bad);
  assert.notEqual(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
  assert.ok(
    (cert.defects || []).some((d) =>
      /METRIC_MISMATCH|LEAN_PIN/i.test(d.type)
    )
  );
}

// 3) Incorrect scenario denominator / rate
{
  const bad = clone(load("spice-island-beach-resort-v1.json"));
  bad.kpis = bad.kpis.map((k) =>
    k.label === "Scenario Presence" ? { ...k, value: "1.0%" } : k
  );
  const cert = await certifyAvaAgainstCustomerFacingAdp(bad);
  assert.notEqual(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
}

// 4) Lean/customer mismatch on Hilton remains blocked (no fabricated MATCH)
{
  const cert = await certifyAvaAgainstCustomerFacingAdp(
    load("hilton-times-square-v1.json")
  );
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED);
  assert.ok(
    (cert.defects || []).some((d) =>
      /METRIC_MISMATCH|LEAN_PIN/i.test(d.type)
    )
  );
}

// 5) Wrong peer ranking
{
  const bad = clone(load("spice-island-beach-resort-v1.json"));
  bad.kpis = bad.kpis.map((k) =>
    /Peer Position/i.test(k.label) ? { ...k, value: "#6 of 6" } : k
  );
  const cert = await certifyAvaAgainstCustomerFacingAdp(bad);
  assert.notEqual(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
}

// 6) Wrong hotel identity
{
  const bad = clone(load("spice-island-beach-resort-v1.json"));
  bad.property.propertyId = "adp_does_not_exist_xyz";
  const cert = await certifyAvaAgainstCustomerFacingAdp(bad);
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED);
}

// 7) Historical period with disclosure (AC Coruña — newer published/approved)
{
  const payload = load("ac-hotel-a-coruna-v1.json");
  assert.ok(payload.reporting?.adpPeriodId);
  const res = resolveCustomerFacingAdpForAvaDelivery(payload.property.propertyId);
  assert.equal(res.alignment.newerApprovedThanAva, true);
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload, {
    requireCurrentPeriodMatch: false,
  });
  assert.ok(
    [
      ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE,
      ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED,
    ].includes(cert.status)
  );
  assert.equal(cert.currentPeriodMatch, false);
}

// 8) New approved ADP package / require current match → reconciliation
{
  const payload = load("ac-hotel-a-coruna-v1.json");
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload, {
    requireCurrentPeriodMatch: true,
  });
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED);
  assert.throws(() =>
    assertAvaAdpCrossReportCertificationForPublication(cert, {
      requireCurrentPeriodMatch: true,
    })
  );
}

// 9) Stale certification / artifact hash mismatch
{
  const payload = load("hilton-times-square-v1.json");
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
  assert.throws(() =>
    assertAvaAdpCrossReportCertificationForPublication(cert, {
      requireCurrentPeriodMatch: true,
      expectedArtifactHashes: {
        avaPayloadSha256: "deadbeef",
      },
    })
  );
}

// 10) Publication lifecycle gate requires CERTIFIED_MATCH
{
  // Base lifecycle gate (auth flags) remains separate from ADP↔AVA cert assert.
  assert.equal(
    isCustomerFacingPublicationAllowed("CUSTOMER_READY", {
      founderPublicationAuthorized: true,
      customerPublicationAuthorized: true,
      founderApprovedForCustomer: true,
    }),
    true
  );
  assert.throws(() =>
    assertAdpAvaCertificationForCustomerReady({
      status: "CERTIFIED_HISTORICAL_REFERENCE",
      jointPublicationAllowed: false,
    })
  );
  assert.equal(
    assertAdpAvaCertificationForCustomerReady({
      status: "CERTIFIED_MATCH",
      jointPublicationAllowed: true,
      ok: true,
    }),
    true
  );
  assert.throws(() =>
    assertAdpAvaCertificationForCustomerReady({
      status: "RECONCILIATION_REQUIRED",
    })
  );
}

// 11) Sync detector catches missing disclosure when periods differ
{
  const bad = clone(load("ac-hotel-a-coruna-v1.json"));
  delete bad.reporting.adpPeriodId;
  delete bad.crossReportIdentity;
  const defects = detectAdpAvaPermanentCertificationDefects(bad);
  assert.ok(defects.some((d) => /differ|disclosure|period/i.test(d.message || "")));
}

// 12) Full portfolio certification script (38 hotels classified; known drifts remain blocked)
{
  const run = spawnSync(
    process.execPath,
    ["scripts/ava-adp-permanent-cross-report-certification-v1.mjs"],
    { cwd: ROOT, encoding: "utf8", timeout: 120000 }
  );
  assert.match(run.stdout, /"hotelCount": 38/);
  assert.match(run.stdout, /CERTIFIED_MATCH/);
  assert.match(run.stdout, /CERTIFIED_HISTORICAL_REFERENCE/);
  assert.ok(run.status === 0 || run.status === 2, run.stdout + "\n" + run.stderr);
}

console.log(`PASS ${ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION}`);
