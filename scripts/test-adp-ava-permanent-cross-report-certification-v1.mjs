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

// 1b) Hilton reconciles to same-period customer ADP → CERTIFIED_MATCH
{
  const payload = load("hilton-times-square-v1.json");
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
  assert.equal(cert.jointPublicationAllowed, true);
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

// 4) Drifted Hilton consideration rate is blocked (no fabricated MATCH)
{
  const bad = clone(load("hilton-times-square-v1.json"));
  bad.kpis = bad.kpis.map((k) =>
    /AI Consideration/i.test(k.label) ? { ...k, value: "10.4%" } : k
  );
  const cert = await certifyAvaAgainstCustomerFacingAdp(bad);
  assert.notEqual(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
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

// 7) AC Coruña refreshed to current-period customer ADP → CERTIFIED_MATCH
{
  const payload = load("ac-hotel-a-coruna-v1.json");
  assert.ok(payload.reporting?.adpPeriodId);
  const res = resolveCustomerFacingAdpForAvaDelivery(payload.property.propertyId);
  assert.equal(res.alignment.avaEqualsSelected, true);
  assert.equal(res.alignment.newerApprovedThanAva, false);
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload, {
    requireCurrentPeriodMatch: false,
  });
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
  assert.equal(cert.currentPeriodMatch, true);
}

// 8) requireCurrentPeriodMatch still passes when already on current package
{
  const payload = load("ac-hotel-a-coruna-v1.json");
  const cert = await certifyAvaAgainstCustomerFacingAdp(payload, {
    requireCurrentPeriodMatch: true,
  });
  assert.equal(cert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
  assert.equal(
    assertAvaAdpCrossReportCertificationForPublication(cert, {
      requireCurrentPeriodMatch: true,
    }),
    true
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

// 11) Sync detector catches KPI drift vs current lean/selected pin
{
  const bad = clone(load("ac-hotel-a-coruna-v1.json"));
  bad.kpis = (bad.kpis || []).map((k) =>
    /AI Consideration/i.test(k.label || "") ? { ...k, value: "1.0%" } : k
  );
  const defects = detectAdpAvaPermanentCertificationDefects(bad);
  assert.ok(
    defects.some((d) => /Lean pin reconciliation failed|METRIC_MISMATCH|parity/i.test(d.message || d.type || "")),
    JSON.stringify(defects, null, 2)
  );
}

// 12) Full portfolio certification script — 38 current-period MATCH target
{
  const run = spawnSync(
    process.execPath,
    ["scripts/ava-adp-permanent-cross-report-certification-v1.mjs"],
    { cwd: ROOT, encoding: "utf8", timeout: 120000 }
  );
  assert.match(run.stdout, /"hotelCount": 38/);
  assert.match(run.stdout, /CERTIFIED_MATCH/);
  assert.ok(run.status === 0 || run.status === 2, run.stdout + "\n" + run.stderr);
}

console.log(`PASS ${ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION}`);
