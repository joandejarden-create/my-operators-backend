#!/usr/bin/env node
/**
 * Permanent regression for Hilton TS Full E2E reprocess v1 artifacts + safety.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT = path.join(ROOT, "reports/gdi-hilton-ts-full-e2e-reprocess-v1");
const HOTEL_ID = "rec35fExUxCClpOP6";

function read(name) {
  return JSON.parse(fs.readFileSync(path.join(REPORT, name), "utf8"));
}

function check(name, fn) {
  try {
    fn();
    console.log(`  PASS ${name}`);
  } catch (err) {
    console.error(`  FAIL ${name}`);
    throw err;
  }
}

// Ensure reports exist (idempotent $0 run without rewriting pointer).
const run = spawnSync(
  process.execPath,
  ["scripts/gdi-hilton-ts-full-e2e-reprocess-v1.mjs"],
  { cwd: ROOT, encoding: "utf8" },
);
assert.equal(run.status, 0, run.stderr || run.stdout);

check("counts_reconcile_45_44_pass_hold_reject", () => {
  const m = read("03_PASS_HOLD_REJECT_MATRIX.json");
  assert.equal(m.counts.ORIGINAL_45, 45);
  assert.equal(m.counts.ORIGINAL_44_RESEARCH_QUALIFIED, 44);
  assert.equal(
    m.counts.FINALIZATION_PASS +
      m.counts.HOLD_FOR_RESEARCH +
      m.counts.REJECT_HISTORICAL,
    45,
  );
  assert.ok(m.counts.FINALIZATION_PASS >= 1);
  assert.ok(m.counts.FINALIZATION_PASS < 44);
});

check("special_cases_disposition", () => {
  const q = read("02_ORIGINAL_44_QUALITY_ASSESSMENT.json");
  const byTitle = Object.fromEntries(
    (q.specialCases || []).map((s) => [s.title, s.disposition]),
  );
  assert.equal(byTitle["LA 2028 Olympics"], "REJECT");
  assert.equal(byTitle["Tech Conference Venues in New York 2026"], "REJECT");
  assert.equal(
    byTitle["PLUS Employment Practices Liability Symposium"],
    "HOLD",
  );
  assert.equal(byTitle["76th Annual Meeting"], "PASS");
});

check("competitive_westin_overflow_recovered", () => {
  const c = read("04_COMPETITIVE_RECOVERY.json");
  assert.equal(c.recoveredCount, 1);
  assert.equal(c.rows[0].competitive.incumbentHotel, "Westin New York at Times Square");
  assert.equal(c.rows[0].competitive.label, "EVIDENCED_OVERFLOW_OPPORTUNITY");
  assert.equal(c.rows[0].disposition, "PASS");
});

check("pass_rows_have_plausible_named_primary", () => {
  const manifest = read("01_ORIGINAL_45_MANIFEST.json");
  const matrix = read("03_PASS_HOLD_REJECT_MATRIX.json");
  for (const p of matrix.pass) {
    const row = manifest.rows.find((r) => r.opportunityId === p.opportunityId);
    assert.ok(row, p.opportunityId);
    assert.equal(row.primaryContact.plausiblePerson, true, p.opportunityId);
    assert.ok(row.namedCoverage.finalizeEligible, p.opportunityId);
  }
});

check("no_staged_pointer_activated", () => {
  const store = createGdiPublicationPackageStore();
  const ptr = store.readPointer(HOTEL_ID, "STAGED");
  assert.equal(ptr, null);
  const cert = read("09_CERTIFICATION.json");
  assert.equal(cert.checks.hiltonStagedPointerNotActivated, true);
  assert.equal(cert.ok, true);
});

check("peer_staged_pointers_still_present", () => {
  const cert = read("09_CERTIFICATION.json");
  for (const row of cert.stagedRegression) {
    assert.equal(row.hasStagedPointer, true, row.hotelName);
  }
});

check("opportunities_json_not_required_dirty", () => {
  const snap = read("ORIGINAL_OPPORTUNITIES_SNAPSHOT.json");
  assert.equal(snap.discoveredCount, 45);
  assert.equal(snap.researchQualifiedCount, 44);
});

console.log("\nOK test-gdi-hilton-ts-full-e2e-reprocess-v1");
