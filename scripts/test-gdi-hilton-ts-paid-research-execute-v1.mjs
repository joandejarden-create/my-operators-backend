#!/usr/bin/env node
/**
 * Offline regression for Hilton TS paid research execute artifacts.
 * Does NOT call Exa / does NOT spend.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT = path.join(ROOT, "reports/gdi-hilton-ts-paid-research-execute-v1");
const HOTEL_ID = "rec35fExUxCClpOP6";
const AUTH = "FOUNDER_APPROVE_HILTON_TS_RESEARCH_COMPLETION_16USD_20261124";
const PKG_VERSION = "gdi_hilton_ts_paid_research_v1_20261124";

function read(name) {
  return JSON.parse(fs.readFileSync(path.join(REPORT, name), "utf8"));
}
function check(name, fn) {
  try {
    fn();
    console.log(`  PASS ${name}`);
  } catch (e) {
    console.error(`  FAIL ${name}`);
    throw e;
  }
}

check("artifacts_present", () => {
  for (const f of [
    "01_SPENDING_LEDGER.json",
    "02_RESEARCH_FINDINGS.json",
    "03_FOUNDER_SCORECARD.json",
    "04_CERTIFICATION.json",
    "05_ID_RECONCILIATION.json",
    "06_CONTACT_MATRIX.json",
    "FOUNDER_REPORT.md",
  ]) {
    assert.ok(fs.existsSync(path.join(REPORT, f)), f);
  }
});

check("spend_within_ceilings", () => {
  const ledger = read("01_SPENDING_LEDGER.json");
  assert.equal(ledger.authorizationToken, AUTH);
  assert.ok(ledger.agentOff === true);
  assert.ok(ledger.surfe === false);
  assert.ok(ledger.findymail === false);
  assert.ok(ledger.spentUsd.aggregate <= 16 + 1e-9);
  assert.ok(ledger.spentUsd.whoAlt <= 5 + 1e-9);
  assert.ok(ledger.spentUsd.competitive <= 8 + 1e-9);
  assert.ok(ledger.spentUsd.how <= 3 + 1e-9);
  assert.ok(ledger.spentUsd.aggregate > 0);
});

check("certification_pass_no_pointer", () => {
  const cert = read("04_CERTIFICATION.json");
  assert.equal(cert.ok, true);
  assert.equal(cert.checks.agentOff, true);
  assert.equal(cert.checks.hiltonPointerNotActivated, true);
  assert.equal(cert.checks.noSurfe, true);
  assert.equal(cert.checks.noFindymail, true);
  assert.equal(cert.checks.noAirtable, true);
  const store = createGdiPublicationPackageStore();
  assert.equal(store.readPointer(HOTEL_ID, "STAGED"), null);
});

check("final_candidates_quality", () => {
  const score = read("03_FOUNDER_SCORECARD.json");
  assert.equal(score.auth, AUTH);
  assert.equal(score.finalization.total, 6);
  assert.equal(score.finalization.salesReady, 0);
  assert.ok(score.finalization.ids.includes("gdi_opp_5282e7a3223f2741"));
  assert.ok(
    score.holdRecovered.some((h) => h.opportunityId === "gdi_opp_5282e7a3223f2741"),
  );
  assert.ok(
    score.holdRemaining.some((h) => h.opportunityId === "gdi_opp_5f3bb06d74145fdc"),
  );
  const plusHold = score.holdRemaining.find(
    (h) => h.opportunityId === "gdi_opp_5f3bb06d74145fdc",
  );
  assert.equal(plusHold.competitiveIntel.incumbentHotel, "New York Marriott Marquis");
});

check("contact_matrix_no_junk_primary", () => {
  const matrix = read("06_CONTACT_MATRIX.json");
  assert.equal(matrix.length, 6);
  const scbwi = matrix.find((r) => /SCBWI/i.test(r.title));
  assert.equal(scbwi.primary.name, "Sarah Baker");
  assert.ok(scbwi.secondary?.name);
  for (const row of matrix) {
    assert.ok(row.primary?.name);
    assert.ok(!/^(Global Virtual|Role Type|Event Travel|Book Writers)$/i.test(row.primary.name));
  }
});

check("candidate_package_written_not_pointed", () => {
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL_ID,
    PKG_VERSION,
    "package.json",
  );
  assert.ok(fs.existsSync(pkgPath), pkgPath);
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  assert.equal(pkg.publicationVersion, PKG_VERSION);
  assert.equal(pkg.opportunities.length, 6);
  const names = pkg.opportunities.map((o) => o.primaryContact?.name);
  assert.ok(names.includes("Sarah Baker"));
  assert.ok(!names.includes("Global Virtual"));
  assert.ok(!names.includes("Role Type"));
  const store = createGdiPublicationPackageStore();
  assert.equal(store.readPointer(HOTEL_ID, "STAGED"), null);
});

console.log("\nOK test-gdi-hilton-ts-paid-research-execute-v1");
