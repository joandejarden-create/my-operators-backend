#!/usr/bin/env node
/**
 * Structural gate for gold-standard certification artifacts + Findymail wiring.
 *   npm run test:gdi-canonical-full-e2e-gold-standard-certification-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

check("cert_script_and_staged_package_exist", () => {
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs"),
    ),
  );
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_46de4104/package.json",
      ),
    ),
  );
});

check("certification_dry_run_produces_reports", () => {
  const r = spawnSync(
    process.execPath,
    ["scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs"],
    { cwd: ROOT, encoding: "utf8", timeout: 120000 },
  );
  assert.equal(r.status, 0, r.stderr || r.stdout);
  const cert = path.join(
    ROOT,
    "reports/gdi-canonical-full-e2e-gold-standard-certification-v1/00-CERTIFICATION.json",
  );
  assert.ok(fs.existsSync(cert));
  const j = JSON.parse(fs.readFileSync(cert, "utf8"));
  assert.equal(j.hotelId, "recnPB3Q02NO8Ma1o");
  assert.equal(j.contactScorecard.after.opportunityCount, 26);
  assert.ok(
    j.certificationStatus === "PARTIAL" ||
      j.certificationStatus ===
        "GDI_CANONICAL_FULL_E2E_GOLD_STANDARD_CERTIFIED",
  );
  assert.equal(j.architecture.findymail.permanentStage4Component, true);
  assert.equal(j.reconstruction.loadableFromBundledPublication, true);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-canonical-full-e2e-gold-standard-certification-v1");
