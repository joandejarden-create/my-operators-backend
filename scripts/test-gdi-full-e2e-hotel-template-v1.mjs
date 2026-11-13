#!/usr/bin/env node
/**
 * Permanent CI gate for GDI_FULL_E2E_HOTEL_TEMPLATE_V1.
 *
 *   npm run test:gdi-full-e2e-hotel-template-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  TEMPLATE_REFERENCE,
  runProspectivePresentationPersistenceCycle,
  runCrossHotelPortabilityChecks,
  readBethesdaPublicationIdentity,
  evaluateFullE2eHotelTemplateCertificate,
  REQUIRED_CUSTOMER_PRESENTATION_KEYS,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-full-e2e-hotel-template-v1");

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("template_id_locked", () => {
  assert.equal(GDI_FULL_E2E_HOTEL_TEMPLATE_V1, "GDI_FULL_E2E_HOTEL_TEMPLATE_V1");
  assert.ok(REQUIRED_CUSTOMER_PRESENTATION_KEYS.includes("salesActionPillLabel"));
  assert.ok(REQUIRED_CUSTOMER_PRESENTATION_KEYS.includes("needDateMatchV1"));
});

check("bethesda_publication_fingerprint_immutable", () => {
  const { current, pkg } = readBethesdaPublicationIdentity();
  assert.equal(
    current.contentFingerprintSha256,
    TEMPLATE_REFERENCE.contentFingerprintSha256,
  );
  assert.equal(
    pkg.contentFingerprintSha256,
    TEMPLATE_REFERENCE.contentFingerprintSha256,
  );
  assert.equal(current.publicationVersion, TEMPLATE_REFERENCE.publicationVersion);
});

check("prospective_airtable_presentation_cycle", () => {
  const r = runProspectivePresentationPersistenceCycle();
  assert.equal(r.ok, true);
  assert.equal(r.mutationDetected, true);
  assert.equal(r.enrichKeepsAction, true);
});

check("cross_hotel_portability_readonly", () => {
  const r = runCrossHotelPortabilityChecks();
  assert.equal(r.ok, true);
  assert.equal(r.uiHasBethesdaLiteral, false);
  assert.equal(r.noDefaultModeBanner, true);
  for (const h of r.hotels) {
    assert.equal(h.configPresent, true, h.hotelId);
    assert.equal(h.bethesdaDataLeak, false, h.hotelId);
  }
});

check("certificate_pack_present_and_certified", () => {
  assert.ok(fs.existsSync(path.join(OUT, "14-TEMPLATE_CERTIFICATE.json")));
  assert.ok(fs.existsSync(path.join(OUT, "FOUNDER_REPORT.md")));
  assert.ok(fs.existsSync(path.join(OUT, "OPERATING_PLAYBOOK.md")));
  const cert = JSON.parse(
    fs.readFileSync(path.join(OUT, "14-TEMPLATE_CERTIFICATE.json"), "utf8"),
  );
  assert.equal(cert.schema, GDI_FULL_E2E_HOTEL_TEMPLATE_V1);
  assert.equal(cert.certified, true);
  assert.equal(cert.status, "CERTIFIED");
  assert.equal(
    cert.reference.contentFingerprintSha256,
    TEMPLATE_REFERENCE.contentFingerprintSha256,
  );
});

check("evaluator_rejects_incomplete_evidence", () => {
  const bad = evaluateFullE2eHotelTemplateCertificate({});
  assert.equal(bad.certified, false);
  assert.ok(bad.fails.length > 0);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-full-e2e-hotel-template-v1");
